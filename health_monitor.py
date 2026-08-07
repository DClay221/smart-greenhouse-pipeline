import json
import time
import logging
import smtplib
import boto3
import requests
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
from botocore.exceptions import ClientError
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Check interval ────────────────────────────────────────────
CHECK_INTERVAL    = 300   # check every 5 minutes
MAX_S3_LAG_SECS   = 120   # alert if no S3 write in 2 minutes
ALERT_COOLDOWN    = 1800  # don't re-alert same issue for 30 minutes

# ── Track alert state to prevent spam ────────────────────────
last_alerts = {}
last_daily_summary = None

def check_kafka() -> tuple:
    """Verify Kafka broker is reachable and topic exists."""
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=config.KAFKA_BROKER,
            request_timeout_ms=5000
        )
        topics = admin.list_topics()
        admin.close()
        if config.KAFKA_TOPIC in topics:
            return True, "Kafka broker reachable, topic exists"
        return False, f"Kafka reachable but topic '{config.KAFKA_TOPIC}' missing"
    except KafkaError as e:
        return False, f"Kafka unreachable: {e}"
    except Exception as e:
        return False, f"Kafka check failed: {e}"

def check_influxdb() -> tuple:
    """Verify InfluxDB health endpoint responds."""
    try:
        response = requests.get(
            f"{config.INFLUXDB_URL}/health",
            timeout=5
        )
        if response.status_code == 200:
            return True, "InfluxDB healthy"
        return False, f"InfluxDB returned status {response.status_code}"
    except Exception as e:
        return False, f"InfluxDB unreachable: {e}"

def check_sensor_api() -> tuple:
    """Verify the sensor API health endpoint responds."""
    try:
        response = requests.get(
            f"http://localhost:{config.API_PORT}/health",
            timeout=5
        )
        data = response.json()
        if data.get("status") == "healthy":
            return True, "Sensor API healthy"
        return False, f"Sensor API returned unexpected status: {data}"
    except Exception as e:
        return False, f"Sensor API unreachable: {e}"

def check_s3_freshness() -> tuple:
    """Verify S3 is receiving recent writes."""
    try:
        s3    = boto3.client("s3", region_name=config.AWS_REGION)
        today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        prefix = f"{config.S3_PREFIX}/{today}/"

        paginator = s3.get_paginator("list_objects_v2")
        latest_obj = None

        for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=prefix):
            objects = page.get("Contents", [])
            if objects:
                page_latest = max(objects, key=lambda x: x["LastModified"])
                if latest_obj is None or page_latest["LastModified"] > latest_obj["LastModified"]:
                    latest_obj = page_latest

        if latest_obj is None:
            return False, "No S3 writes found for today"

        age = (datetime.now(timezone.utc) - latest_obj["LastModified"]).total_seconds()

        if age > MAX_S3_LAG_SECS:
            return False, f"Last S3 write was {age:.0f}s ago — pipeline may be stalled"
        return True, f"S3 receiving data — last write {age:.0f}s ago"

    except ClientError as e:
        return False, f"S3 check failed: {e}"

def check_weather_state() -> tuple:
    """Verify weather fetcher is producing fresh state."""
    try:
        with open(config.WEATHER_STATE_FILE, "r") as f:
            state = json.load(f)
        ts  = datetime.fromisoformat(state["timestamp"])
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        if age > 1800:  # older than 30 minutes
            return False, f"Weather state is {age/60:.0f} minutes old — fetcher may be down"
        return True, f"Weather fetcher active — last update {age/60:.0f} minutes ago"
    except FileNotFoundError:
        return False, "Weather state file not found — fetcher has not run"
    except Exception as e:
        return False, f"Weather state check failed: {e}"

def run_all_checks() -> dict:
    """Run all health checks and return results."""
    checks = {
        "Kafka Broker":   check_kafka,
        "InfluxDB":       check_influxdb,
        "Sensor API":     check_sensor_api,
        "S3 Data Lake":   check_s3_freshness,
        "Weather Fetcher": check_weather_state,
    }

    results = {}
    for name, check_fn in checks.items():
        try:
            healthy, message = check_fn()
            results[name] = {"healthy": healthy, "message": message}
            status = "OK" if healthy else "FAIL"
            logger.info(f"[{status}] {name}: {message}")
        except Exception as e:
            results[name] = {"healthy": False, "message": f"Check threw exception: {e}"}
            logger.error(f"[ERROR] {name} check threw exception: {e}")

    return results

def should_alert(component: str) -> bool:
    """Respect cooldown period to prevent alert spam."""
    if component not in last_alerts:
        return True
    elapsed = time.time() - last_alerts[component]
    return elapsed >= ALERT_COOLDOWN

def build_alert_email(failures: dict) -> str:
    """Build HTML alert email for pipeline failures."""
    now   = datetime.now().strftime("%A, %B %d %Y at %I:%M %p")
    rows  = ""
    for name, result in failures.items():
        rows += f"""
        <tr>
            <td style="padding:10px 16px; font-weight:600;
                       color:#dc3545;">{name}</td>
            <td style="padding:10px 16px;
                       color:#555;">{result['message']}</td>
        </tr>"""

    return f"""
    <html><body style="font-family:Arial,sans-serif; max-width:600px;
                       margin:auto; padding:20px; color:#333;">
        <div style="background:#dc3545; color:white; padding:20px;
                    border-radius:8px; margin-bottom:20px;">
            <h2 style="margin:0;">Pipeline Health Alert</h2>
            <p style="margin:8px 0 0; opacity:0.85;">{now}</p>
        </div>
        <p>The following pipeline components have failed their health checks:</p>
        <table style="width:100%; border-collapse:collapse;
                      border:1px solid #dee2e6; border-radius:6px;
                      overflow:hidden; margin:16px 0;">
            <thead>
                <tr style="background:#f8f9fa;">
                    <th style="padding:10px 16px; text-align:left;
                               font-size:0.8rem; text-transform:uppercase;
                               letter-spacing:0.05em;">Component</th>
                    <th style="padding:10px 16px; text-align:left;
                               font-size:0.8rem; text-transform:uppercase;
                               letter-spacing:0.05em;">Error</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        <p style="color:#666; font-size:0.85rem;">
            SSH into the Pi and run
            <code>sudo systemctl status greenhouse-processor</code>
            (or the relevant service) to investigate.
        </p>
        <p style="color:#999; font-size:0.75rem; margin-top:24px;">
            Sent by Smart Greenhouse Health Monitor —
            {config.DEVICE_ID}
        </p>
    </body></html>
    """

def send_alert(failures: dict):
    """Send health alert email via Gmail SMTP."""
    try:
        html    = build_alert_email(failures)
        msg     = MIMEMultipart("alternative")
        msg["Subject"] = f"Pipeline Alert — {len(failures)} component(s) down"
        msg["From"]    = config.EMAIL_SENDER
        msg["To"]      = config.EMAIL_RECIPIENT
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(config.EMAIL_SENDER, config.EMAIL_APP_PASSWORD)
            server.sendmail(
                config.EMAIL_SENDER,
                config.EMAIL_RECIPIENT,
                msg.as_string()
            )
        logger.warning(
            f"Alert email sent for {len(failures)} failed component(s): "
            f"{list(failures.keys())}"
        )
    except Exception as e:
        logger.error(f"Failed to send alert email: {e}", exc_info=True)

def send_recovery_email(recovered: dict):
    """Send recovery notification when previously failed components come back online."""
    try:
        now   = datetime.now().strftime("%A, %B %d %Y at %I:%M %p")
        rows  = ""
        for name in recovered:
            rows += f"""
            <tr>
                <td style="padding:10px 16px; font-weight:600;
                           color:#28a745;">{name}</td>
                <td style="padding:10px 16px;
                           color:#555;">Component recovered — all checks passing</td>
            </tr>"""

        html = f"""
        <html><body style="font-family:Arial,sans-serif; max-width:600px;
                           margin:auto; padding:20px; color:#333;">
            <div style="background:#28a745; color:white; padding:20px;
                        border-radius:8px; margin-bottom:20px;">
                <h2 style="margin:0;">Pipeline Recovery Notification</h2>
                <p style="margin:8px 0 0; opacity:0.85;">{now}</p>
            </div>
            <p>The following pipeline components have recovered and are
               passing all health checks:</p>
            <table style="width:100%; border-collapse:collapse;
                          border:1px solid #dee2e6; border-radius:6px;
                          overflow:hidden; margin:16px 0;">
                <thead>
                    <tr style="background:#f8f9fa;">
                        <th style="padding:10px 16px; text-align:left;
                                   font-size:0.8rem; text-transform:uppercase;
                                   letter-spacing:0.05em;">Component</th>
                        <th style="padding:10px 16px; text-align:left;
                                   font-size:0.8rem; text-transform:uppercase;
                                   letter-spacing:0.05em;">Status</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            <p style="color:#666; font-size:0.85rem;">
                No action required — the pipeline is operating normally.
            </p>
            <p style="color:#999; font-size:0.75rem; margin-top:24px;">
                Sent by Smart Greenhouse Health Monitor — {config.DEVICE_ID}
            </p>
        </body></html>
        """

        msg             = MIMEMultipart("alternative")
        msg["Subject"]  = f"Pipeline Recovered — {len(recovered)} component(s) back online"
        msg["From"]     = config.EMAIL_SENDER
        msg["To"]       = config.EMAIL_RECIPIENT
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(config.EMAIL_SENDER, config.EMAIL_APP_PASSWORD)
            server.sendmail(
                config.EMAIL_SENDER,
                config.EMAIL_RECIPIENT,
                msg.as_string()
            )
        logger.info(f"Recovery email sent for: {list(recovered.keys())}")
    except Exception as e:
        logger.error(f"Failed to send recovery email: {e}", exc_info=True)

def send_daily_summary():
    """Send daily S3 write count summary at 11:55 PM."""
    try:
        s3    = boto3.client("s3", region_name=config.AWS_REGION)
        today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        prefix = f"{config.S3_PREFIX}/{today}/"

        # Count all objects written today using paginator
        paginator   = s3.get_paginator("list_objects_v2")
        total_count = 0
        for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=prefix):
            total_count += len(page.get("Contents", []))

        expected_count = 5760  # 86400 seconds / 15 second interval
        missing        = max(0, expected_count - total_count)
        coverage_pct   = round((total_count / expected_count) * 100, 1)
        status_color   = "#28a745" if missing < 100 else "#fd7e14" if missing < 500 else "#dc3545"
        status_label   = "Healthy" if missing < 100 else "Degraded" if missing < 500 else "Critical"

        date_str = datetime.now().strftime("%A, %B %d %Y")
        now_str  = datetime.now().strftime("%I:%M %p")

        html = f"""
        <html><body style="font-family:Arial,sans-serif; max-width:600px;
                           margin:auto; padding:20px; color:#333;">
            <div style="background:#1b4332; color:white; padding:20px;
                        border-radius:8px; margin-bottom:20px;">
                <h2 style="margin:0;">Daily Pipeline Summary</h2>
                <p style="margin:8px 0 0; opacity:0.85;">{date_str} at {now_str}</p>
            </div>

            <div style="background:#f8f9fa; border-radius:8px; padding:20px;
                        margin-bottom:20px; text-align:center;">
                <div style="font-size:48px; font-weight:bold;
                            color:{status_color};">{total_count:,}</div>
                <div style="font-size:14px; color:#666; margin-top:4px;">
                    S3 writes today of {expected_count:,} expected
                </div>
                <div style="margin-top:12px;">
                    <span style="background:{status_color}; color:white;
                                 padding:4px 12px; border-radius:12px;
                                 font-size:12px; font-weight:bold;">
                        {status_label} — {coverage_pct}% coverage
                    </span>
                </div>
            </div>

            <table style="width:100%; border-collapse:collapse;
                          border:1px solid #dee2e6; border-radius:6px;
                          overflow:hidden; margin-bottom:20px;">
                <tr style="background:#f8f9fa;">
                    <td style="padding:12px 16px; font-weight:600;">Expected writes</td>
                    <td style="padding:12px 16px;">{expected_count:,}</td>
                </tr>
                <tr>
                    <td style="padding:12px 16px; font-weight:600;">Actual writes</td>
                    <td style="padding:12px 16px;">{total_count:,}</td>
                </tr>
                <tr style="background:#f8f9fa;">
                    <td style="padding:12px 16px; font-weight:600;">Missing writes</td>
                    <td style="padding:12px 16px;
                                color:{'#28a745' if missing < 100 else '#dc3545'};">
                        {missing:,}
                        {' (~' + str(round(missing * 15 / 60, 0)) + ' min of downtime)' if missing > 0 else ' — perfect coverage'}
                    </td>
                </tr>
                <tr>
                    <td style="padding:12px 16px; font-weight:600;">Coverage</td>
                    <td style="padding:12px 16px;">{coverage_pct}%</td>
                </tr>
                <tr style="background:#f8f9fa;">
                    <td style="padding:12px 16px; font-weight:600;">Device</td>
                    <td style="padding:12px 16px;">{config.DEVICE_ID}</td>
                </tr>
            </table>

            <p style="color:#999; font-size:0.75rem; margin-top:24px;">
                Sent by Smart Greenhouse Health Monitor — daily summary
            </p>
        </body></html>
        """

        msg            = MIMEMultipart("alternative")
        msg["Subject"] = f"Greenhouse Daily Summary — {total_count:,} writes ({coverage_pct}% coverage)"
        msg["From"]    = config.EMAIL_SENDER
        msg["To"]      = config.EMAIL_RECIPIENT
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(config.EMAIL_SENDER, config.EMAIL_APP_PASSWORD)
            server.sendmail(
                config.EMAIL_SENDER,
                config.EMAIL_RECIPIENT,
                msg.as_string()
            )
        logger.info(
            f"Daily summary sent — {total_count:,} writes "
            f"({coverage_pct}% coverage, {missing:,} missing)"
        )

    except Exception as e:
        logger.error(f"Failed to send daily summary: {e}", exc_info=True)

def main():
    logger.info(
        f"Starting pipeline health monitor — "
        f"checking every {CHECK_INTERVAL}s"
    )

    while True:
        try:
            logger.info("Running health checks...")
            results  = run_all_checks()
            failures = {
                name: result
                for name, result in results.items()
                if not result["healthy"]
            }

            if failures:
                # Only alert for components not recently alerted
                new_failures = {
                    name: result
                    for name, result in failures.items()
                    if should_alert(name)
                }
                if new_failures:
                    send_alert(new_failures)
                    for name in new_failures:
                        last_alerts[name] = time.time()
            else:
                logger.info("All components healthy")
                # Check for recovered components and send recovery notification
                recovered = {
                    name: {"healthy": True, "message": "Component recovered — all checks passing"}
                    for name in list(last_alerts.keys())
                    if name not in failures
                }

                if recovered:
                    send_recovery_email(recovered)
                    for name in recovered:
                        del last_alerts[name]
                        logger.info(f"[RECOVERED] {name} — alert state cleared, recovery email sent")

        except Exception as e:
            logger.error(f"Health check cycle failed: {e}", exc_info=True)

        # ── Daily summary at 11:55 PM ─────────────────────
            now_local = datetime.now()
            if (now_local.hour == 23 and now_local.minute == 55):
                if last_daily_summary is None or last_daily_summary.date() != now_local.date():
                    logger.info("Sending daily S3 write summary...")
                    send_daily_summary()
                    last_daily_summary = now_local
        logger.info(f"Next health check in {CHECK_INTERVAL}s")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
