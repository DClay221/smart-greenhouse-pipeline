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

        response = s3.list_objects_v2(
            Bucket=config.S3_BUCKET,
            Prefix=prefix,
            MaxKeys=1
        )

        objects = response.get("Contents", [])
        if not objects:
            return False, "No S3 writes found for today"

        # Get the most recent file
        response_full = s3.list_objects_v2(
            Bucket=config.S3_BUCKET,
            Prefix=prefix
        )
        all_objects = response_full.get("Contents", [])
        if not all_objects:
            return False, "No S3 objects found"

        latest = max(all_objects, key=lambda x: x["LastModified"])
        age    = (datetime.now(timezone.utc) - latest["LastModified"]).total_seconds()

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
                # Reset alert state for recovered components
                for name in list(last_alerts.keys()):
                    if name not in failures:
                        del last_alerts[name]
                        logger.info(f"[RECOVERED] {name} — alert state cleared")

        except Exception as e:
            logger.error(f"Health check cycle failed: {e}", exc_info=True)

        logger.info(f"Next health check in {CHECK_INTERVAL}s")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
