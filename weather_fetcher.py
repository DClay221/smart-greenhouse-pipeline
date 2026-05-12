import json
import time
import logging
import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL     = "https://api.openweathermap.org/data/2.5"
CURRENT_URL  = f"{BASE_URL}/weather"
FORECAST_URL = f"{BASE_URL}/forecast"

# ── Track briefing state ──────────────────────────────────────
last_morning_briefing = None
last_evening_briefing = None


def kelvin_to_celsius(k: float) -> float:
    return round(k - 273.15, 2)


def fetch_current_weather() -> dict:
    try:
        response = requests.get(
            CURRENT_URL,
            params={
                "zip":   f"{config.OPENWEATHER_ZIP},{config.OPENWEATHER_COUNTRY}",
                "appid": config.OPENWEATHER_API_KEY,
            },
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        return {
            "temperature": kelvin_to_celsius(data["main"]["temp"]),
            "feels_like":  kelvin_to_celsius(data["main"]["feels_like"]),
            "humidity":    data["main"]["humidity"],
            "pressure":    data["main"]["pressure"],
            "wind_speed":  data["wind"]["speed"],
            "description": data["weather"][0]["description"],
            "cloud_cover": data["clouds"]["all"],
        }
    except Exception as e:
        logger.error(f"Failed to fetch current weather: {e}")
        return {}


def fetch_forecast() -> list:
    try:
        response = requests.get(
            FORECAST_URL,
            params={
                "zip":   f"{config.OPENWEATHER_ZIP},{config.OPENWEATHER_COUNTRY}",
                "appid": config.OPENWEATHER_API_KEY,
                "cnt":   16,  # 16 x 3hr = 48 hours of data
            },
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        forecast = []
        for item in data["list"]:
            forecast.append({
                "timestamp":   item["dt_txt"],
                "temperature": kelvin_to_celsius(item["main"]["temp"]),
                "humidity":    item["main"]["humidity"],
                "rain_chance": item.get("pop", 0.0),
                "description": item["weather"][0]["description"],
            })
        return forecast
    except Exception as e:
        logger.error(f"Failed to fetch forecast: {e}")
        return []


def split_forecast_windows(forecast: list) -> tuple:
    """
    Split forecast into two operational windows:
    Window 1 — Today: now until 7 PM local
    Window 2 — Tonight/Tomorrow AM: 7 PM until 9 AM next day
    """
    now       = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    tomorrow  = (now + timedelta(days=1)).strftime("%Y-%m-%d")

    window_1 = []  # Now → 7 PM today
    window_2 = []  # 7 PM today → 9 AM tomorrow

    for f in forecast:
        ts   = datetime.strptime(f["timestamp"], "%Y-%m-%d %H:%M:%S")
        hour = ts.hour
        date = ts.strftime("%Y-%m-%d")

        if date == today_str and hour < config.TODAY_WINDOW_END_HOUR:
            window_1.append(f)
        elif (date == today_str and hour >= config.TODAY_WINDOW_END_HOUR) or \
             (date == tomorrow  and hour < config.TOMORROW_WINDOW_END_HOUR):
            window_2.append(f)

    return window_1, window_2


def evaluate_window(window: list, label: str) -> list:
    """Evaluate a forecast window and return tiered actions."""
    if not window:
        return []

    actions = []

    max_temp      = max(f["temperature"] for f in window)
    min_temp      = min(f["temperature"] for f in window)
    max_rain      = max(f["rain_chance"] for f in window)
    avg_humidity  = sum(f["humidity"] for f in window) / len(window)

    # ── Temperature high ──────────────────────────────────────
    if max_temp >= config.PROACTIVE_COOL_TEMP + 3:
        actions.append({
            "type":    "PROACTIVE_COOL",
            "urgency": "URGENT",
            "window":  label,
            "reason":  f"Forecast high of {max_temp}°C — immediate pre-cooling required"
        })
    elif max_temp >= config.PROACTIVE_COOL_TEMP:
        actions.append({
            "type":    "PROACTIVE_COOL",
            "urgency": "ADVISORY",
            "window":  label,
            "reason":  f"Forecast high of {max_temp}°C — pre-cooling recommended"
        })

    # ── Temperature low ───────────────────────────────────────
    if min_temp <= config.PROACTIVE_HEAT_TEMP - 3:
        actions.append({
            "type":    "PROACTIVE_HEAT",
            "urgency": "URGENT",
            "window":  label,
            "reason":  f"Forecast low of {min_temp}°C — immediate pre-heating required"
        })
    elif min_temp <= config.PROACTIVE_HEAT_TEMP:
        actions.append({
            "type":    "PROACTIVE_HEAT",
            "urgency": "ADVISORY",
            "window":  label,
            "reason":  f"Forecast low of {min_temp}°C — pre-heating recommended"
        })

    # ── Rain ──────────────────────────────────────────────────
    if max_rain >= config.PROACTIVE_RAIN_CHANCE:
        actions.append({
            "type":    "REDUCE_IRRIGATION",
            "urgency": "ROUTINE",
            "window":  label,
            "reason":  f"Rain probability {max_rain*100:.0f}% — reduce irrigation"
        })

    # ── Humidity ─────────────────────────────────────────────
    if avg_humidity > 85:
        actions.append({
            "type":    "HIGH_HUMIDITY_WATCH",
            "urgency": "ADVISORY",
            "window":  label,
            "reason":  f"Average outdoor humidity {avg_humidity:.0f}% — monitor for mold risk"
        })

    return actions


def send_email(subject: str, html_body: str):
    """Send an HTML formatted email via Gmail SMTP."""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = config.EMAIL_SENDER
        msg["To"]      = config.EMAIL_RECIPIENT
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(config.EMAIL_SENDER, config.EMAIL_APP_PASSWORD)
            server.sendmail(
                config.EMAIL_SENDER,
                config.EMAIL_RECIPIENT,
                msg.as_string()
            )
        logger.info(f"Email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}", exc_info=True)


def urgency_color(urgency: str) -> str:
    return {
        "URGENT":   "#dc3545",
        "ADVISORY": "#fd7e14",
        "ROUTINE":  "#28a745",
    }.get(urgency, "#6c757d")


def build_window_html(label: str, window: list, actions: list) -> str:
    """Build HTML for a single forecast window."""
    if not window:
        return f"<p><em>No forecast data available for {label}.</em></p>"

    max_temp = max(f["temperature"] for f in window)
    min_temp = min(f["temperature"] for f in window)
    max_rain = max(f["rain_chance"] for f in window)

    rows = ""
    for f in window:
        rows += f"""
        <tr>
            <td style="padding:4px 8px;">{f['timestamp']}</td>
            <td style="padding:4px 8px;">{f['temperature']}°C</td>
            <td style="padding:4px 8px;">{f['humidity']}%</td>
            <td style="padding:4px 8px;">{f['rain_chance']*100:.0f}%</td>
            <td style="padding:4px 8px;">{f['description'].title()}</td>
        </tr>"""

    action_html = ""
    for a in actions:
        color = urgency_color(a["urgency"])
        action_html += f"""
        <div style="margin:6px 0; padding:8px 12px; border-left:4px solid {color};
                    background:#f8f9fa; border-radius:4px;">
            <strong style="color:{color};">[{a['urgency']}]</strong>
            {a['type'].replace('_', ' ').title()} — {a['reason']}
        </div>"""

    if not action_html:
        action_html = "<p style='color:#28a745;'>✅ No proactive actions required.</p>"

    return f"""
    <h3 style="color:#2d6a4f; border-bottom:2px solid #2d6a4f; padding-bottom:4px;">
        {label}
    </h3>
    <p>
        🌡️ High: <strong>{max_temp}°C</strong> &nbsp;|&nbsp;
        🌡️ Low: <strong>{min_temp}°C</strong> &nbsp;|&nbsp;
        🌧️ Max Rain: <strong>{max_rain*100:.0f}%</strong>
    </p>
    <table style="border-collapse:collapse; width:100%; font-size:13px;">
        <thead>
            <tr style="background:#2d6a4f; color:white;">
                <th style="padding:6px 8px; text-align:left;">Time</th>
                <th style="padding:6px 8px; text-align:left;">Temp</th>
                <th style="padding:6px 8px; text-align:left;">Humidity</th>
                <th style="padding:6px 8px; text-align:left;">Rain</th>
                <th style="padding:6px 8px; text-align:left;">Conditions</th>
            </tr>
        </thead>
        <tbody>{rows}</tbody>
    </table>
    <h4 style="margin-top:12px;">Recommended Actions:</h4>
    {action_html}
    """


def build_briefing_email(
    briefing_type: str,
    current: dict,
    window_1: list,
    window_2: list,
    actions_1: list,
    actions_2: list
) -> str:
    """Build the full HTML briefing email."""
    now          = datetime.now().strftime("%A, %B %d %Y at %I:%M %p")
    urgent_count = sum(1 for a in actions_1 + actions_2 if a["urgency"] == "URGENT")
    total_count  = len(actions_1) + len(actions_2)

    urgent_banner = ""
    if urgent_count > 0:
        urgent_banner = f"""
        <div style="background:#dc3545; color:white; padding:12px;
                    border-radius:6px; margin-bottom:16px; font-weight:bold;">
            ⚠️ {urgent_count} URGENT action(s) require immediate attention
        </div>"""

    window_1_html = build_window_html(
        "🌅 Today (Now → 7 PM)", window_1, actions_1
    )
    window_2_html = build_window_html(
        "🌙 Tonight & Tomorrow Morning (7 PM → 9 AM)", window_2, actions_2
    )

    return f"""
    <html><body style="font-family:Arial,sans-serif; max-width:700px;
                       margin:auto; padding:20px; color:#333;">

        <div style="background:#2d6a4f; color:white; padding:20px;
                    border-radius:8px; margin-bottom:20px;">
            <h1 style="margin:0;">🌿 Smart Greenhouse</h1>
            <h2 style="margin:4px 0 0;">{briefing_type}</h2>
            <p style="margin:8px 0 0; opacity:0.85;">{now}</p>
        </div>

        {urgent_banner}

        <div style="background:#f8f9fa; padding:16px; border-radius:8px;
                    margin-bottom:20px;">
            <h3 style="margin-top:0;">☀️ Current Outdoor Conditions</h3>
            <table style="width:100%;">
                <tr>
                    <td>🌡️ Temperature</td>
                    <td><strong>{current.get('temperature')}°C</strong>
                        (feels like {current.get('feels_like')}°C)</td>
                </tr>
                <tr>
                    <td>💧 Humidity</td>
                    <td><strong>{current.get('humidity')}%</strong></td>
                </tr>
                <tr>
                    <td>💨 Wind</td>
                    <td><strong>{current.get('wind_speed')} m/s</strong></td>
                </tr>
                <tr>
                    <td>☁️ Conditions</td>
                    <td><strong>{current.get('description','').title()}</strong></td>
                </tr>
            </table>
        </div>

        {window_1_html}
        <br>
        {window_2_html}

        <div style="margin-top:24px; padding:12px; background:#e9ecef;
                    border-radius:6px; font-size:12px; color:#666;">
            <strong>Summary:</strong> {total_count} proactive action(s) identified
            ({urgent_count} urgent) &nbsp;|&nbsp;
            Device: {config.DEVICE_ID} &nbsp;|&nbsp;
            Location: ZIP {config.OPENWEATHER_ZIP}
        </div>

    </body></html>
    """


def send_urgent_alerts(actions: list, current: dict):
    """Send immediate email for any urgent actions."""
    urgent = [a for a in actions if a["urgency"] == "URGENT"]
    if not urgent:
        return

    items = "".join(
        f"<li style='margin:8px 0;'>"
        f"<strong>[{a['window']}]</strong> {a['type'].replace('_',' ').title()}"
        f" — {a['reason']}</li>"
        for a in urgent
    )

    html = f"""
    <html><body style="font-family:Arial,sans-serif; max-width:600px;
                       margin:auto; padding:20px;">
        <div style="background:#dc3545; color:white; padding:16px;
                    border-radius:8px; margin-bottom:16px;">
            <h2 style="margin:0;">⚠️ Greenhouse Urgent Weather Alert</h2>
        </div>
        <p>The following urgent conditions have been detected for
           <strong>ZIP {config.OPENWEATHER_ZIP}</strong>:</p>
        <ul>{items}</ul>
        <p>Current outdoor temperature:
           <strong>{current.get('temperature')}°C</strong>,
           {current.get('description','')}</p>
        <p style="color:#666; font-size:12px;">
            Sent by Smart Greenhouse Pipeline — {config.DEVICE_ID}
        </p>
    </body></html>
    """

    send_email(
        subject=f"⚠️ Greenhouse Urgent Alert — {len(urgent)} condition(s)",
        html_body=html
    )


def should_send_briefing(briefing_hour: int, last_sent: datetime) -> bool:
    """Check if it's time to send a scheduled briefing."""
    now = datetime.now()
    if now.hour != briefing_hour:
        return False
    if last_sent and last_sent.date() == now.date():
        return False
    return True


def save_weather_state(
    current: dict,
    forecast: list,
    window_1: list,
    window_2: list,
    actions_1: list,
    actions_2: list
):
    """Write full weather state to disk for API and processor."""
    state = {
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "location":          f"{config.OPENWEATHER_ZIP}, {config.OPENWEATHER_COUNTRY.upper()}",
        "current":           current,
        "forecast_24h":      forecast,
        "window_today":      window_1,
        "window_overnight":  window_2,
        "actions_today":     actions_1,
        "actions_overnight": actions_2,
        "proactive_actions": actions_1 + actions_2,
        "summary": {
            "outdoor_temp":      current.get("temperature"),
            "outdoor_humidity":  current.get("humidity"),
            "description":       current.get("description"),
            "urgent_count":      sum(1 for a in actions_1 + actions_2
                                     if a["urgency"] == "URGENT"),
            "total_actions":     len(actions_1) + len(actions_2),
        }
    }
    try:
        with open(config.WEATHER_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        logger.info(
            f"Weather state saved — "
            f"{current.get('temperature')}°C, "
            f"{current.get('description')} — "
            f"{state['summary']['total_actions']} action(s) "
            f"({state['summary']['urgent_count']} urgent)"
        )
    except Exception as e:
        logger.error(f"Failed to save weather state: {e}")


def main():
    global last_morning_briefing, last_evening_briefing

    logger.info(
        f"Starting tiered weather fetcher for ZIP {config.OPENWEATHER_ZIP} "
        f"— polling every {config.OPENWEATHER_INTERVAL}s"
    )
    logger.info(
        f"Briefings scheduled: "
        f"Morning {config.BRIEFING_MORNING_HOUR}:00, "
        f"Evening {config.BRIEFING_EVENING_HOUR}:00"
    )

    while True:
        try:
            logger.info("Fetching current weather and forecast...")
            current  = fetch_current_weather()
            forecast = fetch_forecast()

            if current and forecast:
                window_1, window_2 = split_forecast_windows(forecast)
                actions_1 = evaluate_window(window_1, "Today (Now → 7 PM)")
                actions_2 = evaluate_window(window_2, "Tonight & Tomorrow AM")

                save_weather_state(
                    current, forecast,
                    window_1, window_2,
                    actions_1, actions_2
                )

                # ── Urgent alerts — send immediately ──────────
                all_actions = actions_1 + actions_2
                urgent = [a for a in all_actions if a["urgency"] == "URGENT"]
                if urgent:
                    logger.warning(
                        f"[URGENT] {len(urgent)} urgent action(s) detected — "
                        f"sending immediate alert"
                    )
                    send_urgent_alerts(all_actions, current)

                # ── Log all actions ───────────────────────────
                for action in all_actions:
                    logger.info(
                        f"[{action['urgency']}] [{action['window']}] "
                        f"{action['type']}: {action['reason']}"
                    )

                # ── Morning briefing — 7 AM ───────────────────
                if should_send_briefing(
                    config.BRIEFING_MORNING_HOUR,
                    last_morning_briefing
                ):
                    logger.info("Sending morning briefing email...")
                    html = build_briefing_email(
                        "☀️ Morning Briefing",
                        current, window_1, window_2,
                        actions_1, actions_2
                    )
                    send_email(
                        subject="🌿 Greenhouse Morning Briefing — "
                                f"{datetime.now().strftime('%A, %B %d')}",
                        html_body=html
                    )
                    last_morning_briefing = datetime.now()

                # ── Evening briefing — 7 PM ───────────────────
                if should_send_briefing(
                    config.BRIEFING_EVENING_HOUR,
                    last_evening_briefing
                ):
                    logger.info("Sending evening briefing email...")
                    html = build_briefing_email(
                        "🌙 Evening Briefing",
                        current, window_1, window_2,
                        actions_1, actions_2
                    )
                    send_email(
                        subject="🌿 Greenhouse Evening Briefing — "
                                f"{datetime.now().strftime('%A, %B %d')}",
                        html_body=html
                    )
                    last_evening_briefing = datetime.now()

            else:
                logger.warning("Incomplete weather data — skipping cycle")

        except Exception as e:
            logger.error(f"Weather fetch cycle failed: {e}", exc_info=True)

        logger.info(f"Next weather fetch in {config.OPENWEATHER_INTERVAL}s")
        time.sleep(config.OPENWEATHER_INTERVAL)


if __name__ == "__main__":
    main()