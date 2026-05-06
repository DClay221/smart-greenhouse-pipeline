import json
import time
import logging
import boto3
from actuator_manager import ActuatorManager
from collections import defaultdict
from datetime import datetime, timezone
from kafka import KafkaConsumer
from botocore.exceptions import ClientError
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── AWS Clients ───────────────────────────────────────────────
sns = boto3.client("sns", region_name=config.AWS_REGION)
s3  = boto3.client("s3",  region_name=config.AWS_REGION)

# ── Actuation responses ───────────────────────────────────────
ACTUATION_RESPONSES = {
    "temperature":   {"HIGH": "🌡️  ACTION: Activate cooling system.",
                      "LOW":  "🌡️  ACTION: Activate heating system."},
    "humidity":      {"HIGH": "💧 ACTION: Activate dehumidifier.",
                      "LOW":  "💧 ACTION: Activate humidifier."},
    "co2":           {"HIGH": "💨 ACTION: Activate ventilation fans."},
    "soil_moisture": {"LOW":  "🪴 ACTION: Activate irrigation system."},
    "light":         {"LOW":  "💡 ACTION: Activate grow lights."},
    "water_ph":      {"HIGH": "🧪 ACTION: Dispense pH-down solution.",
                      "LOW":  "🧪 ACTION: Dispense pH-up solution."}
}

# ── State tracking for hysteresis and debouncing ──────────────
consecutive_breaches = defaultdict(int)   # sensor -> breach count
actuator_manager = ActuatorManager()                # actuator -> activation timestamp

def validate_reading(sensors: dict) -> bool:
    """Reject physically impossible sensor readings."""
    try:
        checks = [
            config.TEMP_POSSIBLE_MIN     <= sensors["temperature"]["value"]   <= config.TEMP_POSSIBLE_MAX,
            config.HUMIDITY_POSSIBLE_MIN <= sensors["humidity"]["value"]      <= config.HUMIDITY_POSSIBLE_MAX,
            config.CO2_POSSIBLE_MIN      <= sensors["co2"]["value"]           <= config.CO2_POSSIBLE_MAX,
            config.SOIL_POSSIBLE_MIN     <= sensors["soil_moisture"]["value"] <= config.SOIL_POSSIBLE_MAX,
            config.LIGHT_POSSIBLE_MIN    <= sensors["light"]["value"]         <= config.LIGHT_POSSIBLE_MAX,
            config.PH_POSSIBLE_MIN       <= sensors["water_ph"]["value"]      <= config.PH_POSSIBLE_MAX,
        ]
        return all(checks)
    except (KeyError, TypeError):
        return False

def evaluate_sensors(sensors: dict) -> list:
    """Evaluate sensors with consecutive breach counting."""
    responses = []
    for sensor_name, data in sensors.items():
        status = data.get("status", "OK")
        if status != "OK":
            consecutive_breaches[sensor_name] += 1
        else:
            consecutive_breaches[sensor_name] = 0

        if consecutive_breaches[sensor_name] >= config.ALERT_CONSECUTIVE_MIN:
            action = ACTUATION_RESPONSES.get(sensor_name, {}).get(status)
            if action:
                responses.append({
                    "sensor":       sensor_name,
                    "value":        data.get("value"),
                    "unit":         data.get("unit"),
                    "status":       status,
                    "action":       action,
                    "breach_count": consecutive_breaches[sensor_name]
                })
    return responses

def build_alert_message(device_id: str, timestamp: str, responses: list) -> str:
    lines = [
        f"🌿 SMART GREENHOUSE ALERT",
        f"Device:    {device_id}",
        f"Timestamp: {timestamp}",
        f"",
        f"The following conditions require attention:",
        f"{'─' * 45}"
    ]
    for r in responses:
        lines.append(f"Sensor:       {r['sensor'].replace('_', ' ').title()}")
        lines.append(f"Reading:      {r['value']} {r['unit']}")
        lines.append(f"Status:       {r['status']}")
        lines.append(f"Consecutive:  {r['breach_count']} readings")
        lines.append(f"{r['action']}")
        lines.append(f"{'─' * 45}")
    return "\n".join(lines)

def send_sns_alert(device_id: str, timestamp: str, responses: list):
    try:
        # sns.publish(  # uncomment when SNS free tier resets in April
        #     TopicArn=config.SNS_TOPIC_ARN,
        #     Subject=f"🌿 Greenhouse Alert — {len(responses)} condition(s) detected",
        #     Message=build_alert_message(device_id, timestamp, responses)
        # )
        logger.info(f"[SNS] Alert ready to send for {len(responses)} condition(s) — currently paused (free tier)")
    except ClientError as e:
        logger.error(f"Failed to send SNS alert: {e}", exc_info=True)

def write_to_s3(payload: dict, timestamp: str):
    try:
        dt        = datetime.fromisoformat(timestamp)
        date_path = dt.strftime("%Y/%m/%d")
        time_key  = dt.strftime("%H%M%S%f")
        device_id = payload.get("device_id", "unknown")
        s3_key    = f"{config.S3_PREFIX}/{date_path}/{device_id}_{time_key}.json"

        s3.put_object(
            Bucket=config.S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json"
        )
        logger.info(f"Written to S3: s3://{config.S3_BUCKET}/{s3_key}")
    except ClientError as e:
        logger.error(f"Failed to write to S3: {e}", exc_info=True)

def process_message(payload: dict):
    device_id = payload.get("device_id", "unknown")
    timestamp = payload.get("timestamp", "unknown")
    sensors   = payload.get("sensors",   {})

    if not validate_reading(sensors):
        logger.warning(f"[VALIDATION] Rejected invalid reading from {device_id} at {timestamp}")
        return

    logger.info(f"Processing reading from {device_id} at {timestamp}")

    write_to_s3(payload, timestamp)

    responses = evaluate_sensors(sensors)
    actuator_manager.evaluate(sensors, consecutive_breaches)

    if responses:
        logger.warning(
            f"⚠️  {len(responses)} alert(s) for {device_id}: "
            f"{[r['sensor'] for r in responses]}"
        )
        send_sns_alert(device_id, timestamp, responses)
    else:
        logger.info(f"All sensors nominal for {device_id}")

def main():
    logger.info(f"Starting Kafka consumer on topic '{config.KAFKA_TOPIC}'...")

    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BROKER,
        group_id=config.KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    logger.info("Consumer active — waiting for messages. Press Ctrl+C to stop.\n")

    try:
        for message in consumer:
            try:
                process_message(message.value)
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                continue
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed cleanly.")

if __name__ == "__main__":
    main()
