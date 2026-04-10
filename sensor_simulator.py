import time
import json
import random
from datetime import datetime, timezone
from awscrt import mqtt
from awsiot import mqtt_connection_builder
import sys
sys.path.insert(0, "/home/devynclaybrooks/greenhouse-project")
import config

def simulate_reading(prev: dict) -> dict:
    """Generate a realistic sensor reading with gradual drift."""
    def drift(val, lo, hi, delta):
        return round(max(lo, min(hi, val + random.uniform(-delta, delta))), 2)

    return {
        "temperature":   drift(prev["temperature"],   config.TEMP_MIN,     config.TEMP_MAX,     0.5),
        "humidity":      drift(prev["humidity"],       config.HUMIDITY_MIN, config.HUMIDITY_MAX, 1.0),
        "co2_ppm":       drift(prev["co2_ppm"],        config.CO2_MIN,      config.CO2_MAX,      20),
        "soil_moisture": drift(prev["soil_moisture"],  config.SOIL_MIN,     config.SOIL_MAX,     1.5),
        "light_lux":     drift(prev["light_lux"],      config.LIGHT_MIN,    config.LIGHT_MAX,    50.0),
        "water_ph":      drift(prev["water_ph"],       config.PH_MIN,       config.PH_MAX,       0.05),
    }

def validate_reading(reading: dict) -> bool:
    """Reject physically impossible readings."""
    checks = [
        config.TEMP_POSSIBLE_MIN     <= reading["temperature"]   <= config.TEMP_POSSIBLE_MAX,
        config.HUMIDITY_POSSIBLE_MIN <= reading["humidity"]      <= config.HUMIDITY_POSSIBLE_MAX,
        config.CO2_POSSIBLE_MIN      <= reading["co2_ppm"]       <= config.CO2_POSSIBLE_MAX,
        config.SOIL_POSSIBLE_MIN     <= reading["soil_moisture"] <= config.SOIL_POSSIBLE_MAX,
        config.LIGHT_POSSIBLE_MIN    <= reading["light_lux"]     <= config.LIGHT_POSSIBLE_MAX,
        config.PH_POSSIBLE_MIN       <= reading["water_ph"]      <= config.PH_POSSIBLE_MAX,
    ]
    return all(checks)

def get_alerts(r: dict) -> dict:
    return {
        "temperature":   "HIGH" if r["temperature"]   > config.TEMP_HIGH
                    else "LOW"  if r["temperature"]   < config.TEMP_LOW
                    else "OK",
        "humidity":      "HIGH" if r["humidity"]      > config.HUMIDITY_HIGH
                    else "LOW"  if r["humidity"]      < config.HUMIDITY_LOW
                    else "OK",
        "co2":           "HIGH" if r["co2_ppm"]       > config.CO2_HIGH
                    else "OK",
        "soil_moisture": "LOW"  if r["soil_moisture"] < config.SOIL_LOW
                    else "OK",
        "light":         "LOW"  if r["light_lux"]     < config.LIGHT_LOW
                    else "OK",
        "water_ph":      "HIGH" if r["water_ph"]      > config.PH_HIGH
                    else "LOW"  if r["water_ph"]      < config.PH_LOW
                    else "OK",
    }

def build_payload(reading: dict, alerts: dict) -> dict:
    return {
        "device_id": config.MQTT_CLIENT_PI,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sensors": {
            "temperature":   {"value": reading["temperature"],   "unit": "celsius", "status": alerts["temperature"]},
            "humidity":      {"value": reading["humidity"],      "unit": "percent", "status": alerts["humidity"]},
            "co2":           {"value": reading["co2_ppm"],       "unit": "ppm",     "status": alerts["co2"]},
            "soil_moisture": {"value": reading["soil_moisture"], "unit": "percent", "status": alerts["soil_moisture"]},
            "light":         {"value": reading["light_lux"],     "unit": "lux",     "status": alerts["light"]},
            "water_ph":      {"value": reading["water_ph"],      "unit": "pH",      "status": alerts["water_ph"]},
        }
    }

def status_icon(status: str) -> str:
    return {"OK": "✅", "HIGH": "🔴", "LOW": "🟡"}.get(status, "❓")

def on_connection_interrupted(connection, error, **kwargs):
    print(f"[WARN] Connection interrupted: {error}")

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print(f"[INFO] Connection resumed. Return code: {return_code}")

def main():
    print("[INFO] Connecting to AWS IoT Core...")

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=config.IOT_ENDPOINT,
        cert_filepath=config.CERT_PATH,
        pri_key_filepath=config.KEY_PATH,
        ca_filepath=config.CA_PATH,
        client_id=config.MQTT_CLIENT_PI,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        clean_session=False,
        keep_alive_secs=30
    )

    # ── Reconnection loop ─────────────────────────────────────
    connected = False
    retries   = 0
    max_retries = 10

    while not connected and retries < max_retries:
        try:
            connect_future = mqtt_connection.connect()
            connect_future.result()
            connected = True
            print(f"[INFO] Connected. Publishing every {config.PUBLISH_INTERVAL}s\n")
        except Exception as e:
            retries += 1
            wait = min(2 ** retries, 60)
            print(f"[WARN] Connection failed ({retries}/{max_retries}): {e}. Retrying in {wait}s...")
            time.sleep(wait)

    if not connected:
        print("[ERROR] Could not connect after maximum retries. Exiting.")
        return

    previous = {
        "temperature":   24.0,
        "humidity":      60.0,
        "co2_ppm":       800,
        "soil_moisture": 55.0,
        "light_lux":     800.0,
        "water_ph":      6.5,
    }

    try:
        while True:
            reading = simulate_reading(previous)

            if not validate_reading(reading):
                print(f"[WARN] Invalid reading rejected: {reading}")
                continue

            alerts  = get_alerts(reading)
            payload = build_payload(reading, alerts)
            previous = reading

            try:
                mqtt_connection.publish(
                    topic=config.MQTT_TOPIC,
                    payload=json.dumps(payload),
                    qos=mqtt.QoS.AT_LEAST_ONCE
                )
            except Exception as e:
                print(f"[ERROR] Failed to publish: {e}")

            s = payload["sensors"]
            print(
                f"[{payload['timestamp']}]\n"
                f"  Temp:     {s['temperature']['value']:>7}°C   {status_icon(s['temperature']['status'])} {s['temperature']['status']}\n"
                f"  Humidity: {s['humidity']['value']:>7}%    {status_icon(s['humidity']['status'])} {s['humidity']['status']}\n"
                f"  CO2:      {s['co2']['value']:>7}ppm  {status_icon(s['co2']['status'])} {s['co2']['status']}\n"
                f"  Soil:     {s['soil_moisture']['value']:>7}%    {status_icon(s['soil_moisture']['status'])} {s['soil_moisture']['status']}\n"
                f"  Light:    {s['light']['value']:>7}lux  {status_icon(s['light']['status'])} {s['light']['status']}\n"
                f"  pH:       {s['water_ph']['value']:>7}pH   {status_icon(s['water_ph']['status'])} {s['water_ph']['status']}\n"
            )

            time.sleep(config.PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\n[INFO] Stopping simulator...")
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("[INFO] Disconnected cleanly.")

if __name__ == "__main__":
    main()
