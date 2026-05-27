import time
import json
import math
import random
from datetime import datetime, timezone
from awscrt import mqtt
from awsiot import mqtt_connection_builder
import sys
sys.path.insert(0, "/home/devynclaybrooks/greenhouse-project")
import config

def get_time_of_day_factor() -> float:
    """
    Returns a value between 0.0 and 1.0 representing time of day.
    0.0 = coldest point (5 AM), 1.0 = warmest point (2 PM).
    Uses a sinusoidal curve for natural transitions.
    """
    now        = datetime.now()
    hour       = now.hour + now.minute / 60.0
    # Peak at 14:00 (2 PM), trough at 5:00 (5 AM)
    peak_hour  = 14.0
    trough_hour = 5.0
    period     = 24.0
    # Shift so trough is at 0
    shifted    = (hour - trough_hour) % period
    factor     = (1 - math.cos(2 * math.pi * shifted / period)) / 2
    return factor

def get_light_factor() -> float:
    """
    Returns a value between 0.0 and 1.0 representing natural light.
    0.0 = full darkness, 1.0 = peak midday light.
    Sunrise ~6 AM, sunset ~8 PM.
    """
    now    = datetime.now()
    hour   = now.hour + now.minute / 60.0
    sunrise = 6.0
    sunset  = 20.0
    if hour < sunrise or hour > sunset:
        return 0.0
    # Sinusoidal curve peaking at solar noon (~1 PM)
    solar_noon = (sunrise + sunset) / 2
    factor     = math.sin(math.pi * (hour - sunrise) / (sunset - sunrise))
    return max(0.0, factor)

def get_target_values() -> dict:
    """
    Calculate realistic target values based on time of day.
    The drift algorithm will gradually move readings toward these targets.
    """
    tod    = get_time_of_day_factor()   # 0.0 to 1.0
    light  = get_light_factor()         # 0.0 to 1.0

    # Temperature: 18C overnight low, 28C afternoon high in greenhouse
    temp_target = 18.0 + (tod * 10.0)

    # Humidity: inversely correlated with temp
    # Higher overnight (70%), lower midday (50%)
    humidity_target = 70.0 - (tod * 20.0)

    # CO2: higher overnight (plants respire, no photosynthesis)
    # lower during day (photosynthesis consumes CO2)
    co2_target = 1100 - (light * 400)

    # Light: 0 at night, up to 1800 lux at peak midday
    light_target = light * 1800.0

    # Soil moisture: gentle random fluctuation around a stable midpoint
    # Not time-dependent — irrigation and consumption balance each other
    soil_target = 55.0 + random.uniform(-5.0, 5.0)

    # Water pH: very stable, slow random drift around ideal hydroponic range
    ph_target = 6.5 + random.uniform(-0.1, 0.1)

    return {
        "temperature":   temp_target,
        "humidity":      humidity_target,
        "co2_ppm":       co2_target,
        "light_lux":     light_target,
        "soil_moisture": soil_target,
        "water_ph":      ph_target,
    }

def simulate_reading(previous: dict) -> dict:
    """
    Generate a realistic sensor reading by drifting toward
    time-aware target values rather than random walking.
    """
    targets = get_target_values()

    def drift_toward(current, target, max_step, noise):
        """Move current value toward target with small random noise."""
        direction = target - current
        step      = min(abs(direction), max_step) * (1 if direction > 0 else -1)
        noise_val = random.uniform(-noise, noise)
        return round(current + step + noise_val, 2)

    reading = {
        "temperature":   drift_toward(
            previous["temperature"],
            targets["temperature"],
            max_step=0.3, noise=0.1
        ),
        "humidity":      drift_toward(
            previous["humidity"],
            targets["humidity"],
            max_step=0.5, noise=0.2
        ),
        "co2_ppm":       drift_toward(
            previous["co2_ppm"],
            targets["co2_ppm"],
            max_step=15,  noise=5
        ),
        "light_lux":     drift_toward(
            previous["light_lux"],
            targets["light_lux"],
            max_step=30,  noise=10
        ),
        "soil_moisture": drift_toward(
            previous["soil_moisture"],
            targets["soil_moisture"],
            max_step=0.3, noise=0.1
        ),
        "water_ph":      drift_toward(
            previous["water_ph"],
            targets["water_ph"],
            max_step=0.02, noise=0.01
        ),
    }

    # Clamp all values to physically possible ranges
    reading["temperature"]   = max(config.TEMP_POSSIBLE_MIN,
                               min(config.TEMP_POSSIBLE_MAX,
                               reading["temperature"]))
    reading["humidity"]      = max(config.HUMIDITY_POSSIBLE_MIN,
                               min(config.HUMIDITY_POSSIBLE_MAX,
                               reading["humidity"]))
    reading["co2_ppm"]       = max(config.CO2_POSSIBLE_MIN,
                               min(config.CO2_POSSIBLE_MAX,
                               reading["co2_ppm"]))
    reading["light_lux"]     = max(config.LIGHT_POSSIBLE_MIN,
                               min(config.LIGHT_POSSIBLE_MAX,
                               reading["light_lux"]))
    reading["soil_moisture"] = max(config.SOIL_POSSIBLE_MIN,
                               min(config.SOIL_POSSIBLE_MAX,
                               reading["soil_moisture"]))
    reading["water_ph"]      = max(config.PH_POSSIBLE_MIN,
                               min(config.PH_POSSIBLE_MAX,
                               reading["water_ph"]))

    return reading

def validate_reading(reading: dict) -> bool:
    """Reject physically impossible sensor readings."""
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
            "temperature":   {"value": reading["temperature"],
                              "unit": "celsius",  "status": alerts["temperature"]},
            "humidity":      {"value": reading["humidity"],
                              "unit": "percent",  "status": alerts["humidity"]},
            "co2":           {"value": reading["co2_ppm"],
                              "unit": "ppm",      "status": alerts["co2"]},
            "soil_moisture": {"value": reading["soil_moisture"],
                              "unit": "percent",  "status": alerts["soil_moisture"]},
            "light":         {"value": reading["light_lux"],
                              "unit": "lux",      "status": alerts["light"]},
            "water_ph":      {"value": reading["water_ph"],
                              "unit": "pH",       "status": alerts["water_ph"]},
        }
    }

def status_icon(status: str) -> str:
    return {"OK": "OK", "HIGH": "HIGH", "LOW": "LOW"}.get(status, "?")

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
    connected   = False
    retries     = 0
    max_retries = 10

    while not connected and retries < max_retries:
        try:
            connect_future = mqtt_connection.connect()
            connect_future.result()
            connected = True
            print(f"[INFO] Connected. Publishing every "
                  f"{config.PUBLISH_INTERVAL}s\n")
        except Exception as e:
            retries += 1
            wait = min(2 ** retries, 60)
            print(f"[WARN] Connection failed ({retries}/{max_retries}): "
                  f"{e}. Retrying in {wait}s...")
            time.sleep(wait)

    if not connected:
        print("[ERROR] Could not connect after maximum retries. Exiting.")
        return

    # ── Seed starting values based on current time of day ────
    targets = get_target_values()
    previous = {
        "temperature":   targets["temperature"],
        "humidity":      targets["humidity"],
        "co2_ppm":       targets["co2_ppm"],
        "light_lux":     targets["light_lux"],
        "soil_moisture": targets["soil_moisture"],
        "water_ph":      targets["water_ph"],
    }

    print(f"[INFO] Seeding initial values from time-of-day targets:")
    for k, v in previous.items():
        print(f"  {k}: {v}")
    print()

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
                f"  Temp:     {s['temperature']['value']:>7}C   "
                f"{status_icon(s['temperature']['status'])}\n"
                f"  Humidity: {s['humidity']['value']:>7}%    "
                f"{status_icon(s['humidity']['status'])}\n"
                f"  CO2:      {s['co2']['value']:>7}ppm  "
                f"{status_icon(s['co2']['status'])}\n"
                f"  Soil:     {s['soil_moisture']['value']:>7}%    "
                f"{status_icon(s['soil_moisture']['status'])}\n"
                f"  Light:    {s['light']['value']:>7}lux  "
                f"{status_icon(s['light']['status'])}\n"
                f"  pH:       {s['water_ph']['value']:>7}pH   "
                f"{status_icon(s['water_ph']['status'])}\n"
            )

            time.sleep(config.PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\n[INFO] Stopping simulator...")
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("[INFO] Disconnected cleanly.")

if __name__ == "__main__":
    main()
