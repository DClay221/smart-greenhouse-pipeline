import json
import time
import logging
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import config

logger = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name=config.AWS_REGION)

# ── Actuator state definitions ────────────────────────────────
ACTUATOR_DEFINITIONS = {
    "cooling_fan": {
        "display_name":   "Cooling Fan",
        "trigger_sensor": "temperature",
        "trigger_status": "HIGH",
        "min_run_time":   config.MIN_ACTUATION_DURATION,
        "icon":           "🌀"
    },
    "heating_system": {
        "display_name":   "Heating System",
        "trigger_sensor": "temperature",
        "trigger_status": "LOW",
        "min_run_time":   config.MIN_ACTUATION_DURATION,
        "icon":           "🔥"
    },
    "dehumidifier": {
        "display_name":   "Dehumidifier",
        "trigger_sensor": "humidity",
        "trigger_status": "HIGH",
        "min_run_time":   config.MIN_ACTUATION_DURATION,
        "icon":           "💨"
    },
    "humidifier": {
        "display_name":   "Humidifier",
        "trigger_sensor": "humidity",
        "trigger_status": "LOW",
        "min_run_time":   config.MIN_ACTUATION_DURATION,
        "icon":           "💧"
    },
    "ventilation_fans": {
        "display_name":   "Ventilation Fans",
        "trigger_sensor": "co2",
        "trigger_status": "HIGH",
        "min_run_time":   config.MIN_ACTUATION_DURATION,
        "icon":           "🌬️"
    },
    "irrigation_pump": {
        "display_name":   "Irrigation Pump",
        "trigger_sensor": "soil_moisture",
        "trigger_status": "LOW",
        "min_run_time":   config.MIN_ACTUATION_DURATION,
        "icon":           "🪴"
    },
    "grow_lights": {
        "display_name":   "Grow Lights",
        "trigger_sensor": "light",
        "trigger_status": "LOW",
        "min_run_time":   config.MIN_ACTUATION_DURATION,
        "icon":           "💡"
    },
    "ph_down_pump": {
        "display_name":   "pH Down Pump",
        "trigger_sensor": "water_ph",
        "trigger_status": "HIGH",
        "min_run_time":   30,  # shorter run time for pH dosing
        "icon":           "🧪"
    },
    "ph_up_pump": {
        "display_name":   "pH Up Pump",
        "trigger_sensor": "water_ph",
        "trigger_status": "LOW",
        "min_run_time":   30,
        "icon":           "🧪"
    },
}

class ActuatorManager:
    """Manages the state of all greenhouse actuators."""

    def __init__(self):
        # State tracking per actuator
        self.states = {
            name: {
                "active":         False,
                "activated_at":   None,
                "deactivated_at": None,
                "total_cycles":   0,
                "total_run_time": 0.0,
            }
            for name in ACTUATOR_DEFINITIONS
        }
        logger.info(f"ActuatorManager initialized with {len(self.states)} actuators")
        self._save_state()
        logger.info("Initial actuator state written to disk")

    def _elapsed(self, actuator: str) -> float:
        """Return seconds since actuator was activated."""
        activated_at = self.states[actuator]["activated_at"]
        if activated_at is None:
            return 0.0
        return time.time() - activated_at

    def _can_deactivate(self, actuator: str) -> bool:
        """Check if minimum run time has elapsed."""
        defn    = ACTUATOR_DEFINITIONS[actuator]
        elapsed = self._elapsed(actuator)
        return elapsed >= defn["min_run_time"]

    def activate(self, actuator: str, reason: str) -> bool:
        """
        Attempt to activate an actuator.
        Returns True if newly activated, False if already active.
        """
        state = self.states[actuator]
        defn  = ACTUATOR_DEFINITIONS[actuator]

        if state["active"]:
            logger.info(
                f"[ACTUATOR] {defn['icon']} {defn['display_name']} already active "
                f"({self._elapsed(actuator):.0f}s elapsed)"
            )
            return False

        # Activate
        now                    = time.time()
        state["active"]        = True
        state["activated_at"]  = now
        state["total_cycles"] += 1

        logger.warning(
            f"[ACTUATOR] {defn['icon']} {defn['display_name']} ACTIVATED — {reason}"
        )
        self._log_event_to_s3(actuator, "ACTIVATED", reason)
        self._save_state()
        return True

    def deactivate(self, actuator: str, reason: str) -> bool:
        """
        Attempt to deactivate an actuator.
        Returns True if deactivated, False if minimum run time not elapsed.
        """
        state = self.states[actuator]
        defn  = ACTUATOR_DEFINITIONS[actuator]

        if not state["active"]:
            return False

        if not self._can_deactivate(actuator):
            remaining = defn["min_run_time"] - self._elapsed(actuator)
            logger.info(
                f"[HYSTERESIS] {defn['display_name']} — "
                f"minimum run time not elapsed ({remaining:.0f}s remaining)"
            )
            return False

        # Deactivate
        run_time                    = self._elapsed(actuator)
        state["active"]             = False
        state["deactivated_at"]     = time.time()
        state["total_run_time"]    += run_time

        logger.info(
            f"[ACTUATOR] {defn['icon']} {defn['display_name']} DEACTIVATED — "
            f"ran for {run_time:.0f}s — {reason}"
        )
        self._log_event_to_s3(actuator, "DEACTIVATED", reason, run_time=run_time)
        self._save_state()
        return True

    def evaluate(self, sensors: dict, consecutive_breaches: dict):
        """
        Evaluate all actuators against current sensor readings.
        Activates or deactivates each actuator as appropriate.
        """
        for actuator_name, defn in ACTUATOR_DEFINITIONS.items():
            sensor_key    = defn["trigger_sensor"]
            trigger_status = defn["trigger_status"]

            sensor_data = sensors.get(sensor_key, {})
            status      = sensor_data.get("status", "OK")
            value       = sensor_data.get("value",  "N/A")
            unit        = sensor_data.get("unit",   "")
            breaches    = consecutive_breaches.get(sensor_key, 0)

            should_be_active = (
                status == trigger_status and
                breaches >= config.ALERT_CONSECUTIVE_MIN
            )

            if should_be_active:
                self.activate(
                    actuator_name,
                    reason=f"{sensor_key} {status} ({value}{unit}, {breaches} consecutive readings)"
                )
            else:
                self.deactivate(
                    actuator_name,
                    reason=f"{sensor_key} returned to normal ({value}{unit})"
                )
        self._save_state()

    def get_state_snapshot(self) -> dict:
        """Return a clean snapshot of all actuator states for the API."""
        snapshot = {}
        for name, state in self.states.items():
            defn = ACTUATOR_DEFINITIONS[name]
            snapshot[name] = {
                "display_name":   defn["display_name"],
                "icon":           defn["icon"],
                "active":         state["active"],
                "elapsed_seconds": round(self._elapsed(name), 1) if state["active"] else 0,
                "total_cycles":   state["total_cycles"],
                "total_run_time": round(state["total_run_time"], 1),
                "trigger_sensor": defn["trigger_sensor"],
                "trigger_status": defn["trigger_status"],
            }
        return snapshot

    def _save_state(self):
        """Persist current actuator state to disk for API server to read."""
        try:
            snapshot = self.get_state_snapshot()
            actuator_list = [
                {"actuator_id": k, **v}
                for k, v in snapshot.items()
            ]
            with open(config.ACTUATOR_STATE_FILE, "w") as f:
                json.dump(actuator_list, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save actuator state: {e}")

    def _log_event_to_s3(
        self,
        actuator:  str,
        event:     str,
        reason:    str,
        run_time:  float = 0.0
    ):
        """Write actuation event to S3 for audit trail."""
        try:
            now       = datetime.now(timezone.utc)
            date_path = now.strftime("%Y/%m/%d")
            time_key  = now.strftime("%H%M%S%f")
            defn      = ACTUATOR_DEFINITIONS[actuator]

            record = {
                "event_type":    "actuation",
                "timestamp":     now.isoformat(),
                "device_id":     config.DEVICE_ID,
                "actuator":      actuator,
                "display_name":  defn["display_name"],
                "event":         event,
                "reason":        reason,
                "run_time_secs": run_time,
                "total_cycles":  self.states[actuator]["total_cycles"],
            }

            s3_key = (
                f"actuation_events/{date_path}/"
                f"{actuator}_{event}_{time_key}.json"
            )

            s3.put_object(
                Bucket=config.S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(record, indent=2),
                ContentType="application/json"
            )
            logger.info(f"Actuation event logged to S3: {s3_key}")

        except ClientError as e:
            logger.error(f"Failed to log actuation event to S3: {e}")
