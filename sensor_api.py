import json
import boto3
import logging
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from botocore.config import Config
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

s3 = boto3.client(
    "s3",
    region_name=config.AWS_REGION,
    config=Config(
        max_pool_connections=25
    )
)


def get_latest_readings(n: int = 200) -> list:
    """Fetch the n most recent sensor readings from today's S3 prefix only."""
    try:
        today  = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        prefix = f"{config.S3_PREFIX}/{today}/"

        logger.info(f"Listing objects under s3://{config.S3_BUCKET}/{prefix}")

        paginator = s3.get_paginator("list_objects_v2")
        objects   = []
        for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=prefix):
            objects.extend(page.get("Contents", []))

        if not objects:
            logger.warning(f"No objects found under {prefix}")
            return []

        objects.sort(key=lambda x: x["LastModified"], reverse=True)
        latest = objects[:n]

        readings = []
        for obj in latest:
            try:
                file_response = s3.get_object(
                    Bucket=config.S3_BUCKET,
                    Key=obj["Key"]
                )
                payload = json.loads(file_response["Body"].read().decode("utf-8"))
                sensors = payload.get("sensors", {})
                readings.append({
                    "timestamp":       payload.get("timestamp"),
                    "device_id":       payload.get("device_id"),
                    "temperature":     sensors.get("temperature",   {}).get("value"),
                    "humidity":        sensors.get("humidity",      {}).get("value"),
                    "co2_ppm":         sensors.get("co2",           {}).get("value"),
                    "soil_moisture":   sensors.get("soil_moisture", {}).get("value"),
                    "light_lux":       sensors.get("light",         {}).get("value"),
                    "water_ph":        sensors.get("water_ph",      {}).get("value"),
                    "temp_status":     sensors.get("temperature",   {}).get("status"),
                    "humidity_status": sensors.get("humidity",      {}).get("status"),
                    "co2_status":      sensors.get("co2",           {}).get("status"),
                    "soil_status":     sensors.get("soil_moisture", {}).get("status"),
                    "light_status":    sensors.get("light",         {}).get("status"),
                    "ph_status":       sensors.get("water_ph",      {}).get("status"),
                })
            except Exception as e:
                logger.error(f"Failed to read {obj['Key']}: {e}")
                continue

        readings.sort(key=lambda x: x["timestamp"])
        logger.info(
            f"Served {len(readings)} readings — "
            f"most recent: {readings[-1]['timestamp'] if readings else 'none'}"
        )
        return readings

    except ClientError as e:
        logger.error(f"S3 error: {e}")
        return []


def load_json_file(filepath: str, default):
    """Safely load a JSON file, returning default if unavailable."""
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        logger.error(f"Failed to read {filepath}: {e}")
        return default


class SensorAPIHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/sensors":
            readings = get_latest_readings(config.API_MAX_READINGS)
            self._send_json(readings)

        elif self.path == "/actuators":
            actuator_list = load_json_file(config.ACTUATOR_STATE_FILE, default=[])
            self._send_json(actuator_list)

        elif self.path == "/weather":
            weather_state = load_json_file(
                config.WEATHER_STATE_FILE,
                default={"error": "Weather data not yet available"}
            )
            self._send_json(weather_state)

        elif self.path == "/health":
            self._send_json({
                "status":    "healthy",
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

        else:
            self.send_response(404)
            self.end_headers()

    def _send_json(self, data):
        """Helper to send a JSON response with correct headers."""
        response = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format, *args):
        pass  # Suppress default HTTP server logs


class ThreadedHTTPServer(HTTPServer):
    """Handle each request in a separate thread to prevent blocking."""

    def process_request(self, request, client_address):
        thread = Thread(
            target=self.__new_request,
            args=(request, client_address)
        )
        thread.daemon = True
        thread.start()

    def __new_request(self, request, client_address):
        self.finish_request(request, client_address)
        self.shutdown_request(request)


def main():
    logger.info(f"Starting threaded Sensor API server on port {config.API_PORT}...")
    logger.info(f"Sensors endpoint:   http://localhost:{config.API_PORT}/sensors")
    logger.info(f"Health endpoint:    http://localhost:{config.API_PORT}/health")
    logger.info(f"Actuators endpoint: http://localhost:{config.API_PORT}/actuators")
    logger.info(f"Weather endpoint:   http://localhost:{config.API_PORT}/weather")
    server = ThreadedHTTPServer(("0.0.0.0", config.API_PORT), SensorAPIHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down API server...")
        server.server_close()
        logger.info("Server stopped cleanly.")


if __name__ == "__main__":
    main()
