import json
import time
import logging
from awscrt import mqtt
from awsiot import mqtt_connection_builder
from kafka import KafkaProducer
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def create_kafka_producer():
    retries = 0
    while retries < 10:
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                retry_backoff_ms=500
            )
            logger.info(f"Kafka producer connected to {config.KAFKA_BROKER}")
            return producer
        except Exception as e:
            retries += 1
            wait = min(2 ** retries, 60)
            logger.warning(f"Kafka connection failed ({retries}/10): {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Could not connect to Kafka after maximum retries.")

def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    try:
        message = json.loads(payload.decode("utf-8"))
        producer.send(config.KAFKA_TOPIC, value=message)
        producer.flush()
        logger.info(
            f"Forwarded message from {message.get('device_id','unknown')} "
            f"at {message.get('timestamp','unknown')}"
        )
    except Exception as e:
        logger.error(f"Failed to forward message: {e}", exc_info=True)

def on_connection_interrupted(connection, error, **kwargs):
    logger.warning(f"Connection interrupted: {error}")

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    logger.info(f"Connection resumed. Return code: {return_code}")

def main():
    global producer
    logger.info("Starting MQTT to Kafka bridge...")

    producer = create_kafka_producer()

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=config.IOT_ENDPOINT,
        cert_filepath=config.MAC_CERT_PATH,
        pri_key_filepath=config.MAC_KEY_PATH,
        ca_filepath=config.MAC_CA_PATH,
        client_id=config.MQTT_CLIENT_MAC,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        clean_session=False,
        keep_alive_secs=30
    )

    connected = False
    retries   = 0
    while not connected and retries < 10:
        try:
            mqtt_connection.connect().result()
            connected = True
            logger.info(f"Connected to AWS IoT Core. Subscribing to '{config.MQTT_TOPIC}'...")
        except Exception as e:
            retries += 1
            wait = min(2 ** retries, 60)
            logger.warning(f"IoT connection failed ({retries}/10): {e}. Retrying in {wait}s...")
            time.sleep(wait)

    if not connected:
        logger.error("Could not connect to IoT Core after maximum retries. Exiting.")
        return

    subscribe_future, _ = mqtt_connection.subscribe(
        topic=config.MQTT_TOPIC,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received
    )
    subscribe_future.result()
    logger.info("Bridge active — forwarding messages to Kafka. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down bridge...")
        producer.close()
        mqtt_connection.disconnect().result()
        logger.info("Bridge disconnected cleanly.")

if __name__ == "__main__":
    main()
