# ═══════════════════════════════════════════════════════════════
# Smart Greenhouse — Central Configuration
# ═══════════════════════════════════════════════════════════════

# ── Device ────────────────────────────────────────────────────
DEVICE_ID        = "SmartGreenhouse-Pi"
LOCATION         = "greenhouse_01"

# ── AWS ───────────────────────────────────────────────────────
AWS_REGION       = "us-east-1"
IOT_ENDPOINT     = "alxzps1w4o7kt-ats.iot.us-east-1.amazonaws.com"
S3_BUCKET        = "greenhouse-data-667186907481"
S3_PREFIX        = "raw/sensors"
SNS_TOPIC_ARN    = "arn:aws:sns:us-east-1:667186907481:greenhouse-alerts"

# ── IoT Core Certificate Paths (Pi) ───────────────────────────
CERT_PATH        = "/home/devynclaybrooks/greenhouse-project/certs/greenhouse-cert.pem"
KEY_PATH         = "/home/devynclaybrooks/greenhouse-project/certs/greenhouse-private.key"
CA_PATH          = "/home/devynclaybrooks/greenhouse-project/certs/AmazonRootCA1.pem"

# ── IoT Core Certificate Paths (Mac bridge) ───────────────────
MAC_CERT_PATH    = "/Users/devynclaybrooks/greenhouse-project/certs/greenhouse-cert.pem"
MAC_KEY_PATH     = "/Users/devynclaybrooks/greenhouse-project/certs/greenhouse-private.key"
MAC_CA_PATH      = "/Users/devynclaybrooks/greenhouse-project/certs/AmazonRootCA1.pem"

# ── MQTT ──────────────────────────────────────────────────────
MQTT_TOPIC       = "greenhouse/sensors"
MQTT_CLIENT_PI   = "SmartGreenhouse-Pi"
MQTT_CLIENT_MAC  = "GreenhouseBridge-Mac"

# ── Kafka ─────────────────────────────────────────────────────
KAFKA_BROKER     = "localhost:9092"
KAFKA_TOPIC      = "greenhouse-sensors"
KAFKA_GROUP_ID   = "greenhouse-processors"

# ── API Server ────────────────────────────────────────────────
API_PORT         = 8080
API_MAX_READINGS = 200

# ── Publish interval ──────────────────────────────────────────
PUBLISH_INTERVAL = 15   # seconds 

# ── Sensor simulation ranges ──────────────────────────────────
TEMP_MIN,         TEMP_MAX         = 18.0,  35.0   # Celsius
HUMIDITY_MIN,     HUMIDITY_MAX     = 40.0,  90.0   # Percent
CO2_MIN,          CO2_MAX          = 400,   2000   # PPM
SOIL_MIN,         SOIL_MAX         = 10.0,  100.0  # Percent
LIGHT_MIN,        LIGHT_MAX        = 0.0,   2000.0 # Lux
PH_MIN,           PH_MAX           = 5.0,   8.0    # pH

# ── Alert thresholds ──────────────────────────────────────────
TEMP_HIGH         = 30.0   # °C
TEMP_LOW          = 18.0   # °C
HUMIDITY_HIGH     = 80.0   # %
HUMIDITY_LOW      = 40.0   # %
CO2_HIGH          = 1500   # ppm
SOIL_LOW          = 20.0   # %
LIGHT_LOW         = 200.0  # lux
PH_HIGH           = 7.0    # pH
PH_LOW            = 5.5    # pH

# ── Data quality bounds (physically possible limits) ──────────
TEMP_POSSIBLE_MIN,     TEMP_POSSIBLE_MAX     = -10.0, 60.0
HUMIDITY_POSSIBLE_MIN, HUMIDITY_POSSIBLE_MAX =   0.0, 100.0
CO2_POSSIBLE_MIN,      CO2_POSSIBLE_MAX      = 300,   5000
SOIL_POSSIBLE_MIN,     SOIL_POSSIBLE_MAX     =   0.0, 100.0
LIGHT_POSSIBLE_MIN,    LIGHT_POSSIBLE_MAX    =   0.0, 5000.0
PH_POSSIBLE_MIN,       PH_POSSIBLE_MAX       =   0.0, 14.0

# ── Hysteresis / actuation settings ──────────────────────────
MIN_ACTUATION_DURATION = 60    # seconds — minimum time any actuator stays on
ALERT_CONSECUTIVE_MIN  = 3     # number of consecutive breaches before alerting

# ── Actuator definitions ──────────────────────────────────────
ACTUATORS = {
    "cooling_fan":    {"trigger": "temperature",   "condition": "HIGH"},
    "heating_system": {"trigger": "temperature",   "condition": "LOW"},
    "dehumidifier":   {"trigger": "humidity",      "condition": "HIGH"},
    "humidifier":     {"trigger": "humidity",      "condition": "LOW"},
    "ventilation":    {"trigger": "co2",           "condition": "HIGH"},
    "irrigation":     {"trigger": "soil_moisture", "condition": "LOW"},
    "grow_lights":    {"trigger": "light",         "condition": "LOW"},
    "ph_down":        {"trigger": "water_ph",      "condition": "HIGH"},
    "ph_up":          {"trigger": "water_ph",      "condition": "LOW"},
}
