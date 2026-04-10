#  Smart Greenhouse IoT Data Pipeline

A production-style IoT data engineering project that simulates a smart
greenhouse monitoring system. Sensor data is collected from a Raspberry Pi
edge device, streamed through a real-time pipeline, stored in a cloud data
lake, and visualized on a live dashboard.

---

##  Architecture
Raspberry Pi (Edge Device)
│
│  MQTT over TLS
▼
AWS IoT Core
│
│  MQTT Bridge
▼
Apache Kafka (Docker)
│
├──────────────────────┐
│                      │
▼                      ▼
Python Processor          Amazon S3
(Actuation Logic)        (Data Lake)
│
▼
Amazon SNS
(Email Alerts)
│
▼
Sensor API Server
│
▼
Grafana Dashboard
(Live Visualization)

---

## ️ Technology Stack

| Layer | Technology |
|---|---|
| Edge Device | Raspberry Pi 4 (Raspbian OS) |
| IoT Ingestion | AWS IoT Core (MQTT over TLS) |
| Stream Processing | Apache Kafka (Docker) |
| Processing Logic | Python 3.12 |
| Cloud Storage | Amazon S3 |
| Alerting | Amazon SNS |
| Visualization | Grafana 10.2 |
| Infrastructure | Docker, Docker Compose |
| Cloud Provider | AWS (Free Tier) |

---

##  Simulated Sensors

| Sensor | Unit | Alert Thresholds |
|---|---|---|
| Temperature | °C | LOW < 18°C / HIGH > 30°C |
| Humidity | % | LOW < 40% / HIGH > 80% |
| CO2 Level | ppm | HIGH > 1500ppm |
| Soil Moisture | % | LOW < 20% |
| Light Intensity | lux | LOW < 200lux |
| Water pH | pH | LOW < 5.5 / HIGH > 7.0 |

---

##  Pipeline Overview

1. **Ingest** — A Python script on the Raspberry Pi simulates six greenhouse
   sensors and publishes readings every 15 seconds to AWS IoT Core via
   secured MQTT with TLS certificate authentication.

2. **Stream** — An MQTT-Kafka bridge subscribes to the IoT Core topic and
   forwards each message into a local Apache Kafka topic running in Docker.

3. **Process** — A Kafka consumer evaluates each reading against configured
   thresholds with hysteresis and debouncing logic. Actuation responses are
   triggered after 3 consecutive threshold breaches, and each actuator
   respects a minimum 60 second active duration to prevent rapid cycling.

4. **Store** — Every validated reading is written to Amazon S3 as a
   date-partitioned JSON file, forming a queryable raw data lake.

5. **Alert** — When alert conditions persist, Amazon SNS dispatches
   formatted email notifications detailing the affected sensors and
   recommended actuation responses.

6. **Visualize** — A lightweight Python API server reads the latest readings
   from S3 and serves them to a Grafana dashboard displaying six live
   time-series panels with threshold-based color coding.

---

##  Getting Started

### Prerequisites
- Raspberry Pi running Raspbian OS
- AWS Account (Free Tier)
- Docker Desktop
- Python 3.12+
- Apache Kafka (via Docker Compose)

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/smart-greenhouse-pipeline.git
cd smart-greenhouse-pipeline
```

### 2. Configure your environment
Copy `config.py` and fill in your values:
```python
IOT_ENDPOINT  = "your-endpoint-ats.iot.us-east-1.amazonaws.com"
S3_BUCKET     = "your-s3-bucket-name"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:your-account-id:greenhouse-alerts"
```

### 3. Set up AWS IoT Core
- Register a Thing named `SmartGreenhouse-Pi`
- Generate and download TLS certificates
- Attach the `GreenhousePolicy` IoT policy
- Place certificates in the `certs/` directory (not committed to Git)

### 4. Start the infrastructure
```bash
cd kafka
docker compose up -d
```

### 5. Start the pipeline components
```bash
# Terminal 1 — On Raspberry Pi
python3 sensor_simulator.py

# Terminal 2 — MQTT to Kafka bridge
python3 mqtt_to_kafka_bridge.py

# Terminal 3 — Kafka processor
python3 kafka_processor.py

# Terminal 4 — Sensor API server
python3 sensor_api.py
```

### 6. Open Grafana
Navigate to `http://localhost:3000` and log in with your configured
credentials to view the live sensor dashboard.

---

##  Planned Enhancements

- **Phase 6** — Actuation simulation layer with per-device state management
- **Phase 7** — OpenWeather API integration for proactive climate control
- **Phase 8** — AWS Kinesis Data Streams (pending account activation)
- **Phase 9** — InfluxDB time-series database integration
- **Phase 10** — Predictive analytics using NOAA historical climate data

---

##  Project Structure
smart-greenhouse-pipeline/
├── config.py                  # Central configuration and thresholds
├── sensor_simulator.py        # Raspberry Pi edge sensor simulation
├── mqtt_to_kafka_bridge.py    # IoT Core to Kafka message bridge
├── kafka_processor.py         # Stream processor with actuation logic
├── sensor_api.py              # REST API server for Grafana integration
├── kafka/
│   └── docker-compose.yml     # Kafka, Zookeeper, and Grafana containers
├── .gitignore                 # Excludes certificates and credentials
└── README.md                  # Project documentation

---

##  Security Notes

- TLS certificates and private keys are excluded from version control
- AWS credentials are managed via the AWS CLI credentials file
- All MQTT communication is encrypted with mutual TLS authentication
- IAM user follows principle of least privilege

---

*Built as a Data Engineering portfolio project demonstrating IoT ingestion,
real-time stream processing, cloud storage, and live visualization.*
