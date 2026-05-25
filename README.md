# Smart Greenhouse IoT Data Pipeline

A production-style IoT data engineering project that simulates a smart
greenhouse monitoring system. Sensor data is collected from a Raspberry Pi
edge device, streamed through a real-time pipeline, stored in a cloud data
lake, and visualized on a live dashboard.

---

## Architecture

```
Raspberry Pi 5 (Edge Device + Server)
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
Python Processor     Amazon S3
(Actuation Logic)    (Data Lake)
│                      │
▼                      ▼
InfluxDB            Amazon SNS
(Time-Series DB)    (Email Alerts)
│
▼
Grafana Dashboard
(Live Visualization)
│
▼
Health Monitor
(Pipeline Observability)
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Edge Device | Raspberry Pi 5 (Raspbian OS) |
| IoT Ingestion | AWS IoT Core (MQTT over TLS) |
| Stream Processing | Apache Kafka (Docker) |
| Processing Logic | Python 3.12 |
| Cloud Storage | Amazon S3 |
| Time-Series Database | InfluxDB 2.7 |
| Alerting | Amazon SNS + Gmail SMTP |
| Visualization | Grafana 10.2 |
| Weather | OpenWeather API |
| Infrastructure | Docker, Docker Compose |
| Cloud Provider | AWS (Free Tier) |

---

## Simulated Sensors

| Sensor | Unit | Alert Thresholds |
|---|---|---|
| Temperature | °C | LOW < 18°C / HIGH > 30°C |
| Humidity | % | LOW < 40% / HIGH > 80% |
| CO2 Level | ppm | HIGH > 1500ppm |
| Soil Moisture | % | LOW < 20% |
| Light Intensity | lux | LOW < 200lux |
| Water pH | pH | LOW < 5.5 / HIGH > 7.0 |

---

## Pipeline Overview

1. **Ingest** — A Python script on the Raspberry Pi simulates six greenhouse
   sensors and publishes readings every 15 seconds to AWS IoT Core via
   secured MQTT with TLS certificate authentication.

2. **Stream** — An MQTT-Kafka bridge subscribes to the IoT Core topic and
   forwards each message into a local Apache Kafka topic running in Docker.

3. **Process** — A Kafka consumer evaluates each reading against configured
   thresholds with hysteresis and debouncing logic. Actuation responses are
   triggered after 3 consecutive threshold breaches, and each actuator
   respects a minimum 60 second active duration to prevent rapid cycling.

4. **Store** — Every validated reading is written simultaneously to Amazon S3
   as a date-partitioned JSON file (permanent raw archive) and to InfluxDB
   as a tagged time-series point for millisecond-latency queries.

5. **Alert** — When alert conditions persist, Amazon SNS dispatches formatted
   email notifications. The OpenWeather integration delivers tiered proactive
   briefing emails at 7 AM and 7 PM via Gmail SMTP.

6. **Visualize** — Grafana dashboard powered by native Flux queries to
   InfluxDB, displaying twelve panels — six time-series trend graphs and six
   current-value stat panels — plus an actuator status table.

7. **Monitor** — A health monitor service checks all pipeline components every
   five minutes and sends HTML alert emails on failure with a 30 minute
   cooldown to prevent alert spam.

---

## Autonomous Deployment

The pipeline is deployed as a set of systemd services on a Raspberry Pi 5
running 24/7, requiring no manual intervention after initial setup.

### Service Architecture

| Service | Description | Auto-restart |
|---|---|---|
| `greenhouse-docker` | Starts Kafka, InfluxDB, and Grafana containers | Yes |
| `greenhouse-simulator` | Publishes sensor readings every 15 seconds | Yes |
| `greenhouse-bridge` | Forwards MQTT messages to Kafka | Yes |
| `greenhouse-processor` | Processes stream, writes to S3 and InfluxDB | Yes |
| `greenhouse-api` | Serves sensor data REST API | Yes |
| `greenhouse-weather` | Fetches weather and sends briefing emails | Yes |
| `greenhouse-health` | Monitors pipeline component health | Yes |

All services are configured to start automatically on boot and restart
on failure using systemd. The pipeline collects and processes data
continuously regardless of whether any other device is active.

### Accessing the Dashboard

With the pipeline running on the Pi, Grafana and InfluxDB are accessible
from any device on the local network:

- **Grafana:** `http://<pi-ip>:3000`
- **InfluxDB:** `http://<pi-ip>:8086`

---

## Getting Started

### Prerequisites
- Raspberry Pi 5 running Raspbian OS
- AWS Account (Free Tier)
- Docker and Docker Compose
- Python 3.12+

### 1. Clone the repository
```bash
git clone https://github.com/DClay221/smart-greenhouse-pipeline.git
cd smart-greenhouse-pipeline
```

### 2. Configure your environment
Create `config.py` with your values (excluded from version control):
```python
IOT_ENDPOINT       = "your-endpoint-ats.iot.us-east-1.amazonaws.com"
S3_BUCKET          = "your-s3-bucket-name"
SNS_TOPIC_ARN      = "arn:aws:sns:us-east-1:your-account-id:greenhouse-alerts"
OPENWEATHER_API_KEY = "your-openweather-api-key"
OPENWEATHER_ZIP    = "your-zip-code"
EMAIL_SENDER       = "your-gmail@gmail.com"
EMAIL_RECIPIENT    = "your-gmail@gmail.com"
EMAIL_APP_PASSWORD = "your-gmail-app-password"
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

### 5. Enable systemd services
```bash
sudo systemctl enable greenhouse-docker greenhouse-simulator greenhouse-bridge
sudo systemctl enable greenhouse-processor greenhouse-api greenhouse-weather
sudo systemctl enable greenhouse-health
sudo systemctl start greenhouse-docker
# Wait 90 seconds for Kafka to initialize before starting remaining services
sudo systemctl start greenhouse-simulator greenhouse-bridge greenhouse-processor
sudo systemctl start greenhouse-api greenhouse-weather greenhouse-health
```

### 6. Access the dashboard
Navigate to `http://<pi-ip>:3000` and log in with your configured
Grafana credentials to view the live sensor dashboard.

---

## Project Structure

```
smart-greenhouse-pipeline/
├── config.py                  # Central configuration and thresholds
├── sensor_simulator.py        # Raspberry Pi edge sensor simulation
├── mqtt_to_kafka_bridge.py    # IoT Core to Kafka message bridge
├── kafka_processor.py         # Stream processor with actuation logic
├── actuator_manager.py        # Device state management with hysteresis
├── sensor_api.py              # REST API server for Grafana integration
├── weather_fetcher.py         # Tiered weather forecasting and email briefings
├── health_monitor.py          # Pipeline health monitoring and alerting
├── kafka/
│   └── docker-compose.yml     # Kafka, Zookeeper, InfluxDB, and Grafana
├── index.html                 # GitHub Pages project site
├── dashboard-screenshot.png   # Grafana dashboard screenshot
├── .gitignore                 # Excludes certificates and credentials
└── README.md                  # Project documentation
```

---

## Build Phases

| Phase | Title | Status |
|---|---|---|
| 1 | Ingest — Sensor Simulator + AWS IoT Core | Complete |
| 2 | Stream — Apache Kafka + Real-Time Alerting | Complete |
| 3 | Store — Amazon S3 Data Lake | Complete |
| 4 | Visualize — Grafana Dashboard | Complete |
| 5 | Hardening — Production Readiness | Complete |
| 6 | Actuation — Device Simulation Layer | Complete |
| 7 | Weather — OpenWeather API Integration | Complete |
| 8 | AWS Kinesis — Cloud-Native Stream Processing | Pending |
| 9 | Time-Series — InfluxDB Integration | Complete |
| 10 | Predictive Analytics — NOAA + scikit-learn | Planned |
| 11 | Deployment + Observability — Pi 5 + Health Monitor | Complete |

---

## Planned Enhancements

- **Phase 8** — AWS Kinesis Data Streams replacement for Kafka
  (pending AWS account subscription activation)
- **Phase 10** — Predictive analytics using NOAA historical climate data
  combined with collected greenhouse sensor data and scikit-learn ML models

---

## Security Notes

- TLS certificates and private keys are excluded from version control
- AWS credentials are managed via the AWS CLI credentials file
- All MQTT communication is encrypted with mutual TLS authentication
- IAM user follows principle of least privilege
- Gmail App Password used for SMTP — never the account password

---

## Known Development Environment Limitations

- **Grafana tab throttling** — Browsers throttle or pause JavaScript timers
  on inactive tabs to conserve resources. In a production environment Grafana
  would run on a dedicated server where this is not a concern.

- **S3 API latency** — The sensor API fetches live data directly from Amazon
  S3 on every request for the legacy dashboard. This is resolved in the
  InfluxDB dashboard which queries the local time-series database directly
  with millisecond latency.

- **Single greenhouse simulation** — The current pipeline simulates one
  Raspberry Pi device. Multi-greenhouse scaling is architecturally supported
  via Kafka topic partitioning and namespaced MQTT topics but has not been
  implemented in this iteration.

- **AWS Kinesis unavailable** — Amazon Kinesis Data Streams requires a
  subscription that is not available on new AWS accounts at the free tier
  level. The project uses Apache Kafka running in Docker as a functionally
  equivalent replacement. The skills demonstrated (stream ingestion,
  partitioning, consumer groups) are directly transferable to Kinesis.

- **Amazon Timestream unavailable** — Timestream for LiveAnalytics closed
  access to new customers effective June 20 2025. InfluxDB running in Docker
  serves as the time-series database layer with equivalent functionality.

---

*Built as a Data Engineering portfolio project demonstrating IoT ingestion,
real-time stream processing, cloud storage, time-series analytics, and
autonomous edge deployment.*