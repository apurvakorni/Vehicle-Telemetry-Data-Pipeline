# 🚘 Vehicle Telemetry Streaming Pipeline (Kafka + Spark + Postgres)
## 📌 Overview

This project builds a real-time data pipeline for electric vehicle (EV) telemetry.
## 🛠️ Tech Stack

Streaming: Apache Kafka

Database: PostgreSQL

Infra: Docker Compose

Code: Python (producer & consumer)

## 📂 Flow:

1. Producer generates or replays EV telemetry (speed, battery %, temperature).

2. Kafka streams telemetry events in real time.

3. Consumer subscribes to Kafka and writes data to PostgreSQL.

4. Postgres stores the data for analysis and dashboards.

## 🚀 Quick Start

### 1. Start Services
```bash
docker compose up -d
```
### 2. Create Kafka Topic
```bash
docker compose exec kafka kafka-topics --create \
  --topic telemetry --bootstrap-server kafka:9092 \
  --partitions 3 --replication-factor 1
```

### 3. Run Producer
```bash
cd producer
pip install kafka-python
python producer.py --vehicles 50 --rate 5
```

👉 To use a real dataset, modify producer.py to read from CSV/JSON and push rows instead of generating random values.

### 4. Run Consumer
```bash
cd consumer
pip install kafka-python psycopg2-binary
python consumer.py
```
### 5. Query Data in Postgres
```bash
psql "postgresql://evuser:evpass@localhost:5432/telemetrydb" \
  -c "SELECT * FROM telemetry ORDER BY ts DESC LIMIT 5;"
```


