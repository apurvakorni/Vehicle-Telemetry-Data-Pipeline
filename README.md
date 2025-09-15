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
