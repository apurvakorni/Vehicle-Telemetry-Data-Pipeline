# 🚘 Vehicle Telemetry Streaming Pipeline (Kafka + Spark + Postgres)
## 📌 Overview

This project builds a real-time data pipeline for electric vehicle (EV) telemetry.

Flow:

Producer generates or replays EV telemetry (speed, battery %, temperature).

Kafka streams telemetry events in real time.

Spark Structured Streaming computes rolling KPIs in 1-minute windows.

PostgreSQL stores aggregated results for analysis.
