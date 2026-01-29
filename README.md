# Real-Time Donation Tracking & Currency Conversion

## Project Overview
In high-inflation economies like Argentina, fundraising campaigns often set goals in USD while receiving donations in local currency (ARS). Due to currency volatility, the "real value" of the total collected fluctuates daily. 

This project implements a **Real-Time Data Lakehouse** (Medallion Architecture) that ingests donation events and live exchange rates, performs a stream-to-stream join, and calculates the actual USD value of each donation as it happens.

## Architecture 
The current implementation follows a streaming pattern using Dockerized services:
1.  **Ingestion (Bronze Layer):** 
    * **Donation Producer:** Simulates incoming donations with metadata (donation_id, campaign_id, amount_ars, donor_type, timestamp).
    * **Currency Producer:** Fetches live Blue/MEP rates from the **CriptoYa API** every 5 minutes.
    * **Kafka Cluster:** Acts as the message backbone with independent topics for raw data.
2.  **Processing (Silver Layer):**
    * **Spark Structured Streaming:** Consumes from Kafka, enforces schemas, and applies **Watermarking** (10m for prices, 5m for donations).
    * **Interval Join:** Joins streams based on a 1-hour window to ensure every donation finds a corresponding exchange rate even during API downtime.
    * **Data Quality (DQ):** Filters invalid records (nulls or non-positive values) before persistence.
3.  **Storage:** Data is persisted in **Parquet** format, which is optimized for columnar storage and compression.

## Tech Stack
* **Environment:** Docker & Docker Compose
* **Language:** Python (PySpark)
* **Package & Project Manager:** uv
* **Message Broker:** Apache Kafka & Zookeeper
* **Stream Processing:** Apache Spark Structured Streaming
* **Storage:** Parquet (Local Data Lakehouse)

## How to Run
1.  **Clone the repo:** `git clone https://github.com/NehuenYago/purchasing-power-lakehouse`
2.  **Start Services:** `make up`
3.  **Start Kafka Producers:** `make run-dollar` and `make run-donations`
4.  **Run Processing Job:** `make run-process-calculate`
5.  **Inspect Results:** `make check-data`

## Roadmap
- [x] Phase 1: Ingestion & Kafka Setup.
- [x] Phase 2: Silver Layer (Join, DQ, Parquet storage).
- [ ] Phase 3: Gold Layer (Real-time aggregations by campaign).

## Project Structure

```
.
├── docker-compose.yml
├── ingestion
│   └── producers                       <-- Kafka Producers
│       ├── dollar_api_producer.py
│       └── donation_simulator.py
├── Makefile
├── processing
│   ├── archive
│   │   ├── process_currency.py
│   │   └── process_donations.py
│   ├── scripts
│   │   └── check_results.py            <-- Verification Utility
│   └── spark_jobs
│       └── calculate_actual_usd.py     <-- Silver Layer Spark Job
├── pyproject.toml
├── README.md
└── uv.lock
```
