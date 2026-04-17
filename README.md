
````markdown
# 🚀 Tech Job Trends: Real-Time ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Spark](https://img.shields.io/badge/Apache_Spark-4.1.1-orange)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.6-black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-green)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-success)

An end-to-end, distributed Data Engineering pipeline designed to ingest, process, and analyze global technology job market trends in real-time. 

## 📌 Business Context & Value
In a rapidly shifting job market, batch processing is no longer sufficient to track skill demands. This project eliminates data latency by implementing a streaming architecture. It continuously extracts job postings, normalizes skill requirements, and serves clean data to a warehouse, enabling real-time analytics for hiring trends.

## 🏗️ Architecture Design (Medallion Approach)

This pipeline strictly enforces the **Medallion Data Architecture**:

* **🥉 Bronze (Raw Ingestion):** Python-based extractors pull unstructured job postings via APIs/Scraping and push them directly to an **Apache Kafka** topic (`tech_jobs_stream`). Data is appended exactly as it arrives.
* **🥈 Silver (Stream Processing & Cleaning):** **PySpark (4.1.1) Structured Streaming** consumes the Kafka topic. It enforces schema validation, parses JSON payloads, handles missing values, and standardizes job titles.
* **🥇 Gold (Serving Layer):** The transformed micro-batches are continuously upserted into a **PostgreSQL** data warehouse, making the data instantly available for downstream BI tools or advanced analytics.

## 📂 Repository Structure

```text
tech-job/
├── config/
│   └── settings.py          # Environment variables and DB credentials
├── drivers/
│   └── postgresql-*.jar     # JDBC driver for Spark-to-Postgres connection
├── ingestion/
│   ├── scraper.py           # Raw data extraction logic
│   ├── api_ingestion.py     # API connection handlers
│   └── kafka_producer.py    # Kafka Producer: Streams fetched data to broker
├── transformation/
│   ├── silver_cleaning.py   # Batch cleaning and normalization scripts
│   ├── gold_skills.py       # Aggregation logic for the Gold layer
│   └── stream_gold.py       # PySpark Structured Streaming consumer
├── requirements.txt         # Project dependencies
├── .gitignore               # Excludes raw data lake storage and env files
└── README.md
````

## ⚙️ Prerequisites

  * **Python 3.12+**
  * **Java 17+** (Required for Spark 4.x)
  * **Apache Kafka** & **Zookeeper**
  * **Apache Spark 4.1.1** (Configured for Scala 2.13)
  * **PostgreSQL**

## 🚀 Quick Start Guide

### 1\. Environment Setup

Clone the repository and install dependencies:

```bash
git clone git@github.com:abdelali522/tech-job.git
cd tech-job
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2\. Start Infrastructure

Run the following services in separate terminal sessions:

**Terminal 1: Zookeeper**

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Terminal 2: Kafka Broker**

```bash
bin/kafka-server-start.sh config/server.properties
```

**Terminal 3: Database Preparation**
Ensure your PostgreSQL instance is running and create the target database:

```sql
CREATE DATABASE tech_jobs_db;
```

### 3\. Execute the Streaming Pipeline

**Terminal 4: Start the Data Producer**
Continuously fetches job postings and publishes them to Kafka.

```bash
source venv/bin/activate
python ingestion/kafka_producer.py
```

**Terminal 5: Start the Spark Stream-to-Postgres Consumer**
Uses `spark-submit` to inject the necessary JVM dependencies (Kafka connector and PostgreSQL JDBC) at runtime.

```bash
source venv/bin/activate
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2 \
  --jars drivers/postgresql-42.7.2.jar \
  transformation/stream_gold.py
```

## 🧠 Technical Challenges Solved

  * **Scala/Spark Interoperability:** Resolved native `NoSuchMethodError` crashes by aligning the Spark Structured Streaming Kafka connector strictly to the Scala 2.13 ecosystem required by Spark 4.1.1.
  * **Offset Management:** Implemented robust checkpointing and offset handling to ensure exactly-once processing semantics and prevent data loss during stream restarts.
  * **Stateful Fault Tolerance:** Transitioned from a fragile loop-based ingestion to a decoupled Producer/Consumer model, ensuring the pipeline survives API rate limits and connection drops.

## 🔮 Future Roadmap

  * **Containerization:** Dockerize Zookeeper, Kafka, Spark, and PostgreSQL using `docker-compose` for 1-click deployments.
  * **Workflow Orchestration:** Fully integrate the batch fallback mechanisms with Apache Airflow DAGs.
  * **Domain Expansion:** Adapt the ingestion schema to support analytics for specific sectors, such as the demand for software engineers in the medical and healthcare technology domains.

-----

**Author:** Abdelali Marin  
*Engineering Student at INPT (Institut National des Postes et Télécommunications)* [LinkedIn](https://linkedin.com/in/abdelali-marin) | [GitHub](https://github.com/abdelali522)
```
