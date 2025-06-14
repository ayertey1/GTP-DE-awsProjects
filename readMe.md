
# AWS-Based ETL Data Pipeline with Airflow, Redshift, and S3

This project implements an end-to-end ETL pipeline orchestrated with Apache Airflow (deployed on Amazon MWAA). It ingests streaming and metadata from Amazon S3, computes key metrics, and loads results into Amazon Redshift. It also includes logging, data validation, and deduplication.

---

## Components

- **Amazon S3** – Storage for raw CSVs, processed outputs, and logs
- **Amazon MWAA (Airflow)** – DAG orchestration
- **Amazon Redshift** – Data warehouse for queryable tables
- **IAM Role** – Allows Redshift to load data from S3
- **Pandas + Boto3** – Data processing and S3 access
- **PostgresOperator** – Redshift interaction via COPY/INSERT

---

## Prerequisites

- An Amazon MWAA environment (with access to your S3 bucket and DAG path)
- A Redshift cluster with:
  - Publicly accessible or within the same VPC as MWAA
  - Tables: `staging_streams`, `final_streams`, `genre_kpis`, `hourly_kpis`, `etl_logs`
- IAM Role with permissions for Redshift to access your S3 bucket (`RedshiftS3CopyRole`)
- Airflow connection set up as:
  - **Conn ID:** `postgres_conn_id`
  - **Type:** Postgres
  - **Host:** `<Redshift endpoint>`
  - **Login/Password:** Redshift user credentials
  - **Schema:** Redshift DB name
  - **Port:** `5439` (default)

---

## Setup & Run Instructions

### 1. Upload CSVs to S3

Place the following files in your S3 bucket (`rawdata-dag-store` assumed):

```

s3://rawdata-dag-store/
├── data/
│   ├── metadata/
│   │   ├── songs.csv
│   │   └── users.csv
│   └── streams/
│       ├── stream\_2025-06-10.csv
│       ├── stream\_2025-06-11.csv
│       └── ...

````

> Ensure daily stream data files are named consistently and stored in `data/streams/`.

---

### 2. Configure MWAA

- Upload the `etl_pipeline_final_v3.py` DAG to your MWAA DAGs folder (in the S3 bucket linked to MWAA).
- Update your `requirements.txt` with:

```txt
apache-airflow-providers-amazon
pandas
boto3
botocore
````

* Deploy the requirements via MWAA UI.
* Set schedule to `@daily` or trigger manually for testing.

---

### 3. Redshift Table Setup

Run these SQL commands in your Redshift console or client:

```sql
-- 1. Staging table
CREATE TABLE IF NOT EXISTS staging_streams (
    user_id INT,
    track_id VARCHAR(64),
    listen_time TIMESTAMP,
    user_name VARCHAR(128),
    user_age INT,
    user_country VARCHAR(64),
    created_at DATE,
    id INT,
    artist VARCHAR(1024),
    album_name VARCHAR(512),
    title VARCHAR(512),
    popularity VARCHAR(16),      -- store as string for flexibility
    duration INT,
    explicit VARCHAR(8),         
    danceability FLOAT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,
    genre VARCHAR(128)
);

-- 2. Final table
CREATE TABLE IF NOT EXISTS final_streams (
    user_id INT,
    track_id VARCHAR(64),
    listen_time TIMESTAMP,
    user_name VARCHAR(128),
    user_age INT,
    user_country VARCHAR(64),
    created_at DATE,
    id INT,
    artist VARCHAR(1024),
    album_name VARCHAR(512),
    title VARCHAR(512),
    popularity VARCHAR(16),      -- store as string for flexibility
    duration INT,
    explicit VARCHAR(8),         
    danceability FLOAT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,
    genre VARCHAR(128)
);

-- 3. KPI tables
CREATE TABLE IF NOT EXISTS genre_kpis (
    genre VARCHAR,
    listen_count INT,
    avg_duration FLOAT,
    most_popular_track VARCHAR,
    popularity_index FLOAT
);

CREATE TABLE IF NOT EXISTS hourly_kpis (
    hour_bucket TIMESTAMP,
    unique_listeners INT,
    top_artist VARCHAR,
    track_diversity_index FLOAT
);

-- 4. Log table
CREATE TABLE IF NOT EXISTS etl_logs (
    log_time TIMESTAMP,
    dag_id VARCHAR,
    task_id VARCHAR,
    event_type VARCHAR,
    message VARCHAR
);
```

---

## 🛠 DAG Structure

```
etl_pipeline_final_v3 DAG
├── transform_and_compute_kpis     (PythonOperator)
│   ├── Merge metadata + streams
│   ├── Compute genre KPIs
│   └── Compute hourly KPIs
│
├── load_merged_to_staging         (COPY into staging_streams)
├── upsert_streams_to_final        (MERGE to final_streams)
├── load_genre_kpis_to_redshift    (COPY into genre_kpis)
├── load_hourly_kpis_to_redshift   (COPY into hourly_kpis)
└── load_etl_logs_to_redshift      (COPY into etl_logs)
```

---

## 🔍 Validation SQLs in Redshift

Here are some useful queries to validate your data post-load:

```sql
-- Check if final_streams has today’s data
SELECT COUNT(*) FROM final_streams
WHERE listen_time::date = CURRENT_DATE;

-- Check genre KPIs
SELECT * FROM genre_kpis
ORDER BY popularity_index DESC;

-- Hourly diversity validation
SELECT hour_bucket, track_diversity_index
FROM hourly_kpis
ORDER BY hour_bucket;

-- View logs for the latest DAG run
SELECT * FROM etl_logs
ORDER BY log_time DESC
LIMIT 20;
```

---

## 🗂 Folder Layout (S3)

```
rawdata-dag-store/
├── data/
│   ├── metadata/
│   │   ├── songs.csv
│   │   └── users.csv
│   ├── streams/
│   │   ├── stream_YYYY-MM-DD.csv
│   └── processed/
│       ├── YYYY-MM-DD/
│       │   ├── merged_stream_data.csv
│       │   └── kpis/
│       │       ├── genre_kpis.csv
│       │       └── hourly_kpis.csv
├── logs/
│   ├── processed_files.txt
│   └── event_log_YYYY-MM-DD.csv
```

---

## 📋 Notes

* You can backfill by uploading older stream data and setting the `execution_date` manually.
* The DAG logs detailed events in `logs/event_log_<date>.csv` for auditing.

---

## 👨‍💻 Author

Peter Caleb Ayertey – Data Engineer
*Built with Apache Airflow, Pandas, Redshift & S3*

---

