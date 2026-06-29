# nyc-taxi-pipeline

NYC Taxi Trip Data Pipeline

An end-to-end data engineering pipeline built with Apache Airflow, Pandas, DuckDB, Python, YAML, and SQL. Designed as a portfolio project demonstrating production data engineering patterns including orchestration, schema validation, dead-letter handling, idempotent loads, and pipeline auditing.


Architecture

Raw Data Source (TLC API / synthetic fallback)
        |
check_data_freshness   (idempotency gate)
        |
ingest_raw_data        (download + land CSV)
        |
validate_schema        (YAML-driven quality checks)
        |
check_row_threshold    (branch: skip if < 100 rows)
        |
spark_transform        (clean, enrich, aggregate, Parquet write)
        |
write_dead_letter      (persist rejected rows for investigation)
        |
load_to_duckdb         (idempotent partition reload, 5 tables)
        |
update_pipeline_audit  (persist run metrics to audit table)
        |
run_analytics_queries  (business insight SQL + Slack summary)


Version History

v1.0 — Initial Pipeline

The first working version. A linear 5-task Airflow DAG that downloaded NYC TLC Yellow Taxi data, ran a basic Pandas transform, and wrote results to a local CSV.

Tasks: ingest_raw_data > validate_schema > spark_transform > load_to_duckdb > run_analytics_queries

What worked:


Airflow DAG with PythonOperator tasks wired in sequence
YAML schema file defining column types, nullability, and value ranges
Schema validator reading YAML and running Pandas checks
Basic Pandas transform producing Parquet output
DuckDB load from Parquet
Placeholder analytics queries

Project Structure

nyc-taxi-end-end/
├── dags/
│   └── taxi_pipeline_dag.py        Airflow DAG (v2.0)
├── ingestion/
│   ├── __init__.py
│   └── ingest_taxi_data.py         Download + synthetic fallback
├── validation/
│   ├── __init__.py
│   └── schema_validator.py         YAML-driven quality checks
├── transforms/
│   └── spark_transform.py          Standalone Pandas transform (CLI)
├── config/
│   └── schema.yaml                 Column schema contract
├── sql/
│   ├── create_tables.sql           DuckDB DDL
│   └── analytics_queries.sql       Business insight queries
├── tests/
│   ├── test_ingestion.py
│   └── test_transforms.py
├── scripts/
│   ├── setup_local.sh
│   ├── run_pipeline.sh
│   └── run_analytics.py
├── data/
│   ├── raw/                        Landed CSVs by date
│   ├── processed/                  Parquet outputs by date
│   ├── dead_letter/                Rejected rows by date
│   ├── analytics/                  DuckDB database file
│   └── validation_reports/         JSON validation reports
├── docker-compose.yml              Airflow local stack
├── requirements.txt
└── README.md


Local Setup

bash# 1. Clone and enter the project
cd nyc-taxi-end-end

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate          # Mac/Linux
.venv\Scripts\Activate.ps1         # Windows PowerShell

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create data directories
mkdir -p data/raw data/processed data/dead_letter data/analytics data/validation_reports

# 5. Start Airflow
docker compose up airflow-init     # first time only
docker compose up -d

# 6. Open Airflow UI
# URL: http://localhost:8080
# User: admin  |  Password: admin


Running Without Airflow

Each module is independently executable:

bash# Ingest only
python -m ingestion.ingest_taxi_data --date 2024-01-15

# Validate only
python -m validation.schema_validator \
    --data data/raw/2024-01-15/taxi_trips.csv \
    --schema config/schema.yaml

# Transform only
python transforms/spark_transform.py \
    --date 2024-01-15 \
    --input data/raw/2024-01-15/taxi_trips.csv \
    --output data/processed/2024-01-15/

# Analytics only
python scripts/run_analytics.py --date 2024-01-15