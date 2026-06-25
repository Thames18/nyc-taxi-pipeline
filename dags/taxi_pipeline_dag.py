"""
NYC Taxi Trip Data Pipeline — Airflow DAG
=========================================
Orchestrates the full pipeline:
  1. Ingest raw CSV data from NYC TLC public dataset
  2. Validate schema against YAML contract
  3. Run PySpark transformations
  4. Load results to DuckDB analytics layer

Schedule: Daily at 6 AM UTC
Catchup: False (run latest partition only)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


#   Default args 
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


#   DAG definition 
with DAG(
    dag_id="nyc_taxi_pipeline",
    description="End-to-end NYC Taxi trip data pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "nyc-taxi", "spark"],
) as dag:

    #   Task 1: Ingest raw data                ─
    def run_ingestion(**context):
        from ingestion.ingest_taxi_data import TaxiDataIngestor

        execution_date = context["ds"]  # YYYY-MM-DD
        ingestor = TaxiDataIngestor(execution_date=execution_date)
        result = ingestor.run()

        # Push row count to XCom so downstream tasks can inspect it
        context["ti"].xcom_push(key="raw_row_count", value=result["row_count"])
        context["ti"].xcom_push(key="output_path", value=result["output_path"])

        print(f"  Ingested {result['row_count']:,} rows → {result['output_path']}")

    ingest_task = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_ingestion,
        provide_context=True,
    )

    #   Task 2: Validate schema                ─
    def run_validation(**context):
        """Validate ingested CSV against YAML schema contract."""
        from validation.schema_validator import SchemaValidator

        input_path = context["ti"].xcom_pull(
            task_ids="ingest_raw_data", key="output_path"
        )
        validator = SchemaValidator(
            data_path=input_path,
            schema_path="config/schema.yaml",
        )
        report = validator.validate()

        if report["status"] == "FAILED":
            raise ValueError(
                f"Schema validation failed with {report['error_count']} errors. "
                f"See report: {report['report_path']}"
            )

        print(f"  Validation passed — {report['warning_count']} warnings")
        context["ti"].xcom_push(key="validation_report", value=report)

    validate_task = PythonOperator(
        task_id="validate_schema",
        python_callable=run_validation,
        provide_context=True,
    )

    #   Task 3: PySpark transform                
    spark_transform_task = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "cd /opt/airflow && "
            "spark-submit "
            "--master local[*] "
            "--driver-memory 2g "
            "transforms/spark_transform.py "
            "--date {{ ds }} "
            "--input data/raw/{{ ds }}/taxi_trips.csv "
            "--output data/processed/{{ ds }}/"
        ),
    )

    #   Task 4: Load to analytics DB               
    def run_duckdb_load(**context):
        """Load Parquet output into DuckDB for SQL analytics."""
        import duckdb

        date = context["ds"]
        parquet_path = f"data/processed/{date}/trips.parquet"
        db_path = "data/analytics/taxi_analytics.duckdb"

        con = duckdb.connect(db_path)

        # Create or replace today's partition
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS taxi_trips AS
            SELECT * FROM read_parquet('{parquet_path}')
            WHERE 1=0;
        """)

        con.execute(f"""
            DELETE FROM taxi_trips WHERE pickup_date = '{date}';
        """)

        con.execute(f"""
            INSERT INTO taxi_trips
            SELECT * FROM read_parquet('{parquet_path}');
        """)

        row_count = con.execute(
            f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_date = '{date}'"
        ).fetchone()[0]

        con.close()
        print(f" Loaded {row_count:,} rows into DuckDB for {date}")

    load_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=run_duckdb_load,
        provide_context=True,
    )

    #   Task 5: Run analytics SQL                
    run_analytics_task = BashOperator(
        task_id="run_analytics_queries",
        bash_command=(
            "python scripts/run_analytics.py --date {{ ds }}"
        ),
    )

    #   DAG dependency graph                  
    #
    #   ingest_raw_data
    #         │
    #   validate_schema
    #         │
    #   spark_transform
    #         │
    #   load_to_duckdb
    #         │
    #   run_analytics_queries
    #
    ingest_task >> validate_task >> spark_transform_task >> load_task >> run_analytics_task
