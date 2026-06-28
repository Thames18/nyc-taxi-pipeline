"""
NYC Taxi Trip Data Pipeline — Airflow DAG

SPARK FIX NOTE:
  PySpark requires Java (JDK) to be installed in the container.
  The standard apache/airflow image has neither spark-submit NOR Java.
  Solution: replace the Spark transform with a pure pandas implementation.
  Pandas is already installed in the Airflow container, produces identical
  output (Parquet files), and is perfectly appropriate for datasets up to
  ~10M rows. For a portfolio project this demonstrates the same pipeline
  concepts without an infrastructure dependency we can't control.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="End-to-end NYC Taxi trip data pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "nyc-taxi", "pandas"],
) as dag:

    #  Task 1: Ingest 
    def run_ingestion(**context):
        from ingestion.ingest_taxi_data import TaxiDataIngestor
        execution_date = context["ds"]
        ingestor = TaxiDataIngestor(execution_date=execution_date)
        result   = ingestor.run()
        context["ti"].xcom_push(key="raw_row_count", value=result["row_count"])
        context["ti"].xcom_push(key="output_path",   value=result["output_path"])
        print(f"Ingested {result['row_count']:,} rows ({result.get('source','?')}) → {result['output_path']}")

    ingest_task = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_ingestion,
        provide_context=True,
    )

    #  Task 2: Validate 
    def run_validation(**context):
        from validation.schema_validator import SchemaValidator
        input_path = context["ti"].xcom_pull(task_ids="ingest_raw_data", key="output_path")
        validator  = SchemaValidator(
            data_path=input_path,
            schema_path="config/schema.yaml",
        )
        report = validator.validate()
        if report["status"] == "FAILED":
            raise ValueError(
                f"Schema validation FAILED with {report['error_count']} hard errors. "
                f"Errors: {[e['message'] for e in report['errors']]}"
            )
        print(f"Validation {report['status']}: "
              f"{report['error_count']} errors, {report['warning_count']} warnings")
        context["ti"].xcom_push(key="validation_status", value=report["status"])

    validate_task = PythonOperator(
        task_id="validate_schema",
        python_callable=run_validation,
        provide_context=True,
    )

    #  Task 3: Pandas transform (replaces PySpark — no Java needed) 
    def run_transform(**context):
        """
        Pure-pandas transformation producing the same Parquet outputs
        as the original PySpark job. No Java, no spark-submit required.

        Produces:
          data/processed/{date}/trips.parquet        — cleaned trip rows
          data/processed/{date}/agg_by_zone.parquet  — per-zone daily metrics
          data/processed/{date}/agg_by_hour.parquet  — hourly demand pattern
        """
        import os
        import pandas as pd
        import numpy as np

        date        = context["ds"]
        input_path  = f"data/raw/{date}/taxi_trips.csv"
        output_dir  = f"data/processed/{date}"
        os.makedirs(output_dir, exist_ok=True)

        print(f"Reading {input_path}...")
        df = pd.read_csv(input_path, low_memory=False)
        print(f"Raw rows: {len(df):,}")

        #  Cast types 
        df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"],  errors="coerce")
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")
        for col in ["passenger_count", "vendor_id", "payment_type",
                    "pickup_location_id", "dropoff_location_id", "rate_code_id"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        for col in ["trip_distance", "fare_amount", "tip_amount", "total_amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        #  Filter bad rows ─
        before = len(df)
        df = df[
            df["tpep_dropoff_datetime"].notna() &
            df["tpep_pickup_datetime"].notna() &
            (df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]) &
            (df["trip_distance"].fillna(0)  > 0) &
            (df["fare_amount"].fillna(0)    > 0) &
            (df["passenger_count"].fillna(0) > 0)
        ].copy()
        print(f"After filtering: {len(df):,} rows (removed {before - len(df):,})")

        #  Derive features ─
        df["trip_duration_min"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds() / 60
        ).round(2)

        df["avg_speed_mph"] = np.where(
            df["trip_duration_min"] > 0,
            (df["trip_distance"] / (df["trip_duration_min"] / 60)).round(2),
            np.nan
        )

        df["tip_pct"] = np.where(
            df["fare_amount"] > 0,
            ((df["tip_amount"] / df["fare_amount"]) * 100).round(2),
            0.0
        )

        hour = df["tpep_pickup_datetime"].dt.hour
        df["time_of_day"] = pd.cut(
            hour,
            bins=[-1, 5, 11, 16, 21, 23],
            labels=["night", "morning", "afternoon", "evening", "night2"]
        ).astype(str).replace("night2", "night")

        df["pickup_date"] = df["tpep_pickup_datetime"].dt.date.astype(str)
        df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour

        df["payment_label"] = df["payment_type"].map({
            1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"
        }).fillna("unknown")

        #  Write cleaned trips parquet ─
        trips_path = f"{output_dir}/trips.parquet"
        df.to_parquet(trips_path, index=False, engine="pyarrow")
        print(f"Written trips.parquet → {trips_path} ({len(df):,} rows)")

        #  Zone aggregation 
        zone_agg = (
            df.groupby(["pickup_date", "pickup_location_id"], dropna=False)
            .agg(
                total_trips   = ("fare_amount", "count"),
                avg_fare      = ("fare_amount", "mean"),
                avg_tip_pct   = ("tip_pct",     "mean"),
                avg_distance  = ("trip_distance","mean"),
                avg_duration  = ("trip_duration_min", "mean"),
                total_revenue = ("total_amount", "sum"),
            )
            .round(2)
            .reset_index()
        )
        zone_agg.to_parquet(f"{output_dir}/agg_by_zone.parquet", index=False)
        print(f"Written agg_by_zone.parquet ({len(zone_agg):,} rows)")

        #  Hourly aggregation 
        hourly_agg = (
            df.groupby(["pickup_date", "pickup_hour", "time_of_day"], dropna=False)
            .agg(
                trip_count   = ("fare_amount", "count"),
                avg_fare     = ("fare_amount", "mean"),
                avg_passengers = ("passenger_count", "mean"),
            )
            .round(2)
            .reset_index()
        )
        hourly_agg.to_parquet(f"{output_dir}/agg_by_hour.parquet", index=False)
        print(f"Written agg_by_hour.parquet ({len(hourly_agg):,} rows)")

        context["ti"].xcom_push(key="clean_row_count", value=len(df))
        print(f"Transform complete  — {len(df):,} clean rows")

    spark_transform_task = PythonOperator(
        task_id="spark_transform",   # keep task_id so existing runs still map correctly
        python_callable=run_transform,
        provide_context=True,
    )

    #  Task 4: Load to DuckDB 
    def run_duckdb_load(**context):
        import os, subprocess, sys
        try:
            import duckdb
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install",
                                   "duckdb", "--quiet"])
            import duckdb

        date         = context["ds"]
        parquet_path = f"data/processed/{date}/trips.parquet"
        db_path      = "data/analytics/taxi_analytics.duckdb"
        os.makedirs("data/analytics", exist_ok=True)

        con = duckdb.connect(db_path)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS taxi_trips AS
            SELECT * FROM read_parquet('{parquet_path}') WHERE 1=0
        """)
        con.execute(f"DELETE FROM taxi_trips WHERE CAST(pickup_date AS VARCHAR) = '{date}'")
        con.execute(f"INSERT INTO taxi_trips SELECT * FROM read_parquet('{parquet_path}')")
        row_count = con.execute(
            f"SELECT COUNT(*) FROM taxi_trips WHERE CAST(pickup_date AS VARCHAR) = '{date}'"
        ).fetchone()[0]
        con.close()
        print(f"Loaded {row_count:,} rows into DuckDB for {date}")

    load_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=run_duckdb_load,
        provide_context=True,
    )

    #  Task 5: Analytics ─
    def run_analytics(**context):
        import subprocess, sys
        try:
            import duckdb
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install",
                                   "duckdb", "--quiet"])
            import duckdb

        date    = context["ds"]
        db_path = "data/analytics/taxi_analytics.duckdb"
        con     = duckdb.connect(db_path)

        queries = {
            "Total trips loaded":
                "SELECT COUNT(*) AS total_trips, ROUND(AVG(fare_amount),2) AS avg_fare FROM taxi_trips",
            "Top 5 hours by demand":
                "SELECT pickup_hour, COUNT(*) AS trips FROM taxi_trips GROUP BY pickup_hour ORDER BY trips DESC LIMIT 5",
            "Revenue by payment type":
                "SELECT payment_label, COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare FROM taxi_trips GROUP BY payment_label ORDER BY trips DESC",
            "Time of day breakdown":
                "SELECT time_of_day, COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare FROM taxi_trips GROUP BY time_of_day ORDER BY trips DESC",
        }

        for title, sql in queries.items():
            result = con.execute(sql).df()
            print(f"\n {title} ")
            print(result.to_string(index=False))

        con.close()
        print("\nAnalytics complete ")

    run_analytics_task = PythonOperator(
        task_id="run_analytics_queries",
        python_callable=run_analytics,
        provide_context=True,
    )

    #  Dependencies 
    ingest_task >> validate_task >> spark_transform_task >> load_task >> run_analytics_task
