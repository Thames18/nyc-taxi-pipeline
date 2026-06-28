"""
NYC Taxi Trip Data Pipeline — Airflow DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    tags=["data-engineering", "nyc-taxi", "spark"],
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

    #  Task 3: Spark transform (PythonOperator — no spark-submit needed) ─
    def run_spark_transform(**context):
        """
        FIX: Was BashOperator calling spark-submit, which is not installed
        in the standard apache/airflow Docker image.
        Solution: run PySpark directly via Python using SparkSession with
        local[*] master — no separate Spark installation required.
        PySpark is pip-installable and runs fully in-process.
        """
        import subprocess
        import sys
        import os

        date       = context["ds"]
        input_path = f"data/raw/{date}/taxi_trips.csv"
        output_path = f"data/processed/{date}/"

        # Install pyspark inside the container if not already present
        try:
            import pyspark
        except ImportError:
            print("Installing pyspark...")
            subprocess.check_call([sys.executable, "-m", "pip", "install",
                                   "pyspark==3.5.1", "--quiet"])

        # Now import and run the transform inline
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType, DoubleType, TimestampType
        import os

        os.makedirs(output_path, exist_ok=True)

        spark = (
            SparkSession.builder
            .appName("nyc-taxi-transform")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "1g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        print(f"Reading CSV: {input_path}")
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(input_path)
        )

        # Cast types
        df = (
            df
            .withColumn("vendor_id",            F.col("vendor_id").cast(IntegerType()))
            .withColumn("tpep_pickup_datetime",  F.col("tpep_pickup_datetime").cast(TimestampType()))
            .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
            .withColumn("passenger_count",       F.col("passenger_count").cast(IntegerType()))
            .withColumn("trip_distance",         F.col("trip_distance").cast(DoubleType()))
            .withColumn("fare_amount",           F.col("fare_amount").cast(DoubleType()))
            .withColumn("tip_amount",            F.col("tip_amount").cast(DoubleType()))
            .withColumn("total_amount",          F.col("total_amount").cast(DoubleType()))
            .withColumn("payment_type",          F.col("payment_type").cast(IntegerType()))
            .withColumn("pickup_location_id",    F.col("pickup_location_id").cast(IntegerType()))
            .withColumn("dropoff_location_id",   F.col("dropoff_location_id").cast(IntegerType()))
        )

        # Filter bad rows
        df = (
            df
            .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
            .filter(F.col("trip_distance") > 0)
            .filter(F.col("fare_amount") > 0)
            .filter(F.col("passenger_count") > 0)
        )

        # Derive features
        df = (
            df
            .withColumn("trip_duration_min",
                (F.unix_timestamp("tpep_dropoff_datetime") -
                 F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
            .withColumn("avg_speed_mph",
                F.when(F.col("trip_duration_min") > 0,
                    F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)
                ).otherwise(None))
            .withColumn("tip_pct",
                F.when(F.col("fare_amount") > 0,
                    (F.col("tip_amount") / F.col("fare_amount")) * 100
                ).otherwise(0.0))
            .withColumn("time_of_day",
                F.when(F.hour("tpep_pickup_datetime").between(6, 11),  "morning")
                 .when(F.hour("tpep_pickup_datetime").between(12, 16), "afternoon")
                 .when(F.hour("tpep_pickup_datetime").between(17, 21), "evening")
                 .otherwise("night"))
            .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
            .withColumn("payment_label",
                F.when(F.col("payment_type") == 1, "credit_card")
                 .when(F.col("payment_type") == 2, "cash")
                 .when(F.col("payment_type") == 3, "no_charge")
                 .when(F.col("payment_type") == 4, "dispute")
                 .otherwise("unknown"))
        )

        df.cache()
        n = df.count()
        print(f"Clean rows after filtering: {n:,}")

        # Write trips parquet
        trips_out = output_path + "trips.parquet"
        df.write.mode("overwrite").parquet(trips_out)
        print(f"Written trips → {trips_out}")

        # Zone aggregation
        zone_agg = (
            df.groupBy("pickup_date", "pickup_location_id")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
                F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
                F.sum("total_amount").alias("total_revenue"),
            )
        )
        zone_agg.write.mode("overwrite").parquet(output_path + "agg_by_zone.parquet")

        # Hourly aggregation
        hourly_agg = (
            df.groupBy("pickup_date", "pickup_hour", "time_of_day")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            )
        )
        hourly_agg.write.mode("overwrite").parquet(output_path + "agg_by_hour.parquet")

        df.unpersist()
        spark.stop()
        print(f"Spark transform complete  — {n:,} rows processed")
        context["ti"].xcom_push(key="clean_row_count", value=n)

    spark_transform_task = PythonOperator(
        task_id="spark_transform",
        python_callable=run_spark_transform,
        provide_context=True,
    )

    #  Task 4: Load to DuckDB 
    def run_duckdb_load(**context):
        import subprocess, sys
        try:
            import duckdb
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install",
                                   "duckdb==0.10.3", "--quiet"])
            import duckdb

        date         = context["ds"]
        parquet_path = f"data/processed/{date}/trips.parquet"
        db_path      = "data/analytics/taxi_analytics.duckdb"

        import os
        os.makedirs("data/analytics", exist_ok=True)

        con = duckdb.connect(db_path)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS taxi_trips AS
            SELECT * FROM read_parquet('{parquet_path}') WHERE 1=0;
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
                                   "duckdb==0.10.3", "--quiet"])
            import duckdb

        date    = context["ds"]
        db_path = "data/analytics/taxi_analytics.duckdb"
        con     = duckdb.connect(db_path)

        queries = {
            "Total trips loaded":
                f"SELECT COUNT(*) AS total FROM taxi_trips",
            "Top 5 hours by trip count":
                f"SELECT pickup_hour, COUNT(*) AS trips FROM taxi_trips GROUP BY pickup_hour ORDER BY trips DESC LIMIT 5",
            "Avg fare by payment type":
                f"SELECT payment_label, ROUND(AVG(fare_amount),2) AS avg_fare, COUNT(*) AS trips FROM taxi_trips GROUP BY payment_label ORDER BY trips DESC",
        }

        for title, sql in queries.items():
            result = con.execute(sql).df()
            print(f"\n {title} ")
            print(result.to_string(index=False))

        con.close()

    run_analytics_task = PythonOperator(
        task_id="run_analytics_queries",
        python_callable=run_analytics,
        provide_context=True,
    )

    #  Dependencies 
    ingest_task >> validate_task >> spark_transform_task >> load_task >> run_analytics_task
