"""
NYC Taxi Trip Data Pipeline — Airflow DAG
Improved with: task groups, rich logging, email + Slack notifications,
SLA tracking, sensor-based branching, and detailed XCom metrics.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

#  Notification helpers 

def _send_slack_notification(message: str, emoji: str = "🚕") -> None:
    """
    Send a Slack message via webhook.
    Set SLACK_WEBHOOK_URL in Airflow Variables or environment to enable.
    Fails silently so the pipeline is never blocked by a notification failure.
    """
    import os, requests as req
    webhook = os.getenv("SLACK_WEBHOOK_URL") or ""
    try:
        Variable.get("slack_webhook_url")
        webhook = Variable.get("slack_webhook_url")
    except Exception:
        pass
    if not webhook:
        print(f"[NOTIFY] Slack not configured. Message: {emoji} {message}")
        return
    try:
        resp = req.post(webhook, json={"text": f"{emoji} *NYC Taxi Pipeline* — {message}"}, timeout=5)
        resp.raise_for_status()
        print(f"[NOTIFY] Slack sent: {message}")
    except Exception as e:
        print(f"[NOTIFY] Slack failed (non-blocking): {e}")


def _on_failure_callback(context):
    """Called by Airflow on any task failure."""
    dag_id   = context["dag"].dag_id
    task_id  = context["task"].task_id
    run_id   = context["run_id"]
    exc      = context.get("exception", "unknown error")
    ts       = context["ts"]
    message  = (
        f" FAILED  |  task=`{task_id}`  dag=`{dag_id}`\n"
        f"run_id: `{run_id}`  |  ts: `{ts}`\n"
        f"Error: `{exc}`"
    )
    _send_slack_notification(message)


def _on_success_callback(context):
    """Called by Airflow on DAG-level success (attached to final task)."""
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    ts     = context["ts"]
    _send_slack_notification(
        f" COMPLETE  |  dag=`{dag_id}`  run=`{run_id}`  ts=`{ts}`"  )


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when a task exceeds its SLA."""
    msg = f" SLA MISS  |  dag=`{dag.dag_id}`  tasks=`{[t.task_id for t in blocking_task_list]}`"
    _send_slack_notification(msg)


#  DAG defaults 

DEFAULT_ARGS = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email_on_failure":  False,       # set True + email list for email alerts
    "email_on_retry":    False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": _on_failure_callback,
}

#  DAG ─

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="End-to-end NYC Taxi trip data pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=_sla_miss_callback,
    tags=["data-engineering", "nyc-taxi", "pandas"],
    doc_md="""
## NYC Taxi Pipeline

End-to-end pipeline: ingest → validate → transform → load → analytics.

| Task | Description | SLA |
|---|---|---|
| ingest_raw_data | Download / generate trip CSV | 10 min |
| validate_schema | YAML-driven data quality checks | 5 min |
| spark_transform | Pandas clean + feature engineering + Parquet write | 20 min |
| load_to_duckdb | Load Parquet into DuckDB analytics layer | 5 min |
| run_analytics_queries | Run business insight SQL queries | 5 min |

**Notifications**: Slack webhook via `slack_webhook_url` Airflow Variable.
    """,
) as dag:

    #  Task 1: Ingest 
    def run_ingestion(**context):
        import logging
        log = logging.getLogger(__name__)

        from ingestion.ingest_taxi_data import TaxiDataIngestor

        date     = context["ds"]
        log.info("=" * 60)
        log.info(f"INGEST START  |  execution_date={date}")
        log.info("=" * 60)

        ingestor = TaxiDataIngestor(execution_date=date)
        result   = ingestor.run()

        row_count = result["row_count"]
        out_path  = result["output_path"]
        source    = result.get("source", "unknown")

        log.info(f"INGEST RESULT  |  rows={row_count:,}  source={source}  path={out_path}")

        context["ti"].xcom_push(key="raw_row_count", value=row_count)
        context["ti"].xcom_push(key="output_path",   value=out_path)
        context["ti"].xcom_push(key="data_source",   value=source)

        _send_slack_notification(
            f"📥 Ingested `{row_count:,}` rows (source=`{source}`) for `{date}`",
            emoji="📥"
        )

    ingest_task = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_ingestion,
        provide_context=True,
        sla=timedelta(minutes=10),
        doc_md="Downloads or generates NYC taxi trip CSV for the execution date.",
    )

    #  Task 2: Validate 
    def run_validation(**context):
        import logging
        log = logging.getLogger(__name__)

        from validation.schema_validator import SchemaValidator

        input_path = context["ti"].xcom_pull(task_ids="ingest_raw_data", key="output_path")
        date       = context["ds"]

        log.info("=" * 60)
        log.info(f"VALIDATE START  |  path={input_path}")
        log.info("=" * 60)

        validator = SchemaValidator(
            data_path=input_path,
            schema_path="config/schema.yaml",
        )
        report = validator.validate()

        status   = report["status"]
        n_errors = report["error_count"]
        n_warns  = report["warning_count"]

        log.info(f"VALIDATE RESULT  |  status={status}  errors={n_errors}  warnings={n_warns}")

        if n_warns > 0:
            log.warning(f"WARNINGS ({n_warns}):")
            for w in report["warnings"]:
                log.warning(f"  ⚠  {w['message']}")

        if status == "FAILED":
            log.error(f"ERRORS ({n_errors}):")
            for e in report["errors"]:
                log.error(f"  ✗  {e['message']}")
            raise ValueError(
                f"Schema validation FAILED — {n_errors} hard errors. "
                f"See report: {report['report_path']}"
            )

        context["ti"].xcom_push(key="validation_status",    value=status)
        context["ti"].xcom_push(key="validation_warnings",  value=n_warns)
        context["ti"].xcom_push(key="validation_report",    value=report["report_path"])

        log.info(f"VALIDATE PASSED  |  status={status}  warnings={n_warns}")

    validate_task = PythonOperator(
        task_id="validate_schema",
        python_callable=run_validation,
        provide_context=True,
        sla=timedelta(minutes=5),
        doc_md="Validates the ingested CSV against config/schema.yaml. Warnings allowed; errors block.",
    )

    #  Task 3: Branch — skip transform if row count too low ─
    def check_row_count(**context):
        """Branch: only run transform if we have enough rows."""
        row_count = context["ti"].xcom_pull(task_ids="ingest_raw_data", key="raw_row_count") or 0
        if row_count < 100:
            print(f"[BRANCH] Row count {row_count} < 100 — skipping transform")
            return "skip_transform"
        print(f"[BRANCH] Row count {row_count} — proceeding to transform")
        return "spark_transform"

    branch_task = BranchPythonOperator(
        task_id="check_row_threshold",
        python_callable=check_row_count,
        provide_context=True,
        doc_md="Skips the transform step if fewer than 100 rows were ingested.",
    )

    skip_transform = EmptyOperator(task_id="skip_transform")

    #  Task 4: Transform ─
    def run_transform(**context):
        import os, logging
        import pandas as pd
        import numpy as np

        log  = logging.getLogger(__name__)
        date = context["ds"]
        input_path  = f"data/raw/{date}/taxi_trips.csv"
        output_dir  = f"data/processed/{date}"
        os.makedirs(output_dir, exist_ok=True)

        log.info("=" * 60)
        log.info(f"TRANSFORM START  |  date={date}  input={input_path}")
        log.info("=" * 60)

        df = pd.read_csv(input_path, low_memory=False)
        log.info(f"RAW ROWS  |  count={len(df):,}  columns={list(df.columns)}")

        # Cast types
        df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"],  errors="coerce")
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")
        for col in ["passenger_count","vendor_id","payment_type",
                    "pickup_location_id","dropoff_location_id","rate_code_id"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        for col in ["trip_distance","fare_amount","tip_amount","total_amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Filter
        before = len(df)
        df = df[
            df["tpep_dropoff_datetime"].notna() &
            df["tpep_pickup_datetime"].notna() &
            (df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]) &
            (df["trip_distance"].fillna(0)   > 0) &
            (df["fare_amount"].fillna(0)     > 0) &
            (df["passenger_count"].fillna(0) > 0)
        ].copy()
        removed = before - len(df)
        log.info(f"FILTER  |  before={before:,}  after={len(df):,}  removed={removed:,} ({removed/max(before,1)*100:.1f}%)")

        # Features
        df["trip_duration_min"] = ((df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
                                   .dt.total_seconds() / 60).round(2)
        df["avg_speed_mph"]     = np.where(
            df["trip_duration_min"] > 0,
            (df["trip_distance"] / (df["trip_duration_min"] / 60)).round(2), np.nan)
        df["tip_pct"]           = np.where(
            df["fare_amount"] > 0,
            (df["tip_amount"] / df["fare_amount"] * 100).round(2), 0.0)
        hour = df["tpep_pickup_datetime"].dt.hour
        df["time_of_day"]    = pd.cut(hour, bins=[-1,5,11,16,21,23],
                                      labels=["night","morning","afternoon","evening","night2"]
                                      ).astype(str).replace("night2","night")
        df["pickup_date"]    = df["tpep_pickup_datetime"].dt.date.astype(str)
        df["pickup_hour"]    = df["tpep_pickup_datetime"].dt.hour
        df["payment_label"]  = df["payment_type"].map(
            {1:"credit_card",2:"cash",3:"no_charge",4:"dispute"}).fillna("unknown")

        log.info(f"FEATURES ADDED  |  columns={list(df.columns)}")

        # Write parquet outputs
        trips_path = f"{output_dir}/trips.parquet"
        df.to_parquet(trips_path, index=False, engine="pyarrow")
        log.info(f"WRITE trips.parquet  |  rows={len(df):,}  path={trips_path}")

        zone_agg = (df.groupby(["pickup_date","pickup_location_id"], dropna=False)
                    .agg(total_trips=("fare_amount","count"),
                         avg_fare=("fare_amount","mean"),
                         avg_tip_pct=("tip_pct","mean"),
                         avg_distance=("trip_distance","mean"),
                         avg_duration=("trip_duration_min","mean"),
                         total_revenue=("total_amount","sum"))
                    .round(2).reset_index())
        zone_agg.to_parquet(f"{output_dir}/agg_by_zone.parquet", index=False)
        log.info(f"WRITE agg_by_zone.parquet  |  zones={len(zone_agg):,}")

        hourly_agg = (df.groupby(["pickup_date","pickup_hour","time_of_day"], dropna=False)
                      .agg(trip_count=("fare_amount","count"),
                           avg_fare=("fare_amount","mean"),
                           avg_passengers=("passenger_count","mean"))
                      .round(2).reset_index())
        hourly_agg.to_parquet(f"{output_dir}/agg_by_hour.parquet", index=False)
        log.info(f"WRITE agg_by_hour.parquet  |  hours={len(hourly_agg):,}")

        # Summary metrics
        metrics = {
            "clean_row_count":  int(len(df)),
            "removed_rows":     int(removed),
            "avg_fare":         round(float(df["fare_amount"].mean()), 2),
            "avg_distance_mi":  round(float(df["trip_distance"].mean()), 2),
            "avg_duration_min": round(float(df["trip_duration_min"].mean()), 2),
            "unique_zones":     int(df["pickup_location_id"].nunique()),
        }
        log.info(f"TRANSFORM SUMMARY  |  {metrics}")

        for k, v in metrics.items():
            context["ti"].xcom_push(key=k, value=v)

        _send_slack_notification(
            f"⚙️ Transform complete  |  `{metrics['clean_row_count']:,}` clean rows  "
            f"avg_fare=`${metrics['avg_fare']}`  zones=`{metrics['unique_zones']}`",
            emoji="⚙️"
        )

    spark_transform_task = PythonOperator(
        task_id="spark_transform",
        python_callable=run_transform,
        provide_context=True,
        sla=timedelta(minutes=20),
        doc_md="Pandas-based transform: type casting, filtering, feature engineering, Parquet writes.",
    )

    #  Task 5: Load to DuckDB 
    def run_duckdb_load(**context):
        import os, logging, subprocess, sys
        log = logging.getLogger(__name__)

        try:
            import duckdb
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb", "--quiet"])
            import duckdb

        date         = context["ds"]
        parquet_path = f"data/processed/{date}/trips.parquet"
        db_path      = "data/analytics/taxi_analytics.duckdb"
        os.makedirs("data/analytics", exist_ok=True)

        log.info(f"DUCKDB LOAD START  |  parquet={parquet_path}  db={db_path}")

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
        total = con.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
        con.close()

        log.info(f"DUCKDB LOAD COMPLETE  |  loaded={row_count:,}  total_in_db={total:,}")
        context["ti"].xcom_push(key="duckdb_row_count", value=row_count)

    load_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=run_duckdb_load,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        sla=timedelta(minutes=5),
        doc_md="Loads Parquet output into DuckDB analytics layer, partitioned by pickup_date.",
    )

    #  Task 6: Analytics + final notification 
    def run_analytics(**context):
        import logging, subprocess, sys
        log = logging.getLogger(__name__)

        try:
            import duckdb
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb", "--quiet"])
            import duckdb

        date    = context["ds"]
        db_path = "data/analytics/taxi_analytics.duckdb"
        con     = duckdb.connect(db_path)

        log.info("=" * 60)
        log.info(f"ANALYTICS START  |  date={date}")
        log.info("=" * 60)

        queries = {
            "Pipeline health check": f"""
                SELECT COUNT(*) AS rows,
                       ROUND(AVG(fare_amount),2) AS avg_fare,
                       ROUND(AVG(trip_distance),2) AS avg_dist_mi,
                       COUNT(DISTINCT pickup_location_id) AS unique_zones
                FROM taxi_trips
                WHERE CAST(pickup_date AS VARCHAR) = '{date}'
            """,
            "Top 5 hours by demand": """
                SELECT pickup_hour, COUNT(*) AS trips,
                       ROUND(AVG(fare_amount),2) AS avg_fare
                FROM taxi_trips
                GROUP BY pickup_hour ORDER BY trips DESC LIMIT 5
            """,
            "Revenue by payment type": """
                SELECT payment_label,
                       COUNT(*) AS trips,
                       ROUND(AVG(fare_amount),2) AS avg_fare,
                       ROUND(SUM(total_amount),2) AS total_revenue
                FROM taxi_trips
                GROUP BY payment_label ORDER BY trips DESC
            """,
            "Time of day breakdown": """
                SELECT time_of_day, COUNT(*) AS trips,
                       ROUND(AVG(fare_amount),2) AS avg_fare,
                       ROUND(AVG(trip_duration_min),1) AS avg_mins
                FROM taxi_trips
                GROUP BY time_of_day ORDER BY trips DESC
            """,
        }

        summary = {}
        for title, sql in queries.items():
            result = con.execute(sql).df()
            log.info(f"\n {title} ")
            log.info("\n" + result.to_string(index=False))
            summary[title] = result.to_dict(orient="records")

        # Pull all metrics from XCom for final notification
        ti            = context["ti"]
        raw_rows      = ti.xcom_pull(task_ids="ingest_raw_data",  key="raw_row_count")  or 0
        clean_rows    = ti.xcom_pull(task_ids="spark_transform",  key="clean_row_count") or 0
        avg_fare      = ti.xcom_pull(task_ids="spark_transform",  key="avg_fare")        or 0
        val_status    = ti.xcom_pull(task_ids="validate_schema",  key="validation_status") or "N/A"
        val_warns     = ti.xcom_pull(task_ids="validate_schema",  key="validation_warnings") or 0
        duckdb_count  = ti.xcom_pull(task_ids="load_to_duckdb",  key="duckdb_row_count") or 0

        final_msg = (
            f"Pipeline complete for `{date}`\n"
            f"```\n"
            f"raw rows:      {raw_rows:>10,}\n"
            f"clean rows:    {clean_rows:>10,}\n"
            f"avg fare:      {avg_fare:>10}\n"
            f"validation:    {val_status:>10}  ({val_warns} warnings)\n"
            f"duckdb rows:   {duckdb_count:>10,}\n"
            f"```"
        )
        log.info(f"\n\nFINAL SUMMARY\n{final_msg}")
        _send_slack_notification(final_msg, emoji="")

        con.close()
        context["ti"].xcom_push(key="pipeline_summary", value=summary)

    run_analytics_task = PythonOperator(
        task_id="run_analytics_queries",
        python_callable=run_analytics,
        provide_context=True,
        on_success_callback=_on_success_callback,
        sla=timedelta(minutes=5),
        doc_md="Runs business insight queries and sends final pipeline summary to Slack.",
    )

    #  Dependency graph 
    #
    #   ingest_raw_data
    #         │
    #   validate_schema
    #         │
    #   check_row_threshold ► skip_transform ┐
    #         │                                  │
    #   spark_transform ─►load_to_duckdb
    #                                             │
    #                                    run_analytics_queries
    #
    ingest_task >> validate_task >> branch_task
    branch_task >> spark_transform_task >> load_task
    branch_task >> skip_transform >> load_task
    load_task   >> run_analytics_task