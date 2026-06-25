"""
transforms/spark_transform.py
================================
PySpark transformation job for NYC Taxi trip data.

Transformations applied:
  1. Cast and clean column types
  2. Derive new columns (trip duration, speed, time-of-day bucket)
  3. Join with taxi zone lookup table
  4. Aggregate metrics per pickup zone and per hour
  5. Write cleaned trips to Parquet (partitioned by pickup_date)
  6. Write aggregations to separate Parquet files

Run locally:
    spark-submit --master local[*] transforms/spark_transform.py \
        --date 2024-01-15 \
        --input data/raw/2024-01-15/taxi_trips.csv \
        --output data/processed/2024-01-15/
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, TimestampType, StringType
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


#   Spark session factory  
def create_spark_session(app_name: str = "nyc-taxi-transform") -> SparkSession:
    """Create a local Spark session configured for this pipeline."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")       # Keep low for local dev
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")       # AQE for auto-optimization
        .getOrCreate()
    )


#   Step 1: Ingest & cast                  
def read_and_cast(spark: SparkSession, input_path: str):
    """Read raw CSV and enforce schema types."""
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # Explicit schema → no surprises
        .csv(input_path)
    )

    return (
        df
        .withColumn("vendor_id",           F.col("vendor_id").cast(IntegerType()))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime",F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count",     F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance",       F.col("trip_distance").cast(DoubleType()))
        .withColumn("fare_amount",         F.col("fare_amount").cast(DoubleType()))
        .withColumn("tip_amount",          F.col("tip_amount").cast(DoubleType()))
        .withColumn("total_amount",        F.col("total_amount").cast(DoubleType()))
        .withColumn("payment_type",        F.col("payment_type").cast(IntegerType()))
        .withColumn("pickup_location_id",  F.col("pickup_location_id").cast(IntegerType()))
        .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast(IntegerType()))
    )


#   Step 2: Filter bad rows                 ─
def filter_invalid_rows(df):
    """Remove rows that would corrupt downstream analytics."""
    return (
        df
        # Dropoff must come after pickup
        .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        # Sensible distance and fare bounds
        .filter(F.col("trip_distance").between(0.01, 500))
        .filter(F.col("fare_amount").between(0, 1000))
        .filter(F.col("total_amount") >= 0)
        # At least one passenger
        .filter(F.col("passenger_count").between(1, 9))
        # Known vendor IDs only
        .filter(F.col("vendor_id").isin([1, 2]))
    )


#   Step 3: Derive features                 ─
def derive_features(df):
    """Add engineered columns useful for analytics."""
    return (
        df
        # Trip duration in minutes
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60.0
        )
        # Average speed mph (avoid divide-by-zero)
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)
            ).otherwise(None)
        )
        # Tip percentage of fare
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                (F.col("tip_amount") / F.col("fare_amount")) * 100
            ).otherwise(0.0)
        )
        # Time-of-day bucket (for demand pattern analysis)
        .withColumn(
            "time_of_day",
            F.when(F.hour("tpep_pickup_datetime").between(6, 11),  "morning")
             .when(F.hour("tpep_pickup_datetime").between(12, 16), "afternoon")
             .when(F.hour("tpep_pickup_datetime").between(17, 21), "evening")
             .otherwise("night")
        )
        # Date and hour partitions
        .withColumn("pickup_date",  F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour",  F.hour("tpep_pickup_datetime"))
        # Payment type label
        .withColumn(
            "payment_label",
            F.when(F.col("payment_type") == 1, "credit_card")
             .when(F.col("payment_type") == 2, "cash")
             .when(F.col("payment_type") == 3, "no_charge")
             .when(F.col("payment_type") == 4, "dispute")
             .otherwise("unknown")
        )
    )


#   Step 4: Zone join   
def join_zone_lookup(spark: SparkSession, df):
    """
    Join with TLC taxi zone lookup table to get human-readable zone names.
    Falls back gracefully if the lookup file isn't present.
    """
    zone_path = "data/reference/taxi_zone_lookup.csv"
    if not Path(zone_path).exists():
        logger.warning(f"Zone lookup not found at {zone_path} — skipping join")
        return (
            df
            .withColumn("pickup_zone", F.lit("unknown"))
            .withColumn("dropoff_zone", F.lit("unknown"))
        )

    zones = (
        spark.read
        .option("header", "true")
        .csv(zone_path)
        .select(
            F.col("LocationID").cast(IntegerType()).alias("location_id"),
            F.col("Zone").alias("zone_name"),
            F.col("Borough").alias("borough"),
        )
    )

    return (
        df
        .join(zones.alias("pickup_z"),
              df.pickup_location_id == F.col("pickup_z.location_id"), "left")
        .withColumnRenamed("zone_name", "pickup_zone")
        .withColumnRenamed("borough", "pickup_borough")
        .drop("location_id")
        .join(zones.alias("drop_z"),
              df.dropoff_location_id == F.col("drop_z.location_id"), "left")
        .withColumnRenamed("zone_name", "dropoff_zone")
        .withColumnRenamed("borough", "dropoff_borough")
        .drop("location_id")
    )


#   Step 5: Aggregations  
def build_zone_aggregates(df):
    """Per-zone daily aggregation — key business metric."""
    return (
        df.groupBy("pickup_date", "pickup_zone", "pickup_borough")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            F.round(F.avg("avg_speed_mph"), 1).alias("avg_speed_mph"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .orderBy("total_trips", ascending=False)
    )


def build_hourly_demand(df):
    """Hourly demand heatmap — useful for surge pricing models."""
    return (
        df.groupBy("pickup_date", "pickup_hour", "time_of_day")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("passenger_count"), 1).alias("avg_passengers"),
        )
        .orderBy("pickup_hour")
    )


def build_payment_breakdown(df):
    """Payment method analysis — cash vs card trends."""
    total_trips = df.count()
    return (
        df.groupBy("pickup_date", "payment_label")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
        .withColumn(
            "pct_of_total",
            F.round(F.col("trips") / total_trips * 100, 1)
        )
    )


#   Step 6: Write outputs                  
def write_parquet(df, output_path: str, partition_by: list[str] = None) -> int:
    """Write DataFrame to Parquet with optional partitioning."""
    writer = df.write.mode("overwrite").format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(output_path)
    return df.count()


#   Main        
def main(date: str, input_path: str, output_path: str):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Starting Spark transform for date={date}")

    # Pipeline steps
    df_raw      = read_and_cast(spark, input_path)
    df_filtered = filter_invalid_rows(df_raw)
    df_featured = derive_features(df_filtered)
    df_joined   = join_zone_lookup(spark, df_featured)

    # Cache because we build 3 aggregations from it
    df_joined.cache()

    # Write cleaned trips
    trips_out = f"{output_path}/trips.parquet"
    n_trips = write_parquet(df_joined, trips_out)
    logger.info(f"Wrote {n_trips:,} cleaned trip rows → {trips_out}")

    # Write aggregations
    zone_agg   = build_zone_aggregates(df_joined)
    hourly_agg = build_hourly_demand(df_joined)
    pay_agg    = build_payment_breakdown(df_joined)

    write_parquet(zone_agg,   f"{output_path}/agg_by_zone.parquet")
    write_parquet(hourly_agg, f"{output_path}/agg_by_hour.parquet")
    write_parquet(pay_agg,    f"{output_path}/agg_payment.parquet")

    df_joined.unpersist()
    spark.stop()

    logger.info("Spark transform complete  ")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True, help="Execution date YYYY-MM-DD")
    parser.add_argument("--input",  required=True, help="Raw CSV path")
    parser.add_argument("--output", required=True, help="Output directory")
    args = parser.parse_args()

    main(date=args.date, input_path=args.input, output_path=args.output)
