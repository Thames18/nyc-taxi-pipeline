-- sql/create_tables.sql
-- DDL for the NYC Taxi analytics layer (DuckDB compatible)
-- Run once to initialize the analytics database schema.

CREATE TABLE IF NOT EXISTS taxi_trips (
    vendor_id               INTEGER,
    tpep_pickup_datetime    TIMESTAMP NOT NULL,
    tpep_dropoff_datetime   TIMESTAMP NOT NULL,
    passenger_count         INTEGER,
    trip_distance           DOUBLE,
    fare_amount             DOUBLE,
    tip_amount              DOUBLE,
    total_amount            DOUBLE,
    payment_type            INTEGER,
    payment_label           VARCHAR,
    pickup_location_id      INTEGER,
    dropoff_location_id     INTEGER,
    pickup_zone             VARCHAR,
    pickup_borough          VARCHAR,
    dropoff_zone            VARCHAR,
    dropoff_borough         VARCHAR,
    trip_duration_min       DOUBLE,
    avg_speed_mph           DOUBLE,
    tip_pct                 DOUBLE,
    time_of_day             VARCHAR,
    pickup_date             DATE NOT NULL,
    pickup_hour             INTEGER
);

CREATE TABLE IF NOT EXISTS agg_by_zone (
    pickup_date             DATE,
    pickup_zone             VARCHAR,
    pickup_borough          VARCHAR,
    total_trips             INTEGER,
    avg_fare                DOUBLE,
    avg_tip_pct             DOUBLE,
    avg_distance_mi         DOUBLE,
    avg_duration_min        DOUBLE,
    avg_speed_mph           DOUBLE,
    total_revenue           DOUBLE
);

CREATE TABLE IF NOT EXISTS agg_by_hour (
    pickup_date             DATE,
    pickup_hour             INTEGER,
    time_of_day             VARCHAR,
    trip_count              INTEGER,
    avg_fare                DOUBLE,
    avg_passengers          DOUBLE
);

CREATE TABLE IF NOT EXISTS agg_payment (
    pickup_date             DATE,
    payment_label           VARCHAR,
    trips                   INTEGER,
    avg_tip_pct             DOUBLE,
    pct_of_total            DOUBLE
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                  VARCHAR PRIMARY KEY,
    execution_date          DATE,
    started_at              TIMESTAMP,
    completed_at            TIMESTAMP,
    raw_row_count           INTEGER,
    clean_row_count         INTEGER,
    validation_status       VARCHAR,
    status                  VARCHAR
);
