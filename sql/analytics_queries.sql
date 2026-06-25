-- sql/analytics_queries.sql
-- Business insight queries for the NYC Taxi analytics layer.
-- Run via: python scripts/run_analytics.py --date 2024-01-15


-- 1. Top 10 pickup zones by total trips
SELECT
    pickup_zone,
    pickup_borough,
    SUM(total_trips)              AS total_trips,
    ROUND(SUM(total_revenue), 2)  AS total_revenue,
    ROUND(AVG(avg_fare), 2)       AS avg_fare
FROM agg_by_zone
GROUP BY pickup_zone, pickup_borough
ORDER BY total_trips DESC
LIMIT 10;


-- 2. Hourly demand heatmap
SELECT
    pickup_hour,
    time_of_day,
    SUM(trip_count)                              AS trips,
    ROUND(AVG(avg_fare), 2)                      AS avg_fare,
    ROUND(SUM(trip_count) * 100.0 /
          SUM(SUM(trip_count)) OVER (), 1)       AS pct_of_daily_trips
FROM agg_by_hour
GROUP BY pickup_hour, time_of_day
ORDER BY pickup_hour;


-- 3. Payment method breakdown
SELECT
    payment_label,
    SUM(trips)                    AS total_trips,
    ROUND(AVG(avg_tip_pct), 2)    AS avg_tip_pct,
    ROUND(SUM(trips) * 100.0 /
          SUM(SUM(trips)) OVER (), 1) AS market_share_pct
FROM agg_payment
GROUP BY payment_label
ORDER BY total_trips DESC;


-- 4. Borough-level revenue leaderboard
SELECT
    pickup_borough,
    SUM(total_trips)               AS total_trips,
    ROUND(SUM(total_revenue), 2)   AS total_revenue,
    ROUND(AVG(avg_fare), 2)        AS avg_fare,
    ROUND(AVG(avg_distance_mi), 2) AS avg_distance_mi,
    ROUND(AVG(avg_speed_mph), 1)   AS avg_speed_mph
FROM agg_by_zone
WHERE pickup_borough IS NOT NULL
GROUP BY pickup_borough
ORDER BY total_revenue DESC;


-- 5. Long-haul vs short-haul split
SELECT
    CASE
        WHEN trip_distance < 1   THEN 'under_1mi'
        WHEN trip_distance < 3   THEN '1_to_3mi'
        WHEN trip_distance < 10  THEN '3_to_10mi'
        ELSE 'over_10mi'
    END                              AS distance_bucket,
    COUNT(*)                         AS trips,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(AVG(tip_pct), 2)           AS avg_tip_pct,
    ROUND(AVG(trip_duration_min), 1) AS avg_duration_min
FROM taxi_trips
GROUP BY distance_bucket
ORDER BY MIN(trip_distance);


-- 6. Anomaly detection — suspiciously high-fare trips (z-score > 3)
WITH stats AS (
    SELECT
        AVG(fare_amount)    AS mean_fare,
        STDDEV(fare_amount) AS std_fare
    FROM taxi_trips
)
SELECT
    tpep_pickup_datetime,
    pickup_zone,
    dropoff_zone,
    trip_distance,
    fare_amount,
    total_amount,
    ROUND((fare_amount - mean_fare) / std_fare, 2) AS z_score
FROM taxi_trips, stats
WHERE fare_amount > mean_fare + 3 * std_fare
ORDER BY fare_amount DESC
LIMIT 20;


-- 7. Pipeline health check — run after every pipeline execution
SELECT
    pickup_date,
    COUNT(*)                      AS row_count,
    ROUND(AVG(fare_amount), 2)    AS avg_fare,
    MIN(tpep_pickup_datetime)     AS earliest_pickup,
    MAX(tpep_pickup_datetime)     AS latest_pickup,
    COUNT(DISTINCT pickup_zone)   AS unique_zones,
    SUM(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END) AS zero_fare_count
FROM taxi_trips
GROUP BY pickup_date
ORDER BY pickup_date DESC;
