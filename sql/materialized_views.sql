-- ============================================================================
-- MATERIALIZED VIEWS - Pre-Aggregated Data for Query Acceleration
-- ============================================================================
-- Purpose: Pre-compute expensive aggregations for faster query results
-- Strategy: Store results of complex queries in optimized tables

-- Materialized View 1: Daily Location Revenue Summary
-- Pre-computes QUERY 1 results (most expensive query)
-- Benefit: Query 1 runs in milliseconds instead of seconds
CREATE MATERIALIZED VIEW IF NOT EXISTS taxi_db.mv_daily_location_revenue
ENGINE = SummingMergeTree()
ORDER BY (pickup_date, pickup_location)
PARTITION BY toYYYYMM(pickup_date)
AS
SELECT 
    toDate(pickup_datetime) as pickup_date,
    pickup_location,
    COUNT(*) as trip_count,
    SUM(fare_amount) as total_fare_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare_amount,
    SUM(total_amount) as total_revenue
FROM taxi_data_optimized
GROUP BY toDate(pickup_datetime), pickup_location;

-- Materialized View 2: Monthly Route Performance Summary
-- Pre-computes QUERY 2 & 4 results (route analysis)
-- Benefit: Route queries execute without full table scan
CREATE MATERIALIZED VIEW IF NOT EXISTS taxi_db.mv_monthly_routes
ENGINE = SummingMergeTree()
ORDER BY (year_month, route_frequency DESC)
PARTITION BY year_month
AS
SELECT 
    toYYYYMM(pickup_datetime) as year_month,
    pickup_location,
    dropoff_location,
    COUNT(*) as route_frequency,
    SUM(total_amount) as total_route_revenue,
    AVG(trip_distance) as avg_distance,
    COUNT(*) as trip_count
FROM taxi_data_optimized
GROUP BY toYYYYMM(pickup_datetime), pickup_location, dropoff_location;

-- Materialized View 3: Payment Type Daily Summary
-- Pre-computes QUERY 5 results (payment analytics)
-- Benefit: Payment queries return results instantly
CREATE MATERIALIZED VIEW IF NOT EXISTS taxi_db.mv_payment_summary
ENGINE = SummingMergeTree()
ORDER BY (summary_date, payment_type)
PARTITION BY toYYYYMM(summary_date)
AS
SELECT 
    toDate(pickup_datetime) as summary_date,
    payment_type,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction_amount,
    SUM(tip_amount) as total_tips
FROM taxi_data_optimized
WHERE payment_type IS NOT NULL
GROUP BY toDate(pickup_datetime), payment_type;
