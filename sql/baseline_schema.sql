-- ============================================================================
-- BASELINE SCHEMA (Unoptimized) - For Performance Comparison
-- ============================================================================
-- Purpose: Define initial table schemas without optimization
-- Used for: Establishing baseline performance metrics

-- DuckDB Baseline Table
CREATE TABLE IF NOT EXISTS taxi_data (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DECIMAL(10, 2),
    pickup_location VARCHAR,
    dropoff_location VARCHAR,
    fare_amount DECIMAL(10, 2),
    extra DECIMAL(10, 2),
    mta_tax DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2),
    tolls_amount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    payment_type VARCHAR,
    trip_type VARCHAR
);

-- ClickHouse Baseline Table (Unoptimized - Simple ORDER BY only)
CREATE TABLE IF NOT EXISTS taxi_db.taxi_data (
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count UInt8,
    trip_distance Decimal(10, 2),
    pickup_location String,
    dropoff_location String,
    fare_amount Decimal(10, 2),
    extra Decimal(10, 2),
    mta_tax Decimal(10, 2),
    tip_amount Decimal(10, 2),
    tolls_amount Decimal(10, 2),
    total_amount Decimal(10, 2),
    payment_type String,
    trip_type String
)
ENGINE = MergeTree()
ORDER BY (pickup_datetime);
