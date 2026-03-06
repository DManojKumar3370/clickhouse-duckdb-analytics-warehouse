-- ============================================================================
-- OPTIMIZED SCHEMA - With Partitioning, Sorting Keys & Compression
-- ============================================================================
-- Purpose: Define optimized table with partitioning and sorting keys
-- Optimization Strategies:
--   1. PARTITION BY toYYYYMM - Divides data by month for faster queries
--   2. ORDER BY - Orders data on disk by frequently queried columns
--   3. CODEC - Compresses data to reduce storage

-- ClickHouse Optimized Table
CREATE TABLE IF NOT EXISTS taxi_db.taxi_data_optimized (
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
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (pickup_location, dropoff_location, pickup_datetime)
CODEC(ZSTD(10));

-- Populate optimized table from baseline
INSERT INTO taxi_db.taxi_data_optimized
SELECT * FROM taxi_db.taxi_data;
