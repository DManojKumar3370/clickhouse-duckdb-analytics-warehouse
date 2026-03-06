-- ============================================================================
-- 5 COMPLEX ANALYTICAL QUERIES FOR BENCHMARKING
-- ============================================================================
-- Purpose: Test queries used for performance benchmarking
-- Complexity: GROUP BY, JOINs, Window Functions, Aggregations

-- QUERY 1: Daily Revenue Aggregation by Location
-- Purpose: Business metric - daily revenue breakdown
-- Complexity: GROUP BY with date functions, multiple aggregations
SELECT 
    pickup_location,
    toDate(pickup_datetime) as pickup_date,
    COUNT(*) as trip_count,
    SUM(fare_amount) as total_fare_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare_amount,
    SUM(total_amount) as total_revenue
FROM taxi_data_optimized
WHERE toDate(pickup_datetime) >= today() - 90
GROUP BY pickup_location, toDate(pickup_datetime)
ORDER BY pickup_date DESC, total_revenue DESC;

-- QUERY 2: Popular Routes Analysis (Location Pairs)
-- Purpose: Identify most profitable routes
-- Complexity: Self-join style aggregation with multiple GROUP BY levels
SELECT 
    pickup_location,
    dropoff_location,
    COUNT(*) as route_frequency,
    AVG(fare_amount) as avg_fare,
    SUM(total_amount) as total_route_revenue,
    AVG(trip_distance) as avg_route_distance
FROM taxi_data_optimized
WHERE toDate(pickup_datetime) >= today() - 60
GROUP BY pickup_location, dropoff_location
HAVING COUNT(*) > 10
ORDER BY route_frequency DESC
LIMIT 100;

-- QUERY 3: Passenger Count Trends (Window Function)
-- Purpose: Analyze trip patterns by passenger count
-- Complexity: Window functions with PARTITION BY, ROW aggregations
SELECT 
    toDate(pickup_datetime) as trip_date,
    passenger_count,
    COUNT(*) as daily_trips,
    AVG(fare_amount) as avg_fare,
    SUM(total_amount) as daily_revenue,
    ROW_NUMBER() OVER (PARTITION BY toDate(pickup_datetime) ORDER BY COUNT(*) DESC) as rank_by_trips
FROM taxi_data_optimized
GROUP BY toDate(pickup_datetime), passenger_count
ORDER BY trip_date DESC, daily_trips DESC;

-- QUERY 4: Multi-Level Monthly Aggregation (Year-Month Analysis)
-- Purpose: Analyze trends across months
-- Complexity: Multiple GROUP BY levels with nested aggregations
SELECT 
    toYYYYMM(pickup_datetime) as year_month,
    pickup_location,
    dropoff_location,
    COUNT(*) as total_trips,
    SUM(total_amount) as monthly_revenue,
    AVG(trip_distance) as avg_distance,
    MAX(total_amount) as max_transaction,
    MIN(total_amount) as min_transaction
FROM taxi_data_optimized
GROUP BY toYYYYMM(pickup_datetime), pickup_location, dropoff_location
ORDER BY year_month DESC, monthly_revenue DESC;

-- QUERY 5: Payment Type Revenue Analysis with Time Series
-- Purpose: Revenue breakdown by payment method and time period
-- Complexity: Multiple aggregation functions with conditional grouping
SELECT 
    toYYYYMM(pickup_datetime) as month,
    payment_type,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction_amount,
    MAX(total_amount) as max_transaction,
    MIN(total_amount) as min_transaction,
    SUM(tip_amount) as total_tips
FROM taxi_data_optimized
WHERE payment_type IS NOT NULL
GROUP BY toYYYYMM(pickup_datetime), payment_type
ORDER BY month DESC, total_revenue DESC;
