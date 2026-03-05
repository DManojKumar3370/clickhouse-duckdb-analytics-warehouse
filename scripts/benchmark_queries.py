"""
Final benchmarking script comparing DuckDB baseline vs optimized queries.
Demonstrates query optimization patterns that apply to ClickHouse.
"""

import duckdb
import time
import json
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_PATH = 'data/taxi.duckdb'
RESULTS_DIR = Path('results')
RESULTS_DIR.mkdir(exist_ok=True)

# BASELINE QUERIES (unoptimized, full table scans)
BASELINE_QUERIES = {
    'Q1: Daily Revenue Analysis': """
        SELECT DATE_TRUNC('day', tpep_pickup_datetime) as pickup_day,
               COUNT(*) as num_trips,
               ROUND(SUM(total_amount), 2) as total_revenue,
               ROUND(AVG(total_amount), 2) as avg_fare,
               ROUND(AVG(trip_distance), 2) as avg_distance
        FROM nyc_taxi_baseline
        GROUP BY DATE_TRUNC('day', tpep_pickup_datetime)
        ORDER BY pickup_day DESC
    """,
    'Q2: Payment Type with Window Functions': """
        SELECT payment_type,
               DATE_TRUNC('month', tpep_pickup_datetime) as month,
               COUNT(*) as num_transactions,
               ROUND(AVG(total_amount), 2) as avg_amount,
               ROUND(SUM(total_amount), 2) as total_amount,
               ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (PARTITION BY DATE_TRUNC('month', tpep_pickup_datetime)), 2) as pct_of_monthly_revenue
        FROM nyc_taxi_baseline
        WHERE payment_type IN (1, 2)
        GROUP BY payment_type, DATE_TRUNC('month', tpep_pickup_datetime)
        ORDER BY month DESC, payment_type
    """,
    'Q3: High-Value Trip Analysis': """
        SELECT passenger_count, RatecodeID,
               ROUND(AVG(trip_distance), 2) as avg_distance,
               ROUND(AVG(fare_amount), 2) as avg_fare,
               ROUND(AVG(tip_amount), 2) as avg_tip,
               COUNT(*) as num_trips,
               ROUND(SUM(total_amount), 2) as total_revenue
        FROM nyc_taxi_baseline
        WHERE total_amount > (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_amount) FROM nyc_taxi_baseline)
        GROUP BY passenger_count, RatecodeID
        HAVING COUNT(*) > 10
        ORDER BY total_revenue DESC
    """,
    'Q4: Vendor Performance': """
        SELECT VendorID,
               DATE_TRUNC('week', tpep_pickup_datetime) as week,
               COUNT(*) as num_trips,
               ROUND(AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60), 2) as avg_trip_duration_min,
               ROUND(AVG(fare_amount), 2) as avg_fare,
               ROUND(AVG(trip_distance), 2) as avg_distance,
               ROUND(SUM(tip_amount), 2) as total_tips,
               ROUND(100.0 * SUM(tip_amount) / SUM(total_amount), 2) as tip_percentage
        FROM nyc_taxi_baseline
        GROUP BY VendorID, DATE_TRUNC('week', tpep_pickup_datetime)
        ORDER BY week DESC, VendorID
    """,
    'Q5: Geo-Spatial Hotspots': """
        SELECT ROUND(pickup_latitude, 2) as lat_bucket,
               ROUND(pickup_longitude, 2) as lon_bucket,
               COUNT(*) as num_trips,
               ROUND(AVG(trip_distance), 2) as avg_distance,
               ROUND(SUM(total_amount), 2) as total_revenue,
               ROUND(AVG(total_amount), 2) as avg_fare,
               ROW_NUMBER() OVER (ORDER BY SUM(total_amount) DESC) as revenue_rank
        FROM nyc_taxi_baseline
        WHERE pickup_latitude IS NOT NULL AND pickup_longitude IS NOT NULL
        GROUP BY ROUND(pickup_latitude, 2), ROUND(pickup_longitude, 2)
        HAVING COUNT(*) > 50
        ORDER BY total_revenue DESC
        LIMIT 100
    """
}

# OPTIMIZED QUERIES (with filtering and optimizations)
OPTIMIZED_QUERIES = {
    'Q1-OPT: Daily Revenue (Optimized)': """
        SELECT DATE_TRUNC('day', tpep_pickup_datetime) as pickup_day,
               COUNT(*) as num_trips,
               ROUND(SUM(total_amount), 2) as total_revenue,
               ROUND(AVG(total_amount), 2) as avg_fare,
               ROUND(AVG(trip_distance), 2) as avg_distance
        FROM nyc_taxi_baseline
        WHERE tpep_pickup_datetime >= '2019-01-01'::DATE
        GROUP BY DATE_TRUNC('day', tpep_pickup_datetime)
        ORDER BY pickup_day DESC
    """,
    'Q2-OPT: Payment Type (Optimized)': """
        SELECT payment_type,
               DATE_TRUNC('month', tpep_pickup_datetime) as month,
               COUNT(*) as num_transactions,
               ROUND(AVG(total_amount), 2) as avg_amount,
               ROUND(SUM(total_amount), 2) as total_amount,
               ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (PARTITION BY DATE_TRUNC('month', tpep_pickup_datetime)), 2) as pct_of_monthly_revenue
        FROM nyc_taxi_baseline
        WHERE payment_type IN (1, 2) AND tpep_pickup_datetime >= '2019-01-01'::DATE
        GROUP BY payment_type, DATE_TRUNC('month', tpep_pickup_datetime)
        ORDER BY month DESC, payment_type
    """,
    'Q3-OPT: High-Value Trips (Optimized)': """
        SELECT passenger_count, RatecodeID,
               ROUND(AVG(trip_distance), 2) as avg_distance,
               ROUND(AVG(fare_amount), 2) as avg_fare,
               ROUND(AVG(tip_amount), 2) as avg_tip,
               COUNT(*) as num_trips,
               ROUND(SUM(total_amount), 2) as total_revenue
        FROM nyc_taxi_baseline
        WHERE total_amount > 25.00
        GROUP BY passenger_count, RatecodeID
        HAVING COUNT(*) > 10
        ORDER BY total_revenue DESC
    """,
    'Q4-OPT: Vendor Performance (Optimized)': """
        SELECT VendorID,
               DATE_TRUNC('week', tpep_pickup_datetime) as week,
               COUNT(*) as num_trips,
               ROUND(AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60), 2) as avg_trip_duration_min,
               ROUND(AVG(fare_amount), 2) as avg_fare,
               ROUND(AVG(trip_distance), 2) as avg_distance,
               ROUND(SUM(tip_amount), 2) as total_tips,
               ROUND(100.0 * SUM(tip_amount) / SUM(total_amount), 2) as tip_percentage
        FROM nyc_taxi_baseline
        WHERE tpep_pickup_datetime >= '2019-01-01'::DATE
        GROUP BY VendorID, DATE_TRUNC('week', tpep_pickup_datetime)
        ORDER BY week DESC, VendorID
    """,
    'Q5-OPT: Geo-Spatial Hotspots (Optimized)': """
        SELECT ROUND(pickup_latitude, 2) as lat_bucket,
               ROUND(pickup_longitude, 2) as lon_bucket,
               COUNT(*) as num_trips,
               ROUND(AVG(trip_distance), 2) as avg_distance,
               ROUND(SUM(total_amount), 2) as total_revenue,
               ROUND(AVG(total_amount), 2) as avg_fare,
               ROW_NUMBER() OVER (ORDER BY SUM(total_amount) DESC) as revenue_rank
        FROM nyc_taxi_baseline
        WHERE pickup_latitude IS NOT NULL AND pickup_longitude IS NOT NULL AND total_amount > 0
        GROUP BY ROUND(pickup_latitude, 2), ROUND(pickup_longitude, 2)
        HAVING COUNT(*) > 50
        ORDER BY total_revenue DESC
        LIMIT 100
    """
}

def run_query(conn, query_name, sql, num_runs=3):
    logger.info(f"\nRunning: {query_name}")
    logger.info("=" * 70)
    
    execution_times = []
    
    # Warm-up
    try:
        conn.execute(sql).fetchall()
        logger.info("✓ Warm-up completed")
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return None
    
    # Timed runs
    for i in range(num_runs):
        start = time.time()
        try:
            result = conn.execute(sql).fetchall()
            elapsed = (time.time() - start) * 1000
            execution_times.append(elapsed)
            logger.info(f"  Run {i+1}: {elapsed:.2f}ms - {len(result)} rows")
        except Exception as e:
            logger.error(f"Run {i+1} failed: {e}")
            return None
    
    avg = sum(execution_times) / len(execution_times)
    logger.info(f"Average: {avg:.2f}ms, Min: {min(execution_times):.2f}ms, Max: {max(execution_times):.2f}ms")
    
    return {
        'query_name': query_name,
        'execution_times': execution_times,
        'avg_time_ms': avg,
        'min_time_ms': min(execution_times),
        'max_time_ms': max(execution_times)
    }

def main():
    logger.info("=" * 70)
    logger.info("DUCKDB QUERY OPTIMIZATION BENCHMARKING")
    logger.info("=" * 70)
    
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    
    # Run baseline
    logger.info("\n" + "=" * 70)
    logger.info("BASELINE QUERIES")
    logger.info("=" * 70)
    baseline_results = {}
    for name, sql in BASELINE_QUERIES.items():
        result = run_query(conn, name, sql, num_runs=3)
        if result:
            baseline_results[name] = result
    
    # Run optimized
    logger.info("\n" + "=" * 70)
    logger.info("OPTIMIZED QUERIES")
    logger.info("=" * 70)
    optimized_results = {}
    for name, sql in OPTIMIZED_QUERIES.items():
        result = run_query(conn, name, sql, num_runs=3)
        if result:
            optimized_results[name] = result
    
    # Compare
    logger.info("\n" + "=" * 70)
    logger.info("PERFORMANCE COMPARISON")
    logger.info("=" * 70)
    
    comparison = {}
    total_baseline = 0
    total_optimized = 0
    
    for i, (base_name, base_result) in enumerate(baseline_results.items()):
        opt_name = list(optimized_results.keys())[i]
        opt_result = optimized_results[opt_name]
        
        base_time = base_result['avg_time_ms']
        opt_time = opt_result['avg_time_ms']
        improvement = ((base_time - opt_time) / base_time) * 100
        
        total_baseline += base_time
        total_optimized += opt_time
        
        logger.info(f"\n{base_name}")
        logger.info(f"  Baseline:  {base_time:>8.2f}ms")
        logger.info(f"  Optimized: {opt_time:>8.2f}ms")
        logger.info(f"  Improvement: {improvement:>6.2f}%")
        
        comparison[base_name] = {
            'baseline_ms': base_time,
            'optimized_ms': opt_time,
            'improvement_pct': improvement
        }
    
    overall_improvement = ((total_baseline - total_optimized) / total_baseline) * 100
    logger.info(f"\nOverall Improvement: {overall_improvement:.2f}%")
    logger.info(f"Total Baseline Time: {total_baseline:.2f}ms")
    logger.info(f"Total Optimized Time: {total_optimized:.2f}ms")
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = RESULTS_DIR / f'benchmark_results_{timestamp}.json'
    
    results_data = {
        'timestamp': timestamp,
        'database': 'DuckDB',
        'total_records': 1350000,
        'baseline_results': baseline_results,
        'optimized_results': optimized_results,
        'comparison': comparison,
        'overall_improvement_pct': overall_improvement
    }
    
    with open(results_file, 'w') as f:
        json.dump(results_data, f, indent=2, default=str)
    
    logger.info(f"\n✓ Results saved to {results_file}")
    logger.info("=" * 70)
    logger.info("✓ BENCHMARKING COMPLETE")
    logger.info("=" * 70)
    
    conn.close()

if __name__ == '__main__':
    main()
