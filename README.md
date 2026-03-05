# NYC Taxi Query Optimization with DuckDB and ClickHouse

## Project Overview

This project demonstrates query performance optimization techniques for analytical data warehouses using DuckDB and ClickHouse. Using 450K synthetic NYC Taxi records (3 months), the project achieves 35% average query performance improvement through schema optimization and partitioning strategies.

## Results Summary

✅ 5 Complex Analytical Queries  
✅ Baseline & Optimized Benchmarking  
✅ 450,000 Records (Jan-Mar 2019)  
✅ Performance Metrics in `results/`  
✅ Production-Ready Scripts

## Dataset

- **Format**: Parquet (columnar)
- **Records**: 450,000 (150K per month)
- **Columns**: 18 (vendor, datetime, fare, payment, location, etc.)
- **Date Range**: 2019-01-01 to 2019-03-31

## Project Structure

```
clickhouse-duckdb-analytics-warehouse/
├── data/
│   ├── nyc_taxi_2019_01.parquet
│   ├── nyc_taxi_2019_02.parquet
│   ├── nyc_taxi_2019_03.parquet
│   └── taxi.duckdb
├── scripts/
│   ├── generate_nyc_taxi_data.py
│   ├── ingest_data.py
│   └── benchmark_queries.py
├── sql/
│   └── optimization_strategy.md
├── results/
│   └── benchmark_results_*.json
├── requirements.txt
└── README.md
```

## Quick Start

```bash
pip install -r requirements.txt
python scripts/generate_nyc_taxi_data.py
python scripts/ingest_data.py --verify
python scripts/benchmark_queries.py
```

## Queries Implemented

| Query | Technique | Improvement |
|-------|-----------|------------|
| Q1: Daily Revenue | GROUP BY aggregations | 38% |
| Q2: Payment Analysis | Window functions | 28% |
| Q3: High-Value Trips | Percentile filtering | 42% |
| Q4: Vendor Performance | Date arithmetic | 31% |
| Q5: Geo-Spatial Hotspots | Spatial bucketing | 38% |

## Optimization Techniques

- **Partitioning**: By month for 20-25% improvement
- **Sorting Keys**: (datetime, passenger_count, payment_type) for 15-20% improvement
- **Query Filtering**: Explicit date filters for 10-15% improvement

## Performance Results

**Average Improvement: 35% latency reduction**

## Technologies

- DuckDB: Analytical database
- Python: Data pipeline scripting
- Parquet: Columnar storage
- Docker: Infrastructure

## Author

Manoj Kumar Doddi  
Data Engineering Professional  
March 2026
