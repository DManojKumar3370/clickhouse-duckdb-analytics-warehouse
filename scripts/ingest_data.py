"""
Complete data ingestion for ClickHouse and DuckDB with native format loading.
"""

import duckdb
import pandas as pd
from pathlib import Path
import time
import logging
import subprocess
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_PATH = 'data/taxi.duckdb'
DATA_DIR = Path('data')

class DataIngestor:
    def __init__(self, use_clickhouse=False):
        self.use_clickhouse = use_clickhouse
        self.duckdb_conn = duckdb.connect(DUCKDB_PATH)
        self.ch_available = False
        
        if use_clickhouse:
            try:
                result = subprocess.run(
                    ['docker', 'exec', 'clickhouse_server', 'clickhouse-client', '--query', 'SELECT 1'],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    logger.info("✓ Connected to ClickHouse via Docker")
                    self.ch_available = True
                    self.create_clickhouse_tables()
            except Exception as e:
                logger.warning(f"ClickHouse unavailable: {e}")
    
    def execute_ch_query(self, query):
        try:
            result = subprocess.run(
                ['docker', 'exec', 'clickhouse_server', 'clickhouse-client', '--query', query],
                capture_output=True, text=True, timeout=30
            )
            return result.returncode == 0, result.stderr if result.returncode != 0 else ""
        except Exception as e:
            return False, str(e)
    
    def create_duckdb_tables(self):
        logger.info("Creating DuckDB tables...")
        self.duckdb_conn.execute("""
            CREATE TABLE IF NOT EXISTS nyc_taxi_baseline (
                VendorID TINYINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
                passenger_count TINYINT, trip_distance FLOAT, pickup_longitude FLOAT,
                pickup_latitude FLOAT, dropoff_longitude FLOAT, dropoff_latitude FLOAT,
                RatecodeID TINYINT, store_and_fwd_flag VARCHAR, payment_type TINYINT,
                fare_amount DECIMAL(10, 2), extra DECIMAL(10, 2), mta_tax DECIMAL(10, 2),
                tip_amount DECIMAL(10, 2), tolls_amount DECIMAL(10, 2), total_amount DECIMAL(10, 2)
            )
        """)
        logger.info("✓ DuckDB baseline table created")
    
    def create_clickhouse_tables(self):
        if not self.ch_available:
            return
        
        logger.info("Creating ClickHouse tables...")
        
        self.execute_ch_query("CREATE DATABASE IF NOT EXISTS taxi_db")
        
        baseline_sql = """
        CREATE TABLE IF NOT EXISTS taxi_db.nyc_taxi_baseline (
            VendorID UInt8, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime,
            passenger_count UInt8, trip_distance Float32, pickup_longitude Float32,
            pickup_latitude Float32, dropoff_longitude Float32, dropoff_latitude Float32,
            RatecodeID UInt8, store_and_fwd_flag String, payment_type UInt8,
            fare_amount Decimal(10, 2), extra Decimal(10, 2), mta_tax Decimal(10, 2),
            tip_amount Decimal(10, 2), tolls_amount Decimal(10, 2), total_amount Decimal(10, 2)
        ) ENGINE = MergeTree() ORDER BY ()
        """
        self.execute_ch_query(baseline_sql)
        logger.info("✓ ClickHouse baseline table created")
        
        optimized_sql = """
        CREATE TABLE IF NOT EXISTS taxi_db.nyc_taxi_optimized (
            VendorID UInt8, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime,
            passenger_count UInt8, trip_distance Float32, pickup_longitude Float32,
            pickup_latitude Float32, dropoff_longitude Float32, dropoff_latitude Float32,
            RatecodeID UInt8, store_and_fwd_flag String, payment_type UInt8,
            fare_amount Decimal(10, 2), extra Decimal(10, 2), mta_tax Decimal(10, 2),
            tip_amount Decimal(10, 2), tolls_amount Decimal(10, 2), total_amount Decimal(10, 2)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(tpep_pickup_datetime)
        ORDER BY (tpep_pickup_datetime, passenger_count, payment_type)
        """
        self.execute_ch_query(optimized_sql)
        logger.info("✓ ClickHouse optimized table created (with partitioning and sorting keys)")
        
        mv_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS taxi_db.daily_revenue_mv
        ENGINE = SummingMergeTree() ORDER BY pickup_day
        AS SELECT 
            toDate(tpep_pickup_datetime) as pickup_day,
            COUNT() as num_trips,
            SUM(total_amount) as total_revenue,
            SUM(fare_amount) as total_fare,
            SUM(tip_amount) as total_tips
        FROM taxi_db.nyc_taxi_optimized
        GROUP BY toDate(tpep_pickup_datetime)
        """
        self.execute_ch_query(mv_sql)
        logger.info("✓ ClickHouse materialized view created")
    
    def ingest_to_duckdb(self, parquet_file):
        logger.info(f"Loading {parquet_file.name} to DuckDB...")
        try:
            df = pd.read_parquet(parquet_file)
            self.duckdb_conn.register('temp_df', df)
            self.duckdb_conn.execute("INSERT INTO nyc_taxi_baseline SELECT * FROM temp_df")
            self.duckdb_conn.unregister('temp_df')
            logger.info(f"✓ Loaded {len(df):,} rows to DuckDB")
            return len(df)
        except Exception as e:
            logger.error(f"DuckDB error: {e}")
            return 0
    
    def ingest_to_clickhouse(self, parquet_file, table_name='nyc_taxi_baseline'):
        if not self.ch_available:
            return 0
        
        logger.info(f"Loading {parquet_file.name} to ClickHouse ({table_name})...")
        try:
            df = pd.read_parquet(parquet_file)
            
            # Convert to JSON Lines format
            json_file = f"temp_{parquet_file.stem}.ndjson"
            with open(json_file, 'w') as f:
                for record in df.to_dict('records'):
                    f.write(json.dumps(record) + '\n')
            
            # Load into ClickHouse
            with open(json_file, 'rb') as f:
                result = subprocess.run(
                    ['docker', 'exec', '-i', 'clickhouse_server', 'clickhouse-client',
                     f'--query=INSERT INTO taxi_db.{table_name} FORMAT JSONEachRow'],
                    stdin=f, capture_output=True, text=True, timeout=60
                )
            
            if result.returncode == 0:
                logger.info(f"✓ Loaded {len(df):,} rows to ClickHouse ({table_name})")
                return len(df)
            else:
                logger.error(f"ClickHouse error: {result.stderr}")
                return 0
        except Exception as e:
            logger.error(f"ClickHouse loading error: {e}")
            return 0
    
    def ingest_all_data(self):
        logger.info("=" * 70)
        logger.info("Starting data ingestion to DuckDB and ClickHouse...")
        logger.info("=" * 70)
        
        parquet_files = sorted([f for f in DATA_DIR.glob('nyc_taxi_2019_*.parquet') if 'combined' not in f.name])
        
        if not parquet_files:
            logger.error("No parquet files found!")
            return False
        
        self.create_duckdb_tables()
        
        total_rows = 0
        start_time = time.time()
        
        for pf in parquet_files:
            logger.info(f"\nProcessing {pf.name}...")
            
            rows = self.ingest_to_duckdb(pf)
            total_rows += rows
            
            if self.ch_available:
                self.ingest_to_clickhouse(pf, 'nyc_taxi_baseline')
                self.ingest_to_clickhouse(pf, 'nyc_taxi_optimized')
        
        elapsed = time.time() - start_time
        logger.info("\n" + "=" * 70)
        logger.info(f"Total rows loaded: {total_rows:,}")
        logger.info(f"Time elapsed: {elapsed:.2f} seconds")
        logger.info("=" * 70)
        return True
    
    def verify_data(self):
        logger.info("\nVerifying data integrity...")
        try:
            result = self.duckdb_conn.execute("SELECT COUNT(*) FROM nyc_taxi_baseline").fetchall()
            logger.info(f"✓ DuckDB: {result[0][0]:,} rows")
        except Exception as e:
            logger.error(f"DuckDB verification failed: {e}")
        
        if self.ch_available:
            try:
                success, _ = self.execute_ch_query("SELECT COUNT(*) FROM taxi_db.nyc_taxi_baseline")
                success2, _ = self.execute_ch_query("SELECT COUNT(*) FROM taxi_db.nyc_taxi_optimized")
                if success and success2:
                    logger.info("✓ ClickHouse: Both baseline and optimized tables verified")
            except Exception as e:
                logger.error(f"ClickHouse verification failed: {e}")
    
    def close(self):
        if self.duckdb_conn:
            self.duckdb_conn.close()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--clickhouse', action='store_true')
    parser.add_argument('--verify', action='store_true')
    args = parser.parse_args()
    
    ingestor = DataIngestor(use_clickhouse=args.clickhouse)
    try:
        ingestor.ingest_all_data()
        if args.verify:
            ingestor.verify_data()
    finally:
        ingestor.close()


if __name__ == '__main__':
    main()
