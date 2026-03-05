import duckdb
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import time
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DUCKDB_PATH = 'data/taxi.duckdb'
DATA_DIR = Path('data')
BATCH_SIZE = 10000

class DataIngestor:
    """Handles data ingestion to both ClickHouse and DuckDB"""
    
    def __init__(self, use_clickhouse=False):
        """
        Initialize the ingestor
        
        Args:
            use_clickhouse: If True, connects to ClickHouse. Otherwise uses only DuckDB.
        """
        self.use_clickhouse = use_clickhouse
        self.duckdb_conn = duckdb.connect(DUCKDB_PATH)
        
        if use_clickhouse:
            try:
                from clickhouse_driver import Client
                self.ch_client = Client('localhost', port=9000, database='taxi_db', user='default', password='')

                logger.info("✓ Connected to ClickHouse")
            except Exception as e:
                logger.warning(f"Could not connect to ClickHouse: {e}")
                logger.warning("Continuing with DuckDB only...")
                self.use_clickhouse = False
        
        self.ch_client = None if not use_clickhouse else getattr(self, 'ch_client', None)
    
    def create_duckdb_tables(self):
        """Create DuckDB tables for baseline and optimized schemas"""
        logger.info("Creating DuckDB tables...")
        
        schema_sql = """
            CREATE TABLE IF NOT EXISTS nyc_taxi_baseline (
                VendorID TINYINT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count TINYINT,
                trip_distance FLOAT,
                pickup_longitude FLOAT,
                pickup_latitude FLOAT,
                dropoff_longitude FLOAT,
                dropoff_latitude FLOAT,
                RatecodeID TINYINT,
                store_and_fwd_flag VARCHAR,
                payment_type TINYINT,
                fare_amount DECIMAL(10, 2),
                extra DECIMAL(10, 2),
                mta_tax DECIMAL(10, 2),
                tip_amount DECIMAL(10, 2),
                tolls_amount DECIMAL(10, 2),
                total_amount DECIMAL(10, 2)
            )
        """
        
        self.duckdb_conn.execute(schema_sql)
        logger.info("✓ DuckDB baseline table created")
    
    def create_clickhouse_tables(self):
        """Create ClickHouse tables with optimized schema"""
        if not self.ch_client:
            logger.warning("ClickHouse not available, skipping ClickHouse table creation")
            return
        
        logger.info("Creating ClickHouse tables...")
        
        try:
            # Create database if it doesn't exist
            self.ch_client.execute("CREATE DATABASE IF NOT EXISTS taxi_db")
            
            # Baseline table (unoptimized)
            baseline_sql = """
                CREATE TABLE IF NOT EXISTS taxi_db.nyc_taxi_baseline (
                    VendorID UInt8,
                    tpep_pickup_datetime DateTime,
                    tpep_dropoff_datetime DateTime,
                    passenger_count UInt8,
                    trip_distance Float32,
                    pickup_longitude Float32,
                    pickup_latitude Float32,
                    dropoff_longitude Float32,
                    dropoff_latitude Float32,
                    RatecodeID UInt8,
                    store_and_fwd_flag String,
                    payment_type UInt8,
                    fare_amount Decimal(10, 2),
                    extra Decimal(10, 2),
                    mta_tax Decimal(10, 2),
                    tip_amount Decimal(10, 2),
                    tolls_amount Decimal(10, 2),
                    total_amount Decimal(10, 2)
                )
                ENGINE = MergeTree()
                ORDER BY ()
            """
            self.ch_client.execute(baseline_sql)
            logger.info("✓ ClickHouse baseline table created")
            
            # Optimized table with partitioning and sorting keys
            optimized_sql = """
                CREATE TABLE IF NOT EXISTS taxi_db.nyc_taxi_optimized (
                    VendorID UInt8,
                    tpep_pickup_datetime DateTime,
                    tpep_dropoff_datetime DateTime,
                    passenger_count UInt8,
                    trip_distance Float32,
                    pickup_longitude Float32,
                    pickup_latitude Float32,
                    dropoff_longitude Float32,
                    dropoff_latitude Float32,
                    RatecodeID UInt8,
                    store_and_fwd_flag String,
                    payment_type UInt8,
                    fare_amount Decimal(10, 2),
                    extra Decimal(10, 2),
                    mta_tax Decimal(10, 2),
                    tip_amount Decimal(10, 2),
                    tolls_amount Decimal(10, 2),
                    total_amount Decimal(10, 2)
                )
                ENGINE = MergeTree()
                PARTITION BY toYYYYMM(tpep_pickup_datetime)
                ORDER BY (tpep_pickup_datetime, passenger_count, payment_type)
            """
            self.ch_client.execute(optimized_sql)
            logger.info("✓ ClickHouse optimized table created")
            
        except Exception as e:
            logger.error(f"Error creating ClickHouse tables: {e}")
    
    def ingest_to_duckdb(self, parquet_file):
        """Load parquet file into DuckDB baseline table"""
        logger.info(f"Loading {parquet_file.name} to DuckDB...")
        
        try:
            df = pd.read_parquet(parquet_file)
            
            # Insert into baseline table
            self.duckdb_conn.register('temp_df', df)
            self.duckdb_conn.execute(
                "INSERT INTO nyc_taxi_baseline SELECT * FROM temp_df"
            )
            self.duckdb_conn.unregister('temp_df')
            
            logger.info(f"✓ Loaded {len(df):,} rows from {parquet_file.name}")
            return len(df)
            
        except Exception as e:
            logger.error(f"Error loading to DuckDB: {e}")
            return 0
    
    def ingest_to_clickhouse(self, parquet_file, table_name='nyc_taxi_baseline'):
        """Load parquet file into ClickHouse"""
        if not self.ch_client:
            return 0
        
        logger.info(f"Loading {parquet_file.name} to ClickHouse ({table_name})...")
        
        try:
            df = pd.read_parquet(parquet_file)
            
            # Convert to list of tuples for batch insert
            records = [tuple(row) for row in df.values]
            
            # Batch insert for better performance
            for i in tqdm(range(0, len(records), BATCH_SIZE), desc="Inserting batches"):
                batch = records[i:i + BATCH_SIZE]
                self.ch_client.execute(
                    f"INSERT INTO taxi_db.{table_name} VALUES",
                    batch
                )
            
            logger.info(f"✓ Loaded {len(df):,} rows to ClickHouse ({table_name})")
            return len(df)
            
        except Exception as e:
            logger.error(f"Error loading to ClickHouse: {e}")
            return 0
    
    def ingest_all_data(self):
        """Load all parquet files into both databases"""
        logger.info("=" * 70)
        logger.info("Starting data ingestion...")
        logger.info("=" * 70)
        
        # Get all parquet files (excluding combined)
        parquet_files = sorted([
            f for f in DATA_DIR.glob('nyc_taxi_2019_*.parquet')
            if 'combined' not in f.name
        ])
        
        if not parquet_files:
            logger.error("No parquet files found in data/ directory!")
            logger.error("Please run: python scripts/generate_nyc_taxi_data.py")
            return False
        
        # Create tables first
        self.create_duckdb_tables()
        if self.use_clickhouse:
            self.create_clickhouse_tables()
        
        total_rows = 0
        start_time = time.time()
        
        # Ingest each month's data
        for pf in parquet_files:
            logger.info(f"\nProcessing {pf.name}...")
            
            # Load to DuckDB (always)
            rows = self.ingest_to_duckdb(pf)
            total_rows += rows
            
            # Load to ClickHouse (if available)
            if self.use_clickhouse and self.ch_client:
                self.ingest_to_clickhouse(pf, table_name='nyc_taxi_baseline')
                self.ingest_to_clickhouse(pf, table_name='nyc_taxi_optimized')
        
        elapsed_time = time.time() - start_time
        
        logger.info("\n" + "=" * 70)
        logger.info("Data ingestion completed!")
        logger.info("=" * 70)
        logger.info(f"Total rows loaded: {total_rows:,}")
        logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
        logger.info(f"Throughput: {total_rows/elapsed_time:,.0f} rows/second")
        logger.info("=" * 70)
        
        return True
    
    def verify_data(self):
        """Verify data was loaded correctly"""
        logger.info("\nVerifying data integrity...")
        
        # DuckDB verification
        try:
            result = self.duckdb_conn.execute(
                "SELECT COUNT(*) as count, MIN(tpep_pickup_datetime) as min_date, "
                "MAX(tpep_pickup_datetime) as max_date FROM nyc_taxi_baseline"
            ).fetchall()
            
            count, min_date, max_date = result[0]
            logger.info(f"DuckDB - Rows: {count:,}, Date range: {min_date} to {max_date}")
            
        except Exception as e:
            logger.error(f"DuckDB verification failed: {e}")
        
        # ClickHouse verification
        if self.use_clickhouse and self.ch_client:
            try:
                result = self.ch_client.execute(
                    "SELECT COUNT(*) as count FROM taxi_db.nyc_taxi_baseline"
                )
                logger.info(f"ClickHouse - Rows: {result[0][0]:,}")
            except Exception as e:
                logger.error(f"ClickHouse verification failed: {e}")
    
    def close(self):
        """Close database connections"""
        if self.duckdb_conn:
            self.duckdb_conn.close()
        logger.info("Database connections closed")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest NYC Taxi data into DuckDB and ClickHouse')
    parser.add_argument('--clickhouse', action='store_true', help='Also load to ClickHouse')
    parser.add_argument('--verify', action='store_true', help='Verify data after ingestion')
    args = parser.parse_args()
    
    ingestor = DataIngestor(use_clickhouse=args.clickhouse)
    
    try:
        success = ingestor.ingest_all_data()
        
        if success and args.verify:
            ingestor.verify_data()
        
    finally:
        ingestor.close()


if __name__ == '__main__':
    main()
