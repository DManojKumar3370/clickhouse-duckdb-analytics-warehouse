"""
Generate realistic synthetic NYC Taxi data for performance optimization benchmarking.
Creates 3 months of data (Jan-Mar 2019) with ~150K records/month.
Output: Parquet files in data/ directory
"""

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import os
from pathlib import Path

# NYC Taxi realistic parameters
NYC_BOROUGHS = {
    'Manhattan': (40.7580, -73.9855),
    'Queens': (40.7282, -73.7949),
    'Brooklyn': (40.6782, -73.9442),
    'Bronx': (40.8448, -73.8648),
    'Staten Island': (40.5795, -74.1502)
}

PAYMENT_TYPES = ['credit card', 'cash']
VENDOR_IDS = [1, 2]  # NYC Taxi vendors

def generate_coordinates(borough_coords, num_points, variation=0.1):
    """Generate realistic lat/long coordinates around borough center"""
    lat, lon = borough_coords
    lats = np.random.normal(lat, variation, num_points)
    lons = np.random.normal(lon, variation, num_points)
    return lats, lons

def generate_month_data(year, month, num_records=150000):
    """Generate synthetic taxi data for one month"""
    print(f"Generating {num_records:,} records for {year}-{month:02d}...")
    
    # Generate random dates within the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='1H')
    pickup_times = pd.to_datetime(
        np.random.choice(date_range, size=num_records, replace=True)
    )
    
    # Generate realistic trip durations (5-60 minutes)
    trip_duration_minutes = np.random.gamma(shape=2, scale=10, size=num_records)
    trip_duration_minutes = np.clip(trip_duration_minutes, 5, 60)
    
    dropoff_times = pickup_times + pd.to_timedelta(trip_duration_minutes, unit='minutes')
    
    # Random borough selection for pickup/dropoff
    pickup_borough = np.random.choice(list(NYC_BOROUGHS.keys()), size=num_records)
    dropoff_borough = np.random.choice(list(NYC_BOROUGHS.keys()), size=num_records)
    
    # Generate coordinates
    pickup_lats = []
    pickup_lons = []
    dropoff_lats = []
    dropoff_lons = []
    
    for pb, db in zip(pickup_borough, dropoff_borough):
        plat, plon = generate_coordinates(NYC_BOROUGHS[pb], 1, variation=0.05)
        dlat, dlon = generate_coordinates(NYC_BOROUGHS[db], 1, variation=0.05)
        pickup_lats.extend(plat)
        pickup_lons.extend(plon)
        dropoff_lats.extend(dlat)
        dropoff_lons.extend(dlon)
    
    # Calculate distance (simplified haversine)
    from math import radians, sin, cos, sqrt, atan2
    
    def haversine(lat1, lon1, lat2, lon2):
        R = 3959  # Earth radius in miles
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        return R * c
    
    distances = [
        haversine(lat1, lon1, lat2, lon2)
        for lat1, lon1, lat2, lon2 
        in zip(pickup_lats, pickup_lons, dropoff_lats, dropoff_lons)
    ]
    
    # Realistic fare calculation: $2.50 base + $2.50/mile + time surcharge
    base_fare = 2.50
    fare_per_mile = 2.50
    surge_multiplier = np.random.choice([1.0, 1.25, 1.5], size=num_records, p=[0.7, 0.2, 0.1])
    
    fares = (base_fare + np.array(distances) * fare_per_mile) * surge_multiplier
    fares = np.round(fares, 2)
    
    # Tips and taxes
    tips = np.round(fares * np.random.uniform(0.1, 0.25, num_records), 2)
    taxes = np.round(fares * 0.0875, 2)  # NYC tax rate
    total_amount = fares + tips + taxes
    
    # Passenger count (1-6, bimodal distribution)
    passenger_count = np.random.choice([1, 2, 3, 4, 5, 6], 
                                       size=num_records,
                                       p=[0.65, 0.15, 0.10, 0.05, 0.03, 0.02])
    
    # Create DataFrame
    df = pd.DataFrame({
        'VendorID': np.random.choice(VENDOR_IDS, size=num_records),
        'tpep_pickup_datetime': pickup_times,
        'tpep_dropoff_datetime': dropoff_times,
        'passenger_count': passenger_count,
        'trip_distance': np.round(distances, 2),
        'pickup_longitude': np.round(pickup_lons, 4),
        'pickup_latitude': np.round(pickup_lats, 4),
        'dropoff_longitude': np.round(dropoff_lons, 4),
        'dropoff_latitude': np.round(dropoff_lats, 4),
        'RatecodeID': np.random.choice([1, 2, 3, 4, 5], size=num_records, p=[0.85, 0.05, 0.05, 0.03, 0.02]),
        'store_and_fwd_flag': np.random.choice(['Y', 'N'], size=num_records, p=[0.05, 0.95]),
        'payment_type': np.random.choice([1, 2], size=num_records, p=[0.7, 0.3]),
        'fare_amount': fares,
        'extra': np.random.choice([0.0, 0.5, 1.0], size=num_records, p=[0.7, 0.15, 0.15]),
        'mta_tax': 0.5,
        'tip_amount': tips,
        'tolls_amount': np.random.choice([0.0, 5.76, 17.50], size=num_records, p=[0.95, 0.03, 0.02]),
        'total_amount': total_amount
    })
    
    return df

def generate_all_data():
    """Generate 3 months of NYC Taxi data"""
    # Create data directory if it doesn't exist
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    
    dfs = []
    for month in range(1, 4):  # Jan, Feb, Mar
        df = generate_month_data(2019, month, num_records=150000)
        dfs.append(df)
        
        # Save each month as parquet
        output_file = data_dir / f'nyc_taxi_2019_0{month}.parquet'
        df.to_parquet(output_file, compression='snappy', index=False)
        print(f"✓ Saved {output_file} ({len(df):,} rows, {output_file.stat().st_size / (1024**3):.2f} GB)")
    
    # Combine all months
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_file = data_dir / 'nyc_taxi_2019_01_03_combined.parquet'
    combined_df.to_parquet(combined_file, compression='snappy', index=False)
    print(f"\n✓ Combined dataset saved: {combined_file}")
    print(f"  Total records: {len(combined_df):,}")
    print(f"  File size: {combined_file.stat().st_size / (1024**3):.2f} GB")
    print(f"  Date range: {combined_df['tpep_pickup_datetime'].min()} to {combined_df['tpep_pickup_datetime'].max()}")
    
    return combined_df

if __name__ == '__main__':
    print("=" * 70)
    print("NYC Taxi Synthetic Data Generator")
    print("=" * 70)
    generate_all_data()
    print("\n✓ Data generation complete!")
