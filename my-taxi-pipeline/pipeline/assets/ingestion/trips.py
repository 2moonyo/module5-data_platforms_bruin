"""@bruin
name: ingestion.trips

type: python

image: python:3.12

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: Provider that provided the record (1=Creative Mobile Technologies, 2=VeriFone Inc)
  - name: pickup_datetime
    type: timestamp
    description: Date and time when the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the meter was disengaged
  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle (driver entered value)
  - name: trip_distance
    type: float
    description: Trip distance in miles
  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone in which the meter was engaged
  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone in which the meter was disengaged
  - name: rate_code_id
    type: integer
    description: Final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: Whether trip record was held in vehicle memory before sending (Y=store and forward, N=not a store and forward trip)
  - name: payment_type
    type: integer
    description: How the passenger paid for the trip
  - name: fare_amount
    type: float
    description: Time-and-distance fare calculated by the meter
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax that is automatically triggered based on metered rate
  - name: tip_amount
    type: float
    description: Tip amount (automatically populated for credit card tips, cash tips not included)
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge assessed on hailed trips
  - name: total_amount
    type: float
    description: Total amount charged to passengers (does not include cash tips)
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge for trips in Manhattan
  - name: airport_fee
    type: float
    description: Airport fee for trips to/from airports
  - name: taxi_type
    type: string
    description: Type of taxi service (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when data was extracted from source

@bruin"""

import os
import json
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    
    Uses Bruin runtime context:
    - BRUIN_START_DATE / BRUIN_END_DATE for date range
    - BRUIN_VARS for taxi_types configuration
    """
    # Read Bruin environment variables
    start_date = os.getenv('BRUIN_START_DATE')  # YYYY-MM-DD
    end_date = os.getenv('BRUIN_END_DATE')      # YYYY-MM-DD
    bruin_vars = os.getenv('BRUIN_VARS', '{}')
    
    # Parse pipeline variables
    vars_dict = json.loads(bruin_vars)
    taxi_types = vars_dict.get('taxi_types', ['yellow'])
    
    # Parse dates
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Generate list of months to fetch
    months = []
    current = start
    while current <= end:
        months.append(current.strftime('%Y-%m'))
        current += relativedelta(months=1)
    
    # Base URL for NYC TLC trip data
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    
    # Fetch and concatenate data
    dataframes = []
    extraction_time = datetime.now()
    
    for taxi_type in taxi_types:
        for month in months:
            url = f"{base_url}/{taxi_type}_tripdata_{month}.parquet"
            
            try:
                print(f"Fetching {url}...")
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                
                # Read parquet from bytes
                df = pd.read_parquet(BytesIO(response.content))
                
                # Standardize column names (TLC data has inconsistent capitalization)
                df.columns = df.columns.str.lower()
                
                # Rename columns to match our schema
                column_mapping = {
                    'vendorid': 'vendor_id',
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                    'passenger_count': 'passenger_count',
                    'trip_distance': 'trip_distance',
                    'pulocationid': 'pickup_location_id',
                    'dolocationid': 'dropoff_location_id',
                    'ratecodeid': 'rate_code_id',
                    'store_and_fwd_flag': 'store_and_fwd_flag',
                    'payment_type': 'payment_type',
                    'fare_amount': 'fare_amount',
                    'extra': 'extra',
                    'mta_tax': 'mta_tax',
                    'tip_amount': 'tip_amount',
                    'tolls_amount': 'tolls_amount',
                    'improvement_surcharge': 'improvement_surcharge',
                    'total_amount': 'total_amount',
                    'congestion_surcharge': 'congestion_surcharge',
                    'airport_fee': 'airport_fee'
                }
                df = df.rename(columns=column_mapping)
                
                # Add metadata columns
                df['taxi_type'] = taxi_type
                df['extracted_at'] = extraction_time
                
                # Select only columns defined in schema
                expected_columns = [
                    'vendor_id', 'pickup_datetime', 'dropoff_datetime',
                    'passenger_count', 'trip_distance', 'pickup_location_id',
                    'dropoff_location_id', 'rate_code_id', 'store_and_fwd_flag',
                    'payment_type', 'fare_amount', 'extra', 'mta_tax',
                    'tip_amount', 'tolls_amount', 'improvement_surcharge',
                    'total_amount', 'congestion_surcharge', 'airport_fee',
                    'taxi_type', 'extracted_at'
                ]
                
                # Add missing columns with None
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = None
                
                df = df[expected_columns]
                
                dataframes.append(df)
                print(f"  -> Fetched {len(df):,} rows")
                
            except requests.exceptions.RequestException as e:
                print(f"Warning: Failed to fetch {url}: {e}")
                continue
            except Exception as e:
                print(f"Warning: Error processing {url}: {e}")
                continue
    
    if not dataframes:
        print("No data fetched. Returning empty DataFrame.")
        return pd.DataFrame(columns=[
            'vendor_id', 'pickup_datetime', 'dropoff_datetime',
            'passenger_count', 'trip_distance', 'pickup_location_id',
            'dropoff_location_id', 'rate_code_id', 'store_and_fwd_flag',
            'payment_type', 'fare_amount', 'extra', 'mta_tax',
            'tip_amount', 'tolls_amount', 'improvement_surcharge',
            'total_amount', 'congestion_surcharge', 'airport_fee',
            'taxi_type', 'extracted_at'
        ])
    
    # Concatenate all dataframes
    final_df = pd.concat(dataframes, ignore_index=True)
    print(f"\nTotal rows fetched: {len(final_df):,}")
    
    return final_df


