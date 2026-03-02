"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: VendorID
    type: integer
    description: "Vendor identifier"
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup datetime"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff datetime"
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: pickup_location_id
    type: integer
    description: "Pickup location ID"
  - name: dropoff_location_id
    type: integer
    description: "Dropoff location ID"
  - name: payment_type
    type: integer
    description: "Payment type ID"
  - name: fare_amount
    type: float
    description: "Base fare amount"
  - name: extra
    type: float
    description: "Extra charges"
  - name: mta_tax
    type: float
    description: "MTA tax"
  - name: tip_amount
    type: float
    description: "Tip amount"
  - name: tolls_amount
    type: float
    description: "Tolls amount"
  - name: total_amount
    type: float
    description: "Total amount charged"
  - name: taxi_type
    type: string
    description: "Taxi type (yellow/green)"
  - name: extracted_at
    type: timestamp
    description: "Extraction timestamp"

@bruin"""

import os
import json
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

def materialize():
    """
    Fetch NYC Taxi data from TLC public endpoint.
    """
    # Get Bruin runtime variables
    start_date = parse(os.environ['BRUIN_START_DATE']).date()
    end_date = parse(os.environ['BRUIN_END_DATE']).date()
    vars_json = os.environ.get('BRUIN_VARS', '{}')
    pipeline_vars = json.loads(vars_json)
    taxi_types = pipeline_vars.get('taxi_types', ['yellow', 'green'])
    
    # Base URL for NYC TLC data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Generate list of months to fetch
    dataframes = []
    current = start_date.replace(day=1)
    end = end_date.replace(day=1)
    
    while current <= end:
        year_month = current.strftime('%Y-%m')
        
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            url = base_url + filename
            
            try:
                print(f"Fetching {url}...")
                df = pd.read_parquet(url)
                
                # Handle duplicate columns with different cases (e.g., airport_fee vs Airport_fee)
                # Coalesce duplicates before normalizing
                if 'Airport_fee' in df.columns and 'airport_fee' in df.columns:
                    df['airport_fee'] = df['airport_fee'].fillna(df['Airport_fee'])
                    df = df.drop(columns=['Airport_fee'])
                
                # Standardize column names (handle variations between yellow and green)
                column_mapping = {
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id',
                    'RatecodeID': 'rate_code_id',
                    'VendorID': 'vendor_id'
                }
                df = df.rename(columns=column_mapping)
                
                # Normalize all column names to lowercase to avoid collisions
                df.columns = df.columns.str.lower()
                
                # Add metadata columns
                df['taxi_type'] = taxi_type
                df['extracted_at'] = datetime.now()
                
                dataframes.append(df)
                print(f"Successfully fetched {len(df)} rows from {filename}")
            except Exception as e:
                print(f"Warning: Could not fetch {url}: {e}")
        
        current += relativedelta(months=1)
    
    if not dataframes:
        raise ValueError("No data fetched. Check date range and taxi_types.")
    
    # Concatenate all dataframes
    final_df = pd.concat(dataframes, ignore_index=True)
    
    return final_df


