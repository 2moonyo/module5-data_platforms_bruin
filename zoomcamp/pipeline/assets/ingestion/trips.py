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
connection: gcp-default

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
    type: INT64
    description: "Vendor identifier"
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup datetime"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff datetime"
  - name: passenger_count
    type: INT64
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: pickup_location_id
    type: INT64
    description: "Pickup location ID"
  - name: dropoff_location_id
    type: INT64
    description: "Dropoff location ID"
  - name: payment_type
    type: INT64
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
from datetime import datetime
from dateutil.parser import parse
from google.cloud import bigquery

def materialize():
    """
    Fetch NYC Taxi data from BigQuery public dataset.
    Using BigQuery public dataset avoids 403 errors from CloudFront URLs.
    """
    # Get Bruin runtime variables
    start_date = parse(os.environ['BRUIN_START_DATE']).date()
    end_date = parse(os.environ['BRUIN_END_DATE']).date()
    vars_json = os.environ.get('BRUIN_VARS', '{}')
    pipeline_vars = json.loads(vars_json)
    taxi_types = pipeline_vars.get('taxi_types', ['yellow', 'green'])
    
    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Get unique years in the date range
    years = set()
    for year in range(start_date.year, end_date.year + 1):
        years.add(year)
    
    dataframes = []
    
    for taxi_type in taxi_types:
        for year in sorted(years):
            # BigQuery public dataset tables are named by year
            table_id = f'bigquery-public-data.new_york_taxi_trips.tlc_{taxi_type}_trips_{year}'
            
            # Query for the date range within this year
            year_start = max(start_date, datetime(year, 1, 1).date())
            year_end = min(end_date, datetime(year, 12, 31).date())
            
            query = f"""
            SELECT 
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_longitude,
                pickup_latitude,
                rate_code,
                store_and_fwd_flag,
                dropoff_longitude,
                dropoff_latitude,
                payment_type,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                total_amount,
                imp_surcharge as improvement_surcharge,
                pickup_location_id,
                dropoff_location_id
            FROM `{table_id}`
            WHERE DATE(pickup_datetime) BETWEEN '{year_start}' AND '{year_end}'
            """
            
            try:
                print(f"Querying {table_id} for {year_start} to {year_end}...")
                df = client.query(query).to_dataframe()
                
                if len(df) > 0:
                    # Add metadata columns
                    df['taxi_type'] = taxi_type
                    df['extracted_at'] = datetime.now()
                    
                    dataframes.append(df)
                    print(f"Successfully fetched {len(df)} rows from {taxi_type} trips {year}")
                else:
                    print(f"No data found for {taxi_type} trips {year}")
            except Exception as e:
                print(f"Warning: Could not fetch {taxi_type} data for {year}: {e}")
    
    if not dataframes:
        raise ValueError("No data fetched. Check date range and taxi_types.")
    
    # Concatenate all dataframes
    final_df = pd.concat(dataframes, ignore_index=True)
    
    return final_df


