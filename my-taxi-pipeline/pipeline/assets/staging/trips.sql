/* @bruin

name: staging.trips
type: bq.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: vendor_id
    type: integer
    description: Provider that provided the record
  - name: pickup_datetime
    type: timestamp
    description: Date and time when the meter was engaged
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the meter was disengaged
    primary_key: true
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone in which the meter was engaged
    primary_key: true
  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone in which the meter was disengaged
    primary_key: true
  - name: rate_code_id
    type: integer
    description: Final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: Whether trip record was held in vehicle memory
  - name: payment_type
    type: integer
    description: How the passenger paid for the trip
  - name: payment_type_name
    type: string
    description: Human-readable payment type name from lookup table
  - name: fare_amount
    type: float
    description: Time-and-distance fare calculated by the meter
    primary_key: true
    checks:
      - name: non_negative
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax
  - name: tip_amount
    type: float
    description: Tip amount (can be negative for refunds/corrections)
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid
    checks:
      - name: non_negative
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge
  - name: total_amount
    type: float
    description: Total amount charged to passengers
    checks:
      - name: non_negative
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge
  - name: airport_fee
    type: float
    description: Airport fee
  - name: taxi_type
    type: string
    description: Type of taxi service (yellow or green)
    checks:
      - name: not_null

custom_checks:
  - name: no_future_pickups
    description: Ensure no trips have pickup_datetime in the future
    query: |
      SELECT COUNT(*)
      FROM staging.trips
      WHERE pickup_datetime > CURRENT_TIMESTAMP
    value: 0

  - name: valid_dropoff_times
    description: Ensure dropoff_datetime is after pickup_datetime
    query: |
      SELECT COUNT(*)
      FROM staging.trips
      WHERE dropoff_datetime <= pickup_datetime
    value: 0

  - name: no_duplicates_check
    description: Ensure deduplication worked correctly (no duplicate composite keys)
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT 
          pickup_datetime,
          dropoff_datetime,
          pickup_location_id,
          dropoff_location_id,
          fare_amount,
          COUNT(*) as cnt
        FROM staging.trips
        GROUP BY 1, 2, 3, 4, 5
        HAVING COUNT(*) > 1
      )
    value: 0

@bruin */

WITH deduplicated_trips AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        fare_amount
      ORDER BY extracted_at DESC
    ) as row_num
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime > pickup_datetime
    AND fare_amount >= 0
    AND total_amount >= 0
)

SELECT
  t.vendor_id,
  t.pickup_datetime,
  t.dropoff_datetime,
  t.passenger_count,
  t.trip_distance,
  t.pickup_location_id,
  t.dropoff_location_id,
  t.rate_code_id,
  t.store_and_fwd_flag,
  t.payment_type,
  COALESCE(p.payment_type_name, 'unknown') as payment_type_name,
  t.fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  t.improvement_surcharge,
  t.total_amount,
  t.congestion_surcharge,
  t.airport_fee,
  t.taxi_type
FROM deduplicated_trips t
LEFT JOIN ingestion.payment_lookup p
  ON t.payment_type = p.payment_type_id
WHERE t.row_num = 1
