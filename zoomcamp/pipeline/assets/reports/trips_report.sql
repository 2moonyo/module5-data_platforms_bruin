/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: pickup_date
  # TODO: set to `date` or `timestamp`
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: pickup_date
    type: date
    description: "Date of trip pickup"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Taxi type (yellow/green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type name"
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: "Total number of trips"
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: "Total number of passengers"
    checks:
      - name: non_negative
  - name: total_distance
    type: double
    description: "Total trip distance in miles"
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: "Total fare amount (can be negative for refunds)"
  - name: total_tip_amount
    type: double
    description: "Total tip amount (can be negative for refunds/disputes)"
  - name: total_amount
    type: double
    description: "Total amount charged (can be negative for refunds)"
  - name: avg_trip_distance
    type: double
    description: "Average trip distance"
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: double
    description: "Average fare amount"
    checks:
      - name: non_negative

@bruin */

-- Reports: aggregate trips by date, taxi type, and payment type
SELECT
  CAST(pickup_datetime AS DATE) AS pickup_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(passenger_count) AS total_passengers,
  SUM(trip_distance) AS total_distance,
  SUM(fare_amount) AS total_fare_amount,
  SUM(tip_amount) AS total_tip_amount,
  SUM(total_amount) AS total_amount,
  AVG(trip_distance) AS avg_trip_distance,
  AVG(fare_amount) AS avg_fare_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type_name
