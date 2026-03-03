/* @bruin

name: reports.trips_report
type: bq.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: report_date
    type: date
    description: Date of the trips (extracted from pickup_datetime)
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi service (yellow or green)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment method used
    primary_key: true
    checks:
      - name: not_null
  - name: total_trips
    type: bigint
    description: Total number of trips
    checks:
      - name: positive
  - name: total_revenue
    type: float
    description: Total revenue (sum of total_amount)
    checks:
      - name: non_negative
  - name: total_distance
    type: float
    description: Total distance traveled in miles
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: Total number of passengers
    checks:
      - name: non_negative
  - name: total_tips
    type: float
    description: Total tip amount
  - name: avg_fare
    type: float
    description: Average fare amount per trip
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: Average trip distance in miles
    checks:
      - name: non_negative
  - name: avg_passengers_per_trip
    type: float
    description: Average number of passengers per trip
    checks:
      - name: non_negative
  - name: avg_tip_percentage
    type: float
    description: Average tip as percentage of fare amount

custom_checks:
  - name: revenue_matches_trips
    description: Ensure we have positive revenue when we have trips
    query: |
      SELECT COUNT(*)
      FROM reports.trips_report
      WHERE total_trips > 0 AND total_revenue <= 0
    value: 0

  - name: reasonable_avg_fare
    description: Ensure average fare is within reasonable bounds ($1 - $500)
    query: |
      SELECT COUNT(*)
      FROM reports.trips_report
      WHERE avg_fare < 1 OR avg_fare > 500
    value: 0

@bruin */

SELECT
  DATE_TRUNC('day', pickup_datetime)::DATE as report_date,
  taxi_type,
  payment_type_name,
  
  -- Trip counts and totals
  COUNT(*) as total_trips,
  SUM(total_amount) as total_revenue,
  SUM(trip_distance) as total_distance,
  SUM(passenger_count) as total_passengers,
  SUM(tip_amount) as total_tips,
  
  -- Averages
  AVG(fare_amount) as avg_fare,
  AVG(trip_distance) as avg_trip_distance,
  AVG(passenger_count) as avg_passengers_per_trip,
  
  -- Tip percentage (handle division by zero)
  CASE 
    WHEN SUM(fare_amount) > 0 
    THEN (SUM(tip_amount) / SUM(fare_amount)) * 100
    ELSE 0
  END as avg_tip_percentage

FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 
  DATE_TRUNC('day', pickup_datetime),
  taxi_type,
  payment_type_name
