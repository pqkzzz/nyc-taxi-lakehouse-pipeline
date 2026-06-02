SELECT *
FROM {{ source('taxi_silver', 'yellow_taxi_trips_clean') }}
WHERE fare_amount <= 0
   OR total_amount <= 0