SELECT *
FROM {{ source('taxi_silver', 'yellow_taxi_trips_clean') }}
WHERE passenger_count < 1
   OR passenger_count > 6