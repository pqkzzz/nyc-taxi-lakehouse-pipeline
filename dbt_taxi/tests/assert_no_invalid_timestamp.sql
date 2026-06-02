SELECT *
FROM {{ source('taxi_silver', 'yellow_taxi_trips_clean') }}
WHERE dropoff_datetime <= pickup_datetime