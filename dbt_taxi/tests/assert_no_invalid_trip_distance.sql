SELECT *
FROM {{ source('taxi_silver', 'yellow_taxi_trips_clean') }}
WHERE trip_distance <= 0