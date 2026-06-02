from spark.utils.spark_session import create_spark_session


def main():
    spark = create_spark_session("gold_marts_nyc_taxi")

    spark.sql("CREATE DATABASE IF NOT EXISTS taxi_gold")

    daily_revenue = spark.sql("""
        SELECT
            pickup_date,
            COUNT(*) AS total_trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_trip_amount,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
            ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage
        FROM taxi_silver.yellow_taxi_trips_clean
        GROUP BY pickup_date
        ORDER BY pickup_date
    """)

    daily_revenue.writeTo("taxi_gold.mart_daily_revenue").createOrReplace()

    hourly_demand = spark.sql("""
        SELECT
            pickup_hour,
            COUNT(*) AS total_trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes
        FROM taxi_silver.yellow_taxi_trips_clean
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """)

    hourly_demand.writeTo("taxi_gold.mart_hourly_demand").createOrReplace()

    pickup_zone = spark.sql("""
        SELECT
            z.borough,
            z.zone,
            COUNT(*) AS total_trips,
            ROUND(SUM(t.total_amount), 2) AS total_revenue,
            ROUND(AVG(t.trip_distance), 2) AS avg_trip_distance,
            ROUND(AVG(t.total_amount), 2) AS avg_trip_amount
        FROM taxi_silver.yellow_taxi_trips_clean t
        LEFT JOIN taxi_silver.taxi_zones z
            ON t.pickup_location_id = z.location_id
        GROUP BY z.borough, z.zone
        ORDER BY total_trips DESC
    """)

    pickup_zone.writeTo("taxi_gold.mart_pickup_zone_performance").createOrReplace()

    payment_summary = spark.sql("""
        SELECT
            payment_type,
            COUNT(*) AS total_trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage
        FROM taxi_silver.yellow_taxi_trips_clean
        GROUP BY payment_type
        ORDER BY total_trips DESC
    """)

    payment_summary.writeTo("taxi_gold.mart_payment_type_summary").createOrReplace()

    print("Created gold marts in taxi_gold schema")
    spark.stop()


if __name__ == "__main__":
    main()