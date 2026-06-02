from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    dayofweek,
    unix_timestamp,
    round,
    when
)
from spark.utils.spark_session import create_spark_session


def main():
    spark = create_spark_session("silver_clean_nyc_taxi")

    spark.sql("CREATE DATABASE IF NOT EXISTS taxi_silver")

    trips = spark.table("taxi_bronze.yellow_taxi_trips")

    df = (
        trips
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("RatecodeID", "ratecode_id")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
    )

    df_clean = (
        df
        .filter(col("pickup_datetime").isNotNull())
        .filter(col("dropoff_datetime").isNotNull())
        .filter(col("dropoff_datetime") > col("pickup_datetime"))
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") > 0)
        .filter(col("total_amount") > 0)
        .filter((col("passenger_count") >= 1) & (col("passenger_count") <= 6))
        .filter(col("pickup_location_id").isNotNull())
        .filter(col("dropoff_location_id").isNotNull())
    )

    df_silver = (
        df_clean
        .withColumn(
            "trip_duration_minutes",
            round(
                (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60,
                2
            )
        )
        .withColumn("pickup_date", to_date(col("pickup_datetime")))
        .withColumn("pickup_hour", hour(col("pickup_datetime")))
        .withColumn("pickup_day_of_week", dayofweek(col("pickup_datetime")))
        .withColumn("fare_per_mile", round(col("fare_amount") / col("trip_distance"), 2))
        .withColumn(
            "tip_percentage",
            round(
                when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount") * 100)
                .otherwise(0),
                2
            )
        )
    )

    df_silver.writeTo("taxi_silver.yellow_taxi_trips_clean").createOrReplace()

    print("Created table: taxi_silver.yellow_taxi_trips_clean")
    spark.stop()


if __name__ == "__main__":
    main()