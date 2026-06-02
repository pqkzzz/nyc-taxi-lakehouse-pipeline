from pyspark.sql.functions import col
from spark.utils.spark_session import create_spark_session


def main():
    spark = create_spark_session("load_taxi_zones")

    spark.sql("CREATE DATABASE IF NOT EXISTS taxi_silver")

    input_path = "/home/iceberg/project/data/raw/taxi_zone_lookup.csv"

    zones = (
        spark.read
        .option("header", True)
        .csv(input_path)
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
        .withColumnRenamed("service_zone", "service_zone")
        .withColumn("location_id", col("location_id").cast("int"))
    )

    zones.writeTo("taxi_silver.taxi_zones").createOrReplace()

    print("Created table: taxi_silver.taxi_zones")
    spark.stop()


if __name__ == "__main__":
    main()