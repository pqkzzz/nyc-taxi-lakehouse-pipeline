from pyspark.sql.functions import current_timestamp, lit
from spark.utils.spark_session import create_spark_session


def main():
    spark = create_spark_session("bronze_ingest_nyc_taxi")

    spark.sql("CREATE DATABASE IF NOT EXISTS taxi_bronze")

    input_path = "/home/iceberg/project/data/raw/yellow_tripdata_2024-01.parquet"

    df = (
        spark.read.parquet(input_path)
        .withColumn("source_file", lit("yellow_tripdata_2024-01.parquet"))
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    df.writeTo("taxi_bronze.yellow_taxi_trips").createOrReplace()

    print("Created table: taxi_bronze.yellow_taxi_trips")
    spark.stop()


if __name__ == "__main__":
    main()