import argparse
from pyspark.sql.functions import current_timestamp, lit
from spark.utils.spark_session import create_spark_session


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark_session("bronze_ingest_nyc_taxi")

    spark.sql("CREATE DATABASE IF NOT EXISTS taxi_bronze")

    month = args.month.zfill(2)
    file_name = f"yellow_tripdata_{args.year}-{month}.parquet"
    input_path = f"/home/iceberg/project/data/raw/{file_name}"

    df = (
        spark.read.parquet(input_path)
        .withColumn("source_file", lit(file_name))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("year", lit(int(args.year)))
        .withColumn("month", lit(int(month)))
    )

    df.writeTo("taxi_bronze.yellow_taxi_trips").createOrReplace()

    print(f"Created table: taxi_bronze.yellow_taxi_trips from {file_name}")
    spark.stop()


if __name__ == "__main__":
    main()