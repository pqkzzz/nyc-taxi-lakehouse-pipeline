import os
from pyspark.sql import SparkSession


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def create_spark_session(app_name: str) -> SparkSession:
    catalog = os.getenv("ICEBERG_CATALOG", "demo")
    rest_uri = require_env("ICEBERG_REST_URI")
    warehouse = require_env("ICEBERG_WAREHOUSE")
    minio_endpoint = require_env("MINIO_ENDPOINT")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", rest_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", minio_endpoint)
        .config("spark.sql.defaultCatalog", catalog)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark