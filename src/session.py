# Create and return a Spark session for the ETL pipeline.

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "PySpark Sales ETL") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )