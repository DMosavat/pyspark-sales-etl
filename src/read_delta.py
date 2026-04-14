# Read Delta tables from storage.

from pyspark.sql import SparkSession, DataFrame


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)