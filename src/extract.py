# Read source CSV files into Spark DataFrames.

from pyspark.sql import SparkSession, DataFrame


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def read_customers(spark: SparkSession, path: str) -> DataFrame:
    return read_csv(spark, path)


def read_orders(spark: SparkSession, path: str) -> DataFrame:
    return read_csv(spark, path)


def read_order_items(spark: SparkSession, path: str) -> DataFrame:
    return read_csv(spark, path)


def read_products(spark: SparkSession, path: str) -> DataFrame:
    return read_csv(spark, path)