# Read source CSV files into Spark DataFrames.

from pyspark.sql import SparkSession, DataFrame


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def read_customers(spark: SparkSession) -> DataFrame:
    return read_csv(spark, "data/sample/customers.csv")


def read_orders(spark: SparkSession) -> DataFrame:
    return read_csv(spark, "data/sample/orders.csv")


def read_order_items(spark: SparkSession) -> DataFrame:
    return read_csv(spark, "data/sample/order_items.csv")


def read_products(spark: SparkSession) -> DataFrame:
    return read_csv(spark, "data/sample/products.csv")