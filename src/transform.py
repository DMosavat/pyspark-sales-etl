# Apply business transformations to build analytics-ready DataFrames.

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, sum, desc, count, avg


def clean_customers(customers_df: DataFrame) -> DataFrame:
    return customers_df.withColumn("signup_date", to_date(col("signup_date")))


def clean_orders(orders_df: DataFrame) -> DataFrame:
    return orders_df.withColumn("order_date", to_date(col("order_date")))


def get_completed_orders(orders_df: DataFrame) -> DataFrame:
    return orders_df.filter(col("status") == "completed")


def add_line_total(order_items_df: DataFrame) -> DataFrame:
    return order_items_df.withColumn(
        "line_total",
        col("quantity") * col("unit_price")
    )


def build_order_totals(order_items_with_total_df: DataFrame) -> DataFrame:
    return (
        order_items_with_total_df
        .groupBy("order_id")
        .agg(sum("line_total").alias("order_total"))
    )


def build_sales_dataset(
    completed_orders_df: DataFrame,
    order_totals_df: DataFrame,
    customers_df: DataFrame
) -> DataFrame:
    return (
        completed_orders_df
        .join(order_totals_df, on="order_id", how="inner")
        .join(customers_df, on="customer_id", how="inner")
    )


def build_sales_per_customer(sales_df: DataFrame) -> DataFrame:
    return (
        sales_df
        .groupBy("customer_id", "name", "country")
        .agg(sum("order_total").alias("total_sales"))
        .orderBy(desc("total_sales"))
    )


def build_sales_per_country(sales_df: DataFrame) -> DataFrame:
    return (
        sales_df
        .groupBy("country")
        .agg(sum("order_total").alias("total_sales"))
        .orderBy(desc("total_sales"))
    )


def build_product_sales(
    completed_orders_df: DataFrame,
    order_items_with_total_df: DataFrame,
    products_df: DataFrame
) -> DataFrame:
    return (
        completed_orders_df
        .join(order_items_with_total_df, on="order_id", how="inner")
        .join(products_df, on="product_id", how="inner")
        .groupBy("product_id", "product_name", "category")
        .agg(
            sum("quantity").alias("total_quantity"),
            sum("line_total").alias("total_sales")
        )
        .orderBy(desc("total_quantity"))
    )


def build_order_summary(sales_df: DataFrame) -> DataFrame:
    return (
        sales_df
        .agg(
            count("order_id").alias("completed_order_count"),
            avg("order_total").alias("average_order_value")
        )
    )