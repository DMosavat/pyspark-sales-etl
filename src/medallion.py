# Medallion architecture helpers for Bronze, Silver, and Gold layers.

from src.load_delta import write_delta
from src.load_delta import write_partitioned_delta


def write_bronze_layer(config, customers_df, orders_df, order_items_df, products_df) -> None:
    write_delta(customers_df, config.bronze_customers_path)
    write_delta(orders_df, config.bronze_orders_path)
    write_delta(order_items_df, config.bronze_order_items_path)
    write_delta(products_df, config.bronze_products_path)


def write_silver_layer(
    config,
    customers_df,
    completed_orders_df,
    order_items_with_total_df,
    order_totals_df,
    sales_df
) -> None:
    write_delta(customers_df, config.silver_customers_path)
    write_delta(completed_orders_df, config.silver_completed_orders_path)
    write_delta(order_items_with_total_df, config.silver_order_items_with_total_path)
    write_delta(order_totals_df, config.silver_order_totals_path)
    write_delta(sales_df, config.silver_sales_path)


def write_gold_layer(
    config,
    sales_per_customer_df,
    sales_per_country_df,
    product_sales_df,
    order_summary_df
) -> None:
    write_partitioned_delta(
        sales_per_customer_df,
        config.gold_sales_per_customer_path,
        "country"
    )
    write_delta(sales_per_country_df, config.gold_sales_per_country_path)
    write_delta(product_sales_df, config.gold_product_sales_path)
    write_delta(order_summary_df, config.gold_order_summary_path)