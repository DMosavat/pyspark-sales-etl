# Central configuration for input and output paths.

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    customers_path: str = "data/sample/customers.csv"
    customers_incremental_path: str = "data/sample/customers_incremental.csv"
    orders_path: str = "data/sample/orders.csv"
    order_items_path: str = "data/sample/order_items.csv"
    products_path: str = "data/sample/products.csv"

    sales_per_customer_output: str = "output/sales_per_customer"
    sales_per_country_output: str = "output/sales_per_country"
    product_sales_output: str = "output/product_sales"
    order_summary_output: str = "output/order_summary"

    sales_per_customer_delta_output: str = "output_delta/sales_per_customer"
    sales_per_country_delta_output: str = "output_delta/sales_per_country"
    product_sales_delta_output: str = "output_delta/product_sales"
    order_summary_delta_output: str = "output_delta/order_summary"

    customers_delta_output: str = "output_delta/customers"

    bronze_customers_path: str = "medallion/bronze/customers"
    bronze_orders_path: str = "medallion/bronze/orders"
    bronze_order_items_path: str = "medallion/bronze/order_items"
    bronze_products_path: str = "medallion/bronze/products"

    silver_customers_path: str = "medallion/silver/customers"
    silver_completed_orders_path: str = "medallion/silver/completed_orders"
    silver_order_items_with_total_path: str = "medallion/silver/order_items_with_total"
    silver_order_totals_path: str = "medallion/silver/order_totals"
    silver_sales_path: str = "medallion/silver/sales"

    gold_sales_per_customer_path: str = "medallion/gold/sales_per_customer"
    gold_sales_per_country_path: str = "medallion/gold/sales_per_country"
    gold_product_sales_path: str = "medallion/gold/product_sales"
    gold_order_summary_path: str = "medallion/gold/order_summary"