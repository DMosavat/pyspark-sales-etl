# Central configuration for input and output paths.

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    customers_path: str = "data/sample/customers.csv"
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