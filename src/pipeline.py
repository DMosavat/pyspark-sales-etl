# Orchestrate the full ETL pipeline from extract to load.

from src.config import PipelineConfig
from src.logger import get_logger
from src.extract import (
    read_customers,
    read_orders,
    read_order_items,
    read_products,
)
from src.transform import (
    clean_customers,
    clean_orders,
    get_completed_orders,
    add_line_total,
    build_order_totals,
    build_sales_dataset,
    build_sales_per_customer,
    build_sales_per_country,
    build_product_sales,
    build_order_summary,
)
from src.load import write_parquet, write_partitioned_parquet

logger = get_logger(__name__)


def run_pipeline(spark, config: PipelineConfig) -> None:
    logger.info("Starting ETL pipeline")

    customers_df = clean_customers(read_customers(spark, config.customers_path))
    orders_df = clean_orders(read_orders(spark, config.orders_path))
    order_items_df = read_order_items(spark, config.order_items_path)
    products_df = read_products(spark, config.products_path)

    logger.info("Source data loaded successfully")

    completed_orders_df = get_completed_orders(orders_df)
    order_items_with_total_df = add_line_total(order_items_df)
    order_totals_df = build_order_totals(order_items_with_total_df)

    sales_df = build_sales_dataset(
        completed_orders_df,
        order_totals_df,
        customers_df
    )

    sales_per_customer_df = build_sales_per_customer(sales_df)
    sales_per_country_df = build_sales_per_country(sales_df)
    product_sales_df = build_product_sales(
        completed_orders_df,
        order_items_with_total_df,
        products_df
    )
    order_summary_df = build_order_summary(sales_df)

    logger.info("Transformations completed")

    sales_per_customer_df.show()
    sales_per_country_df.show()
    product_sales_df.show()
    order_summary_df.show()

    write_partitioned_parquet(
        sales_per_customer_df,
        config.sales_per_customer_output,
        "country"
    )
    write_parquet(sales_per_country_df, config.sales_per_country_output)
    write_parquet(product_sales_df, config.product_sales_output)
    write_parquet(order_summary_df, config.order_summary_output)

    logger.info("Outputs written successfully")
    logger.info("ETL pipeline finished")