# Orchestrate the full ETL pipeline from extract to load.

from src.config import PipelineConfig
from src.logger import get_logger
from src.extract import (
    read_customers,
    read_orders,
    read_order_items,
    read_products,
    read_csv,
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
from src.load_delta import write_delta, write_partitioned_delta
from src.incremental import write_initial_customers_delta, merge_customers_delta
from src.medallion import (
    write_bronze_layer,
    write_silver_layer,
    write_gold_layer,
)

logger = get_logger(__name__)


def run_pipeline(spark, config: PipelineConfig) -> None:
    logger.info("Starting ETL pipeline")

    raw_customers_df = read_customers(spark, config.customers_path)
    raw_orders_df = read_orders(spark, config.orders_path)
    raw_order_items_df = read_order_items(spark, config.order_items_path)
    raw_products_df = read_products(spark, config.products_path)

    logger.info("Raw source data loaded successfully")

    write_bronze_layer(
        config,
        raw_customers_df,
        raw_orders_df,
        raw_order_items_df,
        raw_products_df,
    )
    logger.info("Bronze layer written successfully")

    customers_df = clean_customers(raw_customers_df)
    orders_df = clean_orders(raw_orders_df)
    order_items_df = raw_order_items_df
    products_df = raw_products_df

    completed_orders_df = get_completed_orders(orders_df)
    order_items_with_total_df = add_line_total(order_items_df)
    order_totals_df = build_order_totals(order_items_with_total_df)

    sales_df = build_sales_dataset(
        completed_orders_df,
        order_totals_df,
        customers_df
    )

    write_silver_layer(
        config,
        customers_df,
        completed_orders_df,
        order_items_with_total_df,
        order_totals_df,
        sales_df,
    )
    logger.info("Silver layer written successfully")

    sales_per_customer_df = build_sales_per_customer(sales_df)
    sales_per_country_df = build_sales_per_country(sales_df)
    product_sales_df = build_product_sales(
        completed_orders_df,
        order_items_with_total_df,
        products_df
    )
    order_summary_df = build_order_summary(sales_df)

    write_gold_layer(
        config,
        sales_per_customer_df,
        sales_per_country_df,
        product_sales_df,
        order_summary_df,
    )
    logger.info("Gold layer written successfully")

    write_partitioned_parquet(
        sales_per_customer_df,
        config.sales_per_customer_output,
        "country"
    )
    write_parquet(sales_per_country_df, config.sales_per_country_output)
    write_parquet(product_sales_df, config.product_sales_output)
    write_parquet(order_summary_df, config.order_summary_output)

    logger.info("Parquet outputs written successfully")

    write_partitioned_delta(
        sales_per_customer_df,
        config.sales_per_customer_delta_output,
        "country"
    )
    write_delta(sales_per_country_df, config.sales_per_country_delta_output)
    write_delta(product_sales_df, config.product_sales_delta_output)
    write_delta(order_summary_df, config.order_summary_delta_output)

    logger.info("Delta outputs written successfully")

    write_initial_customers_delta(customers_df, config.customers_delta_output)
    logger.info("Initial customers Delta table written")

    customers_incremental_df = clean_customers(
        read_csv(spark, config.customers_incremental_path)
    )

    merge_customers_delta(
        spark,
        customers_incremental_df,
        config.customers_delta_output
    )
    logger.info("Customers incremental merge completed")

    merged_customers_df = spark.read.format("delta").load(config.customers_delta_output)

    print("=== MERGED CUSTOMERS DELTA ===")
    merged_customers_df.orderBy("customer_id").show()

    logger.info("ETL pipeline finished")