# Orchestrate the full ETL pipeline from extract to load.

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


def run_pipeline(spark) -> None:
    customers_df = clean_customers(read_customers(spark))
    orders_df = clean_orders(read_orders(spark))
    order_items_df = read_order_items(spark)
    products_df = read_products(spark)

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

    print("=== SALES PER CUSTOMER ===")
    sales_per_customer_df.show()

    print("=== SALES PER COUNTRY ===")
    sales_per_country_df.show()

    print("=== PRODUCT SALES ===")
    product_sales_df.show()

    print("=== ORDER SUMMARY ===")
    order_summary_df.show()

    write_partitioned_parquet(
        sales_per_customer_df,
        "output/sales_per_customer",
        "country"
    )
    write_parquet(sales_per_country_df, "output/sales_per_country")
    write_parquet(product_sales_df, "output/product_sales")
    write_parquet(order_summary_df, "output/order_summary")