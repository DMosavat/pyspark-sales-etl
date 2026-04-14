# Unit tests for transformation functions.

from src.session import create_spark_session
from src.transform import add_line_total, build_order_totals


def test_add_line_total():
    spark = create_spark_session("test-add-line-total")

    data = [
        (101, 1001, 2, 120),
        (101, 1002, 1, 80),
    ]
    df = spark.createDataFrame(data, ["order_id", "product_id", "quantity", "unit_price"])

    result_df = add_line_total(df)
    results = result_df.select("line_total").collect()

    assert results[0]["line_total"] == 240
    assert results[1]["line_total"] == 80

    spark.stop()


def test_build_order_totals():
    spark = create_spark_session("test-build-order-totals")

    data = [
        (101, 1001, 2, 120, 240),
        (101, 1002, 1, 80, 80),
        (103, 1003, 1, 200, 200),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "product_id", "quantity", "unit_price", "line_total"]
    )

    result_df = build_order_totals(df)
    rows = {row["order_id"]: row["order_total"] for row in result_df.collect()}

    assert rows[101] == 320
    assert rows[103] == 200

    spark.stop()