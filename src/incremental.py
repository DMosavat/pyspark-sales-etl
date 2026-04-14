# Delta incremental load utilities using merge/upsert logic.

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def write_initial_customers_delta(customers_df: DataFrame, path: str) -> None:
    customers_df.write.format("delta").mode("overwrite").save(path)


def merge_customers_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str
) -> None:
    delta_table = DeltaTable.forPath(spark, target_path)

    (
        delta_table.alias("target")
        .merge(
            source_df.alias("source"),
            "target.customer_id = source.customer_id"
        )
        .whenMatchedUpdate(
            set={
                "name": "source.name",
                "country": "source.country",
                "signup_date": "source.signup_date",
            }
        )
        .whenNotMatchedInsert(
            values={
                "customer_id": "source.customer_id",
                "name": "source.name",
                "country": "source.country",
                "signup_date": "source.signup_date",
            }
        )
        .execute()
    )