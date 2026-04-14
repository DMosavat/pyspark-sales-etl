# Write transformed DataFrames to Parquet output folders.

from pyspark.sql import DataFrame


def write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def write_partitioned_parquet(df: DataFrame, path: str, partition_col: str) -> None:
    df.write.mode("overwrite").partitionBy(partition_col).parquet(path)