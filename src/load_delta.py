# Write transformed DataFrames to Delta output folders.

from pyspark.sql import DataFrame


def write_delta(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def write_partitioned_delta(df: DataFrame, path: str, partition_col: str) -> None:
    df.write.format("delta").mode("overwrite").partitionBy(partition_col).save(path)