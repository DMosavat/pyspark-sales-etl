# Entry point for running the PySpark sales ETL project.

from src.session import create_spark_session
from src.pipeline import run_pipeline


def main() -> None:
    spark = create_spark_session()
    try:
        run_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()