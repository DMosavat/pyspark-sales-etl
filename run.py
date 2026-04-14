# Entry point for running the PySpark sales ETL project.

from src.session import create_spark_session
from src.pipeline import run_pipeline
from src.config import PipelineConfig


def main() -> None:
    spark = create_spark_session()
    config = PipelineConfig()

    try:
        run_pipeline(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()