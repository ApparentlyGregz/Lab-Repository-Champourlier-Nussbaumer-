"""Force a complete Spark parse of all downloaded IMDb archives."""

from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from src.imdb_loader import load_imdb_tables


def main() -> None:
    spark = SparkSession.builder.appName("imdb-full-validation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for name, table in load_imdb_tables(spark, Path("data/raw")).items():
            # Referencing every column prevents Spark from skipping CSV parsing.
            metrics = [F.count(F.lit(1)).alias("rows")]
            metrics.extend(
                F.sum(F.length(F.coalesce(F.col(column), F.lit(""))))
                .alias(f"characters_{index}")
                for index, column in enumerate(table.columns)
            )
            row_count = table.agg(*metrics).first()["rows"]
            if row_count == 0:
                raise ValueError(f"{name} is empty")
            print(f"{name}: {row_count:,} rows, all columns parsed", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
