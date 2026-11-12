"""Read the seven IMDb TSV archives with Spark."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


TABLE_COLUMNS = {
    "name.basics": (
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession", "knownForTitles",
    ),
    "title.akas": (
        "titleId", "ordering", "title", "region", "language",
        "types", "attributes", "isOriginalTitle",
    ),
    "title.basics": (
        "tconst", "titleType", "primaryTitle", "originalTitle",
        "isAdult", "startYear", "endYear", "runtimeMinutes", "genres",
    ),
    "title.crew": ("tconst", "directors", "writers"),
    "title.episode": (
        "tconst", "parentTconst", "seasonNumber", "episodeNumber",
    ),
    "title.principals": (
        "tconst", "ordering", "nconst", "category", "job", "characters",
    ),
    "title.ratings": ("tconst", "averageRating", "numVotes"),
}


def load_imdb_tables(
    spark: SparkSession, raw_dir: Path
) -> dict[str, DataFrame]:
    """Return IMDb tables with their original fields stored as strings."""
    raw_dir = Path(raw_dir)
    missing = [
        f"{name}.tsv.gz"
        for name in TABLE_COLUMNS
        if not (raw_dir / f"{name}.tsv.gz").is_file()
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing IMDb archives in {raw_dir}: {', '.join(missing)}"
        )

    tables = {}
    for name, columns in TABLE_COLUMNS.items():
        schema = StructType(
            [StructField(column, StringType(), True) for column in columns]
        )
        tables[name] = (
            spark.read.option("sep", "\t")
            .option("header", True)
            .option("quote", "")
            .option("nullValue", r"\N")
            .option("enforceSchema", False)
            .option("mode", "FAILFAST")
            .schema(schema)
            .csv(str(raw_dir / f"{name}.tsv.gz"))
        )
    return tables


def main() -> None:
    default_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    parser = argparse.ArgumentParser(description="Check the IMDb Spark input")
    parser.add_argument("--data-dir", type=Path, default=default_dir)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-loading").getOrCreate()
    try:
        for name, table in load_imdb_tables(spark, args.data_dir).items():
            table.head(1)
            print(f"{name}: {len(table.columns)} columns, read OK", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
