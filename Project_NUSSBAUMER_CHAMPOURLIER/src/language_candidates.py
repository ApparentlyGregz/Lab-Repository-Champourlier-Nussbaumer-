"""Select all successful feature films for the original-language question."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.data_quality import clean_ratings, clean_titles
from src.imdb_loader import load_imdb_tables


MIN_RATING = 7.0
MIN_VOTES = 1000
MIN_RUNTIME = 60
FIRST_YEAR = 1980
LAST_YEAR = 2025


def candidate_films(titles: DataFrame, ratings: DataFrame) -> DataFrame:
    films = clean_titles(titles).where(
        (F.col("titleType") == "movie")
        & (F.col("runtimeMinutes_clean") >= MIN_RUNTIME)
        & F.expr("try_cast(startYear AS INT)").between(
            FIRST_YEAR, LAST_YEAR
        )
    ).select(
        "tconst", "primaryTitle",
        F.expr("try_cast(startYear AS INT)").alias("year"),
    )
    scores = clean_ratings(ratings).where(
        (F.col("averageRating_clean") >= MIN_RATING)
        & (F.col("numVotes_clean") >= MIN_VOTES)
    ).select("tconst", "averageRating_clean", "numVotes_clean")
    return films.join(scores, "tconst")


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=project_dir / "data" / "raw"
    )
    parser.add_argument(
        "--output", type=Path,
        default=project_dir / "output" / "language_candidates.csv",
    )
    args = parser.parse_args()
    spark = SparkSession.builder.appName("language-candidates").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        tables = load_imdb_tables(spark, args.data_dir)
        candidates = candidate_films(
            tables["title.basics"], tables["title.ratings"]
        ).orderBy("tconst")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with args.output.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(
                file, fieldnames=[
                    "tconst", "primaryTitle", "year",
                    "averageRating_clean", "numVotes_clean",
                ],
            )
            writer.writeheader()
            for row in candidates.toLocalIterator():
                writer.writerow(row.asDict())
                count += 1
        print(f"Successful feature films: {count}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
