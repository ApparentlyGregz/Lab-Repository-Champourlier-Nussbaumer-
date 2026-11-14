"""First three IMDb exploration questions using Spark DataFrames."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from src.data_quality import clean_ratings, clean_titles
from src.imdb_loader import load_imdb_tables


FEATURE_MINUTES = 60
IQR_MULTIPLIER = 1.5
QUARTILE_ACCURACY = 10000
FIRST_YEAR = 1980
LAST_YEAR = 2025


def movies_by_genre(titles: DataFrame) -> DataFrame:
    """Return one row per genre assigned to a feature-length movie."""
    movies = clean_titles(titles).where(
        (F.col("titleType") == "movie")
        & (F.col("runtimeMinutes_clean") >= FEATURE_MINUTES)
    )
    return (
        movies.withColumn("year", F.expr("try_cast(startYear AS INT)"))
        .withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
        .withColumn("genre", F.trim(F.col("genre")))
        .where(F.col("genre") != "")
        .select(
            "tconst", "primaryTitle", "year", "genre",
            F.col("runtimeMinutes_clean").alias("runtime_minutes"),
        )
    )


def runtime_outliers(movies: DataFrame) -> DataFrame:
    """Find runtimes beyond each genre's Tukey IQR fences."""
    genre_stats = movies.groupBy("genre").agg(
        F.count(F.lit(1)).alias("genre_movies"),
        F.avg("runtime_minutes").alias("genre_average_minutes"),
        F.percentile_approx(
            "runtime_minutes", [0.25, 0.5, 0.75], QUARTILE_ACCURACY
        ).alias("quartiles"),
    )
    genre_stats = (
        genre_stats.withColumn("genre_q1_minutes", F.col("quartiles")[0])
        .withColumn("genre_median_minutes", F.col("quartiles")[1])
        .withColumn("genre_q3_minutes", F.col("quartiles")[2])
        .withColumn(
            "genre_iqr_minutes",
            F.col("genre_q3_minutes") - F.col("genre_q1_minutes"),
        )
        .withColumn(
            "lower_fence_minutes",
            F.col("genre_q1_minutes")
            - IQR_MULTIPLIER * F.col("genre_iqr_minutes"),
        )
        .withColumn(
            "upper_fence_minutes",
            F.col("genre_q3_minutes")
            + IQR_MULTIPLIER * F.col("genre_iqr_minutes"),
        )
        .drop("quartiles")
    )
    return (
        movies.join(genre_stats, "genre")
        .withColumn(
            "distance_beyond_fence_minutes",
            F.when(
                F.col("runtime_minutes") < F.col("lower_fence_minutes"),
                F.col("lower_fence_minutes") - F.col("runtime_minutes"),
            ).otherwise(
                F.col("runtime_minutes") - F.col("upper_fence_minutes")
            ),
        )
        .where(F.col("distance_beyond_fence_minutes") > 0)
    )


def genre_popularity(
    movies: DataFrame, ratings: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Rank genres by votes and compare the top three with other genres."""
    usable_ratings = clean_ratings(ratings).where(
        F.col("numVotes_clean") > 0
    ).select("tconst", "numVotes_clean")
    scored = (
        movies.where(F.col("year").between(FIRST_YEAR, LAST_YEAR))
        .join(usable_ratings, "tconst", "inner")
    )
    popularity = scored.groupBy("year", "genre").agg(
        F.sum("numVotes_clean").alias("total_votes"),
        F.countDistinct("tconst").alias("movie_count"),
    )
    rank_window = Window.partitionBy("year").orderBy(
        F.desc("total_votes"), F.desc("movie_count"), F.asc("genre")
    )
    ranked = popularity.withColumn("rank", F.row_number().over(rank_window)).cache()
    top_three = ranked.where(F.col("rank") <= 3)

    compared = (
        ranked.withColumn("group", F.when(F.col("rank") <= 3, "top_three")
                          .otherwise("other_genres"))
        .groupBy("year", "group")
        .agg(
            F.sum("total_votes").alias("total_votes"),
            F.sum("movie_count").alias("film_genre_count"),
            F.count(F.lit(1)).alias("genre_count"),
        )
    )
    year_window = Window.partitionBy("year")
    comparison = (
        compared.withColumn(
            "vote_share_pct",
            F.round(
                100 * F.col("total_votes")
                / F.sum("total_votes").over(year_window),
                4,
            ),
        )
        .withColumn(
            "film_genre_share_pct",
            F.round(
                100 * F.col("film_genre_count")
                / F.sum("film_genre_count").over(year_window),
                4,
            ),
        )
    )
    return top_three, comparison


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run core IMDb explorations")
    parser.add_argument(
        "--data-dir", type=Path, default=project_dir / "data" / "raw"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=project_dir / "output"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=project_dir / "results"
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-exploration-core").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = load_imdb_tables(spark, args.data_dir)
        movies = movies_by_genre(tables["title.basics"])

        outlier_path = args.output_dir / "runtime_outliers.parquet"
        runtime_outliers(movies).write.mode("overwrite").parquet(
            str(outlier_path)
        )
        saved_outliers = spark.read.parquet(str(outlier_path))
        outlier_pair_count = saved_outliers.count()
        distinct_movie_count = saved_outliers.select("tconst").distinct().count()
        by_genre = saved_outliers.groupBy("genre").agg(
            F.count(F.lit(1)).alias("outlier_pairs"),
            F.countDistinct("tconst").alias("distinct_movies"),
            F.first("genre_movies").alias("genre_movies"),
            F.first("genre_average_minutes").alias("genre_average_minutes"),
            F.first("genre_median_minutes").alias("genre_median_minutes"),
            F.first("genre_iqr_minutes").alias("genre_iqr_minutes"),
            F.first("lower_fence_minutes").alias("lower_fence_minutes"),
            F.first("upper_fence_minutes").alias("upper_fence_minutes"),
        )
        genre_rows = [
            row.asDict() for row in by_genre.orderBy("genre").collect()
        ]
        examples = [
            row.asDict()
            for row in saved_outliers.orderBy(
                F.desc("distance_beyond_fence_minutes"), F.asc("tconst")
            ).limit(20).collect()
        ]
        print(
            f"Runtime outliers: {outlier_pair_count:,} film/genre pairs, "
            f"{distinct_movie_count:,} distinct movies",
            flush=True,
        )

        top_three, comparison = genre_popularity(
            movies, tables["title.ratings"]
        )
        top_rows = [
            row.asDict()
            for row in top_three.orderBy("year", "rank").collect()
        ]
        comparison_rows = [
            row.asDict()
            for row in comparison.orderBy("year", "group").collect()
        ]
        args.results_dir.mkdir(parents=True, exist_ok=True)
        summary_path = args.results_dir / "runtime_outliers_by_genre.csv"
        with summary_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=by_genre.columns)
            writer.writeheader()
            writer.writerows(genre_rows)

        result = {
            "definitions": {
                "feature_minimum_minutes": FEATURE_MINUTES,
                "runtime_outlier_method": "Tukey_IQR",
                "runtime_outlier_iqr_multiplier": IQR_MULTIPLIER,
                "runtime_quartile_accuracy": QUARTILE_ACCURACY,
                "popularity_metric": "sum_of_IMDb_numVotes",
                "popularity_years": [FIRST_YEAR, LAST_YEAR],
            },
            "runtime_outlier_pair_count": outlier_pair_count,
            "runtime_outlier_distinct_movie_count": distinct_movie_count,
            "runtime_outliers_by_genre": genre_rows,
            "runtime_outlier_examples": examples,
            "top_three_genres_by_year": top_rows,
            "top_three_vs_others_by_year": comparison_rows,
        }
        destination = args.results_dir / "exploration_core.json"
        destination.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Results written to {destination}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
