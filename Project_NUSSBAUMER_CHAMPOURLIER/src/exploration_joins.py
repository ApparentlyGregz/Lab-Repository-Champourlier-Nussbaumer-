"""IMDb exploration questions that combine people, credits, episodes and ratings."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.data_quality import clean_ratings
from src.imdb_loader import load_imdb_tables


HIGH_RATING = 8.0
OTHER_AVERAGE_LIMIT = 6.0
MIN_OTHER_MOVIES = 2
MIN_LONG_SEASONS = 3
MIN_EPISODES_PER_SEASON = 10


def rated_movies(titles: DataFrame, ratings: DataFrame) -> DataFrame:
    """Return rated movies with one row per IMDb title identifier."""
    movies = titles.where(F.col("titleType") == "movie").select(
        "tconst", "primaryTitle", F.expr("try_cast(startYear AS INT)").alias("year")
    )
    usable_ratings = clean_ratings(ratings).where(
        F.col("averageRating_clean").isNotNull()
        & (F.col("numVotes_clean") > 0)
    ).select("tconst", "averageRating_clean", "numVotes_clean")
    return movies.join(usable_ratings, "tconst")


def directors_with_one_high_film(
    titles: DataFrame, crew: DataFrame, ratings: DataFrame, names: DataFrame
) -> DataFrame:
    """Find directors with exactly one >=8 film and >=2 others averaging <=6."""
    director_credits = (
        crew.select(
            "tconst",
            F.explode(F.split(F.col("directors"), ",")).alias("nconst"),
        )
        .where(F.col("nconst").isNotNull() & (F.col("nconst") != r"\N"))
        .dropDuplicates(["tconst", "nconst"])
    )
    films = director_credits.join(rated_movies(titles, ratings), "tconst")
    profile = films.groupBy("nconst").agg(
        F.sum(F.when(F.col("averageRating_clean") >= HIGH_RATING, 1)
              .otherwise(0)).alias("high_movie_count"),
        F.sum(F.when(F.col("averageRating_clean") < HIGH_RATING, 1)
              .otherwise(0)).alias("other_movie_count"),
        F.avg(F.when(F.col("averageRating_clean") < HIGH_RATING,
                     F.col("averageRating_clean"))).alias("other_average_rating"),
        F.min(F.when(F.col("averageRating_clean") >= HIGH_RATING,
                     F.col("tconst"))).alias("high_movie_id"),
        F.max(F.when(F.col("averageRating_clean") >= HIGH_RATING,
                     F.col("averageRating_clean"))).alias("high_rating"),
    ).where(
        (F.col("high_movie_count") == 1)
        & (F.col("other_movie_count") >= MIN_OTHER_MOVIES)
        & (F.col("other_average_rating") <= OTHER_AVERAGE_LIMIT)
    )
    film_names = titles.select(
        F.col("tconst").alias("high_movie_id"),
        F.col("primaryTitle").alias("high_movie_title"),
    )
    return (
        profile.join(names.select("nconst", "primaryName"), "nconst", "left")
        .join(film_names, "high_movie_id", "left")
        .select(
            "nconst", "primaryName", "high_movie_id", "high_movie_title",
            "high_rating", "other_movie_count", "other_average_rating",
        )
    )


def series_with_early_best_and_late_worst(
    titles: DataFrame, episodes: DataFrame, ratings: DataFrame
) -> DataFrame:
    """Find qualifying series, excluding ambiguous best/worst season ties."""
    episode_seasons = (
        episodes.select(
            F.col("tconst").alias("episode_id"), "parentTconst",
            F.expr("try_cast(seasonNumber AS INT)").alias("season"),
        )
        .where(F.col("season") > 0)
        .dropDuplicates(["parentTconst", "episode_id"])
    )
    seasons = episode_seasons.groupBy("parentTconst", "season").agg(
        F.count(F.lit(1)).alias("episode_count")
    )
    eligible = seasons.groupBy("parentTconst").agg(
        F.sum(F.when(F.col("episode_count") >= MIN_EPISODES_PER_SEASON, 1)
              .otherwise(0)).alias("long_season_count"),
        F.max("season").alias("last_season"),
    ).where(F.col("long_season_count") >= MIN_LONG_SEASONS)

    episode_titles = titles.where(F.col("titleType") == "tvEpisode").select(
        F.col("tconst").alias("episode_id"),
        F.col("primaryTitle").alias("episode_title"),
    )
    episode_ratings = clean_ratings(ratings).where(
        F.col("averageRating_clean").isNotNull()
        & (F.col("numVotes_clean") > 0)
    ).select(F.col("tconst").alias("episode_id"), "averageRating_clean")
    rated = (
        episode_seasons.join(eligible, "parentTconst")
        .join(episode_titles, "episode_id")
        .join(episode_ratings, "episode_id")
    )
    extremes = rated.groupBy("parentTconst").agg(
        F.max("averageRating_clean").alias("best_rating"),
        F.min("averageRating_clean").alias("worst_rating"),
    )
    marked = rated.join(extremes, "parentTconst")
    result = marked.groupBy(
        "parentTconst", "long_season_count", "last_season",
        "best_rating", "worst_rating",
    ).agg(
        F.count(F.lit(1)).alias("rated_episode_count"),
        F.max(F.when(F.col("averageRating_clean") == F.col("best_rating"),
                     F.col("season"))).alias("latest_best_season"),
        F.min(F.when(F.col("averageRating_clean") == F.col("worst_rating"),
                     F.col("season"))).alias("earliest_worst_season"),
        F.min(F.when(F.col("averageRating_clean") == F.col("best_rating"),
                     F.col("episode_id"))).alias("best_episode_id"),
        F.min(F.when(F.col("averageRating_clean") == F.col("worst_rating"),
                     F.col("episode_id"))).alias("worst_episode_id"),
    ).where(
        (F.col("latest_best_season") <= 2)
        & (F.col("earliest_worst_season") == F.col("last_season"))
    )
    series_names = titles.where(F.col("titleType") == "tvSeries").select(
        F.col("tconst").alias("parentTconst"),
        F.col("primaryTitle").alias("series_title"),
    )
    return result.join(series_names, "parentTconst").select(
        "parentTconst", "series_title", "long_season_count", "last_season",
        "rated_episode_count", "best_episode_id", "best_rating",
        "latest_best_season", "worst_episode_id", "worst_rating",
        "earliest_worst_season",
    )


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run IMDb director and series analyses")
    parser.add_argument("--data-dir", type=Path, default=project_dir / "data" / "raw")
    parser.add_argument("--output-dir", type=Path, default=project_dir / "output")
    parser.add_argument("--results-dir", type=Path, default=project_dir / "results")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-exploration-joins").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = load_imdb_tables(spark, args.data_dir)
        directors = directors_with_one_high_film(
            tables["title.basics"], tables["title.crew"],
            tables["title.ratings"], tables["name.basics"],
        )
        series = series_with_early_best_and_late_worst(
            tables["title.basics"], tables["title.episode"],
            tables["title.ratings"],
        )
        args.output_dir.mkdir(parents=True, exist_ok=True)
        director_path = args.output_dir / "directors_one_high_film.parquet"
        series_path = args.output_dir / "series_early_best_late_worst.parquet"
        directors.write.mode("overwrite").parquet(str(director_path))
        series.write.mode("overwrite").parquet(str(series_path))
        saved_directors = spark.read.parquet(str(director_path))
        saved_series = spark.read.parquet(str(series_path))
        director_rows = [row.asDict() for row in saved_directors.orderBy(
            F.desc("other_movie_count"), "nconst"
        ).collect()]
        series_rows = [row.asDict() for row in saved_series.orderBy(
            F.desc("rated_episode_count"), "parentTconst"
        ).collect()]
        args.results_dir.mkdir(parents=True, exist_ok=True)
        for filename, columns, rows in (
            ("directors_one_high_film.csv", saved_directors.columns, director_rows),
            ("series_early_best_late_worst.csv", saved_series.columns, series_rows),
        ):
            with (args.results_dir / filename).open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
        result = {
            "definitions": {
                "director_high_rating_at_least": HIGH_RATING,
                "director_other_rated_movies_at_least": MIN_OTHER_MOVIES,
                "director_other_average_rating_at_most": OTHER_AVERAGE_LIMIT,
                "series_seasons_with_at_least_10_episodes": MIN_LONG_SEASONS,
                "series_episode_rating_ties": "all_best_in_seasons_1_or_2_and_all_worst_in_last_season",
            },
            "directors_count": len(director_rows),
            "directors_examples": director_rows[:25],
            "series_count": len(series_rows),
            "series_examples": series_rows[:25],
        }
        destination = args.results_dir / "exploration_joins.json"
        destination.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"Directors: {result['directors_count']}; series: {result['series_count']}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
