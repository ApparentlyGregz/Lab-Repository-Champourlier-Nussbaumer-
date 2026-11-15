"""Explore rating changes across repeated director–actor collaborations."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from src.data_quality import clean_ratings, clean_titles
from src.exploration_core import FEATURE_MINUTES, FIRST_YEAR, LAST_YEAR
from src.imdb_loader import load_imdb_tables


MIN_VOTES = 1000
MIN_COLLABORATIONS = 5
EDGE_FILMS = 2


def repeated_collaborations(
    titles: DataFrame, crew: DataFrame, principals: DataFrame,
    ratings: DataFrame, names: DataFrame,
    role_credits: DataFrame | None = None,
) -> DataFrame:
    """Compare first and last rated feature films for each duo."""
    movies = (
        clean_titles(titles)
        .where(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes_clean") >= FEATURE_MINUTES)
        )
        .withColumn("year", F.expr("try_cast(startYear AS INT)"))
        .where(F.col("year").between(FIRST_YEAR, LAST_YEAR))
        .select("tconst", "year")
    )
    scored = clean_ratings(ratings).where(
        F.col("averageRating_clean").isNotNull()
        & (F.col("numVotes_clean") >= MIN_VOTES)
    ).select("tconst", "averageRating_clean", "numVotes_clean")
    if role_credits is None:
        directors = crew.select(
            "tconst",
            F.explode(F.split(F.col("directors"), ","))
            .alias("director_id"),
        )
        actors = principals.where(
            F.col("category").isin("actor", "actress")
        ).select("tconst", F.col("nconst").alias("actor_id"))
    else:
        directors = role_credits.where(F.col("role") == "director").select(
            "tconst", F.col("nconst").alias("director_id")
        )
        actors = role_credits.where(F.col("role") == "actor").select(
            "tconst", F.col("nconst").alias("actor_id")
        )
    directors = directors.where(
        F.col("director_id").isNotNull() & (F.col("director_id") != r"\N")
    ).dropDuplicates(["tconst", "director_id"])
    actors = actors.where(
        F.col("actor_id").isNotNull() & (F.col("actor_id") != r"\N")
    ).dropDuplicates(["tconst", "actor_id"])
    collaborations = (
        movies.join(scored, "tconst")
        .join(directors, "tconst")
        .join(actors, "tconst")
        .where(F.col("director_id") != F.col("actor_id"))
        .select(
            "director_id", "actor_id", "tconst", "year",
            "averageRating_clean", "numVotes_clean",
        )
    )
    pair_window = Window.partitionBy("director_id", "actor_id")
    ordered_window = pair_window.orderBy("year", "tconst")
    numbered = collaborations.withColumn(
        "collaboration_index", F.row_number().over(ordered_window)
    ).withColumn(
        "collaboration_count", F.count(F.lit(1)).over(pair_window)
    ).where(F.col("collaboration_count") >= MIN_COLLABORATIONS)

    profiles = numbered.groupBy("director_id", "actor_id").agg(
        F.max("collaboration_count").alias("collaboration_count"),
        F.min("year").alias("first_year"),
        F.max("year").alias("last_year"),
        F.avg(F.when(
            F.col("collaboration_index") <= EDGE_FILMS,
            F.col("averageRating_clean"),
        )).alias("first_two_average_rating"),
        F.min(F.when(
            F.col("collaboration_index") <= EDGE_FILMS,
            F.col("numVotes_clean"),
        )).alias("first_two_min_votes"),
        F.avg(F.when(
            F.col("collaboration_index")
            > F.col("collaboration_count") - EDGE_FILMS,
            F.col("averageRating_clean"),
        )).alias("last_two_average_rating"),
        F.min(F.when(
            F.col("collaboration_index")
            > F.col("collaboration_count") - EDGE_FILMS,
            F.col("numVotes_clean"),
        )).alias("last_two_min_votes"),
    ).withColumn(
        # IMDb ratings have one decimal, so two decimals are exact and
        # remove floating-point noise such as 8.9e-16 for equal averages.
        "rating_change",
        F.round(
            F.col("last_two_average_rating")
            - F.col("first_two_average_rating"),
            2,
        ),
    )
    directors_names = names.select(
        F.col("nconst").alias("director_id"),
        F.col("primaryName").alias("director_name"),
    )
    actors_names = names.select(
        F.col("nconst").alias("actor_id"),
        F.col("primaryName").alias("actor_name"),
    )
    return profiles.join(
        directors_names, "director_id", "left"
    ).join(
        actors_names, "actor_id", "left"
    ).select(
        "director_id", "director_name", "actor_id", "actor_name",
        "collaboration_count", "first_year", "last_year",
        "first_two_average_rating", "first_two_min_votes",
        "last_two_average_rating", "last_two_min_votes",
        "rating_change",
    )


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Analyze recurring IMDb collaborations"
    )
    parser.add_argument(
        "--data-dir", type=Path, default=project_dir / "data" / "raw"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=project_dir / "output"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=project_dir / "results"
    )
    parser.add_argument(
        "--role-credits-parquet", type=Path,
        help="Optional existing movie_role_credits.parquet directory",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-open-question").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = load_imdb_tables(spark, args.data_dir)
        pairs = repeated_collaborations(
            tables["title.basics"], tables["title.crew"],
            tables["title.principals"], tables["title.ratings"],
            tables["name.basics"],
            role_credits=(spark.read.parquet(str(args.role_credits_parquet))
                          if args.role_credits_parquet else None),
        )
        args.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = args.output_dir / "director_actor_collaborations.parquet"
        pairs.write.mode("overwrite").parquet(str(output_path))
        saved = spark.read.parquet(str(output_path))
        rows = [row.asDict() for row in saved.orderBy(
            F.desc("collaboration_count"), "director_id", "actor_id"
        ).collect()]
        args.results_dir.mkdir(parents=True, exist_ok=True)
        with (args.results_dir / "director_actor_collaborations.csv").open(
            "w", encoding="utf-8", newline=""
        ) as file:
            writer = csv.DictWriter(file, fieldnames=saved.columns)
            writer.writeheader()
            writer.writerows(rows)
        summary = saved.agg(
            F.count(F.lit(1)).alias("pair_count"),
            F.avg("rating_change").alias("mean_rating_change"),
            F.expr("percentile_approx(rating_change, 0.5)").alias(
                "median_rating_change"
            ),
            F.sum(F.when(
                F.col("rating_change") > 0, 1
            ).otherwise(0)).alias("improved_pairs"),
            F.sum(F.when(
                F.col("rating_change") < 0, 1
            ).otherwise(0)).alias("declined_pairs"),
        ).first().asDict()
        result = {
            "question": (
                "How do ratings change from the first two to the last two "
                "films of recurring director–actor pairs?"
            ),
            "definitions": {
                "years": [FIRST_YEAR, LAST_YEAR],
                "minimum_runtime_minutes": FEATURE_MINUTES,
                "minimum_votes_per_film": MIN_VOTES,
                "minimum_pair_films": MIN_COLLABORATIONS,
                "films_at_each_career_edge": EDGE_FILMS,
                "rating_change": "last_two_mean_minus_first_two_mean",
            },
            "summary": summary,
            "examples": rows[:25],
        }
        destination = args.results_dir / "open_question.json"
        destination.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(
            f"Recurring director–actor pairs: {summary['pair_count']}",
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
