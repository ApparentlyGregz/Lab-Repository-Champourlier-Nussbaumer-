"""IMDb analyses of acting careers and people working in multiple roles."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.data_quality import clean_ratings
from src.imdb_loader import load_imdb_tables


MIN_CAREER_SPAN_YEARS = 20
MIN_CAREER_MOVIES = 15
MAX_RELEASE_YEAR = 2025
MIN_MOVIE_VOTES = 1000


def movie_ids_and_years(titles: DataFrame) -> DataFrame:
    """Return IMDb movies, retaining null years for role analysis."""
    return titles.where(F.col("titleType") == "movie").select(
        "tconst", "primaryTitle",
        F.expr("try_cast(startYear AS INT)").alias("year"),
    )


def usable_ratings(ratings: DataFrame) -> DataFrame:
    return clean_ratings(ratings).where(
        F.col("averageRating_clean").isNotNull()
        & (F.col("numVotes_clean") >= MIN_MOVIE_VOTES)
    ).select("tconst", "averageRating_clean", "numVotes_clean")


def actors_with_early_rating_peak(
    titles: DataFrame, principals: DataFrame, ratings: DataFrame,
    names: DataFrame,
) -> DataFrame:
    """Find 20-year, 15-movie actors whose early ratings beat later ratings."""
    credits = principals.where(
        F.col("category").isin("actor", "actress")
    ).select("tconst", "nconst").dropDuplicates(["tconst", "nconst"])
    movie_credits = (
        credits.join(F.broadcast(usable_ratings(ratings)), "tconst")
        .join(
            F.broadcast(movie_ids_and_years(titles).where(
                F.col("year").between(1900, MAX_RELEASE_YEAR)
            ).select("tconst", "year")),
            "tconst",
        )
    )
    careers = movie_credits.groupBy("nconst").agg(
        F.count(F.lit(1)).alias("movie_count"),
        F.min("year").alias("first_year"),
        F.max("year").alias("last_year"),
    ).where(
        (F.col("movie_count") >= MIN_CAREER_MOVIES)
        & (F.col("last_year") - F.col("first_year") >= MIN_CAREER_SPAN_YEARS)
    )
    scored = (
        movie_credits.join(F.broadcast(careers), "nconst")
        .withColumn(
            "early_career",
            F.col("year") <= F.col("first_year")
            + F.floor((F.col("last_year") - F.col("first_year")) / 2),
        )
    )
    profiles = scored.groupBy(
        "nconst", "movie_count", "first_year", "last_year"
    ).agg(
        F.sum(F.when(F.col("early_career"), 1).otherwise(0)).alias(
            "early_rated_movies"
        ),
        F.sum(F.when(~F.col("early_career"), 1).otherwise(0)).alias(
            "late_rated_movies"
        ),
        F.max(F.when(
            F.col("early_career"),
            F.struct("averageRating_clean", "numVotes_clean", "tconst"),
        )).alias("early_peak"),
        F.max(F.when(
            ~F.col("early_career"),
            F.struct("averageRating_clean", "numVotes_clean", "tconst"),
        )).alias("late_peak"),
        F.avg(F.when(
            F.col("early_career"), F.col("averageRating_clean")
        )).alias("early_average_rating"),
        F.avg(F.when(
            ~F.col("early_career"), F.col("averageRating_clean")
        )).alias("late_average_rating"),
        F.sum(F.when(
            F.col("early_career"), F.col("numVotes_clean")
        ).otherwise(0)).alias("early_total_votes"),
        F.sum(F.when(
            ~F.col("early_career"), F.col("numVotes_clean")
        ).otherwise(0)).alias("late_total_votes"),
    ).withColumn(
        "early_peak_rating", F.col("early_peak.averageRating_clean")
    ).withColumn(
        "late_peak_rating", F.col("late_peak.averageRating_clean")
    ).where(
        (F.col("early_rated_movies") > 0)
        & (F.col("late_rated_movies") > 0)
        & (F.col("early_peak_rating") > F.col("late_peak_rating"))
        & (F.col("early_average_rating") > F.col("late_average_rating"))
    )
    return profiles.join(
        names.select("nconst", "primaryName"), "nconst", "left"
    ).select(
        "nconst", "primaryName", "movie_count", "first_year", "last_year",
        "early_rated_movies", "late_rated_movies", "early_peak_rating",
        "late_peak_rating", "early_average_rating", "late_average_rating",
        F.col("early_peak.tconst").alias("early_peak_movie_id"),
        F.col("early_peak.numVotes_clean").alias("early_peak_votes"),
        F.col("late_peak.tconst").alias("late_peak_movie_id"),
        F.col("late_peak.numVotes_clean").alias("late_peak_votes"),
        "early_total_votes", "late_total_votes",
    )


def movie_role_credits(
    titles: DataFrame, crew: DataFrame, principals: DataFrame
) -> DataFrame:
    """Combine acting, writing and directing credits without duplicates."""
    people_roles = principals.where(
        F.col("category").isin("actor", "actress", "director", "writer")
    ).select(
        "tconst", "nconst",
        F.when(F.col("category").isin("actor", "actress"), "actor")
        .otherwise(F.col("category")).alias("role"),
    )
    directors = crew.select(
        "tconst", F.explode(F.split(F.col("directors"), ",")).alias("nconst"),
        F.lit("director").alias("role"),
    )
    writers = crew.select(
        "tconst", F.explode(F.split(F.col("writers"), ",")).alias("nconst"),
        F.lit("writer").alias("role"),
    )
    return (
        people_roles.unionByName(directors).unionByName(writers)
        .where(F.col("nconst").isNotNull() & (F.col("nconst") != r"\N"))
        .join(movie_ids_and_years(titles).select("tconst"), "tconst")
        .dropDuplicates(["tconst", "nconst", "role"])
    )


def multi_talented_performance(
    titles: DataFrame, crew: DataFrame, principals: DataFrame,
    ratings: DataFrame, names: DataFrame,
    role_credits: DataFrame | None = None,
    project_roles_path: Path | None = None,
    reuse_project_roles: bool = False,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Compare each person's rated multi-role and single-role films."""
    if role_credits is None:
        role_credits = movie_role_credits(titles, crew, principals)
    talent_roles = role_credits.groupBy("nconst").agg(
        F.array_sort(F.collect_set("role")).alias("career_roles")
    ).where(F.size("career_roles") >= 2)
    project_roles = role_credits.groupBy("nconst", "tconst").agg(
        F.array_sort(F.collect_set("role")).alias("project_roles")
    ).join(talent_roles, "nconst")
    if project_roles_path is not None:
        if not reuse_project_roles:
            project_roles.write.mode("overwrite").parquet(
                str(project_roles_path)
            )
        project_roles = titles.sparkSession.read.parquet(
            str(project_roles_path)
        )
    talent_profiles = project_roles.groupBy("nconst").agg(
        F.first("career_roles").alias("career_roles"),
        F.count(F.lit(1)).alias("movie_count"),
        F.sum(F.when(F.size("project_roles") >= 2, 1)
              .otherwise(0)).alias("multi_role_movie_count"),
    ).join(names.select("nconst", "primaryName"), "nconst", "left")
    combinations = project_roles.where(
        F.size("project_roles") >= 2
    ).withColumn(
        "roles", F.concat_ws(" + ", F.col("project_roles"))
    ).groupBy("roles").agg(
        F.count(F.lit(1)).alias("person_movie_pairs"),
        F.countDistinct("nconst").alias("people"),
    )

    scored = project_roles.join(usable_ratings(ratings), "tconst").withColumn(
        "multi_role", F.size("project_roles") >= 2
    )
    profiles = scored.groupBy("nconst").agg(
        F.first("career_roles").alias("career_roles"),
        F.sum(F.when(F.col("multi_role"), 1).otherwise(0)).alias(
            "multi_role_rated_movies"
        ),
        F.sum(F.when(~F.col("multi_role"), 1).otherwise(0)).alias(
            "single_role_rated_movies"
        ),
        F.avg(F.when(
            F.col("multi_role"), F.col("averageRating_clean")
        )).alias("multi_role_average_rating"),
        F.avg(F.when(
            ~F.col("multi_role"), F.col("averageRating_clean")
        )).alias("single_role_average_rating"),
        F.sum(F.when(F.col("multi_role"), F.col("numVotes_clean"))
              .otherwise(0)).alias("multi_role_total_votes"),
        F.sum(F.when(~F.col("multi_role"), F.col("numVotes_clean"))
              .otherwise(0)).alias("single_role_total_votes"),
    ).where(
        (F.col("multi_role_rated_movies") > 0)
        & (F.col("single_role_rated_movies") > 0)
    ).withColumn(
        "rating_difference",
        F.col("multi_role_average_rating")
        - F.col("single_role_average_rating"),
    ).join(names.select("nconst", "primaryName"), "nconst", "left")
    return talent_profiles, profiles, combinations


def write_summary_tables(result: dict, results_dir: Path) -> None:
    """Write small tracked tables to accompany the full local Parquet lists."""
    for key, filename in (
        ("actors_with_early_peak_examples", "actors_early_peak_examples.csv"),
        ("multi_talented_people_examples", "multi_talented_examples.csv"),
        ("multi_talented_examples", "multi_role_performance_examples.csv"),
        ("multi_role_combinations", "multi_role_combinations.csv"),
    ):
        rows = result[key]
        if not rows:
            continue
        with (results_dir / filename).open(
            "w", encoding="utf-8", newline=""
        ) as file:
            writer = csv.DictWriter(file, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run IMDb people analyses")
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
        "--reuse-intermediates", action="store_true",
        help=(
            "Reuse existing role credits and per-film roles for the same "
            "IMDb snapshot"
        ),
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("imdb-exploration-people")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = load_imdb_tables(spark, args.data_dir)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        role_credits_path = args.output_dir / "movie_role_credits.parquet"
        project_roles_path = args.output_dir / "project_roles.parquet"
        if not args.reuse_intermediates:
            movie_role_credits(
                tables["title.basics"], tables["title.crew"],
                tables["title.principals"],
            ).write.mode("overwrite").parquet(str(role_credits_path))
        elif not role_credits_path.exists() or not project_roles_path.exists():
            raise FileNotFoundError("Run without --reuse-intermediates first")
        role_credits = spark.read.parquet(str(role_credits_path))
        actor_credits = role_credits.where(F.col("role") == "actor").select(
            "tconst", "nconst", F.lit("actor").alias("category")
        )
        early = actors_with_early_rating_peak(
            tables["title.basics"], actor_credits,
            tables["title.ratings"], tables["name.basics"],
        )
        talents, profiles, combinations = multi_talented_performance(
            tables["title.basics"], tables["title.crew"],
            tables["title.principals"], tables["title.ratings"],
            tables["name.basics"],
            role_credits=role_credits,
            project_roles_path=project_roles_path,
            reuse_project_roles=args.reuse_intermediates,
        )
        early_path = args.output_dir / "actors_early_rating_peak.parquet"
        talents_path = args.output_dir / "multi_talented_professionals.parquet"
        profiles_path = args.output_dir / "multi_talented_performance.parquet"
        early.write.mode("overwrite").parquet(str(early_path))
        talents.write.mode("overwrite").parquet(str(talents_path))
        profiles.write.mode("overwrite").parquet(str(profiles_path))
        saved_early = spark.read.parquet(str(early_path))
        saved_talents = spark.read.parquet(str(talents_path))
        saved_profiles = spark.read.parquet(str(profiles_path))
        early_rows = [row.asDict() for row in saved_early.orderBy(
            F.desc("movie_count"), "nconst"
        ).collect()]
        args.results_dir.mkdir(parents=True, exist_ok=True)
        with (args.results_dir / "actors_early_rating_peak.csv").open(
            "w", encoding="utf-8", newline=""
        ) as file:
            writer = csv.DictWriter(file, fieldnames=saved_early.columns)
            writer.writeheader()
            writer.writerows(early_rows)
        summary = saved_profiles.agg(
            F.count(F.lit(1)).alias("paired_people"),
            F.avg("rating_difference").alias("mean_rating_difference"),
            F.expr("percentile_approx(rating_difference, 0.5)").alias(
                "median_rating_difference"
            ),
            F.sum(F.when(
                F.col("rating_difference") > 0, 1
            ).otherwise(0)).alias("people_with_higher_multi_role_average"),
        ).first().asDict()
        result = {
            "definitions": {
                "career_minimum_year_span": MIN_CAREER_SPAN_YEARS,
                "career_minimum_movies": MIN_CAREER_MOVIES,
                "career_latest_release_year": MAX_RELEASE_YEAR,
                "movie_minimum_votes": MIN_MOVIE_VOTES,
                "career_movie_count_basis": (
                    "rated_movies_meeting_vote_threshold"
                ),
                "career_early_period": (
                    "first_half_of_release_year_span_inclusive"
                ),
                "career_peak": (
                    "early_max_and_early_mean_rating_both_exceed_late_values"
                ),
                "role_set": ["actor", "director", "writer"],
                "role_comparison": "within_person_rated_movie_averages",
            },
            "actors_with_early_peak_count": len(early_rows),
            "actors_with_early_peak_examples": early_rows[:25],
            "multi_talented_people_count": saved_talents.count(),
            "multi_talented_people_examples": [
                row.asDict()
                for row in saved_talents.orderBy(
                    F.desc("multi_role_movie_count"),
                    F.desc("movie_count"), "nconst",
                ).limit(25).collect()
            ],
            "multi_talented_paired_summary": summary,
            "multi_talented_examples": [
                row.asDict()
                for row in saved_profiles.orderBy(
                    F.desc("multi_role_rated_movies"), "nconst"
                ).limit(25).collect()
            ],
            "multi_role_combinations": [
                row.asDict()
                for row in combinations.orderBy(
                    F.desc("person_movie_pairs"), "roles"
                ).collect()
            ],
        }
        destination = args.results_dir / "exploration_people.json"
        destination.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        write_summary_tables(result, args.results_dir)
        print(
            f"Early-peak actors: {result['actors_with_early_peak_count']}; "
            f"paired multi-role professionals: {summary['paired_people']}",
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
