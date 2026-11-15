"""Select IMDb films and principals that require external enrichment."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from src.data_quality import clean_ratings
from src.exploration_core import FIRST_YEAR, LAST_YEAR, movies_by_genre
from src.imdb_loader import load_imdb_tables


FILMS_PER_GENRE_YEAR = 10
PRINCIPALS_PER_FILM = 3


def top_films_by_genre_year(
    titles: DataFrame, ratings: DataFrame
) -> DataFrame:
    """Rank feature-length films by IMDb votes within each year and genre."""
    films = movies_by_genre(titles).where(
        F.col("year").between(FIRST_YEAR, LAST_YEAR)
    )
    usable_ratings = clean_ratings(ratings).where(
        F.col("averageRating_clean").isNotNull()
        & (F.col("numVotes_clean") > 0)
    ).select("tconst", "averageRating_clean", "numVotes_clean")
    ranked = films.join(usable_ratings, "tconst").withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("year", "genre").orderBy(
                F.desc("numVotes_clean"),
                F.desc("averageRating_clean"),
                F.asc("tconst"),
            )
        ),
    )
    return ranked.where(F.col("rank") <= FILMS_PER_GENRE_YEAR).select(
        "year", "genre", "rank", "tconst", "primaryTitle",
        "runtime_minutes", "averageRating_clean", "numVotes_clean",
    )


def three_principals_per_film(
    selected: DataFrame, principals: DataFrame, names: DataFrame
) -> DataFrame:
    """Keep the first three distinct people by IMDb principal ordering."""
    film_ids = selected.select("tconst").distinct()
    credit_rows = (
        principals.join(F.broadcast(film_ids), "tconst")
        .where(F.col("nconst").isNotNull() & (F.col("nconst") != r"\N"))
        .select(
            "tconst", "nconst", "category",
            F.expr("try_cast(ordering AS INT)").alias("ordering_num"),
        )
        .where(F.col("ordering_num") > 0)
    )
    distinct_people = credit_rows.groupBy("tconst", "nconst").agg(
        F.min("ordering_num").alias("first_ordering"),
        F.min("category").alias("category"),
    )
    ranked = distinct_people.withColumn(
        "principal_position",
        F.row_number().over(
            Window.partitionBy("tconst").orderBy(
                F.asc("first_ordering"), F.asc("nconst")
            )
        ),
    ).where(F.col("principal_position") <= PRINCIPALS_PER_FILM)
    return ranked.join(
        names.select("nconst", "primaryName"), "nconst", "left"
    ).select(
        "tconst", "principal_position", "nconst", "primaryName", "category",
    )


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Select films and people to enrich"
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
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("imdb-enrichment-selection")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = load_imdb_tables(spark, args.data_dir)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        args.results_dir.mkdir(parents=True, exist_ok=True)

        selection_path = args.output_dir / "enrichment_films.parquet"
        top_films_by_genre_year(
            tables["title.basics"], tables["title.ratings"]
        ).write.mode("overwrite").parquet(str(selection_path))
        selected = spark.read.parquet(str(selection_path))
        people_path = args.output_dir / "enrichment_principals.parquet"
        three_principals_per_film(
            selected, tables["title.principals"], tables["name.basics"]
        ).write.mode("overwrite").parquet(str(people_path))
        people = spark.read.parquet(str(people_path))

        film_rows = [row.asDict() for row in selected.orderBy(
            "year", "genre", "rank"
        ).collect()]
        people_rows = [row.asDict() for row in people.orderBy(
            "tconst", "principal_position"
        ).collect()]
        write_csv(
            args.results_dir / "enrichment_films.csv", selected.columns,
            film_rows,
        )
        write_csv(
            args.results_dir / "enrichment_principals.csv", people.columns,
            people_rows,
        )

        distinct_films = selected.select(
            "tconst", "primaryTitle"
        ).dropDuplicates(["tconst"])
        principal_counts = people.groupBy("tconst").agg(
            F.count(F.lit(1)).alias("principal_count")
        )
        shortfalls = distinct_films.join(
            principal_counts, "tconst", "left"
        ).fillna({"principal_count": 0}).where(
            F.coalesce(F.col("principal_count"), F.lit(0))
            < PRINCIPALS_PER_FILM
        ).select("tconst", "primaryTitle", "principal_count")
        shortfall_rows = [
            row.asDict() for row in shortfalls.orderBy("tconst").collect()
        ]
        write_csv(
            args.results_dir / "enrichment_principal_shortfalls.csv",
            shortfalls.columns, shortfall_rows,
        )
        result = {
            "definitions": {
                "years": [FIRST_YEAR, LAST_YEAR],
                "feature_minimum_minutes": 60,
                "films_per_genre_year": FILMS_PER_GENRE_YEAR,
                "ranking": (
                    "IMDb_numVotes_desc_then_averageRating_desc_then_tconst"
                ),
                "principals_per_distinct_film": PRINCIPALS_PER_FILM,
                "principal_ranking": "IMDb_ordering_asc_then_nconst",
            },
            "genre_year_groups": (
                selected.select("year", "genre").distinct().count()
            ),
            "film_genre_year_rows": len(film_rows),
            "distinct_films": distinct_films.count(),
            "principal_rows": len(people_rows),
            "films_with_fewer_than_three_principals": len(shortfall_rows),
            "film_examples": film_rows[:20],
            "principal_examples": people_rows[:20],
            "principal_shortfalls": shortfall_rows,
        }
        destination = args.results_dir / "enrichment_selection.json"
        destination.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(
            f"Selected {result['film_genre_year_rows']} film/genre/year rows, "
            f"{result['distinct_films']} distinct films, "
            f"{result['principal_rows']} principals; "
            f"{result['films_with_fewer_than_three_principals']} "
            "shortfalls",
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
