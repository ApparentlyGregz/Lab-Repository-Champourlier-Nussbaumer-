"""Find actors with well-rated films in distinct original languages."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from src.imdb_loader import load_imdb_tables
from src.language_candidates import MIN_RATING, MIN_VOTES


LANGUAGE_SCHEMA = StructType([
    StructField("imdb_id", StringType()),
    StructField("entity_type", StringType()),
    StructField("status", StringType()),
    StructField("wikidata_uri", StringType()),
    StructField("original_language_uris", ArrayType(StringType())),
])
LABEL_SCHEMA = StructType([
    StructField("language_uri", StringType()),
    StructField("language_name", StringType()),
])


def load_language_map(spark: SparkSession, path: Path) -> DataFrame:
    """Read the collector's exact IMDb-to-Wikidata matches."""
    if not path.is_file():
        raise FileNotFoundError(
            f"Missing {path}; run wikipedia_collect.py --stage map first"
        )
    rows = spark.read.schema(LANGUAGE_SCHEMA).json(str(path)).where(
        F.col("entity_type") == "film"
    )
    totals = rows.agg(
        F.count("imdb_id").alias("rows"),
        F.countDistinct("imdb_id").alias("ids"),
    ).first()
    if totals.rows != totals.ids:
        raise ValueError("Duplicate film IDs in the Wikidata mapping")
    return rows.where(
        F.col("wikidata_uri").isNotNull()
    ).select(
        F.col("imdb_id").alias("tconst"),
        "status", "wikidata_uri", "original_language_uris",
    )


def successful_films(
    candidates: DataFrame, languages: DataFrame,
    labels: DataFrame | None = None,
) -> DataFrame:
    """Keep successful films with a documented original language."""
    films = candidates.select(
        "tconst", "primaryTitle",
        F.col("year").cast("int").alias("year"),
        F.col("averageRating_clean").cast("double").alias(
            "averageRating_clean"
        ),
        F.col("numVotes_clean").cast("long").alias("numVotes_clean"),
    ).dropDuplicates(["tconst"])
    result = (
        F.broadcast(films).join(languages, "tconst", "left")
        .where(F.size(F.col("original_language_uris")) > 0)
        .withColumn(
            "language_uri",
            F.explode(F.array_distinct("original_language_uris")),
        )
        .select(
            "tconst", "primaryTitle", "year", "averageRating_clean",
            "numVotes_clean", "language_uri", "wikidata_uri",
        )
    )
    if labels is not None:
        result = result.join(F.broadcast(labels), "language_uri", "left")
    else:
        result = result.withColumn("language_name", F.lit(None))
    return result.withColumn(
        "language_name",
        F.coalesce(
            F.col("language_name"),
            F.regexp_extract("language_uri", r"[^/]+$", 0),
        ),
    )


def actor_language_evidence(
    films: DataFrame, principals: DataFrame, names: DataFrame
) -> DataFrame:
    """Return one row per actor, film and original language."""
    credits = principals.where(
        F.col("category").isin("actor", "actress")
    ).select("tconst", "nconst").dropDuplicates(["tconst", "nconst"])
    return (
        credits.join(F.broadcast(films), "tconst")
        .join(names.select("nconst", "primaryName"), "nconst", "left")
        .select(
            "nconst", "primaryName", "language_uri", "language_name",
            "tconst",
            "primaryTitle", "year", "averageRating_clean",
            "numVotes_clean", "wikidata_uri",
        )
    )


def multilingual_actors(evidence: DataFrame) -> DataFrame:
    """Keep actors with successful films in at least two languages."""
    return (
        evidence.groupBy("nconst", "primaryName").agg(
            F.array_sort(F.collect_set(F.struct(
                "language_uri", "language_name"
            ))).alias(
                "languages"
            ),
            F.countDistinct("tconst").alias("successful_film_count"),
            F.countDistinct("language_uri").alias("language_count"),
        )
        .where(
            (F.col("language_count") >= 2)
            & (F.col("successful_film_count") >= 2)
        )
        .withColumn(
            "language_uris",
            F.transform("languages", lambda item: item.language_uri),
        )
        .withColumn(
            "language_names",
            F.transform("languages", lambda item: item.language_name),
        )
        .withColumn(
            "at_least_three_languages", F.col("language_count") >= 3
        )
    )


def write_csv(path: Path, rows: DataFrame, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in rows.select(*columns).toLocalIterator():
            writer.writerow(row.asDict())


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=project_dir / "data" / "raw"
    )
    parser.add_argument(
        "--map-path", type=Path,
        default=project_dir / "data" / "external"
        / "wikidata_original_languages.jsonl",
    )
    parser.add_argument(
        "--candidates", type=Path,
        default=project_dir / "output" / "language_candidates.csv",
    )
    parser.add_argument(
        "--labels-path", type=Path,
        default=project_dir / "data" / "external"
        / "wikidata_language_labels.jsonl",
    )
    parser.add_argument(
        "--results-dir", type=Path, default=project_dir / "results"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=project_dir / "output"
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("imdb-original-languages")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        tables = load_imdb_tables(spark, args.data_dir)
        candidates = spark.read.option("header", True).csv(
            str(args.candidates)
        )
        languages = load_language_map(spark, args.map_path)
        if not args.labels_path.is_file():
            raise FileNotFoundError(
                f"Missing {args.labels_path}; collect language labels first"
            )
        labels = spark.read.schema(LABEL_SCHEMA).json(
            str(args.labels_path)
        )
        scored = successful_films(
            candidates, languages, labels
        ).cache()
        evidence = actor_language_evidence(
            scored, tables["title.principals"], tables["name.basics"]
        ).cache()
        actors = multilingual_actors(evidence).cache()

        args.output_dir.mkdir(parents=True, exist_ok=True)
        scored.write.mode("overwrite").parquet(
            str(args.output_dir / "successful_language_films.parquet")
        )
        evidence.write.mode("overwrite").parquet(
            str(args.output_dir / "actor_language_evidence.parquet")
        )
        actor_csv = actors.withColumn(
            "language_uri_list", F.concat_ws("|", "language_uris")
        ).withColumn(
            "language_list", F.concat_ws("|", "language_names")
        ).orderBy(F.desc("language_count"),
                  F.desc("successful_film_count"), "nconst")
        write_csv(
            args.results_dir / "actors_multilingual.csv", actor_csv,
            ["nconst", "primaryName", "language_count",
             "language_list", "language_uri_list",
             "successful_film_count",
             "at_least_three_languages"],
        )
        write_csv(
            args.results_dir / "actor_language_evidence.csv",
            evidence.join(
                F.broadcast(actors.select("nconst")), "nconst"
            ).orderBy("nconst", "language_uri", "tconst"),
            ["nconst", "primaryName", "language_name",
             "language_uri", "tconst",
             "primaryTitle", "year", "averageRating_clean",
             "numVotes_clean", "wikidata_uri"],
        )
        candidate_count = candidates.count()
        film_count = scored.select("tconst").distinct().count()
        map_status = {
            row.status: row["count"]
            for row in spark.read.schema(LANGUAGE_SCHEMA)
            .json(str(args.map_path))
            .groupBy("status").count().collect()
        }
        summary = {
            "scope": "all rated feature films, 1980-2025",
            "minimum_rating": MIN_RATING,
            "minimum_votes": MIN_VOTES,
            "language_source": "Wikidata P364 (original language of film)",
            "successful_film_candidates": candidate_count,
            "wikidata_mapping_status": map_status,
            "successful_films_with_language": film_count,
            "successful_films_without_language": (
                candidate_count - film_count
            ),
            "actor_film_language_rows": evidence.count(),
            "actors_with_successful_films_with_language": (
                evidence.select("nconst").distinct().count()
            ),
            "actors_with_two_or_more_languages": actors.count(),
            "actors_with_three_or_more_languages": actors.where(
                F.col("language_count") >= 3
            ).count(),
        }
        destination = args.results_dir / "exploration_languages.json"
        destination.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(summary, indent=2), flush=True)
        actors.unpersist()
        evidence.unpersist()
        scored.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
