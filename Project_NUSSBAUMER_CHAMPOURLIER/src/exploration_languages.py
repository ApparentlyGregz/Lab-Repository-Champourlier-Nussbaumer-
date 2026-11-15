"""Find actors with well-rated films in distinct original languages."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from src.imdb_loader import load_imdb_tables
from src.language_candidates import MIN_RATING, MIN_VOTES


LANGUAGE_SCHEMA = StructType([
    StructField("imdb_id", StringType()),
    StructField("entity_type", StringType()),
    StructField("status", StringType()),
    StructField("wikidata_uri", StringType()),
    StructField("original_language_uris", ArrayType(StringType())),
    StructField("retrieved_at", StringType()),
])
LABEL_SCHEMA = StructType([
    StructField("language_uri", StringType()),
    StructField("language_name", StringType()),
])
VARIANT_SCHEMA = StructType([
    StructField("variant_uri", StringType()),
    StructField("variant_name", StringType()),
    StructField("canonical_uri", StringType()),
    StructField("canonical_name", StringType()),
])
MATCH_SCHEMA = ArrayType(StructType([
    StructField("language_uri", StringType()),
    StructField("language_name", StringType()),
    StructField("tconst", StringType()),
]))


def load_language_map(spark: SparkSession, path: Path) -> DataFrame:
    """Read the collector's exact IMDb-to-Wikidata matches."""
    if not path.is_file():
        raise FileNotFoundError(
            f"Missing {path}; run wikidata_language_collect.py first"
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
        "status", "wikidata_uri", "original_language_uris", "retrieved_at",
    )


def load_language_variants(spark: SparkSession, path: Path) -> DataFrame:
    """Read the table that maps Wikidata language variants to one language."""
    if not path.is_file():
        raise FileNotFoundError(f"Missing language variant table {path}")
    variants = spark.read.option("header", True).schema(
        VARIANT_SCHEMA
    ).csv(str(path))
    rows = variants.collect()
    variant_uris = [row.variant_uri for row in rows]
    if len(variant_uris) != len(set(variant_uris)):
        raise ValueError("A language variant is listed more than once")
    if set(variant_uris) & {row.canonical_uri for row in rows}:
        raise ValueError("A canonical language is also listed as a variant")
    return variants


def successful_films(
    candidates: DataFrame, languages: DataFrame,
    labels: DataFrame | None = None,
    variants: DataFrame | None = None,
) -> DataFrame:
    """Return one row per successful film and original language.

    Wikidata variants listed in ``variants`` (for example American
    English) are merged into their language (English). The original P364
    values are kept in ``source_language_uris``.
    """
    films = candidates.select(
        "tconst", "primaryTitle",
        F.col("year").cast("int").alias("year"),
        F.col("averageRating_clean").cast("double").alias(
            "averageRating_clean"
        ),
        F.col("numVotes_clean").cast("long").alias("numVotes_clean"),
    ).dropDuplicates(["tconst"])
    if "retrieved_at" not in languages.columns:
        languages = languages.withColumn(
            "retrieved_at", F.lit(None).cast("string")
        )
    exploded = (
        F.broadcast(films).join(languages, "tconst", "left")
        .where(F.size(F.col("original_language_uris")) > 0)
        .withColumn(
            "source_language_uri",
            F.explode(F.array_distinct("original_language_uris")),
        )
    )
    if variants is not None:
        exploded = exploded.join(
            F.broadcast(variants.select(
                F.col("variant_uri").alias("source_language_uri"),
                "canonical_uri", "canonical_name",
            )),
            "source_language_uri", "left",
        )
    else:
        exploded = exploded.withColumn(
            "canonical_uri", F.lit(None).cast("string")
        ).withColumn("canonical_name", F.lit(None).cast("string"))
    film_columns = [
        "tconst", "primaryTitle", "year", "averageRating_clean",
        "numVotes_clean", "wikidata_uri", "retrieved_at",
    ]
    result = exploded.withColumn(
        "language_uri",
        F.coalesce(F.col("canonical_uri"), F.col("source_language_uri")),
    ).groupBy(*film_columns, "language_uri").agg(
        F.array_sort(F.collect_set("source_language_uri")).alias(
            "source_language_uris"
        ),
        F.first("canonical_name", ignorenulls=True).alias("canonical_name"),
    )
    if labels is not None:
        result = result.join(F.broadcast(labels), "language_uri", "left")
    else:
        result = result.withColumn(
            "language_name", F.lit(None).cast("string")
        )
    return result.withColumn(
        "language_name",
        F.coalesce(
            F.col("language_name"),
            F.col("canonical_name"),
            F.regexp_extract("language_uri", r"[^/]+$", 0),
        ),
    ).drop("canonical_name").withColumn(
        "film_language_count",
        F.count(F.lit(1)).over(Window.partitionBy("tconst")),
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
            "source_language_uris", "film_language_count", "tconst",
            "primaryTitle", "year", "averageRating_clean",
            "numVotes_clean", "wikidata_uri", "retrieved_at",
        )
    )


def max_language_matching(pairs) -> list[tuple[str, str, str]]:
    """Pair as many languages as possible with a different film each.

    ``pairs`` holds (tconst, language_uri, language_name) items. The result
    is a maximum bipartite matching found with augmenting paths, so a film
    listed in several languages backs only one of them.
    """
    films_by_language = defaultdict(set)
    names = {}
    for pair in pairs:
        films_by_language[pair["language_uri"]].add(pair["tconst"])
        names[pair["language_uri"]] = pair["language_name"]
    language_by_film: dict[str, str] = {}

    def assign(language: str, seen: set[str]) -> bool:
        for film in sorted(films_by_language[language]):
            if film in seen:
                continue
            seen.add(film)
            if (film not in language_by_film
                    or assign(language_by_film[film], seen)):
                language_by_film[film] = language
                return True
        return False

    for language in sorted(
        films_by_language,
        key=lambda item: (len(films_by_language[item]), item),
    ):
        assign(language, set())
    return sorted(
        (language, names[language], film)
        for film, language in language_by_film.items()
    )


def multilingual_actors(evidence: DataFrame) -> DataFrame:
    """Keep actors with a distinct successful film in at least two languages.

    ``language_count`` is the number of languages that can each be backed
    by a different film. ``listed_language_count`` counts every language of
    every film, even when one multilingual film provides several of them.
    """
    matching = F.udf(max_language_matching, MATCH_SCHEMA, useArrow=False)
    grouped = evidence.groupBy("nconst", "primaryName").agg(
        F.collect_set(F.struct(
            "tconst", "language_uri", "language_name"
        )).alias("pairs"),
        F.countDistinct("tconst").alias("successful_film_count"),
        F.countDistinct("language_uri").alias("listed_language_count"),
        F.array_sort(F.collect_set("language_name")).alias(
            "listed_language_names"
        ),
    ).where(
        (F.col("listed_language_count") >= 2)
        & (F.col("successful_film_count") >= 2)
    )
    return (
        grouped.withColumn("matched", matching("pairs"))
        .withColumn("language_count", F.size("matched"))
        .where(F.col("language_count") >= 2)
        .withColumn(
            "language_uris",
            F.transform("matched", lambda item: item.language_uri),
        )
        .withColumn(
            "language_names",
            F.transform("matched", lambda item: item.language_name),
        )
        .withColumn(
            "language_films",
            F.transform("matched", lambda item: item.tconst),
        )
        .withColumn(
            "at_least_three_languages", F.col("language_count") >= 3
        )
        .drop("pairs", "matched")
    )


def rule_counts(rows: DataFrame) -> dict:
    """Count actors with 2+ and 3+ languages under three counting rules.

    ``rows`` needs nconst, primaryName, tconst, language_uri,
    language_name and film_language_count.
    """
    def counts(frame: DataFrame, column: str) -> dict:
        return {
            "two_or_more": frame.where(F.col(column) >= 2).count(),
            "three_or_more": frame.where(F.col(column) >= 3).count(),
        }

    every_language = rows.groupBy("nconst").agg(
        F.countDistinct("language_uri").alias("languages"),
        F.countDistinct("tconst").alias("films"),
    ).where(F.col("films") >= 2)
    single_language = rows.where(
        F.col("film_language_count") == 1
    ).groupBy("nconst").agg(
        F.countDistinct("language_uri").alias("languages")
    )
    return {
        "every_language_of_every_film": counts(every_language, "languages"),
        "one_distinct_film_per_language": counts(
            multilingual_actors(rows), "language_count"
        ),
        "single_language_films_only": counts(single_language, "languages"),
    }


def raw_language_rows(evidence: DataFrame) -> DataFrame:
    """Rebuild actor rows with the original P364 values, before merging."""
    rows = evidence.select(
        "nconst", "primaryName", "tconst",
        F.explode("source_language_uris").alias("language_uri"),
    ).distinct()
    per_film = rows.select("tconst", "language_uri").distinct().groupBy(
        "tconst"
    ).agg(F.count(F.lit(1)).alias("film_language_count"))
    return rows.join(per_film, "tconst").withColumn(
        "language_name", F.col("language_uri")
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
        "--variants-path", type=Path,
        default=project_dir / "reference" / "language_variants.csv",
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
        variants = load_language_variants(spark, args.variants_path)
        scored = successful_films(
            candidates, languages, labels, variants
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
        ).withColumn(
            "language_film_list", F.concat_ws("|", "language_films")
        ).withColumn(
            "listed_language_list",
            F.concat_ws("|", "listed_language_names"),
        ).orderBy(F.desc("language_count"),
                  F.desc("successful_film_count"), "nconst")
        write_csv(
            args.results_dir / "actors_multilingual.csv", actor_csv,
            ["nconst", "primaryName", "language_count",
             "language_list", "language_uri_list", "language_film_list",
             "listed_language_count", "listed_language_list",
             "successful_film_count", "at_least_three_languages"],
        )
        write_csv(
            args.results_dir / "actor_language_evidence.csv",
            evidence.join(
                F.broadcast(actors.select("nconst")), "nconst"
            ).withColumn(
                "source_language_uri_list",
                F.concat_ws("|", "source_language_uris"),
            ).withColumnRenamed(
                "retrieved_at", "wikidata_retrieved_at"
            ).orderBy("nconst", "language_uri", "tconst"),
            ["nconst", "primaryName", "language_name",
             "language_uri", "source_language_uri_list",
             "film_language_count", "tconst",
             "primaryTitle", "year", "averageRating_clean",
             "numVotes_clean", "wikidata_uri", "wikidata_retrieved_at"],
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
            "counting_rule": (
                "each language must be backed by a different successful "
                "film; listed Wikidata variants are merged"
            ),
            "merged_language_variants": variants.count(),
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
            "sensitivity": {
                "with_variant_merge": rule_counts(evidence),
                "without_variant_merge": rule_counts(
                    raw_language_rows(evidence)
                ),
            },
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
