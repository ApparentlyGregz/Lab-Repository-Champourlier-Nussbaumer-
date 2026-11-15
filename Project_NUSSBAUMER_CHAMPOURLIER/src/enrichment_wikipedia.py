"""Clean Wikipedia extracts in Spark and join them to IMDb selections."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType, LongType, StringType, StructField, StructType,
)


RAW_SCHEMA = StructType([
    StructField("imdb_id", StringType()),
    StructField("entity_type", StringType()),
    StructField("status", StringType()),
    StructField("note", StringType()),
    StructField("wikidata_uri", StringType()),
    StructField("article_url", StringType()),
    StructField("source_uri", StringType()),
    StructField("revision_id", LongType()),
    StructField("license_uri", StringType()),
    StructField("contributors_uri", StringType()),
    StructField("retrieved_at", StringType()),
    StructField("extract", StringType()),
    StructField("original_language_uris", ArrayType(StringType())),
])

PLOT_HEADING = re.compile(
    r"(?im)^==\s*(?:Plot|Plot summary|Synopsis|Story|Storyline|Premise)"
    r"\s*==\s*$"
)
NEXT_HEADING = re.compile(r"(?m)^==[^=].*?==\s*$")


@F.udf(StringType())
def plot_from_extract(extract):
    """Take a human-written plot section from a plain-text API extract."""
    if not extract:
        return None
    heading = PLOT_HEADING.search(extract)
    if heading is None:
        return None
    remainder = extract[heading.end():]
    next_heading = NEXT_HEADING.search(remainder)
    plot = remainder[:next_heading.start()] if next_heading else remainder
    plot = re.sub(r"\n{3,}", "\n\n", plot).strip()
    return plot if len(plot) >= 50 else None


@F.udf(StringType())
def biography_from_extract(extract):
    """Keep Wikipedia's human-written introduction as biography text."""
    if not extract:
        return None
    biography = re.sub(r"\n{3,}", "\n\n", extract).strip()
    return biography if len(biography) >= 50 else None


def read_raw(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        return spark.createDataFrame([], RAW_SCHEMA)
    data = spark.read.schema(RAW_SCHEMA).json(str(path))
    counts = data.agg(
        F.count("imdb_id").alias("rows"),
        F.countDistinct("imdb_id").alias("ids"),
    ).first()
    if counts.rows != counts.ids:
        raise ValueError(f"Duplicate IMDb IDs in {path}")
    return data


def selected_films(spark: SparkSession, results_dir: Path) -> DataFrame:
    rows = (
        spark.read.option("header", True).csv(
            str(results_dir / "enrichment_films.csv")
        )
    )
    return rows.groupBy("tconst", "primaryTitle").agg(
        F.min(F.col("year").cast("int")).alias("year"),
        F.array_sort(F.collect_set("genre")).alias("genres"),
        F.max(F.col("numVotes_clean").cast("long")).alias("numVotes"),
    )


def selected_people(spark: SparkSession, results_dir: Path) -> DataFrame:
    return spark.read.option("header", True).csv(
        str(results_dir / "enrichment_principals.csv")
    ).select(
        "tconst", "nconst", "primaryName", "category",
        F.col("principal_position").cast("int").alias(
            "principal_position"
        ),
    )


def enrich_people(selected: DataFrame, raw: DataFrame) -> DataFrame:
    source = raw.select(
        F.col("imdb_id").alias("nconst"),
        F.col("status").alias("source_status"),
        "wikidata_uri", "article_url", "source_uri", "revision_id",
        "license_uri", "contributors_uri", "retrieved_at", "extract",
    )
    joined = selected.join(source, "nconst", "left")
    joined = joined.withColumn(
        "biography", biography_from_extract(F.col("extract"))
    )
    return joined.withColumn(
        "enrichment_status",
        F.when(F.col("source_status").isNull(), "not_collected")
        .when(F.col("source_status") != "fetched", F.col("source_status"))
        .when(F.col("biography").isNull(), "biography_missing")
        .otherwise("enriched"),
    ).drop("extract")


def enrich_films(
    selected: DataFrame, people: DataFrame, raw: DataFrame
) -> DataFrame:
    source = raw.select(
        F.col("imdb_id").alias("tconst"),
        F.col("status").alias("source_status"),
        "wikidata_uri", "article_url", "source_uri", "revision_id",
        "license_uri", "contributors_uri", "retrieved_at", "extract",
        "original_language_uris",
    )
    person_counts = people.groupBy("tconst").agg(
        F.count("nconst").alias("principal_count"),
        F.sum(F.when(F.col("enrichment_status") == "enriched", 1)
              .otherwise(0)).alias("biography_count"),
    )
    joined = (
        selected.join(source, "tconst", "left")
        .join(person_counts, "tconst", "left")
        .fillna({"principal_count": 0, "biography_count": 0})
        .withColumn("synopsis", plot_from_extract(F.col("extract")))
    )
    return joined.withColumn(
        "enrichment_status",
        F.when(F.col("source_status").isNull(), "not_collected")
        .when(F.col("source_status") != "fetched", F.col("source_status"))
        .when(F.col("synopsis").isNull(), "plot_section_missing")
        .otherwise("enriched"),
    ).withColumn(
        "principal_note",
        F.when(F.col("principal_count") < 3,
               F.concat(F.lit("IMDb has only "),
                        F.col("principal_count").cast("string"),
                        F.lit(" principal credits for this film")))
    ).withColumn(
        "biography_note",
        F.when(F.col("biography_count") < 3,
               F.concat(F.lit("Only "),
                        F.col("biography_count").cast("string"),
                        F.lit(" of three biographies were found")))
    ).drop("extract")


def write_csv(path: Path, data: DataFrame, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in data.select(*columns).toLocalIterator():
            writer.writerow(row.asDict())


def status_counts(data: DataFrame) -> dict[str, int]:
    return {row.enrichment_status: row["count"] for row in
            data.groupBy("enrichment_status").count().collect()}


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-dir", type=Path,
                        default=project_dir / "data" / "external")
    parser.add_argument("--results-dir", type=Path,
                        default=project_dir / "results")
    parser.add_argument("--output-dir", type=Path,
                        default=project_dir / "output")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("imdb-wikipedia-enrichment")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        selected = selected_films(spark, args.results_dir)
        principals = selected_people(spark, args.results_dir)
        raw_films = read_raw(
            spark, args.external_dir / "wikipedia_raw_films.jsonl"
        )
        raw_people = read_raw(
            spark, args.external_dir / "wikipedia_raw_people.jsonl"
        )
        people = enrich_people(principals, raw_people).cache()
        films = enrich_films(selected, people, raw_films).cache()
        args.output_dir.mkdir(parents=True, exist_ok=True)
        films.write.mode("overwrite").parquet(
            str(args.output_dir / "enriched_films.parquet")
        )
        people.write.mode("overwrite").parquet(
            str(args.output_dir / "enriched_principals.parquet")
        )
        film_csv = films.select(
            "*", F.concat_ws("|", "genres").alias("genre_list"),
            F.concat_ws("|", "original_language_uris").alias(
                "original_language_list"
            ),
        ).orderBy("year", "tconst")
        film_columns = [
            "tconst", "primaryTitle", "year", "genre_list", "numVotes",
            "synopsis", "enrichment_status", "principal_count",
            "biography_count", "principal_note", "biography_note",
            "wikidata_uri", "article_url", "source_uri", "revision_id",
            "license_uri", "contributors_uri", "retrieved_at",
            "original_language_list",
        ]
        person_columns = [
            "tconst", "principal_position", "nconst", "primaryName",
            "category", "biography", "enrichment_status",
            "wikidata_uri", "article_url", "source_uri", "revision_id",
            "license_uri", "contributors_uri", "retrieved_at",
        ]
        write_csv(args.results_dir / "enriched_films.csv", film_csv,
                  film_columns)
        write_csv(
            args.results_dir / "enriched_principals.csv",
            people.orderBy("tconst", "principal_position"), person_columns,
        )
        film_status = status_counts(films)
        person_status = status_counts(people)
        summary = {
            "source": "English Wikipedia via Wikidata IMDb ID P345",
            "film_count": films.count(),
            "film_status": film_status,
            "films_with_synopsis": film_status.get("enriched", 0),
            "films_with_three_biographies": films.where(
                F.col("biography_count") >= 3
            ).count(),
            "person_credit_count": people.count(),
            "person_status": person_status,
            "distinct_people_with_biography": people.where(
                F.col("enrichment_status") == "enriched"
            ).select("nconst").distinct().count(),
            "films_with_fewer_than_three_imdb_principals": films.where(
                F.col("principal_count") < 3
            ).count(),
        }
        (args.results_dir / "enrichment_wikipedia_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
        films.unpersist()
        people.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
