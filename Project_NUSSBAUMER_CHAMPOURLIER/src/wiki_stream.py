"""Match live Wikipedia article changes to rated IMDb films and series."""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("wiki", StringType()),
    StructField("title", StringType()),
    StructField("type", StringType()),
    StructField("event_timestamp", LongType()),
    StructField("source_uri", StringType()),
])

TITLE_COLUMNS = (
    "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
    "startYear", "endYear", "runtimeMinutes", "genres",
)
WIKI_DISAMBIGUATION = (
    r"(?i)\s*\((?:(\d{4})\s+)?"
    r"(film|television series|tv series)\)\s*$"
)


def title_key(column):
    """Use the same simple, reproducible key for IMDb and Wikipedia titles."""
    return F.lower(F.trim(F.regexp_replace(column, r"\s+", " ")))


def wikipedia_title_key(column):
    # Wikipedia film pages often have a year or medium in parentheses.
    bare = F.regexp_replace(column, WIKI_DISAMBIGUATION, "")
    return title_key(bare)


def build_title_lookup(spark: SparkSession, raw_dir: Path) -> DataFrame:
    """Read only the IMDb fields needed for a stable stream/static join."""
    title_schema = StructType([
        StructField(name, StringType()) for name in TITLE_COLUMNS
    ])
    titles = (
        spark.read.option("sep", "\t").option("header", True)
        .option("quote", "").option("nullValue", r"\N")
        .option("enforceSchema", False).option("mode", "FAILFAST")
        .schema(title_schema).csv(str(raw_dir / "title.basics.tsv.gz"))
        .filter(F.col("titleType").isin("movie", "tvSeries", "tvMiniSeries"))
        .filter(F.col("startYear").cast("int").between(1980, 2025))
    )
    ratings_schema = StructType([
        StructField("tconst", StringType()),
        StructField("averageRating", StringType()),
        StructField("numVotes", StringType()),
    ])
    rated = (
        spark.read.option("sep", "\t").option("header", True)
        .option("enforceSchema", False).schema(ratings_schema)
        .csv(str(raw_dir / "title.ratings.tsv.gz"))
        .filter(F.col("numVotes").cast("long") >= 100)
        .select("tconst", "numVotes")
    )
    joined = titles.join(rated, "tconst")
    keys = joined.select(
        "tconst", "titleType", "startYear", "numVotes",
        F.explode(F.array("primaryTitle", "originalTitle")).alias(
            "imdb_title"
        ),
    )
    return (
        keys.withColumn("title_key", title_key(F.col("imdb_title")))
        .filter(F.col("title_key").isNotNull() & (F.col("title_key") != ""))
        .dropDuplicates(["tconst", "title_key"])
    )


def parse_kafka_values(kafka_df: DataFrame) -> DataFrame:
    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias(
            "event"
        )
    ).select("event.*")
    filtered = parsed.filter(
        F.col("event_id").isNotNull()
        & F.col("title").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & (F.col("wiki") == "enwiki")
    )
    year_text = F.regexp_extract(
        F.col("title"), WIKI_DISAMBIGUATION, 1
    )
    medium_text = F.lower(F.regexp_extract(
        F.col("title"), WIKI_DISAMBIGUATION, 2
    ))
    return (
        filtered
        .withColumn(
            "event_time", F.to_timestamp(F.from_unixtime("event_timestamp"))
        )
        .withColumn("title_key", wikipedia_title_key(F.col("title")))
        .withColumn(
            "wiki_year", F.when(year_text != "", year_text.cast("int"))
        )
        .withColumn(
            "wiki_medium", F.when(medium_text != "", medium_text)
        )
    )


def match_events(events: DataFrame, lookup: DataFrame) -> DataFrame:
    return (
        events.where(F.col("wiki_medium").isNotNull())
        .join(F.broadcast(lookup), "title_key", "inner")
        .where(
            F.col("wiki_year").isNull()
            | (F.col("wiki_year") == F.col("startYear").cast("int"))
        )
        .where(
            (
                (F.col("wiki_medium") == "film")
                & (F.col("titleType") == "movie")
            )
            | (
                F.col("wiki_medium").isin("tv series", "television series")
                & F.col("titleType").isin("tvSeries", "tvMiniSeries")
            )
        )
        .select(
            "event_id", "event_time", "title", "type", "source_uri",
            "wiki_year", "wiki_medium", "tconst", "imdb_title",
            "titleType", "startYear", "numVotes",
        )
        .dropDuplicates(["event_id", "tconst"])
    )


def summarize(
    results_dir: Path, output_dir: Path, batch_dir: Path,
    run_id: str, spark: SparkSession,
) -> None:
    batches = [json.loads(path.read_text(encoding="utf-8"))
               for path in sorted(batch_dir.glob("batch_*.json"))]
    summary = {
        "run_id": run_id,
        "source": "https://stream.wikimedia.org/v2/stream/recentchange",
        "scope": "English Wikipedia article edits and creations",
        "imdb_scope": "rated films and series, 1980-2025, at least 100 votes",
        "processed_events": sum(item["processed_events"] for item in batches),
        "matched_event_entity_pairs": sum(
            item["matched_pairs"] for item in batches
        ),
        "batches": len(batches),
        "first_event_time": min((item["first_event_time"] for item in batches
                                 if item["first_event_time"]), default=None),
        "last_event_time": max((item["last_event_time"] for item in batches
                                if item["last_event_time"]), default=None),
    }
    results_dir.mkdir(parents=True, exist_ok=True)
    paths = [
        str(path) for path in sorted((output_dir / "matches").glob("batch_*"))
    ]
    sample = []
    columns = [
        "event_id", "event_time", "title", "type", "source_uri",
        "wiki_year", "wiki_medium", "tconst", "imdb_title",
        "titleType", "startYear", "numVotes",
    ]
    if paths:
        matches = spark.read.parquet(*paths)
        summary["distinct_events"] = (
            matches.select("event_id").distinct().count()
        )
        summary["distinct_imdb_titles"] = (
            matches.select("tconst").distinct().count()
        )
        sample = (
            matches.orderBy("event_time", "event_id").limit(100).collect()
        )
    else:
        summary["distinct_events"] = 0
        summary["distinct_imdb_titles"] = 0
    (results_dir / "streaming_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with (results_dir / "streaming_match_examples.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(row.asDict() for row in sample)


def main() -> None:
    base = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=base / "data" / "raw"
    )
    parser.add_argument("--broker", default="broker:19092")
    parser.add_argument("--topic", default="wikipedia-recentchange")
    parser.add_argument("--minutes", type=int, default=20)
    parser.add_argument(
        "--run-id", default=datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        ),
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.run_id):
        parser.error("--run-id may contain only letters, digits, _ and -")
    spark = (
        SparkSession.builder.appName("imdb-wikipedia-stream").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    output_dir = base / "output" / "streaming" / args.run_id
    results_dir = base / "results"
    batch_dir = results_dir / "streaming_batches" / args.run_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    lookup = build_title_lookup(spark, args.data_dir).cache()
    print(f"IMDb title keys: {lookup.count()}", flush=True)

    def process_batch(batch: DataFrame, batch_id: int) -> None:
        batch = batch.cache()
        processed = batch.count()
        matches = match_events(batch, lookup).cache()
        match_count = matches.count()
        if match_count:
            matches.write.mode("overwrite").parquet(
                str(output_dir / "matches" / f"batch_{batch_id:06d}")
            )
        bounds = batch.agg(
            F.min("event_time").alias("first"),
            F.max("event_time").alias("last"),
        ).first()
        metric = {
            "batch_id": batch_id,
            "processed_events": processed,
            "matched_pairs": match_count,
            "first_event_time": str(bounds.first) if bounds.first else None,
            "last_event_time": str(bounds.last) if bounds.last else None,
        }
        (batch_dir / f"batch_{batch_id:06d}.json").write_text(
            json.dumps(metric) + "\n", encoding="utf-8"
        )
        print(
            f"Batch {batch_id}: {processed} changes, "
            f"{match_count} IMDb matches", flush=True,
        )
        matches.unpersist()
        batch.unpersist()

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.broker)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    events = (
        parse_kafka_values(source)
        .withWatermark("event_time", "30 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    query = (
        events.writeStream.foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            str(base / "checkpoints" / "wiki_stream" / args.run_id),
        )
        .trigger(processingTime="30 seconds")
        .start()
    )
    try:
        query.awaitTermination(args.minutes * 60)
    finally:
        query.stop()
        summarize(results_dir, output_dir, batch_dir, args.run_id, spark)
        spark.stop()


if __name__ == "__main__":
    main()
