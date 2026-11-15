"""Focused checks for Wikipedia provenance and Spark enrichment."""

import unittest

from pyspark.sql import SparkSession

from src.enrichment_wikipedia import (
    RAW_SCHEMA, enrich_films, enrich_people,
)
from src.wikipedia_collect import map_batch


class FakeClient:
    def get_json(self, _endpoint, _params):
        return {"results": {"bindings": [
            {
                "imdb": {"value": "tt0000001"},
                "item": {"value": "http://www.wikidata.org/entity/Q1"},
                "article": {"value": "https://en.wikipedia.org/wiki/Film"},
                "language": {"value": "http://www.wikidata.org/entity/Q1860"},
            },
        ]}}


class EnrichmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("wikipedia-enrichment-tests")
            .config("spark.ui.enabled", "false").getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_wikidata_id_mapping(self):
        result = map_batch(
            FakeClient(),
            [("tt0000001", "film"), ("tt0000002", "film")],
        )
        self.assertEqual(result[0]["status"], "matched")
        self.assertEqual(result[0]["wikipedia_url"],
                         "https://en.wikipedia.org/wiki/Film")
        self.assertEqual(result[0]["original_language_uris"],
                         ["http://www.wikidata.org/entity/Q1860"])
        self.assertEqual(result[1]["status"], "no_wikidata_match")

    def test_spark_plot_biography_and_missing_notes(self):
        films = self.spark.createDataFrame(
            [
                ("tt0000001", "Film", 2020, ["Drama"], 1000),
                ("tt0000002", "Unknown", 2021, ["Comedy"], 500),
            ],
            ["tconst", "primaryTitle", "year", "genres", "numVotes"],
        )
        principals = self.spark.createDataFrame(
            [("tt0000001", "nm0000001", "Person", "actor", 1)],
            ["tconst", "nconst", "primaryName", "category",
             "principal_position"],
        )
        film_extract = (
            "This is the film introduction.\n\n"
            "== Plot ==\nA family travels across a distant country and "
            "finds an unexpected home after many difficult events.\n\n"
            "== Cast ==\nSome cast members."
        )
        biography = (
            "Person was a film actor born in a small town. "
            "They worked in several productions over many years."
        )

        def raw(imdb_id, kind, extract):
            return {
                "imdb_id": imdb_id, "entity_type": kind,
                "status": "fetched", "note": None,
                "wikidata_uri": "http://www.wikidata.org/entity/Q1",
                "article_url": "https://en.wikipedia.org/wiki/Film",
                "source_uri": (
                    "https://en.wikipedia.org/w/index.php?oldid=123"
                ),
                "revision_id": 123,
                "license_uri": (
                    "https://creativecommons.org/licenses/by-sa/4.0/"
                ),
                "contributors_uri": "https://en.wikipedia.org/wiki/Film",
                "retrieved_at": "2026-11-15T00:00:00Z",
                "extract": extract,
                "original_language_uris": [],
            }

        raw_films = self.spark.createDataFrame(
            [raw("tt0000001", "film", film_extract)], RAW_SCHEMA
        )
        raw_people = self.spark.createDataFrame(
            [raw("nm0000001", "person", biography)], RAW_SCHEMA
        )
        people = enrich_people(principals, raw_people)
        self.assertEqual(people.first().enrichment_status, "enriched")
        result = {
            row.tconst: row for row in
            enrich_films(films, people, raw_films).collect()
        }
        self.assertEqual(result["tt0000001"].enrichment_status,
                         "enriched")
        self.assertEqual(result["tt0000001"].biography_count, 1)
        self.assertIn("family travels", result["tt0000001"].synopsis)
        self.assertNotIn("Cast", result["tt0000001"].synopsis)
        self.assertIsNotNone(result["tt0000001"].source_uri)
        self.assertEqual(result["tt0000002"].enrichment_status,
                         "not_collected")
        self.assertEqual(result["tt0000002"].principal_count, 0)


if __name__ == "__main__":
    unittest.main()
