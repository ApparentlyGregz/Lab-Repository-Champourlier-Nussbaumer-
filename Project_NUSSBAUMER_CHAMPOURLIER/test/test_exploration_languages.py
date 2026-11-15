"""Check original-language joins and the minimum film evidence."""

import unittest

from pyspark.sql import SparkSession

from src.exploration_languages import (
    actor_language_evidence, max_language_matching, multilingual_actors,
    successful_films,
)
from src.wikidata_language_collect import map_batch


class LanguageExplorationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("original-language-tests")
            .config("spark.ui.enabled", "false").getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_multilingual_requires_two_successful_films(self):
        candidates = self.spark.createDataFrame(
            [("tt1", "One", "2020", "7.5", "2000"),
             ("tt2", "Two", "2021", "8.0", "1500"),
             ("tt3", "Three", "2022", "9.0", "3000")],
            ["tconst", "primaryTitle", "year",
             "averageRating_clean", "numVotes_clean"],
        )
        languages = self.spark.createDataFrame(
            [
                ("tt1", "matched", "https://www.wikidata.org/entity/Q1",
                 ["en"]),
                ("tt2", "matched", "https://www.wikidata.org/entity/Q2",
                 ["fr"]),
                ("tt3", "matched", "https://www.wikidata.org/entity/Q3",
                 ["de", "fr"]),
            ],
            ["tconst", "status", "wikidata_uri",
             "original_language_uris"],
        )
        principals = self.spark.createDataFrame(
            [("tt1", "nm1", "actor"), ("tt2", "nm1", "actor"),
             ("tt3", "nm1", "actor"), ("tt3", "nm2", "actress")],
            ["tconst", "nconst", "category"],
        )
        names = self.spark.createDataFrame(
            [("nm1", "Actor One"), ("nm2", "Actor Two")],
            ["nconst", "primaryName"],
        )

        films = successful_films(candidates, languages)
        evidence = actor_language_evidence(films, principals, names)
        actors = multilingual_actors(evidence).collect()
        self.assertEqual(len(actors), 1)
        self.assertEqual(actors[0].nconst, "nm1")
        self.assertEqual(actors[0].language_count, 3)
        self.assertEqual(actors[0].successful_film_count, 3)
        self.assertTrue(actors[0].at_least_three_languages)
        self.assertEqual(
            evidence.where("nconst = 'nm1'").select(
                "tconst"
            ).distinct().count(), 3,
        )

    def _actors(self, films, credits, variants=None):
        candidates = self.spark.createDataFrame(
            [(tconst, tconst, "2020", "7.5", "2000")
             for tconst, _ in films],
            ["tconst", "primaryTitle", "year",
             "averageRating_clean", "numVotes_clean"],
        )
        languages = self.spark.createDataFrame(
            [(tconst, "matched", f"https://www.wikidata.org/entity/{tconst}",
              uris) for tconst, uris in films],
            ["tconst", "status", "wikidata_uri",
             "original_language_uris"],
        )
        principals = self.spark.createDataFrame(
            [(tconst, nconst, "actor") for tconst, nconst in credits],
            ["tconst", "nconst", "category"],
        )
        names = self.spark.createDataFrame(
            [(nconst, nconst) for nconst in {n for _, n in credits}],
            ["nconst", "primaryName"],
        )
        scored = successful_films(
            candidates, languages, variants=variants
        )
        evidence = actor_language_evidence(scored, principals, names)
        return {row.nconst: row for row in
                multilingual_actors(evidence).collect()}, evidence

    def test_each_language_needs_its_own_film(self):
        actors, _ = self._actors(
            [("tt1", ["en", "fr", "de"]), ("tt2", ["en"]),
             ("tt3", ["de", "fr"]), ("tt4", ["de"])],
            [("tt1", "nm1"), ("tt2", "nm1"),
             ("tt3", "nm2"), ("tt4", "nm2")],
        )
        self.assertEqual(actors["nm1"].listed_language_count, 3)
        self.assertEqual(actors["nm1"].language_count, 2)
        self.assertFalse(actors["nm1"].at_least_three_languages)
        self.assertEqual(
            sorted(actors["nm1"].language_films), ["tt1", "tt2"]
        )
        self.assertEqual(actors["nm2"].language_count, 2)

    def test_language_variants_are_merged(self):
        variants = self.spark.createDataFrame(
            [("en-us", "American English", "en", "English")],
            ["variant_uri", "variant_name", "canonical_uri",
             "canonical_name"],
        )
        actors, evidence = self._actors(
            [("tt1", ["en-us"]), ("tt2", ["en"]),
             ("tt3", ["en", "en-us"]), ("tt4", ["fr"])],
            [("tt1", "nm1"), ("tt2", "nm1"),
             ("tt3", "nm2"), ("tt4", "nm2")],
            variants,
        )
        self.assertNotIn("nm1", actors)
        self.assertEqual(actors["nm2"].language_count, 2)
        merged = evidence.where("tconst = 'tt3'").collect()
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].language_uri, "en")
        self.assertEqual(merged[0].language_name, "English")
        self.assertEqual(merged[0].source_language_uris, ["en", "en-us"])

    def test_matching_finds_the_largest_assignment(self):
        pairs = [
            {"tconst": "tt1", "language_uri": "a", "language_name": "A"},
            {"tconst": "tt1", "language_uri": "b", "language_name": "B"},
            {"tconst": "tt2", "language_uri": "a", "language_name": "A"},
        ]
        self.assertEqual(
            max_language_matching(pairs),
            [("a", "A", "tt2"), ("b", "B", "tt1")],
        )

    def test_ambiguous_wikidata_ids_are_not_assigned_a_language(self):
        response = {"results": {"bindings": [
            {"imdb": {"value": "tt1"},
             "item": {"value": "https://www.wikidata.org/entity/Q1"},
             "language": {"value": "https://www.wikidata.org/entity/L1"}},
            {"imdb": {"value": "tt1"},
             "item": {"value": "https://www.wikidata.org/entity/Q2"},
             "language": {"value": "https://www.wikidata.org/entity/L2"}},
        ]}}
        rows = map_batch(["tt1", "tt2"], response)
        self.assertEqual(rows[0]["status"], "ambiguous_wikidata_match")
        self.assertEqual(rows[0]["original_language_uris"], [])
        self.assertEqual(rows[1]["status"], "no_wikidata_match")


if __name__ == "__main__":
    unittest.main()
