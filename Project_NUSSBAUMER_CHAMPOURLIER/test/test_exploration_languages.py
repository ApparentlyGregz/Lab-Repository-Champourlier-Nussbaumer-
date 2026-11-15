"""Check original-language joins and the minimum film evidence."""

import unittest

from pyspark.sql import SparkSession

from src.exploration_languages import (
    actor_language_evidence, multilingual_actors, successful_films,
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
