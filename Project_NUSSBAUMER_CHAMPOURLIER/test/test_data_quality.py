"""Checks for missing-value counts and Spark cleaning rules."""

import unittest

from pyspark.sql import SparkSession

from src.data_quality import (
    clean_names,
    clean_ratings,
    clean_titles,
    missing_statistics,
    split_references,
)


class DataQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[2]").appName(
            "imdb-quality-tests"
        ).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_missing_statistics_include_null_blank_and_marker(self) -> None:
        table = self.spark.createDataFrame(
            [("x", ""), (None, "  "), (r"\N", "ok")],
            ["first", "second"],
        )
        stats = missing_statistics("sample", table)

        self.assertEqual([row["missing_count"] for row in stats], [2, 2])
        self.assertEqual(stats[0]["row_count"], 3)
        self.assertEqual(stats[0]["missing_percentage"], 66.666667)

    def test_invalid_numeric_values_are_flagged_and_cleaned(self) -> None:
        names = self.spark.createDataFrame(
            [("2000", "1990"), ("bad", "2001")],
            ["birthYear", "deathYear"],
        )
        people = clean_names(names).collect()
        self.assertTrue(people[0].invalid_death_year)
        self.assertIsNone(people[0].deathYear_clean)
        self.assertTrue(people[1].invalid_birth_year)

        titles = self.spark.createDataFrame(
            [("90",), ("0",), ("bad",)], ["runtimeMinutes"]
        )
        runtimes = clean_titles(titles).collect()
        self.assertEqual(runtimes[0].runtimeMinutes_clean, 90)
        self.assertTrue(runtimes[1].invalid_runtime)
        self.assertIsNone(runtimes[2].runtimeMinutes_clean)

        ratings = self.spark.createDataFrame(
            [("8.5", "100"), ("11", "-3")],
            ["averageRating", "numVotes"],
        )
        cleaned = clean_ratings(ratings).collect()
        self.assertEqual(cleaned[0].averageRating_clean, 8.5)
        self.assertTrue(cleaned[1].invalid_rating)
        self.assertTrue(cleaned[1].invalid_votes)

    def test_orphans_are_separated_from_valid_references(self) -> None:
        parent = self.spark.createDataFrame([("tt1",)], ["tconst"])
        child = self.spark.createDataFrame(
            [("tt1",), ("tt2",), (None,)], ["parentTconst"]
        )
        valid, orphan = split_references(
            child, "parentTconst", parent, "tconst"
        )

        self.assertEqual(valid.count(), 1)
        self.assertEqual(orphan.count(), 1)


if __name__ == "__main__":
    unittest.main()
