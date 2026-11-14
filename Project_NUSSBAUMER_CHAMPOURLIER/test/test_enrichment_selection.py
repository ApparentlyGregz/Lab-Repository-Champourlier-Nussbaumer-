"""Checks the film and principal selection used before enrichment."""

import unittest

from pyspark.sql import SparkSession

from src.enrichment_selection import (
    three_principals_per_film,
    top_films_by_genre_year,
)


class EnrichmentSelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[2]").appName(
            "imdb-enrichment-selection-tests"
        ).config("spark.sql.shuffle.partitions", "4").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_top_ten_are_ranked_by_votes_per_genre_and_year(self) -> None:
        titles = self.spark.createDataFrame(
            [(f"tt{i}", "movie", f"Film {i}", "2000", "90", "Drama")
             for i in range(12)] + [
                 ("short", "movie", "Short", "2000", "59", "Drama"),
                 ("series", "tvSeries", "Series", "2000", "90", "Drama"),
                 ("old", "movie", "Old", "1979", "90", "Drama"),
             ],
            ["tconst", "titleType", "primaryTitle", "startYear",
             "runtimeMinutes", "genres"],
        )
        ratings = self.spark.createDataFrame(
            [(f"tt{i}", "7.0", str(100 - i)) for i in range(12)] + [
                ("short", "9.0", "1000"),
                ("series", "9.0", "1000"),
                ("old", "9.0", "1000"),
            ],
            ["tconst", "averageRating", "numVotes"],
        )

        rows = top_films_by_genre_year(titles, ratings).orderBy("rank").collect()

        self.assertEqual(len(rows), 10)
        self.assertEqual([row.tconst for row in rows], [f"tt{i}" for i in range(10)])
        self.assertEqual([row.rank for row in rows], list(range(1, 11)))

    def test_principals_are_distinct_and_ranked_once_per_film(self) -> None:
        selected = self.spark.createDataFrame(
            [("tt1", "Drama"), ("tt1", "Comedy")], ["tconst", "genre"]
        )
        principals = self.spark.createDataFrame(
            [
                ("tt1", "2", "nm1", "actor"),
                ("tt1", "5", "nm1", "actor"),
                ("tt1", "1", "nm2", "director"),
                ("tt1", "3", "nm3", "writer"),
                ("tt1", "4", "nm4", "producer"),
                ("tt2", "1", "nm5", "actor"),
            ],
            ["tconst", "ordering", "nconst", "category"],
        )
        names = self.spark.createDataFrame(
            [(f"nm{i}", f"Person {i}") for i in range(1, 6)],
            ["nconst", "primaryName"],
        )

        rows = three_principals_per_film(
            selected, principals, names
        ).orderBy("principal_position").collect()

        self.assertEqual([row.nconst for row in rows], ["nm2", "nm1", "nm3"])
        self.assertEqual([row.principal_position for row in rows], [1, 2, 3])
        self.assertEqual(rows[0].primaryName, "Person 2")


if __name__ == "__main__":
    unittest.main()
