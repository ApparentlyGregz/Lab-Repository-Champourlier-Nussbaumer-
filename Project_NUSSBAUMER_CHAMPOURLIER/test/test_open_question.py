"""Checks the recurring director–actor collaboration analysis."""

import unittest

from pyspark.sql import SparkSession

from src.open_question import repeated_collaborations


class OpenQuestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[2]").appName(
            "imdb-open-question-tests"
        ).config("spark.sql.shuffle.partitions", "4").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_first_two_and_last_two_ratings_for_repeated_pair(self) -> None:
        films = [
            (f"tt{i}", "movie", f"Film {i}", str(2000 + i), "90")
            for i in range(6)
        ]
        titles = self.spark.createDataFrame(
            films,
            [
                "tconst", "titleType", "primaryTitle", "startYear",
                "runtimeMinutes",
            ],
        )
        crew = self.spark.createDataFrame(
            [(f"tt{i}", "nm_director") for i in range(6)],
            ["tconst", "directors"],
        )
        principals = self.spark.createDataFrame(
            [(f"tt{i}", "nm_actor", "actor") for i in range(6)]
            + [(f"tt{i}", "nm_second", "actress") for i in range(4)],
            ["tconst", "nconst", "category"],
        )
        ratings = self.spark.createDataFrame(
            [(f"tt{i}", str(rating), "1000")
             for i, rating in enumerate([6, 6, 7, 7, 8, 8])],
            ["tconst", "averageRating", "numVotes"],
        )
        names = self.spark.createDataFrame(
            [("nm_director", "Director"), ("nm_actor", "Actor"),
             ("nm_second", "Second actor")],
            ["nconst", "primaryName"],
        )

        rows = repeated_collaborations(
            titles, crew, principals, ratings, names
        ).collect()
        role_credits = self.spark.createDataFrame(
            [(f"tt{i}", "nm_director", "director") for i in range(6)]
            + [(f"tt{i}", "nm_actor", "actor") for i in range(6)]
            + [(f"tt{i}", "nm_second", "actor") for i in range(4)],
            ["tconst", "nconst", "role"],
        )
        cached_rows = repeated_collaborations(
            titles, crew, principals, ratings, names, role_credits=role_credits
        ).collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows, cached_rows)
        self.assertEqual(rows[0].director_name, "Director")
        self.assertEqual(rows[0].actor_name, "Actor")
        self.assertEqual(rows[0].collaboration_count, 6)
        self.assertEqual(rows[0].first_two_average_rating, 6.0)
        self.assertEqual(rows[0].last_two_average_rating, 8.0)
        self.assertEqual(rows[0].first_two_min_votes, 1000)
        self.assertEqual(rows[0].last_two_min_votes, 1000)
        self.assertEqual(rows[0].rating_change, 2.0)


if __name__ == "__main__":
    unittest.main()
