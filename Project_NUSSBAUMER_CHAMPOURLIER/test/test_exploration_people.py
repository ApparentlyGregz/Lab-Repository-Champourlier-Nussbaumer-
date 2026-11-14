"""Check acting careers and role definitions on small IMDb-like data."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from src.exploration_people import (
    actors_with_early_rating_peak,
    movie_role_credits,
    multi_talented_performance,
)


class ExplorationPeopleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[2]").appName(
            "imdb-exploration-people-tests"
        ).config("spark.sql.shuffle.partitions", "4").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_actor_peak_must_be_early_with_twenty_year_career(self) -> None:
        years = list(range(2000, 2016)) + [2020]
        titles = self.spark.createDataFrame(
            [(f"{prefix}{i}", "movie", f"Film {prefix}{i}", str(year))
             for prefix in ("a", "b") for i, year in enumerate(years)]
            + [("a_future", "movie", "Announced film", "2031"),
               ("a_low", "movie", "Few votes", "2024")],
            ["tconst", "titleType", "primaryTitle", "startYear"],
        )
        principals = self.spark.createDataFrame(
            [(f"{prefix}{i}", person, "actor")
             for prefix, person in (("a", "nm1"), ("b", "nm2"))
             for i in range(len(years))]
            + [("a_future", "nm1", "actor"),
               ("a_low", "nm1", "actor")],
            ["tconst", "nconst", "category"],
        )
        ratings = self.spark.createDataFrame(
            [(f"{prefix}{i}",
              "8.0" if (year <= 2010) == (prefix == "a") else "6.0", "1000")
             for prefix in ("a", "b") for i, year in enumerate(years)]
            + [("a_low", "9.9", "2")],
            ["tconst", "averageRating", "numVotes"],
        )
        names = self.spark.createDataFrame(
            [("nm1", "Actor One"), ("nm2", "Actor Two")],
            ["nconst", "primaryName"],
        )
        rows = actors_with_early_rating_peak(
            titles, principals, ratings, names
        ).collect()

        self.assertEqual([row.nconst for row in rows], ["nm1"])
        self.assertEqual(rows[0].movie_count, 17)
        self.assertEqual(rows[0].last_year, 2020)
        self.assertEqual(rows[0].early_peak_rating, 8.0)
        self.assertEqual(rows[0].late_peak_rating, 6.0)
        self.assertEqual(rows[0].early_peak_votes, 1000)
        self.assertEqual(rows[0].late_peak_votes, 1000)

    def test_multi_role_talent_and_rating_comparison(self) -> None:
        titles = self.spark.createDataFrame(
            [(f"tt{i}", "movie", f"Film {i}", "2000") for i in range(1, 5)],
            ["tconst", "titleType", "primaryTitle", "startYear"],
        )
        crew = self.spark.createDataFrame(
            [
                ("tt1", "nm1", r"\N"),
                ("tt2", r"\N", "nm1"),
                ("tt3", r"\N", r"\N"),
                ("tt4", r"\N", "nm2"),
            ],
            ["tconst", "directors", "writers"],
        )
        principals = self.spark.createDataFrame(
            [
                ("tt1", "nm1", "actor"),
                ("tt3", "nm1", "actor"),
                ("tt4", "nm2", "actress"),
            ],
            ["tconst", "nconst", "category"],
        )
        ratings = self.spark.createDataFrame(
            [("tt1", "8.0", "2000"), ("tt2", "6.0", "1000"),
             ("tt3", "5.0", "1000"), ("tt4", "7.0", "1000")],
            ["tconst", "averageRating", "numVotes"],
        )
        names = self.spark.createDataFrame(
            [("nm1", "Person One"), ("nm2", "Person Two")],
            ["nconst", "primaryName"],
        )
        with TemporaryDirectory() as directory:
            talents, paired, combinations = multi_talented_performance(
                titles, crew, principals, ratings, names,
                role_credits=movie_role_credits(titles, crew, principals),
                project_roles_path=Path(directory) / "project_roles.parquet",
            )
            talent_rows = {row.nconst: row for row in talents.collect()}
            paired_rows = paired.collect()
            combo_rows = {row.roles: row for row in combinations.collect()}

        self.assertEqual(set(talent_rows), {"nm1", "nm2"})
        self.assertEqual(talent_rows["nm1"].multi_role_movie_count, 1)
        self.assertEqual([row.nconst for row in paired_rows], ["nm1"])
        self.assertEqual(paired_rows[0].multi_role_average_rating, 8.0)
        self.assertEqual(paired_rows[0].single_role_average_rating, 5.5)
        self.assertEqual(paired_rows[0].rating_difference, 2.5)
        self.assertEqual(paired_rows[0].multi_role_total_votes, 2000)
        self.assertEqual(paired_rows[0].single_role_total_votes, 2000)
        self.assertEqual(combo_rows["actor + director"].person_movie_pairs, 1)
        self.assertEqual(combo_rows["actor + writer"].person_movie_pairs, 1)


if __name__ == "__main__":
    unittest.main()
