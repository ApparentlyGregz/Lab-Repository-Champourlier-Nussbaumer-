"""Checks for the multi-table IMDb director and series questions."""

import unittest

from pyspark.sql import SparkSession

from src.exploration_joins import (
    directors_with_one_high_film,
    series_with_early_best_and_late_worst,
)


class ExplorationJoinsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[2]").appName(
            "imdb-exploration-joins-tests"
        ).config("spark.sql.shuffle.partitions", "4").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_directors_need_one_high_film_and_low_other_average(
        self,
    ) -> None:
        films = self.spark.createDataFrame(
            [
                ("tt1", "movie", "Excellent", "2000"),
                ("tt2", "movie", "Weak A", "2001"),
                ("tt3", "movie", "Weak B", "2002"),
                ("tt4", "movie", "Excellent 2", "2003"),
                ("tt5", "short", "Short", "2004"),
                ("tt6", "movie", "Too few votes", "2005"),
            ],
            ["tconst", "titleType", "primaryTitle", "startYear"],
        )
        crew = self.spark.createDataFrame(
            [
                ("tt1", "nm1,nm2"), ("tt2", "nm1,nm2"),
                ("tt3", "nm1,nm2"), ("tt4", "nm2"),
            ("tt5", "nm1"), ("tt6", "nm1"),
            ],
            ["tconst", "directors"],
        )
        ratings = self.spark.createDataFrame(
            [
                ("tt1", "8.0", "2000"), ("tt2", "5.0", "1000"),
                ("tt3", "6.0", "1000"), ("tt4", "8.5", "2000"),
                ("tt5", "9.0", "2000"), ("tt6", "9.5", "2"),
            ],
            ["tconst", "averageRating", "numVotes"],
        )
        names = self.spark.createDataFrame(
            [("nm1", "Director One"), ("nm2", "Director Two")],
            ["nconst", "primaryName"],
        )
        rows = directors_with_one_high_film(
            films, crew, ratings, names
        ).collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].nconst, "nm1")
        self.assertEqual(rows[0].high_movie_id, "tt1")
        self.assertEqual(rows[0].high_movie_votes, 2000)
        self.assertEqual(rows[0].other_movie_count, 2)
        self.assertEqual(rows[0].other_average_rating, 5.5)

    def test_series_needs_long_seasons_and_clear_extremes(self) -> None:
        titles = []
        episodes = []
        ratings = []
        for series_id in ("s1", "s2", "s3"):
            titles.append((series_id, "tvSeries", f"Series {series_id}"))
            for season in range(1, 4):
                episode_total = 9 if series_id == "s3" and season == 3 else 10
                for number in range(1, episode_total + 1):
                    episode_id = f"{series_id}_{season}_{number}"
                    titles.append((episode_id, "tvEpisode", episode_id))
                    episodes.append((episode_id, series_id, str(season)))
                    rating = "7.0"
                    votes = "100"
                    if season == 1 and number == 1:
                        rating = "9.0"
                    if season == 1 and number == 2:
                        rating, votes = "1.1", "2"
                    if season == 3 and number == 10:
                        rating = "2.0"
                    if series_id == "s2" and season == 3 and number == 2:
                        rating = "9.0"  # A late best excludes s2.
                    ratings.append((episode_id, rating, votes))
        title_df = self.spark.createDataFrame(
            titles, ["tconst", "titleType", "primaryTitle"]
        )
        episode_df = self.spark.createDataFrame(
            episodes, ["tconst", "parentTconst", "seasonNumber"]
        )
        rating_df = self.spark.createDataFrame(
            ratings, ["tconst", "averageRating", "numVotes"]
        )

        rows = series_with_early_best_and_late_worst(
            title_df, episode_df, rating_df
        ).collect()

        self.assertEqual([row.parentTconst for row in rows], ["s1"])
        self.assertEqual(rows[0].best_rating, 9.0)
        self.assertEqual(rows[0].worst_rating, 2.0)
        self.assertEqual(rows[0].best_episode_votes, 100)
        self.assertEqual(rows[0].worst_episode_votes, 100)
        self.assertEqual(rows[0].last_season, 3)
        self.assertEqual(rows[0].rated_episode_count, 29)


if __name__ == "__main__":
    unittest.main()
