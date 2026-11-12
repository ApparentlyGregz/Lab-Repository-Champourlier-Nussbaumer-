"""Behavior checks for the IMDb Spark loader."""

import gzip
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from src.imdb_loader import TABLE_COLUMNS, load_imdb_tables


class ImdbLoaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[2]").appName(
            "imdb-loader-tests"
        ).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.raw_dir = Path(self.temp_dir.name)
        for name, columns in TABLE_COLUMNS.items():
            values = ["example"] * len(columns)
            if name == "name.basics":
                values[columns.index("nconst")] = "nm0000001"
                values[columns.index("deathYear")] = r"\N"
            if name == "title.akas":
                values[columns.index("title")] = '"Rolling in the Deep Dish'
            if name == "title.ratings":
                values[columns.index("averageRating")] = "8.5"
            self.write_archive(name, columns, values)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def write_archive(self, name, columns, values) -> None:
        path = self.raw_dir / f"{name}.tsv.gz"
        with gzip.open(path, "wt", encoding="utf-8") as archive:
            archive.write("\t".join(columns) + "\n")
            archive.write("\t".join(values) + "\n")

    def test_reads_all_archives_and_converts_null_marker(self) -> None:
        tables = load_imdb_tables(self.spark, self.raw_dir)

        self.assertEqual(set(tables), set(TABLE_COLUMNS))
        self.assertEqual(tables["name.basics"].first().nconst, "nm0000001")
        self.assertIsNone(tables["name.basics"].first().deathYear)
        self.assertEqual(
            tables["title.akas"].first().title, '"Rolling in the Deep Dish'
        )
        self.assertEqual(tables["title.ratings"].first().averageRating, "8.5")

    def test_missing_archive_is_reported(self) -> None:
        (self.raw_dir / "title.ratings.tsv.gz").unlink()

        with self.assertRaisesRegex(FileNotFoundError, "title.ratings.tsv.gz"):
            load_imdb_tables(self.spark, self.raw_dir)

    def test_wrong_header_is_rejected(self) -> None:
        columns = ("wrong_id", "averageRating", "numVotes")
        self.write_archive("title.ratings", columns, ("tt1", "8.5", "100"))

        tables = load_imdb_tables(self.spark, self.raw_dir)
        with self.assertRaisesRegex(Exception, "CSV header does not conform"):
            tables["title.ratings"].first()


if __name__ == "__main__":
    unittest.main()
