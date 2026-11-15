"""Small checks for the EventStreams parser and IMDb matching rules."""

import io
import json
import unittest

from pyspark.sql import SparkSession, functions as F

from src.wiki_producer import recent_changes, select_change
from src.wiki_stream import EVENT_SCHEMA, match_events, parse_kafka_values


class WikiStreamTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("wiki-stream-tests")
            .config("spark.ui.enabled", "false").getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_sse_parser_and_article_filter(self):
        payload = {
            "wiki": "enwiki", "namespace": 0, "type": "edit",
            "title": "Alien (1979 film)", "timestamp": 123456,
            "server_url": "https://en.wikipedia.org",
        }
        stream = io.BytesIO(
            b":ok\n\nid: 42\n" +
            ("data: " + json.dumps(payload) + "\n\n").encode("utf-8")
        )
        parsed = list(recent_changes(stream))
        self.assertEqual(len(parsed), 1)
        change = select_change(*parsed[0])
        self.assertEqual(change["event_id"], "42")
        self.assertEqual(change["source_uri"],
                         "https://en.wikipedia.org/wiki/Alien_(1979_film)")
        payload["namespace"] = 1
        self.assertIsNone(select_change("43", json.dumps(payload)))

    def test_json_parse_and_title_match(self):
        wiki_events = [
            {"event_id": "a", "wiki": "enwiki", "title": "Dune (2021 film)",
             "type": "edit", "event_timestamp": 1760000000,
             "source_uri": "https://en.wikipedia.org/wiki/Dune_(2021_film)"},
            {"event_id": "b", "wiki": "enwiki", "title": "Unrelated page",
             "type": "edit", "event_timestamp": 1760000001,
             "source_uri": "https://en.wikipedia.org/wiki/Unrelated_page"},
            {"event_id": "c", "wiki": "enwiki", "title": "Dune (2026 film)",
             "type": "edit", "event_timestamp": 1760000002,
             "source_uri": "https://en.wikipedia.org/wiki/Dune_(2026_film)"},
            {"event_id": "d", "wiki": "enwiki",
             "title": "Fisk (TV series)", "type": "edit",
             "event_timestamp": 1760000003,
             "source_uri": "https://en.wikipedia.org/wiki/Fisk_(TV_series)"},
            {"event_id": "e", "wiki": "enwiki", "title": "Dune",
             "type": "edit", "event_timestamp": 1760000004,
             "source_uri": "https://en.wikipedia.org/wiki/Dune"},
        ]
        kafka = self.spark.createDataFrame(
            [(json.dumps(item).encode("utf-8"),) for item in wiki_events],
            ["value"],
        )
        events = parse_kafka_values(kafka)
        lookup = self.spark.createDataFrame(
            [
                ("tt1160419", "Dune", "movie", "2021", "900000", "dune"),
                ("tt13353168", "Fisk", "tvSeries", "2021", "1000", "fisk"),
            ],
            [
                "tconst", "imdb_title", "titleType", "startYear",
                "numVotes", "title_key",
            ],
        )
        matches = match_events(events, lookup).collect()
        self.assertEqual(len(matches), 2)
        by_id = {row.event_id: row for row in matches}
        self.assertEqual(by_id["a"].tconst, "tt1160419")
        self.assertEqual(by_id["a"].wiki_year, 2021)
        self.assertEqual(by_id["d"].tconst, "tt13353168")
        self.assertEqual(by_id["d"].wiki_medium, "tv series")


if __name__ == "__main__":
    unittest.main()
