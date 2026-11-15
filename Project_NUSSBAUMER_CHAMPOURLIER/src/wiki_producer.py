"""Forward English Wikipedia article changes from EventStreams to Kafka."""

from __future__ import annotations

import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

EVENTSTREAMS_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = (
    "ECE-BDP-IMDb-Project/1.0 "
    "(https://github.com/ApparentlyGregz/"
    "Lab-Repository-Champourlier-Nussbaumer-)"
)


def recent_changes(response):
    """Parse SSE messages, including multiline data and event IDs."""
    event_id = None
    data_lines = []
    for raw_line in response:
        line = raw_line.decode("utf-8").rstrip("\r\n")
        if not line:
            if data_lines:
                yield event_id, "\n".join(data_lines)
            event_id, data_lines = None, []
        elif line.startswith("id:"):
            event_id = line[3:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" "))


def select_change(event_id: str | None, payload: str) -> dict | None:
    """Keep article edits and creations from English Wikipedia."""
    event = json.loads(payload)
    if (event.get("wiki") != "enwiki" or event.get("namespace") != 0
            or event.get("type") not in {"edit", "new"}):
        return None
    meta = event.get("meta") or {}
    title = event.get("title")
    identifier = event_id or meta.get("id") or event.get("id")
    if not title or not identifier:
        return None
    return {
        "event_id": str(identifier),
        "wiki": event["wiki"],
        "title": title,
        "type": event["type"],
        "event_timestamp": event.get("timestamp"),
        "source_uri": (
            (event.get("server_url") or "https://en.wikipedia.org")
            + "/wiki/" + quote(title.replace(" ", "_"), safe="()")
        ),
    }


def main() -> None:
    from confluent_kafka import Producer

    broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
    topic = os.environ.get("KAFKA_TOPIC", "wikipedia-recentchange")
    producer = Producer({
        "bootstrap.servers": broker,
        "client.id": "wiki-imdb",
    })
    last_id = None
    sent = 0
    while True:
        headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            request = Request(EVENTSTREAMS_URL, headers=headers)
            with urlopen(request, timeout=90) as stream:
                for event_id, payload in recent_changes(stream):
                    try:
                        change = select_change(event_id, payload)
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
                    if change is None:
                        continue
                    body = json.dumps(
                        change, ensure_ascii=False
                    ).encode("utf-8")
                    errors = []

                    def delivered(error, _message):
                        if error:
                            errors.append(str(error))

                    while True:
                        try:
                            producer.produce(
                                topic, key=change["event_id"], value=body,
                                on_delivery=delivered,
                            )
                            break
                        except BufferError:
                            producer.poll(1)
                    if producer.flush(10) or errors:
                        raise TimeoutError(
                            f"Kafka did not confirm delivery: {errors}"
                        )
                    last_id = change["event_id"]
                    sent += 1
                    if sent % 100 == 0:
                        print(f"Published {sent} article changes", flush=True)
        except (HTTPError, URLError, OSError, TimeoutError) as exc:
            print(
                f"EventStreams disconnected: {exc}; reconnecting",
                flush=True,
            )
            producer.flush(10)
            time.sleep(5)


if __name__ == "__main__":
    main()
