"""Collect Wikidata original languages for rated IMDb feature films."""

from __future__ import annotations

import argparse
import csv
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = (
    "ECE-BDP-IMDb-Project/1.0 "
    "(https://github.com/ApparentlyGregz/"
    "Lab-Repository-Champourlier-Nussbaumer-)"
)


def candidate_ids(path: Path) -> list[str]:
    with path.open(encoding="utf-8", newline="") as file:
        return sorted({row["tconst"] for row in csv.DictReader(file)})


def completed_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as file:
        return {json.loads(line)["imdb_id"] for line in file if line.strip()}


def get_json(
    endpoint: str, params: dict, delay: float, last_request: list[float]
) -> dict:
    url = endpoint + "?" + urlencode(params)
    for attempt in range(5):
        wait = delay - (time.monotonic() - last_request[0])
        if wait > 0:
            time.sleep(wait)
        last_request[0] = time.monotonic()
        request = Request(
            url, headers={"User-Agent": USER_AGENT,
                          "Accept": "application/json"},
        )
        try:
            with urlopen(request, timeout=60) as response:
                payload = json.load(response)
            if payload.get("error", {}).get("code") == "maxlag":
                time.sleep(5 * (attempt + 1))
                continue
            if "error" in payload:
                raise RuntimeError(payload["error"])
            return payload
        except HTTPError as error:
            if error.code not in (429, 500, 502, 503, 504):
                raise
            retry_after = error.headers.get("Retry-After", "")
            pause = (
                int(retry_after) if retry_after.isdigit()
                else 5 * 2**attempt
            )
            print(f"HTTP {error.code}; retrying", flush=True)
            time.sleep(min(pause, 120))
        except (URLError, TimeoutError, json.JSONDecodeError) as error:
            print(f"Request error {error}; retrying", flush=True)
            time.sleep(min(5 * 2**attempt, 120))
    raise RuntimeError("Wikidata request failed after retries")


def map_batch(ids: list[str], response: dict) -> list[dict]:
    matches = defaultdict(lambda: defaultdict(set))
    for binding in response["results"]["bindings"]:
        imdb_id = binding["imdb"]["value"]
        item_uri = binding["item"]["value"]
        languages = matches[imdb_id][item_uri]
        if "language" in binding:
            languages.add(binding["language"]["value"])
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for imdb_id in ids:
        entities = matches[imdb_id]
        if not entities:
            status = "no_wikidata_match"
        elif len(entities) != 1:
            status = "ambiguous_wikidata_match"
        elif not next(iter(entities.values())):
            status = "no_original_language"
        else:
            status = "matched"
        item_uri, languages = next(iter(entities.items()), (None, set()))
        rows.append({
            "imdb_id": imdb_id,
            "entity_type": "film",
            "status": status,
            "wikidata_uri": item_uri if len(entities) == 1 else None,
            "original_language_uris": (
                sorted(languages) if len(entities) == 1 else []
            ),
            "retrieved_at": now,
        })
    return rows


def collect_labels(
    mapping_path: Path, output: Path, delay: float
) -> None:
    with mapping_path.open(encoding="utf-8") as file:
        uris = {
            uri
            for line in file if line.strip()
            for uri in json.loads(line)["original_language_uris"]
        }
    done = set()
    if output.exists():
        with output.open(encoding="utf-8") as file:
            done = {
                json.loads(line)["language_uri"]
                for line in file if line.strip()
            }
    remaining = sorted(uris - done)
    output.parent.mkdir(parents=True, exist_ok=True)
    last_request = [0.0]
    for start in range(0, len(remaining), 50):
        batch = remaining[start:start + 50]
        qids = [uri.rsplit("/", 1)[-1] for uri in batch]
        response = get_json(
            WIKIDATA_API,
            {"action": "wbgetentities", "format": "json",
             "ids": "|".join(qids), "props": "labels",
             "languages": "en", "maxlag": 5},
            delay, last_request,
        )
        with output.open("a", encoding="utf-8") as file:
            for uri, qid in zip(batch, qids):
                entity = response["entities"].get(qid, {})
                label = entity.get("labels", {}).get("en", {}).get(
                    "value", qid
                )
                file.write(json.dumps({
                    "language_uri": uri, "language_name": label,
                }, ensure_ascii=False) + "\n")
            file.flush()
    print(f"Language labels: {len(remaining)} new", flush=True)


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidates", type=Path,
        default=project_dir / "output" / "language_candidates.csv",
    )
    parser.add_argument(
        "--output", type=Path,
        default=project_dir / "data" / "external"
        / "wikidata_original_languages.jsonl",
    )
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--max-items", type=int)
    parser.add_argument("--stage", choices=("map", "labels"),
                        default="map")
    parser.add_argument(
        "--labels-output", type=Path,
        default=project_dir / "data" / "external"
        / "wikidata_language_labels.jsonl",
    )
    args = parser.parse_args()
    if args.delay < 0.2 or (
        args.max_items is not None and args.max_items < 1
    ):
        parser.error("--delay must be >= 0.2 and --max-items >= 1")
    if args.stage == "labels":
        collect_labels(args.output, args.labels_output, args.delay)
        return
    done = completed_ids(args.output)
    remaining = [imdb_id for imdb_id in candidate_ids(args.candidates)
                 if imdb_id not in done]
    if args.max_items is not None:
        remaining = remaining[:args.max_items]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    last_request = [0.0]
    total = len(remaining)
    for start in range(0, total, 30):
        batch = remaining[start:start + 30]
        values = " ".join(f'"{imdb_id}"' for imdb_id in batch)
        query = (
            "SELECT ?imdb ?item ?language WHERE { "
            f"VALUES ?imdb {{ {values} }} "
            "?item <http://www.wikidata.org/prop/direct/P345> ?imdb . "
            "OPTIONAL { ?item "
            "<http://www.wikidata.org/prop/direct/P364> ?language . } }"
        )
        rows = map_batch(batch, get_json(
            SPARQL_URL, {"query": query, "format": "json"},
            args.delay, last_request,
        ))
        with args.output.open("a", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False) + "\n")
            file.flush()
        if (start // 30 + 1) % 10 == 0 or start + 30 >= total:
            print(f"Mapped {min(start + 30, total)}/{total} films",
                  flush=True)


if __name__ == "__main__":
    main()
