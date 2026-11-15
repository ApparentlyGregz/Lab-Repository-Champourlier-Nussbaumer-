"""Collect human-written Wikipedia text for selected IMDb IDs.

Wikidata's IMDb identifier provides the link to the exact Wikipedia page.
Raw API responses stay in data/external; Spark performs the text cleaning.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlsplit
from urllib.request import Request, urlopen


SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
USER_AGENT = (
    "ECE-BDP-IMDb-Project/1.0 "
    "(https://github.com/ApparentlyGregz/"
    "Lab-Repository-Champourlier-Nussbaumer-)"
)
LICENSE_URI = "https://creativecommons.org/licenses/by-sa/4.0/"


class WikimediaClient:
    """Make sequential requests with a delay and bounded retry/backoff."""

    def __init__(self, delay: float):
        self.delay = delay
        self.last_request = 0.0

    def get_json(self, endpoint: str, params: dict) -> dict:
        url = endpoint + "?" + urlencode(params)
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }
        for attempt in range(5):
            wait = self.delay - (time.monotonic() - self.last_request)
            if wait > 0:
                time.sleep(wait)
            self.last_request = time.monotonic()
            try:
                request = Request(url, headers=headers)
                with urlopen(request, timeout=60) as data:
                    payload = json.load(data)
                if payload.get("error", {}).get("code") == "maxlag":
                    time.sleep(5 * (attempt + 1))
                    continue
                if "error" in payload:
                    raise RuntimeError(payload["error"])
                return payload
            except HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504):
                    raise
                print(f"HTTP {error.code}; retrying", flush=True)
                retry_after = error.headers.get("Retry-After", "")
                pause = (
                    int(retry_after) if retry_after.isdigit()
                    else 5 * 2**attempt
                )
                time.sleep(min(pause, 120))
            except (URLError, TimeoutError, json.JSONDecodeError) as error:
                print(f"Request error {error}; retrying", flush=True)
                time.sleep(min(5 * 2**attempt, 120))
        raise RuntimeError(
            f"Wikimedia request failed after retries: {endpoint}"
        )


def selected_ids(results_dir: Path) -> list[tuple[str, str]]:
    ids = set()
    for filename, field, kind in (
        ("enrichment_films.csv", "tconst", "film"),
        ("enrichment_principals.csv", "nconst", "person"),
    ):
        with (results_dir / filename).open(
            encoding="utf-8", newline=""
        ) as file:
            ids.update((row[field], kind) for row in csv.DictReader(file))
    return sorted(ids, key=lambda item: (item[1], item[0]))


def completed_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as file:
        return {json.loads(line)["imdb_id"] for line in file if line.strip()}


def append_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")
        file.flush()


def chunks(items: list, size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def map_batch(
    client: WikimediaClient, ids: list[tuple[str, str]]
) -> list[dict]:
    values = " ".join(f'"{imdb_id}"' for imdb_id, _ in ids)
    query = (
        "SELECT ?imdb ?item ?article ?language WHERE { "
        f"VALUES ?imdb {{ {values} }} "
        "?item <http://www.wikidata.org/prop/direct/P345> ?imdb . "
        "OPTIONAL { ?article <http://schema.org/about> ?item ; "
        "<http://schema.org/isPartOf> <https://en.wikipedia.org/> . } "
        "OPTIONAL { ?item <http://www.wikidata.org/prop/direct/P364> "
        "?language . } }"
    )
    response = client.get_json(SPARQL_URL, {"query": query, "format": "json"})
    matches = defaultdict(lambda: defaultdict(lambda: {
        "articles": set(), "languages": set()
    }))
    for binding in response["results"]["bindings"]:
        imdb_id = binding["imdb"]["value"]
        item_uri = binding["item"]["value"]
        data = matches[imdb_id][item_uri]
        if "article" in binding:
            data["articles"].add(binding["article"]["value"])
        if "language" in binding:
            data["languages"].add(binding["language"]["value"])

    rows = []
    for imdb_id, kind in ids:
        entities = matches[imdb_id]
        if not entities:
            status = "no_wikidata_match"
        elif len(entities) != 1:
            status = "ambiguous_wikidata_match"
        elif len(next(iter(entities.values()))["articles"]) != 1:
            status = "no_unique_english_article"
        else:
            status = "matched"
        item_uri, data = next(iter(entities.items()), (None, None))
        rows.append({
            "imdb_id": imdb_id,
            "entity_type": kind,
            "status": status,
            "wikidata_uri": item_uri if len(entities) == 1 else None,
            "wikipedia_url": (
                next(iter(data["articles"]))
                if status == "matched" else None
            ),
            "original_language_uris": (
                sorted(data["languages"]) if kind == "film" and data
                else []
            ),
        })
    return rows


def run_map(
    client: WikimediaClient, results_dir: Path, output: Path,
    max_items: int | None,
) -> None:
    done = completed_ids(output)
    remaining = [item for item in selected_ids(results_dir)
                 if item[0] not in done]
    if max_items is not None:
        remaining = remaining[:max_items]
    total = len(remaining)
    for number, batch in enumerate(chunks(remaining, 30), 1):
        append_rows(output, map_batch(client, batch))
        if number % 10 == 0 or number * 30 >= total:
            print(f"Mapped {min(number * 30, total)}/{total} IDs", flush=True)


def article_title(url: str) -> str:
    return unquote(urlsplit(url).path.removeprefix("/wiki/")).replace("_", " ")


def wikipedia_pages(
    client: WikimediaClient, titles: list[str], intro_only: bool
) -> dict:
    params = {
        "action": "query", "format": "json", "formatversion": 2,
        "prop": "extracts|revisions", "titles": "|".join(titles),
        "explaintext": 1, "exsectionformat": "wiki",
        "rvprop": "ids", "redirects": 1, "maxlag": 5,
    }
    if intro_only:
        params.update({"exintro": 1, "exlimit": 20})
    response = client.get_json(WIKIPEDIA_API, params)["query"]
    redirects = {}
    changes = response.get("normalized", []) + response.get("redirects", [])
    for change in changes:
        redirects[change["from"].casefold()] = change["to"]
    pages = {page["title"].casefold(): page for page in response["pages"]}
    result = {}
    for title in titles:
        final_title = title
        for _ in range(3):
            next_title = redirects.get(final_title.casefold())
            if not next_title:
                break
            final_title = next_title
        result[title] = pages.get(final_title.casefold())
    return result


def raw_record(mapping: dict, page: dict | None) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    if mapping["status"] != "matched":
        status = mapping["status"]
    elif page is None or "missing" in page:
        status = "article_missing"
    elif not page.get("extract"):
        status = "article_empty"
    else:
        status = "fetched"
    revision = (page or {}).get("revisions") or []
    revision_id = revision[0].get("revid") if revision else None
    article_url = mapping.get("wikipedia_url")
    return {
        "imdb_id": mapping["imdb_id"],
        "entity_type": mapping["entity_type"],
        "status": status,
        "note": None if status == "fetched" else status,
        "wikidata_uri": mapping.get("wikidata_uri"),
        "article_url": article_url,
        "source_uri": (
            f"https://en.wikipedia.org/w/index.php?oldid={revision_id}"
            if revision_id else article_url
        ),
        "revision_id": revision_id,
        "license_uri": LICENSE_URI if article_url else None,
        "contributors_uri": (
            article_url.replace("/wiki/", "/w/index.php?title=")
            + "&action=history" if article_url else None
        ),
        "retrieved_at": now,
        "extract": page.get("extract") if status == "fetched" else None,
        "original_language_uris": mapping.get("original_language_uris", []),
    }


def run_fetch(
    client: WikimediaClient, mapping_path: Path, output: Path,
    kind: str, max_items: int | None,
) -> None:
    done = completed_ids(output)
    with mapping_path.open(encoding="utf-8") as file:
        remaining = [json.loads(line) for line in file if line.strip()]
    remaining = [
        item for item in remaining
        if item["entity_type"] == kind and item["imdb_id"] not in done
    ]
    if max_items is not None:
        remaining = remaining[:max_items]
    batch_size = 1 if kind == "film" else 20
    total = len(remaining)
    for number, batch in enumerate(chunks(remaining, batch_size), 1):
        matched = [item for item in batch if item["status"] == "matched"]
        titles = [article_title(item["wikipedia_url"]) for item in matched]
        pages = (
            wikipedia_pages(client, titles, kind == "person")
            if titles else {}
        )
        rows = [
            raw_record(item, pages.get(article_title(item["wikipedia_url"])))
            if item["status"] == "matched" else raw_record(item, None)
            for item in batch
        ]
        append_rows(output, rows)
        if number % 20 == 0 or number * batch_size >= total:
            count = min(number * batch_size, total)
            print(f"Fetched {kind} {count}/{total}", flush=True)


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage", choices=("map", "films", "people"), required=True
    )
    parser.add_argument(
        "--results-dir", type=Path, default=project_dir / "results"
    )
    parser.add_argument("--external-dir", type=Path,
                        default=project_dir / "data" / "external")
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--max-items", type=int)
    args = parser.parse_args()
    if args.delay < 0.2 or (args.max_items is not None and args.max_items < 1):
        parser.error("--delay must be >= 0.2 and --max-items >= 1")
    client = WikimediaClient(args.delay)
    mapping_path = args.external_dir / "wikidata_map.jsonl"
    if args.stage == "map":
        run_map(client, args.results_dir, mapping_path, args.max_items)
    else:
        output = args.external_dir / f"wikipedia_raw_{args.stage}.jsonl"
        run_fetch(client, mapping_path, output,
                  "film" if args.stage == "films" else "person",
                  args.max_items)


if __name__ == "__main__":
    main()
