#!/usr/bin/env python3
"""Quick utility to probe PatentsView availability for a given date window."""
import argparse
import json
import logging
import sys
from datetime import datetime, timedelta

import pandas as pd
import requests

DEFAULT_FIELDS = [
    "patent_id",
    "patent_title",
    "patent_date",
    "inventors.inventor_name_first",
    "inventors.inventor_name_last",
    "assignees.assignee_organization"
]

API_URL = "https://search.patentsview.org/api/v1/patent"
MAX_DATA_DATE = datetime(2024, 12, 31).date()


def _default_window(days_back: int = 7) -> tuple[str, str]:
    today = datetime.utcnow().date()
    capped_end = today if today <= MAX_DATA_DATE else MAX_DATA_DATE
    capped_start = capped_end - timedelta(days=days_back)
    return capped_start.strftime('%Y-%m-%d'), capped_end.strftime('%Y-%m-%d')


def parse_args() -> argparse.Namespace:
    default_start, default_end = _default_window()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "start",
        nargs="?",
        default=default_start,
        help=f"Start date (YYYY-MM-DD). Defaults to {default_start} (7 days back)."
    )
    parser.add_argument(
        "end",
        nargs="?",
        default=default_end,
        help=f"End date (YYYY-MM-DD). Defaults to {default_end} (today capped at {MAX_DATA_DATE})."
    )
    parser.add_argument("--api-key", dest="api_key", default="oq371zFI.BjeAbayJsdHdvEgbei0vskz5bTK3KM1S", help="PatentsView API key")
    parser.add_argument("--max-results", type=int, default=10000, help="Maximum patents to fetch (default 10000)")
    parser.add_argument("--per-page", type=int, default=1000, help="Page size for requests (max 1000)")
    parser.add_argument("--fields", nargs="*", default=DEFAULT_FIELDS, help="Optional override for fields list")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default INFO)"
    )
    return parser.parse_args()


def validate_dates(start: str, end: str) -> None:
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid date supplied: {exc}")
    if start_dt > end_dt:
        raise SystemExit("Start date must not be after end date")


def fetch_patents(args: argparse.Namespace) -> list[dict]:
    headers = {"X-Api-Key": args.api_key, "Accept": "application/json"}
    fields = json.dumps(args.fields)
    query = json.dumps({
        "_and": [
            {"_gte": {"patent_date": args.start}},
            {"_lte": {"patent_date": args.end}}
        ]
    })

    patents: list[dict] = []
    seen_ids: set[str] = set()
    page_size = max(1, min(args.per_page, 1000))
    cursor = None
    prev_cursor = None
    page = 0
    total_hits = None

    sort_spec = [{"patent_id": "asc"}]

    while len(patents) < args.max_results:
        options = {"size": max(1, min(page_size, args.max_results - len(patents)))}
        if cursor is not None:
            options["after"] = cursor

        params = {
            "q": query,
            "f": fields,
            "o": json.dumps(options),
            "s": json.dumps(sort_spec)
        }

        logging.debug("Request params: %s", params)
        response = requests.get(API_URL, headers=headers, params=params, timeout=30)
        if response.status_code != 200:
            raise SystemExit(f"HTTP {response.status_code}: {response.text}")

        payload = response.json()
        page += 1

        if page == 1:
            total_hits = payload.get("total_hits") or payload.get("data", {}).get("total_hits")
            logging.info(
                "Response page %s: keys=%s total_hits=%s page_size=%s",
                page,
                list(payload.keys()),
                total_hits,
                options['size']
            )

        batch = payload.get("data", {}).get("patents", []) or payload.get("patents", [])

        if page == 1:
            logging.info(
                "Returned %s patents on first page. Example keys: %s",
                len(batch),
                list(batch[0].keys()) if batch else []
            )

        if not batch:
            logging.debug("No more patents returned (page %s)", page)
            break

        new_patents = 0
        duplicates = 0
        for patent in batch:
            patent_id = patent.get('patent_id')
            if patent_id and patent_id not in seen_ids:
                patents.append(patent)
                seen_ids.add(patent_id)
                new_patents += 1
            else:
                duplicates += 1

        logging.info(
            "Page %s: received=%s new=%s duplicates=%s total_collected=%s",
            page,
            len(batch),
            new_patents,
            duplicates,
            len(patents)
        )

        if len(patents) >= args.max_results:
            logging.info("Reached max_results limit of %s", args.max_results)
            break

        if total_hits is not None and len(patents) >= total_hits:
            logging.info("Collected all %s available patents", total_hits)
            break

        cursor = batch[-1].get('patent_id')
        if not cursor or cursor == prev_cursor:
            logging.debug("No cursor for next page, stopping pagination")
            break
        prev_cursor = cursor

    logging.info(
        "Pagination complete: collected %s unique patents across %s pages (max_results=%s, total_hits=%s)",
        len(patents),
        page,
        args.max_results,
        total_hits
    )

    return patents[: args.max_results]


def summarize(patents: list[dict]) -> None:
    print(f"Patents fetched: {len(patents)}")
    if not patents:
        print("No data returned for requested window.")
        return
    df = pd.json_normalize(patents)
    print("Columns:", ", ".join(df.columns))
    print(df.head(5))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(levelname)s: %(message)s")
    logging.info(
        "Testing PatentsView range %s to %s | max_results=%s per_page=%s",
        args.start,
        args.end,
        args.max_results,
        args.per_page
    )
    logging.info("Requested fields: %s", args.fields)
    validate_dates(args.start, args.end)
    try:
        patents = fetch_patents(args)
    except requests.exceptions.RequestException as exc:
        raise SystemExit(f"Request error: {exc}")
    summarize(patents)


if __name__ == "__main__":
    sys.exit(main() or 0)
