#!/usr/bin/env python3
"""Verify filtered-out people by cross-checking SQL matches.

This script randomly samples people that were filtered out as existing matches
(e.g., from `output/existing_people_found.json`), re-runs the simplified scoring
logic used during Step 1 against the SQL database, and reports whether the
expected matches are still present. The goal is to spot-check a few hundred
records to confirm the de-duplication logic is working as intended.
"""
import argparse
import json
import logging
import random
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from decimal import Decimal

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

# Ensure project root on path so we can import local modules
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.db_manager import DatabaseConfig, DatabaseManager, ExistingDataDAO  # noqa: E402

# Thresholds must match Step 1 logic
AUTO_MATCH_THRESHOLD = 25

logger = logging.getLogger(__name__)


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_people(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Expected JSON array of people records")
    return data


def deduplicate_people(people: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    seen_keys = {}
    duplicate_examples: List[Dict[str, Any]] = []
    duplicates_count = 0
    unique_people: List[Dict[str, Any]] = []

    for person in people:
        key = (
            (person.get("first_name") or "").strip().lower(),
            (person.get("last_name") or "").strip().lower(),
            (person.get("city") or "").strip().lower(),
            (person.get("state") or "").strip().lower(),
            (person.get("person_type") or "").strip().lower(),
            (person.get("patent_number") or "").strip()
        )
        if key in seen_keys:
            duplicates_count += 1
            if len(duplicate_examples) < 25:
                duplicate_examples.append(person)
            continue
        seen_keys[key] = True
        unique_people.append(person)

    duplicates_info = {"count": duplicates_count, "examples": duplicate_examples}
    return unique_people, duplicates_info


def clean_string(value: Optional[str]) -> str:
    if not value or str(value).lower() in {"nan", "none", "null", ""}:
        return ""
    return str(value).strip()


def clean_first_name(name: Optional[str]) -> str:
    cleaned = clean_string(name).lower()
    parts = cleaned.split()
    return parts[0] if parts else ""


def clean_last_name(lastname: Optional[str]) -> str:
    cleaned = clean_string(lastname).lower()
    suffixes = [
        ", jr.", ", jr", " jr.", " jr",
        ", sr.", ", sr", " sr.", " sr",
        ", ii", ", iii", ", iv", ", v",
        " ii", " iii", " iv", " v"
    ]
    for suffix in suffixes:
        if cleaned.endswith(suffix):
            return cleaned[: -len(suffix)].strip()
    return cleaned


def calculate_match_score(
    target_first: str,
    target_last: str,
    target_city: str,
    target_state: str,
    existing_first: str,
    existing_last: str,
    existing_city: str,
    existing_state: str
) -> int:
    t_first = clean_first_name(target_first)
    t_last = clean_last_name(target_last)
    t_city = clean_string(target_city).lower()
    t_state = clean_string(target_state).lower()

    e_first = clean_first_name(existing_first)
    e_last = clean_last_name(existing_last)
    e_city = clean_string(existing_city).lower()
    e_state = clean_string(existing_state).lower()

    if not t_last or not e_last or t_last != e_last:
        return 0

    score = 10  # last-name match
    if t_first and e_first:
        if t_first == e_first:
            score += 40
        elif t_first[0] == e_first[0]:
            score += 10
    if t_state and e_state and t_state == e_state:
        score += 20
        if t_city and e_city and t_city == e_city:
            score += 20
    return score


def normalize_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    normalized: Dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date)):
            normalized[key] = value.isoformat()
        elif isinstance(value, Decimal):
            normalized[key] = float(value)
        else:
            normalized[key] = value
    return normalized


def sample_people(people: List[Dict[str, Any]], sample_size: int, seed: Optional[int] = None) -> List[Dict[str, Any]]:
    if seed is not None:
        random.seed(seed)
    if sample_size >= len(people):
        return list(people)
    return random.sample(people, sample_size)


def build_lookup_by_lastname(results: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in results:
        last_name = clean_last_name(row.get("last_name"))
        if not last_name:
            continue
        grouped.setdefault(last_name, []).append(row)
    return grouped


def verify_matches(
    dao: ExistingDataDAO,
    sampled_people: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Return verification records and summary statistics."""
    if not sampled_people:
        return [], {"sampled": 0, "confirmed": 0, "below_threshold": 0}

    db_rows = dao.find_people_by_batch_selective(sampled_people)
    grouped = build_lookup_by_lastname(db_rows)

    results: List[Dict[str, Any]] = []
    confirmed = 0
    below_threshold = 0

    for person in sampled_people:
        t_first = person.get("first_name", "")
        t_last = person.get("last_name", "")
        t_city = person.get("city", "")
        t_state = person.get("state", "")
        candidate_rows = grouped.get(clean_last_name(t_last), [])

        best_score = 0
        best_match: Optional[Dict[str, Any]] = None

        for db_person in candidate_rows:
            score = calculate_match_score(
                t_first,
                t_last,
                t_city,
                t_state,
                db_person.get("first_name", ""),
                db_person.get("last_name", ""),
                db_person.get("city", ""),
                db_person.get("state", "")
            )
            if score > best_score:
                best_score = score
                best_match = db_person

        match_confirmed = best_score >= AUTO_MATCH_THRESHOLD
        if match_confirmed:
            confirmed += 1
        else:
            below_threshold += 1

        results.append({
            "person": person,
            "best_match": normalize_row(best_match),
            "best_score": best_score,
            "match_confirmed": match_confirmed,
            "source_match_score": person.get("match_score"),
        })

    summary = {
        "sampled": len(sampled_people),
        "confirmed": confirmed,
        "below_threshold": below_threshold,
        "confirmation_rate": (confirmed / len(sampled_people)) if sampled_people else 0.0,
    }
    return results, summary


def save_results(output_path: Path, results: List[Dict[str, Any]], summary: Dict[str, Any], duplicates: Dict[str, Any]) -> None:
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "summary": summary,
        "duplicates_found": duplicates,
        "results": results,
    }
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify filtered people against SQL data")
    parser.add_argument(
        "--input",
        default="output/existing_people_found.json",
        help="Path to JSON file containing filtered-out people"
    )
    parser.add_argument(
        "--output",
        default="output/verify_filtered_people_results.json",
        help="Where to write verification report (JSON)"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=200,
        help="Number of random people to verify"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible samples"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if load_dotenv:
        load_dotenv()
    configure_logging(args.verbose)

    input_path = (PROJECT_ROOT / args.input).resolve()
    output_path = (PROJECT_ROOT / args.output).resolve()

    logger.info("Loading people from %s", input_path)
    people = load_people(input_path)
    logger.info("Loaded %d filtered people", len(people))

    unique_people, duplicates = deduplicate_people(people)
    if duplicates["count"]:
        logger.warning("Detected %d duplicate records in source JSON", duplicates["count"])
    else:
        logger.info("No duplicates detected in source JSON")

    sample = sample_people(unique_people, args.sample_size, seed=args.seed)
    logger.info("Sampling %d people for verification", len(sample))

    db_config = DatabaseConfig.from_env()
    db_manager = DatabaseManager(db_config)
    if not db_manager.test_connection():
        logger.error("Database connection failed; aborting verification")
        return 1

    dao = ExistingDataDAO(db_manager)
    results, summary = verify_matches(dao, sample)

    logger.info(
        "Verification complete — %d/%d confirmed (%.1f%%)",
        summary["confirmed"],
        summary["sampled"],
        summary["confirmation_rate"] * 100
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    save_results(output_path, results, summary, duplicates)
    logger.info("Wrote verification report to %s", output_path)

    if summary["below_threshold"]:
        logger.warning("%d records scored below the auto-match threshold", summary["below_threshold"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
