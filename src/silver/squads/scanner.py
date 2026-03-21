"""
File scanner for squad JSON files.

Discovers all squad snapshot files under data/raw/ regardless of competition
code or season, and returns them sorted chronologically by snapshot date so
the diff strategy is always applied in the correct order.

Expected path structure:
    data/raw/{competition_code}/{season}/squads/{snapshot_date}/{source_team_id}.json
"""

import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)


def _parse_snapshot_date(date_str: str) -> date | None:
    """Parse an ISO date string folder name (YYYY-MM-DD) into a date object."""
    try:
        return date.fromisoformat(date_str)
    except ValueError:
        logger.warning("Skipping folder with unparseable date: '%s'", date_str)
        return None


def scan_squad_files(raw_root: str | Path) -> list[Path]:
    """
    Recursively discover all squad JSON files under raw_root.

    Returns a list of Path objects sorted by snapshot_date ascending
    (chronological order) so initial loads are always processed before
    subsequent diffs. Within the same snapshot date, files are sorted
    alphabetically by path for determinism.

    Args:
        raw_root: Root of the raw data directory (e.g. Path("data/raw")).

    Returns:
        Sorted list of .json file Paths matching the expected structure.

    Raises:
        FileNotFoundError: If raw_root does not exist.
    """
    raw_root = Path(raw_root)
    if not raw_root.exists():
        raise FileNotFoundError(f"Raw data root not found: {raw_root}")

    # Match: {competition_code}/{season}/squads/{snapshot_date}/{source_team_id}.json
    all_files = list(raw_root.glob("*/*/squads/*/*.json"))

    if not all_files:
        logger.warning("No squad JSON files found under %s", raw_root)
        return []

    def sort_key(path: Path) -> tuple:
        """
        Sort by snapshot_date folder (index -2) then full path for determinism.
        Files with unparseable dates are pushed to the end.
        """
        snapshot_date = _parse_snapshot_date(path.parts[-2])
        # Use a far-future date as fallback so malformed folders don't crash the sort
        fallback = date(9999, 12, 31)
        return (snapshot_date or fallback, str(path))

    sorted_files = sorted(all_files, key=sort_key)

    logger.info(
        "Found %d squad file(s) under %s across %d snapshot date(s)",
        len(sorted_files),
        raw_root,
        len({p.parts[-2] for p in sorted_files}),
    )
    return sorted_files


def extract_path_context(file_path: Path) -> dict[str, str | date | None]:
    """
    Extract context from the file path structure without reading the file.

    Returns a dict with keys:
        competition_code, season, snapshot_date, source_team_id

    Returns None values for fields that cannot be parsed, so the validator
    can report them properly.
    """
    parts = file_path.parts
    # Expected: .../{competition_code}/{season}/squads/{snapshot_date}/{file}.json
    # Minimum depth check: need at least 5 parts above the root
    try:
        source_team_id   = file_path.stem                  # filename without .json
        snapshot_date_str = parts[-2]                      # e.g. "2025-08-15"
        # parts[-3] should be "squads"
        season           = parts[-4]                       # e.g. "2025-26"
        competition_code = parts[-5]                       # e.g. "PRD"
    except IndexError:
        return {
            "competition_code": None,
            "season":           None,
            "snapshot_date":    None,
            "source_team_id":   None,
        }

    return {
        "competition_code": competition_code,
        "season":           season,
        "snapshot_date":    _parse_snapshot_date(snapshot_date_str),
        "source_team_id":   source_team_id,
    }