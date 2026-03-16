"""
Validation of squad JSON files before parsing.

Two levels of validation:
  1. Squad level  — the outer wrapper has all required structural fields.
  2. Person level — each person entry has the minimum required fields.

Validation never raises — it returns a ValidationResult so the processor
can decide whether to skip, warn, or abort.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Fields required on the squad-level object (squad[0])
_REQUIRED_SQUAD_FIELDS = {
    "contestantId",
    "tournamentCalendarId",
    "competitionId",
    "competitionName",
    "contestantName",
    "contestantShortName",
    "contestantCode",
    "person",
}

# Fields required on every person entry (players and staff alike)
_REQUIRED_PERSON_FIELDS = {
    "id",
    "firstName",
    "lastName",
    "type",
}


@dataclass
class ValidationResult:
    is_valid:      bool
    errors:        list[str] = field(default_factory=list)
    warnings:      list[str] = field(default_factory=list)
    skipped_persons: list[str] = field(default_factory=list)  # person IDs skipped due to missing fields


def validate_squad_file(
    raw_json: dict,
    expected_source_team_id: str,
    file_path: Path,
) -> ValidationResult:
    """
    Validate a parsed squad JSON payload.

    Args:
        raw_json:                The full parsed JSON dict.
        expected_source_team_id: source_team_id derived from the filename stem.
        file_path:               Path to the file (for error messages only).

    Returns:
        ValidationResult — always returned, never raises.
    """
    result = ValidationResult(is_valid=True)
    label = str(file_path)

    # ------------------------------------------------------------------ #
    # Top-level structure
    # ------------------------------------------------------------------ #
    if "squad" not in raw_json:
        result.errors.append(f"[{label}] Missing top-level 'squad' key.")
        result.is_valid = False
        return result

    squad_list = raw_json["squad"]
    if not isinstance(squad_list, list) or len(squad_list) == 0:
        result.errors.append(f"[{label}] 'squad' is empty or not a list.")
        result.is_valid = False
        return result

    squad = squad_list[0]

    # ------------------------------------------------------------------ #
    # Required squad-level fields
    # ------------------------------------------------------------------ #
    missing_squad_fields = _REQUIRED_SQUAD_FIELDS - set(squad.keys())
    if missing_squad_fields:
        result.errors.append(
            f"[{label}] Missing required squad fields: {sorted(missing_squad_fields)}"
        )
        result.is_valid = False
        return result

    # ------------------------------------------------------------------ #
    # Cross-check: filename stem must match contestantId in the JSON
    # ------------------------------------------------------------------ #
    json_team_id = squad["contestantId"]
    if json_team_id != expected_source_team_id:
        result.errors.append(
            f"[{label}] Filename stem '{expected_source_team_id}' does not match "
            f"contestantId '{json_team_id}' in JSON. File may be misplaced."
        )
        result.is_valid = False
        return result

    # ------------------------------------------------------------------ #
    # Person-level validation
    # ------------------------------------------------------------------ #
    persons = squad.get("person", [])
    if not isinstance(persons, list):
        result.errors.append(f"[{label}] 'person' is not a list.")
        result.is_valid = False
        return result

    if len(persons) == 0:
        result.warnings.append(f"[{label}] Squad has no person entries.")

    for person in persons:
        missing_person_fields = _REQUIRED_PERSON_FIELDS - set(person.keys())
        if missing_person_fields:
            person_id = person.get("id", "<unknown>")
            result.warnings.append(
                f"[{label}] Person '{person_id}' missing fields "
                f"{sorted(missing_person_fields)} — will be skipped."
            )
            result.skipped_persons.append(person_id)

    # ------------------------------------------------------------------ #
    # Log summary
    # ------------------------------------------------------------------ #
    if result.errors:
        logger.error("Validation FAILED for %s: %s", label, result.errors)
    elif result.warnings:
        logger.warning("Validation passed with warnings for %s: %s", label, result.warnings)
    else:
        logger.debug("Validation passed for %s", label)

    return result