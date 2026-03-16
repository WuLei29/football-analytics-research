"""
Parser for squad JSON files.

Transforms a validated raw JSON dict into a SquadSnapshot dataclass.
Only person entries with type == "player" are parsed into PlayerRecord
and SquadEntry instances. Coaching staff are silently ignored here —
the validator has already confirmed the structure is sound.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

from .models import PlayerRecord, SquadEntry, SquadSnapshot

logger = logging.getLogger(__name__)

_PLAYER_TYPE = "player"


def _parse_date(value: Optional[str]) -> Optional[date]:
    """
    Parse a provider date string into a date object.

    The provider uses the format "YYYY-MM-DDZ" (e.g. "2023-07-01Z").
    Standard isoformat() does not handle the trailing Z, so we strip it.
    Returns None for missing or unparseable values without raising.
    """
    if not value:
        return None
    cleaned = value.rstrip("Z").strip()
    try:
        return date.fromisoformat(cleaned)
    except ValueError:
        logger.warning("Could not parse date value: '%s'", value)
        return None


def _parse_shirt_number(value) -> Optional[int]:
    """Safely coerce shirt number to int — provider occasionally sends strings."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        logger.warning("Could not parse shirtNumber: '%s'", value)
        return None


def _parse_player(person: dict) -> Optional[PlayerRecord]:
    """
    Parse a single person dict into a PlayerRecord.

    Returns None if the entry is not a player (coach, assistant coach, etc.)
    or if mandatory fields are absent (should have been caught by validator,
    but we defend anyway).
    """
    if person.get("type", "").lower() != _PLAYER_TYPE:
        return None

    source_id = person.get("id")
    first_name = person.get("firstName")
    last_name = person.get("lastName")

    if not all([source_id, first_name, last_name]):
        logger.warning(
            "Skipping person missing id/firstName/lastName: %s", person.get("id", "<unknown>")
        )
        return None

    return PlayerRecord(
        source_player_id             = source_id,
        first_name                   = first_name,
        last_name                    = last_name,
        short_first_name             = person.get("shortFirstName"),
        short_last_name              = person.get("shortLastName"),
        known_name                   = person.get("knownName"),
        match_name                   = person.get("matchName"),
        gender                       = person.get("gender"),
        place_of_birth               = person.get("placeOfBirth"),
        nationality                  = person.get("nationality"),
        nationality_source_id        = person.get("nationalityId"),
        second_nationality           = person.get("secondNationality"),
        second_nationality_source_id = person.get("secondNationalityId"),
        position_raw                 = person.get("position"),
        # Fields not in provider feed — always None, enriched manually
        date_of_birth                = None,
        preferred_position           = None,
        preferred_foot               = None,
        height_cm                    = None,
    )


def _parse_squad_entry(
    person: dict,
    source_team_id: str,
    source_season_id: str,
    snapshot_date: date,
    is_initial_load: bool,
) -> Optional[SquadEntry]:
    """
    Parse a single person dict into a SquadEntry.

    start_date / end_date semantics depend on load type:
      - Initial load: use person.startDate / person.endDate from the JSON,
        preserving the historically accurate provider dates.
      - Diff load:    start_date = snapshot_date for new arrivals;
        end_date is handled by db.py for departures, not here.

    Returns None for non-player entries.
    """
    if person.get("type", "").lower() != _PLAYER_TYPE:
        return None

    source_player_id = person.get("id")
    if not source_player_id:
        return None

    if is_initial_load:
        start_date = _parse_date(person.get("startDate")) or snapshot_date
        end_date   = _parse_date(person.get("endDate"))
    else:
        # Diff load — new arrivals get snapshot_date as start_date.
        # This branch is only called for genuinely new players (not already in DB).
        start_date = snapshot_date
        end_date   = _parse_date(person.get("endDate"))  # honour provider end if already set

    return SquadEntry(
        source_player_id = source_player_id,
        source_team_id   = source_team_id,
        source_season_id = source_season_id,
        start_date       = start_date,
        end_date         = end_date,
        shirt_number     = _parse_shirt_number(person.get("shirtNumber")),
        active           = str(person.get("active", "no")).lower() == "yes",
    )


def parse_squad_file(
    raw_json: dict,
    competition_code: str,
    season: str,
    snapshot_date: date,
    source_team_id: str,
    is_initial_load: bool,
) -> SquadSnapshot:
    """
    Parse a validated squad JSON dict into a SquadSnapshot dataclass.

    Args:
        raw_json:          Validated JSON dict (output of json.load).
        competition_code:  Derived from file path (e.g. "PRD").
        season:            Derived from file path (e.g. "2025-26").
        snapshot_date:     Derived from file path folder name.
        source_team_id:    Derived from file stem — already cross-checked against JSON.
        is_initial_load:   True if no prior player_squads rows exist for this team-season.

    Returns:
        SquadSnapshot with all valid players and squad entries populated.
    """
    squad = raw_json["squad"][0]

    source_season_id     = squad["tournamentCalendarId"]
    source_competition_id = squad["competitionId"]

    players:       list[PlayerRecord] = []
    squad_entries: list[SquadEntry]   = []
    skipped = 0

    for person in squad.get("person", []):
        player = _parse_player(person)
        if player is None:
            skipped += 1
            continue

        entry = _parse_squad_entry(
            person          = person,
            source_team_id  = source_team_id,
            source_season_id = source_season_id,
            snapshot_date   = snapshot_date,
            is_initial_load = is_initial_load,
        )

        players.append(player)
        if entry:
            squad_entries.append(entry)

    logger.info(
        "Parsed %d player(s) from %s / %s [%s — %s] (skipped %d non-player entries)",
        len(players),
        squad.get("contestantName"),
        season,
        snapshot_date,
        "initial" if is_initial_load else "diff",
        skipped,
    )

    return SquadSnapshot(
        competition_code      = competition_code,
        season                = season,
        snapshot_date         = snapshot_date,
        source_team_id        = source_team_id,
        source_season_id      = source_season_id,
        source_competition_id = source_competition_id,
        competition_name      = squad.get("competitionName", ""),
        team_name             = squad.get("contestantName", ""),
        team_short_name       = squad.get("contestantShortName", ""),
        team_code             = squad.get("contestantCode", ""),
        venue_name            = squad.get("venueName"),
        venue_id              = squad.get("venueId"),
        players               = players,
        squad_entries         = squad_entries,
    )