"""
Dataclasses representing the parsed in-memory structures for squad ingestion.

These are intermediate representations between the raw JSON and the DB — they
carry only the fields that are actually written to the silver layer tables.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class PlayerRecord:
    """
    Maps to the `players` table.
    One instance per unique player across all squads.
    Coaching staff (type != "player") are never instantiated.
    """
    source_player_id:             str
    first_name:                   str
    last_name:                    str
    short_first_name:             Optional[str]
    short_last_name:              Optional[str]
    known_name:                   Optional[str]
    match_name:                   Optional[str]
    gender:                       Optional[str]
    place_of_birth:               Optional[str]
    nationality:                  Optional[str]
    nationality_source_id:        Optional[str]
    second_nationality:           Optional[str]
    second_nationality_source_id: Optional[str]
    position_raw:                 Optional[str]

    # Not in provider feed — always None on ingestion, enriched manually later
    date_of_birth:                Optional[date]  = None
    preferred_position:           Optional[str]   = None
    preferred_foot:               Optional[str]   = None
    height_cm:                    Optional[int]   = None


@dataclass
class SquadEntry:
    """
    Maps to one row in the `player_squads` table.
    One instance per player spell within a team-season.
    """
    source_player_id:      str
    source_team_id:        str
    source_season_id:      str   # tournamentCalendarId — resolved to competition_season_id in DB
    start_date:            date
    end_date:              Optional[date]
    shirt_number:          Optional[int]
    active:                bool  # derived from person.active == "yes"

    # Not in provider feed — defaulted at DB level, enriched manually later
    squad_role:            str   = "first_team"
    transfer_type:         Optional[str] = None


@dataclass
class SquadSnapshot:
    """
    The full parsed contents of a single squad JSON file.
    One instance per file on disk.
    """
    # Context derived from file path
    competition_code: str
    season:           str
    snapshot_date:    date
    source_team_id:   str          # from filename stem — cross-checked against JSON

    # Context from JSON body
    source_season_id:    str       # tournamentCalendarId
    source_competition_id: str     # competitionId
    competition_name:    str
    team_name:           str
    team_short_name:     str
    team_code:           str
    venue_name:          Optional[str]
    venue_id:            Optional[str]

    # Parsed records
    players:       list[PlayerRecord] = field(default_factory=list)
    squad_entries: list[SquadEntry]   = field(default_factory=list)