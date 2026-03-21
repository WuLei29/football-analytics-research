#!/usr/bin/env python3
"""
load_match_lineups.py
─────────────────────────────────────────────────────────────────────────────
Scans  data/raw/{competition_code}/{season_code}/matches/
and populates the match_lineups table via a 5-pass ingestion strategy:

  Pass 1   typeId 34  INSERT all 23×2 player rows per match
  Pass 2a  typeId 18  UPDATE minute_out  (player subbed off)
  Pass 2b  typeId 19  UPDATE minute_in   (substitute enters)
  Pass 2c  typeId 17  UPDATE minute_out  (straight red card)
  Pass 2d  typeId 17  UPDATE minute_out  (second yellow → red)

Idempotent: matches that already have lineup rows are silently skipped.
Isolated: only the matches/ sub-directory is touched; squads/ and any
          other directories are ignored.

Requirements:
  pip install psycopg2-binary python-dotenv
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]

BASE_DATA_PATH = PROJECT_ROOT / "data" / "raw"

# Provider qualifier IDs — typeId 34 (Team set up)
Q_PLAYER_IDS      = 30   # comma-separated source_player_ids
Q_POSITION_CODE   = 44   # comma-separated position codes (1=GK 2=DEF 3=MID 4=FWD 5=SUB)
Q_SHIRT_NUMBERS   = 59   # comma-separated shirt numbers
Q_TEAM_FORMATION  = 130  # raw formation ID (no mapping until appendix 14 obtained)
Q_FORMATION_POS   = 131  # comma-separated formation slots (1-11=starter, 0=bench)
Q_CAPTAIN         = 194  # source_player_id of the captain
Q_STATUS_FLAG     = 227  # unknown flag — preserved raw

# Provider qualifier IDs — typeId 19 (Player on)
Q_SUB_FORMATION_POS = 145  # formation slot the sub takes
Q_SUB_FLAG          = 293  # unknown flag — preserved raw

# Provider qualifier IDs — typeId 17 (Card)
Q_SECOND_YELLOW = 32
Q_RED_CARD      = 33

POSITION_CODE_MAP: dict[str, str] = {
    "1": "GK",
    "2": "DEF",
    "3": "MID",
    "4": "FWD",
    "5": "SUB",
}


# ── Database connection ────────────────────────────────────────────────────────
def get_connection() -> psycopg2.extensions.connection:
    load_dotenv()
    dsn = os.environ.get("FOOTBALL_DB_DSN")
    if not dsn:
        raise EnvironmentError("FOOTBALL_DB_DSN not set in environment / .env file")
    return psycopg2.connect(dsn)


# ── ID resolution (with per-match in-memory cache) ────────────────────────────
def resolve_id(
    cur,
    table: str,
    source_col: str,
    source_val: str,
    pk_col: str,
) -> Optional[int]:
    cur.execute(f"SELECT {pk_col} FROM {table} WHERE {source_col} = %s", (source_val,))
    row = cur.fetchone()
    return row[0] if row else None


def cached_player_id(cur, source_player_id: str, cache: dict) -> Optional[int]:
    if source_player_id not in cache["players"]:
        cache["players"][source_player_id] = resolve_id(
            cur, "silver.players", "source_player_id", source_player_id, "player_id"
        )
    return cache["players"][source_player_id]


def cached_team_id(cur, source_team_id: str, cache: dict) -> Optional[int]:
    if source_team_id not in cache["teams"]:
        cache["teams"][source_team_id] = resolve_id(
            cur, "silver.teams", "source_team_id", source_team_id, "team_id"
        )
    return cache["teams"][source_team_id]


# ── Raw file parsing ───────────────────────────────────────────────────────────
def parse_raw_file(file_path: Path) -> dict:
    """
    Provider files start with a hash prefix before the JSON payload, e.g.:
        W3c016d8247146d83bbaab35a8c936b83c2d3f036b({"matchInfo":...})
    Strategy: find the first '{', slice from there, strip a trailing ')' if present.
    """
    raw = file_path.read_bytes().decode("utf-8", errors="replace")
    start = raw.index("{")
    payload = raw[start:].rstrip()
    if payload.endswith(")"):
        payload = payload[:-1]
    return json.loads(payload)


# ── Qualifier helpers ──────────────────────────────────────────────────────────
def get_qualifier_value(qualifiers: list, qualifier_id: int) -> Optional[str]:
    for q in qualifiers:
        if q["qualifierId"] == qualifier_id:
            return q.get("value")
    return None


def has_qualifier(qualifiers: list, qualifier_id: int) -> bool:
    return any(q["qualifierId"] == qualifier_id for q in qualifiers)


def safe_int(value: Optional[str]) -> Optional[int]:
    if value is not None and str(value).strip().lstrip("-").isdigit():
        return int(value)
    return None


# ── Idempotency check ──────────────────────────────────────────────────────────
def is_already_processed(cur, match_id: int) -> bool:
    cur.execute(
        "SELECT 1 FROM silver.match_lineups WHERE match_id = %s LIMIT 1",
        (match_id,),
    )
    return cur.fetchone() is not None


# ── Pass 1 — typeId 34: INSERT all lineup rows ─────────────────────────────────
def pass1_lineup_event(cur, event: dict, match_id: int, cache: dict) -> int:
    """
    Parses the parallel arrays in qualifiers 30/44/59/131/227,
    inserts one row per player (starters + bench).
    Returns the number of rows inserted.
    """
    source_team_id = event.get("contestantId", "")
    team_id = cached_team_id(cur, source_team_id, cache)
    if team_id is None:
        log.warning("Pass1 — team not found in DB: %s", source_team_id)
        return 0

    qualifiers = event.get("qualifier", [])

    # Parallel comma-separated arrays
    def split_csv(qid: int) -> list[str]:
        raw = get_qualifier_value(qualifiers, qid) or ""
        return [v.strip() for v in raw.split(",")]

    player_src_ids  = split_csv(Q_PLAYER_IDS)
    position_codes  = split_csv(Q_POSITION_CODE)
    shirt_numbers   = split_csv(Q_SHIRT_NUMBERS)
    formation_slots = split_csv(Q_FORMATION_POS)
    status_flags    = split_csv(Q_STATUS_FLAG)

    # Team-level scalars
    team_formation  = get_qualifier_value(qualifiers, Q_TEAM_FORMATION)   # raw ID
    captain_src_id  = get_qualifier_value(qualifiers, Q_CAPTAIN)

    n = len(player_src_ids)
    inserted = 0

    for i in range(n):
        src_pid = player_src_ids[i]
        if not src_pid:
            continue

        player_id = cached_player_id(cur, src_pid, cache)
        if player_id is None:
            log.warning("Pass1 — player not found in DB: %s", src_pid)
            continue

        pos_code_str    = position_codes[i]  if i < len(position_codes)  else None
        shirt_str       = shirt_numbers[i]   if i < len(shirt_numbers)   else None
        fp_str          = formation_slots[i] if i < len(formation_slots) else "0"
        status_flag_str = status_flags[i]    if i < len(status_flags)    else None

        pos_code   = safe_int(pos_code_str)
        position   = POSITION_CODE_MAP.get(pos_code_str or "") 
        shirt_num  = safe_int(shirt_str)
        fp_val     = safe_int(fp_str) or 0
        status_flag = safe_int(status_flag_str)

        starting_xi        = fp_val > 0
        formation_position = fp_val if fp_val > 0 else None
        is_captain         = (src_pid == captain_src_id)
        minute_in          = 0 if starting_xi else None   # updated in pass 2b for subs

        cur.execute(
            """
            INSERT INTO silver.match_lineups (
                match_id, player_id, team_id,
                starting_xi, formation_position,
                position_code, position,
                shirt_number, is_captain,
                team_formation,
                minute_in, minute_out, exit_reason,
                raw_status_flag
            ) VALUES (
                %(match_id)s, %(player_id)s, %(team_id)s,
                %(starting_xi)s, %(formation_position)s,
                %(pos_code)s, %(position)s,
                %(shirt_num)s, %(is_captain)s,
                %(team_formation)s,
                %(minute_in)s, NULL, NULL,
                %(status_flag)s
            )
            ON CONFLICT (match_id, player_id) DO NOTHING
            """,
            {
                "match_id": match_id,
                "player_id": player_id,
                "team_id": team_id,
                "starting_xi": starting_xi,
                "formation_position": formation_position,
                "pos_code": pos_code,
                "position": position,
                "shirt_num": shirt_num,
                "is_captain": is_captain,
                "team_formation": team_formation,
                "minute_in": minute_in,
                "status_flag": status_flag,
            },
        )
        inserted += cur.rowcount

    return inserted


# ── Pass 2a — typeId 18: player subbed off ─────────────────────────────────────
def pass2a_player_off(cur, event: dict, match_id: int, cache: dict):
    src_pid = event.get("playerId")
    if not src_pid:
        return
    player_id = cached_player_id(cur, src_pid, cache)
    if player_id is None:
        log.warning("Pass2a — player not found: %s", src_pid)
        return

    cur.execute(
        """
        UPDATE silver.match_lineups
           SET minute_out  = %s,
               exit_reason = 'substitution'
         WHERE match_id = %s AND player_id = %s
        """,
        (event["timeMin"], match_id, player_id),
    )


# ── Pass 2b — typeId 19: substitute enters ────────────────────────────────────
def pass2b_player_on(cur, event: dict, match_id: int, cache: dict):
    src_pid = event.get("playerId")
    if not src_pid:
        return
    player_id = cached_player_id(cur, src_pid, cache)
    if player_id is None:
        log.warning("Pass2b — player not found: %s", src_pid)
        return

    qualifiers = event.get("qualifier", [])
    formation_pos = safe_int(get_qualifier_value(qualifiers, Q_SUB_FORMATION_POS))
    raw_sub_flag  = safe_int(get_qualifier_value(qualifiers, Q_SUB_FLAG))

    cur.execute(
        """
        UPDATE silver.match_lineups
           SET minute_in        = %s,
               -- only overwrite formation_position if the event provides one
               formation_position = COALESCE(%s, formation_position),
               raw_sub_flag     = %s
         WHERE match_id = %s AND player_id = %s
        """,
        (event["timeMin"], formation_pos, raw_sub_flag, match_id, player_id),
    )


# ── Pass 2c/2d — typeId 17: card event ────────────────────────────────────────
def pass2cd_card(cur, event: dict, match_id: int, cache: dict):
    qualifiers    = event.get("qualifier", [])
    is_red        = has_qualifier(qualifiers, Q_RED_CARD)
    is_sec_yellow = has_qualifier(qualifiers, Q_SECOND_YELLOW)

    # Yellow-only card — no lineup impact
    if not (is_red or is_sec_yellow):
        return

    src_pid = event.get("playerId")
    if not src_pid:
        return
    player_id = cached_player_id(cur, src_pid, cache)
    if player_id is None:
        log.warning("Pass2c/d — player not found: %s", src_pid)
        return

    exit_reason = "red_card" if is_red else "second_yellow"
    cur.execute(
        """
        UPDATE silver.match_lineups
           SET minute_out  = %s,
               exit_reason = %s
         WHERE match_id = %s AND player_id = %s
        """,
        (event["timeMin"], exit_reason, match_id, player_id),
    )


# ── Per-match orchestrator ─────────────────────────────────────────────────────
def process_match_file(conn, file_path: Path) -> str:
    """
    Full 5-pass ingestion for one raw match file.
    Returns: 'processed' | 'skipped' | 'error'
    """
    try:
        data = parse_raw_file(file_path)
    except (ValueError, json.JSONDecodeError) as exc:
        log.error("Parse error  %s — %s", file_path, exc)
        return "error"

    source_match_id = data.get("matchInfo", {}).get("id")
    if not source_match_id:
        log.error("Missing matchInfo.id in %s", file_path)
        return "error"

    # Per-match ID cache — avoids repeated DB lookups for the same player/team
    cache: dict = {"players": {}, "teams": {}}

    with conn.cursor() as cur:
        # ── Resolve internal match_id ─────────────────────────────────────────
        match_id = resolve_id(cur, "silver.matches", "source_match_id", source_match_id, "match_id")
        if match_id is None:
            log.warning(
                "Match not in DB yet — load matches first: %s (%s)",
                source_match_id,
                file_path.name,
            )
            return "skipped"

        # ── Idempotency guard ─────────────────────────────────────────────────
        if is_already_processed(cur, match_id):
            log.debug("Already processed — match_id=%s  source=%s", match_id, source_match_id)
            return "skipped"

        events = data.get("liveData", {}).get("event", [])

        # Sort by eventId to guarantee correct processing order for passes 2a/2b
        events_sorted = sorted(events, key=lambda e: e.get("eventId", 0))

        # ── Pass 1 — INSERT all lineup rows ───────────────────────────────────
        lineup_events = [e for e in events_sorted if e.get("typeId") == 34]
        if not lineup_events:
            log.warning("No typeId:34 events found in %s — skipping", file_path.name)
            return "skipped"

        total_inserted = 0
        for event in lineup_events:
            total_inserted += pass1_lineup_event(cur, event, match_id, cache)

        if total_inserted == 0:
            log.warning(
                "typeId:34 found but 0 rows inserted for match_id=%s — rolling back",
                match_id,
            )
            conn.rollback()
            return "error"

        # ── Passes 2a–2d — UPDATE in event order ──────────────────────────────
        for event in events_sorted:
            type_id = event.get("typeId")
            if   type_id == 18:
                pass2a_player_off(cur, event, match_id, cache)
            elif type_id == 19:
                pass2b_player_on(cur, event, match_id, cache)
            elif type_id == 17:
                pass2cd_card(cur, event, match_id, cache)

    conn.commit()
    log.info(
        "✓  match_id=%-6s  rows=%-3d  source=%-30s  file=%s",
        match_id,
        total_inserted,
        source_match_id,
        file_path.name,
    )
    return "processed"


# ── Directory scanner ──────────────────────────────────────────────────────────
def scan_match_files(base_path: Path):
    """
    Yields Path objects for every file under:
        {base_path}/{competition_code}/{season_code}/matches/

    Directories named anything other than 'matches' (e.g. 'squads') are ignored.
    """
    if not base_path.exists():
        raise FileNotFoundError(f"Base data path not found: {base_path.resolve()}")

    for competition_dir in sorted(base_path.iterdir()):
        if not competition_dir.is_dir():
            continue
        for season_dir in sorted(competition_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            matches_dir = season_dir / "matches"
            if not matches_dir.is_dir():
                log.debug("No matches/ dir in %s — skipping", season_dir)
                continue
            files = sorted(f for f in matches_dir.iterdir() if f.is_file())
            log.info(
                "Scanning  %s/%s/matches/  — %d file(s)",
                competition_dir.name,
                season_dir.name,
                len(files),
            )
            yield from files


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    log.info("─── match_lineups loader starting ───────────────────────────────")
    conn = get_connection()
    log.info("Connected to database")

    counts = {"processed": 0, "skipped": 0, "error": 0}

    try:
        for file_path in scan_match_files(BASE_DATA_PATH):
            try:
                result = process_match_file(conn, file_path)
                counts[result] += 1
            except Exception as exc:
                conn.rollback()
                log.error("Unhandled error on %s: %s", file_path, exc, exc_info=True)
                counts["error"] += 1
    finally:
        conn.close()

    log.info(
        "─── Done — processed=%-4d  skipped=%-4d  errors=%d ─────────────────",
        counts["processed"],
        counts["skipped"],
        counts["error"],
    )


if __name__ == "__main__":
    main()