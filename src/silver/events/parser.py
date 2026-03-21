"""
parser.py
─────────
Transforms a raw provider JSONP file into a list of event dicts.

Responsibilities:
  - Strip the JSONP wrapper and parse JSON
  - Load event/qualifier mapping files
  - Build jersey number and team context lookups
  - Extract and normalise each event row (coordinates, qualifiers, outcome)

No DB access. No carries. No xT. Pure transformation.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


# ── Mapping loader ─────────────────────────────────────────────────────────────

def load_mapping_file(file_path: str) -> Dict[int, Dict[str, str]]:
    """
    Parse a provider JS mapping file (OptaEvents.js / OptaQualifiers.js)
    and return a dict keyed by integer ID.
    """
    with open(file_path, "r", encoding="utf-8") as fh:
        content = fh.read()

    match = re.search(r'var\s+\w+\s*=\s*(\{.*\});?', content, re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract mapping dict from {file_path}")

    js_object = match.group(1)
    js_object = re.sub(r'(\w+):', r'"\1":', js_object)
    js_object = re.sub(r"'([^']*)'", r'"\1"', js_object)

    return {int(k): v for k, v in json.loads(js_object).items()}


# ── JSONP extraction ───────────────────────────────────────────────────────────

def extract_json(file_path: str) -> Dict[str, Any]:
    """Strip the provider JSONP wrapper and return the inner dict."""
    with open(file_path, "r", encoding="utf-8") as fh:
        content = fh.read()

    match = re.search(r'^[^(]+\((.*)\)$', content, re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract JSON from JSONP wrapper in {file_path}")
    return json.loads(match.group(1))


def extract_source_match_id(file_path: str) -> Optional[str]:
    """Quick extraction of matchInfo.id without full processing."""
    try:
        data = extract_json(file_path)
        return data.get("matchInfo", {}).get("id")
    except Exception:
        return None


# ── Context builders ───────────────────────────────────────────────────────────

def build_team_mapping(contestants: List[Dict]) -> Dict[str, Dict[str, str]]:
    """
    Build {source_team_id: {teamName, oppositionTeamName, h_a}} from matchInfo.contestant.
    """
    if len(contestants) != 2:
        raise ValueError(f"Expected exactly 2 contestants, got {len(contestants)}")

    mapping = {}
    for c in contestants:
        mapping[c["id"]] = {
            "teamName": c["name"],
            "h_a":      c["position"],
            "oppositionTeamName": None,
        }

    ids = list(mapping)
    mapping[ids[0]]["oppositionTeamName"] = mapping[ids[1]]["teamName"]
    mapping[ids[1]]["oppositionTeamName"] = mapping[ids[0]]["teamName"]
    return mapping


def build_jersey_mapping(events: List[Dict]) -> Dict[str, int]:
    """
    Extract {source_player_id: jersey_number} from typeId:34 (Team set up) events.
    """
    jersey_map = {}
    for event in events:
        if event.get("typeId") != 34:
            continue
        qualifiers = event.get("qualifier", [])
        player_ids_str  = next((q["value"] for q in qualifiers if q["qualifierId"] == 30), "")
        jersey_nums_str = next((q["value"] for q in qualifiers if q["qualifierId"] == 59), "")

        player_ids  = [p.strip() for p in player_ids_str.split(",")]
        jersey_nums = [n.strip() for n in jersey_nums_str.split(",")]

        if len(player_ids) != len(jersey_nums):
            continue

        for pid, num in zip(player_ids, jersey_nums):
            if pid and num:
                try:
                    jersey_map[pid] = int(num)
                except ValueError:
                    jersey_map[pid] = 0

    return jersey_map


# ── Event extraction ───────────────────────────────────────────────────────────

_OUTCOME_MAP = {1: "success", 0: "failure"}

_BASIC_FIELDS = {
    "id":           "source_event_id",
    "eventId":      "provider_event_id",
    "typeId":       "type_id",
    "timeMin":      "minute",
    "timeSec":      "second",
    "contestantId": "source_team_id",
    "playerId":     "source_player_id",
    "playerName":   "player_name",
    "x":            "x",
    "y":            "y",
    "periodId":     "period",
    "timeStamp":    "timestamp",
}

# Qualifier IDs that map to named coordinate columns
_QUALIFIER_COORD_MAP = {
    "Pass End X":                  ("end_x",       "x"),
    "Pass End Y":                  ("end_y",       "y"),
    "Blocked x co-ordinate":       ("blocked_x",   "x"),
    "Blocked y co-ordinate":       ("blocked_y",   "y"),
    "Goal mouth z coordinate":     ("goal_mouth_z", None),
    "Goal mouth y coordinate":     ("goal_mouth_y", None),
}


def _scale(value: Any, axis: Optional[str]) -> Optional[float]:
    """Convert a 0-100 provider coordinate to real pitch dimensions (105×68)."""
    try:
        v = float(value)
        if axis == "x":
            return (v / 100) * 105
        if axis == "y":
            return (v / 100) * 68
        return v
    except (TypeError, ValueError):
        return None


def _extract_event(
    event: Dict,
    event_codes: Dict[int, Dict],
    qualifier_codes: Dict[int, Dict],
    team_mapping: Dict[str, Dict],
    jersey_mapping: Dict[str, int],
    match_id: int,
) -> Optional[Dict]:
    """
    Transform a single provider event dict into a flat row dict.
    Returns None for 'Unknown' event types (filtered at call site).
    """
    row: Dict[str, Any] = {"match_id": match_id}

    # Basic fields
    for src_key, dst_key in _BASIC_FIELDS.items():
        row[dst_key] = event.get(src_key)

    # event_type
    type_id = event.get("typeId")
    event_info = event_codes.get(type_id, {"name": "Unknown"})
    row["event_type"] = event_info["name"]
    if row["event_type"] == "Unknown":
        return None

    # outcome
    row["outcome"] = _OUTCOME_MAP.get(event.get("outcome"))

    # Scale x/y from provider 0-100 to real pitch
    row["x"] = _scale(row["x"], "x")
    row["y"] = _scale(row["y"], "y")

    # Named coordinate columns from qualifiers
    named_coords: Dict[str, Optional[float]] = {
        "end_x": None, "end_y": None,
        "blocked_x": None, "blocked_y": None,
        "goal_mouth_z": None, "goal_mouth_y": None,
    }
    for q in event.get("qualifier", []):
        qinfo = qualifier_codes.get(q["qualifierId"], {"name": ""})
        qname = qinfo.get("name", "")
        if qname in _QUALIFIER_COORD_MAP:
            col, axis = _QUALIFIER_COORD_MAP[qname]
            named_coords[col] = _scale(q.get("value"), axis)

    row.update(named_coords)

    # Fallback: use x/y as end_x/end_y when not set
    row["end_x"] = row["end_x"] if row["end_x"] is not None else row["x"]
    row["end_y"] = row["end_y"] if row["end_y"] is not None else row["y"]

    # Jersey number
    src_pid = row.get("source_player_id")
    row["jersey_number"] = jersey_mapping.get(src_pid) if src_pid else None

    # Team context
    src_tid = row.get("source_team_id")
    if src_tid and src_tid in team_mapping:
        t = team_mapping[src_tid]
        row["team_name"]           = t["teamName"]
        row["opposition_team_name"] = t["oppositionTeamName"]
        row["h_a"]                 = t["h_a"]
    else:
        row["team_name"] = row["opposition_team_name"] = row["h_a"] = None

    # FK placeholders — resolved later by db.resolve_fk_columns()
    row["team_id"]   = None
    row["player_id"] = None

    # Sequence placeholders — populated in a later pass
    row["sequence_id"]           = None
    row["sequence_start"]        = False
    row["sequence_end"]          = False
    row["sequence_event_number"] = None

    # Threat metrics — populated later by xt.calculate_xt()
    row["start_zone_value_xt"] = None
    row["end_zone_value_xt"]   = None
    row["xt"]                  = None

    # Full payload for future-proofing
    row["raw_data"] = json.dumps(event)

    return row


# ── Public API ─────────────────────────────────────────────────────────────────

def parse_event_file(
    file_path: str,
    match_id: int,
    event_codes: Dict[int, Dict],
    qualifier_codes: Dict[int, Dict],
) -> List[Dict]:
    """
    Parse a single provider JSONP file and return a list of event row dicts.

    Args:
        file_path:        Path to the provider JSONP file.
        match_id:         Internal match_id (already resolved from silver.matches).
        event_codes:      Output of load_mapping_file() for OptaEvents.js.
        qualifier_codes:  Output of load_mapping_file() for OptaQualifiers.js.

    Returns:
        List of flat event dicts ready for DataFrame construction.
        Unknown event types are silently excluded.
    """
    data   = extract_json(file_path)
    events = data["liveData"]["event"]

    team_mapping   = build_team_mapping(data["matchInfo"]["contestant"])
    jersey_mapping = build_jersey_mapping(events)

    rows = []
    for event in events:
        row = _extract_event(event, event_codes, qualifier_codes,
                             team_mapping, jersey_mapping, match_id)
        if row is not None:
            rows.append(row)

    return rows