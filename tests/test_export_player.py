"""Pure helpers of the screen-03 export (src/export/squad.py, player.py,
match.simplify). No database: every function under test takes rows."""

from datetime import date

from src.export.match import simplify
from src.export.config import PlayerKpi
from src.export.player import (
    FLAG_ASSIST, FLAG_COMPLETED, FLAG_PROGRESSIVE, age_on, defence_outcome,
    kpi_value, pass_row, squad_rank, top_ids,
)
from src.export.squad import display_name, squad_order


def test_display_name_prefers_known_name_split_on_last_space():
    assert display_name({"known_name": "Puado", "first_name": "Javier"}) == {
        "first": "", "last": "Puado", "full": "Puado"}
    assert display_name({"known_name": "Kike García"})["last"] == "García"


def test_display_name_falls_back_to_short_names():
    row = {"known_name": None, "first_name": "Omar", "last_name": "El Hilali Mohamed",
           "short_first_name": "Omar", "short_last_name": "El Hilali",
           "match_name": "O. El Hilali"}
    assert display_name(row) == {"first": "Omar", "last": "El Hilali", "full": "O. El Hilali"}


def test_squad_order_groups_by_role_then_minutes_then_id():
    rows = [
        {"player_id": 3, "role_group": "ST", "primary_position_group": "ST", "minutes_played": 900},
        {"player_id": 2, "role_group": "AM", "primary_position_group": "MID", "minutes_played": 500},
        {"player_id": 1, "role_group": "GK", "primary_position_group": "GK", "minutes_played": 90},
        {"player_id": 5, "role_group": "MID", "primary_position_group": "MID", "minutes_played": 500},
        {"player_id": 4, "role_group": "MID", "primary_position_group": "MID", "minutes_played": 500},
    ]
    assert [r["player_id"] for r in squad_order(rows)] == [1, 4, 5, 2, 3]


def test_simplify_keep_forces_the_index_and_default_is_unchanged():
    # A zig-zag: every inner point is far from the start-end line.
    points = [[float(i * 10), 0.0 if i % 2 else 20.0] for i in range(12)]
    plain = simplify(points, 3)
    assert len(plain) == 5
    kept = simplify(points, 3, keep=(6,))
    assert any(p is points[6] for p in kept)
    assert kept[0] is points[0] and kept[-1] is points[-1]
    # Short chains come back whole.
    short = points[:4]
    assert simplify(short, 6, keep=(2,)) is short


def test_defence_outcome_follows_the_brief():
    assert defence_outcome("Challenge", "failure") == "lost"
    assert defence_outcome("Attempted Tackle", "success") == "lost"
    assert defence_outcome("Tackle", "success") == "won"
    assert defence_outcome("Tackle", "failure") == "lost"
    assert defence_outcome("Clearance", "failure") == "won"


def test_pass_row_flags():
    row = {"x": 10.04, "y": 5.0, "end_x": 40.0, "end_y": 6.0, "completed": True,
           "progressive": True, "key_pass": None, "assist": False, "xt": 0.01234}
    assert pass_row(row, 3) == [10.0, 5.0, 40.0, 6.0, FLAG_COMPLETED | FLAG_PROGRESSIVE, 3, 0.012]
    assert pass_row({**row, "completed": False, "progressive": False, "assist": True}, 0)[4] == FLAG_ASSIST


def test_squad_rank_respects_the_gate_and_ties():
    rows = [
        {"player_id": 1, "minutes_played": 900, "v": 2.0},
        {"player_id": 2, "minutes_played": 900, "v": 3.0},
        {"player_id": 3, "minutes_played": 900, "v": 3.0},
        {"player_id": 4, "minutes_played": 40, "v": 9.0},
    ]
    assert squad_rank(rows, 1, "v", 450) == (3, 3)
    assert squad_rank(rows, 3, "v", 450) == (1, 3)
    assert squad_rank(rows, 4, "v", 450) == (None, 3)


def test_age_on():
    assert age_on(date(2003, 9, 12), date(2026, 5, 24)) == 22
    assert age_on(date(2003, 5, 24), date(2026, 5, 24)) == 23
    assert age_on(None, date(2026, 5, 24)) is None


def _match(minutes, **parts):
    return {"minutes": minutes, "kpi_parts": parts}


def test_kpi_value_rebuilds_totals_rates_and_per_90():
    matches = [
        _match(90, goals=1, assists=0, tackles=2, tackles_missed=1, dribbled_past=1, vaep=0.3),
        _match(45, goals=0, assists=2, tackles=0, tackles_missed=0, dribbled_past=0, vaep=0.15),
    ]
    assert kpi_value(PlayerKpi("goal_contributions"), matches) == 3
    # A rate is pooled over the matches, not an average of match rates.
    assert kpi_value(PlayerKpi("ground_duel_win_rate", "pct", 1, total=False), matches) == 50.0
    assert kpi_value(PlayerKpi("vaep_per_90", decimals=3, total=False), matches) == 0.3
    # No denominator in the selection: not applicable, never 0.
    assert kpi_value(PlayerKpi("ground_duel_win_rate", "pct", 1, total=False), matches[1:]) is None


def test_top_ids_puts_nulls_last_and_breaks_ties_on_start_event():
    rows = [
        {"sequence_id": "a", "xt": 0.1, "start_event_id": 9},
        {"sequence_id": "b", "xt": None, "start_event_id": 1},
        {"sequence_id": "c", "xt": 0.1, "start_event_id": 2},
        {"sequence_id": "d", "xt": 0.5, "start_event_id": 5},
    ]
    assert top_ids(rows, "xt", 3) == ["d", "c", "a"]
    assert top_ids(rows, "xt", 9)[-1] == "b"
