"""Pure-function tests for src/export/match.py (no database)."""

from src.export.match import _defence, _progression, _shots, chain_index

SEQUENCES = [{"sequence_id": "70_away_015"}, {"sequence_id": "70_away_002"}]


def _shot(team_id: int, sequence_id: str | None) -> dict:
    return {
        "team_id": team_id, "player_id": 1, "player_name": "A", "minute": 5,
        "x": 90.0, "y": 30.0, "xg": 0.1, "event_type": "Miss", "blocked": False,
        "headed": False, "right_foot": True, "left_foot": False,
        "shot_play_pattern": "regular_play", "first_time": False, "big_chance": False,
        "goal_mouth_y": None, "goal_mouth_z": None, "sequence_id": sequence_id,
    }


def test_chain_index_points_into_the_shipped_list():
    s = chain_index(SEQUENCES)
    assert s({"sequence_id": "70_away_002"}) == 1
    # A chain under the event floor is not shipped, so there is nothing to open.
    assert s({"sequence_id": "70_away_099"}) is None
    assert s({"sequence_id": None}) is None


def test_only_the_published_teams_shots_link():
    s = chain_index(SEQUENCES)
    shots = _shots([_shot(10, "70_away_015"), _shot(20, "70_away_015")], 10, 20, s)
    assert [shot["s"] for shot in shots] == [0, None]


def test_progression_and_defence_rows_carry_s():
    s = chain_index(SEQUENCES)
    prog = _progression([{
        "event_type": "Carry", "x": 40.0, "y": 30.0, "end_x": 60.0, "end_y": 30.0,
        "xt": 0.02, "outcome": "success", "minute": 12, "player_name": "B",
        "sequence_id": "70_away_002",
    }], s)
    assert prog[0]["s"] == 1 and prog[0]["minute"] == 12 and prog[0]["player"] == "B"

    defence = _defence([{
        "event_type": "Ball recovery", "x": 30.0, "y": 20.0, "outcome": "success",
        "minute": 3, "player_name": "C", "sequence_id": "70_home_001",
    }], 35.0, s)
    assert defence["actions"][0]["s"] is None
