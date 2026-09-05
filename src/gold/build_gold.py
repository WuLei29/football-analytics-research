"""Gold-layer build runner.

Spec: md/GOLD_LAYER.md §5 (build order), §6 Step 10, §9 (the weekend guide).

Runs the gold builds in dependency order and reports what each one wrote::

    python -m src.gold.build_gold                     # full rebuild
    python -m src.gold.build_gold --step sequences
    python -m src.gold.build_gold --match-ids 1 2 3   # scope to some matches
    python -m src.gold.build_gold --competition-season-id 2

The runner enforces the §5 order rather than trusting the caller. Running a
step whose input table is empty produces a table that looks complete and is
silently wrong -- ``team_match_stats`` built before the phase pass leaves
``counter_attack_sequences`` at zero across the league -- so every step
declares a precondition and the run aborts if it is not met.

Default posture is a FULL REBUILD (§7). At the current volume the whole chain
runs in a couple of minutes, and a full rebuild is self-healing: it cannot
leave rows computed under an old definition sitting next to rows computed
under a new one. ``--match-ids`` exists for fast iteration while developing a
query, not as a routine refresh strategy.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

log = logging.getLogger("build_gold")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = Path(__file__).resolve().parent
DDL_DIR = PROJECT_ROOT / "sql" / "ddl"

# Applied in this order: functions and pitch zones before anything that uses
# them, team tables last. Each file is idempotent.
DDL_FILES = [
    DDL_DIR / "gold_functions.sql",
    DDL_DIR / "gold_sequences.sql",
    DDL_DIR / "gold_teams.sql",
]

# The §5 dependency order. Do not reorder.
STEP_ORDER = [
    "ddl",
    "sequences",
    "sequence_phases",
    "team_match_stats",
    "team_season_stats",
]


# ───────────────────────────────────────────────────────────────────────────
# Connection
# ───────────────────────────────────────────────────────────────────────────
def connect():
    load_dotenv(PROJECT_ROOT / ".env")
    dsn = os.environ.get("FOOTBALL_DB_DSN")
    if not dsn:
        raise SystemExit(
            "FOOTBALL_DB_DSN is not set. Put it in .env at the project root."
        )
    return psycopg2.connect(dsn)


def _count(cur, table: str) -> int:
    cur.execute(f"SELECT count(*) FROM {table}")
    return cur.fetchone()[0]


def _run_sql_file(cur, path: Path, params: dict | None = None) -> None:
    sql = path.read_text(encoding="utf-8")
    cur.execute(sql, params)


# ───────────────────────────────────────────────────────────────────────────
# Preconditions — §5: "the runner must enforce the order rather than trusting
# the caller". Each returns an error string, or None when satisfied.
# ───────────────────────────────────────────────────────────────────────────
def _check_sequences(cur) -> str | None:
    cur.execute(
        "SELECT count(*) FROM silver.events WHERE sequence_id IS NOT NULL"
    )
    if cur.fetchone()[0] == 0:
        return (
            "silver.events has no sequence_id values. Run "
            "`python -m src.silver.events.sequences` first (§5 step 1)."
        )
    return None


def _check_sequence_phases(cur) -> str | None:
    if _count(cur, "gold.sequences") == 0:
        return "gold.sequences is empty. Run --step sequences first."
    return None


def _check_team_match_stats(cur) -> str | None:
    if _count(cur, "gold.sequences") == 0:
        return "gold.sequences is empty. Run --step sequences first."
    if _count(cur, "gold.sequence_phase_segments") == 0:
        return (
            "gold.sequence_phase_segments is empty. The phase pass is a HARD "
            "dependency of the team build, not an optional enrichment (§5): "
            "without it counter_attack_sequences and high_transition_sequences "
            "are zero across the league, which looks plausible and is wrong. "
            "Run --step sequence_phases first."
        )
    return None


def _check_team_season_stats(cur) -> str | None:
    if _count(cur, "gold.team_match_stats") == 0:
        return "gold.team_match_stats is empty. Run --step team_match_stats first."
    return None


# ───────────────────────────────────────────────────────────────────────────
# Steps
# ───────────────────────────────────────────────────────────────────────────
def step_ddl(conn, args) -> str:
    with conn.cursor() as cur:
        for path in DDL_FILES:
            if not path.exists():
                raise SystemExit(f"missing DDL file: {path}")
            log.info("  applying %s", path.name)
            _run_sql_file(cur, path)
    conn.commit()
    return f"{len(DDL_FILES)} DDL files applied"


def step_sequences(conn, args) -> str:
    params = {"match_ids": args.match_ids}
    with conn.cursor() as cur:
        _run_sql_file(cur, SQL_DIR / "build_sequences.sql", params)
        n_seq = _count(cur, "gold.sequences")
        n_players = _count(cur, "gold.sequence_players")
    conn.commit()
    return f"gold.sequences {n_seq:,} rows | gold.sequence_players {n_players:,} rows"


def step_sequence_phases(conn, args) -> str:
    # Imported here so the SQL-only steps do not pay for it.
    from src.gold.sequence_phases import run_phase_pass

    n_seq, n_segments = run_phase_pass(conn, match_ids=args.match_ids)
    return (
        f"{n_seq:,} sequences classified | "
        f"gold.sequence_phase_segments {n_segments:,} rows"
    )


def step_team_match_stats(conn, args) -> str:
    params = {"match_ids": args.match_ids}
    with conn.cursor() as cur:
        _run_sql_file(cur, SQL_DIR / "build_team_match_stats.sql", params)
        n = _count(cur, "gold.team_match_stats")
    conn.commit()
    return f"gold.team_match_stats {n:,} rows"


def step_team_season_stats(conn, args) -> str:
    # Scoped DELETE + INSERT per competition_season_id (§4.4.2), so loading a
    # Segunda matchday never touches La Liga rows.
    with conn.cursor() as cur:
        if args.competition_season_id is not None:
            season_ids = [args.competition_season_id]
        else:
            cur.execute(
                "SELECT DISTINCT competition_season_id "
                "FROM silver.team_competition_seasons ORDER BY 1"
            )
            season_ids = [r[0] for r in cur.fetchall()]

        sql = (SQL_DIR / "build_team_season_stats.sql").read_text(encoding="utf-8")
        for cs_id in season_ids:
            cur.execute(sql, {"cs_id": cs_id})
        n = _count(cur, "gold.team_season_stats")
    conn.commit()
    return f"gold.team_season_stats {n:,} rows across {len(season_ids)} season(s)"


STEPS = {
    "ddl": (step_ddl, None),
    "sequences": (step_sequences, _check_sequences),
    "sequence_phases": (step_sequence_phases, _check_sequence_phases),
    "team_match_stats": (step_team_match_stats, _check_team_match_stats),
    "team_season_stats": (step_team_season_stats, _check_team_season_stats),
}


# ───────────────────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────────────────
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the gold layer in dependency order (GOLD_LAYER.md §5)."
    )
    parser.add_argument(
        "--step",
        choices=STEP_ORDER,
        help="run a single step instead of the whole chain",
    )
    parser.add_argument(
        "--from-step",
        choices=STEP_ORDER,
        help="run from this step to the end of the chain",
    )
    parser.add_argument(
        "--match-ids",
        type=int,
        nargs="+",
        help=(
            "scope the match-grain builds to these matches. Omit for a full "
            "rebuild, which is the recommended weekend posture (§7)."
        ),
    )
    parser.add_argument(
        "--competition-season-id",
        type=int,
        help="scope the season build to one competition_season_id",
    )
    parser.add_argument(
        "--skip-ddl",
        action="store_true",
        help="skip the DDL step (it is idempotent; skipping only saves a second)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.step:
        steps = [args.step]
    elif args.from_step:
        steps = STEP_ORDER[STEP_ORDER.index(args.from_step):]
    else:
        steps = list(STEP_ORDER)

    if args.skip_ddl and "ddl" in steps:
        steps.remove("ddl")

    if args.match_ids:
        log.info("scoped to %d match(es): %s", len(args.match_ids), args.match_ids)
    elif args.competition_season_id is not None:
        log.info(
            "all matches; season builds scoped to competition_season_id %d",
            args.competition_season_id,
        )
    else:
        log.info("full rebuild (all matches, all seasons)")

    conn = connect()
    conn.autocommit = False
    total_start = time.perf_counter()
    try:
        for name in steps:
            fn, precheck = STEPS[name]
            if precheck is not None:
                with conn.cursor() as cur:
                    problem = precheck(cur)
                if problem:
                    log.error("step '%s' cannot run: %s", name, problem)
                    conn.rollback()
                    return 1

            log.info("── %s ──", name)
            started = time.perf_counter()
            try:
                summary = fn(conn, args)
            except Exception:
                conn.rollback()
                log.exception("step '%s' failed — transaction rolled back", name)
                return 1
            log.info("   %s  (%.1fs)", summary, time.perf_counter() - started)
    finally:
        conn.close()

    log.info("done in %.1fs", time.perf_counter() - total_start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
