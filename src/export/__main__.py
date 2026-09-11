"""Web export runner.

Spec: md/WEB_DATA.md §11. Architecture decision: md/WEB_PLAN.md §2.

Reads gold (and, for the match file, the per-team slice of silver.events) and
writes the static JSON tree the Next.js site is built from::

    python -m src.export                      # everything implemented
    python -m src.export --only manifest,table
    python -m src.export --only match --match-ids 1013
    python -m src.export --season 2026-27
    python -m src.export --dry-run            # report, write nothing
    python -m src.export --out /tmp/data

Posture, matching build_gold: a FULL REWRITE every run. Every file the export
owns is regenerated, so a refresh cannot leave a file written under an old
contract version beside one written under a new one. There is no incremental
mode and there is no need for one -- the whole tree is a few MB.

The connection is READ ONLY. The export never writes to Postgres.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from . import db, league_table, manifest, match, overview
from .config import DEFAULT_OUT, EXPORT_TEAMS
from .io import Writer
from .seasons import load_seasons

log = logging.getLogger("export")

# Order matters only for readability of the log; the files are independent.
STEPS = ["manifest", "table", "overview", "match"]

# Specified in WEB_DATA.md §8-§10 but not yet implemented (§14 item 1).
PLANNED_STEPS = ["squad", "player", "sequences"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m src.export",
        description="Export the gold layer to web/public/data as static JSON.",
    )
    p.add_argument(
        "--only",
        help=f"comma-separated subset of: {', '.join(STEPS)} (default: all)",
    )
    p.add_argument(
        "--season",
        help="restrict to one season slug, e.g. 2026-27 (default: all published)",
    )
    p.add_argument(
        "--match-ids",
        type=int,
        nargs="+",
        help="restrict the match step to these match_ids (fast iteration only; "
             "the weekend run writes every match)",
    )
    p.add_argument("--out", type=Path, default=DEFAULT_OUT,
                   help=f"output root (default: {DEFAULT_OUT})")
    p.add_argument("--dry-run", action="store_true",
                   help="report what would be written without touching disk")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args(argv)

    steps = STEPS
    if args.only:
        steps = [s.strip() for s in args.only.split(",") if s.strip()]
        unknown = [s for s in steps if s not in STEPS]
        if unknown:
            planned = [s for s in unknown if s in PLANNED_STEPS]
            hint = (f" ({', '.join(planned)} is specified in WEB_DATA.md but not "
                    "implemented yet)") if planned else ""
            log.error("unknown step(s): %s%s", ", ".join(unknown), hint)
            return 2

    started = time.time()
    generated_at = datetime.now(timezone.utc)
    writer = Writer(args.out, dry_run=args.dry_run)

    conn = db.connect()
    try:
        seasons = load_seasons(conn)
        if args.season:
            seasons = [s for s in seasons if s.slug == args.season]
            if not seasons:
                log.error("season %s is not published; see EXPORT_SEASONS in "
                          "src/export/config.py", args.season)
                return 2

        for season in seasons:
            log.info("season %s: %d clubs, matchdays complete %d of %d%s",
                     season.slug, season.num_teams, season.matchdays_complete,
                     season.matchdays_scheduled,
                     ", league complete" if season.league_complete else "")

        # manifest.json describes every published season, so it is written once
        # over the whole set even when --season narrows the rest of the run.
        if "manifest" in steps:
            manifest.build(conn, writer, load_seasons(conn), generated_at)

        if "table" in steps:
            for season in seasons:
                league_table.build(conn, writer, season, generated_at)

        if "overview" in steps:
            for team_cfg in EXPORT_TEAMS:
                for season in seasons:
                    overview.build(conn, writer, team_cfg, season, generated_at)

        # One file per match of every published club: the biggest step by far,
        # and the only one that reads silver.events (a few seconds per match).
        if "match" in steps:
            for team_cfg in EXPORT_TEAMS:
                for season in seasons:
                    match_ids = db.fetch_team_match_ids(
                        conn, season.competition_season_id, team_cfg.team_id
                    )
                    if args.match_ids:
                        wanted = set(args.match_ids)
                        skipped = wanted - set(match_ids)
                        match_ids = [m for m in match_ids if m in wanted]
                        if skipped and not match_ids:
                            log.warning(
                                "%s %s: none of %s is a match of this club and "
                                "season", team_cfg.slug, season.slug,
                                ", ".join(str(m) for m in sorted(skipped)),
                            )
                    log.info("%s %s: %d match files", team_cfg.slug, season.slug,
                             len(match_ids))
                    for match_id in match_ids:
                        match.build(conn, writer, team_cfg, season, match_id,
                                    generated_at)
    finally:
        conn.close()

    writer.report()
    log.info("done in %.1fs", time.time() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
