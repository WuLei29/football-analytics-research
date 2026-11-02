---
name: weekend-update
description: >
  The end-to-end runbook for refreshing the Espanyol site with new matchdays:
  scrape Opta match files, download squads, load silver in order, classify
  sequences, rebuild gold, validate, re-export web/public/data and commit.
  Gives the exact commands, the values the scraper asks for, a checkpoint
  after every step (what to count, what "good" looks like) and the Windows
  traps that have already cost time. Use when the user says "weekend run",
  "update the data", "new matchday", "scrape and re-export", or starts a new
  season. Not for changing pipeline code (use football-silver-schema /
  opta-events-reference) or adding charts (new-viz).
---

# Weekend update

Commands and definitions live in `CLAUDE.md` ("How to Run Pipelines") and
`md/GOLD_LAYER.md §9`. This skill is the **order, the checkpoints and the
traps**. Don't copy those docs here; follow them and verify after each step.

**Working agreement:** every step is idempotent, so re-running is safe, but
two things are not: the events truncate (destructive, ask first) and the
final commit (propose, then wait for approval). Report counts after each
step; don't chain blindly.

## 0. Environment (Windows)

- Interpreter: `.venv/Scripts/python.exe` (`py` also works for the scraper).
  In Git Bash plain `python` is the Microsoft Store stub and fails.
- Prefix every run with `PYTHONIOENCODING=utf-8`; the scraper banner crashes
  on cp1252 when output is piped or redirected.
- Never pipe input from PowerShell: it prepends a BOM to the first line (the
  URL becomes invalid). Use bash `printf ... |` if you must pipe.
- In bash, `cmd > file 2>&1` (this order). `2>&1 > file` leaves the logs on the
  terminal, because the loaders log to stderr.
- A python heredoc has no caller frame: `load_dotenv(r"<path>\.env")` needs the
  explicit path.

## 1. Scrape (`py -m src.scraper`) — needs the user

Interactive. Run it **headed**, in the user's terminal (`! py -m src.scraper`).
Headless could not auto-detect the matchday selector and fell back to manual
mode, so it scraped nothing.

Prompts, in order:

| Prompt | Value |
|---|---|
| Results URL | scoresway results page, e.g. `https://www.scoresway.com/en_GB/soccer/primera-divisi%C3%B3n-2026-2027/830epggffy1nfkfyrtpqdwhlg/results` |
| League code | `PRD` |
| Season code | `2026-2027` |
| Matchdays | `all` (files already on disk are skipped) |
| Proceed? | check the list of new matches, then `y` |

Check: file count in `data/raw/PRD/<season>/matches/` rose by the number
listed. A match played after the scrape needs the scraper run again (the user
can also drop the file in by hand).

## 2. Squads — the user downloads them by hand

Folder `data/raw/PRD/<season>/squads/<YYYY-MM-DD>/{team_id}.json`, one file
per club. **Squads must be loaded before lineups** for any new matchday: new
arrivals are not in `silver.players` and their lineup rows are dropped.

Pre-load checks (do them, they have each failed once):

```bash
D=data/raw/PRD/2026-2027/squads/<date>
ls $D | wc -l                                   # 20 for La Liga
for f in $D/*.json; do [ "$(head -c 1 $f)" != "{" ] && echo "NOT JSON: $f"; done
```

- A file starting `W3ce...({"squad"...` is JSONP (wrong copy); the loader says
  `Expecting value: line 1 column 1`. Re-download as plain JSON.
- A club missing from the folder is silent. Diff against the previous
  snapshot's file list (`comm -23`) to see which.

## 3. Load silver (in this order)

```bash
export PYTHONIOENCODING=utf-8; P=.venv/Scripts/python.exe
$P src/silver/load_competition_seasons.py --dry-run   # anchor exists? ("already present")
$P src/bronze/parse_teams_bronze.py                   # [SKIP] already exists is normal
$P src/silver/load_teams.py                           # "All N team(s) already present"
$P -m src.silver.squads --raw-root data/raw
$P src/silver/load_matches.py                         # "inserted: N, skipped: M"
$P src/silver/load_match_lineups.py --reprocess
```

Checkpoints:

- `load_matches`: `inserted` must equal the number of new files. If it says
  nothing was inserted and files are new, the `competition_seasons` anchor is
  missing.
- Lineups: count `not found in DB` warnings **for the current season only**
  (awk from the `Scanning PRD/<season>` line). Target **0**. Warnings from
  2024-2025 are legacy and expected. Non-zero means a squad is missing or
  stale: go back to step 2, re-run squads, then `--reprocess` again.
- Squads log is quiet on success. A `Transaction rolled back` / traceback is a
  failure. (Fixed 2 Nov 2026: `close_squad_entries` used `execute_values` on an
  `UPDATE` with four placeholders; it only runs when a player has left a squad.)

## 4. Events

```bash
$P -m src.silver.events --raw-root data/raw > /tmp/events.log 2>&1
```

Runs parse, carries, xT, SPADL, VAEP, game state, foot preference and xG for
matches not yet loaded. Run it in the background and read the tail. Do **not**
truncate `silver.events` unless re-processing loaded matches (destructive; ask,
and run `VACUUM (FULL, ANALYZE) silver.events` afterwards).

Measured (2 Nov 2026, 40 matches): ~5 min. Log ends with
`Run complete — loaded: N, skipped: M, failed: 0`, then game state, foot
preference and xG (`xG complete: N matches`). `failed` must be 0 and `loaded`
must equal the matches inserted in step 3.

Check: new matches have events, `xg` and `vaep_value` non-null, and no NULL
`player_id` on passes/shots for the current season.

## 5. Sequences, then gold

```bash
$P -m src.silver.events.sequences        # separate step, new matches only
$P -m src.gold.build_gold                # full rebuild, ~5 min
```

Sequences must precede gold (gold reads `sequence_id`). Measured: sequences
32 s for 40 matches; **gold ~10 min** at 536 matches (the docs say ~5; run it
in the background). `team_match_stats` rows must equal 2 x matches.

Then the gates in `md/GOLD_LAYER.md §9.5`. The invariants that matter: G1 gold
sequences = silver sequences, G2 for/against columns symmetric, G3
goals/xG/passes reconcile with silver, G4 shares sum to 1. Counts are
snapshots; a broken invariant is a finding, a changed count is "re-baseline".
Run them read-only with a psycopg2 script (G1-G4 queries are in §9.5).

Known false alarm: G3's xG can read 1410.0 in silver and 1410.1 in gold.
`gold.team_match_stats.xg_for` is `real` (float4), and casting its sum to
numeric keeps 6 significant digits before `round(,1)`. Compare the unrounded
sums (equal to ~1e-3) before calling it a finding.

## 6. Export (`python -m src.export`)

```bash
$P -m src.export --dry-run
$P -m src.export
```

Measured: dry run and real run ~14 s each. The site publishes **only Espanyol**,
so of N new league matches only Espanyol's get a match file (+ players with
first appearances); the rest shows up as changed `table.json`, `manifest.json`,
overview, squad and player files. Expect ~99 modified + a handful of new files
for a normal weekend.

Export player and match files in the same run (they share `sequence_id`).
If the `.tmp` rename fails with WinError 5, OneDrive holds the file: wait and
re-run. Review `git diff --stat web/public/data`: new match files plus changed
`manifest.json`, `table.json`, overview and squad files; unexpected churn in old
matches means a non-unique sort or a rebuilt `sequence_id` (expected after a
sequences re-run).

## 7. Commit

Propose the commit (message, files) and wait for approval. `web/public/data` is
the deploy artifact. Add a dated entry to `md/CHANGELOG.md`.

## Report format

After the run, one table: step, rows in / out, warnings, minutes. Say plainly
what was skipped or failed.
