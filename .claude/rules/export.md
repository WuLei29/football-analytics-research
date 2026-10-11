---
paths:
  - "src/export/**"
  - "web/public/data/**"
---

# Web export rules

Contract: `md/WEB_DATA.md`. Workflow for a new field: the `new-viz` skill.

- **Read-only, one connection, `SET TRANSACTION READ ONLY`.** All SQL lives in
  `db.py`; config and thresholds live in `config.py`.
- **Every sort ends on a unique key.** Ties reshuffle between runs and turn
  the committed `web/public/data` diff into noise (four sorts were fixed on
  29 Sep 2026).
- **Coordinates go in the published team's attacking frame**, in metres,
  rounded to 0.1. Mirror opponent events here, never in a component (WEB_DATA §3.1).
- **Fail loudly.** An unmapped enum, missing season or missing gold row raises.
  `null` means "not applicable", never 0.
- **Shares ship display-scaled** (`possession_pct` 41.7). Keys only, no Spanish.
  Labels live in `web/lib/labels.ts`.
- **Site percentiles use their own gate** (`MIN_MINUTES_SHARE = 0.25` of club
  minutes), not gold's 450-minute floor (WEB_DATA §3.6).
- **`web/lib/data/types.ts` is hand-written.** Mirror every field change there.
  If you remove or retype a field, bump `SCHEMA_VERSION` in both places.
- **The `.tmp` → final rename can fail with WinError 5** because OneDrive holds
  the file. Wait and re-run; it's idempotent.
- `web/public/data/` is committed and is the deploy artifact. Re-export after
  every gold rebuild.
