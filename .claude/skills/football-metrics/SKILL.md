---
name: football-metrics
description: >
  The project's metric glossary: what every football metric shown or computed
  here actually means, in one line each, and where it is computed (SQL helper,
  gold column, export key). Covers pitch regions, the event sets behind shots /
  touches / defensive actions, progressive passes and carries, key passes,
  assists, xA, big chances, SCA, receptions, duels, PPDA, field tilt,
  possession, xG / xT / VAEP, per-90, possession adjustment, percentile peer
  groups and minute floors, plus what Opta does NOT record (pressures, receiver,
  position changes).

  Use this skill whenever a metric name appears in a request, a design handoff,
  a chart legend, a percentile card or a SQL query: before exporting it, before
  labelling it on the site, before comparing it with an outside source, and
  ALWAYS when a design brief states its own definition (briefs are not the
  authority, and several already disagree with the repo; see §8).
---

# Football metrics: the glossary

One line per metric, then where it lives. This skill is an **index, not the
authority**. When it disagrees with the code, the code wins, and you fix this
file.

## 0. Authority order

1. **The SQL**: `sql/ddl/gold_functions.sql` (geometry helpers),
   `src/gold/build_*.sql` (every count), `sql/ddl/gold_*.sql` (generated ratios
   and per-90s).
2. **`md/GOLD_LAYER.md`**: §4.5.0 canonical definitions, §4.5 team metrics,
   §4.6 player metrics, §4.6.14 normalisation, §4.7 receptions, §4.8 game state.
3. **`md/WEB_DATA.md`**: how a metric is shaped for the site (export gates,
   scaling, keys).
4. **This file.**
5. **Design handoffs** (`web/design_handoff_espanyol_analytics/*.md`). These
   are layout authority only. Their metric definitions are the designer's
   placeholders. If a handoff says "≥10 m upfield", that's a question for the
   user, not a spec (§8).

## 1. Frame and regions

- **Metres, 105 × 68.** Silver stores Opta 0–100 scaled (`x*1.05`, `y*0.68`).
- **Every event is in the acting team's attacking frame**: `x = 105` is the
  goal that team attacks. To put an opponent event in our frame:
  `x' = 105 − x`, `y' = 68 − y`. The web export does this mirroring (WEB_DATA §3.1);
  components never flip.
- **Low `y` is the right flank** (839:13 right-footers in slot 2). Don't "fix" it.

| Region | Condition | Helper |
|---|---|---|
| Defensive / middle / final third | `x < 35` / `35 ≤ x < 70` / `x ≥ 70` | `gold.third(x)` |
| Penalty area | `x ≥ 88.5 AND 13.84 ≤ y ≤ 54.16` | `gold.in_box(x, y)` |
| Six-yard box | `x ≥ 99.5 AND 24.84 ≤ y ≤ 43.16` | — |
| PPDA zone | `x ≥ 42` | inline |
| 30 zones | 6 strips × 5 channels, `zone_id = strip*10 + channel` (11…65) | `gold.zone_id(x, y)`; `gold.pitch_zones` is authoritative |
| xT surface | 12 × 8 grid (8.75 m × 8.5 m cells), a **different** grid | `xt.py` `XT_GRID`; `web/lib/viz/pitch.ts` `xtCellRect` |

Pass lengths: short `< 15 m`, medium `15–30 m`, long `≥ 30 m`
(`gold.action_length`). A long **ball** is a different thing: Opta Q157
("launch"), whatever its length.

## 2. Event sets: define once, reuse everywhere

| Set | Members | Notes |
|---|---|---|
| **Shot** | `Goal`, `Attempt Saved`, `Miss`, `Post` | Own goals are `Goal` + Q28 and count for the **opponent**. |
| **On target** | `Goal`, or `Attempt Saved` **without Q82** | Q82 = blocked. Counting every `Attempt Saved` is the classic SoT bug. |
| **Touch** (on-ball) | `Pass, Carry, Take On, Ball touch, Clearance, Ball recovery, Keeper pick-up, Claim, Punch, Smother` + shots | Excludes Tackle / Interception / Blocked Pass / Aerial / Save: those happen while the opponent has the ball. |
| **Defensive actions** (broad) | `Tackle, Interception, Clearance, Blocked Pass, Ball recovery` | Volume metrics, `def_action_avg_x`, defensive line height. |
| **PPDA actions** (narrow) | `Tackle, Interception, Challenge`, `Foul` with `outcome='failure'`, all at `x ≥ 42` | PPDA only. Never mix with the broad set. |
| **Carry** | Synthesised, `type_id = -1`, `event_type='Carry'`, ≥ **1 m** (`MIN_CARRY_DISTANCE_M`) | Not Opta. See `opta-events-reference`. |

Opta semantics that are easy to invert (counted, not assumed; GOLD_LAYER §4.6.8):
- `Challenge` = **this player was dribbled past**. It's always `failure` and pairs
  1:1 with successful `Take On`s. That makes it `dribbled_past`, not a duel.
- `Attempted Tackle` (typeId 83) is a separate, always-failed event, and it's in
  the player `tackle_success_rate` denominator.
- `Foul`: `failure` = committed, `success` = won.
- `Aerial`: both contestants get a row, so won + lost is the true duel count.
- `Ball touch` + `success` is a **deflection**, not a touch the player chose.
- `Error` and `Good Skill` are annotations, not actions.

## 3. Player metrics (`gold.player_match_stats` → `player_season_stats`)

Source of every row: `src/gold/build_player_match_stats.sql`. Season rows sum
match rows. Ratios are STORED GENERATED columns over the summed inputs.

### Scoring
| Metric | Definition |
|---|---|
| `goals`, `goals_non_penalty` | `Goal` events; non-penalty excludes `shot_play_pattern='penalty'` |
| `shots`, `shots_on_target`, `shots_in_box` | §2 sets; in box by **shot origin** |
| `xg`, `npxg` | Σ `silver.events.xg`; npxg drops penalties (fixed 0.79) |
| `xg_overperformance` | `goals_non_penalty − npxg`. Not `goals − xg`, which flatters penalty takers. |
| `big_chances` | Opta Q214 on the shot. `big_chances_missed` = the `Chance missed` event. |
| `xg_per_shot`, `shot_conversion_rate`, `shot_accuracy` | xg/shots, goals/shots, SoT/shots |

### Chance creation
| Metric | Definition |
|---|---|
| `assists` | Pass with `value_assist = '16'` (the shot it created was a Goal) |
| `key_passes` | Pass with `value_assist IN ('13','14','15')`: the shot was Miss/Post/Saved. **Excludes assists.** `chances_created = assists + key_passes`. |
| `xa` | xG of the first shot after the assist-tagged pass, in the same `sequence_id` |
| `second_assists` | Q218 |
| `crosses` / `_completed` / `_from_open_play` | Pass + Q2; open play drops Q5 (free kick) and Q6 (corner) |
| `through_balls` | Pass + Q4 |
| `passes_into_penalty_area` | Pass ending in the box from outside it (attempted, not completed) |
| `shot_creating_actions` | The two previous **teammate** actions before a shot, same `sequence_id`, shooter excluded |

### Passing and progression
| Metric | Definition |
|---|---|
| `passes`, `passes_completed` | `Pass`; completed = `outcome='success'` |
| **progressive** (pass and carry) | `gold.is_progressive`: end point at least **25% closer** to goal centre (105, 34) than the start. **Same rule for passes and carries.** Picked over "≥10 m upfield", which rewards sideways balls near the touchline. `progressive_passes` counts attempts, `_completed` the successful ones. |
| `passes_into_final_third` | `x < 70 → end_x ≥ 70` |
| `passes_forward` / `_backward` | `end_x − x > 2` / `< −2` |
| `switches` | `|end_y − y| ≥ 30` |
| `long_balls` | Q157 |
| `passes_received` | Next same-team event after a completed pass (`lead()` over the team stream). `gold.pass_receptions` is the richer version (§5). |
| `xt_pass` | Σ xt of the player's passes |

### Carrying and dribbling
| Metric | Definition |
|---|---|
| `carries`, `carry_distance_total` | Synthesised carries; distance = Σ `gold.action_length` |
| `progressive_carries` | `gold.is_progressive` on a carry |
| `carries_into_final_third` / `_penalty_area` | Start outside, end inside |
| `take_ons`, `take_ons_won` | `Take On`; won = success |
| `dispossessed` | `Dispossessed` event (lower is better) |
| `touches`, `touches_in_box`, `touches_final_third` | §2 touch set, by location |
| `xt_carry` | Σ xt of carries |

### Defending
| Metric | Definition |
|---|---|
| `tackles`, `tackles_won`, `tackles_missed` | `Tackle`, `Tackle` success, `Attempted Tackle` |
| `tackle_success_rate` | `tackles_won / (tackles + tackles_missed)`. Team grain omits `tackles_missed` and so flatters every side. |
| `dribbled_past` | `Challenge` (see §2) |
| `interceptions`, `clearances`, `blocked_passes`, `ball_recoveries` | One event type each |
| `defensive_actions` | Broad set (§2); `def_actions_final_third` at `x ≥ 70` |
| `aerials_won` / `_lost`, `aerial_win_rate` | `Aerial` by outcome |
| `duels` | **Not a player column.** Team `duels_won` = `tackles_won + aerials_won` (WEB_DATA §14 #5). Don't treat it as independent evidence next to those two. |
| `fouls_committed` / `_won`, `errors`, `offsides_provoked` | As named. `caught_offside` = `Offside Pass` (Opta has no caught-offside event). |

### Goalkeeping (NULL for outfield players)
`saves`, `goals_conceded` (**on-pitch window**, not the match score), `save_pct`,
`xg_faced`, `goals_prevented = xg_faced − goals_conceded`, claims, punches,
sweeper actions, `gk_long_ball_pct`.

### Model values
`xt` (passes + carries), `vaep`, `vaep_offensive`, `vaep_defensive`,
`vaep_actions = COUNT(vaep_value)` (never `COUNT(*)`), `vaep_per_action`.

## 4. Team metrics (`gold.team_match_stats` → `team_season_stats`)

| Metric | Definition |
|---|---|
| `possession_pct` | **Clock-based**: each sequence holds the ball from its start to the next sequence start (either team) or the period end. Pass share is only the sanity check. |
| `field_tilt` | `touches_final_third / (own + opponent final-third touches)` |
| `ppda` | `opponent passes / ppda_def_actions`, both in the PPDA zone. Lower = more pressing (percentile inverted). |
| `high_turnovers` | `Ball recovery` at `x ≥ 70` |
| `defensive_line_height` | Mean `x` of broad defensive actions |
| `pass_share_{def,mid,att}_third` | Share of the team's passes by origin third |
| `counter_attack_sequences`, `high_transition_sequences` | From phase segments. **Zero league-wide means the phase pass didn't run**, not a real value. |
| Game state (`score_state`, `numbers_state`) | `gold.team_game_states`; scoreline and players **before** each event (GOLD_LAYER §4.8) |

## 5. Receptions and sequences

- **Receiver** (`gold.pass_receptions`): Opta records no receiver. The rule is
  the next same-team event by `(json_index, event_id)`, skipping opponent and
  administrative rows. The reception point is the pass end (`rx_x, rx_y,
  rx_zone_id`). `rx_gap_m` lets you drop the 1.9% where the first action is
  > 5 m away. `spell_end` says what the receiver did next (pass_completed,
  pass_failed, miscontrol, shot, foul_won, take_on_failed, dispossessed,
  other). Flags available: `is_progressive`, `is_cross`, `is_through_ball`,
  `is_long_ball`, `rx_in_box`.
- **Progressive reception** = a row of `gold.pass_receptions` with
  `is_progressive`. No separate column is needed.
- **Sequence** = one team's possession (`sequences.py` classifier). Rules and
  traps are in the `phases-of-play` skill and `md/GOLD_SEQUENCES.md`.
  `sequence_number` is per **team**. `sequence_id` renumbers on every rebuild,
  so quote kick-off time instead. Style analysis filters `event_count ≥ 3`.

## 6. Model values

| Model | One line | Detail |
|---|---|---|
| **xG** | XGBoost on StatsBomb open data (~82k shots). Open-play model Platt-calibrated, free-kick model raw. Penalties fixed at **0.79**. Own goals never scored. | `src/silver/events/xg.py` |
| **xT** | `end_zone − start_zone` on a fixed 12×8 grid, passes and carries only. **A failed action earns 0** (since 16 Sep 2026). | `src/silver/events/xt.py` |
| **VAEP** | Retrained, calibrated scores/concedes classifiers; `vaep = offensive + defensive`. Set on SPADL-mappable actions only. | `vaep-model` skill. Keepers inflate per-90 VAEP; leaders exclude GK. |

## 7. Normalisation and percentiles

- **Per-90** = `SUM(metric) * 90 / SUM(minutes)`. It's a generated column, so
  averaging per-match rates can't even be expressed. Ratios get **no** per-90.
- **Possession-adjusted (`padj_*`)** = `metric * 50 / opponent_possession_pct`,
  adjusted **per match and then summed**. It exists for tackles, interceptions,
  clearances, recoveries, blocked passes and defensive actions. Rank defenders
  on `padj_*`, never on raw volume.
- **Position group** comes from the Opta **formation slot** through
  `gold.formation_slot_positions`: `GK, CB, RB, LB, MID, W, ST`. Not from
  `match_lineups.position` (substitutes are `SUB`), and not from
  `players.position_raw`. A season's primary group is the minutes-weighted one.
- **Which metrics are ranked for which group** lives in the seed of
  `gold.percentile_metrics` (`sql/ddl/gold_players.sql`, 36 metrics). Lower is
  better for dispossessed, dribbled_past, fouls, errors and fouls_per_tackle.
  A typo in a metric name yields NULL, not an error (gate G12).
- **Two eligibility gates. Know which one you're showing:**
  - gold `player_season_percentiles`: a flat **450 min**, empty for roughly
    the first 13 matchdays.
  - site export: **25% of the club's available minutes**
    (`MIN_MINUTES_SHARE` in `src/export/config.py`). The site's numbers are
    the export's.
  - RB and LB are pooled into `FB` only when either has fewer than 20 eligible
    players. Always ship `peer_n`.

## 8. What Opta does not give us, and known brief conflicts

**Not in the data. Never promise these:**
- **Pressures / pressing events** (StatsBomb has them; Opta doesn't). Nearest
  honest proxies: `ppda` (team), `padj_ball_recoveries_per_90`,
  `def_actions_final_third`, `high_turnovers`.
- **Pass receiver.** Derived (§5).
- **Position changes in a match.** typeId 35 is absent. The slot is the
  starting one, and formation changes aren't reflected.
- **Caught offside** as its own event (use `Offside Pass`).
- **Off-ball runs, tracking data, defensive line from tracking.**

**`README-PLAYER.md` (screen 03) vs the repo.** The brief was rewritten as v2 on
10 Oct 2026 with the resolutions below. The six it marks **(proposed)** in its
"Open decisions" section still need the user's OK. The table records what the
original mock said.

| Brief says | Repo has | Suggested resolution |
|---|---|---|
| Progressive pass "≥10 m upfield, ends in opp. half" | `gold.is_progressive` (25% closer to goal) | Keep the repo rule and change the legend copy |
| Carry "≥5 m under control" | Carries ≥ 1 m; progressive carries use the 25% rule | Map shows progressive carries or all carries; label says which |
| Touch heat on a 12×8 grid | Player heatmap uses the 30 zones (WEB_DATA §9); 12×8 is the xT grid | 30 zones (`ZoneHeatmap`) |
| Defending percentile: **Pressures** | Not in Opta | Swap for `padj_ball_recoveries_per_90` or `def_actions_final_third` |
| Percentile groups "Forwards / Midfielders / Defenders", 900' floor | `GK CB RB LB MID W ST`, 25% export gate | Use the repo groups; heading names the real group |
| Tables `gold.player_events`, `player_touch_grid`, `player_season_metrics`, `player_profile` | Don't exist. The site reads `players/{id}.json` from `src/export` | Extend the export contract (WEB_DATA §9), not gold |
| Bio: age, born, height, weight, foot, nationality, contract, photo | `silver.players` has `date_of_birth` and `height_cm` (both hand-filled, may be NULL), `nationality`, `preferred_foot` (derived). No weight, contract or photo. | Ship what exists. Weight, contract and photo are hand-authored content or omitted. |

## 9. Adding a metric

1. Define it here in one line **first**, using the §2 sets and the §1 helpers.
   If it needs a new geometric rule, add a function to `gold_functions.sql`;
   don't inline a CASE.
2. Count it at **match grain** in `build_*_match_stats.sql`, add the column to
   both match and season DDL (`ALTER … ADD COLUMN IF NOT EXISTS`), and make
   ratios and per-90s generated columns.
3. To rank it, add a `gold.percentile_metrics` row with its groups and direction.
4. To show it, add the key to `src/export/config.py` and the Spanish label to
   `web/lib/labels.ts`.
5. Rebuild gold, run the GOLD_LAYER §9.5 gates, and record a measured value in
   GOLD_LAYER §4.6.
