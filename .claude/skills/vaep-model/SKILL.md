---
name: vaep-model
description: >
  Complete reference for the retrained VAEP model that values every action in
  silver.events: why the previous production model was degenerate, how the two
  classifiers (scores / concedes) were retrained in the vaep-model repo, the exact
  artifact contract (vaep_model.json + metrics.json, including the load-bearing
  Platt calibrator), the 148-feature set and the four semantics that are easy to get
  backwards, the frame convention, the VAEP formula and its four guards, the ten
  acceptance gates with their measured values, and a symptom → cause → fix table for
  when the numbers come out wrong.

  Use this skill whenever working on src/silver/events/vaep.py, _build_features,
  the VAEP backfill, models/vaep/*.json, calibration, or debugging vaep_value /
  vaep_offensive / vaep_defensive in silver.events — and before changing anything
  about the feature set, since any change must be mirrored in the training repo.
---

# VAEP Model Reference

## 0. Thirty-second orientation

`silver.events` carries three VAEP columns (`vaep_value`, `vaep_offensive`, `vaep_defensive`).
They are produced by **two XGBoost binary classifiers** — `scores` and `concedes` — that answer,
for the game state made of the last three actions: *will the acting team score / concede within
the next 10 actions?*

```
vaep = (P_scores − P_scores_prev) + (P_concedes_prev − P_concedes)
        └── offensive ──┘           └──── defensive ────┘
```

**The model in `models/vaep/` was retrained from scratch in August 2026** in a separate repo,
`C:/Users/Usuario/Documents/Football Analytics/vaep-model`, because the previous weights were
degenerate. Everything below is the accumulated knowledge from that project: the contract, the
traps, and the numbers to check against.

| | old (baseline) | **new (shipped)** |
|---|---|---|
| provenance | third-party HuggingFace upload | trained here, HF SPADL stream (5,424 matches), Platt-calibrated on Opta |
| trees / depth | 100 / 3 | 795 + 748 / 6 |
| features | 145, **no location feature used** | 148, 9 positional in the top 15 |
| top-3 gain share (`scores`) | **0.9624** | **0.4009** |
| overall AUC | 0.8150 | 0.7770 |
| **AUC excl. goal actions** | **0.5222 — a coin flip** | **0.7465** |
| `corr(vaep_offensive, xt)` | −0.1942 | +0.2605 |
| `vaep_defensive` sd | 0.001686 | 0.009629 |

The old model had learned *"was this a successful shot?"*, not *"how valuable is this game
state?"*. Note that **overall AUC barely moved** — it cannot tell these two models apart. That is
the single most important habit this project produced:

> **Never report or judge a VAEP AUC without stating its goal-action policy.**
> The number that matters is AUC with goal-scoring actions **excluded**.

---

## 1. The two repos and who owns what

| | training repo (`vaep-model`) | **this repo** (`football-analytics-research`) |
|---|---|---|
| path | `C:/Users/Usuario/Documents/Football Analytics/vaep-model` | `C:/Users/Usuario/OneDrive/football-analytics-research` |
| owns | training data, feature builder, training, evaluation, **the artifact** | ingestion, `silver.events`, **inference**, backfill |
| Postgres | read-only | read/write |

The handoff is two files: `models/vaep/{vaep_model.json, metrics.json}`, plus a `_build_features`
that matches the training-side `features.py` column-for-column.

**Any change to the feature set must happen in both repos in lockstep**, and must be re-verified
with `scripts/parity_check.py` in the training repo (it diffs our builder against the pinned
reference library *and* against this repo's `_build_features`, in both directions). A feature-set
drift between the two ends produces wrong values with **no error**.

The training repo's `PLAN.md` carries numbered findings **F1–F34**; they are referenced below by
number when the detail matters. `VAEP_RETRAINING_v2.md` there is the original spec (§10 is an
amendment block that supersedes §3.2, §4.6, §7.3, §8.1 — read §10 first if you open it at all).

---

## 2. The artifact contract

### `models/vaep/vaep_model.json` (11.2 MB)

```json
{ "scores_booster_b64": "...", "concedes_booster_b64": "...", ... }
```

Base64 of XGBoost native JSON. Same shape as the old bundle, so the loader is unchanged:

```python
XGBClassifier().load_model(bytearray(base64.b64decode(blob)))
```

The envelope was verified at export to reproduce both heads **bit-identically** (`0.0e+00` over
50,000 real Opta rows) through exactly that call.

### `models/vaep/metrics.json` — the machine-readable contract

Five fields are **required and load-bearing**; export refuses to write if any is missing or empty:

| field | shipped value | what it controls |
|---|---|---|
| `config.feature_names` | ordered list of 148 | the column order `_build_features` must emit |
| `config.n_features` | `148` | assertion target |
| `config.frame_convention` | `"ltr"` | whether `_build_features` mirrors `a1`/`a2` — see §4 |
| `config.library` / `library_version` | `silly-kicks` / `4.73.0` | the semantics the features reproduce |
| `config.calibration` | Platt, two floats per head | **applied at inference — see §5** |

Other useful keys: `config.nb_prev_actions: 3`, `config.nr_actions: 10`, `config.xfns` (12
transformer names), `provenance.run: "b3-hf-colsample05"`, `provenance.git_sha`,
`provenance.xgboost_version: "3.4.0"`, and per-head `roc_auc_excl_goals`.

### Fingerprint — is the right model loaded?

```python
import json
m = json.load(open("models/vaep/metrics.json"))
assert m["config"]["n_features"] == 148                      # old bundle was 145
assert m["config"]["frame_convention"] == "ltr"
assert m["config"]["calibration"]["method"] == "platt"       # old bundle had no calibration key
assert round(m["config"]["calibration"]["scores"]["a"], 4) == 1.0650
assert m["provenance"]["run"] == "b3-hf-colsample05"
```

If `n_features` is 145 or `config.calibration` is absent, **the old degenerate bundle is loaded**.
Its archived copy lives in the training repo at `models/baseline/` with pinned SHA-256s
(`scripts/verify_baseline.py` reproduces its signature); it must never be confused with
`models/vaep/`.

---

## 3. The 148 features

```
47 × 3 (a0/a1/a2)  +  4 cross-action  +  3 goalscore  =  148
```

Per action (47): 23 action-type one-hots, 6 result one-hots, 4 bodypart one-hots, 3 time
(`period_id`, `time_seconds`, `time_seconds_overall`), 4 location (`start_x/y`, `end_x/y`),
4 polar (`start_dist_to_goal`, `start_angle_to_goal`, `end_dist_to_goal`, `end_angle_to_goal`),
3 movement (`dx`, `dy`, `movement`).
Cross-action (4): `team_1`, `team_2`, `time_delta_1`, `time_delta_2`.
Goalscore (3): `goalscore_team`, `goalscore_opponent`, `goalscore_diff`.

Column **order** is: transformers in `config.xfns` order; within each per-action transformer,
all of `a0`, then all of `a1`, then all of `a2`. Never hardcode the list — read
`metrics.json → config.feature_names` and build in that order.

### The two windows — opposite directions, easy to confuse

| parameter | direction | value | controls |
|---|---|---|---|
| `nb_prev_actions` | **backward** | 3 | the game state `a0`,`a1`,`a2` → **features** |
| `nr_actions` | **forward** | 10 | how far ahead a goal counts → **labels** |

### Four semantics that are easy to get backwards

These are exactly the bugs that were found and fixed in this repo's `_build_features`. Each one is
silent when wrong.

1. **`team_i` is a SAME-team indicator, not a turnover flag, and both slots compare to `a0`.**
   `team_1 = (a1.team == a0.team)`, `team_2 = (a2.team == a0.team)` — `team_2` is `a2` vs **`a0`**,
   *not* `a2` vs `a1`.
2. **`time_delta_i = a0.time_seconds − a_i.time_seconds`**, also `a0`-relative for both slots, on
   the **within-period** clock.
3. **`bodypart_head/other` is not constant zero.** It is `bodypart_id ∈ {head, other,
   head/other}` — 1 for every header, deliberately *not* mutually exclusive with `bodypart_head`
   and `bodypart_other`. (The `"head\other"` typo that made this a dead branch upstream is fixed in
   both pinned reference libraries; it is only a trap when reimplementing from an older source.)
4. **The game-state window clamps at period boundaries.** Slot `k` is
   `max(i − k, first_row_of_this_period)`. The window is neither zero-padded nor allowed to cross
   into the previous period, so **the first action of a period has `a1 == a2 == a0`**.

### `goalscore` — the three columns added on top of socceraction's default set

Score **before** the action, from the acting team's perspective, cumulative **within a match** on
chronologically ordered actions (`json_index` for Opta):

```python
is_shot  = np.isin(type_id, SHOT_TYPE_IDS)
goals    = is_shot & (result_id == RESULT_ID["success"])
owngoals = is_shot & (result_id == RESULT_ID["owngoal"])      # note the shot gate

is_a = team == team[0]; is_b = ~is_a
goals_a = (goals & is_a) | (owngoals & is_b)                  # own goals cross-attribute
goals_b = (goals & is_b) | (owngoals & is_a)
score_a = np.cumsum(goals_a) - goals_a                        # cumsum() - self = BEFORE
score_b = np.cumsum(goals_b) - goals_b

goalscore_team     = score_a * is_a + score_b * is_b
goalscore_opponent = score_b * is_a + score_a * is_b
goalscore_diff     = goalscore_team - goalscore_opponent
```

Two traps here:

- The reference library's docstring says "after the action"; **the code computes before**
  (`cumsum() - self`), which is the correct non-leaking definition. Follow the code.
- The **shot gate on the own-goal branch is deliberate** and is specific to `goalscore`. It is
  live on our Opta feed (all 31 La Liga own goals are `shot` + `owngoal`), but the *labels*
  own-goal rule has **no** type gate. Do not unify them — they genuinely differ.

No SQL change was needed for `goalscore`: `_BACKFILL_SELECT_MATCH` already selects `team_id`,
`spadl_type_id` and `spadl_result_id` for the whole match.

---

## 4. The coordinate frame is a *declared convention*, not a fix

The invariant, and the only thing that is "correct":

> **Whatever frame the features are built in at training time must be the frame they are built in
> at inference time.** Neither convention is intrinsically right — only the match between the ends.

| convention | meaning | how features are built |
|---|---|---|
| **`ltr`** ← **what we ship** | every action stays in its own acting team's attacking frame (canonical SPADL LTR) | coordinates fed straight through, **no mirroring** |
| `a0_mirrored` | `a1`/`a2` rotated 180° about (105, 68) into `a0`'s frame | `play_left_to_right`-style mirror |

- Opta in `silver.events` is **already `ltr`** — measured, not assumed (the aerial-duel test found
  10,842 of 10,855 simultaneous pairs mirrored and **0** in a shared frame, the exact signature of
  canonical LTR). Under `ltr` the data needs no conversion at all.
- The HF training stream was measured independently and is **also `ltr`**, so no conversion was
  applied at either end.
- `vaep.py` must read `config.frame_convention` from the artifact and configure itself; it must
  **not** hardcode a mirroring decision. If the key is absent it assumes `"ltr"` — do not rely on
  that default matching by luck.
- **Calling a mirror under `ltr` inverts every away-team row.** That is the double-mirror bug the
  reference library removed in its 3.0.0. Under `ltr`, verify the mirror is genuinely *not applied*,
  not merely applied symmetrically.

Consequence worth internalising when reading feature importances: under `ltr`, an opponent `a1`
carries polar features measured to the **opponent's** goal. That is a consistent encoding, not
corruption — `team_1`/`team_2` tell the model which frame each slot is in.

---

## 5. Calibration — the part that is easiest to skip and silently wrong

**The boosters alone fail the calibration gate on Opta.** A per-head Platt correction, fitted on
363 held-out La Liga matches and judged on the 64 matches reserved before any model existed, is
what makes the artifact acceptable. It ships inside `metrics.json → config.calibration`:

```
scores    p' = sigmoid( 1.0650 · logit(p) + 0.3907 )
concedes  p' = sigmoid( 0.7501 · logit(p) − 1.4817 )
```

Apply **per head, after `predict_proba`, before the VAEP formula.** Use this exact
implementation — it is carried in the training repo as `calibrate.CONSUMER_REFERENCE` and asserted
bit-identical to the fitting code through a JSON round-trip:

```python
def _stable_sigmoid(z):
    """1/(1+exp(-z)) without overflowing on large-magnitude z."""
    out = np.empty_like(z, dtype=np.float64)
    pos = z >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-z[pos]))
    ez = np.exp(z[~pos])
    out[~pos] = ez / (1.0 + ez)
    return out


def _apply_calibration(p, cal):
    """metrics.json -> config.calibration, applied to one head's probabilities."""
    method = cal.get("method", "none")
    if method == "none":
        return p
    if method == "platt":
        q = np.clip(np.asarray(p, dtype=np.float64), 1e-12, 1 - 1e-12)
        return _stable_sigmoid(cal["a"] * np.log(q / (1.0 - q)) + cal["b"])
    if method == "isotonic":
        return np.interp(np.asarray(p, dtype=np.float64), cal["x"], cal["y"])
    raise ValueError(f"unknown calibration method {method!r}")
```

Do **not** rewrite this with the naive `1 / (1 + exp(-z))`: it differs in the last bits and
overflows on extreme log-odds.

`{"method": "none"}` is a valid value meaning the identity. A **missing** `config.calibration` is
not valid — treat it as a corrupt artifact.

### Why the details matter

- **Platt, not isotonic, chosen on measured evidence.** Platt cost exactly `0.000000` AUC against
  isotonic's `−0.001703`. Platt is strictly monotone, so the two ranking gates are **bit-identical**
  before and after — that is what makes it safe to bolt onto an already-gated model. Isotonic's 154
  knots tie distinct game states to the same value.
- **The `concedes` slope of 0.75 is load-bearing.** That head was over-*spread*, not merely too
  high, so a scalar multiplier would not have fixed it.
- **Skipping calibration reverts the model to the state that failed gate 9, with no error.**
  The visible symptom is per-match `SUM(vaep_value)` around **−0.7** instead of ≈ **+2.9**.

---

## 6. The formula and its four guards

Both probabilities are from the **acting team's** perspective, so on a possession change the
previous action's `P(scores)` becomes the new team's `P(concedes)` and vice versa.

Four guards (from `socceraction.vaep.formula`, mirrored in `vaep.py`):

| guard | condition | effect |
|---|---|---|
| `toolong` | gap > 10 s from the previous action | both previous probabilities → 0 |
| `prevgoal` | previous action was a goal | both previous probabilities → 0 |
| `shot_penalty` | previous action is a penalty | `prev_scores = 0.792453` |
| corner | previous type ∈ {5, 6} | `prev_scores = 0.046500` |

`toolong` compares **within-period** `time_seconds`, not `time_seconds_overall`. A consequence
worth knowing rather than rediscovering: **the first action of every new period always trips the
guard**, because the clock resets to ~0 and the difference goes large negative. That is
socceraction's behaviour and this repo matches it deliberately.

---

## 7. Inference contract — invariants that must not break

- `calculate_vaep(df)` stays a **stateless pure transform**; no SQL in the transform path.
- Values written **only** where `spadl_type_id IS NOT NULL`.
- **The first action of each match stays NULL** — no predecessor state.
- Events ordered by **`json_index`** within a match, always. Never rely on row order as read.
- Feature column order comes from `metrics.json → config.feature_names`.
- A feature-name mismatch must **raise**, never yield a silent all-NaN column.
- `float32` throughout the matrix.

---

## 8. Integration / re-backfill checklist

1. Copy `models/vaep/{vaep_model.json, metrics.json}` from the training repo.
2. Confirm `vaep.py` reads `config.frame_convention` and that under `ltr` **no mirror runs**.
3. Confirm `_build_features` emits all **148** columns in `config.feature_names` order, including
   the three `goalscore` columns.
4. Confirm `_apply_calibration` runs per head, after `predict_proba`, before the formula.
5. Re-run the training repo's `scripts/parity_check.py` against the **updated** builder at the
   declared convention. This is the gate on the integration itself.
6. **Reset before backfilling** — `_BACKFILL_DISCOVER` only selects `WHERE vaep_value IS NULL`, so
   a naive re-run silently does nothing:

```sql
UPDATE silver.events
   SET vaep_value = NULL, vaep_offensive = NULL, vaep_defensive = NULL
 WHERE vaep_value IS NOT NULL;

SELECT count(vaep_value) FROM silver.events;   -- must be 0
```

7. `python -m src.silver.events.vaep` (427 matches / ~780k events ≈ 2.5 min).
8. Re-run the behavioural gates **against the database**, not a parquet extract.

---

## 9. The ten gates — and what the numbers should look like

Gates 1–3 are importance diagnostics computed from the booster; 4–10 are measured on held-out
data. Shipped values are from `opta-holdout` (64 matches, 116,584 actions, reserved in Phase 2
before any model existed).

| # | gate | threshold | **shipped** | old model |
|---|---|---|---|---|
| 1 | top-3 gain share, `scores` | < 0.50 | **0.4009** | 0.9624 |
| 2 | fraction of features at zero gain | < 0.25 | 0.1622 | 0.51 (74/145) |
| 3 | **positional** features in top 15 | ≥ 3 | **9** | **0** |
| 4 | **`scores` AUC excl. goal actions** | ≥ 0.65 | **0.7465** | **0.5222** |
| 5 | `concedes` AUC excl. goal actions | ≥ 0.60 | 0.8070 | — |
| 6 | `corr(vaep_offensive, xt)` | > 0 | +0.2605 | −0.1942 |
| 7 | failed pass negative for the loser | ≥ 0.60 | 0.8887 | — |
| 8 | interception positive for the gainer | ≥ 0.60 | 0.9556 | 0.310 |
| 9 | calibration: `sum(P)` vs actual | within 10% | 0.0419 | — |
| 10 | goals top the ranking | ≥ 0.0369 (the baseline's own value) | 0.6109 | 0.0369 |

Two definitions that were recovered by reproducing the old model's published figures and are easy
to get wrong:

- **Gate 1 is *average* gain** (`importance_type="gain"`), not `total_gain` — `total_gain` gives
  0.8444 where the published figure is 0.9624. It is only diagnostic for the `scores` head.
- **Gate 3 counts *positional* features only** — it must exclude `dx`/`dy`/`movement`, because
  counting those lets the spatially-blind old model pass.

Distribution sanity checks at the database level:

| statistic | expected |
|---|---|
| per-match `SUM(vaep_value)` | ≈ **+1.8 to +2.9** (spec says ≈1.8; measured +2.923 on Opta) |
| mean `vaep_defensive` | ≈ −0.00015 (not −0.0012) |
| `vaep_defensive` sd | ≈ **0.0096** (not 0.0017) — see the note below |
| goals' rank in the VAEP ranking | at the top |
| `corr(per-match SUM(vaep), goals)` | ≈ **0.76** — see §10, this one is watched |

> **0.0224 is the *pre-calibration* `vaep_defensive` sd** (PLAN.md §5, alongside gate 6 =
> +0.2348 and corr = 0.5286, which are also pre-calibration). The shipped, calibrated model
> reads **0.009629** (mean −0.000151), which is what `silver.events` holds since the
> 11 Aug 2026 backfill. Checking against 0.0224 reads a correct integration as a 2.3×
> failure — the accepted run's numbers live in
> `reports/accepted/b3-hf-colsample05/gates.md`, not in the §5 comparison table.

---

## 10. Things that look broken and are not

Recorded so nobody "fixes" them:

- **`gates.json → all_gates_passed` is `false` on the accepted run.** It ANDs over every evaluation
  set, and the HF fold both skips gates 6–8 and fails gate 9 *by design*. Acceptance is read from
  the decisive set, never from that flag. (F33)
- **Gate 9 fails on the HF fold after calibration** (1.0300 → 1.1603). Deliberate: the calibrator
  was fitted for Opta and the consumer runs on Opta only. The HF *ranking* gates are untouched —
  that is the check distinguishing a level shift from damage. (F30)
- **The first action of every period trips the `toolong` guard.** Clock reset; socceraction's
  behaviour. (§6)
- **Under `ltr`, opponent `a1`/`a2` rows have polar features pointing at the opponent's goal.**
  Consistent encoding, not corruption. (§4)
- **`end_dist_to_goal_a0` carries ~43.6% of the gain.** Tested on 770,169 **non-shot** Opta
  actions: P(scores) falls monotonically with distance, 17.3× near-to-far against the truth's
  21.2×, versus the old model's flat 1.4×. It is geometry, not a shot proxy. (F23)
- **Overall AUC is slightly *worse* than the old model's** (0.7770 vs 0.8150). Expected, and the
  whole point of rule "don't trust overall AUC".
- **`corr(SUM(vaep), goals)` rose to 0.757 after calibration.** Mechanically explained (fixing the
  defensive drift removed a large per-match noise term uncorrelated with goals). But a correlation
  *approaching 1.0* is the goal-detector failure showing through — the old model's was 0.9446 and
  largely tautological. **0.757 is fine; treat a drift toward 0.9+ as a red flag.** (F31)

---

## 11. Troubleshooting — symptom → cause → fix

| symptom | most likely cause | fix |
|---|---|---|
| Backfill runs, reports success, **nothing changes** | `_BACKFILL_DISCOVER` only picks `WHERE vaep_value IS NULL` | run the reset SQL in §8 first |
| Per-match `SUM(vaep_value)` ≈ **−0.7** instead of ≈ +2.9 | **calibration not applied** (or applied to only one head) | §5 — per head, after `predict_proba`, before the formula |
| Probabilities are `nan`/`inf` after calibration | naive sigmoid overflowing on extreme log-odds | use `_stable_sigmoid` from §5 verbatim |
| `vaep_defensive` sd ≈ 0.0017, mean ≈ −0.0012 | the **old degenerate bundle** is loaded | fingerprint the artifact (§2) |
| `ValueError` on feature names at build time | feature-set drift between the repos (145 vs 148, or ordering) | rebuild from `config.feature_names`; re-run `parity_check.py` |
| An all-NaN feature column, no error | a name mismatch being swallowed | the mismatch must **raise** — restore that behaviour |
| Every away-team row's geometry looks inverted | a mirror applied under `frame_convention: ltr` (double mirror) | §4 — the mirror is a no-op under `ltr`; verify it does not run |
| AUC looks healthy but the model is obviously wrong | AUC measured **including** goal actions | recompute excluding goal actions (gate 4) |
| `goalscore_*` correlates suspiciously well with the label | `cumsum()` without `- self` → the action's own goal leaks in | `cumsum() - self`, per match, on ordered actions |
| `goalscore` drifts across matches | computed globally instead of per match | reset per `match_id` |
| `team_2` / `time_delta_2` look wrong | compared against `a1` instead of `a0` | both slots are **`a0`-relative** (§3) |
| First action of a period has odd `a1`/`a2` | window crossing the period boundary, or zero-padded | clamp: `a1 == a2 == a0` at a period's first action |
| `bodypart_head/other` is all zeros | hardcoded to 0 (an old upstream typo) | `bodypart_id ∈ {head, other, head/other}` |
| VAEP values present on non-SPADL rows | the `spadl_type_id IS NOT NULL` filter dropped | restore it; also keep the first action of each match NULL |
| Numbers shift after reordering events | ordering by timestamp instead of `json_index` | `json_index` is the true chronological order for Opta |
| Reference-library parity suddenly fails | `silly-kicks` version drift (it changed feature semantics once already) | pin `silly-kicks==4.73.0`; a bump is a feature-set change → re-run parity and re-record `library_version` |
| Shots almost all marked successful | the pre-August-2026 `spadl.py` shot-result bug in an old extract | verify: Attempt Saved 5,339 / Miss 3,999 / Post 175 → `result_id = 0`; Goal 1,109 → 1; Goal + Q28 31 → 3 |

---

## 12. Rules carried over from training (do not relearn these the hard way)

1. **Never train on `vaep_value` / `offensive_value` / `defensive_value`.** The HF dataset is the
   old model's *output* table. Fitting those columns distils the degenerate model with no warning.
2. **Do not trust overall AUC.** Gate 4 is the number that matters. Label every AUC with its
   goal-action policy.
3. **Do not use `scale_pos_weight`.** VAEP is a *difference of two probabilities*: the binding
   requirement is calibration, not ranking. It improves AUC and destroys calibration.
4. **Do not drop goal actions or alter the label.** VAEP needs `P(scores) ≈ 1` on a goal-scoring
   shot. The fix for a shortcut is capacity plus honest evaluation, never label surgery.
5. **Split by whole match.** Consecutive actions in a possession share a label; a random row split
   leaks.
6. **Keep `nb_prev_actions=3` and `nr_actions=10`.** Opposite directions; each extra previous
   action adds 47 columns.
7. **Own goals cross-attribute in the labels** — an own goal by the opponent counts toward *my*
   `scores`. (Note the labels rule has **no** shot-type gate, unlike `goalscore`; the reference
   library we follow gates only the latter. All 505 own goals in the HF stream are `bad_touch`-coded,
   so a shot gate there would have found zero.)
8. **Declare the frame convention; never hardcode it.**
9. **Any feature-set change requires the parity test** against the pinned library at the declared
   convention *and* against this repo's `_build_features`.
10. **Sample size is counted in goals, not rows.** 9,094 `scores` positives on La Liga alone are
    only ~1,140 *independent* observations — each goal smears across the ~8 preceding actions of the
    same possession.

---

## 13. If the model needs to be retrained or recalibrated

Work happens in the training repo, not here. Its pipeline is seven ordered, resumable scripts:

```bash
python scripts/01_download_hf.py        # HF SPADL stream → data/raw/hf/
python scripts/02_build_actions.py      # normalise → canonical actions (+ Opta eval extract)
python scripts/03_build_features.py     # per-match X + y → parquet shards
python scripts/04_train.py              # two XGBClassifier fits, grouped CV
python scripts/05_evaluate.py           # the ten gates → reports/<run>/
python scripts/06_calibrate.py          # per-head Platt on held-out Opta
python scripts/05_evaluate.py --calibrator   # re-gate; verdict moves to opta-holdout
python scripts/07_export.py             # base64 envelope + metrics.json
python scripts/parity_check.py          # vs silly-kicks AND vs this repo's _build_features
```

Two hard-won hyperparameter facts, both measured rather than argued:

- **Capacity was not the binding constraint.** `max_depth` 6 → 8 made gate 1 *worse*
  (0.5563 → 0.5623) and early stopping fired at 754/2000 trees. (F21)
- **`colsample_bytree` 0.8 → 0.5 was the lever** that cleared gate 1 (→ 0.4009), at a validation
  cost of +0.000015 logloss, while *improving* gate 3 from 5 to 9 positional features. (F22)

**When more Opta data arrives, re-measure the calibration rather than assuming it holds.** The
margin is honest but not generous: the holdout has only **368** `concedes` positives, giving a
sampling SE of ~5.2% on the ratio, so `concedes` passing at 7.0% error against a 10% bar is within
one standard error of failing. The two halves of Opta already disagreed by 4–7% on that statistic
*before* any calibration. (F29)

The planned next step (6B-full) is a two-stage fit: pretrain on HF, then fine-tune on Opta from the
pretrained booster at a reduced learning rate with Opta-only early stopping — strictly better than
either dataset alone, once Segunda División and earlier La Liga seasons are ingested (~6,000 goals
is the "both heads credible" threshold).
