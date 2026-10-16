/**
 * types.ts — the TypeScript half of the data contract.
 *
 * Every interface here mirrors one file written by `src/export/` in this
 * repository. The full contract, field by field with its gold/silver source,
 * is `md/WEB_DATA.md`; the section number is cited above each block.
 *
 * HAND-WRITTEN FOR NOW. `WEB_DATA.md` §12 specifies `python -m src.export
 * --emit-types`, which would generate this file from the exporters themselves
 * so that a renamed Python field breaks the TypeScript build. That generator
 * needs the exporters to return dataclasses (only `SeasonMeta` does today), so
 * it is still open — `WEB_DATA.md` §14 item 2. Until it lands, a field renamed
 * in Python must be renamed here by hand.
 *
 * Conventions that hold in every file (WEB_DATA.md, "Rules that hold
 * everywhere"):
 *   - `null` means "not applicable or not yet computable", never 0.
 *   - Coordinates are metres on a 105 x 68 pitch, rounded to 0.1.
 *   - Every share is already scaled for display: `possession_pct` is 41.7.
 *   - Dates are `YYYY-MM-DD`; `generated_at` is `YYYY-MM-DDTHH:MM:SSZ`.
 */

/** The envelope every published file carries (§2). */
export interface Envelope {
  schema_version: number;
  generated_at: string;
}

/**
 * The schema version this build of the site understands. A file stamped with
 * anything else means the export and the site are out of step.
 */
export const SCHEMA_VERSION = 1;

/* ==========================================================================
 * manifest.json  (WEB_DATA.md §4)
 * ======================================================================== */

/**
 * One published season of one competition, with its coverage facts.
 * Coverage is DERIVED, never read from `silver.competition_seasons.status`,
 * which stays 'active' forever (§3.5).
 */
export interface SeasonRef {
  /** URL form, `YYYY-YY`. The only season identifier that appears in a path. */
  slug: string;
  /** Human form, `2025/2026`. */
  label: string;
  /** Internal key. Never appears in a URL; kept so a bug can be traced back. */
  competition_season_id: number;
  competition_code: string;
  competition_name: string;
  tier_level: number;
  num_teams: number;
  matchdays_scheduled: number;
  /**
   * Greatest n such that matchdays 1..n are ALL loaded. Not max(matchday):
   * 2026/27 has an early MD6 fixture loaded while only 1-3 are complete.
   */
  matchdays_complete: number;
  /** Highest matchday with any match loaded. */
  last_matchday_loaded: number;
  /** Every club in the league has played every scheduled matchday. */
  league_complete: boolean;
  first_match_date: string | null;
  through_match_date: string | null;
}

/** A published club's coverage within one season. */
export interface TeamSeasonRef {
  slug: string;
  matches_played: number;
  /**
   * THIS club has finished its season — not the same as `league_complete`,
   * since another side may still owe a postponed match.
   */
  is_final: boolean;
}

/** A published club. Adding one is a single entry in `EXPORT_TEAMS`. */
export interface TeamRef {
  slug: string;
  team_id: number;
  name: string;
  short_name: string;
  abbreviation: string;
  city: string | null;
  /** Newest season first, matching `Manifest.seasons`. */
  seasons: TeamSeasonRef[];
}

export interface Manifest extends Envelope {
  source: {
    provider: string;
    /** A key, not prose. The methodology page renders the sentence. */
    metrics_note: string;
  };
  /** What `/` opens: the newest season of the first published club. */
  default: { team: string; season: string };
  /** Newest first. */
  seasons: SeasonRef[];
  teams: TeamRef[];
}

/* ==========================================================================
 * league/{season}/table.json  (WEB_DATA.md §5)
 * ======================================================================== */

/**
 * One club's row in the league table. Zone markers (1-4, 5, 18-20) are
 * presentation and are deliberately not in the data.
 */
export interface LeagueTableRow {
  position: number;
  team_id: number;
  /** Non-null only for published clubs, i.e. the ones with pages to link to. */
  slug: string | null;
  name: string;
  short_name: string;
  abbr: string;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  goals_for: number;
  goals_against: number;
  goal_difference: number;
  xg_for: number;
  xg_against: number;
  xg_difference: number;
  points: number;
}

/** Which of the four team-profile cards a metric belongs to. */
export type ProfileCard =
  | "defensive"
  | "possession"
  | "progression"
  | "finishing";

/** Descriptor for one of the 24 team-profile metrics (§5.1). */
export interface ProfileMetric {
  key: string;
  card: ProfileCard;
  /**
   * `count` metrics are divided by matches played before comparison
   * (WEB_PLAN.md §9.5); `rate` metrics are already normalised.
   */
  kind: "count" | "rate";
  scale: "raw" | "pct";
  decimals: number;
  /** True where LOWER is better (fouls): the percentile is inverted. */
  invert: boolean;
}

/**
 * A metric's value and its percentile among the clubs of that season.
 * `p` is null when the clubs show no variance (the export's guard).
 */
export interface ProfileValue {
  v: number | null;
  p: number | null;
}

export interface LeagueTable extends Envelope {
  season: string;
  competition_name: string;
  matchdays_scheduled: number;
  league_complete: boolean;
  /** Ordered by `position` ascending. */
  table: LeagueTableRow[];
  profile: {
    metrics: ProfileMetric[];
    /** One entry per club, keyed inside by `ProfileMetric.key`. */
    values: { team_id: number; metrics: Record<string, ProfileValue> }[];
  };
}

/* ==========================================================================
 * teams/{team}/{season}/overview.json  (WEB_DATA.md §6)
 * ======================================================================== */

/** One KPI card of the strip at the top of screen 01. */
export interface OverviewKpi {
  key: string;
  value: number;
  unit: string | null;
  /** Rank among `peer_n` clubs, 1 = best (the export sets the direction). */
  rank: number | null;
  peer_n: number;
  /** The small italic note under the value, when the design has one. */
  secondary: { key: string; value: number } | null;
}

/** One played match, as the form strip and the match list read it. */
export interface OverviewMatch {
  match_id: number;
  matchday: number;
  date: string;
  is_home: boolean;
  opponent_team_id: number;
  opponent_name: string;
  opponent_short_name: string;
  opponent_abbr: string;
  goals_for: number;
  goals_against: number;
  result: "W" | "D" | "L";
  xg_for: number;
  xg_against: number;
  xt_for: number;
  xt_against: number;
}

/** One row of a player-leaders box. Each row links to that player's page. */
export interface LeaderRow {
  player_id: number;
  name: string;
  shirt_number: number | null;
  position_group: string | null;
  value: number;
}

export interface OverviewLeaders {
  key: string;
  unit: string | null;
  /** Sorted descending; row 0 is rendered inverted. */
  rows: LeaderRow[];
}

export interface Overview extends Envelope {
  team: { team_id: number; slug: string; short_name: string; abbr: string };
  season: {
    slug: string;
    label: string;
    matches_played: number;
    matchdays_complete: number;
    matchdays_scheduled: number;
    is_final: boolean;
  };
  record: {
    points: number;
    points_per_match: number;
    won: number;
    drawn: number;
    lost: number;
    goals_for: number;
    goals_against: number;
    league_position: number | null;
    /** e.g. "DWWLL", oldest first. */
    form_last_5: string;
  };
  /** Ordered by matchday ascending. */
  matches: OverviewMatch[];
  /**
   * The two rolling charts: a 5-match trailing mean, one point per matchday
   * from MD5 onwards, so `points` is shorter than `matches`.
   */
  rolling: {
    window: number;
    points: {
      matchday: number;
      xg_difference: number;
      xt_difference: number;
    }[];
  };
  kpis: OverviewKpi[];
  leaders: OverviewLeaders[];
}

/* ==========================================================================
 * teams/{team}/{season}/matches/{match_id}.json  (WEB_DATA.md §7)
 *
 * The largest file the site reads, and the only one built from silver.events.
 * Eight blocks, one key each. Two conventions carry the risk, both settled by
 * the export before the site sees the numbers (§3.1):
 *
 *   - Coordinates are FINAL. Both sides are stored attacking towards x = 105,
 *     and the export has already mirrored the opponent in `shots` — the one
 *     both-team figure. No component flips anything.
 *   - The two sides are named `home` and `away`, and `match.team_side` says
 *     which of them is the published club, so no page has to work it out from
 *     a team id.
 * ======================================================================== */

/** One club's side of the match header. */
export interface MatchSide {
  team_id: number;
  short_name: string;
  abbr: string;
  goals: number;
  goals_ht: number | null;
  xg: number;
}

export interface MatchHeader {
  match_id: number;
  matchday: number;
  /** Calendar date, `YYYY-MM-DD`. */
  date: string;
  competition_name: string;
  season: string;
  venue: string | null;
  /** Includes stoppage: 97 on a long match. The momentum axis runs to it. */
  match_length_min: number;
  /** Which of the two sides is the club this site publishes. */
  team_side: "home" | "away";
  home: MatchSide;
  away: MatchSide;
}

/**
 * xT per (period, minute), both sides positive. The site draws `away`
 * downwards. Bins arrive in PLAYING order — every first-half minute, then
 * every second-half minute — and the chart places them by position: the clock
 * restarts at 45 for the second half, so "minute 46" exists twice in a match
 * with stoppage time and the minute alone cannot be an x coordinate.
 */
export interface MomentumBinData {
  period: 1 | 2;
  minute: number;
  home: number;
  away: number;
}

/** A vertical rule on the momentum chart. `label_key` is translated here. */
export interface MomentumMarkerData {
  period: 1 | 2;
  minute: number;
  type: "goal" | "sub" | "card" | "period";
  /** Null on half time, which belongs to neither side. */
  side: "home" | "away" | null;
  label_key: string;
  player_id: number | null;
  name: string | null;
}

export interface MatchNetwork {
  /** Published team only — the design draws one network. */
  side: "home" | "away";
  min_combinations: number;
  nodes: {
    player_id: number;
    surname: string;
    shirt_number: number | null;
    x: number;
    y: number;
    touches: number;
  }[];
  edges: { from: number; to: number; passes: number }[];
}

/** One of the fourteen stat rows, in the design's order (§7.2). */
export interface MatchTotal {
  key: string;
  home: number | null;
  away: number | null;
}

/** One shot, both teams, already in the published team's frame. */
export interface MatchShot {
  shot_id: number;
  side: "home" | "away";
  player_id: number | null;
  name: string;
  minute: number;
  x: number;
  y: number;
  xg: number;
  body_part: string;
  outcome: string;
  play_pattern: string | null;
  first_time: boolean | null;
  big_chance: boolean;
  /** Opta's goalmouth frame (posts 45.2/54.8, crossbar 38). NOT mirrored. */
  goal_mouth_y: number | null;
  goal_mouth_z: number | null;
}

/** The 12 x 8 surface of `xt.py`, non-zero cells only (§3.3). */
export interface MatchXtGrid {
  side: "home" | "away";
  cols: number;
  rows: number;
  cells: { cx: number; cy: number; xt: number }[];
}

/**
 * The same xT sum on the 30 zones of `gold.pitch_zones`, non-zero zones only.
 * This is what the page draws; `xt_grid` stays in the file for a reader who
 * wants the model's own grid.
 */
export interface MatchXtZones {
  side: "home" | "away";
  cells: { zone_id: number; value: number }[];
}

export interface MatchProgression {
  kind: "pass" | "carry";
  x: number;
  y: number;
  end_x: number;
  end_y: number;
  xt: number | null;
  completed: boolean;
}

export interface MatchDefence {
  /** Average defensive line, in the attacking frame: low means deep. */
  line_x: number | null;
  actions: {
    type: string;
    x: number;
    y: number;
    outcome: "won" | "lost";
    minute: number;
    player: string | null;
  }[];
}

/** One action of a chain, for the detailed sequence view (§7.3). */
export interface MatchSequenceAction {
  kind: "pass" | "cross" | "carry" | "take_on" | "shot" | "clearance" | "other";
  /** `silver.events.event_type` as a snake_case key, e.g. `ball_recovery`. */
  type: string;
  /** Match clock of the action (20 Sep 2026). */
  minute: number | null;
  second: number | null;
  x: number;
  y: number;
  /**
   * Null on a point event: a take-on, anything with no destination. On a shot
   * (20 Sep 2026) it is the goal line at the goalmouth y, in metres.
   */
  end_x: number | null;
  end_y: number | null;
  player_id: number | null;
  surname: string | null;
  shirt_number: number | null;
  outcome: "success" | "fail";
  /** Only present on a shot. */
  xg?: number | null;
}

/**
 * One possession sequence of the published team with 3+ events, best xT first.
 * `rank` is 1..12 on the twelve the design draws and null on the rest: the
 * page takes the first twelve as they come and never re-sorts.
 */
export interface MatchSequence {
  sequence_id: string;
  rank: number | null;
  period: number;
  minute: number;
  second: number;
  duration_s: number;
  events: number;
  passes: number;
  xt: number | null;
  xg: number | null;
  vaep: number | null;
  /** Zone id of `gold.pitch_zones` (30-zone grid), not the 12 x 8 grid. */
  start_zone: number | null;
  start_trigger: string | null;
  /**
   * `start_trigger` with a `pass` start refined into its restart — `throw_in`,
   * `free_kick`, `goal_kick`, `corner`, `kick_off` (20 Sep 2026, §7.3).
   */
  start_kind: string | null;
  outcome: string | null;
  /** How a `turnover` lost the ball, from the last action; null otherwise. */
  end_action: string | null;
  primary_phase: string | null;
  final_third_entry: boolean;
  penalty_box_entry: boolean;
  ends_in_shot: boolean;
  /** Simplified trace: start, end and up to 6 waypoints. */
  points: [number, number][];
  /** Every event of the chain, for the detailed view. */
  actions: MatchSequenceAction[];
}

/** One of the four player boxes, both teams, six rows each. */
export interface MatchPlayerBox {
  key: string;
  rows: {
    player_id: number;
    name: string;
    side: "home" | "away";
    abbr: string;
    value: number;
    /** Only on the xG + xA box, which has to name its two parts. */
    components?: Record<string, number | null>;
  }[];
}

export interface MatchFile extends Envelope {
  match: MatchHeader;
  momentum: { bins: MomentumBinData[]; markers: MomentumMarkerData[] };
  network: MatchNetwork;
  totals: MatchTotal[];
  shots: MatchShot[];
  xt_grid: MatchXtGrid;
  xt_zones: MatchXtZones;
  progression: MatchProgression[];
  defence: MatchDefence;
  sequences: MatchSequence[];
  players: MatchPlayerBox[];
}

/* ==========================================================================
 * teams/{team}/{season}/squad.json  (WEB_DATA.md §8)
 *
 * Every player who has played for the club this season, in tab-strip order:
 * GK -> CB -> RB -> LB -> MID -> AM -> W -> ST, then minutes. One player page
 * is built per row.
 * ======================================================================== */

/** The two lines of the name block: Opta `known_name` when set. */
export interface PlayerName {
  /** Given name line; empty for a one-word known name. */
  first: string;
  /** Surname line, the H1 and the tab pill. */
  last: string;
  full: string;
}

export interface SquadPlayer {
  player_id: number;
  name: PlayerName;
  shirt_number: number | null;
  /** Primary formation-slot position: GK CB RB LB DM CM AM RW LW ST. */
  position: string | null;
  position_group: string | null;
  /** Percentile role: GK CB RB LB MID AM W ST. */
  role: string | null;
  matches: number;
  starts: number;
  minutes: number;
}

export interface Squad extends Envelope {
  team: string;
  season: string;
  /** The §3.6 percentile gate for this club so far, in minutes. */
  min_minutes: number;
  players: SquadPlayer[];
}

/* ==========================================================================
 * teams/{team}/{season}/players/{player_id}.json  (WEB_DATA.md §9,
 * README-PLAYER "Data contract")
 *
 * Screen 03. Every coordinate is the player's own event in his team's
 * attacking frame: drawn as shipped, low y at the bottom.
 * ======================================================================== */

export interface PlayerBio {
  player_id: number;
  name: PlayerName;
  shirt_number: number | null;
  nationality: string | null;
  /** `left | right | both`, from event foot qualifiers. */
  preferred_foot: string | null;
  height_cm: number | null;
  date_of_birth: string | null;
  /** On the season's last match date; null without a date of birth. */
  age: number | null;
  position: string | null;
  position_group: string | null;
  role: string;
  /** Primary position under 60 % of minutes (GOLD_LAYER §4.6.2). */
  is_utility: boolean;
  /** Share of minutes in the primary position, display-scaled (62). */
  primary_position_pct: number | null;
  matches: number;
  starts: number;
  minutes: number;
  subbed_off: number;
  yellow_cards: number;
  red_cards: number;
  penalties_taken: number;
  penalties_scored: number;
}

/** One of the four headline KPIs of the profile card, in display order. */
export interface PlayerKpi {
  key: string;
  /** Display-scaled: a pct arrives as 64.2. */
  value: number | null;
  scale: "raw" | "pct";
  decimals: 0 | 1 | 2 | 3;
  /** Season totals only: the per-90 sub line. */
  per_90: number | null;
  /** Rates and per-90s only: rank in the squad among players past the gate. */
  squad_rank: number | null;
  squad_n: number | null;
}

/**
 * A compact pass: `[x, y, end_x, end_y, flags]`. Flag bits: 1 completed,
 * 2 progressive, 4 key pass, 8 assist (`PASS_FLAG`).
 */
export type PassRow = [number, number, number, number, number];
export const PASS_FLAG = { completed: 1, progressive: 2, keyPass: 4, assist: 8 } as const;

/** A compact progressive carry: `[x, y, end_x, end_y, completed 0|1]`. */
export type CarryRow = [number, number, number, number, number];

/** A progressive reception point: `[x, y, in_box 0|1]`. */
export type ReceptionRow = [number, number, number];

export interface ZoneCount {
  zone_id: number;
  count: number;
  /** Share of the player's total, display-scaled. */
  share: number | null;
}

/** The match file's `MatchShot` without `side` and `name`. */
export interface PlayerShot {
  shot_id: number;
  match_id: number;
  matchday: number;
  minute: number;
  x: number;
  y: number;
  xg: number;
  body_part: string;
  outcome: string;
  play_pattern: string | null;
  first_time: boolean | null;
  big_chance: boolean;
  goal_mouth_y: number | null;
  goal_mouth_z: number | null;
}

export interface PlayerDefensiveAction {
  type: string;
  x: number;
  y: number;
  outcome: "won" | "lost";
  minute: number;
  matchday: number;
}

export interface PlayerMaps {
  /** Totals behind every capped list ("150 DE 1,284 PASES"). */
  counts: {
    passes: number;
    progressive_passes: number;
    key_passes: number;
    assists: number;
    progressive_carries: number;
    touches: number;
    receptions: number;
  };
  /** The most recent passes, capped (PLAYER_PASS_MAP_MAX). */
  passes: PassRow[];
  /** Progressive passes, best xT first, capped (PLAYER_PROGRESSIVE_MAX). */
  progressive_passes: PassRow[];
  /** Every key pass and assist. */
  key_passes: PassRow[];
  /** Progressive carries, best xT first, capped. */
  carries: CarryRow[];
  touch_zones: ZoneCount[];
  reception_zones: ZoneCount[];
  /** Progressive receptions within 5 m of the pass end. */
  progressive_receptions: ReceptionRow[];
  shots: PlayerShot[];
  defence: PlayerDefensiveAction[];
}

export interface PercentileMetric {
  key: string;
  value: number | null;
  /** Null = shown without a bar: not ranked for this role, or under its floor. */
  percentile: number | null;
  /** Direction is already applied to `percentile`; this only marks the row. */
  higher_is_better: boolean;
  scale: "raw" | "pct";
  decimals: 0 | 1 | 2 | 3;
  /** "value / out_of" rows (penalties saved of faced). */
  out_of?: number | null;
}

export interface PlayerPercentiles {
  eligible: boolean;
  role: string;
  /** The role, or the pooled set it ranks in (FB, MID). */
  peer_group: string;
  peer_n: number | null;
  pooled_fb: boolean;
  pooled_am: boolean;
  min_minutes: number;
  /** Empty when not eligible. */
  cards: { key: string; metrics: PercentileMetric[] }[];
}

/** One chain of the top-10 lists (README-PLAYER §5). */
export interface PlayerSequence {
  /** `start_event_id`: stable across gold rebuilds, unlike sequence_id. */
  id: number;
  match_id: number;
  matchday: number;
  opponent_abbr: string;
  is_home: boolean;
  period: number;
  minute: number;
  second: number;
  /** The match page's row title key (`sequenceKind`). */
  kind: string;
  outcome: string;
  events: number;
  passes: number;
  player_touches: number;
  xt: number | null;
  vaep: number | null;
  ends_in_shot: boolean;
  points: [number, number][];
  /** Index into `points` of the player's first touch; the gold dot. */
  player_touch_index: number | null;
}

export interface PlayerSequences {
  /** Over every chain with the player and `min_events`+ events. */
  aggregates: {
    sequences: number;
    xt: number | null;
    ends_in_shot: number;
    passes_per_sequence: number | null;
  };
  min_events: number;
  top_xt: PlayerSequence[];
  top_vaep: PlayerSequence[];
}

export interface PlayerFile extends Envelope {
  team: { slug: string; team_id: number; short_name: string; abbr: string };
  season: {
    slug: string;
    label: string;
    competition_name: string;
    /** The club's last matchday played: "HASTA J7". */
    through_matchday: number | null;
  };
  player: PlayerBio;
  kpis: PlayerKpi[];
  maps: PlayerMaps;
  /** Null only for a player with no role (never happens for a squad row). */
  percentiles: PlayerPercentiles | null;
  sequences: PlayerSequences;
}
