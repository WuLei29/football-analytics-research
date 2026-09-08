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
  /** Rank among `peer_n` clubs, 1 = best (PPDA is ranked ascending). */
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
