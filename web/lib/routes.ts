/**
 * routes.ts — every URL the site can produce, in one place.
 *
 * The routes are Spanish and carry the team slug (WEB_PLAN.md §4), so that
 * adding a second club later is adding a slug, not adding a route. Nothing
 * builds a path with string concatenation at the call site; if a route name
 * changes, it changes here and TypeScript finds every caller.
 *
 * `ready: false` on a nav item means the page has not been built yet
 * (WEB_PLAN.md phases 5a/5b). The masthead renders those as inert text rather
 * than links — a static export has no 404 handler worth the name, and a nav
 * that links to nothing is worse than a nav that says "pronto".
 */

export const routes = {
  home: () => "/",
  /** Screen 01 — season overview. Phase 5a. */
  season: (team: string, season: string) => `/${team}/temporada/${season}`,
  /** The plain match list that links to the match pages. Phase 5a. */
  matches: (team: string, season: string) => `/${team}/partidos/${season}`,
  /** Screen 02 — match analysis. Phase 5a. */
  match: (team: string, matchId: number) => `/${team}/partido/${matchId}`,
  /** The sortable squad table. Phase 5b. */
  squad: (team: string, season: string) => `/${team}/plantilla/${season}`,
  /** Screen 03 — player profile. Phase 5b. */
  player: (team: string, playerId: number) => `/${team}/jugador/${playerId}`,
  /** Screen 04 — sequence laboratory. Phase 5b. */
  sequences: (team: string, season: string) => `/${team}/secuencias/${season}`,
  /** Screen 05 — match-group comparison. Phase 5b. */
  compare: (team: string, season: string) => `/${team}/comparar/${season}`,
  /** What the metrics mean, and where the data comes from. Phase 5b. */
  methodology: () => "/metodologia",
} as const;

/** One entry in the masthead nav. */
export interface NavItem {
  key: string;
  /** Spanish label; the copy itself lives in `lib/labels.ts`. */
  label: string;
  /** Built from the current team and season. */
  href: (team: string, season: string) => string;
  /** False until the page exists. Flip it in the PR that adds the page. */
  ready: boolean;
}

export const NAV_ITEMS: NavItem[] = [
  { key: "season", label: "Temporada", href: routes.season, ready: true },
  { key: "matches", label: "Partidos", href: routes.matches, ready: true },
  { key: "squad", label: "Plantilla", href: routes.squad, ready: false },
  { key: "sequences", label: "Secuencias", href: routes.sequences, ready: false },
  { key: "compare", label: "Comparar", href: routes.compare, ready: false },
  {
    key: "methodology",
    label: "Metodología",
    href: () => routes.methodology(),
    ready: false,
  },
];
