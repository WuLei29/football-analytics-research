/**
 * labels.ts — every string a reader sees, in Spanish, in one file.
 *
 * The site is Spanish only (WEB_PLAN.md decision 4). The design prototype is
 * written in English; its copy is translated here as each block is built. The
 * point of the single file is not translation for its own sake — it is that a
 * second language later costs one more file, not a sweep of every component.
 *
 * Rule: a component never contains a Spanish sentence. It imports one from
 * here. (Numbers go through `lib/format.ts` for the same reason.)
 */

/* --------------------------------------------------------------------------
 * The site itself
 * ------------------------------------------------------------------------ */

export const site = {
  /** Used in <title>, the masthead, and the Open Graph card. */
  name: "Espanyol Analytics",
  /** The one-line description under the title and in page metadata. */
  tagline: "La temporada del RCD Espanyol, medida con datos de eventos.",
  /** Shown on the hero, above the H1. */
  eyebrow: "RCD Espanyol · Analítica",
  /** Landing page H1. The second half renders in blue. */
  homeTitle: { plain: "El Espanyol, ", accent: "en datos" },
  homeDek:
    "Un proyecto personal de análisis: cada pase, tiro y secuencia de la " +
    "temporada, procesados desde datos de eventos y convertidos en xG, xT, " +
    "VAEP y fases de juego.",
} as const;

/* --------------------------------------------------------------------------
 * The landing page
 * ------------------------------------------------------------------------ */

export const home = {
  coverageHeading: "Estado de los datos",
  coverageDek:
    "Lo que hay cargado ahora mismo. Se actualiza tras cada jornada, " +
    "ejecutando el pipeline y publicando de nuevo.",
  pagesHeading: "Secciones",
  pagesDek: "Las páginas del sitio, y cuáles están construidas.",
  ready: "Disponible",
  pending: "En construcción",
  seasonFinal: "Temporada completa",
  seasonRunning: "En curso",
  matchdaysComplete: (complete: number, scheduled: number) =>
    `Jornadas completas: ${complete} de ${scheduled}`,
  matchesPlayed: (n: number) => (n === 1 ? "1 partido" : `${n} partidos`),
  leagueComplete: "Liga completa",
  leagueRunning: "Liga en curso",
  throughDate: (date: string) => `Datos hasta el ${date}`,
} as const;

/* --------------------------------------------------------------------------
 * Provenance. Shown in the footer and expanded on the methodology page.
 * WEB_PLAN.md §6: the source is named, the derivations are the author's.
 * ------------------------------------------------------------------------ */

export const provenance = {
  /** Keyed by `manifest.source.metrics_note`. */
  metricsNote: {
    derived_by_author:
      "Todas las métricas (xG, xT, VAEP, secuencias y fases) son derivaciones " +
      "propias del autor a partir de los datos de eventos.",
  } as Record<string, string>,
  sourceLine: (provider: string) => `Datos: ${provider}`,
  unaffiliated:
    "Proyecto personal sin relación con el RCD Espanyol ni con LaLiga. " +
    "Sin fines comerciales.",
  buildLine: (stamp: string) => `Datos generados el ${stamp}`,
} as const;

/* --------------------------------------------------------------------------
 * Metric names.
 *
 * The keys are the ones the export writes; see `md/WEB_DATA.md` §5.1 for the
 * 24 team-profile metrics and §6 for the KPI strip. Every key that appears in
 * a data file must appear here, so a metric added to the export without a
 * Spanish name shows up as a missing label rather than as an English one.
 * ------------------------------------------------------------------------ */

/** The five KPI cards at the top of screen 01. */
export const kpiLabels: Record<string, string> = {
  points: "Puntos",
  xg_difference: "Diferencia de xG",
  possession_pct: "Posesión",
  ppda: "PPDA",
  set_piece_goals_for: "Goles a balón parado",
  // secondary values
  points_per_match: "por partido",
  xg_difference_per_match: "por partido",
  set_piece_goal_share: "de los goles",
};

/** The four team-profile cards (WEB_DATA.md §5.1). */
export const profileCardLabels = {
  defensive: "Defensa",
  possession: "Posesión",
  progression: "Progresión",
  finishing: "Finalización",
} as const;

/** The 24 team-profile metrics, in the design's card order. */
export const metricLabels: Record<string, string> = {
  // Defensa
  duels_won: "Duelos ganados",
  aerials_won: "Duelos aéreos ganados",
  aerial_win_rate: "% duelos aéreos",
  tackles_won: "Entradas ganadas",
  tackle_success_rate: "% entradas",
  fouls_committed: "Faltas cometidas",
  // Posesión
  possession_pct: "Posesión",
  pass_share_def_third: "Pases · campo propio",
  pass_share_mid_third: "Pases · zona media",
  pass_share_att_third: "Pases · último tercio",
  touches_in_box: "Toques en el área",
  field_tilt: "Inclinación del campo",
  // Progresión
  progressive_passes: "Pases progresivos",
  progressive_passes_completed: "Pases prog. completados",
  crosses: "Centros",
  progressive_carries: "Conducciones progresivas",
  take_ons: "Regates intentados",
  take_on_success_rate: "% regates",
  // Finalización
  shot_accuracy: "% tiros a puerta",
  npxg_for: "npxG a favor",
  xg_per_shot: "xG por tiro",
  xg_open_play: "xG en juego abierto",
  xg_set_piece: "xG a balón parado",
  xg_fast_break: "xG en contraataque",
};

/** The four player-leader boxes on screen 01. */
export const leaderLabels: Record<string, string> = {
  goals: "Goles",
  xt_per_90: "xT por 90",
  progressive_passes: "Pases progresivos",
  carries_into_final_third: "Conducciones al último tercio",
};

/**
 * Look a label up, and say so loudly when it is missing. Returning the raw key
 * would put `xg_fast_break` on the page and nobody would notice for weeks.
 */
export function label(dictionary: Record<string, string>, key: string): string {
  return dictionary[key] ?? `⟨${key}⟩`;
}
