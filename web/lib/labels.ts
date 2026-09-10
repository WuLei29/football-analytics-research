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

/* --------------------------------------------------------------------------
 * Visualisations (Phase 4).
 *
 * Marks carry no text, but everything around them does: the accessible title
 * of each figure, the legend under it, and the tooltip that follows the
 * cursor. Same rule as everywhere else — a component holds no Spanish.
 * ------------------------------------------------------------------------ */

/** Body part of a shot, from `shots[].body_part` (WEB_DATA §7). */
const bodyPartLabels: Record<string, string> = {
  right_foot: "Pie derecho",
  left_foot: "Pie izquierdo",
  head: "Cabeza",
  other: "Otra",
};

/** Outcome of a shot, from `shots[].outcome`. */
const shotOutcomeLabels: Record<string, string> = {
  goal: "Gol",
  saved: "Parada",
  off_target: "Fuera",
  woodwork: "Al palo",
  blocked: "Bloqueado",
};

export function shotBodyPart(key: string): string {
  return label(bodyPartLabels, key);
}

export function shotOutcome(key: string): string {
  return label(shotOutcomeLabels, key);
}

/**
 * Type of a defensive action, from `defence.actions[].type` (WEB_DATA §7).
 * The keys are the six Opta event types the export selects.
 */
const defensiveTypeLabels: Record<string, string> = {
  tackle: "Entrada",
  interception: "Intercepción",
  ball_recovery: "Recuperación",
  challenge: "Duelo",
  aerial: "Duelo aéreo",
  blocked_pass: "Pase bloqueado",
  block: "Bloqueo",
  clearance: "Despeje",
  foul: "Falta",
};

export function defensiveActionType(key: string): string {
  return label(defensiveTypeLabels, key);
}

/**
 * `gold.sequences.primary_phase` — the eleven phases of the `phases-of-play`
 * vocabulary plus `chaotic`, which is the classifier saying "none matched"
 * (GOLD_LAYER §4.1.6). Keys are the column's own values, so the export ships
 * the key and the translation happens here, once.
 */
const sequencePhaseLabels: Record<string, string> = {
  buildup: "Salida de balón",
  fast_buildup: "Salida rápida",
  midblock: "Zona media",
  fast_midblock: "Zona media rápida",
  attacking: "Ataque posicional",
  fast_attacking: "Ataque rápido",
  set_piece: "Balón parado",
  counter_attack: "Contraataque",
  high_transition: "Transición alta",
  direct_long: "Juego directo",
  direct_fk_pk: "Falta directa o penalti",
  chaotic: "Jugada caótica",
};

/** `gold.sequences.outcome` — the closed enum of GOLD_LAYER §4.1.3. */
const sequenceOutcomeLabels: Record<string, string> = {
  goal: "Gol",
  shot_saved: "Tiro parado",
  shot_blocked: "Tiro bloqueado",
  shot_off_target: "Tiro fuera",
  shot_woodwork: "Tiro al palo",
  corner_won: "Córner",
  foul_won: "Falta a favor",
  offside: "Fuera de juego",
  ball_out: "Balón fuera",
  keeper_collected: "Atrapa el portero",
  period_end: "Fin del periodo",
  turnover: "Pérdida",
};

export function sequencePhase(key: string): string {
  return label(sequencePhaseLabels, key);
}

export function sequenceOutcome(key: string): string {
  return label(sequenceOutcomeLabels, key);
}

export const viz = {
  /** Read out in place of the drawing by a screen reader. */
  shotMapTitle: "Mapa de tiros del partido",
  pitchTitle: "Campo de fútbol",

  /** Legends. Each is a list of [swatch meaning, text]. */
  legend: {
    /* The three shot fills. The team is named once, in the row's gutter, so
       these stay short — "Gol Espanyol · Tiro a puerta Espanyol · Tiro fuera
       Espanyol" repeats the only word both rows already differ by. */
    shotGoal: "Gol",
    /** Filled at half strength: reached the frame but was not a goal. */
    shotOn: "A puerta",
    /** Hollow: off target, or blocked before the line. */
    shotOff: "Fuera",
    progressivePass: "Pase progresivo",
    carry: "Conducción",
    regain: "Recuperación",
    duelLost: "Duelo perdido",
    low: "Bajo",
    high: "Alto",
    nodeSize: "Tamaño = toques · enlace = 4+ combinaciones",
    selectedChain: "Cadena seleccionada en azul · punto = inicio · anillo = tiro",
    shotArea: "Área ∝ xG · pasa el cursor por un tiro",
    defenceHover: "Pasa el cursor por una acción para ver el minuto y el tipo",

    /* The v1 sequence legend, in its original four terms plus the three marks
       the detailed view also draws (espanyol-viz-design §7 and §8). */
    participants: "Participantes",
    finisher: "Finalización",
    pass: "Pase",
    cross: "Centro",
    takeOn: "Regate",
    shot: "Tiro",
  },

  /** The panel beside the sequence pitch: what `gold.sequences` says about it. */
  sequencePanel: {
    heading: "La jugada",
    phase: "Fase",
    outcome: "Desenlace",
    start: "Inicio",
    duration: "Duración",
    events: "Acciones",
    xt: "xT",
    vaep: "VAEP",
    marks: "Marcas",
  },

  /** The selectable list of sequences beside the pitch. */
  sequenceList: {
    heading: "Jugadas",
    hint: "Selecciona una jugada para verla en el campo.",
    /** Collapsed row summary: "8 acciones · 14.6 s". `n` and `s` are formatted. */
    summary: (actions: string, seconds: string) => `${actions} acciones · ${seconds} s`,
    count: (n: number) => (n === 1 ? "1 jugada" : `${n} jugadas`),
    empty: "No hay jugadas que mostrar.",
  },

  /** Momentum marker labels, built from `momentum.markers[].label_key`. */
  marker: {
    goal: (minute: number) => `GOL ${minute}'`,
    sub: (minute: number) => `CAMBIO ${minute}'`,
    card: (minute: number) => `TARJETA ${minute}'`,
    period: (minute: number) => `DESCANSO ${minute}'`,
  } as Record<string, (minute: number) => string>,

  /** Axis captions for the two rolling charts of screen 01. */
  axis: {
    matchday: "Jornada",
    rollingXgd: "XGD · MEDIA 5",
    rollingXt: "XT · MEDIA 5",
  },
} as const;

/* --------------------------------------------------------------------------
 * The /demo page (Phase 4).
 *
 * A catalogue of the drawing primitives with invented data. Not linked from
 * the nav; it exists so the pieces can be checked in isolation before screens
 * 01 and 02 assemble them.
 * ------------------------------------------------------------------------ */

export const demo = {
  eyebrow: "04 · PRIMITIVAS",
  title: { plain: "Las piezas, ", accent: "por separado" },
  dek:
    "Cada bloque gráfico del sitio, dibujado con datos inventados. Sirve para " +
    "revisar geometría, colores y escalas sin depender del exportador.",
  warning:
    "Los números de esta página son ficticios y deterministas: no vienen de la " +
    "base de datos.",
  blocks: {
    pitchH: "Campo horizontal",
    pitchHDek: "El ataque va hacia la derecha. La banda derecha queda abajo.",
    pitchV: "Campo vertical",
    pitchVDek: "El ataque va hacia arriba. La banda derecha queda a la derecha.",
    shots: "Mapa de tiros",
    shotsDek: "Los dos equipos. El área del círculo es el xG.",
    xtSurface: "Superficie de xT",
    xtSurfaceDek: "Reagrupada en las 30 zonas de gold.pitch_zones.",
    zones: "Mapa de zonas",
    zonesDek: "Las 30 zonas de gold.pitch_zones. Ejemplo: un lateral derecho.",
    network: "Red de pases",
    networkDek: "Posición media, toques y combinaciones del equipo publicado.",
    progression: "Progresión",
    progressionDek: "Pases progresivos y conducciones; discontinuo = fallado.",
    defence: "Acciones defensivas",
    defenceDek: "Las 34 más altas, más la línea defensiva media.",
    sequences: "Secuencias",
    sequencesDek: "Cadenas de posesión. Haz clic para seleccionar una.",
    sequenceDetail: "Jugadas del partido",
    sequenceDetailDek:
      "Elige una jugada en la lista: pase, centro, conducción, regate y tiro.",
    defLine: (metres: string) => `LÍNEA DEF. ${metres} M`,
    rolling: "Media móvil de 5 partidos",
    rollingDek: "xGD y xT, con la misma altura y escalas distintas.",
    momentum: "Momentum",
    momentumDek: "xT neto por minuto, suavizado con una ventana de 5 minutos.",
  },
} as const;
