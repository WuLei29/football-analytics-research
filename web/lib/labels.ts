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
 * Metric definitions.
 *
 * The two action-value metrics a reader can choose between (the sequence
 * browser today, the lab later). `short` is the one-line answer shown on
 * hover; `long` is the paragraph under it. Wherever a selector offers xT and
 * VAEP it must draw these, so the two are never explained two different ways.
 * ------------------------------------------------------------------------ */

export type SequenceMetric = "xt" | "vaep";

export const metricDefinitions: Record<
  SequenceMetric,
  { abbr: string; name: string; short: string; long: string }
> = {
  xt: {
    abbr: "xT",
    name: "Amenaza esperada",
    short: "Cuánto acerca a la portería la acción ejecutada.",
    long:
      "Cada zona del campo tiene un valor según la probabilidad de marcar " +
      "desde ella. Un pase o una conducción vale la diferencia entre la zona " +
      "de llegada y la de salida. Mide la construcción del juego, no el " +
      "remate: un tiro o una entrada no suman xT.",
  },
  vaep: {
    abbr: "VAEP",
    name: "Valor de la acción",
    short: "Cuánto aumenta la acción la probabilidad de la jugada de acabar en gol.",
    long:
      "Un modelo estima, antes y después de cada acción, la probabilidad de " +
      "marcar y de encajar en las próximas diez. El VAEP es la diferencia. " +
      "Valora tiros, regates y acciones defensivas además de pases, y tiene " +
      "en cuenta el contexto (marcador, minuto, acciones anteriores). Un gol " +
      "vale mucho, así que las secuencias que acaban en gol suben arriba.",
  },
};

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
  goals_for: "Goles a favor",
  set_piece_goals_for: "Goles a balón parado",
  // secondary values
  points_per_match: "por partido",
  goals_per_match: "por partido",
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
  ppda: "PPDA",
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
  vaep_per_90: "VAEP por 90",
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
 * The keys are the seven Opta event types the export selects.
 *
 * `challenge` is Opta's "beaten while trying to tackle" -- the defender's
 * side of a take-on. It reads as "Regate sufrido" rather than "Duelo", which
 * would now collide with `viz.legend.duelLost` and with the aerial chip.
 */
const defensiveTypeLabels: Record<string, string> = {
  tackle: "Entrada",
  /** The player page only: went in and missed (README-PLAYER §3b). */
  attempted_tackle: "Entrada fallida",
  interception: "Intercepción",
  ball_recovery: "Recuperación",
  challenge: "Regate sufrido",
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
 * `sequences[].actions[].type` — `silver.events.event_type` as a snake_case
 * key (WEB_DATA §7.3). Only the types that occur inside a possession chain
 * need a word; anything else falls through `label` as `⟨key⟩`.
 */
const sequenceActionTypeLabels: Record<string, string> = {
  pass: "Pase",
  offside_pass: "Pase en fuera de juego",
  carry: "Conducción",
  take_on: "Regate",
  goal: "Gol",
  attempt_saved: "Tiro a puerta",
  miss: "Tiro fuera",
  post: "Tiro al palo",
  clearance: "Despeje",
  ball_recovery: "Recuperación",
  interception: "Intercepción",
  tackle: "Entrada",
  blocked_pass: "Pase bloqueado",
  claim: "Blocaje",
  keeper_pick_up: "Recogida del portero",
  punch: "Despeje de puños",
  smother: "Salida a los pies",
  keeper_sweeper: "Salida del portero",
  ball_touch: "Toque",
  dispossessed: "Pérdida",
  foul: "Falta",
  aerial: "Duelo aéreo",
  challenge: "Duelo",
  shield_ball_opp: "Protección del balón",
  good_skill: "Recurso técnico",
  corner_awarded: "Córner concedido",
};

export function sequenceActionType(key: string): string {
  return label(sequenceActionTypeLabels, key);
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
  corner_conceded: "Córner en contra",
  foul_committed: "Falta cometida",
  period_end: "Fin del periodo",
  interrupted: "Interrupción",
  retained: "Recuperada al instante",
  turnover: "Pérdida",
};

export function sequencePhase(key: string): string {
  return label(sequencePhaseLabels, key);
}

/**
 * `sequences[].kind` (WEB_DATA §7.6, 21 Sep 2026) — the title of a list row.
 * A priority ladder the export walks over the gold phase flags: how the ball
 * was won, then the restart it came from, then a long ball, then where the
 * possession settled. `primary_phase` (the biggest segment) stays in the file
 * but no longer names the row.
 */
const sequenceKindLabels: Record<string, string> = {
  counter_attack: "Contraataque",
  high_transition: "Transición rápida",
  corner: "Córner",
  throw_in: "Saque de banda directo",
  free_kick: "Falta al área",
  direct_long: "Juego directo",
  positional: "Ataque posicional",
  buildup: "Salida de balón",
  fast: "Ataque rápido",
};

export function sequenceKind(key: string): string {
  return label(sequenceKindLabels, key);
}

export function sequenceOutcome(key: string): string {
  return label(sequenceOutcomeLabels, key);
}

/**
 * `sequences[].end_action` (WEB_DATA §7.3, 20 Sep 2026) — HOW a `turnover`
 * lost the ball, from the chain's last action. Lower-case phrases: they are
 * printed after the outcome, "Pérdida · pase largo fallado".
 */
const sequenceEndActionLabels: Record<string, string> = {
  pass: "pase fallado",
  pass_long: "pase largo fallado",
  pass_cross: "centro fallado",
  pass_through: "pase en profundidad fallado",
  pass_head: "pase de cabeza fallado",
  clearance: "despeje perdido",
  take_on: "regate fallado",
  dispossessed: "robo",
  touch: "mal control",
  carry: "conducción perdida",
  loose_regain: "recuperación sin control",
  other: "acción fallida",
};

export function sequenceEndAction(key: string): string {
  return label(sequenceEndActionLabels, key);
}

/** Outcome and, when the export has one, how: "Pérdida · pase largo fallado". */
export function sequenceEnding(outcome: string, endAction: string | null | undefined): string {
  const what = sequenceOutcome(outcome);
  return endAction ? `${what} · ${sequenceEndAction(endAction)}` : what;
}

export const viz = {
  /** Read out in place of the drawing by a screen reader. */
  shotMapTitle: "Mapa de tiros del partido",
  pitchTitle: "Campo de fútbol",

  /** The last line of a hover card whose mark opens its possession chain. */
  openSequence: "Clic: ver la jugada ↓",

  /** The hover card of one zone of a heat map: "34 toques" / "12 % del total". */
  zoneHover: {
    touches: (n: number) => (n === 1 ? "1 toque" : `${n} toques`),
    receptions: (n: number) => (n === 1 ? "1 recepción" : `${n} recepciones`),
    defence: (n: number) => (n === 1 ? "1 acción defensiva" : `${n} acciones defensivas`),
    share: (pct: string) => `${pct} del total`,
  },

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
    clearance: "Despeje",
    otherAction: "Otra acción · pasa el cursor para leerla",
    regain: "Recuperación",
    duelLost: "Duelo perdido",
    low: "Bajo",
    high: "Alto",
    nodeSize: "Tamaño = toques · enlace = 4+ combinaciones",
    selectedChain: "Cadena seleccionada en azul · punto = inicio · anillo = tiro",
    shotArea: "Área ∝ xG · pasa el cursor por un tiro",
    defenceHover: "Pasa el cursor por una acción para ver el minuto y el tipo",
  defenceFilter: "FILTRAR",
  defenceFilterAll: "Todas",
  /** Chip caption: the type name with how many of them the match holds. */
  defenceChip: (type: string, n: number) => `${type} (${n})`,
  defenceEmpty: "Ningún tipo seleccionado.",

    /* The v1 sequence legend, in its original four terms plus the three marks
       the detailed view also draws (espanyol-viz-design §7 and §8). */
    participants: "Participantes",
    finisher: "Finalización",
    pass: "Pase",
    cross: "Centro",
    takeOn: "Regate",
    shot: "Tiro",
    /* The two marks of the 20 Sep 2026 review: where the chain began, where
       the ball was won. The instruction sits under the legend in italics. */
    sequenceStart: "Inicio de la jugada",
    sequenceRegain: "Balón recuperado",
    sequenceHover: "Pasa el cursor por una acción para leerla.",
    /** The player page's gold node: "Romero". */
    focusPlayer: (surname: string) => surname,
  },

  /** The hover card of one action in the detailed sequence view. */
  sequenceTooltip: {
    /** "Inicio · Saque de banda" — the role, then how the chain began. */
    startedBy: (trigger: string) => `Inicio · ${trigger}`,
    /** "Final · Pérdida · pase largo fallado". */
    endedBy: (ending: string) => `Final · ${ending}`,
  },

  /** The panel beside the sequence pitch: what `gold.sequences` says about it. */
  sequencePanel: {
    heading: "La jugada",
    phase: "Fase",
    outcome: "Desenlace",
    /** The clock. "Inicio" is how the chain began (`trigger`). */
    start: "Minuto",
    trigger: "Inicio",
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
    defenceDek: "Todas las acciones del equipo, más la línea defensiva media.",
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

/* --------------------------------------------------------------------------
 * Screen 01 — season overview (Phase 5a).
 *
 * The prototype's copy is English; this is its translation, block by block
 * (WEB_PLAN.md §9.4). The eyebrow keeps the design's "01 ·" numbering because
 * it is part of the visual system, not a label.
 * ------------------------------------------------------------------------ */

export const season = {
  eyebrow: "01 · TEMPORADA",
  title: { plain: "La temporada, ", accent: "medida" },
  /** The italic dek to the right of the H1. `matches` arrives formatted. */
  dek: (team: string, label: string, matches: string) =>
    `Todo lo que el ${team} ha hecho en ${label}, medido sobre ${matches} ` +
    `partidos de datos de eventos: dónde está, hacia dónde va y qué tipo de ` +
    `equipo es.`,
  dekEmpty: (team: string, label: string) =>
    `El ${team} aún no ha jugado en ${label}. Esta página se rellena con la ` +
    `primera jornada cargada.`,

  /** KPI cards. The rank chip reads "11.º de 20". */
  rank: (rank: number, peers: number) => `${rank}.º de ${peers}`,
  rankEmpty: "sin dato",

  tableHeading: "Clasificación",
  tableCaption: (competition: string) => `${competition} · orden final por puntos`,
  tableCaptionRunning: (competition: string) =>
    `${competition} · clasificación con los datos cargados`,
  /** Column heads of the league table, in the design's order. */
  tableColumns: {
    position: "#",
    club: "Club",
    played: "PJ",
    won: "G",
    drawn: "E",
    lost: "P",
    goalDifference: "DG",
    xgDifference: "DxG",
    points: "Pts",
  },

  formHeading: "Forma · últimos 10",
  formCaption: "Resultado, marcador y xG neto de cada partido",
  formEmpty: "Todavía no hay partidos jugados.",

  rollingHeading: "Tendencia",
  rollingCaption: (window: number) =>
    `Media móvil de ${window} partidos · xGD y xT netos por partido`,
  rollingEmpty: (window: number) =>
    `La media móvil arranca en el partido ${window}: aún no hay suficientes ` +
    `jornadas cargadas.`,

  profileHeading: "Perfil del equipo",
  profileCaption:
    "Valor por partido · la barra es el percentil entre los 20 equipos de la " +
    "categoría, más alto es mejor",
  /** The small caption in each profile card head. */
  profileVs: "VS LA LIGA",
  percentileEmpty: "—",

  leadersHeading: "Jugadores destacados",
  leadersCaption: "Totales de la temporada, de mayor a menor",

  readingEyebrow: "LECTURA DE LA TEMPORADA",
  matchesLinkN: (n: string) => `Ver los ${n} partidos`,
} as const;

/* --------------------------------------------------------------------------
 * The plain match list (`/{equipo}/partidos/{season}`).
 * Not a designed screen: it borrows the form-strip row style and exists so
 * that every match page is reachable (WEB_PLAN.md §4).
 * ------------------------------------------------------------------------ */

export const matchList = {
  eyebrow: "PARTIDOS",
  title: { plain: "Los partidos, ", accent: "uno a uno" },
  dek:
    "Cada partido de la temporada con su marcador y su xG. Abre uno para ver " +
    "el análisis completo.",
  columns: {
    matchday: "J",
    date: "Fecha",
    venue: "",
    opponent: "Rival",
    score: "Resultado",
    xg: "xG",
  },
  home: "Casa",
  away: "Fuera",
  homeShort: "L",
  awayShort: "V",
  empty: "No hay partidos cargados en esta temporada.",
} as const;

/* --------------------------------------------------------------------------
 * Screen 02 — match analysis (Phase 5a).
 * ------------------------------------------------------------------------ */

export const match = {
  eyebrow: "02 · ANÁLISIS DEL PARTIDO",
  /** Right-aligned meta, three mono lines. */
  meta: (competition: string, matchday: number) =>
    `${competition} · Jornada ${matchday}`,
  venueLine: (venue: string | null, date: string) =>
    venue ? `${venue} · ${date}` : date,
  xgLine: (home: string, away: string) => `${home} xG – ${away} xG`,
  halfTime: (home: number, away: number) => `Descanso ${home}–${away}`,

  momentumHeading: "Momentum",
  momentumCaption: (window: number) =>
    `xT por minuto, media móvil de ${window} minutos`,

  networkHeading: "Red de pases",
  networkCaption: (min: number) =>
    `Once inicial · posición media y combinaciones (${min} o más)`,

  totalsHeading: "Datos del partido",
  totalsCaption: "La barra reparte cada fila entre los dos equipos",

  shotsHeading: "Mapa de tiros",
  shotsCaption: "Los dos equipos, cada uno atacando hacia su portería",

  xtHeading: "Superficie de xT",
  xtCaption: (team: string) => `Amenaza generada por el ${team}, por zona`,

  progressionHeading: "Progresión",
  progressionCaption: (n: number) =>
    `Los ${n} pases y conducciones progresivas de más xT`,
  /** The hover card of one arrow: "Pase progresivo · Puado" / "57' · xT 0.124". */
  progressionHover: {
    pass: "Pase progresivo",
    carry: "Conducción progresiva",
    xt: (value: string) => `xT ${value}`,
    failed: "Fallado",
  },
  progressionHint: "Pasa el cursor por una flecha para leerla",

  defenceHeading: "Acciones defensivas",
  defenceCaption: (n: number) =>
    `Las ${n} acciones defensivas del equipo, y la línea media`,
  defenceLine: (metres: string) => `LÍNEA DEF. ${metres} M`,

  /** The block title follows the metric the list is sorted by. */
  sequencesHeading: (metric: SequenceMetric) =>
    `Jugadas por ${metricDefinitions[metric].abbr} (${metricDefinitions[metric].name})`,
  sequencesCaption: (n: number, metric: SequenceMetric) =>
    `Las ${n} secuencias de posesión de tres o más acciones, de más a menos ` +
    `${metric === "xt" ? "amenaza" : "valor"}. Elige una para leerla en detalle.`,
  /** The xT / VAEP toggle beside the title. */
  sequencesSortLabel: "Ordenar por",
  sequencesEmpty: "No hay secuencias de tres o más acciones en este partido.",
  sequenceDetailHeading: "La jugada, en detalle",
  /** A row of the sequence list: "SEC-04 · 23 min". */
  sequenceRow: (rank: number, minute: number) =>
    `SEC-${String(rank).padStart(2, "0")} · ${minute} min`,
  /** Its second line: "6 pases · 14.2 s · 0.184 xT". */
  sequenceSummary: (passes: string, seconds: string, xt: string) =>
    `${passes} pases · ${seconds} s · ${xt} xT`,
  sequenceDescription: (trigger: string, outcome: string) =>
    `${trigger} → ${outcome}`,

  playersHeading: "Destacados del partido",
  playersCaption: "Los dos equipos, seis jugadores por métrica",

  readEyebrow: "LECTURA DEL PARTIDO",
  backToList: "Todos los partidos",
} as const;

/** The match dropdown in the header of screen 02 (`MatchPicker`). */
export const matchPicker = {
  label: "Cambiar de partido",
  /** "J3 · 14 sept · L Valencia 2–1" — one line per match, newest first. */
  option: (
    matchday: number,
    date: string,
    venue: string,
    opponent: string,
    goalsFor: number,
    goalsAgainst: number,
  ) => `J${matchday} · ${date} · ${venue} ${opponent} ${goalsFor}–${goalsAgainst}`,
} as const;

/** The eighteen match totals, in the design order (WEB_DATA.md §7.2). */
export const matchTotalLabels: Record<string, string> = {
  possession_pct: "Posesión",
  xg: "xG",
  shots: "Tiros",
  shots_on_target: "Tiros a puerta",
  big_chances: "Ocasiones claras",
  passes_completed: "Pases completados",
  pass_completion_pct: "% acierto en pases",
  final_third_entries: "Entradas al último tercio",
  crosses_completed: "Centros completados",
  xt_created: "xT generado",
  yellow_cards: "Tarjetas amarillas",
  red_cards: "Tarjetas rojas",
  fouls_committed: "Faltas cometidas",
  duels_won: "Duelos ganados",
  recoveries: "Recuperaciones",
  corners: "Córners",
  xg_open_play: "xG en juego abierto",
  xg_set_piece: "xG a balón parado",
};

/** The four player boxes at the foot of screen 02. */
export const matchPlayerBoxLabels: Record<string, string> = {
  recoveries: "Recuperaciones",
  passes_completed: "Pases completados",
  passes_into_final_third: "Pases al último tercio",
  xg_xa: "xG + xA",
};

/**
 * `gold.sequences.start_trigger` — how a possession began. The keys are the
 * column values, so a trigger the classifier adds later shows up as a missing
 * label rather than in English.
 */
const sequenceTriggerLabels: Record<string, string> = {
  recovery: "Recuperación",
  tackle: "Entrada",
  interception: "Intercepción",
  clearance: "Despeje",
  block: "Bloqueo",
  aerial: "Duelo aéreo",
  pass: "Pase",
  carry: "Conducción",
  take_on: "Regate",
  goal_kick: "Saque de puerta",
  throw_in: "Saque de banda",
  corner: "Córner",
  free_kick: "Falta",
  kick_off: "Saque inicial",
  keeper: "Recogida del portero",
  keeper_pickup: "Atrapada del portero",
  rebound: "Rechace",
  drop_ball: "Balón a tierra",
  penalty: "Penalti",
  ball_touch: "Toque",
  challenge: "Duelo",
  dispossessed: "Pérdida",
  error: "Error",
  offside: "Fuera de juego",
  period_start: "Inicio del periodo",
  other: "Otra acción",
  unknown: "Sin determinar",
};

/**
 * Also the words for `sequences[].start_kind` (WEB_DATA §7.3, 20 Sep 2026):
 * the same trigger vocabulary, with a `pass` start refined by the export into
 * the restart it really is — `throw_in`, `free_kick`, `goal_kick`, `corner`,
 * `kick_off`. Those keys were here before anything produced them.
 */
export function sequenceTrigger(key: string): string {
  return label(sequenceTriggerLabels, key);
}

/* --------------------------------------------------------------------------
 * Screen 03 — player profile (Phase 5b, README-PLAYER.md).
 * ------------------------------------------------------------------------ */

/** Primary formation-slot position (`gold.formation_slot_positions`). */
export const positionLabels: Record<string, string> = {
  GK: "Portero",
  CB: "Central",
  RB: "Lateral derecho",
  LB: "Lateral izquierdo",
  DM: "Mediocentro defensivo",
  CM: "Centrocampista",
  AM: "Mediapunta",
  RW: "Extremo derecho",
  LW: "Extremo izquierdo",
  ST: "Delantero",
};

/**
 * The percentile heading names the PEER group, plural: a pooled mediapunta
 * reads "vs centrocampistas", a pooled full back "vs laterales".
 */
export const peerGroupLabels: Record<string, string> = {
  GK: "Porteros",
  CB: "Centrales",
  RB: "Laterales derechos",
  LB: "Laterales izquierdos",
  FB: "Laterales",
  MID: "Centrocampistas",
  AM: "Mediapuntas",
  W: "Extremos",
  ST: "Delanteros",
};

const footLabels: Record<string, string> = {
  left: "Izquierdo",
  right: "Derecho",
  both: "Ambos",
};

export function footLabel(key: string): string {
  return label(footLabels, key);
}

/** The four headline KPIs of the profile card (`PLAYER_KPIS`). */
export const playerKpiLabels: Record<string, string> = {
  goals: "Goles",
  assists: "Asistencias",
  npxg: "npxG",
  vaep_per_90: "VAEP /90",
  goal_contributions: "Goles + asist.",
  xa: "xA",
  key_passes: "Pases clave",
  progressive_passes: "Pases progresivos",
  ball_recoveries: "Recuperaciones",
  pass_completion_rate: "Pases %",
  ground_duel_win_rate: "Duelos suelo %",
  aerial_win_rate: "Duelos aéreos %",
  goals_prevented: "Goles evitados",
  save_pct: "Paradas %",
  clean_sheets: "Porterías a cero",
  gk_pass_completion_rate: "Pases %",
};

/** The card titles of the percentile block, per role (`PLAYER_SPINE`). */
export const spineCardLabels: Record<string, string> = {
  goalkeeping: "Portería",
  area: "Área y salidas",
  distribution: "Distribución",
  value: "Valor",
  defending: "Defensa",
  duels: "Duelos",
  passing: "Pase",
  impact: "Impacto",
  progression: "Progresión",
  creation: "Creación",
  passing_progression: "Pase y progresión",
  pressing: "Presión",
  carrying: "Conducción y regate",
  finishing: "Finalización",
  link_play: "Juego asociativo",
  movement: "Desmarque y regate",
};

/**
 * Every metric on a percentile card, with README-PLAYER §4's wording. The
 * possession-adjusted volumes carry no "/90" because the caption above the
 * cards already says "Por 90".
 */
export const spineMetricLabels: Record<string, string> = {
  save_pct: "Paradas %",
  goals_prevented_per_90: "Goles evitados /90",
  saves_per_90: "Paradas /90",
  goals_conceded_per_90: "Goles encajados /90",
  clean_sheet_rate: "Porterías a cero %",
  xg_faced_per_90: "xG en contra /90",
  claims_per_90: "Blocajes /90",
  claim_rate: "Blocajes %",
  punches_per_90: "Despejes de puños /90",
  keeper_sweeper_actions_per_90: "Salidas fuera del área /90",
  sweeper_avg_x: "Altura media de salida (m)",
  padj_clearances_per_90: "Despejes",
  passes_per_90: "Pases /90",
  gk_pass_completion_rate: "Pases %",
  long_pass_completion_rate: "Pases largos (≥30 m) %",
  gk_long_ball_pct: "Pases en largo, cuota",
  progressive_passes_per_90: "Pases progresivos /90",
  passes_received_per_90: "Pases recibidos /90",
  vaep_per_90: "VAEP /90",
  vaep_defensive_per_90: "VAEP defensivo /90",
  vaep_offensive_per_90: "VAEP ofensivo /90",
  errors_per_90: "Errores /90",
  penalties_saved: "Penaltis parados",
  padj_interceptions_per_90: "Intercepciones",
  shots_blocked_per_90: "Tiros bloqueados /90",
  padj_blocked_passes_per_90: "Pases bloqueados",
  padj_ball_recoveries_per_90: "Recuperaciones",
  padj_tackles_per_90: "Duelos en el suelo ganados",
  ground_duel_win_rate: "Duelos en el suelo %",
  dribbled_past_per_90: "Regateado /90",
  aerials_won_per_90: "Duelos aéreos ganados /90",
  aerial_win_rate: "Duelos aéreos %",
  fouls_committed_per_90: "Faltas cometidas /90",
  pass_completion_rate: "Pases %",
  long_balls_per_90: "Balones largos /90",
  long_ball_accuracy: "Balones largos %",
  passes_into_final_third_per_90: "Pases al último tercio /90",
  progressive_carries_per_90: "Conducciones progresivas /90",
  headed_shots_per_90: "Remates de cabeza /90",
  aerials_won_att_third_per_90: "Aéreos ganados en campo rival /90",
  def_actions_final_third_per_90: "Acciones def. último tercio /90",
  progressive_carries_into_final_third_per_90: "Cond. prog. al último tercio /90",
  receptions_final_third_per_90: "Recepciones último tercio /90",
  take_on_success_rate: "Regates %",
  crosses_per_90: "Centros /90",
  cross_completion_rate: "Centros %",
  passes_into_penalty_area_per_90: "Pases al área /90",
  xa_per_90: "xA /90",
  shot_creating_actions_per_90: "Acciones de creación de tiro /90",
  key_passes_per_90: "Pases clave /90",
  through_balls_per_90: "Pases filtrados /90",
  npxg_per_90: "npxG /90",
  xt_per_90: "xT /90",
  take_ons_per_90: "Regates /90",
  dispossessed_per_90: "Pérdidas /90",
  goals_non_penalty_per_90: "Goles sin penalti /90",
  carries_into_penalty_area_per_90: "Conducciones al área /90",
  progressive_receptions_per_90: "Recepciones progresivas /90",
  shots_per_90: "Tiros /90",
  touches_in_box_per_90: "Toques en el área /90",
  npxg_per_shot: "xG por tiro (sin penaltis)",
  receptions_in_box_per_90: "Recepciones en el área /90",
  fouls_won_per_90: "Faltas recibidas /90",
  xg_overperformance_per_90: "Goles − xG (sin penaltis) /90",
};

/** The eight modes of the event map (`?vista=`), in pill order. */
export type PlayerMapMode =
  | "pases"
  | "progresivos"
  | "conducciones"
  | "toques"
  | "recepciones"
  | "tiros"
  | "defensivas"
  | "pases-clave";

export const PLAYER_MAP_MODES: PlayerMapMode[] = [
  "pases",
  "progresivos",
  "conducciones",
  "toques",
  "recepciones",
  "tiros",
  "defensivas",
  "pases-clave",
];

export const playerMapModes: Record<PlayerMapMode, { pill: string; title: string }> = {
  pases: { pill: "Pases", title: "Mapa de pases" },
  progresivos: { pill: "Progresivos", title: "Pases progresivos" },
  conducciones: { pill: "Conducciones", title: "Conducciones progresivas" },
  toques: { pill: "Toques", title: "Mapa de toques" },
  recepciones: { pill: "Recepciones", title: "Recepciones" },
  tiros: { pill: "Tiros", title: "Mapa de tiros" },
  defensivas: { pill: "Defensivas", title: "Acciones defensivas" },
  "pases-clave": { pill: "Pases clave", title: "Pases clave y asistencias" },
};

export const player = {
  eyebrow: "03 · PERFIL DE JUGADOR",
  shirtCaption: "DORSAL",
  /** "LATERAL DERECHO · 24 AÑOS · 1,284'" — the age only when known. */
  metaLine: (position: string, age: number | null, minutes: string) =>
    [position, age === null ? null : `${age} AÑOS`, `${minutes}'`]
      .filter(Boolean)
      .join(" · "),
  /** "LALIGA 2025/2026 · HASTA J38". */
  seasonLine: (season: string, matchday: number | null) =>
    matchday === null ? `LALIGA ${season}` : `LALIGA ${season} · HASTA J${matchday}`,
  tabsLabel: "Jugadores de la plantilla",
  /** The player picker (replaces the pill strip, 16 Oct 2026). */
  pickerLabel: "Cambiar de jugador",

  /** The match filter: what the card, the map and the sequences cover. */
  filter: {
    label: "Partidos",
    season: "Temporada completa",
    last5: "Últimos 5",
    home: "Como local",
    away: "Como visitante",
    pick: "O elige partidos",
    count: (n: number) => (n === 1 ? "1 partido" : `${n} partidos`),
    /** The button when exactly one match is selected: "J5 · L LEV 3–0". */
    one: (matchday: number, venue: string, abbr: string, gf: number, ga: number) =>
      `J${matchday} · ${venue} ${abbr} ${gf}–${ga}`,
    scope: "Ficha, mapa y jugadas · los percentiles son siempre de toda la temporada",
  },

  bio: {
    age: "Edad",
    born: "Nacimiento",
    height: "Altura",
    foot: "Pie",
    nationality: "Nacionalidad",
    position: "Posición",
    matches: "Partidos",
    starts: "Titularidades",
    minutes: "Minutos",
    subbedOff: "Sustituido",
    cards: "Tarjetas",
    penalties: "Penaltis",
  },
  heightCm: (cm: string) => `${cm} cm`,
  /** "5 · 1": yellow, then red. */
  cards: (yellow: string, red: string) => `${yellow} · ${red}`,
  /** "3/4": scored of taken. */
  penalties: (scored: string, taken: string) => `${scored}/${taken}`,
  kpiPer90: (value: string) => `${value} por 90`,
  kpiSquadRank: (rank: number, n: number) => `${rank}.º de ${n} en la plantilla`,
  kpiUnranked: "pocos minutos para ordenar",
  /** Under a filtered rate, where a squad rank would be a season fact. */
  kpiSelection: (n: number) => (n === 1 ? "en este partido" : `en estos ${n} partidos`),

  /**
   * The caption of the event-map card, a sentence like the match page's
   * captions. `shown`/`total` are formatted; `capped` when a cap cut the list.
   */
  mapCaption: (
    mode: PlayerMapMode,
    shown: string,
    total: string,
    capped: boolean,
    display: "heat" | "events" = "heat",
  ) => {
    const attack = " · ataca hacia la derecha";
    const how = display === "heat" ? "por zona" : "uno a uno";
    switch (mode) {
      case "pases":
        return (capped ? `Los ${shown} más recientes de ${total} pases` : `${total} pases`) + attack;
      case "progresivos":
        return (capped
          ? `Los ${shown} de más xT de ${total} pases progresivos`
          : `${total} pases progresivos`) + attack;
      case "conducciones":
        return (capped
          ? `Las ${shown} de más xT de ${total} conducciones progresivas`
          : `${total} conducciones progresivas`) + attack;
      case "toques":
        return `${total} toques con balón, ${how}` + attack;
      case "recepciones":
        return `${total} pases recibidos, ${how}` + attack;
      case "tiros":
        return `${total} tiros` + attack;
      case "defensivas":
        return `${total} acciones defensivas` + (display === "heat" ? `, ${how}` : "") + attack;
      case "pases-clave":
        return `${total} pases clave y asistencias` + attack;
    }
  },
  mapModesLabel: "Qué dibuja el mapa",
  mapEmpty: "Ninguna acción de este tipo en los partidos elegidos.",
  /** The heat / one-by-one switch of Toques, Recepciones and Defensivas. */
  mapDisplay: {
    label: "VER",
    heat: "Mapa de calor",
    events: "Eventos",
  },
  /** The hover card of one mark on the event map. */
  hover: {
    /** "J5 · LEV (L) · 57'". */
    context: (matchday: number, abbr: string, home: boolean, minute: number | null) =>
      `J${matchday} · ${abbr} (${home ? "L" : "V"})` + (minute === null ? "" : ` · ${minute}'`),
    pass: "Pase",
    progressivePass: "Pase progresivo",
    keyPass: "Pase clave",
    assist: "Asistencia",
    carry: "Conducción progresiva",
    completed: "Completado",
    failed: "Fallado",
    carryLost: "Perdió el balón al final",
    xt: (value: string) => `xT ${value}`,
    reception: "Recepción",
    progressiveReception: "Recepción progresiva",
    inBox: "En el área",
    /** Under a touch: the hover title is its type. */
    touch: "Toque con balón",
  },
  /** Legend items in the match page's style: the mark's name, nothing else. */
  legend: {
    restPasses: "Resto de pases",
    failed: "Fallado",
    progressiveRule: "Progresivo: acerca el balón ≥ 25 % a la portería",
    receptionZones: "Recepciones por zona",
    receptionDot: "Recepción progresiva",
    receptionRing: "En el área",
    /** Events display: every reception, the non-progressive ones quiet. */
    receptionAll: "Resto de recepciones",
    touchDot: "Toque",
    assist: "Asistencia",
    keyPass: "Pase clave",
    /** Under every map whose marks can open their chain. */
    hoverRule: "Pasa el cursor por una acción · clic para ver su jugada",
  },

  percentileHeading: (group: string) => `Percentil vs LaLiga · ${group}`,
  percentileCaption: (peerN: number | null, group: string, minMinutes: string) =>
    `Por 90 · vs ${peerN ?? "—"} ${group.toLowerCase()} con ≥ ${minMinutes}' · ` +
    `barra = percentil, más es mejor`,
  percentileUnit: "percentil",
  utilityChip: (pct: string, position: string) => `POLIVALENTE · ${pct} % ${position}`,
  percentileIneligible: (minutes: string, minMinutes: string) =>
    `Todavía pocos minutos: ${minutes}' de los ${minMinutes}' que hacen falta ` +
    `para compararlo con el resto de la liga.`,
  /** Marks a row whose percentile was flipped because lower is better. */
  lowerIsBetter: "↓",

  sequencesHeading: (surname: string) => `Jugadas con ${surname}`,
  aggregates: {
    sequences: "Jugadas",
    xt: "xT en jugadas",
    shots: "Acaban en tiro",
    passes: "Pases por jugada (media)",
  },
  /** "Las 10 mejores de sus 1,107 jugadas de tres o más acciones, por xT. …" */
  sequencesCaption: (shown: number, total: string, metric: string) =>
    `Las ${shown} mejores de sus ${total} jugadas de tres o más acciones, ` +
    `por ${metric}. Elige una para leerla; en dorado, él.`,
  sequencesEmpty: "No hay jugadas de tres o más acciones con él en estos partidos.",
  sequencesLoading: "Cargando las jugadas de estos partidos…",
  sequencesError: "No se pudieron cargar las jugadas de estos partidos.",
  /** The bar over the browser when a chain was opened from the event map. */
  pinned: {
    note: (context: string) => `Jugada abierta desde el mapa · ${context}`,
    back: "Volver a las 10 mejores",
  },
  /** "J9 · OVI (V)": which match a chain in the list is from. */
  sequenceContext: (matchday: number, abbr: string, home: boolean) =>
    `J${matchday} · ${abbr} (${home ? "L" : "V"})`,

  /** The placeholder page built before the export has written any squad.json. */
  pendingTitle: "Perfiles de jugador",
  pendingHeading: { plain: "Perfiles de jugador, ", accent: "en preparación" },
  pendingDek:
    "Los perfiles de jugador todavía no se han generado para esta temporada. " +
    "Aparecerán con la próxima actualización de datos.",

  demoWarning:
    "Jugador ficticio: todos los números de esta página salen de datos " +
    "inventados y deterministas, no de la base de datos.",
} as const;
