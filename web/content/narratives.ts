/**
 * narratives.ts — the authored prose of the site.
 *
 * Renders into: the "Lectura de la temporada" block of screen 01 and the
 * "Lectura del partido" block of screen 02.
 * Data:  NONE. This is the one file on the site whose content is written by
 *        hand rather than exported (`md/WEB_DATA.md` §3.7). The export ships
 *        numbers; a reading of a season is a judgement, and a judgement has
 *        to be written by someone.
 *
 * Format decision (`WEB_DATA.md` §14 item 4, open until now): plain TypeScript
 * modules, not MDX. The blocks are two or three paragraphs of prose with no
 * markup in them, MDX would add a compiler and a dependency to the build for
 * that, and this way a missing entry is a type error rather than a blank card.
 *
 * Rule for anything written here: **every number in the prose must exist in
 * the data files**. The paragraphs below were written against
 * `overview.json` and `league/{season}/table.json` as generated on
 * 10 Sep 2026. If a refresh moves a number, the prose is stale and has to be
 * re-read — which is why there is a `seasonReading` per season slug rather
 * than one paragraph for the club.
 */

/** One authored block: an eyebrow-and-paragraphs pair. */
export interface Narrative {
  paragraphs: string[];
  /** Optional closing line in mono, e.g. what the reading is based on. */
  provenance?: string;
}

/**
 * The reading of a season, keyed by `{team}/{season}`. A season with no entry
 * renders no block at all, which is the honest state for a season that has
 * barely started.
 */
export const seasonReadings: Record<string, Narrative> = {
  "espanyol/2025-26": {
    paragraphs: [
      "Una temporada de supervivencia resuelta con solvencia. El Espanyol " +
        "acabó undécimo con 46 puntos, doce victorias y dieciséis derrotas, y " +
        "lo hizo sin la pelota: 41.7 % de posesión, el decimonoveno equipo de " +
        "la categoría en ese apartado. La diferencia de xG, −8.3, dice que el " +
        "resultado final estuvo en línea con el juego, no por encima.",
      "El sello del equipo está en el balón parado y en los centros. Once de " +
        "sus 43 goles llegaron a balón parado — un 25.6 % del total, quinto " +
        "registro de la liga — y sus 21.2 centros por partido lo sitúan en el " +
        "percentil 95. La contrapartida aparece en el juego aéreo defensivo, " +
        "donde gana el 45.9 % de los duelos, y en una inclinación del campo " +
        "del 43.9 %: cede terreno con naturalidad y ataca en pocas fases muy " +
        "definidas.",
      "La presión, con un PPDA de 15.3, es la de un equipo que espera antes " +
        "de apretar. Es coherente con todo lo demás: recuperar en zona media, " +
        "progresar rápido por fuera y buscar el área con centros y córners.",
    ],
    provenance:
      "Escrito sobre los 38 partidos de liga de la temporada, con datos de " +
      "eventos procesados en el repositorio.",
  },
  "espanyol/2026-27": {
    paragraphs: [
      "Tres jornadas son tres jornadas: aquí no hay tendencia todavía, solo " +
        "un punto de partida. Una victoria y dos derrotas, cinco goles a " +
        "favor y cuatro en contra, y una diferencia de xG de −1.5 que pide " +
        "prudencia con el 3–0 inicial ante el Levante.",
      "Lo que sí se parece a la temporada anterior es la forma de competir: " +
        "45.6 % de posesión y un PPDA de 16.3, es decir, un equipo que sigue " +
        "sin necesitar el balón para hacer daño. Las medias móviles de esta " +
        "página empiezan a dibujarse a partir del quinto partido.",
    ],
    provenance:
      "Escrito sobre los 3 partidos cargados de la temporada 2026/2027.",
  },
};

/**
 * The reading of one match, keyed by `match_id`. Empty on purpose: 41 matches
 * of prose is 41 matches of writing, and a generated paragraph dressed as a
 * reading would be worse than no block. The page omits the block when there is
 * no entry, so adding one is adding a key here and nothing else.
 */
export const matchReadings: Record<number, Narrative> = {};

export function seasonReading(team: string, season: string): Narrative | null {
  return seasonReadings[`${team}/${season}`] ?? null;
}

export function matchReading(matchId: number): Narrative | null {
  return matchReadings[matchId] ?? null;
}
