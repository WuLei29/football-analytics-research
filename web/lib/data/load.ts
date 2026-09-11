/**
 * load.ts — how a page gets its data.
 *
 * Reads one JSON file from `public/data/` and returns it typed. This runs on
 * the server, at BUILD time only: `next.config.ts` sets `output: "export"`, so
 * by the time a browser sees the page the JSON has already been read, rendered
 * into HTML, and the file is gone from the critical path.
 *
 * Which route reads which file is the table in `md/WEB_DATA.md` §2.1.
 *
 * There is a second, later path for the same files: a client component that
 * needs to re-filter without a rebuild (the sequence lab, screen 04) will
 * `fetch("/data/…")` in the browser instead, because those files are large and
 * the filtering is interactive. That helper does not exist yet — it arrives
 * with the page that needs it, not before.
 *
 * Posture: this module FAILS LOUDLY. A missing file or a mismatched schema
 * version stops the build with a message naming the export command that fixes
 * it. That is deliberate and matches `src/export/` — a page rendered from a
 * stale or half-written file looks plausible and is wrong.
 */

import { readFile } from "node:fs/promises";
import path from "node:path";
import { cache } from "react";

import {
  SCHEMA_VERSION,
  type Envelope,
  type LeagueTable,
  type Manifest,
  type MatchFile,
  type Overview,
} from "./types";

/** `public/` is served at the site root, so `public/data/x.json` is `/data/x.json`. */
const DATA_ROOT = path.join(process.cwd(), "public", "data");

/**
 * Read and parse one exported file.
 *
 * `relPath` is relative to `public/data`, e.g. `"teams/espanyol/2025-26/overview.json"`.
 * Wrapped in React's `cache()` so two components on the same page asking for
 * the same file read the disk once.
 */
export const readData = cache(async function readData<T extends Envelope>(
  relPath: string,
): Promise<T> {
  const absolute = path.join(DATA_ROOT, relPath);

  let raw: string;
  try {
    raw = await readFile(absolute, "utf8");
  } catch {
    throw new Error(
      `Falta el fichero de datos "${relPath}".\n` +
        `Genera el arbol de datos desde la raiz del repositorio:\n` +
        `    python -m src.export\n` +
        `(contrato: md/WEB_DATA.md)`,
    );
  }

  const parsed = JSON.parse(raw) as T;

  if (parsed.schema_version !== SCHEMA_VERSION) {
    throw new Error(
      `"${relPath}" tiene schema_version ${parsed.schema_version}, ` +
        `y este sitio espera ${SCHEMA_VERSION}.\n` +
        `El export y la web estan desincronizados: vuelve a ejecutar ` +
        `"python -m src.export", o actualiza SCHEMA_VERSION en lib/data/types.ts.`,
    );
  }

  return parsed;
});

/**
 * `manifest.json` — the only file loaded before the site knows anything.
 * Every other path is built from what it contains (WEB_DATA.md §4).
 */
export const getManifest = cache(() => readData<Manifest>("manifest.json"));

/** The league table and the 24 team-profile metrics for every club (§5). */
export const getLeagueTable = cache((season: string) =>
  readData<LeagueTable>(`league/${season}/table.json`),
);

/** Screen 01 and the match list, for one club and season (§6). */
export const getOverview = cache((team: string, season: string) =>
  readData<Overview>(`teams/${team}/${season}/overview.json`),
);

/**
 * Screen 02: one match (§7). The biggest file the site reads — 200 to 380 KB
 * of momentum bins, shots, arrows and every sequence of the match with its
 * actions — which is exactly why a page reads its own match file and never a
 * tree of them (WEB_PLAN.md §3.3 point 4).
 */
export const getMatch = cache((team: string, season: string, matchId: number) =>
  readData<MatchFile>(`teams/${team}/${season}/matches/${matchId}.json`),
);

/* --------------------------------------------------------------------------
 * Small lookups over the manifest. These live here rather than in each page
 * so that "which season is this slug?" has exactly one answer.
 * ------------------------------------------------------------------------ */

/** The season with this slug, or throw naming the slugs that do exist. */
export async function getSeason(slug: string) {
  const manifest = await getManifest();
  const season = manifest.seasons.find((s) => s.slug === slug);
  if (!season) {
    throw new Error(
      `Temporada "${slug}" no publicada. Publicadas: ` +
        `${manifest.seasons.map((s) => s.slug).join(", ")} ` +
        `(ver EXPORT_SEASONS en src/export/config.py).`,
    );
  }
  return season;
}

/** The club with this slug, or throw naming the slugs that do exist. */
export async function getTeam(slug: string) {
  const manifest = await getManifest();
  const team = manifest.teams.find((t) => t.slug === slug);
  if (!team) {
    throw new Error(
      `Equipo "${slug}" no publicado. Publicados: ` +
        `${manifest.teams.map((t) => t.slug).join(", ")} ` +
        `(ver EXPORT_TEAMS en src/export/config.py).`,
    );
  }
  return team;
}
