/**
 * PlayerRoute — a whole screen-03 page, in the Pizarra world: the pill nav,
 * `PlayerView`, the numbered exits and the footer.
 *
 * Renders: the page for one (club, season, player), with Plantilla marked
 * in the nav; or, when `playerId` is null, the "en preparación" placeholder
 * the routes build before the export has written a squad.json.
 * Props: `equipo`, `season` slugs, `playerId`.
 * Data:  `manifest.json`, `overview.json` (for the exits), `squad.json`,
 *        `players/{player_id}.json`, read at build time.
 *
 * Shared by `/{equipo}/jugador/{season}/{player_id}` and the Plantilla tab,
 * `/{equipo}/plantilla/{season}`, which opens on the first player of the
 * squad (16 Oct 2026). One page, two addresses, so they cannot drift.
 *
 * Moved to Pizarra on 24 Oct 2026 (md/CHANGELOG.md): the handoff's
 * masthead and footer became `PillNav`, `Exits` and `BoardFooter`; the
 * blocks and their order are unchanged.
 */

import { access } from "node:fs/promises";
import path from "node:path";

import { BoardFooter } from "@/components/shell/BoardFooter";
import { Exits, type ExitRow } from "@/components/shell/Exits";
import { PillNav } from "@/components/shell/PillNav";
import { VizDefs } from "@/components/viz/VizDefs";
import { getManifest, getOverview, getPlayer, getSquad, getTeam } from "@/lib/data/load";
import { landing, player as copy } from "@/lib/labels";
import { routes } from "@/lib/routes";

import { PlayerView } from "./PlayerView";

/** A hand-supplied, licensed photo, if one was dropped in public/players. */
async function photoFor(playerId: number): Promise<string | null> {
  try {
    await access(path.join(process.cwd(), "public", "players", `${playerId}.jpg`));
    return `/players/${playerId}.jpg`;
  } catch {
    return null;
  }
}

export async function PlayerRoute({
  equipo,
  season,
  playerId,
}: {
  equipo: string;
  season: string;
  playerId: number | null;
}) {
  const manifest = await getManifest();
  const team = await getTeam(equipo);
  const overview = await getOverview(equipo, season);
  const squad = await getSquad(equipo, season);
  const latest = overview.matches.at(-1) ?? null;

  const exits: ExitRow[] = [
    {
      key: "season",
      title: landing.chapters.season.title,
      dek: landing.chapters.season.dek(team.seasons.length),
      href: routes.season(equipo, season),
    },
    {
      key: "match",
      title: landing.chapters.match.title,
      dek: landing.chapters.match.dek(overview.matches.length),
      href: overview.matches.length > 0 ? routes.matches(equipo, season) : null,
    },
    {
      key: "sequence",
      title: landing.chapters.sequence.title,
      dek: landing.chapters.sequence.dek,
      href: latest ? routes.match(equipo, latest.match_id) : null,
    },
    {
      key: "player",
      title: landing.chapters.player.title,
      dek: landing.chapters.player.dek(squad?.players.length ?? 0),
      href: squad ? routes.squad(equipo, season) : null,
    },
    { key: "method", title: landing.chapters.method.title, dek: landing.chapters.method.dek, href: null },
  ];

  const file = playerId === null ? null : await getPlayer(equipo, season, playerId);

  return (
    <div className="pizarra">
      <PillNav team={equipo} season={season} seasonLabel={season.replace("-", "/")} active="squad" />

      <main className="mx-auto max-w-[1440px] px-4 pt-[88px] sm:px-8 sm:pt-[104px] lg:px-10">
        {/* The arrowheads `SequenceDetail` references, once. */}
        <VizDefs />

        <div className="mx-auto max-w-[1360px]">
          {file && playerId !== null ? (
            <PlayerView
              file={file}
              squad={squad?.players ?? []}
              team={equipo}
              season={season}
              photo={await photoFor(playerId)}
            />
          ) : (
            <header>
              <h1 className="text-[clamp(34px,3.9vw,58px)] leading-[1.02] font-semibold tracking-[-0.035em] text-balance">
                {copy.pendingHeading.plain}
                <span className="text-team">{copy.pendingHeading.accent}</span>
              </h1>
              <p className="mt-4 max-w-[62ch] text-[17px] leading-[1.5] text-slate">{copy.pendingDek}</p>
            </header>
          )}

          <Exits rows={exits} current="player" />

          <BoardFooter
            source={manifest.source}
            generatedAt={file?.generated_at ?? manifest.generated_at}
            photoCredit={false}
          />
        </div>
      </main>
    </div>
  );
}
