"use client";

/**
 * SeasonMenu — the nav's season selector: the team pill and the seasons it
 * opens. The only JavaScript `PillNav` ships.
 *
 * Renders: `shell/Dropdown` in its team tone, the current season as a marked
 * row (not a link: on a player page it would jump to the first of the squad)
 * and the others as links.
 * Props: `current` (the pill's label), `options`, built by `PillNav`.
 * Data:  none.
 */

import Link from "next/link";

import { landing } from "@/lib/labels";

import { Dropdown } from "./Dropdown";

export interface SeasonOption {
  slug: string;
  label: string;
  href: string;
  current: boolean;
}

export function SeasonMenu({ current, options }: { current: string; options: SeasonOption[] }) {
  return (
    <Dropdown label={landing.seasonMenu(current)} trigger={current} tone="team" align="right" width={132}>
      {(close) => (
        <ul className="flex flex-col gap-0.5 p-1.5 text-[14px]">
          {options.map((o) => (
            <li key={o.slug}>
              {o.current ? (
                <span aria-current="true" className="block rounded-row bg-spot px-3 py-2 font-semibold text-team">
                  {o.label}
                </span>
              ) : (
                <Link
                  href={o.href}
                  onClick={close}
                  className="block rounded-row px-3 py-2 font-medium text-ink transition-colors hover:bg-panel focus-visible:bg-panel"
                >
                  {o.label}
                </Link>
              )}
            </li>
          ))}
        </ul>
      )}
    </Dropdown>
  );
}
