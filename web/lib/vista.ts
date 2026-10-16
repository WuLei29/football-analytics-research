/**
 * vista.ts — the two reader choices of screen 03, mirrored to the URL.
 *
 *   ?vista=     the event-map mode
 *   ?partidos=  the match filter (`lib/data/playerFilter.ts`)
 *
 * Several client components read them: the event map (sets the mode), the
 * match filter (sets the matches), the profile card and the sequences
 * (follow the matches), and the player picker, whose links carry both, so a
 * reader comparing two players keeps the same view of the same matches
 * (README-PLAYER § State). They share no parent client component, so the URL
 * is the store: a setter writes it with `replaceState` (no history entry per
 * click) and announces the change; the hooks subscribe.
 *
 * Static export: the server render has no query string, so the server
 * snapshot is the default and the real value arrives on hydration.
 */

"use client";

import { useSyncExternalStore } from "react";

import { PLAYER_MAP_MODES, type PlayerMapMode } from "./labels";

export const DEFAULT_VISTA: PlayerMapMode = "pases";
const EVENT = "vista-change";

function param(name: string): string | null {
  return new URLSearchParams(window.location.search).get(name);
}

function readVista(): PlayerMapMode {
  const value = param("vista");
  return (PLAYER_MAP_MODES as string[]).includes(value ?? "")
    ? (value as PlayerMapMode)
    : DEFAULT_VISTA;
}

function subscribe(callback: () => void) {
  window.addEventListener(EVENT, callback);
  window.addEventListener("popstate", callback);
  return () => {
    window.removeEventListener(EVENT, callback);
    window.removeEventListener("popstate", callback);
  };
}

function write(name: string, value: string | null) {
  const url = new URL(window.location.href);
  if (value === null) url.searchParams.delete(name);
  else url.searchParams.set(name, value);
  window.history.replaceState(window.history.state, "", url);
  window.dispatchEvent(new Event(EVENT));
}

export function useVista(): PlayerMapMode {
  return useSyncExternalStore(subscribe, readVista, () => DEFAULT_VISTA);
}

export function setVista(mode: PlayerMapMode) {
  write("vista", mode === DEFAULT_VISTA ? null : mode);
}

/** The raw `?partidos=` value; parse it with `parseSelection`. */
export function usePartidos(): string | null {
  return useSyncExternalStore(subscribe, () => param("partidos"), () => null);
}

export function setPartidos(value: string | null) {
  write("partidos", value);
}

/** The query string a link to another player carries: `?vista=…&partidos=…`. */
export function useCarriedQuery(): string {
  const vista = useVista();
  const partidos = usePartidos();
  const q = new URLSearchParams();
  if (vista !== DEFAULT_VISTA) q.set("vista", vista);
  if (partidos) q.set("partidos", partidos);
  const s = q.toString();
  return s ? `?${s}` : "";
}
