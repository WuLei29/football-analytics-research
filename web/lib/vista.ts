/**
 * vista.ts — the event-map mode of screen 03, mirrored to `?vista=`.
 *
 * Two client components read it: the event map (which sets it) and the
 * player tab strip (whose links carry it, so the mode is sticky across
 * players — a scout comparing two players wants the same view;
 * README-PLAYER § State). They share no parent client component, so the URL
 * is the store: `setVista` writes it with `replaceState` (no history entry
 * per pill click) and announces the change; `useVista` subscribes.
 *
 * Static export: the server render has no query string, so the server
 * snapshot is the default and the real value arrives on hydration.
 */

"use client";

import { useSyncExternalStore } from "react";

import { PLAYER_MAP_MODES, type PlayerMapMode } from "./labels";

export const DEFAULT_VISTA: PlayerMapMode = "pases";
const EVENT = "vista-change";

function read(): PlayerMapMode {
  const value = new URLSearchParams(window.location.search).get("vista");
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

export function useVista(): PlayerMapMode {
  return useSyncExternalStore(subscribe, read, () => DEFAULT_VISTA);
}

export function setVista(mode: PlayerMapMode) {
  const url = new URL(window.location.href);
  if (mode === DEFAULT_VISTA) url.searchParams.delete("vista");
  else url.searchParams.set("vista", mode);
  window.history.replaceState(window.history.state, "", url);
  window.dispatchEvent(new Event(EVENT));
}
