/**
 * fixtures.ts — invented data for `/demo`, and nothing else.
 *
 * The Phase 4 primitives exist before the exporters that will feed them
 * (`md/WEB_PLAN.md` §5: the match file lands with screen 02). So that they can
 * be built and looked at now, this file produces one plausible payload per
 * block, shaped exactly like the real thing in `md/WEB_DATA.md` §7.
 *
 * Two rules keep this honest:
 *
 *   1. **Nothing outside `/demo` imports this file.** A page that renders
 *      fixture numbers looks exactly like a page that renders real ones, which
 *      is the worst failure mode this site has.
 *   2. **The numbers are seeded, not random.** The same build produces the
 *      same picture, so a visual change is a code change and not noise.
 *
 * When the match exporter lands, `/demo` can switch to a real file and this
 * file can go. Until then it is also the spec check: if a shape here does not
 * fit a component's props, one of the two is wrong about the contract.
 */

import type { MomentumBin, MomentumMarker } from "@/components/viz/Momentum";
import type { NetworkEdge, NetworkNode } from "@/components/viz/PassNetwork";
import type { ProgressionAction } from "@/components/viz/ProgressionArrows";
import type { DefensiveAction } from "@/components/viz/DefensiveActions";
import type { SequenceEntry } from "@/components/viz/SequenceBrowser";
import type { SequenceTrace } from "@/components/viz/SequenceTraces";
import type { SequenceAction } from "@/components/viz/SequenceDetail";
import type { Shot } from "@/components/viz/ShotMap";
import type { XtCell } from "@/components/viz/HeatSurface";
import type { ZoneCell } from "@/components/viz/ZoneHeatmap";
import type { RollingPoint } from "@/components/viz/RollingArea";
import { ZONE_IDS } from "@/lib/viz/pitch";
import { SCHEMA_VERSION, type PassRow, type PlayerFile, type Squad } from "@/lib/data/types";

/**
 * mulberry32 — a 32-bit seeded PRNG. Four lines, uniform enough for fake
 * coordinates, and deterministic, which is the only property that matters here.
 */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export const demoMatch = {
  teamSide: "home" as const,
  team: { abbr: "ESP", name: "Espanyol" },
  opponent: { abbr: "VAL", name: "Valencia" },
  lengthMin: 96,
};

/* --------------------------------------------------------------------------
 * Shots — both teams, opponent already mirrored by the export (§3.1)
 * ------------------------------------------------------------------------ */

export const demoShots: Shot[] = (() => {
  const random = rng(7717);
  const espShooters = ["Puado", "Roberto Fernández", "Milla", "Dolan", "Kike García"];
  const valShooters = ["Hugo Duro", "Diego López", "Rioja", "Pepelu"];

  return Array.from({ length: 24 }, (_, i) => {
    const isTeam = i < 15;
    const xg = Math.pow(random(), 2.2) * 0.42 + 0.02;
    const along = 2 + random() * 26;
    const y = 34 + (random() - 0.5) * (18 + random() * 24);
    const goal = isTeam ? i < 2 : i === 15;
    const onTarget = goal || random() > 0.6;

    // Opta's goalmouth frame: posts at 45.2 and 54.8, crossbar at 38. Only a
    // shot on target gets a pair — off target the qualifiers run to 0-100.
    const mouthY = 46.4 + random() * 7.2;
    const mouthZ = Math.pow(random(), 1.6) * 34;

    return {
      shot_id: i + 1,
      side: isTeam ? ("home" as const) : ("away" as const),
      name: isTeam
        ? espShooters[i % espShooters.length]
        : valShooters[i % valShooters.length],
      minute: 2 + Math.floor(random() * 92),
      x: Number((isTeam ? 105 - along : along).toFixed(1)),
      y: Number(y.toFixed(1)),
      xg: Number(xg.toFixed(3)),
      body_part: random() > 0.75 ? "head" : random() > 0.4 ? "right_foot" : "left_foot",
      outcome: goal ? "goal" : onTarget ? "saved" : "off_target",
      goal_mouth_y: onTarget ? Number(mouthY.toFixed(1)) : null,
      goal_mouth_z: onTarget ? Number(mouthZ.toFixed(1)) : null,
    };
  });
})();

/* --------------------------------------------------------------------------
 * xT surface — the 12 x 8 grid of `xt.py`
 * ------------------------------------------------------------------------ */

export const demoXtCells: XtCell[] = (() => {
  const random = rng(4242);
  const cells: XtCell[] = [];
  for (let cx = 0; cx < 12; cx++) {
    for (let cy = 0; cy < 8; cy++) {
      // Threat rises towards the opponent's goal and towards the centre.
      const forward = Math.pow(cx / 11, 1.7);
      const central = 0.55 + 0.45 * Math.exp(-Math.pow((cy - 3.5) / 3.2, 2));
      const xt = forward * central * (0.55 + random() * 0.6);
      if (xt > 0.02) cells.push({ cx, cy, xt: Number(xt.toFixed(3)) });
    }
  }
  return cells;
})();

/* --------------------------------------------------------------------------
 * Zone heatmap — the 30 zones of `gold.pitch_zones`
 *
 * Shaped like a right-back's season: heavy in the low-y half (the RIGHT
 * flank), heavy in his own and the middle third. If this ever renders along
 * the top of the pitch, the vertical flip in `lib/viz/pitch.ts` has been lost.
 * ------------------------------------------------------------------------ */

export const demoZoneCells: ZoneCell[] = (() => {
  const random = rng(1310);
  return ZONE_IDS.map((zoneId) => {
    const strip = Math.floor(zoneId / 10);
    const channel = zoneId % 10;
    const alongPitch = Math.exp(-Math.pow((strip - 3) / 1.9, 2));
    const rightFlank = channel <= 2 ? 1 : channel === 3 ? 0.35 : 0.12;
    return {
      zone_id: zoneId,
      value: Number((alongPitch * rightFlank * (60 + random() * 40)).toFixed(0)),
    };
  });
})();

/* --------------------------------------------------------------------------
 * Pass network — eleven starters, mean position, vertical pitch
 * ------------------------------------------------------------------------ */

export const demoNetworkNodes: NetworkNode[] = [
  { player_id: 1, surname: "DMITROVIĆ", x: 9, y: 34, touches: 38 },
  { player_id: 2, surname: "EL HILALI", x: 22, y: 12, touches: 54 },
  { player_id: 3, surname: "CABRERA", x: 24, y: 27, touches: 61 },
  { player_id: 4, surname: "CALERO", x: 24, y: 42, touches: 66 },
  { player_id: 5, surname: "ROMERO", x: 22, y: 56, touches: 57 },
  { player_id: 6, surname: "LOZANO", x: 44, y: 22, touches: 48 },
  { player_id: 7, surname: "EXPÓSITO", x: 46, y: 40, touches: 72 },
  { player_id: 8, surname: "DOLAN", x: 62, y: 11, touches: 41 },
  { player_id: 9, surname: "TERRATS", x: 64, y: 34, touches: 45 },
  { player_id: 10, surname: "MILLA", x: 66, y: 57, touches: 36 },
  { player_id: 11, surname: "PUADO", x: 82, y: 34, touches: 33 },
];

export const demoNetworkEdges: NetworkEdge[] = (() => {
  const random = rng(909);
  const pairs: [number, number][] = [
    [1, 3], [1, 4], [2, 3], [3, 4], [4, 5], [3, 6], [4, 7], [6, 7],
    [6, 8], [7, 9], [7, 10], [9, 11], [8, 11], [10, 11], [2, 6], [5, 10], [9, 10],
  ];
  return pairs.map(([from, to]) => ({
    from,
    to,
    passes: 4 + Math.round(random() * 14),
  }));
})();

/* --------------------------------------------------------------------------
 * Progression, defence, sequences
 * ------------------------------------------------------------------------ */

export const demoProgression: ProgressionAction[] = (() => {
  const random = rng(555);
  return Array.from({ length: 30 }, (_, i) => {
    const carry = i % 3 === 0;
    const x = 12 + random() * 62;
    const y = 5 + random() * 58;
    const length = carry ? 8 + random() * 16 : 12 + random() * 30;
    return {
      kind: carry ? ("carry" as const) : ("pass" as const),
      x: Number(x.toFixed(1)),
      y: Number(y.toFixed(1)),
      end_x: Number(Math.min(103, x + length).toFixed(1)),
      end_y: Number(Math.max(2, Math.min(66, y + (random() - 0.5) * 24)).toFixed(1)),
      completed: random() > 0.18,
    };
  });
})();

/**
 * Every action carries a type, a minute and a player, because the tooltip
 * reads them. The type mix is roughly what the export's six-type selection
 * gives on a real match: recoveries and duels dominate, blocks are rare.
 */
export const demoDefence: { line_x: number; actions: DefensiveAction[] } = (() => {
  const random = rng(31337);
  const types = [
    "ball_recovery",
    "ball_recovery",
    "tackle",
    "tackle",
    "interception",
    "challenge",
    "aerial",
    "clearance",
    "block",
  ];
  const defenders = [
    "Cabrera",
    "Calero",
    "El Hilali",
    "Romero",
    "Expósito",
    "Lozano",
    "Terrats",
    "Milla",
  ];

  return {
    line_x: 41.6,
    actions: Array.from({ length: 34 }, () => ({
      type: types[Math.floor(random() * types.length)],
      x: Number((24 + Math.pow(random(), 0.8) * 66).toFixed(1)),
      y: Number((3 + random() * 62).toFixed(1)),
      outcome: random() > 0.42 ? ("won" as const) : ("lost" as const),
      minute: 1 + Math.floor(random() * 95),
      player: defenders[Math.floor(random() * defenders.length)],
    })).sort((a, b) => a.minute - b.minute),
  };
})();

export const demoSequences: SequenceTrace[] = (() => {
  const random = rng(80808);
  return Array.from({ length: 12 }, (_, i) => {
    const n = 4 + Math.floor(random() * 4);
    let x = 20 + random() * 30;
    let y = 8 + random() * 52;
    const points: [number, number][] = [[Number(x.toFixed(1)), Number(y.toFixed(1))]];
    for (let k = 1; k < n; k++) {
      x = Math.min(103, x + 6 + random() * 18);
      y = Math.max(3, Math.min(65, y + (random() - 0.5) * 22));
      points.push([Number(x.toFixed(1)), Number(y.toFixed(1))]);
    }
    return { sequence_id: `SEQ-${14 + i * 3}`, points, ends_in_shot: i < 7 };
  });
})();

/* --------------------------------------------------------------------------
 * The two charts
 * ------------------------------------------------------------------------ */

/** 34 matchdays of a five-match rolling xG difference. */
export const demoRollingXgd: RollingPoint[] = (() => {
  const random = rng(2451);
  return Array.from({ length: 34 }, (_, i) => ({
    matchday: i + 5,
    value: Number((Math.sin(i / 4.4) * 0.62 + (random() - 0.5) * 0.5).toFixed(3)),
  }));
})();

/** The same season on the xT scale — smaller numbers, same shape of story. */
export const demoRollingXt: RollingPoint[] = demoRollingXgd.map((p, i) => ({
  matchday: p.matchday,
  value: Number((p.value * 0.34 + Math.cos(i / 6) * 0.06).toFixed(3)),
}));

/**
 * A 47 + 49 minute match, laid out as the export does: first half 1–47, then
 * second half 45–93. The overlap (45, 46, 47 appear in both halves) is the
 * point — it is what the chart has to place correctly.
 */
export const demoMomentumBins: MomentumBin[] = (() => {
  const random = rng(60613);
  const halves: { period: 1 | 2; first: number; last: number }[] = [
    { period: 1, first: 1, last: 47 },
    { period: 2, first: 45, last: 93 },
  ];
  return halves.flatMap(({ period, first, last }) =>
    Array.from({ length: last - first + 1 }, (_, i) => {
      const minute = first + i;
      const played = period === 1 ? minute : minute + 3; // running clock, for the shape only
      const swing = Math.sin(played / 11) * 0.5 + 0.5;
      return {
        period,
        minute,
        home: Number((random() * 0.09 * (0.5 + swing)).toFixed(4)),
        away: Number((random() * 0.09 * (1.5 - swing)).toFixed(4)),
      };
    }),
  );
})();

export const demoMomentumMarkers: Omit<MomentumMarker, "label">[] = [
  { period: 1, minute: 23, type: "goal", side: "home" },
  { period: 1, minute: 46, type: "goal", side: "away" }, // first-half stoppage
  { period: 1, minute: 47, type: "period", side: null },
  { period: 2, minute: 58, type: "goal", side: "away" },
  { period: 2, minute: 64, type: "sub", side: "home" },
  { period: 2, minute: 64, type: "sub", side: "home" },
  { period: 2, minute: 88, type: "goal", side: "home" },
  { period: 2, minute: 88, type: "sub", side: "home" },
];

/* --------------------------------------------------------------------------
 * One sequence in full detail — the action-level shape (WEB_DATA §7,
 * `sequences[].actions`) that `SequenceDetail` reads.
 *
 * Hand-written rather than generated: it has to be a plausible *move*, and a
 * seeded random walk is not one. This is a right-flank build-up that ends in a
 * cross and a shot, and it exercises every mark the component draws, including
 * the consecutive-block rule — Lozano carries then passes (one node), Dolan
 * beats his man, carries and crosses (one node).
 *
 * Low `y` is the RIGHT flank, so `y` around 8-15 is the right touchline.
 * ------------------------------------------------------------------------ */

export const demoSequenceActions: SequenceAction[] = [
  { kind: "pass", type: "pass", x: 38, y: 22, end_x: 48, end_y: 15, player_id: 3, surname: "Cabrera", shirt_number: 4, outcome: "success" },
  { kind: "carry", type: "carry", x: 48, y: 15, end_x: 56, end_y: 18, player_id: 6, surname: "Lozano", shirt_number: 21, outcome: "success" },
  { kind: "pass", type: "pass", x: 56, y: 18, end_x: 64, end_y: 34, player_id: 6, surname: "Lozano", shirt_number: 21, outcome: "success" },
  { kind: "pass", type: "pass", x: 64, y: 34, end_x: 72, end_y: 11, player_id: 7, surname: "Expósito", shirt_number: 8, outcome: "success" },
  { kind: "take_on", type: "take_on", x: 72, y: 11, end_x: null, end_y: null, player_id: 8, surname: "Dolan", shirt_number: 11, outcome: "success" },
  { kind: "carry", type: "carry", x: 72, y: 11, end_x: 84, end_y: 8, player_id: 8, surname: "Dolan", shirt_number: 11, outcome: "success" },
  { kind: "cross", type: "pass", x: 84, y: 8, end_x: 95, end_y: 33, player_id: 8, surname: "Dolan", shirt_number: 11, outcome: "success" },
  { kind: "shot", type: "goal", x: 95, y: 33, end_x: null, end_y: null, player_id: 11, surname: "Puado", shirt_number: 10, outcome: "success" },
];

/**
 * The list the browser selects from: seven plays of one match, each with its
 * `gold.sequences` header and its actions.
 *
 * `primary_phase` and `outcome` are the real column values (GOLD_LAYER §4.1.6
 * and §4.1.3), not Spanish — the component translates them. The seven are
 * chosen to cover the range a reader will actually meet: a worked attack, a
 * counter, a corner that scores, a build-up that is given away, a long ball
 * that goes out, a regain that wins a foul, and a quick attack that is saved.
 *
 * Low `y` is the RIGHT flank throughout.
 */
export const demoSequenceEntries: SequenceEntry[] = [
  {
    sequence_id: "S-1042",
    primary_phase: "attacking",
    outcome: "shot_saved",
    start_minute: 37,
    start_second: 12,
    duration_s: 14.6,
    event_count: demoSequenceActions.length,
    xt: 0.087,
    vaep: 0.121,
    actions: demoSequenceActions,
  },
  {
    sequence_id: "S-0118",
    primary_phase: "counter_attack",
    outcome: "shot_off_target",
    start_minute: 9,
    start_second: 41,
    duration_s: 8.2,
    event_count: 4,
    xt: 0.062,
    vaep: 0.074,
    actions: [
      { kind: "pass", type: "pass", x: 41, y: 47, end_x: 57, end_y: 44, player_id: 7, surname: "Expósito", shirt_number: 8, outcome: "success" },
      { kind: "carry", type: "carry", x: 57, y: 44, end_x: 71, end_y: 47, player_id: 10, surname: "Milla", shirt_number: 24, outcome: "success" },
      { kind: "pass", type: "pass", x: 71, y: 47, end_x: 89, end_y: 38, player_id: 10, surname: "Milla", shirt_number: 24, outcome: "success" },
      { kind: "shot", type: "goal", x: 89, y: 38, end_x: null, end_y: null, player_id: 11, surname: "Puado", shirt_number: 10, outcome: "fail" },
    ],
  },
  {
    sequence_id: "S-0603",
    primary_phase: "set_piece",
    outcome: "goal",
    start_minute: 23,
    start_second: 4,
    duration_s: 6.1,
    event_count: 3,
    xt: 0.041,
    vaep: 0.263,
    actions: [
      { kind: "cross", type: "pass", x: 105, y: 68, end_x: 94, end_y: 31, player_id: 10, surname: "Milla", shirt_number: 24, outcome: "success" },
      { kind: "pass", type: "pass", x: 94, y: 31, end_x: 98, end_y: 36, player_id: 4, surname: "Calero", shirt_number: 5, outcome: "success" },
      { kind: "shot", type: "goal", x: 98, y: 36, end_x: null, end_y: null, player_id: 12, surname: "Kike García", shirt_number: 9, outcome: "success" },
    ],
  },
  {
    sequence_id: "S-0271",
    primary_phase: "buildup",
    outcome: "turnover",
    start_minute: 14,
    start_second: 55,
    duration_s: 11.3,
    event_count: 4,
    xt: 0.004,
    vaep: -0.018,
    actions: [
      { kind: "pass", type: "pass", x: 8, y: 34, end_x: 21, end_y: 25, player_id: 1, surname: "Dmitrović", shirt_number: 1, outcome: "success" },
      { kind: "carry", type: "carry", x: 21, y: 25, end_x: 27, end_y: 22, player_id: 3, surname: "Cabrera", shirt_number: 4, outcome: "success" },
      { kind: "pass", type: "pass", x: 27, y: 22, end_x: 34, end_y: 12, player_id: 3, surname: "Cabrera", shirt_number: 4, outcome: "success" },
      { kind: "pass", type: "pass", x: 34, y: 12, end_x: 52, end_y: 9, player_id: 2, surname: "El Hilali", shirt_number: 2, outcome: "fail" },
    ],
  },
  {
    sequence_id: "S-0844",
    primary_phase: "direct_long",
    outcome: "ball_out",
    start_minute: 31,
    start_second: 18,
    duration_s: 4.4,
    event_count: 2,
    xt: 0.011,
    vaep: 0.006,
    actions: [
      { kind: "pass", type: "pass", x: 12, y: 34, end_x: 62, end_y: 58, player_id: 1, surname: "Dmitrović", shirt_number: 1, outcome: "success" },
      { kind: "pass", type: "pass", x: 62, y: 58, end_x: 78, end_y: 67, player_id: 9, surname: "Terrats", shirt_number: 14, outcome: "fail" },
    ],
  },
  {
    sequence_id: "S-1187",
    primary_phase: "high_transition",
    outcome: "foul_won",
    start_minute: 52,
    start_second: 30,
    duration_s: 5.8,
    event_count: 3,
    xt: 0.033,
    vaep: 0.048,
    actions: [
      { kind: "carry", type: "carry", x: 68, y: 20, end_x: 74, end_y: 24, player_id: 6, surname: "Lozano", shirt_number: 21, outcome: "success" },
      { kind: "pass", type: "pass", x: 74, y: 24, end_x: 82, end_y: 30, player_id: 6, surname: "Lozano", shirt_number: 21, outcome: "success" },
      { kind: "take_on", type: "take_on", x: 82, y: 30, end_x: null, end_y: null, player_id: 11, surname: "Puado", shirt_number: 10, outcome: "fail" },
    ],
  },
  {
    sequence_id: "S-1355",
    primary_phase: "fast_attacking",
    outcome: "shot_saved",
    start_minute: 71,
    start_second: 8,
    duration_s: 9.7,
    event_count: 5,
    xt: 0.096,
    vaep: 0.134,
    actions: [
      { kind: "pass", type: "pass", x: 55, y: 40, end_x: 66, end_y: 52, player_id: 7, surname: "Expósito", shirt_number: 8, outcome: "success" },
      { kind: "take_on", type: "take_on", x: 66, y: 52, end_x: null, end_y: null, player_id: 13, surname: "Roberto Fernández", shirt_number: 19, outcome: "success" },
      { kind: "carry", type: "carry", x: 66, y: 52, end_x: 79, end_y: 56, player_id: 13, surname: "Roberto Fernández", shirt_number: 19, outcome: "success" },
      { kind: "cross", type: "pass", x: 79, y: 56, end_x: 93, end_y: 36, player_id: 13, surname: "Roberto Fernández", shirt_number: 19, outcome: "success" },
      { kind: "shot", type: "goal", x: 93, y: 36, end_x: null, end_y: null, player_id: 12, surname: "Kike García", shirt_number: 9, outcome: "success" },
    ],
  },
];

/* --------------------------------------------------------------------------
 * Screen 03 — one whole player file and its squad, for /demo/jugador.
 *
 * A right back on purpose: his touches, passes and defensive actions belong
 * in the LOWER half of a horizontal pitch (low y = right flank), which is
 * the first thing to check on a real file (README-PLAYER §3b). Shaped
 * exactly like `players/{player_id}.json`; if a field here stops fitting
 * `PlayerFile`, one of the two is wrong about the contract.
 * ------------------------------------------------------------------------ */

const DEMO_SURNAMES: [string, string, string, number, number][] = [
  // first, last, role, shirt, minutes
  ["Joan", "García", "GK", 13, 3240],
  ["Fernando", "Calero", "CB", 5, 2980],
  ["Leandro", "Cabrera", "CB", 6, 2710],
  ["Marash", "Kumbulla", "CB", 4, 1220],
  ["Omar", "El Hilali", "RB", 23, 2860],
  ["Rubén", "Sánchez", "RB", 2, 640],
  ["Carlos", "Romero", "LB", 22, 2590],
  ["Pol", "Lozano", "MID", 10, 2410],
  ["Edu", "Expósito", "MID", 8, 2950],
  ["Urko", "González", "MID", 15, 1710],
  ["Álvaro", "Tejero", "AM", 12, 1580],
  ["Jofre", "Carreras", "W", 17, 1890],
  ["Pere", "Milla", "W", 11, 2120],
  ["Antoniu", "Roca", "W", 30, 420],
  ["Javi", "Puado", "ST", 7, 2470],
  ["Roberto", "Fernández", "ST", 9, 1510],
];

export const demoSquad: Squad = {
  schema_version: SCHEMA_VERSION,
  generated_at: "2026-10-16T00:00:00Z",
  team: "espanyol",
  season: "2025-26",
  min_minutes: 855,
  players: DEMO_SURNAMES.map(([first, last, role, shirt, minutes], i) => ({
    player_id: 900 + i,
    name: { first, last, full: `${first} ${last}` },
    shirt_number: shirt,
    position: role === "MID" ? "CM" : role === "W" ? "RW" : role,
    position_group: role === "AM" ? "MID" : role,
    role,
    matches: Math.round(minutes / 80),
    starts: Math.round(minutes / 92),
    minutes,
  })),
};

export const demoPlayerFile: PlayerFile = (() => {
  const random = rng(3003);
  const gauss = () => (random() + random() + random() - 1.5) / 1.5;
  const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v));
  const r1 = (v: number) => Math.round(v * 10) / 10;
  const isProgressive = (x: number, y: number, ex: number, ey: number) =>
    Math.hypot(105 - ex, 34 - ey) <= 0.75 * Math.hypot(105 - x, 34 - y);

  // A right back's pass: from the right flank (low y), mostly forward.
  function pass(flags = 0): PassRow {
    const x = clamp(52 + gauss() * 26, 4, 100);
    const y = clamp(9 + Math.abs(gauss()) * 14, 1, 40);
    const ex = clamp(x + 4 + gauss() * 22, 1, 104);
    const ey = clamp(y + 6 + gauss() * 16, 1, 67);
    const completed = random() < 0.8 ? 1 : 0;
    const prog = isProgressive(x, y, ex, ey) ? 2 : 0;
    return [r1(x), r1(y), r1(ex), r1(ey), completed | prog | flags];
  }

  const passes = Array.from({ length: 150 }, () => pass());
  const progressive: PassRow[] = [];
  while (progressive.length < 60) {
    const p = pass();
    if (p[4] & 2) progressive.push(p);
  }
  const keyPasses: PassRow[] = Array.from({ length: 21 }, (_, i) => {
    const x = clamp(70 + random() * 30, 60, 102);
    const y = clamp(6 + random() * 14, 1, 30);
    return [r1(x), r1(y), r1(92 + random() * 10), r1(26 + random() * 16), 1 | (i < 4 ? 8 : 4)];
  });
  const carries = Array.from({ length: 41 }, () => {
    const x = clamp(40 + random() * 45, 5, 95);
    const y = clamp(5 + random() * 15, 1, 40);
    return [r1(x), r1(y), r1(clamp(x + 9 + random() * 18, 0, 105)), r1(clamp(y + gauss() * 6, 1, 67)), random() < 0.9 ? 1 : 0] as [number, number, number, number, number];
  });

  // Zone heat: strongest on the right channel (y_channel 1 = low y).
  const zoneWeight = (zone: number, peakStrip: number) => {
    const strip = Math.floor(zone / 10);
    const channel = zone % 10;
    return Math.exp(-((channel - 1) ** 2) / 1.2) * Math.exp(-((strip - peakStrip) ** 2) / 3);
  };
  const zones = (total: number, peak: number) => {
    const w = ZONE_IDS.map((z) => zoneWeight(z, peak));
    const sum = w.reduce((a, b) => a + b, 0);
    return ZONE_IDS.map((zone_id, i) => {
      const count = Math.round((w[i] / sum) * total);
      return { zone_id, count, share: Math.round((1000 * count) / total) / 10 };
    }).filter((z) => z.count > 0);
  };

  const shots = Array.from({ length: 14 }, (_, i) => {
    const x = r1(84 + random() * 18);
    const y = r1(clamp(34 + gauss() * 10, 14, 54));
    const outcome = i === 0 ? "goal" : ["saved", "off_target", "blocked", "off_target"][i % 4];
    return {
      shot_id: i + 1,
      match_id: 4 + i,
      matchday: 2 + i * 2,
      minute: Math.floor(random() * 90) + 1,
      x,
      y,
      xg: Math.round((Math.pow(random(), 2) * 0.3 + 0.02) * 1000) / 1000,
      body_part: random() < 0.7 ? "right_foot" : "head",
      outcome,
      play_pattern: "regular_play",
      first_time: random() < 0.4,
      big_chance: i === 0,
      goal_mouth_y: outcome === "goal" || outcome === "saved" ? r1(46 + random() * 8) : null,
      goal_mouth_z: outcome === "goal" || outcome === "saved" ? r1(random() * 30) : null,
    };
  });

  const defTypes = ["ball_recovery", "ball_recovery", "interception", "tackle", "clearance", "challenge", "blocked_pass", "attempted_tackle"];
  const defence = Array.from({ length: 118 }, () => {
    const type = defTypes[Math.floor(random() * defTypes.length)];
    const lost = type === "challenge" || type === "attempted_tackle" || (type === "tackle" && random() < 0.35);
    return {
      type,
      x: r1(clamp(38 + gauss() * 26, 2, 100)),
      y: r1(clamp(10 + Math.abs(gauss()) * 13, 1, 45)),
      outcome: lost ? ("lost" as const) : ("won" as const),
      minute: Math.floor(random() * 92) + 1,
      matchday: 1 + Math.floor(random() * 38),
    };
  });

  const opponents = ["GIR", "VAL", "BET", "SEV", "ATM", "CEL", "OSA", "RAY", "MLL", "ALA", "GET", "VIL"];
  const kinds = ["positional", "counter_attack", "high_transition", "buildup", "direct_long", "fast"];
  const chain = (seed: number) => {
    const n = 4 + Math.floor(random() * 4);
    let x = 20 + random() * 30;
    let y = 8 + random() * 10;
    const pts: [number, number][] = [];
    for (let i = 0; i < n; i++) {
      pts.push([r1(x), r1(y)]);
      x = clamp(x + 6 + random() * 16, 0, 105);
      y = clamp(y + gauss() * 14 + (i > n / 2 ? 5 : 0), 1, 67);
    }
    pts.push([105, r1(30 + random() * 8)]);
    const touch = 1 + Math.floor(random() * (n - 2));
    return { pts, touch, seed };
  };
  const sequences = (metric: "xt" | "vaep") =>
    Array.from({ length: 10 }, (_, i) => {
      const { pts, touch } = chain(i);
      const v = Math.round((0.42 - i * 0.033 + random() * 0.02) * 1000) / 1000;
      return {
        id: (metric === "xt" ? 5000 : 6000) + i,
        match_id: 4 + i,
        matchday: 3 + ((i * 7) % 36),
        opponent_abbr: opponents[i % opponents.length],
        is_home: i % 2 === 0,
        period: i % 2 ? 2 : 1,
        minute: 5 + ((i * 17) % 85),
        second: (i * 23) % 60,
        kind: kinds[i % kinds.length],
        outcome: i % 3 === 0 ? "shot_saved" : "turnover",
        events: pts.length + 2,
        passes: pts.length - 1,
        player_touches: 1 + (i % 3),
        xt: metric === "xt" ? v : Math.round(v * 0.7 * 1000) / 1000,
        vaep: metric === "vaep" ? v : Math.round(v * 0.6 * 1000) / 1000,
        ends_in_shot: i % 3 === 0,
        points: pts,
        player_touch_index: touch,
      };
    });

  const metric = (key: string, value: number | null, percentile: number | null, decimals: 0 | 1 | 2 | 3 = 2, scale: "raw" | "pct" = "raw", higher = true) =>
    ({ key, value, percentile, higher_is_better: higher, scale, decimals });

  return {
    schema_version: SCHEMA_VERSION,
    generated_at: "2026-10-16T00:00:00Z",
    team: { slug: "espanyol", team_id: 8, short_name: "Espanyol", abbr: "ESP" },
    season: { slug: "2025-26", label: "2025/2026", competition_name: "Primera División", through_matchday: 38 },
    player: {
      player_id: 904,
      name: { first: "Omar", last: "El Hilali", full: "Omar El Hilali" },
      shirt_number: 23,
      nationality: "Marruecos",
      preferred_foot: "right",
      height_cm: 180,
      date_of_birth: "2003-09-12",
      age: 22,
      position: "RB",
      position_group: "RB",
      role: "RB",
      is_utility: false,
      primary_position_pct: 91,
      matches: 34,
      starts: 31,
      minutes: 2860,
      subbed_off: 9,
      yellow_cards: 7,
      red_cards: 1,
      penalties_taken: 0,
      penalties_scored: 0,
    },
    kpis: [
      { key: "vaep_per_90", value: 0.214, scale: "raw", decimals: 3, per_90: null, squad_rank: 4, squad_n: 18 },
      { key: "ground_duel_win_rate", value: 61.8, scale: "pct", decimals: 1, per_90: null, squad_rank: 3, squad_n: 18 },
      { key: "aerial_win_rate", value: 48.2, scale: "pct", decimals: 1, per_90: null, squad_rank: 9, squad_n: 18 },
      { key: "progressive_passes", value: 148, scale: "raw", decimals: 0, per_90: 4.66, squad_rank: null, squad_n: null },
    ],
    maps: {
      counts: {
        passes: 1284,
        progressive_passes: 148,
        key_passes: 17,
        assists: 4,
        progressive_carries: 41,
        touches: 1960,
        receptions: 1105,
      },
      passes,
      progressive_passes: progressive,
      key_passes: keyPasses,
      carries,
      touch_zones: zones(1960, 3.4),
      reception_zones: zones(1105, 4.1),
      progressive_receptions: Array.from({ length: 64 }, () => {
        const x = r1(clamp(60 + random() * 40, 40, 104));
        const y = r1(clamp(5 + random() * 20, 1, 50));
        return [x, y, x > 88.5 && y > 13.84 ? 1 : 0] as [number, number, number];
      }),
      shots,
      defence,
    },
    percentiles: {
      eligible: true,
      role: "RB",
      peer_group: "RB",
      peer_n: 31,
      pooled_fb: false,
      pooled_am: false,
      min_minutes: 855,
      cards: [
        { key: "defending", metrics: [
          metric("padj_interceptions_per_90", 1.62, 58), metric("padj_ball_recoveries_per_90", 5.31, 74),
          metric("padj_clearances_per_90", 1.95, 33), metric("shots_blocked_per_90", 0.28, 47),
          metric("def_actions_final_third_per_90", 1.12, 81), metric("vaep_defensive_per_90", 0.061, 66, 3) ] },
        { key: "duels", metrics: [
          metric("padj_tackles_per_90", 2.41, 69), metric("ground_duel_win_rate", 61.8, 72, 1, "pct"),
          metric("dribbled_past_per_90", 0.84, 41, 2, "raw", false), metric("aerials_won_per_90", 1.06, 38),
          metric("aerial_win_rate", 48.2, 44, 1, "pct"), metric("fouls_committed_per_90", 1.21, 29, 2, "raw", false) ] },
        { key: "progression", metrics: [
          metric("pass_completion_rate", 81.4, 52, 1, "pct"), metric("progressive_passes_per_90", 4.66, 77),
          metric("progressive_carries_per_90", 1.29, 64), metric("progressive_carries_into_final_third_per_90", 0.57, 70),
          metric("receptions_final_third_per_90", 9.84, 85), metric("take_on_success_rate", 44.0, null, 1, "pct") ] },
        { key: "creation", metrics: [
          metric("crosses_per_90", 3.02, 88), metric("cross_completion_rate", 26.1, 61, 1, "pct"),
          metric("passes_into_penalty_area_per_90", 1.88, 79), metric("xa_per_90", 0.091, 73, 3),
          metric("shot_creating_actions_per_90", 2.31, 76), metric("vaep_per_90", 0.214, 83, 3) ] },
      ],
    },
    sequences: {
      aggregates: { sequences: 412, xt: 18.74, ends_in_shot: 61, passes_per_sequence: 5.8 },
      min_events: 3,
      top_xt: sequences("xt"),
      top_vaep: sequences("vaep"),
    },
  };
})();
