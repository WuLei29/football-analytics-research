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

export const demoMomentumBins: MomentumBin[] = (() => {
  const random = rng(60613);
  return Array.from({ length: demoMatch.lengthMin }, (_, i) => {
    const minute = i + 1;
    const swing = Math.sin(minute / 11) * 0.5 + 0.5;
    return {
      minute,
      home: Number((random() * 0.09 * (0.5 + swing)).toFixed(4)),
      away: Number((random() * 0.09 * (1.5 - swing)).toFixed(4)),
    };
  });
})();

export const demoMomentumMarkers: Omit<MomentumMarker, "label">[] = [
  { minute: 23, type: "goal", side: "home" },
  { minute: 45, type: "period", side: null },
  { minute: 58, type: "goal", side: "away" },
  { minute: 64, type: "sub", side: "home" },
  { minute: 88, type: "goal", side: "home" },
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
  { kind: "pass", x: 38, y: 22, end_x: 48, end_y: 15, player_id: 3, surname: "Cabrera", shirt_number: 4, outcome: "success" },
  { kind: "carry", x: 48, y: 15, end_x: 56, end_y: 18, player_id: 6, surname: "Lozano", shirt_number: 21, outcome: "success" },
  { kind: "pass", x: 56, y: 18, end_x: 64, end_y: 34, player_id: 6, surname: "Lozano", shirt_number: 21, outcome: "success" },
  { kind: "pass", x: 64, y: 34, end_x: 72, end_y: 11, player_id: 7, surname: "Expósito", shirt_number: 8, outcome: "success" },
  { kind: "take_on", x: 72, y: 11, end_x: null, end_y: null, player_id: 8, surname: "Dolan", shirt_number: 11, outcome: "success" },
  { kind: "carry", x: 72, y: 11, end_x: 84, end_y: 8, player_id: 8, surname: "Dolan", shirt_number: 11, outcome: "success" },
  { kind: "cross", x: 84, y: 8, end_x: 95, end_y: 33, player_id: 8, surname: "Dolan", shirt_number: 11, outcome: "success" },
  { kind: "shot", x: 95, y: 33, end_x: null, end_y: null, player_id: 11, surname: "Puado", shirt_number: 10, outcome: "success" },
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
      { kind: "pass", x: 41, y: 47, end_x: 57, end_y: 44, player_id: 7, surname: "Expósito", shirt_number: 8, outcome: "success" },
      { kind: "carry", x: 57, y: 44, end_x: 71, end_y: 47, player_id: 10, surname: "Milla", shirt_number: 24, outcome: "success" },
      { kind: "pass", x: 71, y: 47, end_x: 89, end_y: 38, player_id: 10, surname: "Milla", shirt_number: 24, outcome: "success" },
      { kind: "shot", x: 89, y: 38, end_x: null, end_y: null, player_id: 11, surname: "Puado", shirt_number: 10, outcome: "fail" },
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
      { kind: "cross", x: 105, y: 68, end_x: 94, end_y: 31, player_id: 10, surname: "Milla", shirt_number: 24, outcome: "success" },
      { kind: "pass", x: 94, y: 31, end_x: 98, end_y: 36, player_id: 4, surname: "Calero", shirt_number: 5, outcome: "success" },
      { kind: "shot", x: 98, y: 36, end_x: null, end_y: null, player_id: 12, surname: "Kike García", shirt_number: 9, outcome: "success" },
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
      { kind: "pass", x: 8, y: 34, end_x: 21, end_y: 25, player_id: 1, surname: "Dmitrović", shirt_number: 1, outcome: "success" },
      { kind: "carry", x: 21, y: 25, end_x: 27, end_y: 22, player_id: 3, surname: "Cabrera", shirt_number: 4, outcome: "success" },
      { kind: "pass", x: 27, y: 22, end_x: 34, end_y: 12, player_id: 3, surname: "Cabrera", shirt_number: 4, outcome: "success" },
      { kind: "pass", x: 34, y: 12, end_x: 52, end_y: 9, player_id: 2, surname: "El Hilali", shirt_number: 2, outcome: "fail" },
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
      { kind: "pass", x: 12, y: 34, end_x: 62, end_y: 58, player_id: 1, surname: "Dmitrović", shirt_number: 1, outcome: "success" },
      { kind: "pass", x: 62, y: 58, end_x: 78, end_y: 67, player_id: 9, surname: "Terrats", shirt_number: 14, outcome: "fail" },
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
      { kind: "carry", x: 68, y: 20, end_x: 74, end_y: 24, player_id: 6, surname: "Lozano", shirt_number: 21, outcome: "success" },
      { kind: "pass", x: 74, y: 24, end_x: 82, end_y: 30, player_id: 6, surname: "Lozano", shirt_number: 21, outcome: "success" },
      { kind: "take_on", x: 82, y: 30, end_x: null, end_y: null, player_id: 11, surname: "Puado", shirt_number: 10, outcome: "fail" },
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
      { kind: "pass", x: 55, y: 40, end_x: 66, end_y: 52, player_id: 7, surname: "Expósito", shirt_number: 8, outcome: "success" },
      { kind: "take_on", x: 66, y: 52, end_x: null, end_y: null, player_id: 13, surname: "Roberto Fernández", shirt_number: 19, outcome: "success" },
      { kind: "carry", x: 66, y: 52, end_x: 79, end_y: 56, player_id: 13, surname: "Roberto Fernández", shirt_number: 19, outcome: "success" },
      { kind: "cross", x: 79, y: 56, end_x: 93, end_y: 36, player_id: 13, surname: "Roberto Fernández", shirt_number: 19, outcome: "success" },
      { kind: "shot", x: 93, y: 36, end_x: null, end_y: null, player_id: 12, surname: "Kike García", shirt_number: 9, outcome: "success" },
    ],
  },
];
