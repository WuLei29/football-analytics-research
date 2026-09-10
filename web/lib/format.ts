/**
 * format.ts — every number and date the site renders passes through here.
 *
 * Two locales, on purpose:
 *
 *   - **Numbers use a decimal POINT** — `41.7`, `1,464`. The Spanish standard
 *     is the comma and this file used to follow it (WEB_PLAN.md decision 4);
 *     the author's call is the point, which is what football numbers are
 *     written with everywhere they are read. Flip `NUMBER_LOCALE` back to
 *     `es-ES` and the whole site follows — that is the point of routing every
 *     number through one file.
 *   - **Dates stay Spanish** — `3 de septiembre de 2026`. Those are words, and
 *     the site is written in Spanish.
 *
 * Getting the separator right by hand in each component is how a site ends up
 * with `41.7` in one card and `41,7` in the next, so nothing hand-formats a
 * number — it calls one of these. That includes digits inside a caption:
 * build the caption from `dec()`, never type the number into a string.
 *
 * The formatters are built once at module load; `Intl.NumberFormat` is
 * expensive to construct and cheap to reuse.
 */

/** Decimal point, comma thousands: `41.7`, `1,464`. */
const NUMBER_LOCALE = "en-GB";
/** Spanish month and day names. */
const DATE_LOCALE = "es-ES";

const integer = new Intl.NumberFormat(NUMBER_LOCALE, { maximumFractionDigits: 0 });

const decimals = [0, 1, 2, 3].map(
  (d) =>
    new Intl.NumberFormat(NUMBER_LOCALE, {
      minimumFractionDigits: d,
      maximumFractionDigits: d,
    }),
);

/** `46` -> "46". Thousands get a comma: `1464` -> "1,464". */
export function num(value: number): string {
  return integer.format(value);
}

/** Fixed decimals, always shown: `dec(1.2, 1)` -> "1.2"; `dec(2, 1)` -> "2.0". */
export function dec(value: number, places: 0 | 1 | 2 | 3 = 1): string {
  return decimals[places].format(value);
}

/**
 * A difference, with an explicit sign — the design colours these by sign and
 * a bare "0.4" reads as an absolute value. `+0.4`, `−1.2`, `0.0`.
 */
export function signed(value: number, places: 0 | 1 | 2 | 3 = 1): string {
  const body = dec(Math.abs(value), places);
  if (value > 0) return `+${body}`;
  if (value < 0) return `−${body}`; // U+2212 minus, not a hyphen
  return body;
}

/** A share that arrives already scaled (`41.7`), rendered as "41.7 %". */
export function pct(value: number, places: 0 | 1 = 1): string {
  return `${dec(value, places)} %`;
}

/** `"nulo"` in a slot the data left empty. Never render a null as 0. */
export const EMPTY = "—"; // em dash

/** Any of the above, but `null` becomes an em dash instead of throwing. */
export function orEmpty(
  value: number | null | undefined,
  render: (v: number) => string,
): string {
  return value === null || value === undefined ? EMPTY : render(value);
}

/* -------------------------------------------------------------------------- */

const dateLong = new Intl.DateTimeFormat(DATE_LOCALE, {
  day: "numeric",
  month: "long",
  year: "numeric",
});

const dateShort = new Intl.DateTimeFormat(DATE_LOCALE, {
  day: "2-digit",
  month: "short",
});

/**
 * The exported dates are plain `YYYY-MM-DD` with no timezone. Parsing that
 * with `new Date(s)` gives UTC midnight, which in a browser west of Greenwich
 * renders as the previous day. Split it instead — these are calendar dates,
 * not instants.
 */
function calendarDate(iso: string): Date {
  const [y, m, d] = iso.split("-").map(Number);
  return new Date(y, m - 1, d);
}

/** `"2026-09-03"` -> "3 de septiembre de 2026". */
export function longDate(iso: string): string {
  return dateLong.format(calendarDate(iso));
}

/** `"2026-09-03"` -> "03 sept". For dense rows. */
export function shortDate(iso: string): string {
  return dateShort.format(calendarDate(iso));
}

/** `generated_at` is a real instant: "7 sept 2026, 16:37". */
export function buildStamp(isoInstant: string): string {
  return new Intl.DateTimeFormat(DATE_LOCALE, {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Europe/Madrid",
  }).format(new Date(isoInstant));
}
