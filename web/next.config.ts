import type { NextConfig } from "next";

/**
 * Static export (WEB_PLAN.md §3).
 *
 * `output: "export"` makes `next build` emit a folder of plain HTML/JS/JSON
 * into `out/`. There is no Node server at runtime: every page is rendered
 * once, at build time, from the JSON files in `public/data/`. That is the
 * whole architecture of this site — the data only changes when the export
 * runs, so the site does not need a database (WEB_PLAN.md §2).
 *
 * Consequence to remember while building pages: anything that reads the
 * filesystem or the database runs at BUILD time only. Anything that must
 * react to a click is a client component ("use client").
 */
const nextConfig: NextConfig = {
  output: "export",
  // Next's image optimiser needs a server; a static export cannot have one.
  images: { unoptimized: true },
  // Emits `out/ruta/index.html` instead of `out/ruta.html`, which every
  // static host (Vercel included) serves without extra rewrite rules.
  trailingSlash: true,
};

export default nextConfig;
