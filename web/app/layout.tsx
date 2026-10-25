/**
 * layout.tsx — the root layout: the <html> document every page is rendered
 * into, and the fonts.
 *
 * Renders: <html lang="es"> with the font variable attached, and a bare
 * <body>. Every page draws its own frame (pill nav, board, footer); the
 * landing runs its photo edge to edge. The handoff's three faces (Outfit,
 * Figtree, DM Mono) and its `(handoff)` route group went on 24 Oct 2026.
 * Props: `children` — the page.
 *
 * The faces are loaded with `next/font/google`, which downloads them at
 * BUILD time and self-hosts the files. Nothing is fetched from Google when a
 * reader opens the site: no third-party request, no layout shift while a font
 * swaps in. The face exposes `--font-hanken`, which `app/globals.css` maps
 * onto `--font-board`.
 */

import type { Metadata } from "next";
import { Hanken_Grotesk } from "next/font/google";

import { site } from "@/lib/labels";

import "./globals.css";

/**
 * The one face of the Pizarra world: figures at 300, text at 400-500,
 * headings at 600. Variable, so one file covers every weight.
 */
const hanken = Hanken_Grotesk({
  subsets: ["latin"],
  variable: "--font-hanken",
  display: "swap",
});

export const metadata: Metadata = {
  title: {
    default: site.name,
    template: `%s · ${site.name}`,
  },
  description: site.tagline,
  // Open Graph matters here: most traffic arrives from a shared link
  // (WEB_PLAN.md §3.3, point 8). The generated share images are Phase 6.
  openGraph: {
    title: site.name,
    description: site.tagline,
    locale: "es_ES",
    type: "website",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html
      lang="es"
      className={hanken.variable}
    >
      <body>{children}</body>
    </html>
  );
}
