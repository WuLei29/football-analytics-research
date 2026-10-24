/**
 * layout.tsx — the root layout: the <html> document every page is rendered
 * into, and the fonts.
 *
 * Renders: <html lang="es"> with the font variables attached, and a bare
 * <body>. The handoff's page padding moved to `app/(handoff)/layout.tsx` on
 * 24 Oct 2026: the landing (the new "Pizarra" world) runs its photo edge to
 * edge and draws its own frame.
 * Props: `children` — the page.
 *
 * The faces are loaded with `next/font/google`, which downloads them at
 * BUILD time and self-hosts the files. Nothing is fetched from Google when a
 * reader opens the site: no third-party request, no layout shift while a font
 * swaps in. Each face exposes a CSS variable, and `app/globals.css` maps those
 * onto `--font-display` / `--font-sans` / `--font-mono`.
 */

import type { Metadata } from "next";
import { DM_Mono, Figtree, Hanken_Grotesk, Outfit } from "next/font/google";

import { site } from "@/lib/labels";

import "./globals.css";

/** Display + numerics: H1, KPI values, stat values. */
const outfit = Outfit({
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  variable: "--font-outfit",
  display: "swap",
});

/** Body and names, including the italic deks. */
const figtree = Figtree({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  style: ["normal", "italic"],
  variable: "--font-figtree",
  display: "swap",
});

/** Every label, axis, meta line and numeric table cell. */
const dmMono = DM_Mono({
  subsets: ["latin"],
  weight: ["400", "500"],
  variable: "--font-dm-mono",
  display: "swap",
});

/**
 * The one face of the Pizarra world (landing first, the other pages as they
 * move over): figures at 300, text at 400-500, headings at 600. Variable, so
 * one file covers every weight.
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
      className={`${outfit.variable} ${figtree.variable} ${dmMono.variable} ${hanken.variable}`}
    >
      <body>{children}</body>
    </html>
  );
}
