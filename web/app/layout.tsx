/**
 * layout.tsx — the root layout: the <html> document every page is rendered
 * into, the three fonts, and the page padding of the design.
 *
 * Renders: <html lang="es"> with the font variables attached, and a <body>
 * whose only job is the 34px/40px/60px page padding from the handoff.
 * Props: `children` — the page.
 *
 * The three faces are loaded with `next/font/google`, which downloads them at
 * BUILD time and self-hosts the files. Nothing is fetched from Google when a
 * reader opens the site: no third-party request, no layout shift while a font
 * swaps in. Each face exposes a CSS variable, and `app/globals.css` maps those
 * onto `--font-display` / `--font-sans` / `--font-mono`.
 */

import type { Metadata } from "next";
import { DM_Mono, Figtree, Outfit } from "next/font/google";

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
      className={`${outfit.variable} ${figtree.variable} ${dmMono.variable}`}
    >
      {/* Page padding from the handoff: 34px top, 40px sides, 60px bottom.
          Desktop-first and fluid, as the design is (WEB_PLAN.md §3.1); the
          mobile collapse pass is Phase 6. */}
      <body className="mx-auto max-w-[1440px] px-5 pt-6 pb-14 md:px-10 md:pt-[34px] md:pb-[60px]">
        {children}
      </body>
    </html>
  );
}
