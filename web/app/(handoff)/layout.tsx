/**
 * (handoff)/layout.tsx — the frame of every page still drawn in the design
 * handoff's visual world: the season, match list, match, squad and player
 * pages, and `/demo`.
 *
 * Renders: the handoff's page padding (34px top, 40px sides, 60px bottom,
 * centred at 1440px) around the page.
 * Props: `children` — the page.
 *
 * WHY A ROUTE GROUP. The landing moved to the new "Pizarra" world (24 Oct
 * 2026, `.impeccable/surfaces/web-app-page-tsx.md`), which needs the stadium
 * photo to run edge to edge, so the padding could no longer live on <body>.
 * The parentheses make `(handoff)` invisible in the URL: `/espanyol/...` is
 * unchanged. As each page moves to the new world it moves out of this folder,
 * and when the folder is empty this file goes with it.
 */

export default function HandoffLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="mx-auto max-w-[1440px] px-5 pt-6 pb-14 md:px-10 md:pt-[34px] md:pb-[60px]">
      {children}
    </div>
  );
}
