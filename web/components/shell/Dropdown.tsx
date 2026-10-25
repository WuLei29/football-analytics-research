"use client";

/**
 * Dropdown — a trigger button and the panel it opens, for the two menus of
 * screen 03 (the player picker and the match filter).
 *
 * Renders: the trigger as a Pizarra tone pill (panel-deep, hover chalk,
 * 14px/600, a chevron), and under it an absolutely positioned white panel
 * (row radius, a hairline ink ring, no shadow: DESIGN.md's two raised things
 * are the nav and the figure cards) with the caller's content. Pizarra
 * since 24 Oct 2026, with screen 03.
 * Props: `label` (the accessible name), `trigger` (its visible content),
 *        `children` (the panel; a function receives `close`), `align`,
 *        `width` of the panel in px.
 * Data:  none.
 *
 * Why not a native `<select>` like `MatchPicker`: the player rows draw a
 * shirt, and the match filter is a multi-select with presets — neither fits
 * an `<option>`. What the native control gave for free is rebuilt here, and
 * no more: Escape and a click outside close the panel, focus returns to the
 * trigger, and `aria-expanded` says whether it is open.
 */

import { useEffect, useRef, useState, type ReactNode } from "react";

export function Dropdown({
  label,
  trigger,
  children,
  align = "left",
  width = 300,
}: {
  label: string;
  trigger: ReactNode;
  children: ReactNode | ((close: () => void) => ReactNode);
  align?: "left" | "right";
  width?: number;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;
    const onPointer = (e: PointerEvent) => {
      if (!root.current?.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setOpen(false);
        button.current?.focus();
      }
    };
    document.addEventListener("pointerdown", onPointer);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("pointerdown", onPointer);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const close = () => setOpen(false);

  return (
    <div ref={root} className="relative">
      <button
        ref={button}
        type="button"
        aria-label={label}
        aria-expanded={open}
        aria-haspopup="true"
        onClick={() => setOpen((o) => !o)}
        className={`flex max-w-full cursor-pointer items-center gap-2 rounded-full py-2 pr-4 pl-3 text-[14px] font-semibold text-ink transition-colors hover:bg-chalk ${
          open ? "bg-chalk" : "bg-panel-deep"
        }`}
      >
        {trigger}
        <svg
          aria-hidden
          viewBox="0 0 10 6"
          width="10"
          height="6"
          className="ml-1 shrink-0 text-slate"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.3"
          strokeLinecap="round"
          style={{ transform: open ? "rotate(180deg)" : undefined }}
        >
          <path d="M1 1l4 4 4-4" />
        </svg>
      </button>

      {open && (
        <div
          className="absolute z-30 mt-2 overflow-hidden rounded-row bg-white ring-1 ring-ink/[0.12]"
          style={{
            width,
            maxWidth: "calc(100vw - 32px)",
            [align === "left" ? "left" : "right"]: 0,
          }}
        >
          {typeof children === "function" ? children(close) : children}
        </div>
      )}
    </div>
  );
}
