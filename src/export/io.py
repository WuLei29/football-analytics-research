"""
io.py
-----
Writing side of the export: the file envelope, atomic writes, and the single
place rounding happens.

Spec: md/WEB_DATA.md §2 (envelope), §11 (behaviour).

Rounding is centralised on purpose. A coordinate rounded in one exporter and
not in another produces files that look fine and diff badly on every refresh.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .config import SCHEMA_VERSION

log = logging.getLogger("export")


# ---------------------------------------------------------------------------
# Rounding (§ "Rules that hold everywhere")
# ---------------------------------------------------------------------------

def r_coord(v: Any) -> float | None:
    """Pitch coordinate, metres, 0.1 m."""
    return None if v is None else round(float(v), 1)


def r_value(v: Any) -> float | None:
    """xT / VAEP, 3 decimals."""
    return None if v is None else round(float(v), 3)


def r_xg(v: Any) -> float | None:
    """A single match's xG, 2 decimals -- the design reads "2.11 xG".

    Season xG totals use r_rate: 47.6 over 38 matches, where a second decimal
    is noise.
    """
    return None if v is None else round(float(v), 2)


def r_rate(v: Any) -> float | None:
    """Percentage or per-match rate, 1 decimal."""
    return None if v is None else round(float(v), 1)


def r(v: Any, decimals: int) -> float | None:
    """Explicit precision, for the metric registries that carry their own."""
    if v is None:
        return None
    out = round(float(v), decimals)
    return int(out) if decimals == 0 else out


def as_int(v: Any) -> int | None:
    return None if v is None else int(v)


# ---------------------------------------------------------------------------
# Envelope and writing
# ---------------------------------------------------------------------------

def envelope(payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    """Wrap a payload in the fields every published file carries (§2)."""
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        **payload,
    }


class _Encoder(json.JSONEncoder):
    """Dates as ISO strings, Decimal as float. Anything else is a bug."""

    def default(self, o: Any) -> Any:
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


class Writer:
    """Writes the tree under `root`. `dry_run` reports without touching disk."""

    def __init__(self, root: Path, dry_run: bool = False) -> None:
        self.root = root
        self.dry_run = dry_run
        self.written: list[tuple[str, int]] = []

    def write(self, rel_path: str, payload: dict[str, Any], generated_at: datetime) -> None:
        body = json.dumps(
            envelope(payload, generated_at),
            cls=_Encoder,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        size = len(body.encode("utf-8"))
        self.written.append((rel_path, size))

        if self.dry_run:
            return

        target = self.root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        # Write beside the target and rename, so an interrupted run never
        # leaves half a JSON file in web/public/ (§11).
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(body, encoding="utf-8")
        os.replace(tmp, target)

    def report(self) -> None:
        total = sum(size for _, size in self.written)
        verb = "would write" if self.dry_run else "wrote"
        for path, size in self.written:
            log.info("  %-58s %8.1f KB", path, size / 1024)
        log.info("%s %d files, %.1f KB total", verb, len(self.written), total / 1024)
