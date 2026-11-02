"""
crests.py
---------
Prepares the club crests the site serves, from the author's originals.

    python -m src.export.crests

Reads  web/resources/crests/{abbr}.png  (the files as supplied, any size)
Writes web/public/brand/crests/{abbr}.png  (what the site serves)

Each crest is trimmed to its visible pixels (several originals carry wide
transparent margins, which would make one crest look smaller than its
neighbour at the same box size) and scaled so its longer side is CREST_PX,
four times the largest size the site draws (32 px in the match header at 2x
density is 64; 128 leaves room). Aspect ratio is kept; the site centres each
crest in a square box.

Not part of `python -m src.export`: the crests change when the author
supplies a file, not when the data does. The export only checks which files
exist (config.club_identity). Needs Pillow, which is not a pipeline
dependency; it is in the project venv.

The file name is the lower-case silver.teams.abbreviation (`mll.png` for
Mallorca, `ray.png` for Rayo), the key every data file ships as `abbr`.
"""

from __future__ import annotations

import sys

from PIL import Image

from .config import CLUB_COLOURS, CREST_DIR, PROJECT_ROOT

SOURCE_DIR = PROJECT_ROOT / "web" / "resources" / "crests"
CREST_PX = 128


def prepare(src, dst) -> tuple[int, int]:
    im = Image.open(src).convert("RGBA")
    box = im.getchannel("A").getbbox()
    if box:
        im = im.crop(box)
    im.thumbnail((CREST_PX, CREST_PX), Image.LANCZOS)
    im.save(dst, optimize=True)
    return im.size


def main() -> int:
    CREST_DIR.mkdir(parents=True, exist_ok=True)
    known = {abbr.lower() for abbr in CLUB_COLOURS}
    found = set()

    for src in sorted(SOURCE_DIR.glob("*.png")):
        abbr = src.stem.lower()
        if abbr not in known:
            print(f"  skip {src.name}: '{abbr.upper()}' is not a club in CLUB_COLOURS")
            continue
        dst = CREST_DIR / f"{abbr}.png"
        w, h = prepare(src, dst)
        found.add(abbr)
        print(f"  {src.name:10} {src.stat().st_size // 1024:5} KB -> {w}x{h} {dst.stat().st_size // 1024:3} KB")

    missing = sorted(known - found)
    if missing:
        print(f"  no crest (the site draws the colour chip): {', '.join(m.upper() for m in missing)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
