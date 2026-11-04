"""
player_photos.py
----------------
Prepares the player photos the site serves, from the author's originals.

    python -m src.export.player_photos

Reads  web/resources/players/{player_id}.{png,jpg,jpeg,webp}  (as supplied)
Writes web/public/players/{player_id}.jpg  (what the site serves)

The originals mix transparent cutouts and photos on a white ground, from 400 px
to 2048 px and up to 3 MB. Each one is flattened onto white, so the two kinds
look the same on the card; trimmed to the player, so a wide margin does not make
one player look smaller than the next; and scaled to fit PHOTO_MAX_PX, twice the
size of the card's photo block. Small originals are not scaled up. The originals
are gitignored (48 MB); only the served copies are committed.

Not part of `python -m src.export`: the photos change when the author supplies a
file, not when the data does. A player without a file keeps the shirt-number
placeholder (components/player/PlayerRoute.tsx looks for the served file). Needs
Pillow, which is not a pipeline dependency; it is in the project venv.
"""

from __future__ import annotations

import sys

from PIL import Image, ImageChops, ImageOps

from .config import PROJECT_ROOT

SOURCE_DIR = PROJECT_ROOT / "web" / "resources" / "players"
PHOTO_DIR = PROJECT_ROOT / "web" / "public" / "players"
SOURCE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp"}
PHOTO_MAX_PX = (800, 1100)
JPEG_QUALITY = 85
# How far from pure white a channel must be to count as the player. Above zero
# because a JPEG's white ground is only nearly white.
WHITE_TOLERANCE = 16


def prepare(src, dst) -> tuple[int, int]:
    with Image.open(src) as original:
        im = ImageOps.exif_transpose(original).convert("RGBA")
    ground = Image.new("RGBA", im.size, "white")
    flat = Image.alpha_composite(ground, im).convert("RGB")

    distance = ImageChops.difference(flat, Image.new("RGB", flat.size, "white")).convert("L")
    box = distance.point(lambda v: 255 if v > WHITE_TOLERANCE else 0).getbbox()
    if box is None:
        raise ValueError(f"{src.name}: nothing but white")

    flat = flat.crop(box)
    flat.thumbnail(PHOTO_MAX_PX, Image.LANCZOS)
    flat.save(dst, quality=JPEG_QUALITY, optimize=True, progressive=True)
    return flat.size


def main() -> int:
    sources: dict[str, object] = {}
    for src in sorted(SOURCE_DIR.iterdir()):
        if src.suffix.lower() not in SOURCE_SUFFIXES:
            continue
        if not src.stem.isdigit():
            print(f"  skip {src.name}: the file name must be the player_id")
            continue
        if src.stem in sources:
            raise ValueError(f"two photos for player {src.stem}: {sources[src.stem].name}, {src.name}")
        sources[src.stem] = src

    PHOTO_DIR.mkdir(parents=True, exist_ok=True)
    for player_id, src in sources.items():
        dst = PHOTO_DIR / f"{player_id}.jpg"
        w, h = prepare(src, dst)
        print(f"  {src.name:12} {src.stat().st_size // 1024:5} KB -> {w}x{h} {dst.stat().st_size // 1024:3} KB")
    print(f"  {len(sources)} photos")
    return 0


if __name__ == "__main__":
    sys.exit(main())
