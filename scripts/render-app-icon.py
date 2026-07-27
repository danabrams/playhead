#!/usr/bin/env python3
"""Render the Playhead app icon (playhead-4hv).

Concept: a dark Ink field with a single off-center Copper playhead line —
the brand motif at its most distilled. No gradients, no shimmer, no
literal podcast furniture (no microphones, no headphones), no purple.

WHY A SCRIPT AND NOT A CHECKED-IN BINARY:
The icon is two rectangles. Keeping the geometry in code means the whole
family regenerates byte-identically from one set of numbers, and the
per-size pixel snapping (the part that actually decides whether the line
survives at 40 px) is explicit and reviewable rather than baked into a
PNG nobody can re-derive.

WHY PIL AND NOT AN SVG RASTERIZER:
This box has no `rsvg-convert`, no `inkscape`, and no `cairosvg`. Rather
than add a dependency for a rectangle and a capsule, the geometry lives
here and `design/icon/playhead-icon.svg` is *emitted* from the same
constants — so the SVG is a true design record that cannot drift from
the shipped PNGs.

Outputs (all regenerated in one pass):
  Playhead/Resources/Assets.xcassets/AppIcon.appiconset/app-icon-1024.png
      the shipping icon; Xcode 27 auto-scales every runtime size from it
  design/icon/playhead-icon.svg
      vector source of record, emitted from the constants below
  design/icon/renders/playhead-icon-<size>.png
      per-size reference renders with the line snapped to whole pixels,
      one per acceptance size; also drop-in ready if we ever move the
      asset catalog to explicit per-size slots
  design/icon/contact-sheet.png
      every size at 1:1 under an iOS-like squircle mask, for eyeballing

Usage:
  python3 scripts/render-app-icon.py            # write all outputs
  python3 scripts/render-app-icon.py --check    # verify committed bytes
  python3 scripts/render-app-icon.py --report   # print the size table
"""

from __future__ import annotations

import argparse
import hashlib
import io
import math
import os
import sys

from PIL import Image, ImageCms, ImageDraw

# --------------------------------------------------------------------------
# Design tokens.
#
# These are the canonical values from Playhead/Design/Colors.swift
# (`Palette.ink` = 0x0E1116, `Palette.copper` = 0x C96A3D), which is the
# source of truth established by the design-tokens bead (playhead-8bb).
# They happen to match the icon spec in playhead-4hv exactly.
# --------------------------------------------------------------------------
INK = (0x0E, 0x11, 0x16)
COPPER = (0xC9, 0x6A, 0x3D)

# Geometry, as fractions of the icon's side length.
LINE_CENTER_X_FRAC = 1.0 / 3.0  # "~1/3 from the left edge"
LINE_WIDTH_FRAC = 0.03  # "~3% of width"
LINE_TOP_FRAC = 0.12
LINE_BOTTOM_FRAC = 0.88

# Legibility floor. See `line_geometry` for the reasoning.
MIN_LINE_WIDTH_PX = 2

# The acceptance sizes from playhead-4hv, largest first.
ACCEPTANCE_SIZES = [1024, 180, 120, 87, 80, 60, 58, 40]

CATALOG_SIZE = 1024

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_PNG = os.path.join(
    REPO_ROOT,
    "Playhead/Resources/Assets.xcassets/AppIcon.appiconset/app-icon-1024.png",
)
DESIGN_DIR = os.path.join(REPO_ROOT, "design", "icon")
SVG_PATH = os.path.join(DESIGN_DIR, "playhead-icon.svg")
RENDER_DIR = os.path.join(DESIGN_DIR, "renders")
CONTACT_SHEET = os.path.join(DESIGN_DIR, "contact-sheet.png")


def _round_half_up(value: float) -> int:
    """Round half away from zero.

    Python's built-in `round` is banker's rounding, which would make the
    snapped geometry depend on whether a coordinate's integer part is even.
    For an asset that must regenerate byte-identically, predictable beats
    statistically-unbiased.
    """
    return int(math.floor(value + 0.5))


def line_geometry(size: int) -> dict:
    """Whole-pixel geometry for the copper line at a given icon size.

    Everything is snapped to integers so the line's long edges land exactly
    on pixel boundaries — no antialiasing down the shaft, at any size. Only
    the rounded caps are allowed to blend.

    The width floor is the one deliberate departure from the literal spec.
    At 40 px, 3% is 1.2 px: it rounds to a single pixel column at 2.5% of
    the width, and any resampling the OS does on top of that (Spotlight
    rows, notification stacks) smears it toward the background until the
    line reads as a smudge. A 2 px minimum keeps the line a solid, fully
    saturated copper mark at every size. It reads as the same hairline —
    below roughly 2 device pixels, perceived weight collapses faster than
    geometric weight does.
    """
    width = max(MIN_LINE_WIDTH_PX, _round_half_up(LINE_WIDTH_FRAC * size))
    x0 = _round_half_up(size * LINE_CENTER_X_FRAC - width / 2.0)
    x1 = x0 + width
    y0 = _round_half_up(size * LINE_TOP_FRAC)
    y1 = _round_half_up(size * LINE_BOTTOM_FRAC)
    return {
        "size": size,
        "width": width,
        "x0": x0,
        "x1": x1,
        "y0": y0,
        "y1": y1,
        "height": y1 - y0,
        "center_x": (x0 + x1) / 2.0,
        "center_frac": (x0 + x1) / 2.0 / size,
        "width_frac": width / size,
    }


def _srgb_profile() -> bytes:
    """sRGB ICC profile bytes, embedded in every PNG we write.

    App Store icon validation cares about two things we can control here:
    no alpha channel, and a sane RGB color space. Embedding sRGB explicitly
    means the answer is recorded in the file rather than inferred.

    The two zeroed spans are not cosmetic. littlecms stamps the ICC header
    with the current wall-clock time (the 12-byte dateTime at offset 24)
    and reserves a profile-id digest right after it (16 bytes at offset
    84). Left alone, those make the PNG bytes differ on every run, so
    re-rendering an unchanged icon would produce a diff and `--check`
    would fail for no reason. Both fields are optional per the ICC spec
    and nothing in the toolchain reads them.
    """
    raw = bytearray(ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes())
    raw[24:36] = b"\x00" * 12
    raw[84:100] = b"\x00" * 16
    return bytes(raw)


def _supersample_factor(size: int) -> int:
    """Antialiasing factor for the rounded caps.

    Capped so the intermediate buffer stays small (<= 4096 on a side). The
    shaft edges are integer multiples of the factor, so a BOX (exact area
    average) downsample reproduces them as hard edges regardless.
    """
    return min(16, max(4, 4096 // size))


def render_icon(size: int) -> Image.Image:
    """Render one flat icon at `size` x `size`, RGB (never RGBA)."""
    geo = line_geometry(size)
    factor = _supersample_factor(size)
    big = Image.new("RGB", (size * factor, size * factor), INK)
    draw = ImageDraw.Draw(big)
    radius = geo["width"] * factor / 2.0
    draw.rounded_rectangle(
        [
            geo["x0"] * factor,
            geo["y0"] * factor,
            geo["x1"] * factor - 1,
            geo["y1"] * factor - 1,
        ],
        radius=radius,
        fill=COPPER,
    )
    return big.resize((size, size), Image.BOX)


def png_bytes(image: Image.Image) -> bytes:
    """Serialize deterministically: RGB, sRGB profile, no ancillary chunks."""
    assert image.mode == "RGB", f"icon must be RGB, got {image.mode}"
    buf = io.BytesIO()
    image.save(buf, "PNG", icc_profile=_srgb_profile(), optimize=True)
    return buf.getvalue()


def svg_source() -> str:
    """Emit the vector source of record from the same constants.

    Rendered at the 1024 grid so the SVG and the shipping PNG describe the
    identical shape; the viewBox makes it resolution independent.
    """
    geo = line_geometry(CATALOG_SIZE)
    radius = geo["width"] / 2.0
    ink = "#%02X%02X%02X" % INK
    copper = "#%02X%02X%02X" % COPPER
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<!-- Playhead app icon (playhead-4hv). Generated by\n"
        "     scripts/render-app-icon.py — edit the constants there, not\n"
        "     this file, then re-run the script. -->\n"
        '<svg xmlns="http://www.w3.org/2000/svg" '
        'width="{s}" height="{s}" viewBox="0 0 {s} {s}" '
        'role="img" aria-label="Playhead">\n'
        "  <title>Playhead</title>\n"
        '  <rect width="{s}" height="{s}" fill="{ink}"/>\n'
        '  <rect x="{x}" y="{y}" width="{w}" height="{h}" '
        'rx="{r}" ry="{r}" fill="{copper}"/>\n'
        "</svg>\n"
    ).format(
        s=CATALOG_SIZE,
        ink=ink,
        copper=copper,
        x=geo["x0"],
        y=geo["y0"],
        w=geo["width"],
        h=geo["height"],
        r=_fmt(radius),
    )


def _fmt(value: float) -> str:
    return ("%.1f" % value).rstrip("0").rstrip(".")


def _squircle_mask(size: int, exponent: float = 5.0) -> Image.Image:
    """An iOS-like superellipse mask, for preview only.

    Never applied to a shipped asset — iOS masks the icon itself, and
    pre-masking would bake the wrong corner shape into the artwork.
    """
    # Its own (smaller) factor: this is a pure-Python per-pixel loop and the
    # render supersample factor would make it quadratically slow for no
    # visible gain on a preview-only mask.
    factor = min(8, max(4, 512 // size))
    n = size * factor
    mask = Image.new("L", (n, n), 0)
    px = mask.load()
    half = n / 2.0
    for y in range(n):
        ny = abs((y + 0.5 - half) / half) ** exponent
        if ny >= 1.0:
            continue
        span = (1.0 - ny) ** (1.0 / exponent) * half
        lo = int(math.floor(half - span))
        hi = int(math.ceil(half + span))
        for x in range(max(0, lo), min(n, hi)):
            if abs((x + 0.5 - half) / half) ** exponent + ny < 1.0:
                px[x, y] = 255
    return mask.resize((size, size), Image.BOX)


def contact_sheet() -> Image.Image:
    """Every acceptance size at 1:1, masked, on a neutral field."""
    preview = 256
    pad = 24
    gap = 20
    tiles = [(preview, render_icon(preview))] + [
        (s, render_icon(s)) for s in ACCEPTANCE_SIZES if s != CATALOG_SIZE
    ]
    width = pad * 2 + sum(t[0] for t in tiles) + gap * (len(tiles) - 1)
    height = pad * 2 + preview
    sheet = Image.new("RGB", (width, height), (0x3A, 0x3D, 0x42))
    x = pad
    for size, tile in tiles:
        masked = Image.new("RGB", (size, size), (0x3A, 0x3D, 0x42))
        masked.paste(tile, (0, 0), _squircle_mask(size))
        sheet.paste(masked, (x, pad + preview - size))
        x += size + gap
    return sheet


# --------------------------------------------------------------------------
# Measurement — report what the pixels actually are, not what we intended.
# --------------------------------------------------------------------------


def measure(image: Image.Image) -> dict:
    """Measure the rendered line straight off the pixels."""
    size = image.width
    mid = size // 2
    row = [image.getpixel((x, mid)) for x in range(size)]

    def coverage(px):
        # Fraction of copper in this pixel, from the red channel (the
        # channel with the widest ink->copper separation).
        return (px[0] - INK[0]) / float(COPPER[0] - INK[0])

    cov = [coverage(p) for p in row]
    full = [i for i, c in enumerate(cov) if c > 0.999]
    partial = [i for i, c in enumerate(cov) if 0.02 < c <= 0.999]
    lit = [i for i, c in enumerate(cov) if c > 0.02]
    return {
        "size": size,
        "full_px": len(full),
        "partial_px": len(partial),
        "peak_coverage": max(cov) if cov else 0.0,
        "first_lit": min(lit) if lit else None,
        "last_lit": max(lit) if lit else None,
        "center_frac": ((min(lit) + max(lit) + 1) / 2.0 / size) if lit else None,
        "bg": row[0],
    }


def report(catalog: Image.Image) -> None:
    print("size  linePx  x0..x1   center%   yTop..yBot  height")
    for s in ACCEPTANCE_SIZES:
        g = line_geometry(s)
        print(
            "%4d  %5dpx  %3d..%-3d  %6.2f%%   %4d..%-4d  %5d"
            % (
                s,
                g["width"],
                g["x0"],
                g["x1"],
                g["center_frac"] * 100,
                g["y0"],
                g["y1"],
                g["height"],
            )
        )

    print()
    print("native renders (measured off the pixels, mid-height scanline):")
    print("size  fullPx  partialPx  peakCoverage  centerMeasured")
    for s in ACCEPTANCE_SIZES:
        m = measure(render_icon(s))
        print(
            "%4d  %6d  %9d  %11.3f  %13.2f%%"
            % (
                s,
                m["full_px"],
                m["partial_px"],
                m["peak_coverage"],
                m["center_frac"] * 100,
            )
        )

    print()
    print("what Xcode produces instead (1024 downsampled, as actool does):")
    print("size  fullPx  partialPx  peakCoverage")
    for s in ACCEPTANCE_SIZES:
        if s == CATALOG_SIZE:
            continue
        for label, filt in (("lanczos", Image.LANCZOS), ("box", Image.BOX)):
            m = measure(catalog.resize((s, s), filt))
            print(
                "%4d  %6d  %9d  %11.3f  (%s)"
                % (s, m["full_px"], m["partial_px"], m["peak_coverage"], label)
            )


# --------------------------------------------------------------------------
# Write / check
# --------------------------------------------------------------------------


def _outputs() -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    catalog = render_icon(CATALOG_SIZE)
    out.append((CATALOG_PNG, png_bytes(catalog)))
    out.append((SVG_PATH, svg_source().encode("utf-8")))
    for s in ACCEPTANCE_SIZES:
        image = catalog if s == CATALOG_SIZE else render_icon(s)
        out.append(
            (os.path.join(RENDER_DIR, "playhead-icon-%d.png" % s), png_bytes(image))
        )
    out.append((CONTACT_SHEET, png_bytes(contact_sheet())))
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the committed files match what this script produces",
    )
    parser.add_argument(
        "--report", action="store_true", help="print the size/measurement table"
    )
    args = parser.parse_args(argv)

    outputs = _outputs()

    if args.report:
        report(render_icon(CATALOG_SIZE))
        return 0

    if args.check:
        bad = []
        for path, data in outputs:
            rel = os.path.relpath(path, REPO_ROOT)
            if not os.path.exists(path):
                bad.append("%s: missing" % rel)
                continue
            with open(path, "rb") as handle:
                have = handle.read()
            if have != data:
                bad.append(
                    "%s: differs (on disk %s, expected %s)"
                    % (
                        rel,
                        hashlib.sha256(have).hexdigest()[:12],
                        hashlib.sha256(data).hexdigest()[:12],
                    )
                )
        if bad:
            print("app icon is out of date; re-run scripts/render-app-icon.py")
            for line in bad:
                print("  " + line)
            return 1
        print("app icon assets match the source geometry (%d files)" % len(outputs))
        return 0

    for path, data in outputs:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(data)
        print(
            "wrote %s (%d bytes, sha256 %s)"
            % (
                os.path.relpath(path, REPO_ROOT),
                len(data),
                hashlib.sha256(data).hexdigest()[:12],
            )
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
