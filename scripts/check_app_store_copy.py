#!/usr/bin/env python3
"""Check the App Store listing copy against App Store Connect's field limits.

playhead-jw63.2. The limits are EXACT and enforced at submission: a subtitle of
31 characters is rejected, and the rejection arrives after an archive, an
upload and a wait. Hand-counting is how a listing goes over — this repo's
standing defect class is a value that names one thing read as though it named
another, and "about thirty characters" is that in miniature.

It also enforces the two external-copy rules, because they are the reason the
copy reads as it does and they are easy to lose in an edit six months from now:

  * "ad detection" and "AI" never appear in copy a listener reads. The KEYWORD
    field is exempt and that exemption is deliberate — see the document's own
    Rule 4. Keywords are the search index, not prose.
  * The listing never names a competing app. The competitor is the 30-second
    skip button.

Usage:
    python3 scripts/check_app_store_copy.py [docs/app-store-listing.md]

Exit 0 clean, 1 with every violation named, 2 if the document cannot be parsed.
"""

from __future__ import annotations

import pathlib
import re
import sys

# App Store Connect, verified against the field editor 2026-09-02.
LIMITS = {
    "App name": 30,
    "Subtitle": 30,
    "Promotional text": 170,
    "Keywords": 100,
    "Description": 4000,
    "What's New": 4000,
}

CAPTION_LIMIT = 60

# Banned in anything a listener reads. Matched case-insensitively, on word
# boundaries so "maintain" does not trip "AI".
BANNED_PROSE = [
    (r"\bad detection\b", "'ad detection' — sell the relief, not the mechanism"),
    (r"\bAI\b", "'AI' — Dan's standing external-copy rule"),
    (r"\bbyte[- ]align", "'byte alignment' — internal vocabulary"),
    (r"\bmachine learning\b", "'machine learning' — same rule as AI"),
]

# Naming a competitor breaks the framing: the competitor is the +30s button.
COMPETITORS = ["overcast", "pocket casts", "castro", "snipd", "spotify", "apple podcasts"]

# The claim the whole listing turns on. If it is gone, the listing has lost the
# one thing no competitor says.
REQUIRED_PHRASE = "only skips what it"


def parse(text: str) -> tuple[dict[str, str], list[str]]:
    """Return {field: body} for fenced blocks under `### <Field> — limit N`,
    plus every screenshot caption found in the table."""
    fields: dict[str, str] = {}
    for match in re.finditer(
        r"^###\s+(?P<name>[^\n—]+?)\s*—[^\n]*\n(?P<rest>.*?)(?=^##|\Z)",
        text,
        re.M | re.S,
    ):
        name = match.group("name").strip()
        block = re.search(r"^```\n(?P<body>.*?)^```", match.group("rest"), re.M | re.S)
        if block:
            fields[name] = block.group("body").rstrip("\n")

    # Captions come from the SCREENSHOTS section only. A document-wide search
    # for a backticked table cell also matches the bead ids in the closing
    # checklist, and reports them as over-long captions or, worse, as captions
    # that are fine — a checker that names one population while counting
    # another is the defect this script exists to prevent, committed by the
    # script. Observed: it reported 10 captions for a table holding 6.
    section = re.search(r"^##\s+Screenshots\s*$(?P<body>.*?)(?=^##\s|\Z)", text, re.M | re.S)
    captions = re.findall(r"\|\s*`([^`]+)`\s*\|", section.group("body")) if section else []
    return fields, captions


def main(argv: list[str]) -> int:
    path = pathlib.Path(argv[0]) if argv else pathlib.Path("docs/app-store-listing.md")
    if not path.exists():
        print(f"no such document: {path}", file=sys.stderr)
        return 2

    text = path.read_text(encoding="utf-8")
    fields, captions = parse(text)

    problems: list[str] = []

    missing = sorted(set(LIMITS) - set(fields))
    if missing:
        problems.append(f"document is missing a fenced block for: {', '.join(missing)}")

    for name, limit in LIMITS.items():
        body = fields.get(name)
        if body is None:
            continue
        length = len(body)
        status = "ok " if length <= limit else "OVER"
        print(f"  {status} {name:<20} {length:>5} / {limit}")
        if length > limit:
            problems.append(f"{name} is {length - limit} character(s) over its {limit} limit")

    for index, caption in enumerate(captions, start=1):
        length = len(caption)
        status = "ok " if length <= CAPTION_LIMIT else "OVER"
        print(f"  {status} caption {index:<12} {length:>5} / {CAPTION_LIMIT}")
        if length > CAPTION_LIMIT:
            problems.append(f"screenshot caption {index} is over {CAPTION_LIMIT}")
    if not captions:
        problems.append("no screenshot captions found — the table is the source for them")

    # Prose rules apply to every field a listener reads. Keywords are exempt by
    # design; an exemption that is not stated reads as an oversight, so it is
    # named here and in the document.
    prose = "\n".join(body for name, body in fields.items() if name != "Keywords")
    for pattern, why in BANNED_PROSE:
        if re.search(pattern, prose, re.I):
            problems.append(f"banned in listener-facing copy: {why}")

    for competitor in COMPETITORS:
        if re.search(rf"\b{re.escape(competitor)}\b", prose, re.I):
            problems.append(
                f"names a competitor ({competitor}) — the competitor is the 30-second skip button"
            )

    if REQUIRED_PHRASE not in prose:
        problems.append(
            "the trust claim is gone: no field says Playhead only skips what it is certain of"
        )

    print()
    if problems:
        print(f"{len(problems)} problem(s):")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("App Store copy is within every limit and obeys the copy rules.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
