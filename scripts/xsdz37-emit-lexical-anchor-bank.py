#!/usr/bin/env python3
"""playhead-xsdz.37: emit the curated LexicalAnchorBank.json bundle resource.

Compiles the two VALIDATED anchor families from the offline prototype's GO
report (playhead-baselines/xsdz37-lexical-prototype-20260716.md) into the
bundled bank the runtime `LexicalAnchorRefiner` consumes:

  Family (a) — per-show READ-ONSET ATTRIBUTION templates. For each GO'd show
    (On The Media, Radiolab — the two shows the metadata bank compiled for and
    that anchored 3/3 gold breaks at onset with +0.7..+1.3s error), the entity
    set {show name} + station names is crossed with the four sponsorship verbs
    {brought to you by, sponsored by, supported by, presented by} plus the
    public-radio inversion "support for <entity> comes from". EXACT-VERB only:
    the report's one false positive was a fuzzy "produced by" ~ "sponsored by"
    collision that exact matching kills at zero cost to edge hits.

  Family (b) — GENERIC (all-show) break-edge FRAMING phrases: the core pair the
    report GO'd (pre = "we'll be right back"/"we will be right back"; resume =
    "and now back to the show"/"and back to the show"). The rejected 2-word /
    fuzzy resume traps ("we're back" 0/4, "welcome back" 0/4, "welcome back to
    the show" 0/4 — the near-antonym trap) are deliberately dropped.

Offsets are the report's measured onset/resume deltas, expressed as the offset
applied AFTER the matched-phrase position (the first matched word's start) so
the snapped edge lands on the true break edge:
  • family (a): read onset trails the gold start by ~+1.0s → offset -1.0.
  • family (b) pre: the phrase finishes just before the cut, starting ~2.0s
    before the gold start → offset +2.0.
  • family (b) resume: the resume phrase starts ~0.6s after the gold end →
    offset -0.6.
`confidence`/`support` are CURATED priors for these exact templates (exact
matching is the precision gate; the refiner does not gate on them in this cut),
not corpus frequencies.

Deterministic output (sorted shows/entities, fixed verb order) so the hermetic
pin in `LexicalAnchorBankTests` catches any drift. Writes only its resource.

Usage:
  python3 scripts/xsdz37-emit-lexical-anchor-bank.py            # write resource
  python3 scripts/xsdz37-emit-lexical-anchor-bank.py --stdout   # preview
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_OUT = REPO_ROOT / "Playhead/Resources/LexicalAnchorBank.json"

SCHEMA_VERSION = 1

# Curated per-family offsets (seconds). See the module docstring for the
# derivation from the report's measured deltas.
FAMILY_A_OFFSET = -1.0
FAMILY_B_PRE_OFFSET = 2.0
FAMILY_B_RESUME_OFFSET = -0.6

# Curated priors for exact templates (exact match is the precision gate).
CURATED_CONFIDENCE = 0.9
CURATED_SUPPORT = 2

SPONSOR_VERBS = (
    "brought to you by",
    "sponsored by",
    "supported by",
    "presented by",
)
INVERTED_FORMAT = "support for {entity} comes from"

# GO'd family-(a) shows. `keys` = corpus slug (measurement join key) + the
# production RSS feed URL alias (runtime join key; verify before the production
# flip). `entities` = the show-name + station entities the attribution
# templates are crossed over.
SHOWS = [
    {
        "showName": "On The Media",
        # OTM feed reused verbatim from the shipped StingerBank alias.
        "showKeys": ["on-the-media", "https://feeds.simplecast.com/o4jAFXaw"],
        "entities": ["On The Media", "WNYC", "WNYC Studios"],
    },
    {
        "showName": "Radiolab",
        # Radiolab is the other WNYC Studios production in the corpus.
        "showKeys": ["radiolab", "https://feeds.simplecast.com/EmVW7VGp"],
        "entities": ["Radiolab", "WNYC", "WNYC Studios"],
    },
]

GENERIC_PRE = ("we'll be right back", "we will be right back")
GENERIC_RESUME = ("and now back to the show", "and back to the show")

_APOSTROPHES = "'’‘ʼ`´"
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_word(raw: str) -> str:
    text = raw.lower()
    for ch in _APOSTROPHES:
        text = text.replace(ch, "")
    return _NON_ALNUM_RE.sub("", text)


def normalize_phrase(raw: str) -> tuple[str, ...]:
    return tuple(w for w in (normalize_word(p) for p in raw.split()) if w)


def anchor(phrase: str, side: str, offset: float) -> dict:
    return {
        "phrase": phrase,
        "side": side,
        "matchPolicy": "exact",
        "edgeOffsetSeconds": offset,
        "confidence": CURATED_CONFIDENCE,
        "support": CURATED_SUPPORT,
    }


def show_anchors(entities: list[str]) -> list[dict]:
    """Family-(a) templates: entities x verbs + inversion, deterministic order.

    Entities are ordered by their normalized token tuple (matches the offline
    prototype's `sorted(entities.items())`); within an entity the four verbs
    come first (fixed order) then the inversion.
    """
    ordered = sorted({normalize_phrase(e): e for e in entities}.items())
    anchors: list[dict] = []
    seen: set[tuple[str, ...]] = set()
    for _key, display in ordered:
        for verb in SPONSOR_VERBS:
            phrase = f"{display} is {verb}"
            if normalize_phrase(phrase) not in seen:
                seen.add(normalize_phrase(phrase))
                anchors.append(anchor(phrase, "pre", FAMILY_A_OFFSET))
        inverted = INVERTED_FORMAT.format(entity=display)
        if normalize_phrase(inverted) not in seen:
            seen.add(normalize_phrase(inverted))
            anchors.append(anchor(inverted, "pre", FAMILY_A_OFFSET))
    return anchors


def build_bank() -> dict:
    shows = [
        {
            "showKeys": show["showKeys"],
            "showName": show["showName"],
            "anchors": show_anchors(show["entities"]),
        }
        for show in sorted(SHOWS, key=lambda s: s["showName"])
    ]
    generic = [anchor(p, "pre", FAMILY_B_PRE_OFFSET) for p in GENERIC_PRE]
    generic += [anchor(p, "post", FAMILY_B_RESUME_OFFSET) for p in GENERIC_RESUME]
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generator": "scripts/xsdz37-emit-lexical-anchor-bank.py",
        "provenance": "playhead-baselines/xsdz37-lexical-prototype-20260716.md (GO verdict)",
        "shows": shows,
        "genericAnchors": generic,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, default=DEFAULT_OUT)
    parser.add_argument("--stdout", action="store_true", help="preview, do not write")
    args = parser.parse_args(argv)

    bank = build_bank()
    text = json.dumps(bank, indent=2) + "\n"
    if args.stdout:
        print(text, end="")
        return 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text, encoding="utf-8")
    print(f"wrote {args.out} ({len(bank['shows'])} shows, "
          f"{sum(len(s['anchors']) for s in bank['shows'])} family-a anchors, "
          f"{len(bank['genericAnchors'])} generic anchors)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
