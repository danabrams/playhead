#!/usr/bin/env python3
"""playhead-xsdz.37 offline prototype: per-show lexical anchors vs gold truth.

Measures two lexical anchor families from Dan's 2026-07-15 ear-audit
observations against the gold v2 evaluation:

  Family (a) — READ-ONSET ATTRIBUTION TEMPLATES
      "<station|show name> is {brought to you by|sponsored by|supported
      by|presented by} ..." (e.g. On The Media / WNYC: "WNYC Studios is
      supported by Wise"), plus the canonical public-radio inversion
      "support for <entity> comes from". Templates are COMPILED from show
      metadata (Snapshots manifest title + a small static station map) —
      never learned from the corpus, never tuned per break.

  Family (b) — BREAK-EDGE FRAMING PHRASES
      Host-spoken boundary framing (SmartLess: "we'll be right back" opens
      a break, "and now back to the show" resumes). A fixed generic phrase
      set applied identically to every show.

MATCHING RULE (documented, fixed for the whole run):
  1. Word stream: whisper token stream per transcript segment; tokens that
     start with a space begin a new word, others append; special tokens
     ("[_BEG_]" etc.) are skipped; words never span segments. Each word
     keeps its token-level start/end timestamps (absolute ms -> seconds).
  2. Normalisation: lowercase; unicode apostrophes folded then stripped;
     every char outside [a-z0-9] removed ("We'll" -> "well").
  3. A phrase (>= 2 normalised words) matches at word index i when the
     first transcript word anchors the first phrase word (equal, prefix of
     one another with >= 3 chars, or difflib ratio >= 0.75) AND some window
     of length n-1..n+2 words satisfies either
       - EXACT: length n and word-for-word equality, or
       - FUZZY: difflib.SequenceMatcher ratio of the space-stripped
         ("condensed") window vs the condensed phrase >= --fuzzy-ratio
         (default 0.85). Condensing makes ASR splits like "smart less" ==
         "smartless" free.
     Windows scoring in [--near-miss-floor, --fuzzy-ratio) are recorded as
     NEAR MISSES: reported next to unanchored gold edges as possible
     ASR-miss cases, flagged only, never counted as hits.
  4. Overlapping matches inside one family are merged; the longest phrase
     (then highest ratio) wins, so "and now back to the show" absorbs its
     "back to the show" substring.

SCORING vs gold (no per-break tuning anywhere):
  - onset hit  : anchor start within +-WINDOW (default 15 s) of a gold
                 break START (families a and b-pre).
  - resume hit : anchor start within +-WINDOW of a gold break END
                 (family b-resume).
  - in-break   : anchor inside a gold break but not an edge hit (family-a
                 mid-pod re-attributions are presence evidence, not onset).
  - false fire : anchor > CLEARANCE (default 30 s) from every gold break
                 interval, or inside a content veto (vetoes are definite
                 human-labeled content). Gold coverage is PARTIAL, so
                 non-veto "false" fires are really *uncorroborated* fires —
                 they may sit in unlabeled ads. Reported per labeled
                 episode hour with that caveat.

Inputs are frozen fixtures (read-only); the script writes only its report.

Usage:
  python3 scripts/l2f-lexical-anchor-prototype.py \
      --report-out /Users/dabrams/playhead-baselines/xsdz37-lexical-prototype-20260716.md
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import pathlib
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_EVALUATION = (
    REPO_ROOT
    / "TestFixtures/Corpus/Evaluations/"
    "earaudit-oracle-gold-c45c86fced101048e0ecc747d57fc27d170fc910844dce96a8cfbacc906f3565.json"
)
DEFAULT_TRANSCRIPTS_DIR = REPO_ROOT / "TestFixtures/Corpus/Transcripts"
DEFAULT_MANIFEST = REPO_ROOT / "TestFixtures/Corpus/Snapshots/manifest.json"

ONSET_WINDOW_S = 15.0
FALSE_FIRE_CLEARANCE_S = 30.0
FUZZY_RATIO_MIN = 0.85
NEAR_MISS_FLOOR = 0.60

# ---------------------------------------------------------------------------
# Family (a): read-onset attribution templates, compiled from metadata.
# ---------------------------------------------------------------------------

# Sponsorship verb phrases from the bead ("is {brought to you by|sponsored
# by|supported by}") plus the equally conventional "is presented by".
SPONSOR_VERB_PHRASES = (
    "is brought to you by",
    "is sponsored by",
    "is supported by",
    "is presented by",
)

# Canonical public-radio underwriting inversion, parameterised on the same
# entities ("Support for <entity> comes from ...").
INVERTED_TEMPLATE_FORMATS = ("support for {entity} comes from",)

# Small documented station map for public-radio shows in the corpus, keyed
# by manifest showSlug. The bead names WNYC for On The Media; Radiolab is
# the other WNYC Studios production in the corpus; Planet Money / Fresh Air
# / Up First are NPR (Fresh Air is produced at WHYY). Entries for shows not
# in the gold set are harmless — templates only compile for gold shows.
STATION_MAP = {
    "on-the-media": ("WNYC", "WNYC Studios"),
    "radiolab": ("WNYC", "WNYC Studios"),
    "planet-money": ("NPR",),
    "fresh-air": ("NPR", "WHYY"),
    "up-first": ("NPR",),
}

# ---------------------------------------------------------------------------
# Family (b): break-edge framing phrases (generic, identical for all shows).
# Provenance: the first two pre phrases and the first resume phrase are
# Dan's SmartLess ear-audit observations (the bead); the rest are standard
# podcast/broadcast framing idioms fixed before scoring. "coming up after
# this" was added from the same audit session's OTM teaser pattern — its
# numbers are as in-sample as the SmartLess ones and are flagged as such in
# the report. No phrase was added or removed after seeing scores.
# ---------------------------------------------------------------------------

PRE_BREAK_PHRASES = (
    "we'll be right back",       # bead: SmartLess break opener
    "we will be right back",     # bead: SmartLess break opener (uncontracted)
    "when we come back",
    "after the break",
    "take a quick break",
    "a word from our sponsors",
    "coming up after this",      # OTM teaser-out (same audit session)
)

RESUME_PHRASES = (
    "and now back to the show",  # bead: SmartLess resume
    "now back to the show",
    "and back to the show",
    "back to the show",
    "welcome back to the show",
    "back to the program",
    "we're back",
    "welcome back",
)

# Whisper control tokens ("[_BEG_]", "[_TT_750]", ...) can appear standalone
# or glued mid-stream; any token containing "[_" is control data, never speech.
_SPECIAL_TOKEN_MARKER = "[_"
_APOSTROPHES = "'’‘ʼ`´"
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


# ---------------------------------------------------------------------------
# Normalisation + word stream
# ---------------------------------------------------------------------------

def normalize_word(raw: str) -> str:
    """Lowercase, fold+strip apostrophes, drop everything outside [a-z0-9]."""
    text = raw.lower()
    for ch in _APOSTROPHES:
        text = text.replace(ch, "")
    return _NON_ALNUM_RE.sub("", text)


def normalize_phrase(raw: str) -> tuple[str, ...]:
    """Phrase -> tuple of normalised words (empties dropped)."""
    return tuple(w for w in (normalize_word(p) for p in raw.split()) if w)


@dataclass(frozen=True)
class Word:
    norm: str
    start_s: float
    end_s: float


def build_word_stream(transcription: list[dict]) -> list[Word]:
    """Token stream -> word stream with token-level timestamps.

    A token starting with a space (or the first content token of a segment)
    begins a new word; other tokens append to the current word. Special
    tokens like "[_BEG_]" are skipped. Words never span segments.
    """
    words: list[Word] = []
    for segment in transcription:
        current_text = ""
        current_start = None
        current_end = None

        def flush():
            nonlocal current_text, current_start, current_end
            if current_text and current_start is not None:
                norm = normalize_word(current_text)
                if norm:
                    words.append(Word(norm, current_start / 1000.0, current_end / 1000.0))
            current_text, current_start, current_end = "", None, None

        for token in segment.get("tokens", ()):
            text = token.get("text", "")
            if _SPECIAL_TOKEN_MARKER in text:
                # Control token: also a word boundary (e.g. "[_TT_750]"
                # between two words that would otherwise glue together).
                flush()
                continue
            offsets = token.get("offsets", {})
            t_from = offsets.get("from")
            t_to = offsets.get("to")
            if text.startswith(" ") or current_start is None:
                flush()
                current_text = text
                current_start, current_end = t_from, t_to
            else:
                current_text += text
                if t_to is not None:
                    current_end = t_to
        flush()
    return words


# ---------------------------------------------------------------------------
# Template compilation (family a)
# ---------------------------------------------------------------------------

def title_variants(title: str) -> list[str]:
    """Show-title variants: full title, halves around '?'/':', 'The '-less."""
    variants = {title.strip()}
    for sep in ("?", ":"):
        for value in list(variants):
            if sep in value:
                head, tail = value.split(sep, 1)
                variants.add(head.strip())
                variants.add(tail.strip())
    for value in list(variants):
        if value.lower().startswith("the ") and len(value) > 4:
            variants.add(value[4:].strip())
    return sorted(v for v in variants if v and normalize_phrase(v))


@dataclass(frozen=True)
class Phrase:
    family: str          # "a", "b-pre", "b-resume"
    label: str           # human-readable phrase / template
    words: tuple[str, ...]
    entity: str = ""     # family a only: the show/station entity


def compile_show_phrases(
    show_slug: str, titles: list[str], extra_names: list[str]
) -> list[Phrase]:
    """Family-a templates for one show: entities x verb phrases + inversions."""
    entities: dict[tuple[str, ...], str] = {}
    for name in list(titles) + list(extra_names):
        for variant in title_variants(name):
            key = normalize_phrase(variant)
            if key and key not in entities:
                entities[key] = variant
    for station in STATION_MAP.get(show_slug, ()):
        key = normalize_phrase(station)
        if key and key not in entities:
            entities[key] = station

    phrases: list[Phrase] = []
    seen: set[tuple[str, ...]] = set()
    for key, display in sorted(entities.items()):
        for verb in SPONSOR_VERB_PHRASES:
            words = key + normalize_phrase(verb)
            if words not in seen:
                seen.add(words)
                phrases.append(Phrase("a", f"{display} {verb}", words, display))
        for fmt in INVERTED_TEMPLATE_FORMATS:
            label = fmt.format(entity=display)
            words = normalize_phrase(label)
            if words not in seen:
                seen.add(words)
                phrases.append(Phrase("a", label, words, display))
    return phrases


def generic_phrases() -> list[Phrase]:
    out = [Phrase("b-pre", p, normalize_phrase(p)) for p in PRE_BREAK_PHRASES]
    out += [Phrase("b-resume", p, normalize_phrase(p)) for p in RESUME_PHRASES]
    return [p for p in out if len(p.words) >= 2]


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

@dataclass
class Match:
    phrase: Phrase
    start_s: float
    end_s: float
    ratio: float
    exact: bool
    matched_text: str


def _first_word_anchor(word: str, target: str) -> bool:
    if word == target:
        return True
    if len(word) >= 3 and len(target) >= 3 and (
        word.startswith(target) or target.startswith(word)
    ):
        return True
    return difflib.SequenceMatcher(None, word, target).ratio() >= 0.75


def find_phrase_matches(
    words: list[Word],
    phrase: Phrase,
    ratio_min: float = FUZZY_RATIO_MIN,
    near_miss_floor: float = NEAR_MISS_FLOOR,
) -> tuple[list[Match], list[Match]]:
    """All (matches, near_misses) for one phrase over one word stream."""
    n = len(phrase.words)
    if n < 2:
        raise ValueError(f"phrase too short to match safely: {phrase.label!r}")
    target = "".join(phrase.words)
    matches: list[Match] = []
    near: list[Match] = []
    total = len(words)
    for i in range(total):
        if not _first_word_anchor(words[i].norm, phrase.words[0]):
            continue
        best: Match | None = None
        for length in range(max(2, n - 1), n + 3):
            if i + length > total:
                break
            window = words[i : i + length]
            condensed = "".join(w.norm for w in window)
            if len(condensed) > 2 * len(target) + 8:
                break
            exact = length == n and all(
                w.norm == p for w, p in zip(window, phrase.words)
            )
            ratio = (
                1.0
                if exact
                else difflib.SequenceMatcher(None, condensed, target).ratio()
            )
            if best is None or ratio > best.ratio:
                best = Match(
                    phrase,
                    window[0].start_s,
                    window[-1].end_s,
                    ratio,
                    exact,
                    " ".join(w.norm for w in window),
                )
        if best is None:
            continue
        if best.ratio >= ratio_min:
            matches.append(best)
        elif best.ratio >= near_miss_floor:
            near.append(best)
    return matches, near


def dedupe_matches(matches: list[Match]) -> list[Match]:
    """Merge time-overlapping matches; exact beats fuzzy, then longest
    phrase, then ratio (so "welcome to the show" heard verbatim is credited
    to its exact phrase, not a longer fuzzy cousin)."""
    ordered = sorted(
        matches,
        key=lambda m: (not m.exact, -len(m.phrase.words), -m.ratio, m.start_s),
    )
    kept: list[Match] = []
    for match in ordered:
        # Closed-interval overlap: corpus word timestamps can be degenerate
        # (zero-width spans), so touching/equal intervals must also merge.
        if any(
            match.start_s <= other.end_s and other.start_s <= match.end_s
            for other in kept
        ):
            continue
        kept.append(match)
    return sorted(kept, key=lambda m: m.start_s)


# ---------------------------------------------------------------------------
# Scoring vs gold
# ---------------------------------------------------------------------------

def interval_distance(t: float, start: float, end: float) -> float:
    if start <= t <= end:
        return 0.0
    return min(abs(t - start), abs(t - end))


@dataclass
class ScoredMatch:
    match: Match
    classification: str      # onset-hit | resume-hit | in-break | false-fire | edge-adjacent
    break_index: int | None
    delta_s: float | None    # onset: t-start; resume: t-end
    in_veto: bool


@dataclass
class EpisodeScore:
    episode_id: str
    show_name: str
    duration_s: float
    breaks: list[dict]
    vetoes: list[dict]
    scored: list[ScoredMatch] = field(default_factory=list)
    near_misses: list[Match] = field(default_factory=list)


def classify_matches(
    matches: list[Match],
    breaks: list[dict],
    vetoes: list[dict],
    window_s: float,
    clearance_s: float,
) -> list[ScoredMatch]:
    scored: list[ScoredMatch] = []
    for match in matches:
        t = match.start_s
        family = match.phrase.family
        in_veto = any(
            v["start_seconds"] <= t <= v["end_seconds"] for v in vetoes
        )

        edge_hit: tuple[int, float] | None = None
        for idx, brk in enumerate(breaks):
            if family in ("a", "b-pre"):
                delta = t - brk["start_seconds"]
            else:
                delta = t - brk["end_seconds"]
            if abs(delta) <= window_s and (
                edge_hit is None or abs(delta) < abs(edge_hit[1])
            ):
                edge_hit = (idx, delta)

        if edge_hit is not None and not in_veto:
            kind = "resume-hit" if family == "b-resume" else "onset-hit"
            scored.append(ScoredMatch(match, kind, edge_hit[0], edge_hit[1], in_veto))
            continue

        dist = min(
            (
                interval_distance(t, b["start_seconds"], b["end_seconds"])
                for b in breaks
            ),
            default=float("inf"),
        )
        inside_idx = next(
            (
                i
                for i, b in enumerate(breaks)
                if b["start_seconds"] <= t <= b["end_seconds"]
            ),
            None,
        )
        if in_veto:
            scored.append(ScoredMatch(match, "false-fire", None, None, True))
        elif inside_idx is not None:
            scored.append(ScoredMatch(match, "in-break", inside_idx, None, False))
        elif dist > clearance_s:
            scored.append(ScoredMatch(match, "false-fire", None, None, False))
        else:
            scored.append(ScoredMatch(match, "edge-adjacent", None, None, False))
    return scored


def score_episode(
    asset: dict,
    words: list[Word],
    phrases: list[Phrase],
    window_s: float,
    clearance_s: float,
    ratio_min: float,
    near_miss_floor: float,
) -> EpisodeScore:
    by_family: dict[str, list[Match]] = defaultdict(list)
    near_all: list[Match] = []
    for phrase in phrases:
        matches, near = find_phrase_matches(words, phrase, ratio_min, near_miss_floor)
        by_family[phrase.family].extend(matches)
        near_all.extend(near)

    scored: list[ScoredMatch] = []
    for family, matches in by_family.items():
        deduped = dedupe_matches(matches)
        scored.extend(
            classify_matches(
                deduped,
                asset["full_breaks"],
                asset.get("content_vetoes", []),
                window_s,
                clearance_s,
            )
        )
    scored.sort(key=lambda s: s.match.start_s)
    return EpisodeScore(
        episode_id=asset["episode_id"],
        show_name=asset["show_name"],
        duration_s=float(asset["duration_seconds"]),
        breaks=asset["full_breaks"],
        vetoes=asset.get("content_vetoes", []),
        scored=scored,
        near_misses=near_all,
    )


# ---------------------------------------------------------------------------
# Aggregation + report
# ---------------------------------------------------------------------------

def _quantiles(values: list[float]) -> str:
    if not values:
        return "-"
    med = statistics.median(values)
    lo, hi = min(values), max(values)
    return f"median {med:+.1f}s, range [{lo:+.1f}, {hi:+.1f}]s, n={len(values)}"


def aggregate(episodes: list[EpisodeScore], window_s: float) -> dict:
    """Corpus- and show-level aggregation used by both report and JSON dump."""
    shows: dict[str, dict] = {}
    total_breaks = 0
    breaks_with_start_anchor = 0
    breaks_with_any_anchor = 0
    per_phrase: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"fires": 0, "edge_hits": 0, "false_fires": 0}
    )

    for ep in episodes:
        show = shows.setdefault(
            ep.show_name,
            {
                "episodes": 0,
                "hours": 0.0,
                "breaks": 0,
                "a_onset_hits": 0,
                "b_pre_hits": 0,
                "b_resume_hits": 0,
                "breaks_with_start_anchor": 0,
                "breaks_with_any_anchor": 0,
                "a_in_break": 0,
                "false_fires": {"a": 0, "b-pre": 0, "b-resume": 0},
                "onset_deltas_a": [],
                "onset_deltas_b_pre": [],
                "resume_deltas": [],
                "break_records": [],
            },
        )
        show["episodes"] += 1
        show["hours"] += ep.duration_s / 3600.0
        show["breaks"] += len(ep.breaks)
        total_breaks += len(ep.breaks)

        start_anchored: dict[int, set] = defaultdict(set)
        end_anchored: dict[int, set] = defaultdict(set)
        for sm in ep.scored:
            fam = sm.match.phrase.family
            key = (fam, sm.match.phrase.label)
            per_phrase[key]["fires"] += 1
            if sm.classification == "onset-hit":
                per_phrase[key]["edge_hits"] += 1
                start_anchored[sm.break_index].add(fam)
                if fam == "a":
                    show["a_onset_hits"] += 1
                    show["onset_deltas_a"].append(sm.delta_s)
                else:
                    show["b_pre_hits"] += 1
                    show["onset_deltas_b_pre"].append(sm.delta_s)
            elif sm.classification == "resume-hit":
                per_phrase[key]["edge_hits"] += 1
                end_anchored[sm.break_index].add(fam)
                show["b_resume_hits"] += 1
                show["resume_deltas"].append(sm.delta_s)
            elif sm.classification == "in-break":
                if fam == "a":
                    show["a_in_break"] += 1
            elif sm.classification == "false-fire":
                show["false_fires"][fam] += 1
                per_phrase[key]["false_fires"] += 1

        for idx, brk in enumerate(ep.breaks):
            has_start = idx in start_anchored
            has_end = idx in end_anchored
            show["break_records"].append(
                {
                    "episode_id": ep.episode_id,
                    "break_index": idx,
                    "start_seconds": brk["start_seconds"],
                    "end_seconds": brk["end_seconds"],
                    "start_families": sorted(start_anchored.get(idx, ())),
                    "end_families": sorted(end_anchored.get(idx, ())),
                }
            )
            if has_start:
                breaks_with_start_anchor += 1
                show["breaks_with_start_anchor"] += 1
            if has_start or has_end:
                breaks_with_any_anchor += 1
                show["breaks_with_any_anchor"] += 1

    return {
        "window_s": window_s,
        "total_breaks": total_breaks,
        "breaks_with_start_anchor": breaks_with_start_anchor,
        "breaks_with_any_anchor": breaks_with_any_anchor,
        "shows": shows,
        "per_phrase": {
            f"{fam}|{label}": stats for (fam, label), stats in sorted(per_phrase.items())
        },
    }


def near_misses_for_unanchored_edges(
    episodes: list[EpisodeScore], agg: dict, window_s: float
) -> list[dict]:
    """Best fuzzy near-miss next to each gold edge that no anchor hit —
    possible ASR-miss cases (flagged only, never counted)."""
    flagged = []
    records = {
        (r["episode_id"], r["break_index"]): r
        for show in agg["shows"].values()
        for r in show["break_records"]
    }
    for ep in episodes:
        for idx, brk in enumerate(ep.breaks):
            record = records[(ep.episode_id, idx)]
            for edge, anchored, ref in (
                ("start", bool(record["start_families"]), brk["start_seconds"]),
                ("end", bool(record["end_families"]), brk["end_seconds"]),
            ):
                if anchored:
                    continue
                nearby = [
                    nm
                    for nm in ep.near_misses
                    if abs(nm.start_s - ref) <= window_s
                    and (
                        (edge == "start" and nm.phrase.family in ("a", "b-pre"))
                        or (edge == "end" and nm.phrase.family == "b-resume")
                    )
                ]
                if not nearby:
                    continue
                best = max(nearby, key=lambda nm: nm.ratio)
                flagged.append(
                    {
                        "episode_id": ep.episode_id,
                        "edge": edge,
                        "edge_seconds": ref,
                        "phrase": best.phrase.label,
                        "family": best.phrase.family,
                        "ratio": round(best.ratio, 3),
                        "matched_text": best.matched_text,
                        "at_seconds": round(best.start_s, 1),
                    }
                )
    return flagged


def render_report(
    agg: dict,
    episodes: list[EpisodeScore],
    flagged: list[dict],
    meta: dict,
) -> str:
    lines: list[str] = []
    add = lines.append
    shows = agg["shows"]
    window = agg["window_s"]

    add("# xsdz.37 lexical anchor prototype — offline eval vs gold v2")
    add("")
    add(f"- Generated: {meta['generated']} by `scripts/l2f-lexical-anchor-prototype.py`")
    add(f"- Evaluation: `{meta['evaluation']}`")
    add(f"  - sha256 `{meta['evaluation_sha256']}`")
    add(f"- Transcripts: `{meta['transcripts_dir']}` (fast-ASR corpus transcripts; quality varies)")
    add(f"- Manifest: `{meta['manifest']}`")
    add(
        f"- Parameters: onset window +-{window:.0f}s, false-fire clearance "
        f">{meta['clearance_s']:.0f}s, fuzzy ratio >= {meta['ratio_min']}, "
        f"near-miss floor {meta['near_miss_floor']}"
    )
    add(f"- Episodes scored: {meta['episodes_scored']} (skipped, no transcript: {meta['episodes_skipped']})")
    add("")
    add("Matching rule and phrase-set provenance are documented in the script header.")
    add("Honesty: phrase sets fixed a priori; no per-break tuning; gold coverage is")
    add("partial, so non-veto false fires are *uncorroborated* (may be unlabeled ads).")
    add("")

    total = agg["total_breaks"]
    add("## Headline")
    add("")
    add(
        f"- **{agg['breaks_with_start_anchor']}/{total} gold breaks** get a lexical "
        f"anchor at their START (family a onset or b-pre) within +-{window:.0f}s."
    )
    add(
        f"- **{agg['breaks_with_any_anchor']}/{total} gold breaks** get at least one "
        f"lexical anchor on either edge (start or resume)."
    )
    add("")

    add("## Per show x family")
    add("")
    add(
        "| Show | Ep | Hours | Breaks | (a) onset hits | (a) in-break | (b) pre hits | "
        "(b) resume hits | Breaks w/ start anchor | Breaks w/ any anchor | "
        "FF a | FF b-pre | FF b-resume | FF/h (all) |"
    )
    add("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for name in sorted(shows):
        s = shows[name]
        ff_total = sum(s["false_fires"].values())
        rate = ff_total / s["hours"] if s["hours"] else 0.0
        add(
            f"| {name} | {s['episodes']} | {s['hours']:.2f} | {s['breaks']} "
            f"| {s['a_onset_hits']} | {s['a_in_break']} | {s['b_pre_hits']} "
            f"| {s['b_resume_hits']} | {s['breaks_with_start_anchor']} "
            f"| {s['breaks_with_any_anchor']} | {s['false_fires']['a']} "
            f"| {s['false_fires']['b-pre']} | {s['false_fires']['b-resume']} "
            f"| {rate:.2f} |"
        )
    total_hours = sum(s["hours"] for s in shows.values()) or float("nan")
    ff_a = sum(s["false_fires"]["a"] for s in shows.values())
    ff_bp = sum(s["false_fires"]["b-pre"] for s in shows.values())
    ff_br = sum(s["false_fires"]["b-resume"] for s in shows.values())
    add(
        f"| **All** | {sum(s['episodes'] for s in shows.values())} | {total_hours:.2f} "
        f"| {total} | {sum(s['a_onset_hits'] for s in shows.values())} "
        f"| {sum(s['a_in_break'] for s in shows.values())} "
        f"| {sum(s['b_pre_hits'] for s in shows.values())} "
        f"| {sum(s['b_resume_hits'] for s in shows.values())} "
        f"| {agg['breaks_with_start_anchor']} | {agg['breaks_with_any_anchor']} "
        f"| {ff_a} | {ff_bp} | {ff_br} "
        f"| {(ff_a + ff_bp + ff_br) / total_hours:.2f} |"
    )
    add("")
    add(
        "FF = false fires (>30s from every gold break, or inside a content veto). "
        f"Corpus-wide rates per labeled hour: family a {ff_a / total_hours:.2f}/h, "
        f"b-pre {ff_bp / total_hours:.2f}/h, b-resume {ff_br / total_hours:.2f}/h."
    )
    add("")

    edge_ratios = [
        sm.match.ratio
        for ep in episodes
        for sm in ep.scored
        if sm.classification in ("onset-hit", "resume-hit")
    ]
    ff_ratios = [
        sm.match.ratio
        for ep in episodes
        for sm in ep.scored
        if sm.classification == "false-fire"
    ]
    add("### Fuzzy-threshold sensitivity (computed, not re-tuned)")
    add("")
    for alt in (0.88, 0.92):
        lost = sum(1 for r in edge_ratios if r < alt)
        avoided = sum(1 for r in ff_ratios if r < alt)
        add(
            f"- At ratio >= {alt:.2f}: {lost}/{len(edge_ratios)} edge hits lost, "
            f"{avoided}/{len(ff_ratios)} false fires avoided."
        )
    add("")

    add("## Onset / resume error distributions (hits only)")
    add("")
    add("| Show | (a) onset delta (anchor - gold start) | (b) pre delta | (b) resume delta (anchor - gold end) |")
    add("|---|---|---|---|")
    for name in sorted(shows):
        s = shows[name]
        if not (s["onset_deltas_a"] or s["onset_deltas_b_pre"] or s["resume_deltas"]):
            continue
        add(
            f"| {name} | {_quantiles(s['onset_deltas_a'])} "
            f"| {_quantiles(s['onset_deltas_b_pre'])} "
            f"| {_quantiles(s['resume_deltas'])} |"
        )
    add("")

    add("## Per-phrase performance")
    add("")
    add("| Family | Phrase / template | Fires | Edge hits | False fires |")
    add("|---|---|---|---|---|")
    for key, stats in agg["per_phrase"].items():
        fam, label = key.split("|", 1)
        add(
            f"| {fam} | {label} | {stats['fires']} | {stats['edge_hits']} "
            f"| {stats['false_fires']} |"
        )
    add("")

    add("## Anchor detail (edge hits)")
    add("")
    for ep in episodes:
        hits = [
            sm
            for sm in ep.scored
            if sm.classification in ("onset-hit", "resume-hit")
        ]
        if not hits:
            continue
        add(f"- `{ep.episode_id}`")
        for sm in hits:
            add(
                f"  - {sm.classification} [{sm.match.phrase.family}] "
                f"\"{sm.match.phrase.label}\" at {sm.match.start_s:.1f}s "
                f"(delta {sm.delta_s:+.1f}s, ratio {sm.match.ratio:.2f}"
                f"{', exact' if sm.match.exact else ''})"
            )
    add("")

    add("## False fires (uncorroborated or in-veto)")
    add("")
    any_ff = False
    for ep in episodes:
        ffs = [sm for sm in ep.scored if sm.classification == "false-fire"]
        if not ffs:
            continue
        any_ff = True
        add(f"- `{ep.episode_id}`")
        for sm in ffs:
            veto = " IN-VETO" if sm.in_veto else ""
            add(
                f"  - [{sm.match.phrase.family}] \"{sm.match.phrase.label}\" at "
                f"{sm.match.start_s:.1f}s (ratio {sm.match.ratio:.2f}){veto} "
                f"matched: \"{sm.match.matched_text}\""
            )
    if not any_ff:
        add("(none)")
    add("")

    add("## Possible ASR-miss cases (near-miss fuzzy scores at unanchored edges)")
    add("")
    add("Flagged only — inferred from fuzzy scores below threshold; NOT counted as")
    add("hits and NOT verified against audio.")
    add("")
    if flagged:
        for f in flagged:
            add(
                f"- `{f['episode_id']}` {f['edge']} @ {f['edge_seconds']:.1f}s: "
                f"\"{f['phrase']}\" ratio {f['ratio']} at {f['at_seconds']}s "
                f"(matched \"{f['matched_text']}\")"
            )
    else:
        add("(none)")
    add("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def load_manifest_titles(path: pathlib.Path) -> dict[str, dict]:
    """episodeId -> {show, showSlug}; empty on missing manifest."""
    if not path or not pathlib.Path(path).is_file():
        return {}
    entries = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    return {
        e["episodeId"]: {"show": e.get("show", ""), "showSlug": e.get("showSlug", "")}
        for e in entries
        if "episodeId" in e
    }


def show_slug_from_episode_id(episode_id: str) -> str:
    match = re.match(r"^(.*?)-(\d{4})-(\d{2})-(\d{2})-", episode_id)
    if not match:
        raise ValueError(f"cannot derive show slug from episode id: {episode_id!r}")
    return match.group(1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="xsdz.37 lexical anchor prototype: measure read-onset "
        "templates and break-edge framing phrases against gold truth."
    )
    parser.add_argument("--evaluation", type=pathlib.Path, default=DEFAULT_EVALUATION)
    parser.add_argument(
        "--transcripts-dir", type=pathlib.Path, default=DEFAULT_TRANSCRIPTS_DIR
    )
    parser.add_argument(
        "--snapshots-manifest", type=pathlib.Path, default=DEFAULT_MANIFEST
    )
    parser.add_argument("--window", type=float, default=ONSET_WINDOW_S)
    parser.add_argument(
        "--false-fire-clearance", type=float, default=FALSE_FIRE_CLEARANCE_S
    )
    parser.add_argument("--fuzzy-ratio", type=float, default=FUZZY_RATIO_MIN)
    parser.add_argument("--near-miss-floor", type=float, default=NEAR_MISS_FLOOR)
    parser.add_argument("--report-out", type=pathlib.Path, default=None)
    parser.add_argument("--json-out", type=pathlib.Path, default=None)
    args = parser.parse_args(argv)

    evaluation_bytes = args.evaluation.read_bytes()
    evaluation = json.loads(evaluation_bytes)
    manifest = load_manifest_titles(args.snapshots_manifest)
    generic = generic_phrases()

    episodes: list[EpisodeScore] = []
    skipped: list[str] = []
    for asset in evaluation["assets"]:
        eid = asset["episode_id"]
        transcript_path = args.transcripts_dir / f"{eid}.json"
        if not transcript_path.is_file():
            skipped.append(eid)
            continue
        transcript = json.loads(transcript_path.read_text(encoding="utf-8"))
        words = build_word_stream(transcript["transcription"])
        entry = manifest.get(eid, {})
        slug = entry.get("showSlug") or show_slug_from_episode_id(eid)
        titles = [t for t in (entry.get("show", ""),) if t]
        phrases = (
            compile_show_phrases(slug, titles, [asset.get("show_name", "")]) + generic
        )
        episodes.append(
            score_episode(
                asset,
                words,
                phrases,
                args.window,
                args.false_fire_clearance,
                args.fuzzy_ratio,
                args.near_miss_floor,
            )
        )

    agg = aggregate(episodes, args.window)
    flagged = near_misses_for_unanchored_edges(episodes, agg, args.window)

    import datetime

    meta = {
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "evaluation": str(args.evaluation),
        "evaluation_sha256": hashlib.sha256(evaluation_bytes).hexdigest(),
        "transcripts_dir": str(args.transcripts_dir),
        "manifest": str(args.snapshots_manifest),
        "clearance_s": args.false_fire_clearance,
        "ratio_min": args.fuzzy_ratio,
        "near_miss_floor": args.near_miss_floor,
        "episodes_scored": len(episodes),
        "episodes_skipped": len(skipped),
    }
    report = render_report(agg, episodes, flagged, meta)

    if args.report_out:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(report, encoding="utf-8")
        print(f"report written: {args.report_out}")
    if args.json_out:
        payload = {
            "meta": meta,
            "aggregate": {
                **agg,
                "shows": {
                    name: {
                        k: v
                        for k, v in s.items()
                    }
                    for name, s in agg["shows"].items()
                },
            },
            "asr_miss_flags": flagged,
            "skipped_episodes": skipped,
        }
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"json written: {args.json_out}")

    total = agg["total_breaks"]
    print(
        f"gold breaks: {total} | start-anchored: {agg['breaks_with_start_anchor']} "
        f"| any-edge-anchored: {agg['breaks_with_any_anchor']}"
    )
    if not args.report_out and not args.json_out:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
