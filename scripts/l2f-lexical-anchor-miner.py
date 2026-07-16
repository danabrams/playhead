#!/usr/bin/env python3
"""playhead-xsdz.41: self-supervised per-show lexical anchor miner.

Learning-side sibling of scripts/l2f-lexical-anchor-prototype.py (xsdz.37).
The prototype MEASURED hand-compiled phrase families against gold truth;
this miner DISCOVERS per-show anchor phrases from weak supervision with
ZERO hand-seeded phrases. It reuses the prototype's transcript loading,
normalisation, and matching machinery by import.

WEAK-LABEL EDGE SOURCES (trust order; source-tagged, structurally guarded):

  gold-v2    Gold v2 full-break edges (ear-audit oracle evaluation).
             Trusted: +-20s mining window, 2.0s offset-spread tolerance
             (the stinger-bank discipline), support >= 2 DISTINCT episodes.

  rediff-t1  Tier-a rediff slot edges (double-fetch DAI width oracle
             pairs). ~36% edge-noise skepticism: wider +-30s mining
             window, 3.0s offset-spread tolerance, and MORE support
             demanded — a candidate that needs rediff edges to qualify
             must show >= 3 distinct episodes.

  NEVER      Edges placed by a lexical channel. None exist today; the
             guard is structural anyway: every edge carries a `source`
             tag, mining refuses any edge whose source is not in
             ALLOWED_EDGE_SOURCES, and a source containing "lex" raises
             CircularEdgeSourceError specifically (circularity rule per
             xsdz.31 — a lexical bank must never learn from itself).

MINING (per show, per side pre/post):

  1. Candidate generation: every normalised 2-8-token n-gram whose first
     word starts within the source-tier window of a weak edge. side=pre
     edges are break/slot starts; side=post edges are break/slot ends.
     Duplicate physical occurrences (same episode/side/word index, seen
     from two edges) collapse to the closest edge, gold preferred.
  2. Offset consistency (spread gate): offset = occurrence start - edge
     time. Median offset over all occurrences; SUPPORTING occurrences
     are those within the source-tier tolerance of the median. Support
     is counted over supporting occurrences only.
  3. Support gate, in trust order:
       - >= 2 distinct episodes with gold-supporting occurrences, or
       - >= 3 distinct episodes with any supporting occurrence
         (rediff-corroborated tier), or
       - PROVISIONAL: the show has exactly ONE weak-labeled episode in
         the corpus AND the phrase supports >= 3 DISTINCT EDGES within
         it. Flagged `single-episode-provisional`, confidence-penalised.
         Rationale: distinct-episode counting is the dedupe lesson
         (never count the same ad read twice); distinct edges within
         one episode are distinct reads. Without this documented
         fallback, single-episode shows (e.g. On The Media: one corpus
         episode) are structurally unminable regardless of evidence.
  4. Edge-affinity gate (kills welcome-back-class traps): count ALL
     occurrences of the candidate across the show's full transcripts,
     classified as near (same-side window of a same-side edge), neutral
     (inside a weak break/slot interval or near an opposite-side edge —
     in-break re-attributions are presence evidence, not contradiction),
     or content (everywhere else). Gates:
       affinity  = near / (near + content)            >= 0.60
       rateRatio = (near/near-window-hours)
                   / (all/full-transcript-hours)      >= 5.0
  5. Exact-only short phrases: candidates under 4 tokens are counted
     exact-only in step 4 and banked matchPolicy="exact" (2-word/fuzzy
     short phrases went 0/12 in the xsdz.37 prototype). Candidates of
     >= 4 tokens are counted with the prototype's fuzzy matcher at
     ratio >= 0.88 (the prototype's zero-hit-loss sensitivity line) and
     banked matchPolicy="fuzzy-0.88".
  6. Subsumption prune: among survivors, a shorter phrase is absorbed by
     a kept longer phrase when its words are a contiguous subsequence
     and every supporting occurrence overlaps one of the longer
     phrase's occurrences ("back to the show" folds into "and now back
     to the show"). Absorbed candidates are reported, not banked.
  7. Span merge: a repeated span longer than NGRAM_MAX tokens (e.g. the
     same ad creative read verbatim near two edges) survives as many
     sliding 8-gram shingles with identical support. Shingles with the
     SAME supporting edge set whose occurrences overlap in time chain
     into one entry: the span-start shingle represents the span (that
     is the anchor-onset semantics) and carries mergedShingleCount +
     spanEndOffsetSeconds. Presentation-level de-redundancy only — it
     never changes which evidence passed the gates.

OUTPUT:

  --bank-out    Candidate bank JSON, StingerBank-shaped lexical entries
                (phrase, side, offset median + spread, support episodes,
                edge affinity, confidence proxy, match policy, status
                "candidate") with content-addressed provenance (sha256
                of every input) like the stinger bank emitter. Entries
                are CANDIDATES ONLY — nothing is accepted until Dan's
                frontier accept/reject pass, and bank regeneration is
                gated through gold scoring before any production wiring.
  --report-out  FRONTIER REPORT markdown: every discovered candidate
                with its evidence, the post-hoc known-anchor validation
                scorecard, and the shows-awaiting-weak-labels growth
                path (new shows with transcripts but no gold/rediff).

HONESTY / VALIDATION: VALIDATION_TARGETS below is POST-HOC ONLY — it is
never passed to the mining functions (mine_show takes no phrase lists;
tests assert discovery works on phrases the targets do not contain). It
exists so the report can say which known-waiting anchors the miner
independently re-discovered and, when one is missed, which gate stopped
it — reported honestly rather than loosening gates to force it.

Confidence is an UNCALIBRATED proxy (affinity x support x offset
tightness x source trust), for ranking the frontier review only.

Usage:
  python3 scripts/l2f-lexical-anchor-miner.py \
      --rediff-baseline /Users/dabrams/playhead-baselines/rebase-198-20260715.json \
      --bank-out  /Users/dabrams/playhead-baselines/xsdz41-lexical-bank-<date>.json \
      --report-out /Users/dabrams/playhead-baselines/xsdz41-lexical-miner-<date>.md
"""

from __future__ import annotations

import argparse
import bisect
import datetime
import functools
import hashlib
import importlib.util
import json
import pathlib
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

BANK_SCHEMA_VERSION = 1


def _load_prototype():
    """Import the hyphenated prototype module (shared machinery)."""
    name = "l2f_lexical_anchor_prototype"
    if name in sys.modules:
        return sys.modules[name]
    path = REPO_ROOT / "scripts" / "l2f-lexical-anchor-prototype.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PROTO = _load_prototype()

# Pure-function memoization only (identical results, large speedup): the
# fuzzy affinity counting pass calls the prototype's first-word anchor
# check once per (stream word, phrase first word) pair, and show-level
# vocabularies repeat heavily.
PROTO._first_word_anchor = functools.lru_cache(maxsize=1_000_000)(
    PROTO._first_word_anchor
)

# ---------------------------------------------------------------------------
# Edge sources + tiers (the circularity guard lives here)
# ---------------------------------------------------------------------------

GOLD_SOURCE = "gold-v2"
REDIFF_SOURCE = "rediff-t1"


@dataclass(frozen=True)
class SourceTier:
    window_s: float       # mining / near-edge window around the edge
    spread_tol_s: float   # max |offset - median| to count as supporting


ALLOWED_EDGE_SOURCES: dict[str, SourceTier] = {
    GOLD_SOURCE: SourceTier(window_s=20.0, spread_tol_s=2.0),
    REDIFF_SOURCE: SourceTier(window_s=30.0, spread_tol_s=3.0),
}

# Support gates (K=2 is provisional given corpus size — documented).
MIN_GOLD_EPISODES = 2
MIN_ANY_EPISODES = 3          # when rediff edges are needed to qualify
SINGLE_EPISODE_MIN_EDGES = 3  # provisional tier for one-episode shows

# Discrimination gates.
MIN_AFFINITY = 0.60
MIN_RATE_RATIO = 5.0
NGRAM_MIN, NGRAM_MAX = 2, 8
SHORT_PHRASE_MAX_TOKENS = 3   # < 4 tokens => exact-only matching
FUZZY_COUNT_RATIO = 0.88      # prototype sensitivity line: 0 hits lost

# Same-episode/same-side gold-vs-rediff edges within this collapse to gold.
EDGE_DEDUPE_S = 15.0


class CircularEdgeSourceError(ValueError):
    """A weak edge claims a lexical-channel source: refusing to learn from it."""


@dataclass(frozen=True)
class Edge:
    episode_id: str
    show_slug: str
    side: str        # "pre" (break/slot start) | "post" (break/slot end)
    time_s: float
    source: str      # must be in ALLOWED_EDGE_SOURCES
    edge_id: str


def guard_edge_sources(edges: list[Edge]) -> None:
    """Structural circularity guard: refuse unknown and lexical sources."""
    for edge in edges:
        if edge.source in ALLOWED_EDGE_SOURCES:
            continue
        if "lex" in edge.source.lower():
            raise CircularEdgeSourceError(
                f"edge {edge.edge_id!r} has lexical-channel source "
                f"{edge.source!r}: a lexical bank must never learn from "
                "lexical-placed edges (xsdz.31 circularity rule)"
            )
        raise ValueError(
            f"edge {edge.edge_id!r} has unknown source {edge.source!r}; "
            f"allowed: {sorted(ALLOWED_EDGE_SOURCES)}"
        )


# ---------------------------------------------------------------------------
# Weak-label loading
# ---------------------------------------------------------------------------

def load_gold_edges(evaluation: dict) -> tuple[list[Edge], dict[str, list[tuple[float, float]]]]:
    """Gold v2 full breaks -> (edges, break intervals per episode)."""
    edges: list[Edge] = []
    intervals: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for asset in evaluation["assets"]:
        eid = asset["episode_id"]
        slug = PROTO.show_slug_from_episode_id(eid)
        for idx, brk in enumerate(asset["full_breaks"]):
            start, end = float(brk["start_seconds"]), float(brk["end_seconds"])
            intervals[eid].append((start, end))
            edges.append(Edge(eid, slug, "pre", start, GOLD_SOURCE, f"{eid}:gold:{idx}:pre"))
            edges.append(Edge(eid, slug, "post", end, GOLD_SOURCE, f"{eid}:gold:{idx}:post"))
    return edges, intervals


def load_rediff_edges(
    rediff: dict, gold_edges: list[Edge]
) -> tuple[list[Edge], dict[str, list[tuple[float, float]]]]:
    """Rediff pairs -> tier-a slot edges, deduped against gold edges.

    A rediff edge within EDGE_DEDUPE_S of a same-episode same-side gold
    edge is dropped (gold supersedes; keeping both would double-anchor
    the same physical boundary with two references).
    """
    gold_by_key: dict[tuple[str, str], list[float]] = defaultdict(list)
    for edge in gold_edges:
        gold_by_key[(edge.episode_id, edge.side)].append(edge.time_s)

    edges: list[Edge] = []
    intervals: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for idx, pair in enumerate(rediff.get("pairs", ())):
        eid = pair["episodeId"]
        slug = PROTO.show_slug_from_episode_id(eid)
        start, end = float(pair["slotStart"]), float(pair["slotEnd"])
        intervals[eid].append((start, end))
        for side, t in (("pre", start), ("post", end)):
            near_gold = any(
                abs(t - g) <= EDGE_DEDUPE_S for g in gold_by_key.get((eid, side), ())
            )
            if near_gold:
                continue
            edges.append(Edge(eid, slug, side, t, REDIFF_SOURCE, f"{eid}:rediff:{idx}:{side}"))
    return edges, intervals


# ---------------------------------------------------------------------------
# Occurrence gathering
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Occurrence:
    episode_id: str
    word_index: int
    start_s: float
    end_s: float
    offset_s: float   # start_s - edge.time_s
    edge: Edge


@dataclass
class Candidate:
    show_slug: str
    side: str
    words: tuple[str, ...]
    occurrences: list[Occurrence]
    # Filled by evaluation:
    median_offset_s: float = 0.0
    supporting: list[Occurrence] = field(default_factory=list)
    spread_s: float = 0.0
    support_kind: str = ""          # gold | rediff-corroborated | single-episode-provisional
    gold_episodes: tuple[str, ...] = ()
    all_episodes: tuple[str, ...] = ()
    supporting_edges: tuple[str, ...] = ()
    near_count: int = 0
    neutral_count: int = 0
    content_count: int = 0
    affinity: float = 0.0
    rate_ratio: float = 0.0
    confidence: float = 0.0
    rejected_reason: str = ""       # empty => passed all gates
    absorbed_into: str = ""         # phrase text of the absorbing candidate
    span_shingle_count: int = 0     # merged same-span shingles (span merge)
    span_end_offset_s: float = 0.0  # last merged shingle's median offset


def gather_occurrences(
    words: list, edges: list[Edge]
) -> dict[tuple[str, tuple[str, ...]], list[Occurrence]]:
    """All 2-8-token n-grams starting within each edge's tier window.

    Returns {(side, ngram): [occurrences]}. Duplicate physical
    occurrences (same episode/side/word index seen from two edges)
    collapse to the closest edge, gold preferred.
    """
    best: dict[tuple[str, tuple[str, ...], int], Occurrence] = {}
    if not edges:
        return {}
    starts = [w.start_s for w in words]
    for edge in edges:
        window = ALLOWED_EDGE_SOURCES[edge.source].window_s
        lo = bisect.bisect_left(starts, edge.time_s - window)
        hi = bisect.bisect_right(starts, edge.time_s + window)
        for i in range(lo, hi):
            for n in range(NGRAM_MIN, NGRAM_MAX + 1):
                if i + n > len(words):
                    break
                gram = tuple(w.norm for w in words[i : i + n])
                occ = Occurrence(
                    episode_id=edge.episode_id,
                    word_index=i,
                    start_s=words[i].start_s,
                    end_s=words[i + n - 1].end_s,
                    offset_s=words[i].start_s - edge.time_s,
                    edge=edge,
                )
                key = (edge.side, gram, i)
                prev = best.get(key)
                if prev is None or _prefer(occ, prev):
                    best[key] = occ

    grouped: dict[tuple[str, tuple[str, ...]], list[Occurrence]] = defaultdict(list)
    for (side, gram, _i), occ in best.items():
        grouped[(side, gram)].append(occ)
    return grouped


def _prefer(a: Occurrence, b: Occurrence) -> bool:
    """True when occurrence a's edge attribution beats b's (gold, then closest)."""
    a_gold = a.edge.source == GOLD_SOURCE
    b_gold = b.edge.source == GOLD_SOURCE
    if a_gold != b_gold:
        return a_gold
    return abs(a.offset_s) < abs(b.offset_s)


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

def apply_offset_and_support_gates(cand: Candidate, labeled_episode_count: int) -> None:
    """Spread gate (median +- tier tolerance) then tiered support gate."""
    offsets = [o.offset_s for o in cand.occurrences]
    cand.median_offset_s = statistics.median(offsets)
    cand.supporting = [
        o
        for o in cand.occurrences
        if abs(o.offset_s - cand.median_offset_s)
        <= ALLOWED_EDGE_SOURCES[o.edge.source].spread_tol_s
    ]
    if len(cand.supporting) < 2:
        cand.rejected_reason = "offset-spread"
        return
    cand.spread_s = max(abs(o.offset_s - cand.median_offset_s) for o in cand.supporting)
    cand.gold_episodes = tuple(
        sorted({o.episode_id for o in cand.supporting if o.edge.source == GOLD_SOURCE})
    )
    cand.all_episodes = tuple(sorted({o.episode_id for o in cand.supporting}))
    cand.supporting_edges = tuple(sorted({o.edge.edge_id for o in cand.supporting}))

    if len(cand.gold_episodes) >= MIN_GOLD_EPISODES:
        cand.support_kind = "gold"
    elif len(cand.all_episodes) >= MIN_ANY_EPISODES:
        cand.support_kind = "rediff-corroborated"
    elif (
        labeled_episode_count == 1
        and len(cand.supporting_edges) >= SINGLE_EPISODE_MIN_EDGES
    ):
        cand.support_kind = "single-episode-provisional"
    else:
        cand.rejected_reason = "support"


def count_candidate_occurrences(
    cand: Candidate, words_by_episode: dict[str, list]
) -> list[tuple[str, float, float]]:
    """All (episode, start_s, end_s) occurrences across the show's transcripts.

    Exact-only for short phrases (< 4 tokens); fuzzy at ratio >= 0.88 via
    the prototype matcher for longer ones (runtime-FP realism).
    """
    hits: list[tuple[str, float, float]] = []
    if len(cand.words) <= SHORT_PHRASE_MAX_TOKENS:
        n = len(cand.words)
        for eid, words in words_by_episode.items():
            for i in range(len(words) - n + 1):
                if all(words[i + j].norm == cand.words[j] for j in range(n)):
                    hits.append((eid, words[i].start_s, words[i + n - 1].end_s))
    else:
        phrase = PROTO.Phrase("mined", " ".join(cand.words), cand.words)
        for eid, words in words_by_episode.items():
            matches, _near = PROTO.find_phrase_matches(
                words, phrase, ratio_min=FUZZY_COUNT_RATIO, near_miss_floor=FUZZY_COUNT_RATIO
            )
            for m in PROTO.dedupe_matches(matches):
                hits.append((eid, m.start_s, m.end_s))
    return hits


def _union_seconds(intervals: list[tuple[float, float]]) -> float:
    total = 0.0
    last_end = None
    for start, end in sorted(intervals):
        if last_end is None or start > last_end:
            total += end - start
            last_end = end
        elif end > last_end:
            total += end - last_end
            last_end = end
    return total


def apply_affinity_gate(
    cand: Candidate,
    words_by_episode: dict[str, list],
    edges_by_episode: dict[str, list[Edge]],
    intervals_by_episode: dict[str, list[tuple[float, float]]],
    episode_seconds: dict[str, float],
) -> None:
    """Edge-affinity ratio: frequency near same-side edges vs base rate.

    Occurrences inside weak break/slot intervals or near opposite-side
    edges are NEUTRAL (in-break re-attributions are presence evidence).
    """
    near = neutral = content = 0
    near_windows: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for eid, edges in edges_by_episode.items():
        for e in edges:
            if e.side == cand.side:
                w = ALLOWED_EDGE_SOURCES[e.source].window_s
                near_windows[eid].append((e.time_s - w, e.time_s + w))

    for eid, start_s, _end_s in count_candidate_occurrences(cand, words_by_episode):
        if any(lo <= start_s <= hi for lo, hi in near_windows.get(eid, ())):
            near += 1
            continue
        in_break = any(
            lo <= start_s <= hi for lo, hi in intervals_by_episode.get(eid, ())
        )
        near_opposite = any(
            abs(start_s - e.time_s) <= ALLOWED_EDGE_SOURCES[e.source].window_s
            for e in edges_by_episode.get(eid, ())
            if e.side != cand.side
        )
        if in_break or near_opposite:
            neutral += 1
        else:
            content += 1

    cand.near_count, cand.neutral_count, cand.content_count = near, neutral, content
    denom = near + content
    cand.affinity = near / denom if denom else 0.0

    # Base rate excludes NEUTRAL occurrences: in-break re-attributions are
    # presence evidence and must not dilute the edge-specificity measure.
    near_hours = sum(_union_seconds(v) for v in near_windows.values()) / 3600.0
    total_hours = sum(episode_seconds.values()) / 3600.0
    rated_count = near + content
    if near_hours <= 0 or total_hours <= 0 or rated_count == 0:
        cand.rate_ratio = 0.0
    else:
        base_rate = rated_count / total_hours
        cand.rate_ratio = (near / near_hours) / base_rate if base_rate else 0.0
    cand.rate_ratio = min(round(cand.rate_ratio, 2), 999.0)

    if cand.affinity < MIN_AFFINITY:
        cand.rejected_reason = "edge-affinity"
    elif cand.rate_ratio < MIN_RATE_RATIO:
        cand.rejected_reason = "rate-ratio"


def prune_subsumed(cands: list[Candidate]) -> None:
    """Absorb shorter phrases whose evidence a kept longer phrase covers."""
    passed = [c for c in cands if not c.rejected_reason]
    passed.sort(
        key=lambda c: (-len(c.words), -len(c.all_episodes), -c.affinity, c.words)
    )
    kept: list[Candidate] = []
    for cand in passed:
        absorber = next(
            (
                k
                for k in kept
                if k.side == cand.side
                and _contiguous_subsequence(cand.words, k.words)
                and all(
                    any(
                        o.episode_id == ko.episode_id
                        and o.start_s <= ko.end_s
                        and ko.start_s <= o.end_s
                        for ko in k.supporting
                    )
                    for o in cand.supporting
                )
            ),
            None,
        )
        if absorber is not None:
            cand.rejected_reason = "subsumed"
            cand.absorbed_into = " ".join(absorber.words)
        else:
            kept.append(cand)


def merge_span_shingles(cands: list[Candidate]) -> None:
    """Chain-collapse sliding shingles of one long repeated span.

    Two passed candidates chain when they share the SAME supporting edge
    set and their supporting occurrences overlap in time at every edge.
    The chain's representative is the span-start shingle (smallest median
    offset — anchor-onset semantics); the rest are absorbed with reason
    "span-merged". Cardinality reduction only, never gate loosening.
    """
    groups: dict[tuple[str, tuple[str, ...]], list[Candidate]] = defaultdict(list)
    for cand in cands:
        if not cand.rejected_reason:
            groups[(cand.side, cand.supporting_edges)].append(cand)
    for members in groups.values():
        if len(members) < 2:
            continue
        members.sort(key=lambda c: (c.median_offset_s, c.words))
        clusters: list[list[Candidate]] = []
        for cand in members:
            for cluster in clusters:
                if _supporting_occurrences_overlap(cluster[-1], cand):
                    cluster.append(cand)
                    break
            else:
                clusters.append([cand])
        for cluster in clusters:
            if len(cluster) < 2:
                continue
            rep = cluster[0]  # smallest median offset: the span start
            rep.span_shingle_count = len(cluster) - 1
            rep.span_end_offset_s = cluster[-1].median_offset_s
            for cand in cluster[1:]:
                cand.rejected_reason = "span-merged"
                cand.absorbed_into = " ".join(rep.words)


def _supporting_occurrences_overlap(a: Candidate, b: Candidate) -> bool:
    """True when a and b overlap in time at every shared supporting edge."""
    b_by_edge: dict[str, list[Occurrence]] = defaultdict(list)
    for o in b.supporting:
        b_by_edge[o.edge.edge_id].append(o)
    for o in a.supporting:
        if not any(
            o.start_s <= ob.end_s and ob.start_s <= o.end_s
            for ob in b_by_edge.get(o.edge.edge_id, ())
        ):
            return False
    return True


def _contiguous_subsequence(needle: tuple[str, ...], haystack: tuple[str, ...]) -> bool:
    if len(needle) >= len(haystack):
        return False
    return any(
        haystack[i : i + len(needle)] == needle
        for i in range(len(haystack) - len(needle) + 1)
    )


def score_confidence(cand: Candidate) -> None:
    """Uncalibrated ranking proxy for the frontier review, documented in header."""
    support_factor = min(1.0, len(cand.all_episodes) / 4.0)
    gold_tol = ALLOWED_EDGE_SOURCES[GOLD_SOURCE].spread_tol_s
    tightness = max(0.25, 1.0 - cand.spread_s / (2.0 * gold_tol))
    source_factor = {
        "gold": 1.0,
        "rediff-corroborated": 0.8,
        "single-episode-provisional": 0.5,
    }[cand.support_kind]
    cand.confidence = round(
        cand.affinity * support_factor * tightness * source_factor, 3
    )


# ---------------------------------------------------------------------------
# Per-show mining
# ---------------------------------------------------------------------------

def mine_show(
    show_slug: str,
    words_by_episode: dict[str, list],
    edges: list[Edge],
    intervals_by_episode: dict[str, list[tuple[float, float]]],
    keep_rejected: "callable | None" = None,
) -> tuple[list[Candidate], list[Candidate]]:
    """Mine one show. Returns (passed_candidates, retained_rejected).

    `keep_rejected(candidate) -> bool` decides which rejected candidates
    to retain for reporting (post-hoc validation tracing); None keeps none.
    NOTE: this function takes no phrase lists — discovery is unseeded by
    construction.
    """
    guard_edge_sources(edges)
    edges_by_episode: dict[str, list[Edge]] = defaultdict(list)
    for e in edges:
        edges_by_episode[e.episode_id].append(e)
    labeled_episode_count = len(
        {e.episode_id for e in edges if e.episode_id in words_by_episode}
    )
    episode_seconds = {
        eid: (words[-1].end_s if words else 0.0)
        for eid, words in words_by_episode.items()
    }

    grouped: dict[tuple[str, tuple[str, ...]], list[Occurrence]] = defaultdict(list)
    for eid, words in sorted(words_by_episode.items()):
        ep_edges = edges_by_episode.get(eid, ())
        if not ep_edges:
            continue
        for key, occs in gather_occurrences(words, list(ep_edges)).items():
            grouped[key].extend(occs)

    candidates: list[Candidate] = []
    for (side, gram), occs in sorted(grouped.items()):
        if len(occs) < 2:
            continue  # cannot possibly meet any support tier
        candidates.append(Candidate(show_slug, side, gram, sorted(occs, key=lambda o: (o.episode_id, o.start_s))))

    survivors: list[Candidate] = []
    retained_rejected: list[Candidate] = []

    def _reject(cand: Candidate) -> None:
        if keep_rejected is not None and keep_rejected(cand):
            retained_rejected.append(cand)

    for cand in candidates:
        apply_offset_and_support_gates(cand, labeled_episode_count)
        if cand.rejected_reason:
            _reject(cand)
        else:
            survivors.append(cand)

    # Affinity is the expensive gate (fuzzy counting for long phrases):
    # run it last, on spread+support survivors only.
    for cand in survivors:
        apply_affinity_gate(
            cand, words_by_episode, edges_by_episode, intervals_by_episode, episode_seconds
        )
        if cand.rejected_reason:
            _reject(cand)

    prune_subsumed(survivors)
    merge_span_shingles(survivors)
    passed = []
    for cand in survivors:
        if cand.rejected_reason:
            if cand.rejected_reason in ("subsumed", "span-merged"):
                _reject(cand)
            continue
        score_confidence(cand)
        passed.append(cand)
    passed.sort(key=lambda c: (-c.confidence, c.side, c.words))
    return passed, retained_rejected


# ---------------------------------------------------------------------------
# Post-hoc validation targets (NEVER passed to mining — see module header)
# ---------------------------------------------------------------------------

VALIDATION_TARGETS = (
    {
        "id": "nikki-when-we-get-back",
        "label": "Nikki Glaser 'when we get back' (or close variant)",
        "show_slug": "the-nikki-glaser-podcast",
        "side": "pre",
        "any_of": (("when", "we", "get", "back"), ("we", "get", "back")),
    },
    {
        "id": "otm-attribution",
        "label": "OTM attribution family ('<station|show> is supported by')",
        "show_slug": "on-the-media",
        "side": "pre",
        "any_of": (("is", "supported", "by"), ("supported", "by")),
    },
    {
        "id": "otm-station-ident-resume",
        "label": "OTM station-ident resume ('This is On The Media')",
        "show_slug": "on-the-media",
        "side": "post",
        "any_of": (("this", "is", "on", "the", "media"),),
    },
    {
        "id": "smartless-pre",
        "label": "SmartLess 'we'll/we will be right back'",
        "show_slug": "smartless",
        "side": "pre",
        "any_of": (("be", "right", "back"),),
    },
    {
        "id": "smartless-resume",
        "label": "SmartLess 'back to the show' family",
        "show_slug": "smartless",
        "side": "post",
        "any_of": (("back", "to", "the", "show"),),
    },
    {
        "id": "techcrunch-preroll-opener",
        "label": "TechCrunch preroll opener ('this episode is brought to you by')",
        "show_slug": "techcrunch-daily-crunch",
        "side": "pre",
        "any_of": (("brought", "to", "you", "by"), ("this", "episode", "is")),
    },
)


def matches_validation_target(cand: Candidate, target: dict) -> bool:
    if cand.show_slug != target["show_slug"] or cand.side != target["side"]:
        return False
    return any(
        _contiguous_subsequence(pat, cand.words) or pat == cand.words
        for pat in target["any_of"]
    )


def validation_scorecard(
    passed_by_show: dict[str, list[Candidate]],
    rejected_by_show: dict[str, list[Candidate]],
) -> list[dict]:
    rows = []
    for target in VALIDATION_TARGETS:
        found = [
            c
            for c in passed_by_show.get(target["show_slug"], ())
            if matches_validation_target(c, target)
        ]
        near = [
            c
            for c in rejected_by_show.get(target["show_slug"], ())
            if matches_validation_target(c, target)
        ]
        rows.append({"target": target, "found": found, "near": near})
    return rows


# ---------------------------------------------------------------------------
# Bank + report
# ---------------------------------------------------------------------------

def _sha256_path(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def transcripts_digest(paths: list[pathlib.Path]) -> str:
    lines = sorted(f"{p.name}:{_sha256_path(p)}" for p in paths)
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def candidate_entry(cand: Candidate) -> dict:
    support_by_source: dict[str, int] = defaultdict(int)
    for eid in cand.all_episodes:
        if eid in cand.gold_episodes:
            support_by_source[GOLD_SOURCE] += 1
        else:
            support_by_source[REDIFF_SOURCE] += 1
    return {
        "phrase": " ".join(cand.words),
        "side": cand.side,
        "offsetMedianSeconds": round(cand.median_offset_s, 2),
        "offsetSpreadSeconds": round(cand.spread_s, 2),
        "supportEpisodes": list(cand.all_episodes),
        "supportEpisodesBySource": dict(sorted(support_by_source.items())),
        "supportEdgeCount": len(cand.supporting_edges),
        "supportKind": cand.support_kind,
        "edgeAffinity": round(cand.affinity, 3),
        "rateRatio": cand.rate_ratio,
        "occurrenceCounts": {
            "near": cand.near_count,
            "neutral": cand.neutral_count,
            "content": cand.content_count,
        },
        "confidence": cand.confidence,
        "matchPolicy": (
            "exact"
            if len(cand.words) <= SHORT_PHRASE_MAX_TOKENS
            else f"fuzzy-{FUZZY_COUNT_RATIO:.2f}"
        ),
        "status": "candidate",
        **(
            {
                "mergedShingleCount": cand.span_shingle_count,
                "spanEndOffsetSeconds": round(cand.span_end_offset_s, 2),
            }
            if cand.span_shingle_count
            else {}
        ),
    }


def build_bank(
    passed_by_show: dict[str, list[Candidate]],
    show_names: dict[str, str],
    feed_urls_by_slug: dict[str, list[str]],
    parameters: dict,
    sources: dict,
) -> dict:
    shows = []
    for slug in sorted(passed_by_show):
        cands = passed_by_show[slug]
        if not cands:
            continue
        shows.append(
            {
                "showKeys": [slug, *feed_urls_by_slug.get(slug, [])],
                "showName": show_names.get(slug, slug),
                "entries": [candidate_entry(c) for c in cands],
            }
        )
    return {
        "schemaVersion": BANK_SCHEMA_VERSION,
        "bankKind": "lexical-anchor-candidates",
        "generator": "scripts/l2f-lexical-anchor-miner.py",
        "protocol": (
            "self-supervised per-show lexical anchors mined from weak-label "
            "break edges (gold-v2 + rediff-t1); zero hand-seeded phrases; "
            "candidates only, pending frontier accept/reject review"
        ),
        "parameters": parameters,
        "sources": sources,
        "shows": shows,
    }


def _fmt_occ(o: Occurrence) -> str:
    return (
        f"`{o.episode_id}` @ {o.start_s:.1f}s (offset {o.offset_s:+.1f}s vs "
        f"{o.edge.side} edge {o.edge.time_s:.1f}s, {o.edge.source})"
    )


def render_report(
    passed_by_show: dict[str, list[Candidate]],
    rejected_by_show: dict[str, list[Candidate]],
    scorecard: list[dict],
    awaiting: list[dict],
    show_names: dict[str, str],
    meta: dict,
) -> str:
    lines: list[str] = []
    add = lines.append
    add("# xsdz.41 lexical anchor miner — candidate frontier report")
    add("")
    add(f"- Generated: {meta['generated']} by `scripts/l2f-lexical-anchor-miner.py`")
    add(f"- Evaluation (gold-v2 edges): `{meta['evaluation']}`")
    add(f"  - sha256 `{meta['evaluation_sha256']}`")
    if meta.get("rediff"):
        add(f"- Rediff baseline (rediff-t1 edges): `{meta['rediff']}`")
        add(f"  - sha256 `{meta['rediff_sha256']}`")
    else:
        add("- Rediff baseline: (none — gold edges only)")
    add(f"- Transcripts: `{meta['transcripts_dir']}` ({meta['transcript_count']} files)")
    add(f"  - digest sha256 `{meta['transcripts_digest']}`")
    add(
        f"- Gates: gold window +-{ALLOWED_EDGE_SOURCES[GOLD_SOURCE].window_s:.0f}s"
        f"/tol {ALLOWED_EDGE_SOURCES[GOLD_SOURCE].spread_tol_s:.1f}s; rediff window "
        f"+-{ALLOWED_EDGE_SOURCES[REDIFF_SOURCE].window_s:.0f}s/tol "
        f"{ALLOWED_EDGE_SOURCES[REDIFF_SOURCE].spread_tol_s:.1f}s; support >= "
        f"{MIN_GOLD_EPISODES} gold episodes or >= {MIN_ANY_EPISODES} any-source "
        f"episodes (K provisional given corpus size) or single-episode fallback "
        f">= {SINGLE_EPISODE_MIN_EDGES} distinct edges; affinity >= {MIN_AFFINITY}; "
        f"rate ratio >= {MIN_RATE_RATIO}; exact-only <= {SHORT_PHRASE_MAX_TOKENS} "
        f"tokens, fuzzy {FUZZY_COUNT_RATIO} above."
    )
    add("")
    add(
        "Every entry below is a CANDIDATE awaiting accept/reject review — "
        "nothing here is production-wired. No phrase was seeded; the "
        "known-anchor section is post-hoc validation only."
    )
    add("")

    add("## Known-anchor validation scorecard (post-hoc; never fed to mining)")
    add("")
    for row in scorecard:
        target = row["target"]
        if row["found"]:
            add(f"- **{target['label']}** — DISCOVERED:")
            for c in row["found"]:
                add(
                    f"  - \"{' '.join(c.words)}\" [{c.side}] offset "
                    f"{c.median_offset_s:+.1f}s +-{c.spread_s:.1f}s, support "
                    f"{len(c.all_episodes)} ep ({len(c.gold_episodes)} gold) / "
                    f"{len(c.supporting_edges)} edges [{c.support_kind}], affinity "
                    f"{c.affinity:.2f}, rateRatio {c.rate_ratio}, conf {c.confidence}"
                )
        else:
            add(f"- **{target['label']}** — NOT DISCOVERED.")
            near = sorted(row["near"], key=lambda c: -len(c.words))[:5]
            if near:
                add("  - Nearest mined candidates and the gate that stopped them:")
                for c in near:
                    detail = (
                        f"; occurrences {len(c.occurrences)}, supporting "
                        f"{len(c.supporting)}, episodes {len(c.all_episodes)}"
                    )
                    if c.rejected_reason in ("edge-affinity", "rate-ratio"):
                        detail += (
                            f"; affinity {c.affinity:.2f}, rateRatio "
                            f"{c.rate_ratio}, near/neutral/content "
                            f"{c.near_count}/{c.neutral_count}/{c.content_count}"
                        )
                    add(
                        f"    - \"{' '.join(c.words)}\" [{c.side}] rejected: "
                        f"{c.rejected_reason}"
                        + (f" (absorbed into \"{c.absorbed_into}\")" if c.absorbed_into else "")
                        + detail
                    )
            else:
                add("  - No matching n-gram was even generated near weak edges.")
    add("")

    add("## Discovered candidates by show")
    add("")
    total = sum(len(v) for v in passed_by_show.values())
    add(f"{total} candidates across {sum(1 for v in passed_by_show.values() if v)} shows.")
    add("")
    for slug in sorted(passed_by_show):
        cands = passed_by_show[slug]
        if not cands:
            continue
        add(f"### {show_names.get(slug, slug)} (`{slug}`)")
        add("")
        add(
            "| Phrase | Side | Offset (median +- spread) | Support | Kind | "
            "Affinity | RateRatio | near/neutral/content | Conf | Match |"
        )
        add("|---|---|---|---|---|---|---|---|---|---|")
        for c in cands:
            add(
                f"| {' '.join(c.words)} | {c.side} | {c.median_offset_s:+.1f}s "
                f"+-{c.spread_s:.1f}s | {len(c.all_episodes)} ep "
                f"({len(c.gold_episodes)} gold) / {len(c.supporting_edges)} edges "
                f"| {c.support_kind} | {c.affinity:.2f} | {c.rate_ratio} "
                f"| {c.near_count}/{c.neutral_count}/{c.content_count} "
                f"| {c.confidence} | "
                f"{'exact' if len(c.words) <= SHORT_PHRASE_MAX_TOKENS else 'fuzzy'} |"
            )
        add("")
        add("Evidence:")
        for c in cands:
            add(f"- \"{' '.join(c.words)}\" [{c.side}]:")
            if c.span_shingle_count:
                add(
                    f"  - SPAN: represents a longer repeated span; merged "
                    f"{c.span_shingle_count} overlapping shingles reaching "
                    f"offset {c.span_end_offset_s:+.1f}s"
                )
            for o in c.supporting:
                add(f"  - {_fmt_occ(o)}")
            outliers = [o for o in c.occurrences if o not in c.supporting]
            for o in outliers[:5]:
                add(f"  - OUTLIER (outside spread tol): {_fmt_occ(o)}")
        add("")

    add("## Shows awaiting weak labels (growth path)")
    add("")
    add(
        "Transcripts exist but no gold or rediff edges do, so the miner has "
        "no weak supervision to learn from — candidate generation is "
        "structurally blocked, not empty-by-accident. These shows become "
        "minable as soon as rediff t1 lands for them (~24h after a second "
        "feed fetch) or gold labels are added. This is the on-device growth "
        "path for a user's own shows."
    )
    add("")
    if awaiting:
        add("| Show | Episodes w/ transcripts | Hours |")
        add("|---|---|---|")
        for row in awaiting:
            add(f"| {row['slug']} | {row['episodes']} | {row['hours']:.2f} |")
    else:
        add("(none — every transcribed show has at least one weak edge)")
    add("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def load_transcript_words(path: pathlib.Path) -> list:
    # errors="replace": a few corpus transcripts (conan/night-vale/
    # unexplained) carry invalid UTF-8 from raw whisper byte tokens.
    # Replacement is lossless for mining — normalisation strips every
    # character outside [a-z0-9] anyway.
    text = path.read_bytes().decode("utf-8", errors="replace")
    transcript = json.loads(text)
    return PROTO.build_word_stream(transcript["transcription"])


def load_feed_urls_by_slug(manifest_path: pathlib.Path | None) -> dict[str, list[str]]:
    if manifest_path is None or not manifest_path.is_file():
        return {}
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))
    feeds: dict[str, set[str]] = {}
    for entry in entries:
        slug, feed = entry.get("showSlug"), entry.get("feedUrl")
        if slug and feed:
            feeds.setdefault(slug, set()).add(feed)
    return {slug: sorted(urls) for slug, urls in feeds.items()}


def load_show_names(manifest_path: pathlib.Path | None) -> dict[str, str]:
    if manifest_path is None or not manifest_path.is_file():
        return {}
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        e["showSlug"]: e.get("show", e["showSlug"])
        for e in entries
        if e.get("showSlug")
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="xsdz.41 self-supervised per-show lexical anchor miner "
        "(zero hand-seeded phrases; see module docstring)."
    )
    parser.add_argument("--evaluation", type=pathlib.Path, default=DEFAULT_EVALUATION)
    parser.add_argument(
        "--rediff-baseline",
        type=pathlib.Path,
        default=None,
        help="rediff pair report (tier-a slot edges); omit for gold-only mining",
    )
    parser.add_argument(
        "--transcripts-dir", type=pathlib.Path, default=DEFAULT_TRANSCRIPTS_DIR
    )
    parser.add_argument(
        "--snapshots-manifest", type=pathlib.Path, default=DEFAULT_MANIFEST
    )
    parser.add_argument("--bank-out", type=pathlib.Path, default=None)
    parser.add_argument("--report-out", type=pathlib.Path, default=None)
    args = parser.parse_args(argv)

    evaluation_bytes = args.evaluation.read_bytes()
    evaluation = json.loads(evaluation_bytes)
    gold_edges, gold_intervals = load_gold_edges(evaluation)

    rediff_sha = None
    rediff_edges: list[Edge] = []
    rediff_intervals: dict[str, list[tuple[float, float]]] = {}
    if args.rediff_baseline is not None:
        rediff_bytes = args.rediff_baseline.read_bytes()
        rediff_sha = hashlib.sha256(rediff_bytes).hexdigest()
        rediff_edges, rediff_intervals = load_rediff_edges(
            json.loads(rediff_bytes), gold_edges
        )

    all_edges = gold_edges + rediff_edges
    guard_edge_sources(all_edges)

    transcript_paths = sorted(args.transcripts_dir.glob("*.json"))
    words_by_show: dict[str, dict[str, list]] = defaultdict(dict)
    for path in transcript_paths:
        eid = path.stem
        slug = PROTO.show_slug_from_episode_id(eid)
        words_by_show[slug][eid] = load_transcript_words(path)

    edges_by_show: dict[str, list[Edge]] = defaultdict(list)
    for edge in all_edges:
        edges_by_show[edge.show_slug].append(edge)
    intervals_by_episode: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for src in (gold_intervals, rediff_intervals):
        for eid, ivals in src.items():
            intervals_by_episode[eid].extend(ivals)

    def keep_rejected(cand: Candidate) -> bool:
        return any(matches_validation_target(cand, t) for t in VALIDATION_TARGETS)

    passed_by_show: dict[str, list[Candidate]] = {}
    rejected_by_show: dict[str, list[Candidate]] = {}
    for slug in sorted(edges_by_show):
        show_words = words_by_show.get(slug, {})
        if not show_words:
            continue
        passed, rejected = mine_show(
            slug,
            show_words,
            edges_by_show[slug],
            {eid: intervals_by_episode.get(eid, []) for eid in show_words},
            keep_rejected=keep_rejected,
        )
        passed_by_show[slug] = passed
        rejected_by_show[slug] = rejected
        print(
            f"{slug:42} edges={len(edges_by_show[slug]):3d} "
            f"candidates={len(passed):3d}",
            flush=True,
        )

    awaiting = []
    for slug in sorted(words_by_show):
        if slug in edges_by_show:
            continue
        eps = words_by_show[slug]
        hours = sum((w[-1].end_s if w else 0.0) for w in eps.values()) / 3600.0
        awaiting.append({"slug": slug, "episodes": len(eps), "hours": hours})

    scorecard = validation_scorecard(passed_by_show, rejected_by_show)
    show_names = load_show_names(args.snapshots_manifest)
    feed_urls = load_feed_urls_by_slug(args.snapshots_manifest)

    parameters = {
        "ngramTokens": [NGRAM_MIN, NGRAM_MAX],
        "tiers": {
            src: {"windowSeconds": t.window_s, "spreadToleranceSeconds": t.spread_tol_s}
            for src, t in ALLOWED_EDGE_SOURCES.items()
        },
        "minGoldEpisodes": MIN_GOLD_EPISODES,
        "minAnyEpisodes": MIN_ANY_EPISODES,
        "singleEpisodeMinEdges": SINGLE_EPISODE_MIN_EDGES,
        "minAffinity": MIN_AFFINITY,
        "minRateRatio": MIN_RATE_RATIO,
        "shortPhraseMaxTokens": SHORT_PHRASE_MAX_TOKENS,
        "fuzzyCountRatio": FUZZY_COUNT_RATIO,
        "edgeDedupeSeconds": EDGE_DEDUPE_S,
    }
    sources = {
        "weakEdgeSources": sorted(ALLOWED_EDGE_SOURCES),
        "evaluation": args.evaluation.name,
        "evaluationSha256": hashlib.sha256(evaluation_bytes).hexdigest(),
        "rediffBaseline": args.rediff_baseline.name if args.rediff_baseline else None,
        "rediffBaselineSha256": rediff_sha,
        "snapshotsManifest": (
            args.snapshots_manifest.name
            if args.snapshots_manifest and args.snapshots_manifest.is_file()
            else None
        ),
        "snapshotsManifestSha256": (
            _sha256_path(args.snapshots_manifest)
            if args.snapshots_manifest and args.snapshots_manifest.is_file()
            else None
        ),
        "transcriptsDigestSha256": transcripts_digest(transcript_paths),
        "transcriptCount": len(transcript_paths),
    }

    bank = build_bank(passed_by_show, show_names, feed_urls, parameters, sources)
    meta = {
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "evaluation": str(args.evaluation),
        "evaluation_sha256": sources["evaluationSha256"],
        "rediff": str(args.rediff_baseline) if args.rediff_baseline else None,
        "rediff_sha256": rediff_sha,
        "transcripts_dir": str(args.transcripts_dir),
        "transcript_count": len(transcript_paths),
        "transcripts_digest": sources["transcriptsDigestSha256"],
    }
    report = render_report(
        passed_by_show, rejected_by_show, scorecard, awaiting, show_names, meta
    )

    if args.bank_out:
        args.bank_out.parent.mkdir(parents=True, exist_ok=True)
        args.bank_out.write_text(
            json.dumps(bank, indent=1, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"bank written: {args.bank_out}")
    if args.report_out:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(report, encoding="utf-8")
        print(f"report written: {args.report_out}")

    print()
    print("validation scorecard:")
    for row in scorecard:
        status = "DISCOVERED" if row["found"] else "NOT DISCOVERED"
        best = (
            max(row["found"], key=lambda c: c.confidence) if row["found"] else None
        )
        detail = (
            f" best=\"{' '.join(best.words)}\" conf={best.confidence}"
            if best
            else ""
        )
        print(f"  {status:15} {row['target']['id']}{detail}")
    if not args.bank_out and not args.report_out:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
