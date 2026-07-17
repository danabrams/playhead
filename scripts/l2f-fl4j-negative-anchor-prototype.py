#!/usr/bin/env python3
"""playhead-fl4j offline prototype: NEGATIVE (promo/keep) lexical anchors.

CONTEXT
-------
The pipeline currently auto-skips ~189s of borderline promo/sponsor content
that Dan wants KEPT (measured as `content_veto` segments in gold v6). Dan's
product decision is to route this grey class to a play-by-default skippable
BANNER instead of auto-skipping. This spike tests the DETECTION prerequisite:

    Can LEXICAL negative anchors reliably identify borderline promos and
    distinguish them from genuine third-party ads?

THE CRUX
--------
Real third-party ads AND self-promos both use sponsor language ("brought to
you by", "supported by", "sponsored by"). A bare phrase match CANNOT separate
them. The distinguishing signal is SELF-REFERENCE: a promo references the SHOW
ITSELF / its social / its network ("be a guest on our show", "follow us",
"rate review and subscribe"); a third-party ad references an EXTERNAL brand.
This script characterises whether lexical signals (phrases + entity
self-reference) can make that distinction, and how cleanly.

CLASSES (gold v6)
-----------------
  POSITIVE (should NOT auto-skip -> banner): the 20 `content_veto` segments.
           Subtyped from Dan's audit notes (self_promo / guest_plug /
           sponsored_editorial / content_boundary).
  NEGATIVE (should auto-skip): the 70 gold `full_break` segments (real ad
           breaks).

The classifier task is "flag as promo/keep" (positive) vs "leave as skip"
(negative). We report precision + recall of the positive flag, the false-fire
count on real ad breaks, per-subtype separability, the self-reference-signal
contribution, and cross-show generalisation.

DETERMINISM
-----------
No stochastic component. Phrase/token banks are fixed constants declared below
(seed reported as 0 for form). Inputs are frozen read-only fixtures; the script
writes only its report + candidate-bank JSON. Input SHA-256s are recorded.

STATUS
------
The emitted negative-anchor bank is status:candidate. It needs Dan's review
before any wiring (like xsdz.41). No production Swift, no bank commit here.

Usage:
  python3 scripts/l2f-fl4j-negative-anchor-prototype.py \
      --report-out /Users/dabrams/playhead-baselines/fl4j-negative-anchor-prototype-2026-07-16.md \
      --bank-out   /Users/dabrams/playhead-baselines/fl4j-negative-anchor-bank-candidate.json
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import pathlib
import random
import re
from collections import defaultdict

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_EVALUATION = (
    REPO_ROOT
    / "TestFixtures/Corpus/Evaluations/"
    "earaudit-oracle-gold-836b81885f6d279a84c1ef0dee83302e7df6ed28f0d20ec2db621b518f1ef220.json"
)
DEFAULT_TRANSCRIPTS_DIR = REPO_ROOT / "TestFixtures/Corpus/Transcripts"
DEFAULT_AUDITS_DIR = REPO_ROOT / "TestFixtures/Corpus/Audits"

# Review files carrying Dan's per-segment notes (the veto subtype labels).
REVIEW_FILES = (
    "danshows-firstgold-review-2026-07-16-eae11172eb31dcca93d44fbb4d3952ff550b824cabbed1e981be00b7ba72c159.json",
    "otm-ted-requalify-review-2026-07-16-777b1d67caf25105f1d7493068027e0d5af0609debfacd59e856c0fe40ce927f.json",
    "oracle-earaudit-review-2026-07-15-9e826c081d1969b8e6ed8cc405d3d6f5a50fbff77844f2baca38b5f5acea0925.json",
)

SEED = 0

# ---------------------------------------------------------------------------
# Normalisation + word stream (same rule as l2f-lexical-anchor-prototype.py)
# ---------------------------------------------------------------------------

_SPECIAL_TOKEN_MARKER = "[_"
_APOSTROPHES = "'’‘ʼ`´"
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_word(raw: str) -> str:
    text = raw.lower()
    for ch in _APOSTROPHES:
        text = text.replace(ch, "")
    return _NON_ALNUM_RE.sub("", text)


def normalize_text(raw: str) -> list[str]:
    return [w for w in (normalize_word(p) for p in raw.split()) if w]


def build_words(transcription: list[dict]) -> list[tuple[str, float, float]]:
    """Token stream -> [(norm_word, start_s, end_s)] with token-level times."""
    words: list[tuple[str, float, float]] = []
    for segment in transcription:
        cur_text = ""
        cur_start = None
        cur_end = None

        def flush():
            nonlocal cur_text, cur_start, cur_end
            if cur_text and cur_start is not None:
                nw = normalize_word(cur_text)
                if nw:
                    words.append((nw, cur_start / 1000.0, cur_end / 1000.0))
            cur_text, cur_start, cur_end = "", None, None

        for token in segment.get("tokens", ()):
            text = token.get("text", "")
            if _SPECIAL_TOKEN_MARKER in text:
                flush()
                continue
            offs = token.get("offsets", {})
            t_from = offs.get("from")
            t_to = offs.get("to")
            if text.startswith(" ") or cur_start is None:
                flush()
                cur_text = text
                cur_start, cur_end = t_from, t_to
            else:
                cur_text += text
                if t_to is not None:
                    cur_end = t_to
        flush()
    return words


def words_in_window(
    words: list[tuple[str, float, float]], start: float, end: float
) -> list[str]:
    """Normalised words whose token span overlaps [start, end]."""
    return [w for (w, s, e) in words if e >= start and s <= end]


# ---------------------------------------------------------------------------
# Anchor banks (fixed a priori; grounded in the observed self-promo patterns
# and standard podcast idioms). Because n_positive is tiny (20), these are
# in-sample; cross-show recurrence is reported as the generalisation check.
# ---------------------------------------------------------------------------

# (C) AMBIGUOUS sponsor phrases: appear in BOTH real ads and self-promos.
#     A bare match on these cannot discriminate — that is the whole point.
SPONSOR_PHRASES = (
    "brought to you by",
    "brought to you",
    "sponsored by",
    "supported by",
    "presented by",
    "partnership with",
    "in partnership with",
    "this episode is sponsored",
    "todays show also brought",
)

# (A) SELF-PROMO ACTION phrases: self-referential calls to the listener that
#     point at the show itself / how to engage with it. Show-agnostic idioms.
SELFPROMO_PHRASES = (
    "rate review and subscribe",
    "rate and review",
    "please rate",
    "rate review",
    "subscribe to our channel",
    "subscribe to the show",
    "subscribe to conan",
    "follow us",
    "reach us online",
    "find us on",
    "find out more at",
    "you can find the podcast",
    "you can find out more",
    "be a guest",
    "want to talk to",
    "talk to conan",
    "how to contact",
    "get in touch",
    "send us",
    "send questions",
    "your questions",
    "available to listen",
    "prefer to listen",
    "will always be available",
    "wherever you get your podcasts",
    "wherever fine podcasts",
    "get tickets",
    "live show",
    "live version",
    "on tour",
    "new ways to watch",
    "if you do want to watch",
    "you can watch",
    "want to talk to conan",
    "visit teamcoco",
)

# (S) SOCIAL platform single-tokens (membership test on the word set).
SOCIAL_TOKENS = frozenset(
    {
        "instagram",
        "facebook",
        "twitter",
        "tiktok",
        "youtube",
        "patreon",
        "bluesky",
        "threads",
        "substack",
    }
)
# Social phrase (two tokens after normalisation).
SOCIAL_PHRASES = ("blue sky", "apple podcasts")

# (B) SHOW SELF-REFERENCE: distinctive show-name tokens + a documented
#     handle/domain map. Derived per show (see build_show_tokens); the map
#     supplies handles/domains ASR renders as single words (punctuation is
#     stripped, so "teamcoco.com" -> "teamcoco", "@nikkiglaserpod" ->
#     "nikkiglaserpod"). These are the entity-match anchors.
SHOW_HANDLE_MAP = {
    "Conan O'Brien Needs A Friend": {"conan", "obrien", "teamcoco", "callconan"},
    "The Diary Of A CEO": {"diaryofaceo"},
    "Unexplained": {"unexplained", "unexplainedpod", "unexplainedpodcast"},
    "THEMOVE": {"themove"},
    "Ted Business": {"tedbusiness"},
    "On The Media": {"onthemedia", "wnyc"},
    "Techcrunch Daily Crunch": {"techcrunch"},
    "The Nikki Glaser Podcast": {"nikkiglaser", "nikkiglaserpod"},
    "Radiolab": {"radiolab"},
    "Smartless": {"smartless"},
    "Morbid": {"morbid"},
    "Why Is This Happening The Chris Hayes Po": {"wamu"},
}

# Generic tokens never counted as a distinctive show reference even if they
# appear in a show title (avoids a real ad tripping the entity match on a
# common word).
SHOW_STOPWORDS = frozenset(
    {
        "the", "a", "an", "of", "and", "is", "this", "to", "on", "in",
        "po", "pod", "podcast", "show", "daily", "needs", "friend",
        "business", "media", "wars", "true", "crime", "fresh", "air",
        "hard", "fork", "planet", "money", "stuff", "you", "should",
        "know", "chris", "hayes", "why", "happening", "with", "diary",
        "ceo", "crunch", "glaser", "nikki",
    }
)


def build_show_tokens(show_name: str) -> set[str]:
    """Distinctive tokens for a show: title tokens (len>=4, not stopword)
    merged with the documented handle/domain map."""
    toks = set()
    for w in normalize_text(show_name):
        if len(w) >= 4 and w not in SHOW_STOPWORDS:
            toks.add(w)
    toks |= SHOW_HANDLE_MAP.get(show_name, set())
    return toks


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------

def spaced(words: list[str]) -> str:
    return " " + " ".join(words) + " "


def phrase_hits(sp: str, spaced_words: str) -> bool:
    return (" " + " ".join(normalize_text(sp)) + " ") in spaced_words


def any_phrase(phrases, spaced_words) -> list[str]:
    return [p for p in phrases if phrase_hits(p, spaced_words)]


def feature_vector(words: list[str], show_tokens: set[str]) -> dict:
    """Boolean lexical features + the concrete hits for a segment."""
    sw = spaced(words)
    wordset = set(words)
    sponsor = any_phrase(SPONSOR_PHRASES, sw)
    selfpromo = any_phrase(SELFPROMO_PHRASES, sw)
    social_tok = sorted(wordset & SOCIAL_TOKENS)
    social_ph = any_phrase(SOCIAL_PHRASES, sw)
    social = social_tok + social_ph
    selfref = sorted(wordset & show_tokens)
    return {
        "sponsor": sponsor,
        "selfpromo": selfpromo,
        "social": social,
        "selfref": selfref,
        "f_sponsor": bool(sponsor),
        "f_selfpromo": bool(selfpromo),
        "f_social": bool(social),
        "f_selfref": bool(selfref),
    }


# Classifier definitions: name -> predicate over a feature vector.
CLASSIFIERS = {
    "C1_bare_sponsor": lambda f: f["f_sponsor"],
    "C2_selfpromo_lexical": lambda f: f["f_selfpromo"] or f["f_social"],
    "C3_selfref_only": lambda f: f["f_selfref"],
    "C4_selfpromo_OR_selfref": lambda f: (
        f["f_selfpromo"] or f["f_social"] or f["f_selfref"]
    ),
    "C5_sponsor_AND_selfref": lambda f: f["f_sponsor"] and f["f_selfref"],
}
CLASSIFIER_DESC = {
    "C1_bare_sponsor": "keep if any sponsor phrase present (baseline: fires on real ads too)",
    "C2_selfpromo_lexical": "keep if self-promo action phrase OR social platform present",
    "C3_selfref_only": "keep if show self-reference (entity match) present",
    "C4_selfpromo_OR_selfref": "keep if self-promo phrase OR social OR self-reference (proposed detector)",
    "C5_sponsor_AND_selfref": "keep if sponsor phrase AND show self-reference (entity-gated sponsor)",
}


# ---------------------------------------------------------------------------
# Subtype labelling from Dan's audit notes
# ---------------------------------------------------------------------------

def load_review_notes(audits_dir: pathlib.Path) -> tuple[dict, list[str]]:
    notes: dict[str, dict] = {}
    used = []
    for rf in REVIEW_FILES:
        p = audits_dir / rf
        if not p.is_file():
            continue
        used.append(rf)
        d = json.loads(p.read_text(encoding="utf-8"))
        for k, v in d.get("reviews", {}).items():
            notes[k] = v
    return notes, used


def subtype_from_note(note: str, episode_id: str) -> str:
    """Map Dan's free-text note to a promo subtype. Rule order matters."""
    n = (note or "").lower()
    if not n:
        # Only empty-note veto is the techcrunch insider-trading news read
        # (genuine news content, no promo language).
        if episode_id.startswith("techcrunch"):
            return "content_boundary"
        return "other"
    if "guest" in n and "plug" in n:
        return "guest_plug"
    if any(t in n for t in ("sponsor", "trivia", "contest", "product placement")):
        return "sponsored_editorial"
    if any(
        t in n
        for t in (
            "genuine content",
            "actual content",
            "silence",
            "stinger",
            "interview",
            "music playing it out",
            "news magazine",
        )
    ):
        return "content_boundary"
    if any(
        t in n
        for t in (
            "social media",
            "contact",
            "listen to the show",
            "watch the show",
            "new ways to watch",
            "live version",
            "get in touch",
            "send questions",
            "be a guest",
            "subscribe",
            "promoting",
            "promotion",
            "listen",
        )
    ):
        return "self_promo"
    return "other"


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def sha256_file(p: pathlib.Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def evaluate(evaluation_path, transcripts_dir, audits_dir):
    evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
    notes, used_reviews = load_review_notes(audits_dir)

    positives = []  # dicts for veto segments
    negatives = []  # dicts for full_break segments

    for asset in evaluation["assets"]:
        eid = asset["episode_id"]
        show = asset["show_name"]
        tp = transcripts_dir / f"{eid}.json"
        if not tp.is_file():
            continue
        words = build_words(json.loads(tp.read_text(encoding="utf-8"))["transcription"])
        show_tokens = build_show_tokens(show)

        for v in asset.get("content_vetoes", []):
            seg_words = words_in_window(words, v["start_seconds"], v["end_seconds"])
            note = ""
            for rid in v.get("source_review_ids", []):
                if rid in notes:
                    note = notes[rid].get("note", "")
                    break
            positives.append(
                {
                    "episode_id": eid,
                    "show": show,
                    "start": v["start_seconds"],
                    "end": v["end_seconds"],
                    "note": note,
                    "subtype": subtype_from_note(note, eid),
                    "features": feature_vector(seg_words, show_tokens),
                    "words": seg_words,
                }
            )
        for b in asset.get("full_breaks", []):
            seg_words = words_in_window(words, b["start_seconds"], b["end_seconds"])
            negatives.append(
                {
                    "episode_id": eid,
                    "show": show,
                    "start": b["start_seconds"],
                    "end": b["end_seconds"],
                    "features": feature_vector(seg_words, show_tokens),
                    "words": seg_words,
                }
            )
    return positives, negatives, used_reviews


def confusion(positives, negatives, predicate):
    tp = sum(1 for p in positives if predicate(p["features"]))
    fn = len(positives) - tp
    fp = sum(1 for n in negatives if predicate(n["features"]))
    tn = len(negatives) - fp
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall)
        else 0.0
    )
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


SUBTYPE_ORDER = ["self_promo", "guest_plug", "sponsored_editorial", "content_boundary", "other"]


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_candidate_bank(meta) -> dict:
    return {
        "status": "candidate",
        "bead": "playhead-fl4j",
        "purpose": (
            "Candidate NEGATIVE (promo/keep) lexical anchors for routing "
            "borderline promo/sponsor content to a play-by-default banner "
            "instead of auto-skipping. NOT wired; needs Dan's review (cf. xsdz.41)."
        ),
        "generated": meta["generated"],
        "provenance": (
            "Mined from the 20 gold-v6 content_veto segments + Dan's audit "
            "notes; measured against 70 gold full_breaks. See report."
        ),
        "families": {
            "ambiguous_sponsor_phrases": {
                "role": "MUST NOT be used bare — fires on real third-party ads too. "
                "Only meaningful when gated by show self-reference.",
                "phrases": list(SPONSOR_PHRASES),
            },
            "self_promo_action_phrases": {
                "role": "Positive promo/keep signal. Self-referential calls to the "
                "listener that point at the show itself.",
                "phrases": list(SELFPROMO_PHRASES),
            },
            "social_tokens": {
                "role": "Positive promo/keep signal (platform mentions).",
                "tokens": sorted(SOCIAL_TOKENS),
                "phrases": list(SOCIAL_PHRASES),
            },
            "show_self_reference": {
                "role": "Entity-match discriminator: distinctive show-name tokens "
                "+ handle/domain map. Present in self-promos, mostly absent in "
                "third-party ads. THE key lexical signal per the crux.",
                "handle_map": {k: sorted(v) for k, v in SHOW_HANDLE_MAP.items()},
                "title_stopwords": sorted(SHOW_STOPWORDS),
            },
        },
        "recommended_classifier": "C2_selfpromo_lexical",
        "recommended_classifier_rationale": (
            "C2 (self-promo action phrases + social handles, NOT bare sponsor "
            "phrases, NOT bare show self-reference) has the best precision (0.78) "
            "and avoids the '<Show> is supported by <brand>' underwriting false "
            "fires. Scope it to the self-promo/social subclass only. C4 adds bare "
            "self-reference for +1 recall but drops precision to 0.57."
        ),
        "known_failure_modes": [
            "sponsored_editorial subtype (host-read sponsor woven into content, "
            "e.g. THEMOVE trivia/betting) is lexically identical to a real ad — "
            "not separable without semantic relevance / entity KB.",
            "guest_plug subtype (guest promotes their OWN external project) has no "
            "show self-reference and looks like a third-party ad.",
            "content_boundary vetoes carry no promo language at all (boundary/"
            "stinger artifacts) — out of scope for a promo classifier.",
            "shows that embed their own self-promo INSIDE the ad pod (Nikki "
            "Glaser 'follow @nikkiglaserpod subscribe', Unexplained social outro "
            "bleeding into the break) make self-reference false-fire on negatives.",
        ],
    }


def render_report(positives, negatives, used_reviews, meta) -> str:
    L = []
    add = L.append

    # -- subtype counts
    by_sub = defaultdict(list)
    for p in positives:
        by_sub[p["subtype"]].append(p)

    add("# playhead-fl4j — negative (promo/keep) lexical-anchor prototype")
    add("")
    add(f"- Generated: {meta['generated']} by `scripts/l2f-fl4j-negative-anchor-prototype.py`")
    add(f"- Evaluation (gold v6): `{meta['evaluation']}`")
    add(f"  - sha256 `{meta['evaluation_sha256']}`")
    add(f"- Transcripts: `{meta['transcripts_dir']}` (fast-ASR corpus; quality varies)")
    add(f"- Audit notes: {', '.join('`'+r+'`' for r in used_reviews)}")
    add(f"- Seed: {SEED} (no stochastic component; fixed phrase/token banks)")
    add("")
    add(
        f"Positive class (should NOT auto-skip -> banner): **{len(positives)} gold "
        f"content_veto segments**. Negative class (should auto-skip): "
        f"**{len(negatives)} gold full_breaks** (real ad breaks)."
    )
    add("")

    # -- executive verdict (hand-authored conclusion, encoded for regenerability)
    add("## Verdict: PARTIAL")
    add("")
    add(
        "Lexical negative anchors **cleanly separate ONE promo subclass** from real "
        "ads and **cannot separate the others** without a semantic signal:"
    )
    add("")
    add(
        "- **GO for the self-promo / social subclass** (7/20 vetoes): "
        "rate-review-subscribe, social handles, be-a-guest / contact-the-show, "
        "live-show / get-tickets, new-ways-to-watch. Recall 7/7; a self-promo "
        "ACTION-phrase detector (C2) hits precision 0.78 with only 2 tightenable "
        "false fires. These carry self-referential CALLS TO ACTION and no "
        "third-party brand, so they are lexically distinctive."
    )
    add(
        "- **NO-GO for the sponsored-editorial subclass** (5/20, all THEMOVE "
        "trivia/betting reads) and the **guest-plug** (1/20): lexically IDENTICAL "
        "to a third-party ad (\"brought to you by X, go to X.com\"). Recall 0/5 and "
        "0/1. Dan keeps them for \"relevant context\" / \"product placement woven "
        "into the show\" - a SEMANTIC judgment lexicon cannot make. Needs an "
        "entity KB or on-device FM relevance classifier."
    )
    add(
        "- **OUT OF SCOPE: the content-boundary subclass** (7/20): genuine "
        "content / music tail / silence / stinger the pipeline over-captured. They "
        "carry NO promo language; a promo classifier is the wrong tool. These are "
        "boundary/stinger bugs (xsdz.38 territory), miscounted into the veto total."
    )
    add("")
    add(
        "**Biggest risk / why not full GO:** show self-reference is NOT a clean "
        "standalone discriminator. The \"<Show/Network> is supported by <external "
        "brand>\" underwriting/host-read construction makes the show name co-occur "
        "with genuine third-party sponsors (\"WNYC Studios is supported by Proof on "
        "Broadway\", \"AudioLab is supported by strawberry.me\", THEMOVE \"this "
        "episode is brought to you by Tofosi Optics\") - so a naive self-reference "
        "gate false-fires on real ads (4/70 negatives). The working signal is the "
        "self-promo ACTION verb, not the entity match. Secondary risk: cross-show "
        "generalisation is unproven - the mined self-promo phrases are almost all "
        "single-show (Conan + Unexplained); only the CATEGORIES are plausibly "
        "show-agnostic, and only ~3 shows contributed labeled self-promos."
    )
    add("")
    add(
        "**Recommended next step:** ship a NARROW self-promo detector scoped to the "
        "self-promo/social subclass (self-promo action phrases + social handles + "
        "show-handle URL match), explicitly NOT using bare sponsor phrases and NOT "
        "bare self-reference; validate it on a larger multi-show labeled self-promo "
        "set to prove generalisation before wiring. Route sponsored-editorial / "
        "guest-plug greys to a SEPARATE relevance track (entity KB or FM), and send "
        "the content-boundary vetoes to the boundary/stinger program, not here. "
        "The emitted bank is status:candidate pending Dan's review (cf. xsdz.41)."
    )
    add("")

    add("## The question")
    add("")
    add(
        "Can LEXICAL negative anchors identify borderline promos and distinguish "
        "them from genuine third-party ads well enough to route to a banner? Real "
        "ads and self-promos share sponsor language (\"brought to you by\", "
        "\"sponsored by\"), so a bare phrase match cannot separate them. The tested "
        "discriminator is SELF-REFERENCE: does the segment reference the SHOW "
        "itself (its name / social / network) rather than an external brand?"
    )
    add("")

    # -- positive-class decomposition
    add("## Positive class decomposition (Dan's audit notes -> subtype)")
    add("")
    add(
        "The veto class is heterogeneous. Only some vetoes are actually borderline "
        "PROMOS; several are boundary/stinger artifacts (genuine content or music "
        "the pipeline over-captured) that carry no promo language at all and are "
        "**out of scope for any promo classifier**."
    )
    add("")
    add("| Subtype | n | What it is | Lexically separable from real ads? |")
    add("|---|---|---|---|")
    sep = {
        "self_promo": "YES — self-URL/social/subscribe/be-a-guest + show self-reference",
        "guest_plug": "NO — guest promotes their OWN external project (looks like a 3rd-party ad)",
        "sponsored_editorial": "NO — identical 'brought to you by X, go to X.com' language; needs semantic relevance",
        "content_boundary": "N/A — not a promo (boundary/stinger/silence artifact); no promo language",
        "other": "-",
    }
    what = {
        "self_promo": "show self-promo: rate/review/subscribe, social, be-a-guest, live show, watch",
        "guest_plug": "a guest plugging their own project",
        "sponsored_editorial": "host-read sponsor woven into content (THEMOVE trivia/betting)",
        "content_boundary": "genuine content / music tail / silence / stinger over-captured at a boundary",
        "other": "unclassified",
    }
    for st in SUBTYPE_ORDER:
        if st in by_sub:
            add(f"| {st} | {len(by_sub[st])} | {what[st]} | {sep[st]} |")
    add("")
    add("Per-veto subtype assignments (audit-verifiable):")
    add("")
    add("| Show | start-end (s) | subtype | Dan's note |")
    add("|---|---|---|---|")
    for p in sorted(positives, key=lambda x: (x["subtype"], x["show"], x["start"])):
        note = (p["note"] or "").replace("|", "/").strip()
        if len(note) > 90:
            note = note[:87] + "..."
        add(
            f"| {p['show'][:22]} | {p['start']:.0f}-{p['end']:.0f} | {p['subtype']} | {note} |"
        )
    add("")

    # -- classifier confusion table
    add("## Classifier confusion (promo/keep = positive)")
    add("")
    add(
        f"Positives = {len(positives)} vetoes; Negatives = {len(negatives)} real ad "
        "breaks. **FP = false fire on a real ad break** (would wrongly banner a "
        "real ad). **FN = missed promo** (would still auto-skip a keep)."
    )
    add("")
    add("| Classifier | Rule | TP | FP | FN | TN | Precision | Recall | F1 |")
    add("|---|---|---|---|---|---|---|---|---|")
    results = {}
    for name, pred in CLASSIFIERS.items():
        c = confusion(positives, negatives, pred)
        results[name] = c
        add(
            f"| {name} | {CLASSIFIER_DESC[name]} | {c['tp']} | {c['fp']} | {c['fn']} "
            f"| {c['tn']} | {c['precision']:.2f} | {c['recall']:.2f} | {c['f1']:.2f} |"
        )
    add("")

    # -- self-reference contribution
    add("## Self-reference signal contribution")
    add("")
    n_selfref_pos = sum(1 for p in positives if p["features"]["f_selfref"])
    n_selfref_neg = sum(1 for n in negatives if n["features"]["f_selfref"])
    n_sponsor_neg = sum(1 for n in negatives if n["features"]["f_sponsor"])
    add(
        f"- Show self-reference present in **{n_selfref_pos}/{len(positives)} "
        f"positives** vs **{n_selfref_neg}/{len(negatives)} negatives**."
    )
    add(
        f"- Bare sponsor phrase present in **{n_sponsor_neg}/{len(negatives)} "
        "negatives** — this is why bare phrase matching (C1) is useless: it fires "
        "on essentially every real ad break."
    )
    c2, c4 = results["C2_selfpromo_lexical"], results["C4_selfpromo_OR_selfref"]
    add(
        f"- Adding self-reference (C2 -> C4) changes recall {c2['recall']:.2f} -> "
        f"{c4['recall']:.2f} and false fires {c2['fp']} -> {c4['fp']}."
    )
    c5 = results["C5_sponsor_AND_selfref"]
    add(
        f"- Entity-gating a bare sponsor phrase (C5 = sponsor AND self-ref): "
        f"precision {c5['precision']:.2f}, recall {c5['recall']:.2f}, "
        f"false fires {c5['fp']} — tests whether the entity match can rescue the "
        "ambiguous 'brought to you by' cases."
    )
    add("")

    # -- per-subtype recall for proposed detector
    add("## Per-subtype separability (proposed detector C4)")
    add("")
    add("| Subtype | n | Recall (C4) | Notes |")
    add("|---|---|---|---|")
    pred = CLASSIFIERS["C4_selfpromo_OR_selfref"]
    for st in SUBTYPE_ORDER:
        if st not in by_sub:
            continue
        segs = by_sub[st]
        hit = sum(1 for p in segs if pred(p["features"]))
        add(f"| {st} | {len(segs)} | {hit}/{len(segs)} | {sep[st]} |")
    add("")

    # -- false fires on real ad breaks (the dangerous errors)
    add("## False fires on real ad breaks (C4) — the dangerous errors")
    add("")
    add(
        "These real ad breaks would be wrongly flagged as promo/keep (bannered "
        "instead of skipped). Cause is shown."
    )
    add("")
    ff = [n for n in negatives if pred(n["features"])]
    if ff:
        add("| Show | start-end (s) | fired on |")
        add("|---|---|---|")
        for n in sorted(ff, key=lambda x: (x["show"], x["start"])):
            fea = n["features"]
            causes = []
            if fea["selfpromo"]:
                causes.append("selfpromo:" + ",".join(fea["selfpromo"][:2]))
            if fea["social"]:
                causes.append("social:" + ",".join(fea["social"][:2]))
            if fea["selfref"]:
                causes.append("selfref:" + ",".join(fea["selfref"][:2]))
            add(f"| {n['show'][:22]} | {n['start']:.0f}-{n['end']:.0f} | {'; '.join(causes)} |")
    else:
        add("(none)")
    add("")

    # -- missed promos (false negatives among the true-promo subtypes)
    add("## Missed promos (C4 false negatives)")
    add("")
    fns = [p for p in positives if not pred(p["features"])]
    if fns:
        add("| Show | start-end (s) | subtype | why missed |")
        add("|---|---|---|---|")
        for p in sorted(fns, key=lambda x: (x["subtype"], x["show"])):
            fea = p["features"]
            why = []
            if fea["sponsor"]:
                why.append("only ambiguous sponsor phrase (" + ",".join(fea["sponsor"][:1]) + ")")
            if not any([fea["f_selfpromo"], fea["f_social"], fea["f_selfref"]]):
                if not why:
                    why.append("no promo/self-ref lexical signal at all")
            add(f"| {p['show'][:22]} | {p['start']:.0f}-{p['end']:.0f} | {p['subtype']} | {'; '.join(why) or '-'} |")
    else:
        add("(none)")
    add("")

    # -- cross-show generalisation
    add("## Cross-show generalisation of self-promo anchors")
    add("")
    add(
        "For each self-promo/social phrase, the number of DISTINCT shows in which "
        "it fires on a positive (veto) segment. Phrases firing in >=2 shows are "
        "show-agnostic; single-show phrases risk over-fitting."
    )
    add("")
    phrase_shows = defaultdict(set)
    for p in positives:
        sw = spaced(p["words"])
        for ph in SELFPROMO_PHRASES:
            if phrase_hits(ph, sw):
                phrase_shows[("selfpromo", ph)].add(p["show"])
        for ph in SOCIAL_PHRASES:
            if phrase_hits(ph, sw):
                phrase_shows[("social", ph)].add(p["show"])
        for tok in set(p["words"]) & SOCIAL_TOKENS:
            phrase_shows[("social_tok", tok)].add(p["show"])
    if phrase_shows:
        add("| Family | Phrase/token | # shows | shows |")
        add("|---|---|---|---|")
        for (fam, ph), shows in sorted(
            phrase_shows.items(), key=lambda kv: (-len(kv[1]), kv[0][1])
        ):
            add(
                f"| {fam} | {ph} | {len(shows)} | "
                f"{', '.join(sorted(s[:14] for s in shows))} |"
            )
    else:
        add("(no self-promo phrases fired)")
    add("")

    # -- headline + verdict placeholder (verdict is written in the .md by hand-review;
    #    here we emit the measured facts the verdict rests on)
    add("## Measured headline facts")
    add("")
    c1 = results["C1_bare_sponsor"]
    sp_pos = [p for p in positives if p["subtype"] in ("self_promo",)]
    sp_hit = sum(1 for p in sp_pos if pred(p["features"]))
    spon_pos = by_sub.get("sponsored_editorial", [])
    spon_hit = sum(1 for p in spon_pos if pred(p["features"]))
    add(
        f"1. Bare sponsor phrase (C1) fires on {c1['fp']}/{len(negatives)} real ad "
        f"breaks -> precision {c1['precision']:.2f}. Bare phrases cannot discriminate."
    )
    add(
        f"2. Proposed detector C4 (self-promo OR self-reference): precision "
        f"{results['C4_selfpromo_OR_selfref']['precision']:.2f}, recall "
        f"{results['C4_selfpromo_OR_selfref']['recall']:.2f}, "
        f"{results['C4_selfpromo_OR_selfref']['fp']} false fires on real ads."
    )
    add(
        f"3. self_promo subtype recall {sp_hit}/{len(sp_pos)} — the clean, "
        "lexically separable class."
    )
    add(
        f"4. sponsored_editorial subtype recall {spon_hit}/{len(spon_pos)} — but "
        "even when caught it is NOT separable from real ads by lexicon alone "
        "(same words); relevance is a semantic judgment."
    )
    add(
        f"5. Show self-reference: {n_selfref_pos}/{len(positives)} positives vs "
        f"{n_selfref_neg}/{len(negatives)} negatives carry it — contaminated by "
        "'<Show> is supported by <brand>' underwriting reads."
    )
    add("")
    add(
        "_Verdict (PARTIAL), risk, and next step are stated in the Verdict section "
        "at the top, derived from these measured facts._"
    )
    add("")
    return "\n".join(L) + "\n"


def main(argv=None) -> int:
    random.seed(SEED)
    ap = argparse.ArgumentParser(description="playhead-fl4j negative-anchor prototype")
    ap.add_argument("--evaluation", type=pathlib.Path, default=DEFAULT_EVALUATION)
    ap.add_argument("--transcripts-dir", type=pathlib.Path, default=DEFAULT_TRANSCRIPTS_DIR)
    ap.add_argument("--audits-dir", type=pathlib.Path, default=DEFAULT_AUDITS_DIR)
    ap.add_argument("--report-out", type=pathlib.Path, default=None)
    ap.add_argument("--bank-out", type=pathlib.Path, default=None)
    ap.add_argument("--json-out", type=pathlib.Path, default=None)
    args = ap.parse_args(argv)

    positives, negatives, used_reviews = evaluate(
        args.evaluation, args.transcripts_dir, args.audits_dir
    )
    meta = {
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "evaluation": str(args.evaluation),
        "evaluation_sha256": sha256_file(args.evaluation),
        "transcripts_dir": str(args.transcripts_dir),
    }
    report = render_report(positives, negatives, used_reviews, meta)
    bank = build_candidate_bank(meta)

    if args.report_out:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(report, encoding="utf-8")
        print(f"report written: {args.report_out}")
    if args.bank_out:
        args.bank_out.parent.mkdir(parents=True, exist_ok=True)
        args.bank_out.write_text(json.dumps(bank, indent=2) + "\n", encoding="utf-8")
        print(f"bank written: {args.bank_out}")
    if args.json_out:
        payload = {
            "meta": meta,
            "confusion": {
                name: confusion(positives, negatives, pred)
                for name, pred in CLASSIFIERS.items()
            },
            "positives": [
                {k: p[k] for k in ("episode_id", "show", "start", "end", "subtype", "note", "features")}
                for p in positives
            ],
            "negatives_flagged_by_C4": [
                {"show": n["show"], "start": n["start"], "end": n["end"], "features": n["features"]}
                for n in negatives
                if CLASSIFIERS["C4_selfpromo_OR_selfref"](n["features"])
            ],
        }
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        print(f"json written: {args.json_out}")

    if not (args.report_out or args.bank_out or args.json_out):
        print(report)
    else:
        # brief stdout summary
        c4 = confusion(positives, negatives, CLASSIFIERS["C4_selfpromo_OR_selfref"])
        print(
            f"positives={len(positives)} negatives={len(negatives)} | "
            f"C4 precision={c4['precision']:.2f} recall={c4['recall']:.2f} "
            f"FP={c4['fp']} FN={c4['fn']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
