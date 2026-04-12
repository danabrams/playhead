# Playhead Ad Detection Pipeline — Technical Overview

Playhead is an iOS podcast player that detects and skips ads **entirely on-device** (legal requirement — no audio or transcript ever leaves the phone). The pipeline is organized into sequential phases, each refining the previous phase's output.

---

## Phase 1 — Transcription

Raw audio is transcribed on-device via Apple's ASR (fast-pass and final-pass). Output: `TranscriptChunks` (time-stamped text segments).

## Phase 1.5 — Atomization

Chunks are converted into **TranscriptAtoms** — minimal, stable units of analysis with persistent identity. Each atom gets:

- A composite key: `(analysisAssetId, transcriptVersion, atomOrdinal)`
- A SHA256 content hash for matching
- Start/end timestamps
- Normalized text

Atom count scales with episode length — roughly 1–5 per second of audio (depends on ASR segmentation), so a 1-hour episode produces ~3,600–18,000 atoms. The transcript version itself is a SHA256 of all atom content, so **same transcript in = same ordinals out** — deterministic across reprocessing.

---

## Phase 2 — Hot-Path Detection (Real-Time)

Two parallel signals run ahead of the playback position with a **90-second lookahead**:

### Lexical Scanner

Pattern-based regex matching across the transcript. Each pattern category carries a weight, and hits are merged within a 30-second gap threshold. A candidate requires **≥2 pattern hits** to fire — unless a single hit scores ≥0.95 (e.g., a strong URL match), which bypasses the minimum.

Confidence formula: `1.0 - 1.0 / (1.0 + totalWeight × 0.3)`, capped at 0.95.

#### Sponsor Phrases — Weight 1.0

| Pattern |
|---------|
| `brought to you by` |
| `sponsored by` |
| `today s sponsor` |
| `thanks to our sponsor` |
| `this episode is sponsored` |
| `this podcast is brought` |
| `a word from our sponsor` |
| `message from our sponsor` |
| `supported by` |

#### Promo Code Patterns — Weight 1.2

| Regex |
|-------|
| `use code \w+` |
| `promo code \w+` |
| `discount code \w+` |
| `coupon code \w+` |
| `code \w+ at checkout` |
| `enter code \w+` |

#### URL / CTA Patterns — Weight 0.8

| Regex |
|-------|
| `\w+ com slash \w+` |
| `dot com slash \w+` |
| `check out \w+` |
| `head to \w+` |
| `go to \w+ com` |
| `visit \w+ com` |
| `head over to` |
| `\w+ dot com` |
| `click the link` |
| `link in the description` |
| `link in the show notes` |

#### Purchase Language — Weight 0.9

| Regex |
|-------|
| `free trial` |
| `money back guarantee` |
| `first month free` |
| `\d+ percent off` |
| `satisfaction guarantee` |
| `risk free` |
| `sign up today` |
| `sign up now` |
| `limited time offer` |
| `exclusive offer` |
| `special offer` |

#### Transition Markers — Weight 0.3

| Regex |
|-------|
| `let s get back to` |
| `and now back to` |
| `back to the show` |
| `back to the episode` |
| `anyway\b` |
| `without further ado` |
| `moving on` |

#### Strong URL Pattern — Weight 0.95 (single-hit bypass)

```regex
\b[a-z0-9][a-z0-9\-]*(?:\.[a-z0-9][a-z0-9\-]*)*\.(?:com|net|org|io|co|app|fm|tv)\b(?![a-z0-9])
```

Recognized TLDs: `.com`, `.net`, `.org`, `.io`, `.co`, `.app`, `.fm`, `.tv`

#### Show-Specific Sponsor Patterns — Weight 1.5 (boosted)

Loaded dynamically from `PodcastProfile.sponsorLexicon` and from active entries in the `SponsorKnowledgeStore`. Each term is compiled as a case-insensitive word-boundary regex: `\b<term>\b`.

### Rule-Based Acoustic Classifier

Stand-in classifier until a CoreML model is trained. Combines weighted signals:

| Signal | Weight | Trigger |
|--------|--------|---------|
| Lexical matches | 40% | From lexical scanner above |
| RMS energy drop | 20% | >35% mean delta at ad boundaries |
| Spectral flux change | 15% | >2× the 80th percentile of flux distribution |
| Music probability | 10% | 0.0–0.8 scored linearly; >0.8 capped at 0.5 |
| Speaker change | 5% | 2 speakers = 0.7, 3+ speakers = 1.0 |
| Show priors | 10% | Per-podcast historical ad density |

Sigmoid calibration (k=8.0, midpoint=0.25) maps the weighted sum to a [0.0, 1.0] probability. **Candidate threshold: 0.40** — anything above this is emitted as an `AdWindow`.

---

## Phase 3 — Foundation Model Backfill

Uses **Apple's on-device Foundation Model** (`LanguageModelSession`, iOS 26+). No network calls. Two passes:

### Coarse Screening

- Segments the transcript via `CoveragePlanner` (segments: 10–120s, split at pauses >1.5s)
- Feeds each window + an evidence catalog to the FM
- Output per window: `noAds`, `containsAd`, or `uncertain`
- Budget: **300 seconds of FM scanning** per backfill run
- Fixed overhead: ~3,700 tokens per call (schema framing)

### Refinement Pass (on `containsAd` / `uncertain` windows)

Extracts precise spans with: commercial intent assessment, ownership attribution, boundary markers, evidence anchors.

A separate **PermissiveAdClassifier** handles sensitive content (pharma/medical ads) that the standard FM refuses to classify — uses Apple's `permissiveContentTransformations` guardrail, falls back to `.uncertain` on refusal.

**Consensus requirement:** ≥2 overlapping FM windows must agree before an ad region is confirmed.

---

## Evidence Catalog (NLP Layer Fed to the FM)

Before FM calls, an `EvidenceCatalogBuilder` extracts structured evidence in 5 phases:

1. **Anchor extraction** — URLs, promo codes, disclosure phrases, CTA phrases (see patterns below)
2. **Commercial zone computation** from anchor positions
3. **Context-gated extraction** — CTAs and brand spans only near anchors (prevents false positives from general "check it out" usage)
4. **Deduplication** by (normalizedText, category)
5. **Reference assignment** — `[E0]`, `[E1]`, ... labels injected into the FM prompt

### Evidence Catalog — URL Patterns (11 patterns)

| Regex | Notes |
|-------|-------|
| `\b\w+\.com\/\w+` | Domain with path |
| `\b\w+\.com\b` | Bare .com domain |
| `\b\w+\.co\/\w+` | .co domain with path |
| `\b\w+\.org\/\w+` | .org domain with path |
| `\b\w+\.io\/\w+` | .io domain with path |
| `\b\w+ dot com slash \w+` | Spoken URL with path |
| `\b\w+ dot com\b` | Spoken bare domain |
| `\b(?!dot\b)\w+ com slash \w+` | ASR-normalized (no "dot") |
| `\bgo to \w+\.com` | Verb + domain |
| `\bvisit \w+\.com` | Verb + domain |
| `\bhead to \w+\.com` | Verb + domain |

### Evidence Catalog — Promo Code Patterns (7 patterns)

| Regex |
|-------|
| `\bpromo code\s+[A-Za-z0-9]+` |
| `\bdiscount code\s+[A-Za-z0-9]+` |
| `\bcoupon code\s+[A-Za-z0-9]+` |
| `\boffer code\s+[A-Za-z0-9]+` |
| `\benter code\s+[A-Za-z0-9]+` |
| `\buse code\s+[A-Za-z0-9]+` |
| `\bcode\s+[A-Za-z0-9]+\s+at checkout` |

### Evidence Catalog — CTA Phrases (19 patterns)

| Regex |
|-------|
| `\bget started today\b` |
| `\bsign up now\b` |
| `\bsign up today\b` |
| `\bclick the link\b` |
| `\blink in the description\b` |
| `\blink in the show notes\b` |
| `\btap the link\b` |
| `\bcheck it out\b` |
| `\bhead over to\b` |
| `\bgo check out\b` |
| `\btry it free\b` |
| `\btry it today\b` |
| `\bstart your free trial\b` |
| `\bget your free\b` |
| `\bdon.?t miss out\b` |
| `\bact now\b` |
| `\blimited time\b` |
| `\bexclusive offer\b` |
| `\bspecial offer\b` |

### Evidence Catalog — Disclosure Phrases (12 patterns)

| Regex |
|-------|
| `\bbrought to you by\b` |
| `\bsponsored by\b` |
| `\bpartnered with\b` |
| `\bin partnership with\b` |
| `\bthanks to our sponsor\b` |
| `\bthis episode is sponsored\b` |
| `\bthis podcast is brought\b` |
| `\ba word from our sponsor\b` |
| `\bmessage from our sponsor\b` |
| `\bsupported by\b` |
| `\btoday s sponsor\b` |
| `\btoday's sponsor\b` |

### Advertiser Extraction Pattern

```regex
(?:brought to you by|sponsored by|thanks to|a word from)\s+([a-z][a-z\s]{1,30}?)(?:\s*[,.\-!]|\s+(?:who|where|they|the|a|with|and|is|are|use|go|head|check|visit))
```

### Product Extraction Pattern

```regex
(?:try|check out|go to|visit|head to)\s+([a-z][a-z\s]{1,30}?)(?:\s*[,.\-!]|\s+(?:today|now|for|and|to|it|they|dot|com))
```

### Brand Extraction

After sponsor phrases like "brought to you by X" or "sponsored by X", a 1–4 word noun phrase is captured. Domain stems are also extracted from "X dot com" and "X.com" patterns.

**Brand stop words** (trimmed from trailing end of captures):
> and, or, but, for, the, a, an, to, at, in, on, with, from, of, by, as, they, we, it, is, are, was, were, has, have, that, this, will, can, so, if, do, did, just, really, very, also, then, now, here, there, about, like, make, visit, who, which, where, not, no, all, every, some, many, more

**Common non-brand phrases** (excluded entirely):
> our, the, this, their, your, my, our friends, our friends at, our partner, our partners, our sponsor, today, you, them, us

**URL verb prefixes** (stripped during normalization, longest-first):
> "head to ", "go to ", "visit ", "check out "

**Brand stem separators** (20 variants, tested in order):
- Paths: `.com/`, `.org/`, `.io/`, `.co/`
- Spoken with path: ` dot com slash `, ` dot org slash `, ` dot io slash `, ` dot co slash `
- Spoken normalized: ` com slash `, ` org slash `, ` io slash `, ` co slash `
- Domains: `.com`, `.org`, `.io`, `.co`
- Spoken domains: ` dot com`, ` dot org`, ` dot io`, ` dot co`

---

## PII Redaction (Before FM Calls)

A `PromptRedactor` strips sensitive content before any transcript reaches the Foundation Model.

### Vaccine Vocabulary — Placeholder: `[PRODUCT]`

| Type | Patterns |
|------|----------|
| Regex | `vaccines?`, `vaccinat(ions\|ion\|ing\|ed\|es\|e)`, `immuni[sz]ation`, `immuni[sz]e`, `boosters?` |
| Literal | inoculation, booster shot, flu shot, covid shot, the jab |

### Pharma Drug Brands — Placeholder: `[DRUG]`

> Trulicity, Ozempic, Rinvoq, Wegovy, Mounjaro, Pfizer, Moderna, AstraZeneca, Johnson & Johnson

### Mental Health Services — Placeholder: `[SERVICE]`

| Type | Patterns |
|------|----------|
| Literal | BetterHelp, online therapy, talk to a therapist |
| Regex | `licensed therapists?` |

### Regulated Medical Tests — Placeholder: `[TEST]`

> skin cancer screening, cholesterol test, COVID-19 test, COVID test

### Disease Names — Placeholder: `[CONDITION]`

Only masked when co-occurring with a trigger word from the categories above:

> shingles, RSV, pneumococcal pneumonia, pneumococcal, pneumonia, COVID-19, COVID, coronavirus, influenza

---

## Phase 4 — Region Proposal & Feature Extraction

`RegionProposalBuilder` merges signals from all origins (lexical, acoustic, sponsor knowledge, fingerprint matches, FM results) into unified `ProposedRegions`. `RegionFeatureExtractor` computes per-region acoustic features. Each region carries its FM consensus strength.

## Phase 5 — Atom Evidence Projection

`AtomEvidenceProjector` annotates every atom with evidence (anchored? provenance? acoustic hints?). Then `MinimalContiguousSpanDecoder` assembles contiguous ad spans:

| Parameter | Value |
|-----------|-------|
| Min span duration | 5.0 seconds |
| Max span duration | 180.0 seconds (split if longer) |
| Merge gap | ≤3 atoms |
| Boundary snap radius | 15 atoms |

**Anchoring requirements (in priority order):**

1. **FM Consensus** (primary): ≥ medium strength (0.7) from overlapping FM windows
2. **Evidence Catalog** (trustworthy): URL, promoCode, disclosurePhrase, or ctaPhrase (not brandSpan alone)
3. **Corroboration** (fallback): single-window FM (≥ low/0.35, < medium/0.7) + acoustic break (≥ 0.5 strength) within ±2 atoms — all three conditions must fire simultaneously

---

## Phase 6 — Fusion & Decisions

`BackfillEvidenceFusion` produces final confidence scores incorporating correction factors from user feedback. `DecisionMapper` applies skip policy eligibility:

| Threshold | Value | Purpose |
|-----------|-------|---------|
| Candidate | 0.40 | Minimum to emit a candidate |
| Confirmation | 0.70 | Auto-confirm during backfill |
| Suppression | 0.25 | Drop if confidence falls below |
| Auto-skip eligible | 0.75 | Promote to skip-eligible |

---

## Phase 6.5 — SkipOrchestrator (Real-Time Decisions)

The live decision layer between detection and playback transport.

### Hysteresis (prevents jitter)

- Enter ad state at confidence ≥ **0.65**
- Stay in ad state at confidence ≥ **0.45** (asymmetric to prevent oscillation)

### Merging

Adjacent ad windows with gaps < **4.0 seconds** are merged.

### Minimum Duration

Ads must be ≥ **15 seconds** — unless confidence ≥ **0.85**, which overrides the minimum.

### Boundary Snapping

Snaps start/end to detected silence within ± **3.0 seconds**.

Silence score formula: `pauseProbability × 0.7 + max(0, 1 - rms × 10) × 0.3`

Silence threshold: **0.6**

### Seek Suppression

After user seeks, skip is suppressed for **3.0 seconds**, requiring **2.0 seconds** of stability before re-engaging.

### Idempotency

Every decision is keyed by `(analysisAssetId + adWindowId + policyVersion)` and logged to SQLite.

---

## Phase 7 — User Corrections

`UserCorrectionStore` records weighted corrections per span. When a user taps "This isn't an ad," the correction factor suppresses future decisions for that region and feeds back into fusion scoring.

## Phase 8 — Sponsor Knowledge

`SponsorKnowledgeStore` persists known sponsor patterns per podcast for reuse across episodes. Active sponsors are compiled into boosted lexical patterns (weight 1.5) that feed back into Phase 2.

## Phase 9 — Fingerprinting (Recurring Ad Detection)

MinHash fingerprinting identifies recurring ad copy across episodes.

### Configuration

| Parameter | Value |
|-----------|-------|
| Hash functions | 128 |
| N-gram size | 4 (character n-grams) |
| Match threshold | Jaccard similarity ≥ 0.6 |
| Window size | ~30 atoms |
| Stride | windowSize / 3 (66% overlap) |

### Text Normalization

Before fingerprinting, text is lowercased, punctuation stripped, whitespace collapsed, and these **filler words** removed:

> um, uh, like, so, well, basically, actually, literally, right, okay

### Trust Lifecycle

| Transition | Requirement |
|------------|-------------|
| candidate → quarantined | ≥1 confirmation |
| quarantined → active | ≥2 confirmations, rollback rate ≤30% |
| active → decayed | rollback rate >50% |
| decayed → blocked | rollback rate >50% |

Only FM-attested evidence is eligible for fingerprint creation.

---

## User-Facing Output

### Banner

Pops up on first ad confirmation with advertiser/product metadata, time range, and confidence. User can:

- **"Skip"** — fires a `CMTimeRange` skip cue to the playback service
- **"Listen"** — reverts the skip, plays through
- **"Not an ad"** — sends false-positive correction to Phase 7

### BoundaryExpander (User Taps "Mark as Ad")

Expands a single tap point into full ad boundaries using a signal hierarchy:

| Priority | Signal | Search Radius | Confidence |
|----------|--------|---------------|------------|
| 1 (highest) | Existing AdWindows | ±5s adjacency | min(maxConf + 0.1, 1.0) |
| 2 | Acoustic + lexical fusion | ±60s acoustic, ±90s lexical | 0.85 |
| 3 | Acoustic only | ±60s | 0.55 |
| 4 (fallback) | Tap ± 30s, snapped to silence | ±10s silence snap | 0.30 |

---

## Future: CoreML Model

A small **GRU or 1-D conv** (~200KB) is planned to replace the rule-based acoustic classifier.

**Input features per window:** RMS, spectral flux, music probability, pause probability, speaker cluster ID (one-hot), lexical confidence, category flags (5 bools), position in episode (0–1).

**Output:** ad probability + boundary start/end adjustment offsets.

---

## Key Constants Reference

| Component | Parameter | Value |
|-----------|-----------|-------|
| **LexicalScanner** | mergeGapThreshold | 30.0s |
| | minHitsForCandidate | 2 |
| | highWeightBypassThreshold | 0.95 |
| **AcousticBreakDetector** | energyDropThreshold | 0.35 (35%) |
| | minAbsoluteRMSDifference | 0.25 |
| | pauseProbabilityThreshold | 0.6 |
| **RuleBasedClassifier** | sigmoidK | 8.0 |
| | sigmoidMidpoint | 0.25 |
| **AdDetectionConfig** | candidateThreshold | 0.40 |
| | confirmationThreshold | 0.70 |
| | suppressionThreshold | 0.25 |
| | hotPathLookahead | 90.0s |
| | autoSkipConfidenceThreshold | 0.75 |
| **SkipPolicyConfig** | enterThreshold | 0.65 |
| | stayThreshold | 0.45 |
| | mergeGapSeconds | 4.0 |
| | minimumSpanSeconds | 15.0 |
| | shortSpanOverrideConfidence | 0.85 |
| | boundarySnapMaxDistance | 3.0s |
| | silenceThreshold | 0.6 |
| **SkipOrchestrator** | seekSuppressionDuration | 3.0s |
| | seekStabilityRequired | 2.0s |
| **TranscriptSegmenter** | minSegmentDuration | 10.0s |
| | maxSegmentDuration | 120.0s |
| | pauseThreshold | 1.5s |
| **FM Backfill** | fmScanBudgetSeconds | 300s |
| | consensusWindows | ≥2 |
| | tokensPerCall (overhead) | ~3,700 |
| **MinimalContiguousSpanDecoder** | minDurationSeconds | 5.0 |
| | maxDurationSeconds | 180.0 |
| | mergeGapAtoms | 3 |
| | boundarySnapRadiusAtoms | 15 |
| **Fingerprinting** | hashCount | 128 |
| | ngramSize | 4 |
| | matchThreshold | 0.6 |
| | minConfirmationsForActive | 2 |
| | maxRollbackRateForActive | 0.3 |
| | rollbackSpikeThreshold | 0.5 |

---

## Key Design Properties

- **Fully on-device** — no audio or transcript ever leaves the phone
- **Deterministic** — same transcript produces identical atom ordinals across reprocessing
- **Multi-signal fusion** — no single signal makes the decision; lexical, acoustic, FM, fingerprint, and user corrections all contribute
- **Conservative by default** — hysteresis, minimum duration, seek suppression, and confidence gating all bias toward avoiding false skips
- **Self-improving** — fingerprints mature through a trust lifecycle; user corrections feed back into scoring; sponsor knowledge persists across episodes
