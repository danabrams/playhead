# Span Expansion & Signal Improvements — Full Design (v2)

**Date:** 2026-04-12
**Status:** Draft v2 — revised per expert review
**Approach:** Layered Hybrid (Approach C) — new orchestrator delegates to existing components

## Problem Statement

The current ad detection pipeline seeds on strong CTA evidence (URLs, promo codes, disclosure phrases) and never grows that seed into the full ad span. The lexical merge window is 30 seconds, but host-read ads commonly run 45–90 seconds — so the sponsor intro and the CTA land in separate clusters, and only the CTA cluster becomes a candidate. The result is detecting the last 10–20% of most host-read ads.

Contributing factors:
- Fixed 30s lexical merge gap misses intro-to-CTA spans
- "Trustworthy" evidence is CTA/URL/promo/disclosure-shaped — no "ad body" concept
- Boundary operations use atom counts (which vary 1–5 per second) instead of stable time units
- Boundary snapping relies only on silence, but host-read ads often transition on speaker turns, music beds, or compression changes
- EvidenceCatalogBuilder deduplicates away repetition count/density, which is a strong ad signal
- BoundaryExpander (which solves exactly this problem) only runs on user taps, never on machine-found seeds
- ASR misrecognizes sponsor names / URLs, causing anchor misses at the transcript layer
- FM budget spent on generic window classification instead of targeted boundary extraction
- Fingerprints only recover CTA fragments, not full ad spans
- Generic redaction placeholders erase distinctions the FM needs

## Phasing

Ordered by lift-to-effort ratio. ASR bias moved to Phase A because anchor misses at the transcript layer are a root cause, and Apple exposes contextual biasing directly.

### Phase A — Fast, Low-Risk Wins

1. Auto-apply BoundaryExpander to machine-found seeds (anchor-type-aware)
2. Replace fixed 30s merge with 60–90s adHypothesis windows
3. Preserve evidence repetition density + sponsor entity extraction
4. ASR bias with sponsor vocabulary + alternative transcript rescans
5. Boundary math in seconds + conservative multi-cue snapping
6. Metrics split + live lead-time / signed-boundary-bias metrics

### Phase B — Heavier Signal / Model Work

7. Optional speaker-change proxy / diarization
8. Music-bed detection improvements
9. Retask FM into structured boundary extraction on suspicious windows
10. Fingerprint full-span recovery via anchor-aware local alignment
11. Selective typed redaction fallback

---

# Phase A Design

## A1. Audio Signal Additions

Speaker labels are **optional and non-blocking**. Phase A must work when `speakerId` is always `nil`. Speaker diarization is deferred to Phase B until availability is verified on target builds.

Introduce `speakerChangeProxyScore` as an optional cue sourced in priority order:
1. Validated ASR / transcriber speaker labels if available on target builds
2. Offline diarization / speaker embedding change detector (Phase B)
3. Fallback acoustic turn-change proxy: pause + spectral shift + timbre change

New parallel `SoundAnalysis` pass using `SNClassifySoundRequest` with `SNClassifierIdentifier.version1` — extract auxiliary music likelihood per time window. Music is an **auxiliary cue**, not a primary boundary owner.

**FeatureWindow new fields:**

| Field | Type | Source |
|-------|------|--------|
| `speakerId` | `Int?` | Optional — validated source only, `nil` until Phase B |
| `speakerChangeProxyScore` | `Double` (0–1) | Best-available turn-change likelihood (acoustic fallback in Phase A) |
| `musicProbability` | `Double` (0–1) | Auxiliary music likelihood from SoundAnalysis |
| `musicBedChangeScore` | `Double` (0–1) | Change in under-bed music / bed onset / bed offset |

These feed into both SpanHypothesisEngine and TimeBoundaryResolver, but Phase A degrades gracefully when speaker data is absent.

---

## A2. SpanHypothesisEngine — Core State Machine

**New file:** `Playhead/Services/AdDetection/SpanHypothesisEngine.swift`

SpanHypothesisEngine is the **single owner of provisional span boundaries**. No other component discovers or relocates boundaries. TimeBoundaryResolver is the only component allowed to snap.

**States:**

```
idle → seeded → accumulating → confirmed → closed
```

- **idle:** No active hypothesis.
- **seeded:** A strong anchor fired (disclosure, sponsor lexicon, URL, promo code, FM-positive). The engine opens a hypothesis window and starts looking for corroborating evidence.
- **accumulating:** Body evidence is arriving (repeated brand mentions, benefit language, imperative language, weaker lexical hits). The hypothesis window stays open subject to idle-gap and decay logic.
- **confirmed:** Enough evidence exists to represent a contiguous ad, but final boundary snapping has not been applied yet.
- **closed:** Final boundaries have been snapped via TimeBoundaryResolver and emitted as a `CandidateAdSpan`.

**Hypothesis struct:**

```swift
struct AdHypothesis {
    let seedAnchor: AnchorEvent
    let seedTime: Double               // seconds into episode
    let anchorType: AnchorType
    var sponsorEntity: NormalizedSponsor?  // extracted sponsor identity
    var polarity: AnchorPolarity           // startAnchored, endAnchored, neutral
    var bodyEvidence: [BodyEvidenceItem]
    var closingAnchor: AnchorEvent?
    var windowDuration: Double         // from config, anchor-type-aware
    var expandedBoundary: ExpandedBoundary?
    var evidenceScore: Double          // accumulated weighted evidence
    var confidence: Double
    var lastEvidenceTime: Double       // for idle-gap detection
    var startCandidateTime: Double     // best-guess ad start
    var endCandidateTime: Double       // best-guess ad end
}

enum AnchorType {
    case disclosure
    case sponsorLexicon
    case url
    case promoCode
    case fmPositive
    case transitionMarker
}

enum AnchorPolarity {
    case startAnchored   // disclosure, sponsor intro — search forward
    case endAnchored     // CTA, URL, promo code — search backward
    case neutral         // FM-positive — search both directions
}
```

**Anchor-type-aware configuration** (`SpanHypothesisConfig`):

| Anchor Type | Polarity | Window Duration | Backward Search | Forward Search |
|-------------|----------|----------------|-----------------|----------------|
| disclosure / sponsorLexicon | startAnchored | 90s | 15s | 90s |
| url / promoCode | endAnchored | 75s | 75s | 15s |
| fmPositive | neutral | 60s | 30s | 30s |
| transitionMarker | endAnchored | 60s | 60s | 5s |

Additional config parameters:

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `maxIdleGapSeconds` | 20s | Close hypothesis if no evidence arrives within this gap |
| `evidenceDecayRate` | 0.95/s | Body evidence weight decays over time |
| `minConfirmedEvidence` | 2.5 | Minimum evidenceScore to reach `confirmed` state |
| `minBodyWeight` | 1.5 | Minimum for timeout-emitted backfill-only candidates |

All values configurable for rapid iteration.

**Integration point:** SpanHypothesisEngine owns the candidate span lifecycle. It consumes anchor events from LexicalScanner, FM, and fingerprint matches. It calls BoundaryExpander.expand() with anchor-type-aware config and TimeBoundaryResolver.snap() for final edges. It emits `CandidateAdSpan` objects with time-based boundaries.

---

## A3. AdHypothesis Window — Replacing Fixed 30s Merge

**LexicalScanner stays stateless.** It keeps scanning chunks and emitting `LexicalHit`s. The 30s merge logic remains as a fallback for when no hypothesis is active. The primary merge path moves to SpanHypothesisEngine.

**Hypothesis lifecycle:**

1. **Open:** LexicalScanner emits a hit with category `.sponsor` or `.disclosurePhrase`, or a sponsor lexicon match fires (weight 1.5). Engine opens an `AdHypothesis` with anchor-type-aware duration and polarity from config.

2. **Accumulate:** While the hypothesis is open, lexical hits contribute as body evidence subject to:
   - `maxIdleGapSeconds` — if no evidence arrives within this gap, close the hypothesis
   - Evidence decay over time — older body evidence contributes less to `evidenceScore`
   - Sponsor-entity compatibility — when `sponsorEntity` is set, evidence mentioning a different sponsor does not contribute

3. **Close explicitly:** A CTA, URL, or promo code hit arrives → closing anchor. Engine transitions to `confirmed`, then calls TimeBoundaryResolver.snap() and emits a `CandidateAdSpan`.

4. **Close on explicit return markers only:** `back to the show`, `back to the episode`, `and now back to ...` close immediately. Generic discourse markers (`anyway`, `moving on`) contribute weak closing evidence but **never close a hypothesis by themselves**.

5. **Close on timeout:** Window duration expires with no closing anchor. Only emit a **backfill-only candidate** (never skip-eligible directly) unless:
   - `evidenceScore` exceeds `minConfirmedEvidence`, AND
   - the hypothesis has both plausible start and end candidates

6. **Close on idle gap:** No evidence for `maxIdleGapSeconds` → close with whatever has accumulated.

**Disclosure single-hit bypass:** When a disclosure phrase co-occurs with a sponsor lexicon match in the same hypothesis window, bypass the 2-hit minimum. Improves recall on sponsor intros that currently get dropped.

**Overlapping hypotheses:** Merge only if:
- Normalized sponsor entity matches, OR
- One side lacks sponsor identity and overlap exceeds a stricter threshold

Otherwise keep separate and prefer the higher-confidence hypothesis.

---

## A4. ASR Bias with Sponsor Vocabulary

Moved from Phase B. This is one of the cheapest wins — Apple exposes contextual biasing directly, and ASR misses are a root cause of anchor failures.

**New struct:** `ASRVocabularyProvider` in `Playhead/Services/TranscriptEngine/`

Compiles vocabulary into the active ASR path:
- `AnalysisContext.contextualStrings` for SpeechAnalyzer / DictationTranscriber (newer path)
- `SFSpeechRecognitionRequest.contextualStrings` for the legacy path

The provider API abstracts over both paths.

**Vocabulary sources (strict priority budget — active sponsors first):**

| Priority | Source | Examples |
|----------|--------|----------|
| 1 (highest) | Active sponsor names for this show | "BetterHelp", "Athletic Greens" |
| 2 | Current sponsor lexicon for this podcast | From PodcastProfile.sponsorLexicon |
| 3 | Domain stems and spoken URL templates | "betterhelp", "betterhelp dot com slash podcast" |
| 4 | Show title + host names | From PodcastProfile |
| 5 | Top historical sponsors only | From fingerprint store, capped |
| 6 (lowest) | Promo-code templates | Only when podcast-specific |

Priority ordering ensures the vocabulary budget stays small and high-value if Apple caps the number of contextual strings.

**Weak-anchor recovery:**

LexicalScanner gains a `rescanAlternatives(chunks:nearTime:radius:)` method. SpanHypothesisEngine calls it **only inside**:
- Open confirmed hypotheses (looking for a closing anchor)
- Sponsor/disclosure neighborhoods (looking for missed intro)
- Low-confidence likely-ad regions

**Never rescan alternatives globally** — that would be expensive and noisy.

**TranscriptChunk new field:** `alternatives: [(text: String, confidence: Double)]?` from `SFSpeechRecognitionResult`.

---

## A5. Multi-Cue Boundary Snapping — `TimeBoundaryResolver`

**New file:** `Playhead/Services/AdDetection/TimeBoundaryResolver.swift`

TimeBoundaryResolver is the **only component allowed to snap or relocate boundaries**. All math in seconds.

**Boundary-type-aware scoring:**

The resolver uses separate cue weights for `.start` and `.end` boundaries, and incorporates distance penalty and continuity:

```
score = cueBlend(boundaryType) - λ * normalizedDistance + continuityBonus - contradictionPenalty
```

**Cue weights for start boundaries:**

| Cue | Weight | Signal |
|-----|--------|--------|
| Pause / VAD transition | 0.25 | `pauseProbability` from FeatureWindow |
| Speaker change proxy | 0.20 | `speakerChangeProxyScore` (acoustic fallback in Phase A) |
| Music bed change | 0.15 | `musicBedChangeScore` — onset/offset |
| Spectral change | 0.20 | `spectralFlux` exceeding local baseline |
| Lexical density delta | 0.20 | Sharp change in ad-like lexical density |

**Cue weights for end boundaries:**

| Cue | Weight | Signal |
|-----|--------|--------|
| Pause / VAD transition | 0.25 | `pauseProbability` from FeatureWindow |
| Speaker change proxy | 0.20 | `speakerChangeProxyScore` |
| Music bed change | 0.15 | `musicBedChangeScore` |
| Spectral change | 0.15 | `spectralFlux` exceeding local baseline |
| Explicit return marker | 0.25 | "back to the show" etc. at this time |

All weights in a `BoundarySnappingConfig` struct for tuning.

**Algorithm:**

1. Given a candidate boundary time, boundary type (`.start` or `.end`), and search radius, scan FeatureWindows within the radius.
2. Score each window with the boundary-type-aware objective including distance penalty (`λ * normalizedDistance`).
3. Return the **nearest local maximum** that exceeds both:
   - `minBoundaryScore` (config, default 0.3), AND
   - `minImprovementOverOriginal` (config, default 0.1)
4. If no window meets both thresholds, return the original time unsnapped.

**Distance penalty** ensures the resolver prefers the nearest plausible boundary, not the absolute best cue anywhere in the radius. This prevents snapping to unrelated speaker changes or pauses.

**Asymmetric snap distance by anchor type:**

| Anchor Type | Max Start Snap | Max End Snap |
|-------------|----------------|--------------|
| disclosure (startAnchored) | 5s | 15s |
| CTA/URL (endAnchored) | 15s | 5s |
| fmPositive (neutral) | 10s | 10s |

Configurable via `maxSnapDistanceByAnchorType` in `BoundarySnappingConfig`.

**What it replaces:**

| Current | New |
|---------|-----|
| `MinimalContiguousSpanDecoder.mergeGapAtoms = 3` | `TimeBoundaryResolver.mergeGapSeconds` (config, default ~3s) |
| `MinimalContiguousSpanDecoder.boundarySnapRadiusAtoms = 15` | `TimeBoundaryResolver.snapRadiusSeconds` (config, default ~8s) |
| `SkipOrchestrator.snapBoundary()` silence-only | **Removed** — SkipOrchestrator consumes finalized spans only |
| `BoundaryExpander` silence scoring | Delegates to `TimeBoundaryResolver.snap()` with boundary type and distance penalty |

**MinimalContiguousSpanDecoder** becomes a fallback reducer for legacy / non-hypothesis evidence only. It converts atom-based constants to seconds but performs no independent boundary snapping or overlap merging for hypothesis-owned spans.

---

## A6. Evidence Catalog — Preserve Repetition Density

**Change to EvidenceCatalogBuilder:**

Stop discarding duplicates entirely. Dedup by `(normalizedText, category)` but preserve:

| New Field | Type | Purpose |
|-----------|------|---------|
| `count` | `Int` | How many times this evidence appeared |
| `firstTime` | `Double` | Earliest atom timestamp |
| `lastTime` | `Double` | Latest atom timestamp |

FM prompt can optionally include repetition info (e.g., `[E3] "BetterHelp" (×4, 12s–67s)`), giving the FM repetition density as an ad signal. Repetition density is strongly ad-like and currently thrown away by dedup.

---

## A7. Metrics Split + Live Metrics

**Offline span quality metrics:**

| Metric | Definition |
|--------|------------|
| **Seed recall** | % of ground-truth ads where at least one anchor fired |
| **Span IoU** | Intersection-over-union between detected span and ground-truth span |
| **Median start error** | Seconds between detected start and ground-truth start |
| **Median end error** | Seconds between detected end and ground-truth end |
| **Signed start bias** | median(detectedStart - gtStart); positive = systematically late starts |
| **Signed end bias** | median(detectedEnd - gtEnd); negative = systematically early exits |
| **Coverage recall** | % of ground-truth ad seconds covered by detected span |
| **Coverage precision** | % of detected ad seconds that lie inside ground-truth |

**Live skip usefulness metrics:**

| Metric | Definition |
|--------|------------|
| **Lead time at first confirmation** | Seconds between first skip-eligible confirmation and GT ad start |

**Slice all metrics by:**
- Ad format (host-read, produced / pre-roll, dynamic if applicable)
- Podcast
- Live path vs backfill path

These supplement existing binary hit/miss tracking. The key diagnostic values: distinguishing "we found the ad but only covered the last 15%" from "we missed it entirely," and measuring whether the system knows early enough to actually skip.

---

# Phase B Design

## B7. Optional Speaker-Change Proxy / Diarization

Deferred from Phase A to reduce implementation risk. Once `SFSpeechRecognitionResult.speechRecognitionMetadata.speakerLabels` is verified on target iOS 26 builds:

- Populate `speakerId` on TranscriptChunk / TranscriptAtom
- Replace the acoustic turn-change proxy in `speakerChangeProxyScore` with validated speaker labels
- `speakerChangeProxyScore` becomes 1.0 at actual speaker turn boundaries (smoothed ±1 window)

If speaker labels are not available, evaluate offline diarization / speaker embedding change detectors as alternatives. The Phase A acoustic proxy (pause + spectral shift + timbre change) continues as fallback.

---

## B8. Music-Bed Detection Improvements

Enhance the SoundAnalysis-based music detection from Phase A:
- Tune `SNClassifySoundRequest` window duration for podcast-specific audio profiles
- Improve `musicBedChangeScore` derivative to distinguish background music beds from foreground music segments
- Evaluate whether music onset/offset reliably correlates with ad boundaries across podcast genres

---

## B9. Retask FM into Structured Boundary Extraction

**Current FM usage:** ~3,700 token overhead per call, 300s scan budget. Two generic passes (coarse yes/no, then refinement) over broad windows. Most of that budget confirms things lexical + acoustic already found.

**New FM strategy:**

### Stage 1 — Coarse detection stays non-FM

Lexical + acoustic + fingerprint + SpanHypothesisEngine (Phase A) produce high-recall suspicious regions. No FM budget spent here.

### Stage 2 — FM boundary extraction on suspicious regions only

For each suspicious region, first **pre-segment transcript into sentence-like / discourse units** (roughly 2–8 seconds each, pause- and punctuation-aware). This avoids token-heavy atom-level labeling — atoms vary 1–5 per second, so a 120s region could be hundreds of atom labels.

Ask the FM to return 1..N contiguous labeled spans over those units.

**Structured output schema:**

```swift
struct FMBoundarySchema {
    let spans: [FMSpanLabel]
    let abstain: Bool              // FM can explicitly decline
}

struct FMSpanLabel {
    let firstSegmentRef: String    // "S3" (sentence/discourse unit)
    let lastSegmentRef: String     // "S6"
    let role: SpanRole
    let commercialIntent: CertaintyBand
    let ownership: OwnershipType
    let evidenceRefs: [String]     // back-references to evidence catalog
}

enum SpanRole: String {
    case show
    case adIntro
    case adBody
    case adCTA
    case returnToShow
}
```

### Prompt design

Include:
- 1 positive host-read ad example
- 1 negative lookalike example (show recommendation / self-promo / content CTA)
- Optionally 1 produced-ad example if budget allows

Exclude schema framing when examples are present (saves ~3,500 tokens). Prewarm the session before boundary extraction per Apple's recommendation.

### Budget reallocation

With coarse scanning removed, the full 300s budget goes toward fewer, more targeted calls. Each boundary extraction call covers one suspicious region (~30–120s of transcript, pre-segmented into discourse units) instead of scanning the whole episode.

### Guardrail

If the FM returns a span that crosses a strong contradiction from the hypothesis engine (e.g., high-confidence non-ad evidence), treat it as uncertain instead of forcing a boundary.

**Changes to FoundationModelClassifier:**

| Current | New |
|---------|-----|
| `coarsePassA()` — generic window classification | Removed — suspicious regions come from SpanHypothesisEngine |
| `refinePassB()` — span-level yes/no | Replaced with `extractBoundaries()` — per-segment role labels; must support clean abstention |
| `planPassA()` — broad window planning | Replaced with `planBoundaryExtractionWindows()` — one window per suspicious region |
| CoveragePlanner fullCoverage vs targetedWithAudit | Simplified — processes what hypothesis engine flags |

---

## B10. Fingerprint Full-Span Recovery via Anchor-Aware Local Alignment

**Current limitation:** `AdCopyFingerprintStore` stores only the MinHash signature + normalized text. No span boundaries. When a fingerprint matches, it can only identify the ~30-atom sliding window that matched — typically the CTA fragment, not the full ad.

### Store span boundaries and anchor landmarks with fingerprints

**New fields on FingerprintEntry:**

| Field | Type | Purpose |
|-------|------|---------|
| `spanStartOffset` | `Double` | Seconds from fingerprint match start to full ad start |
| `spanEndOffset` | `Double` | Seconds from fingerprint match end to full ad end |
| `spanDurationSeconds` | `Double` | Total ad duration when fingerprint was created |
| `canonicalSponsorEntity` | `NormalizedSponsor?` | Sponsor identity for entity-aware matching |
| `anchorLandmarks` | `[AnchorLandmark]` | Typed anchor positions within the ad |

```swift
struct AnchorLandmark {
    let type: AnchorType          // disclosure, url, promoCode, etc.
    let offsetSeconds: Double     // offset from fingerprint match start
    let normalizedText: String?   // e.g., "betterhelp dot com"
}
```

These are relative offsets, not absolute times — so when the CTA fragment matches at a different timestamp in a new episode, the full boundaries transfer: `adStart = matchStart - spanStartOffset`, `adEnd = matchEnd + spanEndOffset`.

Anchor landmarks make transfer robust to host ad-lib variation — the system can verify that expected landmarks (disclosure near start, CTA near end) align at roughly the expected offsets.

### Anchor-aware local alignment

| Match Strength | Jaccard | Behavior |
|----------------|---------|----------|
| Strong match | ≥ 0.8 | Transfer full boundaries only after anchor-landmark alignment and `TimeBoundaryResolver` validation |
| Normal match | 0.6–0.8 | Seed a hypothesis using matched fragment + landmark priors for SpanHypothesisEngine to verify |

### User-marked ad fingerprint seeding

Currently only FM-attested evidence can create fingerprints. Add a second path: when a user marks an ad and BoundaryExpander produces a span with confidence ≥ 0.7, allow that span to seed a fingerprint at `candidate` state. It still goes through the normal trust lifecycle (candidate → quarantined → active), so a single bad user tap doesn't pollute the store.

---

## B11. Selective Typed Redaction Fallback

**Design philosophy change:** The FM is on-device. The hard legal requirement is "no audio or transcript ever leaves the phone," not "the local model may never see sponsor strings." Redacting everything by default removes useful sponsor identity from a task whose main job is boundary finding.

**Two-tier redaction policy:**

| Tier | When | What |
|------|------|------|
| Default path | Ordinary sponsor names in on-device boundary extraction | Minimal / no redaction — FM sees full sponsor names, URLs, promo codes |
| Fallback path | Categories that trigger FM refusals, or when invoking the permissive fallback path | Typed placeholder redaction with stable per-entity IDs |

**Typed placeholder scheme (fallback tier only):**

| Current | New |
|---------|-----|
| `[DRUG]` | `[DRUG_A]`, `[DRUG_B]` |
| `[SERVICE]` | `[SERVICE_A]`, `[SERVICE_B]` |
| `[PRODUCT]` | `[PRODUCT_A]`, `[PRODUCT_B]` |
| `[CONDITION]` | `[CONDITION_A]`, `[CONDITION_B]` |
| `[TEST]` | `[TEST_A]`, `[TEST_B]` |

**Assignment rule:** First occurrence of a unique original string within a category gets `_A`, second unique string gets `_B`, etc. Same original string always maps to the same ID within a single redaction pass — so "Ozempic" appearing 4 times all become `[DRUG_A]`. Assignment is deterministic (alphabetical by first-occurrence order).

**Changes to PromptRedactor:**
- Mapping is ephemeral — not persisted, rebuilt per redaction pass
- Preserve an internal per-pass `sponsorEntityHandle` outside the redacted text so the rest of the pipeline can reason about identity even when redaction is active
- No change to the trigger/cooccurrent gating logic

If there is a non-negotiable internal policy that all FM input must be redacted, the typed placeholders + `sponsorEntityHandle` preserve enough structure for the pipeline to work.

---

# Boundary Ownership Model

A key design principle: **reduce boundary ownership to one primary component per concern.**

| Component | Role | Boundary Authority |
|-----------|------|--------------------|
| **SpanHypothesisEngine** | Owns provisional start/end boundary lifecycle | Discovers and proposes boundaries |
| **TimeBoundaryResolver** | Only component allowed to snap / relocate boundaries | Finalizes boundaries |
| **MinimalContiguousSpanDecoder** | Fallback reducer for legacy / non-hypothesis evidence only | No independent boundary snapping; no overlap merging for hypothesis-owned spans |
| **SkipOrchestrator** | Consumes finalized spans only | Does not discover or move boundaries |
| **BoundaryExpander** | Called by SpanHypothesisEngine with anchor-type config | Proposes expansion; delegates snapping to TimeBoundaryResolver |

This eliminates the current situation where multiple components independently help with span formation, making boundary decisions hard to debug.

---

# Full Component Dependency Graph

```
TranscriptEngine
  ├── ASRVocabularyProvider (Phase A — contextualStrings for both ASR paths)
  ├── speakerLabels (Phase B — optional, nil until verified)
  └── SoundAnalysis pass (Phase A — auxiliary music likelihood)
        │
        ▼
FeatureWindow (+ speakerChangeProxyScore, musicProbability, musicBedChangeScore)
        │
        ▼
LexicalScanner (stateless — emits LexicalHits)
  └── rescanAlternatives() (Phase A — weak-anchor recovery, scoped only)
        │
        ▼
SpanHypothesisEngine (Phase A, NEW — single owner of span lifecycle)
  ├── consumes: LexicalHits, FM results, fingerprint matches
  ├── manages: AdHypothesis lifecycle (seed/accumulate/confirm/close)
  ├── tracks: sponsorEntity, polarity, idle-gap, evidence decay
  ├── calls: BoundaryExpander.expand() with anchor-type-aware config
  ├── calls: TimeBoundaryResolver.snap() for final edges
  └── emits: CandidateAdSpan
        │
        ▼
TimeBoundaryResolver (Phase A, NEW — only snapper)
  ├── boundary-type-aware scoring (separate start/end weights)
  ├── distance penalty (prefer nearest plausible boundary)
  ├── asymmetric snap distance by anchor type
  └── replaces all legacy snap/silence logic
        │
        ▼
MinimalContiguousSpanDecoder (fallback — seconds not atoms, no independent snapping)
        │
        ▼
FoundationModelClassifier (Phase B — retasked)
  ├── extractBoundaries() on pre-segmented discourse units
  ├── per-segment role labels: show / adIntro / adBody / adCTA / returnToShow
  ├── explicit abstain path
  └── consumes suspicious regions from SpanHypothesisEngine
        │
        ▼
BackfillEvidenceFusion → DecisionMapper → SkipOrchestrator (consumes finalized spans only)
        │
        ▼
AdCopyFingerprintStore (Phase B — + offsets, landmarks, canonicalSponsorEntity)
AdCopyFingerprintMatcher (Phase B — anchor-aware transfer + hypothesis seeding)
        │
        ▼
EvidenceCatalogBuilder (Phase A — preserves count, firstTime, lastTime)
        │
        ▼
PromptRedactor (Phase B — selective two-tier redaction)
        │
        ▼
Evaluation Harness (Phase A — seed recall, span IoU, boundary error, lead time, coverage)
```

---

# Files Changed — Full Inventory

## Phase A: New Files

| File | Purpose |
|------|---------|
| `SpanHypothesisEngine.swift` | State machine: seed → accumulate → confirm → close; single span lifecycle owner |
| `SpanHypothesisConfig.swift` | Anchor-type-aware window durations, search radii, polarity, idle-gap, decay |
| `TimeBoundaryResolver.swift` | Multi-cue boundary snapping in seconds; boundary-type-aware; distance penalty |
| `BoundarySnappingConfig.swift` | Cue weights for start/end, distance penalty λ, snap distance by anchor type |
| `ASRVocabularyProvider.swift` | Compiles sponsor/domain/host vocabulary for contextualStrings (both ASR paths) |

## Phase A: Modified Files

| File | Change |
|------|--------|
| `TranscriptEngine.swift` | Populate contextualStrings from ASRVocabularyProvider; expose alternative transcriptions; add SoundAnalysis pass |
| `TranscriptChunk.swift` | Add `speakerId: Int?`, `alternatives: [(text: String, confidence: Double)]?` |
| `TranscriptAtom.swift` | Add `speakerId: Int?` |
| `FeatureWindow.swift` (or containing model) | Add speakerChangeProxyScore, musicProbability, musicBedChangeScore |
| `BoundaryExpander.swift` | Accept anchor-type config overrides; delegate all snapping to TimeBoundaryResolver |
| `LexicalScanner.swift` | Add `rescanAlternatives()` method; 30s merge stays as fallback |
| `MinimalContiguousSpanDecoder.swift` | Convert atom constants to seconds; no independent snapping for hypothesis-owned spans |
| `SkipOrchestrator.swift` | Remove `snapBoundary()`; consume finalized spans only |
| `EvidenceCatalogBuilder.swift` | Preserve count/firstTime/lastTime in dedup |
| `AdDetectionService.swift` | Wire SpanHypothesisEngine into backfill pipeline |
| Evaluation harness files | Add all Phase A metrics + slicing |

## Phase B: Modified Files

| File | Change |
|------|--------|
| `TranscriptEngine.swift` | Wire validated speaker labels when available |
| `TranscriptChunk.swift` / `TranscriptAtom.swift` | Populate `speakerId` from validated source |
| `FoundationModelClassifier.swift` | Replace coarse/refine with `extractBoundaries()` on discourse units; abstain path; one-shot examples |
| `CoveragePlanner.swift` | Simplify — process suspicious regions only |
| `AdCopyFingerprintStore.swift` | Add spanStartOffset, spanEndOffset, spanDurationSeconds, canonicalSponsorEntity, anchorLandmarks |
| `AdCopyFingerprintMatcher.swift` | Anchor-aware transfer; strong-match validation; normal-match hypothesis seeding |
| `PromptRedactor.swift` | Two-tier policy; typed per-entity IDs; sponsorEntityHandle preservation |

---

# Open Questions

1. **Speaker label availability (Phase B gate):** Verify `SFSpeechRecognitionResult.speechRecognitionMetadata.speakerLabels` on iOS 26 beta. Phase A uses acoustic proxy; Phase B upgrades if available.

2. **SoundAnalysis latency:** Music detection pass runs in parallel — confirm it doesn't add meaningful latency to the hot path. If it does, run async and backfill FeatureWindows.

3. **contextualStrings limit:** Apple may cap the number of contextual strings per recognition request. Priority ordering in ASRVocabularyProvider handles this, but need to verify the cap.

4. **Ground-truth corpus for metrics:** The new metrics need labeled ad spans with start/end times, not just binary "this episode has ads." Do we have this, or do we need to build it?

5. **Hypothesis timeout tuning:** The 60–90s window durations are starting points. May need adjustment after measuring on real episodes.

6. **FM one-shot example selection:** Which ad examples to include in boundary extraction prompts? Ideally one host-read and one negative lookalike, curated from real episodes and stored as prompt assets.

7. **Fingerprint span offset stability:** Recurring ads can vary slightly in duration across episodes (host ad-lib). Anchor-landmark alignment + TimeBoundaryResolver snapping should handle this, but worth validating.

8. **Redaction policy decision:** Is there a non-negotiable internal policy requiring all FM input to be redacted? If so, typed placeholders + sponsorEntityHandle are the fallback. If not, the two-tier approach (minimal default, typed fallback) is preferred.

---

# Recommended First Implementation Slice

Per expert recommendation, the highest-value path before CoreML:

1. BoundaryExpander auto-run on machine seeds
2. Ad hypothesis windows (SpanHypothesisEngine)
3. ASR bias (ASRVocabularyProvider + contextualStrings)
4. Conservative seconds-based snapping (TimeBoundaryResolver)
5. Live lead-time metrics

This gets the core span expansion working with better transcript quality, measurable with live-relevant metrics, before tackling FM retasking or fingerprint recovery.
