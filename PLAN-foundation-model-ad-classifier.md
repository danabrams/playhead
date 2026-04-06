# Plan: Foundation Model Ad Classifier (Backfill Rescorer)

## Problem

Ads are getting through detection. Scores land around 0.42 for segments that *are* ads, which is above the 0.40 candidate threshold but below the 0.65 skip-orchestrator enter threshold — so they get flagged as candidates but never skipped.

The root cause: **the entire pipeline is gated on the LexicalScanner finding regex hits.** If a podcast host does a conversational ad read ("I've been using BetterHelp for six months and it's really helped me..."), there are no "brought to you by" phrases, no promo codes, no "dot com slash" patterns. The scanner produces zero hits, zero candidates, and the classifier never even runs. For ads that *do* match a pattern or two, the lexical confidence is low (~0.3-0.5), which propagates through the classifier as a weak signal since lexical weight is 0.40 of the total score.

The backfill pass has the same blind spot — it re-runs the identical LexicalScanner on the final transcript. If regex missed it on the hot path, it'll miss it again on backfill.

## Goal

Add a Foundation Models-based semantic scanner to the **backfill pipeline** that can identify ad segments the lexical scanner misses entirely, and rescore borderline candidates with semantic understanding. Feed transcript-grounded, deterministically extracted sponsor entities into a show-scoped SponsorKnowledgeStore that compiles into a low-latency hot-path artifact for future episodes.

**Non-goals:**
- Replacing the hot-path lexical scanner (Foundation Models is too slow for <100ms)
- Removing the regex scanner (it's still the fastest first pass)
- Changing the SkipOrchestrator thresholds or policy logic
- Running FM on the hot path (but the hot path *may* consult a compiled sponsor lexicon built from FM-grounded results, since that's just string matching)

## Why Foundation Models

1. Already imported and working in `FoundationModelExtractor` for metadata extraction
2. On-device, zero cost, no cloud dependency (matches the on-device mandate)
3. Schema-bound guided generation (`@Generable`) constrains output reliably — supports enums, nested structs, constrained decoding
4. iOS 26+ only — which is our deployment target
5. Can understand conversational ad reads, host-read ads, baked-in ads that have zero lexical signals
6. Backfill has no latency budget — 1-5s per segment is fine
7. Apple exposes `contextSize`, `tokenCount(for:)`, `supportsLocale(_:)`, and `prewarm(promptPrefix:)` as real operational levers

## Architecture

### Current backfill flow
```
Final transcript chunks
  -> LexicalScanner (same regex scan)
  -> ClassifierService (re-score with acoustics)
  -> Promote/suppress existing candidates
  -> MetadataExtractor (enrich confirmed windows)
  -> Update priors
```

### New backfill flow
```
Final transcript
  -> TranscriptAtomizer + TranscriptVersion
  -> TranscriptSegmenter + TranscriptQualityEstimator
  -> CueHarvesters + EvidenceCatalogBuilder
       -> LexicalScanner
       -> AcousticBreakDetector
       -> SponsorKnowledgeMatcher
       -> AdCopyFingerprintMatcher
       -> EvidenceCatalogBuilder (URLs, promo codes, CTA/disclosure spans, brand-like spans)
  -> CoveragePlanner (uncertainty + slot priors + device budget)
  -> FoundationModelSemanticScanner (coarse / zoom / refine, certainty bands)
  -> CommercialEvidenceResolver (resolve FM evidence refs to deterministic catalog entries)
  -> RegionProposalBuilder (source-agnostic canonical regions)
  -> AtomEvidenceProjector (project all evidence onto atom timeline)
  -> MinimalContiguousSpanDecoder (duration-constrained smoothing + correction masks)
  -> BackfillEvidenceFusion (decision mapping + eligibility gates)
  -> BoundaryRefiner
  -> Promote/suppress ONCE
  -> Append Evidence/Decision/Correction events to ledger
  -> Materializers
       -> Current-episode skip cues
       -> SponsorKnowledgeStore
       -> AdCopyFingerprintStore
       -> TrainingExamples
       -> CompiledSponsorLexicon
```

### Key design decisions

0. **Stable transcript identity from day one.** All FM outputs, user corrections, sponsor memory writes, and training examples are keyed by versioned `TranscriptAtom` ordinals — `(analysisAssetId, transcriptVersion, atomOrdinal)`. FM prompts expose compact `lineRef` integers that resolve back to atom keys after generation. Chunk indices remain diagnostic only. When transcripts are reprocessed, a `TranscriptAlignmentMap` preserves correction and training lineage across versions.

1. **The LLM runs on the *full transcript*, not just lexical candidates.** The lexical scanner is a filter — it only produces candidates where regex matches. The FM scanner covers the entire transcript, catching conversational ads with no lexical signals at all.

2. **FM produces grounded evidence, not sponsor facts.** A deterministic `EvidenceCatalogBuilder` runs *before* FM, cataloging URLs, promo codes, CTA phrases, disclosure phrases, and brand-like spans from the transcript. FM refinement points to evidence catalog refs + line refs, not free-text sponsor names. A `CommercialEvidenceResolver` maps FM output back to deterministic catalog entries. Only resolved deterministic spans are eligible for SponsorKnowledgeStore writes. This prevents FM hallucinations from poisoning the knowledge store.

3. **Source-agnostic regions.** After `RegionProposalBuilder`, all proposed regions go through the same feature extraction and fusion pipeline regardless of origin.

4. **FM-only regions require an evidence quorum, not necessarily non-FM corroboration.** For current-episode backfill cues, eligibility can be satisfied by *either*:
   - (a) **Intrinsic semantic corroboration**: multi-window consensus + >= 2 distinct evidence kinds (among sponsor/CTA/URL/promo/disclosure) + good transcript quality + usable/precise boundary precision
   - (b) **External corroboration**: lexical / acoustic / sponsor knowledge / fingerprint / slot prior / repeated cross-episode confirmation
   Knowledge-store promotion remains stricter and still requires external or repeated confirmation.

5. **FM contributes positive evidence only in v1.** We are fixing a recall hole. FM `noAds`, low-confidence negatives, and `abstain` mean "no positive semantic evidence," not "negative evidence." They affect scan scheduling, audit selection, and diagnostics — never suppress existing candidates.

6. **Temporal consensus from overlapping windows.** Overlapping windows are correlated, so raw overlap count is not independent evidence. Consensus requires anchor-consistent spans with sufficient center separation and IoU agreement.

7. **Never override user corrections, and store them with scope.** Correction scopes: `exactSpan`, `sponsorOnShow`, `phraseOnShow`, `campaignOnShow`. FM cannot re-promote any active correction scope without materially new non-FM corroboration.

8. **Anti-churn stability for materialized cues.** Removing a previously shipped skip cue requires stronger counterevidence than adding a new cue, unless the transcript changed materially or the user explicitly corrected it.

9. **Failures are not evidence.** A refusal, decoding failure, guardrail violation, or context overflow is not "not an ad." Each failure mode has explicit handling.

10. **Scan-time evidence and decision-time policy are separate artifacts.** `ScanCohort` captures prompt/schema/windowing provenance. `DecisionCohort` captures feature extraction, fusion, and policy provenance. Policy tweaks recompute decisions from cached scan results without triggering FM rescans.

11. **Certainty bands, not raw doubles.** FM self-reported confidence numbers are unstable across model cohorts. Schemas use `CertaintyBand` (weak/moderate/strong) and `BoundaryPrecision` (rough/usable/precise) enums, calibrated externally per cohort by the `DecisionMapper`. Free-form reasoning is debug/shadow only; production decisions use structured `ReasonTag` enums.

12. **Append-only event ledger, derived stores.** Evidence, decisions, corrections, and knowledge candidates are appended as immutable events. Mutable stores (SponsorKnowledgeStore, AdCopyFingerprintStore, TrainingExamples, CompiledSponsorLexicon) are materialized from the ledger. This enables reproducible replay, postmortems, and counterfactual analysis.

13. **Unknown model cohorts default to shadow.** New `(osBuild, ScanCohort)` combinations are untrusted until the replay harness approves them. Known-bad cohorts can be forced to `.off`.

---

## Schemas

### Transcript identity

```swift
struct TranscriptVersion: Sendable, Codable {
    let transcriptVersion: String    // hash of atom sequence
    let normalizationHash: String    // transcript normalization pipeline
    let sourceHash: String           // ASR model / source identity
}

struct TranscriptAtomKey: Sendable, Codable, Hashable {
    let analysisAssetId: String
    let transcriptVersion: String
    let atomOrdinal: Int
}

struct TranscriptAtom: Sendable {
    let atomKey: TranscriptAtomKey
    let contentHash: String          // for matching/debugging, not primary identity
    let startTime: Double
    let endTime: Double
    let text: String
    let chunkIndex: Int              // diagnostic convenience
}

/// When transcripts are reprocessed, preserves correction and training lineage.
struct TranscriptAlignmentMap: Sendable, Codable {
    let fromTranscriptVersion: String
    let toTranscriptVersion: String
    let mappings: [Int: Int]         // old atomOrdinal -> new atomOrdinal
}

/// Stable anchor for materialized cues that survives transcript version changes.
struct CueAnchor: Sendable, Codable {
    let analysisAssetId: String
    let transcriptVersion: String
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let approxStartTime: Double
    let approxEndTime: Double
    let boundaryFingerprint: String
}
```

### Provenance cohorts

```swift
/// Captures what produced the FM scan results. Changes trigger FM rescan.
struct ScanCohort: Sendable, Codable {
    let promptLabel: String          // human-readable: "classify-v1"
    let promptHash: String           // hash of rendered prompt template
    let schemaHash: String           // hash of @Generable schema definition
    let scanPlanHash: String         // windowing / zoom / clustering logic
    let normalizationHash: String    // transcript normalization pipeline
    let osBuild: String              // model cohort proxy
    let locale: String
    let appBuild: String
}

/// Captures what produced the decisions from scan results. Changes recompute
/// decisions from cached scans without triggering FM rescan.
struct DecisionCohort: Sendable, Codable {
    let featurePipelineHash: String  // feature extraction / mapping code
    let fusionHash: String           // evidence ledger / quorum logic
    let policyHash: String           // policy matrix / gating logic
    let stabilityHash: String        // anti-churn / correction policy
    let appBuild: String
}
```

### FM classification schemas

**Coarse screening** (Pass A — every window, minimal tokens):

```swift
@Generable
enum TranscriptQuality: String, Sendable {
    case good, degraded, unusable
}

@Generable
enum CoarseDisposition: String, Sendable {
    case noAds, containsAd, uncertain, abstain
}

@Generable
enum CertaintyBand: String, Sendable {
    case weak, moderate, strong
}

@Generable
struct CoarseSupportSchema: Sendable {
    @Guide(description: "Prompt-local transcript line references most relevant to the ad judgment. Maximum 2.")
    var supportLineRefs: [Int]

    @Guide(description: "How certain the ad judgment is.")
    var certainty: CertaintyBand
}

@Generable
struct CoarseScreeningSchema: Sendable {
    @Guide(description: "Quality of the transcript in this window.")
    var transcriptQuality: TranscriptQuality

    @Guide(description: "Overall disposition of this window.")
    var disposition: CoarseDisposition

    @Guide(description: "Minimal support for containsAd/uncertain. Omit for noAds or abstain.")
    var support: CoarseSupportSchema?
}
```

**Refinement** (Pass B — only zoomed positive/uncertain regions):

```swift
@Generable
enum CommercialIntent: String, Sendable {
    case paid, owned, affiliate, organic, unknown
}

@Generable
enum Ownership: String, Sendable {
    case thirdParty, show, network, guest, unknown
}

@Generable
enum AlternativeExplanation: String, Sendable {
    case editorialDiscussion, guestSelfPromo, organicRecommendation,
         newsReport, audienceFeedback, none
}

@Generable
enum EvidenceKind: String, Sendable {
    case sponsorName, cta, url, promoCode, disclosure
}

@Generable
enum BoundaryPrecision: String, Sendable {
    case rough, usable, precise
}

@Generable
enum ReasonTag: String, Sendable {
    case sponsorMention, cta, url, promoCode, disclosure
    case editorialMention, guestSelfPromo, housePromo, organicRecommendation
}

@Generable
struct EvidenceAnchorSchema: Sendable {
    @Guide(description: "Prompt-local deterministic evidence catalog reference, when available. -1 if none.")
    var evidenceRef: Int

    @Guide(description: "Prompt-local transcript line reference containing this evidence.")
    var lineRef: Int

    @Guide(description: "Type of evidence present on this line.")
    var kind: EvidenceKind

    @Guide(description: "How certain this evidence is present.")
    var certainty: CertaintyBand
}

@Generable
struct SpanRefinementSchema: Sendable {
    @Guide(description: "Commercial intent of this span.")
    var commercialIntent: CommercialIntent

    @Guide(description: "Who owns/benefits from this promotion.")
    var ownership: Ownership

    @Guide(description: "First line reference in the ad span.")
    var firstLineRef: Int

    @Guide(description: "Last line reference in the ad span.")
    var lastLineRef: Int

    @Guide(description: "How certain this is a genuine paid third-party ad.")
    var certainty: CertaintyBand

    @Guide(description: "How precise the boundaries are.")
    var boundaryPrecision: BoundaryPrecision

    @Guide(description: "Transcript-grounded evidence anchors.")
    var evidenceAnchors: [EvidenceAnchorSchema]

    @Guide(description: "Strongest alternative explanation for why this might not be a paid ad.")
    var alternativeExplanation: AlternativeExplanation

    @Guide(description: "Structured reason tags for this verdict.")
    var reasonTags: [ReasonTag]
}

@Generable
struct RefinementWindowSchema: Sendable {
    @Guide(description: "Quality of the transcript in this window.")
    var transcriptQuality: TranscriptQuality

    @Guide(description: "Refined ad spans. Maximum 2 per window.")
    var spans: [SpanRefinementSchema]
}
```

### Eligibility and policy

```swift
enum SkipEligibilityGate: String, Sendable {
    case eligible
    case blockedByEvidenceQuorum    // insufficient evidence diversity/consensus
    case blockedByPolicy            // housePromo, editorialMention, etc.
    case blockedByUserCorrection    // overlaps active correction scope
    case blockedByConsensus         // insufficient FM consensus
}

enum SkipPolicyAction: String, Sendable {
    case autoSkipEligible
    case detectOnly
    case logOnly
    case suppress
}

/// Maps (commercialIntent, ownership) -> action.
/// v1: only (.paid, .thirdParty) is autoSkipEligible.
struct SkipPolicyMatrix { ... }
```

### Failure handling

```swift
enum SemanticScanStatus: String, Sendable {
    case queued, running, success
    case unavailable
    case unsupportedLocale
    case exceededContextWindow
    case decodingFailure
    case refusal
    case guardrailViolation
    case assetsUnavailable
    case rateLimited
    case thermalDeferred
    case cancelled
    case failedTransient
}
```

**Retry policy:**
- `exceededContextWindow` → shrink window (strip annotations, reduce atom count), retry once
- `decodingFailure` → simplify schema (fall back to coarse schema), retry once
- `refusal` / `guardrailViolation` → persist separately, never reinterpret as "not an ad"
- `assetsUnavailable` → defer and retry later
- `rateLimited` → backoff and retry
- `thermalDeferred` / `cancelled` → resume from checkpoint on next backfill

### Knowledge lifecycle

```swift
enum KnowledgeState: String, Sendable {
    case candidate      // newly discovered, never affects hot path
    case quarantined    // repeated but still under observation
    case active         // eligible for CompiledSponsorLexicon / fingerprint hot-path artifact
    case decayed        // confidence eroded; searchable for diagnostics
    case blocked        // explicitly suppressed by correction or rollback history
}
```

Transitions:
- `candidate` → `quarantined`: deterministic extraction + one high-quality confirmation
- `quarantined` → `active`: >= 2 episode confirmations, rollback below threshold, no active correction scope, stable over a cohort observation window
- `active` → `decayed`/`blocked`: rollback spike, correction conflict, or drift regression

Candidate and quarantined entries may influence backfill scheduling but never the hot path.

### User correction scopes

```swift
enum CorrectionScope: String, Sendable {
    case exactSpan        // this specific time range on this episode
    case sponsorOnShow    // this sponsor entity on this show
    case phraseOnShow     // this phrase on this show
    case campaignOnShow   // this ad campaign on this show
}
```

### Event ledger types

```swift
// Append-only. Stores are materialized from these.
struct EvidenceEvent { ... }           // FM scan result, cue harvester hit, acoustic feature
struct DecisionEvent { ... }           // scored window + eligibility gate + policy action
struct CorrectionEvent { ... }         // user "Listen" tap with scope
struct KnowledgeCandidateEvent { ... } // deterministically extracted entity/fingerprint
```

---

## FM Scanner Design

### Pass A — Coarse screening (full coverage)

- **Token-budgeted windows**: Size each window to fit within context limits, measured via `tokenCount(for:)`. Windowing packs coherent transcript segments (from `TranscriptSegmenter`), not arbitrary time slices.
- **Minimal schema**: `CoarseScreeningSchema` — just quality, disposition, and up to 2 support line refs with certainty band. No reasoning, no evidence breakdown.
- **Full coverage**: Every transcript segment must appear in at least one coarse window.

### Pass A.5 — Adaptive zoom localization (positive/uncertain only)

- Recursively re-window only positive/uncertain windows around support line refs
- Progressively narrow context until target token budget, ambiguity budget, or minimum-span budget is reached
- Produce localized refinement windows for Pass B

### Pass B — Refinement (zoomed windows only)

- Rich schema: `RefinementWindowSchema` with commercial intent, ownership, evidence anchors (pointing to deterministic catalog entries + line refs), alternative explanations, reason tags
- Up to 2 refined spans per window
- Evidence anchors reference catalog entries when available; fall back to line-based resolution

### Prompt design

Terse and hardened. Transcript is quoted data, never instructions.

Coarse prompt:
```
Classify ad content in this podcast transcript window.

Transcript ({startTime}s - {endTime}s):
"""
[0][13.2s] I've been really stressed lately and
[1][18.7s] my therapist recommended I try BetterHelp
[2][24.4s] you can get started today at betterhelp.com slash
"""
```

Refinement prompt (evidence catalog refs injected when available):
```
Analyze these potential ad spans. Identify commercial intent, who benefits,
and point to specific transcript lines or evidence catalog entries containing
sponsor names, CTAs, URLs, promo codes, or disclosures.

Consider whether each span could be editorial discussion, guest self-promotion,
or organic recommendation.

Evidence catalog:
[E0] "betterhelp.com slash" (url, line 2)
[E1] "BetterHelp" (sponsorName, line 1)

Transcript ({startTime}s - {endTime}s):
"""
[0][13.2s] I've been really stressed lately and
[1][18.7s] my therapist recommended I try BetterHelp
[2][24.4s] you can get started today at betterhelp.com slash
"""
```

---

## SQLite Tables (new)

### SemanticScanResult (cached FM outputs)

| Column | Type | Purpose |
|--------|------|---------|
| `id` | TEXT PK | UUID |
| `analysisAssetId` | TEXT | Episode |
| `windowFirstAtomOrdinal` | INTEGER | First atom covered |
| `windowLastAtomOrdinal` | INTEGER | Last atom covered |
| `windowStartTime` | REAL | Scan window start |
| `windowEndTime` | REAL | Scan window end |
| `scanPass` | TEXT | coarse / zoom / refine |
| `transcriptQuality` | TEXT | good / degraded / unusable |
| `disposition` | TEXT | Coarse disposition |
| `spansJSON` | TEXT | JSON array of span results |
| `status` | TEXT | SemanticScanStatus value |
| `attemptCount` | INTEGER | Retry count |
| `errorContext` | TEXT | Structured failure context |
| `inputTokenCount` | INTEGER | Tokens in prompt |
| `outputTokenCount` | INTEGER | Tokens generated |
| `latencyMs` | INTEGER | End-to-end latency |
| `prewarmHit` | INTEGER | Boolean |
| `scanCohortJSON` | TEXT | Persisted ScanCohort |
| `transcriptVersion` | TEXT | Transcript version hash |

### AdDecisionResult (replayable decisions)

| Column | Type | Purpose |
|--------|------|---------|
| `id` | TEXT PK | UUID |
| `analysisAssetId` | TEXT | Episode |
| `decisionCohortJSON` | TEXT | Persisted DecisionCohort |
| `inputArtifactRefs` | TEXT | Scan/proposal artifact IDs |
| `decisionJSON` | TEXT | Final scored windows + gates |
| `createdAt` | REAL | Timestamp |

### BackfillJob

| Column | Type | Purpose |
|--------|------|---------|
| `jobId` | TEXT PK | UUID |
| `analysisAssetId` | TEXT | Episode |
| `podcastId` | TEXT | Show |
| `phase` | TEXT | Current phase name |
| `coveragePolicy` | TEXT | fullCoverage / targetedWithAudit / periodicFullRescan |
| `priority` | REAL | Expected listener value |
| `progressCursor` | TEXT | Resume token |
| `retryCount` | INTEGER | Attempts at current phase |
| `deferReason` | TEXT | thermal / lowBattery / cancelled / none |
| `status` | TEXT | queued / running / deferred / failed / complete |
| `scanCohortJSON` | TEXT | ScanCohort job was planned under |
| `decisionCohortJSON` | TEXT | DecisionCohort |
| `createdAt` | REAL | Timestamp |

### Event ledger tables

| Table | Purpose |
|-------|---------|
| `EvidenceEvent` | Append-only: FM scan results, cue harvester hits, acoustic features |
| `DecisionEvent` | Append-only: scored windows + eligibility gates + policy actions |
| `CorrectionEvent` | Append-only: user corrections with scope |
| `KnowledgeCandidateEvent` | Append-only: deterministically extracted entities/fingerprints |

### TrainingExample

| Column | Type | Purpose |
|--------|------|---------|
| `id` | TEXT PK | UUID |
| `analysisAssetId` | TEXT | Source episode |
| `startAtomOrdinal` | INTEGER | Stable span start |
| `endAtomOrdinal` | INTEGER | Stable span end |
| `transcriptVersion` | TEXT | Transcript version |
| `startTime` | REAL | Span start time |
| `endTime` | REAL | Span end time |
| `textSnapshotHash` | TEXT | Hash of normalized text used for labeling |
| `textSnapshot` | TEXT NULL | Optional snapshot for disagreement/export |
| `bucket` | TEXT | positive / negative / uncertain / disagreement |
| `commercialIntent` | TEXT | paid / owned / affiliate / organic |
| `ownership` | TEXT | thirdParty / show / network / guest |
| `evidenceSources` | TEXT | What contributed |
| `fmCertainty` | TEXT | weak / moderate / strong |
| `classifierConfidence` | REAL | Rule-based score |
| `userAction` | TEXT | skipped / reverted / none |
| `eligibilityGate` | TEXT | Gate at decision time |
| `scanCohortJSON` | TEXT | Full ScanCohort |
| `decisionCohortJSON` | TEXT | Full DecisionCohort |
| `transcriptQuality` | TEXT | good / degraded |

---

## Rollout

FM classification ships behind a rollout mode:

```swift
enum FMBackfillMode: String, Sendable {
    case shadow         // results logged only — no impact on decisions
    case rescoreOnly    // rescores borderline candidates, no new regions
    case proposalOnly   // proposes new regions, not auto-skip eligible
    case full           // rescores + proposes (with evidence quorum gates)
    case off            // FM disabled
}
```

Default: `.shadow`. Promotion requires replay-harness approval.

**Approved cohort gating:**
- Unknown `(osBuild, ScanCohort)` defaults to `.shadow`
- Known-bad cohorts can be forced to `.off`
- Promotion to `.rescoreOnly` / `.proposalOnly` / `.full` requires replay-harness approval
- New OS builds do not inherit production eligibility automatically

**Evaluation metrics (span-level):**
- Span precision / recall at IoU thresholds (0.5, 0.75)
- False-positive skipped seconds per hour
- False-negative ad seconds per hour
- Median boundary error (seconds)
- FM-only proposal precision
- User "Listen" rollback rate for FM-assisted decisions
- Random-audit estimated recall on unproposed regions

**Operational metrics:**
- Wall time per hour of audio
- Energy consumption per episode
- Cache reuse rate, resume success rate
- Per-cohort drift in FM-assisted decision distributions
- Thermal deferral rate

**Prompt regression harness** — required before any mode promotion:
- Benchmark cases: conversational host-read ads, editorial brand mentions, house promos, guest self-promo, noisy ASR, adversarial transcript text
- Block promotion unless benchmark gates pass and cohort-specific replay remains within drift tolerances

---

## Implementation Phases

Each phase is end-to-end and independently verifiable.

### Phase 1 — Transcript Identity + Segmentation

Establish the stable foundation all subsequent phases build on.

**Delivers:**
- `TranscriptAtom` with versioned ordinal identity `(analysisAssetId, transcriptVersion, atomOrdinal)`
- `TranscriptVersion` tracking (normalization hash, source hash)
- `TranscriptSegmenter` — segment episodes into coherent regions using pauses, speaker turns, punctuation, discourse markers
- `TranscriptQualityEstimator` — ASR confidence proxy, punctuation density, overlap/OOV rate, token density per segment
- `TranscriptChunk` gains `transcriptVersion` and `atomOrdinal` fields
- Schema migrations

**Verifiable by:** Every transcript chunk has a stable atom key. Segments align to natural boundaries. Quality scores correlate with known-bad transcript regions.

**Files:**
| File | Change |
|------|--------|
| `TranscriptAtom.swift` | **NEW** |
| `TranscriptSegmenter.swift` | **NEW** |
| `TranscriptQualityEstimator.swift` | **NEW** |
| `TranscriptChunk` | Add `transcriptVersion`, `atomOrdinal` |
| `AnalysisStore.swift` | Schema migrations |

### Phase 2 — Cue Harvesters + Evidence Catalog

Build the deterministic evidence layer that runs before FM.

**Delivers:**
- `AcousticBreakDetector` — energy drops, spectral transitions across full episode from existing FeatureWindows
- `SponsorKnowledgeMatcher` — fuzzy-match known entities from prior episodes (initially empty; populated by later phases)
- `AdCopyFingerprintMatcher` — near-duplicate match against confirmed ad scripts (initially empty)
- `EvidenceCatalogBuilder` — deterministic extraction of URLs, promo codes, CTA/disclosure phrases, brand-like spans from transcript atoms
- Evidence catalog entries are rendered as compact refs in FM prompts

**Verifiable by:** Harvesters produce candidate regions on test episodes. Evidence catalog contains extractable entities that match manual inspection. Coverage is broader than LexicalScanner alone.

**Files:**
| File | Change |
|------|--------|
| `AcousticBreakDetector.swift` | **NEW** |
| `SponsorKnowledgeMatcher.swift` | **NEW** (reads from SponsorKnowledgeStore, initially empty) |
| `AdCopyFingerprintMatcher.swift` | **NEW** (reads from fingerprint store, initially empty) |
| `EvidenceCatalogBuilder.swift` | **NEW** |

### Phase 3 — FM Semantic Scanner + Shadow Logging

The core FM scanning pipeline, running in shadow mode only.

**Delivers:**
- `FoundationModelClassifier.swift` — coarse + zoom + refinement schemas, token-budgeted three-pass scanner
- `CommercialEvidenceResolver` — resolves FM evidence refs to deterministic catalog entries
- `ScanCohort` provenance tracking
- `SemanticScanResult` table with full operational metrics (token counts, latency, prewarm hit)
- Capability gating via `availability` + `supportsLocale(_:)`
- Failure handling with retry policy
- `CoveragePlanner` — selects fullCoverage / targetedWithAudit / periodicFullRescan based on show maturity and device budget
- Minimal `BackfillJob` with checkpointed phases and coverage policy
- Append-only `EvidenceEvent` logging
- Shadow-mode replay harness with benchmark gates

**Verifiable by:** FM produces classifications on test episodes in shadow mode. Coarse pass covers full transcript. Refinement produces grounded evidence anchors that resolve to catalog entries. Operational metrics are within budget. Replay harness produces stable results within a cohort.

**Files:**
| File | Change |
|------|--------|
| `FoundationModelClassifier.swift` | **NEW** |
| `CommercialEvidenceResolver.swift` | **NEW** |
| `ScanCohort.swift` | **NEW** |
| `CoveragePlanner.swift` | **NEW** |
| `BackfillJob` (model + SQLite) | **NEW** |
| `SemanticScanResult` (model + SQLite) | **NEW** |
| `EvidenceEvent` (model + SQLite) | **NEW** |
| `AdDetectionService.swift` | Add FM scan phase to backfill (shadow only) |
| `AdDetectionConfig` | Add `fmBackfillMode` (default `.shadow`) |
| `AnalysisStore.swift` | New tables, migrations |

### Phase 4 — Region Proposals + Feature Extraction

Build the source-agnostic proposal and feature pipeline.

**Delivers:**
- `RegionProposalBuilder` — normalizes cue harvester + FM proposals into canonical regions with atom-keyed boundaries
- FM consensus clustering (anchor-consistent, IoU-based)
- `RegionFeatureExtractor` — computes uniform feature bundle (lexical, acoustic, transcript quality, prior, FM evidence) for all proposed regions

**Verifiable by:** Proposals from different sources are merged into canonical regions. Feature bundles are uniform regardless of origin. FM-origin regions get lexical/acoustic features; lexical-origin regions get FM evidence.

**Files:**
| File | Change |
|------|--------|
| `RegionProposalBuilder.swift` | **NEW** |
| `RegionFeatureExtractor.swift` | **NEW** |
| `ClassifierService.swift` | Extract reusable region-scoring helpers |

### Phase 5 — Timeline Projection + Minimal Decoder

Project evidence onto the atom timeline and decode contiguous spans.

**Delivers:**
- `AtomEvidenceProjector` — projects all region evidence onto per-atom timeline with hard user-correction masks
- `MinimalContiguousSpanDecoder` — duration-constrained smoothing that prevents fragmented micro-spans and over-merging
- Exact-span correction masks on the atom timeline

**Verifiable by:** Decoded spans are contiguous (no micro-fragments). Duration constraints prevent spans shorter than minimum or longer than maximum. Correction masks prevent re-detection of user-reverted spans.

**Files:**
| File | Change |
|------|--------|
| `AtomEvidenceProjector.swift` | **NEW** |
| `MinimalContiguousSpanDecoder.swift` | **NEW** |

### Phase 6 — Evidence Fusion + Current-Episode Decisions

The decision layer. Enables FM to affect current-episode skip cues.

**Delivers:**
- `BackfillEvidenceFusion` — evidence ledger accumulation with `DecisionMapper`
- `SkipEligibilityGate` with evidence quorum logic
- `SkipPolicyMatrix` mapping `(commercialIntent, ownership)` → action
- `DecisionCohort` provenance tracking
- `AdDecisionResult` table (replayable decisions)
- `DecisionEvent` + `CorrectionEvent` append-only logging
- `DecisionStabilityPolicy` — anti-churn rules for materialized cues
- `BoundaryRefiner` integration (extract shared logic from `ClassifierService`)
- Single promote/suppress pass
- `AdWindow` gains `evidenceSources` and `eligibilityGate` fields
- FM certainty bands calibrated per cohort by `DecisionMapper`

**Verifiable by:** FM-assisted rescoring promotes borderline candidates above skip threshold. FM-only regions with sufficient evidence quorum become skip-eligible. FM-only regions without quorum stay blocked. User-reverted spans are not re-promoted. Existing materialized cues are stable across decision recomputation.

**Files:**
| File | Change |
|------|--------|
| `BackfillEvidenceFusion.swift` | **NEW** |
| `DecisionCohort.swift` | **NEW** |
| `DecisionStabilityPolicy.swift` | **NEW** |
| `AdDecisionResult` (model + SQLite) | **NEW** |
| `DecisionEvent` (model + SQLite) | **NEW** |
| `CorrectionEvent` (model + SQLite) | **NEW** |
| `AdWindow` | Add `evidenceSources`, `eligibilityGate` |
| `AdDetectionService.swift` | Wire fusion + decoder + refiner into backfill |

### Phase 7 — User Corrections

Scoped correction memory that feeds back into the pipeline.

**Delivers:**
- `UserCorrectionStore` with scoped corrections (`exactSpan`, `sponsorOnShow`, `phraseOnShow`, `campaignOnShow`)
- Correction scope inference from evidence that produced the skip
- Correction decay over time
- Corrections feed negative memory into SponsorKnowledgeStore
- Integration with `AtomEvidenceProjector` correction masks and `BackfillEvidenceFusion` eligibility gates

**Verifiable by:** User "Listen" tap records scoped correction. Future backfills respect correction scopes. Corrections decay after configured period. Broader scopes (sponsorOnShow) are inferred when evidence supports it.

**Files:**
| File | Change |
|------|--------|
| `UserCorrectionStore.swift` | **NEW** |
| `AtomEvidenceProjector.swift` | Integrate correction masks |
| `BackfillEvidenceFusion.swift` | Check correction scopes in eligibility |
| `SkipOrchestrator.swift` | Wire "Listen" tap to correction store |

### Phase 8 — SponsorKnowledgeStore + Hot-Path Feedback

Knowledge write-back with quarantine lifecycle. Connects FM backfill to hot-path improvement.

**Delivers:**
- `SponsorKnowledgeStore` with lifecycle states (candidate / quarantined / active / decayed / blocked)
- Canonical sponsor entities, CTA fragments, vanity URLs, disclosure variants — all deterministically extracted, never FM-authored
- `KnowledgeCandidateEvent` append-only logging
- `CompiledSponsorLexicon` — fast string matcher compiled from active knowledge entries
- `LexicalScanner` integration — consults compiled lexicon with boosted weight
- Knowledge promotion rules with quarantine observation window
- Negative memory from corrections

**Verifiable by:** FM-discovered sponsors enter `candidate` state after first confirmation. Promoted to `quarantined` after deterministic extraction. Promoted to `active` after 2+ episode confirmations. Active entries appear in compiled lexicon. LexicalScanner produces hits from compiled lexicon on future episodes. User rollbacks decay/block entries.

**Files:**
| File | Change |
|------|--------|
| `SponsorKnowledgeStore.swift` | **NEW** |
| `CompiledSponsorLexicon.swift` | **NEW** |
| `KnowledgeCandidateEvent` (model + SQLite) | **NEW** |
| `SponsorKnowledgeMatcher.swift` | Wire to read from knowledge store |
| `LexicalScanner.swift` | Consult compiled lexicon |

### Phase 9 — Ad-Copy Fingerprinting

Second memory channel: repeated ad scripts.

**Delivers:**
- `AdCopyFingerprintStore` with lifecycle states
- MinHash/SimHash fingerprints over confirmed, low-rollback ad spans
- Campaign-level grouping and recency/precision stats
- `AdCopyFingerprintMatcher` reads from fingerprint store
- Fingerprints influence backfill scheduling and fusion scoring

**Verifiable by:** Confirmed ad spans produce fingerprints. Repeated ad scripts across episodes produce high-confidence matches. Fingerprint matches contribute to evidence quorum. Rollback decays fingerprint entries.

**Files:**
| File | Change |
|------|--------|
| `AdCopyFingerprintStore.swift` | **NEW** |
| `AdCopyFingerprintMatcher.swift` | Wire to read from fingerprint store |

### Phase 10 — Training Data Pipeline

Balanced training corpus for the future CoreML hot-path model.

**Delivers:**
- `TrainingExample` table with full provenance (atom keys, cohort JSON, transcript quality)
- Four-bucket classification: positive / negative / uncertain / disagreement
- Materialized from evidence and decision event ledger
- Disagreements (lexical-vs-FM, model-vs-user) flagged as highest-value examples

**Verifiable by:** Training examples accumulate with each backfill run. All four buckets are populated. Provenance enables cohort filtering. Disagreement examples are correctly identified.

**Files:**
| File | Change |
|------|--------|
| `TrainingExample` (model + SQLite) | **NEW** |
| `AdDetectionService.swift` | Materialize training examples after decisions |

### Phase 11 — Rollout Progression

Move from shadow to production.

**Delivers:**
- Approved cohort gating (unknown cohorts default to shadow)
- Replay harness with benchmark gates
- Rollout sequence: shadow → rescoreOnly → proposalOnly → full
- Operational metrics dashboard
- Random negative auditing for recall estimation

**Verifiable by:** Mode transitions are gated by benchmark results. Unknown OS builds stay in shadow. Operational metrics are within budget targets. Random audits produce recall estimates.

### Phase 12 — Rich Timeline Decoder

Replace minimal decoder with full 4-state sequence decoder.

**Delivers:**
- `TimelineSegmentationDecoder` with 4 states: `content` / `paidAd` / `ownedPromo` / `transition`
- Viterbi decoding with hand-tuned duration priors and transition penalties
- User-correction masks as hard state constraints
- `RegionProposalBuilder` and `BackfillEvidenceFusion` become evidence emitters (Phase 5/6 logic preserved as fallback)

**Verifiable by:** Decoder produces cleaner segmentation than minimal decoder. Fewer fragmented spans, fewer over-merged windows. Duration priors prevent implausible span lengths. Correction masks hold.

**Files:**
| File | Change |
|------|--------|
| `TimelineSegmentationDecoder.swift` | **NEW** |
| `AdDetectionService.swift` | Route through full decoder when available |

### Phase 13 — Hardened Scheduling

Upgrade BackfillJob with watchdog recovery and smarter prioritization.

**Delivers:**
- `lastHeartbeat` on BackfillJob for watchdog recovery
- Priority scheduling by expected information gain (listener value x uncertainty x slot prior x replay potential)
- `ShowAdSlotModel` — per-show position/duration priors for ad breaks
- Adaptive policy escalation (audit misses → fullCoverage)
- Job invalidation when cohort hashes change mid-job
- `TranscriptAlignmentMap` for cross-version correction/training lineage

**Verifiable by:** Jobs resume after crash/suspension. Priority ordering matches expected information gain. Shows with degrading recall escalate to full coverage. Corrections survive transcript reprocessing via alignment map.

**Files:**
| File | Change |
|------|--------|
| `ShowAdSlotModel.swift` | **NEW** |
| `TranscriptAlignmentMap.swift` | **NEW** |
| `BackfillJob` | Add `lastHeartbeat`, richer priority |
| `CoveragePlanner.swift` | Use ShowAdSlotModel + information gain |

### Phase 14 — Learned Models

Replace hand-tuned components with trained models.

**Delivers:**
- Learned fusion/calibration model (logistic/isotonic) trained from shadow-mode data
- Learned transition model for timeline decoder
- CoreML hot-path model trained from TrainingExample corpus

### Phase 15 — Advanced Features

- Parallel backfill jobs under admission control
- Cross-show transfer learning for cold-start episodes
- Speaker continuity profiling (voice clustering for guest-promo vs paid-ad disambiguation)

---

## File Changes Summary (all phases)

| File | Phase | Change |
|------|-------|--------|
| `TranscriptAtom.swift` | 1 | **NEW** |
| `TranscriptSegmenter.swift` | 1 | **NEW** |
| `TranscriptQualityEstimator.swift` | 1 | **NEW** |
| `AcousticBreakDetector.swift` | 2 | **NEW** |
| `EvidenceCatalogBuilder.swift` | 2 | **NEW** |
| `SponsorKnowledgeMatcher.swift` | 2 | **NEW** |
| `AdCopyFingerprintMatcher.swift` | 2 | **NEW** |
| `FoundationModelClassifier.swift` | 3 | **NEW** |
| `CommercialEvidenceResolver.swift` | 3 | **NEW** |
| `ScanCohort.swift` | 3 | **NEW** |
| `CoveragePlanner.swift` | 3 | **NEW** |
| `RegionProposalBuilder.swift` | 4 | **NEW** |
| `RegionFeatureExtractor.swift` | 4 | **NEW** |
| `AtomEvidenceProjector.swift` | 5 | **NEW** |
| `MinimalContiguousSpanDecoder.swift` | 5 | **NEW** |
| `BackfillEvidenceFusion.swift` | 6 | **NEW** |
| `DecisionCohort.swift` | 6 | **NEW** |
| `DecisionStabilityPolicy.swift` | 6 | **NEW** |
| `UserCorrectionStore.swift` | 7 | **NEW** |
| `SponsorKnowledgeStore.swift` | 8 | **NEW** |
| `CompiledSponsorLexicon.swift` | 8 | **NEW** |
| `AdCopyFingerprintStore.swift` | 9 | **NEW** |
| `TimelineSegmentationDecoder.swift` | 12 | **NEW** |
| `ShowAdSlotModel.swift` | 13 | **NEW** |
| `TranscriptAlignmentMap.swift` | 13 | **NEW** |
| `TranscriptChunk` | 1 | Add `transcriptVersion`, `atomOrdinal` |
| `AdWindow` | 6 | Add `evidenceSources`, `eligibilityGate` |
| `AdDetectionService.swift` | 3,6,10,12 | Progressive backfill flow rewiring |
| `AdDetectionConfig` | 3 | Add `fmBackfillMode`, scan budget, consensus threshold |
| `ClassifierService.swift` | 4 | Extract reusable region-scoring helpers |
| `LexicalScanner.swift` | 8 | Consult compiled sponsor lexicon |
| `SkipOrchestrator.swift` | 7 | Wire "Listen" to correction store |
| `AnalysisStore.swift` | 1,3,6,7,8,9,10 | Progressive table additions + migrations |
| `MetadataExtractor.swift` | — | No change |
| `FoundationModelExtractor.swift` | — | No change |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| FM hallucinating sponsor names | FM emits evidence refs, not strings; deterministic catalog builder extracts from transcript |
| FM hallucinating ad classifications | Schema-bound + consensus + evidence quorum + shadow rollout + approved cohort gating |
| FM inference too slow / thermal | Token-budgeted windows + serial default + thermal gating + BackfillJob checkpointing |
| FM not available | Graceful fallback via `availability` enum; backfill runs identically to today |
| FM failures misread as negatives | Explicit failure taxonomy; FM negative/abstain never suppresses in v1 |
| Prompt changes invalidate results | ScanCohort hashes for precise invalidation; DecisionCohort separate from scan |
| Policy tweaks force FM rescans | ScanCohort and DecisionCohort are separate; policy changes only recompute decisions |
| False positives increase | Evidence quorum gates; eligibility expressed as gates, not numeric clamps |
| FM re-promotes user-corrected skips | Scoped UserCorrectionStore with sponsor/phrase/campaign-level memory |
| Cohort drift destabilizes UX | Anti-churn policy; unknown cohorts default to shadow; replay harness approval |
| Uncalibrated FM confidence | Certainty bands calibrated externally per cohort; no raw doubles in policy |
| Positive-only training data | Balanced four-bucket corpus with disagreements |
| Sponsor knowledge poisoning | Quarantine lifecycle; only deterministic extractions enter store; negative memory |
| Transcript identity fragile | Versioned ordinals + alignment maps; content hash is diagnostic, not primary key |

## What This Does NOT Change

- Hot path detection flow (still regex + acoustics + compiled sponsor lexicon, <100ms)
- SkipOrchestrator thresholds or policy
- Metadata extraction (Layer 3 stays separate)
- Trust scoring mechanics
- User-facing skip/listen behavior
