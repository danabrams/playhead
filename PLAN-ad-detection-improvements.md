# Playhead Ad Detection Improvements — Implementation Plan

**Date:** 2026-04-15
**Status:** Draft v3 — post second expert review
**Scope:** 7 improvements to the on-device ad detection pipeline, plus architectural upgrades to decision model, trust system, and rollout safety
**Constraints:** On-device only (legal mandate), precision-over-recall posture, no framework swaps without approval

---

## Table of Contents

0. [Architectural Foundations (new in v2)](#0-architectural-foundations)
1. [Context & Goals](#1-context--goals)
2. [Current Pipeline Summary](#2-current-pipeline-summary)
3. [Improvement 1: Metadata Prewarm](#3-improvement-1-metadata-prewarm)
4. [Improvement 2: Two-Stage Boundary Refinement](#4-improvement-2-two-stage-boundary-refinement)
5. [Improvement 3: Exact Local Corrections (Layer A)](#5-improvement-3-exact-local-corrections-layer-a)
6. [Improvement 4: Hierarchical Priors](#6-improvement-4-hierarchical-priors)
7. [Improvement 5: Reliability-Aware Fusion](#7-improvement-5-reliability-aware-fusion)
8. [Improvement 6: Broader Learned Corrections (Layer B)](#8-improvement-6-broader-learned-corrections-layer-b)
9. [Improvement 7: FM Budget Scheduler](#9-improvement-7-fm-budget-scheduler)
10. [Metrics & Measurement](#10-metrics--measurement)
11. [Rollout Order & Dependencies](#11-rollout-order--dependencies)
12. [Risk Register](#12-risk-register)

---

## 1. Context & Goals

### What Playhead Does

Playhead is an iOS podcast player whose core feature is automatic ad detection and skip. All processing runs entirely on-device — no audio, transcripts, or classification data ever leave the phone. The detection pipeline uses layered signals: lexical scanning (~60-70% of ads caught here), acoustic break detection, music-bed classification (Apple SoundAnalysis), a rule-based classifier, Apple's on-device Foundation Model (FM), and cross-episode ad-copy fingerprinting.

### Why These Improvements

The current pipeline (phases 1-8 landed) is production-grade with strong precision. Three structural weaknesses remain:

1. **Cold-start gap.** First-episode recall is significantly lower than steady-state because per-show priors (sponsors, slot positions, jingle fingerprints) don't exist yet. The lexical scanner has no show-specific vocabulary on episode 1.
2. **Boundary accuracy ceiling.** Feature windows are 2 seconds and transcript atoms are 1-2 seconds. That granularity finds regions but can't deliver sub-second boundary precision. Users perceive boundary errors as "broken" even when the ad was correctly detected.
3. **Correction bluntness.** User corrections are stored but applied with coarse scope. A veto on one false positive can suppress future true positives on the same show, and there's no causal attribution for *why* the error occurred.

### Design Principles

- **Precision over recall.** One bad skip hurts trust more than one missed ad. Every change must preserve or improve precision.
- **Four-stage decision pipeline.** Every signal is tagged with its role: proposal (should a region exist?), classification (what kind of content?), boundary (where does it start/end?), or policy (should the player skip it?). Detection, content classification, and skip eligibility are separate outputs with separate surfaces.
- **No span without anchor — enforced by typed proposal authority.** Proposal authority is graded: `.strong` (URL, promo code, disclosure, FM containsAd, fingerprint) or `.weak` (metadata, position prior, music bracket, lexical without anchor). Auto-skip requires either one strong proposal source or two weak proposal sources from different families, plus confirmation >= threshold. This replaces the convention-based invariant with a mechanically enforced one.
- **On-device mandate.** Legal requirement. No audio, transcript, or classification data leaves the device.
- **Incremental accretion.** Each improvement builds on existing services. No new pipeline stages, no framework swaps, no external dependencies.
- **Orthogonal trust updates.** A source may not raise its own trust unless corroborated by an orthogonal evidence family or explicit user correction. Metadata cannot validate metadata. Position priors cannot validate position priors.
- **Final decisions are deterministic.** A span finalizer enforces non-overlap, minimum content gap, duration bounds, and policy caps after fusion. No hidden interactions between adaptive weights.
- **Calibrated fusion.** Every source passes through a monotone calibration layer into a common evidence space before fusion. Thresholds are versioned data (bundled with ScoreCalibrationProfile), not magic constants. Thresholds must be revalidated whenever a new source, trust multiplier, or cap is introduced.
- **Measurable.** Every change has a replay-harness-testable metric before it ships.

### Origin

These improvements were identified through adversarial cross-model ideation (Claude Opus 4.6 + OpenAI Codex GPT-5.3, 10 ideas total), then refined by two rounds of expert review that widened, sharpened, and resequenced the proposals. The second review (GPT Pro Extended Reasoning) identified structural gaps in the decision model, trust system, and rollout strategy.

---

## 0. Architectural Foundations

*Added in v2. These three cross-cutting changes should be implemented before or alongside the numbered improvements. They address the review's core critique: "the system is adding many new sources of signal, trust, and correction, but the decision model is still mostly scalar."*

### 0.1 Four-Stage Decision Pipeline

The pipeline currently produces a single scalar `skipConfidence`. With richer FM semantics, metadata ownership, and user skip-policy preferences, this scalar conflates "should a region exist," "what kind of content is it," "where are its boundaries," and "should the player skip it." Those are four distinct questions.

**Stage 1 — Proposal:** Should a region exist at all?
Every evidence source carries typed proposal authority:
```swift
enum ProposalAuthority {
    case strong   // URL, promo code, disclosure, FM containsAd, fingerprint match
    case weak     // metadata cue, position prior, music bracket, lexical without anchor
}
```
A candidate region requires either one `.strong` proposal source or two `.weak` sources from different evidence families. This mechanically enforces "no span without anchor."

**Stage 2 — Classification:** What kind of content is it?
```swift
enum ContentClass {
    case thirdPartyPaid     // Classic third-party ad (BetterHelp, Squarespace)
    case affiliatePaid      // Affiliate link promotion
    case networkPromo       // Network cross-promotion
    case showPromo          // Host promoting own product/show
    case ownedProduct       // Host-owned product mention (merch, Patreon)
    case editorialMention   // Not commercial
    case unknown            // Insufficient signal
}
```
Classification uses FM refinement semantics (intent + ownership) and OwnershipGraph resolution. Classification confidence is separate from proposal confidence — a region can be confidently proposed but uncertainly classified.

**Stage 3 — Boundary:** Where does it start/end?
Boundary resolution (Improvement 2) operates on proposed regions. Boundary quality is independent of classification or policy.

**Stage 4 — Policy:** Should the player skip it?
```swift
enum SkipEligibility {
    case autoSkipEligible    // Skip without user action
    case markOnly            // Show marker, user taps to skip
    case userConfigurable    // Respects per-show/per-type policy
    case ineligible          // Do not skip
}

struct SpanDecision {
    let proposalScore: Double
    let classificationScore: Double
    let contentClass: ContentClass
    let skipEligibility: SkipEligibility
    let boundaryEstimate: BoundaryEstimate  // see Improvement 2
}
```
Skip policy is determined by `SkipPolicyMatrix` based on ContentClass, user preferences, and correction history. **Policy never influences proposal or classification scores.** "Some users want to hear host-reads" is a policy question, not a reliability question — it does not lower classification confidence.

**Decision thresholds (updated):**
- Candidate: 0.40 (proposal)
- MarkOnly: 0.60 (proposal + partial classification)
- Confirmation: 0.70 (proposal + classification)
- Auto-skip: 0.80 (proposal + classification + policy allows)

Gray-band spans (0.60-0.80) are surfaced as suspected sponsor segments without automatic skipping. Users can one-tap skip or teach the app a policy.

**Note:** The markOnly/gray-band UX is a significant product decision. The threshold values and UI treatment require explicit approval before implementation.

**Why four stages matter:**
- Prevents metadata/priors/music from quietly gaining proposal authority
- Makes the "no span without anchor" rule mechanically enforceable via typed ProposalAuthority
- "Is commercial" and "should auto-skip" never mix in the same score
- Future configurable skip behavior (skip third-party only, show house promos) is trivial
- Correction semantics are cleaner: "that's not an ad" (classification override) vs "don't skip this type" (policy override)

### 0.2 Deterministic Span Finalizer

After fusion produces SpanDisposition for each candidate, a deterministic finalizer enforces hard constraints:

1. **Non-overlap:** No two spans may overlap. If they do, the higher-confidence span wins; the other is trimmed or suppressed.
2. **Minimum content gap:** Adjacent spans must have >= 3s of content between them, or be merged.
3. **Duration sanity:** Spans < 5s are dropped. Spans > 180s are split at longest internal gap (existing rule, preserved).
4. **Chapter penalties:** Spans crossing into high-quality `.content` chapters get their skipEligibility capped at `.markOnly`.
5. **Action caps:** No more than N minutes of auto-skip per episode (configurable, default 50% of episode duration). Beyond that, excess spans downgrade to markOnly.
6. **Policy enforcement:** User skip-policy overrides are applied here, not in fusion.

**Why a finalizer matters:** With adaptive trust, multiple new evidence sources, and correction-driven demotions all feeding into fusion, the interaction surface is large. A deterministic finalizer is a safety layer that makes the system easier to reason about and debug. It guarantees invariants regardless of upstream behavior.

### 0.3 Sponsor Entity Graph + Ownership Graph

**Pull alias/domain canonicalization forward as a foundational layer.** Without this, metadata prewarm, corrections, priors, fingerprints, and network aggregation all fragment around different spellings and URLs.

```swift
struct SponsorEntityGraph {
    // Canonical sponsor identity
    var canonicalName: String
    var aliases: Set<String>        // AG1, Athletic Greens, drinkag1
    var domains: Set<String>        // athleticgreens.com, drinkag1.com (eTLD+1 normalized)
    var promoCodes: Set<String>     // AG20, GREENS
    var pathShapes: Set<String>     // /podcast, /show-name (tracking params stripped)
}

struct OwnershipGraph {
    var showOwnedDomains: Set<String>
    var networkOwnedDomains: Set<String>
    var sponsorOwnedDomains: [String: String]  // domain → canonical sponsor
    var uncertainDomains: Set<String>
}
```

**Key changes from current SponsorKnowledgeStore:**
- eTLD+1 domain normalization (strip subdomains, tracking parameters, UTM)
- Alias canonicalization (AG1 → Athletic Greens)
- Ownership labels (showOwned, networkOwned, sponsorOwned, uncertain)
- Graded quarantine/backoff instead of binary block: quarantineScore 0.0-1.0 with exponential decay, replacing the current binary blocked state

**This graph is shared by:** metadata parsing, lexical scanning, sponsor promotion, corrections, fingerprint matching, and hierarchical priors. Building it first gives all downstream improvements a canonical identity layer.

### 0.4 Transcript Reliability as First-Class Signal

Add `TranscriptReliability` at the atom and region level:

```swift
struct TranscriptReliability {
    let confidence: Double          // ASR confidence, 0.0-1.0
    let normalizationQuality: Double // how well tokens normalized (URL detection, etc.)
    let alternativeCount: Int       // number of ASR alternatives available
}
```

**Consumption points:**
- **Lexical scanner:** Low-confidence ASR tokens cannot satisfy candidate-hit minimums alone. URL and promo-code hits are discounted by token confidence.
- **Fingerprint matcher:** Text similarity weighted by transcript reliability; low-reliability regions fall back more heavily to structural signature components.
- **Metadata corroboration:** Feed-to-audio sponsor name matching gated on transcript confidence (don't count a miss when ASR couldn't reliably transcribe the region).
- **FM scheduling:** Low transcript reliability is an explicit priority reason for FM follow-up — text-based signals are less trustworthy, so spend FM budget to compensate.

---

## 2. Current Pipeline Summary

### Evidence Fusion (BackfillEvidenceFusion.swift)

- Accumulates per-source evidence into `EvidenceLedgerEntry` ledger
- **Weight caps:** FM 0.40, Classifier 0.30, Fingerprint 0.25, Lexical/Acoustic/Catalog 0.20 each
- **FM positive-only rule:** Only `containsAd` dispositions contribute; `noAds`/`uncertain`/`abstain` silently dropped
- **Decision thresholds:** Candidate 0.40, Confirmation 0.70, Auto-skip 0.75, Suppression 0.25
- **Quorum:** FM consensus requires 2+ evidence kinds + transcript quality + duration [5s, 180s]

### Boundary Resolver (TimeBoundaryResolver.swift)

- **Start cue weights:** pauseVAD 0.25, speakerChange 0.20, musicBedChange 0.15, spectralChange 0.20, lexicalDensity 0.20
- **End cue weights:** pauseVAD 0.25, speakerChange 0.20, musicBedChange 0.15, spectralChange 0.15, returnMarker 0.25
- **Snap logic:** Local maxima scoring = `cueBlend - (lambda * normalizedDistance)`, lambda=0.3
- **Snap constraints:** Min boundary score 0.3, min improvement 0.1, per-anchor-type radii (5-15s)
- Music bed onset/offset scores are directional feature-window fields, currently moderate weight

### Correction Store (UserCorrectionStore.swift)

- **CorrectionScope enum:** exactSpan, sponsorOnShow, phraseOnShow, campaignOnShow (colon-delimited serialized)
- **Decay:** Linear from 1.0 at day 0 to 0.1 at day 180 (floor 0.1)
- **Passthrough factor:** FP corrections gate spans if effective confidence < 0.40
- **Boost factor:** FN corrections clamp to [1.0, 2.0]
- **Scope inference:** recordVeto infers exactSpan always + sponsorOnShow if brandSpan evidence present

### Sponsor Knowledge Store (SponsorKnowledgeStore.swift)

- **Lifecycle:** candidate → quarantined → active → decayed/blocked
- **Entity types:** sponsor, cta, url, disclosure
- **Promotion:** minConfirmations=2, maxRollbackRate=0.3, rollbackSpike=0.5
- No alias canonicalization. No network-level state. Binary block (no graduated backoff). **See Section 0.3 for the canonical entity/ownership graph that replaces this gap.**

### Fingerprint Matcher (AdCopyFingerprintMatcher.swift)

- **Text-only:** Jaccard similarity on transcript windows (~30 atoms, overlapping stride)
- **Strong match:** Jaccard >= 0.8 → full boundary transfer after anchor alignment validation
- **Normal match:** Jaccard 0.6-0.8 → hypothesis seeding only
- **Anchor alignment:** Max drift 10s per landmark, min 50% aligned fraction
- No acoustic signature component. No cross-show matching. **See Section 0.4 for transcript reliability gating and Improvement 5 for composite fingerprint upgrade.**

### Lexical Scanner (LexicalScanner.swift)

- **Categories:** sponsor (1.0), promoCode (1.2), urlCTA (0.8), purchaseLanguage (0.9), transitionMarker (0.3)
- **Per-show lexicon:** From PodcastProfile.sponsorLexicon + CompiledSponsorLexicon from SponsorKnowledgeStore active entries, both at 1.5x weight boost
- **Rules:** Min 2 hits for candidate (bypass at weight >= 0.95), 30s merge gap, sigmoid confidence formula
- No metadata-seeded vocabulary. No distinction between show-owned vs external domains.

### FM Backfill (CoveragePlanner.swift)

- **Policies:** fullCoverage (cold start, first 5 episodes), targetedWithAudit (default, 10-15% audit sample), periodicFullRescan (every 10 episodes)
- **Phase scheduling:** fullEpisodeScan, scanHarvesterProposals, scanLikelyAdSlots, scanRandomAuditWindows
- No priority ordering by decision-changing potential. No metadata-informed targeting.

### Episode Ingestion

- Episode model has feedItemGUID, canonicalEpisodeKey, audioURL, duration, publishedAt
- **Gap:** RSS description/summary metadata is not persisted to Episode. AnalysisSummary schema details unclear.
- No chapter parsing integration into ad detection. Feed parser captures podcast:chapter but doesn't persist to Episode or expose to detection.

### Classifier (ClassifierService.swift)

- **Rule-based:** 6 signals weighted — lexical 0.40, rmsDrop 0.20, spectralChange 0.15, music 0.10, speakerChange 0.05, prior 0.10
- **ShowPriors:** normalizedAdSlotPriors, sponsorLexicon, jingleFingerprints, trust weight (observation count / 20, capped 1.0)
- No network-level priors. No archetype classification.

---

## 3. Improvement 1: Metadata Prewarm

### Goal

Eliminate the cold-start recall gap on first-listen episodes by harvesting sponsor and commercial signals from RSS feed metadata before audio analysis begins.

### Why This Matters

The lexical scanner catches 60-70% of ads but relies on show-specific vocabulary that doesn't exist on episode 1. Show notes contain sponsor disclosures in ~70% of modern podcasts. This is free, deterministic ground truth that the pipeline currently ignores.

### What To Build

#### 3.1 Episode Metadata Cue Extraction

Add a metadata parser that runs at episode enqueue time (before audio decode).

**Input:** RSS `<description>`, `<itunes:summary>`, `<content:encoded>` for the target episode, plus the same fields from the last 5-10 episodes of the same show (for recurring sponsor detection).

**Output:** Array of `EpisodeMetadataCue`:

```swift
struct EpisodeMetadataCue {
    let type: MetadataCueType  // see below
    let normalizedValue: String
    let sourceField: MetadataSourceField  // .description, .summary, .contentEncoded
    let confidence: Double  // 0.0-1.0, based on pattern match quality
}

enum MetadataCueType {
    case disclosure          // "sponsored by", "brought to you by"
    case externalDomain      // betterhelp.com/podcast, squarespace.com/show
    case promoCode           // "code PODCAST20"
    case sponsorAlias        // brand name extracted from disclosure context
    case showOwnedDomain     // host's own website, merch store
    case networkOwnedDomain  // network hub site
}
```

**Extraction patterns (deterministic regex, no ML):**

| Pattern | Target | Confidence |
|---------|--------|------------|
| `this episode is (?:brought to you by\|sponsored by\|supported by) ([A-Z][\w &.-]{2,})` | disclosure + sponsorAlias | 0.85 |
| Bare URLs with known advertiser TLDs + show-slug path segments | externalDomain | 0.90 |
| `code\s+([A-Z0-9]{3,})` in commercial context | promoCode | 0.80 |
| URLs matching show's own feed domain or `<itunes:owner>` domain | showOwnedDomain | 0.75 |
| URLs matching known podcast network hubs (e.g., wondery.com, gimlet.com) | networkOwnedDomain | 0.70 |

#### 3.2 Ownership Graph Integration

Metadata parsing feeds into the canonical OwnershipGraph (Section 0.3), which distinguishes "betterhelp.com/podcast" (sponsorOwned) from "myshow.com/merch" (showOwned).

**Sources for ownership classification:**
- `<link>` element in the RSS feed → showOwned
- `<itunes:owner>` URL → showOwned
- Domains appearing in every episode's show notes (high frequency = showOwned)
- Network identity from feed publisher/author fields → networkOwned
- Domains extracted from disclosure context → sponsorOwned
- Everything else → uncertain (requires audio corroboration to classify)

**Storage:** OwnershipGraph, shared across metadata parsing, lexical scanning, sponsor promotion, corrections, and priors.

**Why this matters:** Without ownership classification, a host plugging their own Patreon or merch store will be treated as a third-party sponsor. The expert specifically flagged this as a false-positive risk. The SponsorEntityGraph (Section 0.3) provides the canonical alias resolution so that "AG1", "Athletic Greens", and "drinkag1.com" all map to one entity.

#### 3.3 Metadata Trust Scoring

Not all show notes are equally reliable. Dynamic ad insertion, stale templates, and regional targeting can cause feed-to-audio divergence.

```swift
struct MetadataReliabilityMatrix {
    let showId: String
    var trustByFieldAndCue: [MetadataSourceField: [MetadataCueType: BetaPosterior]]
    var chapterTrust: BetaPosterior
    var lastUpdated: Date

    /// Aggregate trust for a specific cue type from a specific field
    func trust(field: MetadataSourceField, cue: MetadataCueType) -> Double {
        let posterior = trustByFieldAndCue[field]?[cue] ?? globalPrior(for: cue)
        return posterior.posteriorMean * posterior.confidence
    }
}

struct BetaPosterior {
    var alpha: Double
    var beta: Double
    var posteriorMean: Double { alpha / (alpha + beta) }
    var confidence: Double { (alpha + beta) / (alpha + beta + 10.0) }
}
```

**Why per-cue reliability:** `content:encoded` sponsor aliases, external domains, promo codes, show-owned domains, and chapter titles do not fail the same way. A single per-show trust scalar either underuses strong cues or overtrusts noisy ones. The matrix tracks reliability per source field × cue type.

**Initial priors (per cue type):**
| Cue type | Prior | Rationale |
|----------|-------|-----------|
| externalDomain | Beta(2, 8) → 0.20 | URLs are relatively reliable in show notes |
| promoCode | Beta(2, 8) → 0.20 | Promo codes are usually current |
| disclosure | Beta(1, 9) → 0.10 | Disclosure phrases can be templated/stale |
| sponsorAlias | Beta(1, 9) → 0.10 | Brand names can be stale or cross-episode |
| showOwnedDomain | Beta(1, 4) → 0.20 | Usually stable once identified |
| networkOwnedDomain | Beta(1, 4) → 0.20 | Usually stable once identified |

Priors shrink toward global/network/archetype metadata trust as those become available (Improvement 4).

**Trust update:** After each episode analysis, compare metadata cues against detected audio evidence. Corroborated cue → alpha += 1 for that field×cue bucket. Uncorroborated cue → beta += 1. Trust is the posterior mean × confidence, naturally self-regularizing on sparse shows. Recency weighting: observations older than 90 days contribute at 0.5× weight.

**Orthogonal update rule (Section 0 principle):** A metadata cue may only count as "corroborated" when matched by an audio-derived signal from a different evidence family (lexical, acoustic, FM, fingerprint). Metadata cannot validate metadata. The same episode's metadata extraction cannot both seed and validate the trust update — use holdout-compatible traces.

#### 3.4 Three Consumption Points

Metadata cues feed into three existing pipeline components:

**A. Lexical Scanner Setup (primary)**
- External-domain cues and sponsor aliases inject into per-episode ephemeral sponsor lexicon
- Weight: `baseCategoryWeight × metadataTrust × 0.75` (lower than audio-discovered patterns)
- Show-owned domains inject as *negative* patterns — lexical hits on show-owned domains reduce candidate score
- Metadata-seeded tokens do NOT satisfy the 2-hit minimum alone. They only contribute when corroborated by at least one audio-derived signal.

**B. Classifier Prior Score**
- If metadata cues exist for the episode, the classifier's `priorScore` signal gets a small boost in the sigmoid (shift midpoint from 0.25 to 0.22 for metadata-warmed episodes)
- This makes the classifier slightly more sensitive to weak audio signals when metadata suggests ads are present
- Gated behind metadataTrust >= 0.08

**C. FM Window Prioritization**
- Metadata-seeded but not anchor-backed regions get elevated priority in FM scheduling
- Rationale: metadata says "there's probably a Squarespace ad here" but audio signals are weak — this is exactly where FM's semantic understanding adds the most value
- Implementation: CoveragePlanner adds `.metadataSeededRegion` as a scheduling phase, priority between `scanLikelyAdSlots` and `scanRandomAuditWindows`

#### 3.5 Recent Feed History Parsing

Parse the last 5-10 episodes' descriptions (still on-device, still cheap) to build a `RecentFeedSponsorAtlas`:

```swift
struct RecentFeedSponsorAtlas {
    let showId: String
    var recurringSponsors: [String: Int]  // normalized name → episode count
    var recurringDomains: [String: Int]   // domain → episode count
    var lastUpdated: Date
}
```

Sponsors appearing in 3+ of the last 10 episodes get elevated seeding confidence (0.90 vs 0.85 for one-off mentions). This catches "Squarespace sponsors every episode of this show" patterns before the first playback.

#### 3.6 Evidence Provenance

All metadata-seeded evidence carries `provenance: .rssShowNotes` in the evidence catalog. This provenance:
- Prevents metadata-only spans (corroboration requirement)
- Enables trust tracking (did this cue match audio?)
- Keeps sponsor knowledge store integrity (metadata cues don't promote to persistent store until audio-corroborated)

#### 3.7 Episode Model Changes

**Existing gap:** `Episode` persistence currently drops RSS description metadata.

**Required changes:**
- Add `feedDescription: String?` and `feedSummary: String?` fields to Episode model
- Populate from RSS parser during feed sync
- Add `metadataCues: [EpisodeMetadataCue]?` computed or cached field
- Add `metadataTrust: MetadataTrust?` reference to per-show trust

#### 3.8 Chapter-Marker Integration

Chapter markers (ID3 `CHAP` frames, Podcasting 2.0 `<podcast:chapters>` JSON) are a special case of metadata prewarm with much higher precision.

**Chapter evidence:**
```swift
struct ChapterEvidence {
    let startTime: TimeInterval
    let endTime: TimeInterval
    let title: String
    let source: ChapterSource  // .id3, .pc20
    let disposition: ChapterDisposition  // .adBreak, .content, .ambiguous
    let qualityScore: Double   // 0.0-1.0, see below
}
```

**Disposition classification** (deterministic regex on title):
- `.adBreak`: titles matching `sponsor|ad(s|\s+break)?|promo|support|mid-roll|pre-roll|post-roll`
- `.content`: titles matching `interview|q&a|segment|chapter \d+|main|intro|outro` without ad keywords
- `.ambiguous`: everything else

**Chapter quality scoring:** Real-world chapters are often incomplete, stale, mislabeled, or loosely aligned. `qualityScore` is computed from:
- Title specificity (generic "Chapter 3" = low; "Sponsor: Squarespace" = high)
- Episode coverage (chapters covering 80%+ of episode duration = higher quality)
- Alignment with acoustic boundaries (chapter edges that coincide with music/silence = higher quality)
- Cross-episode consistency (same show's chapters are consistently structured = higher quality)
- Feed trustworthiness (MetadataTrust for this show)

**Integration (quality-gated soft constraints, not hard negatives):**
- High-quality `.adBreak` chapters (qualityScore >= 0.7) register in evidence catalog with provenance `.chapter`, contribute to boundary resolver as strong cues (weight 0.40, snap radius ±12s)
- Low-quality `.adBreak` chapters (qualityScore < 0.7) downgrade to weak hints only (weight 0.15, no preferred snap targeting)
- `.content` chapters contribute a **crossing penalty** and cap skipEligibility at `.markOnly` for spans that cross into them, but are NOT hard negatives. Hard veto only when qualityScore is very high (>= 0.85) AND aligned acoustic/lexical evidence agrees.
- Chapter-backed boundary cues get preferred snap targeting in TimeBoundaryResolver only at high quality
- Chapter sponsor names (e.g., "Sponsor: Athletic Greens") extend per-episode lexicon and feed into SponsorEntityGraph (Section 0.3)

**Why soft constraints, not hard negatives:** Chapters are often incomplete, loosely aligned, or mislabeled. Turning them into hard negatives creates quiet false negatives that are hard to spot in aggregate. The quality-gated approach preserves precision benefits without making chapter quality a hidden recall trap.

**Coverage reality:** Chapters are available on a minority of podcasts but deliver near-perfect boundaries when present and high-quality. This is a high-precision, low-coverage signal.

#### 3.9 Test Plan

- Unit tests for metadata parser: sponsor extraction, domain classification, promo code detection, show-owned domain identification
- Unit tests for metadata trust scoring: update logic, clamping, decay
- Integration tests: metadata-seeded episode vs non-seeded episode, measure recall delta on corpus
- Replay fixtures: episodes with known show-notes sponsors, dynamic-insertion episodes where feed diverges from audio
- Negative tests: show notes with guest plugs, newsletter links, merch — verify no false promotion

---

## 4. Improvement 2: Two-Stage Boundary Refinement

### Goal

Reduce boundary errors from seconds to sub-second on jingle-heavy shows, and improve boundary quality across all shows via a fine-resolution local refinement pass.

### Why This Matters

Boundary errors are the most viscerally annoying failure mode. A skip that fires 4 seconds late means the listener hears the start of an ad read. A skip that ends 3 seconds early clips the host's return. The current boundary resolver operates on 2-second feature windows — enough to find regions but not enough for precise edges.

### What To Build

#### 4.1 Coarse Stage: Music-Bed Bracket Detection

Add a finite-state envelope scanner that identifies music-bed brackets around candidate ad regions.

**Input:** Music probability stream (existing 2s feature windows) around candidate span boundaries.

**Output:**
```swift
struct BracketEvidence {
    let onsetTime: TimeInterval
    let offsetTime: TimeInterval
    let templateClass: BracketTemplate
    let coarseScore: Double      // 0.0-1.0
    let showTrust: Double        // per-show music bracket reliability
}

enum BracketTemplate {
    case stingInBedDryOut    // jingle start, music bed, dry end
    case dryInStingOut       // dry start, jingle end
    case hardInFadeOut       // sharp start, gradual fade
    case symmetricBracket    // classic mirror bracket
    case partialOnset        // only start has music cue
    case partialOffset       // only end has music cue
}
```

**Template detection:** Instead of requiring strict symmetry (original proposal), classify the envelope into template families. Real ads often have asymmetric patterns (sting-in/bed/dry-out is very common). The scanner uses a small state machine:

| State | Transition | Condition |
|-------|------------|-----------|
| idle | → onsetCandidate | musicOnsetScore >= 0.6 near span start |
| onsetCandidate | → bedSustained | music probability > 0.3 for 2+ consecutive windows |
| bedSustained | → offsetCandidate | musicOffsetScore >= 0.6 near span end |
| offsetCandidate | → bracketed | offset within 15s of span end |

Templates are classified by which states were reached and the relative strength of onset vs offset.

**Coarse score formula:** `(onsetScore + offsetScore) / 2 × templateReliability`, where `templateReliability` varies by template (symmetricBracket=1.0, stingInBedDryOut=0.9, partialOnset=0.6, etc.).

**Per-show musicBracketTrust:** Tracks how reliably music brackets correlate with confirmed ad spans on this show. Starts at 0.5, updated after each backfill. Shows with recurring ad jingles (already tracked via jingleFingerprints in ShowPriors) get elevated trust.

#### 4.2 Fine Stage: Local Boundary Refinement

After coarse bracket detection identifies candidate snap points, run a fine-resolution local search.

**Scope:** ±3 seconds around each candidate boundary edge. Only runs on candidate edges, not the whole episode. Cheap because it processes a handful of short windows.

**Resolution:** 100-250ms hops on raw audio features (RMS energy, spectral flux). This is below the 2s feature window granularity.

**Implementation:**
```swift
struct FineBoundaryRefiner {
    let searchRadius: TimeInterval = 3.0      // ±3s from coarse snap point
    let hopSize: TimeInterval = 0.15          // 150ms hops
    let minImprovementOverCoarse: Double = 0.05

    func refine(
        coarseEdge: TimeInterval,
        direction: BoundaryDirection,  // .start or .end
        audioBuffer: AVAudioPCMBuffer,
        localFeatures: [FineFeatureWindow]
    ) -> BoundaryEstimate
}

struct BoundaryEstimate {
    let time: TimeInterval           // best point estimate
    let confidence: Double           // 0.0-1.0
    let lowerBound: TimeInterval     // conservative bound (toward content)
    let upperBound: TimeInterval     // aggressive bound (toward ad)
    let cueBreakdown: [BoundaryCue: Double]  // which cues contributed
}

struct FineFeatureWindow {
    let time: TimeInterval
    let rmsEnergy: Double
    let spectralFlux: Double
    let silenceProbability: Double
}
```

**Snap preference order:**
1. Silence gap (rmsEnergy < 0.02 for >= 100ms) — cleanest possible cut
2. Energy valley (local RMS minimum) — natural transition
3. Spectral discontinuity (flux spike) — timbral change
4. Coarse snap point (fallback) — no degradation from today

#### 4.3 Boundary Error Asymmetry

**Key insight from expert:** Not all boundary errors are equal.

| Error type | What happens | Severity |
|------------|-------------|----------|
| Start too late | Listener hears beginning of ad | Medium (annoying) |
| Start too early | Skip clips editorial content | **High** (trust-destroying) |
| End too late | Skip overruns into next content | **High** (trust-destroying) |
| End too early | Listener hears end of ad | Low (acceptable) |

**Implementation:** Add signed error penalty to snap scoring:
- Errors that clip editorial content (start too early, end too late) get 1.5x penalty weight
- Errors that leak ad audio (start too late, end too early) get 1.0x penalty weight
- This biases boundaries toward "let the user hear a fraction more ad" rather than "clip the host's return"

Metric: Track signed boundary errors separately in replay harness (mean signed error, not just MAE).

#### 4.4 Dynamic Snap Radius

Replace fixed per-anchor-type snap radii with adaptive radii based on local signal quality AND the historical spread of cue-conditional boundary priors (Section 5.2):

| Condition | Snap radius |
|-----------|-------------|
| Strong local cues (bracket score > 0.7, fine-stage silence gap found) | 3-6s |
| Moderate local cues (bracket score 0.4-0.7 or spectral transition found) | 6-8s |
| Weak local cues (no bracket, sparse features), FM says boundary precision is coarse | up to 10s |

**Cue-conditional prior influence:** When a matching boundary prior exists (keyed by show + edge direction + bracket template + anchor family + archetype), the prior's spread narrows or widens the radius. Tight historical distributions → tighter radius, more aggressive snap. Wide historical distributions → wider search, reduced action authority.

This replaces the current fixed radii (5-15s per anchor type) with signal-adaptive radii informed by both current local cues and historical show behavior.

#### 4.5 Envelope Is Boundary-Only

**Critical constraint:** Music-bed envelope evidence should never create or extend a span by itself. It only chooses between candidate snap points after the region already exists via other anchors. This preserves the "no span without anchor" invariant and prevents music-heavy editorial content from generating false spans.

#### 4.6 Integration with TimeBoundaryResolver

**Existing integration point:** TimeBoundaryResolver already has directional `musicBedOnsetScore`/`musicBedOffsetScore` fields.

**Changes:**
- Add `bracketEvidence: BracketEvidence?` to span hypothesis
- When bracket evidence exists with coarseScore >= 0.5, elevate music cue weight from 0.15 to up to 0.40 (scaled by coarseScore × showTrust)
- Add FineBoundaryRefiner as a second pass after coarse snap
- Fine stage only runs when coarse snap produced a candidate within the search radius

**Boundary uncertainty → asymmetric guard margins:** The skip executor uses `BoundaryEstimate.lowerBound` / `upperBound` to apply guard margins. When start boundary is uncertain, bias later (let a fraction of ad leak rather than clip editorial). When end boundary is uncertain, bias earlier (same principle). This is the natural operational extension of the asymmetric error policy.

**Fine-feature band cache:** Add `FineFeatureBandCache` keyed by episode + candidate band to avoid repeated 100-250ms feature extraction when a span is revisited during backfill or correction reprocessing.

**Graceful fallback:** When bracket detection doesn't meet thresholds or fine stage doesn't find improvement >= `minImprovementOverCoarse`, the resolver falls back to current weighted-cue behavior. No existing boundary quality is degraded.

#### 4.7 Test Plan

- Unit tests for bracket template detection: each template family, partial brackets, no-bracket cases
- Unit tests for fine boundary refiner: silence gap detection, energy valley, spectral discontinuity
- Integration tests: boundary MAE on jingle-heavy corpus (NPR, Wondery, Gimlet) vs current baseline
- Signed error tracking: verify asymmetric penalty reduces editorial clipping
- Negative tests: music-heavy editorial shows (music criticism, DJ format) — verify no false bracket detection
- Regression tests: shows without jingles — verify no degradation from fine stage

---

## 5. Improvement 3: Exact Local Corrections (Layer A)

### Goal

Make user corrections immediately effective and precisely scoped, without risk of overgeneralization.

### Why This Matters

The current correction store already has scopes (exactSpan, sponsorOnShow, phraseOnShow, campaignOnShow) and decay semantics. But corrections don't attribute *why* the error occurred, and explicit "you missed an ad here" feedback doesn't create a durable fix for the current episode. The expert recommended splitting corrections into two layers: exact local (low risk, high trust) shipped first, broader learned rules (higher leverage, higher risk) shipped later.

### What To Build

#### 5.1 Causal Attribution

When a user corrects a region, the system should store *what caused the error*, not just *where it happened*.

```swift
struct CorrectionAttribution {
    let correctionType: CorrectionType
    let causalSources: [CausalSource]
    let targetRefs: CorrectionTargetRefs
}

enum CorrectionType {
    case falsePositive        // skipped non-ad
    case falseNegative        // missed ad
    case startTooEarly        // clipped editorial before ad
    case startTooLate         // ad audio leaked at start
    case endTooEarly          // ad audio leaked at end
    case endTooLate           // clipped editorial after ad
}

enum CausalSource {
    case lexical              // pattern match on editorial content
    case foundationModel      // FM classified editorial as ad
    case fingerprint          // fingerprint transferred incorrectly
    case musicBracket         // music bracket snapped to wrong edge
    case metadata             // metadata cue matched non-ad content
    case positionPrior        // slot prior triggered on unusual episode format
    case acousticBreak        // acoustic transition misread
}

struct CorrectionTargetRefs {
    let atomIds: [AtomIdentity]?           // specific atoms corrected
    let evidenceRefs: [Int]?               // evidence catalog entries implicated
    let fingerprintId: String?             // fingerprint that transferred badly
    let domain: String?                    // domain implicated (show-owned vs external)
    let sponsorEntity: String?             // sponsor name if relevant
}
```

**How causal sources are inferred:** When a user taps "not an ad" on a region, examine the region's `anchorProvenance` and `signalBreakdown`:
- If region's top evidence source was lexical and the lexical hits were on common editorial phrases → `causalSource: .lexical`
- If region was FM-driven (FM weight > 0.3 of total) → `causalSource: .foundationModel`
- If region was fingerprint-transferred → `causalSource: .fingerprint, fingerprintId: <id>`
- Multiple causal sources can be attributed to one correction

#### 5.2 Exact Episode-Span Corrections

**False positive (veto):**
- Store permanent exactSpan veto (existing behavior)
- NEW: Also store `CorrectionAttribution` with inferred causal sources
- Causal sources get demoted in future scoring for this show (not globally)
- Passthrough factor applies as today (confidence gate at 0.40)

**False negative ("you missed an ad here"):**
- NEW: Create a synthetic episode-local anchor at the corrected region
- This anchor has `provenance: .userCorrection` and creates a real span on the current episode
- The span is visible immediately — user sees their correction take effect
- The synthetic anchor does NOT propagate to other episodes or to the sponsor knowledge store without further corroboration
- Rationale (from expert): "Without this, a user can correct the app and still not see the exact episode fixed. That is a trust tax you do not need to pay."

**Boundary correction ("start too early" / "end too late"):**
- Store cue-conditional boundary priors keyed by `{ showId, edgeDirection, bracketTemplate, anchorFamily, archetype }` — not just a show-wide scalar
- Each prior is a distribution (median + spread), not a single offset. This lets dynamic snap radius depend on historical spread: tight distributions → aggressive snap, noisy distributions → wider search and reduced action authority.
- Signed correction type (startTooEarly, startTooLate, endTooEarly, endTooLate) updates the context-specific distribution
- Per-show boundary priors decay over 90 days (faster than general correction decay of 180 days, because boundary behavior changes with production style changes)
- **Why cue-conditional:** A show can have several ad styles (crisp jingle-in, mushy dry-out, strong mid-rolls, weak pre-rolls). A single show-wide offset smears these together and causes overcorrection.

#### 5.3 Causal Source Demotion

When a causal source is attributed to a false positive, apply a bounded demotion to that source's contribution for the specific show:

| Causal source | Demotion mechanism | Scope | Bound |
|---------------|-------------------|-------|-------|
| lexical | Reduce implicated pattern's weight by 0.2 for this show | show-local | Floor at 0.3× base weight |
| foundationModel | No demotion (FM is too general to scope-demote) | -- | -- |
| fingerprint | Mark fingerprint as "disputed" — require extra corroboration for future transfers | fingerprint-specific | Cleared after 2 subsequent confirmations |
| musicBracket | Reduce musicBracketTrust for this show by 0.1 | show-local | Floor at 0.2 |
| metadata | Reduce metadataTrust for this show by 0.05 | show-local | Floor at 0.05 |
| positionPrior | Reduce slot prior trust for implicated position | show-local | Floor at 0.3 |

This is the "demote the right mechanism instead of suppressing too much" principle from the expert review.

#### 5.4 Classification Override vs Skip-Policy Override

**Key distinction from expert:** "That was a house promo" is not the same as "never skip that kind of thing."

```swift
enum CorrectionOverrideKind {
    case classificationOverride   // "this is/isn't an ad"
    case skipPolicyOverride        // "I don't want to skip this type of thing"
}
```

For Layer A (exact local), all corrections are `classificationOverride` by default. The user is saying "you got the detection wrong" not "I want to hear house promos." Layer B will add `skipPolicyOverride` for broader intent-based rules.

#### 5.5 Implicit Feedback Signals (v3 addition)

Explicit corrections are excellent but sparse. The product already sees several highly informative behaviors that should generate weak labels:

```swift
struct UserFeedbackSignal {
    let type: FeedbackType
    let confidence: Double          // how strongly this implies a correction
    let region: ClosedRange<TimeInterval>
}

enum FeedbackType {
    case immediateUnskip            // user tapped unskip within 3s → likely FP
    case seekBackIntoSkipped        // user rewound into skipped region → boundary or FP
    case rapidRewindAfterSkip       // rewind within 5s → boundary error
    case repeatedManualSkipForward  // user manually skips same unskipped region → likely FN
    case showAutoSkipDisabled       // user turned off auto-skip for this show → broad distrust
}
```

**Weak label rules:**
- Weak labels never create permanent exactSpan vetoes on their own
- Weak labels contribute to `supportCount` and `diversityCount` for Layer B rule promotion at reduced weight (0.3× explicit correction)
- Weak labels influence source trust updates (e.g., repeated unskips on FM-driven spans reduce FM trust for that show)
- Weak labels are a first-class priority reason for FM audit and replay inspection
- `showAutoSkipDisabled` is logged but does not create per-span corrections — it flags the show for diagnostic review

#### 5.6 UI Surface

**One-tap corrections (existing affordances, enhanced):**
- "Not an ad" button on skip banner → exactSpan veto + causal attribution
- "Missed ad" long-press on waveform → synthetic anchor creation
- "Bad boundary" — new: after a skip, if user rewinds within 5s, show a subtle "Boundary off?" prompt with "Start too early" / "End too late" options

**No scope taxonomy exposed.** The user never sees CorrectionScope, CorrectionType, or CausalSource. All inference is automatic.

#### 5.7 Schema Changes to UserCorrectionStore

Extend existing schema with:

```swift
// New fields on correction entries
let correctionType: CorrectionType        // was: implicit from veto/boost
let causalSources: [CausalSource]         // NEW
let targetRefs: CorrectionTargetRefs      // NEW
let overrideKind: CorrectionOverrideKind  // NEW (default: .classificationOverride)
let supportCount: Int                     // NEW (for Layer B promotion)
let diversityCount: Int                   // NEW (distinct episodes, for Layer B)
```

Backward compatible: existing corrections get `correctionType: .falsePositive`, `causalSources: []`, `overrideKind: .classificationOverride`, `supportCount: 1`, `diversityCount: 1`.

#### 5.8 Test Plan

- Unit tests for causal attribution inference: each causal source type, multi-source attribution
- Unit tests for synthetic anchor creation: span visibility, provenance isolation, no cross-episode leakage
- Unit tests for boundary correction: signed offset accumulation, decay, per-show scoping
- Unit tests for causal source demotion: weight reduction, floor clamping, clearance after confirmations
- Integration tests: false positive correction → same pattern on next episode, verify demotion effect
- Integration tests: false negative correction → verify span appears on current episode
- Negative tests: correction on one show doesn't affect another show's scoring

---

## 6. Improvement 4: Hierarchical Priors

### Goal

Give new and low-volume shows useful priors from day one by inheriting network-level and archetype-level knowledge.

### Why This Matters

Currently, learned priors are show-local: slot positions, sponsor lexicon, jingle fingerprints all require multiple episodes to build. New shows and low-volume shows are structurally underserved. The expert recommended two layers above show-level memory: network priors and archetype priors.

### What To Build

#### 6.1 Network Identity

Derive network identity from RSS feed metadata:
- `<itunes:author>` or `<managingEditor>` field
- Feed URL domain patterns (e.g., feeds.wondery.com → Wondery network)
- Publisher field in podcast metadata

```swift
struct NetworkIdentity {
    let networkId: String           // normalized identifier
    let derivedFrom: NetworkSource  // .author, .feedDomain, .publisher
    let confidence: Double          // how confident we are in the grouping
}
```

**Storage:** Add `networkId: String?` to PodcastProfile. Inferred at feed subscription time, updatable on feed refresh.

#### 6.2 Network Priors

Aggregate show-level priors across all shows sharing the same network identity:

```swift
struct NetworkPriors {
    let networkId: String
    var commonSponsors: [String: Double]     // sponsor → frequency across network shows
    var typicalSlotPositions: [Double]       // normalized episode fractions
    var typicalAdDuration: ClosedRange<TimeInterval>  // network-typical ad length
    var musicBracketPrevalence: Double       // fraction of shows with jingle brackets
    var metadataTrustAverage: Double         // average feed reliability across network
    var observationCount: Int                // shows contributing
}
```

**Update cadence:** After each episode backfill, update network priors from show-level data.

**Consumption:** Network priors feed into the classifier's `priorScore` when show-level priors have fewer than 3 episodes. Weight: 0.5× show-level trust, decaying as show-level data accumulates.

#### 6.3 Show Trait Profile (Continuous, Not Hard Archetypes)

Many shows are hybrids: produced cold opens, dry host-read midrolls, narrative segments with sustained music, or seasonal format changes. A hard archetype label produces category mistakes exactly where smooth behavior is most needed.

**Replace hard archetype with a continuous trait vector:**

```swift
struct ShowTraitProfile {
    var musicDensity: Double            // fraction of windows with music > 0.15
    var speakerTurnRate: Double         // speaker changes per minute
    var singleSpeakerDominance: Double  // fraction of time dominated by one voice
    var structureRegularity: Double     // consistency of segment lengths across episodes
    var sponsorRecurrence: Double       // how often the same sponsors appear
    var insertionVolatility: Double     // how much ad placement varies between episodes
    var transcriptReliability: Double   // average ASR confidence
}
```

**Computation:** After 2-3 episodes, compute trait profile from aggregated feature statistics. Update incrementally with each episode. Store on PodcastProfile.

**Debug archetype label:** Optionally derive a human-readable label (jingle-heavy, dry-host, narrative, chat) for logging and QA. Priors and scheduling consume the trait vector directly, not the label.

**Consumption (trait-vector-driven, not label-driven):**
- `musicDensity` + `structureRegularity` → default musicBracketTrust (high music + regular structure = reliable brackets)
- `singleSpeakerDominance` + low `musicDensity` → FM gets more budget (dry host-read shows need semantic understanding)
- `structureRegularity` → metadata trust expectations (regular shows have more reliable show notes)
- `insertionVolatility` → fingerprint transfer confidence (volatile shows need stricter alignment)

#### 6.4 Prior Hierarchy

```
global defaults → network priors → archetype priors → show-local priors
```

Each level overrides the one above when it has sufficient data. Show-local always wins when it has >= 5 episodes of history.

**Guardrail:** Hierarchical priors mostly affect ranking and prioritization, not direct auto-skip authority. They influence classifier priorScore and FM scheduling, not evidence fusion weights.

#### 6.5 Test Plan

- Unit tests for network identity extraction: various RSS formats, edge cases
- Unit tests for archetype classification: each archetype, transitional shows
- Integration tests: new show on known network gets network priors, verify improved first-episode recall
- Integration tests: archetype classification after 2-3 episodes, verify prior adjustments
- Regression tests: show-local priors still dominate after sufficient episodes

---

## 7. Improvement 5: Reliability-Aware Fusion

### Goal

Make evidence fusion source-aware, output both a CommercialContentClass and SkipEligibility (Section 0.1), and use the FM's richer output semantics (commercial intent, ownership) instead of treating FM as a binary "ad or not" signal. Trust updates must be Bayesian with orthogonal corroboration (Section 0 principles).

### Why This Matters

The current fusion system treats all evidence sources with fixed weight caps and drops all FM outputs except `containsAd`. The expert identified this as the biggest architecture gap: the FM already outputs commercial intent (paid, owned, affiliate, organic) and ownership (thirdParty, show, network, guest), but fusion ignores this structure. The second review further identified that adding adaptive trust without posterior modeling and orthogonal update rules creates small-sample instability and self-confirmation loops.

### What To Build

#### 7.1 Source Trust Multiplier (Bayesian)

Replace fixed weight caps with posterior-based adaptive weights:

```
calibratedContribution = baseWeight × posteriorMean(sourceTrust) × classTrust
```

Where:
- `baseWeight` = existing per-source cap (FM 0.40, Classifier 0.30, etc.)
- `posteriorMean(sourceTrust)` = Beta posterior mean for this source on this show, shrunk toward global/network/archetype prior
- `classTrust` = depends on FM refinement output (see 7.2)

**All trust values use the same Beta posterior model as MetadataTrust (Section 3.3).** Each source tracks alpha (corroborated) and beta (uncorroborated) counts with a prior that shrinks toward the hierarchical level above.

**Source trust tracking:**

| Source | Trust derived from | Prior Beta(α,β) | Update trigger | Orthogonal corroborator |
|--------|-------------------|---------|----------------|------------------------|
| Metadata | MetadataTrust (Improvement 1) | Beta(1,9) → 0.10 | Feed-to-audio match | Lexical, acoustic, FM |
| MusicBracket | musicBracketTrust (Improvement 2) | Beta(5,5) → 0.50 | Bracket-to-confirmed-span | Lexical, FM, fingerprint |
| Fingerprint | Transfer success rate per show | Beta(7,3) → 0.70 | Boundary transfer validation | Acoustic, lexical |
| FM | FM confirmation vs rollback rate | Beta(8,2) → 0.80 | User corrections on FM-driven spans | User correction (always orthogonal) |
| Lexical | False positive rate per show | Beta(17,3) → 0.85 | User corrections on lexical-driven spans | User correction (always orthogonal) |

**Orthogonal update rule:** A source may only gain trust when corroborated by a signal from a different evidence family. Evidence families: `metadata, lexical, acoustic, fingerprint, fm, chapter, correction, prior`. The same episode may not both seed and validate the same trust update. Learned priors are updated on holdout-compatible traces.

#### 7.2 Richer FM Positive Semantics

When FM reports `containsAd` with refinement data, use commercial intent and ownership to modulate contribution:

**Classification confidence** (how confident is the commercial-content detection?):
| Intent | Ownership | classificationTrust | Rationale |
|--------|-----------|---------------------|-----------|
| paid | thirdParty | 1.0 | Strong commercial signal — high classification confidence |
| paid | show | 1.0 | Still clearly commercial — **skip behavior is a policy question (Section 0.1), not a classification discount** |
| owned | show | 0.7 | Ambiguous commercial intent — lower classification confidence |
| affiliate | thirdParty | 0.9 | Usually commercial |
| organic | any | 0.15 | Barely commercial — low classification confidence |
| unknown | any | 0.6 | Can't determine |

**Key v3 change:** `paid | show` no longer gets a lower classificationTrust. It is confidently commercial. Whether to auto-skip it is determined by `SkipPolicyMatrix` in Stage 4, not by classification scoring. This enforces the principle that "some users want to hear host-reads" is a policy question, not a reliability question.

This replaces the current behavior where all `containsAd` dispositions contribute equally at 0.40 cap.

#### 7.3 Negative FM Evidence as Targeted Suppression (Not Global Subtraction)

The current FM positive-only rule (drop all non-`containsAd` results) is conservative but leaves useful structure unused. However, any form of global score subtraction (even bounded) introduces cancellation behavior that is hard to explain and debug. A strong `noAds` FM result should not "fight" a real fingerprint match or disclosure anchor in the same additive pool.

**Instead: targeted suppression of the specific weak evidence FM is refuting.**

**Targeted suppression applies ONLY when ALL of these are true:**
1. The region is weakly anchored (total non-FM evidence < 0.35)
2. The positive evidence is mostly lexical/position-based (no URL, no promo code, no disclosure)
3. Two overlapping FM windows report `noAds` with certainty `strong`
4. The region does NOT have a fingerprint match

**Effect:** Strong FM noAds does NOT subtract globally. Instead it:
1. **Downweights the specific weak evidence types it is refuting** — lexical drift, slot priors, vague metadata cues — for this region only
2. **Raises the local auto-skip requirement** unless at least one non-suppressed `.strong` proposal source remains
3. **Caps skipEligibility at `.markOnly`** if no strong proposal survives suppression
4. **Logs a suppression reason** for replay attribution and correction analysis

`uncertain` and `abstain` FM results are retained for scheduling/audit priority even when they do not affect fusion.

**Why targeted suppression is safer than either global subtraction or simple action caps:** It preserves monotonicity for strong positive anchors (URLs, disclosure phrases, fingerprints are never touched). It only reduces the contribution of the weak signals FM is actually qualified to refute. And it produces legible replay behavior — you can see exactly which evidence was suppressed and why.

#### 7.4 Fusion Weight Changes

Updated weight structure:

```swift
struct AdaptiveFusionWeightConfig {
    // Base caps (unchanged)
    let fmBaseCap: Double = 0.40
    let classifierBaseCap: Double = 0.30
    let fingerprintBaseCap: Double = 0.25
    let lexicalBaseCap: Double = 0.20
    let acousticBaseCap: Double = 0.20
    let catalogBaseCap: Double = 0.20

    // New: metadata source
    let metadataBaseCap: Double = 0.15

    // Calibrated contribution (v3: monotone calibration + posterior trust)
    func calibratedContribution(
        source: EvidenceSource,
        rawSignal: Double,
        sourceTrust: BetaPosterior,
        classTrust: Double,
        calibrationProfile: ScoreCalibrationProfile
    ) -> Double {
        let calibrated = calibrationProfile.calibrator(for: source).apply(rawSignal)
        return baseCap(for: source) * calibrated * sourceTrust.posteriorMean * classTrust
    }
}

/// Versioned calibration data bundled on-device, learned from replay corpus.
/// Thresholds are part of the profile — they must be revalidated when sources change.
struct ScoreCalibrationProfile {
    let version: Int
    let sourceCalibrators: [EvidenceSource: MonotonicCalibrator]
    let thresholds: DecisionThresholds
}

/// Piecewise-linear monotone mapping from raw signal to calibrated evidence space.
/// Lightweight and deterministic — no runtime ML.
struct MonotonicCalibrator {
    let breakpoints: [(input: Double, output: Double)]
    func apply(_ raw: Double) -> Double { /* piecewise-linear interpolation */ }
}

// Output: SpanDecision (Section 0.1) rather than scalar skipConfidence
// Negative FM evidence acts via targeted suppression (7.3), not score subtraction
```

#### 7.5 Composite Fingerprint Upgrade (v2 addition)

The current text-only Jaccard matcher is fragile under ASR errors, host paraphrase, and transcript formatting drift. Upgrade fingerprints to composite signatures:

**Composite fingerprint components:**
1. **Transcript minhash / character n-gram similarity** — more ASR-robust than word-level Jaccard
2. **Lightweight acoustic signature** — music bed contour, prosodic rhythm, spectral sketch near boundaries
3. **Sponsor-marker alignment** — positions of URL, promo code, disclosure within the ad read

**Matching modes:**
- **Same-show strong match:** Composite score >= threshold → boundary transfer after anchor alignment + ownership/commercial-marker validation
- **Cross-show match (new, controlled):** Allowed only for same-network or local-device corpus matches with very strong composite confidence AND commercial markers (URL, promo, disclosure). Cross-show matches seed hypotheses only — never auto-confirm.

**Why cross-show matters:** Many dynamically inserted ads repeat across shows in the same network. Text-only matching misses these because surrounding editorial context differs. Acoustic + sponsor-marker alignment catches them.

**Text similarity weighting:** Gated by TranscriptReliability (Section 0.4). Low-reliability regions fall back more heavily on acoustic and structural signature components.

**Implementation note:** Acoustic fingerprinting is substantial engineering (locality-preserving audio hashes, robust to mixing level and bed presence, bounded false-collision rate). This ships in Phase D alongside hierarchical priors, not in the initial phases.

#### 7.6 Test Plan

- Unit tests for Bayesian source trust: posterior update, shrinkage, orthogonal corroboration enforcement
- Unit tests for classTrust computation from FM refinement output
- Unit tests for negative FM action cap: all guard conditions, markOnly capping, audit triggering
- Integration tests: paid/thirdParty vs owned/show ads — verify differential treatment via CommercialContentClass
- Integration tests: weak lexical region + strong FM noAds — verify markOnly cap (not score subtraction)
- Regression tests: well-anchored regions unaffected by negative FM action cap
- Replay corpus: measure precision/recall delta with adaptive vs fixed weights
- Trust churn tests: verify trust stability on sparse shows (< 3 episodes)
- Composite fingerprint tests: ASR variation corpus, cross-show DAI corpus

---

## 8. Improvement 6: Broader Learned Corrections (Layer B)

### Goal

Turn repeated user corrections into durable per-show rules that prevent recurring errors.

### Why This Matters

Layer A (Improvement 3) handles exact corrections. Layer B generalizes from patterns: "this host always talks about their course and it's not an ad" becomes a persistent phrase-on-show suppression. This is higher leverage but higher risk — overgeneralization can suppress true positives.

### What To Build

#### 8.1 Rule Promotion Requirements

Broader rules only promote after sufficient evidence:

| Scope | Minimum support | Minimum diversity | Promotion trigger |
|-------|----------------|-------------------|-------------------|
| phraseOnShow | 3 corrections | 2 distinct episodes, 2 distinct dates | Same phrase pattern corrected repeatedly |
| sponsorOnShow | 2 corrections | 2 distinct episodes | Same sponsor entity corrected |
| domainOwnershipOnShow | 2 corrections | 2 distinct episodes | Same domain classified as show-owned |
| jingleOnShow | 3 corrections | 2 distinct episodes | Music bracket false positive on same show |

**Why diversity matters:** "Repeated taps on one bad region" should not promote a broad rule. The expert specifically flagged this: corrections must span multiple episodes and dates to demonstrate a persistent pattern, not a one-off error.

#### 8.2 Skip-Policy Override

Layer B introduces the distinction between "that's not an ad" (classification override) and "I don't want to skip this type of thing" (skip-policy override):

- User taps "not an ad" → classificationOverride (system was wrong about detection)
- User taps "don't skip house promos" → skipPolicyOverride (system was right about detection but user wants different behavior)

Skip-policy overrides live in `SkipPolicyMatrix` and affect eligibility gating without changing the underlying confidence score. This separation becomes important if Playhead later offers configurable skip behavior (skip third-party only, skip all, etc.).

#### 8.3 Decay and Expiration

| Scope | Base TTL | Renewal trigger | Expiration behavior |
|-------|---------|----------------|-------------------|
| phraseOnShow | 120 days | Refreshed on each confirming correction | Linear decay to 0.1, then removed |
| sponsorOnShow | 180 days | Refreshed when sponsor is corrected again | Same |
| domainOwnershipOnShow | 360 days | Refreshed on domain correction | Longer TTL because domain ownership is stable |
| jingleOnShow | 90 days | Refreshed on music bracket correction | Shorter because production style changes |

**Interaction with permanent vetoes:** User-confirmed exactSpan vetoes never decay (existing behavior, preserved). Broader inferred rules always decay. These are separate mechanisms that don't conflict.

#### 8.4 Test Plan

- Unit tests for rule promotion: support/diversity thresholds, per-scope requirements
- Unit tests for decay: TTL expiration, renewal, interaction with permanent vetoes
- Unit tests for skipPolicyOverride vs classificationOverride: verify independence
- Integration tests: repeated corrections across episodes → rule promotion → verify effect on subsequent episodes
- Negative tests: corrections on one show don't create rules for another show
- Negative tests: corrections on one episode (below diversity threshold) don't promote to broader rules

---

## 9. Improvement 7: FM Budget Scheduler

### Goal

Allocate the FM's capped compute budget to windows most likely to change the final skip decision.

### Why This Matters

The FM budget is capped at 300 seconds per backfill run. Currently, CoveragePlanner uses fixed policies (full scan for cold start, targeted+audit for steady state). Smarter allocation can improve detection quality within the same budget by focusing FM on windows where it matters most.

### What To Build

#### 9.1 Expected Value of Information (EVI) Score

For each candidate FM window, compute a priority score based on the expected value of running FM classification:

```
EVI = P(decision flip) × utility gain / compute cost
```

This naturally favors windows where an additional model call is likely to matter and cheap enough to justify.

```swift
struct FMWindowPriority {
    let window: TranscriptWindow
    let eviScore: Double        // decision value per unit compute
    let reason: PriorityReason
    let estimatedCost: Double   // estimated FM seconds for this window
}

enum PriorityReason {
    case nearConfirmationThreshold    // score 0.55-0.72, FM could push over
    case coldStartShow                // first few episodes, no priors
    case metadataSeededUnanchored     // metadata says ad, audio signals weak
    case boundaryUncertain            // region exists but edges are fuzzy
    case pharmaFallbackCandidate      // sensitive content, FM might refuse
    case recentUserCorrection         // show has recent corrections, needs recheck
    case lowTranscriptReliability     // ASR confidence low, text signals untrustworthy
    case strongFingerprintMatch       // already confident — deprioritize
    case obviouslyClean               // no signals at all — deprioritize
    case alreadyConfirmed             // strong multi-source confirmation — deprioritize
}
```

#### 9.2 Priority Ordering

**High priority (allocate FM first):**
1. Regions near confirmation threshold (0.55-0.72) — FM is the tiebreaker
2. Cold-start shows (< 5 episodes) — no priors to compensate
3. Metadata-seeded but not anchor-backed regions — metadata suggests ad, needs semantic confirmation
4. Boundary-uncertain regions (no bracket, no strong local cues) — FM boundary precision helps
5. Pharma/medical fallback candidates — known FM refusal risk, needs permissive path routing
6. Shows with recent user corrections — detection may be drifting

**Low priority (deprioritize):**
7. Strong fingerprint matches — already confident, FM is redundant
8. Obviously clean regions (no signals from any layer) — FM won't find anything
9. Already well-confirmed regions with sharp boundaries — FM can't improve the outcome

#### 9.3 BudgetCoordinator (Energy-Aware)

On-device systems care about more than FM seconds. Replace simple tier allocation with a BudgetCoordinator that manages multiple resource budgets:

```swift
struct BudgetCoordinator {
    var fmBudgetSeconds: Double = 300    // FM compute cap
    var dspBudgetSeconds: Double = 60    // Fine-boundary DSP cap
    var thermalHeadroom: ThermalState    // from ProcessInfo.thermalState
    var batteryLevel: Double             // current battery
    var playbackImminence: Double        // how soon will user reach this region

    func shouldAllocate(window: FMWindowPriority) -> Bool {
        // EVI must justify compute cost given current device state
        let adjustedCost = window.estimatedCost * thermalMultiplier * batteryMultiplier
        return window.eviScore / adjustedCost > minimumEVIThreshold
    }
}
```

**Tier allocation (fallback when EVI history is sparse):**

| Tier | Budget share | Target |
|------|-------------|--------|
| Critical (priorities 1-3) | 50% (150s) | Regions where FM changes the decision |
| Important (priorities 4-7) | 35% (105s) | Regions where FM improves quality |
| Audit (sample) | 15% (45s) | Random sample for drift detection |

Within each tier, windows are ordered by eviScore descending. If a tier exhausts its budget, remaining windows fall back to non-FM evidence only.

**Shared feature caches:** CoveragePlanner, FineBoundaryRefiner, and FingerprintMatcher share local audio feature caches to avoid duplicate computation (RMS, spectral flux, music probability for overlapping windows).

#### 9.4 Integration with CoveragePlanner

**Changes to CoveragePlanner:**
- Add `.prioritizedWindows` scheduling phase that replaces the fixed phase ordering for targetedWithAudit policy
- Priority computation runs after hot-path and initial backfill evidence is available (so we know which regions are near thresholds)
- BudgetCoordinator gates allocation based on device state (thermal, battery)
- Cold-start policy (first 5 episodes) unchanged — full coverage is appropriate when we have no signal at all
- `lowTranscriptReliability` is an explicit priority reason (Section 0.4) — spend FM budget where ASR was unreliable

#### 9.5 Test Plan

- Unit tests for priority score computation: each priority reason, boundary conditions
- Unit tests for budget allocation: tier splits, budget exhaustion, fallback behavior
- Integration tests: compare FM spend allocation vs current uniform scanning on corpus
- Metrics: FM seconds per confirmed skip (should decrease), missed-ad rate on deprioritized regions (should not increase)

---

## 10. Metrics & Measurement

### Primary Metrics (from expert review)

| Metric | What it measures | Target direction |
|--------|-----------------|-----------------|
| **Episode-1 recall on unseen shows** | Cold-start gap (Improvements 1, 4) | Up |
| **Clipped editorial ms per playback hour** | Boundary accuracy — start too early, end too late (Improvement 2) | Down |
| **Leaked ad ms per playback hour** | Boundary accuracy — start too late, end too early (Improvement 2) | Down |
| **Repeat FP rate after one veto** | Correction effectiveness (Improvements 3, 6) | Down toward 0 |
| **Repeat FN rate after one "missed ad"** | Correction effectiveness (Improvements 3, 6) | Down toward 0 |
| **Repeated-ad recall under ASR variation** | Fingerprint robustness | Monitor |
| **FM seconds per confirmed skip** | FM budget efficiency (Improvement 7) | Down |
| **Manual unskip / seek-back rate** | Overall user satisfaction | Down |

### Calibration & Tail Metrics (added in v2)

| Metric | What it measures | Why it matters |
|--------|-----------------|---------------|
| **Calibration error (Brier-like) per source** | Is each source's confidence well-calibrated? | Catches overconfident sources before they cause harm |
| **Calibration error on final SpanDisposition** | Is the overall system well-calibrated? | Aggregates all trust/fusion interactions |
| **p95/p99 clipped editorial ms** | Worst-case boundary errors, not just average | Averages hide the skips that destroy trust |
| **p95/p99 leaked ad ms** | Worst-case ad leakage | Same — tails matter more than means |
| **Battery / CPU per playback hour** | On-device resource cost | FM, DSP, and fingerprint work compete for battery |
| **Time-to-unskip** | How quickly users undo bad skips | Fast unskip = the error was obvious and jarring |
| **Trust-update churn** | How much source trust values move per episode | High churn = unstable trust, possible self-reinforcement |
| **Counterfactual regret on frozen traces** | "Would the new system have done better on this historical episode?" | Safely evaluate changes before activation |
| **Per-skip explanation bundles** | What evidence contributed to each skip decision | QA tool for rollback decisions and debugging |
| **Uncertain boundary rate + mean width** | How often and how wide boundary uncertainty is | Tracks fine-stage effectiveness |
| **Shadow/live disagreement rate** | How often shadow-mode system disagrees with live | Pre-activation safety gate |
| **Per-feature rollback rate** | How often each feature flag gets rolled back | Operational health |
| **Derived-state rebuild time** | How long to rebuild trust/priors/traits from canonical data | Migration safety |
| **Score-distribution shift per feature flag** | Did activating a feature move the score distribution? | Threshold drift detection |
| **Peak memory during backfill** | Memory footprint of the full backfill pipeline | On-device resource constraint |

### Measurement Infrastructure

- **Replay harness:** Already exists (PlayheadIntegrationTests). Extend with signed boundary error tracking, per-show/per-network breakdowns, and tail percentile reporting.
- **Correction telemetry:** Add CorrectionAttribution logging (on-device only) for causal source tracking.
- **FM budget tracking:** Add per-tier spend reporting to BudgetCoordinator.
- **Explanation traces:** Every skip decision should produce a compact explanation bundle (evidence sources, weights, trust values, action rationale) stored on-device for QA and debugging. Not user-facing by default.
- **Counterfactual evaluator:** Replay frozen traces through the new system in shadow mode and compare decisions. This is the primary safety gate before activating any behavioral change.

---

## 11. Rollout Order & Dependencies

```
Improvement 1: Metadata Prewarm
├── 1a: Episode model changes (RSS metadata persistence)
├── 1b: Metadata cue extraction + show-owned domain registry
├── 1c: Metadata trust scoring
├── 1d: Lexical scanner integration
├── 1e: Classifier prior integration
├── 1f: FM prioritization integration
├── 1g: Chapter-marker integration
└── 1h: Recent feed history parsing + RecentFeedSponsorAtlas

Improvement 2: Two-Stage Boundary Refinement
├── 2a: Bracket template detection (coarse stage)
├── 2b: Per-show musicBracketTrust
├── 2c: Fine boundary refiner (local 150ms-hop search)
├── 2d: Boundary error asymmetry (signed penalty)
├── 2e: Dynamic snap radius
└── 2f: TimeBoundaryResolver integration

Improvement 3: Exact Local Corrections (Layer A)
├── 3a: CorrectionAttribution schema + causal inference
├── 3b: Synthetic anchor for false-negative corrections
├── 3c: Boundary correction with signed offset priors
├── 3d: Causal source demotion
├── 3e: Classification vs skip-policy override enum
└── 3f: UI surface (boundary correction prompt)

Improvement 4: Hierarchical Priors
├── 4a: Network identity extraction
├── 4b: Network priors aggregation
├── 4c: Archetype classification
└── 4d: Prior hierarchy integration

Improvement 5: Reliability-Aware Fusion
├── 5a: Source trust multiplier
├── 5b: Richer FM positive semantics (classTrust)
├── 5c: Bounded negative FM evidence
└── 5d: Adaptive fusion weight config

Improvement 6: Broader Learned Corrections (Layer B)
├── 6a: Rule promotion requirements (support + diversity)
├── 6b: Skip-policy override integration
└── 6c: Decay and expiration semantics

Improvement 7: FM Budget Scheduler
├── 7a: Decision-changing potential score
├── 7b: Priority ordering + tier allocation
└── 7c: CoveragePlanner integration
```

### Dependency Graph

```
1a ──► 1b ──► 1c ──► 1d ──► 1e ──► 1f
                │                    │
                └──► 1g              │
                └──► 1h              │
                                     ▼
              2a ──► 2b ──► 2c ──► 2d ──► 2e ──► 2f
                                     │
3a ──► 3b                            │
3a ──► 3c                            │
3a ──► 3d ◄──────────────────────────┘ (causal source demotion uses bracket/metadata trust)
3a ──► 3e
3e ──► 3f

4a ──► 4b
4c (independent, needs 2-3 episodes of data)
4b + 4c ──► 4d

1c + 2b + 3d ──► 5a (source trust uses metadata trust, bracket trust, correction demotion)
5a ──► 5b ──► 5c ──► 5d

3a + 3e ──► 6a ──► 6b ──► 6c

1f + 3a + 5d ──► 7a ──► 7b ──► 7c
```

### Recommended Ship Order (Instrument → Shadow → Activate)

The original plan bundled metadata, boundaries, and corrections into one activation phase. Reviews v2 and v3 both identified that this makes it impossible to attribute trust/unskip changes to a specific subsystem. The revised rollout separates instrumentation from behavioral changes, requires shadow-mode validation before activation, and ensures derived state is rebuildable from canonical sources.

**State governance principle (v3):** All higher-level stores (trust, priors, traits, fingerprints) are derived state and must be rebuildable from canonical episode analyses and user feedback events. Canonical sources: raw episode audio features, transcript atoms, evidence catalog entries, user correction events. This makes migrations safe and debugging tractable.

**Episode metadata storage (v3):** Persist normalized/truncated text plus source hashes, not unbounded HTML blobs. Cues can be rebuilt from normalized text without storing raw `content:encoded`.

| Phase | Contents | Behavioral change? |
|-------|---------|-------------------|
| **Phase 0: Foundations** | SponsorEntityGraph + OwnershipGraph (0.3), TranscriptReliability (0.4), explanation traces, replay holdout infrastructure, counterfactual evaluator, ScoreCalibrationProfile scaffolding, feature flags for each subsequent phase | **None.** Pure instrumentation, canonicalization, and schema. No detection or skip behavior changes. |
| **Phase A: Shadow Ingestion** | Metadata cue extraction (1a-1f), chapter parsing (1g-1h), ownership classification, metadata extraction pipeline (HTML stripping, URL normalization, casefolding, canonical entity resolution) — all in **shadow mode**. Cues are extracted, logged, and evaluated against audio outcomes but do NOT influence scoring or skip decisions. | **None.** Shadow-only. Builds MetadataReliabilityMatrix, measures cold-start recall potential. |
| **Phase B: Corrections + Boundaries** | Exact local corrections (3a-3f) activated immediately (low risk, high trust). Implicit feedback signals (5.5) begin collecting. Bracket detection (2a-2b) and fine boundary refiner (2c-2f) in **shadow mode** — compare boundary MAE against current baseline on replay. | **Corrections: yes. Boundaries: shadow first, then activate after MAE improves.** |
| **Phase C: Calibrated Fusion + Four-Stage Split** | Four-stage pipeline (0.1), deterministic finalizer (0.2), calibrated fusion (7.1-7.4) with ScoreCalibrationProfile v1, targeted FM suppression (7.3). Activated after counterfactual regret on frozen traces is acceptable. Thresholds revalidated with calibration profile. | **Yes, but gated on counterfactual evaluation and calibration validation.** |
| **Phase D: Priors + Scheduling + Fingerprints** | ShowTraitProfile (4c/6.3), network priors (4a-4b), EVI budget scheduler (7a-7c), composite fingerprint upgrade (7.5). These consume data accumulated in Phases A-C. | **Ranking/scheduling changes, not direct skip changes.** |
| **Phase E: Broader Corrections + Policy** | Learned correction scopes (6a-6c), skip-policy overrides, gray-band markOnly UX. Requires sufficient correction volume and implicit feedback from Phases B-D. | **Yes, after support volume validates safety.** |

**Why this ordering matters:**
- Phase 0 changes nothing — it builds the graph, trace, and calibration infrastructure everything else needs
- Phase A validates metadata value in shadow before it touches decisions
- Phase B activates the lowest-risk behavioral change (exact corrections) immediately while validating boundaries in shadow
- Phase C is the biggest architectural change (four-stage pipeline) and gets the most rigorous counterfactual evaluation. Calibration profile must be validated before thresholds are trusted.
- Phase D only makes sense after Phases A-C have accumulated enough data for traits and priors
- Phase E requires real correction volume to build support/diversity for broader rules
- **Each phase has an independent feature flag and can be rolled back without affecting other phases**

---

## 12. Risk Register

| Risk | Severity | Mitigation |
|------|----------|------------|
| Metadata false positives from stale show notes | Medium | Corroboration requirement + Bayesian trust starting at Beta(1,9) |
| Music bracket false detection on editorial music | Medium | Template classification + anchor co-occurrence + per-show Bayesian trust |
| Correction overgeneralization (Layer B) | High | Multi-episode/multi-date diversity requirement + decay + Phase E gating |
| Network identity misattribution | Low | Confidence scoring on network derivation + show-local overrides |
| FM negative evidence suppressing real ads | Medium | Action cap (markOnly), not score subtraction. Direct scoring disabled by default. |
| Boundary fine stage introducing latency | Low | Only runs on candidate edges (handful of 6s windows), not whole episode |
| Chapter data quality variation | Medium | Quality-scored soft constraints, not hard negatives. Crossing penalty + markOnly cap instead of veto. |
| RSS parsing surface (security) | Medium | Input sanitization on all RSS fields before regex extraction |
| Self-reinforcing trust updates | High | Orthogonal corroboration rule (Section 0). Holdout-compatible traces. Trust-update churn metric. |
| Small-sample trust instability | Medium | Beta posterior with shrinkage toward hierarchical priors. Posterior confidence dampens low-observation shows. |
| Bundled activation hiding regression source | High | Instrument → shadow → activate rollout. Counterfactual evaluator before each activation. |
| Gray-band UX confusion | Medium | markOnly threshold and UI require explicit product approval. Conservative defaults. |
| Entity graph fragmentation | Low | SponsorEntityGraph with eTLD+1 normalization, alias canonicalization, and tracking parameter stripping |
| Battery/thermal impact from new DSP + FM work | Medium | BudgetCoordinator with thermal/battery gating. Shared feature caches. |

---

## Revision History

- **v1 (2026-04-14):** Initial plan from dueling-wizards + expert review
- **v2 (2026-04-14):** Integrated GPT Pro Extended Reasoning review: added classification/action split, deterministic finalizer, Bayesian trust with orthogonal updates, sponsor entity graph, quality-scored chapters, cue-conditional boundary priors, FM negative as action cap, EVI scheduling with energy awareness, gray-band markOnly UX, instrument→shadow→activate rollout, expanded metrics
- **v3 (2026-04-15):** Integrated second expert review: upgraded to four-stage pipeline (proposal/classification/boundary/policy) with typed ProposalAuthority, monotone calibration layer with versioned thresholds, targeted FM suppression (not action cap or score subtraction), cue-specific metadata reliability matrix, boundary uncertainty intervals with guard margins, continuous ShowTraitProfile replacing hard archetypes, implicit feedback signals as weak labels, state governance with rebuildable derived stores, more granular rollout phasing

*Next step: Round 3 review or convert to beads for implementation.*
