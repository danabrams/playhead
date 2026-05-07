# Chapter Signal for Ad Detection — Design

**Status**: Design approved 2026-05-06. Reconciled with existing code 2026-05-06. Ready for implementation planning.
**Author**: Dan + Claude (brainstorming session)

## Reconciliation note (2026-05-06)

After reading existing code in `Playhead/Services/AdDetection/`, the original design's new `Chapter` and `ChapterPlan` types are reframed to reuse existing types and integration points:

- **No new evidence type.** Existing `ChapterEvidence` (with `ChapterSource ∈ {id3, pc20, rssInline}` and `ChapterDisposition ∈ {adBreak, content, ambiguous}`) is the canonical chapter record. Add a new variant: `ChapterSource.inferred`.
- **No new fusion wiring.** `ChapterMetadataEvidenceBuilder` already projects `[ChapterEvidence]` → `metadataEntries` in `BackfillEvidenceFusion` (cap 0.15). Inferred chapters ride the same path automatically once they emit `ChapterEvidence` with `source = .inferred`.
- **`ChapterPlan` is repurposed** as the *cache artifact* — a content-hash-keyed envelope holding `[ChapterEvidence]` with `ChapterSource.inferred`, plus phase metadata (boundary confidence, plan confidence, generation timestamp). Not a new evidence shape.
- **Gate pattern**: mirror `FMBackfillMode` (`off / shadow / rescoreOnly / proposalOnly / full`), not `Q45fReplayGate`. Add `ChapterSignalMode` to `AdDetectionConfig`.
- **Creator-chapter precedence (Dan, 2026-05-06)**: when an episode already has any `ChapterEvidence` with source ∈ {id3, pc20, rssInline}, `ChapterGenerationPhase` exits early without invoking FM. Creator chapters are near-ground-truth; inferred chapters are statistical inference. Don't pay FM cost when creator labels are available.
- **Consumers**: `CoveragePlanner` (net-new chapter-awareness) and FM prompt builders (net-new context injection) read chapters from the unified path — `ChapterEvidence` array on the episode, regardless of source. They do NOT need to distinguish creator vs. inferred at consumption time, except where confidence weighting matters (creator chapters get full weight; inferred get the existing `metadataCap = 0.15` discount via fusion's existing weighting, plus per-chapter `qualityScore` for finer tuning).

## Problem

Playhead's ad-detection pipeline misses ads in two known ways:

1. **Coverage gaps on conversational shows.** The mature-show coverage policy scans only ~85% of audio, picking ~12% random audit windows. Mid-episode ads on shows like Conan can fall in unscanned regions (memory: `project_ad_detection_weak_on_conan.md`, 2026-04-23 — confirmed show-agnostic).
2. **Per-window classification without macro context.** FoundationModels classifies 2–3 transcript segments at a time, never seeing where in the episode the segment falls or what surrounds it.

A competitor (Superphonic) generates user-facing chapters from transcripts, with chapter labels detecting ads. We're not building user-facing chapters — that contradicts Playhead's "peace of mind, not metrics" positioning. But chapter-level reasoning, used **internally**, could plausibly close both gaps above.

## Goal

A per-episode `ChapterPlan` artifact, generated on-device during backfill, that:

- Directs the `CoveragePlanner`'s attention toward ad-probability-bearing chapter regions (instead of purely random audit windows).
- Adds compact macro-structure context to per-window FM classification prompts.

**Success bar** (Dan, brainstorming Q2-C): measurable eval-suite lift on the dogfood corpus AND at least one specific known-miss case-study fix.

## Non-goals

- Not a user-facing chapter UI. Internal signal only.
- Not first-listen support. Backfill-only is fine (Q4-B); the win is on re-listens, paused/resumed episodes, and pre-downloaded episodes.
- Not a replacement for the existing 3-layer pipeline (Lexical → Sequence Classifier → FM). The chapter signal is purely additive; missing-plan fallback === today's behavior.

## Approach

**Approach B (heuristic skeleton + FM labeling)** chosen over pure-FM chaptering and FM-only-on-uncertain-regions:

- Boundary candidates come from existing acoustic + lexical features (already computed).
- FoundationModels labels each candidate region (one FM call per chapter, ~80 tokens of content per call).
- ~4–12 FM calls per typical 60-min episode — comparable to current refinement-pass cost.

This fits Playhead's existing pipeline shape: FM does work it's good at (label this region) instead of work it's bad at (stitch 25+ windows together coherently across token-budget seams).

## Architecture

A new backfill phase, **`ChapterGenerationPhase`**, runs after final-pass transcript completes. It produces a single artifact, **`ChapterPlan`**, cached per audio content hash.

Two existing components retrofitted to consume the plan:

1. **`CoveragePlanner`** — replaces a fraction (start at 50%) of random audit windows with chapter-informed selection. Prioritizes chapters with ad-probability above audit threshold; deprioritizes high-confidence `content`/`intro`/`outro` chapters.
2. **`FoundationModelClassifier` / `FoundationModelExtractor`** — prompt builder includes a compact chapter-context blob (~30–50 tokens) in per-window prompts.

Phase placement: between final-pass transcript completion and `BackfillEvidenceFusion`. Fusion incorporates chapter ad-probability as a corroborating evidence source over already-detected candidates.

**Cost envelope**: 4–12 FM calls per episode at ~80 content tokens each. Fits inside existing charging+idle backfill budget.

**Failure mode**: any failure → no `ChapterPlan` written → consumers fall back to baseline. Purely additive feature.

## Components

### New (6)

1. **`Chapter`** *(data model)* — `{ startTime, endTime, type: ChapterType, adProbability, confidence, topicLabel?, sourceFingerprint }`. `topicLabel` optional so failures don't break consumers; `sourceFingerprint` hashes inputs for cache invalidation.
2. **`ChapterType`** *(enum)* — `intro | content | hostReadAd | programmaticAdBreak | outro | recap | unclear`. Closed taxonomy keeps FM output stable.
3. **`ChapterPlan`** *(data model)* — `{ episodeContentHash, chapters: [Chapter], schemaVersion, generatedAt, planConfidence }`. `planConfidence` is duration-weighted: `sum(c.confidence × c.duration) / total_duration`.
4. **`ChapterBoundaryDetector`** *(new service)* — pure heuristic boundary generator over existing music probability, RMS, spectral flux, speaker cluster IDs, lexical hits, pause features. Output: ordered `[ChapterCandidate]` with per-boundary confidence. **No FM use; no new acoustic computation.**
5. **`ChapterLabelingService`** *(new service)* — FM-based labeler. One `SystemLanguageModel` call per candidate region with `@Generable` schema. Lightweight context: previous chapter type + episode position.
6. **`ChapterGenerationPhase`** *(new orchestrator)* — backfill phase wiring detector → labeler (parallelized within FM concurrency budget) → assembly → cache write.

### Modified (4)

7. **`BackfillEvidenceFusion`** — chapter ad-probability becomes new evidence input over candidate ranges.
8. **`CoveragePlanner`** — consults plan for audit-window selection.
9. **`FoundationModelClassifier` + `FoundationModelExtractor`** — prompt builder reads plan, includes compact chapter context.
10. **`AdDetectionService`** — wires phase into backfill pipeline; gated behind feature flag.

### Storage

`ChapterPlan` lives in same content-hash-keyed cache as existing FM artifacts (`AnalysisAsset`-adjacent), with `schemaVersion` for invalidation.

## Data flow

**Generation** *(once per episode, post-final-pass)*:

1. Final-pass transcript completes for episode `E` (content hash `H`).
2. `AdDetectionService` checks `chapterSignal.enabled` flag. If on, dispatches `ChapterGenerationPhase(H)` to backfill queue.
3. Phase pulls existing transcript chunks + features from cache. *No new computation.*
4. `ChapterBoundaryDetector.detect(features) → [ChapterCandidate]` (typically 4–12 per 60-min).
5. For each candidate (parallelized within FM concurrency budget): `ChapterLabelingService.label(candidate, prevType, position) → Chapter`. One FM call per candidate.
6. Labels assembled into `ChapterPlan`, persisted to cache. Phase emits `ChapterPlanReady(H)`.

**Fusion**:

7. `BackfillEvidenceFusion` listens for `ChapterPlanReady`. Re-fuses candidate ad windows for `H` using chapter ad-probability as new evidence input. AdWindow scores updated; promotions/demotions/merges possible.

**Consumers** *(future calls for episode E read the plan)*:

8. **Coverage planner**: next `CoveragePlanner.plan(H)` consults cached plan; replaces ~50% of random audit slots with chapter-informed selections.
9. **FM classifier/extractor**: next per-window FM call for `H` includes ~30–50 tokens of chapter context: `"Chapter 4/7: hostReadAd. Prev: content. Topic: <topicLabel>."`.

**Shadow mode toggle** (mirrors recent `Q45fReplayGate` pattern):

- `off` — no plan generated.
- `shadowOnly` — plan generated and cached, consumers ignore it. Used for plan-quality eval without affecting detection.
- `enabled` — plan generated AND consumed.

**Cache invalidation**: `schemaVersion` bump, content hash change (impossible by construction — different bytes = different hash = no plan found), or explicit user rescan.

## Error handling

**Guiding principle**: chapter signal is purely additive. Any failure → baseline pipeline behavior.

### Phase-level failures

- **FM unavailable** (thermal, model not downloaded, region/hardware unsupported): `ChapterGenerationPhase` checks `DeviceAdmissionPolicy` before starting; phase skips silently if denied.
- **Backfill budget exceeded / preempted**: phase respects existing cancellation tokens; partial state discarded.
- **Boundary detector zero candidates**: short / monologue / pure-music episode. Phase exits cleanly without plan. Diagnostic: `chapter_phase_no_candidates`.
- **Pathological boundary rate** (>1 candidate per 90 seconds avg across episode): treat as detector glitch (likely flickering feature input). Phase aborts. Diagnostic: `chapter_phase_pathological_rate`.

### Per-chapter failures

- **Single FM call failure** (timeout, rate-limit, schema validation error): retry once. If still failing, mark chapter `unclear` with `failureMode: operational` flag.
- **Out-of-taxonomy or invalid output**: clamp/coerce to `unclear` with `failureMode: schema`.
- **Operational unclear rate >30% across plan**: treat as system-distrust signal — drop plan entirely, regenerate later. (Semantic unclears are *information* — keep the plan; downstream weights by per-chapter confidence.)

### Density management

- **Cap-and-merge** instead of abort on over-recall: keep top-N by `boundaryConfidence`, target `min(detected, max(8, ceil(episode_minutes / 5)))` (~1 chapter per 5 minutes, floor 8). Lower-confidence boundaries dropped; flanking chapters merge.
- **Floor of 8** ensures we always *try* to label something even on under-recall (monologue) episodes.

### Plan-level confidence

`planConfidence` = `sum(chapter.confidence × chapter.duration) / total_duration`. Duration-weighted: a plan with one big confident chapter and three tiny unclear ones is fine; the inverse is suspect. Consumers weight signal by `planConfidence` rather than gating on a binary threshold.

### Consumer-side fallbacks

- **Plan missing**: every consumer treats missing-plan as default — same code path as today.
- **Schema version mismatch**: treat as missing; regenerate on next backfill window.
- **Decode failure / cache corruption**: treat as missing, log diagnostic, regenerate.

### Lifecycle edges

- **Final-pass transcript revises mid-generation**: phase captures transcript snapshot hash on entry; on exit, write only if hash still matches. Otherwise discard; new generation triggered by new transcript-completion event.
- **Feature flag flipped mid-run**: phase reads flag once on entry; in-progress phase continues. Plan is written or not; consumer behavior switches on next read. Acceptable race.

### Diagnostics

Every phase outcome (success, no-candidates, pathological-rate, FM-unavailable, preempted, low-confidence, decode-failure) emits structured event into existing diagnostics JSON, keyed by hashed episode ID. Same pattern as `scheduler_events`.

## Testing

### Unit tests *(`PlayheadFastTests`, hermetic)*

- `ChapterBoundaryDetectorTests` — synthetic features. Music spike → boundary; speaker shift → boundary; lexical jump → boundary; 1Hz speaker flicker → no boundary (rate gate); zero candidates on monologue; cap-and-merge on dense input.
- `ChapterLabelingServiceTests` — fixture transcripts with `MockFoundationModel`. Schema coercion → `unclear`; retry-once on simulated timeout; operational vs. semantic flag.
- `ChapterPlanTests` — serialization round-trip; schema-version invalidation; duration-weighted confidence math.
- `ChapterPlanCacheTests` — content-hash keying, decode-failure handling, eviction.

### Integration tests *(`PlayheadFastTests`)*

- `ChapterGenerationPhaseIntegrationTests` — end-to-end against fixture episode with pre-computed feature snapshots and stubbed FM. Plan written, event emitted, schema valid.
- `BackfillEvidenceFusionWithChapterPlanTests` — fusion behavior with/without plan. Scores monotonic in chapter ad-probability; missing-plan path matches today byte-for-byte.
- `CoveragePlannerWithChapterPlanTests` — audit-window selection diff with plan present.
- `FoundationModelPromptBuilderTests` — prompt length within token budget when chapter context appended; missing-plan fallback verbatim today's prompt.

### Eval harness *(extends `narl-eval`)*

- **`ChapterPlanGate`** — three-mode gate matching `Q45fReplayGate` pattern (commit `9a44dc03`): `off` / `shadowOnly` / `enabled`.
- **Aggregate metrics** on dogfood corpus: precision, recall, F1, FM call count, plan-generation latency. Bar to keep: measurable lift on at least one of {recall, precision} without regression on the other; FM cost increase ≤2× current refinement-pass cost.
- **Case-study fixture set** — 5–10 known-miss episodes from existing dogfood diagnostics JSONs. With chapter signal enabled, previously-missed ads caught (or, if not caught, *why not* in diagnostics).
- **Plan-quality eval** (shadow mode) — chapter-quality score against hand-labeled fixtures, independent of detection lift. Tunes detector + labeler separately.

### Deliberately not tested

- FM output exact wording (flaky, not behaviorally meaningful).
- Cross-device chapter consistency (no sync/sharing).
- Real-time latency on hot path (phase runs in backfill).

## Open questions for implementation planning

- Tuning of `0.5` audit-window-replacement fraction in `CoveragePlanner` — likely a config var with shadow-mode A/B.
- Concurrency budget for parallel FM labeling within phase — reuse existing FM concurrency limiter or new gate?
- `topicLabel` length cap and its prompt-budget cost in downstream consumers — start at ~20 tokens, revisit after eval.
- Whether to expose `planConfidence` to dogfood diagnostics for offline tuning.

## References

- Pipeline architecture: `Playhead/Services/AdDetection/AdDetectionService.swift`
- Coverage planner: `Playhead/Services/AdDetection/CoveragePlanner.swift`
- FM extractor pattern: `Playhead/Services/AdDetection/FoundationModelExtractor.swift`
- Counterfactual gate precedent: commit `9a44dc03` (`Q45fReplayGate` in `narl-eval`)
- Boundary singleton precedent: commit `706fc636` (`AdDetectionService` lines 355–359)
- Conan coverage finding: memory `project_ad_detection_weak_on_conan.md` (2026-04-23)
- xctestplan filtering rules: memory `project_xctestplan_swift_testing_limitation.md`
