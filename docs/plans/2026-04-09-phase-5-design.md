# Phase 5 Design — Timeline Projection + Minimal Decoder + Transcript Overlay

**Date:** 2026-04-09
**Status:** Design approved; ready for implementation plan
**Bead epic:** [`playhead-4my.5`](../../) — Phase 5: Timeline Projection + Minimal Decoder
**Depends on:** Phase 4 (Region Proposals + Feature Extraction) — already shipped via `bd-1my` outward expansion + `bd-1en` permissive refinement, validated to 4/4 recall on the Conan smoke test on real iPhone hardware.

## Context

The original Phase 5 plan (2026-04-05) framed `MinimalContiguousSpanDecoder` as a hysteresis-based decoder with enter/exit score thresholds, evidence-score accumulation, and per-feature evidence vectors per atom. Brainstorm on 2026-04-09 surfaced two constraints that invalidated that direction:

1. **Zero-shot constraint**: Phase 5 must work on podcasts the user has never seen, with no per-show calibration, no labeled training data, no fitted distributions. Anything that depends on score thresholds, percentile cutoffs, or fitted parameters is forbidden.
2. **Precision-first constraint**: false positives are worse than false negatives. Better to miss an ad and let the user listen through it than to skip past content the user wants to hear.

The brainstorm also surfaced two facts that already exist on `main` and reshape Phase 5's scope:

- The FM-driven detection pipeline (`FoundationModelClassifier` + `bd-1my` outward expansion + `bd-1en` permissive refinement path) **already produces full-coverage ad spans on real device**. Conan smoke test recall is 4/4. Boundary detection is solved at the FM layer.
- `TargetedWindowNarrower` (cycle 8 merge) already narrows FM input to per-anchor windows. The "Phase 5 targeted-window narrowing" work referenced in commit `7a472d1` is this — an FM efficiency optimization, not the full Phase 5 design.

Phase 5's actual remaining job is therefore **finalization, not detection**:

- Project Phase 4 region anchors and user corrections onto the per-atom timeline
- Decode contiguous spans from anchor presence + correction masks + universal duration constraints
- Persist decoded spans to `AnalysisStore`
- Surface them in the transcript view as visual highlights so users can see and (eventually) correct them — even when skipping is disabled

## Design

### Architecture

```
Phase 4 RegionFeatureBundles ──┐
EvidenceEntry list ────────────┼──► AtomEvidenceProjector ──► [AtomEvidence]
CorrectionMaskProvider ────────┘                                    │
                                                                    ▼
                                                       MinimalContiguousSpanDecoder
                                                                    │
                                                                    ▼
                                                              [DecodedSpan]
                                                                    │
                                  ┌─────────────────────────────────┼──────────────┐
                                  ▼                                 ▼              ▼
                            AnalysisStore                  TranscriptAdOverlayView  Phase 6 fusion
                            persistence                        (5.4 UI)              (future)
```

Four units of work, each in its own sub-bead:

1. **`AtomEvidenceProjector`** (5.1) — produces a per-atom annotation array
2. **`MinimalContiguousSpanDecoder`** (5.2) — produces a list of `DecodedSpan`s
3. **Materialization** — persists decoded spans to `AnalysisStore` (lives inside 5.2 for now)
4. **`TranscriptAdOverlayView`** (5.4) — renders decoded spans in the transcript UI

### Anchor strategy (Q2 from brainstorm)

An atom is **anchored** if and only if it is covered by at least one of:

- A Phase 4 region with `.foundationModel` origin AND `fmConsensusStrength >= .medium` (i.e. `fmConsensusStrength.value >= 0.5` — FM agreed across at least 2 windows; single-window FM is excluded). `FMConsensusStrength` is an enum with raw doubles `none=0.0, low=0.35, medium=0.7, high=1.0` — there is no 0.5 case, so the comparison must use the enum form or the `.value` property. See `Playhead/Services/AdDetection/RegionProposalBuilder.swift:13-24`.
- An `EvidenceEntry` (from `EvidenceCatalog`, built by `EvidenceCatalogBuilder`) of trustworthy type (URL, promo code, explicit disclosure phrase, CTA phrase). See `Playhead/Services/AdDetection/EvidenceCatalogBuilder.swift:26`.

That is the entire anchor source list. Two paths. Both come pre-validated by upstream phases. Phase 5 does not run its own anchor detection logic; it consumes anchors that already passed Phase 4's filtering.

What does NOT anchor:

- `.lexical` regex hits (too noisy on their own)
- `.acoustic` breaks (RMS dips happen for many reasons; note also that as of Stage C `AcousticBreakDetector` output is detected but not surfaced as a region origin by `RegionProposalBuilder` — see Risks and Stage C sections below)
- Single-window FM hits (untrustworthy without consensus)
- `.sponsor` origin (the SponsorKnowledgeStore is empty cold-start)
- `.fingerprint` origin (the AdCopyFingerprintStore is empty cold-start)

These can still produce regions in `AnalysisStore` for diagnostics and Phase 6 reasoning, but Phase 5 will not promote them to user-visible ad spans.

### Per-atom data structure (Q3 from brainstorm)

```swift
struct AtomEvidence: Sendable {
    let atomOrdinal: Int
    let isAnchored: Bool
    let anchorProvenance: [AnchorRef]
    let correctionMask: CorrectionState
}

enum AnchorRef: Sendable, Equatable {
    case fmConsensus(regionId: String, consensusStrength: Double)
    case evidenceCatalog(entry: EvidenceEntry)
}

enum CorrectionState: Sendable {
    case none
    case userVetoed       // user said "this isn't an ad"
    case userConfirmed    // user said "yes this is an ad"
}
```

Three fields per atom. No score. No per-feature vector. No normalization. No calibration math.

`anchorProvenance` is preserved through to the UI layer for the tap-to-explain popover (5.4) — when a user taps a highlighted span, we show "Detected from FM consensus + 2 EvidenceEntries: cvs.com URL, promo code CONAN10".

**Cross-bead handoff note (cycle 1 review).** Reviewers flagged that `AtomEvidence` may also need `startTime`/`endTime` fields so downstream consumers (decoder, materialization, overlay UI) do not have to re-derive per-atom timing from the transcript. Status: **open** — resolve during 5.1 implementation. If the projector can cheaply carry timing (it already walks the transcript), add them; otherwise leave atom ordinals as the canonical key and derive time at the decoder boundary.

### Correction mask integration

`AtomEvidenceProjector` consumes a `CorrectionMaskProvider` protocol. Phase 5 ships with a `NoCorrectionMaskProvider` stub that returns `.none` for every atom. Phase 7 (User Corrections) will conform a real `UserCorrectionStore` to this protocol and inject it through the runtime. This avoids a circular dependency between Phases 5 and 7.

The `correctionMask` field on each `AtomEvidence` is the load-bearing innovation that lets user corrections persist across transcript reprocessing — atom ordinals are stable, time intervals are not.

### Decoder algorithm (Q1 from brainstorm)

`MinimalContiguousSpanDecoder` is a linear walk over the `[AtomEvidence]` array. **Rule order is pinned: form runs → merge → split → drop.** The algorithm must be idempotent under composition — `decode(decode(x)) == decode(x)` — so reprocessing a transcript after a backfill does not drift.

```
1. FORM RUNS: Walk atoms left to right. A contiguous run of atoms where
   (isAnchored && correctionMask != .userVetoed) is a candidate span.

2. MERGE: Two adjacent candidate spans merge if separated by fewer than
   MERGE_GAP_ATOMS unanchored atoms AND no .userVetoed atom sits between
   them. Atom-count (not seconds) for transcript-density invariance.

3. SPLIT: Any span above MAX_DURATION_SECONDS (180s) splits at the longest
   internal gap of unanchored atoms. Fallbacks:
     - 100%-anchored span above MAX → split at midpoint ordinal
     - Tied longest gaps → argmin(ordinal) (leftmost tie wins)
     - Recurse until all resulting spans ≤ MAX

4. DROP: Any span below MIN_DURATION_SECONDS (5s — universal industry
   constant) is dropped.

5. Output: [DecodedSpan] with anchorProvenance carried through.
```

Three universal constants. No thresholds on score values. No calibration. Same behavior on every podcast.

**`MERGE_GAP_ATOMS` is pinned at `3`** from first principles: three atoms is roughly two seconds of speech, which fits the "tiny inter-phrase gap" intent while staying far below the 5-second minimum-span floor. This value ships as a constant and is not tuned against the Conan fixture.

```swift
struct DecodedSpan: Sendable {
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let startTime: Double
    let endTime: Double
    let anchorProvenance: [AnchorRef]
}
```

The decoder refuses to emit any span without an upstream anchor. **No anchor → no span. Period.** This is the precision-first invariant.

### Materialization

Decoded spans persist to `AnalysisStore` as a new entity (or extension of `AdWindow` — to be decided in implementation plan based on the existing schema). Re-running the decoder on the same input is idempotent — no duplicate spans, deterministic ordering by atom ordinal.

A migration adds the new table or columns. Existing `AdWindow` rows continue to load.

### Transcript overlay UI (5.4)

`TranscriptAdOverlayView` reads decoded spans from `AnalysisStore` for the currently displayed episode and renders them in the transcript view:

- **Background tint** on the ad text
- **Left-edge accent bar** on lines covered by the span
- **Inline `AD` badge** at the start of the region

These three visual cues compose — every detected ad gets all three. Visualization is intentionally redundant so the affordance is unmistakable.

**Tap behavior**:

- Tap on a highlighted region → provenance popover (read-only) showing which evidence flagged it
- "This isn't an ad" gesture is **present and visible** in the popover but not wired
- The gesture calls a stub method on a `UserCorrectionStore` protocol with a no-op default implementation
- Phase 7 will conform a real store to the protocol and the gesture activates

The visual surface and the gesture stub ship in Phase 5. The actual correction persistence lands in Phase 7. This is a conscious decoupling — users get to SEE the system's predictions in v1 even though they cannot CORRECT them yet.

**Highlights render whether or not skipping is enabled.** The skip behavior is a separate user setting; the visual layer is the system's primary feedback to the user about what it has detected.

### Out of scope for Phase 5

- Score thresholds, percentile normalization, calibration of any kind
- Tier system for "weak vs strong anchors" — pre-validated upstream is enough
- FM invocation (handled by Phase 3-4)
- Anchor-detection logic (handled by Phase 4 + `EvidenceCatalogBuilder`)
- Acoustic feature aggregation
- Persisting user corrections (Phase 7)
- Sponsor-specific blocking from corrections (Phase 7)
- Cross-episode correction propagation (Phase 7)
- Multi-state HMM / Viterbi decoder (Phase 12)

## Tests

Six categories — see `playhead-4my.5.3` for the full list. Highlights:

- **Robustness rail** (the zero-shot guarantee): no-ad episode → zero spans, ad-heavy episode → split correctly, adversarial high-noise no-anchor episode → zero spans, identical inputs → identical outputs (determinism)
- **Round-trip integration** on the Conan fixture: feed Phase 4 region bundles + `EvidenceEntry` list to projector + decoder, assert decoded spans correspond to ground truth ads with measurable improvement over the Phase 4 baseline
- **Materialization round-trip**: persist + reload + idempotent re-run
- **UI snapshot**: rendered transcript with one fixture episode for visual regression

## Success metrics (from playhead-4my.5 acceptance criteria)

Already pinned on the epic from earlier in the brainstorm:

1. `Phase4ShadowBenchmarkTests` on the Conan fixture shows ad-second coverage moving from 15% (confirmed by Stage C against real audio features on 2026-04-09 — both synthetic and real produce identical 15%) toward the doc target of 80%+
2. Decoded spans are contiguous (no micro-fragments < 5s)
3. Duration constraints prevent implausible spans (no 30-minute "ads")
4. Correction masks prevent re-detection of user-reverted spans
5. Lenient region-recall does NOT regress below 75%
6. Zero new test flakes when run parallelized

## Risks

| Risk | Mitigation |
|---|---|
| Anchor count on real episodes might be very low if the EvidenceCatalogBuilder misses common patterns. | The Conan smoke test already shows 4/4 recall through FM consensus alone (without relying on EvidenceEntry as the primary path). FM consensus is the dominant anchor source. |
| Phase 5's anchor sources are FM consensus + `EvidenceEntry` only. If FM is unavailable or under-running on a given show, anchor count may be very low. `AcousticBreakDetector` output COULD supply backup anchors — Stage C confirmed it detects real breaks on real audio — but `RegionProposalBuilder` currently drops those breaks and does not surface them as `origins=acoustic` regions. | Phase 5 ships using FM + `EvidenceEntry` only. The acoustic backup activates automatically once the upstream Phase 4 fix lands (tracked as a separate bead filed in parallel — see Related work). No code changes in Phase 5 will be required when it does. |
| Stage C real-audio benchmark depends on a test fixture mp3 at `/tmp/conan.mp3` (sandbox-reachable) and is gated via a sentinel file at `/tmp/playhead_phase4_real_features`. Neither artifact is checked into the repo. A future reader running the benchmark without both will see the test skip silently. | Documented here in the design doc and in the Stage C section below. The test method is `phase4OnRealFeatures()` in `Phase4ShadowBenchmarkTests.swift`; it will refuse to run without the sentinel + mp3 present. |
| Correction masks integration with Phase 7 may require changes to `CorrectionMaskProvider` protocol. | Keep the protocol minimal in v1 (single method: `correctionMask(for atomOrdinal: Int) -> CorrectionState`). Extending it is non-breaking. |
| Visual overlay design might interfere with transcript readability. | Snapshot tests + manual review on multiple episodes before the v1 design is locked. Background tint + edge bar + badge are independently togglable in early iterations if needed. |

## Implementation order

1. `AtomEvidence` struct + `AnchorRef` enum + `CorrectionState` enum + `CorrectionMaskProvider` protocol with `NoCorrectionMaskProvider` stub
2. `AtomEvidenceProjector.project(...)` with unit tests (5.1)
3. `MinimalContiguousSpanDecoder.decode(...)` with unit tests (5.2)
4. `AnalysisStore` materialization + migration + idempotency tests (5.2)
5. Phase 4 → Phase 5 round-trip integration test against Conan fixture (5.3)
6. Robustness rail tests (5.3)
7. `UserCorrectionStore` protocol with no-op default (5.4 prep)
8. `TranscriptAdOverlayView` rendering layer (5.4)
9. `AdRegionPopover` provenance display + stub gesture (5.4)
10. UI snapshot test (5.4)

Estimated total: ~430 lines of production code + ~600 lines of tests.

## Open questions deferred to implementation

- Whether `DecodedSpan` is a new SQLite table or extends `AdWindow`
- Whether the popover gesture is a long-press, swipe, or button tap (left to UI iteration)
- Whether the background tint color is defined as a new design token or reuses an existing one

## Stage C real-audio findings (2026-04-09)

Stage C wired a real-audio variant of the Phase 4 shadow benchmark and ran it against the Conan fixture. Key findings:

- **Real features wired and running.** `Phase4ShadowBenchmarkTests.phase4OnRealFeatures()` decodes `/tmp/conan.mp3` via `AVAudioFile` (915s 16kHz mono → 457 `FeatureWindow`s) and feeds them to the same Phase 4 shadow pipeline used by the synthetic-feature benchmark. The test is gated on a sentinel file at `/tmp/playhead_phase4_real_features` plus the presence of `/tmp/conan.mp3`; without both it skips.
- **Features are non-degenerate.** Mean RMS 0.1235, mean spectral flux 1.6862, mean pause probability 0.0342. These are real audio statistics, not zeros or constants.
- **Real and synthetic produce identical Phase 4 output.** Same 75% recall / 66% precision / 15% ad-second coverage. Same 3 lexical-only regions. Same atoms. Same orphan false positive at [3:05–3:25]. The 15% baseline is therefore **not** an artifact of synthetic features — it is the true capability of the current Phase 4 wiring on this fixture.
- **Acoustic breaks are detected but not surfaced.** `AcousticBreakDetector` DID find 3 real breaks on the real audio (0 energy-based, 2 spectral, 1 pause-cluster). But `RegionProposalBuilder` does NOT promote any of them to regions with `origins=acoustic`. All 3 resulting regions are `origins=lex` only. This is the upstream bug that Phase 5's acoustic-backup-anchor path depends on; it is being filed as a separate Phase 4 bead by another agent in parallel.
- **Audio/transcript drift.** Audio is 915s; transcript fixture is 990s — ~75s ASR drift. Both the CVS and SiriusXM ads fall within the overlap, so fixture scoring is unaffected.
- **FM disabled in this benchmark.** `fmBackfillMode: .disabled`. The FM consensus anchor path is validated separately via `bd-1my` + `bd-1en` at 4/4 recall on real iPhone hardware — so the Phase 5 design still assumes FM consensus as the dominant anchor source, just not from this particular benchmark.

**Implication for Phase 5.** With FM disabled and the acoustic origin dropped upstream, the only anchor sources this benchmark exercises are `.lexical` (excluded) and `EvidenceEntry` coverage. That is why it sits at 15%. Phase 5 does not try to fix Phase 4 here — it ships on top of FM consensus + `EvidenceEntry`, and inherits acoustic backup anchors automatically when the upstream fix lands.

## Related work

- **Phase 4 region pipeline**: shipped via `913bff6` (wire-up) + `bd-1my` (outward expansion) + `bd-1en` (permissive refinement)
- **TargetedWindowNarrower**: shipped via cycle 8 merge `7a472d1`. Belongs conceptually at Phase 4.5, not Phase 5
- **Acoustic-origin dropped upstream**: `AcousticBreakDetector` produces breaks but `RegionProposalBuilder` does not surface them as `origins=acoustic` regions. Tracked as `playhead-8jd`. Bug confirmed via Stage C real-audio benchmark (3 breaks found, 0 surfaced as `.acoustic` origin). Fix is independent of Phase 5; lands separately.
- **Kelly Ripa expert recommendations**: tracked separately as `playhead-994` (`includeSchemaInPrompt` experiment), `playhead-36t` (`refusal.explanation` capture), `playhead-eu1` (auto-retry via permissive), `playhead-66k` (Feedback Assistant report)
- **Phase 7 user corrections**: will conform `UserCorrectionStore` to the protocol introduced here, replacing the stub
- **Phase 12 timeline segmentation decoder**: will replace `MinimalContiguousSpanDecoder` with a full Viterbi-style decoder once labeled training data is available
