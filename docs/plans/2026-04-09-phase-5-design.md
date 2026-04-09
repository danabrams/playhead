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
EvidenceCatalogEntry list ─────┼──► AtomEvidenceProjector ──► [AtomEvidence]
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

- A Phase 4 region with `.foundationModel` origin AND `fmConsensusStrength >= 0.5` (FM agreed across at least 2 windows — single-window FM is excluded)
- An `EvidenceCatalogEntry` of trustworthy type (URL, promo code, explicit disclosure phrase, CTA phrase)

That is the entire anchor source list. Two paths. Both come pre-validated by upstream phases. Phase 5 does not run its own anchor detection logic; it consumes anchors that already passed Phase 4's filtering.

What does NOT anchor:

- `.lexical` regex hits (too noisy on their own)
- `.acoustic` breaks (RMS dips happen for many reasons)
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
    case evidenceCatalog(entry: EvidenceCatalogEntry)
}

enum CorrectionState: Sendable {
    case none
    case userVetoed       // user said "this isn't an ad"
    case userConfirmed    // user said "yes this is an ad"
}
```

Three fields per atom. No score. No per-feature vector. No normalization. No calibration math.

`anchorProvenance` is preserved through to the UI layer for the tap-to-explain popover (5.4) — when a user taps a highlighted span, we show "Detected from FM consensus + 2 EvidenceCatalogEntries: cvs.com URL, promo code CONAN10".

### Correction mask integration

`AtomEvidenceProjector` consumes a `CorrectionMaskProvider` protocol. Phase 5 ships with a `NoCorrectionMaskProvider` stub that returns `.none` for every atom. Phase 7 (User Corrections) will conform a real `UserCorrectionStore` to this protocol and inject it through the runtime. This avoids a circular dependency between Phases 5 and 7.

The `correctionMask` field on each `AtomEvidence` is the load-bearing innovation that lets user corrections persist across transcript reprocessing — atom ordinals are stable, time intervals are not.

### Decoder algorithm (Q1 from brainstorm)

`MinimalContiguousSpanDecoder` is a linear walk over the `[AtomEvidence]` array:

```
1. Walk atoms left to right
2. A contiguous run of atoms where (isAnchored && correctionMask != .userVetoed) is a candidate span
3. Drop spans below MIN_DURATION_SECONDS (5s — universal industry constant)
4. Split spans above MAX_DURATION_SECONDS (180s) at the longest internal gap of unanchored atoms
5. Two adjacent candidate spans merge if separated by fewer than MERGE_GAP_ATOMS unanchored atoms
   AND no .userVetoed atom sits between them (atom-count, not seconds, for transcript-density invariance)
6. Output: [DecodedSpan] with anchorProvenance carried through
```

Three universal constants. No thresholds on score values. No calibration. Same behavior on every podcast.

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
- **Round-trip integration** on the Conan fixture: feed Phase 4 region bundles + EvidenceCatalogEntry list to projector + decoder, assert decoded spans correspond to ground truth ads with measurable improvement over the Phase 4 baseline
- **Materialization round-trip**: persist + reload + idempotent re-run
- **UI snapshot**: rendered transcript with one fixture episode for visual regression

## Success metrics (from playhead-4my.5 acceptance criteria)

Already pinned on the epic from earlier in the brainstorm:

1. `Phase4ShadowBenchmarkTests` on the Conan fixture shows ad-second coverage moving from 15% (current synthetic-features baseline) toward the doc target of 80%+
2. Decoded spans are contiguous (no micro-fragments < 5s)
3. Duration constraints prevent implausible spans (no 30-minute "ads")
4. Correction masks prevent re-detection of user-reverted spans
5. Lenient region-recall does NOT regress below 75%
6. Zero new test flakes when run parallelized

## Risks

| Risk | Mitigation |
|---|---|
| The 15% benchmark baseline is artifact of synthetic features, not real Phase 4 capability. Real-features baseline could already be near 80%, leaving Phase 5 with a smaller delta to demonstrate. | Stage C real-features benchmark (in progress in background as `Phase4ShadowBenchmarkTests.phase4OnRealFeatures`) will tell us what real Phase 4 produces. Update the baseline number once measured. |
| Anchor count on real episodes might be very low if the EvidenceCatalogBuilder misses common patterns. | The Conan smoke test already shows 4/4 recall through FM consensus alone (without relying on EvidenceCatalogEntry as the primary path). FM consensus is the dominant anchor source. |
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

- Exact value of `MERGE_GAP_ATOMS` (probably 3–5; should be tuned during implementation against the Conan fixture)
- Whether `DecodedSpan` is a new SQLite table or extends `AdWindow`
- Whether the popover gesture is a long-press, swipe, or button tap (left to UI iteration)
- Whether the background tint color is defined as a new design token or reuses an existing one

## Related work

- **Phase 4 region pipeline**: shipped via `913bff6` (wire-up) + `bd-1my` (outward expansion) + `bd-1en` (permissive refinement)
- **TargetedWindowNarrower**: shipped via cycle 8 merge `7a472d1`. Belongs conceptually at Phase 4.5, not Phase 5
- **Kelly Ripa expert recommendations**: tracked separately as `playhead-994` (`includeSchemaInPrompt` experiment), `playhead-36t` (`refusal.explanation` capture), `playhead-eu1` (auto-retry via permissive), `playhead-66k` (Feedback Assistant report)
- **Phase 7 user corrections**: will conform `UserCorrectionStore` to the protocol introduced here, replacing the stub
- **Phase 12 timeline segmentation decoder**: will replace `MinimalContiguousSpanDecoder` with a full Viterbi-style decoder once labeled training data is available
