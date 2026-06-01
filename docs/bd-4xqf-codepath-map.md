# bd-4xqf Code-Path Map

> **Purpose.** The bd-4xqf analyzer (`scripts/l2f-bd4xqf-analyze.py`) emits one of three per-pair verdicts on the next fresh Mac-Catalyst pipeline dump:
> - **OK** — pipeline boundaries cover the rediff slot
> - **CAND_NARROW** — even the candidate decoded spans are too narrow to cover the rediff slot
> - **FUSION_DROP** — candidates are wide enough but the persisted `AdWindow` is narrow
>
> This document maps each verdict to the production file/line where the fix lands and the constants/policies that govern boundary width.
>
> **Status:** read-only scouting; no production changes. Snapshot taken 2026-06-01.

---

## Pipeline overview (per asset/episode)

```
AtomEvidence(s)
  │
  ▼  [ Stage 1 ]  Playhead/Services/AdDetection/MinimalContiguousSpanDecoder.swift
DecodedSpan(s)         "form runs → merge → split → snap → drop"
  │
  ▼  [ Stage 2 ]  Playhead/Services/AdDetection/BackfillEvidenceFusion.swift
DecisionResult         per-span ledger, fusion confidence, gate
  │
  ▼  [ Stage 3a ]  Playhead/Services/AdDetection/AdDetectionService.swift:~3186-3260
Boundary refinement (LIVE PATH):
                              BracketAwareBoundaryRefiner.computeAdjustments(...) tried first
                              → on .bracketRefined path: bracket-aware (startAdj, endAdj)
                              → otherwise: legacy BoundaryRefiner.computeAdjustments(...) fallback
                              (uses transcriptBoundaryHits for `[kgby]` snap)
  │                           Produces `refinedSpan` (= the span recorded in Stage 3b)
  ▼  [ Stage 3b ]  Playhead/Services/AdDetection/AdDetectionService.swift:~3556
FragilityDiagnosticObserver.record(...)   ← captures refinedSpan.startTime/endTime in pipeline-dump
  │                                          (i.e. `candidateDecodedSpanList[*].startTime/endTime`)
  │                                          This is POST-decode, POST-boundary-refinement,
  │                                          POST-fusion-confidence — but PRE-persist.
  ▼  [ Stage 4 ]  Playhead/Services/AdDetection/AdDetectionService.swift:~5374
buildFusionAdWindow → AdWindow              startTime/endTime taken VERBATIM from span (no shrink)
  │
  ▼
store.insertAdWindow → AnalysisStore (SQLite)
                                            ← persisted as `adWindows[*]` in pipeline-dump
```

The dump's `candidateDecodedSpanList` is recorded at Stage 3b (= refined span, post-decode + post-refinement + post-fusion confidence).
The dump's `adWindows` is the AnalysisStore row at the end.
**The gap between Stage 3b and persist is ONLY confidence/state mapping — boundaries match verbatim.**

The PR-#210 dump additions (`boundaryRefinementStartAdjustment` / `boundaryRefinementEndAdjustment` / `wasSkipped`) probe Stage 3a from the test side by re-running `BoundaryRefiner.computeAdjustments` against the persisted bounds + same `featureWindows`. A non-zero delta there is evidence the live refiner shrunk the span between decode and the recorded `refinedSpan`.

> **Dead-code note** — corrected 2026-06-01: the private helper `applyBoundaryRefinement` at `AdDetectionService.swift:~5429` (referenced in the original draft of this map) is itself unreachable. The live refinement path runs inline at ~3186-3260 inside `runBackfill`'s per-span loop. `SpanFinalizer.swift` was unreachable through PR #211; **as of playhead-p56a it is wired behind a default-OFF config flag** (`AdDetectionConfig.spanFinalizerEnabled`) — see the FUSION_DROP suspect table below for the updated status. FUSION_DROP investigation can now toggle the SpanFinalizer arm on the Catalyst dump path and attribute coverage deltas to specific finalizer constraints via the new `spanFinalizerConstraintsFired: [String]?` field on each `DumpAdWindow`. Line numbers in this map are approximate and shift as `AdDetectionService.swift` accretes; `grep` for the symbol if a `:line` no longer resolves.

---

## Verdict → suspect map

### CAND_NARROW (most-likely culprit chain)

The decoded span itself is already short. Look upstream of Stage 3.

| Suspect | File | Constants | Why it could collapse a long DAI block |
|---|---|---|---|
| **`mergeGapSeconds: 3.0`** | `MinimalContiguousSpanDecoder.swift:30` (Configuration.default) | gap-merge threshold | DAI ads often have 3–5 s pauses between voice and music beds; runs on either side of a 4 s silent beat will NOT merge. One DAI block becomes two short spans. |
| **`maxDurationSeconds: 180`** | `DecodedSpan.swift:45` | hard split | A real DAI break can exceed 180 s on long-form shows (Casefile, Joe Rogan, Conan). Anything longer is split into ≤180 s pieces — but the SPLIT POINT is the longest internal gap, so the parts may still be ad-shaped; this is unlikely to cause CAND_NARROW alone. |
| **`minDurationSeconds: 5`** | `DecodedSpan.swift:43` | drop-floor | Won't shrink a long span but can drop fragments after MERGE/SPLIT, contributing to apparent under-coverage. |
| **`snapRadiusSeconds: 8.0`** | `MinimalContiguousSpanDecoder.swift:31` | boundary snap ± window | Used in `applyBoundarySnap` (Step 4). If an acoustic-break is found INSIDE the candidate within ±8 s of either edge, the candidate is snapped INWARD — can produce a narrower span. |
| **AtomEvidence sparsity** | upstream of decoder (transcript anchors) | (no constant) | If only the first 20 s of a DAI block has a sponsor-keyword anchor, the candidate run starts there and ends at the last anchored atom. The rest of the ad has no anchor → no run → no candidate. **Strong CAND_NARROW hypothesis for SmartLess/TED-style baked-in ads where only the disclosure has scoring lexicon.** |

**First-touch fix sites to try (in order of risk):**
1. Read-only: instrument `mergeAdjacentCandidates` (line 214 of `MinimalContiguousSpanDecoder`) to log gap-rejections; if gaps in the 3.0–6.0 s band dominate inside known DAI slots, raise threshold.
2. Add a DAI-aware merge policy: increase `mergeGapSeconds` to 6.0 conditionally when the surrounding region scores as a DAI candidate (cross-reference with rediff or acoustic-break density).
3. Re-examine anchor density inside DAI blocks (separate issue — needs AtomEvidenceProjector trace).

### FUSION_DROP (the dump's recorded refined span IS wide enough but persisted AdWindow is narrow)

Since boundaries pass verbatim from refinedSpan → AdWindow (Stage 4 at `:~5374` does no shrink), a FUSION_DROP verdict on PR #210's instrumented dump implies one of these is happening BEFORE Stage 3b records the span:

| Suspect | File:Line | What to check |
|---|---|---|
| **Live boundary refinement shrinks via acoustic snap** | `AdDetectionService.swift:~3186-3260` calling `BracketAwareBoundaryRefiner.computeAdjustments(...)` (primary) with `BoundaryRefiner.computeAdjustments(...)` fallback | If a strong acoustic break sits a few seconds INSIDE either edge, refinement may pull the boundary inward. **Already instrumented in PR #210**: `boundaryRefinementStartAdjustment` / `boundaryRefinementEndAdjustment` in the dump capture the delta the test re-derives against the persisted bounds. A non-zero delta directly indicts this stage. |
| **Eligibility-gate demotion produces a smaller persisted action range** | `BackfillEvidenceFusion.swift:991` `metadataCorroborationGate()` (and lines 1043+ for FM-consensus/FM-acoustic variants) | `blockedByEvidenceQuorum` (`EvidenceLedgerEntry.swift:21`) doesn't shrink the AdWindow's startTime/endTime in `buildFusionAdWindow` (~5374 — they're verbatim from `span`). So this isn't a direct boundary shrink, BUT it can demote `decisionState` to `.candidate`, which downstream views may render differently. Verify whether the AnalysisStore row reflects the original wide bounds even when gated. |
| **`SpanFinalizer` — wired behind default-OFF flag (`spanFinalizerEnabled`); see PR #<this>** | `SpanFinalizer.swift` (440 lines, unchanged) + wire-in at `AdDetectionService.swift` `runBackfill` between the temporal-regularization block and the emission loop | playhead-p56a (2026-06-01) reverses the PR #209 "VERIFIED UNREACHABLE" status: `SpanFinalizer.finalize(...)` is now invoked exactly once per `runBackfill` after fusion, before persistence, but ONLY when `config.spanFinalizerEnabled == true`. The shipped `AdDetectionConfig.default` keeps the flag OFF, so production behaviour is byte-identical to pre-p56a (asserted by `SpanFinalizerWireInTests.flagOffMatchesDefaultBaseline`). Constraint trace is captured per-span and per-window on a pair of test seams (`spanFinalizerConstraintsBySpanIdForTesting()` / `spanFinalizerConstraintsByWindowIdForTesting()`), and the live Catalyst dump emits the per-window trace as the optional `spanFinalizerConstraintsFired: [String]?` field on `DumpAdWindow` (absent under the OFF default). Flip the flag on a follow-up Catalyst run to measure whether constraint #2 (`<3s content-gap merge`) re-merges CAND_NARROW candidates the upstream `mergeGapSeconds: 3.0` threshold dropped. |
| **AdDecisionState recorded → wider window but `wasSkipped` only fires on the inner sub-span** | downstream of persist | If two overlapping ad windows are persisted and the player auto-skips the inner one but not the outer, the user STILL hears the unskipped portion. The boundary is right in the data but wrong in the playback action. Would explain why rediff says 150 s of ad audio plays even though the AdWindow row covers it. |

**First-touch fix sites:**
1. ~~Confirm SpanFinalizer reachability.~~ **DONE 2026-06-01** — verified UNREACHABLE through PR #211; see `docs/bd-4xqf-spanfinalizer-reachability-2026-06-01.md`. Eliminated as a FUSION_DROP suspect under the (then-default) UNREACHABLE state. **Reversed by playhead-p56a (2026-06-01):** SpanFinalizer is now wired behind a default-OFF config flag; the OFF default keeps it eliminated, but the ON arm is measurable on the Catalyst dump path.
2. ~~Extend the pipeline dump to record the BoundaryRefiner adjustment.~~ **DONE 2026-06-01** — PR #210 added `boundaryRefinementStartAdjustment` / `boundaryRefinementEndAdjustment` / `wasSkipped` to `DumpAdWindow`. Run the env-gated Catalyst dump to populate them on real data. playhead-p56a adds `spanFinalizerConstraintsFired: [String]?` so the same dump can attribute coverage deltas to specific finalizer constraints when the flag is flipped on.
3. **Verify the playback action vs the persisted bounds.** This is the most-pernicious failure mode because the data looks right; only listening reveals it. `wasSkipped: Bool` (also PR #210) is the playback signal; cross-reference with persisted bounds to catch "boundary right, action wrong" cases.

---

## Where to instrument (when fresh dump arrives)

Once a fresh Mac-Catalyst dump with `candidateDecodedSpanList` lands at the repo root:

```bash
scripts/l2f-bd4xqf-analyze.py            # markdown report
scripts/l2f-bd4xqf-analyze.py --json     # machine-readable
```

The analyzer's per-pair table will pinpoint episodes for each verdict; cross-reference with this map to pick the first fix to attempt.

---

## Out-of-scope but worth a follow-up bead

- **Atom anchor density inside known DAI ads.** If CAND_NARROW dominates, the boundary problem is partly an upstream coverage problem in `AtomEvidenceProjector` (atoms are only created at scored-lexicon hits). A DAI-aware atom-densification policy might be needed.
- **`applyBoundaryRefinement` dead-code cleanup.** Still verified unreachable (no in-file callers); the live refinement path is the inline block at ~3186-3260 inside `runBackfill`'s per-span loop. playhead-p56a explicitly left `applyBoundaryRefinement` alone (out of scope). Either rewire or remove in a follow-up bead — limbo code is worse than either.
- **SpanFinalizer status (2026-06-01 update):** wired by playhead-p56a behind `AdDetectionConfig.spanFinalizerEnabled` (default OFF). Measurement on the env-gated Catalyst dump path is the gate before flipping the production default.
- **SpanFinalizer wire-in limitations to account for when interpreting the ON-arm dump (playhead-p56a R2 review, 2026-06-01):**
  - **Merge-context drop.** When constraint #2 (`mergedWithAdjacent`) collapses two pending records into one finalized span, the wire-in's rebuild loop keys by `pendingByOriginalId[span.span.id]` and the surviving WorkingSpan carries only the earlier `prev.spanId`. The merged-away `curr` pending's `ledger`, `effectiveLedger`, `spanFingerprint`, `spanFeatureWindows`, and `spanTopCatalogSimilarity` are silently dropped from emission. Downstream effects on the ON-arm dump: `AdCatalogStore` and `RepeatedAdCache` ingress for the merged span uses only `prev`'s fingerprint context. Pinned by `SpanFinalizerWireInTests.spanFinalizerMergeKeepsFirstCandidateIdAndDropsSecond`.
  - **Split-id collision.** When constraint #3 (`splitAboveMaxDuration`) splits a >180s candidate, both halves share the parent `DecodedSpan.id`. The wire-in appends both to `pendingDecisions` with the same pending record (same fingerprint / ledger / feature windows), and the emission loop runs `adCatalogStore.insert(...)` and `repeatedAdCache.ingress(...)` twice with that identical context but different split-half durations. Both emission iterations look up the same concatenated `lastSpanFinalizerConstraintsBySpanId[parentId]` entry, so both windows in the dump end up tagged with the merged trace of both halves rather than only their own. Pinned by `SpanFinalizerWireInTests.spanFinalizerSplitPreservesParentIdAndTagsBothHalves`.

  Both items are out of scope for p56a (which is wire-in only); a follow-up bead should decide the merge-context union policy and the per-split trace key.

---

## Verified at snapshot 2026-06-01

- `AdDetectionService.swift` total: ~8850 lines (was ~7700 pre-2026-06-01; +213 from the playhead-p56a SpanFinalizer wire-in plus other intervening churn).
- `MinimalContiguousSpanDecoder.swift`: 163 lines (well-commented, single-file responsibility).
- `SpanFinalizer.swift`: 440 lines; wired behind `AdDetectionConfig.spanFinalizerEnabled` (default OFF) as of playhead-p56a (2026-06-01). The public API is unchanged; the OFF path is byte-identical to pre-p56a behavior (asserted by `SpanFinalizerWireInTests.flagOffMatchesDefaultBaseline`). When ON, the finalizer's per-span constraint trace surfaces on the live pipeline-dump path via the new `spanFinalizerConstraintsFired: [String]?` field on each `DumpAdWindow`. The pre-p56a UNREACHABLE state is documented at `docs/bd-4xqf-spanfinalizer-reachability-2026-06-01.md` for historical context.
- `applyBoundaryRefinement` at `AdDetectionService.swift:~5429`: VERIFIED UNREACHABLE (no in-file callers). Live refinement path is the inline block at ~3186-3260 inside `runBackfill`'s per-span loop. playhead-p56a explicitly does NOT wire this helper (out of scope).
- `FragilityDiagnosticObserver.swift`: 155 lines; production-nil-default, tap fires at `AdDetectionService.swift:~3556` (Stage 3b in the pipeline diagram above).

Re-verify before acting; this file is a snapshot, not live.
