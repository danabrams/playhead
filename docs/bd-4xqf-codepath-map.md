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
  ▼  [ Stage 3a ]  Playhead/Services/AdDetection/AdDetectionService.swift:3120-3155
Boundary refinement (LIVE PATH):
                              BracketAwareBoundaryRefiner.computeAdjustments(...) tried first
                              → on .bracketRefined path: bracket-aware (startAdj, endAdj)
                              → otherwise: legacy BoundaryRefiner.computeAdjustments(...) fallback
                              (uses transcriptBoundaryHits for `[kgby]` snap)
  │                           Produces `refinedSpan` (= the span recorded in Stage 3b)
  ▼  [ Stage 3b ]  Playhead/Services/AdDetection/AdDetectionService.swift:3467
FragilityDiagnosticObserver.record(...)   ← captures refinedSpan.startTime/endTime in pipeline-dump
  │                                          (i.e. `candidateDecodedSpanList[*].startTime/endTime`)
  │                                          This is POST-decode, POST-boundary-refinement,
  │                                          POST-fusion-confidence — but PRE-persist.
  ▼  [ Stage 4 ]  Playhead/Services/AdDetection/AdDetectionService.swift:5186
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

> **Dead-code note** — corrected 2026-06-01: the private helper `applyBoundaryRefinement` at `AdDetectionService.swift:5219` (referenced in the original draft of this map) is itself unreachable. The live refinement path runs inline at 3120-3155 inside `runBackfill`'s per-span loop. `SpanFinalizer.swift` is also unreachable (verified in PR #209). FUSION_DROP investigation should target Stage 3a, not those two dead helpers.

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

Since boundaries pass verbatim from refinedSpan → AdWindow (Stage 4 at `:5186` does no shrink), a FUSION_DROP verdict on PR #210's instrumented dump implies one of these is happening BEFORE Stage 3b records the span:

| Suspect | File:Line | What to check |
|---|---|---|
| **Live boundary refinement shrinks via acoustic snap** | `AdDetectionService.swift:3120-3155` calling `BracketAwareBoundaryRefiner.computeAdjustments(...)` (primary) with `BoundaryRefiner.computeAdjustments(...)` fallback | If a strong acoustic break sits a few seconds INSIDE either edge, refinement may pull the boundary inward. **Already instrumented in PR #210**: `boundaryRefinementStartAdjustment` / `boundaryRefinementEndAdjustment` in the dump capture the delta the test re-derives against the persisted bounds. A non-zero delta directly indicts this stage. |
| **Eligibility-gate demotion produces a smaller persisted action range** | `BackfillEvidenceFusion.swift:991` `metadataCorroborationGate()` (and lines 1043+ for FM-consensus/FM-acoustic variants) | `blockedByEvidenceQuorum` (`EvidenceLedgerEntry.swift:21`) doesn't shrink the AdWindow's startTime/endTime in `buildFusionAdWindow` (5186 — they're verbatim from `span`). So this isn't a direct boundary shrink, BUT it can demote `decisionState` to `.candidate`, which downstream views may render differently. Verify whether the AnalysisStore row reflects the original wide bounds even when gated. |
| **`SpanFinalizer` — VERIFIED UNREACHABLE (eliminated as FUSION_DROP suspect 2026-06-01)** | `SpanFinalizer.swift` | Reachability investigation `docs/bd-4xqf-spanfinalizer-reachability-2026-06-01.md` confirms zero direct callers, zero indirect callers (no factory, generic, reflection, or DI path), and zero ever-added/ever-removed call sites in git history since the introducing commit `e5fb5151` (2026-04-15). Only invocations are in `PlayheadTests/.../SpanFinalizerTests.swift`. The file compiles into the Playhead target (`project.pbxproj:4505`) but is never invoked. Cannot be a FUSION_DROP cause. Follow-up: wire it (insertion point: between fusion at `AdDetectionService.swift:~2828` and `buildFusionAdWindow` at `:5186`) or remove it; tracked as a separate bead. |
| **AdDecisionState recorded → wider window but `wasSkipped` only fires on the inner sub-span** | downstream of persist | If two overlapping ad windows are persisted and the player auto-skips the inner one but not the outer, the user STILL hears the unskipped portion. The boundary is right in the data but wrong in the playback action. Would explain why rediff says 150 s of ad audio plays even though the AdWindow row covers it. |

**First-touch fix sites:**
1. ~~Confirm SpanFinalizer reachability.~~ **DONE 2026-06-01** — verified UNREACHABLE; see `docs/bd-4xqf-spanfinalizer-reachability-2026-06-01.md`. Eliminated as a FUSION_DROP suspect.
2. ~~Extend the pipeline dump to record the BoundaryRefiner adjustment.~~ **DONE 2026-06-01** — PR #210 added `boundaryRefinementStartAdjustment` / `boundaryRefinementEndAdjustment` / `wasSkipped` to `DumpAdWindow`. Run the env-gated Catalyst dump to populate them on real data.
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
- **`SpanFinalizer` and `applyBoundaryRefinement` dead-code cleanup.** Both verified unreachable. Either wire them (insertion point for SpanFinalizer noted at `AdDetectionService.swift:~2828` per PR #209) or remove. Limbo code is worse than either. Should be a separate bead.

---

## Verified at snapshot 2026-06-01

- `AdDetectionService.swift` total: 7700+ lines.
- `MinimalContiguousSpanDecoder.swift`: 163 lines (well-commented, single-file responsibility).
- `SpanFinalizer.swift`: 440 lines; VERIFIED UNREACHABLE in production (only callers are tests). See `docs/bd-4xqf-spanfinalizer-reachability-2026-06-01.md`.
- `applyBoundaryRefinement` at `AdDetectionService.swift:5219`: VERIFIED UNREACHABLE (no in-file callers). Live refinement path is the inline block at 3120-3155 inside `runBackfill`'s per-span loop.
- `FragilityDiagnosticObserver.swift`: 155 lines; production-nil-default, tap fires at `AdDetectionService.swift:3467` (Stage 3b in the pipeline diagram above).

Re-verify before acting; this file is a snapshot, not live.
