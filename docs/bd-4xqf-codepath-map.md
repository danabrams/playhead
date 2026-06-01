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
  ▼  [ Stage 3 ]  Playhead/Services/AdDetection/AdDetectionService.swift:3467
FragilityDiagnosticObserver.record(...)   ← captures spanStart/spanEnd recorded in pipeline-dump
  │                                          (i.e. `candidateDecodedSpanList[*].startTime/endTime`)
  │                                          This is the POST-Stage-1, post-fusion REFINED span.
  ▼  [ Stage 4 ]  Playhead/Services/AdDetection/AdDetectionService.swift:5186
buildFusionAdWindow → AdWindow              startTime/endTime taken VERBATIM from span (no shrink)
  │
  ▼  [ Stage 5, optional ]  Playhead/Services/AdDetection/AdDetectionService.swift:5219
applyBoundaryRefinement → BoundaryRefiner.computeAdjustments(...)
                                            CAN shrink or grow ± per acoustic break inside window
  │
  ▼
store.insertAdWindow → AnalysisStore (SQLite)
                                            ← persisted as `adWindows[*]` in pipeline-dump
```

The dump's `candidateDecodedSpanList` is recorded at Stage 3 (= refined span post-decode + post-fusion confidence).
The dump's `adWindows` is the AnalysisStore row at the end.
**The gap between them is Stages 4 + 5.**

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

### FUSION_DROP (between Stages 3 and 5)

The decoded span IS wide enough but the persisted AdWindow is narrow. Look at Stages 4–5.

| Suspect | File:Line | What to check |
|---|---|---|
| **`applyBoundaryRefinement` shrinks via acoustic snap** | `AdDetectionService.swift:5219` calling `BoundaryRefiner.computeAdjustments(...)` (or `BracketAwareBoundaryRefiner` at `BracketAwareBoundaryRefiner.swift:27`) | If the candidate window has a strong acoustic break a few seconds INSIDE either edge, refinement may pull the boundary inward. Verify by comparing pre/post-refinement bounds in a dump-extension. |
| **Eligibility-gate demotion produces a smaller persisted action range** | `BackfillEvidenceFusion.swift:991` `metadataCorroborationGate()` (and lines 1043+ for FM-consensus/FM-acoustic variants) | `blockedByEvidenceQuorum` (`EvidenceLedgerEntry.swift:21`) doesn't shrink the AdWindow's startTime/endTime in `buildFusionAdWindow` (5186 — they're verbatim from `span`). So this isn't a direct boundary shrink, BUT it can demote `decisionState` to `.candidate`, which downstream views may render differently. Verify whether the AnalysisStore row reflects the original wide bounds even when gated. |
| **`SpanFinalizer` (defined but NOT called in production)** | `SpanFinalizer.swift` — has overlap-resolution and minimum-content-gap merge logic (lines 152, 170) | Grep confirms no production callers as of this snapshot (`grep -rn "SpanFinalizer(" Playhead --include="*.swift"` returns 0). **This is suspicious — either dead code or wired through indirect generic.** If FUSION_DROP fires, first action: confirm SpanFinalizer is unreachable. If it is reachable, its constraint #1 (non-overlap, higher confidence wins) can `trimEnd` a wider lower-confidence span to a higher-confidence shorter neighbor (lines 152–164). |
| **AdDecisionState recorded → wider window but `wasSkipped` only fires on the inner sub-span** | downstream of persist | If two overlapping ad windows are persisted and the player auto-skips the inner one but not the outer, the user STILL hears the unskipped portion. The boundary is right in the data but wrong in the playback action. Would explain why rediff says 150 s of ad audio plays even though the AdWindow row covers it. |

**First-touch fix sites:**
1. **Confirm SpanFinalizer reachability.** If unreachable, remove from this map. If reachable, that's the most-likely shrink site.
2. **Extend the pipeline dump to also record the BoundaryRefiner adjustment.** Same pattern as #201 — add `boundaryRefinementDelta` to `DumpAdWindow` so the analyzer can directly read whether refinement is shrinking.
3. **Verify the playback action vs the persisted bounds.** This is the most-pernicious failure mode because the data looks right; only listening reveals it.

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
- **`SpanFinalizer` status.** Either kill it or wire it. Limbo code is worse than either.
- **`BracketAwareBoundaryRefiner`** vs the legacy `BoundaryRefiner` (legacy is referenced at line 5225). If the bracket-aware version is the production path, this map's Stage-5 references are wrong; verify.

---

## Verified at snapshot 2026-06-01

- `AdDetectionService.swift` total: 7700+ lines.
- `MinimalContiguousSpanDecoder.swift`: 163 lines (well-commented, single-file responsibility).
- `SpanFinalizer.swift`: 342+ lines but ZERO production callers per grep.
- `FragilityDiagnosticObserver.swift`: 155 lines; production-nil-default, tap fires at 3467 in service file.

Re-verify before acting; this file is a snapshot, not live.
