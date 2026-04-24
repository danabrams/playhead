# Finding 1 diagnosis — Conan 117-min, why Pred=0 despite full transcript coverage

Investigation bead: `playhead-gtt9.18`
Parent epic: `playhead-gtt9`
Branch: `investigation/gtt9.18`
Fixture: `PlayheadTests/Fixtures/NarlEval/2026-04-24/FrozenTrace-71F0C2AE-7260-4D1E-B41A-BCFD5103A641.json`

## Repro

The 2026-04-24 real-data NARL eval (Finding 1) scored the flagship Conan 117-min
episode **GT=3, Pred=0, Sec-F1=0** despite `terminalReason: "full coverage:
transcript 1.000, feature 1.000"`. This was the first measurement where transcript
coverage was a non-issue — so classifier / promotion / fusion failure was claimed
to be the root cause.

Steps taken:

1. Inspected the frozen-trace JSON (30.6 KB, 14 decision events, 14 window scores,
   139 evidence-catalog entries).
2. Compared `windowScores` + `decisionEvents` against the ground-truth set
   constructed by `NarlGroundTruth.build(for:)`.
3. Traced predictions through `NarlReplayPredictor.predict(trace:config:)`, which
   derives positives from `windowScores.filter { $0.isAdUnderDefault }` (v2 path).
4. Walked the production pipeline that emits `FrozenTrace.FrozenWindowScore` via
   the NARL corpus builder (`NarlEvalCorpusBuilderTests.swift:523-538`).

## Observed

**Ground-truth set (§A.4):** 3 merged ad windows
- `[0.00, 29.82]` (baseline-replay, confidence=1.0)
- `[5670.60, 5690.52]` (baseline-replay, confidence=1.0)
- `[7006.00, 7037.34]` (union of 3 FN corrections)

**windowScores (14 total):** ALL 14 rows have `isAdUnderDefault: false`, including
the two rows with `fusedSkipConfidence: 1.0` and `hasMetadataEvidence: true`
covering the baseline-replay ads.

**decisionEvents policyAction histogram:**

| policyAction | count | windows (start, confidence) |
|---|---|---|
| `hotPathBelowThreshold` | 7 | [0.0, 0.27], [5148.8, 0.344]×3, [5215.1, 0.380]×4 |
| `hotPathCandidate` | 2 | [7006.0, **0.815**], [7006.0, 0.648] |
| `detectOnly` | 2 | [0.0, 1.0], [5670.6, 1.0] |

**Evidence catalog near GT spans:**

| GT span | Sources firing (non-zero weight) |
|---|---|
| [0, 29.82] | classifier (0.12), fm×3 (0.30 each), lexical (0.075), catalog (0.02), metadata×2 (0.15), acoustic (0.20) |
| [5670.60, 5690.52] | classifier (0.0), fm×3 (0.30), catalog (0.02), metadata×2 (0.15), acoustic (0.20) |
| [7006.00, 7037.34] | classifier×2 (0.245, 0.194) **only** — no fm / metadata / lexical / catalog / acoustic |

Globally across the 117-min episode: 1 lexical entry, 1 acoustic entry, 4 metadata,
2 catalog, 6 fm, 14 classifier, 111 shadow-all-zero.

## Diagnosis

The cause is **NOT** classifier blindness, threshold tuning, or
MusicBedLevel disconnection. It is a **corpus-builder / harness plumbing bug**
that prevents BOTH already-detected baseline ads AND high-confidence hot-path
candidates from being counted as predictions. The detector is doing better than
the number suggests; the harness is scoring the wrong bit.

Two concrete failures compound:

### Bug A — `detectOnly` is not recognised as an ad prediction

`NarlEvalCorpusBuilderTests.swift:527-529` derives `isAdUnderDefault` from the
logged policyAction via:

```swift
let isAdUnderDefault = action.contains("autoskip")
    || action.contains("markonly")
    || action.contains("skip") && !action.contains("suppress")
```

`detectOnly` is a valid, positive ad determination in production
(`SkipPolicyMatrix.swift`) — it means "this IS an ad, show a banner, don't
auto-skip." For owned/affiliate/unknown-unknown content the matrix emits
`detectOnly` by design. Both Conan baseline ads [0, 29.82] and [5670.60, 5690.52]
were logged as `detectOnly` with confidence=1.0 and therefore scored
`isAdUnderDefault: false` in the fixture. The harness then drops them from
`predicted`, even though the detector caught them correctly.

This single bug accounts for 2 of the 3 missing predictions on this episode.

### Bug B — Hot-path `autoSkipEligible` carve-out didn't apply to this capture

Commit `6e37335` (2026-04-23 22:29 EDT) promotes hot-path classifier scores
≥ `autoSkipConfidenceThreshold` (0.80) from `hotPathCandidate` →
`autoSkipEligible`. The fixture was `capturedAt: 2026-04-24T03:33:48Z` (~1 hr
after commit), but the device binary producing the trace still logged
`policyAction: "hotPathCandidate"` for the window at [7006, 7008] with
`skipConfidence: 0.8154`. Either the capture device was running a pre-fix
build or `capturedAt` reflects harness ingestion, not device session time.

Under the corpus builder heuristic, `hotPathCandidate` does not match
`autoskip`/`markonly`/`skip`, so `isAdUnderDefault: false`. Even if 6e37335
had applied, harness `Pred=[7006, 7008]` vs `GT=[7006, 7037.34]` has
IoU=2/31.34=0.064 — well below the 0.3 threshold — so the scoring window is
still an FN. The GT-span width comes from the user's third FN correction (30 s
span) that covers the entire closing ad block; the detector only saw the head
chunk. That's a **boundary expansion** problem, not a detection problem.

### Null-effect confirmations

- MusicBedLevel is not the blocker here. The 5670 baseline ad fires `acoustic`
  weight 0.20 and fused to 1.0; the 0 ad fires `acoustic` 0.20 and fused to 1.0.
  MusicBedLevel is evidently connected somewhere in Conan's mid-episode sponsor
  reads. (gtt9.4.1 wiring status unverified here — didn't touch it.)
- Classifier DOES fire in the GT region. The 7006 window ran at 0.8154, well
  above `candidateThreshold` (0.40) and above `autoSkipConfidenceThreshold`
  (0.80). No threshold tuning would help — the gate passed.
- The 5148/5215 scores (0.344, 0.380) are NOT inside any GT span. They are FP
  flutters between ads, legitimately gated out by the 0.40 candidate threshold.

## Hypothesis

The per-episode `GT=3, Pred=0` headline on 71F0C2AE is artifactual. The
detector correctly flagged 2 of the 3 ad windows (the `detectOnly` ads at 0 and
5670) and confidently flagged the head of the third ([7006, 7008], score 0.815
→ should have surfaced as `autoSkipEligible`). The harness discards all three
because the corpus builder's `isAdUnderDefault` heuristic is a substring check
that misses `detectOnly` and missed `hotPathCandidate` from pre-6e37335 builds.
Fix the corpus builder (count `detectOnly` and any `autoSkipEligible` action
as positive) and the Conan Sec-F1 for this episode jumps from 0 to ~0.5
without touching a single classifier or fusion line. The remaining gap on
[7006, 7037.34] is boundary expansion, not classifier recall.

**Consequence for Phase 2 prioritization:** gtt9.12 (acoustic features) and
gtt9.3 (threshold calibration) remain valid on their own merits but are
**NOT** on the critical path for long conversational shows until the harness
plumbing is fixed. Otherwise their improvements won't be visible in Sec-F1.

## Follow-ups filed

- `playhead-gtt9.19` — **corpus builder:** treat `detectOnly` and
  `autoSkipEligible` as `isAdUnderDefault=true`; make the check exact, not
  substring. Blocks any reliable NARL number on shows with owned/affiliate
  content (Conan, most comedy, most daily news).
- `playhead-gtt9.20` — **boundary expansion:** hot-path candidates that clear
  `autoSkipConfidenceThreshold` should carry expanded spans (±10-30 s) into
  the log so the harness has a fighting chance at IoU ≥ 0.3 vs user-marked
  30 s FN corrections. Today the 7006 window emits [7006, 7008] (2 s) even
  when the user marks a 30 s closing block.
- `playhead-gtt9.21` — **capture provenance:** stamp the device binary's
  git-SHA (or detectorVersion) into FrozenTrace so the harness can identify
  pre-fix captures and apply back-compat shims. Today `capturedAt` reflects
  harness ingestion, not the device session.

## Next test capture to validate

After `gtt9.19` lands, re-run the NARL harness against the existing 2026-04-24
fixture (no new capture needed) and confirm:

- Conan 71F0C2AE: Pred ≥ 2 (the detectOnly-flagged baseline ads), Sec-F1 ≥ 0.4.
- ALL-episode Sec-F1 rises by at least 5 pts under both configs.
- DoaC delta (`allEnabled`: 0.446, `default`: 0.496) should shrink or flip
  once detectOnly decisions are no longer silently discarded.

If gtt9.20 also lands before the next capture, the [7006, 7008] prediction
should expand to roughly [6996, 7026] and push the third GT span above
IoU=0.3. That's the next validation point; until then, 2-of-3 is the ceiling
on this episode.

## Caveats

- **I did not re-run the harness.** The Bazel/xcodebuild round-trip on a
  fresh worktree is > 5 minutes per invocation; the diagnosis is entirely
  from FrozenTrace JSON + production source reading + commit-log archaeology.
- **I did not confirm the capture device's build SHA.** The `capturedAt`
  timestamp is 1 h after commit 6e37335; whether the device that produced
  the classifier run was running pre- or post-fix is a provenance gap
  (hence follow-up gtt9.21).
- **The 30 s third-ad FN correction** ([7007.34, 7037.34]) is listed once
  in `corrections` but the harness `mergeOverlaps` collapses it with
  [7006, 7008] and [7007.34, 7009.5] into a single [7006, 7037.34] GT
  span. This matches NARL §A.4 rule 4 but means a 2-s head-of-ad prediction
  can never reach IoU ≥ 0.3 against the merged span.
- I did not verify that MusicBedLevel is actually wired in the production
  fusion path; the fixture simply shows `acoustic` weight fires on the
  baseline ads, which doesn't disprove the gtt9.4 concern. That bead
  remains separately live.
