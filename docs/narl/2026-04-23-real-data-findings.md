# NARL real-data eval findings — 2026-04-23

First run of the NARL counterfactual harness against real captured data (not synthetic fixtures). Results surfaced five distinct issues that collectively reframe "improving ad detection" away from "tune metadata activation" and toward "finish transcribing the audio."

**Detailed data + per-correction tables for external review:** `docs/narl/2026-04-23-expert-report.md`

## Source data

- **Capture bundle:** `.captures/2026-04-23/com.playhead.app 2026-04-23 13:54.43.748.xcappdata/` (gitignored; dev-only; retained for re-analysis)
- **Generated fixtures:** `PlayheadTests/Fixtures/NarlEval/2026-04-23/` — 16 `FrozenTrace-*.json` files
- **Harness output:** `.eval-out/narl/20260423-175930-22C2B0/` (gitignored; regenerated on each run)
- **Sidecars in bundle:**
  - `corpus-export.2026-04-22T00-17-21.320Z.jsonl` — 93 rows: 16 assets + 5 decisions + 72 corrections
  - `decision-log.jsonl` — 147 per-window decisions with fused confidence + evidence breakdown
  - `shadow-decisions.jsonl` — **0 bytes** (shadow capture never populated during this run)
  - `ExportedAnalysisStore/analysis.sqlite` — tables present but row counts are 0 (the live sqlite was written to the sidecars; this copy appears empty)

## Headline numbers

Against 11 non-vetoed real episodes (9 excluded by whole-asset veto):

| Config | F1@0.3 | F1@0.5 | F1@0.7 | **Sec-F1** |
|---|---|---|---|---|
| default | 0.250 | 0.250 | 0.188 | **0.301** |
| allEnabled | 0.303 | 0.242 | 0.182 | **0.372** |

`allEnabled` **improves** second-level F1 by ~7 points on real data — opposite of the synthetic-fixture result. First evidence supporting activation. But the bigger story is below.

## Finding 1 — Pipeline completion dominates everything else

**63 of 72 user corrections are in windows the detector never scored.** Only 9 corrections trace to `causalSource="foundationModel"` (i.e., FM actually made a wrong call). The other 63 are user-adds filling analytical gaps.

Root cause visible in the asset table: four episodes are marked `analysisState='complete'` yet `fastTranscriptCoverageEndTime=90` while `featureCoverageEndTime` runs into the thousands.

| assetId | episodeId_tail | fastTxCov | featCov | ratio |
|---|---|---|---|---|
| 99E86F79 | flightcast:01KNF6VSZK1MXKZRCKT9PQKSZW | 90s | 5704s | 1.6% |
| 5951989F | flightcast:01KM20WJPKVFHRVJZWTNA6Q1XT | 90s | 7036s | 1.3% |
| A53E3CE0 | flightcast:01KP9HAYP5R81VSSVBEPE3C926 | 90s | 2370s | 3.8% |
| D787EAA8 | flightcast:01KMZRVEW8TBWD8QTDH5GJQWE6 | 90s | 1830s | 4.9% |

The uniform `90s` ceiling points at a hard-coded limit or early-exit condition. Not a detection bug — the classifier never ran on 95%+ of the audio.

**Tracked as:** `playhead-gtt9.1`

## Finding 2 — hotPathCandidate → autoSkipEligible promotion gap

Across 147 scored windows, **41 reached `hotPathCandidate` but only 3 reached `autoSkipEligible`**. All 3 promoted windows had `fusedConfidence=1.000`. No window in `(0.40, 0.999)` was ever promoted, despite the nominal classifier threshold being 0.40.

Two concrete missed-promotion cases on asset `DF5C1832` (Diary of a CEO, `01KM20WJPK...`) — both hotPathCandidate windows overlap user-confirmed false-negative spans:

| Asset | Window | Confidence | Action | Overlaps GT span |
|---|---|---|---|---|
| DF5C1832 | [1612, 1613] | **0.45** | hotPathCandidate | FN=[1550, 1621] |
| DF5C1832 | [1676, 1677] | **0.46** | hotPathCandidate | FN=[1624, 1689] |
| C22D6EC6 | [74.0, 83.4] | 0.597 | hotPathCandidate | overlaps FP=[77.1, 85.0] — correctly held |

The C22D6EC6 [74.0, 83.4] @ 0.597 window (initially highlighted as a miss) actually overlaps a user-marked *false-positive* — the detector correctly hesitated rather than auto-skipping something the user does not consider an ad. That's promotion-gap working correctly, not a bug. The real bug lives on DF5C1832: two windows above the 0.40 threshold overlap user-confirmed ads but never promote.

Effectively the promotion path behaves like a hard 1.0 threshold: if fused confidence is not maximal, the window never escalates.

**Tracked as:** `playhead-gtt9.2`

## Finding 3 — PriorShift is inert on real data

Configured thresholds:
- `classifierBaselineMidpoint`: 0.25
- `classifierShiftedMidpoint`: 0.22

PriorShift targets windows in the half-open band `(0.22, 0.25]` — confidences that would flip under the shifted midpoint but not the baseline.

**On 147 real scored windows, that band contains zero.**

Confidence histogram:

```
[0.10, 0.20): 11   ███████████
[0.20, 0.22):  0
[0.22, 0.25):  0   ← priorShift band, empty
[0.25, 0.30): 12   ████████████
[0.30, 0.40): 78   █████████████████████████████████████████████████████████████████████
[0.40, 0.50): 11   ███████████
[0.50, 0.70): 23   ███████████████████████
[0.70, 1.00):  8   ████████
```

Mode is `(0.30, 0.40)` — 53% of windows. The synthetic fixtures placed windows artificially in the priorShift band; real classifier output doesn't cluster there. PriorShift as configured can't add any windows; the two "adds" reported in the eval came only from the synthetic fixtures.

If priorShift is to contribute on real data, its band needs to overlap where real confidences actually live (probably in the low-to-mid 0.30s).

**Tracked as:** `playhead-gtt9.3`

## Finding 4 — Classifier-dominance; other evidence rarely fires

Evidence-source counts across 147 windows:

| Source | Count | % of windows |
|---|---|---|
| classifier | 147 | 100% |
| metadata | 10 | 7% |
| fm | 8 | 5% |
| lexical | 5 | 3% |
| catalog | 5 | 3% |
| acoustic | 3 | 2% |

The metadata-activation machinery NARL gates (`lexical`, `metadata` priors, `fm` scheduling) touches fewer than 10% of decisions. 90% of windows are decided by classifier-only evidence. If this is by design, NARL-flipping activation can't meaningfully move detection quality; if it's a symptom (sources not firing when they should), it's a separate quality issue.

Acoustic evidence is particularly underused — `MusicBedLevel` is tracked but fires 3 of 147 times. Ads have distinctive production; acoustic signal should plausibly be more active.

**Tracked as:** `playhead-gtt9.4`

## Finding 5 — Show-label heuristic misses real-world podcastIds

Eight of 11 non-vetoed episodes rolled up as `"unknown"` in the report because their `podcastId` is the full feed URL (e.g., `https://feeds.simplecast.com/dHoohVNH::311e3daa-60b3-4428-b780-c9a7b8512be8`). The heuristic at `NarlEvalHarnessTests.swift:390-405` only matches substring shapes like `"flightcast"`, `"conan"`, `"diaryofaceo"`. The URL-form podcastIds do contain those tokens but they're nested in URLs the heuristic chokes on.

Consequence: per-show rollups lose most of the real data, and the memory entry `project_ad_detection_weak_on_conan.md` (attributing detection weakness to comedy/conversational content) is based on unreliable show attribution.

**Tracked as:** `playhead-gtt9.5`

## Implications — redirection of effort

1. **Biggest lift is transcript completion, not classifier tuning.** 63 of 72 corrections evaporate if the transcript finishes. Finding 1 subsumes most of the "Conan is weak" narrative in the stale memory entry.
2. **Promotion-gap is small, specific, and testable.** Finding 2 gives a single bug with a single test case.
3. **NARL activation is a second-order lever.** Per finding 4, the thing NARL gates contributes <10% to decisions today. Even if priorShift is retuned (Finding 3), it's still a secondary knob.
4. **"Conan is weak on conversational content" is provisionally wrong.** The data instead says: pipeline stalls on long episodes regardless of show. Requires the show-label fix (Finding 5) and a re-evaluation to re-verify.

## Shadow data note

`shadow-decisions.jsonl` was 0 bytes in this bundle. Either the shadow capture pipeline was never activated in the dogfood build, no episodes were played (Lane A gates on `status == .playing`), or Lane B found no candidates. Shadow coverage remains blocked for the approval-policy `recommendFlip`/`holdOff` path; the approval recommender will continue to return `insufficientData` until shadow rows appear.

## Re-analysis

The capture bundle lives at `.captures/2026-04-23/`. To regenerate fixtures from it:

```bash
TEST_RUNNER_PLAYHEAD_BUILD_NARL_FIXTURES=1 \
TEST_RUNNER_PLAYHEAD_NARL_XCAPPDATA="/Users/dabrams/playhead/.captures/2026-04-23/com.playhead.app 2026-04-23 13:54.43.748.xcappdata" \
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath /tmp/narl-harness-dd \
  -only-testing 'PlayheadTests/NarlEvalCorpusBuilderTests'
```

Then run `NarlEvalHarnessTests` and `NarlApprovalIntegrationTests` to produce a fresh report.
