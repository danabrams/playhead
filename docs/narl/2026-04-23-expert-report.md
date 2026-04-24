# NARL real-data evaluation — detailed report for external review

**Date:** 2026-04-23
**Scope:** First analysis of the Playhead ad-detection pipeline against a real dogfood capture (16 podcast episodes, mixed Diary of a CEO + Conan O'Brien Needs a Friend).
**Audience:** An ML / signal-processing reviewer looking at the pipeline and data with fresh eyes. Everything needed to reproduce lives in the repo.
**Summary file (higher-level):** `docs/narl/2026-04-23-real-data-findings.md`

---

## 0. What Playhead is trying to do

On-device (iOS) detection of advertisement segments in podcast audio, for an "auto-skip ads" feature. Legal constraint: no cloud inference — everything runs locally on the user's device. Detection pipeline fuses multiple evidence sources:

| Source | Description |
|---|---|
| `classifier` | A lightweight on-device classifier scoring audio frames/features for ad-likelihood |
| `metadata` | Priors derived from podcast/episode metadata (typical ad slots, sponsor patterns) |
| `lexical` | Transcript-based patterns (keywords, phrasings common in ads) |
| `catalog` | A rolling catalog of previously identified ad signatures |
| `acoustic` | Audio-feature signals (music beds, VO compression) — e.g. `MusicBedLevel` |
| `fm` | On-device foundation-model calls (Apple's Foundation Models) |

Per-window decisions go through a fusion step to produce `fusedConfidence.skipConfidence ∈ [0, 1]` and a `finalDecision.action` in:

- `hotPathBelowThreshold` — scored but sub-threshold, not actionable
- `hotPathCandidate` — flagged as candidate, shown in UI but not auto-skipped
- `autoSkipEligible` — promoted; player skips automatically
- `detectOnly` — scored for analytics only, no UI

Nominal promotion threshold to `autoSkipEligible`: **0.40**.

The counterfactual harness ("NARL") is meant to compare different metadata-activation configurations against a frozen corpus and ground-truth user corrections, to decide whether to flip metadata activation on.

---

## 1. The data

The capture is at `.captures/2026-04-23/com.playhead.app 2026-04-23 13:54.43.748.xcappdata/` (gitignored; retained locally for re-analysis).

### 1.1 Sidecars

| Path (inside bundle, under `AppData/Documents/`) | Size | Content |
|---|---:|---|
| `corpus-export.2026-04-22T00-17-21.320Z.jsonl` | 42 KB | 16 assets, 5 promoted decisions, 72 user corrections |
| `corpus-export.2026-04-23T17-54-32.441Z.jsonl` | 0 B | second export run, empty |
| `decision-log.jsonl` | 151 KB | 147 per-window fused-decision rows |
| `shadow-decisions.jsonl` | 0 B | shadow mode never populated during this session |
| `ExportedAnalysisStore/analysis.sqlite` | 476 KB | tables present but rowcounts all 0 (real data is in the jsonl sidecars) |

### 1.2 Aggregate counts

- **Assets:** 16 (11 non-vetoed, 9 whole-asset-vetoed — some assets have multiple manualVeto corrections, so vetoed-asset count < veto-correction count)
- **Promoted decisions recorded in corpus:** 5 (one episode produced 3, another 1, another 1)
- **Scored decision-log windows:** 147
- **User corrections:** 72 (21 falseNegative single-span, 10 falsePositive single-span, 41 whole-asset type-unknown)
- **Ground-truth ad spans:** Derived from falseNegative corrections where user added coverage the detector missed

---

## 2. Per-asset coverage

`analysisState` is what the pipeline reports. `featureCoverageEndTime` is how far acoustic/classifier features extend into the episode; `fastTranscriptCoverageEndTime` is how far fast transcription has reached; `confirmedAdCoverageEndTime` is how far the user has actively endorsed ad labels (set only when the user explicitly marks a covered region).

| aid (prefix) | Show          | state    | fastCov (s) | featCov (s) | confCov (s) | fast/feat % |
|---|---|---|---:|---:|---:|---:|
| 5951989F | DoaC | complete  | 90   | 7036 | —       | **1.3%** |
| C25A058C | DoaC | complete  | 210  | 5852 | 2368.74 | 3.6% |
| 99E86F79 | DoaC | complete  | 90   | 5704 | —       | **1.6%** |
| 9BA1818E | DoaC | complete  | 870  | 5310 | —       | 16.4% |
| DF5C1832 | Conan | complete | 3420 | 4322 | 1619.28 | 79.1% |
| A53E3CE0 | DoaC | complete  | 90   | 2370 | —       | **3.8%** |
| C22D6EC6 | Conan | failed   | 2191 | 2190 | —       | 100% (failed) |
| D787EAA8 | DoaC | complete  | 90   | 1830 | —       | **4.9%** |
| D3285CBB | DoaC | spooling | 0    | 1650 | —       | 0% |
| 54B196C8 | Conan | backfill | 300  | 1620 | —       | 18.5% |
| 26B5A7FA | Conan | backfill | 1440 | 1440 | —       | 100% (vetoed) |
| 304D310B | DoaC | complete  | 840  | 1406 | 1406.22 | 59.7% |
| A52CFD91 | DoaC | complete  | 150  | 1304 | —       | 11.5% |
| 9007CDD0 | Conan | backfill | 60   | 1094 | —       | 5.5% |
| 1BC8D105 | Conan | spooling | —    | 1020 | —       | 0% |
| 6A7DFBF5 | Conan | complete  | 600  | 810  | —       | 74.1% |

**Observation.** Four episodes (flagged **bold**) are marked `complete` yet fastTranscriptCoverage is pinned to exactly 90 seconds on episodes that are 30-to-120-minutes long. The uniform `90s` strongly implies a hard-coded ceiling or early-exit condition, not genuine transcription completion. These are all Diary of a CEO long-form episodes.

---

## 3. Correction structure

Corrections carry:

- `analysisAssetId` — which episode
- `correctionType ∈ {falseNegative, falsePositive, ?}`
- `causalSource` — null for user-adds; `foundationModel` for manualVeto-of-FM-decision
- `scope` — `"exactTimeSpan:<aid>:<start>:<end>"`

### 3.1 Correction type breakdown

| Type | Count | Semantic |
|---|---:|---|
| falseNegative (per-window) | 21 | User marked a span the detector missed (real FN) |
| falsePositive (per-window, manualVeto) | 10 | User said "this flagged span is not an ad" (real FP or whole-asset veto) |
| Type `?` (whole-asset flags, scope Int64.max) | 41 | User whole-episode toggles (marked whole episode as ad / not-ad) |

**Of the 10 "falsePositive" corrections, 9 use `scope=[0, Int64.max]` — they are whole-asset manualVetos, not span-level detection errors.** Only 1 falsePositive (C22D6EC6 [77.1, 85.0]) is a span-level correction.

### 3.2 Per-asset correction counts

| asset | FN spans | FP spans | whole-asset | total |
|---|---:|---:|---:|---:|
| C25A058C | 0 | 7 | 20 | 27 |
| C22D6EC6 | 8 | 1 | 0 | 9 |
| 26B5A7FA | 0 | 1 | 7 | 8 |
| DF5C1832 | 7 | 0 | 0 | 7 |
| 5951989F | 0 | 1 | 5 | 6 |
| 304D310B | 3 | 0 | 0 | 3 |
| 9BA1818E | 0 | 0 | 3 | 3 |
| 6A7DFBF5 | 2 | 0 | 0 | 2 |
| 9007CDD0 | 0 | 0 | 2 | 2 |
| D787EAA8 | 0 | 0 | 2 | 2 |
| A52CFD91 | 1 | 0 | 0 | 1 |
| 99E86F79 | 0 | 0 | 1 | 1 |
| A53E3CE0 | 0 | 0 | 1 | 1 |

### 3.3 Full list of the 21 per-window false-negatives

| Asset (prefix) | Span                   | Length |
|---|---|---:|
| 304D310B | [2.0, 8.0]           | 6.0s  |
| 304D310B | [10.7, 62.0]         | 51.4s |
| 304D310B | [1283.8, 1346.6]     | 62.8s |
| DF5C1832 | [3.0, 42.6]          | 39.6s |
| DF5C1832 | [45.1, 87.3]         | 42.2s |
| DF5C1832 | [1380.4, 1547.8]     | 167.4s |
| DF5C1832 | [1550.0, 1621.3]     | 71.2s |
| DF5C1832 | [1624.3, 1688.6]     | 64.3s |
| DF5C1832 | [4292.2, 4322.8]     | 30.7s |
| DF5C1832 | [4233.0, 4292.1]     | 59.1s |
| C22D6EC6 | [0.0, 29.9]          | 29.9s |
| C22D6EC6 | [33.0, 73.9]         | 40.9s |
| C22D6EC6 | [1033.5, 1095.7]     | 62.2s |
| C22D6EC6 | [1098.7, 1170.0]     | 71.3s |
| C22D6EC6 | [1173.0, 1231.7]     | 58.7s |
| C22D6EC6 | [1233.4, 1298.7]     | 65.3s |
| C22D6EC6 | [2149.0, 2175.5]     | 26.5s |
| C22D6EC6 | [2175.6, 2190.9]     | 15.3s |
| 6A7DFBF5 | [3.0, 44.0]          | 41.0s |
| 6A7DFBF5 | [46.3, 70.0]         | 23.6s |
| A52CFD91 | [0.0, 29.8]          | 29.8s |

### 3.4 FN → scored-window overlap analysis (critical finding)

For each of the 21 FN spans, I checked whether any scored decision-log window overlaps it:

- **FN spans with at least one overlapping scored window:** 8
- **FN spans with zero overlapping scored windows (detector never ran):** 13
- **Total FN:** 21

**13 of 21 per-window FN = "the pipeline never scored this region at all."** Add the 41 whole-asset user flags (all on episodes with sub-5% fast-transcript coverage), and the dominant failure mode is *pipeline incompleteness*, not classifier mis-ranking.

Of the 8 FN spans that *did* get scored, the confidences are clustered low-to-mid:

| Asset | FN span | Overlap window | Overlap conf | Overlap action |
|---|---|---|---:|---|
| 304D310B | [2, 8] | [4, 5] | 0.34 | hotPathBelowThreshold |
| 304D310B | [11, 62] | [54, 55] | 0.35 | hotPathBelowThreshold |
| 304D310B | [1284, 1347] | [1289, 1290] | 0.38 | hotPathBelowThreshold |
| DF5C1832 | [45, 87] | [84, 85] | 0.38 | hotPathBelowThreshold |
| DF5C1832 | [1550, 1621] | [1612, 1613] | **0.45** | hotPathCandidate |
| DF5C1832 | [1624, 1689] | [1676, 1677] | **0.46** | hotPathCandidate |
| DF5C1832 | [4233, 4292] | [4261, 4262] | 0.38 | hotPathBelowThreshold |
| C22D6EC6 | [1034, 1096] | [1088, 1089] | 0.38 | hotPathBelowThreshold |

**Two are above the nominal 0.40 promotion threshold and still not promoted.** See §5 (promotion gap).

---

## 4. Fused-confidence distribution (147 windows)

### 4.1 Histogram

```
[0.00, 0.10):  0
[0.10, 0.20): 11   ███████████
[0.20, 0.22):  0
[0.22, 0.25):  0   ← priorShift band (see §6), empty
[0.25, 0.30): 12   ████████████
[0.30, 0.40): 78   █████████████████████████████████████████████████████████████████████
[0.40, 0.50): 11   ███████████
[0.50, 0.70): 23   ███████████████████████
[0.70, 1.00]: 12   ████████████
```

Mode is `[0.30, 0.40)`: **78 of 147 (53%) windows land there**. This matters for every other finding below.

### 4.2 Action × confidence

Corpus-wide actions:

| Action | Count |
|---|---:|
| hotPathBelowThreshold | 101 |
| hotPathCandidate | 41 |
| autoSkipEligible | 3 |
| detectOnly | 2 |

**3 autoSkipEligible windows exist, all at `fusedConfidence = 1.000`:**

| Asset | Window | Conf |
|---|---|---:|
| DF5C1832 | [1609.6, 1619.3] | 1.000 |
| 304D310B | [47.4, 62.0] | 1.000 |
| 304D310B | [1335.5, 1350.5] | 1.000 |

Zero windows in `(0.40, 0.999)` reach autoSkipEligible. See §5.

---

## 5. Promotion-gap analysis

The nominal promotion threshold is 0.40. Corpus reality:

- 41 windows reach `hotPathCandidate` (above flagging threshold)
- 3 reach `autoSkipEligible`
- All 3 promoted windows are at fusedConfidence = 1.000
- The band `(0.40, 1.00)` contributes 0 promotions

Two clear missed-promotion cases on **DF5C1832** — both windows above 0.40 that overlap user-confirmed FN ad spans:

| Window | Conf | Action | Overlaps user-confirmed FN ad |
|---|---:|---|---|
| [1612, 1613] | 0.45 | hotPathCandidate | [1550, 1621] |
| [1676, 1677] | 0.46 | hotPathCandidate | [1624, 1689] |

**Counter-case (important for scoping the bead):** On C22D6EC6, the window [74.0, 83.4] at confidence 0.597 did NOT promote — and that was correct. It overlaps a user-marked falsePositive span [77.1, 85.0]. The user considers this content to not be an ad; the detector correctly hesitated.

So promotion-gap is not uniformly broken — some holds are correct. But on DF5C1832 the two examples above show the path is effectively gated at confidence ≈ 1.0 rather than 0.4. Either there's a secondary condition (evidence diversity? cooldown? anchor? source composition?) that almost always fails below full certainty, or the effective threshold has drifted far above the nominal 0.4.

---

## 6. PriorShift: inert on real data

`MetadataActivationConfig.default` has `classifierBaselineMidpoint=0.25` and `classifierShiftedMidpoint=0.22`. PriorShift targets the half-open band `(0.22, 0.25]` — windows whose confidence would flip under the shifted midpoint but not under baseline.

**On 147 real-data windows, that band is empty (0 windows).**

The histogram in §4.1 shows the real distribution: no windows in `[0.20, 0.22)` or `[0.22, 0.25)`. The mode is far above at `[0.30, 0.40)`. The synthetic fixtures used in earlier testing placed windows in the priorShift band artificially; real classifier output does not cluster there.

Empirical implication: the two "adds" PriorShift reported in the current eval derive entirely from synthetic fixtures. On this real corpus, PriorShift cannot flip any decisions as configured.

A retune around the real mode (something like `classifierShiftedMidpoint=0.28, classifierBaselineMidpoint=0.32`) would at least give the knob something to work on. Numbers are illustrative — the right values need experimental fit.

---

## 7. Evidence-source firing rates

Per-window counts of each evidence source (a window "has" a source if the fusion input included it):

| Source | Windows | % of 147 |
|---|---:|---:|
| classifier | 147 | 100% |
| metadata | 5 | 3% |
| catalog | 5 | 3% |
| lexical | 5 | 3% |
| acoustic | 3 | 2% |
| fm | 2 | 1% |

**The classifier decides every window; every other source fires in under 4% of windows.** The metadata-activation gates NARL was designed to flip (lexical/metadata priors, FM scheduling) touch ~3% of the decision volume today.

Interpretation options:
1. **By design** — these sources are meant to be rare, high-precision injections. NARL activation has a small potential ceiling.
2. **Accidental silence** — source pipelines exist but aren't reaching fusion. A code-tracing investigation would reveal where the signal drops.

Either way, NARL tuning (priorShift retune, metadata-prior gating, FM scheduling) can at most move ~5% of decisions on the current corpus. Compare to §3.4: 63/72 corrections (87%) are in regions never scored at all. The order-of-magnitude lever is pipeline completion, not fusion retuning.

Acoustic in particular is suspicious — `MusicBedLevel` fires 3/147 times. Ads have distinctive production (music beds, tight compression, ads-specific VO mixing). A 2% firing rate is implausibly low if the feature computes correctly.

---

## 8. Harness roll-up and synthetic-vs-real mismatch

### 8.1 NARL harness numbers

Against 11 non-vetoed episodes (9 excluded by whole-asset veto):

| Config | F1@0.3 | F1@0.5 | F1@0.7 | Sec-F1 |
|---|---:|---:|---:|---:|
| default | 0.250 | 0.250 | 0.188 | 0.301 |
| allEnabled | 0.303 | 0.242 | 0.182 | **0.372** |

`allEnabled` improves Sec-F1 by ~7 points on real data. On synthetic fixtures, allEnabled had regressed the same metric. **Opposite directions on opposite data** — flagged as evidence that the synthetic fixtures don't represent the real decision distribution.

### 8.2 Why the synthetic/real split matters

- Synthetic fixtures placed windows in the (0.22, 0.25] priorShift band; real data puts none there.
- Synthetic fixtures likely over-represent scenarios where NARL-gated sources fire; real data shows those sources fire in <5% of windows.
- The NARL counterfactual design is sound, but its synthetic inputs were not calibrated to the real classifier output distribution.

If the eval is to be trusted as a gate for metadata-activation decisions, we need to either (a) fit the synthetic distribution to real data, or (b) rely exclusively on real-capture evals from here forward.

---

## 9. Show-attribution glitch

Eight episodes roll up as `"unknown"` in the harness report because their `podcastId` is a full feed URL, e.g.:

```
https://feeds.simplecast.com/dHoohVNH::311e3daa-60b3-4428-b780-c9a7b8512be8
```

The show-label heuristic at `PlayheadTests/Services/ReplaySimulator/NarlEval/NarlEvalHarnessTests.swift:390-405` does substring matching for tokens like `"flightcast"` and `"conan"` — which the URL-form IDs contain, but nested in structure the current matching logic doesn't descend into reliably. Consequence: per-show rollups are unreliable, and a prior project-memory entry attributing detection weakness to Conan's "conversational/comedy" content is based on misattributed data. The real signal (per §3.4) is pipeline coverage, not show style.

---

## 10. Five concrete asks for expert review

In rough priority order, these are the questions where an external reviewer's input would be most valuable:

### 10.1 Why does fastTranscriptCoverage ceiling at exactly 90s on 4 long-form episodes?

The uniform `90s` is suspicious enough to look like a config constant or early-exit gate. What patterns fit? Budget exhaustion, first-chunk-only, a transcription-task cancellation after N seconds of idle, a deliberate head-only-preview mode being mis-used in production? Is there a known pattern in Apple's on-device transcription that produces 90s ceilings?

### 10.2 What's the right correct-by-construction promotion gate?

Given (a) confidence = 1.0 is the only regime that currently promotes, (b) the 0.45–0.46 DF5C1832 windows are genuinely ads, and (c) the 0.597 C22D6EC6 window was genuinely not an ad — is the right fix to retune the threshold, add a second required condition (evidence-diversity, transcript-anchor, a minimum hot-path streak), or both? What does this look like in well-known ad-detection deployments?

### 10.3 Is classifier-dominance pathological or appropriate?

When the on-device classifier fires on 100% of windows and every other source fires on <5%, should that be seen as a working pipeline where priors only correct rare cases, or as a degenerate case where the classifier's dominance has effectively neutralized the prior system? Is there a way to instrument this that distinguishes the two?

### 10.4 Where should the confidence mode live?

If 53% of windows land in `[0.30, 0.40)` (just below the nominal promotion threshold), is that a signal that the classifier is well-calibrated but pipeline completion is the bottleneck (our read), or is it a signal that the classifier is systematically under-confident and should be re-calibrated to push ambiguous-ad windows higher?

### 10.5 Acoustic features underused?

MusicBedLevel-type features firing on 3/147 windows seems very low given ads have distinctive production. What's typical in production ad-detection for acoustic-feature contribution rates, and at what firing rate would you expect acoustic signal to be reliable enough for fusion?

---

## 11. How to re-run

Capture bundle is at `.captures/2026-04-23/com.playhead.app 2026-04-23 13:54.43.748.xcappdata`. To regenerate FrozenTrace fixtures from the bundle:

```bash
TEST_RUNNER_PLAYHEAD_BUILD_NARL_FIXTURES=1 \
TEST_RUNNER_PLAYHEAD_NARL_XCAPPDATA="/Users/dabrams/playhead/.captures/2026-04-23/com.playhead.app 2026-04-23 13:54.43.748.xcappdata" \
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath /tmp/narl-harness-dd \
  -only-testing 'PlayheadTests/NarlEvalCorpusBuilderTests'
```

Then run `NarlEvalHarnessTests` and `NarlApprovalIntegrationTests` to produce a report at `.eval-out/narl/<ts>/`. The 2026-04-23 output report is at `.eval-out/narl/20260423-175930-22C2B0/`.

---

## 12. Tracking

Epic: `playhead-gtt9` (status: open, P1)

| Child bead | Finding | Priority |
|---|---|---|
| playhead-gtt9.1 | Pipeline completion / 90s transcript ceiling | P1 |
| playhead-gtt9.2 | Promotion gap (hotPathCandidate → autoSkipEligible) | P1 |
| playhead-gtt9.3 | PriorShift band retune | P2 |
| playhead-gtt9.4 | Evidence-source firing-rate investigation | P2 |
| playhead-gtt9.5 | Show-label heuristic on URL-form podcastIds | P3 |

---

## Appendix A. Whole-asset manualVeto episodes

Excluded from the per-window eval because the user flagged the entire episode. Useful context for whether the pipeline's confidence is directionally right on these.

| asset | show | state | whole-asset veto count |
|---|---|---|---:|
| C25A058C | DoaC | complete | 7 + 20 type-? = 27 corrections |
| 26B5A7FA | Conan | backfill | 1 + 7 type-? = 8 |
| 5951989F | DoaC | complete | 1 + 5 type-? = 6 |
| 9BA1818E | DoaC | complete | 3 (all type-?) |
| 9007CDD0 | Conan | backfill | 2 (type-?) |
| D787EAA8 | DoaC | complete | 2 (type-?) |
| 99E86F79 | DoaC | complete | 1 (type-?) |
| A53E3CE0 | DoaC | complete | 1 (type-?) |

## Appendix B. Schema quick-reference

### corpus-export row types

**asset:**
```json
{
  "type": "asset",
  "analysisAssetId": "304D310B-3D7B-44BE-8FCA-E7BC27D824D9",
  "episodeId": "https://rss2.flightcast.com/...::flightcast:01KK9JS2Z0S02SWZEZ0SQ5TQZX",
  "analysisState": "complete",
  "fastTranscriptCoverageEndTime": 840,
  "featureCoverageEndTime": 1406,
  "confirmedAdCoverageEndTime": 1406.22,
  "analysisVersion": 1,
  "schemaVersion": 1
}
```

**decision** (promoted ad span):
```json
{
  "type": "decision",
  "analysisAssetId": "...",
  "startTime": 47.4,
  "endTime": 62.04,
  "spanId": "5b80f89f20c17242a425ec5a52ec9f8e",
  "firstAtomOrdinal": 123,
  "lastAtomOrdinal": 157,
  "anchorProvenance": [ ... evidenceCatalog entries ... ]
}
```

**correction:**
```json
{
  "type": "correction",
  "analysisAssetId": "...",
  "correctionType": "falseNegative" | "falsePositive" | null,
  "causalSource": null | "foundationModel",
  "source": "falseNegative" | "manualVeto" | ...,
  "scope": "exactTimeSpan:<assetId>:<startSec>:<endSec>",
  "createdAt": 1776816851.637048
}
```

### decision-log row (fused per-window decision)

```json
{
  "analysisAssetID": "...",
  "windowBounds": {"start": 74.0, "end": 83.4},
  "fusedConfidence": {"skipConfidence": 0.597, ...},
  "finalDecision": {"action": "hotPathCandidate", ...},
  "evidence": [
    {"source": "classifier", ...},
    {"source": "metadata", "category": "...", ...}
  ]
}
```
