# NARL real-data eval findings — 2026-04-24

Run `20260424-013940-CAC7EA` (run dirs `8A0A7C`/`E4651A`/`335A1F` are identical test-runner duplicates). Fixtures span four date-dirs: `2026-04-22/` (synthetic), `2026-04-23/` (17:39 bundle), `2026-04-23-1354/` (13:54 bundle), `2026-04-24/` (**new: 21:34 bundle**).

## Source data

- **Capture bundle:** `.captures/2026-04-23/com.playhead.app 2026-04-23 21:34.36.596.xcappdata/`
- **New fixtures:** `PlayheadTests/Fixtures/NarlEval/2026-04-24/` — 19 FrozenTrace files
- **Key sidecars now populated for the first time:**
  - `asset-lifecycle-log.jsonl` — 8 rows, 2 sessions, schema v1 (gtt9.8 telemetry live)
  - `shadow-decisions.jsonl` — **150 rows** (was 0 bytes in prior captures)
- Four `corpus-export.*.jsonl` files; newest (`2026-04-24T01:34:27Z`) adds 3 assets + 4 decisions + 12 corrections.

## Headline

| Show | Config | Sec-F1 | AutoSkip Prec | AutoSkip Recall | ScoredCov | ShadowCov |
|---|---|---|---|---|---|---|
| ALL | allEnabled | **0.397** | 0.263 | 0.096 | 0.044 | 4/31 |
| ALL | default | 0.319 | 0.274 | 0.095 | 0.044 | 4/31 |
| Conan | allEnabled | 0.221 | — | — | 0.056 | 2 |
| Conan | default | 0.195 | — | — | 0.056 | 2 |
| DoaC | allEnabled | 0.446 | — | — | 0.028 | 2 |
| DoaC | default | 0.496 | 0.385 | 0.146 | 0.028 | 2 |

`allEnabled` wins ALL (+7.8 pts) and Conan (+2.6 pts); loses DoaC (-5.0 pts). Consistent with the 17:39 result, not a fluke.

## Finding 1 — 9.1.1 works, but classifier+promotion still misses

Flagship episode `71F0C2AE` (Conan 117-min, episodeId `…flightcast:01KM20WJPKVFHRVJZWTNA6Q1XT`) completed with `terminalReason: "full coverage: transcript 1.000, feature 1.000"` in the lifecycle log — the 90s ceiling is *gone*. But the harness scored it **GT=3, Pred=0, Sec-F1=0**. Detector missed all 3 user-marked ads despite full transcription.

First real measurement of detection quality with transcript coverage as a non-issue: classifier+promotion+fusion still fail on a real 2-hour show. This is the next ceiling.

## Finding 2 — gtt9.5 + gtt9.7 delivered on signal

- **Per-show rollups work.** Zero `"unknown"` entries; 18 Conan / 13 DoaC episodes bucketed correctly (9.5).
- **Whole-asset vetoes cleanly excluded.** 9 `wholeAssetVeto` exclusions listed with reasons in the report (9.7). Span-level precision/recall no longer polluted by asset-level corrections.
- **Normalizer counts persisted.** Report JSON now carries `NarlNormalizerCounts` per episode with Codable back-compat.

## Finding 3 — Lifecycle telemetry NOT in FrozenTrace fixtures

Inspection of the two new 21:34 fixtures:

```
71F0C2AE: durationSec=None, analysisState=None, terminalReason=None, fastTranscriptCoverageEndTime=None, featureCoverageEndTime=None
34C7E7CF: same — all None
```

gtt9.8 writes `terminalReason` + coverage fields to `asset-lifecycle-log.jsonl` and the live `AnalysisStore`, but the **NARL corpus builder doesn't thread those fields into FrozenTrace JSON**. The harness therefore can't use 9.8 telemetry for pipeline-coverage classification. This is a follow-up bead.

## Finding 4 — Shadow coverage unblocked but still sparse

`totalEpisodesWithShadowCoverage: 4` (was 0). `shadow-decisions.jsonl` now populates on dogfood builds (`allEnabledShadow` variant, laneA capture). Approval-policy recommender will still return `insufficientData` until more sessions accumulate — but the pipeline is no longer inert.

## Finding 5 — Auto-skip user-facing numbers are the real target

| Config | AutoSkip Prec | AutoSkip Recall | SegmentIoU |
|---|---|---|---|
| default | 0.274 | 0.095 | 0.079 |
| allEnabled | 0.263 | 0.096 | 0.064 |

Precision ~27% means roughly 3 in 4 auto-skips are wrong from the user's perspective. Recall ~10% means most ads still leak through. This is the product-ceiling metric. F1/Sec-F1 improvements only matter insofar as they move these two.

## What still stalls: partial-coverage case (gtt9.14 evidence)

`34C7E7CF` (15-min episode): reached `backfill`, stopped at `tx=840, ft=726`, no terminal row in the lifecycle log — consistent with the foreground-paused-while-loaded hypothesis (gtt9.14). The scheduler gates on `activePlaybackEpisodeId != nil` but can't tell that playback is paused.

## Next-bead candidates (flagged for consideration)

1. **Classifier/promotion investigation on 71F0C2AE** — why did a 117-min fully-transcribed Conan episode yield 0 predictions against 3 ground-truth ads? Likely a follow-on spike under gtt9.4 (which already found: MusicBedLevel disconnected; metadata gates shipped off) — but now with a specific reproducible full-coverage test case.
2. **Corpus-builder extension** — thread `terminalReason`, `analysisState`, `durationSec`, `transcriptCoverageEndSec`, `featureCoverageEndSec` into FrozenTrace schema so the harness can classify pipeline-coverage failures against lifecycle truth (not just evidence-ledger inference).
3. **gtt9.14** — the 34C7E7CF stall is exactly what the bead anticipates. Good validation signal when 9.14 ships.

## Aggregate coverage caveat

`ScoredCoverageRatio = 0.044` across ALL fixtures is a mixed number: most fixtures are from pre-9.1.1 captures where transcript stalled at 90 s. The post-9.1.1 subset (2 assets in the new fixtures) is insufficient to compute a clean "post-fix ScoredCov" number. Will improve as more dogfood sessions accumulate under the current build.
