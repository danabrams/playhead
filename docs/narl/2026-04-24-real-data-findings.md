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

---

## 2026-05-04 — gtt9.4.3 follow-up: post-fix capture verification on 71F0C2AE

The 04-24 finding above ("Pred=0, Sec-F1=0") was captured **before** two fixes that targeted exactly this case:

- **6e37335** (2026-04-23 22:29 EDT) — `emitHotPathDecisionLogs` promotes `hotPathCandidate` → `autoSkipEligible` when `adProbability ≥ 0.80`.
- **a4ea4ca** (2026-04-23 23:28 EDT) — `makeClassifierProposals` seeds classifier-only high-conf regions so they reach BackfillEvidenceFusion.

Captured a fresh dogfood session on 2026-04-25 (fixture `2026-04-25/FrozenTrace-71F0C2AE-7260-4D1E-B41A-BCFD5103A641.json`, `capturedAt: 2026-04-25T11:50:03Z`) and re-ran the harness on 2026-05-04 against current `main` (HEAD `cde03285`).

### Per-fixture comparison (episode `…flightcast:01KM20WJPKVFHRVJZWTNA6Q1XT`)

| Fixture | Config | GT | Pred | Sec-F1 | Precision | Recall | hasShadowCoverage | priorShiftAdds | 7006-7037 GT span FN reason |
|---|---|---|---|---|---|---|---|---|---|
| 2026-04-24 (pre-fix) | default | 3 | 2 | 0.7612 | 1.000 | 0.6145 | false | 0 | "no scored windows overlap this GT span" |
| 2026-04-24 (pre-fix) | allEnabled | 3 | 2 | 0.7612 | 1.000 | 0.6145 | false | 0 | "no scored windows overlap this GT span" |
| **2026-04-25 (post-fix)** | default | 3 | 2 | 0.7612 | 1.000 | 0.6145 | **true** | 0 | **"candidate windows present but none were promoted to auto-skip"** |
| **2026-04-25 (post-fix)** | allEnabled | 3 | 3 | 0.7500 | 0.9623 | 0.6145 | **true** | **4** | **"candidate windows present but none were promoted to auto-skip"** |

Note: the 04-24 row above ("Pred=0, Sec-F1=0") in Finding 1 was a **different fixture run** with different captured trace data; the harness numbers in this comparison table come from the per-fixture episodes in the harness output, not the rollup. Both pre/post-fix fixtures share the same GT (3 user-marked ads, including `[7006, 7037.34]`) but differ in their captured FrozenTrace contents.

### What changed

1. **a4ea4ca delivered.** Pre-fix capture had **zero scored windows** overlapping the [7006, 7037.34] GT span (FN reason: "no scored windows"). Post-fix capture now has **candidate windows present** in that range (FN reason changed to "candidate windows present but none were promoted to auto-skip"). Classifier-only seeding successfully reached fusion.
2. **6e37335 partially delivered.** Hot-path candidates now exist in the borderline span, but they don't clear the `autoSkipEligible` threshold on this episode. The promotion fix raised the floor (more candidates) without raising recall (still 0/1 on the third GT span).
3. **Sec-F1 movement is essentially zero on 71F0C2AE.** Default-config Sec-F1 stayed at 0.7612 (unchanged). allEnabled went 0.7612 → 0.7500 (−1.1pt, one extra prediction in a new region added 2s of FP).
4. **isAdUnderDefault @ 7006-window:** still false post-fix. Pred count under default config did **not** flip from 2 → 3. Candidate generation works; promotion does not.

### Acceptance gate evaluation

| Acceptance | Status |
|---|---|
| Fresh fixture exists with `capturedAt` after 2026-04-24 03:28 UTC for 71F0C2AE | ✅ `capturedAt: 2026-04-25T11:50:03Z` |
| Harness re-run shows whether labelling fix alone moves Sec-F1 | ✅ Answered: it does not — it moves _candidate coverage_ but not _promotion_, leaving Sec-F1 flat |
| Decision: what's the actual next ceiling? | ✅ Promotion-gate score for classifier-seeded candidates — see correction below |

### Correction: 9.4.1 was already live in this fixture

**Previously this section concluded "9.4.1 (boundary expansion) remains the right next move." That was wrong.** `playhead-gtt9.4.1` shipped at merge `d3bb935` on 2026-04-24 11:22 EDT — ~20 hours before the 04-25 capture (`capturedAt: 2026-04-25T11:50:03Z`). Boundary expansion was already in production for the post-fix fixture. So the "candidate windows present but none promoted" FN happens **with 9.4.1 already running**, not as a problem 9.4.1 would fix.

What 9.4.1 actually does is expand the persisted `AdWindow.[startTime, endTime]` outward to the nearest `AcousticBreak` *after* the AdWindow has been admitted. The unpromoted candidates in the [7006, 7037.34] span never become AdWindows in the first place — they fail the upstream promotion gate (`adProbability < 0.80` after fusion). 9.4.1's logic doesn't apply to them.

### Actual next ceiling: promotion-gate score on classifier-seeded candidates

The remaining bottleneck is upstream of 9.4.1: classifier-only seeded candidates (a4ea4ca) that reach BackfillEvidenceFusion but score below the auto-skip eligibility threshold. Real next moves (none of these are filed beads yet):

1. **Score-boost path** — give classifier-seeded candidates additional fusion evidence (acoustic-break alignment bonus, transcript-coverage confidence) so borderline regions can clear 0.80 without lowering the global threshold.
2. **Second-tier promotion path** — route classifier-seeded candidates through a separate eligibility track with looser thresholds but tighter precision controls (e.g., require both classifier ≥0.70 AND acoustic-break alignment).
3. **Threshold tuning** — lower the global auto-skip threshold under specific conditions (high-trust shows, fully-transcribed episodes). Bigger architectural move; risks precision regression.

Filed today as follow-up beads. The doc above is preserved for the diagnostic value of "we observed the FN-reason change and that ruled out scored-coverage as the bottleneck on this episode."

### Caveats / honest limits

- **Single-episode result.** This is one episode. Network-wide Sec-F1 movement under `allEnabled` from the harness rollups (8 included DoaC episodes) is also flat-to-slightly-negative (DoaC `default` Sec-F1 = 0.748, `allEnabled` = 0.682 in the 2026-05-02 run; same harness, same fixtures). The fixes' value is in unblocking subsequent work, not in standalone metric movement.
- **`buildCommitSHA` is empty in fixtures** — couldn't programmatically verify the post-fix capture was taken under a build with both commits, only that `capturedAt` postdates them. The behavioral changes (shadow coverage flipping `true`, priorShiftAdds going 0 → 4, FN reason text change) are sufficient indirect evidence the fix code ran.
- **Same harness, same prediction code under both replays.** The fixture differences are purely in the captured trace inputs; the prediction logic at HEAD is identical for both. So the metric differences reflect "how the new code's outputs flow through replay," not "how new code differs from old." This is the correct setup for measuring fix-induced upstream signal.

### Provenance

- Harness run: `.eval-out/narl/20260504-174000-93AB6B/` (current `main` HEAD `cde03285`).
- Episodes inspected: indices 34/35 (pre-fix capture, no shadow), 90/91 (post-fix capture, shadow coverage live).
- Bead: `playhead-gtt9.4.3` — closes with this finding.
- Follow-up beads filed 2026-05-04:
  - `playhead-fqc8` (P1) — Promotion-gate score path for classifier-seeded candidates (the actual remaining ceiling on this episode).
  - `playhead-d56i` (P2) — FrozenTrace lifecycle threading (corpus-builder extension named in Finding 3).
