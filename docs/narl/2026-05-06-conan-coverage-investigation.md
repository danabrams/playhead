# Conan transcript-coverage investigation (post-plumbing-fix)

**Date:** 2026-05-06
**Source run:** `.eval-out/narl/20260504-235504-79A299/report.json` (main @ `5d3d323f`)
**Predecessor doc:** `2026-04-24-finding1-conan-classifier-diagnosis.md` (which was diagnosed and shipped via gtt9.19/9.20/9.21)

## Why this doc

The 04-24 finding diagnosed Conan's then-Sec-F1=0 as a corpus-builder plumbing bug (`detectOnly` not counted as positive; pre-fix `hotPathCandidate` not promoted). All three follow-ups (gtt9.19/9.20/9.21) shipped. The current rollup still shows Conan ALL Sec-F1 = 0.185 — and `default` and `allEnabled` are bit-identical, because zero lexicalInjection/priorShift adds fire on Conan content. So the metadata-tuning lever has nothing to pull. The remaining bottleneck is pipeline coverage itself, and this doc characterizes it.

## Numbers

| Show / Config       | Episodes (incl) | Pipeline-failure rows | Sec-F1 | Precision | Recall | TP-sec | FN-sec |
|---------------------|-----------------|-----------------------|--------|-----------|--------|--------|--------|
| Conan / default     | 21              | 12                    | 0.1850 | 0.7945    | 0.1047 | 116    | 992    |
| Conan / allEnabled  | 21              | 12                    | 0.1850 | 0.7945    | 0.1047 | 116    | 992    |

Recall ~10% means roughly nine seconds of every ten ad-seconds slip through. TP/FN ratio (116:992) puts the ceiling: even a perfect classifier on the *covered* portion can only recover ~12% of ad-time without the pipeline running on more audio.

## Where the FN-sec actually lives

Per-episode breakdown of pipeline-coverage-failure assets (4 unique episodes in the corpus, captured 2026-04-24 and 2026-04-25):

| episodeId tail                      | duration | TP-sec | FN-sec | pipelineCoverageFNSec | transcriptCovRatio | scoredCovRatio |
|-------------------------------------|----------|--------|--------|-----------------------|--------------------|----------------|
| `…-46c0-ac34-67ec010bc50a`          | 71 min   | 11     | 467    | 238                   | 0.0023             | 0.0033         |
| `…-4428-b780-c9a7b8512be8`          | —        | 0      | 373    | 308                   | 0.0000             | 0.0094         |
| `…-461e-99ec-a3b5bc3c2598`          | —        | 3      | 121    | 121                   | 0.0000–0.1881      | 0.0000–0.1881  |
| `…-4ef3-b446-c9015ac4a770`          | —        | 0      | 65     | 65                    | 0.0000             | 0.0000         |

(`pipelineCoverageFailureAssetCount=12` in the rollup represents these 4 unique episodes counted once per asset/rule pair, not 12 distinct episodes.)

Two patterns:

1. **`transcriptCoverageRatio ≈ 0`**: the transcript pipeline never ran on the audio where the ad lived. The 71-min Conan episode (`…67ec010bc50a`) is the canonical case — atoms count = 0 in the fixture, despite the episode being captured for analysis. windowScores is non-zero (25 windows) but those windows are derived from acoustic/metadata signal sources, not transcript text.
2. **One episode (`…99ec010bc50a`) has split rows** with `transcriptCovRatio` ranging 0.0–0.19 across asset rules — suggesting partial transcript coverage that doesn't extend to the ad regions specifically. That's a coverage-shape problem (which 19% gets transcribed?), not a coverage-volume problem.

## Why the pipeline isn't running

The four failing fixtures were captured 2026-04-24 (3 of them) and 2026-04-25 (1). They predate:

- **`#106` in-process recovery observer for final-pass sweep** (post-2026-04-25)
- **`#108` per-shard cooperative thermal check in final-pass retranscription**
- **`#23` foreground transcript catch-up** (already in by capture time, but coverage shape suggests it didn't engage)

The fixtures also lack `buildCommitSHA` and `terminalReason` (gtt9.21 provenance fields), so we can't confirm exactly which detector binary produced them. But the captured-at dates put them at or before the work that explicitly targeted coverage-on-long-Conan-episodes.

This means **the rollup ceiling on Conan is currently dominated by coverage shape from a corpus that predates the relevant fixes**, not by any classifier-side limit.

## Recommendation

**Re-capture the four failing Conan episodes against current main** (eb4f2a7e+, post-#106/#108) before drawing further conclusions. Two outcomes possible:

1. **Coverage extends to the ad regions** under current code. Rollup recall jumps; the 0.185 ceiling rises substantially. The detection chain isn't broken, only the corpus is stale.
2. **Coverage still stalls at ~0.** Then there's a real on-device budget/thermal limit on long-form Conan-class episodes. That's a product bead — pick one of:
   - Tighten budget allocation specifically for ≥60-min episodes (per-show duration awareness)
   - Wire a structured "transcribe-on-charging" pass that doesn't compete with the foreground budget
   - Accept the recall ceiling on long episodes and surface uncertainty in the UI rather than chasing F1

Either way, **don't tune classifier or fusion thresholds against this rollup** — the FN-sec it reports is upstream of detection.

## What's actionable today (no recapture)

- The four failing fixtures' `episodeId` and capture dates are now logged here. Future rollups can compare against this list to confirm whether they're still failing.
- The `transcriptCoverageRatio` field is the right canary. When it rises above ~0.5 on these specific episodes, the recall ceiling will rise with it.
- Excluded episodes (7 Conan, per rollup) are a separate filter from pipeline-coverage failures — verify they're being dropped for the right reasons (e.g., GT empty + Pred empty → F1=undefined → excluded).

## Caveats

- Fixture episode counts vs. rollup "episodeCount" don't match cleanly (28 unique episode-config pairs vs. rollup's 21). The rollup applies an additional filter that this doc doesn't disambiguate. The 4 failing episodes are not affected.
- I did not re-run the harness or capture new fixtures. The diagnosis is from the existing report.json plus a single fixture spot-check.
- "Partial transcript coverage" inferred from one episode's split-rule rows is suggestive, not conclusive. A targeted recapture would clarify.
