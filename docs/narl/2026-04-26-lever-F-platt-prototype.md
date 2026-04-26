# Lever F — Platt-scaling calibration prototype (2026-04-26)

Bead: `playhead-2xkw`. Research/prototype only — no production code touched.

## Tl;dr

**Insufficient labeled data** to make a confident productionization call today. With the corpus we have (45 user-marked spans across 10 assets; 41 false-negative + 4 false-positive), Platt scaling on the raw classifier output **does not improve ranking quality** (AUC-PR drops from 0.43 raw → 0.39 calibrated under 5-fold CV with implicit-negative augmentation). However the **reliability table gives us the diagnostic the bead asked for**: the raw classifier output is poorly calibrated in a way that explains the 2026-04-25 NARL precision/recall imbalance, and a Platt fit identifies a sensible **raw-threshold remap (~0.61 → calibrated 0.50)** that would cut false positives without yet enough data to verify what it does to recall.

**Recommendation:** *Do not productionize yet.* Capture more labels (target ≥150 windows with ≥30 explicit positives across ≥10 distinct assets, ideally with explicit non-ad spans not just `manualVeto`-vs-implicit), then re-run. When productionizing, calibrate **post-fusion** and **globally** initially; per-show only after per-show n ≥ 30. See "Where calibration goes" below.

## Data source and sample size

Source: every `corpus-export.*.jsonl` and `decision-log*.jsonl` under `.captures/2026-04-{23,25}/com.playhead.app *.xcappdata/AppData/Documents/`. Pair construction in `/tmp/platt-proto/build_pairs.py`; analysis in `/tmp/platt-proto/run_platt.py`.

| Quantity | Count |
|---|---|
| Unique corrections (all scopes) | 95 |
| `exactTimeSpan` corrections (parseable as window labels) | 45 |
| &nbsp;&nbsp;&nbsp;&nbsp;false-negatives (label=1 / "this IS an ad") | 41 |
| &nbsp;&nbsp;&nbsp;&nbsp;false-positives / `manualVeto` (label=0) | 4 |
| Assets reviewed (≥1 correction) | 10 |
| Assets with `analysisState=='completeFull'` and ≥1 correction | 3 |
| Unique decision-log windows (asset × bounds) | 443 |
| Decision-log lines (raw, including replays) | 11,550 |
| **Explicit (clf, label) pairs** (window overlaps a user span) | **27** (25 pos / 2 neg) |
| **Implicit negatives** (window in reviewed asset, no overlap) | **378** |

The 50 corrections we couldn't use have `scope` form `exactSpan:<asset>:0:9223372036854775807` — i.e. whole-asset bulk markers covering the entire episode. They tell us the user thinks the asset has *some* unscored ads but don't bound which seconds; they can't pin a label to a specific decision window.

**Headline:** explicit positives are 25, explicit negatives are 2. Without implicit augmentation the corpus is degenerate (~93% positive). With implicit-neg augmentation we have 405 examples but the negatives are inferred, not user-attested.

This is below the bead's "Platt scaling works with dozens of examples" threshold for an explicit-only fit. The N<50 prototype-inconclusive flag applies to the explicit-only configuration. The implicit-augmented configuration has N=405 but adds an assumption.

## Three configurations evaluated

All use 5-fold stratified cross-validation with pooled out-of-fold predictions (a single 80/20 split is unstable at this N).

| Config | Description | N | Pos | Neg |
|---|---|---|---|---|
| A. Explicit-only | Only user-marked spans; pure-positive-skew | 27 | 25 | 2 |
| B. Implicit-augmented (CF only) | Implicit-negs limited to `completeFull` assets | 202 | 8 | 194 |
| C. Implicit-augmented (all reviewed) | Implicit-negs from any asset with ≥1 correction | 405 | 25 | 380 |

Config C is the one we report headline numbers from. Config B is the strictest (only assets the user definitively reviewed end-to-end). Config A is included to show that user-corrections alone are too one-sided to fit calibration.

## Pre vs post precision/recall at 4 thresholds (Config C, 5-fold OOF)

```
   thr   raw P   raw R   platt P   platt R   iso P   iso R
  0.50   40.0%   32.0%    44.4%    16.0%   71.4%   20.0%
  0.70   80.0%   16.0%    60.0%    12.0%   33.3%    4.0%
  0.85    -       0.0%    50.0%     4.0%   33.3%    4.0%
  0.95    -       0.0%      -       0.0%   50.0%    4.0%
```

`-` means no windows scored at or above the threshold (precision undefined).

**AUC-PR (test, OOF-pooled):** raw 0.426 / platt 0.385 / isotonic 0.374.

**Read of the table:** the raw classifier already separates ads from non-ads better than the Platt-mapped version *as a ranking* — the calibrated probabilities are pessimistic-shifted (Platt fit's intercept −5.4, slope +8.9 maps raw 0.61 → calibrated 0.50, so most raw scores are pushed below 0.5). At threshold 0.7 raw precision is 80% / recall 16%; calibrated precision drops to 60% with similar recall. Calibration is hurting, not helping, ranking — small-N over-fitting plus heavy class imbalance.

For comparison, **Config B** (completeFull only) gave: AUC-PR raw 0.505 / platt 0.383 / iso 0.296 — same conclusion, even more pronounced.

**Config A** (explicit-only) hit AUC-PR raw 0.945 / platt 0.836 / iso 0.896 — but that's measuring a degenerate problem (92.6% prior, almost everything is positive); useless for setting an auto-skip threshold.

## Reliability tables (10 bins, full corpus, Config C)

### Raw classifier scores

```
        bin   n   mean_score   frac_pos
[0.00,0.10)   1     0.000       0.000
[0.10,0.20) 152     0.121       0.026
[0.20,0.30) 156     0.226       0.000
[0.30,0.40)  65     0.335       0.138    <-- mode of 2026-04-25 eval
[0.40,0.50)  11     0.454       0.364
[0.50,0.60)  11     0.554       0.182
[0.60,0.70)   4     0.634       0.500
[0.70,0.80)   1     0.723       1.000
[0.80,0.90)   4     0.819       0.750
[0.90,1.00)   0      -           -
```

A perfectly calibrated classifier would have `frac_pos ≈ mean_score` in each bin. The raw classifier is roughly calibrated below 0.30 (frac_pos near 0) and above 0.70 (frac_pos high), but **inverts in the middle**: bin [0.50, 0.60) has *lower* positive fraction (0.18) than [0.40, 0.50) (0.36). That's the smoking gun the bead predicted: the threshold region is the noisiest part of the raw score's calibration curve.

The high-density [0.30, 0.40) bin contains 65 windows but only 13.8% are actually ads — i.e., a raw 0.35 means roughly "~14% chance this is an ad", far below face value.

### Platt-calibrated scores

```
        bin   n   mean_score   frac_pos
[0.00,0.10) 355     0.028       0.020    <-- 88% of corpus collapses here
[0.10,0.20)  25     0.128       0.320
[0.20,0.30)   8     0.261       0.250
[0.30,0.40)   2     0.331       1.000
[0.40,0.50)   8     0.455       0.125
[0.50,0.60)   1     0.545       1.000
[0.60,0.70)   1     0.666       0.000
[0.70,0.80)   1     0.732       1.000
[0.80,0.90)   4     0.864       0.750
[0.90,1.00)   0      -           -
```

The Platt fit produces a single sigmoid, parameterized as `P(ad) = sigmoid(8.9 * raw − 5.4)`. The crossover point (raw → 0.5 calibrated) is at **raw ≈ 0.61**. Calibrated bins above 0.7 look directionally right (frac_pos ≥ 0.75 at calibrated ≥ 0.80) but the [0.10, 0.50) range is non-monotonic (0.32 → 0.25 → 1.00 → 0.12) — sample sizes there are 1–8, statistically noisy.

The headline scalar from the fit: **raw 0.61 corresponds to calibrated 0.50**. If we wanted to set an auto-skip threshold "P(ad) ≥ 0.85 calibrated", the equivalent raw cut is **0.71** under this fit. Today the system effectively requires raw ≈ 1.0 (per Finding 2 of the 2026-04-23 NARL real-data report — promotion path behaves like a hard 1.0 threshold). A calibration layer reframes that hard cut in interpretable terms but does not by itself change the underlying ranking that AUC-PR measures.

## Distribution shift versus the 2026-04-25 NARL eval

The 2026-04-25 eval reported confidence mode `(0.30, 0.40)` covering 53% of windows. Our corpus shows mode `(0.20, 0.30)` (38% of windows) followed closely by `(0.10, 0.20)` (37%). The discrepancy is because we pool every replayed decision across 18 assets and 5 capture bundles, while the eval scored a specific 11-asset cohort. The 0.30–0.40 mode the eval reported is consistent with a sub-cohort skewed toward moments the user actually played (which are more likely to contain ads or near-ads).

## Recommendation

### Productionize now? **No.**

Three reasons:

1. **Sample size.** 27 explicit pairs (only 2 explicit non-ad) is too thin for Platt scaling to fit a meaningful slope/intercept that generalizes. The implicit-augmented N=405 inflates apparent precision but conflates "user accepted" with "user did not notice / had not yet reviewed." Until the corpus has ≥30 explicit positives and ≥30 explicit negatives across ≥10 assets — and ideally explicit `manualVeto` deletions are joined with explicit accept-confirmations rather than implicit-not-corrected — a fitted calibrator is more likely to memorize noise than to generalize.
2. **Calibration is currently *hurting* ranking, not helping.** AUC-PR drops from 0.43 → 0.39 under both Platt and isotonic. The raw classifier already ranks better than either calibrated mapping at this N. There's no point rolling out a calibration layer that lowers AUC-PR.
3. **The 2026-04-25 eval's precision/recall imbalance is mostly a *threshold-and-promotion* problem, not a *calibration shape* problem.** Per the 2026-04-23 NARL findings, the promotion path effectively requires fused confidence ≈ 1.0, and the priorShift band is empty on real data. Those bugs (gtt9.2, gtt9.3) are upstream of calibration; fixing them moves more windows into the band where a calibration layer would matter. Calibrate after they land.

### What we did learn (worth recording)

- The raw classifier output is **roughly monotonic but locally non-monotonic** in the [0.40, 0.60) range. A calibration layer of *some* form (probably isotonic, which respects monotonicity strictly) would be appropriate eventually.
- Today the auto-skip threshold equivalent to "calibrated P(ad) ≥ 0.85" lives at **raw ≈ 0.71** under our current fit. That is a defensible heuristic for a *threshold* tweak (`gtt9.3`) without rolling out a full calibration layer — set the auto-skip raw threshold around 0.71 and watch precision.
- Implicit-negative reasoning is dangerous because the user does not bulk-reject every non-ad window — they're silent on "this is fine." Future correction-capture work should make accept-confirmations easier (a tap-to-confirm-non-ad UI) so the negative class is real, not inferred.

### Where does calibration go (when we do it)?

**Pre-fusion vs post-fusion: post-fusion.**
- Pre-fusion would mean replacing the classifier's raw probability before it enters fusion. That assumes the fusion logic was tuned for *calibrated* inputs, which it wasn't — fusion currently consumes the raw classifier in known idiosyncratic ways (capApplied=0.3 weak, etc.). Re-calibrating the input would silently shift fusion behavior in non-obvious ways across the other evidence channels.
- Post-fusion calibrates the final fused confidence — exactly the number that gates `AutoSkipPrecisionGate`. This is the simpler intervention point: it does not perturb fusion's internal weighting, it produces a number whose threshold is interpretable ("P(this skip is correct) ≥ 0.85"), and it is straightforward to A/B against the current threshold.

**Per-show vs global: global to start.**
- Even global calibration is undersaturated at 27 explicit positives. Per-show calibration would need ≥30 corrections per show before its slope/intercept are stable; we currently have at most 9 corrections on any single asset (`AA8DCCA6`, 9 spans).
- Per-show only makes sense once we observe that one global fit produces persistently miscalibrated bins for a specific show. We do not have enough data to even diagnose that yet.

**Online vs offline: offline batch refit.**
- Refit on a schedule (e.g., weekly) when ≥N new corrections have arrived since the last fit. Online refit on every correction would chase noise.
- Each refit must be tied to `detectorVersion` + `buildCommitSHA` (gtt9.21 stamps both into FrozenTrace). When detector version changes, the calibration must be re-fitted on data captured under the new version — never apply old calibration to a new model.

## Re-running this prototype

```bash
python3 /tmp/platt-proto/build_pairs.py     # rebuilds pairs from .captures
python3 /tmp/platt-proto/run_platt.py --all-assets --cv   # Config C (headline)
python3 /tmp/platt-proto/run_platt.py --cv                # Config B (CF only)
python3 /tmp/platt-proto/run_platt.py --no-implicit --all-assets --cv  # Config A
```

Both scripts live under `/tmp/platt-proto/` and depend only on `numpy` + `scikit-learn`. They do not touch the Playhead source tree.

## What would unblock a confident productionization call

1. **More explicit corrections of *both* polarities.** A capture with ≥30 positive and ≥30 negative spans, ideally on ≥10 distinct assets across ≥3 shows. The `manualVeto`-as-negative class needs to grow well past 4.
2. **Promotion-path bug fixed (gtt9.2).** Otherwise calibration is operating on a sub-corpus heavily filtered by the broken promotion logic.
3. **Re-run the NARL eval against the same fixtures used here** so we can compare AUC-PR pre and post calibration on a held-out evaluation set, not just the labeled-correction corpus.

When all three are in place: re-fit, expect AUC-PR to start exceeding raw, then ship as a post-fusion global calibrator with version-aware refit.
