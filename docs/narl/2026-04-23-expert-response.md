# NARL expert review response — 2026-04-23

External expert review of `2026-04-23-real-data-findings.md` + `2026-04-23-expert-report.md`. Captured verbatim for durable reference. Action items are tracked as P1/P2/P3 below; see end of doc for bead mapping.

## Bottom line

> I would not start by tuning the classifier. The report points to a more basic failure: large parts of the episode are not being analyzed at all.

Strongest improvement path:

1. Fix pipeline completion and scoring coverage.
2. Make promotion gating explainable and segment-level.
3. Wake up the non-classifier evidence sources, especially acoustic and lexical.
4. Retune priors only after the real-data distribution is understood.
5. Change the eval so it separates "unscored" from "scored but misclassified."

Key evidence: **13 of 21 FN spans had zero overlapping scored windows**, and **41 whole-asset flags mostly occur on episodes with very low fast-transcript coverage**. Dominant failure mode is not "the model saw an ad and missed it"; it is "the detector never ran on the relevant region."

---

## 1. Fix coverage before tuning confidence

### The pattern

Several long episodes marked `complete` with `fastTranscriptCoverageEndTime = 90s` while feature coverage extends thousands of seconds. The uniform `90` value is suspicious — points to a hard-coded cap, early cancellation, preview/head-only mode, or state-machine bug.

### Coverage contract

`analysisState = complete` only if:
- feature scoring has covered the intended audio range,
- decision windows have been emitted for that range,
- transcript-dependent sources are either complete or explicitly marked partial/unavailable.

Replace the monolithic `complete` with:

```
completeFeatureOnly
completeTranscriptPartial
completeFull
failedTranscript
failedFeature
cancelledBudget
waitingForBackfill
```

### Coverage telemetry

Per asset:

```json
{
  "assetId": "...",
  "audioDuration": 7036,
  "featureCoverageEnd": 7036,
  "transcriptCoverageEnd": 90,
  "scoredCoverageEnd": 90,
  "analysisState": "completeTranscriptPartial",
  "terminalReason": "transcriptionBudgetExceeded",
  "unscoredRanges": [[90, 7036]]
}
```

Per transcript/scoring job:

```json
{
  "assetId": "...",
  "chunkStart": 90,
  "chunkEnd": 180,
  "jobState": "cancelled",
  "reason": "budgetLimit | appBackgrounded | timeout | noAudio | speechUnavailable | unknown"
}
```

Turn "coverage mysteriously stopped" into a specific reason.

### Investigation targets

Search for: `90`, `90.0`, `maxDuration`, `preview`, `headOnly`, `firstChunk`, `initialWindow`, `transcriptBudget`, `fastTranscriptCoverageEndTime`, `analysisState = .complete`. Check whether the transcript worker is using a preview path for long-form.

---

## 2. Make the detector run even when transcript is incomplete

Ad detection should have a **feature-only baseline path** that scans the whole episode using cheap local signals.

```
Full-audio cheap pass
  ├─ classifier score
  ├─ acoustic features
  ├─ boundary/change-point detection
  └─ metadata time-position prior
Candidate generation
  ├─ likely pre-rolls / mid-rolls / post-rolls
  └─ abrupt production-style changes
Expensive refinement
  ├─ transcript lexical pass
  ├─ FM classification
  ├─ catalog matching
  └─ promotion decision
```

Transcript should improve decisions, not determine whether the region gets scored at all.

First-pass signals that don't require transcript:

| Signal | Use |
|---|---|
| Time-position prior | Pre-rolls, mid-rolls, post-rolls |
| Audio boundary detection | Silence, bumper music, production shift |
| Music bed / jingle / compression | Ad-production cues |
| Speaker / voice-profile shift | Host-read vs inserted ad |
| Repetition across episodes | Same sponsor copy |
| Local catalog match | Previously-corrected ads from user's library |

---

## 3. Add promotion reject reasons

The report shows nominal threshold 0.40 but only windows at 1.000 promote. DF5C1832 windows at 0.45 and 0.46 overlap confirmed ads yet stay as `hotPathCandidate`.

**Do not fix by blindly lowering threshold.** C22D6EC6 @ 0.597 was correctly held (it's a user-marked FP).

Per candidate window:

```json
{
  "window": [1612, 1613],
  "skipConfidence": 0.45,
  "nominalPromotionThreshold": 0.40,
  "promotionEligible": false,
  "promotionRejectReasons": [
    "insufficientSegmentDuration",
    "missingTranscriptAnchor",
    "noEvidenceDiversity",
    "cooldownActive"
  ]
}
```

Possible reject reasons:

```
belowConfidenceThreshold
insufficientDuration
insufficientHotPathStreak
missingTranscriptAnchor
missingAcousticSupport
missingMetadataSupport
cooldownActive
nearManualVeto
wholeAssetVetoed
insideKnownFalsePositiveRegion
lowCoverage
sourceUnavailable
```

Without this you cannot tell whether promotion is intentionally conservative or accidentally gated at near-certainty.

---

## 4. Promote segments, not isolated one-second windows

Several FN overlaps are one-second scored windows inside much longer user-marked ad spans. Pipeline is making local window decisions but failing to assemble them.

Segment-level state machine:

```
window scores → candidate run → merged segment → promotion gate
```

Logic:

```
Start segment when: score >= candidateThreshold for N nearby windows OR score >= highConfidenceThreshold once
Continue while:     score >= continuationThreshold OR gap <= maxInternalGapSeconds
End when:           score < continuationThreshold for M seconds
Promote when:       segmentScore >= promotionThreshold AND duration >= minAdDuration AND safety passes
```

Hysteresis:

```
candidateThreshold = 0.35
continuationThreshold = 0.28
promotionThreshold = calibrated per target precision
```

Current confidence mode is `[0.30, 0.40)` — a lot of evidence sitting just below the nominal action threshold. A segment aggregator can turn several weak-but-consistent windows into one reliable span.

---

## 5. Separate auto-skip precision from ad-detection recall

Two operating modes:

| Layer | Goal | Behavior |
|---|---|---|
| Detection | High recall | Find likely ads including uncertain candidates |
| UI candidate | Balanced | Show "possible ad" or allow manual skip |
| Auto-skip | High precision | Skip only when confidence + supporting evidence strong |

```
autoSkipEligible =
  calibratedSegmentScore >= threshold
  AND duration is plausible
  AND at least one safety signal is present
```

Safety signals: catalog match, strong lexical ad phrase, metadata slot prior, sustained acoustic ad signature, repeated sponsor pattern, user-confirmed local pattern.

---

## 6. Investigate why evidence sources almost never fire

```
classifier  100%
metadata      3%
catalog       3%
lexical       3%
acoustic      2%
FM            1%
```

Effectively a classifier-only detector. Add a source funnel for each evidence type:

```
source computed
source produced candidate
source passed quality gate
source attached to fusion input
source affected fused score
source affected final action
```

Per source telemetry:

```json
{
  "source": "acoustic",
  "assetId": "...",
  "windowsTotal": 147,
  "computed": 147,
  "producedSignal": 92,
  "passedGate": 46,
  "includedInFusion": 3,
  "affectedDecision": 1
}
```

Distinguish: not computed / computed but empty / filtered out / included but underweighted. Current report only shows fusion inclusion.

---

## 7. Prioritize acoustic features

MusicBedLevel firing on 3/147 windows is too low. Add or validate:

| Feature | Why it helps |
|---|---|
| Music bed probability | Many ads use background music or jingles |
| Loudness / LUFS shift | Inserted ads are mastered differently |
| Dynamic range / compression | Ads often tighter compression |
| Speaker embedding shift | Host/guest → ad voiceover |
| Spectral profile shift | Studio conversation vs produced ad |
| Silence / bumper boundary | Transition gaps |
| Repetition fingerprint | Same creative reused across episodes |
| Tempo / rhythm / music onset | Ad beds and intros |

Candidate fusion:

```
classifierScore + acousticAdScore + timePositionPrior
  + catalogSimilarity + lexicalScore (when transcript exists)
  → calibrated segment score
```

Logistic regression or gradient-boosted model over these features may outperform hand-tuned fusion.

---

## 8. Use the catalog more aggressively

When user marks an ad span, store: compact local audio fingerprint / embedding, transcript snippets, sponsor/brand tokens, show + episode position + duration. Search future episodes for approximate matches.

```
if catalogSimilarity is high:   allow lower classifier threshold
if catalogSimilarity is absent: require stronger acoustic/lexical evidence
```

Especially useful for ads that repeat across episodes or campaigns.

---

## 9. Retune PriorShift only after fixing real-data distribution

PriorShift band `(0.22, 0.25]` has zero real windows. Real mode is `[0.30, 0.40)`. Do not simply move it to 0.30–0.40 and ship. Grid search on real captures:

```
candidateThreshold:  0.25, 0.28, 0.30, 0.32, 0.35
promotionThreshold:  0.38, 0.40, 0.45, 0.50, 0.55
metadataWeight:      0.05, 0.10, 0.15, 0.20
acousticWeight:      0.05, 0.10, 0.15, 0.20
lexicalWeight:       0.05, 0.10, 0.15, 0.20
```

Evaluate: auto-skip precision, auto-skip recall, candidate recall, FP seconds, FN seconds, unscored-FN seconds, calibration error. Weight FP heavier than FN for auto-skip (missed ad is annoying; skipping content breaks trust).

---

## 10. Change the eval harness metrics

Split failures into:

| Metric | Meaning |
|---|---|
| Scored coverage | Did the detector emit windows over the relevant audio? |
| Transcript coverage | Did transcript-dependent evidence have a chance? |
| Candidate recall | Did the system surface likely ad regions? |
| Auto-skip precision | Were skipped spans truly ads? |
| Auto-skip recall | How many true ads were skipped automatically? |
| Segment IoU | Did predicted spans align with real spans? |
| Unscored FN rate | How many user-marked ads were never analyzed? |

**Do not count an unscored region as a classifier false negative.** Track as `pipelineCoverageFailure` — the fix is completely different.

---

## 11. Normalize user corrections before using them as ground truth

Correction set mixes whole-asset toggles, span-level FPs, and span-level FNs. 9 of 10 FP corrections are whole-asset manual vetoes, not span-level FPs.

Normalize before training or evaluating:
- Separate whole-asset from span corrections.
- Merge adjacent FN spans with gap ≤ 5 seconds.
- Deduplicate repeated corrections.
- Preserve uncertainty where correction type is unknown.
- Exclude whole-asset vetoes from span-level precision/recall unless converted carefully.

---

## 12. Fix show attribution, but do not over-index on show style yet

Fix the URL-form `podcastId` heuristic (parse feed URLs robustly with fallback to title metadata), then re-run per-show analysis. But the report's stronger evidence is that detection weakness is driven by incomplete coverage and source silence, not show content.

---

## Recommended implementation order

### P1 — Coverage and completion

**Goal:** eliminate unscored false negatives.

Build: coverage invariant tests, `analysisState` contract, unscored-range telemetry, transcription job lifecycle logs, feature-only full-episode scoring path.

**Success metric:** `>95%` of user-marked ad seconds have overlapping scored windows on downloaded/available audio.

### P1 — Promotion explainability

**Goal:** understand why 0.40–0.999 candidates do not auto-skip.

Build: `promotionRejectReasons`, segment-level candidate aggregation, tests for candidates above nominal threshold, manual-veto-aware suppression.

**Success metric:** every `hotPathCandidate` above threshold has a specific non-promotion reason.

### P2 — Evidence-source funnel

**Goal:** determine why metadata / lexical / catalog / acoustic / FM rarely reach fusion.

Build: source computed/produced/gated/fused/used counters, per-source contribution logs, per-window missing-source reasons.

**Success metric:** source silence explainable as either intentional selectivity or a bug.

### P2 — Acoustic expansion

**Goal:** transcript-independent ad evidence.

Build: music bed, loudness shift, compression/dynamic range, speaker shift, boundary detection, local acoustic fingerprinting.

**Success metric:** acoustic evidence across plausible ad-break regions, not only 2% of windows.

### P3 — PriorShift and fusion retune

**Goal:** tune around real score distributions, not synthetic fixtures.

Build: real-corpus grid search, calibration plots, threshold sweeps, cost-weighted objective.

**Success metric:** improved Sec-F1 and candidate recall without unacceptable auto-skip FPs.

---

## Likely winning architecture

1. **Full-episode local audio scan** — classifier, acoustic, time-position priors, boundary detection.
2. **Candidate segment builder** — merge weak windows, hysteresis, duration constraints.
3. **Evidence enrichment** — transcript lexical where available, FM only for shortlisted regions, local catalog match, metadata slot prior.
4. **Calibrated segment scorer** — trained/calibrated on real corrections, separate candidate score from auto-skip score.
5. **Conservative auto-skip gate** — high precision, explicit reject reasons, user-veto-aware.

Main shift: stop thinking "per-window classifier crosses threshold." Treat it as **coverage → candidate generation → evidence enrichment → segment promotion**. Maps better to podcast ads, which are contiguous events with temporal structure, repeated patterns, production changes, and user-specific correction history.

---

## Bead mapping (as landed)

| Bead | Priority | Scope | Section |
|---|---|---|---|
| `playhead-gtt9.1` | — | activeShards rehydration investigation | closed |
| `playhead-gtt9.1.1` | — | activeShards rehydration fix (persist episodeDuration + fail-safe unknown-duration guards) | closed + merged |
| `playhead-gtt9.2` | P1 | `promotionRejectReasons` instrumentation (reopened + rescoped per §3) | open |
| `playhead-gtt9.3` | P2 | PriorShift retune (real-distribution grid search) | open — blocked by 9.6, 9.7 |
| `playhead-gtt9.4` | P2 | Evidence-source funnel investigation (§6) | in_progress |
| `playhead-gtt9.5` | P2 | Show attribution robustness — URL-parse `podcastId`, title-metadata fallback (§12, expanded scope) | open |
| `playhead-gtt9.6` | **P0** | NARL eval metric split — unscored vs classifier FN (§10) | open — do first |
| `playhead-gtt9.7` | P1 | Correction normalization — whole-asset vs span, gap-merge, dedup (§11) | open |
| `playhead-gtt9.8` | P1 | `analysisState` contract + coverage telemetry (§1) | open |
| `playhead-gtt9.9` | P1 | Full-audio feature-only scoring path (§2) | open — blocked by 9.8 |
| `playhead-gtt9.10` | P1 | Segment-level candidate aggregator with hysteresis (§4) | open — blocked by 9.9 |
| `playhead-gtt9.11` | P2 | Split detection recall from auto-skip precision (§5) | open — blocked by 9.10 |
| `playhead-gtt9.12` | P2 | Acoustic feature expansion (§7) | open |
| `playhead-gtt9.13` | P2 | Catalog as precision signal (§8) | open |

**Execution order:** gtt9.6 first (user directive — P0). Then the P1 cluster in parallel where dependencies permit. Final knob-tuning (gtt9.3) blocked by gtt9.6 + gtt9.7 so every number we chase is trustworthy.
