# MusicUnderstanding DAI-Boundary Study (playhead-xsdz.25)

- run: 2026-07-07T04:42:35Z (schema v1)
- episodes processed: 34, skipped: 7
- slots: 198, edges: 396 (in-range 349, out-of-range 47), controls: 680
- xsdz.24 acoustic baseline: playhead-dogfood-diagnostics-splice-calibration.md, run 2026-07-06T16:32:44Z, 34 episodes / 396 edges / 349 in-range / 680 controls / 168 fully-in-range slots

## Method & Score Mapping

- structure: boundary times = interior edges of sections ∪ segments ∪ phrases (shared edges between consecutive ranges deduped; t <= 1.0 s and t >= duration − 1.0 s excluded); score = tier-agreement fraction (# of the 3 tiers with a boundary within ±0.25 s, inclusive) / 3 → 1/3, 2/3, or 1.0 (tiers are nested, so a section boundary typically scores 1.0, a phrase-only boundary 1/3)
- instrumentActivity: per instrument, each interior range start/end time (same 1.0 s start/end exclusion); score = |mean(activity frames in [t, t+4 s]) − mean(activity frames in [t−4 s, t])| clamped to [0, 1]; an empty window side → score 0 (unscorable transitions stay candidates with score 0)
- loudness: shortTerm LUFS clamped (non-finite and < −70 LUFS → −70, the EBU silence floor); step s_i = |mean(values in (t_i, t_i+4 s]) − mean(values in [t_i−4 s, t_i])| LU at each sample time; candidates = strict local maxima with s_i >= 1.5 LU (pinned min step, bounds candidate density), same start/end exclusion; score = min(1, s_i / 12) — 12 LU pinned as the full-scale hard-cut step (loudness-normalized DAI splices land well below; the sweep interprets the scale)
- merge: concatenate all sources, sort by time (ties: higher score first, then source name), greedy left-to-right clustering — a candidate joins the current cluster iff its time − the cluster's FIRST member's time <= 1.0 s; representative = member with max score (tie → earliest time, then source-name order); merged score = max member score; contributing sources recorded; output sorted by representative time, times strictly deduplicated
- score-at-time analog: scoreAtTruth/scoreAtTime = the loudness-step score min(1, s(t)/12) over the same clamped shortTerm series at any t (dominantSignal "loudnessStep"; scorable=false + score 0 when either ±4 s window has no samples)
- PRIMARY comparison: the PRIMARY separability comparison vs xsdz.24 is AUC(bestWithin8) over MERGED-candidate scores; AUC(scoreAtTruth) is a loudness-channel-only SECONDARY
- episode-record field mapping: reused SpliceCalibrationEpisodeRecord fields are REINTERPRETED: windowCount = clamped shortTerm sample count, breakCount = raw per-source candidate total (structure + activity + loudness), candidateCount = merged candidate count
- pinned constants: start/end exclusion 1.000 s, tier tolerance ±0.250 s, activity window ±4.000 s (both sides inclusive of t), loudness window 4.000 s per side (left [t−4, t], right (t, t+4]), silence floor -70.000 LUFS, min step 1.500 LU, full-scale step 12.000 LU, merge window 1.000 s
- served tolerance: ±8.000 s (inclusive); controls: 20/episode, >= 60.000 s from every slot edge; seed scheme: SplitMix64 seeded with FNV-1a64(episodeId UTF-8) — process-randomization-free

## Timeline-Drift Caveat

adSlots times are FRESH-download (B) coordinates; the staged audio is the SNAPSHOT (A). Per-episode net drift D = fingerprintsB·secondsPerFpB − fingerprintsA·secondsPerFpA. A slot edge's B-time is only exact in A up to the cumulative mismatch of PRIOR ad fills, so low recall can be truth mislocation rather than detector failure — read the drift-tier and head-anchored breakouts before concluding. The same drift can CONTAMINATE controls: they avoid ±60 s of B-coordinate edge times, but under large |D| a control may land near a true A-timeline splice seam, inflating control scores and depressing AUC — treat a premiseProblem verdict on high-drift episodes with suspicion.

durationA here is the ASSET duration (AVURLAsset load(.duration) with AVURLAssetPreferPreciseDurationAndTimingKey — default mp3 duration is header-estimated and can err by seconds on VBR files, which would misclassify tail edges' inRange, misplace the end-exclusion zone, and shift the control-sampling domain) rather than xsdz.24's max-FeatureWindow end, so control TIMES differ slightly from xsdz.24's for the same episode seed — statistically equivalent, not sample-identical.

| episode | rotated | net drift D (s) | duration A (s) | slots | out-of-range edges |
| --- | --- | ---: | ---: | ---: | ---: |
| american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5 | true | -2.975 | 2508.774 | 5 | 0 |
| business-wars-2026-05-28-f1-vs-nascar-f1-roars-into-the-u-s-4 | true | -17.809 | 2610.991 | 9 | 0 |
| casefile-true-crime-2026-05-30-case-340-elisabeth-membrey | true | 64.159 | 6061.584 | 5 | 1 |
| fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros | true | 80.665 | 3233.437 | 6 | 2 |
| hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi | true | 11.399 | 3845.016 | 7 | 1 |
| morbid-2026-05-21-the-matamoros-devil-murders-part-1 | true | 64.243 | 3765.891 | 9 | 2 |
| morbid-2026-05-25-the-matamoros-devil-murders-part-2 | true | 135.886 | 3465.665 | 8 | 4 |
| morbid-2026-05-28-listener-tales-110-playdates-with-the-pa | true | 59.814 | 3435.076 | 9 | 2 |
| morbid-2026-05-29-may-bonus-episode-breaking-dawn-part-1 | true | -62.425 | 6780.787 | 8 | 0 |
| on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8 | true | 111.891 | 3271.158 | 6 | 1 |
| planet-money-2026-05-29-the-sneaky-way-companies-get-new-chemica | true | 15.742 | 2391.876 | 6 | 1 |
| radiolab-2026-05-29-this-american-roach | true | 22.433 | 2498.664 | 2 | 2 |
| smartless-2026-05-11-quot-kareem-rahma-quot | true | 180.577 | 3745.280 | 6 | 2 |
| smartless-2026-05-18-quot-sting-quot | true | 32.459 | 3906.952 | 6 | 1 |
| smartless-2026-05-21-quot-re-release-nate-bargatze-quot | true | 70.220 | 3463.523 | 6 | 4 |
| smartless-2026-05-25-quot-nick-jonas-quot | true | -70.246 | 4107.781 | 5 | 0 |
| stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all | true | 11.331 | 3894.674 | 7 | 1 |
| techcrunch-daily-crunch-2026-05-27-spotify-now-lets-you-stream-narrated-mag | true | 5.260 | 330.292 | 3 | 1 |
| techcrunch-daily-crunch-2026-05-29-google-engineer-charged-with-insider-tra | true | 0.920 | 279.380 | 2 | 1 |
| ted-business-2026-05-25-the-secret-to-making-the-right-career-de | true | 44.243 | 2755.683 | 6 | 1 |
| the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve | true | -14.998 | 2395.768 | 7 | 0 |
| the-ezra-klein-show-2026-05-29-does-trump-want-to-lose-the-midterms | true | -7.433 | 4664.424 | 2 | 0 |
| the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol | true | 95.420 | 5003.546 | 5 | 2 |
| the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t | true | 138.126 | 4433.711 | 6 | 3 |
| the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev | true | 90.061 | 4514.873 | 7 | 3 |
| the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os | true | 83.272 | 4304.509 | 5 | 1 |
| the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m | true | 128.598 | 3985.162 | 6 | 3 |
| the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit | true | -45.089 | 5352.751 | 4 | 0 |
| up-first-2026-06-01-can-graham-platner-survive-another-contr | true | 41.637 | 3008.418 | 3 | 2 |
| why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi | true | 131.509 | 3841.045 | 6 | 2 |
| why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit | true | 117.342 | 3079.079 | 8 | 4 |
| why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with | true | 117.209 | 3501.087 | 9 | 0 |
| why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein | true | -11.034 | 1936.718 | 2 | 0 |
| why-is-this-happening-the-chris-hayes-po-2026-05-26-the-ai-end-game-the-ethics-of-ai-with-ti | true | -71.953 | 3550.772 | 7 | 0 |

## Episodes Covered / Skipped

Processed 34 episodes; skipped 7:

| episode | reason |
| --- | --- |
| last-week-in-ai-2026-05-29-anthropic-reports-965-billion-valuation- | no ad slots (unchanged or none detected) |
| pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american | no ad slots (unchanged or none detected) |
| tech-won-t-save-us-2026-05-28-do-chatbots-really-belong-in-schools-w-t | no ad slots (unchanged or none detected) |
| techcrunch-daily-crunch-2026-05-26-spotify-s-ai-bet-more-of-everything-less | no ad slots (unchanged or none detected) |
| techcrunch-daily-crunch-2026-05-28-youtube-will-now-automatically-label-ai- | no ad slots (unchanged or none detected) |
| the-charlie-kirk-show-2026-05-30-thoughtcrime-ep-129-spanking-your-kids-t | no ad slots (unchanged or none detected) |
| the-rest-is-politics-2026-05-28-who-funds-reform-the-missing-millions | no ad slots (unchanged or none detected) |

## Detector Recall (in-range edges, nearest merged candidate)

| tolerance (s) | recall | xsdz.24 acoustic baseline |
| ---: | ---: | ---: |
| ±2 | 0.837 | 0.037 |
| ±5 | 0.977 | 0.146 |
| ±8 | 1.000 | 0.209 |
| ±15 | 1.000 | 0.344 |

over 349 in-range edges (47 out-of-range edges excluded); baseline over 349 in-range edges.

DENSITY CAVEAT: nearest-candidate recall rises mechanically with candidate density — this study fields 32985 merged candidates over 1998.739 audio minutes (16.503 candidates/min aggregate; per-source and per-episode counts in Runtime Cost). Read the control-side distributions and AUC below before crediting recall to the detector.

## True vs Control Score Distributions

| population | n | min | p25 | median | p75 | max | mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| true bestWithin8 | 349 | 0.333 | 0.667 | 1.000 | 1.000 | 1.000 | 0.890 |
| control bestWithin8 | 680 | 0.333 | 0.667 | 1.000 | 1.000 | 1.000 | 0.899 |
| true scoreAtTruth | 349 | 0.000 | 0.025 | 0.061 | 0.164 | 1.000 | 0.131 |
| true scoreAtNearest | 349 | 0.017 | 0.333 | 0.333 | 0.667 | 1.000 | 0.503 |
| control scoreAtTime | 680 | 0.001 | 0.027 | 0.057 | 0.116 | 1.000 | 0.087 |

Histograms (20 buckets of 0.05 over [0, 1], last bucket upper-inclusive):

- true bestWithin8: [0, 0, 0, 0, 0, 0, 20, 1, 0, 0, 0, 1, 0, 71, 0, 1, 1, 0, 0, 254]
- control bestWithin8: [0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 158, 1, 0, 1, 0, 0, 497]
- true scoreAtTruth: [152, 68, 33, 34, 12, 13, 4, 8, 4, 3, 2, 3, 2, 1, 2, 0, 0, 0, 0, 8]
- true scoreAtNearest: [7, 10, 21, 20, 8, 9, 119, 6, 6, 4, 4, 6, 2, 48, 0, 0, 0, 0, 0, 79]
- control scoreAtTime: [307, 169, 81, 47, 40, 16, 11, 2, 3, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2]

- AUC(bestWithin8, true vs control): 0.495 — PRIMARY vs xsdz.24 baseline 0.547
- AUC(scoreAtTruth vs scoreAtTime): 0.530 — loudness-channel-only SECONDARY vs xsdz.24 baseline 0.511

## Floor Sweep

NOTE: the two studies' score scales are STUDY-DEFINED (merged 0–1 mapping here, acoustic stepScore in xsdz.24) — floors are not directly comparable across studies; the sweep SHAPES and the AUCs are the comparison. Baseline control FP is pinned only at the current production floor 0.150.

| floor | per-edge served | slot feasibility (ceiling) | control FP | xsdz.24 feasibility | xsdz.24 control FP |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0.050 | 1.000 (349/349) | 1.000 (168/168) | 1.000 (680/680) | 0.060 | — |
| 0.075 | 1.000 (349/349) | 1.000 (168/168) | 1.000 (680/680) | 0.054 | — |
| 0.100 | 1.000 (349/349) | 1.000 (168/168) | 1.000 (680/680) | 0.048 | — |
| 0.125 | 1.000 (349/349) | 1.000 (168/168) | 1.000 (680/680) | 0.042 | — |
| 0.150 | 1.000 (349/349) | 1.000 (168/168) | 1.000 (680/680) | 0.036 | 0.059 |
| 0.200 | 1.000 (349/349) | 1.000 (168/168) | 1.000 (680/680) | 0.030 | — |
| 0.250 | 1.000 (349/349) | 1.000 (168/168) | 1.000 (680/680) | 0.018 | — |

baseline edge-served @ 0.150 = 0.135.

## Breakouts

| subset | edges | slots | recall@8 | median bestWithin8 | feasibility@0.150 | note |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rotated slots | 349 | 168 | 1.000 | 1.000 | 1.000 | all slot-bearing rediff episodes are rotated today — split is degenerate by design |
| non-rotated slots | 0 | 0 | — | — | — | expected empty on the current corpus |
| drift \|D\| <= 10s | 22 | 10 | 1.000 | 1.000 | 1.000 |  |
| drift 10s < \|D\| <= 60s | 125 | 60 | 1.000 | 1.000 | 1.000 |  |
| drift \|D\| > 60s | 202 | 98 | 1.000 | 1.000 | 1.000 |  |
| head-anchored edges | 28 | — | 1.000 | 0.667 | — | provably drift-free (first slot, start edge, leftRun == start) |
| other edges | 321 | — | 1.000 | 1.000 | — |  |
| first slot per episode | 68 | 34 | 1.000 | 1.000 | 1.000 |  |
| later slots | 281 | 134 | 1.000 | 1.000 | 1.000 |  |

Baseline (xsdz.24 acoustic): head-anchored recall@8 0.357 vs others 0.196; drift-tier recall@8: low 0.182, mid 0.240, high 0.193; verdict detectorBottleneck.

## Runtime Cost

| episode | audio (s) | session init (s) | analyze (s) | ×realtime | structure | activity | loudness | merged | cands/min |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5 | 2508.774 | 0.136 | 199.653 | 12.566 | 470 | 212 | 101 | 599 | 14.326 |
| business-wars-2026-05-28-f1-vs-nascar-f1-roars-into-the-u-s-4 | 2610.991 | 0.059 | 111.165 | 23.488 | 539 | 141 | 243 | 715 | 16.431 |
| casefile-true-crime-2026-05-30-case-340-elisabeth-membrey | 6061.584 | 0.004 | 345.376 | 17.551 | 1071 | 1849 | 810 | 2329 | 23.053 |
| fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros | 3233.437 | 0.004 | 153.700 | 21.037 | 588 | 419 | 490 | 1003 | 18.612 |
| hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi | 3845.016 | 0.004 | 185.436 | 20.735 | 761 | 335 | 151 | 949 | 14.809 |
| morbid-2026-05-21-the-matamoros-devil-murders-part-1 | 3765.891 | 0.004 | 182.931 | 20.586 | 720 | 178 | 451 | 1012 | 16.124 |
| morbid-2026-05-25-the-matamoros-devil-murders-part-2 | 3465.665 | 0.004 | 152.321 | 22.752 | 683 | 196 | 500 | 1032 | 17.867 |
| morbid-2026-05-28-listener-tales-110-playdates-with-the-pa | 3435.076 | 0.004 | 180.913 | 18.987 | 594 | 202 | 639 | 1036 | 18.096 |
| morbid-2026-05-29-may-bonus-episode-breaking-dawn-part-1 | 6780.787 | 0.004 | 387.037 | 17.520 | 1194 | 630 | 1018 | 1972 | 17.449 |
| on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8 | 3271.158 | 0.004 | 144.310 | 22.668 | 645 | 116 | 243 | 821 | 15.059 |
| planet-money-2026-05-29-the-sneaky-way-companies-get-new-chemica | 2391.876 | 0.004 | 93.979 | 25.451 | 500 | 191 | 311 | 721 | 18.086 |
| radiolab-2026-05-29-this-american-roach | 2498.664 | 0.004 | 107.926 | 23.152 | 491 | 552 | 326 | 860 | 20.651 |
| smartless-2026-05-11-quot-kareem-rahma-quot | 3745.280 | 0.004 | 175.619 | 21.326 | 729 | 154 | 435 | 1012 | 16.212 |
| smartless-2026-05-18-quot-sting-quot | 3906.952 | 0.004 | 176.028 | 22.195 | 800 | 181 | 489 | 1108 | 17.016 |
| smartless-2026-05-21-quot-re-release-nate-bargatze-quot | 3463.523 | 0.005 | 167.110 | 20.726 | 658 | 189 | 457 | 966 | 16.734 |
| smartless-2026-05-25-quot-nick-jonas-quot | 4107.781 | 0.004 | 181.505 | 22.632 | 847 | 361 | 552 | 1240 | 18.112 |
| stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all | 3894.674 | 0.003 | 193.221 | 20.157 | 697 | 261 | 348 | 963 | 14.836 |
| techcrunch-daily-crunch-2026-05-27-spotify-now-lets-you-stream-narrated-mag | 330.292 | 0.004 | 13.922 | 23.725 | 66 | 58 | 29 | 94 | 17.076 |
| techcrunch-daily-crunch-2026-05-29-google-engineer-charged-with-insider-tra | 279.380 | 0.004 | 11.288 | 24.750 | 55 | 62 | 21 | 86 | 18.469 |
| ted-business-2026-05-25-the-secret-to-making-the-right-career-de | 2755.683 | 0.004 | 117.570 | 23.439 | 549 | 361 | 417 | 859 | 18.703 |
| the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve | 2395.768 | 0.005 | 90.672 | 26.422 | 553 | 529 | 324 | 873 | 21.864 |
| the-ezra-klein-show-2026-05-29-does-trump-want-to-lose-the-midterms | 4664.424 | 0.130 | 278.969 | 16.720 | 860 | 305 | 187 | 1068 | 13.738 |
| the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol | 5003.546 | 0.065 | 282.920 | 17.685 | 600 | 922 | 426 | 1241 | 14.881 |
| the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t | 4433.711 | 0.003 | 213.237 | 20.792 | 851 | 358 | 310 | 1141 | 15.441 |
| the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev | 4514.873 | 0.003 | 209.962 | 21.503 | 891 | 434 | 402 | 1227 | 16.306 |
| the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os | 4304.509 | 0.004 | 212.035 | 20.301 | 814 | 392 | 336 | 1122 | 15.639 |
| the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m | 3985.162 | 0.003 | 182.916 | 21.787 | 771 | 298 | 307 | 1037 | 15.613 |
| the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit | 5352.751 | 0.003 | 355.530 | 15.056 | 622 | 556 | 461 | 1145 | 12.835 |
| up-first-2026-06-01-can-graham-platner-survive-another-contr | 3008.418 | 0.004 | 112.547 | 26.730 | 617 | 518 | 419 | 1013 | 20.203 |
| why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi | 3841.045 | 0.004 | 170.427 | 22.538 | 767 | 263 | 75 | 891 | 13.918 |
| why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit | 3079.079 | 0.004 | 133.202 | 23.116 | 643 | 244 | 43 | 749 | 14.595 |
| why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with | 3501.087 | 0.004 | 160.431 | 21.823 | 683 | 146 | 26 | 746 | 12.785 |
| why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein | 1936.718 | 0.008 | 73.648 | 26.297 | 404 | 260 | 148 | 566 | 17.535 |
| why-is-this-happening-the-chris-hayes-po-2026-05-26-the-ai-end-game-the-ethics-of-ai-with-ti | 3550.772 | 0.004 | 160.857 | 22.074 | 699 | 236 | 38 | 789 | 13.332 |

- totals: audio 119924.349 s, session init 0.510 s, analyze 5918.360 s
- analyze cost: 177.663 s per audio hour

## Recommendation

**Verdict:** premiseProblem

Supporting numbers (thresholds reused verbatim from the xsdz.24 study constants):
- recall@8 = 1.000 (healthy >= 0.600; xsdz.24 baseline 0.209)
- AUC(bestWithin8) = 0.495 (healthy >= 0.700; xsdz.24 baseline 0.547)
- slot feasibility @ 0.150 = 1.000 (target >= 0.600)
- control FP @ 0.150 = 1.000 (budget <= 0.100)
- recall@8 head-anchored = 1.000 vs others = 1.000 (drift-guard gap >= 0.250)
- AUC(bestWithin8) 0.495 < 0.700 — true-edge scores are not separable from content controls; CDN splices score content-like at the current feature set.
