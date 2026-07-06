# Splice-Edge Calibration Study (playhead-xsdz.24)

- run: 2026-07-06T16:32:44Z (schema v1)
- episodes processed: 34, skipped: 7
- slots: 198, edges: 396 (in-range 349, out-of-range 47), controls: 680

## Method & Score Equivalence

Scores are EXACTLY what SpliceSlotResolver sees: AudioForensicsBoundaryDetector() with DEFAULT config, candidate times Set(breaks.map(\.time)).sorted(), scoreCandidateEdge called with the FULL unmodified [FeatureWindow] array from extraction (it sorts internally), and a nil return mapped to stepScore 0 — mirroring resolveWithDiagnostics verbatim.

- served predicate: time t is SERVED at floor f iff ∃ candidate c with |c − t| <= 8.0 (inclusive) and stepScore(c) >= f (inclusive); a slot is FEASIBLE at f iff BOTH edges are served
- tolerance: ±8.000 s (inclusive)
- current production floor: 0.150
- controls: 20/episode, >= 60.000 s from every slot edge; seed scheme: SplitMix64 seeded with FNV-1a64(episodeId UTF-8) — process-randomization-free
- bestWithin8 == nil (no candidate within tolerance) is OMITTED from the JSON record (stock JSONEncoder drops nil keys — an ABSENT bestWithin8 key means nil; no explicit null is written) and treated as score 0 in distributions/AUC; it is NEVER served in the floor sweep.

## Timeline-Drift Caveat

adSlots times are FRESH-download (B) coordinates; the staged audio is the SNAPSHOT (A). Per-episode net drift D = fingerprintsB·secondsPerFpB − fingerprintsA·secondsPerFpA. A slot edge's B-time is only exact in A up to the cumulative mismatch of PRIOR ad fills, so low recall can be truth mislocation rather than detector failure — read the drift-tier and head-anchored breakouts before concluding. The same drift can CONTAMINATE controls: they avoid ±60 s of B-coordinate edge times, but under large |D| a control may land near a true A-timeline splice seam, inflating control scores and depressing AUC — treat a premiseProblem verdict on high-drift episodes with suspicion.

| episode | rotated | net drift D (s) | duration A (s) | slots | out-of-range edges |
| --- | --- | ---: | ---: | ---: | ---: |
| american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5 | true | -2.975 | 2508.000 | 5 | 0 |
| business-wars-2026-05-28-f1-vs-nascar-f1-roars-into-the-u-s-4 | true | -17.809 | 2610.000 | 9 | 0 |
| casefile-true-crime-2026-05-30-case-340-elisabeth-membrey | true | 64.159 | 6060.000 | 5 | 1 |
| fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros | true | 80.665 | 3232.000 | 6 | 2 |
| hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi | true | 11.399 | 3844.000 | 7 | 1 |
| morbid-2026-05-21-the-matamoros-devil-murders-part-1 | true | 64.243 | 3764.000 | 9 | 2 |
| morbid-2026-05-25-the-matamoros-devil-murders-part-2 | true | 135.886 | 3464.000 | 8 | 4 |
| morbid-2026-05-28-listener-tales-110-playdates-with-the-pa | true | 59.814 | 3434.000 | 9 | 2 |
| morbid-2026-05-29-may-bonus-episode-breaking-dawn-part-1 | true | -62.425 | 6780.000 | 8 | 0 |
| on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8 | true | 111.891 | 3270.000 | 6 | 1 |
| planet-money-2026-05-29-the-sneaky-way-companies-get-new-chemica | true | 15.742 | 2390.000 | 6 | 1 |
| radiolab-2026-05-29-this-american-roach | true | 22.433 | 2498.000 | 2 | 2 |
| smartless-2026-05-11-quot-kareem-rahma-quot | true | 180.577 | 3744.000 | 6 | 2 |
| smartless-2026-05-18-quot-sting-quot | true | 32.459 | 3906.000 | 6 | 1 |
| smartless-2026-05-21-quot-re-release-nate-bargatze-quot | true | 70.220 | 3462.000 | 6 | 4 |
| smartless-2026-05-25-quot-nick-jonas-quot | true | -70.246 | 4106.000 | 5 | 0 |
| stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all | true | 11.331 | 3894.000 | 7 | 1 |
| techcrunch-daily-crunch-2026-05-27-spotify-now-lets-you-stream-narrated-mag | true | 5.260 | 330.000 | 3 | 1 |
| techcrunch-daily-crunch-2026-05-29-google-engineer-charged-with-insider-tra | true | 0.920 | 278.000 | 2 | 1 |
| ted-business-2026-05-25-the-secret-to-making-the-right-career-de | true | 44.243 | 2754.000 | 6 | 1 |
| the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve | true | -14.998 | 2394.000 | 7 | 0 |
| the-ezra-klein-show-2026-05-29-does-trump-want-to-lose-the-midterms | true | -7.433 | 4664.000 | 2 | 0 |
| the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol | true | 95.420 | 5002.000 | 5 | 2 |
| the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t | true | 138.126 | 4432.000 | 6 | 3 |
| the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev | true | 90.061 | 4514.000 | 7 | 3 |
| the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os | true | 83.272 | 4304.000 | 5 | 1 |
| the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m | true | 128.598 | 3984.000 | 6 | 3 |
| the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit | true | -45.089 | 5352.000 | 4 | 0 |
| up-first-2026-06-01-can-graham-platner-survive-another-contr | true | 41.637 | 3008.000 | 3 | 2 |
| why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi | true | 131.509 | 3840.000 | 6 | 2 |
| why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit | true | 117.342 | 3078.000 | 8 | 4 |
| why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with | true | 117.209 | 3500.000 | 9 | 0 |
| why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein | true | -11.034 | 1936.000 | 2 | 0 |
| why-is-this-happening-the-chris-hayes-po-2026-05-26-the-ai-end-game-the-ethics-of-ai-with-ti | true | -71.953 | 3550.000 | 7 | 0 |

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

## Detector Recall (in-range edges, nearest candidate)

| tolerance (s) | recall |
| ---: | ---: |
| ±2 | 0.037 |
| ±5 | 0.146 |
| ±8 | 0.209 |
| ±15 | 0.344 |

over 349 in-range edges (47 out-of-range edges excluded).

## True vs Control Score Distributions

| population | n | min | p25 | median | p75 | max | mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| true bestWithin8 | 349 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 | 0.076 |
| control bestWithin8 | 680 | 0.000 | 0.000 | 0.000 | 0.000 | 0.759 | 0.024 |
| true scoreAtTruth | 349 | 0.000 | 0.000 | 0.002 | 0.177 | 1.000 | 0.137 |
| true scoreAtNearest | 349 | 0.000 | 0.085 | 0.232 | 0.500 | 1.000 | 0.308 |
| control scoreAtTime | 680 | 0.000 | 0.000 | 0.012 | 0.128 | 1.000 | 0.087 |

Histograms (20 buckets of 0.05 over [0, 1], last bucket upper-inclusive):

- true bestWithin8: [286, 5, 11, 5, 5, 2, 4, 4, 1, 1, 4, 4, 3, 2, 1, 3, 0, 0, 0, 8]
- control bestWithin8: [617, 15, 8, 7, 6, 6, 4, 6, 4, 1, 2, 1, 0, 1, 1, 1, 0, 0, 0, 0]
- true scoreAtTruth: [205, 19, 26, 19, 12, 15, 3, 4, 5, 8, 8, 6, 1, 4, 2, 0, 0, 3, 0, 9]
- true scoreAtNearest: [70, 25, 35, 32, 26, 24, 20, 8, 7, 10, 22, 7, 12, 11, 7, 10, 5, 0, 0, 18]
- control scoreAtTime: [401, 78, 52, 43, 31, 25, 8, 10, 7, 12, 4, 2, 3, 0, 1, 0, 1, 0, 0, 2]

- AUC(bestWithin8, true vs control): 0.547
- AUC(scoreAtTruth vs scoreAtTime): 0.511

## Floor Sweep

| floor | per-edge served | slot feasibility (ceiling) | control FP |
| ---: | ---: | ---: | ---: |
| 0.050 | 0.181 (63/349) | 0.060 (10/168) | 0.093 (63/680) |
| 0.075 | 0.172 (60/349) | 0.054 (9/168) | 0.079 (54/680) |
| 0.100 | 0.166 (58/349) | 0.048 (8/168) | 0.071 (48/680) |
| 0.125 | 0.155 (54/349) | 0.042 (7/168) | 0.062 (42/680) |
| 0.150 | 0.135 (47/349) | 0.036 (6/168) | 0.059 (40/680) |
| 0.200 | 0.120 (42/349) | 0.030 (5/168) | 0.049 (33/680) |
| 0.250 | 0.106 (37/349) | 0.018 (3/168) | 0.040 (27/680) |

## Breakouts

| subset | edges | slots | recall@8 | median bestWithin8 | feasibility@0.150 | note |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rotated slots | 349 | 168 | 0.209 | 0.000 | 0.036 | all slot-bearing rediff episodes are rotated today — split is degenerate by design |
| non-rotated slots | 0 | 0 | — | — | — | expected empty on the current corpus |
| drift \|D\| <= 10s | 22 | 10 | 0.182 | 0.000 | 0.100 |  |
| drift 10s < \|D\| <= 60s | 125 | 60 | 0.240 | 0.000 | 0.017 |  |
| drift \|D\| > 60s | 202 | 98 | 0.193 | 0.000 | 0.041 |  |
| head-anchored edges | 28 | — | 0.357 | 0.000 | — | provably drift-free (first slot, start edge, leftRun == start) |
| other edges | 321 | — | 0.196 | 0.000 | — |  |
| first slot per episode | 68 | 34 | 0.265 | 0.000 | 0.029 |  |
| later slots | 281 | 134 | 0.196 | 0.000 | 0.037 |  |

## Recommendation

**Verdict:** detectorBottleneck

Supporting numbers:
- recall@8 = 0.209 (healthy >= 0.600)
- AUC(bestWithin8) = 0.547 (healthy >= 0.700)
- slot feasibility @ 0.150 = 0.036 (target >= 0.600)
- control FP @ 0.150 = 0.059 (budget <= 0.100)
- recall@8 head-anchored = 0.357 vs others = 0.196 (drift-guard gap >= 0.250)
- recall@8 0.209 < 0.600 — the break detector misses true splice edges; floor tuning cannot recover them.
