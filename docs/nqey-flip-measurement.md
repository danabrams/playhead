# playhead-nqey — what `certaintyTieredSkipEnabled` actually touches

Investigation notes written BEFORE the flip, so the claims in the PR are checkable
against the code rather than against the bead's prose.

## 1. The bead's DE0784D8 premise is WRONG. Corrected here.

nqey's writeup argues the post-roll guard is a "pure loss" because it would demote
Dan's DE0784D8 post-roll (`5462.6–5522.7` on a 5522.65 s episode — byte-exact,
confidence 1.00, measured 2 of 2 correct).

**It would not have, because that window never reaches `DecisionMapper`.**

`AdDetectionService.mintByteExactDayZeroMarks` (AdDetectionService.swift ~11502)
builds its rows by calling `AdWindow(...)` directly and setting
`eligibilityGate: gate.rawValue`, where `gate` is chosen locally as
`.eligible` / `.markOnly` from `RediffActivation.dayZeroByteExactAutoSkipEnabled`
and the strict/segment-recovered split. There is no `DecisionMapper` on that path,
so neither the host-read floor nor the post-roll guard is consulted, with the flag
ON or OFF.

The bead's *conclusion* still stands — the guard does reach the same class of
signal on the lagged `.rediffSlot`-rewritten fusion path, and sik9's exemption is
what makes the flip safe there — but the specific window it named is not the
evidence for it.

## 2. The flag's blast radius is exactly one call site

`DecisionMapper` is constructed in exactly one place in the app target:

    Playhead/Services/AdDetection/AdDetectionService.swift:5195   (runBackfill, Steps 12–14)

and it is the only consumer of `FusionWeightConfig.certaintyTieredEnabled`,
`.hostReadConfidenceFloor` and `.postRollGuardSeconds`. The other three
`FusionWeightConfig()` construction sites (AdDetectionService.swift:3588 hot path,
:10931, :11103) are bare — they read only `.classifierCap`, so they are inert with
respect to this flag by construction.

Therefore **the population the flip can touch is: spans that reach the backfill
fusion decision path and come out `.eligible`.** Everything else in `ad_windows`
— day-0 rediff mints, hot-path rows, aggregator rows, segment-recovered marks —
is untouched, because those producers never consult this mapper.

Two further narrowings that follow from reading the guard site
(BackfillEvidenceFusion.swift:1010 and :1079):

* Both branches begin `gate == .eligible`. `.markOnly` and every `.blockedBy*`
  gate is a fixed point. The flag can only ever move `.eligible → .markOnly`.
* The post-roll branch additionally needs `let episodeDuration, episodeDuration > 0`.
  `runBackfill` passes `episodeDuration > 0 ? episodeDuration : nil`
  (AdDetectionService.swift:5212), so an episode whose duration the store does not
  know is fully exempt from the guard.

## 3. Measured population — 2026-07-21 device pull

Source: `/Users/dabrams/coreai-spike/dogfood-pull/2026-07-22-hb1/analysis.sqlite`
(newest `analysis.sqlite` on this box; content is a 2026-07-21 snapshot).

**This snapshot is STALE and predates y3ya (#326), avbn (#329), lxkq and sik9
(#330). It is reported as a bound on the shape of the population, not as a
prediction of the post-flip field behavior.** Fusion rows are identifiable because
`BackfillJobRunner.makeFusionWindowId` gives them a `fusion-` prefix; every other
id is a different producer.

    producer                     gate       rows   conf < 0.9
    ---------------------------  --------   ----   ----------
    DecisionMapper  (fusion-…)   eligible      7            3
    DecisionMapper  (fusion-…)   markOnly      1            1
    other producer               eligible     10            1
    other producer               autoSkip      4            3
    other producer               markOnly     37           37
    other producer               (null)       29            0

So on this snapshot the flag's reachable population is **7 `.eligible` fusion
windows**, of which **3 sit below the 0.9 host-read floor**. The 14 non-fusion
`.eligible`/`autoSkip` rows are outside the flag entirely.

`.rediffSlot` provenance count in `decoded_spans` on this snapshot: **0 of 16**.
So on this snapshot nothing is exempt and nothing is rediff-anchored — which is
itself why the snapshot cannot answer the post-flip question: the rediff population
is precisely what has changed since.

### What this snapshot CANNOT measure, stated plainly

* **The post-roll guard's field rate.** `analysis_assets.episodeDurationSec` on
  this pull is inconsistent with the windows stored against it — e.g. asset rows
  carrying `episodeDurationSec = 549.3` hold `.eligible` windows ending at
  3309.1 s. A `duration − endTime` computed from those rows is negative and
  meaningless. No honest post-roll count comes out of this file. (Every fusion
  row that *does* have a coherent duration sits thousands of seconds from the end,
  so the only defensible statement is "0 observed demotions on a file whose
  duration column is not trustworthy" — which is not evidence either way.)
* **Whether the host-read floor now has a population.** The floor's whole question
  is whether FM-sourced host-read spans now reach `.eligible`; y3ya and avbn landed
  to make FM verdicts surface, and both postdate this pull. There is no device
  data on this box from after those landed.

Both gaps are answered instead by an executable differential against
`DecisionMapper` itself — see `CertaintyTieredSkipShipsOnTests`.

## 4. A known over-broad predicate that this flip inherits (playhead-6qvf)

`DecodedSpan.carriesRediffByteExactWidth` is `anchorProvenance.contains(.rediffSlot)`,
and `.rediffSlot` is stamped by BOTH the byte-primary differ and the ~1 s
chroma-fingerprint fallback. Five shipped carve-outs — including sik9's post-roll
exemption — therefore treat a chroma-derived width as byte-exact.

This is pre-existing and independent of the flip. Because the flag only ever
demotes, turning it ON is strictly more conservative than today no matter how wide
the exemption is: an over-broad exemption can at worst leave a span exactly where
the OFF build already left it. Nothing in this change should be read as a claim
that the exemption is byte-exact-only. Tracked as playhead-6qvf.
