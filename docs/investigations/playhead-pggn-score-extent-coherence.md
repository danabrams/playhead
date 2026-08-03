# playhead-pggn — a segment's score must describe the span it reports

Investigation notes. Written before the fix; the measurement sections are filled in as they land.

## The defect, restated from the code

`SegmentAggregator.MachineState.ingestIntoOpenSegment` (SegmentAggregator.swift:483-500) calls
`seg.include(w)` on a window that scored **below** `continuationThreshold` (0.28). `include()` adds
`score · duration` to `weightedScoreSum` and `duration` to `totalDuration`; `meanScore` divides one
by the other. The emitted `endTime`, however, is `lastQualifyingEndTime`, which that branch
deliberately does **not** advance.

So the denominator of the reported confidence grows with region the reported geometry does not
cover. The bead's worked example: a 0.62 seed at [1530,1560) followed by a 0.20 window at
[1560,1590) emits `endTime = 1560`, `segmentScore = (0.62·30 + 0.20·30)/60 = 0.41`. A 30-second row
carrying a number that describes 60 seconds.

## Where the two regions can diverge — an exhaustive read of the include sites

`include()` is called from exactly two places.

1. `openSegment(startingWith:context:)` (:532-548). Every included window also updates
   `lastQualifyingEndTime` via `if w.endTime > seg.lastQualifyingEndTime` and `startTime` via
   `if w.startTime < seg.startTime`. **The extent is the union of what it scored.** This path is
   already coherent, and note that it takes the *max* — it is monotone.

2. `ingestIntoOpenSegment` (:456-501), two branches:
   - qualifying (`score >= continuationThreshold`): `seg.lastQualifyingEndTime = w.endTime`,
     assigned **unconditionally, not via max**. See "second finding" below.
   - below continuation: `include()` with no extent update at all. **This is the bead's defect.**

So there is exactly one source of the reported dilution, and it is the sub-continuation branch.

## Second finding, same invariant, different mechanism: `lastQualifyingEndTime` can go BACKWARDS

`ingestIntoOpenSegment` assigns `seg.lastQualifyingEndTime = w.endTime` unconditionally, while
`openSegment` takes the max. Windows are sorted by `startTime` only, and the aggregator's own
contract (file header, :38-42) says overlapping windows from different tiers are **not**
deduplicated. Tier 1 emits 30 s slots and Tier 2 emits 2 s candidates, so the stream
`[1530,1560)@0.5` then `[1540,1542)@0.5` is well-formed input — and it drives
`lastQualifyingEndTime` from 1560 down to 1542.

That is the same invariant violated in the same direction as the bead's headline defect: the
reported extent ends up **narrower** than the region the score covers, which is the
`playhead-4xqf` boundary-collapse family. It also matters mechanically for the fix — a deferred
"fold the tail in once the extent covers it" rule is only well-defined if the extent is monotone.

Decision recorded below with its measurement.

## The invariant choice

Option (a): the score is computed only over the reported extent.
Option (b): the extent grows to cover everything the score consumed.

**Chosen: (a).**

The argument against (b) is what the sub-continuation branch *means*. A window below
`continuationThreshold` is not missing evidence, it is **negative** evidence: the classifier looked
at that region and said it does not look like an ad. Option (b) would push the reported ad boundary
into a region measured as not-ad. With Tier 1's 30 s slots that is not a marginal amount:
`belowContinuationSecondsToEnd` is 3.0 s, so a single 30 s sub-continuation slot both trips the
countdown and, under (b), would widen the reported span by a full slot.

The `playhead-4xqf` argument cuts the other way here, not toward (b). 4xqf is about reporting a
narrower span than the *evidence supports*. A sub-continuation tail is evidence that does not
support the span. Widening onto it is not recovering collapsed boundary, it is spending show audio —
and per the outer-edges-free/inner-edges-precious finding, a trailing inner edge is the expensive
kind. Every second added there is show the user loses if the row is ever acted on.

What (a) gives up: the tail's evidence no longer moderates the reported confidence. A genuine ad
followed by a long quiet decay now reports the strong number for its 30 s. That is correct under the
chosen invariant — the number describes those 30 s and nothing else — but it does mean the score
carries no hint that the region is trailing off. If a downstream consumer ever wants "how confident
are we about the neighbourhood", that is a different quantity and needs its own field, not a
denominator.

The tail is not discarded as *evidence*: it still drives `belowContinuationSeconds`, which is the
end-of-segment decision. That was always its real job.

## Implementation shape

Windows below continuation are held in a **pending tail accumulator** rather than added to the
segment's sums:

- a qualifying window folds the pending tail into the main sums (the extent has now grown past it,
  so those windows are inside the reported span) and then includes itself;
- close / flush drops the pending tail (it lies beyond `lastQualifyingEndTime`);
- a sub-continuation window whose `endTime` is already `<= lastQualifyingEndTime` is inside the
  reported extent and goes straight into the main sums — excluding it would open a hole in the
  score's coverage, which is the same defect mirrored.

Containment, not proration, for a window straddling the extent edge: the file header's own contract
(:32-37) is that a window is an atomic `[start,end)` interval with a single score. Splitting one
would contradict the stated model.

`belowContinuationSeconds` accounting is untouched, so segment geometry — where segments open, close
and merge — is byte-identical apart from the `lastQualifyingEndTime` monotonicity decision.

## Interaction with playhead-bllt (#339)

`runSegmentAggregation` hardcodes `extentSupport = .unanchored` (AdDetectionService.swift:11306) and
runs every gate verdict through `HotPathExtentGate.gatedLabel` with
`config.unanchoredExtentBlocksAutoSkip` (ships `true`). So on this producer an `"autoSkip"` verdict
is demoted to `"markOnly"` unconditionally, *today*, regardless of score.

Consequence for this bead: a score crossing 0.55 upward changes the gate's internal verdict and the
`[bllt]` log line, but does **not** admit the row to auto-skip. The admission that does change is at
0.40 — `promotionThreshold` and `AutoSkipPrecisionGateConfig.uiCandidateThreshold` are both 0.40, so
a segment whose mean rises across 0.40 goes from not-emitted to a persisted `markOnly` row.

This is measured and reported both ways regardless, because bllt's gate is a policy flag and pggn's
number is what a future calibration (playhead-8x59) will be fitted against.

Pinned by `undilutedAggregatorRowStillDemotesUnderTheExtentGate`, which asserts the persisted row's
confidence is **above** 0.55 (so the demotion is the thing under test rather than a score that never
got there) and that its `eligibilityGate` is still `"markOnly"` with both edges unanchored.

## The measurement

### What corpus data exists, and what does not

There is no stored per-window classifier score stream at corpus scale on this box. `TestFixtures/`
carries gold annotations and audit ledgers, not aggregator inputs; `TestFixtures/Corpus/Audio` is
gitignored and empty, so the real classifier cannot be re-run. The `playhead-dogfood-diagnostics-*`
dumps carry `adWindows` (aggregator OUTPUT) and decoded spans, not the per-window scores that go in.
The one real per-window store is `PlayheadTests/Fixtures/NarlEval/**/FrozenTrace*.json` — 64 traces
holding **226 real `fusedSkipConfidence` values** (min 7e-6, max 1.0, mean 0.457, median 0.380).
Those are hot-path survivors, so they are biased high as a population, but the values are real.

So the measurement below is a **differential over generated streams**, run through the actual OLD
and NEW Swift implementations (verbatim copies, only the change under test differing) rather than a
reimplementation. Three of the five populations use documented real parameters; one resamples the
226 real confidences; none of them is a replay of a real episode, and that limitation is stated
rather than papered over. Harness and raw output are in the session scratchpad
(`pggn/Agg.swift`, `pggn/main.swift`).

### Direction: it is not monotone in theory, and it is monotone where it matters

μ_old is a weighted average of μ_new (the extent) and μ_T (the dropped tail), so μ_new can only fall
below μ_old when μ_new < μ_T. Every tail window scores strictly under `continuationThreshold`
(0.28), so μ_T < 0.28 and therefore μ_new < 0.28 < `promotionThreshold` (0.40).

**A segment whose score can fall is a segment neither run would promote.** A downward case IS
constructible — fold a near-zero mid-segment window in, then trail a 0.27 tail:

```
[0,2)@0.35 [2,4)@0.35 [4,6.9)@0.00 [6.9,7.0)@0.28 [7.0,7.01)@0.27
OLD 0.204094   NEW 0.204000   delta −0.000094
```

Both are 0.204, less than half the promotion floor. Nothing is emitted either way.

### Differential results (OLD → NEW), 45-minute episodes unless noted

| population | segments | score UP | score DOWN | promoted 0.40 | ≥0.55 |
|---|---|---|---|---|---|
| A. Tier-1 30 s grid, documented speech (μ .33 σ .06) + ads (μ .52 σ .10), n=4000 | 33,100 | 31,209 (94.3%), mean +0.0196, max +0.284 | **0** | 3,511 → 5,474 (+1,963 / −0) | 18 → 255 (up 237 / down 0) |
| B. Tier-1 grid, bootstrap from the 226 real FrozenTrace confidences, n=4000 | 18,836 | 15,257 (81.0%), mean +0.0453, max +0.500 | **0** | 15,205 → 17,225 (+2,020 / −0) | 1,063 → 2,687 (up 1,624 / down 0) |
| C. Tier-1 30 s + 40 Tier-2 2 s candidates/episode, n=4000 | 81,754 | 34,985 (42.8%), mean +0.0717, max +0.766 | **0** | 9,220 → 12,201 (+2,981 / −0) | 633 → 975 (up 342 / down 0) |
| D. Dense 1 s windows (the 2026-04-23 capture shape), 20 min, n=600 | 4,424 | 3,968 (89.7%), mean +0.0056, max +0.108 | **0** | 471 → 488 (+17 / −0) | 0 → 0 |
| E. Adversarial: 300 Tier-2/episode, wide spread, n=2000 | 163,379 | 73,730 (45.1%), mean +0.1881, max +0.913 | **0** | 10,844 → 15,908 (+5,064 / −0) | 917 → 1,321 (up 404 / down 0) |

Plus two saturation checks:

- **Structured enumeration** over the bead's shape — seed 0.28…0.99 × tail 0.00…0.27 × tail width
  {2, 5, 10, 30}: 7,280 cases, **7,280 up, 0 same, 0 down**.
- **Unconstrained fuzz**, 3,000,000 random 3–10 window streams with arbitrary widths (0.05–35 s),
  arbitrary scores, arbitrary gaps: 1,684,398 up, 2,799,315 unchanged, **0 down, 0 geometry
  differences**.

**Geometry is unchanged in every one of the ~300,000 segments above.** `startTime`, `endTime` and
the segment count are byte-identical; only the score and `windowCount` move. That is the direct
consequence of leaving `belowContinuationSeconds` alone.

### The bead's worked example, through the shipped code

```
[1530,1560)@0.62 then [1560,1590)@0.20
OLD  1530–1560  score 0.41  windowCount 2
NEW  1530–1560  score 0.62  windowCount 1
```

0.41 is under the 0.55 auto-skip threshold; 0.62 is over it. That is the bead's second consequence,
reproduced rather than assumed.

### Net effect on auto-skip admission

**Zero rows are admitted to auto-skip by this change.** The upward 0.55 crossings above are
crossings of the *precision gate's internal threshold*; `runSegmentAggregation` then hands every
verdict to `HotPathExtentGate` with `extentSupport = .unanchored`, which demotes `"autoSkip"` to
`"markOnly"` unconditionally under the shipped `unanchoredExtentBlocksAutoSkip: true`. The last
three beads narrowed auto-skip and this one does not widen it.

What *does* change at the row level is admission at **0.40**, where `promotionThreshold` and
`AutoSkipPrecisionGateConfig.uiCandidateThreshold` coincide: segments that used to score below the
floor now clear it, so more `markOnly` marker rows are persisted (+56% in population A, +13% in B,
+32% in C, +4% in D). Those are markers, not skips — but they are marker rows on segments whose
undiluted evidence supports them, which is the population `playhead-8x59` will calibrate against.

### What was deliberately left out

The second finding — `lastQualifyingEndTime` assigned rather than maxed, so the extent can move
backwards on overlapping heterogeneous input — is filed as **playhead-eqo8** and is NOT fixed here.
Measured with the same harness, swapping the assignment for a max on population C moves segment
count 81,754 → 54,607 (−33%), changes geometry on 26,948 segments, and moves scores in both
directions (27,895 up / 10,355 down) with 1,180 promotions lost against 3,166 gained. That is a
calibration change with its own before/after to justify, and folding it in here would have made
pggn's own numbers unreadable.
