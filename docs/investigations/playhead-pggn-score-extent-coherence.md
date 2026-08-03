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
