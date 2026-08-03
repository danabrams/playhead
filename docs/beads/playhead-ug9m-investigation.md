# playhead-ug9m — investigation notes (read-only survey, tree at 04d42fb4)

Everything below was OBSERVED in the tree, not inferred, unless marked otherwise.

## 1. The chain still holds on main

* `DayZeroRediffAttemptPolicy.decide` (RediffDayZeroOutcome.swift:460) checks
  `record.lastExit.isRetryable` at :465, BEFORE the `policyGeneration` check at :471.
  `isRetryable` returns `false` for exactly one case — `.marked` (:192-204). So an
  asset day-0 has marked is suppressed forever and a generation bump cannot reach it.
* `currentGeneration == 1` (:421).
* `mintByteExactDayZeroMarks` (AdDetectionService.swift:11615) drops every unioned slot
  overlapping ANY existing `AdWindow` (:11746-11748) and exits `.allSlotsAlreadyCovered`
  (:11802-11805). Nothing re-stamps anchors on an existing row.
* qs0d's tiering is at :11762-11768 — `strict ? .rediffByteExact : .unanchored`, and
  `eligibilityGate = (strict && dayZeroByteExactAutoSkipEnabled) ? .eligible : .markOnly`.

## 2. NO DOWNSTREAM GATE would catch a wrong promotion — confirmed

The parent agent's correction is right; I verified it independently.

* playhead-bllt (#339, 04d42fb4) added `HotPathExtentGate.gatedLabel` at exactly two
  sites: `runHotPath` (AdDetectionService.swift:3949) and `runSegmentAggregation`
  (:11307). The fusion path's playhead-2350 gate is `withExtentSupport(_:blockingUnanchoredAutoSkip:)`
  at :5849.
* The day-0 mint at :11615 constructs its `AdWindow` INLINE and picks `eligibilityGate`
  itself. It passes through none of those three.
* `SkipOrchestrator` reads the row: `resolvedEdgeAnchors` (:6580) falls back to the
  row's own persisted columns via `AdWindow.extentSupport`; it feeds `detectorClass`,
  `paddedCueSpan` and veto attribution — it does NOT re-gate eligibility.
* `AutoSkipEdgePadding.isEnabledByDefault == false` (bllt's own note), so the last-mile
  per-edge veto runs for nobody.

=> A day-0 row persisted `eligibilityGate = eligible` auto-skips. The promotion decision
is final.

## 3. STRICTNESS IS NOT ON DISK — so a re-stamp migration cannot be proved

Searched every durable surface:

* `ad_windows` — the qs0d anchor pair IS the persisted strict flag for rows minted
  AFTER qs0d. But its ABSENCE is three-ways ambiguous: pre-qs0d strict, pre-qs0d
  segment-recovered, post-qs0d segment-recovered. Nothing on the row distinguishes them.
* `rediff_day_zero_attempts` (AnalysisStore.swift:6181, 16 columns) records
  `lastMarkCount`, `lastDivergentSlotCount`, `lastBSideCount`, `lastBSidesAccepted`,
  `lastBSidesGateRejected`, `lastBSidesUnreadable`. There is NO monotonic-clean /
  strict count. `bSidesAccepted` does not imply strict: with
  `RediffActivation.nonMonotonicSegmentRecoveryEnabled == true` (:164, activated
  2026-07-23) `gateAndDiffBytes` accepts non-monotonic alignments via 9s6q segment
  recovery, so an accepted B-side may be either arm.
* The B-copies are deleted by construction (never-persist-B, xsdz.28), so the
  `(A,B)` pair that decided `alignment.monotonicClean` cannot be recomputed.

=> For EVERY existing degraded row, strictness is unrecoverable. A migration would have
to guess, and the population it would wrongly promote is exactly the 9s6q
segment-recovered slots qs0d deliberately withheld. **No row is promoted by migration.**

## 4. Consequence for the plan

* Part 1 ships as the SURFACING half: zero rows re-stamped, the frozen state made
  queryable with its reason.
* Part 2 ships as the rescue, and it is the part that makes promotion PROVABLE: a
  bounded re-attempt RE-DERIVES `alignment.monotonicClean` from bytes on the current
  build and runs the same `RediffSlotOwnership.strictByteExactMask` classification a
  first-listen mint runs. Nothing is inferred from an ambiguous row.

Note this contradicts the bead's decision text ("re-stamp anchors from the persisted
byte-exact provenance. No re-fetch needed — the geometry is already on disk"). The
geometry is on disk; the ACCEPTANCE ARM that geometry came from is not, and it is the
acceptance arm — not the geometry — that qs0d keyed the anchor on.

## 5. What was built (2026-08-02)

Both halves, but Part 1 is NOT the re-stamp the bead's decision text asked for — see §3.

**Part 1, surfacing.** ZERO rows are promoted by migration. `DayZeroMarkCensus` counts an
asset's day-0 rows by the only distinction that governs anything — both edges
`.rediffByteExact` or not — and `DayZeroMarkFreezeState` names the result
(`noMarks`/`anchored`/`rescuable`/`frozen`/`settled`). `AnalysisStore.fetchDayZeroMarkFreezeReports`
is the queryable surface and it ships in the diagnostics bundle as `day_zero_mark_freeze`, with
its own `RediffDiagnosticsFetchAdapter.Read` name so an unreadable census cannot render as
"no asset is frozen".

**Part 2, the bounded rescue.** `.marked` becomes retryable across a generation ONLY when the
asset's every day-0 mark is degraded (`DayZeroMarkCensus.isRescuable`), at most
`maxRescueAttempts == 1` per generation, enforced by a persisted
`rediff_day_zero_attempts.rescueAttemptCount` (schema v43). `currentGeneration` 1 -> 2.
The mint's overlap filter is relaxed in exactly one direction: a STRICT slot may supersede this
producer's own degraded, unsettled rows, and nothing else.

**Why the promotion is provable rather than inferred.** The rescue does not re-label an existing
row. It re-runs the byte diff on the current build and lets
`RediffSlotOwnership.strictByteExactMask` classify the result, exactly as a first-listen mint
does. The anchor and the eligibility gate come from that fresh classification. A pre-qs0d row
whose slot really was strict gets its anchor because the differ proves it again; one that was
segment-recovered does not, because the differ says so again.

**The rescue ceiling is keyed on the MARKS, not on `lastExit`.** A rescue that ends in a
retryable exit (`.noDivergentSlot`) would otherwise fall through into the ordinary
three-attempt budget — ~324 MB where the policy promises ~108 MB. `UG09` is the mutation that
proves the difference.

## 6. Two things found while re-reading, not while testing

**A vetoed anchored row was un-proving itself.** `DayZeroMarkCensus.classify` originally tested
`isSettled` before `isFullyAnchored`, so a day-0 mark that was anchored AND later vetoed landed
in `settledCount` and vanished from `anchoredCount`. The rescue's whole safety argument is
"one anchored row proves the mint could stamp an anchor, so the unanchored siblings are
provably 9s6q segment-recovered" — and that proof was being erased by a user gesture that has
nothing to do with it. An asset whose only anchored mark the user vetoed would have read back
`isRescuable` and the rescue would have re-derived exactly the slots qs0d withheld.

The buckets stay disjoint and still sum to the same total either way, which is why the ordering
read as tidiness. Fixed by testing anchored first; `UG14` is the rail, and it is deliberately
phrased as a re-ordering rather than a deletion because that is the shape the defect had.

**The mid-session ingest door is additive-only.** `SkipOrchestrator.forwardPersistedAdWindows`
re-reads `ad_windows` and forwards; nothing removes an in-session window whose row has since
been DELETED. Until this bead no producer retired a day-0 row mid-session, so it could not
matter. Now a strict rescue retires its degraded row and playhead-96ot delivers the replacement
into the live session — the new row arrives, the retired one does not leave, and the listener
can hold a stale banner over the span the new row auto-skips. Bounded (one play, self-corrects
at `endEpisode`) and NOT fixed here: teaching that door to retire vanished ids is a real
behavioural change needing its own rails, and an empty read must never be read as "everything
was deleted". Filed as **playhead-dyhq**.

## 7. The UG mutation battery, and the two rails it repaired

14 mutations, batches 250-260, **0 survivors** after two repairs the battery itself forced:

* **UG03 SURVIVED** on the first pass. `supersedableIsNarrow` claimed "only a degraded,
  UNSETTLED, day-0 row is supersedable" and had no settled case, so dropping `!isSettled`
  left it green while a re-mint deleted a user veto. One negative per conjunct now.
* **UG13 SURVIVED** on a mis-scoped expectation of mine. Every trigger rail handed
  `attemptContextProvider` a hand-built context, so none of them could observe the STORE read
  — a `fetchDayZeroAttemptContext` returning an empty census would leave the whole suite green
  while no trapped asset on any device was ever rescued. Repaired with a NEW rail
  (`rescueReadsTheCensusFromTheStore`) that drives the production provider expression over
  rows and a record actually on disk, plus the anchored-row witness so "it fetched" cannot be
  satisfied by a provider that ignores the census. Not by narrowing the expectation.
