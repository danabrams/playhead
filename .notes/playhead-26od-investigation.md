# playhead-26od — investigation notes

## The defect, restated from the code

`FoundationModelClassifier.coarsePassAUnbounded` banks every screened window in a
LOCAL `var windows: [FMCoarseWindowOutput]` (`FoundationModelClassifier.swift:1904`)
and hands it to the caller only via the `return FMCoarseScanOutput(...)` at the
end. `BackfillJobRunner.runJob` is the only thing that writes a scan row, and it
does so from that returned value (`BackfillJobRunner.swift:1896-1921`). So the
window results have exactly one durability event — pass return — and a pass that
never returns loses all of them.

A healthy coarse pass is 12–45 minutes of FM wall clock. An app-lifecycle window
or a BG grant is ~30 minutes. The two do not fit, which is why the 2026-08-03
device pull shows ~4.6 hours of running time and zero durable scan rows.

## What already exists (and what does not)

* `onProgress` (playhead-qk44) already threads a per-resolved-window callback
  from `runJob` into the pass — `FoundationModelClassifier.swift:1843` (planning
  prologue) and `:1933` (top of iteration N, which proves window N-1 resolved:
  banked, failed or recovered). Today it only touches the `backfill_jobs` lease
  (`BackfillJobRunner.swift:1803-1807`). **This is the correct hook point and it
  is already reasoned about in the source:** the tick is taken at the top of the
  next iteration precisely because the loop body has a dozen `continue` exits.
* Row ids are DETERMINISTIC — `makeScanResult` derives the id from
  `(assetId, transcriptVersion, pass, atomWindowKey, jobId)`
  (`BackfillJobRunner.swift:3757-3766`). Combined with
  `UNIQUE(reuseKeyHash)` + `INSERT OR REPLACE`
  (`AnalysisStore.swift:16819-16828`) a repeat write of the same window is an
  exact replace, not a duplicate row. VERIFIED, as the bead asked.
* `insertSemanticScanResult` probes only for NON-success rows
  (`AnalysisStore.swift:16802-16817`): a `.success` row never bumps
  `attemptCount`, a non-success row that repeats DOES
  (`max(existing + 1, incoming)`). **Consequence: it is safe to write a success
  row twice and NOT safe to write a failure row twice.** The checkpoint therefore
  banks successes only; failures stay at end-of-pass where they are today.
* `checkpointBackfillJobProgress` (`AnalysisStore.swift:15371-15389`) has NO
  status guard — it updates by `jobId` alone, so it is safe to call while the row
  is still `running`.
* `resetStrandedBackfillJobs` (`AnalysisStore.swift:15797-15810`) sets only
  `status` and `updatedAt`, so a `progressCursor` checkpointed mid-flight
  SURVIVES the reaper's `running` -> `queued` flip. That is what makes the
  cursor half of this fix reach the next run.
* `narrowedForResume` (`BackfillJobRunner.swift:1665-1678`) is the ONLY
  sub-episode skip in production, and it is driven by `progressCursor`.
* `fetchReusableSemanticScanResult` (`AnalysisStore.swift:17090`) has **no
  production caller** and could not serve as a skip anyway: `reuseScope` is the
  `jobId`, so rows from job A can never reuse-match job B.

So persisting rows alone satisfies "the screened windows survive" but NOT "a
subsequent run does not re-do them". The cursor must be checkpointed too.

## Which deaths this covers

| death | today | after |
|---|---|---|
| jetsam / process kill | everything lost; reaper -> `queued`; rescan from zero | rows durable, cursor durable, resume skips the covered prefix |
| `FMNoProgressError` (watchdog abandons the pass) | everything lost, job `.failed` | rows durable |
| any other throw out of `coarsePassA` | everything lost | rows durable |
| graceful `CancellationError` | already OK — the pass returns partially and t1kq salvages the cursor | unchanged |

## Design

1. `coarsePassA`/`coarsePassAUnbounded` gain ONE optional callback,
   `onWindowsBanked`, fired at the same place `progress(planIndex)` is fired,
   carrying the episode-ordered plan list plus the windows and failures banked
   so far.
2. The coverage walk currently inline at `BackfillJobRunner.swift:1946-1996` is
   extracted verbatim into a pure `nonisolated static` function so the mid-flight
   cursor is computed by the IDENTICAL rule as the end-of-pass one.
3. `runJob` installs a checkpoint that, per resolved window: writes the success
   rows it has not written yet, and checkpoints the honest cursor when the
   contiguous covered prefix strictly advances.
4. The end-of-pass digest is UNTOUCHED. Its re-insert of an already-checkpointed
   success row is an exact `INSERT OR REPLACE` of an identical row under an
   identical deterministic id, so the database a normally-completing pass leaves
   behind is unchanged.

### Write cost

Per resolved window the checkpoint adds at most one small row INSERT and one
`backfill_jobs` UPDATE. `onProgress` already performs one `backfill_jobs` UPDATE
per resolved window (qk44), so this does not change the write CADENCE, only the
payload. At 12–45 min per pass and tens-to-low-hundreds of windows that is on the
order of one extra row write every 10–20 seconds.

No batching, deliberately: a batch of N trades N-1 windows of guaranteed-durable
FM work for a write saving on a path that is already writing at that cadence, and
the whole point of the bead is that a window's worth of FM compute is expensive
and a row write is not.

## What adversarial review changed

The first cut had a real defect and three weaker spots. Recorded here because the
defect is the same shape as the bug the bead is about — a quantity that claims
more than it knows.

* **The cursor followed INTENT, not persistence.** The row loop steps over a
  failed write on purpose (retrying it every checkpoint would spend the rest of
  the pass on a row a permanent validator error rejects every time). But the
  coverage walk was computed from `banked.windows` — the in-memory outcomes — so
  a window whose row did NOT land still advanced the contiguous prefix, and
  `narrowedForResume` would then skip audio no row covers. A permanent, silent
  coverage hole: exactly pmp9, reintroduced by the fix for a different bug. The
  box now tracks `durableWindowCount` (frozen at the first failed write) apart
  from `processedWindowCount`, and the walk sees only the durable prefix.
* **The checkpoint could outlive its job.** `FMNoProgressWatchdog` abandons a
  wedged pass rather than awaiting it, so the pass body can still fire a
  checkpoint after the drain loop has moved the job to a terminal state.
  `runJob` now defuses the box on every exit path, and the closure holds the
  runner weakly — an abandoned pass parks it forever, on a device whose headline
  failure mode is jetsam.
* **One test was vacuous.** `rerunningTheSamePassIsIdempotent` re-ran a
  `.complete` job, which short-circuits before the classifier, so "no new rows"
  was true for any implementation — including a plain `INSERT` with a random row
  id. It now asserts `coarseCallCount == 0`, which pins the M-5 skip that is the
  real contract there, and the row-level idempotency claim moved to the test
  where the double write actually happens.
* **The walk was cubic in the plan count.** Each plan's line-ref `Set` was built
  inside the innermost closure; with the walk now running once per window that
  is O(P^3 * L) across a pass. Hoisted.

## The one thing deliberately NOT changed

The t1kq cancellation branch (`BackfillJobRunner.swift`, the retry arm) writes
`salvagedCursor ?? job.progressCursor` — the ADMISSION-TIME cursor. Since the
checkpoint writes the database directly, that can rewind a mid-flight
checkpoint to nil.

It looks like a bug and it is not. It fires only when the coarse pass was FULLY
covered (that is the one case where `honestCursorBox` is deliberately left
unset), and there the rewind is correct: keeping a partial cursor would make
`narrowedForResume` trim the resume to the last window, so the refinement for
every earlier ad would never run and the job would then mark complete. That is
precisely what `BackfillRateLimitDeferTests.fullCoverageCancellationDoesNotCheckpoint`
pins, and it still passes.

The cost is real — a fully-covered-then-cancelled pass re-pays its coarse FM —
but the alternative is a coverage hole, and changing it is a deliberate
alteration of t1kq's contract rather than a drive-by. Dan's call.

## Downstream consequence worth a product decision — SETTLED, read this first

**DECIDED BY DAN, 2026-08-03.** The two sections below record how the surfacing
question was raised (round I) and measured (round II). The decision they were
feeding is now made, and it is not the one round I anticipated:

* Coarse rows **persist AND surface exactly as `playhead-y3ya` (#326) already
  ships them.** An earlier reading of "persist but do not surface until refined"
  is WITHDRAWN. Nothing in this bead narrows y3ya, and the rails below encode
  the shipped behaviour rather than a candidate replacement for it:
  `midFlightCoarseOnlyRowsReachTheSuggestTier` asserts an unrefined coarse mark
  ARMS the suggest tier, `aRefinedRowSurfacesAtTheRefinedExtent` asserts a
  refined row surfaces at the refined extent.
* The genuine harm round II identified — a checkpointed row is never re-scanned,
  therefore never refined, so its coarse extent becomes permanent — is filed as
  **`playhead-o98e` (P1)**, and Dan chose "make them refinable" over narrowing
  y3ya or accepting permanent coarse extent. It is OUT OF SCOPE here.

So the "one assertion-flip away in each direction" line at the end of round II
describes a symmetry that still holds mechanically, not an open question. Read
the rest of this section as the record of how the answer was reached.

`SemanticSweepMarkComposer` mints mark-only `AdWindow`s directly from passA
`containsAd` rows, without requiring pass B, up to `maximumMarkDurationSeconds`
(300 s). It ships ON. Durable coarse-only rows used to be rare; after this bead
a killed pass leaves them routinely, and the windows below the resume cursor are
never revisited by a pass B. So the app will show MORE mark-only banners, at
unrefined coarse extent.

Bounded: `markOnly`, `.candidate`, both edges `.unanchored`, and the composer
refuses to emit over an existing window — so the worst case is a wrong banner,
never a wrong cut. But it is a frequency change on a user-visible surface and it
interacts with "a mark is worth far less than a skip and can itself cost show".
Flagged, not guarded: guarding it would be a detection-semantics change, which
this bead's non-goals exclude.

## Review round II — the surfacing question, MEASURED

Round I flagged the surfacing consequence and did not measure it. It is measured
now, and the answer is that guarding it cannot be scoped to this bead.

**What surfaces today.** `SemanticSweepMarkComposer.presenceExtents` takes every
`passA` `containsAd` row that examined its window and, where no `passB`
refinement overlaps it, emits the COARSE extent verbatim
(`SemanticSweepMarkComposer.swift`, stage 2 —
`result.append(contentsOf: narrowed.isEmpty ? [window] : narrowed)`). It ships ON
(`AdDetectionService.swift`, `semanticSweepMarkEnabled: true`) and composes from
two sites: `BackfillJobRunner.runJob`'s tail and `AdDetectionService` step 18c.
Both read every persisted row for the asset, so a row banked by a pass that DIED
is composed on the next run exactly like one from a pass that returned.

`BackfillCoarseCheckpointTests.midFlightCoarseOnlyRowsReachTheSuggestTier` pins
it end to end from REAL rows captured inside a running pass: every mark arms
`.armedSuggest`, appears in `activeSuggestWindowIDs()`, and a head-artifact
control in the same delivery still drops `.tooEarly` — so the observation is
positive in playhead-le02's sense rather than an inference from silence.
`aRefinedRowSurfacesAtTheRefinedExtent` pins the other direction.

**Why 26od makes it worse, specifically.** Before this bead a killed pass left
nothing, so its windows were eventually re-scanned AND refined by a run that
completed. After it, the prefix is durable and the resume cursor means it is
never re-scanned — so it is never refined either. `containsAd` verdicts in that
prefix stay at coarse extent PERMANENTLY. That is a new steady state, not a
larger version of an old one.

**Why the guard is not in this bead.** There is no row-level witness that
separates 26od's population from playhead-y3ya's. y3ya's own field case
(DE0784D8: `containsAd` at 508–599 and 1604–1731, no `passB`) is a pass that
RETURNED with a non-`.success` status, which skips refinement
(`if coarse.status == .success && !coarse.windows.isEmpty`) — and playhead-bkhc
and playhead-t1kq already checkpoint an honest cursor over it. Same rows, same
cursor, same absence of a refinement. Requiring a `passB` refinement before a
coarse verdict may surface therefore narrows y3ya globally.

MEASURED blast radius of that one-line change, run as a probe and reverted:

| suite | issues |
|---|---|
| `The semantic-sweep extent policy` | 21 |
| `A containsAd verdict with no seed under it becomes a candidate` | 3 |
| `A declined verdict produces nothing` | 3 |
| `The sweep marks only where the pipeline produced nothing` | 3 |
| `Semantic-sweep mark wire-in` | 4 |
| `A semantic verdict arms a suggest candidate` | 4 |

38 failing assertions across six merged y3ya suites, including its acceptance
test ("both field verdicts arm the suggest tier"). Every remaining stage of the
composer — merge, clip, width ceiling, additive-only dedupe — loses its input
and therefore its coverage unless every fixture gains a `passB` row.

So: reverting stage 2's fallback is Dan's call on playhead-y3ya, not a rail
playhead-26od can add. The rails above make the axis measured rather than blind,
and the decision is one assertion-flip away in each direction.

**Outcome (see the settled section at the top of this file): Dan took neither
flip.** Coarse rows keep surfacing as y3ya ships them, and the refinement side
gets the fix, as `playhead-o98e`.

## Review round R2 — what it changed

Two defects, one of them the same shape as the one round I caught, plus a stale
statement of fact this bead's own diff falsified.

* **The durable prefix is measured in WINDOWS; coverage is measured in PLANS,
  and one plan can be several windows.** `runCoarseRetry` banks one window per
  sub-prompt of a shrunk plan (`FoundationModelClassifier.swift`, the
  `.success(outputs)` arm) and permissive recovery banks one per recovered
  chunk. So the plan STRADDLING the point where the durable prefix stops has one
  sibling row in the store and one missing — and round I's walk, seeing only the
  sibling that landed, credited the whole plan and advanced the cursor over the
  audio of the one that did not. That is pmp9's hole again, reached through the
  one door a window-index prefix cannot close, and it is reachable because
  `insertSemanticScanResult` rejects PER ROW (window geometry, blob size), not
  only per runner. `coarseCoverageWalk` now takes the unpersisted tail as well
  as the durable prefix and disqualifies its plans exactly the way a failed
  window's plan is disqualified. End-of-pass callers pass nothing and are
  byte-identical (a failed write there THROWS, so no cursor is computed at all).
* **The box recorded a cursor it had only ATTEMPTED to write.** `advanceCursor`
  mutated before the store call, so a cursor write that threw was never offered
  again — the same "credit the intent, not the outcome" mistake `noteWrite`
  exists to avoid, with a smaller blast radius: the cursor only ever lands
  BEHIND the truth, so it costs re-scanned audio rather than a coverage hole.
  Split into `shouldAdvanceCursor` (peek) and `noteCursorWritten` (record after
  the write returns).
* **`FMInferenceDeadline.swift`'s fact 2 — "coarsePassA persists NOTHING until
  it returns" — is what the bead quotes as the statement of the defect, and this
  diff makes it false.** Corrected in place, including WHY the no-progress bound
  is unaffected: a wedge in the pre-window stretch still produces no row to
  observe, because there is no screened window yet.

Then a second R2 pass, prompted by an adversarial reviewer, found the walk's
remaining assumption:

* **The walk assumed plan time ranges do not OVERLAP, and they do.**
  `planPassA` slices plans out of a list sorted by `segmentIndex`, and takes each
  plan's bounds as the min/max over its segments — a shape playhead-csbq
  introduced precisely because the atom sequence is NOT time-monotone on 27 of 30
  device assets. So a COVERED plan can end long after an UNCOVERED plan begins:
  P0 = segments (0,30) (30,60) (60,600) and P1 = (65,95) (95,125) is enough. The
  walk breaks at P1 and returns P0's end, 600 — and `narrowedForResume`, which
  knows nothing about plans and only drops `segment.endTime <= cursor`, then
  deletes both of P1's segments from the resume. Nothing plans them again.

  The cursor is now capped at the earliest uncovered plan's `startTime`.
  Sufficient and exact: every segment of that plan has
  `endTime > startTime >= cursor`, so none can be dropped. On a time-monotone
  episode every uncovered plan starts at or after the covered prefix ends, so the
  `min` picks the walked bound and the cap is a NO-OP — which is why it moves
  nothing pmp9, bkhc, qbib or t1kq compute on a healthy asset, and why no
  existing cursor test changed.

  This one was pre-existing in the shared walk. It is fixed here rather than
  filed because playhead-26od is what makes the cursor get written on the jetsam
  path, and the bead's own field evidence is that the jetsam path is the dominant
  one (10 of 10 jobs in the 2026-08-03 pull).

## What R2 did NOT fix, and why — playhead-u99x (P1, filed)

The same adversarial pass found a SECOND cursor hazard that this bead must not
fix: `lastProcessedUpperBoundSec` is an episode-time SCALAR, and `runJob` applies
`narrowedForResume` to the ALREADY phase-narrowed inputs. For
`.scanHarvesterProposals` / `.scanLikelyAdSlots` / `.scanRandomAuditWindows` the
anchor population is RE-DERIVED every run — and for the audit phase it is
re-derived *deliberately differently*, since `episodesSinceLastFullRescan` is
mixed into `auditSeed` so consecutive observations rotate across distinct audit
windows. A cursor that was honest about run 1's anchors can therefore delete run
2's anchors, which no row covers.

Three reasons it is filed rather than fixed:

1. It is not a defect in any writer. At the instant it is written the cursor is
   honest about its own population; the unsound step is the CONSUMER applying an
   episode-time scalar to a different population. That is pmp9's contract.
2. It is pre-existing and already reachable: pmp9's rate-limit defer and t1kq's
   cancellation salvage both persist this cursor for those phases today.
3. **No 26od-local change closes it.** Gating the mid-flight cursor write to
   `.fullEpisodeScan` — the obvious containment — leaves the identical hole
   reachable through pmp9 and t1kq, so it would buy partial risk reduction by
   giving up this bead's resume win on three phases, and still not fix the bug.

The real fixes (narrow only for `.fullEpisodeScan`; or make the resume key off
"has a durable row" rather than a time scalar; or freeze the anchor set per job)
are all changes to a shipped contract. Filed with those three directions and the
test a fix needs — one that runs a targeted phase twice with a DIFFERENT anchor
set, which no existing test does.
