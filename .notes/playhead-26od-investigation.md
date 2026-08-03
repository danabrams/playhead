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

## Downstream consequence worth a product decision

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
