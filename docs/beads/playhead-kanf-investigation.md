# playhead-kanf — investigation notes

## The defect, confirmed in the current tree

`AnalysisWorkScheduler.enqueue(...)` (`Playhead/Services/PreAnalysis/AnalysisWorkScheduler.swift`)
picks the lane from the one-shot user-intent flag:

```swift
let userInitiated = pendingUserIntentEpisodes.contains(episodeId)
let priority = userInitiated ? 20 : (isExplicitDownload ? 10 : 0)
```

…then mints a **new** `AnalysisJob` and calls `store.insertJob(job)`, which is
`INSERT OR IGNORE` on `workKey TEXT NOT NULL UNIQUE`
(`AnalysisStore.swift`, `insertJob`). When the key is already taken the insert
changes zero rows and the priority-20 value is discarded with the throwaway job
struct. The existing row keeps whatever priority the auto-pipeline gave it (0 for
an auto download, 10 for an explicit one).

Immediately afterwards the flag is consumed unconditionally:

```swift
pendingUserIntentEpisodes.remove(episodeId)
pendingUserIntentCoverage[episodeId] = nil
```

So the tap is a **silent no-op in exactly the case the control exists for** — an
episode whose background analysis is already queued and starving — and the flag
is burned, so even a later enqueue for the same episode cannot recover it.

The old doc comment on `pendingUserIntentEpisodes` documented this as intended
("Recording intent for an episode that already has a queued job does NOT
retroactively promote it"). That documentation is what makes this a bead: the
behaviour is deliberate and wrong for the user's purpose.

## What state a "leased or running" row is actually in

The bead's hard constraint is: promote a `queued` row, never a leased/running one.
Reading the lease writers:

- `acquireLease(jobId:owner:expiresAt:)` — `SET leaseOwner = ?, leaseExpiresAt = ?, state = 'running'`
  guarded by `WHERE jobId = ? AND (leaseOwner IS NULL OR leaseExpiresAt < ?)`.
- `acquireLeaseWithJournal(...)` — same columns plus `generationID` / `schedulerEpoch` rotation.
- `acquireEpisodeLease(...)` — `state = CASE WHEN state IN ('complete','superseded') THEN state ELSE 'running' END`.

So the normal leased row is `state='running' AND leaseOwner IS NOT NULL`. But the
two conditions are **not** equivalent in general:

- `fetchNextEligibleJob` selects `state IN ('queued','paused') AND (leaseOwner IS NULL OR leaseExpiresAt < ?)`
  — i.e. the schema tolerates a `queued` row that still carries a stale lease.
- `restampQueuedJobEpochs` already treats `state = 'queued' AND leaseOwner IS NULL`
  as the safe-to-touch predicate, for exactly this reason.

Therefore the promotion predicate is **`state = 'queued' AND leaseOwner IS NULL`** —
strictly stronger than the bead's `state='queued'`, and both halves are tested
independently (a `running` row with no lease, and a `queued` row with a lease).

`paused` is deliberately **not** promotable even though it is unleased and
dispatchable: one promotable state keeps the rail crisp, and the retained flag
(below) means a paused row that returns to `queued` is promoted by the next tap.

## Why the promotion is a single store call

`fetchJob(byWorkKey:)` followed by `updateJobPriority(jobId:priority:)` would be
two `await` hops with a lease acquisition possible in between. `AnalysisStore` is
an `actor`, so a single store method that reads and then writes cannot be
interleaved by another store call — and the `UPDATE` re-states the full predicate
so it is its own compare-and-swap even if the read were stale.

The existing `updateJobPriority` (playhead-dqfm) is `throws -> Void`: it cannot
tell a caller whether a row changed, which is precisely the fact needed to decide
whether the one-shot flag was earned. Rather than change its signature (one
caller, `AnalysisJobReconciler`, relies on the void form), kanf adds a dedicated
`promoteQueuedJobToUserIntentLane(workKey:priority:now:)` returning a four-case
outcome.

## The one-shot flag

Consumed **iff the intent was served**, which is any of:

1. the insert succeeded — the new row carries priority 20;
2. the existing row was promoted to 20;
3. the existing row was already at/above 20 — nothing to do, the intent holds.

Retained (so a later enqueue or a second tap can still honour it) when the row is
leased, running, paused, terminal, absent, or the store threw. This is the
acceptance criterion "the flag is not consumed without effect".

## Interaction with playhead-glo9

`glo9` is **CLOSED** (admission relaxation for backlog drain during playback:
charging + `.nominal` + hot-path caught up). It changes `LaneAdmission` — which
lane is *admitted* — and never touches how a row's `priority` is chosen or
written. kanf changes only the stored `priority` of an already-queued row. The
two compose: kanf moves the row into `.now`, and `.now` is the lane `glo9`'s
relaxation never gated in the first place (`nowLaneAllowed` / the ewag carve-out
keeps `.now` selectable even when deferred work is blocked).

## Interaction with playhead-kkzu (#342)

`kkzu` established that `handleBackgroundDownloadComplete` can run in a **different
process** from the download that started it, so in-memory scheduler state does not
survive. `pendingUserIntentEpisodes` is in-memory and always was; kanf does not
add new in-memory state and does not make that boundary worse. It does make the
boundary *less* costly in one direction: the durable `priority` column now carries
the promotion, so once a tap has promoted a row the promotion survives a relaunch,
whereas today nothing survived at all.
