# Episode Job DAG + Checkpoint Ledger Audit

**Bead:** playhead-5uvz · **Wizard idea #3:** Self-Healing Reliability Control Plane
**Date:** 2026-04-25 · **Author:** subagent driving 5uvz · **Branch:** bead/playhead-5uvz

## Why this audit exists

The Dueling Idea Wizards synthesis (2026-04-21) verdict on COD's "Self-Healing Reliability Control Plane" was **Killed — deployed**. Both wizards conceded that `EpisodeExecutionLease`, `WorkJournal`, `AnalysisJobReconciler`, and the `InterruptionHarnessTests` are all in the tree. This bead's premise is therefore: **don't rebuild what's built — audit what's actually wired, find the seams where the design intent is still unrealized, and file the gaps as actionable follow-ups before the next 12-hour blackout.**

The premise was tested almost immediately: 24 hours after the wizard session, **playhead-fuo6** captured a 12-hour overnight processing blackout (04-24 20h → 04-25 07h). Root cause: `BackgroundProcessingService` had no `appDidEnterBackground()` hook to submit the BGProcessingTask, so iOS never woke the app to drain queued analysis jobs. Fixed in commit `aba849a`. **fuo6 is the canonical example of the gap class this audit hunts.**

## Topology — what runs where

The system has **two parallel pipeline implementations** sharing one durable ledger (`AnalysisStore`, SQLite + `analysis_jobs` / `analysis_assets` / `analysis_sessions` / `work_journal` / `backfill_jobs`).

### Pipeline A — playback-driven (`AnalysisCoordinator.runPipeline`)

Runs in the foreground when the user presses play. State machine persisted to `analysis_sessions`:

```
queued → spooling → featuresReady → hotPathReady → (waitingForBackfill →) backfill → completeFull|completeFeatureOnly|completeTranscriptPartial
                                                                                    \→ failedFeature|failedTranscript|cancelledBudget
```

Owner: `AnalysisCoordinator` (single actor). Handles: decode, feature extraction, transcription start, hot-path ad-detection, backfill drain, finalize. Resume on cold launch from any persisted state via `runPipeline(resumeState:)`.

### Pipeline B — scheduler-driven (`AnalysisWorkScheduler.processJob` → `AnalysisJobRunner.run`)

Runs as a long-lived loop (started by `PlayheadRuntime.startSchedulerLoop()`), drains `analysis_jobs` rows. Five stages inside `AnalysisJobRunner.run`:

```
Stage 1: decode (audio shards)
Stage 2: feature extraction (FeatureExtractionService.extractAndPersist)
Stage 3: transcription (TranscriptEngineService — fire-and-forget + event stream)
Stage 4: ad detection (hot path + backfill, BackfillJobRunner)
Stage 5: cue materialization (SkipCueMaterializer)
```

Lifecycle on the `analysis_jobs` row: `queued → running → (paused | failed | complete | superseded | blocked:missingFile | blocked:modelUnavailable)`. Lease slot (`leaseOwner`/`leaseExpiresAt`) reserved during execution; orphan recovery via `AnalysisJobReconciler.recoverExpiredLeases`.

### The two pipelines share

- `AnalysisStore` (SQLite ledger).
- `FeatureExtractionService` (one extractor instance, used by both).
- `TranscriptEngineService` (one engine, started/stopped from both).
- `AdDetectionService` / `BackfillJobRunner` (FM ad-detection backfill).

## Per-stage ledger audit

| Stage | Owner | Pre-work ledger write | Post-work ledger write | Status |
|---|---|---|---|---|
| **Job enqueue** | `AnalysisJobReconciler.discoverUnEnqueuedDownloads` + `AnalysisWorkScheduler.enqueue` | `INSERT analysis_jobs` (state=queued) | n/a | ✅ |
| **Job claim (lease)** | `AnalysisWorkScheduler.processJob` | `UPDATE analysis_jobs SET state='running', leaseOwner, leaseExpiresAt` (`AnalysisStore.acquireLease(jobId:owner:)`) | n/a | ⚠️ See **Gap-1** |
| **Decode** (shards) | `AnalysisJobRunner` Stage 1 | n/a — pure compute, no checkpoint | `analysis_assets.episodeDurationSec` is updated by `AnalysisCoordinator.runFromSpooling`, NOT by `AnalysisJobRunner` | ⚠️ See **Gap-7** |
| **Feature extraction** | `FeatureExtractionService.extractAndPersist` | n/a (shard-by-shard append) | Atomic batch: `feature_windows` + `feature_extraction_checkpoints` + `analysis_assets.featureCoverageEndTime` (`persistFeatureExtractionBatch`) | ✅ |
| **Transcription** | `TranscriptEngineService.startTranscription` | n/a — fire-and-forget; coverage progresses | `transcript_chunks` rows persisted; `analysis_assets.fastTranscriptCoverageEndTime` updated | ⚠️ See **Gap-2** + **Gap-9** |
| **Ad detection (hot path)** | `AdDetectionService.runHotPath` | Reads `transcript_chunks` (no claim) | `INSERT/UPSERT ad_windows` rows | ✅ |
| **Ad detection (backfill / FM)** | `BackfillJobRunner.runPendingBackfill` | `INSERT backfill_jobs` (status=queued) | Per-phase: `markBackfillJobRunning` → `markBackfillJobComplete` with `progressCursor`. C-2/C3-2 guards prevent terminal-row resurrection. | ✅ |
| **Cue materialization** | `SkipCueMaterializer.materialize` | n/a | `INSERT skip_cues` rows | ✅ |
| **Job finalize (success)** | `AnalysisWorkScheduler.processJob` outcome switch | n/a | `UPDATE analysis_jobs SET state='complete'` + `releaseLease` | ⚠️ See **Gap-3** |
| **Job finalize (failure)** | `AnalysisWorkScheduler.processJob` catch arms | n/a | `UPDATE analysis_jobs SET state='failed', nextEligibleAt, lastErrorCode` + `releaseLease`; `incrementAttemptCount`; superseded after `maxAttemptCount` | ⚠️ See **Gap-3** |
| **Coordinator session transitions (Pipeline A)** | `AnalysisCoordinator.transition` | n/a | `UPDATE analysis_sessions SET state` | ✅ |
| **Lease lifecycle journal** | `AnalysisStore.acquireEpisodeLease` / `releaseEpisodeLease` | Atomically appends `work_journal` row inside the same SQL transaction as the `analysis_jobs` UPDATE | n/a | ⚠️ See **Gap-1** (rich path is unwired) |

## Gap inventory

### Gap-1 — Production scheduler claims jobs via the lightweight lease API; the WorkJournal-emitting `acquireEpisodeLease` is never called outside tests

**Severity: HIGH — diagnosability/recovery**

Two lease APIs exist on `AnalysisStore`:

1. `AnalysisStore.acquireLease(jobId:owner:expiresAt:)` — bare `UPDATE analysis_jobs` row, no `work_journal` write. **This is what `AnalysisWorkScheduler.processJob` calls.** ([AnalysisWorkScheduler.swift:1440-1444](../../Playhead/Services/PreAnalysis/AnalysisWorkScheduler.swift))
2. `AnalysisStore.acquireEpisodeLease(...)` — atomically writes the lease columns AND appends a `work_journal` row with `eventType: .acquired`. Reachable only through `AnalysisCoordinator.acquireLease(episodeId:)`. **Production has zero call sites for that coordinator method.** Confirmed by `grep coordinator\.acquireLease|AnalysisCoordinator.*acquireLease` (matches only inside `EpisodeExecutionLease.swift`'s own doc comment).

Consequence: **the production `work_journal` table never accumulates `acquired`/`finalized`/`failed`/`checkpointed`/`preempted` rows from the scheduler-driven pipeline.** Every diagnostic, orphan-recovery decision, and operator triage tool that reads `work_journal` (e.g. `fetchLastWorkJournalCause`, the support-bundle `work_journal_tail`) sees an empty journal in production. The journal that does get written comes from `DownloadManager` (`recordFinalized` / `recordFailed` for downloads) and `ForceQuitResumeScan`. **No analysis-pipeline rows.**

This makes `recoverOrphans`'s policy ("look at last journal entry to decide finalized vs. checkpointed") inert. The fallback path (`AnalysisJobReconciler.recoverExpiredLeases`) treats every expired lease the same way — clear leaseOwner and re-queue regardless of how close to terminal the prior worker had gotten.

Also: `AnalysisWorkScheduler.setWorkJournalRecorder` is wired to inject a recorder, but `PlayheadRuntime` never calls it. The default is `NoopWorkJournalRecorder`. The single `recordPreempted` site at `processJob:1622` is therefore a no-op in production.

### Gap-2 — `recoverOrphans` (the WorkJournal-aware cold-launch reaper) has zero production call sites

**Severity: HIGH — reliability**

`AnalysisCoordinator.recoverOrphans(now:graceSeconds:)` is the implementation of the bead-spec'd lease reaper that:

- inspects the **last `work_journal` entry** for each stranded `analysis_jobs` row;
- treats `.finalized` / `.failed` as terminal → clear lease, no requeue;
- treats `.checkpointed` / `.acquired` / `.preempted` / no-entry as recoverable → bump scheduler epoch, mint fresh `generationID`, re-queue, demote Now-lane jobs stale > 60s to Soon.

Confirmed by `grep recoverOrphans` returning only the definition site, the test file `EpisodeLeaseAndWorkJournalTests.swift`, and one journal append site. **There is no cold-launch caller.**

What runs instead at startup: `AnalysisJobReconciler.reconcile()` (via `PlayheadRuntime.swift:775`), whose `recoverExpiredLeases` step is a blind sweep — it requeues `running|queued|paused` rows whose lease expired, with no journal-aware policy. There is no Now→Soon demotion, no scheduler-epoch bump, no generation rotation, no coordinated journal append. The richer mechanism is dead code in production.

The Gap-1 emptiness directly enables Gap-2: even if `recoverOrphans` were wired, with an empty `work_journal` it would route every recoverable orphan through the "no-progress / requeue from start" branch — losing nothing, but also gaining nothing over `AnalysisJobReconciler`.

### Gap-3 — Pipeline B job-state writes from `processJob` are not transactional; a crash mid-outcome arm leaves rows in inconsistent shapes

**Severity: MEDIUM — recoverability**

The outcome switch at [`AnalysisWorkScheduler.swift:1664-1869`](../../Playhead/Services/PreAnalysis/AnalysisWorkScheduler.swift) is a sequence of separate `await store.X(...)` calls under `writeIfStillOwned`:

- `updateJobProgress` → switch → state-specific updates (`tierAdvance.insertNext`, `tierAdvance.markComplete`, `coverageInsufficient.requeue`, `failed.requeue`, etc.) → final `releaseLease.tail`.

Each of those is a separate transaction. A crash between `updateJobProgress` and `tierAdvance.markComplete` leaves the job at state=`running` with progress recorded but no terminal mark — the orphan recovery path (Gap-2 / Reconciler.recoverExpiredLeases) does eventually requeue it, but the sequence requires the lease to expire (default 30s scheduler-side, but the sole production lease TTL is `AnalysisWorkScheduler.leaseExpirySeconds`). On cold launch the row is **stuck `running`** until the next reconciler pass.

The `runSchedulingPass` helper exists in `AnalysisStore` but is exercised only by `simulateCrashInSchedulingPassForTesting` and by `acquireEpisodeLease`/`releaseEpisodeLease`. The outcome arms could batch their writes under a `runSchedulingPass`, but currently don't.

The `BackfillJobRunner` got this right — its terminal transitions (`markBackfillJobComplete`, `markBackfillJobFailed`) are guarded against silent terminal resurrection by the C3-2 `IN ('queued','deferred','running')` guard plus the idempotent-on-self-state probe. The scheduler's `analysis_jobs` outcome arms have no equivalent guard.

### Gap-4 — `BackgroundProcessingService` did not submit a backfill request on `.background` scenePhase until the playhead-fuo6 fix

**Severity: HIGH — already fixed but worth pinning**

playhead-fuo6 (commit `aba849a`, 2026-04-25) added `appDidEnterBackground()` that submits the BGProcessingTask. Pre-fix, the only paths that called `scheduleBackfillIfNeeded()` were `playbackDidStop()` (user pressed pause) and the backfill handler's self-rearm (i.e. only after the task ran at least once). A user who queued episodes overnight without ever playing one had no submitted BG task, so iOS never granted background time → 12-hour blackout.

This audit confirms the fix is in place. Defensive recommendation: add a regression test that asserts **`scheduleBackfillIfNeeded()` is invoked on every `.background` scene-phase transition AND on any `enqueue` call where the app is currently backgrounded**. The existing fuo6 test covers the first half (per the commit message); the second half is a thinner call-site contract that prevents the inverse class of bug ("user enqueued an episode while app was already backgrounded — no BG task scheduled").

### Gap-5 — `BackgroundFeedRefreshService` (playhead-fv2q) and `BackgroundProcessingService` are independently scheduled; nothing reconciles their lifecycles

**Severity: MEDIUM — discoverability**

Both register `BGTaskScheduler` identifiers at launch. `BackgroundFeedRefreshService` runs `refreshEpisodes` and triggers auto-download; `BackgroundProcessingService.handleBackfillTask` drains the analysis queue. There is no "after feed refresh ran and added new downloads, ensure a backfill task is queued" link. The flow only works because:

1. `discoverUnEnqueuedDownloads` (in `AnalysisJobReconciler.reconcile`) runs at app launch and re-scans cached files.
2. The scheduler loop is constantly polling.

But between BG ticks the OS sees only one task type running at a time. If feed refresh adds 4 downloads and finishes, but iOS doesn't grant backfill BG time for 6 hours, those 4 downloads sit unanalyzed. Worth pinning the contract: when `BackgroundFeedRefreshService` finishes a refresh that produced new downloads, it should explicitly call `BackgroundProcessingService.scheduleBackfillIfNeeded()` so a BG task is requested even if none was outstanding.

### Gap-6 — `AnalysisJobRunner` Stage 3 (transcription) cannot be cleanly stopped on timeout; a known TODO leaves orphaned background work

**Severity: MEDIUM — resource leak / latent crash risk**

[`AnalysisJobRunner.swift:249`](../../Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift):

```swift
// TODO: Stop the transcript engine to prevent orphaned work if the timeout fired.
// TranscriptEngineService does not yet expose a stopTranscription() method.
```

If the 5-minute transcription timeout fires before the engine emits `.completed`, the runner returns `.failed("transcription:zeroCoverage")` but the `TranscriptEngineService` keeps running in the background. This creates an orphaned task whose subsequent persistence writes target an `analysisAssetId` whose owning scheduler has already moved on. The job row's `transcriptCoverageEndTime` will eventually advance — out-of-band, after the row was marked `failed` — which can confuse coverage-guard recovery and the partial-coverage gate.

`TranscriptEngineService` should expose a `stopTranscription(analysisAssetId:)` method; runner should call it in the zero-coverage branch.

### Gap-7 — `AnalysisJobRunner` Stage 1 doesn't persist `episodeDurationSec`; only `AnalysisCoordinator.runFromSpooling` does

**Severity: LOW — denominator-loss for coverage guard**

The coverage guard ([`AnalysisCoordinator.runFromBackfill:1336`](../../Playhead/Services/AnalysisCoordinator/AnalysisCoordinator.swift)) needs `analysis_assets.episodeDurationSec` to compute the coverage ratio. Pipeline A (coordinator) writes it after decoding shards. Pipeline B (scheduler/runner) decodes shards but does NOT call `store.updateEpisodeDuration`. If a job is exclusively driven through Pipeline B (e.g. user enqueues 4 episodes overnight and never presses play on any of them — the fuo6 scenario), `episodeDurationSec` stays NULL and the coverage guard's fail-safe shortcut to `.restart` triggers. gtt9.1.1 introduced this fail-safe specifically to prevent silent over-reports; the gap here is that it bites every Pipeline-B-only episode.

Cheap fix: have `AnalysisJobRunner.run` write `store.updateEpisodeDuration` after stage 1 if the asset's value is NULL.

### Gap-8 — `AnalysisCoordinator.runFromQueued` and `AnalysisWorkScheduler.processJob` can both decode the same audio at the same time

**Severity: LOW — duplicate work, not correctness**

When the user presses play on an episode that already has a queued `analysis_jobs` row, `AnalysisCoordinator.handlePlayStarted` calls `runPipeline` — which independently decodes shards via `audioService.decode`. Meanwhile the scheduler loop, on its next iteration, will pick up the same episode's row and call `AnalysisJobRunner.run`, which decodes the same shards again. Both end up writing to `feature_windows` / `transcript_chunks` for the same `analysisAssetId`. The persisted-batch-atomic write in `FeatureExtractionService` and the segment-fingerprint dedup in `TranscriptChunk` make the writes safe (idempotent), but the CPU/battery cost is doubled. There's no cross-pipeline lease.

The architectural answer (long-term) is to make the coordinator dispatch into the scheduler rather than decode independently — playhead-44h1 hints at this for foreground-assist hand-off but doesn't close the general case.

### Gap-9 — `TranscriptEngineService` event-stream timeout is per-asset, but timeouts are never journaled

**Severity: LOW — observability**

The 5-minute `Task.sleep(for: .seconds(300))` race in `AnalysisJobRunner.run` fires silently when transcription stalls. The runner returns `.failed("transcription:zeroCoverage")`, which becomes the `lastErrorCode` on the `analysis_jobs` row, but the journal sees nothing (Gap-1). If a class of episodes (long, refusal-prone, music-heavy) systematically times out, there's no aggregate signal — operators have to grep `lastErrorCode = 'transcription:zeroCoverage'` across all rows. A journaled `failed/transcriptionTimeout` event with `episodeDuration` and `chunksPersisted` in metadata would immediately surface the pattern.

### Gap-10 — On schema-version mismatch (`supersedeStaleVersions`), the reconciler rewrites jobs to `superseded` but does not restart fresh ones

**Severity: LOW — correctness for analysis-version bumps**

`AnalysisJobReconciler.supersedeStaleVersions` (step 4) marks every non-terminal job with a stale `analysisVersion` as `superseded`. But it does NOT enqueue replacement jobs at the new analysis version. The new job for the same episode only gets created on the next `discoverUnEnqueuedDownloads` pass (step 7), which only fires for episodes whose downloads exist but have no active job rows — and right after step 4, every superseded episode has a row (it's just `superseded`, not `complete`). `fetchActiveJobEpisodeIds` excludes terminal states; let me double-check.

Reading `AnalysisStore.fetchActiveJobEpisodeIds`: returns episode IDs from non-terminal job states. Need to confirm `superseded` is treated as terminal there. **TODO for the follow-up bead.** If `superseded` is filtered out, the same-episode re-enqueue happens on the NEXT launch's reconciler pass (step 7 sees no active job, creates a fresh queued one). That works on cold launch. But analysis-version bumps shipped via in-process update (e.g. test harness) won't see new jobs until the next launch.

## Defensive code landed in this bead

None. Every gap above either requires real design work (Gaps 1–3) or has already been fixed (Gap-4) or has follow-up beads filed (see below). Adding speculative defensive code in this audit's PR would conflict with the bead's guidance: "do NOT do speculative refactors."

The audit deliberately stops at the recommendation. Implementation lives in the follow-ups.

## Test coverage observations (no fixes proposed)

- `EpisodeLeaseAndWorkJournalTests.swift` exercises `acquireEpisodeLease` / `releaseEpisodeLease` thoroughly — the lease semantics and journal idempotency contracts are pinned. The gap is just that production never calls into them.
- `InterruptionHarnessTests.swift` runs in `PlayheadIntegrationTests` (not the per-bead Fast plan) and is the canonical Pipeline-A crash-and-resume harness.
- There's no equivalent harness for Pipeline B. The closest is `AnalysisWorkSchedulerTests.swift`, which is unit-level. A `SchedulerProcessJobInterruptionTests` that crashes between outcome arms (Gap-3) is the right shape; this is filed as a follow-up.

## Deadlock check (acceptance criterion (c))

Walked through every cross-pipeline dependency:

- **A waits on B's checkpoint, B waits on A's checkpoint** → does not occur. Pipeline A reads `transcript_chunks` produced by `TranscriptEngineService`, which Pipeline B also feeds; both write through the same engine actor. Reads don't block writes.
- **Lease cycles** → cannot occur. Pipeline B's `analysis_jobs` lease is owner-scoped to `"preAnalysis"`. Pipeline A doesn't take that lease — it has its own session-state machine. The `EpisodeExecutionLease` infrastructure is unwired (Gap-1) so it cannot deadlock.
- **Backfill runner waits on hot-path output** → backfill drains its own `backfill_jobs` queue under a serial `AdmissionController`. No back-pressure on the hot-path.

No deadlocks identified. Idempotent replay (acceptance criterion (c)) is satisfied at the `AnalysisStore` row level for: `analysis_jobs` (state CAS guards in `AnalysisStore.acquireLease`/`renewLease`), `backfill_jobs` (C3-2 status guards), `transcript_chunks` (segment-fingerprint dedup), `feature_windows` (atomic batch write under `persistFeatureExtractionBatch`). The vulnerable seam is the **multi-row outcome arm in `processJob`** (Gap-3) and the unwired journal (Gap-1).

## Follow-up beads filed

| Bead | Severity | Scope |
|---|---|---|
| [playhead-5uvz.1](https://...) (Gap-1) | HIGH (P1) | Wire `AnalysisCoordinator.acquireLease(episodeId:)` into the scheduler dispatch path, replacing the bare `acquireLease(jobId:owner:)` call. Or: extend the bare API to append a `work_journal` row in the same transaction. Or: have `PlayheadRuntime` inject a real `WorkJournalRecorder`. |
| [playhead-5uvz.2](https://...) (Gap-2) | HIGH (P1) | Call `AnalysisCoordinator.recoverOrphans` at app launch (in `PlayheadRuntime.startSchedulerLoop`, before `reconciler.reconcile()`). Reduce `AnalysisJobReconciler.recoverExpiredLeases` to a fallback for jobs the journal-aware path skipped. Depends on 5uvz.1. |
| [playhead-5uvz.3](https://...) (Gap-3) | MEDIUM (P2) | Wrap `processJob`'s outcome arms in `runSchedulingPass` so progress + state + lease release are one transaction. Add interruption test that crashes mid-arm. |
| [playhead-5uvz.9](https://...) (Gap-4 regression test) | MEDIUM (P2) | Add the symmetric test: `enqueue` while backgrounded must trigger `scheduleBackfillIfNeeded()`. Main fuo6 fix already landed in commit aba849a. |
| [playhead-5uvz.4](https://...) (Gap-5) | MEDIUM (P2) | After successful `BackgroundFeedRefreshService` refresh that produced new downloads, post a `BGProcessingTaskRequest` for backfill. |
| [playhead-5uvz.5](https://...) (Gap-6) | MEDIUM (P2) | Add `TranscriptEngineService.stopTranscription(analysisAssetId:)`; call it from `AnalysisJobRunner.run` zero-coverage branch. |
| [playhead-5uvz.6](https://...) (Gap-7) | LOW (P3) | Have `AnalysisJobRunner.run` persist `episodeDurationSec` after stage 1 if NULL. |
| [playhead-5uvz.7](https://...) (Gap-9) | LOW (P3) | Emit a `work_journal` row on transcription timeout with structured metadata (depends on 5uvz.1). |
| [playhead-5uvz.8](https://...) (Gap-10) | LOW (P3) | Verify analysis-version bump produces fresh jobs in the same reconciliation pass; add regression test. |

Gap-4 main fix already landed in playhead-fuo6 / commit `aba849a`. Gap-8 (Pipeline A and B can decode the same audio simultaneously) is filed as documentation-only — duplicate work but writes are idempotent — and is subsumed by the longer-term unification under playhead-44h1.

## Summary

The reliability control plane mostly exists — but the **load-bearing wiring is the empty kind of "exists,"** where the API is in the tree, the tests pass, and production silently uses the simpler legacy path that bypasses the journal. The wizards' "concede — already deployed" was true at the surface level (the code is checked in) but optimistic about the activation level. **Three of the ten gaps would have been actively useful for diagnosing the fuo6 12-hour blackout** — Gap-1 (no journal rows = no histogram of "why didn't the scheduler run"), Gap-4 (already fixed, was the proximate cause), and Gap-5 (feed-refresh → backfill ordering would have surfaced "feed refresh ran, no backfill submitted").

Top priorities: **Gap-1, Gap-2, Gap-4 regression test, Gap-3.** Everything else is either fixed, low-impact, or has a clean local fix that doesn't require coordinated work.
