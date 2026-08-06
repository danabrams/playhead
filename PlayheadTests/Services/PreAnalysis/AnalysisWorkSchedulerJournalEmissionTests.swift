// AnalysisWorkSchedulerJournalEmissionTests.swift
// playhead-work-journal-wiring: pin the WorkJournal terminal-row
// emission contract on the scheduler-driven path. Pre-this-fix, the
// production `work_journal` table held only `acquired` rows because
// `PlayheadRuntime` never installed a real recorder on the scheduler
// — every `recordPreempted/Failed/Finalized` site was a no-op against
// `NoopWorkJournalRecorder`. These tests exercise the actual
// `AnalysisWorkScheduler.processJob` flow with the production
// `AnalysisStoreWorkJournalRecorder` installed, so a regression to
// the no-op default would surface immediately as missing terminal
// rows in `work_journal`.
//
// **Scope: cancel-driven and runner-driven outcome arms.** The
// review-cycle-1 expansion of the WIP covers every outcome arm in
// `processJob` (eleven total). This file pins the deterministic
// subset reachable under stub inputs:
//
//   - `cancelCatch.revertQueued`  (cancel mid-decode, attempts < max) → preempted
//   - `cancelCatch.supersede`     (cancel mid-decode, attempts == max-1) → failed
//   - `failed.supersede`          (runner throws, attempts == max-1) → failed
//   - `failed.requeue`            (runner throws, attempts < max) → preempted
//   - inverse-control: default Noop recorder writes zero terminal rows
//
// The success arms (`tierAdvance` / `allTiersDone` → finalized) and
// the asset-resolution arms cannot be driven cleanly through the
// scheduler in pure-stub form: success requires the full
// decode/feature/transcript/ad-detection/cue pipeline to thread end-
// to-end, and asset-resolution failure requires injectable store
// faults the production `AnalysisStore` does not expose. Those arms
// are reachable in production but their journal emission is not
// pinned by this file. A follow-up integration-style test would
// cover them.
//
// `cancelRace.releaseLease` (cancel-before-runner-start) is also
// instrumented but cannot be reliably driven in stub form: the cancel
// must arrive AFTER lease acquisition but BEFORE the runner enters
// `decode(...)`. The scheduler runs both back-to-back inside one
// actor message, leaving no deterministic window for an external
// canceller to slip in.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — WorkJournal terminal-row emission (work-journal-wiring)")
struct AnalysisWorkSchedulerJournalEmissionTests {

    // MARK: - Test fixtures

    // skeptical-review-cycle-7 L2: the three stubs below are actors
    // rather than `final class … @unchecked Sendable`. The original
    // unchecked-Sendable shape worked for today's single-iteration
    // call pattern, but it required readers to trust that no future
    // test schedules two decode passes against the same stub. Actor
    // isolation makes the counter access fully synchronized so the
    // stub stays correct under any future call shape, including
    // stress-test harnesses that drive multiple concurrent
    // schedulers. The protocol's `async throws` decode requirement
    // is satisfied transparently by an actor-isolated method.

    /// Audio provider stub that hangs in `decode(...)` until cancelled.
    /// Mirrors the shape used in
    /// `AnalysisWorkSchedulerOutcomeBookkeepingTests` so the cancel
    /// arrives mid-decode, not before lease acquisition.
    private actor CancellableAudioStub: AnalysisAudioProviding {
        private(set) var decodeCallCount = 0

        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            decodeCallCount += 1
            // Sleep until cancelled; long upper bound so a missing
            // cancel surfaces as a clean test timeout rather than
            // returning empty shards (which would misroute through
            // the runner's `.failed` arm and mask the emission gap
            // under test).
            try await Task.sleep(for: .seconds(60))
            return []
        }
    }

    /// Audio provider stub that throws on every `decode(...)` call —
    /// drives the runner's `.failed(reason)` outcome arm. Used to
    /// pin `failed.supersede` (terminal) and `failed.requeue` (retry)
    /// emissions that the review-cycle-1 expansion added.
    private actor FailingDecodeStub: AnalysisAudioProviding {
        private(set) var decodeCallCount = 0
        let message: String
        init(message: String = "synthetic decode failure") {
            self.message = message
        }

        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            decodeCallCount += 1
            throw AnalysisAudioError.decodingFailed(message)
        }
    }

    /// Audio provider stub that returns a single empty shard so the
    /// runner clears stage 1 (decode) and reaches the
    /// post-decode `checkStopConditions()` checkpoint at
    /// `AnalysisJobRunner.run` line ~201 — where a critical thermal
    /// state trips the `.pausedForThermal` outcome arm. Used to drive
    /// the scheduler's `pausedForThermal/memoryPressure` arm under
    /// stub control without requiring a full feature/transcript
    /// pipeline pass.
    private actor SingleShardAudioStub: AnalysisAudioProviding {
        private(set) var decodeCallCount = 0

        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            decodeCallCount += 1
            return [
                AnalysisShard(
                    id: 0,
                    episodeID: episodeID,
                    startTime: 0,
                    duration: 30,
                    samples: []
                )
            ]
        }
    }

    /// Builds a scheduler with the production
    /// `AnalysisStoreWorkJournalRecorder` already installed — this is
    /// the unit under test. Without `setWorkJournalRecorder(...)`, the
    /// scheduler retains its `NoopWorkJournalRecorder` default and
    /// the very gap this fix closes would silently re-open.
    private func makeScheduler(
        store: AnalysisStore,
        audioProvider: any AnalysisAudioProviding,
        downloads: StubDownloadProvider,
        thermalStateProvider: @escaping @Sendable () -> ProcessInfo.ThermalState = {
            ProcessInfo.processInfo.thermalState
        },
        // playhead-ngev (review r1): the existing tests trip their arms before
        // transcription, so they neither need nor want a loaded recognizer —
        // both parameters default to exactly the previous behaviour. The
        // interruption tests below DO reach stage 3, which needs
        // `SpeechService.isReady()` to be true or the loop reports
        // `speech_engine_not_ready` and never exercises the arm under test.
        recognizer: any SpeechRecognizer = StubSpeechRecognizer(),
        loadSpeechModel: Bool = false
    ) async -> AnalysisWorkScheduler {
        // `serializesRecognizerRequests` deviates from the production default
        // only for the tests that actually transcribe, so they do not contend
        // on the process-wide recognizer gate with a concurrently running
        // suite. Callers that never reach stage 3 keep the old construction.
        let speechService = loadSpeechModel
            ? SpeechService(recognizer: recognizer, serializesRecognizerRequests: false)
            : SpeechService(recognizer: recognizer)
        if loadSpeechModel { try? await speechService.loadFastModel() }
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioProvider,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider(),
            thermalStateProvider: thermalStateProvider
        )
        let scheduler = AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: downloads,
            batteryProvider: {
                let b = StubBatteryProvider()
                b.level = 0.9
                b.charging = true
                return b
            }(),
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig()
        )
        // Install the production recorder. This is the single line
        // `PlayheadRuntime` adds; the scheduler's `setWorkJournalRecorder`
        // setter mutates an actor-isolated property, so we await it.
        await scheduler.setWorkJournalRecorder(
            AnalysisStoreWorkJournalRecorder(store: store)
        )
        return scheduler
    }

    /// Snapshots all `work_journal` rows for `(episodeId, generationID)`.
    /// We don't know the `generationID` upfront — the scheduler's
    /// `acquireLeaseWithJournal` mints a fresh UUID — so we recover it
    /// from the live `analysis_jobs` row first.
    private func fetchJournalRowsForEpisode(
        store: AnalysisStore,
        jobId: String,
        episodeId: String
    ) async throws -> [WorkJournalEntry] {
        guard let job = try await store.fetchJob(byId: jobId) else { return [] }
        return try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: job.generationID
        )
    }

    // MARK: - playhead-ngev (review r1): interrupted.requeue spends NO attempt

    /// Fails every shard to the loop's own conclusion, so the run is a genuine
    /// total failure rather than a displaced one. The control half of the pair
    /// below — same fixture, same stage, opposite termination.
    private final class ConcludingFailureRecognizer: SpeechRecognizer, @unchecked Sendable {
        private var loaded = false
        func loadModel() async throws { loaded = true }
        func unloadModel() async { loaded = false }
        func isModelLoaded() async -> Bool { loaded }
        func transcribe(
            shard: AnalysisShard, podcastId: String?
        ) async throws -> [TranscriptSegment] {
            guard loaded else { throw TranscriptEngineError.modelNotLoaded }
            throw TranscriptEngineError.vadFailed("shard \(shard.id)")
        }
        func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
            [VADResult(isSpeech: true, speechProbability: 1.0,
                       startTime: shard.startTime,
                       endTime: shard.startTime + shard.duration)]
        }
    }

    /// Drives one full dispatch pass to the transcript stage and reports the
    /// job row afterwards. Both halves of the A/B differ only in the
    /// recognizer, so any difference in `attemptCount` is attributable to the
    /// termination and nothing else.
    private func runOneTranscriptPass(
        jobId: String,
        episodeId: String,
        recognizer: any SpeechRecognizer
    ) async throws -> TranscriptPassResult {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let tmpDir = try makeTempDir(prefix: "ngev-\(jobId)")
        let audioFile = tmpDir.appendingPathComponent("episode.m4a")
        FileManager.default.createFile(atPath: audioFile.path, contents: Data())
        downloads.cachedURLs[episodeId] = audioFile

        try await store.insertJob(
            makeAnalysisJob(
                jobId: jobId,
                jobType: "preAnalysis",
                episodeId: episodeId,
                analysisAssetId: "asset-\(jobId)",
                workKey: "fp-\(jobId):1:preAnalysis",
                sourceFingerprint: "fp-\(jobId)",
                priority: 10,
                desiredCoverageSec: 90,
                state: "queued",
                attemptCount: 0
            )
        )
        // THE ASSET ROW MUST EXIST. `resolveAnalysisAssetId` returns
        // `job.analysisAssetId` VERBATIM when it is set and does not create the
        // row, so without this the pass dies at stage 2 on a FOREIGN KEY
        // constraint and never reaches transcription at all. That is not a
        // hypothetical: the first run of this fixture did exactly that, and
        // because a stage-2 failure also routes through the `.failed` arm, the
        // CONTROL half passed anyway — green for a reason that had nothing to
        // do with what it claims to test. The premise assertion below is what
        // stops that from ever being silent again.
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-\(jobId)",
                episodeId: episodeId,
                assetFingerprint: "fp-\(jobId)",
                weakFingerprint: nil,
                sourceURL: audioFile.absoluteString,
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "queued",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = (0..<4).map {
            makeShard(id: $0, episodeID: episodeId, startTime: Double($0) * 30, duration: 30)
        }
        let scheduler = await makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads,
            recognizer: recognizer,
            loadSpeechModel: true
        )
        // Bracket the pass. The arm computes `nextEligibleAt` from `clock()` at
        // some instant strictly inside this window, so a delay assertion can be
        // EXACT regardless of how starved the machine is — no wall-clock slack,
        // no load-dependent flake.
        let before = Date().timeIntervalSince1970
        _ = await scheduler.processNextDispatchableJobForTesting()
        let after = Date().timeIntervalSince1970
        let row = try await store.fetchJob(byId: jobId)

        // THE PREMISE, ASSERTED RATHER THAN ASSUMED. Both outcome arms write
        // the runner's reason to `lastErrorCode`, so a `transcription:` prefix
        // is proof the pass actually reached stage 3. Anything else — a decode
        // throw, a `features:` FK failure — means the fixture never exercised
        // the arm under test, and both halves of this A/B would otherwise
        // report green while measuring nothing.
        let code = row?.lastErrorCode ?? "<nil>"
        #expect(
            code.hasPrefix("transcription:"),
            """
            the pass never reached the transcript stage (lastErrorCode=\(code)), so this \
            fixture is measuring a different failure than the one it names
            """
        )
        return TranscriptPassResult(job: row, clockBefore: before, clockAfter: after)
    }

    /// The job row plus the bracket around the pass, so a caller can assert on
    /// `nextEligibleAt` exactly rather than with wall-clock slack.
    private struct TranscriptPassResult {
        let job: AnalysisJob?
        let clockBefore: Double
        let clockAfter: Double
    }

    /// A LISTENER MOVING THE PLAYHEAD MUST NOT COST A PERMANENT RETRY.
    ///
    /// The `.failed` arm charges one of five attempts and then supersedes the
    /// job with `nextEligibleAt: nil`. That row cannot come back:
    /// `analysis_jobs.workKey` is UNIQUE, `insertJob` is `INSERT OR IGNORE`,
    /// and the key is `"<fingerprint>:<analysisVersion>:preAnalysis"` — stable
    /// across launches — so every later enqueue for the episode is silently
    /// dropped until an app update bumps `analysisVersion`. The only reset,
    /// `requeueOrphanedLease`, rewrites `state = 'running'` rows and never
    /// touches a superseded one.
    ///
    /// Five scrubs across an episode's analysis lifetime is ordinary listening,
    /// so charging them would permanently kill analysis on exactly the episodes
    /// a listener engages with most.
    @Test("an interrupted transcript pass is requeued without spending an attempt")
    func interruptedPassSpendsNoAttempt() async throws {
        let result = try await runOneTranscriptPass(
            jobId: "ngev-interrupted",
            episodeId: "ep-ngev-interrupted",
            recognizer: CancellingRecognizer()
        )
        let row = try #require(result.job, "the job row must survive the pass")
        #expect(
            row.attemptCount == 0,
            """
            a scrub spent \(row.attemptCount) of five PERMANENT retry attempts. At five \
            the job is superseded with nextEligibleAt: nil and every later enqueue for \
            the episode is silently dropped — analysis is dead for that episode
            """
        )
        #expect(row.state == "queued", "an interrupted job must be retryable, got \(row.state)")

        // THE REQUEUE FLOOR, WHICH IS THE HALF THAT COSTS BATTERY IF IT GOES.
        //
        // Spending no attempt is only half the accounting. `.preempted` and
        // `.cancelledByPlayback` requeue with `nextEligibleAt: nil` — safe for
        // them, because a higher-lane job then holds the slot. An interruption
        // is reported WHILE PLAYBACK CONTINUES, so an immediately re-admitted
        // job collides with the same live engine owner again at once and spins.
        //
        // That regression has no red-test shape of its own: it surfaces as
        // battery drain a listener reports vaguely, months later. It is exactly
        // what a future "simplify the requeue to match `.preempted`" would do.
        // So the floor is pinned here.
        //
        // Bracketed rather than slack-based: the arm reads `clock()` at an
        // instant strictly between `clockBefore` and `clockAfter`, so this is
        // exact under any load. The upper bound also proves the delay is the
        // FLAT first rung and not the failure ladder's escalating value.
        let eligible = try #require(
            row.nextEligibleAt,
            """
            an interrupted job was requeued immediately eligible. It will collide with \
            the live engine owner that just displaced it and requeue again — a hot loop \
            burning battery mid-episode
            """
        )
        #expect(
            eligible >= result.clockBefore + 60,
            "requeued only \(eligible - result.clockAfter)s out, below the 60s floor"
        )
        #expect(
            eligible <= result.clockAfter + 60,
            """
            requeued \(eligible - result.clockBefore)s out — the floor is meant to be the \
            ladder's FLAT first rung, not an escalating backoff. An interruption spends \
            no attempt, so there is nothing to escalate against
            """
        )
    }

    /// THE CONTROL, AND IT IS WHAT KEEPS THE BUDGET MEANINGFUL. Same fixture,
    /// same stage — only the termination differs. A genuinely broken episode
    /// must still exhaust its attempts and stop, or it retries forever and the
    /// poisoned-slot starvation playhead-gyvb.1 fixed comes back.
    @Test("control: a transcript pass that failed on its own still spends an attempt")
    func concludedFailurePassSpendsAnAttempt() async throws {
        let result = try await runOneTranscriptPass(
            jobId: "ngev-concluded",
            episodeId: "ep-ngev-concluded",
            recognizer: ConcludingFailureRecognizer()
        )
        let row = try #require(result.job, "the job row must survive the pass")
        #expect(
            row.attemptCount == 1,
            """
            a genuine transcript failure spent \(row.attemptCount) attempts, not 1 — a \
            permanently broken episode now retries forever and pins its slot
            """
        )
    }

    // MARK: - cancelCatch.taskExpiredRequeue → preempted with .taskExpired

    @Test("cancel-mid-decode (attempts < max) emits a `.preempted` journal row tagged with the cancel cause")
    func cancelMidDecodeEmitsPreemptedWithTaskExpired() async throws {
        // Drives the BG-task expiration shape (`cancelCurrentJob(.taskExpired)`).
        // This is the production path that fires when the OS reclaims a BG
        // processing window before the decoder finishes.
        //
        // playhead-lmrx moved which ARM serves it — `.taskExpired` now takes
        // `cancelCatch.taskExpiredRequeue` (state 'queued', a FLAT requeue
        // floor, and no attempt spent) rather than `cancelCatch.requeue` (state
        // 'queued' with exponential backoff and an attempt spent). What this
        // test pins is unchanged and is what it was written for: the cause on
        // the emitted `.preempted` row is the cancel's own, never the helper's
        // `.pipelineError` default. The attempt accounting is covered next
        // door, in `ExpiredWindowAttemptAccountingTests`.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-task-expired"] = URL(fileURLWithPath: "/tmp/ep-task-expired.mp3")

        let job = makeAnalysisJob(
            jobId: "task-expired",
            jobType: "preAnalysis",
            episodeId: "ep-task-expired",
            analysisAssetId: "asset-task-expired",
            workKey: "fp-task-expired:1:preAnalysis",
            sourceFingerprint: "fp-task-expired",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 0
        )
        try await store.insertJob(job)

        let audioStub = CancellableAudioStub()
        let scheduler = await makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        let processed = await scheduler.processNextDispatchableJobForTesting(
            cancelAfterRunnerStart: .taskExpired
        )

        // Wait for the journal row carrying `.taskExpired`. We pin the
        // cause string here because the `cancelCatch.requeue` arm
        // threads `pendingCancelCause` through `emitJournalPreempted`,
        // and a regression that drops or default-substitutes the cause
        // is the most plausible silent break.
        let landed = await pollUntil {
            let rows = (try? await fetchJournalRowsForEpisode(
                store: store, jobId: "task-expired", episodeId: "ep-task-expired"
            )) ?? []
            return rows.contains { $0.eventType == .preempted && $0.cause == .taskExpired }
        }

        #expect(processed, "Scheduler test hook should process task-expired")
        #expect(landed, "Expected a `.preempted` row tagged `.taskExpired` after cancelCurrentJob(.taskExpired)")

        // Pin the full row shape: `acquired` from
        // `acquireLeaseWithJournal` plus the new `.preempted` from
        // the cancel path. A regression that drops the journal
        // append (e.g. recorder swallowed an error and silently
        // returned) would surface as the `acquired` row alone, so
        // we also assert the count delta.
        let rows = try await fetchJournalRowsForEpisode(
            store: store, jobId: "task-expired", episodeId: "ep-task-expired"
        )
        #expect(rows.contains { $0.eventType == .acquired },
                "Expected an acquired row from acquireLeaseWithJournal")
        let preempted = rows.filter { $0.eventType == .preempted }
        #expect(!preempted.isEmpty, "Expected at least one preempted row")
        // Every preempted row from this run must carry the cancel's
        // cause, never the helper's default `.pipelineError`.
        #expect(preempted.allSatisfy { $0.cause == .taskExpired },
                "All preempted rows must carry cause=.taskExpired (got \(preempted.map { $0.cause?.rawValue ?? "nil" }))")
    }

    // MARK: - cancelCatch.revertQueued → preempted with the cancel's own cause

    @Test("a non-expiry mid-decode cancel emits `.preempted` with ITS cause, not the default")
    func cancelMidDecodeEmitsPreemptedWithUserCancelled() async throws {
        // playhead-lmrx review round: COVERAGE THE EXEMPTION WOULD OTHERWISE
        // HAVE TAKEN. The sibling test above was the only witness that
        // `cancelCatch.requeue` threads `pendingCancelCause` into
        // `emitJournalPreempted` rather than falling back on the helper's
        // `?? .pipelineError`. `.taskExpired` now takes a different arm, so that
        // test no longer reaches `cancelCatch.revertQueued` at all — and a
        // regression that hardcoded `.pipelineError` there would have reddened
        // nothing. `.userCancelled` is the other production cause that reaches
        // it (the explicit-cancel entry point), and it still does.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-user-cancelled"] = URL(fileURLWithPath: "/tmp/ep-user-cancelled.mp3")

        let job = makeAnalysisJob(
            jobId: "user-cancelled",
            jobType: "preAnalysis",
            episodeId: "ep-user-cancelled",
            analysisAssetId: "asset-user-cancelled",
            workKey: "fp-user-cancelled:1:preAnalysis",
            sourceFingerprint: "fp-user-cancelled",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 0
        )
        try await store.insertJob(job)

        let audioStub = CancellableAudioStub()
        let scheduler = await makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        let processed = await scheduler.processNextDispatchableJobForTesting(
            cancelAfterRunnerStart: .userCancelled
        )
        #expect(processed, "Scheduler test hook should process user-cancelled")

        let landed = await pollUntil {
            let rows = (try? await fetchJournalRowsForEpisode(
                store: store, jobId: "user-cancelled", episodeId: "ep-user-cancelled"
            )) ?? []
            return rows.contains { $0.eventType == .preempted && $0.cause == .userCancelled }
        }
        #expect(landed, "Expected a `.preempted` row tagged `.userCancelled` from cancelCatch.revertQueued")

        let rows = try await fetchJournalRowsForEpisode(
            store: store, jobId: "user-cancelled", episodeId: "ep-user-cancelled"
        )
        let preemptedRows = rows.filter { $0.eventType == .preempted }
        #expect(!preemptedRows.isEmpty, "Expected at least one preempted row")
        #expect(preemptedRows.allSatisfy { $0.cause == .userCancelled },
                "All preempted rows must carry cause=.userCancelled (got \(preemptedRows.map { $0.cause?.rawValue ?? "nil" }))")

        // And the arm is the attempt-spending one: a user cancel is not an
        // OS-reclaimed window, so it still charges the retry ladder.
        let after = try #require(try await store.fetchJob(byId: "user-cancelled"))
        #expect(after.state == "queued")
        #expect(after.attemptCount == 1,
                "a non-expiry cancel still spends an attempt — that is what the exemption is carved OUT of")
    }

    // MARK: - cancelCatch.supersede → failed with .pipelineError

    @Test("cancel-mid-decode at maxAttempts emits a `.failed` journal row tagged `.pipelineError`")
    func cancelLoopAtMaxAttemptsEmitsFailed() async throws {
        // Pre-stamp `attemptCount: 4` so a single cancel cycle drives
        // attempts to 5 and trips `cancelCatch.supersede`. That arm is
        // the terminal "poisoned cancel loop" path: the slot will not
        // be retried, so the journal row must emit `.failed` (not
        // `.preempted`) to keep orphan recovery from misclassifying a
        // dead job as recoverable. Cause is `.pipelineError` because
        // supersede after a poisoned cancel loop is a pipeline-class
        // failure per the audit's Gap-1 recommendation.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-cancel-supersede"] = URL(fileURLWithPath: "/tmp/ep-cancel-supersede.mp3")

        let job = makeAnalysisJob(
            jobId: "cancel-supersede",
            jobType: "preAnalysis",
            episodeId: "ep-cancel-supersede",
            analysisAssetId: "asset-cancel-supersede",
            workKey: "fp-cancel-supersede:1:preAnalysis",
            sourceFingerprint: "fp-cancel-supersede",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 4 // one more cancel cycle trips supersede
        )
        try await store.insertJob(job)

        let audioStub = CancellableAudioStub()
        let scheduler = await makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        // playhead-lmrx: this fixture used to pass `.taskExpired`, which was
        // incidental — the test is about the POISONED-JOB escape valve, not
        // about OS expiry. `.taskExpired` no longer reaches this arm: an
        // OS-reclaimed background window spends no attempt (it is evidence
        // about the window, not the job), so it would never supersede and
        // never escalate backoff. `.pipelineError` is the arm's own
        // documented default and the honest cause for a poisoned decode, so
        // every assertion below is unchanged in meaning.
        let processed = await scheduler.processNextDispatchableJobForTesting(
            cancelAfterRunnerStart: .pipelineError
        )

        // Two assertions: the analysis_jobs row must terminate at
        // `superseded` (proving we hit the `cancelCatch.supersede`
        // arm, not `cancelCatch.requeue`), AND the journal row must
        // emit `.failed/.pipelineError`.
        let superseded = await pollUntil {
            let j = try? await store.fetchJob(byId: "cancel-supersede")
            return j?.state == "superseded"
        }
        #expect(superseded, "Expected job to reach state=superseded via cancelCatch.supersede")

        let landed = await pollUntil {
            let rows = (try? await fetchJournalRowsForEpisode(
                store: store, jobId: "cancel-supersede", episodeId: "ep-cancel-supersede"
            )) ?? []
            return rows.contains { $0.eventType == .failed && $0.cause == .pipelineError }
        }

        #expect(processed, "Scheduler test hook should process cancel-supersede")
        #expect(landed, "Expected a `.failed` row tagged `.pipelineError` after cancelCatch.supersede")

        // Also pin: no `.finalized` row should leak in — the
        // poisoned-cancel-loop terminal is `.failed`, not success.
        let rows = try await fetchJournalRowsForEpisode(
            store: store, jobId: "cancel-supersede", episodeId: "ep-cancel-supersede"
        )
        #expect(!rows.contains { $0.eventType == .finalized },
                "cancelCatch.supersede must not emit `.finalized` (got rows: \(rows.map { ($0.eventType, $0.cause?.rawValue ?? "nil") }))")
    }

    // MARK: - failed.supersede → failed with .pipelineError

    @Test("runner failure at maxAttempts emits a `.failed` journal row tagged `.pipelineError`")
    func runnerFailureAtMaxAttemptsEmitsFailed() async throws {
        // Pre-stamp `attemptCount: 4` so a single decode failure
        // drives attempts to 5 and trips `failed.supersede` (the
        // runner-driven terminal failure arm). This is the most
        // common terminal failure shape in production — the WIP
        // pre-review-cycle-1 missed it entirely. Without this test,
        // a regression that drops the `failed.supersede` emit would
        // re-open the same forensic gap the fix is supposed to close.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-runner-failed-supersede"] = URL(fileURLWithPath: "/tmp/ep-runner-failed-supersede.mp3")

        let job = makeAnalysisJob(
            jobId: "runner-failed-supersede",
            jobType: "preAnalysis",
            episodeId: "ep-runner-failed-supersede",
            analysisAssetId: "asset-runner-failed-supersede",
            workKey: "fp-runner-failed-supersede:1:preAnalysis",
            sourceFingerprint: "fp-runner-failed-supersede",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 4 // one more failure trips supersede
        )
        try await store.insertJob(job)

        let audioStub = FailingDecodeStub()
        let scheduler = await makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        let processed = await scheduler.processNextDispatchableJobForTesting()

        // Job must reach `superseded` via `failed.supersede` AND a
        // `.failed/.pipelineError` row must land in the journal.
        #expect(processed, "Scheduler test hook should process runner-failed-supersede")
        let superseded = (try await store.fetchJob(byId: "runner-failed-supersede"))?.state == "superseded"
        #expect(superseded, "Expected job to reach state=superseded via failed.supersede")

        let landed = try await fetchJournalRowsForEpisode(
            store: store,
            jobId: "runner-failed-supersede",
            episodeId: "ep-runner-failed-supersede"
        ).contains { $0.eventType == .failed && $0.cause == .pipelineError }

        #expect(landed, "Expected a `.failed` row tagged `.pipelineError` after failed.supersede")

        // No `.finalized` row should leak in — runner failure is
        // not a successful completion.
        let rows = try await fetchJournalRowsForEpisode(
            store: store, jobId: "runner-failed-supersede", episodeId: "ep-runner-failed-supersede"
        )
        #expect(!rows.contains { $0.eventType == .finalized },
                "failed.supersede must not emit `.finalized` (got rows: \(rows.map { ($0.eventType, $0.cause?.rawValue ?? "nil") }))")
    }

    // MARK: - failed.requeue → preempted with .pipelineError

    @Test("runner failure under maxAttempts emits a `.preempted` journal row tagged `.pipelineError`")
    func runnerFailureUnderMaxAttemptsEmitsPreempted() async throws {
        // Use `attemptCount: 0` so the first failure trips the
        // `failed.requeue` arm (transient retry). The journal row
        // must be `.preempted` (recoverable pause) — emitting
        // `.failed` here would mislead orphan recovery into
        // treating the slot as terminal even though it will retry.
        // The job's analysis_jobs row goes to `state="failed"` with
        // a backoff `nextEligibleAt`.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-runner-failed-requeue"] = URL(fileURLWithPath: "/tmp/ep-runner-failed-requeue.mp3")

        let job = makeAnalysisJob(
            jobId: "runner-failed-requeue",
            jobType: "preAnalysis",
            episodeId: "ep-runner-failed-requeue",
            analysisAssetId: "asset-runner-failed-requeue",
            workKey: "fp-runner-failed-requeue:1:preAnalysis",
            sourceFingerprint: "fp-runner-failed-requeue",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 0
        )
        try await store.insertJob(job)

        let audioStub = FailingDecodeStub()
        let scheduler = await makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        let processed = await scheduler.processNextDispatchableJobForTesting()

        // The arm sets `state="failed"` with a backoff. We poll for
        // either `failed` or for the journal row directly — whichever
        // comes first reliably indicates `failed.requeue` fired.
        #expect(processed, "Scheduler test hook should process runner-failed-requeue")
        let landed = try await fetchJournalRowsForEpisode(
            store: store,
            jobId: "runner-failed-requeue",
            episodeId: "ep-runner-failed-requeue"
        ).contains { $0.eventType == .preempted && $0.cause == .pipelineError }

        #expect(landed, "Expected a `.preempted` row tagged `.pipelineError` after failed.requeue")

        // Sanity: no `.failed` (terminal) row should land — the
        // slot is recoverable, only the supersede arm emits `.failed`.
        let rows = try await fetchJournalRowsForEpisode(
            store: store, jobId: "runner-failed-requeue", episodeId: "ep-runner-failed-requeue"
        )
        let terminalFailed = rows.filter { $0.eventType == .failed }
        #expect(terminalFailed.isEmpty,
                "failed.requeue must emit `.preempted`, not `.failed` (got terminal: \(terminalFailed.map { ($0.eventType, $0.cause?.rawValue ?? "nil") }))")
    }

    // MARK: - pausedForThermal → preempted with .thermal
    //
    // skeptical-review-cycle-5 M-Y2: pin the M4 emission for the
    // `pausedForThermal/memoryPressure` arm. Pre-M4 these arms updated
    // `analysis_jobs.state` to `paused` but the journal carried no row,
    // so a post-mortem could not distinguish a thermal pause from a
    // crashed/reaped slot.
    //
    // The companion `blockedByModel` arm in
    // `AnalysisWorkScheduler.processJob` is also new in M4, but the
    // `AnalysisJobRunner` does not currently produce
    // `.blockedByModel` from any code path — the case exists in
    // `AnalysisOutcome.StopReason` and the scheduler arm handles it,
    // but no producer wires it through the runner today (a future
    // model-availability gate is expected to). Driving it from a
    // stub-runner would require extracting `AnalysisJobRunner` to a
    // protocol so the scheduler accepts a stub instance — that swap
    // exceeds the M-Y2 finding's scope. The scheduler-side emission
    // logic is identical to the thermal arm covered here (same
    // `SliceCompletionInstrumentation.recordPaused` + `emitJournalPreempted`
    // shape, only the cause differs), so this single test guards the
    // shared codepath against regression.

    @Test("pausedForThermal outcome emits a `.preempted` journal row tagged `.thermal`")
    func pausedForThermalEmitsPreemptedWithThermalCause() async throws {
        // Drive the `pausedForThermal/memoryPressure` arm: stage 1
        // (decode) returns one shard, then the runner's post-decode
        // `checkStopConditions()` checkpoint observes
        // `thermalState == .critical` and returns `.pausedForThermal`.
        // The scheduler must record a `.preempted` journal row with
        // cause `.thermal` (not `.pipelineError`, which is the
        // memoryPressure variant of the same arm).
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-thermal-paused"] = URL(fileURLWithPath: "/tmp/ep-thermal-paused.mp3")

        let job = makeAnalysisJob(
            jobId: "thermal-paused",
            jobType: "preAnalysis",
            episodeId: "ep-thermal-paused",
            analysisAssetId: "asset-thermal-paused",
            workKey: "fp-thermal-paused:1:preAnalysis",
            sourceFingerprint: "fp-thermal-paused",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 0
        )
        try await store.insertJob(job)

        let audioStub = SingleShardAudioStub()
        let scheduler = await makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads,
            thermalStateProvider: { .critical }
        )
        let processed = await scheduler.processNextDispatchableJobForTesting()

        // The job must reach `state="paused"` (the
        // `pausedThermalOrMemory` arm's stateUpdate) AND the journal
        // must carry a `.preempted/.thermal` row.
        #expect(processed, "Scheduler test hook should process thermal-paused")
        let paused = (try await store.fetchJob(byId: "thermal-paused"))?.state == "paused"
        #expect(paused, "Expected job to reach state=paused via pausedThermalOrMemory arm")

        let landed = try await fetchJournalRowsForEpisode(
            store: store,
            jobId: "thermal-paused",
            episodeId: "ep-thermal-paused"
        ).contains { $0.eventType == .preempted && $0.cause == .thermal }

        #expect(landed, "Expected a `.preempted` row tagged `.thermal` after pausedForThermal outcome")

        // Pin the row shape: `acquired` from
        // `acquireLeaseWithJournal` plus the new `.preempted` from
        // the thermal arm. No `.failed` (the slot is recoverable) and
        // no `.finalized` (this is not a successful completion).
        let rows = try await fetchJournalRowsForEpisode(
            store: store, jobId: "thermal-paused", episodeId: "ep-thermal-paused"
        )
        #expect(rows.contains { $0.eventType == .acquired },
                "Expected an acquired row from acquireLeaseWithJournal")
        let preempted = rows.filter { $0.eventType == .preempted }
        #expect(!preempted.isEmpty, "Expected at least one preempted row")
        #expect(preempted.allSatisfy { $0.cause == .thermal },
                "All preempted rows must carry cause=.thermal (got \(preempted.map { $0.cause?.rawValue ?? "nil" }))")
        #expect(!rows.contains { $0.eventType == .failed },
                "pausedForThermal must not emit `.failed` (slot is recoverable)")
        #expect(!rows.contains { $0.eventType == .finalized },
                "pausedForThermal must not emit `.finalized` (not a successful completion)")
    }

    // MARK: - Recorder is wired (regression guard for PlayheadRuntime gap)

    @Test("a scheduler without setWorkJournalRecorder(...) writes zero terminal rows — proves the recorder is load-bearing")
    func defaultNoopRecorderProducesZeroTerminalRows() async throws {
        // Inverse-control test. Prove the production gap by
        // constructing a scheduler that uses the default
        // `NoopWorkJournalRecorder` (no `setWorkJournalRecorder` call)
        // and assert that NO terminal rows land in `work_journal`
        // even after a cancel. This is the pre-fix shape — it
        // pins the load-bearing role of the recorder injection in
        // `PlayheadRuntime`. If a future refactor changes the default
        // from Noop to a real recorder (or wires the recorder
        // somewhere else), this test must be updated alongside —
        // making the contract change visible.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-noop"] = URL(fileURLWithPath: "/tmp/ep-noop.mp3")

        let job = makeAnalysisJob(
            jobId: "noop-job",
            jobType: "preAnalysis",
            episodeId: "ep-noop",
            analysisAssetId: "asset-noop",
            workKey: "fp-noop:1:preAnalysis",
            sourceFingerprint: "fp-noop",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 0
        )
        try await store.insertJob(job)

        let audioStub = CancellableAudioStub()
        // Build a scheduler WITHOUT installing the recorder.
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let scheduler = AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: downloads,
            batteryProvider: {
                let b = StubBatteryProvider()
                b.level = 0.9
                b.charging = true
                return b
            }(),
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig()
        )
        // NOTE: deliberately NOT calling `setWorkJournalRecorder(...)`.

        let processed = await scheduler.processNextDispatchableJobForTesting(
            cancelAfterRunnerStart: .taskExpired
        )

        // Wait for the analysis_jobs row to settle into a non-running
        // state, which proves the cancel-cleanup arm fired. With the
        // default Noop recorder, the journal recording step inside
        // that arm is a no-op.
        let cleared = await pollUntil {
            let j = try? await store.fetchJob(byId: "noop-job")
            switch j?.state {
            case "running", nil: return false
            default: return true
            }
        }
        #expect(processed, "Scheduler test hook should process noop-job")
        #expect(cleared, "Cancel-cleanup arm never fired")
        #expect(
            await !scheduler.hasCurrentRunningTaskForTesting(),
            "Cancel-cleanup arm must clear the current running task handle"
        )
        #expect(
            await scheduler.pendingCancelCauseForTesting() == nil,
            "Cancel-cleanup arm must not leak its cause into later dispatches"
        )

        // The `acquired` row IS still written: it is appended
        // atomically by `acquireLeaseWithJournal` inside the store
        // itself (independent of the recorder). The bug being pinned
        // is that NO terminal rows (`.preempted`, `.failed`,
        // `.finalized`) land. Filter `acquired` out and assert the
        // rest is empty.
        let rows = try await fetchJournalRowsForEpisode(
            store: store, jobId: "noop-job", episodeId: "ep-noop"
        )
        let terminal = rows.filter {
            $0.eventType == .preempted
                || $0.eventType == .failed
                || $0.eventType == .finalized
        }
        #expect(terminal.isEmpty,
                "Default NoopWorkJournalRecorder must drop all terminal rows; got \(terminal.map { ($0.eventType, $0.cause?.rawValue ?? "nil") })")
    }

    // MARK: - lostOwnership skip on new M4 pause-arm emissions (C5 #48)

    /// Counting recorder stub: captures every call so the test can
    /// assert the per-emit `lostOwnership` gate inside
    /// `emitJournalPreempted` skipped the recorder invocation. Used by
    /// `lostOwnershipSkipsNewPauseArmEmissions`. Uses an actor for
    /// Swift 6 concurrency safety — NSLock is not available from
    /// async contexts under strict checking.
    private actor RecordingWorkJournalRecorder: WorkJournalRecording {
        struct PreemptedCall: Sendable {
            let episodeId: String
            let cause: InternalMissCause
        }
        private var _preemptedCalls: [PreemptedCall] = []
        func preemptedCalls() -> [PreemptedCall] { _preemptedCalls }
        func reset() { _preemptedCalls.removeAll() }
        // skeptical-review-cycle-8 L2: actor-isolated witness is fine
        // for `async` protocol methods — `nonisolated` here was
        // unnecessary and inconsistent with the `recordPreempted`
        // witness below. Drop it so the pattern reads consistently.
        func recordFinalized(episodeId: String) async {}
        func recordFailed(episodeId: String, cause: InternalMissCause) async {}
        func recordFailed(
            episodeId: String,
            cause: InternalMissCause,
            metadataJSON: String
        ) async {}
        func recordPreempted(
            episodeId: String,
            cause: InternalMissCause,
            metadataJSON: String
        ) async {
            _preemptedCalls.append(PreemptedCall(episodeId: episodeId, cause: cause))
        }
    }

    /// skeptical-review-cycle-5 #48: pin the per-emit `lostOwnership`
    /// gate inside `emitJournalPreempted`. The new M4 pause-arm
    /// emissions (`.thermal`, `.modelTemporarilyUnavailable`,
    /// `.pipelineError` for memoryPressure) all route through that
    /// helper, which means they inherit the helper's
    /// `guard !lostOwnership else { return }` check as defense-in-depth
    /// on top of the umbrella `if lostOwnership { return }` at
    /// `processJob` line ~2893. This test exercises the per-emit gate
    /// in isolation: with `lostOwnership = true`, none of the M4 causes
    /// must reach the recorder. The baseline call (`lostOwnership =
    /// false`) proves the test entry point itself isn't a no-op — a
    /// regression that accidentally short-circuits the helper above
    /// the recorder call (e.g. a moved guard) would fail the baseline.
    @Test("emitJournalPreempted's lostOwnership gate skips the recorder for every new M4 pause-arm cause")
    func lostOwnershipSkipsNewPauseArmEmissions() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = await makeScheduler(
            store: store,
            audioProvider: SingleShardAudioStub(),
            downloads: downloads
        )
        let recorder = RecordingWorkJournalRecorder()
        await scheduler.setWorkJournalRecorder(recorder)

        // M4 pause-arm causes — every cause that the new
        // `.preempted/.<cause>` emissions attach. A regression that
        // adds a fourth M4 cause but forgets to wire it through
        // `emitJournalPreempted` would not be caught here, but every
        // cause currently flowing through M4 is exercised explicitly.
        let m4Causes: [InternalMissCause] = [
            .thermal,
            .modelTemporarilyUnavailable,
            .pipelineError, // memoryPressure shape per M4 design
        ]

        // Baseline: with lostOwnership=false, every cause reaches the
        // recorder. Without this assertion, a regression that broke
        // the helper above the recorder call (e.g. early return on
        // every path) would silently make the lostOwnership=true
        // assertion below trivially true.
        for cause in m4Causes {
            await scheduler.emitJournalPreemptedForTesting(
                episodeId: "ep-baseline-\(cause.rawValue)",
                cause: cause,
                metadataJSON: "{}",
                underLostOwnership: false
            )
        }
        let baselineCalls = await recorder.preemptedCalls()
        #expect(baselineCalls.count == m4Causes.count,
                "Baseline (lostOwnership=false) must reach the recorder for every M4 cause; got \(baselineCalls.map { $0.cause.rawValue })")
        let baselineCauses = Set(baselineCalls.map { $0.cause })
        #expect(baselineCauses == Set(m4Causes),
                "Baseline must hit every M4 cause exactly once; missing: \(Set(m4Causes).subtracting(baselineCauses))")

        await recorder.reset()

        // Contract under test: with lostOwnership=true, NONE of the
        // M4 causes reach the recorder — the per-emit gate skips them.
        for cause in m4Causes {
            await scheduler.emitJournalPreemptedForTesting(
                episodeId: "ep-skip-\(cause.rawValue)",
                cause: cause,
                metadataJSON: "{}",
                underLostOwnership: true
            )
        }
        let skipCalls = await recorder.preemptedCalls()
        #expect(skipCalls.isEmpty,
                "M4 pause-arm emissions must respect the per-emit lostOwnership gate; got leaked calls: \(skipCalls.map { ($0.episodeId, $0.cause.rawValue) })")
    }
}
