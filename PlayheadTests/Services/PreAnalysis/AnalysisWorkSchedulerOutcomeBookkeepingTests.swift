// AnalysisWorkSchedulerOutcomeBookkeepingTests.swift
// playhead-gyvb.1: pin the invariant that EVERY terminal exit from a
// `processJob` lease commits an outcome arm that bumps `attemptCount`
// (when work was performed) so a poisoned job cannot loop forever
// without ever reaching `maxAttemptsReached` and freeing the slot.
//
// The 2026-04-27 incident showed a job stuck at `state='running'`
// across 51 lease acquisitions with `attemptCount = 0`. Two arms in
// `AnalysisWorkScheduler.processJob` fail to commit an attempt
// increment on terminal exit:
//
//   1. The asset-resolution failure arm (`updateJobState(state:
//      "failed", ...)` directly, no `commitOutcomeArm`/
//      `incrementAttempt: true`).
//   2. The mid-run cancellation arm (`CancellationError` catch with
//      `lostOwnership == false`, e.g. `cancelCurrentJob(.taskExpired)`
//      from BackgroundProcessingService): reverts `state='queued'`
//      and releases the lease without bumping the attempt count.
//
// Without an attempt bump, `maxAttemptsReached` (threshold 5) never
// fires and the queued asset behind the poisoned one never advances.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — outcome-arm bookkeeping (playhead-gyvb.1)")
struct AnalysisWorkSchedulerOutcomeBookkeepingTests {

    // MARK: - Test fixture

    /// Audio provider stub that hangs in `decode(...)` until cancelled,
    /// then throws `CancellationError`. Lets a test cancel the running
    /// job mid-decode (the `cancelCurrentJob(.taskExpired)` shape) and
    /// observe the scheduler's CancellationError-cleanup arm.
    private final class CancellableAudioStub: AnalysisAudioProviding, @unchecked Sendable {
        /// Number of `decode` calls observed. Lets the test wait until
        /// the runner has actually entered decode before issuing the
        /// cancel — without this the cancel can land before
        /// `processJob` even acquires the lease, hitting the
        /// pre-runTask `shouldCancelCurrentJob` arm instead.
        private(set) var decodeCallCount = 0

        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            decodeCallCount += 1
            // Sleep until cancelled. Use a long upper-bound so we
            // surface a clear test-timeout if cancellation never fires
            // (rather than spuriously returning empty shards which
            // would route through the runner's `.failed` arm and mask
            // the bug under test).
            try await Task.sleep(for: .seconds(60))
            return []
        }
    }

    /// Audio provider stub whose `decode(...)` always returns `.failed`
    /// (via thrown `decodingFailed`) — emulates the production
    /// "Operation Interrupted" loop. Used to verify the `.failed`
    /// outcome arm does correctly drive `attemptCount` to
    /// `maxAttemptCount` and supersede the job (control test for the
    /// arm that already works).
    private final class FailingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        let message: String
        init(message: String = "Operation Interrupted") { self.message = message }

        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            throw AnalysisAudioError.decodingFailed(message)
        }
    }

    // skeptical-review-cycle-18 M-1: the formerly-private nested
    // StubTransportStatusProvider (cycle-16 #45 root-cause stub for
    // NWPathMonitor first-update flakiness) was promoted to
    // PlayheadTests/Helpers/Stubs.swift.

    private func makeScheduler(
        store: AnalysisStore,
        audioProvider: any AnalysisAudioProviding,
        downloads: StubDownloadProvider
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioProvider,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        return AnalysisWorkScheduler(
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
    }

    // MARK: - Cancellation-arm bookkeeping

    @Test("cancelCurrentJob mid-decode increments attemptCount")
    func cancelMidDecodeBumpsAttempt() async throws {
        // Pins the core bookkeeping invariant for the
        // `cancelCatch.revertQueued` arm (AnalysisWorkScheduler.swift
        // :3198-3199): a mid-decode cancel from attemptCount=0 must bump
        // attemptCount to 1 (0+1 = 1 < maxAttemptCount 5), otherwise a
        // poisoned job loops forever without ever reaching
        // `maxAttemptsReached` and freeing the slot (the 2026-04-27
        // incident).
        //
        // Determinism (playhead-xx7m.2): this bookkeeping invariant is a
        // pure-correctness regression guard, so it must run in the
        // routine fast/integration suites — not just the perf pass. The
        // former version induced the cancel via a race (start processing
        // in a `Task {}`, poll `decodeCallCount >= 1`, then call
        // `cancelCurrentJob` externally hoping it lands mid-decode).
        // Under the full parallel suite that timing was unreliable, so it
        // was gated behind `PerfGate`. We instead drive the SAME
        // `cancelCatch.revertQueued` arm through the deterministic
        // `cancelAfterRunnerStart` test hook, which
        // `processNextDispatchableJobForTesting` uses to cancel the run
        // task synchronously (before awaiting its value) — no
        // Task+poll+external-cancel race, no wall-clock budget. This
        // mirrors the siblings
        // `cancelMidDecodeRequeueAppliesExponentialBackoff` (same
        // revertQueued arm) and `cancelLoopSupersedesAfterMaxAttempts`.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-cancel"] = URL(fileURLWithPath: "/tmp/ep-cancel.mp3")

        let job = makeAnalysisJob(
            jobId: "cancel-mid-decode",
            jobType: "preAnalysis",
            episodeId: "ep-cancel",
            analysisAssetId: "asset-cancel",
            workKey: "fp-cancel:1:preAnalysis",
            sourceFingerprint: "fp-cancel",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 0
        )
        try await store.insertJob(job)

        let audioStub = CancellableAudioStub()
        let scheduler = makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        let processed = await scheduler.processNextDispatchableJobForTesting(
            cancelAfterRunnerStart: .taskExpired
        )

        #expect(processed, "Scheduler test hook should process cancel-mid-decode")
        let after = try await store.fetchJob(byId: "cancel-mid-decode")
        #expect(after?.attemptCount == 1,
                "attemptCount must increment to 1 after a cancel-mid-decode cleanup (cancelCatch.revertQueued), otherwise the job loops forever")
        // Note: we deliberately do NOT assert `leaseOwner == nil` here.
        // A revertQueued cancel leaves the job re-queued for its next
        // attempt (state='queued'), not terminal — the load-bearing
        // claim is that attemptCount climbs, not the transient lease
        // state. The companion test
        // `cancelLoopSupersedesAfterMaxAttempts` covers the terminal
        // shape (state=superseded, leaseOwner=nil) end-to-end.
    }

    // MARK: - Poisoned-decode escape valve (control + invariant)

    @Test("repeated decode failure (.failed outcome) reaches maxAttemptsReached and supersedes the job")
    func decodeFailureSupersedesAfterMaxAttempts() async throws {
        // Control test for the `.failed` arm that already works on
        // main: a decode that surfaces `Operation Interrupted` over
        // and over should drive `attemptCount` up to 5 and supersede
        // the job — freeing the lease slot for downstream work.
        //
        // Pre-stamp the job at attemptCount=4 so a single failure
        // cycle hits maxAttemptsReached without waiting through the
        // exponential backoff between attempts (60s → 120s → 240s
        // → ...). This keeps the test deterministic without a manual
        // clock seam.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-poison"] = URL(fileURLWithPath: "/tmp/ep-poison.mp3")

        let job = makeAnalysisJob(
            jobId: "poison-decode",
            jobType: "preAnalysis",
            episodeId: "ep-poison",
            analysisAssetId: "asset-poison",
            workKey: "fp-poison:1:preAnalysis",
            sourceFingerprint: "fp-poison",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 4 // one more failure must trigger supersede
        )
        try await store.insertJob(job)

        let audioStub = FailingDecodeStub()
        let scheduler = makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process poison-decode")
        let superseded = (try await store.fetchJob(byId: "poison-decode"))?.state == "superseded"
        #expect(superseded, "Repeated decode failure must supersede after maxAttemptsReached")
        let after = try await store.fetchJob(byId: "poison-decode")
        #expect(after?.attemptCount == 5)
        #expect(after?.lastErrorCode?.contains("maxAttemptsReached") == true)
        #expect(after?.leaseOwner == nil)
    }

    @Test("cancel-mid-decode requeue applies exponential backoff to nextEligibleAt")
    func cancelMidDecodeRequeueAppliesExponentialBackoff() async throws {
        // Review-followup (csp / H1): the `cancelCatch.revertQueued`
        // arm previously cleared `nextEligibleAt`, so a user
        // pause/play-loop on a poison-content episode could burn
        // through `maxAttemptCount` instantly. Mirror the
        // `.failed.requeue` arm: each cancel must push
        // `nextEligibleAt` forward by `min(2^attempts * 60, 3600)s`
        // so backoff actually paces the retries.
        //
        // Strategy: drive three separate jobs that begin at
        // attemptCount = 0, 1, 2. Cancel each mid-run and capture the
        // resulting `nextEligibleAt`. Backoff after one cancel must
        // equal min(2^(attemptCount+1) * 60, 3600), so the three
        // observed backoffs must double per step (120s → 240s → 480s).
        //
        // Determinism (playhead-xx7m.2): this backoff assertion is a
        // pure-correctness regression guard for H1/csp, so it must run
        // in the routine fast/integration suites — not just the perf
        // pass. The former version induced the cancel via a race
        // (start processing in a `Task {}`, poll `decodeCallCount >= 1`,
        // then call `cancelCurrentJob` externally hoping it lands
        // mid-decode). Under the full ~7,900-test parallel suite that
        // timing was unreliable and timed out (72–88s under
        // contention), so it was gated behind `PerfGate`. We instead
        // drive the SAME `cancelCatch.revertQueued` arm through the
        // deterministic `cancelAfterRunnerStart` test hook, which
        // `processNextDispatchableJobForTesting` uses to cancel the
        // run task synchronously (before awaiting its value) — no
        // Task+poll+external-cancel race, no wall-clock budget. This is
        // the same mechanism the non-gated sibling
        // `AnalysisWorkSchedulerJournalEmissionTests.cancelMidDecodeEmitsPreemptedWithTaskExpired`
        // relies on to reach this arm.
        //
        // We can't drive three cancels on the same job because the
        // first cancel installs a future `nextEligibleAt`, which the
        // dispatcher honors — the second dispatch never re-selects it.
        // Three independent jobs side-step that: each prior job is left
        // requeued with a future `nextEligibleAt`, so only the fresh
        // job (nextEligibleAt = nil) is eligible on the next pass.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()

        var observedBackoffs: [Double] = []

        for startingAttempts in [0, 1, 2] {
            let jobId = "cancel-backoff-\(startingAttempts)"
            let episodeId = "ep-cancel-backoff-\(startingAttempts)"
            downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(episodeId).mp3")

            let job = makeAnalysisJob(
                jobId: jobId,
                jobType: "preAnalysis",
                episodeId: episodeId,
                analysisAssetId: "asset-\(startingAttempts)",
                workKey: "fp-\(startingAttempts):1:preAnalysis",
                sourceFingerprint: "fp-\(startingAttempts)",
                priority: 10,
                desiredCoverageSec: 90,
                state: "queued",
                attemptCount: startingAttempts
            )
            try await store.insertJob(job)

            let audioStub = CancellableAudioStub()
            let scheduler = makeScheduler(
                store: store,
                audioProvider: audioStub,
                downloads: downloads
            )

            // Capture wall-clock immediately before the synchronous
            // dispatch so we can subtract the scheduler's `clock()`
            // reading (`Date()` by default) to recover the chosen
            // backoff value. The whole pass completes in well under a
            // second, so the residual measurement error is
            // milliseconds — far inside the 30s tolerance below.
            let beforeCancel = Date().timeIntervalSince1970
            let processed = await scheduler.processNextDispatchableJobForTesting(
                cancelAfterRunnerStart: .taskExpired
            )
            #expect(processed, "Scheduler test hook should process \(jobId)")

            let after = try await store.fetchJob(byId: jobId)
            #expect(after?.attemptCount == startingAttempts + 1,
                    "attempt \(startingAttempts + 1) must bump attemptCount via cancelCatch.revertQueued")
            let nextEligible = after?.nextEligibleAt ?? 0
            #expect(nextEligible > beforeCancel,
                    "attempt \(startingAttempts + 1) did not commit a future nextEligibleAt")
            observedBackoffs.append(nextEligible - beforeCancel)
        }

        // Exponential helper: min(2^attempt * 60, 3600). Attempts here
        // are 1, 2, 3 → 120s, 240s, 480s. Use a 30-second jitter
        // margin around each expected target to absorb test-host
        // scheduling jitter while still pinning the doubling.
        #expect(observedBackoffs.count == 3)
        let expected: [Double] = [120, 240, 480]
        for (i, target) in expected.enumerated() {
            #expect(abs(observedBackoffs[i] - target) < 30,
                    "attempt \(i + 1) backoff was \(observedBackoffs[i])s, expected ~\(target)s")
        }
        // Strictly increasing too — defends against a future regression
        // that happens to land each value inside the same 30s window.
        #expect(observedBackoffs[1] > observedBackoffs[0],
                "backoff must grow attempt-over-attempt; got \(observedBackoffs)")
        #expect(observedBackoffs[2] > observedBackoffs[1],
                "backoff must grow attempt-over-attempt; got \(observedBackoffs)")
    }

    @Test("repeated mid-decode cancellation reaches maxAttemptsReached and supersedes the job")
    func cancelLoopSupersedesAfterMaxAttempts() async throws {
        // The 2026-04-27 incident: a job is repeatedly cancelled
        // mid-decode (e.g. `cancelCurrentJob(.taskExpired)` from BG
        // task expirations) and the cleanup arm fails to bump
        // attemptCount, so `maxAttemptsReached` never fires.
        //
        // After the fix, even one cancel-mid-decode cycle from
        // attemptCount=4 must supersede the job. That's the
        // invariant that bounds how long a poisoned slot can stay
        // running before yielding to queued work behind it. This is
        // the ONLY test that exercises the `cancelCatch.supersede`
        // arm (AnalysisWorkScheduler.swift:3143) — distinct from the
        // `.failed.supersede` arm covered by
        // `decodeFailureSupersedesAfterMaxAttempts`.
        //
        // Determinism (playhead-xx7m.2): this supersede invariant is a
        // pure-correctness regression guard for the 2026-04-27
        // incident, so it must run in the routine fast/integration
        // suites — not just the perf pass. The former version induced
        // the cancel via a race (start processing in a `Task {}`, poll
        // `decodeCallCount >= 1`, then call `cancelCurrentJob`
        // externally hoping it lands mid-decode). Under the full
        // parallel suite that timing was unreliable, so it was gated
        // behind `PerfGate`. We instead drive the SAME
        // `cancelCatch.supersede` arm through the deterministic
        // `cancelAfterRunnerStart` test hook, which
        // `processNextDispatchableJobForTesting` uses to cancel the run
        // task synchronously (before awaiting its value) — no
        // Task+poll+external-cancel race, no wall-clock budget. This
        // mirrors the sibling
        // `cancelMidDecodeRequeueAppliesExponentialBackoff` (which
        // drives the `cancelCatch.revertQueued` arm the same way) and
        // `AnalysisWorkSchedulerJournalEmissionTests.cancelMidDecodeEmitsPreemptedWithTaskExpired`.
        //
        // Seed at attemptCount=4 so this single deterministic pass
        // makes attempts = job.attemptCount + 1 = 5 == maxAttemptCount,
        // taking the `attempts >= Self.maxAttemptCount` branch
        // (cancelCatch.supersede at line 3143) rather than
        // cancelCatch.revertQueued.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-cancel-loop"] = URL(fileURLWithPath: "/tmp/ep-cancel-loop.mp3")

        let job = makeAnalysisJob(
            jobId: "cancel-loop",
            jobType: "preAnalysis",
            episodeId: "ep-cancel-loop",
            analysisAssetId: "asset-cancel-loop",
            workKey: "fp-cancel-loop:1:preAnalysis",
            sourceFingerprint: "fp-cancel-loop",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 4 // one more cancellation must trigger supersede
        )
        try await store.insertJob(job)

        let audioStub = CancellableAudioStub()
        let scheduler = makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        let processed = await scheduler.processNextDispatchableJobForTesting(
            cancelAfterRunnerStart: .taskExpired
        )

        #expect(processed, "Scheduler test hook should process cancel-loop")
        let after = try await store.fetchJob(byId: "cancel-loop")
        #expect(after?.state == "superseded",
                "Repeated cancel-mid-decode must supersede after maxAttemptsReached")
        #expect(after?.attemptCount == 5)
        #expect(after?.lastErrorCode?.contains("maxAttemptsReached") == true)
        #expect(after?.leaseOwner == nil)
    }

    // MARK: - Queue-progress invariant

    @Test("a poisoned-decode asset does not block queued work behind it indefinitely")
    func poisonedAssetDoesNotBlockQueue() async throws {
        // End-to-end queue-progress test. Two jobs:
        //  - poisoned: attemptCount=4, decode always fails.
        //  - clean: priority=0, decode succeeds (empty shards →
        //    `.failed("no shards within desired coverage")`, but the
        //    arm still terminates the job through `.failed.requeue`/
        //    `.failed.supersede`, freeing the lease).
        //
        // The poisoned job must reach `superseded` and yield the
        // running slot. Without the fix, the scheduler held the slot
        // forever. With it, the queued asset advances within the test
        // window.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-poison-block"] = URL(fileURLWithPath: "/tmp/ep-poison-block.mp3")
        downloads.cachedURLs["ep-clean"] = URL(fileURLWithPath: "/tmp/ep-clean.mp3")

        let poisoned = makeAnalysisJob(
            jobId: "poison-blocks",
            jobType: "preAnalysis",
            episodeId: "ep-poison-block",
            analysisAssetId: "asset-poison-block",
            workKey: "fp-poison-block:1:preAnalysis",
            sourceFingerprint: "fp-poison-block",
            priority: 10, // higher priority — runs first
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 4
        )
        let clean = makeAnalysisJob(
            jobId: "clean-behind",
            jobType: "preAnalysis",
            episodeId: "ep-clean",
            analysisAssetId: "asset-clean",
            workKey: "fp-clean:1:preAnalysis",
            sourceFingerprint: "fp-clean",
            priority: 0, // lower priority — only runs after poisoned exits
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(poisoned)
        try await store.insertJob(clean)

        let audioStub = FailingDecodeStub()
        let scheduler = makeScheduler(
            store: store,
            audioProvider: audioStub,
            downloads: downloads
        )
        let poisonedProcessed = await scheduler.processNextDispatchableJobForTesting()
        let cleanProcessed = await scheduler.processNextDispatchableJobForTesting()

        #expect(poisonedProcessed, "Scheduler test hook should process poisoned job")
        #expect(cleanProcessed, "Scheduler test hook should process queued clean job")
        let cleanAfter = try await store.fetchJob(byId: "clean-behind")
        let cleanProgressed = cleanAfter?.state != "queued" && cleanAfter != nil
        #expect(cleanProgressed, "Queued work behind a poisoned asset must eventually be admitted")
        let poisonedAfter = try await store.fetchJob(byId: "poison-blocks")
        #expect(poisonedAfter?.state == "superseded",
                "Poisoned job must terminate via maxAttemptsReached")
    }
}
