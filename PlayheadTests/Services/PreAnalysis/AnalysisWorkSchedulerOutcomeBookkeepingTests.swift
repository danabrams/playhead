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
            adDetection: StubAdDetectionProvider(),
            cueMaterializer: SkipCueMaterializer(store: store)
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
            config: PreAnalysisConfig()
        )
    }

    // MARK: - Cancellation-arm bookkeeping

    @Test("cancelCurrentJob mid-decode increments attemptCount")
    func cancelMidDecodeBumpsAttempt() async throws {
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
        await scheduler.startSchedulerLoop()

        // Wait until the loop has acquired the lease and entered the
        // runner's decode call. Polling on `decodeCallCount` is the
        // sharpest signal that we're past the `acquireLease` /
        // `resolveAnalysisAssetId` setup and inside the runTask.
        let entered = await pollUntil {
            audioStub.decodeCallCount >= 1
        }
        #expect(entered, "Decode never started — cancel would hit a different arm")

        // Issue the same cancel that BackgroundProcessingService's
        // expirationHandler issues when its task budget expires.
        await scheduler.cancelCurrentJob(cause: .taskExpired)

        // Wait for the cancel-cleanup arm to commit. The fix routes
        // through `commitOutcomeArm(... incrementAttempt: true ...)`,
        // so attemptCount climbs to 1 within the poll window. On main,
        // the cleanup writes `state='queued'` + `releaseLease()`
        // directly without an increment, so this poll times out.
        let bumped = await pollUntil {
            let j = try? await store.fetchJob(byId: "cancel-mid-decode")
            return (j?.attemptCount ?? 0) >= 1
        }

        await scheduler.stop()

        #expect(bumped,
                "attemptCount must increment after a cancel-mid-decode cleanup, otherwise the job loops forever")
        let after = try await store.fetchJob(byId: "cancel-mid-decode")
        #expect((after?.attemptCount ?? 0) >= 1,
                "attemptCount must remain >= 1 after the cancel cleanup arm fires")
        // Note: we deliberately do NOT assert `leaseOwner == nil` here.
        // The scheduler loop may immediately re-acquire the queued
        // job once the cancel-cleanup arm commits and bumps the
        // attempt count, taking out a fresh lease for the next
        // attempt. Leases are managed transactionally by
        // `commitProcessJobOutcomeArm` (which calls `releaseLease` as
        // its terminal write) and `acquireLeaseWithJournal` — the
        // bookkeeping invariant under test (attemptCount must climb)
        // is the load-bearing claim, not whatever transient state the
        // loop sits in moments after the cancel.
        //
        // The companion test
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
        await scheduler.startSchedulerLoop()

        let superseded = await pollUntil {
            let j = try? await store.fetchJob(byId: "poison-decode")
            return j?.state == "superseded"
        }
        await scheduler.stop()

        #expect(superseded, "Repeated decode failure must supersede after maxAttemptsReached")
        let after = try await store.fetchJob(byId: "poison-decode")
        #expect(after?.attemptCount == 5)
        #expect(after?.lastErrorCode?.contains("maxAttemptsReached") == true)
        #expect(after?.leaseOwner == nil)
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
        // running before yielding to queued work behind it.
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
        await scheduler.startSchedulerLoop()

        let entered = await pollUntil {
            audioStub.decodeCallCount >= 1
        }
        #expect(entered, "Decode never started — cancel would hit a different arm")

        await scheduler.cancelCurrentJob(cause: .taskExpired)

        let superseded = await pollUntil {
            let j = try? await store.fetchJob(byId: "cancel-loop")
            return j?.state == "superseded"
        }
        await scheduler.stop()

        #expect(superseded, "Repeated cancel-mid-decode must supersede after maxAttemptsReached")
        let after = try await store.fetchJob(byId: "cancel-loop")
        #expect(after?.attemptCount == 5)
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
        await scheduler.startSchedulerLoop()

        let cleanProgressed = await pollUntil {
            let j = try? await store.fetchJob(byId: "clean-behind")
            // Once the loop has admitted the clean job, its state
            // moves out of `queued` (running → terminal). Either way
            // proves the slot is no longer pinned by the poisoned
            // job.
            switch j?.state {
            case "queued", nil: return false
            default: return true
            }
        }
        await scheduler.stop()

        #expect(cleanProgressed, "Queued work behind a poisoned asset must eventually be admitted")
        let poisonedAfter = try await store.fetchJob(byId: "poison-blocks")
        #expect(poisonedAfter?.state == "superseded",
                "Poisoned job must terminate via maxAttemptsReached")
    }
}
