// AnalysisWorkSchedulerUserIntentTests.swift
// playhead-3xtw: the on-demand "Download & Analyze" control routes its
// analysis to the USER-INTENT (`.now`) lane so it preempts starving
// background work. These tests prove `markEpisodeUserIntent` +
// `enqueue(...)` and `enqueueUserIntentAnalysis(...)` produce a
// priority-20 (`.now`-lane) job, that the flag is per-episode and one-shot,
// and that the enqueue stays work-key idempotent.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — user-intent lane (playhead-3xtw)")
struct AnalysisWorkSchedulerUserIntentTests {

    /// Minimal scheduler over an in-memory store + stub dependencies —
    /// mirrors `AnalysisWorkSchedulerThreeLaneTests.makeScheduler`. Only
    /// `enqueue` / `markEpisodeUserIntent` are exercised here, none of
    /// which runs the job runner.
    private func makeScheduler(store: AnalysisStore) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig()
        )
    }

    private func queuedJob(
        for episodeId: String,
        in store: AnalysisStore
    ) async throws -> AnalysisJob? {
        let jobs = try await store.fetchJobsByState("queued")
        return jobs.first { $0.episodeId == episodeId }
    }

    // MARK: - mark + enqueue → .now lane

    // playhead-rh69: `StubDownloadProvider()` here has NO cached file, so the
    // enqueue's duration probe finds nothing and the full-coverage request
    // falls back to the feed's declaration. That is why the persisted target
    // equals the declared number below — it is the degrade path, not a
    // measurement. `FullCoverageTargetTests` covers the measured path.
    @Test("marked episode's next enqueue lands at priority 20 (.now lane); unmeasurable audio keeps the declared target")
    func testMarkThenEnqueueIsUserIntent() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.markEpisodeUserIntent(episodeId: "ep-user", feedDeclaredDurationSec: 3600)
        // Simulate the download-completion enqueue (auto flags:
        // isExplicitDownload=false, no explicit coverage).
        await scheduler.enqueue(
            episodeId: "ep-user",
            podcastId: "pod",
            downloadId: "ep-user",
            sourceFingerprint: "fp-user",
            isExplicitDownload: false
        )

        let job = try await queuedJob(for: "ep-user", in: store)
        #expect(job?.priority == 20)
        #expect(job?.schedulerLane == .now)
        #expect(job?.desiredCoverageSec == 3600)
    }

    // Same note as above: no cached file, so 1800 below is the DECLARED
    // fallback rather than a measured length (playhead-rh69).
    @Test("enqueueUserIntentAnalysis lands at priority 20 (.now lane)")
    func testEnqueueUserIntentAnalysis() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-direct",
            podcastId: "pod",
            sourceFingerprint: "fp-direct",
            feedDeclaredDurationSec: 1800,
            podcastTitle: "Pod",
            episodeTitle: "Ep"
        )

        let job = try await queuedJob(for: "ep-direct", in: store)
        #expect(job?.priority == 20)
        #expect(job?.schedulerLane == .now)
        #expect(job?.desiredCoverageSec == 1800)
    }

    // MARK: - Control: without the flag, existing behaviour is unchanged

    @Test("without a user-intent mark, enqueue keeps the legacy priority mapping")
    func testLegacyPrioritiesUnchanged() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueue(
            episodeId: "ep-auto",
            podcastId: nil,
            downloadId: "ep-auto",
            sourceFingerprint: "fp-auto",
            isExplicitDownload: false
        )
        await scheduler.enqueue(
            episodeId: "ep-explicit",
            podcastId: nil,
            downloadId: "ep-explicit",
            sourceFingerprint: "fp-explicit",
            isExplicitDownload: true
        )

        let auto = try await queuedJob(for: "ep-auto", in: store)
        let explicit = try await queuedJob(for: "ep-explicit", in: store)
        #expect(auto?.priority == 0)
        #expect(auto?.schedulerLane == .background)
        #expect(explicit?.priority == 10)
        #expect(explicit?.schedulerLane == .soon)
    }

    // MARK: - Flag is per-episode (does not leak to other episodes)

    @Test("user-intent flag applies only to the marked episode")
    func testFlagIsPerEpisode() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.markEpisodeUserIntent(episodeId: "ep-A", feedDeclaredDurationSec: nil)
        // A different episode enqueues at its normal (auto) priority.
        await scheduler.enqueue(
            episodeId: "ep-B",
            podcastId: nil,
            downloadId: "ep-B",
            sourceFingerprint: "fp-B",
            isExplicitDownload: false
        )
        // The marked one gets the user-intent lane.
        await scheduler.enqueue(
            episodeId: "ep-A",
            podcastId: nil,
            downloadId: "ep-A",
            sourceFingerprint: "fp-A",
            isExplicitDownload: false
        )

        let a = try await queuedJob(for: "ep-A", in: store)
        let b = try await queuedJob(for: "ep-B", in: store)
        #expect(a?.priority == 20)
        #expect(b?.priority == 0)
    }

    // MARK: - Idempotent (work-key dedup)

    @Test("re-enqueuing the same episode+fingerprint keeps a single job row")
    func testIdempotentEnqueue() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-dedup",
            podcastId: nil,
            sourceFingerprint: "fp-dedup",
            feedDeclaredDurationSec: 600,
            podcastTitle: nil,
            episodeTitle: nil
        )
        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-dedup",
            podcastId: nil,
            sourceFingerprint: "fp-dedup",
            feedDeclaredDurationSec: 600,
            podcastTitle: nil,
            episodeTitle: nil
        )

        let jobs = try await store.fetchJobsByState("queued")
        #expect(jobs.filter { $0.episodeId == "ep-dedup" }.count == 1)
    }

    // MARK: - playhead-kanf: promoting an ALREADY-queued job

    private func workKey(fingerprint: String) -> String {
        AnalysisJob.computeWorkKey(
            fingerprint: fingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: "preAnalysis"
        )
    }

    @Test("a tap on an ALREADY-queued episode promotes that job to the .now lane")
    func testTapPromotesAlreadyQueuedJob() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        // The auto-pipeline got there first: background lane, priority 0.
        await scheduler.enqueue(
            episodeId: "ep-starving",
            podcastId: "pod",
            downloadId: "ep-starving",
            sourceFingerprint: "fp-starving",
            isExplicitDownload: false
        )
        let before = try #require(try await queuedJob(for: "ep-starving", in: store))
        #expect(before.priority == 0)
        #expect(before.schedulerLane == .background)

        // The user taps "Download & Analyze". Pre-kanf this was a silent no-op:
        // `insertJob` is INSERT OR IGNORE on workKey, so the priority-20 struct
        // was discarded and the row kept priority 0.
        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-starving",
            podcastId: "pod",
            sourceFingerprint: "fp-starving",
            feedDeclaredDurationSec: 3600,
            podcastTitle: nil,
            episodeTitle: nil
        )

        let after = try #require(try await queuedJob(for: "ep-starving", in: store))
        #expect(after.priority == AnalysisWorkScheduler.nowLanePriorityFloor)
        #expect(after.schedulerLane == .now)
        // Promoted in place — the tap must not mint a second row.
        #expect(after.jobId == before.jobId)
        let rows = try await store.fetchJobsByState("queued")
        #expect(rows.filter { $0.episodeId == "ep-starving" }.count == 1)
    }

    @Test("promotion is a selection nudge: createdAt and the routing pair survive it")
    func testPromotionPreservesRoutingIdentity() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueue(
            episodeId: "ep-ident",
            podcastId: nil,
            downloadId: "ep-ident",
            sourceFingerprint: "fp-ident",
            isExplicitDownload: false
        )
        let before = try #require(try await queuedJob(for: "ep-ident", in: store))

        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-ident",
            podcastId: nil,
            sourceFingerprint: "fp-ident",
            feedDeclaredDurationSec: 3600,
            podcastTitle: nil,
            episodeTitle: nil
        )

        let after = try #require(try await queuedJob(for: "ep-ident", in: store))
        #expect(after.priority == AnalysisWorkScheduler.nowLanePriorityFloor)
        #expect(after.createdAt == before.createdAt, "FIFO tiebreak must not move")
        #expect(after.generationID == before.generationID)
        #expect(after.schedulerEpoch == before.schedulerEpoch)
        #expect(after.state == "queued")
        #expect(after.leaseOwner == nil)
    }

    // MARK: - The negative: a leased / running job is NEVER promoted

    @Test("a LEASED job is not promoted, while a queued episode tapped in the same breath is")
    func testLeasedJobIsNotPromoted() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        // The row under lease.
        await scheduler.enqueue(
            episodeId: "ep-leased",
            podcastId: nil,
            downloadId: "ep-leased",
            sourceFingerprint: "fp-leased",
            isExplicitDownload: false
        )
        let leasedJob = try #require(try await queuedJob(for: "ep-leased", in: store))
        let acquired = try await store.acquireLease(
            jobId: leasedJob.jobId,
            owner: "worker-live",
            expiresAt: Date().timeIntervalSince1970 + 600
        )
        #expect(acquired, "precondition: a worker actually holds this row")

        // The positive witness: an identically-configured queued episode. If
        // this one is NOT promoted the test is vacuous and says nothing about
        // the lease guard (playhead-le02).
        await scheduler.enqueue(
            episodeId: "ep-free",
            podcastId: nil,
            downloadId: "ep-free",
            sourceFingerprint: "fp-free",
            isExplicitDownload: false
        )

        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-leased",
            podcastId: nil,
            sourceFingerprint: "fp-leased",
            feedDeclaredDurationSec: 3600,
            podcastTitle: nil,
            episodeTitle: nil
        )
        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-free",
            podcastId: nil,
            sourceFingerprint: "fp-free",
            feedDeclaredDurationSec: 3600,
            podcastTitle: nil,
            episodeTitle: nil
        )

        let leasedAfter = try #require(
            try await store.fetchJob(byWorkKey: workKey(fingerprint: "fp-leased"))
        )
        #expect(leasedAfter.priority == 0, "a leased row keeps its background priority")
        #expect(leasedAfter.schedulerLane == .background)
        #expect(leasedAfter.state == "running")
        #expect(leasedAfter.leaseOwner == "worker-live")

        let freeAfter = try #require(try await queuedJob(for: "ep-free", in: store))
        #expect(
            freeAfter.priority == AnalysisWorkScheduler.nowLanePriorityFloor,
            "witness: the promotion path IS live in this configuration"
        )
    }

    @Test("a RUNNING job with no live lease is not promoted either")
    func testRunningJobIsNotPromoted() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueue(
            episodeId: "ep-running",
            podcastId: nil,
            downloadId: "ep-running",
            sourceFingerprint: "fp-running",
            isExplicitDownload: false
        )
        let job = try #require(try await queuedJob(for: "ep-running", in: store))
        // No lease owner: the state guard alone must refuse this.
        try await store.updateJobState(jobId: job.jobId, state: "running")

        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-running",
            podcastId: nil,
            sourceFingerprint: "fp-running",
            feedDeclaredDurationSec: 3600,
            podcastTitle: nil,
            episodeTitle: nil
        )

        let after = try #require(
            try await store.fetchJob(byWorkKey: workKey(fingerprint: "fp-running"))
        )
        #expect(after.priority == 0)
        #expect(after.state == "running")
    }

    // MARK: - The one-shot flag is never burned without effect

    @Test("a refused promotion RETAINS the flag, so the next enqueue still honours it")
    func testFlagSurvivesARefusedPromotion() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueue(
            episodeId: "ep-retry",
            podcastId: nil,
            downloadId: "ep-retry",
            sourceFingerprint: "fp-retry",
            isExplicitDownload: false
        )
        let job = try #require(try await queuedJob(for: "ep-retry", in: store))
        try await store.updateJobState(jobId: job.jobId, state: "running")

        // Tap 1: refused — the row is running.
        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-retry",
            podcastId: nil,
            sourceFingerprint: "fp-retry",
            feedDeclaredDurationSec: 3600,
            podcastTitle: nil,
            episodeTitle: nil
        )
        let midway = try #require(
            try await store.fetchJob(byWorkKey: workKey(fingerprint: "fp-retry"))
        )
        #expect(midway.priority == 0)

        // The job falls back to queued (a lease expiry / requeue). A PLAIN auto
        // enqueue now arrives — it carries no user intent of its own, so a
        // priority of 20 can only mean the flag from tap 1 was still standing.
        try await store.updateJobState(jobId: job.jobId, state: "queued")
        await scheduler.enqueue(
            episodeId: "ep-retry",
            podcastId: nil,
            downloadId: "ep-retry",
            sourceFingerprint: "fp-retry",
            isExplicitDownload: false
        )

        let after = try #require(try await queuedJob(for: "ep-retry", in: store))
        #expect(
            after.priority == AnalysisWorkScheduler.nowLanePriorityFloor,
            "the one-shot flag was not burned by the refused tap"
        )
    }

    @Test("a SERVED promotion consumes the flag — a later auto enqueue is not promoted")
    func testServedPromotionConsumesTheFlag() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueue(
            episodeId: "ep-consume",
            podcastId: nil,
            downloadId: "ep-consume",
            sourceFingerprint: "fp-consume",
            isExplicitDownload: false
        )
        // Tap: served (promotes the queued row), so the flag is spent.
        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-consume",
            podcastId: nil,
            sourceFingerprint: "fp-consume",
            feedDeclaredDurationSec: 3600,
            podcastTitle: nil,
            episodeTitle: nil
        )
        let promoted = try #require(try await queuedJob(for: "ep-consume", in: store))
        #expect(promoted.priority == AnalysisWorkScheduler.nowLanePriorityFloor)

        // A re-download changes the fingerprint, so this is a genuinely NEW
        // work key and a fresh insert. It must land at the auto priority.
        await scheduler.enqueue(
            episodeId: "ep-consume",
            podcastId: nil,
            downloadId: "ep-consume",
            sourceFingerprint: "fp-consume-v2",
            isExplicitDownload: false
        )

        let fresh = try #require(
            try await store.fetchJob(byWorkKey: workKey(fingerprint: "fp-consume-v2"))
        )
        #expect(fresh.priority == 0, "the flag was consumed by the promotion it served")
    }

    @Test("re-tapping an already-promoted job is idempotent and consumes the flag")
    func testRetapOfAnAlreadyPromotedJob() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        for _ in 0..<3 {
            await scheduler.enqueueUserIntentAnalysis(
                episodeId: "ep-retap",
                podcastId: nil,
                sourceFingerprint: "fp-retap",
                feedDeclaredDurationSec: 3600,
                podcastTitle: nil,
                episodeTitle: nil
            )
        }

        let rows = try await store.fetchJobsByState("queued")
            .filter { $0.episodeId == "ep-retap" }
        #expect(rows.count == 1)
        #expect(rows.first?.priority == AnalysisWorkScheduler.nowLanePriorityFloor)

        // Flag consumed by the `alreadyPromoted` outcome, not left standing:
        // a later auto enqueue at a new fingerprint is background work.
        await scheduler.enqueue(
            episodeId: "ep-retap",
            podcastId: nil,
            downloadId: "ep-retap",
            sourceFingerprint: "fp-retap-v2",
            isExplicitDownload: false
        )
        let fresh = try #require(
            try await store.fetchJob(byWorkKey: workKey(fingerprint: "fp-retap-v2"))
        )
        #expect(fresh.priority == 0)
    }

    // MARK: - Control: an enqueue with no user intent never promotes

    @Test("a plain auto enqueue over an existing queued row leaves its priority alone")
    func testAutoEnqueueDoesNotPromote() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueue(
            episodeId: "ep-plain",
            podcastId: nil,
            downloadId: "ep-plain",
            sourceFingerprint: "fp-plain",
            isExplicitDownload: false
        )
        // Same work key, and this time flagged as an explicit download — still
        // no user intent, so nothing may be promoted.
        await scheduler.enqueue(
            episodeId: "ep-plain",
            podcastId: nil,
            downloadId: "ep-plain",
            sourceFingerprint: "fp-plain",
            isExplicitDownload: true
        )

        let after = try #require(try await queuedJob(for: "ep-plain", in: store))
        #expect(after.priority == 0)
        #expect(after.schedulerLane == .background)
    }
}
