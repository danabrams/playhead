// CapOutRetryTests.swift
// playhead-y8f3: reaching `AnalysisWorkScheduler.maxAttemptCount` must not be
// PERMANENT — and the way out must stay BOUNDED.
//
// The trap, measured on the 2026-07-31 device pull. A job that exhausts its five
// attempts is written `state = 'superseded'` with `nextEligibleAt = nil`.
// `analysis_jobs.workKey` is `TEXT NOT NULL UNIQUE`, `insertJob` is
// `INSERT OR IGNORE`, and `AnalysisJob.computeWorkKey` is stable across
// launches, so from that moment every enqueue for the episode is a silent no-op.
// The only attempt-reset in the codebase, `AnalysisStore.requeueOrphanedLease`,
// writes `state = CASE WHEN state = 'running' THEN 'queued' ELSE state END` — it
// preserves a superseded row deliberately, because `superseded` is ALSO how a
// genuine supersession is recorded and those must stay retired.
//
// The pull held 8 such rows against a queue of one dispatchable job, 6 of them on
// episodes still under 95% transcribed:
//
//   lastErrorCode                                   n
//   maxAttemptsReached:transcription:zeroCoverage   6
//   maxAttemptsReached:decode: …Operation Interrupted 1
//   maxAttemptsReached:cancelMidRun                 1
//
// Two of those six had ALREADY been transcribed past their own target
// (`D2B8579A`: 2,670 s covered against a 2,649 s target) and superseded anyway,
// because a pass that reads nothing new reports `transcription:zeroCoverage`.
// That is why the retry's target comes off the tier LADDER rather than off the
// terminated job — a fixture shape carried deliberately below.
//
// A test that only asserted a row changed state would pass on an implementation
// that shuffles rows without reading any more audio, so the reach test asserts on
// `analysis_assets.fastTranscriptCoverageEndTime` — seconds of audio actually
// transcribed — and the termination test asserts the chain STOPS, by a named
// cause, under a fixture that can never progress.

import Foundation
import OSLog
import Testing
@testable import Playhead

@Suite("Cap-out retry — the attempt cap is not permanent, and is bounded (playhead-y8f3)")
struct CapOutRetryTests {

    // MARK: - Fixture constants

    private static let episodeId = "ep-y8f3"
    private static let assetId = "asset-y8f3"
    private static let fingerprint = "fp-y8f3"
    private static let shardSeconds: Double = 30
    private static let shardCount = 40
    /// 1,200 s of DECODED audio — past `t2DepthSeconds` (900 s) so the duration
    /// rung is distinguishable from every configured tier.
    private static let shardSpanSec = Double(shardCount) * shardSeconds
    /// The asset's `episodeDurationSec`. Deliberately longer than the decoded
    /// audio, which is the real shape: the column is written from the AVURLAsset
    /// CONTAINER duration while the watermark advances on DECODED shard ends.
    private static let durationSec = shardSpanSec + 12.5

    private static var baseWorkKey: String {
        AnalysisJob.computeWorkKey(
            fingerprint: fingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: "preAnalysis"
        )
    }

    private static func retryKey(_ ordinal: Int) -> String {
        AnalysisWorkScheduler.capOutRetryWorkKey(baseWorkKey: baseWorkKey, ordinal: ordinal)
    }

    private static let defaultTiers: [Double] = {
        let config = PreAnalysisConfig()
        return [config.defaultT0DepthSeconds, config.t1DepthSeconds, config.t2DepthSeconds]
    }()

    // MARK: - Harness

    /// Deterministic clock. Both the reconciler (cooldown) and the scheduler
    /// (backoff) read it, so one instance drives the whole loop and the
    /// hour-long cooldown is stepped over rather than slept through.
    private final class RetryClock: @unchecked Sendable {
        private let lock: OSAllocatedUnfairLock<Date>
        init(start: Date) { lock = OSAllocatedUnfairLock(initialState: start) }
        var value: Date { lock.withLock { $0 } }
        func advance(by seconds: TimeInterval) {
            lock.withLock { $0 = $0.addingTimeInterval(seconds) }
        }
    }

    /// A recognizer that returns one segment per shard, so the transcript engine
    /// persists chunks and `fastTranscriptCoverageEndTime` genuinely advances.
    /// `StubSpeechRecognizer` returns an EMPTY transcript, which the runner
    /// classifies as `transcription:zeroCoverage` — the exact failure that
    /// produced six of the eight device rows, and useless for proving coverage
    /// rises.
    private final class ShardTranscribingRecognizer: SpeechRecognizer, @unchecked Sendable {
        private let lock = OSAllocatedUnfairLock(initialState: false)

        func loadModel() async throws { lock.withLock { $0 = true } }
        func unloadModel() async { lock.withLock { $0 = false } }
        func isModelLoaded() async -> Bool { lock.withLock { $0 } }

        func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
            guard lock.withLock({ $0 }) else { throw TranscriptEngineError.modelNotLoaded }
            let text = "shard-\(Int(shard.startTime))"
            return [
                TranscriptSegment(
                    id: shard.id,
                    words: [
                        TranscriptWord(
                            text: text,
                            startTime: shard.startTime,
                            endTime: shard.startTime + shard.duration,
                            confidence: 0.9
                        ),
                    ],
                    text: text,
                    startTime: shard.startTime,
                    endTime: shard.startTime + shard.duration,
                    avgConfidence: 0.9,
                    passType: .final_,
                    speakerId: nil
                ),
            ]
        }

        func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
            [VADResult(
                isSpeech: true,
                speechProbability: 1.0,
                startTime: shard.startTime,
                endTime: shard.startTime + shard.duration
            )]
        }
    }

    /// Always-throwing decode — the poisoned asset that must never win back an
    /// unbounded amount of budget.
    private final class FailingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            throw AnalysisAudioError.decodingFailed("Operation Interrupted")
        }
    }

    private func makeDownloads() -> StubDownloadProvider {
        let downloads = StubDownloadProvider()
        downloads.cachedURLs[Self.episodeId] = URL(fileURLWithPath: "/tmp/y8f3.m4a")
        downloads.fingerprints[Self.episodeId] = AudioFingerprint(
            weak: Self.fingerprint,
            strong: Self.fingerprint
        )
        return downloads
    }

    private func makeReconciler(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        clock: RetryClock
    ) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: StubCapabilitiesProvider(),
            config: PreAnalysisConfig(),
            clock: { clock.value }
        )
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        audio: any AnalysisAudioProviding,
        clock: RetryClock
    ) async throws -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: ShardTranscribingRecognizer())
        try await speechService.loadFastModel()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio,
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
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig(),
            clock: { clock.value }
        )
    }

    private func decodingAudio() -> StubAnalysisAudioProvider {
        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = (0..<Self.shardCount).map {
            makeShard(
                id: $0,
                episodeID: Self.episodeId,
                startTime: Double($0) * Self.shardSeconds,
                duration: Self.shardSeconds
            )
        }
        return audio
    }

    /// Seeds the device's shape: an asset with a partial transcript and a single
    /// `analysis_jobs` row that reached the attempt cap.
    ///
    /// `priorTranscriptCoverageSec` is written as a watermark AND as a chunk,
    /// because the runner reconciles the watermark up from persisted chunks at
    /// job start — a watermark with nothing behind it would be reconciled away.
    private func seedCappedOutEpisode(
        _ store: AnalysisStore,
        priorTranscriptCoverageSec: Double,
        supersededAt: Double,
        desiredCoverageSec: Double = 90,
        lastErrorCode: String = "maxAttemptsReached:transcription:zeroCoverage",
        state: String = "superseded"
    ) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: Self.fingerprint,
                weakFingerprint: nil,
                sourceURL: "file:///tmp/y8f3.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: priorTranscriptCoverageSec > 0
                    ? priorTranscriptCoverageSec
                    : nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.queued.rawValue,
                analysisVersion: PreAnalysisConfig.analysisVersion,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.durationSec
            )
        )
        if priorTranscriptCoverageSec > 0 {
            _ = try await store.insertTranscriptChunks([
                TranscriptChunk(
                    id: "\(Self.assetId)-prior",
                    analysisAssetId: Self.assetId,
                    segmentFingerprint: "\(Self.assetId)-prior-seg",
                    chunkIndex: 0,
                    startTime: 0,
                    endTime: priorTranscriptCoverageSec,
                    text: "prior transcript",
                    normalizedText: "prior transcript",
                    pass: "fast",
                    modelVersion: "test-asr",
                    transcriptVersion: "tx-v1",
                    atomOrdinal: 0
                ),
            ])
        }
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-y8f3-base",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            analysisAssetId: Self.assetId,
            workKey: Self.baseWorkKey,
            sourceFingerprint: Self.fingerprint,
            desiredCoverageSec: desiredCoverageSec,
            state: state,
            attemptCount: 5,
            lastErrorCode: lastErrorCode,
            createdAt: supersededAt,
            updatedAt: supersededAt
        ))
    }

    private func allJobs(_ store: AnalysisStore) async throws -> [AnalysisJob] {
        var jobs: [AnalysisJob] = []
        for state in ["queued", "paused", "running", "backfill", "complete", "failed", "superseded"] {
            jobs += try await store.fetchJobsByState(state)
        }
        return jobs
    }

    private func transcriptCoverage(_ store: AnalysisStore) async throws -> Double {
        try await store.fetchAsset(id: Self.assetId)?.fastTranscriptCoverageEndTime ?? 0
    }

    // MARK: - The reach claim: seconds of audio, not a row state

    /// THE acceptance test, at the device's real shape. An asset whose only job
    /// is an attempt-cap terminal and whose transcript is well under 95% must
    /// become dispatchable again AND must end up with MORE AUDIO TRANSCRIBED.
    ///
    /// The `#expect(after > before)` on `fastTranscriptCoverageEndTime` is the
    /// load-bearing one. An implementation that minted a row and never read any
    /// audio — the wrong target, a row the scheduler declines to dispatch, a
    /// pass that re-reads only what is already covered — passes every row-state
    /// assertion here and fails this one.
    @Test("a capped-out asset under 95% transcript becomes dispatchable and transcribes more audio")
    func cappedOutAssetTranscribesMoreAudio() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        // 300 s of 1,212.5 s == 25% transcribed, and the terminal is a day old.
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: 300,
            supersededAt: clock.value.timeIntervalSince1970 - 86_400
        )

        let before = try await transcriptCoverage(store)
        #expect(before == 300)
        // Nothing is dispatchable: the one row is superseded with no
        // `nextEligibleAt`. This is the state the product owner's device was in.
        let scheduler = try await makeScheduler(
            store: store, downloads: downloads, audio: decodingAudio(), clock: clock
        )
        #expect(await scheduler.processNextDispatchableJobForTesting() == false,
                "precondition: a superseded row must not be dispatchable")

        let reconciler = makeReconciler(store: store, downloads: downloads, clock: clock)
        let report = try await reconciler.reconcile()

        #expect(report.capOutRetriesMinted == 1)
        #expect(report.unEnqueuedDownloadsCreated == 0,
                "the base key was swallowed; the retry is minted at a fresh ordinal, not at the base")
        #expect(report.reEnqueuesSwallowed == 1, "the swallow itself must stay visible")
        #expect(report.recoveredWorkCount >= 1,
                "a queued dispatchable row that did not exist before IS recovered work")

        // The swallowed terminal is untouched. This bead is prevention only —
        // no historical row is re-opened, reset, or migrated.
        let terminal = try await store.fetchJob(byId: "job-y8f3-base")
        #expect(terminal?.state == "superseded")
        #expect(terminal?.attemptCount == 5)
        #expect(terminal?.workKey == Self.baseWorkKey)
        #expect(terminal?.lastErrorCode == "maxAttemptsReached:transcription:zeroCoverage")

        let retry = try await store.fetchJob(byWorkKey: Self.retryKey(1))
        #expect(retry?.state == "queued")
        #expect(retry?.attemptCount == 0)
        #expect(retry?.analysisAssetId == Self.assetId)
        // The target is the next LADDER rung above the watermark, so the pass has
        // audio it has not read. Re-minting the terminated job's own 90 s target
        // against a 300 s watermark would read nothing and fail identically.
        #expect(retry?.desiredCoverageSec == 900,
                "expected the next rung above 300 s; got \(retry?.desiredCoverageSec ?? -1)")

        // Drive it. The claim is seconds of audio.
        var dispatches = 0
        for _ in 0..<8 {
            if await scheduler.processNextDispatchableJobForTesting() { dispatches += 1 }
            clock.advance(by: 7_200)
        }
        #expect(dispatches >= 1)

        let after = try await transcriptCoverage(store)
        #expect(after > before,
                "the retry must READ AUDIO, not merely move a row: \(before) -> \(after)")
        #expect(after >= 900,
                "the retry asked for the 900 s rung and the audio is present; got \(after)")
    }

    // MARK: - Termination

    /// Many cycles against an asset that can NEVER progress must stop, and stop
    /// for a reason we can name. The fixture's decode always throws, so every
    /// cycle burns its five attempts and supersedes again — precisely the
    /// poisoned asset `maxAttemptCount` exists to stop.
    ///
    /// The bound asserted is the strong one: the TOTAL number of
    /// `analysis_jobs` rows this episode can ever hold, driven well past the
    /// budget. A leak shows up as a fourth row.
    @Test("a poisoned asset spends a bounded budget and then stops, by a named cause")
    func poisonedAssetTerminatesWithNamedCause() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: 0,
            supersededAt: clock.value.timeIntervalSince1970 - 86_400,
            lastErrorCode: "maxAttemptsReached:decode: Decoding failed: Operation Interrupted"
        )
        let reconciler = makeReconciler(store: store, downloads: downloads, clock: clock)
        let scheduler = try await makeScheduler(
            store: store, downloads: downloads, audio: FailingDecodeStub(), clock: clock
        )

        var totalMinted = 0
        // Ten cycles: five times the budget. Each cycle reconciles, then drives
        // the scheduler far enough for any minted row to burn all five attempts
        // and supersede again.
        for _ in 0..<10 {
            totalMinted += try await reconciler.reconcile().capOutRetriesMinted
            for _ in 0..<8 {
                _ = await scheduler.processNextDispatchableJobForTesting()
                clock.advance(by: 7_200)
            }
        }

        #expect(totalMinted == AnalysisWorkScheduler.maxCapOutRetries,
                "the chain must spend exactly its budget and no more; minted \(totalMinted)")

        let jobs = try await allJobs(store)
        #expect(jobs.count == AnalysisWorkScheduler.maxCapOutRetries + 1,
                "base + \(AnalysisWorkScheduler.maxCapOutRetries) retries and nothing else; got \(jobs.count)")
        #expect(Set(jobs.map(\.workKey)) == [Self.baseWorkKey, Self.retryKey(1), Self.retryKey(2)])
        #expect(jobs.allSatisfy { $0.state == "superseded" },
                "every rung must reach a terminal; a leftover queued row means the walk did not stop")

        // No audio was ever read, which is what makes this a poisoned asset and
        // not merely a slow one.
        #expect(try await transcriptCoverage(store) == 0)

        // And the NAMED cause. The chain does not stop by accident or by running
        // out of loop iterations — the decision function refuses, and says why.
        let tail = try #require(try await store.fetchJob(byWorkKey: Self.retryKey(2)))
        let decision = AnalysisWorkScheduler.capOutRetryDecision(
            baseWorkKey: Self.baseWorkKey,
            chainTail: tail,
            nextOrdinal: nil,
            transcriptCoverageSec: 0,
            episodeDurationSec: Self.durationSec,
            tiers: Self.defaultTiers,
            now: clock.value.timeIntervalSince1970
        )
        #expect(decision == .declined(.budgetSpent))
    }

    /// A retry that SUCCEEDS must not charge the budget. This is the
    /// consecutive-vs-lifetime distinction playhead-bkhc and playhead-8d5r were
    /// filed on: a lifetime counter kills a job that had a few unlucky windows
    /// early and then started converging.
    ///
    /// Here the ordinal only ever advances on a cap-out, because a cycle that
    /// reaches its tier terminates `complete` and leaves an ACTIVE successor —
    /// which excludes the episode from step 7 entirely.
    @Test("a retry that makes progress does not spend the retry budget")
    func progressDoesNotSpendBudget() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: 0,
            supersededAt: clock.value.timeIntervalSince1970 - 86_400
        )
        let reconciler = makeReconciler(store: store, downloads: downloads, clock: clock)
        let scheduler = try await makeScheduler(
            store: store, downloads: downloads, audio: decodingAudio(), clock: clock
        )

        #expect(try await reconciler.reconcile().capOutRetriesMinted == 1)

        // Drive the whole ladder to quiescence on healthy audio.
        for _ in 0..<12 {
            _ = await scheduler.processNextDispatchableJobForTesting()
            clock.advance(by: 7_200)
        }

        // Further sweeps mint nothing more: the ladder walked to completion, so
        // the episode's chain tail is `complete`, not a cap-out terminal.
        for _ in 0..<3 {
            #expect(try await reconciler.reconcile().capOutRetriesMinted == 0)
        }
        #expect(try await store.fetchJob(byWorkKey: Self.retryKey(2)) == nil,
                "ordinal 2 must be untouched — success does not charge the ledger")
        #expect(try await transcriptCoverage(store) == Self.shardSpanSec)
    }

    // MARK: - The other meaning of `superseded`

    /// `superseded` means two things. A GENUINE supersession — a stale
    /// `analysisVersion`, a deleted episode, cached audio that no longer matches
    /// the fingerprint — retires a row whose replacement either already exists
    /// or must never exist. Re-requesting those is how a reset path turns into a
    /// duplicate-work bug, which is why `requeueOrphanedLease` preserves the
    /// state rather than resetting it.
    ///
    /// `lastErrorCode` is the discriminator, and this pins it.
    @Test("a genuine supersession is never re-requested — only an attempt-cap terminal is")
    func genuineSupersessionIsNotReRequested() async throws {
        for cause in [nil, "staleFingerprint:cachedAudioMismatch"] {
            let store = try await makeTestStore()
            let downloads = makeDownloads()
            let clock = RetryClock(start: Date(timeIntervalSince1970: 1_700_000_000))
            try await seedCappedOutEpisode(
                store,
                priorTranscriptCoverageSec: 0,
                supersededAt: clock.value.timeIntervalSince1970 - 86_400,
                lastErrorCode: cause ?? ""
            )
            // `makeAnalysisJob` cannot express a NULL lastErrorCode through the
            // loop above, so clear it explicitly for the nil case.
            if cause == nil {
                try await store.updateJobState(jobId: "job-y8f3-base", state: "superseded")
            }

            let report = try await makeReconciler(
                store: store, downloads: downloads, clock: clock
            ).reconcile()

            #expect(report.capOutRetriesMinted == 0,
                    "cause \(cause ?? "nil") is a genuine supersession, not a cap-out")
            #expect(report.reEnqueuesSwallowed == 1,
                    "it is still a swallow, and must still be visible")
            #expect(try await store.fetchJob(byWorkKey: Self.retryKey(1)) == nil)
        }
    }

    /// playhead-gqx4's degraded terminal writes `maxAttemptsReached:coverageInsufficient`
    /// but terminates `state = 'complete'`, not `superseded`. It is a different
    /// bead's terminal with a different remedy, and the prefix alone would
    /// capture it. The STATE check is what keeps this bead in its lane.
    @Test("the coverage-insufficient degraded terminal is not this bead's to re-request")
    func degradedCompleteTerminalIsNotReRequested() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: 90,
            supersededAt: clock.value.timeIntervalSince1970 - 86_400,
            lastErrorCode: "maxAttemptsReached:coverageInsufficient",
            state: "complete"
        )

        let report = try await makeReconciler(
            store: store, downloads: downloads, clock: clock
        ).reconcile()

        #expect(report.capOutRetriesMinted == 0)
        #expect(report.reEnqueuesSwallowed == 1)
    }

    // MARK: - Cooling

    /// A fresh cap-out is not re-requested immediately. The terminal had already
    /// earned an hour between its last two attempts
    /// (`exponentialBackoffSeconds`'s ceiling); re-requesting sooner would pace
    /// the retry faster than the attempts inside the cycle it just failed.
    @Test("a cap-out inside the cooldown is not re-requested, and is after it")
    func coolingWindowIsHonoured() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: 0,
            supersededAt: clock.value.timeIntervalSince1970 - 600 // 10 minutes
        )
        let reconciler = makeReconciler(store: store, downloads: downloads, clock: clock)

        #expect(try await reconciler.reconcile().capOutRetriesMinted == 0)
        #expect(try await store.fetchJob(byWorkKey: Self.retryKey(1)) == nil)

        // Past the cooldown, the same episode mints. Without this half the test
        // would also pass on an implementation that never mints at all.
        clock.advance(by: AnalysisWorkScheduler.capOutRetryCooldownSeconds)
        #expect(try await reconciler.reconcile().capOutRetriesMinted == 1)
        #expect(try await store.fetchJob(byWorkKey: Self.retryKey(1)) != nil)
    }

    /// Repeated sweeps between cap-outs must not each mint a row. The retry is
    /// `queued`, so the episode is ACTIVE and step 7 skips it entirely — and
    /// even if it did not, the ordinal collides on the UNIQUE index.
    @Test("repeated sweeps mint at most one retry per cap-out")
    func repeatedSweepsAreIdempotent() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: 0,
            supersededAt: clock.value.timeIntervalSince1970 - 86_400
        )
        let reconciler = makeReconciler(store: store, downloads: downloads, clock: clock)

        var minted = 0
        for _ in 0..<5 {
            minted += try await reconciler.reconcile().capOutRetriesMinted
            clock.advance(by: 7_200)
        }
        #expect(minted == 1)
        #expect(try await allJobs(store).count == 2)
    }

    // MARK: - Pure decision matrix

    @Test("the cap-out retry ordinal round-trips and rejects every other key shape")
    func ordinalParsing() {
        let base = Self.baseWorkKey
        #expect(AnalysisWorkScheduler.capOutRetryOrdinal(workKey: Self.retryKey(1)) == 1)
        #expect(AnalysisWorkScheduler.capOutRetryOrdinal(workKey: Self.retryKey(2)) == 2)
        #expect(AnalysisWorkScheduler.capOutRetryOrdinal(workKey: base) == nil)
        #expect(AnalysisWorkScheduler.capOutRetryOrdinal(workKey: "\(base):900") == nil)
        #expect(AnalysisWorkScheduler.capOutRetryOrdinal(workKey: "\(base):capRetry:0") == nil)
        #expect(AnalysisWorkScheduler.capOutRetryOrdinal(workKey: "\(base):capRetry:x") == nil)
        #expect(AnalysisWorkScheduler.capOutRetryOrdinal(workKey: "\(base):capRetry") == nil)
    }

    /// The two budgets are independent ledgers. If either parser matched the
    /// other's marker, a chain would launder the other's ordinal away — exactly
    /// the collapse `CoverageTierLadderSchedulerTests` pins for onn6.
    @Test("the cap-out and ad-scan-redrive ordinals do not read each other's keys")
    func ordinalLedgersAreIndependent() {
        let base = Self.baseWorkKey
        let redrive = "\(base):\(AnalysisWorkScheduler.adScanRedriveWorkKeyMarker):1"
        let capRetry = Self.retryKey(1)
        #expect(AnalysisWorkScheduler.capOutRetryOrdinal(workKey: redrive) == nil)
        #expect(AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: capRetry) == nil)
    }

    @Test("only a superseded row with a maxAttemptsReached cause is an attempt-cap terminal")
    func attemptCapTerminalDiscrimination() {
        func job(state: String, cause: String?) -> AnalysisJob {
            makeAnalysisJob(state: state, lastErrorCode: cause)
        }
        #expect(AnalysisWorkScheduler.isAttemptCapTerminal(
            job(state: "superseded", cause: "maxAttemptsReached:transcription:zeroCoverage")))
        #expect(AnalysisWorkScheduler.isAttemptCapTerminal(
            job(state: "superseded", cause: "maxAttemptsReached:cancelMidRun")))
        // Genuine supersessions.
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(
            job(state: "superseded", cause: nil)))
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(
            job(state: "superseded", cause: "staleFingerprint:cachedAudioMismatch")))
        // gqx4's degraded terminal carries the prefix but is not superseded.
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(
            job(state: "complete", cause: "maxAttemptsReached:coverageInsufficient")))
        // A live row is never a terminal, whatever it last complained about.
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(
            job(state: "queued", cause: "maxAttemptsReached:cancelMidRun")))
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(
            job(state: "failed", cause: "transcription:zeroCoverage")))
    }

    /// The outstanding-work predicate, at the shapes taken off the device pull.
    /// The two `nil` cases are the ones that stop this bead from manufacturing
    /// work that cannot achieve anything.
    @Test("the retry target is the next ladder rung above the watermark, or nothing")
    func outstandingTranscriptTargetShapes() {
        func target(covered: Double, duration: Double?) -> Double? {
            AnalysisWorkScheduler.outstandingTranscriptTarget(
                transcriptCoverageSec: covered,
                tiers: Self.defaultTiers,
                episodeDurationSec: duration
            )
        }
        // Nothing read yet -> the cheapest rung.
        #expect(target(covered: 0, duration: 1826) == 90)
        // `9AA1FEA2`: 1,259 s of 1,826 s. Past T2, so the only rung left is the
        // episode itself.
        #expect(target(covered: 1259, duration: 1826) == 1826)
        // `D2B8579A`: 2,670 s of 2,936 s — already deeper than the 2,649 s target
        // its terminated job carried, which is why re-using that target would
        // read nothing.
        #expect(target(covered: 2670, duration: 2936) == 2936)
        // `7A481794`: 3,210 s of 3,213 s. Read end to end; the 3 s shortfall is
        // the container/decoder clock disagreement, not outstanding work.
        #expect(target(covered: 3210, duration: 3213) == nil)
        // Past the top of the ladder entirely.
        #expect(target(covered: 5000, duration: 3213) == nil)
        // No duration known (the job never resolved an asset) -> configured
        // tiers only, cheapest rung first.
        #expect(target(covered: 0, duration: nil) == 90)
        #expect(target(covered: 950, duration: nil) == nil)
        // A negative or non-finite watermark cannot manufacture a deeper rung.
        #expect(target(covered: -50, duration: 1826) == 90)
        #expect(target(covered: .nan, duration: 1826) == 90)
    }

    @Test("every decline path is reachable and named")
    func declineReasons() {
        let now = 2_000_000.0
        let capped = makeAnalysisJob(
            state: "superseded",
            lastErrorCode: "maxAttemptsReached:transcription:zeroCoverage",
            updatedAt: now - 86_400
        )
        func decide(
            job: AnalysisJob = capped,
            nextOrdinal: Int? = 1,
            covered: Double = 0,
            duration: Double? = Self.durationSec
        ) -> AnalysisWorkScheduler.CapOutRetryDecision {
            AnalysisWorkScheduler.capOutRetryDecision(
                baseWorkKey: Self.baseWorkKey,
                chainTail: job,
                nextOrdinal: nextOrdinal,
                transcriptCoverageSec: covered,
                episodeDurationSec: duration,
                tiers: Self.defaultTiers,
                now: now
            )
        }

        #expect(decide() == .mint(.init(
            workKey: Self.retryKey(1), ordinal: 1, desiredCoverageSec: 90
        )))
        #expect(decide(nextOrdinal: 2) == .mint(.init(
            workKey: Self.retryKey(2), ordinal: 2, desiredCoverageSec: 90
        )))
        #expect(decide(job: makeAnalysisJob(state: "superseded", updatedAt: now - 86_400))
            == .declined(.notACapOutTerminal))
        #expect(decide(nextOrdinal: nil) == .declined(.budgetSpent))
        #expect(decide(job: makeAnalysisJob(
            state: "superseded",
            lastErrorCode: "maxAttemptsReached:cancelMidRun",
            updatedAt: now - 60
        )) == .declined(.cooling))
        #expect(decide(covered: Self.durationSec) == .declined(.noOutstandingTranscript))

        // Ordering: budget exhaustion outranks cooling, so a spent chain reports
        // the terminating reason rather than a transient one.
        #expect(decide(
            job: makeAnalysisJob(
                state: "superseded",
                lastErrorCode: "maxAttemptsReached:cancelMidRun",
                updatedAt: now - 60
            ),
            nextOrdinal: nil
        ) == .declined(.budgetSpent))
    }

    /// The cooldown boundary is inclusive, and one second short of it is not.
    @Test("the cooldown boundary is exact")
    func cooldownBoundary() {
        let now = 2_000_000.0
        func decide(age: Double) -> AnalysisWorkScheduler.CapOutRetryDecision {
            AnalysisWorkScheduler.capOutRetryDecision(
                baseWorkKey: Self.baseWorkKey,
                chainTail: makeAnalysisJob(
                    state: "superseded",
                    lastErrorCode: "maxAttemptsReached:transcription:zeroCoverage",
                    updatedAt: now - age
                ),
                nextOrdinal: 1,
                transcriptCoverageSec: 0,
                episodeDurationSec: Self.durationSec,
                tiers: Self.defaultTiers,
                now: now
            )
        }
        let cooldown = AnalysisWorkScheduler.capOutRetryCooldownSeconds
        #expect(decide(age: cooldown - 1) == .declined(.cooling))
        #expect(decide(age: cooldown) == .mint(.init(
            workKey: Self.retryKey(1), ordinal: 1, desiredCoverageSec: 90
        )))
    }
}
