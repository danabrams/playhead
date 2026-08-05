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
    ///
    /// It STARTS AT THE WALL CLOCK and only ever moves forward, which is
    /// load-bearing rather than incidental: `garbageCollectOldJobs` (step 5)
    /// deletes `complete`/`superseded` rows whose `updatedAt` predates
    /// `Date() - 7 days` and reads the REAL clock, not this one. A fixture
    /// anchored at a fixed historical epoch — the pattern the pure scheduler
    /// suites use, because they never run a reconciler — has every seeded
    /// terminal garbage-collected out from under it before step 7 runs, and
    /// then reports a cap-out retry that "did not mint" for a row that no
    /// longer exists.
    private final class RetryClock: @unchecked Sendable {
        private let lock: OSAllocatedUnfairLock<Date>
        init() { lock = OSAllocatedUnfairLock(initialState: Date()) }
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
        let clock = RetryClock()
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
        // audio it has not read. THIS is the assertion that kills a wrong-target
        // implementation — re-using the terminated job's own 90 s target, or
        // reading the job row's `transcriptCoverageSec` (0, because
        // `updateJobProgress` stores the last run rather than a high-water mark)
        // both land on 90 and are caught here.
        #expect(retry?.desiredCoverageSec == 900,
                "expected the next rung above 300 s; got \(retry?.desiredCoverageSec ?? -1)")

        // Drive it. The claim is seconds of audio.
        var dispatches = 0
        for _ in 0..<8 {
            if await scheduler.processNextDispatchableJobForTesting() { dispatches += 1 }
            clock.advance(by: 7_200)
        }
        #expect(dispatches >= 1)

        // Seconds of audio. This is what a row-state assertion cannot see: a
        // minted row the scheduler declines to dispatch, a target the runner
        // cannot act on, or a mint that lands after the lane is already closed
        // all leave `fastTranscriptCoverageEndTime` exactly where it was.
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
        let clock = RetryClock()
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
            adScanFraction: nil,
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
        let clock = RetryClock()
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

    /// The cap-out is not always on the row holding the BASE key. The
    /// tier-advance arm mints successors at `<base>:<depth>`, so an episode that
    /// cleared its 90 s rung and then exhausted its attempts at 300 s leaves the
    /// base row `complete` and the real terminal one row further along.
    ///
    /// Anchoring the decision on the base row alone reads that as "not a
    /// cap-out" and strands the episode exactly as before — a gap that does not
    /// appear on the 2026-07-31 device pull (no row there carries a `:NNN`
    /// suffix; the deep targets came from in-place escalation) but is reachable
    /// the moment a ladder walk gets one rung in before failing.
    @Test("a cap-out on a tier successor is re-requested, not just one on the base key")
    func capOutOnTierSuccessorIsReRequested() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock()
        let terminalAt = clock.value.timeIntervalSince1970 - 86_400
        // The base row cleared its rung cleanly.
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: 300,
            supersededAt: terminalAt - 3_600,
            lastErrorCode: "",
            state: "complete"
        )
        // Its tier successor is where the attempts ran out. Newer than the base
        // row, so it is the episode's chain tail.
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-y8f3-tier300",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            analysisAssetId: Self.assetId,
            workKey: "\(Self.baseWorkKey):300",
            sourceFingerprint: Self.fingerprint,
            desiredCoverageSec: 300,
            state: "superseded",
            attemptCount: 5,
            lastErrorCode: "maxAttemptsReached:transcription:zeroCoverage",
            createdAt: terminalAt,
            updatedAt: terminalAt
        ))

        let report = try await makeReconciler(
            store: store, downloads: downloads, clock: clock
        ).reconcile()

        #expect(report.capOutRetriesMinted == 1)
        let retry = try await store.fetchJob(byWorkKey: Self.retryKey(1))
        #expect(retry?.state == "queued")
        // The ordinal still comes off the BASE key, so the budget is one ledger
        // per episode however deep the ladder got before it failed.
        #expect(retry?.desiredCoverageSec == 900)
    }

    /// The mirror image, and the reason the tail is the NEWEST row rather than
    /// "any cap-out anywhere in the episode's history": an episode whose most
    /// recent outcome was a clean `complete` is not this bead's to re-request,
    /// even though an older row is a cap-out terminal.
    @Test("an older cap-out under a newer clean terminal is not re-requested")
    func staleCapOutUnderNewerCleanTerminalIsNotReRequested() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock()
        let terminalAt = clock.value.timeIntervalSince1970 - 86_400
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: 300,
            supersededAt: terminalAt - 3_600
        )
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-y8f3-tier300",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            analysisAssetId: Self.assetId,
            workKey: "\(Self.baseWorkKey):300",
            sourceFingerprint: Self.fingerprint,
            desiredCoverageSec: 300,
            state: "complete",
            createdAt: terminalAt,
            updatedAt: terminalAt
        ))

        let report = try await makeReconciler(
            store: store, downloads: downloads, clock: clock
        ).reconcile()

        #expect(report.capOutRetriesMinted == 0)
        #expect(report.reEnqueuesSwallowed == 1)
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
            let clock = RetryClock()
            try await seedCappedOutEpisode(
                store,
                priorTranscriptCoverageSec: 0,
                supersededAt: clock.value.timeIntervalSince1970 - 86_400,
                lastErrorCode: cause ?? ""
            )
            // `makeAnalysisJob` cannot express a NULL lastErrorCode through the
            // loop above, so clear it explicitly for the nil case. This is how
            // `supersedeStaleVersions` and the episode-deleted path record a
            // genuine supersession: `updateJobState`'s default NULLs the column.
            if cause == nil {
                try await store.updateJobState(jobId: "job-y8f3-base", state: "superseded")
            }
            // `updateJobState` stamps `updatedAt = Date()`, which resets the age
            // this fixture seeded. Without stepping past the cooldown the nil
            // arm would decline as `.cooling` and prove nothing about the
            // discriminator — dropping the prefix check from
            // `isAttemptCapTerminal` would still leave the test green.
            clock.advance(by: AnalysisWorkScheduler.capOutRetryCooldownSeconds + 60)

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
        let clock = RetryClock()
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
        let clock = RetryClock()
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
        let clock = RetryClock()
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

    @Test("the cap-out retry work key is built off the base, once, without nesting")
    func retryKeyShape() {
        let base = Self.baseWorkKey
        #expect(Self.retryKey(1) == "\(base):capRetry:1")
        #expect(Self.retryKey(2) == "\(base):capRetry:2")
        #expect(Self.retryKey(1) != Self.retryKey(2))
    }

    /// onn6's ordinal parser MUST NOT match a `capRetry` key, and this is live
    /// rather than symmetric bookkeeping: the tier-advance arm keys on
    /// `adScanRedriveOrdinal(workKey:) != nil` to decide a row may not walk the
    /// ladder. If it matched, a cap-out retry that transcribed its rung would be
    /// refused its successor and the episode would stall one rung in — the
    /// opposite of what this bead is for.
    @Test("onn6's ad-scan-redrive ordinal does not match a cap-out retry key")
    func adScanRedriveOrdinalIgnoresCapRetryKeys() {
        #expect(AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: Self.retryKey(1)) == nil)
        #expect(AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: Self.retryKey(2)) == nil)
        // The control: its own key still parses, so this is not vacuously true.
        #expect(AnalysisWorkScheduler.adScanRedriveOrdinal(
            workKey: "\(Self.baseWorkKey):\(AnalysisWorkScheduler.adScanRedriveWorkKeyMarker):1"
        ) == 1)
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
            duration: Double? = Self.durationSec,
            adScanFraction: ReachRatio? = 1.0
        ) -> AnalysisWorkScheduler.CapOutRetryDecision {
            AnalysisWorkScheduler.capOutRetryDecision(
                baseWorkKey: Self.baseWorkKey,
                chainTail: job,
                nextOrdinal: nextOrdinal,
                transcriptCoverageSec: covered,
                episodeDurationSec: duration,
                adScanFraction: adScanFraction,
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
        // playhead-9y9e: the transcript ladder being exhausted is now only HALF
        // of "nothing outstanding" — the ad scan has to clear its floor too, so
        // this case passes `adScanFraction: 1.0` via the helper default.
        #expect(decide(covered: Self.durationSec) == .declined(.noOutstandingWork))

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
                adScanFraction: nil,
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

    // MARK: - playhead-9y9e: an unread episode is outstanding work

    /// RT01 — THE DEFECT. A cap-out terminal on a FULLY TRANSCRIBED episode had
    /// no outstanding transcript by construction, so the ladder term declined
    /// every one of them. That is the exact shape this rescue exists for:
    /// measured on the 2026-08-03 device pull, AD5F3A0A is transcribed 4,281 s
    /// of 4,281 s, its ad scan covers 20.7 %, and its `…:adScanRedrive:1` row is
    /// `superseded` with `maxAttemptsReached:transcription:zeroCoverage`.
    ///
    /// The mint asks for the DEEPEST rung, because the pass's job here is to
    /// reach the ad-detection stage over the whole episode; a shallower target
    /// would bound the shard set it decodes.
    @Test("a fully transcribed episode whose ad scan is short still gets a retry")
    func fullyTranscribedButUnscannedIsOutstandingWork() {
        let now = 2_000_000.0
        func decide(adScanFraction: ReachRatio?) -> AnalysisWorkScheduler.CapOutRetryDecision {
            AnalysisWorkScheduler.capOutRetryDecision(
                baseWorkKey: Self.baseWorkKey,
                chainTail: makeAnalysisJob(
                    state: "superseded",
                    lastErrorCode: "maxAttemptsReached:transcription:zeroCoverage",
                    updatedAt: now - 86_400
                ),
                nextOrdinal: 1,
                // Past the top of the ladder: nothing left to transcribe.
                transcriptCoverageSec: Self.durationSec,
                episodeDurationSec: Self.durationSec,
                adScanFraction: adScanFraction,
                tiers: Self.defaultTiers,
                now: now
            )
        }
        let ladder = AnalysisWorkScheduler.coverageTierLadder(
            tiers: Self.defaultTiers,
            episodeDurationSec: Self.durationSec
        )
        #expect(ladder.last == Self.durationSec,
                "fixture premise: the deepest rung is the episode duration")
        let expected = AnalysisWorkScheduler.CapOutRetryDecision.mint(.init(
            workKey: Self.retryKey(1),
            ordinal: 1,
            desiredCoverageSec: ladder.last ?? Self.durationSec
        ))

        // Measured short → owed → mint.
        #expect(decide(adScanFraction: 0.207) == expected)
        // UNMEASURED is not sufficient. A never-scanned asset has no
        // `semantic_scan_results` rows at all, so its fraction is `nil` rather
        // than a synthetic 0 — reading `nil` as "covered" would make the
        // never-scanned episode the one case this never fires for.
        #expect(decide(adScanFraction: nil) == expected)
        #expect(decide(adScanFraction: ReachRatio(.nan)) == expected)
        // One tick under the floor is still owed.
        let floor = AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
        #expect(decide(adScanFraction: ReachRatio(floor.rawValue - 0.001)) == expected)

        // At or above the floor there is genuinely nothing left to do, and the
        // decline is NAMED. This is the termination bound: without it the
        // rescue would mint forever on an episode that is finished.
        #expect(decide(adScanFraction: floor) == .declined(.noOutstandingWork))
        #expect(decide(adScanFraction: 1.0) == .declined(.noOutstandingWork))
    }

    /// RT02 — the new term is a FALLBACK, not an override. While the transcript
    /// ladder still has a rung, the target must stay the ladder's, whatever the
    /// scan says. Otherwise a half-transcribed episode would be asked for the
    /// whole thing and the ladder's cheapest-probe-first ordering would be gone.
    @Test("an outstanding transcript rung still wins over the ad-scan term")
    func transcriptLadderTakesPrecedence() {
        let now = 2_000_000.0
        func decide(adScanFraction: ReachRatio?) -> AnalysisWorkScheduler.CapOutRetryDecision {
            AnalysisWorkScheduler.capOutRetryDecision(
                baseWorkKey: Self.baseWorkKey,
                chainTail: makeAnalysisJob(
                    state: "superseded",
                    lastErrorCode: "maxAttemptsReached:transcription:zeroCoverage",
                    updatedAt: now - 86_400
                ),
                nextOrdinal: 1,
                transcriptCoverageSec: 0,
                episodeDurationSec: Self.durationSec,
                adScanFraction: adScanFraction,
                tiers: Self.defaultTiers,
                now: now
            )
        }
        let cheapest = AnalysisWorkScheduler.CapOutRetryDecision.mint(.init(
            workKey: Self.retryKey(1), ordinal: 1, desiredCoverageSec: 90
        ))
        #expect(decide(adScanFraction: nil) == cheapest)
        #expect(decide(adScanFraction: 1.0) == cheapest)
    }

    /// RT03 — the ad-scan term does not outrank the guards ahead of it. A row
    /// that is not a cap-out terminal, a spent budget and a cooling terminal all
    /// still refuse, however unscanned the episode is. Without this the new term
    /// would be an unbounded retry wearing a floor.
    @Test("the ad-scan term does not bypass the terminal, budget or cooldown guards")
    func adScanTermDoesNotBypassEarlierGuards() {
        let now = 2_000_000.0
        func decide(
            job: AnalysisJob,
            nextOrdinal: Int?
        ) -> AnalysisWorkScheduler.CapOutRetryDecision {
            AnalysisWorkScheduler.capOutRetryDecision(
                baseWorkKey: Self.baseWorkKey,
                chainTail: job,
                nextOrdinal: nextOrdinal,
                transcriptCoverageSec: Self.durationSec,
                episodeDurationSec: Self.durationSec,
                // Maximally owed.
                adScanFraction: 0,
                tiers: Self.defaultTiers,
                now: now
            )
        }
        let capped = makeAnalysisJob(
            state: "superseded",
            lastErrorCode: "maxAttemptsReached:transcription:zeroCoverage",
            updatedAt: now - 86_400
        )
        #expect(decide(
            job: makeAnalysisJob(state: "complete", updatedAt: now - 86_400),
            nextOrdinal: 1
        ) == .declined(.notACapOutTerminal))
        #expect(decide(job: capped, nextOrdinal: nil) == .declined(.budgetSpent))
        #expect(decide(
            job: makeAnalysisJob(
                state: "superseded",
                lastErrorCode: "maxAttemptsReached:transcription:zeroCoverage",
                updatedAt: now - 60
            ),
            nextOrdinal: 1
        ) == .declined(.cooling))
    }

    // MARK: - playhead-9y9e R3 review: THE WIRE-IN

    /// `scanCohortJSON` is VALIDATED on insert — `AnalysisStore` decodes it as a
    /// `ScanCohort` and throws `invalidScanCohortJSON` otherwise — so `"{}"` is
    /// not a usable placeholder here.
    private static let cohortJSON: String = {
        let cohort = ScanCohort(
            promptLabel: "y8f3-test",
            promptHash: "prompt-v1",
            schemaHash: "schema-v1",
            scanPlanHash: "plan-v1",
            normalizationHash: "norm-v1",
            osBuild: "26A123",
            locale: "en_US",
            appBuild: "1"
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = (try? encoder.encode(cohort)) ?? Data()
        return String(decoding: data, as: UTF8.self)
    }()

    /// One coverage-lane scan row tiling `[0, end]`, so the asset's MEASURED
    /// `adScanFraction` clears its floor. Deliberately minimal: this suite needs
    /// the fraction to come out of the store, not a realistic scan.
    private func seedFullAdScan(_ store: AnalysisStore, end: Double) async throws {
        try await store.insertSemanticScanResult(SemanticScanResult(
            id: "\(Self.assetId)-scan-0",
            analysisAssetId: Self.assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 9,
            windowStartTime: 0,
            windowEndTime: end,
            scanPass: SemanticScanCoverage.coverageScanPass,
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: Self.cohortJSON,
            transcriptVersion: "tx-v1"
        ))
    }

    /// RT15 — **THE WIRE-IN, and the reason this test had to be written.**
    ///
    /// Every other test of the ad-scan arm calls
    /// `AnalysisWorkScheduler.capOutRetryDecision` DIRECTLY, so every one of them
    /// stays green if the reconciler stops measuring and hands the decision a
    /// constant "fully scanned". That is the second failure family this queue
    /// keeps paying for — a correct mechanism production never invokes — and a
    /// decision-matrix test cannot see it by construction. Only a `reconcile()`
    /// can.
    ///
    /// The field case is AD5F3A0A on the 2026-08-03 pull: transcribed 4,281 s of
    /// 4,281 s, ad scan 20.7 %, `…:adScanRedrive:1` an attempt-cap terminal.
    /// Here: fully transcribed, so `outstandingTranscriptTarget` is nil and the
    /// ad-scan arm is the ONLY thing that can mint; and never scanned, so
    /// `adScanFraction` is `nil`, which reads as owed.
    @Test("reconcile mints the cap-out retry for a transcribed but unscanned episode")
    func reconcileMintsForTranscribedButUnscannedEpisode() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock()
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: Self.durationSec,
            supersededAt: clock.value.timeIntervalSince1970 - 86_400
        )
        let reconciler = makeReconciler(store: store, downloads: downloads, clock: clock)

        #expect(try await reconciler.reconcile().capOutRetriesMinted == 1,
                "a fully transcribed, never-scanned episode is outstanding WORK")

        // And it asks for the DEEPEST rung, because the pass's job here is to
        // reach the ad-detection stage over the whole episode.
        let minted = try #require(try await store.fetchJob(byWorkKey: Self.retryKey(1)))
        let ladder = AnalysisWorkScheduler.coverageTierLadder(
            tiers: Self.defaultTiers,
            episodeDurationSec: Self.durationSec
        )
        #expect(minted.desiredCoverageSec == ladder.last)
    }

    /// RT17 — the TERMINATION BOUND of the same wire-in, and the direction the
    /// mint test cannot see.
    ///
    /// If the reconciler simply stopped reading and left `nil`, `isOwed(nil)` is
    /// true and this rescue would mint on a finished episode forever — an
    /// unbounded retry wearing a floor. So the read has to be able to say NO as
    /// well as YES, and only a `reconcile()`-level test proves it does.
    @Test("reconcile mints nothing once the ad scan clears its floor")
    func reconcileMintsNothingOnceAdScanClearsTheFloor() async throws {
        let store = try await makeTestStore()
        let downloads = makeDownloads()
        let clock = RetryClock()
        try await seedCappedOutEpisode(
            store,
            priorTranscriptCoverageSec: Self.durationSec,
            supersededAt: clock.value.timeIntervalSince1970 - 86_400
        )
        try await seedFullAdScan(store, end: Self.durationSec)

        // The premise, stated so fixture drift cannot make this vacuous.
        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([Self.assetId])[Self.assetId]
        )
        let fraction = try #require(summary.adScanFraction)
        #expect(fraction >= AnalysisJobRunner.semanticBackfillSufficientAdScanFraction,
                "premise: this asset's measured ad scan clears the floor")

        let reconciler = makeReconciler(store: store, downloads: downloads, clock: clock)
        #expect(try await reconciler.reconcile().capOutRetriesMinted == 0,
                "nothing is owed, so the rescue must decline — otherwise it mints forever")
        #expect(try await store.fetchJob(byWorkKey: Self.retryKey(1)) == nil)
    }
}
