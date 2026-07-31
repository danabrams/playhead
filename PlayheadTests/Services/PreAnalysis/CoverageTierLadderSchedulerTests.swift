// CoverageTierLadderSchedulerTests.swift
// playhead-8bp2: end-to-end proof that a background episode nobody plays gets
// its audio TRANSCRIBED, driven through the real scheduler + runner + transcript
// engine rather than through the outcome-arm predicates in isolation.
//
// The defect these tests pin, measured on the 2026-07-30 device pull: of the 34
// episodes over 15 minutes, 19 sat in `analysisState = 'queued'` holding 909 of
// the 1,102 untranscribed minutes. Two independent causes, both here:
//
//   1. The tier-advance arm asked `outcome.cueCoverageSec >= desiredCoverageSec`.
//      `cueCoverageSec` is the end time of the last confident AD WINDOW, so an
//      episode whose ads sit early reported "coverage insufficient" for a tier it
//      had fully transcribed, terminated `state='complete'`, and — `workKey`
//      being UNIQUE with `INSERT OR IGNORE` inserts — could never be enqueued
//      again. `25994AA1` is the extreme: 90 seconds transcribed of 6,147.
//
//   2. Even a healthy ladder stopped at `t2DepthSeconds` (900 s), so no amount
//      of clean background passes could transcribe a long episode past fifteen
//      minutes. The only assets at high coverage on the pull were ones a
//      listener's playhead had escalated past the ladder.
//
// A test that only asserted a row changed state would pass on a fix that moves
// rows around without reading any more audio, so both tests below assert on
// `analysis_assets.fastTranscriptCoverageEndTime` — seconds of audio actually
// transcribed — and the second one asserts the walk TERMINATES.

import Foundation
import OSLog
import Testing
@testable import Playhead

@Suite("Coverage tier ladder — background transcription reach (playhead-8bp2)")
struct CoverageTierLadderSchedulerTests {

    private static let episodeId = "ep-8bp2"
    private static let assetId = "asset-8bp2"
    private static let fingerprint = "fp-8bp2"
    private static let shardSeconds: Double = 30
    private static let shardCount = 40
    /// 1,200 s — comfortably past `t2DepthSeconds` (900 s), so the ladder's old
    /// ceiling is visible and the duration rung is distinguishable from T2.
    private static let durationSec = Double(shardCount) * shardSeconds

    /// A recognizer that returns one segment per shard, so the transcript engine
    /// persists chunks and `fastTranscriptCoverageEndTime` genuinely advances.
    /// `StubSpeechRecognizer` returns an empty transcript, which the runner
    /// classifies as `transcription:zeroCoverage` — it never reaches the tier
    /// arms at all.
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

    /// Deterministic clock so the coverage-insufficient / failure backoffs can be
    /// stepped over instead of slept through.
    private final class TierLadderClock: @unchecked Sendable {
        private let lock: OSAllocatedUnfairLock<Date>
        init(start: Date) { lock = OSAllocatedUnfairLock(initialState: start) }
        var value: Date { lock.withLock { $0 } }
        func advance(by seconds: TimeInterval) {
            lock.withLock { $0 = $0.addingTimeInterval(seconds) }
        }
    }

    private func seedEpisode(_ store: AnalysisStore, desiredCoverageSec: Double) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: Self.fingerprint,
                weakFingerprint: nil,
                sourceURL: "file:///tmp/8bp2.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.queued.rawValue,
                analysisVersion: PreAnalysisConfig.analysisVersion,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.durationSec
            )
        )
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-8bp2-t0",
                jobType: "preAnalysis",
                episodeId: Self.episodeId,
                analysisAssetId: Self.assetId,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: Self.fingerprint,
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: Self.fingerprint,
                priority: 10,
                desiredCoverageSec: desiredCoverageSec,
                state: "queued"
            )
        )
    }

    private func makeScheduler(
        store: AnalysisStore,
        clock: @escaping @Sendable () -> Date
    ) async throws -> AnalysisWorkScheduler {
        let downloads = StubDownloadProvider()
        downloads.cachedURLs[Self.episodeId] = URL(fileURLWithPath: "/tmp/8bp2.m4a")
        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = (0..<Self.shardCount).map {
            makeShard(
                id: $0,
                episodeID: Self.episodeId,
                startTime: Double($0) * Self.shardSeconds,
                duration: Self.shardSeconds
            )
        }
        let speechService = SpeechService(recognizer: ShardTranscribingRecognizer())
        try await speechService.loadFastModel()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            // No ad windows at all — so `cueCoverageSec` is 0 on every pass, which
            // is precisely the shape that used to strand these episodes.
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
            clock: clock
        )
    }

    private func transcriptCoverage(_ store: AnalysisStore) async throws -> Double {
        try await store.fetchAsset(id: Self.assetId)?.fastTranscriptCoverageEndTime ?? 0
    }

    private func allJobs(_ store: AnalysisStore) async throws -> [AnalysisJob] {
        var jobs: [AnalysisJob] = []
        for state in ["queued", "paused", "running", "complete", "failed", "superseded"] {
            jobs += try await store.fetchJobsByState(state)
        }
        return jobs
    }

    /// One dispatch of a T0 job on an episode with no ads in its first 90 s must
    /// leave a NEXT RUNG behind. Pre-fix this pass produced no successor: the arm
    /// read `cueCoverageSec` (0) against the 90 s target and routed to
    /// `coverageInsufficient`, which terminates `state='complete'` after a single
    /// no-progress repeat and is unrecoverable through `INSERT OR IGNORE`.
    @Test("a tier with no ads in it still advances the ladder")
    func tierAdvancesWithoutAnyCue() async throws {
        let store = try await makeTestStore()
        try await seedEpisode(store, desiredCoverageSec: 90)
        let clock = TierLadderClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        let scheduler = try await makeScheduler(store: store) { clock.value }

        let dispatched = await scheduler.processNextDispatchableJobForTesting()
        #expect(dispatched)

        let t0 = try await store.fetchJob(byId: "job-8bp2-t0")
        #expect(t0?.state == "complete")
        #expect(t0?.lastErrorCode == nil,
                "the tier's audio was transcribed; terminating it as coverage-insufficient is the bug")

        let successors = try await allJobs(store).filter { $0.jobId != "job-8bp2-t0" }
        #expect(successors.count == 1, "exactly one next rung, not zero and not a fan-out")
        #expect(successors.first?.desiredCoverageSec == 300)
        #expect(successors.first?.workKey == "\(Self.fingerprint):\(PreAnalysisConfig.analysisVersion):preAnalysis:300")

        // The pass read audio, it did not merely move a row.
        #expect(try await transcriptCoverage(store) >= 90)
    }

    /// The reach claim, end to end: driven to quiescence, a 1,200 s episode that
    /// nobody plays and that contains no ads ends up FULLY transcribed, and the
    /// ladder stops on its own. Pre-fix the ceiling was `t2DepthSeconds` = 900 s
    /// even in the best case, and in practice this episode stopped at 90 s.
    @Test("an unplayed long episode is transcribed end to end, and the ladder stops")
    func ladderReachesEndOfEpisodeAndTerminates() async throws {
        let store = try await makeTestStore()
        try await seedEpisode(store, desiredCoverageSec: 90)
        let clock = TierLadderClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        let scheduler = try await makeScheduler(store: store) { clock.value }

        let before = try await transcriptCoverage(store)
        #expect(before == 0)

        // Generously more iterations than the ladder can consume. The count of
        // dispatches is asserted below against the ladder's own bound, so a
        // regression that loops cannot hide inside the loop budget.
        var dispatches = 0
        for _ in 0..<24 {
            if await scheduler.processNextDispatchableJobForTesting() {
                dispatches += 1
            }
            clock.advance(by: 7_200)
        }

        let after = try await transcriptCoverage(store)
        #expect(after >= Self.durationSec,
                "the whole episode must be transcribed, got \(after) of \(Self.durationSec)")
        #expect(after > before)

        // The ladder is at most four rungs (T0, T1, T2, duration), so the whole
        // walk is at most four dispatches. Anything more means a rung repeated.
        #expect(dispatches <= 4, "ladder consumed \(dispatches) dispatches; it is bounded at 4 rungs")

        let jobs = try await allJobs(store)
        #expect(jobs.allSatisfy { $0.state == "complete" },
                "every rung must reach a terminal; a leftover queued/paused row means the walk did not stop")
        #expect(jobs.contains { $0.desiredCoverageSec == Self.durationSec },
                "the last rung must be the episode itself")
        #expect(Set(jobs.map(\.workKey)).count == jobs.count,
                "rungs must not collide on workKey — a collision is swallowed by INSERT OR IGNORE")

        // And it is genuinely quiescent: another pass finds nothing to do.
        clock.advance(by: 7_200)
        #expect(await scheduler.processNextDispatchableJobForTesting() == false)
    }
}
