// AnalysisWorkSchedulerQueueProgressIntegrationTests.swift
// playhead-gyvb.1: integration-level pin on the queued-asset progress
// invariant. Drives a real `AnalysisWorkScheduler` + `AnalysisJobRunner`
// + `AnalysisStore` end-to-end with a poisoned (always-fail) decode
// stub blocking a clean queued job behind it. The poisoned job must
// terminate via `maxAttemptsReached`, freeing the running slot and
// admitting the clean queued job within the test window.
//
// This is the production scenario the 2026-04-27 incident exposed:
// a single asset stuck at `state='running'` indefinitely starves
// every queued asset behind it. The bookkeeping invariant
// (every terminal arm bumps `attemptCount`) is what bounds how long
// a poisoned slot can stay running before yielding. This file lives
// under `PlayheadTests/IntegrationTests/` because the spec
// (playhead-gyvb.1) called for at least one cross-component
// integration test for the fix; the other bookkeeping tests live
// in `PlayheadTests/Services/PreAnalysis/`.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — queue-progress integration (playhead-gyvb.1)")
struct AnalysisWorkSchedulerQueueProgressIntegrationTests {

    /// Audio provider stub whose `decode(...)` always throws — emulates
    /// the production "Operation Interrupted" loop that pinned a slot
    /// pre-fix. Routes the runner through the `.failed` outcome arm,
    /// which (post-fix) bumps `attemptCount` on every cycle and
    /// supersedes the job once it crosses `maxAttemptCount`.
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
            config: PreAnalysisConfig()
        )
    }

    @Test("a poisoned-decode asset frees its slot so a queued asset behind it advances")
    func poisonedAssetEventuallyFreesSlot() async throws {
        // Two jobs sharing a single running-slot:
        //  - poisoned: priority=10, attemptCount=4, decode always fails.
        //    A single failure cycle drives attemptCount→5 and trips
        //    `maxAttemptsReached`, terminating via the `.failed` arm.
        //  - clean: priority=0, lower priority — only admitted once
        //    the poisoned slot is freed.
        //
        // Pre-stamping `attemptCount=4` keeps the test deterministic
        // without waiting through the exponential backoff between
        // failure attempts (60s → 120s → 240s → ...).
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-poison-int"] = URL(fileURLWithPath: "/tmp/ep-poison-int.mp3")
        downloads.cachedURLs["ep-clean-int"] = URL(fileURLWithPath: "/tmp/ep-clean-int.mp3")

        let poisoned = makeAnalysisJob(
            jobId: "poison-int",
            jobType: "preAnalysis",
            episodeId: "ep-poison-int",
            analysisAssetId: "asset-poison-int",
            workKey: "fp-poison-int:1:preAnalysis",
            sourceFingerprint: "fp-poison-int",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 4
        )
        let clean = makeAnalysisJob(
            jobId: "clean-int",
            jobType: "preAnalysis",
            episodeId: "ep-clean-int",
            analysisAssetId: "asset-clean-int",
            workKey: "fp-clean-int:1:preAnalysis",
            sourceFingerprint: "fp-clean-int",
            priority: 0,
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

        // The clean job's state moving out of `queued` (to running or
        // any terminal) is the load-bearing signal: the slot is no
        // longer pinned by the poisoned asset.
        let cleanProgressed = await pollUntil {
            let j = try? await store.fetchJob(byId: "clean-int")
            switch j?.state {
            case "queued", nil: return false
            default: return true
            }
        }
        await scheduler.stop()

        #expect(cleanProgressed,
                "Queued work behind a poisoned asset must eventually be admitted")
        let poisonedAfter = try await store.fetchJob(byId: "poison-int")
        #expect(poisonedAfter?.state == "superseded",
                "Poisoned job must terminate via maxAttemptsReached")
        #expect(poisonedAfter?.leaseOwner == nil,
                "Lease must be released so the slot is genuinely freed")
    }
}
