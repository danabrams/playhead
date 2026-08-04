// TranscriptionAlreadyCompleteTests.swift
// playhead-9y9e: a pass that adds no transcript to an ALREADY TRANSCRIBED asset
// is not a transcription failure, and must reach Stage 4.
//
// THE FIELD SHAPE, 2026-08-03 device pull. Asset AD5F3A0A is 4,281 s long, its
// `fastTranscriptCoverageEndTime` is 4,281 s, and it holds 45
// `semantic_scan_results` rows over 20.7 % of its audio — i.e. transcribed, and
// barely read for ads. playhead-onn6 minted it an ad-scan re-drive, whose whole
// purpose is to reach the semantic scan. Both ordinals of that budget were spent
// without ever reaching it:
//
//   …:preAnalysis:adScanRedrive:1  superseded  maxAttemptsReached:transcription:zeroCoverage  attempts=5
//   …:preAnalysis:adScanRedrive:2  running     transcription:zeroCoverage                     attempts=2
//
// and two `fullEpisodeScan` rows sat `queued` behind them.
//
// WHY IT FAILS THERE AND NOWHERE ELSE. The transcription stage's cap is a FLAT
// 300 s while the work under it scales with the episode:
// `TranscriptEngineService.runTranscriptionLoop` re-runs full ASR over every
// shard, and playhead-mptr deliberately ORDERS already-backed shards last rather
// than skipping them (a skipped shard can never take the
// duplicate-fingerprint arm's `speakerId` / `avgConfidence` upgrade). On an
// asset with nothing left to read the whole budget goes on re-reading, the cap
// wins, and the timeout arm of the runner's task group returns a HARDCODED
// `(0, nil, false)` — it never consults `persistedCoverage()`, which the
// `.completed` arm does. So zero here means "this pass added nothing", which on
// a finished transcript is success wearing a failure's clothes.
//
// (playhead-y8f3's `outstandingTranscriptTarget` documentation asserted the
// opposite — "a pass over already-covered audio reports the full watermark and
// terminates `complete` through `tierAdvance` … Zero coverage arises from an
// engine failure or the runner's silent-engine timeout". The second sentence is
// right and the first only holds on the `.completed` arm; the silent-engine
// timeout is exactly what an already-covered asset produces.)

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-9y9e: an already-transcribed asset reaches ad detection")
struct TranscriptionAlreadyCompleteTests {

    private static let assetId = "rt-asset"
    private static let episodeId = "rt-ep"
    /// 4 shards × 30 s. The decoded span and the declared duration agree, so the
    /// transcript floor is a clean read.
    private static let durationSec: Double = 120

    // MARK: - Fixture

    private func makeRequest(jobId: String) throws -> AnalysisRangeRequest {
        let tmpDir = try makeTempDir(prefix: "TranscriptionAlreadyCompleteTests")
        let audioFile = tmpDir.appendingPathComponent("episode.m4a")
        FileManager.default.createFile(atPath: audioFile.path, contents: Data())
        let localURL = try #require(LocalAudioURL(audioFile))
        return AnalysisRangeRequest(
            jobId: jobId,
            episodeId: Self.episodeId,
            podcastId: "rt-pod",
            analysisAssetId: Self.assetId,
            audioURL: localURL,
            desiredCoverageSec: Self.durationSec,
            mode: .preRollWarmup,
            outputPolicy: .writeWindowsAndCues,
            priority: .medium
        )
    }

    private func seedAsset(
        _ store: AnalysisStore,
        watermark: Double?,
        transcribedTo: Double
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: Self.assetId,
            episodeId: Self.episodeId,
            assetFingerprint: "rt-fp",
            weakFingerprint: nil,
            sourceURL: "",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: watermark,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: Self.durationSec
        ))
        guard transcribedTo > 0 else { return }
        // Contiguous 10 s chunks. Contiguity is deliberate: this suite is about
        // the REACH of the transcript, and a gappy fixture would confound the
        // floor with the bridging behaviour that `SemanticScanClaimTests`
        // already pins.
        var chunks: [TranscriptChunk] = []
        var start = 0.0
        var index = 0
        while start < transcribedTo {
            let end = min(start + 10, transcribedTo)
            chunks.append(TranscriptChunk(
                id: "\(Self.assetId)-c\(index)",
                analysisAssetId: Self.assetId,
                segmentFingerprint: "\(Self.assetId)-fp\(index)",
                chunkIndex: index,
                startTime: start,
                endTime: end,
                text: "seg \(index)",
                normalizedText: "seg \(index)",
                pass: "fast",
                modelVersion: "test-asr",
                transcriptVersion: nil,
                atomOrdinal: nil
            ))
            start = end
            index += 1
        }
        try await store.insertTranscriptChunks(chunks)
    }

    /// Seed the `analysis_jobs` row + lease so the journal row gets a real
    /// `{generationID, schedulerEpoch}` to join on.
    private func seedLeasedJob(_ store: AnalysisStore, jobId: String) async throws -> String {
        let inserted = try await store.insertJob(makeAnalysisJob(
            jobId: jobId,
            episodeId: Self.episodeId,
            analysisAssetId: Self.assetId,
            workKey: "rt-wk-\(UUID().uuidString)"
        ))
        #expect(inserted, "insertJob must succeed for the test premise to hold")
        let acquired = try await store.acquireLeaseWithJournal(
            jobId: jobId,
            episodeId: Self.episodeId,
            owner: "rt-owner",
            expiresAt: Date().timeIntervalSince1970 + 300
        )
        #expect(acquired, "lease acquire must succeed for the test premise to hold")
        return try await store.fetchJob(byId: jobId)?.generationID ?? ""
    }

    /// A runner whose transcript engine cannot produce coverage. `recognizer`
    /// decides HOW it fails: a throwing one reports `.ranToConclusion` (the
    /// silent/failed shape the field rows carry), a cancelling one reports
    /// `.interrupted` (a listener scrubbing).
    private func makeRunner(
        store: AnalysisStore,
        adStub: StubAdDetectionProvider,
        recognizer: SpeechRecognizer
    ) async throws -> AnalysisJobRunner {
        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = (0..<4).map {
            makeShard(id: $0, episodeID: Self.episodeId, startTime: Double($0) * 30, duration: 30)
        }
        let speechService = SpeechService(recognizer: recognizer)
        try await speechService.loadFastModel()
        if let mock = recognizer as? MockSpeechRecognizer {
            // Flip AFTER load so `loadModel` succeeds and only transcription fails.
            mock.shouldThrow = true
        }
        return AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: adStub
        )
    }

    private func journalStages(
        _ store: AnalysisStore,
        generationID: String
    ) async throws -> [(WorkJournalEntry.EventType, String)] {
        let entries = try await store.fetchWorkJournalEntries(
            episodeId: Self.episodeId,
            generationID: generationID
        )
        return entries.map { entry in
            let parsed = (try? JSONSerialization.jsonObject(with: Data(entry.metadata.utf8)))
                as? [String: Any]
            return (entry.eventType, (parsed?["stage"] as? String) ?? "")
        }
    }

    // MARK: - RT08: the defect

    /// The whole bead in one assertion: a fully transcribed asset whose pass
    /// adds no coverage must reach ad detection instead of terminating
    /// `transcription:*`.
    ///
    /// `backfillCallCount` is the load-bearing expectation. Asserting only on
    /// the stop reason would pass for an implementation that renames the
    /// failure — the point of the re-drive is the SCAN, and Stage 4 is where it
    /// happens.
    @Test("a pass that adds nothing to a complete transcript runs ad detection")
    func completeTranscriptReachesAdDetection() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: Self.durationSec, transcribedTo: Self.durationSec)
        let jobId = UUID().uuidString
        let generationID = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store, adStub: adStub, recognizer: MockSpeechRecognizer()
        )

        let outcome = await runner.run(try makeRequest(jobId: jobId))

        if case .failed(let msg) = outcome.stopReason {
            Issue.record("expected the pass to continue; got .failed(\(msg))")
        }
        #expect(adStub.backfillCallCount == 1,
                "the semantic scan is the only thing a re-drive exists to reach")
        // The coverage carried forward is the PERSISTED watermark — the same
        // quantity the `.completed` arm reports — so the scheduler's tier
        // arithmetic sees what is really on disk, not a zero.
        #expect(outcome.transcriptCoverageSec == Self.durationSec)

        // The drop leaves a durable, queryable trace, and it is NOT a failure
        // row: five `asr_failed` rows for a fully transcribed episode is the
        // misattribution this bead removes.
        let stages = try await journalStages(store, generationID: generationID)
        #expect(stages.contains {
            $0.0 == .checkpointed && $0.1 == "analysisJobRunner.run.transcriptionAlreadyComplete"
        }, "no short-circuit trace; got \(stages)")
        #expect(!stages.contains { $0.0 == .failed },
                "a pass that continued to ad detection must not journal a failure; got \(stages)")
    }

    // MARK: - RT09: the floor still bites

    /// Below the transcript floor NOTHING changes. The transcript genuinely is
    /// incomplete, it is still the transcript lane's work, and a silent engine
    /// must keep reading as a failure — otherwise this fix would convert every
    /// stalled transcription into a silent success and delete the
    /// `transcription:zeroCoverage` signal entirely.
    @Test("a half-transcribed asset still fails, and still journals the failure")
    func partialTranscriptStillFails() async throws {
        let store = try await makeTestStore()
        // 50 % transcribed, watermark honest about it.
        try await seedAsset(store, watermark: 60, transcribedTo: 60)
        let jobId = UUID().uuidString
        let generationID = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store, adStub: adStub, recognizer: MockSpeechRecognizer()
        )

        let outcome = await runner.run(try makeRequest(jobId: jobId))

        guard case .failed(let msg) = outcome.stopReason else {
            Issue.record("expected .failed(transcription:…), got \(outcome.stopReason)")
            return
        }
        #expect(msg.hasPrefix("transcription:"))
        #expect(adStub.backfillCallCount == 0)
        #expect(outcome.transcriptCoverageSec == 0)
        let stages = try await journalStages(store, generationID: generationID)
        #expect(stages.contains {
            $0.0 == .failed && $0.1 == "analysisJobRunner.run.transcriptionTimeout"
        }, "the failure accounting must be untouched below the floor; got \(stages)")
    }

    /// A WATERMARK ALONE IS NOT A LICENCE. The high-water reach can read 100 %
    /// over a transcript full of holes (the playhead-sd71 antipattern), so the
    /// gate divides an AREA. This fixture is the exact disagreement: watermark
    /// at the end of the episode, 25 % of it actually backed by chunks.
    @Test("a full watermark over a gappy transcript does not license the short-circuit")
    func fullWatermarkOverGappyTranscriptStillFails() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: Self.durationSec, transcribedTo: 30)
        let jobId = UUID().uuidString
        _ = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store, adStub: adStub, recognizer: MockSpeechRecognizer()
        )

        let outcome = await runner.run(try makeRequest(jobId: jobId))

        guard case .failed = outcome.stopReason else {
            Issue.record("a 25 %-backed transcript must not read as complete; got \(outcome.stopReason)")
            return
        }
        #expect(adStub.backfillCallCount == 0)
    }

    /// An asset with a watermark and NO chunks at all is the playhead-0sro crash
    /// shape — the watermark outliving the rows it claims. It must not license
    /// the short-circuit either, and it is a distinct fixture from the gappy one
    /// because it exercises the empty-ranges guard rather than the ratio.
    @Test("a watermark with no chunks behind it does not license the short-circuit")
    func watermarkWithoutChunksStillFails() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: Self.durationSec, transcribedTo: 0)
        let jobId = UUID().uuidString
        _ = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store, adStub: adStub, recognizer: MockSpeechRecognizer()
        )

        let outcome = await runner.run(try makeRequest(jobId: jobId))

        guard case .failed = outcome.stopReason else {
            Issue.record("a watermark with no chunks must not read as complete; got \(outcome.stopReason)")
            return
        }
        #expect(adStub.backfillCallCount == 0)
    }

    // MARK: - RT10: an interruption is excluded

    /// playhead-ngev made a listener's scrub terminate this observation
    /// instantly and hand back the scheduler's single running slot, at the
    /// deliberate cost of the successor-loop rescue. Continuing into ad
    /// detection on an interruption would hold that slot through a whole
    /// detection pass while the listener is moving the playhead — reintroducing
    /// the cost ngev paid to avoid.
    ///
    /// Nothing is stranded by refusing: `.interrupted` spends none of the job's
    /// five attempts, so the retry comes back.
    @Test("an interrupted pass does not short-circuit, even on a complete transcript")
    func interruptedPassDoesNotShortCircuit() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: Self.durationSec, transcribedTo: Self.durationSec)
        let jobId = UUID().uuidString
        _ = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store, adStub: adStub, recognizer: CancellingRecognizer()
        )

        let outcome = await runner.run(try makeRequest(jobId: jobId))

        if case .interrupted = outcome.stopReason {
            // The expected shape.
        } else {
            Issue.record("expected .interrupted, got \(outcome.stopReason)")
        }
        #expect(adStub.backfillCallCount == 0,
                "a scrub must not buy a full detection pass on the scheduler's only slot")
    }
}
