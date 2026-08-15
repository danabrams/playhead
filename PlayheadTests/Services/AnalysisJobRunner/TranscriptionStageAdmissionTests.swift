// TranscriptionStageAdmissionTests.swift
// playhead-pnb5: a pass whose watermark has already reached its own target must
// not run the transcription stage, whatever the EPISODE-level area floor says.
//
// THE LOOP, as measured by playhead-by07 and witnessed again on the 2026-08-15
// device pull. `desiredCoverageSec` is the shard filter in `AnalysisJobRunner`
// (`allShards.filter { $0.startTime < request.desiredCoverageSec }`). Once the
// transcript watermark has reached that target, every later pass at the same
// target hands the engine a shard set it has already read — and
// `TranscriptEngineService.runTranscriptionLoop` re-runs full ASR over all of
// it, because playhead-mptr ORDERS already-backed shards last rather than
// skipping them. Zero chunks, every time.
//
// The runner's only escape was `transcriptCoverageOfCompletedTranscript`, which
// gates on `SemanticScanClaim.transcriptClearsFinalizeFloor` — the gap-bridged
// AREA of the whole episode's transcript over its DECLARED DURATION, at 0.95.
// That is an EPISODE question being used to settle a PASS question, and it is
// this repo's standing defect class: a value that names one thing read as
// though it named another. by07 measured the discrimination as total, 14 of 14
// — an asset needed >= 0.95 episode coverage to be allowed to finish and
// escalate, and an asset below 0.95 was denied the short-circuit and sent back
// through a stage that could not add anything.
//
// THE THREE WITNESSES on Dan's phone, `db-pull11`, all three queued at priority
// 10/20 with `lastErrorCode = backgroundWindowExpired`:
//
//   3C2FFE10  asked 7,909.0 s  audio 7,999.0 s  watermark 7,920.0 s
//   CD2976E6  asked 1,556.0 s  audio 1,675.8 s  watermark 1,560.0 s
//   E51B25E4  asked 7,219.0 s  audio 7,325.9 s  watermark 7,230.0 s
//
// Their `work_journal` rows are the direct evidence for what this suite fixes:
// `analysisJobRunner.run.transcriptionStageNotRun`'s predecessor, the
// `transcriptionAlreadyComplete` row, carries `slice_duration_ms` of 31,368 /
// 39,342 / 157,645 / 208,572 / 215,980 / 232,346 — and every one of those is
// followed by `analysisWorkScheduler.taskExpiredRequeue` in the SAME SECOND.
// The stage was the window. Because `taskExpiredRequeue` writes no progress and
// mints no successor, no `.reachedTarget` outcome was ever reported, and
// `AnalysisWorkScheduler`'s `case .reachedTarget where tierTargetSatisfied(...)`
// — the only site that mints the tier successor — was never reached at all.
// That is why WIDENING THAT PREDICATE could not have fixed these rows and
// removing the empty stage does.

import Foundation
import os
import Testing
@testable import Playhead

/// A recognizer that COUNTS ASR attempts instead of failing them.
///
/// The does-it-run rail for this bead. Every other observable — the stop
/// reason, the coverage, the backfill count — is identical on a stage that ran
/// and produced nothing and a stage that was never started; the number of times
/// the engine was asked to transcribe a shard is the one quantity that
/// distinguishes them, so it is the one this suite asserts on.
final class TranscribeAttemptCountingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let attempts = OSAllocatedUnfairLock(initialState: 0)

    var transcribeAttempts: Int { attempts.withLock { $0 } }

    func loadModel() async throws {}
    func unloadModel() async {}
    func isModelLoaded() async -> Bool { true }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        attempts.withLock { $0 += 1 }
        return []
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] { [] }
}

@Suite("playhead-pnb5: the transcription stage is not run when nothing is in range")
struct TranscriptionStageAdmissionTests {

    private static let assetId = "pnb5-asset"
    private static let episodeId = "pnb5-ep"
    /// 4 shards x 30 s. The declared duration and the decoded span agree, so the
    /// episode-level floor is a clean read and the only thing under test is
    /// which SHARDS a request admits.
    private static let durationSec: Double = 120

    // MARK: - Fixture

    private func makeRequest(jobId: String, desiredCoverageSec: Double) throws -> AnalysisRangeRequest {
        let tmpDir = try makeTempDir(prefix: "TranscriptionStageAdmissionTests")
        let audioFile = tmpDir.appendingPathComponent("episode.m4a")
        FileManager.default.createFile(atPath: audioFile.path, contents: Data())
        let localURL = try #require(LocalAudioURL(audioFile))
        return AnalysisRangeRequest(
            jobId: jobId,
            episodeId: Self.episodeId,
            podcastId: "pnb5-pod",
            analysisAssetId: Self.assetId,
            audioURL: localURL,
            desiredCoverageSec: desiredCoverageSec,
            mode: .preRollWarmup,
            outputPolicy: .writeWindowsAndCues,
            priority: .medium
        )
    }

    /// Seed the asset with a watermark and a set of transcribed spans.
    ///
    /// `transcribedSpans` rather than a single `transcribedTo` because both
    /// halves of the artifact rule need exercising independently: condition 1 is
    /// the watermark, condition 2 is whether a persisted chunk actually overlaps
    /// the shard, and a fixture that can only express a contiguous prefix cannot
    /// separate them.
    private func seedAsset(
        _ store: AnalysisStore,
        watermark: Double?,
        transcribedSpans: [(start: Double, end: Double)]
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: Self.assetId,
            episodeId: Self.episodeId,
            assetFingerprint: "pnb5-fp",
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
        var chunks: [TranscriptChunk] = []
        var index = 0
        for span in transcribedSpans {
            var start = span.start
            while start < span.end {
                let end = min(start + 10, span.end)
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
        }
        guard !chunks.isEmpty else { return }
        try await store.insertTranscriptChunks(chunks)
    }

    private func seedLeasedJob(_ store: AnalysisStore, jobId: String) async throws -> String {
        let inserted = try await store.insertJob(makeAnalysisJob(
            jobId: jobId,
            episodeId: Self.episodeId,
            analysisAssetId: Self.assetId,
            workKey: "pnb5-wk-\(UUID().uuidString)"
        ))
        #expect(inserted, "insertJob must succeed for the test premise to hold")
        let acquired = try await store.acquireLeaseWithJournal(
            jobId: jobId,
            episodeId: Self.episodeId,
            owner: "pnb5-owner",
            expiresAt: Date().timeIntervalSince1970 + 300
        )
        #expect(acquired, "lease acquire must succeed for the test premise to hold")
        return try await store.fetchJob(byId: jobId)?.generationID ?? ""
    }

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

    // MARK: - PB01: the wedge

    /// THE WHOLE BEAD IN ONE FIXTURE. A 90-second-style rung on an episode the
    /// area floor refuses: the request asked for 30 s, the watermark has reached
    /// 30 s, and the episode as a whole is 25 % transcribed — comfortably under
    /// the 0.95 finalize floor, which is asserted here rather than assumed.
    ///
    /// Before this bead the pass ran the transcription stage over one shard it
    /// had already read, produced nothing, was refused the short-circuit BY THE
    /// EPISODE FLOOR, and terminated `.failed("transcription:…")` — from which no
    /// tier successor can ever be minted, so the target never deepened and the
    /// same pass ran again forever.
    @Test("a pass whose watermark reached its own target skips the stage, even under the 0.95 floor")
    func targetAlreadyReachedSkipsTheStageBelowTheEpisodeFloor() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: 30, transcribedSpans: [(0, 30)])
        let jobId = UUID().uuidString
        let generationID = try await seedLeasedJob(store, jobId: jobId)

        // THE PREMISE, PINNED: the episode-level floor refuses this asset. If a
        // future change made it clear the floor, this test would pass for the
        // wrong reason — it would be exercising playhead-9y9e's branch instead.
        let region = try await store.fetchTranscribedRegion(assetId: Self.assetId)
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(region: region),
            episodeDurationSec: EpisodeSeconds(Self.durationSec)
        ) == false, "the fixture must sit BELOW the finalize floor for this to be pnb5's case")

        let adStub = StubAdDetectionProvider()
        let recognizer = TranscribeAttemptCountingRecognizer()
        let runner = try await makeRunner(store: store, adStub: adStub, recognizer: recognizer)

        let outcome = await runner.run(try makeRequest(jobId: jobId, desiredCoverageSec: 30))

        // The engine was never asked. This is the assertion the bead is about:
        // every other observable below is equally true of a stage that ran and
        // produced nothing.
        #expect(recognizer.transcribeAttempts == 0,
                "the stage must not be started when every admitted shard is already backed")

        guard case .reachedTarget = outcome.stopReason else {
            Issue.record("expected .reachedTarget so the scheduler can mint the successor; got \(outcome.stopReason)")
            return
        }
        // The coverage carried forward is the PERSISTED watermark — the quantity
        // `tierTargetSatisfied` compares against `desiredCoverageSec`.
        #expect(outcome.transcriptCoverageSec == 30)
        #expect(adStub.backfillCallCount == 1,
                "the pass must reach the semantic scan, which is the work it exists to do")

        let stages = try await journalStages(store, generationID: generationID)
        #expect(stages.contains {
            $0.0 == .checkpointed && $0.1 == "analysisJobRunner.run.transcriptionStageNotRun"
        }, "no not-run trace; got \(stages)")
        #expect(!stages.contains { $0.0 == .failed },
                "a pass with nothing to transcribe did not fail; got \(stages)")
        #expect(!stages.contains { $0.1 == "analysisJobRunner.run.transcriptionAlreadyComplete" },
                "a stage that was never started must not be counted as one that paid its cap")
    }

    // MARK: - PB02: the does-it-run direction

    /// THE VACUITY CONTROL. Everything above would also be true of a runner that
    /// had stopped transcribing entirely. Move the target one shard deeper than
    /// the watermark and the stage must run — over that shard, and over the
    /// already-backed one too, because playhead-mptr orders rather than skips.
    @Test("one admitted shard past the watermark still runs the stage")
    func oneUnbackedAdmittedShardStillRunsTheStage() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: 30, transcribedSpans: [(0, 30)])
        let jobId = UUID().uuidString
        let generationID = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let recognizer = TranscribeAttemptCountingRecognizer()
        let runner = try await makeRunner(store: store, adStub: adStub, recognizer: recognizer)

        _ = await runner.run(try makeRequest(jobId: jobId, desiredCoverageSec: 60))

        #expect(recognizer.transcribeAttempts > 0,
                "audio nothing backs yet must still reach the engine")
        let stages = try await journalStages(store, generationID: generationID)
        #expect(!stages.contains { $0.1 == "analysisJobRunner.run.transcriptionStageNotRun" },
                "the skip must not fire while a shard is unbacked; got \(stages)")
    }

    // MARK: - PB03/PB04: both halves of the artifact rule

    /// CONDITION 1, the watermark. A chunk DOES overlap the shard, but the
    /// durable watermark stops halfway through it — the playhead-rfu-aac H3
    /// shape, where a high-water reach is not a promise the audio was read. The
    /// stage must run.
    ///
    /// The fixture stops the CHUNKS at 15 s as well, and that is forced rather
    /// than chosen: `run()` opens with
    /// `AnalysisStore.reconcileFastTranscriptCoverage`, which raises the
    /// watermark to `MAX(endTime)` over the fast chunks. A watermark BEHIND its
    /// own chunks is therefore not a state this code path can be in, so a
    /// fixture that seeded one would be testing the reconcile, not the
    /// admission.
    @Test("a watermark short of the shard's end still runs the stage")
    func watermarkShortOfTheShardEndStillRunsTheStage() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: 15, transcribedSpans: [(0, 15)])
        let jobId = UUID().uuidString
        _ = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let recognizer = TranscribeAttemptCountingRecognizer()
        let runner = try await makeRunner(store: store, adStub: adStub, recognizer: recognizer)

        _ = await runner.run(try makeRequest(jobId: jobId, desiredCoverageSec: 30))

        #expect(recognizer.transcribeAttempts > 0,
                "a watermark inside the shard is not evidence the shard was read")
    }

    /// CONDITION 2, the artifact. The watermark spans the episode but a whole
    /// shard has no chunk behind it — the playhead-0sro shape, a watermark
    /// outliving its rows. The stage must run.
    @Test("a hole wider than a shard still runs the stage")
    func holeWiderThanAShardStillRunsTheStage() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store,
            watermark: Self.durationSec,
            transcribedSpans: [(0, 30), (90, Self.durationSec)]
        )
        let jobId = UUID().uuidString
        _ = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let recognizer = TranscribeAttemptCountingRecognizer()
        let runner = try await makeRunner(store: store, adStub: adStub, recognizer: recognizer)

        _ = await runner.run(try makeRequest(jobId: jobId, desiredCoverageSec: Self.durationSec))

        #expect(recognizer.transcribeAttempts > 0,
                "30-90 s is backed by nothing and must reach the engine")
    }

    /// A NULL watermark over a complete transcript still skips the stage, and
    /// the reason is worth pinning rather than reasoning about.
    ///
    /// The obvious expectation is the opposite — the helper refuses a missing
    /// watermark, so surely the stage runs. It does not, because `run()` opens
    /// with `AnalysisStore.reconcileFastTranscriptCoverage(id:)`
    /// (playhead-0sro), which RAISES the column to `MAX(endTime)` over the
    /// asset's fast chunks before anything else happens. By the time the
    /// admission reads it, a NULL watermark on an asset that has chunks has
    /// become the chunks' own reach.
    ///
    /// So the helper's watermark guard is defensive against a state THIS caller
    /// cannot produce, and the two guards are not the independent pair they
    /// look like. That is a fact about the call path, and a reader who assumed
    /// otherwise would design exactly the fixture this test replaces. Move the
    /// reconcile and this test changes, which is the point.
    @Test("a NULL watermark is reconciled from the chunks before the admission reads it")
    func nilWatermarkIsReconciledFromChunksBeforeAdmission() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: nil, transcribedSpans: [(0, Self.durationSec)])
        let jobId = UUID().uuidString
        _ = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let recognizer = TranscribeAttemptCountingRecognizer()
        let runner = try await makeRunner(store: store, adStub: adStub, recognizer: recognizer)

        _ = await runner.run(try makeRequest(jobId: jobId, desiredCoverageSec: Self.durationSec))

        #expect(recognizer.transcribeAttempts == 0,
                "the reconcile raised the watermark to 120 s, so every admitted shard is backed")
        let reconciled = try await store.fetchFastTranscriptCoverageEndTime(id: Self.assetId)
        #expect(reconciled == Self.durationSec,
                "the premise: the reconcile, not the seed, is what the admission read")
    }

    /// A watermark with no chunks behind it — the other unreadable input.
    @Test("a watermark with no chunks at all still runs the stage")
    func watermarkWithoutChunksStillRunsTheStage() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, watermark: Self.durationSec, transcribedSpans: [])
        let jobId = UUID().uuidString
        _ = try await seedLeasedJob(store, jobId: jobId)
        let adStub = StubAdDetectionProvider()
        let recognizer = TranscribeAttemptCountingRecognizer()
        let runner = try await makeRunner(store: store, adStub: adStub, recognizer: recognizer)

        _ = await runner.run(try makeRequest(jobId: jobId, desiredCoverageSec: Self.durationSec))

        #expect(recognizer.transcribeAttempts > 0)
    }
}

/// The escalation the skip unblocks, computed on the three field witnesses'
/// real numbers.
///
/// `AnalysisJobRunner` returning `.reachedTarget` with the persisted watermark
/// is only half the chain; `AnalysisWorkScheduler` still has to read that
/// outcome as a satisfied tier and find a deeper rung. Both are static, so both
/// can be asked directly rather than inferred.
@Suite("playhead-pnb5: the three wedged rows escalate once the pass can finish")
struct WedgedRowEscalationTests {

    /// `(asset, asked, decoded audio, watermark)` from `db-pull11`, 2026-08-15.
    /// Every one of these three sat `queued` at priority 10/20 with
    /// `lastErrorCode = backgroundWindowExpired`, its `analysis_jobs`
    /// `transcriptCoverageSec` still 0 because `taskExpiredRequeue` writes no
    /// progress.
    static let witnesses: [(asset: String, asked: Double, audio: Double, watermark: Double)] = [
        ("3C2FFE10", 7_909.0, 7_998.9551020408162, 7_920.0),
        ("CD2976E6", 1_556.0, 1_675.7812244897959, 1_560.0),
        ("E51B25E4", 7_219.0, 7_325.9243125, 7_230.0),
    ]

    private func job(asked: Double) -> AnalysisJob {
        makeAnalysisJob(
            jobId: "wedged-\(asked)",
            episodeId: "wedged-ep",
            analysisAssetId: "wedged-asset",
            workKey: "wedged-wk-\(asked)",
            desiredCoverageSec: asked,
            featureCoverageSec: 0,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0
        )
    }

    private func outcome(watermark: Double, asked: Double) -> AnalysisOutcome {
        AnalysisOutcome(
            assetId: "wedged-asset",
            requestedCoverageSec: asked,
            featureCoverageSec: watermark,
            transcriptCoverageSec: watermark,
            cueCoverageSec: 0,
            newCueCount: 0,
            stopReason: .reachedTarget
        )
    }

    /// BEFORE: the pass reported nothing at all — it was cancelled mid-stage and
    /// accounted by `taskExpiredRequeue`, upstream of the outcome switch, so
    /// `tierTargetSatisfied` was never consulted. AFTER: it reports the
    /// watermark, and the tier is satisfied.
    @Test("each wedged row's watermark satisfies the tier it was stuck on")
    func watermarkSatisfiesTheStuckTier() {
        for witness in Self.witnesses {
            #expect(
                AnalysisWorkScheduler.tierTargetSatisfied(
                    job: job(asked: witness.asked),
                    outcome: outcome(watermark: witness.watermark, asked: witness.asked)
                ),
                "\(witness.asset): watermark \(witness.watermark) must satisfy target \(witness.asked)"
            )
            // And the direction that must NOT hold: a pass reporting zero — what
            // every one of these rows actually recorded — deepens nothing.
            #expect(
                AnalysisWorkScheduler.tierTargetSatisfied(
                    job: job(asked: witness.asked),
                    outcome: AnalysisOutcome(
                        assetId: "wedged-asset",
                        requestedCoverageSec: witness.asked,
                        featureCoverageSec: 0,
                        transcriptCoverageSec: 0,
                        cueCoverageSec: 0,
                        newCueCount: 0,
                        stopReason: .reachedTarget
                    )
                ) == false,
                "\(witness.asset): a zero-coverage pass must never deepen the target"
            )
        }
    }

    /// The successor rung is the DECODED AUDIO, which is the whole point:
    /// playhead-rh69 made a full-coverage request resolve against the file
    /// rather than the feed's declaration, and this is the pass that finally
    /// gets to ask for it. The seconds recovered are the tail the feed's
    /// `<itunes:duration>` cut off.
    @Test("the successor rung is the decoded audio, recovering the unseen tail")
    func successorRungIsTheDecodedAudio() {
        let tiers = [90.0, 300.0, 900.0]
        for witness in Self.witnesses {
            let cleared = max(witness.asked, witness.watermark)
            let next = AnalysisWorkScheduler.nextTierCoverage(
                current: cleared,
                tiers: tiers,
                episodeDurationSec: witness.audio
            )
            #expect(next == witness.audio,
                    "\(witness.asset): expected the duration rung \(witness.audio), got \(String(describing: next))")
            let recovered = (next ?? cleared) - cleared
            #expect(recovered > 0,
                    "\(witness.asset): the successor must reach past what this pass read")
        }
    }
}
