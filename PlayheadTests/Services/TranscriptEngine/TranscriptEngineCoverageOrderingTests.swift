// TranscriptEngineCoverageOrderingTests.swift
// playhead-6r4z — the WIRING rail: what `runTranscriptionLoop` actually hands
// its coverage index, observed as the order the recognizer is called in.
//
// `TranscriptCoverageIndexTests` proves the index classifies final-pass audio as
// backed. That is a property of a value type and it is silent about the one line
// that decides whether the fix reaches the device: whether the loop reads
// `fetchTranscribedRegion` (both passes) or `fetchFastTranscriptCoveredRanges`
// (the fast pass alone, which is what shipped). Rail TY33 in
// `scripts/mutation-battery-untypeable.py` makes the narrow read fail to
// COMPILE at that line; this suite is the behavioural half, because a type rail
// cannot see a third spelling that type-checks.
//
// The observable is `SpeechRecognizer.transcribe(shard:podcastId:)` — the ASR
// call itself, which is the resource the flat 300 s stage cap is spent on.

import Foundation
import os
import Testing
@testable import Playhead

/// Records the order shards reach ASR, and returns one segment per shard so the
/// loop persists something and reports `.completed` rather than `.failed`.
private final class OrderTrackingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private var loaded = false
    private let ids = OSAllocatedUnfairLock(initialState: [Int]())
    var transcribedShardIds: [Int] { ids.withLock { $0 } }

    func loadModel() async throws { loaded = true }
    func unloadModel() async { loaded = false }
    func isModelLoaded() async -> Bool { loaded }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard loaded else { throw TranscriptEngineError.modelNotLoaded }
        ids.withLock { $0.append(shard.id) }
        let word = TranscriptWord(
            text: "s\(shard.id)",
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            confidence: 0.9
        )
        return [TranscriptSegment(
            id: shard.id,
            words: [word],
            text: "s\(shard.id)",
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            avgConfidence: 0.9,
            passType: .fast
        )]
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

@Suite("playhead-6r4z: the transcription loop orders on BOTH passes")
struct TranscriptEngineCoverageOrderingTests {

    private func makeAsset(id: String, watermark: Double?) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: watermark,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeFinalChunk(assetId: String, start: Double, end: Double) -> TranscriptChunk {
        TranscriptChunk(
            id: "final-\(assetId)-\(Int(start))",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-final-\(assetId)-\(Int(start))",
            chunkIndex: Int(start),
            startTime: start,
            endTime: end,
            text: "final",
            normalizedText: "final",
            pass: "final",
            modelVersion: "v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// Drive one loop to `.completed` and report the ASR call order.
    private func transcriptionOrder(
        assetId: String,
        seedFinalPassChunk: Bool
    ) async throws -> [Int] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId, watermark: 180))
        if seedFinalPassChunk {
            _ = try await store.insertTranscriptChunks([
                makeFinalChunk(assetId: assetId, start: 0, end: 180)
            ])
        }

        let recognizer = OrderTrackingRecognizer()
        let speech = SpeechService(recognizer: recognizer, serializesRecognizerRequests: false)
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)

        // 8 shards of 30 s. The watermark is 180, so shards 0-5 are below it and
        // shards 6-7 are the audio nothing has reached.
        let shards = (0..<8).map {
            makeShard(id: $0, episodeID: "ep-\(assetId)", startTime: Double($0) * 30, duration: 30)
        }

        let events = await engine.events()
        await engine.startTranscription(
            shards: shards,
            analysisAssetId: assetId,
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: assetId)
        for await event in events {
            switch event {
            case .completed: return recognizer.transcribedShardIds
            case let .failed(_, reason):
                Issue.record("the loop failed rather than completing: \(reason)")
                return recognizer.transcribedShardIds
            case .chunksPersisted: continue
            }
        }
        return recognizer.transcribedShardIds
    }

    /// THE RAIL. Nothing has ever run the fast pass on this asset — there is not
    /// one `pass = 'fast'` row — but a FINAL pass covers `[0, 180)` and the
    /// watermark says 180. Read `pass = 'fast'` alone (what shipped), shards 0-5
    /// have no artifact, sort UNCOVERED, and are decoded first: 180 s of ASR
    /// bought again ahead of the 60 s nobody has read. Read across both passes,
    /// the unread tail leads.
    ///
    /// The CONTROL is the same fixture with the final-pass row removed, where
    /// shards 0-5 genuinely are unread and must lead — so this cannot pass by an
    /// index that classifies everything as covered, or by an ordering that never
    /// moved.
    @Test("final-pass-covered shards are decoded LAST, and without them they are decoded FIRST",
          .timeLimit(.minutes(1)))
    func finalPassCoveredPrefixIsDecodedLast() async throws {
        let withFinalPass = try await transcriptionOrder(
            assetId: "a-6r4z-engine", seedFinalPassChunk: true
        )
        #expect(withFinalPass.count == 8)
        #expect(withFinalPass.prefix(2) == [6, 7],
                "the audio nothing backs must reach ASR before the audio a final-pass row backs")
        #expect(Set(withFinalPass) == Set(0..<8), "this is an ordering — no shard is dropped")

        let control = try await transcriptionOrder(
            assetId: "a-6r4z-engine-control", seedFinalPassChunk: false
        )
        #expect(control.count == 8)
        #expect(control.prefix(2) == [0, 1],
                "with no artifact at all, the watermark alone must not re-order anything")
    }
}
