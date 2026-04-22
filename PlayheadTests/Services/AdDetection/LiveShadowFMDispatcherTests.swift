// LiveShadowFMDispatcherTests.swift
// playhead-narl.2 continuation: focused coverage for the live shadow FM
// dispatcher's wire format and error-classification logic. Swift Testing
// suite.
//
// Coverage:
//   - Happy path: dispatcher fetches overlapping transcript chunks, joins
//     them newline-separated, calls `respondRefinement`, serializes the
//     result into a `ShadowFMPayload` with `errorTag == nil`, returns
//     bytes + modelVersion.
//   - Failure paths produce tagged payloads:
//       * error description contains "refusal"   → tag = "refusal"
//       * error description contains "decoding"  → tag = "decodingFailure"
//       * error description contains "context"   → tag = "exceededContextWindow"
//       * error description contains "unavailable" → tag = "runtimeUnavailable"
//       * any other error                         → tag = "other"
//
// Uses a real `AnalysisStore` so we exercise the live
// `fetchTranscriptChunks(assetId:)` path, and a stub
// `FoundationModelClassifier.Runtime` so the FM call is deterministic.

import Foundation
import Testing

@testable import Playhead

@Suite("LiveShadowFMDispatcher (playhead-narl.2)")
struct LiveShadowFMDispatcherTests {

    // MARK: - Happy path

    @Test("dispatchShadowCall serializes prompt + refinement response")
    func dispatchShadowCallHappyPath() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, id: "asset-fm")
        // Two chunks overlapping window [0, 30].
        try await seedTranscriptChunk(
            store: store, assetId: "asset-fm",
            startTime: 0, endTime: 15, chunkIndex: 0, ordinal: 0,
            text: "hello world"
        )
        try await seedTranscriptChunk(
            store: store, assetId: "asset-fm",
            startTime: 15, endTime: 30, chunkIndex: 1, ordinal: 1,
            text: "second line"
        )
        // A third chunk that starts past the window — should be excluded.
        try await seedTranscriptChunk(
            store: store, assetId: "asset-fm",
            startTime: 30, endTime: 45, chunkIndex: 2, ordinal: 2,
            text: "should-not-appear"
        )

        let runtime = makeStubRuntime(
            respondRefinement: { _ in
                RefinementWindowSchema(spans: [])
            }
        )
        let dispatcher = LiveShadowFMDispatcher(
            store: store,
            runtime: runtime,
            modelVersion: "test-fm.v1"
        )

        let window = ShadowWindow(start: 0, end: 30)
        let result = try await dispatcher.dispatchShadowCall(
            assetId: "asset-fm",
            window: window,
            configVariant: .allEnabledShadow
        )
        #expect(result.fmModelVersion == "test-fm.v1")
        let decoded = try JSONDecoder().decode(
            ShadowFMPayload.self, from: result.fmResponse
        )
        #expect(decoded.payloadSchemaVersion == shadowFMPayloadSchemaVersion)
        #expect(decoded.errorTag == nil)
        #expect(decoded.refinementResponse?.spans.isEmpty == true)
        // Overlapping chunks concatenated with newline separator; third
        // chunk excluded because it starts at 30 (NOT < window.end = 30).
        #expect(decoded.promptText == "hello world\nsecond line")
    }

    // MARK: - Error-tag classification

    @Test("refusal error → tag = 'refusal'")
    func errorTagRefusal() async throws {
        try await assertErrorTag(
            errorDescription: "FMRefusalError: blocked content",
            expectedTag: "refusal"
        )
    }

    @Test("decoding error → tag = 'decodingFailure'")
    func errorTagDecoding() async throws {
        try await assertErrorTag(
            errorDescription: "decoding failure at span",
            expectedTag: "decodingFailure"
        )
    }

    @Test("context-window error → tag = 'exceededContextWindow'")
    func errorTagContext() async throws {
        try await assertErrorTag(
            errorDescription: "prompt exceeded context limit",
            expectedTag: "exceededContextWindow"
        )
    }

    @Test("unavailable error → tag = 'runtimeUnavailable'")
    func errorTagUnavailable() async throws {
        try await assertErrorTag(
            errorDescription: "runtime unavailable: model not loaded",
            expectedTag: "runtimeUnavailable"
        )
    }

    @Test("otherwise-unrecognized error → tag = 'other'")
    func errorTagOther() async throws {
        try await assertErrorTag(
            errorDescription: "catastrophic xyz",
            expectedTag: "other"
        )
    }

    // MARK: - Helpers

    /// Run one dispatcher call whose refinement closure throws an error
    /// whose `String(describing:)` matches `errorDescription`, and assert
    /// the resulting payload's `errorTag` equals `expectedTag`.
    private func assertErrorTag(
        errorDescription: String,
        expectedTag: String
    ) async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, id: "asset-err")
        try await seedTranscriptChunk(
            store: store, assetId: "asset-err",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0,
            text: "some text"
        )

        let runtime = makeStubRuntime(
            respondRefinement: { _ in
                throw StubFMError(message: errorDescription)
            }
        )
        let dispatcher = LiveShadowFMDispatcher(store: store, runtime: runtime)
        let result = try await dispatcher.dispatchShadowCall(
            assetId: "asset-err",
            window: ShadowWindow(start: 0, end: 30),
            configVariant: .allEnabledShadow
        )
        let decoded = try JSONDecoder().decode(
            ShadowFMPayload.self, from: result.fmResponse
        )
        #expect(decoded.errorTag == expectedTag)
        #expect(decoded.refinementResponse == nil)
        #expect(decoded.promptText == "some text")
    }
}

// MARK: - Test helpers (file-scoped)

/// Error whose `String(describing:)` deterministically contains the
/// given `message` — used to drive `errorTag(for:)`'s substring matcher.
private struct StubFMError: Error, CustomStringConvertible {
    let message: String
    var description: String { message }
}

/// Build a `FoundationModelClassifier.Runtime` whose refinement closure is
/// supplied by the caller. All other legs return harmless defaults.
private func makeStubRuntime(
    respondRefinement: @escaping @Sendable (_ prompt: String) async throws -> RefinementWindowSchema
) -> FoundationModelClassifier.Runtime {
    FoundationModelClassifier.Runtime(
        availabilityStatus: { _ in nil },
        contextSize: { 4_096 },
        tokenCount: { prompt in
            max(1, prompt.split(whereSeparator: \.isWhitespace).count)
        },
        coarseSchemaTokenCount: { 16 },
        refinementSchemaTokenCount: { 32 },
        boundarySchemaTokenCount: { 32 },
        makeSession: {
            FoundationModelClassifier.Runtime.Session(
                prewarm: { _ in },
                respondCoarse: { _ in
                    CoarseScreeningSchema(disposition: .noAds, support: nil)
                },
                respondRefinement: respondRefinement
            )
        }
    )
}

private func seedAsset(
    store: AnalysisStore,
    id: String
) async throws {
    let asset = AnalysisAsset(
        id: id,
        episodeId: "episode-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///tmp/\(id).mp3",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
    try await store.insertAsset(asset)
}

private func seedTranscriptChunk(
    store: AnalysisStore,
    assetId: String,
    startTime: TimeInterval,
    endTime: TimeInterval,
    chunkIndex: Int,
    ordinal: Int,
    text: String
) async throws {
    let chunk = TranscriptChunk(
        id: "chunk-\(assetId)-\(chunkIndex)",
        analysisAssetId: assetId,
        segmentFingerprint: "seg-\(assetId)-\(chunkIndex)",
        chunkIndex: chunkIndex,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: text,
        pass: "fast",
        modelVersion: "test.v1",
        transcriptVersion: nil,
        atomOrdinal: ordinal,
        weakAnchorMetadata: nil,
        speakerId: nil
    )
    try await store.insertTranscriptChunk(chunk)
}
