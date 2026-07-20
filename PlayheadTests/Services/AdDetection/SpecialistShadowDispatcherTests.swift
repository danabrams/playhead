// SpecialistShadowDispatcherTests.swift
// playhead-dsbc (Phase B1): focused coverage for the model-agnostic
// specialist-shadow plumbing. Swift Testing suite.
//
// Phase B1 builds ONLY the shadow seam — a classifier that runs alongside
// FM, logs a verdict, and ACTS ON NOTHING. No real model is wired here (that
// is phone-gated Phase B2). These tests pin the invariants that keep the
// plumbing inert:
//
//   - flag-OFF / default-inert: `specialistShadowEnabled` defaults `false`
//     in both `AdDetectionConfig.default` and the memberwise init, and the
//     `PlayheadRuntime` injection seam defaults `nil`. The decision-path
//     assertion is DEFERRED (documented below): B1 does NOT wire the
//     dispatcher into `AdDetectionService`, so there is no detection path to
//     A/B; the augment-is-shadow invariant is proven at the dispatcher level
//     instead (the dispatcher has no store-write seam — its only side effect
//     is the injected `record` sink).
//   - augment-is-shadow: a stub `Runtime` returning a fixed verdict causes
//     the in-memory sink to capture that verdict, while the transcript/asset
//     store rows are byte-identical before and after (read-only dispatch).
//   - throw ⇒ errorTag: a `classify` that throws records a payload with a
//     non-nil `errorTag`, `verdict == nil`, and still no store mutation.
//   - nil runtime + flag-ON ⇒ inert: no crash, no rows recorded, returns nil.
//   - payload round-trips through JSONEncoder/JSONDecoder.
//
// Uses a real `AnalysisStore` so the live `fetchTranscriptChunks(assetId:)`
// prompt-assembly path is exercised, and a stub `SpecialistAdClassifier.Runtime`
// so the classify call is deterministic. Mirrors `LiveShadowFMDispatcherTests`.

import Foundation
import Testing

@testable import Playhead

@Suite("LiveSpecialistShadowDispatcher (playhead-dsbc)")
struct SpecialistShadowDispatcherTests {

    // MARK: - Default-inert / flag-OFF invariant

    @Test("specialistShadowEnabled defaults false (static default + init)")
    func specialistShadowFlagDefaultsFalse() {
        // Static production default is OFF.
        #expect(AdDetectionConfig.default.specialistShadowEnabled == false)
        // Memberwise init default is OFF (param omitted below).
        let cfg = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1"
        )
        #expect(cfg.specialistShadowEnabled == false)
    }

    @Test("nil runtime + dispatch ⇒ inert (no record, returns nil)")
    func nilRuntimeIsInert() async throws {
        let store = try await makeTestStore()
        try await seedSpecialistAsset(store: store, id: "asset-nil")
        try await seedSpecialistChunk(
            store: store, assetId: "asset-nil",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0,
            text: "some text"
        )

        let recorder = SpecialistPayloadRecorder()
        // No runtime supplied — the B1 default until B2 wires the live model.
        let dispatcher = LiveSpecialistShadowDispatcher(
            store: store,
            runtime: nil,
            record: recorder.sink
        )

        let result = try await dispatcher.dispatchShadowCall(
            assetId: "asset-nil",
            window: ShadowWindow(start: 0, end: 30),
            configVariant: .allEnabledShadow
        )
        #expect(result == nil)
        #expect(recorder.payloads.isEmpty)
    }

    // MARK: - Augment-is-shadow: records verdict, decisions unchanged

    @Test("stub verdict recorded; store untouched (augment-is-shadow)")
    func recordsVerdictWithoutMutatingDecisions() async throws {
        let store = try await makeTestStore()
        try await seedSpecialistAsset(store: store, id: "asset-ad")
        // Two chunks overlapping window [0, 30]; a third starting at 30 is
        // excluded (start NOT < window.end).
        try await seedSpecialistChunk(
            store: store, assetId: "asset-ad",
            startTime: 0, endTime: 15, chunkIndex: 0, ordinal: 0,
            text: "buy now"
        )
        try await seedSpecialistChunk(
            store: store, assetId: "asset-ad",
            startTime: 15, endTime: 30, chunkIndex: 1, ordinal: 1,
            text: "limited offer"
        )
        try await seedSpecialistChunk(
            store: store, assetId: "asset-ad",
            startTime: 30, endTime: 45, chunkIndex: 2, ordinal: 2,
            text: "should-not-appear"
        )

        let chunkCountBefore = try await store.fetchTranscriptChunks(assetId: "asset-ad").count
        let assetBefore = try #require(try await store.fetchAsset(id: "asset-ad"))

        let fixed = SpecialistVerdict(isAd: true, confidence: 0.9, adClass: "dai")
        let recorder = SpecialistPayloadRecorder()
        let runtime = makeStubSpecialistRuntime(classify: { _ in fixed })
        let dispatcher = LiveSpecialistShadowDispatcher(
            store: store,
            runtime: runtime,
            record: recorder.sink
        )

        let result = try await dispatcher.dispatchShadowCall(
            assetId: "asset-ad",
            window: ShadowWindow(start: 0, end: 30),
            configVariant: .allEnabledShadow
        )

        // The verdict is recorded verbatim through the sink...
        #expect(recorder.payloads.count == 1)
        let recorded = try #require(recorder.payloads.first)
        #expect(recorded.verdict == fixed)
        #expect(recorded.errorTag == nil)
        #expect(recorded.promptText == "buy now\nlimited offer")
        #expect(recorded.payloadSchemaVersion == specialistShadowPayloadSchemaVersion)
        // ...and the dispatcher returns the same payload it recorded.
        #expect(result == recorded)

        // Augment-is-shadow: dispatch is read-only — the transcript rows and
        // the asset's mutable coverage/state fields are unchanged. The
        // dispatcher owns no store-write seam, so no ad window / decision can
        // move.
        let chunkCountAfter = try await store.fetchTranscriptChunks(assetId: "asset-ad").count
        let assetAfter = try #require(try await store.fetchAsset(id: "asset-ad"))
        #expect(chunkCountAfter == chunkCountBefore)
        #expect(assetAfter.analysisState == assetBefore.analysisState)
        #expect(assetAfter.analysisVersion == assetBefore.analysisVersion)
        #expect(assetAfter.featureCoverageEndTime == assetBefore.featureCoverageEndTime)
        #expect(assetAfter.fastTranscriptCoverageEndTime == assetBefore.fastTranscriptCoverageEndTime)
        #expect(assetAfter.confirmedAdCoverageEndTime == assetBefore.confirmedAdCoverageEndTime)
    }

    // MARK: - Error-tag classification

    @Test("classify throws ⇒ errorTag recorded, verdict nil, no mutation")
    func throwRecordsErrorTag() async throws {
        let store = try await makeTestStore()
        try await seedSpecialistAsset(store: store, id: "asset-err")
        try await seedSpecialistChunk(
            store: store, assetId: "asset-err",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0,
            text: "some text"
        )
        let assetBefore = try #require(try await store.fetchAsset(id: "asset-err"))

        let recorder = SpecialistPayloadRecorder()
        let runtime = makeStubSpecialistRuntime(classify: { _ in
            throw StubSpecialistError(message: "model unavailable: not loaded")
        })
        let dispatcher = LiveSpecialistShadowDispatcher(
            store: store,
            runtime: runtime,
            record: recorder.sink
        )

        let result = try await dispatcher.dispatchShadowCall(
            assetId: "asset-err",
            window: ShadowWindow(start: 0, end: 30),
            configVariant: .allEnabledShadow
        )
        let recorded = try #require(recorder.payloads.first)
        #expect(recorded.errorTag == "runtimeUnavailable")
        #expect(recorded.verdict == nil)
        #expect(recorded.promptText == "some text")
        #expect(result == recorded)
        // Still acts on nothing.
        let assetAfter = try #require(try await store.fetchAsset(id: "asset-err"))
        #expect(assetAfter.analysisState == assetBefore.analysisState)
        #expect(assetAfter.analysisVersion == assetBefore.analysisVersion)
        #expect(assetAfter.confirmedAdCoverageEndTime == assetBefore.confirmedAdCoverageEndTime)
    }

    @Test("CancellationError ⇒ tag = 'cancelled' via typed path")
    func cancellationErrorTag() async throws {
        let store = try await makeTestStore()
        try await seedSpecialistAsset(store: store, id: "asset-cancel")
        try await seedSpecialistChunk(
            store: store, assetId: "asset-cancel",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0,
            text: "some text"
        )
        let recorder = SpecialistPayloadRecorder()
        let runtime = makeStubSpecialistRuntime(classify: { _ in throw CancellationError() })
        let dispatcher = LiveSpecialistShadowDispatcher(
            store: store,
            runtime: runtime,
            record: recorder.sink
        )
        _ = try await dispatcher.dispatchShadowCall(
            assetId: "asset-cancel",
            window: ShadowWindow(start: 0, end: 30),
            configVariant: .allEnabledShadow
        )
        #expect(recorder.payloads.first?.errorTag == "cancelled")
    }

    @Test("opaque error ⇒ tag = 'other'")
    func opaqueErrorTag() async throws {
        let store = try await makeTestStore()
        try await seedSpecialistAsset(store: store, id: "asset-other")
        try await seedSpecialistChunk(
            store: store, assetId: "asset-other",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0,
            text: "some text"
        )
        let recorder = SpecialistPayloadRecorder()
        let runtime = makeStubSpecialistRuntime(classify: { _ in
            throw StubSpecialistError(message: "catastrophic xyz")
        })
        let dispatcher = LiveSpecialistShadowDispatcher(
            store: store,
            runtime: runtime,
            record: recorder.sink
        )
        _ = try await dispatcher.dispatchShadowCall(
            assetId: "asset-other",
            window: ShadowWindow(start: 0, end: 30),
            configVariant: .allEnabledShadow
        )
        #expect(recorder.payloads.first?.errorTag == "other")
    }

    // MARK: - Wire format

    @Test("SpecialistShadowPayload round-trips through JSON")
    func payloadRoundTrips() throws {
        let payload = SpecialistShadowPayload(
            payloadSchemaVersion: specialistShadowPayloadSchemaVersion,
            promptText: "buy now\nlimited offer",
            verdict: SpecialistVerdict(isAd: true, confidence: 0.75, adClass: "hostRead"),
            errorTag: nil
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(payload)
        let decoded = try JSONDecoder().decode(SpecialistShadowPayload.self, from: data)
        #expect(decoded == payload)
    }

    @Test("SpecialistVerdict clamps confidence into 0...1")
    func verdictClampsConfidence() {
        #expect(SpecialistVerdict(isAd: true, confidence: 1.7).confidence == 1.0)
        #expect(SpecialistVerdict(isAd: false, confidence: -0.5).confidence == 0.0)
        #expect(SpecialistVerdict(isAd: true, confidence: 0.42).confidence == 0.42)
    }
}

// MARK: - Test helpers (file-scoped)

/// Thread-safe in-memory sink standing in for the default `os_log` record
/// path. Captures every payload the dispatcher hands to `record`.
private final class SpecialistPayloadRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [SpecialistShadowPayload] = []

    var sink: @Sendable (SpecialistShadowPayload) -> Void {
        { [self] payload in
            lock.lock()
            storage.append(payload)
            lock.unlock()
        }
    }

    var payloads: [SpecialistShadowPayload] {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }
}

/// Error whose `String(describing:)` deterministically contains `message`
/// — drives the dispatcher's substring error classifier.
private struct StubSpecialistError: Error, CustomStringConvertible {
    let message: String
    var description: String { message }
}

/// Build a `SpecialistAdClassifier.Runtime` whose classify closure is
/// supplied by the caller. No CoreAI, no model — a pure closure seam.
private func makeStubSpecialistRuntime(
    classify: @escaping @Sendable (_ prompt: String) async throws -> SpecialistVerdict
) -> SpecialistAdClassifier.Runtime {
    SpecialistAdClassifier.Runtime(
        makeSession: {
            SpecialistAdClassifier.Runtime.Session(classify: classify)
        }
    )
}

private func seedSpecialistAsset(
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

private func seedSpecialistChunk(
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
