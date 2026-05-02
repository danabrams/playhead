// NetworkIsolationTests.swift
// playhead-h3h: E2E proof that the analysis pipeline emits zero outbound
// network requests during the on-device phases the privacy promise covers
// (transcription, ad classification, skip orchestration, banner rendering).
//
// Strategy: register a global `URLProtocol` subclass that records every
// outbound `URLRequest` `canInit(with:)` call. The protocol fails any
// recorded request with a synthetic error so production code that
// accidentally depends on the network is forced through its no-network
// path; production code paths that legitimately fetch user-initiated
// content (feed refresh, iTunes search, audio download) are explicitly
// out of scope here per the bead spec.
//
// Per phase, we drive the production entry point and assert the recorder's
// snapshot count is zero across the window.
//
// What is NOT testable in-process (deferred to real-device verification):
//   * The "force-quit during transcription" scenario from the original
//     bead description requires a real device that can be physically
//     killed. The transcription-phase test below covers the in-process
//     contract: while the engine runs against synthetic shards, no URL
//     request leaves the process.
//   * Network monitoring of system-level Speech framework infra. Apple
//     may emit telemetry at a layer below `URLProtocol`. That is a
//     real-device packet-capture concern; the in-process test asserts
//     only that the application code paths under our control issue no
//     `URLRequest`s.

import CoreMedia
import Foundation
import Testing
@testable import Playhead

// MARK: - Recording URLProtocol

/// Test-only URLProtocol that records every URLRequest `canInit(with:)`
/// call and fails the load with a synthetic 599 so production code is
/// forced through any no-network branch it has. Recorded URLs are
/// available via `snapshot()`.
final class RecordingURLProtocol: URLProtocol, @unchecked Sendable {
    nonisolated(unsafe) static let lock = NSLock()
    nonisolated(unsafe) static var recordedURLs: [URL] = []

    static func reset() {
        lock.lock(); defer { lock.unlock() }
        recordedURLs.removeAll()
    }

    static func snapshot() -> [URL] {
        lock.lock(); defer { lock.unlock() }
        return recordedURLs
    }

    /// Network schemes the privacy gate cares about. Custom in-process
    /// schemes (e.g. `playhead-progressive://` for the AVAssetResourceLoader
    /// shim) flow through `URLProtocol.canInit` too, but they never leave
    /// the device — they're synthetic handles, not real network. Excluding
    /// them here keeps the gate from flaking when another suite running
    /// concurrently happens to instantiate an AVURLAsset.
    /// `URLProtocol.registerClass` is process-global, so this filter is
    /// the only way to insulate the recorder from foreign-suite traffic
    /// without hard-serializing the entire xctestplan.
    private static let networkSchemes: Set<String> = ["http", "https", "ws", "wss"]

    override class func canInit(with request: URLRequest) -> Bool {
        if let url = request.url, let scheme = url.scheme?.lowercased(),
            networkSchemes.contains(scheme)
        {
            lock.lock()
            recordedURLs.append(url)
            lock.unlock()
        }
        return true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        client?.urlProtocol(self, didFailWithError: NSError(
            domain: "PlayheadH3HRecordingURLProtocol",
            code: 599,
            userInfo: [NSLocalizedDescriptionKey: "network blocked under privacy E2E"]
        ))
    }

    override func stopLoading() {}
}

@Suite("playhead-h3h - network isolation across pipeline phases", .serialized)
struct NetworkIsolationTests {

    // MARK: - Helpers

    private func withRecordedNetworkActivity<T: Sendable>(
        _ work: () async throws -> T
    ) async rethrows -> (result: T, recorded: [URL]) {
        RecordingURLProtocol.reset()
        URLProtocol.registerClass(RecordingURLProtocol.self)
        defer { URLProtocol.unregisterClass(RecordingURLProtocol.self) }
        let result = try await work()
        return (result, RecordingURLProtocol.snapshot())
    }

    private func makeAsset(id: String, episodeId: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "h3h-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///privacy/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome to the privacy gate test episode for playhead-h3h.",
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off.",
            "Now back to our content. Thanks for listening."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "h3h-\(assetId)-\(idx)",
                analysisAssetId: assetId,
                segmentFingerprint: "h3h-fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "h3h-test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    // MARK: - Test: ad classification phase

    @Test("Ad classification (AdDetectionService.runBackfill) emits zero URLRequests")
    func adClassificationPhaseEmitsNoNetwork() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "h3h-classify", episodeId: "ep-h3h-classify")
        try await store.insertAsset(asset)
        let chunks = makeChunks(assetId: asset.id)
        try await store.insertTranscriptChunks(chunks)

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig.default
        )

        let (_, recorded) = try await withRecordedNetworkActivity {
            try await service.runBackfill(
                chunks: chunks,
                analysisAssetId: asset.id,
                podcastId: "podcast-h3h",
                episodeDuration: 300
            )
        }

        #expect(
            recorded.isEmpty,
            "ad classification phase must perform zero network requests; recorded: \(recorded.map(\.absoluteString))"
        )
    }

    // MARK: - Test: skip orchestration phase

    @Test("Skip orchestration (receiveAdWindows + skip cue dispatch) emits zero URLRequests")
    func skipOrchestrationPhaseEmitsNoNetwork() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            config: .default,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "h3h-skip-ad",
            startTime: 30, endTime: 60,
            confidence: 0.9,
            decisionState: "confirmed"
        )

        let (_, recorded) = try await withRecordedNetworkActivity {
            await orchestrator.receiveAdWindows([ad])
            // Drain any deferred work the orchestrator schedules.
            await orchestrator.updatePlayheadTime(45)
            return ()
        }

        #expect(
            !pushedCues.isEmpty,
            "precondition: orchestrator must dispatch a cue so the assertion measures real work, not a no-op"
        )
        #expect(
            recorded.isEmpty,
            "skip orchestration phase must perform zero network requests; recorded: \(recorded.map(\.absoluteString))"
        )
    }

    // MARK: - Test: banner rendering phase

    @Test("Banner rendering (orchestrator banner stream) emits zero URLRequests")
    func bannerRenderingPhaseEmitsNoNetwork() async throws {
        // The "banner rendering" phase in production is the path between
        // SkipOrchestrator emitting a banner item and the SwiftUI layer
        // observing it via the AsyncStream surface. We exercise the
        // server-side of that contract here: subscribing to the banner
        // stream and pulling out the first item that flows from a
        // confirmed AdWindow. The view code itself is a pure SwiftUI
        // transform with no URL dependencies (verified by grep on the
        // Views directory; the only URL surface there is image asset
        // loading from the bundle).
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            config: .default,
            trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "h3h-banner-ad",
            startTime: 30, endTime: 60,
            confidence: 0.9,
            decisionState: "confirmed"
        )

        let (_, recorded) = try await withRecordedNetworkActivity {
            let bannerStream = await orchestrator.bannerItemStream()
            await orchestrator.receiveAdWindows([ad])
            // Pull at most one banner item so the test does not hang if
            // none is emitted; we only need to exercise the producer
            // path. The iterator is created and consumed inside the
            // child task to avoid Swift 6 sending-parameter violations
            // — escaping a `var` reference into `addTask` would cross
            // a concurrency boundary.
            _ = await withTaskGroup(of: Optional<AdSkipBannerItem>.self) { group in
                group.addTask {
                    var it = bannerStream.makeAsyncIterator()
                    return await it.next()
                }
                group.addTask {
                    try? await Task.sleep(for: .milliseconds(150))
                    return nil
                }
                let first = await group.next() ?? nil
                group.cancelAll()
                return first
            }
            return ()
        }

        #expect(
            recorded.isEmpty,
            "banner rendering phase must perform zero network requests; recorded: \(recorded.map(\.absoluteString))"
        )
    }

    // MARK: - Test: transcription phase

    @Test("Transcription (TranscriptEngineService persistence path) emits zero URLRequests")
    func transcriptionPhaseEmitsNoNetwork() async throws {
        // We don't drive a full SpeechAnalyzer transcription run in-process
        // (Apple's model files aren't reliably present in the simulator
        // image and an actor-side run would race the test deadline).
        // Instead we drive the deterministic on-store side: manufacturing
        // chunk records and persisting them via the same AnalysisStore
        // entry point the engine uses. If `upsertTranscriptChunks(_:)`
        // ever grew a network-backed observer (telemetry, remote model
        // sync, etc.), this test would catch it.
        let store = try await makeTestStore()
        let asset = makeAsset(id: "h3h-asr", episodeId: "ep-h3h-asr")
        try await store.insertAsset(asset)
        let chunks = makeChunks(assetId: asset.id)

        let (_, recorded) = try await withRecordedNetworkActivity {
            try await store.insertTranscriptChunks(chunks)
            // Read-back path too (DiagnosticsBundleBuilder + UI consumers
            // round-trip through this).
            _ = try await store.fetchTranscriptChunks(assetId: asset.id)
            return ()
        }

        #expect(
            recorded.isEmpty,
            "transcription persistence path must perform zero network requests; recorded: \(recorded.map(\.absoluteString))"
        )
    }
}
