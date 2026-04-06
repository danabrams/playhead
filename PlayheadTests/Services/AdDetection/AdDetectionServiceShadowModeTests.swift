// AdDetectionServiceShadowModeTests.swift
// The shadow invariant: with fmBackfillMode = .shadow, the AdWindows produced
// by AdDetectionService.runBackfill must be byte-identical to the AdWindows
// produced with fmBackfillMode = .disabled. The only observable difference is
// rows in semantic_scan_results / evidence_events.

import Foundation
import Testing

@testable import Playhead

@Suite("AdDetectionService shadow-mode invariant")
struct AdDetectionServiceShadowModeTests {

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
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
            "Welcome to the show. Today we're discussing podcasts and how to find them.",
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off your first purchase at squarespace dot com slash show.",
            "Now back to our interview with our guest about technology trends."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeService(
        store: AnalysisStore,
        config: AdDetectionConfig
    ) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    private func adWindowSignature(_ window: AdWindow) -> String {
        // Strip ID/UUID-like fields so signatures stay byte-identical across runs.
        "\(window.startTime)|\(window.endTime)|\(window.confidence)|\(window.boundaryState)|\(window.decisionState)|\(window.detectorVersion)|\(window.advertiser ?? "-")|\(window.product ?? "-")|\(window.evidenceText ?? "-")"
    }

    @Test("shadow mode produces byte-identical cues to disabled mode")
    func shadowAndDisabledProduceIdenticalAdWindows() async throws {
        let chunksA = makeChunks(assetId: "asset-A")
        let chunksB = makeChunks(assetId: "asset-B")

        // Run #1: disabled
        let storeA = try await makeTestStore()
        try await storeA.insertAsset(makeAsset(id: "asset-A"))
        let serviceA = makeService(
            store: storeA,
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .disabled
            )
        )
        try await serviceA.runBackfill(
            chunks: chunksA,
            analysisAssetId: "asset-A",
            podcastId: "podcast-1",
            episodeDuration: 90
        )
        let disabledWindows = try await storeA.fetchAdWindows(assetId: "asset-A")

        // Run #2: shadow
        let storeB = try await makeTestStore()
        try await storeB.insertAsset(makeAsset(id: "asset-B"))
        let serviceB = makeService(
            store: storeB,
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            )
        )
        try await serviceB.runBackfill(
            chunks: chunksB,
            analysisAssetId: "asset-B",
            podcastId: "podcast-1",
            episodeDuration: 90
        )
        let shadowWindows = try await storeB.fetchAdWindows(assetId: "asset-B")

        // Byte-identical signatures (counts and contents).
        let disabledSigs = disabledWindows.map(adWindowSignature).sorted()
        let shadowSigs = shadowWindows.map(adWindowSignature).sorted()
        #expect(disabledSigs == shadowSigs, "shadow vs disabled cue divergence: \(shadowSigs) vs \(disabledSigs)")

        // The shadow run is allowed to write semantic scan / evidence rows.
        // The disabled run must NOT.
        let disabledScans = try await storeA.fetchSemanticScanResults(analysisAssetId: "asset-A")
        let disabledEvents = try await storeA.fetchEvidenceEvents(analysisAssetId: "asset-A")
        #expect(disabledScans.isEmpty)
        #expect(disabledEvents.isEmpty)
    }

    @Test("shadow mode actually writes semantic_scan_results telemetry")
    func shadowModeWritesScanTelemetry() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-tel"))
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: { store, mode in
                BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(
                        runtime: TestFMRuntime(
                            coarseResponses: [
                                CoarseScreeningSchema(
                                    transcriptQuality: .good,
                                    disposition: .containsAd,
                                    support: CoarseSupportSchema(
                                        supportLineRefs: [1],
                                        certainty: .strong
                                    )
                                )
                            ]
                        ).runtime
                    ),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON(),
                    decisionCohortJSON: nil
                )
            }
        )

        try await service.runBackfill(
            chunks: makeChunks(assetId: "asset-tel"),
            analysisAssetId: "asset-tel",
            podcastId: "podcast-tel",
            episodeDuration: 90
        )

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-tel")
        #expect(!scans.isEmpty, "shadow mode must persist FM scan results to telemetry")
        // And shadow mode must STILL not have promoted any FM source to AdWindows.
        let windows = try await store.fetchAdWindows(assetId: "asset-tel")
        #expect(windows.allSatisfy { $0.detectorVersion == "detection-v1" })
    }
}
