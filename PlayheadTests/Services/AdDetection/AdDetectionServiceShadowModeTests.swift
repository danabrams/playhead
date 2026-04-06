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
        config: AdDetectionConfig,
        factory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)? = nil
    ) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            backfillJobRunnerFactory: factory
        )
    }

    /// Builds a deterministic `BackfillJobRunnerFactory` that routes FM work
    /// through `TestFMRuntime` with a canned coarse response. Both the
    /// shadow and disabled services in test #7 receive the same factory so
    /// the only observable difference is the `fmBackfillMode` gate in
    /// `AdDetectionService.runShadowFMPhase`.
    private func makeDeterministicFactory() -> @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner {
        return { store, mode in
            BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(
                    runtime: TestFMRuntime(
                        coarseResponses: [
                            CoarseScreeningSchema(
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
                scanCohortJSON: makeTestScanCohortJSON()
            )
        }
    }

    private func adWindowSignature(_ window: AdWindow) -> String {
        // Strip ID/UUID-like fields so signatures stay byte-identical across runs.
        "\(window.startTime)|\(window.endTime)|\(window.confidence)|\(window.boundaryState)|\(window.decisionState)|\(window.detectorVersion)|\(window.advertiser ?? "-")|\(window.product ?? "-")|\(window.evidenceText ?? "-")"
    }

    @Test("shadow mode produces byte-identical cues to disabled mode")
    func shadowAndDisabledProduceIdenticalAdWindows() async throws {
        // Test Gap #7 fix: the invariant is meaningful only when the shadow
        // service ACTUALLY runs the FM pipeline. Previously both services
        // passed `nil` factory, so "shadow" degraded to "disabled" and the
        // comparison became vacuous. Now both services receive the same
        // deterministic factory; the only behavioural difference is the
        // `config.fmBackfillMode` gate inside `runShadowFMPhase`.
        let chunksA = makeChunks(assetId: "asset-A")
        let chunksB = makeChunks(assetId: "asset-B")
        let factory = makeDeterministicFactory()

        // Run #1: disabled — factory is wired but the service's shadow gate
        // short-circuits, so the runner never fires.
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
            ),
            factory: factory
        )
        try await serviceA.runBackfill(
            chunks: chunksA,
            analysisAssetId: "asset-A",
            podcastId: "podcast-1",
            episodeDuration: 90
        )
        let disabledWindows = try await storeA.fetchAdWindows(assetId: "asset-A")

        // Run #2: shadow — factory fires, runner drives the FM pipeline
        // through TestFMRuntime and writes semantic_scan_results rows. The
        // shadow invariant says AdWindows must still be byte-identical.
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
            ),
            factory: factory
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

        // The shadow run MUST have written semantic scan rows — otherwise
        // the "actually ran FM" claim is unsupported.
        let shadowScans = try await storeB.fetchSemanticScanResults(analysisAssetId: "asset-B")
        #expect(!shadowScans.isEmpty, "shadow service must have exercised the FM pipeline")

        // The disabled run must NOT have written semantic scan / evidence rows.
        let disabledScans = try await storeA.fetchSemanticScanResults(analysisAssetId: "asset-A")
        let disabledEvents = try await storeA.fetchEvidenceEvents(analysisAssetId: "asset-A")
        #expect(disabledScans.isEmpty)
        #expect(disabledEvents.isEmpty)
    }

    @Test("M-D: shadow phase short-circuits when canUseFoundationModels is false")
    func shadowPhaseSkipsWhenFMUnavailable() async throws {
        // Inject a provider that reports FM is unavailable. The runner
        // factory should never be invoked; no semantic scan rows should
        // land in the store even though fmBackfillMode == .shadow.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-md"))

        // Use a factory that would PANIC if invoked, to prove the guard
        // runs before the factory is touched.
        nonisolated(unsafe) var factoryCallCount = 0
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
                factoryCallCount += 1
                return BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON()
                )
            },
            canUseFoundationModelsProvider: { false }
        )

        try await service.runBackfill(
            chunks: makeChunks(assetId: "asset-md"),
            analysisAssetId: "asset-md",
            podcastId: "podcast-md",
            episodeDuration: 90
        )

        #expect(factoryCallCount == 0, "factory must not be invoked when FM is unavailable")
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-md")
        #expect(scans.isEmpty, "no semantic scan rows should be written")
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
                    scanCohortJSON: makeTestScanCohortJSON()
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
