// TrainingExampleMaterializerTests.swift
// playhead-4my.10.1: end-to-end materialization test. Drives a real
// AnalysisStore through:
//   1. seed semantic_scan_results (the "spine" — one row per window),
//   2. seed evidence_events for each window,
//   3. seed decision_events (post-fusion outcomes) for some windows,
//   4. seed correction_events (user revert / FN report) for some windows,
//   5. invoke `TrainingExampleMaterializer.materialize(...)`,
//   6. read back `loadTrainingExamples(forAsset:)` and assert:
//      - the count matches the number of scan-result rows,
//      - every bucket the test seeded is reachable,
//      - cohort fields round-trip,
//      - re-running the materializer is idempotent (replace, not append).

import Foundation
import Testing

@testable import Playhead

@Suite("TrainingExampleMaterializer — playhead-4my.10.1")
struct TrainingExampleMaterializerTests {

    private let assetId = "asset-mat-1"
    private let transcriptVersion = "tv-mat-1"
    // AnalysisStore validates scanCohortJSON by decoding it as a real
    // `ScanCohort` value (see `validateScanCohortJSON`). Hand-rolling a
    // partial JSON here would fail that gate, so we round-trip through
    // the canonical production cohort. decisionCohortJSON is opaque to
    // the store and only needs to be a non-empty string.
    private let scanCohortJSON = ScanCohort.productionJSON()
    private let decisionCohortJSON =
        "{\"fusionVersion\":\"f1\"}"

    // MARK: - Fixtures

    private func makeAsset() -> AnalysisAsset {
        AnalysisAsset(
            id: assetId,
            episodeId: "ep-mat-1",
            assetFingerprint: "fp-mat-1",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/mat-1.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func scanResult(
        id: String,
        firstOrdinal: Int,
        lastOrdinal: Int,
        startTime: Double,
        endTime: Double,
        disposition: CoarseDisposition,
        quality: TranscriptQuality = .good
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: firstOrdinal,
            windowLastAtomOrdinal: lastOrdinal,
            windowStartTime: startTime,
            windowEndTime: endTime,
            scanPass: "coarse",
            transcriptQuality: quality,
            disposition: disposition,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 100,
            outputTokenCount: 20,
            latencyMs: 50,
            prewarmHit: false,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: transcriptVersion,
            reuseScope: nil,
            runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        )
    }

    private func evidenceEvent(
        id: String,
        sourceType: EvidenceSourceType,
        firstOrdinal: Int,
        lastOrdinal: Int,
        certainty: Double = 0.0
    ) throws -> EvidenceEvent {
        let ordinals = Array(firstOrdinal...lastOrdinal)
        let json = String(
            data: try JSONSerialization.data(withJSONObject: ordinals),
            encoding: .utf8
        ) ?? "[]"
        // Stuff a `certainty` hint into evidenceJSON so the materializer
        // can read it back; the schema is opaque to AnalysisStore.
        let payload = "{\"certainty\":\(certainty)}"
        return EvidenceEvent(
            id: id,
            analysisAssetId: assetId,
            eventType: "scan",
            sourceType: sourceType,
            atomOrdinals: json,
            evidenceJSON: payload,
            scanCohortJSON: scanCohortJSON,
            createdAt: 1_700_000_000,
            runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        )
    }

    private func decisionEvent(
        id: String,
        windowId: String,
        skipConfidence: Double,
        gate: String,
        policy: String
    ) -> DecisionEvent {
        DecisionEvent(
            id: id,
            analysisAssetId: assetId,
            eventType: "fusion",
            windowId: windowId,
            proposalConfidence: skipConfidence,
            skipConfidence: skipConfidence,
            eligibilityGate: gate,
            policyAction: policy,
            decisionCohortJSON: decisionCohortJSON,
            createdAt: 1_700_000_001,
            explanationJSON: nil
        )
    }

    private func adWindow(
        id: String,
        startTime: Double,
        endTime: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            confidence: 0.5,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: AdDetectionConfig.default.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    private func correctionEvent(
        id: String,
        scope: String,
        source: CorrectionSource
    ) -> CorrectionEvent {
        CorrectionEvent(
            id: id,
            analysisAssetId: assetId,
            scope: scope,
            createdAt: 1_700_000_002,
            source: source,
            podcastId: nil,
            correctionType: source.kind.correctionType,
            causalSource: nil,
            targetRefs: nil
        )
    }

    // MARK: - Tests

    @Test("materializer produces one example per scan-result spine row")
    func oneExamplePerScanResult() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // 3 windows on the spine.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-C", firstOrdinal: 21, lastOrdinal: 30,
            startTime: 20, endTime: 30, disposition: .uncertain,
            quality: .unusable
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId,
            store: store,
            now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(loaded.count == 3)
    }

    @Test("all four buckets are reachable from a realistic fixture")
    func allFourBucketsReachable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Window A: confirmed paid ad — FM containsAd, lexical hit, decision
        // skip-eligible, no user revert -> .positive
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-fm", sourceType: .fm,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-lex", sourceType: .lexical,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.8),
            transcriptVersion: transcriptVersion
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-A", windowId: "win-A",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))

        // Window B: editorial mention — FM noAds, lexical noAds, decision
        // not skip-eligible, no user signal -> .negative
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-B", startTime: 10, endTime: 20)
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-B", windowId: "win-B",
            skipConfidence: 0.05, gate: "ineligible", policy: "noAction"
        ))

        // Window C: unusable transcript -> .uncertain
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-C", firstOrdinal: 21, lastOrdinal: 30,
            startTime: 20, endTime: 30, disposition: .abstain,
            quality: .unusable
        ))

        // Window D: FM-positive but user reverted -> .disagreement
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-D", firstOrdinal: 31, lastOrdinal: 40,
            startTime: 30, endTime: 40, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-D", startTime: 30, endTime: 40)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-D-fm", sourceType: .fm,
                          firstOrdinal: 31, lastOrdinal: 40, certainty: 0.93),
            transcriptVersion: transcriptVersion
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-D", windowId: "win-D",
            skipConfidence: 0.85, gate: "eligible", policy: "autoSkipEligible"
        ))
        try await store.appendCorrectionEvent(correctionEvent(
            // Scope encodes the time range so the materializer can match
            // by interval-overlap.
            id: "corr-D",
            scope: "exactSpan:\(assetId):30.0:40.0",
            source: .listenRevert
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId,
            store: store,
            now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let buckets = Set(loaded.map { $0.bucket })
        #expect(buckets.contains(.positive))
        #expect(buckets.contains(.negative))
        #expect(buckets.contains(.uncertain))
        #expect(buckets.contains(.disagreement))
    }

    @Test("materialization is idempotent: re-running replaces, doesn't append")
    func materializationIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_200
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(loaded.count == 1)
    }

    @Test("cohort JSON round-trips through materialization")
    func cohortJSONProvenance() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-A", windowId: "win-A",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let row = try #require(loaded.first)
        #expect(row.scanCohortJSON == scanCohortJSON)
        #expect(row.decisionCohortJSON == decisionCohortJSON)
        #expect(row.transcriptVersion == transcriptVersion)
    }

    @Test("materializer no-ops when the asset has no scan-result spine")
    func emptyAssetDoesNotCrash() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(loaded.isEmpty)
    }
}
