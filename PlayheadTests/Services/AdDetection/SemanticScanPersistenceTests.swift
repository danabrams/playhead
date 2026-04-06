// SemanticScanPersistenceTests.swift
// Store-level regression tests for semantic scan result persistence and
// append-only evidence logging.

import Foundation
import Testing

@testable import Playhead

private func makePersistenceTestAsset(
    id: String = "asset-1",
    episodeId: String = "episode-1"
) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId,
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

private func makeScanCohortJSON(
    promptHash: String = "prompt-v1",
    schemaHash: String = "schema-v1"
) throws -> String {
    let cohort = ScanCohort(
        promptLabel: "phase3-passA",
        promptHash: promptHash,
        schemaHash: schemaHash,
        scanPlanHash: "scan-plan-v1",
        normalizationHash: "normalization-v1",
        osBuild: "26A123",
        locale: "en_US",
        appBuild: "123"
    )
    return String(decoding: try JSONEncoder().encode(cohort), as: UTF8.self)
}

private func makeEquivalentScanCohortJSONWithDifferentKeyOrder(
    promptHash: String = "prompt-v1",
    schemaHash: String = "schema-v1"
) -> String {
    """
    {"schemaHash":"\(schemaHash)","promptHash":"\(promptHash)","promptLabel":"phase3-passA","scanPlanHash":"scan-plan-v1","normalizationHash":"normalization-v1","osBuild":"26A123","locale":"en_US","appBuild":"123"}
    """
}

@Suite("Semantic Scan Persistence")
struct SemanticScanPersistenceTests {

    @Test("SemanticScanResult round-trips and is queryable by asset")
    func semanticScanResultRoundTrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let result = SemanticScanResult(
            id: "scan-1",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 10,
            windowLastAtomOrdinal: 18,
            windowStartTime: 120,
            windowEndTime: 165,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: #"[{"startAtom":11,"endAtom":14}]"#,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 128,
            outputTokenCount: 16,
            latencyMs: 42,
            prewarmHit: true,
            scanCohortJSON: try makeScanCohortJSON(),
            transcriptVersion: "tx-v1"
        )

        try await store.insertSemanticScanResult(result)

        let fetched = try await store.fetchSemanticScanResult(id: result.id)
        let byAsset = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")

        #expect(fetched == result)
        #expect(byAsset == [result])
    }

    @Test("refusal scan results round-trip with failure metadata intact")
    func refusalSemanticScanResultRoundTrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let result = SemanticScanResult(
            id: "scan-refusal",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 31,
            windowLastAtomOrdinal: 36,
            windowStartTime: 310,
            windowEndTime: 360,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: .abstain,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 1,
            errorContext: #"{"reason":"safety refusal","lineRefs":[31,32,33]}"#,
            inputTokenCount: 144,
            outputTokenCount: nil,
            latencyMs: 19,
            prewarmHit: false,
            scanCohortJSON: try makeScanCohortJSON(),
            transcriptVersion: "tx-v1"
        )

        try await store.insertSemanticScanResult(result)

        let fetched = try await store.fetchSemanticScanResult(id: result.id)
        let byAsset = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")

        #expect(fetched == result)
        #expect(fetched?.status == .refusal)
        #expect(fetched?.status.retryPolicy == .persistFailure)
        #expect(fetched?.errorContext == #"{"reason":"safety refusal","lineRefs":[31,32,33]}"#)
        #expect(byAsset == [result])
    }

    @Test("semantic scan reuse invalidates on scan cohort or transcript version but not decision cohort")
    func semanticScanReuseInvalidation() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let result = SemanticScanResult(
            id: "scan-reuse",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 20,
            windowLastAtomOrdinal: 30,
            windowStartTime: 200,
            windowEndTime: 260,
            scanPass: "passA",
            transcriptQuality: .degraded,
            disposition: .uncertain,
            spansJSON: "[]",
            status: .success,
            attemptCount: 2,
            errorContext: nil,
            inputTokenCount: 256,
            outputTokenCount: 24,
            latencyMs: 85,
            prewarmHit: false,
            scanCohortJSON: try makeScanCohortJSON(promptHash: "prompt-v1", schemaHash: "schema-v1"),
            transcriptVersion: "tx-v1"
        )
        try await store.insertSemanticScanResult(result)

        let decisionCohortOnlyChanged = try await store.fetchReusableSemanticScanResult(
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 20,
            windowLastAtomOrdinal: 30,
            scanPass: "passA",
            scanCohortJSON: makeEquivalentScanCohortJSONWithDifferentKeyOrder(
                promptHash: "prompt-v1",
                schemaHash: "schema-v1"
            ),
            transcriptVersion: "tx-v1"
        )
        let scanCohortChanged = try await store.fetchReusableSemanticScanResult(
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 20,
            windowLastAtomOrdinal: 30,
            scanPass: "passA",
            scanCohortJSON: try makeScanCohortJSON(promptHash: "prompt-v2", schemaHash: "schema-v1"),
            transcriptVersion: "tx-v1"
        )
        let transcriptChanged = try await store.fetchReusableSemanticScanResult(
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 20,
            windowLastAtomOrdinal: 30,
            scanPass: "passA",
            scanCohortJSON: try makeScanCohortJSON(promptHash: "prompt-v1", schemaHash: "schema-v1"),
            transcriptVersion: "tx-v2"
        )

        #expect(decisionCohortOnlyChanged?.id == result.id)
        #expect(scanCohortChanged == nil)
        #expect(transcriptChanged == nil)
    }

    @Test("reusable lookup returns the matching stored scan cohort when multiple cohorts exist")
    func semanticScanReuseFindsMatchingCohortAmongHistoricalRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let originalCohortJSON = try makeScanCohortJSON(promptHash: "prompt-v1", schemaHash: "schema-v1")
        let newerCohortJSON = try makeScanCohortJSON(promptHash: "prompt-v2", schemaHash: "schema-v1")

        let original = SemanticScanResult(
            id: "scan-original",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 40,
            windowLastAtomOrdinal: 50,
            windowStartTime: 400,
            windowEndTime: 460,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: #"[{"startAtom":41,"endAtom":45}]"#,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 100,
            outputTokenCount: 20,
            latencyMs: 30,
            prewarmHit: true,
            scanCohortJSON: originalCohortJSON,
            transcriptVersion: "tx-v1"
        )
        let newer = SemanticScanResult(
            id: "scan-newer",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 40,
            windowLastAtomOrdinal: 50,
            windowStartTime: 400,
            windowEndTime: 460,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .uncertain,
            spansJSON: "[]",
            status: .success,
            attemptCount: 3,
            errorContext: nil,
            inputTokenCount: 140,
            outputTokenCount: 18,
            latencyMs: 28,
            prewarmHit: false,
            scanCohortJSON: newerCohortJSON,
            transcriptVersion: "tx-v1"
        )

        try await store.insertSemanticScanResult(original)
        try await store.insertSemanticScanResult(newer)

        let fetchedOriginal = try await store.fetchReusableSemanticScanResult(
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 40,
            windowLastAtomOrdinal: 50,
            scanPass: "passA",
            scanCohortJSON: originalCohortJSON,
            transcriptVersion: "tx-v1"
        )
        let fetchedNewer = try await store.fetchReusableSemanticScanResult(
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 40,
            windowLastAtomOrdinal: 50,
            scanPass: "passA",
            scanCohortJSON: newerCohortJSON,
            transcriptVersion: "tx-v1"
        )

        #expect(fetchedOriginal == original)
        #expect(fetchedNewer == newer)
    }

    @Test("EvidenceEvent is append-only and duplicate IDs do not overwrite earlier evidence")
    func evidenceEventAppendOnly() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let original = EvidenceEvent(
            id: "event-1",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[10,11,12]",
            evidenceJSON: #"{"quote":"supporting text"}"#,
            scanCohortJSON: try makeScanCohortJSON(),
            createdAt: 100
        )
        let second = EvidenceEvent(
            id: "event-2",
            analysisAssetId: "asset-1",
            eventType: "keywordHit",
            sourceType: .lexical,
            atomOrdinals: "[15,16]",
            evidenceJSON: #"{"term":"promo code"}"#,
            scanCohortJSON: try makeScanCohortJSON(),
            createdAt: 200
        )
        let duplicateID = EvidenceEvent(
            id: "event-1",
            analysisAssetId: "asset-1",
            eventType: "mutated",
            sourceType: .catalog,
            atomOrdinals: "[99]",
            evidenceJSON: #"{"mutated":true}"#,
            scanCohortJSON: try makeScanCohortJSON(promptHash: "prompt-v2"),
            createdAt: 300
        )

        try await store.insertEvidenceEvent(original)
        try await store.insertEvidenceEvent(second)
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertEvidenceEvent(duplicateID)
        }

        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")

        #expect(fetched == [original, second])
    }
}
