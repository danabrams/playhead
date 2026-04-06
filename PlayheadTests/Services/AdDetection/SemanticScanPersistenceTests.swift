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

    // MARK: - New behaviour from review fixes

    @Test("C5: identical reuse key collapses to one row, latest wins")
    func semanticScanResultUniqueOnReuseKey() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        func make(id: String, attempt: Int) -> SemanticScanResult {
            SemanticScanResult(
                id: id,
                analysisAssetId: "asset-1",
                windowFirstAtomOrdinal: 5,
                windowLastAtomOrdinal: 9,
                windowStartTime: 50,
                windowEndTime: 90,
                scanPass: "passA",
                transcriptQuality: .good,
                disposition: .containsAd,
                spansJSON: "[]",
                status: .success,
                attemptCount: attempt,
                errorContext: nil,
                inputTokenCount: nil,
                outputTokenCount: nil,
                latencyMs: nil,
                prewarmHit: false,
                scanCohortJSON: cohort,
                transcriptVersion: "tx-v1"
            )
        }

        try await store.insertSemanticScanResult(make(id: "older", attempt: 1))
        try await store.insertSemanticScanResult(make(id: "newer", attempt: 7))

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        #expect(rows.count == 1)
        #expect(rows.first?.id == "newer")
        #expect(rows.first?.attemptCount == 7)
    }

    @Test("C6: reusable lookup ignores failed attempts and prefers latest success")
    func semanticScanReuseSkipsFailedAttempts() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let failed = SemanticScanResult(
            id: "scan-failed",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 60,
            windowLastAtomOrdinal: 70,
            windowStartTime: 600,
            windowEndTime: 700,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .abstain,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 4,
            errorContext: #"{"reason":"safety"}"#,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )
        let succeeded = SemanticScanResult(
            id: "scan-succeeded",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 60,
            windowLastAtomOrdinal: 70,
            windowStartTime: 600,
            windowEndTime: 700,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: #"[{"startAtom":61,"endAtom":67}]"#,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 100,
            outputTokenCount: 20,
            latencyMs: 30,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )

        try await store.insertSemanticScanResult(failed)
        try await store.insertSemanticScanResult(succeeded)

        let reused = try await store.fetchReusableSemanticScanResult(
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 60,
            windowLastAtomOrdinal: 70,
            scanPass: "passA",
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )
        #expect(reused?.id == "scan-succeeded")
    }

    @Test("H11: evidence dedup ignores duplicates with new IDs")
    func evidenceEventDedupOnNaturalKey() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        func event(id: String) -> EvidenceEvent {
            EvidenceEvent(
                id: id,
                analysisAssetId: "asset-1",
                eventType: "windowQuoted",
                sourceType: .fm,
                atomOrdinals: "[10,11,12]",
                evidenceJSON: #"{"quote":"X"}"#,
                scanCohortJSON: cohort,
                createdAt: 100
            )
        }

        try await store.insertEvidenceEvent(event(id: UUID().uuidString))
        try await store.insertEvidenceEvent(event(id: UUID().uuidString))
        try await store.insertEvidenceEvent(event(id: UUID().uuidString))

        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(fetched.count == 1)
    }

    @Test("M25: invalid atomOrdinals JSON throws at the persistence boundary")
    func evidenceEventRejectsMalformedAtomOrdinals() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let bad = EvidenceEvent(
            id: "bad-1",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "not-an-array",
            evidenceJSON: "{}",
            scanCohortJSON: try makeScanCohortJSON(),
            createdAt: 0
        )
        let mixed = EvidenceEvent(
            id: "bad-2",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: #"["nope"]"#,
            evidenceJSON: "{}",
            scanCohortJSON: try makeScanCohortJSON(),
            createdAt: 0
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertEvidenceEvent(bad)
        }
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertEvidenceEvent(mixed)
        }
    }

    @Test("malformed scanCohortJSON throws at the persistence boundary")
    func semanticScanResultRejectsMalformedScanCohortJSON() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let bad = SemanticScanResult(
            id: "bad-cohort",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: 0,
            windowEndTime: 1,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: "{not-json",
            transcriptVersion: "tx-v1"
        )
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertSemanticScanResult(bad)
        }
    }

    @Test("M3: scan cohort decode failure logs and surfaces via observer")
    func scanCohortDecodeFailureLogs() throws {
        nonisolated(unsafe) var observed = 0
        SemanticScanResult.decodeFailureObserver = { _, _ in
            observed += 1
        }
        defer { SemanticScanResult.decodeFailureObserver = nil }

        // Two different non-equal strings, one a valid ScanCohort, the other
        // valid JSON but missing required fields. The mismatch falls into the
        // decode path and the observer must fire.
        let valid = try makeScanCohortJSON()
        let invalid = #"{"missing":"fields"}"#
        let result = SemanticScanResult.matchesScanCohortJSON(valid, invalid)
        #expect(result == false)
        #expect(observed >= 1)
    }

    @Test("M2: recordSemanticScanResult rolls back on partial failure")
    func recordSemanticScanResultRollsBack() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let result = SemanticScanResult(
            id: "rb-scan",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 9,
            windowStartTime: 0,
            windowEndTime: 90,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )
        let goodEvent = EvidenceEvent(
            id: "ev-good",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[1,2,3]",
            evidenceJSON: "{}",
            scanCohortJSON: cohort,
            createdAt: 0
        )
        // Second event has malformed atomOrdinals — the whole batch must
        // roll back, so neither the scan result nor the first event survive.
        let badEvent = EvidenceEvent(
            id: "ev-bad",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "garbage",
            evidenceJSON: "{}",
            scanCohortJSON: cohort,
            createdAt: 0
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.recordSemanticScanResult(result, evidenceEvents: [goodEvent, badEvent])
        }

        let scanRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        let evRows = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(scanRows.isEmpty)
        #expect(evRows.isEmpty)
    }

    @Test("FK cascade: deleting an asset removes its semantic scan rows")
    func semanticScanCascadesOnAssetDelete() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let result = SemanticScanResult(
            id: "fk-1",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: 0,
            windowEndTime: 1,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: try makeScanCohortJSON(),
            transcriptVersion: "tx-v1"
        )
        try await store.insertSemanticScanResult(result)
        try await store.deleteAsset(id: "asset-1")

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        #expect(rows.isEmpty)
    }

    @Test("fetchReusableSemanticScanResult is fast under cohort variation")
    func fetchReusableSemanticScanResultPerformance() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        // Insert 200 rows with varying cohorts and ordinals.
        for i in 0..<200 {
            let cohort = try makeScanCohortJSON(promptHash: "prompt-\(i)")
            try await store.insertSemanticScanResult(
                SemanticScanResult(
                    id: "perf-\(i)",
                    analysisAssetId: "asset-1",
                    windowFirstAtomOrdinal: i,
                    windowLastAtomOrdinal: i + 4,
                    windowStartTime: Double(i),
                    windowEndTime: Double(i + 4),
                    scanPass: "passA",
                    transcriptQuality: .good,
                    disposition: .containsAd,
                    spansJSON: "[]",
                    status: .success,
                    attemptCount: 1,
                    errorContext: nil,
                    inputTokenCount: nil,
                    outputTokenCount: nil,
                    latencyMs: nil,
                    prewarmHit: false,
                    scanCohortJSON: cohort,
                    transcriptVersion: "tx-v1"
                )
            )
        }

        let target = try makeScanCohortJSON(promptHash: "prompt-150")
        let start = Date()
        let found = try await store.fetchReusableSemanticScanResult(
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 150,
            windowLastAtomOrdinal: 154,
            scanPass: "passA",
            scanCohortJSON: target,
            transcriptVersion: "tx-v1"
        )
        let elapsed = Date().timeIntervalSince(start)

        #expect(found?.id == "perf-150")
        #expect(elapsed < 0.05, "fetch took \(elapsed)s")
    }
}
