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

    @Test("bind helper preserves UTF-8 edge cases (emoji, RTL, combining marks)")
    func bindHelperPreservesUTF8EdgeCases() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        // Payload mixes: multi-byte emoji (incl. ZWJ family sequence), RTL
        // Arabic, Devanagari + combining mark, a grapheme cluster with a
        // combining accent, and a 4-byte CJK supplement character. All
        // NUL-free so `withCString` sees the full string.
        let trickySpansJSON = """
        [{"label":"emoji 👨‍👩‍👧‍👦 family","rtl":"مرحبا بالعالم","combining":"e\u{0301}","devanagari":"हिन्दी","cjkSupplement":"\u{2070E}"}]
        """
        let result = SemanticScanResult(
            id: "scan-utf8-edge",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 5,
            windowStartTime: 0,
            windowEndTime: 50,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: trickySpansJSON,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 64,
            outputTokenCount: 8,
            latencyMs: 12,
            prewarmHit: false,
            scanCohortJSON: try makeScanCohortJSON(),
            transcriptVersion: "tx-v1"
        )

        try await store.insertSemanticScanResult(result)

        let fetched = try await store.fetchSemanticScanResult(id: result.id)
        #expect(fetched == result)
        #expect(fetched?.spansJSON == trickySpansJSON)
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

    @Test("H-2: evidence_events batches from different transcript versions coexist for audit")
    func evidenceEventsFromDistinctScansCoexist() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let scanV1 = SemanticScanResult(
            id: "scan-v1",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 9,
            windowStartTime: 0,
            windowEndTime: 90,
            scanPass: "passB",
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
        let scanV2 = SemanticScanResult(
            id: "scan-v2",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 9,
            windowStartTime: 0,
            windowEndTime: 90,
            scanPass: "passB",
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
            transcriptVersion: "tx-v2"
        )
        let eventV1 = EvidenceEvent(
            id: "ev-v1",
            analysisAssetId: "asset-1",
            eventType: "fm.spanRefinement",
            sourceType: .fm,
            atomOrdinals: "[1,2,3]",
            evidenceJSON: #"{"run":"v1"}"#,
            scanCohortJSON: cohort,
            createdAt: 100
        )
        let eventV2 = EvidenceEvent(
            id: "ev-v2",
            analysisAssetId: "asset-1",
            eventType: "fm.spanRefinement",
            // Different eventType is not needed; the natural key includes
            // evidenceJSON so differing payloads allow the row through
            // dedup. Use a different atomOrdinals tuple to guarantee the
            // two rows are distinguishable.
            sourceType: .fm,
            atomOrdinals: "[4,5,6]",
            evidenceJSON: #"{"run":"v2"}"#,
            scanCohortJSON: cohort,
            createdAt: 200
        )

        try await store.recordSemanticScanResult(scanV1, evidenceEvents: [eventV1])
        try await store.recordSemanticScanResult(scanV2, evidenceEvents: [eventV2])

        let events = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(events.count == 2, "evidence events from distinct scan runs must both persist (append-only audit)")
        let ids = Set(events.map(\.id))
        #expect(ids == ["ev-v1", "ev-v2"])
    }

    @Test("H-1: canonicalized cohort JSON collapses reuse key across key-order differences")
    func semanticScanResultCollapsesEquivalentCohortsAcrossKeyOrder() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        // Identical cohort fields but different JSON key order.
        let sortedCohort = try makeScanCohortJSON(promptHash: "ph", schemaHash: "sh")
        let shuffledCohort = makeEquivalentScanCohortJSONWithDifferentKeyOrder(
            promptHash: "ph",
            schemaHash: "sh"
        )
        #expect(sortedCohort != shuffledCohort, "fixture sanity: the two JSON strings must differ")

        func make(id: String, attempt: Int, cohortJSON: String) -> SemanticScanResult {
            SemanticScanResult(
                id: id,
                analysisAssetId: "asset-1",
                windowFirstAtomOrdinal: 100,
                windowLastAtomOrdinal: 110,
                windowStartTime: 1000,
                windowEndTime: 1100,
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
                scanCohortJSON: cohortJSON,
                transcriptVersion: "tx-v1"
            )
        }

        try await store.insertSemanticScanResult(make(id: "sorted", attempt: 1, cohortJSON: sortedCohort))
        try await store.insertSemanticScanResult(make(id: "shuffled", attempt: 9, cohortJSON: shuffledCohort))

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        #expect(rows.count == 1, "UNIQUE(reuseKeyHash) must collapse cohort-equivalent rows")
        #expect(rows.first?.id == "shuffled")
        #expect(rows.first?.attemptCount == 9)
    }

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

    @Test("L-3: validateScanCohortJSON rejects a JSON array at the top level")
    func validateScanCohortJSON_rejectsArrayAtTopLevel() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        // Parseable JSON but not a cohort object: a top-level array.
        let bad = SemanticScanResult(
            id: "bad-array-cohort",
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
            scanCohortJSON: "[]",
            transcriptVersion: "tx-v1"
        )
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertSemanticScanResult(bad)
        }
    }

    @Test("L-4: validateAtomOrdinalsJSON rejects float elements")
    func validateAtomOrdinalsJSON_rejectsFloatElements() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let floats = EvidenceEvent(
            id: "evt-floats",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[1.5, 2.0]",
            evidenceJSON: "{}",
            scanCohortJSON: try makeScanCohortJSON(),
            createdAt: 0
        )
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertEvidenceEvent(floats)
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

        // M-5: the previous version of this test passed a malformed
        // `badEvent` whose atomOrdinals failed validation *before* the
        // transaction opened, so it never actually exercised the
        // BEGIN IMMEDIATE / ROLLBACK path. Rewrite to force a mid-batch
        // failure: pre-plant a row whose id will collide with the second
        // event inside the batch, after the scan row and the first event
        // have already been inserted against an open transaction.
        let preExisting = EvidenceEvent(
            id: "ev-collide",
            analysisAssetId: "asset-1",
            eventType: "preExisting",
            sourceType: .catalog,
            atomOrdinals: "[99]",
            evidenceJSON: #"{"planted":"before batch"}"#,
            scanCohortJSON: cohort,
            createdAt: 1
        )
        try await store.insertEvidenceEvent(preExisting)

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
        // Same id as `preExisting` but different body — triggers the H-2
        // body-mismatch throw mid-batch, forcing ROLLBACK of the
        // already-inserted `result` and `goodEvent`.
        let conflictingEvent = EvidenceEvent(
            id: "ev-collide",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[1,2,3]",
            evidenceJSON: #"{"conflicts":"with planted"}"#,
            scanCohortJSON: cohort,
            createdAt: 2
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.recordSemanticScanResult(
                result,
                evidenceEvents: [goodEvent, conflictingEvent]
            )
        }

        // The scan row and `ev-good` must be rolled back. Only the
        // pre-existing row should remain.
        let scanRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        let evRows = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(scanRows.isEmpty, "scan row must be rolled back")
        #expect(evRows.count == 1, "only the pre-existing row must survive")
        #expect(evRows.first?.id == "ev-collide")
        #expect(evRows.first?.evidenceJSON == #"{"planted":"before batch"}"#)
    }

    // R4-Fix2: recordSemanticScanResult opened BEGIN IMMEDIATE then called
    // insertSemanticScanResult, which silently short-circuits via the H-1
    // success-protection probe when a `.success` row already exists at the
    // same reuseKeyHash. The evidence rows were committed anyway, attaching
    // to a phantom scan that the store never wrote. Verify the refusal
    // retry path leaves both the cached success AND the evidence table
    // unchanged.
    @Test("R4-Fix2: refusal retry over a cached success commits no orphan evidence")
    func recordSemanticScanResultSkipsOrphanEvidenceOnSuccessCollision() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let success = SemanticScanResult(
            id: "cached-success",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 400,
            windowLastAtomOrdinal: 410,
            windowStartTime: 4000,
            windowEndTime: 4100,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: #"[{"startAtom":401,"endAtom":405}]"#,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 128,
            outputTokenCount: 16,
            latencyMs: 42,
            prewarmHit: true,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )
        try await store.insertSemanticScanResult(success)

        // Same reuseKeyHash, but a refusal status. The H-1 probe in
        // insertSemanticScanResult silently no-ops; without the R4-Fix2
        // guard, the surrounding recordSemanticScanResult transaction
        // still commits the 3 evidence events as orphans.
        let refusal = SemanticScanResult(
            id: "later-refusal",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 400,
            windowLastAtomOrdinal: 410,
            windowStartTime: 4000,
            windowEndTime: 4100,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: .abstain,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 2,
            errorContext: #"{"reason":"safety"}"#,
            inputTokenCount: 100,
            outputTokenCount: nil,
            latencyMs: 21,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )
        let orphanEvents = (0..<3).map { idx in
            EvidenceEvent(
                id: "orphan-ev-\(idx)",
                analysisAssetId: "asset-1",
                eventType: "fm.spanRefinement",
                sourceType: .fm,
                atomOrdinals: "[\(401 + idx)]",
                evidenceJSON: #"{"shouldNotBePersisted":true}"#,
                scanCohortJSON: cohort,
                createdAt: Double(idx + 10)
            )
        }

        try await store.recordSemanticScanResult(refusal, evidenceEvents: orphanEvents)

        // The cached success row must be untouched.
        let scanRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        #expect(scanRows.count == 1, "the cached success must remain the only scan row")
        #expect(scanRows.first?.id == "cached-success")
        #expect(scanRows.first?.status == .success)
        #expect(scanRows.first?.spansJSON == #"[{"startAtom":401,"endAtom":405}]"#)

        // None of the orphan evidence rows must have been committed.
        let evRows = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(evRows.isEmpty, "no evidence may attach to a phantom scan; got \(evRows.count) rows")
    }

    // MARK: - Round-2 review fixes

    @Test("H-1: a refusal retry must not overwrite a cached success at the same reuse key")
    func refusalMustNotClobberCachedSuccess() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let success = SemanticScanResult(
            id: "success-row",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 200,
            windowLastAtomOrdinal: 210,
            windowStartTime: 2000,
            windowEndTime: 2100,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: #"[{"startAtom":201,"endAtom":205}]"#,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 128,
            outputTokenCount: 16,
            latencyMs: 42,
            prewarmHit: true,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )
        // Same reuseKeyHash (same asset/window/pass/transcriptVersion/cohort)
        // but a `.refusal` status. The H-1 bug silently REPLACEd the success
        // row with the refusal; the fix must leave the success row intact.
        let refusal = SemanticScanResult(
            id: "refusal-row",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 200,
            windowLastAtomOrdinal: 210,
            windowStartTime: 2000,
            windowEndTime: 2100,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .abstain,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 2,
            errorContext: #"{"reason":"safety"}"#,
            inputTokenCount: 144,
            outputTokenCount: nil,
            latencyMs: 19,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )

        try await store.insertSemanticScanResult(success)
        try await store.insertSemanticScanResult(refusal)

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        #expect(rows.count == 1, "reuseKey collapses to one row")
        #expect(rows.first?.status == .success, "success must survive the refusal retry")
        #expect(rows.first?.id == "success-row")
        #expect(rows.first?.spansJSON == #"[{"startAtom":201,"endAtom":205}]"#)
    }

    @Test("H-1: a later success overwrites an earlier refusal at the same reuse key")
    func successStillOverwritesEarlierRefusal() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let refusal = SemanticScanResult(
            id: "early-refusal",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 300,
            windowLastAtomOrdinal: 310,
            windowStartTime: 3000,
            windowEndTime: 3100,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .abstain,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 1,
            errorContext: #"{"reason":"safety"}"#,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )
        let success = SemanticScanResult(
            id: "later-success",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 300,
            windowLastAtomOrdinal: 310,
            windowStartTime: 3000,
            windowEndTime: 3100,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: #"[{"startAtom":301,"endAtom":305}]"#,
            status: .success,
            attemptCount: 3,
            errorContext: nil,
            inputTokenCount: 100,
            outputTokenCount: 20,
            latencyMs: 30,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )

        try await store.insertSemanticScanResult(refusal)
        try await store.insertSemanticScanResult(success)

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        #expect(rows.count == 1)
        #expect(rows.first?.status == .success)
        #expect(rows.first?.id == "later-success")
    }

    @Test("H-2: insertEvidenceEvent throws when the same id reappears with a different body")
    func evidenceEventMismatchedBodyThrows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let first = EvidenceEvent(
            id: "shared-id",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"quote":"original body"}"#,
            scanCohortJSON: cohort,
            createdAt: 100
        )
        // Same id, same natural key, but different evidenceJSON. The
        // pre-round-2 code silently kept the original body without
        // warning the caller. H-2 requires a loud mismatch throw.
        let mutated = EvidenceEvent(
            id: "shared-id",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"quote":"mutated body"}"#,
            scanCohortJSON: cohort,
            createdAt: 100
        )

        try await store.insertEvidenceEvent(first)
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertEvidenceEvent(mutated)
        }

        // The original body must be preserved.
        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(fetched.count == 1)
        #expect(fetched.first?.evidenceJSON == #"{"quote":"original body"}"#)
    }

    @Test("bd-1tl: insertEvidenceEvent silently dedupes on natural-key collision with a different body")
    func insertEvidenceEvent_silentDedupOnNaturalKeyBodyDrift() async throws {
        // bd-1tl: This test pins the relaxed contract that replaces the
        // earlier H3-1 "throw on natural-key body mismatch" behavior. The
        // H3-1 throw aborted the entire FM backfill job on the FIRST
        // collision in a real on-device run (iOS 26.4, 2026-04-07) because
        // the FM legitimately produces multiple `RefinedAdSpan` entries that
        // collapse onto the same `(asset, eventType, sourceType,
        // atomOrdinals, scanCohortJSON)` natural key with different bodies
        // (different commercialIntent / certainty / lineRefs). The natural
        // key represents "we already have a row for this logical evidence
        // event", so the dedup must be silent. Body integrity for true id
        // reuse is still enforced by `H-2: insertEvidenceEvent throws when
        // the same id reappears with a different body`.
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let first = EvidenceEvent(
            id: "evt-A",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"quote":"original body"}"#,
            scanCohortJSON: cohort,
            createdAt: 100
        )
        let second = EvidenceEvent(
            id: "evt-B",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"quote":"mutated body"}"#,
            scanCohortJSON: cohort,
            createdAt: 200
        )

        try await store.insertEvidenceEvent(first)
        // Must NOT throw — silent natural-key dedup.
        try await store.insertEvidenceEvent(second)

        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(fetched.count == 1, "natural-key dedup must collapse the two writes onto a single row")
        #expect(fetched.first?.id == "evt-A", "the first-written row wins; the second is dedup'd silently")
        #expect(fetched.first?.evidenceJSON == #"{"quote":"original body"}"#,
                "the existing row's body is preserved on dedup; the second body is discarded")
    }

    @Test("H3-1: insertEvidenceEvent is silently idempotent on natural-key collision with matching body")
    func insertEvidenceEvent_silentDedupOnNaturalKeyBodyMatch() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        // Same natural key, same evidenceJSON, distinct ids — legitimate
        // append dedup (e.g. a retried pipeline regenerated the id).
        let first = EvidenceEvent(
            id: "evt-A",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"quote":"same body"}"#,
            scanCohortJSON: cohort,
            createdAt: 100
        )
        let second = EvidenceEvent(
            id: "evt-B",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"quote":"same body"}"#,
            scanCohortJSON: cohort,
            createdAt: 200
        )

        try await store.insertEvidenceEvent(first)
        try await store.insertEvidenceEvent(second)

        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(fetched.count == 1, "idempotent natural-key dedup must keep exactly one row")
        #expect(fetched.first?.id == "evt-A")
    }

    @Test("bd-1tl: recordSemanticScanResult succeeds when two events collapse onto the same natural key")
    func recordSemanticScanResult_evidenceNaturalKeyCollisionIsTolerated() async throws {
        // bd-1tl regression. The on-device run on iOS 26.4 (2026-04-07)
        // failed with `AnalysisStoreError error 9` (evidenceEventBodyMismatch)
        // because BackfillJobRunner.runJob batched two evidence events into
        // one `recordSemanticScanResult` call where both events had the same
        // `(asset, eventType=fm.spanRefinement, sourceType=fm, atomOrdinals,
        // scanCohortJSON)` natural key (the FM produced two refined spans
        // covering the same atom range with different bodies). The H3-1
        // throw aborted the batch transaction and rolled back the scan row
        // along with both evidence events — net persistence = zero rows.
        //
        // After the bd-1tl fix the natural-key dedup is silent: the second
        // event collapses onto the first, the transaction commits, and the
        // scan row + the surviving evidence row both land on disk.
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let scanResult = SemanticScanResult(
            id: "scan-bd1tl",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 10,
            windowLastAtomOrdinal: 11,
            windowStartTime: 0,
            windowEndTime: 6,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: 100,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )
        // Two evidence events with the same atomOrdinals (= same natural
        // key) but different bodies — exactly the shape the FM produced on
        // device when it returned two `RefinedAdSpan` entries covering the
        // same line range with different commercialIntent / certainty.
        let evtFirst = EvidenceEvent(
            id: "evt-bd1tl-1",
            analysisAssetId: "asset-1",
            eventType: "fm.spanRefinement",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"commercialIntent":"paid","certainty":"strong"}"#,
            scanCohortJSON: cohort,
            createdAt: 100
        )
        let evtSecond = EvidenceEvent(
            id: "evt-bd1tl-2",
            analysisAssetId: "asset-1",
            eventType: "fm.spanRefinement",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"commercialIntent":"affiliate","certainty":"moderate"}"#,
            scanCohortJSON: cohort,
            createdAt: 101
        )

        // Must NOT throw — the natural-key dedup must be silent so the
        // surrounding transaction commits the scan row + first evidence row.
        try await store.recordSemanticScanResult(scanResult, evidenceEvents: [evtFirst, evtSecond])

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        #expect(scans.count == 1, "scan row must persist despite the natural-key collision in evidence events")
        #expect(scans.first?.id == "scan-bd1tl")

        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(evidence.count == 1, "the colliding events must collapse onto a single row")
        #expect(evidence.first?.id == "evt-bd1tl-1", "first-write wins; second is silently dedup'd")
    }

    @Test("H-2: insertEvidenceEvent with the identical body at the same id is idempotent")
    func evidenceEventIdenticalBodyIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let event = EvidenceEvent(
            id: "idemp-id",
            analysisAssetId: "asset-1",
            eventType: "windowQuoted",
            sourceType: .fm,
            atomOrdinals: "[10,11]",
            evidenceJSON: #"{"quote":"same body"}"#,
            scanCohortJSON: cohort,
            createdAt: 100
        )

        try await store.insertEvidenceEvent(event)
        try await store.insertEvidenceEvent(event)

        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(fetched.count == 1)
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

        // H-4: insert 2000 rows with varying cohorts and ordinals. Before
        // the fix, the reuse lookup filtered without using the
        // UNIQUE(reuseKeyHash) index and degraded to an O(n) scan; the
        // rewritten query hashes the tuple and hits the unique index for a
        // single lookup. 2000 rows keeps us well under 50ms either way for
        // the indexed path while catching an accidental regression to the
        // scan-order implementation.
        for i in 0..<2000 {
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

    // MARK: - Fix #4: cohort orphan row GC

    @Test("pruneOrphanedScansForCurrentCohort removes rows not matching current cohort")
    func pruneOrphanedScansForCurrentCohort_removesNonCurrentCohortRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        // Canonicalize both cohorts exactly as the store does on insert, so
        // the test compares against the same canonical form that lives in
        // the SQLite rows after persistence.
        let cohortARaw = try makeScanCohortJSON(promptHash: "cohort-A", schemaHash: "schema-A")
        let cohortBRaw = try makeScanCohortJSON(promptHash: "cohort-B", schemaHash: "schema-B")
        func canonicalize(_ raw: String) throws -> String {
            let decoded = try JSONDecoder().decode(ScanCohort.self, from: Data(raw.utf8))
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.sortedKeys]
            return String(decoding: try encoder.encode(decoded), as: UTF8.self)
        }
        let cohortA = try canonicalize(cohortARaw)
        let cohortB = try canonicalize(cohortBRaw)

        func scan(id: String, firstOrdinal: Int, cohort: String) -> SemanticScanResult {
            SemanticScanResult(
                id: id,
                analysisAssetId: "asset-1",
                windowFirstAtomOrdinal: firstOrdinal,
                windowLastAtomOrdinal: firstOrdinal + 4,
                windowStartTime: Double(firstOrdinal) * 10,
                windowEndTime: Double(firstOrdinal) * 10 + 40,
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
        }
        func event(id: String, ordinal: Int, cohort: String) -> EvidenceEvent {
            EvidenceEvent(
                id: id,
                analysisAssetId: "asset-1",
                eventType: "fm.windowScan",
                sourceType: .fm,
                atomOrdinals: "[\(ordinal)]",
                evidenceJSON: #"{"id":"\#(id)"}"#,
                scanCohortJSON: cohort,
                createdAt: Double(ordinal)
            )
        }

        // Insert 3 scan rows + 3 evidence events under cohort A.
        try await store.insertSemanticScanResult(scan(id: "a1", firstOrdinal: 0, cohort: cohortA))
        try await store.insertSemanticScanResult(scan(id: "a2", firstOrdinal: 10, cohort: cohortA))
        try await store.insertSemanticScanResult(scan(id: "a3", firstOrdinal: 20, cohort: cohortA))
        try await store.insertEvidenceEvent(event(id: "ae1", ordinal: 0, cohort: cohortA))
        try await store.insertEvidenceEvent(event(id: "ae2", ordinal: 1, cohort: cohortA))
        try await store.insertEvidenceEvent(event(id: "ae3", ordinal: 2, cohort: cohortA))

        // Insert 2 scan rows + 2 evidence events under cohort B.
        try await store.insertSemanticScanResult(scan(id: "b1", firstOrdinal: 30, cohort: cohortB))
        try await store.insertSemanticScanResult(scan(id: "b2", firstOrdinal: 40, cohort: cohortB))
        try await store.insertEvidenceEvent(event(id: "be1", ordinal: 30, cohort: cohortB))
        try await store.insertEvidenceEvent(event(id: "be2", ordinal: 31, cohort: cohortB))

        // Prune orphans relative to cohort B.
        let deleted = try await store.pruneOrphanedScansForCurrentCohort(
            currentScanCohortJSON: cohortB
        )

        #expect(deleted == 6, "3 scan-A + 3 evidence-A rows must be deleted")

        let scansRemaining = try await store.fetchSemanticScanResults(analysisAssetId: "asset-1")
        let eventsRemaining = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(scansRemaining.count == 2)
        #expect(Set(scansRemaining.map(\.id)) == ["b1", "b2"])
        #expect(eventsRemaining.count == 2)
        #expect(Set(eventsRemaining.map(\.id)) == ["be1", "be2"])
    }

    // MARK: - Fix #7: evidence insert createdAt drift idempotency

    @Test("insertEvidenceEvent idempotent despite createdAt float drift")
    func insertEvidenceEvent_idempotentDespiteCreatedAtDrift() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())

        let cohort = try makeScanCohortJSON()
        let first = EvidenceEvent(
            id: "ev-drift",
            analysisAssetId: "asset-1",
            eventType: "fm.windowScan",
            sourceType: .fm,
            atomOrdinals: "[1,2,3]",
            evidenceJSON: #"{"run":"same"}"#,
            scanCohortJSON: cohort,
            createdAt: 100
        )
        let drifted = EvidenceEvent(
            id: "ev-drift",
            analysisAssetId: "asset-1",
            eventType: "fm.windowScan",
            sourceType: .fm,
            atomOrdinals: "[1,2,3]",
            evidenceJSON: #"{"run":"same"}"#,
            scanCohortJSON: cohort,
            createdAt: 100.0001
        )

        try await store.insertEvidenceEvent(first)
        // Must not throw: createdAt drift alone is metadata and should not
        // cause the body-match probe to flag the row as mismatched.
        try await store.insertEvidenceEvent(drifted)

        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-1")
        #expect(fetched.count == 1)
        #expect(fetched.first?.id == "ev-drift")
    }

    // MARK: - Fix #9: length caps on errorContext / spansJSON

    private func makeLargeString(bytes: Int) -> String {
        String(repeating: "x", count: bytes)
    }

    @Test("insertSemanticScanResult rejects oversized errorContext")
    func insertSemanticScanResult_rejectsLargeErrorContext() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())
        let cohort = try makeScanCohortJSON()

        let oversized = makeLargeString(bytes: 1_500_000)
        let row = SemanticScanResult(
            id: "scan-large-err",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 4,
            windowStartTime: 0,
            windowEndTime: 40,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .abstain,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 1,
            errorContext: oversized,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertSemanticScanResult(row)
        }
    }

    @Test("insertSemanticScanResult rejects oversized spansJSON")
    func insertSemanticScanResult_rejectsLargeSpansJSON() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())
        let cohort = try makeScanCohortJSON()

        let oversized = makeLargeString(bytes: 1_500_000)
        let row = SemanticScanResult(
            id: "scan-large-spans",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 4,
            windowStartTime: 0,
            windowEndTime: 40,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: oversized,
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

        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertSemanticScanResult(row)
        }
    }

    @Test("insertSemanticScanResult accepts blobs under the 1MB cap")
    func insertSemanticScanResult_acceptsBlobsUnderCap() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makePersistenceTestAsset())
        let cohort = try makeScanCohortJSON()

        // 999_000 bytes — safely under the 1_000_000 byte cap.
        let justUnder = makeLargeString(bytes: 999_000)
        let row = SemanticScanResult(
            id: "scan-ok-large",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 4,
            windowStartTime: 0,
            windowEndTime: 40,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .abstain,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 1,
            errorContext: justUnder,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tx-v1"
        )

        try await store.insertSemanticScanResult(row)
        let fetched = try await store.fetchSemanticScanResult(id: row.id)
        #expect(fetched?.errorContext?.count == justUnder.count)
    }
}
