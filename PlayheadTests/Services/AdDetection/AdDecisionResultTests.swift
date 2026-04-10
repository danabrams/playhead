import Foundation
// AdDecisionResultTests.swift
// Phase 6 (playhead-4my.6.3): Round-trip persistence tests for the three
// decision/correction tables introduced in the v5 migration batch.

import Testing
@testable import Playhead

// MARK: - DecisionResultArtifact

@Suite("DecisionResultArtifact Persistence")
struct AdDecisionResultTests {

    private func makeStore() async throws -> AnalysisStore {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()
        return store
    }

    private func makeResult(
        id: String = "r1",
        assetId: String = "asset1"
    ) -> DecisionResultArtifact {
        DecisionResultArtifact(
            id: id,
            analysisAssetId: assetId,
            decisionCohortJSON: #"{"fusionHash":"fu1","policyHash":"p1"}"#,
            inputArtifactRefs: #"["span-a","span-b"]"#,
            decisionJSON: #"{"windows":[]}"#,
            createdAt: 1_000_000.0
        )
    }

    @Test("saveDecisionResultArtifact + loadDecisionResultArtifact round-trip")
    func roundTrip() async throws {
        let store = try await makeStore()
        let result = makeResult()
        try await store.saveDecisionResultArtifact(result)
        let loaded = try await store.loadDecisionResultArtifact(for: "asset1")
        #expect(loaded == result)
    }

    @Test("loadDecisionResultArtifact returns nil for unknown asset")
    func unknownAssetReturnsNil() async throws {
        let store = try await makeStore()
        let loaded = try await store.loadDecisionResultArtifact(for: "no-such-asset")
        #expect(loaded == nil)
    }

    @Test("saveDecisionResultArtifact is an upsert — second save replaces first")
    func upsertReplacesExisting() async throws {
        let store = try await makeStore()
        let first = makeResult(id: "r1")
        let second = DecisionResultArtifact(
            id: "r1",
            analysisAssetId: "asset1",
            decisionCohortJSON: #"{"fusionHash":"fu2"}"#,
            inputArtifactRefs: "[]",
            decisionJSON: #"{"windows":[1]}"#,
            createdAt: 2_000_000.0
        )
        try await store.saveDecisionResultArtifact(first)
        try await store.saveDecisionResultArtifact(second)
        let loaded = try await store.loadDecisionResultArtifact(for: "asset1")
        #expect(loaded?.decisionCohortJSON == #"{"fusionHash":"fu2"}"#)
    }

    // UNIQUE constraint on analysisAssetId means saving a second result for the same
    // asset (even with a different id) replaces the first row. At most one row ever
    // exists per asset. This test verifies the replacement happens even when ids differ.
    @Test("saveDecisionResultArtifact replaces old result even when id differs")
    func differentIdReplacesExistingAssetRow() async throws {
        let store = try await makeStore()
        let old = DecisionResultArtifact(
            id: "r-old", analysisAssetId: "asset1",
            decisionCohortJSON: "old", inputArtifactRefs: "[]",
            decisionJSON: "{}", createdAt: 1_000.0
        )
        let new = DecisionResultArtifact(
            id: "r-new", analysisAssetId: "asset1",
            decisionCohortJSON: "new", inputArtifactRefs: "[]",
            decisionJSON: "{}", createdAt: 2_000.0
        )
        try await store.saveDecisionResultArtifact(old)
        try await store.saveDecisionResultArtifact(new)
        let loaded = try await store.loadDecisionResultArtifact(for: "asset1")
        // Only one row survives (UNIQUE constraint enforces this); it is the newer save.
        #expect(loaded?.id == "r-new")
        #expect(loaded?.decisionCohortJSON == "new")
    }

    @Test("DecisionResultArtifact asset isolation")
    func assetIsolation() async throws {
        let store = try await makeStore()
        let a = DecisionResultArtifact(
            id: "ra", analysisAssetId: "asset-A",
            decisionCohortJSON: "cA", inputArtifactRefs: "[]",
            decisionJSON: "{}", createdAt: 1.0
        )
        let b = DecisionResultArtifact(
            id: "rb", analysisAssetId: "asset-B",
            decisionCohortJSON: "cB", inputArtifactRefs: "[]",
            decisionJSON: "{}", createdAt: 1.0
        )
        try await store.saveDecisionResultArtifact(a)
        try await store.saveDecisionResultArtifact(b)
        #expect(try await store.loadDecisionResultArtifact(for: "asset-A")?.id == "ra")
        #expect(try await store.loadDecisionResultArtifact(for: "asset-B")?.id == "rb")
    }
}

// MARK: - DecisionEvent

@Suite("DecisionEvent Persistence")
struct DecisionEventPersistenceTests {

    private func makeStore() async throws -> AnalysisStore {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()
        return store
    }

    private func makeEvent(
        id: String = "e1",
        assetId: String = "asset1",
        createdAt: Double = 1_000_000.0
    ) -> DecisionEvent {
        DecisionEvent(
            id: id,
            analysisAssetId: assetId,
            eventType: "fusion_decision",
            windowId: "w1",
            proposalConfidence: 0.72,
            skipConfidence: 0.72,
            eligibilityGate: "eligible",
            policyAction: "logOnly",
            decisionCohortJSON: #"{"fusionHash":"fu1"}"#,
            createdAt: createdAt
        )
    }

    @Test("appendDecisionEvent + loadDecisionEvents round-trip")
    func roundTrip() async throws {
        let store = try await makeStore()
        let event = makeEvent()
        try await store.appendDecisionEvent(event)
        let loaded = try await store.loadDecisionEvents(for: "asset1")
        #expect(loaded.count == 1)
        #expect(loaded[0] == event)
    }

    @Test("decision events are append-only — multiple events preserved in order")
    func appendOnly() async throws {
        let store = try await makeStore()
        let e1 = makeEvent(id: "e1", createdAt: 1.0)
        let e2 = makeEvent(id: "e2", createdAt: 2.0)
        try await store.appendDecisionEvent(e1)
        try await store.appendDecisionEvent(e2)
        let loaded = try await store.loadDecisionEvents(for: "asset1")
        #expect(loaded.count == 2)
        #expect(loaded[0].id == "e1")
        #expect(loaded[1].id == "e2")
    }

    @Test("loadDecisionEvents returns empty for unknown asset")
    func unknownAssetEmpty() async throws {
        let store = try await makeStore()
        let loaded = try await store.loadDecisionEvents(for: "no-such-asset")
        #expect(loaded.isEmpty)
    }

    @Test("DecisionEvent asset isolation")
    func assetIsolation() async throws {
        let store = try await makeStore()
        try await store.appendDecisionEvent(makeEvent(id: "eA", assetId: "asset-A"))
        try await store.appendDecisionEvent(makeEvent(id: "eB", assetId: "asset-B"))
        let eventsA = try await store.loadDecisionEvents(for: "asset-A")
        let eventsB = try await store.loadDecisionEvents(for: "asset-B")
        #expect(eventsA.count == 1)
        #expect(eventsA[0].id == "eA")
        #expect(eventsB.count == 1)
        #expect(eventsB[0].id == "eB")
    }
}

// MARK: - CorrectionEvent

@Suite("CorrectionEvent Persistence")
struct CorrectionEventPersistenceTests {

    private func makeStore() async throws -> AnalysisStore {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()
        return store
    }

    private func makeEvent(
        id: String = "c1",
        assetId: String = "asset1",
        createdAt: Double = 1_000_000.0
    ) -> CorrectionEvent {
        CorrectionEvent(
            id: id,
            analysisAssetId: assetId,
            scope: CorrectionScope.exactSpan(assetId: assetId, ordinalRange: 10...25).serialized,
            createdAt: createdAt
        )
    }

    @Test("appendCorrectionEvent + loadCorrectionEvents round-trip")
    func roundTrip() async throws {
        let store = try await makeStore()
        // Insert a parent asset first (FK constraint).
        try await store.insertAsset(AnalysisAsset(
            id: "asset1", episodeId: "ep1", assetFingerprint: "fp1",
            weakFingerprint: nil, sourceURL: "file:///tmp/ep1.m4a",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil
        ))
        let event = makeEvent()
        try await store.appendCorrectionEvent(event)
        let loaded = try await store.loadCorrectionEvents(analysisAssetId: "asset1")
        #expect(loaded.count == 1)
        #expect(loaded[0] == event)
    }

    @Test("correction events are append-only — multiple preserved in order")
    func appendOnly() async throws {
        let store = try await makeStore()
        try await store.insertAsset(AnalysisAsset(
            id: "asset1", episodeId: "ep1", assetFingerprint: "fp1",
            weakFingerprint: nil, sourceURL: "file:///tmp/ep1.m4a",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil
        ))
        try await store.appendCorrectionEvent(makeEvent(id: "c1", createdAt: 1.0))
        try await store.appendCorrectionEvent(makeEvent(id: "c2", createdAt: 2.0))
        let loaded = try await store.loadCorrectionEvents(analysisAssetId: "asset1")
        #expect(loaded.count == 2)
        #expect(loaded[0].id == "c1")
        #expect(loaded[1].id == "c2")
    }

    @Test("loadCorrectionEvents returns empty for unknown asset")
    func unknownAssetEmpty() async throws {
        let store = try await makeStore()
        let loaded = try await store.loadCorrectionEvents(analysisAssetId: "ghost")
        #expect(loaded.isEmpty)
    }

    @Test("CorrectionEvent asset isolation")
    func assetIsolation() async throws {
        let store = try await makeStore()
        for assetId in ["asset-A", "asset-B"] {
            try await store.insertAsset(AnalysisAsset(
                id: assetId, episodeId: "ep-\(assetId)", assetFingerprint: "fp-\(assetId)",
                weakFingerprint: nil, sourceURL: "file:///tmp/\(assetId).m4a",
                featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil, analysisState: "new",
                analysisVersion: 1, capabilitySnapshot: nil
            ))
        }
        try await store.appendCorrectionEvent(makeEvent(id: "cA", assetId: "asset-A"))
        try await store.appendCorrectionEvent(makeEvent(id: "cB", assetId: "asset-B"))
        let eventsA = try await store.loadCorrectionEvents(analysisAssetId: "asset-A")
        let eventsB = try await store.loadCorrectionEvents(analysisAssetId: "asset-B")
        #expect(eventsA.count == 1)
        #expect(eventsA[0].id == "cA")
        #expect(eventsB.count == 1)
        #expect(eventsB[0].id == "cB")
    }
}

// MARK: - Migration Idempotency

@Suite("AnalysisStore v5 migration idempotency")
struct MigrationIdempotencyTests {

    @Test("Calling migrate() twice on the same store is safe and does not corrupt tables")
    func migrateIsIdempotent() async throws {
        let store = try AnalysisStore(path: ":memory:")
        // First migrate
        try await store.migrate()
        // Second migrate — must not throw or corrupt
        try await store.migrate()

        // The decision tables from v5 must still exist and be writable.
        let result = DecisionResultArtifact(
            id: "idempotency-test",
            analysisAssetId: "asset-idem",
            decisionCohortJSON: "{}",
            inputArtifactRefs: "[]",
            decisionJSON: "[]",
            createdAt: 1.0
        )
        try await store.saveDecisionResultArtifact(result)
        let loaded = try await store.loadDecisionResultArtifact(for: "asset-idem")
        #expect(loaded?.id == "idempotency-test", "Double migration should not break table writes")
    }

    @Test("Second migrate() call on a fully migrated store is a no-op and does not corrupt ad_windows")
    func doubleMigrateDoesNotCorruptAdWindows() async throws {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()
        // Second call: schemaVersion >= 5 guards should all fire, making this a pure no-op.
        try await store.migrate()

        // Verify the v5 ad_windows columns (evidenceSources, eligibilityGate) are still writable.
        let asset = AnalysisAsset(
            id: "idem-asset", episodeId: "ep", assetFingerprint: "fp",
            weakFingerprint: nil, sourceURL: "file:///tmp/test.m4a",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new", analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)
        let window = AdWindow(
            id: "w1", analysisAssetId: "idem-asset",
            startTime: 10, endTime: 40, confidence: 0.8,
            boundaryState: "acousticRefined", decisionState: "confirmed",
            detectorVersion: "test-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 10,
            metadataSource: "fusion-v1", metadataConfidence: nil,
            metadataPromptVersion: nil, wasSkipped: false, userDismissedBanner: false,
            evidenceSources: "classifier,lexical", eligibilityGate: "eligible"
        )
        try await store.insertAdWindow(window)
        let windows = try await store.fetchAdWindows(assetId: "idem-asset")
        #expect(windows.count == 1)
        #expect(windows[0].evidenceSources == "classifier,lexical")
    }
}
