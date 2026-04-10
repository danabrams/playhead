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
            correctionScope: "window",
            atomOrdinalRange: "[10, 25]",
            evidenceJSON: #"{"reason":"not_an_ad"}"#,
            createdAt: createdAt
        )
    }

    @Test("appendCorrectionEvent + loadCorrectionEvents round-trip")
    func roundTrip() async throws {
        let store = try await makeStore()
        let event = makeEvent()
        try await store.appendCorrectionEvent(event)
        let loaded = try await store.loadCorrectionEvents(for: "asset1")
        #expect(loaded.count == 1)
        #expect(loaded[0] == event)
    }

    @Test("correction events are append-only — multiple preserved in order")
    func appendOnly() async throws {
        let store = try await makeStore()
        try await store.appendCorrectionEvent(makeEvent(id: "c1", createdAt: 1.0))
        try await store.appendCorrectionEvent(makeEvent(id: "c2", createdAt: 2.0))
        let loaded = try await store.loadCorrectionEvents(for: "asset1")
        #expect(loaded.count == 2)
        #expect(loaded[0].id == "c1")
        #expect(loaded[1].id == "c2")
    }

    @Test("loadCorrectionEvents returns empty for unknown asset")
    func unknownAssetEmpty() async throws {
        let store = try await makeStore()
        let loaded = try await store.loadCorrectionEvents(for: "ghost")
        #expect(loaded.isEmpty)
    }

    @Test("CorrectionEvent asset isolation")
    func assetIsolation() async throws {
        let store = try await makeStore()
        try await store.appendCorrectionEvent(makeEvent(id: "cA", assetId: "asset-A"))
        try await store.appendCorrectionEvent(makeEvent(id: "cB", assetId: "asset-B"))
        let eventsA = try await store.loadCorrectionEvents(for: "asset-A")
        let eventsB = try await store.loadCorrectionEvents(for: "asset-B")
        #expect(eventsA.count == 1)
        #expect(eventsA[0].id == "cA")
        #expect(eventsB.count == 1)
        #expect(eventsB[0].id == "cB")
    }
}
