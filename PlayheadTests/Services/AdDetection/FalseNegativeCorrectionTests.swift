// FalseNegativeCorrectionTests.swift
// playhead-p95: Tests for false negative correction data model,
// correctionBoostFactor, and CorrectionSource.falseNegative.

import Foundation
import Testing
@testable import Playhead

// MARK: - CorrectionSource.falseNegative Data Model

@Suite("CorrectionSource.falseNegative — Data Model")
struct FalseNegativeDataModelTests {

    @Test("CorrectionSource.falseNegative raw value round-trips")
    func falseNegativeRawValueRoundTrips() {
        let source = CorrectionSource.falseNegative
        #expect(source.rawValue == "falseNegative")
        #expect(CorrectionSource(rawValue: "falseNegative") == .falseNegative)
    }

    @Test("CorrectionSource.falseNegative kind is .falseNegative")
    func falseNegativeKind() {
        #expect(CorrectionSource.falseNegative.kind == .falseNegative)
    }

    @Test("CorrectionSource.listenRevert kind is .falsePositive")
    func listenRevertKind() {
        #expect(CorrectionSource.listenRevert.kind == .falsePositive)
    }

    @Test("CorrectionSource.manualVeto kind is .falsePositive")
    func manualVetoKind() {
        #expect(CorrectionSource.manualVeto.kind == .falsePositive)
    }

    @Test("CorrectionEvent with .falseNegative source persists and loads")
    func falseNegativeEventPersistsAndLoads() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fn-1"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-fn-1",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-fn-1",
                ordinalRange: 10...50
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative,
            podcastId: "podcast-fn"
        )
        try await correctionStore.record(event)

        let loaded = try await correctionStore.activeCorrections(for: "asset-fn-1")
        #expect(loaded.count == 1)
        #expect(loaded[0].source == .falseNegative)
        #expect(loaded[0].podcastId == "podcast-fn")
    }

    @Test("CorrectionEvent with .falseNegative Codable round-trip")
    func falseNegativeCodableRoundTrip() throws {
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        let original = CorrectionSource.falseNegative
        let data = try encoder.encode(original)
        let decoded = try decoder.decode(CorrectionSource.self, from: data)
        #expect(decoded == original)
    }
}

// MARK: - correctionBoostFactor

@Suite("correctionBoostFactor — False Negative Boost")
struct CorrectionBoostFactorTests {

    @Test("No corrections → boostFactor = 1.0")
    func noCorrectionsYieldsNoBoost() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-boost-none"))

        let factor = await correctionStore.correctionBoostFactor(for: "asset-boost-none")
        #expect(factor == 1.0, "No corrections should yield boost factor 1.0")
    }

    @Test("Fresh false negative correction → boostFactor ≈ 2.0")
    func freshFalseNegativeYieldsMaxBoost() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-boost-fresh"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-boost-fresh",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-boost-fresh",
                ordinalRange: 0...100
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative
        )
        try await correctionStore.record(event)

        let factor = await correctionStore.correctionBoostFactor(for: "asset-boost-fresh")
        #expect(factor > 1.9 && factor <= 2.0,
                "Fresh false negative should yield boost factor near 2.0, got \(factor)")
    }

    @Test("90-day-old false negative → boostFactor ≈ 1.5")
    func decayedFalseNegativeYieldsPartialBoost() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-boost-decayed"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-boost-decayed",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-boost-decayed",
                ordinalRange: 0...100
            ).serialized,
            createdAt: Date().addingTimeInterval(-90 * 86400).timeIntervalSince1970,
            source: .falseNegative
        )
        try await correctionStore.record(event)

        let factor = await correctionStore.correctionBoostFactor(for: "asset-boost-decayed")
        // 90-day decay: weight = 0.5, boost = 1.0 + 0.5 = 1.5
        #expect(abs(factor - 1.5) < 0.05,
                "90-day-old false negative should yield boost ≈ 1.5, got \(factor)")
    }

    @Test("False positive corrections do NOT contribute to boostFactor")
    func falsePositiveDoesNotBoost() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-boost-fp"))

        // Record a false positive (manualVeto) correction.
        let event = CorrectionEvent(
            analysisAssetId: "asset-boost-fp",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-boost-fp",
                ordinalRange: 0...100
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        let factor = await correctionStore.correctionBoostFactor(for: "asset-boost-fp")
        #expect(factor == 1.0,
                "False positive correction must not boost; got \(factor)")
    }

    @Test("Mixed corrections: only false negatives contribute to boost")
    func mixedCorrectionsOnlyFalseNegativesBoost() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-boost-mixed"))

        let now = Date()

        // False positive (should not contribute to boost).
        let fpEvent = CorrectionEvent(
            analysisAssetId: "asset-boost-mixed",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-boost-mixed",
                ordinalRange: 0...50
            ).serialized,
            createdAt: now.timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(fpEvent)

        // False negative (should contribute to boost).
        let fnEvent = CorrectionEvent(
            analysisAssetId: "asset-boost-mixed",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-boost-mixed",
                ordinalRange: 50...100
            ).serialized,
            createdAt: now.timeIntervalSince1970,
            source: .falseNegative
        )
        try await correctionStore.record(fnEvent)

        let factor = await correctionStore.correctionBoostFactor(for: "asset-boost-mixed")
        #expect(factor > 1.9, "Fresh false negative should dominate boost, got \(factor)")
    }

    @Test("NoOpUserCorrectionStore returns boostFactor 1.0")
    func noOpStoreReturnsNoBoost() async {
        let store = NoOpUserCorrectionStore()
        let factor = await store.correctionBoostFactor(for: "any-asset")
        #expect(factor == 1.0)
    }
}

// MARK: - False Negative + Passthrough Factor Independence

@Suite("False Negative + Passthrough Factor Independence")
struct FalseNegativePassthroughIndependenceTests {

    @Test("False negative correction does NOT affect passthrough factor")
    func falseNegativeDoesNotSuppressPassthrough() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-indep"))

        // Record a false negative correction.
        let event = CorrectionEvent(
            analysisAssetId: "asset-indep",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-indep",
                ordinalRange: 0...100
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative
        )
        try await correctionStore.record(event)

        let passthrough = await correctionStore.correctionPassthroughFactor(for: "asset-indep")
        let boost = await correctionStore.correctionBoostFactor(for: "asset-indep")

        // False negatives must NOT suppress passthrough — they are "missed ad" reports
        // and suppressing detection is the opposite of what the user wants.
        // Only false positive corrections (manualVeto, listenRevert) should suppress.
        #expect(passthrough == 1.0, "False negative must not suppress passthrough; got \(passthrough)")
        #expect(boost > 1.9, "False negative yields boost")
    }
}

// MARK: - Legacy nil-source corrections

@Suite("Legacy nil-source corrections — Passthrough/Boost interaction")
struct LegacyNilSourceCorrectionTests {

    @Test("nil-source corrections count as false positives in passthrough factor")
    func nilSourceIncludedInPassthrough() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-legacy"))

        // Record a correction with nil source (legacy pre-false-negative feature).
        let event = CorrectionEvent(
            analysisAssetId: "asset-legacy",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-legacy",
                ordinalRange: 0...50
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: nil
        )
        try await correctionStore.record(event)

        let passthrough = await correctionStore.correctionPassthroughFactor(for: "asset-legacy")
        let boost = await correctionStore.correctionBoostFactor(for: "asset-legacy")

        // Legacy corrections must suppress (passthrough < 1.0) — they were all false-positive vetoes.
        #expect(passthrough < 0.1, "nil-source legacy correction must suppress; got \(passthrough)")
        // Legacy corrections must NOT boost — they are not false negatives.
        #expect(boost == 1.0, "nil-source must not boost; got \(boost)")
    }
}

// MARK: - Combined passthrough * boost

@Suite("Combined correction factor — passthrough * boost")
struct CombinedCorrectionFactorTests {

    @Test("Mixed FP + FN corrections produce combined factor")
    func mixedCorrectionsProduceCombinedFactor() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-mixed"))

        // Record a false positive correction (suppression).
        let fpEvent = CorrectionEvent(
            analysisAssetId: "asset-mixed",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-mixed",
                ordinalRange: 0...50
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(fpEvent)

        // Record a false negative correction (boost).
        let fnEvent = CorrectionEvent(
            analysisAssetId: "asset-mixed",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-mixed",
                ordinalRange: 0...100
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative
        )
        try await correctionStore.record(fnEvent)

        let passthrough = await correctionStore.correctionPassthroughFactor(for: "asset-mixed")
        let boost = await correctionStore.correctionBoostFactor(for: "asset-mixed")
        let combined = passthrough * boost

        // Both factors are active independently.
        #expect(passthrough < 0.1, "FP correction must suppress; got \(passthrough)")
        #expect(boost > 1.9, "FN correction must boost; got \(boost)")
        // Combined factor: near 0 * near 2 ≈ near 0. FP suppression dominates.
        #expect(combined < 0.2, "Combined factor should be suppression-dominated; got \(combined)")
    }
}

// MARK: - Test Helpers (local to this file)

private func makeTestAsset(id: String) -> AnalysisAsset {
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
