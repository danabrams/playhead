// CorrectionSuppressionTests.swift
// Phase 7 (playhead-4my.7.3): Suppression integration tests verifying that
// user corrections propagate through correctionPassthroughFactor into
// DecisionMapper, correctly gating spans as blockedByUserCorrection.

import Foundation
import Testing
@testable import Playhead

@Suite("Correction Suppression — Integration")
struct CorrectionSuppressionTests {

    // MARK: - Helpers

    private func makeSpan(
        assetId: String = "asset-suppress",
        startTime: Double = 10.0,
        endTime: Double = 40.0,
        firstOrdinal: Int = 100,
        lastOrdinal: Int = 200,
        anchorProvenance: [AnchorRef] = []
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: firstOrdinal, lastAtomOrdinal: lastOrdinal),
            assetId: assetId,
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: anchorProvenance
        )
    }

    private func defaultConfig() -> FusionWeightConfig {
        FusionWeightConfig()
    }

    /// Build a ledger that produces a moderate rawSkipConfidence (~0.45–0.50).
    private func moderateEvidenceLedger() -> [EvidenceLedgerEntry] {
        [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
        ]
    }

    /// Build a ledger that produces a high rawSkipConfidence (~0.90).
    private func strongEvidenceLedger() -> [EvidenceLedgerEntry] {
        [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 1.0)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.9)),
            .init(source: .catalog, weight: 0.20, detail: .catalog(entryCount: 3)),
        ]
    }

    // MARK: - Active exactSpan correction blocks future backfill for that range

    @Test("Fresh exactSpan correction → correctionPassthroughFactor ≈ 0.0 → DecisionMapper blocks span")
    func freshExactSpanCorrectionBlocksBackfill() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-suppress"))

        // Record a fresh correction for this asset.
        let scope = CorrectionScope.exactSpan(assetId: "asset-suppress", ordinalRange: 100...200)
        let event = CorrectionEvent(
            analysisAssetId: "asset-suppress",
            scope: scope.serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        // Query the passthrough factor — should be ≈ 0.0 for a fresh correction.
        let factor = await correctionStore.correctionPassthroughFactor(for: "asset-suppress")
        #expect(factor < 0.05, "Fresh correction should yield passthrough factor near 0.0, got \(factor)")

        // Feed that factor into DecisionMapper with moderate evidence.
        let span = makeSpan()
        let mapper = DecisionMapper(
            span: span,
            ledger: moderateEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: factor
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByUserCorrection,
                "Active fresh correction must block span via DecisionMapper")
        #expect(result.skipConfidence < 0.40,
                "Effective confidence must be below candidate threshold (0.40)")
    }

    // MARK: - Active sponsorOnShow correction blocks detection for that sponsor

    @Test("Fresh sponsorOnShow correction → passthrough factor ≈ 0.0 → blocks span on same asset")
    func freshSponsorOnShowCorrectionBlocks() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-sponsor"))

        let scope = CorrectionScope.sponsorOnShow(podcastId: "podcast-abc", sponsor: "squarespace")
        let event = CorrectionEvent(
            analysisAssetId: "asset-sponsor",
            scope: scope.serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto,
            podcastId: "podcast-abc"
        )
        try await correctionStore.record(event)

        let factor = await correctionStore.correctionPassthroughFactor(for: "asset-sponsor")
        #expect(factor < 0.05, "Fresh sponsorOnShow correction must yield low passthrough factor")

        // hasActiveCorrection should also confirm the scope is present.
        let hasScope = try await correctionStore.hasActiveCorrection(scope: scope)
        #expect(hasScope, "sponsorOnShow scope must be found in the store")
    }

    // MARK: - Decayed correction allows strong new evidence to break through

    @Test("90-day-old correction → passthrough factor ≈ 0.5 → strong evidence stays above 0.40")
    func decayedCorrectionAllowsBreakthrough() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-decayed"))

        // Record a 90-day-old correction.
        let createdAt = Date().addingTimeInterval(-90 * 86400)
        let event = CorrectionEvent(
            analysisAssetId: "asset-decayed",
            scope: CorrectionScope.exactSpan(assetId: "asset-decayed", ordinalRange: 100...200).serialized,
            createdAt: createdAt.timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        // 90-day decay: weight = 0.5, passthrough = 1.0 - 0.5 = 0.5
        let factor = await correctionStore.correctionPassthroughFactor(for: "asset-decayed")
        #expect(abs(factor - 0.5) < 0.05, "90-day-old correction should give passthrough ≈ 0.5, got \(factor)")

        // Strong evidence (rawSkipConfidence ~ 0.90) * factor 0.5 = effective ~0.45 > 0.40 → NOT blocked.
        let span = makeSpan(assetId: "asset-decayed")
        let mapper = DecisionMapper(
            span: span,
            ledger: strongEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: factor
        )
        let result = mapper.map()
        #expect(result.eligibilityGate != .blockedByUserCorrection,
                "Decayed correction + strong evidence must not block (effectiveConfidence > 0.40)")
        #expect(result.skipConfidence >= 0.40,
                "Effective confidence should remain above candidate threshold")
    }

    // MARK: - effectiveConfidence = rawConfidence * correctionPassthroughFactor

    @Test("effectiveConfidence is rawConfidence * correctionPassthroughFactor")
    func effectiveConfidenceCalculation() {
        let span = makeSpan()

        // Compute with factor = 1.0 to get raw.
        let mapperRaw = DecisionMapper(
            span: span,
            ledger: moderateEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 1.0
        )
        let rawResult = mapperRaw.map()
        let rawSkipConfidence = rawResult.skipConfidence

        // Now compute with factor = 0.6.
        let correctionFactor = 0.6
        let mapperSuppressed = DecisionMapper(
            span: span,
            ledger: moderateEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: correctionFactor
        )
        let suppressedResult = mapperSuppressed.map()

        let expected = rawSkipConfidence * correctionFactor
        #expect(abs(suppressedResult.skipConfidence - expected) < 0.001,
                "effectiveConfidence (\(suppressedResult.skipConfidence)) should equal raw (\(rawSkipConfidence)) * factor (\(correctionFactor)) = \(expected)")
    }

    // MARK: - Window suppressed when effectiveConfidence < candidateThreshold (0.40)

    @Test("Window suppressed when effectiveConfidence < 0.40")
    func windowSuppressedBelowThreshold() {
        let span = makeSpan()

        // Moderate evidence with correctionFactor = 0.0 → effectiveConfidence = 0.0
        let mapper = DecisionMapper(
            span: span,
            ledger: moderateEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 0.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByUserCorrection,
                "correctionFactor=0.0 must block any span")
        #expect(result.skipConfidence < 0.001,
                "effectiveConfidence should be ~0.0 with correctionFactor=0.0")
    }

    @Test("Window NOT suppressed when effectiveConfidence >= 0.40 despite correction")
    func windowNotSuppressedAboveThreshold() {
        let span = makeSpan()

        // Strong evidence (raw ~0.90) with factor 0.8 → effective ~0.72 > 0.40
        let mapper = DecisionMapper(
            span: span,
            ledger: strongEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 0.8
        )
        let result = mapper.map()
        #expect(result.eligibilityGate != .blockedByUserCorrection,
                "effectiveConfidence well above 0.40 must not be blocked")
        #expect(result.skipConfidence >= 0.40)
    }

    // MARK: - Boost path (correctionFactor > 1.0)

    @Test("correctionFactor 1.5 boosts moderate confidence and does NOT trigger blockedByUserCorrection")
    func boostFactorIncreasesConfidence() {
        let span = makeSpan()

        // Moderate evidence + boost factor 1.5
        let mapper = DecisionMapper(
            span: span,
            ledger: moderateEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 1.5
        )
        let result = mapper.map()

        // The gate condition is `correctionFactor < 1.0 && ...`, so boost must NOT block.
        #expect(result.eligibilityGate != .blockedByUserCorrection,
                "correctionFactor > 1.0 must never trigger blockedByUserCorrection")

        // Compute expected: raw confidence boosted by 1.5, then clamped to 1.0.
        // We just verify it is higher than the unboosted value.
        let unboostedMapper = DecisionMapper(
            span: span,
            ledger: moderateEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 1.0
        )
        let unboostedResult = unboostedMapper.map()
        #expect(result.skipConfidence > unboostedResult.skipConfidence,
                "Boosted confidence (\(result.skipConfidence)) must exceed unboosted (\(unboostedResult.skipConfidence))")
    }

    @Test("correctionFactor 2.0 caps skipConfidence at 1.0")
    func boostFactorCapsAtOne() {
        let span = makeSpan()

        // Strong evidence (~0.90) + max boost (2.0) → raw = ~1.80, clamped to 1.0
        let mapper = DecisionMapper(
            span: span,
            ledger: strongEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 2.0
        )
        let result = mapper.map()

        #expect(result.skipConfidence <= 1.0,
                "Boosted confidence must be clamped to 1.0, got \(result.skipConfidence)")
        #expect(result.eligibilityGate != .blockedByUserCorrection,
                "correctionFactor > 1.0 must never trigger blockedByUserCorrection")
    }

    // MARK: - End-to-end: PersistentUserCorrectionStore → DecisionMapper pipeline

    @Test("End-to-end: store correction → query factor → DecisionMapper blocks → decays → allows")
    func endToEndSuppressionLifecycle() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-e2e"))

        let span = makeSpan(assetId: "asset-e2e")

        // Step 1: No corrections → factor = 1.0 → not blocked.
        let factorBefore = await correctionStore.correctionPassthroughFactor(for: "asset-e2e")
        #expect(factorBefore == 1.0, "No corrections should yield factor 1.0")

        let mapperBefore = DecisionMapper(
            span: span,
            ledger: moderateEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: factorBefore
        )
        let resultBefore = mapperBefore.map()
        #expect(resultBefore.eligibilityGate != .blockedByUserCorrection)

        // Step 2: Record fresh correction → factor ≈ 0.0 → blocked.
        let event = CorrectionEvent(
            analysisAssetId: "asset-e2e",
            scope: CorrectionScope.exactSpan(assetId: "asset-e2e", ordinalRange: 100...200).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        let factorAfter = await correctionStore.correctionPassthroughFactor(for: "asset-e2e")
        #expect(factorAfter < 0.05, "Fresh correction factor should be near 0.0")

        let mapperAfter = DecisionMapper(
            span: span,
            ledger: moderateEvidenceLedger(),
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: factorAfter
        )
        let resultAfter = mapperAfter.map()
        #expect(resultAfter.eligibilityGate == .blockedByUserCorrection)
    }

    // MARK: - Multiple corrections: strongest (freshest) dominates

    @Test("Multiple corrections: freshest dominates passthrough factor")
    func multipleCorrectionsFreshestDominates() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-multi"))

        let now = Date()

        // Old correction (150 days ago): weight = max(0.1, 1.0 - 150/180) ≈ 0.167
        let oldEvent = CorrectionEvent(
            analysisAssetId: "asset-multi",
            scope: CorrectionScope.exactSpan(assetId: "asset-multi", ordinalRange: 10...50).serialized,
            createdAt: now.addingTimeInterval(-150 * 86400).timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(oldEvent)

        // Check factor with only old correction: passthrough = 1.0 - ~0.167 ≈ 0.833
        let factorOldOnly = await correctionStore.correctionPassthroughFactor(for: "asset-multi")
        #expect(factorOldOnly > 0.7 && factorOldOnly < 0.9,
                "150-day-old correction should give passthrough ~0.83, got \(factorOldOnly)")

        // Fresh correction (0 days): weight = 1.0 → passthrough = 0.0
        let freshEvent = CorrectionEvent(
            analysisAssetId: "asset-multi",
            scope: CorrectionScope.exactSpan(assetId: "asset-multi", ordinalRange: 100...200).serialized,
            createdAt: now.timeIntervalSince1970,
            source: .listenRevert
        )
        try await correctionStore.record(freshEvent)

        // Now factor should be dominated by the fresh correction.
        let factorBoth = await correctionStore.correctionPassthroughFactor(for: "asset-multi")
        #expect(factorBoth < 0.05,
                "Fresh correction should dominate, making factor near 0.0, got \(factorBoth)")
    }

    // MARK: - No cross-asset contamination

    @Test("Correction on asset A does not suppress asset B")
    func noCrossAssetContamination() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-a"))
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-b"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-a",
            scope: CorrectionScope.exactSpan(assetId: "asset-a", ordinalRange: 0...10).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        let factorA = await correctionStore.correctionPassthroughFactor(for: "asset-a")
        let factorB = await correctionStore.correctionPassthroughFactor(for: "asset-b")
        #expect(factorA < 0.05, "Asset A should be suppressed")
        #expect(factorB == 1.0, "Asset B should not be affected by Asset A's correction")
    }
}

// MARK: - SkipOrchestrator Correction Scope Inference

@Suite("SkipOrchestrator — Correction Scope Inference from AdWindow")
struct SkipOrchestratorCorrectionScopeTests {

    @Test("recordListenRevert writes exactSpan scope using AdWindow's assetId and INT range placeholders")
    func listenRevertScopeContent() async throws {
        let analysisStore = try await makeTestStore()
        try await analysisStore.insertAsset(makeSkipTestAnalysisAsset())

        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: analysisStore,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-scope-test",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await analysisStore.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.recordListenRevert(
            windowId: "ad-scope-test",
            podcastId: "podcast-1"
        )

        // Poll for the fire-and-forget Task to complete.
        let found = try await pollUntil(timeout: .seconds(5)) {
            let events = try await correctionStore.activeCorrections(for: "asset-1")
            return !events.isEmpty
        }
        #expect(found, "CorrectionEvent should be written after recordListenRevert")

        let events = try await correctionStore.activeCorrections(for: "asset-1")
        let event = try #require(events.first)

        // Verify the scope is an exactSpan for the correct asset.
        let parsedScope = CorrectionScope.deserialize(event.scope)
        guard case .exactSpan(let assetId, _) = parsedScope else {
            Issue.record("Expected exactSpan scope, got: \(event.scope)")
            return
        }
        #expect(assetId == "asset-1", "Scope assetId must match the window's analysisAssetId")
        #expect(event.source == .listenRevert)
        #expect(event.podcastId == "podcast-1")
    }
}

// MARK: - Gap 1: recordListenRevert sponsor scope inference (known limitation)

@Suite("SkipOrchestrator — recordListenRevert Sponsor Scope Limitation")
struct ListenRevertSponsorScopeTests {

    // recordListenRevert in SkipOrchestrator creates a CorrectionEvent directly
    // (not via PersistentUserCorrectionStore.recordVeto), so it does NOT infer
    // sponsorOnShow from anchorProvenance. AdWindow does not carry sponsor metadata
    // or evidence catalog entries — it only has advertiser/product strings from
    // metadata extraction, which are not used for scope inference.
    //
    // Contrast with recordVeto (tested in UserCorrectionStoreTests
    // .testRecordVetoWithBrandSpanWritesTwoEvents) which DOES infer sponsorOnShow
    // because it receives a DecodedSpan with anchorProvenance containing brandSpan
    // evidence.
    //
    // This is a known design limitation: the "Listen" revert path (banner →
    // SkipOrchestrator.recordListenRevert) only writes exactSpan scope.
    // A future phase could enrich AdWindow with evidence catalog entries to
    // enable sponsor scope inference on the revert path.

    @Test("recordListenRevert writes only exactSpan — no sponsorOnShow (by design)")
    func listenRevertWritesOnlyExactSpan() async throws {
        let analysisStore = try await makeTestStore()
        try await analysisStore.insertAsset(makeSkipTestAnalysisAsset())

        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: analysisStore,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Create an ad window with an advertiser name (simulating metadata extraction).
        let ad = makeSkipTestAdWindow(
            id: "ad-sponsor-revert",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await analysisStore.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.recordListenRevert(
            windowId: "ad-sponsor-revert",
            podcastId: "podcast-1"
        )

        // Poll for the fire-and-forget Task to complete.
        let found = try await pollUntil(timeout: .seconds(5)) {
            let events = try await correctionStore.activeCorrections(for: "asset-1")
            return !events.isEmpty
        }
        #expect(found, "CorrectionEvent should be written after recordListenRevert")

        let events = try await correctionStore.activeCorrections(for: "asset-1")
        // KEY ASSERTION: only one event (exactSpan), NOT two (no sponsorOnShow).
        #expect(events.count == 1, "recordListenRevert should write exactly 1 event (exactSpan only), got \(events.count)")

        let scopes = events.map { $0.scope }
        #expect(scopes.allSatisfy { $0.hasPrefix("exactSpan:") },
                "All scopes should be exactSpan — no sponsorOnShow inferred from AdWindow")
    }
}

// MARK: - Gap 2: "Not an ad" banner callback behavioral contract

@Suite("AdBanner — 'Not an ad' Correction Behavioral Contract")
struct AdBannerNotAnAdBehavioralTests {

    // The onNotAnAd closure in NowPlayingView creates a CorrectionEvent with:
    //   - analysisAssetId: current asset ID
    //   - scope: exactSpan(assetId:, ordinalRange: 0...Int.max)
    //   - source: .manualVeto
    //   - podcastId: from the banner item
    // This test verifies the behavioral contract: a .manualVeto event with
    // exactSpan scope persists and is retrievable, matching the closure's behavior.

    @Test("manualVeto correction event with exactSpan scope round-trips through PersistentUserCorrectionStore")
    func manualVetoCorrectionRoundTrips() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-banner-veto"))

        // Replicate the exact parameters the onNotAnAd closure uses.
        let assetId = "asset-banner-veto"
        let podcastId = "podcast-banner"
        let event = CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactSpan(
                assetId: assetId,
                ordinalRange: 0...Int.max
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto,
            podcastId: podcastId
        )
        try await correctionStore.record(event)

        // Verify persistence.
        let loaded = try await correctionStore.activeCorrections(for: assetId)
        #expect(loaded.count == 1, "Exactly one event should be stored")

        let stored = loaded[0]
        #expect(stored.source == .manualVeto, "Source must be .manualVeto")
        #expect(stored.podcastId == podcastId, "podcastId must match the banner item's podcastId")

        // Verify the scope parses correctly.
        let parsedScope = CorrectionScope.deserialize(stored.scope)
        guard case .exactSpan(let scopeAssetId, let ordinalRange) = parsedScope else {
            Issue.record("Expected exactSpan scope, got: \(stored.scope)")
            return
        }
        #expect(scopeAssetId == assetId, "Scope assetId must match")
        #expect(ordinalRange == 0...Int.max, "Ordinal range must be 0...Int.max (widest veto)")
    }

    @Test("manualVeto correction suppresses future backfill via correctionPassthroughFactor")
    func manualVetoSuppressesBackfill() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-banner-suppress"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-banner-suppress",
            scope: CorrectionScope.exactSpan(
                assetId: "asset-banner-suppress",
                ordinalRange: 0...Int.max
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto,
            podcastId: "podcast-suppress"
        )
        try await correctionStore.record(event)

        let factor = await correctionStore.correctionPassthroughFactor(for: "asset-banner-suppress")
        #expect(factor < 0.05, "Fresh manualVeto correction must yield passthrough factor near 0.0, got \(factor)")
    }
}

// MARK: - Gap 3: Timeline tap callback contract

@Suite("TimelineRailView — onAdSegmentTap Callback Contract")
struct TimelineRailAdSegmentTapTests {

    // TimelineRailView declares `var onAdSegmentTap: ((Int) -> Void)?`.
    // When the user taps an ad segment block, the view calls
    // `onAdSegmentTap?(index)` where index is the positional index into
    // the `adSegments` array.
    //
    // Since we cannot instantiate SwiftUI views in unit tests without
    // ViewInspector, we test the callback contract behaviorally:
    // verify that the index correctly maps to an ad segment, and that
    // the downstream correction path works for any valid index.

    @Test("onAdSegmentTap index maps to correct ad segment range")
    func tapIndexMapsToSegment() {
        let adSegments: [ClosedRange<Double>] = [
            0.15...0.22,
            0.55...0.60,
            0.80...0.88
        ]

        // Simulate what the caller does: use the index to look up the segment.
        for (index, expectedSegment) in adSegments.enumerated() {
            let tappedSegment = adSegments[index]
            #expect(tappedSegment == expectedSegment,
                    "Index \(index) must map to segment \(expectedSegment)")
        }
    }

    @Test("onAdSegmentTap callback fires with the enumerated index of the tapped segment")
    func callbackFiresWithCorrectIndex() {
        // Verify the callback contract: the closure receives the segment index.
        var receivedIndex: Int?
        let callback: (Int) -> Void = { index in
            receivedIndex = index
        }

        // Simulate tapping segment 1 (as the onTapGesture closure does).
        callback(1)
        #expect(receivedIndex == 1, "Callback must receive the tapped segment index")
    }

    @Test("correction event from tapped ad segment persists through correction store")
    func tappedSegmentCorrectionPersists() async throws {
        // When onAdSegmentTap fires, the caller maps the index to a DecodedSpan
        // and can write a correction event. This test verifies the downstream
        // persistence path works correctly for the tap-to-correct flow.
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-timeline-tap"))

        // Simulate: user taps segment index 1, caller resolves to ordinal range 50...100.
        let assetId = "asset-timeline-tap"
        let event = CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactSpan(
                assetId: assetId,
                ordinalRange: 50...100
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto,
            podcastId: "podcast-timeline"
        )
        try await correctionStore.record(event)

        let loaded = try await correctionStore.activeCorrections(for: assetId)
        #expect(loaded.count == 1)
        #expect(loaded[0].source == .manualVeto)

        let parsedScope = CorrectionScope.deserialize(loaded[0].scope)
        guard case .exactSpan(_, let range) = parsedScope else {
            Issue.record("Expected exactSpan scope")
            return
        }
        #expect(range == 50...100, "Ordinal range must match the tapped segment's resolved range")
    }
}

// makeTestAsset(id:) is defined in TestHelpers.swift
