// SelfPromoSuppressionWireInTests.swift
// playhead-fl4j: tests for the self-promo suppression wire-in into
// `AdDetectionService.runBackfill`.
//
// Mirrors LexicalAnchorRefinementWireInTests / CrossEpisodeMemoryWiringTests:
//   (a) Config-default plumbing — `selfPromoSuppressionEnabled` defaults to
//       `false` in `AdDetectionConfig.default`, and the init default matches
//       (this cut ships OFF; the production flip needs multi-show validation).
//   (b) Flag-OFF byte-identity — running `runBackfill` twice on the same
//       deterministic fixture (once flag-explicit-OFF WITH a live self-promo
//       bank injected, once at the config default with nothing injected)
//       produces byte-identical persisted AdWindow rows. Injecting the bank on
//       the OFF arm is the stronger contract: even fully wired, the flag gate
//       alone must keep the suppressor unreachable.
//   (c) Flag-ON — a span whose transcript carries a self-promo action phrase
//       has its persisted `eligibilityGate` flip from `.eligible` to
//       `.markOnly`, the window count is unchanged, and the window routes to the
//       SkipOrchestrator suggest tier (the play-by-default banner).
//   (d) Precision guard — a real third-party ad ("brought to you by <brand>")
//       with NO self-promo action phrase is NOT demoted (stays `.eligible`).
//   (e) Severity guard — a window already at a harder block
//       (`.blockedByUserCorrection`) stays blocked; the demotion (which targets
//       `.markOnly`) never overrides a harder gate.

import Foundation
import Testing
@testable import Playhead

@Suite("SelfPromoSuppression wire-in (playhead-fl4j)")
struct SelfPromoSuppressionWireInTests {

    private static let podcastId = "podcast-fl4j"

    // MARK: - Fixtures

    /// Three chunks with a strong ad-copy break at [60, 90]. When `selfPromo`
    /// is true the ad chunk ALSO carries a curated self-promo action phrase, so
    /// the detected span is BOTH a (mis)detected ad AND a self-promo — the exact
    /// class the bead demotes.
    private func makeChunks(assetId: String, selfPromo: Bool) -> [TranscriptChunk] {
        let adCopy = selfPromo
            ? "This episode is brought to you by Squarespace. Rate review and subscribe wherever you get your podcasts. Use code SHOW for 10 percent off at squarespace dot com slash show."
            : "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show."
        let texts: [(Double, Double, String)] = [
            (0.0, 30.0, "Welcome back to the show today we discuss technology and design."),
            (60.0, 90.0, adCopy),
            (90.0, 120.0, "Back to our regular conversation about new things and ideas."),
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    /// Three chunks with a strong ad-copy break at [60, 90] carrying
    /// `adBreakText` verbatim inside reliable sponsor ad copy (so the break is
    /// always DETECTED as an eligible ad). The surrounding chunks are
    /// deliberately FIRST-PERSON-FREE, so the only self-reference the suppressor
    /// can see for the ad-break span is one placed inside `adBreakText`.
    private func makeChunks(assetId: String, adBreakText: String) -> [TranscriptChunk] {
        let adCopy = "This episode is brought to you by Squarespace. \(adBreakText) Use code SHOW for 10 percent off at squarespace dot com slash show."
        let texts: [(Double, Double, String)] = [
            (0.0, 30.0, "Welcome back to the show. Today the topic is technology and design."),
            (60.0, 90.0, adCopy),
            (90.0, 120.0, "Back to the regular conversation about interesting things and ideas."),
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

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

    /// A self-promo bank built through the real decode/validate path from
    /// `(phrase, selfReference-class)` pairs. Injected so the wire-in tests are
    /// independent of the shipped JSON curation (the shipped set is pinned
    /// separately).
    private static func makeBank(_ phrases: [(String, String)]) throws -> SelfPromoBank {
        let payload: [String: Any] = [
            "schemaVersion": 2,
            "phrases": phrases.map { ["phrase": $0.0, "selfReference": $0.1] },
        ]
        return try SelfPromoBank.decode(JSONSerialization.data(withJSONObject: payload))
    }

    /// Convenience: a bank carrying the fixture's STRONG (self-evident) action
    /// phrase.
    private static func makeBank(_ phrases: [String] = ["rate review and subscribe"]) throws -> SelfPromoBank {
        try makeBank(phrases.map { ($0, "selfEvident") })
    }

    private func makeService(
        store: AnalysisStore,
        selfPromoEnabled: Bool,
        selfPromoBank: SelfPromoBank? = nil
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "fl4j-test",
            fmBackfillMode: .off,
            selfPromoSuppressionEnabled: selfPromoEnabled
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            selfPromoBank: selfPromoBank
        )
    }

    /// Find the persisted window overlapping the ad break at [60, 90].
    private func adWindow(in windows: [AdWindow]) throws -> AdWindow {
        try #require(
            windows.first { $0.startTime < 90 && $0.endTime > 60 },
            "fixture must produce a window overlapping the [60, 90] ad break"
        )
    }

    // MARK: - (a) Config defaults

    @Test("AdDetectionConfig.default ships self-promo suppression OFF")
    func configDefaultsAreOff() {
        #expect(
            AdDetectionConfig.default.selfPromoSuppressionEnabled == false,
            "this cut ships OFF; the production flip needs multi-show validation (out of scope)"
        )
    }

    @Test("AdDetectionConfig init carries selfPromoSuppressionEnabled through")
    func configInitCarriesFlag() {
        let on = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            selfPromoSuppressionEnabled: true
        )
        #expect(on.selfPromoSuppressionEnabled == true)

        let off = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            selfPromoSuppressionEnabled: false
        )
        #expect(off.selfPromoSuppressionEnabled == false)

        // Omitting the arg must match `.default` (OFF).
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1"
        )
        #expect(omitted.selfPromoSuppressionEnabled == false, "init default must match .default")
    }

    // MARK: - (b) Flag-OFF byte-identity

    @Test("Flag OFF: runBackfill is byte-identical to the default config even with a bank wired")
    func flagOffMatchesDefaultBaseline() async throws {
        let storeExplicit = try await makeTestStore()
        let storeDefault = try await makeTestStore()
        let assetId = "asset-fl4j-off"
        try await storeExplicit.insertAsset(makeAsset(id: assetId))
        try await storeDefault.insertAsset(makeAsset(id: assetId))

        // Explicit-OFF arm: flag off but a LIVE bank whose phrase IS present in
        // the fixture — so the ONLY thing keeping the suppressor unreachable is
        // the flag gate.
        let serviceExplicit = makeService(
            store: storeExplicit,
            selfPromoEnabled: false,
            selfPromoBank: try Self.makeBank()
        )
        // Default arm: config default (self-promo OFF), nothing injected.
        let defaultConfig = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "fl4j-test",
            fmBackfillMode: .off
            // selfPromoSuppressionEnabled omitted → default OFF.
        )
        let serviceDefault = AdDetectionService(
            store: storeDefault,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: defaultConfig
        )

        // Fixture WITH the self-promo phrase present (would demote if reachable).
        let chunks = makeChunks(assetId: assetId, selfPromo: true)
        try await serviceExplicit.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 120.0
        )
        try await serviceDefault.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 120.0
        )

        let windowsExplicit = try await storeExplicit.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsDefault = try await storeDefault.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        #expect(
            windowsExplicit.count == windowsDefault.count,
            "explicit-OFF \(windowsExplicit.count) vs default \(windowsDefault.count) — flag OFF must be byte-identical"
        )
        // Exhaustive persisted-field sweep: every `AdWindow` stored field EXCEPT
        // the intentionally-random `id` must be byte-identical under flag OFF.
        // The demotion can only move `eligibilityGate` (cascading to
        // `decisionState`), but a complete sweep future-proofs the byte-identity
        // contract against any new field a later change might let diverge.
        for (a, b) in zip(windowsExplicit, windowsDefault) {
            #expect(a.analysisAssetId == b.analysisAssetId, "analysisAssetId mismatch under flag OFF")
            #expect(a.startTime == b.startTime, "startTime mismatch under flag OFF")
            #expect(a.endTime == b.endTime, "endTime mismatch under flag OFF")
            #expect(a.confidence == b.confidence, "confidence mismatch under flag OFF")
            #expect(a.boundaryState == b.boundaryState, "boundaryState mismatch under flag OFF")
            #expect(a.decisionState == b.decisionState, "decisionState mismatch under flag OFF")
            #expect(a.detectorVersion == b.detectorVersion, "detectorVersion mismatch under flag OFF")
            #expect(a.advertiser == b.advertiser, "advertiser mismatch under flag OFF")
            #expect(a.product == b.product, "product mismatch under flag OFF")
            #expect(a.adDescription == b.adDescription, "adDescription mismatch under flag OFF")
            #expect(a.evidenceText == b.evidenceText, "evidenceText mismatch under flag OFF")
            #expect(a.evidenceStartTime == b.evidenceStartTime, "evidenceStartTime mismatch under flag OFF")
            #expect(a.metadataSource == b.metadataSource, "metadataSource mismatch under flag OFF")
            #expect(a.metadataConfidence == b.metadataConfidence, "metadataConfidence mismatch under flag OFF")
            #expect(a.metadataPromptVersion == b.metadataPromptVersion, "metadataPromptVersion mismatch under flag OFF")
            #expect(a.wasSkipped == b.wasSkipped, "wasSkipped mismatch under flag OFF")
            #expect(a.userDismissedBanner == b.userDismissedBanner, "userDismissedBanner mismatch under flag OFF")
            #expect(a.evidenceSources == b.evidenceSources, "evidenceSources mismatch under flag OFF")
            #expect(a.eligibilityGate == b.eligibilityGate, "eligibilityGate mismatch under flag OFF")
            #expect(a.catalogStoreMatchSimilarity == b.catalogStoreMatchSimilarity, "catalogStoreMatchSimilarity mismatch under flag OFF")
        }

        // The self-promo span stays eligible under flag OFF — the gate did not move.
        let off = try adWindow(in: windowsExplicit)
        #expect(off.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "flag OFF must leave the self-promo span at its undemoted .eligible gate")
    }

    // MARK: - (c) Flag-ON demotion + suggest-tier routing

    @Test("Flag ON: a self-promo span flips eligible → markOnly and routes to the suggest tier")
    func flagOnDemotesEligibleToMarkOnly() async throws {
        let storeOff = try await makeTestStore()
        let storeOn = try await makeTestStore()
        let assetId = "asset-fl4j-on"
        try await storeOff.insertAsset(makeAsset(id: assetId))
        try await storeOn.insertAsset(makeAsset(id: assetId))
        let chunks = makeChunks(assetId: assetId, selfPromo: true)

        // OFF baseline: the self-promo span is a detected, auto-skip-eligible ad.
        let serviceOff = makeService(store: storeOff, selfPromoEnabled: false)
        try await serviceOff.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 120.0
        )
        let windowsOff = try await storeOff.fetchAdWindows(assetId: assetId)
        let off = try adWindow(in: windowsOff)
        try #require(
            off.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
            "baseline self-promo span must be .eligible for the demotion to be observable (got \(off.eligibilityGate ?? "nil"))"
        )

        // ON: same fixture, self-promo suppression enabled with the matching bank.
        let serviceOn = makeService(
            store: storeOn,
            selfPromoEnabled: true,
            selfPromoBank: try Self.makeBank()
        )
        try await serviceOn.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 120.0
        )
        let windowsOn = try await storeOn.fetchAdWindows(assetId: assetId)

        // Never split, never merge.
        #expect(windowsOn.count == windowsOff.count,
                "self-promo suppression must never change the window count (\(windowsOff.count) OFF vs \(windowsOn.count) ON)")

        let on = try adWindow(in: windowsOn)
        #expect(on.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
                "self-promo span must demote to .markOnly (got \(on.eligibilityGate ?? "nil"))")
        // Eligibility change only — geometry is untouched.
        #expect(on.startTime == off.startTime, "boundaries must not move (eligibility-only change)")
        #expect(on.endTime == off.endTime, "boundaries must not move (eligibility-only change)")
        // "Scoring stays honest": the demotion touches ONLY the gate. The
        // persisted confidences must be byte-identical to the undemoted OFF
        // baseline — a regression that clamped a score on the suppression path
        // (instead of forwarding it verbatim) would move these.
        #expect(on.confidence == off.confidence,
                "skipConfidence must be preserved through the demotion (eligibility-only change)")
        #expect(on.metadataConfidence == off.metadataConfidence,
                "proposalConfidence must be preserved through the demotion (eligibility-only change)")

        // Routing: the persisted markOnly window lands in the SkipOrchestrator
        // suggest tier (play-by-default banner), NOT the auto-skip path.
        let orchestrator = SkipOrchestrator(store: storeOn)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, episodeId: assetId, podcastId: Self.podcastId
        )
        await orchestrator.receiveAdWindows([on])
        let suggestIDs = await orchestrator.activeSuggestWindowIDs()
        let autoSkipIDs = await orchestrator.activeWindowIDs()
        #expect(suggestIDs.contains(on.id),
                "a markOnly self-promo window must route to the suggest tier")
        #expect(!autoSkipIDs.contains(on.id),
                "a markOnly self-promo window must NOT enter the auto-skip window set")
    }

    // MARK: - (d) Precision guard: a real third-party ad is not demoted

    @Test("Flag ON: a bare-sponsor third-party ad with no self-promo phrase stays eligible")
    func flagOnDoesNotDemoteRealAd() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fl4j-realad"
        try await store.insertAsset(makeAsset(id: assetId))
        // No self-promo phrase — pure "brought to you by <brand>" ad copy.
        let chunks = makeChunks(assetId: assetId, selfPromo: false)

        let service = makeService(
            store: store,
            selfPromoEnabled: true,
            selfPromoBank: try Self.makeBank()
        )
        try await service.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 120.0
        )
        let windows = try await store.fetchAdWindows(assetId: assetId)
        let ad = try adWindow(in: windows)
        #expect(ad.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "a real third-party ad (bare sponsor phrase, no self-promo action verb) must NOT be demoted (got \(ad.eligibilityGate ?? "nil"))")
    }

    // MARK: - (d.2) Attention→verification: ambiguous phrase discrimination

    @Test("Flag ON: an ambiguous plug demotes ONLY with self-reference (third-party stays eligible)")
    func flagOnAmbiguousRequiresSelfReference() async throws {
        let bank = try Self.makeBank([
            ("get tickets", "requiresCorroboration"),
            ("on tour", "requiresCorroboration"),
        ])

        // Arm 1 — a THIRD-PARTY event plug in real ad copy, NO self-reference.
        // The ambiguous lexical hit is a clue that fails verification ⇒ eligible.
        let storeTP = try await makeTestStore()
        let assetTP = "asset-fl4j-ambig-tp"
        try await storeTP.insertAsset(makeAsset(id: assetTP))
        let tpChunks = makeChunks(
            assetId: assetTP,
            adBreakText: "Get tickets to see the band on tour at Ticketmaster."
        )
        let serviceTP = makeService(store: storeTP, selfPromoEnabled: true, selfPromoBank: bank)
        try await serviceTP.runBackfill(
            chunks: tpChunks, analysisAssetId: assetTP, podcastId: Self.podcastId, episodeDuration: 120.0
        )
        let tp = try adWindow(in: try await storeTP.fetchAdWindows(assetId: assetTP))
        #expect(
            tp.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
            "an ambiguous plug in a third-party ad (no self-reference) must NOT demote (got \(tp.eligibilityGate ?? "nil"))"
        )

        // Arm 2 — the SAME ambiguous plug, now with a first-person self-reference
        // ("our") in the local window ⇒ verified ⇒ demotes to markOnly.
        let storeSP = try await makeTestStore()
        let assetSP = "asset-fl4j-ambig-sp"
        try await storeSP.insertAsset(makeAsset(id: assetSP))
        let spChunks = makeChunks(
            assetId: assetSP,
            adBreakText: "Get tickets to our live show on tour this fall."
        )
        let serviceSP = makeService(store: storeSP, selfPromoEnabled: true, selfPromoBank: bank)
        try await serviceSP.runBackfill(
            chunks: spChunks, analysisAssetId: assetSP, podcastId: Self.podcastId, episodeDuration: 120.0
        )
        let sp = try adWindow(in: try await storeSP.fetchAdWindows(assetId: assetSP))
        #expect(
            sp.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
            "the same ambiguous plug WITH a first-person self-reference must demote to markOnly (got \(sp.eligibilityGate ?? "nil"))"
        )
    }

    // MARK: - (e) Severity guard: a harder block is preserved

    @Test("Flag ON: a self-promo span already at a harder block stays blocked (never promoted to markOnly)")
    func flagOnPreservesHarderBlock() async throws {
        let storeOff = try await makeTestStore()
        let storeOn = try await makeTestStore()
        let assetId = "asset-fl4j-severity"
        try await storeOff.insertAsset(makeAsset(id: assetId))
        try await storeOn.insertAsset(makeAsset(id: assetId))
        // Fixture carries the self-promo phrase, so absent the harder block the
        // suppressor WOULD demote it — the guard is what prevents that here.
        let chunks = makeChunks(assetId: assetId, selfPromo: true)

        // A user veto suppresses the whole asset's confidence below the
        // correction-gate floor, so the span gates to .blockedByUserCorrection
        // (severity 3) — strictly harder than .markOnly (severity 1).
        let corrections = FixedFalsePositiveCorrectionStore(passthrough: 0.05)

        // Flag OFF arm (with correction): establishes the harder gate.
        let serviceOff = makeService(store: storeOff, selfPromoEnabled: false)
        await serviceOff.setUserCorrectionStore(corrections)
        try await serviceOff.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 120.0
        )
        let off = try adWindow(in: try await storeOff.fetchAdWindows(assetId: assetId))
        try #require(
            off.eligibilityGate == SkipEligibilityGate.blockedByUserCorrection.rawValue,
            "the veto must drive the span to .blockedByUserCorrection (got \(off.eligibilityGate ?? "nil"))"
        )

        // Flag ON arm (same correction + a matching self-promo bank): the
        // severity guard must keep the harder block; NO demotion to markOnly.
        let serviceOn = makeService(
            store: storeOn,
            selfPromoEnabled: true,
            selfPromoBank: try Self.makeBank()
        )
        await serviceOn.setUserCorrectionStore(corrections)
        try await serviceOn.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 120.0
        )
        let on = try adWindow(in: try await storeOn.fetchAdWindows(assetId: assetId))
        #expect(on.eligibilityGate == SkipEligibilityGate.blockedByUserCorrection.rawValue,
                "self-promo suppression must NOT pull a harder block down to markOnly (got \(on.eligibilityGate ?? "nil"))")
    }
}

// MARK: - Test double

/// A `UserCorrectionStore` that reports a fixed false-positive passthrough
/// factor for every asset, so a test can deterministically drive a span to
/// `.blockedByUserCorrection` without depending on decay-weight arithmetic.
private struct FixedFalsePositiveCorrectionStore: UserCorrectionStore {
    let passthrough: Double
    func recordVeto(span: DecodedSpan) async {}
    func recordVeto(
        startTime: Double, endTime: Double, assetId: String,
        podcastId: String?, source: CorrectionSource
    ) async {}
    func record(_ event: CorrectionEvent) async throws {}
    func correctionPassthroughFactor(for analysisAssetId: String) async -> Double { passthrough }
    func correctionBoostFactor(for analysisAssetId: String) async -> Double { 1.0 }
    func correctionBoostFactor(
        for analysisAssetId: String, overlapping startTime: Double, endTime: Double
    ) async -> Double { 1.0 }
}
