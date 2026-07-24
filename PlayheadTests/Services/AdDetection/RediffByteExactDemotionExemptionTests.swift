// RediffByteExactDemotionExemptionTests.swift
// playhead-pzy2: byte-exact rediff-certain spans must be EXEMPT from the
// low-certainty lexical / self-promo demotions — deterministic certainty
// outranks a lexical clue.
//
// A byte-exact rediff slot is a 100%-deterministic DAI divergence (the origin
// literally served different ad bytes, recovered by the double-fetch differ),
// tagged on the decoded span as `.rediffSlot` in `anchorProvenance`. The
// self-promo suppressor (playhead-fl4j) is a low-certainty heuristic that can
// wrongly demote a REAL ad to a play-by-default banner because its transcript
// "sounds like" the show promoting itself. When BOTH fire on the same span,
// determinism must WIN — the span keeps its ad status / width / eligibility.
//
// This suite proves the exemption two ways:
//   (a) Predicate unit tests on `DecodedSpan.carriesRediffByteExactWidth` — the
//       single definition of "byte-exact rediff certain". It is true ONLY for
//       `.rediffSlot`; a `.spliceSlot` (acoustic width, NOT byte-exact) and
//       FM / lexical-only spans are NOT exempt, so the exemption can never leak
//       to a non-deterministic span.
//   (b) Behavioural end-to-end tests through `AdDetectionService.runBackfill`:
//       a rediff-width-owned span whose transcript ALSO carries a strong
//       self-promo phrase stays `.eligible` (WOULD be demoted to `.markOnly`
//       without the exemption — RED-before / GREEN-after), while the SAME
//       self-promo trigger on a NON-rediff span still demotes to `.markOnly`
//       (the exemption does not leak).
//
// The rediff harness (deterministic noise PCM + synthetic A-side fingerprint
// stream with an ad block spliced in) mirrors `RediffSlotOwnershipEndToEndTests`;
// the self-promo fixture mirrors `SelfPromoSuppressionWireInTests`.

import Foundation
import Testing

@testable import Playhead

@Suite("Byte-exact rediff demotion exemption (playhead-pzy2)")
struct RediffByteExactDemotionExemptionTests {

    // MARK: - (a) Predicate unit tests

    /// A span whose width the rediff differ owns (`.rediffSlot`) IS byte-exact
    /// rediff certain — the exemption key.
    @Test("carriesRediffByteExactWidth is true for a .rediffSlot span")
    func predicateTrueForRediffSlot() {
        let span = Self.span(provenance: [.rediffSlot])
        #expect(span.carriesRediffByteExactWidth)
    }

    /// A `.spliceSlot` span is ACOUSTIC width, not byte-exact — it is NOT exempt.
    /// Pins the deliberate splice-agnosticism (mirrors `deriveFusionEdgeAnchors`,
    /// which sets `.rediffByteExact` only for `.rediffSlot`): were the predicate
    /// broadened to `isWidthOwnership`, splice would leak into the exemption.
    @Test("carriesRediffByteExactWidth is FALSE for a .spliceSlot (acoustic) span")
    func predicateFalseForSpliceSlot() {
        let span = Self.span(provenance: [.spliceSlot])
        #expect(!span.carriesRediffByteExactWidth,
                "acoustic splice width is NOT byte-exact — it must NOT be exempt")
    }

    /// FM / lexical presence anchors carry no byte-exact certainty — NOT exempt.
    @Test("carriesRediffByteExactWidth is FALSE for FM / lexical-only spans")
    func predicateFalseForNonDeterministicSpans() {
        let fm = Self.span(provenance: [.fmConsensus(regionId: "r", consensusStrength: 0.9)])
        #expect(!fm.carriesRediffByteExactWidth)

        let music = Self.span(provenance: [.sustainedMusicOffset(regionId: "r", confidence: 0.8)])
        #expect(!music.carriesRediffByteExactWidth)

        let empty = Self.span(provenance: [])
        #expect(!empty.carriesRediffByteExactWidth)
    }

    /// Width ownership APPENDS to the transcript/evidence provenance, so a real
    /// rediff span carries `.rediffSlot` alongside other anchors — still exempt.
    @Test("carriesRediffByteExactWidth is true when .rediffSlot is mixed with other anchors")
    func predicateTrueForMixedProvenance() {
        let span = Self.span(provenance: [
            .fmConsensus(regionId: "r", consensusStrength: 0.9),
            .rediffSlot,
        ])
        #expect(span.carriesRediffByteExactWidth)
    }

    // MARK: - (b) Behavioural end-to-end

    /// RED-before / GREEN-after: a rediff-width-owned span whose transcript ALSO
    /// carries a strong self-promo phrase stays `.eligible`. The baseline arm
    /// (self-promo OFF) proves the SAME span is a rediff-owned, auto-skip-eligible
    /// ad, so the only thing that could move the gate is the self-promo
    /// suppressor — which the byte-exact exemption blocks. Without the exemption
    /// this span demotes to `.markOnly` and the assertion fails.
    @Test("A rediff-owned self-promo span stays .eligible (deterministic certainty wins)")
    func rediffOwnedSelfPromoSpanIsNotDemoted() async throws {
        // Baseline: rediff ON, self-promo OFF ⇒ rediff-owned + .eligible.
        let baseline = try await Self.runAndFetchAdWindow(
            assetId: "pzy2-rediff-baseline",
            rediffOwnership: true,
            selfPromoEnabled: false
        )
        try #require(
            baseline.anchorProvenance.contains(.rediffSlot),
            "fixture precondition: the ad span must be rediff-width-owned"
        )
        try #require(
            baseline.window.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
            "fixture precondition: the rediff-owned span must be .eligible for the demotion to be observable (got \(baseline.window.eligibilityGate ?? "nil"))"
        )

        // Exemption arm: rediff ON, self-promo ON with the matching bank. The
        // self-promo phrase is present in the ad chunk, so absent the exemption
        // the fl4j suppressor WOULD demote this span to .markOnly.
        let exempt = try await Self.runAndFetchAdWindow(
            assetId: "pzy2-rediff-selfpromo",
            rediffOwnership: true,
            selfPromoEnabled: true
        )
        #expect(
            exempt.anchorProvenance.contains(.rediffSlot),
            "the span is still rediff-width-owned"
        )
        #expect(
            exempt.window.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
            "a byte-exact rediff span must stay .eligible despite the self-promo phrase (got \(exempt.window.eligibilityGate ?? "nil"))"
        )
        // Eligibility-only contract: geometry and scores are untouched vs baseline.
        #expect(exempt.window.startTime == baseline.window.startTime, "width must be preserved")
        #expect(exempt.window.endTime == baseline.window.endTime, "width must be preserved")
        #expect(exempt.window.confidence == baseline.window.confidence, "score must be preserved")
    }

    /// Exemption-does-not-leak: the SAME self-promo fixture with NO rediff
    /// ownership (no `.rediffSlot`) still demotes to `.markOnly`. Only rediff
    /// ownership differs between this and the arm above, so this pins that the
    /// exemption is scoped exactly to byte-exact rediff spans.
    @Test("A NON-rediff self-promo span is STILL demoted to .markOnly (exemption does not leak)")
    func nonRediffSelfPromoSpanStillDemoted() async throws {
        let result = try await Self.runAndFetchAdWindow(
            assetId: "pzy2-nonrediff-selfpromo",
            rediffOwnership: false,
            selfPromoEnabled: true
        )
        #expect(
            !result.anchorProvenance.contains(.rediffSlot),
            "control precondition: this span is NOT rediff-width-owned"
        )
        #expect(
            result.window.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
            "a non-deterministic self-promo span must STILL demote to .markOnly — the exemption must not leak (got \(result.window.eligibilityGate ?? "nil"))"
        )
    }

    // MARK: - Fixtures & helpers

    private static let adStart = 100.0
    private static let adEnd = 160.0
    private static let podcastId = "podcast-pzy2"

    /// A minimal `DecodedSpan` with the given provenance for the predicate tests.
    private static func span(provenance: [AnchorRef]) -> DecodedSpan {
        DecodedSpan(
            id: "span-pzy2",
            assetId: "asset-pzy2",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 1,
            startTime: adStart,
            endTime: adEnd,
            anchorProvenance: provenance
        )
    }

    private static func asset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id, episodeId: "ep-\(id)", assetFingerprint: "fp-\(id)",
            weakFingerprint: nil, sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil
        )
    }

    /// Host chunk + ad chunk at [100,160] carrying reliable sponsor ad copy AND a
    /// STRONG self-promo action phrase ("rate review and subscribe") — the exact
    /// class the fl4j suppressor demotes. The self-reference is `selfEvident`, so
    /// no show-identity corroboration is required for the suppressor to fire.
    private static func chunks(assetId: String) -> [TranscriptChunk] {
        let adCopy = "This segment is brought to you by Squarespace. Rate review and subscribe wherever you get your podcasts. Use code SHOW for 10 percent off at squarespace dot com slash show. Build your website today."
        let specs: [(Double, Double, String)] = [
            (0, 100, "Welcome to the show. We talk at length about science and history here."),
            (adStart, adEnd, adCopy),
            (160, 280, "Back to the conversation about the future and what comes next for all of us."),
        ]
        return specs.enumerated().map { idx, s in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)", analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)", chunkIndex: idx,
                startTime: s.0, endTime: s.1, text: s.2,
                normalizedText: s.2.lowercased(), pass: "final",
                modelVersion: "test-v1", transcriptVersion: nil, atomOrdinal: nil
            )
        }
    }

    /// Loudness-step feature track over [100,160], so the boundary-refiner path is
    /// ACTIVE (non-empty featureWindows) — a rediff-owned span must BYPASS it and
    /// keep its exact fingerprint-diff edges.
    private static func features(assetId: String, count: Int) -> [FeatureWindow] {
        (0..<count).map { i in
            let start = Double(i) * 2.0
            let inAd = start >= adStart && start < adEnd
            return AcousticFeatureFixtures.window(
                assetId: assetId, startTime: start, endTime: start + 2.0,
                rms: inAd ? 0.6 : 0.2, spectralFlux: 0.05, musicProbability: 0.02
            )
        }
    }

    /// Deterministic SplitMix64 noise so the fixture is byte-stable across runs.
    private struct Noise {
        var state: UInt64
        init(seed: UInt64) { state = seed }
        mutating func next() -> UInt64 {
            state &+= 0x9E37_79B9_7F4A_7C15
            var z = state
            z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
            z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
            return z ^ (z >> 31)
        }
    }

    private static func noisePCM(seconds: Double, seed: UInt64) -> [Float] {
        var rng = Noise(seed: seed)
        let n = Int(seconds * 16_000)
        return (0..<n).map { _ in Float(Int64(bitPattern: rng.next()) % 2_000_000) / 1_000_000.0 }
    }

    private struct FixedBSideProvider: RediffBSideProvider {
        let assetId: String
        let samples: [Float]
        func refetchedBSideMono16kHz(assetId: String) async -> [Float]? {
            assetId == self.assetId ? samples : nil
        }
    }

    /// Stored A-side stream = B's own content fingerprints with a distinct ad
    /// block spliced in at the index mapping to `adStart`, so the differ recovers
    /// a played slot at [adStart, adEnd].
    private static func syntheticASide(
        assetId: String, contentPCM: [Float]
    ) -> EpisodeFingerprintRecord {
        let secPerFp = ChromaFingerprinter.secondsPerFingerprint
        let fpContent = EpisodeFingerprintCapture.fingerprints(mono16kHz: contentPCM)
        let kIns = Int((adStart / secPerFp).rounded())
        let adLen = Int(((adEnd - adStart) / secPerFp).rounded())
        var rng = Noise(seed: 0xADD_5EED)
        let adBlock = (0..<adLen).map { _ in UInt32(truncatingIfNeeded: rng.next()) | 0x8000_0000 }
        precondition(kIns < fpContent.count, "content too short for the insertion index")
        var aFps = Array(fpContent[0..<kIns])
        aFps.append(contentsOf: adBlock)
        aFps.append(contentsOf: fpContent[kIns...])
        return EpisodeFingerprintRecord(
            analysisAssetId: assetId,
            algorithmVersion: ChromaFingerprinter.algorithmVersion,
            secondsPerFingerprint: secPerFp,
            fingerprints: aFps,
            sourceAudioIdentity: "fp-\(assetId)",  // matches asset fingerprint
            capturedAt: 0
        )
    }

    /// A self-promo bank carrying the fixture's STRONG (`selfEvident`) phrase,
    /// built through the real decode/validate path (independent of shipped JSON).
    private static func makeBank() throws -> SelfPromoBank {
        let payload: [String: Any] = [
            "schemaVersion": 2,
            "phrases": [["phrase": "rate review and subscribe", "selfReference": "selfEvident"]],
        ]
        return try SelfPromoBank.decode(JSONSerialization.data(withJSONObject: payload))
    }

    private struct AdWindowResult {
        let window: AdWindow
        let anchorProvenance: [AnchorRef]
    }

    /// Run backfill for the given (rediff, self-promo) configuration and return
    /// the persisted ad window overlapping [100,160] together with its decoded
    /// span's anchor provenance.
    private static func runAndFetchAdWindow(
        assetId: String,
        rediffOwnership: Bool,
        selfPromoEnabled: Bool
    ) async throws -> AdWindowResult {
        let store = try await makeTestStore()
        try await store.insertAsset(asset(id: assetId))
        try await store.insertFeatureWindows(features(assetId: assetId, count: 140))

        var provider: RediffBSideProvider?
        if rediffOwnership {
            let contentPCM = noisePCM(seconds: 180, seed: 7)
            try await store.upsertEpisodeFingerprints(syntheticASide(assetId: assetId, contentPCM: contentPCM))
            provider = FixedBSideProvider(assetId: assetId, samples: contentPCM)
        }

        let config = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70,
            suppressionThreshold: 0.25, hotPathLookahead: 90.0,
            detectorVersion: "pzy2-test", fmBackfillMode: .off,
            rediffSlotOwnershipEnabled: rediffOwnership,
            selfPromoSuppressionEnabled: selfPromoEnabled
        )
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            rediffBSideProvider: provider,
            selfPromoBank: selfPromoEnabled ? try makeBank() : nil
        )
        try await service.runBackfill(
            chunks: chunks(assetId: assetId), analysisAssetId: assetId,
            podcastId: podcastId, episodeDuration: 280.0
        )

        let windows = try await store.fetchAdWindows(assetId: assetId)
        let window = try #require(
            windows.first { $0.startTime < adEnd && $0.endTime > adStart },
            "fixture must produce a window overlapping the [100,160] ad break"
        )
        // Pair the window back to its decoded span to read anchor provenance
        // (the `.rediffSlot` marker lives on the span, not the window row).
        let spans = try await store.fetchDecodedSpans(assetId: assetId)
        let span = spans.first { $0.startTime < adEnd && $0.endTime > adStart }
        return AdWindowResult(window: window, anchorProvenance: span?.anchorProvenance ?? [])
    }
}
