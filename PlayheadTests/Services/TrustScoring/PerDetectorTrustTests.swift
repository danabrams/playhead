// PerDetectorTrustTests.swift
// playhead-gard: trust is per DETECTOR CLASS, not one scalar per show.
//
// The field row this bead is built from, measured on the owner's device
// 2026-08-01:
//
//     skipTrustScore              0.20
//     recentFalseSkipSignals      3
//     implicitFalsePositiveCount  3
//     mode                        manual
//
// Those three signals were vetoes of `segmentAggregated` windows (confidence
// 0.40–0.42, BOTH edges unanchored) that playhead-ynmk has since stopped from
// ever skipping. The aggregator went 0-for-3 — and suppressed byte-exact rediff
// on the same show, which went 2-for-2 on the same corpus.
//
// Four claims are pinned here, and the third is the one a lazy fix would skip:
//   1. that show still auto-skips byte-exact rediff,
//   2. a detector's own bad history still demotes THAT detector,
//   3. per-detector trust that never demotes anything is as broken as one
//      global scalar — so a demotion must still happen, including for the
//      show-trust-exempt class,
//   4. `manual` is escapable, and would not be if the escape were removed.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

// MARK: - Helpers

private let gardPodcastId = "podcast-1"

private func gardProfile(
    mode: String,
    trustScore: Double,
    observations: Int,
    falseSignals: Int,
    detectorTrustJSON: String? = nil
) -> PodcastProfile {
    PodcastProfile(
        podcastId: gardPodcastId,
        sponsorLexicon: nil,
        normalizedAdSlotPriors: nil,
        repeatedCTAFragments: nil,
        jingleFingerprints: nil,
        implicitFalsePositiveCount: falseSignals,
        skipTrustScore: trustScore,
        observationCount: observations,
        mode: mode,
        recentFalseSkipSignals: falseSignals,
        detectorTrustJSON: detectorTrustJSON
    )
}

/// The exact device row, so the regression is named by its evidence.
private func danFieldProfile() -> PodcastProfile {
    gardProfile(
        mode: SkipMode.manual.rawValue,
        trustScore: 0.20,
        observations: 1,
        falseSignals: 3
    )
}

private func gardService(
    seed: PodcastProfile?
) async throws -> (TrustScoringService, AnalysisStore) {
    let store = try await makeTestStore()
    if let seed { try await store.upsertProfile(seed) }
    return (TrustScoringService(store: store), store)
}

private func aggregatorWindow(
    id: String = "agg-1",
    startTime: Double = 60,
    endTime: Double = 120,
    confidence: Double = 0.41
) -> AdWindow {
    AdWindow(
        id: id,
        analysisAssetId: "asset-1",
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        boundaryState: AdBoundaryState.segmentAggregated.rawValue,
        decisionState: AdDecisionState.confirmed.rawValue,
        detectorVersion: "detection-v1",
        advertiser: nil,
        product: nil,
        adDescription: nil,
        evidenceText: "brought to you by",
        evidenceStartTime: startTime,
        metadataSource: "none",
        metadataConfidence: nil,
        metadataPromptVersion: nil,
        wasSkipped: false,
        userDismissedBanner: false
    )
}

private func rediffWindow(
    id: String = "rediff-1",
    startTime: Double = 200,
    endTime: Double = 260,
    confidence: Double = 0.90
) -> AdWindow {
    makeSkipTestAdWindow(
        id: id,
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        decisionState: AdDecisionState.confirmed.rawValue,
        startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
        endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
    )
}

// MARK: - Classification

@Suite("playhead-gard — SkipDetectorClass classification")
struct SkipDetectorClassTests {

    @Test("Both edges byte-exact classify as the deterministic rediff class")
    func byteExactBothEdges() {
        #expect(
            SkipDetectorClass.classify(
                boundaryState: "lexical",
                startAnchor: .rediffByteExact,
                endAnchor: .rediffByteExact
            ) == .rediffByteExact
        )
    }

    @Test("ONE byte-exact edge is not the deterministic class — a span is worth its weaker edge")
    func byteExactOneEdgeOnly() {
        #expect(
            SkipDetectorClass.classify(
                boundaryState: "lexical",
                startAnchor: .rediffByteExact,
                endAnchor: .unanchored
            ) == .fusion
        )
        #expect(
            SkipDetectorClass.classify(
                boundaryState: "lexical",
                startAnchor: .unanchored,
                endAnchor: .rediffByteExact
            ) == .fusion
        )
    }

    @Test("A stinger-snapped pair is corroborated, not deterministic — it is not the rediff class")
    func stingerSnappedIsNotRediff() {
        #expect(
            SkipDetectorClass.classify(
                boundaryState: "lexical",
                startAnchor: .stingerSnapped,
                endAnchor: .stingerSnapped
            ) == .fusion
        )
    }

    @Test("segmentAggregated boundary state classifies as the aggregator")
    func aggregator() {
        #expect(
            SkipDetectorClass.classify(
                boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                startAnchor: .unanchored,
                endAnchor: .unanchored
            ) == .segmentAggregated
        )
    }

    @Test("User gestures classify as userAsserted, ahead of everything else")
    func userAssertions() {
        for assertion in UserSpanAssertion.allCases {
            #expect(
                SkipDetectorClass.classify(
                    boundaryState: assertion.rawValue,
                    startAnchor: .rediffByteExact,
                    endAnchor: .rediffByteExact
                ) == .userAsserted
            )
        }
    }

    @Test("An unrecognised boundary state falls to fusion, never to a weaker gate")
    func unknownBoundaryState() {
        #expect(
            SkipDetectorClass.classify(
                boundaryState: "someFutureProducer",
                startAnchor: .unanchored,
                endAnchor: .unanchored
            ) == .fusion
        )
    }

    @Test("Exactly one class is exempt from the show's trust history")
    func exemptionIsSingular() {
        let exempt = SkipDetectorClass.allCases.filter { !$0.consultsShowTrust }
        #expect(exempt == [.rediffByteExact])
    }

    @Test("AdWindow classifies from its own persisted columns")
    func rowLevelClassification() {
        #expect(rediffWindow().detectorClass == .rediffByteExact)
        #expect(aggregatorWindow().detectorClass == .segmentAggregated)
        #expect(makeSkipTestAdWindow().detectorClass == .fusion)
    }
}

// MARK: - Veto weighting

@Suite("playhead-gard — a veto weighs what it retracted")
struct DetectorVetoWeightTests {

    @Test("Weights are ordered by the certainty of what was skipped")
    func ordering() {
        #expect(
            DetectorVetoWeight.weight(for: .none)
                < DetectorVetoWeight.weight(for: .corroborated)
        )
        #expect(
            DetectorVetoWeight.weight(for: .corroborated)
                < DetectorVetoWeight.weight(for: .deterministic)
        )
    }

    @Test("Three unanchored vetoes weigh 1.5 — under the demotion threshold of 2")
    func theFieldCaseArithmetic() {
        let three = 3 * DetectorVetoWeight.weight(for: .none)
        #expect(three == 1.5)
        #expect(three < Double(TrustScoringConfig.default.autoToManualFalseSignals))
        let four = 4 * DetectorVetoWeight.weight(for: .none)
        #expect(four >= Double(TrustScoringConfig.default.autoToManualFalseSignals))
    }
}

// MARK: - The ledger and its migration

@Suite("playhead-gard — DetectorTrustLedger persistence and seeding", .serialized)
struct DetectorTrustLedgerTests {

    @Test("A pre-gard row seeds every show-governed class from the legacy scalar")
    func migrationPreservesPosture() {
        let profile = danFieldProfile()
        let ledger = profile.detectorTrustLedger
        #expect(ledger.entries.isEmpty, "A pre-gard row carries no ledger")

        for detector in SkipDetectorClass.allCases where detector.consultsShowTrust {
            let entry = ledger.entry(for: detector, seededFrom: profile)
            #expect(entry.mode == SkipMode.manual.rawValue)
            #expect(entry.trustScore == 0.20)
            #expect(entry.falseSkipWeight == 3)
        }
    }

    @Test("The exempt class seeds CLEAN — it does not inherit other detectors' mistakes")
    func migrationReleasesRediff() {
        let entry = DetectorTrustLedger()
            .entry(for: .rediffByteExact, seededFrom: danFieldProfile())
        #expect(entry.skipMode == .auto)
        #expect(entry.falseSkipWeight == 0)
    }

    @Test("An empty ledger encodes to NULL — an untouched show stays byte-identical to a pre-gard row")
    func emptyEncodesToNil() {
        #expect(DetectorTrustLedger().encoded() == nil)
    }

    @Test("Round-trips, and a key this binary does not know SURVIVES the trip")
    func forwardCompatibleRoundTrip() throws {
        var ledger = DetectorTrustLedger()
        ledger.set(
            DetectorTrustEntry(
                trustScore: 0.8, mode: SkipMode.auto.rawValue,
                falseSkipWeight: 0.5, observationCount: 4
            ),
            for: .fusion
        )
        let json = try #require(ledger.encoded())
        // Splice in a class only a newer build knows about.
        let futureJSON = json.replacingOccurrences(
            of: "{",
            with: """
                {"someFutureDetector":{"trustScore":0.9,"mode":"auto",\
                "falseSkipWeight":0,"observationCount":1},
                """,
            options: [],
            range: json.startIndex..<json.index(after: json.startIndex)
        )
        let decoded = DetectorTrustLedger.decode(futureJSON)
        #expect(decoded.entries["someFutureDetector"] != nil)
        #expect(decoded.entries[SkipDetectorClass.fusion.rawValue]?.observationCount == 4)
        let reencoded = try #require(decoded.encoded())
        #expect(DetectorTrustLedger.decode(reencoded).entries["someFutureDetector"] != nil)
    }

    @Test("A corrupt column costs history, never posture")
    func corruptColumnFallsBackToTheSeed() {
        let profile = gardProfile(
            mode: SkipMode.auto.rawValue, trustScore: 0.9,
            observations: 10, falseSignals: 0,
            detectorTrustJSON: "{not json at all"
        )
        let ledger = profile.detectorTrustLedger
        #expect(ledger.entries.isEmpty)
        #expect(
            ledger.entry(for: .fusion, seededFrom: profile).skipMode == .auto
        )
    }

    @Test("The column survives a store round-trip")
    func storeRoundTrip() async throws {
        let store = try await makeTestStore()
        var ledger = DetectorTrustLedger()
        ledger.set(
            DetectorTrustEntry(
                trustScore: 0.42, mode: SkipMode.manual.rawValue,
                falseSkipWeight: 1.5, observationCount: 3
            ),
            for: .segmentAggregated
        )
        try await store.upsertProfile(
            gardProfile(
                mode: SkipMode.auto.rawValue, trustScore: 0.9,
                observations: 10, falseSignals: 0,
                detectorTrustJSON: ledger.encoded()
            )
        )
        let read = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        let entry = try #require(
            read.detectorTrustLedger.entries[
                SkipDetectorClass.segmentAggregated.rawValue
            ]
        )
        #expect(entry.falseSkipWeight == 1.5)
        #expect(entry.observationCount == 3)
    }

    @Test("A nil-ledger write does NOT clobber a persisted ledger (COALESCE-preserve)")
    func nilWritePreserves() async throws {
        let store = try await makeTestStore()
        var ledger = DetectorTrustLedger()
        ledger.set(
            DetectorTrustEntry(
                trustScore: 0.42, mode: SkipMode.manual.rawValue,
                falseSkipWeight: 1.5, observationCount: 3
            ),
            for: .fusion
        )
        try await store.upsertProfile(
            gardProfile(
                mode: SkipMode.auto.rawValue, trustScore: 0.9,
                observations: 10, falseSignals: 0,
                detectorTrustJSON: ledger.encoded()
            )
        )
        // A pre-gard-shaped rebuild: every other column carried, ledger nil.
        try await store.upsertProfile(
            gardProfile(
                mode: SkipMode.auto.rawValue, trustScore: 0.8,
                observations: 11, falseSignals: 0
            )
        )
        let read = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(read.skipTrustScore == 0.8, "the nil-write's own columns land")
        #expect(
            read.detectorTrustLedger.entries[SkipDetectorClass.fusion.rawValue]
                != nil,
            "the ledger it did not know about survives"
        )
    }
}

// MARK: - Resolution

@Suite("playhead-gard — per-detector mode resolution", .serialized)
struct DetectorModeResolutionTests {

    @Test("THE BEAD: the device row still auto-skips byte-exact rediff while the aggregator stays manual")
    func theFieldRowResolvesPerDetector() async throws {
        let (sut, _) = try await gardService(seed: danFieldProfile())
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)

        #expect(modes.showMode == .manual, "the show-level answer is unchanged")
        #expect(modes.mode(for: .rediffByteExact) == .auto)
        #expect(modes.mode(for: .segmentAggregated) == .manual)
        #expect(modes.mode(for: .fusion) == .manual)
        #expect(modes.mode(for: .userAsserted) == .manual)
    }

    @Test("effectiveMode is untouched — the pill and an older binary read the same value they always did")
    func showLevelAnswerUnchanged() async throws {
        let (sut, _) = try await gardService(seed: danFieldProfile())
        #expect(await sut.effectiveMode(podcastId: gardPodcastId) == .manual)
        let resolved = await sut.resolveMode(podcastId: gardPodcastId)
        #expect(resolved.mode == .manual)
        #expect(resolved.resolution == .showTrustProfile)
    }

    @Test("A stored per-detector entry beats the legacy seed")
    func storedEntryWins() async throws {
        var ledger = DetectorTrustLedger()
        ledger.set(
            DetectorTrustEntry(
                trustScore: 0.1, mode: SkipMode.shadow.rawValue,
                falseSkipWeight: 4, observationCount: 2
            ),
            for: .rediffByteExact
        )
        let (sut, _) = try await gardService(
            seed: gardProfile(
                mode: SkipMode.auto.rawValue, trustScore: 0.9,
                observations: 10, falseSignals: 0,
                detectorTrustJSON: ledger.encoded()
            )
        )
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .rediffByteExact) == .shadow)
        #expect(modes.mode(for: .fusion) == .auto)
    }

    @Test("A profile READ failure lands every class on shadow, exemption included")
    func lookupFailureIsNonActioning() async throws {
        // An unrecognised stored mode is the reachable read-side failure.
        let (sut, _) = try await gardService(
            seed: gardProfile(
                mode: "someFutureMode", trustScore: 0.9,
                observations: 10, falseSignals: 0
            )
        )
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.resolution == .unrecognizedTrustProfileMode)
        for detector in SkipDetectorClass.allCases {
            #expect(
                modes.mode(for: detector) == .shadow,
                "\(detector.rawValue) must not act on a failed lookup"
            )
        }
    }

    @Test("A show with no profile is the deliberate new-show default, and the exempt class still resolves")
    func newShowDefault() async throws {
        let (sut, _) = try await gardService(seed: nil)
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.resolution == .newShowDefault)
        #expect(modes.showMode == .shadow)
        #expect(modes.mode(for: .fusion) == .shadow)
        #expect(
            modes.mode(for: .rediffByteExact) == .auto,
            "day-0 byte-exact rediff is the only signal a first listen has"
        )
    }
}

// MARK: - Demotion

@Suite("playhead-gard — a detector's own history demotes that detector", .serialized)
struct PerDetectorDemotionTests {

    private static func autoProfile() -> PodcastProfile {
        gardProfile(
            mode: SkipMode.auto.rawValue, trustScore: 0.9,
            observations: 10, falseSignals: 0
        )
    }

    @Test("THE NEGATIVE: per-detector trust that never demotes is as broken as one scalar")
    func demotionStillHappens() async throws {
        let (sut, _) = try await gardService(seed: Self.autoProfile())
        // Two corroborated-tier vetoes: 1.0 + 1.0 = 2.0, the threshold.
        for _ in 0..<2 {
            await sut.recordFalseSkipSignal(
                podcastId: gardPodcastId,
                attributions: [
                    DetectorVetoAttribution(
                        detector: .segmentAggregated, tier: .corroborated
                    )
                ]
            )
        }
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .segmentAggregated) == .manual)
    }

    @Test("The blamed detector is demoted and the others are NOT")
    func blameIsNotShared() async throws {
        let (sut, _) = try await gardService(seed: Self.autoProfile())
        for _ in 0..<2 {
            await sut.recordFalseSkipSignal(
                podcastId: gardPodcastId,
                attributions: [
                    DetectorVetoAttribution(
                        detector: .segmentAggregated, tier: .corroborated
                    )
                ]
            )
        }
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .segmentAggregated) == .manual)
        #expect(
            modes.mode(for: .rediffByteExact) == .auto,
            "the aggregator's 0-for-3 is not evidence about a byte differ"
        )
        #expect(
            modes.mode(for: .fusion) == .auto,
            "nor about the general fusion path"
        )
    }

    @Test("The EXEMPT class is exempt from the show's history, not from its own")
    func exemptClassIsStillDemotable() async throws {
        let (sut, _) = try await gardService(seed: Self.autoProfile())
        // Two deterministic-tier vetoes of rediff spans: 1.5 + 1.5 = 3.0.
        for _ in 0..<2 {
            await sut.recordFalseSkipSignal(
                podcastId: gardPodcastId,
                attributions: [
                    DetectorVetoAttribution(
                        detector: .rediffByteExact, tier: .deterministic
                    )
                ]
            )
        }
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .manual,
            "byte-exactness proves divergence, not ad-ness — the user must be able to shut it off"
        )
    }

    @Test("THE FIELD CASE: three unanchored aggregator vetoes no longer demote")
    func weightedPenaltyLeavesTheShowInAuto() async throws {
        let (sut, _) = try await gardService(seed: Self.autoProfile())
        for _ in 0..<3 {
            await sut.recordFalseSkipSignal(
                podcastId: gardPodcastId,
                attributions: [
                    DetectorVetoAttribution(
                        detector: .segmentAggregated, tier: .none
                    )
                ]
            )
        }
        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .segmentAggregated) == .auto,
            "1.5 weighted is under the threshold of 2"
        )
        // …and a fourth still does. The tolerance is a weighting, not a hole.
        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(
                    detector: .segmentAggregated, tier: .none
                )
            ]
        )
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .segmentAggregated) == .manual)
    }

    @Test("The LEGACY triple moves exactly once per gesture, however many classes were blamed")
    func legacyScalarSemanticsUnchanged() async throws {
        let (sut, store) = try await gardService(seed: Self.autoProfile())
        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(detector: .segmentAggregated, tier: .none),
                DetectorVetoAttribution(detector: .fusion, tier: .corroborated),
                DetectorVetoAttribution(detector: .rediffByteExact, tier: .deterministic)
            ]
        )
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(profile.recentFalseSkipSignals == 1)
        #expect(profile.implicitFalsePositiveCount == 1)
        #expect(abs(profile.skipTrustScore - 0.8) < 1e-10)
        // Every named class was charged, each at its own weight.
        let ledger = profile.detectorTrustLedger
        #expect(
            ledger.entries[SkipDetectorClass.segmentAggregated.rawValue]?
                .falseSkipWeight == 0.5
        )
        #expect(
            ledger.entries[SkipDetectorClass.fusion.rawValue]?
                .falseSkipWeight == 1.0
        )
        #expect(
            ledger.entries[SkipDetectorClass.rediffByteExact.rawValue]?
                .falseSkipWeight == 1.5
        )
        #expect(
            ledger.entries[SkipDetectorClass.userAsserted.rawValue]?
                .falseSkipWeight == 0,
            """
            A class nobody blamed is MATERIALIZED at the pre-veto seed and             charged nothing. Materializing is what forks the ledger from the             legacy scalar: without it, the scalar this same gesture demotes             would leak back into every unwritten class through the seed, and             blame would still be shared — one hop later.
            """
        )
    }

    @Test("A duplicate class in one gesture is charged ONCE, at its strongest tier")
    func duplicateClassTakesTheStrongestTier() async throws {
        let (sut, store) = try await gardService(seed: Self.autoProfile())
        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(detector: .fusion, tier: .none),
                DetectorVetoAttribution(detector: .fusion, tier: .deterministic),
                DetectorVetoAttribution(detector: .fusion, tier: .none)
            ]
        )
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.detectorTrustLedger
                .entries[SkipDetectorClass.fusion.rawValue]?.falseSkipWeight
                == 1.5,
            "a junk span must not launder a real miss down to its own weight"
        )
    }

    @Test("An inferred revert weighs half an explicit one (the fidelity ladder)")
    func weakSignalIsHalved() async throws {
        let (sut, store) = try await gardService(seed: Self.autoProfile())
        await sut.recordWeakFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(detector: .fusion, tier: .corroborated)
            ]
        )
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.detectorTrustLedger
                .entries[SkipDetectorClass.fusion.rawValue]?.falseSkipWeight
                == 0.5
        )
    }

    @Test("An unattributed veto still moves the show scalar and blames nobody")
    func unattributedVetoIsLegal() async throws {
        let (sut, store) = try await gardService(seed: Self.autoProfile())
        await sut.recordFalseSkipSignal(podcastId: gardPodcastId)
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(profile.recentFalseSkipSignals == 1)
        #expect(profile.detectorTrustLedger.entries.isEmpty)
    }
}

// MARK: - Manual mode escapability

@Suite("playhead-gard — manual mode is escapable", .serialized)
struct ManualModeEscapabilityTests {

    /// The one-way door, stated as arithmetic.
    ///
    /// Leaving `manual` needs `recentFalseSkipSignals == 0`. Before this bead
    /// the ONLY two methods that lower that counter or raise trust —
    /// `recordSuccessfulObservation` and `decayFalseSignals` — had zero
    /// production callers, so the counter was monotonically increasing and the
    /// show could never earn its way back. A confirmed banner was wired to
    /// `recordFalseNegativeSignal`, which lowers trust further.
    @Test("THE DOOR OPENS: correct observations walk the device row back to auto")
    func correctObservationsEscapeManual() async throws {
        let (sut, store) = try await gardService(seed: danFieldProfile())

        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .fusion) == .manual, "the starting posture")

        // manual → auto needs observations >= 8, trust >= 0.75, no signals.
        // From (obs 1, trust 0.20, signals 3): 8 confirmations clear all three.
        for _ in 0..<8 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId, detector: .fusion
            )
        }

        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .fusion) == .auto)

        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.recentFalseSkipSignals == 0,
            "the counter DECAYS — the property that made the door one-way"
        )
        #expect(
            profile.mode == SkipMode.auto.rawValue,
            "the show-level scalar escapes too, so an older binary is not stranded"
        )
    }

    @Test("Each correct observation decays exactly one unit of false-signal evidence")
    func decayIsOneUnitPerObservation() async throws {
        let (sut, store) = try await gardService(seed: danFieldProfile())
        await sut.recordCorrectObservation(
            podcastId: gardPodcastId, detector: .segmentAggregated
        )
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(profile.recentFalseSkipSignals == 2)
        #expect(
            profile.detectorTrustLedger
                .entries[SkipDetectorClass.segmentAggregated.rawValue]?
                .falseSkipWeight == 2
        )
    }

    @Test("Credit goes to the observed detector only")
    func creditIsNotShared() async throws {
        let (sut, _) = try await gardService(seed: danFieldProfile())
        for _ in 0..<8 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId, detector: .fusion
            )
        }
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .fusion) == .auto)
        #expect(
            modes.mode(for: .segmentAggregated) == .manual,
            "the aggregator earned nothing and stays where it was"
        )
    }

    @Test("A correct observation on a show with no profile creates nothing")
    func noLazyCreate() async throws {
        let (sut, store) = try await gardService(seed: nil)
        await sut.recordCorrectObservation(
            podcastId: gardPodcastId, detector: .fusion
        )
        let profile = try await store.fetchProfile(podcastId: gardPodcastId)
        #expect(profile == nil)
    }

    @Test("An explicit user override clears the stale evidence against every detector")
    func userOverrideIsNotSilentlyUndone() async throws {
        let (sut, store) = try await gardService(seed: danFieldProfile())
        await sut.setUserOverride(podcastId: gardPodcastId, mode: .auto)

        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        for detector in SkipDetectorClass.allCases {
            #expect(modes.mode(for: detector) == .auto)
        }
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.recentFalseSkipSignals == 0,
            "without this, the very next veto demotes the show the user just restored"
        )
    }

    /// The question the bead asked, answered by driving the real path.
    ///
    /// Before this bead a confirmed banner reached `recordFalseNegativeSignal`
    /// and nothing else — trust DOWN, counter untouched, mode untouched. It is
    /// now also a correct observation for the class that DREW the span, which
    /// is `.segmentAggregated` here and not `.userAsserted`: the promoted row's
    /// `boundaryState` records the tap, but the detector is who drew the edges.
    @Test("A confirmed banner IS a correct observation, credited to the detector that drew the span")
    func confirmedBannerCreditsTheDrawingDetector() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-gard")
        )
        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(danFieldProfile())
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: TrustScoringService(store: trustStore),
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        let observed = ObservedCorrectObservations()
        await orchestrator._setCorrectObservationHandlerForTesting {
            podcastId, detector in
            await observed.record(podcastId: podcastId, detector: detector)
        }
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-gard",
            podcastId: gardPodcastId
        )

        let suggested = AdWindow(
            id: "gard-suggest",
            analysisAssetId: "asset-1",
            startTime: 4800,
            endTime: 4950,
            confidence: 0.40,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: 4800,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains("gard-suggest"),
            "the window must reach the suggest tier or the tap under test never happens"
        )

        await orchestrator.acceptSuggestedSkip(windowId: "gard-suggest")
        let landed = await pollUntil(timeout: .seconds(10)) {
            await observed.count() > 0
        }
        #expect(landed, "the correct-observation write never landed")

        let recorded = await observed.snapshot()
        #expect(recorded.count == 1)
        #expect(recorded.first?.podcastId == gardPodcastId)
        #expect(recorded.first?.detector == .segmentAggregated)
    }

    /// The canary that fails if the wiring is removed.
    ///
    /// The one-way door was not a policy bug — every threshold in
    /// `TrustScoringConfig` describes a working ladder. It was an ABSENT CALL
    /// SITE, which no behavioural test of the service can see. So the
    /// production caller is asserted directly, the same way this repo pins its
    /// other funnel-discipline invariants.
    @Test("The banner-confirm path calls recordCorrectObservation in PRODUCTION source")
    func theEscapeHasAProductionCallSite() throws {
        let source = try String(
            contentsOf: productionSourceURL(
                "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
            ),
            encoding: .utf8
        )
        #expect(
            source.contains("recordCorrectObservation("),
            """
            SkipOrchestrator no longer calls recordCorrectObservation. \
            That is the one-way door returning: with no production caller, \
            recentFalseSkipSignals only ever increases and no show can leave \
            manual mode.
            """
        )
    }
}

// MARK: - Orchestrator wiring

@Suite("playhead-gard — the skip gate reads the detector's mode", .serialized)
struct PerDetectorSkipGateTests {

    private static func cueCount(_ cues: [CMTimeRange]) -> Int { cues.count }

    /// An orchestrator over Dan's exact device row.
    private static func makeFieldOrchestrator() async throws -> SkipOrchestrator {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(danFieldProfile())
        return SkipOrchestrator(
            store: store,
            trustService: TrustScoringService(store: trustStore)
        )
    }

    @Test("THE ACCEPTANCE: the demoted show auto-skips byte-exact rediff")
    func rediffSkipsOnADemotedShow() async throws {
        let orchestrator = try await Self.makeFieldOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: gardPodcastId
        )
        #expect(
            await orchestrator.currentSkipMode() == .manual,
            "the show-level mode is still manual — the pill is unchanged"
        )

        await orchestrator.receiveAdWindows([rediffWindow()])
        #expect(
            Self.cueCount(pushedCues) == 1,
            "a byte-exact span is not gated by the aggregator's history"
        )
    }

    @Test("…and the aggregator that earned the demotion still does NOT skip")
    func aggregatorStaysBlocked() async throws {
        let orchestrator = try await Self.makeFieldOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: gardPodcastId
        )

        await orchestrator.receiveAdWindows([
            aggregatorWindow(confidence: 0.95)
        ])
        #expect(
            Self.cueCount(pushedCues) == 0,
            "the class with the bad history is exactly the one that stays blocked"
        )
    }

    @Test("A session override governs EVERY detector, exemption included")
    func sessionOverrideCoversTheExemptClass() async throws {
        let orchestrator = try await Self.makeFieldOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: gardPodcastId
        )
        await orchestrator.setActiveSkipMode(.shadow)

        await orchestrator.receiveAdWindows([rediffWindow()])
        #expect(
            Self.cueCount(pushedCues) == 0,
            "exempt from the show's HISTORY, never from a live instruction"
        )
    }

    @Test("A veto is attributed to the detector that DREW the span, through the real orchestrator seam")
    func revertAttributesToTheDrawingDetector() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(
            gardProfile(
                mode: SkipMode.auto.rawValue, trustScore: 0.9,
                observations: 10, falseSignals: 0
            )
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: TrustScoringService(store: trustStore),
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: gardPodcastId
        )

        let window = aggregatorWindow(id: "gard-revert", confidence: 0.95)
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])
        #expect(
            await orchestrator.revertWindow(
                windowId: "gard-revert", podcastId: gardPodcastId
            )
        )

        // The trust write at this seam is fire-and-forget (`Task { … }`), so
        // reading straight through races it. This suite passed alone and went
        // red under the mutation battery's focused set, which is exactly the
        // load the poll exists for.
        let landed = await pollUntil(timeout: .seconds(10)) {
            guard let profile = try? await trustStore.fetchProfile(
                podcastId: gardPodcastId
            ) else { return false }
            return profile.detectorTrustLedger
                .entries[SkipDetectorClass.segmentAggregated.rawValue] != nil
        }
        #expect(landed, "the attributed veto never reached the profile")

        let profile = try #require(
            await trustStore.fetchProfile(podcastId: gardPodcastId)
        )
        let ledger = profile.detectorTrustLedger
        #expect(
            ledger.entries[SkipDetectorClass.segmentAggregated.rawValue]?
                .falseSkipWeight == DetectorVetoWeight.weight(for: .none),
            "the aggregator drew this span and its edges were unanchored"
        )
        #expect(
            ledger.entries[SkipDetectorClass.rediffByteExact.rawValue]?
                .falseSkipWeight == 0,
            "nothing about an aggregator miss is evidence against a byte differ"
        )
    }

    @Test("An unresolved show identity still fires nothing (playhead-djl0 holds)")
    func unresolvedIdentityIsNonActioning() async throws {
        let orchestrator = try await Self.makeFieldOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: nil
        )
        await orchestrator.receiveAdWindows([rediffWindow()])
        #expect(Self.cueCount(pushedCues) == 0)
    }
}

// MARK: - Collectors

/// Records the per-detector correct observations the orchestrator issues.
private actor ObservedCorrectObservations {
    struct Record: Sendable, Equatable {
        let podcastId: String
        let detector: SkipDetectorClass
    }

    private var records: [Record] = []

    func record(podcastId: String, detector: SkipDetectorClass) {
        records.append(Record(podcastId: podcastId, detector: detector))
    }

    func count() -> Int { records.count }
    func snapshot() -> [Record] { records }
}

// MARK: - Source access

/// Resolve a repo-relative production source path from the test bundle.
///
/// Mirrors the existing canary suites: walk up from this file's location until
/// the repo root is reached. `#filePath` is the anchor because the test bundle
/// does not carry production sources.
private func productionSourceURL(_ relativePath: String) -> URL {
    var url = URL(fileURLWithPath: #filePath)
    // …/PlayheadTests/Services/TrustScoring/PerDetectorTrustTests.swift
    for _ in 0..<4 { url.deleteLastPathComponent() }
    return url.appendingPathComponent(relativePath)
}
