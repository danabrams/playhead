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

        // The three classes are NAMED, not filtered by `consultsShowTrust`.
        // The mutation battery caught the filtered form: a mutation that makes
        // every class exempt empties the loop and the test passes describing
        // nothing. `what would this read if the thing never happened?` —
        // feedback_ask_what_the_quantity_measures_2026-07-29, applied to a
        // test's own iteration set.
        for detector in [SkipDetectorClass.segmentAggregated, .userAsserted, .fusion] {
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
    /// playhead-lqcp REVISED THE DESTINATION OF THIS TEST, not its subject.
    ///
    /// The door this bead opened is the one out of a VETO — the false-signal
    /// counter decaying to 0 — and that is still exactly what happens. What no
    /// longer happens is the last step, `manual -> auto`: it is closed at
    /// `AutoPromotionConfidenceEvidence.unavailable` because Dan's ruling makes
    /// auto conditional on HIGH CONFIDENCE and no quantity on this tree can
    /// evaluate that. Auto is the rung that cuts audio with no gesture, so the
    /// unevaluable conditional resolves closed.
    ///
    /// The assertion is therefore `.manual` with the counter at 0 — a show that
    /// has earned back everything it can earn, standing at the closed rung.
    @Test("THE DOOR OPENS: correct observations walk the device row back out of the veto")
    func correctObservationsEscapeManual() async throws {
        let (sut, store) = try await gardService(seed: danFieldProfile())

        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .fusion) == .manual, "the starting posture")

        // Eight confirmations, each on a DIFFERENT episode (playhead-fh5v: the
        // show's `observationCount` is claimed per `(podcastId,
        // analysisAssetId)`, so eight taps on one asset would be one episode).
        for index in 0..<8 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "asset-escape-\(index)",
                detector: .fusion
            )
        }

        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .fusion) == .manual,
            "playhead-lqcp: the observations clear every legacy clause, and the auto rung is closed anyway; got \(String(describing: modes.mode(for: .fusion)))"
        )

        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.recentFalseSkipSignals == 0,
            "the counter DECAYS — the property that made the door one-way"
        )
        #expect(
            abs(profile.skipTrustScore - 1.0) < 1e-9,
            "and trust is at the ceiling, so nothing but the closed rung is holding it; got \(profile.skipTrustScore)"
        )
        #expect(
            profile.mode == SkipMode.manual.rawValue,
            "the show-level scalar stands at the same closed rung as the detector"
        )
        // playhead-fh5v: eight taps on eight distinct episodes are eight
        // episodes, and the ledger is the independent witness for that.
        #expect(profile.observationCount == 9, "the seeded 1 plus eight claimed episodes; got \(profile.observationCount)")
        #expect(try await store.episodeTrustObservationCount(podcastId: gardPodcastId) == 8)
    }

    /// playhead-fh5v: the same eight taps INSIDE ONE EPISODE.
    ///
    /// Before the fix this method was a second, unledgered writer of
    /// `observationCount`, so this scenario bought eight episodes of credit for
    /// one episode — and made the bead's own "the two numbers must agree"
    /// diagnostic unusable, since one side had no rows to count. Everything
    /// per-GESTURE still moves eight times; only the episode count is claimed.
    @Test("playhead-fh5v: eight banner Yeses in ONE episode count as ONE episode")
    func repeatedTapsInOneEpisodeCountOnce() async throws {
        let (sut, store) = try await gardService(seed: danFieldProfile())

        for _ in 0..<8 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "asset-one-episode",
                detector: .fusion
            )
        }

        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.observationCount == 2,
            "the seeded 1 plus the ONE episode these taps were about; got \(profile.observationCount)"
        )
        #expect(
            try await store.episodeTrustObservationCount(podcastId: gardPodcastId) == 1,
            "and the ledger agrees, which is the whole point of having it"
        )
        // The per-gesture quantities are untouched by the claim: eight decays
        // floor the veto counter at 0 and eight bonuses cap trust at 1.0.
        #expect(profile.recentFalseSkipSignals == 0)
        #expect(abs(profile.skipTrustScore - 1.0) < 1e-9)
    }

    /// The claim is shared with the backfill, not a private one — so an episode
    /// the analysis lane already counted is not counted a second time by a tap.
    @Test("playhead-fh5v: a Yes on an episode the backfill already claimed adds no episode")
    func tapOnAnAlreadyClaimedEpisodeAddsNothing() async throws {
        let (sut, store) = try await gardService(seed: danFieldProfile())

        // Stand in for the backfill: it takes the claim first.
        #expect(try await store.claimEpisodeTrustObservation(
            podcastId: gardPodcastId, analysisAssetId: "asset-backfilled"
        ))

        await sut.recordCorrectObservation(
            podcastId: gardPodcastId,
            analysisAssetId: "asset-backfilled",
            detector: .fusion
        )

        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.observationCount == 1,
            "unchanged from the seed — the episode was already counted; got \(profile.observationCount)"
        )
        #expect(try await store.episodeTrustObservationCount(podcastId: gardPodcastId) == 1)
        // But the gesture still did its per-gesture work.
        #expect(profile.recentFalseSkipSignals == 2, "one unit of veto evidence decayed")
    }

    @Test("Each correct observation decays exactly one unit of false-signal evidence")
    func decayIsOneUnitPerObservation() async throws {
        let (sut, store) = try await gardService(seed: danFieldProfile())
        await sut.recordCorrectObservation(
            podcastId: gardPodcastId,
            analysisAssetId: "asset-decay-1",
            detector: .segmentAggregated
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

    /// Credit is still per-class; what changed under playhead-lqcp is that the
    /// two classes are now separated by their EVIDENCE rather than by their
    /// destination. `.fusion` earns eight confirmations and a cleared veto
    /// counter; `.segmentAggregated` earns nothing and keeps its own weight.
    /// Both stand at `.manual` because the auto rung is closed for everybody,
    /// so the ledger's per-class state is what this asserts.
    @Test("Credit goes to the observed detector only")
    func creditIsNotShared() async throws {
        let (sut, store) = try await gardService(seed: danFieldProfile())
        for index in 0..<8 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "asset-credit-\(index)",
                detector: .fusion
            )
        }
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .fusion) == .manual)
        #expect(
            modes.mode(for: .segmentAggregated) == .manual,
            "the aggregator earned nothing and stays where it was"
        )
        // The modes agree, so the separation has to be read off the ledger —
        // otherwise this test would pass with credit shared show-wide.
        let ledger = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        ).detectorTrustLedger
        let fusion = try #require(ledger.entries[SkipDetectorClass.fusion.rawValue])
        let aggregator = try #require(
            ledger.entries[SkipDetectorClass.segmentAggregated.rawValue]
        )
        #expect(fusion.falseSkipWeight == 0, "eight decays cleared it")
        #expect(
            aggregator.falseSkipWeight == 3,
            "the aggregator's own seeded weight is untouched; got \(aggregator.falseSkipWeight)"
        )
        #expect(fusion.trustScore > aggregator.trustScore)
    }

    @Test("A correct observation on a show with no profile creates nothing")
    func noLazyCreate() async throws {
        let (sut, store) = try await gardService(seed: nil)
        await sut.recordCorrectObservation(
            podcastId: gardPodcastId,
            analysisAssetId: "asset-nolazy",
            detector: .fusion
        )
        let profile = try await store.fetchProfile(podcastId: gardPodcastId)
        #expect(profile == nil)
        // playhead-fh5v: and the episode's claim must NOT have been spent on a
        // mutation that never happened — a burnt claim would cost this episode
        // its credit permanently once the profile does appear.
        #expect(
            try await store.episodeTrustObservationCount(podcastId: gardPodcastId) == 0,
            "no profile, no observation, therefore no claim"
        )
        #expect(
            try await store.claimEpisodeTrustObservation(
                podcastId: gardPodcastId, analysisAssetId: "asset-nolazy"
            ),
            "the episode is still claimable afterwards"
        )
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
        #expect(profile.recentFalseSkipSignals == 0)

        // The claim stated as BEHAVIOUR, because the counters are only
        // interesting for what they do next. One ordinary unanchored veto
        // weighs 0.5. Against a cleared ledger that leaves the class in `auto`;
        // against the three stale signals it would be 3.5 and the override the
        // listener just gave would be undone by their first correction.
        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(detector: .fusion, tier: .none)
            ]
        )
        let after = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            after.mode(for: .fusion) == .auto,
            "one veto must not undo an explicit user instruction"
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
            podcastId, analysisAssetId, detector in
            await observed.record(
                podcastId: podcastId,
                analysisAssetId: analysisAssetId,
                detector: detector
            )
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
        // playhead-fh5v / R3: the tap must carry the SUGGESTION's asset, which
        // is what `trust_episode_observations` is claimed against. Before this
        // assertion the production id could be replaced with `""` and the whole
        // suite still passed — and an empty id is refused by the claim, so a
        // banner Yes would silently stop being able to count an episode.
        #expect(
            recorded.first?.analysisAssetId == "asset-1",
            "the claim key must be the suggestion's own episode; got \(String(describing: recorded.first?.analysisAssetId))"
        )
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
        /// R3: the episode the tap was about. `observationCount` is claimed per
        /// `(podcastId, analysisAssetId)`, so this argument is what decides
        /// whether a banner Yes can count an episode at all — and it had no
        /// coverage until this field existed.
        let analysisAssetId: String
        let detector: SkipDetectorClass
    }

    private var records: [Record] = []

    func record(
        podcastId: String,
        analysisAssetId: String,
        detector: SkipDetectorClass
    ) {
        records.append(
            Record(
                podcastId: podcastId,
                analysisAssetId: analysisAssetId,
                detector: detector
            )
        )
    }

    func count() -> Int { records.count }
    func snapshot() -> [Record] { records }
}

// MARK: - Self-promotion reaches the gate, not just the pill (R4)

/// The bead's promotion has to move the value the SKIP GATE reads.
///
/// `SkipOrchestrator.skipMode(for:)` resolves
/// `activeDetectorSkipModes.mode(for:)` — a per-class verdict — and a class
/// tracks `profile.mode` only for as long as it has NO stored ledger entry.
/// Every production veto site names a detector, and an attributed gesture
/// materializes the whole ledger, so the first time a listener rejects
/// anything, every class is pinned. Measured before this was fixed: show
/// scalar `manual`, `.fusion` and `.segmentAggregated` still `shadow`, forever.
@Suite("playhead-mn5e/R4 — self-promotion reaches the per-detector gate", .serialized)
struct SelfObservationReachesTheGateTests {
    /// One veto, then ten clean episodes. The gate must move.
    @Test("a forked ledger still promotes the classes that drew the evidence")
    func selfPromotionReachesTheDetectorGate() async throws {
        let (sut, store) = try await gardService(
            seed: gardProfile(
                mode: SkipMode.shadow.rawValue,
                trustScore: 0.2,
                observations: 0,
                falseSignals: 0
            )
        )
        // Day 0. `.rediffByteExact` seeds at `.auto` with no profile history,
        // so a byte-exact span auto-skips on a brand-new show. The listener
        // rejects that one skip. The gesture NAMES a detector, so it forks the
        // ledger: every class gets a stored entry pinned at the pre-veto
        // profile, which is `shadow`.
        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(
                    detector: .rediffByteExact,
                    tier: .deterministic
                )
            ]
        )
        let forked = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        try #require(
            forked.detectorTrustJSON != nil,
            "the veto must have forked the ledger, or this test proves nothing"
        )
        // Ten clean episodes of backfill self-observation, each confirming
        // windows drawn by the aggregator and the fusion bucket — the
        // production caller playhead-mn5e adds.
        for _ in 0..<10 {
            await sut.recordSuccessfulObservation(
                podcastId: gardPodcastId,
                averageConfidence: 0.8,
                detectors: [.segmentAggregated, .fusion]
            )
        }
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            SkipMode(rawValue: profile.mode) == .manual,
            "the show scalar must have promoted; got \(profile.mode)"
        )
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.showMode == .manual)
        #expect(
            modes.mode(for: .fusion) == .manual,
            "fusion is what the skip gate reads; got \(modes.mode(for: .fusion))"
        )
        #expect(
            modes.mode(for: .segmentAggregated) == .manual,
            "segmentAggregated got \(modes.mode(for: .segmentAggregated))"
        )
    }

    /// Credit is not shared. A class that drew nothing this episode has earned
    /// nothing — the same rule blame follows, and the reason gard exists.
    @Test("a class that drew no window this episode is not credited")
    func unnamedClassIsNotCredited() async throws {
        let (sut, store) = try await gardService(
            seed: gardProfile(
                mode: SkipMode.shadow.rawValue,
                trustScore: 0.2,
                observations: 0,
                falseSignals: 0
            )
        )
        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(
                    detector: .rediffByteExact,
                    tier: .deterministic
                )
            ]
        )
        for _ in 0..<10 {
            await sut.recordSuccessfulObservation(
                podcastId: gardPodcastId,
                averageConfidence: 0.8,
                detectors: [.fusion]
            )
        }
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .fusion) == .manual)
        #expect(
            modes.mode(for: .segmentAggregated) == .shadow,
            "the aggregator produced nothing on these episodes and must not ride the fusion bucket's credit; got \(modes.mode(for: .segmentAggregated))"
        )
    }

    /// A show that has never diverged keeps writing NULL. The seed already
    /// tracks the scalar there, and forking on every backfill would start
    /// persisting a ledger for every show on the device.
    @Test("an un-forked ledger stays NULL through self-observation")
    func virginLedgerIsNotForkedByABackfill() async throws {
        let (sut, store) = try await gardService(
            seed: gardProfile(
                mode: SkipMode.shadow.rawValue,
                trustScore: 0.2,
                observations: 0,
                falseSignals: 0
            )
        )
        for _ in 0..<10 {
            await sut.recordSuccessfulObservation(
                podcastId: gardPodcastId,
                averageConfidence: 0.8,
                detectors: [.segmentAggregated, .fusion]
            )
        }
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.detectorTrustJSON == nil,
            "a show with no gesture history must stay byte-identical to a pre-gard row; got \(String(describing: profile.detectorTrustJSON))"
        )
        // …and the seed carries the promotion to every show-governed class.
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(modes.mode(for: .fusion) == .manual)
        #expect(modes.mode(for: .segmentAggregated) == .manual)
    }

    /// Self-observation must not launder a veto. The weighted counter the
    /// per-detector demotion reads is carried through untouched, exactly as
    /// the show's `recentFalseSkipSignals` already was.
    @Test("self-observation does not decay a detector's false-skip weight")
    func selfObservationDoesNotDecayFalseSkipWeight() async throws {
        let (sut, store) = try await gardService(
            seed: gardProfile(
                mode: SkipMode.manual.rawValue,
                trustScore: 0.5,
                observations: 5,
                falseSignals: 0
            )
        )
        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(detector: .fusion, tier: .deterministic)
            ]
        )
        let charged = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        let chargedWeight = DetectorTrustLedger
            .decode(charged.detectorTrustJSON)
            .entry(for: .fusion, seededFrom: charged)
            .falseSkipWeight
        try #require(chargedWeight > 0, "the veto must have charged the class")

        for _ in 0..<10 {
            await sut.recordSuccessfulObservation(
                podcastId: gardPodcastId,
                averageConfidence: 0.8,
                detectors: [.fusion]
            )
        }
        let after = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        let afterWeight = DetectorTrustLedger
            .decode(after.detectorTrustJSON)
            .entry(for: .fusion, seededFrom: after)
            .falseSkipWeight
        #expect(
            afterWeight == chargedWeight,
            "only a banner Yes may decay a veto; got \(afterWeight) from \(chargedWeight)"
        )
    }
}

// MARK: - Restoration to the seed on decay (playhead-u0vv)

/// The exemption was written as a SEED and behaved as a default that a listener
/// lost on their first gesture.
///
/// `SkipDetectorClass.showIndependentSeedMode` is read by
/// `DetectorTrustLedger.seed` only for a class with NO stored entry — and the
/// first ATTRIBUTED gesture of any kind materializes all four classes, which
/// every production veto site sends. From that moment `.rediffByteExact` has a
/// stored entry forever, two vetoes demote it (1.5 + 1.5 = 3.0, over the
/// threshold of 2), and playhead-lqcp had removed the only route back. Two
/// gestures permanently disabled the one signal that works on a FIRST listen,
/// before any transcript exists.
///
/// Dan's ruling: the class returns to its authority's mode once its
/// `falseSkipWeight` has DECAYED to 0. Vetoes still demote exactly as
/// playhead-gard designed; banner Yeses earn it back.
///
/// **Restoration is not promotion, and these tests are where that is held.**
/// A promotion asks whether a class's own record earned a higher mode; a
/// restoration returns it to a mode a different authority already granted. The
/// unit tests below pin each of the three guards separately, because each one
/// is a place a promotion would have said yes.
@Suite("playhead-u0vv — a discharged veto restores the class's authority mode")
struct SeedRestorationTests {

    // MARK: The three guards, one test each

    @Test("The restored mode is the AUTHORITY's, never the function's own")
    func restorationReturnsTheAuthoritysMode() {
        // The payload is `.manual` here, so a `restoredMode` that names `.auto`
        // itself — rather than reading the authority — fails this outright.
        #expect(
            DetectorTrustLedger.restoredMode(
                under: .showIndependentSeed(.manual),
                currentMode: .shadow,
                weightBefore: 3.0,
                weightAfter: 0
            ) == .manual,
            "a seed retuned below auto must restore below auto"
        )
        #expect(
            DetectorTrustLedger.restoredMode(
                under: .showIndependentSeed(.shadow),
                currentMode: .manual,
                weightBefore: 1.0,
                weightAfter: 0
            ) == .shadow
        )
    }

    @Test("Only a class the show's history does NOT govern has an authority")
    func authorityIsTheExemptClassOnly() {
        for detector in SkipDetectorClass.allCases {
            if detector.consultsShowTrust {
                #expect(
                    detector.modeAuthority == nil,
                    "\(detector.rawValue) is governed by the show's own record; nothing may restore it over that"
                )
            } else {
                #expect(
                    detector.modeAuthority
                        == .showIndependentSeed(
                            SkipDetectorClass.showIndependentSeedMode
                        ),
                    "the authority must CARRY the seed constant, so retuning the seed retunes the restoration; got \(String(describing: detector.modeAuthority))"
                )
            }
        }
        // Named, not filtered: a mutation that makes every class exempt would
        // otherwise empty the `if` arm and this test would describe nothing.
        #expect(SkipDetectorClass.rediffByteExact.modeAuthority != nil)
        for detector in [SkipDetectorClass.segmentAggregated, .userAsserted, .fusion] {
            #expect(detector.modeAuthority == nil)
        }
    }

    @Test("A class with no authority is left exactly where it stands")
    func noAuthorityRestoresNothing() {
        #expect(
            DetectorTrustLedger.restoredMode(
                under: nil,
                currentMode: .manual,
                weightBefore: 3.0,
                weightAfter: 0
            ) == .manual
        )
        #expect(
            DetectorTrustLedger.restoredMode(
                under: SkipDetectorClass.fusion.modeAuthority,
                currentMode: .shadow,
                weightBefore: 3.0,
                weightAfter: 0
            ) == .shadow
        )
    }

    @Test("A weight that was ALREADY zero has not decayed to zero")
    func restorationNeedsADischarge() {
        let authority = SkipDetectorClass.rediffByteExact.modeAuthority
        #expect(
            DetectorTrustLedger.restoredMode(
                under: authority,
                currentMode: .manual,
                weightBefore: 0,
                weightAfter: 0
            ) == .manual,
            "nothing was owed, so nothing was discharged — this is what stops a Yes from overwriting a mode somebody set deliberately"
        )
        #expect(
            DetectorTrustLedger.restoredMode(
                under: authority,
                currentMode: .manual,
                weightBefore: 3.0,
                weightAfter: 2.0
            ) == .manual,
            "a PARTIAL discharge restores nothing"
        )
        #expect(
            DetectorTrustLedger.restoredMode(
                under: authority,
                currentMode: .manual,
                weightBefore: 1.0,
                weightAfter: 0
            ) == SkipDetectorClass.showIndependentSeedMode,
            "…and the last unit does"
        )
    }

    // MARK: The bead, at the service

    /// A fresh show, the exact sequence the bead describes.
    @Test("THE BEAD: two vetoes disable byte-exact rediff, and banner Yeses earn it back")
    func vetoesAreNoLongerPermanent() async throws {
        let (sut, store) = try await gardService(seed: freshShowProfile())

        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .auto,
            "day 0: the byte differ needs no show history"
        )

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
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .manual,
            "the demotion is playhead-gard's and stands; got \(modes.mode(for: .rediffByteExact))"
        )

        // 3.0 of weight, one unit per banner Yes. The first two decay it; the
        // third discharges the last of it and the class returns to its seed.
        for index in 0..<2 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "asset-u0vv-\(index)",
                detector: .rediffByteExact
            )
            modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
            #expect(
                modes.mode(for: .rediffByteExact) == .manual,
                "still owed after \(index + 1) of 3; got \(modes.mode(for: .rediffByteExact))"
            )
        }
        await sut.recordCorrectObservation(
            podcastId: gardPodcastId,
            analysisAssetId: "asset-u0vv-2",
            detector: .rediffByteExact
        )
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .auto,
            "THE FIX: the debt is discharged, so the class returns to the mode its authority granted; got \(modes.mode(for: .rediffByteExact))"
        )
        let profile = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        #expect(
            profile.detectorTrustLedger
                .entries[SkipDetectorClass.rediffByteExact.rawValue]?
                .falseSkipWeight == 0
        )
    }

    /// BOUND: a class the show's history governs must never reach `.auto` this
    /// way — and this one clears every clause playhead-lqcp deleted, so it is
    /// also the re-pin of that closure.
    @Test("BOUND: a show-governed class is NOT restored, however clean its record")
    func showGovernedClassIsNeverRestored() async throws {
        let (sut, store) = try await gardService(
            seed: gardProfile(
                mode: SkipMode.auto.rawValue, trustScore: 0.9,
                observations: 10, falseSignals: 0
            )
        )
        for _ in 0..<2 {
            await sut.recordFalseSkipSignal(
                podcastId: gardPodcastId,
                attributions: [
                    DetectorVetoAttribution(detector: .fusion, tier: .corroborated)
                ]
            )
        }
        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        try #require(
            modes.mode(for: .fusion) == .manual,
            "the demotion must have happened or this test proves nothing"
        )

        for index in 0..<2 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "asset-u0vv-fusion-\(index)",
                detector: .fusion
            )
        }
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .fusion) == .manual,
            "fusion has no authority above the show's own record; got \(modes.mode(for: .fusion))"
        )

        let stored = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        let entry = try #require(
            stored.detectorTrustLedger
                .entries[SkipDetectorClass.fusion.rawValue]
        )
        #expect(entry.falseSkipWeight == 0, "the debt IS discharged — that is not the reason it stayed")
        #expect(
            entry.observationCount >= TrustScoringConfig.default.manualToAutoObservations,
            "and it clears the observation clause lqcp deleted; got \(entry.observationCount)"
        )
        #expect(
            entry.trustScore >= TrustScoringConfig.default.manualToAutoTrustScore,
            "and the trust clause too; got \(entry.trustScore)"
        )
    }

    /// BOUND: the MODE is restored and nothing else. A restored class carries
    /// the record it actually earned — which here clears NEITHER promotion
    /// clause, so a fabricated `observationCount` or `trustScore` would show up
    /// as a class that could self-certify next time.
    @Test("BOUND: restoration moves the mode only — no observation or trust is invented")
    func restorationFabricatesNothing() async throws {
        let (sut, store) = try await gardService(seed: freshShowProfile())
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
        for index in 0..<3 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "asset-u0vv-only-\(index)",
                detector: .rediffByteExact
            )
        }
        let stored = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        let entry = try #require(
            stored.detectorTrustLedger
                .entries[SkipDetectorClass.rediffByteExact.rawValue]
        )
        #expect(entry.skipMode == .auto, "the restoration happened")
        // The seed is trust 0.5; two vetoes took 0.10 each; three Yeses gave
        // 0.10 each. Nothing rounded up to a promotion threshold.
        #expect(
            abs(entry.trustScore - 0.6) < 1e-9,
            "0.5 - 0.2 + 0.3, exactly what the gestures were worth; got \(entry.trustScore)"
        )
        #expect(
            entry.observationCount == 3,
            "three Yeses, not the eight a promotion would have needed; got \(entry.observationCount)"
        )
        #expect(
            entry.trustScore < TrustScoringConfig.default.manualToAutoTrustScore,
            "it holds `.auto` while FAILING the promotion trust clause — which is the difference between a restoration and a promotion"
        )
        #expect(
            entry.observationCount < TrustScoringConfig.default.manualToAutoObservations,
            "and the observation clause"
        )
    }

    /// BOUND: a return trip, not immunity.
    @Test("BOUND: a restored class still demotes on the next two vetoes")
    func restorationIsNotImmunity() async throws {
        let (sut, _) = try await gardService(seed: freshShowProfile())
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
        for index in 0..<3 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "asset-u0vv-again-\(index)",
                detector: .rediffByteExact
            )
        }
        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        try #require(modes.mode(for: .rediffByteExact) == .auto)

        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(
                    detector: .rediffByteExact, tier: .deterministic
                )
            ]
        )
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .auto,
            "1.5 is under the threshold of 2, exactly as before the restoration"
        )
        await sut.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(
                    detector: .rediffByteExact, tier: .deterministic
                )
            ]
        )
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .manual,
            "…and the second crosses it. The user can still shut this off; got \(modes.mode(for: .rediffByteExact))"
        )
    }

    /// The discharge guard, stated as the behaviour it protects.
    ///
    /// `setUserOverride` writes EVERY class's entry at `falseSkipWeight: 0`
    /// with the mode the listener chose. A restoration keyed on the STATE
    /// "weight is zero" would let the listener's very next banner Yes overwrite
    /// that instruction with the seed — a live user instruction undone by an
    /// automatic path, which is `feedback_manual_marks_override_2026-07-29`
    /// pointed the wrong way. Keyed on the DECAY, it cannot.
    @Test("A listener who turned auto-skip OFF is not overruled by their next Yes")
    func explicitOverrideSurvivesACorrectObservation() async throws {
        let (sut, _) = try await gardService(
            seed: gardProfile(
                mode: SkipMode.auto.rawValue, trustScore: 0.9,
                observations: 10, falseSignals: 0
            )
        )
        await sut.setUserOverride(podcastId: gardPodcastId, mode: .manual)
        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        try #require(
            modes.mode(for: .rediffByteExact) == .manual,
            "the override reaches the exempt class too"
        )

        await sut.recordCorrectObservation(
            podcastId: gardPodcastId,
            analysisAssetId: "asset-u0vv-override",
            detector: .rediffByteExact
        )
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .manual,
            "a Yes on a class that owed nothing must not restore anything; got \(modes.mode(for: .rediffByteExact))"
        )
    }

    /// R6: the structural claim — "nothing a class ACCUMULATES can reach this
    /// decision" — was carried by `restoredMode`'s SIGNATURE and by nothing
    /// behavioural, and a signature is not a test.
    ///
    /// `explicitOverrideSurvivesACorrectObservation` above looks like it covers
    /// the case and does not: its seed profile carries 10 observations, but
    /// `DetectorTrustLedger.seed` deliberately DISCARDS the show's record for
    /// `.rediffByteExact`, so the entry `setUserOverride` writes has
    /// `observationCount == 0`. A mutant that routes the class's own record in
    /// through the weight argument — `weightBefore: entry.falseSkipWeight +
    /// Double(entry.observationCount)` — therefore passed the entire suite
    /// (R6/MU2, exit 0). In production that mutant undoes an explicit override
    /// with ONE banner Yes on any class that has already earned a record, which
    /// is the state every restored class is in.
    ///
    /// So the override here lands on an entry that has genuinely accumulated
    /// one, and the `#require`s below are what stop this test decaying back
    /// into the vacuous one.
    @Test("An override lands on an EARNED record, and the next Yes still restores nothing")
    func overrideSurvivesEvenOnAnEarnedRecord() async throws {
        let (sut, store) = try await gardService(seed: freshShowProfile())
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
        for index in 0..<3 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "r6-earned-\(index)",
                detector: .rediffByteExact
            )
        }
        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        try #require(
            modes.mode(for: .rediffByteExact) == .auto,
            "the class must have made the round trip, or there is no record to override"
        )

        await sut.setUserOverride(podcastId: gardPodcastId, mode: .manual)
        let stored = try #require(
            await store.fetchProfile(podcastId: gardPodcastId)
        )
        let entry = try #require(
            stored.detectorTrustLedger
                .entries[SkipDetectorClass.rediffByteExact.rawValue]
        )
        try #require(
            entry.observationCount >= 3,
            "the entry must CARRY a record here or the mutation this test kills is unreachable; got \(entry.observationCount)"
        )
        try #require(
            entry.falseSkipWeight == 0,
            "and it must owe nothing, so the ONLY thing that could fire a restoration is the record itself"
        )

        await sut.recordCorrectObservation(
            podcastId: gardPodcastId,
            analysisAssetId: "r6-earned-after-override",
            detector: .rediffByteExact
        )
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .manual,
            "nothing a class ACCUMULATED may substitute for a discharge; got \(modes.mode(for: .rediffByteExact))"
        )
    }

    /// R6: the restoration must not have SWALLOWED the ladder it wraps.
    ///
    /// `applyCorrectObservation` computes `evaluatePromotion` and then hands its
    /// result to `restoredMode` as `currentMode`. Feeding `entry.skipMode`
    /// instead — dropping the promotion on the floor for every class — passed
    /// the whole suite (R6/MU3, exit 0), because every existing test that
    /// exercises this path starts from an entry already at `.manual`. What that
    /// mutant silently removes is the escape hatch `applyCorrectObservation`'s
    /// own doc comment names: "that class could never leave `shadow` on its own
    /// evidence".
    @Test("A banner Yes still promotes a class OUT OF SHADOW — the restoration did not swallow the ladder")
    func correctObservationsStillPromoteOutOfShadow() async throws {
        let (sut, _) = try await gardService(seed: freshShowProfile())
        var modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        try #require(
            modes.mode(for: .segmentAggregated) == .shadow,
            "a show-governed class starts at the show's mode, which is shadow here"
        )
        for index in 0..<3 {
            await sut.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "r6-shadow-\(index)",
                detector: .segmentAggregated
            )
        }
        modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .segmentAggregated) == .manual,
            "three Yeses is 3 observations at trust 0.5, which clears shadow -> manual; got \(modes.mode(for: .segmentAggregated))"
        )
    }

    /// Self-observation must not be able to restore. Only a listener's gesture
    /// decays a weight, so only a listener's gesture can discharge one — the
    /// property that keeps the detector from arguing its own case.
    @Test("Ten clean backfills do NOT restore a demoted class")
    func selfObservationCannotRestore() async throws {
        let (sut, _) = try await gardService(seed: freshShowProfile())
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
        for _ in 0..<10 {
            await sut.recordSuccessfulObservation(
                podcastId: gardPodcastId,
                averageConfidence: 0.8,
                detectors: [.rediffByteExact]
            )
        }
        let modes = await sut.resolveDetectorModes(podcastId: gardPodcastId)
        #expect(
            modes.mode(for: .rediffByteExact) == .manual,
            "the detector's own output is not evidence against the listener's veto; got \(modes.mode(for: .rediffByteExact))"
        )
    }
}

/// A show nobody has vetoed yet: the state the bead's sequence starts from.
private func freshShowProfile() -> PodcastProfile {
    gardProfile(
        mode: SkipMode.shadow.rawValue,
        trustScore: 0.2,
        observations: 0,
        falseSignals: 0
    )
}

// MARK: - The probe: the SKIP GATE, not the ledger struct (playhead-u0vv)

/// R4's lesson, applied to this bead: a test that asserts on the ledger cannot
/// prove the gate agrees.
///
/// `SkipOrchestrator.skipMode(for:)` reads `activeDetectorSkipModes`, resolved
/// once per episode — so the whole round trip is driven here through the real
/// orchestrator, and the assertion is whether a byte-exact span produces a SKIP
/// CUE. Every veto carries an ATTRIBUTION, which is the shape production sends
/// and the shape that forks the ledger; an unattributed veto is the one shape
/// that would not reproduce the state this bead is about.
@Suite("playhead-u0vv — restoration reaches the skip gate", .serialized)
struct SeedRestorationReachesTheGateTests {

    private static func makeProbe() async throws -> (SkipOrchestrator, TrustScoringService) {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(
            gardProfile(
                mode: SkipMode.shadow.rawValue, trustScore: 0.2,
                observations: 0, falseSignals: 0
            )
        )
        let trust = TrustScoringService(store: trustStore)
        return (
            SkipOrchestrator(store: store, trustService: trust),
            trust
        )
    }

    /// Re-resolve trust for the episode and offer a fresh byte-exact span.
    /// Returns the cues the orchestrator pushed — 1 means it auto-skips.
    private static func cuesForAByteExactSpan(
        _ orchestrator: SkipOrchestrator,
        windowId: String
    ) async -> Int {
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: gardPodcastId
        )
        await orchestrator.receiveAdWindows([rediffWindow(id: windowId)])
        return pushedCues.count
    }

    private static func vetoAByteExactSpan(_ trust: TrustScoringService) async {
        await trust.recordFalseSkipSignal(
            podcastId: gardPodcastId,
            attributions: [
                DetectorVetoAttribution(
                    detector: .rediffByteExact, tier: .deterministic
                )
            ]
        )
    }

    @Test("THE PROBE: skips on day 0, stops after two vetoes, skips again once they are paid off")
    func theGateFollowsTheRestoration() async throws {
        let (orchestrator, trust) = try await Self.makeProbe()

        #expect(
            await Self.cuesForAByteExactSpan(orchestrator, windowId: "probe-day0") == 1,
            "day 0: the byte differ auto-skips before any transcript or history exists"
        )

        await Self.vetoAByteExactSpan(trust)
        await Self.vetoAByteExactSpan(trust)
        #expect(
            await Self.cuesForAByteExactSpan(orchestrator, windowId: "probe-vetoed") == 0,
            "two vetoes demote the class and the GATE stops skipping — playhead-gard, working"
        )

        for index in 0..<3 {
            await trust.recordCorrectObservation(
                podcastId: gardPodcastId,
                analysisAssetId: "asset-1",
                detector: .rediffByteExact
            )
            let cues = await Self.cuesForAByteExactSpan(
                orchestrator, windowId: "probe-yes-\(index)"
            )
            if index < 2 {
                #expect(cues == 0, "still owed after \(index + 1) of 3 Yeses")
            } else {
                #expect(
                    cues == 1,
                    "THE FIX AT THE GATE: the debt is discharged and the byte differ skips again"
                )
            }
        }

        await Self.vetoAByteExactSpan(trust)
        await Self.vetoAByteExactSpan(trust)
        #expect(
            await Self.cuesForAByteExactSpan(orchestrator, windowId: "probe-redemoted") == 0,
            "and it is still demotable afterwards — a return trip, not immunity"
        )
    }
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
