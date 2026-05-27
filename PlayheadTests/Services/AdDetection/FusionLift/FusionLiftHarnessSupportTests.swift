// FusionLiftHarnessSupportTests.swift
// playhead-au2v.1.27 — Phase C tests: hermetic, SYNTHETIC unit tests for
// the harness support helpers in `FusionLiftHarnessSupport.swift`. No
// audio, no Foundation Models, no live pipeline — every input is a
// hand-built value, so these run on the simulator in the default
// `PlayheadFastTests` plan.
//
// Coverage:
//   * FusionLiftTranscriptVersion — reproduces runBackfill's derivation
//     (final-pass filter + norm/source hashes), is deterministic, and the
//     filter matches runBackfill's "all-non-final ⇒ use full set" fallback.
//   * FusionLiftModeAccumulator — GT + detection accumulation, the
//     decision-state filter (audit rows dropped), and that GT pairs with
//     a same-episode detection via greedy IoU.
//   * FusionLiftReport — positive / zero deltas, undefined-metric nil
//     propagation, table rendering, and JSON round-trip.

import Foundation
import Testing
@testable import Playhead

@Suite("FusionLift harness support (au2v.1.27 Phase C)")
struct FusionLiftHarnessSupportTests {

    // MARK: - Fixtures

    private static func chunk(
        index: Int,
        start: Double,
        end: Double,
        text: String,
        pass: String = "final",
        assetId: String = "asset-1"
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: text.lowercased(),
            pass: pass,
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: index
        )
    }

    private static func annotationWindow(
        start: Double,
        end: Double,
        adType: CorpusAnnotation.AdType = .hostRead
    ) -> CorpusAnnotation.AdWindow {
        CorpusAnnotation.AdWindow(
            startSeconds: start,
            endSeconds: end,
            advertiser: "Acme",
            product: "Widget",
            adType: adType,
            transitionType: .explicit,
            confidenceNotes: nil
        )
    }

    private static func storeAdWindow(
        id: String,
        start: Double,
        end: Double,
        confidence: Double = 0.9,
        decisionState: String,
        assetId: String = "asset-1"
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState,
            detectorVersion: "test-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    // MARK: - Transcript-version derivation

    @Test("transcript version reproduces TranscriptAtomizer with runBackfill's exact hashes")
    func transcriptVersion_matchesAtomizer() {
        let chunks = [
            Self.chunk(index: 0, start: 0, end: 30, text: "Welcome back."),
            Self.chunk(index: 1, start: 60, end: 90, text: "Brought to you by ExampleAd."),
        ]
        let derived = FusionLiftTranscriptVersion.derive(chunks: chunks, analysisAssetId: "asset-1")

        // The independently-computed version using the same exact inputs
        // runBackfill uses (final-pass filter, norm-v1/asr-v1).
        let (_, expected) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: "asset-1",
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        #expect(derived == expected.transcriptVersion)
        #expect(!derived.isEmpty)
    }

    @Test("transcript version is deterministic across calls")
    func transcriptVersion_deterministic() {
        let chunks = [Self.chunk(index: 0, start: 0, end: 30, text: "Hello.")]
        let a = FusionLiftTranscriptVersion.derive(chunks: chunks, analysisAssetId: "asset-1")
        let b = FusionLiftTranscriptVersion.derive(chunks: chunks, analysisAssetId: "asset-1")
        #expect(a == b)
    }

    @Test("final-pass filter drops fast-pass chunks when finals exist")
    func finalChunks_dropsFastWhenFinalsExist() {
        let chunks = [
            Self.chunk(index: 0, start: 0, end: 30, text: "final one", pass: "final"),
            Self.chunk(index: 1, start: 30, end: 60, text: "fast one", pass: "fast"),
        ]
        let filtered = FusionLiftTranscriptVersion.finalChunks(from: chunks)
        #expect(filtered.count == 1)
        #expect(filtered.first?.pass == "final")
    }

    @Test("final-pass filter falls back to full set when no finals exist")
    func finalChunks_fallsBackWhenNoFinals() {
        let chunks = [
            Self.chunk(index: 0, start: 0, end: 30, text: "fast one", pass: "fast"),
            Self.chunk(index: 1, start: 30, end: 60, text: "fast two", pass: "fast"),
        ]
        let filtered = FusionLiftTranscriptVersion.finalChunks(from: chunks)
        // Mirrors runBackfill's `filtered.isEmpty ? chunks : filtered`.
        #expect(filtered.count == 2)
    }

    // MARK: - Per-mode accumulation

    @Test("accumulator counts GT spans and skip-eligible detections")
    func accumulator_counts() {
        var acc = FusionLiftModeAccumulator()
        acc.addEpisode(
            annotationWindows: [
                Self.annotationWindow(start: 100, end: 160),
                Self.annotationWindow(start: 400, end: 450),
            ],
            adWindows: [
                Self.storeAdWindow(id: "w1", start: 105, end: 158, decisionState: AdDecisionState.confirmed.rawValue),
                // suppressed + reverted must be dropped by the Phase-A bridge.
                Self.storeAdWindow(id: "w2", start: 800, end: 820, decisionState: AdDecisionState.suppressed.rawValue),
                Self.storeAdWindow(id: "w3", start: 900, end: 920, decisionState: AdDecisionState.reverted.rawValue),
            ],
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        #expect(acc.groundTruth.count == 2)
        #expect(acc.detections.count == 1, "only the confirmed row is a skip-eligible detection")
        #expect(acc.detections.first?.id == "w1")
    }

    @Test("accumulator pairs a GT span with a same-episode overlapping detection")
    func accumulator_pairsTruePositive() {
        var acc = FusionLiftModeAccumulator()
        acc.addEpisode(
            annotationWindows: [Self.annotationWindow(start: 100, end: 160)],
            adWindows: [Self.storeAdWindow(id: "w1", start: 105, end: 158, decisionState: AdDecisionState.applied.rawValue)],
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        let span = acc.spanF1()
        #expect(span.truePositives == 1)
        #expect(span.falsePositives == 0)
        #expect(span.misses == 0)
        #expect(span.precision == 1.0)
        #expect(span.recall == 1.0)
        #expect(span.f1 == 1.0)
    }

    @Test("accumulator keeps episodes separate (no cross-episode pairing)")
    func accumulator_noCrossEpisodeLeak() {
        var acc = FusionLiftModeAccumulator()
        // GT in ep-1, detection at the same time but in ep-2 → must NOT pair.
        acc.addEpisode(
            annotationWindows: [Self.annotationWindow(start: 100, end: 160)],
            adWindows: [],
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        acc.addEpisode(
            annotationWindows: [],
            adWindows: [Self.storeAdWindow(id: "w1", start: 105, end: 158, decisionState: AdDecisionState.confirmed.rawValue, assetId: "ep-2")],
            podcastId: "show-A",
            episodeId: "ep-2"
        )
        let span = acc.spanF1()
        #expect(span.truePositives == 0, "different episodes never pair")
        #expect(span.misses == 1, "ep-1 GT is a miss")
        #expect(span.falsePositives == 1, "ep-2 detection is a false positive")
    }

    // MARK: - Report

    @Test("report computes positive recall lift when enabled catches more ads")
    func report_positiveRecallLift() {
        // Two GT ads per arm. OFF catches 1, ENABLED catches 2.
        var off = FusionLiftModeAccumulator()
        off.addEpisode(
            annotationWindows: [
                Self.annotationWindow(start: 100, end: 160),
                Self.annotationWindow(start: 400, end: 450),
            ],
            adWindows: [Self.storeAdWindow(id: "o1", start: 105, end: 158, decisionState: AdDecisionState.confirmed.rawValue)],
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        var enabled = FusionLiftModeAccumulator()
        enabled.addEpisode(
            annotationWindows: [
                Self.annotationWindow(start: 100, end: 160),
                Self.annotationWindow(start: 400, end: 450),
            ],
            adWindows: [
                Self.storeAdWindow(id: "e1", start: 105, end: 158, decisionState: AdDecisionState.confirmed.rawValue),
                Self.storeAdWindow(id: "e2", start: 405, end: 448, decisionState: AdDecisionState.confirmed.rawValue),
            ],
            podcastId: "show-A",
            episodeId: "ep-1"
        )

        let report = FusionLiftReport(episodeCount: 1, off: off, enabled: enabled)
        #expect(report.offArm.spanRecall == 0.5)
        #expect(report.enabledArm.spanRecall == 1.0)
        // recall lift = 1.0 − 0.5 = +0.5
        #expect(report.spanRecallDelta == 0.5)
        #expect((report.spanF1Delta ?? 0) > 0, "F1 should improve when recall improves at constant precision")
    }

    @Test("report deltas are nil when an arm metric is undefined (no detections)")
    func report_nilDeltaPropagation() {
        // OFF has a GT but no detections → spanPrecision is undefined (nil).
        var off = FusionLiftModeAccumulator()
        off.addEpisode(
            annotationWindows: [Self.annotationWindow(start: 100, end: 160)],
            adWindows: [],
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        var enabled = FusionLiftModeAccumulator()
        enabled.addEpisode(
            annotationWindows: [Self.annotationWindow(start: 100, end: 160)],
            adWindows: [Self.storeAdWindow(id: "e1", start: 105, end: 158, decisionState: AdDecisionState.confirmed.rawValue)],
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        let report = FusionLiftReport(episodeCount: 1, off: off, enabled: enabled)
        #expect(report.offArm.spanPrecision == nil, "no detections ⇒ precision undefined")
        #expect(report.spanPrecisionDelta == nil, "undefined off precision ⇒ nil delta (never a misleading 0.0)")
    }

    @Test("report table renders without crashing and contains both arms")
    func report_tableRenders() {
        var off = FusionLiftModeAccumulator()
        off.addEpisode(
            annotationWindows: [Self.annotationWindow(start: 100, end: 160)],
            adWindows: [Self.storeAdWindow(id: "o1", start: 105, end: 158, decisionState: AdDecisionState.confirmed.rawValue)],
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        let report = FusionLiftReport(episodeCount: 1, off: off, enabled: off)
        let table = report.table()
        #expect(table.contains("Chapter-Fusion Lift"))
        #expect(table.contains("off"))
        #expect(table.contains("enabled"))
        #expect(table.contains("lift (enabled − off)"))
    }

    @Test("report JSON round-trips")
    func report_jsonRoundTrips() throws {
        var off = FusionLiftModeAccumulator()
        off.addEpisode(
            annotationWindows: [Self.annotationWindow(start: 100, end: 160)],
            adWindows: [Self.storeAdWindow(id: "o1", start: 105, end: 158, decisionState: AdDecisionState.confirmed.rawValue)],
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        let report = FusionLiftReport(episodeCount: 1, off: off, enabled: off)
        let data = try report.jsonData()
        let decoded = try JSONDecoder().decode(FusionLiftReport.self, from: data)
        #expect(decoded == report)
    }

    // MARK: - Lexical-scorer per-feature sweep arm config (playhead-xsdz.liveab)
    //
    // The LOAD-BEARING correctness property of the 4-arm sweep: each arm must
    // set EXACTLY the intended (threshold, snapEnabled) combo, and the arms
    // must differ from each other ONLY in those two toggles. A mislabeled arm
    // — a toggle wired to the wrong field, or a non-gate field drifting between
    // arms — would attribute a live regression to the WRONG feature and send
    // the culprit hunt down a false trail. These hermetic tests pin the
    // isolation on the sim before the (expensive, Catalyst-only) live sweep
    // ever runs.
    //
    // The intended sweep matrix (xsdz.1 = auto-ad threshold gate; xsdz.2/.3 =
    // the shared lexicalClusterSnapEnabled gate):
    //   arm         | xsdz.1 | xsdz.2/.3
    //   baseline    | off    | off
    //   xsdz1only   | on     | off
    //   xsdz23only  | off    | on
    //   alon        | on     | on

    /// The four arms in `allCases` order with their intended toggle states —
    /// the single source of truth the matrix tests below assert against.
    private static let expectedArmMatrix: [(arm: LexicalScorerArm, xsdz1On: Bool, xsdz23On: Bool)] = [
        (.baseline, false, false),
        (.xsdz1only, true, false),
        (.xsdz23only, false, true),
        (.alon, true, true),
    ]

    @Test("sweep enumerates exactly the four intended arms in baseline-first order")
    func lexicalArm_fourArmsInOrder() {
        #expect(LexicalScorerArm.allCases == [.baseline, .xsdz1only, .xsdz23only, .alon])
    }

    @Test("each arm's xsdz1On / xsdz23On flags match the intended sweep matrix")
    func lexicalArm_toggleMatrix() {
        for (arm, xsdz1On, xsdz23On) in Self.expectedArmMatrix {
            #expect(arm.xsdz1On == xsdz1On, "arm \(arm.rawValue): xsdz1On should be \(xsdz1On)")
            #expect(arm.xsdz23On == xsdz23On, "arm \(arm.rawValue): xsdz23On should be \(xsdz23On)")
        }
        // The four arms are the four distinct points of the 2×2 toggle grid —
        // no two arms share the same (xsdz1On, xsdz23On) pair.
        let pairs = LexicalScorerArm.allCases.map { [$0.xsdz1On, $0.xsdz23On] }
        #expect(Set(pairs.map { "\($0)" }).count == LexicalScorerArm.allCases.count)
    }

    @Test("programOn is true only for the all-on endpoint, false for every other arm")
    func lexicalArm_programOnIsAllOnEndpoint() {
        #expect(LexicalScorerArm.alon.programOn == true)
        #expect(LexicalScorerArm.baseline.programOn == false)
        #expect(LexicalScorerArm.xsdz1only.programOn == false)
        #expect(LexicalScorerArm.xsdz23only.programOn == false)
    }

    @Test("each arm's AdDetectionConfig threshold matches its xsdz.1 toggle exactly")
    func lexicalArm_thresholdPerArm() {
        let onValue = AdDetectionConfig.default.lexicalAutoAdQualifiedThreshold
        let offValue = LexicalScorerArmConfig.disabledQualifiedThreshold
        for (arm, xsdz1On, _) in Self.expectedArmMatrix {
            let config = LexicalScorerArmConfig.adDetectionConfig(for: arm)
            let expected = xsdz1On ? onValue : offValue
            #expect(
                config.lexicalAutoAdQualifiedThreshold == expected,
                "arm \(arm.rawValue): threshold should be \(expected) (xsdz1On=\(xsdz1On))"
            )
        }
        // The OFF value must clear the auto-skip gate so the track is a true
        // no-op (it can never promote alone), not merely "a higher threshold".
        #expect(offValue >= AdDetectionConfig.default.autoSkipConfidenceThreshold)
    }

    @Test("each arm's NarrowingConfig snap flag matches its xsdz.2/.3 toggle exactly")
    func lexicalArm_snapFlagPerArm() {
        for (arm, _, xsdz23On) in Self.expectedArmMatrix {
            let narrowing = LexicalScorerArmConfig.narrowingConfig(for: arm)
            #expect(
                narrowing.lexicalClusterSnapEnabled == xsdz23On,
                "arm \(arm.rawValue): lexicalClusterSnapEnabled should be \(xsdz23On)"
            )
        }
    }

    @Test("xsdz.2/.3-on arms produce NarrowingConfig byte-identical to .default")
    func lexicalArm_snapOnMatchesDefault() {
        for (arm, _, xsdz23On) in Self.expectedArmMatrix where xsdz23On {
            #expect(
                LexicalScorerArmConfig.narrowingConfig(for: arm) == NarrowingConfig.default,
                "arm \(arm.rawValue): snap-on NarrowingConfig must equal .default"
            )
        }
    }

    @Test("arms differ ONLY in the two program gates — every other field is held constant")
    func lexicalArm_isolation_onlyTwoTogglesVary() {
        // The two reference configs: an xsdz.1-off arm and an xsdz.1-on arm.
        // Threshold is the ONLY field allowed to vary across the AdDetection
        // configs; a re-stamp proves no other field drifted.
        let off = LexicalScorerArmConfig.adDetectionConfig(xsdz1On: false)
        let on = LexicalScorerArmConfig.adDetectionConfig(xsdz1On: true)
        #expect(off.lexicalAutoAdQualifiedThreshold != on.lexicalAutoAdQualifiedThreshold)
        #expect(off.fmBackfillMode == on.fmBackfillMode)
        #expect(off.fmBackfillMode == .full)
        #expect(off.chapterSignalMode == on.chapterSignalMode)
        #expect(off.chapterSignalMode == .off)
        #expect(off.candidateThreshold == on.candidateThreshold)
        #expect(off.confirmationThreshold == on.confirmationThreshold)
        #expect(off.suppressionThreshold == on.suppressionThreshold)
        #expect(off.autoSkipConfidenceThreshold == on.autoSkipConfidenceThreshold)

        // The xsdz.2/.3-off NarrowingConfig must differ from .default in the
        // snap flag ALONE: re-enabling the flag (and nothing else) recovers
        // .default exactly, which proves no sibling field drifted.
        let snapOff = LexicalScorerArmConfig.narrowingConfig(xsdz23On: false)
        let def = NarrowingConfig.default
        #expect(snapOff.lexicalClusterSnapEnabled == false)
        #expect(def.lexicalClusterSnapEnabled == true)
        #expect(snapOff != def)
        #expect(snapOff.perAnchorPaddingSegments == def.perAnchorPaddingSegments)
        #expect(snapOff.maxNarrowedSegmentsPerPhase == def.maxNarrowedSegmentsPerPhase)
        #expect(snapOff.acousticBreakSnapMaxDistanceSeconds == def.acousticBreakSnapMaxDistanceSeconds)
        #expect(snapOff.lexicalClusterGapSeconds == def.lexicalClusterGapSeconds)
        #expect(snapOff.lexicalClusterMarginSegments == def.lexicalClusterMarginSegments)
        #expect(snapOff.lexicalClusterMinHits == def.lexicalClusterMinHits)
        let reEnabled = NarrowingConfig(
            perAnchorPaddingSegments: snapOff.perAnchorPaddingSegments,
            maxNarrowedSegmentsPerPhase: snapOff.maxNarrowedSegmentsPerPhase,
            acousticBreakSnapMaxDistanceSeconds: snapOff.acousticBreakSnapMaxDistanceSeconds,
            lexicalClusterSnapEnabled: true,
            lexicalClusterGapSeconds: snapOff.lexicalClusterGapSeconds,
            lexicalClusterMarginSegments: snapOff.lexicalClusterMarginSegments,
            lexicalClusterMinHits: snapOff.lexicalClusterMinHits
        )
        #expect(reEnabled == def)
    }

    @Test("alon arm reproduces production defaults on both gates (cumulative-treatment endpoint)")
    func lexicalArm_alonMatchesProductionDefaults() {
        let config = LexicalScorerArmConfig.adDetectionConfig(for: .alon)
        #expect(config.lexicalAutoAdQualifiedThreshold == AdDetectionConfig.default.lexicalAutoAdQualifiedThreshold)
        #expect(LexicalScorerArmConfig.narrowingConfig(for: .alon) == NarrowingConfig.default)
        #expect(config.fmBackfillMode == .full)
        #expect(config.chapterSignalMode == .off)
    }

    @Test("baseline arm disables BOTH gates (cumulative-baseline endpoint)")
    func lexicalArm_baselineDisablesBothGates() {
        let config = LexicalScorerArmConfig.adDetectionConfig(for: .baseline)
        #expect(config.lexicalAutoAdQualifiedThreshold == LexicalScorerArmConfig.disabledQualifiedThreshold)
        #expect(LexicalScorerArmConfig.narrowingConfig(for: .baseline).lexicalClusterSnapEnabled == false)
    }

    // MARK: - Lexical-scorer per-feature sweep report

    /// Build an accumulator that catches `caught` of the two GT ads in `ep-1`.
    private static func sweepArmAccumulator(caught: Int) -> FusionLiftModeAccumulator {
        var acc = FusionLiftModeAccumulator()
        var windows: [AdWindow] = []
        if caught >= 1 {
            windows.append(Self.storeAdWindow(id: "d1", start: 105, end: 158, decisionState: AdDecisionState.confirmed.rawValue))
        }
        if caught >= 2 {
            windows.append(Self.storeAdWindow(id: "d2", start: 405, end: 448, decisionState: AdDecisionState.confirmed.rawValue))
        }
        acc.addEpisode(
            annotationWindows: [
                Self.annotationWindow(start: 100, end: 160),
                Self.annotationWindow(start: 400, end: 450),
            ],
            adWindows: windows,
            podcastId: "show-A",
            episodeId: "ep-1"
        )
        return acc
    }

    @Test("sweep report emits one row per arm, baseline first, with the arm's toggle flags")
    func lexicalSweep_rowsPerArm() {
        let report = LexicalScorerSweepReport(
            episodeCount: 1,
            accumulators: [
                .baseline: Self.sweepArmAccumulator(caught: 1),
                .xsdz1only: Self.sweepArmAccumulator(caught: 2),
                .xsdz23only: Self.sweepArmAccumulator(caught: 1),
                .alon: Self.sweepArmAccumulator(caught: 2),
            ]
        )
        #expect(report.rows.map(\.arm) == ["baseline", "xsdz1only", "xsdz23only", "alon"])
        // Each row carries the arm's intended toggle flags (so the JSON dump is
        // self-describing about which feature each row isolates).
        for (arm, xsdz1On, xsdz23On) in Self.expectedArmMatrix {
            let row = report.rows.first { $0.arm == arm.rawValue }
            #expect(row?.xsdz1On == xsdz1On)
            #expect(row?.xsdz23On == xsdz23On)
        }
    }

    @Test("sweep deltas are measured vs baseline; baseline's own deltas are zero")
    func lexicalSweep_deltasVsBaseline() {
        // baseline catches 1 of 2 (recall 0.5); xsdz1only catches 2 (recall
        // 1.0) → +0.5 recall lift; xsdz23only catches 1 (recall 0.5) → 0 lift.
        let report = LexicalScorerSweepReport(
            episodeCount: 1,
            accumulators: [
                .baseline: Self.sweepArmAccumulator(caught: 1),
                .xsdz1only: Self.sweepArmAccumulator(caught: 2),
                .xsdz23only: Self.sweepArmAccumulator(caught: 1),
                .alon: Self.sweepArmAccumulator(caught: 2),
            ]
        )
        func row(_ name: String) -> LexicalScorerSweepReport.ArmRow {
            report.rows.first { $0.arm == name }!
        }
        #expect(row("baseline").spanRecall == 0.5)
        #expect(row("baseline").spanRecallDelta == 0.0, "baseline measured against itself ⇒ zero delta")
        #expect(row("xsdz1only").spanRecall == 1.0)
        #expect(row("xsdz1only").spanRecallDelta == 0.5)
        #expect((row("xsdz1only").spanF1Delta ?? 0) > 0)
        #expect(row("xsdz23only").spanRecall == 0.5)
        #expect(row("xsdz23only").spanRecallDelta == 0.0)
        #expect(row("alon").spanRecallDelta == 0.5)
    }

    @Test("sweep deltas are nil when an arm metric is undefined (no detections)")
    func lexicalSweep_nilDeltaPropagation() {
        // baseline has a GT but no detections ⇒ spanPrecision undefined ⇒ every
        // arm's precision delta-vs-baseline propagates to nil (never 0.0).
        let report = LexicalScorerSweepReport(
            episodeCount: 1,
            accumulators: [
                .baseline: Self.sweepArmAccumulator(caught: 0),
                .xsdz1only: Self.sweepArmAccumulator(caught: 1),
                .xsdz23only: Self.sweepArmAccumulator(caught: 0),
                .alon: Self.sweepArmAccumulator(caught: 1),
            ]
        )
        let baseline = report.rows.first { $0.arm == "baseline" }!
        let xsdz1 = report.rows.first { $0.arm == "xsdz1only" }!
        #expect(baseline.spanPrecision == nil, "no detections ⇒ precision undefined")
        #expect(xsdz1.spanPrecisionDelta == nil, "undefined baseline precision ⇒ nil delta")
    }

    @Test("sweep report table renders all four arms and JSON round-trips")
    func lexicalSweep_tableAndJSON() throws {
        let report = LexicalScorerSweepReport(
            episodeCount: 1,
            accumulators: [
                .baseline: Self.sweepArmAccumulator(caught: 1),
                .xsdz1only: Self.sweepArmAccumulator(caught: 2),
                .xsdz23only: Self.sweepArmAccumulator(caught: 1),
                .alon: Self.sweepArmAccumulator(caught: 2),
            ]
        )
        let table = report.table()
        #expect(table.contains("Lexical-Scorer Per-Feature Sweep"))
        #expect(table.contains("baseline"))
        #expect(table.contains("xsdz1only"))
        #expect(table.contains("xsdz23only"))
        #expect(table.contains("alon"))
        #expect(table.contains("per-arm lift (arm − baseline)"))

        let data = try report.jsonData()
        let decoded = try JSONDecoder().decode(LexicalScorerSweepReport.self, from: data)
        #expect(decoded == report)
    }

    @Test("sweep report tolerates a missing arm accumulator (empty arm ⇒ zero counts)")
    func lexicalSweep_missingArmIsEmpty() {
        // Only baseline supplied; the other three default to empty
        // accumulators rather than crashing — defensive against a harness that
        // skips an arm.
        let report = LexicalScorerSweepReport(
            episodeCount: 0,
            accumulators: [.baseline: Self.sweepArmAccumulator(caught: 1)]
        )
        #expect(report.rows.count == 4)
        let alon = report.rows.first { $0.arm == "alon" }!
        #expect(alon.groundTruthSpans == 0)
        #expect(alon.detectedSpans == 0)
    }
}
