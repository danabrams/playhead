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
}
