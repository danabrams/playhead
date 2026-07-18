// AcousticLedgerBreakAlignmentTests.swift
// playhead-fqc8: Tests for the producer-side break-alignment entry
// emitted by `AdDetectionService.buildAcousticLedgerEntries` when a
// `.classifierSeed`-anchored span lines up with a strong AcousticBreak.

import Foundation
import Testing

@testable import Playhead

@Suite("buildAcousticLedgerEntries — breakAlignment (playhead-fqc8)")
struct AcousticLedgerBreakAlignmentTests {

    // MARK: - Helpers

    private func window(
        start: Double,
        duration: Double = 2.0,
        rms: Double = 0.10
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test-asset",
            startTime: start,
            endTime: start + duration,
            rms: rms,
            spectralFlux: 0.05,
            musicProbability: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 5
        )
    }

    private func makeSpan(
        start: Double,
        end: Double,
        anchorProvenance: [AnchorRef]
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "test-asset", firstAtomOrdinal: 0, lastAtomOrdinal: 10),
            assetId: "test-asset",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: start,
            endTime: end,
            anchorProvenance: anchorProvenance
        )
    }

    private func makeService() async throws -> AdDetectionService {
        let store = try await makeTestStore()
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "fqc8-test",
                fmBackfillMode: .off
            )
        )
    }

    // Produce flat windows so the legacy RMS-drop path returns 0 — we
    // want to assert the ALIGNMENT entry independently of the RMS-drop
    // entry.
    private func flatWindows(start: Double, end: Double) -> [FeatureWindow] {
        var ws: [FeatureWindow] = []
        var t = start
        while t < end {
            ws.append(window(start: t, rms: 0.10))
            t += 2.0
        }
        return ws
    }

    // MARK: - Tests

    @Test("classifierSeed span + aligned strong break emits exactly one breakAlignment entry")
    func emitsBreakAlignmentForClassifierSeed() async throws {
        let svc = try await makeService()
        let span = makeSpan(
            start: 100.0,
            end: 130.0,
            anchorProvenance: [.classifierSeed(regionId: "r1", score: 0.85)]
        )
        // Break at 99.5 — within the ±2.0s tolerance of span.startTime (100.0)
        // — strength 0.7 (>= 0.5 floor).
        let breaks: [AcousticBreak] = [
            AcousticBreak(time: 99.5, breakStrength: 0.7, signals: [.energyDrop])
        ]

        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: flatWindows(start: 100, end: 130),
            fusionConfig: FusionWeightConfig(),
            acousticBreaks: breaks
        )

        let alignment = entries.filter { $0.source == .breakAlignment }
        try #require(alignment.count == 1,
                     "Expected exactly one breakAlignment entry (got \(alignment.count))")
        #expect(alignment[0].source == .breakAlignment)
        #expect(alignment[0].weight > 0)
        let cap = FusionWeightConfig().breakAlignmentCap
        #expect(alignment[0].weight <= cap,
                "Weight must be capped at breakAlignmentCap")
        if case .breakAlignment(let strength) = alignment[0].detail {
            #expect(strength == 0.7)
        } else {
            Issue.record("Expected .breakAlignment detail")
        }
    }

    @Test("No classifierSeed provenance + same break → no breakAlignment entry")
    func noClassifierSeedNoAlignmentEntry() async throws {
        let svc = try await makeService()
        let span = makeSpan(
            start: 100.0,
            end: 130.0,
            anchorProvenance: [
                .fmConsensus(regionId: "r1", consensusStrength: 0.9)
            ]
        )
        let breaks: [AcousticBreak] = [
            AcousticBreak(time: 99.5, breakStrength: 0.7, signals: [.energyDrop])
        ]

        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: flatWindows(start: 100, end: 130),
            fusionConfig: FusionWeightConfig(),
            acousticBreaks: breaks
        )

        let alignment = entries.filter { $0.source == .breakAlignment }
        #expect(alignment.isEmpty,
                "fmConsensus span must NOT receive a breakAlignment corroborator")
    }

    @Test("classifierSeed but break too far away → no breakAlignment entry")
    func breakTooFarAwayNoAlignment() async throws {
        let svc = try await makeService()
        let span = makeSpan(
            start: 100.0,
            end: 130.0,
            anchorProvenance: [.classifierSeed(regionId: "r1", score: 0.85)]
        )
        // Break at 95.0 — that's 5s before startTime, outside ±2.0 tolerance
        // and 35s before endTime — both edges miss.
        let breaks: [AcousticBreak] = [
            AcousticBreak(time: 95.0, breakStrength: 0.9, signals: [.energyDrop])
        ]

        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: flatWindows(start: 100, end: 130),
            fusionConfig: FusionWeightConfig(),
            acousticBreaks: breaks
        )

        let alignment = entries.filter { $0.source == .breakAlignment }
        #expect(alignment.isEmpty,
                "Break 5s away must NOT trigger a breakAlignment entry")
    }

    @Test("classifierSeed but weak break strength → no breakAlignment entry")
    func weakBreakNoAlignment() async throws {
        let svc = try await makeService()
        let span = makeSpan(
            start: 100.0,
            end: 130.0,
            anchorProvenance: [.classifierSeed(regionId: "r1", score: 0.85)]
        )
        // Break aligns to startTime within tolerance, but strength 0.3 < 0.5 floor.
        let breaks: [AcousticBreak] = [
            AcousticBreak(time: 99.5, breakStrength: 0.3, signals: [.energyDrop])
        ]

        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: flatWindows(start: 100, end: 130),
            fusionConfig: FusionWeightConfig(),
            acousticBreaks: breaks
        )

        let alignment = entries.filter { $0.source == .breakAlignment }
        #expect(alignment.isEmpty,
                "Break with strength 0.3 < 0.5 floor must NOT trigger a breakAlignment entry")
    }

    @Test("Empty acousticBreaks array preserves pre-fqc8 behaviour exactly")
    func emptyBreaksArrayBackCompat() async throws {
        let svc = try await makeService()
        let span = makeSpan(
            start: 100.0,
            end: 130.0,
            anchorProvenance: [.classifierSeed(regionId: "r1", score: 0.85)]
        )

        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: flatWindows(start: 100, end: 130),
            fusionConfig: FusionWeightConfig(),
            acousticBreaks: []
        )

        let alignment = entries.filter { $0.source == .breakAlignment }
        #expect(alignment.isEmpty,
                "Empty breaks array → no alignment entry, pre-fqc8 behaviour preserved")
    }

    /// playhead-fqc8 cycle-1 review M-4: the alignment weight must scale
    /// with the matched break's strength. With the default
    /// `breakAlignmentCap = 0.20`, a 0.5 strength produces 0.10 and a 1.0
    /// strength produces the full 0.20.
    @Test("Alignment weight scales linearly with break strength (M-4)")
    func alignmentWeightScalesWithStrength() async throws {
        let svc = try await makeService()
        let span = makeSpan(
            start: 100.0,
            end: 130.0,
            anchorProvenance: [.classifierSeed(regionId: "r1", score: 0.85)]
        )
        let cfg = FusionWeightConfig()  // breakAlignmentCap = 0.20

        // strength 0.5 → 0.5 * 0.20 = 0.10
        let weakBreaks: [AcousticBreak] = [
            AcousticBreak(time: 99.5, breakStrength: 0.5, signals: [.energyDrop])
        ]
        let weakEntries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: flatWindows(start: 100, end: 130),
            fusionConfig: cfg,
            acousticBreaks: weakBreaks
        )
        let weakAlignment = weakEntries.filter { $0.source == .breakAlignment }
        try #require(weakAlignment.count == 1)
        #expect(abs(weakAlignment[0].weight - 0.10) < 1e-9,
                "0.5 strength × 0.20 cap = 0.10")

        // strength 1.0 → 1.0 * 0.20 = 0.20 (full cap)
        let strongBreaks: [AcousticBreak] = [
            AcousticBreak(time: 99.5, breakStrength: 1.0, signals: [.energyDrop])
        ]
        let strongEntries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: flatWindows(start: 100, end: 130),
            fusionConfig: cfg,
            acousticBreaks: strongBreaks
        )
        let strongAlignment = strongEntries.filter { $0.source == .breakAlignment }
        try #require(strongAlignment.count == 1)
        #expect(abs(strongAlignment[0].weight - 0.20) < 1e-9,
                "1.0 strength × 0.20 cap = 0.20")
    }

    /// Cycle-3 review M-A: a span carrying BOTH `.classifierSeed` and
    /// `.fmConsensus` provenance must STILL receive a producer-side
    /// `.breakAlignment` entry. The producer gates on classifier-seed
    /// presence only; track demotion happens later in
    /// `DecisionMapper.computePromotionTrack` (which routes such spans to
    /// `.standard`). Suppressing the entry at the producer would regress
    /// the cycle-2 HIGH-1/HIGH-2 quorum corroboration paths, where
    /// `.breakAlignment` is what lets an FM-anchored span clear the
    /// `quorumGateForFMAcoustic` / `metadataCorroborationGate` boundary
    /// alignment requirement.
    @Test("classifierSeed + fmConsensus combined anchor still emits a breakAlignment entry (cycle-3 M-A)")
    func combinedClassifierSeedAndFMConsensusStillEmitsAlignment() async throws {
        let svc = try await makeService()
        let span = makeSpan(
            start: 100.0,
            end: 130.0,
            anchorProvenance: [
                .classifierSeed(regionId: "r1", score: 0.85),
                .fmConsensus(regionId: "r1", consensusStrength: 0.9)
            ]
        )
        let breaks: [AcousticBreak] = [
            AcousticBreak(time: 99.5, breakStrength: 0.7, signals: [.energyDrop])
        ]

        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: flatWindows(start: 100, end: 130),
            fusionConfig: FusionWeightConfig(),
            acousticBreaks: breaks
        )

        let alignment = entries.filter { $0.source == .breakAlignment }
        #expect(alignment.count == 1,
                "Combined classifierSeed+fmConsensus must STILL produce a breakAlignment entry; track demotion lives in computePromotionTrack, not in the producer")
    }
}
