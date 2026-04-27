// BoundarySnapDiagnosticLoggingTests.swift
// playhead-vn7n.1: Path-exercise rails for the three diagnostic log sites
// added to triage user-perceived ad-skip overshoot:
//
//   1. MinimalContiguousSpanDecoder.applyBoundarySnap
//   2. BoundaryRefiner.resolveBoundary
//   3. SkipOrchestrator.pushMergedCues
//
// OSLogStore-based assertions are intentionally avoided (brittle across
// simulator/device targets and requires entitlements). Instead each test
// drives input that *guarantees* the logged code path is exercised; the
// log statement itself lives in the diff. Reviewers verifying the bead
// can grep on subsystem `com.playhead` + the categories below in
// Console.app or sysdiagnose.

import CoreMedia
import Foundation
import Testing
@testable import Playhead

@Suite("Boundary-snap diagnostic logging (playhead-vn7n.1)")
struct BoundarySnapDiagnosticLoggingTests {

    // MARK: - Site 1: MinimalContiguousSpanDecoder.applyBoundarySnap

    @Test("applyBoundarySnap path is exercised when an acoustic break sits inside the snap radius")
    func decoderApplyBoundarySnapPathExercised() {
        // Build a 6-atom anchored run (ordinals 0-5, t=0..6) with a single
        // acoustic-break atom positioned strictly OUTSIDE the start-edge
        // window but INSIDE the end-edge window. The right-edge snap
        // should expand `endTime` from 6.0 → 11.0, producing a +5.0 s
        // `endDelta` in the diagnostic log. The left-edge snap finds
        // no qualifying atom and leaves `startTime` at 0.0.
        //
        // Snap radius is 8 s.
        //   start window: [0 - 8, 0 + 8] = [-8, 8]
        //   end window:   [6 - 8, 6 + 8] = [-2, 14]
        // Break atom at startTime=10, endTime=11 → outside [-8, 8]
        // (so left edge stays put) but inside [-2, 14] (so right edge
        // snaps). This pins the diagnostic log assertion shape.
        let anchored: ClosedRange<Int> = 0 ... 5
        var atoms: [AtomEvidence] = (0 ..< 12).map { i in
            AtomEvidence(
                atomOrdinal: i,
                startTime: Double(i),
                endTime: Double(i) + 1.0,
                isAnchored: anchored.contains(i),
                anchorProvenance: anchored.contains(i)
                    ? [.fmConsensus(regionId: "r\(i)", consensusStrength: 0.7)]
                    : [],
                hasAcousticBreakHint: false,
                correctionMask: .none
            )
        }
        atoms[10] = AtomEvidence(
            atomOrdinal: 10,
            startTime: 10.0,
            endTime: 11.0,
            isAnchored: false,
            anchorProvenance: [],
            hasAcousticBreakHint: true,
            correctionMask: .none
        )

        let decoder = MinimalContiguousSpanDecoder()
        let spans = decoder.decode(atoms: atoms, assetId: "asset-vn7n1-decoder")

        // The run survives MIN_DURATION (6 s ≥ 5 s) and Use A snaps the
        // right edge to atom 10's endTime (11.0). This guarantees the
        // applyBoundarySnap log call ran with a +5.0 s endDelta.
        #expect(spans.count == 1)
        if let span = spans.first {
            #expect(span.endTime == 11.0)
            #expect(span.startTime == 0.0)
        }
    }

    // MARK: - Site 2: BoundaryRefiner.resolveBoundary

    @Test("resolveBoundary path is exercised once per boundary when feature windows are sufficient")
    func boundaryRefinerResolveBoundaryPathExercised() {
        // Reuse the shape from BoundaryRefinerTests.snapsBothBoundaries —
        // strong pause/spectral cues straddle both candidate boundaries
        // so the resolver runs (not short-circuited by the <3-window guard)
        // and produces non-zero adjustments. Two log lines fire (start
        // and end).
        let windows: [FeatureWindow] = [
            FeatureWindow(
                analysisAssetId: "asset-vn7n1-refiner",
                startTime: 7, endTime: 8,
                rms: 0.05, spectralFlux: 0.95,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0, musicBedChangeScore: 0.0,
                pauseProbability: 0.95,
                speakerClusterId: nil, jingleHash: nil, featureVersion: 1
            ),
            FeatureWindow(
                analysisAssetId: "asset-vn7n1-refiner",
                startTime: 8, endTime: 9,
                rms: 0.05, spectralFlux: 0.05,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0, musicBedChangeScore: 0.0,
                pauseProbability: 0.05,
                speakerClusterId: nil, jingleHash: nil, featureVersion: 1
            ),
            FeatureWindow(
                analysisAssetId: "asset-vn7n1-refiner",
                startTime: 9, endTime: 10,
                rms: 0.05, spectralFlux: 0.05,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0, musicBedChangeScore: 0.0,
                pauseProbability: 0.05,
                speakerClusterId: nil, jingleHash: nil, featureVersion: 1
            ),
            FeatureWindow(
                analysisAssetId: "asset-vn7n1-refiner",
                startTime: 19, endTime: 20,
                rms: 0.05, spectralFlux: 0.05,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0, musicBedChangeScore: 0.0,
                pauseProbability: 0.05,
                speakerClusterId: nil, jingleHash: nil, featureVersion: 1
            ),
            FeatureWindow(
                analysisAssetId: "asset-vn7n1-refiner",
                startTime: 20, endTime: 21,
                rms: 0.05, spectralFlux: 0.95,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0, musicBedChangeScore: 0.0,
                pauseProbability: 0.95,
                speakerClusterId: nil, jingleHash: nil, featureVersion: 1
            ),
            FeatureWindow(
                analysisAssetId: "asset-vn7n1-refiner",
                startTime: 21, endTime: 22,
                rms: 0.05, spectralFlux: 0.05,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0, musicBedChangeScore: 0.0,
                pauseProbability: 0.05,
                speakerClusterId: nil, jingleHash: nil, featureVersion: 1
            ),
        ]

        let adjustments = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )

        // Identical numeric output to BoundaryRefinerTests.snapsBothBoundaries —
        // proves the resolveBoundary path ran for both start and end.
        #expect(adjustments.startAdjust == -2.5)
        #expect(adjustments.endAdjust == 0.5)
    }

    // MARK: - Site 3: SkipOrchestrator.pushMergedCues

    @Test("pushMergedCues path is exercised on injection — handler receives cues and the log fires")
    func skipOrchestratorPushMergedCuesPathExercised() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)

        let accumulator = CueAccumulatorVN7N1()
        await orchestrator.setSkipCueHandler { cues in
            Task { await accumulator.append(cues) }
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // injectUserMarkedAd promotes a window to .applied, which triggers
        // evaluateAndPush → mergeAdjacentWindows → pushMergedCues.
        await orchestrator.injectUserMarkedAd(
            start: 60.0,
            end: 120.0,
            analysisAssetId: "asset-1"
        )

        // Allow the fire-and-forget Task in the handler to complete.
        try await Task.sleep(for: .milliseconds(50))

        let pushed = await accumulator.cues
        #expect(!pushed.isEmpty, "Expected pushMergedCues to have emitted at least one cue batch")
        if let last = pushed.last, let cue = last.first {
            // The presence of a cue in the handler proves pushMergedCues
            // ran end-to-end and therefore so did the diagnostic log line.
            let cueStart = CMTimeGetSeconds(cue.start)
            let cueEnd = CMTimeGetSeconds(cue.end)
            #expect(cueStart == 60.0)
            // playhead-vn7n.2 cushion (default 1.0s) pulls cueEnd in by adTrailingCushionSeconds.
            #expect(cueEnd == 119.0)
        }
    }
}

// MARK: - Local helpers

/// Suite-local thread-safe cue accumulator. Mirrors the pattern in
/// UserMarkedAdInjectionTests but kept private so the suite is
/// self-contained.
private actor CueAccumulatorVN7N1 {
    var cues: [[CMTimeRange]] = []
    func append(_ batch: [CMTimeRange]) { cues.append(batch) }
}
