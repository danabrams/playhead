// SustainedMusicOffsetProposerTests.swift
// playhead-t1py: unit tests for the sustained-music-offset boundary proposer.
//
// Mirrors AcousticMusicProbabilityTests in spirit — deterministic synthesized
// input, byte-reproducible assertions — but at the WINDOW level: the proposer
// thresholds on the already-persisted `FeatureWindow.musicProbability` (the riiz
// composite), so these tests synthesize `[FeatureWindow]` with hand-authored
// `musicProbability` timelines rather than raw audio.

import Foundation
import Testing

@testable import Playhead

@Suite("SustainedMusicOffsetProposer (playhead-t1py)")
struct SustainedMusicOffsetProposerTests {

    private let assetId = "smo-asset"
    private let windowDuration = 2.0

    /// Build contiguous 2s windows starting at t=0, one per `musicProbability`.
    /// Only `musicProbability` and the window times drive the proposer; the
    /// other fields are inert filler.
    private func windows(_ musicProbs: [Double]) -> [FeatureWindow] {
        musicProbs.enumerated().map { index, prob in
            FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(index) * windowDuration,
                endTime: Double(index + 1) * windowDuration,
                rms: 0.4,
                spectralFlux: 0.1,
                musicProbability: prob,
                pauseProbability: 0.0,
                speakerClusterId: 1,
                jingleHash: nil,
                featureVersion: 5
            )
        }
    }

    private func episodeDuration(_ musicProbs: [Double]) -> Double {
        Double(musicProbs.count) * windowDuration
    }

    private func propose(_ musicProbs: [Double]) -> [ProposedSpan] {
        SustainedMusicOffsetProposer.propose(
            featureWindows: windows(musicProbs),
            episodeDuration: episodeDuration(musicProbs)
        )
    }

    // MARK: - Positive: a clean sustained run

    @Test("a clean 12s music run yields exactly one span ending at the music→speech drop")
    func cleanRunProducesOneSpanAtTheDrop() {
        // 6 music windows (0..12s) then 4 speech windows.
        let probs = [0.9, 0.9, 0.9, 0.9, 0.9, 0.9] + [0.1, 0.1, 0.1, 0.1]
        let spans = propose(probs)

        #expect(spans.count == 1)
        let span = spans[0]
        #expect(span.startTime == 0.0)
        // Trailing edge = end of the last music window = the music→speech offset.
        #expect(span.endTime == 12.0)
        // Confidence = mean musicProbability over the run's music windows.
        #expect(abs(span.confidence - 0.9) < 1e-9)
    }

    @Test("a run that reaches end-of-episode still proposes, clamped to episodeDuration")
    func runToEndOfEpisodeProposes() {
        // 5 music windows, no trailing speech at all.
        let probs = [0.9, 0.9, 0.9, 0.9, 0.9]
        let spans = propose(probs)
        #expect(spans.count == 1)
        #expect(spans[0].startTime == 0.0)
        #expect(spans[0].endTime == 10.0)
    }

    // MARK: - Negative cases

    @Test("a 4s run is below minRunSeconds → no span")
    func shortRunProducesNothing() {
        let probs = [0.9, 0.9] + [0.1, 0.1, 0.1, 0.1]
        #expect(propose(probs).isEmpty)
    }

    @Test("all-speech → no span")
    func allSpeechProducesNothing() {
        let probs = Array(repeating: 0.1, count: 12)
        #expect(propose(probs).isEmpty)
    }

    @Test("music-under-speech (composite below threshold) → no span")
    func musicUnderSpeechProducesNothing() {
        // The riiz composite scores music-under-speech well below the 0.76 knee
        // (speech pauses/flux break the predicate). A sustained 0.45 carrier must
        // NOT fire — this is the whole precision point.
        let probs = Array(repeating: 0.45, count: 12)
        #expect(propose(probs).isEmpty)
    }

    // MARK: - Gap tolerance

    @Test("a single 2s dip does NOT split an otherwise sustained run")
    func singleWindowDipDoesNotSplitRun() {
        // 3 music, 1 dip, 3 music, 2 speech. The dip is within tolerance, so the
        // run stays whole and yields ONE span spanning both music halves.
        let probs = [0.9, 0.9, 0.9, 0.3, 0.9, 0.9, 0.9] + [0.1, 0.1]
        let spans = propose(probs)
        #expect(spans.count == 1)
        #expect(spans[0].startTime == 0.0)
        #expect(spans[0].endTime == 14.0)   // end of the 7th window (index 6)
        // Dip window is not counted in the mean → confidence stays 0.9.
        #expect(abs(spans[0].confidence - 0.9) < 1e-9)
    }

    @Test("a two-window gap DOES split into separate runs")
    func twoWindowGapSplitsRun() {
        // 5 music, 2 speech (splits), 5 music, 2 speech. Both halves are >= 8s.
        let probs = [0.9, 0.9, 0.9, 0.9, 0.9] + [0.1, 0.1]
            + [0.9, 0.9, 0.9, 0.9, 0.9] + [0.1, 0.1]
        let spans = propose(probs).sorted { $0.startTime < $1.startTime }
        #expect(spans.count == 2)
        #expect(spans[0].startTime == 0.0)
        #expect(spans[0].endTime == 10.0)
        #expect(spans[1].startTime == 14.0)  // 7 windows * 2s
        #expect(spans[1].endTime == 24.0)
    }

    // MARK: - Confidence monotonicity

    @Test("confidence is monotonic in run strength (stronger music ⇒ higher confidence)")
    func confidenceMonotonicInRunStrength() {
        let strong = propose(Array(repeating: 0.95, count: 6))
        let weak = propose(Array(repeating: 0.80, count: 6))
        #expect(strong.count == 1)
        #expect(weak.count == 1)
        #expect(strong[0].confidence > weak[0].confidence)
    }

    // MARK: - Degenerate input

    @Test("empty windows → empty output")
    func emptyWindowsProducesNothing() {
        #expect(SustainedMusicOffsetProposer.propose(featureWindows: [], episodeDuration: 0).isEmpty)
    }
}
