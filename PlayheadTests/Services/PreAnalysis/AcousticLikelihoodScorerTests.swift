// AcousticLikelihoodScorerTests.swift
// playhead-gtt9.24: scheduler-time acoustic ad-likelihood scoring.
//
// Pure-function tests of `AcousticLikelihoodScorer.scoreOne` and
// `highestLikelihoodBeyond`. The scorer is intentionally stateless so
// these tests don't need a store, a scheduler, or a runner — they
// exercise the math directly against synthesized `FeatureWindow`
// rows.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("AcousticLikelihoodScorer (playhead-gtt9.24)")
struct AcousticLikelihoodScorerTests {

    // MARK: - Synthesizers

    /// Synthesize a "clean host conversation" feature window — no music
    /// bed, no spectral flux spike, no speaker change. This is the
    /// baseline non-ad shape; the scorer should return a near-zero
    /// likelihood here.
    private func cleanSpeechWindow(
        startTime: Double = 0,
        endTime: Double = 5
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-test",
            startTime: startTime,
            endTime: endTime,
            rms: 0.08,
            spectralFlux: 0.05,
            musicProbability: 0.05,
            speakerChangeProxyScore: 0.0,
            musicBedChangeScore: 0.0,
            musicBedOnsetScore: 0.0,
            musicBedOffsetScore: 0.0,
            musicBedLevel: .none,
            pauseProbability: 0.1,
            speakerClusterId: 1,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    /// Synthesize a "clear ad onset" window — strong music-bed onset,
    /// foreground music level, modest speaker-change proxy. This is the
    /// canonical positive shape: a host hand-off into a sponsor read.
    private func adOnsetWindow(
        startTime: Double = 1200,
        endTime: Double = 1205
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-test",
            startTime: startTime,
            endTime: endTime,
            rms: 0.18,
            spectralFlux: 0.40,
            musicProbability: 0.85,
            speakerChangeProxyScore: 0.7,
            musicBedChangeScore: 0.8,
            musicBedOnsetScore: 0.9,
            musicBedOffsetScore: 0.0,
            musicBedLevel: .foreground,
            pauseProbability: 0.2,
            speakerClusterId: 2,
            jingleHash: "ad-jingle-x",
            featureVersion: 1
        )
    }

    /// Synthesize a "borderline" window — only one acoustic cue is
    /// present. Useful for asserting the threshold separates
    /// borderlines from clear positives.
    private func borderlineSpeakerChangeWindow(
        startTime: Double = 600,
        endTime: Double = 605
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-test",
            startTime: startTime,
            endTime: endTime,
            rms: 0.10,
            spectralFlux: 0.10,
            musicProbability: 0.20,
            speakerChangeProxyScore: 0.6,  // moderate speaker change
            musicBedChangeScore: 0.0,
            musicBedOnsetScore: 0.0,
            musicBedOffsetScore: 0.0,
            musicBedLevel: .none,
            pauseProbability: 0.3,
            speakerClusterId: 3,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    // MARK: - scoreOne

    @Test("clean speech windows score near zero")
    func testCleanSpeechScoresNearZero() {
        let window = cleanSpeechWindow()
        let score = AcousticLikelihoodScorer.scoreOne(window)
        #expect(score < 0.2,
                "Clean host conversation has zero music-bed onset/offset, zero speaker change, .none level — should fall well below promotion threshold")
    }

    @Test("clear ad-onset windows score above default threshold")
    func testAdOnsetScoresAboveThreshold() {
        let window = adOnsetWindow()
        let score = AcousticLikelihoodScorer.scoreOne(window)
        #expect(score >= 0.5,
                "Strong music-bed onset + foreground level + speaker change should clear the 0.5 default threshold; got \(score)")
    }

    @Test("score is bounded in [0, 1]")
    func testScoreIsBoundedInUnitInterval() {
        // Engineer a maximal feature stack — every component at 1.0.
        // Even with all cues, the bounded-additive combiner clamps to
        // 1.0 at the ceiling.
        let maxed = FeatureWindow(
            analysisAssetId: "asset-test",
            startTime: 0,
            endTime: 5,
            rms: 1.0,
            spectralFlux: 10.0,           // way above the 0.5 soft cap
            musicProbability: 1.0,
            speakerChangeProxyScore: 1.0,
            musicBedChangeScore: 1.0,
            musicBedOnsetScore: 1.0,
            musicBedOffsetScore: 1.0,
            musicBedLevel: .foreground,
            pauseProbability: 1.0,
            speakerClusterId: 1,
            jingleHash: nil,
            featureVersion: 1
        )
        let score = AcousticLikelihoodScorer.scoreOne(maxed)
        #expect(score >= 0.0 && score <= 1.0,
                "Score must be clamped to [0, 1]; got \(score)")
        #expect(score >= 0.99,
                "All-features-maxed window should reach the score ceiling")
    }

    @Test("borderline single-feature windows do not trigger default threshold")
    func testBorderlineDoesNotCrossThreshold() {
        let window = borderlineSpeakerChangeWindow()
        let score = AcousticLikelihoodScorer.scoreOne(window)
        #expect(score < 0.5,
                "Single moderate cue (speaker change only) must not clear 0.5 — borderline shapes should stay below the gate; got \(score)")
    }

    @Test("musicBedLevel contribution: foreground > background > none")
    func testMusicBedLevelMonotone() {
        // Hold every other feature at zero so the level component is
        // the only signal contributing.
        let baseline = { (level: MusicBedLevel) in
            FeatureWindow(
                analysisAssetId: "asset-test",
                startTime: 0,
                endTime: 5,
                rms: 0,
                spectralFlux: 0,
                musicProbability: 0,
                speakerChangeProxyScore: 0,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: level,
                pauseProbability: 0,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            )
        }
        let none = AcousticLikelihoodScorer.scoreOne(baseline(.none))
        let bg = AcousticLikelihoodScorer.scoreOne(baseline(.background))
        let fg = AcousticLikelihoodScorer.scoreOne(baseline(.foreground))
        #expect(none == 0.0)
        #expect(bg > none)
        #expect(fg > bg)
    }

    @Test("custom weights flow through")
    func testCustomWeightsFlowThrough() {
        // All-features-maxed window. With default weights the score
        // reaches 1.0; with a weight set that puts everything on
        // spectralFlux, the contribution depends only on the flux soft
        // cap. Verify the override is honored.
        let maxed = FeatureWindow(
            analysisAssetId: "asset-test",
            startTime: 0,
            endTime: 5,
            rms: 1.0,
            spectralFlux: 0.25,            // half the soft cap → 0.5
            musicProbability: 0.0,
            speakerChangeProxyScore: 0.0,
            musicBedChangeScore: 0.0,
            musicBedOnsetScore: 0.0,
            musicBedOffsetScore: 0.0,
            musicBedLevel: .none,
            pauseProbability: 0.0,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
        let onlyFluxWeights = AcousticLikelihoodScorer.Weights(
            musicBedOnset: 0,
            musicBedOffset: 0,
            speakerChangeProxy: 0,
            musicBedLevel: 0,
            spectralFlux: 1.0
        )
        let score = AcousticLikelihoodScorer.scoreOne(maxed, weights: onlyFluxWeights)
        #expect(abs(score - 0.5) < 1e-9,
                "All weight on flux + flux at half the soft cap → 0.5 contribution; got \(score)")
    }

    // MARK: - highestLikelihoodBeyond

    @Test("highestLikelihoodBeyond returns nil when no windows exist past coverage")
    func testHighestLikelihoodBeyondEmpty() {
        let result = AcousticLikelihoodScorer.highestLikelihoodBeyond(
            windows: [],
            currentCoverageSec: 600
        )
        #expect(result == nil,
                "Empty input → nil (cold-start fallback)")
    }

    @Test("highestLikelihoodBeyond ignores windows entirely before the cutoff")
    func testHighestLikelihoodBeyondIgnoresPrefix() {
        let windows = [
            adOnsetWindow(startTime: 100, endTime: 105),     // BEFORE cutoff
            cleanSpeechWindow(startTime: 700, endTime: 705)  // after cutoff but low score
        ]
        let result = AcousticLikelihoodScorer.highestLikelihoodBeyond(
            windows: windows,
            currentCoverageSec: 600
        )
        #expect(result == nil,
                "Ad-onset before cutoff is ignored; the after-cutoff clean window is below threshold → nil")
    }

    @Test("highestLikelihoodBeyond picks the highest-score window past the cutoff")
    func testHighestLikelihoodBeyondPicksHighest() {
        let windows = [
            cleanSpeechWindow(startTime: 700, endTime: 705),
            adOnsetWindow(startTime: 800, endTime: 805),
            borderlineSpeakerChangeWindow(startTime: 900, endTime: 905)
        ]
        let result = AcousticLikelihoodScorer.highestLikelihoodBeyond(
            windows: windows,
            currentCoverageSec: 600
        )
        let unwrapped = try? #require(result)
        #expect(unwrapped?.windowStart == 800)
        #expect(unwrapped?.windowEnd == 805)
        #expect((unwrapped?.score ?? 0) >= 0.5)
    }

    @Test("highestLikelihoodBeyond honors custom threshold")
    func testHighestLikelihoodBeyondHonorsThreshold() {
        let windows = [
            borderlineSpeakerChangeWindow(startTime: 900, endTime: 905)  // ~0.12 score
        ]
        // Default threshold (0.5) excludes the borderline window.
        let defaultResult = AcousticLikelihoodScorer.highestLikelihoodBeyond(
            windows: windows,
            currentCoverageSec: 600
        )
        #expect(defaultResult == nil)

        // Aggressive threshold (0.1) includes it.
        let aggressiveResult = AcousticLikelihoodScorer.highestLikelihoodBeyond(
            windows: windows,
            currentCoverageSec: 600,
            threshold: 0.1
        )
        #expect(aggressiveResult != nil,
                "Lowering the threshold to 0.1 should admit a borderline window that the default 0.5 rejects")
    }

    @Test("score(windows:weights:) is index-aligned with input")
    func testScoreArrayIsIndexAligned() {
        let windows = [
            cleanSpeechWindow(startTime: 0, endTime: 5),
            adOnsetWindow(startTime: 100, endTime: 105),
            borderlineSpeakerChangeWindow(startTime: 200, endTime: 205)
        ]
        let scores = AcousticLikelihoodScorer.score(windows: windows)
        #expect(scores.count == windows.count)
        #expect(scores[0].windowStart == 0)
        #expect(scores[1].windowStart == 100)
        #expect(scores[2].windowStart == 200)
        // Ad-onset is the highest of the three.
        #expect(scores[1].score > scores[0].score)
        #expect(scores[1].score > scores[2].score)
    }
}

#endif
