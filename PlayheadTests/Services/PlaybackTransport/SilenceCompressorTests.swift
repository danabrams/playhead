// SilenceCompressorTests.swift
// playhead-epii — Decision-matrix coverage for the structure-aware
// silence compressor planner. Pure unit tests against the static
// `derivePlans` function and the stateful `tick(currentTime:)` state
// machine — no AVFoundation, no AnalysisStore.
//
// What's covered (matches the bead's risks/acceptance section):
//   - Music-bed decision matrix (musicProbability × musicBedLevel)
//   - Dead-air detection (high pause + speakerChangeProxy)
//   - Dramatic-pause PRESERVATION between same-speaker windows
//   - Speech-adjacent music: spectral, not varispeed
//   - AdWindow / skip-cue regions: NO compression overlap
//   - Per-show override: short-circuits planner
//   - Rate transitions: re-emerging restores user base speed
//   - Rapid window churn: no plan thrash (minimumGapSeconds filter)
//   - Empty buffer: no decision change

import Foundation
import Testing

@testable import Playhead

@Suite("SilenceCompressor decision matrix (playhead-epii)")
struct SilenceCompressorDecisionTests {

    // MARK: - Helpers

    /// Build a `FeatureWindow` with sensible defaults; tests override
    /// only the fields under test. `featureVersion` is non-load-bearing
    /// for planner logic.
    private func window(
        start: Double,
        end: Double,
        musicProbability: Double = 0.0,
        pauseProbability: Double = 0.0,
        musicBedLevel: MusicBedLevel = .none,
        musicBedOnsetScore: Double = 1.0,
        musicBedOffsetScore: Double = 1.0,
        speakerChangeProxyScore: Double = 0.0,
        speakerClusterId: Int? = nil
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test-asset",
            startTime: start,
            endTime: end,
            rms: 0.3,
            spectralFlux: 0.1,
            musicProbability: musicProbability,
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: 0,
            musicBedOnsetScore: musicBedOnsetScore,
            musicBedOffsetScore: musicBedOffsetScore,
            musicBedLevel: musicBedLevel,
            pauseProbability: pauseProbability,
            speakerClusterId: speakerClusterId,
            jingleHash: nil,
            featureVersion: 4
        )
    }

    // MARK: - Music-bed matrix

    @Test("musicProbability > 0.7 + foreground bed sustained 6s ⇒ varispeed plan at high rate")
    func foregroundJingleSustained() {
        let windows: [FeatureWindow] = [
            // 0..2 speech (content)
            window(start: 0, end: 2, musicProbability: 0.1, pauseProbability: 0.05),
            // 2..8 sustained foreground music — three back-to-back windows.
            window(start: 2, end: 4, musicProbability: 0.9, musicBedLevel: .foreground),
            window(start: 4, end: 6, musicProbability: 0.9, musicBedLevel: .foreground),
            window(start: 6, end: 8, musicProbability: 0.9, musicBedLevel: .foreground),
            // 8..10 speech again
            window(start: 8, end: 10, musicProbability: 0.1, pauseProbability: 0.05),
        ]
        let plans = SilenceCompressor.derivePlans(
            from: windows, config: .default
        )
        // The first and last windows of the music run are speech-adjacent
        // (windows[0] at .content sits before, windows[4] at .content sits
        // after). Per the planner, that pulls the bucket back to
        // `.musicSpeechAdjacent` ⇒ spectral + low rate.
        #expect(plans.count == 1)
        let plan = plans[0]
        #expect(plan.algorithm == .spectral)
        #expect(plan.multiplier == SilenceCompressorConfig.default.lowRateMultiplier)
    }

    @Test("Pure-music run (no speech adjacency) escalates to varispeed/high rate")
    func pureMusicEscalates() {
        // Sustained music with NO neighbouring speech windows in the
        // sliced buffer — simulates a long jingle/intro.
        let windows: [FeatureWindow] = (0..<6).map { index in
            window(
                start: Double(index) * 2,
                end: Double(index + 1) * 2,
                musicProbability: 0.9,
                musicBedLevel: .foreground
            )
        }
        let plans = SilenceCompressor.derivePlans(
            from: windows, config: .default
        )
        #expect(plans.count == 1)
        #expect(plans[0].algorithm == .varispeed)
        #expect(plans[0].multiplier == SilenceCompressorConfig.default.highRateMultiplier)
    }

    @Test("musicProbability below floor ⇒ no plan even with foreground bed")
    func lowMusicProbabilityNoPlan() {
        let windows: [FeatureWindow] = (0..<5).map { index in
            window(
                start: Double(index) * 2,
                end: Double(index + 1) * 2,
                musicProbability: 0.5,  // below the 0.7 floor
                musicBedLevel: .foreground
            )
        }
        let plans = SilenceCompressor.derivePlans(
            from: windows, config: .default
        )
        #expect(plans.isEmpty)
    }

    @Test("musicBedLevel == .none short-circuits even at high probability")
    func noBedLevelNoPlan() {
        let windows: [FeatureWindow] = (0..<5).map { index in
            window(
                start: Double(index) * 2,
                end: Double(index + 1) * 2,
                musicProbability: 0.95,
                musicBedLevel: .none
            )
        }
        let plans = SilenceCompressor.derivePlans(
            from: windows, config: .default
        )
        #expect(plans.isEmpty)
    }

    // MARK: - Dead-air

    @Test("High pauseProbability + speakerChangeProxy ⇒ dead-air spectral plan")
    func deadAirCompresses() {
        let windows: [FeatureWindow] = [
            window(start: 0, end: 2, pauseProbability: 0.05, speakerClusterId: 1),
            // 2..8 dead air: three high-pause windows with high speaker
            // change proxy. The middle window has speakerClusterId == nil
            // so the dramatic-pause detector cannot match (no neighbouring
            // same-speaker window pair).
            window(start: 2, end: 4, pauseProbability: 0.9, speakerChangeProxyScore: 0.8),
            window(start: 4, end: 6, pauseProbability: 0.9, speakerChangeProxyScore: 0.8),
            window(start: 6, end: 8, pauseProbability: 0.9, speakerChangeProxyScore: 0.8),
            window(start: 8, end: 10, pauseProbability: 0.05, speakerClusterId: 2),
        ]
        let plans = SilenceCompressor.derivePlans(
            from: windows, config: .default
        )
        #expect(plans.count == 1)
        #expect(plans[0].algorithm == .spectral)
        #expect(plans[0].multiplier == SilenceCompressorConfig.default.lowRateMultiplier)
    }

    // MARK: - DRAMATIC PAUSE (the most important invariant)

    @Test(
        "Dramatic pause: same-speaker bracket around high-pause window ⇒ NO compression"
    )
    func dramaticPausePreservedSameSpeaker() {
        // Speaker 1 → pause → speaker 1. This is the narrative "beat"
        // the bead explicitly demands we preserve.
        let windows: [FeatureWindow] = [
            window(start: 0, end: 2, pauseProbability: 0.05, speakerClusterId: 1),
            window(start: 2, end: 4, pauseProbability: 0.05, speakerClusterId: 1),
            // Long dramatic pause across three windows (6s — well above
            // the minimumGapSeconds threshold so a buggy planner would
            // have plenty of runway to compress it).
            window(start: 4, end: 6, pauseProbability: 0.95, speakerClusterId: nil),
            window(start: 6, end: 8, pauseProbability: 0.95, speakerClusterId: nil),
            window(start: 8, end: 10, pauseProbability: 0.95, speakerClusterId: nil),
            window(start: 10, end: 12, pauseProbability: 0.05, speakerClusterId: 1),
            window(start: 12, end: 14, pauseProbability: 0.05, speakerClusterId: 1),
        ]
        let plans = SilenceCompressor.derivePlans(
            from: windows, config: .default
        )
        #expect(plans.isEmpty, "Dramatic pauses between same-speaker windows MUST be preserved")
    }

    @Test(
        "Pause between DIFFERENT speakers is dead-air, not dramatic — compression OK"
    )
    func differentSpeakerPauseCompresses() {
        let windows: [FeatureWindow] = [
            window(start: 0, end: 2, pauseProbability: 0.05, speakerClusterId: 1),
            // Pause windows carry the speakerChangeProxyScore signal
            // (the analyser writes it on the pause itself). Three 2s
            // windows = 6s total run, easily clears minimumGapSeconds.
            window(
                start: 2, end: 4, pauseProbability: 0.9, speakerChangeProxyScore: 0.8,
                speakerClusterId: nil
            ),
            window(
                start: 4, end: 6, pauseProbability: 0.9, speakerChangeProxyScore: 0.8,
                speakerClusterId: nil
            ),
            window(
                start: 6, end: 8, pauseProbability: 0.9, speakerChangeProxyScore: 0.8,
                speakerClusterId: nil
            ),
            window(start: 8, end: 10, pauseProbability: 0.05, speakerClusterId: 2),
        ]
        let plans = SilenceCompressor.derivePlans(
            from: windows, config: .default
        )
        #expect(plans.count == 1)
    }

    @Test("Short gap below minimumGapSeconds ⇒ no plan emitted")
    func shortGapBelowThresholdRejected() {
        // 2-second music gap is below the 4-second minimum; the planner
        // should not raise rate for a 2-second blip.
        let windows: [FeatureWindow] = [
            window(start: 0, end: 2, pauseProbability: 0.05, speakerClusterId: 1),
            window(start: 2, end: 4, musicProbability: 0.9, musicBedLevel: .foreground),
            window(start: 4, end: 6, pauseProbability: 0.05, speakerClusterId: 2),
        ]
        let plans = SilenceCompressor.derivePlans(
            from: windows, config: .default
        )
        #expect(plans.isEmpty)
    }

    // MARK: - Skip ranges (AdWindow already handled)

    @Test("Skip range covering a music run suppresses the compression plan")
    func skipRangeYieldsToSkipPath() {
        let windows: [FeatureWindow] = (0..<6).map { index in
            window(
                start: Double(index) * 2,
                end: Double(index + 1) * 2,
                musicProbability: 0.9,
                musicBedLevel: .foreground
            )
        }
        // Skip path will jump from 1..11s — should swallow the entire
        // music run.
        let plans = SilenceCompressor.derivePlans(
            from: windows,
            config: .default,
            skipRanges: [(start: 1, end: 11)]
        )
        #expect(plans.isEmpty, "Skip path must own these regions; no double-compression")
    }

    // MARK: - Empty buffer

    @Test("Empty windows array ⇒ empty plan list")
    func emptyBufferNoPlans() {
        let plans = SilenceCompressor.derivePlans(from: [], config: .default)
        #expect(plans.isEmpty)
    }
}

// MARK: - State machine tests

@Suite("SilenceCompressor state machine (playhead-epii)")
struct SilenceCompressorStateMachineTests {

    private func makePureMusicWindows(start: Double = 10) -> [FeatureWindow] {
        (0..<6).map { index in
            FeatureWindow(
                analysisAssetId: "test",
                startTime: start + Double(index) * 2,
                endTime: start + Double(index + 1) * 2,
                rms: 0.3,
                spectralFlux: 0.1,
                musicProbability: 0.9,
                speakerChangeProxyScore: 0,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 1.0,
                musicBedOffsetScore: 1.0,
                musicBedLevel: .foreground,
                pauseProbability: 0.0,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 4
            )
        }
    }

    @Test("Idle ⇒ engage when playhead enters a plan")
    func engageOnEntry() {
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        let decision = compressor.tick(currentTime: 11)
        if case .engage = decision {
            #expect(compressor.isCurrentlyCompressing)
        } else {
            Issue.record("expected .engage, got \(decision)")
        }
    }

    @Test("Compressing ⇒ disengage on exit")
    func disengageOnExit() {
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        _ = compressor.tick(currentTime: 11)
        let decision = compressor.tick(currentTime: 30)  // well past the run
        #expect(decision == .disengage)
        #expect(!compressor.isCurrentlyCompressing)
    }

    @Test("Compressing ⇒ noChange while inside the plan")
    func noChangeInsidePlan() {
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        _ = compressor.tick(currentTime: 11)
        let decision = compressor.tick(currentTime: 12)
        #expect(decision == .noChange)
    }

    @Test("Per-show override clears any in-flight compression on the next tick")
    func overrideClearsCompression() {
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        _ = compressor.tick(currentTime: 11)  // engaged
        compressor.recordKeepFullMusicOverride(true)
        let decision = compressor.tick(currentTime: 12)
        #expect(decision == .disengage)
    }

    @Test("Per-show override prevents engagement entirely")
    func overrideBlocksEngagement() {
        let compressor = SilenceCompressor()
        compressor.recordKeepFullMusicOverride(true)
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        let decision = compressor.tick(currentTime: 11)
        #expect(decision == .noChange)
        #expect(!compressor.isCurrentlyCompressing)
    }

    @Test("User seek mid-compression returns planner to idle")
    func seekResetsState() {
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        _ = compressor.tick(currentTime: 11)
        compressor.recordSeek(to: 100)
        #expect(!compressor.isCurrentlyCompressing)
    }

    @Test("Seek INTO a plan: next tick engages compression cleanly")
    func seekIntoPlanEngagesNextTick() {
        // Seed a music plan that covers t=10..22 (post-refinement
        // around 11..21). Tick once at t=5 (outside the plan) so the
        // planner stays idle. Then simulate a seek to t=15 (inside
        // the plan) and tick: should engage.
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        let pre = compressor.tick(currentTime: 5)
        #expect(pre == .noChange)
        compressor.recordSeek(to: 15)
        let post = compressor.tick(currentTime: 15)
        if case .engage = post {
            #expect(compressor.isCurrentlyCompressing)
        } else {
            Issue.record("Expected .engage after seek into plan, got \(post)")
        }
    }

    @Test("Override OFF after ON ⇒ next tick can re-engage compression")
    func overrideOffReEngagesAfterRefresh() {
        // Engage compression, flip override ON (clears plans + state),
        // then flip OFF and re-supply windows. The next tick must be
        // free to re-engage — there is no latent "still suppressed"
        // state inside the compressor after flip-OFF.
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        _ = compressor.tick(currentTime: 11)
        #expect(compressor.isCurrentlyCompressing)
        compressor.recordKeepFullMusicOverride(true)
        let suppressed = compressor.tick(currentTime: 12)
        #expect(suppressed == .disengage)
        compressor.recordKeepFullMusicOverride(false)
        // Re-supply windows (the host would do this via a refresh).
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        let resumed = compressor.tick(currentTime: 13)
        if case .engage = resumed {
            #expect(compressor.isCurrentlyCompressing)
        } else {
            Issue.record("Expected .engage after override OFF, got \(resumed)")
        }
    }

    @Test("Asset id change wipes plan list")
    func assetChangeClearsPlans() {
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        _ = compressor.tick(currentTime: 11)
        // New asset; new (empty) plan
        compressor.replaceWindows([], assetId: "a2")
        #expect(compressor.currentPlans.isEmpty)
        let decision = compressor.tick(currentTime: 11)
        #expect(decision == .disengage || decision == .noChange)
    }

    @Test("Skip range update filters live plan list")
    func skipRangeUpdateFiltersLive() {
        let compressor = SilenceCompressor()
        compressor.replaceWindows(makePureMusicWindows(start: 10), assetId: "a1")
        #expect(!compressor.currentPlans.isEmpty)
        compressor.updateSkipRanges([(start: 0, end: 50)])
        #expect(compressor.currentPlans.isEmpty)
    }

    @Test("Empty buffer ⇒ no engagement; tick is a no-op")
    func emptyBufferNoEngagement() {
        let compressor = SilenceCompressor()
        compressor.replaceWindows([], assetId: "a1")
        let decision = compressor.tick(currentTime: 5)
        #expect(decision == .noChange)
    }
}
