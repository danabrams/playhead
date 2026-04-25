// BracketActivationTests.swift
// playhead-arf8: Regression rails for the BracketAwareBoundaryRefiner
// gate. Verifies that the master flag, per-show trust gate, coarse-score
// gate, fine-confidence gate, and the happy-path bracket refinement each
// route a candidate span to the expected `Path` and produce the expected
// (startAdjust, endAdjust). Pairs with `BracketDetectorTests` (state
// machine) and `FineBoundaryRefinerTests` (precision step) — this file
// is the seam that proves the live activation contract upstream of
// SkipOrchestrator.

import Foundation
import Testing

@testable import Playhead

// MARK: - Test Helpers

/// Build a synthetic `FeatureWindow` for the bracket-aware refiner. Mirrors
/// the helper inside `BracketDetectorTests` so the two suites can share a
/// mental model of "what a fixture window looks like".
private func bracketWindow(
    startTime: Double,
    duration: Double = 2.0,
    rms: Double = 0.1,
    spectralFlux: Double = 0.05,
    musicProbability: Double = 0.0,
    musicBedOnsetScore: Double = 0.0,
    musicBedOffsetScore: Double = 0.0,
    musicBedLevel: MusicBedLevel = .none,
    pauseProbability: Double = 0.0
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: "test-bracket-activation",
        startTime: startTime,
        endTime: startTime + duration,
        rms: rms,
        spectralFlux: spectralFlux,
        musicProbability: musicProbability,
        musicBedOnsetScore: musicBedOnsetScore,
        musicBedOffsetScore: musicBedOffsetScore,
        musicBedLevel: musicBedLevel,
        pauseProbability: pauseProbability,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 4
    )
}

/// Build a windowed envelope that BracketDetector reliably resolves to a
/// full bracket: silence → onset → bed → bed → bed → offset → silence.
/// Mirrors `BracketDetectorTests.fullBracketDetection` so the gate-suite
/// can keep assertions precise about onset / offset times.
///
/// Returns the full window list. Caller chooses `candidateStart` /
/// `candidateEnd` per scenario.
private func fullBracketWindows() -> [FeatureWindow] {
    var windows: [FeatureWindow] = []
    // Pre-ad silence
    windows.append(bracketWindow(startTime: 0.0, rms: 0.05))
    windows.append(bracketWindow(startTime: 2.0, rms: 0.05))
    // Onset: high musicBedOnsetScore near candidate start, plus a strong
    // pause+spectral cue so FineBoundaryRefiner can lock down the edge.
    windows.append(bracketWindow(
        startTime: 4.0,
        rms: 0.3,
        spectralFlux: 0.85,
        musicProbability: 0.5,
        musicBedOnsetScore: 0.5,
        pauseProbability: 0.85
    ))
    // Bed sustained
    windows.append(bracketWindow(
        startTime: 6.0, rms: 0.2, musicProbability: 0.4
    ))
    windows.append(bracketWindow(
        startTime: 8.0, rms: 0.2, musicProbability: 0.35
    ))
    windows.append(bracketWindow(
        startTime: 10.0, rms: 0.2, musicProbability: 0.3
    ))
    // Offset: high musicBedOffsetScore near candidate end, plus strong
    // pause+spectral cue at the trailing edge.
    windows.append(bracketWindow(
        startTime: 12.0,
        rms: 0.15,
        spectralFlux: 0.85,
        musicProbability: 0.1,
        musicBedOffsetScore: 0.5,
        pauseProbability: 0.85
    ))
    // Post-ad silence
    windows.append(bracketWindow(startTime: 14.0, rms: 0.05))
    windows.append(bracketWindow(startTime: 16.0, rms: 0.05))
    return windows
}

/// Config helper: start from `AdDetectionConfig.default` and override one
/// or more bracket-refinement fields without re-typing the unrelated ones.
private func bracketConfig(
    enabled: Bool = true,
    minTrust: Double = 0.40,
    minCoarseScore: Double = 0.30,
    minFineConfidence: Double = 0.20
) -> AdDetectionConfig {
    AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: "detection-v1",
        bracketRefinementEnabled: enabled,
        bracketRefinementMinTrust: minTrust,
        bracketRefinementMinCoarseScore: minCoarseScore,
        bracketRefinementMinFineConfidence: minFineConfidence
    )
}

// MARK: - BracketAwareBoundaryRefiner Gate Tests

@Suite("BracketAwareBoundaryRefiner gates")
struct BracketAwareBoundaryRefinerGateTests {

    // MARK: - Master flag

    @Test("master flag off short-circuits to legacy fallback with zero adjustments")
    func masterFlagOffReturnsLegacy() {
        let windows = fullBracketWindows()

        let result = BracketAwareBoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 4.0,
            candidateEnd: 14.0,
            showTrust: 0.7,
            config: bracketConfig(enabled: false)
        )

        #expect(result.path == .legacy)
        #expect(result.startAdjust == 0.0)
        #expect(result.endAdjust == 0.0)
    }

    // MARK: - Window-count guard

    @Test("fewer than three windows short-circuits to legacy fallback")
    func tooFewWindowsReturnsLegacy() {
        let windows = [
            bracketWindow(startTime: 4.0),
            bracketWindow(startTime: 6.0),
        ]

        let result = BracketAwareBoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 4.0,
            candidateEnd: 14.0,
            showTrust: 0.7,
            config: bracketConfig()
        )

        #expect(result.path == .legacy)
        #expect(result.startAdjust == 0.0)
        #expect(result.endAdjust == 0.0)
    }

    // MARK: - Trust gate

    @Test("trust below floor suppresses bracket path with zero adjustments")
    func trustBelowFloorIsGated() {
        let windows = fullBracketWindows()

        // Trust 0.30 is below the default floor of 0.40 — gate must trip
        // *before* the detector runs, so we never look at the windows.
        let result = BracketAwareBoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 4.0,
            candidateEnd: 14.0,
            showTrust: 0.30,
            config: bracketConfig()
        )

        if case .trustGated(let observedTrust) = result.path {
            #expect(observedTrust == 0.30)
        } else {
            Issue.record("expected .trustGated path, got \(result.path)")
        }
        #expect(result.startAdjust == 0.0)
        #expect(result.endAdjust == 0.0)
    }

    @Test("trust at floor is admitted (boundary inclusive)")
    func trustAtFloorIsAdmitted() {
        let windows = fullBracketWindows()

        // Trust exactly equal to the floor — the gate condition is
        // `>= minTrust`, so this must NOT be gated.
        let result = BracketAwareBoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 4.0,
            candidateEnd: 14.0,
            showTrust: 0.40,
            config: bracketConfig(minTrust: 0.40)
        )

        // Anything other than `.trustGated` confirms the boundary handling.
        // Specific downstream path depends on detector + fine refiner
        // outputs; what we're asserting here is purely the gate semantics.
        if case .trustGated = result.path {
            Issue.record("trust at floor must not be gated; got \(result.path)")
        }
    }

    // MARK: - No-bracket (host-read) graceful no-op

    @Test("host-read with no music bed returns noBracket and zero adjustments")
    func hostReadReturnsNoBracket() {
        // Flat windows with no music probability anywhere — BracketDetector
        // returns nil, the refiner reports `.noBracket`, and the caller
        // falls back to the legacy refiner.
        let windows = (0..<10).map { i in
            bracketWindow(
                startTime: Double(i) * 2.0,
                rms: 0.1,
                musicProbability: 0.0,
                musicBedOnsetScore: 0.0,
                musicBedOffsetScore: 0.0
            )
        }

        let result = BracketAwareBoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 4.0,
            candidateEnd: 14.0,
            showTrust: 0.7,
            config: bracketConfig()
        )

        #expect(result.path == .noBracket)
        #expect(result.startAdjust == 0.0)
        #expect(result.endAdjust == 0.0)
    }

    // MARK: - Coarse-score gate

    @Test("coarse-score floor above bracket strength routes to coarseGated")
    func coarseScoreFloorTooHighIsGated() {
        let windows = fullBracketWindows()

        // Force the gate to fail: floor at 1.5 is above any possible
        // coarseScore (which is bounded to [0, 1]). Bracket evidence is
        // produced, then immediately rejected by the coarse-score gate.
        let result = BracketAwareBoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 4.0,
            candidateEnd: 14.0,
            showTrust: 0.7,
            config: bracketConfig(minCoarseScore: 1.5)
        )

        if case .coarseGated(let observedCoarseScore) = result.path {
            #expect(observedCoarseScore <= 1.0)
            #expect(observedCoarseScore > 0.0)
        } else {
            Issue.record("expected .coarseGated path, got \(result.path)")
        }
        #expect(result.startAdjust == 0.0)
        #expect(result.endAdjust == 0.0)
    }

    // MARK: - Fine-confidence gate

    @Test("fine-confidence floor above achievable confidence routes to fineConfidenceGated")
    func fineConfidenceFloorTooHighIsGated() {
        let windows = fullBracketWindows()

        // Force the fine-confidence gate to fail by setting an unachievable
        // floor. BracketDetector finds a bracket and FineBoundaryRefiner
        // emits estimates, but at least one edge is below the floor, so
        // the bracket adjustments are dropped and the caller falls back.
        let result = BracketAwareBoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 4.0,
            candidateEnd: 14.0,
            showTrust: 0.7,
            config: bracketConfig(minFineConfidence: 1.5)
        )

        if case .fineConfidenceGated(let startConf, let endConf) = result.path {
            #expect(startConf <= 1.0)
            #expect(endConf <= 1.0)
        } else {
            Issue.record("expected .fineConfidenceGated path, got \(result.path)")
        }
        #expect(result.startAdjust == 0.0)
        #expect(result.endAdjust == 0.0)
    }

    // MARK: - Happy path

    @Test("full bracket with trust + scores above floors yields bracketRefined adjustments")
    func happyPathReturnsBracketRefined() {
        let windows = fullBracketWindows()

        // Use very permissive floors so a real fixture can clear them.
        let result = BracketAwareBoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 4.0,
            candidateEnd: 14.0,
            showTrust: 0.7,
            config: bracketConfig(
                minTrust: 0.40,
                minCoarseScore: 0.05,
                minFineConfidence: 0.0
            )
        )

        if case .bracketRefined(let coarseScore, let startConf, let endConf, _) = result.path {
            #expect(coarseScore > 0.0)
            #expect(coarseScore <= 1.0)
            #expect(startConf >= 0.0)
            #expect(startConf <= 1.0)
            #expect(endConf >= 0.0)
            #expect(endConf <= 1.0)
        } else {
            Issue.record("expected .bracketRefined path, got \(result.path)")
        }

        // Adjustments must be inside the ±3s budget enforced by the clamp.
        #expect(abs(result.startAdjust) <= BracketAwareBoundaryRefiner.maxBoundaryAdjust)
        #expect(abs(result.endAdjust) <= BracketAwareBoundaryRefiner.maxBoundaryAdjust)
    }

    // MARK: - Clamp budget

    @Test("clamp budget caps adjustments at +/- 3 seconds")
    func clampBudgetAtThreeSeconds() {
        // The bracket-aware path must never shift a boundary further than
        // the legacy `BoundaryRefiner` could (also clamped to ±3s). Verify
        // by pinning the public constant directly.
        #expect(BracketAwareBoundaryRefiner.maxBoundaryAdjust == 3.0)
    }
}

// MARK: - BracketRefinementCounts Tests

@Suite("BracketRefinementCounts default state")
struct BracketRefinementCountsTests {

    @Test("default-initialised counts are all zero")
    func defaultInitIsAllZero() {
        let counts = BracketRefinementCounts()
        #expect(counts.bracketRefined == 0)
        #expect(counts.noBracket == 0)
        #expect(counts.trustGated == 0)
        #expect(counts.coarseGated == 0)
        #expect(counts.fineConfidenceGated == 0)
        #expect(counts.legacyBypass == 0)
    }
}
