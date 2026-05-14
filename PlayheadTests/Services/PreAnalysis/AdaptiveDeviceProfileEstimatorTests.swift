// AdaptiveDeviceProfileEstimatorTests.swift
// playhead-beh3 (Phase 3 deliverable 5) — math-layer tests for the
// Welford+EWMA estimator that replaces the Phase 1 static seed table.
//
// Scope per the bead spec:
//   * Welford running variance correctness
//   * EWMA convergence under a constant input
//   * Clamp-band enforcement at both extremes
//   * Activation floor (min 30 samples before output departs from seed)
//   * One-notch-per-24h rate limit
//   * Never-zero floor (defensive)
//   * Divergence-revert after K=10 consecutive clamp saturations
//   * project(seed:scaledBy:) preserves non-slice fields verbatim
//
// Tests use a synthetic-grant feed (the bead spec explicitly authorizes
// this in lieu of the locked-core fixtures, which are AUDIO fixtures
// unsuitable for grant-window assertions).

import Foundation
import Testing

@testable import Playhead

@Suite("AdaptiveDeviceProfileEstimator math")
struct AdaptiveDeviceProfileEstimatorTests {

    // MARK: - Fixtures

    private static let referenceDate = Date(timeIntervalSince1970: 1_700_000_000)

    /// Seed for an iPhone17Pro (matches the bead-dh9b seed table).
    /// Tests pick this row because its grant-window (45 s) is large
    /// enough that the clamp-band math has measurable headroom.
    private static func freshState(seedSeconds: Double = 45) -> AdaptiveDeviceProfileState {
        AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone17Pro.rawValue,
            seedGrantWindowSeconds: seedSeconds,
            createdAt: referenceDate
        )
    }

    /// Apply N constant-value observations starting at `referenceDate`,
    /// stepping each observation forward by `step`. Useful for tests
    /// that need to drive activation/convergence without manually
    /// laying out 30+ observation rows.
    @discardableResult
    private static func apply(
        observations count: Int,
        value: Double,
        starting from: Date = referenceDate,
        step: TimeInterval = 60,
        to startState: AdaptiveDeviceProfileState,
        tuning: AdaptiveDeviceProfileTuning = .standard
    ) -> AdaptiveDeviceProfileState {
        var state = startState
        for i in 0..<count {
            let obs = GrantWindowObservation(
                grantWindowSeconds: value,
                observedAt: from.addingTimeInterval(Double(i) * step)
            )
            state = AdaptiveDeviceProfileEstimator.apply(
                observation: obs, to: state, tuning: tuning
            ).state
        }
        return state
    }

    // MARK: - Welford running variance

    @Test("Welford running mean converges to the arithmetic mean")
    func testWelfordMean() {
        var state = Self.freshState()
        let inputs: [Double] = [40, 42, 38, 44, 41]
        let arith = inputs.reduce(0, +) / Double(inputs.count)
        for (i, v) in inputs.enumerated() {
            let obs = GrantWindowObservation(
                grantWindowSeconds: v,
                observedAt: Self.referenceDate.addingTimeInterval(Double(i))
            )
            state = AdaptiveDeviceProfileEstimator.apply(observation: obs, to: state).state
        }
        #expect(abs(state.welfordMean - arith) < 1e-9)
        #expect(state.sampleCount == inputs.count)
    }

    @Test("Welford running variance matches the population variance")
    func testWelfordVariance() {
        var state = Self.freshState()
        let inputs: [Double] = [40, 42, 38, 44, 41]
        let mean = inputs.reduce(0, +) / Double(inputs.count)
        let expectedPopVar = inputs.map { ($0 - mean) * ($0 - mean) }.reduce(0, +) / Double(inputs.count)
        for (i, v) in inputs.enumerated() {
            let obs = GrantWindowObservation(
                grantWindowSeconds: v,
                observedAt: Self.referenceDate.addingTimeInterval(Double(i))
            )
            state = AdaptiveDeviceProfileEstimator.apply(observation: obs, to: state).state
        }
        #expect(abs(state.welfordVariance - expectedPopVar) < 1e-9)
    }

    @Test("Welford variance is zero with one or zero samples")
    func testWelfordVarianceColdStart() {
        let zero = Self.freshState()
        #expect(zero.welfordVariance == 0)

        let one = Self.apply(observations: 1, value: 42, to: zero)
        #expect(one.welfordVariance == 0, "variance is undefined with 1 sample; we report 0 instead of NaN")
        #expect(one.welfordMean == 42)
    }

    // MARK: - EWMA convergence

    @Test("EWMA converges to a constant input within tolerance after 30+ samples")
    func testEWMAConvergence() {
        let target: Double = 60 // 1.33× the 45-s seed → inside [0.5×, 2.0×]
        let state = Self.apply(observations: 60, value: target, to: Self.freshState())
        // After 60 observations with α=0.2 the EWMA is well within 1%
        // of the target value (geometric convergence: (1-α)^60 ≈ 1.5e-6).
        #expect(abs(state.ewmaSeconds - target) / target < 0.01,
                "EWMA should be within 1% of target after 60 samples; got \(state.ewmaSeconds) vs \(target)")
    }

    @Test("EWMA seeds to the first observation (no cold-start bias toward zero)")
    func testEWMAFirstObservationSeed() {
        let state = Self.apply(observations: 1, value: 50, to: Self.freshState())
        #expect(state.ewmaSeconds == 50,
                "first observation must SEED the EWMA, not blend with the zero prior")
    }

    // MARK: - Activation floor

    @Test("resolvedScaleFactor returns 1.0 below the activation floor")
    func testActivationFloorReturnsSeed() {
        // 29 samples = one short of the 30-sample activation floor.
        let belowFloor = Self.apply(observations: 29, value: 90, to: Self.freshState())
        let factor = AdaptiveDeviceProfileEstimator.resolvedScaleFactor(state: belowFloor)
        #expect(factor == 1.0, "estimator must return 1.0 (seed) below activation floor")
        #expect(belowFloor.isActivated(tuning: .standard) == false)
    }

    @Test("resolvedScaleFactor diverges from 1.0 once the activation floor is crossed")
    func testActivationFloorCrossing() {
        // Drive 30 samples at 2× the seed; the persisted factor is
        // rate-limited to one notch (0.1) above 1.0 on activation.
        let activated = Self.apply(observations: 30, value: 90, to: Self.freshState())
        #expect(activated.isActivated(tuning: .standard))
        let factor = AdaptiveDeviceProfileEstimator.resolvedScaleFactor(state: activated)
        #expect(factor > 1.0, "after 30 samples at 2× seed, scale factor must move upward")
        #expect(factor <= 1.1 + 1e-9, "but only by at most one notch (0.1) on the first activation observation")
    }

    // MARK: - Clamp band

    @Test("Clamp band caps the persisted scale factor at 2.0× under extreme over-shoot")
    func testClampBandHigh() {
        // 200 samples at 10× seed, with the notch rate limit disabled
        // (notchWindowSeconds=0) so the persisted factor can walk all
        // the way to the upper bound. notchStep stays at 0.1 so the
        // walk takes ~10 windows to saturate (we run 200 samples to
        // be safe). The clamp must keep us at 2.0 exactly, never above.
        var tuning = AdaptiveDeviceProfileTuning.standard
        tuning = AdaptiveDeviceProfileTuning(
            ewmaAlpha: tuning.ewmaAlpha,
            minSamplesForActivation: tuning.minSamplesForActivation,
            clampBandLower: tuning.clampBandLower,
            clampBandUpper: tuning.clampBandUpper,
            notchStep: tuning.notchStep,
            notchWindowSeconds: 0,
            divergenceObservationThreshold: .max // disable revert for this test
        )
        let state = Self.apply(observations: 200, value: 450, to: Self.freshState(), tuning: tuning)
        #expect(state.persistedScaleFactor <= tuning.clampBandUpper + 1e-9)
        #expect(state.persistedScaleFactor >= tuning.clampBandUpper - 0.1 - 1e-9,
                "with rate-limit disabled the factor must walk all the way to the upper clamp")
    }

    @Test("Clamp band floors the persisted scale factor at 0.5× under extreme under-shoot")
    func testClampBandLow() {
        var tuning = AdaptiveDeviceProfileTuning.standard
        tuning = AdaptiveDeviceProfileTuning(
            ewmaAlpha: tuning.ewmaAlpha,
            minSamplesForActivation: tuning.minSamplesForActivation,
            clampBandLower: tuning.clampBandLower,
            clampBandUpper: tuning.clampBandUpper,
            notchStep: tuning.notchStep,
            notchWindowSeconds: 0,
            divergenceObservationThreshold: .max
        )
        let state = Self.apply(observations: 200, value: 5, to: Self.freshState(), tuning: tuning)
        #expect(state.persistedScaleFactor >= tuning.clampBandLower - 1e-9)
        #expect(state.persistedScaleFactor <= tuning.clampBandLower + 0.1 + 1e-9)
    }

    // MARK: - One-notch-per-24h rate limit

    @Test("Notch rate limit blocks a second move within 24h")
    func testNotchRateLimit() {
        // 30 samples at 2× seed activates and walks one notch (0.1).
        let activated = Self.apply(observations: 30, value: 90, to: Self.freshState())
        let priorFactor = activated.persistedScaleFactor
        #expect(activated.lastNotchChangeAt != nil)

        // Apply one more observation 1 hour later — well inside the
        // 24-h window. The persisted factor must NOT move.
        let oneHourLater = Self.referenceDate.addingTimeInterval(30 * 60 + 60 * 60)
        let obs = GrantWindowObservation(grantWindowSeconds: 90, observedAt: oneHourLater)
        let (next, result) = AdaptiveDeviceProfileEstimator.apply(observation: obs, to: activated)
        #expect(result.persistedScaleFactorChanged == false)
        #expect(result.blockedByNotchRateLimit == true)
        #expect(next.persistedScaleFactor == priorFactor)
    }

    @Test("Notch rate limit releases after 24h have elapsed")
    func testNotchRateLimitReleases() {
        let activated = Self.apply(observations: 30, value: 90, to: Self.freshState())
        let priorFactor = activated.persistedScaleFactor

        // 25 hours later — past the 24-h window. The persisted factor
        // is free to advance one more notch.
        let stepBeyondWindow = Self.referenceDate.addingTimeInterval(
            30 * 60 + 25 * 60 * 60
        )
        let obs = GrantWindowObservation(grantWindowSeconds: 90, observedAt: stepBeyondWindow)
        let (next, result) = AdaptiveDeviceProfileEstimator.apply(observation: obs, to: activated)
        #expect(result.persistedScaleFactorChanged == true)
        #expect(result.blockedByNotchRateLimit == false)
        #expect(next.persistedScaleFactor > priorFactor)
        #expect(next.persistedScaleFactor <= priorFactor + 0.1 + 1e-9,
                "one notch per window — never more than 0.1 per move")
    }

    @Test("Notch step never overshoots the candidate")
    func testNotchStepClampsToCandidate() {
        // 30 samples at 1.05× seed activates with a tiny target. The
        // notch step (0.1) would overshoot; the estimator must walk
        // exactly to the candidate, not past it.
        let target: Double = 45 * 1.05 // 47.25
        let state = Self.apply(observations: 30, value: target, to: Self.freshState())
        let factor = AdaptiveDeviceProfileEstimator.resolvedScaleFactor(state: state)
        // EWMA after 30 samples at 47.25 → factor ≈ 1.05 (within EWMA tolerance).
        #expect(abs(factor - 1.05) < 0.01)
    }

    // MARK: - Never-zero floor

    @Test("Non-positive observations are dropped (never-zero floor)")
    func testNonPositiveDropped() {
        let state = Self.freshState()
        let zeroObs = GrantWindowObservation(grantWindowSeconds: 0, observedAt: Self.referenceDate)
        let (afterZero, resultZero) = AdaptiveDeviceProfileEstimator.apply(observation: zeroObs, to: state)
        #expect(afterZero == state, "zero observations must not advance state")
        #expect(resultZero.persistedScaleFactorChanged == false)

        let negObs = GrantWindowObservation(grantWindowSeconds: -10, observedAt: Self.referenceDate)
        let (afterNeg, _) = AdaptiveDeviceProfileEstimator.apply(observation: negObs, to: state)
        #expect(afterNeg == state, "negative observations must not advance state")
    }

    @Test("resolvedScaleFactor never returns zero even with a corrupted clamp band")
    func testResolvedScaleFactorNeverZero() {
        let corruptTuning = AdaptiveDeviceProfileTuning(
            ewmaAlpha: 0.2,
            minSamplesForActivation: 30,
            clampBandLower: 0, // simulated corruption
            clampBandUpper: 2.0,
            notchStep: 0.1,
            notchWindowSeconds: 24 * 60 * 60,
            divergenceObservationThreshold: 10
        )
        // Activated state with persistedScaleFactor=0 simulating
        // upstream data corruption.
        var state = Self.freshState()
        state = Self.apply(observations: 30, value: 45, to: state, tuning: corruptTuning)
        state.persistedScaleFactor = 0
        let factor = AdaptiveDeviceProfileEstimator.resolvedScaleFactor(state: state, tuning: corruptTuning)
        #expect(factor > 0, "never-zero floor must hold even with a corrupted clamp lower bound")
    }

    @Test("project(seed:scaledBy:) floors scaled fields at 1, never zero")
    func testProjectFloors() {
        let tinySeed = DeviceClassProfile(
            deviceClass: DeviceClass.iPhone14andOlder.rawValue,
            grantWindowMedianSeconds: 1,
            grantWindowP95Seconds: 1,
            nominalSliceSizeBytes: 1,
            cpuWindowSeconds: 1,
            bytesPerCpuSecond: 1,
            avgShardDurationMs: 1
        )
        let projected = AdaptiveDeviceProfileEstimator.project(seed: tinySeed, scaledBy: 0.001)
        #expect(projected.grantWindowMedianSeconds >= 1)
        #expect(projected.grantWindowP95Seconds >= 1)
        #expect(projected.nominalSliceSizeBytes >= 1)
    }

    // MARK: - Divergence-revert

    @Test("Divergence revert fires after K consecutive clamp-saturated observations")
    func testDivergenceRevertHigh() {
        // Use a small K so we don't have to ship 10+ observations.
        let tuning = AdaptiveDeviceProfileTuning(
            ewmaAlpha: 0.2,
            minSamplesForActivation: 30,
            clampBandLower: 0.5,
            clampBandUpper: 2.0,
            notchStep: 0.1,
            notchWindowSeconds: 0, // disable rate limit so the persisted factor walks
            divergenceObservationThreshold: 3
        )
        // 30 normal observations to activate; then 3 wildly-large
        // observations push the EWMA past the 2.0× clamp and trip
        // the divergence-revert.
        var state = Self.apply(observations: 30, value: 45, to: Self.freshState(), tuning: tuning)
        #expect(state.isActivated(tuning: tuning))

        // 3 huge observations — EWMA jumps WAY past the upper clamp.
        // 1000 s / 45 s seed = 22× → way above 2× upper clamp.
        var divergeStart = Self.referenceDate.addingTimeInterval(60 * 60)
        var sawRevert = false
        for _ in 0..<5 {
            let obs = GrantWindowObservation(grantWindowSeconds: 1000, observedAt: divergeStart)
            let (next, result) = AdaptiveDeviceProfileEstimator.apply(
                observation: obs, to: state, tuning: tuning
            )
            state = next
            if result.didRevertToSeed {
                sawRevert = true
                break
            }
            divergeStart = divergeStart.addingTimeInterval(60)
        }
        #expect(sawRevert, "K consecutive clamp-saturated observations must trip divergence revert")
        #expect(state.lastRevertReason == .divergenceClampSaturation)
        #expect(state.sampleCount == 0, "revert must reset the math state to cold")
        #expect(state.ewmaSeconds == 0)
        #expect(state.persistedScaleFactor == 1.0)
    }

    @Test("Interior observations reset the divergence counter")
    func testDivergenceCounterResetsOnInteriorObservation() {
        // α=1.0 makes the EWMA equal the latest observation with no
        // stickiness, so an "interior" observation immediately pulls
        // the raw candidate back inside the clamp band. With the
        // production α=0.2 the EWMA's geometric memory would keep the
        // candidate above the upper bound even after a single in-band
        // observation, and this test would conflate EWMA stickiness
        // with counter-reset semantics. α=1.0 isolates the counter
        // logic, which is what the test is asserting.
        let tuning = AdaptiveDeviceProfileTuning(
            ewmaAlpha: 1.0,
            minSamplesForActivation: 30,
            clampBandLower: 0.5,
            clampBandUpper: 2.0,
            notchStep: 0.1,
            notchWindowSeconds: 0,
            divergenceObservationThreshold: 3
        )
        var state = Self.apply(observations: 30, value: 45, to: Self.freshState(), tuning: tuning)
        // 2 clamp-saturated observations, then 1 interior, then 2 more
        // saturated. The counter must NOT cross the 3-observation
        // threshold because the interior observation reset it.
        let times = (0..<5).map { Self.referenceDate.addingTimeInterval(Double($0 + 30) * 60) }
        let inputs: [(Double, Date)] = [
            (1000, times[0]), // saturated high (ratio = 22.2)
            (1000, times[1]), // saturated high
            (45,   times[2]), // interior (ratio = 1.0) — resets counter to 0
            (1000, times[3]), // saturated high (counter back to 1)
            (1000, times[4]), // saturated high (counter at 2, still under 3)
        ]
        for (v, t) in inputs {
            let obs = GrantWindowObservation(grantWindowSeconds: v, observedAt: t)
            let (next, result) = AdaptiveDeviceProfileEstimator.apply(
                observation: obs, to: state, tuning: tuning
            )
            state = next
            #expect(result.didRevertToSeed == false, "counter reset must prevent revert")
        }
        #expect(state.lastRevertReason == nil, "no revert means no reason latched")
    }

    // MARK: - project()

    @Test("project preserves non-slice fields verbatim")
    func testProjectPreservesNonSliceFields() {
        let seed = DeviceClassProfile.fallback(for: .iPhone17Pro)
        let scaled = AdaptiveDeviceProfileEstimator.project(seed: seed, scaledBy: 1.5)
        #expect(scaled.cpuWindowSeconds == seed.cpuWindowSeconds, "cpuWindowSeconds must pass through unchanged")
        #expect(scaled.bytesPerCpuSecond == seed.bytesPerCpuSecond, "bytesPerCpuSecond must pass through unchanged")
        #expect(scaled.avgShardDurationMs == seed.avgShardDurationMs, "avgShardDurationMs must pass through unchanged")
        #expect(scaled.deviceClass == seed.deviceClass)
    }

    @Test("project scales the slice-relevant fields proportionally")
    func testProjectScalesSliceFields() {
        let seed = DeviceClassProfile.fallback(for: .iPhone17Pro)
        let scaled = AdaptiveDeviceProfileEstimator.project(seed: seed, scaledBy: 2.0)
        #expect(scaled.grantWindowMedianSeconds == seed.grantWindowMedianSeconds * 2)
        #expect(scaled.grantWindowP95Seconds == seed.grantWindowP95Seconds * 2)
        #expect(scaled.nominalSliceSizeBytes == seed.nominalSliceSizeBytes * 2)
    }

    @Test("project at scale 1.0 returns a profile equal to the seed (modulo deviceClass passthrough)")
    func testProjectIdentity() {
        let seed = DeviceClassProfile.fallback(for: .iPhone16Pro)
        let scaled = AdaptiveDeviceProfileEstimator.project(seed: seed, scaledBy: 1.0)
        #expect(scaled == seed, "scale 1.0 must be the identity transform")
    }

    // MARK: - State init defaults

    @Test("Fresh state initializes to seed-equivalent values")
    func testFreshStateDefaults() {
        let state = Self.freshState(seedSeconds: 45)
        #expect(state.sampleCount == 0)
        #expect(state.welfordMean == 0)
        #expect(state.welfordM2 == 0)
        #expect(state.ewmaSeconds == 0)
        #expect(state.persistedScaleFactor == 1.0)
        #expect(state.lastNotchChangeAt == nil)
        #expect(state.consecutiveClampedObservations == 0)
        #expect(state.lastRevertReason == nil)
        #expect(state.schemaVersion == AdaptiveDeviceProfileState.currentSchemaVersion)
        #expect(state.seedGrantWindowSeconds == 45)
        let factor = AdaptiveDeviceProfileEstimator.resolvedScaleFactor(state: state)
        #expect(factor == 1.0, "fresh state must return seed (factor 1.0) — byte-identical to flag-off")
    }
}
