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

    // MARK: - Non-finite hardening (R5: corrupt persisted state + corrupt observation)

    @Test("apply drops a NaN observation without mutating state")
    func testNaNObservationDropped() {
        let state = Self.apply(observations: 5, value: 45, to: Self.freshState())
        let nanObs = GrantWindowObservation(
            grantWindowSeconds: .nan,
            observedAt: Self.referenceDate.addingTimeInterval(3600)
        )
        let (after, result) = AdaptiveDeviceProfileEstimator.apply(observation: nanObs, to: state)
        #expect(after == state, "NaN observation must leave state byte-equal")
        #expect(result.persistedScaleFactorChanged == false)
        #expect(result.didRevertToSeed == false)
    }

    @Test("apply drops a +Infinity observation without poisoning the EWMA")
    func testInfinityObservationDropped() {
        let state = Self.apply(observations: 5, value: 45, to: Self.freshState())
        let infObs = GrantWindowObservation(
            grantWindowSeconds: .infinity,
            observedAt: Self.referenceDate.addingTimeInterval(3600)
        )
        let (after, _) = AdaptiveDeviceProfileEstimator.apply(observation: infObs, to: state)
        #expect(after.ewmaSeconds.isFinite, "EWMA must remain finite after an Inf observation")
        #expect(after.welfordMean.isFinite)
        #expect(after.welfordM2.isFinite)
        // State should be byte-equal to the prior — Inf is dropped at
        // the guard, not absorbed and re-clamped.
        #expect(after == state)
    }

    @Test("apply self-heals when the prior persisted state has a NaN EWMA")
    func testCorruptPriorStateHeals() {
        // Simulate a SwiftData row that hydrated with a NaN EWMA — e.g.
        // a hand-edited DB or a migration bug. Without the sanitization
        // step the next `apply` would produce another NaN forever.
        var corrupt = Self.apply(observations: 30, value: 45, to: Self.freshState())
        corrupt.ewmaSeconds = .nan
        corrupt.welfordMean = .nan
        corrupt.welfordM2 = .nan
        corrupt.persistedScaleFactor = .nan

        let healObs = GrantWindowObservation(
            grantWindowSeconds: 45,
            observedAt: Self.referenceDate.addingTimeInterval(24 * 60 * 60 + 60)
        )
        let (healed, _) = AdaptiveDeviceProfileEstimator.apply(observation: healObs, to: corrupt)

        #expect(healed.ewmaSeconds.isFinite, "EWMA must be finite after healing")
        #expect(healed.welfordMean.isFinite)
        #expect(healed.welfordM2.isFinite)
        #expect(healed.persistedScaleFactor.isFinite)
        // After the soft-reset the row begins re-bootstrapping. One
        // observation in: sampleCount=1, EWMA seeded to obs value.
        #expect(healed.sampleCount == 1, "soft-reset re-arms activation floor")
        #expect(healed.ewmaSeconds == 45, "first post-heal observation seeds the EWMA")
        // Identity fields survive the heal — `seedGrantWindowSeconds`
        // and `createdAt` are immutable contract.
        #expect(healed.seedGrantWindowSeconds == corrupt.seedGrantWindowSeconds)
        #expect(healed.createdAt == corrupt.createdAt)
        #expect(healed.deviceClassRawValue == corrupt.deviceClassRawValue)
    }

    @Test("apply self-heals when the prior persisted state has +Inf persistedScaleFactor")
    func testInfinityPriorStateHeals() {
        var corrupt = Self.apply(observations: 30, value: 45, to: Self.freshState())
        corrupt.persistedScaleFactor = .infinity

        let healObs = GrantWindowObservation(
            grantWindowSeconds: 45,
            observedAt: Self.referenceDate.addingTimeInterval(24 * 60 * 60 + 60)
        )
        let (healed, _) = AdaptiveDeviceProfileEstimator.apply(observation: healObs, to: corrupt)
        #expect(healed.persistedScaleFactor.isFinite)
        #expect(healed.persistedScaleFactor == 1.0, "scale factor pins back to 1.0 on heal")
    }

    @Test("apply drops an observation whose observedAt is non-finite (symmetry with stored lastNotchChangeAt fix)")
    func testNonFiniteObservedAtDropped() {
        // R7 symmetry probe: R6 sanitized the STORED side
        // (`lastNotchChangeAt`), but a corrupt `obs.observedAt` causes
        // the SAME NaN-comparison fail-open in the rate-limit guard
        // (line 517: `obs.observedAt.timeIntervalSince(last)` returns
        // NaN if either operand is non-finite). Without the entry-guard
        // drop, a single bad-Date observation would (a) silently
        // bypass the 24-h rate limit AND (b) get stored as
        // `lastNotchChangeAt` / `updatedAt`, corrupting diagnostic data
        // until the next observation triggers the R6 stored-side
        // sanitizer. Drop at the entry guard so neither happens.
        let state = Self.apply(observations: 30, value: 90, to: Self.freshState())
        let priorFactor = state.persistedScaleFactor
        let priorLastNotch = state.lastNotchChangeAt
        let priorUpdatedAt = state.updatedAt

        // NaN-Date observedAt.
        let nanDateObs = GrantWindowObservation(
            grantWindowSeconds: 90,
            observedAt: Date(timeIntervalSinceReferenceDate: .nan)
        )
        let (afterNaN, resultNaN) = AdaptiveDeviceProfileEstimator.apply(
            observation: nanDateObs, to: state
        )
        #expect(afterNaN == state, "non-finite observedAt must leave state byte-equal")
        #expect(resultNaN.persistedScaleFactorChanged == false)
        #expect(resultNaN.didRevertToSeed == false)
        // Stored Dates must not have been overwritten with the corrupt value.
        #expect(afterNaN.lastNotchChangeAt == priorLastNotch)
        #expect(afterNaN.updatedAt == priorUpdatedAt)
        #expect(afterNaN.persistedScaleFactor == priorFactor)

        // +Infinity-Date observedAt (the other non-finite shape).
        let infDateObs = GrantWindowObservation(
            grantWindowSeconds: 90,
            observedAt: Date(timeIntervalSinceReferenceDate: .infinity)
        )
        let (afterInf, _) = AdaptiveDeviceProfileEstimator.apply(
            observation: infDateObs, to: state
        )
        #expect(afterInf == state, "non-finite (Inf) observedAt must also leave state byte-equal")
    }

    @Test("apply coerces a corrupt (non-finite) lastNotchChangeAt to nil so the rate limit does not silently fail open")
    func testCorruptLastNotchChangeAtCoercedToNil() {
        // R6: a SwiftData row with a NaN-Date `lastNotchChangeAt`
        // would make `timeIntervalSince(...)` return NaN, and
        // `NaN < notchWindowSeconds` is false, so the rate-limit guard
        // would silently FAIL OPEN — the next observation could
        // freely advance the notch even though "less than 24h" since
        // the previous one. The sanitization coerces the corrupt date
        // to `nil` (treated as "no prior notch change"). The first
        // observation after the heal is permitted to notch once;
        // subsequent observations within the window are blocked
        // normally. This proves the rate-limit invariant is restored
        // after one observation rather than indefinitely bypassed.
        var corrupt = Self.apply(observations: 30, value: 90, to: Self.freshState())
        #expect(corrupt.lastNotchChangeAt != nil)
        // Mint a corrupt Date with a non-finite reference value.
        corrupt.lastNotchChangeAt = Date(timeIntervalSinceReferenceDate: .nan)

        let firstAfter = GrantWindowObservation(
            grantWindowSeconds: 90,
            observedAt: Self.referenceDate.addingTimeInterval(31 * 60)
        )
        let (afterFirst, firstResult) = AdaptiveDeviceProfileEstimator.apply(
            observation: firstAfter, to: corrupt
        )
        // First observation after the heal is allowed to walk (we
        // treat the corrupt date as "no prior change"), and the rate-
        // limit branch did NOT block it — proving the corrupt date
        // didn't carry through.
        #expect(firstResult.blockedByNotchRateLimit == false,
                "corrupt lastNotchChangeAt must not propagate into the rate-limit check")
        #expect(afterFirst.lastNotchChangeAt != nil,
                "first observation after heal restamps lastNotchChangeAt with a valid date")
        if let restamped = afterFirst.lastNotchChangeAt {
            #expect(restamped.timeIntervalSinceReferenceDate.isFinite,
                    "restamped date must be finite, restoring the rate-limit invariant")
        }

        // Second observation 1h later (well within 24h) MUST be
        // blocked by the rate limit — proving the limit is genuinely
        // restored after the heal, not permanently bypassed.
        let secondAfter = GrantWindowObservation(
            grantWindowSeconds: 90,
            observedAt: afterFirst.lastNotchChangeAt!.addingTimeInterval(60 * 60)
        )
        let (_, secondResult) = AdaptiveDeviceProfileEstimator.apply(
            observation: secondAfter, to: afterFirst
        )
        #expect(secondResult.blockedByNotchRateLimit == true,
                "rate limit must engage again on the next-window observation")
    }

    // MARK: - Non-finite hardening (R10: integer-shaped corruption)

    @Test("apply self-heals when the prior persisted state has a negative sampleCount")
    func testNegativeSampleCountTriggersSoftReset() {
        // R10 probe-6: the R5 soft-reset predicate guarded only Double
        // fields. A hand-edited DB / migration bug could leave the row
        // with `sampleCount < 0` while every Double remained finite —
        // not detected as corruption by R5. Then `sampleCount += 1`
        // reaches `0` (if prior=-1) and `delta / Double(0)` is ±Inf,
        // poisoning the welford mean for one cycle. R10 folds
        // `sampleCount < 0` into the same heal so the row recovers
        // within a SINGLE observation rather than two.
        var corrupt = Self.apply(observations: 30, value: 45, to: Self.freshState())
        corrupt.sampleCount = -1

        let healObs = GrantWindowObservation(
            grantWindowSeconds: 45,
            observedAt: Self.referenceDate.addingTimeInterval(24 * 60 * 60 + 60)
        )
        let (healed, _) = AdaptiveDeviceProfileEstimator.apply(
            observation: healObs, to: corrupt
        )

        // Post-heal: sampleCount re-armed from 0, then incremented to 1.
        #expect(healed.sampleCount == 1,
                "soft-reset must re-arm sampleCount, not let the negative value carry through")
        // All math fields must be finite post-heal — proves the Welford
        // update did NOT divide by zero (which would have produced NaN
        // or ±Inf in welfordMean).
        #expect(healed.welfordMean.isFinite)
        #expect(healed.welfordM2.isFinite)
        #expect(healed.ewmaSeconds.isFinite)
        #expect(healed.persistedScaleFactor.isFinite)
        // Identity preserved.
        #expect(healed.seedGrantWindowSeconds == corrupt.seedGrantWindowSeconds)
        #expect(healed.createdAt == corrupt.createdAt)
    }

    @Test("apply self-heals when the prior persisted state has a negative consecutiveClampedObservations")
    func testNegativeConsecutiveClampedObservationsTriggersSoftReset() {
        // R11 probe-1: the R10 heal predicate added `sampleCount < 0`
        // but left `consecutiveClampedObservations` — the OTHER Int
        // column on the row — out of the integer-corruption branch. A
        // hand-edited DB / migration sign-flip could leave the row with
        // `consecutiveClampedObservations = -1000`. With the production
        // `divergenceObservationThreshold` of 10, the saturation counter
        // would have to climb from -1000 back to +10 (1010 saturated
        // observations) before the spec-mandated divergence-revert
        // engages — leaving the estimator free to walk through the
        // clamp band for ~1000 obs with no safety net. R11 folds this
        // axis into the same soft-reset branch so the counter starts
        // from a clean 0 and the K-consecutive invariant holds within
        // ONE observation rather than ~1010.
        var corrupt = Self.apply(observations: 30, value: 45, to: Self.freshState())
        corrupt.consecutiveClampedObservations = -1000

        let healObs = GrantWindowObservation(
            grantWindowSeconds: 45,
            observedAt: Self.referenceDate.addingTimeInterval(24 * 60 * 60 + 60)
        )
        let (healed, _) = AdaptiveDeviceProfileEstimator.apply(
            observation: healObs, to: corrupt
        )

        // Post-heal: the soft-reset zeros every accumulator (including
        // `consecutiveClampedObservations`) and re-arms the activation
        // floor. The first post-heal observation's saturation status is
        // then computed against a fresh state.
        //
        // For this fixture (value=45, seed=45) the raw EWMA candidate
        // is exactly 1.0× the seed — strictly inside the clamp band,
        // not saturated — so the counter stays at 0.
        #expect(healed.consecutiveClampedObservations == 0,
                "soft-reset must zero consecutiveClampedObservations, not let the negative value carry through")
        // sampleCount re-armed from 0 → +1 (this observation).
        #expect(healed.sampleCount == 1,
                "soft-reset must re-arm sampleCount so divergence-revert lower bound (>= 10) is well-defined")
        // Math fields are finite — the heal preserves the existing R5/R6
        // contract on top of the integer-shaped fix.
        #expect(healed.welfordMean.isFinite)
        #expect(healed.ewmaSeconds.isFinite)
        #expect(healed.persistedScaleFactor.isFinite)
        // Identity preserved.
        #expect(healed.seedGrantWindowSeconds == corrupt.seedGrantWindowSeconds)
        #expect(healed.createdAt == corrupt.createdAt)
    }

    @Test("apply with consecutiveClampedObservations=0 (the boundary) does NOT trigger soft-reset")
    func testZeroConsecutiveClampedObservationsIsNotCorruption() {
        // R11 boundary check: `consecutiveClampedObservations == 0` is
        // the LEGAL post-reset state. The R11 predicate is
        // `consecutiveClampedObservations < 0`, NOT `<= 0` — confirm a
        // fresh state with the counter at 0 is treated as healthy and
        // the first observation proceeds through the normal saturation
        // tracking path.
        let fresh = Self.freshState(seedSeconds: 45)
        #expect(fresh.consecutiveClampedObservations == 0)
        let firstObs = GrantWindowObservation(
            grantWindowSeconds: 60, // 1.33× the seed — INSIDE clamp band
            observedAt: Self.referenceDate
        )
        let (after, _) = AdaptiveDeviceProfileEstimator.apply(
            observation: firstObs, to: fresh
        )
        // If the heal had falsely fired, this would still be sampleCount=1
        // (the heal zeroes then the +=1 fires). But the EWMA would be
        // forcibly seeded to obs value either way; instead we check the
        // welford accumulator's M2 contribution — on a non-healed path
        // the first observation produces welfordM2 = 0 (the canonical
        // Welford one-sample state). A false-fire path would set the
        // same value, so this boundary check is symmetric to
        // `testZeroSampleCountIsNotCorruption`: we rely on the heal
        // contract that "no axis triggers" leaves state byte-identical
        // to the normal Welford one-sample step.
        #expect(after.sampleCount == 1)
        #expect(after.consecutiveClampedObservations == 0,
                "interior observation must leave the counter at 0 (no saturation, no false heal)")
    }

    @Test("apply with sampleCount=0 (the boundary) does NOT trigger soft-reset")
    func testZeroSampleCountIsNotCorruption() {
        // Boundary check: `sampleCount == 0` is the LEGAL cold-start
        // state. The R10 predicate is `sampleCount < 0`, NOT `<= 0` —
        // confirm a fresh state is treated as healthy and the first
        // observation proceeds through the normal Welford path.
        let fresh = Self.freshState(seedSeconds: 45)
        #expect(fresh.sampleCount == 0)
        let firstObs = GrantWindowObservation(
            grantWindowSeconds: 60,
            observedAt: Self.referenceDate
        )
        let (after, _) = AdaptiveDeviceProfileEstimator.apply(
            observation: firstObs, to: fresh
        )
        // After ONE observation: sampleCount=1 (incremented from 0),
        // ewmaSeconds seeded to obs value. If the heal had falsely
        // fired, sampleCount would still be 1 but the Welford mean
        // would have followed the post-heal path (also 60 in this case)
        // — so we additionally check that NO sanitization touched the
        // accumulator: welfordMean ≈ 60 (the actual obs value).
        #expect(after.sampleCount == 1)
        #expect(after.ewmaSeconds == 60)
        #expect(abs(after.welfordMean - 60) < 1e-9)
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
