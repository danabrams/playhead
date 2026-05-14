// AdaptiveDeviceProfileEstimator.swift
// playhead-beh3 (Phase 3 deliverable 5) — adaptive Welford+EWMA
// estimator for the per-device-class grant-window / slice-sizing
// configuration originally seeded by playhead-dh9b
// (`DeviceClassProfile.fallback(for:)`).
//
// Scope: this file is the *pure math + invariants* substrate. It owns
// the running state for one device class and the rules that mutate it:
//
//   * Welford-style online mean + running variance (M2).
//   * EWMA smoothing with α = 0.2 layered on top of the Welford mean
//     (the EWMA value, NOT the Welford mean, is the estimator output —
//     EWMA reacts faster to environmental shifts while Welford keeps
//     a faithful long-run mean for divergence diagnosis).
//   * Activation floor: until `minSamplesForActivation` (30)
//     observations have accumulated the estimator returns the SEED
//     value verbatim. Behind-flag-off the seed is always returned.
//   * Clamp band: persisted scale factor pinned to [0.5×, 2.0×] of the
//     seed grant-window.
//   * One-notch-per-24h rate limit: the persisted scale factor cannot
//     move by more than `notchStep` (0.1× of seed) in any rolling 24-h
//     wall-clock window. "Notch" = one 0.1-step of the clamp band; the
//     band carries 16 notches end-to-end (0.5, 0.6, …, 2.0).
//   * Never-zero floor: the persisted scale factor is always > 0 by
//     construction (clamped to ≥ 0.5×); resolved slice sizes never
//     return a value ≤ 0.
//   * Divergence revert: if the *raw* EWMA value would saturate the
//     clamp band for `divergenceObservationThreshold` (10) consecutive
//     observations, the estimator self-reverts to the seed and surfaces
//     the reversion in diagnostics.
//
// Persistence is *not* in this file — see `LearnedDeviceProfile`
// (SwiftData @Model) and `LearnedDeviceProfileStore` for the storage
// surface. Keeping the math here in a free `enum`/`struct` substrate
// is intentional so the invariants are unit-testable without spinning
// up a SwiftData container.

import Foundation

// MARK: - Tuning constants

/// All knobs for the adaptive estimator. Lifted into a `Sendable` value
/// type so tests can construct one with tighter values (e.g. K=3 for a
/// divergence-revert assertion that doesn't need to fire 10 observations
/// of clamp saturation). Production always uses ``standard``.
struct AdaptiveDeviceProfileTuning: Sendable, Equatable {
    /// EWMA smoothing factor. Bead spec locks this to 0.2 per
    /// observation.
    let ewmaAlpha: Double

    /// Minimum sample count before the estimator output deviates from
    /// the seed. Bead spec locks this to 30.
    let minSamplesForActivation: Int

    /// Closed clamp band as `[lower, upper]` multipliers of the seed
    /// grant-window. Bead spec locks this to [0.5×, 2.0×].
    let clampBandLower: Double
    let clampBandUpper: Double

    /// Discretization step for the "one-notch per 24h" rate limit.
    /// `notchStep × seedSeconds` is the maximum the persisted EWMA may
    /// move in either direction within `notchWindowSeconds`. 0.1
    /// produces 16 notches across the [0.5×, 2.0×] band, a sensible
    /// trade-off between reactivity and stability.
    let notchStep: Double

    /// Rolling window for the notch rate limit. Defaults to 24h
    /// (86400 s); tests pass a smaller value so the time-warped clock
    /// does not have to advance a full day.
    let notchWindowSeconds: TimeInterval

    /// Number of consecutive observations whose RAW EWMA candidate
    /// saturates one end of the clamp band before the estimator
    /// self-reverts to the seed. Bead spec proposes a "sensible K"; we
    /// use 10 so a brief burst of noisy outliers does not trip the
    /// revert.
    let divergenceObservationThreshold: Int

    static let standard = AdaptiveDeviceProfileTuning(
        ewmaAlpha: 0.2,
        minSamplesForActivation: 30,
        clampBandLower: 0.5,
        clampBandUpper: 2.0,
        notchStep: 0.1,
        notchWindowSeconds: 24 * 60 * 60,
        divergenceObservationThreshold: 10
    )
}

// MARK: - Observation

/// One observed grant-window outcome. The wall-clock `observedAt` is
/// threaded explicitly so tests can drive the notch rate-limit with a
/// synthetic clock.
///
/// The estimator only consumes the grant-window duration (seconds). The
/// observation type is a struct so future fields (e.g. outcome class)
/// can be added without re-fitting every test call site.
struct GrantWindowObservation: Sendable, Equatable {
    /// Effective grant-window length in seconds. Always > 0; the caller
    /// is responsible for filtering out negative-duration / aborted
    /// runs before recording.
    let grantWindowSeconds: Double

    /// Wall-clock timestamp the observation completed. Drives the
    /// "one notch per 24h" rate limit.
    let observedAt: Date

    init(grantWindowSeconds: Double, observedAt: Date) {
        self.grantWindowSeconds = grantWindowSeconds
        self.observedAt = observedAt
    }
}

// MARK: - State

/// Persisted state for one device class, as consumed by the math layer.
///
/// This mirrors the columns on the SwiftData `LearnedDeviceProfile`
/// `@Model`, but is kept as a plain `Sendable` value type so the math
/// can be exercised end-to-end without a `ModelContext`. The
/// SwiftData class projects to/from this struct via
/// `LearnedDeviceProfile.snapshot()` / `LearnedDeviceProfile.apply(_:)`.
struct AdaptiveDeviceProfileState: Sendable, Equatable {

    /// Schema-stamp for the persisted blob. Bumped on any breaking
    /// shape change (new required field, semantic field-meaning shift).
    /// V1 ships with this bead.
    static let currentSchemaVersion: Int = 1

    /// Stable identifier for the device class. Matches
    /// `DeviceClass.rawValue` so the table can be keyed without an
    /// extra enum mapping.
    let deviceClassRawValue: String

    /// Persisted seed grant-window in seconds. Captured at the first
    /// observation so a future ship of a different seed table does
    /// not silently shift the EWMA's anchor point. Equivalent to
    /// `Double(seedProfile.grantWindowMedianSeconds)` for V1.
    let seedGrantWindowSeconds: Double

    /// Welford running mean.
    var welfordMean: Double

    /// Welford running M2 (sum of squared deltas from the running
    /// mean). Variance is `welfordM2 / sampleCount` for a population
    /// variance; consumers that want the sample variance divide by
    /// `sampleCount - 1`.
    var welfordM2: Double

    /// Total observations recorded so far.
    var sampleCount: Int

    /// Current EWMA value, in seconds. Initialized lazily to the first
    /// observation; subsequent observations apply
    /// `ewma = α * obs + (1 - α) * ewma`.
    var ewmaSeconds: Double

    /// Persisted (notch-rate-limited) scale factor. The seed * this
    /// factor is what consumers see when the estimator is activated.
    /// Clamped to [clampBandLower, clampBandUpper]. Initial value 1.0.
    var persistedScaleFactor: Double

    /// Last time the persisted scale factor was advanced toward the
    /// raw EWMA candidate. Drives the 24-h notch rate limit. `nil`
    /// until the first activation-eligible move.
    var lastNotchChangeAt: Date?

    /// Running count of consecutive observations whose RAW EWMA
    /// candidate saturated one end of the clamp band. Reset to 0 on
    /// the first interior observation. Triggers a seed revert at
    /// `divergenceObservationThreshold`.
    var consecutiveClampedObservations: Int

    /// Latched diagnostic: most recent reason the estimator reverted
    /// to the seed (or nil if it has not). Surfaced in the diagnostics
    /// bundle so support can attribute a "stuck-at-seed" observation.
    var lastRevertReason: AdaptiveDeviceProfileRevertReason?

    /// Wall-clock the row was first inserted. Useful for diagnostics.
    let createdAt: Date

    /// Wall-clock the row was last touched (any field).
    var updatedAt: Date

    /// Schema version stamp persisted on the row. Used to gate future
    /// breaking-shape migrations.
    let schemaVersion: Int

    /// Construct a brand-new state with the bead-spec defaults.
    /// `seedGrantWindowSeconds` is captured here so subsequent ships
    /// of a different seed value cannot retroactively shift the
    /// anchor.
    init(
        deviceClassRawValue: String,
        seedGrantWindowSeconds: Double,
        createdAt: Date,
        schemaVersion: Int = AdaptiveDeviceProfileState.currentSchemaVersion
    ) {
        self.deviceClassRawValue = deviceClassRawValue
        self.seedGrantWindowSeconds = seedGrantWindowSeconds
        self.welfordMean = 0
        self.welfordM2 = 0
        self.sampleCount = 0
        self.ewmaSeconds = 0
        self.persistedScaleFactor = 1.0
        self.lastNotchChangeAt = nil
        self.consecutiveClampedObservations = 0
        self.lastRevertReason = nil
        self.createdAt = createdAt
        self.updatedAt = createdAt
        self.schemaVersion = schemaVersion
    }

    /// Memberwise initializer used by the persistence layer to rebuild
    /// the state from a stored row. Not for everyday callers.
    init(
        deviceClassRawValue: String,
        seedGrantWindowSeconds: Double,
        welfordMean: Double,
        welfordM2: Double,
        sampleCount: Int,
        ewmaSeconds: Double,
        persistedScaleFactor: Double,
        lastNotchChangeAt: Date?,
        consecutiveClampedObservations: Int,
        lastRevertReason: AdaptiveDeviceProfileRevertReason?,
        createdAt: Date,
        updatedAt: Date,
        schemaVersion: Int
    ) {
        self.deviceClassRawValue = deviceClassRawValue
        self.seedGrantWindowSeconds = seedGrantWindowSeconds
        self.welfordMean = welfordMean
        self.welfordM2 = welfordM2
        self.sampleCount = sampleCount
        self.ewmaSeconds = ewmaSeconds
        self.persistedScaleFactor = persistedScaleFactor
        self.lastNotchChangeAt = lastNotchChangeAt
        self.consecutiveClampedObservations = consecutiveClampedObservations
        self.lastRevertReason = lastRevertReason
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.schemaVersion = schemaVersion
    }

    /// `true` once the estimator has observed at least
    /// `minSamplesForActivation` rows. Below this point consumers
    /// MUST use the seed grant-window verbatim.
    func isActivated(tuning: AdaptiveDeviceProfileTuning) -> Bool {
        sampleCount >= tuning.minSamplesForActivation
    }

    /// Population variance derived from the Welford accumulator.
    /// Returns 0 when fewer than two observations have been recorded
    /// (the running variance is undefined with one sample; we report
    /// 0 rather than NaN so callers don't have to special-case the
    /// cold-start path).
    var welfordVariance: Double {
        guard sampleCount >= 2 else { return 0 }
        return welfordM2 / Double(sampleCount)
    }
}

// MARK: - Revert reasons

/// Closed enum of reasons the estimator may have self-reverted to the
/// seed. Surfaced through the diagnostics bundle so support engineers
/// can distinguish "never activated" from "reverted after K clamp-
/// saturated observations". `rawValue` is the wire string emitted in
/// JSON.
enum AdaptiveDeviceProfileRevertReason: String, Codable, Sendable, Equatable, CaseIterable {
    /// EWMA candidate saturated one end of the clamp band for
    /// `divergenceObservationThreshold` consecutive observations.
    /// Estimator state was reset to "cold" (sample count zeroed, EWMA
    /// cleared) so the next 30 samples re-bootstrap from the seed
    /// before any adjustment can resume.
    case divergenceClampSaturation = "divergence_clamp_saturation"
}

// MARK: - Result of one observation

/// Side-effects of `AdaptiveDeviceProfileEstimator.apply(...)` exposed
/// so tests can assert which branch fired without scraping the state
/// diff manually. Production callers can ignore the result.
struct AdaptiveDeviceProfileApplyResult: Sendable, Equatable {
    /// `true` if the persisted scale factor actually changed this
    /// observation (after the notch + clamp + rate-limit rules).
    let persistedScaleFactorChanged: Bool

    /// `true` if this observation tripped the divergence-revert path.
    /// When set, the caller may want to log a diagnostic event; the
    /// estimator already reset the state and stamped `lastRevertReason`.
    let didRevertToSeed: Bool

    /// `true` if the rate limiter blocked the notch advance this
    /// observation. Useful for tests; production ignores.
    let blockedByNotchRateLimit: Bool

    /// `true` if the RAW EWMA candidate saturated the clamp band on
    /// this observation. Useful for tests; production ignores.
    let clampSaturatedThisObservation: Bool
}

// MARK: - Estimator

/// Pure math for the adaptive estimator. Free `enum` of statics so the
/// invariants are unit-testable without owning any instance state.
///
/// All mutations are explicit: callers pass in the prior state and the
/// observation, and receive back a new state. Persistence (SwiftData)
/// owns the read-modify-write race.
enum AdaptiveDeviceProfileEstimator {

    /// Apply a single grant-window observation to the prior state.
    /// Returns the updated state and a flag tuple describing what
    /// fired this observation (see `AdaptiveDeviceProfileApplyResult`).
    ///
    /// Semantics:
    ///   1. Validate `obs.grantWindowSeconds` is finite and > 0.
    ///      Non-positive, NaN, and infinite observations are dropped
    ///      (the never-zero floor + the finite-arithmetic invariant);
    ///      the state is returned unchanged with all flags false.
    ///      This protects against caller bugs that pass `0`, negative,
    ///      NaN, or `.infinity` to a math layer that would otherwise
    ///      poison the running EWMA / Welford accumulator forever.
    ///   2. Sanitize the prior state's math fields if a corrupt SwiftData
    ///      row delivered a non-finite `welfordMean`, `welfordM2`,
    ///      `ewmaSeconds`, or `persistedScaleFactor`. Without this step
    ///      one NaN/Inf in storage would propagate through every
    ///      subsequent observation forever (NaN math is sticky). We
    ///      heal by zeroing the math accumulators and pinning the
    ///      scale factor back to 1.0 — equivalent to a soft cold-start
    ///      that preserves identity fields (`createdAt`, seed, device
    ///      class) so diagnostics still attribute the row correctly.
    ///   3. Welford update: increment sampleCount, refresh mean,
    ///      accumulate M2.
    ///   4. EWMA update: on the very first observation, seed EWMA to
    ///      the observation value (avoids the cold-start bias that
    ///      `α * obs + (1-α) * 0` would produce); otherwise apply
    ///      the standard formula.
    ///   5. Clamp candidate: compute `candidate = ewma / seed` and
    ///      track whether it saturated one end of the clamp band for
    ///      the divergence-revert counter.
    ///   6. Activation gate: if sampleCount < activation floor, return
    ///      with the EWMA + Welford updated but the persisted scale
    ///      factor unchanged. This keeps the cold-start path
    ///      byte-identical to the seed.
    ///   7. Rate limit: cap the move toward the candidate at
    ///      ±notchStep from the persisted scale factor, and refuse
    ///      to advance at all if `lastNotchChangeAt` is within
    ///      `notchWindowSeconds` of `obs.observedAt`.
    ///   8. Divergence-revert: if the run-of-clamp-saturated
    ///      observations hits `divergenceObservationThreshold`,
    ///      reset state to "cold" (sample count zeroed, EWMA cleared,
    ///      scale factor pinned back to 1.0), stamp the revert reason,
    ///      and flag `didRevertToSeed`.
    ///   9. updatedAt is always advanced to `obs.observedAt`.
    static func apply(
        observation obs: GrantWindowObservation,
        to state: AdaptiveDeviceProfileState,
        tuning: AdaptiveDeviceProfileTuning = .standard
    ) -> (state: AdaptiveDeviceProfileState, result: AdaptiveDeviceProfileApplyResult) {

        // (1) Drop non-finite or non-positive durations defensively.
        // Never-zero floor is the strongest invariant in the spec, and
        // a NaN/Inf observation would poison the EWMA/Welford
        // accumulators for every subsequent observation (NaN-stickiness
        // through `α * obs + (1-α) * ewma`). `Double.isFinite` covers
        // both NaN and ±Inf; the `> 0` guard then catches genuine zero
        // and negative-duration cases (e.g. NTP step backwards).
        guard obs.grantWindowSeconds.isFinite,
              obs.grantWindowSeconds > 0 else {
            return (state, AdaptiveDeviceProfileApplyResult(
                persistedScaleFactorChanged: false,
                didRevertToSeed: false,
                blockedByNotchRateLimit: false,
                clampSaturatedThisObservation: false
            ))
        }
        // (1b) Drop observations whose `observedAt` is non-finite.
        // R7 symmetry probe: R6 sanitized the STORED `lastNotchChangeAt`,
        // but `obs.observedAt` itself is the OTHER side of the same
        // comparison (`obs.observedAt.timeIntervalSince(last)` further
        // down). If the caller hands us a `Date(timeIntervalSinceReferenceDate:
        // .nan)` — pathological but representable — the rate-limit guard
        // returns NaN from the subtraction and `NaN < notchWindowSeconds`
        // is false, so the 24-h limit silently fails open (identical bug
        // pattern to the R6 stored-side fix). The corrupt date would
        // ALSO get stored as the new `lastNotchChangeAt` / `updatedAt`,
        // corrupting diagnostic data and forcing the next-observation
        // soft-reset path to clean it up. Dropping at the entry guard
        // keeps both invariants (rate-limit closed, stored Dates finite)
        // intact in one place. The production write seam guards against
        // this too (its `grantWindowSeconds.isFinite` already filters a
        // non-finite `nowDate` because the duration arithmetic
        // propagates NaN), but library code must defend independently —
        // test/future callers that construct `GrantWindowObservation`
        // directly do not go through the write seam.
        guard obs.observedAt.timeIntervalSinceReferenceDate.isFinite else {
            return (state, AdaptiveDeviceProfileApplyResult(
                persistedScaleFactorChanged: false,
                didRevertToSeed: false,
                blockedByNotchRateLimit: false,
                clampSaturatedThisObservation: false
            ))
        }

        var next = state

        // (2) Sanitize a corrupt prior state. SwiftData persists raw
        // doubles, so a row with a NaN/Inf field (storage corruption,
        // a hand-edited DB, a future migration bug) would otherwise
        // make `apply` permanently NaN-stuck — every subsequent update
        // produces NaN because NaN is absorbing under +/*. Healing
        // here zeroes the accumulators and pins the scale factor back
        // to 1.0; the consumer surface (`resolvedScaleFactor`) was
        // already NaN-safe by argument ordering of its `min/max`
        // clamps, but the persisted state itself never self-healed
        // until this step. Sample count is zeroed too so the
        // activation floor re-armed (the EWMA needs fresh observations
        // to converge — replaying old observations is not possible).
        //
        // R10 sampleCount addition: Int columns do not have a non-finite
        // bit pattern, but a hand-edited DB / migration bug / unfortunate
        // sign-flip could leave the row with `sampleCount < 0`. The R5
        // predicate guarded only the Double fields, so a negative
        // sampleCount would survive into the Welford update (`sampleCount
        // += 1` reaches `0`, then `delta / Double(0)` is ±Inf and the
        // running mean is poisoned for one cycle until the next apply
        // triggers the R5 heal). Treat `sampleCount < 0` as the integer-
        // shaped cousin of "non-finite math": fold it into the same
        // heal branch so the row recovers within ONE observation rather
        // than two, AND so the store-layer R8 log fires on this axis too
        // (the predicate in `recordObservation` mirrors this list).
        //
        // R11 consecutiveClampedObservations addition: the OTHER Int
        // column on the row. A negative value here does not poison the
        // Welford math directly (no division), but it materially weakens
        // the divergence-revert invariant: with `consecutiveClampedObservations
        // = -1000` (hand-edited DB / migration sign-flip), 1010 saturating
        // observations have to land in a row before the
        // `>= divergenceObservationThreshold (10)` revert fires, so the
        // estimator walks freely through the clamp band for ~1000 obs
        // before the spec-mandated safety engages. This is the same
        // integer-shaped corruption pathology as `sampleCount < 0` — fold
        // it into the same heal branch so the saturation counter starts
        // from a clean 0 and the K-consecutive invariant holds. The
        // store-layer R8 log predicate mirrors this addition.
        if !next.welfordMean.isFinite
            || !next.welfordM2.isFinite
            || !next.ewmaSeconds.isFinite
            || !next.persistedScaleFactor.isFinite
            || next.sampleCount < 0
            || next.consecutiveClampedObservations < 0 {
            next.welfordMean = 0
            next.welfordM2 = 0
            next.sampleCount = 0
            next.ewmaSeconds = 0
            next.persistedScaleFactor = 1.0
            next.consecutiveClampedObservations = 0
        }

        // (2b) Sanitize `lastNotchChangeAt` independently of the math
        // accumulators. `Date` is backed by a `Double` offset, so a
        // hydrated row with a non-finite reference value (storage
        // corruption / hand-edited DB) would make
        // `obs.observedAt.timeIntervalSince(last)` return NaN, and
        // `NaN < notchWindowSeconds` is `false` — so the 24-h rate
        // limit would silently fail open and the notch would advance
        // freely. Coerce to `nil` so the rate-limit branch below
        // treats the row as "no prior notch change", which is the
        // safe interpretation: the next observation is allowed to
        // notch once, then `lastNotchChangeAt` is restamped with the
        // valid `obs.observedAt`. This is the Date-shaped cousin of
        // the Double soft-reset above and lives outside that block
        // because the math accumulators may still be valid.
        if let last = next.lastNotchChangeAt,
           !last.timeIntervalSinceReferenceDate.isFinite {
            next.lastNotchChangeAt = nil
        }

        // (3) Welford update — running mean + M2.
        next.sampleCount += 1
        let delta = obs.grantWindowSeconds - next.welfordMean
        next.welfordMean += delta / Double(next.sampleCount)
        let delta2 = obs.grantWindowSeconds - next.welfordMean
        next.welfordM2 += delta * delta2

        // (4) EWMA update. First observation: seed the EWMA to the
        // observation value to avoid cold-start bias toward zero.
        if next.sampleCount == 1 {
            next.ewmaSeconds = obs.grantWindowSeconds
        } else {
            next.ewmaSeconds =
                tuning.ewmaAlpha * obs.grantWindowSeconds
                + (1 - tuning.ewmaAlpha) * next.ewmaSeconds
        }

        // (5) Compute the raw clamp-band candidate and detect
        // saturation. The candidate may exceed the clamp band even
        // when the persisted factor is still inside — saturation here
        // is about the RAW EWMA, not the persisted output.
        let seed = next.seedGrantWindowSeconds
        let rawCandidate = seed > 0 ? next.ewmaSeconds / seed : 1.0
        // Saturation is detected with STRICT inequality: a raw EWMA
        // candidate sitting exactly at the band edge is the legal target,
        // not an instance of "wanting to go past the legal range". This
        // matters at activation: a healthy estimator that has converged
        // to the upper clamp (ratio == 2.0) must not look like K
        // consecutive saturating observations to the divergence-revert
        // counter.
        let saturatedHigh = rawCandidate > tuning.clampBandUpper
        let saturatedLow = rawCandidate < tuning.clampBandLower
        let saturated = saturatedHigh || saturatedLow

        if saturated {
            next.consecutiveClampedObservations += 1
        } else {
            next.consecutiveClampedObservations = 0
        }

        // (6) Activation gate. Below the floor we keep the EWMA +
        // Welford updates (so the state warms up) but do NOT move
        // the persisted scale factor — consumers see the seed.
        guard next.isActivated(tuning: tuning) else {
            next.updatedAt = obs.observedAt
            return (next, AdaptiveDeviceProfileApplyResult(
                persistedScaleFactorChanged: false,
                didRevertToSeed: false,
                blockedByNotchRateLimit: false,
                clampSaturatedThisObservation: saturated
            ))
        }

        // (8') Divergence-revert is evaluated *after* the activation
        // gate so a pre-activation noisy burst doesn't pre-poison the
        // counter. The counter accumulates pre-activation too, but
        // the revert can only fire once the estimator is live.
        if next.consecutiveClampedObservations >= tuning.divergenceObservationThreshold {
            // Reset state to cold + stamp the reason. The seed will
            // be returned by `resolvedScaleFactor` until the next
            // 30 samples re-bootstrap a healthy EWMA. We intentionally
            // preserve `seedGrantWindowSeconds` and `createdAt` —
            // those are the row's identity, not its math.
            //
            // R13: also preserve the prior `schemaVersion` rather than
            // defaulting to `currentSchemaVersion`. The persisted row's
            // schemaVersion stamps the shape it was inserted under; the
            // in-memory revert path must not silently downgrade that
            // stamp when (in a future migration) a row exists on a
            // higher schema than this binary's `currentSchemaVersion`.
            // `LearnedDeviceProfile.apply(_:)` already refuses to update
            // schemaVersion on the SwiftData row, so a wrong value in
            // the in-memory snapshot would create a confusing
            // snapshot/row mismatch in the diagnostics bundle even
            // though storage stays consistent. Cheap fix; keeps the
            // identity-vs-math separation honest.
            let preserved = AdaptiveDeviceProfileState(
                deviceClassRawValue: next.deviceClassRawValue,
                seedGrantWindowSeconds: next.seedGrantWindowSeconds,
                createdAt: next.createdAt,
                schemaVersion: next.schemaVersion
            )
            var reverted = preserved
            reverted.updatedAt = obs.observedAt
            reverted.lastRevertReason = .divergenceClampSaturation
            return (reverted, AdaptiveDeviceProfileApplyResult(
                persistedScaleFactorChanged: state.persistedScaleFactor != reverted.persistedScaleFactor,
                didRevertToSeed: true,
                blockedByNotchRateLimit: false,
                clampSaturatedThisObservation: saturated
            ))
        }

        // (7) Rate-limit + clamp + notch the persisted scale factor.
        // Clamp the raw candidate first so the rate-limit comparison
        // is against the legal target the estimator wants to occupy.
        let clampedCandidate = min(
            tuning.clampBandUpper,
            max(tuning.clampBandLower, rawCandidate)
        )

        // 24-h notch rate limit: if the last notch change was within
        // the rolling window, block any advance.
        if let last = next.lastNotchChangeAt,
           obs.observedAt.timeIntervalSince(last) < tuning.notchWindowSeconds {
            next.updatedAt = obs.observedAt
            return (next, AdaptiveDeviceProfileApplyResult(
                persistedScaleFactorChanged: false,
                didRevertToSeed: false,
                blockedByNotchRateLimit: true,
                clampSaturatedThisObservation: saturated
            ))
        }

        // Cap the per-observation move to ±notchStep — even if the
        // candidate has drifted multiple notches away, we can only
        // walk one notch per window. This is the "max one-notch
        // change per 24h" rule from the bead spec.
        let prior = next.persistedScaleFactor
        let direction = clampedCandidate - prior
        let stepped: Double
        if abs(direction) <= tuning.notchStep {
            stepped = clampedCandidate
        } else if direction > 0 {
            stepped = prior + tuning.notchStep
        } else {
            stepped = prior - tuning.notchStep
        }

        // Re-clamp after stepping to guard against floating-point
        // drift around the clamp boundary.
        let finalFactor = min(
            tuning.clampBandUpper,
            max(tuning.clampBandLower, stepped)
        )

        let didChange = abs(finalFactor - prior) > .ulpOfOne
        if didChange {
            next.persistedScaleFactor = finalFactor
            next.lastNotchChangeAt = obs.observedAt
        }
        next.updatedAt = obs.observedAt

        return (next, AdaptiveDeviceProfileApplyResult(
            persistedScaleFactorChanged: didChange,
            didRevertToSeed: false,
            blockedByNotchRateLimit: false,
            clampSaturatedThisObservation: saturated
        ))
    }

    /// The scale factor consumers should multiply the seed
    /// `nominalSliceSizeBytes` / `grantWindowMedianSeconds` by. Returns
    /// 1.0 (no change) when the estimator has not yet activated, so
    /// the consumer's seed-based math is byte-identical to today.
    ///
    /// This is the ONLY method consumers should call to read the
    /// estimator's output. Reading `state.persistedScaleFactor`
    /// directly bypasses the activation gate.
    static func resolvedScaleFactor(
        state: AdaptiveDeviceProfileState,
        tuning: AdaptiveDeviceProfileTuning = .standard
    ) -> Double {
        guard state.isActivated(tuning: tuning) else { return 1.0 }
        // Defense-in-depth: clamp at read time too. If a future
        // migration corrupts the persisted factor, the consumer still
        // gets a sane number rather than a runtime crash from a
        // negative slice size downstream.
        let clamped = min(
            tuning.clampBandUpper,
            max(tuning.clampBandLower, state.persistedScaleFactor)
        )
        // Never-zero floor: belt-and-suspenders. The clamp band lower
        // bound (0.5) already enforces this, but this guard catches a
        // corrupted clampBandLower=0 row too.
        return max(0.0001, clamped)
    }

    /// Apply the resolved scale factor to a seed device-class profile
    /// to produce the adaptive output. Returns a brand-new
    /// `DeviceClassProfile` value with the slice-relevant integer
    /// fields scaled; non-slice fields (avgShardDurationMs) are passed
    /// through unchanged so Live Activity ETA is unaffected.
    ///
    /// The scaled fields are:
    ///   * `grantWindowMedianSeconds`
    ///   * `grantWindowP95Seconds`
    ///   * `nominalSliceSizeBytes`
    ///
    /// Each is rounded to the nearest integer with a never-zero floor
    /// (`max(1, ...)`). The seed table's own minima are well above 1
    /// for all current device classes; the floor is purely a defensive
    /// guard against the rare clampBandLower=0.5 × tiny-seed corner.
    static func project(
        seed: DeviceClassProfile,
        scaledBy factor: Double
    ) -> DeviceClassProfile {
        let safeFactor = max(0.0001, factor)
        return DeviceClassProfile(
            deviceClass: seed.deviceClass,
            grantWindowMedianSeconds: max(1, Int((Double(seed.grantWindowMedianSeconds) * safeFactor).rounded())),
            grantWindowP95Seconds: max(1, Int((Double(seed.grantWindowP95Seconds) * safeFactor).rounded())),
            nominalSliceSizeBytes: max(1, Int((Double(seed.nominalSliceSizeBytes) * safeFactor).rounded())),
            cpuWindowSeconds: seed.cpuWindowSeconds,
            bytesPerCpuSecond: seed.bytesPerCpuSecond,
            avgShardDurationMs: seed.avgShardDurationMs
        )
    }
}
