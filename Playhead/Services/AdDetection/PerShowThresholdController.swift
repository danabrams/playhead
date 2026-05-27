// PerShowThresholdController.swift
// playhead-xsdz.11: Per-show AUTO-SKIP THRESHOLD control from user feedback.
//
// What it is
// ----------
// A bounded, deterministic PI-like controller (proportional + integral; NO
// derivative term) that adapts the per-show auto-skip confidence threshold to
// each user's corrections. It is NOT a learned model — both cross-model idea-
// duel reactions deferred an online ML model as too data-hungry and converged
// on this simple scalar controller.
//
// Two-sided personalization
// --------------------------
// Unlike the one-sided precision penalties (which can only LOWER skip
// confidence), this controller can RAISE or LOWER the per-show threshold:
//
//   • FALSE-POSITIVE signal — the user listened THROUGH / reverted an
//     auto-skipped section ("Listen" on an auto-skip, "not an ad" veto). The
//     system was too aggressive on this show → RAISE the threshold (be more
//     conservative). Error term = +1.
//   • MISS signal — the user scrubbed forward through undetected ad content /
//     reported a missed ad. The system was too conservative → LOWER the
//     threshold (be more aggressive). Error term = −1.
//
// Per-show adaptation handles heterogeneity a single global threshold can't:
// a chatty interview show with editorial brand mentions wants a higher bar; a
// network show with crisp ad pods can tolerate a lower one.
//
// PI controller math
// -------------------
// For an ordered correction history, fold each signal's error e ∈ {+1, −1}:
//
//   integral  += e                               (running sum of errors)
//   offset      = clamp(Kp·e_last + Ki·integral, −maxOffset, +maxOffset)
//
// The output `offset` is added to the global auto-skip threshold at the gate;
// the EFFECTIVE threshold is then clamped to `[effectiveMin, effectiveMax]`
// ( [0.55, 0.95] by default). The derivative term is intentionally dropped —
// it is too reactive to sparse, noisy correction signals.
//
// Cold-start / min-sample gate
// ----------------------------
// Below `minSamples` corrections the controller returns a ZERO offset — the
// gate uses the unmodified global threshold. Adaptation only begins once
// enough corrections have accumulated to be a meaningful signal.
//
// Determinism
// -----------
// Pure value-type reduction over the signal sequence: the same history always
// yields the same `(offset, integral, sampleCount)`. No clocks, no RNG, no
// floating-point order hazards (errors are exactly ±1, accumulated as an Int).
//
// Off-by-default
// --------------
// This type is INERT until `AdDetectionConfig.perShowThresholdControlEnabled`
// is true AND a `PerShowThresholdControllerStore` is wired. With the flag off
// (the production default) no state is read or written and the gate uses the
// unmodified global threshold — byte-identical to pre-xsdz.11.

import Foundation

// MARK: - Signal

/// The correction signal that drives one controller update.
enum ThresholdControlSignal: Sendable, Equatable {
    /// The user listened through / reverted an auto-skipped section — a
    /// confirmed false positive. The threshold should RISE (more conservative).
    case falsePositive
    /// The user scrubbed through / reported undetected ad content — a miss. The
    /// threshold should FALL (more aggressive).
    case miss

    /// The PI error term for this signal. `+1` raises the threshold, `−1`
    /// lowers it. Exactly ±1 so the integral accumulates without FP drift.
    var error: Int {
        switch self {
        case .falsePositive: return 1
        case .miss: return -1
        }
    }
}

// MARK: - Parameters

/// Tunable gains and bounds for the PI controller. Conservative defaults; not
/// tuned on real data. Mirrors the "pure value type, caller passes resolved
/// numbers" convention of `AutoSkipPrecisionGateConfig`.
struct PerShowThresholdControllerParameters: Sendable, Equatable {
    /// Proportional gain. Applied to the MOST RECENT signal's error. Small so a
    /// single correction nudges, never lurches.
    let proportionalGain: Double
    /// Integral gain. Applied to the running sum of errors so a persistent
    /// one-sided correction stream slowly accumulates a larger offset.
    let integralGain: Double
    /// Maximum absolute offset the controller may emit, in confidence units.
    /// Bounds the personalization so a runaway correction stream cannot move
    /// the threshold arbitrarily far before the effective clamp even applies.
    let maxOffset: Double
    /// Minimum corrections required before the controller emits any non-zero
    /// offset. Below this the gate uses the unmodified global threshold
    /// (cold-start).
    let minSamples: Int
    /// Lower bound of the EFFECTIVE (global + offset) auto-skip threshold.
    let effectiveMin: Double
    /// Upper bound of the EFFECTIVE (global + offset) auto-skip threshold.
    let effectiveMax: Double

    /// Conservative defaults per the bead: Kp ~ 0.02, Ki ~ 0.005, clamp the
    /// effective threshold to [0.55, 0.95], require a small minimum sample
    /// count before adapting, and bound the offset at ±0.15 (well inside the
    /// 0.40-wide clamp window so the offset, not the clamp, is the primary
    /// limiter on the common path).
    static let `default` = PerShowThresholdControllerParameters(
        proportionalGain: 0.02,
        integralGain: 0.005,
        maxOffset: 0.15,
        minSamples: 5,
        effectiveMin: 0.55,
        effectiveMax: 0.95
    )

    init(
        proportionalGain: Double,
        integralGain: Double,
        maxOffset: Double,
        minSamples: Int,
        effectiveMin: Double,
        effectiveMax: Double
    ) {
        self.proportionalGain = proportionalGain
        self.integralGain = integralGain
        self.maxOffset = maxOffset
        self.minSamples = minSamples
        self.effectiveMin = effectiveMin
        self.effectiveMax = effectiveMax
    }
}

// MARK: - State

/// Persisted per-show controller state. Pure value type; the store serializes
/// these three scalars per show.
struct PerShowThresholdControllerState: Sendable, Equatable {
    /// The current bounded offset to add to the global auto-skip threshold.
    /// Zero until `minSamples` corrections accumulate.
    var offset: Double
    /// Running sum of signal errors (the integral accumulator). FP adds +1,
    /// miss adds −1. Stored as an Int so accumulation is exact and the
    /// "same history → same state" determinism is bit-stable.
    var integral: Int
    /// Number of corrections folded into this state so far (the min-sample
    /// gate counter).
    var sampleCount: Int

    /// The cold-start / empty state: no offset, no integral, no samples.
    static let zero = PerShowThresholdControllerState(offset: 0, integral: 0, sampleCount: 0)

    init(offset: Double, integral: Int, sampleCount: Int) {
        self.offset = offset
        self.integral = integral
        self.sampleCount = sampleCount
    }
}

// MARK: - Controller

/// Pure, deterministic PI-like controller. Stateless namespace — all state is
/// threaded through `PerShowThresholdControllerState`. No clocks, no RNG.
enum PerShowThresholdController {

    /// Fold ONE correction signal into the prior state, returning the new
    /// state. Deterministic: `(prior, signal) → next` is a pure function.
    ///
    /// Update rule:
    ///   integral'    = integral + signal.error
    ///   sampleCount' = sampleCount + 1
    ///   offset'      = (sampleCount' < minSamples) ? 0
    ///                : clamp(Kp·signal.error + Ki·integral', −maxOffset, +maxOffset)
    ///
    /// The min-sample gate forces a zero offset (cold-start) until enough
    /// corrections accumulate; once past the gate the offset is the bounded PI
    /// term. The proportional term uses the most-recent error (no derivative);
    /// the integral term uses the post-update accumulator.
    static func apply(
        signal: ThresholdControlSignal,
        to prior: PerShowThresholdControllerState,
        parameters: PerShowThresholdControllerParameters = .default
    ) -> PerShowThresholdControllerState {
        let integral = prior.integral + signal.error
        let sampleCount = prior.sampleCount + 1

        let offset: Double
        if sampleCount < parameters.minSamples {
            // Cold-start: below the min-sample gate the controller is inert.
            offset = 0
        } else {
            let raw = parameters.proportionalGain * Double(signal.error)
                + parameters.integralGain * Double(integral)
            offset = clampOffset(raw, maxOffset: parameters.maxOffset)
        }

        return PerShowThresholdControllerState(
            offset: offset,
            integral: integral,
            sampleCount: sampleCount
        )
    }

    /// Replay an ENTIRE ordered correction history from the zero state. Pure;
    /// the same sequence always yields the same final state. Used by tests for
    /// the determinism contract and by any caller that wants to recompute a
    /// state from scratch rather than fold incrementally.
    static func replay(
        signals: [ThresholdControlSignal],
        parameters: PerShowThresholdControllerParameters = .default
    ) -> PerShowThresholdControllerState {
        signals.reduce(.zero) { state, signal in
            apply(signal: signal, to: state, parameters: parameters)
        }
    }

    /// Apply a per-show offset to the global auto-skip threshold and clamp the
    /// EFFECTIVE result to `[effectiveMin, effectiveMax]`. This is the single
    /// place the read path turns `(globalThreshold, state)` into the number the
    /// gate compares against — so the clamp invariant is enforced in one spot.
    ///
    /// A non-finite global threshold or offset degrades to the (clamped) global
    /// threshold so a corrupt input can never produce a NaN gate.
    static func effectiveThreshold(
        globalThreshold: Double,
        offset: Double,
        parameters: PerShowThresholdControllerParameters = .default
    ) -> Double {
        let safeOffset = offset.isFinite ? offset : 0
        let safeGlobal = globalThreshold.isFinite ? globalThreshold : parameters.effectiveMin
        let combined = safeGlobal + safeOffset
        return min(parameters.effectiveMax, max(parameters.effectiveMin, combined))
    }

    /// Symmetric clamp of the raw PI term to `[−maxOffset, +maxOffset]`.
    private static func clampOffset(_ value: Double, maxOffset: Double) -> Double {
        guard value.isFinite else { return 0 }
        let bound = abs(maxOffset)
        return min(bound, max(-bound, value))
    }
}
