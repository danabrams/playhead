// ExecutionConditionClassifier.swift
// Classifies device execution conditions into favorable / constrained /
// mixed buckets for SLI cohorting.
//
// Decision rules (defended, encoded here — not in markdown):
//
//   favorable  = Wi-Fi
//                 AND (charging OR battery >= 0.50)
//                 AND thermal <= fair
//                 AND Low Power Mode disabled
//   constrained = cellular
//                 OR (battery < 0.20 AND not charging)
//                 OR thermal >= serious
//                 OR Low Power Mode enabled
//   mixed       = everything else
//
// Precedence: `constrained` wins over `favorable` — if any constrained
// predicate holds, the bucket is `constrained`, even if one or more
// favorable predicates also hold. This matches the intent of the
// threshold ("any serious resource pressure pushes us to constrained").
//
// Low Power Mode is treated as constrained-on-its-own (same shape as
// "battery < 20% AND not charging"): the OS itself is throttling work,
// so even an otherwise-favorable Wi-Fi/charging/nominal device is a
// constrained execution environment for analysis. This also keeps the
// `InternalMissCause.lowPowerMode` taxonomy entry honest — without this
// axis, an LPM-enabled device biases the SLI buckets toward `favorable`.

import Foundation

/// Reachability of the active network path at measurement time.
///
/// Kept intentionally coarse — we only need to distinguish Wi-Fi from
/// cellular for cohorting. "Unknown" collapses into `mixed` by way of
/// neither favorable nor constrained predicates firing on the network axis.
enum SLIReachability: String, CaseIterable, Codable, Sendable, Hashable {
    case wifi
    case cellular
    /// No known reachability (e.g. airplane mode, path monitor not yet
    /// started). Treated as "not Wi-Fi, not cellular" — it never satisfies
    /// the favorable-network predicate and never trips the constrained-
    /// network predicate, so it falls into `mixed`.
    case unknown
}

/// Whether the device battery is currently accepting charge.
///
/// Mirrors the Wi-Fi / cellular split: intentionally coarse and decoupled
/// from UIKit so this type can be exercised from pure unit tests.
enum SLIBatteryState: String, CaseIterable, Codable, Sendable, Hashable {
    case charging
    case notCharging
    /// Charging state is not currently known (e.g. battery monitoring not
    /// enabled yet). Treated as "not charging" for classification purposes.
    case unknown
}

/// Input to ``ExecutionConditionClassifier``. Pure value type so tests
/// can drive every boundary without mocking UIKit or Network.framework.
struct ExecutionConditionInput: Sendable, Equatable {

    /// Active network path reachability.
    let reachability: SLIReachability

    /// Battery level in [0.0, 1.0]. Pass a negative value if unknown;
    /// unknown battery is treated as "not low" and "not >= 50%" — that
    /// is, it satisfies neither the favorable nor constrained battery
    /// predicates on its own.
    /// (Float, not Double, to match `UIDevice.batteryLevel` and
    /// `SLI.swift:25-30`'s convention of mirroring `QualityProfile`'s
    /// `lowBatteryThreshold` which is itself a Float in [0, 1].)
    let batteryLevel: Float

    /// Whether the device is currently charging.
    let batteryState: SLIBatteryState

    /// Thermal state at measurement time.
    let thermalState: ThermalState

    /// Whether Low Power Mode is currently enabled.
    ///
    /// `true` forces the bucket to `constrained` regardless of the other
    /// axes (same shape as "battery < 20% and not charging"). The OS
    /// throttles background work in LPM, so an otherwise-favorable
    /// device is still a constrained execution environment for analysis.
    let isLowPowerMode: Bool
}

/// Favorable battery-level floor (inclusive). Expressed as a ratio in
/// [0.0, 1.0] so the type matches `CapabilitySnapshot`/`QualityProfile`
/// conventions.
enum ExecutionConditionBatteryThresholds {
    /// 0.50 — at or above this, battery contributes to `favorable`
    /// (comparison is `>=`, so exactly 50% qualifies).
    static let favorableFloor: Float = 0.50
    /// 0.20 — strictly below this (and not charging), battery triggers
    /// `constrained` (comparison is `<`, so exactly 20% does not qualify
    /// as constrained on its own).
    static let constrainedCeiling: Float = 0.20
}

/// Classifies an ``ExecutionConditionInput`` into an
/// ``SLIExecutionCondition`` bucket.
///
/// Pure static function — no hidden state, no actor hops. Emitter code
/// is expected to gather the inputs (network path, battery level/state,
/// thermal state) at an SLI emission moment and call `classify` before
/// stamping the resulting cohort onto the emission.
enum ExecutionConditionClassifier {

    /// Returns the execution-condition bucket for the given input.
    ///
    /// Evaluation order:
    /// 1. If any constrained predicate holds, return `.constrained`.
    /// 2. Otherwise, if all favorable predicates hold, return `.favorable`.
    /// 3. Otherwise, return `.mixed`.
    static func classify(_ input: ExecutionConditionInput) -> SLIExecutionCondition {

        // --- Constrained predicates (any one wins) ---

        if input.reachability == .cellular {
            return .constrained
        }

        let batteryKnownAndVeryLow =
            input.batteryLevel >= 0 &&
            input.batteryLevel < ExecutionConditionBatteryThresholds.constrainedCeiling
        if batteryKnownAndVeryLow && input.batteryState != .charging {
            return .constrained
        }

        if input.thermalState == .serious || input.thermalState == .critical {
            return .constrained
        }

        if input.isLowPowerMode {
            return .constrained
        }

        // --- Favorable predicates (all must hold) ---

        let networkFavorable = input.reachability == .wifi

        let powerFavorable: Bool = {
            if input.batteryState == .charging { return true }
            // Unknown battery level cannot satisfy the >= 50% floor.
            if input.batteryLevel < 0 { return false }
            return input.batteryLevel >= ExecutionConditionBatteryThresholds.favorableFloor
        }()

        let thermalFavorable =
            input.thermalState == .nominal || input.thermalState == .fair

        if networkFavorable && powerFavorable && thermalFavorable {
            return .favorable
        }

        return .mixed
    }
}
