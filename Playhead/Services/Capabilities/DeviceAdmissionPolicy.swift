// DeviceAdmissionPolicy.swift
// Shared device-state gating policy for intensive on-device analysis work.
//
// Both AdmissionController (phase-3 backfill scheduling) and
// BackgroundProcessingService (BGProcessingTask + capability observer) need
// to answer the same question: "given the current device state, can we run
// intensive backfill-class work right now?"
//
// This struct is the single source of truth for that decision so the two
// services cannot drift apart on thresholds or conditions. Callers layer
// queue-state and service-specific gates (e.g. an upload in progress) on
// top of the policy result.

import Foundation

struct DeviceAdmissionPolicy: Sendable {

    /// Battery threshold below which non-critical analysis is paused
    /// when the device is not charging. 20% mirrors the documented
    /// pause policy in BackgroundProcessingService.
    static let lowBatteryThreshold: Float = 0.20

    enum Decision: Sendable, Equatable {
        case admit
        case deferred(DeferReason)
    }

    enum DeferReason: String, Sendable, Equatable, CaseIterable {
        case thermalThrottled
        case batteryTooLow
        case lowPowerMode
    }

    /// Evaluate whether intensive backfill-class work may run.
    ///
    /// - Parameters:
    ///   - snapshot: The current capability snapshot.
    ///   - batteryLevel: The most recent battery reading the caller has.
    ///     Pass `nil` (or a negative value) when the level is unknown; the
    ///     policy treats unknown battery as "not low" so work can proceed.
    ///
    /// Precedence:
    ///   1. Thermal throttling (`.critical`)
    ///   2. Low battery while not charging
    ///   3. Low Power Mode
    ///
    /// Note on Low Power Mode: BackgroundProcessingService historically
    /// paused backfill in Low Power Mode while AdmissionController did not.
    /// The consolidated policy is the superset of both gates, so callers
    /// using AdmissionController will now also defer in Low Power Mode.
    static func evaluate(snapshot: CapabilitySnapshot, batteryLevel: Float?) -> Decision {
        // Serious thermal still reduces the foreground hot-path aggressiveness,
        // but it no longer blocks deferred/background work outright. Reserve
        // full admission denial for critical thermal distress.
        if snapshot.thermalState == .critical {
            return .deferred(.thermalThrottled)
        }

        let batteryKnownAndLow: Bool
        if let level = batteryLevel, level >= 0, level < lowBatteryThreshold {
            batteryKnownAndLow = true
        } else {
            batteryKnownAndLow = false
        }

        if batteryKnownAndLow && !snapshot.isCharging {
            return .deferred(.batteryTooLow)
        }

        if snapshot.isLowPowerMode {
            return .deferred(.lowPowerMode)
        }

        return .admit
    }
}
