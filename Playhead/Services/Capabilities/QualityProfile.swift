// QualityProfile.swift
// Unified thermal/battery profile surface for scheduler policy decisions.
//
// Prior to this file, thermal state, low-power mode, battery level, and
// charging state were read piecewise by each consumer (AnalysisWorkScheduler,
// BackgroundProcessingService, DeviceAdmissionPolicy). That produced drift
// across callers when thresholds changed. QualityProfile consolidates the
// read into a single named surface with explicit per-variant policy that the
// scheduler and other consumers can route through.
//
// The four variants intentionally mirror ProcessInfo.ThermalState so callers
// can reason about profile changes using familiar thermal vocabulary. The
// derivation rules (below) layer battery and low-power-mode demotions on top
// of the raw thermal read.
//
// Plan §6 Phase 0 deliverable 7 — playhead-5ih.

import Foundation
import UIKit

/// Four-level device capability profile used by schedulers to decide which
/// work lanes may run. Variants are named to match ProcessInfo.ThermalState
/// because thermal state is the primary signal; battery and low-power
/// conditions can demote the profile by one step (see `derive`).
enum QualityProfile: String, Codable, Sendable, Equatable, CaseIterable, CustomStringConvertible {
    case nominal
    case fair
    case serious
    case critical

    /// Human-readable name for logging. Mirrors the `ThermalState` extension
    /// in `CapabilitySnapshot.swift` so logs across the capability surface
    /// stay consistent.
    var description: String { rawValue }

    // MARK: - Derivation

    /// Threshold below which battery is considered "critically low." Mirrors
    /// `DeviceAdmissionPolicy.lowBatteryThreshold` so the two gates stay in
    /// lockstep. A value of exactly 0.20 is NOT treated as low.
    static let lowBatteryThreshold: Float = 0.20

    /// Pure derivation of a quality profile from the underlying OS reads.
    ///
    /// The derivation policy is:
    ///
    /// 1. Start with a baseline that maps 1:1 from `ProcessInfo.ThermalState`
    ///    to the QualityProfile variant of the same name. This preserves the
    ///    documented correspondence and makes thermal transitions readable in
    ///    logs without translation.
    /// 2. Demote the baseline by one step (nominal→fair, fair→serious) when
    ///    *either* low-power-mode is on, *or* battery is critically low
    ///    (< `lowBatteryThreshold`) while unplugged. `serious` and
    ///    `critical` are NOT demoted further — once we're in the two most
    ///    aggressive throttle states, there is no "worse" scheduler policy
    ///    we can apply without pausing work we may still need (T0 playback
    ///    in `serious`) or keeping work paused that we've already paused
    ///    (`critical`).
    /// 3. Battery level `< 0` is treated as "unknown" (matches UIDevice's
    ///    reporting convention when battery monitoring is off) and does NOT
    ///    trigger demotion. This is the safer interpretation: we would
    ///    rather run work than silently refuse based on a missing signal.
    /// 4. `batteryState == .charging` or `.full` prevents the battery-level
    ///    demotion even when the level is below threshold — being on the
    ///    cord means the level is trending up, not a reason to throttle.
    ///
    /// This function is intentionally pure so the scheduler and tests share
    /// the same derivation; there are no hidden singletons or defaults.
    static func derive(
        thermalState: ProcessInfo.ThermalState,
        batteryLevel: Float,
        batteryState: UIDevice.BatteryState,
        isLowPowerMode: Bool
    ) -> QualityProfile {
        let baseline: QualityProfile = {
            switch thermalState {
            case .nominal: return .nominal
            case .fair: return .fair
            case .serious: return .serious
            case .critical: return .critical
            @unknown default: return .nominal
            }
        }()

        // Only nominal/fair can be demoted. Serious and critical are
        // already as throttled as this enum supports.
        guard baseline == .nominal || baseline == .fair else {
            return baseline
        }

        let isCharging = batteryState == .charging || batteryState == .full
        let batteryKnownAndLow: Bool = {
            // Treat negative levels as "unknown" — UIDevice reports -1 when
            // battery monitoring is off. Unknown battery does not demote.
            guard batteryLevel >= 0 else { return false }
            return batteryLevel < lowBatteryThreshold
        }()

        let shouldDemote = isLowPowerMode || (batteryKnownAndLow && !isCharging)
        guard shouldDemote else { return baseline }

        switch baseline {
        case .nominal: return .fair
        case .fair: return .serious
        case .serious, .critical: return baseline
        }
    }

    // MARK: - Scheduler Policy

    /// The policy the scheduler applies for a given QualityProfile variant.
    ///
    /// "Lanes" correspond to deferred-work tiers in `AnalysisWorkScheduler`:
    /// - **T0 (hot-path / playback)** — zero-coverage playback jobs the
    ///   scheduler must drain promptly. Gated only by `pauseAllWork`.
    /// - **Soon lane (T1)** — shallow deferred pre-analysis (typically
    ///   `t1DepthSeconds`). Gated by `allowSoonLane`.
    /// - **Background lane (T2)** — deep deferred pre-analysis (typically
    ///   `t2DepthSeconds`). Gated by `allowBackgroundLane`.
    ///
    /// `sliceFraction` is the hot-path lookahead / work slice multiplier:
    /// `1.0` means run full slices, `0.5` means run half slices, `0.0` is a
    /// hard pause. Non-scheduler consumers (e.g. hot-path lookahead) may
    /// read this to scale their own work.
    struct SchedulerPolicy: Sendable, Equatable {
        let sliceFraction: Double
        let allowSoonLane: Bool
        let allowBackgroundLane: Bool
        let pauseAllWork: Bool
    }

    /// Per-variant scheduler policy. Matches the playhead-5ih bead spec:
    /// - `nominal`: full slice, all lanes.
    /// - `fair`: full slice, pause Background lane.
    /// - `serious`: half slice, pause Soon + Background lanes.
    /// - `critical`: pause all work.
    var schedulerPolicy: SchedulerPolicy {
        switch self {
        case .nominal:
            return SchedulerPolicy(
                sliceFraction: 1.0,
                allowSoonLane: true,
                allowBackgroundLane: true,
                pauseAllWork: false
            )
        case .fair:
            return SchedulerPolicy(
                sliceFraction: 1.0,
                allowSoonLane: true,
                allowBackgroundLane: false,
                pauseAllWork: false
            )
        case .serious:
            return SchedulerPolicy(
                sliceFraction: 0.5,
                allowSoonLane: false,
                allowBackgroundLane: false,
                pauseAllWork: false
            )
        case .critical:
            return SchedulerPolicy(
                sliceFraction: 0.0,
                allowSoonLane: false,
                allowBackgroundLane: false,
                pauseAllWork: true
            )
        }
    }
}
