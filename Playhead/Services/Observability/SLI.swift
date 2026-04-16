// SLI.swift
// The five cohort-based SLIs for download→ready pipeline health, with
// their defended thresholds encoded inline as constants (not magic
// numbers scattered in documentation).
//
// Emission rule — important, read this before wiring in the Phase 1
// emitter (playhead-1nl6):
//
//   SLIs are emitted from exactly three user-observable moments:
//     1. Play-starts          — user hits play on an episode.
//     2. Listening-window entries — the player enters a new listening
//        window (e.g. resuming after a seek or a long pause).
//     3. Pause transitions    — playback pauses, resumes, stops, or the
//        user scrubs to a new position.
//
//   SLIs are NOT computed from passive cell renders, slice counts, or
//   background tick loops. Passive readers do not reflect user-observed
//   readiness and must not trigger emission.
//
// warm_resume_hit_rate is explicitly NOT an SLI (secondary KPI only).
// Do not add it here.

import Foundation

// MARK: - Threshold Units
//
// Thresholds are expressed in SI units: durations in TimeInterval
// (seconds, Double) and rates as Double fractions in [0, 1]. This matches
// the existing project convention (see DeviceAdmissionPolicy's
// `lowBatteryThreshold` as a Float in [0, 1]).

/// The canonical set of SLIs for download→ready pipeline health.
///
/// Each SLI has a scope (who counts in the denominator), a threshold,
/// and a unit. The `isMeaningful(for:)` method encodes which cohorts
/// are defined for this SLI.
enum SLI: String, CaseIterable, Codable, Sendable, Hashable {

    /// Time from explicit-download tap to the asset being fully downloaded.
    /// Scope: explicit download, 30–90 min episode.
    case timeToDownloaded = "time_to_downloaded"

    /// Time from explicit-download tap to the first proximal skip being
    /// ready. "Proximal" means the next ad the user is actually about
    /// to hit, not the whole episode.
    /// Scope: explicit download, eligibleAndAvailable mode,
    /// 30–90 min episode.
    case timeToProximalSkipReady = "time_to_proximal_skip_ready"

    /// Fraction of play-starts where the episode is skip-ready at the
    /// moment the user hits play.
    case readyByFirstPlayRate = "ready_by_first_play_rate"

    /// Fraction of ready episodes that later produce an unattributed
    /// skip miss or a retraction.
    case falseReadyRate = "false_ready_rate"

    /// Fraction of pauses whose cause cannot be mapped to the Phase-0
    /// cause taxonomy (see playhead-v11, parallel bead).
    case unattributedPauseRate = "unattributed_pause_rate"

    /// The units this SLI is measured in.
    var unit: SLIUnit {
        switch self {
        case .timeToDownloaded, .timeToProximalSkipReady:
            return .durationSeconds
        case .readyByFirstPlayRate, .falseReadyRate, .unattributedPauseRate:
            return .rate
        }
    }

    /// Whether this (SLI × cohort) cell is meaningful. See
    /// ``SLICohortMeaningfulness`` for the full rules. Empty cells
    /// must be emitted as nil, not zero.
    func isMeaningful(for cohort: SLICohort) -> Bool {
        SLICohortMeaningfulness.isMeaningful(sli: self, cohort: cohort)
    }
}

/// The measurement units an SLI's values are reported in.
enum SLIUnit: String, Codable, Sendable, Hashable {
    /// Duration in seconds (TimeInterval). Measured as a latency with
    /// P50/P90 thresholds.
    case durationSeconds
    /// A fraction in [0, 1]. Measured as a rate with a floor or ceiling
    /// threshold.
    case rate
}

// MARK: - Thresholds (defended — no magic numbers)

/// Thresholds for `time_to_downloaded` (explicit download, 30–90 min episode).
///
/// P50 ≤ 15 minutes, P90 ≤ 60 minutes.
enum TimeToDownloadedThresholds {
    /// 15 minutes expressed in seconds.
    static let p50Seconds: TimeInterval = 15 * 60
    /// 60 minutes expressed in seconds.
    static let p90Seconds: TimeInterval = 60 * 60
}

/// Thresholds for `time_to_proximal_skip_ready` (explicit download,
/// eligibleAndAvailable mode, 30–90 min episode).
///
/// P50 ≤ 45 minutes, P90 ≤ 4 hours.
enum TimeToProximalSkipReadyThresholds {
    /// 45 minutes expressed in seconds.
    static let p50Seconds: TimeInterval = 45 * 60
    /// 4 hours expressed in seconds.
    static let p90Seconds: TimeInterval = 4 * 60 * 60
}

/// Threshold for `ready_by_first_play_rate`.
///
/// ≥ 85% of play-starts must be skip-ready at the moment the user hits play.
enum ReadyByFirstPlayRateThresholds {
    /// 0.85 expressed as a fraction in [0, 1]. This is a floor: we want
    /// the observed rate to be AT LEAST this value.
    static let minRate: Double = 0.85
}

/// Thresholds for `false_ready_rate`.
///
/// Dogfood target ≤ 2%. Ship target ≤ 1%. Both are ceilings: we want
/// the observed rate to be AT MOST these values.
enum FalseReadyRateThresholds {
    /// 0.02 — dogfood ceiling.
    static let dogfoodMaxRate: Double = 0.02
    /// 0.01 — shippable ceiling.
    static let shipMaxRate: Double = 0.01
}

/// Thresholds for `unattributed_pause_rate`.
///
/// Harness = 0 (any unattributed pause in replay fails the suite;
/// "harness" = synthetic replay, controlled inputs, no device variance).
/// Field < 0.5% (dogfood/prod; "field" = real users).
enum UnattributedPauseRateThresholds {
    /// 0 — any unattributed pause in the replay harness fails the suite.
    static let harnessMaxRate: Double = 0.0
    /// 0.005 — field ceiling.
    static let fieldMaxRate: Double = 0.005
}
