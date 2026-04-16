// SLICohortAxes.swift
// Cohort axes for the five download→ready pipeline SLIs.
//
// Each of the five SLIs defined in ``SLI`` is cut by all four axes below.
// Not every (SLI × cohort) cell is meaningful — see
// ``SLICohortMeaningfulness`` for the mapping. Empty cells must be emitted
// as nil, not zero.
//
// This is the data-source contract consumed by the Phase 1 instrumentation
// emitter (playhead-1nl6). Do NOT compute SLIs from passive cell renders,
// slice counts, or background tick loops — SLIs are emitted from
// play-starts, listening-window entries, and pause transitions only.

import Foundation

/// What caused the episode's analysis pipeline to be initiated.
enum SLITrigger: String, CaseIterable, Codable, Sendable, Hashable {
    /// User explicitly tapped "download" on an individual episode.
    case explicitDownload
    /// Episode was auto-downloaded because the show is in the user's
    /// subscription queue and auto-download is enabled.
    case subscriptionAutoDownload
}

/// The analysis mode this episode is operating in.
///
/// - `transportOnly`: transport/metadata are ready but no semantic analysis
///   has been initiated (e.g. the device is ineligible or the user has
///   opted out for this episode).
/// - `eligibleButUnavailableNow`: device is eligible for analysis but
///   resources are not currently available (thermal, battery, Low Power
///   Mode, disk, etc.). The episode is queued but work is deferred.
/// - `eligibleAndAvailable`: analysis is actively running or has completed.
enum SLIAnalysisMode: String, CaseIterable, Codable, Sendable, Hashable {
    case transportOnly
    case eligibleButUnavailableNow
    case eligibleAndAvailable
}

/// Coarse classification of device execution conditions at the moment
/// of measurement. See ``ExecutionConditionClassifier`` for the exact
/// decision rules.
///
/// - `favorable`: Wi-Fi AND (charging OR battery ≥ 50%) AND thermal ≤ fair.
/// - `constrained`: cellular OR (battery < 20% AND not charging) OR
///    thermal ≥ serious.
/// - `mixed`: anything else (e.g. Wi-Fi, not charging, 40% battery, fair).
enum SLIExecutionCondition: String, CaseIterable, Codable, Sendable, Hashable {
    case favorable
    case constrained
    case mixed
}

/// Episode duration buckets used for cohorting.
///
/// Boundary rule (see ``EpisodeDurationBucketClassifier``):
/// - `under30m`:         duration <  30 min
/// - `between30and60m`:  30 min ≤ duration ≤ 60 min
/// - `between60and90m`:  60 min <  duration ≤ 90 min
/// - `over90m`:          90 min <  duration
///
/// Exact boundaries (30m, 60m, 90m) belong to the lower bucket to keep
/// the "eligible 30–90 min episode" reporting scope intuitive: an episode
/// of exactly 30 minutes is the smallest member of the 30–60 bucket.
enum SLIEpisodeDurationBucket: String, CaseIterable, Codable, Sendable, Hashable {
    case under30m
    case between30and60m
    case between60and90m
    case over90m
}

/// A full cohort coordinate — one cell of the (trigger × mode × condition
/// × duration) grid. The Phase 1 emitter stamps every SLI emission with
/// a ``SLICohort`` so downstream aggregation can slice by any axis.
struct SLICohort: Codable, Sendable, Hashable {
    let trigger: SLITrigger
    let analysisMode: SLIAnalysisMode
    let executionCondition: SLIExecutionCondition
    let durationBucket: SLIEpisodeDurationBucket
}
