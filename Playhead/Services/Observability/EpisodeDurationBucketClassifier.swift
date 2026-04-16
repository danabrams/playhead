// EpisodeDurationBucketClassifier.swift
// Buckets an episode duration into one of the four cohort-axis values
// for ``SLIEpisodeDurationBucket``.
//
// Boundary convention (inclusive lower, exclusive upper for the open-ended
// top bucket; exact minute boundaries belong to the LOWER bucket):
//
//   under30m         : duration <  30 * 60
//   between30and60m  : duration >= 30 * 60  AND  duration <= 60 * 60
//   between60and90m  : duration >  60 * 60  AND  duration <= 90 * 60
//   over90m          : duration >  90 * 60
//
// This rule is hand-picked so the "30–90 min episode" reporting scope
// used by several SLIs (`time_to_downloaded`, `time_to_proximal_skip_ready`)
// is the union of `between30and60m` and `between60and90m` — i.e. an
// episode of exactly 30 minutes is in scope, and an episode of exactly
// 90 minutes is in scope, but an episode of 30.1 minutes is not in the
// lower bucket and 90.1 minutes is not in scope at all.

import Foundation

/// Duration cutoffs used by ``EpisodeDurationBucketClassifier``. Constants
/// (not magic numbers) so the boundaries are inspectable from tests.
enum EpisodeDurationBucketThresholds {
    /// 30 minutes expressed in seconds.
    static let thirtyMinutesSeconds: TimeInterval = 30 * 60
    /// 60 minutes expressed in seconds.
    static let sixtyMinutesSeconds: TimeInterval = 60 * 60
    /// 90 minutes expressed in seconds.
    static let ninetyMinutesSeconds: TimeInterval = 90 * 60
}

/// Buckets an episode duration (in seconds) into one of the four cohort
/// values. Pure static function — safe to call from any context.
enum EpisodeDurationBucketClassifier {

    /// Returns the bucket for the given duration in seconds.
    ///
    /// Negative durations (sentinel for "unknown") are bucketed as
    /// `under30m`. Downstream emitters should prefer not emitting at
    /// all when the duration is truly unknown, but the classifier must
    /// be total — see the measurement-rule comment on ``SLI``.
    static func bucket(forDurationSeconds seconds: TimeInterval) -> SLIEpisodeDurationBucket {
        let thirty = EpisodeDurationBucketThresholds.thirtyMinutesSeconds
        let sixty = EpisodeDurationBucketThresholds.sixtyMinutesSeconds
        let ninety = EpisodeDurationBucketThresholds.ninetyMinutesSeconds

        if seconds < thirty {
            return .under30m
        }
        if seconds <= sixty {
            return .between30and60m
        }
        if seconds <= ninety {
            return .between60and90m
        }
        return .over90m
    }
}
