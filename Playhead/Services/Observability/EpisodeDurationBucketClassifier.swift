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
    ///
    /// Non-finite inputs (NaN, +Inf, -Inf) are bucketed as `over90m`.
    /// This preserves the de-facto behavior of the original `<` /
    /// `<=` chain (NaN compares false against everything; +Inf falls
    /// through every upper bound) while making the choice explicit and
    /// testable. The mapping is documented rather than escalated to a
    /// new `case malformed` so this fix doesn't force every existing
    /// caller to handle a new variant — that escalation can happen
    /// later if non-finite inputs become a real source of skew.
    ///
    /// In DEBUG builds, non-finite inputs additionally fire an
    /// `assertionFailure`: they can only arise from upstream bugs (a
    /// duration arithmetic error, an unconfigured probe), and silently
    /// folding them into `.over90m` in dev hides those bugs. Tests that
    /// intentionally exercise the non-finite path should call
    /// `bucketIgnoringNonFiniteAssertion(forDurationSeconds:)`.
    static func bucket(forDurationSeconds seconds: TimeInterval) -> SLIEpisodeDurationBucket {
        if !seconds.isFinite {
            #if DEBUG
            assertionFailure(
                "EpisodeDurationBucketClassifier received non-finite seconds: \(seconds)"
            )
            #endif
            return .over90m
        }
        return _bucketAssumingFinite(seconds: seconds)
    }

    #if DEBUG
    /// Test-only entrypoint: returns the same bucket as
    /// `bucket(forDurationSeconds:)` but never fires the DEBUG
    /// `assertionFailure` for non-finite inputs. Intended for tests that
    /// explicitly verify the documented non-finite → `.over90m` fallback.
    /// Compiled out of Release so production code cannot bypass the safety
    /// net by routing through this helper.
    static func bucketIgnoringNonFiniteAssertion(
        forDurationSeconds seconds: TimeInterval
    ) -> SLIEpisodeDurationBucket {
        if !seconds.isFinite {
            return .over90m
        }
        return _bucketAssumingFinite(seconds: seconds)
    }
    #endif

    /// Shared bucketing body for finite inputs. Both the production and the
    /// DEBUG-only test entrypoints route through this so the two paths can
    /// never drift on the finite-domain logic.
    private static func _bucketAssumingFinite(
        seconds: TimeInterval
    ) -> SLIEpisodeDurationBucket {
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
