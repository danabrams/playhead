// AnalyticsCohort.swift
// playhead-jw63.3 — the one place episode-shaped data is allowed to touch
// analytics, and the place it is thrown away.
//
// `EpisodeAnalyticsContext` deliberately carries everything a real call
// site already holds when it wants to count something: the episode id, the
// episode and show titles, the feed URL, and (at an ad banner) the ad
// window's transcript text. None of it is on the envelope allow-list for
// automatic upload. Carrying it here and dropping it here is the point:
// the temptation to "just add the show name so we can segment" is a real
// one, and this type makes the drop explicit, single-sited, and testable.
//
// `AnalyticsCohortResolver.cohort(for:)` reads exactly one property —
// `durationSeconds` — and returns a closed enum. Everything else is
// unreachable from the outbound path.
//
// `AnalyticsEgressSentinelTests` seeds every one of these fields with a
// distinctive sentinel and asserts none of them appear anywhere in the
// encoded outbound record. If someone later widens the resolver, that test
// fails — which is the only reason to believe the claim in this comment.

import Foundation

/// Episode-shaped context a caller may hand to analytics. Everything on it
/// except `durationSeconds` is discarded by `AnalyticsCohortResolver`.
struct EpisodeAnalyticsContext: Sendable, Equatable {
    /// Episode duration in seconds. The only field that survives.
    let durationSeconds: TimeInterval
    /// Prohibited beyond this boundary (envelope §2.2 — mail composer only).
    let episodeId: String?
    /// Prohibited beyond this boundary (envelope §2.2).
    let episodeTitle: String?
    /// Prohibited beyond this boundary (envelope §2.2).
    let showTitle: String?
    /// Prohibited beyond this boundary (envelope §2.2 / §4.6 combination risk).
    let feedURL: String?
    /// Prohibited beyond this boundary, always (envelope §4.1 — transcript
    /// text may not leave the device in any form, opt-in or not).
    let adWindowTranscript: String?

    init(
        durationSeconds: TimeInterval,
        episodeId: String? = nil,
        episodeTitle: String? = nil,
        showTitle: String? = nil,
        feedURL: String? = nil,
        adWindowTranscript: String? = nil
    ) {
        self.durationSeconds = durationSeconds
        self.episodeId = episodeId
        self.episodeTitle = episodeTitle
        self.showTitle = showTitle
        self.feedURL = feedURL
        self.adWindowTranscript = adWindowTranscript
    }
}

/// Reduces episode-shaped context to a single closed-vocabulary cohort key.
enum AnalyticsCohortResolver {

    /// The cohort for a context. Reads `durationSeconds` and nothing else.
    ///
    /// A non-finite or non-positive duration resolves to `.all` rather than
    /// to a duration bucket: "unknown" is not a duration cohort, and
    /// `EpisodeDurationBucketClassifier` fires a DEBUG assertion on
    /// non-finite input because for the SLI pipeline that can only mean an
    /// upstream arithmetic bug. Analytics sees legitimately-unknown
    /// durations (a stream whose duration has not resolved yet), so it
    /// screens them here instead of routing a known-bad value into a
    /// classifier that is entitled to trap on it.
    static func cohort(for context: EpisodeAnalyticsContext) -> AnalyticsCohortKey {
        cohort(forDurationSeconds: context.durationSeconds)
    }

    /// Duration-only entry point for call sites that never had an episode
    /// object to begin with (the remote-command transport path).
    static func cohort(forDurationSeconds seconds: TimeInterval) -> AnalyticsCohortKey {
        guard seconds.isFinite, seconds > 0 else { return .all }
        return AnalyticsCohortKey(
            durationBucket: EpisodeDurationBucketClassifier
                .bucket(forDurationSeconds: seconds)
        )
    }
}

/// Non-isolated recording entry points for counters whose call sites live
/// in different isolation domains.
///
/// `PlayheadRuntime.skipForward()` (the in-app button) is `@MainActor` and
/// `PlaybackService.skipForward(_:)` (the lock-screen / Control Centre /
/// CarPlay / AirPods remote command) is `@PlaybackServiceActor`. They are
/// disjoint — the runtime computes its own seek target rather than calling
/// through to the transport — so instrumenting both counts each reach once
/// and neither counts an ad auto-skip, which never touches either function.
/// `AnalyticsSkipPathCanaryTests` pins that disjointness so a future
/// refactor cannot silently double-count.
enum AnalyticsRecorder {

    /// The listener reached for +30s, with full episode context available.
    static func manualSkipForwardReach(
        context: EpisodeAnalyticsContext,
        store: AnalyticsCounterStore = .shared
    ) {
        store.recordManualSkipForwardReach(
            cohort: AnalyticsCohortResolver.cohort(for: context)
        )
    }

    /// The listener reached for +30s, from a call site that only knows the
    /// current item's duration.
    static func manualSkipForwardReach(
        durationSeconds: TimeInterval,
        store: AnalyticsCounterStore = .shared
    ) {
        store.recordManualSkipForwardReach(
            cohort: AnalyticsCohortResolver.cohort(forDurationSeconds: durationSeconds)
        )
    }
}
