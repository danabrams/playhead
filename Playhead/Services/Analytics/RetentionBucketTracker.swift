// RetentionBucketTracker.swift
// playhead-jw63.3 — retention without device identifiers.
//
// The question is "do listeners come back tomorrow?", and the obvious way
// to answer it is to stamp every session with a device id and count
// distinct ids per day on a server. That is per-user behavior history and
// the telemetry envelope forbids it (§1 rule 3, §4 items 2 and 6).
//
// So the device answers the question about itself, locally, and uploads
// only the answer:
//
//   * On first launch it records `installDayIndex` — a local integer, days
//     since the reference date in the device's own calendar. It never
//     leaves the device, in any form, ever.
//   * On every later foreground it compares today's day index against that
//     one and, the first time the gap reaches 1 / 7 / 30 days, marks the
//     corresponding bucket as reached.
//   * What uploads is `+1` on `retention_d1_returned` (etc.), once per
//     install per bucket, alongside a `+1` on `retention_installs` from
//     first launch. The rollup divides one by the other.
//
// The result carries no identifier, no timestamp, no session count and no
// install date, so two records from the same device are unlinkable — which
// is exactly the property that makes the increments aggregate-only.
//
// The cost of that property, stated plainly: because nothing links a return
// to its install, the ratio is a *window* ratio (returns observed this
// week ÷ installs observed this week), not a true cohort ratio. It is
// stable when install volume is stable and it lags when install volume
// swings. Getting a true cohort ratio would require carrying an install
// week on the record, and an install-week bucket is a linkage surface for
// a bounded population — not worth it at v1 scale. Revisit if install
// volume ever moves fast enough for the lag to mislead.
//
// Buckets are cumulative ladders, not exclusive: a listener whose first
// return is on day 10 satisfies D1 and D7 at once. "Came back by day N or
// later" is the question the north star needs answered.

import Foundation

/// Computes local return buckets and folds them into the persistent state.
enum RetentionBucketTracker {

    /// Bucket ladder: the day gap at which each metric becomes reachable.
    static let ladder: [(days: Int, metric: AnalyticsMetricKey)] = [
        (1, .retentionD1Returned),
        (7, .retentionD7Returned),
        (30, .retentionD30Returned),
    ]

    /// Days since the reference date, in the device's local calendar. Local
    /// midnight is the right boundary because "came back tomorrow" is a
    /// human claim about the listener's day, not about UTC.
    static func dayIndex(
        for date: Date,
        calendar: Calendar = .current
    ) -> Int {
        let origin = calendar.startOfDay(for: Date(timeIntervalSinceReferenceDate: 0))
        let today = calendar.startOfDay(for: date)
        return calendar.dateComponents([.day], from: origin, to: today).day ?? 0
    }

    /// Records first launch and any newly-reached return buckets.
    ///
    /// Idempotent within a day and across launches: each metric increments
    /// at most once per install. Returns the metrics it counted, for
    /// logging and tests.
    @discardableResult
    static func apply(
        to state: inout AnalyticsPersistentState,
        today: Int
    ) -> [AnalyticsMetricKey] {
        var counted: [AnalyticsMetricKey] = []

        guard let installDay = state.installDayIndex else {
            state.installDayIndex = today
            if mark(.retentionInstalls, in: &state) {
                counted.append(.retentionInstalls)
            }
            return counted
        }

        // A device clock that moved backwards past install day would
        // otherwise freeze the ladder forever. Re-anchor to today rather
        // than counting a second install — an install is a fact about this
        // install, not about the clock.
        guard today >= installDay else {
            state.installDayIndex = today
            return counted
        }

        let gap = today - installDay
        for rung in ladder where gap >= rung.days {
            if mark(rung.metric, in: &state) {
                counted.append(rung.metric)
            }
        }
        return counted
    }

    /// Marks a retention metric as counted, incrementing exactly once.
    /// Returns whether this call was the one that counted it.
    private static func mark(
        _ metric: AnalyticsMetricKey,
        in state: inout AnalyticsPersistentState
    ) -> Bool {
        guard !state.reportedRetentionMetrics.contains(metric.rawValue) else {
            return false
        }
        state.reportedRetentionMetrics.append(metric.rawValue)
        state.totals.add(1, to: metric, cohort: .all)
        return true
    }
}
