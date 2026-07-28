// RetentionBucketTrackerTests.swift
// playhead-jw63.3 — retention computed locally, uploaded as counters, with
// no device identifier anywhere in the mechanism.

import Foundation
import Testing

@testable import Playhead

@Suite("RetentionBucketTracker — D1/D7/D30 without device IDs")
struct RetentionBucketTrackerTests {

    @Test("First launch records an install and nothing else")
    func firstLaunchRecordsInstall() {
        var state = AnalyticsPersistentState()
        let counted = RetentionBucketTracker.apply(to: &state, today: 9_000)

        #expect(counted == [.retentionInstalls])
        #expect(state.installDayIndex == 9_000)
        #expect(state.totals.count(.retentionInstalls, cohort: .all) == 1)
        #expect(state.totals.count(.retentionD1Returned, cohort: .all) == 0)
    }

    @Test("Re-opening on the install day counts nothing further")
    func sameDayReopenIsInert() {
        var state = AnalyticsPersistentState()
        RetentionBucketTracker.apply(to: &state, today: 9_000)
        let counted = RetentionBucketTracker.apply(to: &state, today: 9_000)

        #expect(counted.isEmpty)
        #expect(state.totals.count(.retentionInstalls, cohort: .all) == 1)
    }

    @Test("Coming back the next day counts D1, once")
    func nextDayCountsD1Once() {
        var state = AnalyticsPersistentState()
        RetentionBucketTracker.apply(to: &state, today: 9_000)

        #expect(RetentionBucketTracker.apply(to: &state, today: 9_001) == [.retentionD1Returned])
        #expect(RetentionBucketTracker.apply(to: &state, today: 9_002).isEmpty)
        #expect(state.totals.count(.retentionD1Returned, cohort: .all) == 1)
    }

    @Test("The ladder is cumulative — a first return on day 10 counts D1 and D7")
    func ladderIsCumulative() {
        var state = AnalyticsPersistentState()
        RetentionBucketTracker.apply(to: &state, today: 100)

        let counted = RetentionBucketTracker.apply(to: &state, today: 110)
        #expect(counted == [.retentionD1Returned, .retentionD7Returned])
        #expect(state.totals.count(.retentionD30Returned, cohort: .all) == 0)

        #expect(RetentionBucketTracker.apply(to: &state, today: 130) == [.retentionD30Returned])
        #expect(state.totals.count(.retentionD30Returned, cohort: .all) == 1)
    }

    @Test("Each bucket increments at most once per install")
    func eachBucketIsOneShot() {
        var state = AnalyticsPersistentState()
        RetentionBucketTracker.apply(to: &state, today: 0)
        for day in 1...60 {
            RetentionBucketTracker.apply(to: &state, today: day)
        }

        #expect(state.totals.count(.retentionInstalls, cohort: .all) == 1)
        #expect(state.totals.count(.retentionD1Returned, cohort: .all) == 1)
        #expect(state.totals.count(.retentionD7Returned, cohort: .all) == 1)
        #expect(state.totals.count(.retentionD30Returned, cohort: .all) == 1)
    }

    @Test("A clock that jumps backwards re-anchors instead of re-counting an install")
    func backwardsClockReAnchors() {
        var state = AnalyticsPersistentState()
        RetentionBucketTracker.apply(to: &state, today: 500)

        let counted = RetentionBucketTracker.apply(to: &state, today: 400)
        #expect(counted.isEmpty)
        #expect(state.installDayIndex == 400)
        #expect(state.totals.count(.retentionInstalls, cohort: .all) == 1)

        #expect(RetentionBucketTracker.apply(to: &state, today: 401) == [.retentionD1Returned])
    }

    @Test("The day index advances by exactly one across local midnight")
    func dayIndexAdvancesAcrossMidnight() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = try #require(TimeZone(identifier: "America/Los_Angeles"))

        let formatter = ISO8601DateFormatter()
        formatter.timeZone = calendar.timeZone
        formatter.formatOptions = [.withInternetDateTime]

        let lateEvening = try #require(formatter.date(from: "2026-07-24T23:30:00-07:00"))
        let earlyMorning = try #require(formatter.date(from: "2026-07-25T00:30:00-07:00"))

        let before = RetentionBucketTracker.dayIndex(for: lateEvening, calendar: calendar)
        let after = RetentionBucketTracker.dayIndex(for: earlyMorning, calendar: calendar)
        #expect(after - before == 1)
    }

    @Test("Retention state carries no identifier and no install date off-device")
    func retentionUploadsCountersOnly() {
        var state = AnalyticsPersistentState()
        RetentionBucketTracker.apply(to: &state, today: 12_345)
        RetentionBucketTracker.apply(to: &state, today: 12_400)

        let rendered = AnalyticsIncrementPayload
            .records(for: state.totals.delta(since: AnalyticsCounterTotals()))
            .map(AnalyticsIncrementPayload.canonicalDescription(of:))
            .joined(separator: "\n")

        #expect(rendered.contains("retention_installs=1"))
        #expect(rendered.contains("retention_d1_returned=1"))
        // The local install day index is the one thing that must never
        // appear in a payload — it would date-fingerprint the device.
        #expect(!rendered.contains("12345"))
        #expect(!rendered.contains("12400"))
        #expect(!rendered.contains("installDayIndex"))
    }
}
