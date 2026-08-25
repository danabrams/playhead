// AnalyticsCounterStoreTests.swift
// playhead-jw63.3 — durable totals, watermark arithmetic, and the shape of
// what lands on disk.

import Foundation
import Testing

@testable import Playhead

@Suite("AnalyticsCounterStore — local aggregate totals")
struct AnalyticsCounterStoreTests {

    private func makeStore() -> (AnalyticsCounterStore, UserDefaults, String) {
        let suiteName = "AnalyticsCounterStoreTests.\(UUID().uuidString)"
        guard let defaults = UserDefaults(suiteName: suiteName) else {
            fatalError("could not create an isolated defaults suite")
        }
        defaults.removePersistentDomain(forName: suiteName)
        return (AnalyticsCounterStore(defaults: defaults), defaults, suiteName)
    }

    @Test("A fresh store is empty")
    func freshStoreIsEmpty() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        #expect(store.state.totals.isEmpty)
        #expect(store.state.installDayIndex == nil)
        #expect(store.state.consecutiveFailures == 0)
    }

    @Test("Reaches and listening seconds accumulate per cohort")
    func countersAccumulatePerCohort() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        store.recordManualSkipForwardReach(cohort: .under30m)
        store.recordManualSkipForwardReach(cohort: .under30m)
        store.recordManualSkipForwardReach(cohort: .over90m)
        store.addListeningSeconds(600, cohort: .under30m)
        store.addListeningSeconds(120, cohort: .under30m)

        let totals = store.state.totals
        #expect(totals.count(.manualSkipForwardReaches, cohort: .under30m) == 2)
        #expect(totals.count(.manualSkipForwardReaches, cohort: .over90m) == 1)
        #expect(totals.count(.listeningSeconds, cohort: .under30m) == 720)
        #expect(totals.count(.listeningSeconds, cohort: .over90m) == 0)
    }

    @Test("Non-positive listening seconds are ignored")
    func nonPositiveSecondsIgnored() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        store.addListeningSeconds(0, cohort: .all)
        store.addListeningSeconds(-30, cohort: .all)
        #expect(store.state.totals.isEmpty)
    }

    @Test("Totals survive a reopen and persist as a single key")
    func totalsPersistAcrossReopen() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        store.recordManualSkipForwardReach(cohort: .between60and90m)
        store.addListeningSeconds(45, cohort: .between60and90m)

        let reopened = AnalyticsCounterStore(defaults: defaults)
        #expect(
            reopened.state.totals.count(
                .manualSkipForwardReaches, cohort: .between60and90m
            ) == 1
        )
        #expect(
            reopened.state.totals.count(.listeningSeconds, cohort: .between60and90m) == 45
        )

        let domain = defaults.persistentDomain(forName: suiteName) ?? [:]
        #expect(domain.keys.sorted() == ["playhead.analytics.aggregate.v1"])
    }

    @Test("Counts saturate rather than overflowing")
    func countsSaturate() {
        var totals = AnalyticsCounterTotals()
        totals.set(Int.max, for: .listeningSeconds, cohort: .all)
        totals.add(5, to: .listeningSeconds, cohort: .all)
        #expect(totals.count(.listeningSeconds, cohort: .all) == Int.max)
    }

    @Test("Delta is the un-uploaded remainder and never goes negative")
    func deltaIsUnsentRemainder() {
        var totals = AnalyticsCounterTotals()
        totals.set(10, for: .bannersShown, cohort: .all)
        totals.set(4, for: .manualSkipForwardReaches, cohort: .under30m)

        var watermark = AnalyticsCounterTotals()
        watermark.set(6, for: .bannersShown, cohort: .all)
        // A watermark ahead of the totals — a restored backup, or a store
        // cleared without clearing the watermark.
        watermark.set(9, for: .manualSkipForwardReaches, cohort: .under30m)

        let delta = totals.delta(since: watermark)
        #expect(delta.count(.bannersShown, cohort: .all) == 4)
        #expect(delta.count(.manualSkipForwardReaches, cohort: .under30m) == 0)
    }

    @Test("Merging takes the larger of each pair")
    func mergingTakesMaximum() {
        var left = AnalyticsCounterTotals()
        left.set(5, for: .bannersShown, cohort: .all)
        left.set(2, for: .bannersDenied, cohort: .all)

        var right = AnalyticsCounterTotals()
        right.set(3, for: .bannersShown, cohort: .all)
        right.set(7, for: .bannersConfirmed, cohort: .all)

        let merged = left.merging(right)
        #expect(merged.count(.bannersShown, cohort: .all) == 5)
        #expect(merged.count(.bannersDenied, cohort: .all) == 2)
        #expect(merged.count(.bannersConfirmed, cohort: .all) == 7)
    }

    @Test("Populated pairs are deterministically ordered")
    func populatedPairsAreOrdered() {
        var totals = AnalyticsCounterTotals()
        totals.set(1, for: .retentionD7Returned, cohort: .all)
        totals.set(2, for: .bannersShown, cohort: .over90m)
        totals.set(3, for: .bannersShown, cohort: .all)

        let keys = totals.populatedPairs.map { "\($0.metric.rawValue)/\($0.cohort.rawValue)" }
        #expect(keys == ["banners_shown/all", "banners_shown/over90m", "retention_d7_returned/all"])
    }

    @Test("A corrupt blob degrades to an empty store rather than throwing")
    func corruptBlobDegradesGracefully() {
        let (_, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        defaults.set(Data("not json".utf8), forKey: "playhead.analytics.aggregate.v1")
        let store = AnalyticsCounterStore(defaults: defaults)
        #expect(store.state.totals.isEmpty)
    }

    @Test("The shared store is volatile under XCTest")
    func sharedStoreIsTestIsolated() {
        // If this ever binds to `.standard`, a test run would write into the
        // developer's real counters and read them back into assertions.
        //
        // THIS ASSERTS A DELTA, NOT AN ABSOLUTE, AND THE DIFFERENCE IS THE
        // WHOLE TEST (playhead-vk68m). It used to require that `.standard` hold
        // NO value for the key — a property of the DEVICE's whole history, not
        // of this code. The simulator's `com.playhead.app.plist` has carried
        // that key since some earlier launch (verified on the box: PlistBuddy
        // finds it), UserDefaults persists across runs and across `simctl
        // erase` of nothing in particular, so the test failed on every run on
        // this device — in the parallel plan and the serialized one alike, and
        // on two other branches' preserved gate logs — while the code it names
        // was working correctly. A value that names one thing (has anything
        // ever written this key?) read as though it named another (did the
        // shared store just write it?).
        //
        // What the comment above actually claims is that THIS call does not
        // reach `.standard`, and that is a before/after comparison.
        let key = "playhead.analytics.aggregate.v1"
        let before = UserDefaults.standard.data(forKey: key)
        AnalyticsCounterStore.shared.recordManualSkipForwardReach(cohort: .all)
        let after = UserDefaults.standard.data(forKey: key)
        #expect(
            after == before,
            """
            AnalyticsCounterStore.shared wrote into UserDefaults.standard: \
            \(before?.count ?? -1) bytes -> \(after?.count ?? -1) bytes
            """
        )
    }
}
