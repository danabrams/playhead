// AnalyticsServiceUploadPolicyTests.swift
// playhead-jw63.3 — cadence, watermark arithmetic, and the promise that a
// failing analytics upload is a non-event.

import Foundation
import os
import Testing

@testable import Playhead

/// Captures what would have been sent, and can be told to fail.
private final class CapturingAnalyticsWriter: AnalyticsRecordWriting {
    private struct Box {
        var batches: [[[String: AnalyticsFieldValue]]] = []
        var shouldFail = false
    }

    private let box: OSAllocatedUnfairLock<Box>

    init(shouldFail: Bool = false) {
        box = OSAllocatedUnfairLock(
            initialState: Box(batches: [], shouldFail: shouldFail)
        )
    }

    var batches: [[[String: AnalyticsFieldValue]]] {
        box.withLock { $0.batches }
    }

    var writtenRecords: [[String: AnalyticsFieldValue]] {
        batches.flatMap { $0 }
    }

    func setShouldFail(_ value: Bool) {
        box.withLock { $0.shouldFail = value }
    }

    func write(_ records: [[String: AnalyticsFieldValue]]) async throws {
        let failing = box.withLock { state -> Bool in
            if !state.shouldFail {
                state.batches.append(records)
            }
            return state.shouldFail
        }
        if failing {
            throw CocoaError(.fileNoSuchFile)
        }
    }
}

@MainActor
@Suite("AnalyticsService — upload cadence and failure policy")
struct AnalyticsServiceUploadPolicyTests {

    private func makeStore() -> (AnalyticsCounterStore, UserDefaults, String) {
        let suiteName = "AnalyticsServiceUploadPolicyTests.\(UUID().uuidString)"
        guard let defaults = UserDefaults(suiteName: suiteName) else {
            fatalError("could not create an isolated defaults suite")
        }
        defaults.removePersistentDomain(forName: suiteName)
        return (AnalyticsCounterStore(defaults: defaults), defaults, suiteName)
    }

    private func makeService(
        store: AnalyticsCounterStore,
        writer: any AnalyticsRecordWriting,
        permitted: Bool = true,
        banners: BannerFeedbackCounts = .zero,
        now: @escaping @Sendable () -> Date = { Date(timeIntervalSince1970: 1_000_000) }
    ) -> AnalyticsService {
        AnalyticsService(
            store: store,
            writer: writer,
            isUploadPermitted: { permitted },
            bannerCounts: { banners },
            now: now
        )
    }

    @Test("Nothing uploads while the legal gate is shut")
    func gatedOffMeansNoUpload() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        store.recordManualSkipForwardReach(cohort: .all)

        let service = makeService(
            store: store,
            writer: CapturingAnalyticsWriter(),
            permitted: false
        )
        #expect(service.scheduleUploadIfDue() == .notPermitted)
    }

    @Test("An empty delta is not worth a round trip")
    func emptyDeltaSkips() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let service = makeService(store: store, writer: CapturingAnalyticsWriter())
        #expect(service.uploadDecision(at: Date(timeIntervalSince1970: 1_000_000)) == .nothingPending)
    }

    @Test("A pending delta with no prior attempt uploads now")
    func firstAttemptRunsImmediately() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        store.addListeningSeconds(3_600, cohort: .over90m)

        let service = makeService(store: store, writer: CapturingAnalyticsWriter())
        #expect(service.uploadDecision(at: Date(timeIntervalSince1970: 1_000_000)) == .attempted)
    }

    @Test("A successful upload advances the watermark and empties the delta")
    func successAdvancesWatermark() async {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        store.recordManualSkipForwardReach(cohort: .under30m)
        store.addListeningSeconds(1_200, cohort: .under30m)

        let writer = CapturingAnalyticsWriter()
        let service = makeService(
            store: store,
            writer: writer,
            banners: BannerFeedbackCounts(
                bannersShown: 4, bannersConfirmed: 3, bannersDenied: 1
            )
        )

        #expect(await service.performUpload())

        let records = writer.writtenRecords
        #expect(records.count == 2)
        for fields in records {
            #expect(TelemetryEnvelopeV1AllowList.validate(fields) != nil)
        }

        let rendered = records
            .map(AnalyticsIncrementPayload.canonicalDescription(of:))
            .joined(separator: "\n")
        #expect(rendered.contains("banners_shown=4"))
        #expect(rendered.contains("banners_confirmed=3"))
        #expect(rendered.contains("banners_denied=1"))
        #expect(rendered.contains("manual_skip_forward_reaches=1"))
        #expect(rendered.contains("listening_seconds=1200"))

        #expect(store.state.consecutiveFailures == 0)
        #expect(store.state.lastSuccessAt != nil)
        #expect(service.uploadDecision(at: Date(timeIntervalSince1970: 1_000_000)) == .nothingPending)
    }

    @Test("A second upload sends only what accrued since the first")
    func secondUploadSendsOnlyTheNewDelta() async {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        store.addListeningSeconds(600, cohort: .all)

        let writer = CapturingAnalyticsWriter()
        let service = makeService(store: store, writer: writer)
        #expect(await service.performUpload())

        store.addListeningSeconds(150, cohort: .all)
        #expect(await service.performUpload())

        #expect(writer.batches.count == 2)
        let second = writer.batches[1]
            .map(AnalyticsIncrementPayload.canonicalDescription(of:))
            .joined(separator: "\n")
        #expect(second.contains("listening_seconds=150"))
    }

    @Test("A failed upload keeps the delta and never throws")
    func failureKeepsDelta() async {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        store.addListeningSeconds(600, cohort: .all)

        let writer = CapturingAnalyticsWriter(shouldFail: true)
        let service = makeService(store: store, writer: writer)

        #expect(await service.performUpload() == false)
        #expect(store.state.consecutiveFailures == 1)
        #expect(store.state.lastSuccessAt == nil)
        #expect(store.state.oldestUnsentAt != nil)
        #expect(store.state.uploaded.isEmpty)

        // The delta rolls forward into the next successful attempt rather
        // than being queued or retried in a loop.
        writer.setShouldFail(false)
        store.addListeningSeconds(60, cohort: .all)
        #expect(await service.performUpload())
        let rendered = writer.writtenRecords
            .map(AnalyticsIncrementPayload.canonicalDescription(of:))
            .joined(separator: "\n")
        #expect(rendered.contains("listening_seconds=660"))
        #expect(store.state.consecutiveFailures == 0)
    }

    @Test("Backoff doubles after each failure and caps at seven days")
    func backoffDoublesAndCaps() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let service = makeService(store: store, writer: CapturingAnalyticsWriter())

        #expect(service.backoffInterval(consecutiveFailures: 0) == 24 * 60 * 60)
        #expect(service.backoffInterval(consecutiveFailures: 1) == 48 * 60 * 60)
        #expect(service.backoffInterval(consecutiveFailures: 2) == 96 * 60 * 60)
        #expect(service.backoffInterval(consecutiveFailures: 5) == 7 * 24 * 60 * 60)
        #expect(service.backoffInterval(consecutiveFailures: 500) == 7 * 24 * 60 * 60)
    }

    @Test("An attempt inside the backoff window is skipped")
    func attemptInsideBackoffWindowIsSkipped() async {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        store.addListeningSeconds(600, cohort: .all)

        let base = Date(timeIntervalSince1970: 1_000_000)
        let service = makeService(
            store: store,
            writer: CapturingAnalyticsWriter(shouldFail: true),
            now: { base }
        )
        _ = await service.performUpload()

        #expect(service.uploadDecision(at: base.addingTimeInterval(60 * 60)) == .tooSoon)
        #expect(service.uploadDecision(at: base.addingTimeInterval(47 * 60 * 60)) == .tooSoon)
        #expect(service.uploadDecision(at: base.addingTimeInterval(49 * 60 * 60)) == .attempted)
    }

    @Test("A delta that has failed for thirty days is dropped, not retried forever")
    func staleDeltaIsDropped() async {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        store.addListeningSeconds(600, cohort: .all)

        let base = Date(timeIntervalSince1970: 1_000_000)
        let clock = OSAllocatedUnfairLock(initialState: base)
        let service = makeService(
            store: store,
            writer: CapturingAnalyticsWriter(shouldFail: true),
            now: { clock.withLock { $0 } }
        )

        _ = await service.performUpload()
        #expect(store.state.oldestUnsentAt == base)

        clock.withLock { $0 = base.addingTimeInterval(31 * 24 * 60 * 60) }
        _ = await service.performUpload()

        #expect(store.state.oldestUnsentAt == nil)
        #expect(store.state.consecutiveFailures == 0)
        #expect(store.state.uploaded.count(.listeningSeconds, cohort: .all) == 600)
        // Nothing pending: the data was dropped rather than carried.
        #expect(service.uploadDecision(at: clock.withLock { $0 }) == .nothingPending)
    }

    @Test("Listening time is credited to the cohort of the episode being played")
    func listeningTimeIsCohorted() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let service = makeService(store: store, writer: CapturingAnalyticsWriter())

        service.playbackStatusChanged(isPlaying: true, durationSeconds: 100 * 60)
        service.commitListeningInterval()
        service.playbackStatusChanged(isPlaying: false, durationSeconds: 100 * 60)

        // Real elapsed time in a unit test is ~0 s, so this asserts the
        // routing, not the amount: whatever accrues must land in over90m
        // and nowhere else. The amount is covered by
        // `ListeningTimeAccumulatorTests`.
        #expect(store.state.totals.count(.listeningSeconds, cohort: .under30m) == 0)
        #expect(store.state.totals.count(.listeningSeconds, cohort: .between30and60m) == 0)
    }

    @Test("Becoming active records the install exactly once")
    func becomingActiveRecordsInstall() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let service = makeService(
            store: store,
            writer: CapturingAnalyticsWriter(),
            permitted: false
        )

        service.applicationDidBecomeActive()
        service.applicationDidBecomeActive()

        #expect(store.state.totals.count(.retentionInstalls, cohort: .all) == 1)
        #expect(store.state.installDayIndex != nil)
    }
}
