// AnalyticsEgressSentinelTests.swift
// playhead-jw63.3 — the on-device mandate, made falsifiable.
//
// The claim under test: no audio, transcript, or episode content can reach
// an outbound analytics record. A privacy claim without a test that could
// fail is not a claim, so this suite seeds distinctive sentinels at every
// place content could enter the pipeline and asserts, over the *entire*
// encoded payload rather than over fields the test happened to think of,
// that none of them come out.
//
// Three entry vectors are covered:
//   1. A caller handing analytics a full episode context (title, show,
//      feed URL, episode id, ad-window transcript) at the +30s call site.
//   2. A persisted blob that already contains a poisoned key — a corrupt
//      store, a hand-edited plist, a future build with a wider vocabulary.
//   3. Direct construction of an outbound CloudKit record.

import CloudKit
import Foundation
import Testing

@testable import Playhead

@Suite("Analytics egress — content sentinels never leave the device")
struct AnalyticsEgressSentinelTests {

    private static let sentinelEpisodeTitle = "SENTINEL-EPISODE-Kelly-Ripa-Interview"
    private static let sentinelShowTitle = "SENTINEL-SHOW-Diary-Of-A-CEO"
    private static let sentinelFeedURL = "https://sentinel.example.com/feed/rss.xml"
    private static let sentinelEpisodeId = "SENTINEL-EPISODE-ID-buzzsprout-14287739"
    private static let sentinelTranscript =
        "SENTINEL-TRANSCRIPT this episode is brought to you by a sponsor"

    private static var allSentinels: [String] {
        [
            sentinelEpisodeTitle, sentinelShowTitle, sentinelFeedURL,
            sentinelEpisodeId, sentinelTranscript,
        ]
    }

    private static func poisonedContext(
        durationSeconds: TimeInterval = 45 * 60
    ) -> EpisodeAnalyticsContext {
        EpisodeAnalyticsContext(
            durationSeconds: durationSeconds,
            episodeId: sentinelEpisodeId,
            episodeTitle: sentinelEpisodeTitle,
            showTitle: sentinelShowTitle,
            feedURL: sentinelFeedURL,
            adWindowTranscript: sentinelTranscript
        )
    }

    private func makeStore() -> (AnalyticsCounterStore, UserDefaults, String) {
        let suiteName = "AnalyticsEgressSentinelTests.\(UUID().uuidString)"
        guard let defaults = UserDefaults(suiteName: suiteName) else {
            fatalError("could not create an isolated defaults suite")
        }
        defaults.removePersistentDomain(forName: suiteName)
        return (AnalyticsCounterStore(defaults: defaults), defaults, suiteName)
    }

    /// Everything an outbound batch would carry, as one string.
    private func renderedPayload(for delta: AnalyticsCounterTotals) -> String {
        AnalyticsIncrementPayload.records(for: delta)
            .map(AnalyticsIncrementPayload.canonicalDescription(of:))
            .joined(separator: "\n")
    }

    @Test("Episode context seeded at the +30s call site never reaches a record")
    func episodeContextIsDroppedBeforeEgress() {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        AnalyticsRecorder.manualSkipForwardReach(
            context: Self.poisonedContext(),
            store: store
        )

        let delta = store.state.totals.delta(since: AnalyticsCounterTotals())
        let rendered = renderedPayload(for: delta)

        #expect(!rendered.isEmpty, "the counter must actually have been recorded")
        #expect(rendered.contains("manual_skip_forward_reaches=1"))
        #expect(rendered.contains("cohort_duration_bucket=between30and60m"))
        for sentinel in Self.allSentinels {
            #expect(
                !rendered.contains(sentinel),
                "outbound payload leaked \(sentinel)"
            )
        }
    }

    @Test("The sentinel is also absent from the raw persisted blob")
    func episodeContextNeverReachesDisk() throws {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        AnalyticsRecorder.manualSkipForwardReach(
            context: Self.poisonedContext(),
            store: store
        )

        let domain = defaults.persistentDomain(forName: suiteName) ?? [:]
        let blob = try #require(domain["playhead.analytics.aggregate.v1"] as? Data)
        let text = try #require(String(data: blob, encoding: .utf8))
        for sentinel in Self.allSentinels {
            #expect(!text.contains(sentinel), "on-disk state leaked \(sentinel)")
        }
    }

    @Test("A poisoned persisted blob cannot smuggle a key into a record")
    func poisonedStoredStateIsNormalizedAway() throws {
        let (_, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        // A blob such as a corrupt store, a hand-edited plist, or one
        // written by a future build with a wider vocabulary: one legitimate
        // counter next to an unknown metric and an unknown cohort whose
        // names are episode content.
        let poisoned: [String: Any] = [
            "totals": [
                "manual_skip_forward_reaches": ["over90m": 4],
                Self.sentinelEpisodeTitle: ["all": 9],
                "listening_seconds": [Self.sentinelShowTitle: 900],
            ],
            "uploaded": [:],
            "reportedRetentionMetrics": [Self.sentinelFeedURL, "retention_installs"],
            "consecutiveFailures": 0,
        ]
        let blob = try JSONSerialization.data(withJSONObject: poisoned)
        defaults.set(blob, forKey: "playhead.analytics.aggregate.v1")

        let reopened = AnalyticsCounterStore(defaults: defaults)
        let state = reopened.state

        #expect(state.totals.count(.manualSkipForwardReaches, cohort: .over90m) == 4)
        #expect(state.totals.count(.listeningSeconds, cohort: .all) == 0)
        #expect(state.reportedRetentionMetrics == ["retention_installs"])

        let rendered = renderedPayload(
            for: state.totals.delta(since: AnalyticsCounterTotals())
        )
        #expect(rendered.contains("manual_skip_forward_reaches=4"))
        for sentinel in Self.allSentinels {
            #expect(!rendered.contains(sentinel), "normalization let \(sentinel) through")
        }
    }

    @Test("A CloudKit record refuses to carry a non-allow-listed field")
    func cloudKitRecordRefusesPoisonedFields() {
        let poisoned: [String: AnalyticsFieldValue] = [
            "envelope_version": .integer(1),
            "payload_schema": .token("playhead.analytics.increment.v1"),
            "cohort_duration_bucket": .token("all"),
            "episode_title": .integer(1),
        ]
        #expect(throws: AnalyticsWriterError.recordRejectedByEnvelope) {
            _ = try CloudKitPublicAnalyticsWriter.makeRecord(fields: poisoned)
        }
    }

    @Test("A materialized CloudKit record carries only allow-listed keys")
    func cloudKitRecordKeysAreAllowListed() throws {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        AnalyticsRecorder.manualSkipForwardReach(
            context: Self.poisonedContext(durationSeconds: 20 * 60),
            store: store
        )
        store.addListeningSeconds(1_800, cohort: .under30m)

        let records = AnalyticsIncrementPayload.records(
            for: store.state.totals.delta(since: AnalyticsCounterTotals())
        )
        #expect(records.count == 1)

        for fields in records {
            let record = try CloudKitPublicAnalyticsWriter.makeRecord(fields: fields)
            #expect(record.recordType == "AnalyticsIncrement")
            for key in record.allKeys() {
                #expect(
                    TelemetryEnvelopeV1AllowList.permittedKeys.contains(key),
                    "record carried non-allow-listed key \(key)"
                )
            }
            let rendered = record.allKeys()
                .map { "\($0)=\(String(describing: record[$0]))" }
                .joined(separator: "\n")
            for sentinel in Self.allSentinels {
                #expect(!rendered.contains(sentinel), "CKRecord leaked \(sentinel)")
            }
            // The record name must not be a stable device identifier —
            // envelope §4.6. A fresh UUID per record is what makes two
            // uploads from one device unlinkable.
            #expect(UUID(uuidString: record.recordID.recordName) != nil)
        }
    }

    @Test("Cohort resolution reads duration and nothing else")
    func cohortResolutionIsDurationOnly() {
        let base = Self.poisonedContext(durationSeconds: 100 * 60)
        #expect(AnalyticsCohortResolver.cohort(for: base) == .over90m)

        let clean = EpisodeAnalyticsContext(durationSeconds: 100 * 60)
        #expect(AnalyticsCohortResolver.cohort(for: clean) == .over90m)

        // Unknown / not-yet-resolved durations fall back to the uncohorted
        // bucket rather than being bucketed as a very short episode.
        #expect(AnalyticsCohortResolver.cohort(forDurationSeconds: 0) == .all)
        #expect(AnalyticsCohortResolver.cohort(forDurationSeconds: -1) == .all)
        #expect(AnalyticsCohortResolver.cohort(forDurationSeconds: .nan) == .all)
        #expect(AnalyticsCohortResolver.cohort(forDurationSeconds: .infinity) == .all)
    }
}
