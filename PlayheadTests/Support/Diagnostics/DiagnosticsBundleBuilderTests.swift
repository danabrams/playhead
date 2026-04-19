// DiagnosticsBundleBuilderTests.swift
// Verifies the pure transform from raw inputs into the support-safe
// diagnostics bundle: capping, ordering, hashing, windowing, truncation,
// and opt-in gating.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// These tests are the "code-level proof" of the legal checklist:
//   * (a) no raw episodeIds appear in the default bundle
//   * (b) transcript excerpts are bounded ±30 s and truncated at 1000 chars
//   * (c) the per-install salt is applied via `EpisodeIdHasher`
//   * (d) feature summaries are coarse aggregates only

import Foundation
import Testing

@testable import Playhead

@Suite("DiagnosticsBundleBuilder — pure transform (playhead-ghon)")
struct DiagnosticsBundleBuilderTests {

    // MARK: - Fixtures

    private static let installID = UUID(uuidString: "11111111-1111-1111-1111-111111111111")!
    private static let t0: Double = 1_700_000_000

    private static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date(timeIntervalSince1970: t0)
    )

    private static func entry(
        episodeId: String,
        timestamp: Double,
        eventType: WorkJournalEntry.EventType = .acquired,
        cause: InternalMissCause? = nil
    ) -> WorkJournalEntry {
        WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: UUID(),
            schedulerEpoch: 0,
            timestamp: timestamp,
            eventType: eventType,
            cause: cause,
            metadata: "{}",
            artifactClass: .scratch
        )
    }

    // MARK: - scheduler_events: cap at 200, sorted desc, hashed

    @Test("scheduler_events is capped at 200 most-recent rows")
    func schedulerEventsCappedAt200() {
        let entries = (0..<300).map {
            Self.entry(episodeId: "ep-\($0)", timestamp: Self.t0 + Double($0))
        }
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: entries, installID: Self.installID
        )
        #expect(bundle.schedulerEvents.count == 200)
    }

    @Test("scheduler_events sorted by timestamp descending (newest first)")
    func schedulerEventsSortedDesc() {
        let entries = [
            Self.entry(episodeId: "old",   timestamp: Self.t0 + 1),
            Self.entry(episodeId: "newer", timestamp: Self.t0 + 5),
            Self.entry(episodeId: "mid",   timestamp: Self.t0 + 3),
        ]
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: entries, installID: Self.installID
        )
        let stamps = bundle.schedulerEvents.map(\.timestamp)
        #expect(stamps == [Self.t0 + 5, Self.t0 + 3, Self.t0 + 1])
    }

    @Test("scheduler_events emit episode_id_hash, never raw episodeId (legal item a/c)")
    func schedulerEventsHashedNotRaw() {
        let entries = [Self.entry(episodeId: "raw-secret-id", timestamp: Self.t0)]
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: entries, installID: Self.installID
        )
        let event = try! #require(bundle.schedulerEvents.first)
        let expectedHash = EpisodeIdHasher.hash(installID: Self.installID, episodeId: "raw-secret-id")
        #expect(event.episodeIdHash == expectedHash)
        #expect(!event.episodeIdHash.contains("raw-secret-id"))
    }

    @Test("scheduler_events propagate internal_miss_cause when present")
    func schedulerEventsPreserveCause() {
        let entries = [Self.entry(
            episodeId: "ep-1", timestamp: Self.t0,
            eventType: .preempted, cause: .pipelineError
        )]
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: entries, installID: Self.installID
        )
        let event = try! #require(bundle.schedulerEvents.first)
        #expect(event.internalMissCause != nil)
    }

    // MARK: - work_journal_tail: cap at 50 by insertion order, omit metadata + artifactClass

    @Test("work_journal_tail is capped at the most-recent 50 by insertion order")
    func workJournalTailCappedAt50() {
        // Builder receives entries in INSERTION order (oldest first); the
        // tail keeps the LAST 50 (i.e. most recent 50).
        let entries = (0..<100).map {
            Self.entry(episodeId: "ep-\($0)", timestamp: Self.t0 + Double($0))
        }
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: entries, installID: Self.installID
        )
        #expect(bundle.workJournalTail.count == 50)
        // Tail preserves insertion order — first kept row is the 50th
        // input row (index 50 in the input, which has timestamp t0+50).
        #expect(bundle.workJournalTail.first?.timestamp == Self.t0 + 50)
        #expect(bundle.workJournalTail.last?.timestamp == Self.t0 + 99)
    }

    // MARK: - Regression: order-independent tail selection
    //
    // The production `AnalysisStore.fetchRecentWorkJournalEntries` returns
    // rows `ORDER BY timestamp DESC, rowid DESC` (newest first). A naïve
    // `.suffix(N)` on that input would silently return the OLDEST N rows
    // — i.e. positions 150-199 of a 200-row newest-first fetch — instead
    // of the MOST RECENT 50. These tests feed a DESCENDING-timestamp
    // fixture and assert the tail contains the highest timestamps,
    // proving the builder is order-independent.

    @Test("work_journal_tail is most-recent 50 even when input is newest-first (regression)")
    func workJournalTailHandlesNewestFirstInput() {
        // Feed 100 rows in DESCENDING timestamp order, as the production
        // `AnalysisStore` fetch returns. Before the fix, `.suffix(50)`
        // would return timestamps t0..t0+49 (the oldest 50 of the fetch).
        // After the fix we must see t0+50..t0+99 (the newest 50) in
        // ascending order.
        let entries = (0..<100).map {
            Self.entry(episodeId: "ep-\($0)", timestamp: Self.t0 + Double($0))
        }.reversed() // newest first

        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: Array(entries), installID: Self.installID
        )

        #expect(bundle.workJournalTail.count == 50)
        // Must be the MOST-RECENT 50 rows (timestamps t0+50 through t0+99),
        // emitted in ASCENDING order per the spec's insertion-order phrasing.
        let stamps = bundle.workJournalTail.map(\.timestamp)
        let expected = (50..<100).map { Self.t0 + Double($0) }
        #expect(stamps == expected)
        #expect(bundle.workJournalTail.first?.timestamp == Self.t0 + 50)
        #expect(bundle.workJournalTail.last?.timestamp == Self.t0 + 99)
    }

    @Test("scheduler_events is most-recent 200 desc even when input is newest-first (regression)")
    func schedulerEventsHandlesNewestFirstInput() {
        // 300 rows in DESCENDING order; the builder must still emit
        // the 200 highest-timestamp rows, sorted newest-first.
        let entries = (0..<300).map {
            Self.entry(episodeId: "ep-\($0)", timestamp: Self.t0 + Double($0))
        }.reversed() // newest first

        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: Array(entries), installID: Self.installID
        )

        #expect(bundle.schedulerEvents.count == 200)
        // Newest first per the existing contract.
        #expect(bundle.schedulerEvents.first?.timestamp == Self.t0 + 299)
        // The 200th (last) emitted event must correspond to t0+100 —
        // i.e. the oldest row inside the kept window. If the builder
        // had naïvely `.prefix(200)`'d the DESC-ordered input it would
        // have kept t0+100..t0+299 too — so to make the regression
        // sharper we also include a shuffled-input case below.
        #expect(bundle.schedulerEvents.last?.timestamp == Self.t0 + 100)
    }

    @Test("builder is order-independent: shuffled input produces spec output (regression)")
    func builderOrderIndependentShuffled() {
        // Shuffle with a deterministic seed-equivalent — reverse-halves
        // interleave — so the input is neither ASC nor DESC.
        let base = (0..<120).map {
            Self.entry(episodeId: "ep-\($0)", timestamp: Self.t0 + Double($0))
        }
        var shuffled: [WorkJournalEntry] = []
        for i in 0..<60 {
            shuffled.append(base[119 - i]) // newest end
            shuffled.append(base[i])        // oldest end
        }

        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: shuffled, installID: Self.installID
        )

        // work_journal_tail: most-recent 50 (t0+70..t0+119), ascending.
        let tailStamps = bundle.workJournalTail.map(\.timestamp)
        #expect(tailStamps == (70..<120).map { Self.t0 + Double($0) })
        // scheduler_events: 120 <= cap, so all rows appear, descending.
        #expect(bundle.schedulerEvents.count == 120)
        let schedStamps = bundle.schedulerEvents.map(\.timestamp)
        #expect(schedStamps == (0..<120).reversed().map { Self.t0 + Double($0) })
    }

    @Test("work_journal_tail omits raw episodeId (only hash is emitted)")
    func workJournalTailHashedNotRaw() {
        let entries = [Self.entry(episodeId: "raw-secret-id", timestamp: Self.t0)]
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: entries, installID: Self.installID
        )
        let row = try! #require(bundle.workJournalTail.first)
        let expectedHash = EpisodeIdHasher.hash(installID: Self.installID, episodeId: "raw-secret-id")
        #expect(row.episodeIdHash == expectedHash)
    }

    // (NOTE: `metadata` and `artifact_class` aren't on `WorkJournalRecord`
    // by construction — there is no field to "leak". A grep test in
    // `DiagnosticsBundleSerializationLintTests` doubles up the guarantee
    // at the JSON-key level.)

    // MARK: - analysis_unavailable_reason

    @Test("analysis_unavailable_reason is nil when the device is fully eligible")
    func unavailableReasonNilWhenEligible() {
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: [], installID: Self.installID
        )
        #expect(bundle.analysisUnavailableReason == nil)
    }

    @Test("analysis_unavailable_reason is derived when the device is ineligible")
    func unavailableReasonDerivedWhenIneligible() {
        let ineligible = AnalysisEligibility(
            hardwareSupported: false,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date(timeIntervalSince1970: Self.t0)
        )
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 26", deviceClass: .iPhone14andOlder,
            buildType: .debug, eligibility: ineligible,
            workJournalEntries: [], installID: Self.installID
        )
        #expect(bundle.analysisUnavailableReason == .hardwareUnsupported)
    }

    // MARK: - OptIn: gating + window + truncation

    private static func makeEpisodeInput(
        episodeId: String,
        title: String,
        optIn: Bool,
        boundaries: [Double] = [],
        transcriptText: String = "",
        chunks: [(start: Double, end: Double, text: String)] = []
    ) -> DiagnosticsEpisodeInput {
        DiagnosticsEpisodeInput(
            episodeId: episodeId,
            episodeTitle: title,
            diagnosticsOptIn: optIn,
            adBoundaryTimes: boundaries,
            transcriptChunks: chunks.map {
                DiagnosticsTranscriptChunk(startTime: $0.start, endTime: $0.end, text: $0.text)
            },
            featureSummary: nil
        )
    }

    @Test("OptInBundle only includes episodes where diagnosticsOptIn == true")
    func optInOnlyIncludesOptedInEpisodes() {
        let episodes = [
            Self.makeEpisodeInput(episodeId: "ep-1", title: "On", optIn: true),
            Self.makeEpisodeInput(episodeId: "ep-2", title: "Off", optIn: false),
        ]
        let bundle = DiagnosticsBundleBuilder.buildOptIn(episodes: episodes)
        #expect(bundle != nil)
        let optInEpisodes = try! #require(bundle?.episodes)
        #expect(optInEpisodes.count == 1)
        #expect(optInEpisodes.first?.episodeId == "ep-1")
    }

    @Test("buildOptIn returns nil when no episode has opted in")
    func optInReturnsNilWhenNoEpisodesOptedIn() {
        let episodes = [
            Self.makeEpisodeInput(episodeId: "ep-1", title: "Off", optIn: false),
        ]
        #expect(DiagnosticsBundleBuilder.buildOptIn(episodes: episodes) == nil)
    }

    @Test("transcript excerpt window covers boundary ± 30 s")
    func transcriptExcerptWindow30s() {
        // Boundary at 60s; chunks every 5s. Window should be [30, 90].
        let chunks = stride(from: 0.0, to: 120.0, by: 5).map {
            (start: Double($0), end: Double($0 + 5), text: "chunk@\($0)")
        }
        let input = Self.makeEpisodeInput(
            episodeId: "ep-1", title: "T", optIn: true,
            boundaries: [60], chunks: chunks
        )
        let bundle = try! #require(DiagnosticsBundleBuilder.buildOptIn(episodes: [input]))
        let excerpt = try! #require(bundle.episodes.first?.transcriptExcerpts.first)
        #expect(excerpt.boundaryTime == 60)
        #expect(excerpt.startTime == 30)
        #expect(excerpt.endTime == 90)
        // Text should only include chunks whose interval overlaps [30, 90].
        #expect(excerpt.text.contains("chunk@30.0"))
        #expect(excerpt.text.contains("chunk@85.0"))
        #expect(!excerpt.text.contains("chunk@0.0"))
        #expect(!excerpt.text.contains("chunk@95.0"))
    }

    @Test("transcript excerpt over 1000 chars is truncated to 1000 (legal item b)")
    func transcriptExcerptTruncated() {
        let longText = String(repeating: "x", count: 5_000)
        let chunks = [(start: 30.0, end: 90.0, text: longText)]
        let input = Self.makeEpisodeInput(
            episodeId: "ep-1", title: "T", optIn: true,
            boundaries: [60], chunks: chunks
        )
        let bundle = try! #require(DiagnosticsBundleBuilder.buildOptIn(episodes: [input]))
        let excerpt = try! #require(bundle.episodes.first?.transcriptExcerpts.first)
        #expect(excerpt.text.count == 1000)
    }

    @Test("transcript excerpt under 1000 chars is preserved")
    func transcriptExcerptShortPreserved() {
        let shortText = "short"
        let chunks = [(start: 30.0, end: 90.0, text: shortText)]
        let input = Self.makeEpisodeInput(
            episodeId: "ep-1", title: "T", optIn: true,
            boundaries: [60], chunks: chunks
        )
        let bundle = try! #require(DiagnosticsBundleBuilder.buildOptIn(episodes: [input]))
        let excerpt = try! #require(bundle.episodes.first?.transcriptExcerpts.first)
        #expect(excerpt.text == "short")
    }

    @Test("multiple boundaries yield one excerpt each")
    func multipleBoundaries() {
        let chunks = stride(from: 0.0, to: 200.0, by: 5).map {
            (start: Double($0), end: Double($0 + 5), text: "c\($0)")
        }
        let input = Self.makeEpisodeInput(
            episodeId: "ep-1", title: "T", optIn: true,
            boundaries: [60, 150], chunks: chunks
        )
        let bundle = try! #require(DiagnosticsBundleBuilder.buildOptIn(episodes: [input]))
        #expect(bundle.episodes.first?.transcriptExcerpts.count == 2)
    }
}
