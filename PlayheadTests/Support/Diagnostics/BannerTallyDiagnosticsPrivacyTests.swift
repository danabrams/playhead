// BannerTallyDiagnosticsPrivacyTests.swift
// The privacy proof for the per-episode banner tally (playhead-bfq7),
// legal checklist item (g).
//
// The tally is the first counter that is BOTH per-episode AND rides an
// egress surface, so it is the first one that could carry an episode
// reference off the device. The store deliberately keeps the RAW
// episode id (it is local, and the `os_log` breadcrumb names the
// episode plainly by design); everything therefore rests on
// `DiagnosticsBundleBuilder` being the only projection and hashing on
// the way through.
//
// The claim under test is three-part, modelled on
// `StabilityDiagnosticScrubbingTests` (playhead-jw63.4):
//
//   1. SENTINEL SWEEP — a store row whose episode id is stuffed with an
//      episode title, a feed URL, and transcript text projects into a
//      bundle whose encoded bytes contain none of them, while still
//      being COUNTED.
//   2. CLOSED SHAPE — the summary's key set is exactly the declared
//      `CodingKeys`. A future free-text field cannot be added without
//      turning this red.
//   3. SEAM — the whole pipeline (store → fetch → builder → encode)
//      actually carries the number, because a counter that is written
//      and never read is the usual way this breaks.

import Foundation
import Testing

@testable import Playhead

@MainActor
private final class StubBannerTallyPresenter: DiagnosticsExportPresenter {
    func present(
        data: Data,
        filename: String,
        subject: String,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    ) {
        completion(.success(.cancelled))
    }
}

@MainActor
private final class StubBannerTallyOptInSink: DiagnosticsOptInSink {
    func applyResetToEpisodes(matchingEpisodeIds: [String], newValue: Bool) {}
}

@Suite("Diagnostics bundle — banner_tallies (playhead-bfq7, legal item g)")
@MainActor
struct BannerTallyDiagnosticsPrivacyTests {

    private static let now = Date(timeIntervalSince1970: 1_700_000_000)
    private static let installID = UUID(uuidString: "11111111-1111-4111-8111-111111111111")!

    /// An episode id shaped like every kind of content that must never
    /// leave the device. Real episode ids are opaque, but the whole
    /// point of a hostile fixture is to assume they are not.
    private static let sentinelEpisodeTitle = "SENTINELEPISODE The Mattress Hour"
    private static let sentinelFeedURL = "https://sentinelfeed.example.com/rss.xml"
    private static let sentinelTranscript =
        "SENTINELTRANSCRIPT and now a word from our sponsor about mattresses"

    private static var hostileEpisodeId: String {
        "\(sentinelEpisodeTitle) | \(sentinelFeedURL) | \(sentinelTranscript)"
    }

    private static let allSentinels = [
        sentinelEpisodeTitle,
        sentinelFeedURL,
        sentinelTranscript,
        hostileEpisodeId
    ]

    private static let sentinelFragments = [
        "SENTINELEPISODE", "SENTINELTRANSCRIPT", "sentinelfeed",
        "Mattress", "mattress", "sponsor", "rss.xml"
    ]

    // MARK: - Fixtures

    private func makeStore() -> (BannerTallyStore, UserDefaults, String) {
        let suiteName = "BannerTallyDiagnosticsPrivacyTests.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)
        let store = BannerTallyStore(
            defaults: defaults,
            storageKey: "playhead.bannerTally.sessions.test",
            now: { Self.now }
        )
        return (store, defaults, suiteName)
    }

    private func item(
        windowId: String,
        tier: AdBannerTier,
        episodeId: String
    ) -> AdSkipBannerItem {
        AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: windowId,
            advertiser: "SENTINELADVERTISER",
            product: "SENTINELPRODUCT",
            adStartTime: 120,
            adEndTime: 168,
            metadataConfidence: nil,
            metadataSource: "none",
            podcastId: "pod-1",
            episodeId: episodeId,
            playbackLifecycleGeneration: 1,
            evidenceCatalogEntries: [],
            tier: tier
        )
    }

    private func environment() -> DiagnosticsExportEnvironment {
        DiagnosticsExportEnvironment(
            appVersion: "1.0.0",
            osVersion: "iOS 27.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibility: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: Self.now
            ),
            installID: Self.installID,
            now: Self.now
        )
    }

    private func encodedBundle(
        bannerTallies: @escaping DiagnosticsBannerTalliesFetch
    ) async throws -> Data {
        let coordinator = DiagnosticsExportCoordinator(
            environment: environment(),
            presenter: StubBannerTallyPresenter(),
            journalFetch: { [] },
            bannerTalliesFetch: bannerTallies,
            optInSink: StubBannerTallyOptInSink(),
            optInEpisodes: []
        )
        return try await coordinator.buildAndEncode().data
    }

    // MARK: - 1. Sentinel sweep

    @Test("an episode title, feed URL and transcript text stuffed into the episode id never survive projection")
    func sentinelsAreHashedAway() async throws {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        store.recordPresentation(
            of: item(windowId: "w1", tier: .suggest, episodeId: Self.hostileEpisodeId)
        )
        store.recordPresentation(
            of: item(windowId: "w2", tier: .autoSkipped, episodeId: Self.hostileEpisodeId)
        )
        let rows = store.sessions
        #expect(rows.count == 1, "the hostile fixture must still be COUNTED, not dropped")

        let data = try await encodedBundle(bannerTallies: { rows })
        let encoded = String(decoding: data, as: UTF8.self)

        for sentinel in Self.allSentinels {
            #expect(
                !encoded.contains(sentinel),
                "sentinel leaked into the encoded bundle: \(sentinel.prefix(40))…"
            )
        }
        // Fragment sweep, in case a future implementation truncates
        // rather than hashes — half a title is still a title.
        for fragment in Self.sentinelFragments {
            #expect(
                !encoded.contains(fragment),
                "sentinel fragment '\(fragment)' leaked into the encoded bundle"
            )
        }
        // The card's own copy is not carried either.
        for cardField in ["SENTINELADVERTISER", "SENTINELPRODUCT", "w1", "w2"] {
            #expect(
                !encoded.contains(cardField),
                "banner card field '\(cardField)' must not ride the bundle"
            )
        }
        // The local-only session key must not ride along either.
        let sessionKey = try #require(rows.first).sessionKey
        #expect(!encoded.contains(sessionKey))
    }

    @Test("the episode reference that DOES ship is the salted 64-hex EpisodeIdHasher output")
    func episodeReferenceIsTheSaltedHash() async throws {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        store.recordPresentation(
            of: item(windowId: "w1", tier: .suggest, episodeId: Self.hostileEpisodeId)
        )
        let rows = store.sessions
        let data = try await encodedBundle(bannerTallies: { rows })

        let summaries = try Self.bannerTallies(in: data)
        let hash = try #require(summaries.first?["episode_id_hash"] as? String)
        #expect(
            hash == EpisodeIdHasher.hash(
                installID: Self.installID,
                episodeId: Self.hostileEpisodeId
            ),
            "the tally must use the SAME salted reference the rest of the bundle uses"
        )
        let hashShape = #/^[0-9a-f]{64}$/#
        #expect((try? hashShape.wholeMatch(in: hash)) != nil)
    }

    // MARK: - 2. Closed shape

    @Test("an encoded summary's keys are exactly the declared CodingKeys — no surprise field")
    func encodedSummaryShapeIsClosed() async throws {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        store.recordPresentation(
            of: item(windowId: "w1", tier: .suggest, episodeId: "ep-1")
        )
        let rows = store.sessions
        let data = try await encodedBundle(bannerTallies: { rows })
        let summaries = try Self.bannerTallies(in: data)
        let keys = Set(try #require(summaries.first).keys)

        // FROZEN literal set, not `CodingKeys.allCases`. Comparing a
        // type against its own keys is circular — adding an
        // `episodeTitle: String` with a CodingKey would satisfy it. The
        // point is that adding ANY field is a deliberate, reviewed edit
        // to a privacy-audited list.
        let frozen: Set<String> = [
            "episode_id_hash",
            "banner_count",
            "auto_skipped_count",
            "suggest_count",
            "first_shown_at",
            "last_shown_at"
        ]
        let declared = Set(
            DefaultBundle.BannerTallySummary.CodingKeys.allCases.map(\.rawValue)
        )
        #expect(
            declared == frozen,
            """
            DefaultBundle.BannerTallySummary.CodingKeys changed: \
            added \(declared.subtracting(frozen)), removed \(frozen.subtracting(declared)). \
            Every field on this type is privacy-audited (legal checklist item g) — update \
            the checklist and this frozen set together, deliberately.
            """
        )
        #expect(keys == frozen)

        // Field names that would signal a regression, whatever else
        // changes.
        for forbidden in [
            "episode_id", "episodeId", "episode_title", "feed_url",
            "advertiser", "product", "window_id", "session_key", "transcript"
        ] {
            #expect(!keys.contains(forbidden), "leaky key '\(forbidden)' appeared on a summary")
        }
    }

    // MARK: - 3. Seam

    @Test("the tally survives the whole pipeline: store → fetch → builder → encoded bundle")
    func tallyReachesTheBundleWithItsTierBreakdown() async throws {
        let (store, defaults, suiteName) = makeStore()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        // Two cards on one episode, then a different episode.
        store.recordPresentation(of: item(windowId: "w1", tier: .suggest, episodeId: "ep-1"))
        store.recordPresentation(of: item(windowId: "w2", tier: .autoSkipped, episodeId: "ep-1"))
        store.recordPresentation(of: item(windowId: "w3", tier: .suggest, episodeId: "ep-2"))

        let rows = store.sessions
        let data = try await encodedBundle(bannerTallies: { rows })
        let summaries = try Self.bannerTallies(in: data)

        #expect(summaries.count == 2)

        let first = try #require(summaries.first)
        #expect(first["banner_count"] as? Int == 2)
        #expect(first["suggest_count"] as? Int == 1)
        #expect(first["auto_skipped_count"] as? Int == 1)
        #expect(first["first_shown_at"] as? Double == Self.now.timeIntervalSince1970)

        let second = try #require(summaries.last)
        #expect(second["banner_count"] as? Int == 1)
        #expect(second["suggest_count"] as? Int == 1)
        #expect(second["auto_skipped_count"] as? Int == 0)

        // Distinct episodes must not collapse into one reference.
        #expect(
            (first["episode_id_hash"] as? String) != (second["episode_id_hash"] as? String)
        )
    }

    @Test("the key is emitted even with no rows, so 'no banners' is distinguishable from 'old bundle'")
    func keyIsPresentWhenEmpty() async throws {
        let data = try await encodedBundle(bannerTallies: { [] })
        let summaries = try Self.bannerTallies(in: data)
        #expect(summaries.isEmpty)
    }

    @Test("a legacy bundle without the key decodes as an empty tally rather than failing")
    func legacyBundlesStayDecodable() throws {
        let bundle = DefaultBundle(
            appVersion: "1.0.0",
            osVersion: "iOS 27.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibilitySnapshot: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: Self.now
            ),
            analysisUnavailableReason: nil,
            schedulerEvents: [],
            workJournalTail: []
        )
        var object = try #require(
            try JSONSerialization.jsonObject(
                with: try JSONEncoder().encode(bundle), options: []
            ) as? [String: Any]
        )
        object.removeValue(forKey: "banner_tallies")
        let stripped = try JSONSerialization.data(withJSONObject: object, options: [])
        let decoded = try JSONDecoder().decode(DefaultBundle.self, from: stripped)
        #expect(decoded.bannerTallies.isEmpty)
    }

    // MARK: - Helpers

    /// The `banner_tallies` array of an encoded bundle file, as raw JSON
    /// objects. Reading the encoded bytes (rather than the Swift type)
    /// is the point: the wire format is what leaves the device.
    private static func bannerTallies(in data: Data) throws -> [[String: Any]] {
        let root = try #require(
            try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any]
        )
        let defaultSubtree = try #require(root["default"] as? [String: Any])
        return try #require(
            defaultSubtree["banner_tallies"] as? [[String: Any]],
            "banner_tallies must always be encoded, even when empty"
        )
    }
}
