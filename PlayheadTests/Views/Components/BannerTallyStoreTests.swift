// BannerTallyStoreTests.swift
// Per-episode banner-card tally (playhead-bfq7).
//
// Three claims are under test, and they fail independently:
//
//   1. COUNTING — the running index is 1-based, per episode session,
//      and resets on the `beginEpisode` boundary (a change of episode
//      identity OR of playback lifecycle generation).
//   2. HONESTY — the count is taken at the on-screen presentation
//      boundary, so exposure churn and in-place coalescing of the
//      visible card do NOT double-count, while a genuinely new
//      presentation does.
//   3. NO BEHAVIOUR CHANGE — a queue with a tally store attached
//      presents, tiers, and dismisses byte-for-byte like one without.

import Foundation
import XCTest

@testable import Playhead

@MainActor
final class BannerTallyStoreTests: XCTestCase {

    // MARK: - Fixtures

    private func makeDefaults() -> (UserDefaults, String) {
        let suiteName = "BannerTallyStoreTests.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)
        return (defaults, suiteName)
    }

    private func makeStore(_ defaults: UserDefaults) -> BannerTallyStore {
        BannerTallyStore(
            defaults: defaults,
            storageKey: "playhead.bannerTally.sessions.test",
            now: { Date(timeIntervalSince1970: 1_700_000_000) }
        )
    }

    private func item(
        windowId: String,
        tier: AdBannerTier = .suggest,
        episodeId: String? = "ep-1",
        generation: UInt64? = 1,
        start: Double = 100,
        end: Double = 130
    ) -> AdSkipBannerItem {
        AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: windowId,
            advertiser: nil,
            product: nil,
            adStartTime: start,
            adEndTime: end,
            metadataConfidence: nil,
            metadataSource: "none",
            podcastId: "pod-1",
            episodeId: episodeId,
            playbackLifecycleGeneration: generation,
            evidenceCatalogEntries: [],
            tier: tier
        )
    }

    // MARK: - 1. Counting

    func testIndexIsOneBasedAndRunsWithinAnEpisode() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)

        XCTAssertEqual(store.recordPresentation(of: item(windowId: "w1")), 1)
        XCTAssertEqual(store.recordPresentation(of: item(windowId: "w2")), 2)
        XCTAssertEqual(store.recordPresentation(of: item(windowId: "w3")), 3)

        XCTAssertEqual(store.sessions.count, 1)
        XCTAssertEqual(store.sessions.first?.bannerCount, 3)
        XCTAssertEqual(store.currentSessionCount, 3)
    }

    func testIndexResetsWhenTheEpisodeChanges() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)

        XCTAssertEqual(store.recordPresentation(of: item(windowId: "w1")), 1)
        XCTAssertEqual(store.recordPresentation(of: item(windowId: "w2")), 2)
        XCTAssertEqual(
            store.recordPresentation(of: item(windowId: "w3", episodeId: "ep-2")),
            1,
            "a new episode must restart the running index"
        )

        let rows = store.sessions
        XCTAssertEqual(rows.count, 2)
        XCTAssertEqual(rows.first?.episodeId, "ep-1")
        XCTAssertEqual(rows.first?.bannerCount, 2)
        XCTAssertEqual(rows.last?.episodeId, "ep-2")
        XCTAssertEqual(rows.last?.bannerCount, 1)
    }

    /// `beginEpisode` stamps a fresh `playbackLifecycleGeneration` even
    /// when the canonical episode is replayed. That boundary — not the
    /// episode id alone — is what the reset must follow, otherwise a
    /// replay would inflate the previous listen's number.
    func testIndexResetsOnReplayOfTheSameEpisode() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)

        XCTAssertEqual(store.recordPresentation(of: item(windowId: "w1")), 1)
        XCTAssertEqual(store.recordPresentation(of: item(windowId: "w2")), 2)
        XCTAssertEqual(
            store.recordPresentation(of: item(windowId: "w1", generation: 2)),
            1,
            "a new playback lifecycle for the same episode is a new session"
        )

        let rows = store.sessions
        XCTAssertEqual(rows.count, 2, "the replay must open its own row, not extend the first")
        XCTAssertEqual(rows.map(\.episodeId), ["ep-1", "ep-1"])
        XCTAssertEqual(rows.map(\.bannerCount), [2, 1])
    }

    func testPerTierBreakdownSplitsTheTotal() throws {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)

        store.recordPresentation(of: item(windowId: "w1", tier: .suggest))
        store.recordPresentation(of: item(windowId: "w2", tier: .autoSkipped))
        store.recordPresentation(of: item(windowId: "w3", tier: .suggest))

        let row = try XCTUnwrap(store.sessions.first)
        XCTAssertEqual(row.bannerCount, 3)
        XCTAssertEqual(row.suggestCount, 2)
        XCTAssertEqual(row.autoSkippedCount, 1)
        XCTAssertEqual(
            row.suggestCount + row.autoSkippedCount,
            row.bannerCount,
            "the tier split must account for every counted card"
        )
    }

    func testAnItemWithoutAnEpisodeIsLoggedButNotPersisted() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)

        XCTAssertEqual(
            store.recordPresentation(of: item(windowId: "w1", episodeId: nil)),
            0
        )
        XCTAssertTrue(
            store.sessions.isEmpty,
            "a row with no episode reference would pool unrelated cards"
        )
    }

    func testRowsPersistAcrossStoreInstances() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let first = makeStore(defaults)
        first.recordPresentation(of: item(windowId: "w1"))
        first.recordPresentation(of: item(windowId: "w2"))

        let reopened = makeStore(defaults)
        XCTAssertEqual(reopened.sessions.count, 1)
        XCTAssertEqual(reopened.sessions.first?.bannerCount, 2)
        XCTAssertEqual(
            reopened.recordPresentation(of: item(windowId: "w3")),
            1,
            "a relaunch starts a fresh session rather than resuming the old row"
        )
        XCTAssertEqual(reopened.sessions.count, 2)
    }

    func testTheCapEvictsTheOldestSessionNotTheLiveOne() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)

        let overflow = BannerTallyStore.maxSessions + 3
        for index in 0..<overflow {
            store.recordPresentation(
                of: item(windowId: "w\(index)", episodeId: "ep-\(index)")
            )
        }

        let rows = store.sessions
        XCTAssertEqual(rows.count, BannerTallyStore.maxSessions)
        XCTAssertEqual(
            rows.last?.episodeId,
            "ep-\(overflow - 1)",
            "the session being counted right now must survive the cap"
        )
        XCTAssertEqual(rows.first?.episodeId, "ep-3")
    }

    func testRemoveAllClearsRowsAndTheActiveSession() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)

        store.recordPresentation(of: item(windowId: "w1"))
        store.removeAll()

        XCTAssertTrue(store.sessions.isEmpty)
        XCTAssertEqual(store.currentSessionCount, 0)
        XCTAssertEqual(store.recordPresentation(of: item(windowId: "w2")), 1)
    }

    // MARK: - 2. Honesty at the presentation boundary

    /// The queue's `recordBannerShown(for:)` guard is what makes the
    /// tally honest. Repeated exposure callbacks for the SAME visible
    /// card must count once.
    func testRepeatedExposureOfOneVisibleCardCountsOnce() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)
        let queue = AdBannerQueue(tallyStore: store)

        let card = item(windowId: "w1")
        queue.enqueue(card)
        XCTAssertTrue(queue.recordBannerShown(for: card))
        XCTAssertFalse(queue.recordBannerShown(for: card))
        XCTAssertFalse(queue.recordBannerShown(for: card))

        XCTAssertEqual(store.sessions.first?.bannerCount, 1)
    }

    /// An emission that the queue never presents — deduplicated, or
    /// coalesced into the visible card — must not appear in the tally.
    /// This is why the count is taken here and not at the orchestrator's
    /// emit sites.
    func testAnEmissionThatIsNeverPresentedIsNotCounted() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)
        let queue = AdBannerQueue(tallyStore: store)

        let first = item(windowId: "w1", start: 100, end: 130)
        queue.enqueue(first)
        XCTAssertTrue(queue.recordBannerShown(for: first))

        // Adjacent same-tier span: absorbed into the visible card
        // (coalesced or dropped as a duplicate) rather than presented
        // as a second one.
        let adjacent = item(windowId: "w1", start: 100, end: 138)
        queue.enqueue(adjacent)
        XCTAssertFalse(
            queue.recordBannerShown(for: adjacent),
            "the visible presentation already claimed its impression"
        )

        XCTAssertEqual(
            store.sessions.first?.bannerCount,
            1,
            "a coalesced update is the same card, not a second one"
        )
    }

    /// The documented re-presentation semantics: once the lane has
    /// advanced, a fresh presentation is a second card the listener had
    /// to look at, and it counts again.
    func testASecondPresentationAfterDismissalCountsAgain() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)
        let queue = AdBannerQueue(tallyStore: store)

        let first = item(windowId: "w1", start: 100, end: 130)
        queue.enqueue(first)
        XCTAssertTrue(queue.recordBannerShown(for: first))
        queue.dismiss()

        let second = item(windowId: "w2", start: 900, end: 930)
        queue.enqueue(second)
        XCTAssertTrue(queue.recordBannerShown(for: second))

        XCTAssertEqual(store.sessions.first?.bannerCount, 2)
        XCTAssertEqual(store.currentSessionCount, 2)
    }

    // MARK: - 3. No behaviour change

    /// Instrumentation must not move a presentation, a tier, or a
    /// dwell. Drive two queues that differ ONLY in whether a tally
    /// store is attached and assert the observable presentation state
    /// matches at every step.
    func testAttachingTheTallyStoreChangesNoPresentationBehaviour() throws {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = makeStore(defaults)

        let instrumented = AdBannerQueue(tallyStore: store)
        let baseline = AdBannerQueue()

        let suggest = item(windowId: "w1", tier: .suggest, start: 10, end: 40)
        let autoSkipped = item(windowId: "w2", tier: .autoSkipped, start: 900, end: 930)

        for queue in [instrumented, baseline] {
            queue.enqueue(suggest)
            queue.enqueue(autoSkipped)
        }

        // Same card is current, with the same tier.
        XCTAssertEqual(instrumented.currentBanner?.windowId, baseline.currentBanner?.windowId)
        XCTAssertEqual(instrumented.currentBanner?.tier, baseline.currentBanner?.tier)
        XCTAssertEqual(instrumented.currentBanner?.tier, .suggest)

        // Same dwell selection for the visible tier.
        let instrumentedTier = try XCTUnwrap(instrumented.currentBanner).tier
        let baselineTier = try XCTUnwrap(baseline.currentBanner).tier
        XCTAssertEqual(
            AdBannerQueue.dwellSeconds(for: instrumentedTier),
            AdBannerQueue.dwellSeconds(for: baselineTier)
        )

        // Same impression-guard answers, in the same order.
        XCTAssertEqual(
            instrumented.recordBannerShown(for: suggest),
            baseline.recordBannerShown(for: suggest)
        )
        XCTAssertEqual(
            instrumented.recordBannerShown(for: suggest),
            baseline.recordBannerShown(for: suggest)
        )

        // Same dismissal outcome and same queued successor.
        for queue in [instrumented, baseline] {
            queue.dismiss()
        }
        XCTAssertNil(instrumented.currentBanner)
        XCTAssertNil(baseline.currentBanner)
        XCTAssertEqual(
            instrumented.advanceTaskForTesting() != nil,
            baseline.advanceTaskForTesting() != nil,
            "the deferred slide-in must be scheduled identically"
        )

        // And the instrumentation still recorded what it saw.
        XCTAssertEqual(store.sessions.first?.bannerCount, 1)
    }
}
