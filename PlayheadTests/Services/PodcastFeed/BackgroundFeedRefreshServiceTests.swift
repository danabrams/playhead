// BackgroundFeedRefreshServiceTests.swift
// playhead-fv2q — tests for the periodic BGAppRefreshTask-driven feed
// refresh + auto-download handler.
//
// The service-under-test is an actor that (a) snapshots all subscribed
// podcasts via an injectable enumerator, (b) refreshes each feed via an
// injectable refresher, (c) diffs new episodes against the pre-refresh
// episode-id set, and (d) enqueues each truly-new episode through an
// injectable downloader when the user's `AutoDownloadOnSubscribe`
// setting is enabled. The BGAppRefreshTask wiring itself is exercised
// only through the `BackgroundProcessingTaskProtocol` stub (Stubs.swift)
// so no real BGTaskScheduler registration is needed.

import BackgroundTasks
import Foundation
import Testing
@testable import Playhead

// MARK: - Test doubles

/// Pre-refresh snapshot of a single podcast + its known episode-id set.
/// Returned by `StubPodcastEnumerator` so the handler can compute a diff
/// without holding a live SwiftData ModelContext. Mirrors the shape the
/// production enumerator surfaces via a `@MainActor` fetch.
///
/// `autoDownloadOverride` (playhead-5w4) lets the per-show override
/// flow through stubs without the test author having to touch SwiftData.
/// Default `nil` keeps existing tests source-compatible (they all
/// implicitly assert "inherit global").
private struct PodcastFeedSnapshot: Sendable, Equatable {
    let feedURL: URL
    let existingEpisodeGUIDs: Set<String>
    let autoDownloadOverride: AutoDownloadOnSubscribe?

    init(
        feedURL: URL,
        existingEpisodeGUIDs: Set<String>,
        autoDownloadOverride: AutoDownloadOnSubscribe? = nil
    ) {
        self.feedURL = feedURL
        self.existingEpisodeGUIDs = existingEpisodeGUIDs
        self.autoDownloadOverride = autoDownloadOverride
    }
}

/// New episode record produced by a refresh pass. The handler hands
/// these to the downloader when auto-download is enabled.
private struct NewEpisodeRecord: Sendable, Equatable {
    let canonicalEpisodeKey: String
    let audioURL: URL
    let publishedAt: Date?
}

private actor StubPodcastEnumerator: PodcastEnumerating {
    var snapshots: [PodcastFeedSnapshot]

    init(_ snapshots: [PodcastFeedSnapshot]) {
        self.snapshots = snapshots
    }

    func enumeratePodcasts() async -> [FeedRefreshPodcastSnapshot] {
        snapshots.map {
            FeedRefreshPodcastSnapshot(
                feedURL: $0.feedURL,
                existingEpisodeGUIDs: $0.existingEpisodeGUIDs,
                autoDownloadOverride: $0.autoDownloadOverride
            )
        }
    }
}

private actor StubFeedRefresher: FeedRefreshing {
    /// New-episode records to return from a refresh for a given feed URL.
    /// Defaults to empty (no new episodes) if the feed URL is unmapped.
    var newEpisodesByFeedURL: [URL: [NewEpisodeRecord]] = [:]
    /// Errors to throw when refreshing a given feed URL. Overrides
    /// `newEpisodesByFeedURL` when set.
    var errorsByFeedURL: [URL: Error] = [:]
    /// Ordered list of refresh calls for ordering/invocation assertions.
    private(set) var refreshCalls: [URL] = []

    func setNewEpisodes(_ episodes: [NewEpisodeRecord], for feedURL: URL) {
        newEpisodesByFeedURL[feedURL] = episodes
    }

    func setError(_ error: Error, for feedURL: URL) {
        errorsByFeedURL[feedURL] = error
    }

    func refreshCallCount(for feedURL: URL) -> Int {
        refreshCalls.filter { $0 == feedURL }.count
    }

    func totalRefreshCallCount() -> Int { refreshCalls.count }

    func refreshEpisodes(
        feedURL: URL,
        existingEpisodeGUIDs: Set<String>
    ) async throws -> [FeedRefreshNewEpisode] {
        refreshCalls.append(feedURL)
        if let error = errorsByFeedURL[feedURL] {
            throw error
        }
        let records = newEpisodesByFeedURL[feedURL] ?? []
        // playhead-5w4: tag each surfaced episode with the originating
        // feed URL so the handler can group by podcast and resolve the
        // per-show override.
        return records.map {
            FeedRefreshNewEpisode(
                canonicalEpisodeKey: $0.canonicalEpisodeKey,
                feedURL: feedURL,
                audioURL: $0.audioURL,
                publishedAt: $0.publishedAt
            )
        }
    }
}

private actor StubAutoDownloadEnqueuer: AutoDownloadEnqueueing {
    private(set) var enqueued: [(episodeId: String, url: URL)] = []

    func enqueueBackgroundDownload(episodeId: String, from url: URL) async {
        enqueued.append((episodeId: episodeId, url: url))
    }

    func enqueuedEpisodeIds() -> [String] {
        enqueued.map(\.episodeId)
    }
}

private struct StubDownloadsSettingsProvider: DownloadsSettingsProviding {
    var setting: AutoDownloadOnSubscribe
    func currentAutoDownloadSetting() -> AutoDownloadOnSubscribe { setting }
}

/// playhead-5uvz.4 (Gap-5): records `scheduleBackfillIfNeeded` calls so
/// the rearm-after-refresh contract is observable without standing up a
/// real `BackgroundProcessingService`.
private actor StubBackfillScheduler: BackfillScheduling {
    private(set) var scheduleCallCount = 0

    func scheduleBackfillIfNeeded() async {
        scheduleCallCount += 1
    }
}

// MARK: - Factory

private func makeService(
    podcasts: [PodcastFeedSnapshot] = [],
    setting: AutoDownloadOnSubscribe = .off,
    scheduler: StubTaskScheduler = StubTaskScheduler(),
    backfillScheduler: (any BackfillScheduling)? = nil
) -> (
    service: BackgroundFeedRefreshService,
    enumerator: StubPodcastEnumerator,
    refresher: StubFeedRefresher,
    downloader: StubAutoDownloadEnqueuer,
    scheduler: StubTaskScheduler
) {
    let enumerator = StubPodcastEnumerator(podcasts)
    let refresher = StubFeedRefresher()
    let downloader = StubAutoDownloadEnqueuer()
    let settings = StubDownloadsSettingsProvider(setting: setting)

    let service = BackgroundFeedRefreshService(
        enumerator: enumerator,
        refresher: refresher,
        downloader: downloader,
        settingsProvider: settings,
        taskScheduler: scheduler,
        backfillScheduler: backfillScheduler
    )
    return (service, enumerator, refresher, downloader, scheduler)
}

// MARK: - Refresh fan-out

@Suite("BackgroundFeedRefreshService — refresh fan-out")
struct BackgroundFeedRefreshFanoutTests {

    @Test("Handler refreshes every subscribed podcast exactly once")
    func handlerRefreshesEveryPodcastOnce() async throws {
        let feedA = URL(string: "https://example.com/a.xml")!
        let feedB = URL(string: "https://example.com/b.xml")!
        let feedC = URL(string: "https://example.com/c.xml")!

        let (service, _, refresher, _, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feedA, existingEpisodeGUIDs: []),
                PodcastFeedSnapshot(feedURL: feedB, existingEpisodeGUIDs: []),
                PodcastFeedSnapshot(feedURL: feedC, existingEpisodeGUIDs: []),
            ],
            setting: .off
        )
        let task = StubBackgroundTask()

        await service.handleFeedRefreshTask(task)

        // Every podcast must have been refreshed exactly once.
        #expect(await refresher.refreshCallCount(for: feedA) == 1)
        #expect(await refresher.refreshCallCount(for: feedB) == 1)
        #expect(await refresher.refreshCallCount(for: feedC) == 1)
        #expect(await refresher.totalRefreshCallCount() == 3)
        #expect(task.completedSuccess == true)
    }

    @Test("Per-podcast refresh error is caught and does not abort the loop")
    func perPodcastErrorDoesNotAbortLoop() async throws {
        let feedA = URL(string: "https://example.com/a.xml")!
        let feedB = URL(string: "https://example.com/b.xml")!
        let feedC = URL(string: "https://example.com/c.xml")!

        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feedA, existingEpisodeGUIDs: []),
                PodcastFeedSnapshot(feedURL: feedB, existingEpisodeGUIDs: []),
                PodcastFeedSnapshot(feedURL: feedC, existingEpisodeGUIDs: []),
            ],
            setting: .all
        )
        // Middle feed fails; the other two must still refresh.
        await refresher.setError(
            NSError(domain: "test", code: 1), for: feedB
        )
        // Feeds A and C publish new episodes.
        await refresher.setNewEpisodes(
            [newRecord(key: "ep-a-new", feed: feedA)], for: feedA
        )
        await refresher.setNewEpisodes(
            [newRecord(key: "ep-c-new", feed: feedC)], for: feedC
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        #expect(await refresher.totalRefreshCallCount() == 3,
                "Error on feed B must not prevent refresh of feed C")
        // Despite the error on B, downloads for A and C must proceed.
        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(Set(enqueued) == Set(["ep-a-new", "ep-c-new"]))
        #expect(task.completedSuccess == true,
                "Partial refresh success must report completion to iOS")
    }
}

// MARK: - Reschedule

@Suite("BackgroundFeedRefreshService — rescheduling")
struct BackgroundFeedRefreshRescheduleTests {

    @Test("Handler reschedules next refresh before completing")
    func handlerReschedulesNextRefresh() async throws {
        let (service, _, _, _, scheduler) = makeService()
        let task = StubBackgroundTask()

        await service.handleFeedRefreshTask(task)

        let rescheduled = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundFeedRefreshService.taskIdentifier
        }
        #expect(rescheduled.count >= 1,
                "Handler must reschedule the next refresh before completing")

        // Interval contract: 1-hour minimum earliest-begin-date per spec.
        if let request = rescheduled.first as? BGAppRefreshTaskRequest,
           let earliest = request.earliestBeginDate {
            #expect(earliest.timeIntervalSinceNow >= 60 * 60 - 5,
                    "earliestBeginDate must be >= 1 hour from now (got \(earliest.timeIntervalSinceNow))")
        } else {
            Issue.record("Rescheduled request must be a BGAppRefreshTaskRequest with an earliestBeginDate")
        }
    }

    @Test("scheduleNextRefresh submits a BGAppRefreshTaskRequest with the correct identifier")
    func scheduleNextRefreshSubmitsCorrectRequest() async throws {
        let (service, _, _, _, scheduler) = makeService()

        await service.scheduleNextRefresh()

        #expect(scheduler.submittedRequests.count == 1)
        let first = scheduler.submittedRequests.first
        #expect(first?.identifier == BackgroundFeedRefreshService.taskIdentifier)
        #expect(first is BGAppRefreshTaskRequest,
                "Feed refresh is a BGAppRefreshTask — not a processing task")
    }
}

// MARK: - Idempotent reschedule (playhead-y5mk starvation fix)

/// The bug: `scheduleNextRefresh()` unconditionally submitted a
/// `BGAppRefreshTaskRequest`. Because `BGTaskScheduler.submit` dedupes by
/// identifier, each resubmit REPLACED the pending request and pushed its
/// earliest-begin-date another hour out. `start()` runs on every launch —
/// including the frequent ~30-min background launches iOS grants for
/// analysis — so the request never matured (observed: 131 submits, 0
/// dispatches over 8 days). The fix skips the submit when a feed-refresh
/// request is already pending; the post-dispatch reschedule bypasses the
/// guard so a fired task still re-arms.
@Suite("BackgroundFeedRefreshService — idempotent reschedule (playhead-y5mk)")
struct BackgroundFeedRefreshIdempotentRescheduleTests {

    @Test("scheduleNextRefresh submits when NO feed-refresh request is pending")
    func submitsWhenNonePending() async {
        let (service, _, _, _, scheduler) = makeService()

        await service.scheduleNextRefresh()

        let feedRefreshSubmits = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundFeedRefreshService.taskIdentifier
        }
        #expect(feedRefreshSubmits.count == 1,
                "Must submit exactly once when nothing is pending")
    }

    @Test("scheduleNextRefresh SKIPS the submit when a feed-refresh request is already pending")
    func skipsWhenAlreadyPending() async {
        let scheduler = StubTaskScheduler()
        // Configurable pending set: a feed-refresh request is already there.
        scheduler.seedPending(
            BGAppRefreshTaskRequest(identifier: BackgroundFeedRefreshService.taskIdentifier)
        )
        let (service, _, _, _, _) = makeService(scheduler: scheduler)

        await service.scheduleNextRefresh()

        #expect(scheduler.submittedRequests.isEmpty,
                "Must not submit when a feed-refresh request is already pending")
    }

    @Test("A pending request for a DIFFERENT identifier does not suppress the submit")
    func differentIdentifierDoesNotSuppress() async {
        let scheduler = StubTaskScheduler()
        // Some unrelated task is pending — must NOT block feed-refresh.
        scheduler.seedPending(
            BGProcessingTaskRequest(identifier: BackgroundTaskID.backfillProcessing)
        )
        let (service, _, _, _, _) = makeService(scheduler: scheduler)

        await service.scheduleNextRefresh()

        let feedRefreshSubmits = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundFeedRefreshService.taskIdentifier
        }
        #expect(feedRefreshSubmits.count == 1,
                "Only a same-identifier pending request should suppress the submit")
    }

    /// Regression modeling the actual device failure: N background wakes,
    /// each calling `scheduleNextRefresh()` (as `start()` does), must leave
    /// EXACTLY ONE pending feed-refresh request — not N bulldozing
    /// resubmits that keep pushing the begin-date out of reach.
    @Test("N successive reschedules yield exactly one pending request, not N resubmits")
    func repeatedReschedulesDoNotBulldozePending() async {
        let (service, _, _, _, scheduler) = makeService()

        // Simulate 8 background wakes (the observed window) re-arming.
        for _ in 0..<8 {
            await service.scheduleNextRefresh()
        }

        let pendingIdentifiers = await scheduler.pendingTaskRequestIdentifiers()
        let feedRefreshPending = pendingIdentifiers.filter {
            $0 == BackgroundFeedRefreshService.taskIdentifier
        }
        #expect(feedRefreshPending.count == 1,
                "Exactly one feed-refresh request must be pending; got \(feedRefreshPending.count)")

        let feedRefreshSubmits = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundFeedRefreshService.taskIdentifier
        }
        #expect(feedRefreshSubmits.count == 1,
                "Only the first wake should submit; the other 7 must be no-ops, got \(feedRefreshSubmits.count)")
    }

    /// The guard must NEVER block the post-dispatch reschedule. When a task
    /// fires there is nothing pending, so re-arming (a fresh submit) is
    /// correct and required. This pins that `handleFeedRefreshTask` still
    /// submits even when — defensively — a request is already pending.
    @Test("handleFeedRefreshTask re-arms unconditionally even when a request is already pending")
    func handlerReschedulesEvenWhenPending() async {
        let scheduler = StubTaskScheduler()
        // Defensive: a feed-refresh request is already pending going in.
        // A guarded submit would skip; the handler's forced path must not.
        scheduler.seedPending(
            BGAppRefreshTaskRequest(identifier: BackgroundFeedRefreshService.taskIdentifier)
        )
        let (service, _, _, _, _) = makeService(scheduler: scheduler)
        let task = StubBackgroundTask()

        await service.handleFeedRefreshTask(task)

        let feedRefreshSubmits = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundFeedRefreshService.taskIdentifier
        }
        #expect(feedRefreshSubmits.count >= 1,
                "A fired task must re-arm the next fire even though a request was already pending")
        #expect(task.completedSuccess == true)
    }
}

// MARK: - Shared foreground/background enqueue path (playhead-y5mk)

/// `enqueueAutoDownloads(for:)` is the single selection + enqueue entry
/// point that BOTH the BGAppRefreshTask handler and `LibraryView`
/// pull-to-refresh feed. These tests pin the gating that the foreground
/// (manual) path now honors identically to the background path: `.off`
/// enqueues nothing, `.last3` selects the newest three, already-known
/// episodes (empty diff groups) are not re-enqueued, and per-show
/// overrides resolve against the global.
@Suite("BackgroundFeedRefreshService — shared enqueue path (playhead-y5mk)")
struct BackgroundFeedRefreshSharedEnqueuePathTests {

    @Test("Setting .off enqueues nothing even when new episodes were discovered")
    func offEnqueuesNothing() async {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, _, downloader, _) = makeService(setting: .off)
        let groups = [
            FeedRefreshDiscoveryGroup(
                feedURL: feed,
                autoDownloadOverride: nil,
                newEpisodes: [
                    feedEpisode(key: "ep-1", feed: feed),
                    feedEpisode(key: "ep-2", feed: feed),
                ]
            )
        ]

        let count = await service.enqueueAutoDownloads(for: groups)

        #expect(count == 0)
        #expect(await downloader.enqueuedEpisodeIds().isEmpty,
                ".off must resolve to zero enqueues")
    }

    @Test("Setting .last3 selects the three newest undownloaded episodes")
    func last3SelectsNewestThree() async {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, _, downloader, _) = makeService(setting: .last3)
        let t0 = Date.now
        let groups = [
            FeedRefreshDiscoveryGroup(
                feedURL: feed,
                autoDownloadOverride: nil,
                newEpisodes: [
                    feedEpisode(key: "ep-5", feed: feed, published: t0.addingTimeInterval(-500)),
                    feedEpisode(key: "ep-4", feed: feed, published: t0.addingTimeInterval(-400)),
                    feedEpisode(key: "ep-3", feed: feed, published: t0.addingTimeInterval(-300)),
                    feedEpisode(key: "ep-2", feed: feed, published: t0.addingTimeInterval(-200)),
                    feedEpisode(key: "ep-1", feed: feed, published: t0.addingTimeInterval(-100)),
                ]
            )
        ]

        let count = await service.enqueueAutoDownloads(for: groups)

        #expect(count == 3)
        #expect(Set(await downloader.enqueuedEpisodeIds()) == Set(["ep-1", "ep-2", "ep-3"]),
                ".last3 must enqueue only the three newest by publishedAt")
    }

    @Test("Groups whose diff is empty (already-known/downloaded episodes) enqueue nothing")
    func emptyDiffGroupsAreNotEnqueued() async {
        // Group A surfaced one genuinely-new episode; group B's discoveries
        // were all already in the store, so its diff is empty. The already-
        // known episodes must not be re-enqueued.
        let feedA = URL(string: "https://example.com/a.xml")!
        let feedB = URL(string: "https://example.com/b.xml")!
        let (service, _, _, downloader, _) = makeService(setting: .all)
        let groups = [
            FeedRefreshDiscoveryGroup(
                feedURL: feedA,
                autoDownloadOverride: nil,
                newEpisodes: [feedEpisode(key: "a-new", feed: feedA)]
            ),
            FeedRefreshDiscoveryGroup(
                feedURL: feedB,
                autoDownloadOverride: nil,
                newEpisodes: []  // already-known → empty diff
            ),
        ]

        let count = await service.enqueueAutoDownloads(for: groups)

        #expect(count == 1)
        #expect(await downloader.enqueuedEpisodeIds() == ["a-new"],
                "Only genuinely-new episodes enqueue; empty-diff groups are skipped")
    }

    @Test("Per-show override resolves against the global in the shared path")
    func perShowOverrideResolvesAgainstGlobal() async {
        // Global .off; show A overrides to .all, show B inherits (nil).
        let feedA = URL(string: "https://example.com/a.xml")!
        let feedB = URL(string: "https://example.com/b.xml")!
        let (service, _, _, downloader, _) = makeService(setting: .off)
        let groups = [
            FeedRefreshDiscoveryGroup(
                feedURL: feedA,
                autoDownloadOverride: .all,
                newEpisodes: [
                    feedEpisode(key: "a-1", feed: feedA),
                    feedEpisode(key: "a-2", feed: feedA),
                ]
            ),
            FeedRefreshDiscoveryGroup(
                feedURL: feedB,
                autoDownloadOverride: nil,  // inherits global .off
                newEpisodes: [feedEpisode(key: "b-1", feed: feedB)]
            ),
        ]

        let count = await service.enqueueAutoDownloads(for: groups)

        #expect(count == 2)
        #expect(Set(await downloader.enqueuedEpisodeIds()) == Set(["a-1", "a-2"]),
                "Show A's .all override enqueues both; show B inherits .off and enqueues nothing")
    }
}

// MARK: - Diff: only NEW episodes are downloaded

@Suite("BackgroundFeedRefreshService — new-episode diff")
struct BackgroundFeedRefreshDiffTests {

    @Test("Only episodes whose GUID is newly present are enqueued for download")
    func onlyNewEpisodesAreDownloaded() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        // Two episodes already exist; one new one arrives.
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(
                    feedURL: feed,
                    existingEpisodeGUIDs: ["ep-existing-1", "ep-existing-2"]
                )
            ],
            setting: .all
        )
        await refresher.setNewEpisodes(
            [newRecord(key: "ep-brand-new", feed: feed)],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(enqueued == ["ep-brand-new"],
                "Pre-existing episodes must not be re-downloaded")
    }

    @Test("No new episodes → no downloads enqueued")
    func noNewEpisodesMeansNoDownloads() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, _, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(
                    feedURL: feed,
                    existingEpisodeGUIDs: ["ep-existing-1"]
                )
            ],
            setting: .all
        )
        // Refresher defaults to returning []: no new episodes discovered.

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(enqueued.isEmpty,
                "Unchanged feed must not enqueue any downloads")
    }
}

// MARK: - Auto-download setting gating

@Suite("BackgroundFeedRefreshService — auto-download setting")
struct BackgroundFeedRefreshAutoDownloadSettingTests {

    @Test("Setting .off skips downloads even when new episodes are discovered")
    func settingOffSkipsDownloads() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feed, existingEpisodeGUIDs: [])
            ],
            setting: .off
        )
        await refresher.setNewEpisodes(
            [
                newRecord(key: "ep-new-1", feed: feed),
                newRecord(key: "ep-new-2", feed: feed),
            ],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        // Refresh still ran — the diff is still useful for UI.
        #expect(await refresher.refreshCallCount(for: feed) == 1)
        // …but no download was enqueued because the setting is .off.
        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(enqueued.isEmpty,
                "Auto-download .off must suppress all enqueues")
    }

    @Test("Setting .all enqueues every new episode")
    func settingAllEnqueuesEveryNewEpisode() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feed, existingEpisodeGUIDs: [])
            ],
            setting: .all
        )
        await refresher.setNewEpisodes(
            [
                newRecord(key: "ep-1", feed: feed, published: .now),
                newRecord(key: "ep-2", feed: feed, published: .now.addingTimeInterval(-60)),
                newRecord(key: "ep-3", feed: feed, published: .now.addingTimeInterval(-120)),
                newRecord(key: "ep-4", feed: feed, published: .now.addingTimeInterval(-180)),
            ],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(Set(enqueued) == Set(["ep-1", "ep-2", "ep-3", "ep-4"]),
                ".all must enqueue every new episode")
    }

    @Test("Setting .last1 enqueues at most the single newest new episode")
    func settingLast1EnqueuesOnlyNewest() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feed, existingEpisodeGUIDs: [])
            ],
            setting: .last1
        )
        let newest = Date.now
        await refresher.setNewEpisodes(
            [
                newRecord(key: "ep-oldest", feed: feed, published: newest.addingTimeInterval(-200)),
                newRecord(key: "ep-newest", feed: feed, published: newest),
                newRecord(key: "ep-middle", feed: feed, published: newest.addingTimeInterval(-100)),
            ],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(enqueued == ["ep-newest"],
                ".last1 must enqueue only the single newest new episode")
    }

    @Test("Setting .last3 enqueues at most the three newest new episodes")
    func settingLast3EnqueuesAtMostThree() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feed, existingEpisodeGUIDs: [])
            ],
            setting: .last3
        )
        let t0 = Date.now
        await refresher.setNewEpisodes(
            [
                newRecord(key: "ep-5", feed: feed, published: t0.addingTimeInterval(-500)),
                newRecord(key: "ep-4", feed: feed, published: t0.addingTimeInterval(-400)),
                newRecord(key: "ep-3", feed: feed, published: t0.addingTimeInterval(-300)),
                newRecord(key: "ep-2", feed: feed, published: t0.addingTimeInterval(-200)),
                newRecord(key: "ep-1", feed: feed, published: t0.addingTimeInterval(-100)),
            ],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = Set(await downloader.enqueuedEpisodeIds())
        #expect(enqueued == Set(["ep-1", "ep-2", "ep-3"]),
                ".last3 must enqueue only the three newest by publishedAt")
    }
}

// MARK: - Per-podcast override (playhead-5w4)

/// Pins the wiring contract for the per-show override: enumeration
/// carries the override forward, the handler resolves
/// `override ?? global` per feed, and per-show choices never leak
/// across podcasts in the same refresh fire.
@Suite("BackgroundFeedRefreshService — per-show auto-download override (playhead-5w4)")
struct BackgroundFeedRefreshPerShowOverrideTests {

    @Test("Per-show .off override suppresses downloads even when global is .all")
    func perShowOffWinsOverGlobalAll() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(
                    feedURL: feed,
                    existingEpisodeGUIDs: [],
                    autoDownloadOverride: .off
                )
            ],
            // Global setting is "All" — the bead's motivating user
            // story is a noisy show the user wants to silence without
            // changing their global pick.
            setting: .all
        )
        await refresher.setNewEpisodes(
            [
                newRecord(key: "ep-new-1", feed: feed),
                newRecord(key: "ep-new-2", feed: feed),
            ],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(enqueued.isEmpty,
                "Per-show .off must suppress all enqueues even when the global is .all")
    }

    @Test("Per-show .all override enqueues every new episode even when global is .off")
    func perShowAllWinsOverGlobalOff() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(
                    feedURL: feed,
                    existingEpisodeGUIDs: [],
                    autoDownloadOverride: .all
                )
            ],
            // Global setting is "Off" — the bead's other motivating
            // story is a favorite the user wants to auto-download
            // without flipping their global to "All".
            setting: .off
        )
        await refresher.setNewEpisodes(
            [
                newRecord(key: "ep-new-1", feed: feed),
                newRecord(key: "ep-new-2", feed: feed),
            ],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = Set(await downloader.enqueuedEpisodeIds())
        #expect(enqueued == Set(["ep-new-1", "ep-new-2"]),
                "Per-show .all must enqueue every new episode even when the global is .off")
    }

    @Test("Nil override falls back to the global setting")
    func nilOverrideFollowsGlobal() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(
                    feedURL: feed,
                    existingEpisodeGUIDs: [],
                    autoDownloadOverride: nil
                )
            ],
            setting: .all
        )
        await refresher.setNewEpisodes(
            [newRecord(key: "ep-new-1", feed: feed)],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(enqueued == ["ep-new-1"],
                "Nil override must inherit the global .all and enqueue the new episode")
    }

    @Test("Per-show overrides do not leak across podcasts in the same refresh fire")
    func overridesAreIsolatedPerPodcast() async throws {
        // Three feeds in one refresh fire, each with a different
        // policy. The selection must group by feedURL and apply the
        // resolved-per-podcast policy — a bug that re-introduced the
        // pre-fix global-flat path would produce different counts.
        let feedA = URL(string: "https://example.com/a.xml")!
        let feedB = URL(string: "https://example.com/b.xml")!
        let feedC = URL(string: "https://example.com/c.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                // Show A: inherit global (.last3).
                PodcastFeedSnapshot(
                    feedURL: feedA,
                    existingEpisodeGUIDs: [],
                    autoDownloadOverride: nil
                ),
                // Show B: explicit .off, ignoring the global.
                PodcastFeedSnapshot(
                    feedURL: feedB,
                    existingEpisodeGUIDs: [],
                    autoDownloadOverride: .off
                ),
                // Show C: explicit .last1, ignoring the global.
                PodcastFeedSnapshot(
                    feedURL: feedC,
                    existingEpisodeGUIDs: [],
                    autoDownloadOverride: .last1
                ),
            ],
            setting: .last3
        )
        let t0 = Date.now
        // Each feed publishes four new episodes with distinct dates.
        for (feed, prefix) in [(feedA, "a"), (feedB, "b"), (feedC, "c")] {
            await refresher.setNewEpisodes(
                [
                    newRecord(key: "\(prefix)-1", feed: feed, published: t0),
                    newRecord(key: "\(prefix)-2", feed: feed, published: t0.addingTimeInterval(-100)),
                    newRecord(key: "\(prefix)-3", feed: feed, published: t0.addingTimeInterval(-200)),
                    newRecord(key: "\(prefix)-4", feed: feed, published: t0.addingTimeInterval(-300)),
                ],
                for: feed
            )
        }

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = Set(await downloader.enqueuedEpisodeIds())
        // A: inherit .last3 → top 3 newest (a-1, a-2, a-3).
        // B: .off → none.
        // C: .last1 → top 1 newest (c-1).
        let expected = Set(["a-1", "a-2", "a-3", "c-1"])
        #expect(enqueued == expected,
                "Per-show overrides must apply independently; got \(enqueued.sorted())")
    }

    @Test("Override .last1 picks the newest from this show's episodes only, ignoring other feeds")
    func overrideLastNSortsWithinFeed() async throws {
        // If the handler accidentally sorted-and-cut globally, the
        // newest episode across all feeds would win and this show's
        // newest could be lost. Pin the per-feed sort.
        let feedA = URL(string: "https://example.com/a.xml")!
        let feedB = URL(string: "https://example.com/b.xml")!
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(
                    feedURL: feedA,
                    existingEpisodeGUIDs: [],
                    autoDownloadOverride: .last1
                ),
                PodcastFeedSnapshot(
                    feedURL: feedB,
                    existingEpisodeGUIDs: [],
                    autoDownloadOverride: .last1
                ),
            ],
            setting: .off
        )
        let t0 = Date.now
        // Feed A's "newest" is older than feed B's "newest". A global-
        // flat sort would pick only feed B's; the per-feed sort picks
        // one from each.
        await refresher.setNewEpisodes(
            [
                newRecord(key: "a-newest", feed: feedA, published: t0.addingTimeInterval(-500)),
                newRecord(key: "a-older",  feed: feedA, published: t0.addingTimeInterval(-600)),
            ],
            for: feedA
        )
        await refresher.setNewEpisodes(
            [
                newRecord(key: "b-newest", feed: feedB, published: t0),
                newRecord(key: "b-older",  feed: feedB, published: t0.addingTimeInterval(-100)),
            ],
            for: feedB
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let enqueued = Set(await downloader.enqueuedEpisodeIds())
        #expect(enqueued == Set(["a-newest", "b-newest"]),
                "Each per-show .last1 must pick its own feed's newest; got \(enqueued.sorted())")
    }
}

// MARK: - Expiration handler contract

@Suite("BackgroundFeedRefreshService — expiration")
struct BackgroundFeedRefreshExpirationTests {

    /// Refresher that yields per-feed until the test signals it to
    /// return. Lets the test fire the expiration handler mid-work so
    /// both the expiration path and the normal-exit path race for
    /// `setTaskCompleted`. Only one of those calls must land — iOS
    /// terminates the app on a double-complete.
    private actor GatedRefresher: FeedRefreshing {
        private var released = false
        private var waiters: [CheckedContinuation<Void, Never>] = []

        func release() {
            released = true
            for waiter in waiters { waiter.resume() }
            waiters.removeAll()
        }

        func refreshEpisodes(
            feedURL _: URL,
            existingEpisodeGUIDs _: Set<String>
        ) async throws -> [FeedRefreshNewEpisode] {
            if released { return [] }
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                waiters.append(cont)
            }
            return []
        }
    }

    @Test("Expiration mid-refresh completes task exactly once and reports success=false")
    func expirationCompletesTaskExactlyOnce() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let enumerator = StubPodcastEnumerator(
            [PodcastFeedSnapshot(feedURL: feed, existingEpisodeGUIDs: [])]
        )
        let refresher = GatedRefresher()
        let downloader = StubAutoDownloadEnqueuer()
        let settings = StubDownloadsSettingsProvider(setting: .off)
        let scheduler = StubTaskScheduler()

        let service = BackgroundFeedRefreshService(
            enumerator: enumerator,
            refresher: refresher,
            downloader: downloader,
            settingsProvider: settings,
            taskScheduler: scheduler
        )
        let task = StubBackgroundTask()

        // Kick off the handler; refresher is gated so it parks mid-loop.
        async let handlerFinished: Void = service.handleFeedRefreshTask(task)

        // Wait until the handler has installed its expiration handler.
        // A short poll is acceptable: the handler sets it before awaiting
        // `runRefreshOnce`, so it appears within a tick or two.
        for _ in 0..<200 {
            if task.expirationHandler != nil { break }
            try? await Task.sleep(nanoseconds: 1_000_000)  // 1ms
        }
        #expect(task.expirationHandler != nil,
                "Handler must install expirationHandler before awaiting work")

        // Simulate iOS firing expiration while the refresher is still
        // parked. This spawns a Task that hops to the actor; the actor
        // is currently parked at `await withCheckedContinuation` inside
        // the refresher, so it can accept the `markExpiredAndComplete`
        // reentrant message and complete the task with success=false.
        // A second completion from the normal-exit path after `release()`
        // would violate the iOS contract; the idempotence guard prevents
        // that.
        task.simulateExpiration()

        // Wait until the expiration hop has landed and called
        // `setTaskCompleted`. Without this sync point, the test race
        // lets `release()` wake the actor first and the normal-exit
        // path wins, masking a bug where the expiration Task never
        // actually runs.
        for _ in 0..<500 {
            if task.setTaskCompletedCallCount >= 1 { break }
            try? await Task.sleep(nanoseconds: 1_000_000)  // 1ms
        }
        #expect(task.setTaskCompletedCallCount == 1,
                "Expiration hop must complete the task before release")
        #expect(task.completedSuccess == false,
                "Expired handler must report success=false")

        // Release the gate so the handler can unwind. The post-release
        // completeTaskOnce must no-op because the guard is already set.
        await refresher.release()
        _ = await handlerFinished

        #expect(task.setTaskCompletedCallCount == 1,
                "Normal-exit path must NOT call setTaskCompleted a second time")
        #expect(task.completedSuccess == false,
                "Post-release completion must not overwrite the expired status")
    }
}

// MARK: - Backfill rearm (Gap-5)

@Suite("BackgroundFeedRefreshService — backfill rearm after refresh")
struct BackgroundFeedRefreshBackfillRearmTests {

    // playhead-5uvz.4 (Gap-5): regression — `BackgroundFeedRefreshService`
    // and `BackgroundProcessingService` register independent BGTaskScheduler
    // identifiers and have no shared lifecycle. A feed-refresh fire that
    // adds 4 downloads but doesn't explicitly arm a backfill task leaves
    // those 4 downloads unanalyzed until iOS happens to grant a backfill
    // window — empirically up to ~12h overnight (same class of gap as
    // playhead-fuo6's `appDidEnterBackground` fix).
    //
    // The fix wires `BackgroundFeedRefreshService.runRefreshOnce` to call
    // the injected `BackfillScheduling.scheduleBackfillIfNeeded()` after
    // any fire that enqueued at least one new download. iOS coalesces
    // duplicate submissions, so a stale outstanding request does not
    // cause double-work.
    @Test("Refresh that enqueues new downloads rearms the backfill BG task")
    func refreshWithNewDownloadsRearmsBackfill() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let backfillScheduler = StubBackfillScheduler()
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feed, existingEpisodeGUIDs: [])
            ],
            setting: .all,
            backfillScheduler: backfillScheduler
        )
        await refresher.setNewEpisodes(
            [
                newRecord(key: "ep-new-1", feed: feed),
                newRecord(key: "ep-new-2", feed: feed),
            ],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        // Sanity: downloads actually got enqueued.
        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(Set(enqueued) == Set(["ep-new-1", "ep-new-2"]))

        // The contract under test: rearm fires exactly once when the
        // fire enqueued downloads. Idempotent submission on iOS's side
        // means a second call would be safe, but the service shouldn't
        // be making redundant calls per fire — keep the count tight.
        let rearmCount = await backfillScheduler.scheduleCallCount
        #expect(rearmCount == 1,
                "Refresh that enqueued downloads must rearm backfill exactly once (got \(rearmCount))")
    }

    @Test("Refresh with no new downloads does not rearm backfill")
    func refreshWithNoNewDownloadsDoesNotRearm() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let backfillScheduler = StubBackfillScheduler()
        let (service, _, _, _, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feed, existingEpisodeGUIDs: ["ep-known"])
            ],
            setting: .all,
            backfillScheduler: backfillScheduler
        )
        // Refresher returns no new episodes — nothing to enqueue.

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        // No downloads were enqueued, so there's no new analysis work
        // for backfill to drain. Skipping the rearm here keeps the
        // BGTaskScheduler queue lean — iOS heuristics already deprioritize
        // tasks the app submits without them being needed, and avoiding
        // gratuitous submissions matters on an OS that throttles apps
        // which over-submit BG requests.
        let rearmCount = await backfillScheduler.scheduleCallCount
        #expect(rearmCount == 0,
                "Refresh that enqueued zero downloads must not rearm backfill (got \(rearmCount))")
    }

    @Test("Refresh with auto-download .off does not rearm even when new episodes are discovered")
    func refreshWithAutoDownloadOffDoesNotRearm() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let backfillScheduler = StubBackfillScheduler()
        let (service, _, refresher, downloader, _) = makeService(
            podcasts: [
                PodcastFeedSnapshot(feedURL: feed, existingEpisodeGUIDs: [])
            ],
            setting: .off,
            backfillScheduler: backfillScheduler
        )
        await refresher.setNewEpisodes(
            [newRecord(key: "ep-new-1", feed: feed)],
            for: feed
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        // No downloads under `.off`, so backfill has nothing to drain
        // from this fire. The rearm trigger is "did we enqueue downloads",
        // not "did we discover new episodes" — the latter is a UI signal,
        // the former is the analysis-pipeline signal.
        let enqueued = await downloader.enqueuedEpisodeIds()
        #expect(enqueued.isEmpty)
        let rearmCount = await backfillScheduler.scheduleCallCount
        #expect(rearmCount == 0,
                "Refresh under auto-download .off must not rearm backfill")
    }
}

// MARK: - Task identifier contract

@Suite("BackgroundFeedRefreshService — task identifier")
struct BackgroundFeedRefreshIdentifierTests {

    @Test("Task identifier is collision-free with existing BG task ids")
    func taskIdentifierDoesNotCollideWithExistingIds() {
        let fv2q = BackgroundFeedRefreshService.taskIdentifier
        #expect(fv2q != BackgroundTaskID.backfillProcessing)
        #expect(fv2q != BackgroundTaskID.continuedProcessing)
        #expect(fv2q != BackgroundTaskID.preAnalysisRecovery)
        #expect(fv2q.hasPrefix("com.playhead."),
                "Identifier must be under the com.playhead.* namespace")
    }
}

// MARK: - Shared telemetry holder (early-fire fallback wiring)

/// playhead-shpy / M2 (rfu-mn): pin the
/// `attachSharedTelemetry`/`detachSharedTelemetry` lock-holder contract
/// the early-fire fallback path relies on. The fallback closure inside
/// `registerTaskHandler` reads `sharedTelemetry.withLock { $0 }` to find
/// a logger; if that read returns nil the submit-or-fail outcome is
/// silently dropped. Driving the static directly (rather than synthesizing
/// a BGAppRefreshTask) keeps the test deterministic — exercising the
/// actual handler closure would require a real BGTaskScheduler dispatch.
@Suite("BackgroundFeedRefreshService — shared telemetry holder")
struct BackgroundFeedRefreshServiceSharedTelemetryTests {

    @Test("attachSharedTelemetry stores the logger in the lock-held holder")
    func attachStoresLoggerInHolder() async {
        // Defensive: clean state so a leak from another suite can't
        // mask an attach failure.
        BackgroundFeedRefreshService.detachSharedTelemetry()
        #expect(BackgroundFeedRefreshService.sharedTelemetryForTesting == nil)

        let recorder = SharedTelemetryRecordingLogger()
        BackgroundFeedRefreshService.attachSharedTelemetry(recorder)

        let held = BackgroundFeedRefreshService.sharedTelemetryForTesting
        #expect(held != nil)
        // Identity check: the held logger must be the exact instance
        // attached, not a copy. We pin this through a recorder
        // round-trip — recording an event into the held reference and
        // observing it on the original recorder proves they are the
        // same backing storage.
        let probe = BGTaskTelemetryEvent(
            ts: Date(),
            event: "submit",
            identifier: "probe",
            submitSucceeded: true
        )
        await held?.record(probe)
        let observed = await recorder.snapshot()
        #expect(observed.contains { $0.identifier == "probe" })

        BackgroundFeedRefreshService.detachSharedTelemetry()
    }

    @Test("detachSharedTelemetry nils out the holder")
    func detachClearsHolder() {
        let recorder = SharedTelemetryRecordingLogger()
        BackgroundFeedRefreshService.attachSharedTelemetry(recorder)
        #expect(BackgroundFeedRefreshService.sharedTelemetryForTesting != nil)

        BackgroundFeedRefreshService.detachSharedTelemetry()
        #expect(BackgroundFeedRefreshService.sharedTelemetryForTesting == nil)
    }

    @Test("attachSharedTelemetry is idempotent — later calls overwrite")
    func attachIsIdempotent() async {
        BackgroundFeedRefreshService.detachSharedTelemetry()

        let first = SharedTelemetryRecordingLogger()
        let second = SharedTelemetryRecordingLogger()
        BackgroundFeedRefreshService.attachSharedTelemetry(first)
        BackgroundFeedRefreshService.attachSharedTelemetry(second)

        // Probe lands in `second`, not `first`.
        let probe = BGTaskTelemetryEvent(
            ts: Date(),
            event: "submit",
            identifier: "probe-overwrite",
            submitSucceeded: true
        )
        let held = BackgroundFeedRefreshService.sharedTelemetryForTesting
        await held?.record(probe)

        let firstEvents = await first.snapshot()
        let secondEvents = await second.snapshot()
        #expect(firstEvents.isEmpty)
        #expect(secondEvents.contains { $0.identifier == "probe-overwrite" })

        BackgroundFeedRefreshService.detachSharedTelemetry()
    }
}

/// Local recorder for the shared-telemetry contract tests. Mirrors the
/// shape of `RecordingBGTaskTelemetryLogger` in
/// `BackgroundProcessingServiceTelemetryTests` but is declared in-file so
/// these tests don't pick up a cross-suite dependency. Backed by an
/// actor for thread-safe append/snapshot under structured concurrency.
private actor SharedTelemetryRecordingActor {
    private(set) var events: [BGTaskTelemetryEvent] = []

    func append(_ event: BGTaskTelemetryEvent) {
        events.append(event)
    }

    func snapshot() -> [BGTaskTelemetryEvent] {
        events
    }
}

private struct SharedTelemetryRecordingLogger: BGTaskTelemetryLogging {
    let inner = SharedTelemetryRecordingActor()

    func record(_ event: BGTaskTelemetryEvent) async {
        await inner.append(event)
    }

    func snapshot() async -> [BGTaskTelemetryEvent] {
        await inner.snapshot()
    }
}

// MARK: - Helpers

private func newRecord(
    key: String,
    feed: URL,
    published: Date? = nil
) -> NewEpisodeRecord {
    NewEpisodeRecord(
        canonicalEpisodeKey: key,
        audioURL: URL(string: "https://example.com/\(key).mp3")!,
        publishedAt: published
    )
}

/// Builds a production `FeedRefreshNewEpisode` for the shared-enqueue-path
/// tests, which drive `enqueueAutoDownloads(for:)` directly rather than
/// through the refresher stub (playhead-y5mk).
private func feedEpisode(
    key: String,
    feed: URL,
    published: Date? = nil
) -> FeedRefreshNewEpisode {
    FeedRefreshNewEpisode(
        canonicalEpisodeKey: key,
        feedURL: feed,
        audioURL: URL(string: "https://example.com/\(key).mp3")!,
        publishedAt: published
    )
}
