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
private struct PodcastFeedSnapshot: Sendable, Equatable {
    let feedURL: URL
    let existingEpisodeGUIDs: Set<String>
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
                existingEpisodeGUIDs: $0.existingEpisodeGUIDs
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
        return records.map {
            FeedRefreshNewEpisode(
                canonicalEpisodeKey: $0.canonicalEpisodeKey,
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

// MARK: - Factory

private func makeService(
    podcasts: [PodcastFeedSnapshot] = [],
    setting: AutoDownloadOnSubscribe = .off,
    scheduler: StubTaskScheduler = StubTaskScheduler()
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
        taskScheduler: scheduler
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
