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
