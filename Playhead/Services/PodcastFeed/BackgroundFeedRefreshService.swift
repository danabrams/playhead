// BackgroundFeedRefreshService.swift
// playhead-fv2q — periodic BGAppRefreshTask driver for subscribed
// podcast feed refresh + auto-download of newly-published episodes.
//
// Why a sibling service (not a method on BackgroundProcessingService):
//   - Different task class: `BGAppRefreshTask` (short, periodic, no
//     external-power gate) vs `BGProcessingTask` (longer, discretionary,
//     can require power/network).
//   - Different work: feed fetch + download enqueue is completely
//     disjoint from analysis backfill. Keeping them apart means a bug
//     here cannot bring down the analysis scheduler and vice versa.
//   - Independent expiration + reschedule cadence (1-hour floor vs the
//     BPS processing-task cadence governed by iOS heuristics).
//
// Background-launch safety (crash-debug learnings from commit 7a5c30b):
//   - `BGAppRefreshTask` handlers can fire in a "Non UI" launch envelope
//     while the device is locked. `FileProtectionType.complete` files
//     are UNREADABLE in that envelope, which is why `AnalysisStore`
//     lives under `.completeUntilFirstUserAuthentication`.
//   - This handler therefore uses ZERO `try!` on file/db paths. Any
//     thrown error from a collaborator is logged and swallowed; the
//     handler always calls `setTaskCompleted(success:)` and always
//     reschedules before starting work, so a pre-first-unlock launch
//     just schedules the next fire and bails gracefully.
//
// Main-actor hop model: iOS invokes the BGTaskScheduler launch handler
// on a system queue. The registration closure hops into an unstructured
// `Task { await self.handleFeedRefreshTask(task) }`. The handler itself
// is actor-isolated, which serializes overlapping fires but is NOT main
// actor. Every SwiftData read/write is performed inside a
// `@MainActor`-annotated collaborator (`ProductionPodcastEnumerator`
// and `ProductionFeedRefresher`), so the `Podcast` / `Episode` model
// objects never leak off-main. The actor hands the collaborators only
// `Sendable` value snapshots (URLs, GUID sets, canonical keys).

import BackgroundTasks
import Foundation
import os
import OSLog
import SwiftData
import UIKit

// `BGAppRefreshTask` is only ever surfaced to
// `BackgroundFeedRefreshService` through the shared
// `BackgroundProcessingTaskProtocol` facade so tests can drive the
// handler with `StubBackgroundTask`. Conformance is declared here
// (rather than alongside `BGProcessingTask: BackgroundProcessingTaskProtocol`
// in `BackgroundProcessingService.swift`) to keep the BGTask-class
// extension with the service that actually uses it.
extension BGAppRefreshTask: BackgroundProcessingTaskProtocol {}

// MARK: - Collaborator value types

/// Pre-refresh snapshot of a single podcast. Enumerator returns an array
/// of these so the refresh loop can drive each feed without holding a
/// SwiftData ModelContext. The GUID set is the denominator the handler
/// passes to the refresher so the diff-against-existing happens at the
/// layer that actually fetched the feed.
struct FeedRefreshPodcastSnapshot: Sendable, Equatable {
    let feedURL: URL
    let existingEpisodeGUIDs: Set<String>
}

/// A newly-discovered episode, surfaced by the refresher to the handler.
/// Minimal shape — only the bits the downloader needs plus a publish
/// date so `.last1` / `.last3` can do deterministic newest-first cuts.
struct FeedRefreshNewEpisode: Sendable, Equatable {
    let canonicalEpisodeKey: String
    let audioURL: URL
    let publishedAt: Date?
}

// MARK: - Collaborator protocols

/// Returns a pre-refresh snapshot of every subscribed podcast.
///
/// Production implementation (`ProductionPodcastEnumerator`) is
/// `@MainActor`-annotated and fetches from the live `ModelContext`.
/// Test doubles return scripted snapshots so the handler can be
/// exercised without any SwiftData wiring.
protocol PodcastEnumerating: Sendable {
    func enumeratePodcasts() async -> [FeedRefreshPodcastSnapshot]
}

/// Re-fetches a single feed and reports which episodes are new.
///
/// Production implementation (`ProductionFeedRefresher`) is
/// `@MainActor`-bound so it can call `PodcastDiscoveryService.refreshEpisodes(for:in:)`
/// directly; the handler is off-main and hops in via the async-await
/// boundary implied by `@MainActor` on the conforming type.
protocol FeedRefreshing: Sendable {
    func refreshEpisodes(
        feedURL: URL,
        existingEpisodeGUIDs: Set<String>
    ) async throws -> [FeedRefreshNewEpisode]
}

/// Enqueues a background pre-cache download for a newly-discovered
/// episode. Production wires this to `DownloadManager.backgroundDownload`.
protocol AutoDownloadEnqueueing: Sendable {
    func enqueueBackgroundDownload(episodeId: String, from url: URL) async
}

/// Provides the user's current `AutoDownloadOnSubscribe` setting.
/// Reads `DownloadsSettings.load()` in production; tests inject a
/// fixed value to avoid UserDefaults state.
protocol DownloadsSettingsProviding: Sendable {
    func currentAutoDownloadSetting() -> AutoDownloadOnSubscribe
}

// MARK: - BackgroundFeedRefreshService

/// Drives the `BGAppRefreshTask` that periodically re-fetches every
/// subscribed feed and enqueues downloads for newly-published episodes
/// when the user has opted in via Settings → Downloads → Auto-download
/// on subscribe.
actor BackgroundFeedRefreshService {

    /// Task identifier submitted to `BGTaskScheduler`. Must also be
    /// declared in `Info.plist`'s `BGTaskSchedulerPermittedIdentifiers`
    /// and in `project.yml` so the Info.plist regen keeps them in sync.
    /// Kept under the `com.playhead.app.` namespace per the project
    /// convention shared with `BackgroundTaskID` in
    /// `BackgroundProcessingService.swift`.
    static let taskIdentifier: String = "com.playhead.app.feed-refresh"

    /// Minimum interval iOS should wait before firing the next refresh.
    /// iOS is free to defer further based on usage patterns; 1 hour is
    /// a reasonable lower bound that matches the product spec ("periodic
    /// feed refresh for newly-published episodes").
    static let minimumRefreshInterval: TimeInterval = 60 * 60

    /// Process-wide guard so tests that construct multiple
    /// `BackgroundFeedRefreshService` instances in a single process
    /// (e.g. the test host's real `PlayheadApp` launch followed by a
    /// custom runtime) do not re-register with `BGTaskScheduler` and
    /// trigger its `NSInternalInconsistencyException`. Mirrors the
    /// `registerOnce()` pattern in `BackgroundProcessingService`.
    private static let registrationFlag = OSAllocatedUnfairLock<Bool>(initialState: false)

    nonisolated static func registerOnce() -> Bool {
        registrationFlag.withLock { flag in
            if flag { return false }
            flag = true
            return true
        }
    }

    /// Process-wide holder for the live service instance. Populated by
    /// `attachSharedService(_:)` after the `ModelContainer` becomes
    /// available in App scope. Read by the early-registered BGTask
    /// handler to route fires to a real service — or, if the holder is
    /// still nil when a fire lands (e.g. an OS-scheduled fire between
    /// runtime init and the App-scope `.task`), to fall back to
    /// scheduling the next fire and completing the task gracefully.
    ///
    /// The indirection exists because iOS requires `BGTaskScheduler.register`
    /// to complete before launch finishes, but the production service's
    /// SwiftData `ModelContainer` dependency only becomes visible after
    /// `PlayheadApp.init` has constructed both the runtime and the
    /// container. Registering at runtime init with a lock-protected
    /// holder is the minimal wiring that satisfies both constraints
    /// without a second registration call (which BGTaskScheduler would
    /// crash on).
    private static let sharedService = OSAllocatedUnfairLock<BackgroundFeedRefreshService?>(initialState: nil)

    /// Install a live service instance as the target of the early-registered
    /// BGTask handler. Call from `PlayheadApp.task` once the ModelContainer
    /// has been threaded through into production collaborators. Idempotent;
    /// later calls overwrite earlier ones (the App only ever has one
    /// container, so in practice this is called at most once per launch).
    nonisolated static func attachSharedService(_ service: BackgroundFeedRefreshService) {
        sharedService.withLock { $0 = service }
    }

    /// Early BGTask registration. Call from `PlayheadRuntime.init` (NOT
    /// from App `.task`, which runs after launch completes). The
    /// handler resolves the live service through `sharedService` at
    /// dispatch time; if the service has not yet been attached, the
    /// fire is turned into an immediate reschedule + `success: false`.
    ///
    /// Separate from the instance-level `registerBackgroundTasks()`
    /// because the early path has no `self` to bind — the service
    /// instance may not exist yet when the first BGTaskScheduler
    /// callback fires.
    nonisolated static func registerTaskHandler() {
        guard registerOnce() else { return }
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: taskIdentifier,
            using: nil
        ) { task in
            guard let refreshTask = task as? BGAppRefreshTask else {
                task.setTaskCompleted(success: false)
                return
            }
            guard let service = sharedService.withLock({ $0 }) else {
                // Service not yet attached — schedule next directly and
                // bail. Happens if iOS fires the refresh between runtime
                // init and the App-scope `.task` that attaches the
                // service, which is a vanishingly small window but we
                // must handle it without crashing. Submit is best-effort
                // since BGTaskScheduler may itself reject in that window.
                let nextRequest = BGAppRefreshTaskRequest(identifier: taskIdentifier)
                nextRequest.earliestBeginDate = Date(
                    timeIntervalSinceNow: minimumRefreshInterval
                )
                try? BGTaskScheduler.shared.submit(nextRequest)
                task.setTaskCompleted(success: false)
                return
            }
            let box = _UncheckedSendableBox(refreshTask)
            Task { await service.handleFeedRefreshTask(box.value) }
        }
    }

    private let logger = Logger(subsystem: "com.playhead", category: "FeedRefresh")

    // MARK: - Dependencies

    private let enumerator: any PodcastEnumerating
    private let refresher: any FeedRefreshing
    private let downloader: any AutoDownloadEnqueueing
    private let settingsProvider: any DownloadsSettingsProviding
    private let taskScheduler: any BackgroundTaskScheduling

    /// Actor-local expiration flag. iOS's `BGAppRefreshTask.expirationHandler`
    /// fires on an arbitrary system queue; the handler closure hops back
    /// onto the actor to flip this flag so the refresh loop can bail at
    /// its next per-feed boundary. No `Task.isCancelled` here: the task
    /// that runs `handleFeedRefreshTask` is the caller's (the
    /// registration closure's unstructured Task), which we don't own and
    /// must not cancel — cancellation would propagate into the refresher's
    /// URLSession call and produce unrelated transient errors.
    private var expired = false

    // MARK: - Init

    init(
        enumerator: any PodcastEnumerating,
        refresher: any FeedRefreshing,
        downloader: any AutoDownloadEnqueueing,
        settingsProvider: any DownloadsSettingsProviding,
        taskScheduler: any BackgroundTaskScheduling = BGTaskScheduler.shared
    ) {
        self.enumerator = enumerator
        self.refresher = refresher
        self.downloader = downloader
        self.settingsProvider = settingsProvider
        self.taskScheduler = taskScheduler
    }

    // MARK: - Start

    /// Start the service: schedule the first refresh. Call once at
    /// launch from the App-scope task that also calls
    /// `attachSharedService(_:)`. Safe to call before attach; the
    /// submission itself does not depend on the service instance being
    /// visible through the shared holder.
    nonisolated func start() {
        Task { await self.scheduleNextRefresh() }
    }

    // MARK: - Scheduling

    /// Submit a `BGAppRefreshTaskRequest` for the next fire. iOS enforces
    /// the earliest-begin-date as a soft floor; actual dispatch still
    /// depends on its own heuristics (battery, network, usage patterns).
    ///
    /// Failure is logged and swallowed: the user's worst case is missing
    /// one refresh cycle, and Library pull-to-refresh remains as a
    /// manual fallback. A thrown reschedule is NOT fatal to the current
    /// handler fire — see `handleFeedRefreshTask` for why we always
    /// reschedule first and still complete the task.
    func scheduleNextRefresh() {
        let request = BGAppRefreshTaskRequest(identifier: Self.taskIdentifier)
        request.earliestBeginDate = Date(
            timeIntervalSinceNow: Self.minimumRefreshInterval
        )
        do {
            try taskScheduler.submit(request)
            logger.info("Scheduled next feed refresh in \(Self.minimumRefreshInterval, privacy: .public)s")
        } catch {
            logger.error("Failed to schedule feed refresh: \(String(describing: error), privacy: .public)")
        }
    }

    // MARK: - Handler

    /// Drive one feed-refresh fire:
    ///   1. Reschedule the next fire *before* any work so iOS always
    ///      has a pending request even if the current handler crashes.
    ///   2. Install an expiration handler that flips `expired` on the
    ///      actor so the refresh loop bails at its next per-feed
    ///      boundary (no `Task.cancel()` — we don't own the enclosing
    ///      task, and cancellation would bleed into URLSession).
    ///   3. Enumerate subscribed podcasts (pre-refresh GUID snapshots).
    ///   4. For each podcast, refresh the feed and collect newly-seen
    ///      episodes. Per-feed errors are swallowed so one bad feed
    ///      cannot abort the rest.
    ///   5. If auto-download is enabled, pick a newest-first subset of
    ///      new episodes per the user's setting and enqueue each.
    ///   6. Mark the task complete (`success: !expired`).
    func handleFeedRefreshTask(_ task: any BackgroundProcessingTaskProtocol) async {
        logger.info("Feed refresh task started")

        // Reset the per-fire expiration flag. A test or long-lived process
        // may reuse the service across fires; the flag must not leak state
        // from a prior expiration into this one.
        expired = false

        // Reschedule first — see spec: "Next refresh re-scheduled at
        // handler end (even on cancel)." Doing it up front makes that
        // contract unconditional even if the work path throws or is
        // interrupted mid-fire.
        scheduleNextRefresh()

        // The expiration callback fires on an arbitrary queue; bounce
        // through a Task so the flag mutation lands on the actor.
        task.expirationHandler = { [weak self] in
            guard let self else { return }
            Task { await self.markExpired() }
        }

        await runRefreshOnce()

        let succeeded = !expired
        task.setTaskCompleted(success: succeeded)
        logger.info("Feed refresh task completed (success=\(succeeded, privacy: .public))")
    }

    /// Flip the expiration flag. Called from the expiration callback's
    /// Task hop so actor isolation is preserved.
    private func markExpired() {
        expired = true
        logger.info("Feed refresh task expired — bailing at next boundary")
    }

    /// Core work of a single fire. Factored out so a future test can
    /// drive it without a stub task if/when that becomes useful.
    private func runRefreshOnce() async {
        let podcasts = await enumerator.enumeratePodcasts()
        logger.info("Refreshing \(podcasts.count, privacy: .public) subscribed podcasts")

        var allNewEpisodes: [FeedRefreshNewEpisode] = []
        for snapshot in podcasts {
            if expired { break }
            do {
                let newEpisodes = try await refresher.refreshEpisodes(
                    feedURL: snapshot.feedURL,
                    existingEpisodeGUIDs: snapshot.existingEpisodeGUIDs
                )
                allNewEpisodes.append(contentsOf: newEpisodes)
            } catch {
                // Swallow per-feed errors — partial refresh is better
                // than none, and transient network failures on one feed
                // shouldn't block every other subscription. Matches the
                // spec: "Per-podcast refresh error is caught — other
                // feeds still refresh and download."
                logger.error(
                    "Refresh failed for \(snapshot.feedURL.absoluteString, privacy: .public): \(String(describing: error), privacy: .public)"
                )
            }
        }

        let setting = settingsProvider.currentAutoDownloadSetting()
        let toDownload = Self.pickEpisodesForDownload(
            newEpisodes: allNewEpisodes,
            setting: setting
        )
        for episode in toDownload {
            if expired { break }
            await downloader.enqueueBackgroundDownload(
                episodeId: episode.canonicalEpisodeKey,
                from: episode.audioURL
            )
        }
        logger.info(
            "Feed refresh: \(allNewEpisodes.count, privacy: .public) new episodes, \(toDownload.count, privacy: .public) enqueued (setting=\(setting.rawValue, privacy: .public))"
        )
    }

    /// Pure selection rule. Separated so tests can pin the ordering
    /// contract without driving the full handler. Newest-first cut for
    /// `.last1` / `.last3`; all-or-nothing for `.all` / `.off`. Episodes
    /// with a nil `publishedAt` are treated as oldest so a refresh that
    /// surfaces a fresh episode alongside an old one without a date
    /// still picks the dated one first under `.lastN`.
    static func pickEpisodesForDownload(
        newEpisodes: [FeedRefreshNewEpisode],
        setting: AutoDownloadOnSubscribe
    ) -> [FeedRefreshNewEpisode] {
        switch setting {
        case .off:
            return []
        case .all:
            return newEpisodes
        case .last1, .last3:
            let limit = (setting == .last1) ? 1 : 3
            // Stable newest-first sort: sort by publishedAt descending,
            // nil treated as .distantPast so missing-date episodes sink
            // to the tail.
            let sorted = newEpisodes.sorted { lhs, rhs in
                let lDate = lhs.publishedAt ?? .distantPast
                let rDate = rhs.publishedAt ?? .distantPast
                return lDate > rDate
            }
            return Array(sorted.prefix(limit))
        }
    }
}

// MARK: - Production collaborators

/// Production `PodcastEnumerating` backed by a SwiftData `ModelContext`.
/// `@MainActor`-bound because `ModelContext` is, and the service contract
/// requires the returned snapshot be usable off-main (it is — the
/// snapshot is a value type holding only `Sendable` fields).
///
/// A SwiftData fetch that throws is logged and yields an empty
/// snapshot — the handler treats "no podcasts" as "no work", which is
/// the correct fallback when (e.g.) a pre-first-unlock BGTask launch
/// can't reach the store.
@MainActor
struct ProductionPodcastEnumerator: PodcastEnumerating {
    let modelContainer: ModelContainer

    func enumeratePodcasts() async -> [FeedRefreshPodcastSnapshot] {
        let context = modelContainer.mainContext
        let descriptor = FetchDescriptor<Podcast>()
        do {
            let podcasts = try context.fetch(descriptor)
            return podcasts.map { podcast in
                FeedRefreshPodcastSnapshot(
                    feedURL: podcast.feedURL,
                    existingEpisodeGUIDs: Set(podcast.episodes.map(\.feedItemGUID))
                )
            }
        } catch {
            Logger(subsystem: "com.playhead", category: "FeedRefresh")
                .error("Podcast enumeration failed: \(String(describing: error), privacy: .public)")
            return []
        }
    }
}

/// Production `FeedRefreshing` backed by `PodcastDiscoveryService`.
/// `@MainActor`-bound because `PodcastDiscoveryService.refreshEpisodes(for:in:)`
/// is `@MainActor` (it operates on a live `ModelContext`). The conforming
/// type is `Sendable`-by-isolation — no cross-actor state escapes.
@MainActor
struct ProductionFeedRefresher: FeedRefreshing {
    let discoveryService: PodcastDiscoveryService
    let modelContainer: ModelContainer

    func refreshEpisodes(
        feedURL: URL,
        existingEpisodeGUIDs _: Set<String>
    ) async throws -> [FeedRefreshNewEpisode] {
        // Re-resolve the podcast inside the MainActor hop; the handler
        // only holds URL + GUID snapshots. The `existingEpisodeGUIDs`
        // argument is informational for test doubles — the production
        // discovery service computes its own diff from the live
        // `Podcast.episodes` relationship and returns only new rows.
        let context = modelContainer.mainContext
        let podcasts = try context.fetch(FetchDescriptor<Podcast>())
        guard let podcast = podcasts.first(where: { $0.feedURL == feedURL }) else {
            return []  // Subscription deleted mid-refresh — nothing to do.
        }
        let newEpisodes = try await discoveryService.refreshEpisodes(
            for: podcast,
            in: context
        )
        // Flatten to Sendable value type so the result can cross back
        // to the (non-main) service actor.
        return newEpisodes.map {
            FeedRefreshNewEpisode(
                canonicalEpisodeKey: $0.canonicalEpisodeKey,
                audioURL: $0.audioURL,
                publishedAt: $0.publishedAt
            )
        }
    }
}

/// Production `AutoDownloadEnqueueing` backed by `DownloadManager`.
/// Thin wrapper so tests can observe enqueue calls without standing up a
/// real URLSession background session.
struct ProductionAutoDownloadEnqueuer: AutoDownloadEnqueueing {
    let downloadManager: DownloadManager

    func enqueueBackgroundDownload(episodeId: String, from url: URL) async {
        await downloadManager.backgroundDownload(episodeId: episodeId, from: url)
    }
}

/// Production `DownloadsSettingsProviding` backed by UserDefaults via
/// `DownloadsSettings.load()`. Reads lazily so a setting change in
/// Settings is picked up on the very next refresh fire.
struct ProductionDownloadsSettingsProvider: DownloadsSettingsProviding {
    func currentAutoDownloadSetting() -> AutoDownloadOnSubscribe {
        DownloadsSettings.load().autoDownloadOnSubscribe
    }
}

// MARK: - UncheckedSendable helper

/// Local re-declaration so this file does not need to reach into
/// `BackgroundProcessingService.swift`'s `private` helper of the same
/// shape. Both wrappers are pure documentation — the developer is
/// asserting the wrapped value crosses isolation boundaries exactly
/// once (the BG task hand-off) and is accessed on only one actor
/// thereafter.
private struct _UncheckedSendableBox<T>: @unchecked Sendable {
    let value: T
    init(_ value: T) { self.value = value }
}
