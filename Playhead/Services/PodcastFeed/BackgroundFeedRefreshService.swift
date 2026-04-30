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

/// playhead-snp: post-refresh hop that converts the union of all newly-
/// discovered episodes into local user notifications. Production wires
/// this to a `BackgroundFeedRefreshNewEpisodeAnnouncer` adapter that
/// hops onto the MainActor, resolves the SwiftData `Podcast` and
/// `Episode` rows for each `canonicalEpisodeKey`, and forwards to a
/// shared `NewEpisodeNotificationScheduler`. The default conformer is
/// a no-op so older test factories can construct the service without
/// caring about the notification surface.
protocol NewEpisodeAnnouncing: Sendable {
    func announce(newEpisodes: [FeedRefreshNewEpisode]) async
}

/// Default no-op announcer. Used when the runtime does not wire a real
/// announcer (e.g. unit tests that only care about the refresh + diff
/// + auto-download pipeline).
struct NoOpNewEpisodeAnnouncer: NewEpisodeAnnouncing {
    func announce(newEpisodes _: [FeedRefreshNewEpisode]) async {}
}

/// playhead-5uvz.4 (Gap-5): submits a backfill `BGProcessingTask` so
/// iOS wakes the app to drain the analysis queue after a feed refresh
/// just enqueued new downloads.
///
/// `BackgroundFeedRefreshService` and `BackgroundProcessingService`
/// register independent BGTaskScheduler identifiers and have no
/// shared lifecycle. A feed-refresh fire that adds 4 downloads but
/// finishes without explicitly arming a backfill task leaves those
/// 4 downloads unanalyzed until iOS happens to grant a backfill
/// window — empirically up to ~12h while the device is locked
/// overnight (same class of gap as playhead-fuo6).
///
/// Production wires this to `BackgroundProcessingService.scheduleBackfillIfNeeded()`
/// via `ProductionBackfillScheduler`. Tests inject a stub to assert the
/// rearm fires (or doesn't) without standing up the real scheduler.
protocol BackfillScheduling: Sendable {
    func scheduleBackfillIfNeeded() async
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

    /// Process-wide telemetry holder for the early-fire fallback path.
    /// `registerTaskHandler()` runs without `self`, so when the OS fires a
    /// refresh between runtime init and `attachSharedService(_:)`, the
    /// fallback path has no `bgTelemetry` member to call. Wiring this
    /// holder during runtime init (BEFORE `registerTaskHandler()`) lets
    /// the fallback's submit-or-fail outcome land in `bg-task-log.jsonl`
    /// instead of being silently discarded — closing the observability
    /// gap `BGTaskTelemetryLogger` exists for (playhead-shpy).
    ///
    /// Lifetime: process-global. The holder retains the supplied logger
    /// until either (a) another `attachSharedTelemetry(_:)` call replaces
    /// it (production has at most one runtime, so this is rare) or (b)
    /// `detachSharedTelemetry()` nils it out. Tests that construct a
    /// transient logger via `withTestRuntime { ... }` MUST detach in
    /// teardown; otherwise the static holds a dead recorder reference
    /// past the end of the test (bounded leak — one logger per test, no
    /// growth — but still visible in `xctest` heap snapshots).
    ///
    /// The existential is pinned to `any BGTaskTelemetryLogging & Sendable`
    /// so future protocol drift cannot weaken the Sendable invariant the
    /// fallback closure relies on (the closure escapes into a `Task { ... }`
    /// from a `nonisolated static` context — without `Sendable`, that
    /// capture would be a data race).
    private static let sharedTelemetry = OSAllocatedUnfairLock<(any BGTaskTelemetryLogging & Sendable)?>(initialState: nil)

    /// Static logger for the no-`self` fallback path. The instance-level
    /// `logger` is not reachable from `registerTaskHandler` (which runs
    /// before any `BackgroundFeedRefreshService` exists), so the early-
    /// fire fallback uses this one to surface submit failures into
    /// `os_log` even if the telemetry holder is unset.
    private static let staticLogger = Logger(subsystem: "com.playhead", category: "FeedRefresh")

    /// Install a live service instance as the target of the early-registered
    /// BGTask handler. Call from `PlayheadApp.task` once the ModelContainer
    /// has been threaded through into production collaborators. Idempotent;
    /// later calls overwrite earlier ones (the App only ever has one
    /// container, so in practice this is called at most once per launch).
    nonisolated static func attachSharedService(_ service: BackgroundFeedRefreshService) {
        sharedService.withLock { $0 = service }
    }

    /// Install a process-wide telemetry logger for the early-fire fallback
    /// to call before `attachSharedService(_:)` has run. Wire from
    /// `PlayheadRuntime` BEFORE `registerTaskHandler()` so the fallback
    /// always sees a live recorder if iOS dispatches a refresh in the
    /// micro-window between registration and service attach. Idempotent —
    /// later calls overwrite the held logger.
    nonisolated static func attachSharedTelemetry(_ telemetry: any BGTaskTelemetryLogging & Sendable) {
        sharedTelemetry.withLock { $0 = telemetry }
    }

    /// Nil out the process-wide telemetry holder. Tests that construct a
    /// transient logger via `withTestRuntime { ... }` should call this in
    /// teardown so the static doesn't hold a dead recorder past the end
    /// of the test. Production should never call this — the live logger
    /// has process lifetime.
    nonisolated static func detachSharedTelemetry() {
        sharedTelemetry.withLock { $0 = nil }
    }

    #if DEBUG
    /// Test-only accessor returning the currently-held shared telemetry
    /// logger. Used by `BackgroundFeedRefreshServiceTests` to pin the
    /// `attachSharedTelemetry` → `sharedTelemetry` lock contract without
    /// needing to trigger an actual BGTaskScheduler dispatch. Returns
    /// nil before any `attachSharedTelemetry(_:)` call (or after a
    /// `detachSharedTelemetry()`).
    nonisolated static var sharedTelemetryForTesting: (any BGTaskTelemetryLogging & Sendable)? {
        sharedTelemetry.withLock { $0 }
    }
    #endif

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
                //
                // playhead-shpy: this fallback path runs with no
                // service attached, but we still emit a `submit`
                // telemetry row through `sharedTelemetry` (when wired)
                // and surface failures via the static OSLog so the
                // window is no longer a silent observability hole.
                let nextRequest = BGAppRefreshTaskRequest(identifier: taskIdentifier)
                nextRequest.earliestBeginDate = Date(
                    timeIntervalSinceNow: minimumRefreshInterval
                )
                let earliestDelay = nextRequest.earliestBeginDate?.timeIntervalSinceNow
                let telemetry = sharedTelemetry.withLock { $0 }
                let submitSucceeded: Bool
                let submitError: Error?
                do {
                    try BGTaskScheduler.shared.submit(nextRequest)
                    submitSucceeded = true
                    submitError = nil
                } catch {
                    submitSucceeded = false
                    submitError = error
                    staticLogger.error(
                        "Early-fire fallback: BGTaskScheduler.submit failed: \(String(describing: error), privacy: .public)"
                    )
                }
                if let telemetry {
                    let errString = submitError.map { String(describing: $0) }
                    Task {
                        let phase = await BGTaskTelemetryScenePhase.current()
                        await telemetry.record(
                            .submit(
                                identifier: taskIdentifier,
                                succeeded: submitSucceeded,
                                error: errString,
                                earliestBeginDelaySec: earliestDelay,
                                scenePhase: phase,
                                detail: "feed-refresh-early-fire-fallback"
                            )
                        )
                    }
                }
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
    /// playhead-shpy: BG-task lifecycle telemetry. Defaults to a no-op
    /// so existing tests that construct the service without explicitly
    /// threading a logger keep working; production wiring in
    /// `PlayheadApp.task` supplies the live `BGTaskTelemetryLogger`.
    private let bgTelemetry: any BGTaskTelemetryLogging

    /// playhead-5uvz.4 (Gap-5): rearms the backfill BGProcessingTask
    /// when this fire enqueued at least one new download. nil-able so
    /// older test factories that don't care about the rearm path can
    /// continue to construct a service without a scheduler stub. In
    /// production the App-scope wiring always supplies one — see
    /// `ProductionBackfillScheduler`.
    private let backfillScheduler: (any BackfillScheduling)?

    /// playhead-snp: announcer hop invoked once per refresh fire when
    /// at least one new episode was discovered. Defaults to a no-op so
    /// older test factories don't have to thread an announcer through.
    private let newEpisodeAnnouncer: any NewEpisodeAnnouncing

    /// Actor-local expiration flag. iOS's `BGAppRefreshTask.expirationHandler`
    /// fires on an arbitrary system queue; the handler closure hops back
    /// onto the actor to flip this flag so the refresh loop can bail at
    /// its next per-feed boundary. No `Task.isCancelled` here: the task
    /// that runs `handleFeedRefreshTask` is the caller's (the
    /// registration closure's unstructured Task), which we don't own and
    /// must not cancel — cancellation would propagate into the refresher's
    /// URLSession call and produce unrelated transient errors.
    private var expired = false

    /// Idempotence guard for `setTaskCompleted`. iOS rules:
    ///   1. The expiration handler MUST call `setTaskCompleted(success:)`
    ///      before it returns or the system terminates the app.
    ///   2. Calling `setTaskCompleted` twice on the same task is
    ///      undefined behavior (Apple's "task is already completed"
    ///      InternalInconsistencyException has been seen in crash logs).
    /// Both the expiration path and the normal-exit path therefore go
    /// through `completeTaskOnce(_:success:)`, which flips this flag
    /// under the actor and only calls through to the underlying task
    /// the first time.
    private var taskCompleted = false

    // MARK: - Init

    init(
        enumerator: any PodcastEnumerating,
        refresher: any FeedRefreshing,
        downloader: any AutoDownloadEnqueueing,
        settingsProvider: any DownloadsSettingsProviding,
        taskScheduler: any BackgroundTaskScheduling = BGTaskScheduler.shared,
        backfillScheduler: (any BackfillScheduling)? = nil,
        bgTelemetry: any BGTaskTelemetryLogging = NoOpBGTaskTelemetryLogger(),
        newEpisodeAnnouncer: any NewEpisodeAnnouncing = NoOpNewEpisodeAnnouncer()
    ) {
        self.enumerator = enumerator
        self.refresher = refresher
        self.downloader = downloader
        self.settingsProvider = settingsProvider
        self.taskScheduler = taskScheduler
        self.backfillScheduler = backfillScheduler
        self.bgTelemetry = bgTelemetry
        self.newEpisodeAnnouncer = newEpisodeAnnouncer
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
        let earliestDelay = request.earliestBeginDate?.timeIntervalSinceNow
        do {
            try taskScheduler.submit(request)
            logger.info("Scheduled next feed refresh in \(Self.minimumRefreshInterval, privacy: .public)s")
            let identifier = Self.taskIdentifier
            let bgTelemetry = self.bgTelemetry
            Task {
                let phase = await BGTaskTelemetryScenePhase.current()
                await bgTelemetry.record(
                    .submit(
                        identifier: identifier,
                        succeeded: true,
                        earliestBeginDelaySec: earliestDelay,
                        scenePhase: phase,
                        detail: "feed-refresh-reschedule"
                    )
                )
            }
        } catch {
            logger.error("Failed to schedule feed refresh: \(String(describing: error), privacy: .public)")
            let identifier = Self.taskIdentifier
            let errString = String(describing: error)
            let bgTelemetry = self.bgTelemetry
            Task {
                let phase = await BGTaskTelemetryScenePhase.current()
                await bgTelemetry.record(
                    .submit(
                        identifier: identifier,
                        succeeded: false,
                        error: errString,
                        earliestBeginDelaySec: earliestDelay,
                        scenePhase: phase,
                        detail: "feed-refresh-reschedule"
                    )
                )
            }
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

        // playhead-shpy: emit `start` row before any work so the
        // log captures the dispatch even if the handler crashes.
        let identifier = Self.taskIdentifier
        let instanceID = bgTaskInstanceID(for: task as AnyObject)
        let bgTelemetry = self.bgTelemetry
        Task {
            let phase = await BGTaskTelemetryScenePhase.current()
            await bgTelemetry.record(
                .start(
                    identifier: identifier,
                    taskInstanceID: instanceID,
                    timeSinceSubmitSec: nil,
                    scenePhase: phase
                )
            )
        }

        // Reset per-fire flags. A test or long-lived process may reuse
        // the service across fires; the flags must not leak state from
        // a prior expiration/completion into this one.
        expired = false
        taskCompleted = false

        // Reschedule first — see spec: "Next refresh re-scheduled at
        // handler end (even on cancel)." Doing it up front makes that
        // contract unconditional even if the work path throws or is
        // interrupted mid-fire.
        scheduleNextRefresh()

        // The expiration callback fires on an arbitrary queue; bounce
        // through a Task so the flag mutation AND the `setTaskCompleted`
        // call both land on the actor. We MUST call `setTaskCompleted`
        // from the expiration path itself: iOS terminates the app if the
        // expiration handler returns without it, and iOS' post-expiration
        // grace is measured in a handful of seconds — too short to
        // reliably wait for `runRefreshOnce` to notice the flag and
        // unwind through an in-flight URLSession call. `completeTaskOnce`
        // guards against the normal-exit path calling `setTaskCompleted`
        // a second time.
        task.expirationHandler = { [weak self] in
            guard let self else { return }
            let taskBox = _UncheckedSendableBox(task)
            Task {
                let phase = await BGTaskTelemetryScenePhase.current()
                await self.bgTelemetry.record(
                    .expire(
                        identifier: Self.taskIdentifier,
                        taskInstanceID: instanceID,
                        timeInTaskSec: nil,
                        scenePhase: phase,
                        detail: "feed-refresh-expired"
                    )
                )
                await self.markExpiredAndComplete(taskBox.value)
            }
        }

        await runRefreshOnce()

        let succeeded = !expired
        completeTaskOnce(task, success: succeeded)
        logger.info("Feed refresh task completed (success=\(succeeded, privacy: .public))")
    }

    /// Expiration-path hop: flip the flag so `runRefreshOnce` bails at
    /// its next per-feed boundary AND call `setTaskCompleted(success: false)`
    /// through the idempotence guard so iOS' post-expiration termination
    /// timer doesn't fire. The normal-exit path that eventually returns
    /// from `runRefreshOnce` will also call `completeTaskOnce`, but
    /// `taskCompleted` will already be set and the second call is a no-op.
    private func markExpiredAndComplete(_ task: any BackgroundProcessingTaskProtocol) {
        expired = true
        logger.info("Feed refresh task expired — bailing at next boundary")
        completeTaskOnce(task, success: false)
    }

    /// Idempotent `setTaskCompleted`. Guards against the expiration and
    /// normal-exit paths both reaching the BG task's `setTaskCompleted`
    /// (which is undefined behavior per iOS contract). First caller wins.
    private func completeTaskOnce(
        _ task: any BackgroundProcessingTaskProtocol,
        success: Bool
    ) {
        guard !taskCompleted else { return }
        taskCompleted = true
        task.setTaskCompleted(success: success)
        // playhead-shpy: emit a `complete` row paired with the matching
        // `start` instance ID so the log captures every terminal event,
        // including the normal-exit + expiration-race path that goes
        // through this idempotence guard.
        let instanceID = bgTaskInstanceID(for: task as AnyObject)
        let bgTelemetry = self.bgTelemetry
        Task {
            let phase = await BGTaskTelemetryScenePhase.current()
            await bgTelemetry.record(
                .complete(
                    identifier: Self.taskIdentifier,
                    taskInstanceID: instanceID,
                    success: success,
                    timeInTaskSec: nil,
                    scenePhase: phase
                )
            )
        }
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
        var enqueuedCount = 0
        for episode in toDownload {
            if expired { break }
            await downloader.enqueueBackgroundDownload(
                episodeId: episode.canonicalEpisodeKey,
                from: episode.audioURL
            )
            enqueuedCount += 1
        }
        logger.info(
            "Feed refresh: \(allNewEpisodes.count, privacy: .public) new episodes, \(toDownload.count, privacy: .public) enqueued (setting=\(setting.rawValue, privacy: .public))"
        )

        // playhead-5uvz.4 (Gap-5): if this fire enqueued any new
        // downloads, ask `BackgroundProcessingService` to submit a
        // backfill BGProcessingTask so iOS wakes the app to drain the
        // analysis queue. Without this hop, the feed-refresh and
        // backfill schedulers have independent lifecycles and the
        // device can stall for hours before backfill gets a window
        // (overnight blackout class of bug — same shape as fuo6).
        // Skip the rearm when no downloads were enqueued: a refresh
        // that found nothing new doesn't change the backfill backlog.
        if enqueuedCount > 0 {
            await backfillScheduler?.scheduleBackfillIfNeeded()
        }

        // playhead-snp: announce newly-discovered episodes. The
        // announcer is responsible for resolving feed/episode metadata,
        // applying user toggles, dedup, rate limit, and authorization.
        // Skip the call when nothing new surfaced (clean no-op cap on
        // an idle refresh) and when the BG task has expired so we don't
        // run additional MainActor work past the post-expiration grace.
        if !allNewEpisodes.isEmpty && !expired {
            await newEpisodeAnnouncer.announce(newEpisodes: allNewEpisodes)
        }
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

/// Production `BackfillScheduling` backed by `BackgroundProcessingService`.
/// Forwards `scheduleBackfillIfNeeded()` to the live BPS so a feed-refresh
/// fire that just enqueued downloads also rearms the backfill BGProcessingTask
/// (playhead-5uvz.4 / Gap-5). Holding the BPS through a small wrapper rather
/// than threading the actor itself keeps the BackgroundFeedRefreshService
/// dependency surface narrow — the feed-refresh service has no other reason
/// to know about BPS internals.
struct ProductionBackfillScheduler: BackfillScheduling {
    let backgroundProcessingService: BackgroundProcessingService

    func scheduleBackfillIfNeeded() async {
        await backgroundProcessingService.scheduleBackfillIfNeeded()
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
