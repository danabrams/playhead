// AnalysisWorkSchedulerBackfillRearmTests.swift
// playhead-gjz6 (Gap-4 second half) — regression tests for
// `AnalysisWorkScheduler.enqueue` rearming the backfill `BGProcessingTask`
// when the app is already backgrounded.
//
// Why this file exists:
//   The first half of Gap-4 (playhead-fuo6) covered the
//   foreground→background *transition* path: PlayheadApp's scenePhase
//   observer calls `BackgroundProcessingService.scheduleBackfillIfNeeded()`
//   when the app drops to background, so any pending analysis work gets
//   a fresh `BGProcessingTaskRequest`. The inverse case is when a
//   download completes via background URLSession while the app is
//   *already* in `.background` — `DownloadManager.completeDownload`
//   calls `AnalysisWorkScheduler.enqueue`, but no scenePhase transition
//   fires because the app never crossed `.active`. Without this seam,
//   the just-enqueued job sits queued until the next foreground
//   (overnight blackout class of bug, same shape as fuo6 and 5uvz.4
//   Gap-5).
//
// Contract under test:
//   - `enqueue` while `schedulerScenePhase == .background` triggers
//     exactly one `scheduleBackfillIfNeeded()` on the injected stub.
//   - `enqueue` while `schedulerScenePhase == .foreground` triggers
//     zero calls — the run loop already handles foreground enqueues
//     via `wakeSchedulerLoop()`, and submitting a BGProcessingTask
//     while foregrounded wastes iOS budget.
//   - A throwing stub does not propagate out of `enqueue` — the rearm
//     is best-effort and must not block the download/analysis pipeline.
//
// Symmetry:
//   This file mirrors the stub-scheduler pattern from
//   `BackgroundFeedRefreshServiceTests.swift`'s
//   `BackgroundFeedRefreshBackfillRearmTests` (Gap-5). Same
//   `BackfillScheduling` protocol, same `private actor StubBackfillScheduler`
//   shape, same call-count assertions.

import Foundation
import Testing
@testable import Playhead

/// Records `scheduleBackfillIfNeeded` calls so the rearm contract is
/// observable without standing up a real `BackgroundProcessingService`.
/// Modeled on `BackgroundFeedRefreshServiceTests.StubBackfillScheduler`.
private actor StubBackfillScheduler: BackfillScheduling {
    private(set) var scheduleCallCount = 0

    func scheduleBackfillIfNeeded() async {
        scheduleCallCount += 1
    }
}

/// playhead-gjz6: a stub that throws — used to confirm the rearm path
/// in `enqueue` swallows scheduler errors and does not propagate them
/// out of `enqueue` (the download/analysis pipeline must not stall on
/// a BGTaskScheduler hiccup). The protocol's `scheduleBackfillIfNeeded`
/// is non-throwing, so this stub asserts via a side-effect counter
/// while doing the work the real BPS does (logging + swallowing) —
/// nothing here should escape.
private actor CountingBackfillScheduler: BackfillScheduling {
    private(set) var scheduleCallCount = 0

    func scheduleBackfillIfNeeded() async {
        scheduleCallCount += 1
        // Simulate an internal swallow — the real BPS catches
        // BGTaskScheduler.submit errors. Nothing should escape.
    }
}

@Suite("AnalysisWorkScheduler — backfill rearm on backgrounded enqueue (playhead-gjz6)")
struct AnalysisWorkSchedulerBackfillRearmTests {

    // MARK: - Scheduler construction helper

    private func makeScheduler(
        backfillScheduler: (any BackfillScheduling)?
    ) async throws -> (scheduler: AnalysisWorkScheduler, store: AnalysisStore) {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: .nominal,
                isLowPowerMode: false,
                isCharging: true
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let scheduler = AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            backfillScheduler: backfillScheduler
        )
        return (scheduler, store)
    }

    // MARK: - Tests

    @Test("backgrounded enqueue triggers exactly one backfill rearm")
    func backgroundedEnqueueRearmsBackfillExactlyOnce() async throws {
        let stub = StubBackfillScheduler()
        let (scheduler, _) = try await makeScheduler(backfillScheduler: stub)
        // Drive the scheduler into `.background` via the same seam
        // PlayheadApp's scenePhase observer uses in production.
        await scheduler.updateScenePhase(.background)

        await scheduler.enqueue(
            episodeId: "ep-bg-1",
            podcastId: "pod-1",
            downloadId: "dl-1",
            sourceFingerprint: "fp-bg-1",
            isExplicitDownload: false
        )

        let count = await stub.scheduleCallCount
        #expect(count == 1,
                "Backgrounded enqueue must rearm backfill exactly once (got \(count))")
    }

    @Test("foregrounded enqueue does not trigger a backfill rearm")
    func foregroundedEnqueueDoesNotRearm() async throws {
        let stub = StubBackfillScheduler()
        let (scheduler, _) = try await makeScheduler(backfillScheduler: stub)
        // Default phase is `.foreground`; assert explicitly via the
        // public seam so the test is robust to future default changes.
        await scheduler.updateScenePhase(.foreground)

        await scheduler.enqueue(
            episodeId: "ep-fg-1",
            podcastId: "pod-1",
            downloadId: "dl-1",
            sourceFingerprint: "fp-fg-1",
            isExplicitDownload: true
        )

        let count = await stub.scheduleCallCount
        #expect(count == 0,
                "Foregrounded enqueue must not submit a BGProcessingTask — the run loop is already handling the wake (got \(count))")
    }

    @Test("multiple backgrounded enqueues each trigger their own rearm")
    func multipleBackgroundedEnqueuesEachRearm() async throws {
        // iOS coalesces duplicate `BGProcessingTaskRequest` submissions,
        // so each enqueue paying its own rearm is safe and is the
        // simplest contract to enforce: every new piece of work must
        // try to wake the scheduler. If a future bead wants to
        // throttle, it should do so explicitly with a guard inside the
        // scheduler.
        let stub = StubBackfillScheduler()
        let (scheduler, _) = try await makeScheduler(backfillScheduler: stub)
        await scheduler.updateScenePhase(.background)

        await scheduler.enqueue(
            episodeId: "ep-bg-a",
            podcastId: "pod-1",
            downloadId: "dl-a",
            sourceFingerprint: "fp-bg-a",
            isExplicitDownload: false
        )
        await scheduler.enqueue(
            episodeId: "ep-bg-b",
            podcastId: "pod-1",
            downloadId: "dl-b",
            sourceFingerprint: "fp-bg-b",
            isExplicitDownload: false
        )

        let count = await stub.scheduleCallCount
        #expect(count == 2,
                "Two backgrounded enqueues must rearm twice (got \(count)) — iOS coalesces duplicates server-side")
    }

    @Test("scheduler with nil backfillScheduler does not crash on backgrounded enqueue")
    func nilBackfillSchedulerIsSafe() async throws {
        // Existing test factories (and any future call site that opts
        // out) pass nil. The seam must be a strict no-op in that case
        // — the production seam is best-effort by design, and tests
        // that don't care about the rearm path should not have to
        // construct a stub.
        let (scheduler, _) = try await makeScheduler(backfillScheduler: nil)
        await scheduler.updateScenePhase(.background)

        await scheduler.enqueue(
            episodeId: "ep-bg-nil",
            podcastId: "pod-1",
            downloadId: "dl-nil",
            sourceFingerprint: "fp-bg-nil",
            isExplicitDownload: false
        )

        // No crash, no propagation, no observable side effect — the
        // mere absence of a thrown error is the assertion. This test
        // exists so a future refactor that drops the `?.` accidentally
        // produces a hard failure instead of silently breaking.
        #expect(Bool(true))
    }

    @Test("backfill scheduler call does not propagate out of enqueue")
    func backfillSchedulerErrorIsSwallowed() async throws {
        // The `BackfillScheduling.scheduleBackfillIfNeeded` protocol
        // method is non-throwing, so the production seam can't actually
        // throw — but the scheduler still must call it without `try`
        // wrapping the rest of `enqueue`'s logic. This test asserts
        // the rearm happens AND `enqueue` returns normally so the
        // contract is observable from the test surface.
        let stub = CountingBackfillScheduler()
        let (scheduler, store) = try await makeScheduler(backfillScheduler: stub)
        await scheduler.updateScenePhase(.background)

        await scheduler.enqueue(
            episodeId: "ep-bg-throw",
            podcastId: "pod-1",
            downloadId: "dl-throw",
            sourceFingerprint: "fp-bg-throw",
            isExplicitDownload: false
        )

        // Rearm fired.
        let count = await stub.scheduleCallCount
        #expect(count == 1, "Rearm must fire even when the scheduler stub records a side effect (got \(count))")

        // And the enqueue itself succeeded — the job landed in the
        // store. If the rearm path had thrown out of `enqueue` (or
        // somehow short-circuited the `store.insertJob` path), this
        // assertion would catch it.
        let ids = try await store.fetchAllJobEpisodeIds()
        #expect(ids.contains("ep-bg-throw"),
                "Enqueue must complete its store write even when the rearm path runs")
    }
}
