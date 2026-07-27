// AdBannerQueueTests.swift
// Tests for AdBannerQueue coalescing, auto-dismiss, correction wiring,
// unbounded queue growth, and explicit feedback.

import XCTest
import SwiftUI
@testable import Playhead

@MainActor
final class AdBannerQueueTests: XCTestCase {

    // MARK: - Helpers

    private func makeItem(
        id: String = UUID().uuidString,
        windowId: String = "w-1",
        advertiser: String? = "TestBrand",
        product: String? = nil,
        adStartTime: Double = 100.0,
        adEndTime: Double = 130.0,
        metadataConfidence: Double? = 0.85,
        metadataSource: String = "foundationModels",
        episodeId: String? = nil,
        playbackLifecycleGeneration: UInt64? = nil,
        analysisAssetId: String? = nil,
        windowMaterialRevisionToken: String? = nil
    ) -> AdSkipBannerItem {
        AdSkipBannerItem(
            id: id,
            windowId: windowId,
            advertiser: advertiser,
            product: product,
            adStartTime: adStartTime,
            adEndTime: adEndTime,
            metadataConfidence: metadataConfidence,
            metadataSource: metadataSource,
            podcastId: "podcast-test",
            episodeId: episodeId,
            playbackLifecycleGeneration:
                playbackLifecycleGeneration,
            analysisAssetId: analysisAssetId,
            windowMaterialRevisionToken:
                windowMaterialRevisionToken,
            evidenceCatalogEntries: []
        )
    }

    private actor ControlledAutoDismissSleep {
        private var continuation: CheckedContinuation<Void, Never>?
        private var dwell: TimeInterval?
        private var dwellWaiters: [CheckedContinuation<TimeInterval, Never>] = []

        func sleep(seconds: TimeInterval) async {
            await withCheckedContinuation { continuation in
                let waiters = dwellWaiters
                dwellWaiters.removeAll()
                dwell = seconds
                self.continuation = continuation
                for waiter in waiters {
                    waiter.resume(returning: seconds)
                }
            }
        }

        func release() {
            continuation?.resume()
            continuation = nil
        }

        func waitForObservedDwell() async -> TimeInterval {
            if let dwell {
                return dwell
            }
            return await withCheckedContinuation { continuation in
                dwellWaiters.append(continuation)
            }
        }
    }

    private actor ControlledFeedbackAction {
        private var started = false
        private var startWaiters: [CheckedContinuation<Void, Never>] = []
        private var releaseContinuation: CheckedContinuation<Void, Never>?

        func run() async -> Bool {
            started = true
            let waiters = startWaiters
            startWaiters.removeAll()
            for waiter in waiters {
                waiter.resume()
            }
            await withCheckedContinuation { continuation in
                releaseContinuation = continuation
            }
            return true
        }

        func waitUntilStarted() async {
            if started { return }
            await withCheckedContinuation { continuation in
                startWaiters.append(continuation)
            }
        }

        func release() {
            releaseContinuation?.resume()
            releaseContinuation = nil
        }
    }

    // MARK: - Coalescing (AC3)

    func testCoalesceAdjacentSkipsWithinGap() {
        let queue = AdBannerQueue()

        // First item: 100–130
        let first = makeItem(id: "a", adStartTime: 100, adEndTime: 130)
        queue.enqueue(first)
        XCTAssertEqual(queue.currentBanner?.id, "a")

        // Second item starts within 10s of first's end: 135–160
        let second = makeItem(id: "b", adStartTime: 135, adEndTime: 160)
        queue.enqueue(second)

        // Should coalesce — current banner replaced, no queue buildup.
        XCTAssertEqual(queue.currentBanner?.id, "b",
            "Adjacent skip within coalesceGap should replace the current banner")
    }

    func testDoNotCoalesceDistantSkips() {
        let queue = AdBannerQueue()

        // First item: 100–130
        let first = makeItem(id: "a", adStartTime: 100, adEndTime: 130)
        queue.enqueue(first)

        // Second item starts well beyond 10s gap: 200–230
        let second = makeItem(id: "b", adStartTime: 200, adEndTime: 230)
        queue.enqueue(second)

        // First stays visible, second is queued (not coalesced).
        XCTAssertEqual(queue.currentBanner?.id, "a",
            "Distant skip should not replace the current banner")
    }

    func testCoalesceGapIsExactly10Seconds() {
        let queue = AdBannerQueue()

        // First item: 100–130
        let first = makeItem(id: "a", adStartTime: 100, adEndTime: 130)
        queue.enqueue(first)

        // Exactly at 10s boundary: |130 - 140| = 10 <= 10, should coalesce
        let atBoundary = makeItem(id: "b", adStartTime: 140, adEndTime: 170)
        queue.enqueue(atBoundary)
        XCTAssertEqual(queue.currentBanner?.id, "b",
            "Skip at exactly coalesceGap boundary should coalesce")
    }

    func testCoalesceGapExceededByOneSecond() {
        let queue = AdBannerQueue()

        let first = makeItem(id: "a", adStartTime: 100, adEndTime: 130)
        queue.enqueue(first)

        // Just past boundary: |130 - 141| = 11 > 10, should NOT coalesce
        let pastBoundary = makeItem(id: "b", adStartTime: 141, adEndTime: 170)
        queue.enqueue(pastBoundary)
        XCTAssertEqual(queue.currentBanner?.id, "a",
            "Skip past coalesceGap should not coalesce")
    }

    func testDistinctSuggestWindowsDoNotCoalesce() async throws {
        let queue = AdBannerQueue()
        let first = makeSuggestItem(id: "suggest-a", windowId: "window-a")
        let second = makeSuggestItem(id: "suggest-b", windowId: "window-b")
        var exits: [(windowId: String, isExplicitDenial: Bool)] = []
        queue.onSuggestExitWithoutSkip = { item, isExplicitDenial in
            exits.append((item.windowId, isExplicitDenial))
        }

        queue.enqueue(first)
        queue.enqueue(second)

        XCTAssertEqual(queue.currentBanner?.windowId, first.windowId,
            "Each precise suggest window needs its own response presentation")
        queue.dismiss()
        let advanceTask = try XCTUnwrap(queue.advanceTaskForTesting())
        await advanceTask.value

        XCTAssertEqual(exits.map(\.windowId), [first.windowId])
        XCTAssertEqual(exits.map(\.isExplicitDenial), [false],
            "Advancing the first presentation must retire only that suggestion neutrally")
        XCTAssertEqual(queue.currentBanner?.windowId, second.windowId,
            "The second precise suggestion must receive its own presentation")
    }

    func testDistinctAutoSkippedWindowsDoNotCoalesce() async throws {
        let queue = AdBannerQueue()
        let first = makeItem(
            id: "auto-a",
            windowId: "window-a",
            adStartTime: 100,
            adEndTime: 130
        )
        let second = makeItem(
            id: "auto-b",
            windowId: "window-b",
            adStartTime: 135,
            adEndTime: 160
        )

        queue.enqueue(first)
        queue.enqueue(second)

        XCTAssertEqual(
            queue.currentBanner?.windowId,
            first.windowId,
            "A response must stay bound to the first visible auto-skip window"
        )
        queue.dismiss()
        let advanceTask = try XCTUnwrap(queue.advanceTaskForTesting())
        await advanceTask.value
        XCTAssertEqual(
            queue.currentBanner?.windowId,
            second.windowId,
            "The adjacent auto-skip window must receive its own presentation"
        )
    }

    func testExactStreamReplayCoalescesInsteadOfQueuingDuplicatePresentation() {
        let queue = AdBannerQueue()
        let first = makeItem(
            id: "replay-first",
            windowId: "replayed-window",
            adStartTime: 100,
            adEndTime: 130
        )
        let replay = makeItem(
            id: "replay-second-transport-id",
            windowId: first.windowId,
            adStartTime: first.adStartTime,
            adEndTime: first.adEndTime
        )

        queue.enqueue(first)
        queue.enqueue(replay)

        XCTAssertEqual(
            queue.currentBanner?.id,
            first.id,
            "A transport replay must not replace the stable visible presentation identity"
        )
        queue.dismiss()
        XCTAssertNil(
            queue.advanceTaskForTesting(),
            "One logical revision replayed by the stream must remain one presentation"
        )
    }

    func testExactStreamReplayCannotQueueBehindClaimedPresentation() async throws {
        let queue = AdBannerQueue()
        let first = makeItem(
            id: "claimed-replay-first",
            windowId: "claimed-replayed-window",
            adStartTime: 100,
            adEndTime: 130
        )
        let replay = makeItem(
            id: "claimed-replay-second-transport-id",
            windowId: first.windowId,
            adStartTime: first.adStartTime,
            adEndTime: first.adEndTime
        )
        let pending = makeItem(
            id: "unrelated-pending-presentation",
            windowId: "unrelated-pending-window",
            adStartTime: 200,
            adEndTime: 230
        )

        queue.enqueue(first)
        XCTAssertTrue(queue.claimFeedback(for: first))
        queue.enqueue(pending)
        queue.enqueue(replay)
        XCTAssertEqual(queue.currentBanner?.id, first.id)
        XCTAssertTrue(queue.finalizeFeedback(.confirmed, for: first))
        XCTAssertTrue(queue.dismissAfterAcceptedFeedback(for: first))
        let firstAdvance = try XCTUnwrap(queue.advanceTaskForTesting())
        await firstAdvance.value
        XCTAssertEqual(queue.currentBanner?.id, pending.id)

        queue.dismiss()
        XCTAssertNil(
            queue.advanceTaskForTesting(),
            "A replay arriving during persistence must not hide behind an unrelated pending card"
        )
    }

    // MARK: - Auto-Dismiss (AC4)

    func testAutoDismissAfterConfiguredDwell() async throws {
        let sleep = ControlledAutoDismissSleep()
        let queue = AdBannerQueue(
            autoDismissSeconds: 0.01,
            autoDismissSleep: { seconds in
                await sleep.sleep(seconds: seconds)
            }
        )
        let item = makeItem(id: "auto")
        queue.enqueue(item)

        XCTAssertNotNil(queue.currentBanner, "Banner should be showing immediately")

        let dismissTask = try XCTUnwrap(queue.autoDismissTaskForTesting())
        let observedDwell = await sleep.waitForObservedDwell()
        XCTAssertEqual(observedDwell, 0.01)

        await sleep.release()
        await dismissTask.value
        XCTAssertNil(queue.currentBanner,
            "Banner should auto-dismiss after its configured dwell")
    }

    func testAutoSkippedDwellIsEightSeconds() {
        XCTAssertEqual(AdBannerQueue.dwellSeconds(for: .autoSkipped), 8.0)
    }

    func testManualDismissCancelsAutoDismiss() {
        let queue = AdBannerQueue()
        let item = makeItem(id: "manual")
        queue.enqueue(item)

        queue.dismiss()
        XCTAssertNil(queue.currentBanner,
            "Manual dismiss should clear the banner immediately")
    }

    func testAssistiveControlPauseStopsAndRestartsAutoDismiss() {
        let queue = AdBannerQueue()
        queue.enqueue(makeItem(id: "assistive-control"))
        XCTAssertNotNil(queue.autoDismissTaskForTesting())

        queue.setAutoDismissPaused(true)
        XCTAssertNil(queue.autoDismissTaskForTesting(),
            "VoiceOver or Switch Control must remove the transient time limit")
        XCTAssertEqual(queue.currentBanner?.id, "assistive-control")

        queue.setAutoDismissPaused(false)
        XCTAssertNotNil(queue.autoDismissTaskForTesting(),
            "Leaving assistive control should start a fresh dwell")
        queue.dismiss()
    }

    func testAssistiveControlPauseAlsoProtectsSponsorConfirmation() {
        let queue = AdBannerQueue()
        let item = makeItem(id: "assistive-confirmation")
        queue.enqueue(item)
        queue.setAutoDismissPaused(true)

        XCTAssertFalse(queue.dismissConfirmationIfAllowed(for: item),
            "The sponsor receipt must remain visible while assistive controls are active")
        XCTAssertEqual(queue.currentBanner?.id, item.id)

        queue.setAutoDismissPaused(false)
        XCTAssertTrue(queue.dismissConfirmationIfAllowed(for: item),
            "The delayed confirmation may dismiss once transient timers resume")
        XCTAssertNil(queue.currentBanner)
    }

    func testExposureDefersShownAndTimerUntilSurfaceIsVisible() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(id: "exposure")
        let view = AdBannerView(queue: queue)
        queue.enqueue(item)

        view.handlePresentationExposureChange(
            isExposed: false,
            isAssistiveControlActive: false
        )
        XCTAssertNil(queue.autoDismissTaskForTesting())
        XCTAssertEqual(store.snapshot.bannersShown, 0)

        view.handlePresentationExposureChange(
            isExposed: true,
            isAssistiveControlActive: false
        )
        XCTAssertNotNil(queue.autoDismissTaskForTesting())
        XCTAssertEqual(store.snapshot.bannersShown, 1)

        view.handlePresentationExposureChange(
            isExposed: true,
            isAssistiveControlActive: true
        )
        XCTAssertNil(queue.autoDismissTaskForTesting())
        XCTAssertEqual(store.snapshot.bannersShown, 1,
            "Assistive timing protection must not duplicate a visible impression")
    }

    func testHostDisappearNeutrallyRetiresCurrentAndQueuedSuggestions() {
        let queue = AdBannerQueue()
        let first = makeSuggestItem(id: "depart-current", windowId: "window-current")
        let second = makeSuggestItem(id: "depart-queued", windowId: "window-queued")
        let autoSkipped = makeItem(
            id: "depart-auto",
            adStartTime: 700,
            adEndTime: 730
        )
        var exits: [(windowId: String, isExplicitDenial: Bool)] = []
        queue.onSuggestExitWithoutSkip = { item, isExplicitDenial in
            exits.append((item.windowId, isExplicitDenial))
        }

        queue.enqueue(first)
        queue.enqueue(second)
        queue.enqueue(autoSkipped)
        queue.discardAllOnHostDisappear()

        XCTAssertNil(queue.currentBanner)
        XCTAssertNil(queue.autoDismissTaskForTesting())
        XCTAssertNil(queue.advanceTaskForTesting())
        XCTAssertEqual(exits.map(\.windowId), [first.windowId, second.windowId])
        XCTAssertEqual(exits.map(\.isExplicitDenial), [false, false],
            "Leaving Now Playing is a neutral retirement, never explicit negative feedback")

        let fresh = makeItem(
            id: "after-return",
            adStartTime: 1_000,
            adEndTime: 1_030,
            episodeId: "episode-a"
        )
        queue.enqueue(fresh)
        XCTAssertNil(queue.currentBanner,
            "A late enqueue must not repopulate an inactive host")

        let hostGeneration = queue.activateHost(for: "episode-a")
        queue.enqueue(fresh, hostGeneration: hostGeneration)
        XCTAssertEqual(queue.currentBanner?.id, fresh.id,
            "No stale queued banner may survive its owning surface")
        queue.dismiss()
    }

    func testEpisodeChangeNeutrallyRetiresStaleCurrentAndQueuedSuggestions() {
        let queue = AdBannerQueue()
        let current = makeSuggestItem(
            id: "episode-a-current",
            windowId: "episode-a-window-current"
        )
        let pending = makeSuggestItem(
            id: "episode-a-pending",
            windowId: "episode-a-window-pending"
        )
        var exits: [(windowId: String, isExplicitDenial: Bool)] = []
        queue.onSuggestExitWithoutSkip = { item, isExplicitDenial in
            exits.append((item.windowId, isExplicitDenial))
        }

        queue.enqueue(current)
        queue.enqueue(pending)
        let episodeBGeneration = queue.discardAllOnEpisodeChange(
            from: "episode-a",
            to: "episode-b"
        )

        XCTAssertNil(queue.currentBanner)
        XCTAssertNil(queue.autoDismissTaskForTesting())
        XCTAssertNil(queue.advanceTaskForTesting())
        XCTAssertEqual(
            exits.map(\.windowId),
            [current.windowId, pending.windowId]
        )
        XCTAssertEqual(exits.map(\.isExplicitDenial), [false, false],
            "Advancing episodes retires old suggestions without negative feedback")

        let episodeB = makeItem(
            id: "episode-b-current",
            adStartTime: 1_000,
            adEndTime: 1_030,
            episodeId: "episode-b"
        )
        queue.enqueue(episodeB, hostGeneration: episodeBGeneration)
        queue.discardAllOnEpisodeChange(
            from: "episode-b",
            to: "episode-b"
        )
        XCTAssertEqual(queue.currentBanner?.id, episodeB.id,
            "A repeated observation of the same episode must not clear live banners")
        queue.dismiss()
    }

    func testEpisodeChangeRejectsLateOldEpisodeAndOldObservationGeneration() {
        let queue = AdBannerQueue()
        let episodeAGeneration = queue.activateHost(for: "episode-a")
        let episodeA = makeItem(
            id: "episode-a",
            episodeId: "episode-a"
        )
        queue.enqueue(episodeA, hostGeneration: episodeAGeneration)
        XCTAssertEqual(queue.currentBanner?.id, episodeA.id)

        let episodeBGeneration = queue.discardAllOnEpisodeChange(
            from: "episode-a",
            to: "episode-b"
        )
        let lateEpisodeA = makeItem(
            id: "late-episode-a",
            adStartTime: 200,
            adEndTime: 230,
            episodeId: "episode-a"
        )
        queue.enqueue(lateEpisodeA, hostGeneration: episodeBGeneration)
        XCTAssertNil(queue.currentBanner,
            "A genuinely late old-orchestrator emission must fail episode gating")

        let episodeB = makeItem(
            id: "episode-b",
            adStartTime: 300,
            adEndTime: 330,
            episodeId: "episode-b"
        )
        queue.enqueue(episodeB, hostGeneration: episodeAGeneration)
        XCTAssertNil(queue.currentBanner,
            "A buffered event from the cancelled observer must fail generation gating")

        queue.enqueue(episodeB, hostGeneration: episodeBGeneration)
        XCTAssertEqual(queue.currentBanner?.id, episodeB.id)
        queue.dismiss()
    }

    func testSameEpisodeReplacementRetiresAndRejectsOldLifecyclePresentation() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let oldHostGeneration = queue.activateHost(
            for: "episode-a",
            playbackLifecycleGeneration: 41
        )
        let oldItem = makeItem(
            id: "same-episode-old-lifecycle",
            episodeId: "episode-a",
            playbackLifecycleGeneration: 41
        )
        XCTAssertTrue(
            queue.enqueue(
                oldItem,
                hostGeneration: oldHostGeneration
            )
        )

        var currentPlaybackGeneration: UInt64 = 41
        var didListen = false
        var didRevert = false
        let view = AdBannerView(
            queue: queue,
            isItemCurrent: {
                $0.episodeId == "episode-a"
                    && $0.playbackLifecycleGeneration
                        == currentPlaybackGeneration
            },
            onListen: { _ in didListen = true },
            onNotAnAd: { _ in didRevert = true }
        )
        view.handleBannerAppear(for: oldItem)
        XCTAssertEqual(store.snapshot.bannersShown, 1)

        currentPlaybackGeneration = 42
        XCTAssertFalse(view.handleListen(for: oldItem))
        XCTAssertFalse(view.handleFeedback(.denied, for: oldItem))
        XCTAssertFalse(didListen)
        XCTAssertFalse(didRevert)
        XCTAssertEqual(store.snapshot.bannersDenied, 0)

        let newHostGeneration =
            queue.discardAllOnPlaybackContextChange(
                fromEpisodeId: "episode-a",
                toEpisodeId: "episode-a",
                fromPlaybackLifecycleGeneration: 41,
                toPlaybackLifecycleGeneration: 42
            )
        XCTAssertNil(queue.currentBanner)
        XCTAssertFalse(
            queue.enqueue(
                oldItem,
                hostGeneration: newHostGeneration
            ),
            "A late same-episode emission from the replaced lifecycle must be rejected"
        )

        let newItem = makeItem(
            id: "same-episode-new-lifecycle",
            episodeId: "episode-a",
            playbackLifecycleGeneration: 42
        )
        XCTAssertTrue(
            queue.enqueue(
                newItem,
                hostGeneration: newHostGeneration
            )
        )
        XCTAssertEqual(queue.currentBanner?.id, newItem.id)
        queue.dismiss()
    }

    func testActionTimeEpisodeGuardRejectsStaleVisiblePresentation() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let episodeA = makeItem(
            id: "visible-episode-a",
            episodeId: "episode-a"
        )
        queue.enqueue(episodeA)

        var currentEpisodeId = "episode-a"
        var didListen = false
        var didRevert = false
        var didPersistSponsor = false
        let view = AdBannerView(
            queue: queue,
            isItemCurrent: { $0.episodeId == currentEpisodeId },
            onListen: { _ in didListen = true },
            onNotAnAd: { _ in didRevert = true },
            onAlwaysSkipSponsor: { _ in didPersistSponsor = true }
        )

        // Runtime identity can change one render pass before SwiftUI delivers
        // the queue's onChange cleanup. Actions in that interval must not
        // mutate the new episode or record feedback for the old one.
        currentEpisodeId = "episode-b"
        view.handleBannerAppear(for: episodeA)
        XCTAssertFalse(view.handleListen(for: episodeA))
        XCTAssertFalse(view.handleFeedback(.denied, for: episodeA))
        XCTAssertFalse(view.handleAlwaysSkipSponsor(for: episodeA))

        XCTAssertFalse(didListen)
        XCTAssertFalse(didRevert)
        XCTAssertFalse(didPersistSponsor)
        XCTAssertEqual(store.snapshot, .zero)
        XCTAssertEqual(queue.currentBanner?.id, episodeA.id,
            "The lifecycle onChange owns neutral retirement; stale actions only no-op")
        queue.dismiss()
    }

    // MARK: - Auto-Skip Correction Wiring

    func testNotAnAdHandlerIsOptional() {
        let queue = AdBannerQueue()
        let view = AdBannerView(queue: queue, onListen: nil, onNotAnAd: nil)

        XCTAssertTrue(view.onNotAnAd == nil,
            "The auto-skip feedback control must tolerate an absent correction handler")
    }

    func testNotAnAdHandlerCanBeWired() {
        let queue = AdBannerQueue()
        var called = false
        let view = AdBannerView(
            queue: queue,
            onListen: nil,
            onNotAnAd: { _ in called = true }
        )

        XCTAssertNotNil(view.onNotAnAd,
            "The auto-skip denial path should retain its correction callback")
        let testItem = makeItem(id: "test-correction")
        view.onNotAnAd?(testItem)
        XCTAssertTrue(called, "onNotAnAd handler should fire when invoked")
    }

    // MARK: - Generic Copy Fallback (AC6)

    func testGenericCopyWhenNoMetadata() {
        let queue = AdBannerQueue()
        let item = makeItem(
            id: "no-meta",
            advertiser: nil,
            product: nil,
            metadataConfidence: nil,
            metadataSource: "none"
        )
        queue.enqueue(item)

        // The banner exists — the copy logic is private, so we verify
        // the item is enqueued and the banner is visible.
        XCTAssertNotNil(queue.currentBanner)
    }

    func testGenericCopyWhenLowConfidence() {
        let queue = AdBannerQueue()
        let item = makeItem(
            id: "low-conf",
            advertiser: "ShouldNotShow",
            metadataConfidence: 0.3,
            metadataSource: "foundationModels"
        )
        queue.enqueue(item)

        // Banner visible — the view's copy resolver would use generic text
        // since confidence (0.3) < threshold (0.60).
        XCTAssertNotNil(queue.currentBanner)
    }

    // MARK: - Unbounded Queue (AC10)

    func testQueueGrowsUnbounded() {
        let queue = AdBannerQueue()

        // Enqueue 100 non-coalescable items (each far apart in time).
        for i in 0..<100 {
            let start = Double(i) * 1000.0
            let item = makeItem(
                id: "item-\(i)",
                adStartTime: start,
                adEndTime: start + 30.0
            )
            queue.enqueue(item)
        }

        // First item is showing.
        XCTAssertEqual(queue.currentBanner?.id, "item-0")

        // Dismiss the first — second should appear (after a brief delay in production,
        // but in tests the queue state should reflect the pending items).
        queue.dismiss()

        // After dismiss, the queue processes the next item asynchronously.
        // We can at least verify the current banner was cleared.
        // The async Task inside dismiss() will schedule showNext().
    }

    // MARK: - Haptic on Appear (AC8, supplement to AdBannerViewHapticTests)

    func testHapticFiresOnBannerAppearViaInjectedPlayer() {
        let recorder = RecordingHapticPlayer()
        let queue = AdBannerQueue()
        let view = AdBannerView(
            queue: queue,
            onListen: nil,
            hapticPlayer: recorder
        )

        view.handleBannerAppear()

        XCTAssertEqual(recorder.played, [.notice],
            "Banner appear should emit .notice haptic")
    }

    func testRenderedAppearanceRecordsShownAndHapticOnlyOnce() {
        let recorder = RecordingHapticPlayer()
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(id: "rendered-appearance")
        let view = AdBannerView(queue: queue, hapticPlayer: recorder)
        queue.enqueue(item)

        view.handleBannerAppear(for: item)
        view.handleBannerAppear(for: item)

        XCTAssertEqual(store.snapshot.bannersShown, 1)
        XCTAssertEqual(recorder.played, [.notice])
    }

    func testImmediateRetirementReplacementRecordsVisibleReplacement() {
        let recorder = RecordingHapticPlayer()
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let generation = queue.activateHost(
            for: "episode-a",
            playbackLifecycleGeneration: 7
        )
        var suggestion = makeSuggestItem(
            id: "retired-suggestion",
            windowId: "suggestion-window",
            episodeId: "episode-a"
        )
        suggestion.playbackLifecycleGeneration = 7
        let replacement = makeItem(
            id: "replacement-auto-skip",
            windowId: "replacement-window",
            adStartTime: 500,
            adEndTime: 530,
            episodeId: "episode-a",
            playbackLifecycleGeneration: 7
        )
        let view = AdBannerView(queue: queue, hapticPlayer: recorder)

        XCTAssertTrue(
            queue.enqueue(suggestion, hostGeneration: generation)
        )
        view.handleCurrentBannerIdentityChange(isExposed: true)

        XCTAssertTrue(
            queue.retireWindow(
                AdBannerRetirement(
                    windowId: suggestion.windowId,
                    episodeId: "episode-a",
                    playbackLifecycleGeneration: 7
                ),
                hostGeneration: generation
            )
        )
        XCTAssertTrue(
            queue.enqueue(replacement, hostGeneration: generation)
        )

        // SwiftUI may observe only the final identity when retirement and
        // replacement happen in one update. Drive the production identity
        // observer directly: the visible replacement still gets one
        // impression and haptic even if its card never receives a fresh
        // onAppear callback.
        view.handleCurrentBannerIdentityChange(isExposed: true)
        view.handleCurrentBannerIdentityChange(isExposed: true)

        XCTAssertEqual(queue.currentBanner?.id, replacement.id)
        XCTAssertEqual(store.snapshot.bannersShown, 2)
        XCTAssertEqual(recorder.played, [.notice, .notice])
    }

    // MARK: - Queue Ordering

    func testFirstEnqueuedItemShowsFirst() {
        let queue = AdBannerQueue()
        let a = makeItem(id: "first", adStartTime: 0, adEndTime: 30)
        let b = makeItem(id: "second", adStartTime: 500, adEndTime: 530)

        queue.enqueue(a)
        queue.enqueue(b)

        XCTAssertEqual(queue.currentBanner?.id, "first",
            "First enqueued item should display first")
    }

    // MARK: - Suggest exit reason threading (playhead-lc7z)

    private func makeSuggestItem(
        id: String,
        windowId: String = "w-1",
        episodeId: String? = nil,
        playbackLifecycleGeneration: UInt64? = nil
    ) -> AdSkipBannerItem {
        var item = makeItem(
            id: id,
            windowId: windowId,
            episodeId: episodeId,
            playbackLifecycleGeneration:
                playbackLifecycleGeneration
        )
        item.tier = .suggest
        item.suggestionRevisionToken = ["revision", id].joined(separator: "-")
        return item
    }

    func testSuggestExplicitDeclineReportsExplicitDenial() {
        let queue = AdBannerQueue()
        var captured: (id: String, isExplicitDenial: Bool)?
        queue.onSuggestExitWithoutSkip = { item, isExplicitDenial in
            captured = (item.id, isExplicitDenial)
        }
        queue.enqueue(makeSuggestItem(id: "s-dismiss"))

        // An explicit No response forwards isExplicitDenial: true.
        queue.dismiss(isExplicitDenial: true)

        XCTAssertEqual(captured?.id, "s-dismiss")
        XCTAssertEqual(captured?.isExplicitDenial, true,
            "An explicit No response on a suggest banner must report isExplicitDenial=true")
    }

    func testSuggestAutoFadeReportsNoExplicitDenial() async throws {
        let sleep = ControlledAutoDismissSleep()
        let queue = AdBannerQueue(
            suggestAutoDismissSeconds: 0.01,
            autoDismissSleep: { seconds in await sleep.sleep(seconds: seconds) }
        )
        var captured: (id: String, isExplicitDenial: Bool)?
        queue.onSuggestExitWithoutSkip = { item, isExplicitDenial in
            captured = (item.id, isExplicitDenial)
        }
        queue.enqueue(makeSuggestItem(id: "s-fade"))

        let dismissTask = try XCTUnwrap(queue.autoDismissTaskForTesting())
        _ = await sleep.waitForObservedDwell()
        await sleep.release()
        await dismissTask.value

        XCTAssertEqual(captured?.id, "s-fade")
        XCTAssertEqual(captured?.isExplicitDenial, false,
            "A passive auto-fade of a suggest banner must not report explicit denial")
    }

    func testOrchestratorRetirementRemovesCurrentSuggestionWithoutFeedback() {
        let queue = AdBannerQueue()
        let generation = queue.activateHost(
            for: "episode-a",
            playbackLifecycleGeneration: 7
        )
        let item = makeSuggestItem(
            id: "retire-current",
            windowId: "window-current",
            episodeId: "episode-a"
        )
        var scopedItem = item
        scopedItem = AdSkipBannerItem(
            id: item.id,
            windowId: item.windowId,
            advertiser: item.advertiser,
            product: item.product,
            adStartTime: item.adStartTime,
            adEndTime: item.adEndTime,
            metadataConfidence: item.metadataConfidence,
            metadataSource: item.metadataSource,
            podcastId: item.podcastId,
            episodeId: item.episodeId,
            playbackLifecycleGeneration: 7,
            evidenceCatalogEntries: item.evidenceCatalogEntries,
            tier: .suggest
        )
        var exitCalls = 0
        queue.onSuggestExitWithoutSkip = { _, _ in exitCalls += 1 }
        XCTAssertTrue(
            queue.enqueue(scopedItem, hostGeneration: generation)
        )

        XCTAssertTrue(
            queue.retireWindow(
                AdBannerRetirement(
                    windowId: "window-current",
                    episodeId: "episode-a",
                    playbackLifecycleGeneration: 7
                ),
                hostGeneration: generation
            )
        )
        XCTAssertNil(queue.currentBanner)
        XCTAssertEqual(exitCalls, 0,
            "Orchestrator invalidation is not a user decline")
    }

    func testOrchestratorRetirementRemovesAutoSkippedPresentation() {
        let queue = AdBannerQueue()
        let generation = queue.activateHost(
            for: "episode-a",
            playbackLifecycleGeneration: 7
        )
        let item = makeItem(
            id: "retire-auto",
            windowId: "auto-window",
            episodeId: "episode-a",
            playbackLifecycleGeneration: 7
        )
        XCTAssertTrue(
            queue.enqueue(item, hostGeneration: generation)
        )

        XCTAssertTrue(
            queue.retireWindow(
                AdBannerRetirement(
                    windowId: item.windowId,
                    episodeId: "episode-a",
                    playbackLifecycleGeneration: 7
                ),
                hostGeneration: generation
            )
        )
        XCTAssertNil(
            queue.currentBanner,
            "A precision downgrade must remove the stale auto-tier card"
        )
    }

    func testOrchestratorRetirementRemovesOnlyMatchingQueuedSuggestion() async throws {
        let queue = AdBannerQueue()
        let generation = queue.activateHost(
            for: "episode-a",
            playbackLifecycleGeneration: 7
        )
        let current = makeItem(
            id: "retire-queue-current",
            windowId: "auto",
            adStartTime: 0,
            adEndTime: 30,
            episodeId: "episode-a",
            playbackLifecycleGeneration: 7
        )
        var retired = makeSuggestItem(
            id: "retire-queued",
            windowId: "window-retired",
            episodeId: "episode-a"
        )
        retired = AdSkipBannerItem(
            id: retired.id,
            windowId: retired.windowId,
            advertiser: retired.advertiser,
            product: retired.product,
            adStartTime: 100,
            adEndTime: 130,
            metadataConfidence: retired.metadataConfidence,
            metadataSource: retired.metadataSource,
            podcastId: retired.podcastId,
            episodeId: retired.episodeId,
            playbackLifecycleGeneration: 7,
            evidenceCatalogEntries: retired.evidenceCatalogEntries,
            tier: .suggest
        )
        var surviving = retired
        surviving = AdSkipBannerItem(
            id: "surviving",
            windowId: "window-surviving",
            advertiser: retired.advertiser,
            product: retired.product,
            adStartTime: 500,
            adEndTime: 530,
            metadataConfidence: retired.metadataConfidence,
            metadataSource: retired.metadataSource,
            podcastId: retired.podcastId,
            episodeId: retired.episodeId,
            playbackLifecycleGeneration: 7,
            evidenceCatalogEntries: retired.evidenceCatalogEntries,
            tier: .suggest
        )
        queue.enqueue(current, hostGeneration: generation)
        queue.enqueue(retired, hostGeneration: generation)
        queue.enqueue(surviving, hostGeneration: generation)

        XCTAssertTrue(
            queue.retireWindow(
                AdBannerRetirement(
                    windowId: retired.windowId,
                    episodeId: "episode-a",
                    playbackLifecycleGeneration: 7
                ),
                hostGeneration: generation
            )
        )
        queue.dismiss()
        let advance = try XCTUnwrap(queue.advanceTaskForTesting())
        await advance.value
        XCTAssertEqual(queue.currentBanner?.id, surviving.id)
    }

    func testStaleRetirementCannotRemoveReplacementLifecycleSuggestion() {
        let queue = AdBannerQueue()
        let generation = queue.activateHost(
            for: "episode-a",
            playbackLifecycleGeneration: 8
        )
        var item = makeSuggestItem(
            id: "replacement-suggest",
            windowId: "same-window",
            episodeId: "episode-a"
        )
        item = AdSkipBannerItem(
            id: item.id,
            windowId: item.windowId,
            advertiser: item.advertiser,
            product: item.product,
            adStartTime: item.adStartTime,
            adEndTime: item.adEndTime,
            metadataConfidence: item.metadataConfidence,
            metadataSource: item.metadataSource,
            podcastId: item.podcastId,
            episodeId: item.episodeId,
            playbackLifecycleGeneration: 8,
            evidenceCatalogEntries: item.evidenceCatalogEntries,
            tier: .suggest
        )
        queue.enqueue(item, hostGeneration: generation)

        XCTAssertFalse(
            queue.retireWindow(
                AdBannerRetirement(
                    windowId: "same-window",
                    episodeId: "episode-a",
                    playbackLifecycleGeneration: 7
                ),
                hostGeneration: generation
            )
        )
        XCTAssertEqual(queue.currentBanner?.id, item.id)
    }

    // MARK: - Explicit feedback (playhead-jw63.1)

    private func makeFeedbackStore() -> BannerFeedbackCounterStore {
        let suiteName = "AdBannerQueueTests.feedback.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)
        addTeardownBlock {
            defaults.removePersistentDomain(forName: suiteName)
        }
        return BannerFeedbackCounterStore(defaults: defaults)
    }

    func testShownCountsPresentationOnceAcrossCurrentCoalescingAndQueueAdvance() async throws {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)

        let first = makeItem(id: "shown-a", adStartTime: 100, adEndTime: 130)
        let coalesced = makeItem(id: "shown-a2", adStartTime: 135, adEndTime: 160)
        let queued = makeItem(id: "shown-b", adStartTime: 500, adEndTime: 530)
        let view = AdBannerView(queue: queue)

        queue.enqueue(first)
        view.handleBannerAppear(for: first)
        queue.enqueue(coalesced)
        XCTAssertEqual(store.snapshot.bannersShown, 1,
            "Replacing a currently visible banner by coalescing must not count a second presentation")

        queue.enqueue(queued)
        XCTAssertEqual(store.snapshot.bannersShown, 1,
            "Queued work is not shown until it becomes current")

        queue.dismiss()
        let advanceTask = try XCTUnwrap(queue.advanceTaskForTesting())
        await advanceTask.value

        XCTAssertEqual(queue.currentBanner?.id, queued.id)
        view.handleBannerAppear(for: queued)
        XCTAssertEqual(store.snapshot.bannersShown, 2,
            "A queued banner must count once when it actually becomes current")
    }

    func testEnqueueDuringDeferredAdvanceDoesNotOverwritePresentedBanner() async throws {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let first = makeItem(id: "advance-a", adStartTime: 100, adEndTime: 130)
        let second = makeItem(id: "advance-b", adStartTime: 500, adEndTime: 530)
        let duringGap = makeItem(id: "advance-c", adStartTime: 900, adEndTime: 930)
        let view = AdBannerView(queue: queue)

        queue.enqueue(first)
        view.handleBannerAppear(for: first)
        queue.enqueue(second)
        queue.dismiss()
        let firstAdvance = try XCTUnwrap(queue.advanceTaskForTesting())

        // Arrival during the 350 ms slide-out gap must remain queued. If it
        // displays immediately, the retained advance task can overwrite it.
        queue.enqueue(duringGap)
        XCTAssertNil(queue.currentBanner)
        XCTAssertEqual(store.snapshot.bannersShown, 1)

        await firstAdvance.value
        XCTAssertEqual(queue.currentBanner?.id, second.id)
        view.handleBannerAppear(for: second)
        XCTAssertEqual(store.snapshot.bannersShown, 2)

        queue.dismiss()
        let secondAdvance = try XCTUnwrap(queue.advanceTaskForTesting())
        await secondAdvance.value
        XCTAssertEqual(queue.currentBanner?.id, duringGap.id)
        view.handleBannerAppear(for: duringGap)
        XCTAssertEqual(store.snapshot.bannersShown, 3)
    }

    func testOffscreenQueueAdvanceDoesNotRecordShown() async throws {
        let sleep = ControlledAutoDismissSleep()
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(
            autoDismissSeconds: 0.01,
            autoDismissSleep: { seconds in await sleep.sleep(seconds: seconds) },
            feedbackCounterStore: store
        )
        queue.enqueue(makeItem(id: "offscreen"))

        let dismissTask = try XCTUnwrap(queue.autoDismissTaskForTesting())
        _ = await sleep.waitForObservedDwell()
        await sleep.release()
        await dismissTask.value

        XCTAssertNil(queue.currentBanner)
        XCTAssertEqual(store.snapshot.bannersShown, 0,
            "Queue-current banners that never render must not count as shown")
    }

    func testAutoSkippedYesRecordsConfirmedOnlyAfterDurableAction() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(id: "auto-yes")
        var correctionCalls = 0
        var confirmationReceipts = 0
        let view = AdBannerView(
            queue: queue,
            onAutoSkipConfirmed: { received in
                XCTAssertEqual(received.id, item.id)
                confirmationReceipts += 1
            },
            onNotAnAd: { _ in correctionCalls += 1 }
        )
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertTrue(view.handleFeedback(.confirmed, for: item))

        XCTAssertEqual(store.snapshot.bannersConfirmed, 1)
        XCTAssertEqual(store.snapshot.bannersDenied, 0)
        XCTAssertEqual(confirmationReceipts, 1)
        XCTAssertEqual(correctionCalls, 0,
            "Auto-skip Yes uses its dedicated durable receipt route")
        XCTAssertNil(queue.currentBanner)
    }

    func testAutoSkippedYesRejectedReceiptStaysRetryableWithoutCounter() async {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(id: "auto-yes-retry")
        var acceptsReceipt = false
        let view = AdBannerView(
            queue: queue,
            onAutoSkipConfirmedAsync: { _ in acceptsReceipt }
        )
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        let rejected = await view.handleFeedbackAwaitingAction(
            .confirmed,
            for: item
        )
        XCTAssertFalse(rejected)
        XCTAssertEqual(queue.currentBanner?.id, item.id)
        XCTAssertEqual(store.snapshot.bannersConfirmed, 0)
        XCTAssertTrue(
            view.isFeedbackResponseAvailable(.confirmed, for: item)
        )

        acceptsReceipt = true
        let accepted = await view.handleFeedbackAwaitingAction(
            .confirmed,
            for: item
        )
        XCTAssertTrue(accepted)
        XCTAssertNil(queue.currentBanner)
        XCTAssertEqual(store.snapshot.bannersConfirmed, 1)
    }

    func testListenConsumesPresentationWithoutExplicitFeedback() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(id: "listen-consumes")
        var listenCalls = 0
        let view = AdBannerView(
            queue: queue,
            onListen: { received in
                XCTAssertEqual(received.id, item.id)
                listenCalls += 1
            }
        )
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertTrue(view.handleListen(for: item))
        XCTAssertFalse(view.handleListen(for: item),
            "A rapid repeated Listen tap must not replay the revert action")
        XCTAssertFalse(view.handleFeedback(.confirmed, for: item),
            "A reverted presentation must not accept a contradictory Yes")

        XCTAssertEqual(listenCalls, 1)
        XCTAssertEqual(
            store.snapshot,
            BannerFeedbackCounts(
                bannersShown: 1,
                bannersConfirmed: 0,
                bannersDenied: 0
            ),
            "Listen is an existing correction action, not an explicit Yes/No response"
        )
        XCTAssertNil(queue.currentBanner)
    }

    func testAlwaysSkipSponsorClaimsPresentationBeforeDurableAction() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(id: "always-skip-claims")
        var alwaysSkipCalls = 0
        var notAnAdCalls = 0
        var listenCalls = 0
        let view = AdBannerView(
            queue: queue,
            onListen: { _ in listenCalls += 1 },
            onNotAnAd: { _ in notAnAdCalls += 1 },
            onAlwaysSkipSponsor: { received in
                XCTAssertEqual(received.id, item.id)
                alwaysSkipCalls += 1
            }
        )
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertTrue(view.handleAlwaysSkipSponsor(for: item))
        XCTAssertFalse(view.handleAlwaysSkipSponsor(for: item),
            "A rapid repeated sponsor-persistence tap must be ignored")
        XCTAssertFalse(view.handleFeedback(.denied, for: item),
            "Always Skip followed by No must not persist a contradictory correction")
        XCTAssertFalse(view.handleFeedback(.confirmed, for: item),
            "Always Skip followed by Yes must not record a second response")
        XCTAssertFalse(view.handleListen(for: item),
            "Always Skip followed by Listen must not revert the same presentation")

        XCTAssertEqual(alwaysSkipCalls, 1)
        XCTAssertEqual(notAnAdCalls, 0)
        XCTAssertEqual(listenCalls, 0)
        XCTAssertEqual(
            store.snapshot,
            BannerFeedbackCounts(
                bannersShown: 1,
                bannersConfirmed: 0,
                bannersDenied: 0
            ),
            "Always Skip is an existing sponsor action, not an explicit Yes/No aggregate"
        )
        XCTAssertEqual(queue.currentBanner?.id, item.id,
            "The presentation stays visible long enough to show the receipt")
        queue.dismiss()
    }

    func testAlwaysSkipSponsorCancelsOriginalAutoDismissTimer() async throws {
        let sleep = ControlledAutoDismissSleep()
        let queue = AdBannerQueue(
            autoDismissSleep: { seconds in
                await sleep.sleep(seconds: seconds)
            }
        )
        let item = makeItem(id: "receipt-full-dwell")
        let view = AdBannerView(
            queue: queue,
            onAlwaysSkipSponsor: { _ in }
        )
        queue.enqueue(item)
        let originalDismissTask = try XCTUnwrap(queue.autoDismissTaskForTesting())
        _ = await sleep.waitForObservedDwell()

        XCTAssertTrue(view.handleAlwaysSkipSponsor(for: item))
        XCTAssertNil(queue.autoDismissTaskForTesting(),
            "Entering receipt state must cancel the original banner fade")

        await sleep.release()
        await originalDismissTask.value
        XCTAssertEqual(queue.currentBanner?.id, item.id,
            "A late sponsor tap must still receive its dedicated confirmation dwell")
        queue.dismiss()
    }

    func testAlwaysSkipSponsorRejectsWhitespaceOnlyAdvertiserWithoutClaiming() {
        let queue = AdBannerQueue()
        let item = makeItem(
            id: "always-skip-whitespace",
            advertiser: " \t\n\r "
        )
        var alwaysSkipCalls = 0
        let view = AdBannerView(
            queue: queue,
            onAutoSkipConfirmed: { _ in },
            onAlwaysSkipSponsor: { _ in alwaysSkipCalls += 1 }
        )
        queue.enqueue(item)

        XCTAssertFalse(view.handleAlwaysSkipSponsor(for: item))
        XCTAssertEqual(alwaysSkipCalls, 0)
        XCTAssertTrue(view.handleFeedback(.confirmed, for: item),
            "An invalid hidden utility action must not consume the feedback slot")
        XCTAssertNil(queue.currentBanner)
    }

    func testAlwaysSkipSponsorPreservesKnowledgeStoreIdentityForNonBlankValue() {
        XCTAssertEqual(
            AdBannerView.normalizedAlwaysSkipSponsor(" Acme\n"),
            "acme\n",
            "Non-empty sponsor keys must retain SponsorKnowledgeStore's whitespace-only normalization contract"
        )
    }

    func testClaimedSponsorReceiptIsNotReplacedByCurrentCoalescing() async throws {
        let queue = AdBannerQueue()
        let item = makeItem(
            id: "receipt-current",
            adStartTime: 100,
            adEndTime: 130
        )
        let adjacent = makeItem(
            id: "receipt-next",
            adStartTime: 135,
            adEndTime: 160
        )
        var alwaysSkipCalls = 0
        let view = AdBannerView(
            queue: queue,
            onAlwaysSkipSponsor: { _ in alwaysSkipCalls += 1 }
        )
        queue.enqueue(item)

        XCTAssertTrue(view.handleAlwaysSkipSponsor(for: item))
        queue.enqueue(adjacent)
        XCTAssertEqual(queue.currentBanner?.id, item.id,
            "A claimed receipt must remain stable instead of exposing inert replacement controls")

        queue.dismiss()
        let advanceTask = try XCTUnwrap(queue.advanceTaskForTesting())
        await advanceTask.value
        XCTAssertEqual(queue.currentBanner?.id, adjacent.id)
        XCTAssertEqual(alwaysSkipCalls, 1)
        queue.dismiss()
    }

    func testAutoSkippedNoRecordsDeniedAndUsesNotAnAdExactlyOnce() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(id: "auto-no")
        var correctionCalls = 0
        let view = AdBannerView(
            queue: queue,
            onNotAnAd: { received in
                XCTAssertEqual(received.id, item.id)
                correctionCalls += 1
            }
        )
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertTrue(view.handleFeedback(.denied, for: item))
        XCTAssertFalse(view.handleFeedback(.confirmed, for: item),
            "A stale/repeated tap must not record or invoke a conflicting action")

        XCTAssertEqual(store.snapshot.bannersConfirmed, 0)
        XCTAssertEqual(store.snapshot.bannersDenied, 1)
        XCTAssertEqual(correctionCalls, 1)
    }

    func testAutoSkippedNoWithoutCorrectionHandlerDoesNotRecordOrDismiss() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(id: "auto-no-unwired")
        let view = AdBannerView(queue: queue)
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertFalse(view.isFeedbackResponseAvailable(.denied, for: item))
        XCTAssertFalse(view.handleFeedback(.denied, for: item))
        XCTAssertEqual(store.snapshot.bannersDenied, 0,
            "A denial label is invalid when no restore action is available")
        XCTAssertEqual(queue.currentBanner?.id, item.id,
            "The banner must remain actionable rather than pretending the response succeeded")
    }

    func testSuggestYesRecordsConfirmedAndUsesAcceptPathOnly() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeSuggestItem(id: "suggest-yes")
        var acceptCalls = 0
        var declineCalls = 0
        queue.onSuggestExitWithoutSkip = { _, _ in declineCalls += 1 }
        let view = AdBannerView(
            queue: queue,
            onSuggestSkip: { received in
                XCTAssertEqual(received.id, item.id)
                acceptCalls += 1
            }
        )
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertTrue(view.handleFeedback(.confirmed, for: item))
        XCTAssertFalse(view.handleFeedback(.confirmed, for: item))

        XCTAssertEqual(store.snapshot.bannersConfirmed, 1)
        XCTAssertEqual(store.snapshot.bannersDenied, 0)
        XCTAssertEqual(acceptCalls, 1)
        XCTAssertEqual(declineCalls, 0,
            "The accept path must suppress suggest-exit/decline correction")
    }

    func testSuggestYesWithoutAcceptHandlerDoesNotRecordOrDismiss() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeSuggestItem(id: "suggest-yes-unwired")
        let view = AdBannerView(queue: queue)
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertFalse(view.isFeedbackResponseAvailable(.confirmed, for: item))
        XCTAssertFalse(view.handleFeedback(.confirmed, for: item))
        XCTAssertEqual(store.snapshot.bannersConfirmed, 0,
            "A confirmation label is invalid when no skip action is available")
        XCTAssertEqual(queue.currentBanner?.id, item.id,
            "The banner must remain actionable rather than pretending the response succeeded")
    }

    func testSuggestNoRecordsDeniedAndUsesExplicitFalsePositivePath() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeSuggestItem(id: "suggest-no")
        var capturedExit: (id: String, isExplicitDenial: Bool)?
        queue.onSuggestExitWithoutSkip = { received, isExplicitDenial in
            capturedExit = (received.id, isExplicitDenial)
        }
        var acceptCalls = 0
        let view = AdBannerView(
            queue: queue,
            onSuggestSkip: { _ in acceptCalls += 1 }
        )
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertTrue(view.handleFeedback(.denied, for: item))

        XCTAssertEqual(store.snapshot.bannersConfirmed, 0)
        XCTAssertEqual(store.snapshot.bannersDenied, 1)
        XCTAssertEqual(capturedExit?.id, item.id)
        XCTAssertEqual(capturedExit?.isExplicitDenial, true,
            "No must use the existing explicit suggest-decline correction path")
        XCTAssertEqual(acceptCalls, 0)
    }

    func testSuggestNoWithoutDeclineHandlerDoesNotRecordOrDismiss() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeSuggestItem(id: "suggest-no-unwired")
        let view = AdBannerView(queue: queue)
        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        XCTAssertFalse(view.isFeedbackResponseAvailable(.denied, for: item))
        XCTAssertFalse(view.handleFeedback(.denied, for: item))
        XCTAssertEqual(store.snapshot.bannersDenied, 0,
            "A denial label is invalid when no explicit decline action is available")
        XCTAssertEqual(queue.currentBanner?.id, item.id,
            "The banner must remain actionable rather than pretending the response succeeded")
    }

    func testPassiveSuggestAutoFadeDoesNotRecordExplicitFeedback() async throws {
        let sleep = ControlledAutoDismissSleep()
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(
            suggestAutoDismissSeconds: 0.01,
            autoDismissSleep: { seconds in await sleep.sleep(seconds: seconds) },
            feedbackCounterStore: store
        )
        let item = makeSuggestItem(id: "suggest-fade-feedback")
        queue.enqueue(item)
        AdBannerView(queue: queue).handleBannerAppear(for: item)

        let dismissTask = try XCTUnwrap(queue.autoDismissTaskForTesting())
        _ = await sleep.waitForObservedDwell()
        await sleep.release()
        await dismissTask.value

        XCTAssertEqual(store.snapshot.bannersShown, 1)
        XCTAssertEqual(store.snapshot.bannersConfirmed, 0)
        XCTAssertEqual(store.snapshot.bannersDenied, 0,
            "Passive fade must not be mislabeled as explicit denial")
    }

    func testNeutralSuggestDismissDoesNotRecordExplicitFeedback() {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeSuggestItem(id: "suggest-neutral-dismiss")
        var capturedExplicitDenial: Bool?
        queue.onSuggestExitWithoutSkip = { _, isExplicitDenial in
            capturedExplicitDenial = isExplicitDenial
        }
        queue.enqueue(item)
        AdBannerView(queue: queue).handleBannerAppear(for: item)

        queue.dismiss()

        XCTAssertEqual(store.snapshot.bannersShown, 1)
        XCTAssertEqual(store.snapshot.bannersConfirmed, 0)
        XCTAssertEqual(store.snapshot.bannersDenied, 0)
        XCTAssertEqual(capturedExplicitDenial, false)
    }

    func testStaleNeutralDismissCannotRemoveReplacementPresentation() {
        let queue = AdBannerQueue()
        let stale = makeSuggestItem(id: "stale-neutral-dismiss")
        let replacement = makeSuggestItem(id: "replacement-presentation")
        queue.enqueue(stale)
        queue.dismiss()
        queue.enqueue(replacement)
        let view = AdBannerView(queue: queue)

        XCTAssertFalse(view.handleNeutralDismiss(for: stale))
        XCTAssertEqual(queue.currentBanner?.id, replacement.id)
        XCTAssertTrue(view.handleNeutralDismiss(for: replacement))
        XCTAssertNil(queue.currentBanner)
    }

    func testFeedbackCopyAndAccessibilityContractsAreCalmAndExplicit() {
        XCTAssertEqual(AdBannerView.feedbackPrompt, "Was this right?")
        XCTAssertEqual(AdBannerView.confirmFeedbackLabel, "Yes")
        XCTAssertEqual(AdBannerView.denyFeedbackLabel, "No")
        XCTAssertEqual(AdBannerView.feedbackMinimumTapSize, 44)
        XCTAssertEqual(AdBannerView.primaryCopyTypographyRole, .caption)
        XCTAssertEqual(AdBannerView.detailCopyTypographyRole, .timestamp)
        XCTAssertEqual(AdBannerView.evidenceTypographyRole, .caption)
        XCTAssertEqual(AdBannerView.confirmationTypographyRole, .caption)
        for role in [
            AdBannerView.primaryCopyTypographyRole,
            AdBannerView.detailCopyTypographyRole,
            AdBannerView.evidenceTypographyRole,
            AdBannerView.confirmationTypographyRole,
        ] {
            XCTAssertEqual(
                AppTypography.descriptor(for: role).textStyle,
                .caption,
                "Essential banner text must use a Dynamic-Type-relative role"
            )
        }

        let autoSkipped = AdBannerView.feedbackChoiceContent(for: .autoSkipped)
        XCTAssertEqual(autoSkipped.prompt, "Was this right?")
        XCTAssertEqual(autoSkipped.confirmLabel, "Yes")
        XCTAssertEqual(autoSkipped.denyLabel, "No")
        XCTAssertEqual(autoSkipped.confirmAccessibilityLabel, "Yes, the skip was right")
        XCTAssertEqual(autoSkipped.confirmAccessibilityHint, "Confirms Playhead skipped an ad")
        XCTAssertEqual(autoSkipped.denyAccessibilityLabel, "No, this was not an ad")
        XCTAssertEqual(
            autoSkipped.denyAccessibilityHint,
            "Records that this skipped segment was not an ad",
            "Auto-skip No must not promise the separate Listen/rewind action"
        )

        let suggest = AdBannerView.feedbackChoiceContent(for: .suggest)
        XCTAssertEqual(suggest.prompt, "Was this right?")
        XCTAssertEqual(suggest.confirmLabel, "Yes")
        XCTAssertEqual(suggest.denyLabel, "No")
        XCTAssertEqual(suggest.confirmAccessibilityLabel, "Yes, skip this sponsor break")
        XCTAssertEqual(suggest.confirmAccessibilityHint, "Confirms this is an ad and skips it")
        XCTAssertEqual(suggest.denyAccessibilityLabel, "No, this was not an ad")
        XCTAssertEqual(
            suggest.denyAccessibilityHint,
            "Marks this suggestion wrong and leaves playback unchanged"
        )

        XCTAssertFalse(
            AdBannerView.autoSkippedUtilityUsesStackedLayout(for: .large)
        )
        XCTAssertTrue(
            AdBannerView.autoSkippedUtilityUsesStackedLayout(
                for: .accessibility1
            ),
            "Accessibility Dynamic Type must move utility actions onto two rows"
        )
        XCTAssertTrue(
            AdBannerView.autoSkippedUtilityUsesStackedLayout(
                for: .accessibility5
            )
        )
        XCTAssertFalse(
            AdBannerView.bannerHeaderUsesStackedLayout(for: .large)
        )
        XCTAssertTrue(
            AdBannerView.bannerHeaderUsesStackedLayout(
                for: .accessibility1
            )
        )
        XCTAssertTrue(
            AdBannerView.bannerHeaderUsesStackedLayout(
                for: .accessibility5
            )
        )
        XCTAssertFalse(
            AdBannerView.feedbackChoiceUsesStackedLayout(for: .large)
        )
        XCTAssertTrue(
            AdBannerView.feedbackChoiceUsesStackedLayout(
                for: .accessibility1
            ),
            "Accessibility Dynamic Type must keep the question and both answers visible on separate rows"
        )
        XCTAssertTrue(
            AdBannerView.feedbackChoiceUsesStackedLayout(
                for: .accessibility5
            )
        )
        XCTAssertEqual(
            AdBannerView.expandedEvidenceLineLimit(for: .large),
            2
        )
        XCTAssertNil(
            AdBannerView.expandedEvidenceLineLimit(
                for: .accessibility5
            ),
            "Expanded evidence must not truncate at accessibility sizes"
        )
    }

    func testProductionFeedbackCompositionRoutesActionsAndPersistsShownCount() async {
        let store = makeFeedbackStore()
        var reverted: (
            windowId: String,
            podcastId: String,
            analysisAssetId: String?,
            startTime: Double,
            endTime: Double,
            expectedEpisodeId: String?,
            expectedPlaybackGeneration: UInt64?,
            expectedMaterialToken: String?
        )?
        var accepted: (
            windowId: String,
            expectedEpisodeId: String?,
            expectedPlaybackGeneration: UInt64?,
            expectedSuggestionRevisionToken: String?
        )?
        var autoConfirmed: (
            windowId: String,
            analysisAssetId: String?,
            startTime: Double,
            endTime: Double,
            expectedEpisodeId: String?,
            expectedPlaybackGeneration: UInt64?,
            expectedMaterialToken: String?
        )?
        var declines: [(
            windowId: String,
            isExplicitDenial: Bool,
            expectedEpisodeId: String?,
            expectedPlaybackGeneration: UInt64?,
            expectedSuggestionRevisionToken: String?
        )] = []
        let actions = BannerFeedbackProductionActions(
            confirmAutoSkippedBanner: {
                windowId,
                analysisAssetId,
                startTime,
                endTime,
                expectedEpisodeId,
                expectedPlaybackGeneration,
                expectedMaterialToken in
                autoConfirmed = (
                    windowId,
                    analysisAssetId,
                    startTime,
                    endTime,
                    expectedEpisodeId,
                    expectedPlaybackGeneration,
                    expectedMaterialToken
                )
                return true
            },
            revertWindow: {
                windowId,
                podcastId,
                analysisAssetId,
                startTime,
                endTime,
                expectedEpisodeId,
                expectedPlaybackGeneration,
                expectedMaterialToken in
                reverted = (
                    windowId,
                    podcastId,
                    analysisAssetId,
                    startTime,
                    endTime,
                    expectedEpisodeId,
                    expectedPlaybackGeneration,
                    expectedMaterialToken
                )
                return true
            },
            acceptSuggestedSkip: {
                windowId,
                expectedEpisodeId,
                expectedPlaybackGeneration,
                expectedSuggestionRevisionToken in
                accepted = (
                    windowId,
                    expectedEpisodeId,
                    expectedPlaybackGeneration,
                    expectedSuggestionRevisionToken
                )
                return true
            },
            declineSuggestedSkip: {
                windowId,
                isExplicitDenial,
                expectedEpisodeId,
                expectedPlaybackGeneration,
                expectedSuggestionRevisionToken in
                declines.append((
                    windowId,
                    isExplicitDenial,
                    expectedEpisodeId,
                    expectedPlaybackGeneration,
                    expectedSuggestionRevisionToken
                ))
                return true
            }
        )
        let queue = actions.makeQueue(feedbackCounterStore: store)
        let autoSkipped = makeItem(
            id: "production-auto",
            windowId: "window-auto",
            adStartTime: 120,
            adEndTime: 150,
            episodeId: "episode-auto",
            playbackLifecycleGeneration: 70,
            analysisAssetId: "asset-auto",
            windowMaterialRevisionToken: "material-auto"
        )
        let autoConfirmedItem = makeItem(
            id: "production-auto-yes",
            windowId: "window-auto-yes",
            adStartTime: 210,
            adEndTime: 240,
            episodeId: "episode-auto-yes",
            playbackLifecycleGeneration: 71,
            analysisAssetId: "asset-auto-yes",
            windowMaterialRevisionToken: "material-auto-yes"
        )
        let suggestYes = makeSuggestItem(
            id: "production-suggest-yes",
            windowId: "window-suggest-yes",
            episodeId: "episode-suggest-yes"
        )
        let suggestNo = makeSuggestItem(
            id: "production-suggest-no",
            windowId: "window-suggest-no",
            episodeId: "episode-suggest-no"
        )
        let suggestPassive = makeSuggestItem(
            id: "production-suggest-passive",
            windowId: "window-suggest-passive",
            episodeId: "episode-suggest-passive"
        )
        let view = AdBannerView(
            queue: queue,
            onAutoSkipConfirmedAsync: actions.onAutoSkipConfirmed,
            onNotAnAdAsync: actions.onNotAnAd,
            onSuggestSkipAsync: actions.onSuggestSkip,
            onSuggestDeclineAsync: actions.onSuggestDecline
        )

        queue.enqueue(autoSkipped)
        view.handleBannerAppear(for: autoSkipped)
        let didDenyAuto = await view.handleFeedbackAwaitingAction(
            .denied,
            for: autoSkipped
        )
        XCTAssertTrue(didDenyAuto)
        XCTAssertEqual(reverted?.windowId, autoSkipped.windowId)
        XCTAssertEqual(reverted?.podcastId, autoSkipped.podcastId)
        XCTAssertEqual(
            reverted?.analysisAssetId,
            autoSkipped.analysisAssetId
        )
        XCTAssertEqual(reverted?.startTime, autoSkipped.adStartTime)
        XCTAssertEqual(reverted?.endTime, autoSkipped.adEndTime)
        XCTAssertEqual(reverted?.expectedEpisodeId, autoSkipped.episodeId)
        XCTAssertEqual(
            reverted?.expectedPlaybackGeneration,
            autoSkipped.playbackLifecycleGeneration
        )
        XCTAssertEqual(
            reverted?.expectedMaterialToken,
            autoSkipped.windowMaterialRevisionToken
        )

        queue.enqueue(autoConfirmedItem)
        view.handleBannerAppear(for: autoConfirmedItem)
        let didConfirmAuto = await view.handleFeedbackAwaitingAction(
            .confirmed,
            for: autoConfirmedItem
        )
        XCTAssertTrue(didConfirmAuto)
        XCTAssertEqual(autoConfirmed?.windowId, autoConfirmedItem.windowId)
        XCTAssertEqual(
            autoConfirmed?.analysisAssetId,
            autoConfirmedItem.analysisAssetId
        )
        XCTAssertEqual(autoConfirmed?.startTime, autoConfirmedItem.adStartTime)
        XCTAssertEqual(autoConfirmed?.endTime, autoConfirmedItem.adEndTime)
        XCTAssertEqual(
            autoConfirmed?.expectedEpisodeId,
            autoConfirmedItem.episodeId
        )
        XCTAssertEqual(
            autoConfirmed?.expectedPlaybackGeneration,
            autoConfirmedItem.playbackLifecycleGeneration
        )
        XCTAssertEqual(
            autoConfirmed?.expectedMaterialToken,
            autoConfirmedItem.windowMaterialRevisionToken
        )

        queue.enqueue(suggestYes)
        view.handleBannerAppear(for: suggestYes)
        let didConfirmSuggestion = await view.handleFeedbackAwaitingAction(
            .confirmed,
            for: suggestYes
        )
        XCTAssertTrue(didConfirmSuggestion)
        XCTAssertEqual(accepted?.windowId, suggestYes.windowId)
        XCTAssertEqual(accepted?.expectedEpisodeId, suggestYes.episodeId)
        XCTAssertEqual(
            accepted?.expectedPlaybackGeneration,
            suggestYes.playbackLifecycleGeneration
        )
        XCTAssertEqual(
            accepted?.expectedSuggestionRevisionToken,
            suggestYes.suggestionRevisionToken
        )

        queue.enqueue(suggestNo)
        view.handleBannerAppear(for: suggestNo)
        let didDenySuggestion = await view.handleFeedbackAwaitingAction(
            .denied,
            for: suggestNo
        )
        XCTAssertTrue(didDenySuggestion)

        queue.enqueue(suggestPassive)
        view.handleBannerAppear(for: suggestPassive)
        queue.dismiss()
        for _ in 0..<10 where declines.count < 2 {
            await Task.yield()
        }

        XCTAssertEqual(
            store.snapshot,
            BannerFeedbackCounts(
                bannersShown: 5,
                bannersConfirmed: 2,
                bannersDenied: 2
            ),
            "The production composition must persist shown and explicit response aggregates"
        )
        XCTAssertEqual(
            declines.map(\.windowId),
            [suggestNo.windowId, suggestPassive.windowId]
        )
        XCTAssertEqual(
            declines.map(\.isExplicitDenial),
            [true, false],
            "The composed production queue must preserve explicit-No versus passive-exit semantics"
        )
        XCTAssertEqual(
            declines.map(\.expectedEpisodeId),
            [suggestNo.episodeId, suggestPassive.episodeId],
            "Every deferred correction must remain bound to its source episode"
        )
        XCTAssertEqual(
            declines.map(\.expectedSuggestionRevisionToken),
            [
                suggestNo.suggestionRevisionToken,
                suggestPassive.suggestionRevisionToken,
            ],
            "Every suggest response must remain bound to its source revision"
        )
    }

    func testRejectedAsyncFeedbackDoesNotDismissOrIncrementAndCanRetry() async {
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(
            id: "async-rejection",
            windowId: "async-rejection-window"
        )
        var actorAccepts = false
        let view = AdBannerView(
            queue: queue,
            onNotAnAdAsync: { _ in actorAccepts }
        )

        queue.enqueue(item)
        view.handleBannerAppear(for: item)

        let didAcceptRejectedAction = await view.handleFeedbackAwaitingAction(
            .denied,
            for: item
        )
        XCTAssertFalse(didAcceptRejectedAction)
        XCTAssertEqual(queue.currentBanner?.id, item.id)
        XCTAssertEqual(
            store.snapshot,
            BannerFeedbackCounts(
                bannersShown: 1,
                bannersConfirmed: 0,
                bannersDenied: 0
            ),
            "A lifecycle/revision rejection must not mint a feedback receipt"
        )
        XCTAssertTrue(
            view.isFeedbackResponseAvailable(.denied, for: item),
            "A rejected current action must release its claim for retry"
        )

        actorAccepts = true
        let didAcceptRetry = await view.handleFeedbackAwaitingAction(
            .denied,
            for: item
        )
        XCTAssertTrue(didAcceptRetry)
        XCTAssertNil(queue.currentBanner)
        XCTAssertEqual(
            store.snapshot,
            BannerFeedbackCounts(
                bannersShown: 1,
                bannersConfirmed: 0,
                bannersDenied: 1
            )
        )
    }

    func testNeutralDismissIsRejectedWhileAsyncFeedbackIsClaimed() async {
        let action = ControlledFeedbackAction()
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeItem(
            id: "claimed-async-action",
            windowId: "claimed-async-window"
        )
        let view = AdBannerView(
            queue: queue,
            onNotAnAdAsync: { _ in await action.run() }
        )

        queue.enqueue(item)
        view.handleBannerAppear(for: item)
        let responseTask = Task { @MainActor in
            await view.handleFeedbackAwaitingAction(.denied, for: item)
        }
        await action.waitUntilStarted()

        XCTAssertFalse(
            view.handleNeutralDismiss(for: item),
            "Neutral X must not erase a presentation whose accepted response is still in flight"
        )
        XCTAssertEqual(queue.currentBanner?.id, item.id)
        XCTAssertEqual(
            store.snapshot.bannersDenied,
            0,
            "The aggregate is finalized only after the actor accepts"
        )

        await action.release()
        let accepted = await responseTask.value
        XCTAssertTrue(accepted)
        XCTAssertNil(queue.currentBanner)
        XCTAssertEqual(store.snapshot.bannersDenied, 1)
    }

    func testHostDisappearDoesNotNeutralizeInFlightSuggestFeedback() async {
        let action = ControlledFeedbackAction()
        let store = makeFeedbackStore()
        let queue = AdBannerQueue(feedbackCounterStore: store)
        let item = makeSuggestItem(
            id: "claimed-suggest-host-exit",
            windowId: "claimed-suggest-window"
        )
        var neutralExits: [String] = []
        queue.onSuggestExitWithoutSkip = { exited, isExplicitDenial in
            if !isExplicitDenial {
                neutralExits.append(exited.id)
            }
        }
        let view = AdBannerView(
            queue: queue,
            onSuggestSkipAsync: { _ in await action.run() }
        )

        queue.enqueue(item)
        view.handleBannerAppear(for: item)
        let responseTask = Task { @MainActor in
            await view.handleFeedbackAwaitingAction(.confirmed, for: item)
        }
        await action.waitUntilStarted()

        queue.discardAllOnHostDisappear()
        XCTAssertNil(queue.currentBanner)
        XCTAssertTrue(
            neutralExits.isEmpty,
            "Host cleanup must not race an already-claimed explicit response"
        )

        await action.release()
        let accepted = await responseTask.value
        XCTAssertTrue(accepted)
        XCTAssertTrue(neutralExits.isEmpty)
        XCTAssertEqual(store.snapshot.bannersConfirmed, 1)
    }

    func testAcceptedOldAutoFeedbackCannotDismissReplacementHostCard() async {
        let action = ControlledFeedbackAction()
        let queue = AdBannerQueue()
        let old = makeItem(
            id: "old-auto-action",
            episodeId: "episode-a",
            playbackLifecycleGeneration: 1
        )
        let oldGeneration = queue.activateHost(
            for: "episode-a",
            playbackLifecycleGeneration: 1
        )
        let view = AdBannerView(
            queue: queue,
            onNotAnAdAsync: { _ in await action.run() }
        )
        XCTAssertTrue(queue.enqueue(old, hostGeneration: oldGeneration))
        let responseTask = Task { @MainActor in
            await view.handleFeedbackAwaitingAction(.denied, for: old)
        }
        await action.waitUntilStarted()

        queue.discardAllOnHostDisappear()
        let replacement = makeItem(
            id: "replacement-auto-card",
            episodeId: "episode-b",
            playbackLifecycleGeneration: 2
        )
        let replacementGeneration = queue.activateHost(
            for: "episode-b",
            playbackLifecycleGeneration: 2
        )
        XCTAssertTrue(
            queue.enqueue(
                replacement,
                hostGeneration: replacementGeneration
            )
        )

        await action.release()
        let accepted = await responseTask.value
        XCTAssertTrue(accepted)
        XCTAssertEqual(
            queue.currentBanner?.id,
            replacement.id,
            "completion of a retired card must not dismiss the new host's card"
        )
        XCTAssertNotNil(
            queue.autoDismissTaskForTesting(),
            "the replacement card must retain its own dwell timer"
        )
        queue.dismiss()
    }

    func testAcceptedOldSuggestFeedbackCannotCancelReplacementTimer() async {
        let action = ControlledFeedbackAction()
        let queue = AdBannerQueue()
        let old = makeSuggestItem(
            id: "old-suggest-action",
            windowId: "old-suggest-window",
            episodeId: "episode-a",
            playbackLifecycleGeneration: 1
        )
        let oldGeneration = queue.activateHost(
            for: "episode-a",
            playbackLifecycleGeneration: 1
        )
        let view = AdBannerView(
            queue: queue,
            onSuggestSkipAsync: { _ in await action.run() }
        )
        XCTAssertTrue(queue.enqueue(old, hostGeneration: oldGeneration))
        let responseTask = Task { @MainActor in
            await view.handleFeedbackAwaitingAction(.confirmed, for: old)
        }
        await action.waitUntilStarted()

        queue.discardAllOnHostDisappear()
        let replacement = makeSuggestItem(
            id: "replacement-suggest-card",
            windowId: "replacement-suggest-window",
            episodeId: "episode-b",
            playbackLifecycleGeneration: 2
        )
        let replacementGeneration = queue.activateHost(
            for: "episode-b",
            playbackLifecycleGeneration: 2
        )
        XCTAssertTrue(
            queue.enqueue(
                replacement,
                hostGeneration: replacementGeneration
            )
        )

        await action.release()
        let accepted = await responseTask.value
        XCTAssertTrue(accepted)
        XCTAssertEqual(queue.currentBanner?.id, replacement.id)
        XCTAssertNotNil(
            queue.autoDismissTaskForTesting(),
            "an old suggest acceptance must not cancel the new card's dwell"
        )
        queue.dismiss()
    }

    func testRejectedAsyncListenRemainsRetryableUntilAccepted() async {
        let queue = AdBannerQueue()
        let item = makeItem(id: "listen-persistence-retry")
        var attempts = 0
        let view = AdBannerView(
            queue: queue,
            onListenAsync: { _ in
                attempts += 1
                return attempts > 1
            }
        )
        queue.enqueue(item)

        let rejected = await view.handleListenAwaitingAction(for: item)
        XCTAssertFalse(rejected)
        XCTAssertEqual(queue.currentBanner?.id, item.id)
        XCTAssertFalse(queue.hasClaimedCurrentPresentation)

        let accepted = await view.handleListenAwaitingAction(for: item)
        XCTAssertTrue(accepted)
        XCTAssertEqual(attempts, 2)
        XCTAssertNil(queue.currentBanner)
    }

    func testRejectedAsyncSponsorWriteShowsNoReceiptAndCanRetry() async {
        let queue = AdBannerQueue()
        let item = makeItem(id: "sponsor-persistence-retry")
        var attempts = 0
        let view = AdBannerView(
            queue: queue,
            onAutoSkipConfirmed: { _ in },
            onAlwaysSkipSponsorAsync: { _ in
                attempts += 1
                return attempts > 1
            }
        )
        queue.enqueue(item)

        let rejected =
            await view.handleAlwaysSkipSponsorAwaitingPersistence(for: item)
        XCTAssertFalse(rejected)
        XCTAssertEqual(queue.currentBanner?.id, item.id)
        XCTAssertFalse(queue.hasClaimedCurrentPresentation)
        XCTAssertTrue(
            view.isFeedbackResponseAvailable(.confirmed, for: item),
            "a failed sponsor write must not consume the presentation"
        )

        let accepted =
            await view.handleAlwaysSkipSponsorAwaitingPersistence(for: item)
        XCTAssertTrue(accepted)
        XCTAssertEqual(attempts, 2)
        XCTAssertTrue(queue.hasClaimedCurrentPresentation)
        XCTAssertFalse(
            view.handleFeedback(.confirmed, for: item),
            "the accepted sponsor receipt owns the presentation action slot"
        )
        queue.dismiss()
    }
}
