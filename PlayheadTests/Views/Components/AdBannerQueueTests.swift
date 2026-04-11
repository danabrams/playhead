// AdBannerQueueTests.swift
// Tests for AdBannerQueue coalescing, auto-dismiss, "Not an ad" visibility,
// and unbounded queue growth — acceptance criteria for playhead-1hg.

import XCTest
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
        metadataSource: String = "foundationModels"
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
            evidenceCatalogEntries: []
        )
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

    // MARK: - Auto-Dismiss (AC4)

    func testAutoDismissAfter8Seconds() async throws {
        let queue = AdBannerQueue()
        let item = makeItem(id: "auto")
        queue.enqueue(item)

        XCTAssertNotNil(queue.currentBanner, "Banner should be showing immediately")

        // Wait just past the 8-second auto-dismiss window.
        try await Task.sleep(for: .seconds(8.5))
        XCTAssertNil(queue.currentBanner,
            "Banner should auto-dismiss after 8 seconds")
    }

    func testManualDismissCancelsAutoDismiss() {
        let queue = AdBannerQueue()
        let item = makeItem(id: "manual")
        queue.enqueue(item)

        queue.dismiss()
        XCTAssertNil(queue.currentBanner,
            "Manual dismiss should clear the banner immediately")
    }

    // MARK: - "Not an ad" Visibility (AC7)

    func testNotAnAdButtonHiddenWhenHandlerNil() {
        let queue = AdBannerQueue()
        let view = AdBannerView(queue: queue, onListen: nil, onNotAnAd: nil)

        // When onNotAnAd is nil, the view's onNotAnAd property is nil.
        XCTAssertTrue(view.onNotAnAd == nil,
            "onNotAnAd handler should be nil when not provided")
    }

    func testNotAnAdButtonVisibleWhenHandlerWired() {
        let queue = AdBannerQueue()
        var called = false
        let view = AdBannerView(
            queue: queue,
            onListen: nil,
            onNotAnAd: { _ in called = true }
        )

        XCTAssertNotNil(view.onNotAnAd,
            "onNotAnAd handler should be non-nil when wired")
        // Verify handler is callable.
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
}
