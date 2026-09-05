import Foundation
import Testing
@testable import Playhead

/// playhead-pzojm — `AdBannerQueue.onAutoSkipCardPresented` fires at the
/// display boundary and nowhere else.
///
/// The flow rail (`AutoSkipCardDeliveryAgainstTheQueueTests`) proves what the
/// orchestrator ends up believing. This suite proves the one fact that rail
/// takes as given: the queue tells the view about an auto card exactly when
/// `recordBannerShown(for:)` admits it as shown — once per presentation, never
/// for a card that is merely queued, never for the suggest tier, and never for
/// a card the host discarded unseen.
@MainActor
@Suite("playhead-pzojm: the auto-card callback is the display boundary")
struct AutoSkipCardPresentedCallbackTests {

    private func item(_ windowId: String, tier: AdBannerTier = .autoSkipped) -> AdSkipBannerItem {
        AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: windowId,
            advertiser: nil,
            product: nil,
            adStartTime: 120.0,
            adEndTime: 180.0,
            metadataConfidence: nil,
            metadataSource: "none",
            podcastId: "podcast-test",
            evidenceCatalogEntries: [],
            tier: tier
        )
    }

    private func makeQueue() -> AdBannerQueue {
        AdBannerQueue(autoDismissSleep: { _ in
            try? await Task.sleep(for: .seconds(86_400))
        })
    }

    @Test("a presented auto card fires the callback exactly once")
    func presentedAutoCardFiresOnce() {
        let queue = makeQueue()
        var presented: [String] = []
        queue.onAutoSkipCardPresented = { presented.append($0.windowId) }
        let card = item("auto-1")
        queue.enqueue(card)

        #expect(queue.recordBannerShown(for: card))
        #expect(!queue.recordBannerShown(for: card), "the presentation guard must hold")
        #expect(!queue.recordBannerShown(for: card))
        #expect(presented == ["auto-1"], "once per presentation; got \(presented)")
    }

    @Test("a suggest card never fires the auto callback")
    func suggestCardDoesNotFireTheAutoCallback() {
        let queue = makeQueue()
        var presented: [String] = []
        queue.onAutoSkipCardPresented = { presented.append($0.windowId) }
        let card = item("suggest-1", tier: .suggest)
        queue.enqueue(card)

        #expect(queue.recordBannerShown(for: card), "the impression itself is still booked")
        #expect(presented.isEmpty, "the auto seam must not learn about a suggest card")
    }

    @Test("a card queued behind another is not presented, so it does not fire")
    func queuedCardDoesNotFire() {
        let queue = makeQueue()
        var presented: [String] = []
        queue.onAutoSkipCardPresented = { presented.append($0.windowId) }
        let first = item("pod-a")
        let second = item("pod-b")
        queue.enqueue(first)
        queue.enqueue(second)
        #expect(queue.currentBanner?.windowId == "pod-a", "the fixture did not queue the second behind the first")

        #expect(!queue.recordBannerShown(for: second), "a queued card is not on screen")
        #expect(presented.isEmpty)

        #expect(queue.recordBannerShown(for: first))
        #expect(presented == ["pod-a"])
    }

    @Test("a queued card discarded when the host leaves was never presented")
    func discardedUnseenCardNeverFires() {
        let queue = makeQueue()
        var presented: [String] = []
        queue.onAutoSkipCardPresented = { presented.append($0.windowId) }
        let first = item("pod-a")
        let second = item("pod-b")
        queue.enqueue(first)
        queue.enqueue(second)
        #expect(queue.recordBannerShown(for: first))

        queue.discardAllOnHostDisappear()

        #expect(queue.currentBanner == nil)
        #expect(
            presented == ["pod-a"],
            "the pod's second card left with the host and must not read as shown; got \(presented)"
        )
    }

    @Test("with no callback installed, the display boundary still books the impression")
    func nilCallbackIsHarmless() {
        let queue = makeQueue()
        let card = item("auto-1")
        queue.enqueue(card)
        #expect(queue.recordBannerShown(for: card))
    }
}
