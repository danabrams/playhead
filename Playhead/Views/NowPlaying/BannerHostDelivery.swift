// BannerHostDelivery.swift
// playhead-8cjo: THE HOST'S HALF OF THE DELIVERY CONTRACT.
//
// `SkipOrchestrator` cannot see whether a card was presented. It can see that
// somebody subscribed to `bannerEventStream()`, and until this bead it read
// that as the same thing — `emitBannerItem` branched on
// `!bannerContinuations.isEmpty || !bannerEventContinuations.isEmpty` and
// booked the skip as delivered. `AdBannerQueue.enqueue(_:hostGeneration:)`
// returns `false` and DROPS the item whenever the host generation has moved on
// (the reattach window in `NowPlayingView.onChange(of: bannerPlaybackContext)`,
// a `discardAllOnHostDisappear` whose stream has not torn down yet, an episode
// or lifecycle mismatch), and the observation `Task` can be cancelled between
// the yield and the enqueue so nothing reaches the queue at all. In that window
// the skip produced neither a card the listener saw nor a row they could
// correct — playhead-2d6i's own defect, one layer up.
//
// So the queue's verdict has to travel BACK. That is all this type is: one
// event in, the queue's answer reported to the orchestrator.
//
// WHY IT IS A TYPE AND NOT A CLOSURE IN THE VIEW MODEL. The forwarding rule is
// the only place the two acknowledgements live, and it is the one thing no
// orchestrator test and no queue test can observe — each sees its own side of
// the hop. Lifting it out of `NowPlayingViewModel.observeBanners` (which needed
// nothing from the view model but the three values below) makes the REAL rule
// drivable against a REAL `AdBannerQueue` and a REAL `SkipOrchestrator`, so the
// partition can be asserted against the queue rather than at the orchestrator
// boundary where the defect is invisible. A rail that re-implements the
// forwarding would only ever prove that two call sites agree today.
// `BannerHostDeliveryWiringSourceCanaryTests` pins that the view model calls
// this and does not keep a second copy.

/// Forwards orchestrator banner events into a host's `AdBannerQueue` and
/// reports what the queue did with them.
enum BannerHostDelivery {

    /// Deliver one event, then acknowledge it if — and only if — the queue
    /// accepted the item.
    ///
    /// THE ACKNOWLEDGEMENT IS POSITIVE-ONLY IN BOTH TIERS, and that is the
    /// conservative direction rather than an omission. A rejected item needs no
    /// report: the suggest tier stays pending and is replayed to the next host
    /// (`replayPendingSuggestBanners`), and the auto tier's receipt is written
    /// by `emitBannerItem` before the yield and simply stays written. So the
    /// answer to "what happens when this function never runs at all" — a
    /// cancelled observation task, a torn-down host, an app that was killed —
    /// is the same as the answer to "what happens when the queue refuses":
    /// the listener keeps a correctable row for a skip nobody showed them.
    /// There is no interval in which silence reads as a presentation.
    ///
    /// The tier switch is exhaustive on purpose: a third tier must not inherit
    /// whichever acknowledgement happens to be written first.
    static func forward(
        _ event: AdBannerStreamEvent,
        from orchestrator: SkipOrchestrator,
        into queue: AdBannerQueue,
        hostGeneration: UInt64
    ) async {
        switch event {
        case let .present(item):
            let didAccept = await MainActor.run {
                queue.enqueue(item, hostGeneration: hostGeneration)
            }
            guard didAccept else { return }
            switch item.tier {
            case .suggest:
                await orchestrator.acknowledgeSuggestedBannerDelivery(
                    windowId: item.windowId,
                    episodeId: item.episodeId,
                    playbackLifecycleGeneration:
                        item.playbackLifecycleGeneration,
                    suggestionRevisionToken:
                        item.suggestionRevisionToken
                )
            case .autoSkipped:
                await orchestrator.acknowledgeAutoSkippedBannerDelivery(
                    windowId: item.windowId,
                    episodeId: item.episodeId,
                    playbackLifecycleGeneration:
                        item.playbackLifecycleGeneration,
                    windowMaterialRevisionToken:
                        item.windowMaterialRevisionToken
                )
            }
        case let .retireWindow(retirement):
            _ = await MainActor.run {
                queue.retireWindow(
                    retirement,
                    hostGeneration: hostGeneration
                )
            }
        }
    }
}
