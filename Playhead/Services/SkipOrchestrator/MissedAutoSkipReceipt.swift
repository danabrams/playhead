// MissedAutoSkipReceipt.swift
// playhead-2d6i: the record of an auto-skip that fired while NO banner host
// was attached.
//
// THE LOSS THIS CLOSES. `SkipOrchestrator.emitBannerItem` used to return
// without yielding when both continuation dictionaries were empty, and the
// window's one chance had already been consumed before that check — the id was
// removed from `armedAutoSkipBannerWindowIds` (post-bwxi) or inserted into
// `banneredWindowIds` (pre-bwxi) on the way in. So a skip that fired from the
// lock screen, from CarPlay, from a widget start, or during any locked stretch
// left NO receipt at all: the listener got the skip and could never say No to
// it. A false skip they cannot see is one they cannot correct, and corrections
// are the highest-quality signal this project gets.
//
// WHY THIS IS A LIST AND NOT A REPLAYED CARD (Dan, 2026-08-22). The suggest
// tier replays through `replayPendingSuggestBanners`, and that is a genuinely
// different thing: a suggestion is a LIVE AFFORDANCE whose span may still be
// ahead of the playhead, so replaying it as a card offers something the
// listener can still do. This replays a RECORD of something already done. The
// banner's primary action is SKIP and a tap at entry is a PREDICTION
// (feedback_banner_is_a_skip_affordance), so a card for audio already past
// asserts an affordance that no longer exists. A passive list keeps the
// correction and drops the false affordance. Do not blur the two mechanisms
// into one because they share a trigger.
//
// WHY IT CARRIES THE WHOLE `AdSkipBannerItem` RATHER THAN A SUMMARY. The card's
// No goes `AdBannerView.handleFeedbackAwaitingAction` ->
// `BannerFeedbackProductionActions.onNotAnAd(item, .card)` ->
// `SkipOrchestrator.denyAutoSkippedBanner(...)`, and every argument of that
// last call is a field of the item. Carrying the item verbatim is what makes
// "correcting from the list reaches the same path a card's veto does" true BY
// CONSTRUCTION rather than by two call sites that agree today: the list hands
// the identical value to the identical closure. A summary struct would be a
// second derivation of the same arguments, which is how a surface comes to
// promise a correction the transaction will refuse.
//
// playhead-nq8z: the list passes `.missedAutoSkipList` where the card passes
// `.card`, and that argument is the WHOLE difference between the two answers.
// It is not a second path — it is one closure told which door it came through,
// so the resulting `correction_events.source` says so and a corpus reader can
// tell a veto made minutes later from a card's No tapped late. Nothing else
// about the hop changes, which is why the item is still forwarded verbatim.

import Foundation

/// One auto-skip that happened with no banner host attached, held until a
/// surface asks for it.
///
/// Identity is the window: `SkipOrchestrator` keys these by `windowId`, so a
/// window contributes AT MOST ONE entry no matter how many times a host later
/// attaches and detaches. That is the double-delivery direction the suggest
/// tier states as "delivered exactly once", expressed as state rather than as
/// a delivery gate — there is nothing to deliver twice.
struct MissedAutoSkipReceipt: Sendable, Equatable, Identifiable {

    /// The card that WOULD have been presented, verbatim. Its fields are the
    /// arguments `denyAutoSkippedBanner` takes; see the file header.
    let item: AdSkipBannerItem

    /// The listener's position, in episode seconds, at the observation that
    /// announced the skip — i.e. inside `[item.adStartTime, item.adEndTime)`,
    /// the same half-open predicate `PlaybackTransport.checkSkipCues` fires the
    /// skip on.
    ///
    /// This is NOT the value stamped into a correction's
    /// `playheadTimeAtCorrection`. That column records where the listener was
    /// when they TAPPED, and for a list entry those are deliberately different
    /// numbers — the whole point of the list is that the tap comes later, from
    /// somewhere else in the episode. Stamping this one onto a correction would
    /// fabricate containment, which is precisely the reading playhead-bwxi's
    /// V59 column exists to make impossible.
    let playheadTimeAtSkip: TimeInterval

    /// Wall clock at the skip, so a surface can say how long ago it happened.
    let occurredAt: Date

    var id: String { item.windowId }

    /// The window's span, for display. Named rather than reached through
    /// `item` at each call site so a surface never has to know that a list
    /// entry is a banner item underneath.
    var adStartTime: Double { item.adStartTime }
    var adEndTime: Double { item.adEndTime }
    var windowId: String { item.windowId }
    var advertiser: String? { item.advertiser }
    var product: String? { item.product }
}

// MARK: - List copy

/// The two strings a list row shows. On the model rather than inside the
/// view's `private extension` so they are reachable from a test: this copy is
/// subject to the voice rules in `feedback_peace_of_mind_not_metrics` (no
/// quantified language, nothing that reads as an error log) and a rule nothing
/// can assert is a rule nobody keeps.
extension MissedAutoSkipReceipt {

    /// Advertiser, then product, then a neutral phrase. Never a confidence,
    /// never a count, never "detected".
    var displayTitle: String {
        if let advertiser,
           !advertiser.trimmingCharacters(in: .whitespaces).isEmpty {
            return advertiser
        }
        if let product,
           !product.trimmingCharacters(in: .whitespaces).isEmpty {
            return product
        }
        return "Sponsor segment"
    }

    /// WHICH WINDOW and WHEN, which is what Dan asked a list entry to carry.
    ///
    /// The "when" is the EPISODE span, not a wall-clock age. `occurredAt` is
    /// recorded and is deliberately not shown here: "12 minutes ago" tells the
    /// listener how long ago they were on the lock screen and nothing at all
    /// about which audio the row is asking them to judge.
    var spanLabel: String {
        let start = TimeFormatter.formatTime(adStartTime)
        let end = TimeFormatter.formatTime(adEndTime)
        return "Skipped \(start)–\(end)"
    }
}
