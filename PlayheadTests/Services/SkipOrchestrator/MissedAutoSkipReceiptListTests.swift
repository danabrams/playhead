// MissedAutoSkipReceiptListTests.swift
// playhead-2d6i: THE LIST IS ONLY WORTH HAVING IF IT CAN CORRECT.
//
// `BannerPlayheadBiconditionalTests` owns the PARTITION — a card iff the
// playhead is inside the window and a host is attached, otherwise exactly one
// list entry — and that is deliberately not duplicated here. What this suite
// owns is everything downstream of a row existing:
//
//   * a row reaches `denyAutoSkippedBanner`, the SAME seam a card's No reaches,
//     and commits the same durable receipt;
//   * what `playheadTimeAtCorrection` records for it, which is the one place
//     this feature could quietly poison the column playhead-bwxi added;
//   * a row that could NOT be corrected is never shown, so the affordance is
//     never a button that does nothing;
//   * the copy, because it is subject to voice rules nothing else can assert.
//
// WHY THE VETO AND NOT ALSO A YES. Dan's requirement is "the same veto path a
// card offers … if correcting from the list cannot reach
// `confirmAutoSkippedBanner`'s counterpart, the feature is decorative" — and
// that counterpart is `denyAutoSkippedBanner`. The asymmetry is deliberate and
// is the whole reason the column stays readable: a listener who never saw a
// card never HEARD the ad — it was skipped — so a Yes from this list would be
// `bannerAutoSkipConfirmed`, the strongest positive signal the trust system
// takes, recorded for audio they had not reached. That is the 2026-08-21 field
// incident verbatim (playhead-bwxi: four confirmations inside six seconds, three
// of them for windows at 23, 56 and 71 minutes). A veto costs nothing if it is
// wrong; a confirmation from here would be exactly the poison bwxi removed.
//
// OBSERVATION METHOD. No streams and no sentinels: this suite never attaches a
// host, which is the state under test, and every observable is an awaited
// actor call. Emission is synchronous inside the actor, so by the time
// `updatePlayheadTime` returns the list is already whatever that observation
// made it.

import Foundation
import Testing

@testable import Playhead

@Suite(
    "playhead-2d6i — a missed auto-skip receipt can still be corrected",
    .timeLimit(.minutes(1))
)
struct MissedAutoSkipReceiptListTests {

    // MARK: Fixture

    private static let assetId = "asset-1"
    private static let episodeId = "asset-1"
    /// Must be the show `makeSkipTestTrustService` seeds, or nothing reaches
    /// `.applied` and every test here passes vacuously.
    private static let podcastId = "podcast-1"
    private static let episodeDuration: Double = 4309.42
    private static let playbackLifecycleGeneration: UInt64 = 1

    /// The 2026-08-21 pre-roll, the same span the biconditional suite walks.
    private static let preRollStart: Double = 0.0
    private static let preRollEnd: Double = 86.831

    private static func autoWindow(
        id: String,
        start: Double,
        end: Double,
        confidence: Double = 1.0,
        decisionState: String = AdDecisionState.candidate.rawValue,
        advertiser: String? = nil,
        product: String? = nil
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: "lexical",
            decisionState: decisionState,
            detectorVersion: "detection-v1",
            advertiser: advertiser,
            product: product,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: start,
            metadataSource: advertiser == nil && product == nil
                ? "none"
                : "foundationModels",
            metadataConfidence: advertiser == nil && product == nil ? nil : 0.9,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
        )
    }

    private static func makeHarness() async throws
        -> (orchestrator: SkipOrchestrator, store: AnalysisStore) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                episodeDurationSec: episodeDuration
            )
        )
        let trust = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trust,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId,
            playbackLifecycleGeneration: playbackLifecycleGeneration
        )
        return (orchestrator, store)
    }

    /// Drive one unattended auto-skip and hand back its list row.
    ///
    /// The window is inserted DURABLY as well as delivered: `persistDeniedAutoSkip`
    /// re-reads the row it is vetoing, so a correction over a window the store
    /// has never seen is refused — correctly, and it would make every test here
    /// vacuous.
    private static func makeOneMissedReceipt(
        _ orchestrator: SkipOrchestrator,
        _ store: AnalysisStore,
        window: AdWindow,
        observeAt: Double
    ) async throws -> MissedAutoSkipReceipt {
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])
        await orchestrator.updatePlayheadTime(observeAt)
        let receipts = await orchestrator.missedAutoSkipReceipts()
        return try #require(
            receipts.first,
            """
            no missed receipt was recorded for a skip that fired at \
            \(observeAt) s with nothing subscribed. That is the shipped defect \
            playhead-2d6i closes: `emitBannerItem` returned without yielding, \
            and the caller had already spent the window's one chance.
            """
        )
    }

    // MARK: - 1. The veto reaches the card's own seam

    /// The claim Dan made the feature conditional on: "if correcting from the
    /// list cannot reach `confirmAutoSkippedBanner`'s counterpart, the feature
    /// is decorative."
    ///
    /// Asserted end to end rather than at a wiring seam: the row's own fields go
    /// into `denyAutoSkippedBanner`, and the durable `bannerAutoSkipDenied`
    /// receipt plus the reverted decision state come out. If the receipt dropped
    /// a single one of the eight identity fields that call takes, this returns
    /// false.
    @Test("A list row's veto commits the same durable receipt a card's No does")
    func aListRowsVetoReachesDenyAutoSkippedBanner() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let window = Self.autoWindow(
            id: "preroll", start: Self.preRollStart, end: Self.preRollEnd
        )
        let receipt = try await Self.makeOneMissedReceipt(
            orchestrator, store, window: window, observeAt: 40
        )

        let accepted = await orchestrator.denyAutoSkippedBanner(
            windowId: receipt.item.windowId,
            analysisAssetId: receipt.item.analysisAssetId,
            startTime: receipt.item.adStartTime,
            endTime: receipt.item.adEndTime,
            podcastId: receipt.item.podcastId,
            ifCurrentEpisodeId: receipt.item.episodeId,
            ifPlaybackLifecycleGeneration:
                receipt.item.playbackLifecycleGeneration,
            ifWindowMaterialRevisionToken:
                receipt.item.windowMaterialRevisionToken
        )
        #expect(
            accepted,
            """
            the veto was refused. A list entry that cannot correct is \
            decorative — and it is refused here only if the receipt failed to \
            carry one of the identity fields the seam requires (asset, episode, \
            playback generation, span, material token).
            """
        )

        let events = try await store.loadCorrectionEvents(
            analysisAssetId: Self.assetId
        )
        let denial = try #require(
            events.first { $0.source == .bannerAutoSkipDenied },
            """
            expected a bannerAutoSkipDenied row — the SAME source a card's No \
            writes. Got \(events.map { String(describing: $0.source) }).
            """
        )
        #expect(denial.correctionType == .falsePositive)

        let state = await orchestrator._managedDecisionStateForTesting(
            id: receipt.item.windowId
        )
        #expect(
            state == .reverted,
            """
            the window is \(String(describing: state)) after an accepted veto. \
            The correction has to move the SKIP as well as write the receipt, \
            or the listener says "not an ad" and it keeps being skipped.
            """
        )
    }

    /// A row that has been answered is gone. Not because anything prunes it —
    /// nothing does — but because `missedAutoSkipReceipts()` re-derives
    /// vetoability from live state, so a `.reverted` window has nothing left to
    /// list.
    @Test("An answered row leaves the list")
    func anAnsweredRowLeavesTheList() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let window = Self.autoWindow(
            id: "preroll", start: Self.preRollStart, end: Self.preRollEnd
        )
        let receipt = try await Self.makeOneMissedReceipt(
            orchestrator, store, window: window, observeAt: 40
        )
        _ = await orchestrator.denyAutoSkippedBanner(
            windowId: receipt.item.windowId,
            analysisAssetId: receipt.item.analysisAssetId,
            startTime: receipt.item.adStartTime,
            endTime: receipt.item.adEndTime,
            podcastId: receipt.item.podcastId,
            ifCurrentEpisodeId: receipt.item.episodeId,
            ifPlaybackLifecycleGeneration:
                receipt.item.playbackLifecycleGeneration,
            ifWindowMaterialRevisionToken:
                receipt.item.windowMaterialRevisionToken
        )
        let after = await orchestrator.missedAutoSkipReceipts()
        #expect(
            after.isEmpty,
            """
            the row is still listed after being answered: \
            \(after.map(\.windowId)). Tapping it again would be refused, so it \
            is a button that does nothing.
            """
        )
    }

    /// THE STANDING GUARANTEE, stated as itself: what the list offers, the
    /// seam accepts.
    ///
    /// The one way a row's material goes stale UNDER it is a same-ID producer
    /// revision. `receiveAdWindows` retires the old presentation and installs
    /// the replacement, so the window is STILL `.applied` afterwards — the
    /// decision-state clause of the filter cannot see this at all, and the
    /// material-token clause is the only thing standing between the listener
    /// and a row whose veto `denyAutoSkippedBanner` would refuse on its token
    /// check. That is the non-vacuity this test turns on: it asserts the window
    /// is still there and still applied BEFORE asserting the stale row is gone.
    @Test("A row whose producer material was replaced is withheld, not offered and refused")
    func aRowWhoseMaterialWasReplacedIsWithheld() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let window = Self.autoWindow(
            id: "preroll",
            start: Self.preRollStart,
            end: Self.preRollEnd,
            advertiser: "Acme Insurance"
        )
        let receipt = try await Self.makeOneMissedReceipt(
            orchestrator, store, window: window, observeAt: 40
        )
        let staleToken = try #require(
            receipt.item.windowMaterialRevisionToken
        )

        // A same-ID producer value with different material, delivered as the
        // durable `.applied` shape the preload path uses.
        await orchestrator.receiveAdWindows([
            Self.autoWindow(
                id: "preroll",
                start: Self.preRollStart,
                end: Self.preRollEnd,
                confidence: 0.91,
                decisionState: AdDecisionState.applied.rawValue,
                advertiser: "Acme Insurance Group"
            )
        ])

        // NON-VACUITY. If the revision had removed or reverted the window, the
        // decision-state clause would explain an empty list and this test would
        // prove nothing about the token.
        let state = await orchestrator._managedDecisionStateForTesting(
            id: "preroll"
        )
        #expect(
            state == .applied,
            """
            the window is \(String(describing: state)) after a same-ID \
            revision, so an empty list below would be explained by the \
            decision-state clause and this test would say nothing about the \
            material token. Re-derive the fixture.
            """
        )

        let listed = await orchestrator.missedAutoSkipReceipts()
        #expect(
            !listed.contains { $0.item.windowMaterialRevisionToken == staleToken },
            """
            the list still offers a row carrying the producer material the \
            revision replaced. `denyAutoSkippedBanner` compares the token \
            against the window it holds NOW, so tapping it is refused — a \
            button that does nothing, which is exactly the decorative outcome \
            Dan made the feature conditional on avoiding.
            """
        )
        // And every row it DOES offer is vetoable. (Empty here: the replacement
        // has been armed but no observation has entered its span since, so
        // nothing has been skipped under the new material yet.)
        for row in listed {
            let accepted = await orchestrator.denyAutoSkippedBanner(
                windowId: row.item.windowId,
                analysisAssetId: row.item.analysisAssetId,
                startTime: row.item.adStartTime,
                endTime: row.item.adEndTime,
                podcastId: row.item.podcastId,
                ifCurrentEpisodeId: row.item.episodeId,
                ifPlaybackLifecycleGeneration:
                    row.item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    row.item.windowMaterialRevisionToken
            )
            #expect(accepted, "the list offered \(row.windowId) and the seam refused it")
        }
    }

    // MARK: - 2. What the receipt says about WHERE THE LISTENER WAS

    /// playhead-bwxi's V59 column, on the one surface where the listener's
    /// position and the window are far apart BY DESIGN.
    ///
    /// The honest value is the LIVE position — where they were when they
    /// tapped — and this test pins it against the three ways it could be made
    /// to lie:
    ///
    ///   * `nil`, which would say "unknown" about a position that is known;
    ///   * the SPAN, which would fabricate containment and re-create exactly
    ///     the row shape bwxi's column exists to expose;
    ///   * `playheadTimeAtSkip`, which is where they were when the SKIP fired,
    ///     not when they corrected — a plausible-looking substitution of one
    ///     quantity for another.
    @Test("A veto from the list records where the listener actually was, not where the skip was")
    func aListVetoRecordsTheLivePlayheadNotTheSpan() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let window = Self.autoWindow(
            id: "preroll", start: Self.preRollStart, end: Self.preRollEnd
        )
        let receipt = try await Self.makeOneMissedReceipt(
            orchestrator, store, window: window, observeAt: 40
        )
        // THE EXACT OBSERVATION, not merely "somewhere inside the span". The
        // span START is also inside the span, so an `∈ [start, end)` assertion
        // is satisfied by a receipt that records the WINDOW's edge instead of
        // the LISTENER's position — the substitution of one quantity for
        // another that this repo keeps finding, and the one that would make a
        // list row claim the listener was at the top of the ad.
        #expect(
            receipt.playheadTimeAtSkip == 40,
            """
            the receipt says the skip fired at \(receipt.playheadTimeAtSkip) s \
            and the observation that fired it was at 40 s. The span is \
            [\(Self.preRollStart), \(Self.preRollEnd)), so its start would \
            satisfy a containment test while naming the wrong thing.
            """
        )
        #expect(
            receipt.playheadTimeAtSkip >= Self.preRollStart
                && receipt.playheadTimeAtSkip < Self.preRollEnd,
            """
            the receipt says the skip fired OUTSIDE the window it is about. The \
            auto tier's emit trigger is containment on the same half-open \
            predicate the transport fires the skip on, so this cannot happen \
            unless the two have come apart.
            """
        )

        // The listener has played on. This is the whole point of a passive
        // list: the tap comes later, from somewhere else in the episode.
        let positionAtTap: Double = 2000
        await orchestrator.updatePlayheadTime(positionAtTap)

        let accepted = await orchestrator.denyAutoSkippedBanner(
            windowId: receipt.item.windowId,
            analysisAssetId: receipt.item.analysisAssetId,
            startTime: receipt.item.adStartTime,
            endTime: receipt.item.adEndTime,
            podcastId: receipt.item.podcastId,
            ifCurrentEpisodeId: receipt.item.episodeId,
            ifPlaybackLifecycleGeneration:
                receipt.item.playbackLifecycleGeneration,
            ifWindowMaterialRevisionToken:
                receipt.item.windowMaterialRevisionToken
        )
        #expect(accepted)

        let events = try await store.loadCorrectionEvents(
            analysisAssetId: Self.assetId
        )
        let denial = try #require(
            events.first { $0.source == .bannerAutoSkipDenied }
        )
        let recorded = try #require(
            denial.playheadTimeAtCorrection,
            """
            the row records NO position for a gesture made at a known one. \
            `nil` means "nobody observed the playhead", and somebody did.
            """
        )
        #expect(
            recorded == positionAtTap,
            """
            recorded \(recorded) s for a tap made at \(positionAtTap) s. \
            The two other values in scope are the span start \
            (\(Self.preRollStart)) and the position at the skip \
            (\(receipt.playheadTimeAtSkip)); either would make the row claim \
            the listener was inside the window when they answered, which is \
            the reading playhead-bwxi's column exists to make impossible.
            """
        )
        #expect(
            !(recorded >= Self.preRollStart && recorded < Self.preRollEnd),
            """
            a correction made from the passive list landed INSIDE the window's \
            own span. That is the shape of the three poisoned rows of \
            2026-08-21 and it must not be manufactured by a surface whose whole \
            premise is that the listener has moved on.
            """
        )
    }

    /// The mirror, and the reason the confirm seam is deliberately NOT offered
    /// from this surface: `bannerAutoSkipConfirmed` keeps its bwxi invariant.
    /// A confirmation exists only where a card does, i.e. inside the span.
    @Test("A missed skip writes no confirmation — only a card can confirm")
    func aMissedSkipNeverProducesAConfirmation() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let window = Self.autoWindow(
            id: "preroll", start: Self.preRollStart, end: Self.preRollEnd
        )
        let receipt = try await Self.makeOneMissedReceipt(
            orchestrator, store, window: window, observeAt: 40
        )
        await orchestrator.updatePlayheadTime(2000)
        _ = await orchestrator.denyAutoSkippedBanner(
            windowId: receipt.item.windowId,
            analysisAssetId: receipt.item.analysisAssetId,
            startTime: receipt.item.adStartTime,
            endTime: receipt.item.adEndTime,
            podcastId: receipt.item.podcastId,
            ifCurrentEpisodeId: receipt.item.episodeId,
            ifPlaybackLifecycleGeneration:
                receipt.item.playbackLifecycleGeneration,
            ifWindowMaterialRevisionToken:
                receipt.item.windowMaterialRevisionToken
        )
        let events = try await store.loadCorrectionEvents(
            analysisAssetId: Self.assetId
        )
        #expect(
            !events.contains { $0.source == .bannerAutoSkipConfirmed },
            """
            a `bannerAutoSkipConfirmed` row exists for a skip the listener \
            never saw a card for. That is the strongest positive signal the \
            trust system takes, recorded for audio they did not hear — the \
            2026-08-21 incident, arriving through a new door.
            """
        )
    }

    // MARK: - 3. The list is per-episode

    /// A missed receipt must not outlive the episode that produced it. The
    /// live-state filter would hide it anyway, which is exactly why the clear
    /// has to be explicit: a leak nobody can see is still a leak, and window
    /// ids are not unique across episodes.
    @Test("Missed receipts do not survive an episode boundary")
    func missedReceiptsAreClearedAtTheEpisodeBoundary() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let window = Self.autoWindow(
            id: "preroll", start: Self.preRollStart, end: Self.preRollEnd
        )
        _ = try await Self.makeOneMissedReceipt(
            orchestrator, store, window: window, observeAt: 40
        )

        await orchestrator.endEpisode()
        #expect(
            await orchestrator.missedAutoSkipReceipts().isEmpty,
            "a missed receipt survived endEpisode"
        )

        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration + 1
        )
        #expect(
            await orchestrator.missedAutoSkipReceipts().isEmpty,
            """
            a missed receipt from the previous playback transaction is visible \
            in the new one. Its veto carries the OLD lifecycle generation, so \
            it can only be refused.
            """
        )
    }

    // MARK: - 4. Order, and the copy

    /// Episode order, because a list of things that happened reads in the order
    /// they happened. Ties broken by id so the order is total.
    @Test("The list is in episode order")
    func theListIsInEpisodeOrder() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let spans: [(String, Double, Double)] = [
            ("postroll", 4279.302, 4309.420),
            ("preroll", 0.0, 86.831),
            ("midroll", 1369.809, 1548.487),
        ]
        for (id, start, end) in spans {
            let window = Self.autoWindow(id: id, start: start, end: end)
            try await store.insertAdWindow(window)
            await orchestrator.receiveAdWindows([window])
        }
        for (_, start, _) in spans {
            await orchestrator.updatePlayheadTime(start)
        }
        let listed = await orchestrator.missedAutoSkipReceipts()
        #expect(
            listed.map(\.windowId) == ["preroll", "midroll", "postroll"],
            "got \(listed.map(\.windowId))"
        )
    }

    /// The copy, which is subject to `feedback_peace_of_mind_not_metrics` — no
    /// quantified language, nothing that reads as an error log — and which
    /// nothing else in the tree can assert.
    @Test("A row names the sponsor when one is known, and never a quantity")
    func rowCopyNamesTheSponsorAndNeverAQuantity() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let named = try await Self.makeOneMissedReceipt(
            orchestrator,
            store,
            window: Self.autoWindow(
                id: "preroll",
                start: Self.preRollStart,
                end: Self.preRollEnd,
                advertiser: "Acme Insurance"
            ),
            observeAt: 40
        )
        #expect(named.displayTitle == "Acme Insurance")
        #expect(named.spanLabel == "Skipped 0:00–1:26")

        let (bare, bareStore) = try await Self.makeHarness()
        let anonymous = try await Self.makeOneMissedReceipt(
            bare,
            bareStore,
            window: Self.autoWindow(
                id: "midroll", start: 1369.809, end: 1548.487
            ),
            observeAt: 1400
        )
        #expect(
            anonymous.displayTitle == "Sponsor segment",
            """
            a row with no advertiser metadata reads \
            "\(anonymous.displayTitle)". It must be a neutral phrase — never a \
            window id, never a confidence, never "detected".
            """
        )
        #expect(anonymous.spanLabel == "Skipped 22:49–25:48")
    }

    // MARK: - 5. The card's own seam, handed a list row

    /// The production hop, exercised rather than grepped.
    ///
    /// `BannerFeedbackProductionActions.onNotAnAd` is the closure
    /// `AdBannerView`'s No calls; `NowPlayingView` hands the transcript sheet
    /// THAT closure for a list row. So the question this test asks is the one
    /// that can actually go wrong: does a `MissedAutoSkipReceipt` carry
    /// everything that closure forwards, or does the hop drop a field?
    @MainActor
    @Test("A list row driven through the card's own action closure forwards every identity field")
    func aListRowThroughTheCardsActionClosureForwardsEveryField() async throws {
        nonisolated(unsafe) var recorded:
            (
                windowId: String,
                podcastId: String,
                assetId: String?,
                start: Double,
                end: Double,
                episodeId: String?,
                generation: UInt64?,
                token: String?
            )?
        let actions = BannerFeedbackProductionActions(
            revertWindow: { windowId, podcastId, assetId, start, end,
                            episodeId, generation, token in
                recorded = (
                    windowId, podcastId, assetId, start, end,
                    episodeId, generation, token
                )
                return true
            },
            acceptSuggestedSkip: { _, _, _, _ in false },
            declineSuggestedSkip: { _, _, _, _, _ in false }
        )

        let item = AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: "preroll",
            advertiser: "Acme Insurance",
            product: nil,
            adStartTime: Self.preRollStart,
            adEndTime: Self.preRollEnd,
            metadataConfidence: 0.9,
            metadataSource: "foundationModels",
            podcastId: Self.podcastId,
            episodeId: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration,
            analysisAssetId: Self.assetId,
            windowMaterialRevisionToken: "token-abc",
            evidenceCatalogEntries: [],
            tier: .autoSkipped
        )
        let receipt = MissedAutoSkipReceipt(
            item: item,
            playheadTimeAtSkip: 40,
            occurredAt: Date()
        )

        #expect(await actions.onNotAnAd(receipt.item))
        let sent = try #require(recorded)
        #expect(sent.windowId == "preroll")
        #expect(sent.podcastId == Self.podcastId)
        #expect(sent.assetId == Self.assetId)
        #expect(sent.start == Self.preRollStart)
        #expect(sent.end == Self.preRollEnd)
        #expect(sent.episodeId == Self.episodeId)
        #expect(sent.generation == Self.playbackLifecycleGeneration)
        #expect(
            sent.token == "token-abc",
            """
            the material revision token did not survive the hop. Without it \
            `denyAutoSkippedBanner` refuses every list veto, which is the \
            silent way this feature becomes decorative.
            """
        )
    }
}
