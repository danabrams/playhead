// MissedAutoSkipListWiringSourceCanaryTests.swift
// playhead-2d6i: THE HOP NO UNIT TEST CAN SEE.
//
// `MissedAutoSkipReceiptListTests` proves the orchestrator records the receipt
// and that `BannerFeedbackProductionActions.onNotAnAd` forwards every field of
// one. Neither can see the two lines in between, and both of them are places
// this feature goes quiet rather than wrong:
//
//   1. `NowPlayingView` must hand the transcript sheet a PROVIDER. Without it
//      `missedAutoSkipReceipts` is nil, `refreshMissedAutoSkips` sets an empty
//      list, and the section never renders — a perfectly green orchestrator
//      whose output nothing reads. That is the shape of playhead-o89d's
//      six-week gate ("gated and deleted were the same thing") and of the
//      sceneless-launch class in project memory: a capability installed
//      nowhere, whose consumer's `guard let` drops the work silently.
//
//   2. The veto must be `bannerFeedbackActions.onNotAnAd` — the closure the
//      CARD's No calls — and not a second `denyAutoSkippedBanner` call spelled
//      out again. Two call sites that merely agree today is how a surface comes
//      to promise a correction the transaction refuses; one closure cannot
//      drift from itself.
//
//   3. playhead-nq8z: and it must hand that closure the RIGHT SURFACE at BOTH
//      call sites. One closure with a discriminating argument keeps property 2
//      intact, but moves the thing that can go wrong into the argument: a
//      binding that forwards the item and the wrong surface compiles, passes
//      every orchestrator test, and writes a row whose `source` names the
//      other screen. Both directions are pinned below, because a wiring that
//      passed `.missedAutoSkipList` from both surfaces would satisfy either
//      one alone.
//
// AND THE ABSENCE, which is a claim as much as the presence is: the list must
// NOT reach a confirm seam. A listener who never saw a card never heard the ad
// — it was skipped — so a Yes from here would write `bannerAutoSkipConfirmed`,
// the strongest positive signal the trust system takes, for audio they never
// reached. That is the 2026-08-21 field incident verbatim, and a later
// contributor adding "Yes" for symmetry would re-create it through a new door.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-2d6i — the missed-skip list is wired to the CARD's veto")
struct MissedAutoSkipListWiringSourceCanaryTests {

    /// `PlayheadTests/Services/SkipOrchestrator/<this file>` → repo root.
    private static func source(
        _ relative: String,
        filePath: String = #filePath
    ) throws -> String {
        let root = URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()   // SkipOrchestrator
            .deletingLastPathComponent()   // Services
            .deletingLastPathComponent()   // PlayheadTests
            .deletingLastPathComponent()   // repo root
        return try String(
            contentsOf: root.appendingPathComponent(relative),
            encoding: .utf8
        )
    }

    private static let nowPlaying =
        "Playhead/Views/NowPlaying/NowPlayingView.swift"
    private static let transcriptPeek =
        "Playhead/Views/NowPlaying/TranscriptPeekView.swift"

    @Test("NowPlayingView gives the transcript sheet a receipt provider")
    func nowPlayingSuppliesTheProvider() throws {
        let text = try Self.source(Self.nowPlaying)
        #expect(
            text.contains("missedAutoSkipReceipts:"),
            """
            the transcript sheet is constructed without a \
            `missedAutoSkipReceipts:` provider. The parameter is optional so \
            previews and tests stay unchanged, which means omitting it in \
            production compiles, passes every test, and renders nothing.
            """
        )
        #expect(
            text.contains(".missedAutoSkipReceipts()"),
            """
            the provider does not call the orchestrator. A provider that \
            returns a constant is a section that can only ever be empty.
            """
        )
    }

    /// AND IT FILTERS AGAINST THE CAPTURED CONTEXT, which the merge gate had to
    /// teach this bead.
    ///
    /// The first version of the provider guarded on `runtime.currentEpisodeId`
    /// and `ViewLayerCorrectionAttributionCaptureCanaryTests` failed it — a
    /// content closure that is recomputed while its sheet stays mounted must
    /// not read the live runtime (playhead-254m). That canary owns the ABSENCE
    /// of the live read and will keep owning it. What it cannot see is whether
    /// anything took the live read's PLACE: delete the filter entirely and it
    /// stays green, while the sheet starts listing rows belonging to whatever
    /// episode the orchestrator moved on to.
    ///
    /// The filter is also the correct guard on its own terms, which the live
    /// read never was: sampling the runtime BEFORE the `await` says nothing
    /// about which episode's rows come back AFTER it. Each row carries the
    /// episode and playback generation stamped when its skip fired.
    @Test("the provider filters rows against the sheet's own captured transaction")
    func theProviderFiltersAgainstTheCapturedContext() throws {
        let text = try Self.source(Self.nowPlaying)
        for required in [
            "$0.item.episodeId == sourceContext.episodeId",
            "== sourceContext.playbackLifecycleGeneration",
        ] {
            #expect(
                text.contains(required),
                """
                the missed-skip provider does not check `\(required)`. A sheet \
                belongs to ONE playback transaction; without this it lists rows \
                from whichever episode the orchestrator is serving when the \
                closure resumes, and offers a veto that names the wrong audio.
                """
            )
        }
    }

    @Test("the list's veto IS the card's veto closure, not a second seam")
    func theListVetoIsTheCardsClosure() throws {
        let text = try Self.source(Self.nowPlaying)
        #expect(
            text.contains("onMissedAutoSkipNotAnAd:"),
            "the transcript sheet is constructed without a veto callback"
        )
        #expect(
            text.contains("bannerFeedbackActions.onNotAnAd("),
            """
            the list's veto does not route through \
            `bannerFeedbackActions.onNotAnAd` — the exact closure \
            `AdBannerView`'s No calls. If it now spells out its own \
            `denyAutoSkippedBanner(...)` call, the two paths can drift, which \
            is the whole reason a `MissedAutoSkipReceipt` carries the banner \
            item verbatim rather than a summary.
            """
        )
        #expect(
            text.contains("receipt.item, .missedAutoSkipList"),
            """
            the list hands `onNotAnAd` something other than \
            `(receipt.item, .missedAutoSkipList)`. The item is what makes the \
            veto reach the same transaction a card's does; the surface is what \
            makes the resulting row SAY it came from the list \
            (playhead-nq8z). Forwarding the item without the surface still \
            commits a receipt — spelled as a card's — which is the ambiguity \
            that bead removes.
            """
        )
    }

    /// THE MIRROR, and it is a separate test because the two claims fail
    /// differently (playhead-nq8z).
    ///
    /// The test above pins the LIST's binding. A wiring that passed
    /// `.missedAutoSkipList` from BOTH surfaces would satisfy it completely and
    /// would relabel every card's No as a list veto — inverting the bead rather
    /// than fixing it. `AdBannerQueueTests` drives the card's closure and reads
    /// `.card` back, but it constructs its own `BannerFeedbackProductionActions`
    /// and so cannot see production's binding at all; this is the only thing
    /// that looks at the line `NowPlayingView` actually ships.
    @Test("the CARD's binding passes .card — the two surfaces are not one")
    func theCardsBindingNamesTheCardSurface() throws {
        let text = try Self.source(Self.nowPlaying)
        #expect(
            text.contains("bannerFeedbackActions.onNotAnAd(item, .card)"),
            """
            the auto-skip card's `onNotAnAdAsync` binding does not pass \
            `.card`. Both surfaces share one closure precisely so they cannot \
            drift, which means the ONE argument that distinguishes them has to \
            be right at both call sites; a card wired to the list's surface \
            writes `missedAutoSkipListDenied` for a tap made inside the span.
            """
        )
    }

    @Test("the list offers NO confirmation — a skip nobody saw cannot be confirmed")
    func theListOffersNoConfirmation() throws {
        let peek = try Self.source(Self.transcriptPeek)
        for forbidden in [
            "confirmAutoSkippedBanner",
            "onAutoSkipConfirmed",
            "bannerAutoSkipConfirmed",
        ] {
            #expect(
                !peek.contains(forbidden),
                """
                the transcript surface reaches `\(forbidden)`. A Yes from the \
                passive list records a confirmation for audio the listener \
                never heard — it was skipped without a card — which is exactly \
                the 2026-08-21 incident playhead-bwxi's V59 column exists to \
                expose. The veto is the only honest answer from here.
                """
            )
        }
    }

    @Test("the section renders only when there is something to show")
    func theSectionIsAbsentWhenTheListIsEmpty() throws {
        let peek = try Self.source(Self.transcriptPeek)
        #expect(
            peek.contains("if !missedAutoSkips.isEmpty {"),
            """
            the missed-skip section is not gated on having rows. An empty \
            header pinned to the bottom of the transcript sheet on every \
            episode is chrome, and the ordinary case is that nothing was \
            missed at all.
            """
        )
    }
}
