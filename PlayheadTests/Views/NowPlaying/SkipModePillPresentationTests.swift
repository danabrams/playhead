// SkipModePillPresentationTests.swift
// playhead-djl0, the surfacing half.
//
// The Now Playing skip-mode pill read `SkipMode` and nothing else, so the two
// facts `SkipModeResolution` now separates collapsed again at the last step:
// a session that had LOST the show's identity rendered the same "Shadow" pill
// as a show deliberately being observed. Worse, the pill was rendered only
// `if !viewModel.podcastTitle.isEmpty` — and the title comes from the SAME
// `episode.podcast?` relationship the identifier does, so the one session that
// most needed to say something rendered nothing at all.
//
// The standing check applies here too: what would the pill read if the lookup
// had succeeded and the show genuinely sat in shadow? These tests exist so the
// answer can never be "the same".

import Foundation
import Testing

@testable import Playhead

@Suite("The skip-mode pill distinguishes a lost show from a quiet one (playhead-djl0)")
struct SkipModePillPresentationTests {

    /// THE standing check at the surface layer.
    @Test("a deliberate shadow and an unresolved identity do not render the same pill")
    func unresolvedIdentityDoesNotLookLikeShadow() {
        let deliberate = SkipModePillPresentation(
            mode: .shadow, resolution: .showTrustProfile
        )
        let lost = SkipModePillPresentation(
            mode: .shadow, resolution: .unresolvedShowIdentity
        )
        #expect(deliberate.label != lost.label)
        #expect(deliberate.accessibilityLabel != lost.accessibilityLabel)
    }

    @Test("a resolved show keeps its existing pill text exactly",
          arguments: zip(
              [SkipMode.shadow, .manual, .auto],
              ["Shadow", "Manual", "Auto"]
          ))
    func resolvedShowsKeepTheirLabels(mode: SkipMode, expected: String) {
        let presentation = SkipModePillPresentation(
            mode: mode, resolution: .showTrustProfile
        )
        #expect(presentation.label == expected)
        #expect(presentation.isModeSelectable,
                "a known show can still take a per-show preference")
    }

    /// A new show defaults to shadow BY DESIGN. It must keep reading "Shadow"
    /// — renaming the deliberate case would just move the confusion.
    @Test("a new show still reads Shadow")
    func aNewShowReadsShadow() {
        let presentation = SkipModePillPresentation(
            mode: .shadow, resolution: .newShowDefault
        )
        #expect(presentation.label == "Shadow")
        #expect(presentation.isModeSelectable)
    }

    /// `PlayheadRuntime.setShowSkipMode` writes the user's choice through
    /// `trustService.setUserOverride(podcastId:mode:)` and silently skips the
    /// write when `currentPodcastId` is nil. Offering a menu whose selection
    /// cannot be stored is the same defect one layer up, so the control is
    /// withheld rather than lying.
    @Test("an unresolved identity withholds the per-show control")
    func anUnresolvedIdentityIsNotSelectable() {
        let presentation = SkipModePillPresentation(
            mode: .shadow, resolution: .unresolvedShowIdentity
        )
        #expect(!presentation.isModeSelectable)
    }

    /// A profile that failed to READ still belongs to a known show, so the
    /// preference can still be stored and the control stays live.
    @Test("a trust-lookup failure keeps the control — the show is still known")
    func aTrustLookupFailureKeepsTheControl() {
        for resolution in [
            SkipModeResolution.trustProfileUnreadable,
            .unrecognizedTrustProfileMode,
            .trustServiceUnavailable,
        ] {
            let presentation = SkipModePillPresentation(
                mode: .shadow, resolution: resolution
            )
            #expect(presentation.isModeSelectable,
                    "\(resolution.rawValue) still has a show to attach a choice to")
        }
    }

    /// The visibility rule. The pill previously hid whenever the podcast title
    /// was empty — which is exactly the case an unresolved identity produces,
    /// because the title and the identifier come from the same relationship.
    @Test("the pill is shown for an unresolved identity even with no podcast title")
    func theUnresolvedPillIsVisibleWithoutATitle() {
        #expect(SkipModePillPresentation.isVisible(
            podcastTitle: "", resolution: .unresolvedShowIdentity
        ))
    }

    @Test("the pill stays hidden when there is simply nothing playing")
    func thePillIsHiddenWithNoEpisode() {
        #expect(!SkipModePillPresentation.isVisible(
            podcastTitle: "", resolution: .noActiveEpisode
        ))
        #expect(!SkipModePillPresentation.isVisible(
            podcastTitle: "  ", resolution: .noActiveEpisode
        ))
    }

    @Test("a titled show still shows its pill, as before")
    func aTitledShowStillShowsThePill() {
        #expect(SkipModePillPresentation.isVisible(
            podcastTitle: "Conan O'Brien Needs a Friend", resolution: .showTrustProfile
        ))
    }

    /// The pill's copy is user-facing. It must say what happened in the
    /// listener's terms, without naming internals.
    @Test("the unresolved copy names the show, not the machinery")
    func theUnresolvedCopyIsUserFacing() {
        let presentation = SkipModePillPresentation(
            mode: .shadow, resolution: .unresolvedShowIdentity
        )
        let text = presentation.label + " " + presentation.accessibilityLabel
        for forbidden in ["podcastId", "nil", "shadow", "trust", "orchestrator"] {
            #expect(!text.lowercased().contains(forbidden.lowercased()),
                    "user-facing copy leaked \(forbidden)")
        }
    }
}

@Suite("NowPlayingViewModel carries the resolution to the pill (playhead-djl0)")
@MainActor
struct NowPlayingViewModelSkipModeResolutionTests {

    /// The view model starts before any episode does.
    @Test("the view model defaults to no active episode")
    func defaultsToNoActiveEpisode() {
        let viewModel = NowPlayingViewModel(
            runtime: PlayheadRuntime(isPreviewRuntime: true)
        )
        #expect(viewModel.skipModeResolution == .noActiveEpisode)
    }

    /// The user's own selection is not the orchestrator's lookup failure, and
    /// the pill must stop saying so the moment they choose.
    @Test("setting a mode marks the resolution as a session override")
    func settingAModeMarksASessionOverride() {
        let viewModel = NowPlayingViewModel(
            runtime: PlayheadRuntime(isPreviewRuntime: true)
        )
        viewModel.noteSkipModeSelection(.manual)
        #expect(viewModel.activeSkipMode == .manual)
        #expect(viewModel.skipModeResolution == .sessionOverride)
    }

    /// The load hop is where the orchestrator's cause actually reaches the
    /// pill. Carrying only the MODE across it would restore the whole defect at
    /// the last step, with every layer below it correct.
    @Test("loadSkipMode carries the CAUSE across, not just the mode")
    func loadSkipModeCarriesTheCause() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: try await makeSkipTestTrustService(
                mode: "auto", trustScore: 0.9, observations: 10
            )
        )
        let viewModel = NowPlayingViewModel(
            runtime: PlayheadRuntime(isPreviewRuntime: true)
        )

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "ep-1", podcastId: nil
        )
        await viewModel.loadSkipMode(from: orchestrator)
        #expect(viewModel.activeSkipMode == .shadow)
        #expect(viewModel.skipModeResolution == .unresolvedShowIdentity)

        // ...and the resolved case, so the assertion above is not simply the
        // view model's own default leaking through.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "ep-1", podcastId: "podcast-1"
        )
        await viewModel.loadSkipMode(from: orchestrator)
        #expect(viewModel.activeSkipMode == .auto)
        #expect(viewModel.skipModeResolution == .showTrustProfile)
    }
}
