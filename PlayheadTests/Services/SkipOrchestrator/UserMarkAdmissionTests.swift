import CoreMedia
import Foundation
import Testing
@testable import Playhead

/// playhead-d2it — a listener's in-session "Hearing an ad" goes through the
/// same admission door the reload path takes, so it behaves the same way NOW
/// as it did after a relaunch.
///
/// The reload-side claim is pinned by
/// `UserAddedMarkSurvivesBackfillTests.userMarkDoesNotAutoSkipInManualMode`
/// (playhead-wq34): a definitive user mark on a `.manual` show is INGESTED as
/// a suggest-tier card, never silently `.confirmed`. This suite asserts the
/// live path lands in the same place, and that the two paths agree.
@Suite("playhead-d2it: a live user mark takes the door the reload takes")
struct UserMarkAdmissionTests {

    private static let assetId = "asset-d2it"
    private static let episodeId = "ep-d2it"
    private static let podcastId = "podcast-d2it"

    private static func makeOrchestrator(
        mode: SkipMode
    ) async throws -> (SkipOrchestrator, AnalysisStore, TrustScoringService) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let trust = try await makeSkipTestTrustService(
            mode: mode.rawValue,
            trustScore: mode == .auto ? 0.9 : 0.5,
            observations: mode == .auto ? 10 : 0
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trust,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId
        )
        return (orchestrator, store, trust)
    }

    @Test("on a manual show, a live mark becomes a CARD, not a silent confirmed window",
          .timeLimit(.minutes(1)))
    func liveMarkOnManualShowIsOfferedAsACard() async throws {
        let (orchestrator, _, _) = try await Self.makeOrchestrator(mode: .manual)

        await orchestrator.injectUserMarkedAd(
            start: 100, end: 160,
            analysisAssetId: Self.assetId,
            windowId: "live-mark"
        )

        #expect(
            await orchestrator.activeSuggestWindowIDs().contains("live-mark"),
            """
            the listener said "this is an ad" and the app agreed; on a show \
            whose mode is not auto that agreement must reach them as a card \
            they can act on, not vanish into the managed tier's silence
            """
        )
        #expect(
            !(await orchestrator.activeWindowIDs().contains("live-mark")),
            "a silent .confirmed managed window is the defect, not a fallback"
        )
        let applied = await orchestrator.getDecisionLog().filter {
            $0.adWindowId == "live-mark" && $0.decision == .applied
        }
        #expect(applied.isEmpty, "manual mode never auto-skips, even a user's own mark")
    }

    /// The first draft of this control assumed an auto SHOW puts a user mark
    /// in the managed tier. It does not, and finding that out is the point of
    /// the parity test below: a fresh `.eligible` user mark classifies as
    /// `.userAsserted`, which is show-governed and never `.auto` without an
    /// EXPLICIT override (playhead-lqcp; the wq34 comment on the reload test).
    /// So without an override the mark is a card on every show — live and on
    /// reload alike — and only an override reaches the managed tier.
    @Test("on an auto show WITHOUT an override, a live mark is a card — the same as reload",
          .timeLimit(.minutes(1)))
    func liveMarkOnAutoShowWithoutOverrideIsACard() async throws {
        let (orchestrator, _, _) = try await Self.makeOrchestrator(mode: .auto)

        await orchestrator.injectUserMarkedAd(
            start: 100, end: 160,
            analysisAssetId: Self.assetId,
            windowId: "live-mark"
        )

        #expect(await orchestrator.activeSuggestWindowIDs().contains("live-mark"))
        #expect(!(await orchestrator.activeWindowIDs().contains("live-mark")))
    }

    @Test("with an explicit auto override, a live mark lands in the managed tier",
          .timeLimit(.minutes(1)))
    func liveMarkWithAutoOverrideIsManaged() async throws {
        let (orchestrator, _, trust) = try await Self.makeOrchestrator(mode: .auto)
        await trust.setUserOverride(podcastId: Self.podcastId, mode: .auto)
        // The override is read at admission, and the orchestrator resolved the
        // show's mode at beginEpisode; begin again so the override is in force.
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )

        await orchestrator.injectUserMarkedAd(
            start: 100, end: 160,
            analysisAssetId: Self.assetId,
            windowId: "live-mark"
        )

        #expect(
            await orchestrator.activeWindowIDs().contains("live-mark"),
            "an explicit override makes the user-asserted class auto; the mark is managed"
        )
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("live-mark")))
        // The door admits the row as `.confirmed` (its durable state) and
        // `evaluateAndPush` — which runs INSIDE `receiveAdWindows` — decides
        // it on the spot for an auto class: `.applied`, cue pushed. The first
        // draft asserted `.confirmed` here and failed; that was the wrong
        // claim, not a wrong door. The direct insert used to reach the same
        // `.applied` through its own `evaluateAndPush()` call.
        #expect(
            await orchestrator._managedDecisionStateForTesting(id: "live-mark") == .applied,
            "an auto-class mark is DECIDED at admission, not merely admitted"
        )
    }

    @Test("the live path and the reload path put the same mark in the same tier",
          .timeLimit(.minutes(1)))
    func livePathAgreesWithReloadPath() async throws {
        for mode in [SkipMode.manual, .shadow, .auto] {
            // LIVE: inject in-session.
            let (live, _, _) = try await Self.makeOrchestrator(mode: mode)
            await live.injectUserMarkedAd(
                start: 100, end: 160,
                analysisAssetId: Self.assetId,
                windowId: "the-mark"
            )
            let liveManaged = await live.activeWindowIDs().contains("the-mark")
            let liveSuggest = await live.activeSuggestWindowIDs().contains("the-mark")

            // RELOAD: the same durable row, preloaded by beginEpisode.
            let store = try await makeTestStore()
            try await store.insertAsset(
                makeSkipTestAnalysisAsset(id: Self.assetId, episodeId: Self.episodeId)
            )
            let service = AdDetectionService(store: store, metadataExtractor: FallbackExtractor())
            _ = await service.recordUserMarkedAd(
                analysisAssetId: Self.assetId, startTime: 100, endTime: 160,
                podcastId: Self.podcastId, windowId: "the-mark"
            )
            let trust = try await makeSkipTestTrustService(
                mode: mode.rawValue,
                trustScore: mode == .auto ? 0.9 : 0.5,
                observations: mode == .auto ? 10 : 0
            )
            let reload = SkipOrchestrator(
                store: store, trustService: trust,
                correctionStore: PersistentUserCorrectionStore(store: store)
            )
            await reload.setSkipCueHandler { _ in }
            await reload.beginEpisode(
                analysisAssetId: Self.assetId, episodeId: Self.episodeId, podcastId: Self.podcastId
            )
            let reloadManaged = await reload.activeWindowIDs().contains("the-mark")
            let reloadSuggest = await reload.activeSuggestWindowIDs().contains("the-mark")

            #expect(
                liveManaged == reloadManaged && liveSuggest == reloadSuggest,
                """
                \(mode): live (managed=\(liveManaged), suggest=\(liveSuggest)) \
                vs reload (managed=\(reloadManaged), suggest=\(reloadSuggest)). \
                "Works only after you quit the app" is exactly this disagreement.
                """
            )
            #expect(
                liveManaged || liveSuggest,
                "\(mode): the mark must be ingested somewhere, or the agreement above is vacuous"
            )
        }
    }
}
