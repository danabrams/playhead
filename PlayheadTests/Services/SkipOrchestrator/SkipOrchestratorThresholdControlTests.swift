// SkipOrchestratorThresholdControlTests.swift
// playhead-xsdz.11: WRITE-PATH wiring for the per-show auto-skip threshold
// controller. These prove the orchestrator feeds the controller store the
// CORRECT signal at the CORRECT seam:
//   • a Listen revert of a managed auto-skip window  → FALSE-POSITIVE (raise)
//   • a manual "not an ad" revert (revertWindow)      → FALSE-POSITIVE (raise)
//   • denying an auto-skipped banner ("No")           → FALSE-POSITIVE (raise)
//   • accepting a suggested skip we did not auto-skip → MISS (lower)
// and that with NO store wired (the flag-OFF production default) those seams
// still run to completion. The complementary "records NOTHING" contracts —
// which need a wired store to be observable at all — are pinned in the
// negative-coverage section further down.
//
// playhead-i08e: three of those seams (revertWindow, denyAutoSkippedBanner,
// acceptSuggestedSkip) went dead when the explicit-feedback refactor made
// minting the correction receipt throw unless a UserCorrectionStore happened
// to be wired — the throw aborted the seam before its controller write. The
// coverage below now pins each seam in BOTH the minimally-wired configuration
// (no correction store) and the production configuration (correction store
// wired), and asserts an exact sample count so a double write that cancels to
// integral 0 cannot pass either.

import CoreMedia
import Foundation
import Testing
@testable import Playhead

/// Bounded read of the first `.present` banner item. `denyAutoSkippedBanner`
/// validates the action against the exact material the card displayed, so the
/// test has to answer with the orchestrator's own emission rather than a
/// locally reconstructed token. Returns nil instead of parking the suite
/// forever when no presentation arrives.
private func firstPresentedBannerItem(
    _ stream: AsyncStream<AdBannerStreamEvent>
) async -> AdSkipBannerItem? {
    await withTaskGroup(of: AdSkipBannerItem?.self) { group in
        group.addTask {
            for await event in stream {
                if case let .present(item) = event { return item }
            }
            return nil
        }
        group.addTask {
            try? await Task.sleep(for: .seconds(5))
            return nil
        }
        let first = await group.next() ?? nil
        group.cancelAll()
        return first
    }
}

@Suite("SkipOrchestrator per-show threshold control write path (playhead-xsdz.11)")
struct SkipOrchestratorThresholdControlTests {

    private let podcastId = "podcast-1"
    /// The episode the `makeSkipTestAnalysisAsset()` fixture belongs to.
    /// `beginEpisode` must be told the SAME episode id the asset row carries:
    /// the durable explicit-feedback transactions
    /// (`persistDeniedAutoSkip`, `persistAcceptedSuggestionIfCurrent`) refuse
    /// to write when the acting card's episode does not own the asset, which
    /// is exactly what production guarantees by passing the real episode id.
    private let episodeId = "ep-1"

    /// playhead-i08e (eighth pass): this deliberately wires NO trust service.
    ///
    /// The bead's root cause was an early exit keyed on an optional DEPENDENCY
    /// rather than on the lifecycle (`guard correctionStore != nil else
    /// { throw }` above a seam's effects), so that shape deserves a rail at
    /// every seam it killed. `revertByTimeRange` has one —
    /// `anonymousTimeRangeRevertSurvivesEpisodeReplacement` wires neither a
    /// trust service nor show attribution, so an early exit on either drops the
    /// receipt it asserts. `recordListenRevert` had none: every test that
    /// exercised it wired a trust service, so reintroducing the shape there
    /// (`guard trustService != nil else { return }` above the calibration
    /// effects) left the ENTIRE repo-wide gate green. Verified by mutation
    /// before and after: that edit now reddens this test and nothing else.
    ///
    /// The trust service is the only thing dropped, and dropping it costs no
    /// coverage: `noControllerStoreLeavesRevertSeamsIntact` below still drives
    /// this seam with one wired, and `listenRevertSurvivesEpisodeReplacement`
    /// (SkipOrchestratorRevertTests) pins the same sample count and sign in the
    /// trust-wired shape with the trust hop actually suspending.
    @Test("Listen revert of a managed auto-skip window records a FALSE-POSITIVE signal (integral +1)")
    func listenRevertRecordsFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-fp", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(
            await orchestrator.recordListenRevert(
                windowId: "ad-fp",
                podcastId: podcastId
            )
        )

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one revert must record exactly one controller sample")
        #expect(state.integral == 1, "a Listen revert is a FALSE-POSITIVE signal → integral +1")
        // Positive control: the gesture ran to completion, so "recorded one
        // sample" cannot be satisfied by a seam that aborted somewhere else.
        let row = try #require(try await store.fetchAdWindow(id: "ad-fp"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)
        await controllerStore.close()
    }

    @Test("Manual 'not an ad' revertWindow records a FALSE-POSITIVE signal")
    func revertWindowRecordsFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-veto", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(await orchestrator.revertWindow(windowId: "ad-veto", podcastId: podcastId))

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one veto must record exactly one controller sample")
        #expect(state.integral == 1, "a manual veto of a managed window is a FALSE-POSITIVE signal → integral +1")

        // playhead-i08e root-cause rail: the seam went dead because minting the
        // receipt threw when no UserCorrectionStore was wired. Assert the
        // RECEIPT itself, not just its downstream controller sample — otherwise
        // simply hoisting the controller write above the persistence block
        // would satisfy this suite while the user's correction stayed dropped.
        // The receipt's durability is owned by the AnalysisStore transaction,
        // so it must land regardless of the optional learning listener.
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(receipts.count == 1, "the veto must commit exactly one durable receipt")
        #expect(receipts.first?.source == .manualVeto)
        await controllerStore.close()
    }

    /// playhead-i08e: the same seam in the PRODUCTION wiring. A correction
    /// store is always injected by `PlayheadRuntime`, so this is the shape the
    /// app actually runs.
    ///
    /// What this adds over its unwired sibling is ONLY the wired listener:
    /// whether `schedulePostCommitCorrectionLearning` — which no-ops without a
    /// store — induces a second controller write once it is live. (The sibling
    /// is the regression rail: restoring the `correctionStore != nil`
    /// precondition reddens it and leaves this one green.) Deleting the seam's
    /// controller write kills both, so this is not independent coverage of the
    /// write itself.
    ///
    /// playhead-i08e (ninth pass) — correcting the seventh pass, which claimed
    /// the learning INGESTOR is what gives this test its discriminating power
    /// and that without one it "had none at all". Mutation says otherwise: with
    /// the ingestor wiring removed and a second controller write injected into
    /// `schedulePostCommitCorrectionLearning`, this test and its `…Miss` twin
    /// still redden while both unwired siblings stay green. The power comes
    /// from that method's own `guard let correctionStore`, which gates the hop
    /// before `PersistentUserCorrectionStore.correctionDidPersistAtomically`
    /// (and its `guard let learningIngestor`) is ever reached. The ingestor
    /// stays wired for the narrower, honest reason: it is the shape
    /// `PlayheadRuntime` runs, so the hop executes real derived-learning work
    /// here instead of returning at its first statement.
    @Test("Manual 'not an ad' revertWindow records exactly one FALSE-POSITIVE with the correction store wired")
    func revertWindowWithCorrectionStoreRecordsOneFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let correctionStore = PersistentUserCorrectionStore(store: store)
        await correctionStore.setLearningArtifactIngestor(
            LearningArtifactIngestor(
                store: store,
                knowledgeStore: SponsorKnowledgeStore(store: store)
            )
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-veto-wired", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(await orchestrator.revertWindow(windowId: "ad-veto-wired", podcastId: podcastId))

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one veto must record exactly one controller sample")
        #expect(state.integral == 1, "a manual veto of a managed window is a FALSE-POSITIVE signal → integral +1")
        await controllerStore.close()
    }

    @Test("revertByTimeRange of a managed auto-skip window records a FALSE-POSITIVE signal")
    func revertByTimeRangeManagedRecordsFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-tr", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(
            await orchestrator.revertByTimeRange(
                start: 70,
                end: 110,
                podcastId: podcastId
            )
        )

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one time-range revert must record exactly one controller sample")
        #expect(state.integral == 1, "a managed-window time-range revert is a FALSE-POSITIVE signal → integral +1")
        await controllerStore.close()
    }

    /// playhead-i08e: the explicit banner "No" on an applied auto-skip is the
    /// third seam that routes through the atomic correction commit, so it
    /// carries the same dead-write-path risk as `revertWindow`.
    @Test("Denying an auto-skipped banner records a FALSE-POSITIVE signal")
    func denyAutoSkippedBannerRecordsFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: episodeId,
            podcastId: podcastId,
            playbackLifecycleGeneration: 7
        )
        let events = await orchestrator.bannerEventStream()

        let ad = makeSkipTestAdWindow(id: "ad-deny", startTime: 60, endTime: 120, confidence: 0.9, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        let item = try #require(await firstPresentedBannerItem(events))
        #expect(item.windowId == "ad-deny")
        #expect(
            await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration: item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: item.windowMaterialRevisionToken
            ),
            "the exact displayed material must be accepted"
        )

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one denial must record exactly one controller sample")
        #expect(state.integral == 1, "an explicit banner No is a FALSE-POSITIVE signal → integral +1")
        await controllerStore.close()
    }

    @Test("Accepting a suggested (not-auto-skipped) ad records a MISS signal (integral −1)")
    func acceptSuggestedSkipRecordsMiss() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let markOnly = makeSkipTestMarkOnlyWindow(id: "ad-suggest-miss")
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-miss"))

        #expect(await orchestrator.acceptSuggestedSkip(windowId: "ad-suggest-miss"))

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one acceptance must record exactly one controller sample")
        #expect(state.integral == -1, "accepting a suggested (missed) ad is a MISS signal → integral −1")
        await controllerStore.close()
    }

    /// playhead-i08e: the same seam in the PRODUCTION wiring — see
    /// `revertWindowWithCorrectionStoreRecordsOneFalsePositive` for exactly
    /// what the wired variant does and does not add, and for why the learning
    /// ingestor is NOT what supplies that (it is wired to match
    /// `PlayheadRuntime`, not to make the test discriminate).
    @Test("Accepting a suggested ad records exactly one MISS with the correction store wired")
    func acceptSuggestedSkipWithCorrectionStoreRecordsOneMiss() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let correctionStore = PersistentUserCorrectionStore(store: store)
        await correctionStore.setLearningArtifactIngestor(
            LearningArtifactIngestor(
                store: store,
                knowledgeStore: SponsorKnowledgeStore(store: store)
            )
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let markOnly = makeSkipTestMarkOnlyWindow(id: "ad-suggest-miss-wired")
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-miss-wired"))

        #expect(await orchestrator.acceptSuggestedSkip(windowId: "ad-suggest-miss-wired"))

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one acceptance must record exactly one controller sample")
        #expect(state.integral == -1, "accepting a suggested (missed) ad is a MISS signal → integral −1")
        await controllerStore.close()
    }

    /// playhead-i08e (fourth pass): renamed from `noStoreNoWrite` / "performs
    /// no controller write". With no store wired there is no surface on which a
    /// write could be observed, so that title named a contract this test cannot
    /// falsify — the same defect class the bead is cleaning up. What it really
    /// pins, and what the pre-fix code failed, is that both seams RUN TO
    /// COMPLETION in the flag-OFF default instead of trapping on the absent
    /// dependency. The observable half lives in
    /// `anonymousRevertRecordsNoControllerSample`.
    @Test("No controller store wired ⇒ the revert seams still run to completion (flag-OFF default)")
    func noControllerStoreLeavesRevertSeamsIntact() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        // Deliberately do NOT wire a controller store — this is the production
        // flag-OFF default. The revert must still work and must not crash.
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let listened = makeSkipTestAdWindow(id: "ad-none", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        let vetoed = makeSkipTestAdWindow(id: "ad-none-veto", startTime: 300, endTime: 360, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(listened)
        try await store.insertAdWindow(vetoed)
        await orchestrator.receiveAdWindows([listened, vetoed])

        await orchestrator.recordListenRevert(windowId: "ad-none", podcastId: podcastId)
        // playhead-i08e: cover a seam this bead made live again, not only the
        // one that never broke — `revertWindow` is the path whose first
        // statement used to throw when no correction store was wired.
        #expect(await orchestrator.revertWindow(windowId: "ad-none-veto", podcastId: podcastId))

        // Durable proof of completion, not merely "did not crash": an aborted
        // gesture is also silent. `revertWindow`'s receipt is committed by the
        // AnalysisStore transaction, so it lands even with no correction store.
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(
            receipts.count == 1,
            "the veto must still commit its durable receipt with the flag off"
        )
        #expect(receipts.first?.source == .manualVeto)
        let row = try #require(try await store.fetchAdWindow(id: "ad-none-veto"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)
        // `recordListenRevert` needs its own durable evidence. Its receipt goes
        // through `persistManualCorrectionVeto`, which no-ops without a
        // correction store, so the row flip is the only observable it leaves —
        // and without asserting it, making `return` the seam's first statement
        // would keep this test green even though the title names both seams.
        let listenedRow = try #require(try await store.fetchAdWindow(id: "ad-none"))
        #expect(
            listenedRow.decisionState == AdDecisionState.reverted.rawValue,
            "the Listen revert must also have run to completion"
        )

        // playhead-i08e (third pass): a closing probe that built a BRAND-NEW
        // controller store in a fresh temp directory and asserted `count() == 0`
        // was deleted here — empty by construction, unfalsifiable.
    }

    // MARK: - playhead-i08e (third pass): seams that must record NOTHING
    //
    // The suite above proves each calibration seam records exactly ONE sample
    // with the right sign. The other half of that contract — the seams and
    // conditions that must record NO sample — had no coverage at all, and it
    // is the half most exposed by this bead: reviving four dead explicit-
    // feedback paths makes it newly possible for one of them to start writing
    // where it must not. Each test below pairs its negative assertion with a
    // POSITIVE CONTROL (the gesture returned true / its durable receipt or row
    // flip landed) so "recorded nothing" can never be satisfied by a seam that
    // simply aborted — which is exactly how the original regression hid.

    /// The controller is per-show, so a correction carrying no show id has
    /// nowhere to land (`recordThresholdControlSignal`'s second guard). This
    /// replaces the deleted fresh-store probe in
    /// `noControllerStoreLeavesRevertSeamsIntact`: the store IS
    /// wired here, so a regression that fell back to `activePodcastId` — which
    /// `beginEpisode` set to a real value below — turns this red.
    ///
    /// playhead-i08e (tenth pass): that guard has TWO clauses and only the
    /// `nil` one was covered. An EMPTY show id is not a show either — a row
    /// keyed on `""` is per-show state that no episode can ever address, and
    /// every unattributed correction would pile into that one shared bucket.
    ///
    /// What the second gesture below rails is the CONTRACT, not either clause
    /// on its own, and the distinction matters to anyone tempted to simplify
    /// one away. The refusal is written twice — `recordThresholdControlSignal`
    /// declines to call, and `PerShowThresholdControllerStore.record` throws on
    /// an empty id — and that throw is swallowed by the seam's own `catch`, so
    /// removing either half ALONE leaves nothing observable for any test to
    /// catch. Observed: the store-half-alone mutation SURVIVED the focused set
    /// (that is why battery entry N06 now removes both). The
    /// orchestrator-half-alone case is equivalent by the same swallowed-throw
    /// argument rather than by a separate run. Both halves are load-bearing
    /// only together, which is the shape this assertion is mutation-verified in.
    @Test("An anonymous revert (no podcastId, or an empty one) records no controller sample")
    func anonymousRevertRecordsNoControllerSample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-anon", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        // A second window, vetoed under an EMPTY show id, so both clauses of
        // the guard are exercised by the same negative assertion below.
        let empty = makeSkipTestAdWindow(id: "ad-empty-show", startTime: 300, endTime: 360, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        try await store.insertAdWindow(empty)
        await orchestrator.receiveAdWindows([ad, empty])

        #expect(await orchestrator.revertWindow(windowId: "ad-anon", podcastId: nil))
        #expect(await orchestrator.revertWindow(windowId: "ad-empty-show", podcastId: ""))
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(receipts.count == 2, "each veto still commits its own durable receipt")
        #expect(receipts.allSatisfy { $0.source == .manualVeto })

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "an unattributed correction must not be folded into any show's controller state"
        )
        await controllerStore.close()
    }

    /// `confirmAutoSkippedBanner` is capture-only: the user agreeing that a
    /// skip was right is a TRUE positive, and the controller models only
    /// false-positive (raise) and miss (lower). Writing here would push the
    /// threshold on agreement.
    ///
    /// playhead-i08e (tenth pass): the hard-negative bank is the SECOND
    /// learning surface this seam must leave alone, and the only thing saying
    /// so was the MEMORY-POLLUTION GUARD comment above
    /// `ingestNegativeFingerprint` ("reached ONLY from reversion paths").
    /// Mutation-verified: adding an ingest call to this seam left the focused
    /// set green. It is the more expensive of the two mistakes — a confirmed
    /// TRUE positive stored as a hard negative teaches the bank to suppress a
    /// REAL ad whose copy repeats on the next episode.
    @Test("Confirming an auto-skipped banner records no controller sample and no hard negative")
    func confirmAutoSkippedBannerRecordsNoControllerSample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let negativeBank = try NegativeFingerprintBank(
            directoryURL: try makeTempDir(prefix: "i08e-confirm-bank")
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setNegativeFingerprintBank(negativeBank)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: episodeId,
            podcastId: podcastId,
            playbackLifecycleGeneration: 7
        )
        let events = await orchestrator.bannerEventStream()

        let ad = makeSkipTestAdWindow(
            id: "ad-confirm",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: "confirmed",
            // Comfortably clear of the bank's 4-token floor, so "no entry"
            // can never be satisfied by copy too short to be storable.
            evidenceText: "this episode is brought to you by our sponsor"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        let item = try #require(await firstPresentedBannerItem(events))
        #expect(item.windowId == "ad-confirm")
        #expect(
            await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration: item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: item.windowMaterialRevisionToken
            ),
            "the exact displayed material must be accepted"
        )
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(receipts.count == 1, "the Yes ran to completion and committed its receipt")
        #expect(receipts.first?.source == .bannerAutoSkipConfirmed)

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "agreeing with a skip is a true positive — it must not move the threshold"
        )
        await drainOrchestratorEffects(orchestrator)
        #expect(
            try await negativeBank.allEntries().isEmpty,
            """
            Agreeing with a skip is a CONFIRMED TRUE positive. Ingesting its \
            copy as a hard negative would suppress the same ad the next time \
            it airs — the bank's write trigger is the reversion seams only.
            """
        )
        await controllerStore.close()
        await negativeBank.close()
    }

    /// `declineSuggestedSkip` is capture-only for the mirror reason: the
    /// algorithm only OFFERED a banner, it never altered playback, so a No is
    /// too weak to raise the auto-skip threshold.
    ///
    /// This seam was one of the four revived by playhead-i08e, but NOT by this
    /// test: it wires a `PersistentUserCorrectionStore`, so the removed
    /// `correctionStore != nil` precondition never fired here and this test
    /// passed before the fix too. (Confirmed by mutation: restoring the
    /// precondition in `makeSuggestDenialCorrection` reddens only
    /// `suggestNoPersistsWithoutCorrectionStore` in SkipOrchestratorRevertTests,
    /// which is the unwired-configuration rail and must not be deleted as a
    /// duplicate of this one.) What keeps THIS test honest is the positive
    /// control below — the gesture returned true and committed its receipt —
    /// so "recorded nothing" cannot be satisfied by a seam that aborted.
    @Test("An explicit suggest-tier No records no controller sample")
    func declineSuggestedSkipRecordsNoControllerSample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let markOnly = makeSkipTestMarkOnlyWindow(id: "ad-suggest-no")
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-no"))

        #expect(
            await orchestrator.declineSuggestedSkip(
                windowId: "ad-suggest-no",
                isExplicitDenial: true
            ),
            "the explicit No must run to completion"
        )
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(receipts.count == 1, "the No ran to completion and committed its receipt")
        #expect(receipts.first?.source == .bannerSuggestionDenied)

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "a suggest-tier veto never altered playback — too weak to raise the threshold"
        )
        await controllerStore.close()
    }

    /// The `revertedManagedAny` routing that playhead-i08e relocated above the
    /// trust hop. `revertByTimeRangeManagedRecordsFalsePositive` covers the
    /// true arm; dropping the condition entirely would leave that test green,
    /// so the false arm needs its own rail.
    @Test("A suggest-tier-only revertByTimeRange records no controller sample")
    func revertByTimeRangeSuggestOnlyRecordsNoControllerSample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeTestControllerStore(prefix: "xsdz11-orch-store")
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        // markOnly ONLY — the gesture must find no managed auto-skip window,
        // so `revertedManagedAny` stays false.
        let markOnly = makeSkipTestMarkOnlyWindow(id: "ad-suggest-range")
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-range"))

        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: podcastId)

        #expect(
            !(await orchestrator.activeSuggestWindowIDs()).contains("ad-suggest-range"),
            "the suggest tier really was vetoed — the gesture is not a no-op"
        )
        let row = try #require(try await store.fetchAdWindow(id: "ad-suggest-range"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "a suggest-only revert never altered playback — too weak to raise the threshold"
        )
        await controllerStore.close()
    }
}
