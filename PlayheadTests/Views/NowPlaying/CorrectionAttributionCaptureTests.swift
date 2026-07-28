// CorrectionAttributionCaptureTests.swift
// playhead-254m — the view layer must resolve a correction's attribution
// identity BEFORE it suspends, not after.
//
// THE DEFECT. `NowPlayingView` used to build the transcript sheet with
// `podcastId: runtime.currentPodcastId` and to call
// `revertByTimeRange(..., podcastId: runtime.currentPodcastId)` from inside the
// sheet's own callback. Both are LIVE reads taken at callback time, not the
// identity captured when the gesture began: SwiftUI recomputes sheet content
// while the sheet stays mounted, and the veto's `Task` suspends across the
// orchestrator/store hops. If autoplay advanced in between, the correction's
// receipt, trust penalty and per-show threshold sample were attributed to the
// NEW episode's show rather than the one the listener actually corrected.
//
// This is the same defect class playhead-i08e and playhead-o4qr closed one
// layer down, inside `SkipOrchestrator`. The view-layer instance was removed by
// the o4qr merge (`TranscriptPeekPresentationContext` — an immutable identity
// record captured in the synchronous Button action) but nothing pinned it, so
// it could return in one careless line. These rails pin it in both directions:
//
//   • a SOURCE canary, because "no late read exists" is a statement about code
//     shape and cannot be observed behaviourally — the failure it prevents is
//     the reintroduction of the read, not a wrong value from the current one;
//   • a BEHAVIOURAL race, because the captured value only means anything if the
//     seam behind it honours it across a mid-gesture episode replacement.
//
// XCTest for the canary so it stays filterable from the test plan (see project
// memory `xctestplan_swift_testing_limitation`), matching the neighbouring
// `TranscriptPeekViewVetoSourceCanaryTests`.

import Foundation
import Testing
import XCTest

@testable import Playhead

// MARK: - Source canary

final class ViewLayerCorrectionAttributionCaptureCanaryTests: XCTestCase {

    private static let nowPlayingViewPath =
        "Playhead/Views/NowPlaying/NowPlayingView.swift"
    private static let nowPlayingViewModelPath =
        "Playhead/Views/NowPlaying/NowPlayingViewModel.swift"
    private static let transcriptPeekViewPath =
        "Playhead/Views/NowPlaying/TranscriptPeekView.swift"

    /// The whole point of `TranscriptPeekPresentationContext`: the show is
    /// resolved ONCE, in the synchronous Button action that presents the sheet,
    /// and every later async correction reads it from there.
    func testNowPlayingViewResolvesTheShowExactlyOnceAtCaptureTime() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.nowPlayingViewPath
        )
        let stripped = SwiftSourceInspector.strippingComments(source)

        XCTAssertEqual(
            SwiftSourceInspector.occurrences(
                of: "runtime.currentPodcastId",
                in: stripped
            ),
            1,
            """
            NowPlayingView may read the live show exactly ONCE — at the \
            synchronous capture site that builds \
            TranscriptPeekPresentationContext. A second read is a late read by \
            construction: everything else in this file that needs a show runs \
            after a suspension, so it would attribute the listener's \
            correction to whatever episode is playing by then.
            """
        )
        XCTAssertTrue(
            stripped.contains("podcastId: runtime.currentPodcastId,"),
            """
            The one permitted read must still be the capture site inside \
            TranscriptPeekPresentationContext. If it moved, the count \
            assertion above stopped meaning anything.
            """
        )
    }

    func testTranscriptSheetCallbacksUseTheCapturedContextNotTheLiveRuntime() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.nowPlayingViewPath
        )
        let sheetBody = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: source,
                after: ".sheet(item: $transcriptPeekContext)"
            ),
            "Could not locate the transcript peek sheet closure"
        )
        let stripped = SwiftSourceInspector.strippingComments(sheetBody)

        XCTAssertFalse(
            stripped.contains("runtime.currentPodcastId"),
            """
            The transcript sheet's content closure is recomputed while the \
            sheet stays mounted, and its correction callbacks run after a \
            suspension. Reading the live show here is exactly playhead-254m: \
            the veto's receipt, trust penalty and per-show threshold sample \
            would name the episode that is playing when the callback resumes.
            """
        )
        XCTAssertFalse(
            stripped.contains("runtime.currentEpisodeId"),
            "Same rule for the episode identity the store transaction validates against."
        )
        XCTAssertGreaterThanOrEqual(
            SwiftSourceInspector.occurrences(
                of: "sourceContext.podcastId",
                in: stripped
            ),
            3,
            """
            The captured show must reach all three consumers: the view's own \
            podcastId binding, the revert callback, and the mark-ad callback.
            """
        )
    }

    /// The "Always skip this sponsor" seam writes its own CorrectionEvent
    /// rather than routing through the orchestrator, so it owns its own
    /// capture discipline.
    func testAlwaysSkipSponsorAttributesToTheBannerItemNotTheLiveRuntime() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.nowPlayingViewPath
        )
        let body = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: source,
                after: "onAlwaysSkipSponsorAsync:"
            ),
            "Could not locate the onAlwaysSkipSponsorAsync closure"
        )
        let stripped = SwiftSourceInspector.strippingComments(body)

        XCTAssertTrue(
            stripped.contains("let podcastId = item.podcastId"),
            """
            The sponsorOnShow correction must be attributed to the show the \
            BANNER named. AdSkipBannerItem is an immutable value stamped when \
            the card was emitted, which is what makes it a capture.
            """
        )
        XCTAssertFalse(
            stripped.contains("runtime.currentPodcastId"),
            "A live show read here would file the sponsor veto against the wrong podcast."
        )
        let episodeGuard = try XCTUnwrap(
            stripped.range(of: "guard runtime.currentEpisodeId == item.episodeId")
        )
        let assetRead = try XCTUnwrap(
            stripped.range(of: "runtime.currentAnalysisAssetId")
        )
        let firstAwait = try XCTUnwrap(stripped.range(of: "await "))
        XCTAssertLessThan(
            episodeGuard.lowerBound,
            assetRead.lowerBound,
            """
            The live asset id is only usable because the episode guard above it \
            proves the runtime still owns the banner's episode, and because no \
            suspension separates them.
            """
        )
        XCTAssertLessThan(
            assetRead.lowerBound,
            firstAwait.lowerBound,
            "Every identity this receipt carries must be resolved before the first suspension."
        )
    }

    func testReportHearingAdCapturesItsIdentityBeforeTheTask() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.nowPlayingViewModelPath
        )
        let body = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: source,
                after: "func reportHearingAd("
            ),
            "Could not locate reportHearingAd"
        )
        let stripped = SwiftSourceInspector.strippingComments(body)
        let capture = try XCTUnwrap(
            stripped.range(of: "let podcastId = runtime.currentPodcastId")
        )
        let task = try XCTUnwrap(stripped.range(of: "Task {"))
        XCTAssertLessThan(
            capture.lowerBound,
            task.lowerBound,
            """
            "Hearing an ad" expands its boundary across three store reads before \
            it persists anything. Resolving the show after that Task starts \
            would file the user mark and its false-negative trust signal \
            against whatever autoplay moved on to.
            """
        )
    }

    func testTranscriptPeekViewNeverResolvesTheShowItself() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.transcriptPeekViewPath
        )
        let stripped = SwiftSourceInspector.strippingComments(source)
        XCTAssertEqual(
            SwiftSourceInspector.occurrences(
                of: "currentPodcastId",
                in: stripped
            ),
            0,
            """
            TranscriptPeekView takes its show as an injected `podcastId` \
            property bound from the presentation context. Reaching for the live \
            runtime instead would reintroduce playhead-254m one level down.
            """
        )

        for submit in [
            "func submitMarkedChunks(",
            "func submitUntranscribedTailMark(",
        ] {
            let body = try XCTUnwrap(
                SwiftSourceInspector.firstBody(in: source, after: submit),
                "Could not locate \(submit)"
            )
            let strippedBody = SwiftSourceInspector.strippingComments(body)
            let capture = try XCTUnwrap(
                strippedBody.range(of: "let pid = podcastId"),
                "\(submit) must bind the show into a local"
            )
            let task = try XCTUnwrap(
                strippedBody.range(of: "Task {"),
                "\(submit) must submit through a Task"
            )
            XCTAssertLessThan(
                capture.lowerBound,
                task.lowerBound,
                """
                \(submit) resolves the show after its Task starts. The trust \
                signal it issues after `await markAd(...)` would then name the \
                replacement episode's show.
                """
            )
        }
    }
}

// MARK: - Behavioural race

/// Test-local gate: parks a gesture at a real suspension so an episode
/// replacement can be interleaved with it deterministically. Mirrors the
/// private gate `SkipOrchestratorRevertLifecycleRaceTests` uses.
private actor TranscriptVetoGate {
    private var didStart = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func wait() async {
        didStart = true
        let waiters = startWaiters
        startWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func waitUntilStarted() async {
        if didStart { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

@Suite("A view-layer correction is attributed to the show captured at gesture time (playhead-254m)")
struct ViewLayerCorrectionAttributionRaceTests {

    /// The transcript veto's PRODUCTION entry form, which no other race test
    /// drives: the playback-bound `revertByTimeRange` overload, carrying the
    /// synthetic `DecodedSpan` that `TranscriptPeekView.submitNotAdChunks`
    /// builds and the complete identity that `TranscriptPeekPresentationContext`
    /// captured when the sheet was presented.
    ///
    /// The episode is replaced by a DIFFERENT show while the gesture is parked
    /// at the revert barrier — which is what makes "the captured show" an
    /// assertable claim rather than a tautology.
    @Test(
        "A transcript veto whose episode is replaced mid-flight still names the captured show",
        .timeLimit(.minutes(1))
    )
    func transcriptVetoSurvivesEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2")
        )
        // Seed BOTH shows: `recordFalseSkipSignal` never lazy-creates, so an
        // unseeded replacement would swallow a misrouted penalty silently and
        // "the replacement was untouched" would be vacuous.
        let trustStore = try await makeTestStore()
        for show in ["podcast-1", "podcast-2"] {
            try await seedSkipTestTrustProfile(
                in: trustStore,
                podcastId: show,
                mode: "auto",
                trustScore: 0.9,
                observations: 10
            )
        }
        let trustService = TrustScoringService(store: trustStore)
        let controllerStore = try makeTestControllerStore(
            prefix: "254m-transcript-veto"
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }

        let capturedGeneration: UInt64 = 7
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: capturedGeneration
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-transcript-veto",
            startTime: 60,
            endTime: 150,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // Exactly what the sheet captured when it was presented.
        let capturedAssetId = "asset-1"
        let capturedEpisodeId = "ep-1"
        let capturedPodcastId = "podcast-1"
        let vetoStart = 70.0
        let vetoEnd = 140.0
        // Byte-for-byte the shape `submitNotAdChunks` synthesizes.
        let vetoSpan = DecodedSpan(
            id: String(
                format: "%@-veto-%.3f-%.3f",
                capturedAssetId,
                vetoStart,
                vetoEnd
            ),
            assetId: capturedAssetId,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: Int.max,
            startTime: vetoStart,
            endTime: vetoEnd,
            anchorProvenance: []
        )

        let gate = TranscriptVetoGate()
        await orchestrator._setRevertPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            await orchestrator.revertByTimeRange(
                start: vetoStart,
                end: vetoEnd,
                analysisAssetId: capturedAssetId,
                podcastId: capturedPodcastId,
                ifCurrentEpisodeId: capturedEpisodeId,
                ifPlaybackLifecycleGeneration: capturedGeneration,
                correctionSpan: vetoSpan
            )
        }
        await gate.waitUntilStarted()

        // Autoplay advances to a DIFFERENT show while the veto is parked.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "ep-2",
            podcastId: "podcast-2",
            playbackLifecycleGeneration: capturedGeneration &+ 1
        )
        await orchestrator._setRevertPersistenceBarrierForTesting(nil)
        await gate.release()
        #expect(
            await gesture.value,
            "the listener's correction must still commit"
        )

        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: capturedAssetId,
            expected: 1
        )
        #expect(receipts.count == 1)
        #expect(receipts.first?.source == .manualVeto)
        #expect(
            receipts.first?.podcastId == capturedPodcastId,
            """
            The durable receipt names the show that was live when the effect \
            ran, not the one the listener corrected. Show attribution is what \
            routes every downstream learning surface, so this is the whole \
            bead in one field.
            """
        )
        #expect(
            receipts.first?.analysisAssetId == capturedAssetId,
            "and the receipt belongs to the captured episode's asset"
        )

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: capturedPodcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1)
        #expect(
            state.integral == 1,
            "a transcript veto of a managed auto-skip is a FALSE-POSITIVE signal"
        )
        #expect(
            try await controllerRowsExcludingBarrier(
                controllerStore,
                orchestrator
            ) == 1,
            """
            Exactly one show may carry controller state. A second row means the \
            sample followed the episode that is live now.
            """
        )

        let penalised = try #require(
            try await trustStore.fetchProfile(podcastId: capturedPodcastId)
        )
        #expect(penalised.recentFalseSkipSignals == 1)
        let untouched = try #require(
            try await trustStore.fetchProfile(podcastId: "podcast-2")
        )
        #expect(
            untouched.recentFalseSkipSignals == 0,
            "the replacement show must not absorb a penalty for a gesture that predates it"
        )
        #expect(
            untouched.skipTrustScore >= 0.9,
            "and its trust must not move"
        )

        let row = try #require(
            try await store.fetchAdWindow(id: "ad-transcript-veto")
        )
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)

        await controllerStore.close()
    }

    /// The other half of the capture contract: a sheet that outlived its
    /// episode must be REFUSED outright rather than retargeted. Without the
    /// captured identity there is nothing to compare against and the veto
    /// would land on the replacement episode's timeline.
    @Test(
        "A transcript veto submitted after the episode changed is refused, not retargeted",
        .timeLimit(.minutes(1))
    )
    func staleTranscriptVetoIsRefused() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2")
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 7
        )
        let ad = makeSkipTestAdWindow(
            id: "ad-stale-sheet",
            startTime: 60,
            endTime: 150,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // The sheet's captured identity, then autoplay advances BEFORE submit.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "ep-2",
            podcastId: "podcast-2",
            playbackLifecycleGeneration: 8
        )

        let accepted = await orchestrator.revertByTimeRange(
            start: 70,
            end: 140,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 7,
            correctionSpan: nil
        )
        #expect(
            accepted == false,
            """
            A veto carrying a stale captured identity must be refused. Dropping \
            the captured context and reading the live one instead would make \
            this gesture SUCCEED against the replacement episode.
            """
        )
        #expect(
            try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
                .isEmpty,
            "a refused gesture writes no receipt for the captured episode"
        )
        #expect(
            try await store.loadCorrectionEvents(analysisAssetId: "asset-2")
                .isEmpty,
            "and none for the replacement episode either"
        )
    }
}
