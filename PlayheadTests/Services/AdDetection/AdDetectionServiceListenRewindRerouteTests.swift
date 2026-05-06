// AdDetectionServiceListenRewindRerouteTests.swift
// playhead-q45f: pin the reroute of `AdDetectionService.recordListenRewind`
// from an inline `updateProfileIfExists` block to
// `TrustScoringService.recordWeakFalseSkipSignal`. Pre-q45f, the rewind
// only mutated `recentFalseSkipSignals` without ever running the demotion
// state machine — multiple listen-rewinds accumulated indefinitely without
// triggering a mode transition. q45f closes that defect by routing
// the trust-score side-effect through TrustScoringService, which DOES
// run `evaluateDemotion` on every signal.

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService.recordListenRewind reroute (q45f)", .serialized)
struct AdDetectionServiceListenRewindRerouteTests {

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeAdWindow(id: String, assetId: String, startTime: Double = 30.0) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: startTime + 30,
            confidence: 0.95,
            boundaryState: "confirmed",
            decisionState: AdDecisionState.applied.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "test",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false
        )
    }

    private func makeService(
        store: AnalysisStore,
        trust: TrustScoringService
    ) async -> AdDetectionService {
        let svc = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .off
            )
        )
        await svc.setTrustScoringService(trust)
        return svc
    }

    private func makeProfile(
        podcastId: String,
        mode: SkipMode = .auto,
        trust: Double = 0.90,
        observations: Int = 20,
        falseSignals: Int = 0
    ) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: trust,
            observationCount: observations,
            mode: mode.rawValue,
            recentFalseSkipSignals: falseSignals,
            traitProfileJSON: nil,
            title: nil
        )
    }

    @Test("a single recordListenRewind moves trust by exactly weakFalseSignalPenalty (default 0.05)")
    func singleRewindAppliesWeakPenalty() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        let assetId = "asset-q45f-single"
        let podcastId = "pod-q45f-single"
        let windowId = "win-q45f-single"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId))
        try await store.upsertProfile(makeProfile(podcastId: podcastId))

        let service = await makeService(store: store, trust: trust)
        try await service.recordListenRewind(windowId: windowId, podcastId: podcastId)

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(abs((profile?.skipTrustScore ?? -1) - 0.85) < 1e-10,
                "expected 0.85, got \(profile?.skipTrustScore ?? -1)")
        #expect(profile?.recentFalseSkipSignals == 1)
        #expect(profile?.implicitFalsePositiveCount == 1)

        // playhead-q45f cycle-3 M-A: behavioral coverage from the deleted
        // AdDetectionServiceListenRewindTraitJSONTests. The user-visible
        // commit (banner "Listen" tap reveals the ad span) flips
        // AdWindow.decisionState to `.reverted`. A future refactor that
        // drops or short-circuits this update would otherwise pass.
        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows.first(where: { $0.id == windowId })?.decisionState == AdDecisionState.reverted.rawValue,
                "recordListenRewind must flip the AdWindow.decisionState to .reverted")
    }

    @Test("two sequential listen-rewinds demote auto -> manual (closes the q45f defect)")
    func twoRewindsDemoteAutoToManual() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        let assetId = "asset-q45f-demote"
        let podcastId = "pod-q45f-demote"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: "win-1", assetId: assetId, startTime: 10))
        try await store.insertAdWindow(makeAdWindow(id: "win-2", assetId: assetId, startTime: 100))
        try await store.upsertProfile(makeProfile(podcastId: podcastId, mode: .auto, trust: 0.90))

        let service = await makeService(store: store, trust: trust)
        try await service.recordListenRewind(windowId: "win-1", podcastId: podcastId)
        try await service.recordListenRewind(windowId: "win-2", podcastId: podcastId)

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue,
                "two rewinds must demote auto -> manual; pre-q45f this never happened")
        #expect(profile?.recentFalseSkipSignals == 2)
    }

    @Test("event log row still persists for the q45f.1 contract")
    func eventLogRowStillPersists() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        let assetId = "asset-q45f-eventlog"
        let podcastId = "pod-q45f-eventlog"
        let windowId = "win-q45f-eventlog"
        let startTime: Double = 42.0

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId, startTime: startTime))
        try await store.upsertProfile(makeProfile(podcastId: podcastId))

        let service = await makeService(store: store, trust: trust)
        try await service.recordListenRewind(windowId: windowId, podcastId: podcastId)

        let rows = try await store.fetchListenRewinds(forAssetId: assetId)
        #expect(rows.count == 1)
        #expect(rows.first?.time == startTime,
                "q45f.1 contract: row.time mirrors window.startTime")
    }

    @Test("no-trust-service injected: rewind still applies decision flip + event log, no trust mutation")
    func noTrustServiceStillFlipsAndLogs() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-q45f-notrust"
        let podcastId = "pod-q45f-notrust"
        let windowId = "win-q45f-notrust"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId))
        try await store.upsertProfile(makeProfile(podcastId: podcastId, trust: 0.90))

        // Build the service WITHOUT injecting a TrustScoringService.
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .off
            )
        )
        try await service.recordListenRewind(windowId: windowId, podcastId: podcastId)

        // Decision flip + event log still happen.
        let rows = try await store.fetchListenRewinds(forAssetId: assetId)
        #expect(rows.count == 1)

        // playhead-q45f cycle-3 M-A: decision flip happens regardless of
        // trust-service injection — the no-injection branch must NOT
        // silently drop the user-visible reveal.
        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows.first(where: { $0.id == windowId })?.decisionState == AdDecisionState.reverted.rawValue,
                "decision flip must fire even when no trust service is injected")

        // Trust score is untouched (legacy test factories that don't inject
        // the service should keep working without surprise side-effects).
        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile?.skipTrustScore == 0.90,
                "no trust service => no trust mutation; got \(profile?.skipTrustScore ?? -1)")
        #expect(profile?.recentFalseSkipSignals == 0)
    }

    // playhead-q45f cycle-1 M-1: end-to-end carry-forward through the
    // public `recordListenRewind` API. Pre-q45f the deleted
    // `AdDetectionServiceListenRewindTraitJSONTests` pinned this at the
    // public surface; the new TrustScoringServiceWeakSignalTests pin
    // it at the trust-service surface. This test reconnects the contract
    // at the AdDetectionService boundary so any future change that
    // introduces a profile mutation upstream of the trust delegation
    // (e.g. a stat update via bare upsertProfile) gets caught here.
    @Test("recordListenRewind preserves traitProfileJSON, title, adDurationStatsJSON, networkId end-to-end")
    func recordListenRewindCarriesForwardOptionalFieldsEndToEnd() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        let assetId = "asset-q45f-carry"
        let podcastId = "pod-q45f-carry"
        let windowId = "win-q45f-carry"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId))

        let traitJSON = #"{"k":"v"}"#
        let title = "Show Title"
        let adStatsJSON = #"{"sum":12.0}"#
        let networkId = "net-x"
        try await store.upsertProfile(PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.90,
            observationCount: 20,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: traitJSON,
            title: title,
            adDurationStatsJSON: adStatsJSON,
            networkId: networkId
        ))

        let service = await makeService(store: store, trust: trust)
        try await service.recordListenRewind(windowId: windowId, podcastId: podcastId)

        let after = try await store.fetchProfile(podcastId: podcastId)
        // playhead-q45f cycle-2 M-1: assert mutation occurred so this
        // test cannot pass trivially when the trust delegate silently
        // no-ops. The carry-forward checks below ride on top of a
        // proven-fired closure.
        #expect(after?.recentFalseSkipSignals == 1,
                "carry-forward test must prove the trust closure ran; got recentFalseSkipSignals=\(after?.recentFalseSkipSignals ?? -1)")
        #expect(after?.implicitFalsePositiveCount == 1)
        #expect(abs((after?.skipTrustScore ?? -1) - 0.85) < 1e-10,
                "weak signal must drop trust by 0.05 from 0.90; got \(after?.skipTrustScore ?? -1)")

        #expect(after?.traitProfileJSON == traitJSON,
                "traitProfileJSON must survive recordListenRewind; got \(String(describing: after?.traitProfileJSON))")
        #expect(after?.title == title)
        #expect(after?.adDurationStatsJSON == adStatsJSON)
        #expect(after?.networkId == networkId)

        // playhead-q45f cycle-3 M-B: behavioral coverage from the deleted
        // AdDetectionServiceListenRewindTraitJSONTests. observationCount
        // is owned by updatePriors and TrustScoringService — listen-rewind
        // must NOT increment it. (recordWeakFalseSkipSignal carries the
        // value through verbatim today; this pin catches a regression
        // that would silently bump it.)
        #expect(after?.observationCount == 20,
                "observationCount must NOT change in recordListenRewind; got \(after?.observationCount ?? -1)")
    }

    // playhead-q45f cycle-2 M-2: behavioral coverage for the
    // missing-profile branch on the WIRED-UP reroute path. A real
    // TrustScoringService is injected; the podcast has no profile row.
    // The contract: decision flip + event log fire, no profile is
    // lazy-created, no error is raised.
    @Test("recordListenRewind on missing-profile pod with wired-up trust service: no lazy-create, no throw")
    func missingProfileBranchOnWiredUpReroute() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        let assetId = "asset-q45f-missing-pod"
        let podcastId = "pod-q45f-missing-no-row"
        let windowId = "win-q45f-missing-pod"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId))
        // Note: no upsertProfile — this podcast has no profile row.

        let service = await makeService(store: store, trust: trust)
        try await service.recordListenRewind(windowId: windowId, podcastId: podcastId)

        // No lazy-create — fetchProfile must still return nil.
        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile == nil,
                "recordListenRewind must NOT lazy-create a profile on the missing-pod branch; got \(String(describing: profile))")

        // Event log still captures the tap (q45f gate cares about the event).
        let rows = try await store.fetchListenRewinds(forAssetId: assetId)
        #expect(rows.count == 1)
    }
}
