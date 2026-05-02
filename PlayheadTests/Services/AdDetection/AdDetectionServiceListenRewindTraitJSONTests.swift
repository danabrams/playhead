// AdDetectionServiceListenRewindTraitJSONTests.swift
//
// skeptical-review-cycle-17 missing-test (paired with M-1 fix in
// `AdDetectionService.recordListenRewind`).
//
// Pre-cycle-17, `recordListenRewind` used a non-atomic
// `await store.fetchProfile(...)` → mutate → `await store.upsertProfile(...)`
// pair AND its `PodcastProfile(...)` constructor omitted both
// `traitProfileJSON` and `title` (defaulting both to `nil`). Because
// `AnalysisStore.upsertProfile` writes
// `traitProfileJSON = excluded.traitProfileJSON` (NOT COALESCE — see
// `AnalysisStore.swift:upsertProfile`), every "Listen" tap from the
// production NowPlaying flow silently nilled that podcast's persisted
// trait profile. Cycle-15 M-2 closed the same defect in `updatePriors`
// but did not touch this sibling.
//
// The cycle-17 fix routes the body through
// `store.updateProfileIfExists(podcastId:update:)` and carries
// `existing.traitProfileJSON` (and `existing.title`) forward.
//
// This file pins the *behavioral* invariant. The source-canary at
// `AdDetectionServiceUpdatePriorsAtomicityCanaryTests.swift`
// (`testRecordListenRewindBodyUsesUpdateProfileIfExistsAndCarriesTraitJSON`)
// pins the *implementation* shape. Belt-and-suspenders: a future
// refactor that swaps to a different store helper but forgets the
// trait-JSON carry-forward will trip *this* test even if it doesn't
// match the canary regex.

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService.recordListenRewind — traitProfileJSON survival (cycle-17 M-1)")
struct AdDetectionServiceListenRewindTraitJSONTests {

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

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
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
    }

    private func makeAdWindow(id: String, assetId: String) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: 30,
            endTime: 60,
            confidence: 0.95,
            boundaryState: "confirmed",
            decisionState: AdDecisionState.applied.rawValue,
            detectorVersion: "detection-v1",
            advertiser: "Squarespace",
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

    @Test("recordListenRewind preserves a non-nil traitProfileJSON")
    func recordListenRewindPreservesTraitJSON() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-listen-rewind-trait-1"
        let assetId = "asset-listen-rewind-trait-1"
        let windowId = "window-listen-rewind-trait-1"

        // Seed an asset and a window so updateAdWindowDecision has
        // something to flip.
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId))

        // Seed a profile with a non-nil traitProfileJSON. The actual
        // payload doesn't matter to this test — only that the column
        // is non-NULL before the listen-rewind and identical after.
        let traitJSON = "{\"showCategory\":\"interview\",\"averageAdDensity\":0.18}"
        let seed = PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "squarespace,nordvpn",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 2,
            skipTrustScore: 0.85,
            observationCount: 12,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 1,
            traitProfileJSON: traitJSON,
            title: "Diary of a CEO"
        )
        try await store.upsertProfile(seed)

        // Sanity: the seed actually landed.
        let beforeRewind = try #require(await store.fetchProfile(podcastId: podcastId))
        try #require(beforeRewind.traitProfileJSON == traitJSON)
        try #require(beforeRewind.title == "Diary of a CEO")

        // Trigger the production code path.
        let service = makeService(store: store)
        try await service.recordListenRewind(windowId: windowId, podcastId: podcastId)

        let afterRewind = try #require(await store.fetchProfile(podcastId: podcastId))

        // Cycle-17 M-1 invariant: traitProfileJSON survives.
        #expect(
            afterRewind.traitProfileJSON == traitJSON,
            "Cycle-17 M-1 regression: recordListenRewind clobbered the persisted traitProfileJSON. Expected \(traitJSON) but got \(String(describing: afterRewind.traitProfileJSON))"
        )

        // Title also survives (COALESCE-protected, so this would pass
        // even on the pre-cycle-17 code; included here for symmetry
        // with the canary's combined title+traitJSON carry-forward).
        #expect(
            afterRewind.title == "Diary of a CEO",
            "title was lost across recordListenRewind. Expected 'Diary of a CEO' but got \(String(describing: afterRewind.title))"
        )

        // Behavioral mutations the method is supposed to perform —
        // proves the closure ran and we're not just observing a
        // no-op early return.
        #expect(
            afterRewind.implicitFalsePositiveCount == seed.implicitFalsePositiveCount + 1,
            "implicitFalsePositiveCount should increment by 1, got \(afterRewind.implicitFalsePositiveCount)"
        )
        #expect(
            afterRewind.recentFalseSkipSignals == seed.recentFalseSkipSignals + 1,
            "recentFalseSkipSignals should increment by 1, got \(afterRewind.recentFalseSkipSignals)"
        )
        #expect(
            afterRewind.skipTrustScore < seed.skipTrustScore,
            "skipTrustScore should decrement, got \(afterRewind.skipTrustScore)"
        )
        #expect(
            afterRewind.observationCount == seed.observationCount,
            "observationCount must NOT change in recordListenRewind (only updatePriors / TrustScoringService own it)"
        )

        // The window decision flips to reverted (the public-API contract).
        let windows = try await store.fetchAdWindows(assetId: assetId)
        let window = try #require(windows.first { $0.id == windowId })
        #expect(
            window.decisionState == AdDecisionState.reverted.rawValue,
            "AdWindow.decisionState should flip to 'reverted', got \(window.decisionState)"
        )
    }

    @Test("recordListenRewind no-ops gracefully when profile is missing")
    func recordListenRewindMissingProfileNoOps() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-missing-listen-rewind"
        let assetId = "asset-missing-listen-rewind"
        let windowId = "window-missing-listen-rewind"

        // No profile seeded. The method MUST still flip the window's
        // decisionState (that's the user-visible commit) and warn-log
        // about the missing profile rather than throwing or
        // accidentally lazy-creating a stub row.
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId))

        let service = makeService(store: store)
        try await service.recordListenRewind(windowId: windowId, podcastId: podcastId)

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(
            profile == nil,
            "recordListenRewind must NOT lazy-create a podcast profile (use updateProfileIfExists, not mutateProfile). Got \(String(describing: profile))"
        )

        let windows = try await store.fetchAdWindows(assetId: assetId)
        let window = try #require(windows.first { $0.id == windowId })
        #expect(
            window.decisionState == AdDecisionState.reverted.rawValue,
            "Window decisionState must still flip to 'reverted' even when profile is missing, got \(window.decisionState)"
        )
    }
}
