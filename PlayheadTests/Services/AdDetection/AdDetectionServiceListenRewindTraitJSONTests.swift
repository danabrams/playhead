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

        // C26 L-2 paired-with-happy-path assertion: the missing-profile
        // counter MUST stay at 0 when the profile exists. Without this
        // assertion a future regression that inverts the increment
        // condition (incrementing on every call instead of only on
        // missing-profile) would still pass `recordListenRewindMissingProfileNoOps`
        // (which only checks `== 1`).
        let missingCount = await service.missingProfileListenRewindCount
        #expect(
            missingCount == 0,
            "missingProfileListenRewindCount must stay at 0 on the with-profile happy path; got \(missingCount)"
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

        // C26 L-2: telemetry counter must increment on the missing-profile
        // branch. Without this assertion the counter could regress to a
        // dead variable and the test would still pass on the log line
        // alone (which is harder to observe in production).
        let missingCount = await service.missingProfileListenRewindCount
        #expect(
            missingCount == 1,
            "missingProfileListenRewindCount must increment exactly once when recordListenRewind hits the no-profile branch, got \(missingCount)"
        )
    }

    @Test("recordListenRewind missing-profile counter monotonically increments across multiple calls")
    func recordListenRewindMissingProfileCounterIsMonotonic() async throws {
        // C26 L-2 monotonicity contract: the counter docstring claims
        // "monotonic counter incremented every time recordListenRewind
        // reaches the updateProfileIfExists == nil branch". A test that
        // calls the path twice and asserts `== 2` pins that, distinct
        // from the single-call test which only proves `>= 1`.
        let store = try await makeTestStore()
        let podcastId = "podcast-monotonic-listen-rewind"
        let assetId = "asset-monotonic-listen-rewind"
        let windowIdA = "window-monotonic-listen-rewind-a"
        let windowIdB = "window-monotonic-listen-rewind-b"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowIdA, assetId: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowIdB, assetId: assetId))

        let service = makeService(store: store)
        try await service.recordListenRewind(windowId: windowIdA, podcastId: podcastId)
        try await service.recordListenRewind(windowId: windowIdB, podcastId: podcastId)

        let missingCount = await service.missingProfileListenRewindCount
        #expect(
            missingCount == 2,
            "missingProfileListenRewindCount must increment monotonically per missing-profile call; got \(missingCount) after 2 calls"
        )
    }

    @Test("recordListenRewind missing-profile counter only increments on missing branch — interleaved happy/missing calls")
    func recordListenRewindMissingProfileCounterIsBranchScoped() async throws {
        // Cycle-3 L-3: pin that the counter is incremented ONLY on the
        // missing-profile branch and is NOT touched by the happy path.
        // The single-call missing test proves `>= 1` on miss, the
        // monotonic test proves `== 2` on two misses, and the happy-path
        // == 0 assertion proves "never increments without a miss" — but
        // none of those interleave the branches in a single service
        // instance. A regression that increments on EVERY call (or that
        // accidentally resets on a happy-path call) would still pass
        // each of those tests in isolation.
        //
        // This test runs miss → hit → miss → hit (alternating) on the
        // same `AdDetectionService` and asserts the counter equals
        // exactly the number of misses (2), proving:
        //   1. the happy path does not increment, AND
        //   2. the happy path does not reset what the miss path
        //      previously incremented.
        let store = try await makeTestStore()
        let missingPodcastId = "podcast-interleaved-missing"
        let presentPodcastId = "podcast-interleaved-present"
        let assetId = "asset-interleaved-listen-rewind"
        let missWindowA = "window-interleaved-miss-a"
        let hitWindowA  = "window-interleaved-hit-a"
        let missWindowB = "window-interleaved-miss-b"
        let hitWindowB  = "window-interleaved-hit-b"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: missWindowA, assetId: assetId))
        try await store.insertAdWindow(makeAdWindow(id: hitWindowA,  assetId: assetId))
        try await store.insertAdWindow(makeAdWindow(id: missWindowB, assetId: assetId))
        try await store.insertAdWindow(makeAdWindow(id: hitWindowB,  assetId: assetId))

        // Seed a profile under `presentPodcastId` so its calls hit the
        // happy path. `missingPodcastId` has NO profile so its calls hit
        // the missing branch.
        let seed = PodcastProfile(
            podcastId: presentPodcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 5,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: nil,
            title: nil
        )
        try await store.upsertProfile(seed)

        let service = makeService(store: store)
        try await service.recordListenRewind(windowId: missWindowA, podcastId: missingPodcastId) // +1
        try await service.recordListenRewind(windowId: hitWindowA,  podcastId: presentPodcastId) // +0
        try await service.recordListenRewind(windowId: missWindowB, podcastId: missingPodcastId) // +1
        try await service.recordListenRewind(windowId: hitWindowB,  podcastId: presentPodcastId) // +0

        let count = await service.missingProfileListenRewindCount
        #expect(
            count == 2,
            "missingProfileListenRewindCount must equal the number of missing-profile calls (2) and be untouched by happy-path calls; got \(count)"
        )
    }
}
