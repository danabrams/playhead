// AdDetectionServiceListenRewindPersistenceTests.swift
// playhead-q45f.1: pin the new persistence side-effect of
// `recordListenRewind`. Before q45f.1 the method only mutated
// `AdWindowDecision.reverted` + `PodcastProfile` columns; the q45f
// counterfactual gate had no event log to replay against. This file
// asserts the new contract: every successful call lands one row in
// `ad_listen_rewinds` whose `time` mirrors the source window's
// `startTime` (the position the user is rewound to).

import Foundation
import Testing
@testable import Playhead

private actor ListenTrustGate {
    private var started = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func hold() async {
        started = true
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
        if started { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

@Suite("AdDetectionService.recordListenRewind — persistence (playhead-q45f.1)")
struct AdDetectionServiceListenRewindPersistenceTests {

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

    private func makeAdWindow(id: String, assetId: String, startTime: Double) -> AdWindow {
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

    @Test("a successful recordListenRewind persists one ad_listen_rewinds row whose time = window.startTime")
    func recordListenRewindPersistsRow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-rewind-persist-1"
        let podcastId = "pod-rewind-persist-1"
        let windowId = "win-rewind-persist-1"
        let startTime: Double = 87.5

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId, startTime: startTime))
        try await store.upsertProfile(PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.9,
            observationCount: 5,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: nil,
            title: nil
        ))

        let service = makeService(store: store)
        try await service.recordListenRewind(
            windowId: windowId,
            analysisAssetId: assetId,
            podcastId: podcastId
        )

        let rows = try await store.fetchListenRewinds(forAssetId: assetId)
        #expect(rows.count == 1)
        #expect(rows.first?.windowId == windowId)
        #expect(rows.first?.podcastId == podcastId)
        #expect(rows.first?.time == startTime,
                "time field must mirror the source ad_window's startTime, got \(rows.first?.time ?? -1)")
    }

    @Test("repeated rewinds on the same window persist as distinct rows")
    func repeatedRewindsAccumulate() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-rewind-persist-2"
        let podcastId = "pod-rewind-persist-2"
        let windowId = "win-rewind-persist-2"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId, startTime: 12.0))
        try await store.upsertProfile(PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.9,
            observationCount: 5,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: nil,
            title: nil
        ))

        let service = makeService(store: store)
        try await service.recordListenRewind(
            windowId: windowId,
            analysisAssetId: assetId,
            podcastId: podcastId
        )
        try await service.recordListenRewind(
            windowId: windowId,
            analysisAssetId: assetId,
            podcastId: podcastId
        )
        try await service.recordListenRewind(
            windowId: windowId,
            analysisAssetId: assetId,
            podcastId: podcastId
        )

        let rows = try await store.fetchListenRewinds(forAssetId: assetId)
        #expect(rows.count == 3, "each tap is a distinct event; expected 3 rows, got \(rows.count)")
    }

    @Test("rewind on a missing-window id is rejected without a trust mutation")
    func missingWindowRejectsAction() async throws {
        // Defensive contract: if the supplied windowId has no corresponding
        // ad_window row (e.g. raced with a coverage rebuild that dropped
        // the row, or a stale UI state), recordListenRewind must reject the
        // action. The banner then remains retryable and no trust penalty can
        // be minted without the durable correction receipt.
        let (store, dir) = try await makeTestStoreWithDirectory()
        let assetId = "asset-rewind-noWindow"
        let podcastId = "pod-rewind-noWindow"
        let windowId = "win-does-not-exist"

        try await store.insertAsset(makeAsset(id: assetId))
        // Note: no AdWindow inserted with windowId.
        try await store.upsertProfile(PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.9,
            observationCount: 5,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: nil,
            title: nil
        ))

        let trust = TrustScoringService(store: store)
        let service = makeService(store: store)
        await service.setTrustScoringService(trust)
        var rejected = false
        do {
            try await service.recordListenRewind(
                windowId: windowId,
                analysisAssetId: assetId,
                podcastId: podcastId
            )
        } catch {
            rejected = true
        }
        #expect(rejected)

        // The asset-scoped accessor JOINs through `ad_windows`, so a
        // missing window would surface as empty regardless. To pin the
        // real contract — "no orphan row was written to ad_listen_rewinds"
        // — count the table directly, bypassing the JOIN.
        let directRowCount = try probeRowCount(in: dir, table: "ad_listen_rewinds")
        #expect(directRowCount == 0,
                "missing-window rewind must NOT fabricate any row in ad_listen_rewinds (direct count = \(directRowCount))")

        // Belt-and-suspenders: the asset-scoped fetch also returns empty.
        let rows = try await store.fetchListenRewinds(forAssetId: assetId)
        #expect(rows.isEmpty)

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile?.recentFalseSkipSignals == 0,
                "a rejected persistence receipt must not penalize trust")
    }

    @Test("stale Listen receipt cannot mutate a replacement asset's same-ID row")
    func listenReceiptIsAssetScoped() async throws {
        let store = try await makeTestStore()
        let sourceAssetId = "asset-rewind-source"
        let replacementAssetId = "asset-rewind-replacement"
        let podcastId = "pod-rewind-scope"
        let reusedWindowId = "win-rewind-reused"

        try await store.insertAsset(makeAsset(id: sourceAssetId))
        try await store.insertAsset(makeAsset(id: replacementAssetId))
        try await store.insertAdWindow(
            makeAdWindow(
                id: reusedWindowId,
                assetId: sourceAssetId,
                startTime: 12
            )
        )
        try await store.insertOrReplaceAdWindow(
            makeAdWindow(
                id: reusedWindowId,
                assetId: replacementAssetId,
                startTime: 212
            )
        )

        let service = makeService(store: store)
        var rejected = false
        do {
            try await service.recordListenRewind(
                windowId: reusedWindowId,
                analysisAssetId: sourceAssetId,
                podcastId: podcastId
            )
        } catch {
            rejected = true
        }

        #expect(rejected)
        let replacement = try #require(
            try await store.fetchAdWindow(id: reusedWindowId)
        )
        #expect(replacement.analysisAssetId == replacementAssetId)
        #expect(replacement.decisionState == AdDecisionState.applied.rawValue)
        #expect(
            try await store.fetchListenRewinds(
                forAssetId: replacementAssetId
            ).isEmpty
        )
    }

    @Test("event insertion failure rolls back the decision and a retry commits both")
    func decisionAndEventAreAtomicAndRetryable() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-rewind-atomic"
        let podcastId = "pod-rewind-atomic"
        let windowId = "win-rewind-atomic"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(
            makeAdWindow(id: windowId, assetId: assetId, startTime: 33)
        )
        try await store.execForTesting("""
            CREATE TRIGGER fail_listen_rewind_insert
            BEFORE INSERT ON ad_listen_rewinds
            BEGIN
                SELECT RAISE(ABORT, 'injected listen rewind failure');
            END
            """)

        let service = makeService(store: store)
        var rejected = false
        do {
            try await service.recordListenRewind(
                windowId: windowId,
                analysisAssetId: assetId,
                podcastId: podcastId
            )
        } catch {
            rejected = true
        }
        #expect(rejected)
        #expect(
            try await store.fetchAdWindow(id: windowId)?.decisionState
                == AdDecisionState.applied.rawValue,
            "the decision flip must roll back when its event receipt fails"
        )
        #expect(try await store.fetchListenRewinds(forAssetId: assetId).isEmpty)

        try await store.execForTesting(
            "DROP TRIGGER fail_listen_rewind_insert"
        )
        try await service.recordListenRewind(
            windowId: windowId,
            analysisAssetId: assetId,
            podcastId: podcastId
        )

        #expect(
            try await store.fetchAdWindow(id: windowId)?.decisionState
                == AdDecisionState.reverted.rawValue
        )
        #expect(
            try await store.fetchListenRewinds(forAssetId: assetId).count == 1
        )
    }

    @Test("rewind on a missing-profile podcast still persists an event row")
    func missingProfileStillPersistsEvent() async throws {
        // recordListenRewind's trust-side delegate
        // (TrustScoringService.recordWeakFalseSkipSignal) early-returns
        // when no PodcastProfile row exists (it doesn't lazy-create
        // one). The persistence log MUST still capture the tap, because
        // q45f's gate cares about the *event*, not the profile-mutation
        // side effect.
        let store = try await makeTestStore()
        let assetId = "asset-rewind-noprofile"
        let podcastId = "pod-no-profile-yet"
        let windowId = "win-noprofile"

        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(makeAdWindow(id: windowId, assetId: assetId, startTime: 45.0))

        let service = makeService(store: store)
        try await service.recordListenRewind(
            windowId: windowId,
            analysisAssetId: assetId,
            podcastId: podcastId
        )

        let rows = try await store.fetchListenRewinds(forAssetId: assetId)
        #expect(rows.count == 1,
                "missing-profile rewind must still persist an event row for q45f gate replay")
        #expect(rows.first?.time == 45.0)
    }

    @Test(
        "post-commit Listen trust calibration does not gate completion",
        .timeLimit(.minutes(1))
    )
    func listenTrustCalibrationIsNonBlocking() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-rewind-nonblocking"
        let podcastId = "pod-rewind-nonblocking"
        let windowId = "win-rewind-nonblocking"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertAdWindow(
            makeAdWindow(id: windowId, assetId: assetId, startTime: 22)
        )
        let service = makeService(store: store)
        let trustGate = ListenTrustGate()
        await service._setListenRewindTrustHandlerForTesting { _ in
            await trustGate.hold()
        }
        let primaryResultGate = ListenTrustGate()
        let response = Task {
            try await service.recordListenRewind(
                windowId: windowId,
                analysisAssetId: assetId,
                podcastId: podcastId
            )
            await primaryResultGate.hold()
        }

        await trustGate.waitUntilStarted()
        await primaryResultGate.waitUntilStarted()
        #expect(
            try await store.fetchListenRewinds(forAssetId: assetId).count == 1,
            "The durable Listen receipt must exist while trust is held"
        )
        await trustGate.release()
        await primaryResultGate.release()
        try await response.value
    }
}
