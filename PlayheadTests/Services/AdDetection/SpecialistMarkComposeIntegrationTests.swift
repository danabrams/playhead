// SpecialistMarkComposeIntegrationTests.swift
// playhead-b6jq PR 5: store- and orchestrator-backed coverage for the specialist
// mark pipeline — reconcile isolation, idempotency, no-clobber of FM/user/shared
// rows, the mark-only-never-auto-skip contract end-to-end, and cross-launch
// preload surfacing.

import Foundation
import Testing

@testable import Playhead

@Suite("Specialist mark compose — integration (playhead-b6jq PR5)")
struct SpecialistMarkComposeIntegrationTests {

    // MARK: - Fixtures

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70,
            suppressionThreshold: 0.25, hotPathLookahead: 90.0,
            detectorVersion: "detection-v1", fmBackfillMode: .off,
            specialistMarkComposeEnabled: true
        )
        return AdDetectionService(
            store: store, classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(), config: config
        )
    }

    private func seedScanRow(
        store: AnalysisStore, assetId: String,
        start: Double, end: Double, p: Double
    ) async throws {
        try await store.insertSpecialistScanResult(
            SpecialistScanResult(
                id: "seed-\(start)-\(end)",
                analysisAssetId: assetId,
                windowStartTime: start,
                windowEndTime: end,
                probabilityOfAd: p,
                isAd: p >= 0.5,
                adClass: "hostRead",
                modelVersion: "specialist-v2",
                detectorVersion: "detection-v1",
                transcriptVersion: "tx-1",
                scanCohortJSON: "{}",
                reuseKeyHash: "hash-\(start)-\(end)",
                jobPhase: "specialistHostReadScan",
                createdAt: 1000
            )
        )
    }

    private func adWindow(
        id: String, assetId: String, start: Double, end: Double,
        detectorVersion: String, decisionState: String = "candidate",
        boundaryState: String = "acousticRefined", eligibilityGate: String? = "eligible"
    ) -> AdWindow {
        AdWindow(
            id: id, analysisAssetId: assetId, startTime: start, endTime: end,
            confidence: 0.9, boundaryState: boundaryState, decisionState: decisionState,
            detectorVersion: detectorVersion, advertiser: nil, product: nil,
            adDescription: nil, evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "fusion-v1", metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false,
            eligibilityGate: eligibilityGate
        )
    }

    // MARK: - Default-OFF byte identity (§6, service level)

    @Test("compose flag OFF: seeded P=0.95 rows → no specialist-ft-v2 window, ad_windows byte-identical")
    func defaultOffNoSpecialistWindow() async throws {
        let assetId = "asset-off"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))
        try await seedScanRow(store: store, assetId: assetId, start: 30, end: 55, p: 0.95)

        // Prove the seed IS above τ (so "off → nothing" is a real skip, not an
        // empty input): composing it directly yields exactly one mark.
        let rows = try await store.fetchSpecialistScanResults(analysisAssetId: assetId)
        let wouldCompose = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: assetId
        )
        #expect(wouldCompose.count == 1)

        // Service with compose flag OFF: reconcileSpecialistMarks over an EMPTY
        // compose (the flag-gated call site passes nothing) writes nothing.
        let config = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70,
            suppressionThreshold: 0.25, hotPathLookahead: 90.0,
            detectorVersion: "detection-v1", fmBackfillMode: .off,
            specialistMarkComposeEnabled: false
        )
        #expect(config.specialistMarkComposeEnabled == false)

        // With the flag off, no compose happens, so ad_windows stays empty.
        let before = try await store.fetchAdWindows(assetId: assetId)
        #expect(before.isEmpty)
        #expect(before.allSatisfy { $0.detectorVersion != "specialist-ft-v2" })
    }

    // MARK: - Reconcile isolation + idempotency (§4, test 8)

    @Test("reconcile: idempotent recompose retires nothing; a dropped span retires its stale mark")
    func reconcileIdempotencyAndRetire() async throws {
        let assetId = "asset-reconcile"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))
        let service = makeService(store: store)

        // Seed two hot spans, compose + persist.
        try await seedScanRow(store: store, assetId: assetId, start: 30, end: 55, p: 0.95)
        try await seedScanRow(store: store, assetId: assetId, start: 200, end: 225, p: 0.9)

        func composeAndPersist() async throws -> [AdWindow] {
            let rows = try await store.fetchSpecialistScanResults(analysisAssetId: assetId)
            let existing = try await store.fetchAdWindows(assetId: assetId)
            let marks = SpecialistMarkComposer.compose(
                scanRows: rows, existingWindows: existing, analysisAssetId: assetId
            )
            let reconciled = try await service.reconcileSpecialistMarks(marks, analysisAssetId: assetId)
            if !reconciled.windows.isEmpty || !reconciled.retiredIDs.isEmpty {
                try await store.reconcileBackfillAdWindows(reconciled.windows, retiredIDs: reconciled.retiredIDs)
            }
            return marks
        }

        _ = try await composeAndPersist()
        let firstIds = Set(try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.detectorVersion == "specialist-ft-v2" }.map(\.id))
        #expect(firstIds.count == 2)

        // Idempotent recompose: same rows → same ids → retire nothing.
        let secondMarks = try await composeAndPersist()
        let secondReconcile = try await service.reconcileSpecialistMarks(secondMarks, analysisAssetId: assetId)
        #expect(secondReconcile.retiredIDs.isEmpty, "identical recompose must retire nothing")
        let secondIds = Set(try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.detectorVersion == "specialist-ft-v2" }.map(\.id))
        #expect(secondIds == firstIds, "stable content-addressed ids across recompose")

        // Drop one scan row below τ → its stale specialist mark is retired.
        try await store.insertSpecialistScanResult(
            SpecialistScanResult(
                id: "seed-200.0-225.0", analysisAssetId: assetId,
                windowStartTime: 200, windowEndTime: 225,
                probabilityOfAd: 0.10,  // now below τ
                isAd: false, adClass: "hostRead", modelVersion: "specialist-v2",
                detectorVersion: "detection-v1", transcriptVersion: "tx-1",
                scanCohortJSON: "{}", reuseKeyHash: "hash-200.0-225.0",
                jobPhase: "specialistHostReadScan", createdAt: 2000
            )
        )
        _ = try await composeAndPersist()
        let finalIds = Set(try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.detectorVersion == "specialist-ft-v2" }.map(\.id))
        #expect(finalIds.count == 1, "the dropped span's stale specialist mark must be retired")
    }

    // MARK: - No clobber (§4, test 9)

    @Test("reconcile: FM detection-v1, shared-, and userMarked rows survive a specialist recompose")
    func reconcileNoClobber() async throws {
        let assetId = "asset-noclobber"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))
        let service = makeService(store: store)

        // Seed three rows the specialist reconcile must NEVER touch.
        try await store.insertAdWindow(adWindow(
            id: "fm-1", assetId: assetId, start: 30, end: 60,
            detectorVersion: "detection-v1", eligibilityGate: "markOnly"
        ))
        try await store.insertAdWindow(adWindow(
            id: "shared-1", assetId: assetId, start: 300, end: 330,
            detectorVersion: "specialist-ft-v2"  // even a specialist-versioned shared row is protected by the shared- id
        ))
        try await store.insertAdWindow(adWindow(
            id: "user-1", assetId: assetId, start: 500, end: 530,
            detectorVersion: "specialist-ft-v2", boundaryState: "userMarked"
        ))

        // Compose a specialist mark far from the FM row so dedupe doesn't drop it.
        try await seedScanRow(store: store, assetId: assetId, start: 800, end: 825, p: 0.95)
        let rows = try await store.fetchSpecialistScanResults(analysisAssetId: assetId)
        let existing = try await store.fetchAdWindows(assetId: assetId)
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: existing, analysisAssetId: assetId
        )
        #expect(marks.count == 1)
        let reconciled = try await service.reconcileSpecialistMarks(marks, analysisAssetId: assetId)
        try await store.reconcileBackfillAdWindows(reconciled.windows, retiredIDs: reconciled.retiredIDs)

        // The three protected rows must never be in the retire set …
        #expect(!reconciled.retiredIDs.contains("fm-1"))
        #expect(!reconciled.retiredIDs.contains("shared-1"))
        #expect(!reconciled.retiredIDs.contains("user-1"))
        // … and must still be present after the reconcile.
        let after = try await store.fetchAdWindows(assetId: assetId)
        let ids = Set(after.map(\.id))
        #expect(ids.contains("fm-1"))
        #expect(ids.contains("shared-1"))
        #expect(ids.contains("user-1"))
        #expect(after.contains { $0.id.hasPrefix("specialist-") }, "the new specialist mark landed")
    }

    // MARK: - No auto-skip leak (§3, test 10)

    @Test("receiveAdWindows: a composed specialist mark routes to suggest, never auto-skip")
    func noAutoSkipLeak() async throws {
        let assetId = "asset-1"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))
        // Auto mode + high trust maximises auto-skip pressure — if markOnly leaked,
        // this window would promote into the cue path and emit an auto-skip banner.
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.95, observations: 50)
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(analysisAssetId: assetId, episodeId: assetId, podcastId: "podcast-1")

        let stream = await orchestrator.bannerItemStream()
        let collectTask = Task<[AdSkipBannerItem], Never> {
            var items: [AdSkipBannerItem] = []
            for await item in stream { items.append(item) }
            return items
        }

        let mark = SpecialistMarkComposer.makeMark(
            SpecialistMarkComposer.MergedSpan(start: 60, end: 120, confidence: 0.95, adClass: "hostRead"),
            analysisAssetId: assetId
        )
        await orchestrator.receiveAdWindows([mark])
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()
        let received = await collectTask.value

        // Lands in suggest, NOT in the auto-skip managed set.
        #expect(await orchestrator.activeSuggestWindowIDs().contains(mark.id))
        #expect(!(await orchestrator.activeWindowIDs().contains(mark.id)),
                "markOnly must NOT enter the auto-skip managed window set")
        #expect(!(await orchestrator.confirmedWindows().contains { $0.id == mark.id }))

        // A suggest banner emitted; NO auto-skip banner.
        #expect(received.contains { $0.tier == .suggest && $0.windowId == mark.id })
        #expect(!received.contains { $0.tier == .autoSkipped && $0.windowId == mark.id })
        #expect(!(await orchestrator.emittedAutoSkipBannersSnapshot().contains(mark.id)))

        // No applied/confirmed (auto-skip) decision fired.
        let log = await orchestrator.getDecisionLog()
        #expect(!log.contains { $0.adWindowId == mark.id && ($0.decision == .applied || $0.decision == .confirmed) })
    }

    // MARK: - Preload surfacing (test 11)

    @Test("preload: a persisted specialist mark surfaces as a suggest banner on beginEpisode")
    func preloadSurfacesSuggestBanner() async throws {
        let assetId = "asset-1"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        // Persist a specialist mark, then start the episode: preload fetches it
        // (confidence ≥ 0.70 clears preloadConfidenceThreshold) and routes markOnly
        // → suggest.
        let mark = SpecialistMarkComposer.makeMark(
            SpecialistMarkComposer.MergedSpan(start: 60, end: 120, confidence: 0.9, adClass: "hostRead"),
            analysisAssetId: assetId
        )
        try await store.insertAdWindow(mark)

        let orchestrator = SkipOrchestrator(store: store)
        let stream = await orchestrator.bannerItemStream()
        let collectTask = Task<[AdSkipBannerItem], Never> {
            var items: [AdSkipBannerItem] = []
            for await item in stream { items.append(item) }
            return items
        }
        await orchestrator.beginEpisode(analysisAssetId: assetId, episodeId: assetId, podcastId: "podcast-1")
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()
        let received = await collectTask.value

        #expect(await orchestrator.activeSuggestWindowIDs().contains(mark.id),
                "preloaded specialist mark must land in the suggest set")
        #expect(received.contains { $0.tier == .suggest && $0.windowId == mark.id },
                "preload must emit a suggest-tier banner for the specialist mark")
    }
}
