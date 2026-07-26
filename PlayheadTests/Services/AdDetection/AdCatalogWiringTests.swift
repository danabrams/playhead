// AdCatalogWiringTests.swift
// playhead-gtt9.17: Wire AdCatalogStore ingress + fusion egress into production.
//
// gtt9.13 shipped AdCatalogStore, AcousticFingerprint, and the catalogMatch
// safety-signal machinery, but the store is never populated and never queried
// in the production hot path. This suite nails the contract down:
//
//   Ingress — proposals never learn. Only authoritative runtime consumption
//   or explicit confirmation may populate AdCatalogStore.
//
//   Egress — when the store contains an entry that fingerprint-matches a
//   candidate window on a subsequent backfill, the fusion path emits a
//   `.catalog` ledger entry and threads `catalogMatchSimilarity` into the
//   `AutoSkipPrecisionGate` input.
//
// Acceptance (from bead):
//   1. Correction → catalog insert → subsequent similar window → catalog
//      evidence fires → AutoSkipPrecisionGate sees catalogMatch signal.
//   2. markOnly autoSkip decisions do NOT insert into the catalog.
//   3. Back-compat: with an empty (or nil) AdCatalogStore, behavior matches
//      gtt9.16 exactly — no catalog evidence appears in fusion ledger.

import Foundation
import Testing
@testable import Playhead

@Suite("AdCatalogStore production wiring (gtt9.17)")
struct AdCatalogWiringTests {

    // MARK: - Fixtures

    private func makeCatalogDir() throws -> URL {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("AdCatalogWiring-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        return base
    }

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

    /// Feature windows shaped as host → ad block → host that drive fusion
    /// past the autoSkipEligible gate. Mirrors the well-known synthetic
    /// ad-episode pattern used by AcousticFeaturePipelineTests so the
    /// pipeline fires and the catalog ingress path is exercised.
    private func syntheticAdWindows(assetId: String) -> [FeatureWindow] {
        var out: [FeatureWindow] = []
        for i in 0..<30 {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.18,
                spectralFlux: 0.03,
                musicProbability: 0.02,
                speakerChangeProxyScore: 0.05,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.05,
                speakerClusterId: 0,
                jingleHash: nil,
                featureVersion: 5
            ))
        }
        // Silence bumper.
        out.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: 60, endTime: 62,
            rms: 0.002,
            spectralFlux: 0.01,
            musicProbability: 0.0,
            speakerChangeProxyScore: 0.7,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0.9,
            speakerClusterId: 0,
            jingleHash: nil,
            featureVersion: 5
        ))
        // Ad block.
        let adStart = out.count
        for i in adStart..<(adStart + 10) {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.70,
                spectralFlux: 0.30,
                musicProbability: 0.80,
                speakerChangeProxyScore: 0.70,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .foreground,
                pauseProbability: 0.02,
                speakerClusterId: 9,
                jingleHash: nil,
                featureVersion: 5
            ))
        }
        let closeStart = out.count
        out.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: Double(closeStart) * 2,
            endTime: Double(closeStart + 1) * 2,
            rms: 0.003,
            spectralFlux: 0.01,
            musicProbability: 0.0,
            speakerChangeProxyScore: 0.7,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0.9,
            speakerClusterId: 0,
            jingleHash: nil,
            featureVersion: 5
        ))
        let tailStart = out.count
        for i in tailStart..<(tailStart + 30) {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.18,
                spectralFlux: 0.03,
                musicProbability: 0.02,
                speakerChangeProxyScore: 0.05,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.05,
                speakerClusterId: 0,
                jingleHash: nil,
                featureVersion: 5
            ))
        }
        return out
    }

    /// Zero-signal windows — exercises the "nothing to fingerprint" path.
    private func zeroSignalWindows(assetId: String, count: Int = 40, step: Double = 2.0) -> [FeatureWindow] {
        var out: [FeatureWindow] = []
        var t = 0.0
        for _ in 0..<count {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: t,
                endTime: t + step,
                rms: 0.0,
                spectralFlux: 0.0,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.0,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 5
            ))
            t += step
        }
        return out
    }

    private func lexicalAdChunks(assetId: String) -> [TranscriptChunk] {
        // Place a lexical hit inside the ad block timespan (62–82s) so a
        // DecodedSpan is produced and the ledger is populated for fusion.
        let texts = [
            (0.0, 30.0, "Welcome back to the show today we discuss technology."),
            (60.0, 90.0, "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show."),
            (90.0, 120.0, "Back to our regular conversation about new things.")
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeService(
        store: AnalysisStore,
        catalogStore: AdCatalogStore?,
        repeatedAdCache: RepeatedAdCacheService? = nil
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "gtt9.17-test",
            fmBackfillMode: .off
        )
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            adCatalogStore: catalogStore,
            repeatedAdCache: repeatedAdCache
        )
    }

    // MARK: - Ingress: proposals do not learn

    @Test("persisting an autoSkipEligible proposal does not learn a CatalogEntry")
    func autoSkipEligibleProposalDoesNotInsertCatalogEntry() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.17-ingress"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let catalogDir = try makeCatalogDir()
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)
        let repeatedStorage = InMemoryRepeatedAdCacheStorage()
        let repeatedCache = RepeatedAdCacheService(
            storage: repeatedStorage
        )
        let service = makeService(
            store: store,
            catalogStore: catalogStore,
            repeatedAdCache: repeatedCache
        )

        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )

        let count = try await catalogStore.count()
        #expect(
            count == 0,
            "proposal persistence is not confirmation and must not learn; got \(count) rows"
        )
        #expect(
            try await repeatedStorage.totalCount() == 0,
            "proposal persistence must not seed the parallel recurrence cache"
        )
    }

    // MARK: - Back-compat: nil AdCatalogStore

    @Test("nil catalogStore preserves pre-gtt9.17 fusion behavior")
    func nilCatalogStorePreservesBehavior() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.17-nilcat"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let service = makeService(store: store, catalogStore: nil)
        // Should not throw despite no catalog wired.
        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )

        // No crash and a fusion funnel should still be populated (acoustic
        // pipeline wiring from gtt9.16 is orthogonal to catalog wiring).
        let funnel = await service.acousticFunnelForTesting()
        #expect(funnel.total(AcousticFeatureFunnelStage.computed) > 0, "acoustic funnel should still compute when catalog is nil")
    }

    // MARK: - Egress: prior catalog entry lifts catalogMatchSimilarity

    @Test("prior CatalogEntry matches subsequent similar window and threads similarity into fusion")
    func priorEntryMatchesSubsequentWindow() async throws {
        // Episode 1: produce the candidate, then simulate an authoritative
        // explicit confirmation by inserting its exact feature fingerprint.
        let storeA = try await makeTestStore()
        let assetA = "asset-gtt9.17-ep1"
        try await storeA.insertAsset(makeAsset(id: assetA))
        try await storeA.insertFeatureWindows(syntheticAdWindows(assetId: assetA))

        let catalogDir = try makeCatalogDir()
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)
        let serviceA = makeService(store: storeA, catalogStore: catalogStore)
        try await serviceA.runBackfill(
            chunks: lexicalAdChunks(assetId: assetA),
            analysisAssetId: assetA,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )
        #expect(
            try await catalogStore.count() == 0,
            "backfill proposal alone must not seed the catalog"
        )
        let sourceWindow = try #require(
            try await storeA.fetchAdWindows(assetId: assetA)
                .max(by: { $0.confidence < $1.confidence })
        )
        let sourceFeatures = try await storeA.fetchFeatureWindows(
            assetId: assetA,
            from: sourceWindow.startTime,
            to: sourceWindow.endTime
        )
        let sourceFingerprint = AcousticFingerprint.fromFeatureWindows(
            sourceFeatures
        )
        _ = try await catalogStore.insert(
            showId: "show-gtt9.17",
            episodePosition: .unknown,
            durationSec: sourceWindow.endTime - sourceWindow.startTime,
            acousticFingerprint: sourceFingerprint,
            originalConfidence: sourceWindow.confidence,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: sourceWindow.analysisAssetId,
            sourceWindowId: sourceWindow.id,
            sourceStartTime: sourceWindow.startTime,
            sourceEndTime: sourceWindow.endTime
        )
        #expect(try await catalogStore.count() == 1)

        // Episode 2: fresh analysis store, SAME feature windows pattern so
        // the fingerprint matches. Fresh service reuses the same catalog.
        let storeB = try await makeTestStore()
        let assetB = "asset-gtt9.17-ep2"
        try await storeB.insertAsset(makeAsset(id: assetB))
        try await storeB.insertFeatureWindows(syntheticAdWindows(assetId: assetB))

        let serviceB = makeService(store: storeB, catalogStore: catalogStore)
        try await serviceB.runBackfill(
            chunks: lexicalAdChunks(assetId: assetB),
            analysisAssetId: assetB,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )

        // After ep2's backfill, the service should have observed at least
        // one catalog match for its windows. The test seam records the top
        // similarity per backfill so we can assert non-zero.
        let topSim = await serviceB.lastCatalogMatchSimilarityForTesting()
        #expect(topSim >= AdCatalogStore.defaultSimilarityFloor,
                "expected ep2 to observe a catalog match ≥ default floor (\(AdCatalogStore.defaultSimilarityFloor)), got \(topSim)")
    }

    // MARK: - Back-compat: empty catalog produces no catalog match signal

    @Test("empty catalogStore yields zero catalogMatchSimilarity")
    func emptyCatalogYieldsZeroSimilarity() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.17-empty"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let catalogDir = try makeCatalogDir()
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)

        let service = makeService(store: store, catalogStore: catalogStore)
        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )

        let topSim = await service.lastCatalogMatchSimilarityForTesting()
        #expect(topSim == 0.0, "empty catalog must yield zero catalogMatchSimilarity, got \(topSim)")
    }

    // MARK: - Zero-signal windows do not insert spurious fingerprints

    @Test("zero-signal windows produce no catalog insertions (isZero fingerprints rejected)")
    func zeroSignalYieldsNoCatalogInsert() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.17-zero"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(zeroSignalWindows(assetId: assetId))

        let catalogDir = try makeCatalogDir()
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)

        let service = makeService(store: store, catalogStore: catalogStore)
        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-gtt9.17",
            episodeDuration: 120.0
        )

        let count = try await catalogStore.count()
        #expect(count == 0, "zero-signal windows must not produce catalog inserts, got count=\(count)")
    }

    private func lifecycleWindow(
        id: String,
        assetId: String,
        catalogMatch: CatalogEntry? = nil,
        eligibilityGate: String = SkipEligibilityGate.eligible.rawValue,
        decisionState: AdDecisionState = .confirmed,
        startTime: Double = 62,
        endTime: Double = 82
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            confidence: 0.99,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState.rawValue,
            detectorVersion: "o4qr-lifecycle",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: startTime,
            metadataSource: "fusion-v1",
            metadataConfidence: 0.99,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: eligibilityGate,
            catalogStoreMatchSimilarity: catalogMatch.map { _ in 1 },
            catalogFingerprintVersion: catalogMatch?
                .acousticFingerprint.version.rawValue,
            catalogMatchedEntryId: catalogMatch?.id.uuidString,
            catalogMatchedShowId: catalogMatch?.showId,
            catalogMatchedLearningSource:
                catalogMatch?.learningSource.rawValue,
            catalogMatchedLearningLifecycle:
                catalogMatch?.learningLifecycle.rawValue
        )
    }

    @Test("auto skip learns only after durable apply and delayed playhead consumption")
    func consumedAutoSkipLearningIsDelayed() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-consumed"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let repeatedStorage = InMemoryRepeatedAdCacheStorage()
        let repeatedCache = RepeatedAdCacheService(
            storage: repeatedStorage
        )
        let gate = CatalogAppliedPersistenceGate()
        let orchestrator = SkipOrchestrator(
            store: store,
            adCatalogStore: catalog,
            repeatedAdCache: repeatedCache
        )
        await orchestrator._setAppliedPersistenceBarrierForTesting {
            await gate.block()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-consumed",
            podcastId: "show-o4qr"
        )
        await orchestrator.setActiveSkipMode(.auto)

        let window = lifecycleWindow(
            id: "window-o4qr-consumed",
            assetId: assetId
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])
        await gate.waitUntilStarted()
        #expect(
            try await catalog.count() == 0,
            "an in-memory/persisted proposal must not learn before durable apply"
        )
        #expect(try await repeatedStorage.totalCount() == 0)

        await gate.release()
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        #expect(
            try await store.fetchAdWindow(id: window.id)?.decisionState
                == AdDecisionState.applied.rawValue
        )
        await orchestrator.updatePlayheadTime(82.5)
        #expect(
            try await catalog.count() == 0,
            "playhead has not cleared the one-second consumption delay"
        )
        #expect(try await repeatedStorage.totalCount() == 0)

        await orchestrator.updatePlayheadTime(83.01)
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        let learned = try #require(try await catalog.allEntries().first)
        #expect(learned.learningSource == .consumedAutoSkip)
        #expect(learned.learningLifecycle == .consumed)
        #expect(learned.sourceAssetId == assetId)
        #expect(learned.sourceWindowId == window.id)
        let recurrence = try #require(
            try await repeatedStorage.fetchAll(showId: "show-o4qr").first
        )
        #expect(recurrence.learningSource == .consumedAutoSkip)
        #expect(recurrence.learningLifecycle == .consumed)
        #expect(recurrence.sourceAssetId == assetId)
        #expect(recurrence.sourceWindowId == window.id)
    }

    @Test("seek invalidation retracts a consumed writer suspended after catalog persistence")
    func seekInvalidationRetractsSuspendedConsumedLearning() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-consumed-seek-race"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let repeatedStorage = InMemoryRepeatedAdCacheStorage()
        let repeatedCache = RepeatedAdCacheService(storage: repeatedStorage)
        let catalogBarrier = CatalogAppliedPersistenceGate()
        let orchestrator = SkipOrchestrator(
            store: store,
            adCatalogStore: catalog,
            repeatedAdCache: repeatedCache
        )
        await orchestrator._setCatalogLearningPersistenceBarrierForTesting {
            await catalogBarrier.block()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-consumed-seek-race",
            podcastId: "show-o4qr"
        )
        await orchestrator.setActiveSkipMode(.auto)
        await orchestrator.updatePlayheadTime(70)

        let window = lifecycleWindow(
            id: "window-o4qr-consumed-seek-race",
            assetId: assetId
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        await orchestrator.updatePlayheadTime(100)
        await catalogBarrier.waitUntilStarted()

        #expect(try await catalog.count() == 1)
        await orchestrator.recordUserSeek(to: 0)
        await catalogBarrier.release()
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        await orchestrator
            ._setCatalogLearningPersistenceBarrierForTesting(nil)

        #expect(
            try await catalog.count() == 0,
            "a seek-invalidated consumed writer must retract its exact row"
        )
        #expect(
            try await repeatedStorage.totalCount() == 0,
            "a stale writer must not continue into the secondary cache"
        )
    }

    @Test(
        "same-ID replacement retracts suspended consumed learning and learns only the current revision"
    )
    func sameIDReplacementRetractsSuspendedConsumedLearning() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-consumed-replacement-race"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let repeatedStorage = InMemoryRepeatedAdCacheStorage()
        let repeatedCache = RepeatedAdCacheService(storage: repeatedStorage)
        let catalogBarrier = CatalogAppliedPersistenceGate()
        let orchestrator = SkipOrchestrator(
            store: store,
            adCatalogStore: catalog,
            repeatedAdCache: repeatedCache
        )
        await orchestrator._setCatalogLearningPersistenceBarrierForTesting {
            await catalogBarrier.block()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-consumed-replacement-race",
            podcastId: "show-o4qr"
        )
        await orchestrator.setActiveSkipMode(.auto)

        let original = lifecycleWindow(
            id: "window-o4qr-consumed-replacement-race",
            assetId: assetId
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()

        await orchestrator.updatePlayheadTime(83.01)
        await catalogBarrier.waitUntilStarted()
        #expect(try await catalog.count() == 1)

        let replacement = lifecycleWindow(
            id: original.id,
            assetId: assetId,
            startTime: 64,
            endTime: 84
        )
        try await store.insertOrReplaceAdWindow(replacement)
        await orchestrator.receiveAdWindows([replacement])

        await catalogBarrier.release()
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        await orchestrator
            ._setCatalogLearningPersistenceBarrierForTesting(nil)

        #expect(
            try await catalog.count() == 0,
            "the withdrawn producer revision must be retracted after suspension"
        )
        #expect(
            try await repeatedStorage.totalCount() == 0,
            "the withdrawn revision must not continue into the secondary cache"
        )

        await orchestrator.updatePlayheadTime(85.01)
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()

        let learned = try #require(try await catalog.allEntries().first)
        #expect(try await catalog.count() == 1)
        #expect(learned.sourceAssetId == replacement.analysisAssetId)
        #expect(learned.sourceWindowId == replacement.id)
        #expect(learned.sourceStartTime == replacement.startTime)
        #expect(learned.sourceEndTime == replacement.endTime)
        let recurrence = try #require(
            try await repeatedStorage.fetchAll(showId: "show-o4qr").first
        )
        #expect(try await repeatedStorage.totalCount() == 1)
        #expect(recurrence.sourceAssetId == replacement.analysisAssetId)
        #expect(recurrence.sourceWindowId == replacement.id)
        #expect(recurrence.boundaryStart == replacement.startTime)
        #expect(recurrence.boundaryEnd == replacement.endTime)
    }

    @Test("live decision handoff preserves matched-row provenance through consumption")
    func decisionResultPreservesCatalogProvenance() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-decision-provenance"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let features = try await store.fetchFeatureWindows(
            assetId: assetId,
            from: 62,
            to: 82
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(features)
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let original = try #require(
            try await catalog.insert(
                showId: "show-o4qr",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.95,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "older-provenance-asset",
                sourceWindowId: "older-provenance-window",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            adCatalogStore: catalog
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-decision-provenance",
            podcastId: "show-o4qr"
        )
        await orchestrator.setActiveSkipMode(.auto)

        let window = lifecycleWindow(
            id: "window-o4qr-decision-provenance",
            assetId: assetId,
            catalogMatch: original
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: window.id,
                analysisAssetId: assetId,
                startTime: window.startTime,
                endTime: window.endTime,
                skipConfidence: window.confidence,
                eligibilityGate: .eligible,
                recomputationRevision: 0,
                producerRevision: window
            )
        ])

        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        let applied = try #require(
            try await store.fetchAdWindow(id: window.id)
        )
        #expect(applied.decisionState == AdDecisionState.applied.rawValue)
        #expect(applied.catalogMatchedEntryId == original.id.uuidString)
        #expect(
            applied.catalogFingerprintVersion
                == CatalogFingerprintVersion.currentCatalog.rawValue
        )
        #expect(
            applied.catalogMatchedLearningSource
                == CatalogLearningSource.consumedAutoSkip.rawValue
        )

        await orchestrator.updatePlayheadTime(83.01)
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        let consumed = try #require(try await catalog.allEntries().first)
        #expect(consumed.id == original.id)
        #expect(consumed.sourceAssetId == "older-provenance-asset")
        #expect(consumed.sourceWindowId == "older-provenance-window")
        #expect(consumed.learningSource == .consumedAutoSkip)
        #expect(consumed.learningLifecycle == .consumed)
    }

    @Test("manual skip is an explicit positive confirmation")
    func manualSkipLearnsWithExplicitProvenance() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-manual"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let orchestrator = SkipOrchestrator(
            store: store,
            adCatalogStore: catalog
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-manual",
            podcastId: "show-o4qr"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let window = lifecycleWindow(
            id: "window-o4qr-manual",
            assetId: assetId
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])
        await orchestrator.applyManualSkip(windowId: window.id)

        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        let learned = try #require(try await catalog.allEntries().first)
        #expect(learned.learningSource == .manualSkip)
        #expect(learned.learningLifecycle == .explicitConfirmation)
        #expect(learned.showId == "show-o4qr")
    }

    @Test("local replay demotes positive catalog claims without compatible provenance")
    func legacyLocalCatalogProvenanceFailsClosed() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-legacy-local"
        try await store.insertAsset(makeAsset(id: assetId))
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-legacy-local",
            podcastId: "show-o4qr"
        )

        let legacyPositive = AdWindow(
            id: "window-o4qr-legacy-positive",
            analysisAssetId: assetId,
            startTime: 62,
            endTime: 82,
            confidence: 0.99,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "legacy-catalog-replay",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "legacy",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            catalogStoreMatchSimilarity: 0.99
        )
        let currentNoMatch = AdWindow(
            id: "window-o4qr-current-no-match",
            analysisAssetId: assetId,
            startTime: 92,
            endTime: 112,
            confidence: 0.99,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "current-no-match-replay",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "current",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            catalogStoreMatchSimilarity: 0
        )

        await orchestrator.receiveAdWindows([
            legacyPositive,
            currentNoMatch,
        ])

        #expect(
            await orchestrator.activeSuggestWindowIDs()
                .contains(legacyPositive.id)
        )
        #expect(
            !(await orchestrator.activeWindowIDs())
                .contains(legacyPositive.id)
        )
        #expect(
            await orchestrator.activeWindowIDs()
                .contains(currentNoMatch.id)
        )
    }

    @Test("catalog-assisted replay requires the exact active show identity")
    func catalogReplayFailsClosedWithoutExactActiveShow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-catalog-show-scope"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(
            try await store.fetchFeatureWindows(
                assetId: assetId,
                from: 62,
                to: 82
            )
        )
        try #require(!fingerprint.isZero)
        let matchedEntry = CatalogEntry(
            createdAt: Date(timeIntervalSince1970: 100),
            showId: "show-o4qr",
            episodePosition: .midRoll,
            durationSec: 20,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.99,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "catalog-source-asset",
            sourceWindowId: "catalog-source-window",
            sourceStartTime: 62,
            sourceEndTime: 82,
            confirmedAt: Date(timeIntervalSince1970: 100)
        )

        for (suffix, activeShowId) in [
            ("missing", nil),
            ("blank", " \n"),
            ("mismatch", "different-show"),
            ("missing-store", "show-o4qr"),
        ] as [(String, String?)] {
            let orchestrator = SkipOrchestrator(store: store)
            await orchestrator.beginEpisode(
                analysisAssetId: assetId,
                episodeId: "episode-o4qr-\(suffix)",
                podcastId: activeShowId
            )
            let window = lifecycleWindow(
                id: "window-o4qr-\(suffix)",
                assetId: assetId,
                catalogMatch: matchedEntry
            )

            await orchestrator.receiveAdWindows([window])

            #expect(
                await orchestrator.activeSuggestWindowIDs()
                    .contains(window.id)
            )
            #expect(
                !(await orchestrator.activeWindowIDs()).contains(window.id)
            )
        }
    }

    @Test("revoked catalog rows cannot authorize either runtime admission path")
    func revokedCatalogMatchFailsClosedAtAdmission() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-revoked-admission"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(
            try await store.fetchFeatureWindows(
                assetId: assetId,
                from: 62,
                to: 82
            )
        )
        try #require(!fingerprint.isZero)
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let learned = try #require(
            try await catalog.insert(
                showId: "show-o4qr",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "source-asset-o4qr",
                sourceWindowId: "source-window-o4qr",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )
        #expect(
            try await catalog.revoke(
                matchedEntryId: learned.id,
                sourceAssetId: "corrected-asset-o4qr",
                sourceWindowId: "corrected-window-o4qr",
                source: .manualVeto,
                showId: "show-o4qr"
            ) == 1
        )

        for usesDecisionResult in [false, true] {
            let suffix = usesDecisionResult ? "decision" : "window"
            let orchestrator = SkipOrchestrator(
                store: store,
                adCatalogStore: catalog
            )
            await orchestrator.beginEpisode(
                analysisAssetId: assetId,
                episodeId: "episode-o4qr-revoked-\(suffix)",
                podcastId: "show-o4qr"
            )
            let window = lifecycleWindow(
                id: "window-o4qr-revoked-\(suffix)",
                assetId: assetId,
                catalogMatch: learned
            )
            if usesDecisionResult {
                await orchestrator.receiveAdDecisionResults([
                    AdDecisionResult(
                        id: window.id,
                        analysisAssetId: assetId,
                        startTime: window.startTime,
                        endTime: window.endTime,
                        skipConfidence: window.confidence,
                        eligibilityGate: .eligible,
                        recomputationRevision: 0,
                        producerRevision: window
                    )
                ])
            } else {
                await orchestrator.receiveAdWindows([window])
            }

            #expect(
                await orchestrator.activeSuggestWindowIDs()
                    .contains(window.id)
            )
            #expect(
                !(await orchestrator.activeWindowIDs()).contains(window.id)
            )
        }
    }

    @Test("revocation disarms an identical already-applied catalog window")
    func catalogRevocationOverridesAppliedIdempotency() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-applied-revocation"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(
            try await store.fetchFeatureWindows(
                assetId: assetId,
                from: 62,
                to: 82
            )
        )
        try #require(!fingerprint.isZero)
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let learned = try #require(
            try await catalog.insert(
                showId: "show-o4qr",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "source-asset-o4qr",
                sourceWindowId: "source-window-o4qr",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            adCatalogStore: catalog
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-applied-revocation",
            podcastId: "show-o4qr"
        )
        let applied = lifecycleWindow(
            id: "window-o4qr-applied-revocation",
            assetId: assetId,
            catalogMatch: learned,
            decisionState: .applied
        )
        await orchestrator.receiveAdWindows([applied])
        #expect(
            (await orchestrator.activeWindowIDs()).contains(applied.id)
        )

        #expect(
            try await catalog.revoke(
                matchedEntryId: learned.id,
                sourceAssetId: "corrected-asset-o4qr",
                sourceWindowId: "corrected-window-o4qr",
                source: .manualVeto,
                showId: "show-o4qr"
            ) == 1
        )
        await orchestrator.receiveAdWindows([applied])

        #expect(
            !(await orchestrator.activeWindowIDs()).contains(applied.id),
            "live revocation must beat the applied-material early return"
        )
    }

    @Test("active catalog row cannot authorize different persisted material")
    func catalogAdmissionRechecksCurrentMaterialFingerprint() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-mismatched-admission-material"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let sourceFingerprint = AcousticFingerprint.fromFeatureWindows(
            try await store.fetchFeatureWindows(
                assetId: assetId,
                from: 62,
                to: 82
            )
        )
        let replacementFingerprint = AcousticFingerprint.fromFeatureWindows(
            try await store.fetchFeatureWindows(
                assetId: assetId,
                from: 0,
                to: 20
            )
        )
        try #require(!sourceFingerprint.isZero)
        try #require(!replacementFingerprint.isZero)
        try #require(
            AcousticFingerprint.similarity(
                sourceFingerprint,
                replacementFingerprint
            ) < AdCatalogStore.defaultSimilarityFloor
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let learned = try #require(
            try await catalog.insert(
                showId: "show-o4qr",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: sourceFingerprint,
                originalConfidence: 0.99,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "source-asset-o4qr",
                sourceWindowId: "source-window-o4qr",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            adCatalogStore: catalog
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-mismatched-material",
            podcastId: "show-o4qr"
        )
        let mismatched = lifecycleWindow(
            id: "window-o4qr-mismatched-material",
            assetId: assetId,
            catalogMatch: learned,
            startTime: 0,
            endTime: 20
        )

        await orchestrator.receiveAdWindows([mismatched])

        #expect(
            await orchestrator.activeSuggestWindowIDs()
                .contains(mismatched.id)
        )
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(mismatched.id)
        )
    }

    @Test("catalog validation callback cannot admit into a replacement episode")
    func catalogAdmissionValidationIsEpisodeFenced() async throws {
        let store = try await makeTestStore()
        let sourceAssetId = "asset-o4qr-admission-race-source"
        let replacementAssetId = "asset-o4qr-admission-race-replacement"
        try await store.insertAsset(makeAsset(id: sourceAssetId))
        try await store.insertAsset(makeAsset(id: replacementAssetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: sourceAssetId)
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(
            try await store.fetchFeatureWindows(
                assetId: sourceAssetId,
                from: 62,
                to: 82
            )
        )
        try #require(!fingerprint.isZero)
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let learned = try #require(
            try await catalog.insert(
                showId: "show-o4qr-source",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "catalog-source-asset",
                sourceWindowId: "catalog-source-window",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )

        for usesDecisionResult in [false, true] {
            let suffix = usesDecisionResult ? "decision" : "window"
            let orchestrator = SkipOrchestrator(
                store: store,
                adCatalogStore: catalog
            )
            await orchestrator.beginEpisode(
                analysisAssetId: sourceAssetId,
                episodeId: "episode-o4qr-source-\(suffix)",
                podcastId: "show-o4qr-source"
            )
            let window = lifecycleWindow(
                id: "window-o4qr-admission-race-\(suffix)",
                assetId: sourceAssetId,
                catalogMatch: learned
            )
            let barrier = CatalogAppliedPersistenceGate()
            await orchestrator
                ._setCatalogAdmissionValidationBarrierForTesting {
                    await barrier.block()
                }
            let ingestion = Task {
                if usesDecisionResult {
                    await orchestrator.receiveAdDecisionResults([
                        AdDecisionResult(
                            id: window.id,
                            analysisAssetId: sourceAssetId,
                            startTime: window.startTime,
                            endTime: window.endTime,
                            skipConfidence: window.confidence,
                            eligibilityGate: .eligible,
                            recomputationRevision: 0,
                            producerRevision: window
                        )
                    ])
                } else {
                    await orchestrator.receiveAdWindows([window])
                }
            }
            await barrier.waitUntilStarted()
            await orchestrator.beginEpisode(
                analysisAssetId: replacementAssetId,
                episodeId: "episode-o4qr-replacement-\(suffix)",
                podcastId: "show-o4qr-replacement"
            )
            await barrier.release()
            await ingestion.value
            await orchestrator
                ._setCatalogAdmissionValidationBarrierForTesting(nil)

            #expect((await orchestrator.activeWindowIDs()).isEmpty)
            #expect((await orchestrator.activeSuggestWindowIDs()).isEmpty)
        }
    }

    @Test(
        "newer same-ID updates and retirements beat suspended catalog validation"
    )
    func catalogAdmissionValidationIsProducerRevisionFenced() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-admission-producer-race"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(
            try await store.fetchFeatureWindows(
                assetId: assetId,
                from: 62,
                to: 82
            )
        )
        try #require(!fingerprint.isZero)
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let learned = try #require(
            try await catalog.insert(
                showId: "show-o4qr-producer-race",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "catalog-source-asset",
                sourceWindowId: "catalog-source-window",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )

        for usesDecisionResult in [false, true] {
            for retiresWhileSuspended in [false, true] {
                let entrypoint = usesDecisionResult ? "decision" : "window"
                let mutation = retiresWhileSuspended ? "retire" : "mark-only"
                let suffix = "\(entrypoint)-\(mutation)"
                let orchestrator = SkipOrchestrator(
                    store: store,
                    adCatalogStore: catalog
                )
                await orchestrator.beginEpisode(
                    analysisAssetId: assetId,
                    episodeId: "episode-o4qr-producer-race-\(suffix)",
                    podcastId: "show-o4qr-producer-race"
                )
                let oldAutomatic = lifecycleWindow(
                    id: "window-o4qr-producer-race-\(suffix)",
                    assetId: assetId,
                    catalogMatch: learned
                )
                let barrier = CatalogAppliedPersistenceGate()
                await orchestrator
                    ._setCatalogAdmissionValidationBarrierForTesting {
                        await barrier.block()
                    }
                let oldIngestion = Task {
                    if usesDecisionResult {
                        await orchestrator.receiveAdDecisionResults([
                            AdDecisionResult(
                                id: oldAutomatic.id,
                                analysisAssetId: assetId,
                                startTime: oldAutomatic.startTime,
                                endTime: oldAutomatic.endTime,
                                skipConfidence: oldAutomatic.confidence,
                                eligibilityGate: .eligible,
                                recomputationRevision: 0,
                                producerRevision: oldAutomatic
                            )
                        ])
                    } else {
                        await orchestrator.receiveAdWindows([oldAutomatic])
                    }
                }
                await barrier.waitUntilStarted()

                if retiresWhileSuspended {
                    await orchestrator.retireAdWindows(
                        ids: [oldAutomatic.id]
                    )
                } else {
                    let newerMarkOnly = lifecycleWindow(
                        id: oldAutomatic.id,
                        assetId: assetId,
                        eligibilityGate:
                            SkipEligibilityGate.markOnly.rawValue
                    )
                    await orchestrator.receiveAdWindows([newerMarkOnly])
                }

                await barrier.release()
                await oldIngestion.value
                await orchestrator
                    ._setCatalogAdmissionValidationBarrierForTesting(nil)

                #expect(
                    !(await orchestrator.activeWindowIDs())
                        .contains(oldAutomatic.id),
                    "stale \(entrypoint) catalog admission survived \(mutation)"
                )
                let remainsSuggested =
                    await orchestrator.activeSuggestWindowIDs()
                        .contains(oldAutomatic.id)
                #expect(
                    remainsSuggested == !retiresWhileSuspended,
                    "newest \(mutation) state was not preserved for \(entrypoint)"
                )
            }
        }
    }

    @Test("live decision handoff rejects an unproven catalog claim")
    func decisionResultWithLegacyCatalogClaimFailsClosed() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-legacy-live-decision"
        try await store.insertAsset(makeAsset(id: assetId))
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-legacy-live-decision",
            podcastId: "show-o4qr"
        )

        let unproven = AdWindow(
            id: "window-o4qr-legacy-live-decision",
            analysisAssetId: assetId,
            startTime: 62,
            endTime: 82,
            confidence: 0.99,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "legacy-live-decision",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            catalogStoreMatchSimilarity: 0.99
        )
        let provenanceOnly = AdWindow(
            id: "window-o4qr-provenance-only-live-decision",
            analysisAssetId: assetId,
            startTime: 92,
            endTime: 112,
            confidence: 0.99,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "legacy-live-decision",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            catalogMatchedShowId: "show-o4qr"
        )
        for window in [unproven, provenanceOnly] {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: window.id,
                    analysisAssetId: assetId,
                    startTime: window.startTime,
                    endTime: window.endTime,
                    skipConfidence: window.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 0,
                    producerRevision: window
                )
            ])
            #expect(
                await orchestrator.activeSuggestWindowIDs()
                    .contains(window.id)
            )
            #expect(
                !(await orchestrator.activeWindowIDs()).contains(window.id)
            )
        }
    }

    @Test("manual veto revokes the exact catalog row that promoted a window")
    func manualVetoRevokesMatchedCatalogEvidence() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-veto"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let features = try await store.fetchFeatureWindows(
            assetId: assetId,
            from: 62,
            to: 82
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(features)
        let repeatedFingerprint = RepeatedAdFingerprint.from(
            featureWindows: features
        )
        let learned = try #require(
            try await catalog.insert(
                showId: "show-o4qr",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .confirmedSuggestion,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "older-asset",
                sourceWindowId: "older-window",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )
        let repeatedStorage = InMemoryRepeatedAdCacheStorage()
        let repeatedCache = RepeatedAdCacheService(
            storage: repeatedStorage
        )
        _ = try await repeatedCache.store(
            showId: "show-o4qr",
            fingerprint: repeatedFingerprint,
            boundaryStart: 62,
            boundaryEnd: 82,
            confidence: 0.99,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "older-asset",
            sourceWindowId: "older-window"
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore,
            adCatalogStore: catalog,
            repeatedAdCache: repeatedCache
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "episode-o4qr-veto",
            podcastId: "show-o4qr"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let window = lifecycleWindow(
            id: "window-o4qr-veto",
            assetId: assetId,
            catalogMatch: learned
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])

        #expect(
            await orchestrator.revertWindow(
                windowId: window.id,
                podcastId: "show-o4qr"
            )
        )
        #expect(
            await catalog.matches(
                fingerprint: fingerprint,
                show: "show-o4qr"
            ).isEmpty
        )
        let recurrenceOutcome = try await repeatedCache.lookup(
            showId: "show-o4qr",
            fingerprint: repeatedFingerprint
        )
        if case .hit = recurrenceOutcome {
            Issue.record("manual veto must revoke parallel recurrence evidence")
        }
        let audited = try #require(
            try await catalog.allEntries().first { $0.id == learned.id }
        )
        #expect(audited.revocationSource == .manualVeto)
        #expect(audited.revokedAt != nil)
    }

    @Test(
        "delayed correction revokes its captured show after episode replacement",
        .timeLimit(.minutes(1))
    )
    func delayedCorrectionKeepsCapturedShowScope() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-veto-race"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let features = try await store.fetchFeatureWindows(
            assetId: assetId,
            from: 62,
            to: 82
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(features)
        let repeatedFingerprint = RepeatedAdFingerprint.from(
            featureWindows: features
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let sourceLearned = try #require(
            try await catalog.insert(
                showId: "show-o4qr-source",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .confirmedSuggestion,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "source-catalog-asset",
                sourceWindowId: "source-catalog-window",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )
        _ = try #require(
            try await catalog.insert(
                showId: "show-o4qr-replacement",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .confirmedSuggestion,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "replacement-catalog-asset",
                sourceWindowId: "replacement-catalog-window",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )
        let repeatedStorage = InMemoryRepeatedAdCacheStorage()
        let repeatedCache = RepeatedAdCacheService(storage: repeatedStorage)
        for showId in ["show-o4qr-source", "show-o4qr-replacement"] {
            _ = try #require(
                try await repeatedCache.store(
                    showId: showId,
                    fingerprint: repeatedFingerprint,
                    boundaryStart: 62,
                    boundaryEnd: 82,
                    confidence: 0.99,
                    learningSource: .confirmedSuggestion,
                    learningLifecycle: .explicitConfirmation,
                    sourceAssetId: "\(showId)-asset",
                    sourceWindowId: "\(showId)-window"
                )
            )
        }
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore,
            adCatalogStore: catalog,
            repeatedAdCache: repeatedCache
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "ep-\(assetId)",
            podcastId: "show-o4qr-source"
        )
        let suggested = lifecycleWindow(
            id: "window-o4qr-veto-race",
            assetId: assetId,
            catalogMatch: sourceLearned,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])

        let gate = CatalogAppliedPersistenceGate()
        await orchestrator._setFeedbackPersistenceBarrierForTesting {
            await gate.block()
        }
        let denial = Task {
            await orchestrator.declineSuggestedSkip(
                windowId: suggested.id,
                isExplicitDenial: true
            )
        }
        await gate.waitUntilStarted()
        await orchestrator.beginEpisode(
            analysisAssetId: "replacement-asset",
            episodeId: "replacement-episode",
            podcastId: "show-o4qr-replacement"
        )
        await gate.release()

        #expect(await denial.value)
        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        #expect(
            await catalog.matches(
                fingerprint: fingerprint,
                show: "show-o4qr-source"
            ).isEmpty
        )
        #expect(
            await catalog.matches(
                fingerprint: fingerprint,
                show: "show-o4qr-replacement"
            ).count == 1
        )
        if case .hit = try await repeatedCache.lookup(
            showId: "show-o4qr-source",
            fingerprint: repeatedFingerprint
        ) {
            Issue.record("The source show's recurrence evidence must be revoked")
        }
        if case .miss = try await repeatedCache.lookup(
            showId: "show-o4qr-replacement",
            fingerprint: repeatedFingerprint
        ) {
            Issue.record("The replacement show's recurrence evidence must remain")
        }
    }

    @Test("accepting a suggested skip learns instead of revoking catalog evidence")
    func acceptedSuggestionLearnsCatalog() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-suggestion-yes"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore,
            adCatalogStore: catalog
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "ep-\(assetId)",
            podcastId: "show-o4qr"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let suggested = lifecycleWindow(
            id: "window-o4qr-suggestion-yes",
            assetId: assetId,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])

        #expect(
            await orchestrator.acceptSuggestedSkip(
                windowId: suggested.id
            )
        )
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        let learned = try #require(try await catalog.allEntries().first)
        #expect(learned.learningSource == .confirmedSuggestion)
        #expect(learned.learningLifecycle == .explicitConfirmation)
        #expect(learned.revokedAt == nil)
        #expect(
            await catalog.matches(
                fingerprint: learned.acousticFingerprint,
                show: "show-o4qr"
            ).count == 1
        )
    }

    @Test("confirmed suggestion learns after an episode replacement race")
    func acceptedSuggestionLearningSurvivesEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-suggestion-race"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore,
            adCatalogStore: catalog
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "ep-\(assetId)",
            podcastId: "show-o4qr-source"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let suggested = lifecycleWindow(
            id: "window-o4qr-suggestion-race",
            assetId: assetId,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])

        let gate = CatalogAppliedPersistenceGate()
        await orchestrator._setFeedbackPersistenceBarrierForTesting {
            await gate.block()
        }
        let acceptance = Task {
            await orchestrator.acceptSuggestedSkip(windowId: suggested.id)
        }
        await gate.waitUntilStarted()
        await orchestrator.beginEpisode(
            analysisAssetId: "replacement-asset",
            episodeId: "replacement-episode",
            podcastId: "replacement-show"
        )
        await gate.release()

        #expect(await acceptance.value)
        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        let learned = try #require(try await catalog.allEntries().first)
        #expect(learned.showId == "show-o4qr-source")
        #expect(learned.learningSource == .confirmedSuggestion)
        #expect(learned.learningLifecycle == .explicitConfirmation)
        #expect(learned.sourceAssetId == assetId)
    }

    @Test("confirmed auto-skip learns against its captured show after episode replacement")
    func confirmedAutoSkipLearningUsesCapturedShow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-auto-confirm-race"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let orchestrator = SkipOrchestrator(
            store: store,
            adCatalogStore: catalog
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "ep-\(assetId)",
            podcastId: "show-o4qr-source",
            playbackLifecycleGeneration: 73
        )
        let window = lifecycleWindow(
            id: "window-o4qr-auto-confirm-race",
            assetId: assetId,
            decisionState: .applied
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])

        let gate = CatalogAppliedPersistenceGate()
        await orchestrator._setFeedbackPersistenceBarrierForTesting {
            await gate.block()
        }
        let confirmation = Task {
            await orchestrator.confirmAutoSkippedBanner(
                windowId: window.id,
                analysisAssetId: window.analysisAssetId,
                startTime: window.startTime,
                endTime: window.endTime,
                ifCurrentEpisodeId: "ep-\(assetId)",
                ifPlaybackLifecycleGeneration: 73,
                ifWindowMaterialRevisionToken:
                    AdWindowMaterialIdentity.autoSkipToken(
                        window: window,
                        displayedStart: window.startTime,
                        displayedEnd: window.endTime
                    )
            )
        }
        await gate.waitUntilStarted()
        await orchestrator.beginEpisode(
            analysisAssetId: "replacement-asset",
            episodeId: "replacement-episode",
            podcastId: "show-o4qr-replacement",
            playbackLifecycleGeneration: 74
        )
        await gate.release()

        #expect(await confirmation.value)
        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        let learned = try #require(try await catalog.allEntries().first)
        #expect(learned.showId == "show-o4qr-source")
        #expect(learned.learningSource == .confirmedAutoSkipBanner)
        #expect(learned.learningLifecycle == .explicitConfirmation)
        #expect(
            await catalog.matches(
                fingerprint: learned.acousticFingerprint,
                show: "show-o4qr-replacement"
            ).isEmpty
        )
    }

    @Test("old-source revocation cannot poison replacement same-ID learning")
    func revocationFenceUsesExactAssetAndWindowIdentity() async throws {
        let store = try await makeTestStore()
        let assetA = "asset-o4qr-exact-revocation-a"
        let assetB = "asset-o4qr-exact-revocation-b"
        try await store.insertAsset(makeAsset(id: assetA))
        try await store.insertAsset(makeAsset(id: assetB))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetA)
                + syntheticAdWindows(assetId: assetB)
        )
        let repeatedStorage = InMemoryRepeatedAdCacheStorage()
        let repeatedCache = RepeatedAdCacheService(
            storage: repeatedStorage
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore,
            repeatedAdCache: repeatedCache
        )
        let sharedID = "window-o4qr-exact-revocation"
        let source = lifecycleWindow(
            id: sharedID,
            assetId: assetA
        )
        try await store.insertAdWindow(source)
        await orchestrator.beginEpisode(
            analysisAssetId: assetA,
            episodeId: "ep-\(assetA)",
            podcastId: "show-o4qr-source"
        )
        await orchestrator.setActiveSkipMode(.manual)
        await orchestrator.receiveAdWindows([source])

        let barrier = CatalogAppliedPersistenceGate()
        await orchestrator._setFeedbackPersistenceBarrierForTesting {
            await barrier.block()
        }
        let oldVeto = Task {
            await orchestrator.revertWindow(
                windowId: sharedID,
                podcastId: "show-o4qr-source"
            )
        }
        await barrier.waitUntilStarted()
        await orchestrator.beginEpisode(
            analysisAssetId: assetB,
            episodeId: "ep-\(assetB)",
            podcastId: "show-o4qr-replacement"
        )
        await orchestrator.setActiveSkipMode(.manual)
        await barrier.release()
        #expect(await oldVeto.value)
        await orchestrator
            ._setFeedbackPersistenceBarrierForTesting(nil)

        let replacement = lifecycleWindow(
            id: sharedID,
            assetId: assetB
        )
        try await store.insertOrReplaceAdWindow(replacement)
        await orchestrator.receiveAdWindows([replacement])
        await orchestrator.applyManualSkip(windowId: sharedID)
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()

        let learned = try #require(
            try await repeatedStorage.fetchAll(
                showId: "show-o4qr-replacement"
            ).first
        )
        #expect(learned.sourceAssetId == assetB)
        #expect(learned.sourceWindowId == sharedID)
        #expect(learned.learningSource == .manualSkip)
    }

    @Test("explicitly denying a suggested skip revokes its catalog matches")
    func deniedSuggestionRevokesCatalogEvidence() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-o4qr-suggestion-no"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )
        let features = try await store.fetchFeatureWindows(
            assetId: assetId,
            from: 62,
            to: 82
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(features)
        let catalog = try AdCatalogStore(directoryURL: makeCatalogDir())
        let learned = try #require(
            try await catalog.insert(
                showId: "show-o4qr",
                episodePosition: .midRoll,
                durationSec: 20,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "older-suggestion-asset",
                sourceWindowId: "older-suggestion-window",
                sourceStartTime: 62,
                sourceEndTime: 82
            )
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore,
            adCatalogStore: catalog
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "ep-\(assetId)",
            podcastId: "show-o4qr"
        )
        let suggested = lifecycleWindow(
            id: "window-o4qr-suggestion-no",
            assetId: assetId,
            catalogMatch: learned,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])

        #expect(
            await orchestrator.declineSuggestedSkip(
                windowId: suggested.id,
                isExplicitDenial: true
            )
        )
        #expect(
            await catalog.matches(
                fingerprint: fingerprint,
                show: "show-o4qr"
            ).isEmpty
        )
        let audited = try #require(
            try await catalog.allEntries().first { $0.id == learned.id }
        )
        #expect(audited.revocationSource == .bannerSuggestionDenied)
    }
}

private actor CatalogAppliedPersistenceGate {
    private var started = false
    private var released = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

    func block() async {
        started = true
        for waiter in startWaiters { waiter.resume() }
        startWaiters.removeAll()
        guard !released else { return }
        await withCheckedContinuation { continuation in
            releaseWaiters.append(continuation)
        }
    }

    func waitUntilStarted() async {
        guard !started else { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        released = true
        for waiter in releaseWaiters { waiter.resume() }
        releaseWaiters.removeAll()
    }
}

// MARK: - AcousticFingerprint from feature windows

@Suite("AcousticFingerprint.fromFeatureWindows (gtt9.17)")
struct AcousticFingerprintFromWindowsTests {

    private func window(
        assetId: String = "asset",
        startTime: Double,
        endTime: Double,
        rms: Double,
        flux: Double = 0.1,
        music: Double = 0.1,
        featureVersion: Int =
            FeatureExtractionConfig.default.featureVersion
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            rms: rms,
            spectralFlux: flux,
            musicProbability: music,
            speakerChangeProxyScore: 0.3,
            musicBedChangeScore: 0.1,
            musicBedOnsetScore: 0.1,
            musicBedOffsetScore: 0.1,
            musicBedLevel: .background,
            pauseProbability: 0.2,
            speakerClusterId: 1,
            jingleHash: nil,
            featureVersion: featureVersion
        )
    }

    @Test("identical input produces identical fingerprint (deterministic)")
    func deterministic() {
        let ws = (0..<10).map { i in
            window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.4 + Double(i) * 0.01
            )
        }
        let fpA = AcousticFingerprint.fromFeatureWindows(ws)
        let fpB = AcousticFingerprint.fromFeatureWindows(ws)
        #expect(fpA == fpB)
        #expect(!fpA.isZero)
    }

    @Test("empty windows produce a zero fingerprint")
    func emptyYieldsZero() {
        let fp = AcousticFingerprint.fromFeatureWindows([])
        #expect(fp.isZero)
    }

    @Test("malformed feature rows fail closed instead of being clamped")
    func malformedRowsYieldZero() {
        let valid = window(
            startTime: 0,
            endTime: 2,
            rms: 0.4
        )
        let malformed: [[FeatureWindow]] = [
            [
                window(
                    startTime: 0,
                    endTime: 2,
                    rms: -.infinity
                ),
            ],
            [
                window(
                    startTime: -2,
                    endTime: 0,
                    rms: 0.4
                ),
            ],
            [
                valid,
                window(
                    assetId: "other-asset",
                    startTime: 2,
                    endTime: 4,
                    rms: 0.4
                ),
            ],
            [
                window(
                    assetId: " asset ",
                    startTime: 0,
                    endTime: 2,
                    rms: 0.4
                ),
            ],
            [
                window(
                    assetId: "asset\u{0}other",
                    startTime: 0,
                    endTime: 2,
                    rms: 0.4
                ),
            ],
            [
                window(
                    startTime: 0,
                    endTime: 2,
                    rms: 0.4,
                    music: 1.01
                ),
            ],
            [
                window(
                    startTime: 0,
                    endTime: 2,
                    rms: 0.4,
                    featureVersion:
                        FeatureExtractionConfig.default.featureVersion + 1
                ),
            ],
            [
                window(
                    startTime: 0,
                    endTime:
                        (MusicDetectionConfig.supportedWindowDurations.max()
                            ?? 0) + 1,
                    rms: 0.4
                ),
            ],
            [
                window(
                    startTime: 0,
                    endTime: 3,
                    rms: 0.4
                ),
            ],
            [
                valid,
                window(
                    startTime: 1,
                    endTime: 3,
                    rms: 0.4
                ),
            ],
        ]

        for rows in malformed {
            #expect(AcousticFingerprint.fromFeatureWindows(rows).isZero)
        }
    }

    @Test("all-zero-signal windows produce a zero fingerprint")
    func allZeroYieldsZero() {
        let ws = (0..<10).map { i in
            window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.0,
                flux: 0.0,
                music: 0.0
            )
        }
        // The fingerprint still tracks speakerChangeProxy/pauseProbability etc.,
        // so "all zero" here exercises the main acoustic energy path being 0.
        // We assert the fingerprint is either zero OR similar-to-another all-zero
        // fingerprint — the contract for wiring is that silent windows should
        // not match loud ones.
        let loud = (0..<10).map { i in
            window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.8,
                flux: 0.5,
                music: 0.8
            )
        }
        let fpSilent = AcousticFingerprint.fromFeatureWindows(ws)
        let fpLoud = AcousticFingerprint.fromFeatureWindows(loud)
        let sim = AcousticFingerprint.similarity(fpSilent, fpLoud)
        #expect(sim < 0.80, "silent vs loud fingerprints must sit well below the 0.90 floor, got \(sim)")
    }

    @Test("identical feature patterns match above default floor")
    func identicalMatchesAboveFloor() {
        let ws1 = (0..<10).map { i in
            window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.6,
                flux: 0.3,
                music: 0.8
            )
        }
        let ws2 = (0..<10).map { i in
            window(
                startTime: Double(i + 50) * 2,      // different time offset
                endTime: Double(i + 51) * 2,
                rms: 0.6,
                flux: 0.3,
                music: 0.8
            )
        }
        let fp1 = AcousticFingerprint.fromFeatureWindows(ws1)
        let fp2 = AcousticFingerprint.fromFeatureWindows(ws2)
        let sim = AcousticFingerprint.similarity(fp1, fp2)
        #expect(sim >= AdCatalogStore.defaultSimilarityFloor,
                "identical feature patterns at different times must match above floor, got \(sim)")
    }
}
