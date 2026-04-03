// CoreServiceTests.swift
// Unit tests for core services: AnalysisStore, LexicalScanner,
// SkipOrchestrator, TrustScoringService, PreviewBudgetStore,
// CapabilitiesService, and AssetProvider.

import Foundation
import Testing
@testable import Playhead

// MARK: - Test Helpers

private func repoRootURL() -> URL {
    URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
}

private func readRepoSource(_ relativePath: String) throws -> String {
    let url = repoRootURL().appendingPathComponent(relativePath)
    return try String(contentsOf: url, encoding: .utf8)
}

/// Tracks temporary directories created by `makeTestStore()` for cleanup.
private let _testStoreDirs = TestTempDirTracker()

/// Creates an AnalysisStore backed by a temporary directory for isolated testing.
/// The directory is automatically cleaned up when the test process ends.
private func makeTestStore() async throws -> AnalysisStore {
    let dir = try makeTempDir(prefix: "PlayheadTests")
    _testStoreDirs.track(dir)
    let store = try AnalysisStore(directory: dir)
    try await store.migrate()
    return store
}

private func makeAnalysisAsset(
    id: String = "asset-1",
    episodeId: String = "ep-1"
) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId,
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///test/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

private func makeTranscriptChunk(
    id: String = UUID().uuidString,
    assetId: String = "asset-1",
    chunkIndex: Int = 0,
    startTime: Double = 0,
    endTime: Double = 10,
    text: String = "this episode is brought to you by acme corp",
    pass: String = "fast"
) -> TranscriptChunk {
    TranscriptChunk(
        id: id,
        analysisAssetId: assetId,
        segmentFingerprint: "fp-\(id)",
        chunkIndex: chunkIndex,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: text.lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }
            .joined(separator: " "),
        pass: pass,
        modelVersion: "whisper-tiny-v1"
    )
}

private func makeAdWindow(
    id: String = "ad-1",
    assetId: String = "asset-1",
    startTime: Double = 60,
    endTime: Double = 120,
    confidence: Double = 0.75,
    decisionState: String = "confirmed"
) -> AdWindow {
    AdWindow(
        id: id,
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        boundaryState: "lexical",
        decisionState: decisionState,
        detectorVersion: "detection-v1",
        advertiser: nil,
        product: nil,
        adDescription: nil,
        evidenceText: "brought to you by",
        evidenceStartTime: startTime,
        metadataSource: "none",
        metadataConfidence: nil,
        metadataPromptVersion: nil,
        wasSkipped: false,
        userDismissedBanner: false
    )
}

private func makeFeatureWindow(
    assetId: String = "asset-1",
    startTime: Double = 0,
    endTime: Double = 1,
    pauseProbability: Double = 0.1,
    rms: Double = 0.05
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        rms: rms,
        spectralFlux: 0.01,
        musicProbability: 0.0,
        pauseProbability: pauseProbability,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 1
    )
}

private func makePodcastProfile(
    podcastId: String = "podcast-1",
    mode: String = "shadow",
    trustScore: Double = 0.5,
    observations: Int = 0,
    falseSignals: Int = 0
) -> PodcastProfile {
    PodcastProfile(
        podcastId: podcastId,
        sponsorLexicon: nil,
        normalizedAdSlotPriors: nil,
        repeatedCTAFragments: nil,
        jingleFingerprints: nil,
        implicitFalsePositiveCount: 0,
        skipTrustScore: trustScore,
        observationCount: observations,
        mode: mode,
        recentFalseSkipSignals: falseSignals
    )
}

// MARK: - AnalysisStore: Schema & CRUD

@Suite("AnalysisStore - Schema and CRUD")
struct AnalysisStoreCRUDTests {

    @Test("Migration creates all tables without error")
    func migrationSucceeds() async throws {
        let store = try await makeTestStore()
        // Calling migrate a second time should be safe (IF NOT EXISTS).
        try await store.migrate()
    }

    @Test("Insert and fetch AnalysisAsset round-trips correctly")
    func assetRoundTrip() async throws {
        let store = try await makeTestStore()
        let asset = makeAnalysisAsset()
        try await store.insertAsset(asset)
        let fetched = try await store.fetchAsset(id: "asset-1")
        #expect(fetched != nil, "Asset should be fetchable after insert")
        #expect(fetched?.id == "asset-1")
        #expect(fetched?.episodeId == "ep-1")
        #expect(fetched?.assetFingerprint == "fp-asset-1")
        #expect(fetched?.analysisState == "new")
    }

    @Test("Fetch asset by episode ID returns latest")
    func fetchByEpisodeId() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset(id: "a1", episodeId: "ep-1"))
        try await store.insertAsset(makeAnalysisAsset(id: "a2", episodeId: "ep-1"))
        let fetched = try await store.fetchAssetByEpisodeId("ep-1")
        #expect(fetched?.id == "a2", "Should return the latest asset for the episode")
    }

    @Test("Update asset state persists")
    func updateAssetState() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        try await store.updateAssetState(id: "asset-1", state: "analyzing")
        let fetched = try await store.fetchAsset(id: "asset-1")
        #expect(fetched?.analysisState == "analyzing")
    }

    @Test("Delete asset removes it")
    func deleteAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        try await store.deleteAsset(id: "asset-1")
        let fetched = try await store.fetchAsset(id: "asset-1")
        #expect(fetched == nil)
    }

    @Test("Insert and fetch transcript chunks")
    func transcriptChunkCRUD() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let chunk = makeTranscriptChunk()
        try await store.insertTranscriptChunk(chunk)
        let fetched = try await store.fetchTranscriptChunks(assetId: "asset-1")
        #expect(fetched.count == 1)
        #expect(fetched[0].text == chunk.text)
        #expect(fetched[0].pass == "fast")
    }

    @Test("Batch insert transcript chunks in transaction")
    func batchInsertChunks() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let chunks = (0..<5).map { i in
            makeTranscriptChunk(
                id: "chunk-\(i)",
                chunkIndex: i,
                startTime: Double(i * 10),
                endTime: Double((i + 1) * 10)
            )
        }
        try await store.insertTranscriptChunks(chunks)
        let fetched = try await store.fetchTranscriptChunks(assetId: "asset-1")
        #expect(fetched.count == 5)
    }

    @Test("hasTranscriptChunk detects duplicates by fingerprint")
    func chunkDedup() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let chunk = makeTranscriptChunk(id: "c1")
        try await store.insertTranscriptChunk(chunk)
        let exists = try await store.hasTranscriptChunk(
            analysisAssetId: "asset-1",
            segmentFingerprint: "fp-c1"
        )
        #expect(exists == true)
        let missing = try await store.hasTranscriptChunk(
            analysisAssetId: "asset-1",
            segmentFingerprint: "fp-does-not-exist"
        )
        #expect(missing == false)
    }

    @Test("Insert and fetch ad windows")
    func adWindowCRUD() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let ad = makeAdWindow()
        try await store.insertAdWindow(ad)
        let fetched = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(fetched.count == 1)
        #expect(fetched[0].confidence == 0.75)
        #expect(fetched[0].decisionState == "confirmed")
    }

    @Test("Update ad window decision state")
    func updateAdDecision() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        try await store.insertAdWindow(makeAdWindow())
        try await store.updateAdWindowDecision(id: "ad-1", decisionState: "applied")
        let fetched = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(fetched[0].decisionState == "applied")
    }

    @Test("Update ad window wasSkipped flag")
    func updateWasSkipped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        try await store.insertAdWindow(makeAdWindow())
        try await store.updateAdWindowWasSkipped(id: "ad-1", wasSkipped: true)
        let fetched = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(fetched[0].wasSkipped == true)
    }

    @Test("Insert and fetch feature windows")
    func featureWindowCRUD() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let windows = (0..<3).map { i in
            makeFeatureWindow(
                startTime: Double(i),
                endTime: Double(i + 1)
            )
        }
        try await store.insertFeatureWindows(windows)
        let fetched = try await store.fetchFeatureWindows(
            assetId: "asset-1", from: 0, to: 3
        )
        #expect(fetched.count == 3)
    }

    @Test("Insert and fetch analysis sessions")
    func sessionCRUD() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let session = AnalysisSession(
            id: "sess-1",
            analysisAssetId: "asset-1",
            state: "queued",
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        )
        try await store.insertSession(session)
        let fetched = try await store.fetchSession(id: "sess-1")
        #expect(fetched?.state == "queued")

        try await store.updateSessionState(id: "sess-1", state: "running")
        let updated = try await store.fetchSession(id: "sess-1")
        #expect(updated?.state == "running")
    }

    @Test("Podcast profile upsert and fetch")
    func profileUpsert() async throws {
        let store = try await makeTestStore()
        let profile = makePodcastProfile()
        try await store.upsertProfile(profile)
        let fetched = try await store.fetchProfile(podcastId: "podcast-1")
        #expect(fetched != nil)
        #expect(fetched?.mode == "shadow")
        #expect(fetched?.skipTrustScore == 0.5)

        // Upsert updates existing row.
        let updated = makePodcastProfile(
            mode: "manual", trustScore: 0.7, observations: 5
        )
        try await store.upsertProfile(updated)
        let reFetched = try await store.fetchProfile(podcastId: "podcast-1")
        #expect(reFetched?.mode == "manual")
        #expect(reFetched?.skipTrustScore == 0.7)
        #expect(reFetched?.observationCount == 5)
    }

    @Test("Preview budget upsert and fetch")
    func budgetUpsert() async throws {
        let store = try await makeTestStore()
        let budget = PreviewBudget(
            canonicalEpisodeKey: "ep-key-1",
            consumedAnalysisSeconds: 100,
            graceBreakWindow: 0,
            lastUpdated: Date().timeIntervalSince1970
        )
        try await store.upsertBudget(budget)
        let fetched = try await store.fetchBudget(key: "ep-key-1")
        #expect(fetched != nil)
        #expect(fetched?.consumedAnalysisSeconds == 100)
    }
}

// MARK: - AnalysisStore: FTS5

@Suite("AnalysisStore - FTS5 Search")
struct AnalysisStoreFTSTests {

    @Test("FTS5 search returns matching transcript chunks")
    func ftsSearch() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let adChunk = makeTranscriptChunk(
            id: "c1", chunkIndex: 0,
            text: "this episode is brought to you by acme corp"
        )
        let contentChunk = makeTranscriptChunk(
            id: "c2", chunkIndex: 1,
            startTime: 10, endTime: 20,
            text: "so today we are going to talk about quantum physics"
        )
        try await store.insertTranscriptChunks([adChunk, contentChunk])

        // Search for sponsor-related text.
        let results = try await store.searchTranscripts(query: "acme")
        #expect(results.count == 1, "FTS should find the chunk with 'acme'")
        #expect(results[0].id == "c1")
    }

    @Test("FTS5 search returns empty for unmatched query")
    func ftsNoMatch() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let chunk = makeTranscriptChunk(id: "c1", text: "hello world")
        try await store.insertTranscriptChunk(chunk)
        let results = try await store.searchTranscripts(query: "xyznonexistent")
        #expect(results.isEmpty)
    }
}

// MARK: - LexicalScanner: Pattern Matching

@Suite("LexicalScanner - Pattern Matching")
struct LexicalScannerPatternTests {

    @Test("Detects sponsor phrases")
    func sponsorPhrases() {
        let scanner = LexicalScanner()
        let chunk = makeTranscriptChunk(
            text: "this episode is brought to you by acme corp"
        )
        let hits = scanner.scanChunk(chunk)
        let sponsorHits = hits.filter { $0.category == .sponsor }
        #expect(!sponsorHits.isEmpty, "Should detect 'brought to you by' as sponsor")
    }

    @Test("Detects promo codes")
    func promoCodes() {
        let scanner = LexicalScanner()
        let chunk = makeTranscriptChunk(
            text: "use code SAVE20 at checkout for a great deal"
        )
        let hits = scanner.scanChunk(chunk)
        let promoHits = hits.filter { $0.category == .promoCode }
        #expect(!promoHits.isEmpty, "Should detect 'use code SAVE20' as promo code")
    }

    @Test("Detects URL CTAs")
    func urlCTAs() {
        let scanner = LexicalScanner()
        let chunk = makeTranscriptChunk(
            text: "go to acme com slash podcast for more info"
        )
        let hits = scanner.scanChunk(chunk)
        let ctaHits = hits.filter { $0.category == .urlCTA }
        #expect(!ctaHits.isEmpty, "Should detect URL CTA pattern")
    }

    @Test("Detects purchase language")
    func purchaseLanguage() {
        let scanner = LexicalScanner()
        let chunk = makeTranscriptChunk(
            text: "sign up today for a free trial with money back guarantee"
        )
        let hits = scanner.scanChunk(chunk)
        let purchaseHits = hits.filter { $0.category == .purchaseLanguage }
        #expect(purchaseHits.count >= 2, "Should detect 'free trial' and 'money back guarantee'")
    }

    @Test("Detects transition markers")
    func transitionMarkers() {
        let scanner = LexicalScanner()
        let chunk = makeTranscriptChunk(
            text: "and now back to the show we were discussing"
        )
        let hits = scanner.scanChunk(chunk)
        let transitionHits = hits.filter { $0.category == .transitionMarker }
        #expect(!transitionHits.isEmpty, "Should detect transition marker")
    }

    @Test("Empty text produces no hits")
    func emptyText() {
        let scanner = LexicalScanner()
        let chunk = makeTranscriptChunk(text: "")
        let hits = scanner.scanChunk(chunk)
        #expect(hits.isEmpty)
    }

    @Test("Content-only text produces no hits")
    func contentOnly() {
        let scanner = LexicalScanner()
        let chunk = makeTranscriptChunk(
            text: "the cat sat on the mat and looked at the stars"
        )
        let hits = scanner.scanChunk(chunk)
        #expect(hits.isEmpty, "Pure content should produce zero ad hits")
    }
}

// MARK: - LexicalScanner: Candidate Merging

@Suite("LexicalScanner - Candidate Merging and Confidence")
struct LexicalScannerMergingTests {

    @Test("Adjacent hits within gap threshold merge into one candidate")
    func mergeAdjacentHits() {
        let scanner = LexicalScanner()
        // Two chunks close together with ad language.
        let chunk1 = makeTranscriptChunk(
            id: "c1", startTime: 100, endTime: 110,
            text: "this episode is brought to you by acme corp use code SAVE"
        )
        let chunk2 = makeTranscriptChunk(
            id: "c2", chunkIndex: 1, startTime: 115, endTime: 125,
            text: "go to acme com slash podcast for a free trial"
        )
        let candidates = scanner.scan(
            chunks: [chunk1, chunk2],
            analysisAssetId: "asset-1"
        )
        // Both chunks have multiple hits within the 30s merge gap.
        #expect(candidates.count == 1,
                "Chunks within merge gap should produce a single candidate (got \(candidates.count))")
    }

    @Test("Widely spaced hits produce separate candidates")
    func separateHits() {
        let config = LexicalScannerConfig(
            mergeGapThreshold: 30.0,
            minHitsForCandidate: 2,
            detectorVersion: "test-v1"
        )
        let scanner = LexicalScanner(config: config)
        let chunk1 = makeTranscriptChunk(
            id: "c1", startTime: 0, endTime: 10,
            text: "brought to you by acme corp use code SAVE20"
        )
        let chunk2 = makeTranscriptChunk(
            id: "c2", chunkIndex: 1, startTime: 500, endTime: 510,
            text: "sponsored by beta inc promo code BETA50"
        )
        let candidates = scanner.scan(
            chunks: [chunk1, chunk2],
            analysisAssetId: "asset-1"
        )
        #expect(candidates.count == 2,
                "Chunks 500s apart should produce separate candidates")
    }

    @Test("Single hit below minHitsForCandidate is filtered out")
    func belowMinHits() {
        let config = LexicalScannerConfig(
            mergeGapThreshold: 30.0,
            minHitsForCandidate: 3,
            detectorVersion: "test-v1"
        )
        let scanner = LexicalScanner(config: config)
        let chunk = makeTranscriptChunk(
            text: "brought to you by acme"
        )
        let candidates = scanner.scan(
            chunks: [chunk],
            analysisAssetId: "asset-1"
        )
        #expect(candidates.isEmpty,
                "A single hit should not meet minHitsForCandidate=3")
    }

    @Test("Confidence scales with total weight")
    func confidenceScaling() {
        let scanner = LexicalScanner()
        // Heavy ad segment with many signals.
        let chunk = makeTranscriptChunk(
            text: "brought to you by acme corp use code SAVE20 " +
                  "go to acme com slash deal for a free trial " +
                  "sign up today for a special offer"
        )
        let candidates = scanner.scan(
            chunks: [chunk],
            analysisAssetId: "asset-1"
        )
        #expect(!candidates.isEmpty, "Should produce at least one candidate")
        if let candidate = candidates.first {
            #expect(candidate.confidence > 0.5,
                    "Dense ad content should have confidence > 0.5 (got \(candidate.confidence))")
            #expect(candidate.confidence <= 0.95,
                    "Confidence should be capped at 0.95")
        }
    }

    @Test("Per-show sponsor lexicon boosts detection")
    func sponsorLexiconBoost() {
        let profile = PodcastProfile(
            podcastId: "p1",
            sponsorLexicon: "Acme Corp,BetaWidgets",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 3,
            mode: "manual",
            recentFalseSkipSignals: 0
        )
        let scanner = LexicalScanner(podcastProfile: profile)
        let chunk = makeTranscriptChunk(
            text: "and now a word about acme corp and their product"
        )
        let hits = scanner.scanChunk(chunk)
        let sponsorHits = hits.filter { $0.category == .sponsor && $0.weight == 1.5 }
        #expect(!sponsorHits.isEmpty,
                "Per-show sponsor lexicon should produce boosted hits")
    }
}

// MARK: - SkipOrchestrator: Hysteresis

@Suite("SkipOrchestrator - Hysteresis and Gap Merging")
struct SkipOrchestratorHysteresisTests {

    @Test("Window below enter threshold is suppressed in auto mode")
    func belowEnterThreshold() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(analysisAssetId: "asset-1")

        // Force auto mode by wiring a trust service with a pre-set profile.
        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(makePodcastProfile(
            mode: "auto", trustScore: 0.9, observations: 10
        ))
        let trustService = TrustScoringService(store: trustStore)
        let autoOrchestrator = SkipOrchestrator(
            store: store, trustService: trustService
        )
        await autoOrchestrator.beginEpisode(
            analysisAssetId: "asset-1", podcastId: "podcast-1"
        )

        // Send a low-confidence ad window.
        let lowConfAd = makeAdWindow(
            id: "ad-low", confidence: 0.3, decisionState: "candidate"
        )
        await autoOrchestrator.receiveAdWindows([lowConfAd])

        let log = await autoOrchestrator.getDecisionLog()
        let suppressed = log.filter { $0.decision == .suppressed }
        #expect(!suppressed.isEmpty,
                "Window with confidence 0.3 should be suppressed (enter threshold is 0.65)")
    }

    @Test("Window above enter threshold in auto mode is applied")
    func aboveEnterThreshold() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(makePodcastProfile(
            mode: "auto", trustScore: 0.9, observations: 10
        ))
        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(
            store: store, trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", podcastId: "podcast-1"
        )

        let highConfAd = makeAdWindow(
            id: "ad-high", startTime: 60, endTime: 120,
            confidence: 0.80, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([highConfAd])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        #expect(!applied.isEmpty,
                "Window with confidence 0.80 should be applied in auto mode")
    }

    @Test("Seek suppresses auto-skip temporarily")
    func seekSuppression() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(makePodcastProfile(
            mode: "auto", trustScore: 0.9, observations: 10
        ))
        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(
            store: store, trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", podcastId: "podcast-1"
        )

        // Record a seek, then immediately send a high-confidence window.
        await orchestrator.recordUserSeek(to: 50)
        let ad = makeAdWindow(
            id: "ad-seek", startTime: 60, endTime: 120,
            confidence: 0.85, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        #expect(applied.isEmpty,
                "Skip should be suppressed immediately after a user seek")
    }

    @Test("Listen revert sets state to reverted")
    func listenRevert() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(makePodcastProfile(
            mode: "auto", trustScore: 0.9, observations: 10
        ))
        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(
            store: store, trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", podcastId: "podcast-1"
        )

        let ad = makeAdWindow(
            id: "ad-revert", startTime: 60, endTime: 120,
            confidence: 0.85, decisionState: "confirmed"
        )
        // Insert the ad window into the store too so the persist doesn't fail.
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.recordListenRevert(
            windowId: "ad-revert", podcastId: "podcast-1"
        )

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter { $0.decision == .reverted }
        #expect(!reverted.isEmpty,
                "Listen revert should produce a .reverted decision record")
    }

    @Test("Shadow mode confirms but never applies")
    func shadowModeNoSkip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        // No trust service = shadow mode by default.
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(analysisAssetId: "asset-1")

        let ad = makeAdWindow(
            id: "ad-shadow", startTime: 60, endTime: 120,
            confidence: 0.85, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        let confirmed = log.filter { $0.decision == .confirmed }
        #expect(applied.isEmpty, "Shadow mode should never apply a skip")
        #expect(!confirmed.isEmpty, "Shadow mode should confirm the detection")
    }

    @Test("Manual mode confirms but does not auto-apply")
    func manualModeNoAutoSkip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(makePodcastProfile(
            mode: "manual", trustScore: 0.6, observations: 5
        ))
        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(
            store: store, trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", podcastId: "podcast-1"
        )

        let ad = makeAdWindow(
            id: "ad-manual", startTime: 60, endTime: 120,
            confidence: 0.85, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        #expect(applied.isEmpty, "Manual mode should not auto-apply")

        let confirmed = await orchestrator.confirmedWindows()
        #expect(!confirmed.isEmpty, "Manual mode should have confirmed windows for UI")
    }

    @Test("Manual skip applies a confirmed window")
    func manualSkipApplies() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(makePodcastProfile(
            mode: "manual", trustScore: 0.6, observations: 5
        ))
        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(
            store: store, trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", podcastId: "podcast-1"
        )

        let ad = makeAdWindow(
            id: "ad-mskip", startTime: 60, endTime: 120,
            confidence: 0.85, decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // User taps "Skip Ad" in manual mode.
        await orchestrator.applyManualSkip(windowId: "ad-mskip")

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied && $0.reason == "Manual skip by user" }
        #expect(!applied.isEmpty, "Manual skip should apply the window")
    }

    @Test("Short window below minimum span is suppressed")
    func shortWindowSuppressed() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(makePodcastProfile(
            mode: "auto", trustScore: 0.9, observations: 10
        ))
        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(
            store: store, trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", podcastId: "podcast-1"
        )

        // 5-second window is below the 15s minimum span.
        let shortAd = makeAdWindow(
            id: "ad-short", startTime: 60, endTime: 65,
            confidence: 0.70, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([shortAd])

        let log = await orchestrator.getDecisionLog()
        let suppressed = log.filter { $0.decision == .suppressed }
        #expect(!suppressed.isEmpty,
                "5s window at 0.70 confidence should be suppressed (min span 15s)")
    }
}

// MARK: - SkipOrchestrator: Silence Snapping

@Suite("SkipOrchestrator - Boundary Snapping")
struct SkipOrchestratorSnappingTests {

    @Test("Boundaries snap to nearby silence points")
    func snapToSilence() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        // Insert feature windows with a high-pause point near 60s.
        let silentWindow = makeFeatureWindow(
            startTime: 59.5, endTime: 60.5,
            pauseProbability: 0.9, rms: 0.01
        )
        try await store.insertFeatureWindow(silentWindow)

        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(makePodcastProfile(
            mode: "auto", trustScore: 0.9, observations: 10
        ))
        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(
            store: store, trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", podcastId: "podcast-1"
        )

        // Ad window starting at 61s should snap to the silence at 59.5s.
        let ad = makeAdWindow(
            id: "ad-snap", startTime: 61, endTime: 120,
            confidence: 0.85, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let log = await orchestrator.getDecisionLog()
        if let record = log.first {
            #expect(record.snappedStart == 59.5,
                    "Start should snap to silence at 59.5 (got \(record.snappedStart))")
        }
    }
}

// MARK: - TrustScoringService: Promotion & Demotion

@Suite("TrustScoringService - Promotion and Demotion")
struct TrustScoringTests {

    @Test("New shows start in shadow mode")
    func newShowShadow() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        let mode = await trust.effectiveMode(podcastId: "new-show")
        #expect(mode == .shadow)
    }

    @Test("First observation with normal confidence stays shadow")
    func firstObsNormalConfidence() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        await trust.recordSuccessfulObservation(
            podcastId: "show-1", averageConfidence: 0.60
        )
        let mode = await trust.effectiveMode(podcastId: "show-1")
        #expect(mode == .shadow,
                "Single observation at 0.60 should not promote out of shadow")
    }

    @Test("Exceptional first-episode confidence promotes to manual immediately")
    func exceptionalFirstEpisode() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        await trust.recordSuccessfulObservation(
            podcastId: "show-2", averageConfidence: 0.95
        )
        let mode = await trust.effectiveMode(podcastId: "show-2")
        #expect(mode == .manual,
                "Exceptional confidence (0.95 >= 0.92) should promote to manual immediately")
    }

    @Test("Promotion shadow -> manual after sufficient observations")
    func shadowToManual() async throws {
        let store = try await makeTestStore()
        let config = TrustScoringConfig(
            shadowToManualObservations: 3,
            shadowToManualTrustScore: 0.4,
            manualToAutoObservations: 8,
            manualToAutoTrustScore: 0.75,
            autoToManualFalseSignals: 2,
            manualToShadowFalseSignals: 4,
            falseSignalPenalty: 0.10,
            correctObservationBonus: 0.10,
            exceptionalFirstEpisodeConfidence: 0.92
        )
        let trust = TrustScoringService(store: store, config: config)

        // First observation creates the profile at 0.2 trust.
        await trust.recordSuccessfulObservation(
            podcastId: "show-3", averageConfidence: 0.50
        )
        #expect(await trust.effectiveMode(podcastId: "show-3") == .shadow)

        // Second observation: trust = 0.3.
        await trust.recordSuccessfulObservation(
            podcastId: "show-3", averageConfidence: 0.50
        )
        #expect(await trust.effectiveMode(podcastId: "show-3") == .shadow)

        // Third observation: trust = 0.4, obs = 3 -> promote to manual.
        await trust.recordSuccessfulObservation(
            podcastId: "show-3", averageConfidence: 0.50
        )
        let mode = await trust.effectiveMode(podcastId: "show-3")
        #expect(mode == .manual,
                "3 observations at trust 0.4 should promote to manual")
    }

    @Test("Promotion manual -> auto after many observations with high trust")
    func manualToAuto() async throws {
        let store = try await makeTestStore()
        // Pre-seed a profile in manual mode with trust near auto threshold.
        let profile = makePodcastProfile(
            mode: "manual", trustScore: 0.72, observations: 7
        )
        try await store.upsertProfile(profile)

        let config = TrustScoringConfig.default
        let trust = TrustScoringService(store: store, config: config)

        // Next observation bumps trust to 0.77 and obs to 8.
        await trust.recordSuccessfulObservation(
            podcastId: "podcast-1", averageConfidence: 0.80
        )
        let mode = await trust.effectiveMode(podcastId: "podcast-1")
        #expect(mode == .auto,
                "8 observations at 0.77 trust should promote to auto")
    }

    @Test("Demotion auto -> manual on false signals")
    func autoToManualDemotion() async throws {
        let store = try await makeTestStore()
        let profile = makePodcastProfile(
            mode: "auto", trustScore: 0.8, observations: 10
        )
        try await store.upsertProfile(profile)

        let trust = TrustScoringService(store: store)

        // First false signal: still auto (threshold is 2).
        await trust.recordFalseSkipSignal(podcastId: "podcast-1")
        #expect(await trust.effectiveMode(podcastId: "podcast-1") == .auto)

        // Second false signal: demotes to manual.
        await trust.recordFalseSkipSignal(podcastId: "podcast-1")
        let mode = await trust.effectiveMode(podcastId: "podcast-1")
        #expect(mode == .manual,
                "2 false signals should demote auto -> manual")
    }

    @Test("Demotion manual -> shadow on many false signals")
    func manualToShadowDemotion() async throws {
        let store = try await makeTestStore()
        let profile = makePodcastProfile(
            mode: "manual", trustScore: 0.6, observations: 5
        )
        try await store.upsertProfile(profile)

        let trust = TrustScoringService(store: store)

        for _ in 0..<4 {
            await trust.recordFalseSkipSignal(podcastId: "podcast-1")
        }
        let mode = await trust.effectiveMode(podcastId: "podcast-1")
        #expect(mode == .shadow,
                "4 false signals should demote manual -> shadow")
    }

    @Test("User override sets mode regardless of trust score")
    func userOverride() async throws {
        let store = try await makeTestStore()
        let profile = makePodcastProfile(
            mode: "shadow", trustScore: 0.1, observations: 1
        )
        try await store.upsertProfile(profile)

        let trust = TrustScoringService(store: store)
        await trust.setUserOverride(podcastId: "podcast-1", mode: .auto)

        let mode = await trust.effectiveMode(podcastId: "podcast-1")
        #expect(mode == .auto,
                "User override should set mode to auto regardless of trust")
    }

    @Test("User override on nonexistent profile creates it")
    func userOverrideCreatesProfile() async throws {
        let store = try await makeTestStore()
        let trust = TrustScoringService(store: store)
        await trust.setUserOverride(podcastId: "new-show", mode: .manual)
        let mode = await trust.effectiveMode(podcastId: "new-show")
        #expect(mode == .manual)
    }

    @Test("False signal decay halves the count")
    func falseSignalDecay() async throws {
        let store = try await makeTestStore()
        let profile = makePodcastProfile(
            mode: "manual", trustScore: 0.5, observations: 5,
            falseSignals: 4
        )
        try await store.upsertProfile(profile)

        let trust = TrustScoringService(store: store)
        await trust.decayFalseSignals(podcastId: "podcast-1")

        let fetched = try await store.fetchProfile(podcastId: "podcast-1")
        #expect(fetched?.recentFalseSkipSignals == 2,
                "Decay should halve false signals from 4 to 2")
    }

    @Test("Decay of zero false signals is a no-op")
    func decayZeroIsNoop() async throws {
        let store = try await makeTestStore()
        let profile = makePodcastProfile(
            mode: "manual", trustScore: 0.5, observations: 5,
            falseSignals: 0
        )
        try await store.upsertProfile(profile)

        let trust = TrustScoringService(store: store)
        await trust.decayFalseSignals(podcastId: "podcast-1")

        let fetched = try await store.fetchProfile(podcastId: "podcast-1")
        #expect(fetched?.recentFalseSkipSignals == 0)
    }
}

// MARK: - PreviewBudgetStore

@Suite("PreviewBudgetStore - Budget Enforcement")
struct PreviewBudgetTests {

    @Test("New episode has full budget (720s)")
    func fullBudget() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)
        let remaining = await budgetStore.remainingBudget(for: "ep-1")
        #expect(remaining == 720.0,
                "Fresh episode should have 720s budget")
    }

    @Test("Consuming budget reduces remaining")
    func consumeBudget() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)
        let remaining = await budgetStore.consumeBudget(for: "ep-1", seconds: 100)
        #expect(remaining == 620.0)

        let check = await budgetStore.remainingBudget(for: "ep-1")
        #expect(check == 620.0)
    }

    @Test("Budget exhaustion returns zero remaining")
    func budgetExhausted() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)
        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 720)
        let remaining = await budgetStore.remainingBudget(for: "ep-1")
        #expect(remaining == 0)

        let hasBudget = await budgetStore.hasBudget(for: "ep-1")
        #expect(hasBudget == false)
    }

    @Test("Exact boundary: consumed == baseBudgetSeconds yields zero remaining and no grace")
    func exactBoundaryBudgetExhausted() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Consume exactly the base budget (720s).
        _ = await budgetStore.consumeBudget(for: "ep-boundary", seconds: 720)

        // remaining should be exactly 0.
        let remaining = await budgetStore.remainingBudget(for: "ep-boundary")
        #expect(remaining == 0,
                "consumed == baseBudgetSeconds must yield 0 remaining")
        #expect(await budgetStore.hasBudget(for: "ep-boundary") == false,
                "hasBudget must be false at exact boundary")

        // Grace must also be denied at the exact boundary.
        let grace = await budgetStore.graceAllowance(
            for: "ep-boundary", adBreakDuration: 60
        )
        #expect(grace == 0,
                "No grace when consumed == baseBudgetSeconds (budget exhausted)")
    }

    @Test("One second below base budget still has remaining and qualifies for grace")
    func oneBelowBoundary() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Consume one second less than the base budget.
        _ = await budgetStore.consumeBudget(for: "ep-almost", seconds: 719)

        let remaining = await budgetStore.remainingBudget(for: "ep-almost")
        #expect(remaining == 1.0,
                "One second below budget should have 1s remaining")
        #expect(await budgetStore.hasBudget(for: "ep-almost") == true,
                "hasBudget must be true when 1s remains")

        // Grace should still be granted since consumed < baseBudgetSeconds.
        let grace = await budgetStore.graceAllowance(
            for: "ep-almost", adBreakDuration: 60
        )
        #expect(grace == 60.0,
                "Grace should be granted when consumed is below base budget")
    }

    @Test("Grace window allows up to absolute cap (1200s)")
    func graceWindow() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Consume right up to the base budget.
        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 700)
        let grace = await budgetStore.graceAllowance(
            for: "ep-1", adBreakDuration: 60
        )
        // Remaining headroom from 1200 cap: 1200 - 700 = 500.
        // Ad break is 60s, so allowance = 60.
        #expect(grace == 60.0,
                "Grace should allow full 60s ad break when headroom exists")
    }

    @Test("Grace window capped at absolute max headroom")
    func graceWindowCapped() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Consume 1100s (above base budget of 720 but within grace cap of 1200).
        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 710)
        let grace = await budgetStore.graceAllowance(
            for: "ep-1", adBreakDuration: 600
        )
        // Headroom: 1200 - 710 = 490. Ad break is 600s but capped at 490.
        #expect(grace == 490.0,
                "Grace should be capped at remaining headroom (490s)")
    }

    @Test("No grace when already past base budget")
    func noGracePastBudget() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)
        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 800)
        let grace = await budgetStore.graceAllowance(
            for: "ep-1", adBreakDuration: 60
        )
        #expect(grace == 0,
                "No grace should be granted when consumed exceeds base budget")
    }

    @Test("totalConsumed tracks cumulative usage")
    func totalConsumed() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)
        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 50)
        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 30)
        let total = await budgetStore.totalConsumed(for: "ep-1")
        #expect(total == 80.0)
    }
}

// MARK: - CapabilitiesService

@Suite("CapabilitiesService - Snapshot Detection")
struct CapabilitiesServiceTests {

    @Test("Initial snapshot is populated on construction")
    func initialSnapshot() async {
        let service = CapabilitiesService()
        let snapshot = await service.currentSnapshot
        // Verify structural fields are populated (values are device-dependent).
        #expect(snapshot.thermalState.rawValue >= 0)
        #expect(snapshot.availableDiskSpaceBytes >= 0)
        // Background processing should always be supported on iOS 13+.
        #expect(snapshot.backgroundProcessingSupported == true)
    }

    @Test("Refresh updates the snapshot")
    func refreshUpdatesSnapshot() async {
        let service = CapabilitiesService()
        let before = await service.currentSnapshot
        await service.refreshSnapshot()
        let after = await service.currentSnapshot
        // capturedAt should be different (later) after refresh.
        #expect(after.capturedAt >= before.capturedAt)
    }

    @Test("CapabilitySnapshot JSON round-trip")
    func snapshotRoundTrip() throws {
        // Use a fixed snapshot to avoid Date precision issues with live captures.
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: true,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: true,
            thermalState: .fair,
            isLowPowerMode: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 500_000_000,
            capturedAt: Date(timeIntervalSince1970: 1700000000)
        )

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(snapshot)

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(CapabilitySnapshot.self, from: data)

        #expect(decoded == snapshot, "JSON round-trip should produce identical snapshot")
    }

    @Test("shouldThrottleAnalysis based on thermal state")
    func thermalThrottling() {
        let nominalSnapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(nominalSnapshot.shouldThrottleAnalysis == false)

        let seriousSnapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .serious,
            isLowPowerMode: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(seriousSnapshot.shouldThrottleAnalysis == true)
    }

    @Test("shouldReduceHotPath based on low power mode")
    func lowPowerHotPath() {
        let normal = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(normal.shouldReduceHotPath == false)

        let lowPower = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(lowPower.shouldReduceHotPath == true)
    }

    @Test("canUseFoundationModels requires all three flags")
    func foundationModelsGate() {
        let partial = CapabilitySnapshot(
            foundationModelsAvailable: true,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(partial.canUseFoundationModels == false,
                "All three flags must be true")

        let full = CapabilitySnapshot(
            foundationModelsAvailable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(full.canUseFoundationModels == true)
    }
}

// MARK: - AssetProvider: Lifecycle

@Suite("AssetProvider - Verify, Stage, Promote, Rollback")
struct AssetProviderLifecycleTests {

    private func makeTestInventory() throws -> ModelInventory {
        let tempRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadModelTests-\(UUID().uuidString)")
        let manifest = ModelManifest(
            version: 1,
            generatedAt: .now,
            models: [
                ModelEntry(
                    id: "test-model",
                    role: .asrFast,
                    displayName: "Test Model",
                    modelVersion: "1.0.0",
                    downloadURL: URL(string: "https://example.com/model.bin")!,
                    sha256: "abc123",
                    compressedSizeBytes: 1000,
                    uncompressedSizeBytes: 2000,
                    priority: 100,
                    minimumOS: "26.0",
                    requiredCapabilities: []
                )
            ]
        )
        let inventory = ModelInventory(manifest: manifest, rootOverride: tempRoot)
        return inventory
    }

    @Test("Stage moves verified file to staging directory")
    func stageModel() async throws {
        let inventory = try makeTestInventory()
        try await inventory.ensureDirectories()
        let provider = AssetProvider(inventory: inventory)

        // Create a fake verified file.
        let tempFile = FileManager.default.temporaryDirectory
            .appendingPathComponent("verified-\(UUID().uuidString)")
        try "model data".write(to: tempFile, atomically: true, encoding: .utf8)

        let entry = await inventory.manifest.models[0]
        try await provider.stage(entry: entry, verifiedFile: tempFile)

        let status = await inventory.status(for: "test-model")
        #expect(status == .staged, "Model should be in staged state after staging")

        // Verify the file was moved.
        let stagedPath = inventory.stagingDirectory.appendingPathComponent("test-model")
        #expect(FileManager.default.fileExists(atPath: stagedPath.path))
    }

    @Test("Promote moves staged model to active directory")
    func promoteModel() async throws {
        let inventory = try makeTestInventory()
        try await inventory.ensureDirectories()
        let provider = AssetProvider(inventory: inventory)

        // Stage a file first.
        let tempFile = FileManager.default.temporaryDirectory
            .appendingPathComponent("verified-\(UUID().uuidString)")
        try "model data".write(to: tempFile, atomically: true, encoding: .utf8)

        let entry = await inventory.manifest.models[0]
        try await provider.stage(entry: entry, verifiedFile: tempFile)

        // Promote.
        try await provider.promote(modelId: "test-model")

        let activePath = inventory.activeDirectory.appendingPathComponent("test-model")
        #expect(FileManager.default.fileExists(atPath: activePath.path),
                "Model should exist in active directory after promotion")

        let status = await inventory.status(for: "test-model")
        if case .ready = status {
            // Expected.
        } else {
            #expect(Bool(false), "Status should be .ready after promotion, got \(status)")
        }
    }

    @Test("Promote with existing active version creates rollback")
    func promoteCreatesRollback() async throws {
        let inventory = try makeTestInventory()
        try await inventory.ensureDirectories()
        let provider = AssetProvider(inventory: inventory)
        let entry = await inventory.manifest.models[0]

        // Stage and promote v1.
        let v1 = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-\(UUID().uuidString)")
        try "v1 data".write(to: v1, atomically: true, encoding: .utf8)
        try await provider.stage(entry: entry, verifiedFile: v1)
        try await provider.promote(modelId: "test-model")

        // Stage and promote v2.
        let v2 = FileManager.default.temporaryDirectory
            .appendingPathComponent("v2-\(UUID().uuidString)")
        try "v2 data".write(to: v2, atomically: true, encoding: .utf8)
        try await provider.stage(entry: entry, verifiedFile: v2)
        try await provider.promote(modelId: "test-model")

        // Verify rollback exists.
        let rollbackPath = inventory.rollbackDirectory.appendingPathComponent("test-model")
        #expect(FileManager.default.fileExists(atPath: rollbackPath.path),
                "Previous version should be moved to rollback directory")

        // Verify active has v2 content.
        let activePath = inventory.activeDirectory.appendingPathComponent("test-model")
        let activeContent = try String(contentsOf: activePath, encoding: .utf8)
        #expect(activeContent == "v2 data")
    }

    @Test("Rollback restores previous version")
    func rollbackRestores() async throws {
        let inventory = try makeTestInventory()
        try await inventory.ensureDirectories()
        let provider = AssetProvider(inventory: inventory)
        let entry = await inventory.manifest.models[0]

        // Stage and promote v1, then v2.
        let v1 = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-\(UUID().uuidString)")
        try "v1 data".write(to: v1, atomically: true, encoding: .utf8)
        try await provider.stage(entry: entry, verifiedFile: v1)
        try await provider.promote(modelId: "test-model")

        let v2 = FileManager.default.temporaryDirectory
            .appendingPathComponent("v2-\(UUID().uuidString)")
        try "v2 data".write(to: v2, atomically: true, encoding: .utf8)
        try await provider.stage(entry: entry, verifiedFile: v2)
        try await provider.promote(modelId: "test-model")

        // Rollback.
        try await provider.rollback(modelId: "test-model")

        // Active should have v1 content.
        let activePath = inventory.activeDirectory.appendingPathComponent("test-model")
        let content = try String(contentsOf: activePath, encoding: .utf8)
        #expect(content == "v1 data", "Rollback should restore v1 data")
    }

    @Test("Rollback with no previous version throws")
    func rollbackWithNoPrevious() async throws {
        let inventory = try makeTestInventory()
        try await inventory.ensureDirectories()
        let provider = AssetProvider(inventory: inventory)

        await #expect(throws: AssetProviderError.self) {
            try await provider.rollback(modelId: "test-model")
        }
    }

    @Test("Promote with nothing staged throws")
    func promoteNothingStaged() async throws {
        let inventory = try makeTestInventory()
        try await inventory.ensureDirectories()
        let provider = AssetProvider(inventory: inventory)

        await #expect(throws: AssetProviderError.self) {
            try await provider.promote(modelId: "test-model")
        }
    }

    @Test("Checksum verification detects mismatched content")
    func checksumMismatch() async throws {
        let inventory = try makeTestInventory()
        let provider = AssetProvider(inventory: inventory)

        let tempFile = FileManager.default.temporaryDirectory
            .appendingPathComponent("checksum-test-\(UUID().uuidString)")
        try "some content".write(to: tempFile, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: tempFile) }

        let matches = try await provider.verifyChecksum(
            fileURL: tempFile, expected: "0000000000000000000000000000000000000000000000000000000000000000"
        )
        #expect(matches == false, "Checksum should not match arbitrary expected hash")
    }
}

// MARK: - ModelInventory

@Suite("ModelInventory - Scanning and Status")
struct ModelInventoryTests {

    @Test("Scan detects missing models")
    func scanDetectsMissing() async throws {
        let tempRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("InvTests-\(UUID().uuidString)")
        let manifest = ModelManifest(
            version: 1,
            generatedAt: .now,
            models: [
                ModelEntry(
                    id: "model-a",
                    role: .asrFast,
                    displayName: "Model A",
                    modelVersion: "1.0.0",
                    downloadURL: URL(string: "https://example.com/a.bin")!,
                    sha256: "aaa",
                    compressedSizeBytes: 100,
                    uncompressedSizeBytes: 200,
                    priority: 100,
                    minimumOS: "26.0",
                    requiredCapabilities: []
                )
            ]
        )
        let inventory = ModelInventory(manifest: manifest, rootOverride: tempRoot)
        try await inventory.scan()
        let status = await inventory.status(for: "model-a")
        #expect(status == .missing)
    }

    @Test("missingModels returns models sorted by priority")
    func missingModelsPriority() async throws {
        let tempRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("InvTests-\(UUID().uuidString)")
        let manifest = ModelManifest(
            version: 1,
            generatedAt: .now,
            models: [
                ModelEntry(id: "low", role: .classifier, displayName: "Low",
                           modelVersion: "1.0", downloadURL: URL(string: "https://example.com/l")!,
                           sha256: "l", compressedSizeBytes: 100, uncompressedSizeBytes: 200,
                           priority: 10, minimumOS: "26.0", requiredCapabilities: []),
                ModelEntry(id: "high", role: .asrFast, displayName: "High",
                           modelVersion: "1.0", downloadURL: URL(string: "https://example.com/h")!,
                           sha256: "h", compressedSizeBytes: 100, uncompressedSizeBytes: 200,
                           priority: 100, minimumOS: "26.0", requiredCapabilities: [])
            ]
        )
        let inventory = ModelInventory(manifest: manifest, rootOverride: tempRoot)
        try await inventory.scan()
        let missing = await inventory.missingModels()
        #expect(missing.count == 2)
        #expect(missing[0].id == "high", "Higher priority model should come first")
    }
}

// MARK: - SkipMode Enum

@Suite("SkipMode - Enum Behavior")
struct SkipModeTests {

    @Test("All cases have correct raw values")
    func rawValues() {
        #expect(SkipMode.shadow.rawValue == "shadow")
        #expect(SkipMode.manual.rawValue == "manual")
        #expect(SkipMode.auto.rawValue == "auto")
    }

    @Test("All cases are iterable")
    func allCases() {
        #expect(SkipMode.allCases.count == 3)
    }

    @Test("Invalid raw value returns nil")
    func invalidRawValue() {
        #expect(SkipMode(rawValue: "bogus") == nil)
    }
}

// MARK: - Runtime Composition & Resource Contracts

@Suite("Runtime Contracts - Composition and Resources")
struct RuntimeContractTests {

    @Test("Shared runtime owns long-lived playback and background services")
    func runtimeCompositionIsShared() throws {
        let runtimeSource = try readRepoSource("Playhead/App/PlayheadRuntime.swift")
        let contentViewSource = try readRepoSource("Playhead/App/ContentView.swift")
        let episodeListSource = try readRepoSource("Playhead/Views/Library/EpisodeListView.swift")
        let nowPlayingViewSource = try readRepoSource("Playhead/Views/NowPlaying/NowPlayingView.swift")
        let nowPlayingVMSource = try readRepoSource("Playhead/Views/NowPlaying/NowPlayingViewModel.swift")

        #expect(runtimeSource.contains("let playbackService: PlaybackService"))
        #expect(runtimeSource.contains("backgroundProcessingService.registerBackgroundTasks()"))
        #expect(runtimeSource.contains("let analysisStore: AnalysisStore"))
        #expect(runtimeSource.contains("let skipOrchestrator: SkipOrchestrator"))
        #expect(runtimeSource.contains("let modelInventory: ModelInventory"))
        #expect(runtimeSource.contains("let entitlementManager: EntitlementManager"))

        #expect(episodeListSource.contains("NowPlayingView(runtime: runtime"))
        #expect(!episodeListSource.contains("NowPlayingView()"))

        #expect(contentViewSource.contains("SettingsView("))
        #expect(contentViewSource.contains("inventory: runtime.modelInventory"))
        #expect(contentViewSource.contains("assetProvider: runtime.assetProvider"))

        #expect(nowPlayingVMSource.contains("let service = runtime.playbackService"))
        #expect(!nowPlayingVMSource.contains("PlaybackService()"))

        #expect(!nowPlayingViewSource.contains("NowPlayingViewModel()"))
    }

    @Test("Bundled model manifest is present and registered in the app target")
    func modelManifestIsBundled() throws {
        let manifestURL = repoRootURL().appendingPathComponent("Playhead/Resources/ModelManifest.json")
        #expect(FileManager.default.fileExists(atPath: manifestURL.path))

        let projectFile = try readRepoSource("Playhead.xcodeproj/project.pbxproj")
        #expect(projectFile.contains("ModelManifest.json in Resources"))

        let manifest = try ModelInventory.loadBundledManifest()
        #expect(!manifest.models.isEmpty)
        #expect(manifest.models.allSatisfy { !$0.id.isEmpty })
    }
}

@Suite("Runtime Contracts - Settings Storage")
struct SettingsStorageContractTests {

    @MainActor
    @Test("Storage sizes track the actual on-disk directories")
    func computeStorageSizesUsesCurrentServicePaths() async throws {
        let modelRoot = ModelInventory.defaultModelsRoot()
        let analysisShardsRoot = FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("AnalysisShards", isDirectory: true)
        let audioCacheRoot = DownloadManager.defaultCacheDirectory()

        try FileManager.default.createDirectory(at: modelRoot, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: analysisShardsRoot, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: audioCacheRoot, withIntermediateDirectories: true)

        let modelMarker = modelRoot.appendingPathComponent("model-marker.bin")
        let analysisMarker = analysisShardsRoot.appendingPathComponent("analysis-marker.bin")
        let audioMarker = audioCacheRoot.appendingPathComponent("audio-marker.bin")

        defer {
            try? FileManager.default.removeItem(at: modelMarker)
            try? FileManager.default.removeItem(at: analysisMarker)
            try? FileManager.default.removeItem(at: audioMarker)
        }

        try Data(repeating: 0x61, count: 11).write(to: modelMarker)
        try Data(repeating: 0x62, count: 13).write(to: analysisMarker)
        try Data(repeating: 0x63, count: 17).write(to: audioMarker)

        let viewModel = SettingsViewModel()
        await viewModel.computeStorageSizes()

        #expect(viewModel.modelFilesSize >= 11)
        #expect(viewModel.transcriptCacheSize >= 13)
        #expect(viewModel.cachedAudioSize >= 17)
    }
}
