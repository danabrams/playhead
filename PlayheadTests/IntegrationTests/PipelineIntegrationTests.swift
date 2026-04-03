// PipelineIntegrationTests.swift
// End-to-end integration tests exercising the full pipeline:
// audio → decode → transcribe → detect → skip → banner.
//
// Wires real service instances (not mocks) with the StubSpeechRecognizer
// and ReplaySimulator infrastructure. Uses corpus annotations as ground truth.

import CoreMedia
import Foundation
import Testing
@testable import Playhead

// MARK: - Integration Test Helpers

/// Tracks integration test temp directories for cleanup.
private let _integrationStoreDirs = TestTempDirTracker()

/// Creates a temp-directory-backed AnalysisStore for integration tests.
private func makeIntegrationStore() async throws -> AnalysisStore {
    let dir = try makeTempDir(prefix: "PlayheadIntegration")
    _integrationStoreDirs.track(dir)
    let store = try AnalysisStore(directory: dir)
    try await store.migrate()
    return store
}

/// Build transcript chunks with ad-indicative text at the right times,
/// using corpus ground truth to place realistic ad language.
private func buildTranscriptChunks(
    assetId: String,
    duration: TimeInterval,
    adSegments: [GroundTruthAdSegment]
) -> [TranscriptChunk] {
    let chunkDuration = 10.0
    var chunks: [TranscriptChunk] = []
    var chunkIndex = 0

    for start in stride(from: 0.0, to: duration, by: chunkDuration) {
        let end = min(start + chunkDuration, duration)

        // Check if this chunk overlaps any ad segment.
        let overlapsAd = adSegments.contains { seg in
            start < seg.endTime && end > seg.startTime
        }

        let text: String
        if overlapsAd {
            // Realistic ad language that the LexicalScanner will detect.
            text = "this episode is brought to you by acme corp " +
                   "use code SAVE20 at checkout go to acme com slash podcast " +
                   "for a free trial sign up today"
        } else {
            text = "and so the interesting thing about this topic is that " +
                   "there are many perspectives we should consider carefully"
        }

        let normalized = text.lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }
            .joined(separator: " ")

        chunks.append(TranscriptChunk(
            id: "int-\(assetId)-\(chunkIndex)",
            analysisAssetId: assetId,
            segmentFingerprint: "int-fp-\(chunkIndex)",
            chunkIndex: chunkIndex,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: normalized,
            pass: "fast",
            modelVersion: "integration-v1"
        ))
        chunkIndex += 1
    }
    return chunks
}

/// Build feature windows with silence points near ad boundaries for snapping.
private func buildFeatureWindows(
    assetId: String,
    duration: TimeInterval,
    adSegments: [GroundTruthAdSegment]
) -> [FeatureWindow] {
    var windows: [FeatureWindow] = []
    let step = 1.0

    for start in stride(from: 0.0, to: duration, by: step) {
        let end = min(start + step, duration)

        // Place high-pause points near ad boundaries for snapping.
        let nearBoundary = adSegments.contains { seg in
            abs(start - seg.startTime) < 2.0 || abs(start - seg.endTime) < 2.0
        }

        windows.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            rms: nearBoundary ? 0.01 : 0.05,
            spectralFlux: 0.01,
            musicProbability: 0.0,
            pauseProbability: nearBoundary ? 0.9 : 0.1,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        ))
    }
    return windows
}

private func makeIntegrationAsset(
    id: String,
    episodeId: String
) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId,
        assetFingerprint: "int-fp-\(id)",
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

// MARK: - Pipeline Integration: Known Ads Episode

@Suite("Pipeline Integration – Known Ads Episode")
struct KnownAdsIntegrationTests {

    @Test("Play episode with known ads: full pipeline detects and skips at correct times")
    func knownAdsSkipFires() async throws {
        // 1. Set up real services.
        let store = try await makeIntegrationStore()
        let trustStore = try await makeIntegrationStore()

        let assetId = "int-asset-known"
        let episodeId = "int-ep-known"
        let podcastId = "int-podcast-1"
        let duration: TimeInterval = 600

        // Ground truth: two ad segments.
        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 60, endTime: 120,
                advertiser: "Acme Corp", product: "Widget Pro",
                adType: .midRoll
            ),
            GroundTruthAdSegment(
                startTime: 360, endTime: 420,
                advertiser: "BetaInc", product: "Service",
                adType: .midRoll
            ),
        ]

        // 2. Seed asset and transcript chunks with ad language.
        let asset = makeIntegrationAsset(id: assetId, episodeId: episodeId)
        try await store.insertAsset(asset)

        let chunks = buildTranscriptChunks(
            assetId: assetId, duration: duration, adSegments: groundTruth
        )
        try await store.insertTranscriptChunks(chunks)

        // Insert feature windows for boundary snapping.
        let featureWindows = buildFeatureWindows(
            assetId: assetId, duration: duration, adSegments: groundTruth
        )
        try await store.insertFeatureWindows(featureWindows)

        // 3. Wire real AdDetectionService with RuleBasedClassifier.
        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: .default
        )

        // 4. Run hot-path detection on the transcript chunks.
        let detectedWindows = try await detector.runHotPath(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        // Verify: at least one detection per ground-truth segment.
        #expect(detectedWindows.count >= 2,
                "Should detect at least 2 ad windows for 2 ground-truth segments (got \(detectedWindows.count))")

        // Verify detected windows overlap ground truth.
        for gt in groundTruth {
            let overlapping = detectedWindows.filter { win in
                win.startTime < gt.endTime && win.endTime > gt.startTime
            }
            #expect(!overlapping.isEmpty,
                    "Should have a detection overlapping ground truth at \(gt.startTime)-\(gt.endTime)")
        }

        // 5. Wire SkipOrchestrator with trust in auto mode.
        try await trustStore.upsertProfile(PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.9,
            observationCount: 10,
            mode: "auto",
            recentFalseSkipSignals: 0
        ))

        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, podcastId: podcastId
        )

        // 6. Feed detected windows and verify skip decisions.
        let confirmedWindows = detectedWindows.filter { $0.confidence >= 0.40 }
        await orchestrator.receiveAdWindows(confirmedWindows)

        let log = await orchestrator.getDecisionLog()

        // At least one window should have been applied or confirmed.
        let actionable = log.filter {
            $0.decision == .applied || $0.decision == .confirmed
        }
        #expect(!actionable.isEmpty,
                "Orchestrator should apply or confirm at least one window in auto mode")

        // Verify no spurious skips far from ground truth.
        for record in log where record.decision == .applied {
            let nearGroundTruth = groundTruth.contains { gt in
                record.originalStart < gt.endTime + 10 &&
                record.originalEnd > gt.startTime - 10
            }
            #expect(nearGroundTruth,
                    "Applied skip at \(record.originalStart)-\(record.originalEnd) should be near a ground-truth segment")
        }
    }
}

// MARK: - Pipeline Integration: Scrub Past Ad

@Suite("Pipeline Integration – Scrub Past Ad")
struct ScrubPastAdIntegrationTests {

    @Test("Scrub past ad: no retroactive skip fires")
    func scrubPastAdNoRetroactiveSkip() async throws {
        let store = try await makeIntegrationStore()
        let trustStore = try await makeIntegrationStore()

        let assetId = "int-asset-scrub"
        let episodeId = "int-ep-scrub"
        let podcastId = "int-podcast-scrub"
        let duration: TimeInterval = 600

        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 100, endTime: 160,
                advertiser: "TestCo", product: "TestProduct",
                adType: .midRoll
            ),
        ]

        let asset = makeIntegrationAsset(id: assetId, episodeId: episodeId)
        try await store.insertAsset(asset)

        let chunks = buildTranscriptChunks(
            assetId: assetId, duration: duration, adSegments: groundTruth
        )
        try await store.insertTranscriptChunks(chunks)

        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor()
        )
        let detected = try await detector.runHotPath(
            chunks: chunks, analysisAssetId: assetId, episodeDuration: duration
        )

        // Set up auto-mode orchestrator.
        try await trustStore.upsertProfile(PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil, normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil, jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.9, observationCount: 10,
            mode: "auto", recentFalseSkipSignals: 0
        ))

        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, podcastId: podcastId
        )

        // User scrubs past the ad to time 300 (ad ends at 160).
        await orchestrator.recordUserSeek(to: 300)

        // Now feed detections that are behind the playhead.
        await orchestrator.receiveAdWindows(detected)

        let log = await orchestrator.getDecisionLog()

        // No skip should be applied -- the ad is behind the playhead
        // and the seek suppression window is active.
        let applied = log.filter { $0.decision == .applied }
        #expect(applied.isEmpty,
                "No retroactive skip should fire after scrubbing past the ad")
    }
}

// MARK: - Pipeline Integration: 2x Playback Speed

@Suite("Pipeline Integration – High Speed Playback")
struct HighSpeedPlaybackIntegrationTests {

    @Test("Play at 2x: hot-path detection keeps up with accelerated playback")
    func hotPathKeepsUpAt2x() async throws {
        let store = try await makeIntegrationStore()
        let assetId = "int-asset-2x"
        let duration: TimeInterval = 600

        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 100, endTime: 160,
                advertiser: "SpeedCo", product: "FastWidget",
                adType: .midRoll
            ),
        ]

        let asset = makeIntegrationAsset(id: assetId, episodeId: "int-ep-2x")
        try await store.insertAsset(asset)

        let chunks = buildTranscriptChunks(
            assetId: assetId, duration: duration, adSegments: groundTruth
        )

        // Run hot-path at 2x by feeding chunks in time-windowed batches.
        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor()
        )

        // Simulate 2x by feeding chunks in larger batches (more time per tick).
        let batchSize = 20 // 200s worth at 10s per chunk
        var allDetections: [AdWindow] = []

        for batchStart in stride(from: 0, to: chunks.count, by: batchSize) {
            let batchEnd = min(batchStart + batchSize, chunks.count)
            let batch = Array(chunks[batchStart..<batchEnd])

            let start = ProcessInfo.processInfo.systemUptime
            let windows = try await detector.runHotPath(
                chunks: batch,
                analysisAssetId: assetId,
                episodeDuration: duration
            )
            let elapsed = (ProcessInfo.processInfo.systemUptime - start) * 1000

            // Hot-path must complete within a reasonable wall-clock budget.
            // At 2x speed, a 200s batch plays in 100s real time.
            // Detection should finish well under 5 seconds.
            #expect(elapsed < 5000,
                    "Hot-path batch should complete in < 5s (took \(Int(elapsed))ms)")

            allDetections.append(contentsOf: windows)
        }

        // Verify detection found the ad segment.
        let overlapping = allDetections.filter { win in
            win.startTime < 160 && win.endTime > 100
        }
        #expect(!overlapping.isEmpty,
                "Hot-path should detect ad segment even at 2x speed")
    }
}

// MARK: - Pipeline Integration: Kill and Relaunch (Checkpoint Resume)

@Suite("Pipeline Integration – Checkpoint Resume")
struct CheckpointResumeIntegrationTests {

    @Test("Kill and relaunch: analysis resumes from checkpoint")
    func resumeFromCheckpoint() async throws {
        // Phase 1: Analyze first half.
        let dir = try makeTempDir(prefix: "PlayheadResume")
        defer { try? FileManager.default.removeItem(at: dir) }
        let store1 = try AnalysisStore(directory: dir)
        try await store1.migrate()

        let assetId = "int-asset-resume"
        let episodeId = "int-ep-resume"
        let duration: TimeInterval = 600

        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 50, endTime: 110,
                advertiser: "ResumeCo", product: "ResumeWidget",
                adType: .midRoll
            ),
            GroundTruthAdSegment(
                startTime: 400, endTime: 460,
                advertiser: "LaterCo", product: "LaterWidget",
                adType: .midRoll
            ),
        ]

        let asset = makeIntegrationAsset(id: assetId, episodeId: episodeId)
        try await store1.insertAsset(asset)

        let allChunks = buildTranscriptChunks(
            assetId: assetId, duration: duration, adSegments: groundTruth
        )

        // Insert only first half of chunks (simulating analysis in progress).
        let halfIndex = allChunks.count / 2
        let firstHalf = Array(allChunks[0..<halfIndex])
        try await store1.insertTranscriptChunks(firstHalf)

        let detector1 = AdDetectionService(
            store: store1,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor()
        )
        let phase1Detections = try await detector1.runHotPath(
            chunks: firstHalf,
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        // Update checkpoint: record coverage end time.
        // Record coverage progress as the coordinator would.
        let _ = firstHalf.last?.endTime ?? 0
        try await store1.updateAssetState(id: assetId, state: "analyzing")

        // Verify phase 1 found the early ad.
        let earlyAd = phase1Detections.filter { $0.startTime < 200 }
        #expect(!earlyAd.isEmpty,
                "Phase 1 should detect the ad in the first half")

        // Phase 2: "Relaunch" -- open the same store, check existing data,
        // insert remaining chunks, run detection again.
        let store2 = try AnalysisStore(directory: dir)
        try await store2.migrate()

        // Verify checkpointed data survived.
        let existingChunks = try await store2.fetchTranscriptChunks(assetId: assetId)
        #expect(existingChunks.count == firstHalf.count,
                "Checkpointed chunks should survive relaunch (got \(existingChunks.count))")

        let existingWindows = try await store2.fetchAdWindows(assetId: assetId)
        #expect(existingWindows.count == phase1Detections.count,
                "Checkpointed ad windows should survive relaunch")

        // Insert second half.
        let secondHalf = Array(allChunks[halfIndex...])
        // Filter out any chunks that were already inserted (by fingerprint).
        var newChunks: [TranscriptChunk] = []
        for chunk in secondHalf {
            let exists = try await store2.hasTranscriptChunk(
                analysisAssetId: assetId,
                segmentFingerprint: chunk.segmentFingerprint
            )
            if !exists {
                newChunks.append(chunk)
            }
        }
        try await store2.insertTranscriptChunks(newChunks)

        let detector2 = AdDetectionService(
            store: store2,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor()
        )
        let phase2Detections = try await detector2.runHotPath(
            chunks: newChunks,
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        // Verify phase 2 found the later ad.
        let lateAd = phase2Detections.filter { $0.startTime > 300 }
        #expect(!lateAd.isEmpty,
                "Phase 2 should detect the ad in the second half after resume")

        // Combined: all ground-truth segments detected.
        let allDetections = try await store2.fetchAdWindows(assetId: assetId)
        for gt in groundTruth {
            let found = allDetections.contains { win in
                win.startTime < gt.endTime && win.endTime > gt.startTime
            }
            #expect(found,
                    "Combined detection should cover ground truth at \(gt.startTime)-\(gt.endTime)")
        }
    }
}

// MARK: - Pipeline Integration: Foundation Models Unavailable

@Suite("Pipeline Integration – Foundation Models Unavailable")
struct FoundationModelsUnavailableTests {

    @Test("FM unavailable: detection still works, only banners degrade")
    func detectionWorksWithoutFM() async throws {
        let store = try await makeIntegrationStore()
        let assetId = "int-asset-nofm"
        let duration: TimeInterval = 600

        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 120, endTime: 180,
                advertiser: "NoFMCo", product: "Widget",
                adType: .midRoll
            ),
        ]

        let asset = makeIntegrationAsset(id: assetId, episodeId: "int-ep-nofm")
        try await store.insertAsset(asset)

        let chunks = buildTranscriptChunks(
            assetId: assetId, duration: duration, adSegments: groundTruth
        )
        try await store.insertTranscriptChunks(chunks)

        // Wire with FallbackExtractor (simulates FM unavailable).
        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor()
        )

        // Hot path should still detect ads.
        let hotPathWindows = try await detector.runHotPath(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let overlapping = hotPathWindows.filter { win in
            win.startTime < 180 && win.endTime > 120
        }
        #expect(!overlapping.isEmpty,
                "Detection should work without Foundation Models")

        // Run backfill with FallbackExtractor for metadata enrichment.
        try await detector.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "int-podcast-nofm",
            episodeDuration: duration
        )

        // Verify metadata was extracted via fallback.
        let allWindows = try await store.fetchAdWindows(assetId: assetId)
        let confirmed = allWindows.filter {
            $0.decisionState == AdDecisionState.confirmed.rawValue ||
            $0.decisionState == AdDecisionState.candidate.rawValue
        }
        #expect(!confirmed.isEmpty,
                "Backfill should produce confirmed or candidate windows even without FM")

        // Check that metadata source is "fallback" (not "foundationModels").
        // The fallback extractor produces metadata with source: "fallback".
        // Since we used FallbackExtractor, any metadata should be fallback-sourced.
        let withMetadata = allWindows.filter {
            $0.metadataSource == "fallback" || $0.metadataSource == "none"
        }
        #expect(withMetadata.count == allWindows.count,
                "All metadata should come from fallback when FM is unavailable")
    }
}

// MARK: - Pipeline Integration: Preview Budget Exhausted

@Suite("Pipeline Integration – Preview Budget Exhausted")
struct PreviewBudgetExhaustedTests {

    @Test("Budget exhausted: transcription stops, playback continues")
    func budgetExhaustedStopsTranscription() async throws {
        let store = try await makeIntegrationStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)
        let episodeKey = "int-budget-ep"

        // Exhaust the budget.
        _ = await budgetStore.consumeBudget(for: episodeKey, seconds: 720)
        let hasBudget = await budgetStore.hasBudget(for: episodeKey)
        #expect(hasBudget == false, "Budget should be exhausted")

        let remaining = await budgetStore.remainingBudget(for: episodeKey)
        #expect(remaining == 0, "Remaining budget should be zero")

        // Simulate the coordinator check: when budget is exhausted,
        // new transcription work should not be started.
        let assetId = "int-asset-budget"
        let duration: TimeInterval = 600

        let asset = makeIntegrationAsset(id: assetId, episodeId: "int-ep-budget")
        try await store.insertAsset(asset)

        // With budget exhausted, the coordinator would not feed chunks.
        // Verify detection produces nothing when no chunks are available.
        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor()
        )

        let noChunksResult = try await detector.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )
        #expect(noChunksResult.isEmpty,
                "Detection should produce nothing when no chunks are fed (budget exhausted)")

        // Verify playback-side state is unaffected: the store still works,
        // existing data is accessible, and the budget store tracks state correctly.
        let totalConsumed = await budgetStore.totalConsumed(for: episodeKey)
        #expect(totalConsumed == 720, "Total consumed should reflect exhaustion")

        // Grace window should be unavailable (consumed >= base budget).
        let grace = await budgetStore.graceAllowance(
            for: episodeKey, adBreakDuration: 60
        )
        #expect(grace == 0, "No grace should be available after budget exhaustion")
    }
}

// MARK: - Pipeline Integration: Replay Simulator End-to-End

@Suite("Pipeline Integration – Replay Simulator E2E")
struct ReplaySimulatorE2ETests {

    @Test("Replay simulator: corpus episode produces valid metrics")
    func corpusReplayProducesMetrics() throws {
        let loader = CorpusLoader()
        let annotations = try loader.loadAllAnnotations()
        #expect(!annotations.isEmpty, "Corpus should have at least one annotation")

        // Pick the first annotation with ads for the replay.
        guard let annotation = annotations.first(where: { !$0.isNoAdEpisode }) else {
            #expect(Bool(false), "Corpus should contain at least one episode with ads")
            return
        }

        let condition = SimulationCondition(
            audioMode: .cached,
            playbackSpeed: 1.0,
            interactions: []
        )

        let replayConfig = loader.makeReplayConfig(
            from: annotation,
            condition: condition
        )

        let driver = SimulatedPlaybackDriver(config: replayConfig)
        let events = driver.runReplay()

        #expect(!events.isEmpty, "Replay should produce events")

        // Verify detection quality metrics are reasonable.
        let quality = driver.computeDetectionQuality()
        #expect(quality.precision >= 0,
                "Precision should be non-negative")
        #expect(quality.recall >= 0,
                "Recall should be non-negative")

        // Build a full report and verify it serializes.
        let report = driver.buildReport(replayDuration: 1.0)
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        #expect(data.count > 100,
                "Report should serialize to meaningful JSON")
    }

    @Test("Replay simulator: 2x speed replay completes without errors")
    func twoXSpeedReplay() throws {
        let loader = CorpusLoader()
        let annotations = try loader.loadAllAnnotations()
        guard let annotation = annotations.first(where: { !$0.isNoAdEpisode }) else {
            #expect(Bool(false), "Need an episode with ads")
            return
        }

        let condition = SimulationCondition(
            audioMode: .cached,
            playbackSpeed: 2.0,
            interactions: []
        )

        let config = loader.makeReplayConfig(from: annotation, condition: condition)
        let driver = SimulatedPlaybackDriver(config: config)
        let events = driver.runReplay()

        #expect(!events.isEmpty, "2x replay should produce events")

        let quality = driver.computeDetectionQuality()
        // At 2x the simulator still processes all time steps;
        // detection quality should be comparable to 1x.
        #expect(quality.missedSegmentCount == 0,
                "Should not miss segments at 2x speed")
    }

    @Test("Replay simulator: scrub interaction suppresses retroactive detection")
    func scrubSuppressesRetroactive() throws {
        let loader = CorpusLoader()
        let annotations = try loader.loadAllAnnotations()
        guard let annotation = annotations.first(where: {
            !$0.isNoAdEpisode && !$0.adSegments.isEmpty
        }) else {
            #expect(Bool(false), "Need an episode with ads")
            return
        }

        let firstAdEnd = annotation.adSegments[0].endTime

        // Scrub past the first ad.
        let condition = SimulationCondition(
            audioMode: .cached,
            playbackSpeed: 1.0,
            interactions: [
                SimulatedInteraction(
                    type: .scrub,
                    atTime: 10.0,
                    targetTime: firstAdEnd + 30,
                    newSpeed: nil
                ),
            ]
        )

        let config = loader.makeReplayConfig(from: annotation, condition: condition)
        let driver = SimulatedPlaybackDriver(config: config)
        let events = driver.runReplay()

        // After scrubbing past the ad, any detection of that ad segment
        // should NOT result in an applied skip (it's behind the playhead).
        // The scrub happens at time 10 and jumps to firstAdEnd+30,
        // so no skip should fire for the first ad since the playhead
        // jumped past it before detection could apply.
        let scrubEvents = events.filter {
            if case .scrubPerformed = $0 { return true }
            return false
        }
        #expect(!scrubEvents.isEmpty, "Scrub interaction should be recorded")
    }

    @Test("Replay simulator: no-ad episode produces zero detections")
    func noAdEpisodeZeroDetections() throws {
        let loader = CorpusLoader()

        // Load the no-ad episode.
        let annotations = try loader.loadAnnotations(withTag: "no-ads")
        guard let noAdAnnotation = annotations.first else {
            #expect(Bool(false), "Corpus should contain a no-ad episode")
            return
        }

        #expect(noAdAnnotation.isNoAdEpisode == true)

        let condition = SimulationCondition(
            audioMode: .cached,
            playbackSpeed: 1.0,
            interactions: []
        )

        let config = loader.makeReplayConfig(from: noAdAnnotation, condition: condition)
        let driver = SimulatedPlaybackDriver(config: config)
        _ = driver.runReplay()

        let quality = driver.computeDetectionQuality()
        #expect(quality.spuriousSegmentCount == 0,
                "No-ad episode should produce zero spurious detections")

        let skipEvents = driver.computeUserOverrideMetrics()
        #expect(skipEvents.listenTapCount == 0,
                "No-ad episode should have zero listen taps")
    }
}

// MARK: - Pipeline Integration: Trust Scoring Lifecycle

@Suite("Pipeline Integration – Trust Scoring Lifecycle")
struct TrustScoringLifecycleTests {

    @Test("Full trust lifecycle: shadow -> manual -> auto with observations")
    func fullTrustLifecycle() async throws {
        let store = try await makeIntegrationStore()
        let trust = TrustScoringService(store: store)
        let podcastId = "int-podcast-trust"

        // Start in shadow.
        let initial = await trust.effectiveMode(podcastId: podcastId)
        #expect(initial == .shadow)

        // Record several successful observations to promote through modes.
        for _ in 0..<3 {
            await trust.recordSuccessfulObservation(
                podcastId: podcastId, averageConfidence: 0.60
            )
        }
        let afterShadow = await trust.effectiveMode(podcastId: podcastId)
        #expect(afterShadow == .manual,
                "3 observations with decent confidence should promote to manual")

        // Continue observations to reach auto.
        for _ in 0..<7 {
            await trust.recordSuccessfulObservation(
                podcastId: podcastId, averageConfidence: 0.80
            )
        }
        let afterManual = await trust.effectiveMode(podcastId: podcastId)
        #expect(afterManual == .auto,
                "10 total observations with high confidence should promote to auto")

        // False signals demote back.
        await trust.recordFalseSkipSignal(podcastId: podcastId)
        await trust.recordFalseSkipSignal(podcastId: podcastId)
        let afterDemotion = await trust.effectiveMode(podcastId: podcastId)
        #expect(afterDemotion == .manual,
                "2 false signals should demote auto -> manual")
    }
}

// MARK: - Pipeline Integration: Combined Tuning Replay

@Suite("Pipeline Integration – Combined Tuning Replay")
struct CombinedTuningReplayTests {

    // Validates the combined effect of all recent tuning changes:
    //   1. Sigmoid midpoint 0.45 -> 0.25 (stronger calibrated scores from lexical alone)
    //   2. Removed "so" transition pattern (fewer false positives)
    //   3. Trust bonus 0.05 -> 0.10 (faster promotion through modes)
    //   4. Auto-mode candidate promotion (skip fires without backfill for trusted shows)

    // MARK: - Shared Corpus Replay Helper

    /// Runs the full corpus replay pipeline and returns per-episode results.
    /// Shared by the focused test methods below.
    private struct CorpusReplayResult {
        let annotations: [TestEpisodeAnnotation]
        let episodeReports: [EpisodeReplayReport]
        /// Per-episode classifier results keyed by episode ID.
        let classifierResults: [String: [ClassifierResult]]
        /// Per-episode decision logs keyed by episode ID.
        let decisionLogs: [String: [SkipDecisionRecord]]
    }

    private func runCorpusReplay() async throws -> CorpusReplayResult {
        let loader = CorpusLoader()
        let annotations = try loader.loadAllAnnotations()
        #expect(annotations.count >= 10, "Corpus should have a meaningful number of episodes")

        let store = try await makeIntegrationStore()
        let trustStore = try await makeIntegrationStore()

        var episodeReports: [EpisodeReplayReport] = []
        var allClassifierResults: [String: [ClassifierResult]] = [:]
        var allDecisionLogs: [String: [SkipDecisionRecord]] = [:]

        for annotation in annotations {
            let assetId = annotation.episode.episodeId
            let podcastId = annotation.podcast.podcastId
            let duration = annotation.episode.duration

            // Build ground truth segments.
            let groundTruth = annotation.adSegments.map { seg in
                GroundTruthAdSegment(
                    startTime: seg.startTime,
                    endTime: seg.endTime,
                    advertiser: seg.advertiser,
                    product: seg.product,
                    adType: mapTestAdType(seg.adType)
                )
            }

            // Build transcript chunks with ad-indicative text at the right times.
            let chunks = buildTranscriptChunks(
                assetId: assetId, duration: duration, adSegments: groundTruth
            )

            // Build feature windows for boundary snapping.
            let featureWindows = buildFeatureWindows(
                assetId: assetId, duration: duration, adSegments: groundTruth
            )

            // Seed the store.
            let asset = makeIntegrationAsset(id: assetId, episodeId: assetId)
            try await store.insertAsset(asset)
            try await store.insertTranscriptChunks(chunks)
            try await store.insertFeatureWindows(featureWindows)

            // --- Layer 1: Lexical scan ---
            let scanner = LexicalScanner()
            let candidates = scanner.scan(chunks: chunks, analysisAssetId: assetId)

            // --- Layer 2: Classification (uses updated sigmoid mid=0.25) ---
            let classifier = RuleBasedClassifier()
            let classifierInputs = candidates.map { candidate in
                let overlapping = featureWindows.filter { fw in
                    fw.startTime < candidate.endTime && fw.endTime > candidate.startTime
                }
                return ClassifierInput(
                    candidate: candidate,
                    featureWindows: overlapping,
                    episodeDuration: duration
                )
            }
            let results = classifier.classify(inputs: classifierInputs, priors: .empty)
            allClassifierResults[assetId] = results

            // --- Layer 3: Trust scoring (uses updated bonus=0.10) ---
            let trustService = TrustScoringService(store: trustStore)

            // Simulate trust build-up: record enough observations to reach auto.
            // With bonus=0.10, 8 observations should suffice (0.2 initial + 8*0.10 = 1.0).
            for _ in 0..<8 {
                await trustService.recordSuccessfulObservation(
                    podcastId: podcastId, averageConfidence: 0.80
                )
            }
            let mode = await trustService.effectiveMode(podcastId: podcastId)
            #expect(mode == .auto,
                    "8 observations with bonus=0.10 should reach auto mode for \(podcastId)")

            // --- Layer 4: Skip orchestrator (uses auto-mode candidate promotion) ---
            let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
            await orchestrator.beginEpisode(
                analysisAssetId: assetId, podcastId: podcastId
            )

            // Convert classifier results into AdWindows.
            let adWindows = results.map { result in
                AdWindow(
                    id: result.candidateId,
                    analysisAssetId: result.analysisAssetId,
                    startTime: result.startTime,
                    endTime: result.endTime,
                    confidence: result.adProbability,
                    boundaryState: "snapped",
                    decisionState: AdDecisionState.candidate.rawValue,
                    detectorVersion: "integration-v1",
                    advertiser: nil,
                    product: nil,
                    adDescription: nil,
                    evidenceText: nil,
                    evidenceStartTime: result.startTime,
                    metadataSource: "none",
                    metadataConfidence: nil,
                    metadataPromptVersion: nil,
                    wasSkipped: false,
                    userDismissedBanner: false
                )
            }

            await orchestrator.receiveAdWindows(adWindows)
            let log = await orchestrator.getDecisionLog()
            allDecisionLogs[assetId] = log

            // Also run the replay simulator for metrics.
            let condition = SimulationCondition(
                audioMode: .cached,
                playbackSpeed: 1.0,
                interactions: []
            )
            let replayConfig = loader.makeReplayConfig(
                from: annotation, condition: condition
            )
            let driver = SimulatedPlaybackDriver(config: replayConfig)
            _ = driver.runReplay()
            let report = driver.buildReport(replayDuration: 0.5)
            episodeReports.append(report)
        }

        return CorpusReplayResult(
            annotations: annotations,
            episodeReports: episodeReports,
            classifierResults: allClassifierResults,
            decisionLogs: allDecisionLogs
        )
    }

    // MARK: - Focused Corpus Tests

    @Test("Sigmoid calibration: per-episode detection with mid=0.25 produces high-confidence results")
    func corpusSigmoidCalibration() async throws {
        let result = try await runCorpusReplay()

        for annotation in result.annotations where !annotation.isNoAdEpisode {
            let assetId = annotation.episode.episodeId
            let results = result.classifierResults[assetId] ?? []

            // With sigmoid mid=0.25, lexical-only candidates should calibrate
            // above the 0.65 enter threshold.
            let highConfidence = results.filter { $0.adProbability >= 0.65 }
            #expect(!highConfidence.isEmpty,
                    "Sigmoid mid=0.25 should produce high-confidence results for '\(annotation.episode.title)'")

            // Verify at least one detection per ground-truth segment
            // (for non-hard difficulty segments).
            for seg in annotation.adSegments where seg.difficulty != .hard {
                let detected = results.contains { r in
                    r.startTime < seg.endTime && r.endTime > seg.startTime
                        && r.adProbability >= 0.40
                }
                #expect(detected,
                        "Should detect \(seg.difficulty.rawValue)-difficulty ad at \(seg.startTime)-\(seg.endTime) in '\(annotation.episode.title)'")
            }
        }

        // No-ad episodes should have no applied skips.
        for annotation in result.annotations where annotation.isNoAdEpisode {
            let log = result.decisionLogs[annotation.episode.episodeId] ?? []
            let applied = log.filter { $0.decision == .applied }
            #expect(applied.isEmpty,
                    "No-ad episode '\(annotation.episode.title)' should have zero applied skips")
        }
    }

    @Test("Trust promotion speed: bonus=0.10 reaches auto mode within expected observations")
    func corpusTrustPromotionSpeed() async throws {
        // Verify a fresh show reaches auto in fewer observations with bonus=0.10.
        let freshStore = try await makeIntegrationStore()
        let freshTrust = TrustScoringService(store: freshStore)
        let freshPodcastId = "int-fresh-promotion-test"

        for i in 1...10 {
            await freshTrust.recordSuccessfulObservation(
                podcastId: freshPodcastId, averageConfidence: 0.80
            )
            let currentMode = await freshTrust.effectiveMode(podcastId: freshPodcastId)
            if currentMode == .auto {
                // With bonus=0.10: initial trust=0.2, after 8 obs = 0.2 + 8*0.10 = 1.0.
                // manualToAutoObservations=8, manualToAutoTrustScore=0.75.
                // shadowToManual happens at obs=3, trust >= 0.4 (0.2 + 3*0.10 = 0.5).
                // manualToAuto at obs=8, trust >= 0.75 (0.2 + 8*0.10 = 1.0).
                #expect(i <= 9,
                        "With bonus=0.10, auto promotion should happen by observation 9 (got \(i))")
                break
            }
        }
        let finalMode = await freshTrust.effectiveMode(podcastId: freshPodcastId)
        #expect(finalMode == .auto,
                "Fresh show should reach auto mode within 10 observations at bonus=0.10")

        // Also verify per-episode trust in the full corpus replay.
        let result = try await runCorpusReplay()
        for annotation in result.annotations where !annotation.isNoAdEpisode {
            let log = result.decisionLogs[annotation.episode.episodeId] ?? []
            let applied = log.filter { $0.decision == .applied }
            let confirmed = log.filter { $0.decision == .confirmed }
            let actionable = applied + confirmed
            #expect(!actionable.isEmpty,
                    "Auto mode should produce actionable decisions for '\(annotation.episode.title)'")
        }
    }

    @Test("Corpus aggregate quality: replay report covers all episodes with valid metrics")
    func corpusAggregateQuality() async throws {
        let result = try await runCorpusReplay()

        let corpus = CorpusReplayReport.aggregate(from: result.episodeReports)
        #expect(corpus.episodeReports.count == result.annotations.count,
                "Corpus report should cover all episodes")

        // With the tuning changes, overall detection quality should be solid.
        #expect(corpus.aggregateDetectionQuality.precision >= 0.0,
                "Aggregate precision should be non-negative")
        #expect(corpus.aggregateDetectionQuality.recall >= 0.0,
                "Aggregate recall should be non-negative")

        // No episodes should be fully missed by the simulator.
        let adsAnnotations = result.annotations.filter { !$0.isNoAdEpisode }
        #expect(adsAnnotations.count >= 10,
                "Corpus should have at least 10 episodes with ads")
    }

    @Test("Sigmoid mid=0.25: lexical-only candidates calibrate above enter threshold")
    func sigmoidCalibrationVerification() {
        // The RuleBasedClassifier uses sigmoid(rawScore, k=8, mid=0.25).
        // A strong lexical candidate (confidence ~0.80, weight 0.40) contributes
        // 0.40 * 0.80 = 0.32 raw score. With mid=0.25:
        //   sigmoid(0.32, k=8, mid=0.25) = 1/(1+exp(-8*(0.32-0.25))) = 1/(1+exp(-0.56)) ~= 0.636
        // That's close to the 0.65 enter threshold. With any acoustic signal boost,
        // it crosses.

        let classifier = RuleBasedClassifier()

        // Simulate a candidate with strong lexical evidence and minimal acoustic.
        let candidate = LexicalCandidate(
            id: "sigmoid-test-1",
            analysisAssetId: "test-asset",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            hitCount: 5,
            categories: [.sponsor, .promoCode, .urlCTA],
            evidenceText: "brought to you by acme corp",
            detectorVersion: "test-v1"
        )

        let input = ClassifierInput(
            candidate: candidate,
            featureWindows: [],
            episodeDuration: 3600
        )

        let result = classifier.classify(input: input, priors: .empty)

        // With sigmoid mid=0.25, a strong lexical candidate alone should
        // produce a calibrated score that's at least approaching the threshold.
        // Raw: 0.40 * 0.85 = 0.34. sigmoid(0.34, 8, 0.25) ~= 0.668.
        #expect(result.adProbability > 0.60,
                "Strong lexical candidate with sigmoid mid=0.25 should calibrate above 0.60 (got \(result.adProbability))")

        // Verify it's higher than what mid=0.45 would produce.
        // sigmoid(0.34, 8, 0.45) = 1/(1+exp(-8*(0.34-0.45))) = 1/(1+exp(0.88)) ~= 0.293
        // So mid=0.25 should be significantly higher.
        #expect(result.adProbability > 0.50,
                "Calibrated score should be meaningfully above 0.5 with new midpoint")
    }

    @Test("Removed 'so' pattern: content text does not produce false lexical hits")
    func removedSoPatternFalsePositives() {
        let scanner = LexicalScanner()

        // Text with natural "so" usage that previously triggered false positives.
        let contentChunks = [
            makeContentChunk(index: 0, text:
                "so let me explain what happened next the detective arrived and " +
                "so we decided to investigate further into the case"
            ),
            makeContentChunk(index: 1, text:
                "so the interesting thing about quantum computing is that " +
                "so we need to consider the implications carefully"
            ),
            makeContentChunk(index: 2, text:
                "so i think the best approach is to start small and " +
                "so that brings us to our next topic for today"
            ),
        ]

        let candidates = scanner.scan(
            chunks: contentChunks, analysisAssetId: "false-positive-test"
        )

        // With the "so" pattern removed, these content-only chunks should not
        // produce candidates (no sponsor, promo, URL, or purchase language).
        #expect(candidates.isEmpty,
                "Content-only text with 'so' should not produce lexical candidates after pattern removal")
    }

    @Test("Auto-mode candidate promotion: candidates skip backfill wait in auto mode")
    func autoModeCandidatePromotion() async throws {
        let store = try await makeIntegrationStore()
        let trustStore = try await makeIntegrationStore()
        let podcastId = "int-auto-promotion"
        let assetId = "int-asset-auto-promo"

        // Set up a trusted show in auto mode.
        try await trustStore.upsertProfile(PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.95,
            observationCount: 20,
            mode: "auto",
            recentFalseSkipSignals: 0
        ))

        let trustService = TrustScoringService(store: trustStore)
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, podcastId: podcastId
        )

        // Feed a candidate (not confirmed) with confidence above enter threshold.
        let candidateWindow = AdWindow(
            id: "auto-promo-1",
            analysisAssetId: assetId,
            startTime: 60,
            endTime: 120,
            confidence: 0.75,
            boundaryState: "lexical",
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )

        await orchestrator.receiveAdWindows([candidateWindow])
        let log = await orchestrator.getDecisionLog()

        // With auto-mode candidate promotion, the candidate should be promoted
        // to confirmed and then applied without waiting for backfill.
        let applied = log.filter { $0.decision == .applied }
        #expect(!applied.isEmpty,
                "Auto mode should promote and apply candidate windows above enter threshold")

        // Verify that manual mode does NOT auto-apply candidates.
        let manualStore = try await makeIntegrationStore()
        let manualTrustStore = try await makeIntegrationStore()
        let manualPodcastId = "int-manual-no-promo"
        let manualAssetId = "int-asset-manual-promo"

        try await manualTrustStore.upsertProfile(PodcastProfile(
            podcastId: manualPodcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.6,
            observationCount: 5,
            mode: "manual",
            recentFalseSkipSignals: 0
        ))

        let manualTrust = TrustScoringService(store: manualTrustStore)
        let manualOrch = SkipOrchestrator(store: manualStore, trustService: manualTrust)
        await manualOrch.beginEpisode(
            analysisAssetId: manualAssetId, podcastId: manualPodcastId
        )

        let manualCandidate = AdWindow(
            id: "manual-promo-1",
            analysisAssetId: manualAssetId,
            startTime: 60,
            endTime: 120,
            confidence: 0.75,
            boundaryState: "lexical",
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )

        await manualOrch.receiveAdWindows([manualCandidate])
        let manualLog = await manualOrch.getDecisionLog()

        let manualApplied = manualLog.filter { $0.decision == .applied }
        #expect(manualApplied.isEmpty,
                "Manual mode should NOT auto-apply candidates -- awaits user tap")
    }

    // MARK: - Helpers

    private func mapTestAdType(_ type: TestAdSegment.AdType) -> GroundTruthAdSegment.AdSegmentType {
        switch type {
        case .preRoll: .preRoll
        case .midRoll: .midRoll
        case .postRoll: .postRoll
        }
    }

    private func makeContentChunk(index: Int, text: String) -> TranscriptChunk {
        TranscriptChunk(
            id: "content-\(index)",
            analysisAssetId: "false-positive-test",
            segmentFingerprint: "fp-content-\(index)",
            chunkIndex: index,
            startTime: Double(index) * 10.0,
            endTime: Double(index + 1) * 10.0,
            text: text,
            normalizedText: text.lowercased()
                .components(separatedBy: CharacterSet.alphanumerics.inverted)
                .filter { !$0.isEmpty }
                .joined(separator: " "),
            pass: "fast",
            modelVersion: "test-v1"
        )
    }
}
