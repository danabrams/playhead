// NoFoundationModelsTests.swift
// playhead-rk7: degraded-conditions E2E — Foundation Models unavailable.
//
// Asserts the app remains useful when `CapabilitiesService` reports
// `foundationModelsAvailable = false`:
//   1. The hot-path detection layer (lexical scanner + rule-based
//      classifier + boundary refinement) still produces AdWindows when
//      driven against a known-ad transcript. FM is never the primary
//      classifier — ClassifierService.RuleBasedClassifier is what fires.
//   2. The metadata extractor degrades to `FallbackExtractor`
//      (lexical-only). Banner copy renders the generic "Skipped sponsor
//      segment" line via `AdBannerView.bannerCopy(for:)` because the
//      fallback's confidence (0.30 max) sits below the
//      `metadataConfidenceThreshold` (0.60).
//   3. `CapabilitySnapshot` round-trips correctly through JSON encode/
//      decode and `canUseFoundationModels` returns false.
//
// What is NOT testable in-process (deferred to manual QA / real device):
//   * The actual Foundation Models framework runtime path. We can only
//     stub the `foundationModelsAvailable` flag on a `CapabilitySnapshot`
//     — we cannot toggle the device-level Apple Intelligence setting
//     from a test process.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-rk7 - no Foundation Models", .serialized)
struct NoFoundationModelsTests {

    private func makeAsset(id: String, episodeId: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "rk7-fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///rk7/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// A 3-chunk transcript with a clear sponsor read in the middle. The
    /// "brought to you by …" disclosure phrase is the strongest lexical
    /// signal in `LexicalScanner` and is the driver for hot-path
    /// detection without any FM corroboration.
    private func makeKnownAdChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome back to the show. Today we are talking about modern web design.",
            "This episode is brought to you by Squarespace. Use code SHOW for twenty percent off your first purchase. Visit squarespace dot com slash show today.",
            "Now back to our interview. Our guest was telling us about the future of typography."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "rk7-fm-\(assetId)-\(idx)",
                analysisAssetId: assetId,
                segmentFingerprint: "rk7-fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "rk7-test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    // MARK: - Test 1: hot-path still detects without FM

    @Test("Lexical + rule-based classifier produces hot-path AdWindow without FM")
    func hotPathWithoutFMStillDetects() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "rk7-fm-hp", episodeId: "ep-rk7-fm-hp")
        try await store.insertAsset(asset)
        let chunks = makeKnownAdChunks(assetId: asset.id)
        try await store.insertTranscriptChunks(chunks)

        // canUseFoundationModelsProvider: { false } means the FM shadow
        // branch in `AdDetectionService` is short-circuited — no FM
        // calls fire. The hot-path itself never touches FM (FM is
        // backfill-only, layer 3), so this exercises the production
        // configuration on a non-FM device.
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig.default,
            canUseFoundationModelsProvider: { false }
        )

        let windows = try await service.runHotPath(
            chunks: chunks,
            analysisAssetId: asset.id,
            episodeDuration: 90
        )

        #expect(
            !windows.isEmpty,
            "Hot path must produce at least one AdWindow from the lexical+classifier path; FM is not required for this signal."
        )

        // The strongest evidence (sponsor disclosure phrase) is in chunk 1
        // (30..60). The acoustic refiner + classifier may expand or
        // contract the boundaries; we assert overlap rather than equality.
        let adChunkRange = 30.0...60.0
        let overlapping = windows.filter { window in
            window.startTime < adChunkRange.upperBound && window.endTime > adChunkRange.lowerBound
        }
        #expect(
            !overlapping.isEmpty,
            "At least one detected AdWindow must overlap the sponsor-disclosure chunk; got: \(windows.map { "\($0.startTime)…\($0.endTime)" })"
        )

        // The decisionState for hot-path output is `.candidate`. This
        // pins that contract — degraded conditions must not silently
        // change the lifecycle stage.
        for window in windows {
            #expect(
                window.decisionState == AdDecisionState.candidate.rawValue,
                "Hot-path AdWindows must enter as `.candidate`; saw \(window.decisionState)"
            )
        }
    }

    // MARK: - Test 2: metadata extractor degrades to fallback

    @MainActor
    @Test("FallbackExtractor produces below-threshold confidence so banner copy is generic")
    func fallbackExtractorYieldsGenericBannerCopy() async throws {
        // Drive the extractor directly with the same evidence text that
        // the hot-path test exercised. The fallback must produce
        // metadata whose confidence sits at or below
        // `AdBannerView.metadataConfidenceThreshold` so the banner
        // renders the generic "Skipped sponsor segment" copy line.
        let extractor = FallbackExtractor()
        let evidenceText = "This episode is brought to you by Squarespace. Use code SHOW for twenty percent off."

        let metadata = try await extractor.extract(
            evidenceText: evidenceText,
            windowStartTime: 30,
            windowEndTime: 60
        )

        let confidence = try #require(metadata?.confidence)
        #expect(
            confidence < AdBannerView.metadataConfidenceThreshold,
            "Fallback extractor confidence (\(confidence)) must be below the banner threshold (\(AdBannerView.metadataConfidenceThreshold)); otherwise the banner would surface unverified brand text."
        )

        // Drive the banner copy resolver with a banner item carrying
        // the fallback's metadata. `AdBannerView.bannerCopy` is the
        // production code path that ships in `AdBannerView`.
        let bannerItem = AdSkipBannerItem(
            id: "rk7-banner-1",
            windowId: "rk7-window-1",
            advertiser: metadata?.advertiser,
            product: metadata?.product,
            adStartTime: 30,
            adEndTime: 60,
            metadataConfidence: metadata?.confidence,
            metadataSource: metadata?.source ?? "none",
            podcastId: "podcast-rk7",
            evidenceCatalogEntries: [],
            tier: .autoSkipped
        )

        let copy = AdBannerView.bannerCopy(for: bannerItem)

        #expect(
            copy.advertiser == nil,
            "Generic banner copy must not surface an advertiser when metadata confidence is below threshold; saw \(copy.advertiser ?? "nil")"
        )
        #expect(
            copy.prefix == "Skipped sponsor segment",
            "Without high-confidence FM metadata, banner prefix must be the generic line; saw '\(copy.prefix)'"
        )
    }

    // MARK: - Test 3: CapabilitySnapshot persists & decodes correctly

    @Test("CapabilitySnapshot with FM unavailable round-trips through JSON")
    func capabilitySnapshotPersistsCorrectly() throws {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            foundationModelsUsable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 5_000_000_000,
            capturedAt: .now
        )

        #expect(
            snapshot.canUseFoundationModels == false,
            "FM-unavailable snapshot must report canUseFoundationModels=false."
        )
        #expect(
            snapshot.foundationModelsAvailable == false
        )

        // Verify the snapshot encodes and decodes to the same shape
        // the AnalysisStore serializes per analysis run.
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(snapshot)

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(CapabilitySnapshot.self, from: data)

        #expect(decoded.foundationModelsAvailable == false)
        #expect(decoded.foundationModelsUsable == false)
        #expect(decoded.appleIntelligenceEnabled == false)
        #expect(decoded.foundationModelsLocaleSupported == false)
        #expect(decoded.canUseFoundationModels == false)
        #expect(decoded.thermalState == .nominal)
        #expect(decoded.isLowPowerMode == false)
        #expect(decoded.isCharging == true)
    }

    // MARK: - Test 4: skip cue still fires from a hot-path AdWindow

    @Test("SkipOrchestrator dispatches skip cues from hot-path AdWindow without FM")
    func skipCueFiresWithoutFM() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            config: .default,
            trustService: trustService
        )

        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        // A confirmed AdWindow that came out of the lexical+classifier
        // path (no FM metadata enrichment).
        let ad = makeSkipTestAdWindow(
            id: "rk7-fm-skip-ad",
            startTime: 30, endTime: 60,
            confidence: 0.85,
            decisionState: "confirmed"
        )

        await orchestrator.receiveAdWindows([ad])
        await orchestrator.updatePlayheadTime(15)

        #expect(
            !pushedCues.isEmpty,
            "Skip cue must fire from a confirmed AdWindow regardless of FM availability; AdWindow lifecycle is unrelated to metadata enrichment."
        )
    }
}
