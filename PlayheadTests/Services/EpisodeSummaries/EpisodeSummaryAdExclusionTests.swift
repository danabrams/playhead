// EpisodeSummaryAdExclusionTests.swift
// playhead-g4dk: episode summaries were dominated by ads because the
// summarizer was fed the RAW transcript with no ad-exclusion (a car-buying
// sponsor read once crowded out a Tour de France episode). These tests pin
// the DETERMINISTIC exclusion layer — NOT FoundationModels output, which is
// non-deterministic. We assert on the chunk set / prompt handed to the
// transport, never on the model's generated summary.
//
// Coverage:
//   - pure chunk-drop helper (`EpisodeSummaryAdExclusion`)
//   - the coordinator's store-backed `hydrate` path, which drops chunks
//     overlapping confirmed `ad_windows` and decoded ad spans
//   - the prompt handed to the summarizer transport excludes the ad tokens

import Foundation
import Testing

@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

@Suite("EpisodeSummary ad exclusion (playhead-g4dk)")
struct EpisodeSummaryAdExclusionTests {

    // MARK: - Pure helper

    @Test("excludingAds returns chunks unchanged when there are no ad ranges")
    func noAdRangesReturnsUnchanged() {
        let chunks = [
            makeChunk(assetId: "a", index: 0, start: 0, end: 30, text: "one"),
            makeChunk(assetId: "a", index: 1, start: 30, end: 60, text: "two")
        ]
        let result = EpisodeSummaryAdExclusion.excludingAds(chunks: chunks, adRanges: [])
        #expect(result.map(\.id) == chunks.map(\.id))
    }

    @Test("excludingAds drops overlapping chunks and keeps disjoint ones")
    func dropsOverlapping() {
        let chunks = [
            makeChunk(assetId: "a", index: 0, start: 0, end: 30, text: "editorial-a"),
            makeChunk(assetId: "a", index: 1, start: 30, end: 60, text: "AD-TEXT"),
            makeChunk(assetId: "a", index: 2, start: 55, end: 80, text: "straddles-ad"),
            makeChunk(assetId: "a", index: 3, start: 80, end: 110, text: "editorial-b")
        ]
        // Ad range [30, 60) overlaps chunk 1 fully and chunk 2 partially.
        let result = EpisodeSummaryAdExclusion.excludingAds(
            chunks: chunks,
            adRanges: [EpisodeSummaryAdRange(start: 30, end: 60)]
        )
        #expect(result.map(\.id) == ["a-c0", "a-c3"])
        #expect(result.allSatisfy { !$0.text.contains("AD-TEXT") })
    }

    @Test("excludingAds treats ranges as half-open (a touching boundary is not overlap)")
    func halfOpenBoundary() {
        let chunks = [
            makeChunk(assetId: "a", index: 0, start: 0, end: 30, text: "before"),
            makeChunk(assetId: "a", index: 1, start: 60, end: 90, text: "after")
        ]
        // Ad range [30, 60): chunk 0 ends exactly at 30, chunk 1 starts
        // exactly at 60. Neither strictly overlaps, so both survive.
        let result = EpisodeSummaryAdExclusion.excludingAds(
            chunks: chunks,
            adRanges: [EpisodeSummaryAdRange(start: 30, end: 60)]
        )
        #expect(result.map(\.id) == ["a-c0", "a-c1"])
    }

    @Test("excludingAds ignores degenerate and non-finite ad ranges")
    func ignoresDegenerateRanges() {
        let chunks = [
            makeChunk(assetId: "a", index: 0, start: 0, end: 30, text: "keep")
        ]
        let ranges = [
            EpisodeSummaryAdRange(start: 30, end: 30),   // zero width
            EpisodeSummaryAdRange(start: 60, end: 10),   // inverted
            EpisodeSummaryAdRange(start: .nan, end: 100), // non-finite
            EpisodeSummaryAdRange(start: 0, end: .infinity) // non-finite
        ]
        let result = EpisodeSummaryAdExclusion.excludingAds(chunks: chunks, adRanges: ranges)
        #expect(result.map(\.id) == ["a-c0"])
    }

    // MARK: - Store-backed hydrate + transport

    @Test("hydrate drops confirmed-ad chunks so the summarizer prompt excludes the ad")
    func hydrateExcludesConfirmedAdText() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-g4dk-adcontam"
        try await store.insertAsset(makeAsset(id: assetId))

        let confirmedAd = "Carvana"
        let candidateAd = "TotallyUnconfirmedBrand"
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 30,
                      text: "Welcome to today's Tour de France coverage from the Alps."),
            makeChunk(assetId: assetId, index: 1, start: 30, end: 60,
                      text: "This episode is brought to you by \(confirmedAd). Buy your next car online, code RIDE10."),
            makeChunk(assetId: assetId, index: 2, start: 60, end: 90,
                      text: "Back to the stage: the peloton chased the breakaway into the final climb."),
            makeChunk(assetId: assetId, index: 3, start: 90, end: 120,
                      text: "A quick word from \(candidateAd), still under review.")
        ])

        // Confirmed ad over [30, 60) → excluded. Candidate ad over [90, 120)
        // → NOT excluded (only confirmed/applied windows shrink the text).
        try await store.insertAdWindows([
            makeAdWindow(id: "adw-confirmed", assetId: assetId, start: 30, end: 60,
                         decisionState: .confirmed, advertiser: confirmedAd),
            makeAdWindow(id: "adw-candidate", assetId: assetId, start: 90, end: 120,
                         decisionState: .candidate, advertiser: candidateAd)
        ])

        let provider = AnalysisStoreEpisodeSummaryBackfillCandidateProvider(store: store)
        let input = try #require(try await provider.hydrate(assetId: assetId))

        // Confirmed-ad chunk is gone; editorial chunks remain; the
        // unconfirmed candidate chunk is deliberately kept.
        #expect(input.chunks.allSatisfy { !$0.text.contains(confirmedAd) })
        #expect(input.chunks.contains { $0.text.contains("Tour de France") })
        #expect(input.chunks.contains { $0.text.contains("peloton") })
        #expect(input.chunks.contains { $0.text.contains(candidateAd) })

        // End-to-end: the prompt actually handed to the model transport
        // must not carry the confirmed-ad text.
        let transport = RecordingTransport()
        let extractor = EpisodeSummaryExtractor(
            transport: transport,
            capability: StubCapability(allowed: true)
        )
        _ = try await extractor.extract(
            analysisAssetId: input.analysisAssetId,
            episodeTitle: input.episodeTitle,
            showTitle: input.showTitle,
            transcriptVersion: input.transcriptVersion,
            chunks: input.chunks
        )
        let prompt = try #require(await transport.lastSchemaPrompt)
        #expect(!prompt.contains(confirmedAd))
        #expect(prompt.contains("Tour de France"))
        #expect(await transport.schemaCallCount == 1)
        #expect(await transport.permissiveCallCount == 0)
    }

    @Test("hydrate excludes chunks overlapping a decoded ad span")
    func hydrateExcludesDecodedSpans() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-g4dk-decodedspan"
        try await store.insertAsset(makeAsset(id: assetId))

        let adToken = "BetterHelp"
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 30,
                      text: "Editorial: the general classification tightened on the descent."),
            makeChunk(assetId: assetId, index: 1, start: 30, end: 60,
                      text: "Today's sponsor is \(adToken), online therapy on your schedule."),
            makeChunk(assetId: assetId, index: 2, start: 60, end: 90,
                      text: "Editorial: the sprint finish decided the stage in Paris.")
        ])
        // Decoded ad span over [30, 60) — a contiguous ad span by
        // construction, with no corresponding ad_window row.
        try await store.upsertDecodedSpans([
            DecodedSpan(
                id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 10, lastAtomOrdinal: 20),
                assetId: assetId,
                firstAtomOrdinal: 10,
                lastAtomOrdinal: 20,
                startTime: 30,
                endTime: 60,
                anchorProvenance: []
            )
        ])

        let provider = AnalysisStoreEpisodeSummaryBackfillCandidateProvider(store: store)
        let input = try #require(try await provider.hydrate(assetId: assetId))

        #expect(input.chunks.allSatisfy { !$0.text.contains(adToken) })
        #expect(input.chunks.contains { $0.text.contains("general classification") })
        #expect(input.chunks.contains { $0.text.contains("sprint finish") })
    }

    // MARK: - Fixtures

    private func makeChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        text: String
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-c\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(assetId)-\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: text,
            pass: "fast",
            modelVersion: "test",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: 120.0,
            confirmedAdCoverageEndTime: 60.0,
            analysisState: "completeFull",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 120.0
        )
    }

    private func makeAdWindow(
        id: String,
        assetId: String,
        start: Double,
        end: Double,
        decisionState: AdDecisionState,
        advertiser: String
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 0.99,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState.rawValue,
            detectorVersion: "test",
            advertiser: advertiser,
            product: "test product",
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "test",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }
}

// MARK: - Mocks

/// Records the exact prompt handed to the FM transport so the test can
/// assert on the model INPUT (deterministic) rather than its output.
private actor RecordingTransport: EpisodeSummaryTransport {
    private(set) var lastSchemaPrompt: String?
    private(set) var lastPermissivePrompt: String?
    private(set) var schemaCallCount = 0
    private(set) var permissiveCallCount = 0

    func generateSchemaBound(prompt: String) async throws -> (
        summary: String,
        mainTopics: [String],
        notableGuests: [String]
    ) {
        schemaCallCount += 1
        lastSchemaPrompt = prompt
        return (summary: "Editorial summary of the episode.", mainTopics: ["cycling"], notableGuests: [])
    }

    func generatePermissive(prompt: String) async throws -> String {
        permissiveCallCount += 1
        lastPermissivePrompt = prompt
        return "SUMMARY: fallback"
    }
}

private struct StubCapability: EpisodeSummaryCapabilityProvider {
    let allowed: Bool
    func canUseFoundationModels() async -> Bool { allowed }
}
