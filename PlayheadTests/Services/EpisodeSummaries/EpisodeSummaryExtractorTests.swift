// EpisodeSummaryExtractorTests.swift
// playhead-jzik: unit tests for the episode-summary extractor.
//
// The extractor's iOS-26-only `LanguageModelSession` calls are mocked
// through the `EpisodeSummaryTransport` seam, so these tests run on the
// simulator without booting FoundationModels. Tests cover:
//
//   - sampling helper: opening / middle / closing windows + chunk cap
//   - prompt grammar: deterministic permissive parser surface
//   - end-to-end actor: schema-bound success, permissive fallback,
//     bothPathsRefused terminal, capability-unavailable short-circuit,
//     sanitization (topic/guest cap)

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeSummaryExtractor (playhead-jzik)")
struct EpisodeSummaryExtractorTests {

    // MARK: - Sampling

    @Test("Sampler returns empty for empty input")
    func samplerEmptyInput() {
        let result = EpisodeSummarySampler.sample(chunks: [])
        #expect(result.isEmpty)
    }

    @Test("Sampler picks opening + closing windows for a short episode")
    func samplerShortEpisode() {
        // 10-minute episode, chunk every 30s.
        let chunks = (0..<20).map { i in
            makeChunk(
                id: "c-\(i)",
                index: i,
                start: Double(i) * 30,
                end: Double(i + 1) * 30
            )
        }
        let config = EpisodeSummarySamplingConfig(
            openingSeconds: 60,
            closingSeconds: 60,
            middleSeconds: 60,
            maximumChunks: 100
        )
        let sampled = EpisodeSummarySampler.sample(chunks: chunks, config: config)

        // Total duration (600) > opening + closing + middle (60+60+60=180),
        // so the middle window also fires.
        //   Opening: chunks at 0–30, 30–60 (2 chunks).
        //   Middle: window centered at 300 with ±30s span = 270–330,
        //           catching chunks 270–300 and 300–330 (2 chunks).
        //   Closing: chunks at 540–570, 570–600 (2 chunks).
        // After dedup, expect 6 chunks.
        #expect(sampled.count == 6)
        #expect(sampled.first?.startTime == 0)
        #expect(sampled.last?.endTime == 600)
    }

    @Test("Sampler caps chunk count via even-stride subsample")
    func samplerCapsChunkCount() {
        // 60-minute episode, chunk every 5s -> 720 chunks.
        let chunks = (0..<720).map { i in
            makeChunk(
                id: "c-\(i)",
                index: i,
                start: Double(i) * 5,
                end: Double(i + 1) * 5
            )
        }
        let config = EpisodeSummarySamplingConfig(
            openingSeconds: 600,    // first 10 mins -> 120 chunks
            closingSeconds: 600,    // last 10 mins -> 120 chunks
            middleSeconds: 600,     // middle 10 mins -> 120 chunks
            maximumChunks: 80
        )
        let sampled = EpisodeSummarySampler.sample(chunks: chunks, config: config)
        #expect(sampled.count <= 80)
    }

    @Test("Sampler de-duplicates overlapping windows")
    func samplerDedupesOverlap() {
        // Tiny episode so opening and closing overlap entirely.
        let chunks = (0..<3).map { i in
            makeChunk(
                id: "c-\(i)",
                index: i,
                start: Double(i) * 10,
                end: Double(i + 1) * 10
            )
        }
        let config = EpisodeSummarySamplingConfig(
            openingSeconds: 60,
            closingSeconds: 60,
            middleSeconds: 30,
            maximumChunks: 100
        )
        let sampled = EpisodeSummarySampler.sample(chunks: chunks, config: config)
        // 3 unique chunks, not 6.
        #expect(sampled.count == 3)
        #expect(Set(sampled.map(\.id)).count == 3)
    }

    // MARK: - Prompt grammar

    @Test("Permissive parser splits SUMMARY / TOPICS / GUESTS sections")
    func permissiveParserSplits() {
        let raw = """
        SUMMARY:
        First sentence. Second sentence about the topic.

        TOPICS:
        - leadership
        - burnout
        - remote work

        GUESTS:
        - Jane Doe
        - John Smith
        """
        let parsed = EpisodeSummaryGrammar.parsePermissive(raw)
        #expect(parsed.summary.contains("First sentence"))
        #expect(parsed.summary.contains("Second sentence"))
        #expect(parsed.topics == ["leadership", "burnout", "remote work"])
        #expect(parsed.guests == ["Jane Doe", "John Smith"])
    }

    @Test("Permissive parser tolerates missing GUESTS section")
    func permissiveParserMissingGuests() {
        let raw = """
        SUMMARY: A solo monologue about productivity.
        TOPICS:
        - productivity
        - focus
        """
        let parsed = EpisodeSummaryGrammar.parsePermissive(raw)
        #expect(parsed.summary.contains("solo monologue"))
        #expect(parsed.topics == ["productivity", "focus"])
        #expect(parsed.guests.isEmpty)
    }

    @Test("Permissive parser strips numeric and bullet prefixes")
    func permissiveParserStripsPrefixes() {
        let raw = """
        SUMMARY: Test
        TOPICS:
        1. one
        2) two
        - three
        • four
        """
        let parsed = EpisodeSummaryGrammar.parsePermissive(raw)
        #expect(parsed.topics == ["one", "two", "three", "four"])
    }

    @Test("Permissive parser tolerates leading prose without SUMMARY heading")
    func permissiveParserLeadingProse() {
        let raw = """
        This is a leading prose summary that skipped the heading.
        TOPICS:
        - alpha
        """
        let parsed = EpisodeSummaryGrammar.parsePermissive(raw)
        #expect(parsed.summary.contains("leading prose"))
        #expect(parsed.topics == ["alpha"])
    }

    @Test("buildPrompt embeds episode + show titles when present")
    func buildPromptIncludesTitles() {
        let prompt = EpisodeSummaryGrammar.buildPrompt(
            episodeTitle: "How to escape burnout",
            showTitle: "Diary of a CEO",
            chunks: [makeChunk(id: "c-0", index: 0, start: 0, end: 30)]
        )
        #expect(prompt.contains("How to escape burnout"))
        #expect(prompt.contains("Diary of a CEO"))
    }

    @Test("buildPrompt omits title block when both nil")
    func buildPromptNoTitles() {
        let prompt = EpisodeSummaryGrammar.buildPrompt(
            episodeTitle: nil,
            showTitle: nil,
            chunks: [makeChunk(id: "c-0", index: 0, start: 0, end: 30)]
        )
        #expect(!prompt.contains("Episode title"))
        #expect(!prompt.contains("From the show"))
    }

    // MARK: - Sanitize

    @Test("EpisodeSummary.sanitize trims whitespace and caps lists")
    func sanitizeTrimsAndCaps() {
        let topics = (0..<20).map { "  topic-\($0)  " }
        let guests = ["  ", "Real Guest", "", "Another Guest"]
        let cleaned = EpisodeSummary.sanitize(topics: topics, guests: guests)
        #expect(cleaned.topics.count == 8)
        #expect(cleaned.topics.allSatisfy { !$0.contains(" ") || !$0.hasPrefix(" ") })
        #expect(cleaned.guests == ["Real Guest", "Another Guest"])
    }

    // MARK: - Extractor end-to-end

    @Test("Extractor refuses early when capability unavailable")
    func extractorRefusesWhenCapabilityUnavailable() async throws {
        let transport = MockTransport()
        let extractor = EpisodeSummaryExtractor(
            transport: transport,
            capability: MockCapabilityProvider(allowed: false)
        )
        do {
            _ = try await extractor.extract(
                analysisAssetId: "asset-1",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: nil,
                chunks: [makeChunk(id: "c", index: 0, start: 0, end: 60)]
            )
            Issue.record("expected capabilityUnavailable")
        } catch let error as EpisodeSummaryExtractionError {
            #expect(error == .capabilityUnavailable)
        }
        #expect(await transport.schemaCallCount == 0)
        #expect(await transport.permissiveCallCount == 0)
    }

    @Test("Extractor refuses early when no chunks survive sampling")
    func extractorRefusesOnEmptyChunks() async throws {
        let transport = MockTransport()
        let extractor = EpisodeSummaryExtractor(
            transport: transport,
            capability: MockCapabilityProvider(allowed: true)
        )
        do {
            _ = try await extractor.extract(
                analysisAssetId: "asset-1",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: nil,
                chunks: []
            )
            Issue.record("expected insufficientCoverage")
        } catch let error as EpisodeSummaryExtractionError {
            #expect(error == .insufficientCoverage)
        }
    }

    @Test("Extractor returns sanitized summary on schema-bound success")
    func extractorSchemaBoundSuccess() async throws {
        let transport = MockTransport()
        await transport.setSchemaResult(
            .success((
                summary: "This podcast covers leadership challenges.",
                mainTopics: ["leadership", "burnout", "  ", "remote work"],
                notableGuests: ["Jane Doe"]
            ))
        )
        let extractor = EpisodeSummaryExtractor(
            transport: transport,
            capability: MockCapabilityProvider(allowed: true)
        )
        let now = Date(timeIntervalSince1970: 1_714_000_000)
        let result = try await extractor.extract(
            analysisAssetId: "asset-1",
            episodeTitle: "Burnout edition",
            showTitle: "Show",
            transcriptVersion: "v1",
            chunks: [
                makeChunk(id: "c-0", index: 0, start: 0, end: 30, text: "intro talk"),
                makeChunk(id: "c-1", index: 1, start: 30, end: 60, text: "more intro")
            ],
            now: now
        )
        #expect(result.analysisAssetId == "asset-1")
        #expect(result.summary == "This podcast covers leadership challenges.")
        #expect(result.mainTopics == ["leadership", "burnout", "remote work"])
        #expect(result.notableGuests == ["Jane Doe"])
        #expect(result.transcriptVersion == "v1")
        #expect(result.createdAt == now)
        #expect(result.schemaVersion == EpisodeSummary.currentSchemaVersion)
        #expect(await transport.schemaCallCount == 1)
        #expect(await transport.permissiveCallCount == 0)
    }

    @Test("Extractor falls back to permissive when schema path returns empty")
    func extractorPermissiveFallback() async throws {
        let transport = MockTransport()
        await transport.setSchemaResult(
            .success((summary: "", mainTopics: [], notableGuests: []))
        )
        await transport.setPermissiveResult(
            .success("""
            SUMMARY: Permissive recovered the summary.
            TOPICS:
            - alpha
            - beta
            GUESTS:
            - Guest A
            """)
        )

        let extractor = EpisodeSummaryExtractor(
            transport: transport,
            capability: MockCapabilityProvider(allowed: true)
        )
        let result = try await extractor.extract(
            analysisAssetId: "asset-1",
            episodeTitle: nil,
            showTitle: nil,
            transcriptVersion: "v2",
            chunks: [makeChunk(id: "c-0", index: 0, start: 0, end: 60)]
        )
        #expect(result.summary.contains("Permissive recovered"))
        #expect(result.mainTopics == ["alpha", "beta"])
        #expect(result.notableGuests == ["Guest A"])
        #expect(await transport.schemaCallCount == 1)
        #expect(await transport.permissiveCallCount == 1)
    }

    @Test("Extractor surfaces bothPathsRefused when permissive also fails")
    func extractorBothPathsRefused() async throws {
        let transport = MockTransport()
        await transport.setSchemaResult(
            .success((summary: "", mainTopics: [], notableGuests: []))
        )
        await transport.setPermissiveResult(
            .failure(MockTransportError.refused)
        )
        let extractor = EpisodeSummaryExtractor(
            transport: transport,
            capability: MockCapabilityProvider(allowed: true)
        )
        do {
            _ = try await extractor.extract(
                analysisAssetId: "asset-1",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: nil,
                chunks: [makeChunk(id: "c-0", index: 0, start: 0, end: 60)]
            )
            Issue.record("expected bothPathsRefused")
        } catch let error as EpisodeSummaryExtractionError {
            #expect(error == .bothPathsRefused)
        }
    }

    @Test("Extractor surfaces unparseableResponse when permissive returns blank text")
    func extractorUnparseableResponse() async throws {
        let transport = MockTransport()
        await transport.setSchemaResult(
            .success((summary: "", mainTopics: [], notableGuests: []))
        )
        await transport.setPermissiveResult(.success("   \n\n   "))
        let extractor = EpisodeSummaryExtractor(
            transport: transport,
            capability: MockCapabilityProvider(allowed: true)
        )
        do {
            _ = try await extractor.extract(
                analysisAssetId: "asset-1",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: nil,
                chunks: [makeChunk(id: "c-0", index: 0, start: 0, end: 60)]
            )
            Issue.record("expected unparseableResponse")
        } catch let error as EpisodeSummaryExtractionError {
            #expect(error == .unparseableResponse)
        }
    }

    // MARK: - Fixtures

    private func makeChunk(
        id: String,
        index: Int,
        start: Double,
        end: Double,
        text: String = "lorem ipsum"
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: id,
            analysisAssetId: "asset-1",
            segmentFingerprint: "fp-\(id)",
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
}

// MARK: - Mocks

private enum MockTransportError: Error, Equatable {
    case refused
}

private actor MockTransport: EpisodeSummaryTransport {
    var schemaResult: Result<(summary: String, mainTopics: [String], notableGuests: [String]), Error> =
        .failure(MockTransportError.refused)
    var permissiveResult: Result<String, Error> = .failure(MockTransportError.refused)
    var schemaCallCount: Int = 0
    var permissiveCallCount: Int = 0

    func setSchemaResult(
        _ result: Result<(summary: String, mainTopics: [String], notableGuests: [String]), Error>
    ) {
        self.schemaResult = result
    }

    func setPermissiveResult(_ result: Result<String, Error>) {
        self.permissiveResult = result
    }

    func generateSchemaBound(prompt: String) async throws -> (
        summary: String,
        mainTopics: [String],
        notableGuests: [String]
    ) {
        schemaCallCount += 1
        return try schemaResult.get()
    }

    func generatePermissive(prompt: String) async throws -> String {
        permissiveCallCount += 1
        return try permissiveResult.get()
    }
}

private struct MockCapabilityProvider: EpisodeSummaryCapabilityProvider {
    let allowed: Bool
    func canUseFoundationModels() async -> Bool { allowed }
}
