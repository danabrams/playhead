// EpisodeSummaryBackfillCoordinatorTests.swift
// playhead-jzik: tests for the periodic backfill coordinator. Mocks
// the extractor's transport seam and the candidate provider so the
// tests run on the simulator without the FM stack.

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeSummaryBackfillCoordinator (playhead-jzik)")
struct EpisodeSummaryBackfillCoordinatorTests {

    @Test("Pass yields userDisabled when toggle returns false")
    func userDisabledShortCircuits() async throws {
        let coordinator = EpisodeSummaryBackfillCoordinator(
            extractor: makeExtractor(transport: MockTransport()),
            candidates: MockCandidateProvider(),
            sink: MockSink(),
            userToggle: { false }
        )
        let outcome = await coordinator.pollOnce()
        #expect(outcome == .userDisabled)
    }

    @Test("Pass yields noCandidates when query returns empty")
    func noCandidatesYieldsNoCandidates() async throws {
        let coordinator = EpisodeSummaryBackfillCoordinator(
            extractor: makeExtractor(transport: MockTransport()),
            candidates: MockCandidateProvider(),
            sink: MockSink(),
            userToggle: { true }
        )
        let outcome = await coordinator.pollOnce()
        #expect(outcome == .noCandidates)
    }

    @Test("Pass extracts and persists for each hydrated candidate")
    func processesCandidatesAndPersists() async throws {
        let transport = MockTransport()
        await transport.setSchemaResult(
            .success((summary: "Generated.", mainTopics: ["topic"], notableGuests: []))
        )

        let candidates = MockCandidateProvider()
        await candidates.setCandidates(["asset-1", "asset-2"])
        await candidates.setHydration([
            "asset-1": EpisodeSummaryBackfillInput(
                analysisAssetId: "asset-1",
                episodeTitle: "Title 1",
                showTitle: nil,
                transcriptVersion: "v1",
                chunks: [makeChunk(id: "c-0")]
            ),
            "asset-2": EpisodeSummaryBackfillInput(
                analysisAssetId: "asset-2",
                episodeTitle: "Title 2",
                showTitle: nil,
                transcriptVersion: "v2",
                chunks: [makeChunk(id: "c-0")]
            )
        ])

        let sink = MockSink()
        let coordinator = EpisodeSummaryBackfillCoordinator(
            extractor: makeExtractor(transport: transport),
            candidates: candidates,
            sink: sink,
            userToggle: { true }
        )
        let outcome = await coordinator.pollOnce()
        switch outcome {
        case .processed(let succeeded, let refused):
            #expect(succeeded == 2)
            #expect(refused == 0)
        default:
            Issue.record("expected .processed, got \(outcome)")
        }
        #expect(await sink.persistedIds == ["asset-1", "asset-2"])
    }

    @Test("Capability unavailable mid-pass short-circuits")
    func capabilityUnavailableStopsPass() async throws {
        let candidates = MockCandidateProvider()
        await candidates.setCandidates(["asset-1", "asset-2"])
        await candidates.setHydration([
            "asset-1": EpisodeSummaryBackfillInput(
                analysisAssetId: "asset-1",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: "v1",
                chunks: [makeChunk(id: "c-0")]
            ),
            "asset-2": EpisodeSummaryBackfillInput(
                analysisAssetId: "asset-2",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: "v2",
                chunks: [makeChunk(id: "c-0")]
            )
        ])

        let sink = MockSink()
        let coordinator = EpisodeSummaryBackfillCoordinator(
            extractor: makeExtractor(
                transport: MockTransport(),
                capability: MockCapabilityProvider(allowed: false)
            ),
            candidates: candidates,
            sink: sink,
            userToggle: { true }
        )
        let outcome = await coordinator.pollOnce()
        #expect(outcome == .capabilityUnavailable)
        #expect(await sink.persistedIds.isEmpty)
    }

    @Test("Both-paths-refused counts as terminallyRefused but pass continues")
    func bothPathsRefusedCountsButContinues() async throws {
        let transport = MockTransport()
        // Schema empty -> permissive; permissive throws -> bothPathsRefused
        // for asset-1; asset-2 succeeds via the same mock (it's stateful
        // across calls).
        await transport.setSchemaResult(
            .success((summary: "ok!", mainTopics: [], notableGuests: []))
        )
        await transport.setPerCallSchemaResults([
            .success((summary: "", mainTopics: [], notableGuests: [])),
            .success((summary: "ok!", mainTopics: [], notableGuests: []))
        ])
        await transport.setPerCallPermissiveResults([
            .failure(MockTransportError.refused)
        ])

        let candidates = MockCandidateProvider()
        await candidates.setCandidates(["asset-1", "asset-2"])
        await candidates.setHydration([
            "asset-1": EpisodeSummaryBackfillInput(
                analysisAssetId: "asset-1",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: "v1",
                chunks: [makeChunk(id: "c-0")]
            ),
            "asset-2": EpisodeSummaryBackfillInput(
                analysisAssetId: "asset-2",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: "v2",
                chunks: [makeChunk(id: "c-0")]
            )
        ])

        let sink = MockSink()
        let coordinator = EpisodeSummaryBackfillCoordinator(
            extractor: makeExtractor(transport: transport),
            candidates: candidates,
            sink: sink,
            userToggle: { true }
        )
        let outcome = await coordinator.pollOnce()
        switch outcome {
        case .processed(let succeeded, let refused):
            #expect(succeeded == 1)
            #expect(refused == 1)
        default:
            Issue.record("expected .processed, got \(outcome)")
        }
        #expect(await sink.persistedIds == ["asset-2"])
    }

    @Test("Hydration returning nil is skipped")
    func hydrationNilSkipsAsset() async throws {
        let candidates = MockCandidateProvider()
        await candidates.setCandidates(["asset-missing", "asset-good"])
        await candidates.setHydration([
            "asset-good": EpisodeSummaryBackfillInput(
                analysisAssetId: "asset-good",
                episodeTitle: nil,
                showTitle: nil,
                transcriptVersion: "v1",
                chunks: [makeChunk(id: "c-0")]
            )
        ])

        let transport = MockTransport()
        await transport.setSchemaResult(
            .success((summary: "ok", mainTopics: [], notableGuests: []))
        )
        let sink = MockSink()
        let coordinator = EpisodeSummaryBackfillCoordinator(
            extractor: makeExtractor(transport: transport),
            candidates: candidates,
            sink: sink,
            userToggle: { true }
        )
        let outcome = await coordinator.pollOnce()
        switch outcome {
        case .processed(let succeeded, _):
            #expect(succeeded == 1)
        default:
            Issue.record("expected .processed")
        }
        #expect(await sink.persistedIds == ["asset-good"])
    }

    // MARK: - Fixtures

    private func makeExtractor(
        transport: any EpisodeSummaryTransport,
        capability: any EpisodeSummaryCapabilityProvider = MockCapabilityProvider(allowed: true)
    ) -> EpisodeSummaryExtractor {
        EpisodeSummaryExtractor(transport: transport, capability: capability)
    }

    private func makeChunk(id: String) -> TranscriptChunk {
        TranscriptChunk(
            id: id,
            analysisAssetId: "asset-1",
            segmentFingerprint: "fp-\(id)",
            chunkIndex: 0,
            startTime: 0,
            endTime: 30,
            text: "lorem ipsum",
            normalizedText: "lorem ipsum",
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
    private var schemaResult: Result<(summary: String, mainTopics: [String], notableGuests: [String]), Error> =
        .failure(MockTransportError.refused)
    private var permissiveResult: Result<String, Error> = .failure(MockTransportError.refused)
    private var perCallSchemaResults: [Result<(summary: String, mainTopics: [String], notableGuests: [String]), Error>] = []
    private var perCallPermissiveResults: [Result<String, Error>] = []

    func setSchemaResult(
        _ result: Result<(summary: String, mainTopics: [String], notableGuests: [String]), Error>
    ) {
        self.schemaResult = result
    }

    func setPermissiveResult(_ result: Result<String, Error>) {
        self.permissiveResult = result
    }

    func setPerCallSchemaResults(
        _ results: [Result<(summary: String, mainTopics: [String], notableGuests: [String]), Error>]
    ) {
        self.perCallSchemaResults = results
    }

    func setPerCallPermissiveResults(_ results: [Result<String, Error>]) {
        self.perCallPermissiveResults = results
    }

    func generateSchemaBound(prompt: String) async throws -> (
        summary: String,
        mainTopics: [String],
        notableGuests: [String]
    ) {
        if !perCallSchemaResults.isEmpty {
            let next = perCallSchemaResults.removeFirst()
            return try next.get()
        }
        return try schemaResult.get()
    }

    func generatePermissive(prompt: String) async throws -> String {
        if !perCallPermissiveResults.isEmpty {
            let next = perCallPermissiveResults.removeFirst()
            return try next.get()
        }
        return try permissiveResult.get()
    }
}

private actor MockCandidateProvider: EpisodeSummaryBackfillCandidateProvider {
    private var candidatesQueue: [String] = []
    private var hydration: [String: EpisodeSummaryBackfillInput] = [:]

    func setCandidates(_ ids: [String]) {
        self.candidatesQueue = ids
    }

    func setHydration(_ map: [String: EpisodeSummaryBackfillInput]) {
        self.hydration = map
    }

    func candidates(
        coverageFraction: Double,
        currentSchemaVersion: Int,
        limit: Int
    ) async throws -> [String] {
        Array(candidatesQueue.prefix(limit))
    }

    func hydrate(assetId: String) async throws -> EpisodeSummaryBackfillInput? {
        hydration[assetId]
    }
}

private actor MockSink: EpisodeSummaryBackfillSink {
    var persistedIds: [String] = []
    var persistedSummaries: [EpisodeSummary] = []

    func persist(_ summary: EpisodeSummary) async throws {
        persistedIds.append(summary.analysisAssetId)
        persistedSummaries.append(summary)
    }
}

private struct MockCapabilityProvider: EpisodeSummaryCapabilityProvider {
    let allowed: Bool
    func canUseFoundationModels() async -> Bool { allowed }
}
