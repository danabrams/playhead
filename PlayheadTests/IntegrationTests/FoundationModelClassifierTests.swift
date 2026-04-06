import Foundation
import Testing
@testable import Playhead

@Suite("Foundation Model Classifier — Pass A")
struct FoundationModelClassifierTests {

    @Test("coarse screening schema round-trips with certainty bands")
    func coarseScreeningSchemaRoundTrip() throws {
        let schema = CoarseScreeningSchema(
            transcriptQuality: .degraded,
            disposition: .uncertain,
            support: CoarseSupportSchema(
                supportLineRefs: [3, 7],
                certainty: .moderate
            )
        )

        let data = try JSONEncoder().encode(schema)
        let json = try #require(String(data: data, encoding: .utf8))
        let decoded = try JSONDecoder().decode(CoarseScreeningSchema.self, from: data)

        #expect(decoded == schema)
        #expect(json.contains("\"certainty\":\"moderate\""))
        #expect(!json.contains("0."))
    }

    @Test("prompt is minimal and uses quoted line refs")
    func promptFormat() {
        let prompt = FoundationModelClassifier.buildPrompt(for: [
            makeSegment(index: 7, text: "This is the sponsor read."),
            makeSegment(index: 8, text: "Use code SAVE for discounts.")
        ])

        #expect(prompt == """
        Classify ad content.
        7: "This is the sponsor read."
        8: "Use code SAVE for discounts."
        """)
        #expect(!prompt.localizedCaseInsensitiveContains("reason"))
        #expect(!prompt.localizedCaseInsensitiveContains("evidence"))
    }

    @Test("planner covers the full real-episode transcript and respects the token budget")
    func plannerCoverageOnRealEpisode() async throws {
        let segments = buildFixtureSegments()
        #expect(!segments.isEmpty)

        let recorder = RuntimeRecorder(
            contextSize: 30,
            schemaTokens: 4,
            tokenCountRule: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 5
            }
        )
        let classifier = FoundationModelClassifier(
            runtime: recorder.runtime,
            config: .init(safetyMarginTokens: 5, maximumResponseTokens: 6)
        )

        let plans = try await classifier.planPassA(segments: segments)
        let covered = Set(plans.flatMap(\.lineRefs))

        #expect(plans.count > 1)
        #expect(covered == Set(segments.map(\.segmentIndex)))
        #expect(plans.allSatisfy { !$0.lineRefs.isEmpty })
        #expect(plans.allSatisfy { $0.promptTokenCount <= 15 })
        #expect(plans.allSatisfy { isContiguous($0.lineRefs) })
    }

    @Test("coarse pass prewarms once, uses a fresh session per window, and sanitizes support refs")
    func prewarmAndFreshSessionPerWindow() async throws {
        let segments = [
            makeSegment(index: 0, startTime: 0, endTime: 8, text: "The hosts catch up before the break."),
            makeSegment(index: 1, startTime: 8, endTime: 16, text: "This episode is brought to you by ExampleCo."),
            makeSegment(index: 2, startTime: 16, endTime: 24, text: "Use code SAVE for twenty percent off."),
        ]

        let recorder = RuntimeRecorder(
            contextSize: 30,
            schemaTokens: 4,
            tokenCountRule: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 5
            },
            responses: [
                CoarseScreeningSchema(
                    transcriptQuality: .good,
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [999, 1, 1],
                        certainty: .strong
                    )
                ),
                CoarseScreeningSchema(
                    transcriptQuality: .good,
                    disposition: .noAds,
                    support: CoarseSupportSchema(
                        supportLineRefs: [42],
                        certainty: .weak
                    )
                ),
            ]
        )
        let classifier = FoundationModelClassifier(
            runtime: recorder.runtime,
            config: .init(safetyMarginTokens: 5, maximumResponseTokens: 6)
        )

        let output = try await classifier.coarsePassA(segments: segments)
        let snapshot = await recorder.snapshot()

        #expect(output.status == .success)
        #expect(output.windows.count == 2)
        #expect(output.windows.map(\.lineRefs) == [[0, 1], [2]])
        #expect(output.windows[0].screening.support?.supportLineRefs == [1])
        #expect(output.windows[1].screening.support == nil)
        #expect(snapshot.prewarmCalls == [
            RuntimeRecorder.PrewarmCall(sessionID: 1, promptPrefix: "Classify ad content.")
        ])
        #expect(snapshot.respondCalls.count == 2)
        #expect(Set(snapshot.respondCalls.map(\.sessionID)).count == 2)
        #expect(snapshot.sessionCount == 3)
    }

    @Test("capability gating returns the FM status without creating sessions")
    func availabilityGating() async throws {
        let recorder = RuntimeRecorder(
            availabilityStatus: .unsupportedLocale,
            contextSize: 30,
            schemaTokens: 4,
            tokenCountRule: { _ in 1 }
        )
        let classifier = FoundationModelClassifier(runtime: recorder.runtime)

        let output = try await classifier.coarsePassA(segments: [
            makeSegment(index: 0, text: "Bonjour tout le monde.")
        ])
        let snapshot = await recorder.snapshot()

        #expect(output.status == .unsupportedLocale)
        #expect(output.windows.isEmpty)
        #expect(snapshot.sessionCount == 0)
        #expect(snapshot.respondCalls.isEmpty)
        #expect(snapshot.prewarmCalls.isEmpty)
    }
}

private func makeSegment(
    index: Int,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String
) -> AdTranscriptSegment {
    AdTranscriptSegment(
        atoms: [
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "asset-1",
                    transcriptVersion: "transcript-v1",
                    atomOrdinal: index
                ),
                contentHash: "hash-\(index)",
                startTime: startTime,
                endTime: endTime,
                text: text,
                chunkIndex: index
            )
        ],
        segmentIndex: index
    )
}

private func buildFixtureSegments() -> [AdTranscriptSegment] {
    let chunks = ConanFanhausenRevisitedFixture.parseChunks()
    let (atoms, _) = TranscriptAtomizer.atomize(
        chunks: chunks,
        analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
        normalizationHash: "fm-classifier-tests",
        sourceHash: "fixture"
    )
    return TranscriptSegmenter.segment(atoms: atoms)
}

private func isContiguous(_ values: [Int]) -> Bool {
    zip(values, values.dropFirst()).allSatisfy { lhs, rhs in
        rhs == lhs + 1
    }
}

private actor RuntimeRecorder {
    struct PrewarmCall: Sendable, Equatable {
        let sessionID: Int
        let promptPrefix: String
    }

    struct RespondCall: Sendable, Equatable {
        let sessionID: Int
        let prompt: String
    }

    struct Snapshot: Sendable, Equatable {
        let sessionCount: Int
        let prewarmCalls: [PrewarmCall]
        let respondCalls: [RespondCall]
    }

    private let availabilityStatus: SemanticScanStatus?
    private let contextSize: Int
    private let schemaTokens: Int
    private let tokenCountRule: @Sendable (String) -> Int
    private var queuedResponses: [CoarseScreeningSchema]
    private var sessionCount = 0
    private var prewarmCalls: [PrewarmCall] = []
    private var respondCalls: [RespondCall] = []

    init(
        availabilityStatus: SemanticScanStatus? = nil,
        contextSize: Int,
        schemaTokens: Int,
        tokenCountRule: @escaping @Sendable (String) -> Int,
        responses: [CoarseScreeningSchema] = []
    ) {
        self.availabilityStatus = availabilityStatus
        self.contextSize = contextSize
        self.schemaTokens = schemaTokens
        self.tokenCountRule = tokenCountRule
        self.queuedResponses = responses
    }

    nonisolated var runtime: FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in await self.currentAvailabilityStatus() },
            contextSize: { await self.currentContextSize() },
            tokenCount: { prompt in await self.currentTokenCount(for: prompt) },
            schemaTokenCount: { await self.currentSchemaTokenCount() },
            makeSession: {
                let sessionID = await self.nextSessionID()
                return FoundationModelClassifier.Runtime.Session(
                    prewarm: { promptPrefix in
                        await self.recordPrewarm(sessionID: sessionID, promptPrefix: promptPrefix)
                    },
                    respond: { prompt in
                        try await self.recordResponse(sessionID: sessionID, prompt: prompt)
                    }
                )
            }
        )
    }

    func snapshot() -> Snapshot {
        Snapshot(
            sessionCount: sessionCount,
            prewarmCalls: prewarmCalls,
            respondCalls: respondCalls
        )
    }

    private func currentAvailabilityStatus() -> SemanticScanStatus? {
        availabilityStatus
    }

    private func currentContextSize() -> Int {
        contextSize
    }

    private func currentTokenCount(for prompt: String) -> Int {
        tokenCountRule(prompt)
    }

    private func currentSchemaTokenCount() -> Int {
        schemaTokens
    }

    private func nextSessionID() -> Int {
        sessionCount += 1
        return sessionCount
    }

    private func recordPrewarm(sessionID: Int, promptPrefix: String) {
        prewarmCalls.append(
            PrewarmCall(
                sessionID: sessionID,
                promptPrefix: promptPrefix
            )
        )
    }

    private func recordResponse(sessionID: Int, prompt: String) throws -> CoarseScreeningSchema {
        respondCalls.append(RespondCall(sessionID: sessionID, prompt: prompt))
        if queuedResponses.isEmpty {
            return CoarseScreeningSchema(
                transcriptQuality: .good,
                disposition: .noAds,
                support: nil
            )
        }
        return queuedResponses.removeFirst()
    }
}
