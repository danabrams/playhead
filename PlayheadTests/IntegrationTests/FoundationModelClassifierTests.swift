import Foundation
import Testing
@testable import Playhead

@Suite("Foundation Model Classifier — Pass A/B")
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

    @Test("refinement schema round-trips with structured fields")
    func refinementSchemaRoundTrip() throws {
        let schema = RefinementWindowSchema(
            spans: [
                SpanRefinementSchema(
                    commercialIntent: .paid,
                    ownership: .thirdParty,
                    firstLineRef: 11,
                    lastLineRef: 14,
                    certainty: .strong,
                    boundaryPrecision: .precise,
                    evidenceAnchors: [
                        EvidenceAnchorSchema(
                            evidenceRef: 7,
                            lineRef: 12,
                            kind: .url,
                            certainty: .strong
                        )
                    ],
                    alternativeExplanation: .none,
                    reasonTags: [.callToAction, .urlMention]
                )
            ]
        )

        let data = try JSONEncoder().encode(schema)
        let json = try #require(String(data: data, encoding: .utf8))
        let decoded = try JSONDecoder().decode(RefinementWindowSchema.self, from: data)

        #expect(decoded == schema)
        #expect(json.contains("\"commercialIntent\":\"paid\""))
        #expect(json.contains("\"boundaryPrecision\":\"precise\""))
        #expect(!json.localizedCaseInsensitiveContains("reasoning"))
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
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 8,
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
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 8,
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
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 8,
            tokenCountRule: { _ in 1 }
        )
        let classifier = FoundationModelClassifier(
            runtime: recorder.runtime,
            config: .init(
                safetyMarginTokens: 4,
                coarseMaximumResponseTokens: 6,
                refinementMaximumResponseTokens: 12,
                maximumRefinementSpansPerWindow: 3
            )
        )

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

    @Test("adaptive zoom narrows positive windows and injects deterministic evidence refs")
    func adaptiveZoomNarrowsAroundSupportLines() async throws {
        let segments = [
            makeSegment(index: 0, text: "Hosts banter before the ad."),
            makeSegment(index: 1, text: "Visit example.com for the offer."),
            makeSegment(index: 2, text: "Use promo code SAVE."),
            makeSegment(index: 3, text: "Short bridge back to the show."),
            makeSegment(index: 4, text: "Check out our sister show off camera."),
            makeSegment(index: 5, text: "Listen wherever you get your podcasts.")
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: [
                EvidenceEntry(
                    evidenceRef: 11,
                    category: .url,
                    matchedText: "example.com",
                    normalizedText: "example.com",
                    atomOrdinal: 1,
                    startTime: 5,
                    endTime: 10
                ),
                EvidenceEntry(
                    evidenceRef: 12,
                    category: .ctaPhrase,
                    matchedText: "Listen wherever you get your podcasts",
                    normalizedText: "listen wherever you get your podcasts",
                    atomOrdinal: 5,
                    startTime: 25,
                    endTime: 30
                )
            ]
        )

        let recorder = RuntimeRecorder(
            contextSize: 64,
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 8,
            tokenCountRule: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 4
            }
        )
        let classifier = FoundationModelClassifier(
            runtime: recorder.runtime,
            config: .init(
                safetyMarginTokens: 4,
                coarseMaximumResponseTokens: 6,
                refinementMaximumResponseTokens: 10,
                zoomAmbiguityBudget: 0,
                minimumZoomSpanLines: 2,
                maximumRefinementSpansPerWindow: 2
            )
        )

        let coarse = FMCoarseScanOutput(
            status: .success,
            windows: [
                FMCoarseWindowOutput(
                    windowIndex: 0,
                    lineRefs: [0, 1, 2, 3, 4, 5],
                    startTime: 0,
                    endTime: 30,
                    screening: CoarseScreeningSchema(
                        transcriptQuality: .good,
                        disposition: .containsAd,
                        support: CoarseSupportSchema(
                            supportLineRefs: [1, 5],
                            certainty: .moderate
                        )
                    ),
                    latencyMillis: 10
                ),
                FMCoarseWindowOutput(
                    windowIndex: 1,
                    lineRefs: [6, 7],
                    startTime: 30,
                    endTime: 40,
                    screening: CoarseScreeningSchema(
                        transcriptQuality: .good,
                        disposition: .noAds,
                        support: nil
                    ),
                    latencyMillis: 10
                )
            ],
            latencyMillis: 25
        )

        let zoomPlans = try await classifier.planAdaptiveZoom(
            coarse: coarse,
            segments: segments,
            evidenceCatalog: evidenceCatalog
        )

        #expect(zoomPlans.count == 1)
        #expect(zoomPlans[0].sourceWindowIndex == 0)
        #expect(zoomPlans[0].lineRefs == [1, 2, 4, 5])
        #expect(zoomPlans[0].stopReason == .ambiguityBudget)
        #expect(zoomPlans[0].prompt.contains("[E11] \"example.com\" (url, line 1)"))
        #expect(zoomPlans[0].prompt.contains("[E12] \"Listen wherever you get your podcasts\" (ctaPhrase, line 5)"))
    }

    @Test("refinement runs only on zoomed windows and resolves anchors to deterministic catalog entries")
    func refinementResolvesEvidenceAnchors() async throws {
        let segments = [
            makeSegment(index: 1, startTime: 5, endTime: 10, text: "Visit example.com for the offer."),
            makeSegment(index: 2, startTime: 10, endTime: 15, text: "Use promo code SAVE."),
            makeSegment(index: 4, startTime: 20, endTime: 25, text: "Check out our sister show off camera."),
            makeSegment(index: 5, startTime: 25, endTime: 30, text: "Listen wherever you get your podcasts.")
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: [
                EvidenceEntry(
                    evidenceRef: 11,
                    category: .url,
                    matchedText: "example.com",
                    normalizedText: "example.com",
                    atomOrdinal: 1,
                    startTime: 5,
                    endTime: 10
                ),
                EvidenceEntry(
                    evidenceRef: 12,
                    category: .ctaPhrase,
                    matchedText: "Listen wherever you get your podcasts",
                    normalizedText: "listen wherever you get your podcasts",
                    atomOrdinal: 5,
                    startTime: 25,
                    endTime: 30
                )
            ]
        )
        let zoomPlan = RefinementWindowPlan(
            windowIndex: 0,
            sourceWindowIndex: 7,
            lineRefs: [1, 2, 4, 5],
            focusLineRefs: [1, 5],
            focusClusters: [[1], [5]],
            prompt: "Refine ad spans.",
            promptTokenCount: 12,
            startTime: 5,
            endTime: 30,
            stopReason: .tokenBudget,
            promptEvidence: [
                PromptEvidenceEntry(entry: evidenceCatalog.entries[0], lineRef: 1),
                PromptEvidenceEntry(entry: evidenceCatalog.entries[1], lineRef: 5)
            ]
        )

        let recorder = RuntimeRecorder(
            contextSize: 64,
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 8,
            tokenCountRule: { _ in 1 },
            refinementResponses: [
                RefinementWindowSchema(
                    spans: [
                        SpanRefinementSchema(
                            commercialIntent: .paid,
                            ownership: .thirdParty,
                            firstLineRef: 1,
                            lastLineRef: 2,
                            certainty: .strong,
                            boundaryPrecision: .precise,
                            evidenceAnchors: [
                                EvidenceAnchorSchema(
                                    evidenceRef: 11,
                                    lineRef: 999,
                                    kind: .promoCode,
                                    certainty: .moderate
                                )
                            ],
                            alternativeExplanation: .none,
                            reasonTags: [.urlMention, .callToAction]
                        ),
                        SpanRefinementSchema(
                            commercialIntent: .owned,
                            ownership: .show,
                            firstLineRef: 4,
                            lastLineRef: 5,
                            certainty: .moderate,
                            boundaryPrecision: .usable,
                            evidenceAnchors: [
                                EvidenceAnchorSchema(
                                    evidenceRef: 12,
                                    lineRef: 4,
                                    kind: .brandSpan,
                                    certainty: .weak
                                )
                            ],
                            alternativeExplanation: .editorialContext,
                            reasonTags: [.crossPromoLanguage]
                        ),
                        SpanRefinementSchema(
                            commercialIntent: .paid,
                            ownership: .thirdParty,
                            firstLineRef: 1,
                            lastLineRef: 5,
                            certainty: .weak,
                            boundaryPrecision: .rough,
                            evidenceAnchors: [],
                            alternativeExplanation: .unknown,
                            reasonTags: [.hostReadPitch]
                        )
                    ]
                )
            ]
        )
        let classifier = FoundationModelClassifier(runtime: recorder.runtime)

        let output = try await classifier.refinePassB(
            zoomPlans: [zoomPlan],
            segments: segments,
            evidenceCatalog: evidenceCatalog
        )
        let snapshot = await recorder.snapshot()

        #expect(output.status == .success)
        #expect(output.windows.count == 1)
        #expect(output.windows[0].spans.count == 2)
        #expect(output.windows[0].spans[0].firstAtomOrdinal == 1)
        #expect(output.windows[0].spans[0].lastAtomOrdinal == 2)
        #expect(output.windows[0].spans[0].resolvedEvidenceAnchors[0].entry?.evidenceRef == 11)
        #expect(output.windows[0].spans[0].resolvedEvidenceAnchors[0].kind == .url)
        #expect(output.windows[0].spans[0].resolvedEvidenceAnchors[0].lineRef == 1)
        #expect(output.windows[0].spans[1].resolvedEvidenceAnchors[0].entry?.evidenceRef == 12)
        #expect(output.windows[0].spans[1].resolvedEvidenceAnchors[0].kind == .ctaPhrase)
        #expect(output.windows[0].spans[0].memoryWriteEligible)
        #expect(output.windows[0].spans[1].memoryWriteEligible)
        #expect(snapshot.prewarmCalls == [
            RuntimeRecorder.PrewarmCall(sessionID: 1, promptPrefix: "Refine ad spans.")
        ])
        #expect(snapshot.respondRefinementCalls.count == 1)
        #expect(snapshot.respondCalls.isEmpty)
    }

    @Test("refinement ignores fallback anchors that point outside the zoomed window")
    func refinementRejectsOffWindowFallbackAnchors() async throws {
        let segments = [
            makeSegment(index: 1, startTime: 5, endTime: 10, text: "Visit example.com for the offer."),
            makeSegment(index: 2, startTime: 10, endTime: 15, text: "Use promo code SAVE."),
            makeSegment(index: 9, startTime: 45, endTime: 50, text: "Visit outside-example.com for a different offer.")
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: [
                EvidenceEntry(
                    evidenceRef: 11,
                    category: .url,
                    matchedText: "example.com",
                    normalizedText: "example.com",
                    atomOrdinal: 1,
                    startTime: 5,
                    endTime: 10
                ),
                EvidenceEntry(
                    evidenceRef: 99,
                    category: .url,
                    matchedText: "outside-example.com",
                    normalizedText: "outside-example.com",
                    atomOrdinal: 9,
                    startTime: 45,
                    endTime: 50
                )
            ]
        )
        let zoomPlan = RefinementWindowPlan(
            windowIndex: 0,
            sourceWindowIndex: 4,
            lineRefs: [1, 2],
            focusLineRefs: [1, 2],
            focusClusters: [[1, 2]],
            prompt: "Refine ad spans.",
            promptTokenCount: 8,
            startTime: 5,
            endTime: 15,
            stopReason: .minimumSpan,
            promptEvidence: [
                PromptEvidenceEntry(entry: evidenceCatalog.entries[0], lineRef: 1)
            ]
        )

        let recorder = RuntimeRecorder(
            contextSize: 64,
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 8,
            tokenCountRule: { _ in 1 },
            refinementResponses: [
                RefinementWindowSchema(
                    spans: [
                        SpanRefinementSchema(
                            commercialIntent: .paid,
                            ownership: .thirdParty,
                            firstLineRef: 1,
                            lastLineRef: 2,
                            certainty: .strong,
                            boundaryPrecision: .usable,
                            evidenceAnchors: [
                                EvidenceAnchorSchema(
                                    evidenceRef: nil,
                                    lineRef: 9,
                                    kind: .url,
                                    certainty: .moderate
                                )
                            ],
                            alternativeExplanation: .none,
                            reasonTags: [.urlMention]
                        )
                    ]
                )
            ]
        )
        let classifier = FoundationModelClassifier(runtime: recorder.runtime)

        let output = try await classifier.refinePassB(
            zoomPlans: [zoomPlan],
            segments: segments,
            evidenceCatalog: evidenceCatalog
        )

        #expect(output.status == .success)
        #expect(output.windows.count == 1)
        #expect(output.windows[0].spans.count == 1)
        #expect(output.windows[0].spans[0].resolvedEvidenceAnchors.isEmpty)
        #expect(!output.windows[0].spans[0].memoryWriteEligible)
    }

    @Test("refinement keeps unresolved in-window anchors for scoring but blocks memory writes")
    func refinementKeepsUnresolvedWindowAnchors() async throws {
        let segments = [
            makeSegment(index: 1, startTime: 5, endTime: 10, text: "Our sponsor is terrific today."),
            makeSegment(index: 2, startTime: 10, endTime: 15, text: "We love great products.")
        ]
        let zoomPlan = RefinementWindowPlan(
            windowIndex: 0,
            sourceWindowIndex: 4,
            lineRefs: [1, 2],
            focusLineRefs: [1, 2],
            focusClusters: [[1, 2]],
            prompt: "Refine ad spans.",
            promptTokenCount: 8,
            startTime: 5,
            endTime: 15,
            stopReason: .minimumSpan,
            promptEvidence: []
        )
        let recorder = RuntimeRecorder(
            contextSize: 64,
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 8,
            tokenCountRule: { _ in 1 },
            refinementResponses: [
                RefinementWindowSchema(
                    spans: [
                        SpanRefinementSchema(
                            commercialIntent: .paid,
                            ownership: .thirdParty,
                            firstLineRef: 1,
                            lastLineRef: 2,
                            certainty: .moderate,
                            boundaryPrecision: .usable,
                            evidenceAnchors: [
                                EvidenceAnchorSchema(
                                    evidenceRef: nil,
                                    lineRef: 1,
                                    kind: .brandSpan,
                                    certainty: .weak
                                )
                            ],
                            alternativeExplanation: .guestPromotion,
                            reasonTags: [.hostReadPitch]
                        )
                    ]
                )
            ]
        )
        let classifier = FoundationModelClassifier(runtime: recorder.runtime)

        let output = try await classifier.refinePassB(
            zoomPlans: [zoomPlan],
            segments: segments,
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: "asset-1",
                transcriptVersion: "transcript-v1",
                entries: []
            )
        )

        #expect(output.status == .success)
        #expect(output.windows.count == 1)
        #expect(output.windows[0].spans.count == 1)
        #expect(output.windows[0].spans[0].resolvedEvidenceAnchors.count == 1)
        #expect(output.windows[0].spans[0].resolvedEvidenceAnchors[0].entry == nil)
        #expect(output.windows[0].spans[0].resolvedEvidenceAnchors[0].resolutionSource == .unresolved)
        #expect(!output.windows[0].spans[0].resolvedEvidenceAnchors[0].memoryWriteEligible)
        #expect(!output.windows[0].spans[0].memoryWriteEligible)
    }

    @Test("adaptive zoom keeps the Kelly Ripa repeat region when nearby coarse support spans the repeat cluster")
    func kellyRipaRepeatRegression() async throws {
        let segments = buildFixtureSegments()
        let evidenceCatalog = buildFixtureEvidenceCatalog()

        let siriusSegment = try #require(segments.first { $0.text.localizedCaseInsensitiveContains("Siriusxm.com slash Conan") })
        let kellyRepeatSegment = try #require(segments.first { $0.text.localizedCaseInsensitiveContains("Kelly Ripa") })
        let repeatFollowupSegment = try #require(segments.first { $0.text.localizedCaseInsensitiveContains("Let's talk off camera") && $0.startTime >= 970 })

        let candidateLineRefs = segments
            .filter { $0.startTime >= 952 && $0.endTime <= 989 }
            .map(\.segmentIndex)

        let coarse = FMCoarseScanOutput(
            status: .success,
            windows: [
                FMCoarseWindowOutput(
                    windowIndex: 0,
                    lineRefs: candidateLineRefs,
                    startTime: 952,
                    endTime: 989,
                    screening: CoarseScreeningSchema(
                        transcriptQuality: .good,
                        disposition: .uncertain,
                        support: CoarseSupportSchema(
                            supportLineRefs: [
                                siriusSegment.segmentIndex,
                                kellyRepeatSegment.segmentIndex,
                                repeatFollowupSegment.segmentIndex
                            ],
                            certainty: .weak
                        )
                    ),
                    latencyMillis: 1
                )
            ],
            latencyMillis: 1
        )
        let recorder = RuntimeRecorder(
            contextSize: 96,
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 10,
            tokenCountRule: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 3
            }
        )
        let classifier = FoundationModelClassifier(
            runtime: recorder.runtime,
            config: .init(
                safetyMarginTokens: 4,
                coarseMaximumResponseTokens: 6,
                refinementMaximumResponseTokens: 12,
                zoomAmbiguityBudget: 1,
                minimumZoomSpanLines: 2,
                maximumRefinementSpansPerWindow: 2
            )
        )

        let zoomPlans = try await classifier.planAdaptiveZoom(
            coarse: coarse,
            segments: segments,
            evidenceCatalog: evidenceCatalog
        )

        let retainedTexts = zoomPlans
            .flatMap(\.lineRefs)
            .compactMap { lineRef in segments.first { $0.segmentIndex == lineRef }?.text }

        #expect(!zoomPlans.isEmpty)
        #expect(retainedTexts.contains { $0.localizedCaseInsensitiveContains("Kelly Ripa") })
        #expect(retainedTexts.contains { $0.localizedCaseInsensitiveContains("Let's talk off camera") })
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

private func buildFixtureEvidenceCatalog() -> EvidenceCatalog {
    let chunks = ConanFanhausenRevisitedFixture.parseChunks()
    let (atoms, version) = TranscriptAtomizer.atomize(
        chunks: chunks,
        analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
        normalizationHash: "fm-classifier-tests",
        sourceHash: "fixture"
    )
    return EvidenceCatalogBuilder.build(
        atoms: atoms,
        analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
        transcriptVersion: version.transcriptVersion
    )
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

    struct RespondRefinementCall: Sendable, Equatable {
        let sessionID: Int
        let prompt: String
    }

    struct Snapshot: Sendable, Equatable {
        let sessionCount: Int
        let prewarmCalls: [PrewarmCall]
        let respondCalls: [RespondCall]
        let respondRefinementCalls: [RespondRefinementCall]
    }

    private let availabilityStatus: SemanticScanStatus?
    private let contextSize: Int
    private let coarseSchemaTokens: Int
    private let refinementSchemaTokens: Int
    private let tokenCountRule: @Sendable (String) -> Int
    private var queuedResponses: [CoarseScreeningSchema]
    private var queuedRefinementResponses: [RefinementWindowSchema]
    private var sessionCount = 0
    private var prewarmCalls: [PrewarmCall] = []
    private var respondCalls: [RespondCall] = []
    private var respondRefinementCalls: [RespondRefinementCall] = []

    init(
        availabilityStatus: SemanticScanStatus? = nil,
        contextSize: Int,
        coarseSchemaTokens: Int,
        refinementSchemaTokens: Int,
        tokenCountRule: @escaping @Sendable (String) -> Int,
        responses: [CoarseScreeningSchema] = [],
        refinementResponses: [RefinementWindowSchema] = []
    ) {
        self.availabilityStatus = availabilityStatus
        self.contextSize = contextSize
        self.coarseSchemaTokens = coarseSchemaTokens
        self.refinementSchemaTokens = refinementSchemaTokens
        self.tokenCountRule = tokenCountRule
        self.queuedResponses = responses
        self.queuedRefinementResponses = refinementResponses
    }

    nonisolated var runtime: FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in await self.currentAvailabilityStatus() },
            contextSize: { await self.currentContextSize() },
            tokenCount: { prompt in await self.currentTokenCount(for: prompt) },
            coarseSchemaTokenCount: { await self.currentCoarseSchemaTokenCount() },
            refinementSchemaTokenCount: { await self.currentRefinementSchemaTokenCount() },
            makeSession: {
                let sessionID = await self.nextSessionID()
                return FoundationModelClassifier.Runtime.Session(
                    prewarm: { promptPrefix in
                        await self.recordPrewarm(sessionID: sessionID, promptPrefix: promptPrefix)
                    },
                    respondCoarse: { prompt in
                        try await self.recordResponse(sessionID: sessionID, prompt: prompt)
                    },
                    respondRefinement: { prompt in
                        try await self.recordRefinementResponse(sessionID: sessionID, prompt: prompt)
                    }
                )
            }
        )
    }

    func snapshot() -> Snapshot {
        Snapshot(
            sessionCount: sessionCount,
            prewarmCalls: prewarmCalls,
            respondCalls: respondCalls,
            respondRefinementCalls: respondRefinementCalls
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

    private func currentCoarseSchemaTokenCount() -> Int {
        coarseSchemaTokens
    }

    private func currentRefinementSchemaTokenCount() -> Int {
        refinementSchemaTokens
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

    private func recordRefinementResponse(sessionID: Int, prompt: String) throws -> RefinementWindowSchema {
        respondRefinementCalls.append(RespondRefinementCall(sessionID: sessionID, prompt: prompt))
        if queuedRefinementResponses.isEmpty {
            return RefinementWindowSchema(spans: [])
        }
        return queuedRefinementResponses.removeFirst()
    }
}
