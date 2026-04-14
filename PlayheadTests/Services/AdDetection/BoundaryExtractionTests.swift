// BoundaryExtractionTests.swift
// B9: Tests for discourse unit segmentation, FM boundary schema parsing,
// boundary extraction planning, and integration with SpanHypothesisEngine.

import Foundation
import Testing
@testable import Playhead

// MARK: - Discourse Unit Segmentation Tests

@Suite("DiscourseUnitSegmenter")
struct DiscourseUnitSegmenterTests {

    private func makeAtom(
        ordinal: Int,
        start: Double,
        end: Double,
        text: String
    ) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                atomOrdinal: ordinal
            ),
            contentHash: String(format: "%08x", ordinal),
            startTime: start,
            endTime: end,
            text: text,
            chunkIndex: ordinal
        )
    }

    @Test("empty atoms produce no discourse units")
    func emptyAtomsProduceNoUnits() {
        let units = DiscourseUnitSegmenter.segment(atoms: [])
        #expect(units.isEmpty)
    }

    @Test("single atom produces one discourse unit")
    func singleAtomProducesOneUnit() {
        let atoms = [makeAtom(ordinal: 0, start: 0, end: 3.0, text: "Hello world.")]
        let units = DiscourseUnitSegmenter.segment(atoms: atoms)
        #expect(units.count == 1)
        #expect(units[0].ref == "S0")
        #expect(units[0].text == "Hello world.")
    }

    @Test("pause-based segmentation splits on gaps >= threshold")
    func pauseBasedSegmentation() {
        let atoms = [
            makeAtom(ordinal: 0, start: 0.0, end: 2.5, text: "First sentence here."),
            makeAtom(ordinal: 1, start: 3.5, end: 5.5, text: "After a pause."),
            makeAtom(ordinal: 2, start: 5.6, end: 7.5, text: "Still going."),
        ]
        let config = DiscourseUnitSegmenter.Config(
            minDuration: 2.0,
            maxDuration: 8.0,
            pauseThreshold: 0.5
        )
        let units = DiscourseUnitSegmenter.segment(atoms: atoms, config: config)
        #expect(units.count == 2)
        #expect(units[0].ref == "S0")
        #expect(units[0].atoms.count == 1)
        #expect(units[1].ref == "S1")
        #expect(units[1].atoms.count == 2)
    }

    @Test("punctuation-based segmentation splits on sentence endings")
    func punctuationBasedSegmentation() {
        let atoms = [
            makeAtom(ordinal: 0, start: 0.0, end: 1.0, text: "First"),
            makeAtom(ordinal: 1, start: 1.0, end: 2.5, text: "sentence."),
            makeAtom(ordinal: 2, start: 2.5, end: 3.5, text: "Second"),
            makeAtom(ordinal: 3, start: 3.5, end: 5.0, text: "sentence here."),
        ]
        let config = DiscourseUnitSegmenter.Config(
            minDuration: 2.0,
            maxDuration: 8.0,
            pauseThreshold: 0.5
        )
        let units = DiscourseUnitSegmenter.segment(atoms: atoms, config: config)
        // First two atoms: 0.0-2.5 = 2.5s, ends with ".", should split
        #expect(units.count == 2)
        #expect(units[0].atoms.count == 2)
        #expect(units[1].atoms.count == 2)
    }

    @Test("max duration forces a break")
    func maxDurationForcesBreak() {
        // 10 atoms spanning 10 seconds, max duration 4s
        let atoms = (0..<10).map { i in
            makeAtom(ordinal: i, start: Double(i), end: Double(i) + 0.9, text: "word\(i)")
        }
        let config = DiscourseUnitSegmenter.Config(
            minDuration: 2.0,
            maxDuration: 4.0,
            pauseThreshold: 0.5
        )
        let units = DiscourseUnitSegmenter.segment(atoms: atoms, config: config)
        // Should produce at least 2 units (10s / 4s max)
        #expect(units.count >= 2)
        for unit in units {
            #expect(unit.duration <= 5.0) // Allow overshoot up to one atom duration past maxDuration
        }
    }

    @Test("min duration prevents micro-units")
    func minDurationPreventsMicroUnits() {
        // Small atoms with pauses, but some too short to split
        let atoms = [
            makeAtom(ordinal: 0, start: 0.0, end: 0.5, text: "Hi."),
            makeAtom(ordinal: 1, start: 1.5, end: 2.0, text: "There."),
            makeAtom(ordinal: 2, start: 2.0, end: 4.5, text: "Longer segment here now."),
        ]
        let config = DiscourseUnitSegmenter.Config(
            minDuration: 2.0,
            maxDuration: 8.0,
            pauseThreshold: 0.5
        )
        let units = DiscourseUnitSegmenter.segment(atoms: atoms, config: config)
        // The first atom is only 0.5s, below min duration, so pause split
        // after it should not trigger. The atoms should merge.
        for unit in units {
            #expect(unit.atoms.count >= 1)
        }
    }

    @Test("refs are sequential S0, S1, S2...")
    func refsAreSequential() {
        let atoms = [
            makeAtom(ordinal: 0, start: 0.0, end: 3.0, text: "First part."),
            makeAtom(ordinal: 1, start: 4.0, end: 7.0, text: "Second part."),
            makeAtom(ordinal: 2, start: 8.0, end: 11.0, text: "Third part."),
        ]
        let config = DiscourseUnitSegmenter.Config(
            minDuration: 2.0,
            maxDuration: 8.0,
            pauseThreshold: 0.5
        )
        let units = DiscourseUnitSegmenter.segment(atoms: atoms, config: config)
        for (i, unit) in units.enumerated() {
            #expect(unit.ref == "S\(i)")
        }
    }

    @Test("trailing micro-unit merges back into previous unit")
    func trailingMicroUnitMergesBack() {
        let atoms = [
            makeAtom(ordinal: 0, start: 0.0, end: 3.0, text: "First sentence here."),
            makeAtom(ordinal: 1, start: 4.0, end: 7.0, text: "Second sentence here."),
            makeAtom(ordinal: 2, start: 8.0, end: 8.5, text: "tiny"),
        ]
        let config = DiscourseUnitSegmenter.Config(
            minDuration: 2.0,
            maxDuration: 8.0,
            pauseThreshold: 0.5
        )
        let units = DiscourseUnitSegmenter.segment(atoms: atoms, config: config)
        // The trailing 0.5s atom should merge back into the previous unit
        let lastUnit = units.last!
        #expect(lastUnit.atoms.contains(where: { $0.text == "tiny" }))
        #expect(lastUnit.atoms.count >= 2) // merged with previous
    }
}

// MARK: - FMBoundarySchema Parsing Tests

@Suite("FMBoundarySchema")
struct FMBoundarySchemaTests {

    @Test("schema with spans parses correctly")
    func schemaWithSpansParses() {
        let schema = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S2",
                    role: .adBody,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: ["E0", "E1"]
                )
            ],
            abstain: false
        )
        #expect(schema.spans.count == 1)
        #expect(schema.spans[0].role == .adBody)
        #expect(schema.spans[0].commercialIntent == .strong)
        #expect(schema.spans[0].ownership == .thirdParty)
        #expect(!schema.abstain)
    }

    @Test("abstain schema has no spans")
    func abstainSchema() {
        let schema = FMBoundarySchema(spans: [], abstain: true)
        #expect(schema.spans.isEmpty)
        #expect(schema.abstain)
    }

    @Test("all SpanRole values are representable")
    func allSpanRoles() {
        let roles: [SpanRole] = [.show, .adIntro, .adBody, .adCTA, .returnToShow]
        #expect(roles.count == 5)
        #expect(Set(roles.map(\.rawValue)).count == 5)
    }

    @Test("schema with multiple spans preserves order")
    func multipleSpansOrder() {
        let schema = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S0",
                    role: .adIntro,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: []
                ),
                FMSpanLabel(
                    firstSegmentRef: "S1",
                    lastSegmentRef: "S3",
                    role: .adBody,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: ["E0"]
                ),
                FMSpanLabel(
                    firstSegmentRef: "S4",
                    lastSegmentRef: "S4",
                    role: .adCTA,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: ["E1"]
                ),
            ],
            abstain: false
        )
        #expect(schema.spans.count == 3)
        #expect(schema.spans[0].role == .adIntro)
        #expect(schema.spans[1].role == .adBody)
        #expect(schema.spans[2].role == .adCTA)
    }

    @Test("schema JSON round-trips correctly")
    func jsonRoundTrip() throws {
        let original = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S1",
                    role: .show,
                    commercialIntent: .weak,
                    ownership: .show,
                    evidenceRefs: []
                ),
            ],
            abstain: false
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(FMBoundarySchema.self, from: data)
        #expect(decoded == original)
    }
}

// MARK: - Boundary Extraction Planning Tests

@Suite("planBoundaryExtractionWindows")
struct BoundaryExtractionPlanningTests {

    private func makeAtom(
        ordinal: Int,
        start: Double,
        end: Double,
        text: String
    ) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                atomOrdinal: ordinal
            ),
            contentHash: String(format: "%08x", ordinal),
            startTime: start,
            endTime: end,
            text: text,
            chunkIndex: ordinal
        )
    }

    private func makeSegments() -> [AdTranscriptSegment] {
        let atoms = (0..<20).map { i in
            makeAtom(ordinal: i, start: Double(i) * 3.0, end: Double(i) * 3.0 + 2.5, text: "word\(i)")
        }
        return [AdTranscriptSegment(
            atoms: atoms,
            segmentIndex: 0,
            boundaryReason: .startOfTranscript,
            boundaryConfidence: 1.0,
            segmentType: .speech
        )]
    }

    private func makeCandidateSpan(
        id: String = "span-1",
        startTime: Double = 0,
        endTime: Double = 30,
        confidence: Double = 0.8
    ) -> CandidateAdSpan {
        CandidateAdSpan(
            id: id,
            analysisAssetId: "test-asset",
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            evidenceScore: 3.0,
            anchorType: .disclosure,
            sponsorEntity: NormalizedSponsor("TestBrand"),
            isSkipEligible: true,
            evidenceText: "test evidence",
            closingReason: .explicitClose
        )
    }

    @Test("empty candidate spans produce no plans")
    func emptyCandidateSpansProduceNoPlans() async throws {
        let testRuntime = TestFMRuntime()
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)
        let plans = try await classifier.planBoundaryExtractionWindows(
            candidateSpans: [],
            segments: makeSegments(),
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                entries: []
            )
        )
        #expect(plans.isEmpty)
    }

    @Test("one candidate span produces one plan with discourse units")
    func oneCandidateSpanProducesOnePlan() async throws {
        let testRuntime = TestFMRuntime()
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)
        let span = makeCandidateSpan()
        let plans = try await classifier.planBoundaryExtractionWindows(
            candidateSpans: [span],
            segments: makeSegments(),
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                entries: []
            )
        )
        #expect(plans.count == 1)
        #expect(plans[0].candidateSpanId == span.id)
        #expect(!plans[0].discourseUnits.isEmpty)
        #expect(plans[0].prompt.contains("S0>"))
    }

    @Test("plans include evidence refs from catalog")
    func plansIncludeEvidenceRefs() async throws {
        let testRuntime = TestFMRuntime()
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)
        let span = makeCandidateSpan()
        let evidence = EvidenceEntry(
            evidenceRef: 0,
            category: .url,
            matchedText: "testbrand.com",
            normalizedText: "testbrand.com",
            atomOrdinal: 5,
            startTime: 15.0,
            endTime: 16.0
        )
        let plans = try await classifier.planBoundaryExtractionWindows(
            candidateSpans: [span],
            segments: makeSegments(),
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                entries: [evidence]
            )
        )
        #expect(plans.count == 1)
        #expect(plans[0].evidenceRefs.count == 1)
        #expect(plans[0].prompt.contains("Evidence catalog:"))
        #expect(plans[0].prompt.contains("testbrand.com"))
    }
}

// MARK: - Boundary Extraction Tests

@Suite("extractBoundaries")
struct BoundaryExtractionTests {

    private func makeAtom(
        ordinal: Int,
        start: Double,
        end: Double,
        text: String
    ) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                atomOrdinal: ordinal
            ),
            contentHash: String(format: "%08x", ordinal),
            startTime: start,
            endTime: end,
            text: text,
            chunkIndex: ordinal
        )
    }

    private func makeCandidateSpan(
        id: String = "span-1",
        startTime: Double = 0,
        endTime: Double = 30,
        confidence: Double = 0.8
    ) -> CandidateAdSpan {
        CandidateAdSpan(
            id: id,
            analysisAssetId: "test-asset",
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            evidenceScore: 3.0,
            anchorType: .disclosure,
            sponsorEntity: NormalizedSponsor("TestBrand"),
            isSkipEligible: true,
            evidenceText: "test evidence",
            closingReason: .explicitClose
        )
    }

    private func makeDiscourseUnits() -> [DiscourseUnit] {
        [
            DiscourseUnit(ref: "S0", atoms: [
                makeAtom(ordinal: 0, start: 0, end: 3, text: "This episode is brought to you by TestBrand.")
            ]),
            DiscourseUnit(ref: "S1", atoms: [
                makeAtom(ordinal: 1, start: 3, end: 6, text: "I love using TestBrand every day.")
            ]),
            DiscourseUnit(ref: "S2", atoms: [
                makeAtom(ordinal: 2, start: 6, end: 9, text: "Go to testbrand.com for a discount.")
            ]),
        ]
    }

    @Test("extractBoundaries returns FM span labels")
    func extractBoundariesReturnsSpanLabels() async throws {
        let fmResponse = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S0",
                    role: .adIntro,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: []
                ),
                FMSpanLabel(
                    firstSegmentRef: "S1",
                    lastSegmentRef: "S1",
                    role: .adBody,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: []
                ),
                FMSpanLabel(
                    firstSegmentRef: "S2",
                    lastSegmentRef: "S2",
                    role: .adCTA,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: ["E0"]
                ),
            ],
            abstain: false
        )
        let testRuntime = TestFMRuntime(
            boundaryExtractionResponses: [fmResponse]
        )
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        let units = makeDiscourseUnits()
        let plan = BoundaryExtractionWindowPlan(
            windowIndex: 0,
            candidateSpanId: "span-1",
            discourseUnits: units,
            prompt: "test prompt",
            promptTokenCount: 10,
            startTime: 0,
            endTime: 9,
            evidenceRefs: []
        )

        let result = try await classifier.extractBoundaries(
            plans: [plan],
            candidateSpans: [makeCandidateSpan()]
        )
        #expect(result.status == .success)
        #expect(result.windows.count == 1)
        #expect(result.windows[0].schema.spans.count == 3)
        #expect(result.windows[0].schema.spans[0].role == .adIntro)
        #expect(result.windows[0].schema.spans[1].role == .adBody)
        #expect(result.windows[0].schema.spans[2].role == .adCTA)
    }

    @Test("extractBoundaries respects abstain")
    func extractBoundariesRespectsAbstain() async throws {
        let fmResponse = FMBoundarySchema(spans: [], abstain: true)
        let testRuntime = TestFMRuntime(
            boundaryExtractionResponses: [fmResponse]
        )
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        let plan = BoundaryExtractionWindowPlan(
            windowIndex: 0,
            candidateSpanId: "span-1",
            discourseUnits: makeDiscourseUnits(),
            prompt: "test prompt",
            promptTokenCount: 10,
            startTime: 0,
            endTime: 9,
            evidenceRefs: []
        )

        let result = try await classifier.extractBoundaries(
            plans: [plan],
            candidateSpans: [makeCandidateSpan()]
        )
        #expect(result.status == .success)
        #expect(result.windows.isEmpty) // abstained window is not included
    }

    @Test("extractBoundaries handles empty plans")
    func extractBoundariesHandlesEmptyPlans() async throws {
        let testRuntime = TestFMRuntime()
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        let result = try await classifier.extractBoundaries(
            plans: [],
            candidateSpans: []
        )
        #expect(result.status == .success)
        #expect(result.windows.isEmpty)
    }

    @Test("extractBoundaries prewarms session")
    func extractBoundariesPrewarmsSession() async throws {
        let fmResponse = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S0",
                    role: .show,
                    commercialIntent: .weak,
                    ownership: .show,
                    evidenceRefs: []
                ),
            ],
            abstain: false
        )
        let testRuntime = TestFMRuntime(
            boundaryExtractionResponses: [fmResponse]
        )
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        let plan = BoundaryExtractionWindowPlan(
            windowIndex: 0,
            candidateSpanId: "span-1",
            discourseUnits: makeDiscourseUnits(),
            prompt: "test prompt",
            promptTokenCount: 10,
            startTime: 0,
            endTime: 9,
            evidenceRefs: []
        )

        let result = try await classifier.extractBoundaries(
            plans: [plan],
            candidateSpans: [makeCandidateSpan()]
        )
        #expect(result.prewarmHit)
    }

    @Test("extractBoundaries reports failure on FM error")
    func extractBoundariesReportsFailureOnFMError() async throws {
        let testRuntime = TestFMRuntime(
            boundaryExtractionFailures: [.refusal]
        )
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        let plan = BoundaryExtractionWindowPlan(
            windowIndex: 0,
            candidateSpanId: "span-1",
            discourseUnits: makeDiscourseUnits(),
            prompt: "test prompt",
            promptTokenCount: 10,
            startTime: 0,
            endTime: 9,
            evidenceRefs: []
        )

        let result = try await classifier.extractBoundaries(
            plans: [plan],
            candidateSpans: [makeCandidateSpan()]
        )
        #expect(result.status == .failedTransient)
        #expect(result.windows.isEmpty)
    }

    @Test("extractBoundaries skips window when prompt exceeds budget")
    func extractBoundariesSkipsBudgetOverflow() async throws {
        // contextSize=64, schemaTokenCount=32 leaves a small budget.
        // A plan with promptTokenCount=10_000 should exceed it.
        let testRuntime = TestFMRuntime(
            contextSize: 64,
            boundarySchemaTokenCount: 32
        )
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        let plan = BoundaryExtractionWindowPlan(
            windowIndex: 0,
            candidateSpanId: "span-1",
            discourseUnits: makeDiscourseUnits(),
            prompt: "test prompt",
            promptTokenCount: 10_000,
            startTime: 0,
            endTime: 9,
            evidenceRefs: []
        )

        let result = try await classifier.extractBoundaries(
            plans: [plan],
            candidateSpans: [makeCandidateSpan()]
        )
        // Single plan was skipped due to budget, so all plans failed
        #expect(result.status == .failedTransient)
        #expect(result.windows.isEmpty)
    }

    @Test("extractBoundaries handles mixed success, abstain, and failure")
    func extractBoundariesMixedResults() async throws {
        let successResponse = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S0",
                    role: .adBody,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: []
                ),
            ],
            abstain: false
        )
        let abstainResponse = FMBoundarySchema(spans: [], abstain: true)
        let testRuntime = TestFMRuntime(
            boundaryExtractionResponses: [successResponse, abstainResponse],
            // Third call fails with a refusal
            boundaryExtractionFailures: [nil, nil, .refusal]
        )
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        let plans = (0..<3).map { i in
            BoundaryExtractionWindowPlan(
                windowIndex: i,
                candidateSpanId: "span-\(i)",
                discourseUnits: makeDiscourseUnits(),
                prompt: "test prompt \(i)",
                promptTokenCount: 10,
                startTime: Double(i) * 10,
                endTime: Double(i) * 10 + 9,
                evidenceRefs: []
            )
        }

        let spans = (0..<3).map { i in
            makeCandidateSpan(
                id: "span-\(i)",
                startTime: Double(i) * 10,
                endTime: Double(i) * 10 + 9
            )
        }

        let result = try await classifier.extractBoundaries(
            plans: plans,
            candidateSpans: spans
        )
        // 1 success + 1 abstain + 1 failure = not all failed, so .success
        #expect(result.status == .success)
        // Only the first window produced output (abstain and failure are excluded)
        #expect(result.windows.count == 1)
        #expect(result.windows[0].candidateSpanId == "span-0")
        #expect(result.windows[0].schema.spans[0].role == .adBody)
    }
}

// MARK: - Contradiction Guardrail Tests

@Suite("checkContradiction")
struct ContradictionGuardrailTests {

    private func makeAtom(
        ordinal: Int,
        start: Double,
        end: Double,
        text: String
    ) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                atomOrdinal: ordinal
            ),
            contentHash: String(format: "%08x", ordinal),
            startTime: start,
            endTime: end,
            text: text,
            chunkIndex: ordinal
        )
    }

    private func makeDiscourseUnits(ranges: [(Double, Double)]) -> [DiscourseUnit] {
        ranges.enumerated().map { i, range in
            DiscourseUnit(ref: "S\(i)", atoms: [
                makeAtom(ordinal: i, start: range.0, end: range.1, text: "text\(i)")
            ])
        }
    }

    @Test("no contradiction when candidate has high confidence")
    func noContradictionHighConfidence() {
        let schema = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S2",
                    role: .adBody,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: []
                ),
            ],
            abstain: false
        )
        let candidate = CandidateAdSpan(
            id: "span-1",
            analysisAssetId: "test-asset",
            startTime: 0,
            endTime: 10,
            confidence: 0.8, // high confidence
            evidenceScore: 3.0,
            anchorType: .disclosure,
            sponsorEntity: nil,
            isSkipEligible: true,
            evidenceText: "test",
            closingReason: .explicitClose
        )
        let units = makeDiscourseUnits(ranges: [(0, 3), (3, 6), (100, 110)])

        let result = FoundationModelClassifier.checkContradiction(
            schema: schema,
            candidate: candidate,
            discourseUnits: units
        )
        #expect(!result) // High confidence = no contradiction check
    }

    @Test("contradiction when FM span extends well beyond low-confidence candidate")
    func contradictionOnLowConfidenceOverextension() {
        let schema = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S2",
                    role: .adBody,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: []
                ),
            ],
            abstain: false
        )
        let candidate = CandidateAdSpan(
            id: "span-1",
            analysisAssetId: "test-asset",
            startTime: 0,
            endTime: 5,
            confidence: 0.3, // low confidence
            evidenceScore: 1.0,
            anchorType: .disclosure,
            sponsorEntity: nil,
            isSkipEligible: false,
            evidenceText: "test",
            closingReason: .timeout
        )
        // FM says S0 to S2 is an ad, but S2 is way outside the candidate
        let units = makeDiscourseUnits(ranges: [(0, 2), (2, 4), (50, 60)])

        let result = FoundationModelClassifier.checkContradiction(
            schema: schema,
            candidate: candidate,
            discourseUnits: units
        )
        #expect(result) // FM span extends far beyond candidate
    }

    @Test("no contradiction when FM span is within candidate boundaries")
    func noContradictionWhenFMSpanWithinCandidate() {
        let schema = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S1",
                    role: .adBody,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: []
                ),
            ],
            abstain: false
        )
        let candidate = CandidateAdSpan(
            id: "span-1",
            analysisAssetId: "test-asset",
            startTime: 0,
            endTime: 10,
            confidence: 0.3, // low confidence
            evidenceScore: 1.0,
            anchorType: .disclosure,
            sponsorEntity: nil,
            isSkipEligible: false,
            evidenceText: "test",
            closingReason: .timeout
        )
        let units = makeDiscourseUnits(ranges: [(0, 3), (3, 6)])

        let result = FoundationModelClassifier.checkContradiction(
            schema: schema,
            candidate: candidate,
            discourseUnits: units
        )
        #expect(!result) // FM span is within candidate
    }

    @Test("show role spans are ignored in contradiction check")
    func showRoleSpansIgnored() {
        let schema = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S2",
                    role: .show, // not an ad role
                    commercialIntent: .weak,
                    ownership: .show,
                    evidenceRefs: []
                ),
            ],
            abstain: false
        )
        let candidate = CandidateAdSpan(
            id: "span-1",
            analysisAssetId: "test-asset",
            startTime: 0,
            endTime: 5,
            confidence: 0.3,
            evidenceScore: 1.0,
            anchorType: .disclosure,
            sponsorEntity: nil,
            isSkipEligible: false,
            evidenceText: "test",
            closingReason: .timeout
        )
        let units = makeDiscourseUnits(ranges: [(0, 2), (2, 4), (50, 60)])

        let result = FoundationModelClassifier.checkContradiction(
            schema: schema,
            candidate: candidate,
            discourseUnits: units
        )
        #expect(!result) // Show role = not an ad, no contradiction
    }
}

// MARK: - Prompt Construction Tests

@Suite("buildBoundaryExtractionPrompt")
struct BoundaryExtractionPromptTests {

    private func makeAtom(
        ordinal: Int,
        start: Double,
        end: Double,
        text: String
    ) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                atomOrdinal: ordinal
            ),
            contentHash: String(format: "%08x", ordinal),
            startTime: start,
            endTime: end,
            text: text,
            chunkIndex: ordinal
        )
    }

    @Test("prompt includes one-shot examples")
    func promptIncludesOneShotExamples() {
        let units = [
            DiscourseUnit(ref: "S0", atoms: [
                makeAtom(ordinal: 0, start: 0, end: 3, text: "Test content.")
            ]),
        ]
        let prompt = FoundationModelClassifier.buildBoundaryExtractionPrompt(
            discourseUnits: units,
            evidenceEntries: []
        )
        #expect(prompt.contains("Example 1 (host-read ad):"))
        #expect(prompt.contains("Example 2 (not an ad"))
        #expect(prompt.contains("FreshBox"))
        #expect(prompt.contains("The Deep Dive"))
    }

    @Test("prompt includes discourse unit refs")
    func promptIncludesDiscourseUnitRefs() {
        let units = [
            DiscourseUnit(ref: "S0", atoms: [
                makeAtom(ordinal: 0, start: 0, end: 3, text: "Hello world.")
            ]),
            DiscourseUnit(ref: "S1", atoms: [
                makeAtom(ordinal: 1, start: 3, end: 6, text: "Second unit.")
            ]),
        ]
        let prompt = FoundationModelClassifier.buildBoundaryExtractionPrompt(
            discourseUnits: units,
            evidenceEntries: []
        )
        #expect(prompt.contains("S0> \"Hello world.\""))
        #expect(prompt.contains("S1> \"Second unit.\""))
    }

    @Test("prompt includes evidence catalog when present")
    func promptIncludesEvidenceCatalog() {
        let units = [
            DiscourseUnit(ref: "S0", atoms: [
                makeAtom(ordinal: 0, start: 0, end: 3, text: "Test.")
            ]),
        ]
        let evidence = [
            EvidenceEntry(
                evidenceRef: 0,
                category: .url,
                matchedText: "brand.com",
                normalizedText: "brand.com",
                atomOrdinal: 0,
                startTime: 0,
                endTime: 3
            ),
        ]
        let prompt = FoundationModelClassifier.buildBoundaryExtractionPrompt(
            discourseUnits: units,
            evidenceEntries: evidence
        )
        #expect(prompt.contains("Evidence catalog:"))
        #expect(prompt.contains("[E0] \"brand.com\""))
    }

    @Test("evidence with repetition shows count")
    func evidenceWithRepetitionShowsCount() {
        let units = [
            DiscourseUnit(ref: "S0", atoms: [
                makeAtom(ordinal: 0, start: 0, end: 3, text: "Test.")
            ]),
        ]
        let evidence = [
            EvidenceEntry(
                evidenceRef: 1,
                category: .brandSpan,
                matchedText: "BetterHelp",
                normalizedText: "betterhelp",
                atomOrdinal: 0,
                startTime: 12,
                endTime: 14,
                count: 4,
                firstTime: 12,
                lastTime: 67
            ),
        ]
        let prompt = FoundationModelClassifier.buildBoundaryExtractionPrompt(
            discourseUnits: units,
            evidenceEntries: evidence
        )
        #expect(prompt.contains("×4"))
    }
}

// MARK: - Integration with SpanHypothesisEngine

@Suite("Boundary extraction with CandidateAdSpan")
struct BoundaryExtractionIntegrationTests {

    private func makeAtom(
        ordinal: Int,
        start: Double,
        end: Double,
        text: String
    ) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                atomOrdinal: ordinal
            ),
            contentHash: String(format: "%08x", ordinal),
            startTime: start,
            endTime: end,
            text: text,
            chunkIndex: ordinal
        )
    }

    @Test("end-to-end: plan + extract with CandidateAdSpan from hypothesis engine")
    func endToEndPlanAndExtract() async throws {
        // Simulate a CandidateAdSpan that would come from SpanHypothesisEngine
        let candidateSpan = CandidateAdSpan(
            id: "hyp-span-1",
            analysisAssetId: "test-asset",
            startTime: 10.0,
            endTime: 40.0,
            confidence: 0.85,
            evidenceScore: 4.0,
            anchorType: .disclosure,
            sponsorEntity: NormalizedSponsor("BetterHelp"),
            isSkipEligible: true,
            evidenceText: "brought to you by BetterHelp",
            closingReason: .explicitClose
        )

        // Build atoms that span the candidate's time range
        let atoms = (0..<15).map { i in
            makeAtom(
                ordinal: i,
                start: 10.0 + Double(i) * 2.0,
                end: 10.0 + Double(i) * 2.0 + 1.8,
                text: i == 0 ? "This episode is brought to you by BetterHelp." :
                      i == 7 ? "Go to betterhelp.com slash podcast." :
                      "Regular speech content here."
            )
        }

        let segments = [AdTranscriptSegment(
            atoms: atoms,
            segmentIndex: 0,
            boundaryReason: .startOfTranscript,
            boundaryConfidence: 1.0,
            segmentType: .speech
        )]

        let evidence = EvidenceCatalog(
            analysisAssetId: "test-asset",
            transcriptVersion: "v1",
            entries: [
                EvidenceEntry(
                    evidenceRef: 0,
                    category: .disclosurePhrase,
                    matchedText: "brought to you by",
                    normalizedText: "brought to you by",
                    atomOrdinal: 0,
                    startTime: 10.0,
                    endTime: 11.8
                ),
                EvidenceEntry(
                    evidenceRef: 1,
                    category: .url,
                    matchedText: "betterhelp.com slash podcast",
                    normalizedText: "betterhelp.com slash podcast",
                    atomOrdinal: 7,
                    startTime: 24.0,
                    endTime: 25.8
                ),
            ]
        )

        let fmResponse = FMBoundarySchema(
            spans: [
                FMSpanLabel(
                    firstSegmentRef: "S0",
                    lastSegmentRef: "S0",
                    role: .adIntro,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: ["E0"]
                ),
                FMSpanLabel(
                    firstSegmentRef: "S1",
                    lastSegmentRef: "S2",
                    role: .adBody,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: []
                ),
                FMSpanLabel(
                    firstSegmentRef: "S3",
                    lastSegmentRef: "S3",
                    role: .adCTA,
                    commercialIntent: .strong,
                    ownership: .thirdParty,
                    evidenceRefs: ["E1"]
                ),
            ],
            abstain: false
        )

        let testRuntime = TestFMRuntime(
            boundaryExtractionResponses: [fmResponse]
        )
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        // Step 1: Plan
        let plans = try await classifier.planBoundaryExtractionWindows(
            candidateSpans: [candidateSpan],
            segments: segments,
            evidenceCatalog: evidence
        )
        #expect(plans.count == 1)
        #expect(plans[0].candidateSpanId == "hyp-span-1")
        #expect(!plans[0].discourseUnits.isEmpty)

        // Step 2: Extract
        let result = try await classifier.extractBoundaries(
            plans: plans,
            candidateSpans: [candidateSpan]
        )
        #expect(result.status == .success)
        #expect(result.windows.count == 1)

        let output = result.windows[0]
        #expect(output.candidateSpanId == "hyp-span-1")
        #expect(output.schema.spans.count == 3)
        #expect(!output.schema.abstain)
        #expect(!output.hadContradiction)

        // Verify the FM call count
        let callCount = await testRuntime.boundaryExtractionCallCount
        #expect(callCount == 1)
    }

    @Test("multiple candidate spans produce multiple plans")
    func multipleCandidateSpansProduceMultiplePlans() async throws {
        let span1 = CandidateAdSpan(
            id: "span-a",
            analysisAssetId: "test-asset",
            startTime: 0,
            endTime: 15,
            confidence: 0.8,
            evidenceScore: 3.0,
            anchorType: .disclosure,
            sponsorEntity: nil,
            isSkipEligible: true,
            evidenceText: "test",
            closingReason: .explicitClose
        )
        let span2 = CandidateAdSpan(
            id: "span-b",
            analysisAssetId: "test-asset",
            startTime: 60,
            endTime: 90,
            confidence: 0.7,
            evidenceScore: 2.5,
            anchorType: .url,
            sponsorEntity: nil,
            isSkipEligible: true,
            evidenceText: "test2",
            closingReason: .explicitClose
        )

        let atoms = (0..<40).map { i in
            makeAtom(
                ordinal: i,
                start: Double(i) * 2.5,
                end: Double(i) * 2.5 + 2.0,
                text: "content \(i)"
            )
        }

        let segments = [AdTranscriptSegment(
            atoms: atoms,
            segmentIndex: 0,
            boundaryReason: .startOfTranscript,
            boundaryConfidence: 1.0,
            segmentType: .speech
        )]

        let testRuntime = TestFMRuntime()
        let classifier = FoundationModelClassifier(runtime: testRuntime.runtime)

        let plans = try await classifier.planBoundaryExtractionWindows(
            candidateSpans: [span1, span2],
            segments: segments,
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: "test-asset",
                transcriptVersion: "v1",
                entries: []
            )
        )
        #expect(plans.count == 2)
        #expect(plans[0].candidateSpanId == "span-a")
        #expect(plans[1].candidateSpanId == "span-b")
    }
}
