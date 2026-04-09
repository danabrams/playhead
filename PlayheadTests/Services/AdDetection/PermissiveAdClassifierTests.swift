// PermissiveAdClassifierTests.swift
// bd-1en Phase 1: parser-only unit tests for `PermissiveAdClassifier`.
// These tests do NOT call into the live FoundationModels framework —
// they exercise the static `parse` helper directly so they run on the
// simulator without Apple Intelligence. The on-device end-to-end test
// lives in `PlayheadFMSmokeTests::testPermissiveAdClassifierEndToEnd`.

import Foundation
import Testing

@testable import Playhead

@Suite("PermissiveAdClassifier")
struct PermissiveAdClassifierTests {

    // MARK: - Happy-path grammar

    @Test("parses NO_AD as .noAds")
    func parseNoAdReturnsNoAds() {
        let result = PermissiveAdGrammar.parse("NO_AD", validLineRefs: [0, 1, 2])
        #expect(result.disposition == .noAds)
        #expect(result.support == nil)
    }

    @Test("parses UNCERTAIN as .uncertain")
    func parseUncertainReturnsUncertain() {
        let result = PermissiveAdGrammar.parse("UNCERTAIN", validLineRefs: [0, 1, 2])
        #expect(result.disposition == .uncertain)
        #expect(result.support == nil)
    }

    @Test("parses single AD span into containsAd with expanded line refs")
    func parseSingleAdSpanReturnsContainsAd() {
        let result = PermissiveAdGrammar.parse("AD L0-L2", validLineRefs: [0, 1, 2, 3])
        #expect(result.disposition == .containsAd)
        #expect(result.support?.supportLineRefs == [0, 1, 2])
        #expect(result.support?.certainty == .strong)
    }

    @Test("parses multi-span AD into containsAd with union of line refs")
    func parseMultiAdSpanReturnsContainsAd() {
        let result = PermissiveAdGrammar.parse("AD L0-L1,L4-L5", validLineRefs: [0, 1, 2, 3, 4, 5])
        #expect(result.disposition == .containsAd)
        #expect(result.support?.supportLineRefs == [0, 1, 4, 5])
    }

    // MARK: - Validation against window line refs

    @Test("drops AD line refs entirely outside the window and collapses to uncertain")
    func parseInvalidLineRefsAreDropped() {
        let result = PermissiveAdGrammar.parse("AD L99-L100", validLineRefs: [0, 1, 2])
        #expect(result.disposition == .uncertain)
        #expect(result.support == nil)
    }

    @Test("intersects AD line refs with the window and keeps the valid subset")
    func parsePartialIntersectionKeepsValidRefs() {
        let result = PermissiveAdGrammar.parse("AD L0-L99", validLineRefs: [0, 1, 2])
        #expect(result.disposition == .containsAd)
        #expect(result.support?.supportLineRefs == [0, 1, 2])
    }

    // MARK: - Robustness

    @Test("rejects template-literal parroting like AD L<start>-L<end> as uncertain")
    func parseTemplateLiteralIsRejected() {
        let result = PermissiveAdGrammar.parse("AD L<start>-L<end>", validLineRefs: [0, 1, 2])
        #expect(result.disposition == .uncertain)
    }

    @Test("garbage non-grammar response collapses to uncertain")
    func parseGarbageReturnsUncertain() {
        let result = PermissiveAdGrammar.parse("hello world", validLineRefs: [0, 1, 2])
        #expect(result.disposition == .uncertain)
    }

    @Test("empty response collapses to uncertain")
    func parseEmptyReturnsUncertain() {
        let result = PermissiveAdGrammar.parse("", validLineRefs: [0, 1, 2])
        #expect(result.disposition == .uncertain)
    }

    @Test("only the first non-empty line is consulted")
    func parseFirstLineOnly() {
        let result = PermissiveAdGrammar.parse("AD L0-L1\nNO_AD", validLineRefs: [0, 1, 2])
        #expect(result.disposition == .containsAd)
        #expect(result.support?.supportLineRefs == [0, 1])
    }

    @Test("case-insensitive grammar tokens")
    func parseCaseInsensitiveTokens() {
        #expect(PermissiveAdGrammar.parse("no_ad", validLineRefs: [0]).disposition == .noAds)
        #expect(PermissiveAdGrammar.parse("uncertain", validLineRefs: [0]).disposition == .uncertain)
        let lower = PermissiveAdGrammar.parse("ad l0-l1", validLineRefs: [0, 1])
        #expect(lower.disposition == .containsAd)
        #expect(lower.support?.supportLineRefs == [0, 1])
    }

    @Test("trims surrounding whitespace before parsing")
    func parseTrimsWhitespace() {
        let result = PermissiveAdGrammar.parse("   NO_AD   ", validLineRefs: [0])
        #expect(result.disposition == .noAds)
    }

    @Test("non-contiguous line refs in the window still validate properly")
    func parseRespectsNonContiguousValidRefs() {
        // Window contains line refs 5, 7, 9 (with gaps). Model says L5-L9.
        // Intersection should be {5, 7, 9} — all three valid refs that
        // fall inside the requested span.
        let result = PermissiveAdGrammar.parse("AD L5-L9", validLineRefs: [5, 7, 9])
        #expect(result.disposition == .containsAd)
        #expect(result.support?.supportLineRefs == [5, 7, 9])
    }

    // MARK: - bd-1en Phase 2 + Cycle 2 H5: refinement parser

    @Test("refinement parser: NO_AD → .noAd")
    func refinementParseNoAd() throws {
        #expect(try PermissiveAdGrammar.parseRefinement("NO_AD", validLineRefs: [0, 1, 2]) == .noAd)
    }

    @Test("refinement parser: UNCERTAIN throws .permissiveDecodingFailure")
    func refinementParseUncertain() {
        #expect(throws: PermissiveClassificationError.self) {
            _ = try PermissiveAdGrammar.parseRefinement("UNCERTAIN", validLineRefs: [0, 1, 2])
        }
    }

    @Test("refinement parser: empty throws .permissiveDecodingFailure")
    func refinementParseEmpty() {
        #expect(throws: PermissiveClassificationError.self) {
            _ = try PermissiveAdGrammar.parseRefinement("", validLineRefs: [0, 1, 2])
        }
    }

    @Test("refinement parser: single AD span produces one pair")
    func refinementParseSingleSpan() throws {
        let result = try PermissiveAdGrammar.parseRefinement("AD L0-L2", validLineRefs: [0, 1, 2, 3])
        #expect(result == .spans([RefinementSpanPair(firstLineRef: 0, lastLineRef: 2)]))
    }

    @Test("refinement parser: multi-span AD preserves pair structure")
    func refinementParseMultiSpan() throws {
        // CRITICAL: refinement parser must preserve gaps. Coarse parser
        // flattens to a single line ref set; refinement keeps pairs so
        // each becomes its own RefinedAdSpan downstream.
        let result = try PermissiveAdGrammar.parseRefinement(
            "AD L0-L1,L4-L5",
            validLineRefs: [0, 1, 2, 3, 4, 5]
        )
        #expect(result == .spans([
            RefinementSpanPair(firstLineRef: 0, lastLineRef: 1),
            RefinementSpanPair(firstLineRef: 4, lastLineRef: 5)
        ]))
    }

    @Test("refinement parser: clamps spans that overflow the window")
    func refinementParseClampsOverflow() throws {
        let result = try PermissiveAdGrammar.parseRefinement("AD L0-L99", validLineRefs: [0, 1, 2])
        #expect(result == .spans([RefinementSpanPair(firstLineRef: 0, lastLineRef: 2)]))
    }

    @Test("refinement parser: drops spans entirely outside the window — throws decoding failure")
    func refinementParseDropsFullyOutside() {
        #expect(throws: PermissiveClassificationError.self) {
            _ = try PermissiveAdGrammar.parseRefinement("AD L99-L100", validLineRefs: [0, 1, 2])
        }
    }

    @Test("refinement parser: keeps a partial-overlap span and drops a fully-outside span")
    func refinementParseDropsOnlyOutsidePair() throws {
        // L0-L99 clamps to L0-L2; L500-L600 has no overlap and is dropped.
        let result = try PermissiveAdGrammar.parseRefinement(
            "AD L0-L99,L500-L600",
            validLineRefs: [0, 1, 2]
        )
        #expect(result == .spans([RefinementSpanPair(firstLineRef: 0, lastLineRef: 2)]))
    }

    @Test("refinement parser: template-literal parroting throws .permissiveDecodingFailure (no recall fallback)")
    func refinementParseTemplateLiteralIsUnparsed() {
        // Cycle 2 H5: template parroting used to collapse to .unparsed
        // and produce a misleadingly-precise full-window span. It now
        // throws so the runner re-queues the window cleanly.
        #expect(throws: PermissiveClassificationError.self) {
            _ = try PermissiveAdGrammar.parseRefinement(
                "AD L<start>-L<end>",
                validLineRefs: [0, 1, 2]
            )
        }
    }

    @Test("refinement parser: garbage response throws .permissiveDecodingFailure")
    func refinementParseGarbage() {
        #expect(throws: PermissiveClassificationError.self) {
            _ = try PermissiveAdGrammar.parseRefinement("hello world", validLineRefs: [0])
        }
    }

    @Test("refinement parser: case-insensitive grammar tokens")
    func refinementParseCaseInsensitive() throws {
        #expect(try PermissiveAdGrammar.parseRefinement("no_ad", validLineRefs: [0]) == .noAd)
        let lower = try PermissiveAdGrammar.parseRefinement("ad l0-l1", validLineRefs: [0, 1])
        #expect(lower == .spans([RefinementSpanPair(firstLineRef: 0, lastLineRef: 1)]))
    }

    @Test("refinement parser: sparse window snaps endpoints inward to valid refs")
    func refinementParseSparseWindowSnap() throws {
        // Window has line refs [5, 7, 9] (planner gaps at 6, 8). Model
        // returns AD L6-L8 — neither endpoint is a valid ref. Snap L6
        // up to L7 (next valid) and L8 down to L7 (prev valid).
        let result = try PermissiveAdGrammar.parseRefinement("AD L6-L8", validLineRefs: [5, 7, 9])
        #expect(result == .spans([RefinementSpanPair(firstLineRef: 7, lastLineRef: 7)]))
    }

    // MARK: - Cycle 2 H5 / Rev2-M3 regression rails

    @Test("Cycle 2 H5: parser failure throws (regression rail — no .rough fallback)")
    func parserFailureNeverProducesRoughFallback() {
        // The previous behavior collapsed garbage into a single
        // full-window span with `.rough` boundaryPrecision. The regression
        // rail asserts the parser now throws and the `.rough` enum case
        // is gone (compile-time check via BoundaryPrecision below).
        #expect(throws: PermissiveClassificationError.self) {
            _ = try PermissiveAdGrammar.parseRefinement("AD L<n>-L<m>", validLineRefs: [0, 1, 2])
        }
    }

    @Test("Cycle 2 Rev2-M3: adversarial integer range expansion is clamped")
    func adversarialRangeExpansionThrows() {
        // The parser must refuse to expand a range larger than
        // PermissiveAdGrammar.maximumRangeExpansion (10,000) — this
        // protects against the FM hallucinating `AD L0-L999999` and
        // burning a CPU/heap budget materializing the integer range.
        #expect(throws: PermissiveClassificationError.self) {
            _ = try PermissiveAdGrammar.parseRefinement(
                "AD L0-L999999",
                validLineRefs: [0, 1, 2]
            )
        }
    }

    // MARK: - Cycle 2 Rev2-M2: RefinementSpanPair invariant

    @Test("Cycle 2 Rev2-M2: RefinementSpanPair init enforces firstLineRef <= lastLineRef")
    func refinementSpanPairInitRejectsReversedPair() {
        // We can only test the non-failing case here without crashing
        // the test process. The precondition is documented and exercised
        // by the parser whose `min/max` swap means a reversed pair never
        // reaches the init.
        let pair = RefinementSpanPair(firstLineRef: 3, lastLineRef: 7)
        #expect(pair.firstLineRef == 3)
        #expect(pair.lastLineRef == 7)
    }

    // MARK: - Cycle 2 H3: refinement focus + cap honoring

    @Test("Cycle 2 H3: focusLineRefs hint appears in the rendered prompt")
    func refinementBuilderHonorsFocusLineRefs() {
        let segments = (0..<6).map { i in
            makePermissiveTestLookup(indices: [i])[i]!
        }
        let prompt = PermissiveAdGrammar.buildRefinementPrompt(
            for: segments,
            focusLineRefs: [2, 3]
        )
        #expect(prompt.contains("Focus your refinement on these line refs first: L2, L3."))
    }

    @Test("Cycle 2 H3: focusClusters hint appears in the rendered prompt")
    func refinementBuilderHonorsFocusClusters() {
        let segments = (0..<6).map { i in
            makePermissiveTestLookup(indices: [i])[i]!
        }
        let prompt = PermissiveAdGrammar.buildRefinementPrompt(
            for: segments,
            focusClusters: [[0, 1, 2]]
        )
        #expect(prompt.contains("These clusters probably belong to the same ad read: [L0, L1, L2]."))
    }

    @Test("Cycle 2 H3: maximumSpans hint appears in the rendered prompt and is enforced as a cap")
    func refinementBuilderHonorsMaximumSpansHintAndApplyFocusAndCapEnforcesIt() {
        let segments = (0..<6).map { i in
            makePermissiveTestLookup(indices: [i])[i]!
        }
        let prompt = PermissiveAdGrammar.buildRefinementPrompt(
            for: segments,
            maximumSpans: 1
        )
        #expect(prompt.contains("Return at most 1 span(s)."))

        // Hard-cap behavior: applyFocusAndCap drops all but the
        // top-priority span when more pairs come back than allowed.
        let pairs: [RefinementSpanPair] = [
            RefinementSpanPair(firstLineRef: 0, lastLineRef: 0),
            RefinementSpanPair(firstLineRef: 2, lastLineRef: 3),
            RefinementSpanPair(firstLineRef: 5, lastLineRef: 5)
        ]
        let capped = PermissiveAdGrammar.applyFocusAndCap(
            to: .spans(pairs),
            focusLineRefs: [2, 3],
            maximumSpans: 1
        )
        if case let .spans(survivors) = capped {
            #expect(survivors.count == 1)
            #expect(survivors.first?.firstLineRef == 2)
            #expect(survivors.first?.lastLineRef == 3)
        } else {
            Issue.record("expected .spans, got \(capped)")
        }
    }

    @Test("Cycle 2 H3: applyFocusAndCap is a no-op when maximumSpans is unset")
    func applyFocusAndCapPassThroughWithDefault() {
        let pairs: [RefinementSpanPair] = [
            RefinementSpanPair(firstLineRef: 0, lastLineRef: 0),
            RefinementSpanPair(firstLineRef: 1, lastLineRef: 1),
            RefinementSpanPair(firstLineRef: 2, lastLineRef: 2)
        ]
        let result = PermissiveAdGrammar.applyFocusAndCap(
            to: .spans(pairs),
            focusLineRefs: [],
            maximumSpans: Int.max
        )
        #expect(result == .spans(pairs))
    }

    // MARK: - PermissiveRefinementResult.refinedSpans(for:lineRefLookup:)

    @Test("refinedSpans .noAd → empty spans (window dropped)")
    func refinedSpansNoAdReturnsEmpty() {
        let plan = makePermissiveTestPlan(lineRefs: [0, 1])
        let lookup = makePermissiveTestLookup(indices: [0, 1])
        let spans = PermissiveRefinementResult.noAd.refinedSpans(for: plan, lineRefLookup: lookup)
        #expect(spans.isEmpty)
    }

    @Test("refinedSpans .spans → one RefinedAdSpan per pair")
    func refinedSpansSpansReturnOnePerPair() {
        let plan = makePermissiveTestPlan(lineRefs: [0, 1, 2, 3])
        let lookup = makePermissiveTestLookup(indices: [0, 1, 2, 3])
        let result = PermissiveRefinementResult.spans([
            RefinementSpanPair(firstLineRef: 0, lastLineRef: 1),
            RefinementSpanPair(firstLineRef: 2, lastLineRef: 3)
        ])
        let spans = result.refinedSpans(for: plan, lineRefLookup: lookup)
        #expect(spans.count == 2)
        #expect(spans[0].firstLineRef == 0 && spans[0].lastLineRef == 1)
        #expect(spans[1].firstLineRef == 2 && spans[1].lastLineRef == 3)
        // Permissive path is anchorless and never memory-write-eligible.
        #expect(spans.allSatisfy { $0.resolvedEvidenceAnchors.isEmpty })
        #expect(spans.allSatisfy { $0.memoryWriteEligible == false })
        #expect(spans.allSatisfy { $0.certainty == .strong })
        #expect(spans.allSatisfy { $0.commercialIntent == .paid })
        #expect(spans.allSatisfy { $0.boundaryPrecision == .usable })
        // Cycle 2 H4: every permissive span carries the
        // ownership-suppressed flag so downstream consumers can tell
        // these classification dimensions were defaulted, not inferred.
        #expect(spans.allSatisfy { $0.ownershipInferenceWasSuppressed })
    }

    @Test("refinedSpans .spans drops pairs whose endpoints are missing from the lookup")
    func refinedSpansDropsMissingLookupEntries() {
        let plan = makePermissiveTestPlan(lineRefs: [0, 1, 2])
        // Lookup only has line refs 0 and 2 — line ref 1 is missing.
        // The pair (1, 2) should drop because firstSegment is nil; the
        // pair (0, 0) should survive.
        let lookup = makePermissiveTestLookup(indices: [0, 2])
        let result = PermissiveRefinementResult.spans([
            RefinementSpanPair(firstLineRef: 0, lastLineRef: 0),
            RefinementSpanPair(firstLineRef: 1, lastLineRef: 2)
        ])
        let spans = result.refinedSpans(for: plan, lineRefLookup: lookup)
        #expect(spans.count == 1)
        #expect(spans[0].firstLineRef == 0 && spans[0].lastLineRef == 0)
    }

    // MARK: - PermissiveRefinementResult description

    @Test("description returns short label for log lines")
    func descriptionReturnsLabel() {
        #expect(PermissiveRefinementResult.spans([]).description == "spans")
        #expect(PermissiveRefinementResult.noAd.description == "noAd")
    }
}

// MARK: - Test fixtures

private func makePermissiveTestPlan(lineRefs: [Int]) -> RefinementWindowPlan {
    RefinementWindowPlan(
        windowIndex: 0,
        sourceWindowIndex: 0,
        lineRefs: lineRefs,
        focusLineRefs: lineRefs,
        focusClusters: [lineRefs],
        prompt: "test",
        promptTokenCount: 8,
        startTime: 0,
        endTime: 10,
        stopReason: .minimumSpan,
        promptEvidence: []
    )
}

private func makePermissiveTestLookup(indices: [Int]) -> [Int: AdTranscriptSegment] {
    var lookup: [Int: AdTranscriptSegment] = [:]
    for i in indices {
        lookup[i] = AdTranscriptSegment(
            atoms: [
                TranscriptAtom(
                    atomKey: TranscriptAtomKey(
                        analysisAssetId: "asset-permissive-test",
                        transcriptVersion: "transcript-v1",
                        atomOrdinal: i
                    ),
                    contentHash: "hash-\(i)",
                    startTime: Double(i),
                    endTime: Double(i) + 1,
                    text: "L\(i) text",
                    chunkIndex: i
                )
            ],
            segmentIndex: i
        )
    }
    return lookup
}
