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

    // MARK: - bd-1en Phase 2: refinement parser

    @Test("refinement parser: NO_AD → .noAd")
    func refinementParseNoAd() {
        #expect(PermissiveAdGrammar.parseRefinement("NO_AD", validLineRefs: [0, 1, 2]) == .noAd)
    }

    @Test("refinement parser: UNCERTAIN → .unparsed (recall safety net)")
    func refinementParseUncertain() {
        #expect(PermissiveAdGrammar.parseRefinement("UNCERTAIN", validLineRefs: [0, 1, 2]) == .unparsed)
    }

    @Test("refinement parser: empty → .unparsed")
    func refinementParseEmpty() {
        #expect(PermissiveAdGrammar.parseRefinement("", validLineRefs: [0, 1, 2]) == .unparsed)
    }

    @Test("refinement parser: single AD span produces one pair")
    func refinementParseSingleSpan() {
        let result = PermissiveAdGrammar.parseRefinement("AD L0-L2", validLineRefs: [0, 1, 2, 3])
        #expect(result == .spans([RefinementSpanPair(firstLineRef: 0, lastLineRef: 2)]))
    }

    @Test("refinement parser: multi-span AD preserves pair structure")
    func refinementParseMultiSpan() {
        // CRITICAL: refinement parser must preserve gaps. Coarse parser
        // flattens to a single line ref set; refinement keeps pairs so
        // each becomes its own RefinedAdSpan downstream.
        let result = PermissiveAdGrammar.parseRefinement(
            "AD L0-L1,L4-L5",
            validLineRefs: [0, 1, 2, 3, 4, 5]
        )
        #expect(result == .spans([
            RefinementSpanPair(firstLineRef: 0, lastLineRef: 1),
            RefinementSpanPair(firstLineRef: 4, lastLineRef: 5)
        ]))
    }

    @Test("refinement parser: clamps spans that overflow the window")
    func refinementParseClampsOverflow() {
        let result = PermissiveAdGrammar.parseRefinement("AD L0-L99", validLineRefs: [0, 1, 2])
        #expect(result == .spans([RefinementSpanPair(firstLineRef: 0, lastLineRef: 2)]))
    }

    @Test("refinement parser: drops spans entirely outside the window")
    func refinementParseDropsFullyOutside() {
        let result = PermissiveAdGrammar.parseRefinement("AD L99-L100", validLineRefs: [0, 1, 2])
        #expect(result == .unparsed)
    }

    @Test("refinement parser: keeps a partial-overlap span and drops a fully-outside span")
    func refinementParseDropsOnlyOutsidePair() {
        // L0-L99 clamps to L0-L2; L500-L600 has no overlap and is dropped.
        let result = PermissiveAdGrammar.parseRefinement(
            "AD L0-L99,L500-L600",
            validLineRefs: [0, 1, 2]
        )
        #expect(result == .spans([RefinementSpanPair(firstLineRef: 0, lastLineRef: 2)]))
    }

    @Test("refinement parser: template-literal parroting → .unparsed (recall fallback)")
    func refinementParseTemplateLiteralIsUnparsed() {
        let result = PermissiveAdGrammar.parseRefinement(
            "AD L<start>-L<end>",
            validLineRefs: [0, 1, 2]
        )
        #expect(result == .unparsed)
    }

    @Test("refinement parser: garbage response → .unparsed")
    func refinementParseGarbage() {
        #expect(PermissiveAdGrammar.parseRefinement("hello world", validLineRefs: [0]) == .unparsed)
    }

    @Test("refinement parser: case-insensitive grammar tokens")
    func refinementParseCaseInsensitive() {
        #expect(PermissiveAdGrammar.parseRefinement("no_ad", validLineRefs: [0]) == .noAd)
        let lower = PermissiveAdGrammar.parseRefinement("ad l0-l1", validLineRefs: [0, 1])
        #expect(lower == .spans([RefinementSpanPair(firstLineRef: 0, lastLineRef: 1)]))
    }

    @Test("refinement parser: sparse window snaps endpoints inward to valid refs")
    func refinementParseSparseWindowSnap() {
        // Window has line refs [5, 7, 9] (planner gaps at 6, 8). Model
        // returns AD L6-L8 — neither endpoint is a valid ref. Snap L6
        // up to L7 (next valid) and L8 down to L7 (prev valid).
        let result = PermissiveAdGrammar.parseRefinement("AD L6-L8", validLineRefs: [5, 7, 9])
        #expect(result == .spans([RefinementSpanPair(firstLineRef: 7, lastLineRef: 7)]))
    }

    // MARK: - bd-1en Phase 2: buildPermissiveRefinedSpans

    @Test("buildPermissiveRefinedSpans .noAd → empty spans (window dropped)")
    func buildPermissiveSpansNoAdReturnsEmpty() {
        let plan = makePermissiveTestPlan(lineRefs: [0, 1])
        let lookup = makePermissiveTestLookup(indices: [0, 1])
        let spans = FoundationModelClassifier.buildPermissiveRefinedSpans(
            result: .noAd,
            plan: plan,
            lineRefLookup: lookup
        )
        #expect(spans.isEmpty)
    }

    @Test("buildPermissiveRefinedSpans .spans → one RefinedAdSpan per pair")
    func buildPermissiveSpansSpansReturnOnePerPair() {
        let plan = makePermissiveTestPlan(lineRefs: [0, 1, 2, 3])
        let lookup = makePermissiveTestLookup(indices: [0, 1, 2, 3])
        let spans = FoundationModelClassifier.buildPermissiveRefinedSpans(
            result: .spans([
                RefinementSpanPair(firstLineRef: 0, lastLineRef: 1),
                RefinementSpanPair(firstLineRef: 2, lastLineRef: 3)
            ]),
            plan: plan,
            lineRefLookup: lookup
        )
        #expect(spans.count == 2)
        #expect(spans[0].firstLineRef == 0 && spans[0].lastLineRef == 1)
        #expect(spans[1].firstLineRef == 2 && spans[1].lastLineRef == 3)
        // Permissive path is anchorless and never memory-write-eligible.
        #expect(spans.allSatisfy { $0.resolvedEvidenceAnchors.isEmpty })
        #expect(spans.allSatisfy { $0.memoryWriteEligible == false })
        #expect(spans.allSatisfy { $0.certainty == .strong })
        #expect(spans.allSatisfy { $0.commercialIntent == .paid })
        #expect(spans.allSatisfy { $0.boundaryPrecision == .usable })
    }

    @Test("buildPermissiveRefinedSpans .unparsed → single full-window fallback span")
    func buildPermissiveSpansUnparsedFallback() {
        // Recall safety net: parser failure must NOT lose the ad. Emit
        // a single span covering the full plan window with .rough
        // boundaryPrecision so downstream knows it's a wide fallback.
        let plan = makePermissiveTestPlan(lineRefs: [4, 5, 6, 7])
        let lookup = makePermissiveTestLookup(indices: [4, 5, 6, 7])
        let spans = FoundationModelClassifier.buildPermissiveRefinedSpans(
            result: .unparsed,
            plan: plan,
            lineRefLookup: lookup
        )
        #expect(spans.count == 1)
        #expect(spans[0].firstLineRef == 4)
        #expect(spans[0].lastLineRef == 7)
        #expect(spans[0].boundaryPrecision == .rough)
        #expect(spans[0].resolvedEvidenceAnchors.isEmpty)
        #expect(spans[0].memoryWriteEligible == false)
    }

    @Test("buildPermissiveRefinedSpans .spans drops pairs whose endpoints are missing from the lookup")
    func buildPermissiveSpansDropsMissingLookupEntries() {
        let plan = makePermissiveTestPlan(lineRefs: [0, 1, 2])
        // Lookup only has line refs 0 and 2 — line ref 1 is missing.
        // The pair (1, 2) should drop because firstSegment is nil; the
        // pair (0, 0) should survive.
        let lookup = makePermissiveTestLookup(indices: [0, 2])
        let spans = FoundationModelClassifier.buildPermissiveRefinedSpans(
            result: .spans([
                RefinementSpanPair(firstLineRef: 0, lastLineRef: 0),
                RefinementSpanPair(firstLineRef: 1, lastLineRef: 2)
            ]),
            plan: plan,
            lineRefLookup: lookup
        )
        #expect(spans.count == 1)
        #expect(spans[0].firstLineRef == 0 && spans[0].lastLineRef == 0)
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
