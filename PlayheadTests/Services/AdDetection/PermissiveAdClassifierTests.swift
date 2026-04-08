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
}
