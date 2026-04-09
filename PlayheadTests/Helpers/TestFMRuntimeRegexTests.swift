// TestFMRuntimeRegexTests.swift
// Rev4-L2 (cycle 2): pin the tightened submittedLineRefs regex against
// adversarial input that the original `L(\d+)>` pattern would have
// matched as a false positive.

import Foundation
import Testing
@testable import Playhead

@Suite("TestFMRuntime line-ref regex")
struct TestFMRuntimeRegexTests {

    @Test("Rev4-L2: regex matches a real line marker")
    func matchesRealLineMarker() {
        let prompt = "before <L1> some text <L42> tail"
        let refs = TestFMRuntime.submittedLineRefsForTesting(from: prompt)
        #expect(refs == [1, 42])
    }

    @Test("Rev4-L2: regex rejects letter-prefixed false positive")
    func rejectsLetterPrefixedFalsePositive() {
        // Pre-fix: `L(\d+)>` would match the `L42>` substring inside
        // the token `XYL42>` and report 42 as a submitted line ref.
        // Post-fix: the `(?:^|[^A-Za-z])L(\d+)>` prefix forbids that.
        let prompt = "garbage XYL42> tail"
        let refs = TestFMRuntime.submittedLineRefsForTesting(from: prompt)
        #expect(refs == [], "letter-prefixed L<digits>> must not be reported")
    }

    @Test("Rev4-L2: regex matches at start of input")
    func matchesAtStartOfInput() {
        let prompt = "L7> first thing"
        let refs = TestFMRuntime.submittedLineRefsForTesting(from: prompt)
        #expect(refs == [7])
    }

    @Test("Rev4-L2: regex deduplicates and sorts")
    func dedupesAndSorts() {
        let prompt = "<L3> blah <L1> blah <L3> blah <L2>"
        let refs = TestFMRuntime.submittedLineRefsForTesting(from: prompt)
        #expect(refs == [1, 2, 3])
    }
}
