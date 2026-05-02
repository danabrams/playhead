// SwiftSourceInspectorStringStrippingTests.swift
//
// Cycle-19 M-3: pin the behaviour of
// `SwiftSourceInspector.strippingCommentsAndStrings(_:)` against
// every Swift string-literal form. The original implementation
// only knew about single-quoted `"..."`; a triple-quoted XCTest
// failure message or raw-string SQL fixture would terminate at
// the first inner `"`, leaking text into "code" position and
// producing canary false positives.
//
// XCTest (not Swift Testing) so the tests participate in the
// existing test plan via class-name filters — see project memory
// `xctestplan_swift_testing_limitation`.

import XCTest

final class SwiftSourceInspectorStringStrippingTests: XCTestCase {

    // MARK: - Helpers

    /// Asserts that the boundary characters of every string literal
    /// in `expectedSkeleton` survive stripping, AND that
    /// `forbiddenInsideLiterals` does NOT appear in the stripped
    /// output (it must only have appeared inside literal contents in
    /// the input). Also pins the cycle-23 L-7 invariant that the
    /// stripper preserves Character-by-Character length — callers
    /// (e.g. the trait-carry-forward atomicity canary's walk-back
    /// helper) index back into the original source by the stripped
    /// position, so any drift would silently misalign matches.
    private func assertStripped(
        _ source: String,
        keeps expectedSkeleton: String,
        loses forbiddenInsideLiterals: [String],
        file: StaticString = #file,
        line: UInt = #line
    ) {
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        XCTAssertTrue(
            stripped.contains(expectedSkeleton),
            """
            stripped output is missing expected skeleton.
            expected to find: \(expectedSkeleton.debugDescription)
            actual stripped:  \(stripped.debugDescription)
            """,
            file: file,
            line: line
        )
        for needle in forbiddenInsideLiterals {
            XCTAssertFalse(
                stripped.contains(needle),
                """
                stripped output retained literal contents that should \
                have been blanked.
                forbidden token: \(needle.debugDescription)
                actual stripped: \(stripped.debugDescription)
                """,
                file: file,
                line: line
            )
        }
        XCTAssertEqual(
            stripped.count,
            source.count,
            """
            stripped output Character count diverged from source — the \
            cycle-23 L-7 walk-back precondition would trip at runtime.
            source.count:   \(source.count)
            stripped.count: \(stripped.count)
            stripped:       \(stripped.debugDescription)
            """,
            file: file,
            line: line
        )
    }

    // MARK: - Regular strings (regression coverage)

    func testRegularStringContentsBlanked() {
        let source = #"let x = "hello FORBIDDEN world""#
        assertStripped(
            source,
            keeps: "let x = \"",
            loses: ["FORBIDDEN", "hello", "world"]
        )
    }

    func testEscapedQuoteDoesNotPrematurelyClose() {
        // The `\"` should NOT close the string; "after" is INSIDE it.
        let source = #"let x = "before \"FORBIDDEN\" after""#
        assertStripped(
            source,
            keeps: "let x = \"",
            loses: ["FORBIDDEN", "before", "after"]
        )
    }

    // MARK: - Triple-quoted strings (cycle-19 M-3)

    func testTripleQuotedContentsBlankedAcrossNewlines() {
        let source = """
        let msg = \"\"\"
        line one FORBIDDEN
        line two FORBIDDEN
        \"\"\"
        let after = 1
        """
        assertStripped(
            source,
            keeps: "let after = 1",
            loses: ["FORBIDDEN", "line one", "line two"]
        )
    }

    func testTripleQuotedDoesNotMisparseInnerSingleQuotes() {
        // Two inner `"` chars must not close the triple-quoted literal.
        // If they did, the tokens between them would leak into "code".
        let source = """
        let msg = \"\"\"
        He said "FORBIDDEN" loudly.
        \"\"\"
        let after = 1
        """
        assertStripped(
            source,
            keeps: "let after = 1",
            loses: ["FORBIDDEN", "He said", "loudly"]
        )
    }

    // MARK: - Raw strings (cycle-19 M-3)

    func testSingleHashRawStringContentsBlanked() {
        let source = ##"let x = #"raw FORBIDDEN content"#"##
        assertStripped(
            source,
            keeps: "let x = ",
            loses: ["FORBIDDEN", "raw", "content"]
        )
    }

    func testSingleHashRawStringIsNotClosedByBareQuote() {
        // The inner bare `"` must NOT close `#"..."#` — only `"#` does.
        let source = ##"let x = #"a "literal" FORBIDDEN quote"#"##
        assertStripped(
            source,
            keeps: "let x = ",
            loses: ["FORBIDDEN", "literal", "quote"]
        )
    }

    func testMultiHashRawStringRequiresMatchingHashRun() {
        // Closer requires exactly two `#`s. A single `"#` inside the
        // body must NOT close the literal.
        let source = ###"let x = ##"contains "# FORBIDDEN inside"##"###
        assertStripped(
            source,
            keeps: "let x = ",
            loses: ["FORBIDDEN", "contains", "inside"]
        )
    }

    func testRawStringDoesNotProcessBackslashEscapes() {
        // In a raw string, `\"` is two literal characters; the `"`
        // is NOT escaped so closing `"#` after it still closes.
        // Token "AFTER" must therefore be in CODE position.
        let source = ##"let x = #"raw \"FORBIDDEN\" body"# ; let AFTER = 1"##
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        XCTAssertTrue(
            stripped.contains("let AFTER = 1"),
            "raw-string close was not detected; trailing code leaked into string state. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertFalse(
            stripped.contains("FORBIDDEN"),
            "raw-string contents leaked. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertEqual(
            stripped.count,
            source.count,
            "stripped Character count diverged — walk-back precondition would trip. Stripped: \(stripped.debugDescription)"
        )
    }

    // MARK: - Raw triple-quoted strings (cycle-19 M-3)

    func testRawTripleQuotedContentsBlanked() {
        let source = ##"""
        let msg = #"""
        FORBIDDEN line one
        FORBIDDEN "literal triple" inner "" quotes
        """#
        let after = 1
        """##
        assertStripped(
            source,
            keeps: "let after = 1",
            loses: ["FORBIDDEN", "line one", "literal triple", "inner"]
        )
    }

    // MARK: - `#` that is NOT a raw-string opener

    func testHashIfDirectivePassesThroughAsCode() {
        // `#if`, `#available`, `#filePath` are NOT string literals.
        // The `#` and the directive token must both survive in the
        // stripped output so canaries that look for them still work.
        let source = """
        #if DEBUG
        let path = #filePath
        #endif
        """
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        XCTAssertTrue(
            stripped.contains("#if DEBUG"),
            "`#if DEBUG` was stripped — it must survive. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertTrue(
            stripped.contains("#filePath"),
            "`#filePath` was stripped — it must survive. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertTrue(
            stripped.contains("#endif"),
            "`#endif` was stripped — it must survive. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertEqual(
            stripped.count,
            source.count,
            "stripped Character count diverged — walk-back precondition would trip. Stripped: \(stripped.debugDescription)"
        )
    }

    // MARK: - Comment-handling regression coverage

    func testLineCommentContentsBlankedNewlinePreserved() {
        let source = "let x = 1 // FORBIDDEN comment text\nlet y = 2"
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        XCTAssertFalse(
            stripped.contains("FORBIDDEN"),
            "line comment contents leaked. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertTrue(
            stripped.contains("let x = 1"),
            "code before comment was lost. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertTrue(
            stripped.contains("let y = 2"),
            "code after newline was lost. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertEqual(
            stripped.count,
            source.count,
            "stripped Character count diverged — walk-back precondition would trip. Stripped: \(stripped.debugDescription)"
        )
    }

    func testBlockCommentContentsBlanked() {
        let source = "let x = /* FORBIDDEN block */ 1"
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        XCTAssertFalse(
            stripped.contains("FORBIDDEN"),
            "block comment contents leaked. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertTrue(
            stripped.contains("let x ="),
            "code before block comment was lost. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertTrue(
            stripped.contains("1"),
            "code after block comment was lost. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertEqual(
            stripped.count,
            source.count,
            "stripped Character count diverged — walk-back precondition would trip. Stripped: \(stripped.debugDescription)"
        )
    }

    // MARK: - Line-number alignment (canary error messages reference lines)

    func testLineCountIsPreservedAcrossStringsAndComments() {
        // Three logical newlines in input → three logical newlines in
        // output. Tests stripped a multi-line string literal and a
        // multi-line block comment together.
        let source = """
        let a = \"\"\"
        FORBIDDEN
        \"\"\"
        /*
        FORBIDDEN
        */
        let z = 1
        """
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        XCTAssertEqual(
            stripped.filter({ $0 == "\n" }).count,
            source.filter({ $0 == "\n" }).count,
            "newline count drifted between input and stripped output. Stripped: \(stripped.debugDescription)"
        )
        XCTAssertEqual(
            stripped.count,
            source.count,
            "stripped Character count diverged — walk-back precondition would trip. Stripped: \(stripped.debugDescription)"
        )
    }
}
