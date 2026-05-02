// AnalysisWorkSchedulerTransportStubSourceCanaryTests.swift
//
// skeptical-review-cycle-18 M-1 source canary: every test that
// constructs an `AnalysisWorkScheduler(...)` MUST pass an explicit
// `transportStatusProvider:` argument. The default value in
// `AnalysisWorkScheduler.init` is `LiveTransportStatusProvider()`,
// whose `NWPathMonitor` first-update latency is non-deterministic on
// the test simulator. Under heavy parallel load (PlayheadFastTests
// runs 5102 tests across 743 suites concurrently), the path monitor
// sometimes has not delivered its initial `.satisfied` update by the
// time `evaluateAdmissionGate` runs, producing intermittent
// `reject(.noNetwork)` flakes (most recently observed 2026-05-01).
//
// Cycle-16 #45 closed two flake sites by injecting a stub. Cycle-17
// added two more. Cycle-18 M-1 expanded the rollout to the remaining
// 13 holdouts AND added this canary so a future test author who
// forgets the stub trips the canary at build time rather than
// re-introducing the flake months later when CI happens to schedule
// their suite alongside the high-parallelism stragglers.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan selectedTests/skippedTests` silently ignores Swift
// Testing identifiers — see project memory
// `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class AnalysisWorkSchedulerTransportStubSourceCanaryTests: XCTestCase {

    /// Walks `PlayheadTests/` and asserts every `AnalysisWorkScheduler(`
    /// construction passes an explicit `transportStatusProvider:`
    /// argument inside the same parenthesised arg list.
    ///
    /// Note: the walker does NOT path-skip `Helpers/Stubs.swift` or
    /// `Helpers/ManualClock.swift`; both files happen to be free of
    /// `AnalysisWorkScheduler(` ctor calls (the former declares the
    /// stub type only, the latter mentions the scheduler only inside a
    /// doc-comment which the comment-stripper drops). If a future edit
    /// adds a real ctor call inside one of those helpers, the canary
    /// will (correctly) trip there too — at which point the helper
    /// either needs to pass `transportStatusProvider:` or be added to
    /// an explicit allow-list.
    func testEveryAnalysisWorkSchedulerConstructionPinsTransportStatusProvider() throws {
        guard let repoRoot = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
            XCTFail("Could not locate repository root from \(#filePath)")
            return
        }
        let testsRoot = repoRoot.appendingPathComponent("PlayheadTests")

        // Recursively enumerate every .swift file under PlayheadTests/.
        guard let enumerator = FileManager.default.enumerator(
            at: testsRoot,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            XCTFail("Could not enumerate \(testsRoot.path)")
            return
        }

        var holdouts: [String] = []

        for case let url as URL in enumerator {
            guard url.pathExtension == "swift" else { continue }
            let source = try String(contentsOf: url, encoding: .utf8)
            // Strip comments AND string literals so neither a doc-
            // comment usage example (e.g. ManualClock.swift) nor a log
            // message that happens to contain the literal substring
            // "AnalysisWorkScheduler(" can false-trip the canary.
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)

            // Find every `AnalysisWorkScheduler(` ctor call and check
            // that its parenthesised arg list contains the expected
            // labelled argument. The match is whitespace-tolerant
            // (`AnalysisWorkScheduler   (` would still match).
            var searchStart = stripped.startIndex
            while let ctorRange = stripped.range(
                of: #"AnalysisWorkScheduler\s*\("#,
                options: .regularExpression,
                range: searchStart..<stripped.endIndex
            ) {
                // Locate the open paren that we just matched. The
                // regex match ends just AFTER the `(`, so step back one
                // to land on the `(` itself.
                let openParenIndex = stripped.index(before: ctorRange.upperBound)
                precondition(stripped[openParenIndex] == "(", "regex matched without trailing `(`")

                // Walk forward tracking paren depth to find the matching
                // close paren. Comments/strings have already been
                // stripped, so we don't need to track them again.
                let argList = parenBalancedArgList(in: stripped, openParenAt: openParenIndex)

                // Whitespace-tolerant match for the labelled argument.
                let hasArg = argList.range(
                    of: #"\btransportStatusProvider\s*:"#,
                    options: .regularExpression
                ) != nil

                if !hasArg {
                    let line = lineNumber(of: ctorRange.lowerBound, in: stripped)
                    let relPath = url.path.replacingOccurrences(
                        of: repoRoot.path + "/",
                        with: ""
                    )
                    holdouts.append("\(relPath):\(line)")
                }

                searchStart = ctorRange.upperBound
            }
        }

        XCTAssertEqual(
            holdouts.count, 0,
            """
            \(holdouts.count) `AnalysisWorkScheduler(...)` test \
            construction(s) do not pass an explicit \
            `transportStatusProvider:` argument:
              \(holdouts.joined(separator: "\n              "))
            Cycle-18 M-1: every scheduler test must pin the transport \
            axis with `StubTransportStatusProvider()` (defined in \
            `PlayheadTests/Helpers/Stubs.swift`) so the \
            `LiveTransportStatusProvider`'s `NWPathMonitor` first-update \
            latency cannot intermittently reject the test with \
            `.noNetwork` under heavy parallel load. Add the argument \
            (and override `reachability:` / `allowsCellular:` if your \
            test needs to exercise a non-Wi-Fi axis) OR, if the call \
            is in a non-test helper that genuinely needs the live \
            provider, extend this canary's allow-list with a documented \
            rationale.
            """
        )
    }

    /// Cycle-19 M-4: also pin that the value passed to
    /// `transportStatusProvider:` is a stub (a `StubTransportStatusProvider(...)`
    /// expression OR an identifier from the small allow-list of let-
    /// bindings whose initializer has been hand-verified to be a
    /// stub). The label-only canary above catches the test author who
    /// forgot the parameter; this catches the test author who passes
    /// `LiveTransportStatusProvider()` (or any non-stub) and re-opens
    /// the same NWPathMonitor first-update flake.
    ///
    /// Allow-listed identifier names below MUST be hand-verified at
    /// the call site to bind to a `StubTransportStatusProvider(...)`
    /// initializer. If you add a new entry, leave a comment with the
    /// site so the next reviewer can re-verify.
    func testEveryTransportStatusProviderArgumentIsAStub() throws {
        guard let repoRoot = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
            XCTFail("Could not locate repository root from \(#filePath)")
            return
        }
        let testsRoot = repoRoot.appendingPathComponent("PlayheadTests")

        guard let enumerator = FileManager.default.enumerator(
            at: testsRoot,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            XCTFail("Could not enumerate \(testsRoot.path)")
            return
        }

        // Allow-listed identifier names — bare identifiers passed as
        // the value of `transportStatusProvider:`. Each MUST resolve at
        // its call site to a `StubTransportStatusProvider(...)`
        // initializer. Tracked sites:
        //   • `transport` — `AnalysisWorkSchedulerLaneAdmissionTests.swift`,
        //     bound from `let transport = StubTransportStatusProvider(
        //     reachability: .cellular, allowsCellular: true)` to drive
        //     the cellular-rejection axis.
        let allowedIdentifiers: Set<String> = ["transport"]

        var bad: [String] = []

        for case let url as URL in enumerator {
            guard url.pathExtension == "swift" else { continue }
            let source = try String(contentsOf: url, encoding: .utf8)
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)

            // Find every `transportStatusProvider:` label and extract
            // the value expression (everything up to the next top-
            // level `,` or matching `)`). The expression is normalised
            // by trimming whitespace.
            var searchStart = stripped.startIndex
            while let labelRange = stripped.range(
                of: #"\btransportStatusProvider\s*:"#,
                options: .regularExpression,
                range: searchStart..<stripped.endIndex
            ) {
                let valueStart = labelRange.upperBound
                let value = extractArgumentValue(in: stripped, startingAt: valueStart)
                let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)

                let directStub = trimmed.range(
                    of: #"^StubTransportStatusProvider\s*\("#,
                    options: .regularExpression
                ) != nil

                let bareIdentifier = trimmed.range(
                    of: #"^[A-Za-z_][A-Za-z0-9_]*$"#,
                    options: .regularExpression
                ) != nil

                let allowedIdent = bareIdentifier && allowedIdentifiers.contains(trimmed)

                if !(directStub || allowedIdent) {
                    let line = lineNumber(of: labelRange.lowerBound, in: stripped)
                    let relPath = url.path.replacingOccurrences(
                        of: repoRoot.path + "/",
                        with: ""
                    )
                    bad.append("\(relPath):\(line) value=`\(trimmed)`")
                }

                searchStart = labelRange.upperBound
            }
        }

        XCTAssertEqual(
            bad.count, 0,
            """
            \(bad.count) `transportStatusProvider:` argument(s) pass a \
            value that is neither a direct \
            `StubTransportStatusProvider(...)` initializer nor an \
            allow-listed bare identifier:
              \(bad.joined(separator: "\n              "))
            Cycle-19 M-4: the label-only canary above catches the \
            holdout that forgot to pass the argument; this canary \
            catches the holdout that passes a `LiveTransportStatusProvider()` \
            or another non-stub value and re-opens the same \
            NWPathMonitor first-update flake. Either pass \
            `StubTransportStatusProvider()` (with the appropriate \
            `reachability:` / `allowsCellular:` overrides) directly, \
            OR — if you must pass through a let-binding — verify it \
            initializes from `StubTransportStatusProvider(...)` and add \
            its identifier name to `allowedIdentifiers` above with a \
            documented call-site reference.
            """
        )
    }

    // MARK: - Helpers

    /// Returns the substring inside the matching parens, given the
    /// index of the opening `(`. Tracks paren depth so nested calls
    /// don't terminate the slice early. Returns the empty string if
    /// the parens are unbalanced.
    private func parenBalancedArgList(
        in source: String,
        openParenAt openIdx: String.Index
    ) -> String {
        precondition(source[openIdx] == "(")
        var depth = 0
        var i = openIdx
        let endIdx = source.endIndex
        var bodyStart: String.Index?

        while i < endIdx {
            let c = source[i]
            if c == "(" {
                depth += 1
                if depth == 1 {
                    bodyStart = source.index(after: i)
                }
            } else if c == ")" {
                depth -= 1
                if depth == 0, let start = bodyStart {
                    return String(source[start..<i])
                }
            }
            i = source.index(after: i)
        }
        return ""
    }

    /// Returns the 1-based line number of the given index in `source`.
    private func lineNumber(of index: String.Index, in source: String) -> Int {
        source[..<index].reduce(into: 1) { acc, c in
            if c == "\n" { acc += 1 }
        }
    }

    /// Walks forward from `start` and returns the substring of the
    /// argument expression — i.e. everything up to (but not including)
    /// the next top-level `,` or `)` (where "top-level" means at paren
    /// depth zero relative to where we started). Treats brackets,
    /// braces, and angle-brackets as nesting tokens too so a generic
    /// type or closure literal doesn't truncate the slice early.
    /// Comments and strings have already been stripped by the caller.
    private func extractArgumentValue(
        in source: String,
        startingAt start: String.Index
    ) -> String {
        var i = start
        let endIdx = source.endIndex
        var parenDepth = 0
        var bracketDepth = 0
        var braceDepth = 0

        while i < endIdx {
            let c = source[i]
            if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 {
                if c == "," || c == ")" {
                    return String(source[start..<i])
                }
            }
            switch c {
            case "(": parenDepth += 1
            case ")": parenDepth -= 1
            case "[": bracketDepth += 1
            case "]": bracketDepth -= 1
            case "{": braceDepth += 1
            case "}": braceDepth -= 1
            default: break
            }
            i = source.index(after: i)
        }
        return String(source[start..<endIdx])
    }
}
