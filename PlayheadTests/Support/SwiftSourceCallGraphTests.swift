// SwiftSourceCallGraphTests.swift
// playhead-xul6: pin the two heuristics the transitive launch-path canary
// stands on, on fixtures small enough to read.
//
// `SwiftSourceCallGraph` decides two things, and both of them can rot
// SILENTLY — a walker that stops resolving reports zero findings, which is
// exactly what a green canary looks like. The whole-tree walk in
// `PlayheadRuntimeInitLaunchPathSourceCanaryTests` guards against that with
// its `requiredWalkNodes` assertions; these tests guard the same thing one
// level down, where a failure names the rule that broke instead of a node
// that went missing.
//
//   1. WHICH `{` IS A CLOSURE. A closure literal is a VALUE — writing it does
//      not run it — so `Task { … }` must be invisible to the walk while
//      `if cond { … }` must not. Getting this backwards in either direction
//      breaks the canary: too permissive and a real synchronous touch is
//      blanked away, too strict and every lazy factory in the launch path is
//      a false positive that someone will "fix" by weakening the rule.
//   2. WHICH CALLS ARE RESOLVED. Constructions, `Type.member(…)` and
//      same-type calls. See the L-numbered limits in `SwiftSourceCallGraph`
//      for what is deliberately out of reach.
//
// XCTest (not Swift Testing) so the class participates in test-plan
// class-name filters — see project memory `xctestplan_swift_testing_limitation`.

import XCTest

final class SwiftSourceCallGraphTests: XCTestCase {

    // MARK: - Helpers

    private func synchronousText(_ source: String) -> String {
        let bytes = SwiftSourceCallGraph.blankingCommentsAndStrings([UInt8](source.utf8))
        return String(decoding: SwiftSourceCallGraph.synchronousText(of: bytes), as: UTF8.self)
    }

    private func assertKeeps(
        _ source: String,
        _ token: String,
        _ message: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertTrue(
            synchronousText(source).contains(token),
            "\(message)\nsource: \(source.debugDescription)\nscanned: \(synchronousText(source).debugDescription)",
            file: file,
            line: line
        )
    }

    private func assertBlanks(
        _ source: String,
        _ token: String,
        _ message: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertFalse(
            synchronousText(source).contains(token),
            "\(message)\nsource: \(source.debugDescription)\nscanned: \(synchronousText(source).debugDescription)",
            file: file,
            line: line
        )
    }

    // MARK: - Deferred closures are blanked

    func testTaskBodyIsTreatedAsDeferred() {
        assertBlanks(
            "Task { await self.refreshSnapshot() }",
            "refreshSnapshot",
            "A `Task { }` body is a different execution — the launch path's sanctioned deferral."
        )
        assertBlanks(
            "Task.detached(priority: .utility) { SystemLanguageModel.default }",
            "SystemLanguageModel",
            "`Task.detached { }` is deferred for the same reason as `Task { }`."
        )
    }

    func testLazyFactoryClosureIsTreatedAsDeferred() {
        assertBlanks(
            "let box = PermissiveClassifierBox { PermissiveAdClassifier() }",
            "PermissiveAdClassifier",
            """
            A factory closure handed to a box is a VALUE. This is the shape \
            `PermissiveClassifierBoxLazinessTests` proves is lazy, and the canary must not \
            need an allowlist entry for it.
            """
        )
    }

    func testArgumentPositionClosureIsTreatedAsDeferred() {
        assertBlanks(
            "let runtime = Runtime(contextSize: { SystemLanguageModel.default.contextSize })",
            "SystemLanguageModel",
            """
            A closure passed as an argument is stored, not called — this is exactly \
            `FoundationModelClassifier.liveRuntime`, which must not read as a launch-path touch.
            """
        )
    }

    // MARK: - Executed blocks are kept

    func testControlFlowBlocksAreScanned() {
        assertKeeps(
            "if isPreviewRuntime { SystemLanguageModel.default }",
            "SystemLanguageModel",
            "An `if` body runs when the enclosing body runs."
        )
        assertKeeps(
            "guard let value else { SystemLanguageModel.default }",
            "SystemLanguageModel",
            "A `guard ... else` body runs in place."
        )
        assertKeeps(
            "for item in items { SystemLanguageModel.default }",
            "SystemLanguageModel",
            "A `for` body runs in place."
        )
        assertKeeps(
            "do { SystemLanguageModel.default } catch { }",
            "SystemLanguageModel",
            "A `do` body runs in place."
        )
    }

    func testPreprocessorIfDoesNotPromoteAClosure() {
        assertBlanks(
            "#if canImport(FoundationModels)\nlet box = Box { SystemLanguageModel.default }\n#endif",
            "SystemLanguageModel",
            """
            `#if` is a directive, not control flow. Reading the `if` in \
            `#if canImport(FoundationModels)` as a control keyword promoted every following \
            closure to "executed" and made the walker report `liveRuntime` as a launch-path \
            touch — a false positive that would have been silenced by weakening the rule.
            """
        )
    }

    func testImmediatelyAppliedClosureIsScanned() {
        assertKeeps(
            "let value: Int = { SystemLanguageModel.default.contextSize }()",
            "SystemLanguageModel",
            "`{ ... }()` runs immediately, so it is not a deferral."
        )
    }

    func testEagerHigherOrderClosureIsScanned() {
        assertKeeps(
            "items.forEach { SystemLanguageModel.default }",
            "SystemLanguageModel",
            "`forEach` invokes its closure synchronously — see `eagerClosureCallees` (L-2)."
        )
    }

    func testStatementAfterAControlBlockIsNotItselfControl() {
        assertBlanks(
            "if flag { log() }\nlet box = Box { SystemLanguageModel.default }",
            "SystemLanguageModel",
            """
            The control keyword must not leak past its own block. If it did, every closure \
            following any `if` in a 1,000-statement init would be scanned as executed.
            """
        )
    }

    // MARK: - Comments and strings

    func testCommentsAndStringLiteralsCannotTripTheWalk() {
        let bytes = SwiftSourceCallGraph.blankingCommentsAndStrings(
            [UInt8](#"""
            // do not construct SystemLanguageModel here
            let message = "SystemLanguageModel"
            /* SystemLanguageModel */
            """#.utf8)
        )
        XCTAssertFalse(
            SwiftSourceCallGraph.containsWord(Array("SystemLanguageModel".utf8), in: bytes),
            """
            A banned token mentioned in a comment or a string literal is not a call. The init \
            body this canary walks carries audit comments naming every forbidden symbol.
            """
        )
    }

    // MARK: - Callee resolution

    func testResolvesConstructionsAndSameTypeCalls() {
        let bytes = [UInt8]("""
        self.capabilitiesService = CapabilitiesService()
        let state = Self.captureSnapshot()
        let probe = FoundationModelsUsabilityProbe.cachedUsability()
        let other = Wrapped.init(value: 1)
        helper()
        """.utf8)
        let callees = Set(SwiftSourceCallGraph.callees(in: bytes, owner: "Owner"))
        XCTAssertTrue(
            callees.contains(SwiftSourceCallGraph.Node(type: "CapabilitiesService", member: "init")),
            "a `Type()` construction must resolve to that type's initialiser — \(callees)"
        )
        XCTAssertTrue(
            callees.contains(SwiftSourceCallGraph.Node(type: "Owner", member: "captureSnapshot")),
            "`Self.member()` must resolve against the owning type — \(callees)"
        )
        XCTAssertTrue(
            callees.contains(
                SwiftSourceCallGraph.Node(
                    type: "FoundationModelsUsabilityProbe", member: "cachedUsability"
                )
            ),
            "`Type.member()` must resolve — this edge is how the snapshot reaches the FM probe — \(callees)"
        )
        XCTAssertTrue(
            callees.contains(SwiftSourceCallGraph.Node(type: "Wrapped", member: "init")),
            """
            `Type.init(` must resolve. The dot-spelling is how a construction got past the \
            untypeable battery's type-name grep; a walker that only knows `Type(` has the \
            same hole — \(callees)
            """
        )
        XCTAssertTrue(
            callees.contains(SwiftSourceCallGraph.Node(type: "Owner", member: "helper")),
            "a bare call must resolve against the owning type — \(callees)"
        )
    }

    func testDoesNotResolveAMemberAccessAsACall() {
        let bytes = [UInt8]("let value = snapshot.foundationModelsContextSize".utf8)
        XCTAssertTrue(
            SwiftSourceCallGraph.callees(in: bytes, owner: "Owner").isEmpty,
            "a property read is not a call edge"
        )
    }
}
