// PlayheadRuntimeWiringSourceCanaryTests.swift
// playhead-jncn / playhead-6boz follow-up: source canaries that pin the
// *call-site wiring* of two pieces that the per-component canaries don't
// cover.
//
//   1. The post-migrate `ActivityRefreshNotification` post — the only
//      mechanism that wakes the Activity tab when its first refresh
//      raced ahead of `AnalysisStore.migrate()`. If a future refactor
//      removes this call, the Activity view stays empty until the user
//      manually pulls to refresh; no other test guards it.
//
//   2. The six `await <logger>.migrate()` calls in the deferred init
//      Task. The per-logger laziness canaries
//      (PlayheadRuntimeLoggerLazinessTests) confirm each logger's init
//      body is empty of disk I/O, but they do NOT confirm
//      `PlayheadRuntime` actually invokes the deferred `migrate()` from
//      its Task. A regression that drops the call would silently move
//      the I/O to first-record-write — observable as a wedge on the
//      first decision/asset/BG-task event after launch.
//
//   3. The DEBUG-only construction of `TranscriptShadowGateLogger`.
//      Release builds must compile zero shadow-gate disk I/O paths;
//      this is enforced by wrapping `try TranscriptShadowGateLogger()`
//      in `#if DEBUG ... #else preBuiltShadowGateLogger = nil #endif`.
//      A regression that hoists the construction out of the DEBUG arm
//      (e.g. by deleting the `#if`) would ship file-system code in
//      release binaries — silently breaking the on-device legal
//      mandate that no shadow data leaks beyond a developer build.
//
// XCTest (not Swift Testing) so the canary class is filterable through
// the Xcode test plan's `skippedTests` (`xctestplan` silently ignores
// Swift Testing identifiers; see PlayheadFastTests.xctestplan comment).

import Foundation
import XCTest
@testable import Playhead

final class PlayheadRuntimeWiringSourceCanaryTests: XCTestCase {

    /// `PlayheadRuntime`'s deferred init Task posts
    /// `ActivityRefreshNotification` immediately after `analysisStore.migrate()`
    /// succeeds. The Activity tab (and any other consumer of the
    /// notification) relies on this to repopulate when its first
    /// snapshot fetch raced the cold-launch lazy open.
    ///
    /// The canary asserts:
    ///   • the post call appears in `PlayheadRuntime.swift`
    ///   • it appears AFTER the `analysisStore.migrate()` call
    /// Both pieces matter — a regression that moves the post above
    /// `migrate()` would technically still post but the consumer would
    /// re-read an unopened store and short-circuit empty (the very
    /// race playhead-6boz introduced and this commit closes).
    func testPostMigrateActivityRefreshIsWired() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )

        guard let migrateRange = source.range(of: "try await analysisStore.migrate()") else {
            XCTFail(
                "Could not locate `try await analysisStore.migrate()` in PlayheadRuntime.swift — " +
                "either the call moved or the canary anchor needs updating."
            )
            return
        }

        guard let postRange = source.range(
            of: "AnalysisWorkScheduler.postActivityRefreshNotification()",
            range: migrateRange.upperBound..<source.endIndex
        ) else {
            XCTFail(
                """
                `AnalysisWorkScheduler.postActivityRefreshNotification()` is missing \
                AFTER `try await analysisStore.migrate()` in PlayheadRuntime.swift. \
                The Activity tab depends on this post to repopulate when its first \
                refresh raced the cold-launch lazy AnalysisStore open (playhead-6boz). \
                Removing the post leaves the Activity view stuck on an empty state \
                until the user manually pulls to refresh.
                """
            )
            return
        }

        XCTAssertLessThan(
            migrateRange.upperBound, postRange.lowerBound,
            "ActivityRefreshNotification post must follow migrate() — found post at offset \(source.distance(from: source.startIndex, to: postRange.lowerBound)), migrate at offset \(source.distance(from: source.startIndex, to: migrateRange.lowerBound))."
        )
    }

    /// All six lazy-init loggers (FoundationModelsFeedbackStore,
    /// SurfaceStatusInvariantLogger, DecisionLogger, AssetLifecycleLogger,
    /// BGTaskTelemetryLogger — playhead-jncn audit items #4/#8/#10/#15/#17 —
    /// plus TranscriptShadowGateLogger added by playhead-b58j.3)
    /// must have their `migrate()` invoked from `PlayheadRuntime.swift`'s
    /// deferred init Task. The per-component laziness canaries pin the
    /// init bodies as empty; this canary pins that the deferred work is
    /// actually scheduled.
    ///
    /// Anchors are intentionally fragment-shaped (`.migrate()` qualified
    /// by the binding name) so a future refactor that renames the
    /// binding has to also update the canary.
    func testSixLazyLoggersHaveMigrateCalls() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )

        // Each tuple is (forgiving-anchor-substring, human-readable-name).
        // The substrings are chosen to survive both the current call
        // shape (`await store.migrate()`, `await surfaceStatusLogger.migrate()`,
        // `try await decisionLogger.migrate()`, etc.) and any minor
        // formatting drift, while still being specific enough to fail
        // loudly on a missing call.
        // FoundationModelsFeedbackStore is reached through an
        // explicitly-typed `optionalFeedbackStore` binding because the
        // store is non-optional in DEBUG and `nil` in release. Anchoring
        // on the binding name (rather than `store.migrate()`) avoids
        // false-positives from `analysisStore.migrate()` which is a
        // different call earlier in the same Task.
        let expected: [(needle: String, name: String)] = [
            ("optionalFeedbackStore",            "FoundationModelsFeedbackStore"),
            ("surfaceStatusLogger.migrate()",    "SurfaceStatusInvariantLogger"),
            ("decisionLogger.migrate()",         "DecisionLogger"),
            ("assetLifecycleLogger.migrate()",   "AssetLifecycleLogger"),
            ("bgLogger.migrate()",               "BGTaskTelemetryLogger"),
            ("shadowGateLogger.migrate()",       "TranscriptShadowGateLogger"),
        ]

        for entry in expected {
            XCTAssertTrue(
                source.contains(entry.needle),
                """
                PlayheadRuntime.swift no longer contains `\(entry.needle)` — the deferred \
                migrate call for \(entry.name) appears to have been dropped. The init \
                body is verified empty by PlayheadRuntimeLoggerLazinessTests; without a \
                call-site `migrate()`, the I/O silently shifts to first-record-write \
                and any first event after launch wedges on a cold disk path. Re-add \
                the await migrate() call inside the deferred init Task or update this \
                canary if the binding name was intentionally renamed.
                """
            )
        }
    }

    /// `TranscriptShadowGateLogger()` must be constructed only inside a
    /// `#if DEBUG` arm of `PlayheadRuntime.swift` (playhead-b58j.3).
    /// Release builds substitute `NoOpTranscriptShadowGateLogger()` at
    /// the `AnalysisJobRunner(...)` construction site so shipping
    /// binaries do zero shadow-gate disk I/O — the on-device legal
    /// mandate forbids any non-DEBUG path that could materialise a
    /// shadow JSONL on a user's device.
    ///
    /// The canary asserts that:
    ///   • the literal `try TranscriptShadowGateLogger()` appears in
    ///     `PlayheadRuntime.swift`
    ///   • the literal sits inside a `#if DEBUG` … `#else` window —
    ///     specifically: the nearest enclosing `#if DEBUG` precedes the
    ///     literal, and that *same* arm's matching `#else` (or
    ///     `#elseif`) appears AFTER the literal but BEFORE the arm
    ///     closes (i.e. before the enclosing depth pops).
    ///
    /// A regression that hoists the construction outside the DEBUG arm
    /// — or replaces the `#if DEBUG` with a non-DEBUG flag — fails this
    /// test and blocks the merge before the file-system code reaches a
    /// shipping build.
    func testShadowGateLoggerIsConstructedInDebug() throws {
        let rawSource = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )

        // Run all searches and the directive walk against a comment- and
        // string-stripped projection of the file. Without this, a future
        // doc comment that quotes `try TranscriptShadowGateLogger()` (or
        // a log-line string that prints it) could become the canary's
        // anchor and lock the assertions onto comment text instead of
        // real code. `strippingCommentsAndStrings(_:)` preserves newlines
        // so line indices line up with the raw source.
        let source = SwiftSourceInspector.strippingCommentsAndStrings(rawSource)

        let literal = "try TranscriptShadowGateLogger()"
        guard let literalRange = source.range(of: literal) else {
            XCTFail(
                """
                PlayheadRuntime.swift no longer contains `\(literal)` — the DEBUG-only \
                construction added by playhead-b58j.3 appears to have been removed or \
                renamed. Without it the shadow-gate logger is never wired into the \
                AnalysisJobRunner, so DEBUG builds silently lose the JSONL sink. \
                Re-add the construction inside the existing `#if DEBUG` arm or update \
                this canary if the binding was intentionally renamed.
                """
            )
            return
        }

        // Walk preprocessor directives line-by-line up to (and including)
        // the line containing the literal. The literal is correctly DEBUG-
        // guarded iff the active branch stack contains at least one frame
        // and EVERY frame is currently in its `#if DEBUG` arm. Using the
        // same state-machine shape as
        // `CorpusExporterSourceCanaryTests.settingsCallSiteGuarded` so a
        // reader recognises the pattern.
        let lines = source.split(separator: "\n", omittingEmptySubsequences: false)
        let literalOffset = source.distance(from: source.startIndex, to: literalRange.lowerBound)
        // Translate the literal's character offset to a line index by
        // scanning prefix lengths.
        var literalLineIndex = 0
        var consumed = 0
        var foundLiteralLine = false
        for (idx, line) in lines.enumerated() {
            // +1 accounts for the '\n' that `split` consumed between lines.
            let lineLen = line.count + 1
            if consumed + lineLen > literalOffset {
                literalLineIndex = idx
                foundLiteralLine = true
                break
            }
            consumed += lineLen
        }
        if !foundLiteralLine {
            // The literal lives in the final line of the file (after the
            // last `\n`, or if the file has no trailing newline). Without
            // this fallback the index defaults to 0, which would silently
            // mis-anchor the directive walk.
            literalLineIndex = max(0, lines.count - 1)
        }

        // Stack of "active branch is the DEBUG arm of an `#if DEBUG`?".
        // Push true on `#if DEBUG` (strict `==` is intentional — any
        // compound expression like `#if DEBUG || FOO` risks accidentally
        // relaxing the gate; require an explicit canary update if the
        // gate intentionally widens). Push false on any other `#if`.
        // Flip the top of the stack to false on `#elseif`/`#else` (those
        // arms are by definition NOT the DEBUG branch). Pop on `#endif`.
        var stack: [Bool] = []
        // After we have processed the line containing the literal we
        // capture whether the literal was inside a DEBUG arm AND that
        // its enclosing `#if DEBUG` had not yet been closed by an
        // `#else` or `#endif` at the literal's line.
        var inDebugAtLiteral = false
        var sawElseAfterLiteral = false
        var literalDebugDepthAtSighting = 0
        // Once the enclosing arm of the literal closes (its depth pops
        // below the depth we captured at the sighting), any further
        // `#else`/`#elseif` at that depth belongs to a *different*,
        // unrelated `#if` block and must NOT count as the matching else.
        // Latching this flag lets us ignore those late siblings.
        var enclosingArmClosed = false

        for (idx, line) in lines.enumerated() {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if idx == literalLineIndex {
                // The literal's line itself is plain Swift code (not a
                // preprocessor directive). Capture the active state
                // BEFORE applying any directive on this line.
                inDebugAtLiteral = !stack.isEmpty && stack.allSatisfy { $0 }
                literalDebugDepthAtSighting = stack.count
            }
            if trimmed.hasPrefix("#if ") {
                stack.append(trimmed == "#if DEBUG")
            } else if trimmed.hasPrefix("#elseif ") {
                if !stack.isEmpty {
                    // Only counts as the "matching" else for the
                    // literal's enclosing arm iff (a) we're past the
                    // literal, (b) we're still at the same depth the
                    // literal observed, (c) the enclosing arm has not
                    // already closed (so this isn't an unrelated later
                    // sibling), and (d) the current arm is still the
                    // DEBUG branch.
                    if idx > literalLineIndex,
                       !enclosingArmClosed,
                       stack.count == literalDebugDepthAtSighting,
                       stack.last == true {
                        sawElseAfterLiteral = true
                    }
                    stack[stack.count - 1] = false
                }
            } else if trimmed.hasPrefix("#else") {
                if !stack.isEmpty {
                    if idx > literalLineIndex,
                       !enclosingArmClosed,
                       stack.count == literalDebugDepthAtSighting,
                       stack.last == true {
                        sawElseAfterLiteral = true
                    }
                    stack[stack.count - 1] = false
                }
            } else if trimmed.hasPrefix("#endif") {
                if !stack.isEmpty { stack.removeLast() }
                // If the pop drops us below the depth captured at the
                // literal sighting AND we're past the literal, the
                // enclosing arm has officially closed. Any subsequent
                // `#else`/`#elseif` at that depth belongs to a new,
                // unrelated block and must be ignored.
                if idx > literalLineIndex,
                   stack.count < literalDebugDepthAtSighting {
                    enclosingArmClosed = true
                }
            }
        }

        XCTAssertTrue(
            inDebugAtLiteral,
            """
            `\(literal)` in PlayheadRuntime.swift is NOT inside an active \
            `#if DEBUG` arm. Release builds must compile zero shadow-gate \
            disk I/O paths; constructing `TranscriptShadowGateLogger()` \
            outside `#if DEBUG` ships file-system code in shipping binaries \
            and breaks the on-device legal mandate. Wrap the construction \
            back inside the `#if DEBUG ... #else preBuiltShadowGateLogger = nil #endif` \
            block or update this canary if the guard intentionally moved.
            """
        )

        XCTAssertTrue(
            sawElseAfterLiteral,
            """
            `\(literal)` in PlayheadRuntime.swift is not followed by a \
            matching `#else` (or `#elseif`) at the same nesting depth — \
            the DEBUG arm appears to have no release-side fallback. \
            playhead-b58j.3 requires `#if DEBUG ... try TranscriptShadowGateLogger() ... \
            #else preBuiltShadowGateLogger = nil #endif`; without the \
            `#else`, release builds either fail to compile or accidentally \
            reuse the DEBUG construction. Restore the `#else` arm or \
            update this canary if the guard intentionally changed shape.
            """
        )
    }
}
