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

    /// skeptical-review-cycle-6 H-Z1 regression canary: the launch
    /// sweep loop in `runFinalPassBackfillForAllAssetsAtLaunch` must
    /// honor cooperative cancellation BETWEEN assets. The cycle-5 H-Y1
    /// fix tracked the detached sweep task and cancelled it from
    /// `shutdown()`, but the cycle-6 review caught that the per-asset
    /// catch was a generic `catch error` that swallowed
    /// `CancellationError` rethrown by `runner.runFinalPassBackfill` —
    /// so after `shutdown()`, the loop kept marching through every
    /// remaining asset and re-issuing FM/ASR work on each one.
    ///
    /// The fix has two complementary safety nets:
    ///   1. `Task.isCancelled` check at the top of each iteration so a
    ///      cancellation that lands BEFORE the runner is invoked stops
    ///      the sweep without paying the cost of any further DB or
    ///      download lookups.
    ///   2. `catch is CancellationError { break }` BEFORE the generic
    ///      `catch error` arm so a cancellation surfaced THROUGH the
    ///      runner stops the loop instead of being logged as a
    ///      per-asset failure.
    ///
    /// Both arms must remain. A regression that drops either one
    /// re-introduces the H-Z1 leak: `shutdown()` returns immediately,
    /// but the launch sweep keeps issuing per-asset FM/ASR jobs the
    /// user no longer needs. We assert both arms appear in the body
    /// of `runFinalPassBackfillForAllAssetsAtLaunch` specifically (not
    /// just anywhere in the file) so a regression that deletes the
    /// loop's cancellation handling but leaves cancellation tokens
    /// elsewhere in the file still trips the canary.
    func testLaunchSweepHonorsCancellationBetweenAssets() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )

        let signature = "static func runFinalPassBackfillForAllAssetsAtLaunch("
        guard let body = SwiftSourceInspector.firstBody(in: source, after: signature) else {
            XCTFail(
                """
                Could not locate the body of `\(signature)` in \
                PlayheadRuntime.swift — either the function moved/renamed \
                or its signature shape drifted. Update the canary anchor.
                """
            )
            return
        }

        XCTAssertTrue(
            body.contains("if Task.isCancelled { break }"),
            """
            `runFinalPassBackfillForAllAssetsAtLaunch` body no longer \
            contains the `if Task.isCancelled { break }` check at the \
            top of the per-asset loop. Cycle-6 H-Z1: shutdown() \
            cancels the launch-sweep task, but without this top-of-loop \
            check the loop keeps marching through remaining assets and \
            issuing fresh FM/ASR work the user no longer needs. Re-add \
            the check OR update this canary if the cancellation gate \
            intentionally moved.
            """
        )

        XCTAssertTrue(
            body.contains("catch is CancellationError"),
            """
            `runFinalPassBackfillForAllAssetsAtLaunch` body no longer \
            contains a `catch is CancellationError` clause. Cycle-6 \
            H-Z1: cycle-5 made `runner.runFinalPassBackfill` cancellation- \
            aware, but the loop's only catch was a generic `catch error` \
            that logged and continued — silently swallowing the very \
            cancellation that shutdown() relies on to stop the sweep. \
            Restore the typed catch BEFORE the generic catch OR update \
            this canary if the cancellation handling intentionally \
            changed shape.
            """
        )

        // Defense-in-depth: the typed catch must appear BEFORE the
        // generic catch arm of the per-asset do/catch. We anchor on
        // the unique log line in the generic arm rather than the
        // bare `catch {` token, because the function body has an
        // earlier do/catch around `analysisStore.fetchAllAssets()`
        // whose `catch {` would otherwise be matched first and
        // produce a false "typed catch comes after generic catch"
        // failure.
        if let typedRange = body.range(of: "catch is CancellationError"),
           let genericRange = body.range(of: "log.warning(\"Run failed for") {
            XCTAssertLessThan(
                typedRange.lowerBound, genericRange.lowerBound,
                """
                `catch is CancellationError` appears AFTER the generic \
                catch clause that logs `"Run failed for ..."` in the \
                per-asset loop. Swift evaluates catch clauses in source \
                order, so the typed catch is unreachable — every \
                CancellationError is consumed by the generic arm and \
                the loop continues. Reorder so the typed catch precedes \
                the generic catch.
                """
            )
        } else {
            XCTFail(
                """
                Could not locate either `catch is CancellationError` or \
                the per-asset generic catch's `log.warning("Run failed for …` \
                in the body of `runFinalPassBackfillForAllAssetsAtLaunch`. \
                Update the canary anchors if either was renamed.
                """
            )
        }
    }

    /// skeptical-review-cycle-12 T-4: pin the two
    /// `MainActor.assertIsolated()` calls inserted by cycles 9-11 inside
    /// the bare bootstrap `Task { ... }` bodies in `PlayheadRuntime`'s
    /// init. Both Tasks rely on `@MainActor` isolation inherited via
    /// SE-0420 from the `@MainActor`-declared `PlayheadRuntime` class:
    ///   • the launch-sweep installer Task (cycle-8 M1's
    ///     `if !isShutdown { ... }` atomicity) — first MainActor.assertIsolated()
    ///   • the playback-state observer Task (`lastForwardedContext`,
    ///     `lastStatus`, `lastSpeed` mutated across `await` boundaries
    ///     under single-mutator-via-suspension) — second MainActor.assertIsolated()
    ///
    /// A future refactor that strips inheritance (swap to `Task.detached`,
    /// add `@Sendable` that drops actor capture, hoist the body into a
    /// free function, or annotate the surrounding Task with a different
    /// global actor) silently lapses the isolation contract. The runtime
    /// assertion itself catches that on debug builds, but only if it
    /// remains in place; this canary catches a regression that DELETES
    /// the assertion or moves the surrounding Task body out from under
    /// the inheritance.
    ///
    /// What we pin:
    ///   1. At least two `MainActor.assertIsolated()` calls in the file.
    ///   2. Each one appears inside a bare `Task { [...] in ... }` body
    ///      (no `@MainActor`, no `@Sendable`, no `Task.detached`,
    ///      no `Task<...>` type-spec) — i.e. the body actually inherits
    ///      MainActor isolation rather than acquiring it explicitly.
    ///
    /// (2) is the load-bearing half — a refactor that flips
    /// `Task { ... }` to `Task.detached { ... }` would still let the
    /// assertion compile and trip at runtime, but ONLY because the
    /// detached Task hops onto a non-main executor and fails the
    /// assertion. That's the right failure mode in DEBUG, but a
    /// stronger source-level pin guards release builds (where the
    /// assertion is `#if DEBUG`-stripped) too.
    /// cycle-1 H2 regression canary: the `setSkipCueHandler` closure
    /// in `PlayheadRuntime` must serialize the skip-range fan-out into
    /// PlaybackService FIRST and SilenceCompressionCoordinator SECOND,
    /// inside a SINGLE Task body. The prior shape spawned two
    /// independent unstructured Tasks (one to `playbackService.setSkipCues`,
    /// one to `silenceCompressionCoordinator.updateSkipRanges`), making
    /// the relative order between the two hops non-deterministic.
    /// The coordinator's planner reads PlaybackService's marked-skip
    /// view on its next refresh; if the coordinator updates first and
    /// the playback service updates second, the planner can refuse
    /// compression on a region the skip path no longer claims (a brief
    /// window of inverted state).
    ///
    /// What we pin:
    ///   1. Inside the `setSkipCueHandler` closure body, both
    ///      `playbackService.setSkipCues(` and
    ///      `silenceCompressionCoordinator.updateSkipRanges(` appear.
    ///   2. They appear IN THAT ORDER (setSkipCues precedes updateSkipRanges).
    ///   3. Between them, there is exactly ONE `Task` opener — i.e. they
    ///      live in the same Task body. A regression that splits them
    ///      back into two separate `Task { ... }` blocks fails (3).
    func testSkipCueFanOutIsSerialized() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)

        guard let handlerRange = stripped.range(of: "setSkipCueHandler") else {
            XCTFail(
                """
                `setSkipCueHandler` no longer appears in PlayheadRuntime.swift. \
                The skip-cue fan-out hook has moved or been removed; update \
                this canary anchor.
                """
            )
            return
        }

        // Search a bounded window starting from the handler call so we
        // don't accidentally pick up unrelated `setSkipCues` calls
        // elsewhere in the file.
        let searchStart = handlerRange.upperBound
        // Window of ~3000 chars is enough to span the whole closure body
        // (the prior shape was ~25 lines) without bleeding into siblings.
        let windowEnd = stripped.index(
            searchStart,
            offsetBy: min(3000, stripped.distance(from: searchStart, to: stripped.endIndex))
        )
        let window = stripped[searchStart..<windowEnd]

        guard let setSkipCuesRange = window.range(of: "playbackService.setSkipCues(") else {
            XCTFail(
                """
                Inside `setSkipCueHandler` body in PlayheadRuntime.swift, the \
                call `playbackService.setSkipCues(` is missing. Either the \
                fan-out has been removed or the binding name has changed. \
                Update the canary.
                """
            )
            return
        }
        guard let updateSkipRangesRange = window.range(of: "silenceCompressionCoordinator.updateSkipRanges(") else {
            XCTFail(
                """
                Inside `setSkipCueHandler` body in PlayheadRuntime.swift, the \
                call `silenceCompressionCoordinator.updateSkipRanges(` is missing. \
                playhead-epii requires the silence-compression coordinator to \
                receive cue updates. Update the canary or restore the call.
                """
            )
            return
        }

        XCTAssertLessThan(
            setSkipCuesRange.lowerBound, updateSkipRangesRange.lowerBound,
            """
            cycle-1 H2: `playbackService.setSkipCues(...)` must be invoked \
            BEFORE `silenceCompressionCoordinator.updateSkipRanges(...)` \
            inside the `setSkipCueHandler` closure. The coordinator's \
            planner reads PlaybackService's marked-skip view on its \
            next refresh; updating the coordinator first leaves a \
            brief window of inverted state. Reorder the two calls.
            """
        )

        // Count Task openers between the two calls. A single Task body
        // shared by both calls means there should be ZERO Task openers
        // strictly between them. (The Task opener is BEFORE setSkipCues
        // in the new shape.)
        let between = window[setSkipCuesRange.upperBound..<updateSkipRangesRange.lowerBound]
        let taskOpenerRegex = try NSRegularExpression(
            pattern: #"(?<![A-Za-z0-9_.])Task\s*\{"#
        )
        let nsBetween = String(between)
        let matches = taskOpenerRegex.matches(
            in: nsBetween,
            range: NSRange(nsBetween.startIndex..., in: nsBetween)
        )
        XCTAssertEqual(
            matches.count, 0,
            """
            cycle-1 H2: between `playbackService.setSkipCues(...)` and \
            `silenceCompressionCoordinator.updateSkipRanges(...)` inside \
            the `setSkipCueHandler` closure, found \(matches.count) `Task` \
            opener(s). The prior buggy shape spawned two independent \
            unstructured Tasks — one for each call — making the relative \
            order non-deterministic. The fix is to put both calls inside \
            ONE Task with sequential awaits. If the fan-out is still \
            structured but uses a different shape (e.g. a TaskGroup), \
            update this canary; otherwise restore the single-Task ordering.
            """
        )
    }

    func testMainActorAssertIsolatedRemainsInBootstrapTasks() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )
        let stripped = SwiftSourceInspector.strippingComments(source)

        let assertionRegex = try NSRegularExpression(
            pattern: #"MainActor\s*\.\s*assertIsolated\s*\(\s*\)"#
        )
        let fullRange = NSRange(stripped.startIndex..., in: stripped)
        let matches = assertionRegex.matches(in: stripped, range: fullRange)

        XCTAssertGreaterThanOrEqual(
            matches.count, 2,
            """
            Expected at least 2 `MainActor.assertIsolated()` calls in \
            `PlayheadRuntime.swift` (cycle-9 / cycle-10 added one each \
            for the launch-sweep installer Task and the playback-state \
            observer Task). Found \(matches.count). A regression that \
            deletes either assertion silently lapses the cycle-8 M1 \
            atomicity claim or the single-mutator-via-suspension claim \
            on `lastForwardedContext`/`lastStatus`/`lastSpeed`. Restore \
            the assertion(s) OR update this canary if the isolation \
            contract intentionally moved.
            """
        )

        // For each assertion site, find:
        //   • the nearest preceding bare `Task {` opener
        //   • the nearest preceding `Task.detached` opener
        // and require: bare opener is more recent than detached opener.
        //
        // skeptical-review-cycle-13 M-1: the cycle-12 implementation
        // checked only "is there a `Task.detached` near the chosen bare
        // opener?", which was vacuous against its stated threat model:
        // the bare-opener regex `Task\s*\{` cannot match `Task.detached
        // {` (the `.detached` is not whitespace), so a refactor that
        // flips `Task { ... }` at PlayheadRuntime.swift:1156 (or :1529)
        // to `Task.detached { ... }` makes that opener silently
        // disappear from the bare-opener match list, `nearestOpener`
        // falls back to a previous bare sibling (e.g. line 1142 for
        // assertion #1), and a window check around the sibling finds
        // no `.detached`. The test passes while the assertion is
        // actually inside a detached body — exactly the regression we
        // claimed to catch.
        //
        // The fix below scans both the bare openers and the detached
        // openers independently, then asserts the bare opener is the
        // more recent one. A flip-to-detached at the assertion's
        // immediate-enclosing Task makes the detached opener jump past
        // the bare one, tripping the assertion. The bare-Task at line
        // 1495 (`finalPassLaunchSweepTask = Task.detached { ... }`)
        // is currently MORE RECENT than assertion #2's enclosing
        // bare `Task {` opener at 1529 — wait, no, 1495 < 1529, so
        // the bare-at-1529 wins. Confirmed by inspection.
        let taskOpenerRegex = try NSRegularExpression(
            pattern: #"(?<![A-Za-z0-9_.])Task\s*\{"#
        )
        let detachedRegex = try NSRegularExpression(
            pattern: #"Task\s*\.\s*detached"#
        )

        for (i, match) in matches.enumerated() {
            let upToAssertion = NSRange(
                location: 0,
                length: match.range.location
            )
            let bareOpeners = taskOpenerRegex.matches(in: stripped, range: upToAssertion)
            let detachedOpeners = detachedRegex.matches(in: stripped, range: upToAssertion)

            guard let nearestBare = bareOpeners.last else {
                XCTFail(
                    """
                    Could not find a bare `Task {` opener before \
                    `MainActor.assertIsolated()` occurrence #\(i + 1) in \
                    `PlayheadRuntime.swift`. The cycle-9/10/11 \
                    assertions are placed inside bare bootstrap Task \
                    bodies that inherit MainActor isolation via \
                    SE-0420; if the surrounding shape changed (e.g. the \
                    assertion moved to a free function or method, or \
                    every preceding Task was converted to detached), \
                    update this canary.
                    """
                )
                continue
            }

            let nearestBareLoc = nearestBare.range.location
            let nearestDetachedLoc = detachedOpeners.last?.range.location ?? -1

            XCTAssertGreaterThan(
                nearestBareLoc, nearestDetachedLoc,
                """
                `MainActor.assertIsolated()` occurrence #\(i + 1) in \
                `PlayheadRuntime.swift` is preceded by a `Task.detached` \
                opener that is MORE RECENT than any bare `Task {` opener \
                — i.e. the assertion is now inside a detached Task body. \
                Detached Tasks do NOT inherit MainActor isolation from \
                the surrounding `@MainActor` class, so the assertion \
                will trip on a background executor in DEBUG and (if \
                stripped under #if DEBUG) silently let the un-isolated \
                body run in release, dropping the cycle-8 M1 atomicity \
                / single-mutator-via-suspension protection. Restore the \
                bare `Task { ... }` shape OR explicitly annotate the \
                surrounding Task body with `@MainActor` and update this \
                canary.
                """
            )
        }
    }
}
