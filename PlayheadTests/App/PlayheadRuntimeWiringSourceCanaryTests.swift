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
//   2. The five `await <logger>.migrate()` calls in the deferred init
//      Task. The per-logger laziness canaries
//      (PlayheadRuntimeLoggerLazinessTests) confirm each logger's init
//      body is empty of disk I/O, but they do NOT confirm
//      `PlayheadRuntime` actually invokes the deferred `migrate()` from
//      its Task. A regression that drops the call would silently move
//      the I/O to first-record-write — observable as a wedge on the
//      first decision/asset/BG-task event after launch.
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

    /// All five lazy-init loggers (FoundationModelsFeedbackStore,
    /// SurfaceStatusInvariantLogger, DecisionLogger, AssetLifecycleLogger,
    /// BGTaskTelemetryLogger — playhead-jncn audit items #4/#8/#10/#15/#17)
    /// must have their `migrate()` invoked from `PlayheadRuntime.swift`'s
    /// deferred init Task. The per-component laziness canaries pin the
    /// init bodies as empty; this canary pins that the deferred work is
    /// actually scheduled.
    ///
    /// Anchors are intentionally fragment-shaped (`.migrate()` qualified
    /// by the binding name) so a future refactor that renames the
    /// binding has to also update the canary.
    func testFiveLazyLoggersHaveMigrateCalls() throws {
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
}
