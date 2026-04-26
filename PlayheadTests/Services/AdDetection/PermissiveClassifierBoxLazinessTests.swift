// PermissiveClassifierBoxLazinessTests.swift
// playhead-jndk: regression rails for the lazy `PermissiveClassifierBox`
// and a source-level canary that pins the laziness wiring at the call
// site in `PlayheadRuntime.init`.
//
// Why this exists: on iOS 26, `PermissiveAdClassifier()` synchronously
// builds a `SystemLanguageModel(guardrails:)`. That call probes the
// FoundationModels framework and the on-device model availability —
// minutes of main-thread blocking on a real device snapshot from
// 2026-04-25. Constructing the classifier from inside `PlayheadRuntime
// .init` extends the launch-storyboard window (the period BEFORE the
// SwiftUI `RootView` even runs), and the splash defense added in
// playhead-5nwy can't reach back that far.
//
// Two rails:
//   1. Behavior: a `PermissiveClassifierBox` built from a factory must
//      NOT invoke the factory until something asks for the classifier.
//      First-use must invoke exactly once; subsequent calls return the
//      same cached instance.
//   2. Source canary: `PlayheadRuntime.init` MUST NOT contain a direct
//      `PermissiveAdClassifier()` constructor call inside its body. The
//      only allowed mention is the factory closure passed to the box.
//      Mirrors the `Task.sleep`-absence canary added by playhead-5nwy
//      (commit c595f02) for `SplashController`.

import Foundation
import Testing
import XCTest
@testable import Playhead

// MARK: - Behavior rail (Swift Testing)

@Suite("PermissiveClassifierBox laziness (playhead-jndk)")
struct PermissiveClassifierBoxLazinessTests {

    @available(iOS 26.0, *)
    @Test("Box constructed from a factory does NOT invoke the factory at init time")
    func factoryNotInvokedAtInit() async {
        let invocationCount = FactoryInvocationCounter()
        _ = BackfillJobRunner.PermissiveClassifierBox {
            invocationCount.increment()
            return PermissiveAdClassifier()
        }

        // The box was created above; the factory MUST NOT have run yet.
        #expect(invocationCount.value == 0)
    }

    @available(iOS 26.0, *)
    @Test("Box invokes factory exactly once across repeated `classifier` accesses")
    func factoryInvokedExactlyOnce() async {
        let invocationCount = FactoryInvocationCounter()
        let box = BackfillJobRunner.PermissiveClassifierBox {
            invocationCount.increment()
            return PermissiveAdClassifier()
        }

        let first = box.classifier
        let second = box.classifier
        let third = box.classifier

        #expect(invocationCount.value == 1)
        // Same actor instance across repeated accesses (cached).
        #expect(first === second)
        #expect(second === third)
    }

    @available(iOS 26.0, *)
    @Test("Concurrent first-use accesses race-free: factory runs once")
    func concurrentFirstUseRunsFactoryOnce() async {
        let invocationCount = FactoryInvocationCounter()
        let box = BackfillJobRunner.PermissiveClassifierBox {
            invocationCount.increment()
            return PermissiveAdClassifier()
        }

        // Spin up 32 concurrent tasks all racing the first-use lock.
        // The lock around the cache must serialize them so the factory
        // runs exactly once. Without the lock, multiple racers each
        // see `cache == nil` and the factory runs N times.
        await withTaskGroup(of: ObjectIdentifier.self) { group in
            for _ in 0..<32 {
                group.addTask {
                    return ObjectIdentifier(box.classifier)
                }
            }
            var ids: Set<ObjectIdentifier> = []
            for await id in group {
                ids.insert(id)
            }
            // Every racer must observe the SAME cached instance.
            #expect(ids.count == 1)
        }

        #expect(invocationCount.value == 1)
    }
}

/// Threadsafe counter used by the laziness tests above. `OSAllocatedUnfairLock`
/// is overkill for a couple of increments but pins the test against a
/// future move to a stricter concurrency model.
private final class FactoryInvocationCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: Int = 0
    var value: Int {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
    func increment() {
        lock.lock(); defer { lock.unlock() }
        _value += 1
    }
}

// MARK: - Source canary (XCTest)
//
// XCTest, NOT Swift Testing, because the source-canary style in
// `AppNavigationStructureTests.testSplashControllerUsesMainRunloopTimerNotTask`
// (added by playhead-5nwy commit c595f02) is XCTest. Matching the prior
// style keeps the canary discoverable next to its sibling.

final class PlayheadRuntimeInitLazyClassifierSourceCanaryTests: XCTestCase {

    /// Source-level canary: `PlayheadRuntime.init` MUST NOT contain a
    /// direct `PermissiveAdClassifier()` constructor call inside its
    /// body. The only allowed surface is the factory closure passed to
    /// `PermissiveClassifierBox { PermissiveAdClassifier() }` — that
    /// closure does NOT execute until first `classifier` access from
    /// inside the detection pipeline (off-main, post-launch).
    func testInitBodyDoesNotEagerlyConstructPermissiveClassifier() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )

        // Locate init's brace-delimited body.
        guard let body = SwiftSourceInspector.firstBody(
            in: source,
            after: "init(isPreviewRuntime: Bool = false) {"
        ) else {
            XCTFail("PlayheadRuntime.init(isPreviewRuntime:) signature not found — test must be updated alongside any rename.")
            return
        }

        // Allowed: `PermissiveClassifierBox { PermissiveAdClassifier() }`
        //          (factory closure — only fires lazily).
        // Disallowed: any OTHER occurrence of `PermissiveAdClassifier()`,
        //             which would be an eager construction inside init.
        //
        // Strategy: count total `PermissiveAdClassifier()` occurrences
        // and count those that appear inside a `PermissiveClassifierBox {`
        // closure literal. The two counts must match — i.e. every
        // mention is wrapped by the lazy factory.
        //
        // The lazy-wrapped count uses a whitespace-tolerant regex so a
        // future swift-format reflow that breaks the closure across
        // lines (e.g. `PermissiveClassifierBox {\n    PermissiveAdClassifier()\n}`)
        // still matches and doesn't cause a spurious failure here.
        //
        // Strip comments before counting: `bracedBody` returns the raw
        // source slice including any comments inside init. The init
        // currently carries multi-line audit comments that name
        // `PermissiveAdClassifier()` (jndk discussion of the lazy
        // wrapping) — a naïve grep on the raw body false-positives on
        // those comments. Mirrors the pattern adopted in the
        // launch-perf source canaries (PlayheadRuntimeLaunchPerfTests).
        let scrubbed = SwiftSourceInspector.strippingComments(body)
        let totalCalls = SwiftSourceInspector.occurrences(of: "PermissiveAdClassifier()", in: scrubbed)
        let lazyWrappedPattern = #"PermissiveClassifierBox\s*\{\s*PermissiveAdClassifier\s*\(\s*\)\s*\}"#
        let lazyWrapped = SwiftSourceInspector.regexOccurrences(of: lazyWrappedPattern, in: scrubbed)

        XCTAssertEqual(
            totalCalls, lazyWrapped,
            """
            PlayheadRuntime.init body contains \(totalCalls) `PermissiveAdClassifier()` \
            constructor call(s) but only \(lazyWrapped) are wrapped in a \
            `PermissiveClassifierBox { ... }` factory closure. Eager construction \
            inside init is the launch-freeze hazard playhead-jndk fixes — wrap any \
            new mention in the factory closure, or delete the mention entirely.
            """
        )
    }
}
