// TestRuntimeTeardownCanaryTests.swift
// playhead-882eg: every test-side `PlayheadRuntime` must be torn down.
//
// WHAT THIS EXISTS TO STOP, measured rather than argued. A `PlayheadRuntime`
// starts four perpetual loops — `AnalysisWorkScheduler.schedulerTask`,
// `AnalysisCoordinator.capabilityObserverTask`,
// `BackgroundProcessingService.capabilityObserverTask` and
// `EpisodeSummaryBackfillCoordinator.loopTask` — each shaped
//
//     someTask = Task { [weak self] in
//         guard let self else { return }   // strong for the task's WHOLE life
//         await self.runLoop()             // never returns
//     }
//
// so each is a retain cycle that only cancellation can break. `shutdown()` is
// the only thing that cancels them. A test that constructs a runtime and does
// not shut it down therefore leaks that runtime's `AnalysisStore`,
// `AdCatalogStore` and `SurfaceStatusInvariantLogger` — about five file
// descriptors — for the rest of the process.
//
// That is not hypothetical: it is the ~453-descriptor FLOOR the test host
// carried into every full-plan run, 17.7 % of `RLIMIT_NOFILE` soft 2560 gone
// before the next plan opened a single store, on a host that reaches 93-99 % of
// that limit and has lost its host at the ceiling (playhead-vk68m,
// playhead-s34ux). The floor is a FUNCTION OF THE TEST SUITE — five descriptors
// per un-torn-down runtime — so it grows with every test anyone adds.
//
// THE RULE. `PlayheadRuntime(` may be written in exactly two kinds of place:
//
//   * `PlayheadTests/Helpers/WithTestRuntime.swift`, the one helper that
//     guarantees `shutdown()` on both the success and the throwing path; or
//   * a file on `lifecycleSubjectFiles` below — a file whose SUBJECT is the
//     runtime's own construction or teardown, and which therefore cannot use
//     the helper without testing the helper instead. Every one of those must
//     still contain a `.shutdown()` call, which is checked here: an allowlist
//     that does not have to demonstrate teardown is an amnesty, not a licence.
//
// The list is CLOSED IN BOTH DIRECTIONS. A file that constructs a runtime and
// is not listed fails; a listed file that no longer exists, or that no longer
// constructs a runtime, also fails — a licence for a file nobody can find was
// renamed or moved, and whatever inherits the name inherits the amnesty. Same
// rule `scripts/singleton-slot-allowlist.json` applies for the same reason.

import XCTest

final class TestRuntimeTeardownCanaryTests: XCTestCase {

    /// Files permitted to construct a `PlayheadRuntime` directly, with the
    /// reason each one cannot route through `withTestRuntime`.
    private static let lifecycleSubjectFiles: [String: String] = [
        // The helper itself.
        "WithTestRuntime.swift":
            "IS the helper — this is the one construction site the rule exists to funnel into",
        "RuntimeShutdownLifecycleTests.swift":
            "its subject IS shutdown()/deinit ordering; using the helper would test the helper",
        "PlayheadRuntimeLaunchPerfTests.swift":
            "measures the cost of init itself, so the construction must be the timed statement",
        "MainActorFreedomTests.swift":
            "probes the main actor DURING init, so the construction must be the observed statement",
        "RuntimeStoreTeardownTests.swift":
            "its subject IS what shutdown() closes, including the arm that must NOT shut down first",
    ]

    /// Assembled rather than written whole so that THIS file does not contain
    /// the token it scans for. The alternative — allowlisting the canary — would
    /// hand the scanner an entry it can never justify.
    private static let constructionToken = "PlayheadRuntime" + "("

    func testEveryTestSideRuntimeConstructionIsTornDown() throws {
        let testsRoot = try Self.testSourceRoot()
        let fileManager = FileManager.default
        guard let enumerator = fileManager.enumerator(
            at: testsRoot,
            includingPropertiesForKeys: nil
        ) else {
            XCTFail("could not enumerate \(testsRoot.path)")
            return
        }

        var offenders: [String] = []
        var seenAllowlisted: Set<String> = []
        var allowlistedWithoutShutdown: [String] = []

        for case let url as URL in enumerator where url.pathExtension == "swift" {
            let name = url.lastPathComponent
            let source = try String(contentsOf: url, encoding: .utf8)
            let stripped = Self.strippingComments(source)
            guard stripped.contains(Self.constructionToken) else { continue }
            guard Self.lifecycleSubjectFiles[name] != nil else {
                offenders.append(name)
                continue
            }
            seenAllowlisted.insert(name)
            if !stripped.contains(".shutdown()") {
                allowlistedWithoutShutdown.append(name)
            }
        }

        XCTAssertTrue(
            offenders.isEmpty,
            """
            These test files construct a PlayheadRuntime directly: \
            \(offenders.sorted().joined(separator: ", ")).
            Route them through `withTestRuntime { runtime in ... }`, which calls \
            `shutdown()` on both the success and the throwing path. A runtime that \
            is never shut down keeps four perpetual loops alive, and with them its \
            AnalysisStore, AdCatalogStore and SurfaceStatusInvariantLogger — about \
            five file descriptors each, permanently, on a test host that already \
            reaches 93-99 % of RLIMIT_NOFILE. See playhead-882eg.
            """
        )

        XCTAssertTrue(
            allowlistedWithoutShutdown.isEmpty,
            """
            These files are allowed to construct a runtime directly but contain no \
            `.shutdown()` call: \(allowlistedWithoutShutdown.sorted().joined(separator: ", ")). \
            The allowlist is a licence to construct, not a licence to leak.
            """
        )

        let unmatched = Set(Self.lifecycleSubjectFiles.keys).subtracting(seenAllowlisted)
        XCTAssertTrue(
            unmatched.isEmpty,
            """
            These allowlist entries matched no file that constructs a PlayheadRuntime: \
            \(unmatched.sorted().joined(separator: ", ")). Either the file was renamed \
            or moved — in which case whatever inherits the name inherits the amnesty — \
            or it no longer needs the licence and the entry should go. The list is \
            closed in both directions on purpose.
            """
        )
    }

    /// Removes `//` and `/* */` comments. String literals are deliberately NOT
    /// stripped: a correct string-literal scanner has to handle Swift's
    /// multi-line `"""` form, whose quote parity is not what a character-at-a-time
    /// toggle reads it as, and a stripper that gets that wrong SWALLOWS code —
    /// which would make this canary quietly stop seeing real construction sites.
    /// The cost of not stripping them is a false POSITIVE if a test ever puts the
    /// token in a string, and a false positive here is loud and one line to fix.
    /// This file therefore never writes the token followed by `(` outside a
    /// comment.
    static func strippingComments(_ source: String) -> String {
        var out = ""
        var index = source.startIndex
        enum State { case code, lineComment, blockComment }
        var state = State.code
        while index < source.endIndex {
            let ch = source[index]
            let next = source.index(after: index)
            let peek: Character? = next < source.endIndex ? source[next] : nil
            switch state {
            case .code:
                if ch == "/", peek == "/" { state = .lineComment; index = next; break }
                if ch == "/", peek == "*" { state = .blockComment; index = next; break }
                out.append(ch)
            case .lineComment:
                if ch == "\n" { state = .code; out.append(ch) }
            case .blockComment:
                if ch == "*", peek == "/" { state = .code; index = next }
            }
            index = source.index(after: index)
        }
        return out
    }

    /// `PlayheadTests/` resolved from this file's own compile-time path.
    private static func testSourceRoot(file: StaticString = #filePath) throws -> URL {
        var url = URL(fileURLWithPath: "\(file)")
        while url.lastPathComponent != "PlayheadTests" {
            let parent = url.deletingLastPathComponent()
            guard parent.path != url.path else {
                throw NSError(domain: "TestRuntimeTeardownCanary", code: 1, userInfo: [
                    NSLocalizedDescriptionKey:
                        "could not find PlayheadTests/ above \(file)"
                ])
            }
            url = parent
        }
        return url
    }
}
