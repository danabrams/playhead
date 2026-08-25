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
//
// HOW THE SOURCE IS SCANNED, and why it is not a stripper written here. This
// canary first shipped with a hand-rolled comment stripper that deliberately
// did NOT track string literals, on the stated reasoning that "the cost of not
// stripping them is a false POSITIVE if a test ever puts the token in a string,
// and a false positive here is loud and one line to fix". That is wrong in the
// direction that matters, and it was ALREADY WRONG ON THIS TREE. A `/*` inside
// a string literal opened block-comment state, and everything up to the next
// `*/` anywhere in the file — or to end of file if there was no next one — was
// deleted before the token scan. Measured over the 893 Swift files under
// `PlayheadTests/`, two of them put that scanner into a block-comment state it
// never left:
//
//   SponsorEntityGraphTests.swift:165   XCTAssertEqual(shape, "/podcast/*")
//   RediffRefetchTests.swift:720        parseTotalLength("bytes 0-100/*")
//
// costing 352 and 984 lines — 1,336 lines of test source this canary could not
// see at all, in which a runtime construction would have passed SILENTLY. A
// FALSE NEGATIVE, which is the one direction a canary cannot afford, produced
// by the very decision whose comment said it could not happen.
//
// The scan now uses `SwiftSourceInspector.strippingCommentsAndStrings(_:)`,
// which this repo already had, which every other whole-tree canary already
// uses, and which handles all four Swift string forms — `"..."`, `"""..."""`,
// raw `#"..."#` and raw `#"""..."""#` — because it was written for exactly this
// failure (its own header: "the original implementation only understood
// single-quoted literals ... would terminate the literal at the first inner
// `"`, leaking subsequent text into code position"). It also blanks string
// CONTENTS, so the false positive the hand-rolled version accepted as its price
// does not arise either. Re-measured over the same 893 files: the token is
// found in exactly the five allowlisted files, all five carry `.shutdown()`,
// and the length invariant holds everywhere.
//
// Two belts are asserted below rather than assumed, because a whole-tree scan
// is where an unhandled form will actually turn up: the scanner's own
// Character-count invariant, and a CLOSED set of the files that mention the
// token only in prose. The second is the one that matters — a mis-parsed file
// reports its real constructions as "not in code" and would otherwise pass in
// silence, which is precisely the failure described above.

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
        var mentionedOnlyOutsideCode: Set<String> = []
        var lengthInvariantViolations: [String] = []

        for case let url as URL in enumerator where url.pathExtension == "swift" {
            let name = url.lastPathComponent
            let source = try String(contentsOf: url, encoding: .utf8)
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
            if stripped.count != source.count {
                lengthInvariantViolations.append(name)
            }
            guard stripped.contains(Self.constructionToken) else {
                if source.contains(Self.constructionToken) {
                    mentionedOnlyOutsideCode.insert(name)
                }
                continue
            }
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

        // The scanner's LENGTH INVARIANT, which
        // `SwiftSourceInspector.strippingCommentsAndStrings(_:)` documents and
        // its own suite pins: every branch emits exactly one output Character
        // per source Character consumed. A violation means a branch consumed a
        // region without emitting it, which is the shape that would let this
        // canary lose sight of a construction site. Checked here rather than
        // assumed, because a whole-tree scan is where a form nobody thought of
        // will actually turn up.
        XCTAssertTrue(
            lengthInvariantViolations.isEmpty,
            """
            The source scanner broke its own length invariant on: \
            \(lengthInvariantViolations.sorted().joined(separator: ", ")). \
            Its output must have the same Character count as its input; a \
            shorter output means a region was consumed without being emitted, \
            and this canary cannot see what was consumed.
            """
        )

        // CLOSED IN BOTH DIRECTIONS, third list, and the one that keeps the
        // scanner honest. These are the files whose RAW text contains the
        // construction token while their SCANNED text does not — i.e. every
        // mention is inside a comment or a string literal. That is a real and
        // legitimate category (both members below are prose), but it is also
        // exactly what a SWALLOWED region looks like from outside: a file the
        // scanner mis-parsed reports its real constructions as "not in code"
        // and passes silently. Pinning the membership means a third entry
        // appearing is a RED that names the file, rather than a construction
        // site that quietly stopped being scanned.
        let expectedMentionOnly: Set<String> = [
            // Writes the token in its own header prose, and assembles the
            // token it scans for so the assembled form never appears.
            "TestRuntimeTeardownCanaryTests.swift",
            // Names the constructor in a comment; constructs nothing.
            "CoreServiceTests.swift",
        ]
        XCTAssertEqual(
            mentionedOnlyOutsideCode, expectedMentionOnly,
            """
            The set of files that mention `PlayheadRuntime` + `(` ONLY in a \
            comment or string literal has changed. Added: \
            \(mentionedOnlyOutsideCode.subtracting(expectedMentionOnly).sorted().joined(separator: ", ")). \
            Removed: \
            \(expectedMentionOnly.subtracting(mentionedOnlyOutsideCode).sorted().joined(separator: ", ")). \
            An ADDITION is either a new prose mention (add it here) or a file \
            the scanner mis-parsed, in which case a real construction in that \
            file is no longer being seen. A REMOVAL means a prose mention was \
            deleted or became real code. Neither is allowed to happen silently.
            """
        )
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
