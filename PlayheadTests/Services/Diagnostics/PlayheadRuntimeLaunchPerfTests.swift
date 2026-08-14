// PlayheadRuntimeLaunchPerfTests.swift
// playhead-2axy: forever-guard against the launch-storyboard freezes that
// cost two emergency fixes in 24 hours (jndk on 2026-04-25, hkn1 on
// 2026-04-26). The synchronous body of `PlayheadRuntime.init` runs
// BEFORE `RootView` ever resolves, so any expensive work it does
// extends the launch-storyboard window and the splash defense added in
// playhead-5nwy can't reach back that far.
//
// This file ships TWO complementary rails:
//
//   1. **Wall-clock budget** — measure the wall-clock time of
//      `PlayheadRuntime(isPreviewRuntime: false)` and assert it stays
//      below a budget. Start at 250 ms (the same number the hkn1
//      `loadInputs` budget uses); tighten over time as the launch
//      path gets cleaner.
//
//   2. **Per-launch-path source canaries** — pin the post-jndk /
//      post-jncn invariants in source: no `FileManager.default.create*`,
//      no `FileHandle(`, no `Data(...).write(`, no `try ... .write(`,
//      no `SystemLanguageModel(`, no `sqlite3_open*`, no `.open(` on
//      database stores inside the body of `init(isPreviewRuntime:)`.
//      The behaviour rails that drove these fixes already live next
//      door (`PermissiveClassifierBoxLazinessTests`,
//      `PlayheadRuntimeLoggerLazinessTests`); these source canaries
//      are the back-stop that catches a regression at compile-time
//      review even if the behaviour test is overlooked.
//
// XCTest, NOT Swift Testing: the source canary class needs to remain
// filterable through the Xcode test-plan's `skippedTests` list (the
// `xctestplan` filter silently ignores Swift Testing identifiers — see
// the comment in `PlayheadFastTests.xctestplan`). The wall-clock test
// is XCTest for the same reason, so a future "skip on CI" exclusion
// can be applied without per-test code changes.

import Foundation
import XCTest
@testable import Playhead

// MARK: - Wall-clock budget

/// Measures `PlayheadRuntime.init` wall-clock on the simulator and
/// asserts it stays under a generous budget. Two warm-up iterations
/// burn off any first-launch caches (Foundation, dyld, etc.) so the
/// reported median reflects the steady-state cost of the synchronous
/// init body.
///
/// Why a median (and not a max): simulator timings on shared CI agents
/// are notoriously noisy — a single sample can be inflated by an
/// unrelated context switch. Reporting the median of N=5 measurements
/// suppresses single-sample outliers. If the median trips the budget
/// the regression is real; one isolated outlier is not enough.
final class PlayheadRuntimeLaunchPerfTests: XCTestCase {

    // Load-sensitive latency measurement — runs only in the serial perf pass
    // where the CPU is quiescent. See PerfGate / playhead-zx0l.
    override func setUpWithError() throws {
        try XCTSkipUnless(PerfGate.runsMeasurementTests, PerfGate.skipReason)
    }

    /// Initial budget. 250 ms matches the hkn1 `loadInputs` test for
    /// consistency. Real-device init on Dan's iPhone post-jndk/jncn
    /// runs in ~30 ms; simulator on a clean run is ~50–80 ms. The
    /// slack covers shared-agent noise. Tighten over time.
    private static let budgetSeconds: Double = 0.25

    /// Number of warm-up iterations before the measured run.
    /// The first construction pays one-time costs (model manifest
    /// load, redactor pattern compile, signposter creation) that
    /// are amortised across all subsequent constructions in the
    /// process. Two warm-ups are enough to drain those costs.
    private static let warmupIterations = 2

    /// Number of measured iterations. Must be odd so the median is
    /// unambiguous.
    private static let measuredIterations = 5

    @MainActor
    func testInitFitsLaunchBudget() async throws {
        // Warm up: construct + shut down without measuring.
        for _ in 0..<Self.warmupIterations {
            let runtime = PlayheadRuntime(isPreviewRuntime: false)
            await runtime.shutdown()
        }

        // Measured runs. `mach_absolute_time` via `DispatchTime` is the
        // monotonic source on Apple platforms — it cannot drift if the
        // simulator host re-syncs wall-clock mid-test (an `NSDate`-based
        // measurement would silently inflate or compress the sample on
        // an NTP slew). The conversion to seconds uses the API-stable
        // `uptimeNanoseconds` field, which is documented as monotonic
        // mach time.
        var samples: [Double] = []
        samples.reserveCapacity(Self.measuredIterations)
        for _ in 0..<Self.measuredIterations {
            let startNanos = DispatchTime.now().uptimeNanoseconds
            let runtime = PlayheadRuntime(isPreviewRuntime: false)
            let endNanos = DispatchTime.now().uptimeNanoseconds
            samples.append(Double(endNanos - startNanos) / 1_000_000_000.0)
            // Shut the runtime down between samples so the deferred
            // migrate Task spawned by init() doesn't pile up across
            // iterations. `shutdown()` cancels the startup task and
            // drains the observer loop; without it, repeated inits
            // leak tasks and skew later samples.
            await runtime.shutdown()
        }

        let sorted = samples.sorted()
        let median = sorted[sorted.count / 2]

        // Surface the full sample distribution in the failure message
        // so a regression report is self-contained — a CI log of a
        // single failing run tells the on-call which sample(s) blew
        // the budget without a re-run.
        let formatted = samples
            .map { String(format: "%.1fms", $0 * 1000) }
            .joined(separator: ", ")

        // Always log the measurement so a passing run on CI still
        // surfaces the latest median for trend-tracking. The signpost
        // interval added in playhead-jndk gives Instruments-grade
        // detail; this print is the cheap, always-on companion.
        print(
            """
            [LaunchPerf] PlayheadRuntime.init median=\(String(format: "%.1f", median * 1000))ms \
            samples=[\(formatted)] budget=\(String(format: "%.0f", Self.budgetSeconds * 1000))ms
            """
        )
        XCTAssertLessThan(
            median,
            Self.budgetSeconds,
            """
            PlayheadRuntime.init wall-clock median = \(String(format: "%.1f", median * 1000)) ms; \
            budget = \(String(format: "%.0f", Self.budgetSeconds * 1000)) ms. \
            Samples (post-warmup): [\(formatted)]. \
            Inspect the recent diff for new synchronous file-system, FoundationModels, \
            or SQLite work added to init's body — the source canaries in \
            PlayheadRuntimeInitLaunchPathSourceCanaryTests / \
            PermissiveClassifierBoxLazinessTests / \
            PlayheadRuntimeLoggerLazinessSourceCanaryTests pin the known hazards.
            """
        )
    }
}

// MARK: - Per-launch-path source canaries

/// Pins the post-jndk / post-jncn invariants that
/// `PlayheadRuntime.init`'s synchronous body is free of file-system,
/// FoundationModels, and SQLite work. Every check operates on the
/// same brace-isolated body string so a future swift-format reflow
/// can't leak the regex out of the init scope.
final class PlayheadRuntimeInitLaunchPathSourceCanaryTests: XCTestCase {

    /// `FileManager.default.create*` covers `createFile(`,
    /// `createDirectory(`, and any future create-shaped FileManager
    /// addition. The whitespace tolerance handles a swift-format
    /// reflow that breaks `FileManager.default` and `.createFile(`
    /// across lines.
    private static let fileManagerCreatePattern =
        #"FileManager\s*\.\s*default\s*\.\s*create"#

    /// `FileHandle(` matches every FileHandle initialiser shape:
    /// `FileHandle(forReadingAtPath:)`, `FileHandle(forWritingTo:)`,
    /// `FileHandle(fileDescriptor:)`, etc. Whitespace tolerance keeps
    /// the canary stable across formatting changes.
    private static let fileHandleInitPattern = #"FileHandle\s*\("#

    /// `Data(...).write(` — chained `Data(<expr>).write(...)` calls
    /// that synchronously serialise bytes to disk. The `[\s\S]*?`
    /// inside the parens is non-greedy across newlines so a future
    /// reflow into multi-line `Data(\n    foo\n)` still matches.
    private static let dataWritePattern = #"Data\s*\([\s\S]*?\)\s*\.\s*write\s*\("#

    /// `try ... .write(` catches the broader pattern of a synchronous
    /// `try someValue.write(to:)` call inside init. Includes `try?` and
    /// `try!`. The post-`try` body is matched non-greedily up to the
    /// first `.write(` so we don't span across statements.
    /// We restrict the leading `try` to be followed by a value-like
    /// fragment that does NOT itself contain `try` (which would mean
    /// we're spanning two unrelated `try` statements). This keeps the
    /// canary tight while staying whitespace-tolerant.
    ///
    /// Newlines are intentionally **allowed** in the gap between
    /// `try` and `.write(` — a swift-format reflow that splits the
    /// chain across lines (e.g. `try someValue\n    .write(to: …)`)
    /// must still match. Statement separators (`;`, `{`, `}`) remain
    /// excluded so we don't bridge two unrelated `try` statements.
    private static let tryWritePattern = #"\btry[!?]?\s+[^;{}]+?\.write\s*\("#

    /// `SystemLanguageModel(` — direct construction of the iOS-26
    /// FoundationModels system model. This is the call that triggered
    /// jndk's multi-minute first-launch freeze (the framework probes
    /// on-device model availability under the constructor). The
    /// production lazy wrapper is `BackfillJobRunner.PermissiveClassifierBox`,
    /// whose factory closure is allowed to mention the constructor —
    /// but that closure body is only inside the box's literal, not
    /// in the init's body when the box is constructed (the closure
    /// is unevaluated at that point). The brace-aware walker isolates
    /// only the init body, so the closure body inside the box is in
    /// scope here. We compensate by counting matches inside the
    /// `PermissiveClassifierBox { ... }` literal and subtracting them
    /// from the total — same strategy
    /// `PermissiveClassifierBoxLazinessTests` uses.
    private static let systemLanguageModelPattern = #"SystemLanguageModel\s*\("#

    /// `sqlite3_open` and `sqlite3_open_v2` — the C-API entry points
    /// to opening a SQLite database. Synchronous, blocks the calling
    /// thread for the duration of the file open. Production stores
    /// (`AnalysisStore`, `AdCatalogStore`) bury this inside their
    /// own `init(directory:)` which is permitted; this canary checks
    /// that PlayheadRuntime.init does not call sqlite3_open* DIRECTLY
    /// (which would mean someone bypassed the store abstraction).
    private static let sqliteOpenPattern = #"sqlite3_open(?:_v2)?\s*\("#

    /// `.open(` on database-store types — `AnalysisStore.open(`,
    /// `AdCatalogStore.open(`, etc. The bead spec calls these out
    /// explicitly because they're the path that synchronously runs
    /// `init(directory:)` + `migrate()` together. PlayheadRuntime
    /// today uses `try AnalysisStore()` followed by an off-main
    /// `await store.migrate()` — that's the safe shape.
    private static let storeOpenPattern =
        #"\b(?:AnalysisStore|AdCatalogStore)\s*\.\s*open\s*\("#

    func testInitBodyHasNoSyncFileSystemCalls() throws {
        let body = try Self.loadInitBody()

        // FileManager creates: zero tolerance.
        let fmCreates = SwiftSourceInspector.regexOccurrences(
            of: Self.fileManagerCreatePattern, in: body
        )
        XCTAssertEqual(fmCreates, 0, """
        PlayheadRuntime.init body contains \(fmCreates) `FileManager.default.create*` \
        call(s). Synchronous directory or file creation in init extends the \
        launch-storyboard window — defer to an async migrate() / first-use lazy path \
        (mirror playhead-jncn).
        """)

        // FileHandle inits: zero tolerance.
        let fileHandles = SwiftSourceInspector.regexOccurrences(
            of: Self.fileHandleInitPattern, in: body
        )
        XCTAssertEqual(fileHandles, 0, """
        PlayheadRuntime.init body contains \(fileHandles) `FileHandle(...)` \
        constructor call(s). FileHandle initialisers can block on the file system — \
        defer to an async path.
        """)

        // Data(...).write(: zero tolerance.
        let dataWrites = SwiftSourceInspector.regexOccurrences(
            of: Self.dataWritePattern, in: body
        )
        XCTAssertEqual(dataWrites, 0, """
        PlayheadRuntime.init body contains \(dataWrites) `Data(...).write(...)` \
        chain(s). Synchronous Data writes block init — defer to an async path.
        """)

        // try ... .write(: zero tolerance.
        let tryWrites = SwiftSourceInspector.regexOccurrences(
            of: Self.tryWritePattern, in: body
        )
        XCTAssertEqual(tryWrites, 0, """
        PlayheadRuntime.init body contains \(tryWrites) `try ... .write(...)` \
        call(s). Synchronous writes block init — defer to an async path.
        """)
    }

    func testInitBodyHasNoFoundationModelsConstruction() throws {
        let body = try Self.loadInitBody()

        // Total `SystemLanguageModel(` constructor mentions in the
        // init body. Any direct call inside init is the jndk hazard.
        // The production path constructs the model lazily via
        // `PermissiveAdClassifier()`, which itself runs only when
        // `BackfillJobRunner.PermissiveClassifierBox`'s factory
        // closure is invoked off-main. Inside init's source, the
        // factory closure literal CAN appear (it's wrapped in
        // `PermissiveClassifierBox { ... }`), but the closure body
        // does not call `SystemLanguageModel(` directly — it calls
        // `PermissiveAdClassifier()`. So the count here should be
        // exactly zero in either case.
        let count = SwiftSourceInspector.regexOccurrences(
            of: Self.systemLanguageModelPattern, in: body
        )
        XCTAssertEqual(count, 0, """
        PlayheadRuntime.init body contains \(count) `SystemLanguageModel(...)` \
        constructor call(s). On iOS 26 this triggers a multi-minute on-device \
        FoundationModels probe on first launch (playhead-jndk). Wrap any new \
        FM construction in a lazy factory like `BackfillJobRunner.PermissiveClassifierBox`.
        """)
    }

    // MARK: - Transitive rail (playhead-xul6)

    /// The FoundationModels entry points whose first touch is expensive.
    /// `SystemLanguageModel` is the one that cost 0.47–2.13 s of held main
    /// actor; `LanguageModelSession` is the sibling the classifier and the
    /// readiness probe use, and it is on the same daemon.
    private static let foundationModelsEntryPoints = [
        "SystemLanguageModel",
        "LanguageModelSession",
    ]

    /// Nodes the walk MUST reach. These are not decoration: a resolver that
    /// silently stops resolving would turn the FM check green while proving
    /// nothing, and a canary that can only pass is worth less than no canary.
    /// `CapabilitiesService.init` and `.captureSnapshot` are the two frames
    /// that carried playhead-xul6's defect, so pinning them means the exact
    /// path that regressed is still being walked.
    private static let requiredWalkNodes: [SwiftSourceCallGraph.Node] = [
        SwiftSourceCallGraph.Node(type: "CapabilitiesService", member: "init"),
        SwiftSourceCallGraph.Node(type: "CapabilitiesService", member: "captureSnapshot"),
        SwiftSourceCallGraph.Node(type: "PlaybackService", member: "init"),
        SwiftSourceCallGraph.Node(type: "FoundationModelsUsabilityProbe", member: "cachedUsability"),
    ]

    /// **The transitive rail this bead exists for.**
    ///
    /// `testInitBodyHasNoFoundationModelsConstruction` above bans
    /// `SystemLanguageModel(` in init's OWN BODY TEXT — and it passed for the
    /// six weeks the launch path was holding the main actor on exactly that
    /// API, because the read had moved one call deeper into a service whose
    /// initialiser looks free at the call site:
    ///
    ///     PlayheadRuntime.init
    ///       -> CapabilitiesService.init
    ///         -> CapabilitiesService.captureSnapshot
    ///           -> CapabilitiesService.checkFoundationModelsState  ← the read
    ///
    /// A ban on a spelling inside one function is not a guard on a call graph.
    /// This walks the graph: every construction, every same-type call, every
    /// `Type.member(...)`, transitively, following only edges that run
    /// SYNCHRONOUSLY when init runs. Closure literals are values rather than
    /// calls, so `Task { }` and `PermissiveClassifierBox { }` — the sanctioned
    /// deferral shapes — are correctly invisible without an allowlist anyone
    /// can grow. See `SwiftSourceCallGraph` for the walker's named limits.
    ///
    /// What this does NOT say, second edition (review round 1, by mutation):
    /// that the launch path is free of FoundationModels. It says no
    /// FUNCTION CALL from init reaches it. Two shapes get past this walk and
    /// are named as limits L-6 and L-7 in `SwiftSourceCallGraph` — a member
    /// ACCESS that runs a getter (`_ = Self.someComputedProperty`) and a
    /// type-scope property initialiser (`private let x = Foo.expensive()`,
    /// which runs inside every `init` of that type). Both were re-introduced
    /// into `CapabilitiesService` at review as real main-actor reads and this
    /// test PASSED on both. Read a green result as "no call path", not as "no
    /// touch".
    ///
    /// What this does NOT say: that no FoundationModels work happens at
    /// launch. `CapabilitiesService.refreshSnapshot()` still reads
    /// `SystemLanguageModel.default`, deliberately — it runs on the service
    /// actor's own executor, reached only through a `Task`, so the main actor
    /// suspends rather than blocks. The property pinned here is exactly the
    /// one that was violated: nothing on the SYNCHRONOUS main-actor path
    /// touches FoundationModels.
    func testInitSynchronousCallGraphIsFreeOfFoundationModels() throws {
        let result = try SwiftSourceCallGraph.walk(
            sourceRoot: "Playhead",
            rootRelativePath: "Playhead/App/PlayheadRuntime.swift",
            rootType: "PlayheadRuntime",
            rootSignature: "init(isPreviewRuntime: Bool = false) {",
            bannedTokens: Self.foundationModelsEntryPoints
        )

        // Non-vacuity, in the order a broken walk would fail them.
        XCTAssertGreaterThan(result.indexedFileCount, 300, """
        The call-graph index found only \(result.indexedFileCount) Swift files under Playhead/. \
        The walk cannot resolve what it never read — fix the index before reading the verdict.
        """)
        XCTAssertGreaterThan(result.rootBodyByteCount, 20_000, """
        PlayheadRuntime.init's body measured \(result.rootBodyByteCount) bytes, which is far \
        smaller than the ~1,000-statement init this canary is written against. The brace walker \
        or the root signature has drifted; the verdict below would be vacuous.
        """)
        XCTAssertGreaterThan(result.visited.count, 100, """
        The synchronous walk from PlayheadRuntime.init reached only \(result.visited.count) \
        members. It reached 236 when this canary was written; a collapse means the resolver \
        stopped resolving, not that the launch path got simpler.
        """)
        for node in Self.requiredWalkNodes {
            XCTAssertTrue(result.visited.contains(node), """
            The synchronous walk from PlayheadRuntime.init no longer reaches `\(node)`. Either \
            the launch path genuinely stopped calling it — in which case update \
            `requiredWalkNodes` and say so — or the walker stopped resolving that edge, in \
            which case the FoundationModels verdict below is worth nothing. Do not delete this \
            assertion to make the suite green.
            """)
        }

        let report = result.findings.map { "  " + $0.description }.joined(separator: "\n")
        XCTAssertTrue(result.findings.isEmpty, """
        \(result.findings.count) synchronous path(s) from PlayheadRuntime.init reach a \
        FoundationModels entry point:

        \(report)

        `PlayheadRuntime` is @MainActor, so everything on these paths runs on the MAIN ACTOR \
        before RootView can resolve. Reading `SystemLanguageModel` there cost 0.47–2.13 s per \
        launch on the simulator (playhead-xul6), where the repeated 2003 ms readings are a \
        2-second timeout rather than work; playhead-jndk was a multi-minute freeze from the \
        same API on a device. Move the read behind a `Task { }` onto an actor, or behind a lazy \
        accessor the way `CapabilitiesService.foundationModelsContextSize()` does it. Widening \
        this canary is not a fix.
        """)
    }

    func testInitBodyHasNoSqliteOpenCalls() throws {
        let body = try Self.loadInitBody()

        let sqliteOpens = SwiftSourceInspector.regexOccurrences(
            of: Self.sqliteOpenPattern, in: body
        )
        XCTAssertEqual(sqliteOpens, 0, """
        PlayheadRuntime.init body contains \(sqliteOpens) raw `sqlite3_open*(...)` \
        call(s). Direct C-API SQLite opens bypass the store abstraction's lazy / \
        graceful-recovery paths. Construct an `AnalysisStore` (or peer) instead — \
        the store buries the open behind `init(directory:)` and pairs it with an \
        async `migrate()`.
        """)

        let storeOpens = SwiftSourceInspector.regexOccurrences(
            of: Self.storeOpenPattern, in: body
        )
        XCTAssertEqual(storeOpens, 0, """
        PlayheadRuntime.init body contains \(storeOpens) `<DBStore>.open(...)` \
        call(s). The `.open()` factory bundles `init(directory:)` + `migrate()` \
        into one synchronous call, which forces DDL onto the launch path. Use \
        the two-step `AnalysisStore()` + off-main `await store.migrate()` shape \
        that the post-jndk init flow uses.
        """)
    }

    // MARK: - Helper

    /// Loads `PlayheadRuntime.swift` and returns the brace-isolated
    /// body of `init(isPreviewRuntime:Bool = false)` with all `//` and
    /// `/* */` comments stripped AND all string-literal contents
    /// blanked. Stripping comments is critical because the init body
    /// carries multi-line audit comments that mention every forbidden
    /// token by name (`SystemLanguageModel`, `sqlite3_open`, etc.) — a
    /// naive grep on the raw body false-positives on those comments.
    ///
    /// String-literal stripping is also required: a regression that
    /// introduces a log line like `print("retrying try foo.write(...)")`
    /// would falsely trip the `tryWritePattern` if we only stripped
    /// comments. The canary should match the actual call sites, not
    /// log strings that happen to mention the forbidden token. The
    /// quote characters themselves are preserved by
    /// ``SwiftSourceInspector.strippingCommentsAndStrings`` so a
    /// regex anchored on a quote boundary is unaffected.
    private static func loadInitBody() throws -> String {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )
        guard let body = SwiftSourceInspector.firstBody(
            in: source,
            after: "init(isPreviewRuntime: Bool = false) {"
        ) else {
            throw NSError(
                domain: "PlayheadRuntimeInitLaunchPathSourceCanaryTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey:
                    "init(isPreviewRuntime: Bool = false) signature not found in PlayheadRuntime.swift — canary must be updated alongside any rename."
                ]
            )
        }
        return SwiftSourceInspector.strippingCommentsAndStrings(body)
    }
}
