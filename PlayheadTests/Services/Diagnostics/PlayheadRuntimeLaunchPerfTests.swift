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
