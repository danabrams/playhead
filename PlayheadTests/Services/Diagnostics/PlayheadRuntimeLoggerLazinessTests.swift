// PlayheadRuntimeLoggerLazinessTests.swift
// playhead-jncn: Source-canary that asserts each of the 5 sync-loggers
// constructed inside `PlayheadRuntime.init` keep their `init` body free of
// FileManager / FileHandle / Data-write calls. The heavy I/O must move to
// an async `migrate()` (or first-use lazy path) so the synchronous
// `PlayheadRuntime.init` flow — which extends the launch-storyboard
// window — stays off the disk.
//
// Mirrors the style of
// `PermissiveClassifierBoxLazinessTests.testInitBodyDoesNotEagerlyConstructPermissiveClassifier`
// (playhead-jndk): walk the source file, isolate the brace-delimited
// body of the target init, and grep for forbidden tokens.
//
// XCTest (not Swift Testing) so the canary class is filterable through
// the Xcode test plan's `skippedTests` (`xctestplan` silently ignores
// Swift Testing identifiers; see PlayheadFastTests.xctestplan comment).

import Foundation
import XCTest
@testable import Playhead

final class PlayheadRuntimeLoggerLazinessSourceCanaryTests: XCTestCase {

    // MARK: - Per-logger canaries

    /// `FoundationModelsFeedbackStore.init` (audit #4 — DEBUG only) must
    /// not perform synchronous FileManager / FileHandle / Data-write work.
    /// The directory resolution + create-on-demand path is `migrate()`.
    func testFoundationModelsFeedbackStoreInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/Services/AdDetection/FoundationModelsFeedbackStore.swift",
            initSignatures: [
                "init(\n        directory: URL? = nil,",
                "init(directory: URL? = nil,"
            ],
            symbolForMessages: "FoundationModelsFeedbackStore"
        )
    }

    /// `SurfaceStatusInvariantLogger.init` (audit #8) must not perform
    /// synchronous FileManager / FileHandle / Data-write work. Salt load
    /// + directory creation are deferred to first use through
    /// `LoggerState`.
    ///
    /// The shell `SurfaceStatusInvariantLogger.init(directory:)` is a
    /// thin forwarder — the heavyweight prior work lives in
    /// `LoggerState.init`. We canary BOTH bodies so a future regression
    /// that re-introduces sync I/O in either place fails here.
    func testSurfaceStatusInvariantLoggerInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/SurfaceStatus/SurfaceStatusInvariantLogger.swift",
            initSignatures: [
                "init(directory: URL? = nil) {",
                "init(directory: URL?) {"
            ],
            symbolForMessages: "SurfaceStatusInvariantLogger / LoggerState"
        )
    }

    /// `DecisionLogger.init(directory:rotationThresholdBytes:)` (audit
    /// #10 — DEBUG only) must not perform synchronous FileManager
    /// directory create or `contentsOfDirectory` scans. Both move to
    /// `migrate()` / first-use.
    func testDecisionLoggerInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/Services/AdDetection/DecisionLogger.swift",
            initSignatures: [
                "init(\n        directory: URL,",
                "init(directory: URL,"
            ],
            symbolForMessages: "DecisionLogger"
        )
    }

    /// `AssetLifecycleLogger.init(directory:rotationThresholdBytes:)`
    /// (audit #15) must not perform synchronous FileManager directory
    /// create or `contentsOfDirectory` scans. Both move to `migrate()` /
    /// first-use.
    func testAssetLifecycleLoggerInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/Services/AnalysisCoordinator/AssetLifecycleLogger.swift",
            initSignatures: [
                "init(\n        directory: URL,",
                "init(directory: URL,"
            ],
            symbolForMessages: "AssetLifecycleLogger"
        )
    }

    /// `BGTaskTelemetryLogger.init(directory:rotationThresholdBytes:)`
    /// (audit #17) must not perform synchronous FileManager directory
    /// create or `contentsOfDirectory` scans. Both move to `migrate()` /
    /// first-use.
    func testBGTaskTelemetryLoggerInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/Services/Diagnostics/BGTaskTelemetryLogger.swift",
            initSignatures: [
                "init(\n        directory: URL,",
                "init(directory: URL,"
            ],
            symbolForMessages: "BGTaskTelemetryLogger"
        )
    }

    // MARK: - Helper

    /// Forbidden tokens, per the bead spec. Each must NOT appear in the
    /// designated init body. The list captures every realistic synchronous
    /// FileManager / FileHandle / write surface used elsewhere in this
    /// codebase.
    private static let forbiddenTokens: [String] = [
        "FileManager.default.create",   // catches createFile, createDirectory
        "FileHandle(",                   // any FileHandle constructor
        ".write(to:",                    // Data / String write to URL
        ").write(",                      // chained Data().write(...)
        "fileManager.createDirectory",   // injected fileManager
        "fileManager.url(",              // injected fileManager url(create:true)
        "fileManager.createFile",
        "FileManager.default.url(",      // url(create: true) implicit dir create
        "contentsOfDirectory",           // scanNextRotationIndex pre-refactor
    ]

    /// Locate the first init signature in `signatures` that exists in the
    /// source file, isolate its brace-delimited body, and assert that
    /// none of the forbidden tokens appear inside.
    ///
    /// Source walking is delegated to ``SwiftSourceInspector`` (shared
    /// with the playhead-jndk PermissiveClassifierBox canary; extracted
    /// in playhead-2axy).
    private func assertInitBodyHasNoFileSystemCalls(
        sourcePath: String,
        initSignatures: [String],
        symbolForMessages: String,
        file: StaticString = #file,
        line: UInt = #line
    ) throws {
        let source = try SwiftSourceInspector.loadSource(repoRelativePath: sourcePath)

        // For SurfaceStatusInvariantLogger we audit BOTH the public
        // facade init AND `LoggerState.init` — the heavyweight work
        // historically lived in the latter. `combinedBodies` collects
        // every body matching any of the supplied signatures.
        guard let combinedBody = SwiftSourceInspector.combinedBodies(
            in: source,
            matchingAnyOf: initSignatures
        ) else {
            XCTFail(
                "[\(symbolForMessages)] none of the init signatures \(initSignatures) found in \(sourcePath) — test must be updated alongside any rename.",
                file: file,
                line: line
            )
            return
        }

        for token in Self.forbiddenTokens {
            if combinedBody.contains(token) {
                XCTFail(
                    """
                    [\(symbolForMessages)] init body in \(sourcePath) still contains \
                    forbidden synchronous file-system token `\(token)`. The bead \
                    playhead-jncn requires this work to move to an async `migrate()` \
                    (or first-use lazy path) so PlayheadRuntime.init stays off-disk. \
                    Mirror the AdCatalogStore.ensureOpen() pattern (Playhead/Services/\
                    AdDetection/AdCatalogStore.swift).
                    """,
                    file: file,
                    line: line
                )
            }
        }
    }
}
