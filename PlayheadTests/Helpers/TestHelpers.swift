// TestHelpers.swift
// Shared test utilities used across PlayheadTests suites.

import Darwin
import Foundation
import SQLite3
import Testing
import os
@testable import Playhead

/// Vestigial compatibility shim. Originally tracked temp dirs for cleanup in
/// `deinit`, but Swift does not run deinit for globals/static members at
/// process exit, so this never actually cleaned up. Cleanup is now handled
/// centrally by `makeTempDir`'s scratch-root + atexit mechanism below;
/// call sites that construct one of these and call `track(_:)` are no-ops
/// that remain only to avoid churning every test file that referenced them.
final class TestTempDirTracker: @unchecked Sendable {
    func track(_ dir: URL) {}
}

// Process-wide scratch root. Every `makeTempDir` call creates a subdirectory
// under `PlayheadTestScratch/` in the test app's tmp. The root is wiped on
// first use so leftover directories from the previous test process are
// reclaimed (historically 140k+ dirs / 70 GiB accumulated before cleanup
// existed). An atexit hook also removes the root on normal exit — Swift
// does not run deinit for globals at process termination, so a tracker
// class can't be used here.
//
// BOTH OF THESE ARE THE BACKSTOP, NOT THE BUDGET (playhead-cgka). They fire at
// process boundaries and reclaim nothing while the suite runs, which made peak
// disk the SUM of every test's scratch: a full gate consumed ~8 GiB of free
// space and died on ENOSPC near the end of the suite. `TestScratchReaper`
// bounds the peak DURING the run; these two remain because an ABNORMAL exit is
// precisely when the reaper does not get to finish, and a wipe-on-first-use is
// the only thing that then reclaims the wreckage. Do not remove either.
//
// Both now go through `TestScratchReaper.forceRemove`, which repairs
// permissions before giving up, and that is NOT a tidiness change — a plain
// `removeItem` here silently reclaimed NOTHING whenever a previous abnormal
// exit left a 0o300 directory behind, which is the state this suite creates by
// design (DownloadManagerTests' `complete/`, restored in a `defer` that an
// abnormal exit skips). MEASURED 2026-08-02 on this box: `$TMPDIR/Deleting-*`,
// the staging area CoreSimulator moves a device's data into before deleting it
// asynchronously, had accumulated 15 GiB — and every single one of the
// directories still there was stuck on exactly one unreadable
// `PlayheadTestScratch/…/complete`. `simctl erase` had appeared to work and had
// freed nothing. Clearing them took the volume from 9.0 GiB to 25.4 GiB free.
/// The process-boundary wipe, named so it can be tested. A global initialiser
/// and an `atexit` block are both unreachable from a test, so the one property
/// that matters here — that the wipe survives a directory the suite chmodded
/// unreadable — would otherwise be unpinned at exactly the two call sites where
/// its failure is silent and permanent.
func wipeTestScratchRoot(at url: URL) {
    TestScratchReaper.forceRemove(url)
}

private nonisolated(unsafe) var _scratchRoot: URL = {
    let root = FileManager.default.temporaryDirectory
        .appendingPathComponent("PlayheadTestScratch", isDirectory: true)
    wipeTestScratchRoot(at: root)
    try? FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
    atexit {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadTestScratch", isDirectory: true)
        wipeTestScratchRoot(at: url)
    }
    return root
}()

/// Creates a uniquely-named temporary directory for test isolation.
///
/// The directory lives under a shared scratch root that is wiped on the next
/// test process startup and on normal exit. Those two paths are the BACKSTOP,
/// not the budget: they fire at process boundaries, so on their own peak disk
/// is the SUM of every test's scratch rather than the max of any one, which is
/// what made a full gate die with `NSPOSIXErrorDomain Code=28` (playhead-cgka).
///
/// - Parameter owner: bind the directory's lifetime to this object. Once it is
///   deallocated — for an `AnalysisStore` that means its `deinit` has closed the
///   SQLite handle — `TestScratchReaper` reclaims the bytes mid-run. Passing
///   `nil` (the default, so all existing call sites keep compiling) keeps the
///   old behaviour: nothing is reclaimed until the process ends. Prefer
///   `makeTestStore*`, which attaches ownership for you.
func makeTempDir(prefix: String = "PlayheadTests", ownedBy owner: AnyObject? = nil) throws -> URL {
    let url = _scratchRoot
        .appendingPathComponent("\(prefix)-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    if let owner {
        TestScratchReaper.shared.adopt(url, owner: owner)
    } else {
        TestScratchReaper.shared.register(url)
    }
    return url
}

// MARK: - AnalysisStore Factory

/// Creates an AnalysisStore backed by a temporary directory for isolated testing.
/// The directory is automatically cleaned up when the test process ends.
func makeTestStore() async throws -> AnalysisStore {
    let (store, _) = try await makeTestStoreWithDirectory()
    return store
}

/// Variant of `makeTestStore` that also returns the backing directory so
/// callers can probe the underlying SQLite file directly (e.g. with
/// `probeRowCount`). Used by tests that need to verify orphan-row
/// contracts that the public store accessors hide behind JOINs.
func makeTestStoreWithDirectory() async throws -> (AnalysisStore, URL) {
    // playhead-cgka: the prefix is `PlayheadTestStore`, not the default
    // `PlayheadTests`, so `scripts/scratch-sampler.sh --breakdown` can tell a
    // store directory apart from a bare temp directory. They used to share the
    // default prefix, which is why the first measurement could say "2,591 dirs
    // at 732.6 KiB" but not which factory made them.
    let dir = try makeTempDir(prefix: "PlayheadTestStore")
    let store = try AnalysisStore(directory: dir)
    try await store.migrate()
    // Ownership is attached AFTER migrate() so a throw leaves the directory in
    // the unowned-but-registered state rather than adopted by a half-built
    // store — the backstop still reclaims it at process exit.
    TestScratchReaper.shared.adopt(dir, owner: store)
    return (store, dir)
}

// MARK: - ScanCohort test helpers

/// Returns a sorted-keys JSON encoding of a `ScanCohort` whose only varying
/// field is `promptLabel`. Lets tests construct distinct-but-canonical cohort
/// strings without hand-rolling the JSON shape — important because the
/// AnalysisStore validates `scanCohortJSON` by decoding it into a real
/// `ScanCohort`. Used by training-data tests that want to verify cohort
/// provenance without depending on `ScanCohort.productionJSON()` (which is
/// constant per build). (playhead-4my.10.2)
func makeCohortJSON(promptLabel: String) -> String {
    let cohort = ScanCohort(
        promptLabel: promptLabel,
        promptHash: "phase3-prompt-2026-04-06",
        schemaHash: "phase3-schema-2026-04-06",
        scanPlanHash: "phase3-plan-2026-04-06",
        normalizationHash: "phase3-norm-2026-04-06",
        osBuild: "26.0.0",
        locale: "en_US",
        appBuild: "1"
    )
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let data = (try? encoder.encode(cohort)) ?? Data()
    return String(data: data, encoding: .utf8) ?? "{}"
}

// MARK: - Async polling helper

/// Polls an async predicate until it returns true or the deadline expires.
/// Returns `true` if the predicate became true within the deadline, `false`
/// on timeout. Used in place of fixed `Task.sleep` waits in scheduler tests
/// so they remain deterministic under heavy parallel-test CPU contention
/// (playhead-qtc). The poll interval is intentionally small so tests that
/// complete quickly aren't slowed down, and the default deadline is
/// generous enough to absorb simulator scheduling jitter when the whole
/// suite runs in parallel.
func pollUntil(
    timeout: Duration = .seconds(30),
    interval: Duration = .milliseconds(20),
    _ condition: @Sendable () async throws -> Bool
) async rethrows -> Bool {
    let clock = ContinuousClock()
    let deadline = clock.now.advanced(by: timeout)
    while clock.now < deadline {
        if try await condition() { return true }
        // `rethrows` constraint forces `try?` here (Task.sleep throws
        // CancellationError, not from the closure), so check
        // `Task.isCancelled` after the sleep to exit cleanly on
        // cancellation rather than spinning until the outer
        // `.timeLimit`.
        try? await Task.sleep(for: interval)
        if Task.isCancelled { return false }
    }
    return try await condition()
}

/// playhead-xc6b: poll budget for the positive-control / barrier waits in the
/// executor-starvation-sensitive suites (`PlaybackServiceActorTests`,
/// `RuntimeShutdownLifecycleTests`).
///
/// `pollUntil`'s 30 s default is sized for a busy machine, not a starved one.
/// MEASURED on this box, 2026-07-28: in a full `PlayheadFastTests` run, tests
/// carrying a 60 s `.timeLimit` blew it at **119-134 s elapsed** — on
/// `main@89bf541a` as well as on the branch, so it is the host, not a change.
/// A 30 s poll can therefore expire on a merely-starved run and turn a correct
/// implementation red, trading a fail-OPEN vacuity for a fail-CLOSED flake.
///
/// 60 s, with at most two such polls in any one test, stays under those suites'
/// `.timeLimit(.minutes(3))` hang backstop with 60 s of headroom — so a
/// backstop trip still means "hang", not "two polls expired". A satisfied poll
/// exits on its first read, so the budget is only ever spent on the failing
/// path and costs nothing when the code is correct.
///
/// THE TWO-POLL CEILING IS LOAD-BEARING, so treat it as a rule and not a
/// description. Three expiries (180 s) reach the backstop exactly, and the
/// backstop's documented meaning is "a real hang" — a third poll would make
/// the instrument lie about its own failures. If a test genuinely needs a
/// third, raise that test's `.timeLimit` in the same change. Today the only
/// two-poll test is
/// `PlaybackServiceActorIsolationTests.tearDownOwnsEveryLongLivedResource`.
let starvationPollBudget: Duration = .seconds(60)

// MARK: - Migration test helpers

/// H11 (cycle 2): writes `_meta.schema_version = '<version>'` directly into a
/// sqlite file so a subsequent `AnalysisStore(directory:).migrate()` runs the
/// V*IfNeeded ladder from that starting point. Uses a raw sqlite handle so
/// the seed can run before any `AnalysisStore` actor opens the file.
///
/// The caller must ensure the directory exists. The function creates the
/// `_meta` table if necessary so this works on a brand-new directory.
func seedSchemaVersion(_ version: Int, in directory: URL) throws {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(
        dbURL.path,
        &db,
        SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
        nil
    ) == SQLITE_OK else {
        throw NSError(
            domain: "SeedSchemaVersion",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "open failed"]
        )
    }
    defer { sqlite3_close_v2(db) }

    let createMeta = "CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    guard sqlite3_exec(db, createMeta, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedSchemaVersion", code: 2)
    }
    let insertVersion = "INSERT OR REPLACE INTO _meta (key, value) VALUES ('schema_version', '\(version)')"
    guard sqlite3_exec(db, insertVersion, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedSchemaVersion", code: 3)
    }
}

/// H11: probe a column's existence on a freshly-opened sqlite handle, used
/// by migration tests to assert that a particular column was added.
func probeColumnExists(in directory: URL, table: String, column: String) throws -> Bool {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeColumnExists", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let sql = "PRAGMA table_info(\(table))"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeColumnExists", code: 2)
    }
    defer { sqlite3_finalize(stmt) }
    while sqlite3_step(stmt) == SQLITE_ROW {
        if let cName = sqlite3_column_text(stmt, 1),
           String(cString: cName) == column {
            return true
        }
    }
    return false
}

/// H11: probe an index's existence by name.
func probeIndexExists(in directory: URL, indexName: String) throws -> Bool {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeIndexExists", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let sql = "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeIndexExists", code: 2)
    }
    defer { sqlite3_finalize(stmt) }
    sqlite3_bind_text(stmt, 1, indexName, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
    return sqlite3_step(stmt) == SQLITE_ROW
}

/// Cycle 4 H1: hand-builds a v1-shape SQLite database suitable for driving
/// `AnalysisStore.migrateOnlyForTesting()` in isolation from
/// `createTables()`. Only the tables that the V*IfNeeded ladder touches
/// are seeded, in their v1 shape: no `needsShadowRetry`, no
/// `transcriptVersion` on evidence_events, no `phase` columns, and the
/// pre-Phase-6 `ad_windows` shape without `evidenceSources` /
/// `eligibilityGate`. The caller seeds `_meta.schema_version`
/// explicitly via `seedSchemaVersion`.
func seedV1ShapeDatabase(in directory: URL) throws {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(
        dbURL.path,
        &db,
        SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
        nil
    ) == SQLITE_OK else {
        throw NSError(domain: "SeedV1Shape", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let ddl = """
        CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS analysis_assets (
            id TEXT PRIMARY KEY,
            episodeId TEXT NOT NULL,
            assetFingerprint TEXT NOT NULL,
            weakFingerprint TEXT,
            sourceURL TEXT NOT NULL,
            featureCoverageEndTime REAL,
            fastTranscriptCoverageEndTime REAL,
            confirmedAdCoverageEndTime REAL,
            analysisState TEXT NOT NULL,
            analysisVersion INTEGER NOT NULL,
            capabilitySnapshot TEXT,
            createdAt REAL NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS analysis_sessions (
            id TEXT PRIMARY KEY,
            analysisAssetId TEXT NOT NULL,
            state TEXT NOT NULL,
            startedAt REAL NOT NULL,
            updatedAt REAL NOT NULL,
            failureReason TEXT
        );
        CREATE TABLE IF NOT EXISTS evidence_events (
            id TEXT PRIMARY KEY,
            analysisAssetId TEXT NOT NULL,
            eventType TEXT NOT NULL,
            sourceType TEXT NOT NULL,
            atomOrdinals TEXT NOT NULL,
            evidenceJSON TEXT NOT NULL,
            scanCohortJSON TEXT NOT NULL,
            createdAt REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ad_windows (
            id TEXT PRIMARY KEY,
            analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
            startTime REAL NOT NULL,
            endTime REAL NOT NULL,
            confidence REAL NOT NULL,
            boundaryState TEXT NOT NULL,
            decisionState TEXT NOT NULL DEFAULT 'candidate',
            detectorVersion TEXT NOT NULL,
            advertiser TEXT,
            product TEXT,
            adDescription TEXT,
            evidenceText TEXT,
            evidenceStartTime REAL,
            metadataSource TEXT NOT NULL DEFAULT 'none',
            metadataConfidence REAL,
            metadataPromptVersion TEXT,
            wasSkipped INTEGER NOT NULL DEFAULT 0,
            userDismissedBanner INTEGER NOT NULL DEFAULT 0
        );
        """
    guard sqlite3_exec(db, ddl, nil, nil, nil) == SQLITE_OK else {
        let msg = sqlite3_errmsg(db).map { String(cString: $0) } ?? "unknown"
        throw NSError(
            domain: "SeedV1Shape",
            code: 2,
            userInfo: [NSLocalizedDescriptionKey: msg]
        )
    }
}

/// H11: probe a table's existence by name.
func probeTableExists(in directory: URL, table: String) throws -> Bool {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeTableExists", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeTableExists", code: 2)
    }
    defer { sqlite3_finalize(stmt) }
    sqlite3_bind_text(stmt, 1, table, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
    return sqlite3_step(stmt) == SQLITE_ROW
}

/// q45f.1: count rows in a table directly, bypassing any JOINs the
/// public store accessor performs. Used to verify orphan-row contracts
/// (e.g. that a missing-window listen-rewind does NOT insert into
/// `ad_listen_rewinds`, regardless of whether an outward ownership projection
/// would surface it).
/// Table name must be a literal in the test (not user input) — caller
/// is expected to pass a known schema identifier.
func probeRowCount(in directory: URL, table: String) throws -> Int {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeRowCount", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let sql = "SELECT COUNT(*) FROM \(table)"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
        let msg = sqlite3_errmsg(db).map { String(cString: $0) } ?? "unknown"
        throw NSError(
            domain: "ProbeRowCount",
            code: 2,
            userInfo: [NSLocalizedDescriptionKey: msg]
        )
    }
    defer { sqlite3_finalize(stmt) }
    guard sqlite3_step(stmt) == SQLITE_ROW else {
        return 0
    }
    return Int(sqlite3_column_int64(stmt, 0))
}

// MARK: - Correction Test Helpers

/// Creates a minimal AnalysisAsset for correction store tests.
/// Shared across FalseNegativeCorrectionTests, UserCorrectionStoreTests,
/// and CorrectionSuppressionTests.
func makeTestAsset(id: String) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: "ep-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///tmp/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

// MARK: - Skip Orchestrator Test Helpers

/// Shared factory for SkipOrchestrator tests. Used by both
/// SkipOrchestratorCharacterizationTests and CorrectionSuppressionTests.
/// - Parameter episodeDurationSec: playhead-b6r2 — the asset row's duration is
///   what `beginEpisode` loads into the orchestrator's
///   `activeEpisodeDuration`, which is the ONLY input that arms the inventory
///   filter's tail-edge rule. Defaulting it to `nil` (the historical shape of
///   this helper) leaves that rule dormant, which is why the tail half of the
///   playhead-b6r2 defect was invisible to every suite built on this factory.
///   Pass a duration to reproduce production, where `AnalysisCoordinator`
///   pushes one before every `receiveAdWindows`.
func makeSkipTestAnalysisAsset(
    id: String = "asset-1",
    episodeId: String = "ep-1",
    episodeDurationSec: Double? = nil
) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId,
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///test/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil,
        episodeDurationSec: episodeDurationSec
    )
}

func makeSkipTestAdWindow(
    id: String = "ad-1",
    assetId: String = "asset-1",
    startTime: Double = 60,
    endTime: Double = 120,
    confidence: Double = 0.75,
    decisionState: String = "confirmed",
    evidenceText: String? = "brought to you by",
    startEdgeAnchor: String = AutoSkipEdgeAnchor.unanchored.rawValue,
    endEdgeAnchor: String = AutoSkipEdgeAnchor.unanchored.rawValue,
    eligibilityGate: String? = nil
) -> AdWindow {
    AdWindow(
        id: id,
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        boundaryState: "lexical",
        decisionState: decisionState,
        detectorVersion: "detection-v1",
        advertiser: nil,
        product: nil,
        adDescription: nil,
        evidenceText: evidenceText,
        evidenceStartTime: startTime,
        metadataSource: "none",
        metadataConfidence: nil,
        metadataPromptVersion: nil,
        wasSkipped: false,
        userDismissedBanner: false,
        eligibilityGate: eligibilityGate,
        startEdgeAnchor: startEdgeAnchor,
        endEdgeAnchor: endEdgeAnchor
    )
}

// MARK: - Fire-and-forget effect polling (playhead-xsdz.11 / playhead-i08e)
//
// The orchestrator's calibration effects — the durable correction receipt, the
// hard-negative ingest, the per-show threshold-controller sample — are all
// issued through unstructured `Task`s, so a gesture returns before they land.
// Every suite that asserts on them needs the same two primitives: poll until an
// effect appears, and flush pending writes before asserting one did NOT.
// Shared here because two suites need them and a second copy of the polling
// idiom lost the barrier in transcription, which quietly turned an
// upper-bounded assertion into a lower-bounded one.

/// Raised when a fire-and-forget effect never lands inside the polling budget.
///
/// Failing loudly is the point: a helper that returned a zero value on timeout
/// made a DEAD write path (nothing recorded at all) look exactly like a live
/// one that computed zero, which is how playhead-i08e's regression first read
/// as "the controller math is wrong".
struct TestEffectTimeout: Error, CustomStringConvertible {
    let effect: String
    let expected: Int
    let observed: Int

    var description: String {
        """
        \(effect): expected at least \(expected) within the ~10s budget, \
        observed \(observed). The seam under test never issued the effect — \
        this is a DEAD WRITE PATH, not a miscomputed value.
        """
    }
}

/// Show id used ONLY as a write barrier. Never asserted on directly; its row is
/// subtracted out by ``controllerRowsExcludingBarrier(_:_:)``.
let thresholdControllerBarrierShow = "i08e-controller-write-barrier"

func makeTestControllerStore(
    prefix: String = "threshold-controller"
) throws -> PerShowThresholdControllerStore {
    let dir = try makeTempDir(prefix: prefix)
    let store = try PerShowThresholdControllerStore(directoryURL: dir)
    TestScratchReaper.shared.adopt(dir, owner: store)
    return store
}

/// Poll until the show's sampleCount reaches `expected`.
///
/// The budget is generous on purpose — it is only ever spent on the failing
/// path (a satisfied poll exits on its first read), so a saturated machine
/// cannot turn a live write path into a red test.
func pollControllerSampleCount(
    _ store: PerShowThresholdControllerStore,
    show: String,
    expected: Int
) async throws -> PerShowThresholdControllerState {
    var state = PerShowThresholdControllerState.zero
    for _ in 0..<200 { // up to ~10s, only consumed when the write is dead
        state = await store.state(forShow: show)
        if state.sampleCount >= expected { return state }
        try await Task.sleep(nanoseconds: 50_000_000)
    }
    throw TestEffectTimeout(
        effect: "controller samples for show \"\(show)\"",
        expected: expected,
        observed: state.sampleCount
    )
}

/// Flush any controller write the gesture under test may have issued, WITHOUT
/// a wall-clock guess.
///
/// Issues one write of our own through the same production seam and waits for
/// it to become visible. A write the gesture issued is an unstructured `Task`
/// created on the orchestrator's executor (`Task {}` in
/// `recordThresholdControlSignal` inherits actor context, and nothing in the
/// path is `Task.detached`), strictly before this call's; each then hops to the
/// same `PerShowThresholdControllerStore` actor. Same-priority actor jobs run
/// in enqueue order, so observing the barrier's row means the earlier write has
/// landed.
///
/// Two honest limits. That ordering is an implementation property of the
/// default actor executor, not a language guarantee. And it orders only writes
/// whose `Task` was created synchronously inside the gesture — a regression
/// that wrote from a task spawned by a LATER hop (say derived learning calling
/// back into the orchestrator) would still be enqueued after the barrier.
///
/// It replaces a fixed 500 ms settle, which had both limits plus a worse one: a
/// fixed window is the wrong shape for a negative assertion. It never makes a
/// correct implementation fail, but on a loaded machine a REGRESSION's write
/// can land just after it expires and the assertion silently passes. The
/// barrier scales with load instead of racing it.
func drainControllerWrites(
    _ store: PerShowThresholdControllerStore,
    _ orchestrator: SkipOrchestrator
) async throws {
    let before = await store.state(forShow: thresholdControllerBarrierShow).sampleCount
    await orchestrator.recordThresholdControlMiss(
        podcastId: thresholdControllerBarrierShow
    )
    _ = try await pollControllerSampleCount(
        store,
        show: thresholdControllerBarrierShow,
        expected: before + 1
    )
}

/// Wait for the seam's sample, then re-read behind a barrier so callers'
/// `sampleCount == expected` assertions bound the count from ABOVE as well as
/// below — a second, opposing write issued by the same gesture would otherwise
/// land just after the poll returned and cancel the integral unobserved.
func awaitControllerSampleCount(
    _ store: PerShowThresholdControllerStore,
    orchestrator: SkipOrchestrator,
    show: String,
    expected: Int
) async throws -> PerShowThresholdControllerState {
    _ = try await pollControllerSampleCount(store, show: show, expected: expected)
    try await drainControllerWrites(store, orchestrator)
    return await store.state(forShow: show)
}

/// Show id used ONLY as a trust write barrier, never asserted on directly.
/// Seed it alongside the show under test: `recordFalseSkipSignal` never
/// lazy-creates, and a barrier whose write is a no-op is a weaker barrier.
let trustWriteBarrierShow = "o4qr-trust-write-barrier"

/// Flush any per-show TRUST penalty the gesture under test may have issued,
/// WITHOUT a wall-clock guess. The trust twin of ``drainControllerWrites``.
///
/// playhead-o4qr: this exists because the obvious alternative does not work.
/// Watching `falseSkipSignalHandlerForTesting` and calling
/// ``drainOrchestratorEffects`` SURVIVED mutation-battery entry O02 — the drain
/// orders only the FIRST segment of the gesture's `Task`, and with a handler
/// installed that segment merely reaches `await handler(…)`; the recording
/// happens in a later one, after the negative assertion has already read an
/// empty box.
///
/// Going through the store instead makes the same first-segment guarantee
/// sufficient: for the unhandled path the gesture's task does
/// `Task { await trustService.recordFalseSkipSignal(…) }`, so its first segment
/// ENQUEUES on the `TrustScoringService` actor. A write of our own issued after
/// the drain queues behind it, and `recordFalseSkipSignal` awaits its own store
/// update, so by the time this returns any earlier penalty has committed.
///
/// Same honest limits as ``drainControllerWrites(_:_:)``: same-priority FIFO on
/// the default actor executor is an implementation property, not a language
/// guarantee, and only tasks the gesture created synchronously are ordered.
func drainTrustWrites(
    _ service: TrustScoringService,
    _ trustStore: AnalysisStore,
    _ orchestrator: SkipOrchestrator
) async throws {
    await drainOrchestratorEffects(orchestrator)
    await service.recordFalseSkipSignal(podcastId: trustWriteBarrierShow)
    // Fail loudly if the barrier itself is dead, rather than letting a silent
    // no-op turn every caller's negative assertion into a vacuous one.
    let barrier = try await trustStore.fetchProfile(podcastId: trustWriteBarrierShow)
    guard let barrier, barrier.recentFalseSkipSignals > 0 else {
        throw TestEffectTimeout(
            effect: "trust write barrier for \"\(trustWriteBarrierShow)\"",
            expected: 1,
            observed: barrier?.recentFalseSkipSignals ?? 0
        )
    }
}

/// Wait for a show's trust penalties to reach `expected`, then re-read behind
/// ``drainTrustWrites`` so the caller's `== expected` assertion bounds the
/// count from ABOVE as well as below. The trust twin of
/// ``awaitControllerSampleCount(_:orchestrator:show:expected:)``.
///
/// playhead-o4qr: the poll is not belt-and-braces, it is load-bearing, and it
/// was added after measuring. `revertWindow` issues its penalty from an
/// unstructured `Task`, and that task was observed NOT to have reached the
/// trust actor 11 ms and several orchestrator round-trips after the gesture
/// returned — so `drainTrustWrites` alone read a profile that was still 0 and
/// the assertion failed on the UNMUTATED tree. Polling for the positive
/// control first gives the fire-and-forget write the real time it needs;
/// the barrier then closes the window above it.
func awaitTrustFalseSkipSignals(
    _ trustStore: AnalysisStore,
    service: TrustScoringService,
    orchestrator: SkipOrchestrator,
    show: String,
    expected: Int
) async throws -> PodcastProfile {
    var observed = 0
    var landed = false
    for _ in 0..<200 { // up to ~10s, only consumed when the write is dead
        if let profile = try await trustStore.fetchProfile(podcastId: show) {
            observed = profile.recentFalseSkipSignals
            if observed >= expected {
                landed = true
                break
            }
        }
        try await Task.sleep(nanoseconds: 50_000_000)
    }
    guard landed else {
        throw TestEffectTimeout(
            effect: "trust false-skip signals for show \"\(show)\"",
            expected: expected,
            observed: observed
        )
    }
    try await drainTrustWrites(service, trustStore, orchestrator)
    guard let final = try await trustStore.fetchProfile(podcastId: show) else {
        throw TestEffectTimeout(
            effect: "trust profile for show \"\(show)\" after the barrier",
            expected: expected,
            observed: 0
        )
    }
    return final
}

/// Row count for "this seam must record NOTHING" assertions, read behind the
/// barrier above and with the barrier's own row subtracted.
///
/// Counting ROWS (not one show's samples) also catches a write misrouted to
/// some other show id — including the replacement show in a lifecycle race.
func controllerRowsExcludingBarrier(
    _ store: PerShowThresholdControllerStore,
    _ orchestrator: SkipOrchestrator
) async throws -> Int {
    try await drainControllerWrites(store, orchestrator)
    return try await store.count() - 1
}

/// Order every fire-and-forget effect the gesture ALREADY issued ahead of the
/// caller's next read, without a wall-clock guess.
///
/// `persistManualCorrectionVeto` and `ingestNegativeFingerprint` both do
/// `Task { await <collaborator>.<write>(…) }` from the orchestrator's executor
/// (`Task {}` inherits actor context and nothing on either path is
/// `Task.detached`). Enqueuing one job of our own on that same executor and
/// awaiting it therefore runs strictly after each of those tasks' FIRST
/// segment — the segment that enqueues the write on the collaborator — so a
/// read issued afterwards is queued behind them.
///
/// Same two honest limits as ``drainControllerWrites(_:_:)``: same-priority
/// FIFO on the default actor executor is an implementation property rather than
/// a language guarantee, and it orders only tasks the gesture created
/// synchronously.
func drainOrchestratorEffects(_ orchestrator: SkipOrchestrator) async {
    _ = await orchestrator.activeWindowIDs()
}

/// Wait for the seam's receipts, then re-read behind a barrier so callers'
/// `count == expected` assertions bound the count from ABOVE as well as below.
///
/// The barrier is here for the same reason it is on the controller side:
/// polling `>= expected` is satisfied by the FIRST write, so a gesture that
/// issued TWO receipts — e.g. the `didLoseLifecycle` restructure the KNOWN GAP
/// in `revertByTimeRange` recommends, if it minted one per loop instead of one
/// per gesture — can land its second just after the poll returns and the
/// exactness assertion passes unobserved.
///
/// Honest calibration of how much this buys, since the file already carries one
/// over-claimed "verified by mutation" note: minting a second receipt over a
/// DIFFERENT span reddens all four callers with or without this barrier on a
/// quiescent machine — the second write simply wins the race in practice. What
/// the barrier removes is the dependence on winning it, which is the same thing
/// ``awaitControllerSampleCount(_:orchestrator:show:expected:)`` was given a
/// barrier for. (A second receipt over the SAME span is not observable at all
/// and needs no barrier: `appendCorrectionEvent` dedupes on
/// `analysisAssetId + effectiveCorrectionType + normalizedScopeKey +
/// correctionIdentityKey`, so production collapses it by construction.)
///
/// Two hops, because `persistManualCorrectionVeto` writes through the
/// correction store rather than straight to the AnalysisStore. Draining the
/// orchestrator gets each pending task as far as `correctionStore.recordVeto`;
/// awaiting any correction-store read that itself hits the AnalysisStore
/// (`correctionPassthroughFactor` does, unconditionally) then queues behind
/// their appends, so the final load sees all of them.
func awaitCorrectionReceipts(
    _ store: AnalysisStore,
    orchestrator: SkipOrchestrator,
    correctionStore: PersistentUserCorrectionStore,
    assetId: String,
    expected: Int
) async throws -> [CorrectionEvent] {
    var events: [CorrectionEvent] = []
    var landed = false
    for _ in 0..<200 {
        events = try await store.loadCorrectionEvents(analysisAssetId: assetId)
        if events.count >= expected {
            landed = true
            break
        }
        try await Task.sleep(nanoseconds: 50_000_000)
    }
    guard landed else {
        throw TestEffectTimeout(
            effect: "durable correction receipts for asset \"\(assetId)\"",
            expected: expected,
            observed: events.count
        )
    }
    await drainOrchestratorEffects(orchestrator)
    _ = await correctionStore.correctionPassthroughFactor(for: assetId)
    return try await store.loadCorrectionEvents(analysisAssetId: assetId)
}

/// Wait for the hard-negative ingest, then re-read behind the same barrier so
/// `count == expected` bounds from above too. `ingestNegativeFingerprint` is
/// one hop (the task's first segment enqueues on the bank), so draining the
/// orchestrator is sufficient here.
func awaitNegativeBankEntries(
    _ bank: NegativeFingerprintBank,
    orchestrator: SkipOrchestrator,
    expected: Int
) async throws -> [NegativeFingerprintEntry] {
    var entries: [NegativeFingerprintEntry] = []
    var landed = false
    for _ in 0..<200 {
        entries = try await bank.allEntries()
        if entries.count >= expected {
            landed = true
            break
        }
        try await Task.sleep(nanoseconds: 50_000_000)
    }
    guard landed else {
        throw TestEffectTimeout(
            effect: "hard-negative bank entries",
            expected: expected,
            observed: entries.count
        )
    }
    await drainOrchestratorEffects(orchestrator)
    return try await bank.allEntries()
}

/// A markOnly (suggest-tier) window: surfaced as a suggest banner, never
/// auto-skipped. Accepting one is the "we missed an ad" gesture; vetoing one is
/// a suggest-tier-only revert. Distinct enough from `makeSkipTestAdWindow` —
/// different eligibility gate, boundary state, and sub-auto-skip confidence —
/// to be worth its own factory rather than six more parameters on that one.
func makeSkipTestMarkOnlyWindow(
    id: String,
    assetId: String = "asset-1",
    startTime: Double = 60,
    endTime: Double = 120,
    // playhead-ynmk: a banner confirmation only skips when the derived per-edge
    // extent policy has a late-safe window, so callers that observe the
    // acceptance through `applied` / `wasSkipped` / a pushed cue must state the
    // extent they depend on. Default `.unanchored` — the field-case shape —
    // so anchoring is always an explicit, visible choice.
    startEdgeAnchor: AutoSkipEdgeAnchor = .unanchored,
    endEdgeAnchor: AutoSkipEdgeAnchor = .unanchored
) -> AdWindow {
    AdWindow(
        id: id,
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        confidence: 0.55,
        boundaryState: AdBoundaryState.segmentAggregated.rawValue,
        decisionState: AdDecisionState.candidate.rawValue,
        detectorVersion: "test-1",
        advertiser: nil, product: nil, adDescription: nil,
        evidenceText: nil, evidenceStartTime: startTime,
        metadataSource: "none",
        metadataConfidence: nil,
        metadataPromptVersion: nil,
        wasSkipped: false,
        userDismissedBanner: false,
        evidenceSources: nil,
        eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
        startEdgeAnchor: startEdgeAnchor.rawValue,
        endEdgeAnchor: endEdgeAnchor.rawValue
    )
}

/// Seeds one show's trust profile.
///
/// Split out of ``makeSkipTestTrustService(mode:trustScore:observations:falseSignals:)``
/// so a test that needs to READ the penalty back — or to seed a second show so
/// "the OTHER show was not penalised" is positively observable rather than
/// vacuous — can own the store. `recordFalseSkipSignal` never lazy-creates a
/// profile, so an unseeded show absorbs a misrouted penalty silently.
/// `podcastId` is deliberately NOT defaulted: seeding a second show is this
/// helper's whole reason for existing, so a silent wrong-show default is the
/// one mistake it must not make easy.
func seedSkipTestTrustProfile(
    in store: AnalysisStore,
    podcastId: String,
    mode: String,
    trustScore: Double,
    observations: Int,
    falseSignals: Int = 0
) async throws {
    try await store.upsertProfile(
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: trustScore,
            observationCount: observations,
            mode: mode,
            recentFalseSkipSignals: falseSignals
        )
    )
}

func makeSkipTestTrustService(
    mode: String,
    trustScore: Double,
    observations: Int,
    falseSignals: Int = 0
) async throws -> TrustScoringService {
    let trustStore = try await makeTestStore()
    try await seedSkipTestTrustProfile(
        in: trustStore,
        podcastId: "podcast-1",
        mode: mode,
        trustScore: trustScore,
        observations: observations,
        falseSignals: falseSignals
    )
    return TrustScoringService(store: trustStore)
}

// MARK: - AnalysisShard Factory

/// Creates a test AnalysisShard with sensible defaults. Silence samples are used
/// so the shard is lightweight yet passes any non-empty-sample checks.
func makeShard(
    id: Int = 0,
    episodeID: String = "test-ep",
    startTime: TimeInterval = 0,
    duration: TimeInterval = 30
) -> AnalysisShard {
    AnalysisShard(
        id: id,
        episodeID: episodeID,
        startTime: startTime,
        duration: duration,
        samples: [Float](repeating: 0, count: 16000 * Int(duration))
    )
}

// MARK: - Background download retry probe (playhead-7wia)

/// A `DownloadManager.sessionIO` double with **no bound to trip**, for a test
/// whose subject is the download BOOKKEEPING rather than the daemon.
///
/// `.neverAnswers` is answered by a synchronous `return nil` at the top of
/// `BackgroundSessionIO.perform` — before the work queue is touched and before
/// the deadline is armed. So a manager built with this one makes no XPC call,
/// waits on no timer, and shares no queue with anything.
///
/// ─────────────────────────────────────────────────────────────────────────
/// WHY, MEASURED — and the number was never the mechanism
/// ─────────────────────────────────────────────────────────────────────────
/// The default `BackgroundSessionIO.shared` is a process-wide SINGLETON with
/// ONE serial queue, and every `DownloadManager` that does not inject submits
/// `downloadTask(with:)`, `resume()` and the abandon-path `cancel()` onto it.
/// Four tests in the fast plan reach that queue, and in every preserved
/// full-plan log they fail together, always the same four:
///
///     downloadTask(with:) for ep-g2wq-no-blob   … did not answer within 10s
///     downloadTask(with:) for ep-g2wq-harvest   … did not answer within 10s
///     downloadTask(with:) for ep-stage-failure  … did not answer within 10s
///     downloadTask(with:) for kkzu-unattributed … did not answer within 10s
///
/// READ "ALWAYS THE SAME FOUR" AS DATED (playhead-et2d). It holds for the 7
/// pre-7wia logs of the 57-log window measured under ``unsharedSessionIO``,
/// and for none of the 44 after it. `kkzu-unattributed` no longer issues a
/// `downloadTask(with:)` at all, and `DownloadShowAttributionTests`' other
/// seven — on this queue the whole time, and missing from the list only
/// because they were being QUEUED rather than expiring — are private now.
///
/// and then, 50–63 SECONDS LATER, three of the four arrive on one thread
/// microseconds apart, in submission order:
///
///     downloadTask(with:) for ep-stage-failure:  reached the daemon queue
///         after its caller had already given up — not started
///
/// That last line is the whole diagnosis. Their bodies NEVER RAN. They were
/// queued behind the first one, whose `session.downloadTask(with:)` parked
/// inside `nsurlsessiond` for the better part of a minute while every
/// concurrent download test in the plan hammered the same background session
/// identifier. WIDENING THE BOUND WOULD NOT HAVE HELPED and would have made it
/// worse: at 60 s these calls are still queued, and the one test in the family
/// that carries `.timeLimit(.minutes(1))` would have traded an assertion
/// failure for a timeout. The fix is to stop being on that queue at all.
///
/// The PRODUCTION bound is not the thing to touch (playhead-nsjn /
/// playhead-gpdb / playhead-ola7): attaching to `nsurlsessiond` from the
/// cooperative pool is a process-wide deadlock with no timeout, no spinner and
/// no crash report. This is the DOUBLE. `timeout` is stated as the production
/// default rather than some large number precisely so that nobody reads this
/// helper as a widened bound — `.neverAnswers` never arms it.
func daemonSilentSessionIO(
    labelledFor test: String = #function
) -> BackgroundSessionIO {
    BackgroundSessionIO(
        behavior: .neverAnswers,
        timeout: BackgroundSessionIO.defaultTimeout,
        queueLabel: "7wia.test.\(test).\(UUID().uuidString)"
    )
}

/// A `DownloadManager.sessionIO` double that keeps the REAL daemon and the
/// production bound, and takes only the shared QUEUE away.
///
/// Use this — rather than ``daemonSilentSessionIO`` — for a test whose subject
/// needs a genuinely ADMITTED transfer. `.neverAnswers` cannot serve those: all
/// THREE no-answer branches of `backgroundDownload` release the reservation and
/// DELETE the attribution sidecar, which is the very record such a test reads.
///
/// ─────────────────────────────────────────────────────────────────────────
/// WHAT IT CHANGES AND WHAT IT DELIBERATELY DOES NOT (playhead-et2d)
/// ─────────────────────────────────────────────────────────────────────────
/// `behavior` and `timeout` are the production values, stated rather than
/// defaulted so nobody can read this helper as a widened bound — widening is
/// what playhead-nsjn / playhead-gpdb / playhead-ola7 own, and 7wia measured
/// that at 60 s these calls are still queued, so a wider bound trades an
/// assertion failure for a timeout. The ONLY difference from
/// `BackgroundSessionIO.shared` is `queueLabel`: which serial queue the call
/// is submitted to.
///
/// `.shared` is a process-wide singleton with ONE serial queue, and every
/// `DownloadManager` that does not inject submits `downloadTask(with:)`,
/// `resume()` and the abandon-path `cancel()` onto it. MEASURED over 57
/// de-duplicated full-plan logs, 2026-08-15 … 08-24, THAT COUPLING HAS BITTEN
/// IN BOTH DIRECTIONS:
///
///   - INWARD, in the 7 logs before playhead-7wia landed (2026-08-18 23:00):
///     `ForceQuitResumeTests`' `ep-g2wq-no-blob` parked the queue and this
///     suite's `kkzu-unattributed` was one of three transfers whose bodies
///     never ran. It cost no verdict — the arm reading that transfer asserted
///     an ABSENCE, which a deleted sidecar produces just as well.
///   - OUTWARD, in 1 of the 44 logs after: 2026-08-23 08:06, this suite's own
///     `downloadTask(with:) for kkzu-cleared` parked inside `nsurlsessiond`
///     and held the queue 65 s. Six sibling `downloadTask(with:)` calls
///     expired behind it inside 17 ms (08:06:01.288–.305) and a seventh,
///     `kkzu-unattributed`, 10.1 s later. At 08:06:56.63 — when the parked
///     body returned, 55 s after the six and 45 s after the seventh —
///     SEVENTEEN submissions covering TWELVE transfers across THREE suites
///     drained at once, each logging `reached the daemon queue after its
///     caller had already given up — not started`. ELEVEN tests failed: this
///     suite's seven, two in `StreamingDownloadTests`, two in
///     `ForceQuitResumeTests`.
///
/// THE OUTWARD FIVE ARE A DIFFERENT SHAPE FROM THE KKZU SEVEN, and the
/// seventeen only adds up once you see it. `ambiguous-legacy-siblings`,
/// `incomplete-background-retry`, `ep-res`, `ep-fresh` and `ep-rotated` carry
/// TWO of those lines each — a `resume()` and then the abandon-path `cancel
/// unstarted transfer` — and their `downloadTask(with:)` HAD been answered:
/// the log says `was created but not resumed`. Seven + five + five = 17. The
/// park cost those five a STARTED transfer, not a refused one.
///
/// WHETHER A PRIVATE QUEUE WOULD HAVE SAVED ANY OF THE ELEVEN IS UNPROVEN and
/// that log argues both ways. AGAINST: fourteen `allTasks` calls, already on
/// per-manager private queues, blew the same 10 s bound inside
/// 08:06:01–08:06:12 with their bodies having RUN. FOR: the one rail in the
/// plan whose pass depends on `nsurlsessiond` genuinely answering —
/// `BackgroundDownloadDropLedgerTests.aHealthyDownloadWritesNothing`, on a
/// private queue — PASSED, logging `Queued background download for ep-healthy`
/// at 08:05:52.60, 1.3 s after the shared queue stopped draining anything and
/// 64 s before it resumed. DO NOT compress that into "one `Queued background
/// download` line in 14 MB, so the daemon was answering nobody": that window's
/// download tests are dominated by `neverAnswers` / `refusesCallsLabelled`
/// seams DESIGNED never to log it, so the count is a success rate over a
/// population selected to fail.
///
/// So read a green `DownloadShowAttributionTests` as evidence the daemon
/// answered, not as evidence this helper worked. What the helper buys needs no
/// counterfactual: this suite can no longer refuse twelve transfers it has
/// nothing to do with, and can no longer be refused by somebody else's park.
/// The hazard is not cleared — 120 of the 144 `DownloadManager(` constructions
/// in `PlayheadTests` still take the default `.shared` (`DownloadManagerTests`
/// 43, `ForceQuitResumeTests` 19, `StreamingDownloadTests` 18,
/// `PlaceholderAssetUpgradeTests` 11) — this suite just stops being one.
///
/// Three things it does NOT do, each measured rather than argued:
///
///   - It does not make a call immune to a slow daemon. `allTasks for …` has
///     run on a per-manager PRIVATE queue since playhead-rouw
///     (`enumerationIO`, built by `onItsOwnQueue`) and still blows the 10 s
///     bound 137 times across 35 of these 57 logs — with ZERO "already given
///     up" lines anywhere in the 57, so no queue was holding those bodies
///     back. COUNT EXPIRIES OF THIS BOUND: there are 138 `allTasks` expiry
///     lines and the 138th reads `within 30.000000s`, from
///     `BackgroundDownloadDropLedgerTests.answeringIO()`, a double that widens
///     it on purpose. A private queue is not a shorter answer, only an
///     unshared wait.
///   - It does not separate two calls made by the SAME manager: they share one
///     `BackgroundSessionIO` instance and therefore one queue.
///   - It is not what playhead-3rql's EXP2 measured. That run set
///     `"parallelizable": false`, which removes the whole plan's concurrency —
///     including the seven live transfers `DownloadShowAttributionTests` itself
///     starts against a non-resolving host — so it cannot isolate the queue.
///     Nor is its `passed after 0.102 seconds` comparable to a parallel run's
///     figure: on a parallel plan that number is enqueue-to-completion, and
///     ~90 % of PASSING tests report over 60 s (CLAUDE.md).
func unsharedSessionIO(
    labelledFor test: String = #function
) -> BackgroundSessionIO {
    BackgroundSessionIO(
        behavior: .dedicatedThread,
        timeout: BackgroundSessionIO.defaultTimeout,
        queueLabel: "et2d.test.\(test).\(UUID().uuidString)"
    )
}

/// Collects what a `DownloadManager` wrote to the surface-status invariant
/// stream. In production that closure is
/// `SurfaceStatusInvariantLogger.invariantViolated`.
final class RecordedInvariantViolations: @unchecked Sendable {
    private let entries = OSAllocatedUnfairLock<
        [(code: InvariantViolation.Code, description: String)]
    >(initialState: [])

    var recorder: @Sendable (InvariantViolation.Code, String) -> Void {
        { [entries] code, description in
            entries.withLock { $0.append((code, description)) }
        }
    }

    /// Descriptions of every recorded background-session refusal.
    var sessionRefusals: [String] {
        entries.withLock { list in
            list.filter { $0.code == .backgroundSessionCreationRefused }
                .map(\.description)
        }
    }

    /// Refusals raised by one request site — the sites are
    /// `DownloadManager.BackgroundSessionRequestSite` raw values, e.g.
    /// `background_download`.
    func sessionRefusals(from site: String) -> [String] {
        sessionRefusals.filter { $0.contains("site=\(site)") }
    }

    /// playhead-7dgx: descriptions of every abandoned background download
    /// whose durable row could NOT be written. This is the SECOND medium the
    /// drop ledger falls back on, so a test that asserts the database is empty
    /// must be able to assert this is not.
    var unrecordedDrops: [String] {
        descriptions(of: .backgroundDownloadDropNotRecorded)
    }

    /// Every description recorded under one code. Kept general so the next
    /// code added here does not need a third bespoke accessor.
    func descriptions(of code: InvariantViolation.Code) -> [String] {
        entries.withLock { list in
            list.filter { $0.code == code }.map(\.description)
        }
    }
}

/// The retry probe three download terminal-failure tests share: after a
/// transfer has failed terminally, a fresh `backgroundDownload` for the same
/// episode must get PAST the in-flight guard.
///
/// ─────────────────────────────────────────────────────────────────────────
/// WHAT IT ASSERTS, AND WHY IT NO LONGER ASSERTS A COUNTER (playhead-7wia)
/// ─────────────────────────────────────────────────────────────────────────
/// All three tests used to read `_backgroundDownloadAdmissionCountForTesting`
/// going up as "the call did its job". It is not the same claim.
/// `backgroundDownload` increments that counter only AFTER two guarded early
/// returns, and both of them are the product working as designed: an
/// unanswered daemon is a documented, deliberate REFUSAL — the reservation is
/// released, the attribution sidecar is dropped, and the episode stays
/// retryable. So the old assertion could not tell "the retry guard is stuck"
/// from "the daemon refused", and on a loaded box it reported the second as
/// the first, on every merge gate, for long enough to be written into
/// `scripts/gate-baseline.PlayheadFastTests.json` as three permanent entries.
///
/// The OUTCOME these tests actually want is one step earlier and is
/// daemon-free: did the call REACH the daemon crossing? With a
/// `daemonSilentSessionIO` manager the crossing is refused synchronously and
/// `backgroundSession(for:requestedBy:)` RECORDS the refusal on the
/// surface-status invariant stream, tagged with the request site. So:
///
///   * one new `site=background_download` refusal → the guard released and
///     the retry ran (what the test wants to prove);
///   * none, and the episode still holds its in-flight slot → the guard is
///     stuck, which is the regression these tests exist for;
///   * none, and it does not → the call returned at one of the earlier guards
///     (already cached, foreground stream active).
///
/// The admission count is kept as a VACUITY CONTROL rather than as the
/// verdict: `.neverAnswers` cannot admit anything, so a count that moved means
/// the manager was built without the double and the test has quietly gone back
/// to racing the real `nsurlsessiond`.
func expectRetryReachesTheDaemon(
    _ manager: DownloadManager,
    episodeId: String,
    violations: RecordedInvariantViolations,
    _ because: String,
    sourceLocation: SourceLocation = #_sourceLocation
) async {
    let admissionsBefore =
        await manager._backgroundDownloadAdmissionCountForTesting()
    let refusalsBefore =
        violations.sessionRefusals(from: "background_download").count

    await manager.backgroundDownload(
        episodeId: episodeId,
        from: URL(string: "https://example.invalid/retry.mp3")!,
        context: .unattributed(reason: .testHarness, isExplicitDownload: false)
    )

    let refusals = violations.sessionRefusals(from: "background_download")
    let stillHeld = await manager._isBackgroundDownloadInFlightForTesting(
        episodeId: episodeId
    )
    let admissionsAfter =
        await manager._backgroundDownloadAdmissionCountForTesting()

    #expect(
        refusals.count == refusalsBefore + 1,
        """
        \(because) — the retry for \(episodeId) never reached the daemon \
        crossing. \(refusals.count - refusalsBefore) refusals were recorded \
        for site=background_download, expected 1; the episode \
        \(stillHeld ? "STILL HOLDS" : "does not hold") its in-flight slot. \
        Still holding it means the guard was not released, which is the \
        regression this test exists for; not holding it means the call \
        returned at an earlier guard.
        """,
        sourceLocation: sourceLocation
    )
    #expect(
        admissionsAfter == admissionsBefore,
        """
        VACUITY CONTROL: this manager is built with `daemonSilentSessionIO`, \
        which refuses every crossing synchronously, so nothing can ever be \
        admitted. An admission count that moved from \(admissionsBefore) to \
        \(admissionsAfter) means the double is not installed and this test is \
        racing the real background transfer daemon again.
        """,
        sourceLocation: sourceLocation
    )
}
