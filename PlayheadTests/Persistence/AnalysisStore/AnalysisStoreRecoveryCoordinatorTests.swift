// AnalysisStoreRecoveryCoordinatorTests.swift
// What happens when the analysis database will not open (playhead-wvdz).
//
// The regression these tests exist for, stated plainly: a thrown
// `AnalysisStore.migrate()` used to be answered by
//
//     try? FileManager.default.removeItem(at: AnalysisStore.defaultDirectory())
//
// followed by a retry that succeeded BECAUSE the directory was now
// empty. The listener's entire analysis library — including corrections
// they made by hand, of which there is no cloud copy — was destroyed
// silently, and the app launched looking like a fresh install.
//
// Every test below uses a REAL `AnalysisStore` on a real temp directory
// with real rows in it, and a real thrown migration, because the claim
// under test is "the rows are still there afterwards" and a stubbed
// store cannot make that claim. The failure is injected the way the
// existing V39/V40 migration tests inject one — a `BEFORE … RAISE(ABORT)`
// trigger plus a rewound `_meta.schema_version` — so the thrown error
// travels the production `runSchemaMigration` transaction rather than a
// test-only path.

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisStoreRecoveryCoordinator (playhead-wvdz)")
struct AnalysisStoreRecoveryCoordinatorTests {

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    // MARK: - Fixture

    /// A store directory carrying a row we can count afterwards, plus a
    /// trigger that makes the next `migrate()` throw from inside the
    /// production migration transaction.
    ///
    /// The trigger fires on `_meta` writes, which every rung performs via
    /// `setSchemaVersion`. That is deliberately NOT the v39
    /// `analysis_assets` DELETE the bead describes: playhead-0hi9 has
    /// since wrapped v39 in a SAVEPOINT that swallows its own failure, so
    /// a trigger there would no longer throw and the test would prove
    /// nothing. What is under test here is the RECOVERY POLICY for any
    /// throwing rung, so the injection point only has to produce a real
    /// throw from a real rung.
    private struct SabotagedStore {
        let directory: URL
        let assetId: String
    }

    private func makeSabotagedStore() async throws -> SabotagedStore {
        // The store gets its OWN parent directory, mirroring production
        // (`…/Playhead/AnalysisStore/`). Two reasons, and the second is
        // not cosmetic: quarantine creates a SIBLING of the store
        // directory, and `makeTempDir` hands every test a child of one
        // shared scratch root — so a shared parent would make one test's
        // quarantine visible to another test's "nothing was moved"
        // assertion, which is exactly how that assertion first passed
        // for the wrong reason.
        let parent = try makeTempDir(prefix: "AnalysisStoreRecovery")
        let dir = parent.appendingPathComponent("AnalysisStore", isDirectory: true)
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()

        // Real user data. `analysis_assets` is the parent of ad_windows,
        // transcript_chunks, correction_events and training_examples, so
        // its survival is a proxy for the whole library's.
        let assetId = try await seedAsset(bootstrap)
        #expect(try probeRowCount(in: dir, table: "analysis_assets") == 1)

        // Rewind FIRST, so the ladder has work to do on the next open and
        // therefore a `setSchemaVersion` to attempt. Order matters: the
        // trigger below would abort this write too.
        try await bootstrap.setMetaValue(forKey: "schema_version", value: "1")

        // Sabotage: any `_meta` write now aborts.
        try await bootstrap.execForTesting("""
            CREATE TRIGGER wvdz_meta_guard BEFORE INSERT ON _meta
            BEGIN SELECT RAISE(ABORT, 'wvdz recovery fixture'); END
            """)

        return SabotagedStore(directory: dir, assetId: assetId)
    }

    private func seedAsset(_ store: AnalysisStore) async throws -> String {
        let assetId = UUID().uuidString
        try await store.execForTesting("""
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, createdAt)
            VALUES ('\(assetId)', 'wvdz-episode-\(assetId)', 'wvdz-fingerprint', 'https://example.com/a.mp3', 0)
            """)
        return assetId
    }

    /// A fresh `AnalysisStore` over the same directory — what the next
    /// launch does. `resetMigratedPathsForTesting` is required: the
    /// process-global cache would otherwise short-circuit
    /// `runSchemaMigration` and the migrate would not run at all.
    private func relaunchMigrate(_ dir: URL) -> @Sendable () async throws -> Void {
        { @Sendable in
            AnalysisStore.resetMigratedPathsForTesting()
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
        }
    }

    private func makeCoordinator(
        storeDirectory: URL
    ) throws -> AnalysisStoreRecoveryCoordinator {
        let journalDir = try makeTempDir(prefix: "AnalysisStoreRecoveryJournal")
        return AnalysisStoreRecoveryCoordinator(
            journal: AnalysisStoreHealthJournal(directory: journalDir),
            storeDirectory: { storeDirectory }
        )
    }

    // MARK: - The fixture is real

    /// Guards the whole suite against becoming vacuous. If the sabotage
    /// stopped producing a throw, every "the data survived" test below
    /// would pass for the wrong reason.
    @Test("The injected failure really does make migrate() throw")
    func sabotageActuallyThrows() async throws {
        let fixture = try await makeSabotagedStore()
        await #expect(throws: (any Error).self) {
            try await self.relaunchMigrate(fixture.directory)()
        }
    }

    // MARK: - 1. A thrown migration does not destroy the store

    /// THE REGRESSION TEST. Against the pre-wvdz code this fails: the
    /// directory is removed and the row count goes to zero.
    @Test("A thrown migration leaves the store directory and its rows completely intact")
    func thrownMigrationDoesNotDestroyTheStore() async throws {
        let fixture = try await makeSabotagedStore()
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)

        let outcome = await coordinator.openAtLaunch(
            relaunchMigrate(fixture.directory), now: Self.t0
        )

        #expect(outcome == .willRetryOnNextLaunch(attempt: 1))
        #expect(FileManager.default.fileExists(atPath: fixture.directory.path))
        #expect(
            FileManager.default.fileExists(
                atPath: fixture.directory.appendingPathComponent("analysis.sqlite").path
            )
        )
        // The row is the point. A surviving-but-empty database would be
        // the same loss with a different shape.
        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 1)
    }

    @Test("Repeated failures still destroy nothing, right through the escalation threshold")
    func repeatedFailuresDestroyNothing() async throws {
        let fixture = try await makeSabotagedStore()
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)

        for _ in 0..<(AnalysisStoreHealthJournal.failuresBeforeAskingListener + 3) {
            _ = await coordinator.openAtLaunch(
                relaunchMigrate(fixture.directory), now: Self.t0
            )
        }

        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 1)
        // And no quarantine was created either — the app does not move
        // the store on its own any more than it deletes it.
        let siblings = try FileManager.default.contentsOfDirectory(
            atPath: fixture.directory.deletingLastPathComponent().path
        )
        #expect(
            !siblings.contains {
                $0.hasPrefix(AnalysisStoreRecoveryCoordinator.quarantineDirectoryPrefix)
            }
        )
    }

    // MARK: - 2. A transient failure resolves by retrying

    /// The property the bead calls for: a failure that goes away must be
    /// absorbed by a later launch rather than escalating. It works
    /// because `runSchemaMigration` rolls its whole ladder back, so
    /// `_meta.schema_version` is untouched and the retry is meaningful.
    @Test("A transient failure resolves on a later launch, with the data still there")
    func transientFailureResolvesOnRetry() async throws {
        let fixture = try await makeSabotagedStore()
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)

        let first = await coordinator.openAtLaunch(
            relaunchMigrate(fixture.directory), now: Self.t0
        )
        #expect(first == .willRetryOnNextLaunch(attempt: 1))

        // The transient condition clears.
        AnalysisStore.resetMigratedPathsForTesting()
        let repairStore = try AnalysisStore(directory: fixture.directory)
        try await repairStore.openWithoutSchemaMigrationForTesting()
        try await repairStore.execForTesting("DROP TRIGGER wvdz_meta_guard")

        let second = await coordinator.openAtLaunch(
            relaunchMigrate(fixture.directory), now: Self.t0
        )
        #expect(second == .opened)
        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 1)

        let state = await coordinator.currentState()
        #expect(state.status == .healthy)
        #expect(state.consecutiveFailureCount == 0)
    }

    // MARK: - 3. Escalation hands the decision over

    @Test("Escalation reaches awaitingListenerDecision and stops there")
    func escalationStopsAndAsks() async throws {
        let fixture = try await makeSabotagedStore()
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)
        let threshold = AnalysisStoreHealthJournal.failuresBeforeAskingListener

        var outcomes: [AnalysisStoreLaunchOutcome] = []
        for _ in 0..<threshold {
            outcomes.append(
                await coordinator.openAtLaunch(relaunchMigrate(fixture.directory), now: Self.t0)
            )
        }

        #expect(outcomes.dropLast().allSatisfy { if case .willRetryOnNextLaunch = $0 { true } else { false } })
        #expect(outcomes.last == .awaitingListenerDecision(attempt: threshold))
        #expect(await coordinator.currentState().status == .awaitingUserDecision)
        // Still nothing destroyed at the moment of asking — the question
        // is asked BEFORE anything happens, not after.
        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 1)
    }

    // MARK: - 4. The listener-chosen paths

    @Test("An explicit retry clears the counter and changes nothing on disk")
    func listenerRetryIsNonDestructive() async throws {
        let fixture = try await makeSabotagedStore()
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)
        for _ in 0..<AnalysisStoreHealthJournal.failuresBeforeAskingListener {
            _ = await coordinator.openAtLaunch(relaunchMigrate(fixture.directory), now: Self.t0)
        }

        let outcome = await coordinator.retryAtListenerRequest(
            relaunchMigrate(fixture.directory), now: Self.t0
        )

        // It failed again — but from a cleared counter, so the listener
        // is not immediately re-prompted, and the data is untouched.
        #expect(outcome == .willRetryOnNextLaunch(attempt: 1))
        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 1)
    }

    /// "Start fresh" is the only destructive-looking action, and it is
    /// not destructive: the old store is MOVED and stays readable.
    @Test("Start fresh quarantines rather than deletes, and the data is still recoverable")
    func startFreshQuarantinesAndTheDataSurvives() async throws {
        let fixture = try await makeSabotagedStore()
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)

        let outcome = try await coordinator.quarantineAndRebuild(
            relaunchMigrate(fixture.directory), now: Self.t0
        )
        #expect(outcome == .opened)

        // A working, empty store now lives at the live path.
        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 0)

        // And the old one is still on disk, under a recorded name.
        let state = await coordinator.currentState()
        let quarantine = try #require(state.quarantines.last)
        #expect(quarantine.directoryName.hasPrefix(AnalysisStoreRecoveryCoordinator.quarantineDirectoryPrefix))
        #expect(quarantine.byteCount > 0)

        let quarantinedDir = fixture.directory
            .deletingLastPathComponent()
            .appendingPathComponent(quarantine.directoryName, isDirectory: true)
        #expect(FileManager.default.fileExists(atPath: quarantinedDir.path))

        // RECOVERABLE, not merely present: the row is readable out of the
        // quarantined database. "Moved aside" would be an empty promise
        // if the bytes could not be read back.
        #expect(try probeRowCount(in: quarantinedDir, table: "analysis_assets") == 1)
    }

    /// A quarantine that could degrade into a delete when the move fails
    /// — disk full, a read-only container — would reintroduce exactly the
    /// bug this bead removes, at the worst possible moment.
    @Test("A failed quarantine reports the failure and changes nothing — it never falls back to deleting")
    func failedQuarantineDoesNotFallBackToDeleting() async throws {
        let parent = try makeTempDir(prefix: "AnalysisStoreRecoveryUnmovable")
        let live = parent.appendingPathComponent("AnalysisStore", isDirectory: true)
        try FileManager.default.createDirectory(at: live, withIntermediateDirectories: true)
        let payloadURL = live.appendingPathComponent("analysis.sqlite")
        try Data("payload".utf8).write(to: payloadURL)

        // Deny writes on the parent, so no sibling directory can be
        // created and `moveItem` must fail.
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o500], ofItemAtPath: parent.path
        )
        defer {
            try? FileManager.default.setAttributes(
                [.posixPermissions: 0o700], ofItemAtPath: parent.path
            )
        }

        let coordinator = AnalysisStoreRecoveryCoordinator(
            journal: AnalysisStoreHealthJournal(
                directory: try makeTempDir(prefix: "AnalysisStoreRecoveryJournalUnmovable")
            ),
            storeDirectory: { live }
        )

        await #expect(throws: AnalysisStoreRecoveryError.self) {
            // The migrate closure must never run: a failed quarantine
            // has to stop before it rebuilds anything.
            try await coordinator.quarantineAndRebuild(
                { Issue.record("migrate ran after a failed quarantine") },
                now: Self.t0
            )
        }

        #expect(FileManager.default.fileExists(atPath: live.path))
        #expect(try Data(contentsOf: payloadURL) == Data("payload".utf8))
        #expect(await coordinator.currentState().quarantines.isEmpty)
    }

    @Test("Start fresh on a device with no store yet is a no-op that still opens")
    func startFreshWithNoExistingStore() async throws {
        let parent = try makeTempDir(prefix: "AnalysisStoreRecoveryAbsent")
        let live = parent.appendingPathComponent("AnalysisStore", isDirectory: true)
        let coordinator = AnalysisStoreRecoveryCoordinator(
            journal: AnalysisStoreHealthJournal(
                directory: try makeTempDir(prefix: "AnalysisStoreRecoveryJournalAbsent")
            ),
            storeDirectory: { live }
        )

        let outcome = try await coordinator.quarantineAndRebuild(
            relaunchMigrate(live), now: Self.t0
        )
        #expect(outcome == .opened)
        // Nothing was moved, so nothing is recorded as moved. A phantom
        // quarantine record would point at a directory that does not
        // exist.
        #expect(await coordinator.currentState().quarantines.isEmpty)
    }

    // MARK: - 5. The overloads production actually calls

    // Everything above drives the CLOSURE overload. Production calls the
    // three `AnalysisStore`-taking ones, and the difference is not
    // cosmetic: the closure the tests pass builds a brand-new store on
    // every invocation, so the suite structurally cannot observe a store
    // instance carrying `didOpen == true` across a recovery. That is
    // exactly the state the stale-snapshot hazard lives in.

    @Test("The store overload opens a healthy store and records success")
    func storeOverloadOpensHealthyStore() async throws {
        let parent = try makeTempDir(prefix: "AnalysisStoreRecoveryOverload")
        let dir = parent.appendingPathComponent("AnalysisStore", isDirectory: true)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        let coordinator = try makeCoordinator(storeDirectory: dir)

        let outcome = await coordinator.openAtLaunch(store, now: Self.t0)
        #expect(outcome == .opened)
        #expect(await store.isOpen)
        #expect(await coordinator.currentState().status == .healthy)
    }

    @Test("The store overload records a real thrown migration without destroying anything")
    func storeOverloadRecordsFailure() async throws {
        let fixture = try await makeSabotagedStore()
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: fixture.directory)
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)

        let outcome = await coordinator.openAtLaunch(store, now: Self.t0)
        #expect(outcome == .willRetryOnNextLaunch(attempt: 1))
        // `ensureOpen` rolls `didOpen` back and closes the handle on a
        // failed migration, which is what makes the quarantine guard
        // below meaningful.
        #expect(await store.isOpen == false)
        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 1)
    }

    /// THE STALE-SNAPSHOT HAZARD. The Settings row is rendered from a
    /// snapshot of the health document and can be minutes old. If the
    /// store opens in the meantime — the launch attempt finishing, or any
    /// other caller re-entering `ensureOpen()` — and the listener then
    /// taps "Start fresh", moving the directory would rename it out from
    /// under a live SQLite handle. POSIX `rename` leaves descriptors
    /// valid, so the session would keep writing into the quarantine while
    /// being told it had started fresh, and the next launch would strand
    /// those writes behind a new empty store.
    @Test("Start fresh declines when the store is open, so a stale view cannot move a live database")
    func startFreshDeclinesWhileTheStoreIsOpen() async throws {
        let parent = try makeTempDir(prefix: "AnalysisStoreRecoveryStale")
        let dir = parent.appendingPathComponent("AnalysisStore", isDirectory: true)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        _ = try await seedAsset(store)
        #expect(await store.isOpen)

        // The journal still says the app gave up — the stale view the
        // listener is looking at.
        let journalDir = try makeTempDir(prefix: "AnalysisStoreRecoveryStaleJournal")
        let journal = AnalysisStoreHealthJournal(directory: journalDir)
        for _ in 0..<AnalysisStoreHealthJournal.failuresBeforeAskingListener {
            await journal.recordFailure(
                error: AnalysisStoreError.migrationFailed("FOREIGN KEY constraint failed"),
                now: Self.t0
            )
        }
        #expect(await journal.load().status == .awaitingUserDecision)

        let coordinator = AnalysisStoreRecoveryCoordinator(
            journal: journal, storeDirectory: { dir }
        )
        let outcome = try await coordinator.quarantineAndRebuild(store, now: Self.t0)

        // Reports the truth rather than a fiction.
        #expect(outcome == .opened)
        // NOTHING MOVED. The live directory still holds the real data.
        #expect(try probeRowCount(in: dir, table: "analysis_assets") == 1)
        let siblings = try FileManager.default.contentsOfDirectory(atPath: parent.path)
        #expect(
            !siblings.contains {
                $0.hasPrefix(AnalysisStoreRecoveryCoordinator.quarantineDirectoryPrefix)
            }
        )
        // And the stale prompt clears, so the listener is not asked again
        // about a store that is working.
        let state = await coordinator.currentState()
        #expect(state.status == .healthy)
        #expect(state.quarantines.isEmpty)
    }

    /// The guard must not block a genuine recovery: when the store is
    /// really shut, "start fresh" still works.
    @Test("Start fresh still proceeds through the store overload when the store is genuinely shut")
    func startFreshProceedsThroughStoreOverloadWhenClosed() async throws {
        let fixture = try await makeSabotagedStore()
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: fixture.directory)
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)

        _ = await coordinator.openAtLaunch(store, now: Self.t0)
        #expect(await store.isOpen == false)

        let outcome = try await coordinator.quarantineAndRebuild(store, now: Self.t0)
        #expect(outcome == .opened)
        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 0)

        let quarantine = try #require(await coordinator.currentState().quarantines.last)
        let quarantinedDir = fixture.directory
            .deletingLastPathComponent()
            .appendingPathComponent(quarantine.directoryName, isDirectory: true)
        #expect(try probeRowCount(in: quarantinedDir, table: "analysis_assets") == 1)
    }

    @Test("The retry overload clears the counter and re-attempts against the same store")
    func retryOverloadReattempts() async throws {
        let fixture = try await makeSabotagedStore()
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: fixture.directory)
        let coordinator = try makeCoordinator(storeDirectory: fixture.directory)

        for _ in 0..<AnalysisStoreHealthJournal.failuresBeforeAskingListener {
            _ = await coordinator.openAtLaunch(store, now: Self.t0)
        }
        #expect(await coordinator.currentState().status == .awaitingUserDecision)

        let outcome = await coordinator.retryAtListenerRequest(store, now: Self.t0)
        #expect(outcome == .willRetryOnNextLaunch(attempt: 1))
        #expect(try probeRowCount(in: fixture.directory, table: "analysis_assets") == 1)
    }

    // MARK: - 6. Nothing else on the launch path destroys anything

    /// A successful open must not touch the directory either — an easy
    /// regression to introduce while "tidying up" the recovery path.
    @Test("A successful open leaves the store exactly as it found it")
    func successfulOpenTouchesNothing() async throws {
        // Own parent, for the sibling-scan reason in `makeSabotagedStore`.
        let parent = try makeTempDir(prefix: "AnalysisStoreRecoveryHealthy")
        let dir = parent.appendingPathComponent("AnalysisStore", isDirectory: true)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        _ = try await seedAsset(store)

        let coordinator = try makeCoordinator(storeDirectory: dir)
        let outcome = await coordinator.openAtLaunch(relaunchMigrate(dir), now: Self.t0)

        #expect(outcome == .opened)
        #expect(try probeRowCount(in: dir, table: "analysis_assets") == 1)
        let siblings = try FileManager.default.contentsOfDirectory(
            atPath: dir.deletingLastPathComponent().path
        )
        #expect(
            !siblings.contains {
                $0.hasPrefix(AnalysisStoreRecoveryCoordinator.quarantineDirectoryPrefix)
            }
        )
    }
}
