// AnalysisStoreHealthJournalTests.swift
// The durable record of whether the analysis database opened
// (playhead-wvdz, observability half).
//
// What these tests defend, in order of how expensive the regression
// would be:
//
//   1. THE COUNTER IS AUTHORITATIVE ACROSS PROCESSES. It is the only
//      input to the decision to stop retrying and ask the listener what
//      to do, and that decision is the only route to any destructive
//      action. A counter that silently resets means the listener is
//      never asked; a counter that double-counts means they are asked
//      about a store that was fine.
//   2. AN UNREADABLE DOCUMENT IS NOT AN EMPTY ONE. On iOS this is not
//      hypothetical — Data Protection makes it a routine background-launch
//      condition — and treating it as empty would clobber real history.
//   3. THE CLASSIFICATION IS RIGHT WHERE IT IS LOAD-BEARING. Exactly one
//      class (`accessDenied`) changes behaviour rather than only
//      annotating it, so exactly that one is pinned.

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisStoreHealthJournal (playhead-wvdz)")
struct AnalysisStoreHealthJournalTests {

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    private func makeJournal() throws -> (AnalysisStoreHealthJournal, URL) {
        let dir = try makeTempDir(prefix: "AnalysisStoreHealth")
        return (AnalysisStoreHealthJournal(directory: dir), dir)
    }

    /// A `migrationFailed` carrying the message SQLite actually produces
    /// for the failure documented in the bead: playhead-0hi9's v39 rung
    /// issued a bare `DELETE FROM analysis_assets` against rows
    /// referenced by `training_examples.analysisAssetId ON DELETE
    /// RESTRICT`.
    private static let foreignKeyFailure = AnalysisStoreError.migrationFailed(
        "FOREIGN KEY constraint failed (SQL: DELETE FROM analysis_assets WHERE id = ?)"
    )

    // MARK: - Baseline

    @Test("A device that has never run reports healthy, not unknown")
    func freshDeviceIsHealthy() async throws {
        let (journal, _) = try makeJournal()
        let state = await journal.load()
        #expect(state == .healthy)
        #expect(state.consecutiveFailureCount == 0)
        #expect(state.lastSuccessAt == nil)
    }

    // MARK: - The counter

    @Test("Consecutive failures escalate to awaitingUserDecision at the threshold, not before")
    func failuresEscalateAtThreshold() async throws {
        let (journal, _) = try makeJournal()

        let first = await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)
        #expect(first.consecutiveFailureCount == 1)
        #expect(first.status == .retrying)

        let second = await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)
        #expect(second.consecutiveFailureCount == 2)
        #expect(second.status == .retrying)

        let third = await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)
        #expect(third.consecutiveFailureCount == 3)
        #expect(third.status == .awaitingUserDecision)
        #expect(third.consecutiveFailureCount == AnalysisStoreHealthJournal.failuresBeforeAskingListener)
    }

    /// The property the whole design rests on: the counter has to
    /// survive process death, because each retry costs a whole launch.
    /// An in-memory counter would reset on every launch and the listener
    /// would never be asked anything.
    @Test("The counter survives a new journal instance over the same directory")
    func counterSurvivesRelaunch() async throws {
        let (journal, dir) = try makeJournal()
        await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)
        await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)

        let relaunched = AnalysisStoreHealthJournal(directory: dir)
        let state = await relaunched.load()
        #expect(state.consecutiveFailureCount == 2)
        #expect(state.status == .retrying)

        let third = await relaunched.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)
        #expect(third.status == .awaitingUserDecision)
    }

    @Test("Success clears the counter but keeps the forensic history")
    func successClearsCounterAndKeepsHistory() async throws {
        let (journal, _) = try makeJournal()
        await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)
        await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)

        let recovered = await journal.recordSuccess(now: Self.t0.addingTimeInterval(60))
        #expect(recovered.status == .healthy)
        #expect(recovered.consecutiveFailureCount == 0)
        #expect(recovered.firstFailureAt == nil)
        #expect(recovered.lastSuccessAt == Self.t0.addingTimeInterval(60))
        // A store that failed twice and then recovered is exactly the
        // history a support engineer needs. Clearing it on success would
        // make the incident unreconstructable the moment it resolved.
        #expect(recovered.recentFailures.count == 2)
        #expect(recovered.lastFailureAt == Self.t0)
    }

    @Test("An explicit listener retry clears the counter without claiming success")
    func listenerRetryClearsCounterWithoutSuccess() async throws {
        let (journal, _) = try makeJournal()
        for _ in 0..<3 { await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0) }
        #expect(await journal.load().status == .awaitingUserDecision)

        let retried = await journal.recordListenerRequestedRetry()
        #expect(retried.consecutiveFailureCount == 0)
        #expect(retried.status == .retrying)
        // Nothing has actually succeeded — claiming otherwise would make
        // the bundle report a healthy store that has never opened.
        #expect(retried.lastSuccessAt == nil)
    }

    @Test("The failure list is bounded")
    func failureListIsBounded() async throws {
        let (journal, _) = try makeJournal()
        let overflow = AnalysisStoreHealthState.maxFailureRecords + 7
        for _ in 0..<overflow {
            await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)
        }
        let state = await journal.load()
        #expect(state.recentFailures.count == AnalysisStoreHealthState.maxFailureRecords)
        // The counter is NOT bounded by the list cap — it keeps counting
        // past it. Conflating the two would cap escalation at the list
        // size for no reason.
        #expect(state.consecutiveFailureCount == overflow)
        // Newest last: the final record must carry the final count.
        #expect(state.recentFailures.last?.consecutiveFailureCount == overflow)
    }

    // MARK: - Data Protection

    /// The refuse-to-clobber rule. A `BGProcessingTask` can wake the app
    /// after a reboot with the container still protected; if that read
    /// were treated as "no document", the very next write would replace
    /// a real escalation history with a one-entry document and the
    /// counter would reset forever.
    @Test("An unreadable document is not overwritten")
    func unreadableDocumentIsNotClobbered() async throws {
        let (journal, dir) = try makeJournal()
        await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)
        await journal.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)

        // Simulate "present but unreadable" the only way a unit test
        // can: replace the file with a directory of the same name, so
        // `fileExists` is true and `Data(contentsOf:)` fails.
        let fileURL = dir.appendingPathComponent(AnalysisStoreHealthJournal.filename)
        try FileManager.default.removeItem(at: fileURL)
        try FileManager.default.createDirectory(at: fileURL, withIntermediateDirectories: true)

        let blocked = AnalysisStoreHealthJournal(directory: dir)
        _ = await blocked.recordFailure(error: Self.foreignKeyFailure, now: Self.t0)

        // The obstruction is still a directory — nothing was written
        // over it.
        var isDirectory: ObjCBool = false
        #expect(FileManager.default.fileExists(atPath: fileURL.path, isDirectory: &isDirectory))
        #expect(isDirectory.boolValue)
    }

    // MARK: - Classification

    @Test("A constraint failure is classified as such and counts toward escalation")
    func constraintFailureIsClassified() {
        let (phase, failureClass, detail) = AnalysisStoreHealthJournal.classify(Self.foreignKeyFailure)
        #expect(phase == .migration)
        #expect(failureClass == .constraintViolation)
        #expect(failureClass.countsTowardEscalation)
        // The `(SQL: …)` suffix is stripped; what survives is the part a
        // human triages on.
        #expect(detail?.contains("FOREIGN KEY constraint failed") == true)
    }

    @Test("A corrupt database is classified as corrupt, from either the code or the message")
    func corruptDatabaseIsClassified() {
        let byMessage = AnalysisStoreHealthJournal.classify(
            AnalysisStoreError.migrationFailed("database disk image is malformed")
        )
        #expect(byMessage.1 == .databaseCorrupt)

        // SQLITE_NOTADB = 26, with a message that carries no keyword.
        let byCode = AnalysisStoreHealthJournal.classify(
            AnalysisStoreError.openFailed(code: 26, message: "unknown")
        )
        #expect(byCode.0 == .open)
        #expect(byCode.1 == .databaseCorrupt)
        #expect(byCode.1.countsTowardEscalation)
    }

    /// THE ONE CLASSIFICATION THAT CHANGES BEHAVIOUR. The store is
    /// stamped `.completeUntilFirstUserAuthentication`, so a background
    /// launch after a reboot and before the first unlock cannot open it.
    /// Counting those would walk a perfectly healthy library up to
    /// "we could not open your analysis history" after three reboots.
    @Test("A Data-Protection open failure never advances the escalation counter")
    func accessDeniedDoesNotEscalate() async throws {
        let (journal, _) = try makeJournal()
        // SQLITE_CANTOPEN = 14, with the message iOS produces when the
        // container is still protected.
        let locked = AnalysisStoreError.openFailed(
            code: 14, message: "unable to open database file"
        )
        #expect(AnalysisStoreHealthJournal.classify(locked).1 == .accessDenied)
        #expect(AnalysisStoreFailureClass.accessDenied.countsTowardEscalation == false)

        for _ in 0..<10 {
            await journal.recordFailure(error: locked, now: Self.t0)
        }
        let state = await journal.load()
        #expect(state.consecutiveFailureCount == 0)
        #expect(state.status == .retrying)
        // It is still RECORDED — invisible would be the original bug.
        #expect(state.recentFailures.count == 10)
        #expect(state.recentFailures.allSatisfy { $0.failureClass == .accessDenied })
    }

    @Test("A non-store error is recorded as unknown rather than guessed at")
    func foreignErrorIsUnknown() {
        struct Unrelated: Error {}
        let (phase, failureClass, detail) = AnalysisStoreHealthJournal.classify(Unrelated())
        #expect(phase == .unknown)
        #expect(failureClass == .unknown)
        #expect(detail == nil)
    }

    // MARK: - Detail sanitising

    @Test("A detail carrying content that must not leave the device is dropped, not truncated")
    func hostileDetailIsDropped() {
        // Commas, quotes and slashes are all outside the allowlist, so a
        // message shaped like a title or a URL cannot survive.
        #expect(AnalysisStoreHealthDetail.sanitize("no ad span for 'Diary of a CEO', ep 431") == nil)
        #expect(AnalysisStoreHealthDetail.sanitize("https://feeds.example.com/rss.xml") == nil)
        #expect(AnalysisStoreHealthDetail.sanitize(nil) == nil)
        // Rejection means dropped, never truncated-and-kept.
        let long = String(repeating: "a", count: AnalysisStoreHealthDetail.maxLength + 1)
        #expect(AnalysisStoreHealthDetail.sanitize(long) == nil)
    }

    @Test("A real SQLite message survives")
    func realMessageSurvives() {
        #expect(
            AnalysisStoreHealthDetail.sanitize("FOREIGN KEY constraint failed")
                == "FOREIGN KEY constraint failed"
        )
        #expect(
            AnalysisStoreHealthDetail.sanitize("database disk image is malformed")
                == "database disk image is malformed"
        )
    }

    /// The document outlives app versions, so the exporter must not
    /// trust bytes written by an older binary — the same rule
    /// `StabilityDiagnosticRecord.init(from:)` follows.
    @Test("A hostile detail written by an older binary is re-sanitised on decode")
    func decodeReSanitisesDetail() throws {
        let json = """
        {
          "schema_version": 1,
          "occurred_at": "2023-11-14T22:13:20Z",
          "phase": "migration",
          "failure_class": "constraint_violation",
          "consecutive_failure_count": 1,
          "expected_schema_version": 40,
          "detail": "SENTINEL 'Diary of a CEO', https://feeds.example.com/rss.xml"
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let record = try decoder.decode(
            AnalysisStoreFailureRecord.self, from: Data(json.utf8)
        )
        #expect(record.detail == nil)
        #expect(record.failureClass == .constraintViolation)
    }

    @Test("An unknown enum rawValue from a newer binary decodes to unknown rather than failing")
    func decodeToleratesUnknownRawValues() throws {
        let json = """
        {
          "status": "some_future_status",
          "consecutive_failure_count": 4
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let state = try decoder.decode(AnalysisStoreHealthState.self, from: Data(json.utf8))
        #expect(state.status == .healthy)
        #expect(state.consecutiveFailureCount == 4)
    }
}
