// SQLiteSystemErrnoPlatformProbeTests.swift
// playhead-vk68m / playhead-enzva
//
// A PLATFORM MEASUREMENT, KEPT AS A TEST BECAUSE IT HAS TO KEEP BEING TAKEN.
//
// `playhead-enzva` was filed on a premise: `SQLITE_CANTOPEN` says
// `unable to open database file` for several unrelated causes, SQLite keeps the
// real cause in `sqlite3_system_errno(db)`, and `AnalysisStore` throws it away.
// The premise is true of SQLite and true of the Mac. It is FALSE of the
// platform this app runs on, and that is what this file establishes.
//
// MEASURED HERE ON EVERY RUN, through the same `sqlite3_open_v2` flags
// `AnalysisStore.openSQLiteHandle` uses:
//
//   parent path component is a regular file  -> rc 14, extended 14, errno 0
//   parent directory mode 0o000              -> rc 14, extended 14, errno 0
//   the path itself IS a directory           -> rc 14, extended 14, errno 0
//   control: a path that opens fine          -> rc  0, extended  0, errno 0
//
// while SQLite's OWN log line in the same run reads
// `os_unix.c:52971: (20) open(...) - Not a directory`. The library knows the
// cause; neither of its two public discriminators reports it here.
//
// AND IT IS NOT A VERSION SKEW. Both libraries report
// `sqlite3_libversion() == "3.54.0"` — printed below on every run rather than
// assumed, because "the same version" is exactly the inference that would do a
// control's work in the one place where the two demonstrably disagree. Against
// the MAC's `/usr/lib/libsqlite3.dylib` the first two conditions return
// 20 (ENOTDIR) and 13 (EACCES). Same version string, opposite answers: a build
// or configuration difference, not something anyone can wait out.
//
// WHAT WAS TRIED AND WITHDRAWN, so it is not tried again. The capture was
// implemented — `sqlite3_system_errno` read before `sqlite3_close_v2` at
// `openSQLiteHandle` and at `exec`, appended to the thrown message — and then
// REVERTED, because on this platform it appends `errno=0 none recorded` to
// every store failure and two consumers truncate or reject the result:
//
//   * `AnalysisStoreHealthDetail.sanitize` admits a message only if every
//     character is in `DiagnosticTextSanitizer.allowedCharacters`, which has no
//     `[`, `]`, `=` or `:`. Rejection OMITS the field, so the durable on-device
//     `detail` went from `unable to open database file` to nothing at all, for
//     every store open and migration failure — silently, since `failureClass`
//     is computed from the raw message and nothing else moved.
//   * `PersistedStateInvariantEvaluator.sanitize` keeps `.prefix(80)`, so in
//     the diagnostics archive a user can mail, a 39-character clause carrying
//     no information displaced the `(SQL: …)` tail that was the field's only
//     content, and was itself cut mid-word.
//
// Two regressions in consumer surfaces, in exchange for a value measured to be
// zero. So the measurement ships and the capture does not. Revisit
// `playhead-enzva` if a platform is ever found that populates the field; the
// two call sites and their hazard (read it BEFORE `sqlite3_close_v2`, which
// discards it) are recorded there.
//
// FAILURE DIRECTIONS, because one of these is deliberately not symmetric:
// the assertions pin the DEFECT — one message and one extended code for three
// unrelated causes — and NOT `errno == 0`. A platform that starts telling them
// apart is good news, and it turns this red rather than green. That is
// intended: red is how it asks to be revisited.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("SQLITE_CANTOPEN carries no cause on this platform")
struct SQLiteSystemErrnoPlatformProbeTests {

    /// A scratch directory removed however the test leaves.
    private static func withScratch<T>(_ body: (URL) throws -> T) throws -> T {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("enzva-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        return try body(dir)
    }

    /// The same flags `AnalysisStore.openSQLiteHandle` passes, so this measures
    /// the call the app makes rather than a convenient one. Caller closes.
    private static func rawOpen(_ path: String) -> (rc: Int32, handle: OpaquePointer?) {
        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX
        let rc = sqlite3_open_v2(path, &handle, flags, nil)
        return (rc, handle)
    }

    @Test("three unrelated CANTOPEN causes are indistinguishable from what SQLite reports")
    func thePlatformCannotTellThemApart() throws {
        try Self.withScratch { dir in
            let blocker = dir.appendingPathComponent("iAmAFile")
            try Data("x".utf8).write(to: blocker)
            let noPerm = dir.appendingPathComponent("noperm", isDirectory: true)
            try FileManager.default.createDirectory(at: noPerm, withIntermediateDirectories: true)
            try FileManager.default.setAttributes([.posixPermissions: 0], ofItemAtPath: noPerm.path)
            defer {
                // playhead-cgka: a directory left at mode 0 strands the whole
                // device on the next `simctl erase`, because the async reaper
                // cannot walk it. Restore before anything else can fail.
                try? FileManager.default.setAttributes(
                    [.posixPermissions: 0o755], ofItemAtPath: noPerm.path)
            }
            let asDirectory = dir.appendingPathComponent("iAmADirectory", isDirectory: true)
            try FileManager.default.createDirectory(
                at: asDirectory, withIntermediateDirectories: true)

            print("[enzva] simulator SQLite version: "
                  + String(cString: sqlite3_libversion())
                  + " (the Mac's /usr/lib/libsqlite3.dylib measured 3.54.0)")

            let causes: [(String, String)] = [
                ("parent is a FILE (ENOTDIR on the host)",
                 blocker.appendingPathComponent("a.sqlite").path),
                ("parent dir mode 000 (EACCES on the host)",
                 noPerm.appendingPathComponent("a.sqlite").path),
                ("the path IS a directory (EISDIR expected on the host; NOT measured there)",
                 asDirectory.path),
            ]
            var reported: Set<String> = []
            for (label, path) in causes {
                let opened = Self.rawOpen(path)
                defer { if let handle = opened.handle { sqlite3_close_v2(handle) } }
                let message = opened.handle.map { String(cString: sqlite3_errmsg($0)) } ?? "<none>"
                let extended = opened.handle.map { sqlite3_extended_errcode($0) } ?? -1
                let sysErrno = opened.handle.map { sqlite3_system_errno($0) } ?? -1
                print("[enzva] \(label): rc=\(opened.rc) extended=\(extended) "
                      + "system_errno=\(sysErrno) msg=\(message)")
                #expect(opened.rc == SQLITE_CANTOPEN, "\(label)")
                reported.insert("\(message)|\(extended)|\(sysErrno)")
            }
            // THE DEFECT, AS AN ASSERTION: three different bugs, one report.
            // Red here means a platform started discriminating — see the header.
            #expect(reported.count == 1, "\(reported)")
            #expect(reported.first?.hasPrefix("unable to open database file|") == true)
        }
    }

    @Test("a successful open is the control, and reports nothing either")
    func theControlReportsNothing() throws {
        try Self.withScratch { dir in
            let opened = Self.rawOpen(dir.appendingPathComponent("ok.sqlite").path)
            defer { if let handle = opened.handle { sqlite3_close_v2(handle) } }
            #expect(opened.rc == SQLITE_OK)
            // Without this row, `system_errno == 0` on a FAILURE could be read
            // as "SQLite reported success". It reports nothing in both cases,
            // which is the whole reason the field cannot be used here.
            #expect(opened.handle.map { sqlite3_system_errno($0) } == 0)
            #expect(opened.handle.map { sqlite3_extended_errcode($0) } == 0)
        }
    }

    /// The prose is what the gate has to match, so pin that it is still the
    /// prose — `scripts/gate_baseline.py` carries exactly one text match
    /// (`unable to open database file`) and this platform is the reason it
    /// cannot be replaced by an errno.
    @Test("the message the gate classifier matches is still the message SQLite emits")
    func theProseTheGateMatchesIsStillEmitted() throws {
        try Self.withScratch { dir in
            let blocker = dir.appendingPathComponent("iAmAFile")
            try Data("x".utf8).write(to: blocker)
            let opened = Self.rawOpen(blocker.appendingPathComponent("a.sqlite").path)
            defer { if let handle = opened.handle { sqlite3_close_v2(handle) } }
            let message = opened.handle.map { String(cString: sqlite3_errmsg($0)) } ?? ""
            #expect(message == "unable to open database file")
        }
    }

    /// And the store really does surface that prose, unchanged — the property
    /// `gate_baseline.py`'s prose match actually depends on.
    @Test("AnalysisStore surfaces SQLite's prose unchanged")
    func theStoreSurfacesTheProse() async throws {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("enzva-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let blocker = dir.appendingPathComponent("iAmAFile")
        try Data("x".utf8).write(to: blocker)
        let store = try AnalysisStore(path: blocker.appendingPathComponent("a.sqlite").path)
        var thrown: Error?
        do { try await store.migrate() } catch { thrown = error }

        print("[enzva] store message: \(String(describing: thrown))")

        // THE PAYLOAD, NOT THE DESCRIPTION, AND EQUALITY, NOT `contains`.
        // Both of those are the difference between a rail and a rail-shaped
        // thing (playhead-vk68m review round 3). `contains` is satisfied by
        // `…database file [sqlite3_system_errno=0 none recorded]`, and handing
        // `sanitize` a LITERAL tests a string this file wrote rather than the
        // one the store produced — so with both mistakes in place, re-applying
        // the withdrawn capture left this test GREEN, which is precisely the
        // thing it exists to stop.
        guard case .openFailed(let code, let message)? = thrown as? AnalysisStoreError else {
            Issue.record("expected .openFailed, got \(String(describing: thrown))")
            return
        }
        #expect(code == SQLITE_CANTOPEN)
        #expect(message == "unable to open database file")

        // And the DURABLE record keeps that exact message. This is the field
        // the withdrawn capture was silently emptying: `sanitize` admits a
        // message only if every character is in
        // `DiagnosticTextSanitizer.allowedCharacters`, and rejection OMITS the
        // field rather than truncating it. Sanitizing the message the store
        // just produced — not a literal — is what makes re-applying the capture
        // turn this red.
        #expect(AnalysisStoreHealthDetail.sanitize(message) == message)
    }
}
