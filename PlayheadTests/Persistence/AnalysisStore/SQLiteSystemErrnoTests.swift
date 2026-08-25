// SQLiteSystemErrnoTests.swift
// playhead-enzva / playhead-vk68m
//
// THE HEADLINE FROM THESE RAILS IS A REFUTATION, and it is worth reading before
// the assertions. `playhead-enzva` was filed on the premise that SQLite keeps
// the underlying errno in `sqlite3_system_errno(db)` and the store throws it
// away. MEASURED on the iOS 27 simulator, that premise does not hold on this
// platform: three unrelated causes of `SQLITE_CANTOPEN` all report
// `system_errno = 0` AND a bare extended code of 14.
//
//   parent path component is a regular file  -> rc 14, extended 14, errno 0
//   parent directory mode 0o000              -> rc 14, extended 14, errno 0
//   the path itself IS a directory           -> rc 14, extended 14, errno 0
//   control: a path that opens fine          -> rc  0, extended  0, errno 0
//
// Against the MAC's `/usr/lib/libsqlite3.dylib` the FIRST TWO of those return
// 20 (ENOTDIR) and 13 (EACCES). Three things that sentence is careful about,
// because an earlier draft was not: only two of the three conditions were run
// on the Mac (the third Mac row in `artifacts/MEASUREMENTS.md` M3 is descriptor
// EXHAUSTION, which is a different condition); `EISDIR` is what one would
// EXPECT on the host for the third and it has never been measured there; and
// both libraries report `sqlite3_libversion() == "3.54.0"` — MEASURED, printed
// by this suite on every run rather than assumed, and it makes the finding
// sharper rather than softer: the SAME version string answers the SAME two
// conditions differently, so this is a build or configuration difference and
// not a version skew anyone can wait out.
//
// So the value exists on the host and not in the app, and measuring the host
// library and reading the result as a claim about the app is this repo's
// standing defect class — committed here first, corrected here.
//
// WHY THE CALL IS KEPT ANYWAY. It costs one call, it renders 0 as
// `none recorded` rather than as an errno, and it is now the thing that keeps
// measuring this: every denial in every gate log carries the field, so nobody
// has to re-derive it, and if a future SDK starts populating it the value simply
// appears. What it does NOT do is let `scripts/gate_baseline.py` retire its one
// prose match — that is a standing limit on this platform, not a TODO.
//
// The assertions below deliberately pin the RENDERING, which is ours and is
// platform-independent, and NOT the platform's answer — a rail asserting
// `== 0` would go red on a device that behaves like the Mac, which is the good
// outcome, reported as a failure.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("sqlite3_system_errno rendering")
struct SQLiteSystemErrnoTests {

    /// A scratch directory that is removed however the test leaves.
    private static func withScratch<T>(_ body: (URL) throws -> T) throws -> T {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("enzva-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        return try body(dir)
    }

    /// Open `path` with the same flags `AnalysisStore.openSQLiteHandle` uses, so
    /// the rails exercise the real call rather than a stub. Caller closes.
    private static func rawOpen(_ path: String) -> (rc: Int32, handle: OpaquePointer?) {
        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX
        let rc = sqlite3_open_v2(path, &handle, flags, nil)
        return (rc, handle)
    }

    /// THE OBSERVATION THE HEADER IS BUILT ON, re-taken on every run.
    ///
    /// It asserts the half that is a defect either way — three unrelated causes
    /// are INDISTINGUISHABLE from the message and the extended code — and prints
    /// what the platform reported so the header's table can be checked rather
    /// than trusted. It does not assert `errno == 0`, because a platform that
    /// starts reporting it is good news and must not read as a regression.
    @Test("three unrelated CANTOPEN causes are indistinguishable from the message")
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

            let causes: [(String, String)] = [
                ("parent is a FILE (ENOTDIR on the host)", blocker.appendingPathComponent("a.sqlite").path),
                ("parent dir mode 000 (EACCES on the host)", noPerm.appendingPathComponent("a.sqlite").path),
                ("the path IS a directory (EISDIR expected on the host; NOT measured there)",
                 asDirectory.path),
            ]
            print("[enzva] simulator SQLite version: "
                  + String(cString: sqlite3_libversion())
                  + " (the Mac's /usr/lib/libsqlite3.dylib measured 3.54.0)")
            var messages: Set<String> = []
            for (label, path) in causes {
                let opened = Self.rawOpen(path)
                defer { if let handle = opened.handle { sqlite3_close_v2(handle) } }
                let message = opened.handle.map { String(cString: sqlite3_errmsg($0)) } ?? "<none>"
                let extended = opened.handle.map { sqlite3_extended_errcode($0) } ?? -1
                let sysErrno = opened.handle.map { sqlite3_system_errno($0) } ?? -1
                print("[enzva] \(label): rc=\(opened.rc) extended=\(extended) "
                      + "system_errno=\(sysErrno) msg=\(message)")
                #expect(opened.rc == SQLITE_CANTOPEN, "\(label)")
                messages.insert("\(message)|\(extended)")
            }
            // The defect, stated as an assertion: three different bugs, one string.
            #expect(messages.count == 1, "\(messages)")
            #expect(messages.first?.hasPrefix("unable to open database file") == true)
        }
    }

    @Test("a SUCCESSFUL open renders `none recorded`, never a zero that reads as an errno")
    func zeroIsNotAnErrno() throws {
        try Self.withScratch { dir in
            let opened = Self.rawOpen(dir.appendingPathComponent("ok.sqlite").path)
            defer { if let handle = opened.handle { sqlite3_close_v2(handle) } }

            #expect(opened.rc == SQLITE_OK)
            let rendered = SQLiteSystemErrno.suffix(opened.handle)
            #expect(rendered.contains("none recorded"))
            // A bare `=0` with nothing after it would read as errno 0.
            #expect(!rendered.hasSuffix("=0]"))
        }
    }

    @Test("no handle is reported as no handle, not as zero")
    func aMissingHandleMakesNoClaim() {
        let rendered = SQLiteSystemErrno.suffix(nil)
        #expect(rendered.contains("no handle"))
        #expect(!rendered.contains("=0"))
    }

    @Test("an unnamed code still prints its NUMBER rather than an invented name")
    func unknownCodeKeepsTheFact() {
        #expect(SQLiteSystemErrno.name(of: 9) == "EBADF")
        #expect(SQLiteSystemErrno.name(of: 23) == "ENFILE")
        #expect(SQLiteSystemErrno.name(of: 24) == "EMFILE")
        #expect(SQLiteSystemErrno.name(of: 28) == "ENOSPC")
        #expect(SQLiteSystemErrno.name(of: 9999) == nil)
    }

    /// THE RAIL THAT EXISTS BECAUSE OF A NEAR MISS.
    ///
    /// ``AnalysisStoreFailureClass/classify(message:)`` matches SUBSTRINGS of
    /// this same message, and several errno PROSE strings collide with its
    /// vocabulary — `strerror(EROFS)` is "Read-only file system", which contains
    /// "read-only". Appending `strerror` output would silently reclassify a
    /// descriptor problem as `.readOnly` and change what escalates toward the
    /// destructive prompt. So the renderer emits symbolic names only, and this
    /// proves it for every code the table knows.
    /// Every rendering the shipped code can produce, for every code and for the
    /// two handle-less cases. `SQLiteSystemErrno.render` is the SAME function
    /// `suffix` calls, which is the point: an earlier version of this rail built
    /// the decorated string by hand and therefore proved a property of this file
    /// rather than of the renderer — a `suffix` changed to splice `strerror`
    /// prose in would have passed it.
    private static var everyRendering: [String] {
        var all = [SQLiteSystemErrno.render(.noHandle)]
        for code in Int32(0)...Int32(100) {
            all.append(SQLiteSystemErrno.render(.code(code)))
        }
        return all
    }

    @Test("appending the errno cannot change how the message classifies")
    func theSuffixIsInertToTheClassifier() {
        let base = "unable to open database file"
        #expect(AnalysisStoreFailureClass.classify(message: base) == .accessDenied)
        for rendering in Self.everyRendering {
            #expect(
                AnalysisStoreFailureClass.classify(message: base + rendering) == .accessDenied,
                "\(rendering) changed the classification"
            )
        }
        // and the renderer really is what `suffix` uses, so the loop above is
        // about the shipped string rather than a copy of it
        #expect(SQLiteSystemErrno.suffix(nil) == SQLiteSystemErrno.render(.noHandle))
    }

    /// THE REGRESSION THIS SUITE SHIPPED AND THE REVIEW CAUGHT.
    ///
    /// `AnalysisStoreHealthDetail.sanitize` admits a message only if EVERY
    /// character is in `DiagnosticTextSanitizer.allowedCharacters`, which has no
    /// `[`, `]`, `=` or `:`. Appending the errno therefore turned the durable
    /// on-device `detail` field from `unable to open database file` into
    /// **nothing at all**, for every store open and every migration failure —
    /// silently, with `failureClass` unchanged so nothing else moved. The clause
    /// is stripped now, exactly as ` (SQL: …)` already was.
    @Test("the errno clause cannot delete the durable health `detail` field")
    func theDurableDetailSurvivesTheSuffix() {
        let base = "unable to open database file"
        #expect(AnalysisStoreHealthDetail.sanitize(base) == base)
        for rendering in Self.everyRendering {
            #expect(
                AnalysisStoreHealthDetail.sanitize(base + rendering) == base,
                "\(rendering) changed what the durable record keeps"
            )
        }
        // exec's real shape: message, errno clause, then the SQL tail
        #expect(AnalysisStoreHealthDetail.sanitize(
            base + SQLiteSystemErrno.render(.code(24)) + " (SQL: SELECT 1)") == base)
        // A LONG message must be unaffected too, and the rail has to build one
        // that sanitize accepts on its own. The obvious candidate — a real
        // `UNIQUE constraint failed: table.column, table.column` — is REJECTED
        // before the errno is anywhere near it, because `:` and `,` are not in
        // the allowlist either; measured when this rail first went red. So the
        // fixture is allowlist-clean by construction and sized just under the
        // cap: the errno clause is ~38 characters against `maxLength` 96, so a
        // strip that failed to match would reject on LENGTH rather than on a
        // character — a different symptom, the same lost field.
        let long = "unable to open database file " + String(repeating: "x", count: 60)
        #expect(long.count == 89)
        #expect(AnalysisStoreHealthDetail.sanitize(long) == long)
        #expect(long.count + SQLiteSystemErrno.render(.code(9)).count > 96)
        #expect(AnalysisStoreHealthDetail.sanitize(
            long + SQLiteSystemErrno.render(.code(9))) == long)
    }

    /// The wiring, not the value: whatever the platform reports, a store open
    /// failure must SAY what it read. That is what makes `errno 0` a measurement
    /// rather than a silence, and it is the property that would break if
    /// somebody removed the call.
    @Test("AnalysisStore's own open failure always states what the errno read was")
    func theStoreWiresItUp() async throws {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("enzva-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let blocker = dir.appendingPathComponent("iAmAFile")
        try Data("x".utf8).write(to: blocker)
        let store = try AnalysisStore(path: blocker.appendingPathComponent("a.sqlite").path)
        var thrown: Error?
        do { try await store.migrate() } catch { thrown = error }

        let described = String(describing: thrown)
        print("[enzva] store message: \(described)")
        #expect(described.contains("unable to open database file"))
        #expect(described.contains("sqlite3_system_errno="), "\(described)")
    }
}
