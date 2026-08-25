// SQLiteSystemErrno.swift
// playhead-enzva: name the OS error behind `unable to open database file`.
//
// `sqlite3_errmsg` for `SQLITE_CANTOPEN` is the prose "unable to open database
// file" and names NO cause. SQLite keeps the underlying errno from the failed
// syscall in `sqlite3_system_errno(db)` and the store used to throw it away, so
// five different bugs reached the log as one indistinguishable sentence.
//
// AND ON THIS PLATFORM IT IS ZERO — READ THIS BEFORE RELYING ON THE VALUE.
//
// Measured 2026-08-24 (playhead-vk68m). Against the MAC's
// `/usr/lib/libsqlite3.dylib` (SQLite 3.54.0) the value is there and it is
// exactly the discriminator playhead-enzva wanted:
//
//     parent path component is a regular file    ENOTDIR (20)
//     parent directory mode 0o000                EACCES  (13)
//     the descriptor table is full               EBADF   (9)
//
// Against the iOS 27 SIMULATOR — the platform the app actually runs on — all
// three of the first two shapes plus "the path itself is a directory" report
// `system_errno = 0` AND a bare `sqlite3_extended_errcode` of 14, while SQLite's
// own log line says `os_unix.c:52971: (20) open(...) - Not a directory`. So
// NEITHER of SQLite's two discriminators carries the cause here. The rail is
// `SQLiteSystemErrnoTests.thePlatformCannotTellThemApart`.
//
// Two consequences, both stated rather than implied:
//
//   * `scripts/gate_baseline.py`'s ONE prose match on `unable to open database
//     file` cannot be replaced by an errno on this platform. That is a standing
//     limit, not a TODO.
//   * The exhaustion case is NOT measured on the simulator — forcing a full
//     descriptor table inside a shared test host would take the host down with
//     it. Given three unrelated causes all report 0 through the same VFS hook,
//     0 is the expectation; it is an expectation, not a measurement.
//
// The call is kept because it costs nothing, renders 0 as `none recorded`
// rather than as an errno, and keeps measuring this on every denial in every
// gate log — so if a future SDK starts populating it, the value simply appears.
//
// EBADF rather than EMFILE, above, is not a typo: measured in C on this box
// (playhead-vk68m), fill the table to `RLIMIT_NOFILE` and the refusing
// `open(2)` sets errno 9, not the POSIX-documented 24.

import Foundation
import SQLite3

/// Renders `sqlite3_system_errno` for a log line.
enum SQLiteSystemErrno {

    /// A suffix naming the OS errno SQLite recorded for the most recent
    /// failure on `handle`, ready to append to an error message.
    ///
    /// THREE THINGS THIS DELIBERATELY DOES, each because its opposite is this
    /// repo's standing defect class — a value that names one thing read as
    /// though it named another.
    ///
    /// **A ZERO IS NOT AN ERRNO.** A successful open leaves the field at 0, and
    /// SQLite records nothing there for a failure that never reached the OS. So
    /// 0 renders as `none recorded` — never as a number a reader could look up
    /// in `errno.h`, and never as success.
    ///
    /// **A MISSING HANDLE IS NOT A ZERO EITHER.** `sqlite3_open_v2` can fail
    /// without allocating a connection (out of memory), and there is then
    /// nothing to ask. That renders as `no handle`, so a reader can tell "the
    /// instrument had nothing to read" from "the instrument read nothing".
    ///
    /// **ONLY THE SYMBOLIC NAME, NEVER `strerror`'s PROSE.**
    /// ``AnalysisStoreFailureClass/classify(message:)`` matches SUBSTRINGS of
    /// this same message, and `strerror(EROFS)` is "Read-only file system" —
    /// appending it would silently reclassify a descriptor problem as
    /// `.readOnly`. Symbolic names collide with none of that vocabulary.
    ///
    /// - Important: read this BEFORE `sqlite3_close_v2`. Closing the handle
    ///   discards the value.
    static func suffix(_ handle: OpaquePointer?) -> String {
        guard let handle else { return " [sqlite3_system_errno: no handle]" }
        let code = sqlite3_system_errno(handle)
        guard code != 0 else { return " [sqlite3_system_errno=0 none recorded]" }
        guard let name = name(of: code) else { return " [sqlite3_system_errno=\(code)]" }
        return " [sqlite3_system_errno=\(code) \(name)]"
    }

    /// The symbolic name for an errno, or nil when this table does not know it.
    ///
    /// `nil` rather than a guess: an unnamed code still prints its NUMBER, which
    /// is the fact, and inventing a name for it would be the defect this whole
    /// file exists to remove. The table names the codes a store open can
    /// plausibly hit on iOS plus the four the gate classifier already routes
    /// (`scripts/gate_baseline.py`), so a reader and the classifier share a
    /// vocabulary.
    static func name(of code: Int32) -> String? {
        switch code {
        case 1:  return "EPERM"
        case 2:  return "ENOENT"
        case 4:  return "EINTR"
        case 5:  return "EIO"
        case 9:  return "EBADF"       // this platform's errno at the RLIMIT_NOFILE ceiling
        case 12: return "ENOMEM"
        case 13: return "EACCES"
        case 16: return "EBUSY"
        case 17: return "EEXIST"
        case 20: return "ENOTDIR"
        case 21: return "EISDIR"
        case 22: return "EINVAL"
        case 23: return "ENFILE"      // system file table
        case 24: return "EMFILE"      // per-process descriptor limit
        case 27: return "EFBIG"
        case 28: return "ENOSPC"
        case 30: return "EROFS"
        case 35: return "EAGAIN"
        case 45: return "ENOTSUP"
        case 62: return "ELOOP"
        case 63: return "ENAMETOOLONG"
        default: return nil
        }
    }
}
