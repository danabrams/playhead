// TestScratch.swift
// Per-test lifetime for the test scratch root. TEST-ONLY — nothing here is
// referenced by the app target.

import Foundation

/// Reclaims test scratch directories DURING a run, instead of only at process
/// boundaries.
///
/// playhead-cgka. `makeTempDir` hands every test a fresh subdirectory under a
/// process-wide scratch root, and its two cleanup paths — a wipe-on-first-use
/// and an `atexit` hook — both fire at PROCESS boundaries. Nothing reclaimed
/// anything while the suite ran, so peak disk was the SUM of every test's
/// scratch rather than the max of any one, and a full gate died with
/// `NSPOSIXErrorDomain Code=28` near the end of the suite, every time. Those two
/// paths are still here and still wanted — they are the backstop for an
/// ABNORMAL exit, which is exactly when this reaper does not get to finish. What
/// they cannot do is bound the peak.
///
/// MEASURED 2026-08-02, full plan, guard-killed at 9,675 Swift Testing + 900
/// XCTest cases: 2,847 leftover directories / 2.04 GiB, of which 2,591 were
/// 732.6 KiB each — one migrated `analysis.sqlite` (696 KiB) plus its `-shm`
/// (32 KiB). The cost is a STORE, not a directory, which is why the store
/// factories are where ownership is attached.
///
/// ## Why ownership, and not age
///
/// The obvious cheap reaper — "delete anything older than N seconds" — is the
/// one design that must not be built here. Several suites hold an open
/// `AnalysisStore` against their directory for the whole of a long test, and a
/// timer that guesses wrong turns a disk problem into a corruption flake, which
/// is strictly worse. So a directory is reclaimed on one condition only: the
/// object that owns it has been deallocated. `AnalysisStore.deinit` closes its
/// SQLite handle, so an owner that is gone is a database that is closed.
///
/// The reclaim is deferred by one sweep past the first observation of a nil
/// owner. Swift zeroes `weak` references when an object ENTERS deinitialization,
/// so the very first nil observation can race a `deinit` that has not yet run
/// `sqlite3_close_v2`. One extra sweep costs nothing and removes the race
/// entirely.
///
/// ## The honest limit
///
/// A directory from a bare `makeTempDir(prefix:)` with no `ownedBy:` has no
/// owner to watch, and is NEVER reclaimed mid-run — it falls back to the
/// process-boundary backstop, i.e. exactly today's behaviour. That is a
/// deliberate floor, not an oversight: there is no sound way to prove such a
/// directory is idle. Call sites that want the bound pass `ownedBy:`.
final class TestScratchReaper: @unchecked Sendable {
    /// The reaper the test helpers use. Tests of the reaper itself build their
    /// own instance so they cannot perturb — or be perturbed by — the suite.
    static let shared = TestScratchReaper()

    struct Stats: Equatable {
        /// Directories registered so far.
        var registered = 0
        /// Directories whose owner died and whose bytes have been reclaimed.
        var reclaimed = 0
        /// Registered directories not yet reclaimed.
        var live = 0
        /// Registered directories that carry an owner to watch.
        var owned = 0
        /// Sweeps performed.
        var sweeps = 0
    }

    private struct Entry {
        let url: URL
        weak var owner: AnyObject?
        var isOwned: Bool
        /// Sweep ordinal at which `owner` was FIRST seen nil. `nil` while the
        /// owner is alive; reset if the entry is re-adopted.
        var orphanedAtSweep: Int?
    }

    private let lock = NSLock()
    private var entries: [Entry] = []
    private var registered = 0
    private var reclaimed = 0
    private var sweeps = 0
    private let sweepEvery: Int

    /// - Parameter sweepEvery: registrations between automatic sweeps. Sweeping
    ///   on registration rather than on a timer keeps the reaper thread-free and
    ///   puts the work exactly where the pressure is: nothing new is being
    ///   created means nothing new needs reclaiming.
    init(sweepEvery: Int = 16) {
        self.sweepEvery = max(1, sweepEvery)
    }

    /// Record a directory with no owner. It is reclaimed only at process exit
    /// unless something later adopts it.
    func register(_ url: URL) {
        lock.lock()
        entries.append(Entry(url: url, owner: nil, isOwned: false, orphanedAtSweep: nil))
        registered += 1
        let due = registered % sweepEvery == 0
        lock.unlock()
        if due { sweep() }
    }

    /// Tie `url`'s lifetime to `owner`'s. Reclaimed once `owner` is deallocated.
    ///
    /// Adopting an unregistered URL registers it, so a caller cannot silently
    /// get a no-op by adopting before registering.
    func adopt(_ url: URL, owner: AnyObject) {
        lock.lock()
        if let index = entries.lastIndex(where: { $0.url == url }) {
            entries[index].owner = owner
            entries[index].isOwned = true
            entries[index].orphanedAtSweep = nil
        } else {
            entries.append(Entry(url: url, owner: owner, isOwned: true, orphanedAtSweep: nil))
            registered += 1
        }
        lock.unlock()
    }

    /// Reclaim every owned directory whose owner has been gone since before the
    /// previous sweep. Safe to call at any time from any thread.
    @discardableResult
    func sweep() -> Int {
        lock.lock()
        sweeps += 1
        let now = sweeps
        var doomed: [URL] = []
        var kept: [Entry] = []
        kept.reserveCapacity(entries.count)
        for var entry in entries {
            guard entry.isOwned else { kept.append(entry); continue }
            if entry.owner != nil {
                entry.orphanedAtSweep = nil
                kept.append(entry)
                continue
            }
            if let seen = entry.orphanedAtSweep, seen < now {
                doomed.append(entry.url)
                continue
            }
            if entry.orphanedAtSweep == nil { entry.orphanedAtSweep = now }
            kept.append(entry)
        }
        entries = kept
        reclaimed += doomed.count
        lock.unlock()

        for url in doomed { Self.forceRemove(url) }
        return doomed.count
    }

    var stats: Stats {
        lock.lock()
        defer { lock.unlock() }
        return Stats(
            registered: registered,
            reclaimed: reclaimed,
            live: entries.count,
            owned: entries.reduce(0) { $0 + ($1.isOwned ? 1 : 0) },
            sweeps: sweeps
        )
    }

    /// Whether the reaper is still watching `url` — i.e. it has been registered
    /// and not yet reclaimed. Diagnostics for the reaper's own tests.
    func isTracked(_ url: URL) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return entries.contains { $0.url == url }
    }

    /// Whether `url` carries a LIVE owner, and so will be reclaimed mid-run.
    ///
    /// Distinct from ``isTracked(_:)`` on purpose. Tracked-but-unowned is the
    /// state a bare `makeTempDir` leaves behind, and it is reclaimed only at
    /// process exit — so a factory that forgot to adopt still reads as tracked,
    /// and an assertion phrased on tracking alone cannot tell "bounded" from
    /// "exactly as broken as before".
    func isOwned(_ url: URL) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard let entry = entries.last(where: { $0.url == url }) else { return false }
        return entry.isOwned && entry.owner != nil
    }

    /// Remove a directory tree even when a test has deliberately made part of it
    /// unreadable.
    ///
    /// `DownloadManagerTests` chmods a `complete/` directory to 0o300 to prove a
    /// permission failure is handled, and an abnormal exit can leave it that way.
    /// 0o300 is `-wx------`: WRITE is already granted, so the familiar
    /// `chmod -R u+w` is a no-op here — READ is what an enumerator needs, and
    /// without it `removeItem` fails with EACCES and the bytes stay on disk
    /// forever. Repairing permissions is attempted only after a first removal
    /// fails, so the common path stays a single syscall.
    static func forceRemove(_ url: URL) {
        let fileManager = FileManager.default
        do {
            try fileManager.removeItem(at: url)
        } catch {
            makeReadableAndWritable(url)
            try? fileManager.removeItem(at: url)
        }
    }

    private static func makeReadableAndWritable(_ url: URL) {
        let fileManager = FileManager.default
        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: url.path, isDirectory: &isDirectory) else { return }
        try? fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: url.path)
        guard isDirectory.boolValue else { return }
        let children = (try? fileManager.contentsOfDirectory(atPath: url.path)) ?? []
        for child in children {
            makeReadableAndWritable(url.appendingPathComponent(child))
        }
    }
}
