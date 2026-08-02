// TestScratchReaperTests.swift
// playhead-cgka — rails for the per-test scratch lifetime.

import Foundation
import Testing
@testable import Playhead

/// The reaper is test infrastructure, so its failure mode is not a red test but
/// a SILENT one: a reaper that reclaims nothing leaves the gate exactly as
/// broken as before and nothing says so, and a reaper that reclaims too eagerly
/// deletes a database out from under a live suite and presents as an unrelated
/// flake somewhere else. Both directions are pinned here.
///
/// Every test builds its OWN `TestScratchReaper` rather than touching
/// `.shared`: the suite runs in parallel with ~10,000 other tests that are all
/// registering against the shared instance, so an assertion about counts or
/// about "was this reclaimed" is only meaningful against an instance nobody
/// else can reach.
@Suite("TestScratchReaper")
struct TestScratchReaperTests {
    /// A directory outside the suite's own scratch root, so a test of the reaper
    /// can never be reclaimed by the shared reaper mid-assertion.
    private func makeIsolatedRoot() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("ReaperTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private func makeChild(_ root: URL, _ name: String) throws -> URL {
        let url = root.appendingPathComponent(name, isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private final class Owner {}

    // MARK: - The reclaim happens, and only after the owner is gone

    @Test("an owned directory is reclaimed once its owner is deallocated")
    func ownedDirectoryIsReclaimedAfterOwnerDies() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let reaper = TestScratchReaper(sweepEvery: 1_000_000)
        let dir = try makeChild(root, "owned")

        do {
            let owner = Owner()
            reaper.adopt(dir, owner: owner)
            reaper.sweep()
            #expect(
                FileManager.default.fileExists(atPath: dir.path),
                "a LIVE owner's directory must survive every sweep"
            )
        }

        // Two sweeps: the first observes the nil owner, the second reclaims.
        reaper.sweep()
        reaper.sweep()
        #expect(!FileManager.default.fileExists(atPath: dir.path))
        #expect(reaper.stats.reclaimed == 1)
    }

    /// The rail that stops "reclaim on the first nil observation".
    ///
    /// Swift zeroes `weak` references when an object ENTERS deinitialization,
    /// so the first sweep that sees `nil` can be running while
    /// `AnalysisStore.deinit` has not yet reached `sqlite3_close_v2`. The
    /// one-sweep deferral is what makes the reclaim safe, and it is invisible
    /// unless something asserts on it.
    @Test("the reclaim is deferred one sweep past the first nil observation")
    func reclaimIsDeferredOneSweepPastDeath() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let reaper = TestScratchReaper(sweepEvery: 1_000_000)
        let dir = try makeChild(root, "deferred")
        do {
            let owner = Owner()
            reaper.adopt(dir, owner: owner)
        }

        #expect(reaper.sweep() == 0, "the sweep that first SEES the death must not delete")
        #expect(FileManager.default.fileExists(atPath: dir.path))
        #expect(reaper.sweep() == 1, "the NEXT sweep is the one that reclaims")
        #expect(!FileManager.default.fileExists(atPath: dir.path))
    }

    @Test("a live owner's directory is never reclaimed, however many sweeps run")
    func liveOwnerSurvivesRepeatedSweeps() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let reaper = TestScratchReaper(sweepEvery: 1_000_000)
        let dir = try makeChild(root, "live")
        let owner = Owner()
        reaper.adopt(dir, owner: owner)

        for _ in 0..<10 { reaper.sweep() }

        #expect(FileManager.default.fileExists(atPath: dir.path))
        #expect(reaper.stats.reclaimed == 0)
        withExtendedLifetime(owner) {}
    }

    /// Re-adoption must clear the orphan mark. Without the reset, a directory
    /// that lost one owner and gained another would be deleted underneath the
    /// SECOND owner — the exact "removed while still in use" failure this design
    /// exists to avoid.
    @Test("re-adopting a directory clears its pending orphan mark")
    func readoptionCancelsAPendingReclaim() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let reaper = TestScratchReaper(sweepEvery: 1_000_000)
        let dir = try makeChild(root, "readopted")
        do {
            let first = Owner()
            reaper.adopt(dir, owner: first)
        }
        reaper.sweep()  // marks it orphaned

        let second = Owner()
        reaper.adopt(dir, owner: second)
        reaper.sweep()
        reaper.sweep()

        #expect(
            FileManager.default.fileExists(atPath: dir.path),
            "the second owner is alive, so its directory must not be reclaimed"
        )
        withExtendedLifetime(second) {}
    }

    // MARK: - The unowned floor, stated so it cannot regress silently

    /// The deliberate limit, pinned so a future change that quietly starts
    /// deleting unowned directories has to argue with a test. There is no sound
    /// way to prove a bare `makeTempDir` directory is idle, so it falls back to
    /// the process-boundary backstop.
    @Test("an unowned directory is never reclaimed by a sweep")
    func unownedDirectoryIsNeverSwept() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let reaper = TestScratchReaper(sweepEvery: 1_000_000)
        let dir = try makeChild(root, "unowned")
        reaper.register(dir)

        for _ in 0..<5 { reaper.sweep() }

        #expect(FileManager.default.fileExists(atPath: dir.path))
        #expect(reaper.stats.reclaimed == 0)
        #expect(reaper.isTracked(dir))
        #expect(!reaper.isOwned(dir), "nothing owns it, so nothing can prove it idle")
    }

    // MARK: - Automatic sweeping

    @Test("registration drives sweeps without a timer or a thread")
    func registrationTriggersSweeps() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let reaper = TestScratchReaper(sweepEvery: 2)
        let doomed = try makeChild(root, "auto")
        do {
            let owner = Owner()
            reaper.adopt(doomed, owner: owner)
        }

        // Four registrations at sweepEvery: 2 give two sweeps — one to observe
        // the death, one to act on it.
        for index in 0..<4 {
            reaper.register(try makeChild(root, "filler-\(index)"))
        }

        #expect(reaper.stats.sweeps >= 2)
        #expect(!FileManager.default.fileExists(atPath: doomed.path))
    }

    /// A `sweepEvery` of zero or below must not divide by zero or sweep on every
    /// single registration by accident; it is clamped to 1.
    @Test("a non-positive sweep interval is clamped rather than trapping")
    func nonPositiveSweepIntervalIsClamped() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let reaper = TestScratchReaper(sweepEvery: 0)
        reaper.register(try makeChild(root, "clamped"))
        #expect(reaper.stats.sweeps == 1)
    }

    // MARK: - Removal has to survive what the suite actually leaves behind

    /// `DownloadManagerTests` chmods a `complete/` directory to 0o300 to prove a
    /// permission failure is handled. 0o300 is `-wx------`: write is granted,
    /// READ is not — so the reflexive `chmod -R u+w` changes nothing and
    /// `removeItem` fails with EACCES. Measured on this box: an abandoned
    /// scratch root refused `rm -rf` for exactly this reason.
    @Test("an unreadable 0o300 directory is still removed")
    func removalRepairsUnreadableDirectories() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let victim = try makeChild(root, "victim")
        let locked = try makeChild(victim, "complete")
        try "payload".write(
            to: locked.appendingPathComponent("file.txt"),
            atomically: true,
            encoding: .utf8
        )
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o300], ofItemAtPath: locked.path
        )
        // The control: prove the chmod actually bites, so a future OS that
        // ignores it cannot make this test vacuously green.
        #expect(
            (try? FileManager.default.contentsOfDirectory(atPath: locked.path)) == nil,
            "0o300 must really be unreadable, or this test proves nothing"
        )

        TestScratchReaper.forceRemove(victim)

        #expect(!FileManager.default.fileExists(atPath: victim.path))
    }

    @Test("removing a directory that is already gone is not an error")
    func removalOfAMissingDirectoryIsSilent() throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let ghost = root.appendingPathComponent("never-created", isDirectory: true)
        TestScratchReaper.forceRemove(ghost)
        #expect(!FileManager.default.fileExists(atPath: ghost.path))
    }

    // MARK: - Concurrency and isolation

    /// The reaper is reached from every test in a suite that runs ~10,000 of
    /// them concurrently, so its bookkeeping has to hold under real contention
    /// rather than only under a single-threaded assertion.
    @Test("concurrent registration and sweeping keeps the ledger consistent")
    func concurrentRegistrationIsConsistent() async throws {
        let root = try makeIsolatedRoot()
        defer { TestScratchReaper.forceRemove(root) }
        let reaper = TestScratchReaper(sweepEvery: 4)
        let count = 64

        await withTaskGroup(of: Void.self) { group in
            for index in 0..<count {
                group.addTask {
                    guard let dir = try? self.makeChild(root, "concurrent-\(index)") else { return }
                    reaper.register(dir)
                    reaper.sweep()
                }
            }
        }

        let stats = reaper.stats
        #expect(stats.registered == count)
        #expect(stats.live == count, "unowned entries are retained, so none may vanish")
        #expect(stats.owned == 0)
    }

    // MARK: - The wiring, end to end through the real factories

    /// The rail that fails if `makeTestStoreWithDirectory` stops adopting. It
    /// asserts on the SHARED reaper's tracking (never on counts, which the rest
    /// of the suite is churning), so it survives full-parallel execution.
    @Test("makeTestStoreWithDirectory hands its directory to the shared reaper")
    func storeFactoryAttachesOwnership() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        #expect(
            TestScratchReaper.shared.isOwned(dir),
            """
            a store directory that is merely TRACKED is reclaimed only at             process exit — which is the whole defect. Ownership is what bounds             the peak.
            """
        )
        #expect(
            dir.lastPathComponent.hasPrefix("PlayheadTestStore-"),
            "the store prefix is what lets --breakdown separate stores from bare temp dirs"
        )
        #expect(FileManager.default.fileExists(atPath: dir.appendingPathComponent("analysis.sqlite").path))
        withExtendedLifetime(store) {}
    }

    @Test("makeTempDir registers every directory it hands out")
    func makeTempDirRegistersUnownedDirectories() throws {
        let dir = try makeTempDir(prefix: "cgka-unowned")
        #expect(TestScratchReaper.shared.isTracked(dir))
    }

    @Test("makeTempDir(ownedBy:) attaches the owner it was given")
    func makeTempDirAcceptsAnOwner() throws {
        let owner = Owner()
        let dir = try makeTempDir(prefix: "cgka-owned", ownedBy: owner)
        #expect(
            TestScratchReaper.shared.isOwned(dir),
            "ownedBy: that only registers is indistinguishable from not passing it at all"
        )
        #expect(FileManager.default.fileExists(atPath: dir.path))
        withExtendedLifetime(owner) {}
    }
}
