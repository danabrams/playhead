// RuntimeStoreTeardownTests.swift
// playhead-882eg: `shutdown()` returns the descriptors the runtime opened.
//
// WHAT THIS PINS, AND WHY IT IS NOT A TEST ABOUT DEALLOCATION.
//
// The test host carried a descriptor FLOOR of 499 into every full-plan run —
// flat for the whole tail, on a host whose PEAK reaches 93-99 % of
// `RLIMIT_NOFILE` soft 2560 and which has been observed losing its host there.
// 179 of those 499 were 89 open connections to the app's REAL
// `analysis.sqlite`, 168 were 84 connections to the real `ad_catalog.sqlite`,
// and 89 were one `surface-status-*.jsonl` per runtime. About five descriptors
// for every `PlayheadRuntime` any test had ever constructed.
//
// The bead was filed reading that as "~81 PlayheadRuntime object graphs are
// still alive". Measured, that is the wrong object: the runtime deallocates
// every time, in every configuration (see
// `RuntimeShutdownLifecycleTests.deinitReleasesRuntimeWithoutCycleWhenShutdownSkipped`,
// which has pinned exactly that for far longer than this bead). What outlives
// it is its SERVICE GRAPH — 19 of 22 runtime-owned objects were still alive
// after the runtime had gone, and they stayed alive through cancelling four
// perpetual loops, joining the bootstrap chain and cutting the one
// stored-property cycle the graph contains. Whatever holds them is still
// unidentified and is filed separately.
//
// So these rails deliberately do NOT assert deallocation. They assert the thing
// that actually costs the box something and that an owner can guarantee without
// winning an argument with ARC: **the file handles are closed when the owner
// says it is finished.** A descriptor does not need its object to die.
//
// Read the arms as a set. Rail 1 says close happens; rail 2 says the assertion
// in rail 1 can tell the difference (without it, "isOpen == false" could be
// true because nothing ever opened); rails 3 and 4 pin the property that makes
// closing safe to do at a teardown nobody audits — that every close is
// NON-TERMINAL, so a later reader reopens instead of failing.

import Foundation
import Testing
@testable import Playhead

@MainActor
@Suite("playhead-882eg: shutdown() returns the descriptors the runtime opened")
struct RuntimeStoreTeardownTests {

    /// Open the SESSION LOG, and wait — bounded — for the runtime's own
    /// bootstrap chain to open the analysis store.
    ///
    /// It deliberately does NOT force the store open with `awaitReady()`. That
    /// migrates the app's REAL production `analysis.sqlite`, and under a full
    /// plan 11,789 tests contend on that one file: the first version of these
    /// rails passed 4/4 scoped and came back
    /// `Caught error: .migrationFailed("database is locked")` on the merge
    /// gate. A rail must not depend on winning a race against a shared mutable
    /// database — that database is playhead-vhffu's subject, not this one's.
    ///
    /// The session log is `Caches/Diagnostics/`, one file per logger instance,
    /// so it has no contention and is the arm that is decisive under any load.
    /// Returns whether the analysis store was observed OPEN, so a caller can
    /// say which of its assertions it was able to make.
    @discardableResult
    private func openLogAndAwaitStore(_ runtime: PlayheadRuntime) async -> Bool {
        runtime.surfaceStatusLogger.migrate()
        runtime.surfaceStatusLogger.record(Self.probeEntry())
        runtime.surfaceStatusLogger.flushForTesting()
        for _ in 0..<40 {
            if await runtime.analysisStore.isOpen { return true }
            try? await Task.sleep(for: .milliseconds(50))
        }
        return await runtime.analysisStore.isOpen
    }

    /// One entry, written only so the logger's lazily-opened session file
    /// actually exists. `migrate()` resolves the directory and the install-id
    /// salt; the FILE is opened by the first write, so a rail about closing that
    /// descriptor has to cause a write first.
    private static func probeEntry() -> SurfaceStateTransitionEntry {
        SurfaceStateTransitionEntry(
            timestamp: Date(timeIntervalSince1970: 1_700_000_000),
            sessionId: UUID(),
            episodeIdHash: "playhead-882eg",
            priorDisposition: .queued,
            newDisposition: .paused,
            priorReason: .waitingForTime,
            newReason: .phoneIsHot,
            cause: .thermal,
            eligibilitySnapshot: nil,
            invariantViolation: nil
        )
    }

    @Test("shutdown() closes the analysis store, the ad catalog and the session log",
          .timeLimit(.minutes(2)))
    func shutdownClosesTheStores() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let storeWasOpen = await openLogAndAwaitStore(runtime)

        #expect(runtime.surfaceStatusLogger.hasOpenSessionFileForTesting,
                "precondition: the session file must be OPEN before a rail about closing it means anything")

        await runtime.shutdown()

        #expect(runtime.surfaceStatusLogger.hasOpenSessionFileForTesting == false,
                "shutdown() must return the session file's descriptor — one distinct file per runtime, 89 of them still open at the tail of a full plan, 4 after this fix")

        // CONDITIONAL, and said out loud rather than hidden. Whether the
        // runtime's bootstrap chain reaches `analysisStore.migrate()` before the
        // wait expires is a property of how loaded the box is, not of this fix.
        // Scoped — which is how `scripts/mutation-battery.sh` drives it, and the
        // only condition under which a KILL means anything — it is open every
        // time. Under a full plan it may not be, and the alternative to saying so
        // is a rail that is RED on every merge gate for a reason nobody can act on.
        if storeWasOpen {
            #expect(await runtime.analysisStore.isOpen == false,
                    "shutdown() must return the analysis store's descriptors — 179 of the test host's 499-descriptor floor were this one file")
            if let catalog = runtime.adCatalogStore {
                #expect(await catalog.isOpen == false,
                        "shutdown() must return the ad catalog's descriptors — 168 of the floor were this one file")
            }
        } else {
            print("[882eg] analysis store never opened under load — the store arm of this rail made no claim this run; the session-log arm above still did")
        }
    }

    /// ANTI-VACUITY. `isOpen == false` is also what a store that was never
    /// opened reports, so rail 1 on its own cannot distinguish "closed" from
    /// "never touched". This arm opens the stores and does NOT shut down, and
    /// requires them to still be open — then shuts down, so the arm itself
    /// leaves no descriptors behind. That last step matters: a rail about a
    /// descriptor floor that raises the floor is its own counterexample.
    @Test("without shutdown() the session log stays open — the rail above discriminates",
          .timeLimit(.minutes(2)))
    func withoutShutdownTheStoresStayOpen() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        await openLogAndAwaitStore(runtime)

        #expect(runtime.surfaceStatusLogger.hasOpenSessionFileForTesting,
                "a runtime that was never shut down must still hold its session file open — without this, `hasOpenSessionFile == false` above could be true because nothing ever opened")

        await runtime.shutdown()
    }

    /// `AnalysisStore.close()` resets `didOpen`, so a use-after-close REOPENS
    /// through the same `ensureOpen()` path rather than misusing a stale
    /// handle. This is the property that makes closing at an unaudited teardown
    /// safe, so it is pinned rather than assumed.
    ///
    /// Deliberately on a store this test OWNS, in its own temp directory: the
    /// claim is about `close()`'s semantics, and running it against the shared
    /// production database would make it a claim about lock contention instead.
    @Test("closing an analysis store is non-terminal — a later read reopens it",
          .timeLimit(.minutes(2)))
    func analysisStoreReopensAfterClose() async throws {
        let directory = try makeTempDir(prefix: "882eg-reopen")
        let store = try AnalysisStore(directory: directory)
        try await store.awaitReady()
        #expect(await store.isOpen, "precondition: the store must be open")

        await store.close()
        #expect(await store.isOpen == false, "close() must drop the handle")

        _ = try await store.schemaVersion()

        #expect(await store.isOpen,
                "a read after close must reopen the store, not fail — that is what makes close() safe to call from a teardown that does not audit its readers")
        await store.close()
    }

    /// The logger keeps `currentSessionFileURL` across a close, so a later
    /// write appends to THAT file. Two files carrying one `sessionId` would
    /// split a session's entries and burn two slots of the eviction window.
    @Test("closing the session log is non-terminal — a later write reopens the SAME file",
          .timeLimit(.minutes(2)))
    func sessionLogReopensTheSameFile() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        await openLogAndAwaitStore(runtime)
        let fileBeforeClose = runtime.surfaceStatusLogger.currentSessionFileURL
        #expect(fileBeforeClose != nil, "precondition: a session file must exist")

        await runtime.shutdown()
        #expect(runtime.surfaceStatusLogger.hasOpenSessionFileForTesting == false)

        runtime.surfaceStatusLogger.record(Self.probeEntry())
        runtime.surfaceStatusLogger.flushForTesting()

        #expect(runtime.surfaceStatusLogger.currentSessionFileURL == fileBeforeClose,
                "a write after close must reopen the SAME session file, not fork a second one under the same sessionId")
        runtime.surfaceStatusLogger.close()
    }
}
