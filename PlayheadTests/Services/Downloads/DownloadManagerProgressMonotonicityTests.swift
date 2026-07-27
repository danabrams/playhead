// DownloadManagerProgressMonotonicityTests.swift
// playhead-y3q5: the background-progress completion-reset reopened the
// monotonicity window it guards.
//
// `DownloadManager.broadcastBackgroundProgress` drops stale, out-of-order
// background ticks via a per-episode high-water mark so the delivered fraction
// is monotonic within a transfer. Pre-y3q5, the completion tick RESET the
// high-water to `nil`. Because each `didWriteData` callback hops to the actor
// via an unstructured `Task` with NO ordering guarantee, a straggler earlier
// tick that lost the race to the 100% tick then read `nil ?? 0 = 0`, passed the
// `>= highWater` guard, and broadcast a REGRESSED (<100%) fraction AFTER 100% —
// the exact non-monotonicity the guard exists to prevent. The fix pins the
// high-water at `totalBytes` on completion (via a `min` cap) so the straggler
// is dropped; the slot is cleared for a re-download by the fresh-start reset in
// `backgroundDownload`.

import Foundation
import Testing

@testable import Playhead

@Suite("DownloadManager – background-progress monotonicity (playhead-y3q5)")
struct DownloadManagerProgressMonotonicityTests {

    @Test("a straggler tick racing after the 100% completion tick is dropped, never regressing the fraction")
    func stragglerAfterCompletionIsDropped() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)

        let stream = await manager.progressUpdates()
        var iterator = stream.makeAsyncIterator()

        // Normal in-order ticks: 50% then 100%.
        await manager.broadcastBackgroundProgress(episodeId: "ep", bytesWritten: 500, totalBytes: 1000)
        let a = await iterator.next()
        await manager.broadcastBackgroundProgress(episodeId: "ep", bytesWritten: 1000, totalBytes: 1000)
        let b = await iterator.next()

        // The straggler: an earlier (<100%) tick whose actor-hop Task lost the
        // race to the 100% tick's. Pre-y3q5 the completion reset the high-water
        // to nil, so this read 0, passed the guard, and broadcast a regressed
        // 0.9 AFTER 100%. Post-fix the totalBytes-pinned high-water drops it —
        // no yield.
        await manager.broadcastBackgroundProgress(episodeId: "ep", bytesWritten: 900, totalBytes: 1000)
        // A subsequent legitimate 100% tick unblocks the iterator. Its yield
        // must be 1.0 — NOT the dropped straggler's 0.9.
        await manager.broadcastBackgroundProgress(episodeId: "ep", bytesWritten: 1000, totalBytes: 1000)
        let c = await iterator.next()

        #expect(a?.fractionCompleted == 0.5)
        #expect(b?.fractionCompleted == 1.0)
        #expect(
            c?.fractionCompleted == 1.0,
            "a racing straggler (<100%) after completion must be dropped; a regressed fraction here is the y3q5 bug"
        )
    }

    @Test("in-order ticks all flow through, still monotonic (fix does not over-drop)")
    func inOrderTicksStillFlow() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)

        let stream = await manager.progressUpdates()
        var iterator = stream.makeAsyncIterator()

        await manager.broadcastBackgroundProgress(episodeId: "ep", bytesWritten: 250, totalBytes: 1000)
        let a = await iterator.next()
        await manager.broadcastBackgroundProgress(episodeId: "ep", bytesWritten: 600, totalBytes: 1000)
        let b = await iterator.next()
        await manager.broadcastBackgroundProgress(episodeId: "ep", bytesWritten: 1000, totalBytes: 1000)
        let c = await iterator.next()

        #expect(a?.fractionCompleted == 0.25)
        #expect(b?.fractionCompleted == 0.6)
        #expect(c?.fractionCompleted == 1.0)
    }
}
