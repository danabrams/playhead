// TranscriptObservationTests.swift
// playhead-8ysk, review round 2.
//
// `AnalysisJobRunner.observeTranscriptEvents` is the fold that turns the
// engine's event stream into the `(coverage, failure)` pair the rest of `run`
// branches on. Round 1 left it untested and said why: the runner holds a
// concrete `TranscriptEngineService` and a hardcoded 300 s timeout, so no test
// can drive the real stream. That is true of the RUNNER; it is not true of
// this logic, which needs only an `AsyncStream` and a coverage lookup.
//
// Two things are pinned here that nothing else could reach:
//
//   1. `.failed` is TERMINAL. Without the break the runner sits in its task
//      group until the 300 s timeout fires and an instant, named failure is
//      re-labelled `task_expired` — the failure mode the call-site comment
//      warns about and which no test measured.
//
//   2. `.failed` SUPPRESSES the stale-coverage fallback. This is a defect
//      found in round 2, not a refactor: `fastTranscriptCoverageEndTime` is
//      cumulative across passes over one asset, so on a RETRY — the exact
//      shape of this bead's incident — a stale non-zero value made
//      `transcriptCoverage != 0`, skipping the entire zero-coverage branch.
//      No `work_journal` row, no `failure_class`, and `lastErrorCode` never
//      naming the cause. Part B of this bead would have reported nothing at
//      all in its own headline scenario.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-8ysk — the runner's transcript-stream observation")
struct TranscriptObservationTests {

    private let assetId = "asset-under-observation"

    /// Build a finite stream of `events`, then close. A finite stream matters:
    /// it is what lets the "no terminal event" cases terminate at all, and a
    /// build that failed to break on `.failed` would still finish here — so
    /// the terminality assertions are about the RETURNED VALUE, not about the
    /// test hanging.
    private func stream(_ events: [TranscriptEngineEvent]) -> AsyncStream<TranscriptEngineEvent> {
        AsyncStream { continuation in
            for event in events { continuation.yield(event) }
            continuation.finish()
        }
    }

    // MARK: - The round-2 defect

    /// THE REGRESSION. A retry of an asset that once made progress: the store
    /// still reports 812 s of coverage from an earlier pass, and this pass
    /// reported `.failed`. The observation must report ZERO coverage and carry
    /// the reason, because the call site gates every diagnostic it writes on
    /// `transcriptCoverage == 0`.
    @Test(".failed does not inherit coverage a previous pass left behind")
    func failedIgnoresStalePersistedCoverage() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([
                .failed(
                    analysisAssetId: assetId,
                    reason: TranscriptFailureReason(
                        failureClass: .silentShard, code: nil, failedShardCount: 7
                    )
                )
            ]),
            assetId: assetId,
            persistedCoverage: { 812 }
        )

        #expect(
            result.coverage == 0,
            """
            a `.failed` run inherited \(result.coverage)s of coverage from an \
            earlier pass over the same asset. The call site writes the journal \
            row, the `failure_class` and the named `lastErrorCode` ONLY when \
            coverage is zero, so this silently destroys the failure this bead \
            exists to name
            """
        )
        #expect(result.failure?.failureClass == .silentShard)
        #expect(result.failure?.failedShardCount == 7)
    }

    /// The control that keeps the fix honest: the fallback still runs when
    /// there was no failure. Deleting the fallback entirely would also make
    /// the test above pass, and would lose a genuine `.completed`-less
    /// recovery path.
    @Test("control: with no failure, coverage still falls back to the store")
    func fallbackStillAppliesWithoutAFailure() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([]),
            assetId: assetId,
            persistedCoverage: { 812 }
        )
        #expect(
            result.coverage == 812,
            "a stream that ended with no terminal event must still consult the store"
        )
        #expect(result.failure == nil)
    }

    // MARK: - Terminality

    /// `.failed` must stop the fold. Proved by putting a `.completed` for the
    /// SAME asset behind it with non-zero store coverage: a build that did not
    /// break would go on to consume it, return 500 s and drop the reason.
    @Test(".failed is terminal — nothing after it is consumed")
    func failedIsTerminal() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([
                .failed(
                    analysisAssetId: assetId,
                    reason: TranscriptFailureReason(failureClass: .modelNotLoaded)
                ),
                .completed(analysisAssetId: assetId)
            ]),
            assetId: assetId,
            persistedCoverage: { 500 }
        )

        #expect(
            result.failure?.failureClass == .modelNotLoaded,
            """
            the observation continued past `.failed` and was overwritten by a \
            later event. In production nothing follows a `.failed`, so the \
            observable symptom is instead a 300 s wait for an event that will \
            never come — an instant named failure re-labelled `task_expired`
            """
        )
        #expect(result.coverage == 0)
    }

    /// The symmetric control on `.completed`, which reads coverage from the
    /// store by design.
    @Test("control: .completed is terminal and reports the store's coverage")
    func completedReportsStoreCoverage() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([
                .completed(analysisAssetId: assetId),
                .failed(
                    analysisAssetId: assetId,
                    reason: TranscriptFailureReason(failureClass: .unknown)
                )
            ]),
            assetId: assetId,
            persistedCoverage: { 1_337 }
        )
        #expect(result.coverage == 1_337)
        #expect(result.failure == nil, "a `.failed` after `.completed` must not be consumed")
    }

    // MARK: - Asset scoping

    /// The engine's stream is shared across assets, so a `.failed` belonging
    /// to someone else must be ignored entirely — not treated as terminal for
    /// us, and not allowed to zero our coverage.
    @Test(".failed for another asset is ignored, and ours still resolves")
    func failedForAnotherAssetIsIgnored() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([
                .failed(
                    analysisAssetId: "someone-else",
                    reason: TranscriptFailureReason(failureClass: .speechEngineNotReady)
                ),
                .completed(analysisAssetId: assetId)
            ]),
            assetId: assetId,
            persistedCoverage: { 240 }
        )
        #expect(result.failure == nil, "another asset's failure was attributed to this run")
        #expect(result.coverage == 240, "our own `.completed` must still be reached")
    }

    /// And a `.completed` for another asset does not end our observation.
    @Test("control: .completed for another asset is ignored")
    func completedForAnotherAssetIsIgnored() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([
                .completed(analysisAssetId: "someone-else"),
                .failed(
                    analysisAssetId: assetId,
                    reason: TranscriptFailureReason(failureClass: .noShards)
                )
            ]),
            assetId: assetId,
            persistedCoverage: { 999 }
        )
        #expect(result.failure?.failureClass == .noShards)
        #expect(result.coverage == 0)
    }

    /// `.chunksPersisted` is not terminal for either arm.
    @Test("control: .chunksPersisted does not end the observation")
    func chunksPersistedIsNotTerminal() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([
                .chunksPersisted(analysisAssetId: assetId, chunks: []),
                .failed(
                    analysisAssetId: assetId,
                    reason: TranscriptFailureReason(failureClass: .vadFailed, code: 42)
                )
            ]),
            assetId: assetId,
            persistedCoverage: { 77 }
        )
        #expect(result.failure?.failureClass == .vadFailed)
        #expect(result.failure?.code == 42)
        #expect(result.coverage == 0)
    }
}
