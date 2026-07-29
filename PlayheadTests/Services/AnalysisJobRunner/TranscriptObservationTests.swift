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

    // MARK: - playhead-ngev: "the engine said it finished" is a THIRD fact

    /// A `.completed` over an empty transcript and a 300 s silence both leave
    /// the fold with `(coverage: 0, failure: nil)`, so the journal row could
    /// not tell them apart — and they are different bugs in different files
    /// ("the engine believes it succeeded" vs "the engine never spoke").
    @Test(".completed is recorded even when it carried no coverage")
    func completedIsObservableAtZeroCoverage() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([.completed(analysisAssetId: assetId)]),
            assetId: assetId,
            persistedCoverage: { 0 }
        )
        #expect(result.coverage == 0)
        #expect(result.failure == nil)
        #expect(
            result.sawCompleted,
            """
            the engine's own `.completed` was not recorded, so this row is \
            indistinguishable from a five-minute silence — the exact overload \
            `failure_observation` exists to remove
            """
        )
    }

    /// The other two shapes, so the flag cannot be a constant.
    @Test("a stream that ends with no terminal event saw no completion")
    func silenceIsNotACompletion() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([]), assetId: assetId, persistedCoverage: { 0 }
        )
        #expect(result.sawCompleted == false)
    }

    @Test("a .failed is not a completion")
    func failureIsNotACompletion() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([
                .failed(
                    analysisAssetId: assetId,
                    reason: TranscriptFailureReason(failureClass: .noShards)
                )
            ]),
            assetId: assetId,
            persistedCoverage: { 0 }
        )
        #expect(result.sawCompleted == false)
    }

    /// Another asset's `.completed` must not be recorded as ours. The engine's
    /// stream is shared, so this is routine — and crediting it would make a
    /// silent timeout report `engine_completed_zero`, sending a reader to the
    /// engine's completion path for a run the engine never finished.
    @Test("another asset's .completed is not recorded as ours")
    func anotherAssetsCompletionIsNotOurs() async throws {
        let result = await AnalysisJobRunner.observeTranscriptEvents(
            stream: stream([.completed(analysisAssetId: "someone-else")]),
            assetId: assetId,
            persistedCoverage: { 0 }
        )
        #expect(result.sawCompleted == false)
    }

    // MARK: - playhead-ngev: the observation vocabulary

    /// The three-case mapping, stated as a table. Absence of a `failure_class`
    /// was overloaded across four unrelated diagnoses; this is what separates
    /// them, and it is derived from facts the runner always holds.
    @Test("the observation names which of the three endings happened")
    func observationClassification() {
        typealias Observation = AnalysisJobRunner.TranscriptRunObservation

        #expect(
            Observation.classify(
                failure: TranscriptFailureReason(failureClass: .silentShard),
                sawCompleted: false
            ) == .engineReported
        )
        #expect(
            Observation.classify(failure: nil, sawCompleted: true) == .engineCompletedZero
        )
        #expect(
            Observation.classify(failure: nil, sawCompleted: false) == .engineSilentTimeout
        )
        // A reported failure wins over a completion flag: the fold breaks on
        // `.failed`, so the two cannot both describe this run.
        #expect(
            Observation.classify(
                failure: TranscriptFailureReason(failureClass: .noShards),
                sawCompleted: true
            ) == .engineReported
        )
    }

    /// The vocabulary crosses the bundle projection, so it is held to the same
    /// bar as `TranscriptFailureClass`: fixed identifiers only.
    @Test("every observation raw value is a bare snake_case identifier")
    func observationRawValuesAreIdentifierShaped() {
        let shape = #/^[a-z][a-z0-9_]*$/#
        #expect(AnalysisJobRunner.TranscriptRunObservation.allCases.count == 3)
        for observation in AnalysisJobRunner.TranscriptRunObservation.allCases {
            #expect(
                (try? shape.wholeMatch(in: observation.rawValue)) != nil,
                "'\(observation.rawValue)' is not identifier-shaped"
            )
        }
    }

    // MARK: - playhead-ngev: `cause` is no longer hardcoded

    /// A ROW MUST NOT CONTRADICT ITSELF. `cause` was a hardcoded `.asrFailed`
    /// with no reference to the failure, so a row could read
    /// `cause = asr_failed` beside `failure_class = no_shards` — and an
    /// aggregate counts the column that is wrong.
    @Test(
        "a class that proves the recognizer never ran is not journaled as an ASR failure",
        arguments: [
            TranscriptFailureClass.noShards,
            .speechEngineNotReady,
            .modelNotLoaded,
            .persistenceFailed,
            .cancelled,
            .stopped,
        ]
    )
    func nonRecognitionFailuresAreJournaledAsPipelineErrors(
        failureClass: TranscriptFailureClass
    ) {
        let cause = AnalysisJobRunner.journalCause(
            failure: TranscriptFailureReason(failureClass: failureClass),
            observation: .engineReported
        )
        #expect(
            cause == .pipelineError,
            "\(failureClass.rawValue) was journaled as \(cause.rawValue)"
        )
    }

    /// The positive control. Reserving `.asrFailed` is worthless if nothing
    /// ever gets it — the label would still name an absence, just a rarer one.
    @Test(
        "a class that means recognition ran keeps the ASR cause",
        arguments: [
            TranscriptFailureClass.transcriptionFailed,
            .vadFailed,
            .analyzerSessionFailure,
        ]
    )
    func recognitionFailuresKeepTheASRCause(failureClass: TranscriptFailureClass) {
        let cause = AnalysisJobRunner.journalCause(
            failure: TranscriptFailureReason(failureClass: failureClass),
            observation: .engineReported
        )
        #expect(cause == .asrFailed, "\(failureClass.rawValue) was journaled as \(cause.rawValue)")
    }

    /// A silent timeout has no reporter at all, so nothing in it can support a
    /// claim about ASR. This is the row the bead was filed over: 300 s of
    /// silence, journaled as an ASR failure.
    @Test("a silent timeout is a pipeline error, never an ASR failure")
    func silentTimeoutIsAPipelineError() {
        #expect(
            AnalysisJobRunner.journalCause(failure: nil, observation: .engineSilentTimeout)
                == .pipelineError
        )
        // Even if a reason somehow rode along with a timeout, the timeout wins:
        // no one reported, so no one can be blamed.
        #expect(
            AnalysisJobRunner.journalCause(
                failure: TranscriptFailureReason(failureClass: .vadFailed),
                observation: .engineSilentTimeout
            ) == .pipelineError
        )
    }

    @Test("a completion over an empty transcript is a pipeline error")
    func completedZeroIsAPipelineError() {
        #expect(
            AnalysisJobRunner.journalCause(failure: nil, observation: .engineCompletedZero)
                == .pipelineError
        )
    }

    // MARK: - playhead-ngev: the runner must not stop an engine it no longer owns

    /// The zero-coverage stop exists to fence an ORPHANED engine. An
    /// interrupted run is the one case where the engine is not orphaned: it is
    /// shared, and the cancel came from a live owner that has already re-tasked
    /// it (a scrub, a speed change, a different episode). Stopping it there
    /// cancels the listener's own transcription and gates the asset against the
    /// appends that owner is about to make.
    ///
    /// This only became reachable with this bead — an interruption used to be
    /// silent, so the runner sat out its 300 s timeout and the successor was
    /// usually finished by the time the stop landed.
    @Test("an interrupted run does not tear down the engine a live owner re-tasked")
    func interruptedRunLeavesTheEngineAlone() {
        #expect(
            AnalysisJobRunner.shouldStopEngine(
                after: TranscriptFailureReason(
                    failureClass: .cancelled, termination: .interrupted
                )
            ) == false
        )
        // Including when the interruption carries a real shard diagnosis: it is
        // the termination that says someone else owns the engine now.
        #expect(
            AnalysisJobRunner.shouldStopEngine(
                after: TranscriptFailureReason(
                    failureClass: .modelNotLoaded, code: nil, failedShardCount: 4,
                    termination: .interrupted
                )
            ) == false
        )
    }

    /// The control: every other zero-coverage exit still stops the engine.
    /// Without it the fix reads "never stop", which reinstates the orphaned
    /// writer playhead-5uvz.5 fenced.
    @Test("control: a run that concluded, or reported nothing, still stops the engine")
    func nonInterruptedRunsStillStopTheEngine() {
        #expect(AnalysisJobRunner.shouldStopEngine(after: nil))
        #expect(
            AnalysisJobRunner.shouldStopEngine(
                after: TranscriptFailureReason(failureClass: .vadFailed, failedShardCount: 3)
            )
        )
    }
}
