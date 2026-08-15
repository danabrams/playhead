// ZeroCoverageRecoveryRoutingTests.swift
// playhead-rqgr.
//
// `work_journal.eventType` is the input to a terminal-vs-resume decision, not
// the "observability gravy" `AnalysisJobRunner.emitTranscriptionTimeoutJournal`
// called it. `AnalysisCoordinator.recoverOrphans` reads the last row for a
// stranded `{episode, generation}` and routes on it: terminal clears the lease
// with no requeue, anything else resumes.
//
// A zero-coverage exit writes itself down twice — as an `AnalysisOutcome
// .StopReason` for the scheduler, and as that journal row. They disagreed for
// exactly the case the routing exists for: an interruption reported
// `.interrupted` (costs no attempt, the retry comes back — playhead-ngev) and
// journaled `.failed` (the work is over, do not requeue). Two records of one
// event, and the one a cold launch consults was the wrong one.
//
// What is pinned here:
//
//   1. THE MAPPING. `EventType.orphanRecoveryRouting` is the single
//      declaration of which arm each event selects — the coordinator, and the
//      test harness that replays it, both read it rather than spelling out
//      their own `case` lists.
//   2. THE AGREEMENT. Exhaustively over every failure class x termination x
//      observation, the journal event's arm and the stop reason say the same
//      thing about whether this exit was the job's fault.
//   3. THE DIRECTION. `.failed` and `.finalized` are still terminal. The fix
//      is that an interrupted run stops claiming it failed — not that failure
//      stops being terminal, which is the constraint playhead-se0x and
//      playhead-e6d3 rest on.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-rqgr — the zero-coverage journal row is a recovery decision")
struct ZeroCoverageRecoveryRoutingTests {

    // MARK: - 1. The mapping

    /// THE TERMINAL HALF, PINNED ON ITS OWN. Split from the resume half
    /// deliberately: the two directions of this mapping are different defects
    /// with opposite harms, and a single test cannot tell a reader which one
    /// went wrong. An event that wrongly reads TERMINAL strands a job that
    /// nothing will ever dispatch again.
    ///
    /// Widening this arm is also the fix rqgr explicitly did NOT take —
    /// `.failed` must stay terminal, or a job that can never converge retries
    /// forever (playhead-se0x / playhead-e6d3).
    @Test("the terminal arm: failed and finalized both stop")
    func failedAndFinalizedAreTerminal() {
        #expect(WorkJournalEntry.EventType.failed.orphanRecoveryRouting == .terminalNoRequeue)
        #expect(WorkJournalEntry.EventType.finalized.orphanRecoveryRouting == .terminalNoRequeue)
    }

    /// THE RESUME HALF. An event that wrongly reads TERMINAL is what this bead
    /// is about, and `.preempted` is the one it now depends on: it is the row
    /// an interrupted zero-coverage run writes, and the row
    /// `AnalysisWorkScheduler`'s own `.interrupted` arm has written since
    /// playhead-ngev.
    @Test("the resume arm: acquired, checkpointed and preempted all requeue")
    func acquiredCheckpointedAndPreemptedResume() {
        #expect(WorkJournalEntry.EventType.preempted.orphanRecoveryRouting == .requeue)
        #expect(WorkJournalEntry.EventType.checkpointed.orphanRecoveryRouting == .requeue)
        #expect(WorkJournalEntry.EventType.acquired.orphanRecoveryRouting == .requeue)
    }

    /// THE SET EQUALITIES, SPLIT OUT OF THE TWO TESTS ABOVE, AND THE SPLIT WAS
    /// FORCED BY MEASUREMENT RATHER THAN TASTE.
    ///
    /// A mutation that moves ONE event between the arms breaks BOTH set
    /// equalities at once — the terminal set gains a member and the resume set
    /// loses one, or the reverse. So while the per-event assertions and the set
    /// assertions lived in the same two tests, the two opposite mis-scopings
    /// (rails RQ03 and RQ04: `.preempted` reclassified as terminal, and
    /// `.failed` reclassified as resumable) killed an identical set of tests
    /// and neither had a kill of its own. Measured, not predicted: RQ03 and
    /// RQ04 were run alone and their kill sets were `{resume arm, terminal
    /// arm, every event declares…, …}` in both cases.
    ///
    /// Separated, each direction has a witness. `.preempted` still requeuing
    /// is a claim only RQ03 can falsify; `.failed` still stopping is a claim
    /// only RQ04 can. The set equalities remain, here, as the belt that
    /// catches an event moved into NEITHER arm or into both.
    @Test("the two arms partition the event space, with nothing in neither")
    func theTwoArmsPartitionTheEventSpace() {
        let terminal = Set(
            WorkJournalEntry.EventType.allCases
                .filter { $0.orphanRecoveryRouting == .terminalNoRequeue }
        )
        let resumable = Set(
            WorkJournalEntry.EventType.allCases
                .filter { $0.orphanRecoveryRouting == .requeue }
        )
        #expect(terminal == [.finalized, .failed], "terminal set is \(terminal.map(\.rawValue).sorted())")
        #expect(
            resumable == [.acquired, .checkpointed, .preempted],
            "resume set is \(resumable.map(\.rawValue).sorted())"
        )
        #expect(terminal.isDisjoint(with: resumable))
        #expect(terminal.union(resumable) == Set(WorkJournalEntry.EventType.allCases))
    }

    /// The belt over both halves, exhaustive over `EventType.allCases`, so a
    /// new event added to the enum arrives here as a compile error in the
    /// switch below rather than as a silent membership of whichever arm
    /// somebody edited last.
    @Test("every event declares an orphan-recovery arm")
    func eventRoutingIsTheDeclaredMapping() {
        for event in WorkJournalEntry.EventType.allCases {
            let expected: WorkJournalEntry.OrphanRecoveryRouting
            switch event {
            case .finalized, .failed:
                expected = .terminalNoRequeue
            case .acquired, .checkpointed, .preempted:
                expected = .requeue
            }
            #expect(
                event.orphanRecoveryRouting == expected,
                "\(event.rawValue) routes \(event.orphanRecoveryRouting.rawValue)"
            )
        }
    }

    // MARK: - 2. The agreement

    /// THE INVARIANT THE BEAD EXISTS FOR, AND THE REASON THE TWO RECORDS ARE
    /// NOW MINTED BY ONE EXPRESSION.
    ///
    /// `stopReason` decides whether the scheduler spends one of the job's five
    /// permanent attempts. `journalEvent` decides what a cold launch does if
    /// the process dies before that lands. They are two statements about one
    /// question — was this exit the job's fault? — so a run excused by one and
    /// condemned by the other is incoherent whichever way round it happens.
    ///
    /// Exhaustive rather than sampled: the shipped defect was reachable from
    /// every failure class, because the class was never what decided it.
    @Test(
        "the journal event and the stop reason never disagree",
        arguments: TranscriptFailureClass.allCases,
        TranscriptRunTermination.allCases
    )
    func journalEventAgreesWithStopReason(
        failureClass: TranscriptFailureClass,
        termination: TranscriptRunTermination
    ) {
        for observation in AnalysisJobRunner.TranscriptRunObservation.allCases {
            let disposition = AnalysisJobRunner.zeroCoverageDisposition(
                failure: TranscriptFailureReason(
                    failureClass: failureClass, termination: termination
                ),
                observation: observation
            )
            let spendsNoAttempt: Bool
            switch disposition.stopReason {
            case .interrupted: spendsNoAttempt = true
            default: spendsNoAttempt = false
            }
            let resumes = disposition.journalEvent.orphanRecoveryRouting == .requeue

            #expect(
                spendsNoAttempt == resumes,
                """
                \(failureClass.rawValue)/\(termination.rawValue)/\(observation.rawValue): the \
                outcome says \(disposition.stopReason) while the journal row says \
                \(disposition.journalEvent.rawValue), which a cold launch routes \
                \(disposition.journalEvent.orphanRecoveryRouting.rawValue). One event, two \
                records, and a process death between them picks the wrong one
                """
            )
        }
    }

    /// The same invariant for the two exits that carry no failure at all — a
    /// 300 s silence and a `.completed` over an empty transcript. Nobody
    /// reported, so nothing excuses them: both records must say failure.
    @Test("an exit nobody reported on is terminal on both records")
    func unreportedExitIsTerminalOnBothRecords() {
        for observation in AnalysisJobRunner.TranscriptRunObservation.allCases {
            let disposition = AnalysisJobRunner.zeroCoverageDisposition(
                failure: nil,
                observation: observation
            )
            #expect(disposition.journalEvent == .failed)
            #expect(
                disposition.journalEvent.orphanRecoveryRouting == .terminalNoRequeue,
                "an unreported \(observation.rawValue) would be resumed forever"
            )
            guard case .failed = disposition.stopReason else {
                Issue.record("an unreported exit reported \(disposition.stopReason)")
                return
            }
        }
    }

    // MARK: - 3. The direction: what the interrupted row says, and what it does not

    /// `.preempted`, NOT `.checkpointed`, and the difference is not cosmetic.
    ///
    /// Both route to resume, so either would have fixed the strand — which is
    /// exactly why it is worth pinning. `.checkpointed` means "the owner
    /// persisted resumable progress", and a zero-coverage pass persisted
    /// nothing; writing it would be a second value naming one thing and read
    /// as another, in a table that already has three `checkpointed` rows
    /// against 294 `acquired`. `.preempted` is what the event IS — the owner
    /// released because a live listener took the shared engine — and it is the
    /// same event `AnalysisWorkScheduler`'s own `.interrupted` arm writes for
    /// this exit via `emitJournalPreempted(cause: .userPreempted)`.
    @Test(
        "an interrupted run journals .preempted",
        arguments: TranscriptFailureClass.allCases
    )
    func interruptedRunJournalsPreempted(failureClass: TranscriptFailureClass) {
        let disposition = AnalysisJobRunner.zeroCoverageDisposition(
            failure: TranscriptFailureReason(
                failureClass: failureClass, termination: .interrupted
            ),
            observation: .engineReported
        )
        #expect(
            disposition.journalEvent == .preempted,
            """
            \(failureClass.rawValue) interrupted journaled \
            \(disposition.journalEvent.rawValue) — the runner claimed a listener's scrub \
            was the job's own doing
            """
        )
    }

    /// The consequence, pinned separately from the value. Whatever the
    /// interrupted row is spelled, it has to be an event a cold launch
    /// RESUMES — that is the property the fix is for, and it can break either
    /// by the runner writing a different event or by the routing table
    /// re-classifying the one it writes.
    @Test(
        "an interrupted run's row resumes on cold launch",
        arguments: TranscriptFailureClass.allCases
    )
    func interruptedRunResumesOnColdLaunch(failureClass: TranscriptFailureClass) {
        let disposition = AnalysisJobRunner.zeroCoverageDisposition(
            failure: TranscriptFailureReason(
                failureClass: failureClass, termination: .interrupted
            ),
            observation: .engineReported
        )
        #expect(
            disposition.journalEvent.orphanRecoveryRouting == .requeue,
            """
            \(failureClass.rawValue) interrupted writes \
            \(disposition.journalEvent.rawValue), which a cold launch routes \
            \(disposition.journalEvent.orphanRecoveryRouting.rawValue) — the lease is \
            cleared and the job is never re-dispatched
            """
        )
    }

    /// THE CONTROL, AND IT IS THE ONE THAT MUST NOT MOVE. A run that reached
    /// its own conclusion still journals `.failed`, whatever its class — even
    /// a class whose NAME is an interruption. If the loop finished, the run
    /// was not displaced, and a job that can never converge has to be able to
    /// stop.
    @Test(
        "control: a run that concluded on its own still journals .failed",
        arguments: TranscriptFailureClass.allCases
    )
    func concludedRunJournalsFailed(failureClass: TranscriptFailureClass) {
        let disposition = AnalysisJobRunner.zeroCoverageDisposition(
            failure: TranscriptFailureReason(
                failureClass: failureClass, termination: .ranToConclusion
            ),
            observation: .engineReported
        )
        #expect(
            disposition.journalEvent == .failed,
            """
            \(failureClass.rawValue) that ran to conclusion journaled \
            \(disposition.journalEvent.rawValue) — rqgr's fix was that an INTERRUPTED run \
            stops claiming it failed, not that a failed one stops saying so
            """
        )
        guard case .failed = disposition.stopReason else {
            Issue.record("\(failureClass.rawValue) concluded reported \(disposition.stopReason)")
            return
        }
    }

    /// The same control, on the consequence rather than the value: whatever a
    /// concluded run's row is spelled, a cold launch must NOT requeue it, or a
    /// permanently broken episode retries forever.
    @Test(
        "control: a run that concluded on its own stays terminal on cold launch",
        arguments: TranscriptFailureClass.allCases
    )
    func concludedRunStaysTerminalOnColdLaunch(failureClass: TranscriptFailureClass) {
        let disposition = AnalysisJobRunner.zeroCoverageDisposition(
            failure: TranscriptFailureReason(
                failureClass: failureClass, termination: .ranToConclusion
            ),
            observation: .engineReported
        )
        #expect(
            disposition.journalEvent.orphanRecoveryRouting == .terminalNoRequeue,
            """
            \(failureClass.rawValue) that ran to conclusion writes \
            \(disposition.journalEvent.rawValue), which a cold launch routes \
            \(disposition.journalEvent.orphanRecoveryRouting.rawValue) — so a permanently \
            broken episode is retried forever
            """
        )
    }

    /// The `lastErrorCode` half is unchanged by all of this. It was the thing
    /// the old doc comment called the primary signal, and the point is not
    /// that it is wrong — it is that orphan recovery never reads it, so being
    /// right there bought nothing.
    @Test("the diagnosis in lastErrorCode survives the routing change")
    func stopReasonCodeStillNamesTheClass() {
        for termination in TranscriptRunTermination.allCases {
            let disposition = AnalysisJobRunner.zeroCoverageDisposition(
                failure: TranscriptFailureReason(
                    failureClass: .modelNotLoaded, termination: termination
                ),
                observation: .engineReported
            )
            let code: String
            switch disposition.stopReason {
            case .interrupted(let value), .failed(let value): code = value
            default:
                Issue.record("unexpected stop reason \(disposition.stopReason)")
                return
            }
            #expect(code == "transcription:model_not_loaded")
        }
    }

    /// The cause column is orthogonal and stays that way: no recovery arm
    /// reads it, and rqgr did not move it. A scrub still journals
    /// `pipeline_error` — which is wrong, and is playhead-2qe4's, not this
    /// bead's. Pinned so that fix is visible as a change rather than absorbed.
    @Test("the cause column is untouched by the routing fix (playhead-2qe4 still open)")
    func causeIsUnchanged() {
        let disposition = AnalysisJobRunner.zeroCoverageDisposition(
            failure: TranscriptFailureReason(
                failureClass: .cancelled, termination: .interrupted
            ),
            observation: .engineReported
        )
        #expect(disposition.journalCause == .pipelineError)
        #expect(
            disposition.journalCause
                == AnalysisJobRunner.journalCause(
                    failure: TranscriptFailureReason(
                        failureClass: .cancelled, termination: .interrupted
                    ),
                    observation: .engineReported
                )
        )
    }
}
