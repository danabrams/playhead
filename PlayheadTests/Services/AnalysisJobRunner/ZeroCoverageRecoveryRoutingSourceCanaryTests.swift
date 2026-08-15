// ZeroCoverageRecoveryRoutingSourceCanaryTests.swift
// playhead-rqgr.
//
// TWO SITES NO RUNTIME TEST ON THIS HARNESS CAN REACH, AND BOTH ARE WHERE THE
// DEFECT LIVED.
//
//   1. `AnalysisJobRunner.emitTranscriptionTimeoutJournal` is private, and the
//      runner holds a concrete `TranscriptEngineService` with no protocol seam
//      and a hardcoded 300 s timeout — the same constraint
//      `AnalysisJobRunnerSubscribeBeforeStartTests` and playhead-8ysk already
//      document. `ZeroCoverageRecoveryRoutingTests` can prove the DISPOSITION
//      is right; nothing at runtime can prove the emission site uses it. The
//      shipped defect was precisely a correct derivation sitting beside a
//      literal.
//
//   2. `AnalysisCoordinator.recoverOrphans` needs ~6 collaborator services to
//      construct, which is why `EpisodeLeaseAndWorkJournalTests` replays its
//      policy against the store instead of calling it. A replayed policy pins
//      the replica. Both sides now read
//      `WorkJournalEntry.EventType.orphanRecoveryRouting`, so the duplication
//      is one rule rather than two — and this canary is what holds the
//      production side to it.
//
// A SOURCE CANARY IS A WEAK INSTRUMENT AND ITS LIMIT IS NAMED HERE RATHER THAN
// DISCOVERED LATER. It can be out-spelled: a local `let e: WorkJournalEntry
// .EventType = .failed` passed to the initialiser satisfies every pattern
// below. What makes that acceptable is that the out-spelling has to route
// through `disposition` to be reachable at all — the emission function takes
// no event of its own, so a literal has nowhere to come from except a variable
// somebody wrote deliberately. The canary catches the accident; the signature
// catches the shape.
//
// XCTest so the canary is filterable from a test plan (`xctestplan`
// `selectedTests`/`skippedTests` silently ignore Swift Testing identifiers).

import Foundation
import XCTest

final class ZeroCoverageRecoveryRoutingSourceCanaryTests: XCTestCase {

    private func body(ofFunc name: String, inRepoPath path: String) throws -> String {
        let source = try SwiftSourceInspector.loadSource(repoRelativePath: path)
        guard let funcRange = source.range(of: "func \(name)(") else {
            XCTFail("could not locate `func \(name)(` in \(path)")
            return ""
        }
        guard let openBrace = source[funcRange.upperBound...].firstIndex(of: "{") else {
            XCTFail("could not locate `{` after `func \(name)(` in \(path)")
            return ""
        }
        return SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
        )
    }

    /// The runner's zero-coverage row must take its event from the same value
    /// that produced the `StopReason`, and must not name one itself.
    ///
    /// Before playhead-rqgr this site read `eventType: .failed` for every
    /// zero-coverage exit — including the interrupted ones, whose outcome
    /// (`.interrupted`) exists so the job spends no attempt and the retry comes
    /// back. `AnalysisCoordinator.recoverOrphans` routes a cold-launch orphan
    /// on that column, so a process death before the scheduler's requeue turned
    /// "comes back" into "lease cleared, never requeued".
    func testTimeoutJournalTakesItsEventFromTheDisposition() throws {
        let body = try body(
            ofFunc: "emitTranscriptionTimeoutJournal",
            inRepoPath: "Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift"
        )
        XCTAssertTrue(
            body.contains("eventType: disposition.journalEvent"),
            """
            emitTranscriptionTimeoutJournal no longer takes its work_journal event \
            from the disposition that produced the run's StopReason. The two records \
            of one exit can now disagree, which is playhead-rqgr's whole defect.
            """
        )
        XCTAssertFalse(
            body.contains("eventType: .failed"),
            """
            emitTranscriptionTimeoutJournal names a literal work_journal event again. \
            `.failed` here tells a cold launch the work is over — for an interrupted \
            run that is the opposite of what its own outcome asked for.
            """
        )
    }

    /// The in-memory slice counter is the THIRD record of the same event, and
    /// it was the same literal one layer down.
    ///
    /// `SliceCompletionInstrumentation.recordFailed` and `recordPaused` build a
    /// byte-identical `SliceMetadata`; the only difference is which of
    /// `SliceCounters`' two tallies they increment. An unconditional
    /// `recordFailed` therefore counted every listener's scrub into
    /// `slicesFailed` — the one quantity that number exists not to be. Pinned
    /// separately from the event above so a regression in one is not masked by
    /// the other.
    func testTimeoutJournalRoutesTheSliceCounterByTheSameDisposition() throws {
        let body = try body(
            ofFunc: "emitTranscriptionTimeoutJournal",
            inRepoPath: "Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift"
        )
        XCTAssertTrue(
            body.contains("disposition.journalEvent.orphanRecoveryRouting"),
            """
            the slice counter is no longer routed by the disposition, so a listener's \
            scrub is being tallied into slicesFailed again.
            """
        )
        XCTAssertTrue(
            body.contains("SliceCompletionInstrumentation.recordPaused"),
            """
            the paused tally is unreachable from the zero-coverage exit, so no scrub can \
            ever be counted as what it was.
            """
        )
    }

    /// Orphan recovery must route on the declared arm, not on a `case` list of
    /// its own.
    ///
    /// The list is what let a new event join an arm by being appended to
    /// whichever branch a reader edited first, and it is what the store-level
    /// tests were duplicating. `EventType.orphanRecoveryRouting` is exhaustive
    /// with no `default`, so the decision is forced at the declaration instead.
    func testOrphanRecoveryRoutesOnTheDeclaredArm() throws {
        let body = try body(
            ofFunc: "recoverOrphans",
            inRepoPath: "Playhead/Services/AnalysisCoordinator/AnalysisCoordinator.swift"
        )
        XCTAssertTrue(
            body.contains("orphanRecoveryRouting"),
            """
            recoverOrphans no longer reads WorkJournalEntry.EventType.orphanRecoveryRouting. \
            Whatever it reads instead is a second copy of the terminal-vs-resume policy, \
            and the store-level tests replay the FIRST one.
            """
        )
        XCTAssertFalse(
            body.contains("case .finalized"),
            """
            recoverOrphans spells the terminal event set out locally again. That list is \
            exactly what a new EventType can be appended to without anyone deciding what \
            a cold launch should do with it.
            """
        )
    }
}
