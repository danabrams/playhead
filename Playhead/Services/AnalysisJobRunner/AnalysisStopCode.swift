// AnalysisStopCode.swift
// playhead-qlja: the payload of `AnalysisOutcome.StopReason.failed` /
// `.interrupted`, as a TYPE THAT CANNOT HOLD PROSE.
//
// # What a label bought, and what it did not
//
// playhead-q93o converted the runner's five producers to a named
// `DurableThrowRecord` token and added the payload LABEL `code:`, so the
// compiler forces one construction spelling and a source canary can enumerate
// the producers from a single marker. That closed the SPELLINGS a rule must
// know. It did not close the VALUES: the payload was still a `String`, so a
// description could still reach `analysis_jobs.lastErrorCode` and
// `work_journal.metadata`'s `runner_reason` through
//
//   * a laundering LOCAL more than one hop from the construction —
//     `let c = "\(error)"; …; .failed(code: c)`;
//   * a HELPER that returns a description, called at the construction;
//   * a producer in a file the canary's closed-world set has not been extended
//     to (it fails loudly, but the remedy is a human decision).
//
// # What the type closes
//
// `init` is `private` and this type is ALONE IN THIS FILE, which is the
// strongest closure Swift offers short of a module boundary: `private` at type
// scope reaches the enclosing declaration and its extensions IN THIS FILE, so
// no other file can spell a `String` into a stop code. Every route from an
// `Error` to a code runs through `DurableThrowRecord`, whose own tokens are
// closed-grammar (no whitespace, one balanced parenthetical), and the two
// coded literals the runner writes are named constants here.
//
// The canary (`DurableThrowRecordTests`) is now a BACKSTOP rather than the
// guard: it still pins that each stage binds its own record and hands that
// same local to the outcome, which the type cannot express.
struct AnalysisStopCode: Sendable, Equatable, Hashable {
    /// The token as it reaches the durable column. Read it; you cannot write it.
    let rawValue: String

    private init(_ rawValue: String) {
        self.rawValue = rawValue
    }

    /// A caught `Error` at a named runner stage. The ONLY route from an error
    /// to a stop code, and it is `DurableThrowRecord`'s grammar, not prose.
    static func runnerStage(
        _ error: Error,
        stage: DurableThrowRecord.RunnerStage
    ) -> AnalysisStopCode {
        AnalysisStopCode(
            DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: stage)
        )
    }

    /// The request admitted no shard inside the coverage it asked for. Not an
    /// error at all — a coded statement about the request.
    static let noShardsWithinDesiredCoverage = AnalysisStopCode(
        "no shards within desired coverage"
    )

    /// Stage 3 produced zero coverage. `nil` is the case where nobody reported
    /// a class, which is a different statement from any particular class.
    static func transcription(_ failureClass: TranscriptFailureClass?) -> AnalysisStopCode {
        AnalysisStopCode(
            failureClass.map { "transcription:\($0.rawValue)" } ?? "transcription:zeroCoverage"
        )
    }
}
