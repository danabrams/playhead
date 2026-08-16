// AnalysisOutcome.swift
// Result type returned by AnalysisJobRunner after a bounded analysis run.

import Foundation

struct AnalysisOutcome: Sendable {
    enum StopReason: Sendable {
        case reachedTarget
        case cancelledByPlayback
        case pausedForThermal
        case blockedByModel
        case memoryPressure
        case backgroundExpired
        /// A higher-lane admission (playhead-01t8) flipped the
        /// preemption signal and the runner paused at its next safe
        /// point. Coverage fields carry whatever the job managed to
        /// persist before the pause — by contract this is always on
        /// a checkpoint boundary so the next run is resumable.
        case preempted
        /// playhead-ngev (review r1): the run was displaced by something with a
        /// better claim on the shared transcript engine — a scrub, a speed
        /// change, a different episode — rather than failing.
        ///
        /// SEPARATE FROM `.failed` FOR ONE REASON: RETRY ACCOUNTING. The
        /// scheduler charges `.failed` one of five PERMANENT attempts, and at
        /// five the job is `superseded` with `nextEligibleAt: nil`. That row is
        /// terminal and cannot come back: `analysis_jobs.workKey` is UNIQUE,
        /// `insertJob` is `INSERT OR IGNORE`, and the key is
        /// `"<fingerprint>:<analysisVersion>:preAnalysis"` — stable across
        /// launches — so every later enqueue for the episode is silently
        /// dropped until an app update bumps `analysisVersion`. Charging that
        /// budget for moving the playhead would permanently kill analysis on
        /// exactly the episodes a listener engages with most.
        ///
        /// SEPARATE FROM `.preempted` TOO, deliberately: that case carries lane
        /// semantics (a higher-lane admission flipped a `PreemptionSignal`) and
        /// its journal row means something specific. This one is the same
        /// ACCOUNTING with its own diagnosis, so the row keeps saying
        /// `cancelled` or `stopped` — which is true, and is the whole point of
        /// this bead.
        ///
        /// The payload is the same `"transcription:<class>"` string `.failed`
        /// would have carried, so nothing about the reporting is lost.
        case interrupted(code: String)

        /// playhead-q93o: **THE LABEL IS THE CONTRACT, AND IT IS THERE TO MAKE
        /// A CROSS-FILE RULE POSSIBLE AT ALL.**
        ///
        /// This payload is not a message. It is carried, unmodified, into
        /// `analysis_jobs.lastErrorCode` and into `work_journal.metadata`'s
        /// `runner_reason` by ``AnalysisWorkScheduler``'s `.failed` / `.interrupted`
        /// arms, two hops away in another file — the runner builds it, an enum
        /// associated value carries it, a `case .failed(let reason)` unwraps it,
        /// and an interpolation puts it in the column. For four months five of
        /// its producers built it by interpolating a caught `Error`, which is
        /// `String(describing:)` by another name, and the source canary written
        /// to stop exactly that (`DurableThrowRecordSourceCanaryTests`) stayed
        /// GREEN throughout: it filters `lastErrorCode:` ARGUMENTS in the
        /// scheduler, and the offending argument there is the identifier
        /// `reason`.
        ///
        /// The label costs nothing and buys the one thing a source rule needs:
        /// **the compiler now forces every construction of this case to be
        /// spelled `.failed(code:`**, so a canary can enumerate the producers
        /// across the whole tree from one marker instead of guessing at the
        /// spellings a future author might use. `case .failed(let reason)`
        /// pattern matches are unaffected by a payload label, so no consumer
        /// changed.
        ///
        /// What it does NOT buy, stated rather than hoped: the payload is still
        /// a `String`, so a description can still reach it through a local or a
        /// helper. A label bounds the SPELLINGS a rule must know; only a type
        /// bounds the VALUES. See `DurableThrowRecord.swift`'s q93o section for
        /// the residual limits and `playhead-qlja` for the type.
        case failed(code: String)
    }

    let assetId: String
    let requestedCoverageSec: Double
    let featureCoverageSec: Double
    let transcriptCoverageSec: Double
    let cueCoverageSec: Double
    let newCueCount: Int
    let stopReason: StopReason
}
