// PersistedStateInvariantReporter.swift
// playhead-dgly — read the persisted state that survived the last process,
// judge it, and RECORD what is no longer true. Repairs nothing.
//
// ----- Where it runs, and why exactly there -----
//
// `PlayheadRuntime`'s deferred bootstrap Task, immediately after
// `analysisStoreRecovery.openAtLaunch` returns open and BEFORE every repair
// that launch performs:
//
//   openAtLaunch                      <- the schema ladder runs here
//   >>> report() <<<
//   backgroundTaskRunLedger.reapOrphansAtLaunch
//   reconcileDuplicateAnalysisAssetsIfNeeded
//   runEpisodeDurationBackfillIfNeeded
//   reconcilePersistedTerminalStatesIfNeeded    <- rewrites analysisState
//   pruneOrphanedScansForCurrentCohort          <- DELETES semantic_scan_results
//   recoverOrphans / analysisJobReconciler.reconcile  <- clears stranded rows
//
// Every one of those last five would change a population this reporter counts.
// `resetStrandedBackfillJobs` is the sharpest case: it is the only thing that
// flips a hard-killed `status='running'` row back to `queued`, so a reporter
// placed after it would read zero for `playhead-1e86` on every launch and the
// zero would be an artifact of ordering, not a fact about the device.
//
// It is AWAITED in that chain rather than detached. A detached Task would race
// the five repairs and make the reading non-deterministic, which is the same
// defect class the reporter exists to find. The cost of awaiting it is
// therefore load-bearing and is bounded in `fetchPersistedStateSnapshot`'s doc.
//
// The one repair it cannot precede is the SCHEMA LADDER, which runs inside
// `openAtLaunch`. That is correct: `playhead-wogi`'s V51 lowers an
// over-claimed cursor as a shipped, one-shot, version-guarded migration, so
// after it the cursor invariant is a REGRESSION TRIPWIRE rather than a census
// of the backlog. Invariants 3, 4 and 5 are in the same position for the same
// reason and it is stated on each of them.
//
// ----- Where the record goes -----
//
// `SurfaceStatusInvariantLogger` — the per-session JSON Lines file under
// `Caches/Diagnostics/` that every device pull already collects and that
// `playhead-v7q6` settled as THE audit trail. Three deliberate consequences:
//
//   * No schema change. A new table or column would need Dan's call
//     (persistence strategy), and the channel that already exists is literally
//     named for invariant violations and already carries two always-present
//     censuses (`ad_window_ingest_census`, `rediff_day_zero_kickoff_claim_
//     attempted`).
//   * The record is pull-readable as text, with numerator AND denominator on
//     every line — `playhead-8ljj`'s lesson, where 100 of 100 expired
//     background runs carried no reason at all because the phase's whole
//     result was an `Int` logged to OSLog and dropped.
//   * KNOWN LIMIT: `Caches/` is evictable by iOS, so a long-idle device can
//     lose old session files. The logger keeps the most recent 200 sessions
//     and the reporter writes at every launch, so the CURRENT reading survives
//     any eviction that leaves the app installed; what an eviction can cost is
//     history, not the present verdict. If the history turns out to matter,
//     that is a durable-table decision and it is Dan's, not this bead's.

import Foundation
import os

/// Reads ``PersistedStateSnapshot``, evaluates every
/// ``PersistedStateInvariant``, and records the result. **Never writes to the
/// store.**
struct PersistedStateInvariantReporter: Sendable {

    /// Everything the reporter needs from the store, as a closure so a test
    /// can supply a snapshot without SQLite and a mutation of the evaluator is
    /// provable in milliseconds.
    typealias SnapshotProvider = @Sendable () async throws -> PersistedStateSnapshot

    /// "Is this episode's audio on disk?", asked of the download manager's own
    /// cache oracle — the same one the library asks. Asked ONLY of assets in a
    /// registration state, which is normally an empty population.
    typealias AudioPresenceProbe = @Sendable (String) async -> Bool

    /// playhead-gyhw: take the repairs the schema ladder performed during THIS
    /// launch. DRAINING, not reading — see
    /// ``AnalysisStore/drainPersistedStateRepairRecords()``.
    typealias RepairRecordProvider = @Sendable () async -> [PersistedStateRepairRecord]

    private let snapshotProvider: SnapshotProvider
    private let audioPresenceProbe: AudioPresenceProbe
    private let repairRecordProvider: RepairRecordProvider
    private let logger: SurfaceStatusInvariantLogger?
    private let osLogger = Logger(subsystem: "com.playhead", category: "PersistedStateInvariants")

    init(
        snapshotProvider: @escaping SnapshotProvider,
        audioPresenceProbe: @escaping AudioPresenceProbe,
        repairRecordProvider: @escaping RepairRecordProvider = { [] },
        logger: SurfaceStatusInvariantLogger?
    ) {
        self.snapshotProvider = snapshotProvider
        self.audioPresenceProbe = audioPresenceProbe
        self.repairRecordProvider = repairRecordProvider
        self.logger = logger
    }

    /// Read, judge, record. Returns the findings so a caller (or a test) can
    /// assert on them; the return value is deliberately NOT the durable
    /// record — the JSON Lines entries are.
    ///
    /// Throws nothing: a read failure is itself recorded, through
    /// ``InvariantViolation/Code/persistedStateInvariantReadFailed``, and the
    /// launch chain continues. A reporter that could break a launch would be
    /// a worse defect than the ones it looks for.
    @discardableResult
    func report() async -> [PersistedStateInvariantFinding] {
        // FIRST, and before anything that can fail. The repairs happened inside
        // `openAtLaunch`, so they are already in the past by the time this runs;
        // a snapshot read that throws must not also swallow the record of what
        // the ladder just did to the rows it was about to read.
        await reportLadderRepairs()

        let snapshot: PersistedStateSnapshot
        do {
            snapshot = try await snapshotProvider()
        } catch {
            logger?.invariantViolated(
                code: .persistedStateInvariantReadFailed,
                description: "error=\(PersistedStateInvariantEvaluator.sanitize(String(describing: error)))"
            )
            osLogger.error(
                "persisted-state invariant read failed: \(error.localizedDescription, privacy: .public)"
            )
            return []
        }

        let resolved = await resolvingAudioPresence(snapshot)
        let findings = PersistedStateInvariantEvaluator.evaluate(resolved)

        for finding in findings {
            logger?.invariantViolated(
                code: .persistedStateInvariantCensus,
                description: Self.censusDescription(for: finding)
            )
            for witness in finding.witnesses {
                logger?.invariantViolated(
                    code: .persistedStateInvariantViolation,
                    description: "invariant=\(finding.invariant.rawValue) \(witness)"
                )
            }
            if !finding.isClean {
                osLogger.notice(
                    """
                    persisted-state invariant \(finding.invariant.rawValue, privacy: .public) \
                    violated by \(finding.violations, privacy: .public) of \
                    \(finding.population, privacy: .public) row(s)
                    """
                )
            }
        }
        return findings
    }

    /// playhead-gyhw: drain the schema ladder's repair ledger and record it.
    ///
    /// One census line ALWAYS, one line per repaired row. The census is what
    /// makes `repairs=0` a claim instead of a silence — the same argument that
    /// puts a census line under every invariant, applied to the other half of
    /// the pair. Without it a launch that repaired nothing and a launch whose
    /// ledger was never drained (a provider left at its default, a caller that
    /// forgot to pass one) are byte-identical in the pull.
    private func reportLadderRepairs() async {
        let repairs = await repairRecordProvider()
        logger?.invariantViolated(
            code: .persistedStateRepairCensus,
            description: Self.repairCensusDescription(for: repairs)
        )
        for repair in repairs {
            logger?.invariantViolated(
                code: .persistedStateRepairApplied,
                description: repair.wireDescription
            )
            osLogger.notice(
                """
                persisted-state repair \(repair.migration, privacy: .public) on row \
                \(repair.rowId, privacy: .public): \(repair.field, privacy: .public) \
                \(repair.from, privacy: .public) -> \(repair.to, privacy: .public)
                """
            )
        }
    }

    /// The repair census body: the total, then the per-rung breakdown so a
    /// reader can attribute the rows without parsing every violation line.
    ///
    /// `migrations=none` rather than an empty value, because a key whose value
    /// is absent reads as a truncated line rather than as a claim of zero.
    static func repairCensusDescription(for repairs: [PersistedStateRepairRecord]) -> String {
        var counts: [String: Int] = [:]
        for repair in repairs { counts[repair.migration, default: 0] += 1 }
        let breakdown = counts.keys.sorted()
            .map { "\($0):\(counts[$0] ?? 0)" }
            .joined(separator: ",")
        return "repairs=\(repairs.count) migrations=\(breakdown.isEmpty ? "none" : breakdown)"
    }

    /// Ask the audio oracle for the registration-state assets only. Every other
    /// asset keeps `hasAudioOnDisk == nil`, and the invariant abstains on it
    /// rather than assuming an answer nobody gave.
    private func resolvingAudioPresence(
        _ snapshot: PersistedStateSnapshot
    ) async -> PersistedStateSnapshot {
        var assets: [PersistedStateSnapshot.AssetRow] = []
        assets.reserveCapacity(snapshot.assets.count)
        for asset in snapshot.assets {
            guard PersistedStateInvariantEvaluator.registrationStates
                .contains(asset.analysisState) else {
                assets.append(asset)
                continue
            }
            let present = await audioPresenceProbe(asset.episodeId)
            assets.append(asset.resolvingAudioPresence(present))
        }
        return PersistedStateSnapshot(
            backfillJobs: snapshot.backfillJobs,
            assets: assets,
            eligibilityGatedAdWindows: snapshot.eligibilityGatedAdWindows,
            coverageLaneRetryCap: snapshot.coverageLaneRetryCap
        )
    }

    /// The census wire format: space-separated `key=value`, matching
    /// `CoarseScanPhaseReport.ledgerReason` and the `ad_window_ingest_census`
    /// body so one parser reads all three.
    ///
    /// `violations` and `population` are the numerator and denominator over the
    /// WHOLE population. `witnesses` says how much of the numerator the
    /// accompanying violation lines enumerate — a truncated list that did not
    /// say so would be a check reporting on a subset while claiming the whole.
    static func censusDescription(for finding: PersistedStateInvariantFinding) -> String {
        var out = "invariant=\(finding.invariant.rawValue)"
        out += " violations=\(finding.violations)"
        out += " population=\(finding.population)"
        out += " witnesses=\(finding.witnesses.count)/\(finding.violations)"
        if finding.abstained > 0 {
            out += " abstained=\(finding.abstained)"
            out += " abstain_reason=\(finding.abstainReason ?? "unspecified")"
        }
        return out
    }
}
