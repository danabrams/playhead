// BackgroundTaskRunLedger.swift
// playhead-hygc.1.4: Durable per-run outcome ledger for BGProcessingTask
// executions. Complements `BGTaskTelemetryLogger` (the JSONL lifecycle
// log) with a queryable progress accounting surface so dogfood overnight
// runs can be classified — admitted work / no eligible work / deferred /
// expired / cancelled / failed / made zero progress — without reading
// raw logs.
//
// Why this ledger exists (per the bead spec):
//   The May 6, 2026 dogfood data showed 470 BGTask `start` events but
//   only 15 rows in the durable `analysis_jobs` table — so it was
//   impossible to classify whether each wake admitted work, hit no
//   eligible work, throttled on QualityProfile, expired, lost a lease,
//   or fell through with zero coverage progress. Without a per-run
//   record carrying the OUTCOME (not just lifecycle moments), the
//   Activity surface and diagnostics export cannot answer the user
//   question: "is anything actually working overnight?"
//
// Design contract:
//   - Each `BGProcessingTask` invocation creates exactly one
//     `background_task_runs` row when the handler enters
//     (`startRun(...)`) and updates that row with a terminal outcome
//     when the handler exits (`finishRun(...)`).
//   - Outcomes form a closed enum (`BackgroundTaskRunOutcome`) so
//     diagnostic consumers can pattern-match without parsing strings.
//   - Idempotence: `finishRun` is no-op if the row is already terminal,
//     so racing expiration + normal-completion paths don't double-write.
//   - Expiration handlers MUST persist a final outcome before returning
//     (per acceptance criteria). The wiring in
//     `BackgroundProcessingService` calls `finishRun` from inside the
//     expiration closure before `markComplete` to keep this invariant
//     even if a subsequent crash interrupts the rest of teardown.
//   - This is NOT a replacement for `BGTaskTelemetryLogger`. The two
//     coexist: BGTaskTelemetryLogger is append-only JSONL lifecycle
//     telemetry suited to forensic grep; this ledger is queryable
//     SQLite per-run accounting that drives Activity and the diagnostics
//     export (playhead-hygc.1.9).
//
// Schema reference: `background_task_runs` table (AnalysisStore v24).
//   See `migrateBackgroundTaskRunsV24IfNeeded` for the column list and
//   `BackgroundTaskRunRecord` for the in-memory mirror.

import Foundation
import OSLog

// MARK: - BackgroundTaskRunOutcome

/// Closed enum of outcomes a background task run can resolve to.
/// Mapped to/from the `outcome TEXT` column on `background_task_runs`
/// via `rawValue`.
///
/// The taxonomy distinguishes the cases the bead spec calls out:
///   - `admittedWork`: backfill (or recovery) actually advanced work.
///   - `noEligibleWork`: handler ran but the queue was empty (or the
///     reconciler found nothing stale). Distinct from `admittedWork`
///     because it is NOT progress.
///   - `deferredThermal`: QualityProfile pause — `pauseAllWork`.
///   - `deferredCapability`: QualityProfile closed Soon lane (e.g.
///     LPM-stacked-on-fair). Distinct from `deferredThermal` so
///     diagnostics can attribute the cause.
///   - `expired`: iOS fired `expirationHandler`. Final outcome
///     persisted before the handler returns.
///   - `cancelled`: caller cancelled (e.g. teardown / stop()).
///   - `failed`: handler threw / hit an unrecoverable error.
///   - `noOp`: handler entered but had no work to do (e.g. recovery
///     ran with no stranded leases or stale rows). Distinct from
///     `noEligibleWork` to keep the recovery vs. backfill story
///     legible — recovery has a richer "what did I sweep" outcome.
///   - `recoveredWork`: recovery actually repaired leases /
///     resubmitted backfill / unblocked rows.
///   - `rescheduled`: handler self-armed without doing work (e.g.
///     a back-off path).
enum BackgroundTaskRunOutcome: String, Codable, Sendable, Equatable, CaseIterable {
    case admittedWork = "admitted_work"
    case noEligibleWork = "no_eligible_work"
    case deferredThermal = "deferred_thermal"
    case deferredCapability = "deferred_capability"
    case expired = "expired"
    case cancelled = "cancelled"
    case failed = "failed"
    case noOp = "no_op"
    case recoveredWork = "recovered_work"
    case rescheduled = "rescheduled"
    /// In-flight sentinel. Persisted at start so a dogfood snapshot
    /// taken mid-run is still classifiable. Replaced by a terminal
    /// outcome on `finishRun`.
    case running = "running"

    /// Whether this outcome represents a terminal state. Used by
    /// `finishRun` to enforce idempotence — once a terminal outcome
    /// has been written, subsequent finish calls are silently dropped.
    var isTerminal: Bool {
        self != .running
    }

    /// Human-readable summary used by the diagnostics export. Kept
    /// separate from `rawValue` so the on-disk wire string stays
    /// stable while UX copy can evolve.
    var summary: String {
        switch self {
        case .admittedWork: return "Admitted work"
        case .noEligibleWork: return "No eligible work"
        case .deferredThermal: return "Deferred (thermal)"
        case .deferredCapability: return "Deferred (capability)"
        case .expired: return "Expired"
        case .cancelled: return "Cancelled"
        case .failed: return "Failed"
        case .noOp: return "No-op"
        case .recoveredWork: return "Recovered work"
        case .rescheduled: return "Rescheduled"
        case .running: return "Running"
        }
    }
}

// MARK: - BackgroundTaskRunEntryPoint

/// Logical entry point for a background task run. Mapped to/from the
/// `entry_point TEXT` column on `background_task_runs`. Distinct from
/// `taskIdentifier` (which is the BGTaskScheduler identifier string)
/// because the same identifier can be reused across multiple logical
/// entry points if iOS evolves the BGTaskScheduler API.
enum BackgroundTaskRunEntryPoint: String, Codable, Sendable, Equatable, CaseIterable {
    case backfill = "backfill"
    case preAnalysisRecovery = "preanalysis_recovery"
    case continuedProcessing = "continued_processing"
    /// playhead-xsdz.36: the rediff B-side re-fetch BGProcessingTask
    /// (`RediffRefetchService`). Bandwidth totals ride `deferReason`
    /// (`precheckBytes=… fullFetchBytes=…`); the durable byte counters live
    /// in `rediff_bandwidth_ledger`.
    case rediffRefetch = "rediff_refetch"
}

// MARK: - BackgroundTaskRunRecord

/// In-memory mirror of a row in the `background_task_runs` table.
///
/// Field design (per the bead spec design hint):
///   - `runId`: stable identifier for one BGTask invocation. Generated
///     by the caller (UUID) so the in-memory ledger and the on-disk
///     row share identity.
///   - `entryPoint` / `taskIdentifier`: the logical kind of run and
///     the BGTaskScheduler identifier string respectively.
///   - `taskInstanceID`: matches the per-instance pointer-derived ID
///     used by `BGTaskTelemetryLogger.start/.complete/.expire` rows
///     so JSONL forensics and ledger rows can be cross-referenced.
///   - `startedAt` / `finishedAt`: wall-clock seconds since 1970.
///   - `outcome`: closed enum, `running` until a terminal write.
///   - `deferReason`: free-form annotation for `deferred*` outcomes
///     (e.g. "profile=critical", "profile=serious"). Independent of
///     the closed-enum outcome so a future profile axis change won't
///     require a new outcome variant.
///   - `cause`: matches `InternalMissCause.rawValue` when applicable.
///     Lets diagnostics share vocabulary with the work_journal table
///     for `expired` and `failed` rows.
///   - `jobsSeen` / `jobsAdmitted` / `jobsCompleted` / `jobsDeferred`:
///     bookkeeping counters. Optional because not every entry point
///     reports every counter — recovery reports `jobsCompleted` (rows
///     repaired) but does not have a meaningful `jobsAdmitted`.
///   - `coverageBefore` / `coverageAfter`: aggregate coverage
///     attribution. Optional — only populated when the runner
///     measures it.
///   - `assetId`: optional per-asset surfacing key for the "latest
///     outcome per asset" diagnostics query. Nil for global runs
///     (e.g. backfill that drained multiple assets).
///   - `expiration`: true when the run resolved via the expiration
///     handler. Distinct from `outcome == .expired` because a run
///     CAN be expired without firing the iOS expiration callback
///     (e.g. caller-side cancellation that races OS reclaim).
///   - `lastErrorCode`: free-form string for the `failed` arm. Pulled
///     from the work_journal `cause` column or the runner's caught
///     `Error.localizedDescription`.
///   - `scenePhase`: snapshot of `UIApplication.applicationState`
///     stamped at start time. Mirrors the same axis surfaced by
///     `BGTaskTelemetryEvent.scenePhase`.
struct BackgroundTaskRunRecord: Sendable, Equatable {
    let runId: String
    let entryPoint: BackgroundTaskRunEntryPoint
    let taskIdentifier: String
    let taskInstanceID: String?
    let startedAt: Double
    let finishedAt: Double?
    let outcome: BackgroundTaskRunOutcome
    let deferReason: String?
    let cause: String?
    let jobsSeen: Int?
    let jobsAdmitted: Int?
    let jobsCompleted: Int?
    let jobsDeferred: Int?
    let coverageBefore: Double?
    let coverageAfter: Double?
    let assetId: String?
    let expiration: Bool
    let lastErrorCode: String?
    let scenePhase: String?

    init(
        runId: String,
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String? = nil,
        startedAt: Double,
        finishedAt: Double? = nil,
        outcome: BackgroundTaskRunOutcome = .running,
        deferReason: String? = nil,
        cause: String? = nil,
        jobsSeen: Int? = nil,
        jobsAdmitted: Int? = nil,
        jobsCompleted: Int? = nil,
        jobsDeferred: Int? = nil,
        coverageBefore: Double? = nil,
        coverageAfter: Double? = nil,
        assetId: String? = nil,
        expiration: Bool = false,
        lastErrorCode: String? = nil,
        scenePhase: String? = nil
    ) {
        self.runId = runId
        self.entryPoint = entryPoint
        self.taskIdentifier = taskIdentifier
        self.taskInstanceID = taskInstanceID
        self.startedAt = startedAt
        self.finishedAt = finishedAt
        self.outcome = outcome
        self.deferReason = deferReason
        self.cause = cause
        self.jobsSeen = jobsSeen
        self.jobsAdmitted = jobsAdmitted
        self.jobsCompleted = jobsCompleted
        self.jobsDeferred = jobsDeferred
        self.coverageBefore = coverageBefore
        self.coverageAfter = coverageAfter
        self.assetId = assetId
        self.expiration = expiration
        self.lastErrorCode = lastErrorCode
        self.scenePhase = scenePhase
    }
}

// MARK: - BackgroundTaskRunOutcomeUpdate

/// Update payload for `BackgroundTaskRunLedger.finishRun`. Each field
/// is optional so call sites only specify what they have measured;
/// a missing counter does not overwrite a prior populated counter.
///
/// `outcome` is the only required field — every finish needs to
/// resolve the run to a terminal state.
struct BackgroundTaskRunOutcomeUpdate: Sendable {
    let outcome: BackgroundTaskRunOutcome
    let deferReason: String?
    let cause: String?
    let jobsSeen: Int?
    let jobsAdmitted: Int?
    let jobsCompleted: Int?
    let jobsDeferred: Int?
    let coverageBefore: Double?
    let coverageAfter: Double?
    let assetId: String?
    let expiration: Bool?
    let lastErrorCode: String?

    init(
        outcome: BackgroundTaskRunOutcome,
        deferReason: String? = nil,
        cause: String? = nil,
        jobsSeen: Int? = nil,
        jobsAdmitted: Int? = nil,
        jobsCompleted: Int? = nil,
        jobsDeferred: Int? = nil,
        coverageBefore: Double? = nil,
        coverageAfter: Double? = nil,
        assetId: String? = nil,
        expiration: Bool? = nil,
        lastErrorCode: String? = nil
    ) {
        self.outcome = outcome
        self.deferReason = deferReason
        self.cause = cause
        self.jobsSeen = jobsSeen
        self.jobsAdmitted = jobsAdmitted
        self.jobsCompleted = jobsCompleted
        self.jobsDeferred = jobsDeferred
        self.coverageBefore = coverageBefore
        self.coverageAfter = coverageAfter
        self.assetId = assetId
        self.expiration = expiration
        self.lastErrorCode = lastErrorCode
    }
}

// MARK: - BackgroundTaskRunLedger

/// Protocol seam for the durable run-outcome ledger. Production code
/// holds `any BackgroundTaskRunLedger` so tests can inject either:
///   - the real `AnalysisStoreBackgroundTaskRunLedger` against a temp
///     store (preferred — exercises real SQLite),
///   - or `NoOpBackgroundTaskRunLedger` for tests whose subject does
///     not depend on ledger semantics.
///
/// All methods are async because the production impl hops to the
/// `AnalysisStore` actor.
protocol BackgroundTaskRunLedger: Sendable {
    /// Insert a new `running` row for one BGTask invocation. Returns
    /// the assigned `runId` so the caller can pass it to
    /// `finishRun(runId:update:)` when the work resolves.
    ///
    /// `runId` is generated by the ledger (UUID) rather than by the
    /// caller because the call sites are scattered across multiple
    /// handlers and we want a single source of identity. A future
    /// consumer that wants caller-supplied identity can be added
    /// without breaking existing call sites.
    func startRun(
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async -> String

    /// Caller-supplied-`runId` variant of `startRun`. Used by BG-task
    /// handlers that need to mint the runId synchronously up front so
    /// they can install the iOS `expirationHandler` (or hit a critical
    /// suspend point such as `awaitPreAnalysisServicesInjected`) BEFORE
    /// the SQLite insert completes — under heavy concurrent load the
    /// AnalysisStore actor hop can be delayed long enough that
    /// installing the expirationHandler after `await store.insert(...)`
    /// would race the OS reclaim window. Tests still use the runId-less
    /// `startRun` overload above; production handlers fire this variant
    /// from a detached Task so the row insert is best-effort relative
    /// to the handler's critical path.
    ///
    /// Idempotent on `runId` collision in observable behavior: a second
    /// `recordRunStart` with the same `runId` will hit the SQLite PRIMARY
    /// KEY constraint and throw inside the AnalysisStore impl, but the
    /// throw is caught and logged at `.warning` rather than propagating
    /// — callers see no error. This protects against a duplicate
    /// `recordRunStart` in production from leaving the handler half-
    /// initialized; tests should not rely on it as an idempotence
    /// primitive (the call is best-effort, not transactional).
    func recordRunStart(
        runId: String,
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async

    /// Resolve a run to its terminal outcome. Idempotent: if the row
    /// is already at a terminal outcome, the call is a no-op and
    /// returns `false`. Returns `true` when the call advanced the
    /// row to a terminal state.
    @discardableResult
    func finishRun(
        runId: String,
        update: BackgroundTaskRunOutcomeUpdate
    ) async -> Bool

    /// Diagnostic surface: latest run by entry point. Returns the
    /// most recent row across both running and terminal outcomes
    /// for the given entry point. Used by the dogfood diagnostics
    /// export and the Activity row "what did the last backfill do?"
    /// query.
    func fetchLatestRun(
        for entryPoint: BackgroundTaskRunEntryPoint
    ) async -> BackgroundTaskRunRecord?

    /// Diagnostic surface: most recent N runs across all entry points,
    /// ordered by `startedAt` descending. Used by the diagnostics
    /// export to render an at-a-glance overnight summary.
    func fetchRecentRuns(limit: Int) async -> [BackgroundTaskRunRecord]

    /// Diagnostic surface: latest run scoped to a specific asset.
    /// Returns nil when no run has ever recorded the asset id.
    func fetchLatestRun(forAssetId assetId: String) async -> BackgroundTaskRunRecord?

    /// playhead-hygc.1.4 (R1 fix): one-shot launch-time reaper that
    /// flips orphan `.running` rows (left over from a prior process
    /// that crashed or was OS-killed) to `.failed` with
    /// `lastErrorCode = "orphan_at_launch"`. Returns the number of rows
    /// reaped. Sibling to the existing
    /// `resetStrandedBackfillJobs` / `resetStrandedFinalPassJobs`
    /// crash-recovery reapers — without this, dogfood diagnostics
    /// cannot distinguish "this row was alive when we shut down" from
    /// "this row is alive RIGHT NOW".
    ///
    /// Call-time ordering (R10 doc fix):
    ///   The reaper does NOT need to run before BG-task handlers
    ///   register. The production wiring in `PlayheadRuntime` calls
    ///   `registerBackgroundTasks()` synchronously during `init` and
    ///   then runs this reaper from a deferred `Task { ... }` body
    ///   after `migrate()` succeeds — so a fresh handler can have
    ///   already started a `.running` row by the time the reaper
    ///   runs. The temporal filter (`startedBefore`) is what makes
    ///   that race safe: any row inserted by a handler in THIS
    ///   process has `startedAt > processLaunchTimestamp`, so passing
    ///   the launch timestamp as the cutoff guarantees only prior-
    ///   process rows are eligible.
    ///
    /// `startedBefore` is the upper bound on `startedAt` for a row to
    /// be considered an orphan. Callers should capture this timestamp
    /// at process boundary (e.g. `PlayheadRuntime.init`) BEFORE
    /// registering any BG-task handlers so a handler that fires
    /// between init and the reaper running cannot have its fresh
    /// `running` row mistakenly reaped — the handler's
    /// `recordRunStart` will stamp `startedAt` strictly later than the
    /// captured cutoff. Defaults to "now" for callers that don't need
    /// the temporal-race protection (e.g. tests).
    @discardableResult
    func reapOrphansAtLaunch(startedBefore: Double) async -> Int
}

extension BackgroundTaskRunLedger {
    /// Convenience overload that captures the temporal cutoff at the
    /// call site. Production callers (PlayheadRuntime) should prefer
    /// the explicit-cutoff variant so the cutoff is captured BEFORE
    /// `registerBackgroundTasks()` runs; this overload is for tests
    /// and one-off diagnostic call sites.
    @discardableResult
    func reapOrphansAtLaunch() async -> Int {
        await reapOrphansAtLaunch(
            startedBefore: Date().timeIntervalSince1970
        )
    }
}

// MARK: - NoOpBackgroundTaskRunLedger

/// No-op ledger for tests and call sites that do not depend on
/// the durable accounting. Returns a stable but useless `runId` so
/// callers that want to thread it onward keep working.
struct NoOpBackgroundTaskRunLedger: BackgroundTaskRunLedger {
    func startRun(
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async -> String {
        UUID().uuidString
    }

    func recordRunStart(
        runId: String,
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async {}

    func finishRun(
        runId: String,
        update: BackgroundTaskRunOutcomeUpdate
    ) async -> Bool { false }

    func fetchLatestRun(
        for entryPoint: BackgroundTaskRunEntryPoint
    ) async -> BackgroundTaskRunRecord? { nil }

    func fetchRecentRuns(limit: Int) async -> [BackgroundTaskRunRecord] { [] }

    func fetchLatestRun(forAssetId assetId: String) async -> BackgroundTaskRunRecord? { nil }

    @discardableResult
    func reapOrphansAtLaunch(startedBefore: Double) async -> Int { 0 }
}

// MARK: - AnalysisStoreBackgroundTaskRunLedger

/// Production ledger backed by the `background_task_runs` table on
/// `AnalysisStore`. The store actor owns serial write isolation so the
/// ledger does not need its own actor — a struct holding the store is
/// sufficient.
///
/// Every method swallows `AnalysisStoreError` and logs at the
/// subsystem level. The bead spec is explicit that durability of the
/// ledger row is best-effort — if the SQLite write fails (disk full,
/// readonly fallback, etc.) we'd rather lose the diagnostic record
/// than fail the BG task itself, because the BG task fronting the
/// ledger is on a 30-second budget and cannot retry storage failures.
struct AnalysisStoreBackgroundTaskRunLedger: BackgroundTaskRunLedger {
    private let store: AnalysisStore
    private let logger = Logger(
        subsystem: "com.playhead",
        category: "BackgroundTaskRunLedger"
    )

    init(store: AnalysisStore) {
        self.store = store
    }

    func startRun(
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async -> String {
        let runId = UUID().uuidString
        await recordRunStart(
            runId: runId,
            entryPoint: entryPoint,
            taskIdentifier: taskIdentifier,
            taskInstanceID: taskInstanceID,
            scenePhase: scenePhase
        )
        return runId
    }

    func recordRunStart(
        runId: String,
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async {
        let record = BackgroundTaskRunRecord(
            runId: runId,
            entryPoint: entryPoint,
            taskIdentifier: taskIdentifier,
            taskInstanceID: taskInstanceID,
            startedAt: Date().timeIntervalSince1970,
            finishedAt: nil,
            outcome: .running,
            scenePhase: scenePhase
        )
        do {
            try await store.insertBackgroundTaskRun(record)
        } catch {
            logger.warning(
                "recordRunStart failed: \(error.localizedDescription, privacy: .public)"
            )
        }
    }

    @discardableResult
    func finishRun(
        runId: String,
        update: BackgroundTaskRunOutcomeUpdate
    ) async -> Bool {
        do {
            return try await store.updateBackgroundTaskRunOutcome(
                runId: runId,
                update: update,
                finishedAt: Date().timeIntervalSince1970
            )
        } catch {
            logger.warning(
                "finishRun failed for runId \(runId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return false
        }
    }

    func fetchLatestRun(
        for entryPoint: BackgroundTaskRunEntryPoint
    ) async -> BackgroundTaskRunRecord? {
        do {
            return try await store.fetchLatestBackgroundTaskRun(entryPoint: entryPoint)
        } catch {
            logger.warning(
                "fetchLatestRun(entryPoint:) failed: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    func fetchRecentRuns(limit: Int) async -> [BackgroundTaskRunRecord] {
        do {
            return try await store.fetchRecentBackgroundTaskRuns(limit: limit)
        } catch {
            logger.warning(
                "fetchRecentRuns failed: \(error.localizedDescription, privacy: .public)"
            )
            return []
        }
    }

    func fetchLatestRun(forAssetId assetId: String) async -> BackgroundTaskRunRecord? {
        do {
            return try await store.fetchLatestBackgroundTaskRun(assetId: assetId)
        } catch {
            logger.warning(
                "fetchLatestRun(assetId:) failed: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    @discardableResult
    func reapOrphansAtLaunch(startedBefore: Double) async -> Int {
        do {
            let count = try await store.reapOrphanBackgroundTaskRuns(
                olderThan: startedBefore
            )
            if count > 0 {
                logger.info(
                    "Reaped \(count, privacy: .public) orphan running ledger row(s) at launch"
                )
            }
            return count
        } catch {
            logger.warning(
                "reapOrphansAtLaunch failed: \(error.localizedDescription, privacy: .public)"
            )
            return 0
        }
    }
}
