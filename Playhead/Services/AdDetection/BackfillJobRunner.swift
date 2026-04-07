// BackfillJobRunner.swift
// Phase 3 shadow-mode orchestrator. Walks: CoveragePlanner -> AdmissionController
// -> FoundationModelClassifier (coarse + refinement) -> SemanticScanResult /
// EvidenceEvent persistence. The runner never writes AdWindow rows: shadow mode
// is observation-only. Phase 6 will introduce a separate decision-fusion layer
// that promotes FM evidence to user-visible cues; until then, .enabled silently
// degrades to .shadow with a logged warning.

import CryptoKit
import Foundation
import OSLog

actor BackfillJobRunner {

    // MARK: - Inputs / Outputs

    struct AssetInputs: Sendable {
        let analysisAssetId: String
        let podcastId: String
        let segments: [AdTranscriptSegment]
        let evidenceCatalog: EvidenceCatalog
        let transcriptVersion: String
        let plannerContext: CoveragePlannerContext

        init(
            analysisAssetId: String,
            podcastId: String,
            segments: [AdTranscriptSegment],
            evidenceCatalog: EvidenceCatalog,
            transcriptVersion: String,
            plannerContext: CoveragePlannerContext
        ) {
            self.analysisAssetId = analysisAssetId
            self.podcastId = podcastId
            self.segments = segments
            self.evidenceCatalog = evidenceCatalog
            self.transcriptVersion = transcriptVersion
            self.plannerContext = plannerContext
        }
    }

    struct RunResult: Sendable, Equatable {
        let admittedJobIds: [String]
        let scanResultIds: [String]
        let evidenceEventIds: [String]
        let deferredJobIds: [String]

        static let empty = RunResult(
            admittedJobIds: [],
            scanResultIds: [],
            evidenceEventIds: [],
            deferredJobIds: []
        )
    }

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let admissionController: AdmissionController
    private let classifier: FoundationModelClassifier
    private let coveragePlanner: CoveragePlanner
    private let mode: FMBackfillMode
    private let capabilitySnapshotProvider: @Sendable () async -> CapabilitySnapshot
    private let batteryLevelProvider: @Sendable () async -> Float
    private let scanCohortJSON: String
    private let clock: @Sendable () -> Date
    private let logger = Logger(subsystem: "com.playhead", category: "BackfillJobRunner")

    init(
        store: AnalysisStore,
        admissionController: AdmissionController,
        classifier: FoundationModelClassifier,
        coveragePlanner: CoveragePlanner = CoveragePlanner(),
        mode: FMBackfillMode = .shadow,
        capabilitySnapshotProvider: @escaping @Sendable () async -> CapabilitySnapshot,
        batteryLevelProvider: @escaping @Sendable () async -> Float,
        scanCohortJSON: String,
        clock: @escaping @Sendable () -> Date = { Date() }
    ) {
        self.store = store
        self.admissionController = admissionController
        self.classifier = classifier
        self.coveragePlanner = coveragePlanner
        self.mode = mode
        self.capabilitySnapshotProvider = capabilitySnapshotProvider
        self.batteryLevelProvider = batteryLevelProvider
        self.scanCohortJSON = scanCohortJSON
        self.clock = clock
    }

    // MARK: - Entry Point

    /// Walks the FM backfill pipeline for one asset. Idempotent: callers can
    /// re-invoke for the same asset without producing duplicate work as long as
    /// the scan cohort and transcript version are unchanged. Returns a
    /// `RunResult` describing what was admitted, persisted, or deferred.
    func runPendingBackfill(for inputs: AssetInputs) async throws -> RunResult {
        guard mode != .disabled else {
            logger.debug("FM backfill skipped: mode=disabled")
            return .empty
        }

        if mode == .enabled {
            logger.warning(
                "fmBackfillMode=.enabled requested but Phase 6 fusion is not yet implemented; falling back to .shadow"
            )
        }

        let plan = coveragePlanner.plan(for: inputs.plannerContext)
        let now = clock().timeIntervalSince1970

        var enqueuedJobs: [BackfillJob] = []
        for (offset, phase) in plan.phases.enumerated() {
            // R4-Fix6: include `inputs.transcriptVersion` in the jobId so a
            // transcript regeneration produces a fresh row instead of
            // colliding with the prior `.complete` job under the same id
            // (which the M-5 idempotency check would skip, defeating the
            // whole point of reprocessing the new transcript).
            //
            // HIGH-R5-1 note: this transcriptVersion embedding is defensive
            // for a Phase 4 re-trigger path that does not yet exist in
            // production. `AnalysisCoordinator.finalizeBackfill` calls
            // `runBackfill` exactly once per session and then transitions
            // to `.complete`, so at HEAD no caller re-invokes this runner
            // under a new transcriptVersion. Keep the embedding — it costs
            // nothing and unblocks the Phase 4 re-analysis trigger when
            // transcripts regenerate without another round of id surgery.
            // R7-Fix11: jobId is a stable hash over the canonical tuple.
            // String concatenation with `-` separators is ambiguous when
            // `analysisAssetId` or `transcriptVersion` contains a hyphen
            // (UUIDs do), which would let two distinct tuples collide on
            // the same id. SHA-256 truncated to 16 hex chars gives ~64
            // bits of collision resistance — comfortably more than the
            // logical-tuple cardinality of one device's backfill history.
            let jobId = Self.makeJobIdForTesting(
                analysisAssetId: inputs.analysisAssetId,
                transcriptVersion: inputs.transcriptVersion,
                phase: phase,
                offset: offset
            )

            // M5: idempotent re-invocation. Job ids are deterministic, so a
            // second call would otherwise throw `duplicateJobId`. Check first:
            //   - complete: skip entirely (already done)
            //   - queued / running / deferred / failed: re-drive the existing
            //     row by enqueueing it against the admission controller; do
            //     NOT insert a duplicate.
            //   - missing: insert a fresh row.
            if let existing = try await store.fetchBackfillJob(byId: jobId) {
                if existing.status == .complete {
                    continue
                }
                // C-B: enforce retry budget across runBackfill invocations.
                // The factory creates a fresh AdmissionController per call, so
                // the controller's in-memory retry budget is reset every
                // time. The persisted retryCount is the source of truth — if
                // it already meets or exceeds maxRetries the job must not be
                // re-enqueued, otherwise a persistently failing job would
                // loop forever across runs.
                if existing.retryCount >= AdmissionController.maxRetries {
                    logger.warning("FM backfill job exhausted retries: \(existing.jobId, privacy: .public) retries=\(existing.retryCount)")
                    continue
                }
                await admissionController.enqueue(existing)
                enqueuedJobs.append(existing)
                continue
            }

            let job = BackfillJob(
                jobId: jobId,
                analysisAssetId: inputs.analysisAssetId,
                podcastId: inputs.podcastId,
                phase: phase,
                coveragePolicy: plan.policy,
                priority: phasePriority(phase),
                progressCursor: nil,
                retryCount: 0,
                deferReason: nil,
                status: .queued,
                scanCohortJSON: scanCohortJSON,
                createdAt: now + Double(offset) * 0.0001
            )
            try await store.insertBackfillJob(job)
            await admissionController.enqueue(job)
            enqueuedJobs.append(job)
        }

        var admitted: [String] = []
        var deferred: [String] = []
        var scanResultIds: [String] = []
        var evidenceEventIds: [String] = []

        // Drain the queue. AdmissionController is serial; one job at a time.
        for _ in enqueuedJobs {
            try Task.checkCancellation()
            let snapshot = await capabilitySnapshotProvider()
            let battery = await batteryLevelProvider()
            let decision = await admissionController.admitNextEligibleJob(
                snapshot: snapshot,
                batteryLevel: battery
            )

            if let reason = decision.deferReason {
                // H-1 orchestration: mark ALL remaining non-admitted,
                // non-deferred jobs as deferred with the same reason. The
                // prior implementation only marked the first queued job and
                // broke out of the drain loop, leaving any other enqueued
                // jobs stuck in `.queued` forever. A single admission
                // deferral (thermal, battery, low-power) invalidates every
                // job in this batch — they will all fail the same gate.
                for candidate in enqueuedJobs where
                    !admitted.contains(candidate.jobId)
                    && !deferred.contains(candidate.jobId)
                {
                    // C-2 (partial): prefer the split defer API so the
                    // deferReason is never clobbered by a concurrent progress
                    // checkpoint. The terminal `.complete` and `.failed`
                    // transitions still route through the deprecated shim
                    // because the split API has no replacement for them —
                    // tracked in C-2 gap report.
                    // R4-Fix1: tolerate terminal rows. The M-5 idempotency
                    // path re-enqueues `.failed` rows whose retryCount is
                    // still under the budget; when admission then defers,
                    // the C-R3-1 status guard rejects the
                    // `.failed -> .deferred` write with
                    // `invalidStateTransition`. Without a per-iteration
                    // catch, the throw aborts the loop and strands the
                    // remaining `.queued` siblings. Log and continue on
                    // `invalidStateTransition`; propagate any other store
                    // error so we don't silently swallow real bugs.
                    do {
                        try await store.markBackfillJobDeferred(
                            jobId: candidate.jobId,
                            reason: reason.rawValue
                        )
                        deferred.append(candidate.jobId)
                    } catch let error as AnalysisStoreError {
                        if case .invalidStateTransition = error {
                            logger.warning(
                                "Skipping defer for terminal job: \(candidate.jobId, privacy: .public) reason=\(reason.rawValue, privacy: .public)"
                            )
                            continue
                        }
                        throw error
                    }
                }
                logger.info("FM backfill deferred: \(reason.rawValue, privacy: .public) count=\(deferred.count)")
                break
            }

            guard let job = decision.job else { break }
            admitted.append(job.jobId)

            do {
                // C-2: split lifecycle API. `markBackfillJobRunning` preserves
                // progressCursor/retryCount/deferReason so an earlier-deferred
                // row's audit trail is not lost when it resumes.
                try await store.markBackfillJobRunning(jobId: job.jobId)

                let (resultIds, eventIds) = try await runJob(job, inputs: inputs)
                scanResultIds.append(contentsOf: resultIds)
                evidenceEventIds.append(contentsOf: eventIds)

                try await store.markBackfillJobComplete(
                    jobId: job.jobId,
                    progressCursor: BackfillProgressCursor(
                        processedUnitCount: 1,
                        lastProcessedUpperBoundSec: inputs.segments.last?.endTime
                    )
                )
            } catch is CancellationError {
                // H-2: don't let a store error swallow the CancellationError.
                // The caller's Task contract requires the CancellationError
                // to propagate; swallowing it in favor of a SQLite exception
                // would mask cooperative cancellation. We still log the
                // store failure so operators can diagnose stuck `.running`
                // rows.
                await admissionController.finish(jobId: job.jobId)
                do {
                    // H-R3-1: include the phase so operators can tell which
                    // pass was interrupted without cross-referencing logs.
                    try await store.markBackfillJobDeferred(
                        jobId: job.jobId,
                        reason: "cancelled-during-\(job.phase.rawValue)"
                    )
                } catch {
                    logger.error("Failed to mark cancelled job deferred: \(error.localizedDescription, privacy: .public)")
                }
                throw CancellationError()
            } catch let storeError as AnalysisStoreError {
                // C3-1: the C3-2 guards on `markBackfillJobComplete` /
                // `markBackfillJobFailed` / `markBackfillJobRunning` throw
                // `invalidStateTransition` when the runner tries to move a
                // row that is already in a terminal state (e.g. a prior run
                // left it `.failed` and the M-5 idempotency path re-enqueued
                // it). Do not cascade into `markBackfillJobFailed` — that
                // write is wasted at best and, without C3-2's idempotency
                // shim, would double-bump `retryCount`. Log and continue.
                if case .invalidStateTransition = storeError {
                    logger.warning("FM job already in terminal state, skipping: \(job.jobId, privacy: .public)")
                    await admissionController.finish(jobId: job.jobId)
                    continue
                }
                // H-R3-2: classify permanent vs recoverable store errors.
                // Permanent errors (schema validators, malformed cohort,
                // NULL column surprises, evidence-body collisions) will
                // never succeed on retry — replaying the same inputs
                // against the same schema hits the same validator. Burning
                // through `maxRetries` attempts on them is wasted work and
                // delays the failure signal reaching operators. Short-
                // circuit `retryCount` to `maxRetries` so the C-B gate on
                // the next run skips the row immediately.
                let persistedRetryCount = Self.isPermanent(storeError)
                    ? AdmissionController.maxRetries
                    : job.retryCount + 1
                // R4-Fix7: wrap the markBackfillJobFailed write so a
                // racing terminal transition (e.g. another runner just
                // marked the row `.complete`) cannot escape the catch
                // arm and abort the for-loop. Log and continue — the
                // admission ticket is still released below.
                do {
                    try await store.markBackfillJobFailed(
                        jobId: job.jobId,
                        reason: String(describing: storeError),
                        retryCount: persistedRetryCount
                    )
                } catch {
                    logger.warning(
                        "Failed to mark FM job failed (likely racing terminal transition): \(job.jobId, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
                    )
                }
                // bd-1tl: Use the enum's `description` (via String(describing:))
                // and a stable case-name token so on-device Console.app shows
                // the actual case (e.g. `evidenceEventBodyMismatch`) instead
                // of the useless bridged "Playhead.AnalysisStoreError error 9"
                // string from `localizedDescription`. The case name is the
                // single most-actionable diagnostic for triaging persistence
                // failures from the field — without it, every device failure
                // requires reverse-engineering the enum ordinal back to a case.
                let caseName = Self.caseName(of: storeError)
                logger.error(
                    "FM backfill job \(job.jobId, privacy: .public) failed: case=\(caseName, privacy: .public) detail=\(String(describing: storeError), privacy: .public) permanent=\(Self.isPermanent(storeError), privacy: .public)"
                )
            } catch {
                // C-2: markBackfillJobFailed writes deferReason so operators
                // can diagnose the failure without scraping logs. The prior
                // deprecated shim silently dropped the reason on .failed.
                //
                // R4-Fix7: same wrap as the typed-error arm above. A
                // racing terminal transition must not abort the drain
                // loop and strand the rest of the batch.
                do {
                    try await store.markBackfillJobFailed(
                        jobId: job.jobId,
                        reason: String(describing: error),
                        retryCount: job.retryCount + 1
                    )
                } catch {
                    logger.warning(
                        "Failed to mark FM job failed (likely racing terminal transition): \(job.jobId, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
                    )
                }
                // bd-1tl: see the typed-error arm above for why we prefer
                // String(describing:) over localizedDescription for on-device
                // diagnosis. Untyped errors here are typically Swift errors
                // bridged from FoundationModels or CancellationError.
                logger.error(
                    "FM backfill job \(job.jobId, privacy: .public) failed: case=untyped detail=\(String(describing: error), privacy: .public)"
                )
            }

            await admissionController.finish(jobId: job.jobId)
        }

        return RunResult(
            admittedJobIds: admitted,
            scanResultIds: scanResultIds,
            evidenceEventIds: evidenceEventIds,
            deferredJobIds: deferred
        )
    }

    // MARK: - Per-Job Execution

    /// Runs the FM coarse pass and (when warranted) the refinement pass for a
    /// single backfill job, persisting results to `semantic_scan_results` and
    /// `evidence_events`. Returns the IDs that were written.
    private func runJob(
        _ job: BackfillJob,
        inputs: AssetInputs
    ) async throws -> (scanResultIds: [String], evidenceEventIds: [String]) {
        var scanResultIds: [String] = []
        var evidenceEventIds: [String] = []

        let coarse = try await classifier.coarsePassA(segments: inputs.segments)

        for window in coarse.windows {
            try Task.checkCancellation()
            let result = makeScanResult(
                windowOutput: window,
                inputs: inputs,
                scanPass: "passA",
                status: .success
            )
            try await store.insertSemanticScanResult(result)
            scanResultIds.append(result.id)
        }

        if !coarse.failedWindowStatuses.isEmpty || !coarse.windows.isEmpty {
            let coarsePlans = try await classifier.planPassA(segments: inputs.segments)
            let succeededPlanIndices = Set(
                coarse.windows.compactMap { window in
                    coarsePlans.first(where: { plan in
                        Set(window.lineRefs).isSubset(of: Set(plan.lineRefs))
                    })?.windowIndex
                }
            )
            let remainingPlans = coarsePlans.filter { !succeededPlanIndices.contains($0.windowIndex) }
            for (plan, status) in zip(remainingPlans, coarse.failedWindowStatuses) {
                if let failureResult = makeCoarseFailureScanResult(
                    plan: plan,
                    inputs: inputs,
                    status: status,
                    latencyMs: coarse.latencyMillis
                ) {
                    try await store.insertSemanticScanResult(failureResult)
                    scanResultIds.append(failureResult.id)
                }
            }

            if coarse.status != .success,
               let blockingPlan = remainingPlans.dropFirst(coarse.failedWindowStatuses.count).first,
               let failureResult = makeCoarseFailureScanResult(
                    plan: blockingPlan,
                    inputs: inputs,
                    status: coarse.status,
                    latencyMs: coarse.latencyMillis
               ) {
                try await store.insertSemanticScanResult(failureResult)
                scanResultIds.append(failureResult.id)
            }
        } else if coarse.status != .success,
                  let failureResult = makeFailureScanResult(
                    scanPass: "passA",
                    attemptedSegments: inputs.segments,
                    inputs: inputs,
                    status: coarse.status,
                    latencyMs: coarse.latencyMillis
                  ) {
            try await store.insertSemanticScanResult(failureResult)
            scanResultIds.append(failureResult.id)
        }

        if coarse.status == .success && !coarse.windows.isEmpty {
            let zoomPlans = try await classifier.planAdaptiveZoom(
                coarse: coarse,
                segments: inputs.segments,
                evidenceCatalog: inputs.evidenceCatalog
            )
            if !zoomPlans.isEmpty {
                let refinement = try await classifier.refinePassB(
                    zoomPlans: zoomPlans,
                    segments: inputs.segments,
                    evidenceCatalog: inputs.evidenceCatalog
                )
                for window in refinement.windows {
                    try Task.checkCancellation()
                    let result = makeRefinementScanResult(
                        windowOutput: window,
                        inputs: inputs,
                        status: .success
                    )
                    let events = makeEvidenceEvents(
                        windowOutput: window,
                        inputs: inputs,
                        jobId: job.jobId
                    )
                    // C-3: Pass-B scan row and its evidence events must be
                    // written atomically. The store's batch API wraps both in
                    // `BEGIN IMMEDIATE … COMMIT` with rollback on failure, so
                    // a crash mid-write cannot leave orphan scan rows.
                    let persistedEventIds = try await store.recordSemanticScanResult(
                        result,
                        evidenceEvents: events
                    )
                    scanResultIds.append(result.id)
                    evidenceEventIds.append(contentsOf: persistedEventIds)
                }

                if !refinement.failedWindowStatuses.isEmpty || !refinement.windows.isEmpty {
                    let succeededWindowIndices = Set(refinement.windows.map(\.windowIndex))
                    let remainingPlans = zoomPlans.filter { !succeededWindowIndices.contains($0.windowIndex) }
                    for (plan, status) in zip(remainingPlans, refinement.failedWindowStatuses) {
                        if let failureResult = makeRefinementFailureScanResult(
                            plan: plan,
                            inputs: inputs,
                            status: status,
                            latencyMs: refinement.latencyMillis
                        ) {
                            try await store.insertSemanticScanResult(failureResult)
                            scanResultIds.append(failureResult.id)
                        }
                    }

                    if refinement.status != .success,
                       let blockingPlan = remainingPlans.dropFirst(refinement.failedWindowStatuses.count).first,
                       let failureResult = makeRefinementFailureScanResult(
                            plan: blockingPlan,
                            inputs: inputs,
                            status: refinement.status,
                            latencyMs: refinement.latencyMillis
                       ) {
                        try await store.insertSemanticScanResult(failureResult)
                        scanResultIds.append(failureResult.id)
                    }
                } else if refinement.status != .success {
                    let attemptedLineRefs = Set(zoomPlans.flatMap(\.lineRefs))
                    let attemptedSegments = inputs.segments.filter { attemptedLineRefs.contains($0.segmentIndex) }
                    if let failureResult = makeFailureScanResult(
                        scanPass: "passB",
                        attemptedSegments: attemptedSegments,
                        inputs: inputs,
                        status: refinement.status,
                        latencyMs: refinement.latencyMillis
                    ) {
                        try await store.insertSemanticScanResult(failureResult)
                        scanResultIds.append(failureResult.id)
                    }
                }
            }
        }

        return (scanResultIds, evidenceEventIds)
    }

    // MARK: - Helpers

    /// H-R3-2: classify `AnalysisStoreError` cases that will never succeed
    /// on retry against the same schema/inputs. Permanent errors are
    /// short-circuited to `retryCount = maxRetries` so the C-B gate skips
    /// them on the next run — there is no point admitting them again.
    ///
    /// Schema-validator rejections (`invalidEvidenceEvent`,
    /// `invalidScanCohortJSON`), oversized blob payloads (currently
    /// surfaced as `insertFailed("payloadTooLarge: ...")`),
    /// unexpected-NULL surprises (`invalidRow`), and body collisions
    /// (`evidenceEventBodyMismatch`) are all permanent by
    /// construction: the runner will produce the exact same row on
    /// the next attempt.
    /// R7-Fix11: canonical hash helper. SHA-256 of `canonical` truncated
    /// to 16 hex chars (~64 bits). The `|` separator is safe inside the
    /// hash input because it is never parsed back out — the id is an
    /// opaque handle, not a structured key.
    nonisolated private static func hashedId(prefix: String, canonical: String) -> String {
        let digest = SHA256.hash(data: Data(canonical.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return "\(prefix)-\(hex.prefix(16))"
    }

    /// R7-Fix11: stable id for pass-A / pass-B scan rows. Exposed as
    /// `internal static` so tests can assert determinism and
    /// collision-immunity without reaching into the private helpers.
    nonisolated static func makeScanResultIdForTesting(
        assetId: String,
        transcriptVersion: String,
        pass: String,
        windowIndex: Int
    ) -> String {
        let canonical = "asset=\(assetId)|version=\(transcriptVersion)|pass=\(pass)|window=\(windowIndex)"
        return hashedId(prefix: "scan", canonical: canonical)
    }

    /// R7-Fix11: stable id for the "no windows, record the failure"
    /// sentinel row. Distinct keyspace from the normal per-window ids so
    /// a future `windowIndex = -1` tuple cannot collide with it.
    nonisolated static func makeFailureScanResultIdForTesting(
        assetId: String,
        transcriptVersion: String,
        pass: String,
        windowKey: String? = nil
    ) -> String {
        let kind = windowKey.map { "failure|\($0)" } ?? "failure"
        let canonical = "asset=\(assetId)|version=\(transcriptVersion)|pass=\(pass)|kind=\(kind)"
        return hashedId(prefix: "scan", canonical: canonical)
    }

    nonisolated private static func makeEvidenceEventId(
        assetId: String,
        transcriptVersion: String,
        eventType: String,
        sourceType: EvidenceSourceType,
        atomOrdinals: String,
        evidenceJSON: String,
        scanCohortJSON: String
    ) -> String {
        let canonical =
            "asset=\(assetId)|version=\(transcriptVersion)|event=\(eventType)|source=\(sourceType.rawValue)|" +
            "ordinals=\(atomOrdinals)|evidence=\(evidenceJSON)|cohort=\(scanCohortJSON)"
        return hashedId(prefix: "evt", canonical: canonical)
    }

    /// R7-Fix11: stable id for backfill jobs. Mirrors the scan-result
    /// helper so both id spaces are immune to asset-id hyphen drift.
    nonisolated static func makeJobIdForTesting(
        analysisAssetId: String,
        transcriptVersion: String,
        phase: BackfillJobPhase,
        offset: Int
    ) -> String {
        let canonical = "asset=\(analysisAssetId)|version=\(transcriptVersion)|phase=\(phase.rawValue)|offset=\(offset)"
        return hashedId(prefix: "fm", canonical: canonical)
    }

    /// bd-1tl: stable, log-friendly case name for an `AnalysisStoreError`.
    /// `String(describing:)` on an enum with associated values gives the
    /// case name plus the payload (e.g. `evidenceEventBodyMismatch(id: "...")`).
    /// We strip the payload so log scrapers can match on the case name alone
    /// and so production telemetry rolls up cleanly across distinct payloads.
    nonisolated static func caseName(of error: AnalysisStoreError) -> String {
        switch error {
        case .openFailed: return "openFailed"
        case .migrationFailed: return "migrationFailed"
        case .queryFailed: return "queryFailed"
        case .insertFailed: return "insertFailed"
        case .notFound: return "notFound"
        case .duplicateJobId: return "duplicateJobId"
        case .invalidRow: return "invalidRow"
        case .invalidEvidenceEvent: return "invalidEvidenceEvent"
        case .invalidScanCohortJSON: return "invalidScanCohortJSON"
        case .invalidStateTransition: return "invalidStateTransition"
        case .evidenceEventBodyMismatch: return "evidenceEventBodyMismatch"
        }
    }

    private static func isPermanent(_ error: AnalysisStoreError) -> Bool {
        switch error {
        case .invalidEvidenceEvent,
             .evidenceEventBodyMismatch,
             .invalidScanCohortJSON,
             .invalidRow:
            return true
        case .insertFailed(let message):
            return message.hasPrefix("payloadTooLarge:")
        case .openFailed, .migrationFailed, .queryFailed,
             .notFound, .duplicateJobId, .invalidStateTransition:
            return false
        }
    }

    private func phasePriority(_ phase: BackfillJobPhase) -> Int {
        switch phase {
        case .scanLikelyAdSlots: 30
        case .scanHarvesterProposals: 20
        case .scanRandomAuditWindows: 10
        case .fullEpisodeScan: 5
        }
    }

    private func makeScanResult(
        windowOutput: FMCoarseWindowOutput,
        inputs: AssetInputs,
        scanPass: String,
        status: SemanticScanStatus
    ) -> SemanticScanResult {
        let firstAtom = inputs.segments.first(where: {
            $0.segmentIndex == windowOutput.lineRefs.first
        })?.firstAtomOrdinal ?? 0
        let lastAtom = inputs.segments.first(where: {
            $0.segmentIndex == windowOutput.lineRefs.last
        })?.lastAtomOrdinal ?? firstAtom
        return SemanticScanResult(
            // H-3: deterministic id. A random UUID suffix regenerated on
            // every run would leave orphan rows when a prior run crashed
            // after insert but before completion. The `(assetId,
            // transcriptVersion, pass, windowIndex)` quadruple is the
            // logical row key; combined with the `UNIQUE(reuseKeyHash)`
            // constraint + H-1's "refusal can't overwrite success" guard
            // this makes re-runs fully idempotent at the scan-result row
            // level.
            //
            // C-R3-2: transcriptVersion must be in the id itself. It is
            // already part of `reuseKeyHash`, so two runs with different
            // transcript versions produce distinct reuse hashes but would
            // collide on a transcriptVersion-free PK — INSERT OR REPLACE
            // then deletes the prior run's row regardless of H-1's
            // success-protection probe (which keys off reuseKeyHash).
            id: Self.makeScanResultIdForTesting(
                assetId: inputs.analysisAssetId,
                transcriptVersion: inputs.transcriptVersion,
                pass: scanPass,
                windowIndex: windowOutput.windowIndex
            ),
            analysisAssetId: inputs.analysisAssetId,
            windowFirstAtomOrdinal: firstAtom,
            windowLastAtomOrdinal: lastAtom,
            windowStartTime: windowOutput.startTime,
            windowEndTime: windowOutput.endTime,
            scanPass: scanPass,
            transcriptQuality: windowOutput.transcriptQuality,
            disposition: windowOutput.screening.disposition,
            spansJSON: encodeSupport(windowOutput.screening.support),
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: windowOutput.latencyMillis,
            prewarmHit: false,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: inputs.transcriptVersion
        )
    }

    private func makeRefinementScanResult(
        windowOutput: FMRefinementWindowOutput,
        inputs: AssetInputs,
        status: SemanticScanStatus
    ) -> SemanticScanResult {
        let firstAtom = inputs.segments.first(where: {
            $0.segmentIndex == windowOutput.lineRefs.first
        })?.firstAtomOrdinal ?? 0
        let lastAtom = inputs.segments.first(where: {
            $0.segmentIndex == windowOutput.lineRefs.last
        })?.lastAtomOrdinal ?? firstAtom
        let startTime = windowOutput.spans.first.map { span in
            inputs.segments.first(where: { $0.segmentIndex == span.firstLineRef })?.startTime ?? 0
        } ?? 0
        let endTime = windowOutput.spans.last.map { span in
            inputs.segments.first(where: { $0.segmentIndex == span.lastLineRef })?.endTime ?? 0
        } ?? 0
        return SemanticScanResult(
            // H-3: deterministic id (see makeScanResult for the full
            // note). C-R3-2: transcriptVersion must be included so rows
            // from different transcript versions cannot collide on the PK.
            id: Self.makeScanResultIdForTesting(
                assetId: inputs.analysisAssetId,
                transcriptVersion: inputs.transcriptVersion,
                pass: "passB",
                windowIndex: windowOutput.windowIndex
            ),
            analysisAssetId: inputs.analysisAssetId,
            windowFirstAtomOrdinal: firstAtom,
            windowLastAtomOrdinal: lastAtom,
            windowStartTime: startTime,
            windowEndTime: endTime,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: windowOutput.spans.isEmpty ? .noAds : .containsAd,
            spansJSON: encodeRefinedSpans(windowOutput.spans),
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: windowOutput.latencyMillis,
            prewarmHit: false,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: inputs.transcriptVersion
        )
    }

    private func makeFailureScanResult(
        scanPass: String,
        attemptedSegments: [AdTranscriptSegment],
        inputs: AssetInputs,
        status: SemanticScanStatus,
        latencyMs: Double,
        windowKey: String? = nil
    ) -> SemanticScanResult? {
        guard let range = attemptedRange(for: attemptedSegments) else {
            return nil
        }
        return SemanticScanResult(
            id: Self.makeFailureScanResultIdForTesting(
                assetId: inputs.analysisAssetId,
                transcriptVersion: inputs.transcriptVersion,
                pass: scanPass,
                windowKey: windowKey
            ),
            analysisAssetId: inputs.analysisAssetId,
            windowFirstAtomOrdinal: range.firstAtomOrdinal,
            windowLastAtomOrdinal: range.lastAtomOrdinal,
            windowStartTime: range.startTime,
            windowEndTime: range.endTime,
            scanPass: scanPass,
            transcriptQuality: range.transcriptQuality,
            disposition: .abstain,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: latencyMs,
            prewarmHit: false,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: inputs.transcriptVersion
        )
    }

    private func makeRefinementFailureScanResult(
        plan: RefinementWindowPlan,
        inputs: AssetInputs,
        status: SemanticScanStatus,
        latencyMs: Double
    ) -> SemanticScanResult? {
        let attemptedSegments = inputs.segments.filter { plan.lineRefs.contains($0.segmentIndex) }
        return makeFailureScanResult(
            scanPass: "passB",
            attemptedSegments: attemptedSegments,
            inputs: inputs,
            status: status,
            latencyMs: latencyMs,
            windowKey: "window=\(plan.windowIndex)"
        )
    }

    private func makeCoarseFailureScanResult(
        plan: CoarsePassWindowPlan,
        inputs: AssetInputs,
        status: SemanticScanStatus,
        latencyMs: Double
    ) -> SemanticScanResult? {
        let attemptedSegments = inputs.segments.filter { plan.lineRefs.contains($0.segmentIndex) }
        return makeFailureScanResult(
            scanPass: "passA",
            attemptedSegments: attemptedSegments,
            inputs: inputs,
            status: status,
            latencyMs: latencyMs,
            windowKey: "window=\(plan.windowIndex)"
        )
    }

    private func attemptedRange(for segments: [AdTranscriptSegment]) -> AttemptedRange? {
        let ordered = segments.sorted { lhs, rhs in
            if lhs.segmentIndex == rhs.segmentIndex {
                return lhs.startTime < rhs.startTime
            }
            return lhs.segmentIndex < rhs.segmentIndex
        }
        guard let first = ordered.first,
              let last = ordered.last else {
            return nil
        }
        return AttemptedRange(
            firstAtomOrdinal: first.firstAtomOrdinal,
            lastAtomOrdinal: last.lastAtomOrdinal,
            startTime: first.startTime,
            endTime: last.endTime,
            transcriptQuality: aggregateTranscriptQuality(for: ordered)
        )
    }

    private func aggregateTranscriptQuality(for segments: [AdTranscriptSegment]) -> TranscriptQuality {
        let qualities = TranscriptQualityEstimator.assess(segments: segments).map(\.quality)
        if qualities.contains(.unusable) {
            return .unusable
        }
        if qualities.contains(.degraded) {
            return .degraded
        }
        return .good
    }

    private func makeEvidenceEvents(
        windowOutput: FMRefinementWindowOutput,
        inputs: AssetInputs,
        jobId: String
    ) -> [EvidenceEvent] {
        var events: [EvidenceEvent] = []
        var seenEventIds = Set<String>()
        for span in windowOutput.spans {
            // Store validator `validateAtomOrdinalsJSON` requires a JSON array of
            // integers. Previously we wrote the comma-joined form ("1,2,3") which
            // the validator rejected, causing every refinement-pass evidence
            // insert to throw `AnalysisStoreError.invalidEvidenceEvent` and
            // silently drop the row.
            let ordinals = Array(span.firstAtomOrdinal...span.lastAtomOrdinal)
            let atomOrdinals = (try? JSONEncoder().encode(ordinals))
                .flatMap { String(data: $0, encoding: .utf8) } ?? "[]"
            let payload = EvidencePayload(
                commercialIntent: span.commercialIntent.rawValue,
                ownership: span.ownership.rawValue,
                certainty: span.certainty.rawValue,
                boundaryPrecision: span.boundaryPrecision.rawValue,
                firstLineRef: span.firstLineRef,
                lastLineRef: span.lastLineRef,
                jobId: jobId,
                // R4-Fix4: persist `memoryWriteEligible` so the H-R3-1
                // in-memory protection has a production consumer. A future
                // Phase 8 sponsor-memory writer reading
                // `evidence_events.evidenceJSON` can honor the eligibility
                // decision without recomputing it.
                memoryWriteEligible: span.memoryWriteEligible
            )
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.sortedKeys]
            let evidenceJSON = (try? encoder.encode(payload))
                .flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
            let eventId = Self.makeEvidenceEventId(
                assetId: inputs.analysisAssetId,
                transcriptVersion: inputs.transcriptVersion,
                eventType: "fm.spanRefinement",
                sourceType: .fm,
                atomOrdinals: atomOrdinals,
                evidenceJSON: evidenceJSON,
                scanCohortJSON: scanCohortJSON
            )
            guard seenEventIds.insert(eventId).inserted else {
                continue
            }
            events.append(
                EvidenceEvent(
                    id: eventId,
                    analysisAssetId: inputs.analysisAssetId,
                    eventType: "fm.spanRefinement",
                    sourceType: .fm,
                    atomOrdinals: atomOrdinals,
                    evidenceJSON: evidenceJSON,
                    scanCohortJSON: scanCohortJSON,
                    createdAt: clock().timeIntervalSince1970
                )
            )
        }
        return events
    }

    private func encodeSupport(_ support: CoarseSupportSchema?) -> String {
        guard let support else { return "[]" }
        let data = (try? JSONEncoder().encode(support)) ?? Data("{}".utf8)
        return String(data: data, encoding: .utf8) ?? "{}"
    }

    private func encodeRefinedSpans(_ spans: [RefinedAdSpan]) -> String {
        struct Encodable: Codable {
            let firstLineRef: Int
            let lastLineRef: Int
            let commercialIntent: String
            let ownership: String
            let certainty: String
        }
        let payload = spans.map {
            Encodable(
                firstLineRef: $0.firstLineRef,
                lastLineRef: $0.lastLineRef,
                commercialIntent: $0.commercialIntent.rawValue,
                ownership: $0.ownership.rawValue,
                certainty: $0.certainty.rawValue
            )
        }
        let data = (try? JSONEncoder().encode(payload)) ?? Data("[]".utf8)
        return String(data: data, encoding: .utf8) ?? "[]"
    }
}

// MARK: - Evidence payload

private struct EvidencePayload: Codable {
    let commercialIntent: String
    let ownership: String
    let certainty: String
    let boundaryPrecision: String
    let firstLineRef: Int
    let lastLineRef: Int
    let jobId: String
    /// R4-Fix4: persisted span-level eligibility for sponsor-memory writes.
    /// `true` only when every resolved evidence anchor passed the
    /// span-range containment check AND resolved via `.evidenceRef` (the
    /// C8 contract). Phase 8 sponsor-memory writers must gate on this.
    let memoryWriteEligible: Bool
}

private struct AttemptedRange {
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let startTime: Double
    let endTime: Double
    let transcriptQuality: TranscriptQuality
}
