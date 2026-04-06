// BackfillJobRunner.swift
// Phase 3 shadow-mode orchestrator. Walks: CoveragePlanner -> AdmissionController
// -> FoundationModelClassifier (coarse + refinement) -> SemanticScanResult /
// EvidenceEvent persistence. The runner never writes AdWindow rows: shadow mode
// is observation-only. Phase 6 will introduce a separate decision-fusion layer
// that promotes FM evidence to user-visible cues; until then, .enabled silently
// degrades to .shadow with a logged warning.

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
    private let decisionCohortJSON: String?
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
        decisionCohortJSON: String? = nil,
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
        self.decisionCohortJSON = decisionCohortJSON
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
            let job = BackfillJob(
                jobId: "fm-\(inputs.analysisAssetId)-\(phase.rawValue)-\(offset)",
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
                decisionCohortJSON: decisionCohortJSON,
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
                // Mark the next queued job as deferred so retries can pick it up.
                if let next = enqueuedJobs.first(where: {
                    !admitted.contains($0.jobId) && !deferred.contains($0.jobId)
                }) {
                    try await store.checkpointBackfillJob(
                        jobId: next.jobId,
                        progressCursor: nil,
                        status: .deferred,
                        deferReason: reason
                    )
                    deferred.append(next.jobId)
                }
                logger.info("FM backfill deferred: \(reason)")
                break
            }

            guard let job = decision.job else { break }
            admitted.append(job.jobId)

            do {
                try await store.checkpointBackfillJob(
                    jobId: job.jobId,
                    progressCursor: nil,
                    status: .running,
                    deferReason: nil
                )

                let (resultIds, eventIds) = try await runJob(job, inputs: inputs)
                scanResultIds.append(contentsOf: resultIds)
                evidenceEventIds.append(contentsOf: eventIds)

                try await store.checkpointBackfillJob(
                    jobId: job.jobId,
                    progressCursor: BackfillProgressCursor(
                        processedUnitCount: 1,
                        lastProcessedUpperBoundSec: inputs.segments.last?.endTime
                    ),
                    status: .complete,
                    deferReason: nil
                )
            } catch is CancellationError {
                await admissionController.finish(jobId: job.jobId)
                try await store.checkpointBackfillJob(
                    jobId: job.jobId,
                    progressCursor: nil,
                    status: .deferred,
                    deferReason: "cancelled"
                )
                throw CancellationError()
            } catch {
                try await store.checkpointBackfillJob(
                    jobId: job.jobId,
                    progressCursor: nil,
                    status: .failed,
                    retryCount: job.retryCount + 1,
                    deferReason: String(describing: error)
                )
                logger.error("FM backfill job \(job.jobId) failed: \(error.localizedDescription)")
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
                status: coarse.status
            )
            try await store.insertSemanticScanResult(result)
            scanResultIds.append(result.id)
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
                        status: refinement.status
                    )
                    try await store.insertSemanticScanResult(result)
                    scanResultIds.append(result.id)

                    for evidence in makeEvidenceEvents(
                        windowOutput: window,
                        inputs: inputs,
                        jobId: job.jobId
                    ) {
                        try await store.insertEvidenceEvent(evidence)
                        evidenceEventIds.append(evidence.id)
                    }
                }
            }
        }

        return (scanResultIds, evidenceEventIds)
    }

    // MARK: - Helpers

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
            id: "scan-\(inputs.analysisAssetId)-\(scanPass)-\(windowOutput.windowIndex)-\(UUID().uuidString.prefix(8))",
            analysisAssetId: inputs.analysisAssetId,
            windowFirstAtomOrdinal: firstAtom,
            windowLastAtomOrdinal: lastAtom,
            windowStartTime: windowOutput.startTime,
            windowEndTime: windowOutput.endTime,
            scanPass: scanPass,
            transcriptQuality: windowOutput.screening.transcriptQuality,
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
            id: "scan-\(inputs.analysisAssetId)-passB-\(windowOutput.windowIndex)-\(UUID().uuidString.prefix(8))",
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

    private func makeEvidenceEvents(
        windowOutput: FMRefinementWindowOutput,
        inputs: AssetInputs,
        jobId: String
    ) -> [EvidenceEvent] {
        var events: [EvidenceEvent] = []
        for (spanOffset, span) in windowOutput.spans.enumerated() {
            let atomOrdinals = (span.firstAtomOrdinal...span.lastAtomOrdinal)
                .map(String.init)
                .joined(separator: ",")
            let payload = EvidencePayload(
                commercialIntent: span.commercialIntent.rawValue,
                ownership: span.ownership.rawValue,
                certainty: span.certainty.rawValue,
                boundaryPrecision: span.boundaryPrecision.rawValue,
                firstLineRef: span.firstLineRef,
                lastLineRef: span.lastLineRef,
                jobId: jobId
            )
            let evidenceJSON = (try? JSONEncoder().encode(payload))
                .flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
            events.append(
                EvidenceEvent(
                    id: "evt-\(jobId)-\(windowOutput.windowIndex)-\(spanOffset)-\(UUID().uuidString.prefix(8))",
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
}
