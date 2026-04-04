// AnalysisJobReconciler.swift
// Repairs stale, blocked, and broken job state in the analysis_jobs table.
// Runs at app launch and at periodic reconciliation points.

import Foundation
import OSLog

// MARK: - ReconciliationReport

struct ReconciliationReport: Sendable {
    let expiredLeasesRecovered: Int
    let missingFilesUnblocked: Int
    let missingFilesStillBlocked: Int
    let modelsUnblocked: Int
    let staleVersionsSuperseded: Int
    let completedJobsGarbageCollected: Int
    let failedJobsBackedOff: Int
    let unEnqueuedDownloadsCreated: Int
}

// MARK: - AnalysisJobReconciler

actor AnalysisJobReconciler {
    private let store: AnalysisStore
    private let downloadManager: any DownloadProviding
    private let capabilitiesService: any CapabilitiesProviding
    private let config: PreAnalysisConfig
    private let logger = Logger(subsystem: "com.playhead", category: "JobReconciler")
    private var isReconciling = false

    /// The current analysis version. Jobs whose workKey encodes a different
    /// version are considered stale and will be superseded.
    static var currentAnalysisVersion: Int { PreAnalysisConfig.analysisVersion }

    init(
        store: AnalysisStore,
        downloadManager: any DownloadProviding,
        capabilitiesService: any CapabilitiesProviding,
        config: PreAnalysisConfig = .load()
    ) {
        self.store = store
        self.downloadManager = downloadManager
        self.capabilitiesService = capabilitiesService
        self.config = config
    }

    // MARK: - Reconcile

    func reconcile() async throws -> ReconciliationReport {
        guard !isReconciling else {
            logger.info("Reconciliation already in progress, skipping")
            return ReconciliationReport(
                expiredLeasesRecovered: 0, missingFilesUnblocked: 0,
                missingFilesStillBlocked: 0, modelsUnblocked: 0,
                staleVersionsSuperseded: 0, completedJobsGarbageCollected: 0,
                failedJobsBackedOff: 0, unEnqueuedDownloadsCreated: 0
            )
        }
        isReconciling = true
        defer { isReconciling = false }

        let step1 = try await recoverExpiredLeases()
        let step2 = try await unblockMissingFiles()
        let step3 = try await unblockModelUnavailable()
        let step4 = try await supersedeStaleVersions()
        let step5 = try await garbageCollectOldJobs()
        let step6 = try await backoffFailedJobs()
        let step7 = try await discoverUnEnqueuedDownloads()

        let report = ReconciliationReport(
            expiredLeasesRecovered: step1,
            missingFilesUnblocked: step2.unblocked,
            missingFilesStillBlocked: step2.stillBlocked,
            modelsUnblocked: step3,
            staleVersionsSuperseded: step4,
            completedJobsGarbageCollected: step5,
            failedJobsBackedOff: step6,
            unEnqueuedDownloadsCreated: step7
        )

        logger.info("""
        Reconciliation complete: \
        expiredLeases=\(report.expiredLeasesRecovered), \
        missingFilesUnblocked=\(report.missingFilesUnblocked), \
        missingFilesStillBlocked=\(report.missingFilesStillBlocked), \
        modelsUnblocked=\(report.modelsUnblocked), \
        staleVersions=\(report.staleVersionsSuperseded), \
        gc=\(report.completedJobsGarbageCollected), \
        backoff=\(report.failedJobsBackedOff), \
        newJobs=\(report.unEnqueuedDownloadsCreated)
        """)

        return report
    }

    // MARK: - Step 1: Recover expired leases

    private func recoverExpiredLeases() async throws -> Int {
        let now = Date().timeIntervalSince1970
        let expired = try await store.fetchJobsWithExpiredLeases(before: now)
        // Recover any job with an expired lease — the lease proves it was being processed.
        let recoverable = expired.filter { $0.state == "running" || $0.state == "queued" || $0.state == "paused" }
        for job in recoverable {
            try await store.recoverExpiredLease(jobId: job.jobId)
        }
        if !recoverable.isEmpty {
            logger.info("Recovered \(recoverable.count) expired lease(s)")
        }
        return recoverable.count
    }

    // MARK: - Step 2: Unblock missingFile jobs

    private func unblockMissingFiles() async throws -> (unblocked: Int, stillBlocked: Int) {
        let blocked = try await store.fetchJobsByState("blocked:missingFile")
        var unblocked = 0
        var stillBlocked = 0
        for job in blocked {
            let url = await downloadManager.cachedFileURL(for: job.episodeId)
            if url != nil {
                try await store.updateJobState(jobId: job.jobId, state: "queued")
                unblocked += 1
            } else {
                stillBlocked += 1
            }
        }
        if unblocked > 0 {
            logger.info("Unblocked \(unblocked) missingFile job(s)")
        }
        return (unblocked, stillBlocked)
    }

    // MARK: - Step 3: Unblock modelUnavailable jobs

    private func unblockModelUnavailable() async throws -> Int {
        let blocked = try await store.fetchJobsByState("blocked:modelUnavailable")
        guard !blocked.isEmpty else { return 0 }

        let snapshot = await capabilitiesService.currentSnapshot
        guard snapshot.foundationModelsAvailable else { return 0 }

        let ids = blocked.map(\.jobId)
        try await store.batchUpdateJobState(jobIds: ids, state: "queued")
        logger.info("Unblocked \(ids.count) modelUnavailable job(s)")
        return ids.count
    }

    // MARK: - Step 4: Supersede stale versions

    private func supersedeStaleVersions() async throws -> Int {
        // Fetch all non-terminal jobs and check their workKey version.
        let allStates = ["queued", "running", "paused",
                         "blocked:missingFile", "blocked:modelUnavailable", "failed"]
        var superseded = 0
        for state in allStates {
            let jobs = try await store.fetchJobsByState(state)
            for job in jobs {
                let version = parseVersionFromWorkKey(job.workKey)
                if let version, version != Self.currentAnalysisVersion {
                    try await store.updateJobState(jobId: job.jobId, state: "superseded")
                    superseded += 1
                }
            }
        }
        if superseded > 0 {
            logger.info("Superseded \(superseded) stale-version job(s)")
        }
        return superseded
    }

    // MARK: - Step 5: GC old completed/superseded jobs

    private func garbageCollectOldJobs() async throws -> Int {
        let sevenDaysAgo = Date().timeIntervalSince1970 - (7 * 24 * 3600)
        let deleted = try await store.deleteOldJobs(
            olderThan: sevenDaysAgo,
            inStates: ["complete", "superseded"]
        )
        if deleted > 0 {
            logger.info("Garbage collected \(deleted) old job(s)")
        }
        return deleted
    }

    // MARK: - Step 6: Exponential backoff for failed jobs

    private func backoffFailedJobs() async throws -> Int {
        let failed = try await store.fetchJobsByState("failed")
        let needsBackoff = failed.filter { $0.nextEligibleAt == nil }
        let now = Date().timeIntervalSince1970
        for job in needsBackoff {
            let delay = min(pow(2.0, Double(job.attemptCount)) * 60.0, 3600.0)
            let nextEligible = now + delay
            try await store.updateJobState(
                jobId: job.jobId,
                state: "failed",
                nextEligibleAt: nextEligible
            )
        }
        if !needsBackoff.isEmpty {
            logger.info("Applied backoff to \(needsBackoff.count) failed job(s)")
        }
        return needsBackoff.count
    }

    // MARK: - Step 7: Discover un-enqueued downloads

    private func discoverUnEnqueuedDownloads() async throws -> Int {
        let cachedIds = await downloadManager.allCachedEpisodeIds()
        let activeJobIds = try await store.fetchActiveJobEpisodeIds()

        let unEnqueued = cachedIds.subtracting(activeJobIds)
        for episodeId in unEnqueued {
            guard let fp = await downloadManager.fingerprint(for: episodeId) else { continue }
            let workKey = AnalysisJob.computeWorkKey(
                fingerprint: fp.strong ?? fp.weak,
                analysisVersion: Self.currentAnalysisVersion,
                jobType: "preAnalysis"
            )
            let job = AnalysisJob(
                jobId: UUID().uuidString,
                jobType: "preAnalysis",
                episodeId: episodeId,
                podcastId: nil,
                analysisAssetId: nil,
                workKey: workKey,
                sourceFingerprint: fp.strong ?? fp.weak,
                downloadId: episodeId,
                priority: 0,
                desiredCoverageSec: config.defaultT0DepthSeconds,
                featureCoverageSec: 0,
                transcriptCoverageSec: 0,
                cueCoverageSec: 0,
                state: "queued",
                attemptCount: 0,
                nextEligibleAt: nil,
                leaseOwner: nil,
                leaseExpiresAt: nil,
                lastErrorCode: nil,
                createdAt: Date().timeIntervalSince1970,
                updatedAt: Date().timeIntervalSince1970
            )
            try await store.insertJob(job)
        }
        if !unEnqueued.isEmpty {
            logger.info("Created \(unEnqueued.count) job(s) for un-enqueued downloads")
        }
        return unEnqueued.count
    }

    // MARK: - Helpers

    /// Extracts the analysis version from a workKey.
    /// Base format: "fingerprint:version:jobType"
    /// Tier-advanced format: "fingerprint:version:jobType:coverage"
    /// The version is always the component immediately before the jobType token.
    private func parseVersionFromWorkKey(_ workKey: String) -> Int? {
        let parts = workKey.split(separator: ":")
        // Find the jobType component (preAnalysis, playback, backfill)
        let jobTypes: Set<Substring> = ["preAnalysis", "playback", "backfill"]
        guard let jobTypeIndex = parts.firstIndex(where: { jobTypes.contains($0) }),
              jobTypeIndex > 0 else { return nil }
        return Int(parts[jobTypeIndex - 1])
    }
}
