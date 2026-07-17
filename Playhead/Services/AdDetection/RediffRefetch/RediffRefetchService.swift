// RediffRefetchService.swift
// playhead-xsdz.28: the WiFi+charging BGTask that drives the rediff re-fetch
// policy. DEFAULT OFF — with the flag off nothing is scheduled and no network
// or filesystem is touched, so the app is byte-identical (mirrors the flag-OFF
// posture of the other rediff pieces: xsdz.27 capture, xsdz.29 ownership).
//
// WHAT IT DOES (only when enabled + granted a BGProcessingTask window):
//   1. Reschedule the next fire first (so a crash still leaves a pending task).
//   2. Enumerate downloaded episodes as `Sendable` candidates.
//   3. For each due candidate (`RediffRefetchPolicy.eligibility` — the ≥24h
//      gate + backoff + retry budget):
//        a. Ranged-GET head/tail sample of the CURRENT enclosure (NO HEAD/ETag).
//        b. Compare to the played copy's local head/tail sample.
//        c. IDENTICAL → non-rotator → SKIP the full fetch, back off.
//        d. DIFFERENT → full re-fetch → fingerprint the B-side OFF the hot
//           actor → DELETE the B-copy (never persisted) → mark resolved.
//      Bandwidth is accounted per candidate and summed for the sweep.
//   4. Complete the BGTask exactly once (success unless expired).
//
// BGTASK POLICY (spike §5): `BGProcessingTaskRequest` with
// `requiresExternalPower = true` (charging) + `requiresNetworkConnectivity =
// true` (network present); WiFi is pinned by the WiFi-only URLSession in the
// production sampler/fetcher. ~54 MB/episode, ~1.1 GB/library-week — acceptable
// on WiFi overnight, unacceptable on cellular.
//
// OFF THE HOT ACTOR (xsdz.29 R5 residual): this is a DEDICATED actor, not the
// playback / AdDetection actor. The B-side resample+fingerprint runs through
// the `RediffBSideFingerprinting` seam whose production conformer is a plain
// value type — its work lands on the generic executor, never stalling a serial
// hot actor with a full-episode resample.

import BackgroundTasks
import Foundation
import os
import OSLog

/// Drives the WiFi+charging `BGProcessingTask` that re-fetches episode audio for
/// the rediff width oracle. Default OFF.
actor RediffRefetchService {

    // MARK: - Identity & flag

    /// BGTaskScheduler identifier. Declared in `Info.plist`'s
    /// `BGTaskSchedulerPermittedIdentifiers` + `project.yml` (kept in sync by
    /// the pbxproj/Info.plist regen), under the shared `com.playhead.app.`
    /// namespace. Permitted-but-unregistered until activation (xsdz.36) wires
    /// the handler — registering an identifier absent from the plist crashes,
    /// so the entry ships now for flip-readiness.
    static let taskIdentifier = "com.playhead.app.rediff-refetch"

    /// MASTER default-OFF flag. `false` ⇒ no scheduling, no network, no fetch —
    /// the app is byte-identical. Activation (xsdz.36) is the only place this
    /// flips. A service can still be constructed `enabled: true` in tests to
    /// exercise the sweep.
    static let isEnabledByDefault = false

    /// Soft floor iOS should wait before the next fire. iOS defers further on
    /// its own heuristics; the per-episode ≥24h/backoff gates are the real
    /// cadence, so this only bounds how often the app is woken to CHECK.
    static let minimumRefetchInterval: TimeInterval = 6 * 60 * 60

    // MARK: - Dependencies

    private let enabled: Bool
    private let config: RediffRefetchPolicy.Configuration
    private let enumerator: any RediffRefetchEnumerating
    private let rangedSampler: any RangedAudioSampling
    private let localSampler: any LocalAudioSampling
    private let fullFetcher: any FullEpisodeFetching
    private let bsideFingerprinter: any RediffBSideFingerprinting
    private let recorder: any RediffRefetchRecording
    private let fileRemover: any RediffTempFileRemoving
    private let taskScheduler: any BackgroundTaskScheduling
    /// Injectable clock so eligibility is deterministic in tests.
    private let now: @Sendable () -> Double
    private let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    // MARK: - Per-fire state

    /// Flipped by the expiration handler so the sweep bails at the next
    /// candidate boundary (no `Task.cancel` — the enclosing task is not ours).
    private var expired = false
    /// Idempotence guard for `setTaskCompleted` (iOS terminates on a 2nd call).
    private var taskCompleted = false

    // MARK: - Init

    init(
        enabled: Bool = RediffRefetchService.isEnabledByDefault,
        config: RediffRefetchPolicy.Configuration = .default,
        enumerator: any RediffRefetchEnumerating,
        rangedSampler: any RangedAudioSampling,
        localSampler: any LocalAudioSampling,
        fullFetcher: any FullEpisodeFetching,
        bsideFingerprinter: any RediffBSideFingerprinting,
        recorder: any RediffRefetchRecording = LoggingRediffRefetchRecorder(),
        fileRemover: any RediffTempFileRemoving = FileManagerTempFileRemover(),
        taskScheduler: any BackgroundTaskScheduling = BGTaskScheduler.shared,
        now: @escaping @Sendable () -> Double = { Date().timeIntervalSince1970 }
    ) {
        self.enabled = enabled
        self.config = config
        self.enumerator = enumerator
        self.rangedSampler = rangedSampler
        self.localSampler = localSampler
        self.fullFetcher = fullFetcher
        self.bsideFingerprinter = bsideFingerprinter
        self.recorder = recorder
        self.fileRemover = fileRemover
        self.taskScheduler = taskScheduler
        self.now = now
    }

    /// Whether re-fetch is enabled for this instance. Exposed for tests.
    func isEnabled() -> Bool { enabled }

    // MARK: - Scheduling

    /// Submit the next `BGProcessingTaskRequest` (WiFi+charging). A no-op when
    /// disabled — the default-OFF byte-identity contract (nothing scheduled).
    func scheduleNextRefetch() {
        guard enabled else {
            logger.debug("rediff re-fetch disabled — not scheduling")
            return
        }
        let request = BGProcessingTaskRequest(identifier: Self.taskIdentifier)
        request.requiresNetworkConnectivity = true   // network present…
        request.requiresExternalPower = true          // …and charging (WiFi pinned by the URLSession)
        request.earliestBeginDate = Date(timeIntervalSinceNow: Self.minimumRefetchInterval)
        do {
            try taskScheduler.submit(request)
            logger.info("Scheduled rediff re-fetch in \(Self.minimumRefetchInterval, privacy: .public)s")
        } catch {
            logger.error("Failed to schedule rediff re-fetch: \(String(describing: error), privacy: .public)")
        }
    }

    // MARK: - Handler

    /// Drive one BGProcessingTask fire: reschedule, install expiration, run the
    /// sweep, complete once. Defensive no-op-but-complete when disabled (the OS
    /// can only fire a task that was scheduled, which never happens when OFF).
    func handleRefetchTask(_ task: any BackgroundProcessingTaskProtocol) async {
        guard enabled else {
            task.setTaskCompleted(success: true)
            return
        }
        expired = false
        taskCompleted = false

        // Reschedule first so iOS always has a pending request even if the
        // sweep crashes mid-fire.
        scheduleNextRefetch()

        task.expirationHandler = { [weak self] in
            let box = _UncheckedSendableBox(task)
            Task { await self?.markExpiredAndComplete(box.value) }
        }

        await runRefetchSweep()

        completeTaskOnce(task, success: !expired)
    }

    /// The core sweep. Also callable directly by tests without a BGTask.
    func runRefetchSweep() async {
        guard enabled else { return }
        let candidates = await enumerator.candidates()
        let sweepNow = now()
        var totalBytes = 0
        var rotatedCount = 0

        for candidate in candidates {
            if expired { break }
            let eligibility = RediffRefetchPolicy.eligibility(
                now: sweepNow,
                downloadedAt: candidate.downloadedAt,
                state: candidate.attemptState,
                config: config
            )
            guard case .eligible = eligibility else {
                await recorder.recordOutcome(.skippedIneligible(assetId: candidate.assetId, reason: eligibility))
                continue
            }
            let (bytes, rotated) = await processCandidate(candidate, at: sweepNow)
            totalBytes += bytes
            if rotated { rotatedCount += 1 }
        }

        logger.info(
            "rediff re-fetch sweep: \(candidates.count, privacy: .public) candidates, \(rotatedCount, privacy: .public) rotated, \(totalBytes, privacy: .public) bytes"
        )
    }

    /// Pre-check one candidate; full-fetch + fingerprint + DELETE only on a
    /// rotation. Returns (bytes spent, rotated?). Errors are swallowed per
    /// candidate (recorded as `.failed`) so one bad episode cannot abort the
    /// sweep — matching the feed-refresh per-feed-swallow contract.
    private func processCandidate(
        _ candidate: RediffRefetchCandidate,
        at sweepNow: Double
    ) async -> (bytes: Int, rotated: Bool) {
        var precheckBytes = 0
        var fullFetchBytes = 0
        do {
            // (a) Ranged head/tail sample of the CURRENT enclosure (NO HEAD).
            let remote = try await rangedSampler.sample(
                url: candidate.enclosureURL,
                headBytes: config.headSampleBytes,
                tailBytes: config.tailSampleBytes
            )
            precheckBytes = remote.bytesTransferred

            // (b) Local played-copy sample.
            let local = try localSampler.sample(
                fileURL: candidate.localAudioURL,
                headBytes: config.headSampleBytes,
                tailBytes: config.tailSampleBytes
            )

            // (c) Non-rotator → SKIP the full fetch, back off.
            if !RediffRefetchPolicy.isRotated(local: local, remote: remote.fingerprint) {
                let newState = RediffRefetchPolicy.advanceUnchanged(candidate.attemptState, at: sweepNow)
                let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: precheckBytes, fullFetchBytes: 0)
                await recorder.recordOutcome(.unchanged(assetId: candidate.assetId, cost: cost, newState: newState))
                return (cost.totalBytes, false)
            }

            // (d) Rotator → full re-fetch → fingerprint (off hot actor) → DELETE.
            let full = try await fullFetcher.download(url: candidate.enclosureURL)
            fullFetchBytes = full.byteCount
            // NEVER persist the B-copy: delete on EVERY exit from this scope,
            // including a throw out of the fingerprint step below.
            defer { fileRemover.remove(full.fileURL) }

            let fingerprints = try await bsideFingerprinter.fingerprint(fileURL: full.fileURL)

            let newState = RediffRefetchPolicy.markResolved(candidate.attemptState, at: sweepNow)
            let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: precheckBytes, fullFetchBytes: fullFetchBytes)
            await recorder.recordOutcome(.rotated(
                assetId: candidate.assetId,
                cost: cost,
                fingerprintCount: fingerprints.count,
                newState: newState
            ))
            return (cost.totalBytes, true)
        } catch {
            let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: precheckBytes, fullFetchBytes: fullFetchBytes)
            await recorder.recordOutcome(.failed(
                assetId: candidate.assetId,
                cost: cost,
                error: String(describing: error)
            ))
            return (cost.totalBytes, false)
        }
    }

    // MARK: - Completion

    /// Expiration hop: flip `expired` so the sweep bails, and complete once.
    private func markExpiredAndComplete(_ task: any BackgroundProcessingTaskProtocol) {
        expired = true
        logger.info("rediff re-fetch task expired — bailing at next boundary")
        completeTaskOnce(task, success: false)
    }

    /// Idempotent `setTaskCompleted` (first caller wins).
    private func completeTaskOnce(_ task: any BackgroundProcessingTaskProtocol, success: Bool) {
        guard !taskCompleted else { return }
        taskCompleted = true
        task.setTaskCompleted(success: success)
    }
}

// MARK: - UncheckedSendable helper

/// Asserts the wrapped BG task crosses the expiration-callback isolation
/// boundary exactly once and is touched on only one actor thereafter (mirrors
/// the same-named helper in the sibling BGTask services).
private struct _UncheckedSendableBox<T>: @unchecked Sendable {
    let value: T
    init(_ value: T) { self.value = value }
}
