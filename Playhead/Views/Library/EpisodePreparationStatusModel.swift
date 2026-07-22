// EpisodePreparationStatusModel.swift
// playhead-3xtw: shared, list-level snapshot for the per-episode
// "Download & Analyze on demand" controls.
//
// Layer 3 support. Rather than each visible row independently querying the
// analysis store + download manager + settings on every refresh tick
// (O(rows) SQLite lookups + UserDefaults decodes per tick), the episode
// list owns ONE of these models. It batch-reads all inputs once per
// refresh, runs the PURE `deriveEpisodePreparationReadiness` per episode,
// and hands each row a ready-made value. A single download-progress
// subscription updates the affected episode in place. All decisions still
// live in the pure derivation + the coordinator; this model is thin
// input-gathering glue.

import Foundation

@MainActor
@Observable
final class EpisodePreparationStatusModel {

    private let runtime: PlayheadRuntime

    /// Derived, render-ready state per episode key. `@Observable` publishes
    /// changes so rows re-render when their episode's readiness moves.
    private(set) var readinessByEpisode: [String: EpisodePreparationReadiness] = [:]

    /// Cached raw inputs per episode so a single download-progress tick can
    /// re-derive one episode without touching the store.
    private struct Raw {
        var isDownloaded = false
        var analysisActive = false
        var analysisComplete = false
        var analysisFailed = false
        var analysisFraction: Double?
        var downloadPermitted = true
        var snapshotDownloadFraction: Double?
        var liveDownloadFraction: Double?
    }
    private var raw: [String: Raw] = [:]

    /// Per-episode intent latches (user tapped) and the optimistic
    /// "download just kicked" bridge — see `EpisodePreparationControl`'s
    /// former single-row equivalents, now centralized.
    private var userInitiated: Set<String> = []
    private var downloadKicked: Set<String> = []

    init(runtime: PlayheadRuntime) {
        self.runtime = runtime
    }

    // MARK: - Read

    /// Render-ready state for a row. Unknown episodes read as resting idle.
    func readiness(for episodeId: String) -> EpisodePreparationReadiness {
        readinessByEpisode[episodeId]
            ?? EpisodePreparationReadiness(state: .idle, downloadFraction: 0, analysisFraction: 0)
    }

    /// Whether tapping the control is meaningful (only the resting/blocked
    /// states are actionable — mirrors the pure state machine).
    func isActionable(for episodeId: String) -> Bool {
        switch readiness(for: episodeId).state {
        case .idle, .waitingForWifi: return true
        case .downloading, .analyzing, .ready: return false
        }
    }

    // MARK: - Batch refresh (one set of queries for all rows)

    func refresh(episodeIds: [String]) async {
        guard !episodeIds.isEmpty else { return }
        let downloadManager = runtime.downloadManager
        let store = runtime.analysisStore
        let ids = Set(episodeIds)

        // One store query for all episodes' latest assets, one download
        // snapshot, one cached-id scan, one permission read.
        let assets = (try? await store.fetchLatestAssetByEpisodeIdMap()) ?? [:]
        let snapshot = await downloadManager.progressSnapshot()
        let cachedIds = await downloadManager.cachedEpisodeIds(matching: ids)
        let permitted = await runtime.episodePreparationCoordinator.currentDownloadPermission()

        for id in episodeIds {
            var r = raw[id] ?? Raw()
            r.isDownloaded = cachedIds.contains(id)
            r.snapshotDownloadFraction = snapshot[id]
            r.downloadPermitted = permitted
            if let asset = assets[id] {
                let status = EpisodeSurfaceStatusObserver.analysisState(from: asset).persistedStatus
                r.analysisActive = episodePreparationAnalysisActive(status: status)
                r.analysisFraction = Self.analysisFraction(from: asset)
                r.analysisComplete = episodePreparationAnalysisComplete(
                    status: status, analysisFraction: r.analysisFraction
                )
                r.analysisFailed = (status == .failed || status == .cancelled)
            } else {
                r.analysisActive = false
                r.analysisComplete = false
                r.analysisFailed = false
                r.analysisFraction = nil
            }
            // Drop the optimistic download bridge once the real in-flight /
            // cached signal is present, so a transfer that never started
            // cannot strand the bar.
            if r.isDownloaded || r.snapshotDownloadFraction != nil || r.liveDownloadFraction != nil {
                downloadKicked.remove(id)
            }
            raw[id] = r
            derive(id)
        }
    }

    // MARK: - Live download progress (single subscription)

    /// Subscribe once to the download manager's progress stream and update
    /// the affected episode in place. Cancelled with the owning `.task`.
    func observeDownloadProgress() async {
        let stream = await runtime.downloadManager.progressUpdates()
        for await progress in stream {
            if Task.isCancelled { return }
            let id = progress.episodeId
            guard var r = raw[id] else { continue } // only rows we track
            r.liveDownloadFraction = progress.fractionCompleted
            raw[id] = r
            if progress.totalBytes > 0, progress.bytesWritten >= progress.totalBytes {
                // Completed — recheck cache + analysis state for this one.
                await refresh(episodeIds: [id])
            } else {
                derive(id)
            }
        }
    }

    // MARK: - Trigger (playback-free)

    /// User tapped a row's control. Records intent, invokes the
    /// playback-free coordinator, and refreshes that episode. NEVER starts
    /// playback.
    func prepare(_ episode: Episode) async {
        let id = episode.canonicalEpisodeKey
        guard isActionable(for: id) else { return }
        userInitiated.insert(id)
        derive(id)
        let outcome = await runtime.prepareEpisodeForAnalysis(episode)
        if outcome == .startedDownload {
            downloadKicked.insert(id)
        }
        await refresh(episodeIds: [id])
    }

    // MARK: - Private

    private func derive(_ id: String) {
        guard let r = raw[id] else { return }
        let inFlight = !r.isDownloaded
            && (r.snapshotDownloadFraction != nil
                || r.liveDownloadFraction != nil
                || downloadKicked.contains(id))
        let downloadFraction = r.liveDownloadFraction
            ?? r.snapshotDownloadFraction
            ?? (r.isDownloaded ? 1 : nil)
        readinessByEpisode[id] = deriveEpisodePreparationReadiness(
            EpisodePreparationInputs(
                isDownloaded: r.isDownloaded,
                downloadInFlight: inFlight,
                downloadFraction: downloadFraction,
                analysisActive: r.analysisActive,
                analysisComplete: r.analysisComplete,
                analysisFailed: r.analysisFailed,
                analysisFraction: r.analysisFraction,
                userInitiated: userInitiated.contains(id),
                downloadPermitted: r.downloadPermitted
            )
        )
    }

    /// Coverage watermark / duration, clamped to `[0, 1]`; `nil` when the
    /// watermark or a positive duration is unknown. Mirrors the Activity
    /// screen's `max(featureCoverageEndSec, confirmedAdCoverageEndSec)`
    /// derivation so the two surfaces cannot drift.
    private static func analysisFraction(from asset: AnalysisAsset) -> Double? {
        let watermark: Double?
        switch (asset.featureCoverageEndTime, asset.confirmedAdCoverageEndTime) {
        case let (f?, a?): watermark = max(f, a)
        case let (f?, nil): watermark = f
        case let (nil, a?): watermark = a
        case (nil, nil): watermark = nil
        }
        guard let watermark, let duration = asset.episodeDurationSec, duration > 0 else {
            return nil
        }
        return min(1, max(0, watermark / duration))
    }
}
