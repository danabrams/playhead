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
//
// playhead-pz32 — one read model, not a hand-copied mirror. This file used to
// carry a comment claiming its coverage arithmetic mirrored the Activity
// screen's "so the two surfaces cannot drift". It had already drifted:
// playhead-sd71 moved Activity onto the gap-aware analyzed AREA while this
// file still recomputed `max(featureCoverageEndTime, confirmedAdCoverageEndTime)`
// inline. Both surfaces now read the SAME `AnalysisCoverageSummary`
// (`AnalysisStore.fetchCoverageSummariesByAssetIds`), so a copied formula can
// no longer go stale. They still display different SCALARS of that one model,
// deliberately and for different questions:
//   * Activity's AN bar shows `analysisCoveredSec` — "how much transcribed
//     audio lies at or before the analysis frontier" (pipeline progress).
//   * This control's readiness ✓ and analyze zone show `adScanFraction` —
//     "how much audio was actually read for ads" (the user-facing promise).
// AN is the looser of the two, so it can legitimately read higher than the
// ad-scan fraction for the same episode. What may never happen again is the
// ✓ resolving from a quantity that is not ad-scan coverage.

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
        var analysisTerminatedComplete = false
        var analysisFailed = false
        var adScanFraction: Double?
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
        // playhead-pz32: `.partiallyAnalyzed` is informational, not
        // actionable. Tapping would route into
        // `prepareEpisodeForAnalysis`, and whether re-driving a
        // session that already reached a completion terminal does
        // anything is a PIPELINE question (playhead-gqx4 /
        // playhead-i7qe own coverage). Offering a tap that may
        // silently no-op would be a second dishonest affordance.
        case .downloading, .analyzing, .partiallyAnalyzed, .ready: return false
        }
    }

    // MARK: - Batch refresh (one set of queries for all rows)

    func refresh(episodeIds: [String]) async {
        guard !episodeIds.isEmpty else { return }
        let downloadManager = runtime.downloadManager
        let store = runtime.analysisStore
        let ids = Set(episodeIds)

        // One store query for all episodes' latest assets, one coverage-summary
        // batch, one download snapshot, one cached-id scan, one permission read.
        let assets = (try? await store.fetchLatestAssetByEpisodeIdMap()) ?? [:]
        // playhead-pz32: the ad-scan coverage the readiness predicate keys on
        // comes from the SAME `AnalysisCoverageSummary` read model the Activity
        // screen consumes — one batched query for every visible row, and one
        // definition of every coverage quantity for both surfaces.
        let coverageAssetIds = Set(episodeIds.compactMap { assets[$0]?.id })
        let summaries = (try? await store.fetchCoverageSummariesByAssetIds(coverageAssetIds)) ?? [:]
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
                // The RAW column, not the projection: `PersistedStatus` folds
                // all four completion terminals into `.done` and so cannot
                // tell a degraded terminal from a full one.
                let terminal = episodePreparationTerminalCompletion(
                    analysisState: asset.analysisState
                )
                r.analysisActive = episodePreparationAnalysisActive(status: status)
                r.adScanFraction = summaries[asset.id]?.adScanFraction
                r.analysisComplete = episodePreparationAnalysisComplete(
                    status: status,
                    adScanFraction: r.adScanFraction,
                    isDegradedTerminal: terminal?.isDegradedTerminalCompletion ?? false
                )
                r.analysisTerminatedComplete = (terminal != nil)
                r.analysisFailed = (status == .failed || status == .cancelled)
            } else {
                r.analysisActive = false
                r.analysisComplete = false
                r.analysisTerminatedComplete = false
                r.analysisFailed = false
                r.adScanFraction = nil
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
                analysisTerminatedComplete: r.analysisTerminatedComplete,
                analysisFailed: r.analysisFailed,
                adScanFraction: r.adScanFraction,
                userInitiated: userInitiated.contains(id),
                downloadPermitted: r.downloadPermitted
            )
        )
    }
}
