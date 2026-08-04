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
// NEITHER dominates the other: AN can exceed the ad-scan fraction (the frontier
// has passed audio the scan has not reached, the usual case), and the ad-scan
// fraction can exceed AN (a scan window read text lying past the feature/ad
// frontier). Nor is either a strict subset of the transcript union — AN is, but
// the ad-scan area bridges sub-ad-width transcript gaps
// (`AnalysisCoverageMath.adScanBridgeableGapSec`) and so can exceed the raw union
// by those bridged seconds, bounded by the transcript's outer span. So the two
// surfaces can legitimately show different numbers for one episode; do not
// "reconcile" them by making one read the other. What may never happen again is
// the ✓ resolving from a quantity that is not ad-scan coverage.

import Foundation
import OSLog

@MainActor
@Observable
final class EpisodePreparationStatusModel {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "EpisodePreparationStatus"
    )

    private let runtime: PlayheadRuntime

    /// Derived, render-ready state per episode key. `@Observable` publishes
    /// changes so rows re-render when their episode's readiness moves.
    private(set) var readinessByEpisode: [String: EpisodePreparationReadiness] = [:]

    /// Cached raw inputs per episode so a single download-progress tick can
    /// re-derive one episode without touching the store.
    private struct Raw {
        var isDownloaded = false
        /// playhead-pz32: the analysis half, projected by the pure
        /// `episodePreparationAnalysisInputs` from the asset row + its coverage
        /// summary. Held as one value so this model cannot re-derive (or
        /// half-derive) the readiness quantities itself.
        var analysis = EpisodePreparationAnalysisInputs()
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
        // actionable, and that is a measured conclusion rather than
        // caution. A tap routes to `prepareEpisodeForAnalysis` →
        // `AnalysisWorkScheduler.enqueue`, which computes the SAME
        // `workKey` the finished job already owns and inserts with
        // `INSERT OR IGNORE`; only `queued`/`paused`/retryable-`failed`
        // rows dispatch, and a `complete` row is GC'd only after 7
        // days. So the tap would re-drive nothing. An enabled button
        // that provably does nothing is a second dishonest affordance
        // — worse than an honest inert glyph. Re-drive belongs to
        // playhead-gqx4 / playhead-i7qe, which own coverage.
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
        // screen consumes — one definition of every coverage quantity for both
        // surfaces, so a copied formula can no longer go stale.
        //
        // COST, stated honestly: this is new work on this path (the pre-pz32
        // fraction came free off the already-fetched asset row) and
        // `fetchCoverageSummariesByAssetIds` runs four prepared statements per
        // 500-id chunk, including every transcript chunk of BOTH passes for the
        // assets in scope (playhead-9y9e made the final-pass query project rows
        // rather than `MAX(endTime)`, because the ad-scan bound needs its
        // intervals; on the 2026-08-03 device pull that is 8,251 extra rows
        // across twelve assets, +28 % on the 29,247 fast rows already read).
        // It is bounded by the number of episodes that have an
        // `analysis_assets` row — NOT by the list length — which is why the id
        // set is built by `compactMap`ping the assets rather than from
        // `episodeIds`. Most library rows have never been analysed and cost
        // nothing here. If that ever stops being true (a user with thousands of
        // analysed episodes on screen), the fix is to narrow the fetch to the
        // visible window, not to go back to reading a cheaper wrong number.
        let coverageAssetIds = Set(episodeIds.compactMap { assets[$0]?.id })
        // A THROW is not "coverage unknown". `AnalysisStore` is an actor also
        // driven by the pipeline, so one `SQLITE_BUSY` empties the whole batch —
        // and because this is a single call for the entire list, treating that as
        // unknown would flip EVERY completed row at once, until the next
        // `ActivityRefreshNotification` (posted only on job start/finish, so
        // possibly not for a long time on an idle device). So a failed read leaves
        // the previous analysis inputs in place and says so in the log, rather
        // than repainting the library off a transport error.
        let summaries: [String: AnalysisCoverageSummary]?
        do {
            summaries = try await store.fetchCoverageSummariesByAssetIds(coverageAssetIds)
        } catch {
            summaries = nil
            Self.logger.warning(
                "EpisodePreparationStatusModel: coverage read failed for \(coverageAssetIds.count, privacy: .public) asset(s); keeping prior readiness: \(error.localizedDescription, privacy: .public)"
            )
        }
        let snapshot = await downloadManager.progressSnapshot()
        let cachedIds = await downloadManager.cachedEpisodeIds(matching: ids)
        let permitted = await runtime.episodePreparationCoordinator.currentDownloadPermission()

        for id in episodeIds {
            var r = raw[id] ?? Raw()
            r.isDownloaded = cachedIds.contains(id)
            r.snapshotDownloadFraction = snapshot[id]
            r.downloadPermitted = permitted
            if let summaries {
                let asset = assets[id]
                r.analysis = episodePreparationAnalysisInputs(
                    asset: asset,
                    coverage: asset.flatMap { summaries[$0.id] }
                )
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
                analysisActive: r.analysis.analysisActive,
                analysisComplete: r.analysis.analysisComplete,
                analysisTerminatedComplete: r.analysis.analysisTerminatedComplete,
                analysisFailed: r.analysis.analysisFailed,
                adScanFraction: r.analysis.adScanFraction,
                userInitiated: userInitiated.contains(id),
                downloadPermitted: r.downloadPermitted
            )
        )
    }
}
