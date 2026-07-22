// EpisodePreparationCoordinator.swift
// playhead-3xtw: user-intent "prepare this episode" trigger.
//
// Layer 1 of the "Download & Analyze on demand" feature. When the user
// taps the prepare control the coordinator (a) enqueues the audio
// download (if not cached), respecting the `cellularPolicy` setting, and
// (b) enqueues the FULL analysis pipeline at a USER-INTENT lane so
// ad-marks are ready before first listen. It NEVER starts playback — it
// reuses the existing download → `AnalysisWorkScheduler` machinery rather
// than the playback path (`PlayheadRuntime.playEpisode` /
// `AnalysisCoordinator.handlePlaybackEvent(.playStarted)`), which is what
// couples analysis to `playbackService.play()`.
//
// The coordinator talks only to narrow, injectable seams so the trigger
// behaviour (enqueues at the user-intent lane · idempotent · respects
// cellularPolicy · does NOT start playback) is unit-testable without a
// live DownloadManager / scheduler.

import Foundation

// MARK: - Seams

/// Download side of the prepare action. Wraps the parts of
/// `DownloadManager` the coordinator needs.
protocol EpisodePreparationDownloads: Sendable {
    /// Whether the full audio file is already cached on disk.
    func isCached(episodeId: String) async -> Bool
    /// The strong (full-file SHA-256) fingerprint for a cached episode,
    /// computing it from the cached file if the in-memory cache misses
    /// (e.g. after a cold launch — the fingerprint cache is only populated
    /// during a download in the current session). Returns `nil` only when
    /// the file is genuinely unavailable / unhashable. `audioURL` seeds the
    /// weak-fingerprint synthesis on a recompute. Used to enqueue analysis
    /// directly for an already-downloaded episode.
    func strongFingerprint(episodeId: String, audioURL: URL) async -> String?
    /// Start a user-triggered download for the episode. Idempotent — the
    /// underlying manager skips episodes that are already cached / in
    /// flight.
    func startDownload(episodeId: String, from url: URL) async
}

/// Analysis side of the prepare action. Wraps the user-intent enqueue
/// surface of `AnalysisWorkScheduler`.
protocol EpisodePreparationAnalysis: Sendable {
    /// Record that the user has expressed intent to analyze this episode,
    /// so the NEXT analysis enqueue for it (e.g. the one the download
    /// completion fires) lands at the user-intent lane. Used when the
    /// audio is not yet downloaded, so no fingerprint / job exists to
    /// enqueue against yet.
    func markUserIntent(episodeId: String, desiredCoverageSec: Double?) async
    /// Enqueue the full analysis pipeline for an already-downloaded
    /// episode at the user-intent lane. Idempotent (the scheduler dedups
    /// by work key).
    func enqueueUserIntent(
        episodeId: String,
        podcastId: String?,
        sourceFingerprint: String,
        desiredCoverageSec: Double?,
        podcastTitle: String?,
        episodeTitle: String?
    ) async
}

// MARK: - Coordinator

/// Immutable, `Sendable` trigger. One entry point: `prepare(_:)`.
struct EpisodePreparationCoordinator: Sendable {

    /// Descriptor of the episode to prepare. A plain value so the
    /// coordinator never touches SwiftData (`Episode` is not `Sendable`);
    /// the caller reads these fields on the main actor.
    struct Request: Sendable, Equatable {
        let episodeId: String
        let podcastId: String?
        let audioURL: URL
        let durationSec: Double?
        let podcastTitle: String?
        let episodeTitle: String?

        init(
            episodeId: String,
            podcastId: String?,
            audioURL: URL,
            durationSec: Double?,
            podcastTitle: String? = nil,
            episodeTitle: String? = nil
        ) {
            self.episodeId = episodeId
            self.podcastId = podcastId
            self.audioURL = audioURL
            self.durationSec = durationSec
            self.podcastTitle = podcastTitle
            self.episodeTitle = episodeTitle
        }
    }

    /// What `prepare(_:)` actually did — returned so the (non-testable)
    /// SwiftUI control can update its optimistic state precisely (e.g. only
    /// show the download bar when a transfer was really started, and show
    /// the Wi‑Fi-wait glyph only when the cellular gate blocked it). Also
    /// gives the coordinator tests a direct assertion surface.
    enum Outcome: Equatable, Sendable {
        /// Already cached → full analysis enqueued at the user-intent lane.
        case enqueuedAnalysis
        /// Cached but the fingerprint could not be resolved — user intent
        /// recorded so a later observation stamps the user-intent lane.
        case markedIntentOnly
        /// A download was started (and user intent recorded) at user request.
        case startedDownload
        /// A download is required but the cellular policy blocked it on a
        /// metered link — nothing was started.
        case waitingForWifi
    }

    let downloads: any EpisodePreparationDownloads
    let analysis: any EpisodePreparationAnalysis
    /// Reachability seam — reuses the existing `TransportStatusProviding`
    /// (`WifiTransportStatusProvider` for tests/previews,
    /// `LiveTransportStatusProvider` in production).
    let reachability: any TransportStatusProviding
    /// Reads the current `SettingsL274.downloads.cellularPolicy`. Injected
    /// so tests drive the policy without touching UserDefaults.
    let cellularPolicy: @Sendable () -> CellularPolicy

    init(
        downloads: any EpisodePreparationDownloads,
        analysis: any EpisodePreparationAnalysis,
        reachability: any TransportStatusProviding,
        cellularPolicy: @escaping @Sendable () -> CellularPolicy = {
            DownloadsSettings.load().cellularPolicy
        }
    ) {
        self.downloads = downloads
        self.analysis = analysis
        self.reachability = reachability
        self.cellularPolicy = cellularPolicy
    }

    /// Prepare the episode for playback-free analysis. Idempotent and
    /// playback-free by construction (there is no playback seam).
    ///
    ///   * Already cached → enqueue full analysis at the user-intent lane.
    ///   * Not cached, download permitted → record user intent, then start
    ///     the download. Its completion enqueues analysis at the
    ///     user-intent lane (the recorded intent stamps the priority).
    ///   * Not cached, download blocked by `cellularPolicy` on a metered
    ///     link → do nothing. The control derives `.waitingForWifi`.
    @discardableResult
    func prepare(_ request: Request) async -> Outcome {
        if await downloads.isCached(episodeId: request.episodeId) {
            if let fingerprint = await downloads.strongFingerprint(
                episodeId: request.episodeId,
                audioURL: request.audioURL
            ) {
                await analysis.enqueueUserIntent(
                    episodeId: request.episodeId,
                    podcastId: request.podcastId,
                    sourceFingerprint: fingerprint,
                    desiredCoverageSec: request.durationSec,
                    podcastTitle: request.podcastTitle,
                    episodeTitle: request.episodeTitle
                )
                return .enqueuedAnalysis
            }
            // Cached but no strong fingerprint resolvable (unhashable file)
            // — record intent so the next observation stamps user-intent
            // priority rather than blocking indefinitely here.
            await analysis.markUserIntent(
                episodeId: request.episodeId,
                desiredCoverageSec: request.durationSec
            )
            return .markedIntentOnly
        }

        // A download is required. Honor the cellular policy.
        let reach = await reachability.currentReachability()
        guard episodePreparationDownloadPermitted(
            reachability: reach,
            policy: cellularPolicy()
        ) else {
            // Blocked — do NOT download. The control renders `.waitingForWifi`.
            return .waitingForWifi
        }

        // Record intent BEFORE starting the download so the analysis
        // enqueue fired by the download completion inherits user-intent
        // priority, then start the user-triggered download.
        await analysis.markUserIntent(
            episodeId: request.episodeId,
            desiredCoverageSec: request.durationSec
        )
        await downloads.startDownload(episodeId: request.episodeId, from: request.audioURL)
        return .startedDownload
    }

    /// Whether a NEW download may proceed right now, given live
    /// reachability and the current `cellularPolicy`. The control reads
    /// this to render the `.waitingForWifi` variant without duplicating
    /// the gate logic. (An already-cached episode ignores this — analysis
    /// is on-device.)
    func currentDownloadPermission() async -> Bool {
        episodePreparationDownloadPermitted(
            reachability: await reachability.currentReachability(),
            policy: cellularPolicy()
        )
    }
}

// MARK: - Production adapters

/// Adapts `DownloadManager` to `EpisodePreparationDownloads`. Kept as a
/// thin wrapper (rather than a direct conformance on the actor) so the
/// feature's protocol surface doesn't leak into the download manager.
struct DownloadManagerPreparationAdapter: EpisodePreparationDownloads {
    let manager: DownloadManager

    func isCached(episodeId: String) async -> Bool {
        await manager.isCached(episodeId: episodeId)
    }

    func strongFingerprint(episodeId: String, audioURL: URL) async -> String? {
        if let cached = await manager.fingerprint(for: episodeId)?.strong {
            return cached
        }
        // Cold-launch path: the in-memory fingerprint cache is only
        // populated during a download in the current session, so a
        // downloaded-yesterday episode has an empty cache. Recompute the
        // strong SHA from the cached file so the "downloaded, tap to
        // analyze today" flow actually enqueues analysis (playhead-3xtw).
        return try? await manager.computeStrongFingerprint(
            episodeId: episodeId, url: audioURL
        )?.strong
    }

    func startDownload(episodeId: String, from url: URL) async {
        await manager.backgroundDownload(episodeId: episodeId, from: url)
    }
}

/// Adapts `AnalysisWorkScheduler` to `EpisodePreparationAnalysis`.
struct SchedulerPreparationAdapter: EpisodePreparationAnalysis {
    let scheduler: AnalysisWorkScheduler

    func markUserIntent(episodeId: String, desiredCoverageSec: Double?) async {
        await scheduler.markEpisodeUserIntent(
            episodeId: episodeId,
            desiredCoverageSec: desiredCoverageSec
        )
    }

    func enqueueUserIntent(
        episodeId: String,
        podcastId: String?,
        sourceFingerprint: String,
        desiredCoverageSec: Double?,
        podcastTitle: String?,
        episodeTitle: String?
    ) async {
        await scheduler.enqueueUserIntentAnalysis(
            episodeId: episodeId,
            podcastId: podcastId,
            sourceFingerprint: sourceFingerprint,
            desiredCoverageSec: desiredCoverageSec,
            podcastTitle: podcastTitle,
            episodeTitle: episodeTitle
        )
    }
}
