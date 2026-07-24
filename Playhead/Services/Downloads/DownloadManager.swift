// DownloadManager.swift
// Background download management for podcast episode audio.
// Handles progressive caching during streaming playback, background
// transfers for pre-caching, resume after interruption, LRU eviction,
// and asset fingerprinting for the analysis pipeline.

import BackgroundTasks
import CryptoKit
import Foundation
import OSLog
import UIKit
// MARK: - Download State Events

/// Progress and completion events for a single episode download.
struct DownloadProgress: Sendable {
    let episodeId: String
    let bytesWritten: Int64
    let totalBytes: Int64
    var fractionCompleted: Double {
        totalBytes > 0 ? Double(bytesWritten) / Double(totalBytes) : 0
    }
}

/// A chunk of raw compressed audio data from a streaming download.
struct AudioDataChunk: Sendable {
    let episodeId: String
    let data: Data
    /// Total bytes written so far (including this chunk).
    let totalBytesWritten: Int64
}

/// Metadata harvested from HTTP response headers for fingerprinting.
struct HTTPAssetMetadata: Sendable, Equatable {
    let etag: String?
    let contentLength: Int64?
    let lastModified: String?
}

// MARK: - AudioFingerprint

/// Identifies an audio asset across re-downloads and URL changes.
/// Weak fingerprint: enclosure URL + HTTP metadata (available early).
/// Strong fingerprint: full content SHA-256 hash (available after download).
struct AudioFingerprint: Sendable, Equatable {
    /// Enclosure URL + ETag + Content-Length + Last-Modified.
    let weak: String
    /// SHA-256 of full file contents (nil until download completes).
    let strong: String?

    /// Build weak fingerprint from URL and HTTP metadata.
    static func makeWeak(url: URL, metadata: HTTPAssetMetadata) -> String {
        let etag = metadata.etag ?? ""
        let length = metadata.contentLength.map(String.init) ?? ""
        let modified = metadata.lastModified ?? ""
        return "\(url.absoluteString)|\(etag)|\(length)|\(modified)"
    }
}

// MARK: - CacheEntry

/// Tracks a cached audio file on disk for LRU eviction decisions.
struct CacheEntry: Sendable {
    let episodeId: String
    let fileURL: URL
    let fileSize: Int64
    let lastAccessedAt: Date
    let isFullyDownloaded: Bool
    let hasActiveAnalysis: Bool
}

// MARK: - AudioAssetPin

/// playhead-wrj8: the immutable-artifact "content pin" persisted next to a
/// downloaded episode's audio file (`<hash>.pin`). Its presence + the
/// `expectedBytes` field are what let the cache distinguish a COMPLETE,
/// serveable artifact from a truncated / mid-stream / interrupted file — an
/// existence-only check cannot.
///
/// Invariant: for the life of a downloaded episode the pinned artifact is
/// immutable. Once a pin exists whose `expectedBytes` matches the on-disk
/// length, no non-rediff path may overwrite that file in place — the bytes
/// PLAYED == ANALYZED == MARKED-AGAINST never change (DAI shows re-cut a
/// different ad stitch on every fetch, so silently re-fetching would rotate
/// the audio the user marked ads against).
///
/// A `nil` pin (legacy files downloaded before wrj8) is treated as
/// complete-by-existence so the change is non-destructive; freshly
/// downloaded/streamed files always write a pin.
struct AudioAssetPin: Codable, Sendable, Equatable {
    /// Authoritative complete byte length. During a streaming download this
    /// is seeded to the HTTP `Content-Length` (or `Int64.max` when unknown)
    /// so the growing file reads as INCOMPLETE until finalized; on
    /// completion it is rewritten to the actual on-disk size.
    var expectedBytes: Int64
    /// Full-file SHA-256, populated once the download completes. Optional
    /// because it is not known until the bytes are all on disk.
    var sha256: String?
    /// Enclosure URL the bytes were fetched from (diagnostics only).
    var sourceURL: String?
    /// HTTP validator captured at download time (diagnostics only).
    var etag: String?
}

// MARK: - DownloadManagerError

enum DownloadManagerError: Error, CustomStringConvertible {
    case downloadFailed(String, String)
    case fileNotFound(String)
    case checksumMismatch(expected: String, actual: String)
    case insufficientDiskSpace(needed: Int64, available: Int64)
    case alreadyDownloading(String)
    case cancelled

    var description: String {
        switch self {
        case .downloadFailed(let id, let reason):
            "Download failed for episode '\(id)': \(reason)"
        case .fileNotFound(let path):
            "Cached file not found: \(path)"
        case .checksumMismatch(let expected, let actual):
            "Content hash mismatch: expected \(expected), got \(actual)"
        case .insufficientDiskSpace(let needed, let available):
            "Insufficient disk space: need \(needed) bytes, have \(available)"
        case .alreadyDownloading(let id):
            "Already downloading episode '\(id)'"
        case .cancelled:
            "Download cancelled"
        }
    }
}

// MARK: - DownloadContext

/// Metadata passed by the caller to connect a download to the analysis pipeline.
///
/// playhead-i9dj: `podcastTitle` and `episodeTitle` carry the human-readable
/// identifiers from the SwiftData `Podcast`/`Episode` so the AnalysisStore can
/// persist them at first observation. Both fields are optional — callers that
/// don't have the SwiftData side in scope (e.g. background-session completion
/// routes) leave them `nil`, and the AnalysisStore reconciles titles lazily on
/// the next call site that does supply them.
struct DownloadContext: Sendable {
    let podcastId: String?
    let isExplicitDownload: Bool
    let podcastTitle: String?
    let episodeTitle: String?

    init(
        podcastId: String?,
        isExplicitDownload: Bool,
        podcastTitle: String? = nil,
        episodeTitle: String? = nil
    ) {
        self.podcastId = podcastId
        self.isExplicitDownload = isExplicitDownload
        self.podcastTitle = podcastTitle
        self.episodeTitle = episodeTitle
    }
}

// MARK: - DownloadProviding

/// Protocol abstraction for download queries, enabling test stubs.
protocol DownloadProviding: Sendable {
    func cachedFileURL(for episodeId: String) async -> URL?
    func fingerprint(for episodeId: String) async -> AudioFingerprint?
    func allCachedEpisodeIds() async -> Set<String>
}

// MARK: - DownloadManager

/// Manages background downloads and progressive caching for podcast
/// episode audio.
///
/// Responsibilities:
/// - Progressive download: cache audio as it streams for playback
/// - Background URLSession transfers for full episode pre-caching
/// - Asset fingerprinting from HTTP metadata + content hash
/// - LRU eviction with configurable max cache size
/// - Integrity verification on cached files
actor DownloadManager {
    private let logger = Logger(subsystem: "com.playhead", category: "Downloads")

    // MARK: - Configuration

    /// Default max cache size: 2 GB.
    static let defaultMaxCacheBytes: Int64 = 2 * 1024 * 1024 * 1024

    /// Configurable max cache size in bytes.
    private var maxCacheBytes: Int64

    // MARK: - Directories

    /// Root cache directory for episode audio.
    nonisolated let cacheDirectory: URL

    /// Subdirectory for partial/in-progress downloads.
    nonisolated let partialsDirectory: URL

    /// Subdirectory for fully downloaded and verified audio.
    nonisolated let completeDirectory: URL

    /// Subdirectory for URLSession resume-data blobs persisted by
    /// `scanForSuspendedTransfers()` (playhead-hyht). One file per
    /// episode keyed by `safeFilename(for: episodeId)`. Each file's
    /// body is the opaque OS resume-data blob returned from
    /// `URLSessionDownloadTask.cancel(byProducingResumeData:)`.
    nonisolated let resumeDataDirectory: URL

    /// In-memory set of episode IDs the most recent scan reported as
    /// having a persisted resume-data blob. Populated by
    /// `scanForSuspendedTransfers()` and used by the idempotence guard
    /// so a second scan pass does not re-emit preempted events for the
    /// same suspended transfers. Cleared when the transfer is resumed
    /// (blob consumed) or the blob is pruned as corrupted.
    ///
    /// Visibility is `internal` (not `private`) so the scan/resume
    /// extension in `ForceQuitResumeScan.swift` can mutate it — the
    /// actor isolation keeps reads/writes ordered.
    internal var reportedSuspendedTransfers: Set<String> = []

    // MARK: - State

    /// Active download tasks keyed by episode ID.
    private var activeDownloads: [String: Task<URL, Error>] = [:]

    /// playhead-44h1 (fix): last observed progress for each active
    /// download, used to build a ``ForegroundAssistTransferSnapshot``
    /// on `UIApplication.willResignActiveNotification`. Populated from
    /// every `DownloadProgress` broadcast via ``noteTransferProgress``;
    /// cleared when the download completes (on broadcast with
    /// `bytesWritten == totalBytes`) and on explicit cancellation.
    struct ForegroundAssistProgress: Sendable, Equatable {
        let bytesWritten: Int64
        let totalBytes: Int64
        let firstObservedAt: Date
        let firstObservedBytes: Int64
        let updatedAt: Date
    }
    private var foregroundAssistProgress: [String: ForegroundAssistProgress] = [:]

    /// playhead-44h1 (fix): scheduler used to submit
    /// `BGContinuedProcessingTaskRequest` when ``handleWillResignActive``
    /// decides the handoff to a BG task is the right call. Defaults to
    /// the real `BGTaskScheduler.shared`; swapped in tests via
    /// ``setBackgroundTaskSchedulerForTesting(_:)``. Using the same
    /// `BackgroundTaskScheduling` abstraction that BPS does so the
    /// testing surface stays consistent.
    private var backgroundTaskScheduler: any BackgroundTaskScheduling = BGTaskScheduler.shared

    /// playhead-wrj8: fetches the CURRENT server validator (ETag /
    /// Content-Length) for a URL, used to decide whether a persisted
    /// resume-data blob is still safe to splice. Defaults to a real HTTP
    /// HEAD request; tests inject a deterministic stub via
    /// ``setResumeValidatorProviderForTesting(_:)``. Returns `nil` when the
    /// validator cannot be established (treated as "cannot prove freshness"
    /// → re-download fresh rather than risk splicing a rotated stitch).
    private var resumeValidatorProvider: (@Sendable (URL) async -> HTTPAssetMetadata?)?

    /// Metadata cache: episode ID -> HTTP metadata from last response.
    private var metadataCache: [String: HTTPAssetMetadata] = [:]

    /// LRU tracking: episode ID -> last access time.
    private var accessLog: [String: Date] = [:]

    /// Episodes with active/incomplete analysis or in-flight playback
    /// (protected from eviction). playhead-wrj8: refcounted (was a plain
    /// `Set`) so overlapping owners — the playback lifecycle and one or
    /// more analysis jobs on the SAME episode — compose correctly: the
    /// file backing the current episode is only eligible for eviction once
    /// EVERY protector has released. A bare `Set` let whichever owner
    /// finished first drop protection out from under the others.
    private var analysisProtectedEpisodes: [String: Int] = [:]

    /// Episode IDs whose background URLSession download is currently
    /// in flight. Background tasks aren't tracked in `activeDownloads`
    /// (foreground only), so this set gives `evictIfNeeded` a way to
    /// protect just-deposited bg files in the small window between
    /// `didFinishDownloadingTo` moving the file into completeDirectory
    /// and `handleBackgroundDownloadComplete` running `touchAccess`.
    private var bgInFlightEpisodes: Set<String> = []

    /// Fingerprint cache: episode ID -> computed fingerprint.
    private var fingerprintCache: [String: AudioFingerprint] = [:]

    /// Cached file extension per episode ID (e.g. "mp3", "m4a").
    private var extensionCache: [String: String] = [:]

    /// Optional scheduler for enqueuing pre-analysis jobs after download.
    private var analysisWorkScheduler: AnalysisWorkScheduler?

    /// playhead-xsdz.71 (Signal 1, ADDITIVE/observational): optional recorder
    /// that receives the enclosure download's redirect-chain hop hosts so the
    /// DAI-stitch classifier can persist a show-level DAI-EXPECTED prior. `nil`
    /// (default, and every test) ⇒ NO redirect-recording delegate is attached
    /// and the download is byte-identical to before. Injected once by
    /// `PlayheadRuntime`. This only OBSERVES — no consumer wiring.
    private var daiStitchRecorder: (any DAIStitchChainRecording)?

    /// Background URL sessions keyed by role. Lazy-instantiated on first
    /// use so tests can construct a `DownloadManager` without spinning
    /// up NSURLSession state for identifiers they don't exercise.
    /// See `BackgroundSessionRole` for the three lanes.
    private var _sessionsByRole: [BackgroundSessionRole: URLSession] = [:]

    /// Feature flag that gates the 24cm dual-session split. Copied from
    /// `PreAnalysisConfig.useDualBackgroundSessions` at init time and
    /// re-exposed via `setUseDualBackgroundSessions(_:)` so tests can
    /// flip it without reaching into UserDefaults.
    private var useDualBackgroundSessions: Bool

    /// Recorder injected by playhead-uzdq (or any test double) to emit
    /// WorkJournal events from the download delegate callbacks. Defaults
    /// to a no-op so 24cm can ship before uzdq lands.
    ///
    /// Visibility is `internal` (not `private`) so the playhead-hyht
    /// force-quit scan extension in `ForceQuitResumeScan.swift` can emit
    /// preempted/failed rows without re-entering DownloadManager.swift.
    internal var workJournalRecorder: WorkJournalRecording

    /// Delegate for background sessions. A single delegate instance
    /// serves all three identifier lanes — the session identifier is
    /// pulled from `session.configuration.identifier` on each callback
    /// if routing needs to differ per-lane.
    private let sessionDelegate: EpisodeDownloadDelegate

    // MARK: - Streams

    private let progressContinuation: AsyncStream<DownloadProgress>.Continuation
    /// Single-consumer stream (legacy). Prefer progressUpdates() for new code.
    nonisolated let progressStream: AsyncStream<DownloadProgress>

    /// Multi-subscriber continuations for download progress.
    private var progressSubscribers: [UUID: AsyncStream<DownloadProgress>.Continuation] = [:]

    /// playhead-3xtw (L2): highest `totalBytesWritten` broadcast for each
    /// in-flight BACKGROUND transfer. The delegate spawns one actor-hop
    /// `Task` per `didWriteData` callback, so a later Task can execute
    /// before an earlier one; this monotonic high-water mark drops the
    /// out-of-order stragglers so the delivered fraction never regresses.
    /// Reset when a new download starts and cleared on completion.
    private var lastBackgroundProgressBytes: [String: Int64] = [:]

    /// Multi-subscriber continuations for raw audio data chunks.
    private var audioDataSubscribers: [UUID: AsyncStream<AudioDataChunk>.Continuation] = [:]

    // MARK: - Init

    init(
        cacheDirectory: URL? = nil,
        maxCacheBytes: Int64 = DownloadManager.defaultMaxCacheBytes,
        preAnalysisConfig: PreAnalysisConfig? = nil,
        workJournalRecorder: WorkJournalRecording = NoopWorkJournalRecorder()
    ) {
        let root = cacheDirectory ?? Self.defaultCacheDirectory()
        self.cacheDirectory = root
        self.partialsDirectory = root.appendingPathComponent("partials", isDirectory: true)
        self.completeDirectory = root.appendingPathComponent("complete", isDirectory: true)
        self.resumeDataDirectory = root.appendingPathComponent("resumeData", isDirectory: true)
        self.maxCacheBytes = maxCacheBytes
        self.sessionDelegate = EpisodeDownloadDelegate()
        let config = preAnalysisConfig ?? PreAnalysisConfig.load()
        self.useDualBackgroundSessions = config.useDualBackgroundSessions
        self.workJournalRecorder = workJournalRecorder

        let (stream, continuation) = AsyncStream<DownloadProgress>.makeStream()
        self.progressStream = stream
        self.progressContinuation = continuation

        // Wire delegate → manager so finalize / failure events route back
        // onto the actor. The delegate owns the URLSession-side closure;
        // we own the state it needs to mutate.
        self.sessionDelegate.workJournal = workJournalRecorder
        self.sessionDelegate.onUrlSessionDidFinishEvents = { identifier in
            Task { @MainActor in
                if let delegate = DownloadManager.appDelegate {
                    delegate.invokePendingBackgroundCompletionHandler(forIdentifier: identifier)
                }
            }
        }
        // playhead-g2wq: route harvested resume-data blobs from the
        // delegate back into the actor's `resumeDataDirectory`. Wired at
        // init (not per-session like onBackgroundDownloadStaged) because
        // the harvest is independent of which background session fired.
        //
        // Retain-cycle note: `[weak manager = self]` avoids the cycle
        // `delegate → closure → manager → sessionDelegate → closure`.
        // `DownloadManager` is long-lived (typically a singleton), so a
        // strong capture would leak forever and defeat `deinit` cleanup.
        //
        // Async Task hop is safe here (Option A): per the hyht bead's
        // force-quit state machine, `didCompleteWithError` does NOT
        // fire at force-quit time. The OS suspends the in-flight task
        // without delivering completion. The callback only fires while
        // the app is alive — either during normal runtime or during
        // background-session rehydration after cold relaunch (see
        // `scanForSuspendedTransfers`). In both cases the process stays
        // alive long enough for the FileManager write inside
        // `persistResumeData` to complete, so the Task hop introduces
        // no loss-of-write risk.
        self.sessionDelegate.onResumeDataHarvested = { [weak manager = self] episodeId, data, sourceURL, metadata in
            guard let manager else { return }
            Task { [manager, episodeId, data, sourceURL, metadata] in
                do {
                    try await manager.persistResumeData(
                        episodeId: episodeId,
                        data: data,
                        sourceURL: sourceURL,
                        validator: metadata
                    )
                } catch {
                    // A write failure here is best-effort; the next
                    // cold-launch scan simply won't see this blob. Log
                    // via the shared resume-data category so support
                    // triage can correlate.
                    Logger(
                        subsystem: "com.playhead", category: "ForceQuitResume"
                    ).error("persistResumeData (harvest) failed for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)")
                }
            }
        }
        // Wire onBackgroundDownloadStaged once at init (not per-session).
        // Body is identical across sessions — only the actor hop varies,
        // and that's keyed off the staged file/metadata, not the
        // session. The prior per-session reassignment in
        // `backgroundSession(for:)` produced needless closure churn
        // under repeated session instantiation (e.g. cold-launch
        // rehydration of multiple identifiers).
        //
        // playhead-24cm.1: the delegate's job is now to stage the
        // OS-provided file into a process-global temp dir; the actor
        // owns the final placement (which honors `cacheDirectory`,
        // including custom test directories) and the synthesis of a
        // real weak fingerprint from URL + HTTP response metadata
        // harvested on the delegate queue.
        //
        // Retain-cycle note: `[weak manager = self]` mirrors
        // `onResumeDataHarvested` above. The cycle would otherwise be
        // `delegate → closure → manager → sessionDelegate → closure`,
        // leaking every `DownloadManager` forever and defeating
        // `deinit` cleanup of the willResignActive observer.
        self.sessionDelegate.onBackgroundDownloadStaged = {
            [weak manager = self] episodeId, stagedURL, originalURL, metadata in
            guard let manager else { return }
            Task { [manager, episodeId, stagedURL, originalURL, metadata] in
                await manager.handleBackgroundDownloadComplete(
                    episodeId: episodeId,
                    stagedURL: stagedURL,
                    originalURL: originalURL,
                    metadata: metadata
                )
            }
        }
        // playhead-3xtw: surface background-transfer byte progress through
        // the same broadcast surface the foreground path uses, so the
        // per-episode prepare control's download zone advances during a
        // background (pre-cache / on-demand) download. Same init-once wiring
        // + `[weak manager]` retain-cycle guard as the staged hook above.
        self.sessionDelegate.onBackgroundDownloadProgress = {
            [weak manager = self] episodeId, bytesWritten, totalBytes in
            guard let manager else { return }
            Task { [manager, episodeId, bytesWritten, totalBytes] in
                await manager.broadcastBackgroundProgress(
                    episodeId: episodeId,
                    bytesWritten: bytesWritten,
                    totalBytes: totalBytes
                )
            }
        }
    }

    /// playhead-3xtw: actor-isolated receiver for background-transfer
    /// progress harvested on the delegate queue. Yields to the live
    /// progress streams (`progressStream` + `progressUpdates()`
    /// subscribers) so the per-episode prepare control's download zone
    /// advances during a background transfer, but deliberately does NOT
    /// route through `noteTransferProgress`: background-session transfers
    /// keep running while the app is suspended, so they must not enroll in
    /// the foreground-assist `BGContinuedProcessingTaskRequest` keep-alive
    /// (that budget is for foreground transfers only). `totalBytes <= 0`
    /// (size unknown) is dropped rather than broadcast as a
    /// divide-by-zero 0% event.
    private func broadcastBackgroundProgress(
        episodeId: String,
        bytesWritten: Int64,
        totalBytes: Int64
    ) {
        guard totalBytes > 0 else { return }
        // playhead-3xtw (L2): drop stale, out-of-order ticks so the
        // delivered fraction is monotonic within a transfer. Cleared on
        // completion here and on a fresh `backgroundDownload` start.
        let highWater = lastBackgroundProgressBytes[episodeId] ?? 0
        guard bytesWritten >= highWater else { return }
        if bytesWritten >= totalBytes {
            lastBackgroundProgressBytes[episodeId] = nil
        } else {
            lastBackgroundProgressBytes[episodeId] = bytesWritten
        }
        let progress = DownloadProgress(
            episodeId: episodeId,
            bytesWritten: bytesWritten,
            totalBytes: totalBytes
        )
        progressContinuation.yield(progress)
        for (_, continuation) in progressSubscribers {
            continuation.yield(progress)
        }
    }

    /// Returns a fresh AsyncStream that receives all future download progress
    /// events. Each caller gets its own stream — multiple subscribers are supported.
    /// The stream ends when the continuation is cancelled or the manager is deallocated.
    func progressUpdates() -> AsyncStream<DownloadProgress> {
        let id = UUID()
        return AsyncStream { continuation in
            self.progressSubscribers[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeProgressSubscriber(id: id)
                }
            }
        }
    }

    private func removeProgressSubscriber(id: UUID) {
        progressSubscribers.removeValue(forKey: id)
    }

    /// Yield progress to both the legacy single-consumer stream and all subscribers.
    private func broadcastProgress(_ progress: DownloadProgress) {
        progressContinuation.yield(progress)
        for (_, continuation) in progressSubscribers {
            continuation.yield(progress)
        }
        noteTransferProgress(progress)
    }

    /// playhead-44h1 (fix): record the latest `DownloadProgress` for
    /// the foreground-assist handoff snapshot. Called on every
    /// `broadcastProgress` and from background-session delegate
    /// hooks so the `willResignActive` handler has up-to-date byte
    /// counters to feed `ForegroundAssistHandoff.decide(for:)`.
    ///
    /// Clears the slot when the transfer has completed
    /// (`bytesWritten >= totalBytes > 0`) so a subsequent background
    /// transition does not spuriously emit a keep-alive for a
    /// finished download.
    func noteTransferProgress(_ progress: DownloadProgress) {
        // Completed transfer: remove the slot so the snapshot does
        // not observe stale bytes after the work is done.
        if progress.totalBytes > 0 && progress.bytesWritten >= progress.totalBytes {
            foregroundAssistProgress.removeValue(forKey: progress.episodeId)
            return
        }
        let now = Date()
        let existing = foregroundAssistProgress[progress.episodeId]
        // Preserve the first-observation timestamp so the throughput
        // estimate spans the full active window, not just the most
        // recent tick. This gives `ForegroundAssistHandoff` a useful
        // `averageBytesPerSecond` even for freshly-started transfers.
        foregroundAssistProgress[progress.episodeId] = ForegroundAssistProgress(
            bytesWritten: progress.bytesWritten,
            totalBytes: progress.totalBytes,
            firstObservedAt: existing?.firstObservedAt ?? now,
            firstObservedBytes: existing?.firstObservedBytes ?? progress.bytesWritten,
            updatedAt: now
        )
    }

    /// Returns a fresh AsyncStream of raw audio data chunks for streaming decode.
    /// Each caller gets its own stream — multiple subscribers supported.
    func audioDataUpdates() -> AsyncStream<AudioDataChunk> {
        let id = UUID()
        return AsyncStream { continuation in
            self.audioDataSubscribers[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeAudioDataSubscriber(id: id)
                }
            }
        }
    }

    private func removeAudioDataSubscriber(id: UUID) {
        audioDataSubscribers.removeValue(forKey: id)
    }

    private func broadcastAudioData(_ chunk: AudioDataChunk) {
        for (_, continuation) in audioDataSubscribers {
            continuation.yield(chunk)
        }
    }

    /// Finish all audio data subscriber streams so `for await` loops exit.
    private func finishAudioDataSubscribers() {
        for (id, continuation) in audioDataSubscribers {
            continuation.finish()
        }
        audioDataSubscribers.removeAll()
    }

    /// Wire up the analysis scheduler so downloads automatically enqueue jobs.
    func setAnalysisWorkScheduler(_ scheduler: AnalysisWorkScheduler) {
        self.analysisWorkScheduler = scheduler
    }

    /// playhead-xsdz.71 (Signal 1): inject the DAI-stitch redirect-chain
    /// recorder. Wired once by `PlayheadRuntime`; left `nil` in tests so the
    /// download path stays byte-identical.
    func setDAIStitchRecorder(_ recorder: any DAIStitchChainRecording) {
        self.daiStitchRecorder = recorder
    }

    /// playhead-xsdz.71 (Signal 1): build a redirect-recording delegate when a
    /// recorder is wired AND we know the show, else `nil` (no delegate → the
    /// download call is byte-identical). Shared by the download + streaming
    /// paths.
    private func makeRedirectRecordingDelegate(
        url: URL,
        context: DownloadContext?
    ) -> RedirectChainRecordingDelegate? {
        guard daiStitchRecorder != nil, context?.podcastId != nil else { return nil }
        return RedirectChainRecordingDelegate(initialHost: url.host)
    }

    /// playhead-xsdz.71 (Signal 1): hand the observed redirect chain to the
    /// recorder off the download's critical path. Best-effort/observational; a
    /// `nil` recorder/delegate/podcastId is a no-op. `finalHost` is the final
    /// response URL host, appended when it differs from the last recorded hop.
    private func recordDAIStitchChain(
        delegate: RedirectChainRecordingDelegate?,
        context: DownloadContext?,
        finalHost: String?
    ) {
        guard let recorder = daiStitchRecorder,
              let delegate,
              let podcastId = context?.podcastId else { return }
        var hosts = delegate.hopHosts
        if let finalHost, !finalHost.isEmpty, hosts.last != finalHost {
            hosts.append(finalHost)
        }
        Task { await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: hosts) }
    }

    /// playhead-44h1 (fix): inject a `BackgroundTaskScheduling` so
    /// tests can observe the `BGContinuedProcessingTaskRequest`
    /// submission path without touching `BGTaskScheduler.shared`.
    /// Production leaves the default `.shared` scheduler in place.
    func setBackgroundTaskSchedulerForTesting(_ scheduler: any BackgroundTaskScheduling) {
        self.backgroundTaskScheduler = scheduler
    }

    /// playhead-wrj8: inject the resume-freshness validator provider so
    /// tests can exercise the ETag/length-mismatch → re-download-fresh path
    /// without real network. Production leaves this `nil` and the resume
    /// path issues a real HTTP HEAD.
    func setResumeValidatorProviderForTesting(
        _ provider: @escaping @Sendable (URL) async -> HTTPAssetMetadata?
    ) {
        self.resumeValidatorProvider = provider
    }

    /// playhead-wrj8: resolves the current server validator for `url`,
    /// using the injected provider when present, otherwise a real HTTP HEAD.
    func currentServerValidator(for url: URL) async -> HTTPAssetMetadata? {
        if let provider = resumeValidatorProvider {
            return await provider(url)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "HEAD"
        guard let (_, response) = try? await URLSession.shared.data(for: request),
              let http = response as? HTTPURLResponse else {
            return nil
        }
        let len = http.expectedContentLength
        return HTTPAssetMetadata(
            etag: http.value(forHTTPHeaderField: "ETag"),
            contentLength: len > 0 ? len : nil,
            lastModified: http.value(forHTTPHeaderField: "Last-Modified")
        )
    }

    // MARK: - playhead-44h1 (fix): Foreground-assist lifecycle

    /// Register a `UIApplication.willResignActiveNotification` observer
    /// so the `ForegroundAssistHandoff.decide(for:)` entry point has at
    /// least one production call-site. When the app backgrounds with an
    /// in-flight download, this observer builds a snapshot from
    /// ``foregroundAssistProgress`` and routes it through the
    /// decision module. On a `.submitContinuedProcessingRequest`
    /// verdict, it submits a `BGContinuedProcessingTaskRequest` via
    /// the injected `BackgroundTaskScheduling`.
    ///
    /// Scope note (spec state-machine step 3): the keep-alive /
    /// URLSession-background plumbing half of the decision is
    /// explicitly deferred to playhead-iwiy; the `.keepForegroundAssistAlive`
    /// branch here logs and does no work. The observer exists so a
    /// reader grepping for `decide(for:` in production code lands on
    /// this site rather than the bare module definition.
    ///
    /// Idempotent: a second call is a no-op so app-lifecycle wiring
    /// can safely invoke this during both `didFinishLaunching` and
    /// `sceneWillConnectToSession`-equivalent paths.
    func registerForegroundAssistLifecycleObserver() {
        guard foregroundAssistObserverToken == nil else { return }
        let center = NotificationCenter.default
        let token = center.addObserver(
            forName: UIApplication.willResignActiveNotification,
            object: nil,
            queue: nil
        ) { [weak self] _ in
            Task { [weak self] in
                await self?.handleWillResignActive()
            }
        }
        foregroundAssistObserverToken = token
    }

    /// Token returned by `NotificationCenter.addObserver(forName:...)`
    /// so ``deregisterForegroundAssistLifecycleObserver`` can reverse
    /// the registration. An opaque `NSObjectProtocol` per the API
    /// contract. Marked `nonisolated(unsafe)` so the actor's nonisolated
    /// `deinit` can read the token and remove the observer from
    /// `NotificationCenter.default` (itself thread-safe) without
    /// hopping onto the actor. All non-deinit mutations still happen
    /// on the actor so reads/writes remain serialized in practice.
    nonisolated(unsafe) private var foregroundAssistObserverToken: (any NSObjectProtocol)?

    /// playhead-44h1 (fix): remove the `willResignActive` observer on
    /// deinit so a released `DownloadManager` does not leave a stray
    /// `NotificationCenter` registration pointing at freed memory.
    /// Actor `deinit` is nonisolated; `NotificationCenter.removeObserver`
    /// is documented thread-safe and the token is `nonisolated(unsafe)`,
    /// so reading it here is sound. All live mutation of the token
    /// happens through actor-isolated methods, which cannot race with
    /// `deinit` (the last reference has already dropped).
    deinit {
        if let token = foregroundAssistObserverToken {
            NotificationCenter.default.removeObserver(token)
        }
    }

    /// Tear down the `willResignActive` observer. Primarily for
    /// tests so a second registration does not accumulate observers
    /// across test cases.
    func deregisterForegroundAssistLifecycleObserver() {
        if let token = foregroundAssistObserverToken {
            NotificationCenter.default.removeObserver(token)
            foregroundAssistObserverToken = nil
        }
    }

    /// Entry point for the `willResignActive` handoff decision.
    /// Exposed as a non-notification method so tests can drive it
    /// directly without posting through `NotificationCenter`.
    ///
    /// For each active transfer: build a
    /// ``ForegroundAssistTransferSnapshot``, call
    /// ``ForegroundAssistHandoff/decide(for:)``, and act on the
    /// verdict. A single `willResignActive` cycle may emit multiple
    /// submissions — one per still-active episode. The outer loop
    /// iterates distinct keys of ``foregroundAssistProgress`` so the
    /// same episode cannot be submitted twice within one cycle. Across
    /// cycles, iOS coalesces duplicate `BGContinuedProcessingTaskRequest`
    /// identifiers server-side, so we do not maintain any app-side
    /// dedupe set here.
    @discardableResult
    func handleWillResignActive(
        now: Date = Date()
    ) -> [ForegroundAssistHandoffDecision] {
        var decisions: [ForegroundAssistHandoffDecision] = []
        for (episodeId, progress) in foregroundAssistProgress {
            let snapshot = makeSnapshot(for: progress, now: now)
            let decision = ForegroundAssistHandoff.decide(for: snapshot)
            decisions.append(decision)
            logger.info(
                "foreground-assist handoff: episode=\(episodeId, privacy: .public) fraction=\(snapshot.fractionCompleted) etaSeconds=\(snapshot.remainingSeconds) decision=\(String(describing: decision), privacy: .public)"
            )
            switch decision {
            case .submitContinuedProcessingRequest:
                submitContinuedProcessingRequest(for: episodeId)
            case .keepForegroundAssistAlive:
                // Routing into a URLSession background-session
                // keep-alive is playhead-iwiy's territory. This
                // observer's job for the keep-alive branch is just
                // to LOG that a decision was made so reviewers can
                // see the handoff fired at a real call-site.
                break
            }
        }
        return decisions
    }

    /// Build a ``ForegroundAssistTransferSnapshot`` from a stored
    /// progress entry. Throughput is estimated over the full
    /// observation window (first-observed timestamp → `now`) so a
    /// freshly-resumed transfer's throughput does not alias to 0
    /// just because the most recent progress tick was a moment ago.
    private func makeSnapshot(
        for progress: ForegroundAssistProgress,
        now: Date
    ) -> ForegroundAssistTransferSnapshot {
        let elapsed = now.timeIntervalSince(progress.firstObservedAt)
        let bytesDelta = max(0, progress.bytesWritten - progress.firstObservedBytes)
        let throughput: Double
        if elapsed > 0 && bytesDelta > 0 {
            throughput = Double(bytesDelta) / elapsed
        } else {
            // No observable delta yet — treat as unknown so the
            // handoff decision errs toward BG task (the safe choice).
            throughput = 0
        }
        return ForegroundAssistTransferSnapshot(
            totalBytesWritten: progress.bytesWritten,
            totalBytesExpectedToWrite: progress.totalBytes,
            averageBytesPerSecond: throughput
        )
    }

    /// Submit a `BGContinuedProcessingTaskRequest` for `episodeId`.
    /// The identifier follows the wildcard convention
    /// `"<BackgroundTaskID.continuedProcessing>.<episodeId>"` that
    /// `BackgroundProcessingService.parseEpisodeId(from:)` expects.
    ///
    /// No app-side dedupe: iOS `BGTaskScheduler` coalesces duplicate
    /// identifiers, and `handleWillResignActive`'s caller loop already
    /// iterates distinct `episodeId`s, so a single cycle cannot submit
    /// twice for the same episode.
    ///
    /// playhead-izvj.1 (Mac Catalyst spike): `BGContinuedProcessingTaskRequest`
    /// is iOS-only — the API is unavailable in Mac Catalyst. On Catalyst
    /// we no-op and log; the desktop process is not subject to the same
    /// suspend-on-background lifecycle, so the 80%/2-min decision still
    /// flows through `ForegroundAssistHandoff.decide(...)` (so the call
    /// site logs it) but the BG task submission is skipped. A future
    /// Catalyst polish bead can decide whether to keep the URL session
    /// alive explicitly or rely on the OS not killing the process.
    private func submitContinuedProcessingRequest(for episodeId: String) {
        let identifier = "\(BackgroundTaskID.continuedProcessing).\(episodeId)"
        #if targetEnvironment(macCatalyst)
        logger.info(
            "Skipping BGContinuedProcessingTaskRequest on Mac Catalyst (API unavailable): \(identifier, privacy: .public)"
        )
        #else
        let request = BGContinuedProcessingTaskRequest(
            identifier: identifier,
            title: "Finishing download",
            subtitle: "We'll wrap up this episode in the background."
        )
        request.strategy = .fail
        do {
            try backgroundTaskScheduler.submit(request)
            logger.info("Submitted BGContinuedProcessingTaskRequest: \(identifier, privacy: .public)")
        } catch {
            logger.error(
                "Failed to submit BGContinuedProcessingTaskRequest \(identifier, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }
        #endif
    }

    /// Test hook: read the current stored foreground-assist progress
    /// for an episode. Used by unit tests to verify that
    /// `noteTransferProgress` updates the snapshot state.
    func foregroundAssistProgressForTesting(episodeId: String) -> ForegroundAssistProgress? {
        foregroundAssistProgress[episodeId]
    }

    /// Test hook: seed the foreground-assist progress map directly
    /// so tests exercising `handleWillResignActive` do not have to
    /// drive a real download to simulate bytes written.
    func seedForegroundAssistProgressForTesting(
        episodeId: String,
        bytesWritten: Int64,
        totalBytes: Int64,
        firstObservedAt: Date,
        firstObservedBytes: Int64 = 0,
        updatedAt: Date = Date()
    ) {
        foregroundAssistProgress[episodeId] = ForegroundAssistProgress(
            bytesWritten: bytesWritten,
            totalBytes: totalBytes,
            firstObservedAt: firstObservedAt,
            firstObservedBytes: firstObservedBytes,
            updatedAt: updatedAt
        )
    }

    static func defaultCacheDirectory() -> URL {
        FileManager.default
            .urls(for: .cachesDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Playhead", isDirectory: true)
            .appendingPathComponent("AudioCache", isDirectory: true)
    }

    /// Create required directories on first use.
    func bootstrap() throws {
        let fm = FileManager.default
        for dir in [cacheDirectory, partialsDirectory, completeDirectory, resumeDataDirectory] {
            if !fm.fileExists(atPath: dir.path) {
                try fm.createDirectory(at: dir, withIntermediateDirectories: true)
            }
            // playhead-h3h: stamp the audio-cache directories with the
            // same protection class as the AnalysisStore. The bead's
            // wishlist asks for `.complete`, but the same BG-launch
            // constraint that forced AnalysisStore down to
            // `.completeUntilFirstUserAuthentication` applies here:
            // AnalysisCoordinator opens cached audio during
            // BGProcessingTask windows that may begin pre-first-unlock.
            // `.complete` would block those reads. Re-stamping
            // unconditionally migrates pre-h3h installs.
            try? fm.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: dir.path
            )
        }
        // Remove stale files with unrecognized extensions (e.g. ".audio"
        // from earlier builds) that AVURLAsset can't open.
        if let files = try? fm.contentsOfDirectory(atPath: completeDirectory.path) {
            for file in files {
                let ext = (file as NSString).pathExtension
                if !ext.isEmpty, !Self.knownAudioExtensions.contains(ext), ext != "partial" {
                    let staleURL = completeDirectory.appendingPathComponent(file)
                    try? fm.removeItem(at: staleURL)
                    logger.info("Removed stale cache file: \(file)")
                }
            }
        }

        // Rebuild access log from file system.
        try rebuildAccessLog()
        logger.info("DownloadManager bootstrapped at \(self.cacheDirectory.path)")
    }

    // MARK: - Background Session

    /// Roles (configurations) under which a background URLSession may be
    /// instantiated. playhead-24cm introduces `interactive` and
    /// `maintenance`; `legacy` remains live for one release cycle so
    /// resume data from the pre-24cm single-session build can drain.
    enum BackgroundSessionRole: Hashable {
        /// User-initiated downloads. `isDiscretionary = false`.
        case interactive
        /// Subscription auto-downloads. `isDiscretionary = true`,
        /// `allowsCellularAccess` follows the user preference.
        case maintenance
        /// Legacy single-session identifier. Retained during rollout.
        case legacy

        var identifier: String {
            switch self {
            case .interactive: return BackgroundSessionIdentifier.interactive
            case .maintenance: return BackgroundSessionIdentifier.maintenance
            case .legacy:      return BackgroundSessionIdentifier.legacy
            }
        }

        static func role(for identifier: String) -> BackgroundSessionRole? {
            switch identifier {
            case BackgroundSessionIdentifier.interactive: return .interactive
            case BackgroundSessionIdentifier.maintenance: return .maintenance
            case BackgroundSessionIdentifier.legacy:      return .legacy
            default: return nil
            }
        }
    }

    /// Returns the URLSession for the given role, instantiating it lazily.
    /// When the 24cm feature flag is OFF, callers that ask for `.interactive`
    /// or `.maintenance` are transparently routed to `.legacy` so the
    /// behavior matches the pre-24cm build exactly.
    ///
    /// Visibility is `internal` (not `private`) so the playhead-hyht
    /// force-quit scan extension in `ForceQuitResumeScan.swift` can hand
    /// a resume-data blob back to the interactive session.
    internal func backgroundSession(for role: BackgroundSessionRole) -> URLSession {
        let resolvedRole: BackgroundSessionRole = {
            if !useDualBackgroundSessions, role != .legacy { return .legacy }
            return role
        }()

        if let existing = _sessionsByRole[resolvedRole] { return existing }

        let config: URLSessionConfiguration
        switch resolvedRole {
        case .interactive:
            config = URLSessionConfiguration.background(
                withIdentifier: BackgroundSessionIdentifier.interactive
            )
            config.sessionSendsLaunchEvents = true
            config.isDiscretionary = false
            config.allowsCellularAccess = true
        case .maintenance:
            config = URLSessionConfiguration.background(
                withIdentifier: BackgroundSessionIdentifier.maintenance
            )
            config.sessionSendsLaunchEvents = true
            config.isDiscretionary = true
            // UserPreferences.allowsCellular governs the maintenance lane
            // because auto-downloads are the surface most likely to
            // surprise users on cellular.
            config.allowsCellularAccess = UserPreferencesSnapshot.current.allowsCellular
        case .legacy:
            config = URLSessionConfiguration.background(
                withIdentifier: BackgroundSessionIdentifier.legacy
            )
            config.sessionSendsLaunchEvents = true
            config.isDiscretionary = false
            config.allowsCellularAccess = true
        }

        let session = URLSession(
            configuration: config,
            delegate: sessionDelegate,
            delegateQueue: nil
        )

        // onBackgroundDownloadStaged is wired once at init — see
        // DownloadManager initializer. No per-session reassignment
        // needed.

        _sessionsByRole[resolvedRole] = session
        return session
    }

    /// Legacy single-session accessor preserved for existing call sites
    /// (`backgroundDownload(episodeId:from:)` below). Routes to the
    /// legacy identifier unless the feature flag is on — in which case
    /// user-initiated downloads use the interactive lane.
    private func backgroundSession() -> URLSession {
        backgroundSession(for: useDualBackgroundSessions ? .interactive : .legacy)
    }

    /// Re-instantiates the URLSession for `identifier` so its delegate
    /// callbacks fire. Invoked by `PlayheadAppDelegate` when iOS wakes
    /// the app to relay pending background events.
    func resumeSession(identifier: String) {
        guard let role = BackgroundSessionRole.role(for: identifier) else { return }
        _ = backgroundSession(for: role)
    }

    /// Flip the 24cm feature flag in-process. Called from two paths:
    ///   1. Tests that want to exercise the dual-session code path
    ///      without touching UserDefaults.
    ///   2. Settings → Diagnostics → Feature flags: when the user
    ///      toggles `playhead-24cm`, `SettingsView` persists the value
    ///      via `PreAnalysisConfig.save()` and then calls this method
    ///      on the shared manager so the new lane selection takes
    ///      effect without waiting for the next app launch.
    func setUseDualBackgroundSessions(_ value: Bool) {
        self.useDualBackgroundSessions = value
    }

    // MARK: - Test hooks (internal)

    func backgroundSessionForTesting(role: BackgroundSessionRole) -> URLSession {
        backgroundSession(for: role)
    }

    func instantiatedSessionIdentifiersForTesting() -> Set<String> {
        Set(_sessionsByRole.values.compactMap { $0.configuration.identifier })
    }

    /// Snapshot of currently-instantiated background URLSessions across
    /// all roles. Used by `ForceQuitResumeScan.liveBackgroundDownloadEpisodeIds`
    /// to dedup the cold-launch scan against transfers the OS still
    /// owns, without triggering lazy instantiation of cold sessions
    /// (which would blow the 2 s scan SLA).
    func backgroundSessionsAlreadyInstantiated() -> [URLSession] {
        Array(_sessionsByRole.values)
    }

    #if DEBUG
    /// playhead-g2wq test seam: exposes the URLSession delegate so tests
    /// can drive `didCompleteWithError` directly and verify the
    /// resume-data harvest path writes into `resumeDataDirectory`.
    /// DEBUG-only to keep production binaries free of the delegate-escape
    /// surface.
    func sessionDelegateForTesting() -> EpisodeDownloadDelegate {
        sessionDelegate
    }

    /// playhead-6e8m test seam: cancels every in-flight task on every
    /// instantiated background URLSession and invalidates the sessions
    /// themselves. Required by tests that exercise the resume path
    /// (e.g. `ResumeSuspendedTransferTests.resumeConsumesBlob`) which
    /// hand garbage `Data` blobs to a real background URLSession on the
    /// process-global `com.playhead.transfer.interactive` identifier —
    /// without invalidation the orphaned task stays alive for the rest
    /// of the process and leaks into sibling tests that construct a
    /// fresh `DownloadManager`.
    ///
    /// Idempotent: a session that has already been invalidated is
    /// dropped from the role map, so a second call is a no-op. Drops
    /// the role map after invalidation so subsequent calls to
    /// `backgroundSession(for:)` would lazily create a fresh session
    /// (callers should treat this as an end-of-life signal for the
    /// manager-under-test).
    func invalidateBackgroundSessionsForTesting() {
        for session in _sessionsByRole.values {
            session.invalidateAndCancel()
        }
        _sessionsByRole.removeAll()
    }
    #endif

    // MARK: - Progressive Download (Streaming Cache)

    /// Starts a progressive download, caching audio as it arrives.
    /// Returns the local file URL once enough data is available for playback.
    /// The download continues in the background until complete.
    ///
    /// If the file is already fully cached, returns immediately.
    func progressiveDownload(
        episodeId: String,
        from url: URL,
        context: DownloadContext? = nil
    ) async throws -> URL {
        // Cache the source extension for this episode.
        let sourceExt = url.pathExtension.isEmpty ? "mp3" : url.pathExtension
        extensionCache[episodeId] = sourceExt

        // Already fully cached? playhead-wrj8: completeness-gated so a
        // truncated file is re-fetched rather than served, and a complete
        // pinned artifact is returned as-is (never re-fetched → never
        // rotated by a fresh DAI stitch).
        if let complete = servingURLIfComplete(for: episodeId) {
            touchAccess(episodeId: episodeId)
            return complete
        }

        // Already downloading?
        if let existing = activeDownloads[episodeId] {
            return try await existing.value
        }

        let task = Task<URL, Error> { [weak self] in
            guard let self else { throw DownloadManagerError.cancelled }
            return try await self.performDownload(episodeId: episodeId, url: url, context: context)
        }

        activeDownloads[episodeId] = task

        do {
            let result = try await task.value
            activeDownloads[episodeId] = nil
            return result
        } catch {
            activeDownloads[episodeId] = nil
            throw error
        }
    }

    /// Core download logic: downloads to a temp file, then moves to cache.
    /// Uses URLSession.shared.download(for:) to avoid byte-at-a-time iteration.
    private func performDownload(episodeId: String, url: URL, context: DownloadContext? = nil) async throws -> URL {
        let completeURL = completeFileURL(for: episodeId)

        let request = URLRequest(url: url)
        let fm = FileManager.default

        // playhead-xsdz.71 (Signal 1, additive): observe the enclosure's
        // redirect chain when a recorder is wired. The delegate only records hop
        // hosts and returns the proposed redirect, so passing it (or `nil`) is
        // byte-identical to today's `download(for:)`.
        let redirectDelegate = makeRedirectRecordingDelegate(url: url, context: context)

        // Download to a temporary file (handled efficiently by URLSession).
        let (tempURL, response) = try await URLSession.shared.download(
            for: request, delegate: redirectDelegate
        )
        // Clean up temp file on any error path.
        defer { try? fm.removeItem(at: tempURL) }

        guard let httpResponse = response as? HTTPURLResponse,
              (200...299).contains(httpResponse.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            throw DownloadManagerError.downloadFailed(episodeId, "HTTP \(code)")
        }

        // playhead-xsdz.71 (Signal 1): record the observed redirect chain
        // (no-op unless a recorder is wired).
        recordDAIStitchChain(
            delegate: redirectDelegate, context: context, finalHost: httpResponse.url?.host
        )

        // Harvest HTTP metadata for weak fingerprinting.
        let reportedLength = httpResponse.expectedContentLength
        let totalContentLength: Int64? = reportedLength > 0 ? reportedLength : nil
        let metadata = HTTPAssetMetadata(
            etag: httpResponse.value(forHTTPHeaderField: "ETag"),
            contentLength: totalContentLength,
            lastModified: httpResponse.value(forHTTPHeaderField: "Last-Modified")
        )
        metadataCache[episodeId] = metadata

        // Build weak fingerprint immediately.
        let weakFP = AudioFingerprint.makeWeak(url: url, metadata: metadata)
        fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: nil)

        // playhead-wrj8: refuse to overwrite an already-complete pinned
        // artifact. If a complete file materialized between the
        // early-return check and now (a concurrent writer, or a bg
        // pre-cache that finished first), keep it — never replace the
        // played/analyzed bytes with a freshly-cut DAI stitch. The temp
        // file is cleaned up by the `defer` above.
        if let existing = servingURLIfComplete(for: episodeId) {
            touchAccess(episodeId: episodeId)
            logger.info("Download for \(episodeId): complete pinned artifact already present — keeping it, discarding re-fetch")
            return existing
        }

        // Move temp -> complete first, then hash from final location. An
        // incomplete leftover (partial from a failed stream) is safe to
        // replace — `servingURLIfComplete` returned nil for it above.
        if fm.fileExists(atPath: completeURL.path) {
            try fm.removeItem(at: completeURL)
        }
        try fm.copyItem(at: tempURL, to: completeURL)

        // Get the file size.
        let attrs = try fm.attributesOfItem(atPath: completeURL.path)
        let downloaded = (attrs[.size] as? Int64) ?? 0

        // Compute strong fingerprint from the final file.
        let strongHash = try FileHasher.sha256(fileURL: completeURL)
        fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: strongHash)

        // playhead-wrj8: pin the artifact as COMPLETE. From here the file
        // is immutable — cachedFileURL/streaming cache-hit/overwrite guards
        // all treat it as the single served copy.
        writePin(
            AudioAssetPin(
                expectedBytes: downloaded,
                sha256: strongHash,
                sourceURL: url.absoluteString,
                etag: metadata.etag
            ),
            for: episodeId
        )

        // Enqueue pre-analysis if scheduler is wired up.
        if let scheduler = analysisWorkScheduler {
            await scheduler.enqueue(
                episodeId: episodeId,
                podcastId: context?.podcastId,
                downloadId: episodeId,
                sourceFingerprint: strongHash,
                isExplicitDownload: context?.isExplicitDownload ?? false,
                podcastTitle: context?.podcastTitle,
                episodeTitle: context?.episodeTitle
            )
        }

        touchAccess(episodeId: episodeId)

        logger.info("Download complete for \(episodeId): \(downloaded) bytes, hash=\(strongHash.prefix(16))...")

        finishAudioDataSubscribers()

        // Evict if over budget.
        try await evictIfNeeded()

        broadcastProgress(DownloadProgress(
            episodeId: episodeId,
            bytesWritten: downloaded,
            totalBytes: downloaded
        ))

        return completeURL
    }

    // MARK: - Streaming Download (Play While Downloading)

    /// Minimum bytes before signaling playback can start.
    /// ~60s at 256 kbps = ~1.9 MB. Round up to 2 MB.
    static let defaultPlayableThreshold: Int64 = 8 * 1024 * 1024

    /// Result of a streaming download: the local file URL is available for
    /// playback once the threshold is reached; await `downloadComplete` before
    /// starting analysis (which needs the full file).
    struct StreamingDownloadResult: Sendable {
        /// Local file URL — available for playback immediately.
        let fileURL: URL
        /// Total expected file size from HTTP Content-Length, or nil if unknown.
        let totalBytes: Int64?
        /// Audio content type UTI (e.g. "public.mp3").
        let contentType: String
        /// Resolves when the entire file has been written to disk.
        let downloadComplete: @Sendable () async throws -> Void
    }

    /// Downloads episode audio incrementally, returning the local file URL
    /// as soon as `playableThreshold` bytes have been written.
    /// The download continues in the background until complete.
    ///
    /// If the file is already fully cached, returns immediately with a
    /// no-op `downloadComplete`.
    func streamingDownload(
        episodeId: String,
        from url: URL,
        playableThreshold: Int64 = DownloadManager.defaultPlayableThreshold,
        context: DownloadContext? = nil
    ) async throws -> StreamingDownloadResult {
        let sourceExt = url.pathExtension.isEmpty ? "mp3" : url.pathExtension
        extensionCache[episodeId] = sourceExt

        let completeURL = completeFileURL(for: episodeId)
        // playhead-wrj8: completeness-gated cache-hit. A COMPLETE pinned
        // artifact is served as-is (never re-streamed → the played bytes
        // can't be swapped for a different DAI stitch). A mid-stream /
        // truncated leftover (pin present but under-length) is NOT a
        // cache-hit and falls through to a fresh stream below.
        if let complete = servingURLIfComplete(for: episodeId) {
            touchAccess(episodeId: episodeId)
            let uti = Self.utiForExtension(sourceExt)
            let attrs = try? FileManager.default.attributesOfItem(atPath: complete.path)
            let size = (attrs?[.size] as? Int64)
            return StreamingDownloadResult(fileURL: complete, totalBytes: size, contentType: uti, downloadComplete: {})
        }

        // Write directly to the final location so AVPlayer can read it.
        // Any leftover here is an incomplete partial (the complete case
        // returned above); replacing it is safe.
        let fm = FileManager.default
        if fm.fileExists(atPath: completeURL.path) {
            try fm.removeItem(at: completeURL)
        }
        // playhead-wrj8: drop any stale pin from a prior interrupted
        // attempt so the fresh stream re-pins from scratch below.
        deletePin(for: episodeId)
        // playhead-h3h: create with explicit
        // `.completeUntilFirstUserAuthentication` rather than letting the
        // file inherit the system default. Aligns with the AnalysisStore
        // protection class so a BGProcessingTask reading cached audio
        // pre-first-unlock cannot fail with EPERM mid-pipeline.
        fm.createFile(
            atPath: completeURL.path,
            contents: nil,
            attributes: [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication]
        )
        let fileHandle = try FileHandle(forWritingTo: completeURL)

        // playhead-wrj8 (R1): seed an always-incomplete pin (Int64.max) the
        // instant the empty file exists, BEFORE the network await below.
        // Without it, `servingURLIfComplete` treats the freshly-created
        // 0-byte / mid-connection file as complete-by-existence (no pin yet)
        // for the whole connection-setup window, so a concurrent cache-hit
        // reader could be handed a truncated file — the exact "serve a
        // partial" hole the invariant forbids. The real Content-Length
        // rewrites this a few lines down; `finalizeStreamingPin` stamps the
        // true length at completion.
        writePin(
            AudioAssetPin(
                expectedBytes: Int64.max,
                sha256: nil,
                sourceURL: url.absoluteString,
                etag: nil
            ),
            for: episodeId
        )

        let request = URLRequest(url: url)
        // playhead-xsdz.71 (Signal 1, additive): observe the enclosure's
        // redirect chain when a recorder is wired. Behavior-preserving — the
        // delegate only records hop hosts and follows the proposed redirect, so
        // passing it (or `nil`) is byte-identical to today's `bytes(for:)`.
        let redirectDelegate = makeRedirectRecordingDelegate(url: url, context: context)
        let (bytes, response) = try await URLSession.shared.bytes(
            for: request, delegate: redirectDelegate
        )

        guard let httpResponse = response as? HTTPURLResponse,
              (200...299).contains(httpResponse.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            try? fileHandle.close()
            try? fm.removeItem(at: completeURL)
            // Drop the seed pin so a removed file leaves no orphan pin behind.
            deletePin(for: episodeId)
            throw DownloadManagerError.downloadFailed(episodeId, "HTTP \(code)")
        }

        // playhead-xsdz.71 (Signal 1): record the observed redirect chain now
        // that the response headers are in (the redirects completed during the
        // `bytes(for:)` await). No-op unless a recorder is wired. Fired before
        // the detached streaming body so it stays off the byte-copy loop.
        recordDAIStitchChain(
            delegate: redirectDelegate, context: context, finalHost: httpResponse.url?.host
        )

        // Harvest HTTP metadata for weak fingerprinting.
        let reportedLength = httpResponse.expectedContentLength
        let totalContentLength: Int64? = reportedLength > 0 ? reportedLength : nil
        let metadata = HTTPAssetMetadata(
            etag: httpResponse.value(forHTTPHeaderField: "ETag"),
            contentLength: totalContentLength,
            lastModified: httpResponse.value(forHTTPHeaderField: "Last-Modified")
        )
        metadataCache[episodeId] = metadata
        let weakFP = AudioFingerprint.makeWeak(url: url, metadata: metadata)
        fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: nil)

        // playhead-wrj8: seed an INCOMPLETE pin — `expectedBytes` is the
        // full Content-Length (or Int64.max when the server omits it).
        // While the file grows below its size stays under `expectedBytes`,
        // so `servingURLIfComplete` withholds it from every cache path
        // (and a force-quit mid-stream leaves it withheld across relaunch,
        // never serving a truncated file). `finalizeStreamingPin` rewrites
        // it to the real length on completion.
        writePin(
            AudioAssetPin(
                expectedBytes: totalContentLength ?? Int64.max,
                sha256: nil,
                sourceURL: url.absoluteString,
                etag: metadata.etag
            ),
            for: episodeId
        )

        let signalURL = completeURL
        let threshold = min(playableThreshold, totalContentLength ?? playableThreshold)
        let audioUTI = Self.utiForExtension(sourceExt)

        // Completion continuation — signaled when the full file is written.
        let completionStream = AsyncStream<Result<Void, Error>>.makeStream()

        // Playback-ready continuation — signaled when threshold is reached.
        let result: StreamingDownloadResult = try await withCheckedThrowingContinuation { continuation in
            let capturedLogger = self.logger
            let capturedEpisodeId = episodeId
            // playhead-wrj8: carry the source URL + validator into the
            // detached completion so the finalized pin records them.
            let capturedSourceURL = url.absoluteString
            let capturedEtag = metadata.etag
            let completionContinuation = completionStream.1
            Task.detached { [weak self] in
                var bytesWritten: Int64 = 0
                var signaled = false
                var buffer = Data()
                let flushSize = 64 * 1024

                do {
                    for try await byte in bytes {
                        buffer.append(byte)

                        if buffer.count >= flushSize {
                            fileHandle.write(buffer)
                            bytesWritten += Int64(buffer.count)
                            await self?.broadcastAudioData(AudioDataChunk(
                                episodeId: capturedEpisodeId,
                                data: buffer,
                                totalBytesWritten: bytesWritten
                            ))
                            buffer.removeAll(keepingCapacity: true)

                            if !signaled, bytesWritten >= threshold {
                                signaled = true
                                capturedLogger.info("Playable threshold reached for \(capturedEpisodeId): \(bytesWritten) bytes")
                                let waitForComplete: @Sendable () async throws -> Void = {
                                    for await result in completionStream.0 {
                                        switch result {
                                        case .success: return
                                        case .failure(let error): throw error
                                        }
                                    }
                                }
                                continuation.resume(returning: StreamingDownloadResult(
                                    fileURL: signalURL,
                                    totalBytes: totalContentLength,
                                    contentType: audioUTI,
                                    downloadComplete: waitForComplete
                                ))
                            }

                            await self?.broadcastProgress(DownloadProgress(
                                episodeId: capturedEpisodeId,
                                bytesWritten: bytesWritten,
                                totalBytes: totalContentLength ?? bytesWritten
                            ))
                        }
                    }

                    // Flush remaining bytes.
                    if !buffer.isEmpty {
                        fileHandle.write(buffer)
                        bytesWritten += Int64(buffer.count)
                        await self?.broadcastAudioData(AudioDataChunk(
                            episodeId: capturedEpisodeId,
                            data: buffer,
                            totalBytesWritten: bytesWritten
                        ))
                    }
                    try fileHandle.close()

                    // If file was smaller than threshold, signal both at once.
                    if !signaled {
                        continuation.resume(returning: StreamingDownloadResult(
                            fileURL: signalURL,
                            totalBytes: totalContentLength,
                            contentType: audioUTI,
                            downloadComplete: {}
                        ))
                    }

                    // Compute strong fingerprint now that file is complete.
                    let strongHash = try? FileHasher.sha256(fileURL: signalURL)
                    // playhead-wrj8: finalize the completeness pin to the
                    // real on-disk length BEFORE eviction/serving, so the
                    // just-completed stream becomes the single immutable
                    // artifact. Done unconditionally (even when hashing
                    // fails) so the file can never remain wedged as
                    // "incomplete" and force a re-stream that would land a
                    // different DAI stitch.
                    await self?.finalizeStreamingPin(
                        episodeId: capturedEpisodeId,
                        sourceURL: capturedSourceURL,
                        etag: capturedEtag,
                        sha256: strongHash
                    )
                    if let strongHash {
                        await self?.setFingerprint(
                            episodeId: capturedEpisodeId, weak: weakFP, strong: strongHash
                        )
                        // Enqueue pre-analysis if scheduler is wired up.
                        await self?.enqueueAnalysisIfNeeded(
                            episodeId: capturedEpisodeId,
                            sourceFingerprint: strongHash,
                            context: context
                        )
                        capturedLogger.info("Download complete for \(capturedEpisodeId): \(bytesWritten) bytes, hash=\(strongHash.prefix(16))...")
                    }

                    await self?.finishAudioDataSubscribers()
                    await self?.touchAccess(episodeId: capturedEpisodeId)
                    try await self?.evictIfNeeded()

                    await self?.broadcastProgress(DownloadProgress(
                        episodeId: capturedEpisodeId,
                        bytesWritten: bytesWritten,
                        totalBytes: bytesWritten
                    ))

                    // Signal download complete.
                    completionContinuation.yield(.success(()))
                    completionContinuation.finish()
                } catch {
                    try? fileHandle.close()
                    if !signaled {
                        continuation.resume(throwing: error)
                    }
                    await self?.finishAudioDataSubscribers()
                    completionContinuation.yield(.failure(error))
                    completionContinuation.finish()
                    capturedLogger.error("Streaming download failed for \(capturedEpisodeId): \(error)")
                }
            }
        }

        return result
    }

    /// Helper for detached task to update fingerprint cache.
    fileprivate func setFingerprint(episodeId: String, weak weakFP: String, strong strongFP: String) {
        fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: strongFP)
    }

    /// Helper for detached task to enqueue analysis after download completes.
    fileprivate func enqueueAnalysisIfNeeded(
        episodeId: String,
        sourceFingerprint: String,
        context: DownloadContext?
    ) async {
        guard let scheduler = analysisWorkScheduler else { return }
        await scheduler.enqueue(
            episodeId: episodeId,
            podcastId: context?.podcastId,
            downloadId: episodeId,
            sourceFingerprint: sourceFingerprint,
            isExplicitDownload: context?.isExplicitDownload ?? false,
            // playhead-i9dj: human-readable titles flow through to
            // AnalysisStore writes inside the scheduler so an exported
            // analysis.sqlite is legible on its own.
            podcastTitle: context?.podcastTitle,
            episodeTitle: context?.episodeTitle
        )
    }

    /// Map file extension to UTI for AVAssetResourceLoaderDelegate.
    static func utiForExtension(_ ext: String) -> String {
        switch ext.lowercased() {
        case "mp3":  return "public.mp3"
        case "m4a":  return "public.mpeg-4-audio"
        case "aac":  return "public.aac-audio"
        case "wav":  return "com.microsoft.waveform-audio"
        case "mp4":  return "public.mpeg-4"
        case "ogg":  return "org.xiph.ogg"
        case "opus": return "org.xiph.opus"
        default:     return "public.audio"
        }
    }

    // MARK: - Background Pre-Cache

    /// Queues a background download for an episode (pre-caching).
    /// Completes even if the app is suspended.
    func backgroundDownload(episodeId: String, from url: URL) {
        let sourceExt = url.pathExtension.isEmpty ? "mp3" : url.pathExtension
        extensionCache[episodeId] = sourceExt
        let completeURL = completeFileURL(for: episodeId)
        guard !FileManager.default.fileExists(atPath: completeURL.path) else {
            logger.debug("Skipping background download for \(episodeId): already cached")
            return
        }
        // playhead-3xtw: idempotent — never start a second concurrent
        // transfer for an episode already downloading (a rapid double-tap
        // of the prepare control, or an auto + on-demand collision). The
        // slot is cleared on completion/failure, so a genuine retry after a
        // finished attempt still proceeds.
        guard !bgInFlightEpisodes.contains(episodeId) else {
            logger.debug("Skipping background download for \(episodeId): already in flight")
            return
        }

        // Pre-cache work: route through the maintenance lane when the
        // dual-session flag is on so it cannot starve user-initiated
        // (.interactive) downloads. When the flag is off, fall through
        // to the legacy single session.
        let session = useDualBackgroundSessions
            ? backgroundSession(for: .maintenance)
            : backgroundSession(for: .legacy)
        let task = session.downloadTask(with: url)
        task.taskDescription = episodeId
        bgInFlightEpisodes.insert(episodeId)
        // playhead-3xtw (L2): reset the progress high-water mark for a fresh
        // transfer so a retry's early ticks aren't dropped as "stale".
        lastBackgroundProgressBytes[episodeId] = nil
        task.resume()
        logger.info("Queued background download for \(episodeId)")
    }

    // MARK: - Cancel

    /// Cancels an active download for the given episode.
    func cancelDownload(episodeId: String) {
        if let task = activeDownloads[episodeId] {
            task.cancel()
            activeDownloads[episodeId] = nil
            logger.info("Cancelled download for \(episodeId)")
        }
    }

    // MARK: - File Locations

    /// Derive a filesystem-safe name from an episode ID.
    /// Episode IDs can contain URL characters (://) so we SHA-256 hash them.
    static func safeFilename(for episodeId: String) -> String {
        let digest = SHA256.hash(data: Data(episodeId.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    /// URL for a partially-downloaded episode file.
    func partialFileURL(for episodeId: String) -> URL {
        partialsDirectory.appendingPathComponent("\(Self.safeFilename(for: episodeId)).partial")
    }

    /// URL for a fully-downloaded, verified episode file.
    /// Uses the cached source extension so AVURLAsset can identify the codec.
    func completeFileURL(for episodeId: String) -> URL {
        let ext = resolveExtension(for: episodeId)
        return completeDirectory.appendingPathComponent("\(Self.safeFilename(for: episodeId)).\(ext)")
    }

    /// Audio extensions AVURLAsset can identify.
    private static let knownAudioExtensions: Set<String> = [
        "mp3", "m4a", "aac", "wav", "caf", "aiff", "mp4", "ogg", "opus"
    ]

    // MARK: - Completeness pin (playhead-wrj8)

    /// File extension for the per-episode completeness pin sidecar.
    static let pinExtension = "pin"

    /// URL of the `<hash>.pin` completeness sidecar for an episode. Shares
    /// the audio file's hashed basename but a distinct extension, so it is
    /// never mistaken for the audio file by `resolveExtension`/eviction/etc.
    func pinFileURL(for episodeId: String) -> URL {
        completeDirectory
            .appendingPathComponent("\(Self.safeFilename(for: episodeId)).\(Self.pinExtension)")
    }

    /// Loads the persisted completeness pin for an episode, or `nil` when
    /// none is stored (legacy files, or a not-yet-downloaded episode).
    func loadPin(for episodeId: String) -> AudioAssetPin? {
        let url = pinFileURL(for: episodeId)
        guard let data = try? Data(contentsOf: url) else { return nil }
        return try? JSONDecoder().decode(AudioAssetPin.self, from: data)
    }

    /// Atomically writes/overwrites the completeness pin for an episode.
    func writePin(_ pin: AudioAssetPin, for episodeId: String) {
        let url = pinFileURL(for: episodeId)
        guard let data = try? JSONEncoder().encode(pin) else { return }
        do {
            let dir = url.deletingLastPathComponent()
            if !FileManager.default.fileExists(atPath: dir.path) {
                try FileManager.default.createDirectory(
                    at: dir, withIntermediateDirectories: true
                )
            }
            try data.write(to: url, options: .atomic)
        } catch {
            logger.error(
                "Failed to write completeness pin for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }
    }

    /// Removes the completeness pin for an episode (no-op when absent).
    func deletePin(for episodeId: String) {
        try? FileManager.default.removeItem(at: pinFileURL(for: episodeId))
    }

    /// playhead-wrj8: finalize a streaming download's pin to the actual
    /// on-disk length so `servingURLIfComplete` starts serving it. Reads the
    /// true size from disk (authoritative — the streamed byte counter could
    /// drift) and stamps the optional strong hash. Called from the detached
    /// streaming-completion task.
    fileprivate func finalizeStreamingPin(
        episodeId: String,
        sourceURL: String,
        etag: String?,
        sha256: String?
    ) {
        let size = completeFileSize(for: episodeId) ?? 0
        guard size > 0 else {
            // No bytes on disk — leave any incomplete pin in place so the
            // file stays withheld rather than being marked complete-at-zero.
            return
        }
        writePin(
            AudioAssetPin(
                expectedBytes: size,
                sha256: sha256,
                sourceURL: sourceURL,
                etag: etag
            ),
            for: episodeId
        )
    }

    /// On-disk byte length of the complete audio file, or `nil` if absent.
    private func completeFileSize(for episodeId: String) -> Int64? {
        let url = completeFileURL(for: episodeId)
        guard let attrs = try? FileManager.default.attributesOfItem(atPath: url.path) else {
            return nil
        }
        return (attrs[.size] as? Int64)
    }

    /// playhead-wrj8: the single completeness gate. Returns the audio file
    /// URL ONLY when the episode has a fully-downloaded, serveable artifact:
    ///
    ///   * file present AND a pin exists → serveable iff the on-disk length
    ///     reached the pin's `expectedBytes` (a truncated / mid-stream /
    ///     interrupted file has fewer bytes and is withheld);
    ///   * file present AND no pin → treated as complete-by-existence
    ///     (legacy files downloaded before wrj8), so the change is
    ///     non-destructive;
    ///   * file absent → `nil`.
    ///
    /// Every "is this cached?" and "may I overwrite this?" decision routes
    /// through here so playback, analysis, and the download writers all
    /// agree on exactly one immutable artifact per episode.
    func servingURLIfComplete(for episodeId: String) -> URL? {
        guard let size = completeFileSize(for: episodeId) else { return nil }
        if let pin = loadPin(for: episodeId) {
            guard pin.expectedBytes > 0, size >= pin.expectedBytes else { return nil }
        }
        return completeFileURL(for: episodeId)
    }

    /// Resolve the file extension for an episode. Checks the in-memory cache
    /// first, then scans the complete directory for a matching file.
    private func resolveExtension(for episodeId: String) -> String {
        if let cached = extensionCache[episodeId] {
            return cached
        }
        // Scan the directory for any file matching this hash prefix.
        let prefix = Self.safeFilename(for: episodeId)
        let fm = FileManager.default
        if let files = try? fm.contentsOfDirectory(atPath: completeDirectory.path) {
            for file in files where file.hasPrefix(prefix) {
                let ext = (file as NSString).pathExtension
                if Self.knownAudioExtensions.contains(ext) {
                    extensionCache[episodeId] = ext
                    return ext
                }
            }
        }
        return "mp3"
    }

    /// Returns the cached file URL if the episode is fully downloaded.
    /// playhead-wrj8: gated on completeness (a truncated / mid-stream file
    /// no longer reads as "cached"), so playback + analysis never resolve a
    /// partial artifact.
    func cachedFileURL(for episodeId: String) -> URL? {
        guard let url = servingURLIfComplete(for: episodeId) else { return nil }
        touchAccess(episodeId: episodeId)
        return url
    }

    /// Returns true if the episode audio is fully cached on disk.
    func isCached(episodeId: String) -> Bool {
        servingURLIfComplete(for: episodeId) != nil
    }

    /// Returns the set of episode IDs that have fully-downloaded cached audio.
    /// Scans the complete directory and reverse-maps filenames back to episode IDs
    /// using the access log (which tracks all episodes that have been downloaded).
    func allCachedEpisodeIds() -> Set<String> {
        Set(accessLog.keys.filter { isCached(episodeId: $0) })
    }

    /// Returns the subset of the supplied episode IDs whose complete audio
    /// file is present on disk.
    ///
    /// Unlike `allCachedEpisodeIds()`, this does not depend on the LRU access
    /// log being keyed by episode ID; Activity already knows the relevant
    /// episode IDs, so we can match their safe filenames against the complete
    /// directory in one pass.
    func cachedEpisodeIds(matching episodeIds: Set<String>) -> Set<String> {
        guard !episodeIds.isEmpty else { return [] }
        let fm = FileManager.default
        guard let files = try? fm.contentsOfDirectory(atPath: completeDirectory.path) else {
            return []
        }
        let completeBasenames = Set(files.compactMap { file -> String? in
            let filename = file as NSString
            let ext = filename.pathExtension.lowercased()
            guard Self.knownAudioExtensions.contains(ext) else { return nil }
            return filename.deletingPathExtension
        })
        return Set(episodeIds.filter { completeBasenames.contains(Self.safeFilename(for: $0)) })
    }

    // MARK: - Fingerprinting

    /// Returns the current fingerprint for an episode, if available.
    func fingerprint(for episodeId: String) -> AudioFingerprint? {
        fingerprintCache[episodeId]
    }

    /// Computes or returns the strong fingerprint (full SHA-256) for a cached file.
    /// Returns nil if the file is not fully cached.
    func computeStrongFingerprint(episodeId: String, url: URL) throws -> AudioFingerprint? {
        let fileURL = completeFileURL(for: episodeId)
        guard FileManager.default.fileExists(atPath: fileURL.path) else { return nil }

        // If we already have a strong fingerprint, return it.
        if let existing = fingerprintCache[episodeId], existing.strong != nil {
            return existing
        }

        let hash = try FileHasher.sha256(fileURL: fileURL)
        let metadata = metadataCache[episodeId] ?? HTTPAssetMetadata(
            etag: nil, contentLength: nil, lastModified: nil
        )
        let weakFP = AudioFingerprint.makeWeak(url: url, metadata: metadata)
        let fp = AudioFingerprint(weak: weakFP, strong: hash)
        fingerprintCache[episodeId] = fp
        return fp
    }

    // MARK: - Integrity Verification

    /// Verifies that a cached file matches the expected strong fingerprint.
    func verifyIntegrity(episodeId: String, expectedHash: String) throws -> Bool {
        let fileURL = completeFileURL(for: episodeId)
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            throw DownloadManagerError.fileNotFound(fileURL.path)
        }
        let actualHash = try FileHasher.sha256(fileURL: fileURL)
        return actualHash.lowercased() == expectedHash.lowercased()
    }

    // MARK: - Analysis Protection

    /// Marks an episode as in-use (active analysis or in-flight playback),
    /// protecting its cached audio file from LRU eviction. playhead-wrj8:
    /// refcounted — balance every call with exactly one
    /// ``unprotectFromAnalysis(episodeId:)``.
    func protectForAnalysis(episodeId: String) {
        analysisProtectedEpisodes[episodeId, default: 0] += 1
    }

    /// Releases one unit of eviction protection. The episode becomes
    /// eviction-eligible only when the refcount returns to zero.
    func unprotectFromAnalysis(episodeId: String) {
        guard let count = analysisProtectedEpisodes[episodeId] else { return }
        if count <= 1 {
            analysisProtectedEpisodes.removeValue(forKey: episodeId)
        } else {
            analysisProtectedEpisodes[episodeId] = count - 1
        }
    }

    /// Test/diagnostic accessor: episodes currently protected from eviction.
    func protectedEpisodeIdsForTesting() -> Set<String> {
        Set(analysisProtectedEpisodes.keys)
    }

    // MARK: - Cache Size & Eviction

    /// Updates the maximum cache size.
    func setMaxCacheSize(_ bytes: Int64) {
        maxCacheBytes = bytes
    }

    /// Returns the current total size of cached audio files.
    func currentCacheSize() throws -> Int64 {
        let fm = FileManager.default
        var total: Int64 = 0
        for dir in [completeDirectory, partialsDirectory] {
            guard let contents = try? fm.contentsOfDirectory(
                at: dir, includingPropertiesForKeys: [.fileSizeKey]
            ) else { continue }
            for fileURL in contents {
                let values = try? fileURL.resourceValues(forKeys: [.fileSizeKey])
                total += Int64(values?.fileSize ?? 0)
            }
        }
        return total
    }

    /// Evicts least-recently-used cached files until under the size limit.
    /// Never evicts episodes with active analysis.
    func evictIfNeeded() async throws {
        var currentSize = try currentCacheSize()
        guard currentSize > maxCacheBytes else { return }

        logger.info("Cache over budget: \(currentSize) / \(self.maxCacheBytes) bytes. Evicting...")

        // Build eviction candidates: completed files, sorted by last access (oldest first).
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(
            at: completeDirectory,
            includingPropertiesForKeys: [.fileSizeKey]
        ) else { return }

        var candidates: [(episodeId: String?, displayName: String, url: URL, size: Int64, lastAccess: Date)] = []
        // Build a reverse map from hashed filename → episode ID. Union
        // accessLog with protected and active sets so a file deposited
        // outside the manager (or before its accessLog entry was
        // written) can still be identified for protection checks.
        let knownEpisodeIds = Set(accessLog.keys)
            .union(analysisProtectedEpisodes.keys)
            .union(activeDownloads.keys)
            .union(bgInFlightEpisodes)
        let hashToEpisodeId: [String: String] = Dictionary(
            uniqueKeysWithValues: knownEpisodeIds.map { (Self.safeFilename(for: $0), $0) }
        )
        for fileURL in contents {
            // playhead-wrj8: never evict a `.pin` sidecar directly — it is
            // deleted alongside its audio file below. Treating it as a
            // standalone eviction candidate would strip the completeness
            // marker off a still-present audio file.
            guard fileURL.pathExtension != Self.pinExtension else { continue }
            let name = fileURL.deletingPathExtension().lastPathComponent
            let episodeId = hashToEpisodeId[name]
            guard !(episodeId.map { analysisProtectedEpisodes[$0] != nil } ?? false) else { continue }
            guard !(episodeId.map { activeDownloads.keys.contains($0) } ?? false) else { continue }
            guard !(episodeId.map { bgInFlightEpisodes.contains($0) } ?? false) else { continue }

            let values = try? fileURL.resourceValues(
                forKeys: [.fileSizeKey, .contentModificationDateKey]
            )
            let size = Int64(values?.fileSize ?? 0)
            // Fall back to file mtime — not `.distantPast` — so a
            // freshly-deposited background download whose accessLog
            // entry hasn't been written yet isn't the first victim.
            let lastAccess = episodeId.flatMap { accessLog[$0] }
                ?? values?.contentModificationDate
                ?? .distantPast
            candidates.append((episodeId, episodeId ?? name, fileURL, size, lastAccess))
        }

        // Sort: least recently accessed first.
        candidates.sort { $0.lastAccess < $1.lastAccess }

        for candidate in candidates {
            guard currentSize > maxCacheBytes else { break }

            try fm.removeItem(at: candidate.url)
            // playhead-wrj8: drop the completeness pin alongside the audio
            // so a re-download starts from a clean slate (no stale pin
            // claiming an evicted file is still complete).
            let pinURL = candidate.url
                .deletingPathExtension()
                .appendingPathExtension(Self.pinExtension)
            try? fm.removeItem(at: pinURL)
            currentSize -= candidate.size
            // Only scrub the per-episode caches when we resolved a real
            // episode id. Writing nil at the hashed-filename key would
            // be a no-op AND, worse, leave any cache entries keyed by
            // the real id (held under a different filename hash) leaked.
            if let id = candidate.episodeId {
                accessLog[id] = nil
                fingerprintCache[id] = nil
                metadataCache[id] = nil
            }
            logger.info("Evicted \(candidate.displayName): freed \(candidate.size) bytes")
        }
    }

    /// Manually clear all cached episode audio.
    func clearCache() throws {
        let fm = FileManager.default
        // Include resumeDataDirectory so a clearCache + relaunch sequence
        // doesn't resurrect phantom suspended-transfer events for episodes
        // whose audio is gone.
        for dir in [completeDirectory, partialsDirectory, resumeDataDirectory] {
            guard let contents = try? fm.contentsOfDirectory(
                at: dir, includingPropertiesForKeys: nil
            ) else { continue }
            for fileURL in contents {
                try fm.removeItem(at: fileURL)
            }
        }
        accessLog.removeAll()
        fingerprintCache.removeAll()
        metadataCache.removeAll()
        logger.info("Cache cleared")
    }

    /// Removes cached audio for a specific episode.
    func removeCache(for episodeId: String) throws {
        let fm = FileManager.default
        let complete = completeFileURL(for: episodeId)
        let partial = partialFileURL(for: episodeId)
        if fm.fileExists(atPath: complete.path) {
            try fm.removeItem(at: complete)
        }
        if fm.fileExists(atPath: partial.path) {
            try fm.removeItem(at: partial)
        }
        // playhead-wrj8: drop the completeness pin too so a later
        // re-download is treated as a fresh artifact rather than colliding
        // with a stale "complete" claim.
        deletePin(for: episodeId)
        // Symmetric blob cleanup so a future scan doesn't resurrect a
        // suspended-transfer event for an episode the user just deleted.
        try? deleteResumeData(episodeId: episodeId)
        accessLog[episodeId] = nil
        fingerprintCache[episodeId] = nil
        metadataCache[episodeId] = nil
    }

    // MARK: - Helpers

    private func touchAccess(episodeId: String) {
        accessLog[episodeId] = Date()
    }

    /// Called by the background download delegate when a transfer
    /// completes. Owns the final-placement file move (so it honors
    /// `cacheDirectory`, including custom directories injected by tests
    /// or future multi-profile hosts), synthesizes a real weak
    /// fingerprint from URL + HTTP response metadata, computes the
    /// strong fingerprint, and enqueues analysis. (playhead-24cm.1
    /// I3 + I4.)
    ///
    /// `stagedURL` points at a process-global temp file the delegate
    /// moved out of the OS-owned location during the synchronous
    /// callback. We are responsible for moving it into
    /// `completeFileURL(for:)` and cleaning up if the move fails.
    /// `originalURL` and `metadata` may be nil if the delegate could
    /// not harvest them (e.g. the task carried no HTTP response); in
    /// that case we preserve whatever weak fingerprint a prior
    /// progressive/streaming pass already cached, rather than
    /// regressing it to the empty sentinel.
    func handleBackgroundDownloadComplete(
        episodeId: String,
        stagedURL: URL,
        originalURL: URL?,
        metadata: HTTPAssetMetadata?
    ) async {
        let fm = FileManager.default

        // Cache the file extension so `completeFileURL(for:)` returns
        // the right path. Mirrors the progressive path which sets
        // `extensionCache` from `url.pathExtension` before computing
        // `completeFileURL`.
        let stagedExt = stagedURL.pathExtension
        if !stagedExt.isEmpty {
            extensionCache[episodeId] = stagedExt
        } else if let originalExt = originalURL?.pathExtension, !originalExt.isEmpty {
            extensionCache[episodeId] = originalExt
        }

        let destURL = completeFileURL(for: episodeId)
        let destDir = destURL.deletingLastPathComponent()

        // playhead-wrj8: REFUSE to overwrite an already-complete pinned
        // artifact. This is the vector that best matches the incident: a
        // background transfer (or a force-quit RESUME, which finalizes
        // through this same path) completing with a DIFFERENT DAI ad
        // stitch must NOT clobber the bytes the user already played and
        // marked ads against. Discard the staged deposit, adopt a pin for
        // the existing file if it has none, and keep the played copy.
        if let existing = servingURLIfComplete(for: episodeId) {
            try? fm.removeItem(at: stagedURL)
            if loadPin(for: episodeId) == nil,
               let size = completeFileSize(for: episodeId) {
                writePin(
                    AudioAssetPin(
                        expectedBytes: size,
                        sha256: fingerprintCache[episodeId]?.strong,
                        sourceURL: originalURL?.absoluteString,
                        etag: metadata?.etag
                    ),
                    for: episodeId
                )
            }
            touchAccess(episodeId: episodeId)
            bgInFlightEpisodes.remove(episodeId)
            try? deleteResumeData(episodeId: episodeId)
            logger.info("Background completion for \(episodeId, privacy: .public): complete pinned artifact already present — kept it, discarded re-fetch at \(existing.lastPathComponent, privacy: .public)")
            return
        }

        do {
            if !fm.fileExists(atPath: destDir.path) {
                try fm.createDirectory(at: destDir, withIntermediateDirectories: true)
            }
            if fm.fileExists(atPath: destURL.path) {
                try fm.removeItem(at: destURL)
            }
            try fm.moveItem(at: stagedURL, to: destURL)
            // playhead-h3h: stamp the freshly-deposited cached audio so
            // the protection class matches the parent directory. Files
            // moved in from the URLSession session container inherit
            // the system-default class, which is `.complete` on
            // background-session containers — that would block reads
            // during pre-first-unlock BG processing windows.
            try? fm.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: destURL.path
            )
            logger.info("Background download complete for \(episodeId)")
        } catch {
            logger.error(
                "Failed to place background download for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
            // Best-effort cleanup of the staged file; otherwise it
            // accumulates in the temp directory.
            try? fm.removeItem(at: stagedURL)
            bgInFlightEpisodes.remove(episodeId)
            return
        }

        // Synthesize the weak fingerprint from URL + HTTP metadata
        // harvested by the delegate. Mirrors what the progressive path
        // does at `performStreamingDownload`. If the delegate could
        // not harvest a URL or response, fall back to whatever a prior
        // foreground pass already populated — never overwrite a real
        // weak with the empty sentinel (playhead-24cm.1 I4).
        if let originalURL {
            let synthesizedMetadata = metadata ?? HTTPAssetMetadata(
                etag: nil, contentLength: nil, lastModified: nil
            )
            metadataCache[episodeId] = synthesizedMetadata
            let weakFP = AudioFingerprint.makeWeak(
                url: originalURL, metadata: synthesizedMetadata
            )
            fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: nil)
        }

        // Compute the strong fingerprint (full SHA-256). Logging the
        // hash failure rather than swallowing it via `try?` so support
        // triage can spot a corrupt deposit; the cache entry without
        // the strong field still carries the weak fingerprint and is
        // useful to dedup re-downloads (playhead-24cm.1 I4).
        // playhead-wrj8: pin the freshly-deposited artifact as COMPLETE
        // (actual on-disk length) so it becomes the single immutable served
        // copy. Written regardless of whether the strong-hash step below
        // succeeds, so the file can never remain "unpinned/incomplete" and
        // get re-fetched into a different stitch.
        if let size = completeFileSize(for: episodeId) {
            writePin(
                AudioAssetPin(
                    expectedBytes: size,
                    sha256: nil,
                    sourceURL: originalURL?.absoluteString,
                    etag: metadata?.etag
                ),
                for: episodeId
            )
        }

        do {
            let strongHash = try FileHasher.sha256(fileURL: destURL)
            let weakFP = fingerprintCache[episodeId]?.weak ?? ""
            fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: strongHash)
            // Backfill the strong hash into the pin now that it's computed.
            if var pin = loadPin(for: episodeId) {
                pin.sha256 = strongHash
                writePin(pin, for: episodeId)
            }
            await enqueueAnalysisIfNeeded(
                episodeId: episodeId,
                sourceFingerprint: strongHash,
                context: nil
            )
        } catch {
            logger.error(
                "Strong fingerprint hash failed for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }

        touchAccess(episodeId: episodeId)
        bgInFlightEpisodes.remove(episodeId)
        // A successful completion can resurrect a stale resume-data blob
        // from a prior failed attempt: without this delete, the next
        // cold-launch `scanForSuspendedTransfers` would emit a phantom
        // `appForceQuitRequiresRelaunch` event for an episode that's
        // already fully downloaded.
        try? deleteResumeData(episodeId: episodeId)
        // Background pre-cache deposits weren't subject to LRU eviction;
        // without this, the cache could grow past `maxCacheBytes` until
        // the next foreground download. Best-effort: a failure here just
        // defers cleanup to the next eviction trigger.
        try? await evictIfNeeded()
    }

    /// Rebuilds the LRU access log from file modification dates.
    private func rebuildAccessLog() throws {
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(
            at: completeDirectory,
            includingPropertiesForKeys: [.contentModificationDateKey]
        ) else { return }

        for fileURL in contents {
            let name = fileURL.deletingPathExtension().lastPathComponent
            let values = try? fileURL.resourceValues(
                forKeys: [.contentModificationDateKey]
            )
            accessLog[name] = values?.contentModificationDate ?? Date.distantPast
        }
    }

}

// MARK: - DownloadProviding Conformance

extension DownloadManager: DownloadProviding {}

// MARK: - Progress Snapshot (playhead-btoa.2)

extension DownloadManager {
    /// Snapshot of the per-episode foreground download fraction for
    /// in-flight transfers. Episodes not currently downloading are absent
    /// from the map. Computed from the same `ForegroundAssistProgress`
    /// state that drives `progressUpdates()`. Per-call: O(N) over active
    /// downloads (typically tiny — single-digit episodes).
    ///
    /// Entries with `totalBytes == 0` (size-unknown transfers) are
    /// skipped to avoid divide-by-zero. Background-session transfers are
    /// out of scope here — see `bgInFlightEpisodes` for that lane.
    ///
    /// Used by the Activity provider once per refresh tick to populate
    /// `ActivityEpisodeInput.downloadFraction`.
    func progressSnapshot() -> [String: Double] {
        var result: [String: Double] = [:]
        for (episodeId, progress) in foregroundAssistProgress {
            guard progress.totalBytes > 0 else { continue }
            result[episodeId] =
                Double(progress.bytesWritten) / Double(progress.totalBytes)
        }
        return result
    }
}

// MARK: - Shared Reference Plumbing (playhead-24cm)

extension DownloadManager {
    /// Non-owning shared reference used by `PlayheadAppDelegate` to
    /// reach the live download manager during background wake events.
    /// Stored weakly so tests and non-app hosts don't keep a manager
    /// alive. The `PlayheadRuntime` registers its manager at boot.
    ///
    /// This is intentionally minimal — we don't expose a service
    /// locator, just a single slot the app delegate can consult.
    nonisolated(unsafe) private static var _shared: DownloadManager?

    /// Registers `manager` as the app-wide shared DownloadManager for
    /// background session wake-up routing. Pass `nil` to clear the slot
    /// — useful for test teardown so a later test starting in a fresh
    /// state does not accidentally observe a previous test's manager.
    @MainActor
    static func registerShared(_ manager: DownloadManager?) {
        _shared = manager
    }

    /// Current shared DownloadManager, if one has been registered.
    static var shared: DownloadManager? {
        _shared
    }

    /// Non-owning reference to the app delegate. Set by
    /// `PlayheadApp.registerAppDelegate(_:)` at boot so the URLSession
    /// finish-events callback can reach the pending-handler map.
    nonisolated(unsafe) private static var _appDelegate: PlayheadAppDelegate?

    @MainActor
    static func registerAppDelegate(_ delegate: PlayheadAppDelegate) {
        _appDelegate = delegate
    }

    static var appDelegate: PlayheadAppDelegate? {
        _appDelegate
    }
}

// MARK: - UserPreferencesSnapshot

/// Snapshot of the subset of `UserPreferences` that download manager
/// background configuration needs to read at URLSession-construction
/// time (which may be off-main, synchronous, and before SwiftData is
/// ready). Persisted in UserDefaults by the settings UI. See
/// `UserPreferences.allowsCellular` for the source of truth.
struct UserPreferencesSnapshot: Sendable {
    var allowsCellular: Bool
    /// playhead-jzik: mirrored copy of `UserPreferences.episodeSummariesEnabled`
    /// so the off-main-actor `EpisodeSummaryBackfillCoordinator` can read
    /// the toggle without a SwiftData hop. Defaults to `true` to match the
    /// SwiftData default; the Settings toggle calls
    /// `save(episodeSummariesEnabled:)` to keep the slot in sync.
    var episodeSummariesEnabled: Bool

    static let defaultsKey = "UserPreferencesSnapshot.allowsCellular"
    static let episodeSummariesDefaultsKey = "UserPreferencesSnapshot.episodeSummariesEnabled"

    static var current: UserPreferencesSnapshot {
        let allows = UserDefaults.standard.object(forKey: defaultsKey) as? Bool ?? true
        let summaries = UserDefaults.standard.object(forKey: episodeSummariesDefaultsKey) as? Bool ?? true
        return UserPreferencesSnapshot(
            allowsCellular: allows,
            episodeSummariesEnabled: summaries
        )
    }

    static func save(allowsCellular: Bool) {
        UserDefaults.standard.set(allowsCellular, forKey: defaultsKey)
    }

    static func save(episodeSummariesEnabled: Bool) {
        UserDefaults.standard.set(episodeSummariesEnabled, forKey: episodeSummariesDefaultsKey)
    }
}

// MARK: - EpisodeDownloadDelegate

/// URLSession delegate for handling background episode download events.
///
/// Serves both the 24cm-split `interactive`/`maintenance` sessions and the
/// legacy `com.playhead.episode-downloads` session during the rollout
/// window. The session identifier is pulled from
/// `session.configuration.identifier` on each callback so downstream
/// observers can tell the lanes apart without the delegate tracking
/// per-lane state.
final class EpisodeDownloadDelegate: NSObject, URLSessionDownloadDelegate, Sendable {
    private let logger = Logger(subsystem: "com.playhead", category: "EpisodeDownload")

    /// Callback fired on the delegate queue once a completed background
    /// transfer has been staged into a process-global temp directory.
    /// Carries the episode ID, the staged file URL (caller takes
    /// ownership and is expected to move it into the cache), the
    /// original request URL, and HTTP response metadata harvested for
    /// weak-fingerprint synthesis (playhead-24cm.1 I3 + I4).
    ///
    /// Same init-once / read-many invariant as `onResumeDataHarvested`.
    nonisolated(unsafe) var onBackgroundDownloadStaged: (
        (String, URL, URL?, HTTPAssetMetadata?) -> Void
    )?

    /// playhead-3xtw: fired on the delegate queue for each byte-progress
    /// callback of a background (pre-cache / on-demand) transfer, carrying
    /// `(episodeId, totalBytesWritten, totalBytesExpectedToWrite)`. The
    /// background session previously only LOGGED progress — so the
    /// download zone of the per-episode prepare control (and the Activity
    /// screen) could not observe an in-flight background transfer. Wiring
    /// this hook into `DownloadManager.broadcastProgress` closes that gap
    /// (the "44h1 will add" hook the didWriteData comment anticipated).
    /// Same init-once / read-many invariant as `onBackgroundDownloadStaged`.
    nonisolated(unsafe) var onBackgroundDownloadProgress: (
        (String, Int64, Int64) -> Void
    )?

    /// Invoked when the URLSession has drained all pending events after
    /// a background wake. Forwards the session's identifier so the app
    /// delegate can match it against its pending completion-handler map.
    nonisolated(unsafe) var onUrlSessionDidFinishEvents: ((String) -> Void)?

    /// Invoked when a terminated transfer's didCompleteWithError callback
    /// carries an `NSURLSessionDownloadTaskResumeData` blob in its
    /// userInfo. Routes the blob back into DownloadManager's
    /// `resumeDataDirectory` via `persistResumeData(episodeId:data:)`
    /// (playhead-g2wq) so the next cold-launch `scanForSuspendedTransfers`
    /// pass can see it. Without this callback the resume-data directory
    /// stays empty in production and the hyht follow-up UX never fires.
    ///
    /// Thread-safety invariant: init-once / read-many. The property is
    /// assigned exactly once by `DownloadManager.init(...)` during actor
    /// construction (before the delegate is handed to any URLSession),
    /// and is only READ thereafter from URLSession's delegate queue.
    /// Mutation after init is forbidden — the `nonisolated(unsafe)`
    /// qualifier opts out of Swift concurrency checking and relies on
    /// this invariant for safety. Same contract as
    /// `onBackgroundDownloadStaged` and `onUrlSessionDidFinishEvents`
    /// above.
    /// playhead-wrj8: widened to also carry the terminated transfer's
    /// source URL and HTTP validator (ETag / Content-Length), harvested
    /// from the task on the delegate queue. The resume path persists these
    /// alongside the blob so a later `downloadTask(withResumeData:)` can be
    /// validated against the live server — a rotated DAI enclosure whose
    /// ETag/length no longer matches must NOT be spliced into the played
    /// file (it would land a different ad stitch).
    nonisolated(unsafe) var onResumeDataHarvested: ((String, Data, URL?, HTTPAssetMetadata?) -> Void)?

    /// WorkJournal recorder for finalized / failed events. Defaults to
    /// `NoopWorkJournalRecorder`; the real implementation is injected at
    /// init via `DownloadManager(workJournalRecorder:)` and assigned
    /// here in one shot by `DownloadManager.init(...)` before the
    /// delegate is handed to any URLSession (same init-once contract as
    /// `onBackgroundDownloadStaged`). Mutation after init is forbidden.
    nonisolated(unsafe) var workJournal: WorkJournalRecording = NoopWorkJournalRecorder()

    func urlSession(
        _ session: URLSession,
        downloadTask: URLSessionDownloadTask,
        didFinishDownloadingTo location: URL
    ) {
        guard let episodeId = downloadTask.taskDescription else {
            logger.warning("Background download finished but no episode ID set")
            return
        }

        // Derive extension from the original request URL, falling back to mp3.
        let originalURL = downloadTask.originalRequest?.url
        let ext: String = {
            let raw = originalURL?.pathExtension ?? ""
            return raw.isEmpty ? "mp3" : raw
        }()
        let filename = DownloadManager.safeFilename(for: episodeId)

        // Harvest HTTP metadata for the weak fingerprint here, while the
        // delegate-queue stack still has access to the task. The actor
        // hop below cannot read `downloadTask.response` because
        // URLSessionDownloadTask is non-Sendable.
        let httpMetadata: HTTPAssetMetadata? = {
            guard let response = downloadTask.response as? HTTPURLResponse else {
                return nil
            }
            let reportedLength = response.expectedContentLength
            return HTTPAssetMetadata(
                etag: response.value(forHTTPHeaderField: "ETag"),
                contentLength: reportedLength > 0 ? reportedLength : nil,
                lastModified: response.value(forHTTPHeaderField: "Last-Modified")
            )
        }()

        // playhead-24cm.1 (I3): stage the file into a process-global temp
        // directory synchronously on the delegate queue. The OS-provided
        // `location` URL is only valid during this callback; once we
        // return, the file may be deleted. Staging into a stable temp
        // path lets the actor — which knows the real `cacheDirectory`,
        // even when it's not the default — perform the final placement.
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStaging", isDirectory: true)
        let stagedURL = stagingDir.appendingPathComponent("\(filename).\(ext)")

        let fm = FileManager.default
        do {
            if !fm.fileExists(atPath: stagingDir.path) {
                try fm.createDirectory(at: stagingDir, withIntermediateDirectories: true)
            }
            if fm.fileExists(atPath: stagedURL.path) {
                try fm.removeItem(at: stagedURL)
            }
            try fm.moveItem(at: location, to: stagedURL)
            logger.info("Background download staged for \(episodeId)")

            // Hand the staged file to the actor, which will move it into
            // the correct `cacheDirectory`-relative `complete/` directory
            // and populate fingerprint state with a real weak fingerprint
            // (playhead-24cm.1 I3 + I4).
            onBackgroundDownloadStaged?(episodeId, stagedURL, originalURL, httpMetadata)

            // Emit finalized event for WorkJournal (playhead-uzdq).
            let recorder = workJournal
            Task {
                await recorder.recordFinalized(episodeId: episodeId)
            }
        } catch {
            logger.error("Failed to stage background download for \(episodeId): \(error.localizedDescription)")
            let recorder = workJournal
            let errorDescription = error.localizedDescription
            Task {
                // Background URLSession callbacks land outside the app's
                // foreground lifecycle; we don't have a wall-clock slice
                // start timestamp here, so sliceDurationMs is 0. The byte
                // count is what URLSession reported for the transfer
                // itself (captured from the downloaded file at `location`).
                let bytesProcessed = (try? FileManager.default
                    .attributesOfItem(atPath: location.path)[.size] as? Int) ?? 0
                let metadata = await SliceCompletionInstrumentation.recordFailed(
                    cause: .pipelineError,
                    deviceClass: DeviceClass.detect(),
                    sliceDurationMs: 0,
                    bytesProcessed: bytesProcessed,
                    shardsCompleted: 0,
                    extras: [
                        "stage": "downloadManager.didFinishDownloadingTo",
                        "error": errorDescription,
                    ]
                )
                await recorder.recordFailed(
                    episodeId: episodeId,
                    cause: .pipelineError,
                    metadataJSON: metadata.encodeJSON()
                )
            }
        }
    }

    func urlSession(
        _ session: URLSession,
        downloadTask: URLSessionDownloadTask,
        didWriteData bytesWritten: Int64,
        totalBytesWritten: Int64,
        totalBytesExpectedToWrite: Int64
    ) {
        let progress = totalBytesExpectedToWrite > 0
            ? Double(totalBytesWritten) / Double(totalBytesExpectedToWrite)
            : 0
        let episodeId = downloadTask.taskDescription ?? "<missing-task-description>"
        logger.debug("Episode \(episodeId, privacy: .public) download: \(String(format: "%.1f", progress * 100))%")
        // playhead-3xtw: emit through the DownloadManager broadcast
        // surface so `progressUpdates()` / `progressSnapshot()` reflect
        // in-flight BACKGROUND transfers (previously this callback only
        // logged). Guard the missing-taskDescription sentinel so we never
        // key progress under a bogus episode id.
        if downloadTask.taskDescription != nil {
            onBackgroundDownloadProgress?(episodeId, totalBytesWritten, totalBytesExpectedToWrite)
        }
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: (any Error)?
    ) {
        guard let error else {
            // Success path is handled by didFinishDownloadingTo. Nothing
            // to do here — don't double-emit a finalized event.
            return
        }
        let episodeId = task.taskDescription ?? "<missing-task-description>"
        let cause = InternalMissCause.fromTaskError(error)
        logger.error("Episode \(episodeId, privacy: .public) download failed (\(cause.rawValue)): \(error.localizedDescription)")

        // playhead-g2wq: harvest OS-produced resume-data BEFORE emitting
        // `recordFailed`. URLSession stashes the resume blob in
        // `NSError.userInfo[NSURLSessionDownloadTaskResumeData]` whenever
        // the terminated transfer is eligible for `downloadTask(withResumeData:)`
        // replay. Persisting it here is what populates `resumeDataDirectory`
        // so `scanForSuspendedTransfers` can find suspended transfers on
        // the next cold launch. If the error carries no blob (server-side
        // failure, name resolution error, etc.) we skip the harvest and
        // fall through to `recordFailed` unchanged.
        //
        // Lifecycle note: the callback closure hops onto an async Task
        // to write to disk. That hop is safe because this delegate
        // callback only fires while the app is alive — either during
        // normal runtime or during background-session rehydration after
        // cold relaunch (the hyht force-quit flow: OS suspends the
        // in-flight task at force-quit WITHOUT delivering
        // didCompleteWithError; on the next cold launch URLSession
        // re-attaches the delegate and drains pending events, at which
        // point this callback fires with the process alive and running
        // `scanForSuspendedTransfers`). The FileManager write is
        // therefore guaranteed to complete before the process exits.
        let nsError = error as NSError
        // Only URLSession populates `NSURLSessionDownloadTaskResumeData`
        // in `NSURLErrorDomain` errors. Filtering on domain prevents a
        // foreign-domain error that coincidentally carries the key from
        // being persisted as if it were a valid resume blob.
        if nsError.domain == NSURLErrorDomain,
           let resumeData = nsError.userInfo[NSURLSessionDownloadTaskResumeData] as? Data,
           !resumeData.isEmpty {
            // playhead-wrj8: harvest the source URL + HTTP validator while
            // the delegate-queue stack still has the task (it is
            // non-Sendable and cannot cross the actor hop below).
            let sourceURL = task.originalRequest?.url ?? task.currentRequest?.url
            let harvestedMetadata: HTTPAssetMetadata? = {
                guard let response = task.response as? HTTPURLResponse else { return nil }
                let len = response.expectedContentLength
                return HTTPAssetMetadata(
                    etag: response.value(forHTTPHeaderField: "ETag"),
                    contentLength: len > 0 ? len : nil,
                    lastModified: response.value(forHTTPHeaderField: "Last-Modified")
                )
            }()
            onResumeDataHarvested?(episodeId, resumeData, sourceURL, harvestedMetadata)
        }

        let recorder = workJournal
        let bytesReceived = Int(task.countOfBytesReceived)
        let errorDescription = error.localizedDescription
        Task {
            // Background URLSession does not expose a wall-clock slice
            // start to the delegate, so sliceDurationMs is 0 here. This
            // is NOT skipping the metadata blob — every acquired→terminal
            // transition emits metadata per the 1nl6 spec; we just record
            // the fields we have from URLSession's own accounting.
            let metadata = await SliceCompletionInstrumentation.recordFailed(
                cause: cause,
                deviceClass: DeviceClass.detect(),
                sliceDurationMs: 0,
                bytesProcessed: bytesReceived,
                shardsCompleted: 0,
                extras: [
                    "stage": "downloadManager.didCompleteWithError",
                    "error": errorDescription,
                ]
            )
            await recorder.recordFailed(
                episodeId: episodeId,
                cause: cause,
                metadataJSON: metadata.encodeJSON()
            )
        }
    }

    /// Called by URLSession after all pending background events have
    /// been delivered to this delegate. Forwards the identifier so the
    /// `PlayheadAppDelegate` can invoke its stored completion handler.
    func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
        guard let identifier = session.configuration.identifier else { return }
        onUrlSessionDidFinishEvents?(identifier)
    }
}
