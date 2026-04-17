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
struct DownloadContext: Sendable {
    let podcastId: String?
    let isExplicitDownload: Bool
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
/// episode audio. Model asset downloads are handled by ``AssetProvider``.
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

    /// Metadata cache: episode ID -> HTTP metadata from last response.
    private var metadataCache: [String: HTTPAssetMetadata] = [:]

    /// LRU tracking: episode ID -> last access time.
    private var accessLog: [String: Date] = [:]

    /// Episodes with active/incomplete analysis (protected from eviction).
    private var analysisProtectedEpisodes: Set<String> = []

    /// Fingerprint cache: episode ID -> computed fingerprint.
    private var fingerprintCache: [String: AudioFingerprint] = [:]

    /// Cached file extension per episode ID (e.g. "mp3", "m4a").
    private var extensionCache: [String: String] = [:]

    /// Optional scheduler for enqueuing pre-analysis jobs after download.
    private var analysisWorkScheduler: AnalysisWorkScheduler?

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

    /// playhead-44h1 (fix): inject a `BackgroundTaskScheduling` so
    /// tests can observe the `BGContinuedProcessingTaskRequest`
    /// submission path without touching `BGTaskScheduler.shared`.
    /// Production leaves the default `.shared` scheduler in place.
    func setBackgroundTaskSchedulerForTesting(_ scheduler: any BackgroundTaskScheduling) {
        self.backgroundTaskScheduler = scheduler
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
    private func submitContinuedProcessingRequest(for episodeId: String) {
        let identifier = "\(BackgroundTaskID.continuedProcessing).\(episodeId)"
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

        // Wire the onDownloadComplete callback once per session.
        sessionDelegate.onDownloadComplete = { [manager = self] episodeId, fileURL in
            Task {
                await manager.handleBackgroundDownloadComplete(
                    episodeId: episodeId,
                    fileURL: fileURL
                )
            }
        }

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

    /// Overridable for tests: flip the 24cm feature flag in-process.
    func setUseDualBackgroundSessionsForTesting(_ value: Bool) {
        self.useDualBackgroundSessions = value
    }

    /// Overridable for tests / uzdq integration: replace the work-journal
    /// recorder after construction.
    func setWorkJournalRecorder(_ recorder: WorkJournalRecording) {
        self.workJournalRecorder = recorder
        self.sessionDelegate.workJournal = recorder
    }

    // MARK: - Test hooks (internal)

    func backgroundSessionForTesting(role: BackgroundSessionRole) -> URLSession {
        backgroundSession(for: role)
    }

    func instantiatedSessionIdentifiersForTesting() -> Set<String> {
        Set(_sessionsByRole.values.compactMap { $0.configuration.identifier })
    }

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

        // Already fully cached?
        let completeURL = completeFileURL(for: episodeId)
        if FileManager.default.fileExists(atPath: completeURL.path) {
            touchAccess(episodeId: episodeId)
            return completeURL
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

        // Download to a temporary file (handled efficiently by URLSession).
        let (tempURL, response) = try await URLSession.shared.download(for: request)
        // Clean up temp file on any error path.
        defer { try? fm.removeItem(at: tempURL) }

        guard let httpResponse = response as? HTTPURLResponse,
              (200...299).contains(httpResponse.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            throw DownloadManagerError.downloadFailed(episodeId, "HTTP \(code)")
        }

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

        // Move temp -> complete first, then hash from final location.
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

        // Enqueue pre-analysis if scheduler is wired up.
        if let scheduler = analysisWorkScheduler {
            await scheduler.enqueue(
                episodeId: episodeId,
                podcastId: context?.podcastId,
                downloadId: episodeId,
                sourceFingerprint: strongHash,
                isExplicitDownload: context?.isExplicitDownload ?? false
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
        if FileManager.default.fileExists(atPath: completeURL.path) {
            touchAccess(episodeId: episodeId)
            let uti = Self.utiForExtension(sourceExt)
            // File size is the total for a complete file.
            let attrs = try? FileManager.default.attributesOfItem(atPath: completeURL.path)
            let size = (attrs?[.size] as? Int64)
            return StreamingDownloadResult(fileURL: completeURL, totalBytes: size, contentType: uti, downloadComplete: {})
        }

        // Write directly to the final location so AVPlayer can read it.
        let fm = FileManager.default
        if fm.fileExists(atPath: completeURL.path) {
            try fm.removeItem(at: completeURL)
        }
        fm.createFile(atPath: completeURL.path, contents: nil)
        let fileHandle = try FileHandle(forWritingTo: completeURL)

        let request = URLRequest(url: url)
        let (bytes, response) = try await URLSession.shared.bytes(for: request)

        guard let httpResponse = response as? HTTPURLResponse,
              (200...299).contains(httpResponse.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            try? fileHandle.close()
            try? fm.removeItem(at: completeURL)
            throw DownloadManagerError.downloadFailed(episodeId, "HTTP \(code)")
        }

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

        let signalURL = completeURL
        let threshold = min(playableThreshold, totalContentLength ?? playableThreshold)
        let audioUTI = Self.utiForExtension(sourceExt)

        // Completion continuation — signaled when the full file is written.
        let completionStream = AsyncStream<Result<Void, Error>>.makeStream()

        // Playback-ready continuation — signaled when threshold is reached.
        let result: StreamingDownloadResult = try await withCheckedThrowingContinuation { continuation in
            let capturedLogger = self.logger
            let capturedEpisodeId = episodeId
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
                    if let strongHash = try? FileHasher.sha256(fileURL: signalURL) {
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
            isExplicitDownload: context?.isExplicitDownload ?? false
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

        let session = backgroundSession()
        let task = session.downloadTask(with: url)
        task.taskDescription = episodeId
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
    func cachedFileURL(for episodeId: String) -> URL? {
        let url = completeFileURL(for: episodeId)
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        touchAccess(episodeId: episodeId)
        return url
    }

    /// Returns true if the episode audio is fully cached on disk.
    func isCached(episodeId: String) -> Bool {
        FileManager.default.fileExists(atPath: completeFileURL(for: episodeId).path)
    }

    /// Returns the set of episode IDs that have fully-downloaded cached audio.
    /// Scans the complete directory and reverse-maps filenames back to episode IDs
    /// using the access log (which tracks all episodes that have been downloaded).
    func allCachedEpisodeIds() -> Set<String> {
        Set(accessLog.keys.filter { isCached(episodeId: $0) })
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

    /// Marks an episode as having active analysis, protecting it from eviction.
    func protectForAnalysis(episodeId: String) {
        analysisProtectedEpisodes.insert(episodeId)
    }

    /// Removes analysis protection, allowing the episode to be evicted.
    func unprotectFromAnalysis(episodeId: String) {
        analysisProtectedEpisodes.remove(episodeId)
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

        var candidates: [(episodeId: String, url: URL, size: Int64, lastAccess: Date)] = []
        // Build a reverse map from hashed filename → episode ID using the access log,
        // so we can check analysis protection correctly.
        let hashToEpisodeId: [String: String] = Dictionary(
            uniqueKeysWithValues: accessLog.keys.map { (Self.safeFilename(for: $0), $0) }
        )
        for fileURL in contents {
            let name = fileURL.deletingPathExtension().lastPathComponent
            let episodeId = hashToEpisodeId[name]
            guard !(episodeId.map { analysisProtectedEpisodes.contains($0) } ?? false) else { continue }
            guard !(episodeId.map { activeDownloads.keys.contains($0) } ?? false) else { continue }

            let values = try? fileURL.resourceValues(forKeys: [.fileSizeKey])
            let size = Int64(values?.fileSize ?? 0)
            let lastAccess = episodeId.flatMap { accessLog[$0] } ?? .distantPast
            candidates.append((episodeId ?? name, fileURL, size, lastAccess))
        }

        // Sort: least recently accessed first.
        candidates.sort { $0.lastAccess < $1.lastAccess }

        for candidate in candidates {
            guard currentSize > maxCacheBytes else { break }

            try fm.removeItem(at: candidate.url)
            currentSize -= candidate.size
            accessLog[candidate.episodeId] = nil
            fingerprintCache[candidate.episodeId] = nil
            metadataCache[candidate.episodeId] = nil
            logger.info("Evicted \(candidate.episodeId): freed \(candidate.size) bytes")
        }
    }

    /// Manually clear all cached episode audio.
    func clearCache() throws {
        let fm = FileManager.default
        for dir in [completeDirectory, partialsDirectory] {
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
        accessLog[episodeId] = nil
        fingerprintCache[episodeId] = nil
        metadataCache[episodeId] = nil
    }

    // MARK: - Helpers

    private func touchAccess(episodeId: String) {
        accessLog[episodeId] = Date()
    }

    /// Called by the background download delegate when a transfer completes.
    /// Computes a strong fingerprint and enqueues analysis.
    func handleBackgroundDownloadComplete(episodeId: String, fileURL: URL) async {
        if let strongHash = try? FileHasher.sha256(fileURL: fileURL) {
            fingerprintCache[episodeId] = AudioFingerprint(weak: "", strong: strongHash)
            await enqueueAnalysisIfNeeded(
                episodeId: episodeId,
                sourceFingerprint: strongHash,
                context: nil
            )
        }
        touchAccess(episodeId: episodeId)
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

    static let defaultsKey = "UserPreferencesSnapshot.allowsCellular"

    static var current: UserPreferencesSnapshot {
        let allows = UserDefaults.standard.object(forKey: defaultsKey) as? Bool ?? true
        return UserPreferencesSnapshot(allowsCellular: allows)
    }

    static func save(allowsCellular: Bool) {
        UserDefaults.standard.set(allowsCellular, forKey: defaultsKey)
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

    /// Callback for completed downloads: (episodeId, fileURL) -> Void.
    nonisolated(unsafe) var onDownloadComplete: ((String, URL) -> Void)?

    /// Invoked when the URLSession has drained all pending events after
    /// a background wake. Forwards the session's identifier so the app
    /// delegate can match it against its pending completion-handler map.
    nonisolated(unsafe) var onUrlSessionDidFinishEvents: ((String) -> Void)?

    /// WorkJournal recorder for finalized / failed events. Swapped from
    /// `NoopWorkJournalRecorder` to the real implementation by
    /// `DownloadManager.setWorkJournalRecorder(_:)` once playhead-uzdq
    /// is wired up.
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
        let ext: String = {
            let raw = downloadTask.originalRequest?.url?.pathExtension ?? ""
            return raw.isEmpty ? "mp3" : raw
        }()
        let filename = DownloadManager.safeFilename(for: episodeId)

        // Move the file to the complete directory. Note: StorageBudget
        // (playhead-h7r) placement lands as the media artifact class —
        // for now we call into the existing complete/ directory which
        // is the media class on disk.
        let cacheDir = DownloadManager.defaultCacheDirectory()
            .appendingPathComponent("complete", isDirectory: true)
        let destURL = cacheDir.appendingPathComponent("\(filename).\(ext)")

        let fm = FileManager.default
        do {
            // Ensure the complete directory exists — the delegate may fire
            // after a cold launch before bootstrap() has been called.
            if !fm.fileExists(atPath: cacheDir.path) {
                try fm.createDirectory(at: cacheDir, withIntermediateDirectories: true)
            }
            if fm.fileExists(atPath: destURL.path) {
                try fm.removeItem(at: destURL)
            }
            try fm.moveItem(at: location, to: destURL)
            logger.info("Background download complete for \(episodeId)")
            onDownloadComplete?(episodeId, destURL)

            // Emit finalized event for WorkJournal (playhead-uzdq).
            let recorder = workJournal
            Task {
                await recorder.recordFinalized(episodeId: episodeId)
            }
        } catch {
            logger.error("Failed to move background download for \(episodeId): \(error.localizedDescription)")
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
        let episodeId = downloadTask.taskDescription ?? "unknown"
        logger.debug("Episode \(episodeId) download: \(String(format: "%.1f", progress * 100))%")
        // playhead-44h1 owns the Live Activity update. For 24cm we log
        // progress and emit through the existing DownloadManager
        // broadcast surface downstream via onDownloadProgress hooks that
        // 44h1 will add.
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
        let episodeId = task.taskDescription ?? "unknown"
        let cause = InternalMissCause.fromTaskError(error)
        logger.error("Episode \(episodeId) download failed (\(cause.rawValue)): \(error.localizedDescription)")
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
