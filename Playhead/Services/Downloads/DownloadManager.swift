// DownloadManager.swift
// Background download management for podcast episode audio.
// Handles progressive caching during streaming playback, background
// transfers for pre-caching, resume after interruption, LRU eviction,
// and asset fingerprinting for the analysis pipeline.

import CryptoKit
import Foundation
import OSLog
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

    // MARK: - State

    /// Active download tasks keyed by episode ID.
    private var activeDownloads: [String: Task<URL, Error>] = [:]

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

    /// Background URL session for pre-caching.
    private var _backgroundSession: URLSession?

    /// Delegate for background session.
    private let sessionDelegate: EpisodeDownloadDelegate

    // MARK: - Streams

    private let progressContinuation: AsyncStream<DownloadProgress>.Continuation
    /// Subscribe for download progress updates.
    nonisolated let progressStream: AsyncStream<DownloadProgress>

    // MARK: - Init

    init(
        cacheDirectory: URL? = nil,
        maxCacheBytes: Int64 = DownloadManager.defaultMaxCacheBytes
    ) {
        let root = cacheDirectory ?? Self.defaultCacheDirectory()
        self.cacheDirectory = root
        self.partialsDirectory = root.appendingPathComponent("partials", isDirectory: true)
        self.completeDirectory = root.appendingPathComponent("complete", isDirectory: true)
        self.maxCacheBytes = maxCacheBytes
        self.sessionDelegate = EpisodeDownloadDelegate()

        let (stream, continuation) = AsyncStream<DownloadProgress>.makeStream()
        self.progressStream = stream
        self.progressContinuation = continuation
    }

    /// Wire up the analysis scheduler so downloads automatically enqueue jobs.
    func setAnalysisWorkScheduler(_ scheduler: AnalysisWorkScheduler) {
        self.analysisWorkScheduler = scheduler
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
        for dir in [cacheDirectory, partialsDirectory, completeDirectory] {
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

    private func backgroundSession() -> URLSession {
        if let existing = _backgroundSession { return existing }
        let config = URLSessionConfiguration.background(
            withIdentifier: "com.playhead.episode-downloads"
        )
        config.isDiscretionary = false
        config.sessionSendsLaunchEvents = true
        config.allowsCellularAccess = true
        let session = URLSession(
            configuration: config,
            delegate: sessionDelegate,
            delegateQueue: nil
        )
        sessionDelegate.onDownloadComplete = { [weak self] episodeId, fileURL in
            Task { await self?.handleBackgroundDownloadComplete(episodeId: episodeId, fileURL: fileURL) }
        }
        _backgroundSession = session
        return session
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

        // Evict if over budget.
        try await evictIfNeeded()

        progressContinuation.yield(DownloadProgress(
            episodeId: episodeId,
            bytesWritten: downloaded,
            totalBytes: downloaded
        ))

        return completeURL
    }

    // MARK: - Streaming Download (Play While Downloading)

    /// Minimum bytes before signaling playback can start.
    /// ~60s at 256 kbps = ~1.9 MB. Round up to 2 MB.
    static let defaultPlayableThreshold: Int64 = 2 * 1024 * 1024

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

                            await self?.progressContinuation.yield(DownloadProgress(
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

                    await self?.touchAccess(episodeId: capturedEpisodeId)
                    try await self?.evictIfNeeded()

                    await self?.progressContinuation.yield(DownloadProgress(
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

// MARK: - EpisodeDownloadDelegate

/// URLSession delegate for handling background episode download events.
final class EpisodeDownloadDelegate: NSObject, URLSessionDownloadDelegate, Sendable {
    private let logger = Logger(subsystem: "com.playhead", category: "EpisodeDownload")

    /// Callback for completed downloads: (episodeId, fileURL) -> Void.
    nonisolated(unsafe) var onDownloadComplete: ((String, URL) -> Void)?

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

        // Move the file to the complete directory.
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
        } catch {
            logger.error("Failed to move background download for \(episodeId): \(error.localizedDescription)")
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
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: (any Error)?
    ) {
        if let error {
            let episodeId = task.taskDescription ?? "unknown"
            logger.error("Episode \(episodeId) download failed: \(error.localizedDescription)")
        }
    }
}
