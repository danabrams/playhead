// AssetProvider.swift
// Handles downloading, SHA-256 verification, staging, and atomic
// promotion of ML model files. Supports resume of interrupted
// downloads and rollback of bad model versions.

import Foundation
import OSLog
import CryptoKit

// MARK: - AssetProvider

/// Actor responsible for the full lifecycle of model assets:
/// download -> verify -> stage -> promote (or rollback).
///
/// Download strategy:
/// 1. Fast-path ASR model first (unblocks hot-path analysis).
/// 2. Final-path ASR + classifier deferred until fast-path ready.
/// 3. Uses URLSession background transfers for resilience.
/// 4. Interrupted downloads resume via HTTP Range headers.
actor AssetProvider {
    private let logger = Logger(subsystem: "com.playhead", category: "AssetProvider")

    /// The inventory this provider updates as models move through states.
    private let inventory: ModelInventory

    /// Lazy-initialized background URLSession.
    private var _session: URLSession?

    /// Tracks active download tasks by model ID.
    private var activeDownloads: [String: URLSessionDownloadTask] = [:]

    /// Delegate to handle background download events.
    private let sessionDelegate: AssetDownloadDelegate

    // MARK: Lifecycle

    init(inventory: ModelInventory) {
        self.inventory = inventory
        self.sessionDelegate = AssetDownloadDelegate()
    }

    private func urlSession() -> URLSession {
        if let existing = _session { return existing }
        let config = URLSessionConfiguration.background(
            withIdentifier: "com.playhead.model-downloads"
        )
        config.isDiscretionary = false
        config.sessionSendsLaunchEvents = true
        config.allowsCellularAccess = true
        let session = URLSession(
            configuration: config,
            delegate: sessionDelegate,
            delegateQueue: nil
        )
        _session = session
        return session
    }

    // MARK: - Download Orchestration

    /// Kicks off the prioritized download sequence:
    /// 1. Fast-path ASR (highest priority, unblocks analysis)
    /// 2. Then remaining models by priority
    func beginPrioritizedDownloads() async throws {
        // Always download fast-path ASR first.
        let fastASR = await inventory.fastPathASRIfMissing()
        if let entry = fastASR {
            logger.info("Starting priority download: fast-path ASR (\(entry.id))")
            try await download(entry: entry)
        }

        // Then download remaining missing models by priority.
        let remaining = await inventory.missingModels()
        for entry in remaining {
            logger.info("Starting deferred download: \(entry.id)")
            try await download(entry: entry)
        }
    }

    /// Downloads a single model entry. Resumes if a partial file exists.
    func download(entry: ModelEntry) async throws {
        let downloadsDir = inventory.downloadsDirectory
        let partialURL = downloadsDir.appendingPathComponent("\(entry.id).partial")

        await inventory.updateDownloadProgress(modelId: entry.id, progress: 0)

        var request = URLRequest(url: entry.downloadURL)

        // Resume support: if a partial file exists, request remaining bytes.
        let fm = FileManager.default
        if fm.fileExists(atPath: partialURL.path),
           let attrs = try? fm.attributesOfItem(atPath: partialURL.path),
           let existingSize = attrs[.size] as? Int64, existingSize > 0 {
            request.setValue("bytes=\(existingSize)-", forHTTPHeaderField: "Range")
            logger.info("Resuming download for \(entry.id) from byte \(existingSize)")
        }

        // Use async bytes for streaming download with progress tracking.
        let (asyncBytes, response) = try await URLSession.shared.bytes(for: request)

        guard let httpResponse = response as? HTTPURLResponse,
              (200...299).contains(httpResponse.statusCode) || httpResponse.statusCode == 206 else {
            throw AssetProviderError.downloadFailed(
                entry.id,
                "HTTP \((response as? HTTPURLResponse)?.statusCode ?? 0)"
            )
        }

        let totalSize = entry.compressedSizeBytes
        let fileHandle: FileHandle

        if httpResponse.statusCode == 206 {
            // Appending to existing partial file.
            fileHandle = try FileHandle(forWritingTo: partialURL)
            fileHandle.seekToEndOfFile()
        } else {
            // Fresh download — create or overwrite.
            fm.createFile(atPath: partialURL.path, contents: nil)
            fileHandle = try FileHandle(forWritingTo: partialURL)
        }

        defer { try? fileHandle.close() }

        var downloaded: Int64 = 0
        let existingBytes: Int64 = httpResponse.statusCode == 206
            ? (try? fm.attributesOfItem(atPath: partialURL.path))?[.size] as? Int64 ?? 0
            : 0
        downloaded = existingBytes

        var buffer = Data()
        let flushThreshold = 256 * 1024 // Flush every 256 KB

        for try await byte in asyncBytes {
            buffer.append(byte)
            if buffer.count >= flushThreshold {
                fileHandle.write(buffer)
                downloaded += Int64(buffer.count)
                buffer.removeAll(keepingCapacity: true)

                let progress = totalSize > 0 ? Double(downloaded) / Double(totalSize) : 0
                await inventory.updateDownloadProgress(modelId: entry.id, progress: min(progress, 1.0))
            }
        }

        // Flush remaining bytes.
        if !buffer.isEmpty {
            fileHandle.write(buffer)
            downloaded += Int64(buffer.count)
        }

        try fileHandle.close()

        logger.info("Download complete for \(entry.id): \(downloaded) bytes")

        // Verify and stage.
        try await verifyAndStage(entry: entry, downloadedFile: partialURL)
    }

    // MARK: - Verification

    /// Verifies the SHA-256 checksum of a downloaded file.
    func verifyChecksum(fileURL: URL, expected: String) throws -> Bool {
        let fileHandle = try FileHandle(forReadingFrom: fileURL)
        defer { try? fileHandle.close() }

        var hasher = SHA256()
        let chunkSize = 1024 * 1024 // 1 MB chunks

        while autoreleasepool(invoking: {
            let chunk = fileHandle.readData(ofLength: chunkSize)
            guard !chunk.isEmpty else { return false }
            hasher.update(data: chunk)
            return true
        }) { }

        let digest = hasher.finalize()
        let hex = digest.map { String(format: "%02x", $0) }.joined()

        let matches = hex.lowercased() == expected.lowercased()
        if !matches {
            logger.error("Checksum mismatch for file: expected \(expected), got \(hex)")
        }
        return matches
    }

    // MARK: - Staging

    /// Verifies checksum, then moves the downloaded file to the staging directory.
    private func verifyAndStage(entry: ModelEntry, downloadedFile: URL) async throws {
        let checksumValid = try verifyChecksum(fileURL: downloadedFile, expected: entry.sha256)
        guard checksumValid else {
            // Remove the corrupt download.
            try? FileManager.default.removeItem(at: downloadedFile)
            await inventory.markMissing(modelId: entry.id)
            throw AssetProviderError.checksumMismatch(entry.id)
        }

        try await stage(entry: entry, verifiedFile: downloadedFile)
    }

    /// Moves a verified file into the staging directory.
    func stage(entry: ModelEntry, verifiedFile: URL) async throws {
        let stagingDir = inventory.stagingDirectory
        let stagedURL = stagingDir.appendingPathComponent(entry.id)

        let fm = FileManager.default

        // Remove any previously-staged version.
        if fm.fileExists(atPath: stagedURL.path) {
            try fm.removeItem(at: stagedURL)
        }

        try fm.moveItem(at: verifiedFile, to: stagedURL)

        // Write version sidecar.
        try await inventory.writeVersionSidecar(modelId: entry.id, version: entry.modelVersion, in: stagingDir)
        await inventory.markStaged(modelId: entry.id)

        logger.info("Model \(entry.id) v\(entry.modelVersion) staged successfully")
    }

    // MARK: - Promotion

    /// Atomically promotes a staged model to the active directory.
    /// If a previous version exists, it is moved to the rollback directory first.
    func promote(modelId: String) async throws {
        let activeDir = inventory.activeDirectory
        let stagingDir = inventory.stagingDirectory
        let rollbackDir = inventory.rollbackDirectory

        let stagedURL = stagingDir.appendingPathComponent(modelId)
        let activeURL = activeDir.appendingPathComponent(modelId)

        let fm = FileManager.default

        guard fm.fileExists(atPath: stagedURL.path) else {
            throw AssetProviderError.nothingStaged(modelId)
        }

        // If there is already an active version, archive it for rollback.
        if fm.fileExists(atPath: activeURL.path) {
            let rollbackURL = rollbackDir.appendingPathComponent(modelId)
            if fm.fileExists(atPath: rollbackURL.path) {
                try fm.removeItem(at: rollbackURL)
            }
            try fm.moveItem(at: activeURL, to: rollbackURL)

            // Also move the version sidecar.
            let activeSidecar = activeDir.appendingPathComponent("\(modelId).version")
            let rollbackSidecar = rollbackDir.appendingPathComponent("\(modelId).version")
            if fm.fileExists(atPath: activeSidecar.path) {
                if fm.fileExists(atPath: rollbackSidecar.path) {
                    try fm.removeItem(at: rollbackSidecar)
                }
                try fm.moveItem(at: activeSidecar, to: rollbackSidecar)
            }

            logger.info("Archived previous \(modelId) to rollback directory")
        }

        // Atomic promotion: move staged -> active.
        try fm.moveItem(at: stagedURL, to: activeURL)

        // Move version sidecar.
        let stagedSidecar = stagingDir.appendingPathComponent("\(modelId).version")
        let activeSidecar = activeDir.appendingPathComponent("\(modelId).version")
        if fm.fileExists(atPath: stagedSidecar.path) {
            if fm.fileExists(atPath: activeSidecar.path) {
                try fm.removeItem(at: activeSidecar)
            }
            try fm.moveItem(at: stagedSidecar, to: activeSidecar)
        }

        // Read promoted version for status update.
        let version = try? String(
            contentsOf: activeSidecar, encoding: .utf8
        ).trimmingCharacters(in: .whitespacesAndNewlines)

        await inventory.markReady(modelId: modelId, version: version ?? "unknown")
        logger.info("Model \(modelId) promoted to active")
    }

    // MARK: - Rollback

    /// Rolls back to the previously-archived version of a model.
    /// Useful if a new model version causes inference failures.
    func rollback(modelId: String) async throws {
        let activeDir = inventory.activeDirectory
        let rollbackDir = inventory.rollbackDirectory

        let activeURL = activeDir.appendingPathComponent(modelId)
        let rollbackURL = rollbackDir.appendingPathComponent(modelId)

        let fm = FileManager.default

        guard fm.fileExists(atPath: rollbackURL.path) else {
            throw AssetProviderError.noRollbackAvailable(modelId)
        }

        // Remove the current active version (it is the bad one).
        if fm.fileExists(atPath: activeURL.path) {
            try fm.removeItem(at: activeURL)
        }

        // Restore from rollback.
        try fm.moveItem(at: rollbackURL, to: activeURL)

        // Restore version sidecar.
        let rollbackSidecar = rollbackDir.appendingPathComponent("\(modelId).version")
        let activeSidecar = activeDir.appendingPathComponent("\(modelId).version")
        if fm.fileExists(atPath: rollbackSidecar.path) {
            if fm.fileExists(atPath: activeSidecar.path) {
                try fm.removeItem(at: activeSidecar)
            }
            try fm.moveItem(at: rollbackSidecar, to: activeSidecar)
        }

        let version = try? String(
            contentsOf: activeSidecar, encoding: .utf8
        ).trimmingCharacters(in: .whitespacesAndNewlines)

        await inventory.markReady(modelId: modelId, version: version ?? "unknown")
        logger.info("Model \(modelId) rolled back to previous version")
    }

    // MARK: - Cleanup

    /// Removes all partial downloads, freeing disk space.
    func cleanPartialDownloads() async throws {
        let downloadsDir = inventory.downloadsDirectory
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(
            at: downloadsDir,
            includingPropertiesForKeys: nil
        ) else { return }

        for fileURL in contents {
            try fm.removeItem(at: fileURL)
        }
        logger.info("Cleaned partial downloads")
    }

    /// Removes all rollback archives, freeing disk space.
    func cleanRollbacks() async throws {
        let rollbackDir = await inventory.rollbackDirectory
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(
            at: rollbackDir,
            includingPropertiesForKeys: nil
        ) else { return }

        for fileURL in contents {
            try fm.removeItem(at: fileURL)
        }
        logger.info("Cleaned rollback archives")
    }
}

// MARK: - Errors

enum AssetProviderError: Error, CustomStringConvertible {
    case downloadFailed(String, String)
    case checksumMismatch(String)
    case nothingStaged(String)
    case noRollbackAvailable(String)
    case insufficientDiskSpace(needed: Int64, available: Int64)

    var description: String {
        switch self {
        case .downloadFailed(let id, let reason):
            "Download failed for '\(id)': \(reason)"
        case .checksumMismatch(let id):
            "SHA-256 checksum mismatch for '\(id)'"
        case .nothingStaged(let id):
            "No staged version available for '\(id)'"
        case .noRollbackAvailable(let id):
            "No rollback version available for '\(id)'"
        case .insufficientDiskSpace(let needed, let available):
            "Insufficient disk space: need \(needed) bytes, have \(available)"
        }
    }
}

// MARK: - AssetDownloadDelegate

/// URLSession delegate for handling background download events.
final class AssetDownloadDelegate: NSObject, URLSessionDownloadDelegate, Sendable {
    private let logger = Logger(subsystem: "com.playhead", category: "AssetDownload")

    func urlSession(
        _ session: URLSession,
        downloadTask: URLSessionDownloadTask,
        didFinishDownloadingTo location: URL
    ) {
        // Background download completion is handled via the task's
        // completion handler or the AssetProvider polling mechanism.
        logger.info("Background download finished to \(location.path)")
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
        logger.debug("Download progress: \(String(format: "%.1f", progress * 100))%")
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: (any Error)?
    ) {
        if let error {
            logger.error("Download task failed: \(error.localizedDescription)")
        }
    }
}
