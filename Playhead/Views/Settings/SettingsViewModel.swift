// SettingsViewModel.swift
// ViewModel for the Settings screen. Bridges UserPreferences (SwiftData),
// ModelInventory (model management), EntitlementManager (restore purchases),
// and file-system queries (storage sizes).

import Foundation
import SwiftData
import OSLog

// MARK: - SettingsViewModel

@MainActor
@Observable
final class SettingsViewModel {
    private let logger = Logger(subsystem: "com.playhead", category: "Settings")

    // MARK: - Storage sizes

    var modelFilesSize: Int64 = 0
    var transcriptCacheSize: Int64 = 0
    var cachedAudioSize: Int64 = 0

    // MARK: - Model statuses (snapshot for UI)

    var modelStatuses: [(ModelEntry, ModelStatus)] = []

    // MARK: - Restore state

    var isRestoring = false
    var restoreError: String?
    var restoreSucceeded = false

    // MARK: - Delete model state

    var isDeletingModel: String?

    // MARK: - Compute storage

    func computeStorageSizes() async {
        modelFilesSize = directorySize(at: ModelInventory.defaultModelsRoot())
        transcriptCacheSize = directorySize(at: analysisShardsDirectory())
        cachedAudioSize = directorySize(at: audioCacheDirectory())
    }

    /// Refreshes model statuses from the inventory.
    func refreshModelStatuses(inventory: ModelInventory) async {
        let allEntries = await inventory.manifest.models
        var result: [(ModelEntry, ModelStatus)] = []
        for entry in allEntries {
            let status = await inventory.status(for: entry.id)
            result.append((entry, status))
        }
        modelStatuses = result
    }

    /// Deletes a model from disk (active + staged + rollback + downloads).
    func deleteModel(entry: ModelEntry, inventory: ModelInventory) async {
        isDeletingModel = entry.id
        let fm = FileManager.default
        let dirs = [
            inventory.activeDirectory,
            inventory.stagingDirectory,
            inventory.rollbackDirectory,
            inventory.downloadsDirectory,
        ]
        for dir in dirs {
            let modelURL = dir.appendingPathComponent(entry.id)
            let sidecarURL = dir.appendingPathComponent("\(entry.id).version")
            let partialURL = dir.appendingPathComponent("\(entry.id).partial")
            try? fm.removeItem(at: modelURL)
            try? fm.removeItem(at: sidecarURL)
            try? fm.removeItem(at: partialURL)
        }
        await inventory.markMissing(modelId: entry.id)
        await refreshModelStatuses(inventory: inventory)
        await computeStorageSizes()
        isDeletingModel = nil
        logger.info("Deleted model \(entry.id)")
    }

    /// Triggers EntitlementManager restore flow.
    func restorePurchases(entitlementManager: EntitlementManager) async {
        isRestoring = true
        restoreError = nil
        restoreSucceeded = false
        do {
            try await entitlementManager.restorePurchases()
            restoreSucceeded = true
            logger.info("Restore purchases succeeded")
        } catch {
            restoreError = error.localizedDescription
            logger.error("Restore purchases failed: \(error.localizedDescription)")
        }
        isRestoring = false
    }

    /// Clears transcript cache files.
    func clearTranscriptCache() async {
        removeContents(of: analysisShardsDirectory())
        await computeStorageSizes()
    }

    /// Clears cached audio files.
    func clearAudioCache() async {
        removeContents(of: audioCacheDirectory())
        await computeStorageSizes()
    }

    // MARK: - Helpers

    private func directorySize(at url: URL) -> Int64 {
        let fm = FileManager.default
        guard let enumerator = fm.enumerator(
            at: url,
            includingPropertiesForKeys: [.fileSizeKey, .isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else { return 0 }

        var total: Int64 = 0
        for case let fileURL as URL in enumerator {
            guard let values = try? fileURL.resourceValues(forKeys: [.fileSizeKey, .isRegularFileKey]),
                  values.isRegularFile == true,
                  let size = values.fileSize else { continue }
            total += Int64(size)
        }
        return total
    }

    private func removeContents(of directory: URL) {
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        ) else { return }
        for item in contents {
            try? fm.removeItem(at: item)
        }
    }

    private func analysisShardsDirectory() -> URL {
        // Must match ShardCache.cacheDirectory in AnalysisAudio.swift.
        FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("AnalysisShards", isDirectory: true)
    }

    private func audioCacheDirectory() -> URL {
        DownloadManager.defaultCacheDirectory()
    }
}

// MARK: - Formatting

extension SettingsViewModel {
    /// Formats bytes as a human-readable size string.
    static func formattedSize(_ bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.countStyle = .file
        return formatter.string(fromByteCount: bytes)
    }
}
