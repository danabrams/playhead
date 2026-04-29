// SettingsViewModel.swift
// ViewModel for the Settings screen. Bridges UserPreferences (SwiftData),
// EntitlementManager (restore purchases + premium status),
// AnalysisEligibilityEvaluator (model-status readout — playhead-j2u), and
// `StorageBreakdownReporter` (file-system queries).

import Foundation
import SwiftData
import OSLog

// MARK: - SettingsViewModel

@MainActor
@Observable
final class SettingsViewModel {
    private let logger = Logger(subsystem: "com.playhead", category: "Settings")

    // MARK: - Storage breakdown

    /// playhead-j2u: per-category + total storage usage. Driven by
    /// `StorageBreakdownReporter` so unit tests can pin the numbers
    /// against a temp tree without launching the full runtime.
    var storage: StorageBreakdown = .empty

    /// Convenience: legacy property access kept so the existing
    /// (l274) `storageUsageRow` keeps working without a name flip.
    var cachedAudioSize: Int64 { storage.cachedAudioBytes }
    /// Convenience: transcript-DB total exposed under the previous
    /// "transcript cache" label so existing call sites continue to compile.
    var transcriptCacheSize: Int64 { storage.transcriptDatabaseBytes }

    // MARK: - Restore state

    var isRestoring = false
    var restoreError: String?
    var restoreSucceeded = false

    // MARK: - Clear-in-progress state (playhead-j2u)

    /// `true` while `clearAudioCache()` is running. The Storage row
    /// renders a slim trailing `ProgressView` instead of the "Clear"
    /// button while this is set, so the user gets immediate feedback
    /// without a modal HUD or percentage bar.
    var isClearingAudioCache = false
    var isClearingTranscriptCache = false

    // MARK: - Premium status (playhead-j2u)

    /// Mirrors `EntitlementManager.isPremium`. Updated via
    /// `observePremiumStatus(_:)`. Defaults to `false` so the UI renders
    /// "Free preview" until the first stream emission lands.
    var isPremium: Bool = false

    // MARK: - Eligibility (playhead-j2u, model-status readout)

    /// Latest snapshot from `AnalysisEligibilityEvaluator.evaluate()`.
    /// `nil` until the first refresh — the UI treats `nil` as "Checking…"
    /// rather than guessing a verdict.
    var eligibility: AnalysisEligibility?

    // MARK: - Injected reporter

    /// The reporter used to compute the storage breakdown. Defaults to
    /// the live wiring; tests inject an explicit reporter pointing at a
    /// temp tree.
    var storageReporter: StorageBreakdownReporter = .live()

    // MARK: - Compute storage

    /// playhead-j2u: refresh the on-screen storage figures. The walk
    /// runs on a detached background task so the main actor never blocks
    /// on file-system enumeration; the result is hopped back to the
    /// MainActor before mutating the observable property.
    func computeStorageSizes() async {
        let reporter = storageReporter
        let snapshot = await Task.detached(priority: .utility) {
            reporter.measure()
        }.value
        self.storage = snapshot
    }

    /// playhead-j2u: refresh the model-status verdict. The evaluator's
    /// `evaluate()` is documented non-blocking and safe to call from the
    /// main actor.
    func refreshEligibility(using evaluator: AnalysisEligibilityEvaluating) {
        eligibility = evaluator.evaluate()
    }

    /// Triggers EntitlementManager restore flow.
    func restorePurchases(entitlementManager: EntitlementManager) async {
        isRestoring = true
        restoreError = nil
        restoreSucceeded = false
        do {
            try await entitlementManager.restorePurchases()
            restoreSucceeded = true
            isPremium = await entitlementManager.isPremium
            logger.info("Restore purchases succeeded")
        } catch {
            restoreError = error.localizedDescription
            logger.error("Restore purchases failed: \(error.localizedDescription)")
        }
        isRestoring = false
    }

    /// playhead-j2u: subscribe to the entitlement manager's premium
    /// stream so the Purchases section's status line stays in sync with
    /// transactions arriving from other devices / Family Sharing /
    /// out-of-band restores. Call once per view lifecycle.
    func observePremiumStatus(_ entitlementManager: EntitlementManager) async {
        for await value in entitlementManager.premiumUpdates {
            self.isPremium = value
        }
    }

    /// Clears transcript cache files.
    func clearTranscriptCache() async {
        // playhead-j2u: walk every transcript-DB root the reporter knows
        // about so the "Clear" action stays in sync with the displayed
        // total. This used to hard-code a single Application-Support
        // subdirectory; routing through the reporter keeps the two
        // surfaces honest. The actual disk walk runs on a detached
        // background task so the main actor never blocks on `FileManager`.
        isClearingTranscriptCache = true
        defer { isClearingTranscriptCache = false }
        let dirs = storageReporter.transcriptDirectories
        await Task.detached(priority: .utility) {
            for dir in dirs {
                Self.removeAnalysisArtifactsStatic(in: dir)
            }
        }.value
        await computeStorageSizes()
    }

    /// Clears cached audio files.
    func clearAudioCache() async {
        isClearingAudioCache = true
        defer { isClearingAudioCache = false }
        let dirs = storageReporter.audioDirectories
        await Task.detached(priority: .utility) {
            for dir in dirs {
                Self.removeContentsStatic(of: dir)
            }
        }.value
        await computeStorageSizes()
    }

    // MARK: - Helpers

    /// Removes the contents of `directory` (one level deep). Used by the
    /// audio-cache clear path where the cache directory itself must
    /// remain so subsequent downloads can land. `nonisolated` so the
    /// detached background task that performs the walk does not require
    /// MainActor isolation (`SettingsViewModel` is `@MainActor`).
    nonisolated static func removeContentsStatic(of directory: URL) {
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        ) else { return }
        for item in contents {
            try? fm.removeItem(at: item)
        }
    }

    /// Conservative deletion path for transcript-DB roots: only removes
    /// the analysis-shards subtree and any `analysis.sqlite*` siblings,
    /// matching the reporter's measurement contract. Other Application
    /// Support contents (preferences, user data) are left untouched.
    nonisolated static func removeAnalysisArtifactsStatic(in directory: URL) {
        let fm = FileManager.default
        guard fm.fileExists(atPath: directory.path) else { return }

        if directory.lastPathComponent == "AnalysisShards" {
            // Wipe shard contents but keep the directory so the cache
            // path can repopulate without a bootstrap step.
            removeContentsStatic(of: directory)
            return
        }

        if let entries = try? fm.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        ) {
            for entry in entries
            where entry.lastPathComponent.hasPrefix("analysis.sqlite") {
                try? fm.removeItem(at: entry)
            }
        }
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
