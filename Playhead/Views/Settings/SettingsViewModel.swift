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

    // MARK: - iCloud sync status (playhead-5c1t)

    /// Mirrors `ICloudSyncCoordinator.isSyncEnabled`. `nil` until the
    /// first observation lands so the view can suppress the footer
    /// rather than flash a wrong value at launch (peace-of-mind, not
    /// metrics — no badge, no animation, no quantified counter).
    var iCloudSyncEnabled: Bool?

    // MARK: - Eligibility (playhead-j2u, model-status readout)

    /// Latest snapshot from `AnalysisEligibilityEvaluator.evaluate()`.
    /// `nil` until the first refresh — the UI treats `nil` as "Checking…"
    /// rather than guessing a verdict.
    var eligibility: AnalysisEligibility?

    /// `true` while the user-triggered recheck (Settings → Apple
    /// Intelligence → Recheck button) is in flight. The UI treats this
    /// as the same "Checking…" state it shows when `eligibility` is
    /// `nil` at first launch, so the user sees an honest pending
    /// indicator instead of the stale "Unavailable" verdict that
    /// prompted them to recheck in the first place.
    var isRecheckingModels = false

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

    /// Subscribe to capability snapshot updates and re-evaluate
    /// eligibility on each emission so the Apple Intelligence row stays
    /// current after asynchronous events — most importantly, the FM
    /// usability probe that the recheck flow schedules but does not
    /// await.
    ///
    /// R2 audit: explicitly `invalidate()` the evaluator before each
    /// `evaluate()` call. `AnalysisEligibilityEvaluator` caches its
    /// verdict for `defaultTTL` (4 hours); a peer task in `PlayheadRuntime`
    /// also subscribes to `capabilityUpdates()` and invalidates on every
    /// snapshot, but the two consumers race on the same snapshot —
    /// nothing orders them. If THIS task wins (`evaluate()` before the
    /// runtime's `invalidate()`), the verdict cache is still populated
    /// with the pre-snapshot value (e.g. "Unavailable, modelAvailableNow
    /// = false") and the providers are never re-read against the fresh
    /// snapshot. That is exactly the stuck-Unavailable bug the recheck
    /// flow exists to fix, except recurring on the post-probe snapshot.
    /// Invalidating locally guarantees a fresh recompute from providers
    /// on every emission. The cost is a single provider sweep
    /// (documented non-blocking) per snapshot — the snapshot rate is
    /// low (thermal/power/battery + occasional probe completions), so
    /// the overhead is negligible.
    ///
    /// This loop also clears `isRecheckingModels` once a settled
    /// "Available" snapshot lands — that is the moment the user's
    /// recheck conclusively succeeded. If the snapshot reports
    /// `foundationModelsUsable == false`, the pending flag is left
    /// alone so the row keeps reading "Checking…" until either the
    /// probe lands a positive result or the in-flight `recheckModels`
    /// call returns. (Either path eventually clears the flag.)
    ///
    /// The loop suspends until the consuming task is cancelled —
    /// SwiftUI tears `.task` down when the view leaves the hierarchy.
    /// Call once per view lifecycle.
    func observeCapabilitySnapshots(
        _ capabilities: CapabilitiesService,
        evaluator: AnalysisEligibilityEvaluating
    ) async {
        let stream = await capabilities.capabilityUpdates()
        for await snapshot in stream {
            // R2 audit: invalidate first so `evaluate()` re-reads the
            // providers against THIS snapshot — see the doc comment for
            // the race rationale.
            evaluator.invalidate()
            eligibility = evaluator.evaluate()
            // If a recheck was in flight and the FM probe has now
            // landed a usable verdict, clear the pending flag so the
            // "Checking…" indicator releases without waiting for the
            // user to tap Recheck a second time. A non-usable verdict
            // leaves the flag alone — `recheckModels` clears it when
            // it returns.
            if isRecheckingModels, snapshot.foundationModelsUsable {
                isRecheckingModels = false
            }
        }
    }

    /// User-triggered recheck for the Apple Intelligence status row.
    ///
    /// Steps, in order, so the UI gives an honest answer once the dust
    /// settles:
    ///   1. Drop the persisted FM usability probe cache so any prior
    ///      `usable == false` verdict is forgotten.
    ///   2. Invalidate the eligibility evaluator's in-memory snapshot
    ///      so the next `evaluate()` call recomputes.
    ///   3. Clear the local `eligibility` field and flip
    ///      `isRecheckingModels = true` so the View flips back to
    ///      "Checking…" while the async work runs.
    ///   4. Ask `CapabilitiesService.refreshSnapshot()` to publish a
    ///      fresh snapshot — that path schedules a fresh FM usability
    ///      probe in the background.
    ///   5. After the snapshot work returns, re-evaluate so the row
    ///      reflects the latest verdict and drop the pending flag.
    ///
    /// Note: the FM probe itself runs asynchronously inside the
    /// capabilities service. Step 4's `refreshSnapshot()` schedules it;
    /// the row may still read "Unavailable" right after this method
    /// returns if the probe is still in flight. That is correct — the
    /// snapshot will be republished from the service once the probe
    /// finishes (`finishFoundationModelsProbe()` calls `refreshSnapshot`
    /// again), which will re-fire the View's `.task` block and bring
    /// the row up to date. Until then the row will show whatever the
    /// freshest eligibility verdict says, which is the most honest
    /// answer available.
    func recheckModels(
        using evaluator: AnalysisEligibilityEvaluating,
        capabilities: CapabilitiesService
    ) async {
        FoundationModelsUsabilityProbe.clearCache()
        evaluator.invalidate()
        eligibility = nil
        isRecheckingModels = true
        await capabilities.refreshSnapshot()
        eligibility = evaluator.evaluate()
        isRecheckingModels = false
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

    /// playhead-5c1t: subscribe to the coordinator's sync-enabled
    /// stream so the Settings footer reflects sign-out / sign-in
    /// without a view re-appear. The stream emits the current value
    /// on subscription and a fresh value whenever
    /// `handleAccountStatusChange` observes a change. Call once per
    /// view lifecycle — the loop suspends until the consuming task is
    /// cancelled (typically by SwiftUI tearing down the `.task`
    /// modifier when the view leaves the hierarchy).
    func observeICloudSyncStatus(_ coordinator: ICloudSyncCoordinator) async {
        let stream = await coordinator.syncEnabledUpdates()
        for await enabled in stream {
            self.iCloudSyncEnabled = enabled
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
