// ModelDownloadViewModel.swift
// Drives the model download step of onboarding.
//
// Strategy: download the fast-path ASR model first (small, unblocks
// real-time analysis). Once it is ready the user can proceed.
// Remaining models (final ASR, classifier) download in the background.

import Foundation
import OSLog

@MainActor
final class ModelDownloadViewModel: ObservableObject {

    private let logger = Logger(subsystem: "com.playhead", category: "OnboardingDownload")

    // MARK: - Published State

    /// Progress of the fast-path model download (0.0 ... 1.0).
    @Published var displayProgress: Double = 0

    /// True once the fast-path ASR model is ready for inference.
    @Published var fastPathReady = false

    /// True once every model in the manifest is ready.
    @Published var allModelsReady = false

    /// Number of background models still downloading after the fast path.
    @Published var backgroundModelsRemaining: Int = 0

    /// Human-readable status line shown below the progress indicator.
    @Published var statusMessage: String = "Checking models..."

    /// True once the fast-path model is ready (user can tap Continue).
    var canProceed: Bool { fastPathReady }

    // MARK: - Dependencies

    private var inventory: ModelInventory?
    private var assetProvider: AssetProvider?

    // MARK: - Download Orchestration

    /// Entry point called from .task { } in the view.
    func startDownloads() async {
        do {
            let manifest = try ModelInventory.loadBundledManifest()
            let inv = ModelInventory(manifest: manifest)
            let provider = AssetProvider(inventory: inv)

            self.inventory = inv
            self.assetProvider = provider

            try await inv.scan()

            // Check if fast-path is already available (e.g. app reinstall
            // with cached models still on disk).
            let fastPathModelId = await inv.manifest.preferred(for: .asrFast)?.id

            if let id = fastPathModelId, await inv.isReady(role: .asrFast) {
                logger.info("Fast-path ASR already on disk: \(id)")
                displayProgress = 1.0
                fastPathReady = true
                statusMessage = "Ad detection model ready."
                await checkRemainingModels(inv)
                return
            }

            // Download fast-path ASR with progress tracking.
            if let fastEntry = await inv.fastPathASRIfMissing() {
                statusMessage = "Downloading detection model..."

                let modelId = fastEntry.id

                // Start polling progress while download runs concurrently.
                let downloadComplete = UnsafeSendableFlag()

                let downloadTask = Task.detached { [provider] in
                    try await provider.download(entry: fastEntry)
                    try await provider.promote(modelId: fastEntry.id)
                    downloadComplete.set()
                }

                // Poll every 250ms for progress.
                while !downloadComplete.value {
                    try? await Task.sleep(for: .milliseconds(250))

                    let status = await inv.status(for: modelId)
                    switch status {
                    case .downloading(let progress):
                        displayProgress = progress
                        let pct = Int(progress * 100)
                        statusMessage = "Downloading detection model... \(pct)%"
                    case .staged:
                        displayProgress = 0.95
                        statusMessage = "Verifying..."
                    case .ready:
                        displayProgress = 1.0
                        fastPathReady = true
                        statusMessage = "Ad detection model ready."
                    default:
                        break
                    }

                    if fastPathReady { break }
                }

                // Await the download task to propagate any errors.
                try await downloadTask.value

                // Ensure state is correct after completion.
                if !fastPathReady {
                    let finalStatus = await inv.status(for: modelId)
                    if case .ready = finalStatus {
                        displayProgress = 1.0
                        fastPathReady = true
                        statusMessage = "Ad detection model ready."
                    }
                }
            } else {
                // No fast-path model needed (already ready or no manifest entry).
                fastPathReady = true
                displayProgress = 1.0
                statusMessage = "Models ready."
            }

            // Kick off remaining downloads in background.
            await checkRemainingModels(inv)
            startBackgroundDownloads(provider: provider, inventory: inv)

        } catch {
            logger.error("Model download failed: \(error.localizedDescription)")
            statusMessage = "Download issue. You can retry in Settings."
            // Allow proceeding even on failure — the app degrades gracefully.
            fastPathReady = true
        }
    }

    // MARK: - Background Downloads

    private func checkRemainingModels(_ inventory: ModelInventory) async {
        let missing = await inventory.missingModels()
        backgroundModelsRemaining = missing.count
        allModelsReady = missing.isEmpty
    }

    private func startBackgroundDownloads(provider: AssetProvider, inventory: ModelInventory) {
        Task.detached(priority: .utility) { [weak self] in
            do {
                let remaining = await inventory.missingModels()
                for entry in remaining {
                    try await provider.download(entry: entry)
                    try await provider.promote(modelId: entry.id)
                    await MainActor.run {
                        self?.backgroundModelsRemaining -= 1
                    }
                }
                await MainActor.run {
                    self?.allModelsReady = true
                }
            } catch {
                // Background failures are non-blocking. The app will
                // retry on next launch via the normal model-check path.
            }
        }
    }
}

// MARK: - Sendable Atomic Flag

/// Simple thread-safe boolean flag for cross-task signaling.
/// Uses `OSAtomicOr32Barrier` semantics via an actor to stay Swift 6 safe.
private final class UnsafeSendableFlag: @unchecked Sendable {
    private let _lock = NSLock()
    private var _value = false

    var value: Bool {
        _lock.lock()
        defer { _lock.unlock() }
        return _value
    }

    func set() {
        _lock.lock()
        _value = true
        _lock.unlock()
    }
}
