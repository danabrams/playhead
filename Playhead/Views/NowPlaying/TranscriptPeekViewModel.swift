// TranscriptPeekViewModel.swift
// Drives the transcript peek sheet: loads chunks from AnalysisStore,
// polls for new fast-pass arrivals, resolves the active segment index
// from the current playback time, and identifies ad regions.

import Foundation
import OSLog

@MainActor
@Observable
final class TranscriptPeekViewModel {

    // MARK: - State

    /// Transcript chunks sorted by startTime, fast-pass included.
    private(set) var chunks: [TranscriptChunk] = []

    /// Ad windows for visual muting of ad segments.
    private(set) var adWindows: [AdWindow] = []

    /// Index of the chunk containing the current playback position, or nil.
    private(set) var activeChunkIndex: Int?

    /// True while the initial load is in progress.
    private(set) var isLoading: Bool = true

    // MARK: - Configuration

    private let analysisAssetId: String
    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptPeek")

    /// How often to poll for new chunks (seconds).
    /// Polling is intentional here: AnalysisStore does not emit granular
    /// notifications for individual chunk inserts. The 2-second interval
    /// balances responsiveness with efficiency. When AnalysisStore gains
    /// change notifications, this should be replaced with event-driven updates.
    private static let pollInterval: TimeInterval = 2.0

    private var pollTask: Task<Void, Never>?

    // MARK: - Init

    init(analysisAssetId: String, store: AnalysisStore) {
        self.analysisAssetId = analysisAssetId
        self.store = store
    }

    // MARK: - Lifecycle

    func startPolling() {
        pollTask?.cancel()
        pollTask = Task { [weak self] in
            guard let self else { return }
            // Initial load
            self.logger.info("Transcript peek: starting initial load for asset \(self.analysisAssetId)")
            let start = ContinuousClock.now
            await self.refresh()
            self.isLoading = false
            self.logger.info("Transcript peek: initial load done in \(ContinuousClock.now - start), \(self.chunks.count) chunks")

            // Continuous polling for new fast-pass chunks
            while !Task.isCancelled {
                try? await Task.sleep(for: .seconds(Self.pollInterval))
                guard !Task.isCancelled else { break }
                let before = self.chunks.count
                await self.refresh()
                let after = self.chunks.count
                if after != before {
                    self.logger.info("Transcript peek: \(before) → \(after) chunks")
                }
            }
        }
    }

    func stopPolling() {
        pollTask?.cancel()
        pollTask = nil
    }

    // MARK: - Position Tracking

    /// Call from the view whenever playback time updates.
    func updatePlaybackPosition(_ currentTime: TimeInterval) {
        guard !chunks.isEmpty else {
            activeChunkIndex = nil
            return
        }

        // Binary-ish search: chunks are sorted by startTime.
        // Find the last chunk whose startTime <= currentTime.
        var best: Int?
        for (index, chunk) in chunks.enumerated() {
            if chunk.startTime <= currentTime {
                best = index
            } else {
                break
            }
        }

        // Verify the chunk actually covers currentTime (within endTime).
        if let idx = best, chunks[idx].endTime >= currentTime {
            activeChunkIndex = idx
        } else {
            // Between chunks or past the end — keep closest preceding chunk.
            activeChunkIndex = best
        }
    }

    /// Returns true if the given time falls within any known ad window.
    func isAdSegment(startTime: Double, endTime: Double) -> Bool {
        adWindows.contains { ad in
            ad.startTime < endTime && ad.endTime > startTime
        }
    }

    /// Returns the highest ad confidence score overlapping this chunk, or nil.
    func adConfidence(startTime: Double, endTime: Double) -> Double? {
        let overlapping = adWindows.filter { ad in
            ad.startTime < endTime && ad.endTime > startTime
        }
        return overlapping.map(\.confidence).max()
    }

    /// Debug stats summary for TestFlight diagnostics.
    var debugStats: String {
        let count = chunks.count
        guard count > 0 else { return "0 chunks" }
        let minTime = chunks.first?.startTime ?? 0
        let maxTime = chunks.last?.endTime ?? 0
        let adCount = adWindows.count
        let fmt = { (t: Double) -> String in
            let m = Int(t) / 60
            let s = Int(t) % 60
            return String(format: "%d:%02d", m, s)
        }
        return "\(count) chunks · \(fmt(minTime))–\(fmt(maxTime)) · \(adCount) ads"
    }

    // MARK: - Private

    private func refresh() async {
        do {
            let freshChunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
            let freshAds = try await store.fetchAdWindows(assetId: analysisAssetId)

            // Deduplicate: if both fast and final exist for the same segment,
            // prefer final. Group by chunkIndex, keep final if available.
            let grouped = Dictionary(grouping: freshChunks, by: { $0.chunkIndex })
            let deduped = grouped.values.map { group -> TranscriptChunk in
                group.first(where: { $0.pass == "final" }) ?? group[0]
            }
            .sorted { $0.startTime < $1.startTime }

            chunks = deduped
            adWindows = freshAds
        } catch {
            logger.error("Failed to refresh transcript chunks: \(error)")
        }
    }
}
