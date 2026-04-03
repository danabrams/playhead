// TranscriptPeekViewModel.swift
// Drives the transcript peek sheet: loads chunks from AnalysisStore,
// polls for new fast-pass arrivals, resolves the active segment index
// from the current playback time, and identifies ad regions.

import Foundation
import OSLog

@MainActor
final class TranscriptPeekViewModel: ObservableObject {

    // MARK: - Published State

    /// Transcript chunks sorted by startTime, fast-pass included.
    @Published private(set) var chunks: [TranscriptChunk] = []

    /// Ad windows for visual muting of ad segments.
    @Published private(set) var adWindows: [AdWindow] = []

    /// Index of the chunk containing the current playback position, or nil.
    @Published private(set) var activeChunkIndex: Int?

    /// True while the initial load is in progress.
    @Published private(set) var isLoading: Bool = true

    // MARK: - Configuration

    private let analysisAssetId: String
    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptPeek")

    /// How often to poll for new chunks (seconds).
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
            await self.refresh()
            self.isLoading = false

            // Continuous polling for new fast-pass chunks
            while !Task.isCancelled {
                try? await Task.sleep(for: .seconds(Self.pollInterval))
                guard !Task.isCancelled else { break }
                await self.refresh()
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
