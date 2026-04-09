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

    /// Ad windows for visual muting of ad segments (legacy Phase 2 path).
    private(set) var adWindows: [AdWindow] = []

    /// Phase 5 decoded spans for the new overlay rendering.
    private(set) var decodedSpans: [DecodedSpan] = []

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

    /// Returns true if the given time falls within any known ad window (legacy path).
    func isAdSegment(startTime: Double, endTime: Double) -> Bool {
        adWindows.contains { ad in
            ad.startTime < endTime && ad.endTime > startTime
        }
    }

    /// Returns the highest ad confidence score overlapping this chunk, or nil (legacy path).
    func adConfidence(startTime: Double, endTime: Double) -> Double? {
        let overlapping = adWindows.filter { ad in
            ad.startTime < endTime && ad.endTime > startTime
        }
        return overlapping.map(\.confidence).max()
    }

    /// Returns all Phase 5 decoded spans overlapping the given time range.
    func decodedSpansOverlapping(startTime: Double, endTime: Double) -> [DecodedSpan] {
        decodedSpans.filter { span in
            span.startTime < endTime && span.endTime > startTime
        }
    }

    /// Returns true if the given time range is covered by any Phase 5 decoded span.
    func isDecodedAdSegment(startTime: Double, endTime: Double) -> Bool {
        !decodedSpansOverlapping(startTime: startTime, endTime: endTime).isEmpty
    }

    /// Debug stats summary for TestFlight diagnostics.
    private(set) var debugStats: String = "loading…"

    private func updateDebugStats() async {
        let count = chunks.count
        let fmt = { (t: Double) -> String in
            let m = Int(t) / 60
            let s = Int(t) % 60
            return String(format: "%d:%02d", m, s)
        }

        var parts: [String] = []

        // Chunk count + time range
        if count > 0 {
            let minTime = chunks.first?.startTime ?? 0
            let maxTime = chunks.last?.endTime ?? 0
            parts.append("\(count) chunks \(fmt(minTime))–\(fmt(maxTime))")
        } else {
            parts.append("0 chunks")
        }

        // Ad window count
        parts.append("\(adWindows.count) ads")

        // Raw chunk count (before dedup) to detect if writes are missing
        do {
            let rawChunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
            if rawChunks.count != count {
                parts.append("raw \(rawChunks.count)")
            }
        } catch {}

        // Asset coverage watermarks + session state from store
        do {
            let asset = try await store.fetchAsset(id: analysisAssetId)
            if let featCov = asset?.featureCoverageEndTime {
                parts.append("feat \(fmt(featCov))")
            }
            if let txCov = asset?.fastTranscriptCoverageEndTime {
                parts.append("tx \(fmt(txCov))")
            }

            let session = try await store.fetchLatestSessionForAsset(assetId: analysisAssetId)
            if let session {
                parts.append(session.state)
            }
        } catch {
            parts.append("err")
        }

        // Streaming decode diagnostics.
#if DEBUG
        let seed = UserDefaults.standard.integer(forKey: "debug_streamingSeeded")
        let chunks = UserDefaults.standard.integer(forKey: "debug_streamingChunks")
        let strShards = UserDefaults.standard.integer(forKey: "debug_streamingShards")
        parts.append("s:\(seed/1024)k c:\(chunks) sh:\(strShards)")
#endif

        debugStats = parts.joined(separator: " · ")
    }

    // MARK: - Private

    private func refresh() async {
        do {
            let freshChunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
            let freshAds = try await store.fetchAdWindows(assetId: analysisAssetId)
            let freshSpans = try await store.fetchDecodedSpans(assetId: analysisAssetId)

            // Deduplicate: if both fast and final exist for the same segment,
            // prefer final. Group by chunkIndex, keep final if available.
            let grouped = Dictionary(grouping: freshChunks, by: { $0.chunkIndex })
            let deduped = grouped.values.map { group -> TranscriptChunk in
                group.first(where: { $0.pass == "final" }) ?? group[0]
            }
            .sorted { $0.startTime < $1.startTime }

            chunks = deduped
            adWindows = freshAds
            decodedSpans = freshSpans
            await updateDebugStats()
        } catch {
            logger.error("Failed to refresh transcript chunks: \(error)")
        }
    }
}
