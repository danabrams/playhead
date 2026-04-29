// TranscriptPeekViewModel.swift
// Drives the transcript peek sheet: pulls snapshots from a
// `TranscriptPeekDataSource`, polls for new fast-pass arrivals,
// resolves the active segment index from the current playback time,
// and identifies ad regions.
//
// playhead-fwvz: this file is on the UI-layer contract — it consumes
// the `TranscriptPeekSnapshot` boundary type only and does not
// reference `AnalysisStore` (or any other forbidden module-boundary
// token enforced by `SurfaceStatusUILintTests`). The fetch logic that
// previously lived here moved to `LiveTranscriptPeekDataSource`.

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

    /// Pre-computed mapping from chunk index to overlapping decoded spans.
    /// Rebuilt each refresh cycle so per-row lookups are O(1).
    private var spansByChunkIndex: [Int: [DecodedSpan]] = [:]

    /// Chunk indices that overlap a user-marked AdWindow (boundaryState "userMarked").
    /// These get visual ad highlighting even without a corresponding DecodedSpan.
    private var userMarkedChunkIndices: Set<Int> = []

    /// Index of the chunk containing the current playback position, or nil.
    private(set) var activeChunkIndex: Int?

    /// True while the initial load is in progress.
    private(set) var isLoading: Bool = true

    // MARK: - Configuration

    let analysisAssetId: String
    private let dataSource: TranscriptPeekDataSource
    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptPeek")

    /// How often to poll for new chunks (seconds).
    /// Polling is intentional here: the data source does not emit
    /// granular notifications for individual chunk inserts. The
    /// 2-second interval balances responsiveness with efficiency.
    /// When the data source gains change notifications, this should
    /// be replaced with event-driven updates.
    private static let pollInterval: TimeInterval = 2.0

    private var pollTask: Task<Void, Never>?

    // MARK: - Init

    init(analysisAssetId: String, dataSource: TranscriptPeekDataSource) {
        self.analysisAssetId = analysisAssetId
        self.dataSource = dataSource
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

    /// Returns all Phase 5 decoded spans overlapping the chunk at `chunkIndex`.
    /// Uses the pre-computed mapping built during refresh() for O(1) lookup.
    func decodedSpansOverlapping(chunkIndex: Int) -> [DecodedSpan] {
        spansByChunkIndex[chunkIndex] ?? []
    }

    /// Whether this chunk should receive ad highlighting (copper bar, background tint).
    /// True if the chunk overlaps any DecodedSpan OR any user-marked AdWindow.
    func isAdHighlighted(chunkIndex: Int) -> Bool {
        (spansByChunkIndex[chunkIndex] != nil) || userMarkedChunkIndices.contains(chunkIndex)
    }

    /// Returns all Phase 5 decoded spans overlapping the given time range.
    /// Retained for callers that don't have a chunk index handy.
    func decodedSpansOverlapping(startTime: Double, endTime: Double) -> [DecodedSpan] {
        decodedSpans.filter { span in
            span.startTime < endTime && span.endTime > startTime
        }
    }

    /// Debug stats summary for TestFlight diagnostics.
    private(set) var debugStats: String = "loading…"

    private func updateDebugStats(snapshot: TranscriptPeekSnapshot) {
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
        if snapshot.rawChunkCount != count {
            parts.append("raw \(snapshot.rawChunkCount)")
        }

        // Asset coverage watermarks + session state
        if let featCov = snapshot.featureCoverageEnd {
            parts.append("feat \(fmt(featCov))")
        }
        if let txCov = snapshot.fastTranscriptCoverageEnd {
            parts.append("tx \(fmt(txCov))")
        }
        if let session = snapshot.latestSessionState {
            parts.append(session)
        }
        if snapshot.fetchFailed {
            parts.append("err")
        }

        // Streaming decode diagnostics.
#if DEBUG
        let seed = UserDefaults.standard.integer(forKey: "debug_streamingSeeded")
        let streamingChunks = UserDefaults.standard.integer(forKey: "debug_streamingChunks")
        let strShards = UserDefaults.standard.integer(forKey: "debug_streamingShards")
        parts.append("s:\(seed/1024)k c:\(streamingChunks) sh:\(strShards)")
#endif

        debugStats = parts.joined(separator: " · ")
    }

    // MARK: - Private

    /// Rebuild the chunk-index → overlapping-spans lookup table and
    /// the user-marked chunk index set.
    /// Called once per refresh cycle so per-row view queries are O(1).
    private func rebuildSpansByChunkIndex() {
        var mapping: [Int: [DecodedSpan]] = [:]
        var userMarked = Set<Int>()

        let userMarkedWindows = adWindows.filter { $0.boundaryState == "userMarked" }

        for (idx, chunk) in chunks.enumerated() {
            let overlapping = decodedSpans.filter { span in
                span.startTime < chunk.endTime && span.endTime > chunk.startTime
            }
            if !overlapping.isEmpty {
                mapping[idx] = overlapping
            }

            if userMarkedWindows.contains(where: { ad in
                ad.startTime < chunk.endTime && ad.endTime > chunk.startTime
            }) {
                userMarked.insert(idx)
            }
        }
        spansByChunkIndex = mapping
        userMarkedChunkIndices = userMarked
    }

    private func refresh() async {
        let snapshot = await dataSource.fetchSnapshot(assetId: analysisAssetId)
        if snapshot.fetchFailed {
            logger.error("Transcript peek: snapshot fetch reported failure for asset \(self.analysisAssetId)")
        }
        chunks = snapshot.chunks
        adWindows = snapshot.adWindows
        decodedSpans = snapshot.decodedSpans
        rebuildSpansByChunkIndex()
        updateDebugStats(snapshot: snapshot)
    }
}
