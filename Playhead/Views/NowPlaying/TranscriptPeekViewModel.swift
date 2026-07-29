// TranscriptPeekViewModel.swift
// Drives the transcript peek sheet: pulls snapshots from a
// `TranscriptPeekDataSource`, polls for canonical transcript changes,
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

/// Immutable audio envelope captured when a visible transcript row is
/// selected. Polling may replace or reorder canonical rows before the user
/// confirms a mark, so submission state must not depend on a later array
/// offset.
struct TranscriptChunkSelection: Hashable, Sendable {
    let startTime: TimeInterval
    let endTime: TimeInterval

    init(chunk: TranscriptChunk) {
        startTime = chunk.startTime
        endTime = chunk.endTime
    }
}

@MainActor
@Observable
final class TranscriptPeekViewModel {

    // MARK: - State

    /// Canonical display chunks sorted by startTime.
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

    /// Most recent playback position supplied by the view. A poll can change
    /// canonical row cardinality while playback is paused, so refresh uses
    /// this value to resolve the active row against the new projection.
    private var latestPlaybackPosition: TimeInterval?

    /// True while the initial load is in progress.
    private(set) var isLoading: Bool = true

    /// `AnalysisAsset.fastTranscriptCoverageEndTime` for the asset, or nil
    /// when not yet computed. This is the SCAN watermark: how far the fast
    /// pass has looked, which is not the same as how far it found speech
    /// (playhead-7tn8). It separates "not yet scanned" from "scanned, found
    /// nothing" for the untranscribed-tail mark affordance (playhead-m1l9).
    private(set) var fastTranscriptCoverageEndTime: TimeInterval?

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
            // `refresh()` rejects a cancelled generation's snapshot, but
            // this caller must also avoid publishing lifecycle state after
            // `startPolling()` has replaced it with a newer task.
            guard !Task.isCancelled else { return }
            self.isLoading = false
            self.logger.info("Transcript peek: initial load done in \(ContinuousClock.now - start), \(self.chunks.count) chunks")

            // Continuous polling for transcript projection changes.
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
        latestPlaybackPosition = currentTime

        guard !chunks.isEmpty else {
            if activeChunkIndex != nil {
                activeChunkIndex = nil
            }
            return
        }

        // Chunks are sorted by startTime, but retained partial-overlap rows
        // can have non-monotonic end times. Prefer a final-pass row whenever
        // both passes cover playback, then the latest-starting row within
        // the same pass. Only fall back to the closest preceding row when
        // playback lies in a transcript gap or past the final row.
        var preceding: Int?
        var covering: Int?
        for (index, chunk) in chunks.enumerated() {
            if chunk.startTime <= currentTime {
                // Nested rows make latest-start and closest-preceding
                // different concepts. In a gap, retain the row whose audio
                // ended most recently; use later display order only to break
                // an equal-end tie.
                if let currentPreceding = preceding {
                    if chunk.endTime >= chunks[currentPreceding].endTime {
                        preceding = index
                    }
                } else {
                    preceding = index
                }
                // Transcript audio envelopes are half-open [start, end), so
                // a row ending exactly here must not outrank one that starts
                // at the same boundary.
                if chunk.endTime > currentTime {
                    if let currentCovering = covering {
                        let currentChunk = chunks[currentCovering]
                        let candidateIsFinal =
                            chunk.pass == TranscriptPassType.final_.rawValue
                        let currentIsFinal =
                            currentChunk.pass == TranscriptPassType.final_.rawValue

                        if candidateIsFinal || !currentIsFinal {
                            covering = index
                        }
                    } else {
                        covering = index
                    }
                }
            } else {
                break
            }
        }
        let resolved = covering ?? preceding
        if activeChunkIndex != resolved {
            activeChunkIndex = resolved
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

    /// Resolve currently visible audio envelopes captured from displayed rows
    /// to the combined range used by the mark-ad and not-ad submission paths.
    /// Capturing the envelope at tap time prevents a later polling refresh
    /// from redirecting an array offset to unrelated audio; intersecting with
    /// the latest projection closes the window between SwiftUI's deferred
    /// selection reconciliation and a confirmation action.
    func selectedChunkTimeRange(
        selections: Set<TranscriptChunkSelection>
    ) -> (start: TimeInterval, end: TimeInterval)? {
        let visibleSelections = reconciledSelections(selections)
        guard let start = visibleSelections.map(\.startTime).min(),
              let end = visibleSelections.map(\.endTime).max()
        else {
            return nil
        }
        return (start: start, end: end)
    }

    /// Drop captured envelopes that no longer correspond to any visible
    /// canonical row. Exact-span fast→final replacement remains selected,
    /// while a differently bounded replacement cannot become an invisible,
    /// impossible-to-deselect mark or veto.
    func reconciledSelections(
        _ selections: Set<TranscriptChunkSelection>
    ) -> Set<TranscriptChunkSelection> {
        let visibleSelections = Set(chunks.map(TranscriptChunkSelection.init))
        return selections.intersection(visibleSelections)
    }

    // MARK: - Untranscribed-tail mark (playhead-m1l9)

    /// Shortest tail span we will offer to mark — avoids degenerate sub-2s
    /// marks when the playhead is a hair from the episode end.
    private static let minTailMarkWidth: TimeInterval = 2.0

    /// Longest remaining-to-end span we treat as a "post-roll to the end".
    /// Beyond this the playhead isn't plausibly inside a post-roll and a
    /// mark-to-end would over-mark a large untranscribed region; the player
    /// "Hearing an ad" button (a bounded ±window seed) is the right tool
    /// there. Generous enough to cover long ad pods (5 minutes).
    private static let maxTailMarkWidth: TimeInterval = 300.0

    /// The furthest transcript time the chunk-selection mark flow can reach:
    /// the max chunk end. That flow assembles a span by tapping rows, so it
    /// stops where the rows stop — this is TRANSCRIPT EVIDENCE ("how far did
    /// we find speech?"), not scan reach.
    /// With no chunks at all this is 0 — the same sentinel the pre-7tn8 gate
    /// used, so a playhead at exactly 0.0 on a transcript-less episode keeps
    /// its old (no-affordance) behavior.
    var lastTranscribedTime: TimeInterval {
        chunks.lazy.map(\.endTime).max() ?? 0
    }

    /// The furthest time the fast pass has SCANNED ("how far did we look?"):
    /// the evidence end unioned with the coverage watermark. The watermark can
    /// legitimately run past the last chunk, because a shard that yields zero
    /// segments still advances coverage (`TranscriptEngineService`) — we
    /// looked at that audio and found no speech in it.
    ///
    /// playhead-7tn8: this is deliberately NOT what gates the tail-mark
    /// affordance any more. It stays because the scan question is real and
    /// distinct — it is what separates a transient not-yet-analyzed tail from
    /// a genuine silent blind spot in ``tailCoverage(at:)``.
    var lastCoveredTime: TimeInterval {
        max(lastTranscribedTime, fastTranscriptCoverageEndTime ?? 0)
    }

    /// What is known about the audio under a given playback time.
    ///
    /// Coverage answers two different questions and playhead-7tn8 turns on
    /// keeping them apart: the watermark answers "did we LOOK here?", the
    /// chunks answer "did we FIND SPEECH here?".
    enum TailCoverage: Equatable, Sendable {
        /// Transcript rows reach this time — the chunk-selection "Mark ad"
        /// flow works, so the tail affordance is not needed.
        case transcribed
        /// Inside the scan watermark but past the last chunk: we looked and
        /// found no speech. A music-bedded ad produces exactly this, and no
        /// deterministic or lexical signal can bite on it — the listener is
        /// the only witness, so the affordance must be offered.
        case scannedWithoutSpeech
        /// Past the scan watermark: analysis has not reached here yet. Kept
        /// distinct from ``scannedWithoutSpeech`` — this one is transient and
        /// segments may still arrive.
        case notYetScanned
    }

    /// Classify `time` against the two coverage questions. See ``TailCoverage``.
    func tailCoverage(at time: TimeInterval) -> TailCoverage {
        // Both boundaries are INCLUSIVE of their region: a time landing exactly
        // on the last chunk end is still `.transcribed`, and one landing
        // exactly on the watermark is still `.scannedWithoutSpeech`. That
        // reproduces the pre-7tn8 `currentTime > lastCoveredTime` gate — a
        // playhead sitting on the edge is not yet in the tail.
        guard time > lastTranscribedTime else { return .transcribed }
        return time <= lastCoveredTime ? .scannedWithoutSpeech : .notYetScanned
    }

    /// A coverage-FREE ad-mark span for the untranscribed tail, or nil when
    /// the tail affordance does not apply.
    ///
    /// The chunk-selection "Mark ad" flow can only build a mark from existing
    /// transcript chunks, which extend only to `lastTranscribedTime`. When a
    /// post-roll runs past the last chunk to the episode end, there are no
    /// chunks to tap and the ad is unmarkable (playhead-m1l9). This returns
    /// `[currentTime, episodeDuration]` so the untranscribed tail can be
    /// marked without any chunks — routed through the same coverage-free
    /// `injectUserMarkedAd` path the player "Hearing an ad" button uses.
    ///
    /// playhead-7tn8: the gate keys on TRANSCRIPT EVIDENCE, not scan reach.
    /// Keying it on `lastCoveredTime` suppressed the affordance in exactly the
    /// case it exists for — a music-bedded post-roll, where the shards were
    /// scanned, produced zero segments, advanced the watermark past the last
    /// chunk, and left the listener as the only witness that the ad happened.
    /// A scanned-but-silent stretch is often just music, so the affordance can
    /// appear over non-ad audio; that is accepted deliberately, because it is
    /// OPT-IN and only fires when someone actually heard an ad. Weight the
    /// resulting correction through the existing certainty tiering rather than
    /// suppressing the report.
    ///
    /// Returns nil unless the playhead sits PAST the transcript evidence
    /// (otherwise the ordinary chunk-selection flow already works) AND the
    /// remaining span to the episode end is post-roll-sized (a meaningful
    /// span that isn't a large over-mark). The bounded-window player button
    /// handles the "hearing an ad far from the end" case.
    func untranscribedTailMarkSpan(
        currentTime: TimeInterval,
        episodeDuration: TimeInterval
    ) -> (start: Double, end: Double)? {
        guard episodeDuration > 0 else { return nil }
        // The playhead must be in the untranscribed tail — territory the
        // chunk-selection flow cannot reach. Both uncovered states qualify:
        // `.scannedWithoutSpeech` is the silent-shard blind spot this bead
        // reopened, `.notYetScanned` is m1l9's original post-roll case.
        guard tailCoverage(at: currentTime) != .transcribed else { return nil }
        // A post-roll-sized span must remain to the episode end.
        let remaining = episodeDuration - currentTime
        guard remaining >= Self.minTailMarkWidth,
              remaining <= Self.maxTailMarkWidth else { return nil }
        return (start: currentTime, end: episodeDuration)
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
            let maxTime = chunks.map(\.endTime).max() ?? 0
            parts.append("\(count) chunks \(fmt(minTime))–\(fmt(maxTime))")
        } else {
            parts.append("0 chunks")
        }

        // Ad window count
        parts.append("\(adWindows.count) ads")

        // Raw chunk count (before display canonicalization) to detect whether
        // writes are missing or overlapping rows were projected away.
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

        // playhead-u45d: the same defect one field over. This set used to
        // filter on `boundaryState` alone, so vetoing an ad the listener had
        // marked themselves flipped `decisionState` to `reverted` and left the
        // rows lit — the write landing where nothing looked, exactly as with
        // decoded spans. `reverted` IS the durable record the veto writes;
        // reading it is the correction taking effect, not the view hiding a
        // row it was never told about.
        let userMarkedWindows = adWindows.filter {
            $0.boundaryState == "userMarked"
                && $0.decisionState != AdDecisionState.reverted.rawValue
        }

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

    /// Pull a fresh snapshot and apply it to observable state. Internal (not
    /// private) so tests can drive one deterministic load without racing the
    /// `startPolling` Task.
    func refresh() async {
        let snapshot = await dataSource.fetchSnapshot(assetId: analysisAssetId)
        // A cancelled polling generation may finish after a replacement task
        // because data sources are not required to cooperate with
        // cancellation. Never let that stale response overwrite newer state.
        guard !Task.isCancelled else { return }

        if snapshot.fetchFailed {
            logger.error("Transcript peek: snapshot fetch reported failure for asset \(self.analysisAssetId)")
            // Poll failures are not authoritative empty projections. Retain
            // the last good rows so active playback and in-progress mark/veto
            // selections survive a transient store error.
            updateDebugStats(snapshot: snapshot)
            return
        }
        chunks = snapshot.chunks
        if let latestPlaybackPosition {
            updatePlaybackPosition(latestPlaybackPosition)
        } else {
            activeChunkIndex = nil
        }
        adWindows = snapshot.adWindows
        decodedSpans = snapshot.decodedSpans
        fastTranscriptCoverageEndTime = snapshot.fastTranscriptCoverageEnd
        rebuildSpansByChunkIndex()
        updateDebugStats(snapshot: snapshot)
    }
}
