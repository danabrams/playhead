// TranscriptPeekDataSource.swift
// Boundary type + service that hands the SwiftUI transcript-peek
// view model a fully-resolved snapshot of one episode's transcript /
// ad / decoded-span / coverage state — no `AnalysisStore` reference
// crosses the module boundary into the UI layer.
//
// Why this exists (playhead-fwvz):
// Before this bead, `TranscriptPeekViewModel` (a UI-layer file under
// `Playhead/Views/`) accepted an `AnalysisStore` and called five of
// its fetch methods directly. The
// `SurfaceStatusUILintTests.testNoSchedulerOrPersistenceTypesInUIViews`
// regression guard forbids that — UI files must consume the
// `EpisodeSurfaceStatus` boundary type (or a peer boundary type for
// data the surface-status reducer doesn't carry).
//
// `EpisodeSurfaceStatus` is a per-episode readiness state struct; it
// does not (and should not) carry the per-chunk transcript payload,
// the ad-window list, decoded spans, or live debug-stat counters
// that the transcript-peek view renders. Those are TRANSCRIPT-VIEW
// data, not READINESS-STATE data. So we add a sibling boundary type
// — `TranscriptPeekSnapshot` — and a `TranscriptPeekDataSource`
// protocol whose live implementation is the only file that touches
// `AnalysisStore` for this view's needs.
//
// The snapshot mirrors the fields the view model previously fetched
// per refresh cycle:
//
//   - `chunks`               — fast-or-final dedup of `TranscriptChunk` rows
//   - `adWindows`            — every `AdWindow` row for the asset
//   - `decodedSpans`         — every `DecodedSpan` row (Phase 5 overlay)
//   - `featureCoverageEnd`   — `AnalysisAsset.featureCoverageEndTime`
//   - `fastTranscriptCoverageEnd`
//                            — `AnalysisAsset.fastTranscriptCoverageEndTime`
//   - `latestSessionState`   — `AnalysisSession.state` for the asset
//   - `rawChunkCount`        — pre-dedup chunk count, used to surface
//                              missing-write conditions in debug stats

import Foundation

// MARK: - Snapshot value type

/// A fully-resolved view of one episode's transcript-peek state at a
/// point in time. Produced by a `TranscriptPeekDataSource` and consumed
/// by `TranscriptPeekViewModel`. UI files only ever see this struct +
/// its constituent row types — never `AnalysisStore`.
struct TranscriptPeekSnapshot: Sendable {

    /// All transcript chunks for the asset, post fast/final dedup,
    /// sorted by `startTime`. Empty when no chunks exist yet.
    let chunks: [TranscriptChunk]

    /// Pre-dedup chunk count from the source store. Used by the debug
    /// stats line to surface missing-write conditions; equal to
    /// `chunks.count` when no fast→final overlap exists.
    let rawChunkCount: Int

    /// All ad windows persisted for the asset.
    let adWindows: [AdWindow]

    /// All Phase 5 decoded spans persisted for the asset.
    let decodedSpans: [DecodedSpan]

    /// `AnalysisAsset.featureCoverageEndTime` for the asset, or `nil`
    /// when the asset row isn't present yet.
    let featureCoverageEnd: TimeInterval?

    /// `AnalysisAsset.fastTranscriptCoverageEndTime` for the asset, or
    /// `nil` when not yet computed.
    let fastTranscriptCoverageEnd: TimeInterval?

    /// `AnalysisSession.state` for the most recent session attached to
    /// the asset, or `nil` when no session row exists.
    let latestSessionState: String?

    /// True when the underlying fetch failed; the view model uses this
    /// to render an "err" hint in the debug stats line.
    let fetchFailed: Bool

    init(
        chunks: [TranscriptChunk],
        rawChunkCount: Int,
        adWindows: [AdWindow],
        decodedSpans: [DecodedSpan],
        featureCoverageEnd: TimeInterval?,
        fastTranscriptCoverageEnd: TimeInterval?,
        latestSessionState: String?,
        fetchFailed: Bool
    ) {
        self.chunks = chunks
        self.rawChunkCount = rawChunkCount
        self.adWindows = adWindows
        self.decodedSpans = decodedSpans
        self.featureCoverageEnd = featureCoverageEnd
        self.fastTranscriptCoverageEnd = fastTranscriptCoverageEnd
        self.latestSessionState = latestSessionState
        self.fetchFailed = fetchFailed
    }

    /// An empty snapshot used as the initial value before the first
    /// fetch lands. Equivalent to "no data yet".
    static let empty = TranscriptPeekSnapshot(
        chunks: [],
        rawChunkCount: 0,
        adWindows: [],
        decodedSpans: [],
        featureCoverageEnd: nil,
        fastTranscriptCoverageEnd: nil,
        latestSessionState: nil,
        fetchFailed: false
    )
}

// MARK: - Data source protocol

/// Boundary protocol for the SwiftUI transcript peek. Conformers
/// resolve a `TranscriptPeekSnapshot` for a given asset id; the live
/// implementation wraps `AnalysisStore`, while previews / tests can
/// substitute a static snapshot without dragging persistence into UI
/// builds.
protocol TranscriptPeekDataSource: Sendable {
    /// Fetch a fresh snapshot for `assetId`. The returned snapshot
    /// always honors the dedup contract documented on
    /// `TranscriptPeekSnapshot.chunks`. The function does not throw —
    /// underlying errors are reported via `fetchFailed`.
    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot
}

// MARK: - Live implementation

/// Live data source backed by `AnalysisStore`. This is the ONLY type
/// in the transcript-peek path that references `AnalysisStore`; the
/// view model and the SwiftUI view consume `TranscriptPeekSnapshot`.
final class LiveTranscriptPeekDataSource: TranscriptPeekDataSource {

    private let store: AnalysisStore

    init(store: AnalysisStore) {
        self.store = store
    }

    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot {
        do {
            let freshChunks = try await store.fetchTranscriptChunks(assetId: assetId)
            let freshAds = try await store.fetchAdWindows(assetId: assetId)
            let freshSpans = try await store.fetchDecodedSpans(assetId: assetId)

            // Deduplicate: if both fast and final exist for the same
            // segment, prefer final. Group by chunkIndex, keep final
            // when available.
            let grouped = Dictionary(grouping: freshChunks, by: { $0.chunkIndex })
            let deduped = grouped.values.map { group -> TranscriptChunk in
                group.first(where: { $0.pass == "final" }) ?? group[0]
            }
            .sorted { $0.startTime < $1.startTime }

            // Asset coverage + session state (best-effort; treat as
            // optional so partial failures don't blow away the
            // chunks/ads/spans we just resolved).
            let asset = (try? await store.fetchAsset(id: assetId)) ?? nil
            let session = (try? await store.fetchLatestSessionForAsset(assetId: assetId)) ?? nil

            return TranscriptPeekSnapshot(
                chunks: deduped,
                rawChunkCount: freshChunks.count,
                adWindows: freshAds,
                decodedSpans: freshSpans,
                featureCoverageEnd: asset?.featureCoverageEndTime,
                fastTranscriptCoverageEnd: asset?.fastTranscriptCoverageEndTime,
                latestSessionState: session?.state,
                fetchFailed: false
            )
        } catch {
            return TranscriptPeekSnapshot(
                chunks: [],
                rawChunkCount: 0,
                adWindows: [],
                decodedSpans: [],
                featureCoverageEnd: nil,
                fastTranscriptCoverageEnd: nil,
                latestSessionState: nil,
                fetchFailed: true
            )
        }
    }
}
