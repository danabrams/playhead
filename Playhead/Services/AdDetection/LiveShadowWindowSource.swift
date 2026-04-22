// LiveShadowWindowSource.swift
// playhead-narl.2: Live `ShadowWindowSource` that enumerates candidate
// windows via a coarse fixed-width grid over the asset timeline.
//
// Q4 decision (narl.2 continuation): D — coarse-grid candidate
// enumerator, NOT a config-variant simulator. The question of "what does
// `.allEnabled` schedule that `.default` skips" is answered DOWNSTREAM by
// narl.1/narl.3 when they compare persisted shadow rows against
// production decisions. This source's job is simply to produce a
// deterministic backlog of windows that cover the full asset timeline.
//
// Grid shape:
//   - Stride = 30 seconds, width = 30 seconds (one non-overlapping
//     window per 30s slice). The bead spec leaves this tunable; 30s
//     matches the refinement-window grain used elsewhere in the
//     pipeline, which keeps the backlog size bounded (a typical 60-min
//     episode → 120 windows).
//   - All window bounds canonicalized through
//     `ShadowFMResponse.canonicalize(seconds:)` so they round-trip
//     through the REAL-typed PK without IEEE-754 drift.
//
// `assetsWithIncompleteCoverage()` compares the window count the grid
// would produce against the count already in `shadow_fm_responses`. An
// asset with a transcript but zero shadow responses is listed; one
// with a full set is not. The ordering is "earliest-gap-first" by
// `createdAt`, so Lane B walks assets deterministically.

import Foundation
import os

/// Live `ShadowWindowSource` backed by `AnalysisStore`. Produces
/// coarse-grid candidate windows and filters out windows already present
/// in `shadow_fm_responses` via the set handed in by the coordinator.
///
/// Grid parameters are injectable so tests can drive the source with
/// smaller strides without rewriting this logic.
actor LiveShadowWindowSource: ShadowWindowSource {

    /// Default grid stride, in seconds. Matches the coarse/refinement
    /// window grain used elsewhere in the pipeline.
    static let defaultStrideSeconds: TimeInterval = 30

    /// Default grid window width, in seconds. Equal to `stride` so windows
    /// tile without gaps or overlaps. Distinct from `stride` so future
    /// tuning (e.g. overlap for boundary refinement) doesn't require
    /// re-threading both constants.
    static let defaultWidthSeconds: TimeInterval = 30

    private let store: AnalysisStore
    private let strideSeconds: TimeInterval
    private let widthSeconds: TimeInterval
    private let logger: Logger

    init(
        store: AnalysisStore,
        strideSeconds: TimeInterval = LiveShadowWindowSource.defaultStrideSeconds,
        widthSeconds: TimeInterval = LiveShadowWindowSource.defaultWidthSeconds,
        logger: Logger = Logger(
            subsystem: "com.playhead",
            category: "ShadowWindowSource"
        )
    ) {
        precondition(strideSeconds > 0, "stride must be positive")
        precondition(widthSeconds > 0, "width must be positive")
        self.store = store
        self.strideSeconds = strideSeconds
        self.widthSeconds = widthSeconds
        self.logger = logger
    }

    // MARK: - ShadowWindowSource

    func laneACandidates(
        assetId: String,
        fromSeconds: TimeInterval,
        lookaheadSeconds: TimeInterval,
        alreadyCaptured: Set<ShadowWindowKey>
    ) async throws -> [ShadowWindow] {
        // Lane A: windows in `[fromSeconds, fromSeconds + lookaheadSeconds]`.
        // Short-circuit a zero/negative lookahead explicitly. The later
        // `end > from` guard handles the common case but silently admits
        // windows when `fromSeconds > 0 && lookaheadSeconds == 0` (end
        // still exceeds the grid-floored `from`). That combination isn't
        // realistic today but a future hot-flip of the lookahead to zero
        // should produce an empty backlog, not a full grid slice.
        guard lookaheadSeconds > 0 else { return [] }
        // Clamp `from` to the grid's origin so moving windows produce
        // stable keys: Lane A dispatched at playhead=7s and at playhead=8s
        // should resolve to the same 0..30 window, not 7..37 and 8..38.
        let from = max(0, Self.floorToGrid(fromSeconds, stride: strideSeconds))
        let end = fromSeconds + max(0, lookaheadSeconds)
        guard end > from else { return [] }

        let windows = Self.gridWindows(
            fromSeconds: from,
            toSeconds: end,
            strideSeconds: strideSeconds,
            widthSeconds: widthSeconds
        )
        return windows.filter { window in
            !alreadyCaptured.contains(
                ShadowWindowKey.canonical(start: window.start, end: window.end)
            )
        }
    }

    func laneBCandidates(
        assetId: String,
        alreadyCaptured: Set<ShadowWindowKey>
    ) async throws -> [ShadowWindow] {
        // Lane B: cover the full asset. Use the transcript's max endTime as
        // duration so we only generate windows for timeline we actually
        // have text for. Assets with no transcript rows yield an empty
        // candidate list (nothing to do).
        let duration = try await transcriptDurationSeconds(assetId: assetId)
        guard duration > 0 else { return [] }

        let windows = Self.gridWindows(
            fromSeconds: 0,
            toSeconds: duration,
            strideSeconds: strideSeconds,
            widthSeconds: widthSeconds
        )
        // Filter against already-captured; the remaining list is already
        // earliest-first because `gridWindows` emits in ascending order.
        return windows.filter { window in
            !alreadyCaptured.contains(
                ShadowWindowKey.canonical(start: window.start, end: window.end)
            )
        }
    }

    func assetsWithIncompleteCoverage() async throws -> [String] {
        // Enumerate every asset that has at least one transcript chunk
        // (a prerequisite for generating a shadow window) and compare
        // the grid-implied window count to the `shadow_fm_responses`
        // row count. Assets where the latter is lower are incomplete.
        //
        // Ordering: by ascending `createdAt` so long-standing gaps get
        // worked down before fresh ones. Ties broken by id for stability.
        return try await store.assetsWithIncompleteShadowCoverage(
            strideSeconds: strideSeconds,
            widthSeconds: widthSeconds,
            configVariant: ShadowConfigVariant.allEnabledShadow.rawValue
        )
    }

    // MARK: - Helpers

    /// Build coarse-grid windows covering `[fromSeconds, toSeconds]`. The
    /// first window starts at the grid floor of `fromSeconds` (already
    /// computed by the caller for Lane A) or at 0 (Lane B). Each window
    /// has fixed `widthSeconds`; adjacent windows are `strideSeconds`
    /// apart. The final window is truncated at `toSeconds` so Lane A
    /// lookaheads that straddle the asset end never dispatch past the
    /// transcript. All bounds canonicalized for PK stability.
    static func gridWindows(
        fromSeconds: TimeInterval,
        toSeconds: TimeInterval,
        strideSeconds: TimeInterval,
        widthSeconds: TimeInterval
    ) -> [ShadowWindow] {
        guard toSeconds > fromSeconds else { return [] }
        var windows: [ShadowWindow] = []
        var cursor = fromSeconds
        while cursor < toSeconds {
            let rawStart = cursor
            let rawEnd = min(cursor + widthSeconds, toSeconds)
            // Skip degenerate windows (start == end after clamping).
            if rawEnd > rawStart {
                let start = ShadowFMResponse.canonicalize(seconds: rawStart)
                let end = ShadowFMResponse.canonicalize(seconds: rawEnd)
                windows.append(ShadowWindow(start: start, end: end))
            }
            cursor += strideSeconds
        }
        return windows
    }

    /// Snap a seconds value down to the nearest multiple of `stride`. Used
    /// by Lane A so moving playheads resolve to a stable window key.
    static func floorToGrid(_ seconds: TimeInterval, stride: TimeInterval) -> TimeInterval {
        guard stride > 0, seconds.isFinite else { return seconds }
        let n = (seconds / stride).rounded(.down)
        return n * stride
    }

    /// Pull the transcript's last `endTime` for `assetId`. Returns `0`
    /// when the asset has no transcript rows, which the caller treats as
    /// "nothing to do".
    private func transcriptDurationSeconds(assetId: String) async throws -> TimeInterval {
        let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
        return chunks.map(\.endTime).max() ?? 0
    }
}
