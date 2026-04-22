// LiveShadowWindowSourceTests.swift
// playhead-narl.2 continuation: coverage for the live coarse-grid window
// source. Swift Testing suite.
//
// Coverage:
//   - `gridWindows(fromSeconds:toSeconds:strideSeconds:widthSeconds:)`:
//       * empty range → no windows
//       * exactly one stride of range → exactly one window
//       * straddling range truncates final window at `toSeconds`
//       * bounds are canonicalized to integer milliseconds
//   - `floorToGrid(_:stride:)`:
//       * snaps to stride multiple
//       * nonfinite passthrough
//   - `laneACandidates(...)`:
//       * starts at grid floor of fromSeconds
//       * filters by `alreadyCaptured` membership
//       * zero lookahead produces no windows
//   - `laneBCandidates(...)`:
//       * empty when transcript duration is 0
//       * covers full transcript duration
//   - `assetsWithIncompleteCoverage()`:
//       * returns assets with transcript but no shadow rows
//       * excludes assets that are fully covered

import Foundation
import Testing

@testable import Playhead

@Suite("LiveShadowWindowSource (playhead-narl.2)")
struct LiveShadowWindowSourceTests {

    // MARK: - gridWindows

    @Test("gridWindows: empty range yields no windows")
    func gridWindowsEmptyRange() {
        let ws = LiveShadowWindowSource.gridWindows(
            fromSeconds: 10, toSeconds: 10,
            strideSeconds: 30, widthSeconds: 30
        )
        #expect(ws.isEmpty)
    }

    @Test("gridWindows: single-stride range yields exactly one window")
    func gridWindowsSingleWindow() {
        let ws = LiveShadowWindowSource.gridWindows(
            fromSeconds: 0, toSeconds: 30,
            strideSeconds: 30, widthSeconds: 30
        )
        #expect(ws.count == 1)
        #expect(ws[0].start == 0)
        #expect(ws[0].end == 30)
    }

    @Test("gridWindows: final window truncates at toSeconds")
    func gridWindowsTruncates() {
        let ws = LiveShadowWindowSource.gridWindows(
            fromSeconds: 0, toSeconds: 45,
            strideSeconds: 30, widthSeconds: 30
        )
        #expect(ws.count == 2)
        #expect(ws[0].start == 0)
        #expect(ws[0].end == 30)
        #expect(ws[1].start == 30)
        #expect(ws[1].end == 45)
    }

    @Test("gridWindows: bounds are canonicalized to integer milliseconds")
    func gridWindowsCanonicalized() {
        // A start that's not aligned to ms boundaries rounds to the nearest
        // integer ms — downstream PKs stay stable.
        let ws = LiveShadowWindowSource.gridWindows(
            fromSeconds: 0.123456789, toSeconds: 0.654321,
            strideSeconds: 0.3, widthSeconds: 0.3
        )
        #expect(!ws.isEmpty)
        for w in ws {
            let startMs = (w.start * 1000.0).rounded()
            let endMs = (w.end * 1000.0).rounded()
            #expect(w.start == startMs / 1000.0)
            #expect(w.end == endMs / 1000.0)
        }
    }

    // MARK: - floorToGrid

    @Test("floorToGrid snaps down to the nearest stride multiple")
    func floorToGridSnap() {
        #expect(LiveShadowWindowSource.floorToGrid(0, stride: 30) == 0)
        #expect(LiveShadowWindowSource.floorToGrid(29.9, stride: 30) == 0)
        #expect(LiveShadowWindowSource.floorToGrid(30, stride: 30) == 30)
        #expect(LiveShadowWindowSource.floorToGrid(45, stride: 30) == 30)
        #expect(LiveShadowWindowSource.floorToGrid(67.5, stride: 30) == 60)
    }

    @Test("floorToGrid passes through non-finite seconds unchanged")
    func floorToGridNonFinite() {
        #expect(LiveShadowWindowSource.floorToGrid(.infinity, stride: 30).isInfinite)
        #expect(LiveShadowWindowSource.floorToGrid(.nan, stride: 30).isNaN)
    }

    // MARK: - laneACandidates

    @Test("laneACandidates starts at grid floor and filters alreadyCaptured")
    func laneACandidatesFiltersAndSnaps() async throws {
        let store = try await makeTestStore()
        let source = LiveShadowWindowSource(store: store, strideSeconds: 30, widthSeconds: 30)
        // Playhead at 37s, lookahead 60s → floor = 30, range = [30, 97].
        // Grid produces windows [30..60], [60..90], [90..97 truncated].
        // Simulate that [30..60] is already captured.
        let already: Set<ShadowWindowKey> = [
            ShadowWindowKey.canonical(start: 30, end: 60)
        ]
        let result = try await source.laneACandidates(
            assetId: "asset-a",
            fromSeconds: 37,
            lookaheadSeconds: 60,
            alreadyCaptured: already
        )
        #expect(result.count == 2)
        #expect(result[0].start == 60)
        #expect(result[0].end == 90)
        #expect(result[1].start == 90)
        #expect(result[1].end == 97)
    }

    @Test("laneACandidates with zero lookahead returns no windows")
    func laneACandidatesZeroLookahead() async throws {
        let store = try await makeTestStore()
        let source = LiveShadowWindowSource(store: store)
        let result = try await source.laneACandidates(
            assetId: "x", fromSeconds: 0, lookaheadSeconds: 0,
            alreadyCaptured: []
        )
        #expect(result.isEmpty)
    }

    /// Regression: without the explicit `lookaheadSeconds > 0` short-circuit,
    /// a non-zero `fromSeconds` with zero lookahead would fall through the
    /// `end > from` guard (end = 60, floored from = 60) and still return an
    /// empty result for this particular pair — but swap `fromSeconds` to a
    /// non-grid multiple (e.g. 61) and the guard silently admits a window.
    /// Pinning both scenarios here so any future regression trips a test.
    @Test("laneACandidates with zero lookahead at non-zero fromSeconds returns no windows")
    func laneACandidatesZeroLookaheadNonZeroFrom() async throws {
        let store = try await makeTestStore()
        let source = LiveShadowWindowSource(store: store, strideSeconds: 30, widthSeconds: 30)
        // On-grid `fromSeconds`: floored from=60, end=60 → old guard would
        // already reject. Included as a control case.
        let onGrid = try await source.laneACandidates(
            assetId: "x", fromSeconds: 60, lookaheadSeconds: 0,
            alreadyCaptured: []
        )
        #expect(onGrid.isEmpty)
        // Off-grid `fromSeconds`: floored from=60, end=61 → old guard would
        // admit a 60..61 window. The new `lookaheadSeconds > 0` short-circuit
        // keeps the backlog empty.
        let offGrid = try await source.laneACandidates(
            assetId: "x", fromSeconds: 61, lookaheadSeconds: 0,
            alreadyCaptured: []
        )
        #expect(offGrid.isEmpty)
    }

    // MARK: - laneBCandidates

    @Test("laneBCandidates: empty when asset has no transcript rows")
    func laneBCandidatesNoTranscript() async throws {
        let store = try await makeTestStore()
        let source = LiveShadowWindowSource(store: store)
        let result = try await source.laneBCandidates(
            assetId: "ghost",
            alreadyCaptured: []
        )
        #expect(result.isEmpty)
    }

    @Test("laneBCandidates covers the full transcript duration")
    func laneBCandidatesFullCoverage() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, id: "asset-b")
        try await seedTranscriptChunk(
            store: store, assetId: "asset-b",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0
        )
        try await seedTranscriptChunk(
            store: store, assetId: "asset-b",
            startTime: 30, endTime: 60, chunkIndex: 1, ordinal: 1
        )

        let source = LiveShadowWindowSource(
            store: store, strideSeconds: 30, widthSeconds: 30
        )
        let result = try await source.laneBCandidates(
            assetId: "asset-b", alreadyCaptured: []
        )
        #expect(result.count == 2)
        #expect(result[0].start == 0)
        #expect(result[0].end == 30)
        #expect(result[1].start == 30)
        #expect(result[1].end == 60)
    }

    // MARK: - assetsWithIncompleteCoverage

    @Test("assetsWithIncompleteCoverage lists uncovered assets, id-ASC tie-break")
    func assetsWithIncompleteCoverageOrdered() async throws {
        let store = try await makeTestStore()
        // Both have transcripts, no shadow rows → both are incomplete.
        // The store query orders by (createdAt ASC, id ASC). Pin both
        // rows' `createdAt` to the same value via the DEBUG-only setter
        // so the tie-break on id is exercised deterministically — no
        // reliance on `strftime('%s','now')` producing identical
        // second-granularity timestamps for back-to-back inserts.
        try await seedAsset(store: store, id: "asset-a")
        try await seedTranscriptChunk(
            store: store, assetId: "asset-a",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0
        )
        try await seedAsset(store: store, id: "asset-b")
        try await seedTranscriptChunk(
            store: store, assetId: "asset-b",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0
        )
        // Pin identical createdAt timestamps so id-ASC is the sole
        // tie-break axis. Value is arbitrary — just needs to match.
        try await store.setAssetCreatedAtForTesting(id: "asset-a", createdAt: 1_700_000_000)
        try await store.setAssetCreatedAtForTesting(id: "asset-b", createdAt: 1_700_000_000)

        let source = LiveShadowWindowSource(store: store)
        let result = try await source.assetsWithIncompleteCoverage()
        #expect(result.contains("asset-a"))
        #expect(result.contains("asset-b"))
        // Assert absolute positions rather than the old silent-pass guard:
        // if either id is absent from the result, `firstIndex(of:)` returns
        // nil and the test should fail, not silently pass.
        let ai = try #require(result.firstIndex(of: "asset-a"))
        let bi = try #require(result.firstIndex(of: "asset-b"))
        #expect(ai < bi, "id-ASC tie-break should place asset-a before asset-b")
    }

    @Test("assetsWithIncompleteCoverage excludes fully covered assets")
    func assetsWithIncompleteCoverageExcludesCovered() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, id: "covered")
        try await seedTranscriptChunk(
            store: store, assetId: "covered",
            startTime: 0, endTime: 30, chunkIndex: 0, ordinal: 0
        )
        // Insert a matching shadow row so the asset appears fully covered
        // at (stride=30, width=30, duration=30) → expected 1 window.
        let row = ShadowFMResponse(
            assetId: "covered",
            windowStart: 0,
            windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0x00]),
            capturedAt: 1_700_000_000,
            capturedBy: .laneB,
            fmModelVersion: "fm-test"
        )
        try await store.upsertShadowFMResponse(row)

        let source = LiveShadowWindowSource(store: store)
        let result = try await source.assetsWithIncompleteCoverage()
        #expect(!result.contains("covered"))
    }
}

// MARK: - seeding helpers

private func seedAsset(
    store: AnalysisStore,
    id: String
) async throws {
    let asset = AnalysisAsset(
        id: id,
        episodeId: "episode-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///tmp/\(id).mp3",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
    try await store.insertAsset(asset)
}

private func seedTranscriptChunk(
    store: AnalysisStore,
    assetId: String,
    startTime: TimeInterval,
    endTime: TimeInterval,
    chunkIndex: Int,
    ordinal: Int
) async throws {
    let chunk = TranscriptChunk(
        id: "chunk-\(assetId)-\(chunkIndex)",
        analysisAssetId: assetId,
        segmentFingerprint: "seg-\(assetId)-\(chunkIndex)",
        chunkIndex: chunkIndex,
        startTime: startTime,
        endTime: endTime,
        text: "text \(chunkIndex)",
        normalizedText: "text \(chunkIndex)",
        pass: "fast",
        modelVersion: "test.v1",
        transcriptVersion: nil,
        atomOrdinal: ordinal,
        weakAnchorMetadata: nil,
        speakerId: nil
    )
    try await store.insertTranscriptChunk(chunk)
}
