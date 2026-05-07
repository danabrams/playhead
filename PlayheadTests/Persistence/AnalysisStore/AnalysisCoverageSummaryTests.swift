// AnalysisCoverageSummaryTests.swift
// playhead-hygc.1.2: tests for the canonical pipeline-progress read model.
//
// Pre-hygc.1.2 the Activity / dogfood paths SUMmed fast-chunk durations
// (which double-counts overlapping chunks AND under-explains gaps) and
// transparently fell through to the asset's stale fast-transcript
// watermark whenever a chunk-derived value was absent. This suite pins
// the canonical reconciliation:
//   - interval-unioned fast coverage seconds (overlap-aware, gap-aware),
//   - high-water `MAX(endTime)` for fast and final-pass chunks,
//   - watermark fallback only when chunks are absent,
//   - per-field provenance tags so dogfood diagnostics expose whether a
//     displayed percentage came from real artifacts or a stale watermark.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisCoverageMath — interval union (playhead-hygc.1.2)")
struct AnalysisCoverageMathTests {

    /// Empty input is the trivial zero-coverage case and must not crash
    /// the pure helper.
    @Test("empty input produces zero seconds")
    func emptyIntervalsProduceZero() {
        let result = AnalysisCoverageMath.unionedSeconds([])
        #expect(result == 0)
    }

    /// Disjoint intervals: union seconds equal the sum of widths.
    @Test("disjoint intervals: union equals sum of widths")
    func disjointIntervalsUnionEqualsSum() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 10),
            (start: 20, end: 25),
            (start: 30, end: 50)
        ])
        // 10 + 5 + 20 = 35
        #expect(result == 35)
    }

    /// Touching intervals (`end1 == start2`) collapse into a single
    /// covered range. The Swift union sweep treats them as a single
    /// continuous run.
    @Test("touching intervals collapse")
    func touchingIntervalsCollapse() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 10),
            (start: 10, end: 20)
        ])
        #expect(result == 20)
    }

    /// Overlapping intervals: the canonical reason the old SUM(...)
    /// query was wrong. Sum would say 100+50=150; union says 100.
    @Test("overlapping intervals — union differs from sum")
    func overlappingIntervalsUnionLessThanSum() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 100),
            (start: 50, end: 100)
        ])
        // Sum-of-widths = 150, union = 100.
        #expect(result == 100)
    }

    /// Fully-contained intervals also collapse to the outer width.
    @Test("contained intervals collapse to outer width")
    func containedIntervalsCollapse() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 100),
            (start: 25, end: 75)
        ])
        #expect(result == 100)
    }

    /// Identical intervals must not double-count.
    @Test("identical intervals — single coverage")
    func identicalIntervalsCoverOnce() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 5, end: 15),
            (start: 5, end: 15)
        ])
        #expect(result == 10)
    }

    /// Degenerate intervals (`end <= start`) contribute zero. The
    /// production query skips them at SQL-emit time but the math helper
    /// must also be defensive (the helper is reachable independently of
    /// the SQL filter).
    @Test("degenerate intervals contribute zero")
    func degenerateIntervalsContributeZero() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 5, end: 5),
            (start: 10, end: 10),
            (start: 100, end: 50),
            (start: 0, end: 7)
        ])
        #expect(result == 7)
    }

    /// Intervals presented in arbitrary order must produce the same
    /// answer as sorted input — sort stability is internal to the helper.
    @Test("unsorted input matches sorted result")
    func unsortedInputProducesSameResult() {
        let sorted = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 10),
            (start: 20, end: 30),
            (start: 25, end: 50)
        ])
        let unsorted = AnalysisCoverageMath.unionedSeconds([
            (start: 25, end: 50),
            (start: 0, end: 10),
            (start: 20, end: 30)
        ])
        // Sorted: [0,10] + ([20,30] ∪ [25,50] = [20,50]) → 10 + 30 = 40
        #expect(sorted == 40)
        #expect(sorted == unsorted)
    }

    /// Real-world shape: gapped chunks. Union must reflect the gap,
    /// NOT the extent (so a row with a 10-min gap doesn't claim full
    /// coverage just because chunks reach the end of the audio).
    @Test("gapped chunks — union seconds reflect the gap, not the extent")
    func gappedChunksReflectGap() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 600),
            (start: 1200, end: 1800)
        ])
        // 600 + 600 = 1200 covered, NOT 1800 (extent).
        #expect(result == 1200)
    }
}

@Suite("AnalysisStore.fetchCoverageSummariesByAssetIds (playhead-hygc.1.2)")
struct AnalysisStoreFetchCoverageSummariesTests {

    private func makeAsset(
        id: String,
        episodeDurationSec: Double? = 300,
        featureCoverageEndTime: Double? = nil,
        fastTranscriptCoverageEndTime: Double? = nil,
        confirmedAdCoverageEndTime: Double? = nil,
        finalPassCoverageEndTime: Double? = nil,
        analysisState: String = "queued"
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///\(id).m4a",
            featureCoverageEndTime: featureCoverageEndTime,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: confirmedAdCoverageEndTime,
            analysisState: analysisState,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec,
            finalPassCoverageEndTime: finalPassCoverageEndTime
        )
    }

    private func makeChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        pass: String = "fast"
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(index)-\(pass)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp-\(index)-\(pass)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "segment \(index)",
            normalizedText: "segment \(index)",
            pass: pass,
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// (a) overlapping fast chunks: union, NOT sum. The canonical
    /// regression: an asset with chunks 0..200 and 100..300 has true
    /// covered audio of 300s, NOT 200+200=400.
    @Test("(a) overlapping fast chunks union to the unique covered range")
    func overlappingFastChunksUnionNotSum() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-overlap",
            episodeDurationSec: 600,
            fastTranscriptCoverageEndTime: 50 // intentionally stale
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-overlap", index: 0, start: 0, end: 200),
            makeChunk(assetId: "a-overlap", index: 1, start: 100, end: 300)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-overlap"])
        let summary = try #require(summaries["a-overlap"])
        // Sum-of-widths would be 400; union is 300.
        #expect(summary.fastTranscriptCoveredSec == 300)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
        // High-water max(endTime) is 300 — disjoint signal.
        #expect(summary.fastTranscriptCoverageEndSec == 300)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
    }

    /// (b) gapped fast chunks: union reflects the gap (500s), high-water
    /// reflects max end (1800s). These are two distinct numbers and must
    /// stay distinct.
    @Test("(b) gapped fast chunks: union ≠ high-water max")
    func gappedFastChunksDistinguishUnionFromHighWater() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-gap",
            episodeDurationSec: 1800
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-gap", index: 0, start: 0, end: 200),
            makeChunk(assetId: "a-gap", index: 1, start: 200, end: 500),
            // Gap of 1000s.
            makeChunk(assetId: "a-gap", index: 2, start: 1500, end: 1800)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-gap"])
        let summary = try #require(summaries["a-gap"])
        // Union: 500 + 300 = 800.
        #expect(summary.fastTranscriptCoveredSec == 800)
        // High-water: 1800.
        #expect(summary.fastTranscriptCoverageEndSec == 1800)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
    }

    /// (c) Stale watermark + complete chunks → display follows the
    /// chunks. This is the asset_004 dogfood signal in a unit test: the
    /// stored watermark says 90s but real chunks cover 3960s.
    @Test("(c) stale watermark + complete chunks → coverage follows chunks")
    func staleWatermarkOverriddenByChunks() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-stale",
            episodeDurationSec: 4000,
            // Mimics asset_004: claimed full coverage with a
            // 90-second watermark.
            fastTranscriptCoverageEndTime: 90,
            analysisState: "completeFull"
        ))
        // Single dense run of fast chunks reaching 3960s.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-stale", index: 0, start: 0, end: 3960)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-stale"])
        let summary = try #require(summaries["a-stale"])
        #expect(summary.fastTranscriptCoveredSec == 3960)
        #expect(summary.fastTranscriptCoverageEndSec == 3960)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
    }

    /// (e) Feature-only analysis coverage (no confirmed-ad rows) still
    /// produces a meaningful summary. Provenance for an unset
    /// confirmed-ad column is `unknown`, not a synthetic 0.
    @Test("(e) feature coverage populated, no confirmed-ad rows")
    func featureOnlyCoverageProvenance() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-feature",
            featureCoverageEndTime: 250
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-feature"])
        let summary = try #require(summaries["a-feature"])
        #expect(summary.featureCoverageEndSec == 250)
        #expect(summary.featureCoverageEndSource == .assetWatermark)
        #expect(summary.confirmedAdCoverageEndSec == nil)
        #expect(summary.confirmedAdCoverageEndSource == .unknown)
    }

    /// (f) Confirmed-ad coverage exceeding feature coverage stays in the
    /// summary unchanged — the read model surfaces both numbers; the
    /// caller decides how to combine them. This pins the per-field
    /// provenance.
    @Test("(f) confirmed-ad coverage exceeds feature coverage")
    func confirmedAdExceedsFeatureCoverage() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-ad",
            featureCoverageEndTime: 100,
            confirmedAdCoverageEndTime: 200
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-ad"])
        let summary = try #require(summaries["a-ad"])
        #expect(summary.featureCoverageEndSec == 100)
        #expect(summary.featureCoverageEndSource == .assetWatermark)
        #expect(summary.confirmedAdCoverageEndSec == 200)
        #expect(summary.confirmedAdCoverageEndSource == .assetWatermark)
    }

    /// (g) Final-pass coverage from chunks beats the asset watermark
    /// column when both exist; provenance becomes `final_pass_chunks`.
    @Test("(g) final-pass chunk MAX(endTime) wins over watermark column")
    func finalPassChunksOverrideFinalPassWatermark() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-final",
            episodeDurationSec: 600,
            // Watermark column says 100 — chunks reach further.
            finalPassCoverageEndTime: 100
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-final", index: 0, start: 0, end: 400, pass: "final"),
            makeChunk(assetId: "a-final", index: 1, start: 0, end: 100, pass: "fast")
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-final"])
        let summary = try #require(summaries["a-final"])
        #expect(summary.finalPassCoverageEndSec == 400)
        #expect(summary.finalPassCoverageEndSource == .finalPassChunks)
    }

    /// Final-pass watermark fallback: when only the asset column has a
    /// value, provenance must be `asset_watermark` (not `unknown`).
    @Test("final-pass: chunks absent → asset watermark used as fallback")
    func finalPassFallsBackToAssetWatermark() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-final-wm",
            episodeDurationSec: 600,
            finalPassCoverageEndTime: 250
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-final-wm"])
        let summary = try #require(summaries["a-final-wm"])
        #expect(summary.finalPassCoverageEndSec == 250)
        #expect(summary.finalPassCoverageEndSource == .assetWatermark)
    }

    /// (h) All-nil artifacts → unknown progress (not synthetic 0%).
    @Test("(h) all-nil artifacts → unknown provenance, no synthetic zero")
    func allNilArtifactsProduceUnknown() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-empty",
            episodeDurationSec: nil
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-empty"])
        let summary = try #require(summaries["a-empty"])
        #expect(summary.episodeDurationSec == nil)
        #expect(summary.fastTranscriptCoveredSec == nil)
        #expect(summary.fastTranscriptCoveredSource == .unknown)
        #expect(summary.fastTranscriptCoverageEndSec == nil)
        #expect(summary.fastTranscriptCoverageEndSource == .unknown)
        #expect(summary.featureCoverageEndSec == nil)
        #expect(summary.featureCoverageEndSource == .unknown)
        #expect(summary.confirmedAdCoverageEndSec == nil)
        #expect(summary.confirmedAdCoverageEndSource == .unknown)
        #expect(summary.finalPassCoverageEndSec == nil)
        #expect(summary.finalPassCoverageEndSource == .unknown)
    }

    /// Watermark fallback for fast coverage: chunks absent, watermark
    /// present → covered/end seconds both come from the watermark with
    /// `asset_watermark` provenance. Pins that the read model does NOT
    /// silently emit `unknown` when the asset has at least the
    /// scheduler watermark to show.
    @Test("fast coverage falls back to asset watermark when chunks absent")
    func fastCoverageFallsBackToWatermarkWhenChunksAbsent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-only-wm",
            episodeDurationSec: 600,
            fastTranscriptCoverageEndTime: 120
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-only-wm"])
        let summary = try #require(summaries["a-only-wm"])
        #expect(summary.fastTranscriptCoveredSec == 120)
        #expect(summary.fastTranscriptCoveredSource == .assetWatermark)
        #expect(summary.fastTranscriptCoverageEndSec == 120)
        #expect(summary.fastTranscriptCoverageEndSource == .assetWatermark)
    }

    /// Empty input must short-circuit to an empty dictionary without
    /// preparing a SQL statement at all (zero placeholders is otherwise
    /// invalid SQL: `WHERE id IN ()`).
    @Test("empty input returns empty dictionary")
    func emptyInputReturnsEmpty() async throws {
        let store = try await makeTestStore()
        let result = try await store.fetchCoverageSummariesByAssetIds([])
        #expect(result.isEmpty)
    }

    /// Inputs larger than the chunk size (500) must still produce a
    /// single dictionary covering every present asset. Mirrors the
    /// `BulkAssetFetchTests` chunk-size regression.
    @Test("input above chunk-size boundary produces full result")
    func inputAboveChunkSizeProducesFullResult() async throws {
        let store = try await makeTestStore()
        let n = 750
        var ids: Set<String> = []
        ids.reserveCapacity(n)
        for i in 0..<n {
            let id = "a-bulk-\(i)"
            ids.insert(id)
            try await store.insertAsset(makeAsset(
                id: id,
                episodeDurationSec: 300,
                fastTranscriptCoverageEndTime: 30
            ))
        }
        let result = try await store.fetchCoverageSummariesByAssetIds(ids)
        #expect(result.count == n)
    }
}

@Suite("Dogfood fixture integration: asset_004 chunk-vs-watermark (playhead-hygc.1.2)")
struct DogfoodFixtureCoverageSummaryTests {

    /// Replays the asset_004 contradiction directly through the store-
    /// level read model: insert the asset with the dogfood-captured
    /// stale watermark and a single dense fast-chunk reaching ~3960s,
    /// then assert the canonical summary picks up the chunk maxima
    /// rather than the 90s watermark.
    ///
    /// Why we don't just decode the fixture and replay every row: the
    /// fixture is sanitized for SHARED dogfood signals (correction-row
    /// duplicates, terminal-state contradictions, FA event histograms)
    /// — its `transcript_chunk_maxima` rows are aggregates, not full
    /// chunk dumps, so we can't faithfully repopulate `transcript_chunks`
    /// from it without inventing chunk boundaries the fixture doesn't
    /// pin. Driving the store with the fixture's headline numbers is the
    /// right contract: "given the dogfood-shaped data, the read model
    /// reports chunk-derived coverage, not watermark-derived".
    @Test("fixture-shaped inputs: asset_004 coverage reflects chunk maxima")
    func dogfoodAsset004CoverageReflectsChunkMaxima() async throws {
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        // Pull the named asset's headline shape from the fixture so this
        // test breaks loudly if a future fixture regenerate moves the
        // signal off asset_004.
        let analysisAssets = fixture.analysisAssets
        let assetFixtureOpt = analysisAssets.first(where: { row in
            row.id == "asset_004"
        })
        let assetFixture = try #require(
            assetFixtureOpt,
            "fixture must continue to contain asset_004"
        )
        let chunkMaxima = fixture.transcriptChunkMaxima
        let chunkMaxOpt = chunkMaxima.first(where: { row in
            row.assetId == "asset_004" && row.pass == "fast"
        })
        let chunkMax = try #require(
            chunkMaxOpt,
            "fixture must continue to contain asset_004 fast chunk maxima"
        )

        // Sanity: the contradiction the dogfood capture documents must
        // still be present in the fixture — the watermark must be far
        // smaller than the chunk max. (If a regenerated fixture removed
        // the signal, the rest of this test would silently pass against
        // a healthy asset and stop guarding the regression.)
        let watermark = try #require(assetFixture.fastTranscriptCoverageEndSec)
        #expect(chunkMax.maxEndTimeSec > watermark + 1000,
                "fixture asset_004 no longer carries the >>watermark chunk-coverage signal: chunk \(chunkMax.maxEndTimeSec)s vs. watermark \(watermark)s")

        let store = try await makeTestStore()
        try await store.insertAsset(AnalysisAsset(
            id: "asset_004",
            episodeId: "ep-asset_004",
            assetFingerprint: "fp-asset_004",
            weakFingerprint: nil,
            sourceURL: "file:///asset_004.m4a",
            featureCoverageEndTime: assetFixture.featureCoverageEndSec,
            fastTranscriptCoverageEndTime: assetFixture.fastTranscriptCoverageEndSec,
            confirmedAdCoverageEndTime: assetFixture.confirmedAdCoverageEndSec,
            analysisState: assetFixture.analysisState,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: assetFixture.episodeDurationSec,
            finalPassCoverageEndTime: assetFixture.finalPassCoverageEndSec
        ))
        // Single dense fast chunk reaching the dogfood-captured max
        // end. The fixture aggregates 4334 chunks into a single max-
        // end-time row; replaying that as one dense interval gives the
        // same chunk-derived coverage answer (the read model only cares
        // about the union and the high-water).
        try await store.insertTranscriptChunks([
            TranscriptChunk(
                id: "asset_004-chunk-0-fast",
                analysisAssetId: "asset_004",
                segmentFingerprint: "asset_004-fp-0-fast",
                chunkIndex: 0,
                startTime: 0,
                endTime: chunkMax.maxEndTimeSec,
                text: "dogfood-asset_004",
                normalizedText: "dogfood-asset_004",
                pass: "fast",
                modelVersion: "test-asr",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["asset_004"])
        let summary = try #require(summaries["asset_004"])
        // Chunk maxima dominate, not the 90-second watermark.
        #expect(summary.fastTranscriptCoverageEndSec == chunkMax.maxEndTimeSec)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
        // The 90s watermark must NOT be the surfaced value.
        #expect(summary.fastTranscriptCoverageEndSec != watermark)
        // Bead spec: the asset_004 chunk maxima are ~3959.77s (66 min).
        // Pin the order of magnitude so a future fixture regenerate
        // that subtly weakens the signal can't quietly slip past.
        #expect(summary.fastTranscriptCoverageEndSec ?? 0 > 3000,
                "expected chunk-derived coverage to clear 50 minutes; got \(summary.fastTranscriptCoverageEndSec ?? 0)s")
        #expect(summary.fastTranscriptCoveredSec == chunkMax.maxEndTimeSec)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
    }
}
