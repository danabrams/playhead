// TranscriptPeekTailMarkTests.swift
// playhead-m1l9: decouple markability from transcript coverage.
//
// Dogfood bug (THEMOVE, 2026-07-22): the user wanted to mark a POST-ROLL ad
// but the fast transcript hadn't reached the episode end, so the post-roll
// was both UNCOVERED and UNMARKABLE. The chunk-selection "Mark ad" flow can
// only assemble a span from transcript chunks that exist (chunks extend only
// to `fastTranscriptCoverageEndTime`); with no chunks in the tail there was
// nothing to tap and `submitMarkedChunks` early-returned.
//
// The fix surfaces a coverage-FREE affordance from the transcript peek's
// untranscribed tail that routes to the SAME `injectUserMarkedAd` /
// `recordUserMarkedAd` path the player "Hearing an ad" button uses, seeding a
// span from the playhead to the episode end.
//
// These tests exercise the smallest layer that catches the coverage-gating
// regression: the `TranscriptPeekViewModel` tail-span computation feeding the
// arbitrary-span `AdDetectionService.recordUserMarkedAd` persistence — NOT a
// full UI test (`submitMarkedChunks` is @State-bound on the SwiftUI view; see
// `TranscriptPeekNotAdModeTests`).

import Foundation
import Testing

@testable import Playhead

// MARK: - Stub data source

/// A `TranscriptPeekDataSource` that returns one fixed snapshot, so the view
/// model can be driven through a single deterministic `refresh()` without the
/// live `AnalysisStore`.
private struct FixedTranscriptPeekDataSource: TranscriptPeekDataSource {
    let snapshot: TranscriptPeekSnapshot
    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot { snapshot }
}

// MARK: - Builders

private enum TailMark {

    /// Fast-pass chunks covering [0, transcribedEnd) at 600s (10 min) each,
    /// with a trailing partial row when `transcribedEnd` is not a multiple of
    /// the width. NONE extend past `transcribedEnd`.
    static func coveredChunks(assetId: String, coverageEnd transcribedEnd: Double) -> [TranscriptChunk] {
        let width = 600.0
        var bounds: [(start: Double, end: Double)] = []
        var cursor = 0.0
        while cursor < transcribedEnd {
            bounds.append((start: cursor, end: min(cursor + width, transcribedEnd)))
            cursor += width
        }
        return bounds.enumerated().map { i, span in
            TranscriptChunk(
                id: "c\(i)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(i)",
                chunkIndex: i,
                startTime: span.start,
                endTime: span.end,
                text: "covered segment \(i)",
                normalizedText: "covered segment \(i)",
                pass: "fast",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    static func snapshot(chunks: [TranscriptChunk], coverageEnd: Double) -> TranscriptPeekSnapshot {
        TranscriptPeekSnapshot(
            chunks: chunks,
            rawChunkCount: chunks.count,
            adWindows: [],
            decodedSpans: [],
            featureCoverageEnd: coverageEnd,
            fastTranscriptCoverageEnd: coverageEnd,
            latestSessionState: "backfill",
            fetchFailed: false
        )
    }

    /// A view model whose transcript rows reach `transcribedEnd` while the
    /// fast-transcript SCAN watermark reaches `scannedEnd`. The gap between
    /// them is territory the pass looked at and found no speech in — shards
    /// that produced zero segments still advance coverage (playhead-7tn8).
    @MainActor
    static func viewModel(
        assetId: String,
        transcribedEnd: Double,
        scannedEnd: Double
    ) async -> TranscriptPeekViewModel {
        let chunks = coveredChunks(assetId: assetId, coverageEnd: transcribedEnd)
        let source = FixedTranscriptPeekDataSource(
            snapshot: snapshot(chunks: chunks, coverageEnd: scannedEnd)
        )
        let vm = TranscriptPeekViewModel(analysisAssetId: assetId, dataSource: source)
        await vm.refresh()
        return vm
    }

    /// The ordinary shape: every scanned second produced speech, so the
    /// transcript evidence and the scan watermark coincide.
    @MainActor
    static func viewModel(assetId: String, coverageEnd: Double) async -> TranscriptPeekViewModel {
        await viewModel(assetId: assetId, transcribedEnd: coverageEnd, scannedEnd: coverageEnd)
    }

    static func service(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "test-detection-v1",
                fmBackfillMode: .off
            )
        )
    }
}

// MARK: - Regression: mark past coverage → userMarked AdWindow persists

@Suite("playhead-m1l9 — mark an untranscribed-tail ad past transcript coverage")
@MainActor
struct TranscriptPeekTailMarkTests {

    /// CRUX (bead-required). Transcript coverage reaches 1800s (30:00); the
    /// episode is 1900s (31:40). The user is listening to the post-roll at
    /// 1850s — PAST coverage, in territory with NO transcript chunks. The peek's
    /// coverage-free tail affordance must produce a span [playhead, episodeEnd]
    /// that persists via `recordUserMarkedAd` as a `userMarked` AdWindow with
    /// EXACTLY those bounds — proving markability is decoupled from coverage.
    ///
    /// Tightening ("what broken impl would still pass this?"):
    ///   * end clamped to coverage (the pre-fix chunk-derived bound) → `end ==
    ///     episodeDuration` FAILS.
    ///   * start clamped to coverage → `start > coverageEnd` FAILS (start would
    ///     equal, not exceed, the watermark).
    ///   * the mark actually landing in transcribed territory → the no-chunk
    ///     assertion FAILS (chunks exist only up to coverage).
    ///   * `recordUserMarkedAd` dropping the 527u eligibility stamp → the gate
    ///     assertion FAILS (regressing auto-skip-as-definitive).
    @Test("a post-roll span entirely past coverage persists as userMarked with those exact bounds")
    func markPastCoveragePersistsWithBounds() async throws {
        let assetId = "asset-m1l9-tail"
        let coverageEnd = 1800.0
        let currentTime = 1850.0
        let episodeDuration = 1900.0

        // (1) View-model layer: the coverage-free tail span.
        let vm = await TailMark.viewModel(assetId: assetId, coverageEnd: coverageEnd)
        #expect(vm.fastTranscriptCoverageEndTime == coverageEnd,
                "the coverage watermark must flow from the snapshot into the view model")
        // CONTRACT CHANGE (playhead-7tn8). This assertion used to read "no
        // chunk extends past coverage, so lastCoveredTime is the watermark",
        // pinning the watermark-dominant branch as what the affordance keys
        // on. That was wrong once playhead-0sro made the watermark truthful: a
        // shard yielding ZERO segments still advances it, so on a music-bedded
        // post-roll the watermark ran past the last chunk and suppressed the
        // affordance in exactly the case it exists for. The affordance now
        // keys on transcript EVIDENCE (`lastTranscribedTime`). The watermark is
        // kept — it still answers the distinct "did we look here?" question
        // that separates a transient un-analyzed tail from a silent blind spot.
        //
        // In THIS fixture the fast pass found speech across everything it
        // scanned, so both values are the coverage end and m1l9's behaviour is
        // unchanged; `silentShardPostRollIsStillMarkable` covers the shape
        // where they diverge.
        #expect(vm.lastTranscribedTime == coverageEnd,
                "no chunk extends past coverage, so the transcript evidence ends at the coverage end")
        #expect(vm.lastCoveredTime == coverageEnd,
                "the pass found speech everywhere it looked, so the scan watermark coincides")
        #expect(vm.tailCoverage(at: currentTime) == .notYetScanned,
                "the post-roll is past the scan watermark — analysis has not reached it")

        let span = try #require(
            vm.untranscribedTailMarkSpan(
                currentTime: currentTime,
                episodeDuration: episodeDuration
            ),
            "a playhead past coverage inside a post-roll-sized tail must yield a mark span"
        )
        #expect(span.start == currentTime, "the tail mark starts at the playhead")
        #expect(span.end == episodeDuration, "the tail mark runs all the way to the episode end")
        #expect(span.start > coverageEnd, "the span is ENTIRELY past the transcript coverage watermark")

        // (2) Persistence layer: the arbitrary-span path stamps a userMarked row.
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        // Persist the covered chunks so the no-chunk-in-mark assertion is
        // load-bearing (chunks exist up to coverage, none in the tail).
        try await store.insertTranscriptChunks(
            TailMark.coveredChunks(assetId: assetId, coverageEnd: coverageEnd)
        )
        let service = TailMark.service(store: store)

        await service.recordUserMarkedAd(
            analysisAssetId: assetId,
            startTime: span.start,
            endTime: span.end,
            podcastId: "podcast-m1l9"
        )

        let windows = try await store.fetchAdWindows(assetId: assetId)
        let mark = try #require(
            windows.first { $0.boundaryState == "userMarked" },
            "the tail mark must persist as a userMarked AdWindow"
        )
        #expect(mark.startTime == span.start, "persisted start matches the requested tail bound")
        #expect(mark.endTime == span.end, "persisted end matches the requested tail bound")
        #expect(mark.startTime > coverageEnd,
                "the persisted mark is entirely past coverage — NOT clamped to the transcript watermark")
        #expect(mark.endTime == episodeDuration,
                "the persisted mark runs all the way to the episode end — NOT clamped to coverage")
        // playhead-527u must not regress: a definitive user mark is auto-skip-eligible.
        #expect(mark.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "the userMarked row stays auto-skip-eligible (527u), even placed in the untranscribed tail")

        // The mark lands in genuinely untranscribed territory: no chunk overlaps it.
        let persistedChunks = try await store.fetchTranscriptChunks(assetId: assetId)
        #expect(!persistedChunks.isEmpty, "transcript chunks exist up to coverage (precondition)")
        #expect(persistedChunks.allSatisfy { $0.endTime <= coverageEnd },
                "no persisted chunk extends past coverage")
        #expect(!persistedChunks.contains { $0.startTime < mark.endTime && $0.endTime > mark.startTime },
                "the mark span overlaps NO transcript chunk — it was placed without any chunks to tap")
    }

    /// GATE (negation set). The tail affordance must NOT appear when the
    /// ordinary chunk-selection flow already works, nor when a mark-to-end
    /// would over-mark a large untranscribed region (that is the bounded-window
    /// player "Hearing an ad" button's job). Each returns nil from the same
    /// method that returns a span in the crux test — proving the span is gated,
    /// not unconditional.
    @Test("tail span is nil within coverage, far from the end, and for a degenerate sliver")
    func tailSpanGating() async throws {
        let coverageEnd = 1800.0
        let vm = await TailMark.viewModel(assetId: "asset-m1l9-gate", coverageEnd: coverageEnd)

        // Playhead WITHIN coverage — chunk-selection suffices.
        #expect(vm.untranscribedTailMarkSpan(currentTime: 1000, episodeDuration: 1900) == nil,
                "a playhead inside transcript coverage does not need the tail affordance")

        // Playhead exactly AT the coverage edge — not yet in untranscribed territory.
        #expect(vm.untranscribedTailMarkSpan(currentTime: coverageEnd, episodeDuration: 1900) == nil,
                "a playhead at the coverage watermark is still covered")

        // Past coverage but FAR from the end (remaining 1750s > 300s cap) —
        // mark-to-end would over-mark; the player button handles this.
        #expect(vm.untranscribedTailMarkSpan(currentTime: 1850, episodeDuration: 3600) == nil,
                "a playhead far from the episode end must not offer a mark-to-end (would over-mark)")

        // Past coverage but a degenerate sliver remains (< 2s min width).
        #expect(vm.untranscribedTailMarkSpan(currentTime: 1899.5, episodeDuration: 1900) == nil,
                "a sub-2s remaining span is too small to mark")

        // Unknown duration.
        #expect(vm.untranscribedTailMarkSpan(currentTime: 1850, episodeDuration: 0) == nil,
                "an unknown episode duration yields no tail span")

        // The valid post-roll case still resolves (the gate is not vacuously always-nil).
        let valid = vm.untranscribedTailMarkSpan(currentTime: 1850, episodeDuration: 1900)
        #expect(valid?.start == 1850 && valid?.end == 1900,
                "the genuine post-roll case still yields a playhead-to-end span")
    }

    /// REGRESSION (playhead-7tn8). The silent-shard shape: transcript chunks
    /// reach 3400s, but the fast pass SCANNED to 3600s because the shards
    /// covering 3400→3600 produced ZERO segments and still advanced the
    /// coverage watermark. That is what a music-bedded post-roll looks like —
    /// no speech, so no lexical anchor, no FM judgment, nothing deterministic
    /// to bite on. The listener at 3450s is the only witness, so the tail
    /// affordance must appear and the mark must persist.
    ///
    /// Before this bead the affordance keyed on `lastCoveredTime`
    /// (max(chunks, watermark) = 3600), so 3450 read as covered and the ad was
    /// unmarkable by EITHER flow: no chunks to tap, no tail affordance.
    ///
    /// Tightening ("what broken impl would still pass this?"):
    ///   * gating on the watermark again → the `#require` FAILS at 3450.
    ///   * discarding the watermark entirely (option (a), key on chunkMax
    ///     only) → the `.scannedWithoutSpeech` / `.notYetScanned` assertions
    ///     FAIL, because both regions would collapse into one state.
    ///   * offering the affordance inside transcript → the within-evidence nil
    ///     assertion FAILS.
    @Test("a scanned-but-silent post-roll past the last chunk is still markable")
    func silentShardPostRollIsStillMarkable() async throws {
        let assetId = "asset-7tn8-silent-shard"
        let transcribedEnd = 3400.0
        let scannedEnd = 3600.0
        let currentTime = 3450.0
        let episodeDuration = 3600.0

        let vm = await TailMark.viewModel(
            assetId: assetId,
            transcribedEnd: transcribedEnd,
            scannedEnd: scannedEnd
        )

        // The two coverage questions genuinely diverge in this fixture.
        #expect(vm.lastTranscribedTime == transcribedEnd,
                "transcript evidence stops at the last chunk")
        #expect(vm.lastCoveredTime == scannedEnd,
                "the scan watermark is RETAINED and still reaches the silent shards' end")

        // …and the distinction Dan asked to preserve survives: scanned-and-
        // silent is not the same state as not-yet-analyzed.
        #expect(vm.tailCoverage(at: currentTime) == .scannedWithoutSpeech,
                "3450s was looked at and yielded no speech — the genuine blind spot")
        #expect(vm.tailCoverage(at: 3590) == .scannedWithoutSpeech,
                "the whole 3400→3600 stretch is scanned-but-silent")
        #expect(vm.tailCoverage(at: scannedEnd + 10) == .notYetScanned,
                "past the watermark is a DIFFERENT state — transient, segments may still arrive")
        #expect(vm.tailCoverage(at: 3000) == .transcribed,
                "inside the transcript the chunk-selection flow already works")

        // The affordance the bead exists for.
        let span = try #require(
            vm.untranscribedTailMarkSpan(
                currentTime: currentTime,
                episodeDuration: episodeDuration
            ),
            "a playhead inside a scanned-but-silent shard must still offer the tail mark"
        )
        #expect(span.start == currentTime, "the tail mark starts at the playhead")
        #expect(span.end == episodeDuration, "the tail mark runs to the episode end")
        #expect(span.start < vm.lastCoveredTime,
                "the span sits INSIDE the scan watermark — coverage no longer suppresses it")

        // Still gated inside transcript territory; the affordance did not
        // become unconditional.
        #expect(vm.untranscribedTailMarkSpan(currentTime: 3000, episodeDuration: episodeDuration) == nil,
                "a playhead with transcript under it does not need the coverage-free affordance")

        // End-to-end: the span persists as a userMarked window on the same
        // arbitrary-span path, in territory with no chunks to tap.
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        try await store.insertTranscriptChunks(
            TailMark.coveredChunks(assetId: assetId, coverageEnd: transcribedEnd)
        )
        let service = TailMark.service(store: store)
        await service.recordUserMarkedAd(
            analysisAssetId: assetId,
            startTime: span.start,
            endTime: span.end,
            podcastId: "podcast-7tn8"
        )

        let windows = try await store.fetchAdWindows(assetId: assetId)
        let mark = try #require(
            windows.first { $0.boundaryState == "userMarked" },
            "the silent-shard tail mark must persist as a userMarked AdWindow"
        )
        #expect(mark.startTime == currentTime, "persisted start matches the playhead")
        #expect(mark.endTime == episodeDuration, "persisted end runs to the episode end")

        let persistedChunks = try await store.fetchTranscriptChunks(assetId: assetId)
        #expect(!persistedChunks.contains { $0.startTime < mark.endTime && $0.endTime > mark.startTime },
                "the mark overlaps NO transcript chunk — there was nothing to tap")
    }
}
