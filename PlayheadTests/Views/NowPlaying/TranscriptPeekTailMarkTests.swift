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

    /// Fast-pass chunks covering [0, coverageEnd) at 600s (10 min) each — the
    /// transcript the fast pass has reached. NONE extend past `coverageEnd`.
    static func coveredChunks(assetId: String, coverageEnd: Double) -> [TranscriptChunk] {
        let width = 600.0
        let count = Int(coverageEnd / width)
        return (0..<count).map { i in
            TranscriptChunk(
                id: "c\(i)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(i)",
                chunkIndex: i,
                startTime: Double(i) * width,
                endTime: Double(i + 1) * width,
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

    @MainActor
    static func viewModel(assetId: String, coverageEnd: Double) async -> TranscriptPeekViewModel {
        let chunks = coveredChunks(assetId: assetId, coverageEnd: coverageEnd)
        let source = FixedTranscriptPeekDataSource(
            snapshot: snapshot(chunks: chunks, coverageEnd: coverageEnd)
        )
        let vm = TranscriptPeekViewModel(analysisAssetId: assetId, dataSource: source)
        await vm.refresh()
        return vm
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
        #expect(vm.lastCoveredTime == coverageEnd,
                "no chunk extends past coverage, so lastCoveredTime is the watermark")

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
}
