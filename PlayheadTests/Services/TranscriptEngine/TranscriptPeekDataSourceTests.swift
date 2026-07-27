// TranscriptPeekDataSourceTests.swift
// Regression coverage for the read/display transcript projection shared by
// Transcript Peek and Full Transcript.

import Foundation
import Testing

@testable import Playhead

private struct FixedTranscriptPeekDataSource: TranscriptPeekDataSource {
    let snapshot: TranscriptPeekSnapshot

    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot {
        snapshot
    }
}

private actor SequencedTranscriptPeekDataSource: TranscriptPeekDataSource {
    private var snapshots: [TranscriptPeekSnapshot]

    init(snapshots: [TranscriptPeekSnapshot]) {
        self.snapshots = snapshots
    }

    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot {
        guard !snapshots.isEmpty else { return .empty }
        return snapshots.removeFirst()
    }
}

private actor SuspendedTranscriptPeekDataSource: TranscriptPeekDataSource {
    private var nextCall = 0
    private var pending: [
        Int: CheckedContinuation<TranscriptPeekSnapshot, Never>
    ] = [:]
    private var countWaiters: [
        (target: Int, continuation: CheckedContinuation<Void, Never>)
    ] = []

    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot {
        let call = nextCall
        nextCall += 1
        return await withCheckedContinuation { continuation in
            pending[call] = continuation
            resumeReadyCountWaiters()
        }
    }

    func waitForPendingCount(_ target: Int) async {
        guard pending.count < target else { return }
        await withCheckedContinuation { continuation in
            countWaiters.append((target, continuation))
        }
    }

    func resume(
        call: Int,
        returning snapshot: TranscriptPeekSnapshot
    ) {
        pending.removeValue(forKey: call)?.resume(returning: snapshot)
    }

    private func resumeReadyCountWaiters() {
        let ready = countWaiters.filter { pending.count >= $0.target }
        countWaiters.removeAll { pending.count >= $0.target }
        ready.forEach { $0.continuation.resume() }
    }
}

@Suite("TranscriptPeekDataSourceTests")
struct TranscriptPeekDataSourceTests {

    private func chunk(
        id: String,
        index: Int,
        start: Double,
        end: Double,
        text: String,
        pass: String,
        transcriptVersion: String? = nil,
        atomOrdinal: Int? = nil,
        weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil,
        speakerId: Int? = nil,
        avgConfidence: Float? = nil
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: id,
            analysisAssetId: "asset-kcz1",
            segmentFingerprint: "fingerprint-\(id)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: text.lowercased(),
            pass: pass,
            modelVersion: "\(pass)-test-v1",
            transcriptVersion: transcriptVersion,
            atomOrdinal: atomOrdinal,
            weakAnchorMetadata: weakAnchorMetadata,
            speakerId: speakerId,
            avgConfidence: avgConfidence
        )
    }

    private func deviceEvidenceChunks() -> [TranscriptChunk] {
        [
            chunk(
                id: "fast-intro",
                index: 0,
                start: 0,
                end: 4,
                text: "Fast intro",
                pass: "fast"
            ),
            chunk(
                id: "fast-overlap",
                index: 1,
                start: 10,
                end: 14,
                text: "Stale fast sponsor text",
                pass: "fast"
            ),
            chunk(
                id: "fast-tail",
                index: 2,
                start: 20,
                end: 24,
                text: "Fast tail",
                pass: "fast"
            ),
            chunk(
                id: "final-overlap",
                index: 3,
                start: 10,
                end: 14,
                text: "Corrected final sponsor text",
                pass: "final"
            ),
        ]
    }

    private func snapshot(
        chunks: [TranscriptChunk],
        adWindows: [AdWindow]
    ) -> TranscriptPeekSnapshot {
        TranscriptPeekSnapshot(
            chunks: chunks,
            rawChunkCount: chunks.count,
            adWindows: adWindows,
            decodedSpans: [],
            featureCoverageEnd: nil,
            fastTranscriptCoverageEnd: nil,
            latestSessionState: nil,
            fetchFailed: false
        )
    }

    private func adWindow(start: Double, end: Double) -> AdWindow {
        AdWindow(
            id: "ad-\(start)-\(end)",
            analysisAssetId: "asset-kcz1",
            startTime: start,
            endTime: end,
            confidence: 0.9,
            boundaryState: "snapped",
            decisionState: "decided",
            detectorVersion: "test",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "synthetic",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    /// Device evidence: final-pass rows are appended after all fast rows, so
    /// their chunk indexes differ even when their timestamps cover exactly the
    /// same audio. The display snapshot must treat interval coverage—not index
    /// equality—as the replacement key.
    @Test("appended final row replaces a timestamp-identical fast row")
    func appendedFinalRowReplacesTimestampIdenticalFastRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-kcz1"))

        let persisted = deviceEvidenceChunks()
        try await store.insertTranscriptChunks(persisted)

        let snapshot = await LiveTranscriptPeekDataSource(store: store)
            .fetchSnapshot(assetId: "asset-kcz1")

        #expect(snapshot.fetchFailed == false)
        #expect(snapshot.rawChunkCount == 4)
        #expect(snapshot.chunks.count == 3)
        #expect(snapshot.chunks.map(\.id) == [
            "fast-intro",
            "final-overlap",
            "fast-tail",
        ])
        #expect(snapshot.chunks.map(\.text) == [
            "Fast intro",
            "Corrected final sponsor text",
            "Fast tail",
        ])
        #expect(snapshot.chunks.map(\.startTime) == [0, 10, 20])

        // Canonicalization is a projection: all four persisted rows, including
        // their original indexes, remain available for diagnostics/reprocessing.
        let raw = try await store.fetchTranscriptChunks(assetId: "asset-kcz1")
        #expect(raw.map(\.id) == persisted.map(\.id))
        #expect(raw.map(\.chunkIndex) == [0, 1, 2, 3])
    }

    @Test("exact-span final replacement keeps a tapped audio selection")
    @MainActor
    func exactSpanReplacementKeepsTappedAudioSelection() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-kcz1"))

        let persisted = deviceEvidenceChunks()
        try await store.insertTranscriptChunks(Array(persisted.prefix(3)))

        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: LiveTranscriptPeekDataSource(store: store)
        )
        await peek.refresh()
        #expect(peek.chunks.map(\.id) == [
            "fast-intro",
            "fast-overlap",
            "fast-tail",
        ])

        let selection: Set<TranscriptChunkSelection> = [
            TranscriptChunkSelection(chunk: peek.chunks[1]),
        ]

        // The final row is appended at a new persisted index, then replaces
        // the selected fast row in the display projection.
        try await store.insertTranscriptChunk(persisted[3])
        await peek.refresh()
        #expect(peek.chunks.map(\.id) == [
            "fast-intro",
            "final-overlap",
            "fast-tail",
        ])
        #expect(peek.reconciledSelections(selection) == selection)

        let submittedRange = try #require(
            peek.selectedChunkTimeRange(selections: selection)
        )
        #expect(submittedRange.start == 10)
        #expect(submittedRange.end == 14)
    }

    @Test("canonical rows drive active and mark selection on both transcript surfaces")
    @MainActor
    func canonicalRowsDriveTranscriptSurfaceSelection() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-kcz1"))
        try await store.insertTranscriptChunks(deviceEvidenceChunks())
        let dataSource = LiveTranscriptPeekDataSource(store: store)

        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: dataSource
        )
        await peek.refresh()

        #expect(peek.chunks.map(\.id) == [
            "fast-intro",
            "final-overlap",
            "fast-tail",
        ])
        peek.updatePlaybackPosition(12)
        #expect(peek.activeChunkIndex == 1)
        #expect(peek.chunks[1].pass == "final")

        let markedSelections = Set([
            TranscriptChunkSelection(chunk: peek.chunks[1]),
            TranscriptChunkSelection(chunk: peek.chunks[2]),
        ])
        let markedRange = try #require(
            peek.selectedChunkTimeRange(selections: markedSelections)
        )
        #expect(markedRange.start == 10)
        #expect(markedRange.end == 24)

        let full = FullTranscriptViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: dataSource
        )
        await full.load()

        // The fixture gaps split all three visible rows into paragraphs. The
        // middle paragraph must be the canonical final row, never fast+final.
        #expect(full.paragraphs.count == 3)
        #expect(full.paragraphs[1].chunks.map(\.id) == ["final-overlap"])
        #expect(full.paragraphs[1].text == "Corrected final sponsor text")

        full.updatePlaybackPosition(12)
        #expect(full.activeParagraphIndex == 1)
        full.longPressedParagraph(at: 1)
        #expect(full.selectedParagraphIds == ["fingerprint-final-overlap"])
    }

    @Test("poll refresh keeps playback and mark selection on the same audio")
    @MainActor
    func refreshKeepsPlaybackAndMarkSelectionOnSameAudio() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-kcz1"))
        try await store.insertTranscriptChunks([
            chunk(
                id: "fast-intro",
                index: 0,
                start: 0,
                end: 4,
                text: "Intro",
                pass: "fast"
            ),
            chunk(
                id: "fast-middle-a",
                index: 1,
                start: 10,
                end: 12,
                text: "Old middle A",
                pass: "fast"
            ),
            chunk(
                id: "fast-middle-b",
                index: 2,
                start: 12,
                end: 14,
                text: "Old middle B",
                pass: "fast"
            ),
            chunk(
                id: "fast-tail",
                index: 3,
                start: 20,
                end: 24,
                text: "Tail selected for marking",
                pass: "fast"
            ),
        ])

        let dataSource = LiveTranscriptPeekDataSource(store: store)
        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: dataSource
        )
        await peek.refresh()
        peek.updatePlaybackPosition(21)
        #expect(peek.activeChunkIndex == 3)

        // Mirror the view freezing the displayed row's audio envelope before
        // the next poll.
        let selectedChunks: Set<TranscriptChunkSelection> = [
            TranscriptChunkSelection(chunk: peek.chunks[3]),
        ]
        let initialRange = try #require(
            peek.selectedChunkTimeRange(selections: selectedChunks)
        )
        #expect(initialRange.start == 20)
        #expect(initialRange.end == 24)

        // One final row replaces two earlier fast rows, shifting the tail
        // from displayed offset 3 to 2 while playback remains paused at 21.
        try await store.insertTranscriptChunk(
            chunk(
                id: "final-middle",
                index: 4,
                start: 10,
                end: 14,
                text: "Corrected middle",
                pass: "final"
            )
        )
        await peek.refresh()

        #expect(peek.chunks.map(\.id) == [
            "fast-intro",
            "final-middle",
            "fast-tail",
        ])
        #expect(peek.activeChunkIndex == 2)

        let refreshedRange = try #require(
            peek.selectedChunkTimeRange(selections: selectedChunks)
        )
        #expect(refreshedRange.start == 20)
        #expect(refreshedRange.end == 24)
        #expect(peek.reconciledSelections(selectedChunks) == selectedChunks)

        // A differently bounded final row can fully replace the selected
        // fast row. The old audio envelope must then leave selection state;
        // otherwise the header reports a hidden selection that no visible
        // row can deselect.
        try await store.insertTranscriptChunk(
            chunk(
                id: "final-expanded-tail",
                index: 5,
                start: 19,
                end: 25,
                text: "Expanded corrected tail",
                pass: "final"
            )
        )
        await peek.refresh()

        #expect(peek.chunks.map(\.id) == [
            "fast-intro",
            "final-middle",
            "final-expanded-tail",
        ])
        #expect(peek.activeChunkIndex == 2)
        #expect(peek.reconciledSelections(selectedChunks).isEmpty)
        #expect(
            peek.selectedChunkTimeRange(selections: selectedChunks) == nil
        )
    }

    @Test("failed poll retains the last good projection and selection")
    @MainActor
    func failedPollRetainsLastGoodProjectionAndSelection() async throws {
        let selected = chunk(
            id: "selected-row",
            index: 0,
            start: 10,
            end: 14,
            text: "Selected audio",
            pass: "fast"
        )
        let successful = snapshot(chunks: [selected], adWindows: [])
        let failed = TranscriptPeekSnapshot(
            chunks: [],
            rawChunkCount: 0,
            adWindows: [],
            decodedSpans: [],
            featureCoverageEnd: nil,
            fastTranscriptCoverageEnd: nil,
            latestSessionState: nil,
            fetchFailed: true
        )
        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: SequencedTranscriptPeekDataSource(
                snapshots: [successful, failed]
            )
        )

        await peek.refresh()
        peek.updatePlaybackPosition(12)
        let selections: Set<TranscriptChunkSelection> = [
            TranscriptChunkSelection(chunk: selected),
        ]

        await peek.refresh()

        #expect(peek.chunks.map(\.id) == ["selected-row"])
        #expect(peek.activeChunkIndex == 0)
        #expect(peek.reconciledSelections(selections) == selections)
        let range = try #require(
            peek.selectedChunkTimeRange(selections: selections)
        )
        #expect(range.start == 10)
        #expect(range.end == 14)
        #expect(peek.debugStats.contains("err"))
    }

    @Test("cancelled older refresh cannot overwrite a newer projection")
    @MainActor
    func cancelledRefreshCannotOverwriteNewerProjection() async {
        let dataSource = SuspendedTranscriptPeekDataSource()
        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: dataSource
        )
        let stale = snapshot(
            chunks: [
                chunk(
                    id: "stale-row",
                    index: 0,
                    start: 0,
                    end: 4,
                    text: "Stale",
                    pass: "fast"
                ),
            ],
            adWindows: []
        )
        let fresh = snapshot(
            chunks: [
                chunk(
                    id: "fresh-row",
                    index: 0,
                    start: 10,
                    end: 14,
                    text: "Fresh",
                    pass: "final"
                ),
            ],
            adWindows: []
        )

        let staleRefresh = Task { await peek.refresh() }
        await dataSource.waitForPendingCount(1)
        staleRefresh.cancel()

        let freshRefresh = Task { await peek.refresh() }
        await dataSource.waitForPendingCount(2)
        await dataSource.resume(call: 1, returning: fresh)
        await freshRefresh.value

        await dataSource.resume(call: 0, returning: stale)
        await staleRefresh.value

        #expect(peek.chunks.map(\.id) == ["fresh-row"])
    }

    @Test("partial final overlap retains the fast row's uncovered edges")
    @MainActor
    func partialFinalOverlapRetainsFastCoverage() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-kcz1"))

        let persisted = [
            chunk(
                id: "fast-intro",
                index: 0,
                start: 0,
                end: 4,
                text: "Fast intro",
                pass: "fast"
            ),
            chunk(
                id: "fast-wide",
                index: 1,
                start: 10,
                end: 20,
                text: "Fast text covering both edges",
                pass: "fast"
            ),
            chunk(
                id: "final-middle",
                index: 2,
                start: 15,
                end: 18,
                text: "Final text for the middle only",
                pass: "final"
            ),
            chunk(
                id: "fast-following",
                index: 3,
                start: 21,
                end: 23,
                text: "Fast text after the retained edge",
                pass: "fast"
            ),
        ]
        try await store.insertTranscriptChunks(persisted)

        let dataSource = LiveTranscriptPeekDataSource(store: store)
        let snapshot = await dataSource.fetchSnapshot(assetId: "asset-kcz1")

        #expect(snapshot.rawChunkCount == 4)
        #expect(snapshot.chunks.map(\.id) == [
            "fast-intro",
            "fast-wide",
            "final-middle",
            "fast-following",
        ])
        #expect(snapshot.chunks[1].startTime == 10)
        #expect(snapshot.chunks[1].endTime == 20)
        #expect(snapshot.chunks[2].startTime == 15)
        #expect(snapshot.chunks[2].endTime == 18)

        // Retaining the wide fast row makes end times non-monotonic. Peek
        // must reactivate that row on its uncovered trailing edge instead of
        // leaving the already-ended final row active.
        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: dataSource
        )
        await peek.refresh()
        peek.updatePlaybackPosition(19)
        let activeChunkIndex = try #require(peek.activeChunkIndex)
        #expect(activeChunkIndex == 1)
        #expect(peek.chunks[activeChunkIndex].id == "fast-wide")

        // Full Transcript must likewise measure the next gap from the
        // farthest covered edge (20), not the nested final row's end (18).
        // The true one-second gap to [21,23] stays within one paragraph.
        let full = FullTranscriptViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: dataSource
        )
        await full.load()
        #expect(full.paragraphs.count == 2)
        #expect(full.paragraphs[1].chunks.map(\.id) == [
            "fast-wide",
            "final-middle",
            "fast-following",
        ])
        #expect(full.paragraphs[1].endTime == 23)
    }

    @Test("final row wins active selection inside a retained trailing-edge overlap")
    @MainActor
    func finalWinsActiveSelectionInsideTrailingEdgeOverlap() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-kcz1"))
        try await store.insertTranscriptChunks([
            chunk(
                id: "fast-trailing-edge",
                index: 0,
                start: 15,
                end: 20,
                text: "Fast text with an uncovered trailing edge",
                pass: "fast"
            ),
            chunk(
                id: "final-leading",
                index: 1,
                start: 10,
                end: 18,
                text: "Final text for the covered overlap",
                pass: "final"
            ),
        ])

        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: LiveTranscriptPeekDataSource(store: store)
        )
        await peek.refresh()
        #expect(peek.chunks.map(\.id) == [
            "final-leading",
            "fast-trailing-edge",
        ])

        // Both rows cover 16, so the higher-quality final row wins even
        // though the retained fast row starts later.
        peek.updatePlaybackPosition(16)
        #expect(peek.activeChunkIndex == 0)

        // Past the final interval, the retained fast edge becomes active.
        peek.updatePlaybackPosition(19)
        #expect(peek.activeChunkIndex == 1)
    }

    @Test("touching boundary activates the row that starts there on both surfaces")
    @MainActor
    func touchingBoundaryActivatesStartingRow() async throws {
        let boundary = 10.25
        let finalLeading = chunk(
            id: "final-leading",
            index: 0,
            start: 0,
            end: boundary,
            text: "Final text before the boundary",
            pass: "final"
        )
        let fastFollowing = chunk(
            id: "fast-following",
            index: 1,
            start: boundary,
            end: 20,
            text: "Fast text beginning at the boundary",
            pass: "fast"
        )
        let boundarySnapshot = snapshot(
            chunks: [finalLeading, fastFollowing],
            // Half-open ad overlap splits the rows into two paragraphs so
            // Full Transcript must resolve the same pass boundary as Peek.
            adWindows: [adWindow(start: 0, end: boundary)]
        )

        let dataSource = FixedTranscriptPeekDataSource(
            snapshot: boundarySnapshot
        )
        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: dataSource
        )
        await peek.refresh()
        peek.updatePlaybackPosition(boundary - 0.01)
        #expect(peek.activeChunkIndex == 0)
        peek.updatePlaybackPosition(boundary)
        #expect(peek.activeChunkIndex == 1)

        let full = FullTranscriptViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: dataSource
        )
        await full.load()
        #expect(full.paragraphs.count == 2)
        full.updatePlaybackPosition(boundary - 0.01)
        #expect(full.activeParagraphIndex == 0)
        full.updatePlaybackPosition(boundary)
        #expect(full.activeParagraphIndex == 1)
    }

    @Test("Full Transcript resolves nested overlap by coverage and pass quality")
    @MainActor
    func fullTranscriptResolvesNestedOverlapByCoverageAndPass() async throws {
        let fastLeading = chunk(
            id: "fast-leading",
            index: 0,
            start: 10,
            end: 20,
            text: "Fast row with a trailing edge",
            pass: "fast"
        )
        let finalMiddle = chunk(
            id: "final-middle",
            index: 1,
            start: 15,
            end: 18,
            text: "Final row nested in fast coverage",
            pass: "final"
        )
        let trailingSnapshot = snapshot(
            chunks: [fastLeading, finalMiddle],
            // Only the leading fast row overlaps this window, forcing the
            // nested rows into separate paragraphs.
            adWindows: [adWindow(start: 10, end: 12)]
        )
        let trailingPeek = TranscriptPeekViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: FixedTranscriptPeekDataSource(snapshot: trailingSnapshot)
        )
        await trailingPeek.refresh()

        // Start order does not imply end order for nested rows. Diagnostics
        // must report the farthest covered edge, not the last row's end.
        #expect(trailingPeek.debugStats.contains("2 chunks 0:10–0:20"))

        // Once every row has ended, gap fallback follows the most recently
        // covered audio edge (fast ends at 20), not the later-starting nested
        // final row that ended at 18.
        trailingPeek.updatePlaybackPosition(20.5)
        #expect(trailingPeek.activeChunkIndex == 0)

        let trailingVM = FullTranscriptViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: FixedTranscriptPeekDataSource(
                snapshot: trailingSnapshot
            )
        )
        await trailingVM.load()
        #expect(trailingVM.paragraphs.count == 2)

        // Once the nested final row ends, Full Transcript must reactivate
        // the still-covering fast trailing edge.
        trailingVM.updatePlaybackPosition(19)
        #expect(trailingVM.activeParagraphIndex == 0)

        // The same farthest-end rule applies immediately after both
        // overlapping paragraphs have ended.
        trailingVM.updatePlaybackPosition(20.5)
        #expect(trailingVM.activeParagraphIndex == 0)

        let finalLeading = chunk(
            id: "final-leading",
            index: 0,
            start: 10,
            end: 18,
            text: "Final row covering the overlap",
            pass: "final"
        )
        let fastTrailing = chunk(
            id: "fast-trailing",
            index: 1,
            start: 15,
            end: 20,
            text: "Fast row with an uncovered trailing edge",
            pass: "fast"
        )
        let qualityVM = FullTranscriptViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: FixedTranscriptPeekDataSource(
                snapshot: snapshot(
                    chunks: [finalLeading, fastTrailing],
                    // Only the fast row overlaps this later window, forcing
                    // the two passes into separate paragraphs.
                    adWindows: [adWindow(start: 19, end: 20)]
                )
            )
        )
        await qualityVM.load()
        #expect(qualityVM.paragraphs.count == 2)

        // While both rows cover playback, the final-pass paragraph wins.
        qualityVM.updatePlaybackPosition(16)
        #expect(qualityVM.activeParagraphIndex == 0)

        // Past final coverage, the retained fast edge becomes active.
        qualityVM.updatePlaybackPosition(19)
        #expect(qualityVM.activeParagraphIndex == 1)
    }

    @Test(
        "single-pass snapshots preserve display order, row identity, and persisted indexes",
        arguments: ["fast", "final"]
    )
    @MainActor
    func singlePassPreservesDisplayOrderIdentityAndIndexes(pass: String) async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-kcz1"))

        // Persisted indexes are deliberately not chronological. The existing
        // UI boundary sorts rows for display without rewriting row identity or
        // indexes; canonicalization must leave that single-pass behavior alone.
        let metadata = TranscriptWeakAnchorMetadata(
            averageConfidence: 0.72,
            minimumConfidence: 0.41,
            alternativeTexts: ["alternate"],
            lowConfidencePhrases: [
                WeakAnchorPhrase(
                    text: "uncertain",
                    startTime: 1,
                    endTime: 2,
                    confidence: 0.41
                ),
            ]
        )
        func row(
            id: String,
            index: Int,
            start: Double,
            end: Double,
            text: String
        ) -> TranscriptChunk {
            chunk(
                id: id,
                index: index,
                start: start,
                end: end,
                text: text,
                pass: pass,
                transcriptVersion: "version-\(id)",
                atomOrdinal: index + 40,
                weakAnchorMetadata: metadata,
                speakerId: index + 10,
                avgConfidence: Float(index + 1) / 10
            )
        }
        let persisted = [
            row(
                id: "\(pass)-later",
                index: 0,
                start: 10,
                end: 14,
                text: "Later"
            ),
            row(
                id: "\(pass)-earlier",
                index: 1,
                start: 0,
                end: 4,
                text: "Earlier"
            ),
            row(
                id: "\(pass)-tie-first",
                index: 2,
                start: 20,
                end: 24,
                text: "First equal-start row"
            ),
            row(
                id: "\(pass)-tie-second",
                index: 3,
                start: 20,
                end: 22,
                text: "Second equal-start row"
            ),
        ]
        try await store.insertTranscriptChunks(persisted)

        let snapshot = await LiveTranscriptPeekDataSource(store: store)
            .fetchSnapshot(assetId: "asset-kcz1")

        #expect(snapshot.rawChunkCount == 4)
        #expect(snapshot.chunks.map(\.id) == [
            "\(pass)-earlier",
            "\(pass)-later",
            "\(pass)-tie-first",
            "\(pass)-tie-second",
        ])
        #expect(snapshot.chunks.map(\.segmentFingerprint) == [
            "fingerprint-\(pass)-earlier",
            "fingerprint-\(pass)-later",
            "fingerprint-\(pass)-tie-first",
            "fingerprint-\(pass)-tie-second",
        ])
        #expect(snapshot.chunks.map(\.chunkIndex) == [1, 0, 2, 3])
        #expect(snapshot.chunks.map(\.startTime) == [0, 10, 20, 20])
        let expectedDisplay = [
            persisted[1],
            persisted[0],
            persisted[2],
            persisted[3],
        ]
        #expect(snapshot.chunks.map(\.analysisAssetId)
            == expectedDisplay.map(\.analysisAssetId))
        #expect(snapshot.chunks.map(\.endTime)
            == expectedDisplay.map(\.endTime))
        #expect(snapshot.chunks.map(\.text)
            == expectedDisplay.map(\.text))
        #expect(snapshot.chunks.map(\.normalizedText)
            == expectedDisplay.map(\.normalizedText))
        #expect(snapshot.chunks.map(\.pass)
            == expectedDisplay.map(\.pass))
        #expect(snapshot.chunks.map(\.modelVersion)
            == expectedDisplay.map(\.modelVersion))
        #expect(snapshot.chunks.map(\.transcriptVersion)
            == expectedDisplay.map(\.transcriptVersion))
        #expect(snapshot.chunks.map(\.atomOrdinal)
            == expectedDisplay.map(\.atomOrdinal))
        #expect(snapshot.chunks.map(\.weakAnchorMetadata)
            == expectedDisplay.map(\.weakAnchorMetadata))
        #expect(snapshot.chunks.map(\.speakerId)
            == expectedDisplay.map(\.speakerId))
        #expect(snapshot.chunks.map(\.avgConfidence)
            == expectedDisplay.map(\.avgConfidence))

        // Swift's stable sort preserves the store's chunk-index order for
        // equal starts; Full Transcript's stable start sort must preserve
        // that same display order rather than inventing a second tie order.
        let full = FullTranscriptViewModel(
            analysisAssetId: "asset-kcz1",
            dataSource: FixedTranscriptPeekDataSource(snapshot: snapshot)
        )
        await full.load()
        #expect(full.paragraphs.flatMap(\.chunks).map(\.id) == [
            "\(pass)-earlier",
            "\(pass)-later",
            "\(pass)-tie-first",
            "\(pass)-tie-second",
        ])
    }
}
