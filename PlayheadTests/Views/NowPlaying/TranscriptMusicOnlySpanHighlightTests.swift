// TranscriptMusicOnlySpanHighlightTests.swift
// playhead-d666: a bare sustained-music hint is not an ad CLAIM, so the
// transcript must not paint the show's own words as an ad on nothing else.
//
// THE GROUND TRUTH (Dan, 2026-08-10, asset 48E903D7). He vetoed
// 2044.500–2048.820 as a `falsePositive`: "marked as ad in transcript … but it
// was actually the host reading his normal end of episode script for this kind
// of shorter episode". The transcript there is the clip outro — "to that full
// episode, I've linked it down below. Check the description. Thank you."
//
// WHAT ACTUALLY MARKED IT, measured on `db-corrected2`:
//   • ZERO `ad_windows` rows overlap 2040–2050 on that asset. The marking was
//     never a window. It was the single `decoded_spans` row
//     `91f16f7d5219c6c593b844bae12f636d`, [2044.5, 2056.98], whose ONLY anchor
//     is `.sustainedMusicOffset` at confidence 0.9434829950332642.
//   • That number is bit-for-bit the mean `musicProbability` over the five
//     2-second `feature_windows` in [2046, 2056) — 0.952/0.952/0.952/0.93/0.93.
//     So the signal is a MUSIC BED and nothing else; no lexical, self-promo,
//     recurrence or FM path contributed, and none of them could have.
//   • `TranscriptPeekViewModel.isAdHighlighted` lit the row because a decoded
//     span overlapped it — full stop. Provenance was never consulted.
//
// WHY THAT IS A DEFECT AND NOT A PREFERENCE. `AnchorRef.sustainedMusicOffset`
// and `BackfillEvidenceFusion`'s music-only clause BOTH state the policy: the
// sustained-music proposer is "a TARGETING signal ('an ad likely begins right
// after this music'), never a verdict", which is why `DecisionMapper` demotes
// such a span to `.markOnly` and never lets it auto-skip. The decision path
// honoured that; the display seam had no way to ask the question, so a hint
// about what comes NEXT was drawn as a verdict about the audio it covers.
//
// These tests drive `TranscriptPeekViewModel` directly through a fixed
// snapshot: the smallest layer that carries the defect.

import Foundation
import Testing

@testable import Playhead

// MARK: - Stub data source

private struct MusicOnlyPeekDataSource: TranscriptPeekDataSource {
    let snapshot: TranscriptPeekSnapshot
    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot { snapshot }
}

// MARK: - Builders

private enum ClipOutro {

    static let assetId = "48E903D7-DD18-42D7-931E-B6D5F37F7F44"

    /// The real chunk geometry either side of the veto, from
    /// `transcript_chunks` on the asset. Index 0 is the row Dan vetoed.
    static let rows: [(start: Double, end: Double, text: String)] = [
        (2044.5, 2047.2, "to that full episode, I've linked it down below."),
        (2047.26, 2047.44, "Check"),
        (2052.9, 2055.96,
         "If you're running a business, people have probably told you to use"),
    ]

    static func chunks() -> [TranscriptChunk] {
        rows.enumerated().map { index, row in
            TranscriptChunk(
                id: "chunk-\(index)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(index)",
                chunkIndex: index,
                startTime: row.start,
                endTime: row.end,
                text: row.text,
                normalizedText: row.text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: index
            )
        }
    }

    /// The persisted span, with its real extent, ordinals and anchor.
    static func span(anchors: [AnchorRef]) -> DecodedSpan {
        DecodedSpan(
            id: "91f16f7d5219c6c593b844bae12f636d",
            assetId: assetId,
            firstAtomOrdinal: 2061,
            lastAtomOrdinal: 2068,
            startTime: 2044.5,
            endTime: 2056.98,
            anchorProvenance: anchors
        )
    }

    static let musicAnchor = AnchorRef.sustainedMusicOffset(
        regionId: "\(assetId)#2061-2068",
        confidence: 0.9434829950332642
    )

    static func snapshot(
        spans: [DecodedSpan],
        adWindows: [AdWindow] = []
    ) -> TranscriptPeekSnapshot {
        TranscriptPeekSnapshot(
            chunks: chunks(),
            rawChunkCount: rows.count,
            adWindows: adWindows,
            decodedSpans: spans,
            featureCoverageEnd: 2400,
            fastTranscriptCoverageEnd: 2400,
            latestSessionState: nil,
            fetchFailed: false
        )
    }

    @MainActor
    static func loaded(snapshot: TranscriptPeekSnapshot) async -> TranscriptPeekViewModel {
        let peek = TranscriptPeekViewModel(
            analysisAssetId: assetId,
            dataSource: MusicOnlyPeekDataSource(snapshot: snapshot)
        )
        await peek.refresh()
        return peek
    }

    /// A user-marked window, the shape `rebuildSpansByChunkIndex` reads for
    /// `userMarkedChunkIndices`.
    static func userMarkedWindow(start: Double, end: Double) -> AdWindow {
        AdWindow(
            id: "user-marked",
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: start,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        )
    }
}

// MARK: - Tests

@Suite("A bare music hint is not an ad claim in the transcript (playhead-d666)")
struct TranscriptMusicOnlySpanHighlightTests {

    @Test("THE BEAD: the clip outro is not painted as an ad by a music bed alone")
    @MainActor
    func musicOnlySpanDoesNotHighlightTheOutro() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])]
            )
        )

        #expect(peek.chunks.count == 3)
        // Row 0 is Dan's veto — "to that full episode, I've linked it down
        // below." Row 2 is the real post-roll open, which the same span
        // covers. Neither is an ad CLAIM on this evidence.
        #expect(
            peek.isAdHighlighted(chunkIndex: 0) == false,
            """
            The clip outro must not carry the AD badge. Its only evidence is a \
            sustained music bed, which the pipeline's own policy calls a \
            targeting hint about what comes NEXT — never a verdict about the \
            audio it covers.
            """
        )
        #expect(peek.isAdHighlighted(chunkIndex: 1) == false)
        #expect(peek.isAdHighlighted(chunkIndex: 2) == false)
    }

    @Test("The span is still REACHABLE — only the paint is withheld")
    @MainActor
    func musicOnlySpanIsStillReturnedForThePopoverAndTheVeto() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])]
            )
        )

        // `TranscriptPeekView` builds its popover tap target and its
        // "sustained music" explanation from THIS, and
        // `revertByTimeRange`'s window-less branch requires an overlapping
        // decoded span to exist before it will record a veto at all. Silencing
        // the highlight must not silence either.
        #expect(peek.decodedSpansOverlapping(chunkIndex: 0).count == 1)
        #expect(
            peek.decodedSpansOverlapping(startTime: 2044.5, endTime: 2048.82)
                .count == 1,
            """
            Withholding the highlight must not remove the span from the \
            overlap reads — a listener who taps still gets the explanation, \
            and the veto path still has something to retract.
            """
        )
    }

    @Test("Music PLUS corroborating presence evidence still highlights")
    @MainActor
    func musicWithCorroborationStillHighlights() async throws {
        let corroborated = ClipOutro.span(anchors: [
            ClipOutro.musicAnchor,
            .fmConsensus(regionId: "r1", consensusStrength: 0.8),
        ])
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(spans: [corroborated])
        )

        #expect(
            peek.isAdHighlighted(chunkIndex: 0),
            """
            Only the BARE hint is silenced. A span the FM grid also called an \
            ad is a verdict, and the music anchor riding along with it must \
            not demote it — otherwise this fix would eat real recall.
            """
        )
        #expect(peek.isAdHighlighted(chunkIndex: 2))
    }

    @Test("A width marker is not corroboration — it sets width, not presence")
    @MainActor
    func widthOwnershipDoesNotCorroborateAMusicHint() async throws {
        // `.spliceSlot` / `.rediffSlot` / `.rediffSlotChroma` answer "how wide",
        // never "is there an ad here". `BackfillEvidenceFusion` excludes them
        // from corroboration for exactly this reason; the display seam reads
        // the same predicate, so it must agree.
        for widthMarker in [AnchorRef.spliceSlot, .rediffSlot, .rediffSlotChroma] {
            let peek = await ClipOutro.loaded(
                snapshot: ClipOutro.snapshot(
                    spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor, widthMarker])]
                )
            )
            #expect(
                peek.isAdHighlighted(chunkIndex: 0) == false,
                "a \(widthMarker.provenanceKind) marker sets width, not presence"
            )
        }
    }

    @Test("A row under BOTH a music-only span and a real one stays highlighted")
    @MainActor
    func anyNonMusicSpanOnTheRowKeepsTheHighlight() async throws {
        // ALL, not ANY: one corroborated span overlapping this row is a claim
        // about this row, and a bare music hint layered on top cannot retract
        // it. Row 2 (the real post-roll open) carries both.
        let realAd = DecodedSpan(
            id: "real-postroll",
            assetId: ClipOutro.assetId,
            firstAtomOrdinal: 2068,
            lastAtomOrdinal: 2070,
            startTime: 2052.9,
            endTime: 2112.9,
            anchorProvenance: [.classifierSeed(regionId: "r2", score: 0.9)]
        )
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor]), realAd]
            )
        )

        #expect(
            peek.isAdHighlighted(chunkIndex: 0) == false,
            "row 0 is covered only by the music-only span"
        )
        #expect(
            peek.isAdHighlighted(chunkIndex: 2),
            """
            Row 2 is covered by BOTH. The classifier-seeded span is a real \
            claim about this row and must survive the music-only test.
            """
        )
    }

    @Test("The listener's OWN mark always wins over the music test")
    @MainActor
    func userMarkedWindowStillHighlightsUnderAMusicOnlySpan() async throws {
        // A `userMarked` window IS a verdict — the listener's. It reaches
        // `isAdHighlighted` by a different route than decoded spans, and the
        // music test must not be able to swallow it.
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])],
                adWindows: [ClipOutro.userMarkedWindow(start: 2044.5, end: 2047.2)]
            )
        )

        #expect(
            peek.isAdHighlighted(chunkIndex: 0),
            "the listener marked this row themselves; nothing here may unmark it"
        )
        #expect(peek.isAdHighlighted(chunkIndex: 2) == false)
    }

    @Test("A span with NO anchors at all is unaffected by this change")
    @MainActor
    func spanWithoutAnyProvenanceKeepsItsPriorBehaviour() async throws {
        // `carriesOnlyMusicPresenceHint` requires a music anchor to be
        // PRESENT. An empty provenance array — what the pre-Phase-5 fixtures
        // and several legacy rows carry — is not music-only and keeps the
        // highlight it has always had.
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(spans: [ClipOutro.span(anchors: [])])
        )
        #expect(peek.isAdHighlighted(chunkIndex: 0))
    }
}

// MARK: - The AD badge

@Suite("Silencing a music hint must not swallow a real span's AD badge (playhead-d666)")
struct TranscriptMusicOnlySpanBadgeTests {

    /// The real post-roll, starting inside the music-only span's extent — the
    /// geometry the sustained-music proposer exists to produce ("an ad likely
    /// begins right AFTER this music"), so row 2 is under BOTH.
    private static func realPostRoll(
        id: String = "real-postroll",
        start: Double = 2052.9
    ) -> DecodedSpan {
        DecodedSpan(
            id: id,
            assetId: ClipOutro.assetId,
            firstAtomOrdinal: 2068,
            lastAtomOrdinal: 2090,
            startTime: start,
            endTime: 2112.9,
            anchorProvenance: [.classifierSeed(regionId: "r2", score: 0.9)]
        )
    }

    @Test("The claim set drops the music-only span and keeps the real one")
    @MainActor
    func claimingSpansExcludeTheMusicOnlySpan() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor]), Self.realPostRoll()]
            )
        )

        // Row 0 is covered by the music-only span alone: nothing claims.
        #expect(peek.adClaimingSpansOverlapping(chunkIndex: 0).isEmpty)
        #expect(peek.decodedSpansOverlapping(chunkIndex: 0).count == 1)

        // Row 2 is covered by both. The FULL overlap read still returns both —
        // the popover and the veto path depend on it — while the claim read
        // returns only the span entitled to assert an ad.
        #expect(peek.decodedSpansOverlapping(chunkIndex: 2).count == 2)
        #expect(
            peek.adClaimingSpansOverlapping(chunkIndex: 2).map(\.id) == ["real-postroll"],
            """
            The claim set is what the AD badge groups by. A music-only id \
            leaking into it joins row 2 to row 1, and the real span then looks \
            like a continuation of something that was never drawn.
            """
        )
    }

    @Test("THE R1 DEFECT: the real post-roll still gets its badge")
    @MainActor
    func realSpanKeepsItsBadgeUnderAStraddlingMusicOnlySpan() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor]), Self.realPostRoll()]
            )
        )

        #expect(peek.showsAdBadge(chunkIndex: 0) == false)
        #expect(peek.showsAdBadge(chunkIndex: 1) == false)
        #expect(
            peek.showsAdBadge(chunkIndex: 2),
            """
            Row 2 is the first row of the classifier-seeded post-roll and is \
            highlighted, so it must carry the badge. Grouping by the FULL \
            overlap set instead makes the silenced music span — which covers \
            rows 0-2 — shared with row 1, so row 2 reads as a continuation and \
            the entire ad region renders with a copper bar and no label.
            """
        )
    }

    @Test("The badge marks the FIRST row of a span, not every row")
    @MainActor
    func badgeIsNotRepeatedOnContinuationRows() async throws {
        // A post-roll that opens on row 1 and continues through row 2.
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [
                    ClipOutro.span(anchors: [ClipOutro.musicAnchor]),
                    Self.realPostRoll(start: 2047.3),
                ]
            )
        )

        #expect(peek.showsAdBadge(chunkIndex: 0) == false)
        #expect(peek.showsAdBadge(chunkIndex: 1))
        #expect(
            peek.showsAdBadge(chunkIndex: 2) == false,
            "row 2 is the same span continuing — one badge per span, not per row"
        )
    }

    @Test("A user-marked region under a music-only span still gets its badge")
    @MainActor
    func userMarkedRegionKeepsItsBadge() async throws {
        // The user-marked route carries no span id, so the badge falls back to
        // the highlight run. Grouping by the full overlap set sent this row
        // down the decoded branch instead — where the music-only span it shares
        // with row 0 suppressed the badge on a region the LISTENER marked.
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])],
                adWindows: [ClipOutro.userMarkedWindow(start: 2047.26, end: 2055.96)]
            )
        )

        #expect(peek.showsAdBadge(chunkIndex: 0) == false)
        #expect(peek.showsAdBadge(chunkIndex: 1))
        #expect(peek.showsAdBadge(chunkIndex: 2) == false)
    }

    @Test("A silenced row never carries the badge, including row 0")
    @MainActor
    func aMusicOnlyRowNeverBadges() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])]
            )
        )

        // Row 0 is the index-0 special case — highlighted rows badge
        // unconditionally there, so the silencing has to hold BEFORE it.
        #expect(peek.showsAdBadge(chunkIndex: 0) == false)
        #expect(peek.showsAdBadge(chunkIndex: 1) == false)
        #expect(peek.showsAdBadge(chunkIndex: 2) == false)
    }

    @Test("A corroborated span on row 0 still badges")
    @MainActor
    func corroboratedSpanOnTheFirstRowBadges() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [
                    ClipOutro.musicAnchor,
                    .fmConsensus(regionId: "r1", consensusStrength: 0.8),
                ])]
            )
        )
        #expect(peek.showsAdBadge(chunkIndex: 0))
        #expect(peek.showsAdBadge(chunkIndex: 1) == false)
    }
}

// MARK: - What VoiceOver says

@Suite("VoiceOver may not claim an ad the transcript stopped drawing (playhead-d666)")
struct TranscriptRowAccessibilityClaimTests {

    private static var outroRow: TranscriptChunk { ClipOutro.chunks()[0] }

    @Test("THE PIN: a silenced row is spoken as plain transcript, with no ad claim")
    func silencedRowMakesNoSpokenClaim() {
        let label = TranscriptRowAccessibility.label(
            chunk: Self.outroRow,
            isAd: false,
            claimingSpans: []
        )

        #expect(
            label.contains("Ad segment") == false,
            """
            The music-only span is still in the row's OVERLAP set — the popover \
            and the veto path need it there — and is absent only from the CLAIM \
            set. That absence is the only thing standing between it and a spoken \
            "Ad segment, 12 seconds, detected from sustained music" on a row a \
            sighted listener sees unpainted.
            """
        )
        #expect(label == "34:04: \(Self.outroRow.text)")
    }

    @Test("A claiming row is still announced, with its provenance")
    func claimingRowIsAnnounced() {
        let real = DecodedSpan(
            id: "real", assetId: ClipOutro.assetId,
            firstAtomOrdinal: 2061, lastAtomOrdinal: 2068,
            startTime: 2044.5, endTime: 2056.98,
            anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9)]
        )
        let label = TranscriptRowAccessibility.label(
            chunk: Self.outroRow,
            isAd: false,
            claimingSpans: [real]
        )

        #expect(label.hasPrefix("Ad segment, 12 seconds, detected from classifier."))
    }

    @Test("The legacy ad-window label is untouched by the claim gate")
    func legacyWindowLabelIsUnchanged() {
        // No decoded spans at all: the `isAd` branch is the pre-Phase-5 path
        // and this change must not have moved it.
        let label = TranscriptRowAccessibility.label(
            chunk: Self.outroRow,
            isAd: true,
            claimingSpans: []
        )
        #expect(label == "Ad segment at 34:04: \(Self.outroRow.text)")
    }
}

// MARK: - What VoiceOver says, composed the way the app composes it

/// playhead-d666 R2. The suite above asserts the FORMATTER. These assert the
/// COMPOSITION — which population the app hands it — because that is where the
/// residual defect was: R1's per-row boolean stopped a silenced row speaking,
/// and left a claiming row free to be described by the silenced span sitting on
/// top of it.
@Suite("VoiceOver names the span that CLAIMS, not the one that merely overlaps (playhead-d666)")
struct TranscriptRowSpokenClaimCompositionTests {

    /// The corroborated post-roll the music hint is pointing AT: it opens on
    /// row 2, runs a full minute, and is the only thing on that row entitled to
    /// assert an ad. The music-only span starts EARLIER (2044.5), and
    /// `fetchDecodedSpans` orders by `startTime`, so the full overlap set puts
    /// the silenced span first — which is what the view used to read.
    private static func realPostRoll() -> DecodedSpan {
        DecodedSpan(
            id: "real-postroll",
            assetId: ClipOutro.assetId,
            firstAtomOrdinal: 2068, lastAtomOrdinal: 2090,
            startTime: 2052.9, endTime: 2112.9,
            anchorProvenance: [.classifierSeed(regionId: "r2", score: 0.9)]
        )
    }

    @Test("THE R2 DEFECT: a claiming row is announced with the CLAIMING span's length and reason")
    @MainActor
    func claimingRowIsNotDescribedByTheSilencedSpan() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor]), Self.realPostRoll()]
            )
        )

        // Row 2 is under BOTH spans, and is drawn as an ad because of the
        // post-roll alone.
        #expect(peek.decodedSpansOverlapping(chunkIndex: 2).count == 2)
        #expect(peek.isAdHighlighted(chunkIndex: 2))

        let label = peek.accessibilityLabel(chunkIndex: 2)
        #expect(
            label.contains("sustained music") == false,
            """
            The row's ad-ness comes from a 60-second classifier-seeded post-roll. \
            Reading the FULL overlap set picks the earliest-starting span — the \
            silenced music hint — so VoiceOver was told "Ad segment, 12 seconds, \
            detected from sustained music": the wrong length, and a reason this \
            bead established may never speak for an ad at all.
            """
        )
        #expect(label.hasPrefix("Ad segment, 60 seconds, detected from classifier."))
    }

    @Test("A silenced row speaks no claim through the view model either")
    @MainActor
    func silencedRowSpeaksNoClaimThroughTheViewModel() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])]
            )
        )

        for index in 0..<3 {
            #expect(
                peek.accessibilityLabel(chunkIndex: index).contains("Ad segment") == false,
                "row \(index) is drawn unpainted, so it must be spoken unpainted"
            )
        }
        #expect(peek.accessibilityLabel(chunkIndex: 0) == "34:04: \(ClipOutro.rows[0].text)")
    }

    @Test("A listener's own mark is still announced as an ad, not as sustained music")
    @MainActor
    func userMarkedRowIsAnnouncedWithoutBorrowingTheHintsProvenance() async throws {
        // The listener marked rows 1-2 themselves, under a music-only span that
        // covers all three rows. The mark is a verdict; the hint is not.
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])],
                adWindows: [ClipOutro.userMarkedWindow(start: 2047.26, end: 2055.96)]
            )
        )

        #expect(peek.isAdHighlighted(chunkIndex: 1))
        let label = peek.accessibilityLabel(chunkIndex: 1)
        #expect(
            label.contains("sustained music") == false,
            "the listener's mark is the reason, and a music bed may not be cited as one"
        )
        #expect(label == "Ad segment at 34:07: \(ClipOutro.rows[1].text)")
    }

    @Test("An index off the end of the transcript has nothing to say")
    @MainActor
    func outOfRangeRowIsEmpty() async throws {
        let peek = await ClipOutro.loaded(snapshot: ClipOutro.snapshot(spans: []))
        #expect(peek.accessibilityLabel(chunkIndex: 3).isEmpty)
        #expect(peek.accessibilityLabel(chunkIndex: -1).isEmpty)
    }
}

// MARK: - What a tap opens, and therefore what a veto retracts

/// playhead-d666 R3. The bar, the badge and the spoken label all read the
/// CLAIMING set. The tap target did not — it took
/// `decodedSpansOverlapping(chunkIndex:).first`, and `fetchDecodedSpans` orders
/// by `startTime`, so on this bead's geometry that is the silenced span.
///
/// This is not only a caption defect. `AdRegionPopover`'s "This isn't an ad"
/// calls `onRevertAdWindows(span)` with whatever span it was opened for, so a
/// listener vetoing the ad they can see retracted the music hint beside it and
/// the row stayed lit.
///
/// Measured on `db-corrected2`: 112 transcript rows overlap both a music-only
/// span and a claiming span, and the music-only span sorts first on all 112.
@Suite("A tap opens the span that CLAIMS, falling back to the hint (playhead-d666)")
struct TranscriptRowPopoverTargetTests {

    private static func realPostRoll() -> DecodedSpan {
        DecodedSpan(
            id: "real-postroll",
            assetId: ClipOutro.assetId,
            firstAtomOrdinal: 2068, lastAtomOrdinal: 2090,
            startTime: 2052.9, endTime: 2112.9,
            anchorProvenance: [.classifierSeed(regionId: "r2", score: 0.9)]
        )
    }

    @Test("THE R3 DEFECT: a row under both spans opens the CLAIMING one")
    @MainActor
    func bothOverlapOpensTheClaimingSpan() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor]), Self.realPostRoll()]
            )
        )

        // Row 2 is under BOTH, and the silenced span sorts first.
        #expect(peek.decodedSpansOverlapping(chunkIndex: 2).count == 2)
        #expect(peek.decodedSpansOverlapping(chunkIndex: 2).first?.id == ClipOutro.span(anchors: []).id)
        #expect(peek.isAdHighlighted(chunkIndex: 2))

        #expect(
            peek.popoverSpan(chunkIndex: 2)?.id == "real-postroll",
            """
            The row is drawn, badged and spoken for the 60-second post-roll. \
            Opening the music hint's popover there headlines "AD SEGMENT / \
            DETECTED FROM: sustained music" with the hint's 12-second duration, \
            and its "This isn't an ad" retracts the hint instead of the claim.
            """
        )
    }

    @Test("A row with only a silenced span still opens it — the veto affordance is untouched")
    @MainActor
    func musicOnlyRowStillOpensTheHint() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])]
            )
        )

        #expect(peek.isAdHighlighted(chunkIndex: 0) == false)
        #expect(peek.popoverSpan(chunkIndex: 0)?.id == ClipOutro.span(anchors: []).id)
    }

    @Test("A row with no spans at all opens nothing, in range or out")
    @MainActor
    func noSpansOpensNothing() async throws {
        let peek = await ClipOutro.loaded(snapshot: ClipOutro.snapshot(spans: []))
        #expect(peek.popoverSpan(chunkIndex: 0) == nil)
        #expect(peek.popoverSpan(chunkIndex: 3) == nil)
        #expect(peek.popoverSpan(chunkIndex: -1) == nil)
    }

    @Test("THE R4 DEFECT: a row lit by the LISTENER'S OWN mark never opens the hint")
    @MainActor
    func userMarkedRowDoesNotOpenTheHint() async throws {
        // The real geometry on 48E903D7: the music-only span is 2044.5–2056.98
        // and Dan's own post-roll mark is 2052.9–2112.9 (`C0CC71D0`), which he
        // made because the app MISSED that post-roll. Row 2 sits inside both.
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [ClipOutro.span(anchors: [ClipOutro.musicAnchor])],
                adWindows: [ClipOutro.userMarkedWindow(start: 2052.9, end: 2112.9)]
            )
        )

        // The row IS painted — by the listener's verdict, not by any claim.
        #expect(peek.isAdHighlighted(chunkIndex: 2))
        #expect(peek.adClaimingSpansOverlapping(chunkIndex: 2).isEmpty)
        #expect(peek.decodedSpansOverlapping(chunkIndex: 2).count == 1)

        #expect(
            peek.popoverSpan(chunkIndex: 2) == nil,
            """
            The fallback exists for an UNPAINTED row. This row is lit by the \
            listener's own mark, so opening the hint headlines "AD SEGMENT / \
            DETECTED FROM: sustained music" over their own verdict — and its \
            "This isn't an ad" reverts by the HINT's range (2044.5–2056.98), \
            which overlaps and therefore retracts the mark itself, and records \
            a manualVeto correction over a range they never chose.
            """
        )

        // The documented fallback is untouched where its own justification
        // holds: row 0 is unpainted and still reaches the hint.
        #expect(peek.isAdHighlighted(chunkIndex: 0) == false)
        #expect(peek.popoverSpan(chunkIndex: 0)?.id == ClipOutro.span(anchors: []).id)
    }

    @Test("A user-marked row that ALSO carries a claim still opens the claim")
    @MainActor
    func userMarkedRowWithAClaimStillOpensTheClaim() async throws {
        let peek = await ClipOutro.loaded(
            snapshot: ClipOutro.snapshot(
                spans: [
                    ClipOutro.span(anchors: [ClipOutro.musicAnchor]),
                    Self.realPostRoll(),
                ],
                adWindows: [ClipOutro.userMarkedWindow(start: 2052.9, end: 2112.9)]
            )
        )

        #expect(peek.isAdHighlighted(chunkIndex: 2))
        #expect(
            peek.popoverSpan(chunkIndex: 2)?.id == "real-postroll",
            "the user-mark guard must not suppress a claim the row genuinely carries"
        )
    }
}

// MARK: - The shared predicate

@Suite("carriesOnlyMusicPresenceHint is one definition, two consumers (playhead-d666)")
struct MusicOnlyPresenceHintPredicateTests {

    private static let music = AnchorRef.sustainedMusicOffset(
        regionId: "r", confidence: 0.94
    )

    @Test("Bare music is music-only")
    func bareMusicIsMusicOnly() {
        #expect([Self.music].carriesOnlyMusicPresenceHint)
    }

    @Test("No music anchor is never music-only")
    func absentMusicIsNotMusicOnly() {
        #expect([AnchorRef]().carriesOnlyMusicPresenceHint == false)
        #expect(
            [AnchorRef.classifierSeed(regionId: "r", score: 0.9)]
                .carriesOnlyMusicPresenceHint == false
        )
        // Width markers alone carry no presence claim either way, but with no
        // music anchor there is nothing for this predicate to be true about.
        #expect([AnchorRef.rediffSlot].carriesOnlyMusicPresenceHint == false)
    }

    @Test("Every presence anchor corroborates; every width marker does not")
    func corroborationSetIsExact() {
        // EVERY case of `AnchorRef` appears here or in the width loop below.
        // A mutation that moves any one of them between the two groups fails
        // this test, which is what stops the corroboration set drifting.
        let corroborating: [AnchorRef] = [
            .fmConsensus(regionId: "r", consensusStrength: 0.8),
            .fmAcousticCorroborated(regionId: "r", breakStrength: 0.8),
            .classifierSeed(regionId: "r", score: 0.9),
            .userCorrection(correctionId: "c", reportedTime: 2044.5),
            .evidenceCatalog(entry: EvidenceEntry(
                evidenceRef: 0,
                category: .ctaPhrase,
                matchedText: "link in the description",
                normalizedText: "link in the description",
                atomOrdinal: 2061,
                startTime: 2044.5,
                endTime: 2047.2
            )),
        ]
        for anchor in corroborating {
            #expect(
                [Self.music, anchor].carriesOnlyMusicPresenceHint == false,
                "\(anchor.provenanceKind) is PRESENCE evidence and corroborates"
            )
        }
        for marker in [AnchorRef.spliceSlot, .rediffSlot, .rediffSlotChroma] {
            #expect(
                [Self.music, marker].carriesOnlyMusicPresenceHint,
                "\(marker.provenanceKind) is WIDTH, not presence — no corroboration"
            )
        }
    }
}
