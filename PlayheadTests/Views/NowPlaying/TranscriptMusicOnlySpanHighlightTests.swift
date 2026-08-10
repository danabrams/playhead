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
