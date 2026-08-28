// AutoSkipDenialSurfaceTests.swift
// playhead-nq8z: A `bannerAutoSkipDenied` ROW COULD NOT SAY WHICH SURFACE
// PRODUCED IT.
//
// playhead-bwxi added `correction_events.playheadTimeAtCorrection` (V59) so
// that `position ∈ [span.start, span.end)` is a question a row can be ASKED —
// it was filed because three `bannerAutoSkipConfirmed` rows for windows at 23,
// 56 and 71 minutes were recorded on 2026-08-21 by a listener 87 seconds in,
// and nothing in the row said so. playhead-2d6i then added a surface whose
// premise is that the listener has MOVED ON: the passive missed-skip list. A
// veto from it is made from wherever they are now, so its position is
// deliberately outside the span — honest, and until this bead indistinguishable
// from a card's No tapped after the playhead left the span, because cards
// persist past the span end. Two producers, one value, and the whole worth of
// V59 is that a corpus can be FILTERED on it rather than trusted wholesale.
//
// WHAT THIS SUITE OWNS. `MissedAutoSkipReceiptListTests` proves the list's veto
// reaches the seam and writes the new source end to end;
// `MissedAutoSkipListWiringSourceCanaryTests` proves both bindings name their
// own surface. What is left is everything about the DISCRIMINATOR itself:
//
//   * the surface -> source mapping, over `allCases` rather than by example;
//   * that the new case behaves as an explicit PRIVATE receipt in the three
//     mechanisms `isExplicitBannerFeedback` gates, so it cannot leak into a
//     diagnostic export or take the generic identity;
//   * that the persisted spelling is a durable on-disk value, not a name;
//   * that the store REFUSES a receipt whose source disagrees with the door it
//     came through — in BOTH directions, since a guard that only ever refuses
//     one of the two proves nothing about the other;
//   * that the two denial sources are DIFFERENT durable identities, which is
//     what stops a card's No and a list veto for one window collapsing into a
//     single row whose surface is then whichever arrived first.
//
// WHY THERE IS NO SCHEMA VERSION HERE. `correction_events.source` is plain
// `TEXT` with no `CHECK` constraint — verified in `migrateCorrectionEventsV6`
// and its `CREATE TABLE IF NOT EXISTS` twin — and every SQL predicate over the
// vocabulary (`correctionSourceFidelitySQL`, `genericCrossSourceConflictSQL`,
// `suppressingCorrectionScopesPresent`) is GENERATED from
// `CorrectionSource.allCases` at query time. So widening the vocabulary changes
// no DDL and needs no migration; what it does need is the round-trip below,
// because "no migration was required" is a claim and not a permission.

import Foundation
import Testing

@testable import Playhead

@Suite(
    "playhead-nq8z — a denial row names the surface that produced it",
    .timeLimit(.minutes(1))
)
struct AutoSkipDenialSurfaceTests {

    private static let assetId = "asset-nq8z"
    private static let episodeId = "episode-nq8z"
    private static let windowStart: Double = 60
    private static let windowEnd: Double = 120

    // MARK: - 1. The mapping

    /// Over `allCases`, not by example: a mapping asserted case by case is one
    /// a new case joins silently.
    @Test("every surface maps to its own denial source, and to no other kind")
    func everySurfaceMapsToItsOwnSource() {
        #expect(
            AutoSkipDenialSurface.card.correctionSource
                == .bannerAutoSkipDenied
        )
        #expect(
            AutoSkipDenialSurface.missedAutoSkipList.correctionSource
                == .missedAutoSkipListDenied
        )
        let mapped = AutoSkipDenialSurface.allCases.map(\.correctionSource)
        #expect(
            Set(mapped).count == AutoSkipDenialSurface.allCases.count,
            """
            two surfaces map to the same `CorrectionSource`: \(mapped). The \
            type exists to make the surface recoverable from the row, and a \
            collision makes it unrecoverable for the surfaces that share a \
            value — which is the state this bead started from.
            """
        )
        for source in mapped {
            #expect(
                source.kind == .falsePositive,
                """
                \(source) is a DENIAL source and must be false-positive \
                direction. A boost-direction value here would make "not an ad" \
                teach the detector that it WAS one.
                """
            )
        }
    }

    /// The spelling is on disk, so it is data rather than a name.
    ///
    /// Every row written since this shipped carries the literal below; renaming
    /// the case would silently orphan them (`CorrectionSource(rawValue:)`
    /// returns nil, and `loadCorrectionEvents` decodes an unrecognised source
    /// to `nil` — the row survives and becomes invisible to every reader,
    /// including the fidelity ladder, which floors an unknown source at 0).
    @Test("the persisted spelling is pinned — renaming the case orphans rows")
    func thePersistedSpellingIsPinned() {
        #expect(
            CorrectionSource.missedAutoSkipListDenied.rawValue
                == "missedAutoSkipListDenied"
        )
        #expect(
            CorrectionSource(rawValue: "missedAutoSkipListDenied")
                == .missedAutoSkipListDenied
        )
    }

    // MARK: - 2. It is an explicit PRIVATE receipt

    /// The three mechanisms `isExplicitBannerFeedback` gates, asserted against
    /// the card's denial rather than against constants — the claim is that the
    /// two surfaces are the same KIND of thing, and pinning `2` and `true`
    /// separately would still pass if the card's own values moved.
    @Test("the list's denial has the card denial's privacy class and fidelity")
    func theListDenialSharesTheCardDenialsClass() {
        let card = CorrectionSource.bannerAutoSkipDenied
        let list = CorrectionSource.missedAutoSkipListDenied
        #expect(list.isExplicitBannerFeedback == card.isExplicitBannerFeedback)
        #expect(list.isExplicitBannerFeedback)
        #expect(list.fidelityRank == card.fidelityRank)
        #expect(list.kind == card.kind)
        #expect(list.kind == .falsePositive)
        // And the two are still DISTINCT values, or none of the above is
        // evidence about anything.
        #expect(list != card)
    }

    /// `CorpusExporter` is where a private receipt would leak, and it filters
    /// on `isPrivateExplicitFeedbackReceipt` — which `CorrectionEvent.init`
    /// derives from `source`. Driven through the serializer rather than
    /// asserted about the flag, because the flag is not what ships bytes.
    @Test("a list denial never serializes into the diagnostic corpus")
    func aListDenialNeverSerializes() throws {
        let event = Self.makeDenialEvent(source: .missedAutoSkipListDenied)
        #expect(
            event.isPrivateExplicitFeedbackReceipt,
            "the receipt is not private, so nothing downstream will withhold it"
        )
        #expect(
            try CorpusExporter.correctionLine(event) == nil,
            """
            a `missedAutoSkipListDenied` receipt serialized into the corpus \
            export. A veto is a private answer about this listener's own \
            episode whichever surface asked, and the other four explicit \
            sources have never been exported.
            """
        )
        // The positive control: a source OUTSIDE the private class does
        // serialize, so the assertion above is about this row and not about a
        // serializer that returns nil for everything.
        let veto = CorrectionEvent(
            analysisAssetId: Self.assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: Self.assetId,
                startTime: Self.windowStart,
                endTime: Self.windowEnd
            ).serialized,
            source: .manualVeto
        )
        #expect(try CorpusExporter.correctionLine(veto) != nil)
    }

    /// A card's No and a list veto for the SAME window are different durable
    /// identities. If they were not, the second to arrive would land on the
    /// first's row as an audit bump, the stored `source` would stay whichever
    /// came first, and the discriminator would be a coin flip.
    @Test("the two denial surfaces are different durable identities")
    func theTwoDenialSurfacesAreDifferentIdentities() throws {
        let card = Self.makeDenialEvent(source: .bannerAutoSkipDenied)
        let list = Self.makeDenialEvent(source: .missedAutoSkipListDenied)
        let cardKey = try #require(card.explicitReceiptIdentityKey)
        let listKey = try #require(list.explicitReceiptIdentityKey)
        #expect(
            cardKey != listKey,
            """
            both surfaces produce the identity key \(cardKey) for one window, \
            so the second answer would be absorbed as a re-submission of the \
            first and the row's `source` would record whichever gesture \
            happened to arrive earlier.
            """
        )
        #expect(card.identity != list.identity)
    }

    // MARK: - 3. The store checks the door against the source

    /// Both directions. A guard that only refuses the list's source arriving
    /// through the card's door leaves the mirror — the older, bigger
    /// population — unchecked, and vice versa.
    @Test("persistDeniedAutoSkip refuses a correction whose source is not its surface")
    func theStoreRefusesASourceSurfaceMismatch() async throws {
        for (surface, wrongSource) in [
            (AutoSkipDenialSurface.card, CorrectionSource.missedAutoSkipListDenied),
            (AutoSkipDenialSurface.missedAutoSkipList, CorrectionSource.bannerAutoSkipDenied),
        ] {
            let (store, window) = try await Self.makeSeededStore()
            let mismatched = try await store.persistDeniedAutoSkip(
                windowId: window.id,
                analysisAssetId: Self.assetId,
                expectedEpisodeId: Self.episodeId,
                expectedStartTime: window.startTime,
                expectedEndTime: window.endTime,
                expectedProducerRevision: window,
                expectedMaterialToken: Self.token(for: window),
                surface: surface,
                correction: Self.makeDenialEvent(source: wrongSource)
            )
            #expect(
                mismatched == nil,
                """
                the store committed a \(wrongSource) receipt through the \
                \(surface) door. The surface is what the caller SAYS it is; \
                checking it against the correction is what makes "the source \
                on disk is the door you came through" a property of the \
                transaction rather than a convention.
                """
            )
            let afterRefusal = try await store.loadCorrectionEvents(
                analysisAssetId: Self.assetId
            )
            #expect(afterRefusal.isEmpty)

            // POSITIVE CONTROL, on the same store and the same window: the
            // matching source through the same door is accepted. Without it a
            // `persistDeniedAutoSkip` that refused EVERYTHING would pass the
            // assertion above.
            let accepted = try await store.persistDeniedAutoSkip(
                windowId: window.id,
                analysisAssetId: Self.assetId,
                expectedEpisodeId: Self.episodeId,
                expectedStartTime: window.startTime,
                expectedEndTime: window.endTime,
                expectedProducerRevision: window,
                expectedMaterialToken: Self.token(for: window),
                surface: surface,
                correction: Self.makeDenialEvent(
                    source: surface.correctionSource
                )
            )
            #expect(
                accepted == true,
                "\(surface) refused its OWN source, so the refusal above is not evidence"
            )
            await store.close()
        }
    }

    // MARK: - 4. The round trip

    /// The claim "no schema change was required", exercised rather than
    /// argued: the new spelling survives a real SQLite write and read, and
    /// comes back as a private receipt with its durable identity intact.
    @Test("a list denial round-trips through SQLite with its source and identity")
    func aListDenialRoundTripsThroughSQLite() async throws {
        let (store, window) = try await Self.makeSeededStore()
        let correction = Self.makeDenialEvent(
            source: .missedAutoSkipListDenied,
            // playhead-bwxi's column, carrying what this surface exists to
            // record: a position OUTSIDE the window's own span.
            playheadTimeAtCorrection: 2_000
        )
        let inserted = try await store.persistDeniedAutoSkip(
            windowId: window.id,
            analysisAssetId: Self.assetId,
            expectedEpisodeId: Self.episodeId,
            expectedStartTime: window.startTime,
            expectedEndTime: window.endTime,
            expectedProducerRevision: window,
            expectedMaterialToken: Self.token(for: window),
            surface: .missedAutoSkipList,
            correction: correction
        )
        #expect(inserted == true)

        let loaded = try await store.loadCorrectionEvents(
            analysisAssetId: Self.assetId
        )
        let row = try #require(loaded.first)
        #expect(
            row.source == .missedAutoSkipListDenied,
            """
            the row came back as \(String(describing: row.source)). An \
            unrecognised `source` string decodes to nil in \
            `loadCorrectionEvents`, so a spelling this build cannot read is \
            invisible to every reader rather than loud.
            """
        )
        #expect(row.correctionType == .falsePositive)
        #expect(row.isPrivateExplicitFeedbackReceipt)
        #expect(
            row.persistedCorrectionIdentityKey?.isEmpty == false,
            """
            the row stored the EMPTY identity key, i.e. the generic v23 \
            three-column identity. That is what a source outside \
            `isExplicitBannerFeedback` gets, and it would let this receipt \
            dedupe against an unrelated manual veto of the same span.
            """
        )
        // The reading the whole bead is for: an out-of-span position that says
        // which surface it came from.
        let position = try #require(row.playheadTimeAtCorrection)
        #expect(position == 2_000)
        #expect(!(position >= window.startTime && position < window.endTime))
        await store.close()
    }

    // MARK: - Fixture

    private static func token(for window: AdWindow) -> String {
        AdWindowMaterialIdentity.autoSkipToken(
            window: window,
            displayedStart: window.startTime,
            displayedEnd: window.endTime
        )
    }

    private static func makeDenialEvent(
        source: CorrectionSource,
        playheadTimeAtCorrection: Double? = nil
    ) -> CorrectionEvent {
        let window = makeWindow()
        return CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId,
                startTime: windowStart,
                endTime: windowEnd
            ).serialized,
            source: source,
            correctionType: source.kind.correctionType,
            playheadTimeAtCorrection: playheadTimeAtCorrection,
            targetRefs: CorrectionTargetRefs(
                adWindowId: window.id,
                adWindowIds: [window.id],
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(window),
                exactFeedbackSpan: ExactFeedbackSpan(
                    startTime: windowStart,
                    endTime: windowEnd
                )
            )
        )
    }

    private static func makeWindow() -> AdWindow {
        makeSkipTestAdWindow(
            id: "window-nq8z",
            assetId: assetId,
            startTime: windowStart,
            endTime: windowEnd,
            decisionState: AdDecisionState.applied.rawValue
        )
    }

    private static func makeSeededStore() async throws -> (AnalysisStore, AdWindow) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let window = makeWindow()
        try await store.insertAdWindow(window)
        return (store, window)
    }
}
