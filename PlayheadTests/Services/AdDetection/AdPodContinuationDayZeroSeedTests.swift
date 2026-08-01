// AdPodContinuationDayZeroSeedTests.swift
// playhead-evc1: a DAY-0 BYTE-EXACT REDIFF mark may seed a pod-continuation
// chain — and nothing else that is merely `.candidate` may.
//
// THE DEFECT
// ----------
// `AdDetectionService.mintByteExactDayZeroMarks` persists every day-0 byte-exact
// rediff slot with `decisionState == .candidate`, and
// `AdPodContinuation.seedDecisionStates` is `{confirmed, applied}`. So the one
// row type that can find an ad in the first seconds of a FIRST listen could not
// seed the mechanism built to recover that ad's pod neighbours. Isolated
// two-armed by playhead-eks2 through the production link/barrier derivation —
// same window, same 1.00 confidence, same provenance, same links, same barriers,
// only `decisionState` differing:
//
//     .candidate  ->  0.0 s recovered
//     .confirmed  ->  [45.1, 94.3) — all of ad 2 plus the 2.5 s stitch gap,
//                     terminating exactly at the show boundary
//
// WHAT THE CARVE-OUT IS, AND WHAT IT IS NOT
// -----------------------------------------
// It is scoped on PROVENANCE, not on `decisionState`. The original exclusion is
// RIGHT about the population it names: `seedDecisionStates`' own doc justifies
// refusing `.candidate` because "the segment aggregator tiles fixed 30 s regions
// as candidates", and those tiles must stay out — the field aggregator went
// 0-for-3 and cost 210 s of show. Admitting `.candidate` wholesale would seed
// pod walks off 0.40-confidence tiles.
//
// So the admitted row must carry the day-0 byte-exact `boundaryState` AND
// `.rediffByteExact` on BOTH edges (the STRICT monotonic-clean arm). The
// exclusion tests below are the half that matters: `everyOtherCandidateRowIsStillRefused`
// walks every `boundaryState` a producer in this codebase writes, and its
// vacuity control proves the refusal is the CARVE's doing rather than the
// fixture's.
//
// SAFETY IS TESTED, NOT ARGUED. A continuation mark is mark-only / candidate /
// unanchored whatever seeded it, so playhead-2350 (an unanchored edge cannot
// auto-skip) and playhead-ynmk (a confirmation asserts presence, never extent)
// must still hold with a day-0 seed. Both are asserted here against rows
// `runBackfill` actually persisted, each with a vacuity control that fires a
// real cue in the same harness — an assertion that merely restates the emitted
// literal proves nothing.
//
// THE END-TO-END EFFECT IS NOT THE ONE THIS BEAD EXPECTED, and the tests say so
// rather than being tuned until they agree with the expectation. "The day-0 seed
// recovers seconds nothing else does" is TRUE at compose level, where the day-0
// mark is the only seed in the input, and FALSE through `runBackfill` on this
// fixture: the pipeline finds ad 2's opening by itself, seeds its own walk, and
// the day-0 arm, an aggregator-provenance control and an arm with NO seed row at
// all all emit `[58.0, 94.3)` for the same 36.3 s. The difference end to end is
// the mark's CONFIDENCE — 0.70 from the byte-exact seed against 0.33 from the
// pipeline's own sub-threshold window — and 0.70 is exactly
// `SkipOrchestrator.preloadConfidenceThreshold`, so the control's row is
// filtered out of both delivery doors and never banners. On this fixture the
// carve-out is the difference between a row nobody sees and a banner. See
// `theCarveOutRaisesAContinuationRowAcrossTheDeliveryFloor`.
//
// A first draft of the end-to-end tests asserted the seconds claim and PASSED
// against unmodified `main`, where a day-0 row cannot seed anything at all.
// That is why every end-to-end assertion here is a difference between arms.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

// MARK: - Banner reader

/// Single-consumer reader over the banner stream. Owns the iterator, so a pull
/// returns already-buffered items without depending on any other task getting
/// scheduled.
private struct DayZeroSeedBannerReader {
    private var iterator: AsyncStream<AdSkipBannerItem>.AsyncIterator

    init(_ stream: AsyncStream<AdSkipBannerItem>) {
        iterator = stream.makeAsyncIterator()
    }

    /// Every item up to `sentinel`, consuming the sentinel. Empty means the
    /// operation under test emitted nothing — a positive observation, not a
    /// timeout.
    mutating func drain(until sentinel: String) async -> [AdSkipBannerItem] {
        var collected: [AdSkipBannerItem] = []
        while let item = await iterator.next() {
            if item.windowId == sentinel {
                return collected
            }
            collected.append(item)
        }
        return collected
    }
}

@Suite("Day-0 byte-exact rediff seeds a pod walk (playhead-evc1)", .timeLimit(.minutes(2)))
struct AdPodContinuationDayZeroSeedTests {

    // MARK: - The field fixture

    private static let assetId = "asset-evc1-d9b513cd"
    /// MUST be the show `makeSkipTestTrustService` seeds — a show with no
    /// profile resolves to `.shadow`, and every cue assertion below would become
    /// vacuous. `assertAutoMode` is the guard that proves it did not.
    private static let podcastId = "podcast-1"
    private static let episodeDuration = 240.0

    /// Ad 1, verbatim from the field: the byte-exact day-0 rediff slot.
    private static let adOne = (start: 0.0, end: 45.1)
    /// Ad 2, verbatim from the field: byte-identical in both fetches, so rediff
    /// is structurally blind to it. Dan marked it by hand.
    private static let adTwo = (start: 47.6, end: 94.3)
    /// A recovered second at or after this point is SHOW, not pod.
    private static let showResumes = 94.3

    /// The pre-roll pod of the field episode as a transcript: two finished DAI
    /// creatives back to back, then the show. Byte-identical to the fixture
    /// playhead-eks2 measured the blocker on, deliberately — the two suites must
    /// describe the SAME pod, or the "before" and "after" of this bead are about
    /// different audio.
    private static func fieldChunks() -> [TranscriptChunk] {
        let spans: [(start: Double, end: Double, text: String)] = [
            // ── ad 1: 0.0 - 45.1, the slot rediff caught byte-exactly ──
            (0.0, 15.0, "Squarespace is the all in one platform to build your brand and grow your business online with award winning templates."),
            (15.0, 30.0, "Head to squarespace com slash conan and use code CONAN for ten percent off your first purchase."),
            (30.0, 45.1, "That is squarespace com slash conan. Offer valid for new subscriptions only."),
            // ── the 2.5 s stitch gap ──
            // ── ad 2: 47.6 - 94.3, byte-identical in both fetches ──
            (47.6, 58.0, "This break is brought to you by Rocket Money. Use code CONAN and head to rocketmoney com slash conan. Terms and conditions apply."),
            (58.0, 70.0, "Cancel your unwanted subscriptions and lower your bills without lifting a finger. Offer ends soon and exclusions may apply."),
            (70.0, 82.0, "Carters has your family covered for every summer first, from the first splash to the last popsicle, while supplies last."),
            (82.0, 94.3, "Shop the latest styles at participating retailers, and see our website for more details before the season turns."),
            // ── show resumes ──
            (94.3, 120.0, "So I was telling you about the drive up the coast and the diner where the waitress recognised my hair before she recognised me."),
            (120.0, 160.0, "She had a photograph behind the register of a man who looked exactly like my father, which I found genuinely unsettling for about an hour."),
            (160.0, 200.0, "And the whole time Sona is in the back seat pretending she has never met either of us, which is her strongest performance to date."),
            (200.0, 240.0, "We drove another two hours after that and nobody said a single word about the photograph, which I think says something about this family.")
        ]
        return spans.enumerated().map { index, span in
            TranscriptChunk(
                id: "evc1-c\(index)",
                analysisAssetId: assetId,
                segmentFingerprint: "evc1-fp-\(index)",
                chunkIndex: index,
                startTime: span.start,
                endTime: span.end,
                text: span.text,
                normalizedText: span.text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    /// A day-0 rediff mark EXACTLY as `mintByteExactDayZeroMarks` persists one.
    ///
    /// Every field is read off the production mint site rather than paraphrased,
    /// because the whole bead is about which of those fields the seed predicate
    /// reads. `anchor` and `gate` are the two the STRICT / segment-recovered
    /// split moves: a monotonic-clean slot records `.rediffByteExact` on both
    /// edges (and, under `RediffActivation.dayZeroByteExactAutoSkipEnabled`,
    /// `.eligible`); a playhead-9s6q segment-recovered slot keeps `.unanchored`
    /// + `.markOnly` until playhead-pyq7 validates those boundaries.
    private static func dayZeroRediffMark(
        id: String = "evc1-day0-ad1",
        start: Double = adOne.start,
        end: Double = adOne.end,
        decisionState: String = AdDecisionState.candidate.rawValue,
        anchor: AutoSkipEdgeAnchor = .rediffByteExact,
        gate: SkipEligibilityGate = .markOnly,
        detectorVersion: String = "evc1-detection-v1"
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: decisionState,
            detectorVersion: detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: gate.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: anchor.rawValue,
            endEdgeAnchor: anchor.rawValue
        )
    }

    /// An arbitrary non-day-0 row, used to prove the carve-out is keyed on
    /// PROVENANCE and not on the shape of any single field.
    private static func row(
        id: String = "evc1-row",
        start: Double = 100.0,
        end: Double = 130.0,
        confidence: Double = 0.4,
        boundaryState: String,
        decisionState: String = AdDecisionState.candidate.rawValue,
        anchor: AutoSkipEdgeAnchor = .unanchored,
        gate: SkipEligibilityGate = .markOnly,
        detectorVersion: String = "evc1-detection-v1"
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: boundaryState,
            decisionState: decisionState,
            detectorVersion: detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: gate.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: anchor.rawValue,
            endEdgeAnchor: anchor.rawValue
        )
    }

    /// Every `boundaryState` a producer in this codebase writes onto an
    /// `AdWindow`, day-0 excluded. Written as the constants their producers use
    /// wherever the constant is reachable, so a rename breaks the build instead
    /// of silently shrinking the exclusion sweep to a set of dead strings. The
    /// three literals have no reachable constant —
    /// `AdDetectionService.correctionReplayBoundaryState` is `private`, and the
    /// two user states are literals at their write sites too — and are pinned
    /// against the sets that DO name them by
    /// `everyProductionBoundaryStateIsCoveredBySweep`.
    private static let nonDayZeroBoundaryStates: [String] = [
        AdBoundaryState.lexical.rawValue,
        AdBoundaryState.acousticRefined.rawValue,
        AdBoundaryState.segmentAggregated.rawValue,
        AdPodContinuation.boundaryState,
        SpecialistMarkComposer.boundaryState,
        "correctionReplay",
        "userMarked",
        "userConfirmedSuggested"
    ]

    /// The link + barrier derivation `runBackfill` Step 18b performs, byte for
    /// byte. Kept in one place so a compose-level test can never measure a
    /// configuration production does not run.
    private static func productionLinks(
        _ chunks: [TranscriptChunk]
    ) -> [AdPodContinuation.AdCopyLink] {
        let hits = LexicalScanner().collectHits(chunks: chunks)
        return AdPodContinuation.mergeLinks(
            AdPodContinuation.adCopyLinks(chunks: chunks, hits: hits)
                + AdPodContinuation.rhetoricalLinks(chunks: chunks)
        )
    }

    private static func productionBarriers(
        _ chunks: [TranscriptChunk]
    ) -> [AdPodContinuation.ContentBarrier] {
        AdPodContinuation.contentBarriers(
            semanticScanResults: [],
            lexicalHits: LexicalScanner().collectHits(chunks: chunks),
            chunks: chunks
        )
    }

    /// Seconds of `windows` that land inside `span`.
    private static func overlap(
        _ windows: [AdWindow],
        with span: (start: Double, end: Double)
    ) -> Double {
        windows.reduce(0.0) { total, window in
            total + max(0, min(window.endTime, span.end) - max(window.startTime, span.start))
        }
    }

    // MARK: - 1. The carve-out's scope

    /// THE BEAD, in one assertion. A strict day-0 byte-exact rediff mark is
    /// `.candidate` forever — nothing in the pipeline promotes it — so if it
    /// cannot seed while `.candidate` it can never seed at all.
    @Test("a strict day-0 byte-exact mark seeds while still candidate")
    func strictDayZeroMarkSeeds() {
        #expect(AdPodContinuation.isSeed(Self.dayZeroRediffMark()))
    }

    /// The gate is a SKIP-policy field and says nothing about whether the row is
    /// evidence. `RediffActivation.dayZeroByteExactAutoSkipEnabled` moves it, so
    /// keying the carve-out on it would make pod recovery an accidental
    /// side-effect of an auto-skip flag.
    @Test("the day-0 carve-out does not read the eligibility gate")
    func dayZeroSeedIgnoresTheEligibilityGate() {
        for gate in [SkipEligibilityGate.markOnly, .eligible] {
            #expect(
                AdPodContinuation.isSeed(Self.dayZeroRediffMark(gate: gate)),
                "gate \(gate.rawValue) changed whether a day-0 row is evidence"
            )
        }
    }

    /// THE EXCLUSION THAT MUST SURVIVE. The original filter is right about the
    /// aggregator: it tiles fixed 30 s regions as `.candidate`, and chaining a
    /// pod claim off a 0.40-confidence tile is how this pass would start eating
    /// show. Every other producer's `.candidate` row stays refused.
    ///
    /// NON-VACUOUS BY CONSTRUCTION: the vacuity control below re-runs the SAME
    /// geometry, confidence, anchors and decision state with only the
    /// `boundaryState` swapped to day-0's, and requires it to seed. Without that
    /// arm, a predicate that refused everything would pass this test.
    @Test("every other candidate row is still refused as a seed")
    func everyOtherCandidateRowIsStillRefused() {
        for state in Self.nonDayZeroBoundaryStates {
            #expect(
                !AdPodContinuation.isSeed(
                    Self.row(boundaryState: state, anchor: .unanchored)
                ),
                "\(state) candidate row seeded a pod walk"
            )
            // Anchors alone must not admit anything: the carve is on PROVENANCE.
            #expect(
                !AdPodContinuation.isSeed(
                    Self.row(boundaryState: state, anchor: .rediffByteExact)
                ),
                "\(state) candidate row seeded once it carried rediff anchors"
            )
        }
        // VACUITY CONTROL — same row, day-0 provenance.
        #expect(
            AdPodContinuation.isSeed(
                Self.row(
                    boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
                    anchor: .rediffByteExact
                )
            ),
            "the control must seed, or the refusals above are about a predicate that refuses everything"
        )
    }

    /// The three hand-written literals in `nonDayZeroBoundaryStates` are pinned
    /// against the two sets that name the same strings through a constant. A
    /// typo in the sweep would otherwise make it silently test nothing —
    /// `isSeed("uzerMarked-candidate")` is false for the boring reason and the
    /// assertion would still be green.
    @Test("the exclusion sweep covers every boundary state the protected sets name")
    func everyProductionBoundaryStateIsCoveredBySweep() {
        let swept = Set(Self.nonDayZeroBoundaryStates)
        #expect(AdPodContinuation.userOwnedBoundaryStates.isSubset(of: swept))
        #expect(
            AdDetectionService.reconcileProtectedBoundaryStates
                .subtracting([AdDetectionService.dayZeroRediffByteExactBoundaryState])
                .isSubset(of: swept)
        )
        #expect(
            !swept.contains(AdDetectionService.dayZeroRediffByteExactBoundaryState),
            "the sweep must not contain the one state the carve-out admits"
        )
    }

    /// The coarse aggregator tile, measured through `compose` rather than
    /// through the predicate, because that is where a widened predicate would
    /// actually cost show seconds. The control arm proves the fixture CAN
    /// produce a mark.
    @Test("an aggregator candidate tile composes nothing while a day-0 row composes a mark")
    func aggregatorTileComposesNothing() {
        let links = [
            AdPodContinuation.AdCopyLink(start: 700.0, end: 760.0)
        ]
        let tile = Self.row(
            id: "evc1-tile",
            start: 780.0,
            end: 810.0,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue
        )
        let fromTile = AdPodContinuation.compose(
            existingWindows: [tile],
            adCopyLinks: links,
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470.0,
            analysisAssetId: Self.assetId
        )
        #expect(
            fromTile.isEmpty,
            "an aggregator tile seeded a pod walk: \(fromTile.map { ($0.startTime, $0.endTime) })"
        )

        let dayZero = Self.dayZeroRediffMark(id: "evc1-day0-tilecontrol", start: 780.0, end: 810.0)
        let fromDayZero = AdPodContinuation.compose(
            existingWindows: [dayZero],
            adCopyLinks: links,
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470.0,
            analysisAssetId: Self.assetId
        )
        #expect(
            !fromDayZero.isEmpty,
            "the control must compose a mark, or the tile's silence is about the fixture"
        )
    }

    /// A playhead-9s6q SEGMENT-RECOVERED day-0 slot keeps `.unanchored` on both
    /// edges precisely because its A-timeline mapping dropped runs to get there
    /// — playhead-pyq7 owns validating those boundaries. A pod walk starts AT a
    /// seed edge, so an unvalidated edge is the one thing that could put the
    /// walk's first step inside the show rather than at the pod's rim.
    @Test("a segment-recovered day-0 slot does not seed until its edges are validated")
    func segmentRecoveredDayZeroSlotDoesNotSeed() {
        #expect(!AdPodContinuation.isSeed(Self.dayZeroRediffMark(anchor: .unanchored)))
        // One anchored edge is not two: a half-validated boundary is still an
        // unvalidated one on the other side, and the walk uses BOTH.
        let halfAnchored = AdWindow(
            id: "evc1-day0-half",
            analysisAssetId: Self.assetId,
            startTime: Self.adOne.start,
            endTime: Self.adOne.end,
            confidence: 1.0,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "evc1-detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: Self.adOne.start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
        #expect(!AdPodContinuation.isSeed(halfAnchored))
    }

    /// A day-0 mark is retirable ONLY by an explicit user veto
    /// (`decisionState = .reverted`) — every reconcile and hot-path retirement
    /// path exempts it. That makes the veto the single durable "no" a listener
    /// can say about this row type, and a vetoed row must not go on seeding
    /// marks in the neighbourhood it was vetoed out of.
    @Test("a vetoed or suppressed day-0 row never seeds")
    func vetoedDayZeroRowNeverSeeds() {
        for state in [AdDecisionState.reverted, .suppressed] {
            #expect(
                !AdPodContinuation.isSeed(
                    Self.dayZeroRediffMark(decisionState: state.rawValue)
                ),
                "a \(state.rawValue) day-0 row seeded a pod walk"
            )
        }
        // The other two visible states are admitted, so the refusals above are
        // about the STATE and not about the fixture.
        for state in [AdDecisionState.candidate, .confirmed, .applied] {
            #expect(
                AdPodContinuation.isSeed(
                    Self.dayZeroRediffMark(decisionState: state.rawValue)
                ),
                "a \(state.rawValue) day-0 row was refused"
            )
        }
    }

    /// A continuation mark carries `boundaryState == "podContinuation"`, never
    /// day-0's — so the carve-out cannot make this pass chain off its own
    /// output. Asserted on the row the emitter actually builds.
    @Test("the carve-out cannot make a continuation mark seed another continuation")
    func continuationMarkStillNeverSeeds() {
        let mark = AdPodContinuation.makeMark(
            start: 700.0,
            end: 760.0,
            confidence: 0.7,
            analysisAssetId: Self.assetId
        )
        #expect(mark.boundaryState != AdDetectionService.dayZeroRediffByteExactBoundaryState)
        #expect(!AdPodContinuation.isSeed(mark))
    }

    // MARK: - 2. Dan's field pod, measured

    /// THE FIELD CASE. The day-0 mark that found ad 1, composed through the
    /// production link/barrier derivation over the field transcript, must now
    /// mark ad 2 — the creative that was byte-identical in both fetches and that
    /// nothing in the pipeline detected.
    ///
    /// The `.confirmed` arm is kept as the reference: the carve-out must recover
    /// what the eks2 isolation proved was reachable, not merely something.
    @Test("Field case: the day-0 mark that found ad 1 now marks ad 2")
    func fieldCaseDayZeroSeedRecoversAdTwo() throws {
        let chunks = Self.fieldChunks()
        let links = Self.productionLinks(chunks)
        let barriers = Self.productionBarriers(chunks)
        #expect(
            links.contains { $0.start < Self.adTwo.end && $0.end > Self.adTwo.start },
            "fixture must carry ad-copy evidence inside ad 2, else this test is vacuous"
        )

        let asMinted = Self.dayZeroRediffMark()
        let marks = AdPodContinuation.compose(
            existingWindows: [asMinted],
            adCopyLinks: links,
            contentBarriers: barriers,
            protectedRegions: [],
            episodeDuration: Self.episodeDuration,
            analysisAssetId: Self.assetId
        )
        let recovered = Self.overlap(marks, with: Self.adTwo)
        let adTwoWidth = Self.adTwo.end - Self.adTwo.start
        #expect(
            recovered >= adTwoWidth * 0.9,
            "the day-0 seed must recover ad 2; got \(recovered) s of \(adTwoWidth) s"
        )

        // The reference arm. Same row, `.confirmed` — what eks2 measured as
        // reachable. The carve-out must reach the SAME span, not a smaller one.
        let asConfirmed = Self.dayZeroRediffMark(
            decisionState: AdDecisionState.confirmed.rawValue
        )
        let reference = AdPodContinuation.compose(
            existingWindows: [asConfirmed],
            adCopyLinks: links,
            contentBarriers: barriers,
            protectedRegions: [],
            episodeDuration: Self.episodeDuration,
            analysisAssetId: Self.assetId
        )
        #expect(
            marks.map({ [$0.startTime, $0.endTime] })
                == reference.map({ [$0.startTime, $0.endTime] }),
            "as-minted \(marks.map { ($0.startTime, $0.endTime) }) != confirmed \(reference.map { ($0.startTime, $0.endTime) })"
        )

        // ZERO SHOW SECONDS. The walk terminates at the last link's end and the
        // show resumes at 94.3 — a mark reaching past it is the failure the
        // whole mechanism exists to avoid.
        for mark in marks {
            #expect(
                mark.endTime <= Self.showResumes,
                "mark \(mark.startTime)-\(mark.endTime) reached into the show"
            )
        }

        // And the mark is banner-tier whatever seeded it.
        for mark in marks {
            #expect(mark.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
            #expect(mark.decisionState == AdDecisionState.candidate.rawValue)
            #expect(mark.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(mark.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        }
    }

    /// The seed row is never touched. A day-0 mark is deterministic ground truth
    /// for the listener's own stitch; a recall pass that rewrote its geometry,
    /// gate or anchors would trade the certain thing for the derived one.
    @Test("the day-0 seed row itself is never modified")
    func dayZeroSeedRowIsNeverModified() {
        let chunks = Self.fieldChunks()
        let seed = Self.dayZeroRediffMark()
        let marks = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: Self.productionLinks(chunks),
            contentBarriers: Self.productionBarriers(chunks),
            protectedRegions: [],
            episodeDuration: Self.episodeDuration,
            analysisAssetId: Self.assetId
        )
        #expect(!marks.isEmpty)
        #expect(!marks.contains { $0.id == seed.id })
        #expect(
            !marks.contains { $0.startTime < seed.endTime && $0.endTime > seed.startTime },
            "a mark overlapped the seed's own span"
        )
    }

    // MARK: - 3. End to end through the production flag

    /// Which row sits over ad 1 when the backfill runs.
    ///
    /// THE PIPELINE FINDS THINGS ON ITS OWN in this fixture — `RuleBasedClassifier`
    /// reads the same ad copy the links are derived from — so "the backfill
    /// persisted a continuation mark" is NOT by itself evidence about this bead.
    /// It was measured green against unmodified `main`, where a day-0 row cannot
    /// seed anything. Every end-to-end claim below is therefore made as a
    /// DIFFERENCE between these arms.
    private enum SeedArm {
        /// Production first-listen state: the day-0 byte-exact mark, as minted.
        case dayZero
        /// The SAME row — same geometry, same 1.00 confidence, same `.candidate`
        /// state, same `.rediffByteExact` anchors, same `detectorVersion` — with
        /// ONLY `boundaryState` swapped to the aggregator's. This is the control
        /// that isolates the carve-out: it differs from `dayZero` in exactly the
        /// field the carve-out reads.
        case aggregatorLookalike
        /// Nothing on disk but the asset: what the pipeline finds by itself.
        case bare
    }

    /// Run the real backfill at the SHIPPED flag value with `arm`'s row on disk.
    ///
    /// The seed carries a `detectorVersion` DISTINCT from the service's so that
    /// `isReconcilableBackfillWindow` leaves both arms alone. The production
    /// mint stamps `config.detectorVersion` instead and survives on its
    /// `boundaryState` exemption — which
    /// `productionFaithfulSeedSurvivesTheBackfill` pins separately. Holding the
    /// version fixed here keeps `boundaryState` the ONLY difference between the
    /// two arms; letting the control be reconcilable would have changed two
    /// things at once and made the difference unattributable.
    private static func runFirstListenBackfill(
        arm: SeedArm,
        seedDetectorVersion: String = "evc1-seed-detector-v1"
    ) async throws -> (store: AnalysisStore, continuation: [AdWindow], allRows: [AdWindow]) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: assetId)
        )
        switch arm {
        case .dayZero:
            try await store.insertAdWindows([
                dayZeroRediffMark(detectorVersion: seedDetectorVersion)
            ])
        case .aggregatorLookalike:
            try await store.insertAdWindows([
                row(
                    id: "evc1-day0-ad1",
                    start: adOne.start,
                    end: adOne.end,
                    confidence: 1.0,
                    boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                    decisionState: AdDecisionState.candidate.rawValue,
                    anchor: .rediffByteExact,
                    gate: .markOnly,
                    detectorVersion: seedDetectorVersion
                )
            ])
        case .bare:
            break
        }
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "evc1-detection-v1",
                fmBackfillMode: .off,
                podContinuationEnabled:
                    AdDetectionConfig.default.podContinuationEnabled
            )
        )
        try await service.runBackfill(
            chunks: fieldChunks(),
            analysisAssetId: assetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration
        )
        let rows = try await store.fetchAdWindows(assetId: assetId)
        return (
            store,
            rows
                .filter { $0.detectorVersion == AdPodContinuation.detectorVersion }
                .sorted { $0.startTime < $1.startTime },
            rows
        )
    }

    /// The continuation rows the DAY-0 PROVENANCE is responsible for: the rows
    /// the `dayZero` arm persists whose span the `aggregatorLookalike` control
    /// does not also produce.
    ///
    /// This subtraction is what makes every safety assertion below about THIS
    /// bead. Without it they would be satisfied by rows the pipeline produces on
    /// its own — which, over this transcript, it does.
    ///
    /// A row counts as attributable when the control arm produces nothing
    /// matching it on span AND confidence. Confidence is part of the identity on
    /// purpose: on this fixture both arms emit the same SPAN and the whole
    /// observable difference is the confidence the mark inherits from its seed —
    /// which is the difference between a row the session filters out and a
    /// banner. See `theCarveOutRaisesAContinuationRowAcrossTheDeliveryFloor`.
    private static func dayZeroAttributableRows() async throws
        -> (session: [AdWindow], attributable: [AdWindow]) {
        let treatment = try await runFirstListenBackfill(arm: .dayZero)
        let control = try await runFirstListenBackfill(arm: .aggregatorLookalike)
        let attributable = treatment.continuation.filter { candidate in
            !control.continuation.contains {
                abs($0.startTime - candidate.startTime) < 0.001
                    && abs($0.endTime - candidate.endTime) < 0.001
                    && abs($0.confidence - candidate.confidence) < 0.001
            }
        }
        return (treatment.continuation, attributable)
    }

    /// THE END-TO-END EFFECT, AS MEASURED — and it is not the one this bead was
    /// written expecting, so read the numbers before quoting the headline.
    ///
    /// The expectation was "the day-0 seed recovers seconds nothing else does".
    /// End to end on this fixture that is FALSE, and the arms say so: the
    /// pipeline finds ad 2's opening by itself (the transcript really does say
    /// "brought to you by Rocket Money"), seeds its own walk, and BOTH arms —
    /// and an arm with no seed row at all — emit `[58.0, 94.3)` for the same
    /// 36.3 s. Recovered seconds are not the axis here. The compose-level
    /// `fieldCaseDayZeroSeedRecoversAdTwo` is where the seconds claim lives,
    /// because there the day-0 mark is the only seed in the input.
    ///
    /// What DOES differ end to end is the row's CONFIDENCE, and it is decisive.
    /// A continuation mark inherits its seed's presence confidence capped at
    /// `markConfidenceCeiling` (0.70). Seeded by the byte-exact day-0 mark it is
    /// 0.70; seeded by the pipeline's own sub-threshold window it is 0.33. And
    /// 0.70 IS `SkipOrchestrator.preloadConfidenceThreshold`, the floor
    /// `preloadAdmissibleWindows` applies on BOTH the cross-launch preload and
    /// the playhead-96ot mid-session ingest. At 0.33 the row is filtered out of
    /// both doors: the mark sits in the database and no banner ever fires.
    ///
    /// So the honest end-to-end statement is that on this fixture the carve-out
    /// is the difference between a row nobody sees and a banner — not extra
    /// seconds.
    @Test("the carve-out raises a continuation row across the delivery floor")
    func theCarveOutRaisesAContinuationRowAcrossTheDeliveryFloor() async throws {
        let treatment = try await Self.runFirstListenBackfill(arm: .dayZero)
        let control = try await Self.runFirstListenBackfill(arm: .aggregatorLookalike)
        let bare = try await Self.runFirstListenBackfill(arm: .bare)
        // `SkipOrchestrator.preloadConfidenceThreshold`, which is `private` and
        // so cannot be read here. Written as the literal deliberately rather
        // than as `markConfidenceCeiling`: reading the ceiling would make this
        // test move WITH the number it is supposed to check against, and a
        // ceiling dropped below the floor — the exact defect mutation L09
        // injects — would leave it green while every continuation row silently
        // stopped reaching the session. The behavioural witness for the same
        // coupling is `dayZeroSeededWindowArmsOnIngestAndFiresOnceOnEntry`.
        let floor = 0.70
        func deliverable(_ rows: [AdWindow]) -> Double {
            Self.overlap(rows.filter { $0.confidence >= floor }, with: Self.adTwo)
        }
        print(
            """

            == playhead-evc1 end-to-end arms (runBackfill, shipped flag) ========
            day-0 rows   \(treatment.continuation.map { (round($0.startTime * 10) / 10, round($0.endTime * 10) / 10, round($0.confidence * 100) / 100) })
            control rows \(control.continuation.map { (round($0.startTime * 10) / 10, round($0.endTime * 10) / 10, round($0.confidence * 100) / 100) })
            no-seed rows \(bare.continuation.map { (round($0.startTime * 10) / 10, round($0.endTime * 10) / 10, round($0.confidence * 100) / 100) })
            ad 2 seconds, ALL rows:  day-0 \(Self.overlap(treatment.continuation, with: Self.adTwo))  control \(Self.overlap(control.continuation, with: Self.adTwo))  no seed \(Self.overlap(bare.continuation, with: Self.adTwo))
            ad 2 seconds, DELIVERABLE (>= \(floor)): day-0 \(deliverable(treatment.continuation))  control \(deliverable(control.continuation))  no seed \(deliverable(bare.continuation))
            =====================================================================

            """
        )
        #expect(
            deliverable(treatment.continuation) > deliverable(control.continuation),
            """
            The day-0 arm delivered \(deliverable(treatment.continuation)) s of ad 2 \
            above the \(floor) preload floor and the control delivered \
            \(deliverable(control.continuation)) s. With no difference the carve-out \
            changes nothing a listener can see on this fixture.
            """
        )
        #expect(
            deliverable(control.continuation) == 0.0,
            "the control's rows cleared the delivery floor without a day-0 seed"
        )
        #expect(
            deliverable(bare.continuation) == 0.0,
            "rows composed with no seed row at all cleared the delivery floor"
        )
        for row in treatment.continuation {
            #expect(
                row.endTime <= Self.showResumes,
                "row \(row.startTime)-\(row.endTime) reached into the show"
            )
        }
    }

    /// The production mint stamps `config.detectorVersion` on a day-0 mark —
    /// the SAME version the backfill reconciles against — and the row survives
    /// only because `reconcileProtectedBoundaryStates` and the hot-path
    /// retirement filter both exempt `dayZeroRediffByteExact`. If that exemption
    /// ever lapses the seed is gone before Step 18b reads the store, and the
    /// recovery above would quietly stop happening in production while every
    /// compose-level test stayed green.
    @Test("the production-faithful day-0 seed survives its own backfill")
    func productionFaithfulSeedSurvivesTheBackfill() async throws {
        let (_, continuation, allRows) = try await Self.runFirstListenBackfill(
            arm: .dayZero,
            seedDetectorVersion: "evc1-detection-v1"
        )
        #expect(
            allRows.contains {
                $0.boundaryState == AdDetectionService.dayZeroRediffByteExactBoundaryState
                    && $0.decisionState == AdDecisionState.candidate.rawValue
                    && $0.startTime == Self.adOne.start
                    && $0.endTime == Self.adOne.end
            },
            "the day-0 seed was reconciled away: \(allRows.map { ($0.boundaryState, $0.decisionState, $0.startTime, $0.endTime) })"
        )
        // The seed reached Step 18b: a mark over ad 2 carries the confidence
        // only a 1.00-confidence seed can produce. A row at the pipeline's own
        // 0.33 would mean the seed was retired and something else chained.
        #expect(
            continuation.contains {
                $0.startTime < Self.adTwo.end
                    && $0.endTime > Self.adTwo.start
                    && $0.confidence >= 0.70
            },
            "no mark over ad 2 carries the day-0 seed's confidence: \(continuation.map { ($0.startTime, $0.endTime, $0.confidence) })"
        )
    }

    // MARK: - 4/5/6. The orchestrator harness

    /// A LIVE-SESSION store carrying the asset and exactly the rows given.
    ///
    /// Deliberately NOT the backfill's own store: that one also holds the day-0
    /// seed, a 1.00-confidence row the preload would forward, so "no cue fired"
    /// could be about the wrong window. Here the only detector material is the
    /// continuation output, and the positive control below proves this harness
    /// can fire a cue at all.
    private static func makeSessionStore(
        with rows: [AdWindow]
    ) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: assetId)
        )
        if !rows.isEmpty {
            try await store.insertAdWindows(rows)
        }
        return store
    }

    private static func makeOrchestrator(
        store: AnalysisStore
    ) async throws -> SkipOrchestrator {
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        return SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
    }

    /// Fail loudly if the orchestrator is not in auto mode. Every "no cue fired"
    /// assertion below is about the AUTOMATIC path; in `.shadow` or `.manual` no
    /// cue ever fires and the negatives pass for the wrong reason.
    private static func assertAutoMode(_ orchestrator: SkipOrchestrator) async {
        #expect(
            await orchestrator.currentSkipMode() == .auto,
            "fixture is not in auto mode — cue assertions would be vacuous"
        )
    }

    /// The VACUITY CONTROL. An ordinary confirmed window with no precision gate
    /// cues immediately on ingest. If this stops firing, every "no cue"
    /// assertion in this suite has become an assertion about a broken harness.
    private static func autoSkippableControl() -> AdWindow {
        makeSkipTestAdWindow(
            id: "evc1-control-eligible",
            assetId: assetId,
            startTime: 150,
            endTime: 170,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
    }

    /// A mark-only sentinel used to bound a stream drain. Deliberately NOT a
    /// continuation row: it exists only to mark the frame boundary.
    private static func sentinelSuggestion(id: String, at time: Double) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: time,
            endTime: time + 4,
            confidence: 0.71,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "evc1-sentinel-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: time,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    // MARK: - 4. playhead-2350 with a day-0 seed

    /// A continuation row seeded by a day-0 mark and walked past by the playhead
    /// in AUTO mode must never produce a skip cue — while still producing a
    /// banner, so the negative is not vacuous.
    ///
    /// The seed's own edges are `.rediffByteExact` and its gate is `.eligible`;
    /// the derived row's are `.unanchored` / `.markOnly`. That the certainty does
    /// NOT propagate along the chain is the whole safety claim of this bead.
    @Test("2350: a day-0 seeded continuation span never auto-skips, and still banners")
    func dayZeroSeededSpanNeverAutoSkips() async throws {
        let (continuation, attributable) = try await Self.dayZeroAttributableRows()
        let row = try #require(
            attributable.first,
            "no persisted row is attributable to the day-0 seed — this test would be about the pipeline's own output"
        )
        #expect(row.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
        #expect(row.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(row.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)

        let store = try await Self.makeSessionStore(with: continuation)
        let orchestrator = try await Self.makeOrchestrator(store: store)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)
        var reader = DayZeroSeedBannerReader(await orchestrator.bannerItemStream())

        // VACUITY CONTROL FIRST, at [150, 170) — past the pod and past the walk
        // below, so it can never be consumed by it.
        await orchestrator.receiveAdWindows([Self.autoSkippableControl()])
        #expect(
            !pushedCues.isEmpty,
            "the control window must cue — otherwise every 'no cue' below is about a broken harness"
        )
        let baselineCueCount = pushedCues.count

        await orchestrator.receiveAdWindows(continuation)
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(row.id),
            "a continuation row must reach the SUGGEST tier — otherwise nothing below is exercised"
        )
        #expect(
            pushedCues.count == baselineCueCount,
            "ingesting day-0 seeded continuation rows added a cue"
        )

        let spanStart = continuation.map(\.startTime).min() ?? 0
        let spanEnd = continuation.map(\.endTime).max() ?? 0
        var time = max(0, spanStart - 1)
        while time <= spanEnd + 1 {
            await orchestrator.updatePlayheadTime(time)
            time += PlaybackService.periodicTimeObserverIntervalSeconds
        }
        #expect(
            pushedCues.count == baselineCueCount,
            "walking the day-0 seeded span added a cue"
        )
        let trespassing = pushedCues.filter { cue in
            let cueStart = CMTimeGetSeconds(cue.start)
            let cueEnd = CMTimeGetSeconds(cue.start + cue.duration)
            return continuation.contains {
                cueStart < $0.endTime && cueEnd > $0.startTime
            }
        }
        #expect(
            trespassing.isEmpty,
            """
            A span derived from a byte-exact seed defined a skip boundary. The \
            seed's certainty must not propagate along the chain — playhead-2350 \
            is the gate that must prevent it. Got \
            \(trespassing.map { (CMTimeGetSeconds($0.start), CMTimeGetSeconds($0.start + $0.duration)) })
            """
        )

        // The banner DID fire — so "no cue" above is a policy result, not silence.
        await orchestrator.receiveAdWindows([
            Self.sentinelSuggestion(id: "evc1-2350-sentinel", at: 120)
        ])
        await orchestrator.updatePlayheadTime(120)
        let banners = await reader.drain(until: "evc1-2350-sentinel")
        #expect(
            banners.contains { $0.windowId == row.id && $0.tier == .suggest },
            "the recovered pod material must still reach the listener as a banner; got \(banners.map(\.windowId))"
        )
    }

    // MARK: - 5. playhead-ynmk with a day-0 seed

    /// Tapping Yes on a day-0 seeded continuation banner records a MARK. It
    /// cannot cut: a confirmation asserts presence, and the row has no anchored
    /// edge to define extent from — however anchored its seed was.
    @Test("ynmk: confirming a day-0 seeded banner marks, it does not skip")
    func confirmingADayZeroSeededBannerMarksRatherThanSkips() async throws {
        let (continuation, attributable) = try await Self.dayZeroAttributableRows()
        let row = try #require(
            attributable.first,
            "no persisted row is attributable to the day-0 seed — this test would be about the pipeline's own output"
        )

        let store = try await Self.makeSessionStore(with: continuation)
        let orchestrator = try await Self.makeOrchestrator(store: store)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        await orchestrator.receiveAdWindows([row])
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(row.id),
            "the tap under test never happens unless the row reaches the suggest tier"
        )
        #expect(
            await orchestrator.acceptSuggestedSkip(windowId: row.id),
            "the gesture is accepted as FEEDBACK even when no extent is skippable"
        )
        #expect(
            pushedCues.isEmpty,
            """
            One tap turned day-0 seeded pod material into a skip. That is the \
            ynmk field defect (210 s of show, 2026-07-31) reached through a new \
            door. Got \
            \(pushedCues.map { (CMTimeGetSeconds($0.start), CMTimeGetSeconds($0.start + $0.duration)) })
            """
        )

        let rows = try await store.fetchAdWindows(assetId: Self.assetId)
        let promoted = try #require(
            rows.first { $0.boundaryState == "userConfirmedSuggested" },
            "the confirmation must still create its durable promoted row"
        )
        #expect(
            promoted.wasSkipped == false,
            "no cue fired, so `wasSkipped` must not say one did"
        )
        #expect(
            promoted.decisionState == AdDecisionState.confirmed.rawValue,
            "`applied` means the listener's audio was skipped; nothing was"
        )

        // VACUITY CONTROL: this harness CAN fire a cue.
        await orchestrator.receiveAdWindows([Self.autoSkippableControl()])
        #expect(
            !pushedCues.isEmpty,
            "the control window must cue — otherwise 'the tap fired no cue' is about a broken harness"
        )
    }

    // MARK: - 6. playhead-d3g0 + playhead-96ot delivery

    /// The real mid-listen shape for a FIRST listen: the day-0 mint lands, the
    /// backfill composes off it, and the playhead is ALREADY inside the recovered
    /// span when the rows reach the live session through
    /// `ingestPersistedAdWindows`.
    ///
    /// Three things at once, because separating them would let each pass for the
    /// other's reason: ingest ARMS but does not emit; the next tick INSIDE the
    /// span emits (containment, not a transition); and however many further
    /// ticks land inside, it emits exactly ONCE.
    ///
    /// This is also the rail on the confidence coupling. A day-0 seed carries
    /// confidence 1.00; the mark inherits it capped at
    /// `markConfidenceCeiling` (0.70), which IS
    /// `SkipOrchestrator.preloadConfidenceThreshold`. Drop the ceiling and
    /// `preloadAdmissibleWindows` filters every continuation row out, `forwarded`
    /// is 0, and the banner this whole bead exists for never arrives.
    @Test("d3g0: a day-0 seeded continuation window arms on ingest, fires on entry, once")
    func dayZeroSeededWindowArmsOnIngestAndFiresOnceOnEntry() async throws {
        let (continuation, attributable) = try await Self.dayZeroAttributableRows()
        let row = try #require(
            attributable.first,
            "no persisted row is attributable to the day-0 seed — this test would be about the pipeline's own output"
        )
        let inside = (row.startTime + row.endTime) / 2.0
        #expect(
            row.confidence >= 0.7,
            "a continuation row below the preload floor never reaches the session at all"
        )

        // The real ordering: the session begins with NO continuation rows on
        // disk, the listener plays into the pod, and only then does the backfill
        // persist and 96ot deliver.
        let store = try await Self.makeSessionStore(with: [])
        let orchestrator = try await Self.makeOrchestrator(store: store)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.podcastId
        )
        var reader = DayZeroSeedBannerReader(await orchestrator.bannerItemStream())

        await orchestrator.updatePlayheadTime(inside)
        try await store.insertAdWindows(continuation)

        let forwarded = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Self.assetId
        )
        #expect(forwarded > 0, "96ot must forward the freshly-persisted rows")
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(row.id),
            "ingest must ARM the continuation window"
        )

        // Ingest itself emits nothing — d3g0's whole point. A sentinel driven
        // OUTSIDE every continuation span proves the buffer is empty rather than
        // proving a race we won.
        await orchestrator.receiveAdWindows([
            Self.sentinelSuggestion(id: "evc1-d3g0-arm", at: Self.episodeDuration - 5)
        ])
        await orchestrator.updatePlayheadTime(Self.episodeDuration - 5)
        let atIngest = await reader.drain(until: "evc1-d3g0-arm")
        #expect(
            !atIngest.contains { $0.windowId == row.id },
            "ingest must not emit; d3g0 gives emission to the position path"
        )

        // Back inside the span: the next tick fires it. Containment, not a
        // transition — the playhead never crossed the start edge.
        await orchestrator.updatePlayheadTime(inside)
        await orchestrator.receiveAdWindows([
            Self.sentinelSuggestion(id: "evc1-d3g0-entry", at: Self.episodeDuration - 10)
        ])
        await orchestrator.updatePlayheadTime(Self.episodeDuration - 10)
        let onEntry = await reader.drain(until: "evc1-d3g0-entry")
        #expect(
            onEntry.contains { $0.windowId == row.id && $0.tier == .suggest },
            "a window armed while the playhead is inside must fire on the next tick; got \(onEntry.map(\.windowId))"
        )

        // Twelve more ticks inside the same span must ask nothing further.
        var time = inside
        for _ in 0..<12 {
            time += PlaybackService.periodicTimeObserverIntervalSeconds
            if time >= row.endTime { break }
            await orchestrator.updatePlayheadTime(time)
        }
        await orchestrator.receiveAdWindows([
            Self.sentinelSuggestion(id: "evc1-d3g0-once", at: Self.episodeDuration - 15)
        ])
        await orchestrator.updatePlayheadTime(Self.episodeDuration - 15)
        let repeats = await reader.drain(until: "evc1-d3g0-once")
        #expect(
            !repeats.contains { $0.windowId == row.id },
            "fires at most once per window per episode; got \(repeats.map(\.windowId))"
        )
    }
}
