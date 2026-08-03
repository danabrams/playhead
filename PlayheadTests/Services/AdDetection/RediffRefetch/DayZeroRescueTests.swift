// DayZeroRescueTests.swift
// playhead-ug9m: a day-0 mark is not frozen at its first attempt's quality.
//
// WHAT IS BEING PROVED, and why the shape of the proof matters.
//
// The bead's dangerous direction is PROMOTION. A day-0 row persisted
// `eligibilityGate = .eligible` auto-skips with nothing downstream to catch it —
// playhead-bllt's `HotPathExtentGate` sits on `runHotPath` and
// `runSegmentAggregation`, playhead-2350's sits on the fusion path, and the
// day-0 mint passes through none of them. So the promotion decision is FINAL,
// and the negative that matters most is that a playhead-9s6q SEGMENT-RECOVERED
// slot is never promoted. It is asserted three separate ways below: at the
// policy (a rescue is not even granted when an anchored sibling proves the mint
// could already stamp anchors), at the mint (a non-strict re-mint supersedes
// nothing at all), and at the persisted row (the recovered lane still reads
// `unanchored`/`markOnly` after a rescue).
//
// WHY THERE IS NO MIGRATION TEST. There is no migration. The strict /
// segment-recovered classification was never persisted — not on `ad_windows`,
// not in `rediff_day_zero_attempts` — and the B-copies it was computed from are
// deleted by construction, so no row's strictness can be PROVED from disk. A
// re-stamp would have to guess, and the population it would guess wrong about is
// exactly the recovered one. The rescue instead RE-DERIVES strictness from bytes
// on the current build, through the same `strictByteExactMask` a first-listen
// mint runs — which is why the tests below can assert over real divergent MP3
// bytes rather than over a label somebody wrote down.

import Foundation
import Testing

@testable import Playhead

// MARK: - Shared fixtures

private enum RescueFixture {

    static func window(
        id: String,
        assetId: String = "a1",
        start: Double = 100,
        end: Double = 160,
        boundaryState: String = AdDetectionService.dayZeroRediffByteExactBoundaryState,
        decisionState: AdDecisionState = .candidate,
        anchor: AutoSkipEdgeAnchor = .unanchored,
        endAnchor: AutoSkipEdgeAnchor? = nil,
        gate: SkipEligibilityGate = .markOnly,
        wasSkipped: Bool = false,
        userDismissedBanner: Bool = false
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: boundaryState,
            decisionState: decisionState.rawValue,
            detectorVersion: "ug9m-test",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: wasSkipped,
            userDismissedBanner: userDismissedBanner,
            evidenceSources: nil,
            eligibilityGate: gate.rawValue,
            startEdgeAnchor: anchor.rawValue,
            endEdgeAnchor: (endAnchor ?? anchor).rawValue
        )
    }

    static func record(
        assetId: String = "a1",
        attemptCount: Int = 1,
        lastAttemptAt: Double = 0,
        lastExit: RediffDayZeroExit = .marked,
        lastMarkCount: Int = 2,
        generation: Int,
        rescueAttemptCount: Int = 0
    ) -> RediffDayZeroAttemptRecord {
        RediffDayZeroAttemptRecord(
            analysisAssetId: assetId,
            attemptCount: attemptCount,
            lastAttemptAt: lastAttemptAt,
            lastExit: lastExit,
            lastMarkCount: lastMarkCount,
            policyGeneration: generation,
            rescueAttemptCount: rescueAttemptCount
        )
    }

    /// The trapped shape: day-0 marked, every mark degraded.
    static let degradedCensus = DayZeroMarkCensus(
        anchoredCount: 0, degradedCount: 2, settledCount: 0
    )
    /// The NOT-trapped shape: the mint could stamp anchors and did, so the
    /// unanchored sibling is a deliberate qs0d withholding.
    static let mixedCensus = DayZeroMarkCensus(
        anchoredCount: 1, degradedCount: 3, settledCount: 0
    )

    /// A generation that is definitely not the shipping one, without pinning a
    /// literal that a future bump would silently invalidate.
    static var foreignGeneration: Int { DayZeroRediffAttemptPolicy.currentGeneration - 1 }
}

// MARK: - 1. The census (pure)

@Suite("Day-0 mark census (playhead-ug9m)")
struct DayZeroMarkCensusTests {

    @Test("an anchored day-0 row counts as anchored, an unanchored one as degraded")
    func classifiesByAnchorPair() {
        let census = DayZeroMarkCensus.classify(rows: [
            RescueFixture.window(id: "w1", anchor: .rediffByteExact),
            RescueFixture.window(id: "w2", anchor: .unanchored)
        ])
        #expect(census.anchoredCount == 1)
        #expect(census.degradedCount == 1)
        #expect(census.settledCount == 0)
    }

    /// A HALF-anchored row cannot be produced by the mint (it stamps one value
    /// on both edges), but if one ever appears it must read as degraded — a
    /// rescue that improves it is harmless, a promotion that assumes it is fine
    /// is not.
    @Test("a HALF-anchored row is degraded, not anchored")
    func halfAnchoredIsDegraded() {
        let census = DayZeroMarkCensus.classify(rows: [
            RescueFixture.window(id: "w1", anchor: .rediffByteExact, endAnchor: .unanchored)
        ])
        #expect(census.anchoredCount == 0)
        #expect(census.degradedCount == 1)
    }

    /// Rows from OTHER producers must not move the census in either direction —
    /// an aggregator window can neither make an asset look rescuable nor block
    /// a rescue.
    @Test("a row from another producer is ignored entirely")
    func otherProducersAreIgnored() {
        let census = DayZeroMarkCensus.classify(rows: [
            RescueFixture.window(id: "w1", boundaryState: AdBoundaryState.segmentAggregated.rawValue),
            RescueFixture.window(id: "w2", boundaryState: AdBoundaryState.lexical.rawValue,
                                 anchor: .rediffByteExact)
        ])
        #expect(census == .empty, "neither row is a day-0 byte-exact mark")
    }

    /// Every settled shape, one per axis, because "the user decided" arrives
    /// through three different columns and a rescue must respect all three.
    @Test("a vetoed, dismissed, skipped or confirmed row is SETTLED, never degraded")
    func settledRowsAreOffLimits() {
        let settled = [
            RescueFixture.window(id: "veto", decisionState: .reverted),
            RescueFixture.window(id: "confirmed", decisionState: .confirmed),
            RescueFixture.window(id: "applied", decisionState: .applied),
            RescueFixture.window(id: "skipped", wasSkipped: true),
            RescueFixture.window(id: "dismissed", userDismissedBanner: true)
        ]
        let census = DayZeroMarkCensus.classify(rows: settled)
        #expect(census.settledCount == settled.count)
        #expect(census.degradedCount == 0)
        #expect(!census.isRescuable, "a settled asset has nothing a re-fetch may touch")
        for row in settled {
            #expect(!DayZeroMarkCensus.isSupersedable(row), "\(row.id) must not be supersedable")
        }
    }

    /// The ordering hole this caught, and it is not hypothetical: a settled row
    /// bucketed away from `anchoredCount` would make an asset whose ONLY
    /// anchored mark the user vetoed read back as rescuable — and the rescue
    /// would then re-derive exactly the 9s6q segment-recovered siblings qs0d
    /// withheld. A veto retracts a MARK; it does not un-prove that the mint
    /// could stamp an anchor.
    @Test("a VETOED but ANCHORED row still counts as anchored, and still blocks the rescue")
    func vetoedAnchoredRowStillProvesTheMintCouldStamp() {
        let census = DayZeroMarkCensus.classify(rows: [
            RescueFixture.window(id: "vetoed", anchor: .rediffByteExact, decisionState: .reverted),
            RescueFixture.window(id: "degraded", start: 400, end: 460)
        ])
        #expect(census.anchoredCount == 1, "anchored is tested BEFORE settled")
        #expect(census.settledCount == 0)
        #expect(census.degradedCount == 1)
        #expect(!census.isRescuable)
        #expect(DayZeroRediffAttemptPolicy.decide(
            record: RescueFixture.record(generation: RescueFixture.foreignGeneration),
            markCensus: census,
            now: 10_000_000
        ) == .suppress(reason: .marked, nextEligibleAt: nil))
        // The witness that the fixture is discriminating: drop the vetoed
        // anchored row and the SAME degraded row is rescuable again.
        #expect(DayZeroMarkCensus.classify(rows: [
            RescueFixture.window(id: "degraded", start: 400, end: 460)
        ]).isRescuable)
        // And it is still not supersedable — a veto is off limits either way.
        #expect(!DayZeroMarkCensus.isSupersedable(
            RescueFixture.window(id: "vetoed", anchor: .rediffByteExact, decisionState: .reverted)))
    }

    /// THE conjunction the whole bound rests on.
    @Test("isRescuable requires degraded rows AND no anchored row")
    func rescuableIsAConjunction() {
        #expect(RescueFixture.degradedCensus.isRescuable)
        #expect(!RescueFixture.mixedCensus.isRescuable,
                "one anchored row proves the mint could stamp anchors — the rest are withheld ON PURPOSE")
        #expect(!DayZeroMarkCensus.empty.isRescuable, "no marks is a FIRST attempt, not a rescue")
        #expect(!DayZeroMarkCensus(anchoredCount: 2, degradedCount: 0, settledCount: 0).isRescuable)
    }

    @Test("freezeState names every reachable state")
    func freezeStateTruthTable() {
        #expect(DayZeroMarkCensus.empty.freezeState(rescueAttemptCount: 0) == .noMarks)
        #expect(RescueFixture.mixedCensus.freezeState(rescueAttemptCount: 0) == .anchored)
        #expect(RescueFixture.degradedCensus.freezeState(rescueAttemptCount: 0) == .rescuable)
        #expect(RescueFixture.degradedCensus
            .freezeState(rescueAttemptCount: DayZeroRediffAttemptPolicy.maxRescueAttempts) == .frozen)
        #expect(DayZeroMarkCensus(anchoredCount: 0, degradedCount: 0, settledCount: 2)
            .freezeState(rescueAttemptCount: 0) == .settled)
    }

    /// `isSupersedable` is what the mint's relaxed overlap filter consults, so
    /// it gets its own positive case rather than being proved only by negatives.
    @Test("only a degraded, unsettled, day-0 row is supersedable")
    func supersedableIsNarrow() {
        #expect(DayZeroMarkCensus.isSupersedable(RescueFixture.window(id: "degraded")))
        #expect(!DayZeroMarkCensus.isSupersedable(
            RescueFixture.window(id: "anchored", anchor: .rediffByteExact)))
        #expect(!DayZeroMarkCensus.isSupersedable(
            RescueFixture.window(id: "other", boundaryState: AdBoundaryState.segmentAggregated.rawValue)))
    }
}

// MARK: - 2. The policy

@Suite("Day-0 rescue policy (playhead-ug9m)")
struct DayZeroRescuePolicyTests {

    /// The pre-existing contract, restated: within a generation `.marked` is as
    /// terminal as it ever was. A rescue is granted by a generation BUMP, which
    /// is a deliberate act, not by replaying an episode.
    @Test("a marked exit in the CURRENT generation is still terminal, degraded marks or not")
    func markedStaysTerminalWithinAGeneration() {
        let decision = DayZeroRediffAttemptPolicy.decide(
            record: RescueFixture.record(generation: DayZeroRediffAttemptPolicy.currentGeneration),
            markCensus: RescueFixture.degradedCensus,
            now: 10_000_000
        )
        #expect(decision == .suppress(reason: .marked, nextEligibleAt: nil))
    }

    /// THE RESCUE.
    @Test("a marked exit from an OLDER generation with WHOLLY DEGRADED marks re-attempts once")
    func rescueIsGranted() {
        let decision = DayZeroRediffAttemptPolicy.decide(
            record: RescueFixture.record(generation: RescueFixture.foreignGeneration),
            markCensus: RescueFixture.degradedCensus,
            now: 10_000_000
        )
        #expect(decision == .attempt(attemptNumber: 1))
    }

    /// THE CENTREPIECE NEGATIVE, at the policy tier. An asset that already has
    /// one anchored day-0 row is NOT rescued — its unanchored siblings are
    /// provably segment-recovered, which playhead-qs0d withheld deliberately,
    /// and a re-fetch would spend ~108 MB to re-derive the same verdict.
    @Test("an asset with an ANCHORED day-0 row is never rescued — its unanchored siblings are 9s6q-recovered")
    func anchoredSiblingBlocksTheRescue() {
        let decision = DayZeroRediffAttemptPolicy.decide(
            record: RescueFixture.record(generation: RescueFixture.foreignGeneration),
            markCensus: RescueFixture.mixedCensus,
            now: 10_000_000
        )
        #expect(decision == .suppress(reason: .marked, nextEligibleAt: nil))
    }

    @Test("an asset whose day-0 marks are all USER-SETTLED is never rescued")
    func settledMarksBlockTheRescue() {
        let decision = DayZeroRediffAttemptPolicy.decide(
            record: RescueFixture.record(generation: RescueFixture.foreignGeneration),
            markCensus: DayZeroMarkCensus(anchoredCount: 0, degradedCount: 0, settledCount: 3),
            now: 10_000_000
        )
        #expect(decision == .suppress(reason: .marked, nextEligibleAt: nil))
    }

    /// THE BOUND, and the state the bead exists to make visible.
    @Test("a spent rescue suppresses as rescueExhausted, not as silence")
    func spentRescueIsNamed() {
        let decision = DayZeroRediffAttemptPolicy.decide(
            record: RescueFixture.record(
                generation: RescueFixture.foreignGeneration,
                rescueAttemptCount: DayZeroRediffAttemptPolicy.maxRescueAttempts
            ),
            markCensus: RescueFixture.degradedCensus,
            now: 10_000_000
        )
        #expect(decision == .suppress(reason: .rescueExhausted, nextEligibleAt: nil))
    }

    /// The leak this ceiling closes. A rescue that ends in a RETRYABLE exit
    /// leaves `lastExit != .marked`, so without a mark-keyed ceiling it would
    /// fall straight through into the ordinary three-attempt budget — turning a
    /// ~108 MB second chance into ~324 MB.
    @Test("the ceiling holds even when the rescue ended in a RETRYABLE exit")
    func ceilingSurvivesARetryableRescueOutcome() {
        let record = RescueFixture.record(
            attemptCount: 1,
            lastAttemptAt: 0,
            lastExit: .noDivergentSlot,
            generation: DayZeroRediffAttemptPolicy.currentGeneration,
            rescueAttemptCount: 1
        )
        // Far past any backoff window, so ONLY the ceiling can be suppressing.
        let decision = DayZeroRediffAttemptPolicy.decide(
            record: record,
            markCensus: RescueFixture.degradedCensus,
            now: 100 * 24 * 60 * 60
        )
        #expect(decision == .suppress(reason: .rescueExhausted, nextEligibleAt: nil))

        // The positive witness: with NO marks on disk the same record is an
        // ordinary retry, so the suppression above is the ceiling and not the
        // backoff or the budget.
        let noMarks = DayZeroRediffAttemptPolicy.decide(
            record: record, markCensus: .empty, now: 100 * 24 * 60 * 60
        )
        #expect(noMarks == .attempt(attemptNumber: 2))
    }

    /// The default argument must reproduce the pre-bead behaviour EXACTLY, in
    /// the conservative direction: no rescue, no ceiling.
    @Test("omitting the census reproduces the pre-ug9m policy exactly")
    func defaultCensusIsInert() {
        for generation in [RescueFixture.foreignGeneration, DayZeroRediffAttemptPolicy.currentGeneration] {
            #expect(
                DayZeroRediffAttemptPolicy.decide(
                    record: RescueFixture.record(generation: generation), now: 0
                ) == .suppress(reason: .marked, nextEligibleAt: nil),
                "generation \(generation) must stay terminal without a census"
            )
        }
    }

    @Test("rescueExhausted is free and terminal")
    func rescueExhaustedTaxonomy() {
        #expect(!RediffDayZeroExit.rescueExhausted.spentBandwidth,
                "the refusal happens before any fetch")
        #expect(!RediffDayZeroExit.rescueExhausted.isRetryable,
                "it is a terminal statement about this generation")
    }

    /// EXACTLY ONE re-attempt, enforced — driven through the real
    /// `decide` → `advance` loop rather than asserted on the counter directly,
    /// because the claim is about the pair, not about either half.
    @Test("a trapped asset spends EXACTLY ONE rescue across repeated plays")
    func exactlyOneRescueOverManyPlays() {
        var record: RediffDayZeroAttemptRecord? = RescueFixture.record(
            generation: RescueFixture.foreignGeneration
        )
        // The rescue keeps producing a wholly-degraded mark set — the worst
        // case, and the one an unbounded policy would loop on forever.
        let census = RescueFixture.degradedCensus
        var attempts = 0
        var reasons: [RediffDayZeroExit] = []
        var now: Double = 0
        for _ in 0..<12 {
            now += 30 * 24 * 60 * 60   // a month between plays: no backoff can be masking this
            switch DayZeroRediffAttemptPolicy.decide(record: record, markCensus: census, now: now) {
            case .attempt:
                attempts += 1
                record = DayZeroRediffAttemptPolicy.advance(
                    record: record,
                    assetId: "a1",
                    outcome: RediffDayZeroMintOutcome(markCount: 2, exit: .marked, strictMarkCount: 0),
                    fullFetchBytes: 108_000_000,
                    at: now
                )
            case .suppress(let reason, _):
                reasons.append(reason)
            }
        }
        #expect(attempts == 1, "the rescue budget is 1 per generation")
        #expect(record?.rescueAttemptCount == 1)
        #expect(reasons.allSatisfy { $0 == .rescueExhausted },
                "every later play must NAME the frozen state — got \(Set(reasons))")
        #expect(!reasons.isEmpty, "vacuity guard: later plays really were evaluated")
    }

    @Test("advance counts a rescue only for a marked prior that spent bytes")
    func advanceCountsTheRightAttempts() {
        let prior = RescueFixture.record(generation: RescueFixture.foreignGeneration)

        let spent = DayZeroRediffAttemptPolicy.advance(
            record: prior, assetId: "a1",
            outcome: RediffDayZeroMintOutcome(markCount: 1, exit: .marked),
            fullFetchBytes: 108_000_000, at: 1
        )
        #expect(spent.rescueAttemptCount == 1)

        // A FREE exit is not a rescue — it spent nothing to bound.
        let free = DayZeroRediffAttemptPolicy.advance(
            record: prior, assetId: "a1",
            outcome: RediffDayZeroMintOutcome(exit: .aSideNotAnchored),
            fullFetchBytes: 0, at: 1
        )
        #expect(free.rescueAttemptCount == 0)

        // An ordinary first-listen attempt is not a rescue either.
        let firstListen = DayZeroRediffAttemptPolicy.advance(
            record: nil, assetId: "a1",
            outcome: RediffDayZeroMintOutcome(markCount: 1, exit: .marked),
            fullFetchBytes: 108_000_000, at: 1
        )
        #expect(firstListen.rescueAttemptCount == 0)
    }
}

// MARK: - 3. The mint, over REAL divergent MP3 bytes

@Suite("Day-0 rescue mint supersedes only what it can prove (playhead-ug9m)")
struct DayZeroRescueMintTests {

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40, confirmationThreshold: 0.70,
                suppressionThreshold: 0.25, hotPathLookahead: 90.0,
                detectorVersion: "ug9m-test", fmBackfillMode: .off,
                rediffSlotOwnershipEnabled: true
            ),
            rediffBSideProvider: nil
        )
    }

    private func insertAsset(store: AnalysisStore, assetId: String, sourceURL: String) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: "ep-\(assetId)", assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil, sourceURL: sourceURL,
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 280
        ))
    }

    /// The same STRICT A/B construction playhead-qs0d's suite uses: one
    /// ID3-separated ad block over ≈[95, 165] s, a chain the byte gate accepts
    /// on its monotonic-clean arm.
    private struct StrictPair {
        let aURL: URL, b0: URL, b1: URL, aData: Data, bData: Data

        static func stage(in dir: URL) throws -> StrictPair {
            let adStartFrame = 3637
            let adFrames = 2680
            let contentFrames = 10_719
            let c1 = SyntheticMP3.frames(count: adStartFrame, seed: 0xC0FFEE)
            let c2 = SyntheticMP3.frames(count: contentFrames - adStartFrame - adFrames, seed: 0xFACADE)
            let ad = SyntheticMP3.frames(count: adFrames, seed: 0xAD_B10C)
            let aData = SyntheticMP3.file(c1 + [SyntheticMP3.id3v2(payloadBytes: 32)] + ad + c2)
            let bData = SyntheticMP3.file(c1 + c2)
            let aURL = dir.appendingPathComponent("a.mp3", isDirectory: false)
            let b0 = dir.appendingPathComponent("b0.fresh.mp3", isDirectory: false)
            let b1 = dir.appendingPathComponent("b1.fresh.mp3", isDirectory: false)
            try aData.write(to: aURL)
            try bData.write(to: b0)
            try bData.write(to: b1)
            return StrictPair(aURL: aURL, b0: b0, b1: b1, aData: aData, bData: bData)
        }
    }

    /// The playhead-9s6q lane: two removed-in-B blocks, so the run chain drops a
    /// run and only SEGMENT RECOVERY can accept it.
    private struct RecoveredPair {
        let aURL: URL, b0: URL, b1: URL, aData: Data, bData: Data

        static func stage(in dir: URL) throws -> RecoveredPair {
            let contentFrames = 300
            let adFrames = 300
            let c0 = SyntheticMP3.frames(count: contentFrames, seed: 0x0C0_0001)
            let c1 = SyntheticMP3.frames(count: contentFrames, seed: 0x0C1_0002)
            let c2 = SyntheticMP3.frames(count: contentFrames, seed: 0x0C2_0003)
            let ad1 = SyntheticMP3.frames(count: adFrames, seed: 0xAD1_0004)
            let ad2 = SyntheticMP3.frames(count: adFrames, seed: 0xAD2_0005)
            let aData = SyntheticMP3.file(c0 + ad1 + c1 + ad2 + c2)
            let bData = SyntheticMP3.file(c0 + c1 + c2)
            let aURL = dir.appendingPathComponent("a.mp3", isDirectory: false)
            let b0 = dir.appendingPathComponent("b0.fresh.mp3", isDirectory: false)
            let b1 = dir.appendingPathComponent("b1.fresh.mp3", isDirectory: false)
            try aData.write(to: aURL)
            try bData.write(to: b0)
            try bData.write(to: b1)
            return RecoveredPair(aURL: aURL, b0: b0, b1: b1, aData: aData, bData: bData)
        }
    }

    /// A degraded day-0 row covering the whole span the fixture's mint finds,
    /// standing in for a pre-qs0d mint of the same episode.
    private func seedDegradedDayZeroRow(
        store: AnalysisStore,
        assetId: String,
        start: Double,
        end: Double,
        decisionState: AdDecisionState = .candidate,
        boundaryState: String = AdDetectionService.dayZeroRediffByteExactBoundaryState,
        anchor: AutoSkipEdgeAnchor = .unanchored
    ) async throws -> String {
        let id = "seeded-\(assetId)-\(Int(start))"
        try await store.upsertHotPathAdWindows(
            [RescueFixture.window(
                id: id, assetId: assetId, start: start, end: end,
                boundaryState: boundaryState, decisionState: decisionState, anchor: anchor
            )],
            existingIDs: [],
            retiredIDs: []
        )
        return id
    }

    /// THE AUDIBLE HALF. A pre-qs0d row is unanchored and banners forever; the
    /// rescue re-derives the SAME geometry on this build, proves it strict, and
    /// replaces the row with an anchored, auto-skip-eligible one.
    @Test("a STRICT re-mint supersedes its own degraded day-0 row and persists anchors + eligible")
    func strictRescueSupersedesDegradedRow() async throws {
        let dir = try makeTempDir(prefix: "Ug9mStrictRescue")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try StrictPair.stage(in: dir)
        try #require(RediffByteAligner.align(aData: pair.aData, bData: pair.bData).monotonicClean,
                     "fixture control: this pair must take the STRICT arm")

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        let staleId = try await seedDegradedDayZeroRow(store: store, assetId: "a1", start: 90, end: 170)

        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .marked, "got \(outcome.exit)")
        #expect(outcome.strictMarkCount == 1)
        #expect(outcome.supersededMarkCount == 1)

        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(!rows.contains { $0.id == staleId }, "the degraded row must be RETIRED, not left beside")
        let row = try #require(rows.first)
        #expect(rows.count == 1, "exactly one row survives — got \(rows.count)")
        #expect(row.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue)
        #expect(row.endEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue)
        #expect(row.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "THE point of the bead: this row can now auto-skip")
    }

    /// THE CENTREPIECE NEGATIVE, at the mint tier. A segment-recovered re-mint
    /// must supersede NOTHING — not even its own degraded row — because a
    /// second draw that dropped runs is not evidence that it is better than the
    /// first. The existing row survives, unanchored, and nothing is promoted.
    @Test("a SEGMENT-RECOVERED re-mint supersedes nothing and promotes nothing")
    func recoveredRescueSupersedesNothing() async throws {
        let dir = try makeTempDir(prefix: "Ug9mRecoveredRescue")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try RecoveredPair.stage(in: dir)
        try #require(!RediffByteAligner.align(aData: pair.aData, bData: pair.bData).monotonicClean,
                     "fixture control: the multi-break chain must go NON-monotonic")
        try #require(RediffActivation.nonMonotonicSegmentRecoveryEnabled,
                     "fixture control: day-0 must be opted into segment recovery")

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)

        // Vacuity guard: without a seeded row this same fixture DOES mint, so
        // the `.allSlotsAlreadyCovered` below is the overlap filter refusing to
        // supersede — not the fixture failing to diverge.
        let control = try await makeTestStore()
        try await insertAsset(store: control, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        let controlOutcome = await makeService(store: control)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])
        try #require(controlOutcome.exit == .marked, "fixture control: the pair really diverges")
        try #require(controlOutcome.strictMarkCount == 0,
                     "fixture control: NOTHING here may be strict")
        let controlRows = try await control.fetchAdWindows(assetId: "a1")
        try #require(!controlRows.isEmpty)

        // Now seed a degraded day-0 row over every slot the recovered mint finds.
        var staleIds: [String] = []
        for row in controlRows {
            staleIds.append(try await seedDegradedDayZeroRow(
                store: store, assetId: "a1",
                start: row.startTime - 1, end: row.endTime + 1
            ))
        }

        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .allSlotsAlreadyCovered,
                "a non-strict slot may not replace anything — got \(outcome.exit)")
        #expect(outcome.supersededMarkCount == 0)

        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(Set(rows.map(\.id)) == Set(staleIds), "every seeded row must survive untouched")
        for row in rows {
            #expect(row.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(row.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(row.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
                    "a 9s6q segment-recovered slot is NEVER promoted")
        }
    }

    /// The fidelity ladder is not negotiable by a re-fetch: a user veto blocks
    /// the slot exactly as it always did, even for a STRICT re-mint.
    @Test("a STRICT re-mint does not supersede a user-VETOED day-0 row")
    func vetoBlocksTheSupersede() async throws {
        let dir = try makeTempDir(prefix: "Ug9mVetoRescue")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try StrictPair.stage(in: dir)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        let vetoedId = try await seedDegradedDayZeroRow(
            store: store, assetId: "a1", start: 90, end: 170, decisionState: .reverted
        )

        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .allSlotsAlreadyCovered, "got \(outcome.exit)")
        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(rows.map(\.id) == [vetoedId], "the veto survives and nothing new is minted")
    }

    /// Another producer's window keeps its own life. The relaxation is scoped to
    /// the day-0 mint's OWN rows and nothing else.
    @Test("a STRICT re-mint does not supersede another producer's window")
    func otherProducerBlocksTheSupersede() async throws {
        let dir = try makeTempDir(prefix: "Ug9mForeignRescue")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try StrictPair.stage(in: dir)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        let foreignId = try await seedDegradedDayZeroRow(
            store: store, assetId: "a1", start: 90, end: 170,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue
        )

        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .allSlotsAlreadyCovered, "got \(outcome.exit)")
        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(rows.map(\.id) == [foreignId])
    }

    /// An ALREADY-anchored day-0 row is not replaced either: there is nothing to
    /// improve, and this fetch's geometry has no claim to be better than the one
    /// already proven.
    @Test("a STRICT re-mint does not supersede an already-ANCHORED day-0 row")
    func anchoredRowIsNotReplaced() async throws {
        let dir = try makeTempDir(prefix: "Ug9mAnchoredRescue")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try StrictPair.stage(in: dir)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        let anchoredId = try await seedDegradedDayZeroRow(
            store: store, assetId: "a1", start: 90, end: 170, anchor: .rediffByteExact
        )

        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .allSlotsAlreadyCovered, "got \(outcome.exit)")
        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(rows.map(\.id) == [anchoredId])
    }

    /// A first-listen mint is BYTE-IDENTICAL to before: nothing to supersede,
    /// and the new counters read zero rather than firing on every mint.
    @Test("a first-listen mint supersedes nothing and reports it")
    func firstListenIsUnchanged() async throws {
        let dir = try makeTempDir(prefix: "Ug9mFirstListen")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try StrictPair.stage(in: dir)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .marked)
        #expect(outcome.supersededMarkCount == 0)
        #expect(outcome.strictMarkCount == outcome.markCount)
    }
}

// MARK: - 4. Persistence + the surfaced frozen state

@Suite("Day-0 rescue persistence and surfacing (playhead-ug9m)")
struct DayZeroRescuePersistenceTests {

    private func insertAsset(store: AnalysisStore, assetId: String) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: "ep-\(assetId)", assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil, sourceURL: "file:///dev/null/\(assetId).mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 280
        ))
    }

    @Test("rescueAttemptCount round-trips through SQLite")
    func rescueCountRoundTrips() async throws {
        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1")
        try await store.upsertRediffDayZeroAttempt(
            RescueFixture.record(
                generation: DayZeroRediffAttemptPolicy.currentGeneration,
                rescueAttemptCount: 1
            )
        )
        let read = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "a1"))
        #expect(read.rescueAttemptCount == 1)
        #expect(read.lastExit == .marked)

        // The v43 column defaults to 0 for a row this build did not write —
        // asserted by writing a record that carries the default.
        try await store.upsertRediffDayZeroAttempt(
            RescueFixture.record(generation: DayZeroRediffAttemptPolicy.currentGeneration)
        )
        #expect(try await store.fetchRediffDayZeroAttempt(assetId: "a1")?.rescueAttemptCount == 0)
    }

    /// The two legs of the decision must come from ONE store call, so a mint
    /// cannot land between them.
    @Test("fetchDayZeroAttemptContext returns the record AND the census together")
    func contextIsOneSnapshot() async throws {
        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1")
        try await store.upsertRediffDayZeroAttempt(
            RescueFixture.record(generation: RescueFixture.foreignGeneration)
        )
        try await store.upsertHotPathAdWindows(
            [RescueFixture.window(id: "w1"), RescueFixture.window(id: "w2", start: 400, end: 460)],
            existingIDs: [], retiredIDs: []
        )

        let context = try await store.fetchDayZeroAttemptContext(assetId: "a1")
        #expect(context.record?.lastExit == .marked)
        #expect(context.markCensus.degradedCount == 2)
        #expect(context.markCensus.isRescuable)
        #expect(DayZeroRediffAttemptPolicy.decide(
            record: context.record, markCensus: context.markCensus, now: 0
        ) == .attempt(attemptNumber: 1))
    }

    /// THE SURFACING. A frozen asset must be QUERYABLE — the acceptance
    /// criterion is that its state is reported, not merely absent.
    @Test("fetchDayZeroMarkFreezeReports names rescuable, frozen and anchored assets")
    func freezeReportsNameEveryState() async throws {
        let store = try await makeTestStore()
        for assetId in ["rescuable", "frozen", "anchored"] {
            try await insertAsset(store: store, assetId: assetId)
        }
        try await store.upsertHotPathAdWindows([
            RescueFixture.window(id: "r1", assetId: "rescuable"),
            RescueFixture.window(id: "f1", assetId: "frozen"),
            RescueFixture.window(id: "a1", assetId: "anchored", anchor: .rediffByteExact),
            RescueFixture.window(id: "a2", assetId: "anchored", start: 400, end: 460)
        ], existingIDs: [], retiredIDs: [])
        try await store.upsertRediffDayZeroAttempt(
            RescueFixture.record(assetId: "rescuable", generation: RescueFixture.foreignGeneration)
        )
        try await store.upsertRediffDayZeroAttempt(
            RescueFixture.record(
                assetId: "frozen", generation: DayZeroRediffAttemptPolicy.currentGeneration,
                rescueAttemptCount: DayZeroRediffAttemptPolicy.maxRescueAttempts
            )
        )
        try await store.upsertRediffDayZeroAttempt(
            RescueFixture.record(assetId: "anchored", generation: DayZeroRediffAttemptPolicy.currentGeneration)
        )

        let reports = try await store.fetchDayZeroMarkFreezeReports()
        let byAsset = Dictionary(uniqueKeysWithValues: reports.map { ($0.analysisAssetId, $0) })
        #expect(reports.count == 3, "every asset with day-0 marks is reported")
        #expect(byAsset["rescuable"]?.state == .rescuable)
        #expect(byAsset["frozen"]?.state == .frozen,
                "THE state this bead exists to make visible")
        #expect(byAsset["anchored"]?.state == .anchored)
        #expect(byAsset["anchored"]?.census.anchoredCount == 1)
        #expect(byAsset["anchored"]?.census.degradedCount == 1,
                "the withheld 9s6q sibling is reported, not hidden")
        #expect(byAsset["frozen"]?.lastExit == .marked)
    }

    /// An asset with no day-0 rows must not appear at all — a report that fires
    /// on every asset says nothing about any of them.
    @Test("an asset with no day-0 marks is not reported")
    func nonDayZeroAssetsAreNotReported() async throws {
        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1")
        try await store.upsertHotPathAdWindows(
            [RescueFixture.window(id: "w1", boundaryState: AdBoundaryState.segmentAggregated.rawValue)],
            existingIDs: [], retiredIDs: []
        )
        #expect(try await store.fetchDayZeroMarkFreezeReports().isEmpty)
    }
}

// MARK: - 5. The trigger actually spends (and refuses) the rescue

/// The wiring tier. Everything above is true of values and of the store; none of
/// it can see a trigger that computes the census and then hands `decide` an
/// empty one — which would restore the frozen state with every other rail green.
@Suite("Day-0 rescue reaches the fetch (playhead-ug9m)")
struct DayZeroRescueTriggerTests {

    private static let enclosure = URL(string: "https://cdn.example.com/ep.mp3")!
    private static let played = URL(fileURLWithPath: "/tmp/ug9m-played.mp3")

    private func makeTrigger(
        context: DayZeroAttemptContext,
        fetcher: KWaySpyFullFetcher,
        suppressions: SuppressionBox
    ) -> DayZeroRediffTrigger {
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: SpyDayZeroMinter(),
            now: { 0 }
        )
        return DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: 2,
            transportProvider: { .testWifi },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            attemptContextProvider: { _ in context },
            suppressionRecorder: { assetId, reason, at in
                suppressions.recorded.append((assetId, reason, at))
            },
            mintedMarkDelivery: { _ in },
            budgetWindowProvider: { .empty },
            budgetSpendRecorder: { _, _ in }
        )
    }

    final class SuppressionBox: @unchecked Sendable {
        var recorded: [(assetId: String, reason: RediffDayZeroExit, at: Double)] = []
    }

    private func fire(_ trigger: DayZeroRediffTrigger) async {
        _ = await trigger.triggerIfEligible(
            analysisAssetId: "a1",
            enclosureURL: Self.enclosure,
            playedFileURL: Self.played,
            at: 10_000_000
        )
    }

    /// THE AUDIBLE HALF, at the wiring tier: a trapped asset really does reach
    /// the fetch again.
    @Test("a trapped asset's rescue reaches the k-way fetch")
    func trappedAssetRefetches() async throws {
        let fetcher = KWaySpyFullFetcher()
        let suppressions = SuppressionBox()
        await fire(makeTrigger(
            context: DayZeroAttemptContext(
                record: RescueFixture.record(generation: RescueFixture.foreignGeneration),
                markCensus: RescueFixture.degradedCensus
            ),
            fetcher: fetcher,
            suppressions: suppressions
        ))
        #expect(!fetcher.calls.isEmpty, "the rescue must actually spend the fetch")
        #expect(suppressions.recorded.isEmpty)
    }

    /// The same record with an ANCHORED sibling must NOT refetch — the census
    /// is what distinguishes them, so this pair is the proof that the trigger
    /// reads it rather than passing `.empty`.
    @Test("an anchored sibling stops the trigger before any bytes are spent")
    func anchoredSiblingStopsTheFetch() async throws {
        let fetcher = KWaySpyFullFetcher()
        let suppressions = SuppressionBox()
        await fire(makeTrigger(
            context: DayZeroAttemptContext(
                record: RescueFixture.record(generation: RescueFixture.foreignGeneration),
                markCensus: RescueFixture.mixedCensus
            ),
            fetcher: fetcher,
            suppressions: suppressions
        ))
        #expect(fetcher.calls.isEmpty, "a 9s6q-recovered sibling is not a rescue case")
        #expect(suppressions.recorded.map(\.reason) == [.marked])
    }

    /// And the bound, surfaced: a spent rescue records `rescueExhausted`.
    @Test("a spent rescue refuses at the trigger and NAMES itself")
    func spentRescueRefusesAndIsNamed() async throws {
        let fetcher = KWaySpyFullFetcher()
        let suppressions = SuppressionBox()
        await fire(makeTrigger(
            context: DayZeroAttemptContext(
                record: RescueFixture.record(
                    generation: RescueFixture.foreignGeneration,
                    rescueAttemptCount: DayZeroRediffAttemptPolicy.maxRescueAttempts
                ),
                markCensus: RescueFixture.degradedCensus
            ),
            fetcher: fetcher,
            suppressions: suppressions
        ))
        #expect(fetcher.calls.isEmpty)
        #expect(suppressions.recorded.map(\.reason) == [.rescueExhausted],
                "the permanently-frozen state is recorded, not silent")
    }
}
