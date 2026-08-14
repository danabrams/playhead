// DayZeroRescueTests.swift
// playhead-ug9m: a day-0 mark is not frozen at its first attempt's quality.
//
// WHAT IS BEING PROVED, and why the shape of the proof matters.
//
// The bead's dangerous direction is PROMOTION. A day-0 row persisted
// `eligibilityGate = .eligible` auto-skips with nothing downstream to catch it —
// playhead-bllt's `HotPathExtentGate` sits on `runHotPath` and
// `runSegmentAggregation`, playhead-2350's sits on the fusion path, and the
// day-0 mint passes through none of them. So the promotion decision is FINAL.
//
// UNDER playhead-ug9m the negative that carried that weight was "a playhead-9s6q
// SEGMENT-RECOVERED slot is never promoted". **playhead-pyq7 measured that arm
// and promoted it**, and playhead-c7ef extends the promotion to a RE-mint over
// an episode day-0 has already marked — so that negative is gone and something
// has to hold the same weight. Three things do, and they are what the tests
// below are now organised around:
//
//   * at the policy — a rescue is not granted at all unless every day-0 mark on
//     the asset is degraded (one anchored sibling proves the mint could already
//     stamp anchors), and it is granted at most ONCE per asset per generation;
//   * at the supersede rule — the fidelity ladder is untouched (a veto, a
//     dismissal, an applied skip, another producer's window and an already
//     anchored row all still block the slot), and on top of it a re-mint may
//     retire exactly ONE row and only with a slot that lies inside the span that
//     row already marked, so **the padded cut is a subset of what the retired
//     row had marked as an ad**;
//   * at the row — the survivor of a successful re-mint is fully anchored, which
//     is what makes a second pass a no-op rather than a churn.
//
// WHY THE GENERATION BUMP IS TESTED AS A RE-ENTRY AND NOT AS A CONSTANT.
// `.marked` is terminal and playhead-c7ef does not loosen that. The rescue
// branch needs a FOREIGN `policyGeneration`, and every attempt row on the owner's
// device carried the generation that shipped — so before this bead the lane was
// CLOSED, no rescue fired, and no byte was spent. `fieldRecordsAreReachable`
// pins the literal the field carries rather than `foreignGeneration`, because
// the derived helper would keep passing through a revert of the bump.
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
            RescueFixture.window(id: "vetoed", decisionState: .reverted, anchor: .rediffByteExact),
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
            RescueFixture.window(id: "vetoed", decisionState: .reverted, anchor: .rediffByteExact)))
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
    ///
    /// ONE NEGATIVE PER CONJUNCT, and the settled one is here because the UG03
    /// mutation proved it had to be: dropping `!isSettled` left this rail green
    /// while a re-mint deleted a user veto. A rail named "only a degraded,
    /// UNSETTLED, day-0 row" that could not see the settled axis was making a
    /// claim it did not test.
    @Test("only a degraded, unsettled, day-0 row is supersedable")
    func supersedableIsNarrow() {
        #expect(DayZeroMarkCensus.isSupersedable(RescueFixture.window(id: "degraded")))
        #expect(!DayZeroMarkCensus.isSupersedable(
            RescueFixture.window(id: "anchored", anchor: .rediffByteExact)))
        #expect(!DayZeroMarkCensus.isSupersedable(
            RescueFixture.window(id: "other", boundaryState: AdBoundaryState.segmentAggregated.rawValue)))
        #expect(!DayZeroMarkCensus.isSupersedable(
            RescueFixture.window(id: "vetoed", decisionState: .reverted)),
            "a user veto is off limits to a re-fetch")
        #expect(!DayZeroMarkCensus.isSupersedable(
            RescueFixture.window(id: "dismissed", userDismissedBanner: true)))
        #expect(!DayZeroMarkCensus.isSupersedable(
            RescueFixture.window(id: "skipped", wasSkipped: true)))
    }
}

// MARK: - 1b. The re-mint supersede rule (pure, playhead-c7ef)

/// `DayZeroMarkCensus.reMintMayReplace` is the whole of what playhead-c7ef
/// relaxed, so it gets a matrix rather than only the end-to-end mint tests: with
/// the promotion switch a compile-time `static let`, the mint can only ever
/// exercise one column, and the OFF column is the rollback.
@Suite("Day-0 re-mint supersede rule (playhead-c7ef)")
struct DayZeroReMintSupersedeRuleTests {

    /// A degraded row spanning [100, 160], the thing a rescue is trying to
    /// improve.
    private static let row = RescueFixture.window(id: "degraded", start: 100, end: 160)

    private static func mayReplace(
        _ start: Double, _ end: Double,
        overlapping: [AdWindow] = [row],
        skipGrade: Bool = true
    ) -> Bool {
        DayZeroMarkCensus.reMintMayReplace(
            slotStartSeconds: start, slotEndSeconds: end,
            overlapping: overlapping, slotIsSkipGrade: skipGrade
        )
    }

    @Test("nothing to retire is never a refusal — a first-listen mint is unaffected")
    func emptyOverlapIsAdmitted() {
        #expect(Self.mayReplace(100, 160, overlapping: []))
        #expect(Self.mayReplace(100, 160, overlapping: [], skipGrade: false),
                "a mark-only slot that collides with nothing still mints, exactly as before")
    }

    /// CONJUNCT 1, and it is the rollback. With the pyq7 promotion switch off a
    /// recovered slot is not skip grade, and this rule collapses to ug9m's
    /// behaviour: nothing is ever superseded.
    @Test("a slot that is NOT skip grade replaces nothing, however well it fits")
    func skipGradeIsRequired() {
        #expect(Self.mayReplace(110, 150, skipGrade: true), "the control: this fit is admissible")
        #expect(!Self.mayReplace(110, 150, skipGrade: false))
    }

    /// CONJUNCT 2 — UNCHANGED by this bead, restated here so the matrix is total
    /// rather than assuming the census tests cover it.
    @Test("the fidelity ladder still blocks a settled, foreign or anchored row")
    func fidelityLadderIsUntouched() {
        for blocked in [
            RescueFixture.window(id: "vetoed", start: 100, end: 160, decisionState: .reverted),
            RescueFixture.window(id: "dismissed", start: 100, end: 160, userDismissedBanner: true),
            RescueFixture.window(id: "skipped", start: 100, end: 160, wasSkipped: true),
            RescueFixture.window(id: "anchored", start: 100, end: 160, anchor: .rediffByteExact),
            RescueFixture.window(id: "foreign", start: 100, end: 160,
                                 boundaryState: AdBoundaryState.segmentAggregated.rawValue)
        ] {
            #expect(!Self.mayReplace(110, 150, overlapping: [blocked]),
                    "\(blocked.id) must still block the slot")
        }
    }

    /// CONJUNCT 3a — the hazard ug9m's comment named and `strict` never covered.
    @Test("a slot straddling TWO degraded rows retires neither")
    func fuseIsRefused() {
        let left = RescueFixture.window(id: "left", start: 100, end: 120)
        let right = RescueFixture.window(id: "right", start: 140, end: 160)
        #expect(!Self.mayReplace(100, 160, overlapping: [left, right]))
        #expect(Self.mayReplace(100, 120, overlapping: [left]),
                "the vacuity witness: one at a time is fine")
    }

    /// CONJUNCT 3b — the containment boundary, both edges, both directions, at
    /// the exact margin. The tolerances are read from `AutoSkipEdgePadding`
    /// rather than spelled as literals: they are the SAME numbers the cut uses,
    /// and that identity is the derivation.
    @Test("containment holds to the auto-skip margin and refuses one epsilon past it")
    func containmentBoundary() {
        let startTol = AutoSkipEdgePadding.startMarginRediffByteExactSeconds
        let endTol = AutoSkipEdgePadding.endMarginRediffByteExactSeconds

        #expect(Self.mayReplace(100 - startTol, 160), "exactly at the start margin")
        #expect(!Self.mayReplace(100 - startTol - 0.01, 160), "one hundredth past it")
        #expect(Self.mayReplace(100, 160 + endTol), "exactly at the end margin")
        #expect(!Self.mayReplace(100, 160 + endTol + 0.01))

        // SHRINKING IS ADMITTED ON PURPOSE — it cuts less than the row marked,
        // and the seconds it gives up were only ever bannered.
        #expect(Self.mayReplace(130, 140))
        // And sub-millisecond arm jitter, the measured +0.0002 s, is nowhere
        // near the boundary in either direction.
        #expect(Self.mayReplace(100 - 0.0002, 160 + 0.0002))
    }

    /// **THE THEOREM**, asserted rather than argued, over a sweep of every
    /// admitted geometry: the audio a superseding row actually CUTS is a subset
    /// of the span the row it retired had already marked as an ad. Computed
    /// through the production `AutoSkipEdgePadding.skipWindow`, so a margin
    /// change that broke the derivation fails here rather than on a device.
    @Test("every admitted supersede cuts only seconds the retired row already marked")
    func admittedSupersedeNeverCutsNewAudio() throws {
        var admitted = 0
        var refused = 0
        for startOffset in stride(from: -2.0, through: 2.0, by: 0.05) {
            for endOffset in stride(from: -2.0, through: 2.0, by: 0.05) {
                let slotStart = 100 + startOffset
                let slotEnd = 160 + endOffset
                guard slotEnd - slotStart > 0 else { continue }
                guard Self.mayReplace(slotStart, slotEnd) else { refused += 1; continue }
                admitted += 1
                guard let cut = AutoSkipEdgePadding.skipWindow(
                    spanStart: slotStart, spanEnd: slotEnd,
                    startAnchor: .rediffByteExact, endAnchor: .rediffByteExact
                ) else { continue }
                #expect(cut.start >= Self.row.startTime,
                        "cut start \(cut.start) escaped the retired row at \(Self.row.startTime)")
                #expect(cut.end <= Self.row.endTime,
                        "cut end \(cut.end) escaped the retired row at \(Self.row.endTime)")
            }
        }
        #expect(admitted > 0, "vacuity guard: the sweep really admitted geometries")
        #expect(refused > 0, "vacuity guard: and really refused some")
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

    // MARK: playhead-c7ef — the re-entry

    /// **THE RE-ENTRY RAIL, and the one that would have caught this bead being a
    /// no-op.** playhead-c7ef's filing says the ug9m rescue lane is "OPEN on all
    /// three Conan assets right now". It was not. Every day-0 attempt row on
    /// `db-pull10` (build 4f6bd5d3, 2026-08-13) reads `lastExit = marked`,
    /// `policyGeneration = 2`, `rescueAttemptCount = 0` — and the rescue branch
    /// requires a FOREIGN generation, so against a shipped
    /// `currentGeneration = 2` `decide` returned `.suppress(.marked)` and spent
    /// nothing. Widening the mint's supersede rule without moving the generation
    /// would have improved a code path no device could reach.
    ///
    /// So this pins the reachability directly, on the LITERAL the field carries
    /// rather than on `foreignGeneration` (which is defined as
    /// `currentGeneration - 1` and would keep passing through any bump, including
    /// a revert of this one).
    @Test("a generation-2 device record — the shape db-pull10 actually carries — is rescuable NOW")
    func fieldRecordsAreReachable() {
        let deviceRecord = RescueFixture.record(
            attemptCount: 1, lastAttemptAt: 0, lastExit: .marked,
            lastMarkCount: 3, generation: 2, rescueAttemptCount: 0
        )
        #expect(DayZeroRediffAttemptPolicy.decide(
            record: deviceRecord, markCensus: RescueFixture.degradedCensus, now: 10_000_000
        ) == .attempt(attemptNumber: 1),
        "generation 2 must be FOREIGN to this build — the bump is the whole re-entry")

        // The counter-witness, so the assertion above is about the generation
        // and not about the census or the exit: the SAME record stamped with
        // this build's generation stays terminal.
        #expect(DayZeroRediffAttemptPolicy.decide(
            record: RescueFixture.record(
                generation: DayZeroRediffAttemptPolicy.currentGeneration
            ),
            markCensus: RescueFixture.degradedCensus, now: 10_000_000
        ) == .suppress(reason: .marked, nextEligibleAt: nil),
        "`.marked` is NOT loosened — it stays terminal within a generation")
    }

    /// IDEMPOTENCE at the policy tier, driven from the field's own record shape
    /// rather than a synthetic one: the bump grants exactly ONE further fetch per
    /// asset, and every later play names `.rescueExhausted`. The bump re-opens a
    /// second chance; it does not open a tap.
    @Test("the c7ef bump buys a device asset exactly ONE rescue, then names itself")
    func deviceAssetSpendsExactlyOneRescue() {
        var record: RediffDayZeroAttemptRecord? = RescueFixture.record(
            attemptCount: 1, lastExit: .marked, lastMarkCount: 3,
            generation: 2, rescueAttemptCount: 0
        )
        var attempts = 0
        var spent = 0
        var reasons: [RediffDayZeroExit] = []
        var now: Double = 0
        for _ in 0..<8 {
            now += 30 * 24 * 60 * 60
            switch DayZeroRediffAttemptPolicy.decide(
                record: record, markCensus: RescueFixture.degradedCensus, now: now
            ) {
            case .attempt:
                attempts += 1
                spent += 51_931_606   // AA6CD430's measured `lastFullFetchBytes`
                record = DayZeroRediffAttemptPolicy.advance(
                    record: record, assetId: "AA6CD430",
                    // The pessimistic outcome: the fresh draw is recovered again
                    // and the marks stay wholly degraded. An unbounded policy
                    // would loop here forever.
                    outcome: RediffDayZeroMintOutcome(
                        markCount: 3, exit: .marked, strictMarkCount: 0,
                        segmentRecoveredSkipGradeMarkCount: 3, supersededMarkCount: 3
                    ),
                    fullFetchBytes: 51_931_606, at: now
                )
            case .suppress(let reason, _):
                reasons.append(reason)
            }
        }
        #expect(attempts == 1)
        #expect(spent == 51_931_606, "one asset, one fetch, one measured spend")
        #expect(record?.rescueAttemptCount == 1)
        #expect(record?.policyGeneration == DayZeroRediffAttemptPolicy.currentGeneration)
        #expect(record?.totalFullFetchBytes == 51_931_606,
                "and the spend is ACCOUNTABLE in the record, not only in a log line")
        #expect(!reasons.isEmpty, "vacuity guard: later plays really were evaluated")
        #expect(reasons.allSatisfy { $0 == .rescueExhausted },
                "got \(Set(reasons))")
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

    /// THE CENTREPIECE, **INVERTED IN PLACE BY playhead-c7ef** — it is the same
    /// fixture, the same seeding and the same vacuity control, asserting the
    /// opposite outcome, and it is left here rather than deleted so the reversal
    /// is one diff instead of a disappearance.
    ///
    /// WHAT IT USED TO ASSERT, and why that stopped being right. Under
    /// playhead-ug9m this read "a SEGMENT-RECOVERED re-mint supersedes nothing
    /// and promotes nothing": the mint's guard demanded `strict`, on the
    /// reasoning that a second draw which dropped runs is not evidence it is
    /// better than the first. playhead-pyq7 then measured the recovered arm and
    /// promoted it — a FRESH recovered mint stamps `.rediffByteExact` on both
    /// edges and auto-skips — which left the two halves contradicting each
    /// other: the same geometry, from the same arm, was skip-grade on a virgin
    /// asset and not even good enough to replace an `unanchored` banner on one
    /// that had been minted before. That is not a safety position, it is a
    /// leftover.
    ///
    /// WHAT REPLACES `strict` IS NOT NOTHING. The seeded rows here are each ONE
    /// row containing the slot that replaces it, which is exactly what
    /// `reMintMayReplace` requires; the fuse and the over-run are refused by
    /// their own tests below.
    @Test("a SEGMENT-RECOVERED re-mint supersedes its own degraded row and promotes it (playhead-c7ef)")
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

        #expect(outcome.exit == .marked,
                "a skip-grade recovered slot may now replace its own degraded row — got \(outcome.exit)")
        #expect(outcome.strictMarkCount == 0,
                "and it is STILL not strict — the counter names the acceptance ARM")
        #expect(outcome.supersededMarkCount == staleIds.count)

        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(rows.allSatisfy { !staleIds.contains($0.id) },
                "every seeded degraded row must be RETIRED, not left beside the new one")
        #expect(rows.count == controlRows.count,
                "one row out for one row in — got \(rows.count) for \(controlRows.count)")
        for row in rows {
            #expect(row.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue)
            #expect(row.endEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue)
            #expect(row.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                    "THE point of playhead-c7ef: the row Dan has been bannering can now auto-skip")
        }

        // THE THEOREM, asserted on the real rows rather than restated: every
        // second this rescue made auto-skippable was ALREADY marked as an ad by
        // the row it retired. Read off `AutoSkipEdgePadding` itself, so a margin
        // change that broke the derivation would fail here.
        let seeded = controlRows.map { (start: $0.startTime - 1, end: $0.endTime + 1) }
        for row in rows {
            let cut = try #require(AutoSkipEdgePadding.skipWindow(
                spanStart: row.startTime, spanEnd: row.endTime,
                startAnchor: .rediffByteExact, endAnchor: .rediffByteExact
            ), "an anchored row of this width must produce a skip window")
            let covering = seeded.first { $0.start <= row.startTime && $0.end >= row.endTime }
            let retired = try #require(covering, "each new row replaced one seeded row")
            #expect(cut.start >= retired.start && cut.end <= retired.end,
                    "the padded CUT \(cut) escaped the span the retired row had marked \(retired)")
        }
    }

    /// The slot geometry a fixture's mint produces, learned from a control store
    /// rather than hardcoded, so a fixture tweak cannot silently make a
    /// containment test vacuous.
    private func mintedGeometry(
        pairA: URL, b0: URL, b1: URL
    ) async throws -> [(start: Double, end: Double)] {
        let control = try await makeTestStore()
        try await insertAsset(store: control, assetId: "a1", sourceURL: pairA.absoluteString)
        let outcome = await makeService(store: control)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [b0, b1])
        try #require(outcome.exit == .marked, "fixture control: the pair really diverges")
        return try await control.fetchAdWindows(assetId: "a1")
            .sorted { $0.startTime < $1.startTime }
            .map { (start: $0.startTime, end: $0.endTime) }
    }

    /// playhead-c7ef, THE HAZARD ug9m's OWN COMMENT NAMED: *"a second, worse
    /// draw could retire two correct banners and mint one."* `strict` never
    /// addressed that — it is a statement about the acceptance arm, and a strict
    /// slot can straddle two marks as easily as a recovered one. The
    /// one-row clause does, and it is the reason widening the guard is not the
    /// same thing as removing it.
    ///
    /// Both banners survive and the wide slot is dropped, so the listener keeps
    /// two marks over real ads instead of one auto-skip over the show between
    /// them.
    @Test("a re-mint that would FUSE two degraded rows into one cut is refused")
    func fusedReMintIsRefused() async throws {
        let dir = try makeTempDir(prefix: "C7efFuse")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try StrictPair.stage(in: dir)
        let minted = try await mintedGeometry(pairA: pair.aURL, b0: pair.b0, b1: pair.b1)
        let slot = try #require(minted.first)
        try #require(minted.count == 1, "this fixture mints exactly one slot")
        try #require(slot.end - slot.start > 40, "and it is wide enough to hold two seeded rows")

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        // TWO degraded rows, both strictly INSIDE the one slot the re-mint finds
        // — so every other conjunct passes and only the fuse clause can refuse.
        let first = try await seedDegradedDayZeroRow(
            store: store, assetId: "a1", start: slot.start + 5, end: slot.start + 15
        )
        let second = try await seedDegradedDayZeroRow(
            store: store, assetId: "a1", start: slot.end - 15, end: slot.end - 5
        )

        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .allSlotsAlreadyCovered, "got \(outcome.exit)")
        #expect(outcome.supersededMarkCount == 0)
        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(Set(rows.map(\.id)) == Set([first, second]), "both banners survive")
        #expect(rows.allSatisfy { $0.eligibilityGate == SkipEligibilityGate.markOnly.rawValue },
                "and neither was promoted")
    }

    /// playhead-c7ef, THE OTHER DIRECTION. One row, every ladder conjunct
    /// satisfied — and the slot runs 10 s past the end of the span that row
    /// marked. Admitting it would auto-skip audio NOTHING had ever marked as an
    /// ad, which is precisely the inner-edge exposure the containment clause
    /// exists to refuse.
    @Test("a re-mint that runs PAST the row it would replace is refused")
    func overRunningReMintIsRefused() async throws {
        let dir = try makeTempDir(prefix: "C7efOverRun")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try StrictPair.stage(in: dir)
        let minted = try await mintedGeometry(pairA: pair.aURL, b0: pair.b0, b1: pair.b1)
        let slot = try #require(minted.first)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        // Starts EARLIER than the slot (so the start clause is satisfied) and
        // ends 10 s short of it — the slot escapes only at the end edge.
        let staleId = try await seedDegradedDayZeroRow(
            store: store, assetId: "a1", start: slot.start - 1, end: slot.end - 10
        )

        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .allSlotsAlreadyCovered, "got \(outcome.exit)")
        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(rows.map(\.id) == [staleId], "the banner survives, unpromoted")
        #expect(rows.first?.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)

        // THE VACUITY WITNESS: move the SAME seeded row's end just past the slot
        // and the identical mint supersedes it. Without this the refusal above
        // could be any of the other conjuncts.
        let permissive = try await makeTestStore()
        try await insertAsset(store: permissive, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        _ = try await seedDegradedDayZeroRow(
            store: permissive, assetId: "a1", start: slot.start - 1, end: slot.end + 1
        )
        let admitted = await makeService(store: permissive)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])
        #expect(admitted.exit == .marked, "got \(admitted.exit)")
        #expect(admitted.supersededMarkCount == 1)
    }

    /// playhead-c7ef, THE IDEMPOTENCE PROOF at the tier that could actually
    /// loop. The policy bounds how many times a rescue may FETCH; this bounds
    /// what a re-mint does if it runs again anyway — a widened supersede rule
    /// that kept replacing its own output would churn ids and retire rows
    /// forever, and no attempt counter would ever see it.
    ///
    /// It cannot, and the reason is structural rather than a counter: the row a
    /// successful re-mint leaves behind is `.rediffByteExact` on both edges, so
    /// `isSupersedable` is false for it and the second pass is refused by the
    /// UNCHANGED fidelity-ladder conjunct.
    @Test("running the rescue re-mint a second time is a NO-OP, to the row id")
    func reMintIsIdempotent() async throws {
        let dir = try makeTempDir(prefix: "C7efIdempotent")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try RecoveredPair.stage(in: dir)
        let minted = try await mintedGeometry(pairA: pair.aURL, b0: pair.b0, b1: pair.b1)
        try #require(!minted.isEmpty)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        for slot in minted {
            _ = try await seedDegradedDayZeroRow(
                store: store, assetId: "a1", start: slot.start - 1, end: slot.end + 1
            )
        }
        let service = makeService(store: store)

        let first = await service.mintByteExactDayZeroMarks(
            analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])
        #expect(first.exit == .marked, "got \(first.exit)")
        #expect(first.supersededMarkCount == minted.count)
        let afterFirst = try await store.fetchAdWindows(assetId: "a1")
            .sorted { $0.startTime < $1.startTime }

        let second = await service.mintByteExactDayZeroMarks(
            analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])
        #expect(second.exit == .allSlotsAlreadyCovered,
                "the second pass must supersede NOTHING — got \(second.exit)")
        #expect(second.supersededMarkCount == 0)

        let afterSecond = try await store.fetchAdWindows(assetId: "a1")
            .sorted { $0.startTime < $1.startTime }
        #expect(afterSecond.map(\.id) == afterFirst.map(\.id),
                "no row was retired and no id was churned")
        #expect(afterSecond.map(\.startTime) == afterFirst.map(\.startTime))
        #expect(afterSecond.map(\.endTime) == afterFirst.map(\.endTime))
        #expect(afterSecond.map(\.eligibilityGate) == afterFirst.map(\.eligibilityGate))
        #expect(afterSecond.map(\.startEdgeAnchor) == afterFirst.map(\.startEdgeAnchor))
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
            budgetSpendRecorder: { _, _ in },
            // playhead-3oyz: the retry lane is owned by DayZeroSameSessionRetryTests — opt out.
            retryClaimRecorder: { _ in }
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

    /// THE WIRING, end to end over a REAL store — the rail UG13 proved was
    /// missing.
    ///
    /// Every other trigger test here hands `attemptContextProvider` a
    /// hand-built `DayZeroAttemptContext`, so none of them can see the store
    /// read at all: a `fetchDayZeroAttemptContext` that returned an empty
    /// census would leave every one of them green while no trapped asset on any
    /// device was ever rescued. This one drives the SAME provider expression
    /// production uses, over rows and an attempt record that are actually on
    /// disk.
    @Test("the rescue decision reads the census from the STORE, not from a fixture")
    func rescueReadsTheCensusFromTheStore() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(AnalysisAsset(
            id: "a1", episodeId: "ep-a1", assetFingerprint: "fp-a1",
            weakFingerprint: nil, sourceURL: "file:///dev/null/a1.mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 280
        ))
        // The trapped shape, ON DISK: day-0 marked under an older generation,
        // and every one of its marks unanchored.
        try await store.upsertHotPathAdWindows(
            [RescueFixture.window(id: "w1"), RescueFixture.window(id: "w2", start: 400, end: 460)],
            existingIDs: [], retiredIDs: []
        )
        try await store.upsertRediffDayZeroAttempt(
            RescueFixture.record(generation: RescueFixture.foreignGeneration)
        )

        let fetcher = KWaySpyFullFetcher()
        let suppressions = SuppressionBox()
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
        let trigger = DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: 2,
            transportProvider: { .testWifi },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            // The PRODUCTION expression, verbatim.
            attemptContextProvider: { [store] assetId in
                (try? await store.fetchDayZeroAttemptContext(assetId: assetId)) ?? .never
            },
            suppressionRecorder: { assetId, reason, at in
                suppressions.recorded.append((assetId, reason, at))
            },
            mintedMarkDelivery: { _ in },
            budgetWindowProvider: { .empty },
            budgetSpendRecorder: { _, _ in },
            // playhead-3oyz: the retry lane is owned by DayZeroSameSessionRetryTests — opt out.
            retryClaimRecorder: { _ in }
        )
        await fire(trigger)
        #expect(!fetcher.calls.isEmpty,
                "the census the store computed is what unlocks the rescue")
        #expect(suppressions.recorded.isEmpty)

        // The discriminating witness: ANCHOR one of the same rows on disk and
        // the same store, the same record and the same provider must now refuse.
        // Without this, "it fetched" would be satisfied by a provider that
        // ignored the census entirely.
        let anchoredStore = try await makeTestStore()
        try await anchoredStore.insertAsset(AnalysisAsset(
            id: "a1", episodeId: "ep-a1", assetFingerprint: "fp-a1",
            weakFingerprint: nil, sourceURL: "file:///dev/null/a1.mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 280
        ))
        try await anchoredStore.upsertHotPathAdWindows(
            [RescueFixture.window(id: "w1", anchor: .rediffByteExact),
             RescueFixture.window(id: "w2", start: 400, end: 460)],
            existingIDs: [], retiredIDs: []
        )
        try await anchoredStore.upsertRediffDayZeroAttempt(
            RescueFixture.record(generation: RescueFixture.foreignGeneration)
        )
        let anchoredFetcher = KWaySpyFullFetcher()
        let anchoredSuppressions = SuppressionBox()
        let anchoredService = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: anchoredFetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: SpyDayZeroMinter(),
            now: { 0 }
        )
        await fire(DayZeroRediffTrigger(
            service: anchoredService,
            enabled: true,
            kWayFetchCount: 2,
            transportProvider: { .testWifi },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            attemptContextProvider: { [anchoredStore] assetId in
                (try? await anchoredStore.fetchDayZeroAttemptContext(assetId: assetId)) ?? .never
            },
            suppressionRecorder: { assetId, reason, at in
                anchoredSuppressions.recorded.append((assetId, reason, at))
            },
            mintedMarkDelivery: { _ in },
            budgetWindowProvider: { .empty },
            budgetSpendRecorder: { _, _ in },
            // playhead-3oyz: the retry lane is owned by DayZeroSameSessionRetryTests — opt out.
            retryClaimRecorder: { _ in }
        ))
        #expect(anchoredFetcher.calls.isEmpty,
                "one anchored row on disk must stop the same rescue")
        #expect(anchoredSuppressions.recorded.map(\.reason) == [.marked])
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
