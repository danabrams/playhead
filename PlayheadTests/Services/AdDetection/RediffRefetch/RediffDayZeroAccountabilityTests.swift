// RediffDayZeroAccountabilityTests.swift
// playhead-p70f: the day-0 rediff path spent 299.6 MB on the owner's device and
// produced zero ad windows, zero state rows, and four outcome counters reading
// 0. These tests pin the two mechanisms that make that impossible to repeat:
//
//   1. Every day-0 exit is DISTINGUISHABLE (`RediffDayZeroExit`) and every
//      attempt leaves a durable record naming which one fired.
//   2. A repeat play does NOT re-spend the ~108 MB k-way fetch
//      (`DayZeroRediffAttemptPolicy`).
//
// Pure over `(record, now)` — no store, no clock, no network.

import Foundation
import Testing

@testable import Playhead

@Suite("Rediff day-0 accountability (playhead-p70f)")
struct RediffDayZeroAccountabilityTests {

    typealias Policy = DayZeroRediffAttemptPolicy
    typealias Exit = RediffDayZeroExit
    static let hour: Double = 3600
    static let day: Double = 24 * 3600

    private static func record(
        attempts: Int,
        at lastAttemptAt: Double,
        exit: Exit,
        totalBytes: Int = 0
    ) -> RediffDayZeroAttemptRecord {
        RediffDayZeroAttemptRecord(
            analysisAssetId: "asset-1",
            attemptCount: attempts,
            lastAttemptAt: lastAttemptAt,
            lastExit: exit,
            totalFullFetchBytes: totalBytes
        )
    }

    // MARK: - Exit taxonomy

    @Test("every formerly silent mint exit has its own raw value")
    func exitRawValuesAreDistinct() {
        let raws = Exit.allCases.map(\.rawValue)
        #expect(Set(raws).count == raws.count, "raw values are persisted — collisions would merge two exits in the DB")
        // The exits the p70f trace could not tell apart. If any two of these
        // ever collapse to one case again, this fails.
        let indistinguishableBefore: [Exit] = [
            .assetRowMissing, .assetFetchFailed, .aSideNotAnchored, .aSideReadFailed,
            .fetchFailed, .tooFewBCopies, .noAcceptedByteDiff, .noDivergentSlot,
            .allSlotsAlreadyCovered, .persistFailed
        ]
        #expect(Set(indistinguishableBefore).count == indistinguishableBefore.count)
    }

    @Test("the AAC-behind-an-.mp3-suffix hypothesis is distinguishable from clean-but-identical copies")
    func gateRejectVersusNoDivergence() {
        // If RediffByteAligner (an MP3-frame parser) finds no frames because the
        // bytes are AAC, EVERY B-copy gate-rejects.
        let allRejected = RediffDayZeroMintOutcome(
            exit: .noAcceptedByteDiff, bSideCount: 2, bSidesAccepted: 0,
            bSidesGateRejected: 2, bSidesUnreadable: 0, divergentSlotCount: 0
        )
        // If the bytes ARE MP3 and the CDN simply served identical copies, the
        // diffs are accepted and the union is empty.
        let diffedButIdentical = RediffDayZeroMintOutcome(
            exit: .noDivergentSlot, bSideCount: 2, bSidesAccepted: 2,
            bSidesGateRejected: 0, bSidesUnreadable: 0, divergentSlotCount: 0
        )
        #expect(allRejected.exit != diffedButIdentical.exit)
        #expect(allRejected.bSidesGateRejected == allRejected.bSideCount)
        #expect(diffedButIdentical.bSidesAccepted == diffedButIdentical.bSideCount)
    }

    @Test("pre-fetch exits are marked free; post-fetch exits are marked as having spent bandwidth")
    func spentBandwidthClassification() {
        for exit in [Exit.minterUnavailable, .assetRowMissing, .assetFetchFailed,
                     .aSideNotAnchored, .aSideReadFailed, .suppressedByBackoff, .alreadyInFlight] {
            #expect(exit.spentBandwidth == false, "\(exit.rawValue) must be reachable before the ~108 MB fetch")
        }
        for exit in [Exit.fetchFailed, .tooFewBCopies, .noAcceptedByteDiff,
                     .noDivergentSlot, .allSlotsAlreadyCovered, .persistFailed, .marked] {
            #expect(exit.spentBandwidth, "\(exit.rawValue) only happens after the fetch")
        }
    }

    // MARK: - Idempotency: the replay bleed

    @Test("a first attempt is always allowed")
    func firstAttemptAllowed() {
        #expect(Policy.decide(record: nil, now: 1_000) == .attempt(attemptNumber: 1))
    }

    @Test("REPLAY BLEED: replaying the same episode inside the backoff window spends nothing")
    func replayWithinBackoffIsSuppressed() {
        // The exact device scenario: one day-0 run diffed cleanly and found no
        // divergence, then the user replayed the episode.
        let after = Self.record(attempts: 1, at: 1_000, exit: .noDivergentSlot, totalBytes: 108_000_000)
        // Minutes later — a replay in the same listening session.
        let decision = Policy.decide(record: after, now: 1_000 + 10 * Self.hour)
        #expect(decision == .suppress(reason: .suppressedByBackoff, nextEligibleAt: 1_000 + Self.day))
        #expect(decision.isAttempt == false, "a replay must not re-spend ~108 MB")
    }

    @Test("after the backoff elapses the next attempt is allowed and numbered")
    func attemptAllowedAfterBackoff() {
        let after = Self.record(attempts: 1, at: 1_000, exit: .noDivergentSlot)
        #expect(Policy.decide(record: after, now: 1_000 + Self.day) == .attempt(attemptNumber: 2))
        #expect(Policy.decide(record: after, now: 1_000 + Self.day - 1).isAttempt == false)
    }

    @Test("backoff escalates 24h → 48h → 96h and is capped at 7 days")
    func backoffEscalates() {
        #expect(Policy.backoff(afterAttempts: 0) == 0)
        #expect(Policy.backoff(afterAttempts: 1) == Self.day)
        #expect(Policy.backoff(afterAttempts: 2) == 2 * Self.day)
        #expect(Policy.backoff(afterAttempts: 3) == 4 * Self.day)
        #expect(Policy.backoff(afterAttempts: 10) == Policy.maxBackoff)
        // A corrupted/absurd count must not trap on the shift.
        #expect(Policy.backoff(afterAttempts: Int.max) == Policy.maxBackoff)
    }

    @Test("the attempt budget is a hard cap — never eligible again once spent")
    func attemptBudgetIsTerminal() {
        let spent = Self.record(attempts: Policy.maxAttempts, at: 1_000, exit: .noDivergentSlot)
        let decision = Policy.decide(record: spent, now: 1_000 + 365 * Self.day)
        #expect(decision == .suppress(reason: .suppressedByBackoff, nextEligibleAt: nil))
    }

    @Test("a SUCCESSFUL day-0 never re-fetches, no matter how much time passes")
    func markedIsPermanentlyTerminal() {
        let marked = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset-1", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .marked, lastMarkCount: 3
        )
        #expect(Policy.decide(record: marked, now: 1_000 + 365 * Self.day)
                == .suppress(reason: .marked, nextEligibleAt: nil))
    }

    // MARK: - Record folding

    @Test("advance accumulates attempts AND cumulative bytes so per-asset bleed is visible in one row")
    func advanceAccumulates() {
        let first = Policy.advance(
            record: nil, assetId: "a",
            outcome: RediffDayZeroMintOutcome(exit: .noDivergentSlot, bSideCount: 2, bSidesAccepted: 2),
            fullFetchBytes: 108_000_000, at: 100
        )
        #expect(first.attemptCount == 1)
        #expect(first.totalFullFetchBytes == 108_000_000)
        #expect(first.lastExit == .noDivergentSlot)
        #expect(first.lastBSidesAccepted == 2)

        let second = Policy.advance(
            record: first, assetId: "a",
            outcome: RediffDayZeroMintOutcome(exit: .noAcceptedByteDiff, bSideCount: 2, bSidesGateRejected: 2),
            fullFetchBytes: 108_000_000, at: 200
        )
        #expect(second.attemptCount == 2)
        #expect(second.totalFullFetchBytes == 216_000_000, "cumulative bytes are the bleed signal")
        #expect(second.lastFullFetchBytes == 108_000_000)
        #expect(second.lastExit == .noAcceptedByteDiff)
        #expect(second.lastAttemptAt == 200)
    }

    @Test("a suppressed attempt records ZERO bytes")
    func suppressedAttemptRecordsNoBytes() {
        let prior = Self.record(attempts: 1, at: 100, exit: .noDivergentSlot, totalBytes: 108_000_000)
        let folded = Policy.advance(
            record: prior, assetId: "a",
            outcome: .blocked(.suppressedByBackoff), fullFetchBytes: 0, at: 200
        )
        #expect(folded.totalFullFetchBytes == 108_000_000, "suppression must not add bytes")
        #expect(folded.lastFullFetchBytes == 0)
    }

    @Test("persisted detail is truncated — the database is not a log")
    func detailIsTruncated() {
        let huge = String(repeating: "x", count: 5_000)
        let folded = Policy.advance(
            record: nil, assetId: "a",
            outcome: RediffDayZeroMintOutcome(exit: .fetchFailed, detail: huge),
            fullFetchBytes: 0, at: 1
        )
        #expect(folded.lastDetail?.count == Policy.detailCharCap)
    }
}
