// ShowMusicBedProfileEvaluatorTests.swift
// playhead-2hpn (Plan §6 Phase 3 deliverable 4): pure-evaluator tests
// for the scoped-music-bed-generalization feature. These exercise the
// in-memory state transitions (`apply(...)`) and the slice hashing
// (`extractEpisodeJingleHashes(...)`) without touching SwiftData.
//
// The store-side persistence round-trip is covered by
// `ShowMusicBedProfileStorePersistenceTests`.

import Foundation
import Testing

@testable import Playhead

@Suite("ShowMusicBedProfileEvaluator")
struct ShowMusicBedProfileEvaluatorTests {

    // MARK: - Match / no-match rule

    @Test("match advances confirmationCount and zeroes missCount")
    func matchAdvancesConfirmation() {
        // Episode #2: previously stored one hash, this episode produces
        // an exact-match start hash → confirmationCount goes 1 → 2,
        // consecutiveMissCount resets to 0.
        let stored = RepeatedAdFingerprint(bits: 0xDEAD_BEEF_DEAD_BEEF)
        let outcome = ShowMusicBedEpisodeOutcome(
            startHash: stored, // exact match
            endHash: .zero
        )
        let mutation = ShowMusicBedProfileEvaluator.apply(
            outcome: outcome,
            toShowIdentifier: "show-A",
            confirmedHashes: [stored],
            confirmationCount: 1,
            consecutiveMissCount: 5 // simulate prior misses to verify reset
        )
        #expect(mutation.matched == true)
        #expect(mutation.confirmationCount == 2)
        #expect(mutation.consecutiveMissCount == 0)
        #expect(mutation.confirmedHashes == [stored],
                "Existing hash should be preserved unchanged on a match")
    }

    @Test("Hamming distance just under threshold counts as a match")
    func nearMatchAtThresholdCounts() {
        // Stored hash with all bits 0; outcome flips exactly 8 bits
        // (the matchThreshold). That should still match.
        let stored = RepeatedAdFingerprint(bits: 0)
        // 8 bits set: 0x00000000_000000FF
        let candidate = RepeatedAdFingerprint(bits: 0xFF)
        #expect(stored.hammingDistance(to: candidate) == ShowMusicBedProfileEvaluator.matchThreshold)

        let mutation = ShowMusicBedProfileEvaluator.apply(
            outcome: ShowMusicBedEpisodeOutcome(startHash: candidate, endHash: .zero),
            toShowIdentifier: "show-A",
            confirmedHashes: [stored],
            confirmationCount: 0,
            consecutiveMissCount: 0
        )
        #expect(mutation.matched == true,
                "Hamming = matchThreshold (≤) should still match")
    }

    @Test("Hamming distance just over threshold is not a match")
    func justOverThresholdNotMatch() {
        let stored = RepeatedAdFingerprint(bits: 0)
        // 9 bits set: one more than the 8-bit threshold.
        let candidate = RepeatedAdFingerprint(bits: 0x1FF)
        #expect(stored.hammingDistance(to: candidate) == ShowMusicBedProfileEvaluator.matchThreshold + 1)

        let mutation = ShowMusicBedProfileEvaluator.apply(
            outcome: ShowMusicBedEpisodeOutcome(startHash: candidate, endHash: .zero),
            toShowIdentifier: "show-A",
            confirmedHashes: [stored],
            confirmationCount: 0,
            consecutiveMissCount: 0
        )
        #expect(mutation.matched == false)
        #expect(mutation.consecutiveMissCount == 1)
    }

    @Test("zero hashes never match — even against another zero")
    func zeroHashesNeverCollide() {
        // The sentinel `.zero` hash represents "no derivable signal".
        // If we let it match every other `.zero`, every empty-edge
        // episode would falsely confirm every empty-edge stored hash.
        let mutation = ShowMusicBedProfileEvaluator.apply(
            outcome: ShowMusicBedEpisodeOutcome(startHash: .zero, endHash: .zero),
            toShowIdentifier: "show-A",
            confirmedHashes: [.zero],
            confirmationCount: 1,
            consecutiveMissCount: 0
        )
        #expect(mutation.matched == false)
        #expect(mutation.consecutiveMissCount == 1)
    }

    // MARK: - Confirmation threshold (3-episode rule)

    @Test("three consecutive matches reach the confirmation threshold")
    func threeEpisodeConfirmation() {
        let hash = RepeatedAdFingerprint(bits: 0xCAFE_BABE_CAFE_BABE)

        var hashes: [RepeatedAdFingerprint] = []
        var confirmations = 0
        var misses = 0
        let outcome = ShowMusicBedEpisodeOutcome(startHash: hash, endHash: .zero)

        // Episode #1: first observation. No previous hashes → no match,
        // but the hash is recorded for future comparison.
        let m1 = ShowMusicBedProfileEvaluator.apply(
            outcome: outcome,
            toShowIdentifier: "show-A",
            confirmedHashes: hashes,
            confirmationCount: confirmations,
            consecutiveMissCount: misses
        )
        #expect(m1.matched == false, "Episode 1 has no prior hashes to match against")
        #expect(m1.confirmedHashes == [hash])
        hashes = m1.confirmedHashes; confirmations = m1.confirmationCount; misses = m1.consecutiveMissCount

        // Episode #2: matches → confirmationCount = 1.
        let m2 = ShowMusicBedProfileEvaluator.apply(
            outcome: outcome,
            toShowIdentifier: "show-A",
            confirmedHashes: hashes,
            confirmationCount: confirmations,
            consecutiveMissCount: misses
        )
        #expect(m2.matched == true)
        #expect(m2.confirmationCount == 1)
        hashes = m2.confirmedHashes; confirmations = m2.confirmationCount; misses = m2.consecutiveMissCount

        // Episode #3: matches → confirmationCount = 2.
        let m3 = ShowMusicBedProfileEvaluator.apply(
            outcome: outcome,
            toShowIdentifier: "show-A",
            confirmedHashes: hashes,
            confirmationCount: confirmations,
            consecutiveMissCount: misses
        )
        #expect(m3.matched == true)
        #expect(m3.confirmationCount == 2)
        hashes = m3.confirmedHashes; confirmations = m3.confirmationCount; misses = m3.consecutiveMissCount

        // Episode #4: matches → confirmationCount = 3 (now confirmed).
        let m4 = ShowMusicBedProfileEvaluator.apply(
            outcome: outcome,
            toShowIdentifier: "show-A",
            confirmedHashes: hashes,
            confirmationCount: confirmations,
            consecutiveMissCount: misses
        )
        #expect(m4.confirmationCount == ShowMusicBedProfile.confirmationThreshold)
        // Snapshot-equivalent guard: a profile derived from this mutation
        // would satisfy `isConfirmed` (count ≥ 3 AND hashes non-empty).
        #expect(m4.confirmationCount >= ShowMusicBedProfile.confirmationThreshold)
        #expect(!m4.confirmedHashes.isEmpty)
    }

    // MARK: - Eviction (30-consecutive-miss rule)

    @Test("30 consecutive misses reset the profile")
    func thirtyMissEviction() {
        // Seed a profile with one hash and a 29-miss streak; next miss
        // should evict.
        let stored = RepeatedAdFingerprint(bits: 0xAAAA_AAAA_AAAA_AAAA)
        let nonMatching = RepeatedAdFingerprint(bits: 0x5555_5555_5555_5555)
        #expect(stored.hammingDistance(to: nonMatching) == 64,
                "These two patterns must differ in all 64 bits to guarantee a miss")

        let mutation = ShowMusicBedProfileEvaluator.apply(
            outcome: ShowMusicBedEpisodeOutcome(startHash: nonMatching, endHash: .zero),
            toShowIdentifier: "show-A",
            confirmedHashes: [stored],
            confirmationCount: 5,
            consecutiveMissCount: ShowMusicBedProfile.evictionThreshold - 1
        )
        #expect(mutation.matched == false)
        #expect(mutation.confirmedHashes.isEmpty,
                "Eviction must clear stored hashes")
        #expect(mutation.confirmationCount == 0)
        #expect(mutation.consecutiveMissCount == 0,
                "Eviction must reset miss count so the next observation starts fresh")
    }

    @Test("miss before eviction threshold accumulates without reset")
    func missBelowThresholdJustAccumulates() {
        let stored = RepeatedAdFingerprint(bits: 0xAAAA_AAAA_AAAA_AAAA)
        let nonMatching = RepeatedAdFingerprint(bits: 0x5555_5555_5555_5555)

        let mutation = ShowMusicBedProfileEvaluator.apply(
            outcome: ShowMusicBedEpisodeOutcome(startHash: nonMatching, endHash: .zero),
            toShowIdentifier: "show-A",
            confirmedHashes: [stored],
            confirmationCount: 5,
            consecutiveMissCount: 10
        )
        #expect(mutation.matched == false)
        #expect(mutation.confirmedHashes.contains(stored),
                "Existing hash must remain present")
        // Non-matching hash is also recorded (FIFO-bounded) so the next
        // episode has both to compare against — the bead-spec accumulation
        // path.
        #expect(mutation.confirmedHashes.contains(nonMatching))
        #expect(mutation.confirmationCount == 5)
        #expect(mutation.consecutiveMissCount == 11)
    }

    // MARK: - FIFO eviction at maxStoredHashes

    @Test("stored hashes are FIFO-capped at maxStoredHashes")
    func storedHashesFifoCapped() {
        // Seed with maxStoredHashes distinct hashes that each differ
        // from every other by well above the 8-bit match threshold.
        // We use a golden-ratio multiplier to scramble small ints into
        // high-entropy 64-bit values — pair-wise distances are
        // empirically ≥ 22 bits.
        let max = ShowMusicBedProfile.maxStoredHashes
        let goldenRatio: UInt64 = 0x9E37_79B9_7F4A_7C15
        let seeded: [RepeatedAdFingerprint] = (0..<max).map {
            RepeatedAdFingerprint(bits: UInt64($0 + 1) &* goldenRatio)
        }
        // Self-check: every pair must be beyond the match threshold so
        // the de-dup path treats them as distinct.
        for (i, a) in seeded.enumerated() {
            for b in seeded.dropFirst(i + 1) {
                #expect(a.hammingDistance(to: b) > ShowMusicBedProfileEvaluator.matchThreshold,
                        "Seeded hashes must be pair-wise distinct beyond matchThreshold")
            }
        }
        // The newcomer must also be distant from every seeded entry.
        let newcomer = RepeatedAdFingerprint(bits: UInt64(max + 1) &* goldenRatio)
        for s in seeded {
            #expect(s.hammingDistance(to: newcomer) > ShowMusicBedProfileEvaluator.matchThreshold,
                    "Newcomer must be distinct from every seeded hash")
        }

        let mutation = ShowMusicBedProfileEvaluator.apply(
            outcome: ShowMusicBedEpisodeOutcome(startHash: newcomer, endHash: .zero),
            toShowIdentifier: "show-A",
            confirmedHashes: seeded,
            confirmationCount: 0,
            consecutiveMissCount: 0
        )
        #expect(mutation.confirmedHashes.count == max,
                "Should still hold maxStoredHashes after FIFO eviction")
        #expect(mutation.confirmedHashes.last == newcomer,
                "Newcomer should be appended at the end")
        #expect(mutation.confirmedHashes.first == seeded[1],
                "Oldest entry (seeded[0]) should have been FIFO-evicted")
    }

    @Test("near-duplicate candidate is not re-recorded")
    func nearDuplicateNotReadded() {
        let stored = RepeatedAdFingerprint(bits: 0)
        let nearDup = RepeatedAdFingerprint(bits: 0x07) // 3 bits set, ≤ 8
        let mutation = ShowMusicBedProfileEvaluator.apply(
            outcome: ShowMusicBedEpisodeOutcome(startHash: nearDup, endHash: .zero),
            toShowIdentifier: "show-A",
            confirmedHashes: [stored],
            confirmationCount: 0,
            consecutiveMissCount: 0
        )
        #expect(mutation.matched == true)
        #expect(mutation.confirmedHashes.count == 1,
                "Near-duplicate hash should not bloat the stored set")
    }

    // MARK: - Slice hashing

    @Test("extractEpisodeJingleHashes handles short episodes safely")
    func shortEpisodeSuppressesEndHash() {
        // Episode shorter than 2 * jingleSliceSeconds (i.e. < 20 s) →
        // end hash must be `.zero` so it does not double-count audio
        // already covered by the start slice.
        let windows = [
            featureWindow(start: 0, end: 2),
            featureWindow(start: 2, end: 4),
            featureWindow(start: 4, end: 6),
        ]
        let outcome = ShowMusicBedProfileEvaluator.extractEpisodeJingleHashes(
            featureWindows: windows,
            episodeDuration: 15.0
        )
        #expect(outcome.endHash.isZero,
                "Short episodes must not emit an end-slice hash")
    }

    @Test("extractEpisodeJingleHashes returns deterministic hashes")
    func slicesAreDeterministic() {
        let windows = (0..<30).map {
            featureWindow(start: Double($0) * 2.0, end: Double($0) * 2.0 + 2.0)
        }
        let outcome1 = ShowMusicBedProfileEvaluator.extractEpisodeJingleHashes(
            featureWindows: windows,
            episodeDuration: 60.0
        )
        let outcome2 = ShowMusicBedProfileEvaluator.extractEpisodeJingleHashes(
            featureWindows: windows,
            episodeDuration: 60.0
        )
        #expect(outcome1 == outcome2,
                "Same input must produce byte-identical hashes")
    }

    @Test("extractEpisodeJingleHashes on empty windows returns zero pair")
    func emptyWindowsZeroOutput() {
        let outcome = ShowMusicBedProfileEvaluator.extractEpisodeJingleHashes(
            featureWindows: [],
            episodeDuration: 60.0
        )
        #expect(outcome.startHash.isZero)
        #expect(outcome.endHash.isZero)
    }

    // MARK: - Cross-show isolation

    @Test("cross-show isolation: showIdentifier does not bleed across apply calls")
    func crossShowIsolation() {
        // Show A is confirmed (3 matches with hash X). Show B has never
        // observed hash X. Applying Show B's outcome with hash X must
        // be evaluated against Show B's (empty) stored set — the
        // evaluator must NEVER reach into another show's state. This
        // test simulates the rule by passing Show B's empty stored
        // hashes; the evaluator must return matched=false.
        let storedForA = RepeatedAdFingerprint(bits: 0xDEAD_BEEF_DEAD_BEEF)
        let outcomeForB = ShowMusicBedEpisodeOutcome(
            startHash: storedForA, // same bytes — would match if we used Show A's state
            endHash: .zero
        )
        let mutationForB = ShowMusicBedProfileEvaluator.apply(
            outcome: outcomeForB,
            toShowIdentifier: "show-B",
            confirmedHashes: [], // Show B has never observed this hash
            confirmationCount: 0,
            consecutiveMissCount: 0
        )
        #expect(mutationForB.matched == false,
                "Show B must not match a hash only Show A has stored")
        #expect(mutationForB.confirmationCount == 0)
    }

    // MARK: - Helpers

    private func featureWindow(start: Double, end: Double) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test",
            startTime: start,
            endTime: end,
            rms: 0.1,
            spectralFlux: 0.05,
            musicProbability: 0.6,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .background,
            pauseProbability: 0,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 4
        )
    }
}
