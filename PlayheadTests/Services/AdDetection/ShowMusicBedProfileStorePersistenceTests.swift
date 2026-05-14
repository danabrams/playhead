// ShowMusicBedProfileStorePersistenceTests.swift
// playhead-2hpn (Plan §6 Phase 3 deliverable 4): persistence round-trip
// tests for `ShowMusicBedProfileStore`. Uses an in-memory `ModelContainer`
// so the suite never touches disk and runs alongside `PlayheadFastTests`.
//
// Coverage:
//   * `recordEpisodeOutcome` creates a fresh row when one does not exist.
//   * `recordEpisodeOutcome` updates the existing row in place across calls.
//   * `snapshot` returns nil for unknown shows; non-nil after first record.
//   * `allSnapshots` enumerates every persisted profile.
//   * Cross-show isolation: a record on Show A leaves Show B untouched.
//   * The hash bit-pattern round-trips losslessly through Int64 storage.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("ShowMusicBedProfileStore — persistence round-trip")
@MainActor
struct ShowMusicBedProfileStorePersistenceTests {

    private static func makeContainer() throws -> ModelContainer {
        let schema = Schema([ShowMusicBedProfile.self])
        let config = ModelConfiguration(
            "ShowMusicBedProfileStoreTests",
            schema: schema,
            isStoredInMemoryOnly: true,
            allowsSave: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    @Test("first record creates a row and snapshot reflects it")
    func firstRecordCreatesRow() async throws {
        let container = try Self.makeContainer()
        let store = ShowMusicBedProfileStore(modelContainer: container)

        #expect(await store.snapshot(showIdentifier: "show-A") == nil,
                "Unknown shows return nil snapshots")

        let hash = RepeatedAdFingerprint(bits: 0xDEAD_BEEF_DEAD_BEEF)
        let snapshot = await store.recordEpisodeOutcome(
            showIdentifier: "show-A",
            outcome: ShowMusicBedEpisodeOutcome(startHash: hash, endHash: .zero),
            now: Date()
        )

        #expect(snapshot.showIdentifier == "show-A")
        // First observation: no prior hashes → matched=false, but the
        // hash is recorded for the next comparison.
        #expect(snapshot.confirmedJingleHashes == [hash])
        #expect(snapshot.confirmationCount == 0)
        #expect(snapshot.consecutiveMissCount == 1,
                "First episode with no prior hashes counts as a miss")
        #expect(snapshot.versionStamp == ShowMusicBedProfile.currentVersionStamp)

        let reread = await store.snapshot(showIdentifier: "show-A")
        #expect(reread == snapshot)
    }

    @Test("subsequent records update the same row in place")
    func updatesExistingRow() async throws {
        let container = try Self.makeContainer()
        let store = ShowMusicBedProfileStore(modelContainer: container)

        let hash = RepeatedAdFingerprint(bits: 0xCAFE_BABE_CAFE_BABE)

        // Episode 1: seed.
        _ = await store.recordEpisodeOutcome(
            showIdentifier: "show-A",
            outcome: ShowMusicBedEpisodeOutcome(startHash: hash, endHash: .zero),
            now: Date()
        )

        // Episode 2: same hash → match, confirmationCount → 1.
        let snapshot2 = await store.recordEpisodeOutcome(
            showIdentifier: "show-A",
            outcome: ShowMusicBedEpisodeOutcome(startHash: hash, endHash: .zero),
            now: Date()
        )
        #expect(snapshot2.confirmationCount == 1)
        #expect(snapshot2.consecutiveMissCount == 0)
        #expect(snapshot2.confirmedJingleHashes == [hash])

        // The store should still have exactly ONE row for this show.
        let all = await store.allSnapshots()
        #expect(all.filter { $0.showIdentifier == "show-A" }.count == 1,
                "Successive recordEpisodeOutcome must update, not insert")
    }

    @Test("allSnapshots enumerates every stored profile")
    func allSnapshotsEnumeratesEveryProfile() async throws {
        let container = try Self.makeContainer()
        let store = ShowMusicBedProfileStore(modelContainer: container)

        _ = await store.recordEpisodeOutcome(
            showIdentifier: "show-A",
            outcome: ShowMusicBedEpisodeOutcome(
                startHash: RepeatedAdFingerprint(bits: 0xAAAA),
                endHash: .zero
            ),
            now: Date()
        )
        _ = await store.recordEpisodeOutcome(
            showIdentifier: "show-B",
            outcome: ShowMusicBedEpisodeOutcome(
                startHash: RepeatedAdFingerprint(bits: 0xBBBB),
                endHash: .zero
            ),
            now: Date()
        )
        let all = await store.allSnapshots()
        let ids = Set(all.map(\.showIdentifier))
        #expect(ids == ["show-A", "show-B"])
    }

    @Test("cross-show isolation — recording show A leaves show B untouched")
    func crossShowIsolation() async throws {
        let container = try Self.makeContainer()
        let store = ShowMusicBedProfileStore(modelContainer: container)

        let hashShared = RepeatedAdFingerprint(bits: 0xDEAD_BEEF_DEAD_BEEF)

        // Confirm show A with three matching episodes.
        for _ in 0..<4 {
            _ = await store.recordEpisodeOutcome(
                showIdentifier: "show-A",
                outcome: ShowMusicBedEpisodeOutcome(startHash: hashShared, endHash: .zero),
                now: Date()
            )
        }
        let snapshotA = await store.snapshot(showIdentifier: "show-A")
        #expect(snapshotA?.isConfirmed == true,
                "Show A should be confirmed after 3 matching episodes")

        // Show B has never been recorded. Its snapshot must remain nil
        // — show A's confirmation has not bled across the show boundary.
        let snapshotB = await store.snapshot(showIdentifier: "show-B")
        #expect(snapshotB == nil,
                "Show B must remain unobserved despite Show A's confirmation")
    }

    @Test("hash bit-pattern round-trips losslessly via Int64 storage")
    func hashBitPatternRoundTrips() async throws {
        let container = try Self.makeContainer()
        let store = ShowMusicBedProfileStore(modelContainer: container)

        // High bit set — verifies the `Int64(bitPattern:)` path preserves
        // the full UInt64 instead of clipping the sign bit.
        let highBit = RepeatedAdFingerprint(bits: 0x8000_0000_0000_0000)
        _ = await store.recordEpisodeOutcome(
            showIdentifier: "show-A",
            outcome: ShowMusicBedEpisodeOutcome(startHash: highBit, endHash: .zero),
            now: Date()
        )
        let snapshot = await store.snapshot(showIdentifier: "show-A")
        #expect(snapshot?.confirmedJingleHashes == [highBit],
                "High-bit-set UInt64 hashes must round-trip through Int64 column storage")
    }

    @Test("evictionThreshold consecutive misses resets the profile row")
    func evictionResetsRow() async throws {
        let container = try Self.makeContainer()
        let store = ShowMusicBedProfileStore(modelContainer: container)

        // Seed with one hash.
        let stored = RepeatedAdFingerprint(bits: 0xAAAA_AAAA_AAAA_AAAA)
        _ = await store.recordEpisodeOutcome(
            showIdentifier: "show-A",
            outcome: ShowMusicBedEpisodeOutcome(startHash: stored, endHash: .zero),
            now: Date()
        )

        // Drive `evictionThreshold - 1` more non-matching episodes
        // through (the seed already counts as miss #1, since it had no
        // prior hashes to match against). Total misses = 30 → eviction
        // triggers on the final mutation.
        //
        // We MUST use a different hash each time: when a non-matching
        // hash is first observed it gets recorded into the stored set,
        // so a SECOND occurrence of the same hash would then match (a
        // real accumulator behavior, not a bug). Synthetic distinct
        // hashes here keep the streak going.
        //
        // Generation: `(i+1) * golden-ratio-multiplier` scrambles small
        // ints into high-entropy 64-bit values. Pair-wise Hamming
        // distance between any two products is empirically ≥ 22 bits
        // (well above the 8-bit match threshold). The +1 offset keeps
        // i=0 from producing the all-zero sentinel.
        let goldenRatio: UInt64 = 0x9E37_79B9_7F4A_7C15
        let candidates: [RepeatedAdFingerprint] = (0..<(ShowMusicBedProfile.evictionThreshold - 1)).map {
            RepeatedAdFingerprint(bits: UInt64($0 + 1) &* goldenRatio)
        }
        // Belt-and-suspenders: walk every pair we're about to feed and
        // confirm distance > matchThreshold so the test fails loudly if
        // some future refactor of `evictionThreshold` ever drives `i`
        // into a pair that happens to collide.
        for a in candidates {
            #expect(stored.hammingDistance(to: a) > ShowMusicBedProfileEvaluator.matchThreshold,
                    "Each candidate must miss the seeded hash")
            for b in candidates where b.bits != a.bits {
                #expect(a.hammingDistance(to: b) > ShowMusicBedProfileEvaluator.matchThreshold,
                        "Candidates must be pair-wise distinct beyond matchThreshold")
            }
        }
        for candidate in candidates {
            _ = await store.recordEpisodeOutcome(
                showIdentifier: "show-A",
                outcome: ShowMusicBedEpisodeOutcome(startHash: candidate, endHash: .zero),
                now: Date()
            )
        }
        let snapshot = await store.snapshot(showIdentifier: "show-A")
        #expect(snapshot?.confirmedJingleHashes.isEmpty == true,
                "After evictionThreshold misses, stored hashes must be cleared")
        #expect(snapshot?.confirmationCount == 0)
        #expect(snapshot?.consecutiveMissCount == 0)
    }
}
