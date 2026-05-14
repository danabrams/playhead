// ShowCapabilityProfileStorePersistenceTests.swift
// playhead-h6a6: persistence round-trip tests for
// `ShowCapabilityProfileStore`. Uses an in-memory `ModelContainer` so
// the suite never touches disk and runs alongside `PlayheadFastTests`.
//
// Coverage:
//   * `recordEpisodeOutcome` creates a fresh row on first call.
//   * Subsequent records update the same row in place.
//   * `snapshot` returns nil for unknown shows; non-nil after first record.
//   * Cross-show isolation: a record on Show A leaves Show B untouched.
//   * `allSnapshots` enumerates every persisted profile.
//   * Music-bed-reliable flows from the 2hpn `musicBedConfirmed` flag.
//   * The activation floor + SLI gate are honored across the round-trip.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("ShowCapabilityProfileStore — persistence round-trip")
@MainActor
struct ShowCapabilityProfileStorePersistenceTests {

    private static func makeContainer() throws -> ModelContainer {
        let schema = Schema([ShowCapabilityProfile.self])
        let config = ModelConfiguration(
            "ShowCapabilityProfileStoreTests",
            schema: schema,
            isStoredInMemoryOnly: true,
            allowsSave: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    /// Open gate — every show's cohort is "in bounds". Tests pass
    /// this when they want to exercise the post-floor path.
    private static let openGate: ShowCapabilitySLIGate = { _ in true }

    @Test("first record creates a row and snapshot reflects it")
    func firstRecordCreatesRow() async throws {
        let container = try Self.makeContainer()
        let store = ShowCapabilityProfileStore(modelContainer: container)

        #expect(await store.snapshot(showIdentifier: "show-A") == nil,
                "Unknown shows return nil snapshots")

        let snap = await store.recordEpisodeOutcome(
            showIdentifier: "show-A",
            outcome: .nothingObserved,
            musicBedConfirmed: false,
            sliGate: Self.openGate,
            now: Date()
        )

        #expect(snap.showIdentifier == "show-A")
        #expect(snap.completedEpisodeCount == 1)
        #expect(snap.kind == .unknown, "1 episode is below the activation floor")
        #expect(snap.schemaVersion == ShowCapabilityProfile.currentSchemaVersion)
        #expect(!snap.isObserved)

        let reread = await store.snapshot(showIdentifier: "show-A")
        #expect(reread == snap)
    }

    @Test("subsequent records update the same row in place")
    func updatesExistingRow() async throws {
        let container = try Self.makeContainer()
        let store = ShowCapabilityProfileStore(modelContainer: container)

        for _ in 0..<5 {
            _ = await store.recordEpisodeOutcome(
                showIdentifier: "show-A",
                outcome: ShowCapabilityEpisodeOutcome(
                    chapterMatched: true,
                    hostVoiced: false,
                    sponsorDeclared: false,
                    dynamicInsertionShift: false
                ),
                musicBedConfirmed: false,
                sliGate: Self.openGate,
                now: Date()
            )
        }

        let snap = try #require(await store.snapshot(showIdentifier: "show-A"))
        #expect(snap.completedEpisodeCount == 5)
        #expect(snap.chapterMatchedEpisodeCount == 5)
        #expect(snap.kind == .chapterRich,
                "5/5 chapter-matched episodes at floor count should be chapter-rich")
        #expect(snap.isObserved)
    }

    @Test("cross-show isolation: writes to A do not touch B")
    func crossShowIsolation() async throws {
        let container = try Self.makeContainer()
        let store = ShowCapabilityProfileStore(modelContainer: container)

        // 5 host-voiced episodes for A; 1 nothing for B.
        for _ in 0..<5 {
            _ = await store.recordEpisodeOutcome(
                showIdentifier: "show-A",
                outcome: ShowCapabilityEpisodeOutcome(
                    chapterMatched: false,
                    hostVoiced: true,
                    sponsorDeclared: false,
                    dynamicInsertionShift: false
                ),
                musicBedConfirmed: false,
                sliGate: Self.openGate,
                now: Date()
            )
        }
        _ = await store.recordEpisodeOutcome(
            showIdentifier: "show-B",
            outcome: .nothingObserved,
            musicBedConfirmed: false,
            sliGate: Self.openGate,
            now: Date()
        )

        let a = try #require(await store.snapshot(showIdentifier: "show-A"))
        let b = try #require(await store.snapshot(showIdentifier: "show-B"))
        #expect(a.completedEpisodeCount == 5)
        #expect(a.kind == .hostReadOnly)
        #expect(b.completedEpisodeCount == 1)
        #expect(b.kind == .unknown)
    }

    @Test("allSnapshots enumerates every persisted profile")
    func allSnapshotsEnumeratesAll() async throws {
        let container = try Self.makeContainer()
        let store = ShowCapabilityProfileStore(modelContainer: container)

        for show in ["show-A", "show-B", "show-C"] {
            _ = await store.recordEpisodeOutcome(
                showIdentifier: show,
                outcome: .nothingObserved,
                musicBedConfirmed: false,
                sliGate: Self.openGate,
                now: Date()
            )
        }

        let all = await store.allSnapshots()
        #expect(Set(all.map(\.showIdentifier)) == ["show-A", "show-B", "show-C"])
    }

    @Test("music-bed-reliable kind flows from the 2hpn confirmed flag")
    func musicBedReliableFromTwoHpnFlag() async throws {
        let container = try Self.makeContainer()
        let store = ShowCapabilityProfileStore(modelContainer: container)

        // 5 nothing-observed episodes, but the 2hpn signal is confirmed.
        // The activation floor is met (5 episodes). The SLI gate is
        // open. The kind should transition to .musicBedReliable on
        // the 5th record.
        var lastKind: ShowCapabilityProfileKind = .unknown
        for i in 0..<5 {
            // Only confirm on the final episode so we can also
            // observe the transition.
            let confirmed = (i == 4)
            let snap = await store.recordEpisodeOutcome(
                showIdentifier: "show-jingle",
                outcome: .nothingObserved,
                musicBedConfirmed: confirmed,
                sliGate: Self.openGate,
                now: Date()
            )
            lastKind = snap.kind
        }
        #expect(lastKind == .musicBedReliable)
    }

    @Test("closed SLI gate keeps the kind at unknown even past the floor")
    func closedSLIGateBlocksObservation() async throws {
        let container = try Self.makeContainer()
        let store = ShowCapabilityProfileStore(modelContainer: container)

        // 10 chapter-matched episodes, but the SLI gate is closed.
        for _ in 0..<10 {
            _ = await store.recordEpisodeOutcome(
                showIdentifier: "show-A",
                outcome: ShowCapabilityEpisodeOutcome(
                    chapterMatched: true,
                    hostVoiced: false,
                    sponsorDeclared: false,
                    dynamicInsertionShift: false
                ),
                musicBedConfirmed: false,
                sliGate: { _ in false },
                now: Date()
            )
        }

        let snap = try #require(await store.snapshot(showIdentifier: "show-A"))
        #expect(snap.completedEpisodeCount == 10)
        #expect(snap.kind == .unknown,
                "Closed SLI gate must pin the kind at .unknown")
    }
}
