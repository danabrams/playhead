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

    // MARK: - h6a6 R10: persistence-format pinning + every-kind round-trip

    @Test("ShowCapabilityProfileKind raw values are the on-disk format — pin verbatim")
    func kindRawValuesArePinned() {
        // h6a6 R10 review gap: the `String` rawValue of each
        // `ShowCapabilityProfileKind` case IS the persisted on-disk
        // format. `ShowCapabilityProfile.kindRawValue` stores it, and
        // `ShowCapabilityProfileStore.snapshot(from:)` reads it back via
        // `ShowCapabilityProfileKind(rawValue:)`. A refactor that
        // accidentally drops the explicit raw strings (e.g. `case
        // chapterRich = "chapter-rich"` → `case chapterRich`) would
        // silently flip every persisted row's decoded kind to
        // `.unknown` (the `?? .unknown` corruption-fallback in the
        // store). Pinning verbatim so the schema invariant is a loud
        // test failure rather than a silent data loss for every user
        // whose backfill has run.
        #expect(ShowCapabilityProfileKind.unknown.rawValue == "unknown")
        #expect(ShowCapabilityProfileKind.chapterRich.rawValue == "chapter-rich")
        #expect(ShowCapabilityProfileKind.hostReadOnly.rawValue == "host-read-only")
        #expect(ShowCapabilityProfileKind.musicBedReliable.rawValue == "music-bed-reliable")
        #expect(ShowCapabilityProfileKind.sponsorDeclared.rawValue == "sponsor-declared")
        #expect(ShowCapabilityProfileKind.dynamicInsertionHeavy.rawValue == "dynamic-insertion-heavy")
        // Belt-and-braces: every case round-trips through its rawValue.
        // Catches a future case-rename that updates one of the explicit
        // strings but forgets to migrate persisted rows.
        for kind in ShowCapabilityProfileKind.allCases {
            #expect(ShowCapabilityProfileKind(rawValue: kind.rawValue) == kind,
                    "\(kind) must round-trip through its rawValue")
        }
    }

    @Test("unrecognized kindRawValue decodes back to .unknown — corruption safety")
    func unknownKindRawValueFallsBackToUnknown() async throws {
        // h6a6 R10 review gap: `ShowCapabilityProfileStore.snapshot(from:)`
        // line 237 wraps the decode in `ShowCapabilityProfileKind(
        // rawValue: row.kindRawValue) ?? .unknown` so a corrupt /
        // future-schema / hand-edited row degrades gracefully rather
        // than failing the whole fetch. Pinning the fallback explicitly
        // so a future refactor that drops the `?? .unknown` (or replaces
        // it with a precondition) is a loud test failure rather than a
        // crash on every cold launch after a schema rev.
        let container = try Self.makeContainer()
        let context = container.mainContext

        // Hand-craft a row with an unrecognized kindRawValue. The
        // schema accepts any String; the read-side guard is what we're
        // pinning here.
        let row = ShowCapabilityProfile(
            showIdentifier: "show-corrupt",
            completedEpisodeCount: 7,
            chapterMatchedEpisodeCount: 7,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            kindRawValue: "future-schema-only-kind",
            schemaVersion: ShowCapabilityProfile.currentSchemaVersion
        )
        context.insert(row)
        try context.save()

        // Read back through the store; the unrecognized rawValue must
        // surface as `.unknown` rather than nil or a crash.
        let store = ShowCapabilityProfileStore(modelContainer: container)
        let snap = try #require(await store.snapshot(showIdentifier: "show-corrupt"))
        #expect(snap.kind == .unknown,
                "Unrecognized kindRawValue must decode as .unknown — pinning the corruption-safety fallback")
        #expect(snap.completedEpisodeCount == 7,
                "Counters are independent of the kind decode; they must survive an unknown kindRawValue")
        #expect(!snap.isObserved,
                "An unknown-decoded kind is not observed regardless of the counter state")
    }

    @Test("every non-unknown kind survives a write-then-read round-trip with its rawValue intact")
    func everyKindRoundTripsThroughPersistence() async throws {
        // h6a6 R10 review gap: the prior persistence tests exercised
        // chapter-rich, host-read-only, music-bed-reliable via behavior
        // but never sponsor-declared or dynamic-insertion-heavy. A
        // typo in the evaluator's kind→rawValue mapping for either of
        // those two would have shipped silently. This test drives the
        // store through the floor for each of the five non-unknown
        // kinds and asserts the persisted snapshot decodes the exact
        // expected kind.
        struct Case {
            let kind: ShowCapabilityProfileKind
            let outcome: ShowCapabilityEpisodeOutcome
            let musicBedConfirmed: Bool
        }
        let cases: [Case] = [
            // chapter-rich: 5/5 chapter-matched
            Case(
                kind: .chapterRich,
                outcome: ShowCapabilityEpisodeOutcome(
                    chapterMatched: true,
                    hostVoiced: false,
                    sponsorDeclared: false,
                    dynamicInsertionShift: false
                ),
                musicBedConfirmed: false
            ),
            // host-read-only: 5/5 host-voiced
            Case(
                kind: .hostReadOnly,
                outcome: ShowCapabilityEpisodeOutcome(
                    chapterMatched: false,
                    hostVoiced: true,
                    sponsorDeclared: false,
                    dynamicInsertionShift: false
                ),
                musicBedConfirmed: false
            ),
            // music-bed-reliable: 5 nothing-observed + 2hpn confirmed
            Case(
                kind: .musicBedReliable,
                outcome: .nothingObserved,
                musicBedConfirmed: true
            ),
            // sponsor-declared: 5/5 sponsor-declared (and not host-voiced
            // so host-read-only's higher-priority predicate doesn't win)
            Case(
                kind: .sponsorDeclared,
                outcome: ShowCapabilityEpisodeOutcome(
                    chapterMatched: false,
                    hostVoiced: false,
                    sponsorDeclared: true,
                    dynamicInsertionShift: false
                ),
                musicBedConfirmed: false
            ),
            // dynamic-insertion-heavy: 5/5 boundary shift
            Case(
                kind: .dynamicInsertionHeavy,
                outcome: ShowCapabilityEpisodeOutcome(
                    chapterMatched: false,
                    hostVoiced: false,
                    sponsorDeclared: false,
                    dynamicInsertionShift: true
                ),
                musicBedConfirmed: false
            ),
        ]

        for testCase in cases {
            let container = try Self.makeContainer()
            let store = ShowCapabilityProfileStore(modelContainer: container)
            let showID = "show-\(testCase.kind.rawValue)"

            for _ in 0..<5 {
                _ = await store.recordEpisodeOutcome(
                    showIdentifier: showID,
                    outcome: testCase.outcome,
                    musicBedConfirmed: testCase.musicBedConfirmed,
                    sliGate: Self.openGate,
                    now: Date()
                )
            }

            // Re-read through the snapshot path so we exercise the
            // decode-from-kindRawValue branch (not just the in-process
            // mutation return).
            let snap = try #require(await store.snapshot(showIdentifier: showID))
            #expect(snap.kind == testCase.kind,
                    "Expected \(testCase.kind) after 5 outcomes for \(testCase.kind.rawValue)")
            #expect(snap.schemaVersion == ShowCapabilityProfile.currentSchemaVersion)
            #expect(snap.isObserved)
        }
    }
}
