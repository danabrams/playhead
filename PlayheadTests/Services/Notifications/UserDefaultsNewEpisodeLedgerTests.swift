// UserDefaultsNewEpisodeLedgerTests.swift
// playhead-snp — Bounded UserDefaults-backed ledger that survives app
// restarts and prunes oldest entries when capacity is exceeded.

import Foundation
import Testing

@testable import Playhead

@Suite("UserDefaultsNewEpisodeLedger — bounded persistence (playhead-snp)")
@MainActor
struct UserDefaultsNewEpisodeLedgerTests {

    private static func makeSuite(_ name: String) -> UserDefaults {
        let suiteName = "UserDefaultsNewEpisodeLedgerTests.\(name)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)
        return defaults
    }

    @Test("New ledger is empty")
    func startsEmpty() {
        let defaults = Self.makeSuite("startsEmpty")
        let ledger = UserDefaultsNewEpisodeLedger(defaults: defaults)
        #expect(!ledger.contains("anything"))
    }

    @Test("record + contains roundtrip")
    func recordRoundtrip() {
        let defaults = Self.makeSuite("recordRoundtrip")
        let ledger = UserDefaultsNewEpisodeLedger(defaults: defaults)
        ledger.record("k1")
        #expect(ledger.contains("k1"))
        #expect(!ledger.contains("k2"))
    }

    @Test("Ledger persists across instances backed by same defaults")
    func persistsAcrossInstances() {
        let defaults = Self.makeSuite("persistsAcrossInstances")

        let first = UserDefaultsNewEpisodeLedger(defaults: defaults)
        first.record("k1")
        first.record("k2")

        let second = UserDefaultsNewEpisodeLedger(defaults: defaults)
        #expect(second.contains("k1"))
        #expect(second.contains("k2"))
        #expect(!second.contains("k3"))
    }

    @Test("Ledger evicts oldest entries when capacity is exceeded")
    func evictsOldestWhenOverCapacity() {
        let defaults = Self.makeSuite("evictsOldestWhenOverCapacity")
        let ledger = UserDefaultsNewEpisodeLedger(defaults: defaults, capacity: 3)

        ledger.record("a")
        ledger.record("b")
        ledger.record("c")
        ledger.record("d")  // evicts "a"

        #expect(!ledger.contains("a"))
        #expect(ledger.contains("b"))
        #expect(ledger.contains("c"))
        #expect(ledger.contains("d"))
    }

    @Test("Re-recording an existing key keeps it (no double count)")
    func reRecordingIsIdempotent() {
        let defaults = Self.makeSuite("reRecordingIsIdempotent")
        let ledger = UserDefaultsNewEpisodeLedger(defaults: defaults, capacity: 3)

        ledger.record("a")
        ledger.record("b")
        ledger.record("a")
        ledger.record("c")
        // No eviction expected — still 3 unique keys.
        #expect(ledger.contains("a"))
        #expect(ledger.contains("b"))
        #expect(ledger.contains("c"))
    }

    @Test("count returns the current size of the ledger")
    func countReportsSize() {
        let defaults = Self.makeSuite("countReportsSize")
        let ledger = UserDefaultsNewEpisodeLedger(defaults: defaults, capacity: 5)
        #expect(ledger.count == 0)
        ledger.record("a")
        ledger.record("b")
        #expect(ledger.count == 2)
        ledger.record("a")  // duplicate
        #expect(ledger.count == 2)
    }

    @Test("clear removes everything")
    func clearWipesLedger() {
        let defaults = Self.makeSuite("clearWipesLedger")
        let ledger = UserDefaultsNewEpisodeLedger(defaults: defaults)
        ledger.record("a")
        ledger.record("b")
        ledger.clear()
        #expect(!ledger.contains("a"))
        #expect(!ledger.contains("b"))
        #expect(ledger.count == 0)
    }
}
