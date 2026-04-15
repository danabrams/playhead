// CausalSourceDemotionStoreTests.swift
// ef2.3.3: Tests for CausalSourceDemotionStore — bounded, show-local
// demotion of causal sources after false positives.

import Foundation
import Testing
@testable import Playhead

// MARK: - CausalSource Enum

@Suite("CausalSource — Enum Basics")
struct CausalSourceEnumTests {

    @Test("All cases round-trip through rawValue")
    func allCasesRoundTrip() {
        for source in CausalSource.allCases {
            #expect(CausalSource(rawValue: source.rawValue) == source)
        }
    }

    @Test("Expected case count is 7")
    func caseCount() {
        #expect(CausalSource.allCases.count == 7)
    }
}

// MARK: - SourceDemotion + FingerprintDispute Types

@Suite("SourceDemotion + FingerprintDispute — Value Types")
struct DemotionTypeTests {

    @Test("SourceDemotion stores all fields")
    func sourceDemotionFields() {
        let d = SourceDemotion(
            causalSource: .lexical,
            showId: "show-1",
            demotionDelta: 0.2,
            currentMultiplier: 0.8,
            floor: 0.3,
            createdAt: 1000,
            updatedAt: 1000
        )
        #expect(d.causalSource == .lexical)
        #expect(d.showId == "show-1")
        #expect(d.demotionDelta == 0.2)
        #expect(d.currentMultiplier == 0.8)
        #expect(d.floor == 0.3)
    }

    @Test("FingerprintDispute stores all fields")
    func fingerprintDisputeFields() {
        let fd = FingerprintDispute(
            fingerprintId: "fp-1",
            showId: "show-1",
            disputeCount: 1,
            confirmationCount: 0,
            status: .disputed
        )
        #expect(fd.fingerprintId == "fp-1")
        #expect(fd.status == .disputed)
    }

    @Test("FingerprintDisputeStatus raw values")
    func fingerprintDisputeStatusRawValues() {
        #expect(FingerprintDisputeStatus.disputed.rawValue == "disputed")
        #expect(FingerprintDisputeStatus.cleared.rawValue == "cleared")
    }
}

// MARK: - CausalSourceDemotionStore — Demotion Logic

@Suite("CausalSourceDemotionStore — Demotion Logic")
struct DemotionStoreTests {

    @Test("Exempt sources always return 1.0")
    func exemptSourcesReturnOne() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        let fmFactor = await demotionStore.demotionFactor(source: .foundationModel, showId: "show-1")
        #expect(fmFactor == 1.0)

        let acousticFactor = await demotionStore.demotionFactor(source: .acoustic, showId: "show-1")
        #expect(acousticFactor == 1.0)
    }

    @Test("Exempt sources are no-op on applyDemotion")
    func exemptSourcesNoOpDemotion() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        await demotionStore.applyDemotion(source: .foundationModel, showId: "show-1", correctionId: "c1")
        let factor = await demotionStore.demotionFactor(source: .foundationModel, showId: "show-1")
        #expect(factor == 1.0)

        await demotionStore.applyDemotion(source: .acoustic, showId: "show-1", correctionId: "c2")
        let factor2 = await demotionStore.demotionFactor(source: .acoustic, showId: "show-1")
        #expect(factor2 == 1.0)
    }

    @Test("Undeclared source returns 1.0")
    func noRowReturnsOne() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        let factor = await demotionStore.demotionFactor(source: .lexical, showId: "show-no-demotions")
        #expect(factor == 1.0)
    }

    @Test("Lexical demotion reduces by 0.2, floor 0.3")
    func lexicalDemotion() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        await demotionStore.applyDemotion(source: .lexical, showId: "show-1", correctionId: "c1")
        let f1 = await demotionStore.demotionFactor(source: .lexical, showId: "show-1")
        #expect(abs(f1 - 0.8) < 0.001)

        await demotionStore.applyDemotion(source: .lexical, showId: "show-1", correctionId: "c2")
        let f2 = await demotionStore.demotionFactor(source: .lexical, showId: "show-1")
        #expect(abs(f2 - 0.6) < 0.001)

        await demotionStore.applyDemotion(source: .lexical, showId: "show-1", correctionId: "c3")
        let f3 = await demotionStore.demotionFactor(source: .lexical, showId: "show-1")
        #expect(abs(f3 - 0.4) < 0.001)

        // Fourth demotion should hit floor at 0.3
        await demotionStore.applyDemotion(source: .lexical, showId: "show-1", correctionId: "c4")
        let f4 = await demotionStore.demotionFactor(source: .lexical, showId: "show-1")
        #expect(abs(f4 - 0.3) < 0.001)

        // Fifth demotion should stay at floor
        await demotionStore.applyDemotion(source: .lexical, showId: "show-1", correctionId: "c5")
        let f5 = await demotionStore.demotionFactor(source: .lexical, showId: "show-1")
        #expect(abs(f5 - 0.3) < 0.001)
    }

    @Test("Metadata demotion reduces by 0.05, floor 0.05")
    func metadataDemotion() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        await demotionStore.applyDemotion(source: .metadata, showId: "show-1", correctionId: "c1")
        let f1 = await demotionStore.demotionFactor(source: .metadata, showId: "show-1")
        #expect(abs(f1 - 0.95) < 0.001)
    }

    @Test("MusicBracket demotion reduces by 0.1, floor 0.2")
    func musicBracketDemotion() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        // 9 demotions to reach and stay at floor
        for i in 1...9 {
            await demotionStore.applyDemotion(source: .musicBracket, showId: "show-1", correctionId: "c\(i)")
        }
        let factor = await demotionStore.demotionFactor(source: .musicBracket, showId: "show-1")
        #expect(abs(factor - 0.2) < 0.001)
    }

    @Test("PositionPrior demotion reduces by 0.1, floor 0.3")
    func positionPriorDemotion() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        for i in 1...8 {
            await demotionStore.applyDemotion(source: .positionPrior, showId: "show-1", correctionId: "c\(i)")
        }
        let factor = await demotionStore.demotionFactor(source: .positionPrior, showId: "show-1")
        #expect(abs(factor - 0.3) < 0.001)
    }

    @Test("Demotions are show-local")
    func showLocal() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        await demotionStore.applyDemotion(source: .lexical, showId: "show-A", correctionId: "c1")
        let factorA = await demotionStore.demotionFactor(source: .lexical, showId: "show-A")
        let factorB = await demotionStore.demotionFactor(source: .lexical, showId: "show-B")
        #expect(abs(factorA - 0.8) < 0.001)
        #expect(factorB == 1.0)
    }
}

// MARK: - CausalSourceDemotionStore — Fingerprint Disputes

@Suite("CausalSourceDemotionStore — Fingerprint Disputes")
struct FingerprintDisputeTests {

    @Test("Fingerprint demotion creates dispute")
    func fingerprintDemotionCreatesDispute() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        await demotionStore.applyDemotion(source: .fingerprint, showId: "show-1", correctionId: "fp-abc")
        let disputed = await demotionStore.isFingerprintDisputed(fingerprintId: "fp-abc", showId: "show-1")
        #expect(disputed == true)
    }

    @Test("Non-existent fingerprint is not disputed")
    func nonExistentFingerprintNotDisputed() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        let disputed = await demotionStore.isFingerprintDisputed(fingerprintId: "fp-never", showId: "show-1")
        #expect(disputed == false)
    }

    @Test("Fingerprint dispute cleared after 2 confirmations")
    func disputeClearedAfterTwoConfirmations() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        // Create dispute
        await demotionStore.applyDemotion(source: .fingerprint, showId: "show-1", correctionId: "fp-abc")
        #expect(await demotionStore.isFingerprintDisputed(fingerprintId: "fp-abc", showId: "show-1") == true)

        // First confirmation — still disputed
        await demotionStore.recordFingerprintConfirmation(fingerprintId: "fp-abc", showId: "show-1")
        #expect(await demotionStore.isFingerprintDisputed(fingerprintId: "fp-abc", showId: "show-1") == true)

        // Second confirmation — cleared
        await demotionStore.recordFingerprintConfirmation(fingerprintId: "fp-abc", showId: "show-1")
        #expect(await demotionStore.isFingerprintDisputed(fingerprintId: "fp-abc", showId: "show-1") == false)
    }

    @Test("Fingerprint disputes are show-local")
    func fingerprintDisputeShowLocal() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        await demotionStore.applyDemotion(source: .fingerprint, showId: "show-A", correctionId: "fp-abc")
        #expect(await demotionStore.isFingerprintDisputed(fingerprintId: "fp-abc", showId: "show-A") == true)
        #expect(await demotionStore.isFingerprintDisputed(fingerprintId: "fp-abc", showId: "show-B") == false)
    }

    @Test("Fingerprint demotion also reduces multiplier")
    func fingerprintDemotionReducesMultiplier() async throws {
        let store = try await makeTestStore()
        let demotionStore = CausalSourceDemotionStore(store: store)

        await demotionStore.applyDemotion(source: .fingerprint, showId: "show-1", correctionId: "fp-abc")
        let factor = await demotionStore.demotionFactor(source: .fingerprint, showId: "show-1")
        #expect(abs(factor - 0.9) < 0.001)
    }
}

// MARK: - Schema Migration

@Suite("Source Demotions — V11 Migration")
struct DemotionMigrationTests {

    @Test("V11 migration creates tables")
    func v11MigrationCreatesTables() async throws {
        let store = try await makeTestStore()
        // If migration worked, we can write and read without error.
        try await store.upsertSourceDemotion(
            source: "lexical", showId: "show-1",
            currentMultiplier: 0.8, floor: 0.3, updatedAt: 1000
        )
        let multiplier = try await store.loadSourceDemotionMultiplier(source: "lexical", showId: "show-1")
        #expect(multiplier == 0.8)
    }

    @Test("V11 migration creates fingerprint_disputes table")
    func v11MigrationCreatesFingerprintDisputes() async throws {
        let store = try await makeTestStore()
        try await store.upsertFingerprintDispute(
            fingerprintId: "fp-1", showId: "show-1",
            incrementDispute: true, incrementConfirmation: false, now: 1000
        )
        let status = try await store.loadFingerprintDisputeStatus(fingerprintId: "fp-1", showId: "show-1")
        #expect(status == "disputed")
    }
}
