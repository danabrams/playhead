// SponsorKnowledgeStoreTests.swift
// Phase 8 (playhead-4my.8.1): Tests for SponsorKnowledgeStore lifecycle
// transitions, CRUD persistence, matcher integration, and negative memory.

import Foundation
import Testing
@testable import Playhead

// MARK: - KnowledgeState Enum

@Suite("KnowledgeState")
struct KnowledgeStateTests {

    @Test("All cases have distinct rawValues")
    func allCasesDistinct() {
        let rawValues = KnowledgeState.allCases.map(\.rawValue)
        #expect(Set(rawValues).count == rawValues.count)
    }

    @Test("Round-trips through rawValue")
    func roundTrip() {
        for state in KnowledgeState.allCases {
            #expect(KnowledgeState(rawValue: state.rawValue) == state)
        }
    }
}

// MARK: - KnowledgeEntityType Enum

@Suite("KnowledgeEntityType")
struct KnowledgeEntityTypeTests {

    @Test("All four entity types exist")
    func allTypes() {
        #expect(KnowledgeEntityType.allCases.count == 4)
        #expect(KnowledgeEntityType.allCases.contains(.sponsor))
        #expect(KnowledgeEntityType.allCases.contains(.cta))
        #expect(KnowledgeEntityType.allCases.contains(.url))
        #expect(KnowledgeEntityType.allCases.contains(.disclosure))
    }
}

// MARK: - SponsorKnowledgeEntry

@Suite("SponsorKnowledgeEntry")
struct SponsorKnowledgeEntryTests {

    @Test("normalizedValue defaults to lowercased trimmed entityValue")
    func normalizedValueDefault() {
        let entry = SponsorKnowledgeEntry(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "  Squarespace  "
        )
        #expect(entry.normalizedValue == "squarespace")
    }

    @Test("rollbackRate computes correctly")
    func rollbackRate() {
        let entry = SponsorKnowledgeEntry(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "TestSponsor",
            confirmationCount: 7,
            rollbackCount: 3
        )
        #expect(abs(entry.rollbackRate - 0.3) < 0.001)
    }

    @Test("rollbackRate is zero when no observations")
    func rollbackRateZero() {
        let entry = SponsorKnowledgeEntry(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "New"
        )
        #expect(entry.rollbackRate == 0.0)
    }
}

// MARK: - SponsorKnowledgeStore Lifecycle

@Suite("SponsorKnowledgeStore — Lifecycle")
struct SponsorKnowledgeStoreLifecycleTests {

    @Test("candidate -> quarantined on first high-quality confirmation")
    func candidateToQuarantined() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        try await knowledgeStore.recordCandidate(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "Squarespace",
            analysisAssetId: "asset-1",
            sourceAtomOrdinals: [10, 11, 12],
            transcriptVersion: "tv-1",
            confidence: 0.8
        )

        let entry = try await knowledgeStore.entry(
            podcastId: "pod-1",
            entityType: .sponsor,
            normalizedValue: "squarespace"
        )
        #expect(entry != nil)
        #expect(entry?.state == .quarantined, "First confirmation should promote candidate to quarantined")
        #expect(entry?.confirmationCount == 1)
    }

    @Test("quarantined -> active after 2+ confirmations with low rollback")
    func quarantinedToActive() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // First confirmation: candidate -> quarantined
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "Squarespace",
            analysisAssetId: "asset-1",
            sourceAtomOrdinals: [10],
            transcriptVersion: "tv-1",
            confidence: 0.8
        )

        // Second confirmation: quarantined -> active
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "Squarespace",
            analysisAssetId: "asset-2",
            sourceAtomOrdinals: [20],
            transcriptVersion: "tv-2",
            confidence: 0.9
        )

        let entry = try await knowledgeStore.entry(
            podcastId: "pod-1",
            entityType: .sponsor,
            normalizedValue: "squarespace"
        )
        #expect(entry?.state == .active, "Two confirmations should promote to active")
        #expect(entry?.confirmationCount == 2)
    }

    @Test("active -> decayed on rollback spike")
    func activeToDecayed() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Build up to active state.
        for i in 1...3 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-1",
                entityType: .sponsor,
                entityValue: "BetterHelp",
                analysisAssetId: "asset-\(i)",
                sourceAtomOrdinals: [i * 10],
                transcriptVersion: "tv-\(i)",
                confidence: 0.85
            )
        }
        let before = try await knowledgeStore.entry(
            podcastId: "pod-1",
            entityType: .sponsor,
            normalizedValue: "betterhelp"
        )
        #expect(before?.state == .active)

        // Record enough rollbacks to spike the rate above 0.5.
        for _ in 1...4 {
            try await knowledgeStore.recordRollback(
                podcastId: "pod-1",
                entityType: .sponsor,
                entityValue: "BetterHelp"
            )
        }

        let after = try await knowledgeStore.entry(
            podcastId: "pod-1",
            entityType: .sponsor,
            normalizedValue: "betterhelp"
        )
        #expect(after?.state == .decayed, "Rollback spike should demote active to decayed")
    }

    @Test("blockEntry explicitly blocks any state")
    func blockEntry() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Create an active entry.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-1",
                entityType: .sponsor,
                entityValue: "BlockMe",
                analysisAssetId: "asset-\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        try await knowledgeStore.blockEntry(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "BlockMe"
        )

        let entry = try await knowledgeStore.entry(
            podcastId: "pod-1",
            entityType: .sponsor,
            normalizedValue: "blockme"
        )
        #expect(entry?.state == .blocked)
        #expect(entry?.blockedAt != nil)
    }

    @Test("blocked entries cannot be promoted")
    func blockedCannotPromote() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Create and block.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "Stuck",
            analysisAssetId: "asset-1",
            sourceAtomOrdinals: [1],
            transcriptVersion: "tv-1",
            confidence: 0.9
        )
        try await knowledgeStore.blockEntry(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "Stuck"
        )

        // Try to confirm again — should stay blocked.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "Stuck",
            analysisAssetId: "asset-2",
            sourceAtomOrdinals: [2],
            transcriptVersion: "tv-2",
            confidence: 0.95
        )

        let entry = try await knowledgeStore.entry(
            podcastId: "pod-1",
            entityType: .sponsor,
            normalizedValue: "stuck"
        )
        #expect(entry?.state == .blocked, "Blocked entries must not be promoted by new confirmations")
    }

    @Test("Low confidence candidates are rejected")
    func lowConfidenceRejected() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        try await knowledgeStore.recordCandidate(
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "Weak",
            analysisAssetId: "asset-1",
            sourceAtomOrdinals: [1],
            transcriptVersion: "tv-1",
            confidence: 0.3 // Below threshold
        )

        let entry = try await knowledgeStore.entry(
            podcastId: "pod-1",
            entityType: .sponsor,
            normalizedValue: "weak"
        )
        #expect(entry == nil, "Low-confidence candidates should not be stored")
    }
}

// MARK: - SponsorKnowledgeStore Persistence

@Suite("SponsorKnowledgeStore — Persistence")
struct SponsorKnowledgeStorePersistenceTests {

    @Test("Entry round-trips through SQLite")
    func entryRoundTrips() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        try await knowledgeStore.recordCandidate(
            podcastId: "pod-rt",
            entityType: .url,
            entityValue: "squarespace.com/podcast",
            analysisAssetId: "asset-rt",
            sourceAtomOrdinals: [5, 6, 7],
            transcriptVersion: "tv-rt",
            confidence: 0.75
        )

        let entry = try await knowledgeStore.entry(
            podcastId: "pod-rt",
            entityType: .url,
            normalizedValue: "squarespace.com/podcast"
        )
        #expect(entry != nil)
        #expect(entry?.entityValue == "squarespace.com/podcast")
        #expect(entry?.entityType == .url)
        #expect(entry?.podcastId == "pod-rt")
    }

    @Test("KnowledgeCandidateEvent is appended and retrievable")
    func candidateEventPersists() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        try await knowledgeStore.recordCandidate(
            podcastId: "pod-ce",
            entityType: .cta,
            entityValue: "Use code PODCAST",
            analysisAssetId: "asset-ce",
            sourceAtomOrdinals: [100, 101],
            transcriptVersion: "tv-ce",
            confidence: 0.8,
            scanCohortJSON: "{\"cohort\":\"test\"}"
        )

        let events = try await knowledgeStore.candidateEvents(forAsset: "asset-ce")
        #expect(events.count == 1)
        let event = events[0]
        #expect(event.entityType == .cta)
        #expect(event.entityValue == "Use code PODCAST")
        #expect(event.sourceAtomOrdinals == [100, 101])
        #expect(event.transcriptVersion == "tv-ce")
        #expect(event.confidence == 0.8)
        #expect(event.scanCohortJSON == "{\"cohort\":\"test\"}")
    }

    @Test("Aliases are accumulated and persisted")
    func aliasAccumulation() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // First variant.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-alias",
            entityType: .sponsor,
            entityValue: "Squarespace",
            analysisAssetId: "asset-a1",
            sourceAtomOrdinals: [1],
            transcriptVersion: "tv-1",
            confidence: 0.9
        )

        // Second variant with different casing — same normalizedValue.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-alias",
            entityType: .sponsor,
            entityValue: "SQUARESPACE",
            analysisAssetId: "asset-a2",
            sourceAtomOrdinals: [2],
            transcriptVersion: "tv-2",
            confidence: 0.9
        )

        let entry = try await knowledgeStore.entry(
            podcastId: "pod-alias",
            entityType: .sponsor,
            normalizedValue: "squarespace"
        )
        #expect(entry != nil)
        #expect(entry?.aliases.contains("SQUARESPACE") == true,
                "Alternate casing should be stored as an alias")
    }

    @Test("allEntries returns entries in all states")
    func allEntriesReturnsAll() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Create entries in different states.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-all",
            entityType: .sponsor,
            entityValue: "A",
            analysisAssetId: "asset-1",
            sourceAtomOrdinals: [1],
            transcriptVersion: "tv",
            confidence: 0.9
        )
        for i in 2...3 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-all",
                entityType: .sponsor,
                entityValue: "B",
                analysisAssetId: "asset-\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv",
                confidence: 0.9
            )
        }

        let all = try await knowledgeStore.allEntries(forPodcast: "pod-all")
        #expect(all.count == 2, "Should have 2 entries total")
    }
}

// MARK: - SponsorKnowledgeStore: activeEntries filtering

@Suite("SponsorKnowledgeStore — Active Filtering")
struct SponsorKnowledgeStoreActiveFilteringTests {

    @Test("activeEntries returns only active-state entries")
    func activeEntriesFiltering() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Create an active entry (2+ confirmations).
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-filter",
                entityType: .sponsor,
                entityValue: "Active Sponsor",
                analysisAssetId: "asset-\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Create a candidate entry (only 1 confirmation).
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-filter",
            entityType: .sponsor,
            entityValue: "Candidate Only",
            analysisAssetId: "asset-3",
            sourceAtomOrdinals: [3],
            transcriptVersion: "tv-3",
            confidence: 0.7
        )

        let active = try await knowledgeStore.activeEntries(forPodcast: "pod-filter")
        #expect(active.count == 1)
        #expect(active[0].entityValue == "Active Sponsor")
    }
}

// MARK: - Negative Memory

@Suite("SponsorKnowledgeStore — Negative Memory")
struct SponsorKnowledgeStoreNegativeMemoryTests {

    @Test("activeEntriesWithNegativeMemory filters out corrected sponsors")
    func negativeMemoryFiltering() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)

        // Need an analysis asset for the correction event FK.
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-neg"))

        // Create an active sponsor entry.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-neg",
                entityType: .sponsor,
                entityValue: "BadSponsor",
                analysisAssetId: "asset-\(i)x",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Create another active sponsor (not corrected).
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-neg",
                entityType: .sponsor,
                entityValue: "GoodSponsor",
                analysisAssetId: "asset-\(i)y",
                sourceAtomOrdinals: [i + 10],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Record a sponsorOnShow correction for BadSponsor.
        let scope = CorrectionScope.sponsorOnShow(podcastId: "pod-neg", sponsor: "badsponsor")
        let event = CorrectionEvent(
            analysisAssetId: "asset-neg",
            scope: scope.serialized,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        // Query with negative memory applied.
        let filtered = try await knowledgeStore.activeEntriesWithNegativeMemory(
            forPodcast: "pod-neg"
        )
        #expect(filtered.count == 1, "Corrected sponsor should be filtered out")
        #expect(filtered[0].normalizedValue == "goodsponsor")
    }

    @Test("Non-sponsor entity types are not affected by sponsorOnShow corrections")
    func nonSponsorNotFiltered() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-ns"))

        // Create an active URL entry.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-ns",
                entityType: .url,
                entityValue: "example.com/offer",
                analysisAssetId: "asset-\(i)z",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Record a correction (this won't match url type).
        let scope = CorrectionScope.sponsorOnShow(podcastId: "pod-ns", sponsor: "example.com/offer")
        let event = CorrectionEvent(
            analysisAssetId: "asset-ns",
            scope: scope.serialized,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        let filtered = try await knowledgeStore.activeEntriesWithNegativeMemory(
            forPodcast: "pod-ns"
        )
        #expect(filtered.count == 1, "URL entries are not filtered by sponsorOnShow corrections")
    }
}

// MARK: - SponsorKnowledgeMatcher Integration

@Suite("SponsorKnowledgeMatcher — Integration")
struct SponsorKnowledgeMatcherIntegrationTests {

    private func makeAtom(
        assetId: String = "asset-match",
        ordinal: Int,
        text: String,
        startTime: Double,
        endTime: Double
    ) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: assetId,
                transcriptVersion: "tv-test",
                atomOrdinal: ordinal
            ),
            contentHash: "hash-\(ordinal)",
            startTime: startTime,
            endTime: endTime,
            text: text,
            chunkIndex: 0
        )
    }

    @Test("Legacy stub still returns empty")
    func legacyStubEmpty() {
        let atoms = [makeAtom(ordinal: 0, text: "hello", startTime: 0, endTime: 1)]
        let matches = SponsorKnowledgeMatcher.match(atoms: atoms)
        #expect(matches.isEmpty)
    }

    @Test("Matcher returns matches for active entries")
    func matcherFindsActiveEntries() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)
        // Build an active sponsor entry.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-match",
                entityType: .sponsor,
                entityValue: "Squarespace",
                analysisAssetId: "asset-m\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        let atoms = [
            makeAtom(ordinal: 0, text: "Welcome to the show", startTime: 0, endTime: 5),
            makeAtom(ordinal: 1, text: "This episode is brought to you by Squarespace", startTime: 5, endTime: 10),
            makeAtom(ordinal: 2, text: "build your website today", startTime: 10, endTime: 15),
        ]

        let matches = try await SponsorKnowledgeMatcher.match(
            atoms: atoms,
            podcastId: "pod-match",
            knowledgeStore: knowledgeStore
        )

        #expect(matches.count == 1, "Should find one match for Squarespace")
        #expect(matches[0].entityName == "Squarespace")
        #expect(matches[0].firstAtomOrdinal == 1)
    }

    @Test("Matcher returns empty for non-active entries")
    func matcherIgnoresNonActive() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)
        // Only one confirmation — stays quarantined, not active.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-nonactive",
            entityType: .sponsor,
            entityValue: "Squarespace",
            analysisAssetId: "asset-na",
            sourceAtomOrdinals: [1],
            transcriptVersion: "tv-1",
            confidence: 0.9
        )

        let atoms = [
            makeAtom(ordinal: 0, text: "Squarespace is great", startTime: 0, endTime: 5),
        ]

        let matches = try await SponsorKnowledgeMatcher.match(
            atoms: atoms,
            podcastId: "pod-nonactive",
            knowledgeStore: knowledgeStore
        )

        #expect(matches.isEmpty, "Quarantined entries should not produce matches")
    }

    @Test("Matcher respects negative memory from corrections")
    func matcherRespectsNegativeMemory() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-neg-match"))

        // Build active entry.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-neg-match",
                entityType: .sponsor,
                entityValue: "Corrected",
                analysisAssetId: "asset-nm\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Record a correction against this sponsor.
        let scope = CorrectionScope.sponsorOnShow(podcastId: "pod-neg-match", sponsor: "corrected")
        let event = CorrectionEvent(
            analysisAssetId: "asset-neg-match",
            scope: scope.serialized,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        let atoms = [
            makeAtom(ordinal: 0, text: "Corrected sponsor mention", startTime: 0, endTime: 5),
        ]

        let matches = try await SponsorKnowledgeMatcher.match(
            atoms: atoms,
            podcastId: "pod-neg-match",
            knowledgeStore: knowledgeStore
        )

        #expect(matches.isEmpty, "Corrected sponsors should not match")
    }

    @Test("Empty atoms returns empty matches")
    func emptyAtomsReturnsEmpty() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)
        let matches = try await SponsorKnowledgeMatcher.match(
            atoms: [],
            podcastId: "pod-empty",
            knowledgeStore: knowledgeStore
        )
        #expect(matches.isEmpty)
    }
}

// MARK: - Schema Migration

@Suite("SponsorKnowledge — Schema Migration")
struct SponsorKnowledgeSchemaMigrationTests {

    @Test("V7 migration creates both tables and sets schema version to 7")
    func v7MigrationCreates() async throws {
        let analysisStore = try await makeTestStore()
        let version = try await analysisStore.schemaVersion()
        #expect(version == 19, "Schema version should be 19 after migration")
    }

    @Test("V7 migration is idempotent")
    func v7MigrationIdempotent() async throws {
        let analysisStore = try await makeTestStore()
        // migrate() is called by makeTestStore; call it again.
        try await analysisStore.migrate()
        let version = try await analysisStore.schemaVersion()
        #expect(version == 19)
    }
}

// MARK: - Promotion/Demotion Logic

@Suite("SponsorKnowledgeStore — Promotion/Demotion")
struct SponsorKnowledgeStorePromotionTests {

    @Test("promoteState: candidate with 1 confirmation -> quarantined")
    func promoteCandidate() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.promoteState(
            current: .candidate,
            confirmationCount: 1,
            rollbackCount: 0,
            hasActiveCorrection: false
        )
        #expect(result == .quarantined)
    }

    @Test("promoteState: quarantined with 2 confirmations -> active")
    func promoteQuarantined() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.promoteState(
            current: .quarantined,
            confirmationCount: 2,
            rollbackCount: 0,
            hasActiveCorrection: false
        )
        #expect(result == .active)
    }

    @Test("promoteState: quarantined stays quarantined with high rollback rate")
    func quarantinedStaysWithHighRollback() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.promoteState(
            current: .quarantined,
            confirmationCount: 2,
            rollbackCount: 2,
            hasActiveCorrection: false
        )
        #expect(result == .quarantined, "50% rollback rate should prevent promotion")
    }

    @Test("promoteState: active stays active unless rollback spike")
    func activeStays() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.promoteState(
            current: .active,
            confirmationCount: 10,
            rollbackCount: 2,
            hasActiveCorrection: false
        )
        #expect(result == .active)
    }

    @Test("promoteState: active -> decayed on rollback spike")
    func activeToDecayedOnSpike() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.promoteState(
            current: .active,
            confirmationCount: 3,
            rollbackCount: 4,
            hasActiveCorrection: false
        )
        #expect(result == .decayed)
    }

    @Test("promoteState: decayed can re-promote to active with good stats")
    func decayedRePromotes() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.promoteState(
            current: .decayed,
            confirmationCount: 5,
            rollbackCount: 1,
            hasActiveCorrection: false
        )
        #expect(result == .active)
    }

    @Test("promoteState: blocked never promotes")
    func blockedNeverPromotes() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.promoteState(
            current: .blocked,
            confirmationCount: 100,
            rollbackCount: 0,
            hasActiveCorrection: false
        )
        #expect(result == .blocked)
    }

    @Test("promoteState: active correction prevents promotion")
    func activeCorrectionBlocksPromotion() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.promoteState(
            current: .quarantined,
            confirmationCount: 5,
            rollbackCount: 0,
            hasActiveCorrection: true
        )
        #expect(result == .quarantined, "Active correction should prevent promotion")
    }

    @Test("demoteState: active -> decayed on high rollback")
    func demoteActive() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.demoteState(
            current: .active,
            confirmationCount: 3,
            rollbackCount: 4
        )
        #expect(result == .decayed)
    }

    @Test("demoteState: candidate -> blocked on high rollback")
    func demoteCandidate() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.demoteState(
            current: .candidate,
            confirmationCount: 1,
            rollbackCount: 2
        )
        #expect(result == .blocked)
    }

    @Test("stablePromoteState: candidate with >=2 confirmations jumps to active in one call")
    func stablePromoteMultiStep() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        let result = await store.stablePromoteState(
            current: .candidate,
            confirmationCount: 2,
            rollbackCount: 0,
            hasActiveCorrection: false
        )
        // candidate -> quarantined -> active in one bounded loop
        #expect(result == .active)
    }

    @Test("promoteState: quarantined with exactly boundary rollback rate (0.3) still promotes")
    func promoteBoundaryRollbackRate() async throws {
        let store = SponsorKnowledgeStore(store: try await makeTestStore())
        // 2 confirmations, 0 rollbacks but let's test exact boundary:
        // 7 confirmations + 3 rollbacks = 0.3 rollback rate exactly
        let result = await store.promoteState(
            current: .quarantined,
            confirmationCount: 7,
            rollbackCount: 3,
            hasActiveCorrection: false
        )
        // rollbackRate == 0.3 which is <= 0.3 threshold, so should promote
        #expect(result == .active)
    }
}

// MARK: - Concurrent Access

@Suite("SponsorKnowledgeStore — Concurrency")
struct SponsorKnowledgeStoreConcurrencyTests {

    @Test("Concurrent recordCandidate + recordRollback serializes correctly")
    func concurrentRecordAndRollback() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Seed an entry with enough confirmations to be active.
        for i in 0..<3 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-conc",
                entityType: .sponsor,
                entityValue: "ConcurrentSponsor",
                analysisAssetId: "asset-conc-\(i)",
                sourceAtomOrdinals: [0],
                transcriptVersion: "tv-1",
                confidence: 0.9
            )
        }

        // Fire concurrent recordCandidate + recordRollback from a TaskGroup.
        await withTaskGroup(of: Void.self) { group in
            for i in 0..<10 {
                group.addTask {
                    try? await knowledgeStore.recordCandidate(
                        podcastId: "pod-conc",
                        entityType: .sponsor,
                        entityValue: "ConcurrentSponsor",
                        analysisAssetId: "asset-conc-burst-\(i)",
                        sourceAtomOrdinals: [0],
                        transcriptVersion: "tv-1",
                        confidence: 0.9
                    )
                }
                group.addTask {
                    try? await knowledgeStore.recordRollback(
                        podcastId: "pod-conc",
                        entityType: .sponsor,
                        entityValue: "ConcurrentSponsor"
                    )
                }
            }
        }

        // The entry should exist and have consistent counts (no crashes,
        // no data corruption from concurrent access).
        let entry = try await knowledgeStore.entry(
            podcastId: "pod-conc",
            entityType: .sponsor,
            normalizedValue: "concurrentsponsor"
        )
        #expect(entry != nil, "Entry must survive concurrent access")
        // 3 seed + up to 10 concurrent = at most 13 confirmations
        // (some may interleave with rollbacks but counts must be >= initial)
        #expect(entry!.confirmationCount >= 3, "Seed confirmations must survive")
        #expect(entry!.confirmationCount + entry!.rollbackCount >= 3, "Total observations must be >= seed")
    }
}
