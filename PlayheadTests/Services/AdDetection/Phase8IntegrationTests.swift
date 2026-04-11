// Phase8IntegrationTests.swift
// Phase 8 (playhead-4my.8.3): Integration tests that exercise the full
// Phase 8 pipeline end-to-end: SponsorKnowledgeStore -> CompiledSponsorLexicon
// -> LexicalScanner working together, plus lifecycle round-trips through
// SQLite and negative memory integration.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeChunk(
    text: String,
    normalizedText: String? = nil,
    assetId: String = "asset-int",
    startTime: Double = 0.0,
    endTime: Double = 30.0
) -> TranscriptChunk {
    TranscriptChunk(
        id: UUID().uuidString,
        analysisAssetId: assetId,
        segmentFingerprint: UUID().uuidString,
        chunkIndex: 0,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: normalizedText ?? text.lowercased(),
        pass: "final",
        modelVersion: "test",
        transcriptVersion: nil,
        atomOrdinal: nil
    )
}

// MARK: - Full Pipeline Integration

@Suite("Phase 8 Integration — Full Pipeline")
struct Phase8FullPipelineTests {

    @Test("Store -> compile -> scan: active entry produces scanner hits")
    func storeToCompileToScan() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Record two confirmations to promote to active.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-pipe",
                entityType: .sponsor,
                entityValue: "Squarespace",
                analysisAssetId: "asset-pipe-\(i)",
                sourceAtomOrdinals: [i * 10],
                transcriptVersion: "tv-\(i)",
                confidence: 0.85
            )
        }

        // Verify active state.
        let entry = try await knowledgeStore.entry(
            podcastId: "pod-pipe",
            entityType: .sponsor,
            normalizedValue: "squarespace"
        )
        #expect(entry?.state == .active)

        // Compile lexicon from store's active entries.
        let activeEntries = try await knowledgeStore.activeEntries(forPodcast: "pod-pipe")
        let lexicon = CompiledSponsorLexicon(entries: activeEntries)
        #expect(lexicon.entryCount == 1)

        // Scan a transcript chunk containing the sponsor name.
        let scanner = LexicalScanner(compiledLexicon: lexicon)
        let chunk = makeChunk(text: "today we are joined by Squarespace to talk about websites")
        let hits = scanner.scanChunk(chunk)

        let sponsorHits = hits.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("squarespace")
        }
        #expect(!sponsorHits.isEmpty, "Active entry compiled into lexicon should produce scanner hits")
        #expect(sponsorHits[0].weight == 1.5, "Compiled lexicon hits should have boosted weight")
    }

    @Test("Lifecycle affects scanning: rollback decays entry, recompile removes hits")
    func lifecycleAffectsScanning() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Build up to active (3 confirmations).
        for i in 1...3 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-life",
                entityType: .sponsor,
                entityValue: "BetterHelp",
                analysisAssetId: "asset-life-\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Compile and scan — should find hits.
        let activeBeforeRollback = try await knowledgeStore.activeEntries(forPodcast: "pod-life")
        let lexiconBefore = CompiledSponsorLexicon(entries: activeBeforeRollback)
        let scanner1 = LexicalScanner(compiledLexicon: lexiconBefore)
        let chunk = makeChunk(text: "BetterHelp is great for online therapy")
        let hitsBefore = scanner1.scanChunk(chunk)
        let matchesBefore = hitsBefore.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("betterhelp")
        }
        #expect(!matchesBefore.isEmpty, "Active entry should produce hits before rollback")

        // Rollback enough to decay (4 rollbacks on 3 confirmations > 50% rate).
        for _ in 1...4 {
            try await knowledgeStore.recordRollback(
                podcastId: "pod-life",
                entityType: .sponsor,
                entityValue: "BetterHelp"
            )
        }

        let afterEntry = try await knowledgeStore.entry(
            podcastId: "pod-life",
            entityType: .sponsor,
            normalizedValue: "betterhelp"
        )
        #expect(afterEntry?.state == .decayed, "Rollback spike should decay the entry")

        // Recompile from active entries — decayed entry should be excluded.
        let activeAfterRollback = try await knowledgeStore.activeEntries(forPodcast: "pod-life")
        let lexiconAfter = CompiledSponsorLexicon(entries: activeAfterRollback)
        #expect(lexiconAfter.entryCount == 0, "Decayed entry should not appear in active entries")

        let scanner2 = LexicalScanner(compiledLexicon: lexiconAfter)
        let hitsAfter = scanner2.scanChunk(chunk)
        let matchesAfter = hitsAfter.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("betterhelp")
        }
        #expect(matchesAfter.isEmpty, "Decayed entry should not produce scanner hits after recompile")
    }

    @Test("Full-batch scan produces LexicalCandidates from compiled lexicon")
    func fullBatchScanProducesCandidates() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Build active entry.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-batch",
                entityType: .sponsor,
                entityValue: "Athletic Greens",
                analysisAssetId: "asset-batch-\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        let active = try await knowledgeStore.activeEntries(forPodcast: "pod-batch")
        let lexicon = CompiledSponsorLexicon(entries: active)
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        // Chunk with sponsor mention plus a built-in pattern hit for the merge.
        let chunks = [
            makeChunk(
                text: "This episode is brought to you by Athletic Greens",
                assetId: "asset-batch-scan",
                startTime: 0.0,
                endTime: 15.0
            ),
        ]

        let candidates = scanner.scan(chunks: chunks, analysisAssetId: "asset-batch-scan")
        #expect(!candidates.isEmpty, "Compiled lexicon + built-in patterns should produce candidates")
    }
}

// MARK: - Multi-Entity Type Integration

@Suite("Phase 8 Integration — Multi-Entity Types")
struct Phase8MultiEntityTests {

    @Test("All entity types compile and scan correctly")
    func allEntityTypesCompileAndScan() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Record active entries for each entity type.
        let entities: [(KnowledgeEntityType, String)] = [
            (.sponsor, "Squarespace"),
            (.cta, "use code podcast"),
            (.url, "squarespace.com/offer"),
            (.disclosure, "brought to you by squarespace"),
        ]

        for (entityType, value) in entities {
            for i in 1...2 {
                try await knowledgeStore.recordCandidate(
                    podcastId: "pod-multi",
                    entityType: entityType,
                    entityValue: value,
                    analysisAssetId: "asset-multi-\(entityType.rawValue)-\(i)",
                    sourceAtomOrdinals: [i],
                    transcriptVersion: "tv-\(i)",
                    confidence: 0.85
                )
            }
        }

        let active = try await knowledgeStore.activeEntries(forPodcast: "pod-multi")
        #expect(active.count == 4, "All four entity types should be active")

        let lexicon = CompiledSponsorLexicon(entries: active)
        #expect(lexicon.entryCount == 4, "All four active entries should contribute patterns")

        // Scan a chunk containing multiple entity mentions.
        let scanner = LexicalScanner(compiledLexicon: lexicon)
        let chunk = makeChunk(
            text: "Squarespace is great and use code podcast at squarespace.com/offer"
        )
        let hits = scanner.scanChunk(chunk)

        let sponsorHits = hits.filter { $0.category == .sponsor }
        // Compiled lexicon hits all use .sponsor category regardless of entity type.
        #expect(sponsorHits.count >= 2, "Multiple entity type patterns should produce hits")
    }
}

// MARK: - Negative Memory Integration

@Suite("Phase 8 Integration — Negative Memory")
struct Phase8NegativeMemoryTests {

    @Test("Correction filters active entry from compiled lexicon via activeEntriesWithNegativeMemory")
    func correctionFiltersFromLexicon() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)

        // Need an asset for the correction FK.
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-neg-int"))

        // Build two active sponsors.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-neg-int",
                entityType: .sponsor,
                entityValue: "BadSponsor",
                analysisAssetId: "asset-neg-int-bad-\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-neg-int",
                entityType: .sponsor,
                entityValue: "GoodSponsor",
                analysisAssetId: "asset-neg-int-good-\(i)",
                sourceAtomOrdinals: [i + 10],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Record correction for BadSponsor.
        let scope = CorrectionScope.sponsorOnShow(podcastId: "pod-neg-int", sponsor: "badsponsor")
        let event = CorrectionEvent(
            analysisAssetId: "asset-neg-int",
            scope: scope.serialized,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        // Build lexicon from negative-memory-filtered entries.
        let filtered = try await knowledgeStore.activeEntriesWithNegativeMemory(
            forPodcast: "pod-neg-int",
            correctionStore: correctionStore
        )
        let lexicon = CompiledSponsorLexicon(entries: filtered)
        #expect(lexicon.entryCount == 1, "Only non-corrected sponsor should remain")

        // Scan — BadSponsor should not produce hits, GoodSponsor should.
        let scanner = LexicalScanner(compiledLexicon: lexicon)
        let chunk = makeChunk(text: "BadSponsor and GoodSponsor both appear here")
        let hits = scanner.scanChunk(chunk)

        let badHits = hits.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("badsponsor")
        }
        let goodHits = hits.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("goodsponsor")
        }

        #expect(badHits.isEmpty, "Corrected sponsor should not produce hits")
        #expect(!goodHits.isEmpty, "Non-corrected sponsor should still produce hits")
    }
}

// MARK: - Cross-Episode Confirmation Integration

@Suite("Phase 8 Integration — Cross-Episode Confirmation")
struct Phase8CrossEpisodeTests {

    @Test("Two recordings from different assets promote quarantined to active")
    func crossEpisodeConfirmation() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Episode 1: first mention -> quarantined.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-cross",
            entityType: .sponsor,
            entityValue: "HelloFresh",
            analysisAssetId: "episode-100",
            sourceAtomOrdinals: [5, 6, 7],
            transcriptVersion: "tv-ep100",
            confidence: 0.8
        )

        let afterEp1 = try await knowledgeStore.entry(
            podcastId: "pod-cross",
            entityType: .sponsor,
            normalizedValue: "hellofresh"
        )
        #expect(afterEp1?.state == .quarantined, "First episode should yield quarantined")

        // No hits yet from compiled lexicon (quarantined excluded).
        let activeAfterEp1 = try await knowledgeStore.activeEntries(forPodcast: "pod-cross")
        let lexicon1 = CompiledSponsorLexicon(entries: activeAfterEp1)
        #expect(lexicon1.entryCount == 0, "Quarantined entries should not compile into lexicon")

        // Episode 2: second mention -> active.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-cross",
            entityType: .sponsor,
            entityValue: "HelloFresh",
            analysisAssetId: "episode-101",
            sourceAtomOrdinals: [12, 13],
            transcriptVersion: "tv-ep101",
            confidence: 0.85
        )

        let afterEp2 = try await knowledgeStore.entry(
            podcastId: "pod-cross",
            entityType: .sponsor,
            normalizedValue: "hellofresh"
        )
        #expect(afterEp2?.state == .active, "Second episode should promote to active")

        // Now lexicon should include it.
        let activeAfterEp2 = try await knowledgeStore.activeEntries(forPodcast: "pod-cross")
        let lexicon2 = CompiledSponsorLexicon(entries: activeAfterEp2)
        #expect(lexicon2.entryCount == 1)

        // Scan should find it.
        let scanner = LexicalScanner(compiledLexicon: lexicon2)
        let chunk = makeChunk(text: "HelloFresh delivers fresh ingredients to your door")
        let hits = scanner.scanChunk(chunk)
        let hfHits = hits.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("hellofresh")
        }
        #expect(!hfHits.isEmpty, "Cross-episode promoted entry should produce scanner hits")
    }

    @Test("Provenance events tracked for each episode")
    func provenanceEventsPerEpisode() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Record from two different episodes.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-prov",
            entityType: .sponsor,
            entityValue: "NordVPN",
            analysisAssetId: "ep-200",
            sourceAtomOrdinals: [1, 2],
            transcriptVersion: "tv-200",
            confidence: 0.75
        )
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-prov",
            entityType: .sponsor,
            entityValue: "NordVPN",
            analysisAssetId: "ep-201",
            sourceAtomOrdinals: [5],
            transcriptVersion: "tv-201",
            confidence: 0.9
        )

        let events200 = try await knowledgeStore.candidateEvents(forAsset: "ep-200")
        let events201 = try await knowledgeStore.candidateEvents(forAsset: "ep-201")

        #expect(events200.count == 1, "Episode 200 should have one provenance event")
        #expect(events201.count == 1, "Episode 201 should have one provenance event")
        #expect(events200[0].sourceAtomOrdinals == [1, 2])
        #expect(events201[0].sourceAtomOrdinals == [5])
    }
}

// MARK: - Compiled Lexicon State Exclusion Integration

@Suite("Phase 8 Integration — Compiled Lexicon Excludes Non-Active")
struct Phase8LexiconExclusionTests {

    @Test("Compiled lexicon excludes decayed and blocked entries from store")
    func excludesDecayedAndBlocked() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Create active entry for Sponsor A.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-excl",
                entityType: .sponsor,
                entityValue: "ActiveSponsor",
                analysisAssetId: "asset-excl-a-\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Create and decay Sponsor B.
        for i in 1...3 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-excl",
                entityType: .sponsor,
                entityValue: "DecayedSponsor",
                analysisAssetId: "asset-excl-b-\(i)",
                sourceAtomOrdinals: [i + 10],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }
        for _ in 1...4 {
            try await knowledgeStore.recordRollback(
                podcastId: "pod-excl",
                entityType: .sponsor,
                entityValue: "DecayedSponsor"
            )
        }

        // Create and block Sponsor C.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-excl",
                entityType: .sponsor,
                entityValue: "BlockedSponsor",
                analysisAssetId: "asset-excl-c-\(i)",
                sourceAtomOrdinals: [i + 20],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }
        try await knowledgeStore.blockEntry(
            podcastId: "pod-excl",
            entityType: .sponsor,
            entityValue: "BlockedSponsor"
        )

        // Verify states.
        let allEntries = try await knowledgeStore.allEntries(forPodcast: "pod-excl")
        #expect(allEntries.count == 3, "Should have 3 entries total")
        let states = Set(allEntries.map(\.state))
        #expect(states.contains(.active))
        #expect(states.contains(.decayed))
        #expect(states.contains(.blocked))

        // Compile lexicon from activeEntries.
        let active = try await knowledgeStore.activeEntries(forPodcast: "pod-excl")
        let lexicon = CompiledSponsorLexicon(entries: active)
        #expect(lexicon.entryCount == 1, "Only active entry should be in compiled lexicon")

        // Scan — only ActiveSponsor should hit.
        let scanner = LexicalScanner(compiledLexicon: lexicon)
        let chunk = makeChunk(
            text: "ActiveSponsor and DecayedSponsor and BlockedSponsor all mentioned"
        )
        let hits = scanner.scanChunk(chunk)

        let activeHits = hits.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("activesponsor")
        }
        let decayedHits = hits.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("decayedsponsor")
        }
        let blockedHits = hits.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("blockedsponsor")
        }

        #expect(!activeHits.isEmpty, "Active entry should produce hits")
        #expect(decayedHits.isEmpty, "Decayed entry should not produce hits")
        #expect(blockedHits.isEmpty, "Blocked entry should not produce hits")
    }
}

// MARK: - Alias Matching Integration

@Suite("Phase 8 Integration — Alias Matching")
struct Phase8AliasMatchingTests {

    @Test("Alias text in transcript produces scanner hits")
    func aliasMatchesInTranscript() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Record with initial name.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-alias-int",
            entityType: .sponsor,
            entityValue: "Squarespace",
            analysisAssetId: "asset-alias-1",
            sourceAtomOrdinals: [1],
            transcriptVersion: "tv-1",
            confidence: 0.9
        )

        // Record with different casing — adds alias.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-alias-int",
            entityType: .sponsor,
            entityValue: "SQUARESPACE",
            analysisAssetId: "asset-alias-2",
            sourceAtomOrdinals: [2],
            transcriptVersion: "tv-2",
            confidence: 0.9
        )

        let entry = try await knowledgeStore.entry(
            podcastId: "pod-alias-int",
            entityType: .sponsor,
            normalizedValue: "squarespace"
        )
        #expect(entry?.state == .active)
        #expect(entry?.aliases.contains("SQUARESPACE") == true)

        // Compile and scan — the alias (normalized) should also match.
        let active = try await knowledgeStore.activeEntries(forPodcast: "pod-alias-int")
        let lexicon = CompiledSponsorLexicon(entries: active)
        // Should have patterns for both "squarespace" (normalized value) and
        // "squarespace" (alias normalized) — but they're the same, so deduped to 1.
        #expect(lexicon.patterns.count >= 1)

        let scanner = LexicalScanner(compiledLexicon: lexicon)
        let chunk = makeChunk(text: "Check out Squarespace for your website needs")
        let hits = scanner.scanChunk(chunk)

        let sqHits = hits.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("squarespace")
        }
        #expect(!sqHits.isEmpty, "Sponsor name should match via compiled lexicon patterns")
    }
}

// MARK: - Full Lifecycle Round-Trip

@Suite("Phase 8 Integration — Full Lifecycle Round-Trip")
struct Phase8LifecycleRoundTripTests {

    @Test("candidate -> quarantined -> active -> decayed -> blocked through SQLite")
    func fullLifecycleRoundTrip() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Step 1: candidate -> quarantined (first confirmation).
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-rt",
            entityType: .sponsor,
            entityValue: "LifecycleSponsor",
            analysisAssetId: "asset-rt-1",
            sourceAtomOrdinals: [1],
            transcriptVersion: "tv-1",
            confidence: 0.8
        )
        let step1 = try await knowledgeStore.entry(
            podcastId: "pod-rt",
            entityType: .sponsor,
            normalizedValue: "lifecyclesponsor"
        )
        #expect(step1?.state == .quarantined, "Step 1: should be quarantined")
        #expect(step1?.confirmationCount == 1)

        // Step 2: quarantined -> active (second confirmation).
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-rt",
            entityType: .sponsor,
            entityValue: "LifecycleSponsor",
            analysisAssetId: "asset-rt-2",
            sourceAtomOrdinals: [2],
            transcriptVersion: "tv-2",
            confidence: 0.85
        )
        let step2 = try await knowledgeStore.entry(
            podcastId: "pod-rt",
            entityType: .sponsor,
            normalizedValue: "lifecyclesponsor"
        )
        #expect(step2?.state == .active, "Step 2: should be active")
        #expect(step2?.confirmationCount == 2)

        // Verify it appears in active entries and compiles.
        let activeStep2 = try await knowledgeStore.activeEntries(forPodcast: "pod-rt")
        #expect(activeStep2.count == 1)
        let lexiconStep2 = CompiledSponsorLexicon(entries: activeStep2)
        #expect(lexiconStep2.entryCount == 1)

        // Step 3: active -> decayed (rollback spike).
        // 2 confirmations + 3 rollbacks = 3/5 = 60% rollback rate (> 50%).
        for _ in 1...3 {
            try await knowledgeStore.recordRollback(
                podcastId: "pod-rt",
                entityType: .sponsor,
                entityValue: "LifecycleSponsor"
            )
        }
        let step3 = try await knowledgeStore.entry(
            podcastId: "pod-rt",
            entityType: .sponsor,
            normalizedValue: "lifecyclesponsor"
        )
        #expect(step3?.state == .decayed, "Step 3: should be decayed")
        #expect(step3?.rollbackCount == 3)
        #expect(step3?.decayedAt != nil)

        // Verify decayed is excluded from active entries.
        let activeStep3 = try await knowledgeStore.activeEntries(forPodcast: "pod-rt")
        #expect(activeStep3.isEmpty)

        // Step 4: explicitly block the decayed entry.
        try await knowledgeStore.blockEntry(
            podcastId: "pod-rt",
            entityType: .sponsor,
            entityValue: "LifecycleSponsor"
        )
        let step4 = try await knowledgeStore.entry(
            podcastId: "pod-rt",
            entityType: .sponsor,
            normalizedValue: "lifecyclesponsor"
        )
        #expect(step4?.state == .blocked, "Step 4: should be blocked")
        #expect(step4?.blockedAt != nil)

        // Verify blocked cannot be promoted even with new confirmations.
        try await knowledgeStore.recordCandidate(
            podcastId: "pod-rt",
            entityType: .sponsor,
            entityValue: "LifecycleSponsor",
            analysisAssetId: "asset-rt-3",
            sourceAtomOrdinals: [3],
            transcriptVersion: "tv-3",
            confidence: 0.95
        )
        let step4b = try await knowledgeStore.entry(
            podcastId: "pod-rt",
            entityType: .sponsor,
            normalizedValue: "lifecyclesponsor"
        )
        #expect(step4b?.state == .blocked, "Blocked entry must not be promoted")

        // Verify blocked entry never appears in compiled lexicon.
        let allEntries = try await knowledgeStore.allEntries(forPodcast: "pod-rt")
        let lexiconFinal = CompiledSponsorLexicon(entries: allEntries)
        #expect(lexiconFinal.entryCount == 0, "Blocked entry should not compile into lexicon")
    }

    @Test("Rollback triggers decay — verified through store and compiled lexicon")
    func rollbackTriggersDecay() async throws {
        let analysisStore = try await makeTestStore()
        let knowledgeStore = SponsorKnowledgeStore(store: analysisStore)

        // Build active entry with 2 confirmations.
        for i in 1...2 {
            try await knowledgeStore.recordCandidate(
                podcastId: "pod-decay",
                entityType: .sponsor,
                entityValue: "DecayTarget",
                analysisAssetId: "asset-decay-\(i)",
                sourceAtomOrdinals: [i],
                transcriptVersion: "tv-\(i)",
                confidence: 0.9
            )
        }

        // Verify active and scannable.
        let activeBefore = try await knowledgeStore.activeEntries(forPodcast: "pod-decay")
        #expect(activeBefore.count == 1)
        let lexBefore = CompiledSponsorLexicon(entries: activeBefore)
        let scannerBefore = LexicalScanner(compiledLexicon: lexBefore)
        let chunk = makeChunk(text: "DecayTarget is mentioned here")
        let hitsBefore = scannerBefore.scanChunk(chunk)
        #expect(hitsBefore.contains { $0.matchedText.lowercased().contains("decaytarget") })

        // Record rollbacks to trigger decay.
        for _ in 1...3 {
            try await knowledgeStore.recordRollback(
                podcastId: "pod-decay",
                entityType: .sponsor,
                entityValue: "DecayTarget"
            )
        }

        // Verify decayed.
        let entry = try await knowledgeStore.entry(
            podcastId: "pod-decay",
            entityType: .sponsor,
            normalizedValue: "decaytarget"
        )
        #expect(entry?.state == .decayed)

        // Verify not in active set and not scannable.
        let activeAfter = try await knowledgeStore.activeEntries(forPodcast: "pod-decay")
        #expect(activeAfter.isEmpty)
        let lexAfter = CompiledSponsorLexicon(entries: activeAfter)
        let scannerAfter = LexicalScanner(compiledLexicon: lexAfter)
        let hitsAfter = scannerAfter.scanChunk(chunk)
        let decayHits = hitsAfter.filter {
            $0.category == .sponsor && $0.matchedText.lowercased().contains("decaytarget")
        }
        #expect(decayHits.isEmpty, "Decayed entry should not produce scanner hits")
    }
}
