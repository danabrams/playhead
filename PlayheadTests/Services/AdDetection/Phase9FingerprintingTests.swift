// Phase9FingerprintingTests.swift
// Phase 9 (playhead-4my.9.2): Tests for fingerprint generation, near-duplicate
// matching, lifecycle transitions, evidence integration, and provenance tracking.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeAtom(
    assetId: String = "asset-fp",
    ordinal: Int,
    text: String,
    startTime: Double,
    endTime: Double
) -> TranscriptAtom {
    TranscriptAtom(
        atomKey: TranscriptAtomKey(
            analysisAssetId: assetId,
            transcriptVersion: "tv-fp",
            atomOrdinal: ordinal
        ),
        contentHash: "deadbeef",
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: ordinal
    )
}

private func makeTestSpan(
    assetId: String = "asset-fp",
    firstOrdinal: Int = 0,
    lastOrdinal: Int = 10,
    startTime: Double = 0.0,
    endTime: Double = 30.0,
    anchorProvenance: [AnchorRef] = []
) -> DecodedSpan {
    DecodedSpan(
        id: DecodedSpan.makeId(
            assetId: assetId,
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal
        ),
        assetId: assetId,
        firstAtomOrdinal: firstOrdinal,
        lastAtomOrdinal: lastOrdinal,
        startTime: startTime,
        endTime: endTime,
        anchorProvenance: anchorProvenance
    )
}

// MARK: - Fingerprint Generation

@Suite("Phase 9 — Fingerprint Generation")
struct FingerprintGenerationTests {

    @Test("Normalization strips punctuation, filler words, and lowercases")
    func normalization() {
        let input = "Um, this is ACTUALLY a test! You know, like, basically."
        let result = MinHashUtilities.normalizeText(input)
        // Filler word removal is per-word: "um", "actually", "like", "basically" removed.
        // "you" and "know" are not filler words individually.
        #expect(result == "this is a test you know")
    }

    @Test("Normalization preserves alphanumeric content")
    func normalizationPreservesContent() {
        let input = "Squarespace offers 20 percent off"
        let result = MinHashUtilities.normalizeText(input)
        #expect(result == "squarespace offers 20 percent off")
    }

    @Test("4-gram generation produces correct features")
    func ngramGeneration() {
        let ngrams = MinHashUtilities.generateNgrams("abcdef")
        #expect(ngrams.contains("abcd"))
        #expect(ngrams.contains("bcde"))
        #expect(ngrams.contains("cdef"))
        #expect(ngrams.count == 3)
    }

    @Test("Short text returns text itself as single feature")
    func shortTextNgrams() {
        let ngrams = MinHashUtilities.generateNgrams("abc")
        #expect(ngrams == ["abc"])
    }

    @Test("Empty text produces empty ngrams")
    func emptyNgrams() {
        let ngrams = MinHashUtilities.generateNgrams("")
        #expect(ngrams.isEmpty)
    }

    @Test("MinHash is deterministic")
    func minHashDeterministic() {
        let text = "this is a test advertisement for squarespace"
        let ngrams = MinHashUtilities.generateNgrams(text)
        let hash1 = MinHashUtilities.computeMinHash(features: ngrams)
        let hash2 = MinHashUtilities.computeMinHash(features: ngrams)
        #expect(hash1 == hash2)
    }

    @Test("MinHash produces 128 hash values")
    func minHashSize() {
        let ngrams = MinHashUtilities.generateNgrams("test fingerprint")
        let hash = MinHashUtilities.computeMinHash(features: ngrams)
        #expect(hash.count == 128)
    }

    @Test("Different text produces different fingerprints")
    func differentTextDifferentHash() {
        let hash1 = MinHashUtilities.generateFingerprint(from: "squarespace website builder premium")
        let hash2 = MinHashUtilities.generateFingerprint(from: "betterhelp online therapy sessions")
        #expect(hash1 != hash2)
    }

    @Test("Hex encode/decode round-trips")
    func hexRoundTrip() {
        let ngrams = MinHashUtilities.generateNgrams("round trip test")
        let original = MinHashUtilities.computeMinHash(features: ngrams)
        let encoded = MinHashUtilities.encodeSignature(original)
        let decoded = MinHashUtilities.decodeSignature(encoded)
        #expect(decoded == original)
    }

    @Test("Unicode text handled correctly")
    func unicodeHandling() {
        let text1 = "café résumé naïve"
        let result1 = MinHashUtilities.normalizeText(text1)
        #expect(!result1.isEmpty)

        let hash1 = MinHashUtilities.generateFingerprint(from: text1)
        let hash2 = MinHashUtilities.generateFingerprint(from: text1)
        #expect(hash1 == hash2, "Unicode text should produce deterministic fingerprints")
    }

    @Test("Empty/whitespace-only text produces no usable fingerprint via store")
    func emptyTextNormalization() {
        let normalized = MinHashUtilities.normalizeText("   ")
        #expect(normalized.isEmpty)
    }

    @Test("decodeSignature rejects wrong-length hex")
    func decodeWrongLength() {
        #expect(MinHashUtilities.decodeSignature("deadbeef") == nil)
        #expect(MinHashUtilities.decodeSignature("") == nil)
    }

    @Test("decodeSignature rejects non-hex characters")
    func decodeInvalidHex() {
        // Build a string of the right length (128 * 16 = 2048) but with invalid chars.
        let badHex = String(repeating: "zzzzzzzzzzzzzzzz", count: 128)
        #expect(MinHashUtilities.decodeSignature(badHex) == nil)
    }

    @Test("decodeSignature accepts valid hex of correct length")
    func decodeValidHex() {
        let validHex = String(repeating: "0000000000000000", count: 128)
        let decoded = MinHashUtilities.decodeSignature(validHex)
        #expect(decoded != nil)
        #expect(decoded?.count == 128)
        #expect(decoded?.allSatisfy { $0 == 0 } == true)
    }
}

// MARK: - Near-Duplicate Matching

@Suite("Phase 9 — Near-Duplicate Matching")
struct NearDuplicateMatchingTests {

    @Test("Identical signatures produce Jaccard 1.0")
    func identicalSignatures() {
        let hash = MinHashUtilities.computeMinHash(
            features: MinHashUtilities.generateNgrams("identical text for matching")
        )
        let similarity = MinHashUtilities.jaccardSimilarity(hash, hash)
        #expect(similarity == 1.0)
    }

    @Test("Same ad script with slight word variation matches (Jaccard >= 0.6)")
    func slightVariationMatches() {
        let text1 = "squarespace is the all in one website platform for entrepreneurs to stand out online and grow your business"
        let text2 = "squarespace is the all in one website platform for creators to stand out online and grow their business"
        let hash1 = MinHashUtilities.computeMinHash(features: MinHashUtilities.generateNgrams(
            MinHashUtilities.normalizeText(text1)))
        let hash2 = MinHashUtilities.computeMinHash(features: MinHashUtilities.generateNgrams(
            MinHashUtilities.normalizeText(text2)))
        let similarity = MinHashUtilities.jaccardSimilarity(hash1, hash2)
        #expect(similarity >= 0.6, "Similar ad scripts should match: got \(similarity)")
    }

    @Test("Same script with different filler words matches")
    func fillerWordVariation() {
        let text1 = "Um, basically, Squarespace is, you know, the best website builder"
        let text2 = "Like, actually, Squarespace is, I mean, the best website builder"
        let hash1 = MinHashUtilities.computeMinHash(features: MinHashUtilities.generateNgrams(
            MinHashUtilities.normalizeText(text1)))
        let hash2 = MinHashUtilities.computeMinHash(features: MinHashUtilities.generateNgrams(
            MinHashUtilities.normalizeText(text2)))
        let similarity = MinHashUtilities.jaccardSimilarity(hash1, hash2)
        #expect(similarity >= 0.6, "Filler word differences should not prevent matching: got \(similarity)")
    }

    @Test("Completely different content does not match")
    func differentContentNoMatch() {
        let text1 = "squarespace is the all in one website platform for entrepreneurs"
        let text2 = "the weather forecast shows rain tomorrow with temperatures dropping to the low fifties"
        let hash1 = MinHashUtilities.computeMinHash(features: MinHashUtilities.generateNgrams(
            MinHashUtilities.normalizeText(text1)))
        let hash2 = MinHashUtilities.computeMinHash(features: MinHashUtilities.generateNgrams(
            MinHashUtilities.normalizeText(text2)))
        let similarity = MinHashUtilities.jaccardSimilarity(hash1, hash2)
        #expect(similarity < 0.6, "Completely different content should not match: got \(similarity)")
    }

    @Test("Empty features produce UInt64.max signature")
    func emptyFeatures() {
        let hash = MinHashUtilities.computeMinHash(features: [])
        #expect(hash.allSatisfy { $0 == UInt64.max })
    }

    @Test("Mismatched signature lengths return 0.0")
    func mismatchedLengths() {
        let similarity = MinHashUtilities.jaccardSimilarity([1, 2, 3], [1, 2])
        #expect(similarity == 0.0)
    }

    @Test("Empty signatures return 0.0")
    func emptySignatures() {
        let similarity = MinHashUtilities.jaccardSimilarity([], [])
        #expect(similarity == 0.0)
    }
}

// MARK: - Lifecycle Transitions

@Suite("Phase 9 — Lifecycle Transitions")
struct LifecycleTransitionTests {

    @Test("New fingerprint starts as candidate via recordCandidate")
    func newCandidateState() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        try await store.recordCandidate(
            podcastId: "pod-lc",
            text: "this is a test ad for squarespace website builder premium plans",
            analysisAssetId: "asset-1",
            sourceAdWindowId: "window-1",
            confidence: 0.8
        )

        let entries = try await store.allEntries(forPodcast: "pod-lc")
        #expect(entries.count == 1)
        // First recordCandidate: confirmationCount=1 → promotes candidate→quarantined
        #expect(entries[0].state == .quarantined)
        #expect(entries[0].confirmationCount == 1)
    }

    @Test("Second confirmation with low rollback promotes to active")
    func promoteToActive() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        for i in 1...2 {
            try await store.recordCandidate(
                podcastId: "pod-lc",
                text: "this is a test ad for squarespace website builder premium plans",
                analysisAssetId: "asset-\(i)",
                sourceAdWindowId: "window-\(i)",
                confidence: 0.8
            )
        }

        let entries = try await store.allEntries(forPodcast: "pod-lc")
        #expect(entries.count == 1)
        #expect(entries[0].state == .active)
        #expect(entries[0].confirmationCount == 2)
    }

    @Test("Only active fingerprints are returned by activeEntries")
    func onlyActiveReturned() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        // Create one entry that stays quarantined (1 confirmation)
        try await store.recordCandidate(
            podcastId: "pod-lc",
            text: "quarantined ad script for betterhelp online therapy sessions",
            analysisAssetId: "asset-q1",
            sourceAdWindowId: "window-q1",
            confidence: 0.8
        )

        // Create one entry that reaches active (2 confirmations)
        for i in 1...2 {
            try await store.recordCandidate(
                podcastId: "pod-lc",
                text: "active ad script for athletic greens supplement daily nutrition",
                analysisAssetId: "asset-a\(i)",
                sourceAdWindowId: "window-a\(i)",
                confidence: 0.8
            )
        }

        let active = try await store.activeEntries(forPodcast: "pod-lc")
        #expect(active.count == 1)
        #expect(active[0].normalizedText.contains("athletic"))
    }

    @Test("Rollback spike demotes active to decayed")
    func rollbackDecay() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        // Build to active (2 confirmations).
        let adText = "this is a test ad for squarespace website builder premium plans"
        for i in 1...2 {
            try await store.recordCandidate(
                podcastId: "pod-lc",
                text: adText,
                analysisAssetId: "asset-\(i)",
                sourceAdWindowId: "window-\(i)",
                confidence: 0.8
            )
        }

        let before = try await store.allEntries(forPodcast: "pod-lc")
        #expect(before[0].state == .active)
        let fingerprintHash = before[0].fingerprintHash

        // Rollback enough to trigger spike (3 rollbacks on 2 confirmations = 60% > 50%)
        for _ in 1...3 {
            try await store.recordRollback(
                podcastId: "pod-lc",
                fingerprintHash: fingerprintHash
            )
        }

        let after = try await store.allEntries(forPodcast: "pod-lc")
        #expect(after[0].state == .decayed)
        #expect(after[0].rollbackCount == 3)
    }

    @Test("Decayed entry recovers to active with new confirmations")
    func decayedRecovery() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let adText = "this is a test ad for squarespace website builder premium plans"

        // Build to active.
        for i in 1...2 {
            try await store.recordCandidate(
                podcastId: "pod-lc",
                text: adText,
                analysisAssetId: "asset-\(i)",
                sourceAdWindowId: "window-\(i)",
                confidence: 0.8
            )
        }
        let entry = try await store.allEntries(forPodcast: "pod-lc")[0]
        #expect(entry.state == .active)

        // Decay it with rollbacks.
        for _ in 1...3 {
            try await store.recordRollback(
                podcastId: "pod-lc",
                fingerprintHash: entry.fingerprintHash
            )
        }
        let decayed = try await store.allEntries(forPodcast: "pod-lc")[0]
        #expect(decayed.state == .decayed)

        // Re-confirm to recover. Need enough confirmations to get RR back below 0.3.
        // Currently: 2 conf, 3 roll. Need RR <= 0.3 and conf >= 2.
        // After 8 more confirmations: 10 conf, 3 roll → RR = 0.23 < 0.3 ✓
        for i in 3...10 {
            try await store.recordCandidate(
                podcastId: "pod-lc",
                text: adText,
                analysisAssetId: "asset-recovery-\(i)",
                sourceAdWindowId: "window-recovery-\(i)",
                confidence: 0.8
            )
        }

        let recovered = try await store.allEntries(forPodcast: "pod-lc")[0]
        #expect(recovered.state == .active, "Decayed entry should recover to active: got \(recovered.state)")
    }

    @Test("Blocked entry is skipped during near-duplicate and exact-match confirmation")
    func blockedEntrySkippedOnConfirm() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let adText = "this is a premium ad for our sponsor acme widgets and more"

        // Confirm once to create the entry as quarantined.
        try await store.recordCandidate(
            podcastId: "pod-blocked",
            text: adText,
            analysisAssetId: "asset-b1",
            sourceAdWindowId: "window-b1",
            confidence: 0.9
        )

        // Drive the entry to blocked via many rollbacks (spike > 0.5).
        for i in 1...5 {
            let entries = try await store.allEntries(forPodcast: "pod-blocked")
            if let entry = entries.first {
                try await store.recordRollback(
                    podcastId: "pod-blocked",
                    fingerprintHash: entry.fingerprintHash
                )
            }
        }

        let blockedEntries = try await store.allEntries(forPodcast: "pod-blocked")
        #expect(blockedEntries.count == 1)
        #expect(blockedEntries[0].state == .blocked, "Entry should be blocked after rollback spike")
        let blockedCount = blockedEntries[0].confirmationCount

        // Now re-confirm with the exact same text. The blocked entry should be
        // returned as-is without incrementing confirmations — terminal states
        // are inert.
        try await store.recordCandidate(
            podcastId: "pod-blocked",
            text: adText,
            analysisAssetId: "asset-b2",
            sourceAdWindowId: "window-b2",
            confidence: 0.9
        )

        let afterEntries = try await store.allEntries(forPodcast: "pod-blocked")
        // Still just 1 entry — the blocked one, unchanged.
        #expect(afterEntries.count == 1, "Blocked entry returned as-is: got \(afterEntries.count)")
        #expect(afterEntries[0].state == .blocked)
        #expect(afterEntries[0].confirmationCount == blockedCount,
                "Blocked entry should not have accumulated extra confirmations")
    }

    @Test("Empty store returns no matches")
    func emptyStoreNoMatches() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let active = try await store.activeEntries(forPodcast: "pod-empty")
        #expect(active.isEmpty)
    }

    @Test("Low confidence candidates are skipped")
    func lowConfidenceSkipped() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        try await store.recordCandidate(
            podcastId: "pod-lc",
            text: "low confidence ad text for testing",
            analysisAssetId: "asset-low",
            sourceAdWindowId: "window-low",
            confidence: 0.3  // Below minCandidateConfidence (0.5)
        )

        let entries = try await store.allEntries(forPodcast: "pod-lc")
        #expect(entries.isEmpty, "Low confidence should not create an entry")
    }

    @Test("Rollback on nonexistent entry is a no-op")
    func rollbackNonexistent() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        // Should not throw or create any side effects.
        try await store.recordRollback(
            podcastId: "pod-noexist",
            fingerprintHash: "nonexistent-hash"
        )

        let entries = try await store.allEntries(forPodcast: "pod-noexist")
        #expect(entries.isEmpty, "Rollback on nonexistent entry should not create entries")
    }

    @Test("Near-duplicate ad reads coalesce into one entry")
    func nearDuplicateCoalescing() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        // Two slightly different reads of the same ad script.
        let text1 = "squarespace is the all in one website platform for entrepreneurs to stand out online and grow your business visit squarespace dot com"
        let text2 = "squarespace is the all in one website platform for creators to stand out online and grow their business visit squarespace dot com"

        try await store.recordCandidate(
            podcastId: "pod-dup",
            text: text1,
            analysisAssetId: "asset-1",
            sourceAdWindowId: "window-1",
            confidence: 0.9
        )
        try await store.recordCandidate(
            podcastId: "pod-dup",
            text: text2,
            analysisAssetId: "asset-2",
            sourceAdWindowId: "window-2",
            confidence: 0.9
        )

        let entries = try await store.allEntries(forPodcast: "pod-dup")
        #expect(entries.count == 1, "Near-duplicate texts should coalesce into one entry, got \(entries.count)")
        #expect(entries[0].confirmationCount == 2, "Coalesced entry should have 2 confirmations")
    }

    @Test("Near-duplicate provenance events use resolved hash")
    func nearDuplicateProvenanceHash() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let text1 = "squarespace is the all in one website platform for entrepreneurs to stand out online and grow your business visit squarespace dot com"
        let text2 = "squarespace is the all in one website platform for creators to stand out online and grow their business visit squarespace dot com"

        try await store.recordCandidate(
            podcastId: "pod-prov-dup",
            text: text1,
            analysisAssetId: "asset-prov-1",
            sourceAdWindowId: "window-prov-1",
            confidence: 0.9
        )
        try await store.recordCandidate(
            podcastId: "pod-prov-dup",
            text: text2,
            analysisAssetId: "asset-prov-2",
            sourceAdWindowId: "window-prov-2",
            confidence: 0.9
        )

        let entries = try await store.allEntries(forPodcast: "pod-prov-dup")
        #expect(entries.count == 1)
        let storedHash = entries[0].fingerprintHash

        // Both provenance events should reference the stored entry's hash.
        let events1 = try await store.sourceEvents(forAsset: "asset-prov-1")
        let events2 = try await store.sourceEvents(forAsset: "asset-prov-2")
        #expect(events1.count == 1)
        #expect(events2.count == 1)
        #expect(events1[0].fingerprintHash == storedHash,
                "First event hash should match stored entry")
        #expect(events2[0].fingerprintHash == storedHash,
                "Near-duplicate event hash should match stored entry, not raw input hash")
    }
}

// MARK: - Store CRUD

@Suite("Phase 9 — AnalysisStore Fingerprint CRUD")
struct FingerprintCRUDTests {

    @Test("upsertFingerprintEntry round-trips correctly")
    func upsertRoundTrip() async throws {
        let store = try await makeTestStore()
        let entry = FingerprintEntry(
            id: "test-id",
            podcastId: "pod-crud",
            fingerprintHash: "deadbeef",
            normalizedText: "test text",
            state: .candidate,
            confirmationCount: 1,
            rollbackCount: 0,
            firstSeenAt: 1000.0,
            lastConfirmedAt: 1000.0
        )
        try await store.upsertFingerprintEntry(entry)

        let loaded = try await store.loadFingerprintEntry(podcastId: "pod-crud", fingerprintHash: "deadbeef")
        #expect(loaded != nil)
        #expect(loaded?.id == "test-id")
        #expect(loaded?.podcastId == "pod-crud")
        #expect(loaded?.normalizedText == "test text")
        #expect(loaded?.state == .candidate)
        #expect(loaded?.confirmationCount == 1)
    }

    @Test("loadFingerprintEntries filters by state")
    func filterByState() async throws {
        let store = try await makeTestStore()

        let candidate = FingerprintEntry(
            podcastId: "pod-filter", fingerprintHash: "hash-c",
            normalizedText: "candidate", state: .candidate, confirmationCount: 1
        )
        let active = FingerprintEntry(
            podcastId: "pod-filter", fingerprintHash: "hash-a",
            normalizedText: "active", state: .active, confirmationCount: 3
        )
        try await store.upsertFingerprintEntry(candidate)
        try await store.upsertFingerprintEntry(active)

        let candidates = try await store.loadFingerprintEntries(podcastId: "pod-filter", state: .candidate)
        #expect(candidates.count == 1)
        #expect(candidates[0].fingerprintHash == "hash-c")

        let actives = try await store.loadFingerprintEntries(podcastId: "pod-filter", state: .active)
        #expect(actives.count == 1)
        #expect(actives[0].fingerprintHash == "hash-a")
    }

    @Test("loadAllFingerprintEntries returns all states")
    func loadAll() async throws {
        let store = try await makeTestStore()

        for (i, state) in [KnowledgeState.candidate, .quarantined, .active].enumerated() {
            let entry = FingerprintEntry(
                podcastId: "pod-all", fingerprintHash: "hash-\(i)",
                normalizedText: "text-\(i)", state: state, confirmationCount: i + 1
            )
            try await store.upsertFingerprintEntry(entry)
        }

        let all = try await store.loadAllFingerprintEntries(podcastId: "pod-all")
        #expect(all.count == 3)
    }

    @Test("upsert with same natural key updates existing entry")
    func upsertUpdate() async throws {
        let store = try await makeTestStore()

        let v1 = FingerprintEntry(
            id: "id-1", podcastId: "pod-up", fingerprintHash: "hash-up",
            normalizedText: "v1", state: .candidate, confirmationCount: 1
        )
        try await store.upsertFingerprintEntry(v1)

        let v2 = FingerprintEntry(
            id: "id-2", podcastId: "pod-up", fingerprintHash: "hash-up",
            normalizedText: "v2", state: .active, confirmationCount: 3
        )
        try await store.upsertFingerprintEntry(v2)

        let loaded = try await store.loadFingerprintEntry(podcastId: "pod-up", fingerprintHash: "hash-up")
        #expect(loaded?.state == .active)
        #expect(loaded?.confirmationCount == 3)
        #expect(loaded?.normalizedText == "v2")
    }
}

// MARK: - Provenance Events

@Suite("Phase 9 — Provenance Events")
struct ProvenanceEventTests {

    @Test("appendFingerprintSourceEvent round-trips correctly")
    func sourceEventRoundTrip() async throws {
        let store = try await makeTestStore()

        let event = FingerprintSourceEvent(
            id: "evt-1",
            analysisAssetId: "asset-prov",
            fingerprintHash: "hash-prov",
            sourceAdWindowId: "window-prov",
            confidence: 0.85,
            createdAt: 1000.0
        )
        try await store.appendFingerprintSourceEvent(event)

        let loaded = try await store.loadFingerprintSourceEvents(analysisAssetId: "asset-prov")
        #expect(loaded.count == 1)
        #expect(loaded[0].id == "evt-1")
        #expect(loaded[0].fingerprintHash == "hash-prov")
        #expect(loaded[0].sourceAdWindowId == "window-prov")
        #expect(loaded[0].confidence == 0.85)
    }

    @Test("Duplicate source events are handled gracefully (INSERT OR IGNORE)")
    func duplicateEvents() async throws {
        let store = try await makeTestStore()

        let event = FingerprintSourceEvent(
            id: "evt-dup",
            analysisAssetId: "asset-dup",
            fingerprintHash: "hash-dup",
            sourceAdWindowId: "window-dup",
            confidence: 0.9
        )
        try await store.appendFingerprintSourceEvent(event)
        try await store.appendFingerprintSourceEvent(event) // Should not throw

        let loaded = try await store.loadFingerprintSourceEvents(analysisAssetId: "asset-dup")
        #expect(loaded.count == 1, "Duplicate event should be ignored")
    }

    @Test("Source events logged during recordCandidate")
    func provenanceFromRecordCandidate() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        try await fpStore.recordCandidate(
            podcastId: "pod-prov",
            text: "ad script for provenance test tracking source events",
            analysisAssetId: "asset-prov-rc",
            sourceAdWindowId: "window-prov-rc",
            confidence: 0.85
        )

        let events = try await fpStore.sourceEvents(forAsset: "asset-prov-rc")
        #expect(events.count == 1)
        #expect(events[0].sourceAdWindowId == "window-prov-rc")
        #expect(events[0].confidence == 0.85)
    }

    @Test("Multiple events loadable by analysisAssetId")
    func multipleEventsPerAsset() async throws {
        let store = try await makeTestStore()

        for i in 1...3 {
            let event = FingerprintSourceEvent(
                analysisAssetId: "asset-multi",
                fingerprintHash: "hash-\(i)",
                sourceAdWindowId: "window-\(i)",
                confidence: Double(i) * 0.1
            )
            try await store.appendFingerprintSourceEvent(event)
        }

        let loaded = try await store.loadFingerprintSourceEvents(analysisAssetId: "asset-multi")
        #expect(loaded.count == 3)
    }
}

// MARK: - Matcher Integration

@Suite("Phase 9 — Matcher Integration")
struct MatcherIntegrationTests {

    @Test("Legacy stub match(atoms:) still returns empty")
    func legacyStubEmpty() {
        let atoms = (0..<5).map { i in
            makeAtom(ordinal: i, text: "word\(i)", startTime: Double(i), endTime: Double(i + 1))
        }
        let result = AdCopyFingerprintMatcher.match(atoms: atoms)
        #expect(result.isEmpty)
    }

    @Test("Store-backed match with active fingerprints returns matches")
    func storeBackedMatch() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        // Build a known ad script and promote to active.
        let adScript = "squarespace is the all in one website platform for entrepreneurs to stand out online and grow your business visit squarespace dot com slash podcast for a free trial"
        for i in 1...2 {
            try await fpStore.recordCandidate(
                podcastId: "pod-match",
                text: adScript,
                analysisAssetId: "asset-\(i)",
                sourceAdWindowId: "window-\(i)",
                confidence: 0.9
            )
        }

        let active = try await fpStore.activeEntries(forPodcast: "pod-match")
        #expect(active.count == 1, "Should have one active entry")

        // Build atoms with the same text (split into words → atoms).
        let words = adScript.split(separator: " ").map(String.init)
        let atoms = words.enumerated().map { (i, word) in
            makeAtom(ordinal: i, text: word, startTime: Double(i), endTime: Double(i + 1))
        }

        let matches = try await AdCopyFingerprintMatcher.match(
            atoms: atoms,
            podcastId: "pod-match",
            fingerprintStore: fpStore
        )

        #expect(!matches.isEmpty, "Should find fingerprint matches against active entries")
        #expect(matches[0].similarity >= 0.6)
    }

    @Test("Store-backed match with no active fingerprints returns empty")
    func noActiveNoMatch() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        // Only one confirmation → quarantined, not active
        try await fpStore.recordCandidate(
            podcastId: "pod-nomatch",
            text: "quarantined ad for betterhelp therapy sessions online counseling",
            analysisAssetId: "asset-1",
            sourceAdWindowId: "window-1",
            confidence: 0.8
        )

        let atoms = (0..<10).map { i in
            makeAtom(ordinal: i, text: "betterhelp", startTime: Double(i), endTime: Double(i + 1))
        }

        let matches = try await AdCopyFingerprintMatcher.match(
            atoms: atoms,
            podcastId: "pod-nomatch",
            fingerprintStore: fpStore
        )

        #expect(matches.isEmpty, "Quarantined entries should not produce matches")
    }

    @Test("Empty atoms returns empty matches")
    func emptyAtomsNoMatch() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        let matches = try await AdCopyFingerprintMatcher.match(
            atoms: [],
            podcastId: "pod-empty",
            fingerprintStore: fpStore
        )
        #expect(matches.isEmpty)
    }

    @Test("Corrupt fingerprint hash in store is skipped without crash")
    func corruptHashResilience() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        // Insert a valid active entry.
        let adScript = "squarespace is the all in one website platform for entrepreneurs to stand out online and grow your business visit squarespace dot com slash podcast"
        for i in 1...2 {
            try await fpStore.recordCandidate(
                podcastId: "pod-corrupt",
                text: adScript,
                analysisAssetId: "asset-\(i)",
                sourceAdWindowId: "window-\(i)",
                confidence: 0.9
            )
        }

        // Directly insert a corrupt entry into the store.
        let corruptEntry = FingerprintEntry(
            podcastId: "pod-corrupt",
            fingerprintHash: "not-valid-hex-garbage",
            normalizedText: "corrupt",
            state: .active,
            confirmationCount: 5
        )
        try await analysisStore.upsertFingerprintEntry(corruptEntry)

        // Matcher should skip the corrupt entry and still find the valid one.
        let words = adScript.split(separator: " ").map(String.init)
        let atoms = words.enumerated().map { (i, word) in
            makeAtom(ordinal: i, text: word, startTime: Double(i), endTime: Double(i + 1))
        }
        let matches = try await AdCopyFingerprintMatcher.match(
            atoms: atoms,
            podcastId: "pod-corrupt",
            fingerprintStore: fpStore
        )
        // Should not crash, and should still find the valid entry.
        #expect(!matches.isEmpty, "Valid fingerprint should still match despite corrupt sibling entry")
    }

    @Test("Overlapping windows for same fingerprint are merged")
    func overlappingWindowsMerge() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        // Build a long ad script and promote to active.
        let adWords = (0..<60).map { "word\($0)" }
        let adScript = adWords.joined(separator: " ")
        for i in 1...2 {
            try await fpStore.recordCandidate(
                podcastId: "pod-merge",
                text: adScript,
                analysisAssetId: "asset-merge-\(i)",
                sourceAdWindowId: "window-merge-\(i)",
                confidence: 0.9
            )
        }

        // Build atoms with the same text.
        let atoms = adWords.enumerated().map { (i, word) in
            makeAtom(ordinal: i, text: word, startTime: Double(i), endTime: Double(i + 1))
        }
        let matches = try await AdCopyFingerprintMatcher.match(
            atoms: atoms,
            podcastId: "pod-merge",
            fingerprintStore: fpStore
        )

        // Multiple sliding windows should match and merge into fewer contiguous spans.
        // With 60 atoms, window=30, stride=10 → 4 windows, all matching the same
        // fingerprint. After merge, they should collapse to 1 contiguous match.
        #expect(matches.count == 1, "Overlapping windows for same fingerprint should merge to 1: got \(matches.count)")
    }

    @Test("Interleaved matches from different fingerprints merge independently")
    func interleavedFingerprintsMergeIndependently() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        // Create two very distinct ad scripts with no shared vocabulary.
        let adWordsA = (0..<60).map { "xylophone\($0)aardvark" }
        let adWordsB = (0..<60).map { "zeppelin\($0)butterfly" }

        // Promote both to active with enough confirmations.
        for i in 1...3 {
            try await fpStore.recordCandidate(
                podcastId: "pod-interleave",
                text: adWordsA.joined(separator: " "),
                analysisAssetId: "asset-il-a\(i)",
                sourceAdWindowId: "window-il-a\(i)",
                confidence: 0.9
            )
            try await fpStore.recordCandidate(
                podcastId: "pod-interleave",
                text: adWordsB.joined(separator: " "),
                analysisAssetId: "asset-il-b\(i)",
                sourceAdWindowId: "window-il-b\(i)",
                confidence: 0.9
            )
        }

        // Build atoms that contain both scripts back-to-back.
        let allWords = adWordsA + adWordsB
        let atoms = allWords.enumerated().map { (i, word) in
            makeAtom(ordinal: i, text: word, startTime: Double(i), endTime: Double(i + 1))
        }
        let matches = try await AdCopyFingerprintMatcher.match(
            atoms: atoms,
            podcastId: "pod-interleave",
            fingerprintStore: fpStore
        )

        // Each fingerprint's windows should merge independently.
        // The grouping-by-fingerprintId fix ensures interleaving in the sorted
        // list doesn't break the merge chain.
        #expect(!matches.isEmpty, "Should produce at least one match")
        let fingerprintIds = Set(matches.map(\.fingerprintId))
        #expect(matches.count <= fingerprintIds.count,
                "Each fingerprint should merge to at most 1 match: got \(matches.count) matches for \(fingerprintIds.count) fingerprints")
    }
}

// MARK: - Evidence Integration

@Suite("Phase 9 — Evidence Integration")
struct EvidenceIntegrationTests {

    @Test("EvidenceSourceType.fingerprint exists")
    func fingerprintSourceType() {
        let source = EvidenceSourceType.fingerprint
        #expect(source.rawValue == "fingerprint")
    }

    @Test("EvidenceLedgerDetail.fingerprint carries matchCount and averageSimilarity")
    func fingerprintLedgerDetail() {
        let detail = EvidenceLedgerDetail.fingerprint(matchCount: 3, averageSimilarity: 0.75)
        if case .fingerprint(let count, let similarity) = detail {
            #expect(count == 3)
            #expect(similarity == 0.75)
        } else {
            #expect(Bool(false), "Expected .fingerprint detail")
        }
    }

    @Test("FusionWeightConfig.fingerprintCap defaults to 0.25")
    func fingerprintCapDefault() {
        let config = FusionWeightConfig()
        #expect(config.fingerprintCap == 0.25)
    }

    @Test("Fingerprint entries produce capped ledger entries in buildLedger")
    func fingerprintInBuildLedger() {
        let span = makeTestSpan()
        let fpEntry = EvidenceLedgerEntry(
            source: .fingerprint,
            weight: 0.5,
            detail: .fingerprint(matchCount: 2, averageSimilarity: 0.8)
        )

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            fingerprintEntries: [fpEntry],
            mode: .off,
            config: FusionWeightConfig()
        )

        let ledger = fusion.buildLedger()
        let fpLedgerEntries = ledger.filter { $0.source == .fingerprint }
        #expect(fpLedgerEntries.count == 1)
        // Weight should be capped at fingerprintCap (0.25), not the raw 0.5
        #expect(fpLedgerEntries[0].weight == 0.25)
    }

    @Test("Empty fingerprint entries produce no ledger entries")
    func emptyFingerprintNoLedger() {
        let span = makeTestSpan()
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .off,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let fpEntries = ledger.filter { $0.source == .fingerprint }
        #expect(fpEntries.isEmpty)
    }

    @Test("Fingerprint counts as distinct evidence kind for quorum gate")
    func fingerprintCountsForQuorum() {
        let span = makeTestSpan(
            anchorProvenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9)]
        )

        // Classifier + fingerprint = 2 distinct kinds → should pass quorum
        let fpEntry = EvidenceLedgerEntry(
            source: .fingerprint,
            weight: 0.2,
            detail: .fingerprint(matchCount: 1, averageSimilarity: 0.7)
        )

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.5,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            fingerprintEntries: [fpEntry],
            mode: .off,
            config: FusionWeightConfig()
        )

        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: FusionWeightConfig())
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible,
                "Classifier + fingerprint should satisfy 2+ distinct kinds quorum")
    }

    @Test("Fingerprint is external corroboration for fmAcousticCorroborated")
    func fingerprintAsExternalCorroboration() {
        let span = makeTestSpan(
            anchorProvenance: [.fmAcousticCorroborated(regionId: "r1", breakStrength: 0.8)]
        )

        let fpEntry = EvidenceLedgerEntry(
            source: .fingerprint,
            weight: 0.2,
            detail: .fingerprint(matchCount: 1, averageSimilarity: 0.7)
        )

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            fingerprintEntries: [fpEntry],
            mode: .off,
            config: FusionWeightConfig()
        )

        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: FusionWeightConfig())
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible,
                "Fingerprint should count as external corroboration for fmAcousticCorroborated")
    }
}

// MARK: - Rollback Decay (Cross-Episode)

@Suite("Phase 9 — Rollback Decay Cross-Episode")
struct RollbackDecayCrossEpisodeTests {

    @Test("Active entry decays and stops matching until re-confirmed")
    func crossEpisodeDecay() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        let adText = "squarespace is the all in one website platform for entrepreneurs to stand out online and grow your business visit squarespace dot com"

        // Confirm in ep1 + ep2 → active
        for ep in 1...2 {
            try await fpStore.recordCandidate(
                podcastId: "pod-cross",
                text: adText,
                analysisAssetId: "episode-\(ep)",
                sourceAdWindowId: "window-ep\(ep)",
                confidence: 0.9
            )
        }

        let afterConfirm = try await fpStore.allEntries(forPodcast: "pod-cross")
        #expect(afterConfirm[0].state == .active)

        // Rollback in ep3 (enough to trigger spike)
        for _ in 1...3 {
            try await fpStore.recordRollback(
                podcastId: "pod-cross",
                fingerprintHash: afterConfirm[0].fingerprintHash
            )
        }

        let afterRollback = try await fpStore.allEntries(forPodcast: "pod-cross")
        #expect(afterRollback[0].state == .decayed)

        // ep4: should not match because entry is decayed
        let active = try await fpStore.activeEntries(forPodcast: "pod-cross")
        #expect(active.isEmpty, "Decayed entry should not appear in active entries")
    }

    @Test("Decayed entry blocks after further rollbacks")
    func decayedToBlocked() async throws {
        let analysisStore = try await makeTestStore()
        let fpStore = AdCopyFingerprintStore(store: analysisStore)

        let adText = "squarespace is the all in one website platform for entrepreneurs to stand out online and grow your business visit squarespace dot com"

        // Build to active (2 confirmations).
        for ep in 1...2 {
            try await fpStore.recordCandidate(
                podcastId: "pod-block",
                text: adText,
                analysisAssetId: "block-ep-\(ep)",
                sourceAdWindowId: "block-win-\(ep)",
                confidence: 0.9
            )
        }
        let entry = try await fpStore.allEntries(forPodcast: "pod-block")[0]
        #expect(entry.state == .active)

        // Decay it: 3 rollbacks → RR = 3/5 = 0.6 > 0.5 → decayed.
        for _ in 1...3 {
            try await fpStore.recordRollback(
                podcastId: "pod-block",
                fingerprintHash: entry.fingerprintHash
            )
        }
        let decayed = try await fpStore.allEntries(forPodcast: "pod-block")[0]
        #expect(decayed.state == .decayed)

        // Further rollback while decayed: RR = 4/6 = 0.67 > 0.5 → blocked.
        try await fpStore.recordRollback(
            podcastId: "pod-block",
            fingerprintHash: entry.fingerprintHash
        )
        let blocked = try await fpStore.allEntries(forPodcast: "pod-block")[0]
        #expect(blocked.state == .blocked, "Decayed entry should block after further rollbacks: got \(blocked.state)")

        // Blocked is terminal — more confirmations don't change state.
        try await fpStore.recordCandidate(
            podcastId: "pod-block",
            text: adText,
            analysisAssetId: "block-ep-3",
            sourceAdWindowId: "block-win-3",
            confidence: 0.9
        )
        let stillBlocked = try await fpStore.allEntries(forPodcast: "pod-block")[0]
        #expect(stillBlocked.state == .blocked, "Blocked state should be terminal")
    }
}

// MARK: - Promotion Thresholds

@Suite("Phase 9 — FingerprintPromotionThresholds")
struct PromotionThresholdTests {

    @Test("Thresholds are independent from KnowledgePromotionThresholds")
    func thresholdsAreIndependent() {
        // Verify our thresholds exist as a separate type
        #expect(FingerprintPromotionThresholds.minConfirmationsForActive == 2)
        #expect(FingerprintPromotionThresholds.maxRollbackRateForActive == 0.3)
        #expect(FingerprintPromotionThresholds.rollbackSpikeThreshold == 0.5)
        #expect(FingerprintPromotionThresholds.minCandidateConfidence == 0.5)
    }

    @Test("Promotion state machine: candidate -> quarantined -> active")
    func promotionStateMachine() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        // candidate → quarantined (1 conf)
        let nextFromCandidate = store.promoteState(
            current: .candidate, confirmationCount: 1, rollbackCount: 0
        )
        #expect(nextFromCandidate == .quarantined)

        // quarantined → active (2+ conf, RR <= 0.3)
        let nextFromQuarantined = store.promoteState(
            current: .quarantined, confirmationCount: 2, rollbackCount: 0
        )
        #expect(nextFromQuarantined == .active)

        // quarantined stays quarantined (only 1 conf)
        let staysQuarantined = store.promoteState(
            current: .quarantined, confirmationCount: 1, rollbackCount: 0
        )
        #expect(staysQuarantined == .quarantined)
    }

    @Test("Demotion state machine: active -> decayed, decayed -> blocked")
    func demotionStateMachine() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        // active → decayed (RR > 0.5)
        let demotedFromActive = store.demoteState(
            current: .active, confirmationCount: 2, rollbackCount: 3
        )
        #expect(demotedFromActive == .decayed)

        // decayed → blocked (RR > 0.5)
        let demotedFromDecayed = store.demoteState(
            current: .decayed, confirmationCount: 2, rollbackCount: 3
        )
        #expect(demotedFromDecayed == .blocked)

        // blocked stays blocked
        let staysBlocked = store.demoteState(
            current: .blocked, confirmationCount: 2, rollbackCount: 3
        )
        #expect(staysBlocked == .blocked)
    }
}

// MARK: - FingerprintEntry

@Suite("Phase 9 — FingerprintEntry")
struct FingerprintEntryTests {

    @Test("Rollback rate calculation")
    func rollbackRate() {
        let entry = FingerprintEntry(
            podcastId: "pod-rr",
            fingerprintHash: "hash-rr",
            normalizedText: "test",
            confirmationCount: 3,
            rollbackCount: 1
        )
        #expect(entry.rollbackRate == 0.25) // 1/(3+1)
    }

    @Test("Rollback rate with zero observations is 0.0")
    func rollbackRateZero() {
        let entry = FingerprintEntry(
            podcastId: "pod-rr",
            fingerprintHash: "hash-rr",
            normalizedText: "test",
            confirmationCount: 0,
            rollbackCount: 0
        )
        #expect(entry.rollbackRate == 0.0)
    }

    @Test("FingerprintEntry is Equatable and Sendable")
    func conformances() {
        let now = Date().timeIntervalSince1970
        let a = FingerprintEntry(
            id: "id-1", podcastId: "pod", fingerprintHash: "hash",
            normalizedText: "text", confirmationCount: 1, firstSeenAt: now
        )
        let b = FingerprintEntry(
            id: "id-1", podcastId: "pod", fingerprintHash: "hash",
            normalizedText: "text", confirmationCount: 1, firstSeenAt: now
        )
        #expect(a == b)
    }
}
