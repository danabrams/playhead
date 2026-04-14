// B10FingerprintSpanRecoveryTests.swift
// Tests for B10: Fingerprint full-span recovery via anchor-aware local alignment.
// Covers: span offset fields, anchor-landmark alignment, transferred boundaries,
// match strength classification, and user-marked ad fingerprint seeding.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeAtom(
    assetId: String = "asset-b10",
    ordinal: Int,
    text: String,
    startTime: Double,
    endTime: Double
) -> TranscriptAtom {
    TranscriptAtom(
        atomKey: TranscriptAtomKey(
            analysisAssetId: assetId,
            transcriptVersion: "tv-b10",
            atomOrdinal: ordinal
        ),
        contentHash: "deadbeef",
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: ordinal
    )
}

private func makeEntryWithSpan(
    id: String = "fp-1",
    podcastId: String = "pod-b10",
    text: String = "squarespace is the all in one website platform",
    state: KnowledgeState = .active,
    spanStartOffset: Double = 15.0,
    spanEndOffset: Double = 10.0,
    spanDurationSeconds: Double = 55.0,
    sponsorEntity: NormalizedSponsor? = NormalizedSponsor("Squarespace"),
    anchorLandmarks: [AnchorLandmark] = []
) -> FingerprintEntry {
    FingerprintEntry(
        id: id,
        podcastId: podcastId,
        fingerprintHash: MinHashUtilities.generateFingerprint(from: text),
        normalizedText: MinHashUtilities.normalizeText(text),
        state: state,
        confirmationCount: 3,
        firstSeenAt: 1000,
        lastConfirmedAt: 2000,
        spanStartOffset: spanStartOffset,
        spanEndOffset: spanEndOffset,
        spanDurationSeconds: spanDurationSeconds,
        canonicalSponsorEntity: sponsorEntity,
        anchorLandmarks: anchorLandmarks
    )
}

private func makeAnchorEvent(
    type: AnchorType,
    startTime: Double,
    endTime: Double? = nil,
    text: String = "anchor",
    sponsorEntity: NormalizedSponsor? = nil
) -> AnchorEvent {
    AnchorEvent(
        anchorType: type,
        matchedText: text,
        startTime: startTime,
        endTime: endTime ?? (startTime + 1.0),
        weight: 1.0,
        sponsorEntity: sponsorEntity
    )
}

// MARK: - FingerprintEntry Span Fields

@Suite("B10 — FingerprintEntry Span Fields")
struct FingerprintEntrySpanFieldTests {

    @Test("New FingerprintEntry defaults have zero span offsets")
    func defaultSpanOffsets() {
        let entry = FingerprintEntry(
            podcastId: "pod-1",
            fingerprintHash: "abc",
            normalizedText: "test"
        )
        #expect(entry.spanStartOffset == 0)
        #expect(entry.spanEndOffset == 0)
        #expect(entry.spanDurationSeconds == 0)
        #expect(entry.canonicalSponsorEntity == nil)
        #expect(entry.anchorLandmarks.isEmpty)
        #expect(!entry.hasSpanOffsets)
    }

    @Test("Entry with span offsets reports hasSpanOffsets true")
    func hasSpanOffsetsTrue() {
        let entry = makeEntryWithSpan()
        #expect(entry.hasSpanOffsets)
        #expect(entry.spanStartOffset == 15.0)
        #expect(entry.spanEndOffset == 10.0)
        #expect(entry.spanDurationSeconds == 55.0)
    }

    @Test("AnchorLandmark stores type, offset, and text")
    func anchorLandmarkFields() {
        let landmark = AnchorLandmark(
            type: .url,
            offsetSeconds: 42.0,
            normalizedText: "betterhelp dot com"
        )
        #expect(landmark.type == .url)
        #expect(landmark.offsetSeconds == 42.0)
        #expect(landmark.normalizedText == "betterhelp dot com")
    }

    @Test("AnchorLandmark with nil text")
    func anchorLandmarkNilText() {
        let landmark = AnchorLandmark(
            type: .disclosure,
            offsetSeconds: 2.0,
            normalizedText: nil
        )
        #expect(landmark.normalizedText == nil)
    }

    @Test("AnchorLandmark Codable round-trip")
    func anchorLandmarkCodable() throws {
        let landmarks = [
            AnchorLandmark(type: .disclosure, offsetSeconds: 2.0, normalizedText: "sponsored by"),
            AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: "squarespace dot com"),
            AnchorLandmark(type: .promoCode, offsetSeconds: 48.0, normalizedText: nil)
        ]
        let data = try JSONEncoder().encode(landmarks)
        let decoded = try JSONDecoder().decode([AnchorLandmark].self, from: data)
        #expect(decoded == landmarks)
    }
}

// MARK: - Span Offset Persistence

@Suite("B10 — Span Offset Persistence")
struct SpanOffsetPersistenceTests {

    @Test("Span offsets round-trip through AnalysisStore")
    func spanOffsetsRoundTrip() async throws {
        let analysisStore = try await makeTestStore()
        let landmarks = [
            AnchorLandmark(type: .disclosure, offsetSeconds: 2.0, normalizedText: "sponsored by"),
            AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: "squarespace dot com")
        ]
        let entry = FingerprintEntry(
            id: "fp-rt",
            podcastId: "pod-rt",
            fingerprintHash: MinHashUtilities.generateFingerprint(from: "round trip test"),
            normalizedText: "round trip test",
            state: .active,
            confirmationCount: 2,
            firstSeenAt: 1000,
            spanStartOffset: 15.0,
            spanEndOffset: 10.0,
            spanDurationSeconds: 55.0,
            canonicalSponsorEntity: NormalizedSponsor("Squarespace"),
            anchorLandmarks: landmarks
        )
        try await analysisStore.upsertFingerprintEntry(entry)

        let loaded = try await analysisStore.loadFingerprintEntry(
            podcastId: "pod-rt",
            fingerprintHash: entry.fingerprintHash
        )
        #expect(loaded != nil)
        #expect(loaded?.spanStartOffset == 15.0)
        #expect(loaded?.spanEndOffset == 10.0)
        #expect(loaded?.spanDurationSeconds == 55.0)
        #expect(loaded?.canonicalSponsorEntity == NormalizedSponsor("Squarespace"))
        #expect(loaded?.anchorLandmarks.count == 2)
        #expect(loaded?.anchorLandmarks[0].type == .disclosure)
        #expect(loaded?.anchorLandmarks[1].type == .url)
        #expect(loaded?.anchorLandmarks[1].normalizedText == "squarespace dot com")
    }

    @Test("Entry without span offsets loads with defaults")
    func noSpanOffsetsDefaults() async throws {
        let analysisStore = try await makeTestStore()
        let entry = FingerprintEntry(
            id: "fp-nospan",
            podcastId: "pod-nospan",
            fingerprintHash: MinHashUtilities.generateFingerprint(from: "no span test"),
            normalizedText: "no span test",
            state: .active,
            confirmationCount: 2,
            firstSeenAt: 1000
        )
        try await analysisStore.upsertFingerprintEntry(entry)

        let loaded = try await analysisStore.loadFingerprintEntry(
            podcastId: "pod-nospan",
            fingerprintHash: entry.fingerprintHash
        )
        #expect(loaded != nil)
        #expect(loaded?.spanStartOffset == 0)
        #expect(loaded?.spanEndOffset == 0)
        #expect(loaded?.spanDurationSeconds == 0)
        #expect(loaded?.canonicalSponsorEntity == nil)
        #expect(loaded?.anchorLandmarks.isEmpty == true)
        #expect(loaded?.hasSpanOffsets == false)
    }

    @Test("Span offsets preserved through atomic confirm")
    func spanOffsetsPreservedThroughConfirm() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        // Seed an entry with span offsets using direct upsert.
        let entry = makeEntryWithSpan(
            podcastId: "pod-confirm",
            spanStartOffset: 12.0,
            spanEndOffset: 8.0,
            spanDurationSeconds: 50.0
        )
        try await analysisStore.upsertFingerprintEntry(entry)

        // Confirm via recordCandidate (which uses atomicConfirmFingerprint).
        try await store.recordCandidate(
            podcastId: "pod-confirm",
            text: "squarespace is the all in one website platform",
            analysisAssetId: "asset-c1",
            sourceAdWindowId: "window-c1",
            confidence: 0.8
        )

        let loaded = try await store.allEntries(forPodcast: "pod-confirm")
        #expect(loaded.count == 1)
        #expect(loaded[0].spanStartOffset == 12.0)
        #expect(loaded[0].spanEndOffset == 8.0)
        #expect(loaded[0].spanDurationSeconds == 50.0)
    }
}

// MARK: - Anchor Landmark Alignment

@Suite("B10 — Anchor Landmark Alignment")
struct AnchorLandmarkAlignmentTests {

    @Test("Empty landmarks produce trivially valid alignment")
    func emptyLandmarks() {
        let result = AdCopyFingerprintMatcher.alignAnchorLandmarks(
            storedLandmarks: [],
            episodeAnchors: [],
            matchStartTime: 100.0
        )
        #expect(result.isValid)
        #expect(result.alignedCount == 0)
        #expect(result.totalCount == 0)
    }

    @Test("Perfect alignment when anchors match expected positions")
    func perfectAlignment() {
        let landmarks = [
            AnchorLandmark(type: .disclosure, offsetSeconds: 2.0, normalizedText: "sponsored by"),
            AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: "squarespace dot com")
        ]
        let episodeAnchors = [
            makeAnchorEvent(type: .disclosure, startTime: 102.0),
            makeAnchorEvent(type: .url, startTime: 145.0)
        ]
        let result = AdCopyFingerprintMatcher.alignAnchorLandmarks(
            storedLandmarks: landmarks,
            episodeAnchors: episodeAnchors,
            matchStartTime: 100.0
        )
        #expect(result.isValid)
        #expect(result.alignedCount == 2)
        #expect(result.totalCount == 2)
        #expect(result.maxDriftSeconds == 0.0)
    }

    @Test("Alignment tolerates drift within threshold")
    func driftWithinThreshold() {
        let landmarks = [
            AnchorLandmark(type: .disclosure, offsetSeconds: 2.0, normalizedText: nil),
            AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: nil)
        ]
        // Anchors are shifted by 5s and 8s respectively (within 10s tolerance).
        let episodeAnchors = [
            makeAnchorEvent(type: .disclosure, startTime: 107.0),
            makeAnchorEvent(type: .url, startTime: 153.0)
        ]
        let result = AdCopyFingerprintMatcher.alignAnchorLandmarks(
            storedLandmarks: landmarks,
            episodeAnchors: episodeAnchors,
            matchStartTime: 100.0
        )
        #expect(result.isValid)
        #expect(result.alignedCount == 2)
        #expect(result.maxDriftSeconds == 8.0)
    }

    @Test("Alignment fails when drift exceeds threshold")
    func driftExceedsThreshold() {
        let landmarks = [
            AnchorLandmark(type: .disclosure, offsetSeconds: 2.0, normalizedText: nil),
            AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: nil)
        ]
        // Both anchors are shifted by > 10s.
        let episodeAnchors = [
            makeAnchorEvent(type: .disclosure, startTime: 115.0),
            makeAnchorEvent(type: .url, startTime: 160.0)
        ]
        let result = AdCopyFingerprintMatcher.alignAnchorLandmarks(
            storedLandmarks: landmarks,
            episodeAnchors: episodeAnchors,
            matchStartTime: 100.0
        )
        #expect(!result.isValid)
        #expect(result.alignedCount == 0)
    }

    @Test("Partial alignment: one of two landmarks aligns")
    func partialAlignment() {
        let landmarks = [
            AnchorLandmark(type: .disclosure, offsetSeconds: 2.0, normalizedText: nil),
            AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: nil)
        ]
        // Disclosure aligns, URL drifted too far.
        let episodeAnchors = [
            makeAnchorEvent(type: .disclosure, startTime: 102.0),
            makeAnchorEvent(type: .url, startTime: 200.0)
        ]
        let result = AdCopyFingerprintMatcher.alignAnchorLandmarks(
            storedLandmarks: landmarks,
            episodeAnchors: episodeAnchors,
            matchStartTime: 100.0
        )
        // 1/2 = 50% >= 50% threshold → valid
        #expect(result.isValid)
        #expect(result.alignedCount == 1)
        #expect(result.totalCount == 2)
    }

    @Test("Alignment ignores anchors of wrong type")
    func wrongAnchorType() {
        let landmarks = [
            AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: nil)
        ]
        // Only disclosure anchor present, no URL.
        let episodeAnchors = [
            makeAnchorEvent(type: .disclosure, startTime: 145.0)
        ]
        let result = AdCopyFingerprintMatcher.alignAnchorLandmarks(
            storedLandmarks: landmarks,
            episodeAnchors: episodeAnchors,
            matchStartTime: 100.0
        )
        #expect(!result.isValid)
        #expect(result.alignedCount == 0)
    }

    @Test("Host ad-lib variation: 3 of 4 landmarks align")
    func hostAdLibVariation() {
        let landmarks = [
            AnchorLandmark(type: .disclosure, offsetSeconds: 0.0, normalizedText: nil),
            AnchorLandmark(type: .sponsorLexicon, offsetSeconds: 5.0, normalizedText: nil),
            AnchorLandmark(type: .promoCode, offsetSeconds: 40.0, normalizedText: nil),
            AnchorLandmark(type: .url, offsetSeconds: 50.0, normalizedText: nil)
        ]
        let episodeAnchors = [
            makeAnchorEvent(type: .disclosure, startTime: 200.5),
            makeAnchorEvent(type: .sponsorLexicon, startTime: 206.0),
            // promoCode missing (host skipped it this time)
            makeAnchorEvent(type: .url, startTime: 252.0)
        ]
        let result = AdCopyFingerprintMatcher.alignAnchorLandmarks(
            storedLandmarks: landmarks,
            episodeAnchors: episodeAnchors,
            matchStartTime: 200.0
        )
        // 3/4 = 75% >= 50% threshold → valid
        #expect(result.isValid)
        #expect(result.alignedCount == 3)
        #expect(result.totalCount == 4)
    }
}

// MARK: - Span Boundary Transfer

@Suite("B10 — Span Boundary Transfer")
struct SpanBoundaryTransferTests {

    @Test("Strong match transfers full boundaries with valid alignment")
    func strongMatchTransfers() {
        let entry = makeEntryWithSpan(
            spanStartOffset: 15.0,
            spanEndOffset: 10.0,
            spanDurationSeconds: 55.0,
            anchorLandmarks: [
                AnchorLandmark(type: .disclosure, offsetSeconds: 2.0, normalizedText: nil)
            ]
        )
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: entry.id,
            similarity: 0.85,
            startTime: 120.0,
            endTime: 150.0,
            matchStrength: .strong
        )
        let episodeAnchors = [
            makeAnchorEvent(type: .disclosure, startTime: 122.0)
        ]

        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: episodeAnchors
        )

        #expect(transferred != nil)
        #expect(transferred?.adStartTime == 105.0)  // 120 - 15
        #expect(transferred?.adEndTime == 160.0)     // 150 + 10
        #expect(transferred?.matchStrength == .strong)
        #expect(transferred?.alignment.isValid == true)
    }

    @Test("Strong match blocked when alignment fails")
    func strongMatchBlockedByAlignment() {
        let entry = makeEntryWithSpan(
            anchorLandmarks: [
                AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: nil)
            ]
        )
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: entry.id,
            similarity: 0.85,
            startTime: 120.0,
            endTime: 150.0,
            matchStrength: .strong
        )
        // No matching anchors at all.
        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: []
        )
        #expect(transferred == nil)
    }

    @Test("Strong match with no landmarks transfers without alignment check")
    func strongMatchNoLandmarks() {
        let entry = makeEntryWithSpan(anchorLandmarks: [])
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: entry.id,
            similarity: 0.85,
            startTime: 120.0,
            endTime: 150.0,
            matchStrength: .strong
        )
        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: []
        )
        #expect(transferred != nil)
        #expect(transferred?.adStartTime == 105.0)
        #expect(transferred?.adEndTime == 160.0)
    }

    @Test("Normal match returns transferred boundary for hypothesis seeding")
    func normalMatchSeedsHypothesis() {
        let entry = makeEntryWithSpan(
            anchorLandmarks: [
                AnchorLandmark(type: .url, offsetSeconds: 45.0, normalizedText: nil)
            ]
        )
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: entry.id,
            similarity: 0.7,
            startTime: 120.0,
            endTime: 150.0,
            matchStrength: .normal
        )
        // Even without matching anchors, normal match still returns boundary
        // for hypothesis engine verification.
        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: []
        )
        #expect(transferred != nil)
        #expect(transferred?.matchStrength == .normal)
        #expect(transferred?.alignment.isValid == false)
    }

    @Test("Transfer returns nil when entry has no span offsets")
    func noSpanOffsetsReturnsNil() {
        let entry = FingerprintEntry(
            id: "fp-nospan",
            podcastId: "pod-b10",
            fingerprintHash: "abc",
            normalizedText: "test",
            state: .active,
            confirmationCount: 2,
            firstSeenAt: 1000
        )
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: entry.id,
            similarity: 0.85,
            startTime: 120.0,
            endTime: 150.0,
            matchStrength: .strong
        )
        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: []
        )
        #expect(transferred == nil)
    }

    @Test("Transfer rejects absurd duration (2x stored)")
    func rejectsAbsurdDuration() {
        let entry = makeEntryWithSpan(
            spanStartOffset: 100.0,
            spanEndOffset: 100.0,
            spanDurationSeconds: 55.0  // But transferred would be 230s
        )
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: entry.id,
            similarity: 0.85,
            startTime: 120.0,
            endTime: 150.0,
            matchStrength: .strong
        )
        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: []
        )
        #expect(transferred == nil)
    }
}

// MARK: - Match Strength Classification

@Suite("B10 — Match Strength Classification")
struct MatchStrengthTests {

    @Test("Similarity >= 0.8 classified as strong")
    func strongMatchClassification() {
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: "fp-1",
            similarity: 0.85,
            startTime: 0,
            endTime: 30,
            matchStrength: .strong
        )
        #expect(match.matchStrength == .strong)
    }

    @Test("Similarity 0.6-0.8 classified as normal")
    func normalMatchClassification() {
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: "fp-1",
            similarity: 0.7,
            startTime: 0,
            endTime: 30,
            matchStrength: .normal
        )
        #expect(match.matchStrength == .normal)
    }

    @Test("Auto-classification from similarity score without explicit matchStrength")
    func autoClassificationFromSimilarity() {
        // Strong: similarity >= 0.8
        let strongMatch = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: "fp-auto-strong",
            similarity: 0.85,
            startTime: 0,
            endTime: 30
        )
        #expect(strongMatch.matchStrength == .strong)

        // Boundary: similarity == 0.8 exactly
        let boundaryMatch = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: "fp-auto-boundary",
            similarity: 0.8,
            startTime: 0,
            endTime: 30
        )
        #expect(boundaryMatch.matchStrength == .strong)

        // Normal: similarity 0.6-0.8
        let normalMatch = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: "fp-auto-normal",
            similarity: 0.7,
            startTime: 0,
            endTime: 30
        )
        #expect(normalMatch.matchStrength == .normal)

        // Normal: similarity just below 0.8
        let justBelowMatch = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            fingerprintId: "fp-auto-below",
            similarity: 0.79,
            startTime: 0,
            endTime: 30
        )
        #expect(justBelowMatch.matchStrength == .normal)
    }
}

// MARK: - User-Marked Ad Fingerprint Seeding

@Suite("B10 — User-Marked Ad Fingerprint Seeding")
struct UserMarkedFingerprintSeedingTests {

    @Test("User-marked ad with confidence >= 0.7 seeds candidate fingerprint")
    func seedsCandidate() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let boundary = ExpandedBoundary(
            startTime: 100.0,
            endTime: 160.0,
            boundaryConfidence: 0.85,
            source: .acousticAndLexical
        )

        try await store.seedFromUserMarkedAd(
            podcastId: "pod-user",
            text: "this ad is brought to you by betterhelp online therapy",
            analysisAssetId: "asset-u1",
            sourceAdWindowId: "window-u1",
            boundary: boundary,
            matchStartTime: 115.0,
            matchEndTime: 145.0,
            sponsorEntity: NormalizedSponsor("BetterHelp"),
            anchorLandmarks: [
                AnchorLandmark(type: .disclosure, offsetSeconds: 0.0, normalizedText: "brought to you by")
            ]
        )

        let entries = try await store.allEntries(forPodcast: "pod-user")
        #expect(entries.count == 1)
        #expect(entries[0].state == .candidate)
        #expect(entries[0].spanStartOffset == 15.0)  // 115 - 100
        #expect(entries[0].spanEndOffset == 15.0)     // 160 - 145
        #expect(entries[0].spanDurationSeconds == 60.0)  // 160 - 100
        #expect(entries[0].canonicalSponsorEntity == NormalizedSponsor("BetterHelp"))
        #expect(entries[0].anchorLandmarks.count == 1)
        #expect(entries[0].hasSpanOffsets)
    }

    @Test("User-marked ad with confidence < 0.7 is rejected")
    func rejectsLowConfidence() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let boundary = ExpandedBoundary(
            startTime: 100.0,
            endTime: 160.0,
            boundaryConfidence: 0.5,
            source: .fallback
        )

        try await store.seedFromUserMarkedAd(
            podcastId: "pod-user",
            text: "this ad is brought to you by betterhelp online therapy",
            analysisAssetId: "asset-u1",
            sourceAdWindowId: "window-u1",
            boundary: boundary,
            matchStartTime: 115.0,
            matchEndTime: 145.0
        )

        let entries = try await store.allEntries(forPodcast: "pod-user")
        #expect(entries.isEmpty)
    }

    @Test("User-marked ad with empty text is rejected")
    func rejectsEmptyText() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let boundary = ExpandedBoundary(
            startTime: 100.0,
            endTime: 160.0,
            boundaryConfidence: 0.85,
            source: .acousticAndLexical
        )

        try await store.seedFromUserMarkedAd(
            podcastId: "pod-user",
            text: "   ",
            analysisAssetId: "asset-u1",
            sourceAdWindowId: "window-u1",
            boundary: boundary,
            matchStartTime: 115.0,
            matchEndTime: 145.0
        )

        let entries = try await store.allEntries(forPodcast: "pod-user")
        #expect(entries.isEmpty)
    }

    @Test("User-marked fingerprint goes through normal trust lifecycle")
    func normalTrustLifecycle() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let text = "this ad is brought to you by betterhelp online therapy sessions for everyone"
        let boundary = ExpandedBoundary(
            startTime: 100.0,
            endTime: 160.0,
            boundaryConfidence: 0.85,
            source: .acousticAndLexical
        )

        // First seed: candidate state.
        try await store.seedFromUserMarkedAd(
            podcastId: "pod-lifecycle",
            text: text,
            analysisAssetId: "asset-1",
            sourceAdWindowId: "window-1",
            boundary: boundary,
            matchStartTime: 115.0,
            matchEndTime: 145.0
        )

        var entries = try await store.allEntries(forPodcast: "pod-lifecycle")
        #expect(entries.count == 1)
        #expect(entries[0].state == .candidate)

        // Confirm via standard path → quarantined.
        try await store.recordCandidate(
            podcastId: "pod-lifecycle",
            text: text,
            analysisAssetId: "asset-2",
            sourceAdWindowId: "window-2",
            confidence: 0.8
        )

        entries = try await store.allEntries(forPodcast: "pod-lifecycle")
        #expect(entries.count == 1)
        #expect(entries[0].state == .quarantined)

        // Second confirm → active.
        try await store.recordCandidate(
            podcastId: "pod-lifecycle",
            text: text,
            analysisAssetId: "asset-3",
            sourceAdWindowId: "window-3",
            confidence: 0.8
        )

        entries = try await store.allEntries(forPodcast: "pod-lifecycle")
        #expect(entries.count == 1)
        #expect(entries[0].state == .active)

        // Verify span offsets survived the lifecycle transitions.
        #expect(entries[0].spanStartOffset == 15.0)
        #expect(entries[0].spanDurationSeconds == 60.0)
    }

    @Test("User-marked ad creates provenance event")
    func createsProvenanceEvent() async throws {
        let analysisStore = try await makeTestStore()
        let store = AdCopyFingerprintStore(store: analysisStore)

        let boundary = ExpandedBoundary(
            startTime: 100.0,
            endTime: 160.0,
            boundaryConfidence: 0.85,
            source: .acousticAndLexical
        )

        try await store.seedFromUserMarkedAd(
            podcastId: "pod-prov",
            text: "this ad is brought to you by betterhelp online therapy",
            analysisAssetId: "asset-prov",
            sourceAdWindowId: "window-prov",
            boundary: boundary,
            matchStartTime: 115.0,
            matchEndTime: 145.0
        )

        let events = try await store.sourceEvents(forAsset: "asset-prov")
        #expect(events.count == 1)
        #expect(events[0].confidence == 0.85)
        #expect(events[0].sourceAdWindowId == "window-prov")
    }

    // MARK: - Negative adStartTime clamping

    @Test("Transfer clamps adStartTime to zero when spanStartOffset exceeds match start")
    func negativeAdStartTimeClamped() {
        let entry = makeEntryWithSpan(
            spanStartOffset: 15.0,
            spanEndOffset: 5.0,
            spanDurationSeconds: 25.0,
            anchorLandmarks: []
        )
        // match.startTime (5.0) < entry.spanStartOffset (15.0)
        // would produce adStartTime = -10.0 without clamp
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 5,
            fingerprintId: entry.id,
            similarity: 0.7,
            startTime: 5.0,
            endTime: 20.0,
            matchStrength: .normal
        )
        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: []
        )
        #expect(transferred != nil)
        #expect(transferred?.adStartTime == 0.0)  // clamped, not -10.0
        #expect(transferred?.adEndTime == 25.0)    // 20 + 5
    }

    @Test("Transfer with zero match start and large spanStartOffset clamps to zero")
    func zeroMatchStartClamped() {
        let entry = makeEntryWithSpan(
            spanStartOffset: 30.0,
            spanEndOffset: 5.0,
            spanDurationSeconds: 50.0,
            anchorLandmarks: []
        )
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 3,
            fingerprintId: entry.id,
            similarity: 0.65,
            startTime: 0.0,
            endTime: 15.0,
            matchStrength: .normal
        )
        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: []
        )
        #expect(transferred != nil)
        #expect(transferred?.adStartTime == 0.0)
    }

    @Test("Transfer returns nil for zero-duration span (adEndTime == adStartTime)")
    func zeroDurationTransferReturnsNil() {
        // spanStartOffset = 10, spanEndOffset = 0, match at [10, 20]
        // adStartTime = max(0, 10 - 10) = 0, adEndTime = 20 + 0 = 20 → valid
        // But with a degenerate case: match at [10, 10], offsets [10, 0]
        // adStartTime = 0, adEndTime = 10 → still valid (end > start)
        // For true zero-duration: match at [5, 5], offsets [5, 0]
        // adStartTime = 0, adEndTime = 5 → valid
        // For actual equal: match [10, 10], offsets [10, -10] — but offsets
        // can't be negative. Use entry where after clamp start == end.
        // Simplest: match [0, 0], spanStartOffset=0, spanEndOffset=0
        let entry = makeEntryWithSpan(
            spanStartOffset: 0,
            spanEndOffset: 0,
            spanDurationSeconds: 5.0,
            anchorLandmarks: []
        )
        let match = FingerprintMatch(
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 0,
            fingerprintId: entry.id,
            similarity: 0.7,
            startTime: 0.0,
            endTime: 0.0,
            matchStrength: .normal
        )
        let transferred = AdCopyFingerprintMatcher.transferSpanBoundary(
            match: match,
            entry: entry,
            episodeAnchors: []
        )
        // adStartTime = 0, adEndTime = 0 → guard adEndTime > adStartTime fails → nil
        #expect(transferred == nil)
    }
}
