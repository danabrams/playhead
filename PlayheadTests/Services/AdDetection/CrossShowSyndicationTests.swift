// CrossShowSyndicationTests.swift
// playhead-xsdz.13: Hermetic unit tests for the cross-show syndication precision
// signal — the CrossShowSyndicationStore (SQLite, sponsor-entity observations
// keyed by normalized entity across shows, spread-ratio query, LRU eviction,
// deterministic), the CrossShowSyndicationEvaluator (spread + temporal-
// persistence gate, capped modest boost), and the AdDetectionService sponsor-
// entity extraction helpers.
//
// These are FULLY hermetic (no FM, no audio, no corpus). The store tests use a
// temp-dir SQLite file and tear it down per test.

import Foundation
import Testing
@testable import Playhead

// MARK: - Store

@Suite("CrossShowSyndicationStore (playhead-xsdz.13)")
struct CrossShowSyndicationStoreTests {

    private func makeStore() throws -> CrossShowSyndicationStore {
        let dir = try makeTempDir(prefix: "xsdz13-store")
        return try CrossShowSyndicationStore(directoryURL: dir)
    }

    @Test("Records observations and computes spread-ratio across distinct shows")
    func recordsAndComputesSpread() async throws {
        let store = try makeStore()
        // BetterHelp appears in 3 distinct shows; library has 4 observed shows.
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "showA", confidence: 0.9)
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "showB", confidence: 0.8)
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "showC", confidence: 0.7)
        // A different, show-specific entity only seen in one show — it pulls the
        // library's observed-show count up to 4 without spreading itself.
        try await store.recordObservation(normalizedEntity: "localbrand", podcastId: "showD", confidence: 0.9)

        let total = await store.totalObservedShowCount()
        #expect(total == 4)

        let profile = try #require(await store.spreadProfile(forEntity: "betterhelp", totalObservedShows: total))
        #expect(profile.distinctShowCount == 3)
        #expect(profile.totalObservedShows == 4)
        #expect(abs(profile.spreadRatio - 0.75) < 1e-9)

        let local = try #require(await store.spreadProfile(forEntity: "localbrand", totalObservedShows: total))
        #expect(local.distinctShowCount == 1)
        #expect(abs(local.spreadRatio - 0.25) < 1e-9)
        await store.close()
    }

    @Test("Re-observing the same (entity, show) increments count, not distinct-show spread")
    func reobservationDoesNotInflateSpread() async throws {
        let store = try makeStore()
        try await store.recordObservation(normalizedEntity: "squarespace", podcastId: "showA", confidence: 0.6)
        try await store.recordObservation(normalizedEntity: "squarespace", podcastId: "showA", confidence: 0.9)

        let entries = try await store.entries(forEntity: "squarespace")
        #expect(entries.count == 1)
        #expect(entries.first?.observationCount == 2)
        // max_confidence is raised, not overwritten with the lower second value.
        #expect(entries.first?.maxConfidence == 0.9)

        let profile = try #require(await store.spreadProfile(forEntity: "squarespace", totalObservedShows: 1))
        #expect(profile.distinctShowCount == 1)
        await store.close()
    }

    @Test("No observations ⇒ nil profile; denominator floors at distinct-show count")
    func edgeCases() async throws {
        let store = try makeStore()
        // No observations at all.
        #expect(await store.totalObservedShowCount() == 0)
        #expect(await store.spreadProfile(forEntity: "ghost", totalObservedShows: 0) == nil)

        // Entity in ALL shows: ratio is exactly 1.0. Also exercise the
        // denominator floor — passing a smaller-than-distinct denominator must
        // not produce a ratio > 1.
        try await store.recordObservation(normalizedEntity: "acme", podcastId: "s1", confidence: 0.9)
        try await store.recordObservation(normalizedEntity: "acme", podcastId: "s2", confidence: 0.9)
        let profile = try #require(await store.spreadProfile(forEntity: "acme", totalObservedShows: 1))
        #expect(profile.distinctShowCount == 2)
        #expect(profile.totalObservedShows == 2) // floored up from the bad denom
        #expect(profile.spreadRatio == 1.0)
        await store.close()
    }

    @Test("Too-short entity / empty podcast id are rejected on write")
    func rejectsInvalidWrites() async throws {
        let store = try makeStore()
        #expect(try await store.recordObservation(normalizedEntity: "ab", podcastId: "s1", confidence: 0.9) == false)
        #expect(try await store.recordObservation(normalizedEntity: "acme", podcastId: "", confidence: 0.9) == false)
        #expect(try await store.count() == 0)
        await store.close()
    }

    @Test("Persistence days reflect the first/last-seen spread across shows")
    func persistenceDays() async throws {
        let store = try makeStore()
        let day: Double = 86_400
        let t0: Double = 1_700_000_000
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "a", confidence: 0.9, now: t0)
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "b", confidence: 0.9, now: t0 + 30 * day)
        let profile = try #require(await store.spreadProfile(forEntity: "betterhelp", totalObservedShows: 2))
        #expect(abs(profile.persistenceDays - 30) < 1e-6)
        await store.close()
    }

    @Test("Determinism: identical write sequences (fixed clock) yield identical profiles")
    func deterministic() async throws {
        // Feed FIXED timestamps so the profile (including first/last-seen) is a
        // pure function of the inputs — proving the store does not introduce any
        // non-determinism of its own. Using the wall clock would (correctly)
        // vary the timestamps; the point here is input → output stability.
        let t0: Double = 1_700_000_000
        let day: Double = 86_400
        func build() async throws -> CrossShowSpreadProfile {
            let store = try makeStore()
            let writes: [(String, String, Double)] = [
                ("betterhelp", "a", t0),
                ("betterhelp", "b", t0 + 10 * day),
                ("betterhelp", "c", t0 + 20 * day),
                ("other", "d", t0 + 5 * day),
            ]
            for (entity, show, now) in writes {
                try await store.recordObservation(normalizedEntity: entity, podcastId: show, confidence: 0.8, now: now)
            }
            let total = await store.totalObservedShowCount()
            let p = try #require(await store.spreadProfile(forEntity: "betterhelp", totalObservedShows: total))
            await store.close()
            return p
        }
        let a = try await build()
        let b = try await build()
        #expect(a == b)
    }
}

// MARK: - Evaluator

@Suite("CrossShowSyndicationEvaluator (playhead-xsdz.13)")
struct CrossShowSyndicationEvaluatorTests {

    private let evaluator = CrossShowSyndicationEvaluator()
    private let cap = 0.20
    private let day: Double = 86_400
    private let t0: Double = 1_700_000_000

    private func profile(
        distinct: Int,
        total: Int,
        persistenceDays: Double
    ) -> CrossShowSpreadProfile {
        CrossShowSpreadProfile(
            normalizedEntity: "betterhelp",
            distinctShowCount: distinct,
            totalObservedShows: total,
            earliestFirstSeenAt: t0,
            latestLastSeenAt: t0 + persistenceDays * day
        )
    }

    @Test("High-spread, persistent entity emits a capped boost")
    func highSpreadBoosts() {
        // 3 of 4 shows (ratio 0.75 ≥ 0.40), 3 distinct shows (≥ 3), 30 days (≥ 14).
        let p = profile(distinct: 3, total: 4, persistenceDays: 30)
        let entries = evaluator.buildBoostEntries(profile: p, cap: cap)
        #expect(entries.count == 1)
        let weight = try? #require(entries.first?.weight)
        // weight = ratio * cap = 0.75 * 0.20 = 0.15, clamped to cap.
        #expect(abs((weight ?? 0) - 0.15) < 1e-9)
        #expect((weight ?? 1) <= cap)
    }

    @Test("Single-show entity does NOT boost (no syndication)")
    func singleShowNoBoost() {
        // 1 of 1 show: ratio 1.0 but only ONE distinct show — below minDistinctShows.
        let p = profile(distinct: 1, total: 1, persistenceDays: 60)
        #expect(evaluator.buildBoostEntries(profile: p, cap: cap).isEmpty)
        #expect(evaluator.qualifies(p) == false)
    }

    @Test("Multi-show RECENT BURST does NOT boost without temporal persistence")
    func recentBurstNoBoost() {
        // Apple at a product launch: spans many shows the same week (ratio high,
        // distinct high) but only 3 days of persistence — below minPersistenceDays.
        let p = profile(distinct: 5, total: 6, persistenceDays: 3)
        #expect(evaluator.qualifies(p) == false)
        #expect(evaluator.buildBoostEntries(profile: p, cap: cap).isEmpty)
    }

    @Test("Spread ratio below threshold does NOT boost even with persistence")
    func lowSpreadNoBoost() {
        // 3 distinct shows but a large library (3 of 20 = 0.15 < 0.40).
        let p = profile(distinct: 3, total: 20, persistenceDays: 60)
        #expect(evaluator.qualifies(p) == false)
        #expect(evaluator.buildBoostEntries(profile: p, cap: cap).isEmpty)
    }

    @Test("Nil profile / zero cap ⇒ no entry")
    func nilOrZeroCap() {
        #expect(evaluator.buildBoostEntries(profile: nil, cap: cap).isEmpty)
        let p = profile(distinct: 3, total: 4, persistenceDays: 30)
        #expect(evaluator.buildBoostEntries(profile: p, cap: 0).isEmpty)
    }

    @Test("Boost weight never exceeds the cap (blanket coverage)")
    func neverExceedsCap() {
        let p = profile(distinct: 6, total: 6, persistenceDays: 90) // ratio 1.0
        let weight = evaluator.buildBoostEntries(profile: p, cap: cap).first?.weight ?? 0
        #expect(weight == cap)
        #expect(weight <= cap)
    }

    @Test("Source kind is .crossShowSyndication with distinct-show count detail")
    func sourceKind() throws {
        let p = profile(distinct: 4, total: 5, persistenceDays: 30)
        let entry = try #require(evaluator.buildBoostEntries(profile: p, cap: cap).first)
        #expect(entry.source == .crossShowSyndication)
        if case .catalog(let entryCount) = entry.detail {
            #expect(entryCount == 4)
        } else {
            Issue.record("expected .catalog detail carrying the distinct-show count")
        }
    }
}

// MARK: - Sponsor-entity extraction helpers

@Suite("CrossShowSyndication sponsor-entity extraction (playhead-xsdz.13)")
struct CrossShowSyndicationExtractionTests {

    private func brandEntry(
        ref: Int,
        normalized: String,
        count: Int = 1,
        start: Double = 10,
        end: Double = 12
    ) -> EvidenceEntry {
        EvidenceEntry(
            evidenceRef: ref,
            category: .brandSpan,
            matchedText: normalized,
            normalizedText: normalized,
            atomOrdinal: ref,
            startTime: start,
            endTime: end,
            count: count,
            firstTime: start,
            lastTime: end
        )
    }

    @Test("Write set: only brand spans, deduped, confidence rises with repetition")
    func writeSet() {
        let entries = [
            brandEntry(ref: 0, normalized: "betterhelp", count: 1),
            brandEntry(ref: 1, normalized: "squarespace", count: 4),
            // A non-brand entry must be ignored.
            EvidenceEntry(
                evidenceRef: 2, category: .url, matchedText: "betterhelp.com",
                normalizedText: "betterhelp.com", atomOrdinal: 2, startTime: 1, endTime: 2
            ),
            // Too-short entity is filtered.
            brandEntry(ref: 3, normalized: "ab", count: 9),
        ]
        let obs = AdDetectionService.crossShowSponsorObservations(from: entries)
        #expect(obs.map(\.normalizedEntity) == ["betterhelp", "squarespace"])
        // single mention → 0.5 (just clears the 0.5 write bar)
        #expect(abs(obs[0].confidence - 0.5) < 1e-9)
        // 4 mentions → 0.5 + 0.1*3 = 0.8
        #expect(abs(obs[1].confidence - 0.8) < 1e-9)
    }

    @Test("Read set: only brand spans overlapping the span, deterministic order")
    func readSet() {
        let span = DecodedSpan(
            id: "s1",
            assetId: "asset-xsdz13",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 9,
            startTime: 10.0,
            endTime: 20.0,
            anchorProvenance: []
        )
        let entries = [
            brandEntry(ref: 0, normalized: "betterhelp", start: 11, end: 12), // overlaps
            brandEntry(ref: 1, normalized: "faraway", start: 100, end: 101),   // no overlap
            brandEntry(ref: 2, normalized: "squarespace", start: 18, end: 19), // overlaps
        ]
        let read = AdDetectionService.crossShowSponsorEntities(from: entries, overlapping: span)
        #expect(read == ["betterhelp", "squarespace"])
    }
}
