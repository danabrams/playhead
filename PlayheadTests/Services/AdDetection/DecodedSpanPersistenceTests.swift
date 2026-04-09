// DecodedSpanPersistenceTests.swift
// Phase 5 (playhead-4my.5.2): Tests for decoded_spans persistence round-trip.
// Covers: upsertDecodedSpans, fetchDecodedSpans, idempotent re-runs.

import Foundation
import Testing

@testable import Playhead

@Suite("DecodedSpan Persistence", .serialized)
struct DecodedSpanPersistenceTests {

    // MARK: - Helpers

    private static let storeDirs = TestTempDirTracker()

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "DecodedSpanPersistenceTests")
        Self.storeDirs.track(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeSpan(
        id: String = "span-1",
        assetId: String = "asset-1",
        firstOrdinal: Int = 10,
        lastOrdinal: Int = 30,
        startTime: Double = 10.0,
        endTime: Double = 30.0,
        provenance: [AnchorRef] = []
    ) -> DecodedSpan {
        DecodedSpan(
            id: id,
            assetId: assetId,
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: provenance
        )
    }

    // MARK: - Tests

    @Test("DecodedSpan rows persist and round-trip via fetchDecodedSpans(assetId:)")
    func persistAndRoundTrip() async throws {
        let store = try await makeStore()
        let assetId = "round-trip-asset"
        try await store.insertAsset(makeAsset(id: assetId))

        let spans = [
            makeSpan(
                id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 5, lastAtomOrdinal: 15),
                assetId: assetId,
                firstOrdinal: 5,
                lastOrdinal: 15,
                startTime: 5.0,
                endTime: 15.0,
                provenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.7)]
            ),
            makeSpan(
                id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 25, lastAtomOrdinal: 40),
                assetId: assetId,
                firstOrdinal: 25,
                lastOrdinal: 40,
                startTime: 25.0,
                endTime: 40.0,
                provenance: [.fmAcousticCorroborated(regionId: "r2", breakStrength: 0.6)]
            )
        ]

        try await store.upsertDecodedSpans(spans)
        let fetched = try await store.fetchDecodedSpans(assetId: assetId)

        #expect(fetched.count == 2)
        let first = fetched.first { $0.firstAtomOrdinal == 5 }
        let second = fetched.first { $0.firstAtomOrdinal == 25 }

        #expect(first != nil)
        #expect(first?.lastAtomOrdinal == 15)
        #expect(first?.startTime == 5.0)
        #expect(first?.endTime == 15.0)

        #expect(second != nil)
        #expect(second?.lastAtomOrdinal == 40)

        // Provenance round-trips
        guard let firstProv = first?.anchorProvenance.first else {
            Issue.record("Expected anchor provenance on first span")
            return
        }
        if case .fmConsensus(let regionId, let strength) = firstProv {
            #expect(regionId == "r1")
            #expect(strength == 0.7)
        } else {
            Issue.record("Expected fmConsensus provenance")
        }
    }

    @Test("Idempotent upsert: re-inserting same spans does not create duplicates")
    func idempotentUpsert() async throws {
        let store = try await makeStore()
        let assetId = "idempotent-asset"
        try await store.insertAsset(makeAsset(id: assetId))

        let span = makeSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 10, lastAtomOrdinal: 20),
            assetId: assetId,
            firstOrdinal: 10,
            lastOrdinal: 20,
            startTime: 10.0,
            endTime: 20.0
        )

        try await store.upsertDecodedSpans([span])
        try await store.upsertDecodedSpans([span])  // second insert — should not duplicate

        let fetched = try await store.fetchDecodedSpans(assetId: assetId)
        #expect(fetched.count == 1)
    }

    @Test("fetchDecodedSpans returns spans ordered by startTime")
    func fetchOrderedByStartTime() async throws {
        let store = try await makeStore()
        let assetId = "order-asset"
        try await store.insertAsset(makeAsset(id: assetId))

        let spans = [
            makeSpan(
                id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 50, lastAtomOrdinal: 60),
                assetId: assetId,
                firstOrdinal: 50,
                lastOrdinal: 60,
                startTime: 50.0,
                endTime: 60.0
            ),
            makeSpan(
                id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 10, lastAtomOrdinal: 20),
                assetId: assetId,
                firstOrdinal: 10,
                lastOrdinal: 20,
                startTime: 10.0,
                endTime: 20.0
            ),
        ]

        try await store.upsertDecodedSpans(spans)
        let fetched = try await store.fetchDecodedSpans(assetId: assetId)

        #expect(fetched.count == 2)
        #expect(fetched[0].startTime < fetched[1].startTime)
        #expect(fetched[0].firstAtomOrdinal == 10)
        #expect(fetched[1].firstAtomOrdinal == 50)
    }

    @Test("fetchDecodedSpans for different assetIds are isolated")
    func differentAssetsAreIsolated() async throws {
        let store = try await makeStore()
        let assetA = "asset-A"
        let assetB = "asset-B"
        try await store.insertAsset(makeAsset(id: assetA))
        try await store.insertAsset(makeAsset(id: assetB))

        let spanA = makeSpan(
            id: DecodedSpan.makeId(assetId: assetA, firstAtomOrdinal: 5, lastAtomOrdinal: 15),
            assetId: assetA,
            firstOrdinal: 5,
            lastOrdinal: 15,
            startTime: 5.0,
            endTime: 15.0
        )
        let spanB = makeSpan(
            id: DecodedSpan.makeId(assetId: assetB, firstAtomOrdinal: 5, lastAtomOrdinal: 15),
            assetId: assetB,
            firstOrdinal: 5,
            lastOrdinal: 15,
            startTime: 5.0,
            endTime: 15.0
        )

        try await store.upsertDecodedSpans([spanA, spanB])

        let fetchedA = try await store.fetchDecodedSpans(assetId: assetA)
        let fetchedB = try await store.fetchDecodedSpans(assetId: assetB)

        #expect(fetchedA.count == 1)
        #expect(fetchedA[0].assetId == assetA)
        #expect(fetchedB.count == 1)
        #expect(fetchedB[0].assetId == assetB)
    }

    @Test("Spans with empty provenance round-trip correctly")
    func emptyProvenanceRoundTrips() async throws {
        let store = try await makeStore()
        let assetId = "empty-prov-asset"
        try await store.insertAsset(makeAsset(id: assetId))

        let span = makeSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 0, lastAtomOrdinal: 10),
            assetId: assetId,
            firstOrdinal: 0,
            lastOrdinal: 10,
            startTime: 0.0,
            endTime: 10.0,
            provenance: []
        )

        try await store.upsertDecodedSpans([span])
        let fetched = try await store.fetchDecodedSpans(assetId: assetId)
        #expect(fetched.count == 1)
        #expect(fetched[0].anchorProvenance.isEmpty)
    }

    @Test("Evidencecatalog provenance entry round-trips via JSON")
    func evidenceCatalogProvenanceRoundTrips() async throws {
        let store = try await makeStore()
        let assetId = "evidence-asset"
        try await store.insertAsset(makeAsset(id: assetId))

        let entry = EvidenceEntry(
            evidenceRef: 42,
            category: .url,
            matchedText: "acme.com",
            normalizedText: "acme.com",
            atomOrdinal: 5,
            startTime: 5.0,
            endTime: 5.5
        )
        let span = makeSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 5, lastAtomOrdinal: 15),
            assetId: assetId,
            firstOrdinal: 5,
            lastOrdinal: 15,
            startTime: 5.0,
            endTime: 15.0,
            provenance: [.evidenceCatalog(entry: entry)]
        )

        try await store.upsertDecodedSpans([span])
        let fetched = try await store.fetchDecodedSpans(assetId: assetId)

        guard let first = fetched.first, let prov = first.anchorProvenance.first else {
            Issue.record("Expected fetched span with provenance")
            return
        }
        if case .evidenceCatalog(let fetchedEntry) = prov {
            #expect(fetchedEntry.evidenceRef == 42)
            #expect(fetchedEntry.matchedText == "acme.com")
            #expect(fetchedEntry.category == .url)
        } else {
            Issue.record("Expected evidenceCatalog provenance")
        }
    }
}
