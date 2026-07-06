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

    @Test("Mixed anchor provenance round-trips all types and values (M3-UI decode-failure guard)")
    func mixedAnchorProvenanceRoundTrips() async throws {
        let store = try await makeStore()
        let assetId = "mixed-prov-asset"
        try await store.insertAsset(makeAsset(id: assetId))

        let evidenceEntry = EvidenceEntry(
            evidenceRef: 99,
            category: .url,
            matchedText: "sponsor.example.com/offer",
            normalizedText: "sponsor.example.com",
            atomOrdinal: 12,
            startTime: 12.0,
            endTime: 12.8
        )
        let provenance: [AnchorRef] = [
            .fmConsensus(regionId: "rgn-alpha", consensusStrength: 0.85),
            .fmAcousticCorroborated(regionId: "rgn-beta", breakStrength: 0.42),
            .evidenceCatalog(entry: evidenceEntry),
            .fmConsensus(regionId: "rgn-gamma", consensusStrength: 1.0),
        ]

        let span = makeSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 10, lastAtomOrdinal: 35),
            assetId: assetId,
            firstOrdinal: 10,
            lastOrdinal: 35,
            startTime: 10.0,
            endTime: 35.0,
            provenance: provenance
        )

        try await store.upsertDecodedSpans([span])
        let fetched = try await store.fetchDecodedSpans(assetId: assetId)

        #expect(fetched.count == 1)
        let fetchedProv = fetched[0].anchorProvenance
        #expect(fetchedProv.count == provenance.count, "Anchor count must survive round-trip — silent decode failures drop entries")

        // Verify each entry by index to catch ordering or type corruption.
        if case .fmConsensus(let rid, let str) = fetchedProv[0] {
            #expect(rid == "rgn-alpha")
            #expect(str == 0.85)
        } else {
            Issue.record("Expected fmConsensus at index 0, got \(fetchedProv[0])")
        }

        if case .fmAcousticCorroborated(let rid, let str) = fetchedProv[1] {
            #expect(rid == "rgn-beta")
            #expect(str == 0.42)
        } else {
            Issue.record("Expected fmAcousticCorroborated at index 1, got \(fetchedProv[1])")
        }

        if case .evidenceCatalog(let entry) = fetchedProv[2] {
            #expect(entry.evidenceRef == 99)
            #expect(entry.matchedText == "sponsor.example.com/offer")
            #expect(entry.normalizedText == "sponsor.example.com")
            #expect(entry.category == .url)
            #expect(entry.atomOrdinal == 12)
            #expect(entry.startTime == 12.0)
            #expect(entry.endTime == 12.8)
        } else {
            Issue.record("Expected evidenceCatalog at index 2, got \(fetchedProv[2])")
        }

        if case .fmConsensus(let rid, let str) = fetchedProv[3] {
            #expect(rid == "rgn-gamma")
            #expect(str == 1.0)
        } else {
            Issue.record("Expected fmConsensus at index 3, got \(fetchedProv[3])")
        }
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

// MARK: - AnchorRef.spliceSlot (playhead-xsdz.22)

/// Plumbing coverage for the bare `AnchorRef.spliceSlot` provenance marker
/// added by playhead-xsdz.22. The case carries NO associated values —
/// presence in `anchorProvenance` is the entire marker. These tests pin the
/// three failure modes the bare shape is exposed to:
///
///   1. The MANUAL `Equatable` has a `default: return false` arm, so a missing
///      `.spliceSlot` case would silently make `.spliceSlot != .spliceSlot`
///      WITHOUT a compiler error — breaking `DecodedSpan` equality and
///      `contains(.spliceSlot)`. We test `== self` and `!=` every sibling.
///   2. Codable must use a STABLE `"spliceSlot"` type string, and unknown
///      types must still throw at the element level (feeding the Lossy
///      rollback-drop path).
///   3. The case is INERT to eligibility gating: `[X, .spliceSlot]` must yield
///      the IDENTICAL `SkipEligibilityGate` as `[X]` for every anchor class.
@Suite("AnchorRef.spliceSlot plumbing")
struct AnchorRefSpliceSlotTests {

    // MARK: - Fixtures

    /// One representative value of every NON-spliceSlot case, for `!=` coverage.
    private static let otherCases: [AnchorRef] = [
        .fmConsensus(regionId: "r1", consensusStrength: 0.9),
        .evidenceCatalog(entry: EvidenceEntry(
            evidenceRef: 7,
            category: .url,
            matchedText: "acme.com",
            normalizedText: "acme.com",
            atomOrdinal: 3,
            startTime: 3.0,
            endTime: 3.5
        )),
        .fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7),
        .userCorrection(correctionId: "c1", reportedTime: 12.0),
        .classifierSeed(regionId: "r3", score: 0.8),
    ]

    // MARK: - Equatable (default:false trap)

    @Test("spliceSlot == spliceSlot (default:false trap closed)")
    func spliceSlotEqualsItself() {
        #expect(AnchorRef.spliceSlot == AnchorRef.spliceSlot)
    }

    @Test("spliceSlot != every other AnchorRef case")
    func spliceSlotNotEqualToOthers() {
        for other in Self.otherCases {
            #expect(AnchorRef.spliceSlot != other, "spliceSlot must differ from \(other)")
            #expect(other != AnchorRef.spliceSlot, "== must be symmetric for \(other)")
        }
    }

    // MARK: - Codable

    @Test("spliceSlot Codable round-trips to itself")
    func spliceSlotCodableRoundTrip() throws {
        let data = try JSONEncoder().encode(AnchorRef.spliceSlot)
        let decoded = try JSONDecoder().decode(AnchorRef.self, from: data)
        #expect(decoded == .spliceSlot)
    }

    @Test("spliceSlot encodes a STABLE 'spliceSlot' type string with no payload")
    func spliceSlotStableTypeString() throws {
        let data = try JSONEncoder().encode(AnchorRef.spliceSlot)
        let object = try JSONDecoder().decode([String: String].self, from: data)
        #expect(object["type"] == "spliceSlot")
        // Bare case: the type string is the ENTIRE encoding — no other keys.
        #expect(object.count == 1, "spliceSlot must encode no associated values, got \(object)")
    }

    @Test("Array containing spliceSlot round-trips with order and neighbors intact")
    func spliceSlotArrayRoundTrip() throws {
        let provenance: [AnchorRef] = [
            .fmConsensus(regionId: "r1", consensusStrength: 0.5),
            .spliceSlot,
            .evidenceCatalog(entry: EvidenceEntry(
                evidenceRef: 1,
                category: .promoCode,
                matchedText: "SAVE10",
                normalizedText: "save10",
                atomOrdinal: 2,
                startTime: 2.0,
                endTime: 2.4
            )),
        ]
        let data = try JSONEncoder().encode(provenance)
        let decoded = try JSONDecoder().decode([AnchorRef].self, from: data)
        #expect(decoded == provenance)
        #expect(decoded[1] == .spliceSlot)
    }

    @Test("Adding spliceSlot does not break decoding pre-change persisted artifacts")
    func backwardCompatDecodeOfPreChangeArtifact() throws {
        // A literal `anchorProvenance` JSON exactly as a build PREDATING
        // xsdz.22 would have written it (no `spliceSlot` case existed). The
        // additive switch arm must leave every legacy arm untouched.
        let legacyJSON = """
        [
          {"type":"fmConsensus","regionId":"rgn-alpha","consensusStrength":0.85},
          {"type":"fmAcousticCorroborated","regionId":"rgn-beta","breakStrength":0.42},
          {"type":"userCorrection","correctionId":"corr-1","reportedTime":33.5},
          {"type":"classifierSeed","regionId":"rgn-gamma","score":0.91}
        ]
        """
        let decoded = try JSONDecoder().decode([AnchorRef].self, from: Data(legacyJSON.utf8))
        #expect(decoded.count == 4)
        #expect(decoded[0] == .fmConsensus(regionId: "rgn-alpha", consensusStrength: 0.85))
        #expect(decoded[1] == .fmAcousticCorroborated(regionId: "rgn-beta", breakStrength: 0.42))
        #expect(decoded[2] == .userCorrection(correctionId: "corr-1", reportedTime: 33.5))
        #expect(decoded[3] == .classifierSeed(regionId: "rgn-gamma", score: 0.91))
    }

    // MARK: - Rollback semantics (LossyAnchorRef unknown-type drop)

    @Test("AnchorRef(from:) throws on an unknown type string")
    func anchorRefThrowsOnUnknownType() {
        let unknownJSON = Data(#"{"type":"futureUnknownCase"}"#.utf8)
        #expect(throws: (any Error).self) {
            _ = try JSONDecoder().decode(AnchorRef.self, from: unknownJSON)
        }
    }

    @Test("LossyAnchorRef drops an unknown-type element (rollback drop semantics)")
    func lossyAnchorRefDropsUnknownType() throws {
        // Simulates an OLDER build reading a row written by a NEWER build that
        // carries a case the old build has no switch arm for. Relative to any
        // build predating xsdz.22, `.spliceSlot` IS exactly such a type: the
        // old `AnchorRef(from:)` throws on it, `LossyAnchorRef` swallows the
        // throw to `nil`, and the surrounding span keeps every anchor it DOES
        // recognize plus its interval — reading as non-slot-owned.
        let single = try JSONDecoder().decode(
            LossyAnchorRef.self,
            from: Data(#"{"type":"futureUnknownCase"}"#.utf8)
        )
        #expect(single.value == nil)

        let mixedJSON = """
        [
          {"type":"fmConsensus","regionId":"r1","consensusStrength":0.5},
          {"type":"futureUnknownCase"},
          {"type":"classifierSeed","regionId":"r2","score":0.8}
        ]
        """
        let wrapped = try JSONDecoder().decode([LossyAnchorRef].self, from: Data(mixedJSON.utf8))
        let survivors = wrapped.compactMap(\.value)
        #expect(survivors.count == 2, "only the unknown-type element should drop")
        #expect(survivors[0] == .fmConsensus(regionId: "r1", consensusStrength: 0.5))
        #expect(survivors[1] == .classifierSeed(regionId: "r2", score: 0.8))
    }

    @Test("A newer build persists+fetches a spliceSlot row intact (forward round-trip)")
    func spliceSlotStoreRoundTripSurvives() async throws {
        // On the CURRENT build (which knows `.spliceSlot`), the per-element
        // tolerant fetch path in AnalysisStore.fetchDecodedSpans preserves the
        // marker — only an OLDER build drops it (covered above at the JSON
        // level, since a persisted `.spliceSlot` row cannot be produced without
        // this build's encoder).
        let dir = try makeTempDir(prefix: "AnchorRefSpliceSlotTests")
        Self.storeDirs.track(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let assetId = "spliceslot-asset"
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "ep-\(assetId)",
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        let provenance: [AnchorRef] = [
            .spliceSlot,
            .fmConsensus(regionId: "r1", consensusStrength: 0.7),
        ]
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 5, lastAtomOrdinal: 15),
            assetId: assetId,
            firstAtomOrdinal: 5,
            lastAtomOrdinal: 15,
            startTime: 5.0,
            endTime: 15.0,
            anchorProvenance: provenance
        )
        try await store.upsertDecodedSpans([span])
        let fetched = try await store.fetchDecodedSpans(assetId: assetId)

        #expect(fetched.count == 1)
        let fetchedProv = fetched[0].anchorProvenance
        #expect(fetchedProv.count == 2, "spliceSlot marker must survive a same-build round-trip")
        #expect(fetchedProv.contains(.spliceSlot))
    }

    private static let storeDirs = TestTempDirTracker()

    // MARK: - Gate inertness (item 5: [X, .spliceSlot] gate == [X] gate)

    /// Build the eligibility gate for a span with the given provenance and ledger.
    private func gate(
        provenance: [AnchorRef],
        ledger: [EvidenceLedgerEntry]
    ) -> SkipEligibilityGate {
        // 30s span (within the [5, 180] quorum window) so duration never
        // confounds the fmConsensus branch.
        let span = DecodedSpan(
            id: "gate-span",
            assetId: "gate-asset",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: provenance
        )
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        return mapper.map().eligibilityGate
    }

    @Test("spliceSlot is inert to the fmConsensus gate branch")
    func gateInertFMConsensus() {
        // Two distinct corroborating ledger kinds → fmConsensus quorum met.
        let ledger: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .lexical, weight: 0.2, detail: .lexical(matchedCategories: ["cta"])),
            EvidenceLedgerEntry(source: .acoustic, weight: 0.2, detail: .acoustic(breakStrength: 0.5)),
        ]
        let base = gate(provenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9)], ledger: ledger)
        let withSlot = gate(
            provenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9), .spliceSlot],
            ledger: ledger
        )
        #expect(base == .eligible, "sanity: base fmConsensus span should be eligible")
        #expect(withSlot == base, "spliceSlot must not change the fmConsensus gate")
    }

    @Test("spliceSlot is inert to the fmAcousticCorroborated gate branch")
    func gateInertFMAcoustic() {
        let ledger: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .acoustic, weight: 0.2, detail: .acoustic(breakStrength: 0.5)),
        ]
        let base = gate(provenance: [.fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7)], ledger: ledger)
        let withSlot = gate(
            provenance: [.fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7), .spliceSlot],
            ledger: ledger
        )
        #expect(base == .eligible, "sanity: fmAcoustic span with acoustic corroboration should be eligible")
        #expect(withSlot == base, "spliceSlot must not change the fmAcoustic gate")
    }

    @Test("spliceSlot does not count as a corroborator in the non-FM (metadata) gate")
    func gateInertNonFMBlocked() {
        // Metadata-only ledger, non-FM provenance → metadataCorroborationGate
        // blocks for lack of in-audio corroboration. If spliceSlot leaked into
        // the quorum it would hand this span a free corroborator and flip the
        // gate to .eligible — the exact double-count the design forbids.
        let ledger: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(
                source: .metadata,
                weight: 0.15,
                detail: .metadata(cueCount: 1, sourceField: .description, dominantCueType: .sponsorAlias)
            ),
        ]
        let base = gate(provenance: [.classifierSeed(regionId: "r3", score: 0.6)], ledger: ledger)
        let withSlot = gate(
            provenance: [.classifierSeed(regionId: "r3", score: 0.6), .spliceSlot],
            ledger: ledger
        )
        #expect(base == .blockedByEvidenceQuorum, "sanity: metadata-only non-FM span must be blocked")
        #expect(withSlot == base, "spliceSlot must NOT corroborate — gate stays blocked")
    }
}
