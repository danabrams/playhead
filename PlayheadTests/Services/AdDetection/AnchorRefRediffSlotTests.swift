// AnchorRefRediffSlotTests.swift
// playhead-xsdz.29: plumbing coverage for the bare `AnchorRef.rediffSlot`
// provenance marker. MIRRORS the xsdz.22 `AnchorRefSpliceSlotTests` set exactly
// (that suite lives in DecodedSpanPersistenceTests.swift) because `.rediffSlot`
// is the same BARE shape and is exposed to the same three failure modes:
//
//   1. The MANUAL `Equatable` has a `default: return false` arm, so a missing
//      `.rediffSlot` case would silently make `.rediffSlot != .rediffSlot`
//      WITHOUT a compiler error — breaking `DecodedSpan` equality and
//      `contains(.rediffSlot)`. We test `== self` and `!=` every sibling
//      (including `.spliceSlot`, so the two bare markers stay distinct).
//   2. Codable must use a STABLE `"rediffSlot"` type string, and unknown types
//      must still throw at the element level (feeding the Lossy rollback-drop
//      path).
//   3. The case is INERT to eligibility gating: `[X, .rediffSlot]` must yield
//      the IDENTICAL `SkipEligibilityGate` as `[X]` for every anchor class.

import Foundation
import Testing
@testable import Playhead

@Suite("AnchorRef.rediffSlot plumbing")
struct AnchorRefRediffSlotTests {

    // MARK: - Fixtures

    /// One representative value of every NON-rediffSlot case, for `!=` coverage.
    /// INCLUDES `.spliceSlot` — the sibling bare marker must stay distinct.
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
        .spliceSlot,
    ]

    // MARK: - Equatable (default:false trap)

    @Test("rediffSlot == rediffSlot (default:false trap closed)")
    func rediffSlotEqualsItself() {
        #expect(AnchorRef.rediffSlot == AnchorRef.rediffSlot)
    }

    @Test("rediffSlot != every other AnchorRef case (incl. the sibling spliceSlot)")
    func rediffSlotNotEqualToOthers() {
        for other in Self.otherCases {
            #expect(AnchorRef.rediffSlot != other, "rediffSlot must differ from \(other)")
            #expect(other != AnchorRef.rediffSlot, "== must be symmetric for \(other)")
        }
    }

    @Test("the two bare markers are distinct (rediffSlot != spliceSlot)")
    func rediffSlotDistinctFromSpliceSlot() {
        #expect(AnchorRef.rediffSlot != AnchorRef.spliceSlot)
        #expect(AnchorRef.spliceSlot != AnchorRef.rediffSlot)
    }

    // MARK: - isWidthOwnership (the shared width-ownership proxy predicate)

    @Test("isWidthOwnership is true for BOTH bare slot markers, false for every presence anchor")
    func isWidthOwnershipCoversBothMarkers() {
        #expect(AnchorRef.rediffSlot.isWidthOwnership)
        #expect(AnchorRef.spliceSlot.isWidthOwnership)
        for other in Self.otherCases where other != .spliceSlot {
            #expect(!other.isWidthOwnership, "\(other) is a PRESENCE anchor, not a width marker")
        }
    }

    // MARK: - Codable

    @Test("rediffSlot Codable round-trips to itself")
    func rediffSlotCodableRoundTrip() throws {
        let data = try JSONEncoder().encode(AnchorRef.rediffSlot)
        let decoded = try JSONDecoder().decode(AnchorRef.self, from: data)
        #expect(decoded == .rediffSlot)
    }

    @Test("rediffSlot encodes a STABLE 'rediffSlot' type string with no payload")
    func rediffSlotStableTypeString() throws {
        let data = try JSONEncoder().encode(AnchorRef.rediffSlot)
        let object = try JSONDecoder().decode([String: String].self, from: data)
        #expect(object["type"] == "rediffSlot")
        // Bare case: the type string is the ENTIRE encoding — no other keys.
        #expect(object.count == 1, "rediffSlot must encode no associated values, got \(object)")
    }

    @Test("rediffSlot and spliceSlot encode DIFFERENT stable type strings")
    func rediffAndSpliceEncodeDistinctTypeStrings() throws {
        let rediff = try JSONDecoder().decode(
            [String: String].self, from: try JSONEncoder().encode(AnchorRef.rediffSlot))
        let splice = try JSONDecoder().decode(
            [String: String].self, from: try JSONEncoder().encode(AnchorRef.spliceSlot))
        #expect(rediff["type"] == "rediffSlot")
        #expect(splice["type"] == "spliceSlot")
        #expect(rediff["type"] != splice["type"])
    }

    @Test("Array containing rediffSlot round-trips with order and neighbors intact")
    func rediffSlotArrayRoundTrip() throws {
        let provenance: [AnchorRef] = [
            .fmConsensus(regionId: "r1", consensusStrength: 0.5),
            .rediffSlot,
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
        #expect(decoded[1] == .rediffSlot)
        #expect(decoded[2] == .spliceSlot)
    }

    @Test("Adding rediffSlot does not break decoding pre-change persisted artifacts")
    func backwardCompatDecodeOfPreChangeArtifact() throws {
        // A literal `anchorProvenance` JSON exactly as a build PREDATING
        // xsdz.29 would have written it (no `rediffSlot` case existed). Includes
        // a `spliceSlot` element (which DID exist pre-xsdz.29) to prove the
        // additive arm leaves every legacy arm untouched.
        let legacyJSON = """
        [
          {"type":"fmConsensus","regionId":"rgn-alpha","consensusStrength":0.85},
          {"type":"spliceSlot"},
          {"type":"userCorrection","correctionId":"corr-1","reportedTime":33.5},
          {"type":"classifierSeed","regionId":"rgn-gamma","score":0.91}
        ]
        """
        let decoded = try JSONDecoder().decode([AnchorRef].self, from: Data(legacyJSON.utf8))
        #expect(decoded.count == 4)
        #expect(decoded[0] == .fmConsensus(regionId: "rgn-alpha", consensusStrength: 0.85))
        #expect(decoded[1] == .spliceSlot)
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
        // build predating xsdz.29, `.rediffSlot` IS exactly such a type: the old
        // `AnchorRef(from:)` throws on it, `LossyAnchorRef` swallows the throw to
        // `nil`, and the surrounding span keeps every anchor it DOES recognize
        // plus its interval — reading as non-rediff-slot-owned (status-quo width).
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

    @Test("A newer build persists+fetches a rediffSlot row intact (forward round-trip)")
    func rediffSlotStoreRoundTripSurvives() async throws {
        // On the CURRENT build (which knows `.rediffSlot`), the per-element
        // tolerant fetch path in AnalysisStore.fetchDecodedSpans preserves the
        // marker — only an OLDER build drops it (covered above at the JSON level,
        // since a persisted `.rediffSlot` row cannot be produced without this
        // build's encoder).
        let dir = try Self.makeTempDir()
        Self.storeDirs.append(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let assetId = "rediffslot-asset"
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
            .rediffSlot,
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
        #expect(fetchedProv.count == 2, "rediffSlot marker must survive a same-build round-trip")
        #expect(fetchedProv.contains(.rediffSlot))
    }

    // MARK: - Gate inertness (item 5: [X, .rediffSlot] gate == [X] gate)

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

    @Test("rediffSlot is inert to the fmConsensus gate branch")
    func gateInertFMConsensus() {
        let ledger: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .lexical, weight: 0.2, detail: .lexical(matchedCategories: ["cta"])),
            EvidenceLedgerEntry(source: .acoustic, weight: 0.2, detail: .acoustic(breakStrength: 0.5)),
        ]
        let base = gate(provenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9)], ledger: ledger)
        let withSlot = gate(
            provenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9), .rediffSlot],
            ledger: ledger
        )
        #expect(base == .eligible, "sanity: base fmConsensus span should be eligible")
        #expect(withSlot == base, "rediffSlot must not change the fmConsensus gate")
    }

    @Test("rediffSlot is inert to the fmAcousticCorroborated gate branch")
    func gateInertFMAcoustic() {
        let ledger: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .acoustic, weight: 0.2, detail: .acoustic(breakStrength: 0.5)),
        ]
        let base = gate(provenance: [.fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7)], ledger: ledger)
        let withSlot = gate(
            provenance: [.fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7), .rediffSlot],
            ledger: ledger
        )
        #expect(base == .eligible, "sanity: fmAcoustic span with acoustic corroboration should be eligible")
        #expect(withSlot == base, "rediffSlot must not change the fmAcoustic gate")
    }

    @Test("rediffSlot does not count as a corroborator in the non-FM (metadata) gate")
    func gateInertNonFMBlocked() {
        // Metadata-only ledger, non-FM provenance → metadataCorroborationGate
        // blocks for lack of in-audio corroboration. If rediffSlot leaked into
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
            provenance: [.classifierSeed(regionId: "r3", score: 0.6), .rediffSlot],
            ledger: ledger
        )
        #expect(base == .blockedByEvidenceQuorum, "sanity: metadata-only non-FM span must be blocked")
        #expect(withSlot == base, "rediffSlot must NOT corroborate — gate stays blocked")
    }

    // MARK: - Temp dir helpers (self-contained)

    private static let storeDirs = TestTempDirBox()

    private static func makeTempDir() throws -> URL {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("AnchorRefRediffSlotTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }
}

/// Minimal thread-safe temp-dir tracker (avoids depending on the test-target's
/// `TestTempDirTracker` internals; cleanup is best-effort at process exit).
final class TestTempDirBox: @unchecked Sendable {
    private let lock = NSLock()
    private var dirs: [URL] = []
    func append(_ url: URL) {
        lock.lock(); dirs.append(url); lock.unlock()
    }
    deinit {
        for dir in dirs { try? FileManager.default.removeItem(at: dir) }
    }
}
