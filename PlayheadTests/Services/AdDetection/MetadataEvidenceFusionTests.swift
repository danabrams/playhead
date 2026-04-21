// MetadataEvidenceFusionTests.swift
// playhead-z3ch: TDD tests for metadata signal elevation through fusion.
//
// Coverage:
//   1. metadataCap (FusionWeightConfig default = 0.15) is enforced via
//      FusionBudgetClamp on metadata entries inside `buildLedger()`.
//   2. Metadata-only ledgers gate to `.blockedByEvidenceQuorum` via the
//      no-FM-provenance corroboration check (Q3).
//   3. Metadata + a corroborating in-audio (lexical) hit gate to `.eligible`.
//   4. Integration: a synthetic FeedDescriptionEvidenceBuilder pipeline
//      (FeedDescriptionMetadata → MetadataCueExtractor → builder → fusion)
//      lands metadata in the ledger and respects the clamp + gate above.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-z3ch — metadata fusion elevation")
struct MetadataEvidenceFusionTests {

    // MARK: - Fixtures

    private func makeSpan(
        assetId: String = "asset-z3ch",
        startTime: Double = 10.0,
        endTime: Double = 40.0,
        anchorProvenance: [AnchorRef] = []
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 100, lastAtomOrdinal: 200),
            assetId: assetId,
            firstAtomOrdinal: 100,
            lastAtomOrdinal: 200,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: anchorProvenance
        )
    }

    private func metadataEntry(weight: Double) -> EvidenceLedgerEntry {
        EvidenceLedgerEntry(
            source: .metadata,
            weight: weight,
            detail: .metadata(
                cueCount: 1,
                sourceField: .description,
                dominantCueType: .disclosure
            )
        )
    }

    // MARK: - Cap enforcement (Q4 #1)

    @Test("metadata entries above metadataCap are hard-clamped to 0.15 inside buildLedger()")
    func metadataEntryClampedAtCap() {
        let span = makeSpan()
        let oversized = metadataEntry(weight: 0.9) // raw > metadataCap (0.15)
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            metadataEntries: [oversized],
            mode: .off,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let metadataInLedger = ledger.filter { $0.source == .metadata }
        #expect(metadataInLedger.count == 1)
        #expect(metadataInLedger.first?.weight == 0.15, "metadata entry must be clamped at metadataCap (0.15)")
    }

    @Test("metadata entries at or below metadataCap pass through unchanged")
    func metadataEntryUnderCapUnchanged() {
        let span = makeSpan()
        let underCap = metadataEntry(weight: 0.10)
        let atCap = metadataEntry(weight: 0.15)
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            metadataEntries: [underCap, atCap],
            mode: .off,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let weights = ledger.filter { $0.source == .metadata }.map(\.weight).sorted()
        #expect(weights == [0.10, 0.15])
    }

    @Test("FusionWeightConfig defaults metadataCap to 0.15 (Plan §7.4)")
    func metadataCapDefaultMatchesSpec() {
        let cfg = FusionWeightConfig()
        #expect(cfg.metadataCap == 0.15)
    }

    // MARK: - Corroboration gate (Q3, Q4 #2 + #3)

    @Test("metadata-only ledger gates to blockedByEvidenceQuorum")
    func metadataAloneIsBlocked() {
        // Build a span with NO FM provenance, classifier score = 0 (so the
        // always-present .classifier entry has zero weight). Only metadata
        // contributes weighted evidence — the corroboration gate must block.
        let span = makeSpan(anchorProvenance: [])
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            metadataEntries: [metadataEntry(weight: 0.15)],
            mode: .off,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let decision = mapper.map()
        #expect(decision.eligibilityGate == .blockedByEvidenceQuorum,
            "metadata alone (no in-audio corroboration) must NEVER trigger a skip — must gate to blockedByEvidenceQuorum")
    }

    @Test("metadata + corroborating lexical hit produces eligible decision")
    func metadataPlusLexicalIsEligible() {
        let span = makeSpan(anchorProvenance: [])
        let lexical = EvidenceLedgerEntry(
            source: .lexical,
            weight: 0.18,
            detail: .lexical(matchedCategories: ["sponsor"])
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [lexical],
            acousticEntries: [],
            catalogEntries: [],
            metadataEntries: [metadataEntry(weight: 0.15)],
            mode: .off,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let decision = mapper.map()
        #expect(decision.eligibilityGate == .eligible,
            "metadata + at least one in-audio (lexical) hit must satisfy corroboration → .eligible")
    }

    // MARK: - Integration through FeedDescriptionEvidenceBuilder (Q4 fixture)

    @Test("synthetic FeedDescriptionMetadata flows through builder + fusion path with clamp")
    func feedDescriptionPipelineRespectsClamp() {
        // Build a synthetic metadata blob that will trigger MULTIPLE strong cues —
        // disclosure ("brought to you by") + sponsor name + URL — so the builder's
        // raw weight aggregation comfortably exceeds the metadataCap of 0.15.
        let metadata = FeedDescriptionMetadata(
            feedDescription: "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace.com. Ad-free this week thanks to our sponsor.",
            feedSummary: "Sponsored by Squarespace. Visit squarespace.com to start your free trial.",
            sourceHashes: .init(descriptionHash: 1, summaryHash: 2)
        )
        let extractor = MetadataCueExtractor(
            knownSponsors: ["squarespace"],
            showOwnedDomains: [],
            networkOwnedDomains: []
        )
        let cues = extractor.extractCues(
            description: metadata.feedDescription,
            summary: metadata.feedSummary
        )
        #expect(!cues.isEmpty, "synthetic metadata should produce at least one cue")

        let span = makeSpan(anchorProvenance: [])
        let builder = FeedDescriptionEvidenceBuilder()
        let metadataEntries = builder.buildEntries(cues: cues, for: span)

        // The raw aggregate weight from cues SHOULD exceed metadataCap so the
        // clamp is genuinely exercised here (regression catch for raw weight
        // regressions that quietly fall under the cap).
        let rawTotal = metadataEntries.reduce(0.0) { $0 + $1.weight }
        #expect(rawTotal > FusionWeightConfig().metadataCap,
            "synthetic metadata should produce raw weight > metadataCap so the clamp is exercised")

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            metadataEntries: metadataEntries,
            mode: .off,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let metadataLedger = ledger.filter { $0.source == .metadata }
        for entry in metadataLedger {
            #expect(entry.weight <= FusionWeightConfig().metadataCap + 1e-9,
                "every metadata entry in the ledger must respect metadataCap")
        }

        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let decision = mapper.map()
        #expect(decision.eligibilityGate == .blockedByEvidenceQuorum,
            "metadata-only synthetic fixture must gate to blockedByEvidenceQuorum (no in-audio corroboration)")
    }
}
