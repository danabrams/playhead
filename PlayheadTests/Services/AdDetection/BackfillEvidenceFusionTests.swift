// BackfillEvidenceFusionTests.swift
// Tests for EvidenceLedgerEntry, BackfillEvidenceFusion, and DecisionMapper.
//
// TDD: these tests were written first to specify the contract before implementation.

import Foundation
import Testing
@testable import Playhead

@Suite("BackfillEvidenceFusion")
struct BackfillEvidenceFusionTests {

    // MARK: - Helpers

    private func makeSpan(
        assetId: String = "asset-1",
        startTime: Double = 10.0,
        endTime: Double = 40.0,
        anchorProvenance: [AnchorRef] = []
    ) -> DecodedSpan {
        let first = 100
        let last = 200
        return DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: first, lastAtomOrdinal: last),
            assetId: assetId,
            firstAtomOrdinal: first,
            lastAtomOrdinal: last,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: anchorProvenance
        )
    }

    private func makeSpanWithFMConsensus(startTime: Double = 10.0, endTime: Double = 40.0) -> DecodedSpan {
        makeSpan(
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9)]
        )
    }

    private func makeSpanWithFMAcoustic(startTime: Double = 10.0, endTime: Double = 40.0) -> DecodedSpan {
        makeSpan(
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: [.fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7)]
        )
    }

    private func defaultConfig() -> FusionWeightConfig {
        FusionWeightConfig()
    }

    // MARK: - EvidenceLedgerEntry

    @Test("EvidenceLedgerEntry captures source, weight, and detail")
    func ledgerEntryFields() {
        let entry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.35,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )
        #expect(entry.source == .fm)
        #expect(entry.weight == 0.35)
        if case .fm(let disp, let band, let label) = entry.detail {
            #expect(disp == .containsAd)
            #expect(band == .strong)
            #expect(label == "v1")
        } else {
            Issue.record("Expected .fm detail")
        }
    }

    @Test("EvidenceLedgerDetail has all required source-specific variants")
    func ledgerDetailVariants() {
        // classifier
        let c = EvidenceLedgerDetail.classifier(score: 0.8)
        if case .classifier(let s) = c { #expect(s == 0.8) } else { Issue.record("bad classifier") }

        // fm
        let fm = EvidenceLedgerDetail.fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "lbl")
        if case .fm(let d, let b, let l) = fm {
            #expect(d == .containsAd)
            #expect(b == .moderate)
            #expect(l == "lbl")
        } else { Issue.record("bad fm") }

        // lexical
        let lex = EvidenceLedgerDetail.lexical(matchedCategories: ["url", "promoCode"])
        if case .lexical(let cats) = lex { #expect(cats == ["url", "promoCode"]) } else { Issue.record("bad lexical") }

        // acoustic
        let ac = EvidenceLedgerDetail.acoustic(breakStrength: 0.85)
        if case .acoustic(let s) = ac { #expect(s == 0.85) } else { Issue.record("bad acoustic") }

        // catalog
        let cat = EvidenceLedgerDetail.catalog(entryCount: 3)
        if case .catalog(let n) = cat { #expect(n == 3) } else { Issue.record("bad catalog") }
    }

    // MARK: - SkipEligibilityGate

    @Test("SkipEligibilityGate has all six required cases")
    func eligibilityGateCases() {
        let cases: [SkipEligibilityGate] = [
            .eligible,
            .blockedByEvidenceQuorum,
            .blockedByPolicy,
            .markOnly,
            .blockedByUserCorrection,
            .cappedByFMSuppression
        ]
        #expect(cases.count == 6)

        // Codable round-trip
        for gate in cases {
            let encoded = try? JSONEncoder().encode(gate)
            #expect(encoded != nil)
            if let data = encoded {
                let decoded = try? JSONDecoder().decode(SkipEligibilityGate.self, from: data)
                #expect(decoded == gate)
            }
        }
    }

    // MARK: - FusionWeightConfig

    @Test("FusionWeightConfig defaults match spec")
    func weightConfigDefaults() {
        let config = FusionWeightConfig()
        #expect(config.fmCap == 0.4)
        #expect(config.classifierCap == 0.3)
        #expect(config.lexicalCap == 0.2)
        #expect(config.acousticCap == 0.2)
        #expect(config.catalogCap == 0.2)
        // playhead-wraj: certainty-tiered gate defaults are inert (flag off).
        #expect(config.certaintyTieredEnabled == false)
        #expect(config.hostReadConfidenceFloor == 0.9)
        #expect(config.postRollGuardSeconds == 90.0)
    }

    // MARK: - BackfillEvidenceFusion — basic ledger accumulation

    @Test("Fusion accumulates classifier entry when mode is .off")
    func classifierEntryInOffMode() {
        let span = makeSpan()
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.75,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .off,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let classifierEntries = ledger.filter { $0.source == .classifier }
        #expect(classifierEntries.count == 1)
        #expect(classifierEntries[0].weight <= 0.3)
    }

    @Test("Fusion includes FM entries when mode is .full")
    func fmEntriesInFullMode() {
        let span = makeSpan()
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.35,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.6,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.count == 1)
    }

    @Test("Fusion excludes FM entries when mode is .off")
    func fmEntriesExcludedInOffMode() {
        let span = makeSpan()
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.35,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.6,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .off,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.isEmpty)
    }

    @Test("Fusion excludes FM entries from decision ledger when mode is .shadow")
    func fmEntriesExcludedInShadowMode() {
        let span = makeSpan()
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.35,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.6,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .shadow,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.isEmpty)
    }

    @Test("FM entries join existing candidates ledger in .rescoreOnly mode")
    func fmEntriesInRescoreOnlyMode() {
        let span = makeSpan()
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.3,
            detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1")
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.5,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .rescoreOnly,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.count == 1)
    }

    @Test("Lexical and acoustic entries are always included for eligible modes")
    func lexicalAndAcousticAlwaysIncluded() {
        let span = makeSpan()
        let lexEntry = EvidenceLedgerEntry(
            source: .lexical,
            weight: 0.18,
            detail: .lexical(matchedCategories: ["url"])
        )
        let acEntry = EvidenceLedgerEntry(
            source: .acoustic,
            weight: 0.15,
            detail: .acoustic(breakStrength: 0.7)
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.5,
            fmEntries: [],
            lexicalEntries: [lexEntry],
            acousticEntries: [acEntry],
            catalogEntries: [],
            mode: .off,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        #expect(ledger.filter { $0.source == .lexical }.count == 1)
        #expect(ledger.filter { $0.source == .acoustic }.count == 1)
    }

    @Test("Catalog overlap uses repeated evidence coverage window, not representative occurrence only")
    func catalogOverlapUsesRepeatedCoverageWindow() async throws {
        let store = try AnalysisStore(path: ":memory:")
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: .default
        )
        let span = makeSpan(startTime: 595.0, endTime: 605.0)
        let repeatedEntry = EvidenceEntry(
            evidenceRef: 0,
            category: .promoCode,
            matchedText: "promo code SAVE20",
            normalizedText: "promo code save20",
            atomOrdinal: 10,
            startTime: 10.0,
            endTime: 12.0,
            count: 2,
            firstTime: 10.0,
            lastTime: 602.0
        )

        let ledger = await service.buildCatalogLedgerEntries(
            span: span,
            entries: [repeatedEntry],
            fusionConfig: defaultConfig()
        )

        #expect(ledger.count == 1)
        if case .catalog(let entryCount) = try #require(ledger.first).detail {
            #expect(entryCount == 1)
        } else {
            Issue.record("Expected catalog ledger detail")
        }
    }

    // MARK: - Weight capping

    @Test("FM entries are capped at fmCap")
    func fmWeightCap() {
        let span = makeSpan()
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.9, // exceeds cap
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )
        let config = FusionWeightConfig()
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: config
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.allSatisfy { $0.weight <= config.fmCap })
    }

    @Test("Classifier entries are capped at classifierCap")
    func classifierWeightCap() {
        let span = makeSpan()
        let config = FusionWeightConfig()
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 1.0, // raw score that produces high weight
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .off,
            config: config
        )
        let ledger = fusion.buildLedger()
        let classifierEntries = ledger.filter { $0.source == .classifier }
        #expect(classifierEntries.allSatisfy { $0.weight <= config.classifierCap })
    }

    // MARK: - DecisionMapper

    @Test("DecisionMapper produces proposalConfidence capped at 1.0")
    func proposalConfidenceCap() {
        let span = makeSpanWithFMConsensus()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.4, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            .init(source: .classifier, weight: 0.3, detail: .classifier(score: 0.9)),
            .init(source: .lexical, weight: 0.2, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.2, detail: .acoustic(breakStrength: 0.8)),
            .init(source: .catalog, weight: 0.2, detail: .catalog(entryCount: 2))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.proposalConfidence <= 1.0)
        #expect(result.proposalConfidence >= 0.0)
    }

    @Test("DecisionMapper skipConfidence is in 0–1 range")
    func skipConfidenceRange() {
        let span = makeSpanWithFMConsensus()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.skipConfidence >= 0.0)
        #expect(result.skipConfidence <= 1.0)
    }

    @Test("DecisionMapper produces eligible gate for span with fmConsensus and sufficient evidence")
    func eligibleGateWithFMConsensus() {
        // fmConsensus + 2 distinct kinds + good quality + valid duration (10–40s)
        let span = makeSpanWithFMConsensus(startTime: 10.0, endTime: 40.0)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.4, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.15, detail: .acoustic(breakStrength: 0.7))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("DecisionMapper blocks by quorum when only fmAcousticCorroborated and no external corroboration")
    func blockedByQuorumFMAcousticOnly() {
        let span = makeSpanWithFMAcoustic(startTime: 10.0, endTime: 40.0)
        // Only FM evidence — no lexical/catalog/acoustic to corroborate
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1"))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
    }

    @Test("DecisionMapper allows fmAcousticCorroborated when external corroboration present")
    func fmAcousticCorroboratedWithExternalEvidence() {
        let span = makeSpanWithFMAcoustic(startTime: 10.0, endTime: 40.0)
        // External corroboration from lexical
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1")),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"]))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("DecisionMapper allows fmAcousticCorroborated when classifier is the sole corroboration")
    func fmAcousticCorroboratedWithClassifierCorroboration() {
        let span = makeSpanWithFMAcoustic(startTime: 10.0, endTime: 40.0)
        // Classifier is a non-FM, independent signal that satisfies the corroboration requirement
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1")),
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.62))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("DecisionMapper blocks fmAcousticCorroborated when classifier weight is zero")
    func fmAcousticCorroboratedZeroWeightClassifierBlocks() {
        // Regression: a classifier entry with weight == 0 must NOT count as
        // external corroboration. Cycle 2 H2 fix: only weight > 0 classifier
        // entries satisfy quorum.
        let span = makeSpanWithFMAcoustic(startTime: 10.0, endTime: 40.0)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1")),
            .init(source: .classifier, weight: 0.0, detail: .classifier(score: 0.62))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
    }

    @Test("DecisionMapper blocks by quorum for fmConsensus with too-short span")
    func blockedByQuorumShortSpan() {
        // Span < 5s
        let span = makeSpanWithFMConsensus(startTime: 10.0, endTime: 13.0)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.4, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.15, detail: .acoustic(breakStrength: 0.7))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
    }

    @Test("DecisionMapper blocks by quorum for fmConsensus with too-long span")
    func blockedByQuorumLongSpan() {
        // Span > 180s
        let span = makeSpanWithFMConsensus(startTime: 0.0, endTime: 181.0)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.4, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.15, detail: .acoustic(breakStrength: 0.7))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
    }

    @Test("DecisionMapper blocks by quorum for fmConsensus with degraded transcript")
    func blockedByQuorumDegradedTranscript() {
        let span = makeSpanWithFMConsensus(startTime: 10.0, endTime: 40.0)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.4, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.15, detail: .acoustic(breakStrength: 0.7))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .degraded)
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
    }

    @Test("Gate blocks action without clamping skip confidence")
    func gateDoesNotClampSkipConfidence() {
        // fmAcousticCorroborated with FM-only entry — blocks by quorum (no external corroboration)
        // but the score must remain honest (> 0) regardless of the gate value.
        let span = makeSpanWithFMAcoustic(startTime: 10.0, endTime: 40.0)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.4, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1"))
            // No external-corroboration entry — quorum fails for fmAcousticCorroborated
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        // Gate blocks because no external corroboration (classifier/lexical/acoustic/catalog) is present.
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
        // Score is honest regardless of gate: weight sum of 0.4 (capped at fmCap 0.4).
        #expect(result.skipConfidence > 0.0, "Score should be honest even when gate blocks")
    }

    @Test("Non-FM span has no quorum check applied (gate is eligible when score is sufficient)")
    func nonFMSpanNoQuorumCheck() {
        // Span with no FM provenance — quorum not applicable
        let span = makeSpan(anchorProvenance: [
            .evidenceCatalog(entry: EvidenceEntry(
                evidenceRef: 0,
                category: .url,
                matchedText: "example.com",
                normalizedText: "example.com",
                atomOrdinal: 100,
                startTime: 10.0,
                endTime: 11.0
            ))
        ])
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"]))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        // No FM provenance means quorum check is not applicable — should not block by quorum
        #expect(result.eligibilityGate != .blockedByEvidenceQuorum)
    }

    @Test("Classifier-seeded span with co-seeded lexical produces multi-kind ledger and eligible gate")
    func classifierSeededSpanFusionProducesMultiKindLedger() {
        // Integration-shape: exercises the path unblocked by the classifier
        // seeding fix. In production, classifier inputs are derived from
        // lexical candidates (`classifyCandidates` takes `[LexicalCandidate]`),
        // so a classifier-seeded region always co-occurs with a lexical
        // proposal that merges into the same region — the fusion ledger
        // therefore naturally has both a `.classifier` entry (from the
        // always-on path in `buildLedger`) AND a co-seeded lexical entry.
        // This test pins that downstream shape and asserts the gate
        // returns `.eligible` once the span reaches fusion.
        //
        // Why this matters: prior to the fix, a classifier-seeded region
        // never seeded a ProposedRegion → AtomEvidenceProjector never
        // anchored → MinimalContiguousSpanDecoder emitted no DecodedSpan
        // → this fusion call never happened at all.
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r-classifier", score: 0.8154)
        ])
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.8154,
            fmEntries: [],
            lexicalEntries: [
                .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["cta"]))
            ],
            acousticEntries: [],
            catalogEntries: [],
            mode: .off,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let distinctKinds = Set(ledger.map(\.source))
        // Classifier (always-on) + lexical → at least 2 kinds.
        #expect(distinctKinds.count >= 2)
        #expect(distinctKinds.contains(.classifier))
        #expect(distinctKinds.contains(.lexical))
        // The fusion path must adjudicate the span — no FM provenance,
        // so DecisionMapper's metadataCorroborationGate runs. With an
        // in-audio lexical entry present (and metadata entries absent),
        // that gate returns `.eligible`.
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: defaultConfig(),
            transcriptQuality: .good
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
        #expect(result.skipConfidence > 0.0)
    }

    // MARK: - DecisionMapper: correctionFactor suppression (Phase 7.2)

    @Test("correctionFactor < 1.0 gates span as blockedByUserCorrection when effective confidence < 0.40")
    func correctionFactorBlocksLowEffectiveConfidence() {
        // rawSkipConfidence = 0.50, correctionFactor = 0.3 → effective = 0.15 < 0.40 → blocked
        let span = makeSpan()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 0.3
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByUserCorrection,
                "correctionFactor=0.3 with rawSkipConfidence ~0.45 should block")
        #expect(result.skipConfidence < 0.40)
    }

    @Test("Negative correctionFactor clamps to zero and gates as blockedByUserCorrection")
    func negativeCorrectionFactorClampsToZero() {
        // correctionFactor = -0.5 is theoretically impossible from the store,
        // but map() defensively applies max(0.0, correctionFactor).
        // With rawSkipConfidence ~0.45 and effective factor 0.0 → effective = 0.0 → blocked.
        let span = makeSpan()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: -0.5
        )
        let result = mapper.map()
        #expect(result.skipConfidence < 1e-9,
                "Negative correctionFactor clamped to 0 should yield ~0.0 effective confidence")
        #expect(result.eligibilityGate == .blockedByUserCorrection,
                "Effective confidence of 0.0 must gate as blockedByUserCorrection")
    }

    @Test("correctionFactor = 1.0 does not alter gate (no suppression)")
    func correctionFactorOneDoesNotAlterGate() {
        // Same ledger as above but factor = 1.0 → no suppression
        let span = makeSpan()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 1.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate != .blockedByUserCorrection,
                "correctionFactor=1.0 must not trigger correction blocking")
    }

    @Test("correctionFactor that leaves effectiveConfidence >= 0.40 does not block")
    func correctionFactorHighEnoughDoesNotBlock() {
        // Arrange ledger to produce rawSkipConfidence ~ 0.9 (many sources)
        // correctionFactor = 0.6 → effective = 0.54 >= 0.40 → no block
        let span = makeSpan()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 1.0)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.9)),
            .init(source: .catalog, weight: 0.20, detail: .catalog(entryCount: 3)),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: defaultConfig(),
            transcriptQuality: .good,
            correctionFactor: 0.6
        )
        let result = mapper.map()
        #expect(result.eligibilityGate != .blockedByUserCorrection,
                "effectiveConfidence >= 0.40 must not be blocked by correction factor")
        #expect(result.skipConfidence >= 0.40)
    }

    // MARK: - Certainty-tiered auto-skip gate (playhead-wraj)

    /// Non-FM, non-rediff provenance so `computeGate()` routes through
    /// `metadataCorroborationGate()` and (with no `.metadata` ledger entry)
    /// returns `.eligible`. `.classifierSeed` is deliberately NOT `.rediffSlot`.
    private func makeHostReadSpan() -> DecodedSpan {
        makeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7)])
    }

    /// Same shape as `makeHostReadSpan()` but WIDTH-owned by the rediff oracle.
    private func makeRediffSpan() -> DecodedSpan {
        makeSpan(anchorProvenance: [.rediffSlot])
    }

    /// Ledger summing to skipConfidence ≈ 0.70 under the default v0 (identity)
    /// calibration, unit correction factor, and identity duration prior.
    private func belowFloorLedger() -> [EvidenceLedgerEntry] {
        [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.7)),
        ]
    }

    /// Ledger producing skipConfidence == 0.90 bit-exactly: a SINGLE weight of
    /// `0.9` compiles to the same IEEE-754 double as the config's `0.9` floor
    /// literal, so `skipConfidence < floor` is unambiguously `false` at the
    /// boundary. (A multi-entry sum like `0.3+0.2+0.2+0.2` rounds to
    /// `0.8999999999999999`, which is genuinely — and correctly — below floor.)
    private func atFloorLedger() -> [EvidenceLedgerEntry] {
        [
            .init(source: .classifier, weight: 0.90, detail: .classifier(score: 0.9)),
        ]
    }

    @Test("Flag OFF: non-rediff below-floor eligible span stays eligible with scores untouched")
    func certaintyTieredFlagOffIsByteIdentical() {
        let span = makeHostReadSpan()
        let mapper = DecisionMapper(
            span: span,
            ledger: belowFloorLedger(),
            config: defaultConfig(), // certaintyTieredEnabled defaults to false
            transcriptQuality: .good
        )
        let result = mapper.map()
        // Today's behavior: a non-FM span with in-audio corroboration is eligible.
        #expect(result.eligibilityGate == .eligible)
        // Scores are the honest weight sum, NOT touched by the (disabled) gate.
        #expect(abs(result.proposalConfidence - 0.70) < 0.001)
        #expect(abs(result.skipConfidence - 0.70) < 0.001)
    }

    @Test("Flag ON: non-rediff eligible span below floor is downgraded to markOnly")
    func certaintyTieredFlagOnDowngradesHostReadBelowFloor() {
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: belowFloorLedger(),
            config: config,
            transcriptQuality: .good
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .markOnly)
        // Invariant: the downgrade must NOT touch either score.
        #expect(abs(result.proposalConfidence - 0.70) < 0.001)
        #expect(abs(result.skipConfidence - 0.70) < 0.001)
    }

    @Test("Flag ON: rediff-anchored eligible span below floor bypasses the floor and stays eligible")
    func certaintyTieredFlagOnRediffBypassesFloor() {
        let span = makeRediffSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: belowFloorLedger(),
            config: config,
            transcriptQuality: .good
        )
        let result = mapper.map()
        // Identical inputs to the host-read downgrade case EXCEPT `.rediffSlot`.
        #expect(result.eligibilityGate == .eligible)
        #expect(abs(result.skipConfidence - 0.70) < 0.001)
    }

    @Test("Flag ON: non-rediff eligible span at/above floor stays eligible")
    func certaintyTieredFlagOnAtFloorStaysEligible() {
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good
        )
        let result = mapper.map()
        // skipConfidence == 0.9 == floor → `0.9 < 0.9` is false → not downgraded.
        #expect(abs(result.skipConfidence - 0.90) < 0.001)
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("Flag ON: a blockedByEvidenceQuorum span is never promoted or otherwise touched")
    func certaintyTieredFlagOnLeavesBlockedGateUntouched() {
        // FM-acoustic provenance with only an FM entry → blockedByEvidenceQuorum.
        // It is also non-rediff and below-floor, so if the downgrade fired on a
        // non-eligible gate it would (wrongly) rewrite it. It must not.
        let span = makeSpanWithFMAcoustic(startTime: 10.0, endTime: 40.0)
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1"))
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: config,
            transcriptQuality: .good
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
    }

    @Test("Flag ON: hostReadConfidenceFloor is read, not hardcoded (custom floor 0.6 keeps 0.7 eligible)")
    func certaintyTieredCustomFloorIsHonored() {
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true, hostReadConfidenceFloor: 0.6)
        let mapper = DecisionMapper(
            span: span,
            ledger: belowFloorLedger(), // skipConfidence ≈ 0.70
            config: config,
            transcriptQuality: .good
        )
        let result = mapper.map()
        // 0.70 >= 0.60 → clears the custom floor → stays eligible.
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("Flag ON: non-rediff eligible span strictly above floor stays eligible")
    func certaintyTieredFlagOnAboveFloorStaysEligible() {
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: [.init(source: .classifier, weight: 0.95, detail: .classifier(score: 0.95))],
            config: config,
            transcriptQuality: .good
        )
        let result = mapper.map()
        // skipConfidence == 0.95 > 0.9 floor → not downgraded.
        #expect(abs(result.skipConfidence - 0.95) < 0.001)
        #expect(result.eligibilityGate == .eligible)
    }

    // MARK: - Post-roll guard (playhead-wraj, Dan 2026-07-19)
    //
    // All spans below use the default `makeSpan` end time of 40.0s. Episode
    // durations are chosen relative to that end and the 90s default window:
    //   100.0 → gap  60s  (within  → guard fires)
    //   130.0 → gap  90s  (boundary → "within" is inclusive, guard fires;
    //                      130.0 - 40.0 == 90.0 is exact in IEEE-754)
    //   200.0 → gap 160s  (outside → guard inert)

    @Test("Post-roll guard: eligible span ending within 90s of a known duration is demoted to markOnly, scores untouched")
    func postRollGuardDemotesNearEndSpan() {
        // atFloorLedger → skipConfidence 0.9 == floor, so the host-read floor
        // does NOT demote — any markOnly here is the post-roll guard's doing.
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: 100.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .markOnly)
        // Invariant: the demotion must NOT touch either score.
        #expect(abs(result.proposalConfidence - 0.90) < 0.001)
        #expect(abs(result.skipConfidence - 0.90) < 0.001)
    }

    @Test("Post-roll guard: fires even for a rediff-anchored span below the floor (no rediff exemption)")
    func postRollGuardOverridesRediffBypass() {
        // Identical inputs to certaintyTieredFlagOnRediffBypassesFloor — which
        // proves .eligible without a duration — EXCEPT episodeDuration 100.0.
        let span = makeRediffSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: belowFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: 100.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .markOnly)
        #expect(abs(result.skipConfidence - 0.70) < 0.001)
    }

    @Test("Post-roll guard: fires even for a rediff-anchored span at/above the floor")
    func postRollGuardOverridesRediffAtFloor() {
        let span = makeRediffSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(), // skipConfidence 0.9 — clears the floor too
            config: config,
            transcriptQuality: .good,
            episodeDuration: 100.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .markOnly)
    }

    @Test("Post-roll guard: nil episode duration leaves the guard inert (never guess the episode end)")
    func postRollGuardInertWhenDurationUnknown() {
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: nil
        )
        let result = mapper.map()
        // Would fire under any plausible finite duration near 40s — but the
        // duration is UNKNOWN, so the span must stay eligible.
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("Post-roll guard: a non-positive duration is treated as unknown (defense-in-depth belt)", arguments: [0.0, -1.0])
    func postRollGuardInertForNonPositiveDuration(bogusDuration: Double) {
        // AdDetectionService normalizes its `0 == unknown` sentinel to nil,
        // but the mapper must ALSO refuse to fire on a raw non-positive value
        // from any future caller that skips that normalization — without this
        // belt, `0 - 40 = -40 <= 90` would demote on an unknown duration.
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: bogusDuration
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("Post-roll guard: a non-finite duration stays inert (NaN and ±inf never demote)", arguments: [Double.nan, .infinity, -.infinity])
    func postRollGuardInertForNonFiniteDuration(garbageDuration: Double) {
        // R2 review: pins the guard's documented garbage-input contract.
        // NaN and -inf fail the `> 0` check; +inf leaves
        // `episodeDuration - endTime <= window` false (inf <= 90). All three
        // must leave the span eligible — the guard fails safe-inert.
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: garbageDuration
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("Post-roll guard: a qualified promotion track survives the demotion (gate moves, track and scores stay honest)")
    func postRollGuardPreservesPromotionTrack() {
        // R2 review: the guard is a gate-only demotion — `promotionTrack` is
        // computed AFTER the gate from provenance + ledger and must come
        // through untouched, because downstream re-stamp paths (finalizer
        // wire-in, fragility diagnostics) read it. classifierSeed provenance
        // + classifier score >= 0.70 + a .breakAlignment entry qualify the
        // span for .classifierSeedQualified; weights sum to 0.95 (>= 0.9
        // floor) so any markOnly here is the post-roll guard's doing.
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.80, detail: .classifier(score: 0.9)),
            .init(source: .breakAlignment, weight: 0.15, detail: .breakAlignment(breakStrength: 0.7)),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: config,
            transcriptQuality: .good,
            episodeDuration: 100.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .markOnly)
        #expect(result.promotionTrack == .classifierSeedQualified)
        #expect(abs(result.proposalConfidence - 0.95) < 0.001)
        #expect(abs(result.skipConfidence - 0.95) < 0.001)
    }

    @Test("Post-roll guard: span ending outside the window is unaffected")
    func postRollGuardInertOutsideWindow() {
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: 200.0 // gap 160s > 90s
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("Post-roll guard: exactly postRollGuardSeconds from the end counts as within (inclusive boundary)")
    func postRollGuardBoundaryIsInclusive() {
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: 130.0 // 130.0 - 40.0 == 90.0 bit-exactly
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .markOnly)
    }

    @Test("Post-roll guard: span ending past the claimed episode end still counts as near-end")
    func postRollGuardFiresWhenSpanEndExceedsDuration() {
        // endTime 40.0 > episodeDuration 35.0 (e.g. clock skew between the
        // transcript timeline and the asset's reported duration): the gap is
        // negative, unambiguously "near the end" — demote, don't skip.
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: 35.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .markOnly)
    }

    @Test("Post-roll guard: custom postRollGuardSeconds is read, not hardcoded (narrow 30s window keeps a 60s-gap span eligible)")
    func postRollGuardCustomWindowIsHonored() {
        let span = makeHostReadSpan()
        let config = FusionWeightConfig(
            certaintyTieredEnabled: true,
            postRollGuardSeconds: 30.0
        )
        let mapper = DecisionMapper(
            span: span,
            ledger: atFloorLedger(),
            config: config,
            transcriptQuality: .good,
            episodeDuration: 100.0 // gap 60s > custom 30s; the DEFAULT 90s would fire
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
    }

    @Test("Post-roll guard: flag OFF leaves a near-end span eligible with scores untouched (byte-identical path)")
    func postRollGuardFlagOffIsByteIdentical() {
        let span = makeHostReadSpan()
        let mapper = DecisionMapper(
            span: span,
            ledger: belowFloorLedger(), // below floor AND near end: both demotions would fire if armed
            config: defaultConfig(),    // certaintyTieredEnabled defaults to false
            transcriptQuality: .good,
            episodeDuration: 100.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .eligible)
        #expect(abs(result.proposalConfidence - 0.70) < 0.001)
        #expect(abs(result.skipConfidence - 0.70) < 0.001)
    }

    @Test("Post-roll guard: a blockedByEvidenceQuorum span near the end is never promoted or otherwise touched")
    func postRollGuardLeavesBlockedGateUntouched() {
        // Same shape as certaintyTieredFlagOnLeavesBlockedGateUntouched, plus a
        // near-end duration: the guard must only ever inspect an `.eligible`
        // gate, so the blocked gate must survive unchanged.
        let span = makeSpanWithFMAcoustic(startTime: 10.0, endTime: 40.0)
        let config = FusionWeightConfig(certaintyTieredEnabled: true)
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1"))
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: config,
            transcriptQuality: .good,
            episodeDuration: 100.0
        )
        let result = mapper.map()
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
    }

    // MARK: - FM Positive-Only Rule

    @Test("FM noAds disposition does not produce ledger entries in BackfillEvidenceFusion")
    func fmNoAdsDoesNotContributeToLedger() {
        // This test verifies the caller contract: noAds entries should not be in fmEntries
        // BackfillEvidenceFusion itself enforces this by filtering
        let span = makeSpan()
        let noAdsEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.1,
            detail: .fm(disposition: .noAds, band: .weak, cohortPromptLabel: "v1")
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.3,
            fmEntries: [noAdsEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.isEmpty, "noAds FM entries must not appear in decision ledger")
    }

    @Test("FM abstain disposition does not produce ledger entries")
    func fmAbstainDoesNotContributeToLedger() {
        let span = makeSpan()
        let abstainEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.1,
            detail: .fm(disposition: .abstain, band: .weak, cohortPromptLabel: "v1")
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.3,
            fmEntries: [abstainEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.isEmpty, "abstain FM entries must not appear in decision ledger")
    }

    @Test("FM uncertain disposition does not produce ledger entries")
    func fmUncertainDoesNotContributeToLedger() {
        let span = makeSpan()
        let uncertainEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.2,
            detail: .fm(disposition: .uncertain, band: .weak, cohortPromptLabel: "v1")
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.3,
            fmEntries: [uncertainEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.isEmpty, "uncertain FM entries must not appear in decision ledger")
    }

    // MARK: - DecisionResult

    @Test("DecisionResult is Sendable and carries all three outputs")
    func decisionResultFields() {
        let span = makeSpanWithFMConsensus()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        // Can access all fields
        _ = result.proposalConfidence
        _ = result.skipConfidence
        _ = result.eligibilityGate
    }

    // MARK: - proposalConfidence accumulation

    @Test("proposalConfidence is sum of all weights capped at 1.0")
    func proposalConfidenceIsSum() {
        let span = makeSpanWithFMConsensus()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.3, detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1")),
            .init(source: .classifier, weight: 0.2, detail: .classifier(score: 0.6))
        ]
        let mapper = DecisionMapper(span: span, ledger: entries, config: defaultConfig(), transcriptQuality: .good)
        let result = mapper.map()
        let expectedRaw = min(1.0, 0.3 + 0.2)
        #expect(abs(result.proposalConfidence - expectedRaw) < 0.001)
    }

    // MARK: - FusionWeightConfig custom values

    @Test("FusionWeightConfig custom caps are respected")
    func customWeightCaps() {
        let config = FusionWeightConfig(fmCap: 0.5, classifierCap: 0.4, lexicalCap: 0.3, acousticCap: 0.3, catalogCap: 0.25)
        #expect(config.fmCap == 0.5)
        #expect(config.classifierCap == 0.4)
        #expect(config.lexicalCap == 0.3)
        #expect(config.acousticCap == 0.3)
        #expect(config.catalogCap == 0.25)
    }

    // MARK: - ClassificationTrustMatrix (ef2.4.5)

    @Test("ClassificationTrustMatrix: paid|thirdParty returns 1.0")
    func trustPaidThirdParty() {
        let trust = ClassificationTrustMatrix.trust(commercialIntent: .paid, ownership: .thirdParty)
        #expect(trust == 1.0)
    }

    @Test("ClassificationTrustMatrix: paid|show returns 1.0 (NOT discounted — skip is policy)")
    func trustPaidShow() {
        let trust = ClassificationTrustMatrix.trust(commercialIntent: .paid, ownership: .show)
        #expect(trust == 1.0, "paid|show must NOT be discounted — skip behavior is policy, not classification")
    }

    @Test("ClassificationTrustMatrix: owned|show returns 0.7")
    func trustOwnedShow() {
        let trust = ClassificationTrustMatrix.trust(commercialIntent: .owned, ownership: .show)
        #expect(trust == 0.7)
    }

    @Test("ClassificationTrustMatrix: affiliate|thirdParty returns 0.9")
    func trustAffiliateThirdParty() {
        let trust = ClassificationTrustMatrix.trust(commercialIntent: .affiliate, ownership: .thirdParty)
        #expect(trust == 0.9)
    }

    @Test("ClassificationTrustMatrix: organic returns 0.15 for any ownership")
    func trustOrganicAny() {
        let allOwnerships: [Ownership] = [.thirdParty, .show, .network, .guest, .unknown]
        for ownership in allOwnerships {
            let trust = ClassificationTrustMatrix.trust(commercialIntent: .organic, ownership: ownership)
            #expect(trust == 0.15, "organic|\(ownership) should return 0.15")
        }
    }

    @Test("ClassificationTrustMatrix: unknown returns 0.6 for any ownership")
    func trustUnknownAny() {
        let allOwnerships: [Ownership] = [.thirdParty, .show, .network, .guest, .unknown]
        for ownership in allOwnerships {
            let trust = ClassificationTrustMatrix.trust(commercialIntent: .unknown, ownership: ownership)
            #expect(trust == 0.6, "unknown|\(ownership) should return 0.6")
        }
    }

    @Test("ClassificationTrustMatrix: unmapped combinations fall back to 0.6")
    func trustUnmappedFallback() {
        // owned|thirdParty is not explicitly mapped — should fall back to 0.6
        let trust = ClassificationTrustMatrix.trust(commercialIntent: .owned, ownership: .thirdParty)
        #expect(trust == ClassificationTrustMatrix.fallback)
        #expect(trust == 0.6)
    }

    @Test("ClassificationTrustMatrix: unknown×unknown returns fallback")
    func trustUnknownUnknown() {
        let trust = ClassificationTrustMatrix.trust(commercialIntent: .unknown, ownership: .unknown)
        #expect(trust == 0.6)
    }

    @Test("ClassificationTrustMatrix: all CommercialIntent cases covered")
    func trustAllIntentsCovered() {
        // Verify no crash for all combinations
        let allOwnerships: [Ownership] = [.thirdParty, .show, .network, .guest, .unknown]
        for intent in CommercialIntent.allCases {
            for ownership in allOwnerships {
                let trust = ClassificationTrustMatrix.trust(commercialIntent: intent, ownership: ownership)
                #expect(trust > 0.0 && trust <= 1.0, "\(intent)|\(ownership) trust should be in (0, 1]")
            }
        }
    }

    // MARK: - classificationTrust modulation in buildLedger (ef2.4.5)

    @Test("FM entry with classificationTrust < 1.0 has weight modulated in buildLedger")
    func fmTrustModulatesWeight() {
        let span = makeSpan()
        // Weight 0.3 with trust 0.7 → modulated weight = 0.21
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.3,
            detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1"),
            classificationTrust: 0.7
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.count == 1)
        #expect(abs(fmEntries[0].weight - 0.21) < 0.001, "weight should be 0.3 * 0.7 = 0.21")
        #expect(fmEntries[0].classificationTrust == 0.7)
    }

    @Test("FM entry with classificationTrust 1.0 has weight unchanged in buildLedger")
    func fmTrustOneDoesNotModulate() {
        let span = makeSpan()
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.3,
            detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1"),
            classificationTrust: 1.0
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.count == 1)
        #expect(abs(fmEntries[0].weight - 0.3) < 0.001, "trust=1.0 should not change weight")
    }

    @Test("FM entry with organic trust 0.15 dramatically reduces weight")
    func fmOrganicTrustReducesWeight() {
        let span = makeSpan()
        // Weight 0.4 (fmCap) with trust 0.15 → modulated = 0.06
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.4,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1"),
            classificationTrust: 0.15
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.count == 1)
        #expect(abs(fmEntries[0].weight - 0.06) < 0.001, "organic trust should reduce 0.4 to 0.06")
    }

    @Test("FM trust modulation happens before fmCap capping")
    func fmTrustModulationBeforeCapping() {
        let span = makeSpan()
        // Weight 0.5 (above fmCap 0.4) with trust 0.7 → modulated = 0.35, then capped at 0.4 → 0.35
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.5,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1"),
            classificationTrust: 0.7
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let fmEntries = ledger.filter { $0.source == .fm }
        #expect(fmEntries.count == 1)
        // 0.5 * 0.7 = 0.35, which is below fmCap 0.4, so no capping
        #expect(abs(fmEntries[0].weight - 0.35) < 0.001)
    }

    @Test("Default classificationTrust of 1.0 preserves backward compatibility")
    func defaultTrustBackwardCompatible() {
        let entry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.3,
            detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1")
        )
        #expect(entry.classificationTrust == 1.0, "Default trust must be 1.0 for backward compat")
    }

    @Test("Non-FM entries are not affected by classificationTrust in buildLedger")
    func nonFMEntriesUnaffectedByTrust() {
        let span = makeSpan()
        // Even if someone set trust on a non-FM entry, buildLedger should not modulate it
        let lexEntry = EvidenceLedgerEntry(
            source: .lexical,
            weight: 0.18,
            detail: .lexical(matchedCategories: ["url"]),
            classificationTrust: 0.5  // should be ignored for non-FM
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [lexEntry],
            acousticEntries: [],
            catalogEntries: [],
            mode: .off,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let lexEntries = ledger.filter { $0.source == .lexical }
        #expect(lexEntries.count == 1)
        #expect(abs(lexEntries[0].weight - 0.18) < 0.001, "Lexical weight should not be modulated by trust")
    }

    // MARK: - playhead-fqc8 — PromotionTrack quorum (classifier-seeded)

    /// Helper: build a fusion ledger from a span + entries, then run DecisionMapper.
    /// `classifierScore` controls the always-on classifier ledger entry that
    /// `buildLedger()` synthesizes; explicit `extraClassifierEntries` lets a
    /// test inject additional `.classifier(score:)` records that the
    /// promotion gate inspects.
    private func mapDecision(
        span: DecodedSpan,
        classifierScore: Double,
        acousticEntries: [EvidenceLedgerEntry] = [],
        lexicalEntries: [EvidenceLedgerEntry] = [],
        catalogEntries: [EvidenceLedgerEntry] = [],
        fmEntries: [EvidenceLedgerEntry] = [],
        metadataEntries: [EvidenceLedgerEntry] = [],
        breakAlignmentEntries: [EvidenceLedgerEntry] = [],
        transcriptQuality: TranscriptQuality = .good
    ) -> DecisionResult {
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: classifierScore,
            fmEntries: fmEntries,
            lexicalEntries: lexicalEntries,
            acousticEntries: acousticEntries,
            catalogEntries: catalogEntries,
            metadataEntries: metadataEntries,
            breakAlignmentEntries: breakAlignmentEntries,
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: defaultConfig(),
            transcriptQuality: transcriptQuality
        )
        return mapper.map()
    }

    @Test("Classifier-only span with no break alignment sits on standard promotion track")
    func classifierOnlyStandardTrack() {
        // Classifier-seeded span, classifierScore=0.90, NO breakAlignment
        // entry → quorum NOT met → standard track. Score is bounded by
        // the 0.30 classifier ceiling.
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "x", score: 0.90)
        ])
        let result = mapDecision(span: span, classifierScore: 0.90)
        #expect(result.promotionTrack == .standard,
                "No breakAlignment entry → must remain on standard track")
        #expect(result.skipConfidence < 0.80,
                "Classifier-only span ceiling is 0.30; standard 0.80 gate is unreachable")
    }

    @Test("Classifier-seeded span with break-alignment entry promotes to qualified track")
    func classifierSeededQualifiedPromotion() {
        // classifierScore=0.82 (>= 0.70), classifierSeed provenance,
        // .breakAlignment entry → quorum met → qualified track.
        // Score must stay honest (no clamp to either threshold).
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r1", score: 0.82)
        ])
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.82,
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.promotionTrack == .classifierSeedQualified,
                "classifierSeed + classifier>=0.70 + breakAlignment must promote to qualified track")
        #expect(result.skipConfidence >= 0.40 && result.skipConfidence < 0.80,
                "Score stays honest: classifier (0.246) + breakAlignment (0.18) ≈ 0.43, far below 0.80 standard gate")
    }

    @Test("Low classifier score does not qualify for the loose track")
    func lowClassifierScoreStaysStandard() {
        // classifierScore=0.65 fails the >= 0.70 component of the quorum.
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r1", score: 0.65)
        ])
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.65,
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.promotionTrack == .standard,
                "classifier score 0.65 < 0.70 → must NOT qualify even with breakAlignment")
    }

    @Test("Lexical-only span with break-alignment entry stays on standard track")
    func lexicalOnlyWithBreakAlignmentStaysStandard() {
        // anchorProvenance is empty (no classifierSeed) → never qualified.
        let span = makeSpan(anchorProvenance: [])
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.82,
            lexicalEntries: [
                .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"]))
            ],
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.promotionTrack == .standard,
                "No classifierSeed provenance → must remain on standard track")
    }

    @Test("Break alignment without classifierSeed provenance stays on standard track")
    func fmConsensusWithBreakAlignmentStaysStandard() {
        // fmConsensus provenance — classifierSeed gate is mandatory.
        let span = makeSpan(anchorProvenance: [
            .fmConsensus(regionId: "r1", consensusStrength: 0.9)
        ])
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.82,
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.promotionTrack == .standard,
                "fmConsensus provenance must NOT qualify — classifierSeed gate is mandatory")
    }

    @Test("Plain acoustic entry without breakAlignment kind does not qualify")
    func plainAcousticEntryDoesNotQualify() {
        // classifierSeed + classifier >= 0.70 + plain `.acoustic` entry
        // (the standard RMS-drop path, no `.breakAlignment` corroborator)
        // → quorum NOT met.
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r1", score: 0.82)
        ])
        let plainAcoustic = EvidenceLedgerEntry(
            source: .acoustic,
            weight: 0.18,
            detail: .acoustic(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.82,
            acousticEntries: [plainAcoustic]
        )
        #expect(result.promotionTrack == .standard,
                "Plain `.acoustic` entry is not a breakAlignment corroborator")
    }

    /// playhead-fqc8 cycle-1 review M-2: the qualified track is for
    /// classifier-only candidates. A span carrying both a classifierSeed
    /// AND an FM-class anchor (e.g. `.fmConsensus`) must NOT qualify even
    /// if the rest of the quorum is met — it has independent FM evidence
    /// and should clear the standard 0.80 gate via the standard track.
    @Test("classifierSeed + fmConsensus + breakAlignment stays on standard track (M-2 fix)")
    func classifierSeedPlusFMConsensusStaysStandard() {
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r1", score: 0.85),
            .fmConsensus(regionId: "r1", consensusStrength: 0.9)
        ])
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.82,
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.promotionTrack == .standard,
                "FM-class anchor coexisting with classifierSeed must keep the span on the standard track")
    }

    @Test("PromotionTrack default for legacy 3-arg DecisionResult is .standard")
    func promotionTrackDefaultIsStandard() {
        let decision = DecisionResult(
            proposalConfidence: 0.5,
            skipConfidence: 0.5,
            eligibilityGate: .eligible
        )
        #expect(decision.promotionTrack == .standard,
                "Back-compat: 3-arg init must default promotionTrack to .standard")
    }

    /// playhead-fqc8 cycle-1 review HIGH-1: the acoustic family budget
    /// (`acousticCap`) and the breakAlignment family budget
    /// (`breakAlignmentCap`) must be ENFORCED INDEPENDENTLY. The previous
    /// design emitted `.acoustic` + `subSource: .breakAlignment` and capped
    /// each entry against `acousticCap`, letting the acoustic family
    /// contribute up to 2 × `acousticCap` = 0.40. Pin the fix here: an
    /// over-cap RMS-drop AND an over-cap breakAlignment must each be
    /// capped against their OWN budget independently, AND the
    /// proposalConfidence must reflect both budgets summed honestly.
    @Test("Acoustic family budget is independent from breakAlignment budget (HIGH-1)")
    func acousticAndBreakAlignmentBudgetsAreIndependent() {
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r1", score: 0.82)
        ])
        // BOTH entries arrive over-cap; the fusion code must cap each
        // against its own per-source budget independently.
        let rmsDrop = EvidenceLedgerEntry(
            source: .acoustic,
            weight: 0.50,  // way above acousticCap
            detail: .acoustic(breakStrength: 1.0)
        )
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.50,  // way above breakAlignmentCap
            detail: .breakAlignment(breakStrength: 1.0)
        )
        let cfg = defaultConfig()
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [rmsDrop],
            catalogEntries: [],
            breakAlignmentEntries: [breakAlignment],
            mode: .full,
            config: cfg
        )
        let ledger = fusion.buildLedger()

        // Find the post-cap acoustic and breakAlignment entries.
        let acousticEntries = ledger.filter { $0.source == .acoustic }
        let breakAlignmentEntries = ledger.filter { $0.source == .breakAlignment }
        try? #require(acousticEntries.count == 1)
        try? #require(breakAlignmentEntries.count == 1)

        // Each family is independently capped against its own budget.
        #expect(acousticEntries[0].weight <= cfg.acousticCap + 1e-9,
                "Acoustic family must be capped at acousticCap independently")
        #expect(breakAlignmentEntries[0].weight <= cfg.breakAlignmentCap + 1e-9,
                "BreakAlignment family must be capped at breakAlignmentCap independently")

        // The mapper sums each family's contribution honestly: if both
        // caps fire at 0.20 each, the ledger contributes ~0.40 from the
        // two acoustic-modality kinds combined — the documented design.
        let mapper = DecisionMapper(span: span, ledger: ledger, config: cfg)
        let result = mapper.map()
        #expect(result.proposalConfidence >= cfg.acousticCap + cfg.breakAlignmentCap - 1e-9,
                "proposalConfidence must reflect BOTH budgets summed honestly (≥ 0.40)")
    }

    // MARK: - playhead-fqc8 cycle-2 review — gate boundary tests

    /// playhead-fqc8 cycle-2 review HIGH-1: a span anchored by
    /// `.fmAcousticCorroborated` whose ONLY non-FM evidence is a
    /// `.breakAlignment` ledger entry must clear `quorumGateForFMAcoustic`.
    /// Pre-fqc8 the alignment corroborator was emitted as
    /// `source: .acoustic + subSource: .breakAlignment` and satisfied the gate
    /// via `.acoustic`. The cycle-1 family-budget fix promoted alignment to
    /// its own top-level kind; without adding `.breakAlignment` to the
    /// corroboration set, this case silently regresses to
    /// `.blockedByEvidenceQuorum`. Pin the fix here so the regression
    /// boundary is guarded.
    @Test("fmAcousticCorroborated + breakAlignment-only corroboration is eligible (HIGH-1)")
    func fmAcousticCorroboratedWithBreakAlignmentIsEligible() {
        let span = makeSpan(anchorProvenance: [
            .fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7)
        ])
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.0,  // no classifier corroboration
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.eligibilityGate == .eligible,
                "breakAlignment must satisfy the FM-acoustic corroboration set")
    }

    /// playhead-fqc8 cycle-2 review HIGH-2: a span with only metadata + a
    /// `.breakAlignment` ledger entry must resolve to `.eligible` — the
    /// boundary-alignment kind is real in-audio signal corroborating the
    /// metadata cue. Same root cause as HIGH-1: the cycle-1 family-budget
    /// fix promoted alignment to its own kind, and any gate that
    /// previously accepted the alignment corroborator under `.acoustic`
    /// must explicitly accept `.breakAlignment` now.
    @Test("metadata + breakAlignment-only corroboration is eligible (HIGH-2)")
    func metadataWithBreakAlignmentIsEligible() {
        let span = makeSpan(anchorProvenance: [])  // no FM provenance
        let metadata = EvidenceLedgerEntry(
            source: .metadata,
            weight: 0.10,
            detail: .metadata(
                cueCount: 1,
                sourceField: .description,
                dominantCueType: .disclosure
            )
        )
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.0,
            metadataEntries: [metadata],
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.eligibilityGate == .eligible,
                "breakAlignment must satisfy the metadata corroboration gate")
    }

    /// playhead-fqc8 cycle-2 review M-4: the FM-anchor exclusion guard in
    /// `computePromotionTrack` must fire for `.fmAcousticCorroborated` too,
    /// not just `.fmConsensus`. The existing FM-coexistence test only
    /// covered `.fmConsensus`. Pin the guard's coverage of the second
    /// FM-class anchor case here so a future refactor can't silently
    /// loosen the exclusion.
    @Test("classifierSeed + fmAcousticCorroborated + breakAlignment stays on standard track (M-4)")
    func classifierSeedPlusFMAcousticStaysStandard() {
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r1", score: 0.85),
            .fmAcousticCorroborated(regionId: "r1", breakStrength: 0.7)
        ])
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.82,
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.promotionTrack == .standard,
                ".fmAcousticCorroborated coexisting with classifierSeed must keep the span on the standard track")
    }

    /// playhead-fqc8 cycle-2 review: `.userCorrection` is NOT an FM-class
    /// anchor — only `.fmConsensus` and `.fmAcousticCorroborated` are.
    /// A span anchored by `.classifierSeed` + `.userCorrection` with the
    /// rest of the quorum met (classifier ≥ 0.70 + breakAlignment) MUST
    /// promote to `.classifierSeedQualified`. Pin this so a future
    /// refactor cannot accidentally treat `.userCorrection` as FM-class.
    @Test("classifierSeed + userCorrection still qualifies for the loose track")
    func classifierSeedPlusUserCorrectionQualifies() {
        let span = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r1", score: 0.85),
            .userCorrection(correctionId: "x", reportedTime: 100.0)
        ])
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let result = mapDecision(
            span: span,
            classifierScore: 0.82,
            breakAlignmentEntries: [breakAlignment]
        )
        #expect(result.promotionTrack == .classifierSeedQualified,
                "userCorrection is not FM-class — quorum still satisfied → qualified track")
    }

    /// Cycle-3 review (score-honesty contract): two `DecisionMapper`
    /// invocations with byte-identical ledgers but spans whose anchor
    /// provenance differs (one routes to `.standard`, the other to
    /// `.classifierSeedQualified`) must emit equal `proposalConfidence`
    /// and `skipConfidence`. The track is a pure threshold-selector
    /// consumed downstream — never a score modulator.
    @Test("Score-honesty contract: identical ledgers produce identical scores across promotion tracks")
    func promotionTrackDoesNotMutateScore() {
        // Same ledger entries for both mappers. Distinguish only by
        // anchor provenance and (for the qualified track) the addition
        // of the .breakAlignment entry that gates the track. We then
        // re-run the standard-track mapper with the SAME ledger so the
        // ledger itself is byte-identical between the two map() calls.
        let breakAlignment = EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: 0.18,
            detail: .breakAlignment(breakStrength: 0.7)
        )
        let classifier = EvidenceLedgerEntry(
            source: .classifier,
            weight: 0.30,
            detail: .classifier(score: 0.85)
        )
        let sharedLedger = [classifier, breakAlignment]

        // Standard-track span: no .classifierSeed → standard.
        let standardSpan = makeSpan(anchorProvenance: [
            .fmConsensus(regionId: "r1", consensusStrength: 0.9)
        ])
        let standardMapper = DecisionMapper(
            span: standardSpan,
            ledger: sharedLedger,
            config: defaultConfig(),
            transcriptQuality: .good
        )
        let standardResult = standardMapper.map()

        // Qualified-track span: classifierSeed + no FM-class anchor +
        // classifier score >= 0.70 + .breakAlignment entry → qualified.
        let qualifiedSpan = makeSpan(anchorProvenance: [
            .classifierSeed(regionId: "r1", score: 0.85)
        ])
        let qualifiedMapper = DecisionMapper(
            span: qualifiedSpan,
            ledger: sharedLedger,
            config: defaultConfig(),
            transcriptQuality: .good
        )
        let qualifiedResult = qualifiedMapper.map()

        // Track selection differs by design.
        #expect(standardResult.promotionTrack == .standard)
        #expect(qualifiedResult.promotionTrack == .classifierSeedQualified)

        // But scores are byte-identical: ledger is the only score input.
        #expect(standardResult.proposalConfidence == qualifiedResult.proposalConfidence,
                "promotionTrack must NOT mutate proposalConfidence — score is a function of ledger only")
        #expect(standardResult.skipConfidence == qualifiedResult.skipConfidence,
                "promotionTrack must NOT mutate skipConfidence — score is a function of ledger only")
    }

    /// playhead-fqc8 cycle-2 review LOW-2: every cap re-stamp loop in
    /// `BackfillEvidenceFusion.buildLedger()` must preserve the input
    /// entry's `subSource`. Today no producer in any family except
    /// `.catalog` populates `subSource`, but the uniform invariant lets
    /// future producers add a sub-source label without a hidden silent
    /// drop. Exercise every family by feeding a hand-constructed entry
    /// with a non-nil `subSource` and asserting it survives the cap
    /// re-stamp into the output ledger.
    @Test("subSource passthrough is preserved across every family's cap re-stamp")
    func subSourcePassthroughIsPreservedAcrossFamilies() {
        let span = makeSpan(anchorProvenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9)])
        // The `EvidenceSubSource` enum is currently catalog-specific
        // (`.transcriptCatalog` / `.fingerprintStore`), but the
        // pass-through invariant is family-agnostic — pick an arbitrary
        // case and assert it survives every loop.
        let probe: EvidenceSubSource = .transcriptCatalog
        let lexEntry = EvidenceLedgerEntry(
            source: .lexical, weight: 0.10,
            detail: .lexical(matchedCategories: ["url"]),
            subSource: probe
        )
        let fmEntry = EvidenceLedgerEntry(
            source: .fm, weight: 0.20,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1"),
            subSource: probe
        )
        let fingerprintEntry = EvidenceLedgerEntry(
            source: .fingerprint, weight: 0.10,
            detail: .fingerprint(matchCount: 1, averageSimilarity: 0.8),
            subSource: probe
        )
        let metadataEntry = EvidenceLedgerEntry(
            source: .metadata, weight: 0.10,
            detail: .metadata(cueCount: 1, sourceField: .description, dominantCueType: .disclosure),
            subSource: probe
        )
        let acousticEntry = EvidenceLedgerEntry(
            source: .acoustic, weight: 0.10,
            detail: .acoustic(breakStrength: 0.7),
            subSource: probe
        )
        let catalogEntry = EvidenceLedgerEntry(
            source: .catalog, weight: 0.10,
            detail: .catalog(entryCount: 2),
            subSource: probe
        )
        let breakAlignmentEntry = EvidenceLedgerEntry(
            source: .breakAlignment, weight: 0.10,
            detail: .breakAlignment(breakStrength: 0.7),
            subSource: probe
        )

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [fmEntry],
            lexicalEntries: [lexEntry],
            acousticEntries: [acousticEntry],
            catalogEntries: [catalogEntry],
            fingerprintEntries: [fingerprintEntry],
            metadataEntries: [metadataEntry],
            breakAlignmentEntries: [breakAlignmentEntry],
            mode: .full,
            config: defaultConfig()
        )
        let ledger = fusion.buildLedger()

        // Every output entry from a family that received a probed input
        // must surface `probe` on its `subSource`. The `.classifier` entry
        // synthesized internally by `buildLedger()` has no producer-side
        // `subSource`, so exclude it.
        let producedFamilies: Set<EvidenceSourceType> = [
            .lexical, .fm, .fingerprint, .metadata,
            .acoustic, .catalog, .breakAlignment
        ]
        for source in producedFamilies {
            let entries = ledger.filter { $0.source == source }
            #expect(entries.count == 1,
                    "expected exactly one \(source) entry in the ledger")
            #expect(entries.first?.subSource == probe,
                    "subSource must survive the \(source) cap re-stamp")
        }
    }
}

// MARK: - ClassificationTrustMatrix integration with fusion pipeline (ef2.4.5)

@Suite("ClassificationTrustMatrix Integration")
struct ClassificationTrustMatrixIntegrationTests {

    private func makeSpan(
        startTime: Double = 10.0,
        endTime: Double = 40.0
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-1", firstAtomOrdinal: 100, lastAtomOrdinal: 200),
            assetId: "asset-1",
            firstAtomOrdinal: 100,
            lastAtomOrdinal: 200,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9)]
        )
    }

    @Test("Trust-modulated FM entry flows through to DecisionMapper proposalConfidence")
    func trustModulationAffectsProposalConfidence() {
        let span = makeSpan()
        // FM entry with trust 0.5: weight 0.4 * 0.5 = 0.2
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.4,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1"),
            classificationTrust: 0.5
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()

        // Classifier contributes 0.0, FM contributes 0.2 (0.4 * 0.5)
        let fmWeight = ledger.filter { $0.source == .fm }.map(\.weight).reduce(0, +)
        #expect(abs(fmWeight - 0.2) < 0.001)

        let mapper = DecisionMapper(span: span, ledger: ledger, config: FusionWeightConfig(), transcriptQuality: .good)
        let result = mapper.map()
        // proposalConfidence should reflect the reduced FM weight
        #expect(result.proposalConfidence < 0.4, "Trust-modulated weight should reduce proposalConfidence")
    }
}

// MARK: - AdDetectionService.estimateTranscriptQuality threshold

@Suite("estimateTranscriptQuality 30% threshold")
struct EstimateTranscriptQualityTests {

    private func makeAtom(ordinal: Int, anchored: Bool) -> AtomEvidence {
        AtomEvidence(
            atomOrdinal: ordinal,
            startTime: Double(ordinal) * 2,
            endTime: Double(ordinal) * 2 + 2,
            isAnchored: anchored,
            anchorProvenance: [],
            hasAcousticBreakHint: false,
            correctionMask: .none
        )
    }

    private func makeService() -> AdDetectionService {
        let store = try! AnalysisStore(path: ":memory:")
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: .default
        )
    }

    @Test("Empty atom list → degraded")
    func emptyAtomsReturnsDegraded() async {
        let service = makeService()
        let quality = await service.estimateTranscriptQuality(atoms: [])
        #expect(quality == .degraded)
    }

    @Test("29 of 100 anchored atoms (29%) → degraded (below 30% threshold)")
    func twentyNinePercentReturnsDegraded() async {
        let service = makeService()
        let atoms = (0..<100).map { makeAtom(ordinal: $0, anchored: $0 < 29) }
        let quality = await service.estimateTranscriptQuality(atoms: atoms)
        #expect(quality == .degraded, "anchoredFraction=0.29 should be below the 30% threshold")
    }

    @Test("30 of 100 anchored atoms (30%) → degraded (threshold is strictly > 0.3)")
    func exactlyThirtyPercentReturnsDegraded() async {
        let service = makeService()
        let atoms = (0..<100).map { makeAtom(ordinal: $0, anchored: $0 < 30) }
        let quality = await service.estimateTranscriptQuality(atoms: atoms)
        #expect(quality == .degraded, "anchoredFraction=0.30 is not > 0.30, so should be degraded")
    }

    @Test("31 of 100 anchored atoms (31%) → good (above 30% threshold)")
    func thirtyOnePercentReturnsGood() async {
        let service = makeService()
        let atoms = (0..<100).map { makeAtom(ordinal: $0, anchored: $0 < 31) }
        let quality = await service.estimateTranscriptQuality(atoms: atoms)
        #expect(quality == .good, "anchoredFraction=0.31 should be above the 30% threshold")
    }
}
