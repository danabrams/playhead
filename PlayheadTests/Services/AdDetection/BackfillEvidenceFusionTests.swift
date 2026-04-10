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

    @Test("SkipEligibilityGate has all four required cases")
    func eligibilityGateCases() {
        let cases: [SkipEligibilityGate] = [
            .eligible,
            .blockedByEvidenceQuorum,
            .blockedByPolicy,
            .blockedByUserCorrection
        ]
        #expect(cases.count == 4)

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
