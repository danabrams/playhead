// RediffConfirmedKindFusionTests.swift
// playhead-xsdz.62: a BYTE-EXACT rediff slot is a deterministic corroborating
// EVIDENCE KIND in the `BackfillEvidenceFusion` corroboration quorum.
//
// Today a byte-exact rediff slot is only a WIDTH-OWNERSHIP marker
// (`AnchorRef.rediffSlot`) — it sets span geometry but does NOT increment the
// corroboration quorum's `distinctKinds.count`. Per the epic thesis (data-backed
// by the xsdz.63 FM eval: on-device FM HEDGES/UNCERTAIN on produced-DAI ads
// carrying URLs/"sponsored by"), a byte-exact-rediff-confirmed rotating region
// IS a DAI-inserted ad BY DEFINITION (deterministic ground truth). So, behind
// `rediffConfirmedKindEnabled`, `buildLedger()` emits a weight-0
// `.rediffConfirmed` ledger entry for any span whose width is owned by the
// byte-exact rediff oracle (`DecodedSpan.carriesRediffByteExactWidth`), giving
// rediff-confirmed DAI a reproducible 2nd deterministic kind so FM's vote stops
// being load-bearing for their eligibility.
//
// Coverage (the four acceptance regression tests + invariants):
//   (a) a byte-exact rediff slot adds a DISTINCT kind → increments the quorum
//       count (RED→GREEN on the fmConsensus count gate).
//   (b) a rediff-confirmed region + one other corroborating kind reaches the
//       eligibility quorum WITHOUT an FM vote (RED→GREEN on the metadata gate).
//   (c) acoustic splice (`.spliceSlot`) does NOT become a kind — not
//       deterministic.
//   (d) non-rediff eligibility, the existing `distinctKinds` behavior, and
//       rediff's width-ownership are unchanged; flag-OFF is byte-identical.

import Foundation
import Testing
@testable import Playhead

@Suite("Rediff-confirmed evidence kind (playhead-xsdz.62)")
struct RediffConfirmedKindFusionTests {

    // MARK: - Fixtures

    /// 30s span (well inside the [5, 180] fmConsensus duration window) with the
    /// given anchor provenance, so duration/quality never confound the gate.
    private func makeSpan(anchorProvenance: [AnchorRef]) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-62", firstAtomOrdinal: 100, lastAtomOrdinal: 200),
            assetId: "asset-62",
            firstAtomOrdinal: 100,
            lastAtomOrdinal: 200,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: anchorProvenance
        )
    }

    /// A positive FM ledger entry (`.containsAd`) — the "FM vote" whose presence
    /// the corroboration quorum otherwise depends on.
    private func fmContainsAd(weight: Double = 0.4) -> EvidenceLedgerEntry {
        .init(source: .fm, weight: weight,
              detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1"))
    }

    /// A metadata cue entry (RSS "sponsored by …") — a soft, non-in-audio
    /// pre-seed that alone CANNOT clear `metadataCorroborationGate`.
    private func metadataCue() -> EvidenceLedgerEntry {
        .init(source: .metadata, weight: 0.15,
              detail: .metadata(cueCount: 1, sourceField: .description, dominantCueType: .sponsorAlias))
    }

    /// Build the fusion ledger for a span. `classifierScore: 0` keeps the
    /// always-present `.classifier` entry weight-0 (excluded from the quorum), so
    /// the ONLY corroborating kinds are the ones we deliberately pass in.
    private func buildLedger(
        span: DecodedSpan,
        rediffKindEnabled: Bool,
        fmEntries: [EvidenceLedgerEntry] = [],
        metadataEntries: [EvidenceLedgerEntry] = []
    ) -> [EvidenceLedgerEntry] {
        BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: fmEntries,
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            metadataEntries: metadataEntries,
            mode: .full,
            config: FusionWeightConfig(rediffConfirmedKindEnabled: rediffKindEnabled)
        ).buildLedger()
    }

    /// Run the mapper over a pre-built ledger and return the eligibility gate.
    private func gate(span: DecodedSpan, ledger: [EvidenceLedgerEntry], rediffKindEnabled: Bool) -> SkipEligibilityGate {
        DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(rediffConfirmedKindEnabled: rediffKindEnabled),
            transcriptQuality: .good
        ).map().eligibilityGate
    }

    // MARK: - (a) A byte-exact rediff slot adds a DISTINCT kind

    @Test("(a) buildLedger emits a weight-0 .rediffConfirmed kind for a byte-exact rediff span (flag ON), none flag OFF")
    func byteExactRediffSlotEmitsDistinctKind() {
        let span = makeSpan(anchorProvenance: [.rediffSlot])

        let ledgerOn = buildLedger(span: span, rediffKindEnabled: true)
        let rediffEntries = ledgerOn.filter { $0.source == .rediffConfirmed }
        #expect(rediffEntries.count == 1, "flag ON: exactly one .rediffConfirmed kind is emitted")
        #expect(rediffEntries.first?.weight == 0.0, "the kind is a deterministic presence marker — zero score mass")

        let ledgerOff = buildLedger(span: span, rediffKindEnabled: false)
        #expect(!ledgerOff.contains { $0.source == .rediffConfirmed },
                "flag OFF: NO .rediffConfirmed kind — byte-identical to pre-xsdz.62")
    }

    @Test("(a) RED→GREEN: rediff supplies the 2nd distinct kind to the fmConsensus count quorum")
    func rediffIncrementsFMConsensusQuorum() {
        // fmConsensus span whose ONLY weighted evidence is the single FM vote.
        // The count quorum needs 2+ distinct corroborating kinds; with just FM
        // it is 1 → blocked. Rediff supplies a deterministic 2nd kind → eligible.
        let span = makeSpan(anchorProvenance: [.fmConsensus(regionId: "r", consensusStrength: 0.9), .rediffSlot])

        // RED (current FM-dependent quorum): flag OFF, only the FM kind counts.
        let ledgerOff = buildLedger(span: span, rediffKindEnabled: false, fmEntries: [fmContainsAd()])
        #expect(gate(span: span, ledger: ledgerOff, rediffKindEnabled: false) == .blockedByEvidenceQuorum,
                "RED: fmConsensus + single FM kind = 1 distinct kind → blocked")

        // GREEN: flag ON, rediff is the deterministic 2nd distinct kind.
        let ledgerOn = buildLedger(span: span, rediffKindEnabled: true, fmEntries: [fmContainsAd()])
        #expect(gate(span: span, ledger: ledgerOn, rediffKindEnabled: true) == .eligible,
                "GREEN: {fm, rediffConfirmed} = 2 distinct kinds → eligible")
    }

    // MARK: - (b) Reaches the quorum WITHOUT an FM vote (the crux)

    @Test("(b) RED→GREEN: rediff + one other kind reaches eligibility WITHOUT an FM vote")
    func rediffReachesQuorumWithoutFM() {
        // No FM anchor and NO FM ledger entry (FM absent/uncertain — the xsdz.63
        // produced-DAI case). A presence anchor + `.rediffSlot`, metadata cue
        // only. computeGate routes through `metadataCorroborationGate`.
        let span = makeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7), .rediffSlot])

        // RED (current): metadata alone cannot corroborate → blocked.
        let ledgerOff = buildLedger(span: span, rediffKindEnabled: false, metadataEntries: [metadataCue()])
        #expect(!ledgerOff.contains { $0.source == .fm }, "precondition: there is NO FM vote in the ledger")
        #expect(gate(span: span, ledger: ledgerOff, rediffKindEnabled: false) == .blockedByEvidenceQuorum,
                "RED: metadata-only, no FM → blocked by evidence quorum")

        // GREEN: the deterministic rediff kind corroborates the metadata cue.
        let ledgerOn = buildLedger(span: span, rediffKindEnabled: true, metadataEntries: [metadataCue()])
        #expect(!ledgerOn.contains { $0.source == .fm }, "still NO FM vote — eligibility comes from rediff, not FM")
        #expect(gate(span: span, ledger: ledgerOn, rediffKindEnabled: true) == .eligible,
                "GREEN: metadata + deterministic rediff kind → eligible, no FM required")
    }

    @Test("(b-bonus) rediff corroborates an fmAcoustic-anchored span with no other non-FM evidence")
    func rediffCorroboratesFMAcoustic() {
        // fmAcousticCorroborated needs a non-FM external corroborator. With only
        // the single FM entry it blocks; the deterministic rediff kind clears it.
        let span = makeSpan(anchorProvenance: [.fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7), .rediffSlot])

        let ledgerOff = buildLedger(span: span, rediffKindEnabled: false, fmEntries: [fmContainsAd(weight: 0.35)])
        #expect(gate(span: span, ledger: ledgerOff, rediffKindEnabled: false) == .blockedByEvidenceQuorum,
                "RED: fmAcoustic + FM only, no external corroboration → blocked")

        let ledgerOn = buildLedger(span: span, rediffKindEnabled: true, fmEntries: [fmContainsAd(weight: 0.35)])
        #expect(gate(span: span, ledger: ledgerOn, rediffKindEnabled: true) == .eligible,
                "GREEN: rediff kind is deterministic external corroboration → eligible")
    }

    // MARK: - (c) Acoustic splice does NOT become a kind

    @Test("(c) acoustic splice (.spliceSlot) never becomes a kind and never corroborates")
    func acousticSpliceDoesNotBecomeKind() {
        // Same setup as (b) but WIDTH-owned by acoustic splice, not byte-exact
        // rediff. Splice is not deterministic — it must never emit the kind.
        let spliceSpan = makeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7), .spliceSlot])
        let spliceLedger = buildLedger(span: spliceSpan, rediffKindEnabled: true, metadataEntries: [metadataCue()])
        #expect(!spliceLedger.contains { $0.source == .rediffConfirmed },
                "flag ON: a .spliceSlot span emits NO .rediffConfirmed kind (not byte-exact)")
        #expect(gate(span: spliceSpan, ledger: spliceLedger, rediffKindEnabled: true) == .blockedByEvidenceQuorum,
                "acoustic splice does NOT corroborate — metadata-only stays blocked")

        // Direct contrast: byte-exact rediff on the identical setup IS eligible.
        let rediffSpan = makeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7), .rediffSlot])
        let rediffLedger = buildLedger(span: rediffSpan, rediffKindEnabled: true, metadataEntries: [metadataCue()])
        #expect(gate(span: rediffSpan, ledger: rediffLedger, rediffKindEnabled: true) == .eligible,
                "byte-exact rediff DOES corroborate — the deterministic distinction")
    }

    // MARK: - (d) No regression to non-rediff eligibility / existing behavior / width

    @Test("(d) a non-rediff span's ledger sources are identical flag ON vs OFF")
    func nonRediffLedgerUnchanged() {
        let span = makeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7)])
        let sourcesOff = buildLedger(span: span, rediffKindEnabled: false, metadataEntries: [metadataCue()]).map(\.source)
        let sourcesOn = buildLedger(span: span, rediffKindEnabled: true, metadataEntries: [metadataCue()]).map(\.source)
        #expect(sourcesOn == sourcesOff, "no .rediffSlot width → the flag adds nothing; ledger is byte-identical")
        #expect(!sourcesOn.contains(.rediffConfirmed))
    }

    @Test("(d) a genuine non-rediff 2-kind fmConsensus span stays eligible, flag ON and OFF")
    func nonRediffQuorumStillEligible() {
        let span = makeSpan(anchorProvenance: [.fmConsensus(regionId: "r", consensusStrength: 0.9)])
        let ledger: [EvidenceLedgerEntry] = [
            fmContainsAd(),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"])),
        ]
        #expect(gate(span: span, ledger: ledger, rediffKindEnabled: false) == .eligible)
        #expect(gate(span: span, ledger: ledger, rediffKindEnabled: true) == .eligible,
                "the rediff-kind flag never touches a non-rediff span's existing quorum")
    }

    @Test("(d) flag ON adds NO score mass — proposal/skip confidence stay byte-identical")
    func rediffKindAddsNoScoreMass() {
        let span = makeSpan(anchorProvenance: [.fmConsensus(regionId: "r", consensusStrength: 0.9), .rediffSlot])
        let fm = [fmContainsAd()]

        let ledgerOff = buildLedger(span: span, rediffKindEnabled: false, fmEntries: fm)
        let ledgerOn = buildLedger(span: span, rediffKindEnabled: true, fmEntries: fm)
        let off = DecisionMapper(span: span, ledger: ledgerOff, config: FusionWeightConfig(), transcriptQuality: .good).map()
        let on = DecisionMapper(span: span, ledger: ledgerOn, config: FusionWeightConfig(rediffConfirmedKindEnabled: true), transcriptQuality: .good).map()

        #expect(on.proposalConfidence == off.proposalConfidence, "weight-0 kind must not change proposalConfidence")
        #expect(on.skipConfidence == off.skipConfidence, "weight-0 kind must not change skipConfidence")
    }

    @Test("(d) buildLedger never mutates rediff width-ownership on the span")
    func rediffWidthOwnershipPreserved() {
        let span = makeSpan(anchorProvenance: [.rediffSlot])
        _ = buildLedger(span: span, rediffKindEnabled: true)
        // The kind is an additive ledger entry; the span's width provenance is
        // untouched, so the one canonical definition still reports byte-exact.
        #expect(span.carriesRediffByteExactWidth)
        #expect(span.anchorProvenance.contains(.rediffSlot))
    }

    // MARK: - (e) Config wire-in: ships OFF, threads verbatim (mirrors the
    // sibling CertaintyTieredSkipFlagsWireInTests). Guards the eligibility-
    // affecting flag against a silent default-flip or a dropped
    // AdDetectionConfig → FusionWeightConfig threading — the FusionWeightConfig
    // default is separately pinned below.

    @Test("(e) AdDetectionConfig.default ships the rediff-confirmed KIND flag OFF")
    func configDefaultShipsOff() {
        #expect(AdDetectionConfig.default.rediffConfirmedKindEnabled == false,
                "eligibility-affecting KIND flag ships OFF — the flip is a corpus-A/B decision")
    }

    @Test("(e) AdDetectionConfig.init defaults rediffConfirmedKindEnabled to false when omitted")
    func configInitOmittedDefaultsOff() {
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1"
        )
        #expect(omitted.rediffConfirmedKindEnabled == false, "init default must match .default (OFF)")
    }

    @Test("(e) AdDetectionConfig.init carries rediffConfirmedKindEnabled through verbatim")
    func configInitCarriesFlagVerbatim() {
        let on = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            rediffConfirmedKindEnabled: true
        )
        #expect(on.rediffConfirmedKindEnabled == true,
                "the flag must thread through the init verbatim, not be pinned to a constant")
    }

    @Test("(e) FusionWeightConfig() defaults the rediff-confirmed KIND flag OFF")
    func fusionConfigDefaultsOff() {
        #expect(FusionWeightConfig().rediffConfirmedKindEnabled == false,
                "the bare FusionWeightConfig used at the non-threaded decision-log sites emits no kind")
    }
}
