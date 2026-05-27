// AudioForensicsFusionTests.swift
// playhead-xsdz.8: Fusion-seam contract for the composite audio-forensics
// boundary channel.
//
// `AdDetectionService.buildEvidenceLedger` gates the channel with a single
// ternary: `config.audioForensicsEnabled ? detector.buildEntries(...) : []`.
// The flag-OFF result is therefore an EMPTY `audioForensicsEntries` array fed
// to `BackfillEvidenceFusion`. These tests pin both halves of that contract at
// the fusion seam:
//   • EMPTY audioForensicsEntries (= flag OFF) ⇒ ledger byte-identical to the
//     pre-xsdz.8 call that never passed the parameter at all.
//   • POPULATED audioForensicsEntries (= flag ON, detector fired) ⇒ exactly
//     one `.audioForensics` entry, capped at `audioForensicsCap`, as its own
//     distinct kind (one merged channel, not three).

import Foundation
import Testing

@testable import Playhead

@Suite("AudioForensics fusion seam (playhead-xsdz.8)")
struct AudioForensicsFusionTests {

    private func makeSpan() -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-1", firstAtomOrdinal: 100, lastAtomOrdinal: 200),
            assetId: "asset-1",
            firstAtomOrdinal: 100,
            lastAtomOrdinal: 200,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: [.classifierSeed(regionId: "r1", score: 0.85)]
        )
    }

    /// A representative non-empty ledger input so the byte-identity assertion
    /// has real content to compare (not just the always-present classifier).
    private func baseFusion(
        audioForensicsEntries: [EvidenceLedgerEntry],
        config: FusionWeightConfig = FusionWeightConfig()
    ) -> BackfillEvidenceFusion {
        BackfillEvidenceFusion(
            span: makeSpan(),
            classifierScore: 0.6,
            fmEntries: [],
            lexicalEntries: [
                EvidenceLedgerEntry(source: .lexical, weight: 0.15, detail: .lexical(matchedCategories: ["sponsor"]))
            ],
            acousticEntries: [
                EvidenceLedgerEntry(source: .acoustic, weight: 0.10, detail: .acoustic(breakStrength: 0.5))
            ],
            catalogEntries: [],
            audioForensicsEntries: audioForensicsEntries,
            mode: .full,
            config: config
        )
    }

    /// Structural fingerprint of a ledger entry for byte-identity comparison.
    private func fingerprint(_ ledger: [EvidenceLedgerEntry]) -> [String] {
        ledger.map { "\($0.source.rawValue):\($0.weight):\($0.classificationTrust):\(String(describing: $0.subSource))" }
    }

    // MARK: - Flag OFF (empty entries) ⇒ byte-identical

    @Test("Empty audioForensicsEntries ⇒ no .audioForensics entry in ledger")
    func flagOffNoEntry() {
        let ledger = baseFusion(audioForensicsEntries: []).buildLedger()
        #expect(!ledger.contains { $0.source == .audioForensics },
                "Flag-off (empty entries) must produce NO audioForensics ledger entry")
    }

    @Test("Empty audioForensicsEntries ⇒ ledger byte-identical to the pre-xsdz.8 call shape")
    func flagOffByteIdentical() {
        // The pre-xsdz.8 call shape: no audioForensicsEntries argument at all
        // (defaults to []). The flag-off path passes [] explicitly. Both must
        // produce the SAME ledger.
        let withDefault = BackfillEvidenceFusion(
            span: makeSpan(),
            classifierScore: 0.6,
            fmEntries: [],
            lexicalEntries: [
                EvidenceLedgerEntry(source: .lexical, weight: 0.15, detail: .lexical(matchedCategories: ["sponsor"]))
            ],
            acousticEntries: [
                EvidenceLedgerEntry(source: .acoustic, weight: 0.10, detail: .acoustic(breakStrength: 0.5))
            ],
            catalogEntries: [],
            mode: .full,
            config: FusionWeightConfig()
        ).buildLedger()

        let withExplicitEmpty = baseFusion(audioForensicsEntries: []).buildLedger()

        #expect(fingerprint(withDefault) == fingerprint(withExplicitEmpty),
                "Flag-off path must be byte-identical to the pre-xsdz.8 default-parameter call")
    }

    // MARK: - Flag ON (populated entries) ⇒ one capped distinct kind

    @Test("Populated audioForensicsEntries ⇒ exactly one .audioForensics entry, capped")
    func flagOnEmitsCappedEntry() throws {
        let cfg = FusionWeightConfig()  // audioForensicsCap = 0.20
        let input = [
            EvidenceLedgerEntry(
                source: .audioForensics,
                weight: 0.18,
                detail: .audioForensics(boundaryScore: 0.9, dominantSignal: "loudnessJump", contributingSignalCount: 3)
            )
        ]
        let ledger = baseFusion(audioForensicsEntries: input, config: cfg).buildLedger()

        let af = ledger.filter { $0.source == .audioForensics }
        try #require(af.count == 1, "Exactly one merged audioForensics entry expected")
        #expect(af[0].weight <= cfg.audioForensicsCap)
        #expect(abs(af[0].weight - 0.18) < 1e-9, "Weight below cap passes through unchanged")
    }

    @Test("Over-cap audioForensics weight is clamped to audioForensicsCap")
    func overCapClamped() throws {
        let cfg = FusionWeightConfig()  // audioForensicsCap = 0.20
        let input = [
            EvidenceLedgerEntry(
                source: .audioForensics,
                weight: 0.95,  // way over cap
                detail: .audioForensics(boundaryScore: 1.0, dominantSignal: "spectralShift", contributingSignalCount: 4)
            )
        ]
        let ledger = baseFusion(audioForensicsEntries: input, config: cfg).buildLedger()
        let af = ledger.filter { $0.source == .audioForensics }
        try #require(af.count == 1)
        #expect(abs(af[0].weight - cfg.audioForensicsCap) < 1e-9,
                "Over-cap weight must clamp to audioForensicsCap (0.20)")
    }

    @Test("audioForensics is a DISTINCT kind — it increments distinctKinds for the quorum gate")
    func distinctKindFromAcoustic() {
        let cfg = FusionWeightConfig()
        let input = [
            EvidenceLedgerEntry(
                source: .audioForensics,
                weight: 0.18,
                detail: .audioForensics(boundaryScore: 0.9, dominantSignal: "noiseFloor", contributingSignalCount: 2)
            )
        ]
        let ledger = baseFusion(audioForensicsEntries: input, config: cfg).buildLedger()

        // The ledger carries both an .acoustic and an .audioForensics entry —
        // they must remain separate kinds (the merged channel is ONE kind, but
        // distinct from .acoustic) so the corroboration quorum counts them as
        // two independent contributions.
        let kinds = Set(ledger.map(\.source))
        #expect(kinds.contains(.acoustic))
        #expect(kinds.contains(.audioForensics))
    }

    // MARK: - SourceEvidenceFamily orthogonality

    @Test("audioForensics shares the acoustic family (cannot self-corroborate the acoustic channels)")
    func sharesAcousticFamily() {
        #expect(SourceEvidenceFamily.for(.audioForensics) == .acoustic)
        // Same family as .acoustic / .breakAlignment / .musicBed ⇒ a
        // .audioForensics decision still needs a DIFFERENT-family corroborator
        // (textual / model / reference), honoring "never the sole promoter".
        #expect(SourceEvidenceFamily.for(.audioForensics) == SourceEvidenceFamily.for(.acoustic))
        #expect(SourceEvidenceFamily.for(.audioForensics) == SourceEvidenceFamily.for(.breakAlignment))
    }

    @Test("audioForensics is NOT observability-only (it is a fusion input)")
    func notObservabilityOnly() {
        #expect(EvidenceSourceType.audioForensics.isObservabilityOnly == false)
    }

    // MARK: - Corroboration-gate parity with acoustic-family peers

    @Test("FM-acoustic span corroborated only by audioForensics is eligible (parity with .acoustic/.breakAlignment)")
    func audioForensicsCorroboratesFMAcousticSpan() {
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-1", firstAtomOrdinal: 100, lastAtomOrdinal: 200),
            assetId: "asset-1",
            firstAtomOrdinal: 100,
            lastAtomOrdinal: 200,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: [.fmAcousticCorroborated(regionId: "r1", breakStrength: 0.7)]
        )
        // Ledger: an FM positive entry + a single audioForensics corroborator.
        // The FM-acoustic quorum gate needs ONE non-FM in-audio corroborator;
        // audioForensics must satisfy it, exactly like .acoustic would.
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.0, detail: .classifier(score: 0.0)),
            EvidenceLedgerEntry(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(
                source: .audioForensics,
                weight: 0.18,
                detail: .audioForensics(boundaryScore: 0.9, dominantSignal: "loudnessJump", contributingSignalCount: 3)
            ),
        ]
        let decision = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        ).map()

        #expect(decision.eligibilityGate == .eligible,
                "audioForensics must corroborate an FM-acoustic span on par with .acoustic / .breakAlignment")
    }

    // MARK: - OFF-by-default config contract

    @Test("AdDetectionConfig.default keeps audioForensicsEnabled false")
    func defaultConfigFlagOff() {
        #expect(AdDetectionConfig.default.audioForensicsEnabled == false,
                "OFF-by-default is load-bearing: .default must keep the channel inert")
    }

    @Test("AdDetectionConfig initializer defaults audioForensicsEnabled to false")
    func initializerDefaultsFlagOff() {
        let cfg = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "xsdz8-test"
        )
        #expect(cfg.audioForensicsEnabled == false)
    }

    @Test("FusionWeightConfig.default exposes a modest audioForensicsCap (0.20)")
    func defaultCapIsModest() {
        let cfg = FusionWeightConfig()
        #expect(abs(cfg.audioForensicsCap - 0.20) < 1e-9)
        // Conservative-corroborator intent: the cap is no larger than the
        // RMS-drop acoustic cap and far below the lexical-auto-ad cap that can
        // drive a skip alone.
        #expect(cfg.audioForensicsCap <= cfg.acousticCap + 1e-9)
        #expect(cfg.audioForensicsCap < cfg.lexicalAutoAdCap)
    }
}
