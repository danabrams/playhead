// CertaintyTieredSkipShipsOnTests.swift
// playhead-nqey: `certaintyTieredSkipEnabled` ships ON.
//
// WHAT THIS SUITE IS FOR, AND WHY IT IS NOT A DUPLICATE OF THE sik9 SUITE.
// `PostRollGuardByteAnchoredExemptionTests` (playhead-sik9) proves the guard's
// LOGIC by handing `DecisionMapper` a config it builds itself
// (`FusionWeightConfig(certaintyTieredEnabled: true)`). That test passes whether
// the flag ships ON or OFF — it never asks what production actually sends.
//
// This suite asks exactly that. Every arm below builds its `FusionWeightConfig`
// the way `AdDetectionService.runBackfill` does — from
// `AdDetectionConfig.default`, field for field — so flipping the shipped default
// back to OFF turns these red. That is the point: the enablement is the
// deliverable, so the enablement is what is pinned.
//
// THE FAILURE MODE THIS SUITE IS SHAPED AGAINST. An enablement that demotes
// NOTHING is a no-op dressed as a decision; an enablement that demotes
// EVERYTHING has lost the "certainty-tiered" half of its name. So the arms come
// in pairs, and each pair differs in ONE input:
//
//   host-read floor   demotes  0.70 non-rediff, mid-episode   → .markOnly
//                     spares   0.90 non-rediff, mid-episode   → .eligible
//   post-roll guard   demotes  unanchored tail inside 90 s    → .markOnly
//                     spares   byte-exact rediff tail, same   → .eligible
//
// Neither half is allowed to be inert and neither is allowed to be total.
//
// SCOPE, PINNED AS A TEST RATHER THAN A COMMENT (`flagIsInertForBareFusionConfig`
// below). The flag has exactly ONE consumer: `DecisionMapper`, constructed once,
// in `runBackfill`. Rows minted by `mintByteExactDayZeroMarks`, by the hot path
// and by the aggregator are built from a bare `FusionWeightConfig()` or from no
// fusion config at all, and are untouched by this flip. The bead's writeup says
// otherwise — it argues the flip would have demoted Dan's DE0784D8 post-roll at
// 5462.6–5522.7 — but that window is a `dayZeroRediffByteExact` mint, which
// writes its `AdWindow` and its `eligibilityGate` directly and never consults
// this mapper. sik9's exemption is still needed, on the lagged
// `.rediffSlot`-rewritten fusion path; it is just not needed for the window the
// bead named.
//
// INHERITED NARROWNESS (playhead-6qvf), stated so nothing here is read as a
// claim it does not make: `carriesRediffByteExactWidth` is
// `anchorProvenance.contains(.rediffSlot)`, and `.rediffSlot` is stamped by the
// byte-primary differ AND by the ~1 s chroma fallback. The exemption is
// therefore NOT byte-exact-only. It does not matter for the safety of this flip
// — the flag can only move `.eligible → .markOnly`, so an over-broad exemption
// can at worst leave a span exactly where the OFF build already left it — but it
// is not fixed here.

import Foundation
import Testing

@testable import Playhead

@Suite("Certainty-tiered skip ships ON (playhead-nqey)")
struct CertaintyTieredSkipShipsOnTests {

    // MARK: - The shipped config, reconstructed the way runBackfill does

    /// `AdDetectionService.runBackfill` (Steps 12–14) builds the fusion config
    /// from `AdDetectionConfig` by threading exactly these fields. Rebuilding it
    /// here from `AdDetectionConfig.default` — rather than hand-writing
    /// `certaintyTieredEnabled: true` — is what makes every arm below a test of
    /// the SHIPPED state instead of a test of the mechanism.
    private static var shippedFusionConfig: FusionWeightConfig {
        FusionWeightConfig(
            certaintyTieredEnabled: AdDetectionConfig.default.certaintyTieredSkipEnabled,
            hostReadConfidenceFloor: AdDetectionConfig.default.hostReadConfidenceFloor,
            postRollGuardSeconds: AdDetectionConfig.default.postRollGuardSeconds,
            rediffConfirmedKindEnabled: AdDetectionConfig.default.rediffConfirmedKindEnabled
        )
    }

    /// A 3600 s episode. Long enough that a mid-episode span is nowhere near the
    /// 90 s post-roll window, so the floor arms below isolate the floor.
    private static let episodeDuration = 3600.0

    /// Mid-episode: 1800–1830. `episodeDuration - endTime == 1770`, so the
    /// post-roll guard cannot fire and only the floor is observable.
    private func midEpisodeSpan(anchorProvenance: [AnchorRef]) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "nqey-mid", firstAtomOrdinal: 400, lastAtomOrdinal: 430),
            assetId: "nqey-mid",
            firstAtomOrdinal: 400,
            lastAtomOrdinal: 430,
            startTime: 1800.0,
            endTime: 1830.0,
            anchorProvenance: anchorProvenance
        )
    }

    /// A tail span ending 30 s before the episode end — INSIDE the shipped 90 s
    /// guard window, and by a margin (60 s) large enough that the arm is not
    /// testing a boundary rounding.
    private func tailSpan(anchorProvenance: [AnchorRef]) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "nqey-tail", firstAtomOrdinal: 900, lastAtomOrdinal: 930),
            assetId: "nqey-tail",
            firstAtomOrdinal: 900,
            lastAtomOrdinal: 930,
            startTime: 3520.0,
            endTime: Self.episodeDuration - 30.0,
            anchorProvenance: anchorProvenance
        )
    }

    /// skipConfidence ≈ 0.70 — BELOW the shipped 0.9 floor.
    private func belowFloorLedger() -> [EvidenceLedgerEntry] {
        [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.7)),
        ]
    }

    /// skipConfidence == 0.90 bit-exactly — AT the shipped floor, so `0.9 < 0.9`
    /// is false and the floor cannot fire. Any demotion seen with this ledger is
    /// the post-roll guard's doing and nothing else's.
    private func atFloorLedger() -> [EvidenceLedgerEntry] {
        [.init(source: .classifier, weight: 0.90, detail: .classifier(score: 0.9))]
    }

    private func map(
        _ span: DecodedSpan,
        ledger: [EvidenceLedgerEntry],
        config: FusionWeightConfig = CertaintyTieredSkipShipsOnTests.shippedFusionConfig,
        episodeDuration: Double? = CertaintyTieredSkipShipsOnTests.episodeDuration
    ) -> DecisionResult {
        DecisionMapper(
            span: span,
            ledger: ledger,
            config: config,
            transcriptQuality: .good,
            episodeDuration: episodeDuration
        ).map()
    }

    // MARK: - The flip itself

    // NOTE the wording. It must NOT collide with the wire-in suite's own
    // default-shipping test: `mutation-battery.sh` identifies an expected
    // failure by its Swift Testing DISPLAY name, so two tests sharing a name
    // across suites would make every expectation naming it ambiguous.
    @Test("the shipped AdDetectionConfig.default has the certainty-tiered gate ON at 0.9 / 90.0")
    func shippedDefaultIsOn() {
        #expect(AdDetectionConfig.default.certaintyTieredSkipEnabled == true,
                "playhead-nqey: the gate ships ON (Dan's Gate-2 decision, 2026-08-01, conditional on sik9 = #330)")
        #expect(AdDetectionConfig.default.hostReadConfidenceFloor == 0.9,
                "flipping the switch must not silently move T off the 2026-07-17 themove calibration")
        #expect(AdDetectionConfig.default.postRollGuardSeconds == 90.0,
                "flipping the switch must not silently move the guard off Dan's 2026-07-19 90 s")
    }

    /// The init default and `.default` must agree, or a caller that omits the
    /// field gets a different policy from production — the divergence this repo
    /// has been bitten by before.
    @Test("the AdDetectionConfig init default matches the shipped .default")
    func initDefaultMatchesShippedDefault() {
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1"
        )
        #expect(omitted.certaintyTieredSkipEnabled == AdDetectionConfig.default.certaintyTieredSkipEnabled)
        #expect(omitted.hostReadConfidenceFloor == AdDetectionConfig.default.hostReadConfidenceFloor)
        #expect(omitted.postRollGuardSeconds == AdDetectionConfig.default.postRollGuardSeconds)
    }

    // MARK: - Direction 1: what NEWLY DEMOTES at the shipped default

    /// The host-read half. A non-rediff span at 0.70, mid-episode: OFF it is
    /// `.eligible`, ON it is `.markOnly`. Both arms are run here so the DELTA is
    /// the assertion, not just the post-state.
    @Test("NEWLY DEMOTED: a 0.70 non-rediff host-read span is eligible OFF and markOnly at the shipped default")
    func hostReadBelowFloorNewlyDemotes() {
        let span = midEpisodeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7)])
        let off = map(span, ledger: belowFloorLedger(),
                      config: FusionWeightConfig(certaintyTieredEnabled: false))
        let on = map(span, ledger: belowFloorLedger())

        #expect(off.eligibilityGate == .eligible,
                "pre-flip baseline: this span auto-skips today — if it did not, the arm would be vacuous")
        #expect(on.eligibilityGate == .markOnly,
                "playhead-nqey: an uncertain host-read now banners instead of skipping")
        #expect(abs(on.skipConfidence - off.skipConfidence) < 1e-12,
                "the demotion must not move the score (score-does-not-follow-the-gate)")
    }

    /// The post-roll half. An unanchored tail inside the 90 s window: OFF
    /// `.eligible`, ON `.markOnly`. The ledger is AT the floor, so the floor is
    /// provably not what fired.
    @Test("NEWLY DEMOTED: an unanchored tail inside the 90s window is eligible OFF and markOnly at the shipped default")
    func unanchoredTailNewlyDemotes() {
        let span = tailSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.9)])
        let off = map(span, ledger: atFloorLedger(),
                      config: FusionWeightConfig(certaintyTieredEnabled: false))
        let on = map(span, ledger: atFloorLedger())

        #expect(off.eligibilityGate == .eligible, "pre-flip baseline")
        #expect(on.eligibilityGate == .markOnly,
                "playhead-nqey: an invented tail edge near the episode end now banners")
        #expect(abs(on.skipConfidence - 0.90) < 1e-12,
                "AT the floor, so the floor cannot be what demoted this — and the score is untouched")
    }

    // MARK: - Direction 2: what STAYS ELIGIBLE at the shipped default

    /// The floor's spare. Same geometry, same provenance shape, ONE input
    /// changed — the ledger clears 0.9 — and the span keeps auto-skipping. This
    /// is the arm that fails if the flip demotes everything.
    @Test("STILL ELIGIBLE: a non-rediff span AT the 0.9 floor keeps auto-skipping at the shipped default")
    func hostReadAtFloorStaysEligible() {
        let span = midEpisodeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.9)])
        let result = map(span, ledger: atFloorLedger())
        #expect(result.eligibilityGate == .eligible,
                "0.9 < 0.9 is false — the floor is a floor, not a ceiling on everything")
    }

    /// sik9's exemption, read through the SHIPPED config rather than a
    /// hand-built one. Same tail geometry as `unanchoredTailNewlyDemotes`, same
    /// at-floor ledger; the ONLY difference is that the width is rediff-owned.
    @Test("STILL ELIGIBLE: a rediff-anchored tail inside the 90s window is exempt at the shipped default")
    func rediffAnchoredTailStaysEligible() {
        let exempt = map(tailSpan(anchorProvenance: [.rediffSlot]), ledger: atFloorLedger())
        let guarded = map(tailSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.9)]),
                          ledger: atFloorLedger())

        #expect(exempt.eligibilityGate == .eligible,
                "playhead-sik9 holds at the shipped parameters: position alone must not demote a byte-anchored tail")
        #expect(guarded.eligibilityGate == .markOnly,
                "the discriminating negative — without rediff width the same geometry demotes")
        #expect(abs(exempt.skipConfidence - guarded.skipConfidence) < 1e-12,
                "only ACTIONABILITY differs between the exempt and the guarded shape")
    }

    /// The floor's rediff exemption, at the shipped parameters. A rediff span at
    /// 0.70 mid-episode — below the floor, nowhere near the guard — stays
    /// eligible, so the two exemptions are confirmed to be the same carve-out.
    @Test("STILL ELIGIBLE: a rediff-anchored span below the 0.9 floor is exempt at the shipped default")
    func rediffAnchoredBelowFloorStaysEligible() {
        let result = map(midEpisodeSpan(anchorProvenance: [.rediffSlot]), ledger: belowFloorLedger())
        #expect(result.eligibilityGate == .eligible,
                "the certainty is in the bytes, not in the ledger sum")
        #expect(abs(result.skipConfidence - 0.70) < 0.001,
                "declining to demote must not touch the presence score")
    }

    /// playhead-6qvf: the DISCRIMINATING negative for the floor carve-out —
    /// the one an implementation keyed on "width ownership" rather than on
    /// "byte-exact" gets wrong while passing every test above.
    ///
    /// `.rediffSlotChroma` is the rediff oracle's OTHER differ arm: the same
    /// re-fetch, the same slot machinery, a ~1 s chroma-fingerprint alignment
    /// instead of a byte-run one. It really does own the span's width. It has
    /// no claim on the floor exemption, whose whole justification is that the
    /// origin served different BYTES over exactly this range.
    ///
    /// Both directions are asserted together because the pair is the point: the
    /// two spans differ ONLY in which differ arm set the width, and they must
    /// land on opposite sides of the floor.
    @Test("NOT EXEMPT: a rediff CHROMA span below the 0.9 floor demotes, where the byte arm is spared")
    func rediffChromaBelowFloorIsNotExempt() {
        let chroma = map(midEpisodeSpan(anchorProvenance: [.rediffSlotChroma]), ledger: belowFloorLedger())
        #expect(chroma.eligibilityGate == .markOnly,
                "a ~1 s chroma alignment is not the deterministic certainty the carve-out is for")
        let byteExact = map(midEpisodeSpan(anchorProvenance: [.rediffSlot]), ledger: belowFloorLedger())
        #expect(byteExact.eligibilityGate == .eligible,
                "the discriminating positive — same ledger, same geometry, byte-derived width")
        #expect(abs(chroma.skipConfidence - byteExact.skipConfidence) < 1e-12,
                "only ACTIONABILITY differs; the presence score is identical")
    }

    // MARK: - Neither inert nor total, asserted as one property

    /// The whole acceptance criterion in one arm: across a population of four
    /// shapes at the SHIPPED parameters, the flip must demote a strict, non-empty
    /// SUBSET. A flip that demotes 0 of 4 bought nothing; a flip that demotes
    /// 4 of 4 has stopped being certainty-tiered.
    @Test("the shipped flip demotes a strict non-empty subset — neither inert nor total")
    func flipIsNeitherInertNorTotal() {
        let population: [(label: String, span: DecodedSpan, ledger: [EvidenceLedgerEntry])] = [
            ("host-read 0.70 mid-episode",
             midEpisodeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7)]),
             belowFloorLedger()),
            ("host-read 0.90 mid-episode",
             midEpisodeSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.9)]),
             atFloorLedger()),
            ("unanchored tail in-window",
             tailSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.9)]),
             atFloorLedger()),
            ("rediff tail in-window",
             tailSpan(anchorProvenance: [.rediffSlot]),
             atFloorLedger()),
        ]

        var demoted: [String] = []
        var kept: [String] = []
        for case let (label, span, ledger) in population {
            let off = map(span, ledger: ledger, config: FusionWeightConfig(certaintyTieredEnabled: false))
            let on = map(span, ledger: ledger)
            // The flip must NEVER promote, for any shape.
            #expect(on.eligibilityGate.severity >= off.eligibilityGate.severity,
                    "\(label): the flip moved the gate towards MORE actionable — it must only ever demote")
            #expect(abs(on.skipConfidence - off.skipConfidence) < 1e-12,
                    "\(label): the flip must never move a score")
            if off.eligibilityGate == .eligible && on.eligibilityGate != .eligible {
                demoted.append(label)
            } else if on.eligibilityGate == .eligible {
                kept.append(label)
            }
        }

        #expect(demoted.count == 2, "expected exactly the two uncertain shapes to demote, got \(demoted)")
        #expect(kept.count == 2, "expected exactly the two certain shapes to survive, got \(kept)")
        #expect(!demoted.isEmpty, "an enablement that demotes nothing is a no-op dressed as a decision")
        #expect(!kept.isEmpty, "an enablement that demotes everything is not certainty-TIERED")
    }

    // MARK: - Scope: the flip cannot reach a producer that does not use this config

    /// The other three `FusionWeightConfig()` construction sites in
    /// `AdDetectionService` are bare — they read only `.classifierCap`. A bare
    /// config must therefore stay OFF even though `AdDetectionConfig.default` is
    /// now ON, or the flip would silently leak into the hot path and the
    /// aggregator decision logs.
    @Test("a bare FusionWeightConfig stays OFF — the flip does not leak to the hot path or the aggregator")
    func flagIsInertForBareFusionConfig() {
        #expect(FusionWeightConfig().certaintyTieredEnabled == false,
                "the FusionWeightConfig init default is the OFF-by-construction seam the three bare sites rely on")

        let span = tailSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.9)])
        let bare = map(span, ledger: atFloorLedger(), config: FusionWeightConfig())
        #expect(bare.eligibilityGate == .eligible,
                "a bare config must decide exactly as it did before nqey")
    }

    /// The guard is inert without a known duration, at the SHIPPED parameters —
    /// so the flip cannot demote anything on an episode whose length the store
    /// does not know. `runBackfill` normalizes its `0 == unknown` sentinel to
    /// `nil` before it reaches the mapper.
    @Test("unknown episode duration keeps the shipped guard inert")
    func unknownDurationKeepsGuardInert() {
        let span = tailSpan(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.9)])
        let result = map(span, ledger: atFloorLedger(), episodeDuration: nil)
        #expect(result.eligibilityGate == .eligible,
                "never guess the episode end — unknown duration means the guard does not fire")
    }
}
