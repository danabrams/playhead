// UnanchoredExtentAutoSkipGateTests.swift
// playhead-2350: presence-vs-extent split + the unanchored-edge auto-skip block.
//
// The bug this pins, from the 2026-07-25 THEMOVE Catalyst replay: the SAME four
// boundaries came out of the catalog-on, catalog-off and correct-show arms —
// 74.58–89.82, 1241.70–1286.94, 1326.10–1335.90, 3493.02–3537.95 — every
// persisted edge `.unanchored`, every window `eligibilityGate == .eligible`, and
// in the correct-show arm all four `autoSkipEligible`. FM coarse scans had
// correctly reported `containsAd` over huge windows (17.04–1183.62,
// 49.02–1376.94, 3010.44–3575.88), but fusion attached that PRESENCE mass to
// narrow lexical/music SEEDS and shipped the seed geometry as the ad. The
// 3493.02 seed opens 12.72 s inside the host's sign-off (3492.36–3498.48),
// covers one whole HAIKS ad, then clips 1.55 s into the following Libsyn ad.
//
// Scope note: this suite proves auto-skip is SAFE (nothing skips on an invented
// edge). It does NOT prove detection is WIDE — recovering the real pod extents
// is playhead-4xqf and is deliberately out of scope, so the width figures in the
// MEASUREMENT section are recorded, never asserted as a bar.

import Foundation
import Testing

@testable import Playhead

@Suite("Unanchored-extent auto-skip gate (playhead-2350)")
struct UnanchoredExtentAutoSkipGateTests {

    // MARK: - THEMOVE Catalyst replay fixture

    /// One replayed window: the persisted geometry and the anchor provenance the
    /// replay actually recorded for it.
    private struct ReplayedWindow {
        let label: String
        let start: Double
        let end: Double
        /// Every replayed edge was `.unanchored` — the seeds were lexical/music,
        /// no rediff slot owned the width and no stinger snap fired.
        let provenance: [AnchorRef]
        /// `skipConfidence` high enough to clear the 0.80 auto-skip promotion —
        /// the correct-show arm's condition, which is what turned these four
        /// into `autoSkipEligible`.
        let skipConfidence: Double
    }

    /// The exact four boundaries the replay produced in all three arms.
    private static let replayedWindows: [ReplayedWindow] = [
        ReplayedWindow(
            label: "intro seed",
            start: 74.58, end: 89.82,
            provenance: [.classifierSeed(regionId: "r-intro", score: 0.82)],
            skipConfidence: 0.86
        ),
        ReplayedWindow(
            label: "midroll seed",
            start: 1241.70, end: 1286.94,
            provenance: [.evidenceCatalog(entry: Self.sponsorEntry(ordinal: 410, time: 1241.70))],
            skipConfidence: 0.91
        ),
        ReplayedWindow(
            label: "midroll tail seed",
            start: 1326.10, end: 1335.90,
            provenance: [.sustainedMusicOffset(regionId: "r-music", confidence: 0.74)],
            skipConfidence: 0.83
        ),
        ReplayedWindow(
            label: "outro seed (opens inside the sign-off)",
            start: 3493.02, end: 3537.95,
            provenance: [.evidenceCatalog(entry: Self.sponsorEntry(ordinal: 1180, time: 3493.02))],
            skipConfidence: 0.95
        )
    ]

    /// The host's show sign-off. A window whose LEADING edge lands in here is
    /// opening on real content, not on an ad.
    private static let signOff = (start: 3492.36, end: 3498.48)

    /// Episode duration observed on the replayed asset (the last FM coarse scan
    /// ran to 3575.88).
    private static let episodeDuration = 3575.88

    private static func sponsorEntry(ordinal: Int, time: Double) -> EvidenceEntry {
        EvidenceEntry(
            evidenceRef: ordinal,
            category: .disclosurePhrase,
            matchedText: "brought to you by",
            normalizedText: "brought to you by",
            atomOrdinal: ordinal,
            startTime: time,
            endTime: time + 2.0
        )
    }

    private static func span(
        id: String,
        start: Double,
        end: Double,
        provenance: [AnchorRef]
    ) -> DecodedSpan {
        DecodedSpan(
            id: id,
            assetId: "asset-themove-catalyst",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 1,
            startTime: start,
            endTime: end,
            anchorProvenance: provenance
        )
    }

    /// The production verdict shape the replay recorded before the gate existed:
    /// presence high, gate `.eligible`.
    private static func presenceEligibleVerdict(skipConfidence: Double) -> DecisionResult {
        DecisionResult(
            proposalConfidence: skipConfidence,
            skipConfidence: skipConfidence,
            eligibilityGate: .eligible
        )
    }

    /// The production auto-skip promotion, verbatim from `runBackfill`'s emission
    /// loop: a `.detectOnly` policy is promoted to `.autoSkipEligible` only when
    /// the gate is `.eligible` AND presence clears the threshold.
    private static func promotedPolicyAction(
        for decision: DecisionResult,
        threshold: Double = 0.80
    ) -> SkipPolicyAction {
        guard decision.eligibilityGate == .eligible,
              decision.skipConfidence.isFinite,
              decision.skipConfidence >= threshold else {
            return .detectOnly
        }
        return .autoSkipEligible
    }

    // MARK: - (1) PRESENCE and EXTENT are separate

    @Test("Presence and extent are separate fields: the gate reads extent, the score never moves")
    func presenceAndExtentAreSeparate() {
        let strongPresence = Self.presenceEligibleVerdict(skipConfidence: 0.95)
        #expect(strongPresence.extentSupport == .unanchored,
                "a verdict that has not derived extent must default to the conservative value")

        let demoted = strongPresence.withExtentSupport(
            .unanchored,
            blockingUnanchoredAutoSkip: true
        )
        // EXTENT changed the gate ...
        #expect(demoted.eligibilityGate == .markOnly,
                "an unanchored extent must demote an eligible verdict to mark-only")
        // ... and left PRESENCE completely alone.
        #expect(demoted.skipConfidence == strongPresence.skipConfidence,
                "presence confidence must survive an extent demotion verbatim")
        #expect(demoted.proposalConfidence == strongPresence.proposalConfidence,
                "proposal confidence must survive an extent demotion verbatim")
        #expect(demoted.promotionTrack == strongPresence.promotionTrack,
                "the promotion track is a presence-side selector and must not move")
        #expect(demoted.extentSupport == .unanchored,
                "the extent must be recorded on the verdict, not just consumed")
    }

    @Test("Extent tiers rank by independence, and a span is worth its weaker edge")
    func extentTiersRankByIndependence() {
        #expect(AutoSkipEdgeAnchor.rediffByteExact.extentTier == .deterministic)
        #expect(AutoSkipEdgeAnchor.stingerSnapped.extentTier == .corroborated)
        #expect(AutoSkipEdgeAnchor.unanchored.extentTier == .none)
        #expect(ExtentAnchorTier.none < ExtentAnchorTier.corroborated)
        #expect(ExtentAnchorTier.corroborated < ExtentAnchorTier.deterministic)

        let mixed = SpanExtentSupport(startAnchor: .rediffByteExact, endAnchor: .unanchored)
        #expect(mixed.tier == .none,
                "a byte-exact start does not rescue an invented end — the span is worth its weaker edge")
        #expect(mixed.isFullyAnchored == false)
        #expect(mixed.unanchoredEdges == ["end"])

        let both = SpanExtentSupport(startAnchor: .stingerSnapped, endAnchor: .rediffByteExact)
        #expect(both.tier == .corroborated)
        #expect(both.isFullyAnchored)
        #expect(both.unanchoredEdges.isEmpty)

        let deterministic = SpanExtentSupport(
            startAnchor: .rediffByteExact,
            endAnchor: .rediffByteExact
        )
        #expect(deterministic.tier == .deterministic)
        #expect(SpanExtentSupport.unanchored.tier == .none)
        #expect(SpanExtentSupport.unanchored.unanchoredEdges == ["start", "end"])
    }

    // MARK: - (2) What counts as an anchored edge

    @Test("Extent derivation: rediff width ownership anchors both edges deterministically")
    func rediffOwnershipAnchorsBothEdges() {
        let support = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlot],
            stingerTrace: nil
        )
        #expect(support == SpanExtentSupport(startAnchor: .rediffByteExact, endAnchor: .rediffByteExact))
        #expect(support.isFullyAnchored)
    }

    @Test("Extent derivation: a stinger snap anchors only the edge it snapped")
    func stingerSnapAnchorsOnlyItsOwnEdge() {
        var startOnly = StingerRefinementTrace()
        startOnly.startSnapped = true
        let startSupport = SpanExtentSupport.derive(anchorProvenance: [], stingerTrace: startOnly)
        #expect(startSupport.startAnchor == .stingerSnapped)
        #expect(startSupport.endAnchor == .unanchored)
        #expect(startSupport.isFullyAnchored == false,
                "one snapped edge is not an anchored span — the other edge is still invented")

        var bothEdges = StingerRefinementTrace()
        bothEdges.startSnapped = true
        bothEdges.endSnapped = true
        #expect(SpanExtentSupport.derive(anchorProvenance: [], stingerTrace: bothEdges).isFullyAnchored)
    }

    @Test("Extent derivation: acoustic splice width and presence-only anchors are NOT edge anchors")
    func nonEdgeAnchorsDoNotCount() {
        // `.spliceSlot` is ACOUSTIC width, not byte-exact — it does not anchor.
        #expect(SpanExtentSupport.derive(anchorProvenance: [.spliceSlot], stingerTrace: nil) == .unanchored)
        // Presence anchors say an ad is HERE, never where it starts or stops.
        let presenceOnly: [AnchorRef] = [
            .fmConsensus(regionId: "r", consensusStrength: 1.0),
            .fmAcousticCorroborated(regionId: "r", breakStrength: 0.9),
            .classifierSeed(regionId: "r", score: 0.99),
            .sustainedMusicOffset(regionId: "r", confidence: 0.99),
            .evidenceCatalog(entry: Self.sponsorEntry(ordinal: 1, time: 10))
        ]
        #expect(SpanExtentSupport.derive(anchorProvenance: presenceOnly, stingerTrace: nil) == .unanchored,
                "strong presence evidence must never be mistaken for boundary evidence — that IS the bug")
    }

    @Test("Extent derivation: a finalizer geometry rewrite invalidates both edge claims")
    func geometryRewriteInvalidatesAnchors() {
        var trace = StingerRefinementTrace()
        trace.startSnapped = true
        trace.endSnapped = true
        let rewritten = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlot],
            stingerTrace: trace,
            geometryWasRewritten: true
        )
        #expect(rewritten == .unanchored,
                "trimmed/merged/split geometry is nobody's observation, however strong the original anchors")
    }

    @Test("deriveFusionEdgeAnchors and SpanExtentSupport.derive stay one definition")
    func edgeAnchorDerivationHasOneDefinition() {
        var trace = StingerRefinementTrace()
        trace.startSnapped = true
        let cases: [(provenance: [AnchorRef], trace: StingerRefinementTrace?, rewritten: Bool)] = [
            ([], nil, false),
            ([.rediffSlot], nil, false),
            ([.spliceSlot], trace, false),
            ([], trace, false),
            ([.rediffSlot], trace, true)
        ]
        for probe in cases {
            let tuple = AdDetectionService.deriveFusionEdgeAnchors(
                anchorProvenance: probe.provenance,
                stingerTrace: probe.trace,
                geometryWasRewritten: probe.rewritten
            )
            let support = SpanExtentSupport.derive(
                anchorProvenance: probe.provenance,
                stingerTrace: probe.trace,
                geometryWasRewritten: probe.rewritten
            )
            #expect(tuple.start == support.startAnchor,
                    "the persisted start tier must equal the tier the gate reads")
            #expect(tuple.end == support.endAnchor,
                    "the persisted end tier must equal the tier the gate reads")
        }
    }

    // MARK: - (3) The gate never promotes

    @Test("The extent gate only ever demotes: anchored spans keep their gate, blocked spans stay blocked")
    func extentGateNeverPromotes() {
        let anchored = SpanExtentSupport(startAnchor: .rediffByteExact, endAnchor: .rediffByteExact)

        let eligible = Self.presenceEligibleVerdict(skipConfidence: 0.95)
            .withExtentSupport(anchored, blockingUnanchoredAutoSkip: true)
        #expect(eligible.eligibilityGate == .eligible,
                "a fully anchored span keeps the gate fusion gave it")

        // Every non-eligible gate must survive BOTH an anchored and an
        // unanchored extent — the gate is a demoter, never a promoter.
        let nonEligible: [SkipEligibilityGate] = [
            .markOnly, .blockedByEvidenceQuorum, .blockedByPolicy,
            .blockedByUserCorrection, .cappedByFMSuppression
        ]
        for gate in nonEligible {
            for support in [anchored, SpanExtentSupport.unanchored] {
                let result = DecisionResult(
                    proposalConfidence: 0.99,
                    skipConfidence: 0.99,
                    eligibilityGate: gate
                ).withExtentSupport(support, blockingUnanchoredAutoSkip: true)
                #expect(result.eligibilityGate == gate,
                        "extent must not move a \(gate.rawValue) verdict (support: \(support))")
            }
        }
    }

    @Test("Gate disabled: extent is still recorded, but no demotion is applied")
    func gateDisabledRecordsExtentWithoutDemoting() {
        let result = Self.presenceEligibleVerdict(skipConfidence: 0.95)
            .withExtentSupport(.unanchored, blockingUnanchoredAutoSkip: false)
        #expect(result.eligibilityGate == .eligible,
                "with the safety gate off the verdict is unchanged — the kill switch really is a kill switch")
        #expect(result.extentSupport == .unanchored,
                "extent must be recorded even when it is not acted on")
    }

    // MARK: - (4) THEMOVE Catalyst regression

    @Test("THEMOVE replay: all four windows become mark-only and none is autoSkipEligible")
    func themoveReplayWindowsAreAllMarkOnly() {
        for window in Self.replayedWindows {
            let replayedSpan = Self.span(
                id: "span-\(window.start)",
                start: window.start,
                end: window.end,
                provenance: window.provenance
            )
            // Extent as the pipeline derives it for this span: no rediff slot,
            // no stinger snap — exactly what the replay persisted.
            let extent = SpanExtentSupport.derive(
                anchorProvenance: replayedSpan.anchorProvenance,
                stingerTrace: nil
            )
            #expect(extent == .unanchored,
                    "\(window.label): the replay persisted both edges unanchored")

            // The pre-2350 verdict: presence-eligible, above the 0.80 promotion
            // threshold — the correct-show arm's condition.
            let before = Self.presenceEligibleVerdict(skipConfidence: window.skipConfidence)
            #expect(Self.promotedPolicyAction(for: before) == .autoSkipEligible,
                    "\(window.label): non-vacuity — this window WAS autoSkipEligible before the gate")

            let after = before.withExtentSupport(extent, blockingUnanchoredAutoSkip: true)
            #expect(after.eligibilityGate == .markOnly,
                    "\(window.label) [\(window.start)–\(window.end)] must be mark-only, not eligible")
            #expect(Self.promotedPolicyAction(for: after) == .detectOnly,
                    "\(window.label) must not reach autoSkipEligible — the promotion requires an eligible gate")
            #expect(after.skipConfidence == window.skipConfidence,
                    "\(window.label): the banner keeps its honest presence confidence")
        }
    }

    @Test("THEMOVE replay: a leading edge overlapping the 3492.36–3498.48 sign-off can never auto-skip")
    func signOffOverlappingLeadingEdgeNeverAutoSkips() {
        // Reason 1 — unconditional, ships ON: the sign-off region has no edge
        // anchor of any kind, so every candidate leading edge inside it is
        // invented and the extent gate demotes it. Swept across the whole
        // sign-off interval so the property is not pinned to one number.
        let leadingEdges = stride(from: Self.signOff.start, through: Self.signOff.end, by: 0.51)
        var swept = 0
        for leadingEdge in leadingEdges {
            swept += 1
            let extent = SpanExtentSupport.derive(
                anchorProvenance: [.evidenceCatalog(entry: Self.sponsorEntry(ordinal: 1180, time: leadingEdge))],
                stingerTrace: nil
            )
            let verdict = Self.presenceEligibleVerdict(skipConfidence: 1.0)
                .withExtentSupport(extent, blockingUnanchoredAutoSkip: true)
            #expect(verdict.eligibilityGate == .markOnly,
                    "a window opening at \(leadingEdge) inside the sign-off must be mark-only")
            #expect(Self.promotedPolicyAction(for: verdict) == .detectOnly,
                    "a window opening at \(leadingEdge) inside the sign-off must never be autoSkipEligible")
        }
        #expect(swept >= 10, "the sweep must actually cover the sign-off interval (covered \(swept) edges)")

        // Reason 2 — independent, and it holds even under the STRONGEST
        // hypothetical anchoring (both edges byte-exact): the span ends
        // 3537.95, i.e. 37.93 s from the episode end, inside the 90 s post-roll
        // guard. Run it through the real `DecisionMapper` so this is the
        // production rule, not a restatement of it.
        let outro = Self.span(
            id: "span-outro-hypothetically-anchored",
            start: 3493.02,
            end: 3537.95,
            provenance: [.rediffSlot]
        )
        let mapped = DecisionMapper(
            span: outro,
            ledger: [
                EvidenceLedgerEntry(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["sponsor"])),
                EvidenceLedgerEntry(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.9))
            ],
            config: FusionWeightConfig(certaintyTieredEnabled: true),
            episodeDuration: Self.episodeDuration
        ).map()
        #expect(mapped.eligibilityGate == .markOnly,
                "even byte-exact edges cannot auto-skip a post-roll span: the wraj post-roll guard demotes it")
        let gated = mapped.withExtentSupport(
            SpanExtentSupport(startAnchor: .rediffByteExact, endAnchor: .rediffByteExact),
            blockingUnanchoredAutoSkip: true
        )
        #expect(gated.eligibilityGate == .markOnly,
                "the extent gate must not promote the post-roll demotion back to eligible")
    }

    // MARK: - (5) MEASUREMENT — recorded, never a gate (bead criterion 4)

    @Test("MEASUREMENT: replayed seed width vs the FM coarse windows that contained them")
    func measureSeedWidthAgainstFMCoarseExtent() {
        // The FM coarse scans that reported `containsAd` over the replayed
        // regions. Recording the ratio quantifies the boundary-collapse this
        // bead deliberately does NOT fix (playhead-4xqf owns widening).
        let coarseWindows: [(start: Double, end: Double)] = [
            (17.04, 1183.62), (49.02, 1376.94), (3010.44, 3575.88)
        ]
        var lines: [String] = []
        for window in Self.replayedWindows {
            let containing = coarseWindows.filter { $0.start <= window.start && $0.end >= window.end }
            let seedWidth = window.end - window.start
            let coarseWidth = containing.map { $0.end - $0.start }.min()
            let ratio = coarseWidth.map { seedWidth / $0 }
            // Structural only — no bar is asserted, by design.
            #expect(seedWidth > 0, "\(window.label): replayed geometry must be well-formed")
            if let ratio {
                #expect(ratio.isFinite && ratio > 0 && ratio <= 1.0,
                        "\(window.label): a contained seed cannot be wider than its container")
            }
            lines.append(
                "\(window.label): seed \(seedWidth)s"
                    + (coarseWidth.map { " / FM coarse \($0)s = \(seedWidth / $0)" } ?? " / no containing FM coarse window")
            )
        }
        // Recorded to the test log, deliberately NOT asserted against a bar:
        // widening these seeds is playhead-4xqf's job, not this bead's.
        print("[2350 MEASUREMENT] seed-vs-FM-coarse width\n" + lines.joined(separator: "\n"))
    }

    // MARK: - (6) End-to-end: the gate fires at the real runBackfill seam

    private static let wireInPodcastId = "podcast-2350"
    private static let wireInEpisodeDuration = 1800.0

    /// Lexical ad fixture (the Squarespace transcript the ncv6 / wraj / xsdz.37
    /// wire-in suites share). Its span is lexical-seeded: no rediff slot, no
    /// stinger snap — so both edges are `.unanchored` and the gate must demote it.
    private func makeAdSignalChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "This is the introduction to the program with our host and guest.",
            "We will be right back. This episode is brought to you by Squarespace. "
                + "Use code SHOW for 10 percent off at squarespace dot com slash show. "
                + "Sign up today and make your website.",
            "Now we continue our discussion about technology and the future."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeSeededStore(assetId: String) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: assetId,
                episodeId: "ep-\(assetId)",
                assetFingerprint: "fp-\(assetId)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(assetId).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )
        return store
    }

    private func makeConfig(blocking: Bool) -> AdDetectionConfig {
        AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            unanchoredExtentBlocksAutoSkip: blocking
        )
    }

    private func runBackfill(
        assetId: String,
        blocking: Bool
    ) async throws -> [AdWindow] {
        let store = try await makeSeededStore(assetId: assetId)
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: makeConfig(blocking: blocking)
        )
        try await service.runBackfill(
            chunks: makeAdSignalChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: Self.wireInPodcastId,
            episodeDuration: Self.wireInEpisodeDuration
        )
        return try await store.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
    }

    @Test("runBackfill: an unanchored fusion span persists as markOnly, and the gate is why")
    func runBackfillDemotesUnanchoredSpans() async throws {
        // Control arm: gate OFF — the same fixture yields an eligible span. This
        // keeps the test arm non-vacuous (it proves the fixture CAN be eligible).
        let control = try await runBackfill(assetId: "asset-2350-off", blocking: false)
        try #require(!control.isEmpty, "fixture must produce a fusion window")
        let controlEligible = control.filter {
            $0.eligibilityGate == SkipEligibilityGate.eligible.rawValue
        }
        try #require(
            !controlEligible.isEmpty,
            "with the gate off the fixture must yield an eligible span — otherwise the test arm proves nothing"
        )
        for window in controlEligible {
            #expect(window.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(window.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        }

        // Test arm: shipped default (gate ON) — no window survives as eligible.
        let gated = try await runBackfill(assetId: "asset-2350-on", blocking: true)
        #expect(gated.count == control.count,
                "the gate must only change the verdict, never add or drop a window: \(gated.count) vs \(control.count)")
        for window in gated {
            #expect(
                window.eligibilityGate != SkipEligibilityGate.eligible.rawValue,
                "window [\(window.startTime)-\(window.endTime)] with anchors \(window.startEdgeAnchor)/\(window.endEdgeAnchor) must not be eligible"
            )
        }
        let demoted = gated.filter {
            $0.eligibilityGate == SkipEligibilityGate.markOnly.rawValue
        }
        #expect(!demoted.isEmpty, "the demoted spans must land on the mark-only (banner) tier")
        // Presence is untouched: the same confidences, just not auto-skippable.
        #expect(
            gated.map(\.confidence) == control.map(\.confidence),
            "the extent gate must not move presence confidence"
        )
    }
}
