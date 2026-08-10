// DE0784D8MidRollPodRegressionTests.swift
// playhead-j4wi: pins the measured mid-roll hole on episode DE0784D8 (Diary
// of a CEO, Ray Dalio) — 114.78 s of back-to-back host-read ads at
// 2838.18-2953.68 that detection produced NOTHING for, next to an 8.04 s
// false positive it DID produce.
//
// These are KNOWN-HOLE assertions. They pin today's broken behaviour
// honestly: when the hole is fixed (a .ai URL pattern, a "our sponsor called"
// disclosure pattern, a floor that keeps a sub-5 s lexical anchor alive, or a
// targeted scan seeded by the seam fragment), the known-hole expectations
// here MUST fail, and whoever fixes it updates this suite and the bead —
// that is the fixture doing its job.
//
// Ground truth and inputs: `DE0784D8MidRollPodFixture` (re-derived from the
// 2026-08-02 device pull) and the canonical L2F annotation
// `doac-2026-07-31-ray-dalio-de0784d8.json`.

import Foundation
import Testing

@testable import Playhead

@Suite("DE0784D8 mid-roll pod regression (playhead-j4wi)")
struct DE0784D8MidRollPodRegressionTests {

    private typealias Fixture = DE0784D8MidRollPodFixture

    // MARK: - The hole, as the device shipped it

    @Test("KNOWN HOLE: the frozen device pull has zero detection-produced seconds inside the 114.78s pod")
    func devicePullPodCoverageIsZero() throws {
        let detectionWindows = Fixture.devicePullAdWindows.filter(\.isDetectionProduced)
        // Every row in the frozen pull is accounted for: 12 total, 5 of them
        // user-sourced (the 2 pod marks carry detectorVersion
        // "userCorrection"; the 3 banner confirms carry boundaryState
        // "userConfirmedSuggested"), leaving 7 detection-produced windows.
        #expect(Fixture.devicePullAdWindows.count == 12)
        #expect(detectionWindows.count == 7)

        // Numerator: seconds of the pod covered by any detection-produced
        // window. Denominator: the pod's width. If detection had ever touched
        // the pod, this would be non-zero — it is zero, the measured hole.
        let coveredSeconds = detectionWindows.reduce(0.0) {
            $0 + $1.overlap(with: Fixture.creative1) + $1.overlap(with: Fixture.creative2)
        }
        let podWidth = Fixture.podWidthSeconds
        #expect(abs(podWidth - 114.78) < 0.001)
        #expect(
            coveredSeconds == 0.0,
            """
            KNOWN HOLE (playhead-j4wi): the mid-roll pod at \
            \(Fixture.creative1.lowerBound)-\(Fixture.creative2.upperBound) was entirely \
            undetected in the frozen device pull. This is a CAPTURE pin — frozen data cannot \
            observe a production fix — so a failure here means the fixture table itself was \
            edited. The live fix-detectors are livePipelineReproducesSeamFPAndPodHole and \
            ketoneURLAnchorIsDroppedByMinimumDuration.
            """
        )
    }

    @Test("The 8.04s false positive was emitted at skip-tier confidence ~0.00 yet persisted, right before the pod")
    func frozenFalsePositiveShape() throws {
        let fp = try #require(Fixture.frozenFalsePositiveWindow)
        // The vetoed span sits 1.74 s before the pod's first creative.
        #expect(fp.startTime == Fixture.acousticFalsePositive.lowerBound)
        #expect(fp.endTime == Fixture.acousticFalsePositive.upperBound)
        #expect(abs((Fixture.creative1.lowerBound - fp.endTime) - 1.74) < 0.001)

        // playhead-ar60 UPDATE. This is a FROZEN-DATA capture of a PRE-V47
        // device row, so no production change can move it — what changed is
        // what the number is allowed to be CALLED. The pin below used to read
        // `#expect(fp.confidence < 0.002)` under a comment explaining that
        // `ad_windows.confidence` "for fusion windows stores skipConfidence".
        // That is no longer a property of the schema; it is a property of THIS
        // ROW, written before the split, and it is now pinned EXACTLY rather
        // than by a loose bound — the loose bound would keep passing if the
        // fixture were edited to any other small number.
        let decision = try #require(Fixture.frozenFalsePositiveDecision)
        #expect(fp.confidence == decision.skipConfidence)
        #expect(fp.confidence == 0.001150758771374174)

        // The quantity the column was standing in for, and the quantity it
        // destroyed. `metadataConfidence` carried `proposalConfidence` at
        // write time and the metadata extractor overwrote it with 0.1, so the
        // detection number was recoverable ONLY from `decision_events` — which
        // is where V47's migration goes to get it.
        #expect(decision.proposalConfidence == 0.45634516843301726)
        #expect(decision.proposalConfidence / decision.skipConfidence > 390,
                "the column was hiding a ~397x difference between the two quantities")

        // What the SAME row reads as after V47: the detection number in
        // `confidence`, the actuation number in `skipConfidence`, and
        // `actuationConfidence` — the value every skip gate reads — UNCHANGED.
        // That last one is the whole reason the migration is safe.
        let migrated = AdWindow(
            id: fp.id,
            analysisAssetId: Fixture.analysisAssetId,
            startTime: fp.startTime,
            endTime: fp.endTime,
            confidence: decision.proposalConfidence,
            skipConfidence: decision.skipConfidence,
            boundaryState: fp.boundaryState,
            decisionState: fp.decisionState,
            detectorVersion: fp.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: fp.startTime,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: fp.eligibilityGate
        )
        #expect(migrated.actuationConfidence == fp.confidence,
                "the split must not change what any actuation reader sees")
        #expect(migrated.confidence != fp.confidence,
                "…while the DETECTION reader stops seeing the actuation number")

        // The boundary label says "acousticRefined" but the trigger was
        // lexical (causalSource "lexical", atom 2551): buildFusionAdWindow
        // hardcodes this boundaryState for every fusion window. Still true —
        // ar60 did not touch it (that is playhead-j4wi's finding 0, unfixed).
        #expect(fp.boundaryState == "acousticRefined")
        #expect(fp.eligibilityGate == "blockedByUserCorrection")
    }

    @Test("KNOWN-SHAPE, superseded: two fusion rows shipped decisionState=confirmed while gated blockedByUserCorrection")
    func frozenConfirmedDespiteUserCorrection() throws {
        // playhead-ar60 mechanism 3, as the device shipped it. `detectOnly`
        // persisted `.confirmed` UNCONDITIONALLY and `rawPolicyAction` is
        // `.detectOnly` for every unpromoted fusion span, so a span whose gate
        // records the user's own "not an ad" came back as a CONFIRMED,
        // banner-bearing row. Two of the five fusion rows on this asset are in
        // exactly that state (the other three reached `.reverted` only because
        // Dan vetoed them a SECOND time).
        let confirmedDespiteVeto = Fixture.devicePullAdWindows.filter {
            $0.id.hasPrefix("fusion-")
                && $0.decisionState == "confirmed"
                && $0.eligibilityGate == "blockedByUserCorrection"
        }
        #expect(confirmedDespiteVeto.count == 2)
        #expect(Set(confirmedDespiteVeto.map(\.id)) == [
            "fusion-7892299324c9e90f",
            "fusion-d4e332f1a5d9221c",
        ])
        // This is a CAPTURE pin — frozen data cannot observe a production fix.
        // The live pin for the fix is
        // `FusionEmissionShapeTests.blockedByUserCorrectionPersistsSuppressed`.
        #expect(
            confirmedDespiteVeto.allSatisfy { $0.confidence < 0.005 },
            "…and every one of them displayed as conf 0.00"
        )
    }

    @Test("Outer rediff slots are excellent: they cover the annotated pre/post rolls with sub-0.1s edge error")
    func outerRediffSlotsAreExcellent() throws {
        let rediff = Fixture.devicePullAdWindows.filter {
            $0.boundaryState == "dayZeroRediffByteExact"
        }
        #expect(rediff.count == 2)
        let preRoll = try #require(rediff.first { $0.startTime == 0.0 })
        let postRoll = try #require(rediff.first { $0.startTime > 5000 })

        // Pre-roll: truth (from Dan's overshoot veto) ends at 60.06; the
        // window overshoots by 0.073 s. The veto span itself is 2.04 s wide
        // because the applied skip ran past the window end, but the WINDOW
        // error is sub-0.1 s.
        let preRollTruthEnd = Fixture.preRollOvershootVeto.lowerBound
        #expect(abs(preRoll.overlap(with: 0.0 ... preRollTruthEnd) - preRollTruthEnd) < 0.001)
        #expect(abs(preRoll.endTime - preRollTruthEnd) < 0.1)

        // Post-roll: runs to end-of-file; the window end exceeds the asset's
        // recorded episode duration by 0.026 s (clamped in the annotation).
        #expect(abs(postRoll.endTime - Fixture.episodeDurationSeconds) < 0.05)
        #expect(postRoll.startTime == 5462.570296660244)
    }

    // MARK: - The hole, reproduced live through the production lexical path

    @Test("KNOWN HOLE, live: catalog -> projector -> decoder over the frozen transcript yields ONLY the 8.04s seam FP and nothing in the pod")
    func livePipelineReproducesSeamFPAndPodHole() async throws {
        let atoms = Fixture.atoms()
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: Fixture.analysisAssetId,
            transcriptVersion: "device-pull-2026-08-02"
        )
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [],
            catalog: catalog,
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )
        let spans = MinimalContiguousSpanDecoder().decode(
            atoms: evidence,
            assetId: Fixture.analysisAssetId
        )

        // Exactly one span decodes from this 141 s of real transcript: the
        // seam chunk the disclosure regex false-fired on. Its geometry is the
        // ASR chunk's width — the same 2828.40-2836.44 the device persisted.
        #expect(spans.count == 1)
        let seamSpan = try #require(spans.first)
        #expect(seamSpan.startTime == Fixture.acousticFalsePositive.lowerBound)
        #expect(seamSpan.endTime == Fixture.acousticFalsePositive.upperBound)

        // The false anchor: "in partnership with" (disclosurePhrase) matched
        // SHOW content — the host talking about AI partnership.
        let seamAnchors = seamSpan.anchorProvenance.compactMap { anchor -> EvidenceEntry? in
            if case .evidenceCatalog(let entry) = anchor { return entry }
            return nil
        }
        #expect(seamAnchors.contains {
            $0.category == .disclosurePhrase && $0.normalizedText == "in partnership with"
        })

        // KNOWN HOLE (playhead-j4wi): no decoded span touches either creative.
        for span in spans {
            let inPod = max(0, min(span.endTime, Fixture.creative2.upperBound)
                - max(span.startTime, Fixture.creative1.lowerBound))
            #expect(
                inPod == 0.0,
                """
                KNOWN HOLE (playhead-j4wi): the lexical decode path is expected to \
                produce NOTHING inside the pod (2838.18-2953.68). A span now overlaps it \
                (\(span.startTime)-\(span.endTime)) — if the lexicon or decoder floor was \
                fixed, update this fixture's expectations and comment on playhead-j4wi.
                """
            )
        }
    }

    @Test("The pod's only true lexical anchor (ketone.com) fires in the catalog but dies at the 5s micro-fragment floor")
    func ketoneURLAnchorIsDroppedByMinimumDuration() throws {
        let atoms = Fixture.atoms()
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: Fixture.analysisAssetId,
            transcriptVersion: "device-pull-2026-08-02"
        )

        // The catalog DOES see creative 2's URL: "go to ketone.com slash" in
        // the 2940.84-2944.44 chunk.
        let urlEntries = catalog.entries.filter { $0.category == .url }
        let ketone = try #require(urlEntries.first { $0.normalizedText == "ketone.com" })
        let ketoneAtom = try #require(atoms.first { $0.atomKey.atomOrdinal == ketone.atomOrdinal })
        #expect(ketoneAtom.startTime == 2940.84)

        // But the anchored atom is 3.60 s wide with unanchored neighbours, so
        // its single-atom span is below DecoderConstants.minDurationSeconds
        // and is dropped in the decoder's DROP step — the pod's only true
        // lexical hit dies as a "micro-fragment" while the seam's FALSE hit
        // survives only because its host chunk happens to be 8.04 s long.
        let ketoneAtomWidth = ketoneAtom.endTime - ketoneAtom.startTime
        #expect(abs(ketoneAtomWidth - 3.6) < 0.001)
        #expect(ketoneAtomWidth < DecoderConstants.minDurationSeconds)

        // Creative 1 has NO anchoring catalog entry at all: "whisperflow.ai"
        // is a .ai domain (the URL patterns cover .com/.co/.org/.io only),
        // the CTA is "head to" (only "head over to" is a pattern), and the
        // disclosure is "because of our sponsor called ..." (not in the
        // disclosure list). This set mirrors the PRIVATE
        // `AtomEvidenceProjector.anchoringCategories` (url / promoCode /
        // disclosurePhrase / ctaPhrase; brandSpan never anchors) — if a new
        // anchoring category is added there, update this copy. A fix arriving
        // through a stale copy still trips `spans.count == 1` in the live
        // decode test, so there is no silent-green path.
        let anchoringCategories: Set<EvidenceCategory> = [
            .url, .promoCode, .disclosurePhrase, .ctaPhrase,
        ]
        let creative1Anchors = catalog.entries.filter { entry in
            anchoringCategories.contains(entry.category)
                && entry.startTime >= Fixture.creative1.lowerBound
                && entry.endTime <= Fixture.creative1.upperBound
        }
        #expect(
            creative1Anchors.isEmpty,
            """
            KNOWN HOLE (playhead-j4wi): creative 1 (WhisperFlow, 2838.18-2897.94) is \
            expected to produce zero anchoring lexical evidence today. It now produces \
            \(creative1Anchors.map(\.normalizedText)) — if the lexicon grew, update this \
            fixture and comment on playhead-j4wi.
            """
        )
    }

    // MARK: - Canonical L2F annotation agreement

    @Test("The canonical L2F annotation exists, is silver, and matches the device-pull ground truth")
    func corpusAnnotationMatchesDevicePull() throws {
        // repoRoot is passed EXPLICITLY: CorpusAnnotationLoader's `filePath:
        // String = #filePath` default expands at the CALL SITE, and its fixed
        // five-component walk-up assumes a caller exactly four directories
        // deep (every prior caller is). This file is three deep, so the
        // default resolves one level ABOVE the repo root (playhead-7iqi).
        let repoRoot = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()  // AdDetection/
            .deletingLastPathComponent()  // Services/
            .deletingLastPathComponent()  // PlayheadTests/
            .deletingLastPathComponent()  // <repo root>
        let loader = CorpusAnnotationLoader(repoRoot: repoRoot)
        // No lightweight-checkout escape hatch: the annotations are committed,
        // and a checkout without them should fail here as loudly as
        // `canonicalManifestAndTiers` does.
        let annotations = try loader.loadAll(verifyAudioFingerprints: false)
        let annotation = try #require(
            annotations.first { $0.episodeId == Fixture.corpusEpisodeId }
        )

        // Silver, never gold: single first-pass reviewer, no second listener.
        #expect(annotation.labelTier == .silver)
        #expect(!annotation.isEligibleForGoldEvaluation)
        #expect(annotation.durationSeconds == Fixture.episodeDurationSeconds)

        // The two pod creatives carry the exact un-retracted mark boundaries.
        let hostReads = annotation.adWindows
            .filter { $0.adType == .hostRead }
            .sorted { $0.startSeconds < $1.startSeconds }
        #expect(hostReads.count == 2)
        #expect(hostReads.first?.startSeconds == Fixture.creative1.lowerBound)
        #expect(hostReads.first?.endSeconds == Fixture.creative1.upperBound)
        #expect(hostReads.last?.startSeconds == Fixture.creative2.lowerBound)
        #expect(hostReads.last?.endSeconds == Fixture.creative2.upperBound)

        // The vetoed false positive lies strictly inside a CONTENT window —
        // the annotation asserts that span must never be skipped.
        let fpRange = Fixture.acousticFalsePositive
        let containingContent = annotation.contentWindows.first {
            $0.startSeconds <= fpRange.lowerBound && $0.endSeconds >= fpRange.upperBound
        }
        #expect(containingContent != nil)

        // Outer slots: pre-roll ends at the veto-derived 60.06; post-roll runs
        // to the (clamped) episode end.
        let outer = annotation.adWindows.filter { $0.adType == .dynamicInsertion }
        #expect(outer.count == 2)
        #expect(outer.contains { $0.startSeconds == 0.0 && $0.endSeconds == 60.06 })
        #expect(outer.contains {
            $0.startSeconds == 5462.570296660244 && $0.endSeconds == Fixture.episodeDurationSeconds
        })
    }
}
