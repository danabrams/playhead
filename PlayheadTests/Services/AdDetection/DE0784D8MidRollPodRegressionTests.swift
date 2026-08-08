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
        // Every row in the frozen pull is accounted for: 12 total, 3 of them
        // replayed user corrections (2 marks + 3 confirmed banners = 5 rows
        // are user-sourced; 2 marks carry detectorVersion "userCorrection",
        // 3 banner confirms carry boundaryState "userConfirmedSuggested").
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
            \(Fixture.creative1.lowerBound)-\(Fixture.creative2.upperBound) is expected to be \
            entirely undetected in the frozen device pull. Detection now covers \
            \(coveredSeconds)s of \(podWidth)s — if that is a real fix, update this fixture \
            and comment on playhead-j4wi; if not, a detection window is leaking into the pod.
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
        // `ad_windows.confidence` for fusion windows stores DecisionResult
        // .skipConfidence — the actuation number after the ASSET-WIDE user
        // correction suppression factor — not the detection (proposal)
        // confidence, which was 0.456 for this span (decision_events witness).
        // The UI rendered this as "conf 0.00" while the row itself persisted.
        #expect(fp.confidence < 0.002)
        // The boundary label says "acousticRefined" but the trigger was
        // lexical (causalSource "lexical", atom 2551): buildFusionAdWindow
        // hardcodes this boundaryState for every fusion window.
        #expect(fp.boundaryState == "acousticRefined")
        #expect(fp.eligibilityGate == "blockedByUserCorrection")
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
        #expect(preRoll.overlap(with: 0.0 ... preRollTruthEnd) == preRollTruthEnd)
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
    func ketoneURLAnchorIsDroppedByMinimumDuration() async throws {
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
        // disclosure list). Anchoring categories are url / promoCode /
        // disclosurePhrase / ctaPhrase; brandSpan never anchors.
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
        let loader = CorpusAnnotationLoader()
        let annotations: [CorpusAnnotation]
        do {
            annotations = try loader.loadAll(verifyAudioFingerprints: false)
        } catch CorpusAnnotationLoaderError.directoryNotFound {
            // Lightweight checkouts may omit TestFixtures; the composition
            // test in CorpusAnnotationTests governs presence there.
            return
        }
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
