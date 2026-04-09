// RegionShadowPhase.swift
// playhead-xba (Phase 4 shadow wire-up):
//
// Thin composition helper that runs the Phase 4 region pipeline
// (`RegionProposalBuilder.build` → `RegionFeatureExtractor.extract`) over the
// same inputs the existing backfill path already computes. This helper is
// intentionally pure — no actors, no persistence, no I/O — so it can be
// driven from `AdDetectionService.runBackfill` with a narrow blast radius
// and exercised directly from integration tests without a full runtime.
//
// Why a standalone helper and not inline code in `AdDetectionService`:
//   1. Keeps the live decision path untouched. The backfill function only
//      gains a single call site guarded by the optional observer parameter.
//   2. Makes the wire-up trivially testable: tests can construct the full
//      input set, call `RegionShadowPhase.run`, and assert on the returned
//      bundles without needing an `AdDetectionService`.
//   3. Matches the shape of the other Phase 4 entry points
//      (`RegionProposalBuilder`, `RegionFeatureExtractor`) — both are plain
//      static enums — so there's no new actor surface to reason about.
//
// FM-origin regions are NOT exercised by this helper yet. The Phase 3
// `BackfillJobRunner.runPendingBackfill` builds `FMRefinementWindowOutput`
// values internally but does not surface them through `RunResult`. Pulling
// them out would require either (a) extending `RunResult` with the raw
// window outputs, or (b) reconstructing them from persisted
// `semantic_scan_results` rows. Both are meaningful refactors that belong
// in follow-up work (see playhead-xba for the tracking note). Until then,
// `RegionShadowPhase.run` passes `fmWindows: []`, which exercises the
// lexical / acoustic / sponsor / fingerprint origin paths of
// `RegionProposalBuilder` and the full uniform feature-bundle pipeline of
// `RegionFeatureExtractor`.

import Foundation
import OSLog

enum RegionShadowPhase {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "RegionShadowPhase"
    )

    struct Input: Sendable {
        let analysisAssetId: String
        let chunks: [TranscriptChunk]
        let lexicalCandidates: [LexicalCandidate]
        let featureWindows: [FeatureWindow]
        let episodeDuration: Double
        let priors: ShowPriors
        let podcastProfile: PodcastProfile?

        init(
            analysisAssetId: String,
            chunks: [TranscriptChunk],
            lexicalCandidates: [LexicalCandidate],
            featureWindows: [FeatureWindow],
            episodeDuration: Double,
            priors: ShowPriors,
            podcastProfile: PodcastProfile? = nil
        ) {
            self.analysisAssetId = analysisAssetId
            self.chunks = chunks
            self.lexicalCandidates = lexicalCandidates
            self.featureWindows = featureWindows
            self.episodeDuration = episodeDuration
            self.priors = priors
            self.podcastProfile = podcastProfile
        }
    }

    /// Run the Phase 4 region pipeline end-to-end and return the resulting
    /// feature bundles. Safe to call with empty inputs — returns `[]`.
    ///
    /// This function is the ONLY production call site for
    /// `RegionProposalBuilder.build` and `RegionFeatureExtractor.extract`
    /// as of playhead-xba. Prior to this wire-up, both types were reachable
    /// only from their own unit tests.
    static func run(_ input: Input) -> [RegionFeatureBundle] {
        guard !input.chunks.isEmpty else { return [] }

        // Reuse the same atomization call shape as `runShadowFMPhase`. The
        // normalization/source hash strings are deliberately pinned to the
        // same tokens that `AdDetectionService.runShadowFMPhase` uses so
        // that any future reconciliation between the two shadow phases
        // lines up on identical transcript version hashes.
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: input.chunks,
            analysisAssetId: input.analysisAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        guard !atoms.isEmpty else { return [] }

        // Detect acoustic breaks across the whole episode's feature windows.
        // `AcousticBreakDetector.detectBreaks` is pure computation on value
        // types — no persistence, no side effects.
        let acousticBreaks = AcousticBreakDetector.detectBreaks(in: input.featureWindows)

        // Stub matchers (Phases 8 & 9). Both return `[]` today; including
        // them keeps the call sites live so that when the stores land, the
        // Phase 4 shadow output immediately starts reflecting them.
        let sponsorMatches = SponsorKnowledgeMatcher.match(atoms: atoms)
        let fingerprintMatches = AdCopyFingerprintMatcher.match(atoms: atoms)

        let proposalInput = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: input.lexicalCandidates,
            acousticBreaks: acousticBreaks,
            sponsorMatches: sponsorMatches,
            fingerprintMatches: fingerprintMatches,
            // FM windows are not yet threaded through `RunResult` — see the
            // file header. Until that's plumbed, FM-origin clustering stays
            // unexercised in this shadow path. Non-FM origins still flow.
            fmWindows: []
        )

        let proposals = RegionProposalBuilder.build(proposalInput)
        guard !proposals.isEmpty else {
            logger.debug("Region shadow phase: no proposals for asset \(input.analysisAssetId, privacy: .public)")
            return []
        }

        let featureInput = RegionFeatureExtractor.Input(
            regions: proposals,
            atoms: atoms,
            featureWindows: input.featureWindows,
            episodeDuration: input.episodeDuration,
            priors: input.priors,
            podcastProfile: input.podcastProfile,
            fmTranscriptQualityWindows: []
        )

        let bundles = RegionFeatureExtractor.extract(featureInput)
        logger.info(
            "Region shadow phase: asset=\(input.analysisAssetId, privacy: .public) proposals=\(proposals.count, privacy: .public) bundles=\(bundles.count, privacy: .public)"
        )
        return bundles
    }
}
