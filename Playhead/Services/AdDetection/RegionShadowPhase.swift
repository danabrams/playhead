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
// Compiled in all configurations. The shadow phase only runs when an
// observer is injected; PlayheadRuntime constructs the observer behind
// `#if DEBUG`, so production builds never reach this code.
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
// playhead-xba follow-up: FM-origin regions ARE now exercised by this
// helper when `AdDetectionService.runBackfill` supplies a non-empty
// `fmWindows` array. The Phase 3 `BackfillJobRunner.runPendingBackfill`
// now surfaces the raw `FMRefinementWindowOutput` values it emits via
// `RunResult.fmRefinementWindows`; `AdDetectionService.runBackfill`
// captures them from the Phase 3 shadow pass (step 9) and hands them to
// the Phase 4 shadow phase (step 10) through `RegionShadowPhase.Input`.
// When the caller passes an empty array (e.g. shadow mode disabled, no
// FM runner factory injected, or the runner produced no windows), the
// lexical / acoustic / sponsor / fingerprint origin paths still flow
// through `RegionProposalBuilder` exactly as before.

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
        /// playhead-xba follow-up: raw FM refinement windows emitted by
        /// the Phase 3 shadow pass. Fed directly into
        /// `RegionProposalBuilder.makeFMProposals` so the FM-origin
        /// clustering path is exercised in shadow mode. Pass `[]` when
        /// no FM windows are available (shadow disabled, runner not
        /// wired, or runner produced nothing).
        let fmWindows: [FMRefinementWindowOutput]

        /// Phase 9 (playhead-4my.9.1): optional fingerprint store for
        /// store-backed fingerprint matching. When nil, falls back to the
        /// legacy stub that returns empty results.
        let fingerprintStore: AdCopyFingerprintStore?
        /// Phase 9 (playhead-4my.9.1): podcast identifier for fingerprint
        /// store queries. Required when fingerprintStore is non-nil.
        let podcastId: String?
        /// High-confidence classifier results, threaded through so that
        /// `RegionProposalBuilder.makeClassifierProposals` can seed
        /// classifier-origin proposals. When empty (legacy callers), no
        /// classifier-origin regions are seeded.
        let classifierResults: [ClassifierResult]

        init(
            analysisAssetId: String,
            chunks: [TranscriptChunk],
            lexicalCandidates: [LexicalCandidate],
            featureWindows: [FeatureWindow],
            episodeDuration: Double,
            priors: ShowPriors,
            podcastProfile: PodcastProfile? = nil,
            fmWindows: [FMRefinementWindowOutput] = [],
            fingerprintStore: AdCopyFingerprintStore? = nil,
            podcastId: String? = nil,
            classifierResults: [ClassifierResult] = []
        ) {
            self.analysisAssetId = analysisAssetId
            self.chunks = chunks
            self.lexicalCandidates = lexicalCandidates
            self.featureWindows = featureWindows
            self.episodeDuration = episodeDuration
            self.priors = priors
            self.podcastProfile = podcastProfile
            self.fmWindows = fmWindows
            self.fingerprintStore = fingerprintStore
            self.podcastId = podcastId
            self.classifierResults = classifierResults
        }
    }

    /// Run the Phase 4 region pipeline end-to-end and return the resulting
    /// feature bundles. Safe to call with empty inputs — returns `[]`.
    ///
    /// This function is the ONLY production call site for
    /// `RegionProposalBuilder.build` and `RegionFeatureExtractor.extract`
    /// as of playhead-xba. Prior to this wire-up, both types were reachable
    /// only from their own unit tests.
    static func run(_ input: Input) async throws -> [RegionFeatureBundle] {
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

        // Phase 8 sponsor matcher: stub (returns []) when no store provided.
        let sponsorMatches = SponsorKnowledgeMatcher.match(atoms: atoms)

        // Phase 9 fingerprint matcher: use store-backed overload when both
        // a fingerprintStore and podcastId are provided; otherwise fall back
        // to the legacy stub that returns [].
        let fingerprintMatches: [FingerprintMatch]
        if let fpStore = input.fingerprintStore, let podcastId = input.podcastId {
            fingerprintMatches = try await AdCopyFingerprintMatcher.match(
                atoms: atoms,
                podcastId: podcastId,
                fingerprintStore: fpStore
            )
        } else {
            fingerprintMatches = AdCopyFingerprintMatcher.match(atoms: atoms)
        }

        let proposalInput = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: input.lexicalCandidates,
            acousticBreaks: acousticBreaks,
            sponsorMatches: sponsorMatches,
            fingerprintMatches: fingerprintMatches,
            // playhead-xba follow-up: FM windows are now plumbed through
            // `RunResult.fmRefinementWindows` → `AdDetectionService` →
            // `RegionShadowPhase.Input.fmWindows`. Empty when shadow FM
            // was not run (off mode, no factory, or zero spans).
            fmWindows: input.fmWindows,
            classifierResults: input.classifierResults
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
