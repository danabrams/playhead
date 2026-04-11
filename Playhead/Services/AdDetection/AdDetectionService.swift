// AdDetectionService.swift
// Composes the detection layers and outputs AdWindows.
//
// Hot path: lexical scan -> acoustic boundary refinement -> classifier
//   Produces skip-ready AdWindows with decisionState = .candidate
//   ahead of the playhead.
//
// Backfill: re-classify on final-pass transcript -> metadata extraction
//   -> prior update -> promote to .confirmed or .suppressed.
//
// Results keyed by analysisAssetId in SQLite. Different audio bytes =
// different AnalysisAsset = fresh analysis (no stale cache).

import Foundation
import OSLog

// MARK: - Detection Configuration

struct AdDetectionConfig: Sendable {
    /// Minimum classifier probability to emit a candidate AdWindow.
    let candidateThreshold: Double
    /// Minimum classifier probability to auto-confirm during backfill.
    let confirmationThreshold: Double
    /// Maximum probability below which a candidate is suppressed.
    let suppressionThreshold: Double
    /// How far ahead of the playhead (seconds) to run hot-path detection.
    let hotPathLookahead: TimeInterval
    /// Detector version tag written to each AdWindow.
    let detectorVersion: String
    /// Phase 3 Foundation Model backfill toggle. Defaults to `.full`:
    /// FM runs, persists results, and contributes to the decision ledger. See
    /// `FMBackfillMode` for the full contract.
    let fmBackfillMode: FMBackfillMode
    /// Upper bound for FM scanning work per backfill run.
    let fmScanBudgetSeconds: TimeInterval
    /// Minimum overlapping FM windows needed to count as consensus.
    let fmConsensusThreshold: Int
    /// Phase 6.5b (playhead-4my.17): skipConfidence threshold above which an otherwise
    /// detectOnly/logOnly eligible span is promoted to autoSkipEligible. Promotion
    /// applies only when eligibilityGate == .eligible and policyAction is not .suppress.
    /// Default 0.75 is conservative; lower as calibration improves.
    let autoSkipConfidenceThreshold: Double

    init(
        candidateThreshold: Double,
        confirmationThreshold: Double,
        suppressionThreshold: Double,
        hotPathLookahead: TimeInterval,
        detectorVersion: String,
        fmBackfillMode: FMBackfillMode = .full,
        fmScanBudgetSeconds: TimeInterval = 300,
        fmConsensusThreshold: Int = 2,
        autoSkipConfidenceThreshold: Double = 0.75
    ) {
        self.candidateThreshold = candidateThreshold
        self.confirmationThreshold = confirmationThreshold
        self.suppressionThreshold = suppressionThreshold
        self.hotPathLookahead = hotPathLookahead
        self.detectorVersion = detectorVersion
        self.fmBackfillMode = fmBackfillMode
        self.fmScanBudgetSeconds = fmScanBudgetSeconds
        self.fmConsensusThreshold = fmConsensusThreshold
        self.autoSkipConfidenceThreshold = autoSkipConfidenceThreshold
    }

    static let `default` = AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: "detection-v1",
        fmBackfillMode: .full,
        fmScanBudgetSeconds: 300,
        fmConsensusThreshold: 2,
        autoSkipConfidenceThreshold: 0.75
    )
}

// MARK: - Decision State

/// Lifecycle of an AdWindow from detection through confirmation.
enum AdDecisionState: String, Sendable {
    /// Initial detection from hot path -- skip-ready but not yet confirmed.
    case candidate
    /// Confirmed by backfill re-classification with full context.
    case confirmed
    /// Suppressed: below threshold after backfill re-classification.
    case suppressed
    /// Skip was applied to the listener (audio was skipped).
    case applied
    /// User tapped "Listen" — skip reverted, plays through the ad.
    case reverted
}

// MARK: - Boundary State

/// How the window boundaries were derived.
enum AdBoundaryState: String, Sendable {
    /// Rough boundaries from lexical scanner only.
    case lexical
    /// Boundaries refined using acoustic feature transitions.
    case acousticRefined
}

// MARK: - AdDetectionProviding

/// Protocol abstraction for ad detection, enabling test stubs.
protocol AdDetectionProviding: Sendable {
    func runHotPath(chunks: [TranscriptChunk], analysisAssetId: String, episodeDuration: Double) async throws -> [AdWindow]

    /// Cycle 4 H5: callers that know the analysis session id at dispatch
    /// time (e.g. `AnalysisCoordinator.finalizeBackfill`) pass it here so
    /// the shadow phase can stamp `needsShadowRetry` on the exact session
    /// without a `fetchLatestSessionForAsset` lookup that races concurrent
    /// reprocessing. Legacy callers that don't track sessions (e.g.
    /// `AnalysisJobRunner`, which operates on analysis_jobs not sessions)
    /// pass `nil` and the marker is skipped on bail — acceptable because
    /// pre-roll warmup does not yet have a user-facing session to retry.
    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String?
    ) async throws
}

extension AdDetectionProviding {
    /// Convenience for callers that don't track session ids. Delegates
    /// to the primary entry point with `sessionId: nil`.
    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double
    ) async throws {
        try await runBackfill(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration,
            sessionId: nil
        )
    }
}

// MARK: - AdDetectionService

/// Composes LexicalScanner (Layer 1), acoustic boundary refinement (Layer 0),
/// ClassifierService (Layer 2), and MetadataExtractor (Layer 3) into a
/// unified detection pipeline with hot-path and backfill flows.
actor AdDetectionService {

    private let logger = Logger(subsystem: "com.playhead", category: "AdDetectionService")

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let classifier: ClassifierService
    private let metadataExtractor: MetadataExtractor
    private let config: AdDetectionConfig
    /// Optional factory that returns a `BackfillJobRunner` for the FM shadow
    /// phase. When `nil`, FM is skipped entirely (equivalent to .off).
    /// Tests inject a deterministic runner; production wiring lives in
    /// `PlayheadRuntime`.
    private let backfillJobRunnerFactory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)?
    /// M-D: predicate the runner consults before doing any shadow-phase work
    /// (atomization, segmentation, catalog build). Returning `false` makes the
    /// phase an immediate no-op on devices that cannot run Foundation Models
    /// — previously we built the entire input graph and then let the runner
    /// tear it down inside the factory closure. Production wiring captures a
    /// reference to `CapabilitiesService.currentSnapshot`; tests default to
    /// `{ true }` so existing fixtures continue to exercise the shadow path.
    private let canUseFoundationModelsProvider: @Sendable () async -> Bool
    /// bd-3bz (Phase 4) / H7 (cycle 2): called from `runShadowFMPhase` when
    /// the shadow guard bails on `canUseFoundationModels == false`, so the
    /// session can be flagged for a later retry.
    ///
    /// H7 fix: the marker now receives an explicit `sessionId` captured at
    /// the START of the shadow phase, before any concurrent reprocessing
    /// can race a fresh session row in for the same asset. The previous
    /// closure shape was `(assetId, podcastId)` and the runtime side did a
    /// `fetchLatestSessionForAsset` lookup at marker time, which under
    /// concurrent reprocessing could mark the wrong (newer) session. Tests
    /// that don't care about FM availability default this to a no-op.
    private let shadowSkipMarker: @Sendable (_ sessionId: String, _ podcastId: String) async -> Void
    /// playhead-xba (Phase 4 shadow wire-up): optional observation-only sink
    /// for `RegionProposalBuilder` + `RegionFeatureExtractor` output. When
    /// `nil`, the Phase 4 shadow phase inside `runBackfill` is a no-op — no
    /// atomization, no region building, no feature extraction. Production
    /// release builds construct this service with `nil`, mirroring the
    /// DEBUG-only `FoundationModelsFeedbackStore` pattern on
    /// `PlayheadRuntime`. Tests inject a live observer and assert that the
    /// pipeline produced bundles.
    private let regionShadowObserver: RegionShadowObserver?

    /// playhead-4my.5 (Phase 5): optional observer for the AtomEvidenceProjector
    /// + MinimalContiguousSpanDecoder pipeline. When nil, step 11 is a no-op.
    /// Production release builds never inject this (DEBUG-only pattern, same as
    /// `regionShadowObserver`). Tests inject a live observer to assert Phase 5
    /// output without affecting live AdWindow or skip-cue decisions.
    private let phase5ProjectorObserver: Phase5ProjectorObserver?

    /// Phase 6.5 (playhead-4my.16): optional skip orchestrator. When non-nil, eligible
    /// fusion decisions are forwarded after each backfill run, enabling Phase 7
    /// (UserCorrections) to have banner impressions to correct against.
    /// Production wiring lives in PlayheadRuntime. Tests inject a real orchestrator
    /// to assert that results flow through; nil suppresses the forwarding call.
    private let skipOrchestrator: SkipOrchestrator?

    /// Phase 7.2: optional correction store. When non-nil, `runBackfill` pre-computes
    /// a per-span correction factor by querying the store's weighted corrections for
    /// the asset. The factor is passed to `DecisionMapper` so correction-suppressed
    /// spans gate to `.blockedByUserCorrection` without making the struct async.
    private(set) var correctionStore: (any UserCorrectionStore)?

    // MARK: - Cached State

    /// Scanner is recreated per-episode when profile changes.
    private var scanner: LexicalScanner
    /// Per-show priors parsed from the current PodcastProfile.
    private var showPriors: ShowPriors
    /// playhead-8n1: cache the current PodcastProfile so the Phase 4
    /// shadow phase can thread it into `RegionFeatureExtractor`, which
    /// in turn constructs a `LexicalScanner` with per-show sponsor
    /// patterns. Kept in sync with `scanner`/`showPriors` in init,
    /// `updateProfile`, and `updatePriorsFromObservation`.
    private var currentPodcastProfile: PodcastProfile?
    /// Episode duration for position-based scoring.
    private var episodeDuration: Double = 0

    // MARK: - Init

    init(
        store: AnalysisStore,
        classifier: ClassifierService = RuleBasedClassifier(),
        metadataExtractor: MetadataExtractor,
        config: AdDetectionConfig = .default,
        podcastProfile: PodcastProfile? = nil,
        backfillJobRunnerFactory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)? = nil,
        canUseFoundationModelsProvider: @escaping @Sendable () async -> Bool = { true },
        shadowSkipMarker: @escaping @Sendable (_ sessionId: String, _ podcastId: String) async -> Void = { _, _ in },
        regionShadowObserver: RegionShadowObserver? = nil,
        phase5ProjectorObserver: Phase5ProjectorObserver? = nil,
        skipOrchestrator: SkipOrchestrator? = nil
    ) {
        self.store = store
        self.classifier = classifier
        self.metadataExtractor = metadataExtractor
        self.config = config
        self.scanner = LexicalScanner(podcastProfile: podcastProfile)
        self.showPriors = ShowPriors.from(profile: podcastProfile)
        self.currentPodcastProfile = podcastProfile
        self.backfillJobRunnerFactory = backfillJobRunnerFactory
        self.canUseFoundationModelsProvider = canUseFoundationModelsProvider
        self.shadowSkipMarker = shadowSkipMarker
        self.regionShadowObserver = regionShadowObserver
        self.phase5ProjectorObserver = phase5ProjectorObserver
        self.skipOrchestrator = skipOrchestrator
    }

    #if DEBUG
    /// Cycle 8 M-5 call-site rail: DEBUG accessor that returns the factory
    /// closure the service was constructed with, so a test can invoke the
    /// very closure defined on `PlayheadRuntime.swift:214` and inspect the
    /// runner it produces. This is the "real call-site rail" the cycle-7
    /// reviewer asked for: a regression that swaps the live redactor for
    /// `.noop` inside the closure body fails the test at the construction
    /// site, not at some parallel factory.
    func backfillJobRunnerFactoryForTesting() -> (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)? {
        backfillJobRunnerFactory
    }
    #endif

    // MARK: - Phase 7.2: Correction Store Injection

    /// Set the user correction store. Called from PlayheadRuntime after init
    /// (actor property writes must be asynchronous from an init context).
    func setUserCorrectionStore(_ store: any UserCorrectionStore) {
        self.correctionStore = store
    }

    // MARK: - Profile Update

    /// Update the scanner and priors when the podcast profile changes.
    func updateProfile(_ profile: PodcastProfile?) {
        scanner = LexicalScanner(podcastProfile: profile)
        showPriors = ShowPriors.from(profile: profile)
        currentPodcastProfile = profile
    }

    // MARK: - Hot Path

    /// Run the hot-path detection pipeline on fast-pass transcript chunks
    /// and feature windows. Produces candidate AdWindows ahead of the playhead.
    ///
    /// Flow:
    ///   1. LexicalScanner -> candidate regions from transcript
    ///   2. Fetch overlapping FeatureWindows from SQLite
    ///   3. ClassifierService -> scored results with boundary refinement
    ///   4. Filter by candidateThreshold and persist as AdWindows
    ///   5. Return new AdWindows for SkipOrchestrator
    ///
    /// - Parameters:
    ///   - chunks: Fast-pass TranscriptChunks from TranscriptEngineService.
    ///   - analysisAssetId: The analysis asset being processed.
    ///   - episodeDuration: Total episode duration in seconds.
    /// - Returns: Newly detected AdWindows with decisionState = .candidate.
    func runHotPath(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double
    ) async throws -> [AdWindow] {
        self.episodeDuration = episodeDuration
        guard !chunks.isEmpty else { return [] }

        // Layer 1: Lexical scan for candidate regions.
        let lexicalCandidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: analysisAssetId
        )

        guard !lexicalCandidates.isEmpty else {
            logger.info("Hot path: no lexical candidates from \(chunks.count) chunks")
            return []
        }

        logger.info("Hot path: \(lexicalCandidates.count) lexical candidates from \(chunks.count) chunks")

        // Layer 0 + Layer 2: Fetch features, classify, refine boundaries.
        let classifierResults = try await classifyCandidates(
            lexicalCandidates,
            analysisAssetId: analysisAssetId
        )

        // Filter by candidate threshold and build AdWindows.
        let adWindows = classifierResults
            .filter { $0.adProbability >= config.candidateThreshold }
            .map { result in
                buildAdWindow(
                    from: result,
                    boundaryState: .acousticRefined,
                    decisionState: .candidate,
                    evidenceText: lexicalCandidates
                        .first { $0.id == result.candidateId }?.evidenceText
                )
            }

        guard !adWindows.isEmpty else {
            logger.info("Hot path: all \(classifierResults.count) results below threshold")
            return []
        }

        // Persist to SQLite.
        try await store.insertAdWindows(adWindows)

        logger.info("Hot path: persisted \(adWindows.count) candidate AdWindows")

        return adWindows
    }

    // MARK: - Backfill

    /// Run the backfill pipeline: full Phase 1–16 fusion pipeline.
    /// BackfillEvidenceFusion + DecisionMapper are the sole decision authority.
    /// The old promote/suppress path (resolveDecision) is removed.
    ///
    /// Pipeline:
    ///   1.  TranscriptAtomizer
    ///   2.  TranscriptSegmenter + QualityEstimator
    ///   3.  CueHarvesters + EvidenceCatalogBuilder
    ///   4.  RuleBasedClassifier → .classifier ledger entries
    ///   5.  CoveragePlanner
    ///   6.  FM scanning (FMBackfillMode-gated)
    ///   7.  CommercialEvidenceResolver
    ///   8.  RegionProposalBuilder
    ///   9.  RegionFeatureExtractor
    ///   10. AtomEvidenceProjector
    ///   11. MinimalContiguousSpanDecoder
    ///   12. BackfillEvidenceFusion + DecisionMapper
    ///   13. BoundaryRefiner
    ///   14. SkipPolicyMatrix + confidence promotion (Phase 6.5: detectOnly for unknown spans; autoSkipEligible at >=0.75)
    ///   15. MetadataExtractor
    ///   16. EvidenceEvent + DecisionEvent logging
    ///   17. Forward eligible results to SkipOrchestrator (Phase 6.5)
    ///
    /// - Parameters:
    ///   - chunks: Final-pass TranscriptChunks (full episode).
    ///   - analysisAssetId: The analysis asset being processed.
    ///   - podcastId: Podcast ID for profile prior updates.
    ///   - episodeDuration: Total episode duration in seconds.
    ///   - sessionId: Optional analysis session id for shadow retry tracking.
    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String? = nil
    ) async throws {
        self.episodeDuration = episodeDuration
        guard !chunks.isEmpty else { return }

        // ── Steps 1–3: Atomize, segment, build catalog ───────────────────────

        let finalChunks: [TranscriptChunk] = {
            let filtered = chunks.filter { $0.pass == "final" }
            return filtered.isEmpty ? chunks : filtered
        }()

        let (atoms, transcriptVersion) = TranscriptAtomizer.atomize(
            chunks: finalChunks,
            analysisAssetId: analysisAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )

        let evidenceCatalog: EvidenceCatalog
        if !atoms.isEmpty {
            evidenceCatalog = EvidenceCatalogBuilder.build(
                atoms: atoms,
                analysisAssetId: analysisAssetId,
                transcriptVersion: transcriptVersion.transcriptVersion
            )
        } else {
            evidenceCatalog = EvidenceCatalog(
                analysisAssetId: analysisAssetId,
                transcriptVersion: "",
                entries: []
            )
        }

        // ── Step 4: Lexical scan + RuleBasedClassifier ───────────────────────

        let lexicalCandidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: analysisAssetId
        )

        logger.info("Backfill: \(lexicalCandidates.count) lexical candidates from \(chunks.count) final chunks")

        let classifierResults: [ClassifierResult]
        if !lexicalCandidates.isEmpty {
            classifierResults = try await classifyCandidates(
                lexicalCandidates,
                analysisAssetId: analysisAssetId
            )
        } else {
            classifierResults = []
        }

        // ── Steps 5–6: CoveragePlanner + FM scanning ─────────────────────────
        // FM scanning: persists SemanticScanResults for downstream ledger
        // construction. Gated by fmBackfillMode; failures are swallowed so
        // they never block the fusion path.

        var fmRefinementWindows: [FMRefinementWindowOutput] = []
        if config.fmBackfillMode != .off {
            if podcastId.isEmpty {
                logger.info("Backfill: skipping FM scan phase — missing podcastId for asset \(analysisAssetId)")
            } else {
                let shadowResult = await runShadowFMPhase(
                    chunks: chunks,
                    analysisAssetId: analysisAssetId,
                    podcastId: podcastId,
                    sessionIdOverride: sessionId
                )
                fmRefinementWindows = shadowResult.fmRefinementWindows
            }
        }

        // ── Steps 7–9: Region proposal + feature extraction ──────────────────
        // Runs inline (production, not shadow-only) to produce RegionFeatureBundles.
        // Also feeds the optional regionShadowObserver for diagnostics.

        let featureWindows: [FeatureWindow]
        do {
            featureWindows = episodeDuration > 0
                ? try await store.fetchFeatureWindows(
                    assetId: analysisAssetId,
                    from: 0,
                    to: episodeDuration
                )
                : []
        } catch {
            logger.warning("Backfill: fetchFeatureWindows failed (continuing without acoustic features): \(error.localizedDescription)")
            featureWindows = []
        }

        let regionInput = RegionShadowPhase.Input(
            analysisAssetId: analysisAssetId,
            chunks: chunks,
            lexicalCandidates: lexicalCandidates,
            featureWindows: featureWindows,
            episodeDuration: episodeDuration,
            priors: showPriors,
            podcastProfile: currentPodcastProfile,
            fmWindows: fmRefinementWindows
        )
        let regionBundles = RegionShadowPhase.run(regionInput)

        // Also feed the shadow observer for diagnostics (no-op when nil).
        if let observer = regionShadowObserver, episodeDuration > 0 {
            await observer.record(assetId: analysisAssetId, bundles: regionBundles)
        }

        // ── Steps 10–11: AtomEvidenceProjector + MinimalContiguousSpanDecoder ─

        let projector = AtomEvidenceProjector()
        let atomEvidence = await projector.project(
            regions: regionBundles,
            catalog: evidenceCatalog,
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        let decoder = MinimalContiguousSpanDecoder()
        let decodedSpans = decoder.decode(atoms: atomEvidence, assetId: analysisAssetId)

        // Persist decoded spans so TranscriptPeekView can read them.
        if !decodedSpans.isEmpty {
            do {
                try await store.upsertDecodedSpans(decodedSpans)
            } catch {
                logger.warning("Backfill: failed to persist decoded spans: \(error.localizedDescription)")
            }
        }

        // Also feed the Phase 5 shadow observer for diagnostics (no-op when nil).
        if let p5observer = phase5ProjectorObserver, !decodedSpans.isEmpty {
            await p5observer.record(assetId: analysisAssetId, spans: decodedSpans, evidence: atomEvidence)
        }

        logger.info(
            "Backfill: asset=\(analysisAssetId) atoms=\(atoms.count) anchored=\(atomEvidence.filter(\.isAnchored).count) spans=\(decodedSpans.count)"
        )

        // ── Steps 12–14: Fusion + DecisionMapper + SkipPolicyMatrix ──────────

        // Fetch any persisted FM scan results for this asset to build FM ledger entries.
        let semanticScanResults: [SemanticScanResult]
        do {
            semanticScanResults = try await store.fetchSemanticScanResults(
                analysisAssetId: analysisAssetId
            )
        } catch {
            logger.warning("Backfill: fetchSemanticScanResults failed (no FM evidence): \(error.localizedDescription)")
            semanticScanResults = []
        }

        let fusionConfig = FusionWeightConfig()
        // transcriptQuality is the same for every span (derived from the full atom array),
        // so compute it once outside the loop rather than redundantly per span.
        let transcriptQuality = estimateTranscriptQuality(atoms: atomEvidence)
        var fusionWindows: [AdWindow] = []
        var decisionEvents: [DecisionEvent] = []
        // Phase 6.5 (playhead-4my.16): accumulate AdDecisionResult for step 17 forwarding.
        var fusionDecisionResults: [AdDecisionResult] = []

        // Phase 7.2: pre-compute correction factor for this asset (actor-context query).
        // Combines passthrough (false-positive suppression, [0.0, 1.0]) and boost
        // (false-negative amplification, [1.0, 2.0]) into a single multiplier.
        // Result: 1.0 = no correction effect; < 1.0 = FP suppression; > 1.0 = FN boost.
        // Queried once per backfill run (not per span) for performance.
        let assetCorrectionFactor: Double
        if let correctionStore {
            let passthrough = await correctionStore.correctionPassthroughFactor(for: analysisAssetId)
            let boost = await correctionStore.correctionBoostFactor(for: analysisAssetId)
            assetCorrectionFactor = passthrough * boost
        } else {
            assetCorrectionFactor = 1.0
        }

        for span in decodedSpans {
            try Task.checkCancellation()

            // Step 13 (moved before fusion): snap span boundaries to acoustic transitions
            // so that the evidence lookup and gate decision use the final refined boundaries.
            let (startAdj, endAdj) = featureWindows.isEmpty ? (0.0, 0.0) :
                BoundaryRefiner.computeAdjustments(
                    windows: featureWindows,
                    candidateStart: span.startTime,
                    candidateEnd: span.endTime
                )
            let refinedSpan = DecodedSpan(
                id: span.id,
                assetId: span.assetId,
                firstAtomOrdinal: span.firstAtomOrdinal,
                lastAtomOrdinal: span.lastAtomOrdinal,
                startTime: span.startTime + startAdj,
                endTime: span.endTime + endAdj,
                anchorProvenance: span.anchorProvenance
            )

            let ledger = buildEvidenceLedger(
                span: refinedSpan,
                classifierResults: classifierResults,
                lexicalCandidates: lexicalCandidates,
                featureWindows: featureWindows,
                catalogEntries: evidenceCatalog.entries,
                semanticScanResults: semanticScanResults,
                fusionConfig: fusionConfig
            )

            let mapper = DecisionMapper(
                span: refinedSpan,
                ledger: ledger,
                config: fusionConfig,
                transcriptQuality: transcriptQuality,
                correctionFactor: assetCorrectionFactor
            )
            let decision = mapper.map()

            // Step 14: SkipPolicyMatrix + confidence promotion.
            // Phase 6.5 (playhead-4my.16): (.unknown, .unknown) → .detectOnly so Phase 7
            // has banner impressions to correct against.
            let rawPolicyAction = SkipPolicyMatrix.action(for: .unknown, ownership: .unknown)

            // Phase 6.5b (playhead-4my.17): confidence-gated autoSkipEligible promotion.
            // Eligible spans with skipConfidence >= threshold are promoted from
            // detectOnly/logOnly → autoSkipEligible. .suppress is never overridden.
            // Gate-blocked spans are excluded by the eligibilityGate check.
            let autoSkipThreshold = config.autoSkipConfidenceThreshold
            let policyAction: SkipPolicyAction
            if (rawPolicyAction == .detectOnly || rawPolicyAction == .logOnly),
               decision.eligibilityGate == .eligible,
               decision.skipConfidence.isFinite,
               decision.skipConfidence >= autoSkipThreshold {
                policyAction = .autoSkipEligible
                logger.debug(
                    "Backfill: span \(refinedSpan.id, privacy: .public) promoted detectOnly→autoSkipEligible (skipConfidence=\(decision.skipConfidence, format: .fixed(precision: 2)) >= \(autoSkipThreshold, format: .fixed(precision: 2)))"
                )
            } else {
                policyAction = rawPolicyAction
            }

            // Build AdWindow from fusion decision (uses already-refined span boundaries).
            let window = buildFusionAdWindow(
                span: refinedSpan,
                decision: decision,
                policyAction: policyAction,
                analysisAssetId: analysisAssetId
            )
            fusionWindows.append(window)

            // Accumulate AdDecisionResult for step 17 (orchestrator forwarding).
            // SkipEligibilityGate has more cases than AdDecisionEligibilityGate; collapse
            // all non-eligible variants to .blocked — receiveAdDecisionResults guards on this.
            let orchestratorGate: AdDecisionEligibilityGate =
                decision.eligibilityGate == .eligible ? .eligible : .blocked
            fusionDecisionResults.append(AdDecisionResult(
                id: window.id,
                analysisAssetId: analysisAssetId,
                startTime: refinedSpan.startTime,
                endTime: refinedSpan.endTime,
                skipConfidence: decision.skipConfidence,
                eligibilityGate: orchestratorGate,
                recomputationRevision: 0
            ))

            // Accumulate DecisionEvent for step 16.
            let decisionCohort = DecisionCohort.production(appBuild: config.detectorVersion)
            let cohortJSON = (try? JSONEncoder().encode(decisionCohort))
                .flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
            decisionEvents.append(DecisionEvent(
                id: UUID().uuidString,
                analysisAssetId: analysisAssetId,
                eventType: "backfill_fusion",
                windowId: window.id,
                proposalConfidence: decision.proposalConfidence,
                skipConfidence: decision.skipConfidence,
                eligibilityGate: decision.eligibilityGate.rawValue,
                policyAction: policyAction.rawValue,
                decisionCohortJSON: cohortJSON,
                createdAt: Date().timeIntervalSince1970
            ))
        }

        // Persist fusion windows.
        if !fusionWindows.isEmpty {
            try await store.insertAdWindows(fusionWindows)
            logger.info("Backfill: persisted \(fusionWindows.count) fusion windows")
        }

        // ── Step 15: MetadataExtractor ────────────────────────────────────────
        // Extract metadata for windows visible to the user (confirmed + candidate, not suppressed).
        // This set is also used by updatePriors and the coverage watermark below.
        let nonSuppressedWindows = fusionWindows.filter { $0.decisionState != AdDecisionState.suppressed.rawValue }
        for window in nonSuppressedWindows {
            try Task.checkCancellation()
            await extractAndPersistMetadata(window: window, chunks: chunks)
        }

        // ── Step 16: Event logging ────────────────────────────────────────────

        for event in decisionEvents {
            do {
                try await store.appendDecisionEvent(event)
            } catch {
                logger.warning("Backfill: appendDecisionEvent failed for window \(event.windowId): \(error.localizedDescription)")
            }
        }

        // ── Step 17: Forward eligible decisions to SkipOrchestrator ──────────
        // Phase 6.5 (playhead-4my.16): wires fusion output to the orchestrator so
        // Phase 7 (UserCorrections) has banner impressions + skip cues to correct.
        // The orchestrator guards on activeAssetId and eligibilityGate internally.
        if let orchestrator = skipOrchestrator, !fusionDecisionResults.isEmpty {
            await orchestrator.receiveAdDecisionResults(fusionDecisionResults)
            let eligibleCount = fusionDecisionResults.filter { $0.eligibilityGate == .eligible }.count
            logger.info("Backfill: forwarded \(fusionDecisionResults.count) fusion results (\(eligibleCount) eligible) to SkipOrchestrator")
        }

        // ── Post-pipeline: priors + coverage watermark ────────────────────────

        if podcastId.isEmpty {
            logger.info("Backfill: skipping priors update — missing podcastId for asset \(analysisAssetId)")
        } else {
            try await updatePriors(
                podcastId: podcastId,
                nonSuppressedWindows: nonSuppressedWindows,
                episodeDuration: episodeDuration
            )
        }

        if let maxEnd = nonSuppressedWindows.map(\.endTime).max() {
            try await store.updateConfirmedAdCoverage(
                id: analysisAssetId,
                endTime: maxEnd
            )
        }

        logger.info("Backfill complete: spans=\(decodedSpans.count) fusion_windows=\(fusionWindows.count) decision_events=\(decisionEvents.count)")
    }

    // MARK: - Fusion Evidence Construction (playhead-4my.6.4)

    /// Build an evidence ledger for a single DecodedSpan by gathering contributions
    /// from all available evidence sources. The ledger is consumed by DecisionMapper.
    ///
    /// Evidence sources:
    ///   - classifier: best-matching ClassifierResult for the span's time range
    ///   - fm: SemanticScanResults overlapping the span (positive-only: containsAd)
    ///   - lexical: LexicalCandidates overlapping the span
    ///   - acoustic: FeatureWindows in the span with energy-transition signals
    ///   - catalog: EvidenceCatalog entries overlapping the span
    private func buildEvidenceLedger(
        span: DecodedSpan,
        classifierResults: [ClassifierResult],
        lexicalCandidates: [LexicalCandidate],
        featureWindows: [FeatureWindow],
        catalogEntries: [EvidenceEntry],
        semanticScanResults: [SemanticScanResult],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        // Classifier entry: find the best-matching ClassifierResult for this span.
        let classifierScore = bestClassifierScore(
            for: span,
            results: classifierResults
        )

        // FM entries: positive-only, mode-gated, from persisted scan results.
        let fmEntries = buildFMLedgerEntries(
            span: span,
            scanResults: semanticScanResults,
            mode: config.fmBackfillMode,
            fusionConfig: fusionConfig
        )

        // Lexical entries: from LexicalCandidates overlapping the span.
        let lexicalEntries = buildLexicalLedgerEntries(
            span: span,
            candidates: lexicalCandidates,
            fusionConfig: fusionConfig
        )

        // Acoustic entries: from FeatureWindows in the span range.
        let acousticEntries = buildAcousticLedgerEntries(
            span: span,
            featureWindows: featureWindows,
            fusionConfig: fusionConfig
        )

        // Catalog entries: from EvidenceEntry items overlapping the span.
        let catalogLedgerEntries = buildCatalogLedgerEntries(
            span: span,
            entries: catalogEntries,
            fusionConfig: fusionConfig
        )

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: classifierScore,
            fmEntries: fmEntries,
            lexicalEntries: lexicalEntries,
            acousticEntries: acousticEntries,
            catalogEntries: catalogLedgerEntries,
            mode: config.fmBackfillMode,
            config: fusionConfig
        )
        return fusion.buildLedger()
    }

    /// Find the best-matching ClassifierResult for a DecodedSpan (by time overlap).
    private func bestClassifierScore(
        for span: DecodedSpan,
        results: [ClassifierResult]
    ) -> Double {
        let overlapping = results.filter { result in
            let overlapStart = max(span.startTime, result.startTime)
            let overlapEnd = min(span.endTime, result.endTime)
            return overlapEnd > overlapStart
        }
        // Use the highest adProbability among overlapping results as the classifier score.
        return overlapping.map(\.adProbability).max() ?? 0.0
    }

    /// Build FM ledger entries from SemanticScanResults overlapping the span.
    /// Applies the Positive-Only Rule: only containsAd dispositions contribute.
    private func buildFMLedgerEntries(
        span: DecodedSpan,
        scanResults: [SemanticScanResult],
        mode: FMBackfillMode,
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        guard mode.contributesToExistingCandidateLedger else { return [] }

        return scanResults.compactMap { result in
            // Only positive FM evidence contributes.
            guard result.disposition == .containsAd else { return nil }

            // Check time overlap with span.
            let overlapStart = max(span.startTime, result.windowStartTime)
            let overlapEnd = min(span.endTime, result.windowEndTime)
            guard overlapEnd > overlapStart else { return nil }

            // Map scan result to a certainty band. The coarse scan
            // carries transcript quality; use it as a band proxy.
            // Strong quality → .moderate, degraded → .weak.
            let band: CertaintyBand = result.transcriptQuality == .good ? .moderate : .weak

            // Weight proportional to band.
            let weight: Double
            switch band {
            case .strong: weight = fusionConfig.fmCap
            case .moderate: weight = fusionConfig.fmCap * 0.75
            case .weak: weight = fusionConfig.fmCap * 0.5
            }

            return EvidenceLedgerEntry(
                source: .fm,
                weight: weight,
                detail: .fm(
                    disposition: .containsAd,
                    band: band,
                    cohortPromptLabel: result.scanCohortJSON
                )
            )
        }
    }

    /// Build lexical ledger entries from LexicalCandidates overlapping the span.
    private func buildLexicalLedgerEntries(
        span: DecodedSpan,
        candidates: [LexicalCandidate],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        candidates.compactMap { candidate in
            let overlapStart = max(span.startTime, candidate.startTime)
            let overlapEnd = min(span.endTime, candidate.endTime)
            guard overlapEnd > overlapStart else { return nil }

            let weight = min(candidate.confidence * fusionConfig.lexicalCap, fusionConfig.lexicalCap)
            let categories = candidate.categories.map(\.rawValue)
            return EvidenceLedgerEntry(
                source: .lexical,
                weight: weight,
                detail: .lexical(matchedCategories: categories)
            )
        }
    }

    /// Build acoustic ledger entries from FeatureWindows in the span's time range.
    private func buildAcousticLedgerEntries(
        span: DecodedSpan,
        featureWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        let spanWindows = featureWindows.filter { fw in
            fw.startTime < span.endTime && fw.endTime > span.startTime
        }
        guard !spanWindows.isEmpty else { return [] }

        let breakStrength = RegionScoring.computeRmsDropScore(windows: spanWindows)
        guard breakStrength > 0 else { return [] }

        let weight = min(breakStrength * fusionConfig.acousticCap, fusionConfig.acousticCap)
        return [EvidenceLedgerEntry(
            source: .acoustic,
            weight: weight,
            detail: .acoustic(breakStrength: breakStrength)
        )]
    }

    /// Build catalog ledger entries from EvidenceEntry items overlapping the span.
    private func buildCatalogLedgerEntries(
        span: DecodedSpan,
        entries: [EvidenceEntry],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        let overlapping = entries.filter { entry in
            entry.startTime < span.endTime && entry.endTime > span.startTime
        }
        guard !overlapping.isEmpty else { return [] }

        let weight = min(
            Double(overlapping.count) * 0.05 * fusionConfig.catalogCap,
            fusionConfig.catalogCap
        )
        return [EvidenceLedgerEntry(
            source: .catalog,
            weight: weight,
            detail: .catalog(entryCount: overlapping.count)
        )]
    }

    /// Estimate transcript quality from the projected atom evidence.
    /// `internal` for unit-testing the 30% anchor threshold (see BackfillEvidenceFusionTests).
    func estimateTranscriptQuality(atoms: [AtomEvidence]) -> TranscriptQuality {
        guard !atoms.isEmpty else { return .degraded }
        // Use the proportion of anchored atoms as a quality proxy.
        // If > 30% of atoms are anchored, quality is considered good.
        let anchoredFraction = Double(atoms.filter(\.isAnchored).count) / Double(atoms.count)
        return anchoredFraction > 0.3 ? .good : .degraded
    }

    /// Build an AdWindow from a fusion DecisionResult.
    private func buildFusionAdWindow(
        span: DecodedSpan,
        decision: DecisionResult,
        policyAction: SkipPolicyAction,
        analysisAssetId: String
    ) -> AdWindow {
        // Map fusion policy action + gate to AdDecisionState for persistence.
        // autoSkipEligible: confirmed when gate passes, candidate otherwise.
        // detectOnly/logOnly: always confirmed (banner shown; data preserved for Phase 7).
        // suppress: always suppressed (never shown to user).
        let decisionState: AdDecisionState
        switch policyAction {
        case .autoSkipEligible:
            decisionState = decision.eligibilityGate == .eligible ? .confirmed : .candidate
        case .detectOnly, .logOnly:
            // logOnly and detectOnly: persist but don't auto-skip.
            decisionState = .confirmed
        case .suppress:
            decisionState = .suppressed
        }

        return AdWindow(
            id: UUID().uuidString,
            analysisAssetId: analysisAssetId,
            startTime: span.startTime,
            endTime: span.endTime,
            confidence: decision.skipConfidence,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState.rawValue,
            detectorVersion: config.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: span.startTime,
            metadataSource: "fusion-v1",
            metadataConfidence: decision.proposalConfidence,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    /// Apply BoundaryRefiner to snap an AdWindow's boundaries to acoustic transitions.
    private func applyBoundaryRefinement(
        window: AdWindow,
        featureWindows: [FeatureWindow]
    ) -> AdWindow {
        guard !featureWindows.isEmpty else { return window }

        let (startAdj, endAdj) = BoundaryRefiner.computeAdjustments(
            windows: featureWindows,
            candidateStart: window.startTime,
            candidateEnd: window.endTime
        )

        guard startAdj != 0 || endAdj != 0 else { return window }

        return AdWindow(
            id: window.id,
            analysisAssetId: window.analysisAssetId,
            startTime: window.startTime + startAdj,
            endTime: window.endTime + endAdj,
            confidence: window.confidence,
            boundaryState: window.boundaryState,
            decisionState: window.decisionState,
            detectorVersion: window.detectorVersion,
            advertiser: window.advertiser,
            product: window.product,
            adDescription: window.adDescription,
            evidenceText: window.evidenceText,
            evidenceStartTime: window.evidenceStartTime,
            metadataSource: window.metadataSource,
            metadataConfidence: window.metadataConfidence,
            metadataPromptVersion: window.metadataPromptVersion,
            wasSkipped: window.wasSkipped,
            userDismissedBanner: window.userDismissedBanner
        )
    }

    // MARK: - Region Shadow Phase (playhead-xba)

    /// Runs `RegionShadowPhase.run` and records the resulting bundles in
    /// the injected observer. Any failure fetching feature windows is
    /// logged and the phase is skipped — shadow telemetry must never
    /// affect user-visible behavior.
    private func runRegionShadowPhase(
        observer: RegionShadowObserver,
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double,
        lexicalCandidates: [LexicalCandidate],
        fmWindows: [FMRefinementWindowOutput]
    ) async {
        // Defense-in-depth: `episodeDuration` must be supplied by the caller
        // explicitly. Historically this read `self.episodeDuration`, an
        // instance field mutated by `runBackfill` — a refactor that reordered
        // assignment or added a new entry point could silently invoke shadow
        // with a zero duration, causing a `from: 0, to: 0` fetch that bypasses
        // the Phase 4 break detector entirely. Trip in DEBUG if that happens.
        precondition(
            episodeDuration > 0,
            "runRegionShadowPhase requires a positive episodeDuration; received \(episodeDuration)"
        )

        // Fetch every feature window for the asset. The Phase 4 acoustic
        // break detector expects a contiguous view of the episode; fetching
        // a narrow sub-range would bias the break detector toward false
        // positives at the slice edges.
        let featureWindows: [FeatureWindow]
        do {
            featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: 0,
                to: episodeDuration
            )
        } catch {
            logger.warning("Region shadow phase: fetchFeatureWindows failed (skipping): \(error.localizedDescription)")
            return
        }

        let input = RegionShadowPhase.Input(
            analysisAssetId: analysisAssetId,
            chunks: chunks,
            lexicalCandidates: lexicalCandidates,
            featureWindows: featureWindows,
            episodeDuration: episodeDuration,
            priors: showPriors,
            // playhead-8n1: thread the cached PodcastProfile so the
            // Phase 4 shadow phase's `RegionFeatureExtractor` can
            // construct a `LexicalScanner` that actually consults
            // the per-show sponsor lexicon. Prior to this change we
            // hard-coded `nil`, silently skipping show-specific
            // sponsor patterns in every shadow bundle.
            podcastProfile: currentPodcastProfile,
            fmWindows: fmWindows
        )
        let bundles = RegionShadowPhase.run(input)
        await observer.record(assetId: analysisAssetId, bundles: bundles)
    }

    // MARK: - Phase 5 Projector Phase (playhead-4my.5)

    /// Runs AtomEvidenceProjector + MinimalContiguousSpanDecoder on the Phase 4
    /// bundles and records the resulting decoded spans in the injected observer.
    /// Failures are logged and swallowed — shadow telemetry must never affect
    /// user-visible behavior.
    private func runPhase5ProjectorPhase(
        observer: Phase5ProjectorObserver,
        bundles: [RegionFeatureBundle],
        chunks: [TranscriptChunk],
        analysisAssetId: String
    ) async {
        guard !chunks.isEmpty else { return }

        // Atomize the same transcript the Phase 4 shadow phase used.
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks.filter { $0.pass == "final" }.isEmpty ? chunks : chunks.filter { $0.pass == "final" },
            analysisAssetId: analysisAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        guard !atoms.isEmpty else {
            logger.warning("Phase 5 projector: no atoms produced for asset \(analysisAssetId)")
            return
        }

        // Build the evidence catalog.
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: analysisAssetId,
            transcriptVersion: atoms[0].atomKey.transcriptVersion
        )

        // Project atoms.
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: bundles,
            catalog: catalog,
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        // Decode spans.
        let decoder = MinimalContiguousSpanDecoder()
        let spans = decoder.decode(atoms: evidence, assetId: analysisAssetId)

        // Persist to SQLite so TranscriptPeekView can read them.
        do {
            try await store.upsertDecodedSpans(spans)
        } catch {
            logger.error("Phase 5 projector: failed to persist decoded spans for asset \(analysisAssetId): \(error)")
        }

        // Record results in observer (DEBUG diagnostics).
        await observer.record(assetId: analysisAssetId, spans: spans, evidence: evidence)

        logger.info(
            "Phase 5 projector: asset=\(analysisAssetId) atoms=\(atoms.count) anchored=\(evidence.filter(\.isAnchored).count) spans=\(spans.count)"
        )
    }

    // MARK: - Shadow FM Phase

    private struct ShadowFMPhaseResult: Sendable {
        let outcome: ShadowFMPhaseOutcome
        /// playhead-xba follow-up: the raw refinement windows the runner
        /// emitted for this shadow invocation, threaded through so that
        /// the Phase 4 shadow phase (step 10 of `runBackfill`) can feed
        /// them into `RegionProposalBuilder`'s FM clustering path.
        /// Empty when the phase was skipped, failed, or produced no
        /// windows.
        let fmRefinementWindows: [FMRefinementWindowOutput]

        static let skipped = ShadowFMPhaseResult(outcome: .skipped, fmRefinementWindows: [])
    }

    private enum ShadowFMPhaseOutcome: Sendable {
        case skipped
        case requeued
        case ranNeedsRetry
        case ranSucceeded
        case ranFailed

        var didExecute: Bool {
            self == .ranSucceeded || self == .ranFailed || self == .ranNeedsRetry
        }

        var shouldClearRetryFlag: Bool {
            self == .ranSucceeded
        }
    }

    /// Invokes `BackfillJobRunner` to execute the Foundation Model backfill in
    /// shadow mode. Failures are logged but never propagated, because shadow
    /// mode must never affect cue computation or user-visible behavior. Reads
    /// `config.fmBackfillMode` to decide whether to actually execute.
    private func runShadowFMPhase(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        sessionIdOverride: String? = nil
    ) async -> ShadowFMPhaseResult {
        guard config.fmBackfillMode != .off else { return .skipped }

        guard let factory = backfillJobRunnerFactory else {
            logger.warning("Shadow FM phase skipped: no runner factory injected — FM evidence will be absent. Check PlayheadRuntime wiring.")
            return .skipped
        }
        func wrap(_ outcome: ShadowFMPhaseOutcome, _ windows: [FMRefinementWindowOutput] = []) -> ShadowFMPhaseResult {
            ShadowFMPhaseResult(outcome: outcome, fmRefinementWindows: windows)
        }

        // Cycle 4 H5: `sessionIdOverride` is the only source of truth for
        // the session id. It's captured by the caller at dispatch time:
        //   • `AnalysisCoordinator.finalizeBackfill` threads the session
        //     id it already knows through `runBackfill(sessionId:)` →
        //     `runShadowFMPhase(sessionIdOverride:)`.
        //   • `retryShadowFMPhaseForSession` passes the exact session id
        //     being retried.
        //   • `AnalysisJobRunner` (pre-roll warmup) has no session
        //     concept and passes nil — the marker is then skipped on
        //     bail, which is correct for that path (no user-facing
        //     session to retry).
        //
        // The previous `fetchLatestSessionForAsset` fallback was removed
        // because it raced concurrent reprocessing: session B for asset
        // X could land between the start of the shadow phase and the
        // marker call, and the marker would tag the wrong (newer) row.
        // With the override-only model, the race is unrepresentable.
        let resolvedSessionId: String? = sessionIdOverride

        // M-D: skip the entire shadow phase on devices that can't run
        // Foundation Models. Atomization, segmentation, and catalog builds
        // are not free — there's no point doing the work only to have the
        // runner's admission controller immediately reject it.
        //
        // bd-3bz (Phase 4): this gate used to be one-shot — a transient
        // false (Apple Intelligence still downloading, thermal probe
        // momentarily failing, locale flip) permanently dropped shadow
        // telemetry for the episode. Now we flag the session via
        // `shadowSkipMarker` before returning, and the capability observer
        // in `PlayheadRuntime` drains flagged sessions after FM becomes
        // stably available again (60s debounce). See
        // `retryShadowFMPhaseForSession` for the re-entrant retry path.
        guard await canUseFoundationModelsProvider() else {
            logger.debug("Shadow FM phase skipped: canUseFoundationModels=false (bd-3bz: marking session for retry)")
            if let resolvedSessionId {
                await shadowSkipMarker(resolvedSessionId, podcastId)
            } else {
                logger.debug("Shadow FM phase: no session id resolved, marker skipped")
            }
            return wrap(.requeued)
        }

        let runner = factory(store, config.fmBackfillMode)
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        let segments = TranscriptSegmenter.segment(atoms: atoms)
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: analysisAssetId,
            transcriptVersion: version.transcriptVersion
        )
        // bd-m8k: read the real per-podcast planner state from AnalysisStore
        // instead of hardwiring cold-start values. The legacy hardwire
        // pinned `observedEpisodeCount = 0` and `stableRecall = false`,
        // which made `CoveragePlanner.shouldUseFullCoverage` always true and
        // left the targeted-with-audit branch permanently unreachable.
        // Cycle 2 C4: the field was historically named `stablePrecision`;
        // it is semantically a stable-recall flag.
        //
        // Lazy semantics: a missing row means we have never observed this
        // podcast, so we fall back to the conservative cold-start defaults.
        // The runner's `recordPodcastEpisodeObservation` call site (also
        // bd-m8k) materializes the row, advances observed-episode counters,
        // and persists full-rescan recall samples derived from the shared
        // targeted-window narrowing helper.
        //
        // Failure mode: a fetch error here must NEVER block the shadow
        // pass — the whole point of shadow mode is that it cannot affect
        // user-visible behavior. We log and fall through to the cold-start
        // defaults so the runner still runs against `fullCoverage`.
        let plannerState: PodcastPlannerState?
        do {
            plannerState = try await store.fetchPodcastPlannerState(podcastId: podcastId)
        } catch {
            logger.warning("bd-m8k: planner state fetch failed (defaulting to cold start): \(error.localizedDescription)")
            plannerState = nil
        }
        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: plannerState?.observedEpisodeCount ?? 0,
            // historical: stored as "stablePrecisionFlag"; semantically recall
            stableRecall: plannerState?.stableRecallFlag ?? false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: plannerState?.episodesSinceLastFullRescan ?? 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        // playhead-7q3 (Phase 4): compute acoustic breaks from the episode
        // feature windows and thread them into `TargetedWindowNarrower` via
        // `AssetInputs.acousticBreaks`. The narrower snaps per-anchor
        // window edges to nearby natural audio transitions (Option D).
        //
        // Failure mode: fetching or detecting breaks must NEVER block the
        // shadow phase — shadow mode is observation-only and the narrower
        // falls back cleanly to the legacy fixed-padding behavior on an
        // empty break list. On any error we log and pass `[]`.
        let acousticBreaks: [AcousticBreak]
        if self.episodeDuration > 0 {
            do {
                let featureWindows = try await store.fetchFeatureWindows(
                    assetId: analysisAssetId,
                    from: 0,
                    to: self.episodeDuration
                )
                acousticBreaks = AcousticBreakDetector.detectBreaks(in: featureWindows)
            } catch {
                logger.warning("playhead-7q3: fetchFeatureWindows failed for break snap (falling back to fixed padding): \(error.localizedDescription)")
                acousticBreaks = []
            }
        } else {
            acousticBreaks = []
        }

        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: version.transcriptVersion,
            plannerContext: plannerContext,
            acousticBreaks: acousticBreaks
        )

        do {
            let result = try await runner.runPendingBackfill(for: inputs)
            logger.info("Shadow FM phase: admitted=\(result.admittedJobIds.count) scans=\(result.scanResultIds.count) deferred=\(result.deferredJobIds.count) fmWindows=\(result.fmRefinementWindows.count)")
            if result.deferredJobIds.isEmpty {
                return wrap(.ranSucceeded, result.fmRefinementWindows)
            }
            return wrap(.ranNeedsRetry, result.fmRefinementWindows)
        } catch {
            logger.warning("Shadow FM phase failed (suppressed by invariant): \(error.localizedDescription)")
            return wrap(.ranFailed)
        }
    }

    // MARK: - Shadow FM Retry (bd-3bz Phase 4)

    /// bd-3bz (Phase 4): re-entrant retry of the Foundation Models shadow
    /// phase for a single session that was previously flagged via
    /// `markSessionNeedsShadowRetry` when the FM capability was unavailable.
    ///
    /// This path is intentionally narrow:
    ///   • It re-reads the persisted transcript chunks for the asset and
    ///     re-runs ONLY the shadow phase — transcription and coarse
    ///     detection are left alone (they are far more expensive and did
    ///     not depend on FM availability).
    ///   • It must be re-entrant against a session whose transcription and
    ///     coarse phases already completed: `BackfillJobRunner.jobId`
    ///     already keys on `transcriptVersion` so duplicate FM jobs are
    ///     deduped at the store level, not by accident here.
    ///   • It does not modify `AnalysisCoordinator` state. The session
    ///     stays in whatever state it was in (typically `.complete`).
    ///   • If the FM capability has flipped back to `false` before the
    ///     drain actually runs, the inner guard bails and re-marks the
    ///     session — the retry queue effectively rolls forward to the
    ///     next stable-true window.
    ///   • The session's `needsShadowRetry` flag is cleared ONLY when
    ///     the shadow phase runs to completion under a true capability.
    ///     Failures inside the runner (network, thermal, etc.) leave the
    ///     flag set so the next capability transition retries again.
    ///
    /// Returns `true` if the drain actually executed the shadow phase
    /// (regardless of runner outcome), `false` if the session was missing,
    /// not flagged, lacked chunks, the FM capability guard bailed, or the
    /// shadow phase could not even start (for example, no runner factory).
    @discardableResult
    func retryShadowFMPhaseForSession(sessionId: String) async -> Bool {
        guard let session = try? await store.fetchSession(id: sessionId) else {
            logger.debug("Shadow retry skipped: session \(sessionId) not found")
            return false
        }
        guard session.needsShadowRetry, let podcastId = session.shadowRetryPodcastId else {
            logger.debug("Shadow retry skipped: session \(sessionId) not flagged")
            return false
        }

        let analysisAssetId = session.analysisAssetId
        let chunks: [TranscriptChunk]
        do {
            chunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
        } catch {
            logger.warning("Shadow retry skipped: failed to fetch chunks for \(analysisAssetId): \(error.localizedDescription)")
            return false
        }
        // Only the final-pass chunks drive the FM shadow phase — they carry
        // the stable `transcriptVersion` that `BackfillJobRunner.jobId`
        // consumes for dedupe. Fast-pass chunks are ignored.
        let finalChunks = chunks.filter { $0.pass == TranscriptPassType.final_.rawValue }
        guard !finalChunks.isEmpty else {
            logger.debug("Shadow retry skipped: no final transcript chunks for \(analysisAssetId)")
            return false
        }

        // Re-check the capability inline. If it bailed again in the window
        // between the observer's drain decision and this call, the inner
        // `runShadowFMPhase` guard will re-mark the session; returning
        // early here keeps the telemetry clean ("drain skipped" vs "drain
        // executed + bailed").
        guard await canUseFoundationModelsProvider() else {
            logger.debug("Shadow retry bailed: canUseFoundationModels flipped false before drain")
            // H7: pass the explicit `sessionId` we already have so the
            // marker stamps the same session that was being retried,
            // never a newer concurrent session for the same asset.
            await shadowSkipMarker(sessionId, podcastId)
            return false
        }

        logger.info("Shadow retry: draining session \(sessionId) asset=\(analysisAssetId)")
        // playhead-xba follow-up: the retry path intentionally does NOT
        // feed FM windows into the Phase 4 region shadow phase. That
        // phase ran once when `runBackfill` first completed for this
        // session; re-running it from a shadow-retry drain would
        // double-record Phase 4 bundles for the same asset under
        // different window sets and is outside the retry contract.
        let shadowResult = await runShadowFMPhase(
            chunks: finalChunks,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            sessionIdOverride: sessionId
        )
        let outcome = shadowResult.outcome
        guard outcome.didExecute else {
            return false
        }
        guard outcome.shouldClearRetryFlag else {
            logger.debug("Shadow retry: shadow phase still has outstanding work for \(sessionId), leaving retry flag set")
            return true
        }
        // Clear only if the inner guard didn't re-stamp the session. A
        // race window exists where capability could have flipped mid-run,
        // but `runShadowFMPhase` would have re-marked the session via the
        // skip marker — so we re-read before clearing.
        do {
            if let refreshed = try await store.fetchSession(id: sessionId),
               refreshed.needsShadowRetry,
               refreshed.updatedAt > session.updatedAt {
                logger.debug("Shadow retry: session \(sessionId) re-flagged during drain, leaving flag set")
            } else {
                try await store.clearSessionShadowRetry(id: sessionId)
            }
        } catch {
            logger.warning("Shadow retry: failed to finalize flag for \(sessionId): \(error.localizedDescription)")
        }
        return true
    }

    // MARK: - User Behavior Feedback

    /// Record that the user rewound back into a skipped ad window,
    /// signaling a potential false positive. Updates the podcast profile.
    func recordListenRewind(
        windowId: String,
        podcastId: String
    ) async throws {
        // Revert the window (user tapped "Listen" to play through).
        try await store.updateAdWindowDecision(
            id: windowId,
            decisionState: AdDecisionState.reverted.rawValue
        )

        // Increment false-positive signal on the profile.
        guard let profile = try await store.fetchProfile(podcastId: podcastId) else {
            logger.warning("No profile found for podcast \(podcastId) during listen-rewind recording")
            return
        }
        let updatedProfile = PodcastProfile(
            podcastId: profile.podcastId,
            sponsorLexicon: profile.sponsorLexicon,
            normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
            repeatedCTAFragments: profile.repeatedCTAFragments,
            jingleFingerprints: profile.jingleFingerprints,
            implicitFalsePositiveCount: profile.implicitFalsePositiveCount + 1,
            skipTrustScore: max(0, profile.skipTrustScore - 0.05),
            observationCount: profile.observationCount,
            mode: profile.mode,
            recentFalseSkipSignals: profile.recentFalseSkipSignals + 1
        )
        try await store.upsertProfile(updatedProfile)

        logger.info("Recorded listen-rewind for window \(windowId), podcast \(podcastId)")
    }

    // MARK: - Classification Pipeline

    /// Fetch feature windows for each lexical candidate and run the classifier.
    private func classifyCandidates(
        _ candidates: [LexicalCandidate],
        analysisAssetId: String
    ) async throws -> [ClassifierResult] {
        var inputs: [ClassifierInput] = []

        for candidate in candidates {
            // Layer 0: Fetch acoustic features overlapping this candidate.
            // Extend the search range slightly to allow boundary snapping.
            let margin = 5.0
            let featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: candidate.startTime - margin,
                to: candidate.endTime + margin
            )

            inputs.append(ClassifierInput(
                candidate: candidate,
                featureWindows: featureWindows,
                episodeDuration: episodeDuration
            ))
        }

        // Layer 2: Classify all candidates.
        return classifier.classify(inputs: inputs, priors: showPriors)
    }

    // MARK: - AdWindow Construction (hot path)

    private func buildAdWindow(
        from result: ClassifierResult,
        boundaryState: AdBoundaryState,
        decisionState: AdDecisionState,
        evidenceText: String?
    ) -> AdWindow {
        AdWindow(
            id: UUID().uuidString,
            analysisAssetId: result.analysisAssetId,
            startTime: result.startTime,
            endTime: result.endTime,
            confidence: result.adProbability,
            boundaryState: boundaryState.rawValue,
            decisionState: decisionState.rawValue,
            detectorVersion: config.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: evidenceText,
            evidenceStartTime: result.startTime,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    // MARK: - Metadata Extraction

    /// Extract metadata for a confirmed window and persist to SQLite.
    private func extractAndPersistMetadata(
        window: AdWindow,
        chunks: [TranscriptChunk]
    ) async {
        // Skip if metadata is already current.
        if !MetadataExtractorFactory.needsReExtraction(
            currentPromptVersion: window.metadataPromptVersion,
            currentSource: window.metadataSource
        ) { return }

        // Gather transcript text overlapping this window.
        let overlappingText = chunks
            .filter { $0.startTime < window.endTime && $0.endTime > window.startTime }
            .map(\.text)
            .joined(separator: " ")

        guard !overlappingText.isEmpty else { return }

        do {
            guard let metadata = try await metadataExtractor.extract(
                evidenceText: overlappingText,
                windowStartTime: window.startTime,
                windowEndTime: window.endTime
            ) else { return }

            try await store.updateAdWindowMetadata(
                id: window.id,
                advertiser: metadata.advertiser,
                product: metadata.product,
                evidenceText: metadata.evidenceText,
                metadataSource: metadata.source,
                metadataConfidence: metadata.confidence,
                metadataPromptVersion: metadata.promptVersion
            )
        } catch {
            logger.warning("Metadata extraction failed for window \(window.id): \(error.localizedDescription)")
        }
    }

    // MARK: - Prior Updates

    /// Update PodcastProfile priors from confirmed ad windows.
    /// Learns ad slot positions and sponsor names over time.
    private func updatePriors(
        podcastId: String,
        nonSuppressedWindows: [AdWindow],
        episodeDuration: Double
    ) async throws {
        guard !nonSuppressedWindows.isEmpty, episodeDuration > 0 else { return }

        let existingProfile = try await store.fetchProfile(podcastId: podcastId)

        // Compute normalized ad slot positions from confirmed windows.
        let newSlotPositions = nonSuppressedWindows.map { window in
            let center = (window.startTime + window.endTime) / 2.0
            return center / episodeDuration
        }

        // Merge with existing slot positions (exponential moving average).
        let mergedSlots: [Double]
        if let existing = existingProfile,
           let json = existing.normalizedAdSlotPriors,
           let data = json.data(using: .utf8),
           let existingSlots = try? JSONDecoder().decode([Double].self, from: data) {
            mergedSlots = mergeSlotPositions(
                existing: existingSlots,
                new: newSlotPositions
            )
        } else {
            mergedSlots = newSlotPositions
        }

        let slotsJSON: String?
        if let data = try? JSONEncoder().encode(mergedSlots) {
            slotsJSON = String(data: data, encoding: .utf8)
        } else {
            slotsJSON = nil
        }

        // Collect advertiser names from confirmed windows with metadata.
        let newSponsors = nonSuppressedWindows
            .compactMap(\.advertiser)
            .map { $0.lowercased() }

        let mergedSponsorLexicon: String?
        if let existing = existingProfile?.sponsorLexicon {
            let existingNames = Set(
                existing.components(separatedBy: ",")
                    .map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
                    .filter { !$0.isEmpty }
            )
            let allNames = existingNames.union(newSponsors)
            mergedSponsorLexicon = allNames.sorted().joined(separator: ",")
        } else if !newSponsors.isEmpty {
            mergedSponsorLexicon = Set(newSponsors).sorted().joined(separator: ",")
        } else {
            mergedSponsorLexicon = existingProfile?.sponsorLexicon
        }

        let observationCount = (existingProfile?.observationCount ?? 0) + 1
        // Trust score approaches 1.0 as observations grow, but FP signals reduce it.
        let fpCount = existingProfile?.implicitFalsePositiveCount ?? 0
        let rawTrust = Double(observationCount) / (Double(observationCount) + 5.0)
        let fpPenalty = Double(fpCount) * 0.02
        let trustScore = max(0, min(1.0, rawTrust - fpPenalty))

        let updatedProfile = PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: mergedSponsorLexicon,
            normalizedAdSlotPriors: slotsJSON,
            repeatedCTAFragments: existingProfile?.repeatedCTAFragments,
            jingleFingerprints: existingProfile?.jingleFingerprints,
            implicitFalsePositiveCount: existingProfile?.implicitFalsePositiveCount ?? 0,
            skipTrustScore: trustScore,
            observationCount: observationCount,
            mode: existingProfile?.mode ?? "shadow",
            recentFalseSkipSignals: existingProfile?.recentFalseSkipSignals ?? 0
        )

        try await store.upsertProfile(updatedProfile)

        // Refresh the in-memory priors for subsequent use.
        showPriors = ShowPriors.from(profile: updatedProfile)
        scanner = LexicalScanner(podcastProfile: updatedProfile)
        currentPodcastProfile = updatedProfile

        logger.info("Updated priors for podcast \(podcastId): observations=\(observationCount) trust=\(trustScore, format: .fixed(precision: 2))")
    }

    /// Merge new slot positions with existing ones. Deduplicates slots that
    /// are within 5% of each other (same ad slot across episodes).
    private func mergeSlotPositions(
        existing: [Double],
        new: [Double]
    ) -> [Double] {
        let proximityThreshold = 0.05
        var merged = existing

        for newSlot in new {
            let alreadyExists = merged.contains { abs($0 - newSlot) < proximityThreshold }
            if !alreadyExists {
                merged.append(newSlot)
            } else {
                // Nudge existing toward the new observation (EMA with alpha=0.3).
                merged = merged.map { existing in
                    if abs(existing - newSlot) < proximityThreshold {
                        return existing * 0.7 + newSlot * 0.3
                    }
                    return existing
                }
            }
        }

        return merged.sorted()
    }
}

// MARK: - AdDetectionProviding Conformance

extension AdDetectionService: AdDetectionProviding {}
