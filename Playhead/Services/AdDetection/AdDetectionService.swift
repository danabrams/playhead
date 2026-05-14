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
    /// ef2.6.3: Minimum skipConfidence to show a lightweight gray-band marker
    /// (no auto-skip). Spans in [markOnlyThreshold, autoSkipConfidenceThreshold)
    /// surface a "likely sponsor segment" marker with one-tap user actions.
    let markOnlyThreshold: Double
    /// Phase 6.5b (playhead-4my.17): skipConfidence threshold above which an otherwise
    /// detectOnly/logOnly eligible span is promoted to autoSkipEligible. Promotion
    /// applies only when eligibilityGate == .eligible and policyAction is not .suppress.
    /// ef2.6.3: raised from 0.75 to 0.80 per product-approved band spec.
    let autoSkipConfidenceThreshold: Double

    /// playhead-fqc8: Looser auto-skip threshold for spans on the
    /// `PromotionTrack.classifierSeedQualified` track — i.e. classifier-only
    /// candidates that picked up an `.acoustic`/`breakAlignment` corroborator
    /// AND whose stored classifier score is `>= 0.70`. The `0.30` structural
    /// ceiling on a classifier-only ledger sum makes the standard `0.80`
    /// threshold unreachable for these spans no matter how strong the
    /// classifier signal is. The qualified track gives them a separate
    /// eligibility floor (default `0.50`) that respects the same gate /
    /// policy guards as the standard path. Setting this `>=
    /// autoSkipConfidenceThreshold` makes the qualified track no-op.
    let classifierSeedQualifiedThreshold: Double

    /// playhead-gtt9.11: Segment-level UI-candidate threshold. A segment-
    /// aggregated score at or above this value qualifies as a "possible ad"
    /// marker in the UI; below it the segment is telemetry-only. Distinct
    /// from `candidateThreshold` (which is the per-window classifier floor)
    /// and `markOnlyThreshold` (which is the span-level skipConfidence band
    /// for ef2.6.3 gray markers). Default 0.40 matches
    /// `SegmentAggregator.promotionThreshold` so aggregator promotion and
    /// UI-candidate persistence agree.
    let segmentUICandidateThreshold: Double

    /// playhead-gtt9.11: Segment-level auto-skip threshold. A segment at or
    /// above this value is eligible for auto-skip PROVIDED the safety-signal
    /// conjunction also fires (see `AutoSkipPrecisionGate`). Intentionally
    /// stricter than `segmentUICandidateThreshold` — "possible ad" markers
    /// should appear at lower confidence than actual auto-skips. Default
    /// 0.55 sits midway between the 0.40 aggregator promotion floor and the
    /// 0.60 single-window high-confidence seed. Not calibrated on real data;
    /// gtt9.3 owns calibration.
    let segmentAutoSkipThreshold: Double

    /// playhead-arf8: master kill switch for music-bracket boundary
    /// refinement (BracketDetector + FineBoundaryRefiner graduated from
    /// shadow). Default `true` because the components have shipped under
    /// shadow telemetry; flipping to `false` is the one-line rollback if
    /// dogfooding reveals a regression. When `false`, the backfill loop
    /// uses only the legacy `BoundaryRefiner.computeAdjustments` path.
    let bracketRefinementEnabled: Bool

    /// playhead-arf8: per-show `musicBracketTrust` floor below which
    /// bracket evidence is suppressed. Trust is sampled from the
    /// `MusicBracketTrustStore` Beta posterior; the prior mean is 0.50,
    /// so a floor of 0.40 leaves bracket refinement active for every show
    /// until accumulated outcome history pulls trust below the floor.
    /// Tightening this (e.g. to 0.55) makes the gate more conservative.
    let bracketRefinementMinTrust: Double

    /// playhead-arf8: minimum `BracketEvidence.coarseScore` required for
    /// bracket detection to influence boundary refinement. Below this
    /// threshold the detector reported a candidate but the envelope
    /// signal was too weak to override the legacy snap. Default 0.30
    /// matches `BracketDetector.Config.default.onsetScoreThreshold`.
    let bracketRefinementMinCoarseScore: Double

    /// playhead-arf8: minimum `BoundaryEstimate.confidence` from
    /// `FineBoundaryRefiner` required to apply the fine-grained snap.
    /// When the local search yields a low-confidence cue (no silence,
    /// no energy valley, no spectral discontinuity), the legacy
    /// `BoundaryRefiner` adjustment is used instead. Default 0.20 keeps
    /// the bar low because the bracket trust gate already filtered out
    /// untrustworthy shows.
    let bracketRefinementMinFineConfidence: Double

    /// playhead-kgby: master flag for the transcript-aware boundary cue.
    /// When `true` (the conservative default), `runBackfill` builds
    /// `[TranscriptBoundaryHit]` from the final-pass transcript chunks
    /// and threads them into `BoundaryRefiner.computeAdjustments`. The
    /// resolver then runs with `transcriptBoundary` weight 0.20 and
    /// `pauseVAD` weight 0.70 (down from 0.90) so a sentence terminal
    /// near a candidate boundary contributes a soft cue.
    ///
    /// Setting this flag to `false` is the one-line rollback: the
    /// service passes empty transcript hits, the resolver picks the
    /// legacy 90/10 weight schedule, and the snap output is bit-
    /// identical to pre-kgby behaviour.
    let transcriptBoundaryCueEnabled: Bool

    /// playhead-au2v.1.2: Tri-state gate for the chapter-signal feature
    /// (epic playhead-au2v.1). Defaults to `.off` for production safety
    /// — the chapter-generation phase, CoveragePlanner audit-window read,
    /// and FM-prompt chapter-context read are all no-ops until this is
    /// flipped. See `ChapterSignalMode` for the per-mode contract.
    ///
    /// Insertion points (planned, no live consumers yet):
    ///   - `ChapterGenerationPhase` entry guard (playhead-au2v.1.10):
    ///     `guard config.chapterSignalMode.runsChapterGeneration else { return }`
    ///   - CoveragePlanner audit-window read (playhead-au2v.1.14):
    ///     `if config.chapterSignalMode.consumersReadChapterPlan { ... }`
    ///   - FM prompt builders (playhead-au2v.1.16):
    ///     same `consumersReadChapterPlan` predicate.
    let chapterSignalMode: ChapterSignalMode

    /// ef2.6.3: Derive ConfidenceBandThresholds from config fields for band classification.
    /// Requires candidate < markOnly < confirmation < autoSkip (asserted in debug).
    var bandThresholds: ConfidenceBandThresholds {
        ConfidenceBandThresholds(
            candidate: candidateThreshold,
            markOnly: markOnlyThreshold,
            confirm: confirmationThreshold,
            autoSkip: autoSkipConfidenceThreshold
        )
    }

    init(
        candidateThreshold: Double,
        confirmationThreshold: Double,
        suppressionThreshold: Double,
        hotPathLookahead: TimeInterval,
        detectorVersion: String,
        fmBackfillMode: FMBackfillMode = .full,
        fmScanBudgetSeconds: TimeInterval = 300,
        fmConsensusThreshold: Int = 2,
        markOnlyThreshold: Double = 0.60,
        autoSkipConfidenceThreshold: Double = 0.80,
        classifierSeedQualifiedThreshold: Double = 0.50,
        segmentUICandidateThreshold: Double = 0.40,
        segmentAutoSkipThreshold: Double = 0.55,
        bracketRefinementEnabled: Bool = true,
        bracketRefinementMinTrust: Double = 0.40,
        bracketRefinementMinCoarseScore: Double = 0.30,
        bracketRefinementMinFineConfidence: Double = 0.20,
        transcriptBoundaryCueEnabled: Bool = true,
        chapterSignalMode: ChapterSignalMode = .off
    ) {
        self.candidateThreshold = candidateThreshold
        self.confirmationThreshold = confirmationThreshold
        self.suppressionThreshold = suppressionThreshold
        self.hotPathLookahead = hotPathLookahead
        self.detectorVersion = detectorVersion
        self.fmBackfillMode = fmBackfillMode
        self.fmScanBudgetSeconds = fmScanBudgetSeconds
        self.fmConsensusThreshold = fmConsensusThreshold
        self.markOnlyThreshold = markOnlyThreshold
        self.autoSkipConfidenceThreshold = autoSkipConfidenceThreshold
        self.classifierSeedQualifiedThreshold = classifierSeedQualifiedThreshold
        self.segmentUICandidateThreshold = segmentUICandidateThreshold
        self.segmentAutoSkipThreshold = segmentAutoSkipThreshold
        self.bracketRefinementEnabled = bracketRefinementEnabled
        self.bracketRefinementMinTrust = bracketRefinementMinTrust
        self.bracketRefinementMinCoarseScore = bracketRefinementMinCoarseScore
        self.bracketRefinementMinFineConfidence = bracketRefinementMinFineConfidence
        self.transcriptBoundaryCueEnabled = transcriptBoundaryCueEnabled
        self.chapterSignalMode = chapterSignalMode
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
        markOnlyThreshold: 0.60,
        autoSkipConfidenceThreshold: 0.80,
        classifierSeedQualifiedThreshold: 0.50,
        segmentUICandidateThreshold: 0.40,
        segmentAutoSkipThreshold: 0.55,
        bracketRefinementEnabled: true,
        bracketRefinementMinTrust: 0.40,
        bracketRefinementMinCoarseScore: 0.30,
        bracketRefinementMinFineConfidence: 0.20,
        transcriptBoundaryCueEnabled: true,
        chapterSignalMode: .off
    )

    /// playhead-fqc8: Pure helper that returns the active auto-skip
    /// threshold for a given `PromotionTrack`. Centralizing the switch
    /// here (rather than inlining at the call site in the service) keeps
    /// the threshold lookup unit-testable without spinning up the actor.
    func effectiveAutoSkipThreshold(for track: PromotionTrack) -> Double {
        switch track {
        case .standard:
            return autoSkipConfidenceThreshold
        case .classifierSeedQualified:
            return classifierSeedQualifiedThreshold
        }
    }
}

// MARK: - Bracket Refinement Telemetry

/// playhead-arf8: per-`runBackfill` aggregate counts for the bracket-
/// refinement gate. Lets tests assert how the live activation distributed
/// spans across the gate paths without scraping logs.
struct BracketRefinementCounts: Sendable, Equatable {
    /// Spans where the bracket-aware refiner actually moved the boundary
    /// (path == .bracketRefined). The legacy refiner did not run for these.
    var bracketRefined: Int = 0
    /// Spans where bracket refinement was active but the detector found
    /// no envelope (host-read ad copy with no music bed).
    var noBracket: Int = 0
    /// Spans where bracket evidence existed but the per-show trust gate
    /// suppressed it. Caller fell back to the legacy refiner.
    var trustGated: Int = 0
    /// Spans where bracket evidence existed but coarse score was below
    /// the floor. Caller fell back to the legacy refiner.
    var coarseGated: Int = 0
    /// Spans where bracket evidence existed but at least one fine
    /// boundary estimate was below the confidence floor. Caller fell
    /// back to the legacy refiner.
    var fineConfidenceGated: Int = 0
    /// Spans where the bracket path was bypassed by configuration
    /// (master flag off, or not enough feature windows).
    var legacyBypass: Int = 0
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
    /// Span came from `SegmentAggregator` fusing multiple sub-threshold
    /// per-window scores into a coherent segment. Its extents are the
    /// aggregator's `[startTime, endTime)`; `gtt9.4.1` boundary expansion
    /// still composes independently on top. playhead-0usd.
    case segmentAggregated
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

    /// playhead-zx6i — B4 fast revalidation entry point. Re-runs only
    /// the classifier + fusion + boundary stages against the persisted
    /// `TranscriptChunk` rows for `analysisAssetId`, without re-running
    /// ASR / decode / feature extraction. Called by `AnalysisJobRunner`
    /// when the `b4_revalidation_from_features_enabled` flag is ON and
    /// the `RevalidationStateStore` reports a pipeline-version bump for
    /// this asset. Throws when persisted chunks cannot be fetched — the
    /// runner falls back to the full-analysis path in that case.
    func revalidateFromFeatures(
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

    /// playhead-zx6i convenience overload for the sessionId-less call
    /// sites (mirrors the `runBackfill` overload above).
    func revalidateFromFeatures(
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double
    ) async throws {
        try await revalidateFromFeatures(
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
    /// `modelVersion` tag written to synthetic replay transcript chunks
    /// produced by `syntheticReplayChunk(...)` and surfaced by the
    /// Diagnostics UI as the detection model identifier. Exposed as a
    /// static constant so the Settings panel reads the same symbol the
    /// producer writes — see playhead-l274 code-review I1.
    static let hotPathReplayModelVersion: String = "hot-path-replay"

    struct HotPathRunResult: Sendable {
        let windows: [AdWindow]
        let retiredWindowIDs: Set<String>
    }

    private struct HotPathHypothesisCandidate: Sendable {
        let candidate: LexicalCandidate
        let evidenceCount: Int
        let hasClosingAnchor: Bool
        let supportingHits: [LexicalHit]
    }

    private struct ReconciledHotPathWindow: Sendable {
        let window: AdWindow
        let matchedExistingID: String?
        let retiredExistingIDs: Set<String>
    }

    private struct ReplaySignalProfile: Sendable {
        let hasSignal: Bool
        let hasDirectionalSignal: Bool
        let backwardReach: TimeInterval
        let forwardReach: TimeInterval

        static let none = ReplaySignalProfile(
            hasSignal: false,
            hasDirectionalSignal: false,
            backwardReach: 0,
            forwardReach: 0
        )
    }

    private static let hotPathCandidateIdentityTolerance: Double = 5
    /// playhead-9ro7: Dogfood false negatives are concentrated in
    /// pre-roll/post-roll slots where Tier 1 often sees exactly one
    /// candidate-strength 30 s window. Keep this recall rule boundary-
    /// scoped and mark-only-gated so mid-episode precision still relies
    /// on SegmentAggregator's N-nearby corroboration.
    private static let boundarySingletonStartWindowSeconds: Double = 120
    private static let boundarySingletonEndWindowSeconds: Double = 180
    private static let boundarySingletonMinimumWindowSeconds: Double = 20

    private let logger = Logger(subsystem: "com.playhead", category: "AdDetectionService")

    /// cycle-1 M2 / cycle-2 M1 / cycle-3 L-1: Static logger for `private
    /// static` helpers that need to log without `self` (callers today:
    /// `decodeAdDurationStats`, `mergedTraitProfileJSON`,
    /// `initialTraitProfileJSON`). These helpers run inside
    /// `store.mutateProfile` closures on the AnalysisStore actor and
    /// must not capture `self`. Same subsystem as the instance logger
    /// so DiagnosticReports group both streams under the
    /// AdDetectionService category.
    private static let staticLogger = Logger(
        subsystem: "com.playhead",
        category: "AdDetectionService"
    )

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

    /// playhead-gtt9.17: optional on-device ad-catalog store. When non-nil,
    /// `runBackfill` queries the store for each decoded span (egress) and
    /// inserts a fingerprint when a span gates to `.autoSkipEligible`
    /// (ingress). `nil` preserves the pre-gtt9.17 behavior exactly — no
    /// catalog evidence in the ledger, no inserts, `lastCatalogMatchSimilarity`
    /// stays at 0. Production wires a real `AdCatalogStore`; tests inject a
    /// temp-dir store or leave nil to exercise the back-compat path.
    private let adCatalogStore: AdCatalogStore?

    /// Phase 7.2: optional correction store. When non-nil, `runBackfill` pre-computes
    /// a per-span correction factor by querying the store's weighted corrections for
    /// the asset. The factor is passed to `DecisionMapper` so correction-suppressed
    /// spans gate to `.blockedByUserCorrection` without making the struct async.
    private(set) var correctionStore: (any UserCorrectionStore)?

    /// playhead-q45f: the TrustScoringService that owns the per-show
    /// trust state machine. Set post-init via `setTrustScoringService(_:)`
    /// so the runtime can wire it without a circular init dependency
    /// (mirrors `setUserCorrectionStore`). Optional because legacy test
    /// factories that don't construct a TrustScoringService still need
    /// `recordListenRewind` to succeed (decision flip + event log row);
    /// in that case the trust mutation simply no-ops. Production wiring
    /// in `PlayheadRuntime` always installs a real service before the
    /// first user tap.
    private(set) var trustScoringService: TrustScoringService?

    /// playhead-z3ch: Provider for feed-description metadata. `runBackfill`
    /// queries it once per asset and synthesizes `.metadata` ledger entries
    /// that are clamped at `metadataCap` (0.15) and gated by the corroboration
    /// check in `BackfillEvidenceFusion`. Production wiring lives in
    /// `PlayheadRuntime` (SwiftData-backed lookup); tests inject a
    /// deterministic stub. Defaults to a `NullEpisodeMetadataProvider` so
    /// existing call sites continue to behave identically. Mutable so the
    /// runtime can install a SwiftData-backed implementation post-init
    /// (mirrors the `setUserCorrectionStore` pattern — the SwiftData
    /// `ModelContext` isn't available until after the runtime + container
    /// are both alive).
    private(set) var episodeMetadataProvider: EpisodeMetadataProvider

    /// playhead-2hpn: Optional per-show music-bed profile store. When
    /// `nil` (default — production wiring may leave unset; tests that
    /// don't exercise the flag pass `nil`), the scoped-music-bed code
    /// path is fully disabled regardless of the
    /// `scopedMusicBedGeneralization` flag. Mutable so the runtime can
    /// install a SwiftData-backed implementation post-init (mirrors the
    /// `setEpisodeMetadataProvider` pattern). When the store is wired
    /// AND the flag is ON, `runBackfill` reads the snapshot once per
    /// episode and writes back the post-episode mutation.
    private(set) var showMusicBedProfileStore: (any ShowMusicBedProfileResolving)?

    /// playhead-2hpn: Cached snapshot of `PreAnalysisConfig` resolved at
    /// init from the persisted user config. Used by `runBackfill` to
    /// decide whether the scoped-music-bed-generalization flag is on
    /// for THIS process. Re-resolving inside the per-span loop would
    /// be wasteful — the flag is process-stable per-launch.
    ///
    /// Rollback latency: flipping the flag via Settings persists to
    /// `UserDefaults` immediately but this cache is only refreshed at
    /// the next `AdDetectionService` construction (i.e. next app
    /// launch). This matches the `xr3t` flag's rollback latency
    /// (consumer caches the value at init and re-reads on next init).
    /// Note: `24cm` — the OTHER `PreAnalysisConfig`-backed flag —
    /// additionally applies live via `DownloadManager
    /// .setUseDualBackgroundSessions(_:)` so its effective rollback is
    /// instant, NOT next-launch; the latency contract here is the
    /// next-init one, identical to `xr3t` (which persists to
    /// `LightweightInventoryChecksSettings`, not `PreAnalysisConfig`,
    /// but shares the same "read at consumer init" pattern). R11
    /// adversarial doc audit fix.
    private let preAnalysisConfig: PreAnalysisConfig = PreAnalysisConfig.load()

    /// playhead-8em9 (narL): Optional decision logger for offline replay.
    /// DEBUG-only; release builds keep the `NoOpDecisionLogger` default so
    /// no log file is ever written on a shipping binary.
    private(set) var decisionLogger: DecisionLoggerProtocol = NoOpDecisionLogger()

    /// playhead-gtt9.26: Versioned profile of fitted Platt-scaling
    /// coefficients applied to the post-fusion classifier score that
    /// drives `AutoSkipPrecisionGate`. Defaults to
    /// `ClassifierCalibrationProfile.production`, which currently ships
    /// empty so production behaviour is byte-identical to pre-gtt9.26
    /// — every lookup returns `.identity` (pass-through) until a fit is
    /// baked in. Tests inject `.empty` to assert the cold-start
    /// contract, or a fit-bearing profile to assert the calibration
    /// math is plumbed end-to-end.
    private let classifierCalibrationProfile: ClassifierCalibrationProfile

    // MARK: - Cached State

    /// Scanner is recreated per-episode when profile changes.
    private var scanner: LexicalScanner
    /// Per-show priors parsed from the current PodcastProfile.
    private var showPriors: ShowPriors
    /// playhead-8n1: cache the current PodcastProfile so the Phase 4
    /// shadow phase can thread it into `RegionFeatureExtractor`, which
    /// in turn constructs a `LexicalScanner` with per-show sponsor
    /// patterns. Kept in sync with `scanner`/`showPriors` in init and
    /// in `updatePriors`. (skeptical-review-cycle-16 M-1: the public
    /// `updateProfile(_:)` setter was removed because it had zero
    /// callers and its post-hoc in-memory write could clobber an
    /// in-flight `updatePriors` if a future caller were added. If the
    /// API needs to come back, gate the post-await assignments in
    /// `updatePriors` on a generation token before re-introducing it.)
    private var currentPodcastProfile: PodcastProfile?
    /// Episode duration for position-based scoring.
    private var episodeDuration: Double = 0

    /// playhead-hygc.1.8 (R7): per-asset in-flight tracker for
    /// `runHotPathResult`. Enforces the no-concurrent-runs-per-asset
    /// invariant that `correctionReplayCandidates` documents (UUID
    /// allocation across actor `await`s could otherwise race and
    /// double-insert replay rows). On entry the assetId is inserted; on
    /// exit (`defer`) it is removed. A second in-flight call for the same
    /// asset hits an `assertionFailure` (DEBUG: trap; RELEASE: no-op) and
    /// also emits a `logger.warning` so RELEASE has at least an
    /// observable footprint. R8: choosing `assertionFailure` over
    /// `preconditionFailure` is deliberate — a single concurrent invocation
    /// in production should NOT crash the player; the legacy
    /// belt-and-suspenders dedupe in `correctionReplayCandidates`
    /// (overlap check against the in-flight `emitted` set + the persisted
    /// `existing` set) keeps the same-call duplicate-row hazard tolerable
    /// even when the assertion is compiled out. Different assets remain
    /// independent. The actor's serialized re-entrance via `await` is the
    /// only realistic source of contention given the production caller
    /// chain (`AnalysisCoordinator.handlePersistedTranscriptChunks` and
    /// `AnalysisJobRunner.run` are both single-shot per asset).
    private var hotPathRunInFlightAssetIds: Set<String> = []

    // playhead-gtt9.16: Last snapshot of the `AcousticFeaturePipeline` funnel.
    // Captured at the end of each `runBackfill` invocation so that tests (and
    // future telemetry surfaces) can inspect which features were computed /
    // produced signal / passed gate / were included in fusion. Initialized to
    // an empty funnel so a service with no backfill runs yet reports zeros
    // rather than surfacing stale data.
    private var lastAcousticFunnel = AcousticFeatureFunnel()
    /// playhead-gtt9.16: Per-window fusion output from the most recent
    /// `AcousticFeaturePipeline.run`. Test-observable so the back-compat
    /// contract (zero signal → zero combined mass) can be asserted without
    /// round-tripping through the full decision pipeline.
    private var lastAcousticPipelineFusion: [AcousticFeatureFusion.WindowFusion] = []

    /// playhead-gtt9.17: Top `CatalogMatch.similarity` observed across all
    /// decoded spans in the most recent `runBackfill` invocation. Reset to
    /// zero at the start of every backfill so stale values from a prior
    /// episode cannot leak into a fresh one. Zero means "either the catalog
    /// was nil/empty, or nothing matched above the default similarity
    /// floor". Test-observable via `lastCatalogMatchSimilarityForTesting`.
    private var lastCatalogMatchSimilarity: Float = 0

    /// playhead-arf8: per-show music-bracket trust store. Lazily-built
    /// actor that wraps the same `AnalysisStore` used by the rest of the
    /// service. `nil` until first lookup so tests / runs that never cross
    /// the bracket-refinement gate don't pay the actor-init cost.
    /// Outcome recording is intentionally absent in this bead — trust
    /// stays at the prior `Beta(5,5)` default, which keeps every show
    /// above the configured floor (0.40) until later work introduces
    /// hit/miss signals.
    private var bracketTrustStore: MusicBracketTrustStore?

    /// playhead-arf8: counters for the most recent `runBackfill` showing
    /// how each decoded span flowed through the bracket-refinement gate.
    /// Test-observable so the activation contract can be asserted at
    /// integration level. Reset at the start of every backfill run.
    private var lastBracketRefinementCounts = BracketRefinementCounts()

    /// playhead-43ed: optional repeated-ad cache. When non-nil and enabled,
    /// `classifyCandidates` derives a 128-bit perceptual fingerprint from
    /// each candidate's feature windows and looks it up against entries
    /// stored for the current podcast. A cache hit synthesizes a
    /// `ClassifierResult` from the cached `(boundaryStart, boundaryEnd,
    /// confidence)` and skips the FM classifier round-trip for that
    /// candidate. Cache misses fall through to the normal classifier.
    /// `nil` preserves pre-43ed behaviour exactly. Production wires a
    /// real `RepeatedAdCacheService`; tests inject deterministic seams.
    private let repeatedAdCache: RepeatedAdCacheService?

    /// playhead-au2v.1.13: optional factory that constructs a
    /// `ChapterGenerationPhase` per-backfill. When `nil`, the chapter-
    /// signal phase is never invoked from `runBackfill` regardless of
    /// `config.chapterSignalMode` — the wire-in is dormant. Production
    /// wiring (a later runtime bead) installs a factory that captures
    /// the live admission policy, boundary detector, labeler, plan
    /// cache, and event sinks. Tests inject deterministic factories
    /// closing over canned mocks.
    ///
    /// The factory shape is `() -> ChapterGenerationPhase` (no
    /// per-backfill arguments) because every dependency the phase
    /// requires is either episode-stable (admission policy, labeler,
    /// cache) or supplied at `phase.run(...)` call time (`mode`,
    /// `episodeId`, `installID`). A factory rather than a single shared
    /// instance is used so the production wiring can rebuild the phase
    /// with a fresh transcript-hash provider per call (the snapshot vs
    /// recheck contract requires the provider to read whatever the
    /// transcript pipeline most recently published).
    private let chapterGenerationPhaseFactory: (@Sendable () -> ChapterGenerationPhase)?

    /// playhead-au2v.1.13: optional cache used for the cache-hit
    /// short-circuit in `runBackfill`. When non-nil and a valid plan is
    /// found for the current content hash, the chapter-generation phase
    /// is skipped entirely (zero FM cost). When `nil`, every run that
    /// would otherwise hit the phase invokes it unconditionally — the
    /// phase still owns its own internal cache write on success, so no
    /// data is lost; the only effect of a missing short-circuit cache is
    /// re-running the phase even if a fresh plan is already on disk.
    /// Tests typically inject the same cache they pass into the
    /// factory's `ChapterGenerationPhase` so the read here and the
    /// phase's write target the same store.
    private let chapterPlanCache: ChapterPlanCache?

    /// playhead-au2v.1.13: producer of the install identifier the
    /// chapter-phase diagnostics need to emit privacy-locked event
    /// payloads. Not stored in `init` parameters list as a keyword arg
    /// because the production runtime needs a stable per-install UUID
    /// (matches the wider diagnostics privacy contract — see
    /// `EpisodeIdHasher`); tests can pass `{ UUID() }` for ergonomic
    /// fixtures. Defaults to a fresh UUID per call so an unwired test
    /// does not crash, but a production wiring bug that omits this
    /// closure surfaces as a per-run UUID churn rather than a stable
    /// hash — one of the integration tests asserts the wiring uses the
    /// injected closure verbatim.
    private let chapterPhaseInstallIDProvider: @Sendable () -> UUID

    /// Cycle 1 H2: rollout gate that intersects `config.fmBackfillMode` with
    /// per-cohort approvals before any FM execution or fusion-time consumption.
    /// `nil` = legacy behavior (the requested mode is used verbatim). When
    /// non-nil, `effectiveFMBackfillMode` consults the registry against the
    /// stored `runtimeCohort` captured at init.
    ///
    /// Value semantics are load-bearing: the struct is captured by value at
    /// init time, so subsequent mutations to a registry held elsewhere do NOT
    /// propagate into this service. If a future refactor wraps the registry
    /// in a class/actor for shared mutation, every service holding a copy
    /// will diverge — and the cohort-rollout contract becomes untestable.
    private let approvedCohortRegistry: ApprovedCohortRegistry?
    /// Cycle 3 H3: the `ScanCohort` captured ONCE at init via the supplied
    /// `scanCohortProvider`. Production uses `ScanCohort.production()` which
    /// reads `Locale.current.identifier` and other process-mutable values —
    /// re-reading on every `effectiveFMBackfillMode` access (the cycle-2
    /// design) was vulnerable to region/calendar locale flips between the
    /// bootstrap-time approval at PlayheadRuntime init and a later runtime
    /// query. iOS does NOT relaunch on region-only changes, so the registry
    /// key would silently miss its approval. Capturing once locks the cohort
    /// to the same value the bootstrap registry was keyed against, and as a
    /// side benefit eliminates the per-access JSON encode in
    /// `CohortKey.canonicalIdentity`.
    private let runtimeCohort: ScanCohort

    // MARK: - Init

    init(
        store: AnalysisStore,
        classifier: ClassifierService = CoreMLSequenceClassifier(),
        metadataExtractor: MetadataExtractor,
        config: AdDetectionConfig = .default,
        podcastProfile: PodcastProfile? = nil,
        backfillJobRunnerFactory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)? = nil,
        canUseFoundationModelsProvider: @escaping @Sendable () async -> Bool = { true },
        shadowSkipMarker: @escaping @Sendable (_ sessionId: String, _ podcastId: String) async -> Void = { _, _ in },
        regionShadowObserver: RegionShadowObserver? = nil,
        phase5ProjectorObserver: Phase5ProjectorObserver? = nil,
        skipOrchestrator: SkipOrchestrator? = nil,
        adCatalogStore: AdCatalogStore? = nil,
        episodeMetadataProvider: EpisodeMetadataProvider = NullEpisodeMetadataProvider(),
        decisionLogger: DecisionLoggerProtocol? = nil,
        classifierCalibrationProfile: ClassifierCalibrationProfile = .production,
        repeatedAdCache: RepeatedAdCacheService? = nil,
        chapterGenerationPhaseFactory: (@Sendable () -> ChapterGenerationPhase)? = nil,
        chapterPlanCache: ChapterPlanCache? = nil,
        chapterPhaseInstallIDProvider: @escaping @Sendable () -> UUID = { UUID() },
        approvedCohortRegistry: ApprovedCohortRegistry? = nil,
        scanCohortProvider: @escaping @Sendable () -> ScanCohort = { ScanCohort.production() }
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
        self.adCatalogStore = adCatalogStore
        self.episodeMetadataProvider = episodeMetadataProvider
        // playhead-8em9 (narL): allow the logger to be installed at init
        // time so there is no race with the first backfill. PlayheadRuntime
        // passes a real DecisionLogger under DEBUG; production and tests
        // that don't care about logging leave this nil, keeping the
        // NoOpDecisionLogger default already on `decisionLogger`.
        if let decisionLogger {
            self.decisionLogger = decisionLogger
        }
        self.classifierCalibrationProfile = classifierCalibrationProfile
        self.repeatedAdCache = repeatedAdCache
        self.chapterGenerationPhaseFactory = chapterGenerationPhaseFactory
        self.chapterPlanCache = chapterPlanCache
        self.chapterPhaseInstallIDProvider = chapterPhaseInstallIDProvider
        self.approvedCohortRegistry = approvedCohortRegistry
        // Cycle 3 H3: capture the cohort exactly once at init. See
        // `runtimeCohort`'s docstring for the rationale (region-only locale
        // flips during process lifetime previously caused silent FM demotion).
        self.runtimeCohort = scanCohortProvider()
    }

    /// Cycle 1 H2 / Cycle 3 H3: effective FM mode after intersecting
    /// `config.fmBackfillMode` with the approved-cohort registry decision
    /// for the cohort captured at init. When no registry is wired, returns
    /// `config.fmBackfillMode` verbatim — preserving legacy behavior for
    /// tests and any caller that hasn't opted into cohort gating.
    private var effectiveFMBackfillMode: FMBackfillMode {
        guard let registry = approvedCohortRegistry else {
            return config.fmBackfillMode
        }
        return registry.effectiveMode(
            osBuild: runtimeCohort.osBuild,
            scanCohort: runtimeCohort,
            requestedMode: config.fmBackfillMode
        )
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

    /// Cycle 2 H1: DEBUG accessor for the cohort-intersected FM mode.
    /// Production callers (four `effectiveFMBackfillMode` reads at the
    /// runBackfill admission gate, the FM ledger-entry builder, the fusion
    /// constructor, and the shadow-phase resolver — the shadow path
    /// captures once into `resolvedMode` and re-uses for the runner factory)
    /// resolve the mode internally; tests use this to assert that an
    /// injected `approvedCohortRegistry` + `scanCohortProvider` combination
    /// resolves to the expected effective mode without standing up the
    /// full backfill pipeline. Without this accessor the cycle-1 H2
    /// wire-up was untested end-to-end (the cycle-2 reviewer's H1 gap).
    func effectiveFMBackfillModeForTesting() -> FMBackfillMode {
        effectiveFMBackfillMode
    }
    #endif

    // MARK: - Phase 7.2: Correction Store Injection

    /// Set the user correction store. Called from PlayheadRuntime after init
    /// (actor property writes must be asynchronous from an init context).
    func setUserCorrectionStore(_ store: any UserCorrectionStore) {
        self.correctionStore = store
    }

    /// playhead-q45f: install the TrustScoringService post-init.
    /// Mirrors `setUserCorrectionStore`. PlayheadRuntime calls this in
    /// a Task after both actors exist; tests exercising the
    /// listen-rewind reroute call it directly on the service they
    /// constructed.
    func setTrustScoringService(_ service: TrustScoringService) {
        self.trustScoringService = service
    }

    #if DEBUG
    /// playhead-8em9 (narL): Test seam for installing a decision logger
    /// post-init. Production wires the logger via `init(decisionLogger:)`
    /// to avoid a Task-install race; this setter exists only for tests that
    /// swap the logger mid-life. DEBUG-only to prevent a future regression
    /// from re-introducing the race in release builds.
    func setDecisionLogger(_ logger: DecisionLoggerProtocol) {
        self.decisionLogger = logger
    }
    #endif

    /// playhead-z3ch: Set the EpisodeMetadataProvider. Called from
    /// `PlayheadApp` (after the SwiftData ModelContainer is available) so
    /// `runBackfill` can pre-seed metadata-derived ledger entries. Mirrors
    /// the `setUserCorrectionStore` pattern.
    func setEpisodeMetadataProvider(_ provider: EpisodeMetadataProvider) {
        self.episodeMetadataProvider = provider
    }

    /// playhead-2hpn: Install the per-show music-bed profile store
    /// post-init. The flag-off path never reads from this store, so
    /// installing it is safe whether or not the feature flag is on.
    /// Mirrors `setEpisodeMetadataProvider`.
    func setShowMusicBedProfileStore(_ store: any ShowMusicBedProfileResolving) {
        self.showMusicBedProfileStore = store
    }

    // MARK: - playhead-gtt9.16: AcousticFeaturePipeline accessors

    /// Test seam: return the funnel snapshot captured during the most recent
    /// `runBackfill` invocation. Production callers read the same state via
    /// log lines emitted at the end of backfill (`logger.info("Backfill
    /// acoustic-pipeline funnel: ...")`), but tests need direct access to
    /// the structured counters.
    func acousticFunnelForTesting() -> AcousticFeatureFunnel {
        lastAcousticFunnel
    }

    /// Test seam: return the per-window fusion output from the most recent
    /// `AcousticFeaturePipeline.run`. Empty until the first backfill
    /// completes.
    func lastAcousticPipelineFusionForTesting() -> [AcousticFeatureFusion.WindowFusion] {
        lastAcousticPipelineFusion
    }

    /// playhead-gtt9.17: Return the top `CatalogMatch.similarity` from the
    /// most recent `runBackfill`. Zero if no catalog was wired, the catalog
    /// was empty, or nothing scored above `AdCatalogStore.defaultSimilarityFloor`.
    /// Used by `AdCatalogWiringTests` to verify that prior entries do lift
    /// similarity and empty/absent catalogs do not.
    func lastCatalogMatchSimilarityForTesting() -> Float {
        lastCatalogMatchSimilarity
    }

    // MARK: - playhead-arf8: Bracket Refinement Telemetry / Trust Lookup

    /// Test seam: returns the per-`runBackfill` aggregate counts emitted by
    /// the bracket-aware refiner gate. Resets to all-zero at the start of
    /// every backfill run so successive calls reflect only the most recent
    /// run. Used by `BracketActivationTests` to assert that the master
    /// flag, trust gate, and confidence gates route spans to the expected
    /// path without log scraping.
    func bracketRefinementCountsForTesting() -> BracketRefinementCounts {
        lastBracketRefinementCounts
    }

    /// playhead-hygc.1.8 (R11): test seam exposing the per-asset in-flight
    /// tracker so the `defer { hotPathRunInFlightAssetIds.remove(...) }`
    /// post-condition documented at `runHotPathResult`'s entry can be
    /// verified directly. Without this seam the cleanup contract is
    /// observable only by triggering the assertionFailure (which crashes
    /// in DEBUG) — leaving the "the defer ran" half of the concurrency
    /// invariant unpinned. R7 added the assertionFailure; R11 pins the
    /// release-side cleanup so a future refactor that drops the `defer`
    /// (e.g. moving the insert/remove inside a child task that aborts
    /// without unwinding the actor frame) fails this single test.
    func hotPathInFlightAssetIdsForTesting() -> Set<String> {
        hotPathRunInFlightAssetIds
    }

    /// Lazy accessor for `MusicBracketTrustStore`. Constructs the actor on
    /// the first request and caches it for the lifetime of the service.
    /// Both the actor itself and its `AnalysisStore` backing are safe to
    /// share across runs, so reuse is the cheapest correct option.
    private func bracketTrustStoreLazy() -> MusicBracketTrustStore {
        if let existing = bracketTrustStore {
            return existing
        }
        let fresh = MusicBracketTrustStore(store: store)
        bracketTrustStore = fresh
        return fresh
    }

    /// Increment the matching counter on `lastBracketRefinementCounts` so
    /// `bracketRefinementCountsForTesting()` and any future log emission
    /// can attribute decoded spans to gate paths. Pure bookkeeping — does
    /// not feed back into `MusicBracketTrustStore`. Outcome accumulation
    /// is intentionally deferred until offline ground-truth signals exist.
    private func tallyBracketRefinementOutcome(_ path: BracketAwareBoundaryRefiner.Path) {
        switch path {
        case .legacy:
            lastBracketRefinementCounts.legacyBypass += 1
        case .noBracket:
            lastBracketRefinementCounts.noBracket += 1
        case .trustGated:
            lastBracketRefinementCounts.trustGated += 1
        case .coarseGated:
            lastBracketRefinementCounts.coarseGated += 1
        case .fineConfidenceGated:
            lastBracketRefinementCounts.fineConfidenceGated += 1
        case .bracketRefined:
            lastBracketRefinementCounts.bracketRefined += 1
        }
    }

    // MARK: - User Correction Persistence

    /// Persist a user-marked ad region as an AdWindow and CorrectionEvent.
    /// Called from PlayheadRuntime when the user reports hearing an ad that
    /// the detector missed (false negative correction).
    func recordUserMarkedAd(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        podcastId: String?
    ) async {
        let windowId = UUID().uuidString
        let adWindow = AdWindow(
            id: windowId,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: startTime,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        )

        do {
            try await store.insertAdWindow(adWindow)
        } catch {
            logger.warning("Failed to persist user-marked ad window: \(error.localizedDescription)")
        }

        // Record a false-negative correction event for the trust/learning pipeline.
        // playhead-zskc: the caller (NowPlayingViewModel.reportHearingAd) has
        // already run BoundaryExpander, so `startTime` and `endTime` represent
        // the real ad boundaries. Persist them as `.exactTimeSpan` rather than
        // collapsing to the coarse `exactSpan:0:Int.max` whole-episode veto.
        //
        // This path does NOT call `recordFalseNegative`: that API is for
        // contexts where we only have a single reported time and must
        // synthesize a ±15s window + AdWindow. Here the AdWindow is already
        // persisted above and the boundaries came from real features.
        if let correctionStore {
            await correctionStore.recordVeto(
                startTime: startTime,
                endTime: endTime,
                assetId: analysisAssetId,
                podcastId: podcastId,
                source: .falseNegative
            )
        }

        // playhead-gtt9.17: catalog ingress on user-confirmed ad. A user
        // marking a span as ad is the strongest possible label — higher
        // confidence than an autoSkipEligible fusion decision. Fingerprint
        // the user-marked span's overlapping feature windows and insert so
        // subsequent episodes of the same show match on this creative.
        //
        // Absent feature windows (e.g. a podcast whose audio never ran
        // through feature extraction) yields an empty slice → zero
        // fingerprint → skipped by `AdCatalogStore.insert` guard. Silent
        // on any SQLite failure; a correction-path catalog miss is
        // strictly a missed precision opportunity, not a correctness bug.
        if let adCatalogStore {
            let featureWindows: [FeatureWindow]
            do {
                featureWindows = try await store.fetchFeatureWindows(
                    assetId: analysisAssetId,
                    from: startTime,
                    to: endTime
                )
            } catch {
                logger.warning("recordUserMarkedAd: fetchFeatureWindows failed (skipping catalog insert): \(error.localizedDescription, privacy: .public)")
                return
            }
            let fingerprint = AcousticFingerprint.fromFeatureWindows(featureWindows)
            guard !fingerprint.isZero else { return }
            do {
                _ = try await adCatalogStore.insert(
                    showId: podcastId,
                    episodePosition: .unknown,
                    durationSec: max(0, endTime - startTime),
                    acousticFingerprint: fingerprint,
                    transcriptSnippet: nil,
                    sponsorTokens: nil,
                    originalConfidence: 1.0
                )
                logger.debug("recordUserMarkedAd: inserted catalog entry for user-marked ad on asset \(analysisAssetId, privacy: .public)")
            } catch {
                logger.warning("recordUserMarkedAd: catalog insert failed: \(error.localizedDescription, privacy: .public)")
            }
        }
    }

    // skeptical-review-cycle-16 M-1: dead `updateProfile(_:)` setter
    // removed (zero callers in production or tests). The post-hoc
    // in-memory write at `currentPodcastProfile = profile` could clobber
    // an in-flight `updatePriors`'s post-await assignment if a future
    // caller were ever added. If the API needs to come back, also gate
    // the post-await assignments in `updatePriors` on a generation
    // token / fingerprint check before re-introducing the public setter.

    /// Rebuild the smallest hot-path replay slice that can still reproduce the
    /// hypothesis engine's transitive context growth for duplicate chunk
    /// re-emits. The closure grows only through chunks that already present a
    /// primary lexical signal or weak-anchor recovery text, then fills in the
    /// intervening transcript for boundary expansion and lexical fallback.
    func hotPathReplayContextChunks(
        from allChunks: [TranscriptChunk],
        around persistedChunks: [TranscriptChunk]
    ) -> [TranscriptChunk] {
        let fastAllChunks = allChunks
            .filter { $0.pass == TranscriptPassType.fast.rawValue }
            .sorted { lhs, rhs in
                if lhs.startTime != rhs.startTime {
                    return lhs.startTime < rhs.startTime
                }
                return lhs.endTime < rhs.endTime
            }
        let seedIDs = Set(
            persistedChunks
                .filter { $0.pass == TranscriptPassType.fast.rawValue }
                .map(\.id)
        )
        let seedChunks = fastAllChunks.filter { seedIDs.contains($0.id) }
        guard !seedChunks.isEmpty else { return [] }

        let padding = SpanHypothesisConfig.default.maximumContextPadding
        let signalProfilesByChunkID: [String: ReplaySignalProfile] = Dictionary(
            uniqueKeysWithValues: fastAllChunks.compactMap { chunk in
                let profile = replaySignalProfile(for: chunk)
                guard profile.hasSignal else { return nil }
                return (chunk.id, profile)
            }
        )
        var relevantChunkIDs = Set(seedChunks.map(\.id))
        var windowStart = seedChunks.map(\.startTime).min() ?? 0
        var windowEnd = seedChunks.map(\.endTime).max() ?? 0
        var currentBackwardReach: TimeInterval = padding
        var currentForwardReach: TimeInterval = padding

        let seedProfiles: [ReplaySignalProfile] = seedChunks.compactMap { signalProfilesByChunkID[$0.id] }
        let seedHasDirectionalSignal = seedProfiles.contains { $0.hasDirectionalSignal }
        if seedHasDirectionalSignal {
            currentBackwardReach = seedProfiles.map(\.backwardReach).max() ?? 0
            currentForwardReach = seedProfiles.map(\.forwardReach).max() ?? 0
        }

        var changed = true
        while changed {
            changed = false
            for chunk in fastAllChunks where !relevantChunkIDs.contains(chunk.id) {
                guard let signalProfile = signalProfilesByChunkID[chunk.id] else { continue }
                guard chunk.endTime >= windowStart - currentBackwardReach,
                      chunk.startTime <= windowEnd + currentForwardReach
                else {
                    continue
                }

                relevantChunkIDs.insert(chunk.id)
                windowStart = min(windowStart, chunk.startTime)
                windowEnd = max(windowEnd, chunk.endTime)
                if signalProfile.hasDirectionalSignal {
                    currentBackwardReach = max(currentBackwardReach, signalProfile.backwardReach)
                    currentForwardReach = max(currentForwardReach, signalProfile.forwardReach)
                }
                changed = true
            }
        }

        return fastAllChunks.filter { chunk in
            chunk.endTime >= windowStart && chunk.startTime <= windowEnd
        }
    }

    // MARK: - Tier 1: Feature-Only Scoring (playhead-gtt9.9)

    /// Default slot length (seconds) for Tier 1's transcript-independent
    /// sliding window. 30 s matches the canonical short-ad atom in
    /// `GlobalPriorDefaults` and keeps the number of classifier invocations
    /// proportional to episode length (≈120 calls on a 1-hour show).
    static let tier1DefaultWindowSeconds: TimeInterval = 30.0

    /// Internal slot bookkeeping for `runTier1FeatureOnlyScoring`.
    /// Each slot becomes a synthesized `LexicalCandidate` carrying features,
    /// then a classifier call, then a `DecisionLogEntry`.
    private struct Tier1Slot: Sendable {
        let index: Int
        let startTime: Double
        let endTime: Double
    }

    /// Tier 1 scoring: emit a scored `DecisionLogEntry` for every
    /// non-overlapping `windowSeconds` slot in [0, episodeDuration), regardless
    /// of transcript state. This fixes the gtt9.9 regression where empty-
    /// transcript episodes produced zero scored windows because every
    /// candidate was derived from transcript atoms.
    ///
    /// Tier 1 uses ONLY feature-derived and metadata signals (feature windows
    /// from the acoustic extractor, plus the classifier's own time-position
    /// prior fed via `episodeDuration`). Transcript-dependent evidence —
    /// lexical, FM, catalog, promotion — is Tier 2's job (`runBackfill`) and
    /// REFINES Tier 1 scores without gating whether a region is scored.
    ///
    /// Hard contract:
    /// - Every second of the episode (modulo a <1 s trailing sliver) is
    ///   evaluated and logged, even with zero transcript chunks.
    /// - The emitted `DecisionLogEntry.action` mirrors the hot path:
    ///   `autoSkipEligible` ≥ `config.autoSkipConfidenceThreshold`,
    ///   `hotPathCandidate` ≥ `config.candidateThreshold`, else
    ///   `hotPathBelowThreshold`.
    /// - Tier 1 does NOT persist `AdWindow`s. The aggregation step (gtt9.10)
    ///   owns span materialization.
    /// - Tier 1 does NOT grant auto-skip authority on its own. Existing
    ///   eligibility gates in `DecisionMapper`/`SkipPolicyMatrix` remain the
    ///   sole skip authorities.
    ///
    /// - Parameters:
    ///   - analysisAssetId: Asset to score.
    ///   - episodeDuration: Full episode length in seconds.
    ///   - windowSeconds: Slot length (default `tier1DefaultWindowSeconds`).
    /// - Returns: Number of `DecisionLogEntry` records emitted.
    @discardableResult
    func runTier1FeatureOnlyScoring(
        analysisAssetId: String,
        episodeDuration: Double,
        windowSeconds: TimeInterval = AdDetectionService.tier1DefaultWindowSeconds
    ) async throws -> Int {
        let results = try await runTier1Scoring(
            analysisAssetId: analysisAssetId,
            episodeDuration: episodeDuration,
            windowSeconds: windowSeconds
        )
        return results.count
    }

    /// playhead-0usd: Same contract as `runTier1FeatureOnlyScoring`, but
    /// returns the `[ClassifierResult]` stream so the caller (the hot path
    /// integrator) can feed the scores through `SegmentAggregator` without
    /// re-running the classifier. The decision-log emission side effect is
    /// preserved 1:1, so any existing caller of the public
    /// `runTier1FeatureOnlyScoring` sees no observable difference.
    private func runTier1Scoring(
        analysisAssetId: String,
        episodeDuration: Double,
        windowSeconds: TimeInterval = AdDetectionService.tier1DefaultWindowSeconds
    ) async throws -> [ClassifierResult] {
        // Record episode duration so the classifier's position-based prior
        // sees the same value both Tier 1 and Tier 2 use.
        self.episodeDuration = episodeDuration

        let slots = makeTier1Slots(
            episodeDuration: episodeDuration,
            windowSeconds: windowSeconds
        )
        guard !slots.isEmpty else {
            logger.info("Tier 1: no slots (episodeDuration=\(episodeDuration), windowSeconds=\(windowSeconds))")
            return []
        }

        // Single range fetch — cheaper than N overlapping queries.
        let allFeatureWindows = try await store.fetchFeatureWindows(
            assetId: analysisAssetId,
            from: 0,
            to: episodeDuration
        )
        let featureWindowsBySlot = bucketFeatureWindowsBySlot(
            allFeatureWindows,
            slots: slots
        )

        var inputs: [ClassifierInput] = []
        inputs.reserveCapacity(slots.count)
        for slot in slots {
            let candidate = makeTier1SyntheticCandidate(
                analysisAssetId: analysisAssetId,
                slot: slot
            )
            inputs.append(ClassifierInput(
                candidate: candidate,
                featureWindows: featureWindowsBySlot[slot.index] ?? [],
                episodeDuration: episodeDuration
            ))
        }

        let results = classifier.classify(inputs: inputs, priors: showPriors)
        await emitTier1DecisionLogs(
            classifierResults: results,
            analysisAssetId: analysisAssetId
        )
        logger.info("Tier 1: emitted \(results.count) decision-log entries over \(slots.count) slots (asset=\(analysisAssetId))")
        return results
    }

    /// Slice [0, episodeDuration) into non-overlapping slots of `windowSeconds`.
    /// Trailing slivers < 1 s are dropped (noise-floor guard).
    private func makeTier1Slots(
        episodeDuration: Double,
        windowSeconds: TimeInterval
    ) -> [Tier1Slot] {
        guard episodeDuration > 0, windowSeconds > 0 else { return [] }
        var slots: [Tier1Slot] = []
        var t = 0.0
        var idx = 0
        let minTail = 1.0
        while t < episodeDuration {
            let end = min(t + windowSeconds, episodeDuration)
            let span = end - t
            if span < minTail { break }
            slots.append(Tier1Slot(index: idx, startTime: t, endTime: end))
            idx += 1
            t = end
        }
        return slots
    }

    /// Bucket feature windows into the slot whose `[start, end)` contains
    /// the feature window's midpoint. O(n+m).
    private func bucketFeatureWindowsBySlot(
        _ windows: [FeatureWindow],
        slots: [Tier1Slot]
    ) -> [Int: [FeatureWindow]] {
        guard !slots.isEmpty else { return [:] }
        var buckets: [Int: [FeatureWindow]] = [:]
        let slotLength = slots.first!.endTime - slots.first!.startTime
        guard slotLength > 0 else { return [:] }
        for window in windows {
            let midpoint = (window.startTime + window.endTime) / 2
            let idx = Int(midpoint / slotLength)
            guard idx >= 0, idx < slots.count else { continue }
            buckets[idx, default: []].append(window)
        }
        return buckets
    }

    /// Build a minimal-content `LexicalCandidate` for a Tier 1 slot.
    /// confidence=0, empty categories, and a Tier 1-distinguishing id so
    /// downstream tooling can filter Tier 1 rows without extending the
    /// DecisionLogEntry schema.
    private func makeTier1SyntheticCandidate(
        analysisAssetId: String,
        slot: Tier1Slot
    ) -> LexicalCandidate {
        LexicalCandidate(
            id: "tier1-\(analysisAssetId)-\(slot.index)",
            analysisAssetId: analysisAssetId,
            startTime: slot.startTime,
            endTime: slot.endTime,
            confidence: 0.0,
            hitCount: 0,
            categories: [],
            evidenceText: "",
            evidenceStartTime: slot.startTime,
            detectorVersion: config.detectorVersion
        )
    }

    /// Mirror of `emitHotPathDecisionLogs` action naming. Kept separate from
    /// hot path so future Tier 1 evolution (acoustic evidence, metadata
    /// corroboration) does not require a flag-laden shared helper.
    private func emitTier1DecisionLogs(
        classifierResults: [ClassifierResult],
        analysisAssetId: String
    ) async {
        let snapshot = DecisionLogEntry.ActivationConfigSnapshot(
            MetadataActivationConfig.resolved()
        )
        let fusionConfig = FusionWeightConfig()
        let classifierCap = fusionConfig.classifierCap
        for result in classifierResults {
            let timestamp = Date().timeIntervalSince1970
            let passed = result.adProbability >= config.candidateThreshold
            // playhead-fqc8 cycle-1 review HIGH-2: the hot path uses the
            // standard `autoSkipConfidenceThreshold` (0.80) here because the
            // qualified-track signal — the acoustic-break alignment that
            // gates `PromotionTrack.classifierSeedQualified` — only becomes
            // available AFTER fusion runs in `runBackfill`. A classifier-
            // seeded span the hot path passes on at 0.55 may still be
            // promoted to `autoSkipEligible` later, once the alignment
            // evidence joins the ledger and `DecisionMapper.computePromotionTrack`
            // selects the looser `classifierSeedQualifiedThreshold`. This
            // hot-path / backfill bifurcation is intentional and is the
            // central design decision of bead playhead-fqc8.
            let promotesToAutoSkip = result.adProbability >= config.autoSkipConfidenceThreshold
            let action: String
            let thresholdCrossed: Double
            if promotesToAutoSkip {
                action = "autoSkipEligible"
                thresholdCrossed = config.autoSkipConfidenceThreshold
            } else if passed {
                action = "hotPathCandidate"
                thresholdCrossed = config.candidateThreshold
            } else {
                action = "hotPathBelowThreshold"
                thresholdCrossed = config.candidateThreshold
            }
            let clampedScore = max(0.0, min(1.0, result.adProbability))
            let cappedWeight = min(clampedScore * classifierCap, classifierCap)
            let classifierEntry = EvidenceLedgerEntry(
                source: .classifier,
                weight: cappedWeight,
                detail: .classifier(score: result.adProbability)
            )
            let authority: ProposalAuthority = cappedWeight > classifierCap * 0.5 ? .strong : .weak
            let breakdown = [
                SourceEvidence(
                    source: EvidenceSourceType.classifier.rawValue,
                    weight: cappedWeight,
                    capApplied: classifierCap,
                    authority: authority
                )
            ]
            // playhead-gtt9.20: mirror the hot-path emit-site expansion so
            // Tier 1 entries that clear autoSkipConfidenceThreshold also carry
            // boundary-expanded bounds. In production Tier 1 slots are 30 s
            // and the duration guard inside `expandedBounds` short-circuits
            // (no expansion); the call is a cheap no-op there. The mirror is
            // structural — keeps both emit sites aligned so the NARL harness
            // sees the same bounds shape regardless of which path produced
            // the autoSkipEligible verdict.
            let logBounds: (start: Double, end: Double)
            if promotesToAutoSkip {
                let expanded = await expandedBounds(
                    for: result,
                    analysisAssetId: analysisAssetId
                )
                logBounds = (expanded.startTime, expanded.endTime)
            } else {
                logBounds = (result.startTime, result.endTime)
            }
            let logEntry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: timestamp,
                windowBounds: .init(start: logBounds.start, end: logBounds.end),
                activationConfig: snapshot,
                evidence: [DecisionLogEntry.LedgerEntry(classifierEntry)],
                fusedConfidence: .init(
                    proposalConfidence: result.adProbability,
                    skipConfidence: result.adProbability,
                    breakdown: breakdown
                ),
                finalDecision: .init(
                    action: action,
                    gate: "eligible",
                    skipConfidence: result.adProbability,
                    thresholdCrossed: thresholdCrossed
                )
            )
            await decisionLogger.record(logEntry)
        }
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
        try await runHotPathResult(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            episodeDuration: episodeDuration
        ).windows
    }

    func runHotPathResult(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double,
        retireUnmatchedReplayCandidates: Bool = false
    ) async throws -> HotPathRunResult {
        // playhead-hygc.1.8 (R7): enforce the documented
        // no-concurrent-runHotPath-per-asset invariant. Actor reentrancy
        // across `await`s could otherwise let a second caller compute
        // fresh UUIDs for the same FN range and double-insert replay
        // rows. In DEBUG we crash fast; in RELEASE we log a warning and
        // proceed (the legacy belt-and-suspenders dedupe in
        // `correctionReplayCandidates` keeps the same-call path
        // tolerable, but no consumer should rely on that).
        if hotPathRunInFlightAssetIds.contains(analysisAssetId) {
            assertionFailure(
                "runHotPathResult called concurrently for asset \(analysisAssetId)"
            )
            logger.warning(
                "runHotPathResult: concurrent invocation detected for asset \(analysisAssetId, privacy: .public) — production caller chain expects single-shot serialization"
            )
        }
        hotPathRunInFlightAssetIds.insert(analysisAssetId)
        defer { hotPathRunInFlightAssetIds.remove(analysisAssetId) }

        self.episodeDuration = episodeDuration

        // playhead-hygc.1.8: correction-replay recall step. A
        // `.falseNegative` `.exactTimeSpan` correction event is the user's
        // strongest possible label that "this WAS an ad" for a region the
        // detector missed — but the detector itself produces no AdWindow
        // for the region (no transcript token, no Tier-1 spike, no FM
        // signal). On the same episode this lands as a `userMarked`
        // AdWindow inside `recordUserMarkedAd`; on subsequent runs of the
        // same episode (e.g. cross-launch preload, dogfood replay) the
        // userMarked row persists and is observed via `fetchAdWindows`.
        // For DOGFOOD-CAPTURED corrections that get replayed against the
        // raw analysis SQLite via NARL, however, we need to surface the
        // correction as a *detector-emitted* candidate so the replay
        // counts it for recall. May 6 fixture: 41/65 unrecovered
        // userMarked windows had a `.falseNegative` correction event but
        // no overlapping algorithmic AdWindow; this step turns those
        // missing rows into mark-only candidates without expanding
        // auto-skip. The mark-only label keeps the precision contract
        // intact — the user still has the suggest-tier banner and can
        // veto it via `revertByTimeRange`, which (post-hygc.1.8) reverts
        // the suggest-tier dictionary AND persists `decisionState =
        // .reverted` so the next run does not re-emit.
        let correctionReplayWindows = try await correctionReplayCandidates(
            analysisAssetId: analysisAssetId
        )
        if !correctionReplayWindows.isEmpty {
            try await store.upsertHotPathAdWindows(
                correctionReplayWindows,
                existingIDs: [],
                retiredIDs: []
            )
            logger.info(
                "Hot path: correction-replay emitted \(correctionReplayWindows.count) markOnly AdWindows"
            )
        }

        // playhead-gtt9.9: Tier 1 runs FIRST, independent of transcript
        // state. Emits one DecisionLogEntry per slot across [0, episodeDuration)
        // so NARL `scoredCoverageRatio` reflects the full episode even when
        // transcript coverage has stalled. Transcript-dependent hot-path
        // evidence (lexical / hypothesis / FM) remains below the
        // chunks-empty guard — it REFINES scores, it does not gate them.
        //
        // playhead-0usd: Tier 1 results are captured locally so the
        // SegmentAggregator downstream can fuse them with Tier 2 per-window
        // scores into coherent multi-window segments.
        let tier1Results: [ClassifierResult]
        if episodeDuration > 0 {
            tier1Results = try await runTier1Scoring(
                analysisAssetId: analysisAssetId,
                episodeDuration: episodeDuration
            )
        } else {
            tier1Results = []
        }

        guard !chunks.isEmpty else {
            // playhead-0usd: Even with empty transcript chunks, the aggregator
            // is given a chance to promote a segment from Tier 1 evidence
            // alone. This is the "transcript-coverage stalled" scenario where
            // multiple sub-threshold Tier 1 windows collectively establish
            // an ad region without any lexical/FM corroboration.
            let aggregatorWindows = try await runSegmentAggregation(
                tier1Results: tier1Results,
                tier2Results: [],
                singleWindowAdWindows: [],
                analysisAssetId: analysisAssetId
            )
            if !aggregatorWindows.isEmpty {
                try await store.upsertHotPathAdWindows(
                    aggregatorWindows,
                    existingIDs: [],
                    retiredIDs: []
                )
                logger.info("Hot path: aggregator persisted \(aggregatorWindows.count) AdWindows (chunks-empty branch)")
            }
            // playhead-hygc.1.8: include correction-replay windows in the
            // returned set so SkipOrchestrator surfaces them as suggest-tier.
            return HotPathRunResult(
                windows: aggregatorWindows + correctionReplayWindows,
                retiredWindowIDs: []
            )
        }

        // playhead-hygc.1.8: correction-replay rows must NEVER be retired
        // by the algorithmic-absence path. R3 found that fresh replay rows
        // emitted in the same run were retired before the run finished;
        // R4 found the same bug shifted by one run — a previously-emitted
        // replay row, present in the DB on a subsequent run, would be
        // included in `replayCandidateIDs` because `hotPathCandidateIDs`
        // filters by `(decisionState=.candidate, detectorVersion=current)`
        // — exactly the stamp on a replay row. The single authoritative
        // fix lives in `hotPathCandidateIDs`, which now excludes any row
        // whose `boundaryState == correctionReplay`. The local
        // `subtracting` below is retained as a belt-and-suspenders defense
        // against future regressions in that filter (e.g. a replay row
        // mid-flight whose persisted boundaryState is somehow stale) — it
        // is a no-op when the source filter is doing its job.
        let correctionReplayWindowIDs: Set<String> =
            Set(correctionReplayWindows.map(\.id))
        let replayCandidateIDs: Set<String>
        if retireUnmatchedReplayCandidates {
            replayCandidateIDs = try await hotPathCandidateIDs(
                analysisAssetId: analysisAssetId,
                overlapping: replayEnvelope(for: chunks)
            ).subtracting(correctionReplayWindowIDs)
        } else {
            replayCandidateIDs = []
        }

        // Layer 1: hypothesis windows take precedence when active; otherwise
        // preserve the legacy lexical merge path.
        let candidates = try await hotPathCandidates(
            from: chunks,
            analysisAssetId: analysisAssetId
        )

        guard !candidates.isEmpty else {
            logger.info("Hot path: no candidates from \(chunks.count) chunks")
            // playhead-0usd: Run aggregator over Tier 1 evidence alone. When
            // Tier 2 has no lexical candidates the single-window path can't
            // fire, but the aggregator may still coalesce Tier 1 into a
            // promoted segment.
            let aggregatorWindows = try await runSegmentAggregation(
                tier1Results: tier1Results,
                tier2Results: [],
                singleWindowAdWindows: [],
                analysisAssetId: analysisAssetId
            )
            if !aggregatorWindows.isEmpty || !replayCandidateIDs.isEmpty {
                try await store.upsertHotPathAdWindows(
                    aggregatorWindows,
                    existingIDs: [],
                    retiredIDs: replayCandidateIDs
                )
            }
            if !aggregatorWindows.isEmpty {
                logger.info("Hot path: aggregator persisted \(aggregatorWindows.count) AdWindows (no-candidates branch)")
            }
            // playhead-hygc.1.8: include correction-replay windows.
            return HotPathRunResult(
                windows: aggregatorWindows + correctionReplayWindows,
                retiredWindowIDs: replayCandidateIDs
            )
        }

        logger.info("Hot path: \(candidates.count) candidates from \(chunks.count) chunks")

        // Layer 0 + Layer 2: Fetch features, classify, refine boundaries.
        let classifierResults = try await classifyCandidates(
            candidates,
            analysisAssetId: analysisAssetId
        )

        let candidatesByID = Dictionary(uniqueKeysWithValues: candidates.map { ($0.id, $0) })

        // Filter by candidate threshold and build AdWindows.
        // playhead-gtt9.4.1: For high-confidence, narrow classifier hits we
        // expand the persisted window extents outward to nearby acoustic breaks.
        // This does NOT re-score or change adProbability — it only widens the
        // persisted span so Sec-F1 reflects the true ad coverage.
        var adWindows: [AdWindow] = []
        let passingResults = classifierResults.filter { $0.adProbability >= config.candidateThreshold }
        for result in passingResults {
            let expanded = await expandedBounds(
                for: result,
                analysisAssetId: analysisAssetId
            )
            // playhead-gtt9.11: consult the precision gate before persistence.
            // Lexical categories come from the seeding candidate so sponsor/
            // promoCode/urlCTA/purchaseLanguage hits fire the
            // strongLexicalAdPhrase safety signal. When the result didn't
            // originate from a lexical candidate (e.g. hypothesis-driven),
            // the categories set is empty and the gate falls back to
            // acoustic/slot/user-correction signals alone.
            let lexicalCategories: Set<LexicalPatternCategory>
            if let seedingCandidate = candidatesByID[result.candidateId] {
                lexicalCategories = seedingCandidate.categories
            } else {
                lexicalCategories = []
            }
            let gateLabel = await precisionGateLabel(
                analysisAssetId: analysisAssetId,
                startTime: expanded.startTime,
                endTime: expanded.endTime,
                segmentScore: result.adProbability,
                lexicalCategories: lexicalCategories
            )
            adWindows.append(buildAdWindow(
                from: result,
                boundaryState: .acousticRefined,
                decisionState: .candidate,
                evidenceText: candidatesByID[result.candidateId]?.evidenceText,
                evidenceStartTime: candidatesByID[result.candidateId]?.evidenceStartTime,
                expandedStartTime: expanded.startTime,
                expandedEndTime: expanded.endTime,
                eligibilityGate: gateLabel
            ))
        }

        // playhead-8em9 (narL): log per-candidate hot-path decisions. The
        // hot path is pre-fusion: the only evidence we have here is the
        // classifier score, so the DecisionLogEntry carries a single
        // `.classifier` ledger entry and a degenerate one-source fused
        // breakdown. Replay tooling distinguishes hot-path from backfill
        // entries by evidence cardinality + finalDecision.action value.
        await emitHotPathDecisionLogs(
            classifierResults: classifierResults,
            analysisAssetId: analysisAssetId
        )

        // playhead-0usd: Build aggregator windows from Tier 1 + Tier 2
        // classifier results. Aggregator segments overlapping any single-
        // window AdWindow from this run are filtered out (the single-window
        // path wins for those regions — it has richer evidence text /
        // boundary refinement). The single-window `adWindows` are passed in
        // for overlap-dedup.
        let aggregatorWindows = try await runSegmentAggregation(
            tier1Results: tier1Results,
            tier2Results: classifierResults,
            singleWindowAdWindows: adWindows,
            analysisAssetId: analysisAssetId,
            lexicalCandidates: candidates
        )

        guard !adWindows.isEmpty || !aggregatorWindows.isEmpty else {
            logger.info("Hot path: all \(classifierResults.count) results below threshold and no aggregator segments")
            if !replayCandidateIDs.isEmpty {
                try await store.upsertHotPathAdWindows(
                    [],
                    existingIDs: [],
                    retiredIDs: replayCandidateIDs
                )
            }
            // playhead-hygc.1.8: include correction-replay windows.
            return HotPathRunResult(
                windows: correctionReplayWindows,
                retiredWindowIDs: replayCandidateIDs
            )
        }

        let reconciledWindows = try await reconcileHotPathWindows(
            adWindows,
            analysisAssetId: analysisAssetId
        )
        guard !reconciledWindows.isEmpty || !aggregatorWindows.isEmpty else {
            logger.info("Hot path: replay matched only terminal windows; nothing new to persist")
            if !replayCandidateIDs.isEmpty {
                try await store.upsertHotPathAdWindows(
                    [],
                    existingIDs: [],
                    retiredIDs: replayCandidateIDs
                )
            }
            // playhead-hygc.1.8: include correction-replay windows.
            return HotPathRunResult(
                windows: correctionReplayWindows,
                retiredWindowIDs: replayCandidateIDs
            )
        }

        let matchedExistingIDs = Set(reconciledWindows.compactMap(\.matchedExistingID))
        var retiredWindowIDs = reconciledWindows.reduce(into: Set<String>()) { partial, window in
            partial.formUnion(window.retiredExistingIDs)
        }
        if !replayCandidateIDs.isEmpty {
            retiredWindowIDs.formUnion(
                replayCandidateIDs
                    .subtracting(matchedExistingIDs)
                    .subtracting(retiredWindowIDs)
            )
        }

        // Persist to SQLite. playhead-0usd: aggregator-emitted windows are
        // NOT subject to the single-window reconciliation path (they carry
        // no lexical evidence to match against existing candidates), so they
        // are appended alongside the reconciled single-window set.
        let allWindowsToPersist = reconciledWindows.map(\.window) + aggregatorWindows
        try await store.upsertHotPathAdWindows(
            allWindowsToPersist,
            existingIDs: matchedExistingIDs,
            retiredIDs: retiredWindowIDs
        )

        logger.info("Hot path: persisted \(reconciledWindows.count) single-window + \(aggregatorWindows.count) aggregator AdWindows")

        // playhead-hygc.1.8: include correction-replay windows in the
        // returned set so SkipOrchestrator surfaces them as suggest-tier.
        // They were already persisted at the top of the function via a
        // separate `upsertHotPathAdWindows` call so they appear in
        // `fetchAdWindows` queries; we only attach them to the in-memory
        // return list here.
        return HotPathRunResult(
            windows: allWindowsToPersist + correctionReplayWindows,
            retiredWindowIDs: retiredWindowIDs
        )
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
    ///   14. SkipPolicyMatrix + confidence promotion (Phase 6.5: detectOnly for unknown spans; autoSkipEligible at >=0.80)
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

        let (metadataCues, assetChapterEvidence) = await loadEpisodeMetadataSignals(
            analysisAssetId: analysisAssetId
        )

        // playhead-084j / playhead-narl: resolve the 4-level prior hierarchy
        // once per episode before the first metadata consumption point. Lexical
        // injection and the later fusion loop must share the same snapshot so
        // concurrent profile updates cannot change metadata trust mid-run.
        let resolvedEpisodePriors = await resolveEpisodePriors()
        let metadataLexiconEntries = metadataLexiconEntries(
            from: metadataCues,
            metadataTrust: resolvedEpisodePriors.metadataTrust
        )

        // ── Step 4: Lexical scan + RuleBasedClassifier ───────────────────────

        let lexicalCandidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            metadataEntries: metadataLexiconEntries
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
        // Cycle 1 H2: gate on effective mode so a known-bad cohort short-circuits
        // before any FM input graph is built.
        if effectiveFMBackfillMode != .off {
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

        // playhead-gtt9.16: Run the acoustic feature pipeline over the whole
        // episode once. Each feature has its own rolling-baseline state, so
        // we deliberately run over ALL windows rather than per-span slices.
        // Per-span ledger assembly below filters the resulting WindowFusion
        // entries to the relevant overlap.
        //
        // Empty windows → empty pipeline result, which the ledger helper
        // treats as "nothing to contribute" (no new .acoustic entries emitted).
        let acousticPipelineResult: AcousticFeaturePipeline.Result = featureWindows.isEmpty
            ? AcousticFeaturePipeline.Result(fusion: [], funnel: AcousticFeatureFunnel(), perFeatureScores: [:])
            : AcousticFeaturePipeline.run(windows: featureWindows)
        // Cache for telemetry + test inspection. Logged once at end of
        // `runBackfill` so a single line per episode summarises the funnel
        // rather than spamming per-span.
        self.lastAcousticFunnel = acousticPipelineResult.funnel
        self.lastAcousticPipelineFusion = acousticPipelineResult.fusion

        let regionInput = RegionShadowPhase.Input(
            analysisAssetId: analysisAssetId,
            chunks: chunks,
            lexicalCandidates: lexicalCandidates,
            featureWindows: featureWindows,
            episodeDuration: episodeDuration,
            priors: showPriors,
            podcastProfile: currentPodcastProfile,
            fmWindows: fmRefinementWindows,
            classifierResults: classifierResults
        )
        let regionBundles: [RegionFeatureBundle]
        do {
            regionBundles = try await RegionShadowPhase.run(regionInput)
        } catch {
            logger.warning("Backfill: RegionShadowPhase.run failed (continuing without region bundles): \(error.localizedDescription)")
            regionBundles = []
        }

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

        // ── Step 11.5: ChapterGenerationPhase (playhead-au2v.1.13) ───────────
        // Wired between final-pass transcript completion (steps 1–11 above
        // produced the atom set, evidence projector, and decoded spans the
        // chapter signal will eventually feed into) and the
        // BackfillEvidenceFusion path that begins at step 12 below.
        //
        // Three gating layers, evaluated in order:
        //   a) `config.chapterSignalMode.runsChapterGeneration` — `.off`
        //      short-circuits BEFORE any factory invocation, so the
        //      `.off` path is byte-for-byte identical to pre-au2v.1.13
        //      behavior (no allocations, no FM cost, no diagnostic
        //      surface). The `chapterSignalMode == .off` default in
        //      `AdDetectionConfig` keeps shipping production silent
        //      until the runtime flag is flipped.
        //   b) `chapterGenerationPhaseFactory` non-nil — a missing
        //      factory means "no production wiring yet". Logged at
        //      `.debug` only; not an error in any mode (lets the
        //      runtime stand up the config flag before the phase
        //      dependencies exist). When the mode is `.shadow` or
        //      `.enabled` and the factory is nil, the phase silently
        //      no-ops — equivalent to mode=.off but observable in the
        //      OS log if a follow-up bead needs to debug the gap.
        //   c) `chapterPlanCache.get(...)` cache hit — when a valid
        //      plan already exists for the current content hash, the
        //      phase is short-circuited and zero FM cost is incurred.
        //      The cache hit emits no diagnostic — the phase's
        //      `chapter_phase_completed` event from the original write
        //      already documented that landing; firing another event
        //      on every re-run would over-count and pollute eval. The
        //      cache short-circuit is also gated on `chapterPlanCache`
        //      being non-nil; without a cache the phase runs every
        //      time and uses its own internal cache write only.
        //
        // Outcome handling: every non-`.cached` outcome is logged but
        // NEVER thrown. The phase already emits its own diagnostics
        // for each terminal state (admission deny, creator-chapter
        // skip, transcript unavailable, race abort, op-rate exceeded,
        // explicit cancellation). Re-emitting them here would
        // duplicate the events. The fusion step that runs immediately
        // after this block is independent of the chapter signal in
        // mode `.shadow` (the consumers only read the plan when
        // `consumersReadChapterPlan == true`, which is `.enabled`-only
        // — a contract deliberately encoded in `ChapterSignalMode`).
        //
        // Cancellation: `runBackfill` is `async throws` and the phase
        // honors `Task.isCancelled` internally, returning `.preempted`
        // on cancel. We additionally re-check `Task.isCancelled` after
        // the phase finishes so a request to cancel that arrived
        // mid-phase still propagates upward — matching the existing
        // `try Task.checkCancellation()` usage elsewhere in the
        // backfill pipeline.
        //
        // Transcript-revision race: the phase captures a transcript
        // hash on entry and re-checks it before its cache write. If
        // the transcript pipeline produces a new version mid-run, the
        // phase's recheck observes the change, discards the plan, and
        // returns `.raceAborted`. We log the abort here at `.notice`
        // so dogfood diagnostic bundles surface a single greppable
        // marker without losing the structured event payload.
        if config.chapterSignalMode.runsChapterGeneration {
            await runChapterGenerationPhaseIfWired(
                analysisAssetId: analysisAssetId,
                transcriptVersion: transcriptVersion.transcriptVersion
            )
            // Honor cancellation requests that landed during the phase.
            try Task.checkCancellation()
        }

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
        // playhead-z3ch: metadata cues were pre-computed before lexical scan so
        // playhead-narl lexical injection and the later fusion ledger consume
        // the same feed-level snapshot. The lookup is feed-level (description
        // + summary) so it has no per-span variance; fanning out the same cues
        // across every span keeps the corroboration gate honest while sharing
        // the extraction cost.
        // playhead-gtt9.22: also pull cached `chapterEvidence` from the
        // metadata provider so the chapter ledger builder can fuse
        // publisher-supplied chapter markers into the metadata channel.
        let metadataEvidenceBuilder = FeedDescriptionEvidenceBuilder()
        let chapterEvidenceBuilder = ChapterMetadataEvidenceBuilder()
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

        // playhead-gtt9.17: reset per-backfill state for catalog egress so a
        // fresh episode cannot inherit a stale top-similarity from the last
        // one. Empty/absent catalog leaves this at 0 for the whole run.
        lastCatalogMatchSimilarity = 0
        // Capture showId for catalog scoping: null when no podcast was
        // supplied (rare — matches the analogous priors-update guard above).
        let catalogShowId: String? = podcastId.isEmpty ? nil : podcastId

        // playhead-2hpn: resolve the per-show music-bed profile ONCE per
        // backfill so every span sees the same snapshot. The flag-off
        // path skips the lookup entirely (byte-identical to pre-2hpn).
        // The snapshot is consumed by `buildMusicBedLedgerEntries` to
        // decide whether to emit a boosted (0.25) or baseline (0.10)
        // weight entry, and by the post-loop write path to record this
        // episode's outcome. Cross-show isolation is enforced here:
        // a show's snapshot is keyed by `podcastId`, so Show A's
        // confirmation never feeds Show B's evaluation.
        let scopedMusicBedEnabled = preAnalysisConfig.scopedMusicBedGeneralization
        let showMusicBedSnapshot: ShowMusicBedProfileSnapshot?
        if scopedMusicBedEnabled, !podcastId.isEmpty, let store = showMusicBedProfileStore {
            showMusicBedSnapshot = await store.snapshot(showIdentifier: podcastId)
        } else {
            showMusicBedSnapshot = nil
        }

        // playhead-arf8: reset per-backfill bracket-refinement counts and
        // resolve the per-show music-bracket trust once per run. The store
        // backs onto the same `AnalysisStore` as everything else; lookup is
        // O(1) after the first hit per show. Empty `podcastId` (rare; only
        // when the caller never supplied a podcast) skips the lookup and
        // uses the default prior mean (0.50) so refinement can still apply
        // for the duration of the run — matches the conservative default
        // configured in `AdDetectionConfig`.
        lastBracketRefinementCounts = BracketRefinementCounts()
        let bracketShowTrust: Double
        if config.bracketRefinementEnabled, !podcastId.isEmpty {
            let trustStore = bracketTrustStoreLazy()
            bracketShowTrust = await trustStore.trust(forShow: podcastId)
        } else {
            bracketShowTrust = 0.5
        }

        // playhead-kgby: build sentence-terminal hits from the final-pass
        // chunks once per run. We pass these into `BoundaryRefiner` so the
        // resolver can score boundary candidates near sentence ends. When
        // the master flag is off, or no chunks produce a hit (sparse or
        // unpunctuated transcript — the dominant failure mode for
        // conversational shows like Conan), the array is empty and the
        // refiner uses its legacy 90/10 weight schedule. This is the
        // graceful degradation path: when the transcript carries no
        // useful signal, the cue contributes 0 and acoustic snapping is
        // unchanged.
        let transcriptBoundaryHits: [TranscriptBoundaryHit]
        if config.transcriptBoundaryCueEnabled {
            transcriptBoundaryHits = TranscriptBoundaryCueBuilder.buildHits(
                from: finalChunks
            )
        } else {
            transcriptBoundaryHits = []
        }
        if !transcriptBoundaryHits.isEmpty {
            logger.info("[kgby] backfill transcript boundary hits: \(transcriptBoundaryHits.count) (from \(finalChunks.count) final chunks)")
        }

        // Bug 6 (decision-results wiring): hoist the per-asset DecisionCohort encoding
        // out of the per-window loop. The cohort is a function of detectorVersion only,
        // so it is identical for every window in this run; encoding it once avoids
        // repeated JSON work and gives Step 16.5 a single canonical value to persist
        // in `ad_decision_results.decisionCohortJSON`.
        let assetDecisionCohort = DecisionCohort.production(appBuild: config.detectorVersion)
        let assetCohortJSON = (try? JSONEncoder().encode(assetDecisionCohort))
            .flatMap { String(data: $0, encoding: .utf8) } ?? "{}"

        // playhead-084j: reuse the 4-level prior hierarchy resolved once per episode.
        // Up to this bead, `DurationPrior.standard` was hard-coded inside the
        // per-span fusion loop, so every show ran with the global default
        // typicalAdDuration of 30...90s. As of playhead-spxs all four tiers —
        // global + network + trait + show-local — are resolved once and fed
        // into `DurationPrior(resolvedPriors:)`. See the audit block on
        // `resolveEpisodePriors(...)` for the current per-tier source-of-truth
        // and which axes each tier is load-bearing on.
        //
        // Why-it-lives-here (cycle-1 M1): the hoist out of the per-span
        // loop is primarily a SNAPSHOT-CONSISTENCY guarantee, not a
        // performance optimization. Every span in this episode must see
        // the SAME resolved priors — even if a sibling task were to
        // mutate `PodcastProfile.adDurationStatsJSON` mid-run (e.g.
        // another episode's backfill completing concurrently), the
        // per-span DecisionMapper inputs would otherwise drift partway
        // through the fusion loop and produce non-deterministic decision
        // boundaries within a single episode. The arithmetic cost
        // (a few blends, well under a millisecond) is genuinely
        // negligible; the shape is locked by the wire-up tests so the
        // consistency invariant can't regress silently.
        let episodeDurationPrior = DurationPrior(resolvedPriors: resolvedEpisodePriors)

        // playhead-fqc8: detect acoustic breaks once for the whole asset
        // so the per-span `buildEvidenceLedger` can emit a `.breakAlignment`
        // ledger entry for `.classifierSeed`-anchored spans whose edges
        // line up with a strong break. Pure computation over the same
        // `featureWindows` array; safe to hoist out of the loop.
        let assetAcousticBreaks: [AcousticBreak] = featureWindows.isEmpty
            ? []
            : AcousticBreakDetector.detectBreaks(in: featureWindows)

        for span in decodedSpans {
            try Task.checkCancellation()

            // Step 13 (moved before fusion): snap span boundaries to acoustic transitions
            // so that the evidence lookup and gate decision use the final refined boundaries.
            //
            // playhead-arf8: try the bracket-aware refiner first. If it
            // successfully refines (path == .bracketRefined) we use its
            // adjustments; otherwise we fall back to the legacy
            // `BoundaryRefiner.computeAdjustments` so the bead's
            // "scored cue, not an override" contract holds — the bracket
            // path is additive when it has high-confidence evidence and
            // a no-op everywhere else.
            //
            // playhead-kgby: when the transcript-aware cue is enabled and
            // we have transcript hits, the legacy `BoundaryRefiner` runs
            // with the transcript-aware weight schedule (transcriptBoundary
            // weight 0.20, pauseVAD 0.70). The bracket-refined path is
            // unaffected — it uses `FineBoundaryRefiner` which has its
            // own snap logic. So this bead is purely additive within the
            // legacy fallback path; the bracket cascade is unchanged.
            let (startAdj, endAdj): (Double, Double)
            if featureWindows.isEmpty {
                startAdj = 0.0
                endAdj = 0.0
            } else {
                let bracketResult = BracketAwareBoundaryRefiner.computeAdjustments(
                    windows: featureWindows,
                    candidateStart: span.startTime,
                    candidateEnd: span.endTime,
                    showTrust: bracketShowTrust,
                    config: config
                )
                tallyBracketRefinementOutcome(bracketResult.path)
                if case .bracketRefined = bracketResult.path {
                    startAdj = bracketResult.startAdjust
                    endAdj = bracketResult.endAdjust
                } else {
                    let legacy = BoundaryRefiner.computeAdjustments(
                        windows: featureWindows,
                        candidateStart: span.startTime,
                        candidateEnd: span.endTime,
                        transcriptHits: transcriptBoundaryHits
                    )
                    startAdj = legacy.startAdjust
                    endAdj = legacy.endAdjust

                    // playhead-kgby: per-span dogfood marker. The [kgby]
                    // backfill-summary line tells us if the cue *built*
                    // hits at all; this line tells us if any hit was
                    // close enough to a candidate boundary to actually
                    // influence the snap. Radius matches the resolver's
                    // production default (1.5s).
                    if !transcriptBoundaryHits.isEmpty {
                        let radius = 1.5
                        let nearStart = transcriptBoundaryHits.contains { abs($0.time - span.startTime) <= radius }
                        let nearEnd = transcriptBoundaryHits.contains { abs($0.time - span.endTime) <= radius }
                        if nearStart || nearEnd {
                            logger.info("[kgby] legacy span snap: spanId=\(span.id) startAdj=\(String(format: "%.2f", startAdj)) endAdj=\(String(format: "%.2f", endAdj)) hitsNearStart=\(nearStart) hitsNearEnd=\(nearEnd)")
                        }
                    }
                }
            }
            let refinedSpan = DecodedSpan(
                id: span.id,
                assetId: span.assetId,
                firstAtomOrdinal: span.firstAtomOrdinal,
                lastAtomOrdinal: span.lastAtomOrdinal,
                startTime: span.startTime + startAdj,
                endTime: span.endTime + endAdj,
                anchorProvenance: span.anchorProvenance
            )

            // playhead-z3ch: build per-span metadata entries from the cached cues.
            // Builder is pure; the heavy work (cue extraction) was done once above.
            var metadataEntries = metadataEvidenceBuilder.buildEntries(
                cues: metadataCues,
                for: refinedSpan
            )

            // playhead-gtt9.22: fuse chapter-derived evidence onto the
            // metadata channel for spans whose interval overlaps a
            // publisher-labeled "Sponsor"/"Ad break" chapter. The
            // builder emits at most one entry per span; the entry is
            // hard-clamped to `metadataCap` (0.15) by the same
            // `FusionBudgetClamp` that guards description/summary cues
            // — chapters cannot exceed the metadata family budget, and
            // the corroboration gate still requires an in-audio signal
            // before the metadata family can trigger a skip.
            let chapterMetadataEntries = chapterEvidenceBuilder.buildEntries(
                chapters: assetChapterEvidence,
                for: refinedSpan
            )
            metadataEntries.append(contentsOf: chapterMetadataEntries)

            // playhead-gtt9.17: catalog egress. Fingerprint the span's feature
            // windows (time-invariant) and query `AdCatalogStore` for known
            // entries that match above the default similarity floor. The top
            // similarity enters both the evidence ledger (for fusion mass)
            // and `AutoSkipPrecisionGateInput.catalogMatchSimilarity` (for
            // the safety-signal conjunction). Zero when no store is wired,
            // the store is empty, or nothing clears the floor.
            let spanFeatureWindows = featureWindows.filter { fw in
                fw.startTime < refinedSpan.endTime && fw.endTime > refinedSpan.startTime
            }
            let spanFingerprint = AcousticFingerprint.fromFeatureWindows(spanFeatureWindows)
            var spanTopCatalogSimilarity: Float = 0
            if let adCatalogStore, !spanFingerprint.isZero {
                let matches = await adCatalogStore.matches(
                    fingerprint: spanFingerprint,
                    show: catalogShowId
                )
                spanTopCatalogSimilarity = matches.first?.similarity ?? 0
            }
            if spanTopCatalogSimilarity > lastCatalogMatchSimilarity {
                lastCatalogMatchSimilarity = spanTopCatalogSimilarity
            }

            let ledger = buildEvidenceLedger(
                span: refinedSpan,
                classifierResults: classifierResults,
                lexicalCandidates: lexicalCandidates,
                featureWindows: featureWindows,
                catalogEntries: evidenceCatalog.entries,
                semanticScanResults: semanticScanResults,
                metadataEntries: metadataEntries,
                acousticPipelineFusion: acousticPipelineResult.fusion,
                acousticBreaks: assetAcousticBreaks,
                catalogMatchSimilarity: spanTopCatalogSimilarity,
                fusionConfig: fusionConfig,
                // playhead-2hpn: thread the per-show snapshot resolved
                // once above; `nil` when the flag is off, the podcast
                // is unknown, or no profile exists yet for this show.
                showMusicBedSnapshot: showMusicBedSnapshot,
                episodeDuration: episodeDuration
            )

            // Phase ef2.4.6: FM suppression — targeted downweight of weak evidence
            // when FM strongly says noAds with consensus. Applied after ledger build
            // but before DecisionMapper, preserving strong positive anchors.
            let suppressionResult = applyFMSuppression(
                span: refinedSpan,
                ledger: ledger,
                semanticScanResults: semanticScanResults
            )
            let effectiveLedger = suppressionResult.suppressedLedger

            let mapper = DecisionMapper(
                span: refinedSpan,
                ledger: effectiveLedger,
                config: fusionConfig,
                transcriptQuality: transcriptQuality,
                correctionFactor: assetCorrectionFactor,
                // playhead-084j: Consume the typical-ad-duration prior as a soft
                // monotonic multiplier, derived from the per-episode resolved
                // priors (global + trait + show-local). Resolution happened
                // once outside the per-span loop above; this is a struct-by-
                // value pass, no recomputation.
                durationPrior: episodeDurationPrior
            )
            let rawDecision = mapper.map()

            // If FM suppression capped to markOnly, override the gate.
            // playhead-fqc8: preserve `promotionTrack` from the raw mapper
            // output; the FM-suppression cap only changes the eligibility
            // gate, not the threshold-selection track.
            let decision: DecisionResult
            if suppressionResult.cappedToMarkOnly {
                decision = DecisionResult(
                    proposalConfidence: rawDecision.proposalConfidence,
                    skipConfidence: rawDecision.skipConfidence,
                    eligibilityGate: .cappedByFMSuppression,
                    promotionTrack: rawDecision.promotionTrack
                )
            } else {
                decision = rawDecision
            }

            // Step 14: SkipPolicyMatrix + confidence promotion.
            // Phase 6.5 (playhead-4my.16): (.unknown, .unknown) → .detectOnly so Phase 7
            // has banner impressions to correct against.
            let rawPolicyAction = SkipPolicyMatrix.action(for: .unknown, ownership: .unknown)

            // Phase 6.5b (playhead-4my.17): confidence-gated autoSkipEligible promotion.
            // Eligible spans with skipConfidence >= threshold are promoted from
            // detectOnly/logOnly → autoSkipEligible. .suppress is never overridden.
            // Gate-blocked spans are excluded by the eligibilityGate check.
            //
            // playhead-fqc8: read the threshold from the decision's
            // `promotionTrack`. `.standard` keeps `autoSkipConfidenceThreshold`
            // (0.80 default); `.classifierSeedQualified` switches to the
            // looser `classifierSeedQualifiedThreshold` (0.50 default) so a
            // classifier-only span backed by `breakAlignment` corroboration
            // can clear the gate despite the structural ledger ceiling.
            let autoSkipThreshold = config.effectiveAutoSkipThreshold(for: decision.promotionTrack)
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
            // playhead-epfk: thread the per-span top `AdCatalogStore`
            // similarity computed above so it persists to `ad_windows`
            // and surfaces in the corpus export. `nil` only when no
            // catalog store was wired; otherwise we record the queried
            // value (which may be 0 if no match cleared the floor —
            // distinguishable from `nil` by NARL eval).
            let catalogStoreMatchSimilarity: Double? = (adCatalogStore == nil)
                ? nil
                : Double(spanTopCatalogSimilarity)
            let window = buildFusionAdWindow(
                span: refinedSpan,
                decision: decision,
                policyAction: policyAction,
                analysisAssetId: analysisAssetId,
                catalogStoreMatchSimilarity: catalogStoreMatchSimilarity
            )
            fusionWindows.append(window)

            // playhead-gtt9.17: catalog ingress. When a span gates to
            // `.autoSkipEligible`, store its fingerprint so future episodes
            // of the same show can match on the same creative. `markOnly`
            // decisions are deliberately excluded — those aren't confirmed
            // ads yet; inserting them would inflate false-positive recurrence
            // later. A zero fingerprint (e.g., silent span with no feature
            // signal) is also rejected by `AdCatalogStore.insert` but we
            // short-circuit the call to avoid touching SQLite needlessly.
            if let adCatalogStore,
               policyAction == .autoSkipEligible,
               !spanFingerprint.isZero {
                do {
                    _ = try await adCatalogStore.insert(
                        showId: catalogShowId,
                        episodePosition: .unknown,
                        durationSec: max(0, refinedSpan.endTime - refinedSpan.startTime),
                        acousticFingerprint: spanFingerprint,
                        transcriptSnippet: nil,
                        sponsorTokens: nil,
                        originalConfidence: decision.skipConfidence
                    )
                    logger.debug("Backfill: inserted catalog entry for autoSkipEligible span \(refinedSpan.id, privacy: .public) show=\(catalogShowId ?? "<none>", privacy: .public)")
                } catch {
                    logger.warning("Backfill: catalog insert failed for span \(refinedSpan.id, privacy: .public): \(error.localizedDescription, privacy: .public)")
                }
            }

            // playhead-43ed: B3 RepeatedAdCache ingress. Same trigger
            // condition as the catalog insert above (`autoSkipEligible`
            // backfill verdict), but writes to the per-show RepeatedAdCache
            // so the next episode of THIS show can short-circuit the
            // classifier on a fingerprint hit. Three guards:
            //   1. Service wired (nil-default in init).
            //   2. Non-empty showId — cache rows are per-show-scoped; an
            //      anonymous span has nowhere to land.
            //   3. RepeatedAdFingerprint derives non-zero from the span's
            //      feature windows. The cache uses a different fingerprint
            //      kind than `AdCatalogStore` (128-bit median-binarized
            //      vs the legacy 64-bit catalog fingerprint), so we
            //      recompute here rather than reusing `spanFingerprint`.
            //   4. Confidence ≥ store threshold is enforced inside
            //      `RepeatedAdCacheService.store` so we don't duplicate
            //      it here.
            // Errors are swallowed — a cache write failure must never bring
            // down the backfill pipeline; missed cache writes degrade to
            // a future cache miss, never to a wrong skip.
            if let repeatedAdCache,
               policyAction == .autoSkipEligible,
               let cacheShowId = catalogShowId,
               !cacheShowId.isEmpty {
                let repeatedFp = RepeatedAdFingerprint.from(
                    featureWindows: spanFeatureWindows
                )
                if !repeatedFp.isZero {
                    do {
                        _ = try await repeatedAdCache.store(
                            showId: cacheShowId,
                            fingerprint: repeatedFp,
                            boundaryStart: refinedSpan.startTime,
                            boundaryEnd: refinedSpan.endTime,
                            confidence: decision.skipConfidence
                        )
                    } catch {
                        logger.warning("RepeatedAdCache: store failed for span \(refinedSpan.id, privacy: .public): \(error.localizedDescription, privacy: .public)")
                    }
                }
            }

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

            // Accumulate DecisionEvent for step 16. Bug 6: cohortJSON is hoisted
            // to asset scope (`assetCohortJSON`) so the per-asset
            // `ad_decision_results` row and every per-window `decision_events` row
            // serialize the same cohort string.
            let cohortJSON = assetCohortJSON
            // playhead-ef2.1.4: build structured explanation trace from ledger + decision
            let explanation = DecisionExplanation.build(
                ledger: ledger,
                decision: decision,
                policyAction: policyAction,
                config: fusionConfig,
                skipThreshold: autoSkipThreshold
            )
            let explanationJSON = (try? JSONEncoder().encode(explanation))
                .flatMap { String(data: $0, encoding: .utf8) }

            // Capture a single wall-clock once so DecisionEvent and
            // DecisionLogEntry share the exact same timestamp (previously
            // each called Date() independently, diverging by microseconds).
            let decisionTimestamp = Date().timeIntervalSince1970

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
                createdAt: decisionTimestamp,
                explanationJSON: explanationJSON
            ))

            // playhead-8em9 (narL): emit per-window DecisionLogEntry for
            // offline replay. Resolves MetadataActivationConfig so the
            // snapshot matches the gated consumers' view at this decision.
            let logEntry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: decisionTimestamp,
                windowBounds: .init(
                    start: refinedSpan.startTime,
                    end: refinedSpan.endTime
                ),
                activationConfig: .init(MetadataActivationConfig.resolved()),
                evidence: effectiveLedger.map(DecisionLogEntry.LedgerEntry.init),
                fusedConfidence: .init(
                    proposalConfidence: decision.proposalConfidence,
                    skipConfidence: decision.skipConfidence,
                    breakdown: explanation.evidenceBreakdown
                ),
                finalDecision: .init(
                    action: policyAction.rawValue,
                    gate: decision.eligibilityGate.rawValue,
                    skipConfidence: decision.skipConfidence,
                    thresholdCrossed: autoSkipThreshold
                )
            )
            await decisionLogger.record(logEntry)
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

        // ── Step 16.5: Persist the per-asset DecisionResultArtifact ──────────
        // Bug 6: prior to this block, `ad_decision_results` rows were never
        // written by production code (only by tests), even though every other
        // artifact of the backfill pipeline lands in SQLite. Building it here
        // — after `decision_events` are flushed and before SkipOrchestrator
        // forwarding — gives downstream eval/replay a single canonical roll-up
        // per asset that matches the events that were just persisted.
        //
        // The UNIQUE(analysisAssetId) constraint plus `INSERT OR REPLACE` in
        // `saveDecisionResultArtifact` makes this idempotent across re-runs
        // (e.g. cohort recomputes after a settings change). We persist even
        // when `fusionDecisionResults` is empty so that "no ads found" is
        // representable as `decisionJSON == "[]"` rather than a missing row;
        // a missing row is ambiguous (never analysed vs analysed-with-zero).
        //
        // L5 (skeptical-review-cycle-1): two distinct "empty" axes —
        //   1. `decodedSpans.isEmpty`     — upstream phases produced no
        //      anchored evidence at all. We treat this as "not analysed
        //      enough to summarise" and skip writing the row so a future
        //      run with richer transcript coverage can produce the
        //      canonical artifact.
        //   2. `fusionDecisionResults.isEmpty` — spans existed but fusion
        //      confirmed no ads. We DO write the row with `decisionJSON
        //      == "[]"` so downstream readers can distinguish
        //      analysed-with-zero from never-analysed.
        // The guard below is on (1), not (2) — the comment above describes
        // (2)'s behaviour inside the guard.
        if !decodedSpans.isEmpty {
            let inputArtifactRefs = (try? JSONEncoder().encode(fusionWindows.map(\.id)))
                .flatMap { String(data: $0, encoding: .utf8) } ?? "[]"
            let decisionJSON = (try? JSONEncoder().encode(
                fusionDecisionResults.map(PersistedDecisionResult.init(_:))
            )).flatMap { String(data: $0, encoding: .utf8) } ?? "[]"
            let artifact = DecisionResultArtifact(
                id: UUID().uuidString,
                analysisAssetId: analysisAssetId,
                decisionCohortJSON: assetCohortJSON,
                inputArtifactRefs: inputArtifactRefs,
                decisionJSON: decisionJSON,
                createdAt: Date().timeIntervalSince1970
            )
            do {
                try await store.saveDecisionResultArtifact(artifact)
            } catch {
                logger.warning("Backfill: saveDecisionResultArtifact failed for asset \(analysisAssetId): \(error.localizedDescription)")
            }
        }

        // ── Step 17: Forward eligible decisions to SkipOrchestrator ──────────
        // Phase 6.5 (playhead-4my.16): wires fusion output to the orchestrator so
        // Phase 7 (UserCorrections) has banner impressions + skip cues to correct.
        // The orchestrator guards on activeAssetId and eligibilityGate internally.
        if let orchestrator = skipOrchestrator {
            // playhead-xr3t: push the inventory-sanity-filter context
            // (declared chapters + episode duration) to the orchestrator
            // BEFORE forwarding the fusion decisions, so the filter has
            // the up-to-date context when each span is evaluated. The
            // orchestrator drops mismatched-asset pushes silently
            // (asset-switch race guard), so an out-of-order delivery
            // during an episode change is benign.
            //
            // We push unconditionally even when `fusionDecisionResults`
            // is empty: a future hot-path push using the AdWindow path
            // can still benefit from refreshed chapter / duration
            // state, and the setters are pure assignment + a guard
            // check (cheap).
            await orchestrator.setDeclaredChapters(
                assetChapterEvidence,
                analysisAssetId: analysisAssetId
            )
            await orchestrator.setEpisodeDuration(
                episodeDuration,
                analysisAssetId: analysisAssetId
            )

            if !fusionDecisionResults.isEmpty {
                await orchestrator.receiveAdDecisionResults(fusionDecisionResults)
                let eligibleCount = fusionDecisionResults.filter { $0.eligibilityGate == .eligible }.count
                logger.info("Backfill: forwarded \(fusionDecisionResults.count) fusion results (\(eligibleCount) eligible) to SkipOrchestrator")
            }
        }

        // ── Post-pipeline: priors + coverage watermark ────────────────────────

        if podcastId.isEmpty {
            logger.info("Backfill: skipping priors update — missing podcastId for asset \(analysisAssetId)")
        } else {
            try await updatePriors(
                podcastId: podcastId,
                nonSuppressedWindows: nonSuppressedWindows,
                episodeDuration: episodeDuration,
                featureWindows: featureWindows,
                chunks: finalChunks
            )
        }

        if let maxEnd = nonSuppressedWindows.map(\.endTime).max() {
            try await store.updateConfirmedAdCoverage(
                id: analysisAssetId,
                endTime: maxEnd
            )
        }

        logger.info("Backfill complete: spans=\(decodedSpans.count) fusion_windows=\(fusionWindows.count) decision_events=\(decisionEvents.count)")

        // playhead-gtt9.16: one-line acoustic-pipeline funnel summary per
        // episode. Emits the per-stage totals so gtt9.3 calibration can see
        // which features are producing signal and passing the fusion gate
        // without scraping per-window logs.
        let funnel = acousticPipelineResult.funnel
        logger.info(
            "Backfill acoustic-pipeline funnel: computed=\(funnel.total(.computed)) producedSignal=\(funnel.total(.producedSignal)) passedGate=\(funnel.total(.passedGate)) includedInFusion=\(funnel.total(.includedInFusion))"
        )

        // playhead-arf8: per-run bracket-refinement cascade counts. Greppable
        // marker `[arf8]` lets dogfood verify activation is firing and which
        // gate is shedding spans without scraping per-window logs.
        let arf8Counts = lastBracketRefinementCounts
        logger.info(
            "[arf8] backfill bracket counts: refined=\(arf8Counts.bracketRefined) noBracket=\(arf8Counts.noBracket) trustGated=\(arf8Counts.trustGated) coarseGated=\(arf8Counts.coarseGated) fineGated=\(arf8Counts.fineConfidenceGated) legacyBypass=\(arf8Counts.legacyBypass) showTrust=\(String(format: "%.2f", bracketShowTrust))"
        )

        // playhead-2hpn (write path): once per episode, after all spans
        // have been scored and persisted, push this episode's intro/outro
        // jingle hashes into the show's profile. Gated by the same
        // conditions as the read path so flag-OFF behavior is byte
        // identical to pre-2hpn (no store mutation, no allocation).
        //
        // Cross-show isolation: `podcastId` is the bead-spec "per show"
        // key — the store records this outcome ONLY against that show's
        // row. Show A's profile is never touched by Show B's runBackfill.
        //
        // We log a single `[2hpn]` marker so dogfood can grep activation
        // and see the post-update confirmation count / miss count without
        // scraping per-window logs.
        if scopedMusicBedEnabled,
           !podcastId.isEmpty,
           let store = showMusicBedProfileStore,
           episodeDuration > 0 {
            let outcome = ShowMusicBedProfileEvaluator.extractEpisodeJingleHashes(
                featureWindows: featureWindows,
                episodeDuration: episodeDuration
            )
            let updated = await store.recordEpisodeOutcome(
                showIdentifier: podcastId,
                outcome: outcome,
                now: Date()
            )
            logger.info(
                "[2hpn] show=\(podcastId, privacy: .public) confirmed=\(updated.isConfirmed) confirmationCount=\(updated.confirmationCount) missCount=\(updated.consecutiveMissCount) storedHashes=\(updated.confirmedJingleHashes.count)"
            )
        }

        // playhead-zx6i — Success stamp for the B4 revalidation
        // short-circuit. ONLY runs when the flag is ON; flag-OFF
        // behavior is byte-identical to pre-zx6i (no UserDefaults
        // write, no allocation). The stamp records that this asset's
        // persisted `AdWindow` / classifier / feature rows are
        // up-to-date with the current `PipelineVersions` triple. On
        // the next `AnalysisJobRunner.run` for this asset, if the
        // current triple still matches, the runner takes the existing
        // skip-hot-path / skip-backfill no-op branches; if the triple
        // has bumped, the runner takes the revalidation short-circuit
        // (skipping decode / features / ASR) and routes through
        // `revalidateFromFeatures`. Stamped only here — at the end of
        // a successful `runBackfill`, after every stage wrote — so a
        // failure that returns/throws earlier leaves the prior stamp
        // (or absent state) intact and the next run will redo the
        // work rather than incorrectly trust an aborted run.
        if preAnalysisConfig.b4RevalidationFromFeaturesEnabled {
            RevalidationStateStore.recordCompleted(
                versions: PipelineVersions.current(),
                forAsset: analysisAssetId
            )
        }
    }

    /// playhead-zx6i — B4 fast revalidation entry point.
    ///
    /// Fetches the persisted `TranscriptChunk` rows for `analysisAssetId`
    /// and delegates to `runBackfill`, which already (a) accepts chunks
    /// as a parameter rather than re-running ASR, and (b) fetches its
    /// own `FeatureWindow` rows from the store internally. The
    /// short-circuit therefore comes from the CALLER (the
    /// `AnalysisJobRunner.run` branch that skips stages 1–3); this
    /// method's job is to provide a clean named entry point so the
    /// stub-based unit tests can assert "the runner took the
    /// revalidation path, not the full-analysis path" without grovelling
    /// through `runBackfill` call-site internals.
    ///
    /// If persisted chunks are empty the call returns without doing
    /// any classifier work — the caller's gate already checked for
    /// non-empty chunks, but defense-in-depth here keeps a
    /// chunk-races-deletion path from triggering a no-op classifier
    /// sweep. The `runBackfill` body's own `guard !chunks.isEmpty` is
    /// the structural fallback.
    func revalidateFromFeatures(
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String? = nil
    ) async throws {
        let chunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
        guard !chunks.isEmpty else {
            logger.info("Revalidation: skipping — no persisted chunks for asset \(analysisAssetId)")
            return
        }
        logger.info("Revalidation: re-running classifier+fusion+boundary over \(chunks.count) persisted chunks for asset \(analysisAssetId)")
        try await runBackfill(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration,
            sessionId: sessionId
        )
    }

    // MARK: - ChapterGenerationPhase wire-up (playhead-au2v.1.13)

    /// Invoke the chapter-generation phase from `runBackfill` when a
    /// production factory has been wired in. The mode gate is checked
    /// at the call site (only `.shadow` and `.enabled` reach this
    /// helper); the cache short-circuit, factory presence, and outcome
    /// logging live here.
    ///
    /// `transcriptVersion` is the same hash `TranscriptAtomizer.atomize`
    /// computed for the current final-pass atom set. We use it as the
    /// cache key for the short-circuit on the assumption that production
    /// wiring installs a `TranscriptHashProviding` whose output matches
    /// `transcriptVersion` for the same atom set — that contract is what
    /// makes "valid plan exists for content hash → skip phase" coherent.
    ///
    /// Tests intentionally diverge the two (e.g. by injecting a
    /// `.race(entry:recheck:)` or `.unavailable` hash provider that the
    /// service-level short-circuit knows nothing about) to exercise the
    /// phase's internal protections in isolation; that's a feature of
    /// the test seam, not a contract violation. Production wiring is
    /// responsible for keeping the two keys aligned (see runtime bead).
    ///
    /// Outcomes are logged via the actor's `logger` and never thrown:
    /// the phase already emits structured `ChapterPhaseEvent`s for each
    /// terminal state, so a re-throw here would force every backfill
    /// caller to add a `try` site for what is, in product terms, an
    /// optional signal generator. Cancellation is the one exception: a
    /// `Task.isCancelled` check after this helper returns lets upstream
    /// callers observe cancellation through the existing `try`
    /// machinery in `runBackfill` (the caller-side `try
    /// Task.checkCancellation()` handles propagation; the phase itself
    /// returns `.preempted` and never raises).
    ///
    /// `transcriptVersion` empty (e.g. no atoms produced) is a no-op:
    /// the hash isn't a useful cache key in that case and the phase's
    /// `TranscriptHashProviding` would itself surface
    /// `.transcriptUnavailable`. Bailing here saves the round-trip.
    private func runChapterGenerationPhaseIfWired(
        analysisAssetId: String,
        transcriptVersion: String
    ) async {
        // Cancellation pre-check. The call site re-checks
        // `Task.checkCancellation()` after this helper returns, but if
        // the parent task was already cancelled when we got here, we
        // would otherwise still pay for the factory build, the cache
        // get, and the phase entry just to have the phase observe
        // cancellation and return `.preempted`. Bailing now skips that
        // wasted work and matches the "honor cancellation early" pattern
        // used elsewhere in `runBackfill`.
        guard !Task.isCancelled else {
            logger.debug(
                "chapterphase.backfill_wireup: cancellation observed at entry for asset=\(analysisAssetId, privacy: .public) — skipping phase"
            )
            return
        }

        // Empty transcript hash → nothing to cache against, nothing
        // for the phase to anchor on. Logged at `.debug` so dogfood
        // OS log searches can confirm the gate fired without polluting
        // the default-level logs.
        guard !transcriptVersion.isEmpty else {
            logger.debug(
                "chapterphase.backfill_wireup: empty transcriptVersion for asset=\(analysisAssetId, privacy: .public) — skipping phase"
            )
            return
        }

        // Factory not wired → mode is on but no production dependencies
        // exist yet. Mode-gate ensures we only get here on `.shadow` or
        // `.enabled`; logging at `.debug` keeps the gap observable
        // without alarming dogfood reports.
        guard let factory = chapterGenerationPhaseFactory else {
            logger.debug(
                "chapterphase.backfill_wireup: mode=\(self.config.chapterSignalMode.rawValue, privacy: .public) but no phase factory wired — skipping"
            )
            return
        }

        // Cache short-circuit. A non-nil cache + a hit on the current
        // content hash is a "fresh plan already on disk" signal — we
        // refuse to pay FM cost re-running the phase for an unchanged
        // input. The cache returns `nil` on schema mismatch / decode
        // failure / missing file (all treated as misses by
        // `ChapterPlanCache.get`), which correctly falls through to
        // re-running the phase. We deliberately do NOT emit a
        // diagnostic for the cache hit — the original phase run that
        // produced the plan already emitted `chapter_phase_completed`,
        // and re-emitting on every replay would over-count plan
        // generations in eval. The phase's own internal cache write
        // path is unchanged.
        if let cache = chapterPlanCache {
            if let cachedPlan = await cache.get(contentHash: transcriptVersion) {
                logger.debug(
                    "chapterphase.backfill_wireup: cache hit for asset=\(analysisAssetId, privacy: .public) hash=\(transcriptVersion, privacy: .public) chapters=\(cachedPlan.chapters.count, privacy: .public) — skipping phase"
                )
                return
            }
        }

        // Cache miss (or no cache wired) → invoke the phase. Production
        // wiring rebuilds the phase per call so the
        // `TranscriptHashProviding` it captures observes the
        // most-recently-published transcript hash.
        //
        // Resolve the actual episode identifier from the analysis asset.
        // The phase API takes `episodeId: String` and uses it as (a) the
        // key for `creatorChapterProvider.creatorChapters(episodeId:)`
        // — production providers look up creator chapters by EPISODE,
        // not by re-analysis ASSET — and (b) the privacy-locked input
        // to `EpisodeIdHasher.hash(installID:episodeId:)`, which must
        // hash the same episode across re-analysis attempts so dogfood
        // diagnostics correlate events from the same listener session.
        // Passing `analysisAssetId` here would break both: the provider
        // would miss real chapters, and the hash would shift per
        // re-analysis. Falls back to `analysisAssetId` only when the
        // store lookup fails (deleted asset row, transient SQLite
        // error) — better to emit a phase event keyed to the asset
        // than to skip the phase entirely. The fallback is logged so
        // dogfood bundles surface the lookup miss.
        let resolvedEpisodeId: String
        do {
            if let asset = try await store.fetchAsset(id: analysisAssetId) {
                resolvedEpisodeId = asset.episodeId
            } else {
                logger.notice(
                    "chapterphase.backfill_wireup: asset row missing for asset=\(analysisAssetId, privacy: .public) — falling back to analysisAssetId as episodeId"
                )
                resolvedEpisodeId = analysisAssetId
            }
        } catch {
            logger.error(
                "chapterphase.backfill_wireup: asset lookup failed for asset=\(analysisAssetId, privacy: .public) — falling back to analysisAssetId as episodeId, error=\(String(describing: error), privacy: .public)"
            )
            resolvedEpisodeId = analysisAssetId
        }

        let phase = factory()
        let installID = chapterPhaseInstallIDProvider()
        let outcome = await phase.run(
            mode: config.chapterSignalMode,
            episodeId: resolvedEpisodeId,
            installID: installID
        )

        // One-line outcome log per run. The structured event payload is
        // already emitted by the phase via its `ChapterPhaseEventSink`;
        // this string is for greppable dogfood diagnostics.
        switch outcome {
        case .modeOff:
            // Unreachable here — call site checks
            // `runsChapterGeneration` before invoking the helper. Logged
            // at `.error` because reaching this branch indicates the
            // phase's internal mode handling diverged from the gate's
            // predicate (a bug in either the phase or in
            // `ChapterSignalMode.runsChapterGeneration`). The error log
            // gives a single greppable signal during dogfood without
            // crashing release builds; in DEBUG we trip an
            // `assertionFailure` so the desync is loud at test time.
            logger.error(
                "chapterphase.backfill_wireup: unexpected modeOff outcome for asset=\(analysisAssetId, privacy: .public) — phase/gate desync"
            )
            assertionFailure(
                "chapterphase.backfill_wireup: phase returned .modeOff while ChapterSignalMode.runsChapterGeneration was true — gate/phase desync"
            )
        case .admissionDenied(let reason):
            logger.notice(
                "chapterphase.backfill_wireup: admission denied for asset=\(analysisAssetId, privacy: .public) reason=\(reason, privacy: .public)"
            )
        case .skippedCreatorChapters(let creatorChapterCount):
            logger.notice(
                "chapterphase.backfill_wireup: skipped — \(creatorChapterCount, privacy: .public) creator chapters present for asset=\(analysisAssetId, privacy: .public)"
            )
        case .noCandidates:
            logger.notice(
                "chapterphase.backfill_wireup: no candidates for asset=\(analysisAssetId, privacy: .public)"
            )
        case .transcriptUnavailable:
            logger.notice(
                "chapterphase.backfill_wireup: transcript unavailable for asset=\(analysisAssetId, privacy: .public)"
            )
        case .raceAborted:
            logger.notice(
                "chapterphase.backfill_wireup: transcript race for asset=\(analysisAssetId, privacy: .public) — plan discarded"
            )
        case .preempted:
            logger.notice(
                "chapterphase.backfill_wireup: preempted for asset=\(analysisAssetId, privacy: .public)"
            )
        case .operationalRateExceeded(let rate, let threshold):
            logger.notice(
                "chapterphase.backfill_wireup: op-rate exceeded for asset=\(analysisAssetId, privacy: .public) rate=\(rate, privacy: .public) threshold=\(threshold, privacy: .public)"
            )
        case .cached(let chapterCount, let planConfidence):
            // Phase-level `.cached` is the SUCCESS terminal: the phase
            // ran end-to-end, generated a plan, and persisted it via
            // its internal cache write. Not to be confused with the
            // service-level cache short-circuit above, which logs at
            // `.debug` ("cache hit … skipping phase") because no phase
            // run occurred.
            logger.info(
                "chapterphase.backfill_wireup: plan generated and cached for asset=\(analysisAssetId, privacy: .public) chapters=\(chapterCount, privacy: .public) confidence=\(planConfidence, privacy: .public)"
            )
        }
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
        metadataEntries: [EvidenceLedgerEntry] = [],
        acousticPipelineFusion: [AcousticFeatureFusion.WindowFusion] = [],
        acousticBreaks: [AcousticBreak] = [],
        catalogMatchSimilarity: Float = 0,
        fusionConfig: FusionWeightConfig,
        // playhead-2hpn: when both are non-nil/positive AND the flag is
        // on, the music-bed evaluator switches to fixed weights
        // (0.10 baseline / 0.25 jingle-overlap). `nil` when the flag is
        // off — preserves byte-identical pre-2hpn fusion output.
        showMusicBedSnapshot: ShowMusicBedProfileSnapshot? = nil,
        episodeDuration: Double = 0
    ) -> [EvidenceLedgerEntry] {
        // Classifier entry: find the best-matching ClassifierResult for this span.
        let classifierScore = bestClassifierScore(
            for: span,
            results: classifierResults
        )

        // FM entries: positive-only, mode-gated, from persisted scan results.
        // Cycle 1 H2: use the effective (registry-intersected) mode so a
        // demoted cohort does not fold FM evidence into the ledger.
        let fmEntries = buildFMLedgerEntries(
            span: span,
            scanResults: semanticScanResults,
            mode: effectiveFMBackfillMode,
            fusionConfig: fusionConfig
        )

        // Lexical entries: from LexicalCandidates overlapping the span.
        let lexicalEntries = buildLexicalLedgerEntries(
            span: span,
            candidates: lexicalCandidates,
            fusionConfig: fusionConfig
        )

        // Acoustic entries: from FeatureWindows in the span range.
        // playhead-fqc8: pass `acousticBreaks` so a `.classifierSeed`-anchored
        // span with an aligned break also gets a `.breakAlignment` entry,
        // which is the gate for `PromotionTrack.classifierSeedQualified`.
        let acousticAndAlignmentEntries = buildAcousticLedgerEntries(
            span: span,
            featureWindows: featureWindows,
            fusionConfig: fusionConfig,
            acousticBreaks: acousticBreaks
        )
        // playhead-fqc8 cycle-1 review HIGH-1: the helper returns RMS-drop
        // `.acoustic` and the alignment corroborator (`.breakAlignment`)
        // mixed in one list. Split them by source kind here so each family
        // flows into its own dedicated parameter on
        // `BackfillEvidenceFusion`, giving each family its own honest
        // weight cap.
        let acousticEntries = acousticAndAlignmentEntries.filter { $0.source != .breakAlignment }
        let breakAlignmentEntries = acousticAndAlignmentEntries.filter { $0.source == .breakAlignment }

        // 2026-04-23 Finding 4: music-bed coverage produces its own
        // `.musicBed` ledger entry (distinct EvidenceSourceType) so
        // the quorum gate's `distinctKinds.count` increments when a
        // span has both an RMS-drop edge and an interior bed.
        // playhead-2hpn: when the scoped-music-bed-generalization flag
        // is ON (signalled by a non-nil `showMusicBedSnapshot`), the
        // evaluator returns a boosted weight (0.25) for spans that
        // overlap a detected jingle on a confirmed show, or 0.10
        // baseline otherwise. Flag-off path leaves the evaluator on its
        // legacy presenceFraction*acousticCap math.
        let musicBedEntries = buildMusicBedLedgerEntries(
            span: span,
            featureWindows: featureWindows,
            fusionConfig: fusionConfig,
            showMusicBedSnapshot: showMusicBedSnapshot,
            episodeDuration: episodeDuration
        )
        // musicBed entries are merged into the acousticEntries list
        // passed to BackfillEvidenceFusion. The fusion code already
        // iterates over acousticEntries and preserves each entry's
        // `source`, so a `.musicBed`-sourced entry flows through with
        // the correct kind and increments distinctKinds.count.
        //
        // playhead-gtt9.16: also add aggregated `.acoustic` entries from
        // the acoustic feature pipeline output. When the pipeline produced
        // zero combined mass over the span (features all returned 0), the
        // helper returns empty, preserving pre-wire back-compat.
        let pipelineAcousticEntries = buildAcousticPipelineLedgerEntries(
            span: span,
            pipelineFusion: acousticPipelineFusion,
            fusionConfig: fusionConfig
        )
        let combinedAcousticEntries = acousticEntries + musicBedEntries + pipelineAcousticEntries

        // Catalog entries: from EvidenceEntry items overlapping the span.
        var catalogLedgerEntries = buildCatalogLedgerEntries(
            span: span,
            entries: catalogEntries,
            fusionConfig: fusionConfig
        )

        // playhead-gtt9.17: add a catalog ledger entry from the
        // `AdCatalogStore` match similarity when a prior stored ad creative
        // fingerprint-matches this span above the default floor. The weight
        // is scaled by similarity so a near-perfect match (≈1.0) gets full
        // `fusionConfig.catalogCap` mass and borderline matches (≈0.8) get
        // proportionally less. `entryCount: 1` preserves the existing
        // `.catalog(entryCount:)` detail variant — the fusion distinctKinds
        // gate only cares about the source type, not the count.
        if catalogMatchSimilarity >= AdCatalogStore.defaultSimilarityFloor {
            let weight = Double(catalogMatchSimilarity) * fusionConfig.catalogCap
            catalogLedgerEntries.append(EvidenceLedgerEntry(
                source: .catalog,
                weight: weight,
                detail: .catalog(entryCount: 1),
                // playhead-epfk: stamp the cross-episode `AdCatalogStore`
                // fingerprint match so NARL replay can attribute this entry
                // to the correction-loop channel (vs. the transcript token
                // catalog which uses `.transcriptCatalog`). The raw
                // similarity that produced this weight is also persisted
                // on `AdWindow.catalogStoreMatchSimilarity` for direct
                // inspection in the corpus export.
                subSource: .fingerprintStore
            ))
        }

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: classifierScore,
            fmEntries: fmEntries,
            lexicalEntries: lexicalEntries,
            acousticEntries: combinedAcousticEntries,
            catalogEntries: catalogLedgerEntries,
            metadataEntries: metadataEntries,
            breakAlignmentEntries: breakAlignmentEntries,
            // Cycle 1 H2: effective mode so fusion's `contributesToExistingCandidateLedger`
            // gate honors the registry's decision for this cohort.
            mode: effectiveFMBackfillMode,
            config: fusionConfig
        )
        return fusion.buildLedger()
    }

    // MARK: - FM Suppression (Phase ef2.4.6)

    /// Apply targeted FM suppression to a ledger when FM strongly says noAds.
    ///
    /// Builds FMSuppressionWindow entries from overlapping scan results, evaluates
    /// the suppression guard, and applies downweighting if all guards pass.
    private func applyFMSuppression(
        span: DecodedSpan,
        ledger: [EvidenceLedgerEntry],
        semanticScanResults: [SemanticScanResult]
    ) -> FMSuppressionResult {
        // Build suppression windows from FM scan results overlapping this span.
        let overlappingWindows: [FMSuppressionWindow] = semanticScanResults.compactMap { result in
            let overlapStart = max(span.startTime, result.windowStartTime)
            let overlapEnd = min(span.endTime, result.windowEndTime)
            guard overlapEnd > overlapStart else { return nil }

            let band: CertaintyBand = result.transcriptQuality == .good ? .moderate : .weak
            return FMSuppressionWindow(
                disposition: result.disposition,
                band: band
            )
        }

        let guard_ = FMSuppressionGuard(
            overlappingFMResults: overlappingWindows,
            ledger: ledger,
            anchorProvenance: span.anchorProvenance
        )
        let guardResult = guard_.evaluate()

        let applicator = FMSuppressionApplicator()
        return applicator.apply(guardResult: guardResult, ledger: ledger)
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
    /// ef2.4.5: Minimal decode struct for extracting (commercialIntent, ownership)
    /// from `SemanticScanResult.spansJSON`. Mirrors the encoding in
    /// `BackfillJobRunner.EncodedRefinedSpan` but decodes only the two fields
    /// needed for `ClassificationTrustMatrix` lookup.
    private struct SpanTrustDecode: Decodable {
        let commercialIntent: String
        let ownership: String
    }

    /// ef2.4.5: Extract the dominant classificationTrust from a scan result's spansJSON.
    /// Decodes the refined spans, maps each to a trust value, and returns the maximum
    /// (most commercially confident span wins). Returns 1.0 if spansJSON is empty or
    /// cannot be decoded (backward-compatible default).
    private func classificationTrust(from spansJSON: String) -> Double {
        guard let data = spansJSON.data(using: .utf8),
              let spans = try? JSONDecoder().decode([SpanTrustDecode].self, from: data),
              !spans.isEmpty else {
            return 1.0
        }

        return spans.map { span in
            let intent = CommercialIntent(rawValue: span.commercialIntent) ?? .unknown
            let owner = Ownership(rawValue: span.ownership) ?? .unknown
            return ClassificationTrustMatrix.trust(commercialIntent: intent, ownership: owner)
        }.max() ?? 1.0
    }

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

            // ef2.4.5: look up classificationTrust from refinement data in spansJSON.
            let trust = classificationTrust(from: result.spansJSON)

            return EvidenceLedgerEntry(
                source: .fm,
                weight: weight,
                detail: .fm(
                    disposition: .containsAd,
                    band: band,
                    cohortPromptLabel: result.scanCohortJSON
                ),
                classificationTrust: trust
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
    ///
    /// `.acoustic` captures the audio-energy break (RMS-drop) signal at
    /// span boundaries. MusicBed is captured separately by
    /// `MusicBedLedgerEvaluator`'s `.musicBed` entry. Both can co-emit
    /// on the same span when both signals are present, contributing as
    /// physically independent evidence kinds (boundary energy shift vs.
    /// sustained interior music coverage).
    ///
    /// playhead-sqhj history: a 2026-04-26 follow-up to gtt9.4 briefly
    /// fused music-bed coverage into this method's combined strength.
    /// Cross-review caught that the music-bed signal already reaches
    /// production via `MusicBedLedgerEvaluator`, so emitting `.acoustic`
    /// on a music-bed-only span double-counted the same physical
    /// evidence into the quorum gate's `distinctKinds.count`. The fused
    /// path was reverted; `.acoustic` once again fires only when
    /// `breakStrength > 0`.
    ///
    /// playhead-fqc8: Optionally also emits a `source: .breakAlignment`
    /// entry when (a) the span has a `.classifierSeed` anchor in its
    /// provenance AND (b) at least one `AcousticBreak` lies within
    /// `breakAlignmentTolerance` (±2.0s) of either span boundary AND
    /// (c) that break's `breakStrength` is `>= breakAlignmentMinStrength`
    /// (0.5). The alignment entry is what gates
    /// `PromotionTrack.classifierSeedQualified` in
    /// `DecisionMapper.computePromotionTrack`. Pass `acousticBreaks: []`
    /// (the default) to preserve pre-fqc8 behavior at every existing
    /// call site.
    ///
    /// playhead-fqc8 cycle-1 review: the alignment entry now uses the
    /// dedicated `EvidenceSourceType.breakAlignment` kind (not
    /// `.acoustic` + `.subSource = .breakAlignment`) so the alignment
    /// evidence is capped against `FusionWeightConfig.breakAlignmentCap`
    /// — its own honest budget — instead of stealing from the acoustic
    /// family budget. Callers that want the entries split by family
    /// must filter on `source` (see `buildEvidenceLedger`).
    func buildAcousticLedgerEntries(
        span: DecodedSpan,
        featureWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig,
        acousticBreaks: [AcousticBreak] = []
    ) -> [EvidenceLedgerEntry] {
        let spanWindows = featureWindows.filter { fw in
            fw.startTime < span.endTime && fw.endTime > span.startTime
        }

        var entries: [EvidenceLedgerEntry] = []
        if !spanWindows.isEmpty {
            let breakStrength = RegionScoring.computeRmsDropScore(windows: spanWindows)
            if breakStrength > 0 {
                let weight = min(breakStrength * fusionConfig.acousticCap, fusionConfig.acousticCap)
                entries.append(EvidenceLedgerEntry(
                    source: .acoustic,
                    weight: weight,
                    detail: .acoustic(breakStrength: breakStrength)
                ))
            }
        }

        // playhead-fqc8: classifier-seed break-alignment corroborator.
        if let alignment = breakAlignmentEntry(
            for: span,
            acousticBreaks: acousticBreaks,
            fusionConfig: fusionConfig
        ) {
            entries.append(alignment)
        }

        return entries
    }

    /// playhead-fqc8: Tolerance window (seconds) for matching an
    /// `AcousticBreak` to either edge of a `.classifierSeed`-anchored
    /// span. Symmetric (±2.0s on each side).
    static let breakAlignmentTolerance: Double = 2.0

    /// playhead-fqc8: Minimum `AcousticBreak.breakStrength` required for
    /// the matched break to count as a corroborator. The 0.5 floor is
    /// the same threshold the boundary refiner uses to discriminate a
    /// useful cue from background noise.
    static let breakAlignmentMinStrength: Double = 0.5

    /// playhead-fqc8: Returns a `.breakAlignment` ledger entry when the
    /// span carries `.classifierSeed` provenance AND at least one
    /// `AcousticBreak` aligns to either edge with sufficient strength.
    ///
    /// playhead-fqc8 cycle-1 review M-4: the weight now scales with the
    /// matched break's `breakStrength` so a barely-passing 0.5 strength
    /// produces a 0.10 contribution while a clean 1.0 strength reaches
    /// the full `breakAlignmentCap` (default 0.20). The minimum-strength
    /// gate (0.5) is enforced before this method, so the dynamic-range
    /// floor is `0.5 × breakAlignmentCap`. Returns `nil` when any
    /// precondition fails.
    private func breakAlignmentEntry(
        for span: DecodedSpan,
        acousticBreaks: [AcousticBreak],
        fusionConfig: FusionWeightConfig
    ) -> EvidenceLedgerEntry? {
        guard !acousticBreaks.isEmpty else { return nil }
        let hasClassifierSeed = span.anchorProvenance.contains {
            if case .classifierSeed = $0 { return true }
            return false
        }
        guard hasClassifierSeed else { return nil }

        let tolerance = Self.breakAlignmentTolerance
        let minStrength = Self.breakAlignmentMinStrength

        let aligned = acousticBreaks
            .filter { brk in
                let nearStart = abs(brk.time - span.startTime) <= tolerance
                let nearEnd = abs(brk.time - span.endTime) <= tolerance
                return (nearStart || nearEnd) && brk.breakStrength >= minStrength
            }
            .max(by: { $0.breakStrength < $1.breakStrength })

        guard let matched = aligned else { return nil }

        // M-4 strength scaling: 0.5 → 0.10, 1.0 → 0.20 with the default
        // `breakAlignmentCap = 0.20`. Clamp the strength to [0, 1] so
        // an out-of-band detector value can't inflate the contribution
        // past the cap.
        let normalizedStrength = max(0.0, min(1.0, matched.breakStrength))
        let weight = min(
            normalizedStrength * fusionConfig.breakAlignmentCap,
            fusionConfig.breakAlignmentCap
        )
        return EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: weight,
            detail: .breakAlignment(breakStrength: matched.breakStrength)
        )
    }

    /// playhead-gtt9.16: build a single aggregated `.acoustic` ledger entry
    /// from the `AcousticFeaturePipeline` output that overlaps this span.
    ///
    /// The pipeline's per-window `combinedScore` is a weighted blend of the
    /// eight acoustic features with `AcousticFeatureFusion.Weights.defaultPriors`.
    /// We take the maximum across windows overlapping the span, multiply by
    /// `fusionConfig.acousticCap`, and return a single entry. Returns an
    /// empty array when:
    ///   * `pipelineFusion` is empty (no windows in the episode), or
    ///   * the maximum combined score across overlapping windows is zero
    ///     (all features returned zero — back-compat: no behaviour change
    ///     vs. pre-wiring).
    ///
    /// The entry uses `source: .acoustic` and encodes the combined score as
    /// `breakStrength` in the `.acoustic(...)` detail. Downstream
    /// `BackfillEvidenceFusion` caps each entry at `config.acousticCap`
    /// separately, so the pipeline contribution is additive to the existing
    /// RMS-drop `.acoustic` entry but each entry respects the same family
    /// budget. This matches gtt9.12's design (features are new evidence,
    /// not a replacement for the RMS-drop path).
    private func buildAcousticPipelineLedgerEntries(
        span: DecodedSpan,
        pipelineFusion: [AcousticFeatureFusion.WindowFusion],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        guard !pipelineFusion.isEmpty else { return [] }
        let overlapping = pipelineFusion.filter { fusion in
            fusion.windowStart < span.endTime && fusion.windowEnd > span.startTime
        }
        guard !overlapping.isEmpty else { return [] }
        let maxCombined = overlapping.map(\.combinedScore).max() ?? 0
        guard maxCombined > 0 else { return [] }
        let weight = min(maxCombined * fusionConfig.acousticCap, fusionConfig.acousticCap)
        return [EvidenceLedgerEntry(
            source: .acoustic,
            weight: weight,
            detail: .acoustic(breakStrength: maxCombined)
        )]
    }

    /// 2026-04-23 Finding 4: build `.musicBed`-source ledger entries
    /// from the span's interior `MusicBedLevel` coverage.
    ///
    /// Delegates the threshold/weight logic to the pure
    /// `MusicBedLedgerEvaluator`; this method is just the span-window
    /// filter + plumbing.
    ///
    /// playhead-2hpn: when `showMusicBedSnapshot` is non-nil (set by the
    /// caller when the `scopedMusicBedGeneralization` flag is on AND a
    /// profile resolver is wired AND the podcastId is non-empty), the
    /// evaluator runs the flag-on weighting (0.10/0.25); otherwise it
    /// preserves the legacy presenceFraction*acousticCap path
    /// byte-identically.
    private func buildMusicBedLedgerEntries(
        span: DecodedSpan,
        featureWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig,
        showMusicBedSnapshot: ShowMusicBedProfileSnapshot? = nil,
        episodeDuration: Double = 0
    ) -> [EvidenceLedgerEntry] {
        let spanWindows = featureWindows.filter { fw in
            fw.startTime < span.endTime && fw.endTime > span.startTime
        }
        let jingleBoost = makeJingleBoost(
            for: span,
            snapshot: showMusicBedSnapshot,
            episodeDuration: episodeDuration
        )
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: spanWindows,
            fusionConfig: fusionConfig,
            jingleBoost: jingleBoost
        )
        if let entry = result.entry {
            return [entry]
        }
        return []
    }

    /// playhead-2hpn: build the optional `JingleBoost` context for a
    /// single span.
    ///
    /// Returns `nil` (legacy fusion behavior) when:
    ///   * the show snapshot is missing (flag off, no podcastId, or no
    ///     profile recorded yet), OR
    ///   * the episode duration is non-positive (no overlap region can
    ///     be computed).
    ///
    /// Returns a non-nil `JingleBoost` otherwise. `isConfirmed` mirrors
    /// the snapshot's confirmation state; `spanOverlapsJingle` is
    /// `true` when the span overlaps `[0, jingleSliceSeconds)` or
    /// `[episodeDuration - jingleSliceSeconds, episodeDuration)` — the
    /// intro/outro slices the evaluator hashes against.
    private func makeJingleBoost(
        for span: DecodedSpan,
        snapshot: ShowMusicBedProfileSnapshot?,
        episodeDuration: Double
    ) -> MusicBedLedgerEvaluator.JingleBoost? {
        guard let snapshot, episodeDuration > 0 else { return nil }
        let overlaps = ShowMusicBedProfileEvaluator.spanOverlapsJingleRegion(
            spanStart: span.startTime,
            spanEnd: span.endTime,
            episodeDuration: episodeDuration
        )
        return MusicBedLedgerEvaluator.JingleBoost(
            isConfirmed: snapshot.isConfirmed,
            spanOverlapsJingle: overlaps
        )
    }

    /// Build catalog ledger entries from EvidenceEntry items overlapping the span.
    func buildCatalogLedgerEntries(
        span: DecodedSpan,
        entries: [EvidenceEntry],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        let overlapping = entries.filter { entry in
            // Repeated evidence expands its coverage window across the earliest
            // and latest occurrence, while startTime/endTime remain the
            // representative local hit used for display/fallback anchoring.
            entry.coverageStartTime < span.endTime && entry.coverageEndTime > span.startTime
        }
        guard !overlapping.isEmpty else { return [] }

        let weight = min(
            Double(overlapping.count) * 0.05 * fusionConfig.catalogCap,
            fusionConfig.catalogCap
        )
        return [EvidenceLedgerEntry(
            source: .catalog,
            weight: weight,
            detail: .catalog(entryCount: overlapping.count),
            // playhead-epfk: stamp the in-pipeline transcript-token catalog
            // so NARL replay can distinguish it from `AdCatalogStore`
            // fingerprint matches that share the `.catalog` source label.
            subSource: .transcriptCatalog
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
    /// playhead-epfk: `catalogStoreMatchSimilarity` carries the per-span
    /// top similarity from `AdCatalogStore.matches`. Pass `nil` when the
    /// catalog store was not wired or no match was attempted; `0` means
    /// "wired and queried but no match cleared the floor"; positive
    /// values surface in the corpus export so NARL can measure the
    /// fingerprint-store firing rate.
    private func buildFusionAdWindow(
        span: DecodedSpan,
        decision: DecisionResult,
        policyAction: SkipPolicyAction,
        analysisAssetId: String,
        catalogStoreMatchSimilarity: Double? = nil
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
            userDismissedBanner: false,
            // playhead-ux6r: stamp the eligibility gate on the persisted
            // fusion window. Without this, the live decision-time gate
            // (consumed via AdDecisionResult) is silently dropped on
            // restart, and SkipOrchestrator.beginEpisode preload re-arms
            // a previously-demoted markOnly span for auto-skip. The
            // value here is the same SkipEligibilityGate.rawValue that
            // decision_events records for the same window, keeping the
            // ad_windows row and decision_events row consistent.
            eligibilityGate: decision.eligibilityGate.rawValue,
            catalogStoreMatchSimilarity: catalogStoreMatchSimilarity
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
            userDismissedBanner: window.userDismissedBanner,
            evidenceSources: window.evidenceSources,
            eligibilityGate: window.eligibilityGate,
            // playhead-epfk: preserve catalog-store match similarity
            // across boundary refinement; this branch only adjusts time
            // bounds, never re-runs the AdCatalogStore query.
            catalogStoreMatchSimilarity: window.catalogStoreMatchSimilarity
        )
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
        // Cycle 1 H2: gate on effective mode so a known-bad cohort skips
        // the entire shadow phase rather than handing the runner a mode
        // that would have been demoted to .off downstream anyway.
        let resolvedMode = effectiveFMBackfillMode
        guard resolvedMode != .off else { return .skipped }

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

        // Cycle 1 H2: pass the effective mode so the runner persists scan
        // results stamped with the cohort-approved capability set.
        let runner = factory(store, resolvedMode)
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
            // playhead-4my.10.1: snapshot the evidence + decision +
            // correction ledger into `training_examples` while the
            // cohort is still warm. The materializer's failures are
            // surfaced via `logger.error` (so SQLite write failures are
            // visible in production) but NEVER propagated — shadow-mode
            // invariant applies (the FM phase must not affect cue
            // computation, even when materialization explodes).
            await materializeTrainingExamples(forAsset: analysisAssetId)
            if result.deferredJobIds.isEmpty {
                return wrap(.ranSucceeded, result.fmRefinementWindows)
            }
            return wrap(.ranNeedsRetry, result.fmRefinementWindows)
        } catch {
            // cycle-3 L3: `AnalysisStoreError` (and most other errors thrown
            // off the runner path) does NOT conform to `LocalizedError`, so
            // `error.localizedDescription` returns the bridged-NSError
            // boilerplate ("The operation couldn't be completed. (X error
            // N.)") with no detail. Use `String(describing:)` (which calls
            // `description`) to surface the actual case + payload, mirroring
            // the inner catch in `materializeTrainingExamples` ~25 lines
            // below.
            logger.warning("Shadow FM phase failed (suppressed by invariant): \(String(describing: error))")
            return wrap(.ranFailed)
        }
    }

    /// playhead-4my.10.1: post-fusion materialization hook. Called from
    /// `runShadowFMPhase` after a backfill run completes (regardless of
    /// `fmBackfillMode` — `runShadowFMPhase` runs in production whenever
    /// the mode is not `.off`).
    ///
    /// Failures must NOT propagate (the shadow-mode contract is that the
    /// FM phase never affects cue computation), but they also must not be
    /// silently dropped. The materializer touches SQLite directly — a
    /// disk-full / FK-violation / migration-mismatch is exactly the kind
    /// of error we need a server-visible log line for. We log at `error`
    /// level (not `warning`) so the line surfaces in production telemetry.
    private func materializeTrainingExamples(forAsset analysisAssetId: String) async {
        let materializer = TrainingExampleMaterializer()
        do {
            try await materializer.materialize(
                forAsset: analysisAssetId,
                store: store
            )
        } catch {
            // Persistence failure: log loudly. Suppression is the
            // shadow-contract requirement; silence is not.
            //
            // playhead-4my.10.1 (cycle-2 H-A): `AnalysisStoreError` conforms to
            // `Error`/`CustomStringConvertible` but NOT `LocalizedError`, so
            // `error.localizedDescription` returns the useless bridged string
            // ("The operation couldn't be completed. (Playhead.AnalysisStoreError
            // error N.)"). Use `String(describing:)` (which calls `description`)
            // and surface a stable case-name token when the error is one of
            // ours, mirroring the `BackfillJobRunner` pattern at line ~608.
            let detail = String(describing: error)
            if let storeError = error as? AnalysisStoreError {
                let caseName = BackfillJobRunner.caseName(of: storeError)
                logger.error(
                    "TrainingExample materialization failed for asset \(analysisAssetId, privacy: .public) — error suppressed by shadow invariant: case=\(caseName, privacy: .public) detail=\(detail, privacy: .public)"
                )
            } else {
                logger.error(
                    "TrainingExample materialization failed for asset \(analysisAssetId, privacy: .public) — error suppressed by shadow invariant: detail=\(detail, privacy: .public)"
                )
            }
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
        // Prefer final-pass chunks for the FM shadow phase — they carry the
        // higher-accuracy `transcriptVersion` that `BackfillJobRunner.jobId`
        // consumes for dedupe. When no final-pass chunks exist (the common
        // case until the charge-gated final-pass backfill phase has run),
        // fall back to fast-pass chunks. This mirrors the fallback pattern
        // used at lines 1395 and 2607 of this file. Without the fallback
        // the shadow-retry drain bails unconditionally because production
        // currently persists only `pass='fast'` rows.
        let finalChunks = chunks.filter { $0.pass == TranscriptPassType.final_.rawValue }
        let chunksForReplay = finalChunks.isEmpty ? chunks : finalChunks
        guard !chunksForReplay.isEmpty else {
            logger.debug("Shadow retry skipped: no transcript chunks for \(analysisAssetId)")
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
            chunks: chunksForReplay,
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
    /// signaling a potential false positive.
    ///
    /// **Trust-score writer policy** (post-q45f): all `skipTrustScore`
    /// mutations now live in `TrustScoringService`. This method delegates
    /// to `recordWeakFalseSkipSignal` (q45f), which uses the smaller
    /// `weakFalseSignalPenalty` (default `0.05`) AND runs `evaluateDemotion`,
    /// so two listen-rewinds in a row demote `auto -> manual` as expected.
    /// Pre-q45f the trust mutation was an inline `updateProfileIfExists`
    /// block here that bypassed the demotion state machine.
    ///
    /// Magnitude rationale: a listen-rewind is a noisier FP signal than an
    /// explicit "Not an ad" revert (the user might've been distracted, or
    /// not minded the ad). `weakFalseSignalPenalty = 0.05` is half of
    /// `falseSignalPenalty = 0.10` to reflect that weaker confidence, while
    /// still letting repeated rewinds accumulate into a mode demotion.
    ///
    /// Side effects on every call:
    ///   1. Flip `AdWindowDecision` to `.reverted`.
    ///   2. Append a row to `ad_listen_rewinds` (q45f.1 event log) keyed
    ///      to `window.startTime`. Skipped if the window lookup fails.
    ///   3. Delegate to `trustScoringService?.recordWeakFalseSkipSignal`
    ///      for the atomic profile mutation + demotion evaluation. Optional
    ///      chaining lets legacy test factories (no trust service injected)
    ///      still get steps 1 & 2.
    ///
    /// Do NOT re-introduce an inline profile mutation here — the
    /// `testRecordListenRewindBodyRoutesThroughTrustScoringService`
    /// canary blocks that regression at source-inspection time.
    func recordListenRewind(
        windowId: String,
        podcastId: String
    ) async throws {
        // Revert the window (user tapped "Listen" to play through).
        try await store.updateAdWindowDecision(
            id: windowId,
            decisionState: AdDecisionState.reverted.rawValue
        )

        // playhead-q45f.1: append an event row to `ad_listen_rewinds`
        // so the q45f counterfactual gate's frozen-trace replay can see
        // *that* the user rewound, in addition to the indirect signal
        // already encoded in `AdWindowDecision.reverted`. The row's
        // `time` is the source window's `startTime` — the position the
        // banner's "Listen" tap rewinds the player to, mirroring
        // `seek(to: item.adStartTime)` in `NowPlayingViewModel`. Done
        // before the profile mutation so a missing-profile early
        // return still leaves the event in the log (q45f's gate cares
        // about the *event*, not the trust-score side-effect).
        if let window = try await store.fetchAdWindow(id: windowId) {
            try await store.insertListenRewind(
                windowId: windowId,
                podcastId: podcastId,
                time: window.startTime,
                createdAt: Date()
            )
        } else {
            logger.warning("recordListenRewind: no ad_window for id=\(windowId); skipping event log")
        }

        // playhead-q45f: route the trust-score side-effect through
        // `TrustScoringService.recordWeakFalseSkipSignal`. The pre-q45f
        // inline `updateProfileIfExists` block decremented trust by
        // 0.05 but bypassed the demotion state machine entirely — two
        // listen-rewinds in a row never demoted an `auto` show to
        // `manual`. Routing through the service keeps the weaker 0.05
        // magnitude (matched to the pre-q45f hard-code) AND now passes
        // through `evaluateDemotion`, closing the q45f defect.
        //
        // The optional chaining is deliberate: legacy test factories
        // (and any future caller that constructs an `AdDetectionService`
        // without injecting a trust service) still get the decision
        // flip + event log row. Production wiring in `PlayheadRuntime`
        // always installs a real service before the first user tap.
        await trustScoringService?.recordWeakFalseSkipSignal(podcastId: podcastId)

        logger.info("Recorded listen-rewind for window \(windowId), podcast \(podcastId)")
    }

    // MARK: - Classification Pipeline

    /// Route hot-path lexical hits through the span hypothesis engine when it
    /// can produce windows; otherwise fall back to the legacy 30-second lexical
    /// merge path unchanged.
    private func hotPathCandidates(
        from chunks: [TranscriptChunk],
        analysisAssetId: String
    ) async throws -> [LexicalCandidate] {
        let orderedChunks = chunks.sorted { lhs, rhs in
            if lhs.startTime != rhs.startTime {
                return lhs.startTime < rhs.startTime
            }
            return lhs.endTime < rhs.endTime
        }
        let (metadataCues, _) = await loadEpisodeMetadataSignals(
            analysisAssetId: analysisAssetId
        )
        let metadataEntries = await metadataLexiconEntries(from: metadataCues)

        let hypothesisCandidates = try await hypothesisCandidates(
            from: orderedChunks,
            analysisAssetId: analysisAssetId
        )
        let lexicalCandidates = scanner.scan(
            chunks: orderedChunks,
            analysisAssetId: analysisAssetId,
            metadataEntries: metadataEntries
        )

        if !hypothesisCandidates.isEmpty {
            let survivingLexicalCandidates = lexicalCandidates.filter { lexicalCandidate in
                !hypothesisCandidates.contains { hypothesisCandidate in
                    candidatesOverlap(lexicalCandidate, hypothesisCandidate)
                }
            }
            let mergedCandidates = (hypothesisCandidates + survivingLexicalCandidates).sorted { lhs, rhs in
                if lhs.startTime != rhs.startTime {
                    return lhs.startTime < rhs.startTime
                }
                return lhs.endTime < rhs.endTime
            }

            logger.info("Hot path: hypothesis engine emitted \(hypothesisCandidates.count) candidates and preserved \(survivingLexicalCandidates.count) non-overlapping lexical candidates")
            return mergedCandidates
        }

        if !lexicalCandidates.isEmpty {
            logger.info("Hot path: lexical fallback emitted \(lexicalCandidates.count) candidates")
        }
        return lexicalCandidates
    }

    private func loadEpisodeMetadataSignals(
        analysisAssetId: String
    ) async -> (cues: [EpisodeMetadataCue], chapterEvidence: [ChapterEvidence]) {
        guard let snapshot = await episodeMetadataProvider.metadataSnapshot(
            for: analysisAssetId
        ) else {
            return ([], [])
        }

        let feedMetadata = snapshot.feedMetadata
        let extractor = MetadataCueExtractor(
            showOwnedDomains: snapshot.showOwnedDomains,
            networkOwnedDomains: snapshot.networkOwnedDomains
        )
        let cues = extractor.extractCues(
            description: feedMetadata.feedDescription,
            summary: feedMetadata.feedSummary
        )
        return (cues, feedMetadata.chapterEvidence ?? [])
    }

    private func metadataLexiconEntries(
        from cues: [EpisodeMetadataCue]
    ) async -> [MetadataLexiconEntry] {
        guard !cues.isEmpty else { return [] }
        let priors = await resolveEpisodePriors()
        return metadataLexiconEntries(
            from: cues,
            metadataTrust: priors.metadataTrust
        )
    }

    private func metadataLexiconEntries(
        from cues: [EpisodeMetadataCue],
        metadataTrust: Float
    ) -> [MetadataLexiconEntry] {
        guard !cues.isEmpty else { return [] }
        let activationConfig = MetadataActivationConfig.resolved()
        guard activationConfig.isLexicalInjectionActive else { return [] }

        let injector = MetadataLexiconInjector(config: activationConfig)
        return injector.inject(
            cues: cues,
            metadataTrust: metadataTrust
        )
    }

    #if DEBUG
    func hotPathCandidatesForTesting(
        from chunks: [TranscriptChunk],
        analysisAssetId: String
    ) async throws -> [LexicalCandidate] {
        try await hotPathCandidates(
            from: chunks,
            analysisAssetId: analysisAssetId
        )
    }
    #endif

    private func chunkHasReplaySignal(_ chunk: TranscriptChunk) -> Bool {
        replaySignalProfile(for: chunk).hasSignal
    }

    private func replaySignalProfile(for chunk: TranscriptChunk) -> ReplaySignalProfile {
        let hits = replaySignalHits(for: chunk)
        guard !hits.isEmpty else { return .none }

        var backwardReach: TimeInterval = 0
        var forwardReach: TimeInterval = 0
        var hasDirectionalSignal = false

        for hit in hits {
            guard let anchorEvent = SpanHypothesisEngine.mapToAnchorEvent(hit) else { continue }
            let anchorConfig = SpanHypothesisConfig.default.config(for: anchorEvent.anchorType)
            backwardReach = max(backwardReach, anchorConfig.backwardSearchRadius)
            forwardReach = max(forwardReach, anchorConfig.forwardSearchRadius)
            hasDirectionalSignal = true
        }

        return ReplaySignalProfile(
            hasSignal: true,
            hasDirectionalSignal: hasDirectionalSignal,
            backwardReach: backwardReach,
            forwardReach: forwardReach
        )
    }

    private func replaySignalHits(for chunk: TranscriptChunk) -> [LexicalHit] {
        var hits = scanner.scanChunk(chunk)
        guard let metadata = chunk.weakAnchorMetadata else { return hits }

        for alternativeText in metadata.alternativeTexts {
            guard !alternativeText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { continue }
            hits.append(contentsOf: scanner.scanChunk(
                syntheticReplayChunk(
                    text: alternativeText,
                    analysisAssetId: chunk.analysisAssetId,
                    startTime: chunk.startTime,
                    endTime: chunk.endTime
                )
            ))
        }

        for phrase in metadata.lowConfidencePhrases {
            let startTime = max(chunk.startTime, phrase.startTime)
            let endTime = min(chunk.endTime, phrase.endTime)
            guard endTime > startTime else { continue }
            hits.append(contentsOf: scanner.scanChunk(
                syntheticReplayChunk(
                    text: phrase.text,
                    analysisAssetId: chunk.analysisAssetId,
                    startTime: startTime,
                    endTime: endTime
                )
            ))
        }

        return hits
    }

    private func reconcileHotPathWindows(
        _ adWindows: [AdWindow],
        analysisAssetId: String
    ) async throws -> [ReconciledHotPathWindow] {
        let existingWindows = try await currentHotPathCandidateWindows(
            analysisAssetId: analysisAssetId
        )
        var matchedExistingIDs = Set<String>()
        var reconciled: [ReconciledHotPathWindow] = []

        for adWindow in adWindows.sorted(by: hotPathWindowOrdering) {
            let matchingWindows = matchingHotPathWindows(
                for: adWindow,
                in: existingWindows,
                excluding: matchedExistingIDs
            )
            guard let existing = bestMatchingHotPathWindow(
                for: adWindow,
                in: matchingWindows
            ) else {
                reconciled.append(
                    ReconciledHotPathWindow(
                        window: adWindow,
                        matchedExistingID: nil,
                        retiredExistingIDs: []
                    )
                )
                continue
            }

            let allMatchingIDs = Set(matchingWindows.map(\.id))
            matchedExistingIDs.formUnion(allMatchingIDs)
            let retiredExistingIDs = allMatchingIDs.subtracting([existing.id])

            let preservedWindow = AdWindow(
                id: existing.id,
                analysisAssetId: adWindow.analysisAssetId,
                startTime: adWindow.startTime,
                endTime: adWindow.endTime,
                confidence: adWindow.confidence,
                boundaryState: adWindow.boundaryState,
                decisionState: existing.decisionState,
                detectorVersion: adWindow.detectorVersion,
                advertiser: existing.advertiser,
                product: existing.product,
                adDescription: existing.adDescription,
                evidenceText: adWindow.evidenceText,
                evidenceStartTime: existing.evidenceStartTime ?? adWindow.evidenceStartTime,
                metadataSource: existing.metadataSource,
                metadataConfidence: existing.metadataConfidence,
                metadataPromptVersion: existing.metadataPromptVersion,
                wasSkipped: existing.wasSkipped,
                userDismissedBanner: existing.userDismissedBanner,
                evidenceSources: existing.evidenceSources,
                eligibilityGate: existing.eligibilityGate,
                // playhead-epfk: hot-path reconciliation preserves the
                // existing row's catalog-store match similarity. Hot-path
                // candidates are not re-fingerprinted; the value lives or
                // dies with the originating backfill row.
                catalogStoreMatchSimilarity: existing.catalogStoreMatchSimilarity
            )
            reconciled.append(
                ReconciledHotPathWindow(
                    window: preservedWindow,
                    matchedExistingID: existing.id,
                    retiredExistingIDs: retiredExistingIDs
                )
            )
        }

        return reconciled
    }

    private func currentHotPathCandidateWindows(
        analysisAssetId: String
    ) async throws -> [AdWindow] {
        try await store.fetchAdWindows(assetId: analysisAssetId)
            .filter {
                $0.detectorVersion == config.detectorVersion
                    && $0.decisionState == AdDecisionState.candidate.rawValue
            }
    }

    private func hotPathCandidateIDs(
        analysisAssetId: String,
        overlapping replayEnvelope: ClosedRange<Double>
    ) async throws -> Set<String> {
        let windows = try await currentHotPathCandidateWindows(
            analysisAssetId: analysisAssetId
        )
        return Set(
            windows
                .filter { window in
                    // playhead-hygc.1.8 (R4): correction-replay rows are
                    // user-correction-backed shadow windows that the
                    // algorithmic detector by design does NOT re-emit (the
                    // overlap check in `correctionReplayCandidates` short-
                    // circuits whenever an existing AdWindow covers the FN
                    // span). They therefore can never appear in the new
                    // `adWindows` set produced by classification, so they
                    // would always land in `retiredWindowIDs` on a run
                    // whose chunks envelope overlaps them — which both R3
                    // (same-run) and R4 (subsequent-run) had to defuse.
                    // The cleanest invariant: correction-replay rows are
                    // never retired by the algorithmic-absence path. They
                    // are retired only by an explicit user veto via
                    // `SkipOrchestrator.revertByTimeRange`, which flips
                    // `decisionState` to `.reverted` (filtered out below
                    // by `currentHotPathCandidateWindows`).
                    window.boundaryState != Self.correctionReplayBoundaryState
                        && window.endTime > replayEnvelope.lowerBound
                        && window.startTime < replayEnvelope.upperBound
                }
                .map(\.id)
        )
    }

    private func replayEnvelope(for chunks: [TranscriptChunk]) -> ClosedRange<Double> {
        let start = chunks.map(\.startTime).min() ?? 0
        let end = chunks.map(\.endTime).max() ?? start
        return start...max(start, end)
    }

    private func matchingHotPathWindows(
        for incoming: AdWindow,
        in existingWindows: [AdWindow],
        excluding excludedIDs: Set<String>
    ) -> [AdWindow] {
        existingWindows
            .filter { existing in
                !excludedIDs.contains(existing.id)
                    && existing.analysisAssetId == incoming.analysisAssetId
                    && existing.boundaryState == incoming.boundaryState
                    && existing.endTime > incoming.startTime
                    && existing.startTime < incoming.endTime
                    && hotPathWindowsShareIdentity(existing: existing, incoming: incoming)
            }
    }

    private func bestMatchingHotPathWindow(
        for incoming: AdWindow,
        in matchingWindows: [AdWindow]
    ) -> AdWindow? {
        matchingWindows.max { lhs, rhs in
                let lhsScore = hotPathWindowMatchScore(existing: lhs, incoming: incoming)
                let rhsScore = hotPathWindowMatchScore(existing: rhs, incoming: incoming)
                if lhsScore != rhsScore {
                    return lhsScore < rhsScore
                }

                let lhsDistance = abs(lhs.startTime - incoming.startTime) + abs(lhs.endTime - incoming.endTime)
                let rhsDistance = abs(rhs.startTime - incoming.startTime) + abs(rhs.endTime - incoming.endTime)
                if lhsDistance != rhsDistance {
                    return lhsDistance > rhsDistance
                }
                return lhs.id > rhs.id
            }
    }

    private func hotPathWindowMatchScore(existing: AdWindow, incoming: AdWindow) -> Double {
        let overlapStart = max(existing.startTime, incoming.startTime)
        let overlapEnd = min(existing.endTime, incoming.endTime)
        let overlap = max(0, overlapEnd - overlapStart)
        let union = max(existing.endTime, incoming.endTime) - min(existing.startTime, incoming.startTime)
        guard union > 0 else { return 1 }
        return overlap / union
    }

    private func hotPathWindowsShareIdentity(existing: AdWindow, incoming: AdWindow) -> Bool {
        if abs(existing.startTime - incoming.startTime) <= Self.hotPathCandidateIdentityTolerance {
            return true
        }
        if abs(existing.endTime - incoming.endTime) <= Self.hotPathCandidateIdentityTolerance {
            return true
        }
        if let existingEvidenceStartTime = existing.evidenceStartTime,
           let incomingEvidenceStartTime = incoming.evidenceStartTime,
           abs(existingEvidenceStartTime - incomingEvidenceStartTime) <= Self.hotPathCandidateIdentityTolerance
        {
            return true
        }
        if hotPathEvidenceTextSharesIdentity(existing: existing, incoming: incoming) {
            return true
        }
        return false
    }

    private func hotPathEvidenceTextSharesIdentity(existing: AdWindow, incoming: AdWindow) -> Bool {
        guard let existingText = normalizedHotPathEvidenceText(existing.evidenceText),
              let incomingText = normalizedHotPathEvidenceText(incoming.evidenceText)
        else {
            return false
        }

        let (shorter, longer) = existingText.count <= incomingText.count
            ? (existingText, incomingText)
            : (incomingText, existingText)
        guard shorter.count >= 12 else { return false }
        return longer.contains(shorter)
    }

    private func normalizedHotPathEvidenceText(_ text: String?) -> String? {
        guard let text else { return nil }
        let normalized = TranscriptEngineService.normalizeText(text)
        return normalized.isEmpty ? nil : normalized
    }

    private func hotPathWindowOrdering(_ lhs: AdWindow, _ rhs: AdWindow) -> Bool {
        if lhs.startTime != rhs.startTime {
            return lhs.startTime < rhs.startTime
        }
        return lhs.endTime < rhs.endTime
    }

    private func syntheticReplayChunk(
        text: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: UUID().uuidString,
            analysisAssetId: analysisAssetId,
            segmentFingerprint: UUID().uuidString,
            chunkIndex: 0,
            startTime: startTime,
            endTime: endTime,
            text: text,
            normalizedText: TranscriptEngineService.normalizeText(text),
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: Self.hotPathReplayModelVersion,
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    private func hypothesisCandidates(
        from chunks: [TranscriptChunk],
        analysisAssetId: String
    ) async throws -> [LexicalCandidate] {
        let spanConfig = SpanHypothesisConfig.default
        let boundaryContext = try await hotPathBoundaryExpansionContext(
            for: chunks,
            analysisAssetId: analysisAssetId,
            config: spanConfig
        )
        var engine = SpanHypothesisEngine(
            config: spanConfig,
            boundaryExpansionContext: boundaryContext
        )
        _ = engine.process(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            scanner: scanner
        )
        let hits = engine.observedHits
        guard !hits.isEmpty else { return [] }

        let finishTime = max(
            chunks.last?.endTime ?? 0,
            hits.last?.endTime ?? 0
        )
        _ = engine.finish(
            analysisAssetId: analysisAssetId,
            at: finishTime
        )

        guard !engine.closedHypotheses.isEmpty else { return [] }
        let envelopes: [HotPathHypothesisCandidate] = engine.closedHypotheses.compactMap {
            (hypothesis: SpanHypothesis) -> HotPathHypothesisCandidate? in
            guard shouldPromoteHotPathHypothesis(hypothesis) else { return nil }

            return makeHotPathHypothesisCandidate(
                from: hypothesis,
                analysisAssetId: analysisAssetId,
                allHits: hits,
                transcriptChunks: chunks,
                featureWindows: boundaryContext.featureWindows,
                minConfirmedEvidence: spanConfig.minConfirmedEvidence
            )
        }.filter { envelope in
            envelope.candidate.endTime > envelope.candidate.startTime
        }

        return collapseHotPathHypothesisCandidates(envelopes)
            .map(\.candidate)
    }

    private func collapseHotPathHypothesisCandidates(
        _ candidates: [HotPathHypothesisCandidate]
    ) -> [HotPathHypothesisCandidate] {
        let orderedCandidates = candidates.sorted { lhs, rhs in
            if lhs.candidate.startTime != rhs.candidate.startTime {
                return lhs.candidate.startTime < rhs.candidate.startTime
            }
            return lhs.candidate.endTime < rhs.candidate.endTime
        }

        var collapsed: [HotPathHypothesisCandidate] = []
        for candidate in orderedCandidates {
            guard let last = collapsed.last else {
                collapsed.append(candidate)
                continue
            }

            if candidatesOverlap(last.candidate, candidate.candidate) {
                let preferred = prefersHotPathCandidate(candidate, over: last) ? candidate : last
                let other = preferred.candidate.id == candidate.candidate.id ? last : candidate
                collapsed[collapsed.count - 1] = mergeHotPathCandidates(preferred, with: other)
            } else {
                collapsed.append(candidate)
            }
        }

        return collapsed
    }

    private func candidatesOverlap(_ lhs: LexicalCandidate, _ rhs: LexicalCandidate) -> Bool {
        lhs.endTime >= rhs.startTime && rhs.endTime >= lhs.startTime
    }

    private func prefersHotPathCandidate(
        _ lhs: HotPathHypothesisCandidate,
        over rhs: HotPathHypothesisCandidate
    ) -> Bool {
        if lhs.evidenceCount != rhs.evidenceCount {
            return lhs.evidenceCount > rhs.evidenceCount
        }

        if lhs.hasClosingAnchor != rhs.hasClosingAnchor {
            return lhs.hasClosingAnchor
        }

        if lhs.candidate.categories.count != rhs.candidate.categories.count {
            return lhs.candidate.categories.count > rhs.candidate.categories.count
        }

        if lhs.candidate.evidenceText.count != rhs.candidate.evidenceText.count {
            return lhs.candidate.evidenceText.count > rhs.candidate.evidenceText.count
        }

        if lhs.candidate.hitCount != rhs.candidate.hitCount {
            return lhs.candidate.hitCount > rhs.candidate.hitCount
        }

        if lhs.candidate.startTime != rhs.candidate.startTime {
            return lhs.candidate.startTime < rhs.candidate.startTime
        }

        let lhsDuration = lhs.candidate.endTime - lhs.candidate.startTime
        let rhsDuration = rhs.candidate.endTime - rhs.candidate.startTime
        if lhsDuration != rhsDuration {
            return lhsDuration < rhsDuration
        }

        if lhs.candidate.confidence != rhs.candidate.confidence {
            return lhs.candidate.confidence > rhs.candidate.confidence
        }

        return lhs.candidate.endTime < rhs.candidate.endTime
    }

    private func mergeHotPathCandidates(
        _ preferred: HotPathHypothesisCandidate,
        with other: HotPathHypothesisCandidate
    ) -> HotPathHypothesisCandidate {
        let mergedHits = deduplicatedHotPathHits(preferred.supportingHits + other.supportingHits)
        let mergedStart = min(preferred.candidate.startTime, other.candidate.startTime)
        let mergedEnd = max(preferred.candidate.endTime, other.candidate.endTime)
        let categories = mergedHits.isEmpty
            ? preferred.candidate.categories.union(other.candidate.categories)
            : Set(mergedHits.map(\.category))
        let evidenceText = hotPathEvidenceText(
            from: mergedHits,
            fallbackTexts: [preferred.candidate.evidenceText, other.candidate.evidenceText]
        )

        return HotPathHypothesisCandidate(
            candidate: LexicalCandidate(
                id: preferred.candidate.id,
                analysisAssetId: preferred.candidate.analysisAssetId,
                startTime: mergedStart,
                endTime: mergedEnd,
                confidence: max(preferred.candidate.confidence, other.candidate.confidence),
                hitCount: max(preferred.candidate.hitCount, other.candidate.hitCount, mergedHits.count),
                categories: categories,
                evidenceText: evidenceText,
                evidenceStartTime: preferred.candidate.evidenceStartTime,
                detectorVersion: preferred.candidate.detectorVersion
            ),
            evidenceCount: max(preferred.evidenceCount, other.evidenceCount),
            hasClosingAnchor: preferred.hasClosingAnchor || other.hasClosingAnchor,
            supportingHits: mergedHits
        )
    }

    private func hotPathBoundaryExpansionContext(
        for chunks: [TranscriptChunk],
        analysisAssetId: String,
        config: SpanHypothesisConfig
    ) async throws -> SpanHypothesisEngine.BoundaryExpansionContext {
        let minStart = chunks.map(\.startTime).min() ?? 0
        let maxEnd = chunks.map(\.endTime).max() ?? 0
        let hypothesisBackwardMargin = config.anchorTypeConfigByType.values
            .map(\.backwardSearchRadius)
            .max() ?? 0
        let hypothesisForwardMargin = config.anchorTypeConfigByType.values
            .map(\.forwardSearchRadius)
            .max() ?? 0
        let expansionConfigs: [BoundaryExpander.ExpansionConfig] = [
            .startAnchored,
            .endAnchored,
            .neutral
        ]
        let acousticBackwardMargin = expansionConfigs
            .map(\.acousticBackwardSearchRadius)
            .max() ?? 0
        let acousticForwardMargin = expansionConfigs
            .map(\.acousticForwardSearchRadius)
            .max() ?? 0
        let backwardMargin = max(hypothesisBackwardMargin, acousticBackwardMargin)
        let forwardMargin = max(hypothesisForwardMargin, acousticForwardMargin)

        let featureWindows = try await store.fetchFeatureWindows(
            assetId: analysisAssetId,
            from: max(0, minStart - backwardMargin),
            to: maxEnd + forwardMargin
        )

        return SpanHypothesisEngine.BoundaryExpansionContext(
            featureWindows: featureWindows,
            transcriptChunks: chunks
        )
    }

    private func makeHotPathHypothesisCandidate(
        from hypothesis: SpanHypothesis,
        analysisAssetId: String,
        allHits: [LexicalHit],
        transcriptChunks: [TranscriptChunk],
        featureWindows: [FeatureWindow],
        minConfirmedEvidence: Double
    ) -> HotPathHypothesisCandidate {
        let boundary = hotPathExpandedBoundary(
            for: hypothesis,
            featureWindows: featureWindows,
            transcriptChunks: transcriptChunks
        )
        let startTime = boundary?.startTime ?? hypothesis.startCandidateTime
        let endTime = boundary?.endTime ?? hypothesis.endCandidateTime
        let supportingHits = allHits.filter { hit in
            hit.endTime >= startTime && hit.startTime <= endTime
        }
        let categories = supportingHits.isEmpty
            ? [defaultCategory(for: hypothesis.anchorType)]
            : Array(Set(supportingHits.map(\.category)))
        let evidenceTexts = supportingHits.isEmpty
            ? hypothesis.allEvidenceTexts
            : supportingHits
                .sorted { lhs, rhs in
                    if lhs.startTime != rhs.startTime {
                        return lhs.startTime < rhs.startTime
                    }
                    return lhs.endTime < rhs.endTime
                }
                .map(\.matchedText)
        let evidenceText = evidenceTexts.reduce(into: [String]()) { partial, text in
            if !partial.contains(text) {
                partial.append(text)
            }
        }.joined(separator: " | ")
        let confidence = min(
            1.0,
            hypothesis.score(at: hypothesis.lastEvidenceTime) / max(minConfirmedEvidence, 1.0)
        )
        let evidenceStartTime = hotPathEvidenceStartTime(for: hypothesis)

        return HotPathHypothesisCandidate(
            candidate: LexicalCandidate(
                id: [
                    analysisAssetId,
                    String(format: "%.3f", locale: Locale(identifier: "en_US_POSIX"), startTime),
                    String(format: "%.3f", locale: Locale(identifier: "en_US_POSIX"), endTime),
                    "hypothesis"
                ].joined(separator: ":"),
                analysisAssetId: analysisAssetId,
                startTime: startTime,
                endTime: endTime,
                confidence: confidence,
                hitCount: max(1, supportingHits.count),
                categories: Set(categories),
                evidenceText: evidenceText,
                evidenceStartTime: evidenceStartTime,
                detectorVersion: "hypothesis-v1"
            ),
            evidenceCount: 1 + hypothesis.supportingAnchors.count + hypothesis.bodyEvidence.count + (hypothesis.closingAnchor == nil ? 0 : 1),
            hasClosingAnchor: hypothesis.closingAnchor != nil,
            supportingHits: supportingHits
        )
    }

    private func hotPathEvidenceStartTime(for hypothesis: SpanHypothesis) -> Double {
        if let bodyTimestamp = hypothesis.bodyEvidence.map(\.timestamp).min() {
            return bodyTimestamp
        }
        if let closingAnchor = hypothesis.closingAnchor {
            return closingAnchor.startTime
        }
        if let supportingAnchorStart = hypothesis.supportingAnchors.map(\.startTime).min() {
            return supportingAnchorStart
        }
        return hypothesis.seedAnchor.startTime
    }

    private func hotPathExpandedBoundary(
        for hypothesis: SpanHypothesis,
        featureWindows: [FeatureWindow],
        transcriptChunks: [TranscriptChunk]
    ) -> ExpandedBoundary? {
        let additionalEvidenceCount = hypothesis.supportingAnchors.count + hypothesis.bodyEvidence.count + (hypothesis.closingAnchor == nil ? 0 : 1)
        guard additionalEvidenceCount > 0 else { return nil }

        let seed = hotPathBoundarySeed(for: hypothesis)
        let config = BoundaryExpander.ExpansionConfig.forPolarity(hypothesis.polarity)
        let evidenceTimes = hotPathEvidenceTimes(for: hypothesis)
        let hasTemporalSpread = hotPathHasTemporalSpread(hypothesis)

        let acousticOnly = BoundaryExpander().expand(
            seed: seed,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: [],
            config: config,
            anchorType: hypothesis.anchorType
        )
        let usableAcousticOnly = acousticOnly.source == .fallback || !boundaryCoversEvidenceTimes(acousticOnly, evidenceTimes: evidenceTimes)
            ? nil
            : acousticOnly

        let lexicalAware = BoundaryExpander().expand(
            seed: seed,
            featureWindows: featureWindows,
            transcriptChunks: transcriptChunks,
            adWindows: [],
            config: config,
            anchorType: hypothesis.anchorType
        )
        let usableLexicalAware = lexicalAware.source == .fallback || !boundaryCoversEvidenceTimes(lexicalAware, evidenceTimes: evidenceTimes)
            ? nil
            : lexicalAware

        if hasTemporalSpread {
            return usableAcousticOnly ?? usableLexicalAware
        }

        if hypothesis.closingAnchor != nil {
            return usableLexicalAware ?? usableAcousticOnly
        }

        // Same-chunk corroboration should stay tight instead of widening into a
        // speculative open-ended window.
        if additionalEvidenceCount > 0 {
            return usableLexicalAware ?? usableAcousticOnly
        }

        return usableAcousticOnly ?? usableLexicalAware
    }

    private func hotPathBoundarySeed(for hypothesis: SpanHypothesis) -> Double {
        switch hypothesis.polarity {
        case .startAnchored:
            return hypothesis.seedAnchor.startTime
        case .endAnchored:
            return hypothesis.seedAnchor.endTime
        case .neutral:
            return (hypothesis.seedAnchor.startTime + hypothesis.seedAnchor.endTime) / 2.0
        }
    }

    private func defaultCategory(for anchorType: AnchorType) -> LexicalPatternCategory {
        switch anchorType {
        case .disclosure, .sponsorLexicon, .fmPositive:
            return .sponsor
        case .url:
            return .urlCTA
        case .promoCode:
            return .promoCode
        case .transitionMarker:
            return .transitionMarker
        }
    }

    private func shouldPromoteHotPathHypothesis(_ hypothesis: SpanHypothesis) -> Bool {
        let additionalEvidenceCount = hypothesis.supportingAnchors.count
            + hypothesis.bodyEvidence.count
            + (hypothesis.closingAnchor == nil ? 0 : 1)
        guard additionalEvidenceCount > 0 else { return false }

        if hypothesis.sponsorEntity != nil {
            return true
        }

        let anchors = [hypothesis.seedAnchor] + hypothesis.supportingAnchors
        return anchors.contains { anchor in
            switch anchor.anchorType {
            case .disclosure, .sponsorLexicon, .fmPositive:
                return true
            case .url, .promoCode, .transitionMarker:
                return false
            }
        }
    }

    private func hotPathEvidenceTimes(for hypothesis: SpanHypothesis) -> [Double] {
        var timestamps: [Double] = [
            hypothesis.seedAnchor.startTime,
            hypothesis.seedAnchor.endTime,
        ]
        timestamps.append(contentsOf: hypothesis.supportingAnchors.flatMap { [$0.startTime, $0.endTime] })
        timestamps.append(contentsOf: hypothesis.bodyEvidence.map(\.timestamp))
        if let closingAnchor = hypothesis.closingAnchor {
            timestamps.append(contentsOf: [closingAnchor.startTime, closingAnchor.endTime])
        }
        return timestamps
    }

    private func hotPathHasTemporalSpread(_ hypothesis: SpanHypothesis) -> Bool {
        let temporalSpreadThreshold = 5.0
        let seedTime = hypothesis.seedAnchor.endTime

        let supportingAnchorTimes = hypothesis.supportingAnchors.flatMap { [$0.startTime, $0.endTime] }
        if supportingAnchorTimes.contains(where: { abs($0 - seedTime) >= temporalSpreadThreshold }) {
            return true
        }

        if hypothesis.bodyEvidence.contains(where: { abs($0.timestamp - seedTime) >= temporalSpreadThreshold }) {
            return true
        }

        if let closingAnchor = hypothesis.closingAnchor {
            return abs(closingAnchor.endTime - seedTime) >= temporalSpreadThreshold
                || abs(closingAnchor.startTime - seedTime) >= temporalSpreadThreshold
        }

        return false
    }

    private func boundaryCoversEvidenceTimes(
        _ boundary: ExpandedBoundary,
        evidenceTimes: [Double]
    ) -> Bool {
        evidenceTimes.allSatisfy { time in
            time >= boundary.startTime && time <= boundary.endTime
        }
    }

    private func deduplicatedHotPathHits(_ hits: [LexicalHit]) -> [LexicalHit] {
        let orderedHits = hits.sorted { lhs, rhs in
            if lhs.startTime != rhs.startTime {
                return lhs.startTime < rhs.startTime
            }
            if lhs.endTime != rhs.endTime {
                return lhs.endTime < rhs.endTime
            }
            if lhs.category != rhs.category {
                return lhs.category.rawValue < rhs.category.rawValue
            }
            return lhs.matchedText < rhs.matchedText
        }

        var seen = Set<String>()
        var deduplicated: [LexicalHit] = []
        for hit in orderedHits {
            let key = [
                hit.category.rawValue,
                hit.matchedText,
                String(format: "%.6f", locale: Locale(identifier: "en_US_POSIX"), hit.startTime),
                String(format: "%.6f", locale: Locale(identifier: "en_US_POSIX"), hit.endTime)
            ].joined(separator: "|")
            if seen.insert(key).inserted {
                deduplicated.append(hit)
            }
        }
        return deduplicated
    }

    private func hotPathEvidenceText(
        from hits: [LexicalHit],
        fallbackTexts: [String]
    ) -> String {
        let orderedFragments = hits.isEmpty
            ? fallbackTexts
            : hits.map(\.matchedText)

        return orderedFragments.reduce(into: [String]()) { partial, fragment in
            guard !fragment.isEmpty else { return }
            if !partial.contains(fragment) {
                partial.append(fragment)
            }
        }.joined(separator: " | ")
    }

    /// Fetch feature windows for each lexical candidate and run the classifier.
    ///
    /// playhead-43ed: when `repeatedAdCache` is wired and an entry for the
    /// current show matches a candidate's perceptual fingerprint, the
    /// classifier round-trip is skipped and the cached
    /// `(boundaryStart, boundaryEnd, confidence)` is replayed as a
    /// synthesized `ClassifierResult`. The cache lookup is per-candidate;
    /// hits and misses interleave freely. Misses fall through to the
    /// usual classifier path.
    private func classifyCandidates(
        _ candidates: [LexicalCandidate],
        analysisAssetId: String
    ) async throws -> [ClassifierResult] {
        var inputs: [ClassifierInput] = []
        // Index back from the in-order `inputs` array to its candidate so
        // we can splice cached hits back into the same positions in the
        // returned result without losing input ordering.
        var inputIndexByCandidate: [String: Int] = [:]
        var cacheHits: [String: ClassifierResult] = [:]
        // playhead-43ed C2: candidates whose cache lookup MISSED. We
        // intentionally do NOT call `recordOutcome(false)` at miss time
        // because most lexical candidates are non-ads — recording a miss
        // for a candidate the classifier ultimately rejects below the
        // store-confidence floor would saturate the rolling-window
        // hit-rate metric with noise on every fresh show, tripping the
        // 5% auto-disable floor on the very first session before the
        // cache has had any chance to warm.
        //
        // Instead we defer the outcome to AFTER classification and only
        // record `recordOutcome(false)` when the classifier verdict
        // clears the same gate that controls `store(...)`
        // (`adProbability >= storeConfidenceThreshold`). That makes the
        // metric "out of confirmed-ad candidates, how many were answered
        // from cache" — the actual signal the auto-disable guard wants.
        var deferredMissCandidateIds: [String] = []

        let cacheShowId = currentPodcastProfile?.podcastId

        for candidate in candidates {
            // Layer 0: Fetch acoustic features overlapping this candidate.
            // Extend the search range slightly to allow boundary snapping.
            let margin = 5.0
            let featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: candidate.startTime - margin,
                to: candidate.endTime + margin
            )

            // playhead-43ed: cache lookup BEFORE invoking the classifier.
            // Three guards keep the cache strictly opt-in and side-effect-
            // safe: (1) a real RepeatedAdCacheService has been wired,
            // (2) the current podcast profile carries a non-empty showId,
            // (3) the fingerprint derives to a non-zero value (zero is a
            // documented "do not cache" sentinel — see
            // RepeatedAdFingerprint.zero).
            if let cache = repeatedAdCache,
               let showId = cacheShowId,
               !showId.isEmpty {
                let fp = RepeatedAdFingerprint.from(featureWindows: featureWindows)
                if !fp.isZero {
                    // Both lookup and outcome recording are best-effort:
                    // a transient SQLite hiccup must not bring down the
                    // hot path. Failures are logged once and the candidate
                    // falls through to the classifier as if the cache had
                    // missed.
                    do {
                        let outcome = try await cache.lookup(showId: showId, fingerprint: fp)
                        switch outcome {
                        case let .hit(entry):
                            // Synthesize a ClassifierResult that the rest
                            // of the hot path can consume identically to a
                            // real classifier output. Boundary times come
                            // from the cached entry; `adProbability` is
                            // the cached detection confidence (always
                            // ≥ 0.85 — `store` enforces that floor). The
                            // signal breakdown is zeroed — the cache
                            // doesn't preserve individual contributors.
                            cacheHits[candidate.id] = ClassifierResult(
                                candidateId: candidate.id,
                                analysisAssetId: analysisAssetId,
                                startTime: entry.boundaryStart,
                                endTime: entry.boundaryEnd,
                                adProbability: entry.confidence,
                                startAdjustment: 0,
                                endAdjustment: 0,
                                signalBreakdown: SignalBreakdown(
                                    lexicalScore: 0,
                                    rmsDropScore: 0,
                                    spectralChangeScore: 0,
                                    musicScore: 0,
                                    speakerChangeScore: 0,
                                    priorScore: 0
                                )
                            )
                            // playhead-43ed C2: A hit's synthesized
                            // adProbability is `entry.confidence`, which
                            // `store(...)` guarantees is
                            // ≥ storeConfidenceThreshold. So a hit here
                            // is unambiguously a confirmed-ad-shaped
                            // outcome and we can record it immediately.
                            try? await cache.recordOutcome(hit: true)
                            // Skip building a ClassifierInput for this
                            // candidate — the cache hit replaces the
                            // classifier round-trip outright.
                            continue
                        case .miss:
                            // playhead-43ed C2: defer the miss outcome
                            // until classification finishes. We only
                            // record `recordOutcome(false)` if the
                            // classifier verdict clears the same
                            // `storeConfidenceThreshold` gate that
                            // controls `store(...)`. Pre-fix this fired
                            // for every lexical candidate (mostly non-
                            // ads) and tripped the 5% auto-disable floor
                            // on the first session.
                            deferredMissCandidateIds.append(candidate.id)
                        case .skippedDisabled:
                            // Cache is currently disabled (kill-switch or
                            // auto-disable). Do NOT record an outcome —
                            // the rolling window only counts samples taken
                            // while the cache is live.
                            break
                        }
                    } catch {
                        logger.error("RepeatedAdCache lookup failed; falling through to classifier: \(String(describing: error))")
                    }
                }
            }

            inputIndexByCandidate[candidate.id] = inputs.count
            inputs.append(ClassifierInput(
                candidate: candidate,
                featureWindows: featureWindows,
                episodeDuration: episodeDuration
            ))
        }

        // Layer 2: Classify the candidates that didn't hit the cache.
        let classifierResults = classifier.classify(inputs: inputs, priors: showPriors)

        // playhead-43ed C2: now that classification is done, replay any
        // deferred miss outcomes — but only for candidates whose verdict
        // clears the same `storeConfidenceThreshold` gate that controls
        // `store(...)`. A non-ad candidate with low classifier
        // probability is noise, not signal, and must not feed the
        // rolling-window auto-disable metric.
        if let cache = repeatedAdCache, !deferredMissCandidateIds.isEmpty {
            let storeFloor = cache.config.storeConfidenceThreshold
            for candidateId in deferredMissCandidateIds {
                guard let idx = inputIndexByCandidate[candidateId],
                      idx < classifierResults.count else { continue }
                let result = classifierResults[idx]
                if result.adProbability >= storeFloor {
                    try? await cache.recordOutcome(hit: false)
                }
            }
        }

        // Reassemble in the original candidate order: hits first replaced
        // by their synthesized result, misses by their classifier output.
        // Iterating `candidates` (rather than `classifierResults`)
        // preserves the input ordering that the rest of the hot path
        // assumes (`runHotPathResult` indexes by `candidatesByID`, which
        // is order-agnostic, but emit-decision-logs is positional).
        var combined: [ClassifierResult] = []
        combined.reserveCapacity(candidates.count)
        for candidate in candidates {
            if let hit = cacheHits[candidate.id] {
                combined.append(hit)
            } else if let idx = inputIndexByCandidate[candidate.id], idx < classifierResults.count {
                combined.append(classifierResults[idx])
            }
        }
        return combined
    }

    // MARK: - Hot-path Decision Logging

    /// playhead-8em9 (narL): emit one DecisionLogEntry per classifier
    /// result produced by the hot path. Pre-fusion, so the ledger has a
    /// single `.classifier` entry and the fused breakdown degenerates
    /// to one source. `finalDecision.action` is "hotPathCandidate" when
    /// the result passed the candidate threshold and "hotPathBelowThreshold"
    /// otherwise — replay tooling can filter on this to distinguish from
    /// backfill-fusion entries.
    private func emitHotPathDecisionLogs(
        classifierResults: [ClassifierResult],
        analysisAssetId: String
    ) async {
        let snapshot = DecisionLogEntry.ActivationConfigSnapshot(
            MetadataActivationConfig.resolved()
        )
        // Match BackfillEvidenceFusion.buildLedger: cap-scale the classifier
        // score so hot-path and backfill ledger entries are directly comparable.
        let fusionConfig = FusionWeightConfig()
        let classifierCap = fusionConfig.classifierCap
        for result in classifierResults {
            let timestamp = Date().timeIntervalSince1970
            let passed = result.adProbability >= config.candidateThreshold
            // A classifier-only window clearing the autoSkip threshold is
            // skip-worthy on its own merit. Surfacing it as "autoSkipEligible"
            // (rather than generic "hotPathCandidate") makes the signal
            // visible to downstream consumers — including the NARL corpus
            // builder, whose `isAdUnderDefault(policyAction:)` mapping
            // (playhead-gtt9.19) treats `autoSkipEligible` as a positive ad
            // determination via exact raw-value match. `hotPathCandidate`
            // is explicitly mapped to `false` because it's an intermediate
            // state, not a final ad verdict.
            //
            // Regression: 2026-04-23 dogfood capture asset
            // 71F0C2AE-7260-4D1E-B41A-BCFD5103A641 @ [7006..7008],
            // classifier 0.8154, surfaced as "hotPathCandidate" → invisible
            // to the harness → GT=3, Pred=0, Sec-F1=0.
            //
            // playhead-fqc8 cycle-1 review HIGH-2: the hot path keeps the
            // standard 0.80 threshold here because the qualified-track
            // signal (the acoustic-break alignment that gates
            // `PromotionTrack.classifierSeedQualified`) only joins the
            // ledger after fusion runs in `runBackfill`. A classifier-
            // seeded span we passed on at, say, 0.55 may still be promoted
            // to `autoSkipEligible` later via the looser
            // `classifierSeedQualifiedThreshold` once the alignment
            // evidence is available. This bifurcation is intentional —
            // see the header docstring on `PromotionTrack`.
            let promotesToAutoSkip = result.adProbability >= config.autoSkipConfidenceThreshold
            let action: String
            let thresholdCrossed: Double
            if promotesToAutoSkip {
                action = "autoSkipEligible"
                thresholdCrossed = config.autoSkipConfidenceThreshold
            } else if passed {
                action = "hotPathCandidate"
                thresholdCrossed = config.candidateThreshold
            } else {
                action = "hotPathBelowThreshold"
                thresholdCrossed = config.candidateThreshold
            }
            let clampedScore = max(0.0, min(1.0, result.adProbability))
            let cappedWeight = min(clampedScore * classifierCap, classifierCap)
            let classifierEntry = EvidenceLedgerEntry(
                source: .classifier,
                weight: cappedWeight,
                detail: .classifier(score: result.adProbability)
            )
            // Authority mirrors DecisionExplanation.build: weight > cap/2 → strong.
            let authority: ProposalAuthority = cappedWeight > classifierCap * 0.5 ? .strong : .weak
            let breakdown = [
                SourceEvidence(
                    source: EvidenceSourceType.classifier.rawValue,
                    weight: cappedWeight,
                    capApplied: classifierCap,
                    authority: authority
                )
            ]
            // playhead-gtt9.20: for autoSkip-eligible candidates, carry the
            // gtt9.4.1 boundary-expanded bounds into the decision log instead
            // of the raw 2-s classifier slot. AdWindow already gets expanded
            // bounds via `expandedBounds(for:)` in `runHotPath`; without this
            // mirror, `DecisionLogEntry.windowBounds` stays at the narrow slot
            // and the NARL harness scores even confidently-detected closing-
            // block ads as FN (IoU = 2 / span_width ≪ 0.3).
            //
            // Below-autoSkip results keep raw bounds — they're informational
            // shadow logs, and `expandedBounds` short-circuits anyway.
            let logBounds: (start: Double, end: Double)
            if promotesToAutoSkip {
                let expanded = await expandedBounds(
                    for: result,
                    analysisAssetId: analysisAssetId
                )
                logBounds = (expanded.startTime, expanded.endTime)
            } else {
                logBounds = (result.startTime, result.endTime)
            }
            let logEntry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: timestamp,
                windowBounds: .init(start: logBounds.start, end: logBounds.end),
                activationConfig: snapshot,
                evidence: [DecisionLogEntry.LedgerEntry(classifierEntry)],
                fusedConfidence: .init(
                    proposalConfidence: result.adProbability,
                    skipConfidence: result.adProbability,
                    breakdown: breakdown
                ),
                finalDecision: .init(
                    action: action,
                    gate: "eligible",
                    skipConfidence: result.adProbability,
                    thresholdCrossed: thresholdCrossed
                )
            )
            await decisionLogger.record(logEntry)
        }
    }

    // MARK: - Segment Aggregation (playhead-0usd)

    /// Fuse per-window classifier scores from Tier 1 + Tier 2 into coherent
    /// segments via `SegmentAggregator`, build `AdWindow`s for promoted
    /// segments that don't overlap an existing single-window AdWindow from
    /// this run, and emit distinguishing decision-log entries for
    /// observability.
    ///
    /// Additive contract: the aggregator path is a parallel channel to the
    /// existing single-window promotion — it never overrides a single-window
    /// AdWindow, only ADDS windows when the single-window path missed them.
    ///
    /// - Parameters:
    ///   - tier1Results: Per-Tier-1-slot classifier results (30 s slots).
    ///   - tier2Results: Per-hot-path-candidate classifier results (2 s
    ///     lexical-derived regions). Pass `[]` when the hot path bypasses
    ///     transcript scoring (empty chunks, or no candidates from chunks).
    ///   - singleWindowAdWindows: AdWindows already produced by the single-
    ///     window hot path in this run. Aggregator segments overlapping any
    ///     of these are dropped — the single-window result wins (it carries
    ///     richer evidence text / gtt9.4.1 boundary expansion).
    ///   - analysisAssetId: Asset under analysis.
    /// - Returns: Net-new aggregator-promoted AdWindows ready for
    ///   persistence. Caller wires them into `upsertHotPathAdWindows`.
    private func runSegmentAggregation(
        tier1Results: [ClassifierResult],
        tier2Results: [ClassifierResult],
        singleWindowAdWindows: [AdWindow],
        analysisAssetId: String,
        lexicalCandidates: [LexicalCandidate] = []
    ) async throws -> [AdWindow] {
        // Merge tier 1 + tier 2 into a single sorted WindowScore stream.
        // SegmentAggregator requires ASC by startTime.
        let allResults = (tier1Results + tier2Results)
            .filter { $0.endTime > $0.startTime }
        guard !allResults.isEmpty else { return [] }

        let windowScores: [SegmentAggregator.WindowScore] = allResults
            .map {
                SegmentAggregator.WindowScore(
                    startTime: $0.startTime,
                    endTime: $0.endTime,
                    score: max(0.0, min(1.0, $0.adProbability))
                )
            }
            .sorted { lhs, rhs in
                if lhs.startTime != rhs.startTime { return lhs.startTime < rhs.startTime }
                return lhs.endTime < rhs.endTime
            }

        let segments = SegmentAggregator.aggregate(windows: windowScores)
        let promotedSegments = segments.filter(\.promoted)
        let boundarySingletonSegments = boundarySingletonPromotedSegments(
            tier1Results: tier1Results,
            existingSegments: promotedSegments,
            episodeDuration: episodeDuration
        )
        let surfacedSegments = promotedSegments + boundarySingletonSegments
        // playhead-9ro7: boundary-singleton mark-only enforcement now
        // lives in `precisionGateLabel` (slot-only firedSignals → demote
        // to "markOnly"), so both this aggregator path and the single-
        // window path at the top of `runHotPath` honor the same rule.
        guard !surfacedSegments.isEmpty else { return [] }

        // Observability: emit one decision-log entry per promoted segment
        // with a distinguishing action string so replay tooling can
        // distinguish aggregator promotions from single-window promotions
        // (which carry "hotPathCandidate" / "autoSkipEligible").
        let fusionConfig = FusionWeightConfig()
        let classifierCap = fusionConfig.classifierCap
        let activationSnapshot = DecisionLogEntry.ActivationConfigSnapshot(
            MetadataActivationConfig.resolved()
        )
        for segment in surfacedSegments {
            let timestamp = Date().timeIntervalSince1970
            let clampedScore = max(0.0, min(1.0, segment.segmentScore))
            let cappedWeight = min(clampedScore * classifierCap, classifierCap)
            let classifierEntry = EvidenceLedgerEntry(
                source: .classifier,
                weight: cappedWeight,
                detail: .classifier(score: segment.segmentScore)
            )
            let authority: ProposalAuthority = cappedWeight > classifierCap * 0.5 ? .strong : .weak
            let breakdown = [
                SourceEvidence(
                    source: EvidenceSourceType.classifier.rawValue,
                    weight: cappedWeight,
                    capApplied: classifierCap,
                    authority: authority
                )
            ]
            let entry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: timestamp,
                windowBounds: .init(start: segment.startTime, end: segment.endTime),
                activationConfig: activationSnapshot,
                evidence: [DecisionLogEntry.LedgerEntry(classifierEntry)],
                fusedConfidence: .init(
                    proposalConfidence: segment.segmentScore,
                    skipConfidence: segment.segmentScore,
                    breakdown: breakdown
                ),
                finalDecision: .init(
                    action: Self.segmentAggregatorPromotedAction,
                    gate: "eligible",
                    skipConfidence: segment.segmentScore,
                    thresholdCrossed: SegmentAggregatorConfig.default.promotionThreshold
                )
            )
            await decisionLogger.record(entry)
        }

        // Drop segments overlapping any single-window AdWindow already
        // produced this run. A half-open-interval overlap test suffices:
        // [s.start, s.end) intersects [w.start, w.end) iff s.end > w.start
        // && s.start < w.end.
        //
        // Also drop segments overlapping any previously-persisted AdWindow
        // for this asset, regardless of boundaryState. Without this guard,
        // re-running the hot path (e.g. on transcript-coverage progress)
        // would insert duplicate aggregator windows at the same span with
        // fresh UUIDs, because aggregator windows carry no lexical evidence
        // to reconcile against. Including single-window AdWindows in this
        // check covers replays where a prior single-window window is still
        // persisted but the current replay has no transcript chunks to
        // regenerate it — the aggregator would otherwise add a duplicate-
        // span aggregator window next to the existing single-window one.
        let previouslyPersistedWindows = try await store
            .fetchAdWindows(assetId: analysisAssetId)
            .filter { $0.detectorVersion == config.detectorVersion }
        let surviving = surfacedSegments.filter { segment in
            let overlapsSingleWindow = singleWindowAdWindows.contains { window in
                segment.endTime > window.startTime && segment.startTime < window.endTime
            }
            let overlapsExistingWindow = previouslyPersistedWindows.contains { window in
                segment.endTime > window.startTime && segment.startTime < window.endTime
            }
            return !overlapsSingleWindow && !overlapsExistingWindow
        }
        guard !surviving.isEmpty else { return [] }

        // Build AdWindows for surviving segments. boundaryState uses the
        // dedicated `.segmentAggregated` marker so downstream observability
        // can tell aggregator windows from lexical / acoustic-refined ones.
        //
        // playhead-gtt9.11: each aggregator segment passes through the
        // precision gate before persistence. The gate determines
        // eligibilityGate = "autoSkip" | "markOnly" based on score,
        // duration, and the safety-signal conjunction. Lexical categories
        // for the gate are the union across lexical candidates that
        // overlap the segment span (Tier 1-only segments carry an empty
        // set — this is honest: no lexical evidence exists).
        var newWindows: [AdWindow] = []
        for segment in surviving {
            let overlappingCategories = lexicalCandidates
                .filter { lc in
                    // Half-open overlap on [start, end).
                    lc.endTime > segment.startTime && lc.startTime < segment.endTime
                }
                .reduce(into: Set<LexicalPatternCategory>()) { acc, lc in
                    acc.formUnion(lc.categories)
                }
            let gateLabel = await precisionGateLabel(
                analysisAssetId: analysisAssetId,
                startTime: segment.startTime,
                endTime: segment.endTime,
                segmentScore: segment.segmentScore,
                lexicalCategories: overlappingCategories
            )
            newWindows.append(
                AdWindow(
                    id: UUID().uuidString,
                    analysisAssetId: analysisAssetId,
                    startTime: segment.startTime,
                    endTime: segment.endTime,
                    confidence: segment.segmentScore,
                    boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                    decisionState: AdDecisionState.candidate.rawValue,
                    detectorVersion: config.detectorVersion,
                    advertiser: nil,
                    product: nil,
                    adDescription: nil,
                    evidenceText: nil,
                    evidenceStartTime: nil,
                    metadataSource: "none",
                    metadataConfidence: nil,
                    metadataPromptVersion: nil,
                    wasSkipped: false,
                    userDismissedBanner: false,
                    eligibilityGate: gateLabel
                )
            )
        }
        logger.info("Hot path: aggregator produced \(newWindows.count) windows (of \(surfacedSegments.count) surfaced segments, \(surfacedSegments.count - surviving.count) deduped against single-window path)")
        return newWindows
    }

    // MARK: - Correction Replay (playhead-hygc.1.8)

    /// `boundaryState` literal stamped on AdWindows produced by the
    /// correction-replay recall step. Distinct from `userMarked` (the
    /// `recordUserMarkedAd` row written when the user first reports the
    /// ad) so dogfood / NARL telemetry can attribute recall recovery to
    /// the replay path specifically.
    private static let correctionReplayBoundaryState: String = "correctionReplay"

    /// `metadataSource` stamp that mirrors `correctionReplayBoundaryState`
    /// for correction-replay rows. Keeps the `userCorrection` source
    /// (used by `recordUserMarkedAd` for the original userMarked write)
    /// distinct from this replay-derived shadow row.
    private static let correctionReplayMetadataSource: String = "userCorrectionReplay"

    /// Build mark-only AdWindows from `.falseNegative` `.exactTimeSpan`
    /// correction events that have no overlapping AdWindow on the asset.
    ///
    /// Why this exists: a `.falseNegative` correction is the user's
    /// strongest possible label — they explicitly said "this WAS an ad."
    /// In the live runtime path this becomes a `userMarked` AdWindow
    /// inside `recordUserMarkedAd`, so subsequent runs of the same
    /// episode see the row via `fetchAdWindows`. But for any analysis
    /// SQLite that arrives mid-pipeline (cross-launch preload, NARL
    /// dogfood replay, schema-rebased restore) where the userMarked
    /// AdWindow row is missing while the correction event survives, we
    /// MUST still surface a candidate or the recall metric drops on
    /// every replay.
    ///
    /// Idempotency: rows whose span overlaps any existing AdWindow on
    /// the asset are skipped — including AdWindows in `.reverted` state
    /// (so a later `.falsePositive` veto via `revertByTimeRange` is not
    /// undone by the next hot-path run). The first run synthesizes the
    /// AdWindow; the second run finds it via `fetchAdWindows` and
    /// short-circuits before re-emitting it.
    ///
    /// Suppression: a `.falseNegative` correction whose time range is
    /// fully covered by any `.falsePositive` correction (regardless of
    /// relative ordering — typically the FP is recorded after the FN, but
    /// we don't depend on `createdAt` since clock skew can scramble it)
    /// is suppressed. This protects the precision contract when the user
    /// has effectively retracted their false-negative report (e.g.
    /// reported the ad, then realised it was content and vetoed it).
    ///
    /// All emitted rows are stamped with `eligibilityGate = "markOnly"`
    /// so the suggest-tier banner — and explicitly NOT auto-skip — is
    /// the surface. This is the precision-safe recall lever.
    ///
    /// Concurrency precondition: this function runs to completion under
    /// the `AdDetectionService` actor's serialization, BUT it spans
    /// multiple `await`s (`loadCorrectionEvents`, `fetchAdWindows`, and
    /// the caller's subsequent `upsertHotPathAdWindows`). Actor
    /// reentrancy could in principle let two concurrent
    /// `runHotPathResult` calls each compute fresh UUIDs from the same
    /// FN range and double-insert. The production caller chain holds the
    /// no-concurrent-runHotPath-per-asset invariant:
    ///   - `AnalysisCoordinator.handlePersistedTranscriptChunks` is
    ///     dispatched from a single `for await` loop (single-shot per
    ///     `chunksPersisted` event, serialized).
    ///   - `AnalysisJobRunner.run` invokes `runHotPath` once per asset
    ///     job and the runner is itself a serial executor.
    /// R7: the invariant is now ENFORCED — `runHotPathResult` tracks
    /// per-asset in-flight state in `hotPathRunInFlightAssetIds` and
    /// fires `assertionFailure` (DEBUG crash, RELEASE warning log) on a
    /// concurrent re-entry for the same asset. The earlier rounds (R0–R6)
    /// only documented the invariant; R7 added the runtime check so a
    /// future caller cannot quietly violate it.
    private func correctionReplayCandidates(
        analysisAssetId: String
    ) async throws -> [AdWindow] {
        let events: [CorrectionEvent]
        do {
            events = try await store.loadCorrectionEvents(
                analysisAssetId: analysisAssetId
            )
        } catch {
            logger.warning("correctionReplayCandidates: loadCorrectionEvents failed: \(error.localizedDescription, privacy: .public)")
            return []
        }
        guard !events.isEmpty else { return [] }

        // Build the set of `.exactTimeSpan` ranges that have a
        // `.falsePositive` correction. These mask `.falseNegative`
        // ranges they fully cover so a vetoed span is never re-emitted.
        var falsePositiveRanges: [(start: Double, end: Double)] = []
        for event in events where event.correctionType == .falsePositive {
            guard let scope = CorrectionScope.deserialize(event.scope) else { continue }
            guard case .exactTimeSpan(_, let s, let e) = scope else { continue }
            falsePositiveRanges.append((s, e))
        }

        // Collect unique `.falseNegative` `.exactTimeSpan` ranges.
        var falseNegativeRanges: [(start: Double, end: Double)] = []
        var seen: Set<String> = []
        for event in events where event.correctionType == .falseNegative {
            guard let scope = CorrectionScope.deserialize(event.scope) else { continue }
            guard case .exactTimeSpan(_, let s, let e) = scope else { continue }
            // Defensive: reject non-finite or zero-duration spans.
            guard s.isFinite, e.isFinite, e > s else { continue }
            // Suppress if any falsePositive range fully covers this span.
            let suppressed = falsePositiveRanges.contains { fp in
                fp.start <= s && fp.end >= e
            }
            if suppressed { continue }
            // Dedupe by serialized %.3f-%.3f pair (matches scope serialization
            // precision). One emit per unique span even if the user reported
            // it multiple times.
            let key = String(format: "%.3f-%.3f", s, e)
            if seen.contains(key) { continue }
            seen.insert(key)
            falseNegativeRanges.append((s, e))
        }
        guard !falseNegativeRanges.isEmpty else { return [] }

        // Skip ranges that already have an overlapping AdWindow (any
        // decisionState — candidate, confirmed, applied, suppressed,
        // reverted). `.reverted` rows persist a user veto, and we MUST
        // NOT resurface those.
        let existing: [AdWindow]
        do {
            existing = try await store.fetchAdWindows(assetId: analysisAssetId)
        } catch {
            logger.warning("correctionReplayCandidates: fetchAdWindows failed: \(error.localizedDescription, privacy: .public)")
            return []
        }

        var emitted: [AdWindow] = []
        let detectorVersion = config.detectorVersion
        for (start, end) in falseNegativeRanges {
            let overlapsExisting = existing.contains { window in
                window.startTime < end && window.endTime > start
            }
            if overlapsExisting { continue }
            // playhead-hygc.1.8 (R7): dedupe-by-overlap across the in-flight
            // emit set. The exact-match `seen` key above (`%.3f-%.3f`)
            // catches duplicate user reports with identical span numerics
            // but not slightly-different overlapping spans (e.g. a single
            // ad reported with ranges [600, 680] and [610, 690]). Without
            // this guard both would persist, leaving two suggest banners
            // — and an `acceptSuggestedSkip` on either would only veto its
            // own row. Skip any FN range that already overlaps a row we
            // just queued in this same call.
            let overlapsEmitted = emitted.contains { window in
                window.startTime < end && window.endTime > start
            }
            if overlapsEmitted { continue }
            let id = UUID().uuidString
            let row = AdWindow(
                id: id,
                analysisAssetId: analysisAssetId,
                startTime: start,
                endTime: end,
                confidence: 1.0,
                boundaryState: Self.correctionReplayBoundaryState,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: detectorVersion,
                advertiser: nil,
                product: nil,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: start,
                metadataSource: Self.correctionReplayMetadataSource,
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
                catalogStoreMatchSimilarity: nil
            )
            emitted.append(row)
        }
        return emitted
    }

    private func boundarySingletonPromotedSegments(
        tier1Results: [ClassifierResult],
        existingSegments: [AdSegmentCandidate],
        episodeDuration: Double
    ) -> [AdSegmentCandidate] {
        guard episodeDuration > 0 else { return [] }

        let slotFraction = AutoSkipPrecisionGateConfig.default.slotFraction
        let startWindow = min(
            Self.boundarySingletonStartWindowSeconds,
            max(AdDetectionService.tier1DefaultWindowSeconds, episodeDuration * slotFraction)
        )
        let endWindow = min(
            Self.boundarySingletonEndWindowSeconds,
            max(AdDetectionService.tier1DefaultWindowSeconds, episodeDuration * slotFraction)
        )
        let scoreFloor = AutoSkipPrecisionGateConfig.default.uiCandidateThreshold
        let gapTolerance = SegmentAggregatorConfig.default.maxInternalGapSeconds

        let boundaryCandidates = tier1Results
            .filter { result in
                let duration = result.endTime - result.startTime
                guard duration >= Self.boundarySingletonMinimumWindowSeconds,
                      result.adProbability >= scoreFloor
                else { return false }

                let center = (result.startTime + result.endTime) / 2
                let inStartSlot = center <= startWindow
                let inEndSlot = center >= (episodeDuration - endWindow)
                guard inStartSlot || inEndSlot else { return false }

                return !existingSegments.contains { segment in
                    result.endTime > segment.startTime && result.startTime < segment.endTime
                }
            }
            .sorted {
                if $0.startTime != $1.startTime { return $0.startTime < $1.startTime }
                return $0.endTime < $1.endTime
            }

        guard !boundaryCandidates.isEmpty else { return [] }

        var groups: [[ClassifierResult]] = []
        for result in boundaryCandidates {
            if let lastGroup = groups.last,
               let last = lastGroup.last,
               result.startTime <= last.endTime + gapTolerance {
                groups[groups.count - 1].append(result)
            } else {
                groups.append([result])
            }
        }

        return groups.compactMap { group in
            guard let first = group.first, let last = group.last else { return nil }
            let weighted = group.reduce(into: (sum: 0.0, duration: 0.0)) { acc, result in
                let duration = max(0, result.endTime - result.startTime)
                acc.sum += max(0, min(1, result.adProbability)) * duration
                acc.duration += duration
            }
            guard weighted.duration >= GlobalPriorDefaults.standard.typicalAdDuration.lowerBound,
                  weighted.duration > 0
            else { return nil }

            let score = weighted.sum / weighted.duration
            guard score >= scoreFloor else { return nil }
            return AdSegmentCandidate(
                startTime: first.startTime,
                endTime: last.endTime,
                segmentScore: score,
                windowCount: group.count,
                promoted: true
            )
        }
    }

    /// Decision-log `finalDecision.action` string stamped on aggregator-
    /// promoted segments so replay tooling can filter them out vs. single-
    /// window promotions. playhead-0usd.
    static let segmentAggregatorPromotedAction: String = "segmentAggregatorPromoted"

    // MARK: - AdWindow Construction (hot path)

    /// Build an `AdWindow` from a classifier result.
    ///
    /// - Parameters:
    ///   - expandedStartTime: Optional override for the persisted start time
    ///     produced by `PostClassifyBoundaryExpansion`. When nil, the classifier
    ///     result's own `startTime` is used. playhead-gtt9.4.1.
    ///   - expandedEndTime: Optional override for the persisted end time. Same
    ///     contract as `expandedStartTime`.
    ///   - eligibilityGate: playhead-gtt9.11 precision-gate stamp. "autoSkip"
    ///     admits the window to `SkipOrchestrator.receiveAdWindows` auto-skip
    ///     path; "markOnly" keeps it visible as a UI marker but blocks
    ///     auto-skip. Nil preserves legacy behavior (no stamp).
    private func buildAdWindow(
        from result: ClassifierResult,
        boundaryState: AdBoundaryState,
        decisionState: AdDecisionState,
        evidenceText: String?,
        evidenceStartTime: Double?,
        expandedStartTime: Double? = nil,
        expandedEndTime: Double? = nil,
        eligibilityGate: String? = nil
    ) -> AdWindow {
        AdWindow(
            id: UUID().uuidString,
            analysisAssetId: result.analysisAssetId,
            startTime: expandedStartTime ?? result.startTime,
            endTime: expandedEndTime ?? result.endTime,
            confidence: result.adProbability,
            boundaryState: boundaryState.rawValue,
            decisionState: decisionState.rawValue,
            detectorVersion: config.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: evidenceText,
            evidenceStartTime: evidenceStartTime,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: eligibilityGate
        )
    }

    // MARK: - Precision-gate wiring (playhead-gtt9.11)

    /// playhead-gtt9.11: consult the `AutoSkipPrecisionGate` for a
    /// prospective hot-path AdWindow. Returns the string label to stamp on
    /// `AdWindow.eligibilityGate` ("autoSkip" when the gate admits the
    /// window to auto-skip, "markOnly" when the gate demotes it to UI-only,
    /// nil when the gate says detection-only — in which case callers should
    /// NOT persist a window).
    ///
    /// Inputs fetched here (not passed in) are those the call sites don't
    /// already carry. Keeping the fetch inside this helper avoids threading
    /// the full input surface through every AdWindow construction site.
    ///
    /// - Parameter analysisAssetId: asset for feature-window + correction-
    ///   store queries.
    /// - Parameter startTime: window start time in episode audio seconds.
    /// - Parameter endTime: window end time in episode audio seconds.
    /// - Parameter segmentScore: the confidence value that drives the gate's
    ///   threshold comparison (classifier `adProbability` for single-window,
    ///   `segmentScore` for aggregator).
    /// - Parameter lexicalCategories: union of lexical-pattern categories
    ///   associated with any evidence seeding this window. Aggregator path
    ///   passes an empty set when Tier 1 alone drove the segment (no
    ///   lexical evidence exists in that case — this is honest signal
    ///   absence, not a stub). Single-window path passes
    ///   `LexicalCandidate.categories` from the seeding candidate.
    /// - Returns: `"autoSkip"`, `"markOnly"`, or `nil`.
    private func precisionGateLabel(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        segmentScore: Double,
        lexicalCategories: Set<LexicalPatternCategory>
    ) async -> String? {
        let overlappingFeatureWindows: [FeatureWindow]
        do {
            overlappingFeatureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: startTime,
                to: endTime
            )
        } catch {
            logger.warning("precisionGateLabel: fetchFeatureWindows failed (continuing with empty features): \(error.localizedDescription)")
            overlappingFeatureWindows = []
        }

        // TODO(gtt9.11): correctionStore is optional and only present once
        // PlayheadRuntime installs it post-init. Absence → factor 1.0, which
        // disables the userConfirmedLocalPattern safety signal for this
        // window. This is honest: without a correction store we genuinely
        // have no user-confirmation evidence.
        //
        // playhead-rfu-sad: scope the boost to the span being evaluated.
        // Asset-wide `correctionBoostFactor` would fire
        // `userConfirmedLocalPattern` on every window in the asset once
        // any single span had been corrected, including unrelated
        // segments — defeating the precision-gate purpose. The
        // span-local overload returns > 1.0 only when a false-negative
        // correction overlaps `[startTime, endTime]`.
        let boost: Double
        if let correctionStore {
            boost = await correctionStore.correctionBoostFactor(
                for: analysisAssetId,
                overlapping: startTime,
                endTime: endTime
            )
        } else {
            boost = 1.0
        }

        let gateConfig = AutoSkipPrecisionGateConfig(
            uiCandidateThreshold: config.segmentUICandidateThreshold,
            autoSkipThreshold: config.segmentAutoSkipThreshold,
            typicalAdDuration: GlobalPriorDefaults.standard.typicalAdDuration,
            minMusicBedCoverage: AutoSkipPrecisionGateConfig.default.minMusicBedCoverage,
            slotFraction: AutoSkipPrecisionGateConfig.default.slotFraction
        )

        // playhead-2m2i: query the catalog so `SafetySignal.catalogMatch`
        // actually contributes to the auto-vs-markOnly decision. Prior to
        // this bead, `precisionGateLabel` constructed
        // `AutoSkipPrecisionGateInput` without `catalogMatchSimilarity`,
        // so the field defaulted to 0 and the catalog signal could never
        // fire from the hot path — even when the catalog had a real
        // fingerprint match for the span. Reusing
        // `overlappingFeatureWindows` (already fetched above) means the
        // catalog fingerprint is built from the same feature-window slice
        // the gate uses for the acoustic safety signal — no second
        // `fetchFeatureWindows` round trip. The catalog DB query itself
        // (SQLite) still runs, but only when a catalog store is wired.
        // Returns 0 when no catalog store is wired (preserves the
        // pre-bead behaviour byte-for-byte) or when the fingerprint is
        // zero (sparse / silent span — `AdCatalogStore.matches` would
        // refuse to match anyway).
        //
        // Show context: the hot path doesn't carry `podcastId` through
        // `runHotPath`, so this query passes `show: nil` (global match
        // across all shows). `AdCatalogStore.matches(show:)` accepts a
        // nil show and falls back to the unscoped query, which matches
        // both `show_id IS NULL` rows and any per-show entry; that is
        // strictly looser than the fusion path's per-show query (which
        // does have `podcastId` available) but still correct: a
        // cross-show fingerprint collision at the 0.80 default floor is
        // an extreme positive, and elevating those is precisely the
        // precision win the catalog signal is designed to deliver.
        let catalogMatchSimilarity: Float
        if let adCatalogStore, !overlappingFeatureWindows.isEmpty {
            let fingerprint = AcousticFingerprint.fromFeatureWindows(overlappingFeatureWindows)
            if fingerprint.isZero {
                catalogMatchSimilarity = 0
            } else {
                let matches = await adCatalogStore.matches(
                    fingerprint: fingerprint,
                    show: nil
                )
                catalogMatchSimilarity = matches.first?.similarity ?? 0
            }
        } else {
            catalogMatchSimilarity = 0
        }

        // playhead-gtt9.26: Calibrate the post-fusion classifier score
        // before it enters the gate. Cold-start (`.production` ships
        // empty) returns `.identity` so the calibrated score equals the
        // raw score and behaviour is byte-identical to pre-gtt9.26.
        // Once a fit is baked in for the active
        // (detectorVersion, buildCommitSHA), the calibrated score
        // replaces the raw score everywhere the gate compares against
        // its thresholds.
        let calibrator = classifierCalibrationProfile.calibrator(
            detectorVersion: config.detectorVersion,
            buildCommitSHA: BuildInfo.commitSHA
        )
        let calibratedScore = calibrator.calibrate(segmentScore)

        let input = AutoSkipPrecisionGateInput(
            segmentStartTime: startTime,
            segmentEndTime: endTime,
            segmentScore: calibratedScore,
            episodeDuration: episodeDuration,
            overlappingFeatureWindows: overlappingFeatureWindows,
            lexicalCategories: lexicalCategories,
            userCorrectionBoostFactor: boost,
            catalogMatchSimilarity: catalogMatchSimilarity
        )

        switch AutoSkipPrecisionGate.classify(input: input, config: gateConfig) {
        case .detectionOnly:
            return nil
        case .uiCandidate:
            return "markOnly"
        case .autoSkipEligible(let firedSignals):
            // playhead-9ro7 (cycle-2 follow-up): metadataSlotPrior fires by
            // construction whenever the segment center is in the first/last
            // `slotFraction` of the episode. The pure gate admits any
            // non-empty signal set so it preserves its single-knob
            // contract (see `AutoSkipPrecisionGateTests
            // .autoSkipAdmittedBySlotPriorPreRoll`), but the service-layer
            // policy is stricter: a slot-only autoSkip is just "score in
            // the slot," with no independent corroboration. Demote those
            // to mark-only at the helper so both the single-window path
            // (line ~1345) and the aggregator path (line ~4683) inherit
            // the rule without bespoke per-call-site overrides.
            //
            // Modeled as an explicit "no strong corroborator" check rather
            // than `firedSignals == [.metadataSlotPrior]` set-equality.
            // The set-equality form silently re-admits the moment a
            // second weak signal is introduced; the inclusion-list form
            // forces any future SafetySignal author to decide whether
            // their case counts as independent corroboration.
            let strongCorroborators: Set<SafetySignal> = [
                .strongLexicalAdPhrase,
                .sustainedAcousticAdSignature,
                .userConfirmedLocalPattern,
                .catalogMatch,
            ]
            if firedSignals.isDisjoint(with: strongCorroborators) {
                return "markOnly"
            }
            return "autoSkip"
        }
    }

    /// playhead-gtt9.4.1: compute the expanded persisted window extents for a
    /// classifier result. Fetches a wider-radius feature-window slice than the
    /// classifier used (±60 s via `BoundaryExpander.ExpansionConfig.neutral`)
    /// and delegates to `PostClassifyBoundaryExpansion.expand`.
    ///
    /// No-ops (returns the result's own extents) when the expansion
    /// preconditions inside the helper do not hold — keeps the extra DB fetch
    /// to high-confidence candidates via an up-front guard.
    private func expandedBounds(
        for result: ClassifierResult,
        analysisAssetId: String
    ) async -> (startTime: Double, endTime: Double) {
        // Up-front guard: only pay the extra feature-window fetch cost for
        // candidates that might actually expand (confidence ≥ autoSkip).
        guard result.adProbability >= config.autoSkipConfidenceThreshold else {
            return (result.startTime, result.endTime)
        }

        let typicalAdDuration = GlobalPriorDefaults.standard.typicalAdDuration
        let shortCandidateThreshold = typicalAdDuration.lowerBound / 2.0
        guard (result.endTime - result.startTime) < shortCandidateThreshold else {
            return (result.startTime, result.endTime)
        }

        let expansionConfig = BoundaryExpander.ExpansionConfig.neutral
        let expandedFrom = max(0, result.startTime - expansionConfig.acousticBackwardSearchRadius)
        let expandedTo = result.endTime + expansionConfig.acousticForwardSearchRadius

        let featureWindows: [FeatureWindow]
        do {
            featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: expandedFrom,
                to: expandedTo
            )
        } catch {
            logger.warning("PostClassifyBoundaryExpansion: feature fetch failed, keeping original extents: \(error.localizedDescription)")
            return (result.startTime, result.endTime)
        }

        return PostClassifyBoundaryExpansion.expand(
            startTime: result.startTime,
            endTime: result.endTime,
            adProbability: result.adProbability,
            featureWindows: featureWindows,
            autoSkipConfidenceThreshold: config.autoSkipConfidenceThreshold,
            typicalAdDuration: typicalAdDuration
        )
    }

    // MARK: - Metadata Extraction

    /// Extract metadata for a non-suppressed window (confirmed or candidate) and persist to SQLite.
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

    // MARK: - Prior Hierarchy Resolution (playhead-084j)

    /// Resolve the 4-level prior hierarchy for the current episode.
    ///
    /// Called from `runBackfill` exactly once per episode (outside the
    /// per-span loop). The result feeds `DurationPrior(resolvedPriors:)` so
    /// fusion is show-aware, not stuck on `GlobalPriorDefaults.standard`.
    ///
    /// Audit (as of spxs):
    ///   • Global: always `GlobalPriorDefaults.standard`.
    ///   • Network: derived via `NetworkPriorsBuilder.build` from all
    ///     `PodcastProfile` rows that share the current show's `networkId`.
    ///     `nil` when the current show has no `networkId` recorded yet
    ///     (RSS-metadata writer lands in a follow-up bead) or when no
    ///     sibling shows in the network meet the per-show sample-count
    ///     threshold. The network fetch is `async` because of the SQL hop;
    ///     the `await` is a real reentrancy point on this actor, NOT an
    ///     atomicity guarantee. The cycle-5 L-2 fix snapshots
    ///     `networkId` and `observationCount` into locals BEFORE the await
    ///     so an interleaving turn that rewrites `currentPodcastProfile`
    ///     can't desync the network tier's inputs.
    ///   • Trait: `currentPodcastProfile?.traitProfile ?? .unknown`. The
    ///     persistence layer reads cleanly; profiles without a writer
    ///     fall through to `.unknown`, which is non-reliable, so the trait
    ///     level stays inactive — graceful degradation.
    ///   • Show-local: derived from `currentPodcastProfile.adDurationStatsJSON`
    ///     via `ShowLocalPriorsBuilder.build`. Returns nil for shows below
    ///     `ShowLocalPriorsBuilder.minSampleCount`, which keeps the resolver
    ///     at global defaults until enough confirmed ads have been observed.
    ///
    /// Failure semantics: this method does not throw. Corrupt JSON, missing
    /// columns, network-fetch errors, or any other malformed input results
    /// in `nil` priors at that tier, and the resolver falls through to the
    /// next tier (graceful degradation).
    ///
    /// Snapshot consistency: the `await` on `store.fetchProfiles(forNetworkId:)`
    /// can interleave with other turns on this actor, so this method does
    /// NOT execute as a single uninterrupted actor turn. Instead, the
    /// network tier's two inputs (`networkId` and `observationCount`) are
    /// snapshotted into locals before the await; the resolver call below
    /// is pure. Tests pin the snapshot pattern via
    /// `resolveEpisodePriorsSnapshotsObservationCountPreAwait` (cycle-6)
    /// and the `mutateProfile`-style write-side canaries in
    /// `AdDetectionServiceUpdatePriorsAtomicityCanaryTests`.
    private func resolveEpisodePriors() async -> ResolvedPriors {
        let traitProfile = currentPodcastProfile?.traitProfile ?? .unknown
        let showLocal = ShowLocalPriorsBuilder.build(from: currentPodcastProfile)

        // playhead-spxs: gather the network tier. Skipped when the current
        // show has no networkId recorded — falls back to nil which the
        // resolver treats as "tier inactive". Errors in the SQL fetch are
        // also treated as "tier inactive" (logged then dropped) so a
        // transient persistence failure can't block ad detection.
        //
        // cycle-5 L-2: snapshot `networkId` AND `observationCount` from
        // `currentPodcastProfile` BEFORE the `fetchProfiles` await.
        // The await is a real reentrancy point on this actor, so an
        // interleaving `updatePriors` / `recordSuccessfulObservation`
        // turn could rewrite `currentPodcastProfile` between the two
        // reads. Snapshotting both fields up front keeps the network
        // tier's decay weight consistent with the networkId it was
        // computed from — the snapshot-consistency contract that the
        // resolver header documents.
        var networkPriors: NetworkPriors? = nil
        var networkDecay: Float = 0
        if let snapshotProfile = currentPodcastProfile,
           let networkId = snapshotProfile.networkId,
           !networkId.isEmpty {
            let observedAtSnapshot = snapshotProfile.observationCount
            do {
                let siblings = try await store.fetchProfiles(forNetworkId: networkId)
                if let priors = NetworkPriorsBuilder.build(from: siblings) {
                    networkPriors = priors
                    networkDecay = NetworkPriors.decayedWeight(episodesObserved: observedAtSnapshot)
                }
            } catch {
                logger.warning("Failed to fetch network siblings for networkId=\(networkId): \(error.localizedDescription)")
            }
        }

        return PriorHierarchyResolver.resolve(
            globalDefaults: .standard,
            networkPriors: networkPriors,
            networkDecay: networkDecay,
            traitProfile: traitProfile,
            showLocalPriors: showLocal
        )
    }

    #if DEBUG
    /// Test-only entry point that mirrors the resolver call inside
    /// `runBackfill` without requiring a full backfill setup. Locks the
    /// invariant that the production wire-up uses the in-actor profile and
    /// produces a `ResolvedPriors` matching what the fusion path consumes.
    ///
    /// `#if DEBUG` matches the existing pattern for other `*ForTesting`
    /// entry points in this file (see `acousticFunnelForTesting` etc.).
    func resolveEpisodePriorsForTesting() async -> ResolvedPriors {
        await resolveEpisodePriors()
    }

    /// Test-only entry point that drives `updatePriors` end-to-end.
    /// Locks the wire-up of `adDurationStatsJSON` accumulation through the
    /// actual create / update closures inside `mutateProfile` — the same
    /// path `runBackfill` exercises post-fusion.
    ///
    /// cycle-1 L4: accepts `featureWindows` and `chunks` so tests can drive
    /// the trait-snapshot derivations with realistic signal (rather than
    /// defaulting to empty arrays, which collapse every snapshot to the
    /// no-signal neutral defaults and never exercise the real producer
    /// math). No defaults — callers without a real signal pass `[]`
    /// explicitly so the choice is visible at the call site.
    func updatePriorsForTesting(
        podcastId: String,
        nonSuppressedWindows: [AdWindow],
        episodeDuration: Double,
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk]
    ) async throws {
        try await updatePriors(
            podcastId: podcastId,
            nonSuppressedWindows: nonSuppressedWindows,
            episodeDuration: episodeDuration,
            featureWindows: featureWindows,
            chunks: chunks
        )
    }
    #endif

    // MARK: - Prior Updates

    /// Update PodcastProfile priors from confirmed ad windows.
    /// Learns ad slot positions and sponsor names over time.
    ///
    /// skeptical-review-cycle-15 M-1 / M-2: routed through
    /// `store.mutateProfile` so the read-modify-write happens inside one
    /// AnalysisStore actor turn. Two earlier defects this closes:
    ///
    ///   • M-1 (lost-update race): the previous body did `await
    ///     store.fetchProfile()` then `await store.upsertProfile()` as
    ///     two independent actor hops. A concurrent
    ///     `TrustScoringService.recordFalseSkipSignal` (which is itself
    ///     atomic via `updateProfileIfExistsCapturing`) landing between
    ///     those hops would be silently overwritten by the carry-forward
    ///     upsert below. Post-q45f the writer policy is single-source:
    ///     TrustScoringService is the sole writer of `skipTrustScore`
    ///     (via `recordSuccessfulObservation`, `recordFalseSkipSignal`,
    ///     and the new `recordWeakFalseSkipSignal` that listen-rewind
    ///     now delegates to). `updatePriors` itself does not write
    ///     `skipTrustScore` — it only carries the existing value
    ///     through, so any concurrent decrement must remain visible
    ///     after this update commits.
    ///
    ///   • M-2 (traitProfileJSON clobber): the previous
    ///     `PodcastProfile(...)` constructor here omitted
    ///     `traitProfileJSON`. The default initializer parameter is
    ///     `nil`, and `upsertProfile`'s SQL writes
    ///     `traitProfileJSON = excluded.traitProfileJSON` (NOT
    ///     COALESCE), so every priors update silently nilled the
    ///     persisted trait profile. The `update` closure below
    ///     explicitly carries `existing.traitProfileJSON` forward.
    ///     (`title` was COALESCE-safe but is also passed through for
    ///     symmetry / defensiveness.)
    /// cycle-1 L3: `featureWindows` and `chunks` are REQUIRED — no
    /// defaults. The single production caller (`runBackfill`) always has
    /// the full signal vector in scope and must thread it through; the
    /// `updatePriorsForTesting` shim explicitly forwards them. Defaults
    /// of `[]` previously made it possible for a future refactor to
    /// introduce a new caller that silently dropped the signal and
    /// regressed the trait-tier activation back to the no-signal neutral
    /// defaults — invisible to most behavioral tests because the EMA
    /// path still increments `episodesObserved`. Required parameters
    /// surface the choice at the call site.
    private func updatePriors(
        podcastId: String,
        nonSuppressedWindows: [AdWindow],
        episodeDuration: Double,
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk]
    ) async throws {
        guard !nonSuppressedWindows.isEmpty, episodeDuration > 0 else { return }

        // cycle-1 residual log: surface the trait-snapshot input cardinalities
        // at the point of consumption so an empty-input regression (a future
        // caller that drops the signal vector and silently regresses the
        // trait tier to no-signal neutral defaults) is visible in
        // DiagnosticReports / `log show` queries without a debugger
        // attached. Counts only — no payload content — so privacy-public is
        // safe.
        logger.debug(
            "[traitSnapshot] featureWindows=\(featureWindows.count, privacy: .public) chunks=\(chunks.count, privacy: .public)"
        )

        // Compute normalized ad slot positions from confirmed windows.
        // These do not depend on the existing profile so we compute them
        // once outside the closure (also keeps the closure simple).
        let newSlotPositions = nonSuppressedWindows.map { window in
            let center = (window.startTime + window.endTime) / 2.0
            return center / episodeDuration
        }

        // Collect advertiser names from confirmed windows with metadata.
        let newSponsors = nonSuppressedWindows
            .compactMap(\.advertiser)
            .map { $0.lowercased() }

        // playhead-084j: capture the per-window durations of every confirmed
        // ad in this episode so the show-local `AdDurationStats` aggregate
        // can extend with a streaming-mean update inside the mutate closure.
        // We filter to finite, positive durations to keep the EMA from being
        // perturbed by malformed window rows.
        let newAdDurations: [TimeInterval] = nonSuppressedWindows.compactMap { window in
            let d = window.endTime - window.startTime
            guard d.isFinite, d > 0 else { return nil }
            return d
        }

        let updatedProfile: PodcastProfile
        do {
            updatedProfile = try await store.mutateProfile(
                podcastId: podcastId,
                create: {
                    let initialSlotsJSON: String?
                    if let data = try? JSONEncoder().encode(newSlotPositions) {
                        initialSlotsJSON = String(data: data, encoding: .utf8)
                    } else {
                        initialSlotsJSON = nil
                    }
                    let initialSponsors: String? = newSponsors.isEmpty
                        ? nil
                        : Set(newSponsors).sorted().joined(separator: ",")

                    // playhead-084j: encode the brand-new ad-duration aggregate
                    // from this episode's observations. nil if no usable
                    // durations were captured (the COALESCE in upsertProfile
                    // will then leave the persisted column NULL — which is
                    // also what `ShowLocalPriorsBuilder.build` expects for a
                    // fresh show).
                    let initialAdDurationStatsJSON = Self.encodeAdDurationStats(
                        merging: .empty,
                        with: newAdDurations
                    )

                    // playhead-v7v8: bootstrap the trait profile from this
                    // episode's signal. Without this seed, a fresh show
                    // would walk through `episodesObserved == 1` carrying a
                    // .unknown profile (since the EMA only fires on
                    // `update:` rebases), forcing the resolver back to the
                    // global tier even though we already have one
                    // observation in hand. The first-episode merge below
                    // mirrors `ShowTraitProfile.updated(from:)` for the
                    // sentinel case (replace, don't blend).
                    let initialTraitProfileJSON = Self.initialTraitProfileJSON(
                        featureWindows: featureWindows,
                        chunks: chunks,
                        confirmedAdWindows: nonSuppressedWindows,
                        episodeDuration: episodeDuration
                    )

                    // Bug 4a default: brand-new profile gets trust=0.5
                    // (matches `setUserOverride`'s new-profile default).
                    // TrustScoringService.recordSuccessfulObservation
                    // owns subsequent trust adjustments.
                    return PodcastProfile(
                        podcastId: podcastId,
                        sponsorLexicon: initialSponsors,
                        normalizedAdSlotPriors: initialSlotsJSON,
                        repeatedCTAFragments: nil,
                        jingleFingerprints: nil,
                        implicitFalsePositiveCount: 0,
                        skipTrustScore: 0.5,
                        observationCount: 1,
                        mode: "shadow",
                        recentFalseSkipSignals: 0,
                        traitProfileJSON: initialTraitProfileJSON,
                        adDurationStatsJSON: initialAdDurationStatsJSON,
                        // playhead-spxs: brand-new profile starts with
                        // `networkId == nil`. A future bead populates it
                        // from RSS metadata via NetworkIdentityExtractor;
                        // until then the network-priors tier remains a
                        // graceful no-op for first-observation profiles.
                        networkId: nil
                    )
                },
                update: { existing in
                    // Merge slot positions (exponential moving average).
                    let existingSlots: [Double]
                    if let json = existing.normalizedAdSlotPriors,
                       let data = json.data(using: .utf8),
                       let decoded = try? JSONDecoder().decode([Double].self, from: data) {
                        existingSlots = decoded
                    } else {
                        existingSlots = []
                    }
                    let mergedSlots = Self.mergeSlotPositions(
                        existing: existingSlots,
                        new: newSlotPositions
                    )
                    let slotsJSON: String?
                    if let data = try? JSONEncoder().encode(mergedSlots) {
                        slotsJSON = String(data: data, encoding: .utf8)
                    } else {
                        slotsJSON = nil
                    }

                    // Merge sponsor lexicon.
                    let mergedSponsorLexicon: String?
                    if let lex = existing.sponsorLexicon {
                        let existingNames = Set(
                            lex.components(separatedBy: ",")
                                .map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
                                .filter { !$0.isEmpty }
                        )
                        let allNames = existingNames.union(newSponsors)
                        mergedSponsorLexicon = allNames.sorted().joined(separator: ",")
                    } else if !newSponsors.isEmpty {
                        mergedSponsorLexicon = Set(newSponsors).sorted().joined(separator: ",")
                    } else {
                        mergedSponsorLexicon = existing.sponsorLexicon
                    }

                    // playhead-084j: extend the persisted `AdDurationStats`
                    // aggregate with this episode's confirmed-ad durations.
                    // We carry the existing JSON forward when no new
                    // observations were captured AND no decode-able prior
                    // exists, so a `nil` or corrupt prior value is recovered
                    // on the next episode that DOES carry observations.
                    let mergedAdDurationStatsJSON: String? = {
                        let existingStats = Self.decodeAdDurationStats(existing.adDurationStatsJSON)
                            ?? .empty
                        if newAdDurations.isEmpty {
                            // Preserve prior column verbatim so a backfill
                            // with no new ads doesn't blank out history.
                            return existing.adDurationStatsJSON
                        }
                        return Self.encodeAdDurationStats(
                            merging: existingStats,
                            with: newAdDurations
                        )
                    }()

                    // playhead-v7v8: derive the per-episode trait snapshot
                    // from the live signal that flowed into this backfill,
                    // then advance the persisted ShowTraitProfile via the
                    // existing EMA path on `ShowTraitProfile.updated(from:)`.
                    // Done INSIDE the mutate closure so the read-modify-write
                    // is part of the same AnalysisStore actor turn — same
                    // atomicity contract that cycles 15/17 enforced for the
                    // sponsorLexicon/slot priors.
                    //
                    // cycle-2 L1: compute the resolved trait JSON once via
                    // nil-coalescing (`merged ?? existing.traitProfileJSON`)
                    // rather than rebinding `existing` to an alias profile.
                    // The carry-forward is still load-bearing — the
                    // cycle-22 L-5 whole-file canary requires every
                    // PodcastProfile-constructing `existing in` closure to
                    // mention `<ident>.traitProfileJSON` somewhere in its
                    // body, which the `?? existing.traitProfileJSON`
                    // fallback satisfies on both branches.
                    let mergedTraitProfileJSON = Self.mergedTraitProfileJSON(
                        existing: existing,
                        featureWindows: featureWindows,
                        chunks: chunks,
                        confirmedAdWindows: nonSuppressedWindows,
                        episodeDuration: episodeDuration
                    )
                    let resolvedTraitProfileJSON = mergedTraitProfileJSON ?? existing.traitProfileJSON

                    // cycle-2 M2: in DEBUG, assert that a successful merge
                    // never regresses `episodesObserved`. The EMA path
                    // (`ShowTraitProfile.updated(from:)`) increments by 1
                    // on each call, so any drop signals a serialization or
                    // version-skew bug that would otherwise silently
                    // corrupt the persisted profile.
                    #if DEBUG
                    if let merged = mergedTraitProfileJSON,
                       let mergedData = merged.data(using: .utf8),
                       let mergedProfile = try? JSONDecoder().decode(
                           ShowTraitProfile.self, from: mergedData
                       ) {
                        assert(
                            mergedProfile.episodesObserved >= existing.traitProfile.episodesObserved,
                            "cycle-2 M2: merged traitProfile.episodesObserved (\(mergedProfile.episodesObserved)) regressed below existing (\(existing.traitProfile.episodesObserved)) for podcast \(podcastId)"
                        )
                    }
                    #endif

                    // Bug 4a (trust carry-forward): updatePriors does not
                    // touch `skipTrustScore`. The two writers under the
                    // current policy (C26 H-1, playhead-od4j; see
                    // `recordListenRewind` docstring for the full
                    // contract) are `TrustScoringService` and
                    // `recordListenRewind`; we copy the existing value
                    // through so neither writer's decrement is silently
                    // overwritten by this priors update.
                    return PodcastProfile(
                        podcastId: existing.podcastId,
                        sponsorLexicon: mergedSponsorLexicon,
                        normalizedAdSlotPriors: slotsJSON,
                        repeatedCTAFragments: existing.repeatedCTAFragments,
                        jingleFingerprints: existing.jingleFingerprints,
                        implicitFalsePositiveCount: existing.implicitFalsePositiveCount,
                        skipTrustScore: existing.skipTrustScore,
                        observationCount: existing.observationCount + 1,
                        mode: existing.mode,
                        recentFalseSkipSignals: existing.recentFalseSkipSignals,
                        traitProfileJSON: resolvedTraitProfileJSON,
                        title: existing.title,
                        adDurationStatsJSON: mergedAdDurationStatsJSON,
                        // playhead-spxs: carry-forward (mirror of
                        // `adDurationStatsJSON`). `networkId` is
                        // COALESCE-protected in `upsertProfile`, but
                        // explicit carry-forward keeps every constructor
                        // in this file consistent with the canary
                        // contract — see
                        // `AdDetectionServiceUpdatePriorsAtomicityCanaryTests`.
                        networkId: existing.networkId
                    )
                }
            )
        }

        // Refresh the in-memory priors for subsequent use.
        showPriors = ShowPriors.from(profile: updatedProfile)
        scanner = LexicalScanner(podcastProfile: updatedProfile)
        currentPodcastProfile = updatedProfile

        logger.info("Updated priors for podcast \(podcastId): observations=\(updatedProfile.observationCount) trust=\(updatedProfile.skipTrustScore, format: .fixed(precision: 2))")
    }

    // MARK: - Trait profile merge helpers (playhead-v7v8)
    //
    // Both helpers are `private static` so the `store.mutateProfile`
    // closures (which run inside the `AnalysisStore` actor) can call them
    // without capturing `self`. Same constraint and rationale as
    // `mergeSlotPositions` and the `AdDurationStats` helpers below.

    /// Build the trait snapshot for this episode and merge it into the
    /// existing profile's persisted `ShowTraitProfile` via the standard
    /// EMA path (`ShowTraitProfile.updated(from:)`). Returns the encoded
    /// JSON, or `nil` if encoding fails (which leaves the existing
    /// `traitProfileJSON` undisturbed in the carry-forward path).
    ///
    /// Called inside the `update` closure of `mutateProfile`, where the
    /// snapshot is derived from THIS episode's signal but the EMA target
    /// is the stored profile's prior trait state.
    private static func mergedTraitProfileJSON(
        existing: PodcastProfile,
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk],
        confirmedAdWindows: [AdWindow],
        episodeDuration: Double
    ) -> String? {
        let snapshot = EpisodeTraitSnapshotBuilder.build(
            featureWindows: featureWindows,
            chunks: chunks,
            confirmedAdWindows: confirmedAdWindows,
            existingProfile: existing,
            episodeDuration: episodeDuration
        )
        let mergedProfile = existing.traitProfile.updated(from: snapshot)
        // cycle-2 M1: surface encode failures so a silent `nil` return
        // (which leaves the existing `traitProfileJSON` undisturbed) is
        // still visible in DiagnosticReports / `log show` queries. The
        // `nil` semantic is preserved because callers depend on it.
        do {
            let data = try JSONEncoder().encode(mergedProfile)
            guard let json = String(data: data, encoding: .utf8) else {
                staticLogger.error(
                    "[traitSnapshot] mergedTraitProfileJSON: utf8 conversion produced no string"
                )
                return nil
            }
            return json
        } catch {
            staticLogger.error(
                "[traitSnapshot] mergedTraitProfileJSON: encode failed: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// Build the seed trait snapshot for a brand-new profile and encode
    /// it. The first-episode merge of `ShowTraitProfile.unknown.updated(
    /// from: snapshot)` replaces the sentinel directly (no blending), so
    /// the result is just the snapshot promoted to a one-episode profile.
    private static func initialTraitProfileJSON(
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk],
        confirmedAdWindows: [AdWindow],
        episodeDuration: Double
    ) -> String? {
        let snapshot = EpisodeTraitSnapshotBuilder.build(
            featureWindows: featureWindows,
            chunks: chunks,
            confirmedAdWindows: confirmedAdWindows,
            existingProfile: nil,
            episodeDuration: episodeDuration
        )
        let seedProfile = ShowTraitProfile.unknown.updated(from: snapshot)
        // cycle-2 M1: surface encode failures so the bootstrap-skip path
        // (column persisted as nil) is observable in DiagnosticReports
        // rather than disappearing silently.
        do {
            let data = try JSONEncoder().encode(seedProfile)
            guard let json = String(data: data, encoding: .utf8) else {
                staticLogger.error(
                    "[traitSnapshot] initialTraitProfileJSON: utf8 conversion produced no string"
                )
                return nil
            }
            return json
        } catch {
            staticLogger.error(
                "[traitSnapshot] initialTraitProfileJSON: encode failed: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// Merge new slot positions with existing ones. Deduplicates slots that
    /// are within 5% of each other (same ad slot across episodes).
    ///
    /// skeptical-review-cycle-15 M-1: declared `static` so the
    /// `store.mutateProfile` closure (which runs inside the AnalysisStore
    /// actor) can call it without capturing `self`.
    private static func mergeSlotPositions(
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

    // playhead-084j: declared `private static` so the `store.mutateProfile`
    // closures can call them without capturing `self` (the closures run
    // inside the AnalysisStore actor, not the AdDetectionService actor —
    // same constraint as `mergeSlotPositions` above). `private` matches
    // `mergeSlotPositions`'s access level so neither helper leaks to the
    // app target's public surface.

    /// Decode a persisted `AdDurationStatsJSON` value into the typed struct,
    /// or `nil` when the column is empty / malformed.
    ///
    /// cycle-1 M2: a malformed payload silently returns `nil`, which lets
    /// the resolver fall through to global defaults — but the corrupt
    /// JSON stays on the column and every backfill thereafter pays the
    /// same decode cost without ever surfacing the corruption to a
    /// diagnostic report. Log decode failures at `.error` so the corruption
    /// is visible in DiagnosticReports / `log show` queries. Empty/nil
    /// payloads are NOT logged (the column is unset for any new show
    /// and that is the expected steady state, not a failure).
    private static func decodeAdDurationStats(_ json: String?) -> AdDurationStats? {
        guard let json, !json.isEmpty else { return nil }
        guard let data = json.data(using: .utf8) else {
            staticLogger.error(
                "[AdDurationStats] decode failed: utf8 conversion produced no data"
            )
            return nil
        }
        do {
            return try JSONDecoder().decode(AdDurationStats.self, from: data)
        } catch {
            staticLogger.error(
                "[AdDurationStats] decode failed: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// Encode a freshly-merged `AdDurationStats` for persistence, or `nil`
    /// when no new observations would change the aggregate AND the existing
    /// aggregate is empty (so we don't write `{"meanDuration":0,"sampleCount":0}`).
    ///
    /// cycle-1 L4: uses the default `JSONEncoder` formatting (compact,
    /// no pretty-printing). The column footprint is intentionally
    /// minimal — two scalar fields — and pretty-printed output would
    /// only add bytes without benefiting any consumer (the value is
    /// never inspected by humans through the column). DiagnosticsExport
    /// pretty-prints separately at export time when readability matters.
    private static func encodeAdDurationStats(
        merging existing: AdDurationStats,
        with newDurations: [TimeInterval]
    ) -> String? {
        let merged = ShowLocalPriorsBuilder.mergeDurations(
            existing: existing,
            newDurations: newDurations
        )
        guard merged.sampleCount > 0 else { return nil }
        guard let data = try? JSONEncoder().encode(merged),
              let json = String(data: data, encoding: .utf8)
        else { return nil }
        return json
    }
}

// MARK: - AdDetectionProviding Conformance

extension AdDetectionService: AdDetectionProviding {}

// MARK: - PostClassifyBoundaryExpansion (playhead-gtt9.4.1)

/// Stateless helper that widens an AdWindow's persisted [startTime, endTime]
/// when the classifier produced a high-confidence hit on a narrow
/// LexicalCandidate (2-s window) inside a wider ad envelope.
///
/// Context (2026-04-24 Conan 71F0C2AE regression):
/// the classifier only scores on LexicalCandidate windows (2 s wide), so a
/// GT ad span of 30 s that matches only a single lexical hit gets persisted
/// as a 2-s AdWindow against the 30-s truth — Sec-F1 caps around 0.04.
///
/// Surface fix: when the classifier clears `autoSkipConfidenceThreshold`
/// (0.80 by default) on a candidate whose duration is shorter than
/// `typicalAdDuration.lowerBound / 2` (15 s by default), look for acoustic
/// breaks within `BoundaryExpander.ExpansionConfig.neutral` radii and expand
/// the persisted extents outward to them. Fallback to a `typicalAdDuration`-
/// wide extent centered on the candidate midpoint when no breaks are found.
///
/// Does NOT:
/// - rescore the expanded span
/// - change the `adProbability` attached to the AdWindow
/// - modify the classifier candidate's own boundaries (that's a different
///   layer — `BoundaryRefiner`)
/// - touch the evidence ledger
///
/// Downstream window reconciliation (`reconcileHotPathWindows`) already merges
/// overlapping windows, so independent expansion of adjacent high-confidence
/// candidates is safe — overlaps collapse at persistence time.
enum PostClassifyBoundaryExpansion {

    /// Expand the persisted window extents for a high-confidence, narrow
    /// classifier hit. Returns the original extents unchanged when the
    /// expansion preconditions do not hold.
    ///
    /// - Parameters:
    ///   - startTime: Classifier result start time (seconds).
    ///   - endTime: Classifier result end time (seconds).
    ///   - adProbability: Classifier ad probability.
    ///   - featureWindows: Acoustic feature windows in the vicinity of the
    ///     classifier hit. Expansion searches for AcousticBreaks within these.
    ///   - autoSkipConfidenceThreshold: Confidence threshold above which a
    ///     candidate is eligible for expansion (default 0.80 per
    ///     `AdDetectionConfig.autoSkipConfidenceThreshold`).
    ///   - typicalAdDuration: Prior on ad duration in seconds. Used twice:
    ///     (1) gate: expand only when candidate duration < lowerBound / 2.
    ///     (2) fallback extent when no acoustic break is found on a side.
    /// - Returns: Expanded `(startTime, endTime)` tuple. Returns the original
    ///   bounds unchanged when the confidence gate, duration gate, or
    ///   (bounded) non-inversion safety checks would be violated.
    static func expand(
        startTime: Double,
        endTime: Double,
        adProbability: Double,
        featureWindows: [FeatureWindow],
        autoSkipConfidenceThreshold: Double,
        typicalAdDuration: ClosedRange<TimeInterval>
    ) -> (startTime: Double, endTime: Double) {
        // Confidence gate: only expand when the classifier is confident enough
        // that a false positive is unlikely. At 0.80 (default) + the broad
        // feature-window scan, a spurious expansion onto a silent show-intro
        // gap is ~order-of-magnitude rarer than the narrow-hit problem we are
        // fixing.
        guard adProbability >= autoSkipConfidenceThreshold else {
            return (startTime, endTime)
        }

        let duration = endTime - startTime

        // Duration gate: only expand when the candidate is materially shorter
        // than a typical ad. `typicalAdDuration.lowerBound / 2` is the most
        // conservative interpretation of "shorter than half a typical ad"
        // (default 30/2 = 15 s). Candidates already wider than 15 s are left
        // alone — they plausibly cover most of the real span already.
        let shortCandidateThreshold = typicalAdDuration.lowerBound / 2.0
        guard duration < shortCandidateThreshold else {
            return (startTime, endTime)
        }

        // Non-finite / degenerate durations short-circuit to no-op.
        guard duration.isFinite, duration >= 0 else {
            return (startTime, endTime)
        }

        let expansionConfig = BoundaryExpander.ExpansionConfig.neutral
        let backwardRadius = expansionConfig.acousticBackwardSearchRadius
        let forwardRadius = expansionConfig.acousticForwardSearchRadius

        // Narrow the feature-window input to the search envelope to keep
        // AcousticBreakDetector work bounded when callers pass a larger window.
        let searchStart = startTime - backwardRadius
        let searchEnd = endTime + forwardRadius
        let nearbyWindows = featureWindows.filter { fw in
            fw.endTime >= searchStart && fw.startTime <= searchEnd
        }

        let breaks = AcousticBreakDetector.detectBreaks(in: nearbyWindows)

        // Leading break: the nearest AcousticBreak at or before `startTime`
        // within `backwardRadius`. We anchor on `startTime` (not the center)
        // because the lexical hit typically sits at or near the leading edge
        // of the ad (greeting / sponsor name / jingle are the lexical patterns
        // that seed the candidate). Picking at-or-before avoids pulling the
        // start forward into the ad body.
        let leadingBreak = breaks
            .filter { $0.time <= startTime && $0.time >= startTime - backwardRadius }
            .max(by: { $0.time < $1.time }) // nearest to startTime

        // Trailing break: the nearest AcousticBreak at or after `endTime`
        // within `forwardRadius`, for symmetric reasons.
        let trailingBreak = breaks
            .filter { $0.time >= endTime && $0.time <= endTime + forwardRadius }
            .min(by: { $0.time < $1.time }) // nearest to endTime

        // Per-side fallback: `typicalAdDuration.lowerBound / 2` (15 s by
        // default) — just enough to cover the characteristic ad half-width.
        // Using lowerBound keeps the fallback conservative; using upperBound
        // or midpoint would over-expand when AcousticBreakDetector is silent
        // because features are noisy.
        let perSideFallbackWidth = typicalAdDuration.lowerBound / 2.0

        let expandedStart: Double
        if let leading = leadingBreak {
            expandedStart = leading.time
        } else {
            expandedStart = startTime - perSideFallbackWidth
        }

        let expandedEnd: Double
        if let trailing = trailingBreak {
            expandedEnd = trailing.time
        } else {
            expandedEnd = endTime + perSideFallbackWidth
        }

        // Safety: never narrow the persisted window, never invert it, never
        // produce a negative start time.
        let finalStart = max(0, min(expandedStart, startTime))
        let finalEnd = max(expandedEnd, endTime)

        return (finalStart, finalEnd)
    }
}

// MARK: - DecisionResultArtifact serialization (Bug 6)

/// Codable DTO mirroring `AdDecisionResult` for persistence in
/// `ad_decision_results.decisionJSON`.
///
/// `AdDecisionResult` itself is a runtime type used by `SkipOrchestrator` and is
/// intentionally not `Codable` — adding the conformance there would expand the
/// model's contract beyond the orchestrator's needs and pull
/// `AdDecisionEligibilityGate` into the persistence surface. Encoding through a
/// local DTO keeps the on-disk schema decoupled from the runtime struct so a
/// future field rename in `AdDecisionResult` does not silently change the JSON
/// shape that downstream consumers (replay, eval, NARL) depend on.
struct PersistedDecisionResult: Codable, Equatable {
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let skipConfidence: Double
    let eligibilityGate: String
    let recomputationRevision: Int

    init(_ result: AdDecisionResult) {
        self.id = result.id
        self.analysisAssetId = result.analysisAssetId
        self.startTime = result.startTime
        self.endTime = result.endTime
        self.skipConfidence = result.skipConfidence
        self.eligibilityGate = result.eligibilityGate.rawValue
        self.recomputationRevision = result.recomputationRevision
    }
}
