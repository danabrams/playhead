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
        segmentUICandidateThreshold: Double = 0.40,
        segmentAutoSkipThreshold: Double = 0.55
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
        self.segmentUICandidateThreshold = segmentUICandidateThreshold
        self.segmentAutoSkipThreshold = segmentAutoSkipThreshold
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
        segmentUICandidateThreshold: 0.40,
        segmentAutoSkipThreshold: 0.55
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

    /// playhead-8em9 (narL): Optional decision logger for offline replay.
    /// DEBUG-only; release builds keep the `NoOpDecisionLogger` default so
    /// no log file is ever written on a shipping binary.
    private(set) var decisionLogger: DecisionLoggerProtocol = NoOpDecisionLogger()

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
        skipOrchestrator: SkipOrchestrator? = nil,
        episodeMetadataProvider: EpisodeMetadataProvider = NullEpisodeMetadataProvider(),
        decisionLogger: DecisionLoggerProtocol? = nil
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
        self.episodeMetadataProvider = episodeMetadataProvider
        // playhead-8em9 (narL): allow the logger to be installed at init
        // time so there is no race with the first backfill. PlayheadRuntime
        // passes a real DecisionLogger under DEBUG; production and tests
        // that don't care about logging leave this nil, keeping the
        // NoOpDecisionLogger default already on `decisionLogger`.
        if let decisionLogger {
            self.decisionLogger = decisionLogger
        }
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
    }

    // MARK: - Profile Update

    /// Update the scanner and priors when the podcast profile changes.
    func updateProfile(_ profile: PodcastProfile?) {
        scanner = LexicalScanner(podcastProfile: profile)
        showPriors = ShowPriors.from(profile: profile)
        currentPodcastProfile = profile
    }

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
            let logEntry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: timestamp,
                windowBounds: .init(start: result.startTime, end: result.endTime),
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
        self.episodeDuration = episodeDuration

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
            return HotPathRunResult(windows: aggregatorWindows, retiredWindowIDs: [])
        }

        let replayCandidateIDs: Set<String>
        if retireUnmatchedReplayCandidates {
            replayCandidateIDs = try await hotPathCandidateIDs(
                analysisAssetId: analysisAssetId,
                overlapping: replayEnvelope(for: chunks)
            )
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
            return HotPathRunResult(
                windows: aggregatorWindows,
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
            return HotPathRunResult(windows: [], retiredWindowIDs: replayCandidateIDs)
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
            return HotPathRunResult(windows: [], retiredWindowIDs: replayCandidateIDs)
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

        return HotPathRunResult(
            windows: allWindowsToPersist,
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
        // playhead-z3ch: pre-compute metadata cues once per asset. The lookup
        // is feed-level (description + summary) so it has no per-span variance;
        // fanning out the same cues across every span keeps the corroboration
        // gate honest while sharing the extraction cost.
        let metadataCues: [EpisodeMetadataCue]
        if let feedMetadata = await episodeMetadataProvider.metadata(for: analysisAssetId) {
            let extractor = MetadataCueExtractor()
            metadataCues = extractor.extractCues(
                description: feedMetadata.feedDescription,
                summary: feedMetadata.feedSummary
            )
        } else {
            metadataCues = []
        }
        let metadataEvidenceBuilder = FeedDescriptionEvidenceBuilder()
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

            // playhead-z3ch: build per-span metadata entries from the cached cues.
            // Builder is pure; the heavy work (cue extraction) was done once above.
            let metadataEntries = metadataEvidenceBuilder.buildEntries(
                cues: metadataCues,
                for: refinedSpan
            )

            let ledger = buildEvidenceLedger(
                span: refinedSpan,
                classifierResults: classifierResults,
                lexicalCandidates: lexicalCandidates,
                featureWindows: featureWindows,
                catalogEntries: evidenceCatalog.entries,
                semanticScanResults: semanticScanResults,
                metadataEntries: metadataEntries,
                fusionConfig: fusionConfig
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
                // playhead-p2iv: Consume the typical-ad-duration prior as a soft
                // monotonic multiplier. Until the full 4-level PriorHierarchyResolver
                // is wired into this actor, use the resolved-global default (30...90s).
                // When resolver plumbing lands, pass `DurationPrior(resolvedPriors:)`.
                durationPrior: .standard
            )
            let rawDecision = mapper.map()

            // If FM suppression capped to markOnly, override the gate.
            let decision: DecisionResult
            if suppressionResult.cappedToMarkOnly {
                decision = DecisionResult(
                    proposalConfidence: rawDecision.proposalConfidence,
                    skipConfidence: rawDecision.skipConfidence,
                    eligibilityGate: .cappedByFMSuppression
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
        metadataEntries: [EvidenceLedgerEntry] = [],
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

        // 2026-04-23 Finding 4: music-bed coverage produces its own
        // `.musicBed` ledger entry (distinct EvidenceSourceType) so
        // the quorum gate's `distinctKinds.count` increments when a
        // span has both an RMS-drop edge and an interior bed.
        let musicBedEntries = buildMusicBedLedgerEntries(
            span: span,
            featureWindows: featureWindows,
            fusionConfig: fusionConfig
        )
        // musicBed entries are merged into the acousticEntries list
        // passed to BackfillEvidenceFusion. The fusion code already
        // iterates over acousticEntries and preserves each entry's
        // `source`, so a `.musicBed`-sourced entry flows through with
        // the correct kind and increments distinctKinds.count.
        let combinedAcousticEntries = acousticEntries + musicBedEntries

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
            acousticEntries: combinedAcousticEntries,
            catalogEntries: catalogLedgerEntries,
            metadataEntries: metadataEntries,
            mode: config.fmBackfillMode,
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

    /// 2026-04-23 Finding 4: build `.musicBed`-source ledger entries
    /// from the span's interior `MusicBedLevel` coverage.
    ///
    /// Delegates the threshold/weight logic to the pure
    /// `MusicBedLedgerEvaluator`; this method is just the span-window
    /// filter + plumbing.
    private func buildMusicBedLedgerEntries(
        span: DecodedSpan,
        featureWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        let spanWindows = featureWindows.filter { fw in
            fw.startTime < span.endTime && fw.endTime > span.startTime
        }
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: spanWindows,
            fusionConfig: fusionConfig
        )
        if let entry = result.entry {
            return [entry]
        }
        return []
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

        let hypothesisCandidates = try await hypothesisCandidates(
            from: orderedChunks,
            analysisAssetId: analysisAssetId
        )
        let lexicalCandidates = scanner.scan(
            chunks: orderedChunks,
            analysisAssetId: analysisAssetId
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
                eligibilityGate: existing.eligibilityGate
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
                    window.endTime > replayEnvelope.lowerBound
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
            // builder, whose isAdUnderDefault heuristic matches on
            // "autoskip"/"markonly"/"skip" in the logged action.
            //
            // Regression: 2026-04-23 dogfood capture asset
            // 71F0C2AE-7260-4D1E-B41A-BCFD5103A641 @ [7006..7008],
            // classifier 0.8154, surfaced as "hotPathCandidate" → invisible
            // to the harness → GT=3, Pred=0, Sec-F1=0.
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
            let logEntry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: timestamp,
                windowBounds: .init(start: result.startTime, end: result.endTime),
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
        guard !promotedSegments.isEmpty else { return [] }

        // Observability: emit one decision-log entry per promoted segment
        // with a distinguishing action string so replay tooling can
        // distinguish aggregator promotions from single-window promotions
        // (which carry "hotPathCandidate" / "autoSkipEligible").
        let fusionConfig = FusionWeightConfig()
        let classifierCap = fusionConfig.classifierCap
        let activationSnapshot = DecisionLogEntry.ActivationConfigSnapshot(
            MetadataActivationConfig.resolved()
        )
        for segment in promotedSegments {
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
        let surviving = promotedSegments.filter { segment in
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
        logger.info("Hot path: aggregator produced \(newWindows.count) windows (of \(promotedSegments.count) promoted segments, \(promotedSegments.count - surviving.count) deduped against single-window path)")
        return newWindows
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
        let boost: Double
        if let correctionStore {
            boost = await correctionStore.correctionBoostFactor(for: analysisAssetId)
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

        let input = AutoSkipPrecisionGateInput(
            segmentStartTime: startTime,
            segmentEndTime: endTime,
            segmentScore: segmentScore,
            episodeDuration: episodeDuration,
            overlappingFeatureWindows: overlappingFeatureWindows,
            lexicalCategories: lexicalCategories,
            userCorrectionBoostFactor: boost
        )

        switch AutoSkipPrecisionGate.classify(input: input, config: gateConfig) {
        case .detectionOnly:
            return nil
        case .uiCandidate:
            return "markOnly"
        case .autoSkipEligible:
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
