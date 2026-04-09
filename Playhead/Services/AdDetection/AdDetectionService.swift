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
    /// Phase 3 Foundation Model backfill toggle. Defaults to `.shadow`:
    /// FM runs and persists results, but never influences skip cues. See
    /// `FMBackfillMode` for the full contract.
    let fmBackfillMode: FMBackfillMode

    init(
        candidateThreshold: Double,
        confirmationThreshold: Double,
        suppressionThreshold: Double,
        hotPathLookahead: TimeInterval,
        detectorVersion: String,
        fmBackfillMode: FMBackfillMode = .shadow
    ) {
        self.candidateThreshold = candidateThreshold
        self.confirmationThreshold = confirmationThreshold
        self.suppressionThreshold = suppressionThreshold
        self.hotPathLookahead = hotPathLookahead
        self.detectorVersion = detectorVersion
        self.fmBackfillMode = fmBackfillMode
    }

    static let `default` = AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: "detection-v1",
        fmBackfillMode: .shadow
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
    /// phase. When `nil`, FM is skipped entirely (equivalent to .disabled).
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

    // MARK: - Cached State

    /// Scanner is recreated per-episode when profile changes.
    private var scanner: LexicalScanner
    /// Per-show priors parsed from the current PodcastProfile.
    private var showPriors: ShowPriors
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
        regionShadowObserver: RegionShadowObserver? = nil
    ) {
        self.store = store
        self.classifier = classifier
        self.metadataExtractor = metadataExtractor
        self.config = config
        self.scanner = LexicalScanner(podcastProfile: podcastProfile)
        self.showPriors = ShowPriors.from(profile: podcastProfile)
        self.backfillJobRunnerFactory = backfillJobRunnerFactory
        self.canUseFoundationModelsProvider = canUseFoundationModelsProvider
        self.shadowSkipMarker = shadowSkipMarker
        self.regionShadowObserver = regionShadowObserver
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

    // MARK: - Profile Update

    /// Update the scanner and priors when the podcast profile changes.
    func updateProfile(_ profile: PodcastProfile?) {
        scanner = LexicalScanner(podcastProfile: profile)
        showPriors = ShowPriors.from(profile: profile)
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

    /// Run the backfill pipeline: re-classify with final-pass transcript,
    /// extract metadata, update priors, promote/suppress candidates.
    ///
    /// Flow:
    ///   1. Re-run lexical scan on final-pass transcript chunks
    ///   2. Re-classify with full context
    ///   3. Promote high-confidence to .confirmed, suppress low-confidence
    ///   4. Run Layer 3 (metadata extraction) on confirmed windows
    ///   5. Update PodcastProfile priors
    ///
    /// - Parameters:
    ///   - chunks: Final-pass TranscriptChunks (full episode).
    ///   - analysisAssetId: The analysis asset being processed.
    ///   - podcastId: Podcast ID for profile prior updates.
    ///   - episodeDuration: Total episode duration in seconds.
    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String? = nil
    ) async throws {
        self.episodeDuration = episodeDuration
        guard !chunks.isEmpty else { return }

        // 1. Re-run lexical scan on final transcript.
        let lexicalCandidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: analysisAssetId
        )

        logger.info("Backfill: \(lexicalCandidates.count) lexical candidates from \(chunks.count) final chunks")

        // 2. Re-classify with full context.
        let classifierResults: [ClassifierResult]
        if !lexicalCandidates.isEmpty {
            classifierResults = try await classifyCandidates(
                lexicalCandidates,
                analysisAssetId: analysisAssetId
            )
        } else {
            classifierResults = []
        }

        // 3. Load existing candidate AdWindows for this asset.
        let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
        let existingCandidates = existingWindows.filter {
            $0.decisionState == AdDecisionState.candidate.rawValue
        }

        // 4. Promote or suppress each existing candidate based on backfill results.
        var confirmedWindowIds: [String] = []
        for existing in existingCandidates {
            try Task.checkCancellation()

            let newDecision = resolveDecision(
                existing: existing,
                backfillResults: classifierResults
            )

            if newDecision != existing.decisionState {
                try await store.updateAdWindowDecision(
                    id: existing.id,
                    decisionState: newDecision
                )
            }

            if newDecision == AdDecisionState.confirmed.rawValue {
                confirmedWindowIds.append(existing.id)
            }
        }

        // 5. Insert any new backfill-only detections above confirmation threshold.
        let newBackfillWindows = buildNewBackfillWindows(
            classifierResults: classifierResults,
            existingWindows: existingWindows,
            analysisAssetId: analysisAssetId,
            lexicalCandidates: lexicalCandidates
        )
        if !newBackfillWindows.isEmpty {
            try await store.insertAdWindows(newBackfillWindows)
            confirmedWindowIds.append(contentsOf: newBackfillWindows.map(\.id))
            logger.info("Backfill: inserted \(newBackfillWindows.count) new confirmed windows")
        }

        // 6. Run Layer 3 metadata extraction on confirmed windows.
        let allWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
        let confirmedWindows = allWindows.filter {
            $0.decisionState == AdDecisionState.confirmed.rawValue
        }

        for window in confirmedWindows {
            try Task.checkCancellation()
            await extractAndPersistMetadata(
                window: window,
                chunks: chunks
            )
        }

        // 7. Update PodcastProfile priors from confirmed results.
        if podcastId.isEmpty {
            logger.info("Skipping priors update: missing podcastId for asset \(analysisAssetId)")
        } else {
            try await updatePriors(
                podcastId: podcastId,
                confirmedWindows: confirmedWindows,
                episodeDuration: episodeDuration
            )
        }

        // 8. Update coverage watermark.
        if let maxEnd = confirmedWindows.map(\.endTime).max() {
            try await store.updateConfirmedAdCoverage(
                id: analysisAssetId,
                endTime: maxEnd
            )
        }

        logger.info("Backfill complete: \(confirmedWindows.count) confirmed, \(existingCandidates.count - confirmedWindowIds.count) suppressed")

        // 9. Phase 3 shadow phase. Runs the FM classifier purely for telemetry;
        // its output never feeds back into AdWindow rows in this phase. The
        // shadow invariant test in PlayheadTests pins this property.
        if config.fmBackfillMode != .disabled {
            if podcastId.isEmpty {
                logger.info("Skipping shadow FM phase: missing podcastId for asset \(analysisAssetId)")
            } else {
                _ = await runShadowFMPhase(
                    chunks: chunks,
                    analysisAssetId: analysisAssetId,
                    podcastId: podcastId,
                    sessionIdOverride: sessionId
                )
            }
        }

        // 10. playhead-xba (Phase 4 shadow wire-up): run the region proposal +
        // feature extraction pipeline purely for observation. Gated on a
        // non-nil `regionShadowObserver`, which production release builds
        // never construct — this matches the DEBUG-only injection pattern
        // used for `FoundationModelsFeedbackStore`. Nothing here can affect
        // AdWindow rows, skip cues, or metadata; the only side effect is a
        // write into an in-memory actor. Failures are logged and swallowed.
        if let observer = regionShadowObserver {
            await runRegionShadowPhase(
                observer: observer,
                chunks: chunks,
                analysisAssetId: analysisAssetId,
                episodeDuration: episodeDuration,
                lexicalCandidates: lexicalCandidates
            )
        }
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
        lexicalCandidates: [LexicalCandidate]
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
            podcastProfile: nil
        )
        let bundles = RegionShadowPhase.run(input)
        await observer.record(assetId: analysisAssetId, bundles: bundles)
    }

    // MARK: - Shadow FM Phase

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
    ) async -> ShadowFMPhaseOutcome {
        guard config.fmBackfillMode != .disabled else { return .skipped }

        guard let factory = backfillJobRunnerFactory else {
            logger.debug("Shadow FM phase skipped: no runner factory injected")
            return .skipped
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
            return .requeued
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
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: version.transcriptVersion,
            plannerContext: plannerContext
        )

        do {
            let result = try await runner.runPendingBackfill(for: inputs)
            logger.info("Shadow FM phase: admitted=\(result.admittedJobIds.count) scans=\(result.scanResultIds.count) deferred=\(result.deferredJobIds.count)")
            if result.deferredJobIds.isEmpty {
                return .ranSucceeded
            }
            return .ranNeedsRetry
        } catch {
            logger.warning("Shadow FM phase failed (suppressed by invariant): \(error.localizedDescription)")
            return .ranFailed
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
        let outcome = await runShadowFMPhase(
            chunks: finalChunks,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            sessionIdOverride: sessionId
        )
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

    // MARK: - Decision Resolution

    /// Determine whether to confirm or suppress an existing candidate
    /// based on backfill classifier results.
    private func resolveDecision(
        existing: AdWindow,
        backfillResults: [ClassifierResult]
    ) -> String {
        // Find the backfill result that best overlaps this window.
        let bestMatch = backfillResults.first { result in
            let overlapStart = max(existing.startTime, result.startTime)
            let overlapEnd = min(existing.endTime, result.endTime)
            return overlapEnd - overlapStart > 0
        }

        guard let match = bestMatch else {
            // No backfill result overlaps -- suppress if confidence was borderline.
            if existing.confidence < config.confirmationThreshold {
                return AdDecisionState.suppressed.rawValue
            }
            return existing.decisionState
        }

        if match.adProbability >= config.confirmationThreshold {
            return AdDecisionState.confirmed.rawValue
        } else if match.adProbability < config.suppressionThreshold {
            return AdDecisionState.suppressed.rawValue
        }

        // Between suppression and confirmation: keep as candidate.
        return existing.decisionState
    }

    // MARK: - New Backfill Windows

    /// Build AdWindows for backfill-only detections that don't overlap
    /// any existing window.
    private func buildNewBackfillWindows(
        classifierResults: [ClassifierResult],
        existingWindows: [AdWindow],
        analysisAssetId: String,
        lexicalCandidates: [LexicalCandidate]
    ) -> [AdWindow] {
        classifierResults
            .filter { result in
                result.adProbability >= config.confirmationThreshold
                    && !overlapsExisting(result: result, existing: existingWindows)
            }
            .map { result in
                buildAdWindow(
                    from: result,
                    boundaryState: .acousticRefined,
                    decisionState: .confirmed,
                    evidenceText: lexicalCandidates
                        .first { $0.id == result.candidateId }?.evidenceText
                )
            }
    }

    /// Check whether a classifier result overlaps any existing AdWindow.
    private func overlapsExisting(
        result: ClassifierResult,
        existing: [AdWindow]
    ) -> Bool {
        existing.contains { window in
            let overlapStart = max(window.startTime, result.startTime)
            let overlapEnd = min(window.endTime, result.endTime)
            return overlapEnd - overlapStart > 0
        }
    }

    // MARK: - AdWindow Construction

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
        confirmedWindows: [AdWindow],
        episodeDuration: Double
    ) async throws {
        guard !confirmedWindows.isEmpty, episodeDuration > 0 else { return }

        let existingProfile = try await store.fetchProfile(podcastId: podcastId)

        // Compute normalized ad slot positions from confirmed windows.
        let newSlotPositions = confirmedWindows.map { window in
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
        let newSponsors = confirmedWindows
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
