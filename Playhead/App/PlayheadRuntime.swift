// PlayheadRuntime.swift
// Shared app-level composition root for long-lived services.

@preconcurrency import AVFoundation
import Foundation
import os
import OSLog
import SwiftData
import UIKit

enum PlaybackPositionPersistenceTrigger: String, Sendable {
    case periodic
    case paused
    case seek
    case stopped
    case background
}

@MainActor
@Observable
final class PlayheadRuntime {

    let playbackService: PlaybackService
    let capabilitiesService: CapabilitiesService
    let analysisStore: AnalysisStore
    let modelInventory: ModelInventory
    let assetProvider: AssetProvider
    let entitlementManager: EntitlementManager
    let audioService: AnalysisAudioService
    let featureService: FeatureExtractionService
    let transcriptEngine: TranscriptEngineService
    let trustService: TrustScoringService
    let adDetectionService: AdDetectionService
    let skipOrchestrator: SkipOrchestrator
    let analysisCoordinator: AnalysisCoordinator
    let backgroundProcessingService: BackgroundProcessingService
    let downloadManager: DownloadManager
    let analysisJobRunner: AnalysisJobRunner
    let analysisWorkScheduler: AnalysisWorkScheduler
    let analysisJobReconciler: AnalysisJobReconciler
    /// playhead-01t8: runtime-owned coordinator that lets the scheduler
    /// flip a preemption flag on active lower-lane jobs when admitting
    /// a Now-lane job. Installed on `analysisWorkScheduler` via
    /// `setLanePreemptionHandler(_:)` and threaded into
    /// `analysisJobRunner` so the runner can register each job and
    /// receive a `PreemptionSignal` for the downstream services to
    /// poll at their safe points.
    let lanePreemptionCoordinator: LanePreemptionCoordinator

    /// playhead-o45p: production consumer of `EpisodeSurfaceStatusReducer`.
    /// Wired so `ready_entered` events fire on real episode lifecycle
    /// edges (cold-start play of an already-complete episode, and
    /// analysis-completion transitions from the coordinator). The
    /// observer is the SINGLE production call-site of the reducer +
    /// transition emitter — without it the Wave 4 false_ready_rate
    /// metric's denominator collapses to zero.
    let surfaceStatusObserver: EpisodeSurfaceStatusObserver

    /// playhead-o45p: Shared `SurfaceStatusInvariantLogger` instance that
    /// underwrites every tier-B audit event in the surface-status JSONL
    /// stream. Produced events come from two sides of the
    /// false_ready_rate pair: `auto_skip_fired` (from SkipOrchestrator)
    /// and `ready_entered` (from EpisodeSurfaceStatusObserver). Sharing
    /// the instance keeps both sides on the same session file with the
    /// same installId — a precondition for byte-identical
    /// `episode_id_hash` values across the pair.
    let surfaceStatusLogger: SurfaceStatusInvariantLogger

    /// Phase 7.2: Shared user correction store. Wired to `PersistentUserCorrectionStore`
    /// in production; views (TranscriptPeekView, AdBannerView callback) consume this
    /// to persist vetoes and listen-reverts without knowing the concrete type.
    let correctionStore: any UserCorrectionStore

    /// bd-fmfb: DEBUG-only sink for `LanguageModelSession.logFeedbackAttachment`
    /// payloads. Apple's iOS 26.4 on-device safety classifier rejects benign
    /// podcast advertising; we capture machine-readable feedback so the
    /// FoundationModels team can fix it. Release builds intentionally leave
    /// this `nil` — production users should not have feedback attachments
    /// piling up in their app sandbox, and `FoundationModelClassifier` skips
    /// the entire capture path when this is `nil`.
    #if DEBUG
    let feedbackStore: FoundationModelsFeedbackStore?
    #else
    let feedbackStore: FoundationModelsFeedbackStore? = nil
    #endif

    /// playhead-xba (Phase 4 shadow wire-up): DEBUG-only observation sink
    /// for `RegionProposalBuilder` + `RegionFeatureExtractor` output. The
    /// `AdDetectionService` backfill path writes into this observer only
    /// when it is non-nil, so release builds skip the Phase 4 shadow phase
    /// entirely. This mirrors the `feedbackStore` injection pattern and
    /// keeps production users free of any Phase 4 runtime footprint until
    /// the pipeline graduates out of shadow mode.
    #if DEBUG
    let regionShadowObserver: RegionShadowObserver?
    #else
    let regionShadowObserver: RegionShadowObserver? = nil
    #endif

    /// Phase 5 DEBUG-only observation sink for `AtomEvidenceProjector` output.
    /// The `AdDetectionService` backfill path writes into this observer only
    /// when it is non-nil, so release builds skip the Phase 5 projector phase
    /// entirely. This mirrors the `regionShadowObserver` injection pattern.
    #if DEBUG
    let phase5ProjectorObserver: Phase5ProjectorObserver?
    #else
    let phase5ProjectorObserver: Phase5ProjectorObserver? = nil
    #endif

    private let isPreviewRuntime: Bool
    private let logger = Logger(subsystem: "com.playhead", category: "Runtime")
    @ObservationIgnored
    private var playbackPositionPersistenceHandler: (@MainActor (PlaybackPositionPersistenceTrigger) async -> Void)?

    private(set) var currentEpisodeId: String?
    private(set) var currentPodcastId: String?
    private(set) var currentAnalysisAssetId: String?
    private(set) var currentEpisodeTitle: String?
    private(set) var currentPodcastTitle: String?
    private(set) var currentArtworkURL: URL?

    /// Background download task for the current episode's audio cache.
    private var audioCacheTask: Task<Void, Never>?
    /// Retains the progressive resource loader outside PlaybackServiceActor.
    private var activeProgressiveLoader: ProgressiveResourceLoader?
    /// bd-3bz (Phase 4): observer that watches `canUseFoundationModels` for
    /// false→true transitions and, after a 60s-stable-true debounce, drains
    /// any `analysis_sessions` rows flagged with `needsShadowRetry = 1`.
    /// Held strongly by the runtime so its observer loop survives until the
    /// runtime is torn down. Stopped explicitly via `shutdown()`; tests that
    /// spin up transient runtimes should call that to avoid leaking
    /// AsyncStream subscriptions across tests. `deinit` also makes a
    /// best-effort stop so an abandoned runtime does not keep the observer
    /// alive forever.
    @ObservationIgnored
    private let shadowRetryObserver: ShadowRetryObserver?
    @ObservationIgnored
    private var shadowRetryObserverStartupTask: Task<Void, Never>?
    private var isShutdown = false

    /// True when an episode is actively loaded for playback.
    var isPlayingEpisode: Bool {
        currentEpisodeId != nil
    }

    /// Error state flag for catastrophic initialization failures.
    var initializationError: String?

    init(isPreviewRuntime: Bool = false) {
        self.isPreviewRuntime = isPreviewRuntime
        self.playbackService = PlaybackService()
        self.capabilitiesService = CapabilitiesService()

        // AnalysisStore: attempt creation with graceful recovery.
        // 1. Normal open  2. Delete + retry  3. Temp directory (ephemeral)
        var resolvedStore: AnalysisStore
        var storeError: String?
        if let store = try? AnalysisStore() {
            resolvedStore = store
        } else {
            // Delete corrupted store and retry
            try? FileManager.default.removeItem(at: AnalysisStore.defaultDirectory())
            if let store = try? AnalysisStore() {
                resolvedStore = store
            } else {
                // Fallback to temp directory — analysis won't persist across launches
                let tmpDir = FileManager.default.temporaryDirectory
                    .appendingPathComponent("PlayheadAnalysis-\(ProcessInfo.processInfo.globallyUniqueString)", isDirectory: true)
                // If even temp directory fails, the device is in severe trouble.
                // Use force-try as last resort — the app cannot function without any store.
                resolvedStore = try! AnalysisStore(directory: tmpDir)
                storeError = "Analysis database could not be opened. Ad detection may not persist across launches."
            }
        }
        self.analysisStore = resolvedStore

        let manifest: ModelManifest
        do {
            manifest = try ModelInventory.loadBundledManifest()
        } catch {
            manifest = ModelManifest(version: 1, generatedAt: .now, models: [])
        }
        self.modelInventory = ModelInventory(manifest: manifest)
        self.assetProvider = AssetProvider(inventory: modelInventory)
        self.entitlementManager = EntitlementManager()

        self.audioService = AnalysisAudioService()

        let speechService = SpeechService(
            vocabularyProvider: ASRVocabularyProvider(store: analysisStore)
        )

        self.featureService = FeatureExtractionService(store: analysisStore)
        self.transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: analysisStore
        )
        self.trustService = TrustScoringService(store: analysisStore)

        // Phase 3 shadow-mode wiring. The factory receives the store and the
        // config-selected FMBackfillMode and returns a fully-wired runner that
        // uses:
        //   • FoundationModelClassifier's live on-device runtime (default init)
        //   • CapabilitiesService.currentSnapshot for thermal/charging state
        //   • UIDeviceBatteryProvider for battery level (same source as BPS)
        //   • ScanCohort.productionJSON() as the reuse-cache key
        // When AdDetectionConfig.fmBackfillMode == .off, the factory is
        // never invoked. When .full (default), FM runs, writes telemetry,
        // and contributes to the decision ledger.
        let capabilitiesServiceForFactory = capabilitiesService
        let batteryProvider = UIDeviceBatteryProvider()

        // bd-fmfb: instantiate the feedback store ONLY in DEBUG. Release
        // builds get a nil store and `FoundationModelClassifier` short-
        // circuits the capture path so production users never accumulate
        // attachment files in the sandbox.
        #if DEBUG
        let feedbackStore = FoundationModelsFeedbackStore()
        self.feedbackStore = feedbackStore
        #else
        let feedbackStore: FoundationModelsFeedbackStore? = nil
        #endif

        // playhead-xba: DEBUG-only Phase 4 shadow observer. Release builds
        // leave the stored property nil (declared above) so the backfill
        // skips the entire region pipeline.
        #if DEBUG
        let regionShadowObserver = RegionShadowObserver()
        self.regionShadowObserver = regionShadowObserver
        #else
        let regionShadowObserver: RegionShadowObserver? = nil
        #endif

        // Phase 5 DEBUG-only projector observer. Release builds leave the
        // stored property nil so the backfill skips the Phase 5 atom
        // evidence projector entirely. Mirrors RegionShadowObserver pattern.
        #if DEBUG
        let phase5ProjectorObserver = Phase5ProjectorObserver()
        self.phase5ProjectorObserver = phase5ProjectorObserver
        #else
        let phase5ProjectorObserver: Phase5ProjectorObserver? = nil
        #endif

        // HIGH-1: round-2's M-B hoist shared one AdmissionController across
        // every factory invocation. AdDetectionService is actor-reentrant on
        // `await`, so two concurrent `runBackfill` calls would contend on the
        // same controller: the second call saw `runningJob != nil` and
        // mass-deferred its entire batch with `serialBusy`. Per-call
        // controllers correctly give each `runBackfill` invocation its own
        // queue. The original M-B concern (parallel FM inference) is not
        // solved by sharing the controller — it would require a separate FM
        // session manager, which is out of scope here. The concurrent
        // no-mass-defer invariant is pinned by
        // `concurrentRunBackfillsDoNotMassDeferEachOther` in
        // BackfillJobRunnerTests.
        // bd-1en Phase 1 + Cycle 2 C1/H9: build the sensitive-window
        // router AND the production redactor up front so:
        //
        //   - The router and the redactor share the same compiled
        //     RedactionRules.json (single source of truth for trigger
        //     vocabulary).
        //   - FoundationModelClassifier is constructed with the redactor
        //     non-noop in production. The standard `@Generable` coarse
        //     and refinement prompts are redacted before submission;
        //     router-to-permissive bypasses are NOT also redacted (no
        //     double-mitigation — the permissive guardrails are the
        //     only relaxation needed once the router fires).
        //   - Missing/malformed/invalid-pattern manifests fail loud at
        //     startup with a precondition message that names the cause
        //     instead of silently degrading to a noop redactor.
        //
        // The permissive classifier is constructed only on iOS 26+ since
        // `SystemLanguageModel(guardrails:)` is gated.
        let bd1enRedactor: PromptRedactor
        do {
            bd1enRedactor = try PromptRedactor.loadDefault()
        } catch let failure as PromptRedactor.LoadFailure {
            preconditionFailure("PromptRedactor.loadDefault failed: \(failure)")
        } catch {
            preconditionFailure("PromptRedactor.loadDefault failed with unknown error: \(error)")
        }
        let bd1enRouter = SensitiveWindowRouter(redactor: bd1enRedactor)
        let bd1enPermissiveBox: BackfillJobRunner.PermissiveClassifierBox? = {
            if #available(iOS 26.0, *) {
                return BackfillJobRunner.PermissiveClassifierBox(PermissiveAdClassifier())
            }
            return nil
        }()

        let backfillJobRunnerFactory: @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner = {
            [capabilitiesServiceForFactory, batteryProvider, feedbackStore, bd1enRouter, bd1enPermissiveBox, bd1enRedactor] store, mode in
            BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                // Cycle 2 C1: the standard @Generable path gets the
                // production redactor. The permissive bypass branch
                // inside dispatch does NOT call into this redactor —
                // it sends the original window text to the permissive
                // model unmasked.
                // Cycle 6 M-5: route through the shared factory so the
                // regression rail in Cycle4RedactorWiringTests pins the
                // wiring — any future change that passes `.noop` here
                // fails an automated test.
                classifier: PlayheadRuntime.makeFoundationModelClassifier(
                    redactor: bd1enRedactor,
                    feedbackStore: feedbackStore
                ),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { await capabilitiesServiceForFactory.currentSnapshot },
                batteryLevelProvider: { await batteryProvider.currentBatteryState().level },
                // H-A: compute the scan cohort JSON per factory invocation
                // rather than freezing it at runtime init. Locale changes
                // mid-process were previously invisible to the cohort hash,
                // leaving reuse-cache lookups keyed off stale values. The
                // per-call cost is microseconds (a handful of string fields
                // JSON-encoded with sorted keys) and buys cohort freshness.
                scanCohortJSON: ScanCohort.productionJSON(),
                sensitiveRouter: bd1enRouter,
                permissiveClassifier: bd1enPermissiveBox
            )
        }
        // bd-3bz (Phase 4) / H7 (cycle 2): when the shadow phase bails on
        // FM unavailability, mark the explicit session id supplied by the
        // shadow phase. The marker no longer does an asset→session lookup
        // (see H7 in AdDetectionService.runShadowFMPhase): the session id
        // is captured at the START of the shadow phase to fix a race
        // window where concurrent reprocessing on the same asset could
        // create a newer session and the marker would tag the wrong row.
        //
        // H2 (cycle 2): after marking, immediately wake the shadow retry
        // observer. Without this, a session flagged while capability is
        // already stable-true would never re-drain — the observer's
        // capability stream only fires on transitions, not on flags.
        // The observer is constructed below (after AdDetectionService,
        // since the observer depends on the service as its drainer), so
        // we capture a Sendable holder here and populate it once the
        // observer exists. The closure reads through the holder lazily.
        let analysisStoreForMarker = analysisStore
        let observerHolder = ShadowRetryObserverHolder()
        let shadowSkipMarker: @Sendable (String, String) async -> Void = { sessionId, podcastId in
            do {
                try await analysisStoreForMarker.markSessionNeedsShadowRetry(
                    id: sessionId,
                    podcastId: podcastId
                )
                if let observer = observerHolder.observer {
                    await observer.wake()
                }
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .warning("bd-3bz: failed to mark session \(sessionId) for shadow retry: \(error.localizedDescription)")
            }
        }

        // Phase 7.2: construct the correction store before the skip orchestrator
        // and ad detection service so it can be injected into both.
        // PersistentUserCorrectionStore only needs the AnalysisStore (already initialized).
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        self.correctionStore = correctionStore

        // playhead-o45p: construct ONE shared SurfaceStatusInvariantLogger
        // instance and thread it through both the SkipOrchestrator (which
        // emits `auto_skip_fired`) and the EpisodeSurfaceStatusObserver
        // (which emits `ready_entered`). Sharing the instance ensures both
        // sides of the false_ready_rate pair land on the same session
        // file with the same installId, so their episode_id_hash values
        // are byte-identical.
        let surfaceStatusLogger = SurfaceStatusInvariantLogger()
        self.surfaceStatusLogger = surfaceStatusLogger
        let surfaceStatusHasher: @Sendable (String) -> String = { [surfaceStatusLogger] episodeId in
            surfaceStatusLogger.hashEpisodeId(episodeId)
        }

        // Phase 6.5 (playhead-4my.16): skipOrchestrator is constructed before
        // adDetectionService so it can be injected for step-17 forwarding.
        // The orchestrator is otherwise wired identically to before this change.
        self.skipOrchestrator = SkipOrchestrator(
            store: analysisStore,
            trustService: trustService,
            correctionStore: correctionStore,
            invariantLogger: surfaceStatusLogger,
            episodeIdHasher: surfaceStatusHasher
        )
        self.adDetectionService = AdDetectionService(
            store: analysisStore,
            metadataExtractor: FallbackExtractor(),
            backfillJobRunnerFactory: backfillJobRunnerFactory,
            // M-D: capabilities provider lets the shadow phase short-circuit
            // before building atom/segment/catalog inputs on devices that
            // cannot run Foundation Models. The closure hits the actor on
            // every invocation so the result stays current with runtime
            // capability changes (FM became unavailable, AI disabled, etc.).
            canUseFoundationModelsProvider: { [capabilitiesServiceForFactory] in
                await capabilitiesServiceForFactory.currentSnapshot.canUseFoundationModels
            },
            shadowSkipMarker: shadowSkipMarker,
            // playhead-xba (Phase 4 shadow wire-up): hand the DEBUG-only
            // region observer to the service. In release builds this is
            // `nil`, which makes the Phase 4 shadow phase a no-op.
            regionShadowObserver: regionShadowObserver,
            // Phase 5 projector wire-up: hand the DEBUG-only projector
            // observer to the service. In release builds this is `nil`,
            // which makes the Phase 5 atom evidence projector a no-op.
            phase5ProjectorObserver: phase5ProjectorObserver,
            // Phase 6.5 (playhead-4my.16): forward eligible fusion decisions
            // to the orchestrator after each backfill run (step 17).
            skipOrchestrator: skipOrchestrator
        )
        self.downloadManager = DownloadManager()

        // playhead-o45p: construct the surface-status observer before
        // the coordinator so it can be injected via the coordinator's
        // optional DI slot. The observer reads capability snapshots
        // through a closure that dispatches onto `capabilitiesService`
        // at call time, so the snapshot is always current with runtime
        // state (thermal/power/AI-toggle changes).
        let capabilitiesServiceForObserver = capabilitiesService
        self.surfaceStatusObserver = EpisodeSurfaceStatusObserver(
            store: analysisStore,
            capabilitySnapshotProvider: { [capabilitiesServiceForObserver] in
                await capabilitiesServiceForObserver.currentSnapshot
            },
            invariantLogger: surfaceStatusLogger,
            episodeIdHasher: surfaceStatusHasher
        )

        self.analysisCoordinator = AnalysisCoordinator(
            store: analysisStore,
            audioService: audioService,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            capabilitiesService: capabilitiesService,
            adDetectionService: adDetectionService,
            skipOrchestrator: skipOrchestrator,
            downloadManager: downloadManager,
            surfaceStatusObserver: surfaceStatusObserver
        )
        self.backgroundProcessingService = BackgroundProcessingService(
            coordinator: analysisCoordinator,
            capabilitiesService: capabilitiesService
        )

        let cueMaterializer = SkipCueMaterializer(store: analysisStore)
        let lanePreemptionCoordinator = LanePreemptionCoordinator()
        self.lanePreemptionCoordinator = lanePreemptionCoordinator
        self.analysisJobRunner = AnalysisJobRunner(
            store: analysisStore,
            audioProvider: audioService,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adDetectionService,
            cueMaterializer: cueMaterializer,
            preemptionCoordinator: lanePreemptionCoordinator
        )
        // playhead-xiz6: wire a real `CandidateWindowCascade` into the
        // scheduler so the c3pi entry points (`seedCandidateWindows`,
        // `noteCommittedPlayhead`, `currentCandidateWindows`) are
        // reachable in production. Without this, the previous bead's
        // foundation is unused: the scheduler's optional cascade
        // defaults to `nil` and every entry point silently no-ops,
        // which would block the seek-event wiring (`playhead-vhha`)
        // and runner consumption (`playhead-swws`) from doing anything
        // observable on a real device. Constructed with the cascade's
        // own defaults — both `PreAnalysisConfig.load()` and the
        // canonical `Logger` subsystem/category live on the cascade.
        self.analysisWorkScheduler = AnalysisWorkScheduler(
            store: analysisStore,
            jobRunner: analysisJobRunner,
            capabilitiesService: capabilitiesService,
            downloadManager: downloadManager,
            batteryProvider: batteryProvider,
            candidateWindowCascade: CandidateWindowCascade()
        )
        self.analysisJobReconciler = AnalysisJobReconciler(
            store: analysisStore,
            downloadManager: downloadManager,
            capabilitiesService: capabilitiesService
        )

        // bd-3bz (Phase 4): construct the shadow-retry observer once all
        // dependencies (capabilitiesService, analysisStore, adDetectionService)
        // are initialized. The observer is skipped in preview runtimes — the
        // SwiftUI preview canvas spins up many transient runtimes and we don't
        // want stray AsyncStream subscriptions piling up there. The observer
        // task is started below after `super`-style initialization completes.
        if !isPreviewRuntime {
            let observer = ShadowRetryObserver(
                capabilities: capabilitiesService,
                store: analysisStore,
                drainer: adDetectionService
            )
            self.shadowRetryObserver = observer
            // H2: publish the observer through the holder captured by the
            // shadow-skip marker closure so subsequent marker invocations
            // can wake() the observer.
            observerHolder.observer = observer
        } else {
            self.shadowRetryObserver = nil
        }

        // Set error state after all stored properties are initialized.
        if let storeError {
            self.initializationError = storeError
        }

        // H-B: enable battery monitoring exactly once at runtime init rather
        // than paying a MainActor hop every time `UIDeviceBatteryProvider`
        // reads the level. The provider's per-call setter becomes a harmless
        // no-op and the warning about "every refinement window pays a hop
        // for the setter" is mitigated. The actual battery read still
        // requires a MainActor hop but that's unavoidable on UIKit.
        Task { @MainActor in
            UIDevice.current.isBatteryMonitoringEnabled = true
        }

        Task { await capabilitiesService.startObserving() }

        // Phase 7.2: inject the correction store into adDetectionService.
        // SkipOrchestrator receives it at init (see above); AdDetectionService
        // is an actor so the mutable property is set via an async Task.
        // Race note: this Task may execute after the first backfill run starts.
        // Until it completes, adDetectionService.correctionStore is nil, so
        // correctionPassthroughFactor defaults to 1.0 (no suppression) — the safe
        // default, since it means "no corrections loaded yet, don't suppress anything."
        Task { [adDetectionService, correctionStore] in
            await adDetectionService.setUserCorrectionStore(correctionStore)
        }

        // playhead-8em9 (narL): DEBUG-only DecisionLogger installation.
        // Production release builds never compile this branch, so no
        // decision-log.jsonl is ever written on a shipping binary. The
        // logger is safe to construct on a best-effort basis — any
        // FileManager failure (e.g. read-only Documents directory) is
        // logged and the service falls back to the installed NoOp.
        #if DEBUG
        Task { [adDetectionService] in
            do {
                let logger = try DecisionLogger()
                await adDetectionService.setDecisionLogger(logger)
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .warning("DecisionLogger init failed — logging disabled: \(error.localizedDescription, privacy: .public)")
            }
        }
        #endif

        Task { [analysisStore, downloadManager, analysisWorkScheduler, analysisJobReconciler, backgroundProcessingService, lanePreemptionCoordinator] in
            // Migrate the analysis store before any component queries its tables.
            do {
                try await analysisStore.migrate()
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .fault("Analysis store migration failed — pre-analysis pipeline disabled: \(error)")
                return  // Don't start the pipeline if tables don't exist
            }

            // bd-200: prune scan rows under stale cohort hashes (locale change,
            // app upgrade, prompt/schema/plan/normalization revs). Best-effort —
            // failures are logged but don't block app launch. Must run AFTER
            // migrate() succeeds but BEFORE any production code reads the store,
            // and must use the SAME ScanCohort.productionJSON() value the
            // BackfillJobRunner factory uses so the reuse-cache invariant holds.
            do {
                let pruned = try await analysisStore.pruneOrphanedScansForCurrentCohort(
                    currentScanCohortJSON: ScanCohort.productionJSON()
                )
                if pruned > 0 {
                    Logger(subsystem: "com.playhead", category: "Runtime")
                        .info("Pruned \(pruned, privacy: .public) orphan scan/evidence rows under prior cohorts")
                }
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .warning("Cohort orphan prune failed: \(error.localizedDescription, privacy: .public)")
            }

            // bd-3bz (Phase 4): only start the shadow-retry observer after
            // the store is definitely migrated. The observer immediately
            // queries the sessions table, so starting it earlier races the
            // schema bootstrap on cold launch.
            if let shadowRetryObserver {
                await self.startShadowRetryObserverIfNeeded(observer: shadowRetryObserver)
            }

            await downloadManager.setAnalysisWorkScheduler(analysisWorkScheduler)
            await backgroundProcessingService.setPreAnalysisServices(
                scheduler: analysisWorkScheduler,
                reconciler: analysisJobReconciler
            )
            // playhead-01t8: install the preemption coordinator as the
            // scheduler's `LanePreemptionHandler` before the loop
            // starts. The runner is already wired with the same
            // coordinator instance, so a Now-lane admission by the
            // scheduler flips the flag on the exact signal the running
            // job is polling.
            await analysisWorkScheduler.setLanePreemptionHandler(lanePreemptionCoordinator)
            do {
                _ = try await analysisJobReconciler.reconcile()
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .error("Job reconciliation failed at startup: \(error)")
            }
            await analysisWorkScheduler.startSchedulerLoop()
        }

        Task { [playbackService, analysisCoordinator, skipOrchestrator] in
            await skipOrchestrator.setSkipCueHandler { cues in
                Task { @PlaybackServiceActor in
                    playbackService.setSkipCues(cues)
                }
            }

            var lastStatus: PlaybackState.Status = .idle
            var lastSpeed: Float = 1.0

            let stateStream = await playbackService.observeStates()
            for await state in stateStream {
                await skipOrchestrator.updatePlayheadTime(state.currentTime)

                if state.playbackSpeed != lastSpeed {
                    lastSpeed = state.playbackSpeed
                    await analysisCoordinator.handlePlaybackEvent(
                        .speedChanged(rate: state.playbackSpeed, time: state.currentTime)
                    )
                }

                switch state.status {
                case .playing:
                    let rate = state.rate > 0 ? state.rate : state.playbackSpeed
                    await analysisCoordinator.handlePlaybackEvent(
                        .timeUpdate(time: state.currentTime, rate: rate)
                    )

                case .paused:
                    if lastStatus != .paused {
                        await analysisCoordinator.handlePlaybackEvent(.paused(time: state.currentTime))
                    }

                default:
                    break
                }

                lastStatus = state.status
            }
        }

        guard !isPreviewRuntime else { return }

        backgroundProcessingService.registerBackgroundTasks()

        Task { [downloadManager] in
            do {
                try await downloadManager.bootstrap()
            } catch {
                // Non-fatal: downloads will fail but playback still works.
            }

            // playhead-44h1 (fix): wire the willResignActive observer so
            // the foreground-assist hand-off decision runs in a shipped
            // build. Without this call, `handleWillResignActive` only
            // fires from tests and the entire bead deliverable is dead
            // code in production. `registerForegroundAssistLifecycleObserver`
            // is idempotent, so re-invocation (e.g. a preview runtime
            // mis-wire) is a safe no-op.
            await downloadManager.registerForegroundAssistLifecycleObserver()

            do {
                try await modelInventory.scan()
            } catch {
                // Settings can still render, but model lifecycle reporting will be empty.
            }

            do {
                try await speechService.loadFastModel(from: modelInventory.activeDirectory)
            } catch {
                // Speech asset preparation is best-effort at launch; the
                // transcript engine will surface failures when first used.
            }

            await backgroundProcessingService.start()
            await entitlementManager.start()
            await capabilitiesService.runSelfTest()
        }
    }

    deinit {
        // C7b: previously this `deinit` spawned `Task { await observer.stop() }`
        // to chase the async observer teardown. That was racy: the spawned
        // task could outlive the runtime by an unbounded amount, and a new
        // PlayheadRuntime construction back-to-back could find a stale
        // observer task still draining the wake stream from the prior
        // instance. Calling `shutdown()` is now mandatory (`withTestRuntime`
        // enforces this in tests). The `deinit` only does the synchronous
        // safety nets — cancelling the startup task and the drain timer —
        // and leaves the observer loop teardown to `shutdown()`.
        shadowRetryObserverStartupTask?.cancel()
    }

    // MARK: - FM classifier factory (Cycle 6 M-5 regression rail)

    /// Cycle 6 M-5: single factory that both the production
    /// `backfillJobRunnerFactory` closure AND unit tests call, so a
    /// regression that swaps in `.noop` on the production construction
    /// site fails an automated test instead of shipping silently.
    ///
    /// The factory is deliberately thin — it only forwards arguments
    /// into `FoundationModelClassifier.init`. Keeping it pure makes the
    /// regression rail meaningful: a test can hand it a real redactor
    /// and assert the returned classifier exposes that exact redactor
    /// via `redactorForTesting`. If the body is ever changed to ignore
    /// the incoming `redactor` parameter (or substitute `.noop`),
    /// `runtimeFactoryProducesActiveRedactor` fails at the assertion
    /// site; the negative rail
    /// `runtimeFactoryWithNoopProducesNoopRedactor` proves that the
    /// assertion actually discriminates between active and inactive
    /// redactors.
    ///
    /// `nonisolated` so tests (which are not on MainActor by default)
    /// can invoke it without hopping onto the main actor just to
    /// construct a classifier.
    nonisolated static func makeFoundationModelClassifier(
        redactor: PromptRedactor,
        feedbackStore: FoundationModelsFeedbackStore? = nil
    ) -> FoundationModelClassifier {
        FoundationModelClassifier(
            feedbackStore: feedbackStore,
            redactor: redactor
        )
    }

    #if DEBUG
    /// Cycle 4 H3: DEBUG-only accessor so `RuntimeTeardownTests` can
    /// assert that the shadow retry observer's loop actually exited
    /// after `shutdown()`. Exposed as a test seam rather than making the
    /// stored property internal to keep production access patterns
    /// unchanged.
    func _shadowRetryObserverForTesting() -> ShadowRetryObserver? {
        shadowRetryObserver
    }
    #endif

    /// Stops long-lived runtime observers and cancels any pending startup
    /// task. Safe to call more than once.
    func shutdown() async {
        guard !isShutdown else { return }
        isShutdown = true
        shadowRetryObserverStartupTask?.cancel()
        shadowRetryObserverStartupTask = nil
        await shadowRetryObserver?.stop()
    }

    @MainActor
    private func startShadowRetryObserverIfNeeded(observer: ShadowRetryObserver) {
        guard !isShutdown, shadowRetryObserverStartupTask == nil else { return }
        shadowRetryObserverStartupTask = Task { [observer] in
            guard !Task.isCancelled else { return }
            await observer.start()
        }
    }

    /// Capture and expose the current playback position for persistence.
    /// Called from playback and scene handlers to save state opportunistically.
    /// Returns the position in seconds, or nil if nothing is playing.
    func capturePlaybackPosition() async -> (episodeId: String, position: TimeInterval)? {
        guard let episodeId = currentEpisodeId else { return nil }
        let snapshot = await playbackService.snapshot()
        logger.info("Captured playback position: \(snapshot.currentTime)s")
        return (episodeId, snapshot.currentTime)
    }

    /// playhead-vhha: notify the candidate-window cascade of a committed
    /// playhead update. Called from `PlayheadApp.persistPlaybackPosition`
    /// after each successful SwiftData save so the cascade can re-latch
    /// the resumed-window selection when the user seeks more than
    /// `seekRelatchThresholdSeconds` (30 s by default) away from the
    /// prior anchor. Sub-threshold commits are silently no-op'd inside
    /// the cascade.
    ///
    /// playhead-swws: chapter evidence is sourced from the cascade's
    /// own per-episode cache (populated at `seedCandidateWindows` time).
    /// The commit-point caller does not carry chapter evidence in
    /// scope, so omitting it here preserves any sponsor-chapter
    /// windows the cascade selected at seed time — earlier we passed
    /// `[]` as a placeholder, which erased sponsors on every re-latch.
    func noteCommittedPlayhead(
        episodeId: String,
        position: TimeInterval,
        episodeDuration: TimeInterval?
    ) async {
        await analysisWorkScheduler.noteCommittedPlayhead(
            episodeId: episodeId,
            newPosition: position,
            episodeDuration: episodeDuration
        )
    }

    func setPlaybackPositionPersistenceHandler(
        _ handler: @escaping @MainActor (PlaybackPositionPersistenceTrigger) async -> Void
    ) {
        playbackPositionPersistenceHandler = handler
    }

    private func requestPlaybackPositionPersistence(
        _ trigger: PlaybackPositionPersistenceTrigger
    ) async {
        guard let handler = playbackPositionPersistenceHandler else { return }
        await handler(trigger)
    }

    func playEpisode(_ episode: Episode) async {
        let episodeId = episode.canonicalEpisodeKey
        let podcastId = episode.podcast?.feedURL.absoluteString
        let position = episode.playbackPosition

        currentEpisodeId = episodeId
        currentPodcastId = podcastId
        currentEpisodeTitle = episode.title
        currentPodcastTitle = episode.podcast?.title
        currentArtworkURL = episode.podcast?.artworkURL
        currentAnalysisAssetId = nil

        await backgroundProcessingService.playbackDidStart()

        // playhead-o45p: run the surface-status reducer in the
        // play-started context so a cold-start ready_entered fires for
        // episodes that are already `.complete` in the persistence
        // store. No-ops when the episode has no persisted
        // `analysis_assets` row yet — the analysis-completion edge in
        // `AnalysisCoordinator.transition` will handle that case once
        // the pipeline runs.
        await surfaceStatusObserver.observeEpisodePlayStarted(episodeId: episodeId)

        // Load artwork for lock screen / CarPlay Now Playing.
        var artwork: UIImage?
        if let artworkURL = episode.podcast?.artworkURL {
            if let (data, _) = try? await URLSession.shared.data(from: artworkURL),
               let image = UIImage(data: data) {
                artwork = image
            }
        }

        await playbackService.setNowPlayingMetadata(
            title: episode.title,
            artist: episode.podcast?.author,
            albumTitle: episode.podcast?.title,
            artworkImage: artwork
        )

        // Resolve a local audio file. Both playback and analysis use the
        // same file to avoid dynamic ad insertion serving different ads
        // on separate HTTP requests.
        let localURL: URL
        if let cached = episode.cachedAudioURL {
            localURL = cached
        } else if let cached = await downloadManager.cachedFileURL(for: episodeId) {
            localURL = cached
        } else {
            // Not cached — stream-download and play once enough is buffered.
            logger.info("Episode not cached — streaming download: \(episodeId)")
            audioCacheTask?.cancel()
            audioCacheTask = Task { [weak self, downloadManager, analysisCoordinator] in
                guard let self else { return }
                do {
                    let result = try await downloadManager.streamingDownload(
                        episodeId: episodeId,
                        from: episode.audioURL
                    )
                    guard !Task.isCancelled, self.currentEpisodeId == episodeId else { return }

                    // Build the progressive player item outside the actor.
                    // The ProgressiveResourceLoader must NOT live on
                    // PlaybackServiceActor — its AVFoundation delegate callbacks
                    // run on a separate dispatch queue and trigger actor executor
                    // assertions during Siri/phone call interruptions.
                    if let totalBytes = result.totalBytes {
                        let loader = ProgressiveResourceLoader(
                            fileURL: result.fileURL,
                            totalBytes: totalBytes,
                            contentType: result.contentType
                        )
                        // Hold a strong reference so the loader outlives this scope.
                        self.activeProgressiveLoader = loader

                        var components = URLComponents()
                        components.scheme = "playhead-progressive"
                        components.host = "audio"
                        components.path = "/\(result.fileURL.lastPathComponent)"
                        if let proxyURL = components.url {
                            let asset = AVURLAsset(url: proxyURL)
                            asset.resourceLoader.setDelegate(loader, queue: loader.queue)
                            let item = AVPlayerItem(asset: asset)
                            await self.playbackService.loadItem(item, startPosition: position)
                        } else {
                            await self.playbackService.load(url: result.fileURL, startPosition: position)
                        }
                    } else {
                        await self.playbackService.load(url: result.fileURL, startPosition: position)
                    }
                    await self.playbackService.play()
                    await self.analysisWorkScheduler.playbackStarted(episodeId: episodeId)

                    // Resolve the analysis asset ID early so pre-materialized
                    // skip cues can be loaded before the download finishes.
                    guard let analysisURL = LocalAudioURL(result.fileURL) else {
                        self.logger.error("Download returned non-local URL: \(result.fileURL)")
                        return
                    }
                    let resolvedAssetId = await analysisCoordinator.handlePlaybackEvent(
                        .playStarted(
                            episodeId: episodeId,
                            podcastId: podcastId,
                            audioURL: analysisURL,
                            time: position,
                            rate: 1.0
                        )
                    )
                    self.currentAnalysisAssetId = resolvedAssetId
                    if let assetId = resolvedAssetId {
                        await self.skipOrchestrator.beginEpisode(
                            analysisAssetId: assetId,
                            episodeId: episodeId,
                            podcastId: podcastId
                        )
                    }

                    // Wait for the full download before starting analysis —
                    // the decoder needs the complete file to get all shards.
                    try await result.downloadComplete()
                    guard !Task.isCancelled, self.currentEpisodeId == episodeId else { return }

                    // Release the progressive loader — download is complete,
                    // all bytes are on disk and already served.
                    self.activeProgressiveLoader = nil

                    // Evict any stale shard cache from a prior partial decode
                    // (the truncated 2MB file had different shard count/content).
                    await self.audioService.evictCache(episodeID: episodeId)
                } catch {
                    guard !Task.isCancelled else { return }
                    self.logger.error("Episode download failed: \(error)")
                }
            }
            return
        }

        // Audio is local — play and analyze from the same file.
        await playbackService.load(url: localURL, startPosition: position)
        await playbackService.play()
        await analysisWorkScheduler.playbackStarted(episodeId: episodeId)

        guard let analysisURL = LocalAudioURL(localURL) else { return }
        let resolvedAssetId = await analysisCoordinator.handlePlaybackEvent(
            .playStarted(
                episodeId: episodeId,
                podcastId: podcastId,
                audioURL: analysisURL,
                time: position,
                rate: 1.0
            )
        )
        currentAnalysisAssetId = resolvedAssetId

        if let assetId = resolvedAssetId {
            await skipOrchestrator.beginEpisode(
                analysisAssetId: assetId,
                episodeId: episodeId,
                podcastId: podcastId
            )
        }
    }

    func stopPlayback() async {
        await requestPlaybackPositionPersistence(.stopped)
        audioCacheTask?.cancel()
        audioCacheTask = nil
        await analysisWorkScheduler.playbackStopped()
        await backgroundProcessingService.playbackDidStop()
        await analysisCoordinator.handlePlaybackEvent(.stopped)
        await skipOrchestrator.endEpisode()
        await playbackService.pause()
        currentEpisodeId = nil
        currentPodcastId = nil
        currentAnalysisAssetId = nil
        currentEpisodeTitle = nil
        currentPodcastTitle = nil
        currentArtworkURL = nil
    }

    func recordListenRewind(windowId: String, podcastId: String) async {
        do {
            try await adDetectionService.recordListenRewind(
                windowId: windowId,
                podcastId: podcastId
            )
        } catch {
            // Rewinds are user-facing; trust scoring remains best-effort.
        }
    }

    func togglePlayPause(isPlaying: Bool) async {
        if isPlaying {
            let snapshot = await playbackService.snapshot()
            await playbackService.pause()
            await analysisCoordinator.handlePlaybackEvent(.paused(time: snapshot.currentTime))
            await requestPlaybackPositionPersistence(.paused)
        } else {
            let snapshot = await playbackService.snapshot()
            await playbackService.play()
            await analysisCoordinator.handlePlaybackEvent(
                .timeUpdate(
                    time: snapshot.currentTime,
                    rate: snapshot.playbackSpeed
                )
            )
        }
    }

    func skipForward() async {
        let snapshot = await playbackService.snapshot()
        let target = min(
            snapshot.currentTime + PlaybackService.skipForwardSeconds,
            snapshot.duration
        )
        await seek(to: target)
    }

    func skipBackward() async {
        let snapshot = await playbackService.snapshot()
        let target = max(
            snapshot.currentTime - PlaybackService.skipBackwardSeconds,
            0
        )
        await seek(to: target)
    }

    func seek(to seconds: TimeInterval) async {
        let snapshot = await playbackService.snapshot()
        await skipOrchestrator.recordUserSeek(to: seconds)
        await playbackService.seek(to: seconds)
        await analysisCoordinator.handlePlaybackEvent(
            .scrubbed(to: seconds, rate: snapshot.playbackSpeed)
        )
        await requestPlaybackPositionPersistence(.seek)
    }

    func setSpeed(_ speed: Float) async {
        let snapshot = await playbackService.snapshot()
        await playbackService.setSpeed(speed)
        await analysisCoordinator.handlePlaybackEvent(
            .speedChanged(rate: speed, time: snapshot.currentTime)
        )
    }

    // MARK: - User Correction: Mark as Ad

    /// Inject a user-marked ad region for immediate skip + persistence.
    /// Called from NowPlayingViewModel (hearing-an-ad button) and
    /// TranscriptPeekView (transcript chunk selection).
    ///
    /// 1. Tells SkipOrchestrator to inject the window for immediate effect
    ///    (skip cues, banner, timeline markers).
    /// 2. Tells AdDetectionService to persist the AdWindow and CorrectionEvent
    ///    so future analysis incorporates the correction.
    func injectUserMarkedAd(start: Double, end: Double) async {
        guard let assetId = currentAnalysisAssetId else { return }
        let podcastId = currentPodcastId

        // Immediate effect: inject into the live skip orchestrator.
        await skipOrchestrator.injectUserMarkedAd(
            start: start,
            end: end,
            analysisAssetId: assetId
        )

        // Durable persistence: write AdWindow + CorrectionEvent to SQLite.
        await adDetectionService.recordUserMarkedAd(
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            podcastId: podcastId
        )
    }

    func setShowSkipMode(_ mode: SkipMode, orchestrator: SkipOrchestrator) async {
        if let podcastId = currentPodcastId {
            await trustService.setUserOverride(podcastId: podcastId, mode: mode)
        }
        await orchestrator.setActiveSkipMode(mode)
    }

    // MARK: - Activity screen wiring (playhead-quh7)

    /// Build a `LiveActivitySnapshotProvider` bound to this runtime's
    /// long-lived services. Called from `ContentView` so the Activity
    /// tab's view-model can re-aggregate from real persistence + live
    /// scheduler state on each `ActivityRefreshNotification` post.
    ///
    /// Lives on the runtime (not on a SwiftUI view) so the closure
    /// captures only Sendable references; the view passes the model
    /// context through because the SwiftData container is owned by the
    /// app's environment, not the runtime.
    func makeActivitySnapshotProvider(
        modelContext: ModelContext
    ) -> LiveActivitySnapshotProvider {
        LiveActivitySnapshotProvider(
            store: analysisStore,
            capabilitySnapshotProvider: { [capabilitiesService] in
                let snapshot = await capabilitiesService.currentSnapshot
                return Optional(snapshot)
            },
            runningEpisodeIdProvider: { [analysisWorkScheduler] in
                await analysisWorkScheduler.currentlyRunningEpisodeId()
            },
            modelContext: modelContext
        )
    }

    // MARK: - playhead-zp0x / playhead-0a0s: batch summary builder

    /// Build the `summaryBuilder` closure passed into
    /// `BatchNotificationCoordinator`.
    ///
    /// playhead-0a0s wired the full per-episode `EpisodeSurfaceStatus`
    /// reducer through the batch path so `blocked*` notifications fire
    /// on real per-episode failures (storage cap, Wi-Fi-only policy,
    /// Apple Intelligence disabled, etc). The closure resolves three
    /// inputs per child:
    ///   1. The persisted `Episode` row (downloadState + analysisSummary
    ///      → isReady, plus coverage/anchor for the reducer).
    ///   2. The most-recent `InternalMissCause` from the work-journal
    ///      (drives Rules 3 / 4 of the reducer — storage and transient-
    ///      wait blockers).
    ///   3. The live `AnalysisEligibility` derived from
    ///      `CapabilitiesService.currentSnapshot` (drives Rule 1 —
    ///      analysis-unavailable blockers).
    ///
    /// `BatchSummaryBuilder.makeSummary(...)` is the pure projection
    /// from those inputs to a `BatchChildSurfaceSummary`; it routes
    /// through `episodeSurfaceStatus(...)` and centralises the
    /// `userFixable` derivation at the boundary the
    /// `BatchNotificationReducer` documents as its source of truth.
    ///
    /// Lives on PlayheadRuntime (rather than PlayheadApp) so the
    /// PlayheadApp UI layer never references `AnalysisStore` by type —
    /// `SurfaceStatusUILintTests` polices that boundary. The runtime
    /// owns both `analysisStore` and `capabilitiesService`, so the
    /// caller only has to pass the `ModelContainer` through (the one
    /// long-lived dependency owned by the `App`-scope environment).
    func makeBatchSummaryBuilder(
        modelContainer: ModelContainer
    ) -> @Sendable ([String]) async -> [BatchChildSurfaceSummary] {
        let analysisStore = self.analysisStore
        let capabilitiesService = self.capabilitiesService
        let builder = BatchSummaryBuilder(
            episodeLookup: { @Sendable key in
                await MainActor.run {
                    let context = modelContainer.mainContext
                    let descriptor = FetchDescriptor<Episode>(
                        predicate: #Predicate { $0.canonicalEpisodeKey == key }
                    )
                    guard let episode = try? context.fetch(descriptor).first else {
                        return nil
                    }
                    return EpisodeProjection(episode)
                }
            },
            causeLookup: { @Sendable key in
                // Best-effort: a SQLite read failure surfaces as `nil`
                // (no live cause), which the reducer treats as Rule 5
                // "queued / waitingForTime" — a safe fallback that
                // never promotes a child to action-required.
                try? await analysisStore.fetchLastWorkJournalCause(episodeId: key)
            },
            eligibilityProvider: { @Sendable in
                let snapshot = await capabilitiesService.currentSnapshot
                return EpisodeSurfaceStatusObserver.eligibility(from: snapshot)
            }
        )
        return { episodeKeys in
            await builder.summaries(for: episodeKeys)
        }
    }

}

/// Thread-safe holder for the lazily-initialized `ShadowRetryObserver`.
///
/// The shadow-skip-marker closure is built BEFORE the observer is
/// constructed (the observer depends on `AdDetectionService`, which
/// already captured the marker closure). The closure therefore needs a
/// box it can read through at call time. The write happens exactly
/// once, on the MainActor during runtime init, but reads happen from
/// arbitrary actor contexts whenever a shadow session is flagged —
/// that's a cross-thread read of a mutable field and must be
/// synchronized.
///
/// Cycle-4 M1: previously implemented as `@unchecked Sendable` with a
/// plain `var`, justified by "writes happen before the first marker
/// call can reach the closure". That argument is functionally correct
/// but brittle — a future refactor that hoists a marker call earlier in
/// init would silently race. `OSAllocatedUnfairLock` makes the
/// synchronization explicit and lets the type drop the `@unchecked`
/// escape hatch.
private final class ShadowRetryObserverHolder: Sendable {
    private let storage = OSAllocatedUnfairLock<ShadowRetryObserver?>(initialState: nil)

    var observer: ShadowRetryObserver? {
        get { storage.withLock { $0 } }
        set { storage.withLock { $0 = newValue } }
    }
}
