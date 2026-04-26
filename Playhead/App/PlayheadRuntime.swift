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
    /// playhead-shpy: BG-task lifecycle telemetry logger. Exposed so the
    /// App-scope wiring in `PlayheadApp.task` can thread it into the
    /// `BackgroundFeedRefreshService` it constructs separately. Test
    /// hosts and preview runtimes get a no-op instance.
    let bgTaskTelemetryLogger: any BGTaskTelemetryLogging
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

    /// playhead-narl.2 shadow FM dual-run capture coordinator. `nil` in
    /// preview runtimes so SwiftUI canvas instances don't spin up an FM
    /// runtime. Lane A is driven from the playback state loop below; Lane B
    /// is driven from `AnalysisWorkScheduler`'s idle slot via
    /// `setShadowLaneTickHandler`.
    let shadowCaptureCoordinator: ShadowCaptureCoordinator?

    /// Retained so the shadow coordinator's synchronous protocol getters
    /// have a stable producer for the currently loaded episode's asset id.
    /// Updated by `PlayheadRuntime.setCurrentEpisode(...)` indirectly through
    /// `currentAnalysisAssetId`. Read via `@Sendable` closure by
    /// `LivePlaybackSignalProvider`.
    private let shadowPlaybackSignal: LivePlaybackSignalProvider?
    private let shadowEnvironmentSignal: LiveEnvironmentSignalProvider?

    /// playhead-narl.2: lock-protected mirror of `currentAnalysisAssetId` that
    /// `LivePlaybackSignalProvider`'s `@Sendable` closure reads from. The
    /// MainActor-confined `currentAnalysisAssetId` observable property can't
    /// be touched by a `@Sendable` closure, so every site that mutates it
    /// also publishes into this mirror. Declared in preview runtimes too so
    /// the property layout is uniform; preview runtimes just never attach a
    /// coordinator that would read it.
    private let currentAssetIdMirror = OSAllocatedUnfairLock<String?>(initialState: nil)

    /// playhead-yqax: lock-protected mirror of `currentEpisodeId` that the
    /// transport-status observer Task reads from when forwarding the live
    /// playhead position into the scheduler's foreground catch-up trigger.
    /// The Task captures dependencies by value (no `self`) to avoid
    /// retaining the runtime through this hot loop, so it cannot read the
    /// MainActor-confined `currentEpisodeId` directly. Every site that
    /// mutates `currentEpisodeId` MUST also publish into this mirror via
    /// ``setCurrentEpisodeId(_:)`` — a forgotten mirror update would
    /// silently break catch-up dispatch (the position would forward with
    /// a stale or nil episodeId and be dropped by the scheduler's
    /// stale-tick filter).
    private let currentEpisodeIdMirror = OSAllocatedUnfairLock<String?>(initialState: nil)

    private let isPreviewRuntime: Bool
    private let logger = Logger(subsystem: "com.playhead", category: "Runtime")
    @ObservationIgnored
    private var playbackPositionPersistenceHandler: (@MainActor (PlaybackPositionPersistenceTrigger) async -> Void)?

    private(set) var currentEpisodeId: String?
    private(set) var currentPodcastId: String?
    /// MainActor-confined observable property. External readers go through
    /// `@Observable` change tracking. Writers MUST use
    /// ``setCurrentAnalysisAssetId(_:)`` so the Sendable-closure-readable
    /// `currentAssetIdMirror` stays in sync — a forgotten mirror update
    /// would silently break Lane A. `private(set)` enforces this at the
    /// type boundary; the setter is the only internal write site.
    private(set) var currentAnalysisAssetId: String?
    private(set) var currentEpisodeTitle: String?
    private(set) var currentPodcastTitle: String?
    private(set) var currentArtworkURL: URL?

    /// Single write site for `currentAnalysisAssetId`. Updates both the
    /// `@Observable` property (for SwiftUI/main-actor observers) and the
    /// `OSAllocatedUnfairLock`-protected `currentAssetIdMirror` that the
    /// `LivePlaybackSignalProvider`'s `@Sendable` closure reads. Call
    /// this instead of assigning `currentAnalysisAssetId` directly;
    /// future mutation sites that forget the mirror would silently break
    /// Lane A.
    private func setCurrentAnalysisAssetId(_ newValue: String?) {
        currentAnalysisAssetId = newValue
        // playhead-narl.2: mirror for Sendable closure access.
        currentAssetIdMirror.withLock { $0 = newValue }
    }

    /// Single write site for `currentEpisodeId`. Updates both the
    /// MainActor-confined `@Observable` property and the
    /// `OSAllocatedUnfairLock`-protected `currentEpisodeIdMirror` that
    /// the transport-status observer Task reads from. Call this instead
    /// of assigning `currentEpisodeId` directly; see playhead-yqax.
    private func setCurrentEpisodeId(_ newValue: String?) {
        currentEpisodeId = newValue
        currentEpisodeIdMirror.withLock { $0 = newValue }
    }

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
            } else if let tmpStore = try? AnalysisStore(
                directory: FileManager.default.temporaryDirectory
                    .appendingPathComponent("PlayheadAnalysis-\(ProcessInfo.processInfo.globallyUniqueString)", isDirectory: true)
            ) {
                // Temp-dir fallback — analysis won't persist across launches.
                resolvedStore = tmpStore
                storeError = "Analysis database could not be opened. Ad detection may not persist across launches."
            } else {
                // Last-resort in-memory store: no file is written, so this
                // path survives Data Protection and disk failures that
                // brick both the Documents and tmp opens. The previous
                // implementation used `try!` here, which crashed the
                // process on background launches when the device was
                // locked (Data Protection `.complete` made the SQLite
                // file unreadable). Crashing the app before any work
                // runs is strictly worse than running one in-memory
                // session: the next launch that happens with the device
                // unlocked will drop back to the default path cleanly.
                // `:memory:` is a pure RAM open — it does not touch disk
                // or Data Protection, so it cannot hit the failure mode
                // that brought us here. If even this fails the device is
                // in deep trouble and there is no useful work to do.
                resolvedStore = try! AnalysisStore(path: ":memory:")
                storeError = "Analysis database could not be opened. Using in-memory store for this launch."
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
        // playhead-8em9 (narL): DEBUG-only DecisionLogger. Constructed
        // synchronously BEFORE AdDetectionService so the very first
        // backfill or hot-path run observes the installed logger (the
        // prior async `setDecisionLogger` Task could race the first
        // backfill and drop its decision records). Production release
        // builds never compile this branch; any FileManager failure
        // (e.g. read-only Documents) is logged and the service keeps
        // its built-in NoOp default.
        let preBuiltDecisionLogger: DecisionLoggerProtocol?
        #if DEBUG
        do {
            preBuiltDecisionLogger = try DecisionLogger()
        } catch {
            Logger(subsystem: "com.playhead", category: "Runtime")
                .warning("DecisionLogger init failed — logging disabled: \(error.localizedDescription, privacy: .public)")
            preBuiltDecisionLogger = nil
        }
        #else
        preBuiltDecisionLogger = nil
        #endif

        // playhead-gtt9.17: on-device ad catalog. Opened synchronously at
        // startup so the first `runBackfill` observes an initialized store.
        // A failure to open (e.g. read-only disk) falls back to `nil`,
        // which degrades to the pre-gtt9.17 behavior: no catalog ingress,
        // no catalog egress, no runtime crash. We log the failure so
        // diagnostics can surface the regression rather than silently
        // losing the feature.
        let adCatalogStore: AdCatalogStore?
        do {
            let dir = try AdCatalogStore.defaultDirectory()
            adCatalogStore = try AdCatalogStore(directoryURL: dir)
        } catch {
            Logger(subsystem: "com.playhead", category: "Runtime")
                .warning("AdCatalogStore init failed — catalog disabled: \(error.localizedDescription, privacy: .public)")
            adCatalogStore = nil
        }

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
            skipOrchestrator: skipOrchestrator,
            // playhead-gtt9.17: on-device catalog for catalog ingress
            // (autoSkipEligible → insert) and egress (fingerprint → match).
            adCatalogStore: adCatalogStore,
            decisionLogger: preBuiltDecisionLogger
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

        // playhead-gtt9.8: per-asset lifecycle logger. Production
        // builds write to `Documents/asset-lifecycle-log.jsonl`;
        // release builds log too — this is a small telemetry signal
        // (~one line per transition, rotated at 10 MB) that the NARL
        // harness reads back to reconstruct per-asset timelines.
        // Any FileManager failure (e.g. read-only Documents in test)
        // falls back to the no-op logger.
        let lifecycleLogger: AssetLifecycleLoggerProtocol
        do {
            lifecycleLogger = try AssetLifecycleLogger()
        } catch {
            Logger(subsystem: "com.playhead", category: "Runtime")
                .warning("AssetLifecycleLogger init failed — logging disabled: \(error.localizedDescription, privacy: .public)")
            lifecycleLogger = NoOpAssetLifecycleLogger()
        }

        self.analysisCoordinator = AnalysisCoordinator(
            store: analysisStore,
            audioService: audioService,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            capabilitiesService: capabilitiesService,
            adDetectionService: adDetectionService,
            skipOrchestrator: skipOrchestrator,
            downloadManager: downloadManager,
            surfaceStatusObserver: surfaceStatusObserver,
            lifecycleLogger: lifecycleLogger
        )
        // playhead-shpy: shared BG-task lifecycle telemetry logger.
        // Constructed once and threaded into every actor that calls
        // BGTaskScheduler.submit / sets up a launch handler. A
        // FileManager failure (e.g. read-only Documents in the test
        // host) demotes to the no-op logger so the service path stays
        // alive — observability is additive, never load-bearing.
        let bgTaskTelemetry: any BGTaskTelemetryLogging
        do {
            bgTaskTelemetry = try BGTaskTelemetryLogger()
        } catch {
            Logger(subsystem: "com.playhead", category: "Runtime")
                .warning("BGTaskTelemetryLogger init failed — telemetry disabled: \(error.localizedDescription, privacy: .public)")
            bgTaskTelemetry = NoOpBGTaskTelemetryLogger()
        }
        self.bgTaskTelemetryLogger = bgTaskTelemetry
        self.backgroundProcessingService = BackgroundProcessingService(
            coordinator: analysisCoordinator,
            capabilitiesService: capabilitiesService,
            bgTelemetry: bgTaskTelemetry
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
        // playhead-gjz6 (Gap-4 second half): wire the live
        // `BackgroundProcessingService` into the scheduler via the
        // existing `ProductionBackfillScheduler` adapter so an
        // `enqueue` call that lands while the app is already
        // backgrounded triggers a fresh `BGProcessingTaskRequest`.
        // Without this seam, downloads that complete via background
        // URLSession after the foreground→background transition has
        // already fired leave their analysis work queued until the
        // next foreground. Reuses the `BackfillScheduling` adapter
        // shipped for Gap-5 (`BackgroundFeedRefreshService.swift`) —
        // same protocol, same production wrapper.
        self.analysisWorkScheduler = AnalysisWorkScheduler(
            store: analysisStore,
            jobRunner: analysisJobRunner,
            capabilitiesService: capabilitiesService,
            downloadManager: downloadManager,
            batteryProvider: batteryProvider,
            candidateWindowCascade: CandidateWindowCascade(),
            backfillScheduler: ProductionBackfillScheduler(
                backgroundProcessingService: backgroundProcessingService
            )
        )
        // playhead-gtt9.14: wire the scheduler as the coordinator's
        // scheduler-state snapshot source so every lifecycle-log record
        // carries the (scenePhase, playbackContext, qualityProfile)
        // triple. The coordinator must be constructed first (above) so
        // it can accept this setter; attaching post-init keeps the two
        // dependency cones decoupled.
        do {
            let adapter = AnalysisWorkSchedulerStateSnapshotAdapter(
                scheduler: analysisWorkScheduler
            )
            let coordinator = analysisCoordinator
            Task { await coordinator.setSchedulerStateSnapshotProvider(adapter) }
        }
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

        // playhead-narl.2: construct the shadow FM dual-run pipeline. Preview
        // runtimes skip the entire block (no FM runtime, no observers, no
        // coordinator) so SwiftUI canvases stay lightweight. Lane A is
        // ticked from the playback state loop below; Lane B is ticked from
        // the analysis work scheduler via `setShadowLaneTickHandler`.
        if !isPreviewRuntime {
            // Make a local capture so the @Sendable closure that reads the
            // currently loaded episode's analysis asset id doesn't have to
            // reach back into `self`. The lock instance is reference-typed,
            // so capturing it by value carries the shared state.
            let assetIdMirror = currentAssetIdMirror
            let playbackSignal = LivePlaybackSignalProvider(
                playbackService: playbackService,
                assetIdProvider: { assetIdMirror.withLock { $0 } }
            )
            self.shadowPlaybackSignal = playbackSignal
            let environmentSignal = LiveEnvironmentSignalProvider(
                capabilitiesService: capabilitiesService
            )
            self.shadowEnvironmentSignal = environmentSignal
            let shadowRuntime = FoundationModelClassifier.makeLiveRuntimeForShadow()
            let dispatcher = LiveShadowFMDispatcher(
                store: analysisStore,
                runtime: shadowRuntime
            )
            let windowSource = LiveShadowWindowSource(store: analysisStore)
            self.shadowCaptureCoordinator = ShadowCaptureCoordinator(
                store: analysisStore,
                dispatcher: dispatcher,
                windowSource: windowSource,
                playbackSignal: playbackSignal,
                environmentSignal: environmentSignal
            )
        } else {
            self.shadowPlaybackSignal = nil
            self.shadowEnvironmentSignal = nil
            self.shadowCaptureCoordinator = nil
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

        // playhead-8em9 (narL): DecisionLogger installation moved upstream
        // — it's now constructed before AdDetectionService and passed via
        // init, so there's no race with the first backfill/hot-path run.

        Task { [analysisStore, downloadManager, analysisWorkScheduler, analysisJobReconciler, backgroundProcessingService, lanePreemptionCoordinator, analysisCoordinator, shadowCaptureCoordinator] in
            // Migrate the analysis store before any component queries its tables.
            do {
                try await analysisStore.migrate()
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .fault("Analysis store migration failed — pre-analysis pipeline disabled: \(error)")
                return  // Don't start the pipeline if tables don't exist
            }

            // Recover any sessions that the finalizeBackfill coverage guard
            // stranded in `.failed` before the transcript had fully caught
            // up. The sweep re-checks each candidate's current transcript
            // coverage against the preserved denominator and flips the
            // row back to `.backfill` when the ratio now clears the
            // minimum, so the next pipeline run finalizes them cleanly.
            // Errors inside the sweep are logged and swallowed — the
            // pipeline start below must not be blocked by diagnostic
            // recovery work.
            _ = await analysisCoordinator.recoverCoverageGuardFailures()

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
            // playhead-narl.2: install the shadow Lane B tick handler before
            // starting the scheduler loop. The handler forwards idle ticks
            // into `ShadowCaptureCoordinator.tickLaneB()`. `nil` coordinator
            // (preview runtime) leaves the scheduler's handler unwired and
            // the shadow pipeline silent.
            if let shadowCaptureCoordinator {
                await analysisWorkScheduler.setShadowLaneTickHandler(
                    ShadowLaneBAdapter(coordinator: shadowCaptureCoordinator)
                )
            }
            // playhead-5uvz.2 (Gap-2): journal-aware orphan recovery.
            // Runs BEFORE the reconciler's blind `recoverExpiredLeases`
            // sweep so the journal-aware policy (terminal vs. resumable
            // based on the last `work_journal` event) gets first dibs on
            // every stranded `analysis_jobs` row. Bumps schedulerEpoch,
            // mints fresh generationIDs, demotes Now-lane orphans stale
            // > 60 s to Soon. Errors are logged and swallowed — the
            // reconciler.reconcile fallback below still runs on failure
            // so a corrupt journal cannot brick startup.
            //
            // Empty-journal cold-launch behavior: when the journal has
            // no row for an orphan's {episodeId, generationID} (the
            // legitimate first-launch-after-rollout state, or any row
            // last touched before the playhead-5uvz.1 wiring landed),
            // `recoverOrphans` routes via `decisionEvent == .none` →
            // resume branch (requeue with fresh epoch + generation).
            // Same effective behavior as the reconciler's blind sweep,
            // but with the additional epoch bump + lane demotion that
            // the reconciler skips.
            //
            // graceSeconds=30: large enough to absorb clock skew at
            // cold launch, small enough that an orphan whose owner
            // crashed seconds before the relaunch still gets recovered
            // on this pass rather than the next.
            do {
                let now = Date().timeIntervalSince1970
                let rebuilt = try await analysisCoordinator.recoverOrphans(
                    now: now,
                    graceSeconds: 30
                )
                if !rebuilt.isEmpty {
                    Logger(subsystem: "com.playhead", category: "Runtime")
                        .info("recoverOrphans rebuilt \(rebuilt.count, privacy: .public) lease(s) at cold launch")
                }
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .error("recoverOrphans failed at startup; falling back to reconciler sweep: \(error)")
            }

            // Reconciler.reconcile remains the fallback path. Its
            // `recoverExpiredLeases` step is now redundant for any
            // orphan the journal-aware path already requeued (the
            // journal-aware path cleared the lease slot OR moved the
            // row to a fresh epoch), but kept as cheap insurance for
            // edge cases the journal-aware path skips: corrupt epoch
            // (journal row's epoch > _meta.scheduler_epoch), per-job
            // DB errors, and the journal-empty rows that appeared
            // BEFORE this PR shipped (their generationID is "", so
            // `fetchLastWorkJournalEntry` returns nil → resume branch
            // requeues them too — but the reconciler's blind sweep
            // catches anything missed).
            do {
                _ = try await analysisJobReconciler.reconcile()
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .error("Job reconciliation failed at startup: \(error)")
            }
            await analysisWorkScheduler.startSchedulerLoop()
        }

        Task { [playbackService, analysisCoordinator, skipOrchestrator, shadowCaptureCoordinator, analysisWorkScheduler, currentEpisodeIdMirror] in
            await skipOrchestrator.setSkipCueHandler { cues in
                Task { @PlaybackServiceActor in
                    playbackService.setSkipCues(cues)
                }
            }

            var lastStatus: PlaybackState.Status = .idle
            var lastSpeed: Float = 1.0
            // playhead-gtt9.14: coalesce PlaybackState.Status → scheduler
            // PlaybackContext updates. The transport stream emits on every
            // AVPlayer periodic-time tick; re-entering the actor on each
            // one would hammer the scheduler. Forward only when the mapped
            // context actually changes.
            var lastForwardedContext: AnalysisWorkScheduler.PlaybackContext = .idle

            // playhead-yqax: coalesce playhead-position forwarding to the
            // scheduler at whole-second granularity. The transport stream
            // emits on every AVPlayer periodic-time tick (sub-second
            // cadence) — forwarding each tick into the actor would
            // re-evaluate the catch-up trigger many times per second for
            // no real signal change. Whole-second coalescing keeps the
            // scheduler responsive (catch-up fires within ~1 s of the
            // trigger condition becoming true) without burning actor
            // hops on duplicate work. `Int.max` sentinel ensures the
            // first observed position always forwards.
            var lastForwardedPlayheadSec: Int = .max

            let stateStream = await playbackService.observeStates()
            for await state in stateStream {
                await skipOrchestrator.updatePlayheadTime(state.currentTime)

                if state.playbackSpeed != lastSpeed {
                    lastSpeed = state.playbackSpeed
                    await analysisCoordinator.handlePlaybackEvent(
                        .speedChanged(rate: state.playbackSpeed, time: state.currentTime)
                    )
                }

                // playhead-gtt9.14: forward transport status to the
                // scheduler so its admission filter distinguishes playing
                // from paused. `.loading` and `.failed` fold into
                // `.paused` (loaded but not producing audio).
                let mappedContext: AnalysisWorkScheduler.PlaybackContext
                switch state.status {
                case .playing:                   mappedContext = .playing
                case .paused, .loading, .failed: mappedContext = .paused
                case .idle:                      mappedContext = .idle
                }
                if mappedContext != lastForwardedContext {
                    lastForwardedContext = mappedContext
                    await analysisWorkScheduler.updatePlaybackContext(mappedContext)
                }

                // playhead-yqax: forward the live playhead position so
                // the scheduler can fire foreground transcript catch-up
                // when transcribed audio runs low ahead of the user.
                // We only forward while playing — `.paused` / `.idle` /
                // `.loading` / `.failed` either aren't producing forward
                // motion (paused) or have already cleared the position
                // via `updatePlaybackContext`. Coalesce at 1 s
                // granularity so the actor sees one update per second
                // worst case rather than the 5–10 Hz periodic-time
                // cadence.
                if mappedContext == .playing,
                   let activeEpisodeId = currentEpisodeIdMirror.withLock({ $0 }) {
                    let bucketed = Int(state.currentTime)
                    if bucketed != lastForwardedPlayheadSec {
                        lastForwardedPlayheadSec = bucketed
                        await analysisWorkScheduler.noteCurrentPlayheadPosition(
                            episodeId: activeEpisodeId,
                            position: state.currentTime
                        )
                    }
                } else if mappedContext == .idle {
                    // Reset the coalescing sentinel so the next
                    // play-start always forwards its first observed
                    // position regardless of whole-second alignment.
                    lastForwardedPlayheadSec = .max
                }

                switch state.status {
                case .playing:
                    let rate = state.rate > 0 ? state.rate : state.playbackSpeed
                    await analysisCoordinator.handlePlaybackEvent(
                        .timeUpdate(time: state.currentTime, rate: rate)
                    )
                    // playhead-narl.2: Lane A tick piggybacks on the playback
                    // heartbeat. The coordinator samples its own playback
                    // signal provider internally (no args), gates on the kill
                    // switch + strict-playback + rate-limit, and returns
                    // promptly on any no-op branch.
                    //
                    // Fire-and-forget: the coordinator is an actor with its
                    // own in-flight accounting (`laneAInFlight`,
                    // `laneAMaxInFlight = 1`), so an unstructured Task cannot
                    // race itself. Awaiting here would stall subsequent
                    // `skipOrchestrator.updatePlayheadTime(...)` ticks on the
                    // ~3s FM call and regress user-visible skip-cue latency.
                    // Capture the coordinator by value (not via `self`) to
                    // avoid retaining the runtime through this hot loop.
                    Task { [shadowCaptureCoordinator] in
                        await shadowCaptureCoordinator?.tickLaneA()
                    }

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
        // playhead-fv2q: register the periodic BGAppRefreshTask identifier
        // before launch completes. The real service instance is built and
        // attached later by `PlayheadApp.task` once the `ModelContainer`
        // is available; see `BackgroundFeedRefreshService.attachSharedService`.
        BackgroundFeedRefreshService.registerTaskHandler()

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

        setCurrentEpisodeId(episodeId)
        currentPodcastId = podcastId
        currentEpisodeTitle = episode.title
        currentPodcastTitle = episode.podcast?.title
        currentArtworkURL = episode.podcast?.artworkURL
        setCurrentAnalysisAssetId(nil)

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

            // playhead-i9dj: capture the SwiftData titles before entering
            // the Task closure so the AnalysisStore can persist them for
            // self-describing exports without re-reading SwiftData inside
            // the Task. The podcastId mirrors the rest of the runtime
            // (`feedURL.absoluteString`) — see line 1032. Both title
            // fields are optional: `Episode.title` is required-non-empty
            // in our SwiftData schema, but `Podcast.title` may legitimately
            // be missing on partial feeds.
            let titleContext = DownloadContext(
                podcastId: podcastId,
                isExplicitDownload: false,
                podcastTitle: episode.podcast?.title,
                episodeTitle: episode.title
            )
            let audioURL = episode.audioURL

            audioCacheTask = Task { [weak self, downloadManager, analysisCoordinator] in
                guard let self else { return }
                do {
                    let result = try await downloadManager.streamingDownload(
                        episodeId: episodeId,
                        from: audioURL,
                        context: titleContext
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
                    self.setCurrentAnalysisAssetId(resolvedAssetId)
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
        setCurrentAnalysisAssetId(resolvedAssetId)

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
        setCurrentEpisodeId(nil)
        currentPodcastId = nil
        setCurrentAnalysisAssetId(nil)
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

// MARK: - ShadowLaneBAdapter

/// playhead-narl.2: thin adapter so `AnalysisWorkScheduler` can install a
/// `ShadowLaneTickHandler` that forwards into `ShadowCaptureCoordinator`'s
/// actor-isolated `tickLaneB()`. The scheduler's protocol is deliberately
/// narrow — a one-method `Sendable` — so this adapter has nothing to own
/// besides a reference to the coordinator.
private struct ShadowLaneBAdapter: ShadowLaneTickHandler {
    let coordinator: ShadowCaptureCoordinator

    func shadowLaneBTick() async {
        await coordinator.tickLaneB()
    }
}
