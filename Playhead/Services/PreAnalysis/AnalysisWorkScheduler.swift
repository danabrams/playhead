// AnalysisWorkScheduler.swift
// Eligibility-aware job scheduler for pre-analysis work.
// Selects the highest-priority eligible job under the shared deferred-work
// admission policy and delegates execution to AnalysisJobRunner.

import Foundation
import OSLog

/// Hook the scheduler calls when admitting a `SchedulerLane.now` job so that a
/// later bead (playhead-01t8) can implement preemption of active Soon and
/// Background jobs at the next safe checkpoint boundary. This bead does not
/// implement the handler; it only defines the protocol surface so downstream
/// work can plug in without re-opening `AnalysisWorkScheduler`.
///
/// Lane vocabulary leaks across module boundaries here because preemption is
/// inherently a scheduler-internal concern; consumers that implement this
/// protocol are expected to live inside `Playhead/Services/` alongside the
/// scheduler itself. The UI-lint test (`SchedulerLaneUILintTests`) enforces
/// that restriction.
protocol LanePreemptionHandler: Sendable {
    /// Called when the scheduler is about to admit a job in the given lane.
    /// Implementations should demote (pause) active jobs in strictly lower
    /// lanes at the next safe checkpoint. No-op is a valid default.
    func preemptLowerLanes(for incoming: AnalysisWorkScheduler.SchedulerLane) async
}

/// playhead-narl.2: hook the scheduler invokes on idle ticks to let shadow
/// capture Lane B piggyback on the existing background drain cadence. The
/// handler is installed by `PlayheadRuntime` and forwards to
/// `ShadowCaptureCoordinator.tickLaneB()`. The scheduler treats the call as
/// best-effort — any error or long await is the handler's own concern, and
/// the scheduler's sleep-until-wake cadence is unchanged regardless of the
/// handler's return.
///
/// Why idle-tick vs. injecting an `AnalysisJob`: shadow capture is not a
/// unit of user-visible work — it has no `episodeId`, no coverage target,
/// and no JobRunner-compatible execution surface. Shoehorning it into the
/// job table would pollute the work journal and admission gates with
/// non-job rows. An idle-tick hook keeps shadow capture purely co-resident
/// and lets us remove it by nil-ing the handler without a schema touch.
protocol ShadowLaneTickHandler: Sendable {
    /// Called when the scheduler finds no dispatchable job and is about to
    /// sleep for `idlePollSeconds`. Implementations should dispatch at
    /// most one Lane-B shadow tick and return promptly — the scheduler
    /// will sleep regardless.
    func shadowLaneBTick() async
}

// `TransportStatusProviding` + `WifiTransportStatusProvider` live in
// TransportStatusProviding.swift in this directory. A live
// `NWPathMonitor`-backed provider will land in playhead-ml96.

actor AnalysisWorkScheduler {
    // MARK: - PlaybackContext + ScenePhase signals (playhead-gtt9.14)

    /// Transport-level playback state the scheduler consults when deciding
    /// whether to admit deferred work. Threaded from `PlaybackState.Status`
    /// by `PlayheadRuntime`'s status-observer loop.
    ///
    /// - `playing`: the audio decoder is actively producing frames. Deferred
    ///   pre-analysis must stand down so the shared pipeline bandwidth stays
    ///   available to the hot path.
    /// - `paused`: an episode is loaded but audio is halted. This is the
    ///   MOST aggressive mode for deferred work — device is awake, user is
    ///   engaged in the app, no OS time limit applies.
    /// - `idle`: no episode is loaded, or playback stopped entirely.
    ///
    /// `.loading` and `.failed` states from `PlaybackState.Status` are
    /// folded into `.paused` (loaded but not producing audio) so the
    /// scheduler does not have to enumerate every transport variant.
    enum PlaybackContext: Sendable, Equatable {
        case playing
        case paused
        case idle
    }

    /// Scene-phase projection consumed by the admission filter. A deliberate
    /// stripped-down mirror of SwiftUI's `ScenePhase` so the scheduler
    /// doesn't import SwiftUI and tests can drive state without an
    /// `@Environment` harness. `.inactive` (SwiftUI) folds into
    /// `.foreground` because the user is still holding the device — the
    /// scheduler's BGProcessingTask handoff only fires on a true
    /// `.background` transition.
    enum SchedulerScenePhase: Sendable, Equatable {
        case foreground
        case background
    }

    private static let coverageProgressEpsilon = 0.001
    /// Back-off applied when the scheduler decides not to run the job at
    /// the top of the queue on this pass — either the Soon-vs-Background
    /// deferred filter skipped it, the config is disabled, or the
    /// multi-resource admission gate (playhead-bnrs) rejected it. Longer
    /// than `idlePollSeconds` because in each of those cases the same job
    /// would come straight back to the top of the queue; a short sleep
    /// would produce a hot log/poll loop. A capability change or an
    /// explicit `wake()` preempts the sleep.
    private static let rejectionBackoffSeconds: UInt64 = 30
    /// Default sleep between idle scheduler passes when there's nothing to
    /// admit but no explicit reason to back off harder. Wake() preempts.
    private static let idlePollSeconds: UInt64 = 5
    /// Lease lifetime applied at acquire and at each renewal CAS. Renewal
    /// happens every `leaseRenewalIntervalSeconds`, well inside this window.
    private static let leaseExpirySeconds: TimeInterval = 300
    /// Renewal cadence for in-flight job leases. Must be < leaseExpirySeconds
    /// with margin so a missed wakeup doesn't lose the lease.
    private static let leaseRenewalIntervalSeconds: UInt64 = 120

    /// Centralized exponential-backoff for failed/retrying jobs. Doubles per
    /// attempt, capped at 1 hour. `attempt` is 1-indexed (first retry is 2 min).
    private static func exponentialBackoffSeconds(attempt: Int) -> Double {
        min(pow(2.0, Double(attempt)) * 60, 3600)
    }

    private let store: AnalysisStore
    private let jobRunner: AnalysisJobRunner
    private let capabilitiesService: any CapabilitiesProviding
    private let downloadManager: any DownloadProviding
    private let batteryProvider: any BatteryStateProviding
    /// playhead-bnrs: transport-status provider consumed by the
    /// admission gate. Defaults to `WifiTransportStatusProvider` so
    /// production behavior is unchanged until a real
    /// `NWPathMonitor`-backed provider lands in playhead-ml96.
    private let transportStatusProvider: any TransportStatusProviding
    /// playhead-c3pi: advisory cascade that tracks the readiness anchor
    /// and candidate-window ordering per episode. When nil, the
    /// scheduler operates exactly as it did before c3pi — the
    /// cascade is a side channel consumed by the Phase 2 surfaces
    /// (CoverageSummary derivation, future per-slice execution in
    /// playhead-1iq1). A production runtime supplies a real instance
    /// via `PlayheadRuntime`; tests that don't care about candidate
    /// windows can omit it.
    private let candidateWindowCascade: CandidateWindowCascade?
    private let config: PreAnalysisConfig
    /// playhead-e2vw: injectable clock for synthetic-time test harnesses.
    /// Defaults to `Date.init` so production behavior is byte-identical;
    /// the cascade-attributed proximal-readiness SLI test
    /// (`CandidateWindowCascadeProximalReadinessSLITest`) installs a
    /// `ManualClock` and drives the full enqueue → seed →
    /// selectNextDispatchableSlice → lease/timestamp pipeline through
    /// it so the recorded latencies are clock-driven rather than
    /// model-derived.
    private let clock: @Sendable () -> Date
    private let logger = Logger(subsystem: "com.playhead", category: "WorkScheduler")

    // MARK: - SchedulerLane (playhead-r835)

    /// Three-lane partition of the work queue. Derived from
    /// `AnalysisJob.priority` via the `schedulerLane` computed property on
    /// the job model. The partition is orthogonal to the existing T0/T1/T2
    /// coverage tiers — a T0 playback job can live in the Now lane, while a
    /// deep T2 backfill lives in Background.
    ///
    /// The variant names are deliberately scheduler-internal: they MUST NOT
    /// surface in UI copy, diagnostics text, or activity strings. The UI-lint
    /// test `SchedulerLaneUILintTests` enforces that prohibition by scanning
    /// every non-Services Swift source in the app target.
    ///
    /// Priority ranges (see also `AnalysisJob.schedulerLane`):
    /// - `.now` — priority >= 20 (user-initiated Play / Download promotions,
    ///   including playback T0)
    /// - `.soon` — priority 1..<20 (auto-download w/ proximity hints /
    ///   upcoming episodes)
    /// - `.background` — priority <= 0 (deferred auto-download, bulk backfill)
    enum SchedulerLane: Sendable, Equatable, CaseIterable {
        case now
        case soon
        case background
    }

    /// Per-lane concurrency caps. Bead spec:
    /// - Now:        <= 2 concurrent non-playback jobs
    /// - Soon:       <= 1 concurrent
    /// - Background: <= 1 concurrent
    /// - T0 playback jobs (`jobType == "playback"`) are EXEMPT from the Now
    ///   cap — they are always admitted when not globally paused.
    ///
    /// Concurrency accounting lives directly on the actor (`laneActive`,
    /// `canAdmit`, `didStart`, `didFinish`) rather than in a separate
    /// reference type. Actor isolation guarantees data-race safety for the
    /// mutable counter, so no `@unchecked Sendable` escape hatch is needed.
    /// The caps are compile-time constants; swap them here if the bead spec
    /// changes.
    static let nowCap = 2
    static let soonCap = 1
    static let backgroundCap = 1

    /// Admission decision the scheduler derives from the current QualityProfile
    /// and applies to every loop iteration. Consolidates thermal/battery/
    /// low-power gating into a single surface — see `QualityProfile.derive`.
    struct LaneAdmission: Sendable, Equatable {
        let qualityProfile: QualityProfile
        let policy: QualityProfile.SchedulerPolicy

        /// Whether any work at all may run. Mirrors `policy.pauseAllWork` for
        /// readability at call sites.
        var pauseAllWork: Bool { policy.pauseAllWork }

        /// Whether a deferred job of the given coverage depth is allowed
        /// under the current QualityProfile. T0 (playback) jobs are never
        /// gated here — the store selects them on the hot-path criteria;
        /// only `pauseAllWork` can stop them.
        ///
        /// A job is classified as Background when its desired coverage is at
        /// or above `t2Threshold`. Anything below that is Soon lane.
        func allowsDeferredJob(desiredCoverageSec: Double, t2Threshold: Double) -> Bool {
            if pauseAllWork { return false }
            let isBackgroundLane = desiredCoverageSec >= t2Threshold
            if isBackgroundLane {
                return policy.allowBackgroundLane
            } else {
                return policy.allowSoonLane
            }
        }

        /// Whether a job in the given `SchedulerLane` is admitted under the
        /// current QualityProfile. This is the priority-derived dual of
        /// `allowsDeferredJob(desiredCoverageSec:t2Threshold:)` — the latter
        /// gates by coverage depth, this one gates by lane.
        ///
        /// Semantics:
        /// - Any lane is blocked when `pauseAllWork` is true (critical).
        /// - `.now` is admitted unless `pauseAllWork`; it ignores the Soon
        ///   and Background gates because Now-lane jobs are user-initiated
        ///   (Play / explicit Download) and must drain promptly even in
        ///   serious thermal states.
        /// - `.soon` is admitted only when `policy.allowSoonLane` is true.
        /// - `.background` is admitted only when `policy.allowBackgroundLane`
        ///   is true.
        func allows(lane: SchedulerLane) -> Bool {
            if pauseAllWork { return false }
            switch lane {
            case .now:        return true
            case .soon:       return policy.allowSoonLane
            case .background: return policy.allowBackgroundLane
            }
        }
    }

    private var schedulerTask: Task<Void, Never>?
    private var currentRunningTask: Task<Void, Never>?
    private var currentJobId: String?
    private var currentEpisodeId: String?
    /// Episode id of the currently-loaded playback session, if any.
    /// Retained alongside `playbackContext` because several cancellation
    /// paths key on the episode identity rather than the coarse context.
    /// A `nil` value is equivalent to `playbackContext == .idle`.
    private var activePlaybackEpisodeId: String?
    /// playhead-gtt9.14: transport-level playback state, threaded from
    /// `PlaybackState.Status` by `PlayheadRuntime`. The admission filter
    /// in `runLoop()` blocks deferred work only when this is `.playing`
    /// AND `scenePhase == .foreground`. See the 4-state matrix in the
    /// `PlaybackContext` doc comment.
    private var playbackContext: PlaybackContext = .idle
    /// playhead-gtt9.14: SwiftUI scene-phase projection forwarded from
    /// `PlayheadApp`'s `.onChange(of: scenePhase)` observer. Starts at
    /// `.foreground` so a scheduler constructed mid-session (e.g. in a
    /// background runtime that hasn't received a phase signal yet) does
    /// not silently admit background work it shouldn't. The first real
    /// `.background` transition re-gates appropriately.
    private var schedulerScenePhase: SchedulerScenePhase = .foreground
    private var shouldCancelCurrentJob = false
    /// Set to `true` by the lease-renewal task when its CAS finds no
    /// matching row — i.e. orphan recovery (or another scheduler
    /// instance) has reclaimed the lease and may already have re-queued
    /// or completed the job under a new owner. When set, the run loop
    /// must skip every store write in its cleanup paths (state revert,
    /// progress update, retry/backoff, releaseLease) because those
    /// writes would clobber the new owner's bookkeeping. Reset at the
    /// start of every job so the flag never bleeds across iterations.
    private var lostOwnership = false
    /// Cause to thread into WorkJournal when the current running job is
    /// cancelled. Set by `cancelCurrentJob(cause:)`; consumed on the
    /// cancellation branch of the run loop. Resets to `nil` after each
    /// job finishes (whether cancelled or not) so a subsequent job
    /// doesn't inherit a stale cause tag.
    private var pendingCancelCause: InternalMissCause?
    private var leaseRenewalTask: Task<Void, any Error>?
    /// Optional WorkJournal recorder. When non-nil the scheduler emits
    /// a `recordFailed(..., cause:, metadataJSON:)` row on the
    /// cancellation path so causes like `.taskExpired` and
    /// `.userCancelled` land in `work_journal.cause`. Nil (default) is
    /// fine for unit tests that don't exercise the journal — the
    /// emission is a best-effort tail call after the lease release.
    private var workJournalRecorder: WorkJournalRecording = NoopWorkJournalRecorder()
    private static let maxAttemptCount = 5

    /// Per-lane running-job counter. Enforces the Now/Soon/Background
    /// concurrency caps spelled out in playhead-r835. Today the scheduler
    /// runs at most one job at a time via `currentRunningTask`, so the
    /// counter's per-lane caps are not yet the binding constraint on real
    /// execution — they are the contract the admission path uses so that
    /// later beads can fan the scheduler out to honest multi-lane
    /// concurrency without re-opening admission policy.
    ///
    /// Stored inline on the actor for data-race safety by isolation — see
    /// the `nowCap` / `soonCap` / `backgroundCap` constants above.
    private var laneActive: [SchedulerLane: Int] = [
        .now: 0,
        .soon: 0,
        .background: 0,
    ]

    /// Hook installed by downstream beads (playhead-01t8) to implement
    /// preemption of active Soon / Background jobs when a Now-lane job is
    /// admitted. Nil means "no preemption" — which is the only behavior this
    /// bead ships.
    private var preemptionHandler: (any LanePreemptionHandler)?

    /// playhead-narl.2: hook installed by `PlayheadRuntime` to let
    /// `ShadowCaptureCoordinator.tickLaneB()` piggyback on the scheduler's
    /// idle ticks. Nil means "no shadow capture" — the normal state in
    /// preview runtimes and in tests that don't wire shadow mode. When
    /// non-nil, the scheduler calls `shadowLaneBTick()` exactly before
    /// sleeping in the no-dispatchable-job branch of the run loop.
    private var shadowLaneTickHandler: (any ShadowLaneTickHandler)?

    private var wakeContinuation: AsyncStream<Void>.Continuation?
    private var wakeStream: AsyncStream<Void>

    /// Tracks OSSignposter queue-wait intervals keyed by jobId.
    private var queueWaitStates: [String: OSSignpostIntervalState] = [:]

    /// playhead-i9dj: stash episode titles observed at `enqueue(...)` time
    /// so `resolveAnalysisAssetId` can populate `episodeTitle` on the
    /// `analysis_assets` row at first insert.
    ///
    /// Without this seam the very first enqueue would lose the title
    /// (the asset row does not yet exist, so `updateAssetEpisodeTitle`
    /// finds nothing to update) and the column would only be populated
    /// on a later observation. Subsequent enqueues that include the
    /// title overwrite the entry; missing titles are no-ops.
    ///
    /// The dictionary is cleared per-episode at materialization time;
    /// it is purely best-effort and never blocks enqueue or processing.
    private var pendingEpisodeTitles: [String: String] = [:]

    init(
        store: AnalysisStore,
        jobRunner: AnalysisJobRunner,
        capabilitiesService: any CapabilitiesProviding,
        downloadManager: any DownloadProviding,
        batteryProvider: any BatteryStateProviding = UIDeviceBatteryProvider(),
        transportStatusProvider: any TransportStatusProviding = WifiTransportStatusProvider(),
        candidateWindowCascade: CandidateWindowCascade? = nil,
        config: PreAnalysisConfig = .load(),
        clock: @escaping @Sendable () -> Date = { Date() }
    ) {
        self.store = store
        self.jobRunner = jobRunner
        self.capabilitiesService = capabilitiesService
        self.downloadManager = downloadManager
        self.batteryProvider = batteryProvider
        self.transportStatusProvider = transportStatusProvider
        self.candidateWindowCascade = candidateWindowCascade
        self.config = config
        self.clock = clock
        var continuation: AsyncStream<Void>.Continuation?
        self.wakeStream = AsyncStream<Void> { continuation = $0 }
        self.wakeContinuation = continuation
    }

    // MARK: - Public API

    /// Enqueue a new pre-analysis job for an episode.
    /// Explicit downloads get priority=10, auto-downloads get priority=0.
    ///
    /// playhead-i9dj: `podcastTitle` and `episodeTitle` (when supplied)
    /// are persisted on the AnalysisStore rows immediately so an
    /// exported analysis.sqlite is legible without joining to the
    /// SwiftData side. Both fields are optional — if nil, AnalysisStore
    /// title columns are left untouched (the `nil`-write contract on
    /// `updateAssetEpisodeTitle` / `updateProfileTitle` is a no-op, not a
    /// NULL overwrite).
    func enqueue(
        episodeId: String,
        podcastId: String?,
        downloadId: String,
        sourceFingerprint: String,
        isExplicitDownload: Bool,
        desiredCoverage: Double? = nil,
        podcastTitle: String? = nil,
        episodeTitle: String? = nil
    ) async {
        let priority = isExplicitDownload ? 10 : 0
        let coverage = desiredCoverage ?? config.defaultT0DepthSeconds
        let workKey = AnalysisJob.computeWorkKey(
            fingerprint: sourceFingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: "preAnalysis"
        )
        let now = clock().timeIntervalSince1970
        let job = AnalysisJob(
            jobId: UUID().uuidString,
            jobType: "preAnalysis",
            episodeId: episodeId,
            podcastId: podcastId,
            analysisAssetId: nil,
            workKey: workKey,
            sourceFingerprint: sourceFingerprint,
            downloadId: downloadId,
            priority: priority,
            desiredCoverageSec: coverage,
            featureCoverageSec: 0,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0,
            state: "queued",
            attemptCount: 0,
            nextEligibleAt: nil,
            leaseOwner: nil,
            leaseExpiresAt: nil,
            lastErrorCode: nil,
            createdAt: now,
            updatedAt: now
        )
        do {
            try await store.insertJob(job)
            queueWaitStates[job.jobId] = PreAnalysisInstrumentation.beginQueueWait(jobId: job.jobId)
            logger.info("Enqueued job \(job.jobId) for episode \(episodeId), priority=\(priority), coverage=\(coverage)s")
        } catch {
            logger.error("Failed to enqueue job: \(error)")
        }

        // playhead-i9dj: write self-describing titles to the
        // AnalysisStore as soon as the SwiftData side has them in scope.
        // Both writes are best-effort — a SQL hiccup must not block the
        // download / analysis pipeline. The setters are nil-safe (a nil
        // title is a no-op, never a NULL overwrite), so call sites that
        // partially populate (e.g. only `podcastTitle`) work too.
        if let podcastTitle, let podcastId {
            do {
                try await store.updateProfileTitle(podcastId: podcastId, title: podcastTitle)
            } catch {
                logger.warning("Failed to persist podcast title for \(podcastId): \(error)")
            }
        }
        if let episodeTitle {
            // The asset row may not exist yet (it's created lazily by
            // `resolveAnalysisAssetId` at job execution time). Look it
            // up by episodeId and write opportunistically.
            do {
                if let asset = try await store.fetchAssetByEpisodeId(episodeId) {
                    try await store.updateAssetEpisodeTitle(id: asset.id, episodeTitle: episodeTitle)
                }
            } catch {
                logger.warning("Failed to persist episode title for \(episodeId): \(error)")
            }
        }

        // playhead-i9dj: stash the titles for `resolveAnalysisAssetId`
        // to consume when it materializes the analysis_assets row at
        // job execution time. Without this seam, the very first enqueue
        // (asset row does not yet exist) would lose the episodeTitle
        // until the second observation rewrites it.
        if let episodeTitle {
            pendingEpisodeTitles[episodeId] = episodeTitle
        }

        wakeSchedulerLoop()
    }

    /// playhead-c3pi: seed the candidate-window cascade for an episode.
    /// Call this after `enqueue(...)` once the metadata + chapter
    /// evidence have been parsed (typically from the download
    /// completion path). When no cascade was injected at construction
    /// the call is a no-op and returns an empty window list.
    ///
    /// - Returns: The ordered candidate windows the cascade now
    ///   associates with this episode (empty when no cascade is wired).
    @discardableResult
    func seedCandidateWindows(
        episodeId: String,
        episodeDuration: TimeInterval?,
        playbackAnchor: TimeInterval?,
        chapterEvidence: [ChapterEvidence]
    ) async -> [CandidateWindow] {
        guard let cascade = candidateWindowCascade else { return [] }
        return await cascade.seed(
            episodeId: episodeId,
            episodeDuration: episodeDuration,
            playbackAnchor: playbackAnchor,
            chapterEvidence: chapterEvidence
        )
    }

    /// playhead-c3pi: notify the scheduler of a committed playhead
    /// update for an episode. Invoked from the playback service /
    /// `PlayheadApp.persistPlaybackPosition` so the cascade can re-latch
    /// when the user seeks more than `seekRelatchThresholdSeconds` away
    /// from the prior anchor.
    ///
    /// playhead-swws: `chapterEvidence` is optional. The cascade caches
    /// the evidence captured at the most recent `seedCandidateWindows`
    /// call; commit-point callers (which don't carry chapter evidence
    /// in scope) should pass `nil` so cached sponsor-chapter windows
    /// survive the re-latch instead of being erased on every seek.
    /// Pass an explicit array only when fresh evidence is available
    /// (e.g. after a metadata reparse).
    ///
    /// - Returns: The new candidate-window order on a re-latch, or
    ///   `nil` when the delta did not exceed the threshold (no
    ///   re-latch). When no cascade was injected, always returns nil.
    @discardableResult
    func noteCommittedPlayhead(
        episodeId: String,
        newPosition: TimeInterval,
        episodeDuration: TimeInterval?,
        chapterEvidence: [ChapterEvidence]? = nil
    ) async -> [CandidateWindow]? {
        guard let cascade = candidateWindowCascade else { return nil }
        return await cascade.noteSeek(
            episodeId: episodeId,
            newPosition: newPosition,
            episodeDuration: episodeDuration,
            chapterEvidence: chapterEvidence
        )
    }

    /// playhead-c3pi: read-only accessor for the current candidate
    /// windows associated with an episode. Surfaces / SLI emitters use
    /// this to report the planned cascade order without standing up a
    /// fresh selector. Returns an empty array when no cascade is
    /// wired or the episode is unknown to the cascade.
    func currentCandidateWindows(for episodeId: String) async -> [CandidateWindow] {
        guard let cascade = candidateWindowCascade else { return [] }
        return await cascade.currentWindows(for: episodeId) ?? []
    }

    /// playhead-swws: select the next slice the scheduler would
    /// dispatch on the next loop iteration WITHOUT mutating any state
    /// (no lease acquisition, no state transitions, no signposts).
    /// Returns the chosen job paired with the cascade's first candidate
    /// window for that job's episode.
    ///
    /// This is the production selector consumed by `runLoop()`. The
    /// loop calls `selectNextDispatchableJob(...)` (the value-bearing
    /// inner helper); this `DispatchableSlice` form exists for the
    /// swws ordering test which asserts on the same selector that the
    /// production loop uses — there is no longer a test-only seam.
    ///
    /// Selection rule:
    ///
    ///   1. Fetch the FIFO winner from the store
    ///      (`priority DESC, createdAt ASC` via
    ///      `fetchNextEligibleJob`). This preserves the existing job
    ///      contract for the long tail of unseeded episodes.
    ///   2. If the candidate-window cascade is wired AND has at least
    ///      one seeded episode, scan the eligible-state rows
    ///      (queued / paused / failed) and pick the highest
    ///      cascade-priority candidate. Sponsor-chapter > proximal >
    ///      no cascade window; ties fall back to the same FIFO order
    ///      the store would have applied (priority DESC, createdAt
    ///      ASC).
    ///   3. With no cascade seeds, return the FIFO winner unchanged —
    ///      no re-scan, no extra store work.
    ///
    /// Returns `nil` when no eligible job exists OR when the
    /// admission policy currently bars all work (`pauseAllWork`); the
    /// loop's per-pass back-off behavior still applies, this accessor
    /// just reports the absence of a dispatchable slice without
    /// taking the standard sleep.
    func selectNextDispatchableSlice() async -> DispatchableSlice? {
        guard config.isEnabled else { return nil }
        let admission = await currentLaneAdmission()
        guard !admission.pauseAllWork else { return nil }

        let deferredWorkAllowed = admission.policy.allowSoonLane
            || admission.policy.allowBackgroundLane
        let now = clock().timeIntervalSince1970

        guard let selected = await selectNextDispatchableJob(
            deferredWorkAllowed: deferredWorkAllowed,
            now: now
        ) else { return nil }

        return DispatchableSlice(
            jobId: selected.job.jobId,
            episodeId: selected.job.episodeId,
            cascadeWindow: selected.cascadeWindow
        )
    }

    /// Inner cascade-aware selector used by both `runLoop()` (the
    /// production dispatch path) and `selectNextDispatchableSlice()`
    /// (the test-facing peek). Encodes the cascade-overrides-FIFO
    /// rule documented on `selectNextDispatchableSlice`. Returns
    /// `nil` when no eligible job exists.
    ///
    /// Implementation detail: the FIFO winner is always fetched first
    /// because (a) it is the cheapest single-row store call and (b)
    /// it is the answer for every iteration where the cascade is
    /// either unwired or has no seeded episodes. Only when the
    /// cascade is wired AND has seeds do we pay for the
    /// `fetchJobsByState` scan + Swift-side eligibility filter.
    private func selectNextDispatchableJob(
        deferredWorkAllowed: Bool,
        now: TimeInterval
    ) async -> (job: AnalysisJob, cascadeWindow: CandidateWindow?)? {
        // 1. FIFO winner. This is the legacy contract — preserved as
        // the answer for every code path where the cascade has
        // nothing to say.
        guard let fifoJob = try? await store.fetchNextEligibleJob(
            deferredWorkAllowed: deferredWorkAllowed,
            t0ThresholdSec: config.defaultT0DepthSeconds,
            now: now
        ) else { return nil }

        guard let cascade = candidateWindowCascade else {
            return (fifoJob, nil)
        }

        let seededIds = await cascade.seededEpisodeIds()
        // No seeded episodes ⇒ cascade has no preference; FIFO wins
        // and the cascade window is `nil` (the FIFO winner is some
        // unseeded episode). Skip the rescan for the steady-state
        // "no Phase 2 episodes seeded yet" hot path.
        guard !seededIds.isEmpty else {
            return (fifoJob, nil)
        }

        let fifoCascadeWindow = (await cascade.currentWindows(for: fifoJob.episodeId))?.first

        // 2. Gather candidate eligible rows. Only scan the three
        // states `fetchNextEligibleJob` itself selects from
        // (queued / paused / failed); other states are not
        // eligible. Apply the same eligibility predicate in Swift.
        let candidates = await gatherCascadeRescanCandidates(
            deferredWorkAllowed: deferredWorkAllowed,
            now: now
        )

        // 3. Score each candidate by cascade priority. Sponsor >
        // proximal > none. Higher score wins.
        struct Scored {
            let job: AnalysisJob
            let cascadeWindow: CandidateWindow?
            let cascadePriority: Int
        }
        var scored: [Scored] = []
        scored.reserveCapacity(candidates.count)
        for candidate in candidates {
            let window: CandidateWindow?
            if seededIds.contains(candidate.episodeId) {
                window = (await cascade.currentWindows(for: candidate.episodeId))?.first
            } else {
                window = nil
            }
            scored.append(
                Scored(
                    job: candidate,
                    cascadeWindow: window,
                    cascadePriority: cascadePriorityRank(window)
                )
            )
        }

        let fifoPriority = cascadePriorityRank(fifoCascadeWindow)
        guard let best = scored.max(by: { lhs, rhs in
            // Higher cascadePriority wins. Tiebreak using
            // `priority DESC, createdAt ASC` so we preserve the
            // store's FIFO ordering inside an equal cascade tier.
            if lhs.cascadePriority != rhs.cascadePriority {
                return lhs.cascadePriority < rhs.cascadePriority
            }
            if lhs.job.priority != rhs.job.priority {
                return lhs.job.priority < rhs.job.priority
            }
            return lhs.job.createdAt > rhs.job.createdAt
        }) else {
            return (fifoJob, fifoCascadeWindow)
        }

        // Cascade-aware override only fires when the best candidate
        // genuinely outranks the FIFO winner. If the cascade has
        // nothing to add (best matches FIFO tier), keep the FIFO
        // winner so we do not invent reordering churn for ties.
        if best.cascadePriority > fifoPriority {
            logger.info(
                "Cascade override: dispatching job \(best.job.jobId) episode=\(best.job.episodeId) cascadePriority=\(best.cascadePriority) over FIFO winner \(fifoJob.jobId) episode=\(fifoJob.episodeId) cascadePriority=\(fifoPriority)"
            )
            return (best.job, best.cascadeWindow)
        }
        return (fifoJob, fifoCascadeWindow)
    }

    /// playhead-swws: rank a cascade window by dispatch priority.
    /// Higher number wins. Sponsor-chapter (high-confidence positive)
    /// outranks proximal (default unplayed depth) outranks no
    /// cascade window at all (unseeded episode → legacy FIFO).
    private func cascadePriorityRank(_ window: CandidateWindow?) -> Int {
        guard let window else { return 0 }
        switch window.kind {
        case .sponsorChapter: return 2
        case .proximal:       return 1
        }
    }

    /// playhead-swws: collect the eligible-state job rows that the
    /// cascade-aware selector should consider re-ordering, applying
    /// the same Swift-side predicate `fetchNextEligibleJob` encodes
    /// in SQL (states queued/paused/failed; lease expired or
    /// absent; nextEligibleAt due; deferredWorkAllowed gate). This
    /// helper does NOT change SQL — it composes existing
    /// `fetchJobsByState` calls and replays the eligibility check
    /// in Swift so the cascade can pick among the same candidate
    /// set the store's FIFO query would have considered.
    private func gatherCascadeRescanCandidates(
        deferredWorkAllowed: Bool,
        now: TimeInterval
    ) async -> [AnalysisJob] {
        var collected: [AnalysisJob] = []
        let states = ["queued", "paused", "failed"]
        for state in states {
            guard let rows = try? await store.fetchJobsByState(state) else { continue }
            for job in rows {
                guard isEligibleForDispatch(
                    job: job,
                    deferredWorkAllowed: deferredWorkAllowed,
                    now: now
                ) else { continue }
                collected.append(job)
            }
        }
        return collected
    }

    /// playhead-swws: Swift-side eligibility predicate that mirrors
    /// the SQL `fetchNextEligibleJob` uses. Kept in lock step with
    /// the store query — any change to the SQL predicate here MUST
    /// be reflected in `AnalysisStore.fetchNextEligibleJob`. Lives
    /// on the scheduler (not the store) because adding a "fetch
    /// many eligible" SQL primitive would change the persistence
    /// surface; this Swift mirror sidesteps that.
    private func isEligibleForDispatch(
        job: AnalysisJob,
        deferredWorkAllowed: Bool,
        now: TimeInterval
    ) -> Bool {
        // State / lease / nextEligibleAt: queued|paused are eligible
        // when the lease is absent or expired AND nextEligibleAt is
        // due. failed rows require an explicit nextEligibleAt that
        // is due (and ignore the lease — failed rows have already
        // released).
        let leaseFree: Bool = {
            guard let owner = job.leaseOwner, !owner.isEmpty else { return true }
            guard let expires = job.leaseExpiresAt else { return true }
            return expires < now
        }()
        let nextEligibleDue: Bool = {
            guard let next = job.nextEligibleAt else { return true }
            return next <= now
        }()
        let stateEligible: Bool
        switch job.state {
        case "queued", "paused":
            stateEligible = leaseFree && nextEligibleDue
        case "failed":
            stateEligible = (job.nextEligibleAt != nil) && nextEligibleDue
        default:
            stateEligible = false
        }
        guard stateEligible else { return false }

        // T0 / deferred split — same as the SQL.
        let isT0Playback = job.jobType == "playback"
            && job.featureCoverageSec < config.defaultT0DepthSeconds
        let isDeferredAllowed = deferredWorkAllowed && nextEligibleDue
        return isT0Playback || isDeferredAllowed
    }

    /// Notify the scheduler that playback has started for an episode.
    /// Cancel any running pre-analysis work while the foreground hot path owns
    /// the shared analysis pipeline.
    ///
    /// Compatibility shim: sets `playbackContext = .playing` under the hood
    /// so existing call sites (PlayheadRuntime.playEpisode) need no change.
    /// Newer call sites that distinguish play vs. load should prefer
    /// `updatePlaybackContext(_:)` directly.
    func playbackStarted(episodeId: String) async {
        activePlaybackEpisodeId = episodeId
        playbackContext = .playing
        if currentRunningTask != nil {
            shouldCancelCurrentJob = true
            currentRunningTask?.cancel()
            logger.info("Playback preempted pre-analysis while episode \(episodeId) is active")
        }
        wakeSchedulerLoop()
    }

    /// Notify the scheduler that foreground playback has stopped, allowing
    /// queued deferred work to resume.
    func playbackStopped() {
        activePlaybackEpisodeId = nil
        playbackContext = .idle
        wakeSchedulerLoop()
    }

    /// playhead-gtt9.14: update the transport-level playback context. The
    /// admission filter in `runLoop` consults this together with
    /// `schedulerScenePhase` to decide whether deferred work may admit.
    ///
    /// Call from `PlayheadRuntime`'s status observer whenever
    /// `PlaybackState.Status` flips between `.playing`, `.paused`, and
    /// `.idle` (or the `.loading` / `.failed` states, which both fold
    /// into `.paused` for admission purposes). A status update that
    /// doesn't change the admission state still wakes the loop — the
    /// scheduler's own back-off decides whether to reconsider immediately.
    func updatePlaybackContext(_ context: PlaybackContext) {
        let priorContext = playbackContext
        playbackContext = context
        if context == .idle {
            activePlaybackEpisodeId = nil
        }
        // Wake only on a genuine transition so the idle poll loop doesn't
        // get hammered by coalesced status ticks (observeStates fires at
        // AVPlayer's periodic-time cadence — many ticks per second).
        if priorContext != context {
            wakeSchedulerLoop()
        }
    }

    /// playhead-gtt9.14: update the scene-phase projection. Forwarded by
    /// `PlayheadApp.onChange(of: scenePhase)` on the main actor. Foreground
    /// → background transitions preempt the idle poll so the filter
    /// re-evaluates on the next iteration.
    func updateScenePhase(_ phase: SchedulerScenePhase) {
        let priorPhase = schedulerScenePhase
        schedulerScenePhase = phase
        if priorPhase != phase {
            wakeSchedulerLoop()
        }
    }

    #if DEBUG
    /// Test-only accessor for the current playback context.
    func playbackContextForTesting() -> PlaybackContext {
        playbackContext
    }

    /// Test-only accessor for the current scene phase.
    func scenePhaseForTesting() -> SchedulerScenePhase {
        schedulerScenePhase
    }

    /// Test-only projection of the admission-filter predicate. Returns
    /// `true` iff the scheduler's `runLoop` would admit deferred work on
    /// the next iteration under the current (scenePhase, playbackContext,
    /// QualityProfile) triple. Thermal `.critical` (`pauseAllWork`) still
    /// dominates — this returns `false` in that case regardless of
    /// scene/playback state.
    func wouldAdmitDeferredWorkForTesting() async -> Bool {
        let admission = await currentLaneAdmission()
        if admission.pauseAllWork { return false }
        return !admissionBlocksDeferred()
    }
    #endif

    /// Decide whether the admission filter should short-circuit the run
    /// loop before reaching the store fetch. True ⇒ skip this pass and
    /// sleep. This is the 4-state matrix from playhead-gtt9.14:
    ///
    ///   (foreground, playing)  → BLOCK  (audio pipeline owns bandwidth)
    ///   (foreground, paused)   → ADMIT  (most aggressive mode)
    ///   (foreground, idle)     → ADMIT
    ///   (background, playing)  → BLOCK  (episode loaded; BPS owns window)
    ///   (background, paused)   → BLOCK
    ///   (background, idle)     → ADMIT
    ///
    /// The background row preserves the pre-gtt9.14 contract: when an
    /// episode is loaded, the scheduler defers to
    /// `BackgroundProcessingService`'s BGProcessingTask window rather than
    /// sneaking opportunistic work under the audio session.
    private func admissionBlocksDeferred() -> Bool {
        switch schedulerScenePhase {
        case .foreground:
            return playbackContext == .playing
        case .background:
            return playbackContext != .idle
        }
    }

    /// Whether the scheduler is in the "foreground-paused or foreground-idle"
    /// mode where it relaxes the thermal gate by one step so that
    /// `QualityProfile == .serious` still admits Soon-lane work. The
    /// Background lane remains gated because maintenance transfers have
    /// independent reasons (transport preference, charging heuristics)
    /// to wait for a cooler device.
    private func isForegroundAggressiveMode() -> Bool {
        schedulerScenePhase == .foreground && playbackContext != .playing
    }

    // MARK: - SchedulerStateSnapshotProviding (playhead-gtt9.14)

    /// Current scheduler-state snapshot — the triple the lifecycle
    /// logger records at every session-state transition. Safe to call
    /// from any isolation domain thanks to actor-hop on `await`. A
    /// `critical` thermal read is still reported as the dominant
    /// profile — callers use the tuple for bucketing, not for policy
    /// decisions.
    func currentSchedulerStateSnapshot() async -> SchedulerStateSnapshot {
        let sceneString: String = {
            switch schedulerScenePhase {
            case .foreground: return "foreground"
            case .background: return "background"
            }
        }()
        let contextString: String = {
            switch playbackContext {
            case .playing: return "playing"
            case .paused:  return "paused"
            case .idle:    return "idle"
            }
        }()
        let admission = await currentLaneAdmission()
        return SchedulerStateSnapshot(
            scenePhase: sceneString,
            playbackContext: contextString,
            qualityProfile: admission.qualityProfile.rawValue
        )
    }

    /// Mark jobs for a deleted episode as superseded.
    func episodeDeleted(episodeId: String) async {
        if currentEpisodeId == episodeId {
            shouldCancelCurrentJob = true
            currentRunningTask?.cancel()
        }
        do {
            let states = ["queued", "paused", "running", "failed",
                          "blocked:missingFile", "blocked:modelUnavailable"]
            for state in states {
                let jobs = try await store.fetchJobsByState(state)
                for job in jobs where job.episodeId == episodeId {
                    try await store.updateJobState(jobId: job.jobId, state: "superseded")
                }
            }
        } catch {
            logger.error("Failed to supersede jobs for deleted episode \(episodeId): \(error)")
        }
        // playhead-c3pi: drop the cascade entry so a re-subscribe to
        // the same episode does not inherit a stale anchor.
        await candidateWindowCascade?.forget(episodeId: episodeId)
    }

    /// Cancel the currently executing analysis job.
    ///
    /// `cause` is the `InternalMissCause` that the scheduler will emit
    /// on the cancellation branch of the run loop (via the injected
    /// `WorkJournalRecording` recorder). The default is
    /// `.pipelineError` so existing callers that don't know a better
    /// cause still produce a typed cause tag rather than `nil`.
    /// `.taskExpired` is passed from `BackgroundProcessingService`'s
    /// expirationHandler; `.userCancelled` is the explicit-cancel
    /// entry point.
    func cancelCurrentJob(cause: InternalMissCause = .pipelineError) {
        shouldCancelCurrentJob = true
        // Concurrent cancels with different causes must not stomp each
        // other with last-writer-wins. Route through
        // `CauseAttributionPolicy.primaryCause` so precedence (e.g.
        // `.userCancelled` outranks `.taskExpired`) is honored regardless
        // of arrival order. Context values are conservative defaults
        // that keep the tier ranking of the causes the cancel path uses
        // (`.userCancelled`, `.userPreempted`, `.taskExpired`,
        // `.pipelineError`) stable — retryBudgetRemaining only matters
        // for `.taskExpired`, and both tiers (environmentalTransient /
        // resourceExhausted) still lose to `.userInitiated`.
        let resolved: InternalMissCause
        if let existing = pendingCancelCause, existing != cause {
            let context = CauseAttributionContext(
                modelAvailableNow: true,
                retryBudgetRemaining: 0
            )
            resolved = CauseAttributionPolicy.primaryCause(
                among: [existing, cause],
                context: context
            ) ?? cause
        } else {
            resolved = cause
        }
        pendingCancelCause = resolved
        currentRunningTask?.cancel()
    }

    #if DEBUG
    /// Test-only accessor for the `pendingCancelCause` field so unit
    /// tests can verify `cancelCurrentJob(cause:)` precedence without
    /// having to run the full scheduler loop. Do not wire into
    /// production code — the cause is consumed on the cancellation
    /// branch of `runOneIteration` and should not be observed
    /// externally.
    func pendingCancelCauseForTesting() -> InternalMissCause? {
        pendingCancelCause
    }
    #endif

    /// Install the WorkJournal recorder the scheduler uses on the
    /// cancellation branch. Optional — tests that don't need the
    /// journal can leave the default `NoopWorkJournalRecorder` in
    /// place. Called from `PlayheadRuntime` once the real recorder is
    /// available.
    func setWorkJournalRecorder(_ recorder: WorkJournalRecording) {
        self.workJournalRecorder = recorder
    }

    /// Wake the scheduler loop externally. Used by BackgroundProcessingService
    /// when a BGProcessingTask window opens so the loop immediately polls for
    /// eligible jobs instead of waiting out its current sleep interval.
    ///
    /// This is the explicit public handle for the private `wakeSchedulerLoop`
    /// signal and makes the BPS→WorkScheduler dependency visible at the call
    /// site.
    func wake() {
        wakeSchedulerLoop()
    }

    /// Install a lane-preemption handler. This bead (playhead-r835) only
    /// defines the protocol surface — it installs no default handler. A
    /// later bead (playhead-01t8) will wire an implementation that pauses
    /// active Soon / Background jobs at their next safe checkpoint when the
    /// scheduler admits a Now-lane job.
    func setLanePreemptionHandler(_ handler: (any LanePreemptionHandler)?) {
        self.preemptionHandler = handler
    }

    /// playhead-narl.2: install the shadow Lane B tick handler. Pass `nil`
    /// to detach. Idempotent — re-installing replaces the prior handler.
    func setShadowLaneTickHandler(_ handler: (any ShadowLaneTickHandler)?) {
        self.shadowLaneTickHandler = handler
    }

    /// Start the scheduler loop. Call after reconciliation is complete.
    func startSchedulerLoop() {
        schedulerTask?.cancel()
        schedulerTask = Task { [weak self] in
            guard let self else { return }
            await self.runLoop()
        }
    }

    /// Stop the scheduler loop and any running job.
    func stop() {
        schedulerTask?.cancel()
        schedulerTask = nil
        currentRunningTask?.cancel()
        currentRunningTask = nil
        leaseRenewalTask?.cancel()
        leaseRenewalTask = nil
    }

    // MARK: - Scheduler Loop

    private func runLoop() async {
        while !Task.isCancelled {
            guard config.isEnabled else {
                await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
                continue
            }

            // playhead-gtt9.14: 4-state admission filter over
            // (scenePhase, playbackContext). Prior to gtt9.14 the
            // scheduler blocked deferred work whenever an episode was
            // loaded — treating foreground-paused the same as
            // foreground-playing, which is the opposite of what the
            // device's capability envelope suggests. See
            // `admissionBlocksDeferred()` for the full matrix.
            if admissionBlocksDeferred() {
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            let admission = await currentLaneAdmission()

            // Critical thermal (or equivalent) pauses every lane, including
            // T0 playback drains. Wait for the next wake/capability change.
            if admission.pauseAllWork {
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            // `deferredWorkAllowed` gates the store's deferred (T1+) selection.
            // Critical cases where T2 is paused but Soon is allowed are handled
            // after fetch via `admission.allowsDeferredJob`, because the store
            // predicate only distinguishes T0 vs. deferred, not Soon vs.
            // Background.
            let deferredWorkAllowed = admission.policy.allowSoonLane
                || admission.policy.allowBackgroundLane
            let now = clock().timeIntervalSince1970

            // playhead-swws: cascade-aware job selection. When the
            // candidate-window cascade has at least one seeded
            // episode, prefer the queued job whose episode has the
            // highest-priority cascade window (sponsor > proximal)
            // over the strict FIFO winner from
            // `fetchNextEligibleJob`. Falls back to FIFO when the
            // cascade is unwired, has no seeds, or has nothing to
            // re-order.
            guard let selected = await selectNextDispatchableJob(
                deferredWorkAllowed: deferredWorkAllowed,
                now: now
            ) else {
                // playhead-narl.2: no dispatchable job → the scheduler is
                // genuinely idle. Give the shadow Lane B coordinator a
                // chance to fire one tick before we sleep. The handler's
                // own gate (thermal + charging + kill switch) determines
                // whether the tick does any work; the scheduler treats the
                // call as fire-and-forget and always sleeps afterward.
                //
                // Genuinely fire-and-forget (don't await): with
                // `laneBCallsPerTick = 2` default, a single idle-tick can
                // issue up to 2 sequential FM calls (~6s). Awaiting would
                // delay the T0 job start when the user hits play
                // mid-Lane-B tick. Capture the handler by value so the
                // detached task does not close over the scheduler actor.
                if let shadowLaneTickHandler {
                    Task { [shadowLaneTickHandler] in
                        await shadowLaneTickHandler.shadowLaneBTick()
                    }
                }
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }
            let job = selected.job
            let dispatchedCascadeWindow = selected.cascadeWindow

            // Secondary filter for the Soon-vs-Background lane split. Only
            // deferred jobs are subject to this; T0 playback jobs are always
            // admitted when not paused-all (checked above). We back off
            // longer here (30s) rather than the default 5s because the store
            // predicate can't express "Soon only," so the same Background
            // job will come back to the top of the queue on every re-fetch —
            // a short sleep would produce a hot log/poll loop. A capability
            // change or an explicit wake() will preempt the sleep.
            if job.jobType != "playback",
               !admission.allowsDeferredJob(
                    desiredCoverageSec: job.desiredCoverageSec,
                    t2Threshold: config.t2DepthSeconds
               ) {
                logger.info("Skipping job \(job.jobId) (depth=\(job.desiredCoverageSec)s) under QualityProfile \(admission.qualityProfile.rawValue, privacy: .public)")
                await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
                continue
            }

            // Per-lane concurrency cap (playhead-r835). T0 playback jobs are
            // exempt from the Now cap; `canAdmit` encodes that rule. If the
            // cap is saturated, fall back to the standard sleep so we do not
            // re-fetch the same job in a tight loop.
            guard canAdmit(job: job) else {
                logger.info("Skipping job \(job.jobId) — lane \(String(describing: job.schedulerLane), privacy: .public) at capacity")
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            // Multi-resource admission gate (playhead-bnrs). Consults
            // thermal / transport / storage / CPU axes; a hard rejection
            // here skips this pass with a logged cause. On the reject
            // path we do NOT mutate job state (no `updateJobState` /
            // WorkJournal write) — the scheduler will re-fetch the job
            // on the next pass once the failing axis clears (a
            // capability change wakes the loop). This preserves the
            // retry semantics that existed before the gate was wired.
            let gateDecision = await evaluateAdmissionGate(for: job)
            switch gateDecision {
            case .reject(let cause):
                logger.info("AdmissionGate rejected job \(job.jobId) episode=\(job.episodeId) lane=\(String(describing: job.schedulerLane), privacy: .public) cause=\(cause.rawValue, privacy: .public)")
                await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
                continue
            case .admit(let sliceBytes):
                // Audit trail: record the gate's computed slice budget
                // so rejection-vs-admission traces have symmetric log
                // coverage. Nothing downstream consumes sliceBytes yet
                // because the execution unit is still the full job, not
                // a slice.
                // TODO(playhead-1iq1): plumb sliceBytes into AnalysisRangeRequest once per-slice execution is the scheduling unit
                logger.info("AdmissionGate admitted job \(job.jobId) episode=\(job.episodeId) lane=\(String(describing: job.schedulerLane), privacy: .public) sliceBytes=\(sliceBytes)")
            }

            // Now-lane admission demotes active Soon / Background jobs at
            // their next safe checkpoint. The protocol is owned by
            // playhead-01t8; this bead only wires the hook. Pass the job's
            // lane rather than a hardcoded `.now` so that if the guard above
            // ever widens (e.g. Soon-on-Background preemption), the call
            // site does not silently misreport the incoming lane.
            if job.schedulerLane == .now, let preempt = preemptionHandler {
                await preempt.preemptLowerLanes(for: job.schedulerLane)
            }

            didStart(job: job)
            await processJob(job, cascadeWindow: dispatchedCascadeWindow)
            didFinish(job: job)
        }
    }

    // MARK: - Lane concurrency accounting (playhead-r835)

    /// Current running-job count in `lane`. Exposed for instrumentation and
    /// tests; the scheduler loop uses `canAdmit` / `didStart` / `didFinish`
    /// directly.
    func laneActiveCount(_ lane: SchedulerLane) -> Int {
        laneActive[lane] ?? 0
    }

    /// playhead-quh7: episode id for the job currently held by the
    /// scheduler's run loop, if any. Read-only accessor consumed by
    /// `LiveActivitySnapshotProvider` to drive the Now-vs-Up-Next
    /// split for `disposition == .queued` rows. `nil` when the loop
    /// is idle or between admissions.
    func currentlyRunningEpisodeId() -> String? {
        currentEpisodeId
    }

    /// Whether `job` may be admitted under the current per-lane count. T0
    /// playback jobs (`jobType == "playback"`) bypass the Now cap
    /// unconditionally — the hot-path must always be able to drain.
    func canAdmit(job: AnalysisJob) -> Bool {
        let lane = job.schedulerLane
        if lane == .now && job.jobType == "playback" {
            return true
        }
        let cap: Int
        switch lane {
        case .now:        cap = Self.nowCap
        case .soon:       cap = Self.soonCap
        case .background: cap = Self.backgroundCap
        }
        return laneActiveCount(lane) < cap
    }

    /// Record that `job` has started running in its lane.
    func didStart(job: AnalysisJob) {
        let lane = job.schedulerLane
        laneActive[lane, default: 0] += 1
        Self.postActivityRefreshNotification()
    }

    /// Record that `job` has finished running in its lane. Clamped at zero
    /// so a stray double-finish does not produce negative counts.
    func didFinish(job: AnalysisJob) {
        let lane = job.schedulerLane
        let current = laneActive[lane, default: 0]
        laneActive[lane] = max(0, current - 1)
        Self.postActivityRefreshNotification()
    }

    /// playhead-quh7: notify the Activity screen to re-aggregate its
    /// snapshot. Posted from the two scheduler-state edges that flip
    /// the section bucketing (a job moving from queued → running, and
    /// a job moving from running → terminal). The Activity view
    /// observes this notification as its sole refresh trigger; without
    /// it the view would have to poll on a Timer, which the bead spec
    /// explicitly forbids.
    ///
    /// `nonisolated` so the call site inside the actor's isolated
    /// methods does not need to hop off the actor — `NotificationCenter`
    /// is thread-safe.
    nonisolated static func postActivityRefreshNotification() {
        NotificationCenter.default.post(
            name: ActivityRefreshNotification.name,
            object: nil
        )
    }

    /// Evaluate the full multi-resource admission gate for `job`. Returns
    /// the `GateAdmissionDecision` the scheduler will act on: `.admit` means
    /// the caller may proceed to `processJob(_:)`, `.reject(cause)` means
    /// the scheduler must skip this pass and log the cause.
    ///
    /// playhead-bnrs: this is the production consumer of
    /// `AdmissionGate.admit(...)`. It stitches together the four gate
    /// inputs:
    ///
    /// - `profile`: derived from the capabilities snapshot + live
    ///   battery, same source as `currentLaneAdmission()`.
    /// - `deviceClass` / `deviceProfile`: from the snapshot + the
    ///   playhead-dh9b hard-coded fallback table (the JSON manifest
    ///   loader is not plumbed here — slice-sizing uses the fallback row
    ///   until a loader is injected).
    /// - `transport`: synthesized from `transportStatusProvider`
    ///   (defaults to `WifiTransportStatusProvider`) and the job's lane.
    ///   Background-lane jobs map to `.maintenance` (Wi-Fi only); every
    ///   other lane maps to `.interactive`.
    /// - `storage`: plentiful-default snapshot — the per-class cap check
    ///   is still performed by `StorageBudget.admit` at write time; this
    ///   gate's role in the bnrs wire is to ensure the transport / CPU /
    ///   thermal axes are consulted at admission. Plumbing a live
    ///   `StorageBudget` snapshot into the scheduler is playhead-1iq1.
    func evaluateAdmissionGate(for job: AnalysisJob) async -> GateAdmissionDecision {
        let snapshot = await capabilitiesService.currentSnapshot
        let batteryState = await batteryProvider.currentBatteryState()
        let profile = snapshot.qualityProfile(
            batteryLevel: batteryState.level,
            isCharging: batteryState.isCharging
        )
        let deviceClass = snapshot.deviceClass
        let deviceProfile = DeviceClassProfile.fallback(for: deviceClass)

        let reachability = await transportStatusProvider.currentReachability()
        let allowsCellular = await transportStatusProvider.userAllowsCellular()
        // Background-lane jobs are maintenance transfers (auto-download
        // / bulk backfill). Everything else is interactive — user-
        // initiated Play / explicit Download (Now), or a proximate
        // upcoming-episode preload (Soon). This mirrors the
        // BackgroundSessionIdentifier split in closed bead playhead-24cm.
        let session: TransportSnapshot.Session = (job.schedulerLane == .background)
            ? .maintenance
            : .interactive
        let transport = TransportSnapshot(
            reachability: reachability,
            session: session,
            userAllowsCellular: allowsCellular
        )

        // Storage snapshot: plentiful by construction for this bead.
        // The per-class admission check is performed by
        // `StorageBudget.admit(class:sizeBytes:)` at the actual write
        // site; this gate's contribution is to keep transport/CPU/
        // thermal consulted at scheduling time. playhead-1iq1 will
        // inject a live StorageBudget reference and replace this with a
        // real snapshot.
        let storage = StorageSnapshot.plentiful

        let admissionJob = AdmissionJob(
            artifactClasses: [job.artifactClass],
            estimatedWriteBytes: max(0, job.estimatedWriteBytes)
        )

        return AdmissionGate.admit(
            job: admissionJob,
            profile: profile,
            deviceClass: deviceClass,
            deviceProfile: deviceProfile,
            storage: storage,
            transport: transport
        )
    }

    /// Evaluate the current `LaneAdmission` from the capabilities snapshot and
    /// a live battery reading. Exposed internally so tests (and integrators
    /// like BackgroundProcessingService) can ask what the scheduler would do
    /// right now without driving the full loop.
    ///
    /// All thermal/battery/low-power reads route through `QualityProfile` —
    /// there are no direct `ProcessInfo.thermalState` or `isLowPowerMode`
    /// reads in this actor. The thermal gate of the broader
    /// multi-resource admission policy (playhead-bnrs) is honored here
    /// via `policy.pauseAllWork`; the transport, storage, and CPU gates
    /// are consulted by `evaluateAdmissionGate(for:)` which the scheduler
    /// loop calls after `canAdmit(job:)` succeeds.
    func currentLaneAdmission() async -> LaneAdmission {
        let snapshot = await capabilitiesService.currentSnapshot
        let batteryState = await batteryProvider.currentBatteryState()
        // Route every thermal/battery/low-power read through the snapshot's
        // QualityProfile surface. The `isCharging:` overload is preferred
        // because the battery provider's charging signal is fresher than the
        // snapshot's (which only refreshes on `batteryStateDidChange`).
        let profile = snapshot.qualityProfile(
            batteryLevel: batteryState.level,
            isCharging: batteryState.isCharging
        )
        // playhead-gtt9.14: foreground-paused / foreground-idle is the
        // MOST aggressive scheduling mode — device awake, user engaged,
        // no audio producer, no OS time limit. Under `.serious` thermal
        // the baseline policy blocks both Soon and Background; the
        // relaxation opens Soon back up so deferred transcript work
        // drains while the user is looking at the app. Background lane
        // stays gated (maintenance transfers defer to a cooler device).
        // `.critical` is never relaxed — `pauseAllWork` is dominant in
        // every state.
        let effectivePolicy = relaxedPolicy(
            for: profile.schedulerPolicy,
            profile: profile,
            foregroundAggressive: isForegroundAggressiveMode()
        )
        return LaneAdmission(qualityProfile: profile, policy: effectivePolicy)
    }

    /// playhead-gtt9.14: derive the effective `SchedulerPolicy` from the
    /// baseline `QualityProfile` policy. When the scheduler is in the
    /// foreground-aggressive mode (foreground + paused/idle) and the
    /// thermal baseline is `.serious`, reopen the Soon lane. All other
    /// inputs pass through unchanged — this is not a general-purpose
    /// profile override.
    private func relaxedPolicy(
        for policy: QualityProfile.SchedulerPolicy,
        profile: QualityProfile,
        foregroundAggressive: Bool
    ) -> QualityProfile.SchedulerPolicy {
        guard foregroundAggressive, profile == .serious else { return policy }
        return QualityProfile.SchedulerPolicy(
            sliceFraction: policy.sliceFraction,
            allowSoonLane: true,
            allowBackgroundLane: policy.allowBackgroundLane,
            pauseAllWork: policy.pauseAllWork
        )
    }

    private func sleepOrWake(seconds: UInt64) async {
        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                try? await Task.sleep(for: .seconds(seconds))
            }
            group.addTask { [wakeStream] in
                var iterator = wakeStream.makeAsyncIterator()
                _ = await iterator.next()
            }
            _ = await group.next()
            group.cancelAll()
        }
    }

    private func wakeSchedulerLoop() {
        wakeContinuation?.yield()
    }

    // MARK: - Job Processing

    private func processJob(_ job: AnalysisJob, cascadeWindow: CandidateWindow? = nil) async {
        // Resolve audio URL from download cache.
        guard let fileURL = await downloadManager.cachedFileURL(for: job.episodeId) else {
            logger.warning("No cached audio for episode \(job.episodeId), blocking job \(job.jobId)")
            do {
                try await store.updateJobState(jobId: job.jobId, state: "blocked:missingFile")
            } catch {
                logger.error("Failed to update job state: \(error)")
            }
            return
        }

        guard let localAudioURL = LocalAudioURL(fileURL) else {
            logger.error("cachedFileURL returned non-file URL for episode \(job.episodeId)")
            do {
                try await store.updateJobState(jobId: job.jobId, state: "blocked:missingFile")
            } catch {
                logger.error("Failed to update job state: \(error)")
            }
            return
        }

        // Acquire lease. playhead-5uvz.1 (Gap-1): use the journal-aware
        // variant so the lease UPDATE and the `acquired` work_journal
        // row land in the SAME SQL transaction. Without this the
        // production journal stays empty and AnalysisCoordinator's
        // `recoverOrphans` (the journal-aware cold-launch reaper)
        // degrades to the same blind sweep AnalysisJobReconciler runs.
        let now = clock().timeIntervalSince1970
        let leaseExpiry = now + Self.leaseExpirySeconds
        let leaseAcquired: Bool
        do {
            leaseAcquired = try await store.acquireLeaseWithJournal(
                jobId: job.jobId,
                episodeId: job.episodeId,
                owner: "preAnalysis",
                expiresAt: leaseExpiry,
                now: now
            )
        } catch {
            // playhead-5uvz.1 NIT #3: surface the thrown error in the
            // log instead of silently coercing to `false`. Without this,
            // a sustained SQLite-side problem (disk full, locked DB,
            // schema drift) is indistinguishable from "lease already
            // taken" — the scheduler retries forever with no signal.
            logger.error("acquireLeaseWithJournal threw for job \(job.jobId): \(error)")
            leaseAcquired = false
        }

        guard leaseAcquired else {
            logger.info("Failed to acquire lease for job \(job.jobId), skipping")
            return
        }

        currentJobId = job.jobId
        currentEpisodeId = job.episodeId
        shouldCancelCurrentJob = false
        lostOwnership = false

        // End queue-wait signpost interval.
        if let queueState = queueWaitStates.removeValue(forKey: job.jobId) {
            PreAnalysisInstrumentation.endQueueWait(queueState)
        }

        // Lease renewal task. If the CAS finds no matching row, orphan
        // recovery (or another scheduler instance) has reclaimed the lease
        // — set `lostOwnership` so the cleanup paths skip every store
        // write (state revert, progress update, backoff, releaseLease)
        // that would otherwise clobber the new owner's bookkeeping, then
        // cancel the running task so the run loop unwinds promptly.
        leaseRenewalTask = Task { [clock] in
            while !Task.isCancelled {
                try await Task.sleep(for: .seconds(Self.leaseRenewalIntervalSeconds))
                let newExpiry = clock().timeIntervalSince1970 + Self.leaseExpirySeconds
                let stillOwned = (try? await self.store.renewLease(
                    jobId: job.jobId,
                    owner: "preAnalysis",
                    newExpiresAt: newExpiry
                )) ?? false
                if !stillOwned {
                    self.lostOwnership = true
                    self.currentRunningTask?.cancel()
                    break
                }
            }
        }

        defer {
            leaseRenewalTask?.cancel()
            leaseRenewalTask = nil
            currentJobId = nil
            currentEpisodeId = nil
        }

        // Build request and run.
        let assetId: String
        do {
            assetId = try await resolveAnalysisAssetId(for: job, localAudioURL: localAudioURL)
        } catch {
            logger.error("Failed to resolve analysis asset for job \(job.jobId): \(error)")
            guard !lostOwnership else {
                logger.warning("Skipping asset-resolution failure writes for job \(job.jobId): lease reclaimed by orphan recovery")
                return
            }
            let backoff = Self.exponentialBackoffSeconds(attempt: job.attemptCount + 1)
            await writeIfStillOwned("assetResolution.markFailed") {
                try await store.updateJobState(
                    jobId: job.jobId,
                    state: "failed",
                    nextEligibleAt: clock().timeIntervalSince1970 + backoff,
                    lastErrorCode: "assetResolution: \(error)"
                )
            }
            await writeIfStillOwned("assetResolution.releaseLease") {
                try await store.releaseLease(jobId: job.jobId)
            }
            return
        }
        // playhead-swws: cascade window is now resolved by the
        // production selector (`selectNextDispatchableJob`) and
        // threaded in by `runLoop()`. Callers that invoke
        // `processJob` directly (none today outside the run loop)
        // pass `nil` and inherit the legacy "process [0,
        // desiredCoverageSec]" depth-first behavior. The runner
        // (and downstream slice-execution work in playhead-1iq1)
        // will use this `windowRange` to prioritize the
        // proximal/sponsor window.
        let resolvedCascadeWindow = cascadeWindow
        let request = AnalysisRangeRequest(
            jobId: job.jobId,
            episodeId: job.episodeId,
            podcastId: job.podcastId ?? "",
            analysisAssetId: assetId,
            audioURL: localAudioURL,
            desiredCoverageSec: job.desiredCoverageSec,
            mode: .preRollWarmup,
            outputPolicy: .writeWindowsAndCues,
            priority: .medium,
            schedulerLane: job.schedulerLane,
            windowRange: resolvedCascadeWindow?.range
        )

        let jobSignpost = PreAnalysisInstrumentation.beginJobDuration(jobId: job.jobId)
        do {
            guard !shouldCancelCurrentJob else {
                PreAnalysisInstrumentation.endJobDuration(jobSignpost)
                // `acquireLease` set state='running' atomically; if we
                // skip without reverting, the job is stranded at
                // 'running' with leaseOwner=NULL — invisible to
                // `fetchNextEligibleJob` (queued|paused|failed only) and
                // to `recoverExpiredLease` (leaseOwner IS NOT NULL
                // only). Revert to 'queued' before releasing the lease.
                await writeIfStillOwned("cancelRace.revertQueued") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                await writeIfStillOwned("cancelRace.releaseLease") {
                    try await store.releaseLease(jobId: job.jobId)
                }
                // Clear cancel state so it doesn't leak into the next
                // job picked up by the loop. `pendingCancelCause` was
                // set by the racing canceller for the now-skipped job;
                // a stale value would mis-attribute the next job's
                // cancellation if one arrives.
                shouldCancelCurrentJob = false
                pendingCancelCause = nil
                return
            }

            let runTask = Task<AnalysisOutcome, Error> {
                try Task.checkCancellation()
                let result = await self.jobRunner.run(request)
                try Task.checkCancellation()
                return result
            }
            currentRunningTask = Task {
                await withTaskCancellationHandler {
                    _ = try? await runTask.value
                } onCancel: {
                    runTask.cancel()
                }
            }

            let outcome: AnalysisOutcome
            do {
                outcome = try await runTask.value
            } catch is CancellationError {
                PreAnalysisInstrumentation.endJobDuration(jobSignpost)
                if lostOwnership {
                    // Lease was reclaimed by orphan recovery. The new
                    // owner is the source of truth for state, retry
                    // count, and cause; any write here would clobber
                    // its bookkeeping. Drop the cancel cause to avoid
                    // bleeding it into the next job.
                    logger.warning("Skipping cancel cleanup writes for job \(job.jobId): lease reclaimed by orphan recovery")
                    pendingCancelCause = nil
                    return
                }
                await writeIfStillOwned("cancelCatch.revertQueued") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                await writeIfStillOwned("cancelCatch.releaseLease") {
                    try await store.releaseLease(jobId: job.jobId)
                }
                // playhead-1nl6: emit the cause that accompanied the
                // cancel into the WorkJournal via the injected recorder.
                // Default cause is `.pipelineError` so callers that
                // forgot to pass one still produce a typed tag; the
                // expirationHandler in BackgroundProcessingService
                // passes `.taskExpired`, the explicit user cancel path
                // passes `.userCancelled`.
                let cause = pendingCancelCause ?? .pipelineError
                pendingCancelCause = nil
                let recorder = workJournalRecorder
                let episodeId = job.episodeId
                let metadata = await SliceCompletionInstrumentation.recordPaused(
                    cause: cause,
                    deviceClass: DeviceClass.detect(),
                    sliceDurationMs: 0,
                    bytesProcessed: 0,
                    shardsCompleted: 0,
                    extras: [
                        "stage": "analysisWorkScheduler.cancelCurrentJob",
                        "job_id": job.jobId,
                    ]
                )
                await recorder.recordPreempted(
                    episodeId: episodeId,
                    cause: cause,
                    metadataJSON: metadata.encodeJSON()
                )
                return
            }
            currentRunningTask = nil
            // Non-cancel path: clear any stale cause so the next job
            // doesn't inherit it.
            pendingCancelCause = nil

            PreAnalysisInstrumentation.endJobDuration(jobSignpost)

            // Log outcome metric.
            PreAnalysisInstrumentation.logJobOutcome(
                jobId: job.jobId,
                stopReason: String(describing: outcome.stopReason),
                coverageSec: outcome.cueCoverageSec
            )

            // First check before the outcome write chain. Each
            // individual `store.X(...)` below is also wrapped in
            // `writeIfStillOwned` so the renewer flipping the flag at
            // any subsequent `await` suspension point does not slip a
            // late write through.
            if lostOwnership {
                logger.warning("Skipping outcome writes for job \(job.jobId): lease reclaimed by orphan recovery")
                return
            }

            // Update progress in the store.
            await writeIfStillOwned("updateJobProgress") {
                try await store.updateJobProgress(
                    jobId: job.jobId,
                    featureCoverageSec: outcome.featureCoverageSec,
                    transcriptCoverageSec: outcome.transcriptCoverageSec,
                    cueCoverageSec: outcome.cueCoverageSec
                )
            }

            // Handle outcome.
            switch outcome.stopReason {
            case .reachedTarget where outcome.cueCoverageSec >= job.desiredCoverageSec:
                if let nextCoverage = nextTierCoverage(current: job.desiredCoverageSec) {
                    let tierWorkKey = AnalysisJob.computeWorkKey(
                        fingerprint: job.sourceFingerprint,
                        analysisVersion: PreAnalysisConfig.analysisVersion,
                        jobType: "preAnalysis"
                    ) + ":\(Int(nextCoverage))"
                    let now = clock().timeIntervalSince1970
                    let nextJob = AnalysisJob(
                        jobId: UUID().uuidString,
                        jobType: "preAnalysis",
                        episodeId: job.episodeId,
                        podcastId: job.podcastId,
                        analysisAssetId: assetId,
                        workKey: tierWorkKey,
                        sourceFingerprint: job.sourceFingerprint,
                        downloadId: job.downloadId,
                        priority: 0,
                        desiredCoverageSec: nextCoverage,
                        featureCoverageSec: outcome.featureCoverageSec,
                        transcriptCoverageSec: outcome.transcriptCoverageSec,
                        cueCoverageSec: outcome.cueCoverageSec,
                        state: "paused",
                        attemptCount: 0,
                        nextEligibleAt: nil,
                        leaseOwner: nil,
                        leaseExpiresAt: nil,
                        lastErrorCode: nil,
                        createdAt: now,
                        updatedAt: now
                    )
                    await writeIfStillOwned("tierAdvance.insertNext") {
                        try await store.insertJob(nextJob)
                    }
                    await writeIfStillOwned("tierAdvance.markComplete") {
                        try await store.updateJobState(jobId: job.jobId, state: "complete")
                    }
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Tier advancement: \(job.desiredCoverageSec)s -> \(nextCoverage)s for episode \(job.episodeId)")
                } else {
                    await writeIfStillOwned("allTiersDone.markComplete") {
                        try await store.updateJobState(jobId: job.jobId, state: "complete")
                    }
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Job \(job.jobId) complete (all tiers done)")
                }

            case .reachedTarget:
                // Re-queue, but with a guard against infinite loops for short episodes
                // or episodes that can never reach the desired coverage.
                if !Self.shouldRetryCoverageInsufficient(job: job, outcome: outcome) {
                    await writeIfStillOwned("coverageInsufficient.noProgress") {
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "complete",
                            lastErrorCode: "coverageInsufficient:noProgress"
                        )
                    }
                    logger.info("Job \(job.jobId) marked complete after no-progress pass (coverage insufficient)")
                } else {
                    await writeIfStillOwned("coverageInsufficient.increment") {
                        try await store.incrementAttemptCount(jobId: job.jobId)
                    }
                    let updatedJob = lostOwnership ? nil : (try? await store.fetchJob(byId: job.jobId))
                    if (updatedJob?.attemptCount ?? 0) >= Self.maxAttemptCount {
                        await writeIfStillOwned("coverageInsufficient.maxAttempts") {
                            try await store.updateJobState(
                                jobId: job.jobId,
                                state: "complete",
                                lastErrorCode: "maxAttemptsReached:coverageInsufficient"
                            )
                        }
                        logger.info("Job \(job.jobId) marked complete after max attempts (coverage insufficient)")
                    } else {
                        // Backoff before next attempt: without a
                        // gap, the scheduler loop wakes
                        // immediately, picks the same job, and
                        // burns the full decode pipeline N more
                        // times in a tight loop. Match the
                        // `.failed` exponential backoff so
                        // attempt-N waits min(2^N * 60, 3600) s.
                        let attemptIndex = Double((updatedJob?.attemptCount ?? 1))
                        let backoff = min(pow(2.0, attemptIndex) * 60, 3600)
                        await writeIfStillOwned("coverageInsufficient.requeue") {
                            try await store.updateJobState(
                                jobId: job.jobId,
                                state: "queued",
                                nextEligibleAt: clock().timeIntervalSince1970 + backoff
                            )
                        }
                    }
                }

            case .blockedByModel:
                let nextEligible = clock().timeIntervalSince1970 + 300
                await writeIfStillOwned("blockedByModel") {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "blocked:modelUnavailable",
                        nextEligibleAt: nextEligible
                    )
                }
                logger.info("Job \(job.jobId) blocked: model unavailable, retry in 300s")

            case .pausedForThermal, .memoryPressure:
                let nextEligible = clock().timeIntervalSince1970 + 30
                await writeIfStillOwned("pausedThermalOrMemory") {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "paused",
                        nextEligibleAt: nextEligible
                    )
                }
                logger.info("Job \(job.jobId) paused for thermal/memory, retry in 30s")

            case .failed(let reason):
                await writeIfStillOwned("failed.increment") {
                    try await store.incrementAttemptCount(jobId: job.jobId)
                }
                let updated = lostOwnership ? nil : (try? await store.fetchJob(byId: job.jobId))
                let attempts = updated?.attemptCount ?? job.attemptCount + 1
                if attempts >= Self.maxAttemptCount {
                    await writeIfStillOwned("failed.supersede") {
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "superseded",
                            lastErrorCode: "maxAttemptsReached:\(reason)"
                        )
                    }
                    logger.warning("Job \(job.jobId) abandoned after \(attempts) attempts: \(reason)")
                } else {
                    let backoff = Self.exponentialBackoffSeconds(attempt: attempts)
                    let nextEligible = clock().timeIntervalSince1970 + backoff
                    await writeIfStillOwned("failed.requeue") {
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "failed",
                            nextEligibleAt: nextEligible,
                            lastErrorCode: reason
                        )
                    }
                    logger.warning("Job \(job.jobId) failed: \(reason), attempt \(attempts), backoff \(backoff)s")
                }

            case .backgroundExpired:
                await writeIfStillOwned("backgroundExpired.requeue") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                logger.info("Job \(job.jobId) background expired, requeued")

            case .cancelledByPlayback:
                await writeIfStillOwned("cancelledByPlayback.requeue") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                logger.info("Job \(job.jobId) cancelled by playback, requeued")

            case .preempted:
                await writeIfStillOwned("preempted.requeue") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                logger.info("Job \(job.jobId) preempted by higher-lane work, requeued")
            }
        } catch {
            PreAnalysisInstrumentation.endJobDuration(jobSignpost)
            if lostOwnership {
                logger.warning("Skipping failure cleanup writes for job \(job.jobId): lease reclaimed by orphan recovery (error: \(error))")
                return
            }
            await writeIfStillOwned("outerCatch.increment") {
                try await store.incrementAttemptCount(jobId: job.jobId)
            }
            let updated = lostOwnership ? nil : (try? await store.fetchJob(byId: job.jobId))
            let attempts = updated?.attemptCount ?? job.attemptCount + 1
            if attempts >= Self.maxAttemptCount {
                await writeIfStillOwned("outerCatch.supersede") {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "superseded",
                        lastErrorCode: "maxAttemptsReached:\(error.localizedDescription)"
                    )
                }
            } else {
                let backoff = Self.exponentialBackoffSeconds(attempt: attempts)
                let nextEligible = clock().timeIntervalSince1970 + backoff
                await writeIfStillOwned("outerCatch.requeue") {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "failed",
                        nextEligibleAt: nextEligible,
                        lastErrorCode: error.localizedDescription
                    )
                }
            }
            logger.error("Job \(job.jobId) threw: \(error)")
        }

        // Final releaseLease is the load-bearing tail for the
        // happy-path arms (e.g. `.reachedTarget` with all tiers done)
        // that don't early-return. Gated like every other store write
        // because AnalysisStore.releaseLease is a blind UPDATE-by-jobId
        // (not owner-scoped); calling it after losing ownership
        // clears the new owner's lease.
        await writeIfStillOwned("releaseLease.tail") {
            try await store.releaseLease(jobId: job.jobId)
        }
    }

    // MARK: - Lease-aware write helper

    /// Performs `body` only if this scheduler still owns the job's
    /// lease. `lostOwnership` may be flipped to `true` by the renewal
    /// task at any actor suspension point — so a single early-return
    /// guard before a chain of `await store.X(...)` calls is not
    /// sufficient. Wrap every cleanup-path store call so the check is
    /// re-evaluated immediately before the write.
    ///
    /// `body` errors are caught and logged here (matches the prior
    /// `do { try } catch { logger.error(...) }` pattern) so callers
    /// don't need to wrap each call themselves.
    private func writeIfStillOwned(
        _ what: String,
        _ body: () async throws -> Void
    ) async {
        guard !lostOwnership else { return }
        do {
            try await body()
        } catch is CancellationError {
            logger.warning("Cleanup write [\(what)] cancelled (likely lease reclaim mid-write)")
        } catch {
            logger.error("Failed cleanup write [\(what)]: \(error)")
        }
    }

    // MARK: - Tier Definitions

    /// Returns the next tier's coverage target, or nil if all tiers are complete.
    private func nextTierCoverage(current: Double) -> Double? {
        // Ensure tiers are ascending; skip any that aren't.
        let tiers = [config.defaultT0DepthSeconds, config.t1DepthSeconds, config.t2DepthSeconds]
            .sorted()
        for tier in tiers where tier > current {
            return tier
        }
        return nil
    }

    private func resolveAnalysisAssetId(
        for job: AnalysisJob,
        localAudioURL: LocalAudioURL
    ) async throws -> String {
        if let analysisAssetId = job.analysisAssetId {
            return analysisAssetId
        }

        // Lease-aware writes: this method runs after `leaseRenewalTask` is
        // armed, so any `await` here is a suspension point where the renewer
        // can flip `lostOwnership`. Throw CancellationError on a flip so the
        // existing asset-resolution catch in processJob hits its
        // `guard !lostOwnership` and skips its own cleanup writes too.
        // Orphan recovery (or the new owner) will redo this work cleanly.
        if let existing = try await store.fetchAssetByEpisodeId(job.episodeId) {
            guard !lostOwnership else { throw CancellationError() }
            try await store.updateJobAnalysisAssetId(jobId: job.jobId, analysisAssetId: existing.id)
            return existing.id
        }

        let capabilityJSON: String?
        do {
            let snapshot = await capabilitiesService.currentSnapshot
            let data = try JSONEncoder().encode(snapshot)
            capabilityJSON = String(data: data, encoding: .utf8)
        } catch {
            capabilityJSON = nil
        }

        let assetId = UUID().uuidString
        // playhead-i9dj: consume any stashed episode title from `enqueue(...)`
        // so the asset row carries the self-describing metadata at first
        // insert. The stash is cleared regardless — a missing entry simply
        // means no title was observed yet (lazy backfill on next enqueue).
        let stashedEpisodeTitle = pendingEpisodeTitles.removeValue(forKey: job.episodeId)
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: job.episodeId,
            assetFingerprint: job.sourceFingerprint,
            weakFingerprint: nil,
            sourceURL: localAudioURL.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: PreAnalysisConfig.analysisVersion,
            capabilitySnapshot: capabilityJSON,
            episodeTitle: stashedEpisodeTitle
        )
        guard !lostOwnership else { throw CancellationError() }
        try await store.insertAsset(asset)
        guard !lostOwnership else { throw CancellationError() }
        try await store.updateJobAnalysisAssetId(jobId: job.jobId, analysisAssetId: assetId)
        return assetId
    }

    static func shouldRetryCoverageInsufficient(job: AnalysisJob, outcome: AnalysisOutcome) -> Bool {
        let epsilon = coverageProgressEpsilon
        let featureAdvanced = outcome.featureCoverageSec > job.featureCoverageSec + epsilon
        let transcriptAdvanced = outcome.transcriptCoverageSec > job.transcriptCoverageSec + epsilon
        let cueAdvanced = outcome.cueCoverageSec > job.cueCoverageSec + epsilon
        let cuesCreated = outcome.newCueCount > 0

        return featureAdvanced || transcriptAdvanced || cueAdvanced || cuesCreated
    }
}

// MARK: - AnalysisJob → SchedulerLane derivation (playhead-r835)

extension AnalysisJob {
    /// Maps the job's `priority` into the scheduler's three-lane partition.
    ///
    /// The boundaries are:
    /// - `priority >= 20`       → `.now`
    /// - `priority 1..<20`      → `.soon`
    /// - `priority <= 0`        → `.background`
    ///
    /// These ranges are the ones spelled out in the playhead-r835 bead
    /// spec. Keep the ranges contiguous and non-overlapping — every integer
    /// priority must map to exactly one lane.
    var schedulerLane: AnalysisWorkScheduler.SchedulerLane {
        switch priority {
        case 20...:    return .now
        case 1..<20:   return .soon
        default:       return .background
        }
    }
}
