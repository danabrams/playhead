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

    // MARK: - PlayheadCatchupPolicy (playhead-yqax)

    /// Configuration for the foreground transcript catch-up escalation
    /// (playhead-yqax). When the user is actively playing an episode in
    /// the foreground and the playhead is approaching the end of the
    /// transcribed region, the scheduler escalates the active episode's
    /// `analysis_jobs` row to a deeper `desiredCoverageSec` so
    /// transcription chases the playhead rather than running out behind
    /// it. Long-form podcasts (Conan ≈ 117 min) systematically overflow
    /// the BG-task budget ceiling — without catch-up, the trailing
    /// 60–90 min are never transcribed and ad windows in that tail are
    /// scored only by the limited audio-feature path.
    ///
    /// The trigger is **distance-based**: when transcribed audio
    /// remaining ahead of the playhead is less than
    /// `triggerThresholdSec`, the catch-up bypass admits a single Now-
    /// lane dispatch for the active episode despite the standard
    /// `(foreground, playing)` block on deferred work in
    /// ``admissionBlocksDeferred()``. The escalated coverage target is
    /// `playheadPositionSec + lookaheadWindowSec`, capped at the
    /// episode duration when known.
    ///
    /// Backpressure: catch-up reuses the existing ``LaneAdmission``
    /// gate. ``LaneAdmission.pauseAllWork`` (thermal `.critical`) still
    /// dominates and blocks catch-up the same way it blocks every other
    /// admission. The bead documents that pipeline-FN time on an
    /// actively-listening user is worse than transient bandwidth
    /// contention at `.serious` thermal, so catch-up is admitted under
    /// `.serious` even though Soon/Background lanes are gated there.
    struct PlayheadCatchupPolicy: Sendable, Equatable {
        /// When transcribed-ahead < `triggerThresholdSec`, fire catch-up.
        /// 60 s default — enough lead time to absorb an FM cold start
        /// (~3 s) plus shard decode for the next chunk at 1× playback,
        /// while still being short enough that we are not ahead-prefetching
        /// for a user who might pause / change episode. Tied to the
        /// 30 s `seekRelatchThresholdSeconds` only by analogy — the
        /// units differ (transcript-coverage runway vs. seek delta).
        let triggerThresholdSec: TimeInterval

        /// How far ahead of the current playhead we want transcription
        /// to extend on each catch-up dispatch. 300 s default — five
        /// minutes of headroom is enough to outrun 1.5× / 2× playback
        /// while one stage 3 transcription pass runs (typically 30–60 s
        /// per minute of audio on M-class silicon). Independent of the
        /// T0/T1/T2 tier ladder.
        let lookaheadWindowSec: TimeInterval

        /// Default policy used when the scheduler is constructed without
        /// an explicit override.
        static let `default` = PlayheadCatchupPolicy(
            triggerThresholdSec: 60,
            lookaheadWindowSec: 300
        )

        /// Disabled policy — both thresholds zero. Used by tests that
        /// want to assert catch-up does NOT fire under the current
        /// (scenePhase, playbackContext, position) snapshot.
        static let disabled = PlayheadCatchupPolicy(
            triggerThresholdSec: 0,
            lookaheadWindowSec: 0
        )
    }

    /// Resolved catch-up opportunity. Returned by
    /// ``currentCatchupOpportunity()`` when a foreground catch-up
    /// dispatch should fire on the next loop iteration. The scheduler
    /// uses `escalatedDesiredCoverageSec` to override the persisted
    /// row's `desiredCoverageSec` before dispatch so the runner
    /// transcribes deeper than the standard tier ladder allows.
    struct CatchupOpportunity: Sendable, Equatable {
        let jobId: String
        let episodeId: String
        let priorDesiredCoverageSec: Double
        let escalatedDesiredCoverageSec: Double
        /// Transcript coverage end time read from the asset row at
        /// trigger time. Surfaced for instrumentation only — the
        /// scheduler does not consume it after the dispatch decision.
        let transcribedAheadSec: Double
        /// Playhead position observed at trigger time. Surfaced for
        /// instrumentation only.
        let playheadPositionSec: TimeInterval
    }

    // MARK: - AcousticPromotionPolicy (playhead-gtt9.24)

    /// Configuration for acoustic-triggered transcription scheduling
    /// (playhead-gtt9.24). Acoustic features are extracted cheaply at
    /// Stage 2 (`FeatureExtractionService.extractAndPersist`) and
    /// persisted to `feature_windows`. When feature coverage extends
    /// beyond transcript coverage — typically because the episode is
    /// long enough that the tier ladder will hit T2 short of the end —
    /// the scheduler scores each unscored window for ad-likelihood via
    /// ``AcousticLikelihoodScorer`` and picks the highest-scoring
    /// region as the next coverage target. The ad region transcribes
    /// before equivalent-position clean speech because the escalation
    /// happens immediately, before linear progression has a chance to
    /// burn the BG-task budget on the prefix.
    ///
    /// **Composition with ``PlayheadCatchupPolicy``:** foreground
    /// catch-up (``currentCatchupOpportunity()``) is playhead-driven
    /// — fired on every (foreground, playing) tick when the user is
    /// catching up to the trailing edge of transcribed audio. Acoustic
    /// promotion is content-driven — fired whenever an unscored window
    /// past the current target scores above the threshold. The two
    /// compose: the scheduler consults catch-up FIRST (it's the more
    /// time-sensitive bypass — user is actively listening), and falls
    /// through to acoustic promotion when no catch-up opportunity
    /// exists. Both ultimately escalate `desiredCoverageSec` via the
    /// same `updateJobDesiredCoverage` mechanism, so they cannot
    /// fight each other — the runner sees the deeper of the two
    /// targets on its next dispatch.
    ///
    /// **Cold-start behaviour:** when an asset has no persisted
    /// feature windows yet (first run, before Stage 2 has produced
    /// any output for this asset), `highestLikelihoodBeyond(...)`
    /// returns `nil` and acoustic promotion is a no-op. The scheduler
    /// falls back to the standard tier ladder, which is the right
    /// answer — without features there is no acoustic signal to act
    /// on. As soon as the first tier (T0 = 90 s) completes, features
    /// for that prefix exist and promotion can begin to fire on
    /// subsequent passes.
    struct AcousticPromotionPolicy: Sendable, Equatable {
        /// Minimum acoustic-likelihood score (in `[0, 1]`) for a
        /// window beyond the current coverage target to trigger
        /// promotion. Higher = pickier; lower = more aggressive
        /// (will burn more BG-task wakes on borderline regions).
        ///
        /// 0.5 default — half the theoretical max. The scorer's
        /// default-prior weights (see ``AcousticLikelihoodScorer.Weights``)
        /// are calibrated so that a window with foreground music bed
        /// + clear speaker change crosses 0.5 even without spectral
        /// flux contribution; clean host conversation rarely scores
        /// above 0.2.
        let scoreThreshold: Double

        /// Minimum lookahead (seconds) the promotion target must
        /// extend past the current `desiredCoverageSec`. Below this
        /// gap, the standard tier ladder will reach the high-score
        /// window soon enough on its own and we don't burn an extra
        /// admission cycle. 60 s default — one tier-ladder step
        /// beyond T0's depth granularity.
        let minimumEscalationGapSec: Double

        /// Default policy used when the scheduler is constructed
        /// without an explicit override.
        static let `default` = AcousticPromotionPolicy(
            scoreThreshold: 0.5,
            minimumEscalationGapSec: 60
        )

        /// Disabled policy — score threshold above 1.0, so no window
        /// can ever pass. Used by tests + experiments that want to
        /// assert promotion does NOT fire under a given snapshot.
        static let disabled = AcousticPromotionPolicy(
            scoreThreshold: 2.0,
            minimumEscalationGapSec: 0
        )
    }

    /// Resolved acoustic-promotion opportunity. Returned by
    /// ``currentAcousticPromotionOpportunity(for:)`` when a window past
    /// the current coverage target scores above the policy's score
    /// threshold AND the escalated target is far enough beyond the
    /// current target to be worth a separate dispatch.
    struct AcousticPromotionOpportunity: Sendable, Equatable {
        let jobId: String
        let episodeId: String
        let priorDesiredCoverageSec: Double
        let escalatedDesiredCoverageSec: Double
        /// Episode-time start of the window that triggered the
        /// promotion. Surfaced for instrumentation only.
        let triggerWindowStartSec: Double
        /// Episode-time end of the window that triggered the
        /// promotion. The promoted coverage target equals this value
        /// (capped at episode duration when known).
        let triggerWindowEndSec: Double
        /// Acoustic-likelihood score of the trigger window
        /// (`[0, 1]`). Surfaced for instrumentation + telemetry —
        /// callers stamp it onto FrozenTrace so the harness can
        /// distinguish a "high-confidence" promotion from a
        /// borderline one.
        let triggerWindowScore: Double
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

    /// playhead-yqax: live playhead position for the actively-playing
    /// episode. Updated from the run-loop status observer via
    /// ``noteCurrentPlayheadPosition(episodeId:position:)`` on every
    /// distinct ~1 s tick (the runtime call site coalesces sub-second
    /// updates so the scheduler isn't hammered). `nil` when no episode
    /// is loaded or playback has fully stopped. Catch-up reads this
    /// alongside `activePlaybackEpisodeId` and the asset's
    /// `fastTranscriptCoverageEndTime` to decide whether to fire.
    private var playheadPositionSec: TimeInterval?

    /// playhead-yqax: the foreground-catch-up policy this scheduler
    /// applies. Production callers pass `.default`; tests pass either a
    /// custom policy to explore boundary cases or `.disabled` to assert
    /// catch-up fires zero times under a given snapshot.
    private let catchupPolicy: PlayheadCatchupPolicy

    /// playhead-gtt9.24: the acoustic-promotion policy this scheduler
    /// applies. Production callers pass `.default`; tests pass either a
    /// custom policy (e.g. lower threshold, smaller escalation gap) to
    /// explore boundary cases or `.disabled` to assert acoustic
    /// promotion fires zero times under a given snapshot. See
    /// ``AcousticPromotionPolicy`` for the composition contract with
    /// foreground catch-up.
    private let acousticPromotionPolicy: AcousticPromotionPolicy

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

    /// playhead-gjz6 (Gap-4 second half): submits a backfill
    /// `BGProcessingTask` so iOS wakes the app to drain the analysis
    /// queue when `enqueue` is called while the app is already
    /// backgrounded. The first half of Gap-4 (playhead-fuo6) covered
    /// the `.background` *transition* path via PlayheadApp's scenePhase
    /// observer; this seam covers the inverse case where a download
    /// completes via background URLSession, lands on `enqueue`, and no
    /// scenePhase transition fires because the app was already in
    /// `.background`. Without this rearm the new analysis job sits
    /// queued until the next foreground.
    ///
    /// Production wires this to `BackgroundProcessingService.scheduleBackfillIfNeeded()`
    /// via `ProductionBackfillScheduler`. Tests inject a stub to assert
    /// the rearm fires (or doesn't) without standing up the real BPS.
    /// nil-able so existing test factories that don't care about the
    /// rearm path can continue to construct a scheduler without a
    /// scheduler stub. Reuses the `BackfillScheduling` protocol declared
    /// in `BackgroundFeedRefreshService.swift` (Gap-5 fix) — same
    /// contract, same production adapter (`ProductionBackfillScheduler`).
    private let backfillScheduler: (any BackfillScheduling)?

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

    /// playhead-gyvb.2: stash audio-file durations probed at `enqueue(...)`
    /// time so `resolveAnalysisAssetId` can populate `episodeDurationSec`
    /// on the `analysis_assets` row at first insert.
    ///
    /// Mirrors the `pendingEpisodeTitles` shape. The probe runs once per
    /// download against the cached file; the result is written to an
    /// existing asset row immediately and otherwise stashed here so the
    /// new asset row created by `resolveAnalysisAssetId` carries the
    /// measured duration without waiting for the spool/decode pass.
    ///
    /// Cleared per-episode at materialization time. Best-effort: a probe
    /// failure simply leaves the dictionary entry absent.
    private var pendingProbedEpisodeDurations: [String: Double] = [:]

    init(
        store: AnalysisStore,
        jobRunner: AnalysisJobRunner,
        capabilitiesService: any CapabilitiesProviding,
        downloadManager: any DownloadProviding,
        batteryProvider: any BatteryStateProviding = UIDeviceBatteryProvider(),
        transportStatusProvider: any TransportStatusProviding = WifiTransportStatusProvider(),
        candidateWindowCascade: CandidateWindowCascade? = nil,
        config: PreAnalysisConfig = .load(),
        clock: @escaping @Sendable () -> Date = { Date() },
        backfillScheduler: (any BackfillScheduling)? = nil,
        catchupPolicy: PlayheadCatchupPolicy = .default,
        acousticPromotionPolicy: AcousticPromotionPolicy = .default
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
        self.backfillScheduler = backfillScheduler
        self.catchupPolicy = catchupPolicy
        self.acousticPromotionPolicy = acousticPromotionPolicy
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

        // playhead-gyvb.2: measure-on-download. Real-world incident
        // (2026-04-27) — feed metadata `<itunes:duration>` was off by
        // up to 13.8× on libsyn/flightcast feeds. Once the file is on
        // disk, AVURLAsset reads its container header and tells us the
        // truth. Per the bead: "Once we have the real runtime from the
        // file that should be the source of truth."
        //
        // Best-effort:
        //   - missing cached file (download not yet landed) → skip
        //   - probe returns nil (non-audio, indeterminate) → skip
        //   - probe returns a positive duration → overwrite an
        //     existing asset row, OR stash for the lazy
        //     `resolveAnalysisAssetId` path so the freshly-inserted
        //     row carries the probed value at first insert.
        if let cachedURL = await downloadManager.cachedFileURL(for: episodeId),
           let probedDuration = await AudioFileDurationProbe.probeDuration(at: cachedURL) {
            do {
                if let asset = try await store.fetchAssetByEpisodeId(episodeId) {
                    try await store.updateEpisodeDuration(
                        id: asset.id,
                        episodeDurationSec: probedDuration
                    )
                } else {
                    // Same potential-leak shape as `pendingEpisodeTitles`: a
                    // racing concurrent insert could leave this entry
                    // unconsumed. Intentional parity with the existing
                    // pattern — `resolveAnalysisAssetId` drains both stashes.
                    pendingProbedEpisodeDurations[episodeId] = probedDuration
                }
            } catch {
                logger.warning("Failed to persist probed duration for \(episodeId): \(error)")
            }
        }

        wakeSchedulerLoop()

        // playhead-gjz6 (Gap-4 second half): if the app is currently
        // backgrounded, ask `BackgroundProcessingService` to submit a
        // backfill `BGProcessingTask` so iOS wakes the app to drain the
        // analysis queue. Without this hop, a download that completes
        // via background URLSession while the app is already in
        // `.background` produces no scenePhase transition (the
        // first-half PlayheadApp observer in playhead-fuo6 covers
        // foreground→background), so the just-enqueued job sits queued
        // until the next foreground (overnight blackout class of bug
        // — same shape as fuo6 / 5uvz.4 Gap-5).
        //
        // Skip the rearm in foreground: the scheduler's run loop is
        // already eligible to pick up the new job on its next iteration
        // (we just woke it above). Submitting a BGProcessingTask while
        // foregrounded is wasted iOS budget and would compete with the
        // foreground run loop for the same queue.
        if schedulerScenePhase == .background {
            await backfillScheduler?.scheduleBackfillIfNeeded()
        }
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
        // playhead-yqax: drop the playhead snapshot in lockstep with
        // the active-episode reset so a stale position cannot fire
        // catch-up against a future episode load.
        playheadPositionSec = nil
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
            // playhead-yqax: drop the playhead snapshot when the
            // transport reports idle so a stale position from the
            // prior episode cannot fire catch-up against the next
            // load. Repopulated by the next
            // ``noteCurrentPlayheadPosition(episodeId:position:)``
            // call once playback resumes.
            playheadPositionSec = nil
        }
        // Wake only on a genuine transition so the idle poll loop doesn't
        // get hammered by coalesced status ticks (observeStates fires at
        // AVPlayer's periodic-time cadence — many ticks per second).
        if priorContext != context {
            wakeSchedulerLoop()
        }
    }

    /// playhead-yqax: update the live playhead position for the
    /// actively-playing episode. Called from the runtime's transport-
    /// status observer on each periodic-time tick. The scheduler reads
    /// this in ``currentCatchupOpportunity()`` together with
    /// ``activePlaybackEpisodeId`` to decide whether to fire a
    /// foreground catch-up dispatch despite the standard
    /// `(foreground, playing)` block on deferred work.
    ///
    /// `episodeId` must match `activePlaybackEpisodeId`; if it does
    /// not (e.g. a stale tick arriving after a track-change) the call
    /// is silently dropped so the catch-up trigger cannot run on a
    /// position from the prior episode. A `nil` position from the
    /// caller resets the field — used at end-of-episode and in tests.
    ///
    /// Coalescing: callers should already throttle (the runtime
    /// observer forwards only on whole-second changes) so this method
    /// does NOT re-implement throttling. Each call wakes the loop
    /// because a position change can flip the catch-up trigger from
    /// "no opportunity" to "fire now"; a wake here is the fastest path
    /// to react. The wake is no-op if the loop is already running.
    func noteCurrentPlayheadPosition(
        episodeId: String,
        position: TimeInterval?
    ) {
        guard episodeId == activePlaybackEpisodeId else { return }
        playheadPositionSec = position
        // Wake the loop so a newly-eligible catch-up dispatch is
        // considered immediately rather than after the next idle poll.
        wakeSchedulerLoop()
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

    // MARK: - Foreground catch-up (playhead-yqax)

    /// playhead-yqax: evaluate whether a foreground transcript catch-up
    /// dispatch should fire on this loop iteration. Returns the
    /// resolved opportunity (job to escalate + escalated coverage) or
    /// `nil` when no catch-up is needed.
    ///
    /// Trigger preconditions (all must hold):
    ///   1. `(scenePhase, playbackContext) == (.foreground, .playing)`.
    ///   2. An episode is loaded (`activePlaybackEpisodeId != nil`).
    ///   3. A live playhead position has been observed for this
    ///      episode.
    ///   4. The latest non-terminal job for the active episode is
    ///      eligible for dispatch (queued/paused/failed,
    ///      lease free/expired, `nextEligibleAt` due).
    ///   5. `transcriptCoverageEnd - playheadPosition < triggerThresholdSec`.
    ///   6. The escalated coverage (`playheadPosition + lookaheadWindowSec`,
    ///      capped to episode duration when known) strictly exceeds
    ///      the job's persisted `desiredCoverageSec` — otherwise the
    ///      job is already targeted at a deeper coverage and the
    ///      standard scheduler path will pick it up the moment
    ///      audio releases the pipeline.
    ///   7. The current `LaneAdmission` does not pause all work
    ///      (thermal `.critical`); we deliberately admit catch-up at
    ///      `.serious` because the bead's premise is that pipeline-FN
    ///      time on an actively-listening user is worse than transient
    ///      bandwidth contention.
    ///
    /// Returns `nil` when any precondition fails. Best-effort: a SQL
    /// hiccup mid-evaluation logs and returns nil rather than
    /// propagating, so a transient store error cannot stall the loop.
    private func currentCatchupOpportunity(
        admission: LaneAdmission,
        now: TimeInterval
    ) async -> CatchupOpportunity? {
        // Precondition 7 — pauseAllWork dominates everything.
        guard !admission.pauseAllWork else { return nil }
        // Preconditions 1 + 2.
        guard schedulerScenePhase == .foreground,
              playbackContext == .playing,
              let episodeId = activePlaybackEpisodeId
        else { return nil }
        // Precondition 3.
        guard let playheadPosition = playheadPositionSec else { return nil }
        // Trivially-misconfigured policy guard. A zero trigger
        // threshold means "never fire" by construction; bail before
        // any store work.
        guard catchupPolicy.triggerThresholdSec > 0,
              catchupPolicy.lookaheadWindowSec > 0
        else { return nil }

        // Resolve the latest job row for this episode; a missing row
        // means the episode was deleted or never enqueued — no
        // catch-up to fire.
        let job: AnalysisJob
        do {
            guard let row = try await store.fetchLatestJobForEpisode(episodeId)
            else { return nil }
            job = row
        } catch {
            logger.warning("currentCatchupOpportunity: fetchLatestJobForEpisode threw for \(episodeId): \(error)")
            return nil
        }

        // Precondition 4 — same eligibility predicate the loop's
        // selector applies. We do NOT consult `deferredWorkAllowed`:
        // catch-up is the explicit override of the deferred-work
        // block in `(foreground, playing)`, and the lane-cap +
        // QualityProfile checks happen later in the run loop using
        // the same gates regular admissions use.
        guard isEligibleForDispatch(job: job, deferredWorkAllowed: true, now: now)
        else { return nil }

        // Read transcript coverage from the asset row. If no asset row
        // exists yet (first run, asset not materialized), the runner
        // will create one — but coverage is necessarily zero, so a
        // distance check against `playheadPosition` always trips and
        // catch-up should fire if the playhead is non-trivial.
        let asset: AnalysisAsset?
        do {
            if let assetId = job.analysisAssetId {
                asset = try await store.fetchAsset(id: assetId)
            } else if let byEpisode = try await store.fetchAssetByEpisodeId(episodeId) {
                asset = byEpisode
            } else {
                asset = nil
            }
        } catch {
            logger.warning("currentCatchupOpportunity: fetchAsset threw for \(episodeId): \(error)")
            return nil
        }
        let transcriptCoverageEnd = asset?.fastTranscriptCoverageEndTime ?? 0
        let transcribedAhead = max(0, transcriptCoverageEnd - playheadPosition)

        // Precondition 5 — distance gate.
        guard transcribedAhead < catchupPolicy.triggerThresholdSec else {
            return nil
        }

        // Compute the escalated coverage target.
        // playheadPosition + lookaheadWindowSec, clamped at episode
        // duration when known. Episode duration is `nil` until
        // Stage 1 of the runner persists it (Pipeline B path) or
        // `AnalysisCoordinator.runFromSpooling` writes it (Pipeline A
        // path). Without a duration we cap at a large but finite
        // value (`Double.greatestFiniteMagnitude` would feed
        // confusing telemetry) — use the playhead + lookahead
        // unclamped which is the natural target.
        let unclampedTarget = playheadPosition + catchupPolicy.lookaheadWindowSec
        let escalatedTarget: Double = {
            guard let duration = asset?.episodeDurationSec else { return unclampedTarget }
            return min(unclampedTarget, duration)
        }()

        // Precondition 6 — only fire when the escalation is strictly
        // greater than the persisted target. Equal-or-less means the
        // existing tier ladder already handles the runway and the
        // standard `(foreground, playing)` block correctly applies.
        guard escalatedTarget > job.desiredCoverageSec + 0.001 else {
            return nil
        }

        return CatchupOpportunity(
            jobId: job.jobId,
            episodeId: episodeId,
            priorDesiredCoverageSec: job.desiredCoverageSec,
            escalatedDesiredCoverageSec: escalatedTarget,
            transcribedAheadSec: transcribedAhead,
            playheadPositionSec: playheadPosition
        )
    }

    #if DEBUG
    /// Test-only accessor returning the `CatchupOpportunity` the run
    /// loop would dispatch on the next iteration, or `nil` if no
    /// catch-up should fire under the current snapshot. Tests use this
    /// to assert the trigger predicate without driving the full loop.
    func currentCatchupOpportunityForTesting() async -> CatchupOpportunity? {
        let admission = await currentLaneAdmission()
        let now = clock().timeIntervalSince1970
        return await currentCatchupOpportunity(admission: admission, now: now)
    }

    /// Test-only accessor for the persisted catch-up policy.
    func catchupPolicyForTesting() -> PlayheadCatchupPolicy {
        catchupPolicy
    }

    /// Test-only accessor for the live playhead position field.
    func playheadPositionSecForTesting() -> TimeInterval? {
        playheadPositionSec
    }
    #endif

    // MARK: - Acoustic-triggered promotion evaluation (playhead-gtt9.24)

    /// Evaluate whether the scheduler should fire an acoustic-triggered
    /// dispatch on the next run-loop iteration. Inspects persisted
    /// `feature_windows` for the candidate job's asset and asks
    /// ``AcousticLikelihoodScorer.highestLikelihoodBeyond(...)`` for the
    /// highest-scoring window past the job's current
    /// `desiredCoverageSec`. When that window's score crosses the
    /// policy threshold AND the implied escalation is a non-trivial
    /// step beyond the current target (per
    /// ``AcousticPromotionPolicy.minimumEscalationGapSec``), returns an
    /// ``AcousticPromotionOpportunity`` describing the promotion.
    ///
    /// **Why "candidate job" not "currently-playing episode":** unlike
    /// foreground catch-up, acoustic promotion is content-driven — it
    /// asks "is there an ad-shaped region waiting in the unscored
    /// portion of the queue's current target episode?" The "queue's
    /// current target" is whatever job `selectNextDispatchableJob`
    /// would dispatch next; if the cascade re-orders the queue (swws),
    /// promotion follows the cascade winner. The two questions
    /// (content-driven vs playhead-driven) are deliberately separated
    /// so a backgrounded catch-up does not preempt foreground catch-up,
    /// and so a foreground listening session whose runway is fine still
    /// gets ad-region pre-fetch in the background.
    ///
    /// **Cold-start fallback:** when the asset has no persisted
    /// feature windows yet (first run, before Stage 2 has produced
    /// any output for this asset), the scorer returns nil and this
    /// method also returns nil. The scheduler falls back to the
    /// standard tier ladder, which is the right answer — without
    /// features there is no acoustic signal to act on. Once T0 has
    /// completed, features for that prefix exist and promotion can
    /// fire on subsequent passes.
    ///
    /// Returns `nil` when:
    ///   - `pauseAllWork` admission (thermal `.critical`).
    ///   - Policy threshold is misconfigured above 1.0 (`disabled`).
    ///   - No candidate job exists for the queue.
    ///   - The candidate job is not eligible for dispatch
    ///     (state/lease/nextEligibleAt).
    ///   - The asset has no `analysisAssetId` persisted yet.
    ///   - No persisted feature window past `desiredCoverageSec`
    ///     scores above the threshold.
    ///   - The escalation gap to the trigger window is below
    ///     `minimumEscalationGapSec`.
    ///
    /// Best-effort: SQL hiccups log and return nil rather than
    /// propagating, so a transient store error cannot stall the loop.
    private func currentAcousticPromotionOpportunity(
        admission: LaneAdmission,
        deferredWorkAllowed: Bool,
        now: TimeInterval
    ) async -> AcousticPromotionOpportunity? {
        // Pause-all dominates everything (matches catch-up semantics).
        guard !admission.pauseAllWork else { return nil }

        // Trivially-misconfigured policy guard. A score threshold above
        // 1.0 is the `.disabled` sentinel — bail before any store work.
        guard acousticPromotionPolicy.scoreThreshold <= 1.0 else { return nil }

        // Resolve the candidate job: whichever job the run loop would
        // dispatch next under the current admission.
        guard let selected = await selectNextDispatchableJob(
            deferredWorkAllowed: deferredWorkAllowed,
            now: now
        ) else { return nil }
        let job = selected.job

        // The asset must already have an analysisAssetId; without it
        // there are no persisted feature_windows to score.
        guard let assetId = job.analysisAssetId else { return nil }

        // Fetch persisted feature windows past the current coverage
        // target. We bound the read at `Double.greatestFiniteMagnitude`
        // so the asset's full feature coverage is considered (the
        // scorer's `highestLikelihoodBeyond` already filters internally
        // by `endTime > currentCoverageSec`).
        let windows: [FeatureWindow]
        do {
            windows = try await store.fetchFeatureWindows(
                assetId: assetId,
                from: 0,
                to: Double.greatestFiniteMagnitude
            )
        } catch {
            logger.warning("currentAcousticPromotionOpportunity: fetchFeatureWindows threw for asset \(assetId): \(error)")
            return nil
        }

        // Empty feature window set → cold start (Stage 2 hasn't run yet
        // for this asset). Scorer also returns nil for empty input but
        // we early-out for clarity / log volume.
        guard !windows.isEmpty else { return nil }

        // Score the windows past the current target and pick the
        // highest. The scorer applies the threshold internally.
        let currentCoverage = job.desiredCoverageSec
        guard let best = AcousticLikelihoodScorer.highestLikelihoodBeyond(
            windows: windows,
            currentCoverageSec: currentCoverage,
            threshold: acousticPromotionPolicy.scoreThreshold
        ) else { return nil }

        // Compute the escalation target: the trigger window's end time,
        // capped at episode duration when the asset row knows it.
        // Reading the asset is best-effort — if the row is missing the
        // duration cap is simply skipped.
        let asset: AnalysisAsset?
        do {
            asset = try await store.fetchAsset(id: assetId)
        } catch {
            logger.warning("currentAcousticPromotionOpportunity: fetchAsset threw for asset \(assetId): \(error)")
            asset = nil
        }
        let unclampedTarget = best.windowEnd
        let escalatedTarget: Double = {
            guard let duration = asset?.episodeDurationSec else { return unclampedTarget }
            return min(unclampedTarget, duration)
        }()

        // Escalation-gap gate: only fire when the new target is at
        // least `minimumEscalationGapSec` past the current target.
        // Below this gap the standard tier ladder will reach the
        // window soon enough on its own.
        guard escalatedTarget - currentCoverage >= acousticPromotionPolicy.minimumEscalationGapSec else {
            return nil
        }

        return AcousticPromotionOpportunity(
            jobId: job.jobId,
            episodeId: job.episodeId,
            priorDesiredCoverageSec: currentCoverage,
            escalatedDesiredCoverageSec: escalatedTarget,
            triggerWindowStartSec: best.windowStart,
            triggerWindowEndSec: best.windowEnd,
            triggerWindowScore: best.score
        )
    }

    #if DEBUG
    /// Test-only accessor returning the `AcousticPromotionOpportunity`
    /// the run loop would dispatch on the next iteration, or `nil` if
    /// no acoustic promotion should fire under the current snapshot.
    /// Tests use this to assert the trigger predicate without driving
    /// the full loop.
    func currentAcousticPromotionOpportunityForTesting() async -> AcousticPromotionOpportunity? {
        let admission = await currentLaneAdmission()
        let now = clock().timeIntervalSince1970
        let deferredWorkAllowed = admission.policy.allowSoonLane
            || admission.policy.allowBackgroundLane
        return await currentAcousticPromotionOpportunity(
            admission: admission,
            deferredWorkAllowed: deferredWorkAllowed,
            now: now
        )
    }

    /// Test-only accessor for the persisted acoustic-promotion policy.
    func acousticPromotionPolicyForTesting() -> AcousticPromotionPolicy {
        acousticPromotionPolicy
    }
    #endif

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

            let admission = await currentLaneAdmission()

            // Critical thermal (or equivalent) pauses every lane, including
            // T0 playback drains AND foreground catch-up. Wait for the next
            // wake/capability change.
            if admission.pauseAllWork {
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            let now = clock().timeIntervalSince1970

            // playhead-yqax: foreground transcript catch-up bypass.
            // When the user is actively playing an episode in the
            // foreground and the playhead is approaching the end of
            // the transcribed region, escalate the active episode's
            // job's `desiredCoverageSec` and dispatch it as a Now-lane
            // job — bypassing the standard `(foreground, playing)`
            // block on deferred work. The bypass is consulted BEFORE
            // `admissionBlocksDeferred()` so the (foreground, playing)
            // sleep does not pre-empt the catch-up evaluation; the
            // opportunity itself implicitly requires
            // (foreground, playing) so non-catch-up states fall through
            // unchanged.
            //
            // The escalation persists to the `analysis_jobs` row
            // (`updateJobDesiredCoverage`) so the runner sees the new
            // target, the outcome arms route through "all tiers done"
            // when the post-run coverage exceeds T2, and a crash mid-
            // catch-up does not lose the deeper target on resume.
            if let opportunity = await currentCatchupOpportunity(
                admission: admission,
                now: now
            ) {
                await dispatchForegroundCatchup(opportunity: opportunity)
                continue
            }

            // playhead-gtt9.14: 4-state admission filter over
            // (scenePhase, playbackContext). Prior to gtt9.14 the
            // scheduler blocked deferred work whenever an episode was
            // loaded — treating foreground-paused the same as
            // foreground-playing, which is the opposite of what the
            // device's capability envelope suggests. See
            // `admissionBlocksDeferred()` for the full matrix.
            //
            // playhead-yqax: this block now follows the catch-up
            // bypass above. When `(foreground, playing)` is hit we
            // first ask "is catch-up needed?" — if yes we dispatched
            // and `continue`'d above; if no we fall through to the
            // pre-yqax behavior (sleep the loop).
            if admissionBlocksDeferred() {
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

            // playhead-gtt9.24: acoustic-triggered transcription
            // scheduling. Inspect persisted feature_windows for the
            // queue's current candidate job and ask whether any
            // unscored window past `desiredCoverageSec` carries enough
            // ad-likelihood mass to justify escalating the coverage
            // target ahead of the standard tier ladder.
            //
            // Why this fires AFTER catchup and admissionBlocksDeferred:
            //   - Catchup is the most time-sensitive bypass — user is
            //     actively listening at the trailing edge of the
            //     transcribed region — it always wins.
            //   - admissionBlocksDeferred() is the (foreground, playing)
            //     guard that prevents the scheduler from competing with
            //     the playback decode path. Acoustic promotion is
            //     deferred work by definition, so it must respect that
            //     guard.
            //
            // The dispatch persists the new `desiredCoverageSec` to the
            // job row via `updateJobDesiredCoverage`, then falls
            // through to the standard `selectNextDispatchableJob` path.
            // When the standard path picks the same job (which it will,
            // because it's the same `selectNextDispatchableJob` query)
            // the dispatch carries the deeper coverage target into the
            // runner. A crash mid-promotion does not lose the deeper
            // target on resume — same persistence guarantee as
            // foreground catchup.
            if let promotion = await currentAcousticPromotionOpportunity(
                admission: admission,
                deferredWorkAllowed: deferredWorkAllowed,
                now: now
            ) {
                await dispatchAcousticPromotion(opportunity: promotion)
                continue
            }

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

    // MARK: - Foreground catch-up dispatch (playhead-yqax)

    /// Dispatch a foreground transcript catch-up admission. Persists
    /// the escalated `desiredCoverageSec` onto the `analysis_jobs` row
    /// so the runner picks up the deeper target, then routes through
    /// the standard `processJob` path. The job's lane (computed from
    /// its `priority`) determines which lane counter increments — for
    /// the typical `preAnalysis` row at priority 0 / 10 this lands in
    /// Background or Soon, and the Now-cap is therefore not consumed.
    ///
    /// Why a separate dispatch entrypoint:
    ///   1. The standard `selectNextDispatchableJob` honors FIFO across
    ///      every eligible episode; catch-up specifically wants THIS
    ///      episode's job, not whichever happens to be top-of-queue.
    ///   2. The lane-cap (`canAdmit`) and per-lane `evaluateAdmissionGate`
    ///      checks are still consulted here so catch-up can't bust the
    ///      Now-cap or skip the multi-resource gate. A rejection at
    ///      either point falls back to the standard sleep (no special
    ///      catch-up retry path — the next loop iteration will
    ///      re-evaluate the trigger predicate against the next observed
    ///      playhead).
    private func dispatchForegroundCatchup(opportunity: CatchupOpportunity) async {
        // Persist the escalation so the runner reads the deeper
        // target on its next `fetchJob(byId:)` (and a crash mid-
        // catch-up resumes against the deeper target rather than the
        // stale tier value).
        do {
            try await store.updateJobDesiredCoverage(
                jobId: opportunity.jobId,
                desiredCoverageSec: opportunity.escalatedDesiredCoverageSec
            )
        } catch {
            logger.warning("Foreground catch-up: updateJobDesiredCoverage threw for job \(opportunity.jobId): \(error)")
            await sleepOrWake(seconds: Self.idlePollSeconds)
            return
        }

        // Re-fetch the row so the dispatch reflects the persisted
        // escalation. A `nil` here is unusual (the row existed at
        // `currentCatchupOpportunity` evaluation time moments ago)
        // but bail safely if the row was concurrently superseded.
        let job: AnalysisJob
        do {
            guard let refreshed = try await store.fetchJob(byId: opportunity.jobId) else {
                logger.warning("Foreground catch-up: job \(opportunity.jobId) disappeared after escalation")
                return
            }
            job = refreshed
        } catch {
            logger.warning("Foreground catch-up: fetchJob threw for \(opportunity.jobId): \(error)")
            return
        }

        // Lane-cap and admission-gate checks mirror `runLoop()`. We
        // consult both because catch-up should never bust the Now-cap
        // or skip the bnrs gate — both invariants are preserved when
        // catch-up escalates the same row that would have been
        // dispatched normally; the only thing that changed is the
        // coverage target.
        guard canAdmit(job: job) else {
            logger.info("Foreground catch-up: lane \(String(describing: job.schedulerLane), privacy: .public) at capacity; deferring")
            await sleepOrWake(seconds: Self.idlePollSeconds)
            return
        }

        let gateDecision = await evaluateAdmissionGate(for: job)
        if case .reject(let cause) = gateDecision {
            logger.info("Foreground catch-up: AdmissionGate rejected job \(job.jobId) cause=\(cause.rawValue, privacy: .public)")
            await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
            return
        }

        PreAnalysisInstrumentation.logForegroundCatchUp(
            episodeId: opportunity.episodeId,
            jobId: opportunity.jobId,
            priorCoverageSec: opportunity.priorDesiredCoverageSec,
            escalatedCoverageSec: opportunity.escalatedDesiredCoverageSec,
            playheadPositionSec: opportunity.playheadPositionSec,
            transcribedAheadSec: opportunity.transcribedAheadSec
        )

        didStart(job: job)
        await processJob(job, cascadeWindow: nil)
        didFinish(job: job)
    }

    // MARK: - Acoustic-triggered promotion dispatch (playhead-gtt9.24)

    /// Dispatch an acoustic-triggered transcription promotion. Persists
    /// the escalated `desiredCoverageSec` onto the `analysis_jobs` row
    /// so the runner picks up the deeper target, emits the
    /// `acousticPromoted` instrumentation line, then routes through
    /// the standard `processJob` path.
    ///
    /// Why a separate dispatch entrypoint (mirrors yqax catchup):
    ///   1. Telemetry — the dispatch reason needs to be stamped before
    ///      `processJob` so the harness can distinguish acoustic
    ///      promotion from linear progression.
    ///   2. Re-fetch — after `updateJobDesiredCoverage` the in-memory
    ///      job row is stale; the dispatch must read the persisted row
    ///      so the runner's coverage gating sees the new target.
    ///   3. The lane-cap (`canAdmit`) and per-lane
    ///      `evaluateAdmissionGate` checks still gate this path so
    ///      promotion can't bust the Now-cap or skip the multi-resource
    ///      gate. A rejection at either point falls back to the
    ///      standard sleep — the next loop iteration will re-evaluate
    ///      promotion (with the persisted escalation still in place;
    ///      the standard path will eventually pick it up).
    ///
    /// **Composition note:** because `currentAcousticPromotionOpportunity`
    /// uses `selectNextDispatchableJob` to pick the candidate, the
    /// dispatched job here is the same one `selectNextDispatchableJob`
    /// would pick under the standard path. We therefore process it
    /// directly with `cascadeWindow: nil` (the cascade entry is
    /// observability-only per the audit; running through processJob
    /// without it does not bypass any execution semantic — slice
    /// execution is playhead-1iq1).
    private func dispatchAcousticPromotion(opportunity: AcousticPromotionOpportunity) async {
        // Persist the escalation so the runner reads the deeper
        // target on its next `fetchJob(byId:)` (and a crash mid-
        // promotion resumes against the deeper target rather than the
        // stale tier value).
        do {
            try await store.updateJobDesiredCoverage(
                jobId: opportunity.jobId,
                desiredCoverageSec: opportunity.escalatedDesiredCoverageSec
            )
        } catch {
            logger.warning("Acoustic promotion: updateJobDesiredCoverage threw for job \(opportunity.jobId): \(error)")
            await sleepOrWake(seconds: Self.idlePollSeconds)
            return
        }

        // Re-fetch the row so the dispatch reflects the persisted
        // escalation. A `nil` here is unusual (the row existed at
        // `currentAcousticPromotionOpportunity` evaluation moments ago)
        // but bail safely if the row was concurrently superseded.
        let job: AnalysisJob
        do {
            guard let refreshed = try await store.fetchJob(byId: opportunity.jobId) else {
                logger.warning("Acoustic promotion: job \(opportunity.jobId) disappeared after escalation")
                return
            }
            job = refreshed
        } catch {
            logger.warning("Acoustic promotion: fetchJob threw for \(opportunity.jobId): \(error)")
            return
        }

        // Lane-cap and admission-gate checks mirror `runLoop()` and
        // `dispatchForegroundCatchup`. We consult both because
        // promotion should never bust the Now-cap or skip the bnrs
        // gate.
        guard canAdmit(job: job) else {
            logger.info("Acoustic promotion: lane \(String(describing: job.schedulerLane), privacy: .public) at capacity; deferring")
            await sleepOrWake(seconds: Self.idlePollSeconds)
            return
        }

        let gateDecision = await evaluateAdmissionGate(for: job)
        if case .reject(let cause) = gateDecision {
            logger.info("Acoustic promotion: AdmissionGate rejected job \(job.jobId) cause=\(cause.rawValue, privacy: .public)")
            await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
            return
        }

        PreAnalysisInstrumentation.logAcousticPromotion(
            episodeId: opportunity.episodeId,
            jobId: opportunity.jobId,
            priorCoverageSec: opportunity.priorDesiredCoverageSec,
            escalatedCoverageSec: opportunity.escalatedDesiredCoverageSec,
            windowStartSec: opportunity.triggerWindowStartSec,
            windowEndSec: opportunity.triggerWindowEndSec,
            score: opportunity.triggerWindowScore
        )

        didStart(job: job)
        await processJob(job, cascadeWindow: nil)
        didFinish(job: job)
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
            // playhead-gyvb.1: route the asset-resolution failure
            // through `commitOutcomeArm(... incrementAttempt: true ...)`
            // so attemptCount climbs toward `maxAttemptCount`. Without
            // the increment, an asset-resolution error that recurs
            // every cycle (e.g. a SQLite-side fault while inserting
            // the placeholder asset row) would cycle forever — same
            // failure shape as the cancel-path bug fixed alongside
            // this arm. On `maxAttemptsReached`, supersede so the
            // slot frees for queued work behind it.
            let attempts = job.attemptCount + 1
            if attempts >= Self.maxAttemptCount {
                await commitOutcomeArm(
                    "assetResolution.supersede",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        incrementAttempt: true,
                        stateUpdate: .init(
                            state: "superseded",
                            nextEligibleAt: nil,
                            lastErrorCode: "maxAttemptsReached:assetResolution: \(error)"
                        )
                    )
                )
                logger.warning("Job \(job.jobId) abandoned after \(attempts) attempts: assetResolution: \(error)")
            } else {
                let backoff = Self.exponentialBackoffSeconds(attempt: attempts)
                await commitOutcomeArm(
                    "assetResolution.requeue",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        incrementAttempt: true,
                        stateUpdate: .init(
                            state: "failed",
                            nextEligibleAt: clock().timeIntervalSince1970 + backoff,
                            lastErrorCode: "assetResolution: \(error)"
                        )
                    )
                )
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
                //
                // Intentionally NOT bumping `attemptCount` here: this
                // arm fires only when the cancel arrived BEFORE
                // `runTask` started, so no decode work was performed.
                // Preserving the attempt budget keeps the job's
                // remaining retries available for actual work attempts
                // — bumping on every preempt-before-start would burn
                // through `maxAttemptCount` from churn rather than from
                // genuine failures.
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
                // Bump `attemptCount`: repeated mid-decode cancellation
                // must eventually reach `maxAttemptsReached` so a poisoned
                // job supersedes and frees the lease slot. Terminal branch
                // also drops `nextEligibleAt` to make the job non-dispatchable.
                let attempts = job.attemptCount + 1
                if attempts >= Self.maxAttemptCount {
                    await commitOutcomeArm(
                        "cancelCatch.supersede",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            incrementAttempt: true,
                            stateUpdate: .init(
                                state: "superseded",
                                nextEligibleAt: nil,
                                lastErrorCode: "maxAttemptsReached:cancelMidRun"
                            )
                        )
                    )
                    logger.warning("Job \(job.jobId) abandoned after \(attempts) attempts: cancelMidRun")
                } else {
                    // Mirror the `.failed.requeue` arm and apply
                    // exponential backoff: a user pause/play loop on a
                    // poison-content episode used to hammer through
                    // `maxAttemptCount` instantly because the requeue
                    // dropped `nextEligibleAt`. With backoff, the Nth
                    // cancel pushes `nextEligibleAt` to
                    // `now + min(2^N * 60, 3600)s`, matching how the
                    // `.failed` arm paces unhealthy jobs and giving
                    // queued work behind it a chance to dispatch.
                    let backoff = Self.exponentialBackoffSeconds(attempt: attempts)
                    let nextEligible = clock().timeIntervalSince1970 + backoff
                    await commitOutcomeArm(
                        "cancelCatch.revertQueued",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            incrementAttempt: true,
                            stateUpdate: .init(
                                state: "queued",
                                nextEligibleAt: nextEligible,
                                lastErrorCode: nil
                            )
                        )
                    )
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

            // playhead-5uvz.3 (Gap-3): each outcome arm now lands as a
            // single `BEGIN IMMEDIATE..COMMIT` transaction via
            // `commitOutcomeArm`. Progress + state + lease release
            // commit or roll back together, so a process kill mid-arm
            // can no longer leave the row at `state='running'` with
            // progress recorded but no terminal mark. The single
            // `lostOwnership` check below is sufficient because each
            // arm is now one atomic await — the renewer cannot wedge a
            // partial state across an internal suspension point.
            if lostOwnership {
                logger.warning("Skipping outcome writes for job \(job.jobId): lease reclaimed by orphan recovery")
                return
            }

            let progress = AnalysisStore.ProcessJobOutcomeArmCommit.ProgressUpdate(
                featureCoverageSec: outcome.featureCoverageSec,
                transcriptCoverageSec: outcome.transcriptCoverageSec,
                cueCoverageSec: outcome.cueCoverageSec
            )

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
                    await commitOutcomeArm(
                        "tierAdvance",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            progress: progress,
                            insertNextJob: nextJob,
                            stateUpdate: .init(state: "complete", nextEligibleAt: nil, lastErrorCode: nil)
                        )
                    )
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Tier advancement: \(job.desiredCoverageSec)s -> \(nextCoverage)s for episode \(job.episodeId)")
                } else {
                    await commitOutcomeArm(
                        "allTiersDone",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            progress: progress,
                            stateUpdate: .init(state: "complete", nextEligibleAt: nil, lastErrorCode: nil)
                        )
                    )
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Job \(job.jobId) complete (all tiers done)")
                }

            case .reachedTarget:
                // Re-queue, but with a guard against infinite loops for short episodes
                // or episodes that can never reach the desired coverage.
                if !Self.shouldRetryCoverageInsufficient(job: job, outcome: outcome) {
                    await commitOutcomeArm(
                        "coverageInsufficient.noProgress",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            progress: progress,
                            stateUpdate: .init(
                                state: "complete",
                                nextEligibleAt: nil,
                                lastErrorCode: "coverageInsufficient:noProgress"
                            )
                        )
                    )
                    logger.info("Job \(job.jobId) marked complete after no-progress pass (coverage insufficient)")
                } else {
                    // playhead-5uvz.3: predict the post-increment value
                    // from the in-memory `job.attemptCount`. We hold the
                    // lease, so no concurrent writer races us; this
                    // matches the existing fallback (see prior
                    // `updated?.attemptCount ?? job.attemptCount + 1`)
                    // and lets the increment + terminal write commit
                    // atomically without a mid-arm fetch.
                    let attempts = job.attemptCount + 1
                    if attempts >= Self.maxAttemptCount {
                        await commitOutcomeArm(
                            "coverageInsufficient.maxAttempts",
                            AnalysisStore.ProcessJobOutcomeArmCommit(
                                jobId: job.jobId,
                                progress: progress,
                                incrementAttempt: true,
                                stateUpdate: .init(
                                    state: "complete",
                                    nextEligibleAt: nil,
                                    lastErrorCode: "maxAttemptsReached:coverageInsufficient"
                                )
                            )
                        )
                        logger.info("Job \(job.jobId) marked complete after max attempts (coverage insufficient)")
                    } else {
                        // Backoff before next attempt: without a
                        // gap, the scheduler loop wakes
                        // immediately, picks the same job, and
                        // burns the full decode pipeline N more
                        // times in a tight loop. Match the
                        // `.failed` exponential backoff so
                        // attempt-N waits min(2^N * 60, 3600) s.
                        let attemptIndex = Double(attempts)
                        let backoff = min(pow(2.0, attemptIndex) * 60, 3600)
                        await commitOutcomeArm(
                            "coverageInsufficient.requeue",
                            AnalysisStore.ProcessJobOutcomeArmCommit(
                                jobId: job.jobId,
                                progress: progress,
                                incrementAttempt: true,
                                stateUpdate: .init(
                                    state: "queued",
                                    nextEligibleAt: clock().timeIntervalSince1970 + backoff,
                                    lastErrorCode: nil
                                )
                            )
                        )
                    }
                }

            case .blockedByModel:
                let nextEligible = clock().timeIntervalSince1970 + 300
                await commitOutcomeArm(
                    "blockedByModel",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        progress: progress,
                        stateUpdate: .init(
                            state: "blocked:modelUnavailable",
                            nextEligibleAt: nextEligible,
                            lastErrorCode: nil
                        )
                    )
                )
                logger.info("Job \(job.jobId) blocked: model unavailable, retry in 300s")

            case .pausedForThermal, .memoryPressure:
                let nextEligible = clock().timeIntervalSince1970 + 30
                await commitOutcomeArm(
                    "pausedThermalOrMemory",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        progress: progress,
                        stateUpdate: .init(
                            state: "paused",
                            nextEligibleAt: nextEligible,
                            lastErrorCode: nil
                        )
                    )
                )
                logger.info("Job \(job.jobId) paused for thermal/memory, retry in 30s")

            case .failed(let reason):
                let attempts = job.attemptCount + 1
                if attempts >= Self.maxAttemptCount {
                    await commitOutcomeArm(
                        "failed.supersede",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            progress: progress,
                            incrementAttempt: true,
                            stateUpdate: .init(
                                state: "superseded",
                                nextEligibleAt: nil,
                                lastErrorCode: "maxAttemptsReached:\(reason)"
                            )
                        )
                    )
                    logger.warning("Job \(job.jobId) abandoned after \(attempts) attempts: \(reason)")
                } else {
                    let backoff = Self.exponentialBackoffSeconds(attempt: attempts)
                    let nextEligible = clock().timeIntervalSince1970 + backoff
                    await commitOutcomeArm(
                        "failed.requeue",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            progress: progress,
                            incrementAttempt: true,
                            stateUpdate: .init(
                                state: "failed",
                                nextEligibleAt: nextEligible,
                                lastErrorCode: reason
                            )
                        )
                    )
                    logger.warning("Job \(job.jobId) failed: \(reason), attempt \(attempts), backoff \(backoff)s")
                }

            case .backgroundExpired:
                await commitOutcomeArm(
                    "backgroundExpired.requeue",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        progress: progress,
                        stateUpdate: .init(state: "queued", nextEligibleAt: nil, lastErrorCode: nil)
                    )
                )
                logger.info("Job \(job.jobId) background expired, requeued")

            case .cancelledByPlayback:
                await commitOutcomeArm(
                    "cancelledByPlayback.requeue",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        progress: progress,
                        stateUpdate: .init(state: "queued", nextEligibleAt: nil, lastErrorCode: nil)
                    )
                )
                logger.info("Job \(job.jobId) cancelled by playback, requeued")

            case .preempted:
                await commitOutcomeArm(
                    "preempted.requeue",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        progress: progress,
                        stateUpdate: .init(state: "queued", nextEligibleAt: nil, lastErrorCode: nil)
                    )
                )
                logger.info("Job \(job.jobId) preempted by higher-lane work, requeued")
            }
        } catch {
            PreAnalysisInstrumentation.endJobDuration(jobSignpost)
            if lostOwnership {
                logger.warning("Skipping failure cleanup writes for job \(job.jobId): lease reclaimed by orphan recovery (error: \(error))")
                return
            }
            // playhead-5uvz.3 (Gap-3): the outer-catch path also commits
            // as one transaction so the increment + terminal mark +
            // lease release roll back together if any one fails.
            let attempts = job.attemptCount + 1
            if attempts >= Self.maxAttemptCount {
                await commitOutcomeArm(
                    "outerCatch.supersede",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        incrementAttempt: true,
                        stateUpdate: .init(
                            state: "superseded",
                            nextEligibleAt: nil,
                            lastErrorCode: "maxAttemptsReached:\(error.localizedDescription)"
                        )
                    )
                )
            } else {
                let backoff = Self.exponentialBackoffSeconds(attempt: attempts)
                let nextEligible = clock().timeIntervalSince1970 + backoff
                await commitOutcomeArm(
                    "outerCatch.requeue",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        incrementAttempt: true,
                        stateUpdate: .init(
                            state: "failed",
                            nextEligibleAt: nextEligible,
                            lastErrorCode: error.localizedDescription
                        )
                    )
                )
            }
            logger.error("Job \(job.jobId) threw: \(error)")
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

    /// playhead-5uvz.3 (Gap-3): submits an outcome-arm's writes to the
    /// store as a single `BEGIN IMMEDIATE..COMMIT` transaction via
    /// `AnalysisStore.commitProcessJobOutcomeArm`. Mirrors
    /// `writeIfStillOwned` for the lostOwnership gate and the
    /// catch-and-log semantics. If any inner write throws, the entire
    /// transaction rolls back — so progress + state + lease release
    /// commit or roll back as one unit. Closes the Gap-3 crash window
    /// where a process kill between separate transactions could leave
    /// the `analysis_jobs` row at `state='running'` with progress
    /// recorded but no terminal mark.
    ///
    /// Arms that route through this helper (review-followup csp / M3):
    ///   1. `assetResolution.{supersede,requeue}` — pre-runner asset
    ///      lookup failed; row is requeued or terminated.
    ///   2. `cancelCatch.{supersede,revertQueued}` — `CancellationError`
    ///      caught with `lostOwnership == false` (mid-decode cancel).
    ///   3. `tierAdvance` / `allTiersDone` — `.reachedTarget` outcome
    ///      with coverage met.
    ///   4. `coverageInsufficient.{noProgress,maxAttempts,requeue}` —
    ///      `.reachedTarget` outcome that did not actually clear the
    ///      desired tier.
    ///   5. `blockedByModel` — `.blockedByModel` outcome.
    ///   6. `pausedThermalOrMemory` — `.pausedForThermal` /
    ///      `.memoryPressure` outcome.
    ///   7. `failed.{supersede,requeue}` /
    ///      `backgroundExpired.requeue` / `cancelledByPlayback.requeue`
    ///      / `preempted.requeue` — explicit non-fatal outcomes.
    ///   8. `outerCatch.{supersede,requeue}` — outer-try catch arm
    ///      that catches anything the runner rethrew.
    ///
    /// **Lease leakage invariant.**
    /// `ProcessJobOutcomeArmCommit.releaseLease` defaults to `true`,
    /// so every arm's transaction terminates with `releaseLease(jobId:)`
    /// unless the call site explicitly sets it to `false`. Each arm
    /// MUST honor that default unless it has a specific, documented
    /// reason to keep the lease (today, no arm above sets
    /// `releaseLease: false`). A leaked lease is silently corrosive:
    /// the row stays invisible to the dispatcher until the lease
    /// expires (300s), and the lane counter never decrements, so a
    /// repeating leak burns out lane capacity over the session.
    private func commitOutcomeArm(
        _ what: String,
        _ commit: AnalysisStore.ProcessJobOutcomeArmCommit
    ) async {
        guard !lostOwnership else { return }
        do {
            try await store.commitProcessJobOutcomeArm(commit)
        } catch is CancellationError {
            logger.warning("Outcome arm [\(what)] cancelled (likely lease reclaim mid-transaction)")
        } catch {
            logger.error("Failed outcome arm [\(what)]: \(error)")
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
        // playhead-gyvb.2: consume any stashed duration probed at
        // `enqueue(...)` time. Same lazy-backfill semantics as the title
        // stash — a missing entry simply means the file wasn't on disk
        // (or wasn't an audio container) at enqueue time, in which case
        // the column stays nil until spool / the launch-time backfill
        // sweep heals it.
        let stashedDuration = pendingProbedEpisodeDurations.removeValue(forKey: job.episodeId)
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
            episodeDurationSec: stashedDuration,
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
