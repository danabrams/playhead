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
/// best-effort and keeps it out of the main dispatch path; the handler must
/// own its own backoff for empty shadow backlogs.
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

// `TransportStatusProviding`, `WifiTransportStatusProvider`, and the
// production `LiveTransportStatusProvider` (NWPathMonitor-backed) live
// in TransportStatusProviding.swift in this directory.

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

    /// playhead-ngev (review r1): how long a run displaced by playback waits
    /// before it is eligible again.
    ///
    /// The FIRST RUNG of the ladder above, taken as a flat floor rather than a
    /// ladder position — an interruption never escalates, because it never
    /// spends an attempt and there is nothing to escalate. It exists only to
    /// stop a job that keeps colliding with a live engine owner from requeueing
    /// in a hot loop; the successor's coverage is durable, so one minute later
    /// the retry usually finds the work already done.
    private static let interruptedRequeueDelaySeconds = exponentialBackoffSeconds(attempt: 0)

    private let store: AnalysisStore
    private let jobRunner: AnalysisJobRunner
    private let capabilitiesService: any CapabilitiesProviding
    private let downloadManager: any DownloadProviding
    private let batteryProvider: any BatteryStateProviding
    /// playhead-bnrs / playhead-ml96: transport-status provider
    /// consumed by the admission gate. Defaults to
    /// `LiveTransportStatusProvider` (NWPathMonitor + user pref) so the
    /// admission gate actually sees cellular / unreachable in
    /// production. Tests inject `StubTransportStatusProvider`.
    private let transportStatusProvider: any TransportStatusProviding
    /// playhead-1iq1: storage-budget snapshot provider consumed by the
    /// admission gate. Production wiring passes the live `StorageBudget`
    /// actor (which conforms to `StorageBudgetSnapshotting` via an
    /// extension), making the storage axis a real pre-admission gate;
    /// the previous `StorageSnapshot.plentiful` literal is gone. Tests
    /// inject a stub that drives the axis through the full truth table.
    private let storageBudgetSnapshotter: any StorageBudgetSnapshotting
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

    /// playhead-ewag: lowest `AnalysisJob.priority` that maps to `.now`.
    ///
    /// Extracted from the literal that used to live only inside
    /// ``AnalysisJob/schedulerLane`` because the store's eligibility SELECT now
    /// needs the SAME number: `fetchNextEligibleJob` carves Now-lane rows out
    /// of the `deferredWorkAllowed` gate by comparing `priority` against this
    /// floor in SQL. Two copies of "20" — one in Swift, one in a query string —
    /// is precisely the drift that lets a lane silently stop existing. Keep
    /// this the ONLY definition.
    static let nowLanePriorityFloor = 20

    /// playhead-ewag: lowest `AnalysisJob.priority` that maps to `.soon`.
    /// Anything below is `.background`. Same single-definition rule as
    /// ``nowLanePriorityFloor``.
    static let soonLanePriorityFloor = 1

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

    /// playhead-glo9: conservative "hot path comfortably ahead" runway
    /// (seconds) gating the opportunistic backlog-drain relaxation. Set
    /// to 2× the default catch-up trigger
    /// (`PlayheadCatchupPolicy.default.triggerThresholdSec` == 60 s) so
    /// other-episode backlog drain begins only when the active episode
    /// has a clear margin of transcript runway beyond the catch-up
    /// trigger distance. The 2× gap is a deliberate hysteresis band: in
    /// `[triggerThreshold, 2×triggerThreshold)` the scheduler neither
    /// drains other-episode work nor fires catch-up — it keeps the
    /// pre-glo9 block — which avoids thrashing between backlog drain and
    /// hot-path catch-up right at the boundary. See
    /// `activeEpisodeHotPathCaughtUp`.
    static let opportunisticDrainRunwaySec: TimeInterval = 120

    /// Admission decision the scheduler derives from the current QualityProfile
    /// and applies to every loop iteration. Consolidates thermal/battery/
    /// low-power gating into a single surface — see `QualityProfile.derive`.
    ///
    /// **Foreground-aggressive relaxation surface** (review-followup
    /// csp / L4). When the scheduler is in foreground-aggressive mode
    /// (see `isForegroundAggressiveMode()`), `relaxedPolicy(for:profile:foregroundAggressive:)`
    /// rewrites the baseline `SchedulerPolicy` derived from the
    /// QualityProfile to widen Soon-lane admission. The relaxation is
    /// deliberately narrow:
    ///
    /// - Triggers ONLY when `profile == .serious`. `.nominal`,
    ///   `.fair`, and `.critical` pass through untouched. In
    ///   particular `.critical` is never relaxed because
    ///   `pauseAllWork` is dominant in every state and the device is
    ///   too hot to pile on more work safely.
    /// - Reopens ONLY `allowSoonLane`. `allowBackgroundLane` and
    ///   `pauseAllWork` keep the baseline policy's values, so deep
    ///   T2 backfill stays gated and global pause is honored.
    /// - `sliceFraction` is forwarded unchanged — slice sizing is
    ///   the QualityProfile's responsibility and not part of the
    ///   relaxation surface.
    ///
    /// Anyone widening this relaxation MUST keep the
    /// foreground-aggressive precondition and the
    /// `pauseAllWork`-dominant safety property intact.
    struct LaneAdmission: Sendable, Equatable {
        let qualityProfile: QualityProfile
        let policy: QualityProfile.SchedulerPolicy

        /// playhead-ewag: WHY this profile is throttled, in the taxonomy the
        /// user-facing surface already speaks.
        ///
        /// `QualityProfile` is a severity, not a cause: `.fair` is reached
        /// either by a warm SoC or by Low Power Mode or by a low battery off
        /// the cord, and those produce three different honest sentences
        /// ("phone is too hot" vs "low battery"). Telling a user their phone
        /// is hot when the truth is Low Power Mode is the same defect this
        /// bead is about — a value read as something it does not measure — so
        /// the cause is carried alongside the severity rather than guessed at
        /// the surface.
        ///
        /// Defaulted to `.thermal` so every existing construction site (tests
        /// that build a profile/policy pair directly) is unchanged; the live
        /// value is computed in ``currentLaneAdmission()``, which is the only
        /// place that has the battery and low-power reads in hand.
        let throttleCause: InternalMissCause

        init(
            qualityProfile: QualityProfile,
            policy: QualityProfile.SchedulerPolicy,
            throttleCause: InternalMissCause = .thermal
        ) {
            self.qualityProfile = qualityProfile
            self.policy = policy
            self.throttleCause = throttleCause
        }

        /// Whether any work at all may run. Mirrors `policy.pauseAllWork` for
        /// readability at call sites.
        var pauseAllWork: Bool { policy.pauseAllWork }

        /// Whether a job in the given `SchedulerLane` is admitted under the
        /// current QualityProfile. T0 (playback) jobs are never gated here —
        /// the store selects them on the hot-path criteria; only
        /// `pauseAllWork` can stop them.
        ///
        /// **playhead-ewag.** This replaced a coverage-DEPTH dual
        /// (`desiredCoverageSec >= t2DepthSeconds ⇒ Background`) that was the
        /// scheduler's only lane classifier for months while this method sat
        /// unused. The depth test was written when every enqueue carried
        /// `desiredCoverageSec = 90` and the tier ladder escalated later, so
        /// depth was a fair proxy for the cost of the NEXT dispatch. Once
        /// playhead-3xtw made the explicit-download path stamp the FULL
        /// episode duration at enqueue, that proxy inverted: a user's very
        /// first download pass was classified deep-Background before one
        /// second of audio had been read, and `allowBackgroundLane` is true
        /// only at `.nominal`. Five user downloads sat `queued` with
        /// `attemptCount = 0` on a foregrounded charging device, forever.
        /// `desiredCoverageSec` names the eventual coverage TARGET; it never
        /// named the cost of a dispatch. Gate on the lane, which is derived
        /// from priority — i.e. from who asked for the work.
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

    // MARK: - Lane gate (playhead-ewag)

    /// What the post-selection lane gate decided about one candidate job.
    ///
    /// Three outcomes rather than a `Bool` because "held" and "admitted at a
    /// reduced depth" are genuinely different states that must be observable
    /// apart: the first is a wait the user needs to be told about, the second
    /// is the bounded escape hatch that stops a wait from being unbounded.
    enum LaneGateOutcome: Sendable, Equatable {
        /// The job's lane is open under the current QualityProfile. Dispatch
        /// the job exactly as enqueued.
        case admit

        /// The job's lane is CLOSED, but the job has been queued past
        /// ``LaneGatePolicy/progressFloorAfterSec`` without ever being
        /// attempted. Dispatch ONE Soon-depth slice — `coverageCapSec` is the
        /// coverage target for this pass only and is deliberately NOT
        /// persisted, so the job's real target survives.
        case admitProgressFloor(coverageCapSec: Double)

        /// The job's lane is closed. `reason` is written durably to the job
        /// row (`lastRejectReason`) so the hold is queryable rather than
        /// silent, and is stable enough to group on.
        case hold(reason: String)
    }

    /// playhead-ewag: tunables for the lane gate's progress floor.
    ///
    /// The floor exists because Option A fixes the lane a user download lands
    /// in but does not, on its own, make "queued forever" impossible for every
    /// other lane. A `.background` row on a device that never returns to
    /// `.nominal` would still wait indefinitely, and indefinitely is not a
    /// state this pipeline is allowed to have.
    enum LaneGatePolicy {
        /// How long a job may sit queued, never once attempted, before the
        /// gate admits one bounded slice despite its lane being closed.
        ///
        /// 10 minutes: long enough that a transient thermal excursion (which
        /// clears in seconds-to-minutes) resolves on its own and the floor
        /// never fires, short enough that a user who downloaded an episode and
        /// left the app open sees progress inside one sitting. The field
        /// incident's five jobs sat untouched for 20+ minutes.
        static let progressFloorAfterSec: TimeInterval = 600
    }

    /// playhead-ewag: the post-selection admission decision for one candidate
    /// job, as a pure function of (lane, profile, how long it has waited).
    ///
    /// Pure and `static` on purpose: this is the predicate that was wrong for
    /// months, and the only way to keep it honest is to be able to assert on
    /// the whole truth table without standing up a scheduler, a store, and a
    /// simulated thermal ramp.
    ///
    /// Ordering of the three rules is load-bearing:
    ///   1. `pauseAllWork` (thermal `.critical`) dominates everything,
    ///      including the progress floor. A hot enough device does no work,
    ///      full stop — the floor is a liveness bound, not a licence to cook
    ///      the phone.
    ///   2. An open lane admits at full depth.
    ///   3. Only then may the floor admit a closed lane, and only for a job
    ///      that has NEVER been attempted (`attemptCount == 0`). That
    ///      condition is what makes the floor one-shot without any extra
    ///      bookkeeping: every dispatch outcome arm moves `attemptCount` off
    ///      zero, so a job gets at most one floor slice and then waits for its
    ///      lane to open like everything else.
    ///
    /// - Parameters:
    ///   - queuedForSec: wall-clock seconds since the row was created. Clamped
    ///     at zero by the caller's `max` so a clock skew cannot fabricate an
    ///     ancient job.
    ///   - desiredCoverageSec: the job's real target, used only to cap the
    ///     floor slice — a floor pass never asks for MORE than the job wants.
    ///   - t1DepthSeconds: the Soon-lane depth. The floor slice is capped here
    ///     so a floor dispatch costs a Soon-lane pass, never a Background one.
    static func evaluateLaneGate(
        lane: SchedulerLane,
        admission: LaneAdmission,
        queuedForSec: TimeInterval,
        attemptCount: Int,
        desiredCoverageSec: Double,
        t1DepthSeconds: Double,
        progressFloorAfterSec: TimeInterval = LaneGatePolicy.progressFloorAfterSec
    ) -> LaneGateOutcome {
        let reason = laneGateRejectReason(profile: admission.qualityProfile)
        if admission.pauseAllWork {
            return .hold(reason: reason)
        }
        if admission.allows(lane: lane) {
            return .admit
        }
        if attemptCount == 0, queuedForSec >= progressFloorAfterSec {
            return .admitProgressFloor(
                coverageCapSec: min(desiredCoverageSec, t1DepthSeconds)
            )
        }
        return .hold(reason: reason)
    }

    /// playhead-ewag: the durable advisory written to
    /// `analysis_jobs.lastRejectReason` when the lane gate holds a job.
    ///
    /// Shaped `laneGate:<profile>` — the gate that fired, then the condition
    /// that closed it. Named `laneGate` and not `depthGate` because after this
    /// bead the gate really does read the lane; a reason string that named the
    /// old depth test would be one more value describing something it does not
    /// measure, which is the defect class this bead exists to close.
    static func laneGateRejectReason(profile: QualityProfile) -> String {
        "laneGate:\(profile.rawValue)"
    }

    /// playhead-ewag: a job the lane gate is currently holding, with the
    /// CONSECUTIVE skip count.
    ///
    /// Consecutive, not lifetime. A lifetime counter cannot distinguish "this
    /// job has been stuck since the app launched" from "this job has been
    /// dispatched forty times and was skipped once each time it came back
    /// around" — and only the first is a stall. The record is dropped the
    /// moment the job dispatches, so a non-nil record always means "held right
    /// now, and has been for `consecutiveSkips` passes".
    struct LaneHoldRecord: Sendable, Equatable {
        let jobId: String
        let episodeId: String
        let lane: SchedulerLane
        let qualityProfile: QualityProfile
        /// Why the profile is throttled, in the taxonomy the Activity surface
        /// renders. Carried from ``LaneAdmission/throttleCause`` so the copy
        /// the user reads names the real constraint.
        let cause: InternalMissCause
        /// Number of back-to-back gate passes that have held this job with no
        /// intervening dispatch. Starts at 1.
        let consecutiveSkips: Int
        /// Wall-clock of the FIRST hold in the current consecutive run.
        let firstHeldAt: TimeInterval
        /// Wall-clock of the most recent hold.
        let lastHeldAt: TimeInterval
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

    /// playhead-beh3 (Phase 3 deliverable 5): adaptive Welford+EWMA
    /// estimator for per-device-class slice/grant-window sizing. Held
    /// behind the `LearnedDeviceProfileProviding` protocol seam so the
    /// scheduler does not have to know about SwiftData. Defaults to
    /// `NoOpLearnedDeviceProfileProvider`, which short-circuits every
    /// call back to the seed — that is the byte-identical-to-today
    /// rollback path. The feature flag
    /// (`PreAnalysisConfig.useAdaptiveDeviceProfile`) gates whether the
    /// scheduler even queries the provider at the call site, so a flag-
    /// off run never touches the estimator state.
    ///
    /// R13 fix: switched from `let` to `var` so the production runtime
    /// can install a `SwiftDataLearnedDeviceProfileStore` AFTER
    /// `AnalysisWorkScheduler.init` (init runs synchronously before the
    /// `ModelContainer` is available — see `PlayheadRuntime.init` notes).
    /// Without the setter the production scheduler permanently held the
    /// No-Op provider and the `useAdaptiveDeviceProfile` flag had no
    /// effect: `recordObservation` was dropped, `resolvedDeviceProfile`
    /// returned the seed verbatim, and the SwiftData
    /// `LearnedDeviceProfile` table stayed empty even with the flag ON
    /// — the bead's "Adaptive estimator runs in production, gated by the
    /// flag" acceptance criterion was silently unmet. Mutation is safe
    /// because the field lives on an `actor` so reads + writes serialize.
    private var learnedDeviceProfileProvider: any LearnedDeviceProfileProviding

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

    /// playhead-y8f3: the `lastErrorCode` prefix every attempt-cap terminal
    /// carries, and the ONLY thing that tells a cap-out apart from a genuine
    /// supersession.
    ///
    /// `state = 'superseded'` means two different things in this table, which is
    /// why the one attempt-reset path (`AnalysisStore.requeueOrphanedLease`)
    /// deliberately preserves it. Genuine supersession — a stale
    /// `analysisVersion` (`AnalysisJobReconciler.supersedeStaleVersions`), a
    /// deleted episode, cached audio that no longer matches
    /// (`staleFingerprint:cachedAudioMismatch`) — retires a row whose
    /// replacement either already exists or must never exist. A cap-out is the
    /// opposite: the work is still wanted and nothing replaced it.
    ///
    /// Reader and writers share this constant so the discriminator cannot drift
    /// away from the strings the four supersede arms actually write. Note the
    /// FIFTH writer, `coverageInsufficient.maxAttempts`, terminates
    /// `state = 'complete'` (playhead-gqx4's degraded terminal) and so is
    /// excluded by the state check in ``isAttemptCapTerminal(_:)`` rather than
    /// by the prefix.
    static let maxAttemptsReachedPrefix = "maxAttemptsReached:"

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

    /// playhead-ewag: jobs the lane gate is currently holding, keyed by jobId.
    ///
    /// In-memory rather than persisted because the quantity is CONSECUTIVE
    /// skips, and "consecutive" is only meaningful within one scheduler
    /// lifetime — a count that survived a relaunch would claim continuity
    /// across a gap in which nothing was even asked. The durable half of the
    /// record (that a hold happened, and why) goes to
    /// `analysis_jobs.lastRejectReason` / `lastRejectAt` on every hold.
    ///
    /// An entry is removed the instant its job dispatches (see
    /// ``clearLaneHold(jobId:)``), so a present entry always means "held right
    /// now".
    private var laneHolds: [String: LaneHoldRecord] = [:]

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

    /// True while an idle-tick shadow Lane B task is still draining. The
    /// scheduler fires Lane B as an unstructured task so user-visible jobs can
    /// start without waiting for shadow FM work, but it must not enqueue an
    /// unbounded pile of identical Lane B probes while the previous probe is
    /// still in SQLite or FoundationModels.
    private var shadowLaneTickInFlight = false

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
    /// The probe runs once per download against the cached file; the
    /// result is written to an existing asset row immediately and
    /// otherwise stashed here so the new asset row created by
    /// `resolveAnalysisAssetId` carries the measured duration without
    /// waiting for the spool/decode pass.
    ///
    /// Keyed by episode plus source fingerprint so a stale canonical-SHA
    /// job cannot consume or clear the current file's duration after a
    /// feed correction / re-download. Best-effort: a probe failure simply
    /// leaves the dictionary entry absent.
    private var pendingProbedEpisodeDurations: [DurationStashKey: Double] = [:]

    /// playhead-3xtw: episodes the user explicitly asked to prepare via
    /// the on-demand "Download & Analyze" control. The next `enqueue`
    /// for an episode in this set is stamped at the user-intent (`.now`)
    /// lane (priority 20) so it preempts starving background work — the
    /// whole point of the foreground on-demand control. The entry is
    /// consumed (removed) by that enqueue — but only once it has actually
    /// been *served*: it is a one-shot promotion of the imminent analysis
    /// job, not a standing flag, and not a token spent on nothing.
    ///
    /// playhead-kanf: recording intent for an episode that ALREADY has a
    /// queued job now promotes that row in place (`insertJob` is
    /// `INSERT OR IGNORE` on `workKey`, so the freshly-minted priority-20
    /// struct is otherwise discarded whole). That is the starving-background-job
    /// case the control exists for. A leased / running row is deliberately NOT
    /// promoted — re-ranking work a worker holds is a lease/epoch decision made
    /// elsewhere — and in that case the flag is RETAINED rather than burned, so
    /// a second tap, or the next enqueue once the row returns to `queued`, can
    /// still honour it.
    private var pendingUserIntentEpisodes: Set<String> = []
    /// playhead-3xtw: requested analysis coverage (the episode duration)
    /// for a user-intent episode, applied when the enqueue omits an
    /// explicit `desiredCoverage`. Consumed alongside
    /// `pendingUserIntentEpisodes`.
    private var pendingUserIntentCoverage: [String: Double] = [:]
    /// Monotonic ownership epoch for enqueue work derived from a cached
    /// download. Cache removal increments the episode's value before deleting
    /// scheduler rows; an enqueue suspended in AnalysisStore must observe the
    /// mismatch and clean up instead of resurrecting the removed job.
    private var downloadRetirementGenerationByEpisode:
        [String: UInt64] = [:]
    private var enqueueBarrierForTesting:
        (@Sendable () async -> Void)?

    init(
        store: AnalysisStore,
        jobRunner: AnalysisJobRunner,
        capabilitiesService: any CapabilitiesProviding,
        downloadManager: any DownloadProviding,
        batteryProvider: any BatteryStateProviding = UIDeviceBatteryProvider(),
        transportStatusProvider: any TransportStatusProviding = LiveTransportStatusProvider(),
        storageBudgetSnapshotter: any StorageBudgetSnapshotting = PlentifulStorageBudgetSnapshotter(),
        candidateWindowCascade: CandidateWindowCascade? = nil,
        config: PreAnalysisConfig = .load(),
        clock: @escaping @Sendable () -> Date = { Date() },
        backfillScheduler: (any BackfillScheduling)? = nil,
        catchupPolicy: PlayheadCatchupPolicy = .default,
        acousticPromotionPolicy: AcousticPromotionPolicy = .default,
        learnedDeviceProfileProvider: (any LearnedDeviceProfileProviding)? = nil
    ) {
        self.store = store
        self.jobRunner = jobRunner
        self.capabilitiesService = capabilitiesService
        self.downloadManager = downloadManager
        self.batteryProvider = batteryProvider
        self.transportStatusProvider = transportStatusProvider
        self.storageBudgetSnapshotter = storageBudgetSnapshotter
        self.candidateWindowCascade = candidateWindowCascade
        self.config = config
        self.clock = clock
        self.backfillScheduler = backfillScheduler
        self.catchupPolicy = catchupPolicy
        self.acousticPromotionPolicy = acousticPromotionPolicy
        // playhead-beh3: adaptive device-profile estimator seam. Nil
        // (or `NoOpLearnedDeviceProfileProvider` injected by the tests
        // that exercise the flag-on path) means "use the seed verbatim"
        // — byte-identical to today's behavior.
        self.learnedDeviceProfileProvider =
            learnedDeviceProfileProvider ?? NoOpLearnedDeviceProfileProvider()
        var continuation: AsyncStream<Void>.Continuation?
        self.wakeStream = AsyncStream<Void>(bufferingPolicy: .bufferingNewest(1)) { continuation = $0 }
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
        let capturedRetirementGeneration =
            downloadRetirementGenerationByEpisode[episodeId, default: 0]
        if let enqueueBarrierForTesting {
            await enqueueBarrierForTesting()
        }
        // playhead-3xtw: a user tapped "Download & Analyze" for this
        // episode (recorded via `markEpisodeUserIntent`). Route its
        // analysis to the user-intent (`.now`) lane — priority >= 20 —
        // so it preempts starving background work. The flag is one-shot:
        // consumed and cleared just below, after the insert attempt, so
        // it applies to exactly the imminent job. `isExplicitDownload`
        // (priority 10, `.soon`) and auto (priority 0, `.background`)
        // are unchanged when no user intent is pending.
        let userInitiated = pendingUserIntentEpisodes.contains(episodeId)
        let priority = userInitiated ? 20 : (isExplicitDownload ? 10 : 0)
        let coverage = desiredCoverage
            ?? pendingUserIntentCoverage[episodeId]
            ?? config.defaultT0DepthSeconds
        let workKey = AnalysisJob.computeWorkKey(
            fingerprint: sourceFingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: "preAnalysis"
        )
        let now = clock().timeIntervalSince1970
        // playhead-gy2s (RC-3, orphan-recovery-routing correctness — NOT the
        // dispatch fix): stamp the row with the current scheduler epoch and a
        // fresh generation ID at enqueue so a later orphan sweep routes the
        // row under the session it was minted in, rather than the epoch-0 /
        // blank sentinel a bare enqueue used to leave. Dispatch eligibility
        // (`fetchNextEligibleJob`) never consulted `schedulerEpoch`, so this
        // changes no dispatch behavior. Safe because the lease acquisition
        // overwrites `generationID` with its own UUID, and nothing branches on
        // the empty-string sentinel (the live-lease check is
        // `leaseOwner IS NOT NULL`).
        let currentEpoch = (try? await store.fetchSchedulerEpoch()) ?? 0
        guard downloadRetirementGenerationByEpisode[
            episodeId,
            default: 0
        ] == capturedRetirementGeneration else {
            clearPendingDownloadState(episodeId: episodeId)
            return
        }
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
            updatedAt: now,
            generationID: UUID().uuidString,
            schedulerEpoch: currentEpoch
        )
        // playhead-kanf: whether this call actually put the episode's work in
        // the user-intent lane. Only a served intent may consume the one-shot
        // flag below. When no intent is pending there is nothing to serve, so
        // the (no-op) clear runs exactly as it always did.
        var userIntentServed = !userInitiated
        do {
            let inserted = try await store.insertJob(job)
            if inserted {
                // The minted row already carries `priority` == 20.
                userIntentServed = true
            }
            if !inserted {
                // playhead-y8f3: `insertJob` is `INSERT OR IGNORE`, so this
                // return value is the ONLY evidence that a re-request did
                // nothing. Discarding it here is how the most visible enqueue
                // path in the app — download completion, and the user tapping
                // Download & Analyze — became silent when an episode's base
                // `workKey` was already held by an attempt-cap terminal.
                //
                // The row is NOT minted here. Re-requesting a swallowed key is
                // `AnalysisJobReconciler`'s step 7, which owns the retry budget
                // and the cooldown, and duplicating the mint at a second site
                // would let a launch-then-tap sequence spend the budget twice.
                // What belongs here is the fact that it happened.
                logger.info(
                    """
                    enqueue_swallowed episode=\(episodeId, privacy: .public) \
                    workKey_collision=true userInitiated=\(userInitiated) \
                    (see playhead-y8f3; a bounded retry is minted by the next reconcile)
                    """
                )
            }
            guard downloadRetirementGenerationByEpisode[
                episodeId,
                default: 0
            ] == capturedRetirementGeneration else {
                try await store.deleteRetiredAnalysisEnqueue(
                    jobId: job.jobId,
                    generationID: job.generationID
                )
                queueWaitStates.removeValue(
                    forKey: job.jobId
                )
                clearPendingDownloadState(episodeId: episodeId)
                return
            }
            if !inserted, userInitiated {
                // playhead-kanf: the key was already held, so the priority-20
                // struct above went nowhere. Promote the EXISTING row instead —
                // this is the whole point of the control, and the only case in
                // which the tap used to do nothing at all. Placed after the
                // retirement guard so a download the user deleted mid-flight is
                // never promoted.
                userIntentServed = await promoteExistingJobToUserIntentLane(
                    episodeId: episodeId,
                    workKey: workKey,
                    priority: priority
                )
            }
            queueWaitStates[job.jobId] = PreAnalysisInstrumentation.beginQueueWait(jobId: job.jobId)
            logger.info("Enqueued job \(job.jobId) for episode \(episodeId), priority=\(priority), coverage=\(coverage)s")
        } catch {
            logger.error("Failed to enqueue job: \(error)")
        }

        // playhead-3xtw: consume the one-shot user-intent flag now that
        // the job (or its work-key-idempotent no-op) has been recorded, so
        // a later auto enqueue for the same episode does not inherit the
        // user-intent lane.
        //
        // playhead-kanf: …but ONLY once the intent was served — the row was
        // minted at the `.now` floor, promoted to it, or already sat at or
        // above it. A tap that could not be honoured (the row is leased,
        // running, paused, terminal, gone, or the store threw) leaves the flag
        // standing so the next enqueue or the next tap can still honour it.
        // Burning a one-shot token on nothing is the defect this bead exists
        // to remove; burning it twice over would just move the defect.
        if userIntentServed {
            pendingUserIntentEpisodes.remove(episodeId)
            pendingUserIntentCoverage[episodeId] = nil
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
        //   - probe returns a positive duration → overwrite the matching
        //     existing asset row, OR stash for the lazy
        //     `resolveAnalysisAssetId` path so the freshly-inserted
        //     row carries the probed value at first insert. If a
        //     canonical SHA job sees an older canonical asset for the
        //     same episode with different bytes, do not mutate the old
        //     asset; the probed duration belongs to the new SHA row.
        if let cachedURL = await downloadManager.cachedFileURL(for: episodeId),
           let probedDuration = await AudioFileDurationProbe.probeDuration(at: cachedURL) {
            let sourceIsCanonicalSHA = CrossUserAnalysisShareKey
                .isCanonicalFullFileSHA(sourceFingerprint)
            let cachedAudioFingerprint = sourceIsCanonicalSHA
                ? cachedAudioCanonicalFingerprint(cachedURL: cachedURL, episodeId: episodeId)
                : nil
            if sourceIsCanonicalSHA, cachedAudioFingerprint == nil {
                logger.warning("Skipping probed duration for canonical-SHA enqueue on episode \(episodeId): cached audio could not be hashed")
            } else if let cachedAudioFingerprint,
                      cachedAudioFingerprint != sourceFingerprint {
                do {
                    let currentAudioFingerprint = await downloadManager.fingerprint(for: episodeId)
                    if let currentAsset = try await store.fetchAssetByEpisodeId(
                        episodeId,
                        assetFingerprint: cachedAudioFingerprint
                    ) {
                        try await store.updateEpisodeDuration(
                            id: currentAsset.id,
                            episodeDurationSec: probedDuration
                        )
                    } else if let weakAsset = try await fetchUpgradeableWeakAssetForCanonicalSHA(
                        episodeId: episodeId,
                        canonicalFingerprint: cachedAudioFingerprint,
                        currentAudioFingerprint: currentAudioFingerprint
                    ) {
                        try await store.updateEpisodeDuration(
                            id: weakAsset.id,
                            episodeDurationSec: probedDuration
                        )
                    } else {
                        pendingProbedEpisodeDurations[
                            Self.durationStashKey(
                                episodeId: episodeId,
                                sourceFingerprint: cachedAudioFingerprint
                            )
                        ] = probedDuration
                    }
                } catch {
                    logger.warning(
                        "Failed to persist probed duration for current cached SHA on stale enqueue for \(episodeId): \(error)"
                    )
                }
            } else {
                do {
                    let currentAudioFingerprint = sourceIsCanonicalSHA
                        ? await downloadManager.fingerprint(for: episodeId)
                        : nil
                    if sourceIsCanonicalSHA,
                       let exactAsset = try await store.fetchAssetByEpisodeId(
                           episodeId,
                           assetFingerprint: sourceFingerprint
                       ) {
                        try await store.updateEpisodeDuration(
                            id: exactAsset.id,
                            episodeDurationSec: probedDuration
                        )
                    } else if sourceIsCanonicalSHA,
                              let weakAsset = try await fetchUpgradeableWeakAssetForCanonicalSHA(
                                  episodeId: episodeId,
                                  canonicalFingerprint: sourceFingerprint,
                                  currentAudioFingerprint: currentAudioFingerprint
                              ) {
                        try await store.updateEpisodeDuration(
                            id: weakAsset.id,
                            episodeDurationSec: probedDuration
                        )
                    } else if let asset = try await store.fetchAssetByEpisodeId(episodeId),
                              !Self.shouldStashProbedDurationForDifferentCanonicalSource(
                                  existing: asset,
                                  sourceFingerprint: sourceFingerprint,
                                  sourceIsCanonicalSHA: sourceIsCanonicalSHA,
                                  currentAudioFingerprint: currentAudioFingerprint
                              ) {
                        try await store.updateEpisodeDuration(
                            id: asset.id,
                            episodeDurationSec: probedDuration
                        )
                    } else {
                        pendingProbedEpisodeDurations[
                            Self.durationStashKey(
                                episodeId: episodeId,
                                sourceFingerprint: sourceFingerprint
                            )
                        ] = probedDuration
                    }
                } catch {
                    logger.warning("Failed to persist probed duration for \(episodeId): \(error)")
                }
            }
        }

        guard downloadRetirementGenerationByEpisode[
            episodeId,
            default: 0
        ] == capturedRetirementGeneration else {
            clearPendingDownloadState(episodeId: episodeId)
            return
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

    /// Retires every scheduler artifact owned by the removed cached download.
    /// The generation increment occurs before the first suspension, closing
    /// both sides of the enqueue/remove race on this actor.
    func retireDownloadAnalysis(
        episodeId: String,
        downloadId: String
    ) async {
        downloadRetirementGenerationByEpisode[
            episodeId,
            default: 0
        ] &+= 1
        clearPendingDownloadState(episodeId: episodeId)
        if currentEpisodeId == episodeId {
            shouldCancelCurrentJob = true
            lostOwnership = true
            leaseRenewalTask?.cancel()
            currentRunningTask?.cancel()
        }
        do {
            let removed = try await store
                .deleteAnalysisJobsForRemovedDownload(
                    episodeId: episodeId,
                    downloadId: downloadId
                )
            for jobID in removed {
                queueWaitStates.removeValue(forKey: jobID)
            }
        } catch {
            logger.error(
                "Failed to retire download analysis for \(episodeId): \(error)"
            )
        }
    }

    /// playhead-kanf: promote the already-queued row occupying `workKey` into
    /// the user-intent (`.now`) lane and wake the loop so it re-ranks.
    ///
    /// - Returns: `true` when the user's intent is now served — the row was
    ///   promoted, or was already at/above the `.now` floor. `false` when it
    ///   could not be honoured, which is the caller's signal to KEEP the
    ///   one-shot flag.
    ///
    /// The refusal cases are the point as much as the promotion is. A leased or
    /// running row is left exactly as it is: re-ranking work a worker holds
    /// would race the lease/epoch stamping, and the deliberate choice (per the
    /// bead) is to decline rather than to make that call here. The user is not
    /// left worse off — a running job is already the best available outcome for
    /// this episode, and the retained flag means the next tap promotes it if it
    /// ever falls back to `queued`.
    private func promoteExistingJobToUserIntentLane(
        episodeId: String,
        workKey: String,
        priority: Int
    ) async -> Bool {
        do {
            let outcome = try await store.promoteQueuedJobToUserIntentLane(
                workKey: workKey,
                priority: priority
            )
            switch outcome {
            case .promoted(let jobId, let fromPriority, let toPriority):
                logger.info(
                    """
                    user_intent_promoted episode=\(episodeId, privacy: .public) \
                    job=\(jobId, privacy: .public) \
                    priority=\(fromPriority)->\(toPriority) (playhead-kanf)
                    """
                )
                // Re-rank now rather than at the next poll: the whole promise of
                // the control is preemption, and `preemptLowerLanes` fires from
                // the dispatch path once the loop selects this `.now` row.
                wakeSchedulerLoop()
                return true
            case .alreadyPromoted(let jobId, let rowPriority):
                logger.info(
                    """
                    user_intent_already_now_lane episode=\(episodeId, privacy: .public) \
                    job=\(jobId, privacy: .public) priority=\(rowPriority) (playhead-kanf)
                    """
                )
                return true
            case .notPromotable(let jobId, let state, let rowPriority, let leased):
                logger.info(
                    """
                    user_intent_promotion_declined episode=\(episodeId, privacy: .public) \
                    job=\(jobId, privacy: .public) state=\(state, privacy: .public) \
                    leased=\(leased) priority=\(rowPriority) \
                    (playhead-kanf: never re-rank a leased/running row; flag retained)
                    """
                )
                return false
            case .noRow:
                logger.info(
                    """
                    user_intent_promotion_no_row episode=\(episodeId, privacy: .public) \
                    (playhead-kanf: work key held at insert time, gone at promote time; flag retained)
                    """
                )
                return false
            }
        } catch {
            logger.error(
                "Failed to promote user-intent job for \(episodeId): \(error)"
            )
            return false
        }
    }

    private func clearPendingDownloadState(episodeId: String) {
        pendingUserIntentEpisodes.remove(episodeId)
        pendingUserIntentCoverage[episodeId] = nil
        pendingEpisodeTitles[episodeId] = nil
        pendingProbedEpisodeDurations = pendingProbedEpisodeDurations
            .filter { $0.key.episodeId != episodeId }
    }

    #if DEBUG
    func _setEnqueueBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        enqueueBarrierForTesting = barrier
    }
    #endif

    // MARK: - User-intent preparation (playhead-3xtw)

    /// Record that the user has explicitly asked to prepare `episodeId`
    /// via the on-demand "Download & Analyze" control. The next
    /// `enqueue(episodeId:)` — typically fired by the download completion
    /// — lands at the user-intent (`.now`) lane with the requested
    /// coverage.
    ///
    /// One-shot: the flag is consumed by the next `enqueue` **that manages to
    /// serve it** (playhead-kanf) — mint at the `.now` floor, promote an
    /// already-queued row to it, or find the row already there. An enqueue that
    /// cannot honour it (leased / running row) leaves the flag standing.
    /// Idempotent — re-marking is a plain set insert. Wakes the loop so an
    /// already-enqueued-and-about-to-run pass re-evaluates promptly.
    func markEpisodeUserIntent(episodeId: String, desiredCoverageSec: Double?) {
        pendingUserIntentEpisodes.insert(episodeId)
        if let desiredCoverageSec, desiredCoverageSec > 0 {
            // Keep the deepest requested coverage if marked more than once.
            let existing = pendingUserIntentCoverage[episodeId] ?? 0
            pendingUserIntentCoverage[episodeId] = max(existing, desiredCoverageSec)
        }
        wakeSchedulerLoop()
    }

    /// Enqueue the full analysis pipeline for an ALREADY-downloaded
    /// episode at the user-intent (`.now`) lane. Marks user intent (so the
    /// enqueue is stamped priority 20) then enqueues through the shared
    /// `enqueue(...)` path, inheriting its work-key dedup (idempotent — the
    /// episode never gains a second row), title/duration bookkeeping, and
    /// background rearm.
    ///
    /// playhead-kanf: work-key dedup no longer means the tap does *nothing*.
    /// When the auto-pipeline already queued this episode, the existing row is
    /// promoted to the `.now` lane in place; a leased/running row is left
    /// alone. `desiredCoverageSec` (the
    /// episode duration) requests full coverage; the backfill machinery
    /// drives it to completion.
    func enqueueUserIntentAnalysis(
        episodeId: String,
        podcastId: String?,
        sourceFingerprint: String,
        desiredCoverageSec: Double?,
        podcastTitle: String?,
        episodeTitle: String?
    ) async {
        markEpisodeUserIntent(episodeId: episodeId, desiredCoverageSec: desiredCoverageSec)
        await enqueue(
            episodeId: episodeId,
            podcastId: podcastId,
            downloadId: episodeId,
            sourceFingerprint: sourceFingerprint,
            isExplicitDownload: true,
            desiredCoverage: desiredCoverageSec,
            podcastTitle: podcastTitle,
            episodeTitle: episodeTitle
        )
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
            nowLaneAllowed: admission.allows(lane: .now),
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
    ///
    /// - Parameter nowLaneAllowed: playhead-ewag. When true, rows at or above
    ///   ``AnalysisWorkScheduler/nowLanePriorityFloor`` are selectable even
    ///   when `deferredWorkAllowed` is false. Without it, an unrelaxed
    ///   `.serious` profile binds `deferredWorkAllowed = 0` and the SELECT
    ///   hides every non-playback row, so a user's explicit download is
    ///   invisible to the dispatcher before any lane logic gets to see it.
    private func selectNextDispatchableJob(
        deferredWorkAllowed: Bool,
        nowLaneAllowed: Bool,
        now: TimeInterval
    ) async -> (job: AnalysisJob, cascadeWindow: CandidateWindow?)? {
        // 1. FIFO winner. This is the legacy contract — preserved as
        // the answer for every code path where the cascade has
        // nothing to say.
        guard let fifoJob = try? await store.fetchNextEligibleJob(
            deferredWorkAllowed: deferredWorkAllowed,
            nowLanePriorityFloor: nowLaneAllowed ? Self.nowLanePriorityFloor : nil,
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
            nowLaneAllowed: nowLaneAllowed,
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
        nowLaneAllowed: Bool,
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
                    nowLaneAllowed: nowLaneAllowed,
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
        nowLaneAllowed: Bool,
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
        // playhead-ewag: the Now-lane carve-out mirrors the SQL's
        // `priority >= nowLanePriorityFloor` disjunct. Keep the two in lock
        // step; a Swift mirror that quietly disagrees with the query is how
        // the cascade path and the FIFO path start selecting different sets.
        let isT0Playback = job.jobType == "playback"
            && job.featureCoverageSec < config.defaultT0DepthSeconds
        let isNowLane = nowLaneAllowed
            && job.priority >= Self.nowLanePriorityFloor
        let isDeferredAllowed = (deferredWorkAllowed || isNowLane) && nextEligibleDue
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
        // playhead-glo9: mirror the run loop's effective decision,
        // including the flag-gated opportunistic backlog-drain
        // relaxation, so this seam stays truthful once the flag is on.
        return !(await shouldBlockDeferredWork(admission: admission))
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

    // MARK: - Opportunistic backlog drain during playback (playhead-glo9)

    /// playhead-glo9: the effective "block deferred work this pass?"
    /// decision the run loop applies. The baseline is
    /// ``admissionBlocksDeferred()`` (the gtt9.14 4-state matrix). The
    /// opportunistic backlog-drain relaxation (flag-gated, DEFAULT-OFF)
    /// can override a `(foreground, playing)` block to ADMIT
    /// other-episode Soon/Background backlog during a foreground
    /// listening session under a safe charging-only gate.
    ///
    /// **Flag OFF ⇒ byte-identical to pre-glo9.** The leading guard
    /// returns immediately for the admit cases (no new work), and when
    /// the baseline blocks, ``opportunisticDrainRelaxationApplies(admission:)``
    /// short-circuits on the flag before any battery/store read — so the
    /// returned decision equals ``admissionBlocksDeferred()`` exactly for
    /// every (scenePhase, playbackContext, QualityProfile) combination.
    private func shouldBlockDeferredWork(admission: LaneAdmission) async -> Bool {
        guard admissionBlocksDeferred() else { return false }
        // Baseline blocks this pass — consider the flag-gated relaxation.
        if await opportunisticDrainRelaxationApplies(admission: admission) {
            return false
        }
        return true
    }

    /// playhead-glo9: opportunistic backlog-drain relaxation predicate.
    /// Returns `true` when the standard `(foreground, playing)` block on
    /// deferred work should be RELAXED so OTHER-episode Soon/Background
    /// backlog can drain during a foreground listening session. Ships
    /// behind a DEFAULT-OFF flag; when the flag is off this returns
    /// `false` immediately (no battery/store read), so admission is
    /// byte-identical to pre-glo9.
    ///
    /// All of the following must hold (Dan's ratified charging-only gate,
    /// 2026-07-23):
    ///   1. `config.opportunisticBacklogDrainDuringPlayback` — flag ON.
    ///   2. `(foreground, playing)` — a live foreground listening
    ///      session. Background states are NEVER relaxed here:
    ///      `BackgroundProcessingService` owns the background window and
    ///      the hot-path-caught-up signal (condition 5) is only defined
    ///      for foreground playback, so background playback can never
    ///      satisfy this predicate. This keeps the BG granting
    ///      architecture untouched.
    ///   3. `QualityProfile == .nominal` — no thermal / low-power /
    ///      low-battery stress. `.fair`, `.serious`, and `.critical` all
    ///      leave the block in place (the whole relaxation is off under
    ///      any stress). This also means `pauseAllWork` (`.critical`) can
    ///      never be relaxed, independent of the run loop's earlier
    ///      pauseAllWork guard.
    ///   4. The device is CHARGING. FM-heavy analysis work is charging-
    ///      only by this same condition — there is no separate off-charge
    ///      route through this relaxation.
    ///   5. The active episode's hot path is comfortably caught up
    ///      (``activeEpisodeHotPathCaughtUp()``) — its transcript
    ///      coverage sits at least ``opportunisticDrainRunwaySec`` ahead
    ///      of the playhead. This is what guarantees the active episode
    ///      is never starved: other-episode drain is only admitted while
    ///      the hot path has clear runway, and the run loop re-evaluates
    ///      the catch-up bypass FIRST on every iteration, so the moment
    ///      the active episode falls behind, catch-up wins and this
    ///      predicate flips false.
    private func opportunisticDrainRelaxationApplies(admission: LaneAdmission) async -> Bool {
        // Condition 1 — flag ON (default OFF → ships dormant). Checked
        // first so the flag-off path does zero extra work.
        guard config.opportunisticBacklogDrainDuringPlayback else { return false }
        // Condition 2 — foreground listening session only.
        guard schedulerScenePhase == .foreground, playbackContext == .playing else {
            return false
        }
        // Condition 3 — nominal profile only (no thermal/low-power/low-
        // battery stress). `admission.qualityProfile` is the RAW derived
        // profile (not the foreground-aggressive-relaxed policy).
        guard admission.qualityProfile == .nominal else { return false }
        // Condition 4 — charging only. Reuses the actor's existing
        // `batteryProvider` (the same charge source consumed by
        // `currentLaneAdmission` / `evaluateAdmissionGate`).
        let batteryState = await batteryProvider.currentBatteryState()
        guard batteryState.isCharging else { return false }
        // Condition 5 — active-episode hot path comfortably ahead.
        return await activeEpisodeHotPathCaughtUp()
    }

    /// playhead-glo9: `true` when the actively-playing foreground
    /// episode's transcript coverage sits at least
    /// ``opportunisticDrainRunwaySec`` ahead of the live playhead — i.e.
    /// the hot path is comfortably caught up, so draining OTHER-episode
    /// backlog will not steal bandwidth the active episode imminently
    /// needs.
    ///
    /// Reuses the exact transcribed-ahead computation from
    /// ``currentCatchupOpportunity(admission:now:)`` (latest job → asset
    /// → `fastTranscriptCoverageEndTime − playhead`) so the two signals
    /// are consistent duals: whenever this returns `true` the catch-up
    /// trigger (runway `< triggerThresholdSec`, a strictly smaller bound)
    /// cannot also be pending. Best-effort: a missing job/asset or a
    /// store hiccup returns `false` (stay blocked — the conservative
    /// choice), because without a persisted hot-path signal we cannot
    /// prove the active episode is caught up.
    private func activeEpisodeHotPathCaughtUp() async -> Bool {
        guard schedulerScenePhase == .foreground,
              playbackContext == .playing,
              let episodeId = activePlaybackEpisodeId,
              let playheadPosition = playheadPositionSec
        else { return false }

        let job: AnalysisJob
        do {
            guard let row = try await store.fetchLatestJobForEpisode(episodeId) else { return false }
            job = row
        } catch {
            logger.warning("activeEpisodeHotPathCaughtUp: fetchLatestJobForEpisode threw for \(episodeId): \(error)")
            return false
        }

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
            logger.warning("activeEpisodeHotPathCaughtUp: fetchAsset threw for \(episodeId): \(error)")
            return false
        }

        let transcriptCoverageEnd = asset?.fastTranscriptCoverageEndTime ?? 0
        let transcribedAhead = max(0, transcriptCoverageEnd - playheadPosition)
        return transcribedAhead >= Self.opportunisticDrainRunwaySec
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
        guard isEligibleForDispatch(
            job: job,
            deferredWorkAllowed: true,
            nowLaneAllowed: true,
            now: now
        ) else { return nil }

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

    /// Test-only entrypoint into `dispatchForegroundCatchup` for tests
    /// that need to verify the order-of-operations between admission
    /// gating and the persisted `desiredCoverageSec` write
    /// (review-followup csp / M4). Production code routes through the
    /// run loop, which calls the private function directly.
    func dispatchForegroundCatchupForTesting(opportunity: CatchupOpportunity) async {
        await dispatchForegroundCatchup(opportunity: opportunity)
    }

    /// Test-only entrypoint into `dispatchAcousticPromotion` for tests
    /// that need to verify the order-of-operations between admission
    /// gating and the persisted `desiredCoverageSec` write
    /// (review-followup csp / H1). Production code routes through the
    /// run loop, which calls the private function directly.
    func dispatchAcousticPromotionForTesting(opportunity: AcousticPromotionOpportunity) async {
        await dispatchAcousticPromotion(opportunity: opportunity)
    }

    /// Runs one standard dispatch pass synchronously for tests that need to
    /// assert `processJob` outcomes without racing the background run loop
    /// against full-suite cooperative-pool load.
    @discardableResult
    func processNextDispatchableJobForTesting(
        cancelAfterRunnerStart cause: InternalMissCause? = nil
    ) async -> Bool {
        await runSingleDispatchPass(cancelAfterRunnerStart: cause)
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
            nowLaneAllowed: admission.allows(lane: .now),
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

    func hasCurrentRunningTaskForTesting() -> Bool {
        currentRunningTask != nil
    }

    /// skeptical-review-cycle-5 #48 test-only entry point. The
    /// `emitJournalPreempted` helper carries a `guard !lostOwnership
    /// else { return }` defense-in-depth gate so a future edit that
    /// adds a new `.preempted` emission site cannot accidentally
    /// bypass the lostOwnership skip even if the umbrella check at
    /// `processJob` line ~2893 is moved or removed. Production code
    /// paths reach `emitJournalPreempted` only after that umbrella
    /// check, so the per-emit gate is impossible to exercise from a
    /// black-box test that drives the real scheduler. This entry
    /// flips `lostOwnership` and invokes the private helper directly,
    /// letting the test pin the per-emit gate behavior for each new
    /// M4 pause-arm cause (`.thermal`,
    /// `.modelTemporarilyUnavailable`, `.pipelineError`). Production
    /// code MUST NOT call this — the saved/restore pattern keeps the
    /// scheduler's lostOwnership state untouched after the call.
    func emitJournalPreemptedForTesting(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String,
        underLostOwnership: Bool
    ) async {
        // skeptical-review-cycle-7 L1: the save/restore brackets cross
        // an `await emitJournalPreempted(...)`. On an actor, an `await`
        // is a re-entrancy point: a concurrent caller invoking any
        // OTHER actor method during the suspension will observe the
        // test-installed `lostOwnership` value, not the caller's
        // saved one. That is benign for the current test pattern
        // (single test serially poking this entry) but would silently
        // bleed into a future stress-test harness that drove the
        // production scheduler in parallel. Callers must therefore
        // treat this entry as a single-test-at-a-time hook: do NOT
        // invoke it concurrently with any production scheduler activity
        // on the same instance, and do NOT fan it out across multiple
        // tasks against a shared scheduler.
        let saved = lostOwnership
        lostOwnership = underLostOwnership
        await emitJournalPreempted(
            episodeId: episodeId,
            cause: cause,
            metadataJSON: metadataJSON
        )
        lostOwnership = saved
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

    /// playhead-hygc.1.4: count rows in `analysis_jobs` that the
    /// scheduler considers candidate work for the durable run-outcome
    /// ledger. Mirrors the polling-loop view in `AnalysisCoordinator.fetchPendingJobCount`
    /// (queued + running + paused) but is exposed here as a dedicated
    /// helper so `BackgroundProcessingService` can capture a baseline
    /// without depending on the coordinator's polling loop. Returns 0
    /// on read failure rather than throwing — the caller is on a 30 s
    /// BGProcessingTask budget and does not want a transient SQLite
    /// hiccup to surface as a thrown error.
    func pendingJobCountForLedger() async -> Int {
        let states = ["queued", "running", "paused"]
        var total = 0
        for state in states {
            if let rows = try? await store.fetchJobsByState(state) {
                total += rows.count
            }
        }
        return total
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

    /// playhead-beh3 (R13): install the adaptive-estimator provider once
    /// the SwiftData `ModelContainer` is available. The scheduler is
    /// constructed in `PlayheadRuntime.init` BEFORE the container exists
    /// (init is synchronous; the container is built by `PlayheadApp.task`
    /// and threaded back in via setters), so without this seam the
    /// production scheduler permanently holds the
    /// `NoOpLearnedDeviceProfileProvider` default — every
    /// `recordObservation` is dropped, `resolvedDeviceProfile` returns
    /// the seed verbatim, and the `LearnedDeviceProfile` table never
    /// fills even with `useAdaptiveDeviceProfile = true`.
    ///
    /// Mirrors `setWorkJournalRecorder` / `setLanePreemptionHandler`:
    /// idempotent (re-installing replaces the prior provider), call-once
    /// in production (from `PlayheadRuntime.attachLearnedDeviceProfileStore`),
    /// no-op in preview runtimes that never reach that wiring step.
    func setLearnedDeviceProfileProvider(_ provider: any LearnedDeviceProfileProviding) {
        self.learnedDeviceProfileProvider = provider
    }

    /// Start the scheduler loop. Call after reconciliation is complete.
    func startSchedulerLoop() {
        schedulerTask?.cancel()
        schedulerTask = Task { [weak self] in
            guard let self else { return }
            await self.runLoop()
        }
    }

    /// playhead-gy2s (RC-1): start the scheduler loop only if it is not
    /// already running. Production normally boots the loop from
    /// `PlayheadRuntime`'s SwiftUI `.task`, but a background launch with no
    /// scene may never fire that view modifier — leaving `wake()` a no-op
    /// against a loop that was never started, so eligible work sits forever.
    /// The BG-task handlers call this before draining so the loop exists.
    /// Idempotent: a live (non-cancelled) loop task is left untouched;
    /// `startSchedulerLoop()` would otherwise cancel-and-restart it.
    func ensureSchedulerLoopStarted() {
        if let task = schedulerTask, !task.isCancelled { return }
        startSchedulerLoop()
    }

    /// playhead-gy2s (RC-1): actively drain the eligible queue during a
    /// background wake. The BG-task handlers used to only `wake()` the loop
    /// and poll `fetchPendingJobCount` — but if the loop had never started (a
    /// sceneless background launch), or was silently rejecting compute-only
    /// pre-analysis (RC-2), pending never dropped and the whole OS budget
    /// burned with 0 jobs done (`task_expired`, jobsCompleted=0). This method
    /// repeatedly runs one standard dispatch pass until the queue has nothing
    /// dispatchable OR the OS deadline / cancellation stops it. Each pass
    /// awaits its job to completion, so draining is serial and the per-lane
    /// cap never blocks the next pass. Budget-aware: honors `Task.isCancelled`
    /// (BG expiration) and the caller-supplied `deadline`. Safe to run
    /// alongside the long-lived `runLoop()` — dispatch is lease-guarded, so a
    /// racing pass on the same job cleanly loses the CAS and skips.
    func drainEligible(deadline: ContinuousClock.Instant) async {
        guard config.isEnabled else { return }
        while !Task.isCancelled, ContinuousClock.now < deadline {
            let dispatched = await runSingleDispatchPass()
            if !dispatched { break }
            // playhead-bbut: cooperative yield between passes. In the one
            // pathology where the eligibility SELECT keeps succeeding but the
            // lease UPDATE persistently throws (a stuck DB write-lock),
            // `runSingleDispatchPass` returns `true` without the row leaving the
            // queue, so this loop would otherwise TIGHT-spin (re-selecting the
            // same row) until the caller deadline. A plain CPU-pegging spin
            // starves other actor work and delays cancellation observation; the
            // yield makes the loop cooperative and gives `Task.isCancelled` /
            // the deadline a prompt suspension point without changing dispatch
            // semantics on the healthy path (where each pass runs a real,
            // slow, awaited job).
            await Task.yield()
        }
    }

    /// playhead-gy2s (RC-1): run exactly one standard dispatch pass and return
    /// whether it dispatched a job. Extracted from the former
    /// `processNextDispatchableJobForTesting` body so the production
    /// background drain (`drainEligible`) and the DEBUG single-pass test seam
    /// share one code path. Mirrors the runLoop's per-iteration standard
    /// dispatch (admission → selection → lane/gate checks → lease + run),
    /// minus the loop's foreground-catchup / acoustic-promotion / shadow-lane
    /// side channels. Returns `false` (dispatched nothing) when work is paused,
    /// deferred-blocked, the queue has no eligible job, the lane is at
    /// capacity, or the admission gate rejects — the same conditions under
    /// which the loop would sleep and re-poll.
    @discardableResult
    private func runSingleDispatchPass(
        cancelAfterRunnerStart cause: InternalMissCause? = nil
    ) async -> Bool {
        guard config.isEnabled else { return false }

        let admission = await currentLaneAdmission()
        guard !admission.pauseAllWork else { return false }
        guard !admissionBlocksDeferred() else { return false }

        let deferredWorkAllowed = admission.policy.allowSoonLane
            || admission.policy.allowBackgroundLane
        let now = clock().timeIntervalSince1970

        guard let selected = await selectNextDispatchableJob(
            deferredWorkAllowed: deferredWorkAllowed,
            nowLaneAllowed: admission.allows(lane: .now),
            now: now
        ) else {
            return false
        }

        let job = selected.job
        // playhead-ewag: same lane gate the run loop applies. This path serves
        // `drainEligible`, i.e. the BGTask overnight drain — pre-fix it carried
        // an identical copy of the depth misclassification, so the overnight
        // recovery path was frozen in exactly the same way.
        var coverageOverride: Double?
        if job.jobType != "playback" {
            switch await applyLaneGate(to: job, admission: admission, now: now) {
            case .hold:
                return false
            case .admitProgressFloor(let cap):
                coverageOverride = cap
            case .admit:
                break
            }
        }

        guard canAdmit(job: job) else { return false }

        let gateDecision = await evaluateAdmissionGate(for: job)
        switch gateDecision {
        case .reject:
            return false
        case .admit:
            break
        }

        if job.schedulerLane == .now, let preempt = preemptionHandler {
            await preempt.preemptLowerLanes(for: job.schedulerLane)
        }

        didStart(job: job)
        defer { didFinish(job: job) }
        await processJob(
            job,
            cascadeWindow: selected.cascadeWindow,
            desiredCoverageOverride: coverageOverride,
            testCancelAfterRunnerStart: cause
        )
        return true
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
            // and `continue`'d above; if no we fall through here.
            //
            // playhead-glo9: `shouldBlockDeferredWork` folds the
            // opportunistic backlog-drain relaxation into the baseline
            // `admissionBlocksDeferred()` matrix. With the flag OFF
            // (default) this is byte-identical to the pre-glo9 block.
            // With the flag ON, a `(foreground, playing)` block is
            // RELAXED — admitting OTHER-episode Soon/Background backlog —
            // only when the device is charging, the QualityProfile is
            // `.nominal`, and the active episode's hot path is
            // comfortably caught up. The admitted work still flows
            // through the same lane caps, admission gate, and
            // preemption hook below, and the catch-up bypass above wins
            // on every iteration if the active episode falls behind.
            if await shouldBlockDeferredWork(admission: admission) {
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            // `deferredWorkAllowed` gates the store's deferred (T1+) selection.
            // Cases where Background is paused but Soon is allowed are handled
            // after fetch via the lane gate, because the store predicate only
            // distinguishes T0 vs. deferred, not Soon vs. Background.
            //
            // playhead-ewag: `nowLaneAllowed` is the store-side half of the
            // lane fix and is NOT optional. Under an unrelaxed `.serious` both
            // policy flags are false, so `deferredWorkAllowed` binds 0 and the
            // SELECT itself hides every non-playback row — honouring the Now
            // lane only after selection would leave the fix inert on exactly
            // the path (BGTask drain, backgrounded device) where it matters
            // most.
            let deferredWorkAllowed = admission.policy.allowSoonLane
                || admission.policy.allowBackgroundLane
            let nowLaneAllowed = admission.allows(lane: .now)

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
                nowLaneAllowed: nowLaneAllowed,
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
                // unstructured task does not close over the scheduler actor.
                startShadowLaneTickIfIdle()
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
            //
            // playhead-ewag: the filter now reads the job's LANE, not its
            // coverage depth, and every hold leaves a durable advisory on the
            // row plus a bounded progress floor. The 30 s re-select loop is
            // unchanged — what changed is that it is no longer silent and no
            // longer unbounded.
            var coverageOverride: Double?
            if job.jobType != "playback" {
                switch await applyLaneGate(to: job, admission: admission, now: now) {
                case .hold:
                    await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
                    continue
                case .admitProgressFloor(let cap):
                    coverageOverride = cap
                case .admit:
                    break
                }
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

            // skeptical-review-cycle-3 H-B: pair didStart with `defer
            // didFinish` so a future thrown error or actor-injected
            // CancellationError between the two cannot leak the lane
            // counter. A leaked increment permanently saturates the lane
            // (Now-cap is 1) and produces the user-visible "Now: nothing
            // running, Up next: a lot of rows" stall. processJob
            // currently swallows internal errors, but the defer is
            // structural insurance against future refactors.
            didStart(job: job)
            defer { didFinish(job: job) }
            await processJob(
                job,
                cascadeWindow: dispatchedCascadeWindow,
                desiredCoverageOverride: coverageOverride
            )
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
        // Fetch the current row so admission decisions read live state
        // (lane, priority, fingerprint) without depending on the
        // escalation having landed first. A `nil` here is unusual (the
        // row existed at `currentCatchupOpportunity` evaluation time
        // moments ago) but bail safely if the row was concurrently
        // superseded.
        let preEscalationJob: AnalysisJob
        do {
            guard let refreshed = try await store.fetchJob(byId: opportunity.jobId) else {
                logger.warning("Foreground catch-up: job \(opportunity.jobId) disappeared before escalation")
                return
            }
            preEscalationJob = refreshed
        } catch {
            logger.warning("Foreground catch-up: pre-admission fetchJob threw for \(opportunity.jobId): \(error)")
            return
        }

        // Lane-cap and admission-gate checks mirror `runLoop()`. We
        // consult both because catch-up should never bust the Now-cap
        // or skip the bnrs gate — both invariants are preserved when
        // catch-up escalates the same row that would have been
        // dispatched normally; the only thing that changed is the
        // coverage target.
        //
        // Order matters (review-followup csp / M4): admission MUST
        // happen BEFORE the persisted `desiredCoverageSec` escalation.
        // A rejection that has already written the deeper target
        // permanently raises the row's coverage demand without ever
        // performing the work, so the next dispatch sees an inflated
        // tier the runner can't satisfy in one pass. Persisting only
        // after admission succeeds keeps denied admissions side-effect
        // free.
        guard canAdmit(job: preEscalationJob) else {
            logger.info("Foreground catch-up: lane \(String(describing: preEscalationJob.schedulerLane), privacy: .public) at capacity; deferring")
            await sleepOrWake(seconds: Self.idlePollSeconds)
            return
        }

        let gateDecision = await evaluateAdmissionGate(for: preEscalationJob)
        if case .reject(let cause) = gateDecision {
            logger.info("Foreground catch-up: AdmissionGate rejected job \(preEscalationJob.jobId) cause=\(cause.rawValue, privacy: .public)")
            await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
            return
        }

        // Admission cleared. Persist the escalation so the runner
        // reads the deeper target on its next `fetchJob(byId:)` (and a
        // crash mid-catch-up resumes against the deeper target rather
        // than the stale tier value).
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
        // escalation. Same null-safety reasoning as the pre-admission
        // fetch above.
        let job: AnalysisJob
        do {
            guard let refreshed = try await store.fetchJob(byId: opportunity.jobId) else {
                logger.warning("Foreground catch-up: job \(opportunity.jobId) disappeared after escalation")
                return
            }
            job = refreshed
        } catch {
            logger.warning("Foreground catch-up: post-escalation fetchJob threw for \(opportunity.jobId): \(error)")
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

        // skeptical-review-cycle-3 H-B: defer-paired lane accounting.
        didStart(job: job)
        defer { didFinish(job: job) }
        await processJob(job, cascadeWindow: nil)
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
        // Fetch the current row so admission decisions read live state
        // (lane, priority, fingerprint) without depending on the
        // escalation having landed first. A `nil` here is unusual (the
        // row existed at `currentAcousticPromotionOpportunity` evaluation
        // moments ago) but bail safely if the row was concurrently
        // superseded.
        let preEscalationJob: AnalysisJob
        do {
            guard let refreshed = try await store.fetchJob(byId: opportunity.jobId) else {
                logger.warning("Acoustic promotion: job \(opportunity.jobId) disappeared before escalation")
                return
            }
            preEscalationJob = refreshed
        } catch {
            logger.warning("Acoustic promotion: pre-admission fetchJob threw for \(opportunity.jobId): \(error)")
            return
        }

        // Lane-cap and admission-gate checks mirror `runLoop()` and
        // `dispatchForegroundCatchup`. We consult both because
        // promotion should never bust the Now-cap or skip the bnrs
        // gate.
        //
        // Order matters (review-followup csp / H1, mirroring M4):
        // admission MUST happen BEFORE the persisted
        // `desiredCoverageSec` escalation. A rejection that has already
        // written the deeper target permanently raises the row's
        // coverage demand without ever performing the work, so the next
        // dispatch sees an inflated tier the runner can't satisfy in
        // one pass. Persisting only after admission succeeds keeps
        // denied admissions side-effect free.
        guard canAdmit(job: preEscalationJob) else {
            logger.info("Acoustic promotion: lane \(String(describing: preEscalationJob.schedulerLane), privacy: .public) at capacity; deferring")
            await sleepOrWake(seconds: Self.idlePollSeconds)
            return
        }

        let gateDecision = await evaluateAdmissionGate(for: preEscalationJob)
        if case .reject(let cause) = gateDecision {
            logger.info("Acoustic promotion: AdmissionGate rejected job \(preEscalationJob.jobId) cause=\(cause.rawValue, privacy: .public)")
            await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
            return
        }

        // Admission cleared. Persist the escalation so the runner
        // reads the deeper target on its next `fetchJob(byId:)` (and a
        // crash mid-promotion resumes against the deeper target rather
        // than the stale tier value).
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
        // escalation. Same null-safety reasoning as the pre-admission
        // fetch above.
        let job: AnalysisJob
        do {
            guard let refreshed = try await store.fetchJob(byId: opportunity.jobId) else {
                logger.warning("Acoustic promotion: job \(opportunity.jobId) disappeared after escalation")
                return
            }
            job = refreshed
        } catch {
            logger.warning("Acoustic promotion: post-escalation fetchJob threw for \(opportunity.jobId): \(error)")
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

        // skeptical-review-cycle-3 H-B: defer-paired lane accounting.
        didStart(job: job)
        defer { didFinish(job: job) }
        await processJob(job, cascadeWindow: nil)
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

    // MARK: - Lane gate application (playhead-ewag)

    /// Apply the post-selection lane gate to `job` and record the outcome.
    ///
    /// This is the ONLY place the lane gate is evaluated. Both dispatch paths
    /// — the run loop and `runSingleDispatchPass` (which serves `drainEligible`
    /// and therefore the BGTask overnight drain) — route through it, because
    /// the field bug was present at BOTH sites and a fix applied to one would
    /// have left the overnight path frozen.
    ///
    /// Side effects on a hold, all three mandated by the bead:
    ///   * bumps the CONSECUTIVE skip count for this job,
    ///   * writes the durable `lastRejectReason` advisory so the hold is
    ///     visible in a pulled database instead of being pure silence, and
    ///   * posts the Activity refresh so the surface can say why.
    /// On either admit arm the hold record is dropped, so the count means what
    /// it says.
    private func applyLaneGate(
        to job: AnalysisJob,
        admission: LaneAdmission,
        now: TimeInterval
    ) async -> LaneGateOutcome {
        let lane = job.schedulerLane
        let queuedForSec = max(0, now - job.createdAt)
        let outcome = Self.evaluateLaneGate(
            lane: lane,
            admission: admission,
            queuedForSec: queuedForSec,
            attemptCount: job.attemptCount,
            desiredCoverageSec: job.desiredCoverageSec,
            t1DepthSeconds: config.t1DepthSeconds
        )

        switch outcome {
        case .admit:
            clearLaneHold(jobId: job.jobId)
        case .admitProgressFloor(let cap):
            clearLaneHold(jobId: job.jobId)
            logger.info(
                "Progress floor admitting job \(job.jobId) episode=\(job.episodeId) lane=\(String(describing: lane), privacy: .public) queuedForSec=\(Int(queuedForSec)) cappedCoverageSec=\(Int(cap)) profile=\(admission.qualityProfile.rawValue, privacy: .public)"
            )
        case .hold(let reason):
            let prior = laneHolds[job.jobId]
            let record = LaneHoldRecord(
                jobId: job.jobId,
                episodeId: job.episodeId,
                lane: lane,
                qualityProfile: admission.qualityProfile,
                cause: admission.throttleCause,
                consecutiveSkips: (prior?.consecutiveSkips ?? 0) + 1,
                firstHeldAt: prior?.firstHeldAt ?? now,
                lastHeldAt: now
            )
            laneHolds[job.jobId] = record
            // Durable, UPDATE-in-place advisory. Best-effort in exactly the
            // same sense as the multi-resource gate's reject write: a store
            // hiccup must never change the admission verdict.
            do {
                try await store.recordJobAdmissionReject(
                    jobId: job.jobId,
                    reason: reason,
                    at: now
                )
            } catch {
                logger.warning("Failed to record lane-hold advisory for job \(job.jobId): \(error)")
            }
            logger.info(
                "Holding job \(job.jobId) episode=\(job.episodeId) lane=\(String(describing: lane), privacy: .public) reason=\(reason, privacy: .public) cause=\(record.cause.rawValue, privacy: .public) consecutiveSkips=\(record.consecutiveSkips)"
            )
            Self.postActivityRefreshNotification()
        }
        return outcome
    }

    /// Drop the hold record for `jobId`, if any. Called on every admit so the
    /// consecutive count never carries across a dispatch.
    private func clearLaneHold(jobId: String) {
        guard laneHolds.removeValue(forKey: jobId) != nil else { return }
        Self.postActivityRefreshNotification()
    }

    /// playhead-ewag: every job the lane gate is currently holding, with its
    /// consecutive skip count. Read by diagnostics so a stall is countable.
    func currentLaneHolds() -> [LaneHoldRecord] {
        laneHolds.values.sorted { lhs, rhs in
            if lhs.firstHeldAt != rhs.firstHeldAt { return lhs.firstHeldAt < rhs.firstHeldAt }
            return lhs.jobId < rhs.jobId
        }
    }

    /// playhead-ewag: the lane-gate hold currently recorded for `jobId`, or
    /// `nil` when the job is not being held.
    func laneHold(forJobId jobId: String) -> LaneHoldRecord? {
        laneHolds[jobId]
    }

    /// playhead-ewag: episodeId → why its analysis is being held, for the
    /// Activity surface.
    ///
    /// This is the "surface it" half of the bead's bound. Before it, a job the
    /// scheduler had decided not to run contributed NOTHING to the UI — the
    /// Activity screen's `cause` input was a hardcoded `nil`, so a thermally
    /// held episode was indistinguishable from one merely waiting its turn,
    /// and the whole queue read as the silent "Nothing running" that let this
    /// bug live for weeks. Feeding the cause in turns that into the Paused row
    /// the copy table already has words for.
    ///
    /// Last hold wins when two jobs of the same episode are held; they share a
    /// profile, so the cause is identical either way.
    func heldEpisodeCauses() -> [String: InternalMissCause] {
        var causes: [String: InternalMissCause] = [:]
        for record in laneHolds.values {
            causes[record.episodeId] = record.cause
        }
        return causes
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
    ///   until a loader is injected). playhead-beh3 layers an adaptive
    ///   Welford+EWMA estimator over this seed: when the
    ///   `useAdaptiveDeviceProfile` flag is ON and the estimator has
    ///   activated (≥30 grant-window observations), the
    ///   `learnedDeviceProfileProvider` returns a scaled copy of the
    ///   seed row; flag-off (or pre-activation) returns the seed
    ///   verbatim.
    /// - `transport`: synthesized from `transportStatusProvider`
    ///   (defaults to `LiveTransportStatusProvider`, which wraps
    ///   `NWPathMonitor` + the user's `allowsCellular` pref) and the
    ///   job's lane. Background-lane jobs map to `.maintenance` (Wi-Fi
    ///   only); every other lane maps to `.interactive`.
    /// - `storage`: live snapshot synthesized from the injected
    ///   `StorageBudgetSnapshotting` (typically the live `StorageBudget`
    ///   actor). The pre-admission gate now genuinely rejects
    ///   media-writing jobs when the media cap is reached — the
    ///   previous `StorageSnapshot.plentiful` no-op is gone. Write-time
    ///   `StorageBudget.admit(class:sizeBytes:)` continues to enforce
    ///   independently as a defense-in-depth backstop. (playhead-1iq1.)
    func evaluateAdmissionGate(for job: AnalysisJob) async -> GateAdmissionDecision {
        let snapshot = await capabilitiesService.currentSnapshot
        let batteryState = await batteryProvider.currentBatteryState()
        let profile = snapshot.qualityProfile(
            batteryLevel: batteryState.level,
            isCharging: batteryState.isCharging
        )
        let deviceClass = snapshot.deviceClass
        // playhead-beh3: seed remains the Phase-1 static fallback. The
        // adaptive estimator (`learnedDeviceProfileProvider`) layers on
        // top — when the feature flag is ON and the estimator has
        // activated (≥30 samples), the provider returns a SCALED copy
        // of the seed. Flag OFF (or pre-activation) returns the seed
        // verbatim, which is byte-identical to the pre-beh3 behavior.
        let seedDeviceProfile = DeviceClassProfile.fallback(for: deviceClass)
        let deviceProfile: DeviceClassProfile
        if config.useAdaptiveDeviceProfile {
            deviceProfile = await learnedDeviceProfileProvider.resolvedDeviceProfile(
                seed: seedDeviceProfile,
                deviceClass: deviceClass
            )
        } else {
            deviceProfile = seedDeviceProfile
        }

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

        let estimatedBytes = max(0, job.estimatedWriteBytes)
        let storage = await synthesizeStorageSnapshot(
            for: job.artifactClass,
            estimatedBytes: estimatedBytes
        )

        let admissionJob = AdmissionJob(
            artifactClasses: [job.artifactClass],
            estimatedWriteBytes: estimatedBytes
        )

        // playhead-gy2s (RC-2): a pre-analysis job whose input file is already
        // on disk performs on-device transcription with ZERO network transfer.
        // Because a background-lane job maps to a `.maintenance` transport
        // session, such work was being mis-gated as a Wi-Fi-only transfer and
        // silently rejected on cellular / unreachable — wedging the whole
        // queue with eligible work and nothing running. Detect that class here
        // and exempt ONLY the transport gate; storage / thermal / battery are
        // unchanged.
        // (Split rather than folded into `&&` because the RHS is an
        // autoclosure that cannot host an `await`.)
        let isComputeOnlyPreAnalysis: Bool
        if job.jobType == "preAnalysis" {
            isComputeOnlyPreAnalysis = (await downloadManager.cachedFileURL(for: job.episodeId)) != nil
        } else {
            isComputeOnlyPreAnalysis = false
        }

        let decision = AdmissionGate.admit(
            job: admissionJob,
            profile: profile,
            deviceClass: deviceClass,
            deviceProfile: deviceProfile,
            storage: storage,
            transport: transport,
            transportExempt: isComputeOnlyPreAnalysis
        )

        // playhead-gy2s (RC-2): make the reject OBSERVABLE. On a hard reject we
        // write a durable advisory reason to the job row (UPDATE in place, not
        // an append — so a job that keeps rejecting every 30 s refreshes one
        // row instead of spamming). This turns a silent "Nothing running" into
        // a diagnosable "waiting for storage / Wi-Fi". Best-effort: a store
        // hiccup must not change the admission verdict.
        if case .reject(let cause) = decision {
            let rejectedAt = clock().timeIntervalSince1970
            do {
                try await store.recordJobAdmissionReject(
                    jobId: job.jobId,
                    reason: cause.rawValue,
                    at: rejectedAt
                )
            } catch {
                logger.warning("Failed to record admission-reject advisory for job \(job.jobId): \(error)")
            }
        }

        return decision
    }

    /// playhead-1iq1: build a per-admission `StorageSnapshot` from the
    /// injected snapshotter. The job's `artifactClass` drives the
    /// per-class `canAdmit` query for the projected write; the other
    /// classes are admitted by default (the gate consults only the
    /// classes the job actually writes to). `remainingBytes` is queried
    /// for every class so the slice-sizing path has the headroom view
    /// it expects.
    private func synthesizeStorageSnapshot(
        for cls: ArtifactClass,
        estimatedBytes: Int64
    ) async -> StorageSnapshot {
        var canAdmit: [ArtifactClass: Bool] = [
            .media: true,
            .warmResumeBundle: true,
            .scratch: true,
        ]
        canAdmit[cls] = await storageBudgetSnapshotter.canAdmit(
            cls,
            bytes: estimatedBytes
        )

        var remaining: [ArtifactClass: Int64] = [:]
        for c in ArtifactClass.allCases {
            remaining[c] = await storageBudgetSnapshotter.remainingBytes(c)
        }

        return StorageSnapshot(canAdmit: canAdmit, remainingBytes: remaining)
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
        return LaneAdmission(
            qualityProfile: profile,
            policy: effectivePolicy,
            throttleCause: Self.throttleCause(
                isLowPowerMode: snapshot.isLowPowerMode,
                batteryLevel: batteryState.level,
                isCharging: batteryState.isCharging
            )
        )
    }

    /// playhead-ewag: which of the three demotion inputs `QualityProfile`
    /// consumed is the one to name to the user.
    ///
    /// Mirrors the precedence in ``QualityProfile/derive(thermalState:batteryLevel:batteryState:isLowPowerMode:)``
    /// — Low Power Mode is tested first there, and a charging device is never
    /// demoted for its battery level. Thermal is the residual: if neither
    /// power condition holds, the profile can only have come from the SoC.
    ///
    /// This is deliberately a NAMING decision, not a second derivation: it
    /// never decides whether to throttle, only which sentence describes a
    /// throttle that `QualityProfile` already chose. If the two ever disagree
    /// the worst outcome is imprecise copy, never a wrong admission.
    static func throttleCause(
        isLowPowerMode: Bool,
        batteryLevel: Float,
        isCharging: Bool
    ) -> InternalMissCause {
        if isLowPowerMode { return .lowPowerMode }
        // A negative level is UIDevice's "monitoring off" sentinel and never
        // demotes, so it must not be named as the cause either.
        if batteryLevel >= 0,
           batteryLevel < QualityProfile.lowBatteryThreshold,
           !isCharging {
            return .batteryLowUnplugged
        }
        return .thermal
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

    private func startShadowLaneTickIfIdle() {
        guard let shadowLaneTickHandler, !shadowLaneTickInFlight else { return }
        shadowLaneTickInFlight = true
        Task { [weak self, shadowLaneTickHandler] in
            await shadowLaneTickHandler.shadowLaneBTick()
            await self?.shadowLaneTickDidFinish()
        }
    }

    private func shadowLaneTickDidFinish() {
        shadowLaneTickInFlight = false
    }

    // MARK: - Job Processing

    /// - Parameter desiredCoverageOverride: playhead-ewag. Coverage target for
    ///   THIS dispatch only, used by the lane gate's progress floor to admit a
    ///   bounded slice of a job whose lane is closed. Deliberately NOT
    ///   persisted: `updateJobDesiredCoverage` would permanently shrink the
    ///   job's real target, so a thermal hold would silently downgrade how much
    ///   of the episode ever gets analysed. `nil` (every other caller) reads
    ///   the row's own target, exactly as before.
    private func processJob(
        _ job: AnalysisJob,
        cascadeWindow: CandidateWindow? = nil,
        desiredCoverageOverride: Double? = nil,
        testCancelAfterRunnerStart: InternalMissCause? = nil
    ) async {
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

        // playhead-wrj8 (#4): protect the cached audio file from LRU
        // eviction for the duration of this analysis job, so a large cache
        // over budget can't delete the file mid-analysis (which would strand
        // the job or, worse, force a re-fetch of a different DAI stitch).
        // Refcounted + released on every exit via `defer`, so it composes
        // with the playback-side protection on the same episode. Reached
        // via a concrete cast (the eviction refcount is DownloadManager
        // state, deliberately NOT on the `DownloadProviding` query
        // protocol) so lightweight test stubs are unaffected.
        let protectedEpisodeId = job.episodeId
        let evictionProtector = downloadManager as? DownloadManager
        await evictionProtector?.protectForAnalysis(episodeId: protectedEpisodeId)
        defer {
            if let evictionProtector {
                Task { await evictionProtector.unprotectFromAnalysis(episodeId: protectedEpisodeId) }
            }
        }

        // Acquire lease. playhead-5uvz.1 (Gap-1): use the journal-aware
        // variant so the lease UPDATE and the `acquired` work_journal
        // row land in the SAME SQL transaction. Without this the
        // production journal stays empty and AnalysisCoordinator's
        // `recoverOrphans` (the journal-aware cold-launch reaper)
        // degrades to the same blind sweep AnalysisJobReconciler runs.
        let now = clock().timeIntervalSince1970
        // playhead-beh3 (R2): capture the lease-acquired wall-clock for
        // the adaptive estimator write seam. Equal to `now` but kept as
        // a separate binding so the success outcome arms below can
        // compute `grantWindowSeconds = clock().timeIntervalSince1970 -
        // leaseAcquiredAt` without re-reading the local that the lease-
        // expiry math mutates conceptually.
        let leaseAcquiredAt = now
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
            currentRunningTask?.cancel()
            currentRunningTask = nil
            currentJobId = nil
            currentEpisodeId = nil
            shouldCancelCurrentJob = false
            pendingCancelCause = nil
        }

        if case .changed(let currentFingerprint) = await cachedCanonicalFingerprintStatus(
            for: job,
            localAudioURL: localAudioURL
        ) {
            pendingProbedEpisodeDurations.removeValue(
                forKey: Self.durationStashKey(
                    episodeId: job.episodeId,
                    sourceFingerprint: job.sourceFingerprint
                )
            )
            let journalJobSnapshot = try? await store.fetchJob(byId: job.jobId)
            let replacementJob = replacementJobForCurrentCanonicalAudio(
                replacing: job,
                currentFingerprint: currentFingerprint
            )
            let staleMetadata = SliceCompletionInstrumentation
                .buildMetadata(
                    sliceDurationMs: 0,
                    bytesProcessed: 0,
                    shardsCompleted: 0,
                    deviceClass: DeviceClass.detect(),
                    extras: [
                        "stage": "analysisWorkScheduler.staleCanonicalFingerprintSupersede",
                        "job_id": job.jobId,
                        "stale_fingerprint": job.sourceFingerprint,
                        "current_fingerprint": currentFingerprint,
                    ]
                )
                .encodeJSON()
            logger.warning("Superseding stale canonical-SHA job \(job.jobId): cached audio for episode \(job.episodeId) no longer matches \(job.sourceFingerprint)")
            await commitOutcomeArm(
                "staleCanonicalFingerprint.supersede",
                AnalysisStore.ProcessJobOutcomeArmCommit(
                    jobId: job.jobId,
                    insertNextJob: replacementJob,
                    workKeyUpdate: Self.retiredStaleCanonicalWorkKey(for: job),
                    stateUpdate: .init(
                        state: "superseded",
                        nextEligibleAt: nil,
                        lastErrorCode: "staleFingerprint:cachedAudioMismatch"
                    )
                )
            )
            await emitJournalFailedForJobSnapshot(
                episodeId: job.episodeId,
                jobSnapshot: journalJobSnapshot,
                cause: .pipelineError,
                metadataJSON: staleMetadata
            )
            return
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
            // playhead-work-journal-wiring: asset-resolution failure
            // is a pipeline error (the decoder never ran). Use
            // `.pipelineError` so the journal's `cause` column
            // accurately distinguishes this from cancel-driven
            // (`.userCancelled` / `.taskExpired`) and runner-driven
            // (`.asrFailed`, etc.) failures.
            let assetResolutionMetadata = SliceCompletionInstrumentation
                .buildMetadata(
                    sliceDurationMs: 0,
                    bytesProcessed: 0,
                    shardsCompleted: 0,
                    deviceClass: DeviceClass.detect(),
                    extras: [
                        "stage": "analysisWorkScheduler.assetResolution",
                        "job_id": job.jobId,
                        "attempt": "\(attempts)",
                    ]
                )
                .encodeJSON()
            if attempts >= Self.maxAttemptCount {
                await commitOutcomeArm(
                    "assetResolution.supersede",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        incrementAttempt: true,
                        stateUpdate: .init(
                            state: "superseded",
                            nextEligibleAt: nil,
                            lastErrorCode: "\(Self.maxAttemptsReachedPrefix)assetResolution: \(error)"
                        )
                    )
                )
                await emitJournalFailed(
                    episodeId: job.episodeId,
                    cause: .pipelineError,
                    metadataJSON: assetResolutionMetadata
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
                await emitJournalFailed(
                    episodeId: job.episodeId,
                    cause: .pipelineError,
                    metadataJSON: assetResolutionMetadata
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
            desiredCoverageSec: desiredCoverageOverride ?? job.desiredCoverageSec,
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
                // playhead-work-journal-wiring: emit the preempt row
                // for the cancel-before-runner-start path. This arm
                // fires when `shouldCancelCurrentJob` is already true
                // when the lease is acquired — i.e. the user (or BG
                // task expiration) issued the cancel before the
                // decoder started. The cancel cause defaults to
                // `.userCancelled` because the only canceller that
                // can reach this arm before runTask is the explicit
                // user-cancel path; the BG-task expiration cancels
                // mid-decode and lands in the catch arm below
                // (which already emits via the existing recorder
                // call). `pendingCancelCause` overrides when the
                // racing canceller passed a more specific cause.
                let cancelRaceCause = pendingCancelCause ?? .userCancelled
                let cancelRaceMetadata = await SliceCompletionInstrumentation
                    .recordPaused(
                        cause: cancelRaceCause,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": "analysisWorkScheduler.cancelRace",
                            "job_id": job.jobId,
                        ]
                    )
                    .encodeJSON()
                await emitJournalPreempted(
                    episodeId: job.episodeId,
                    cause: cancelRaceCause,
                    metadataJSON: cancelRaceMetadata
                )
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

            #if DEBUG
            if let testCancelAfterRunnerStart {
                pendingCancelCause = testCancelAfterRunnerStart
                shouldCancelCurrentJob = true
                currentRunningTask?.cancel()
                runTask.cancel()
            }
            #endif

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
                // playhead-1nl6: cause that accompanied the cancel.
                // Default `.pipelineError` for callers that forgot to
                // pass one; BG-task expiration passes `.taskExpired`;
                // explicit user cancel passes `.userCancelled`.
                let cause = pendingCancelCause ?? .pipelineError
                pendingCancelCause = nil
                let episodeId = job.episodeId
                if attempts >= Self.maxAttemptCount {
                    await commitOutcomeArm(
                        "cancelCatch.supersede",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            incrementAttempt: true,
                            stateUpdate: .init(
                                state: "superseded",
                                nextEligibleAt: nil,
                                lastErrorCode: "\(Self.maxAttemptsReachedPrefix)cancelMidRun"
                            )
                        )
                    )
                    // playhead-work-journal-wiring: terminal supersede
                    // is a non-recoverable failure (the slot will not
                    // be retried). Emit `.failed` with
                    // `cause: .pipelineError` per the audit's Gap-1
                    // recommendation — supersede after a poisoned
                    // cancel loop is a pipeline-class failure, not a
                    // transient pause. The `recordFailed` counter on
                    // `SliceCompletionInstrumentation` is incremented
                    // here so the cause taxonomy stays consistent.
                    let supersedeMetadata = await SliceCompletionInstrumentation
                        .recordFailed(
                            cause: .pipelineError,
                            deviceClass: DeviceClass.detect(),
                            sliceDurationMs: 0,
                            bytesProcessed: 0,
                            shardsCompleted: 0,
                            extras: [
                                "stage": "analysisWorkScheduler.cancelCatchSupersede",
                                "job_id": job.jobId,
                                "original_cancel_cause": cause.rawValue,
                                "attempts": "\(attempts)",
                            ]
                        )
                        .encodeJSON()
                    await emitJournalFailed(
                        episodeId: episodeId,
                        cause: .pipelineError,
                        metadataJSON: supersedeMetadata
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
                    // playhead-1nl6 / work-journal-wiring: requeue
                    // path is a transient pause — emit `.preempted`
                    // with the cancel's typed cause. Routes through
                    // the helper so the recorder snapshot is taken on
                    // the actor and `lostOwnership` is re-checked at
                    // emission time.
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
                    await emitJournalPreempted(
                        episodeId: episodeId,
                        cause: cause,
                        metadataJSON: metadata.encodeJSON()
                    )
                }
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
            case .reachedTarget where Self.tierTargetSatisfied(job: job, outcome: outcome):
                // playhead-8bp2: the rung we have actually cleared is the
                // deeper of "what this tier asked for" and "what the transcript
                // already covers". Reading it off the transcript matters when a
                // playback session (or an earlier catch-up escalation) already
                // transcribed far past this tier: without it the ladder would
                // spend one full dispatch per rung re-confirming audio already
                // on disk. Both terms are monotone, so `current` never moves
                // backwards and the ladder still terminates.
                let clearedCoverage = max(job.desiredCoverageSec, outcome.transcriptCoverageSec)
                // playhead-8bp2 / playhead-onn6: an ad-scan re-drive NEVER walks
                // the tier ladder. Its whole contract is "finish the scan at the
                // depth the episode already reached" (see `adScanRedriveJob`), and
                // its budget ledger is the `adScanRedrive:<n>` ordinal carried in
                // its own `workKey`. The tier successor's key is rebuilt from the
                // BASE key below, which would LAUNDER that ordinal away: the
                // successor would terminate, `nextAdScanRedriveWorkKey` would parse
                // no ordinal, re-mint `:1`, and collide with the row still on disk
                // — silently collapsing onn6's budget from two passes to one.
                // Re-drives fall through to the `allTiersDone` arm, which is where
                // the ordinal is read and advanced correctly.
                let isAdScanRedrive = Self.adScanRedriveOrdinal(workKey: job.workKey) != nil
                let episodeDurationSec = isAdScanRedrive
                    ? nil
                    : await tierLadderEpisodeDuration(assetId: assetId)
                if !isAdScanRedrive, let nextCoverage = nextTierCoverage(
                    current: clearedCoverage,
                    episodeDurationSec: episodeDurationSec
                ) {
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
                    // playhead-work-journal-wiring: tier advance is a
                    // successful completion of the current tier. Emit
                    // `.finalized` so the journal records a clean
                    // success row for forensic audit. The next-tier
                    // job is inserted in the same transaction; its
                    // own `acquired` row will land when the scheduler
                    // claims it on a future iteration.
                    await emitJournalFinalized(episodeId: job.episodeId)
                    // playhead-beh3 (R2): record the grant-window
                    // outcome for the adaptive estimator. Tier advance
                    // = the slice ran end-to-end under our lease and
                    // produced the next-tier paused row, so the
                    // wall-clock interval since lease acquisition is a
                    // genuine grant-window observation. Flag-gated
                    // inside the helper — flag-off path never reaches
                    // the provider.
                    await recordGrantWindowObservationIfEnabled(
                        leaseAcquiredAt: leaseAcquiredAt
                    )
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Tier advancement: \(job.desiredCoverageSec)s -> \(nextCoverage)s for episode \(job.episodeId)")
                } else {
                    // playhead-onn6: the tiers are done, but "done" has never
                    // meant "the audio was read for ads". Mint a bounded
                    // re-drive when the coverage lane still holds resumable work
                    // and measured ad-scan coverage is not proven sufficient.
                    let redrive = await adScanRedriveJob(
                        for: job,
                        assetId: assetId,
                        outcome: outcome
                    )
                    await commitOutcomeArm(
                        "allTiersDone",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            progress: progress,
                            insertNextJob: redrive,
                            stateUpdate: .init(state: "complete", nextEligibleAt: nil, lastErrorCode: nil)
                        )
                    )
                    // playhead-work-journal-wiring: all tiers
                    // complete is the terminal success — the episode
                    // has reached its highest coverage target and
                    // will not requeue. Emit `.finalized` to seal
                    // the journal trail.
                    await emitJournalFinalized(episodeId: job.episodeId)
                    // playhead-beh3 (R2): record the grant-window
                    // outcome. All tiers done = the episode reached
                    // its highest coverage target under our lease;
                    // same accounting as the tier-advance arm above.
                    await recordGrantWindowObservationIfEnabled(
                        leaseAcquiredAt: leaseAcquiredAt
                    )
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Job \(job.jobId) complete (all tiers done)")
                }

            case .reachedTarget:
                // Re-queue, but with a guard against infinite loops for short episodes
                // or episodes that can never reach the desired coverage.
                if !Self.shouldRetryCoverageInsufficient(job: job, outcome: outcome) {
                    // playhead-onn6: cue coverage stalled, which says nothing
                    // about whether the semantic scan reached the audio. Same
                    // bounded re-drive decision as `allTiersDone`.
                    let redrive = await adScanRedriveJob(
                        for: job,
                        assetId: assetId,
                        outcome: outcome
                    )
                    await commitOutcomeArm(
                        "coverageInsufficient.noProgress",
                        AnalysisStore.ProcessJobOutcomeArmCommit(
                            jobId: job.jobId,
                            progress: progress,
                            insertNextJob: redrive,
                            stateUpdate: .init(
                                state: "complete",
                                nextEligibleAt: nil,
                                lastErrorCode: "coverageInsufficient:noProgress"
                            )
                        )
                    )
                    // playhead-work-journal-wiring (review-cycle-1):
                    // analysis_jobs row terminates `state="complete"`.
                    // The "no progress" suffix is forensic detail
                    // carried in `lastErrorCode`; semantically the job
                    // is done — coverage just was not reachable. Emit
                    // `.finalized` so the journal mirrors the row's
                    // terminal-success classification (orphan recovery
                    // treats `.finalized` as "nothing to resume").
                    await emitJournalFinalized(episodeId: job.episodeId)
                    // playhead-beh3 (R2): record the grant-window
                    // outcome. `coverageInsufficient.noProgress` is a
                    // graceful give-up that terminates `state="complete"`
                    // — the runner held the lease for the full grant
                    // window and processed every shard it could, the
                    // tier just couldn't reach the target. This is a
                    // valid grant-window observation: the slice ran
                    // end-to-end under our scheduler.
                    await recordGrantWindowObservationIfEnabled(
                        leaseAcquiredAt: leaseAcquiredAt
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
                        // playhead-onn6: the tier gave up on cue coverage. The
                        // ad scan is a separate question with its own budget.
                        let redrive = await adScanRedriveJob(
                            for: job,
                            assetId: assetId,
                            outcome: outcome
                        )
                        await commitOutcomeArm(
                            "coverageInsufficient.maxAttempts",
                            AnalysisStore.ProcessJobOutcomeArmCommit(
                                jobId: job.jobId,
                                progress: progress,
                                incrementAttempt: true,
                                insertNextJob: redrive,
                                stateUpdate: .init(
                                    state: "complete",
                                    nextEligibleAt: nil,
                                    lastErrorCode: "\(Self.maxAttemptsReachedPrefix)coverageInsufficient"
                                )
                            )
                        )
                        // playhead-work-journal-wiring (review-cycle-1):
                        // identical reasoning to noProgress above —
                        // `state="complete"` means the job will not
                        // requeue. The retry-budget exhaustion is a
                        // graceful give-up, not a runner failure, so
                        // `.finalized` is the correct lifecycle event.
                        await emitJournalFinalized(episodeId: job.episodeId)
                        // playhead-beh3 (R2): record the grant-window
                        // outcome. Max-attempts give-up terminates
                        // `state="complete"` after the runner exhausted
                        // retries; identical lease-end-to-end accounting
                        // to the noProgress arm above.
                        await recordGrantWindowObservationIfEnabled(
                            leaseAcquiredAt: leaseAcquiredAt
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
                        // playhead-work-journal-wiring (review-cycle-1):
                        // transient pause — the job will retry. Emit
                        // `.preempted/.pipelineError` to record the
                        // pause without misclassifying it as a
                        // terminal failure.
                        let coverageRequeueMetadata = await SliceCompletionInstrumentation
                            .recordPaused(
                                cause: .pipelineError,
                                deviceClass: DeviceClass.detect(),
                                sliceDurationMs: 0,
                                bytesProcessed: 0,
                                shardsCompleted: 0,
                                extras: [
                                    "stage": "analysisWorkScheduler.coverageInsufficientRequeue",
                                    "job_id": job.jobId,
                                    "attempts": "\(attempts)",
                                ]
                            )
                            .encodeJSON()
                        await emitJournalPreempted(
                            episodeId: job.episodeId,
                            cause: .pipelineError,
                            metadataJSON: coverageRequeueMetadata
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
                // skeptical-review-cycle-1: emit a `.preempted` journal
                // row so forensic debugging can distinguish a
                // model-unavailable pause from a thermal pause. Pre-fix,
                // these arms updated `analysis_jobs.state` only and the
                // journal carried no record of the transition.
                let modelBlockedMetadata = await SliceCompletionInstrumentation
                    .recordPaused(
                        cause: .modelTemporarilyUnavailable,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": "analysisWorkScheduler.blockedByModel",
                            "job_id": job.jobId,
                            "nextEligibleAt": "\(nextEligible)",
                        ]
                    )
                    .encodeJSON()
                await emitJournalPreempted(
                    episodeId: job.episodeId,
                    cause: .modelTemporarilyUnavailable,
                    metadataJSON: modelBlockedMetadata
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
                // skeptical-review-cycle-1: emit a `.preempted` row.
                // Distinguish thermal vs memory at the cause level —
                // `.thermal` for the thermal arm, `.pipelineError` for
                // memory pressure (no dedicated cause exists in the
                // 16-row taxonomy yet, so the metadata stage carries
                // the discriminator).
                let isThermal: Bool = {
                    if case .pausedForThermal = outcome.stopReason { return true }
                    return false
                }()
                let pauseCause: InternalMissCause = isThermal ? .thermal : .pipelineError
                let pauseStage = isThermal
                    ? "analysisWorkScheduler.pausedForThermal"
                    : "analysisWorkScheduler.memoryPressure"
                let pauseMetadata = await SliceCompletionInstrumentation
                    .recordPaused(
                        cause: pauseCause,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": pauseStage,
                            "job_id": job.jobId,
                            "nextEligibleAt": "\(nextEligible)",
                        ]
                    )
                    .encodeJSON()
                await emitJournalPreempted(
                    episodeId: job.episodeId,
                    cause: pauseCause,
                    metadataJSON: pauseMetadata
                )
                logger.info("Job \(job.jobId) paused for thermal/memory, retry in 30s")

            case .interrupted(let reason):
                // playhead-ngev (review r1): PREEMPTION-STYLE ACCOUNTING FOR A
                // RUN THAT WAS DISPLACED, NOT ONE THAT FAILED.
                //
                // The runner reports this when the shared transcript engine was
                // re-tasked out from under it — a scrub, a speed change, a
                // different episode. Nothing about the analysis went wrong, so
                // it must not consume one of the five PERMANENT attempts the
                // `.failed` arm below spends: at `maxAttemptCount` that arm
                // supersedes the job with `nextEligibleAt: nil`, and a
                // superseded row never comes back (`workKey` is UNIQUE and
                // `insertJob` is `INSERT OR IGNORE` over a key stable across
                // launches, so later enqueues are silently dropped). Five
                // scrubs is ordinary listening.
                //
                // This is the accounting `.cancelledByPlayback` already uses
                // for the same event class — "playback displaced the work" —
                // reached through a different door. `incrementAttempt` is left
                // at its `false` default, which is the entire fix.
                //
                // ONE DELIBERATE DEPARTURE from the `.preempted` /
                // `.cancelledByPlayback` shape: those requeue with
                // `nextEligibleAt: nil` (immediately eligible), which is safe
                // for them because a higher-lane job is now occupying the slot.
                // An interruption is reported WHILE PLAYBACK CONTINUES, so an
                // immediately re-admitted job can collide with the same live
                // owner again at once — a hot requeue loop burning battery
                // mid-episode. A flat floor off the first rung of the existing
                // ladder bounds that without ever growing, so the job still
                // retries indefinitely and still cannot die.
                //
                // `lastErrorCode` keeps the diagnosis. The row is `queued`, not
                // failed, so it does not read as a failure — it says why the
                // last pass produced nothing, which is what this bead is for.
                let interruptedNextEligible =
                    clock().timeIntervalSince1970 + Self.interruptedRequeueDelaySeconds
                await commitOutcomeArm(
                    "interrupted.requeue",
                    AnalysisStore.ProcessJobOutcomeArmCommit(
                        jobId: job.jobId,
                        progress: progress,
                        stateUpdate: .init(
                            state: "queued",
                            nextEligibleAt: interruptedNextEligible,
                            lastErrorCode: reason
                        )
                    )
                )
                let interruptedMetadata = await SliceCompletionInstrumentation
                    .recordPaused(
                        cause: .userPreempted,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": "analysisWorkScheduler.interruptedRequeue",
                            "job_id": job.jobId,
                            "runner_reason": reason,
                        ]
                    )
                    .encodeJSON()
                await emitJournalPreempted(
                    episodeId: job.episodeId,
                    cause: .userPreempted,
                    metadataJSON: interruptedMetadata
                )
                logger.info("Job \(job.jobId) interrupted (\(reason)), requeued without spending an attempt")

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
                                lastErrorCode: "\(Self.maxAttemptsReachedPrefix)\(reason)"
                            )
                        )
                    )
                    // playhead-work-journal-wiring (review-cycle-1):
                    // runner-driven failure exhausted retry budget —
                    // analysis_jobs is `superseded`, slot will not
                    // requeue. This is the most common terminal
                    // failure shape in production (decode/ASR/feature
                    // errors); the prior WIP missed it. Emit
                    // `.failed/.pipelineError` so orphan recovery and
                    // forensic audit see the terminal row.
                    let failedSupersedeMetadata = await SliceCompletionInstrumentation
                        .recordFailed(
                            cause: .pipelineError,
                            deviceClass: DeviceClass.detect(),
                            sliceDurationMs: 0,
                            bytesProcessed: 0,
                            shardsCompleted: 0,
                            extras: [
                                "stage": "analysisWorkScheduler.failedSupersede",
                                "job_id": job.jobId,
                                "runner_reason": reason,
                                "attempts": "\(attempts)",
                            ]
                        )
                        .encodeJSON()
                    await emitJournalFailed(
                        episodeId: job.episodeId,
                        cause: .pipelineError,
                        metadataJSON: failedSupersedeMetadata
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
                    // playhead-work-journal-wiring (review-cycle-1):
                    // transient runner failure — analysis_jobs goes
                    // to `state="failed"` with backoff but will retry
                    // on a future scheduler tick. Emit
                    // `.preempted/.pipelineError` (not `.failed`)
                    // because the slot is recoverable; using
                    // `.failed` would make orphan recovery treat the
                    // job as terminal.
                    let failedRequeueMetadata = await SliceCompletionInstrumentation
                        .recordPaused(
                            cause: .pipelineError,
                            deviceClass: DeviceClass.detect(),
                            sliceDurationMs: 0,
                            bytesProcessed: 0,
                            shardsCompleted: 0,
                            extras: [
                                "stage": "analysisWorkScheduler.failedRequeue",
                                "job_id": job.jobId,
                                "runner_reason": reason,
                                "attempts": "\(attempts)",
                            ]
                        )
                        .encodeJSON()
                    await emitJournalPreempted(
                        episodeId: job.episodeId,
                        cause: .pipelineError,
                        metadataJSON: failedRequeueMetadata
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
                // playhead-work-journal-wiring (review-cycle-1):
                // BG-task expiration is the canonical
                // `.preempted/.taskExpired` shape — runner reported
                // it from inside the decode loop. Job is requeued.
                let bgExpiredMetadata = await SliceCompletionInstrumentation
                    .recordPaused(
                        cause: .taskExpired,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": "analysisWorkScheduler.backgroundExpiredRequeue",
                            "job_id": job.jobId,
                        ]
                    )
                    .encodeJSON()
                await emitJournalPreempted(
                    episodeId: job.episodeId,
                    cause: .taskExpired,
                    metadataJSON: bgExpiredMetadata
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
                // playhead-work-journal-wiring (review-cycle-1):
                // playback started — analysis was preempted by
                // user-driven foreground work. Use `.userPreempted`
                // (the cause taxonomy distinguishes this from
                // `.userCancelled` which is an explicit user-issued
                // stop).
                let playbackCancelMetadata = await SliceCompletionInstrumentation
                    .recordPaused(
                        cause: .userPreempted,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": "analysisWorkScheduler.cancelledByPlaybackRequeue",
                            "job_id": job.jobId,
                        ]
                    )
                    .encodeJSON()
                await emitJournalPreempted(
                    episodeId: job.episodeId,
                    cause: .userPreempted,
                    metadataJSON: playbackCancelMetadata
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
                // playhead-work-journal-wiring (review-cycle-1):
                // higher-lane work bumped this slot. The runner
                // surfaced `.preempted` directly — emit the matching
                // journal row with `.userPreempted` so the cause
                // taxonomy is consistent across the two arms that
                // produce `.preempted` from non-cancel-driven sources.
                let runnerPreemptMetadata = await SliceCompletionInstrumentation
                    .recordPaused(
                        cause: .userPreempted,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": "analysisWorkScheduler.preemptedRequeue",
                            "job_id": job.jobId,
                        ]
                    )
                    .encodeJSON()
                await emitJournalPreempted(
                    episodeId: job.episodeId,
                    cause: .userPreempted,
                    metadataJSON: runnerPreemptMetadata
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
                            lastErrorCode: "\(Self.maxAttemptsReachedPrefix)\(error.localizedDescription)"
                        )
                    )
                )
                // playhead-work-journal-wiring (review-cycle-1):
                // uncaught exception exhausted retries — terminal
                // failure. Emit `.failed/.pipelineError`.
                let outerSupersedeMetadata = await SliceCompletionInstrumentation
                    .recordFailed(
                        cause: .pipelineError,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": "analysisWorkScheduler.outerCatchSupersede",
                            "job_id": job.jobId,
                            "error": error.localizedDescription,
                            "attempts": "\(attempts)",
                        ]
                    )
                    .encodeJSON()
                await emitJournalFailed(
                    episodeId: job.episodeId,
                    cause: .pipelineError,
                    metadataJSON: outerSupersedeMetadata
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
                // playhead-work-journal-wiring (review-cycle-1):
                // uncaught exception, transient retry — emit
                // `.preempted/.pipelineError`. Same shape as
                // `failed.requeue`: the slot is recoverable, so
                // `.preempted` (not `.failed`) keeps orphan recovery
                // honest about the slot's resumability.
                let outerRequeueMetadata = await SliceCompletionInstrumentation
                    .recordPaused(
                        cause: .pipelineError,
                        deviceClass: DeviceClass.detect(),
                        sliceDurationMs: 0,
                        bytesProcessed: 0,
                        shardsCompleted: 0,
                        extras: [
                            "stage": "analysisWorkScheduler.outerCatchRequeue",
                            "job_id": job.jobId,
                            "error": error.localizedDescription,
                            "attempts": "\(attempts)",
                        ]
                    )
                    .encodeJSON()
                await emitJournalPreempted(
                    episodeId: job.episodeId,
                    cause: .pipelineError,
                    metadataJSON: outerRequeueMetadata
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

    // MARK: - WorkJournal lifecycle emission helpers (work-journal-wiring)
    //
    // **Why these exist.** Pre-this-fix, `processJob` only emitted a
    // `recordPreempted(...)` row from the cancel-mid-decode catch arm
    // — and even that was a no-op against the default
    // `NoopWorkJournalRecorder` because `PlayheadRuntime` never
    // installed a real recorder. The captured production DB had 66 of
    // 66 `acquired` rows and zero terminal rows, which made the
    // journal useless for forensic debugging when background analysis
    // halted.
    //
    // The audit at `docs/audits/2026-04-25-episode-job-dag-audit.md`
    // (Gap-1) called out the cancel arms as the priority. The
    // review-cycle-1 expansion below covers the full outcome-arm
    // taxonomy in `processJob` so the journal stays complete on
    // every terminal/recoverable transition (tracked arms enumerated
    // on `commitOutcomeArm` above).
    //
    // skeptical-review-cycle-3 M-A: the policy on pause-only arms
    // flipped during cycle-1. Originally those arms (`blockedByModel`,
    // `pausedThermalOrMemory`) deliberately did NOT emit on the
    // theory that orphan recovery + `analysis_jobs.state` was enough
    // to disambiguate them. Cycle-1 M4 added `.preempted` rows to
    // both arms after a forensic-debugging gap was identified — the
    // captured DB on a stuck device showed `state='blocked:modelUnavailable'`
    // but the journal carried no record of the pause transition,
    // forcing operators to cross-reference logs. The new emission is
    // structurally safe: `requeueOrphanedLease` preserves non-`'running'`
    // states (AnalysisStore.swift:6485), and a `decisionEvent ==
    // .preempted` resume branch is identical to `decisionEvent == nil`
    // for these state strings. Each helper is a thin wrapper
    // around the matching `WorkJournalRecording` method that:
    //
    //   - Captures the recorder + episodeId on the actor (snapshot
    //     once per call so the helper is `nonisolated` from the
    //     recorder's perspective).
    //   - Skips emission when `lostOwnership == true` — orphan
    //     recovery has already reclaimed the lease and the new owner
    //     will write its own journal row when its outcome arm fires.
    //     Writing here would corrupt the new owner's audit trail with
    //     a mid-stream row attributed to the old owner's epoch.
    //   - Awaits the recorder. The recorder is best-effort (errors
    //     logged + swallowed at the recorder), so a journal-append
    //     failure cannot disrupt the scheduler's state machine.
    //
    // The recorder is invoked AFTER `commitOutcomeArm` lands the row's
    // state-machine update, so the journal row is a tail emission
    // that observes (not drives) the terminal transition. If the
    // process dies between `commitOutcomeArm` and the recorder call,
    // the analysis_jobs row is still terminally correct — only the
    // journal row is missing. Orphan recovery already tolerates
    // missing journal rows (Gap-1's "empty journal" case routes via
    // `decisionEvent == .none` → resume branch).

    /// Emit a `.finalized` row for `episodeId`. Called from the
    /// success outcome arms (`tierAdvance`, `allTiersDone`,
    /// `coverageInsufficient.noProgress`,
    /// `coverageInsufficient.maxAttempts`) — all cases where
    /// `analysis_jobs.state="complete"`.
    private func emitJournalFinalized(episodeId: String) async {
        guard !lostOwnership else { return }
        let recorder = workJournalRecorder
        await recorder.recordFinalized(episodeId: episodeId)
    }

    /// Emit a `.failed` row for `episodeId`. Called from the
    /// terminal-failure arms: `assetResolution.{supersede,requeue}`,
    /// `cancelCatch.supersede`, `failed.supersede`, and
    /// `outerCatch.supersede`. `metadataJSON` carries caller-specific
    /// context (job id, stage, attempt count, runner_reason) for the
    /// journal row's `metadata` column.
    private func emitJournalFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        guard !lostOwnership else { return }
        let recorder = workJournalRecorder
        await recorder.recordFailed(
            episodeId: episodeId,
            cause: cause,
            metadataJSON: metadataJSON
        )
    }

    /// Emit a `.failed` row pinned to a specific leased job generation.
    /// The stale-canonical branch inserts a replacement job for the same
    /// episode before emitting its journal tail; an episode-only recorder
    /// backed by `fetchLatestJobForEpisode` would otherwise attach the
    /// terminal row to that replacement job. Use the production recorder's
    /// explicit-generation seam when available, while preserving the no-op
    /// behavior of `NoopWorkJournalRecorder` and lightweight test recorders.
    private func emitJournalFailedForJobSnapshot(
        episodeId: String,
        jobSnapshot: AnalysisJob?,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        guard !lostOwnership else { return }
        let recorder = workJournalRecorder
        if let storeRecorder = recorder as? AnalysisStoreWorkJournalRecorder {
            guard let jobSnapshot else {
                logger.warning("Skipping WorkJournal failed row for \(episodeId): missing leased job snapshot")
                return
            }
            await storeRecorder.recordFailed(
                episodeId: episodeId,
                generationID: jobSnapshot.generationID,
                schedulerEpoch: jobSnapshot.schedulerEpoch,
                cause: cause,
                metadataJSON: metadataJSON
            )
        } else {
            await recorder.recordFailed(
                episodeId: episodeId,
                cause: cause,
                metadataJSON: metadataJSON
            )
        }
    }

    /// Emit a `.preempted` row for `episodeId`. Called from the
    /// recoverable-pause arms: `cancelRace.releaseLease`,
    /// `cancelCatch.revertQueued` (the cancel-mid-decode requeue),
    /// `coverageInsufficient.requeue`, `failed.requeue`,
    /// `backgroundExpired.requeue`, `cancelledByPlayback.requeue`,
    /// `preempted.requeue`, and `outerCatch.requeue`. All represent
    /// "lease released, slot will retry" — distinct from `.failed`
    /// which marks a terminal non-recoverable failure.
    private func emitJournalPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        guard !lostOwnership else { return }
        let recorder = workJournalRecorder
        await recorder.recordPreempted(
            episodeId: episodeId,
            cause: cause,
            metadataJSON: metadataJSON
        )
    }

    // MARK: - playhead-beh3 adaptive estimator write seam

    /// playhead-beh3 (Phase 3 deliverable 5, R2): record one grant-window
    /// observation against the adaptive estimator. Invoked from every
    /// success outcome arm in `processJob` (`tierAdvance`, `allTiersDone`,
    /// `coverageInsufficient.{noProgress,maxAttempts}`) — those are the
    /// arms where the runner held the lease end-to-end and the resulting
    /// wall-clock interval is a faithful "grant-window slice completion"
    /// duration the spec asks the estimator to learn from.
    ///
    /// Failure arms excluded — rationale (R3 explicit): `.failed`,
    /// `.pausedForThermal`, `.memoryPressure`, `.blockedByModel`,
    /// `.backgroundExpired`, `.cancelledByPlayback`, `.preempted`, and
    /// the outer-catch / cancel-catch paths all SKIP this helper. These
    /// outcomes either (a) terminated mid-slice (thermal trip, BG-task
    /// expiry, runner exception), or (b) never produced a real slice in
    /// the first place (model unavailable, playback preemption). The
    /// resulting wall-clock interval is not a "we ran a slice end-to-end"
    /// observation; recording would teach the EWMA that grant windows
    /// are bounded by external events (thermals, OS time budgets) rather
    /// than the device's actual throughput. The trade-off is a mild
    /// survivorship bias: a device that consistently trips thermals on
    /// long slices will never feed those long-and-aborted windows into
    /// the EWMA, so the estimator may converge slightly higher than the
    /// "true" sustainable budget. The clamp band (≤2× seed) caps the
    /// magnitude of that drift, and the divergence-revert path catches
    /// pathological cases. We prefer the bias direction (slightly over-
    /// optimistic) over polluting the estimator with externally-bounded
    /// durations.
    ///
    /// Flag gating: the method is the SINGLE production write site for
    /// the estimator. It returns immediately when
    /// `config.useAdaptiveDeviceProfile == false`, so a flag-off run
    /// never touches the SwiftData row, never invokes the provider, and
    /// never mutates estimator state — that is the byte-identical
    /// rollback contract from the bead spec, mechanically enforced here
    /// rather than at every call site.
    ///
    /// `leaseAcquiredAt` capture point: the timestamp is captured at
    /// the top of `processJob` BEFORE the `await store.acquireLeaseWithJournal(...)`
    /// call, identical to the `now` value the SQL layer is given as its
    /// transaction timestamp. The measured `grantWindowSeconds`
    /// therefore includes any SQLite lease-acquisition latency. This is
    /// intentional: the next slice will also pay that cost, so an EWMA
    /// trained on "from-attempted-acquire" durations predicts the next
    /// slice's wall-clock budget more faithfully than a "from-confirmed-
    /// acquire" alternative would.
    ///
    /// `lostOwnership` mirror: the success arms commit BEFORE the
    /// `lostOwnership` short-circuit higher up (the renewer cannot flip
    /// the flag across an atomic `commitOutcomeArm` await), but we
    /// re-check anyway so a future refactor that moves the call site
    /// later in `processJob` cannot accidentally fire after the lease
    /// was reclaimed by orphan recovery.
    ///
    /// Non-positive guard: the estimator's `apply(...)` already drops
    /// observations whose `grantWindowSeconds <= 0` (the never-zero
    /// floor invariant). We compute the duration off `clock()` here, so
    /// in practice the duration is always strictly positive — the math
    /// layer's guard is defense-in-depth, not the primary check.
    ///
    /// Device-class resolution: detect inside the helper (one call to
    /// the pure `DeviceClass.detect()` mapping) rather than capturing
    /// at lease-acquisition time. The earlier draft hopped to
    /// `capabilitiesService.currentSnapshot.deviceClass` between lease
    /// acquisition and the lease-renewal task arming — that suspension
    /// point sometimes serialized poorly with the run-loop's mailbox
    /// under real-time tests, leaving `pollUntil`-style waits stuck.
    /// `DeviceClass.detect()` is a pure `utsname.machine` lookup; the
    /// vanishing mid-run device-class-rotation concern is moot at the
    /// resolution this estimator works at.
    ///
    /// Internal (rather than `private`) visibility so the R2 write-seam
    /// tests can drive the helper directly. The success outcome arms in
    /// `processJob` cannot be reached cleanly under stub inputs (see
    /// the `AnalysisWorkSchedulerJournalEmissionTests` file header for
    /// the full reasoning), so a unit test that synthesizes a lease-
    /// acquired timestamp and calls this method is the cheapest way to
    /// pin the flag-gating contract. The method is still called only
    /// from the success arms in production.
    func recordGrantWindowObservationIfEnabled(
        leaseAcquiredAt: TimeInterval
    ) async {
        // Single flag check, single early return — the entire estimator
        // surface is bypassed when the feature is OFF.
        guard config.useAdaptiveDeviceProfile else { return }
        // Mirror the journal helpers: if we no longer own the lease,
        // the slice work didn't terminate under our scheduler's
        // observation. Recording would attribute the new owner's
        // outcome to our estimator state.
        guard !lostOwnership else { return }

        let nowDate = clock()
        let grantWindowSeconds = nowDate.timeIntervalSince1970 - leaseAcquiredAt
        // Drop non-finite or non-positive durations defensively: a
        // clock-skew event (NTP step backward, simulated-clock test
        // that didn't advance) or a corrupt clock (`Date` with a
        // non-finite reference value — pathological but representable
        // in the Swift type) would otherwise produce a meaningless
        // observation. The estimator's math layer also drops these
        // (R5 hardening tightened the entry guard to `isFinite && > 0`),
        // but filtering here means we don't even build the value type
        // for the no-op case. Defense-in-depth at the source mirrors
        // the math layer's invariant so both ends agree on what
        // qualifies as a real grant-window observation.
        guard grantWindowSeconds.isFinite, grantWindowSeconds > 0 else { return }

        let deviceClass = DeviceClass.detect()
        let seed = DeviceClassProfile.fallback(for: deviceClass)
        let observation = GrantWindowObservation(
            grantWindowSeconds: grantWindowSeconds,
            observedAt: nowDate
        )
        _ = await learnedDeviceProfileProvider.recordObservation(
            observation,
            deviceClass: deviceClass,
            seed: seed
        )
    }

    // MARK: - Tier Definitions

    /// playhead-8bp2: minimum gap between two rungs of the ladder.
    ///
    /// The tier-advance arm derives a rung's `workKey` suffix as
    /// `":\(Int(nextCoverage))"`, so two rungs less than a second apart would
    /// TRUNCATE to the same key — and `insertJob` is `INSERT OR IGNORE`, which
    /// would swallow the deeper rung silently. Keeping the rungs a whole second
    /// apart is what makes the suffix a faithful name for the rung. Concretely:
    /// a 900.4 s episode must not produce both a `:900` T2 rung and a `:900`
    /// duration rung.
    private static let tierLadderMinimumRungGapSeconds: Double = 1

    /// playhead-8bp2: an upper sanity bound on a duration used as a tier rung.
    ///
    /// `analysis_assets.episodeDurationSec` is a `REAL` read off disk. A rung
    /// becomes a `workKey` suffix via `Int(nextCoverage)`, and `Int(_: Double)`
    /// TRAPS on a non-finite or out-of-range value — a corrupt row would crash
    /// the scheduler rather than degrade. 24 hours is far past any podcast
    /// episode and comfortably inside `Int`; anything non-finite or beyond it is
    /// treated as unusable, which costs only the pre-8bp2 ceiling.
    static let maximumTierLadderDurationSeconds: Double = 24 * 60 * 60

    /// playhead-8bp2: the coverage-depth ladder a background episode walks,
    /// ASCENDING and terminated by the episode itself.
    ///
    /// **Why the episode duration is the last rung.** `desiredCoverageSec` is
    /// the transcription budget, literally — `AnalysisJobRunner` keeps only
    /// `allShards.filter { $0.startTime < request.desiredCoverageSec }`. The
    /// configured tiers stop at `t2DepthSeconds` (900 s), so an episode nobody
    /// plays could never be transcribed past fifteen minutes no matter how many
    /// clean passes it got, and "all tiers done" terminated it `complete` — after
    /// which `workKey` UNIQUE + `INSERT OR IGNORE` swallow every re-enqueue until
    /// ``AnalysisJobReconciler``'s 7-day GC deletes the terminal row and step 7
    /// re-mints the episode back at `defaultT0DepthSeconds`. So the block is a
    /// WEEKLY RESTART AT 90 SECONDS, not a permanent death — which is worse to
    /// diagnose and no better to live with. Measured
    /// on the 2026-07-30 device pull, the 34 episodes over 15 minutes held
    /// 1,900 minutes of audio and this ladder could reach at most 510 of them
    /// (26.8%); the 797 minutes actually transcribed got past that ceiling only
    /// where a listener's playhead had escalated the target through
    /// `dispatchForegroundCatchup`. The rungs a playhead already reaches must
    /// not be the only rungs that exist.
    ///
    /// **Bounded, and by construction.** The ladder is at most four rungs: the
    /// duration, plus each configured tier that sits at least
    /// ``tierLadderMinimumRungGapSeconds`` below it. Configured tiers at or past
    /// the duration are DROPPED, so we never set a target the audio cannot
    /// satisfy (which would burn a pass reaching for seconds that do not exist),
    /// and the list is strictly ascending, so ``nextTierCoverage(current:tiers:episodeDurationSec:)``
    /// strictly increases and terminates. With no usable duration we return
    /// exactly the configured tiers — the pre-8bp2 ladder — so a missing or
    /// corrupt `episodeDurationSec` degrades rather than guessing.
    static func coverageTierLadder(tiers: [Double], episodeDurationSec: Double?) -> [Double] {
        // The configured tiers are `Decodable` (hot config / test harness), so
        // they get the same finiteness and range screen as the duration —
        // `PreAnalysisConfig`'s strictly-ascending validator accepts
        // `90 < 300 < .infinity`, and every rung ends up in `Int(_:)`.
        let ascending = tiers
            .filter { $0.isFinite && $0 > 0 && $0 <= maximumTierLadderDurationSeconds }
            .sorted()
        guard let duration = episodeDurationSec,
              duration.isFinite,
              duration > 0,
              duration <= maximumTierLadderDurationSeconds else { return ascending }
        return ascending.filter { $0 <= duration - tierLadderMinimumRungGapSeconds } + [duration]
    }

    /// Returns the next tier's coverage target, or nil if all tiers are complete.
    static func nextTierCoverage(
        current: Double,
        tiers: [Double],
        episodeDurationSec: Double?
    ) -> Double? {
        coverageTierLadder(tiers: tiers, episodeDurationSec: episodeDurationSec)
            .first { $0 > current }
    }

    /// Instance wrapper binding the configured tier depths.
    private func nextTierCoverage(current: Double, episodeDurationSec: Double?) -> Double? {
        Self.nextTierCoverage(
            current: current,
            tiers: [config.defaultT0DepthSeconds, config.t1DepthSeconds, config.t2DepthSeconds],
            episodeDurationSec: episodeDurationSec
        )
    }

    /// playhead-8bp2: the episode's own duration, for the last rung of the tier
    /// ladder. `nil` — including on a read failure — means "use the configured
    /// tiers only", i.e. exactly the pre-8bp2 ladder. Failing CLOSED here would
    /// mean deepening on a guess; failing open costs at worst the old ceiling.
    /// Range/finiteness sanity lives in ``coverageTierLadder(tiers:episodeDurationSec:)``
    /// so the rule is testable without a store.
    private func tierLadderEpisodeDuration(assetId: String) async -> Double? {
        do {
            return try await store.fetchAsset(id: assetId)?.episodeDurationSec
        } catch {
            logger.warning(
                "playhead-8bp2: episode duration read failed for asset \(assetId): \(error); tier ladder falls back to configured tiers"
            )
            return nil
        }
    }

    // MARK: - Ad-scan re-drive minting (playhead-onn6)

    /// playhead-onn6: the re-drive `analysis_jobs` row to insert alongside a
    /// terminal outcome arm, or `nil` when this episode gets no further pass.
    ///
    /// **Why the terminal arms are the hook.** `shouldSkipSemanticBackfill`
    /// (playhead-i7qe) governs a run that is ALREADY being dispatched; it cannot
    /// cause one. Once the tiers finish, `analysis_jobs.state` is `'complete'` and
    /// only `queued` / `paused` / retryable-`failed` rows dispatch, so the episode
    /// had no dispatchable job and no path to another scan — measured on the
    /// 2026-07-29 device pull, 20 of the 34 episodes over 15 minutes were in
    /// exactly that state. These three arms are the only places the scheduler
    /// says "this episode is done", so they are the only places that can decide
    /// it is not.
    ///
    /// **Why this drains the orphaned coverage lane rather than duplicating it.**
    /// `BackfillJobRunner.runPendingBackfill` cannot be invoked standalone — it
    /// needs `AssetInputs` (transcript segments, evidence catalog, transcript
    /// version, planner context, acoustic breaks) that only the analysis-job lane
    /// assembles. So the queued `backfill_jobs` rows are SELECTED here (the query
    /// that did not exist) and DISPATCHED by the pass this mint causes: the
    /// runner's M-5 idempotency branch re-enqueues every non-complete row for the
    /// asset through the admission controller instead of inserting duplicates.
    /// Building a second standalone drain lane would mean re-architecting the
    /// scheduler, which is out of this bead's scope.
    ///
    /// Reads are ordered cheapest-first so an episode that is not a candidate
    /// pays almost nothing: the budget check is pure string work, the resumable
    /// count is one indexed `COUNT(*)`, and only then do we pay for the coverage
    /// summary (four prepared statements plus an interval union over the asset's
    /// fast-chunk set). Same idiom as the i7qe call site.
    ///
    /// **Lease safety.** These reads suspend, so the renewer can flip
    /// `lostOwnership` between them and the commit. That is safe and does not
    /// weaken the playhead-5uvz.3 Gap-3 invariant: the reads are OUTSIDE the arm,
    /// the arm itself is still one atomic `commitOutcomeArm` await, and that
    /// helper re-checks `lostOwnership` before writing. A reclaim mid-decision
    /// costs two wasted reads and inserts nothing.
    private func adScanRedriveJob(
        for job: AnalysisJob,
        assetId: String,
        outcome: AnalysisOutcome
    ) async -> AnalysisJob? {
        guard let workKey = Self.nextAdScanRedriveWorkKey(for: job) else { return nil }

        let resumableCount: Int
        do {
            resumableCount = try await store.countResumableBackfillJobs(assetId: assetId)
        } catch {
            // A read failure must not mint: we would be guessing that work is
            // outstanding, and guessing "yes" is how an unbounded retry starts.
            logger.warning(
                "playhead-onn6: resumable coverage-lane count failed for asset \(assetId): \(error); no ad-scan re-drive"
            )
            return nil
        }
        guard resumableCount > 0 else { return nil }

        let adScanFraction: Double?
        do {
            adScanFraction = try await store
                .fetchCoverageSummariesByAssetIds([assetId])[assetId]?
                .adScanFraction
        } catch {
            logger.warning(
                "playhead-onn6: ad-scan coverage read failed for asset \(assetId): \(error); no ad-scan re-drive"
            )
            return nil
        }

        guard Self.shouldMintAdScanRedrive(
            adScanFraction: adScanFraction,
            resumableCoverageJobCount: resumableCount
        ) else {
            return nil
        }

        let now = clock().timeIntervalSince1970
        logger.info(
            """
            playhead-onn6: minting ad-scan re-drive for asset \(assetId) \
            (workKey suffix \(Self.adScanRedriveWorkKeyMarker):\
            \(Self.adScanRedriveOrdinal(workKey: workKey) ?? 0), \
            resumableCoverageJobs=\(resumableCount))
            """
        )
        return AnalysisJob(
            jobId: UUID().uuidString,
            jobType: job.jobType,
            episodeId: job.episodeId,
            podcastId: job.podcastId,
            analysisAssetId: assetId,
            workKey: workKey,
            sourceFingerprint: job.sourceFingerprint,
            downloadId: job.downloadId,
            // Background lane. A re-drive is repair work behind everything the
            // user is waiting on; it must never preempt a `.now`/`.soon` job.
            priority: 0,
            // The tier this episode already reached — a re-drive finishes the
            // scan, it does not deepen coverage targets.
            desiredCoverageSec: job.desiredCoverageSec,
            // Seeded from the OUTCOME, not zero. `shouldRetryCoverageInsufficient`
            // compares the next outcome against these fields, so zeroes would make
            // a no-op re-drive look like progress and re-queue it.
            featureCoverageSec: outcome.featureCoverageSec,
            transcriptCoverageSec: outcome.transcriptCoverageSec,
            cueCoverageSec: outcome.cueCoverageSec,
            // `queued`, not the tier ladder's `paused`: this row has never run,
            // and `paused` is in `fetchStrandedActiveJobs`' active set.
            state: "queued",
            attemptCount: 0,
            nextEligibleAt: nil,
            leaseOwner: nil,
            leaseExpiresAt: nil,
            lastErrorCode: nil,
            createdAt: now,
            updatedAt: now
        )
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
        let jobUsesCanonicalFullFileSHA = CrossUserAnalysisShareKey.isCanonicalFullFileSHA(job.sourceFingerprint)
        if jobUsesCanonicalFullFileSHA,
           let exactMatch = try await store.fetchAssetByEpisodeId(
               job.episodeId,
               assetFingerprint: job.sourceFingerprint
           ) {
            guard !lostOwnership else { throw CancellationError() }
            await consumePendingProbedDurationIfPresent(for: job, assetId: exactMatch.id)
            guard !lostOwnership else { throw CancellationError() }
            try await store.updateJobAnalysisAssetId(
                jobId: job.jobId,
                analysisAssetId: exactMatch.id
            )
            return exactMatch.id
        }

        let currentAudioFingerprint = jobUsesCanonicalFullFileSHA
            ? await downloadManager.fingerprint(for: job.episodeId)
            : nil
        guard !lostOwnership else { throw CancellationError() }

        if jobUsesCanonicalFullFileSHA,
           let weakMatch = try await fetchUpgradeableWeakAssetForCanonicalSHA(
               episodeId: job.episodeId,
               canonicalFingerprint: job.sourceFingerprint,
               currentAudioFingerprint: currentAudioFingerprint
           ) {
            guard !lostOwnership else { throw CancellationError() }
            let preservedWeakFingerprint = currentAudioFingerprint?.weak
                ?? weakMatch.weakFingerprint
                ?? weakMatch.assetFingerprint
            try await store.updateAssetFingerprint(
                id: weakMatch.id,
                assetFingerprint: job.sourceFingerprint,
                weakFingerprint: preservedWeakFingerprint
            )
            guard !lostOwnership else { throw CancellationError() }
            await consumePendingProbedDurationIfPresent(for: job, assetId: weakMatch.id)
            guard !lostOwnership else { throw CancellationError() }
            try await store.updateJobAnalysisAssetId(
                jobId: job.jobId,
                analysisAssetId: weakMatch.id
            )
            return weakMatch.id
        }

        if let existing = try await store.fetchAssetByEpisodeId(job.episodeId) {
            guard !lostOwnership else { throw CancellationError() }
            if jobUsesCanonicalFullFileSHA {
                if CrossUserAnalysisShareKey.isCanonicalFullFileSHA(existing.assetFingerprint) {
                    // Same episode id can legitimately point at changed
                    // bytes after a feed correction or re-download. Keep
                    // the old analysis anchored to its SHA and create a
                    // new asset for this file below.
                } else {
                    if Self.canUpgradeWeakAssetToCanonicalSHA(
                        existing,
                        jobSourceFingerprint: job.sourceFingerprint,
                        currentAudioFingerprint: currentAudioFingerprint
                    ) {
                        let preservedWeakFingerprint = currentAudioFingerprint?.weak
                            ?? existing.weakFingerprint
                            ?? existing.assetFingerprint
                        try await store.updateAssetFingerprint(
                            id: existing.id,
                            assetFingerprint: job.sourceFingerprint,
                            weakFingerprint: preservedWeakFingerprint
                        )
                        guard !lostOwnership else { throw CancellationError() }
                        await consumePendingProbedDurationIfPresent(for: job, assetId: existing.id)
                        guard !lostOwnership else { throw CancellationError() }
                        try await store.updateJobAnalysisAssetId(
                            jobId: job.jobId,
                            analysisAssetId: existing.id
                        )
                        return existing.id
                    }
                }
            } else {
                await consumePendingProbedDurationIfPresent(for: job, assetId: existing.id)
                guard !lostOwnership else { throw CancellationError() }
                try await store.updateJobAnalysisAssetId(
                    jobId: job.jobId,
                    analysisAssetId: existing.id
                )
                return existing.id
            }
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
        let stashedDuration = pendingProbedEpisodeDurations.removeValue(
            forKey: Self.durationStashKey(
                episodeId: job.episodeId,
                sourceFingerprint: job.sourceFingerprint
            )
        )
        // playhead-0hi9: record the weak fingerprint alongside the canonical
        // SHA. This is the row Pipeline B mints; carrying the weak identity
        // makes the pair symmetric with the Pipeline A placeholder, so
        // whichever row a later run finds first, `fetchAssetsByEpisodeId(_:
        // weakFingerprint:)` and `canUpgradeWeakAssetToCanonicalSHA` can
        // recognise it as the same audio instead of minting a third row.
        // `currentAudioFingerprint` is already resolved above for the
        // canonical-SHA case; the second read covers the non-canonical case,
        // and `fingerprint(for:)` now survives relaunch via the `.pin`
        // sidecar.
        let observedWeak: String?
        if let currentAudioFingerprint {
            observedWeak = currentAudioFingerprint.weak
        } else {
            observedWeak = await downloadManager.fingerprint(for: job.episodeId)?.weak
        }
        let insertWeakFingerprint = AudioFingerprint.nonEmptyWeak(observedWeak)
        // playhead-0hi9: `downloadManager.fingerprint(for:)` above is a NEW
        // suspension point on this path, and the lease-renewal task flips
        // `lostOwnership` from outside. Inserting after the lease was
        // reclaimed mints a second `analysis_assets` row for an episode the
        // new owner is already working — the exact defect this bead removes.
        //
        // R4: that window is closed by the pre-existing
        // `guard !lostOwnership` a few lines below, and R3's duplicate guard
        // at this point has been REMOVED rather than left in place as an
        // "untested but additive" rail. It was neither: `AnalysisAsset(...)`
        // is a memberwise struct init containing no `await`, so on this actor
        // no suspension can occur between the two points and `lostOwnership`
        // is provably identical at both. The guard had no behaviour distinct
        // from its neighbour, which is why no test could reach it — and a
        // comment claiming an untested safety property that does not exist
        // costs the next reader more than the line saved.
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: job.episodeId,
            assetFingerprint: job.sourceFingerprint,
            weakFingerprint: insertWeakFingerprint,
            // playhead-b8hj: container-PORTABLE, never `absoluteString`. The
            // audio-cache path carries the Data-container UUID, which iOS
            // rewrites on reinstall and restore, so an absolute string is a
            // dead reference the moment the container moves — and this column
            // is write-once, with no repair path.
            sourceURL: AudioCacheLocation.portableString(for: localAudioURL.url),
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

    private func consumePendingProbedDurationIfPresent(
        for job: AnalysisJob,
        assetId: String
    ) async {
        let key = Self.durationStashKey(
            episodeId: job.episodeId,
            sourceFingerprint: job.sourceFingerprint
        )
        guard let stashedDuration = pendingProbedEpisodeDurations.removeValue(forKey: key) else {
            return
        }
        do {
            try await store.updateEpisodeDuration(
                id: assetId,
                episodeDurationSec: stashedDuration
            )
        } catch {
            logger.warning("Failed to apply stashed probed duration for \(job.episodeId): \(error)")
        }
    }

    private enum CachedCanonicalFingerprintStatus {
        case unchangedOrUnknown
        case changed(currentFingerprint: String)
    }

    private func cachedCanonicalFingerprintStatus(
        for job: AnalysisJob,
        localAudioURL: LocalAudioURL
    ) async -> CachedCanonicalFingerprintStatus {
        guard CrossUserAnalysisShareKey.isCanonicalFullFileSHA(job.sourceFingerprint) else {
            return .unchangedOrUnknown
        }
        do {
            // The file on disk is the binding source of truth. The
            // fingerprint cache is only a fallback because it can lag a
            // feed correction or cache rewrite in tests and recovery paths.
            let currentFingerprint = try FileHasher.sha256(fileURL: localAudioURL.url)
            return currentFingerprint == job.sourceFingerprint
                ? .unchangedOrUnknown
                : .changed(currentFingerprint: currentFingerprint)
        } catch {
            if let currentStrongFingerprint = await downloadManager.fingerprint(for: job.episodeId)?.strong,
               CrossUserAnalysisShareKey.isCanonicalFullFileSHA(currentStrongFingerprint) {
                return currentStrongFingerprint == job.sourceFingerprint
                    ? .unchangedOrUnknown
                    : .changed(currentFingerprint: currentStrongFingerprint)
            }
            logger.warning("Could not hash cached audio for stale canonical-SHA check on job \(job.jobId): \(error)")
            return .unchangedOrUnknown
        }
    }

    private func replacementJobForCurrentCanonicalAudio(
        replacing staleJob: AnalysisJob,
        currentFingerprint: String
    ) -> AnalysisJob {
        let now = clock().timeIntervalSince1970
        let workKey = replacementWorkKeyForCurrentCanonicalAudio(
            replacing: staleJob,
            currentFingerprint: currentFingerprint
        )
        return AnalysisJob(
            jobId: UUID().uuidString,
            jobType: staleJob.jobType,
            episodeId: staleJob.episodeId,
            podcastId: staleJob.podcastId,
            analysisAssetId: nil,
            workKey: workKey,
            sourceFingerprint: currentFingerprint,
            downloadId: staleJob.downloadId,
            priority: staleJob.priority,
            desiredCoverageSec: staleJob.desiredCoverageSec,
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
    }

    private func replacementWorkKeyForCurrentCanonicalAudio(
        replacing staleJob: AnalysisJob,
        currentFingerprint: String
    ) -> String {
        let baseWorkKey = AnalysisJob.computeWorkKey(
            fingerprint: currentFingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: staleJob.jobType
        )
        let components = staleJob.workKey.split(separator: ":", omittingEmptySubsequences: false)
        guard let jobTypeIndex = components.firstIndex(of: Substring(staleJob.jobType)) else {
            return baseWorkKey
        }
        let suffixStart = components.index(after: jobTypeIndex)
        guard suffixStart < components.endIndex else {
            return baseWorkKey
        }
        let suffix = components[suffixStart...]
            .map(String.init)
            .joined(separator: ":")
        return "\(baseWorkKey):\(suffix)"
    }

    private func cachedAudioCanonicalFingerprint(cachedURL: URL, episodeId: String) -> String? {
        do {
            return try FileHasher.sha256(fileURL: cachedURL)
        } catch {
            logger.warning("Could not hash cached audio for canonical-SHA duration persistence on episode \(episodeId): \(error)")
            return nil
        }
    }

    private func fetchUpgradeableWeakAssetForCanonicalSHA(
        episodeId: String,
        canonicalFingerprint: String,
        currentAudioFingerprint: AudioFingerprint?
    ) async throws -> AnalysisAsset? {
        guard let currentAudioFingerprint,
              currentAudioFingerprint.strong == canonicalFingerprint else {
            return nil
        }
        let currentWeakFingerprint = currentAudioFingerprint.weak
        guard !currentWeakFingerprint.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return nil
        }
        if let asset = try await store.fetchAssetByEpisodeId(
            episodeId,
            assetFingerprint: currentWeakFingerprint
        ), Self.canUpgradeWeakAssetToCanonicalSHA(
            asset,
            jobSourceFingerprint: canonicalFingerprint,
            currentAudioFingerprint: currentAudioFingerprint
        ) {
            return asset
        }
        let weakFingerprintMatches = try await store.fetchAssetsByEpisodeId(
            episodeId,
            weakFingerprint: currentWeakFingerprint
        )
        for asset in weakFingerprintMatches
            where Self.canUpgradeWeakAssetToCanonicalSHA(
                asset,
                jobSourceFingerprint: canonicalFingerprint,
                currentAudioFingerprint: currentAudioFingerprint
            ) {
            return asset
        }
        return nil
    }

    private static func retiredStaleCanonicalWorkKey(for job: AnalysisJob) -> String {
        "\(job.workKey):staleFingerprint:\(job.jobId)"
    }

    private struct DurationStashKey: Hashable {
        let episodeId: String
        let sourceFingerprint: String
    }

    private static func durationStashKey(episodeId: String, sourceFingerprint: String) -> DurationStashKey {
        DurationStashKey(episodeId: episodeId, sourceFingerprint: sourceFingerprint)
    }

    private static func canUpgradeWeakAssetToCanonicalSHA(
        _ asset: AnalysisAsset,
        jobSourceFingerprint: String,
        currentAudioFingerprint: AudioFingerprint?
    ) -> Bool {
        guard let currentAudioFingerprint,
              currentAudioFingerprint.strong == jobSourceFingerprint else {
            return false
        }
        guard !CrossUserAnalysisShareKey.isCanonicalFullFileSHA(asset.assetFingerprint) else {
            return false
        }
        let currentWeakFingerprint = currentAudioFingerprint.weak
        guard !currentWeakFingerprint.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return false
        }
        return asset.assetFingerprint == currentWeakFingerprint
            || asset.weakFingerprint == currentWeakFingerprint
    }

    private static func shouldStashProbedDurationForDifferentCanonicalSource(
        existing asset: AnalysisAsset,
        sourceFingerprint: String,
        sourceIsCanonicalSHA: Bool,
        currentAudioFingerprint: AudioFingerprint?
    ) -> Bool {
        guard sourceIsCanonicalSHA else { return false }
        if CrossUserAnalysisShareKey.isCanonicalFullFileSHA(asset.assetFingerprint) {
            return asset.assetFingerprint != sourceFingerprint
        }
        return !canUpgradeWeakAssetToCanonicalSHA(
            asset,
            jobSourceFingerprint: sourceFingerprint,
            currentAudioFingerprint: currentAudioFingerprint
        )
    }

    /// playhead-8bp2: did this pass DO the tier's work?
    ///
    /// **What was wrong before.** The tier-advance arm asked
    /// `outcome.cueCoverageSec >= job.desiredCoverageSec`. `cueCoverageSec` is
    /// `finalWindows.filter(isCueWindow).map(\.endTime).max() ?? 0` — the end
    /// time of the LAST ad window above the cue-confidence threshold. It
    /// measures where the ads are, not how much audio the pass read. So the
    /// ladder advanced or died on ad placement: an episode whose ads all sit in
    /// the first two minutes reported "coverage insufficient" for a 300 s tier
    /// it had fully transcribed, while an episode with one late ad advanced for
    /// free. On the 2026-07-30 device pull ALL SEVEN jobs that terminated
    /// `coverageInsufficient:*` had `transcriptCoverageSec >= desiredCoverageSec`
    /// — the work was finished in every single case — and four of them stopped
    /// at a 90 s / 300 s / 420 s depth on episodes of 26 to 102 minutes. The
    /// three that DID pass the old gate were the three whose target had already
    /// been escalated to 91–98% of the episode by playback catch-up, i.e. deep
    /// enough that a last ad fell inside it: the gate was reading ad placement,
    /// not work.
    ///
    /// Such a terminal is not permanent, and calling it permanent misdiagnoses
    /// it: `analysis_jobs.workKey` is UNIQUE and `insertJob` is
    /// `INSERT OR IGNORE`, so re-enqueues are swallowed only until
    /// ``AnalysisJobReconciler``'s 7-day GC deletes the row, after which step 7
    /// mints a fresh job at `defaultT0DepthSeconds` and this same predicate
    /// strands it again. The steady state was an episode restarting at 90
    /// seconds every week, forever.
    ///
    /// **The predicate now.** A tier is satisfied when the pass READ the audio
    /// the tier asked for. `desiredCoverageSec` is the shard filter in
    /// ``AnalysisJobRunner`` (`startTime < desiredCoverageSec`), so the
    /// transcript watermark is the direct record of that depth being read;
    /// coverage lands at the end of the last admitted shard, hence `>=`.
    ///
    /// The old cue condition is KEPT as a sufficient-but-not-necessary arm. It
    /// is not load-bearing any more, but a confident ad window past the target
    /// does imply the detector saw that far, and dropping it would silently
    /// change which jobs advance for reasons unrelated to this fix.
    ///
    /// Feature coverage is deliberately NOT accepted: feature extraction sweeps
    /// independently of transcription, and three superseded rows on the same
    /// device pull carried `featureCoverageSec` at full duration against
    /// `transcriptCoverageSec == 0`. Advancing those would deepen the target of
    /// an episode with no transcript at all.
    ///
    /// **Why the comparison needs slack.** For the DURATION rung the two numbers
    /// come off different clocks. `analysis_assets.episodeDurationSec` is written
    /// on the playback path from the AVURLAsset CONTAINER duration
    /// (``AnalysisCoordinator``), while the watermark is a high-water mark over
    /// DECODED SHARD ends (`TranscriptEngineService.updateCoverage`, raised but
    /// never lowered by the chunk-max reconcile). The two disagree by whatever
    /// the container header and the decoder disagree by, and a shard that failed
    /// mid-pass can leave the watermark at the chunk max instead. An exact
    /// comparison would send the deepest and most expensive rung into the
    /// coverage-insufficient arm to stamp `coverageInsufficient:noProgress` — the
    /// exact forensic signature this bead calls the bug — on an episode that had
    /// just been read end to end. ``tierCoverageSlack(target:)`` is that slack.
    ///
    /// A silent or musical closing shard, by contrast, is NOT a reason for slack:
    /// the watermark advances on shard ends, so audio that was read but produced
    /// no speech still counts. Measured, not assumed — see
    /// `CoverageTierLadderSchedulerTests`, whose fixture goes silent for its last
    /// shard and still lands the watermark on the shard sum.
    static func tierTargetSatisfied(job: AnalysisJob, outcome: AnalysisOutcome) -> Bool {
        let target = job.desiredCoverageSec
        if outcome.cueCoverageSec >= target { return true }
        // A pass that produced NO transcript never satisfies a tier, however
        // small the tier: the slack exists to forgive a shard boundary, not to
        // let an episode with no transcript at all deepen its target.
        guard outcome.transcriptCoverageSec > 0 else { return false }
        return outcome.transcriptCoverageSec + tierCoverageSlack(target: target) >= target
    }

    /// playhead-8bp2: how far short of a tier target the transcript watermark may
    /// land and still count as having read the tier.
    ///
    /// ONE DECODE SHARD, because that is the granularity everything upstream
    /// works at: the runner admits shards by
    /// `allShards.filter { $0.startTime < request.desiredCoverageSec }` and the
    /// watermark advances by shard ends. A rung that lands inside a shard, or a
    /// container duration that overshoots the decoded audio, cannot put the
    /// watermark more than one shard behind. More slack than that would forgive
    /// audio that genuinely was not read. It is the same quantity
    /// ``AnalysisCoverageSummary/adScanDurationToleranceSec(episodeDurationSec:)``
    /// already uses to reconcile these two clocks elsewhere.
    ///
    /// Capped at half the target so a small tier cannot be satisfied by a
    /// token amount of transcript, and floored at ``coverageProgressEpsilon`` so
    /// the exact-hit case still compares true against float error.
    ///
    /// RESIDUAL, stated honestly: if the container duration overshoots the
    /// decoded audio by MORE than one shard — a malformed feed, of which this
    /// project has real examples — the last rung still fails, burns one further
    /// pass, and terminates `coverageInsufficient:noProgress` with the audio
    /// read. This bead does not introduce a second measure to try to tell those
    /// apart.
    static func tierCoverageSlack(target: Double) -> Double {
        max(coverageProgressEpsilon, min(AnalysisAudioService.defaultShardDuration, target / 2))
    }

    static func shouldRetryCoverageInsufficient(job: AnalysisJob, outcome: AnalysisOutcome) -> Bool {
        let epsilon = coverageProgressEpsilon
        let featureAdvanced = outcome.featureCoverageSec > job.featureCoverageSec + epsilon
        let transcriptAdvanced = outcome.transcriptCoverageSec > job.transcriptCoverageSec + epsilon
        let cueAdvanced = outcome.cueCoverageSec > job.cueCoverageSec + epsilon
        let cuesCreated = outcome.newCueCount > 0

        return featureAdvanced || transcriptAdvanced || cueAdvanced || cuesCreated
    }

    // MARK: - Ad-scan re-drive (playhead-onn6)

    /// playhead-onn6: `workKey` discriminator that marks a job as an ad-scan
    /// re-drive, and carries its ordinal.
    ///
    /// **Why the workKey and not a new column or a new jobType.** Once an
    /// episode's tiers finish, `analysis_jobs.state` is `'complete'` and
    /// `workKey` is `TEXT NOT NULL UNIQUE` with `INSERT OR IGNORE` inserts, so a
    /// re-enqueue at the same key is a silent no-op — that collision is exactly
    /// why an under-scanned episode had no re-drive. The tier ladder already
    /// solves the same problem the same way (`…:preAnalysis:600`), and
    /// `replacementWorkKeyForCurrentCanonicalAudio` already preserves any suffix
    /// past the jobType across a fingerprint rebase, so the ordinal survives a
    /// re-download of the same audio. A new `jobType` would need parallel
    /// handling in every dispatch/lane/reconcile path; a new column would need a
    /// migration. This needs neither.
    ///
    /// The ordinal IS the budget ledger: it is read back off the terminating
    /// job's own key, so the chain is `base → …:adScanRedrive:1 →
    /// …:adScanRedrive:2 → stop`, and `INSERT OR IGNORE` makes even a duplicated
    /// mint idempotent. See ``maxAdScanRedrives``.
    static let adScanRedriveWorkKeyMarker = "adScanRedrive"

    /// playhead-onn6: how many ad-scan re-drive passes one episode may ever get,
    /// per `(sourceFingerprint, analysisVersion)`.
    ///
    /// **Bounded is the whole point.** playhead-gqx4 chose to terminate an
    /// under-scanned episode into a degraded state rather than decline to
    /// terminate, precisely because an unbounded retry is a worse bug than the
    /// one it fixed. Two guards hold here and they are independent:
    ///
    ///   1. This absolute cap. The ordinal lives in the UNIQUE `workKey`, so the
    ///      chain cannot exceed it even if every other guard is wrong.
    ///   2. The no-progress guard in ``shouldMintAdScanRedrive`` — a pass is only
    ///      minted when the coverage lane still holds work the runner would
    ///      actually resume. A pass that drains its rows to `complete`, or fails
    ///      them past `AdmissionController.maxRetries`, drives that count to zero
    ///      and the chain stops on its own, usually after one re-drive.
    ///
    /// Two, not more: one pass drains the outstanding rows, and the second exists
    /// for the case the first was deferred wholesale by a thermal/battery
    /// admission gate (which defers every job in the batch — see the H-1
    /// orchestration block in `BackfillJobRunner`). Past that, more passes buy
    /// FM time and no coverage.
    static let maxAdScanRedrives = 2

    /// playhead-onn6: the re-drive ordinal encoded in `workKey`, or `nil` when
    /// this is not a re-drive key.
    ///
    /// Matches on the SUFFIX so it is indifferent to the base key's shape
    /// (`…:preAnalysis`, `…:preAnalysis:600`, or a fingerprint-rebased variant).
    /// A retired tombstone key (`…:adScanRedrive:1:staleFingerprint:<jobId>`,
    /// see ``retiredStaleCanonicalWorkKey(for:)``) deliberately does NOT parse as
    /// a re-drive: that row has been retired and must not extend a chain.
    static func adScanRedriveOrdinal(workKey: String) -> Int? {
        let components = workKey.split(separator: ":", omittingEmptySubsequences: false)
        guard components.count >= 2,
              components[components.count - 2] == Substring(adScanRedriveWorkKeyMarker),
              let ordinal = Int(components[components.count - 1]),
              ordinal > 0 else {
            return nil
        }
        return ordinal
    }

    /// playhead-onn6: the `workKey` for the next re-drive after `job`, or `nil`
    /// when the budget is spent or the lane does not take re-drives.
    ///
    /// Rebuilt from `job.sourceFingerprint` rather than by string-appending to
    /// `job.workKey`, mirroring the tier-advance arm — so a chained re-drive
    /// replaces its predecessor's ordinal instead of nesting markers.
    static func nextAdScanRedriveWorkKey(for job: AnalysisJob) -> String? {
        // Only the pre-analysis lane runs the semantic scan. `playback` jobs are
        // hot-path only and have no coverage lane to re-drive.
        guard job.jobType == "preAnalysis" else { return nil }
        let ordinal = (adScanRedriveOrdinal(workKey: job.workKey) ?? 0) + 1
        guard ordinal <= maxAdScanRedrives else { return nil }
        let base = AnalysisJob.computeWorkKey(
            fingerprint: job.sourceFingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: job.jobType
        )
        return "\(base):\(adScanRedriveWorkKeyMarker):\(ordinal)"
    }

    /// playhead-onn6: should a terminating episode get another ad-scan pass?
    ///
    /// Pure, so the decision matrix is unit-testable without the scheduler graph
    /// (same reason `classifyBackfillTerminal` is static).
    ///
    /// - Parameter adScanFraction: MEASURED semantic ad-scan coverage, from
    ///   ``AnalysisCoverageSummary/adScanFraction`` (playhead-pz32). Never
    ///   recomputed here — the re-drive decision, the runner's skip decision and
    ///   the library ✓ must divide the same numerator by the same denominator.
    ///   `nil` means UNMEASURED, which is not the same as sufficient: erring
    ///   towards running the scan costs one bounded pass, erring the other way is
    ///   the bug (the same call this bead's sibling
    ///   ``AnalysisJobRunner/measuredAdScanFraction(assetId:)`` makes on a store
    ///   failure). It is only ever reached when `resumableCoverageJobCount > 0`,
    ///   so an unmeasurable asset cannot spin: there is provably outstanding work
    ///   and the absolute cap still applies.
    /// - Parameter resumableCoverageJobCount: from
    ///   ``AnalysisStore/countResumableBackfillJobs(assetId:)``. **This is the
    ///   no-progress guard**, and it is structural rather than historical: zero
    ///   resumable rows means a fresh pass would re-derive the same deterministic
    ///   jobIds, find them `complete` (or retry-exhausted), skip every one, and
    ///   produce no coverage at all. Refusing to mint there is what keeps this
    ///   from being "a job that runs and achieves nothing".
    static func shouldMintAdScanRedrive(
        adScanFraction: Double?,
        resumableCoverageJobCount: Int
    ) -> Bool {
        guard resumableCoverageJobCount > 0 else { return false }
        guard let adScanFraction, adScanFraction.isFinite else { return true }
        // The SAME floor the runner uses to decide it may skip the semantic
        // backfill. If this were lower, the scheduler would mint passes the
        // runner then declines to run.
        return adScanFraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
    }

    // MARK: - Cap-out retry (playhead-y8f3)

    /// playhead-y8f3: `workKey` discriminator marking a job as a bounded retry
    /// of an episode whose predecessor exhausted ``maxAttemptCount``.
    ///
    /// **Why the workKey, again.** This is playhead-onn6's mechanism applied to
    /// a second instance of the same defect shape, deliberately rather than
    /// inventing a parallel one. `analysis_jobs.workKey` is `TEXT NOT NULL
    /// UNIQUE` and `insertJob` is `INSERT OR IGNORE` over a key that is stable
    /// across launches, so a re-request for an episode whose row is already on
    /// disk is a silent no-op. An ordinal in the key is simultaneously the way
    /// past the collision AND the budget ledger, needs no migration and no new
    /// column, and cannot be exceeded even if every other guard is wrong.
    ///
    /// Distinct from ``adScanRedriveWorkKeyMarker`` because the budgets are
    /// independent: one buys another semantic ad scan over audio already read,
    /// this one buys another attempt at reading the audio at all. A row can
    /// legitimately need both, and the ordinal parsers key on their own marker
    /// so neither chain launders the other's ledger.
    static let capOutRetryWorkKeyMarker = "capRetry"

    /// playhead-y8f3: how many times one episode's analysis may be re-requested
    /// after exhausting its attempt budget, per `(sourceFingerprint,
    /// analysisVersion)`.
    ///
    /// **The cap is the point.** `maxAttemptCount` exists so a poisoned asset
    /// eventually stops consuming budget; a reset path without a ceiling
    /// recreates exactly the bug the cap was defending against.
    ///
    /// **What is actually bounded is ROWS, not dispatches**, and the distinction
    /// is worth stating because the obvious arithmetic is wrong. This admits at
    /// most `1 + maxCapOutRetries` = 3 `analysis_jobs` rows per
    /// `(sourceFingerprint, analysisVersion)` per 7-day GC window, enforced by
    /// the UNIQUE index on `workKey`. It does NOT bound dispatches at
    /// `5 * 3 = 15`: `fetchNextEligibleJob` has no `attemptCount` predicate, and
    /// `AnalysisStore.requeueOrphanedLease` resets `attemptCount` to 0 whenever
    /// a lease is recovered — which is why two rows on the 2026-07-31 device
    /// pull carry `attemptCount` 8 and 10 against a cap of 5. Rows are the
    /// ledger this bead can hold; dispatch counting was never the cap's job.
    /// Each cycle is additionally paced by the existing exponential backoff
    /// between attempts and by ``capOutRetryCooldownSeconds`` between cycles.
    ///
    /// **Why this reads as a CONSECUTIVE failure count, not a lifetime one**
    /// (the lesson from playhead-bkhc and playhead-8d5r, where a lifetime
    /// counter killed a job that had a few unlucky windows early and then
    /// started converging). The ordinal only ever advances when a cycle
    /// TERMINATES AT THE CAP. A cycle that reaches its tier terminates
    /// `complete`, mints a deeper rung, and leaves the episode with an active
    /// job — which excludes it from `discoverUnEnqueuedDownloads` entirely, so
    /// nothing charges the ledger. Progress does not spend budget; only five
    /// more consecutive failures do.
    ///
    /// Two, matching ``maxAdScanRedrives``: one cycle covers a transient the
    /// first five attempts happened to straddle (a wedged engine, a model that
    /// was unavailable all evening, a decode that failed against a file still
    /// being written), the second covers a second such window. Past that the
    /// cause is structural and more cycles buy dispatches, not coverage.
    static let maxCapOutRetries = 2

    /// playhead-y8f3: how long after an attempt-cap terminal before the episode
    /// may be re-requested.
    ///
    /// One hour, which is ``exponentialBackoffSeconds(attempt:)``'s own ceiling.
    /// The job that just capped out had already earned that gap between its last
    /// two attempts, so re-requesting sooner would pace the retry FASTER than
    /// the attempts inside the cycle it just failed. It also stops a burst of
    /// launches or `BGProcessingTask` wakes from converting the whole budget
    /// into a queue of doomed work ahead of episodes that have never been tried
    /// at all.
    ///
    /// Not load-bearing for termination — the budget ordinal is — so a device
    /// whose clock jumps cannot manufacture extra passes, only earlier ones.
    static let capOutRetryCooldownSeconds: TimeInterval = 3600

    /// playhead-y8f3: is this row an attempt-cap terminal, as opposed to a
    /// genuine supersession? See ``maxAttemptsReachedPrefix``.
    static func isAttemptCapTerminal(_ job: AnalysisJob) -> Bool {
        job.state == "superseded"
            && job.lastErrorCode?.hasPrefix(maxAttemptsReachedPrefix) == true
    }

    /// playhead-y8f3: the `workKey` for cap-out retry `ordinal` off `baseWorkKey`.
    ///
    /// There is deliberately NO matching `capOutRetryOrdinal(workKey:)` parser
    /// to mirror ``adScanRedriveOrdinal(workKey:)``. onn6 needs one because it
    /// reads its budget off the TERMINATING ROW's own key; this ledger is read
    /// off the keys PRESENT IN THE TABLE, by construction and lookup, so a
    /// parser would be production-dead code whose existence implied a guard
    /// nothing performs. What does matter is the converse — that
    /// `adScanRedriveOrdinal` does not match a `capRetry` key, or a cap-out
    /// retry could never walk the tier ladder — and that is pinned by test.
    static func capOutRetryWorkKey(baseWorkKey: String, ordinal: Int) -> String {
        "\(baseWorkKey):\(capOutRetryWorkKeyMarker):\(ordinal)"
    }

    /// playhead-y8f3: why a cap-out retry was NOT minted. Every refusal is
    /// named, because "the retry chain stopped" and "the retry chain stopped for
    /// a reason we can state" are different claims and only the second one is
    /// evidence that the bound holds.
    enum CapOutRetryDeclineReason: String, Sendable, Equatable {
        /// The row that swallowed the re-enqueue is not an attempt-cap terminal
        /// — a live job, a clean `complete`, or a genuine supersession.
        case notACapOutTerminal
        /// Every ordinal up to ``maxCapOutRetries`` is already on disk. This is
        /// the terminating case: it is what stops an asset that never progresses.
        case budgetSpent
        /// The terminal is younger than ``capOutRetryCooldownSeconds``.
        case cooling
        /// The transcript already reaches the top of the coverage ladder AND the
        /// measured ad scan already clears its floor, so a retry would read no
        /// audio it has not already read and screen nothing it has not already
        /// screened. Refusing here is what keeps this from being "a job that
        /// runs and achieves nothing".
        ///
        /// **playhead-9y9e renamed this from `noOutstandingTranscript`, and the
        /// rename is the fix.** The transcript ladder was the ONLY term, which
        /// made a fully transcribed but never-scanned episode the one shape this
        /// rescue systematically refused — precisely the shape that needs it.
        /// Measured on the 2026-08-03 device pull: AD5F3A0A is transcribed to
        /// 4,281 s of 4,281 s and its ad scan covers 20.7 %, and its
        /// `…:adScanRedrive:1` is an attempt-cap terminal
        /// (`maxAttemptsReached:transcription:zeroCoverage`) — i.e. a row this
        /// mechanism was built to rescue, declined on a measure of the one
        /// resource it was not short of.
        case noOutstandingWork
    }

    /// playhead-y8f3: mint a bounded retry, or decline with a named reason.
    ///
    /// Pure, so the whole decision matrix — including the termination bound — is
    /// unit-testable without a store or a scheduler, the same reason
    /// ``shouldMintAdScanRedrive(adScanFraction:resumableCoverageJobCount:)``
    /// and `classifyBackfillTerminal` are static.
    ///
    /// - Parameter chainTail: the newest row already on disk in this episode's
    ///   `base → capRetry:1 → capRetry:2` chain. The caller walks the chain
    ///   because only the caller can read the store; this function owns the
    ///   policy.
    /// - Parameter nextOrdinal: the lowest unused ordinal, or `nil` when the
    ///   budget is spent. Deriving it from the KEYS ON DISK rather than from a
    ///   counter is what makes a duplicated or racing sweep idempotent — a
    ///   second mint at the same ordinal collides on the UNIQUE index and does
    ///   nothing.
    /// - Parameter transcriptCoverageSec: the ASSET's transcript watermark
    ///   (`analysis_assets.fastTranscriptCoverageEndTime`), never the job row's
    ///   `transcriptCoverageSec`. `updateJobProgress` overwrites the job column
    ///   with the LAST RUN's output rather than a high-water mark, so on all
    ///   eight superseded rows of the 2026-07-31 device pull it reads 0.0 while
    ///   the assets behind them are 69–100% transcribed. Reading the job column
    ///   here would conclude "no progress ever" for an episode that is nearly
    ///   done.
    /// - Parameter adScanFraction: MEASURED semantic ad-scan coverage
    ///   (``AnalysisCoverageSummary/adScanFraction``), or `nil` when
    ///   unmeasurable. playhead-9y9e: the SECOND kind of outstanding work, and
    ///   the reason this rescue reached none of the episodes it exists for. A
    ///   cap-out terminal on a fully transcribed episode had no outstanding
    ///   TRANSCRIPT by construction, so the ladder term declined every one of
    ///   them — while the thing the retry actually buys is a pass that reaches
    ///   the ad-detection stage. `nil` reads as OWED, the same direction
    ///   ``shouldMintAdScanRedrive(adScanFraction:resumableCoverageJobCount:)``
    ///   and ``SemanticScanClaim/isOwed(adScanFraction:)`` take, and for the same
    ///   reason: a never-scanned asset has no `semantic_scan_results` rows and so
    ///   measures `nil` rather than a synthetic 0.
    static func capOutRetryDecision(
        baseWorkKey: String,
        chainTail: AnalysisJob,
        nextOrdinal: Int?,
        transcriptCoverageSec: Double,
        episodeDurationSec: Double?,
        adScanFraction: Double?,
        tiers: [Double],
        now: Double
    ) -> CapOutRetryDecision {
        guard isAttemptCapTerminal(chainTail) else { return .declined(.notACapOutTerminal) }
        guard let nextOrdinal else { return .declined(.budgetSpent) }
        guard capOutRetryCooldownElapsed(chainTail: chainTail, now: now) else {
            return .declined(.cooling)
        }
        let ladder = coverageTierLadder(tiers: tiers, episodeDurationSec: episodeDurationSec)
        let target: Double
        if let outstanding = outstandingTranscriptTarget(
            transcriptCoverageSec: transcriptCoverageSec,
            tiers: tiers,
            episodeDurationSec: episodeDurationSec
        ) {
            target = outstanding
        } else if SemanticScanClaim.isOwed(adScanFraction: adScanFraction), let deepest = ladder.last {
            // Nothing left to transcribe, but the audio has not been read for
            // ads. Ask for the DEEPEST rung: the pass must reach the
            // ad-detection stage over the whole episode, and any shallower
            // target would bound the shard set it decodes. The transcription
            // stage itself is a no-op on this asset — playhead-9y9e's
            // short-circuit in `AnalysisJobRunner` is what makes that true
            // rather than a 300 s timeout — so the cost is the scan, which is
            // the point.
            target = deepest
        } else {
            return .declined(.noOutstandingWork)
        }
        return .mint(CapOutRetryPlan(
            workKey: capOutRetryWorkKey(baseWorkKey: baseWorkKey, ordinal: nextOrdinal),
            ordinal: nextOrdinal,
            desiredCoverageSec: target
        ))
    }

    /// playhead-9y9e (R1 review): has `chainTail`'s terminal cooled enough for a
    /// cap-out retry?
    ///
    /// Extracted so the CALLER can ask the question before paying for the inputs
    /// ``capOutRetryDecision(baseWorkKey:chainTail:nextOrdinal:transcriptCoverageSec:episodeDurationSec:adScanFraction:tiers:now:)``
    /// needs. `adScanFraction` costs a full coverage-summary read — four
    /// prepared statements and every transcript chunk of BOTH passes for the
    /// asset — and it is discarded on the `budgetSpent` and `cooling` arms,
    /// which is where a spent or freshly-terminated episode lands on EVERY
    /// sweep, forever. One expression, used by the caller's pre-check and by the
    /// decision itself, so the two cannot drift into disagreeing about when the
    /// read is safe to skip.
    static func capOutRetryCooldownElapsed(chainTail: AnalysisJob, now: Double) -> Bool {
        now - chainTail.updatedAt >= capOutRetryCooldownSeconds
    }

    /// playhead-y8f3: the outcome of ``capOutRetryDecision(baseWorkKey:chainTail:nextOrdinal:transcriptCoverageSec:episodeDurationSec:adScanFraction:tiers:now:)``.
    enum CapOutRetryDecision: Sendable, Equatable {
        case mint(CapOutRetryPlan)
        case declined(CapOutRetryDeclineReason)
    }

    /// playhead-y8f3: what a minted cap-out retry should ask for.
    struct CapOutRetryPlan: Sendable, Equatable {
        let workKey: String
        let ordinal: Int
        let desiredCoverageSec: Double
    }

    /// playhead-y8f3: the coverage target a retry should carry — the next rung
    /// of the EXISTING ladder above what the transcript already covers — or
    /// `nil` when the transcript has already reached the top and there is
    /// nothing outstanding.
    ///
    /// This is the "still has work outstanding" predicate, and it is structural
    /// rather than historical for the same reason onn6's `resumableCoverageJobCount`
    /// is: it asks whether a fresh pass WOULD read audio, not whether past
    /// passes did.
    ///
    /// **Why the ladder and not the terminated job's own target.** Two of the
    /// six re-requestable rows on the 2026-07-31 device pull had already been
    /// transcribed PAST the target their job carried (`D2B8579A`: 2,670 s
    /// covered against a 2,649 s target; `1B0C0D33`: 3,840 against 3,832), so
    /// re-minting at the terminated job's own target would ask for audio the
    /// asset already holds. The ladder rung is by construction strictly deeper
    /// than the watermark, so the retry always has something left to read.
    ///
    /// A CLAIM MADE HERE THAT WAS HALF WRONG, corrected by playhead-9y9e rather
    /// than deleted, because the correction is the whole of that bead. It read:
    /// re-using the old target would NOT fail via `transcription:zeroCoverage`,
    /// because the runner's coverage comes from `persistedCoverage()` — the
    /// ASSET watermark — so a pass over already-covered audio reports the full
    /// watermark and terminates `complete` through `tierAdvance`.
    ///
    /// That is true of the `.completed` arm of `observeTranscriptEvents` and
    /// only that arm. The SIBLING timeout arm — named in the same sentence as a
    /// source of zero coverage — returns a hardcoded `(0, nil, false)` and never
    /// consults `persistedCoverage()` at all, and on an already-covered asset
    /// the timeout is the arm that wins: the 300 s stage cap is flat while the
    /// ASR re-run under it scales with the episode. On the 2026-08-03 device
    /// pull that is asset AD5F3A0A, 4,281 s and fully transcribed, whose re-drive
    /// carries `maxAttemptsReached:transcription:zeroCoverage` after 5 attempts.
    /// `AnalysisJobRunner` now carries the persisted coverage forward on that
    /// path (see its `transcriptCoverageOfCompletedTranscript`), which makes the
    /// original claim true of both arms.
    ///
    /// The CONCLUSION is unchanged and still rests on its first reason alone:
    /// asking for a target the asset already covers wastes a pass, so the ladder
    /// rung — strictly deeper than the watermark by construction — is what the
    /// retry must ask for.
    ///
    /// **The slack, and why the comparison needs it.** The last rung is the
    /// asset's `episodeDurationSec`, which comes off the AVURLAsset CONTAINER
    /// while the watermark advances on DECODED SHARD ends; the two disagree by
    /// whatever the header and the decoder disagree by. Without slack, an
    /// episode read end to end (`7A481794`: 3,210 s of 3,213 s) would look like
    /// 3 s of outstanding work forever, and would burn its whole retry budget
    /// re-reading a finished episode. ``tierCoverageSlack(target:)`` is the same
    /// quantity ``tierTargetSatisfied(job:outcome:)`` already uses to reconcile
    /// those two clocks.
    static func outstandingTranscriptTarget(
        transcriptCoverageSec: Double,
        tiers: [Double],
        episodeDurationSec: Double?
    ) -> Double? {
        let covered = transcriptCoverageSec.isFinite ? max(0, transcriptCoverageSec) : 0
        return coverageTierLadder(tiers: tiers, episodeDurationSec: episodeDurationSec)
            .first { $0 > covered + tierCoverageSlack(target: $0) }
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
        if priority >= AnalysisWorkScheduler.nowLanePriorityFloor { return .now }
        if priority >= AnalysisWorkScheduler.soonLanePriorityFloor { return .soon }
        return .background
    }
}
