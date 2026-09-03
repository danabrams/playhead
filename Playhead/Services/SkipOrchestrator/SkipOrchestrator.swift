// SkipOrchestrator.swift
// Decision layer between ad detection and playback transport.
//
// Consumes AdWindows from AdDetectionService, applies skip policy
// (hysteresis, merging, suppression after seek),
// and pushes skip cues to PlaybackService as CMTimeRanges.
//
// Every skip decision is idempotent, keyed by
//   analysisAssetId + adWindowId + policyVersion.
//
// NEVER queries SQLite synchronously from the playback callback path.
// All state is maintained in-memory; SQLite writes are fire-and-forget
// for the decision log.

import CoreMedia
import Foundation
import OSLog

private enum SkipOrchestratorFeedbackError: Error {
    case invalidCorrectionRange
    case staleDurableMaterial
}

// MARK: - Runtime Decision Contract

/// Runtime decision material emitted by fusion and consumed by the
/// orchestrator after the final persisted producer row has been reloaded.
///
/// Naming note: `AdDecisionResult` (this type) is the **runtime per-window decision**
/// that `SkipOrchestrator` consumes during active playback. It is distinct from
/// `DecisionResultArtifact` (in AdDecisionResult.swift), which is the SQLite persistence
/// container that stores an array of these decisions as JSON. The separation is intentional:
/// one type is optimized for live evaluation, the other for durable storage.
enum AdDecisionEligibilityGate: String, Sendable {
    case eligible
    case blocked
}

struct AdDecisionResult: Sendable {
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let skipConfidence: Double
    let eligibilityGate: AdDecisionEligibilityGate
    let recomputationRevision: Int
    /// Exact persisted producer material for live orchestration. Carrying the
    /// row avoids reconstructing a lossy synthetic revision that drops catalog
    /// provenance (or any future producer field) before the durable apply
    /// fence. Eligible decisions without an exact matching revision fail
    /// closed.
    let producerRevision: AdWindow?

    init(
        id: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        skipConfidence: Double,
        eligibilityGate: AdDecisionEligibilityGate,
        recomputationRevision: Int,
        producerRevision: AdWindow? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.startTime = startTime
        self.endTime = endTime
        self.skipConfidence = skipConfidence
        self.eligibilityGate = eligibilityGate
        self.recomputationRevision = recomputationRevision
        self.producerRevision = producerRevision
    }

    func withProducerRevision(_ revision: AdWindow?) -> AdDecisionResult {
        guard let revision else {
            return AdDecisionResult(
                id: id,
                analysisAssetId: analysisAssetId,
                startTime: startTime,
                endTime: endTime,
                skipConfidence: skipConfidence,
                eligibilityGate: eligibilityGate,
                recomputationRevision: recomputationRevision,
                producerRevision: nil
            )
        }
        let persistedGate: AdDecisionEligibilityGate =
            revision.eligibilityGate
                == SkipEligibilityGate.eligible.rawValue
            ? .eligible
            : .blocked
        let persistedState = SkipDecisionState(
            rawValue: revision.decisionState
        )
        guard revision.id == id,
              revision.analysisAssetId == analysisAssetId,
              let persistedState else {
            return AdDecisionResult(
                id: id,
                analysisAssetId: analysisAssetId,
                startTime: startTime,
                endTime: endTime,
                skipConfidence: skipConfidence,
                eligibilityGate: eligibilityGate,
                recomputationRevision: recomputationRevision,
                producerRevision: nil
            )
        }
        let handedOffGate: AdDecisionEligibilityGate
        switch persistedState {
        case .candidate, .confirmed, .applied:
            guard persistedGate == eligibilityGate else {
                return AdDecisionResult(
                    id: id,
                    analysisAssetId: analysisAssetId,
                    startTime: startTime,
                    endTime: endTime,
                    skipConfidence: skipConfidence,
                    eligibilityGate: eligibilityGate,
                    recomputationRevision: recomputationRevision,
                    producerRevision: nil
                )
            }
            handedOffGate = eligibilityGate
        case .suppressed, .reverted:
            // Preserve the exact terminal producer revision so the
            // orchestrator can retire and tombstone that material. The
            // redundant fusion envelope must never keep an eligible grant
            // after the durable producer has become terminal.
            handedOffGate = .blocked
        }
        return AdDecisionResult(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: revision.startTime,
            endTime: revision.endTime,
            // playhead-ar60: an ACTUATION reader. `AdDecisionResult
            // .skipConfidence` is the number the skip policy gates on, so it
            // must come from the row's actuation column, not from
            // `confidence` (which is the DETECTION number since V47).
            // `actuationConfidence` falls back to `confidence` for producers
            // with a single number, so every pre-V47 row and every non-fusion
            // row projects exactly the value it did before.
            skipConfidence: revision.actuationConfidence,
            eligibilityGate: handedOffGate,
            recomputationRevision: recomputationRevision,
            producerRevision: revision
        )
    }
}

/// Ordered UI events emitted by the skip orchestrator. Presentations and
/// invalidations share one stream so a gate flip retires a stale suggestion
/// before any replacement auto-skip presentation is delivered.
enum AdBannerStreamEvent: Sendable {
    case present(AdSkipBannerItem)
    case retireWindow(AdBannerRetirement)
}

/// Identity of a banner presentation that is no longer actionable.
struct AdBannerRetirement: Sendable {
    let windowId: String
    let episodeId: String?
    let playbackLifecycleGeneration: UInt64?
}

// MARK: - Skip Decision State

/// Lifecycle of an AdWindow through the skip orchestrator.
/// Extends the detection-side states (candidate, confirmed, suppressed)
/// with skip-execution states.
enum SkipDecisionState: String, Sendable, CaseIterable {
    /// Detection produced a candidate -- not yet actionable.
    case candidate
    /// Detection confirmed the window -- eligible for skip policy.
    case confirmed
    /// Skip policy accepted and skip cue was fired.
    case applied
    /// Skip was suppressed by policy (too short, ambiguous, etc.).
    case suppressed
    /// User tapped "Listen" -- revert the skip.
    case reverted
}

// MARK: - Skip Policy Configuration

struct SkipPolicyConfig: Sendable {
    /// Hysteresis: probability threshold to enter ad state.
    let enterThreshold: Double
    /// Hysteresis: probability threshold to stay in ad state (lower).
    let stayThreshold: Double
    /// Merge adjacent ad windows with gaps smaller than this (seconds).
    let mergeGapSeconds: TimeInterval
    /// Ignore ad windows shorter than this unless sponsor evidence is strong.
    let minimumSpanSeconds: TimeInterval
    /// Confidence threshold for short-span override (strong sponsor evidence).
    let shortSpanOverrideConfidence: Double
    /// Seconds after a user seek during which auto-skip is suppressed.
    let seekSuppressionSeconds: TimeInterval
    /// Seconds of stability required after seek before re-enabling skip.
    let seekStabilitySeconds: TimeInterval
    /// Policy version tag for idempotency keys.
    let policyVersion: String
    /// Cushion (seconds) subtracted from the trailing edge of an ad pod when
    /// the next thing is program audio (or end-of-episode). Trades a small
    /// sliver of ad-tail for protection against program-start clipping.
    /// Applied per merged pod, not per individual ad — internal seams between
    /// ads in the same pod do not receive a cushion. Clamped at the pod start
    /// so the skip end can never precede the skip start.
    let adTrailingCushionSeconds: TimeInterval

    static let `default` = SkipPolicyConfig(
        enterThreshold: 0.65,
        stayThreshold: 0.45,
        mergeGapSeconds: 4.0,
        minimumSpanSeconds: 15.0,
        shortSpanOverrideConfidence: 0.85,
        seekSuppressionSeconds: 3.0,
        seekStabilitySeconds: 2.0,
        policyVersion: "skip-policy-v1",
        adTrailingCushionSeconds: 1.0
    )
}

// MARK: - Skip Decision Record

/// Immutable record of a skip decision for the evaluation harness.
struct SkipDecisionRecord: Sendable {
    let idempotencyKey: String
    let adWindowId: String
    let analysisAssetId: String
    let policyVersion: String
    let decision: SkipDecisionState
    let reason: String
    let originalStart: Double
    let originalEnd: Double
    let snappedStart: Double
    let snappedEnd: Double
    let confidence: Double
    let timestamp: Double
}

// MARK: - Managed Ad Window

/// In-memory representation of an AdWindow with skip orchestrator state.
private struct ManagedWindow: Sendable {
    let adWindow: AdWindow
    var decisionState: SkipDecisionState
    var snappedStart: Double
    var snappedEnd: Double
    var idempotencyKey: String
    /// Whether the skip cue has been pushed to PlaybackService.
    var cueActive: Bool
}

// MARK: - SkipOrchestrator

/// Consumes ad detection events and produces skip cues for PlaybackService.
/// Maintains hysteresis state, merges short gaps, and suppresses skips
/// after user seeks.
///
/// All decisions are logged for the evaluation harness.
actor SkipOrchestrator {

    private let logger = Logger(subsystem: "com.playhead", category: "SkipOrchestrator")

    /// Minimum ACTUATION confidence required for an `AdWindow` to be eligible
    /// for the cross-launch preload — i.e. `AdWindow.actuationConfidence`, the
    /// post-calibration, post-user-correction number, NOT raw `confidence`.
    /// See `preloadAdmissibleWindows` for which quantity this is compared
    /// against and why (playhead-atr3). Kept private to this actor — the only
    /// consumers are `beginEpisode`'s preload and the playhead-96ot mid-session
    /// ingest, both through `preloadAdmissibleWindows`.
    ///
    /// Bug 5 (skip-cues-deletion): the VALUE 0.7 is inherited from the
    /// (now-deleted) `SkipCueMaterializer`, which is where this preload's rows
    /// used to come from. Only the number is inherited — the materializer had
    /// one confidence column to read, so it cannot be authority for which of
    /// V47's two quantities this floor applies to.
    private static let preloadConfidenceThreshold: Double = 0.7

    /// Cycle-21 H-1: returns whether a decision state is allowed to
    /// flow through the `beginEpisode` preload into `receiveAdWindows`.
    /// `.candidate`, `.confirmed`, `.applied` are eligible; `.suppressed`
    /// (terminal "no-skip") and `.reverted` (user chose "Listen") are
    /// not. `.applied` is eligible so a previously-skipped ad pushes
    /// its cue on the next app launch (cross-launch auto-skip
    /// continuity); banner re-emission for those rows is suppressed in
    /// `beginEpisode` by pre-populating `banneredWindowIds`.
    ///
    /// Cycle-22 M-1: implemented as an exhaustive `switch` over
    /// `SkipDecisionState` (rather than an array of three cases) so
    /// the compiler forces a deliberate decision when a new case is
    /// added — the new case won't silently default to ineligible
    /// without an author choice.
    private static func isPreloadEligible(_ state: SkipDecisionState) -> Bool {
        switch state {
        case .candidate, .confirmed, .applied:
            return true
        case .suppressed, .reverted:
            return false
        }
    }

    /// Cycle-21 L-1: derived from `SkipDecisionState.allCases` (cycle-22
    /// M-1 made the enum `CaseIterable`) so the on-disk filter cannot
    /// drift from the in-actor enum across renames, rawValue changes,
    /// or new cases. The exhaustive partition lives in
    /// `isPreloadEligible(_:)`.
    private static let preloadEligibleDecisionStates: Set<String> = Set(
        SkipDecisionState.allCases
            .filter(SkipOrchestrator.isPreloadEligible)
            .map(\.rawValue)
    )

    /// playhead-u45d: the persisted decision states
    /// `AnalysisStore.persistRevertedAdWindowsIfCurrent` will accept as veto
    /// targets. Deliberately a copy of the store's own literal rather than a
    /// reference to `preloadEligibleDecisionStates`, which happens to hold the
    /// same three values for an unrelated reason (cross-launch cue
    /// continuity): the two are free to diverge, and a shared constant would
    /// couple them by accident. A row outside this set would make the whole
    /// transaction return nil, so admitting one here would convert a working
    /// veto into a refused one.
    private static let userVetoRevertibleDecisionStates: Set<String> = [
        AdDecisionState.candidate.rawValue,
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue,
    ]

    /// playhead-95cf: whether a persisted row may be SWEPT UP by a range veto.
    ///
    /// THE DEFECT. `revertByTimeRange` folded in every persisted window
    /// overlapping the range, filtered by `decisionState` and nothing else. A
    /// row whose `boundaryState` is `userMarked` is the LISTENER'S OWN MARK —
    /// the top of the fidelity ladder, above banner responses and above every
    /// inferred signal — and one "This isn't an ad" tap silently retracted it
    /// along with the app's detections. Measured on the 2026-09-02 device pull:
    /// **25 `userMarked` rows exposed.**
    ///
    /// It is worse than a lost row. `AnalysisStore.userVetoedTimeRanges` then
    /// suppresses every decoded span merely OVERLAPPING the reverted range, so
    /// retracting one hand-mark darkens material around it too.
    ///
    /// THE RULE IS NOT "never revert a user mark", which would trap a listener
    /// who mis-marked something with no way to undo it. It is: **a hand-mark is
    /// never collateral.** It can only be reverted when the gesture NAMES it —
    /// when the requested range is exactly that row's range, which is what the
    /// transcript popover produces when the listener taps the mark itself.
    /// Anything wider is a sweep, and a sweep may not take it.
    ///
    /// Compared on the canonical bit pattern, the same equality
    /// `revertByTimeRange` already uses to validate `correctionSpan`, so a
    /// float that prints the same cannot be mistaken for a different range.
    private static func rangeVetoMaySweepUp(
        _ window: AdWindow,
        requestedStart: Double,
        requestedEnd: Double
    ) -> Bool {
        guard window.boundaryState == UserSpanAssertion.userMarked.rawValue else {
            return true
        }
        let canonical = RecurrenceMaterialIdentity.canonicalTimeBitPattern
        return canonical(window.startTime) == canonical(requestedStart)
            && canonical(window.endTime) == canonical(requestedEnd)
    }

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let adCatalogStore: AdCatalogStore?
    private let repeatedAdCache: RepeatedAdCacheService?
    private let config: SkipPolicyConfig
    private let trustService: TrustScoringService?
    /// Test override for holding the secondary false-skip calibration write.
    /// Production leaves this nil and uses `trustService`.
    private var falseSkipSignalHandlerForTesting:
        (@Sendable (String) async -> Void)?
    /// Test override for holding secondary false-negative calibration.
    /// Production leaves this nil and uses `trustService`.
    private var falseNegativeSignalHandlerForTesting:
        (@Sendable (String) async -> Void)?
    /// playhead-gard: test override for the per-detector CORRECT-observation
    /// write — the escape from `manual`. Production leaves this nil and uses
    /// `trustService`.
    /// R3: carries `analysisAssetId` as its second argument. Without it the
    /// seam could not observe the id production passes, and that argument had
    /// no coverage at its only call site.
    private var correctObservationHandlerForTesting:
        (@Sendable (String, String, SkipDetectorClass) async -> Void)?
    private var feedbackPersistenceBarrierForTesting:
        (@Sendable () async -> Void)?
    /// Test-only suspension point for the fire-and-forget durable applied
    /// transition. Production leaves this nil.
    private var appliedPersistenceBarrierForTesting:
        (@Sendable () async -> Void)?
    /// Test-only suspension after an exact catalog row has been validated but
    /// before the active episode generation is rechecked.
    private var catalogAdmissionValidationBarrierForTesting:
        (@Sendable () async -> Void)?
    /// Test-only suspension after a replacement episode has cleared its
    /// synchronous state but before trust/profile hydration starts.
    private var beginEpisodeHydrationBarrierForTesting:
        (@Sendable () async -> Void)?
    /// playhead-i08e: test-only suspension point taken by the REVERT seams
    /// (`recordListenRevert` and both of `revertByTimeRange`'s loops)
    /// immediately after each durable decision-state write and before the
    /// live-lifecycle guard that follows it. Production leaves this nil.
    ///
    /// It exists so `SkipOrchestratorRevertLifecycleRaceTests` can interleave
    /// an episode switch with the exact suspension those guards were written
    /// for, and therefore assert the invariant BEHAVIOURALLY: a gesture whose
    /// lifecycle is replaced mid-flight still delivers its durable receipt and
    /// its calibration samples to the CAPTURED show, still stops mutating live
    /// state, and no longer republishes cues. Without it the invariant is only
    /// reachable by scanning this file's source text, which is what five
    /// review rounds of playhead-i08e spent themselves on.
    private var revertPersistenceBarrierForTesting:
        (@Sendable () async -> Void)?

    // MARK: - Phase 7.2: User Correction Store

    /// Injected by PlayheadRuntime after init. Primary feedback is committed
    /// through AnalysisStore transactions; this dependency receives
    /// post-commit learning notifications.
    private(set) var correctionStore: (any UserCorrectionStore)?

    // MARK: - playhead-xsdz.9: Hard-negative fingerprint bank

    /// Optional HARD-NEGATIVE fingerprint bank. When wired, a user "Listen"
    /// revert / "not an ad" veto of an auto-skipped (or markOnly) window is the
    /// confirmed-FP WRITE TRIGGER: the reverted window's `evidenceText` is
    /// ingested as a hard-negative via `recordConfirmedFalsePositive`. A bank is
    /// wired ONLY when the `AdDetectionConfig.crossEpisodeMemoryEnabled` feature
    /// flag is on (PlayheadRuntime constructs and injects it behind that flag),
    /// so the WHOLE feature — construction, migration, this write trigger, and
    /// the suppression read — rides the one off-by-default flag. `nil` (the
    /// default for all existing call sites AND the flag-OFF production default)
    /// ⇒ no negative-bank writes, byte-identical to pre-xsdz.9.
    ///
    /// MEMORY-POLLUTION GUARD: only reversions (confirmed FPs) write here. The
    /// auto-skip-eligible / catalog-ingress path NEVER writes to this bank.
    private(set) var negativeFingerprintBank: NegativeFingerprintBank?

    /// Install (or replace) the hard-negative bank post-init. Mirrors the
    /// runtime's post-init wiring of other optional dependencies.
    func setNegativeFingerprintBank(_ bank: NegativeFingerprintBank?) {
        self.negativeFingerprintBank = bank
    }

    // MARK: - playhead-xsdz.11: Per-show auto-skip threshold controller

    /// Optional per-show auto-skip threshold controller store. This is the
    /// WRITE path for the PI controller: a user "Listen" revert / "not an ad"
    /// veto of an auto-skipped (or markOnly) window is a FALSE-POSITIVE signal
    /// that RAISES the show's threshold (be more conservative). A store is wired
    /// ONLY when the `AdDetectionConfig.perShowThresholdControlEnabled` feature
    /// flag is on (PlayheadRuntime constructs and injects it behind that flag),
    /// so the WHOLE feature — construction, migration, this write trigger, and
    /// the offset read at the detection gate — rides the one off-by-default
    /// flag. `nil` (the default for all existing call sites AND the flag-OFF
    /// production default) ⇒ no controller writes, byte-identical to pre-xsdz.11.
    ///
    /// Miss-side note: the symmetric MISS signal (the user scrubbed through
    /// undetected ad content → LOWER the threshold) has no clean, distinct
    /// gesture at this orchestration layer today — the only "missed ad" signal
    /// is the explicit false-negative correction routed through the
    /// `UserCorrectionStore`, not a scrub-through. `recordThresholdControlMiss`
    /// is the defined miss-side API; it is wired at the false-negative seam
    /// where `podcastId` is available. The FP side (below) is fully wired.
    private(set) var perShowThresholdControllerStore: PerShowThresholdControllerStore?

    /// Install (or replace) the per-show threshold controller store post-init.
    /// Mirrors `setNegativeFingerprintBank`.
    func setPerShowThresholdControllerStore(_ store: PerShowThresholdControllerStore?) {
        self.perShowThresholdControllerStore = store
    }

    // MARK: - playhead-98co: asymmetric auto-skip edge padding

    /// Feature flag for the derived edge-padding policy (default OFF —
    /// `AutoSkipEdgePadding.isEnabledByDefault`; auto-skip itself is held
    /// behind Gate 2). OFF ⇒ byte-identical orchestrator behavior: skip
    /// cues use the snapped span bounds exactly as before.
    private(set) var edgePaddingEnabled: Bool = AutoSkipEdgePadding.isEnabledByDefault

    /// Flip the edge-padding policy and re-evaluate pending windows so a
    /// mid-episode change takes effect on the next cue push.
    func setEdgePaddingEnabled(_ enabled: Bool) {
        edgePaddingEnabled = enabled
        evaluateAndPush()
    }

    /// Per-window edge-anchor provenance, keyed by adWindowId. This is the
    /// stamping seam for the Gate-2 provenance bead (rediff byte-exact /
    /// stinger-snap traces are not yet persisted on AdWindow rows).
    /// Absent entry ⇒ both edges `.unanchored` — the conservative default
    /// under which flag-ON auto-skips nothing (derivation doc §8.6).
    private var edgeAnchorsByWindowId: [String: (start: AutoSkipEdgeAnchor, end: AutoSkipEdgeAnchor)] = [:]

    /// Record the edge-anchor provenance for a window and re-evaluate.
    ///
    /// ORDERING CAVEAT for the Gate-2 stamping bead: stamp anchors BEFORE
    /// a window can promote to `.applied`. Stamping after promotion cannot
    /// retract the promotion — `evaluateAndPush`'s terminal-state branch
    /// keeps the window `.applied` (its `auto_skip_fired` event and
    /// `wasSkipped` persistence already happened); a downgrade-to-
    /// `.unanchored` re-stamp only drops the CUE (via `paddedCueSpan`
    /// returning nil in step 2), leaving decision state and audit trail
    /// asserting a skip that will no longer fire.
    func setEdgeAnchors(
        start: AutoSkipEdgeAnchor,
        end: AutoSkipEdgeAnchor,
        forWindowId id: String
    ) {
        edgeAnchorsByWindowId[id] = (start: start, end: end)
        evaluateAndPush()
    }

    /// Windows whose skip was explicitly user-initiated (manual "Skip Ad"
    /// tap). User-initiated skips are exempt from edge padding: the user
    /// chose the span deliberately. User-MARKED windows are exempted via
    /// their `boundaryState` stamp in `isUserInitiatedSkip(_:)`; this set
    /// covers the manual-tap path whose window is an ordinary detection row.
    ///
    /// playhead-ynmk: ACCEPTED SUGGESTIONS are no longer in either category.
    /// A banner Yes asserts presence over a span the detector drew, so its
    /// extent is governed by `AutoSkipEdgePadding`. Two independent guards
    /// keep them out: `applyManualSkip` does not insert here for explicit
    /// banner feedback, and `isUserInitiatedSkip` consults `UserSpanAssertion`
    /// BEFORE this set. Each alone is sufficient — the mutation battery shows
    /// either can be reverted with the suite still green — but reverting BOTH
    /// re-exempts a confirmation and is caught.
    ///
    /// SCOPE CAVEAT: this set is in-memory and per-session — a manual
    /// skip's exemption does NOT survive relaunch. A previously
    /// manually-skipped detection row preloads as `.applied` with its
    /// original (non-user) `boundaryState`, so under flag-ON with no
    /// anchors stamped its cue is suppressed like any pipeline span
    /// (conservative direction: plays ad, never clips content). Persisting
    /// a user-initiated marker on the AdWindow row is a schema decision
    /// for the Gate-2 provenance bead, not this one.
    private var userInitiatedSkipWindowIds: Set<String> = []

    // MARK: - State

    /// All managed windows for the current episode, keyed by adWindowId.
    private var windows: [String: ManagedWindow] = [:]
    /// Auto-skip windows whose live cue has been retired for a banner Listen
    /// action but whose durable rewind receipt has not yet committed. Retaining
    /// the ID makes a persistence failure retryable without re-arming the cue.
    private var pendingListenRewindWindowIds: Set<String> = []

    /// Current analysis asset ID.
    private var activeAssetId: String?

    /// Current episode ID (canonical episode key). Used for the
    /// `episode_id_hash` stamped on `auto_skip_fired` events so the hash
    /// byte-matches the one `EpisodeSurfaceStatusObserver` stamps on
    /// `ready_entered`. Windows/decisions remain keyed by `activeAssetId`.
    private var activeEpisodeId: String?

    /// Whether we are currently "in ad state" (hysteresis tracking).
    private var inAdState: Bool = false

    /// Timestamp of the most recent user-initiated seek.
    private var lastSeekTime: Date?

    /// Whether skip is currently suppressed due to recent seek.
    private var skipSuppressedAfterSeek: Bool = false

    /// Latest known playhead position.
    private var currentPlayheadTime: TimeInterval = 0
    /// Auto skips become positive catalog evidence only after the durable
    /// applied transition and an observed playhead position beyond the span.
    private struct PendingCatalogLearning {
        let window: AdWindow
        let showId: String
        let source: CatalogLearningSource
        let lifecycle: CatalogLearningLifecycle
        let eligiblePlayheadTime: TimeInterval
        let learningGeneration: UInt64
    }
    /// Exact durable source identity for recurrence learning and revocation.
    /// Producer IDs alone are not globally unique and may be reused after an
    /// episode replacement while an old actor-reentrant task is suspended.
    private struct RecurrenceSourceKey: Hashable {
        let analysisAssetId: String
        let windowId: String
        let startTimeBits: UInt64
        let endTimeBits: UInt64

        init(_ window: AdWindow) {
            analysisAssetId = window.analysisAssetId
            windowId = window.id
            startTimeBits =
                RecurrenceMaterialIdentity.canonicalTimeBitPattern(
                    window.startTime
                )
            endTimeBits =
                RecurrenceMaterialIdentity.canonicalTimeBitPattern(
                    window.endTime
                )
        }
    }
    private var pendingCatalogLearning:
        [RecurrenceSourceKey: PendingCatalogLearning] = [:]
    private var catalogLearningGeneration: UInt64 = 0
    private var recurrenceBackgroundWorkCount: Int = 0
    /// Corrections fence actor-reentrant learning tasks that already passed
    /// their generation check but have not persisted yet.
    private var revokedLearningSources: Set<RecurrenceSourceKey> = []
    private static let catalogConsumptionDelaySeconds: TimeInterval = 1
    /// Runtime operation token admitted by the newest same-lifecycle seek.
    /// This is separate from episode lifecycle because overlapping seeks may
    /// target the same episode and transport item.
    private var latestUserSeekOperationGeneration: UInt64 = 0
    #if DEBUG
    private var userSeekEffectHookForTesting:
        (@Sendable () async -> Void)?
    private var catalogLearningPersistenceBarrierForTesting:
        (@Sendable () async -> Void)?
    private var recurrenceBackgroundWorkWaitersForTesting:
        [CheckedContinuation<Void, Never>] = []
    #endif

    /// Decision log for evaluation harness. Capped to prevent unbounded growth.
    private var decisionLog: [SkipDecisionRecord] = []
    private let decisionLogCapacity = 500

    /// Callback to push skip cues to PlaybackService.
    /// Set via `setSkipCueHandler`. Avoids direct PlaybackServiceActor coupling.
    private var skipCueHandler: (([CMTimeRange]) -> Void)?

    /// Per-show skip mode for the current episode. Loaded from TrustScoringService
    /// at episode start. Defaults to `.shadow` if no trust service is wired.
    private var activeSkipMode: SkipMode = .shadow

    /// playhead-djl0: WHY `activeSkipMode` holds its current value.
    ///
    /// `activeSkipMode` alone cannot distinguish a show deliberately being
    /// observed from a session that lost the show's identity — both are
    /// `.shadow`. This field carries the distinction so the failures can be
    /// counted, recorded and shown. Nothing in the skip policy reads it: the
    /// policy input is still `activeSkipMode` and only `activeSkipMode`.
    private var activeSkipModeResolution: SkipModeResolution = .noActiveEpisode

    /// playhead-gard: the PER-DETECTOR skip modes for this episode.
    ///
    /// `activeSkipMode` above is still the show-level answer — it is what the
    /// Now Playing pill renders, what `currentSkipMode()` returns, and what an
    /// older binary reads out of `podcast_profiles.mode`. It is no longer the
    /// SKIP POLICY INPUT. Every window is now evaluated against the mode of the
    /// detector class that produced it, because a `segmentAggregated` window's
    /// vetoes are not evidence about the byte-exact rediff differ.
    ///
    /// Resolved once per episode, in the same `beginEpisode` lookup that
    /// resolves the show mode — one row read, four classes, no per-window
    /// suspension. A class this map does not carry falls back to
    /// `activeSkipMode`, which is what governed everything before this bead.
    private var activeDetectorSkipModes: DetectorSkipModes = .noActiveEpisode

    /// playhead-djl0: how many episode starts have hit each lookup failure,
    /// this process. Per cause rather than one total, so "the trust service is
    /// unwired" can never be read as "we keep losing show identities" — the
    /// mistake the single silent `.shadow` branch made possible in the first
    /// place. Non-failure resolutions are never counted (see
    /// `SkipModeResolution.isLookupFailure`).
    private var skipModeResolutionFailureCounts: [SkipModeResolution: Int] = [:]

    /// playhead-isp5: how many windows have hit each ingest disposition, this
    /// process. A process-lifetime tally like `skipModeResolutionFailureCounts`
    /// above and for the same reason — a per-episode counter answers "did this
    /// episode lose anything?" but not "does this build lose PRE-ROLLS", which
    /// is the shape the field defect actually had.
    ///
    /// Written by every `receiveAdWindows` caller, not just the two persisted
    /// doors: a drop cause is not a different fact because a different producer
    /// supplied the row. Per-DELIVERY attribution is the census's job.
    private var adWindowIngestOutcomeCounts: [AdWindowIngestOutcome: Int] = [:]

    /// playhead-zxqj: how many "Dismiss ad" gestures have reached each terminal
    /// disposition this PROCESS. Process-scoped rather than per-episode, like
    /// `adWindowIngestOutcomeCounts` above, because the question it answers
    /// ("did the listener's dismiss do anything?") spans episodes.
    private var manualVetoOutcomeCounts: [ManualVetoOutcome: Int] = [:]

    /// playhead-isp5: the most recent terminal disposition of each window id,
    /// with its sub-cause where one exists. Per-EPISODE (cleared alongside
    /// every other per-episode collection) because it exists only so a door can
    /// read back where the rows IT forwarded ended up, after
    /// `receiveAdWindows` returns.
    ///
    /// Keyed by window id rather than accumulated into a per-call buffer
    /// because `receiveAdWindows` suspends on the catalog actor: a buffer would
    /// silently absorb an interleaved producer's rows and misattribute them.
    private var lastIngestOutcomeByWindowId:
        [String: (outcome: AdWindowIngestOutcome, detail: String?)] = [:]

    /// playhead-usn1: continuation-backed stream of `SkipModeSnapshot`.
    ///
    /// The mode and its cause are only correct AFTER `beginEpisode` has resolved
    /// the show — which happens many suspensions into `PlayheadRuntime
    /// .performPlayEpisode`, long after the Now Playing screen has appeared and
    /// taken its one-shot reading. A pull could only ever be a race; this pushes.
    private var skipModeContinuations: [UUID: AsyncStream<SkipModeSnapshot>.Continuation] = [:]

    /// Continuation-backed stream of applied ad segment time ranges (seconds).
    /// Consumers receive the full set of applied segments whenever the set changes.
    private var segmentContinuations: [UUID: AsyncStream<[(start: Double, end: Double)]>.Continuation] = [:]

    /// Continuation-backed stream of banner items.
    /// playhead-bwxi: emits once per window, on the position observation that
    /// ENTERS an `.applied` window's span — not when the decision was made.
    private var bannerContinuations: [UUID: AsyncStream<AdSkipBannerItem>.Continuation] = [:]

    /// Production banner stream, including ordered invalidations.
    private var bannerEventContinuations:
        [UUID: AsyncStream<AdBannerStreamEvent>.Continuation] = [:]

    /// Window IDs for which a banner has already been emitted. Prevents re-fires.
    private var banneredWindowIds: Set<String> = []

    /// Cycle-23 H-1: window IDs for which `emitBannerItem` was actually
    /// invoked this episode (not merely "banner suppression flagged").
    /// `banneredWindowIds` is populated both BY emission AND by
    /// `beginEpisode`'s pre-population for preloaded `.applied` rows
    /// — meaning a snapshot of `banneredWindowIds` cannot distinguish
    /// "the gate was pre-populated" from "the eval loop emitted and
    /// then inserted." This separate set records ONLY actual auto-
    /// skip-tier banner emissions, so tests can deterministically
    /// assert "no auto-skip banner was emitted for window X" without
    /// any iteration-order coupling.
    ///
    /// **Cycle-26 L-1: TEST-ONLY OBSERVABILITY.** Production logic does
    /// NOT read this set — the gate that suppresses re-emission is
    /// `banneredWindowIds`, not this. The only reader is
    /// `emittedAutoSkipBannersSnapshot()`, called from
    /// `SkipOrchestratorPreloadTests`. Three operations on this set are
    /// load-bearing for those tests; do NOT delete any of them as "dead
    /// state":
    ///   • the `insert` in `emitBannerItem` (records actual emissions),
    ///   • the `removeAll` in `beginEpisode` (resets per-episode state),
    ///   • the `removeAll` in `endEpisode` (the cross-episode regression
    ///     test `testEmittedAutoSkipBannersDoesNotLeakAcrossEpisodes`
    ///     fails if this clear is dropped).
    private var emittedAutoSkipBannerWindowIds: Set<String> = []

    /// playhead-bwxi: window IDs whose auto-skip-tier banner is DECIDED but
    /// not yet PRESENTED, because the playhead has not entered their span.
    ///
    /// THE FIELD PROOF (Dan's device, 2026-08-21, asset 0FF7EFF3, 4309.4 s).
    /// `correction_events`, verbatim:
    ///
    ///     08:31:36  bannerAutoSkipConfirmed  [   0.000 -   86.831]
    ///     08:31:39  bannerAutoSkipConfirmed  [1369.809 - 1548.487]
    ///     08:31:40  bannerAutoSkipConfirmed  [3367.262 - 3534.576]
    ///     08:31:42  bannerAutoSkipConfirmed  [4279.302 - 4309.420]
    ///
    /// Four banners inside six seconds, for windows at 0, 23, 56 and 71
    /// MINUTES. Only the first was audio the listener had reached. Three
    /// `bannerAutoSkipConfirmed` rows — the strongest positive signal the trust
    /// system takes — were recorded for audio he had not heard, and he stopped
    /// using the banner inside one episode.
    ///
    /// THE CAUSE, and it is the mirror of playhead-d3g0 one tier over.
    /// `evaluateAndPush` promoted every eligible window to `.applied` in one
    /// pass and called `emitBannerItem` from inside that loop, so the auto tier
    /// presented at DECISION time. The decision is made for the whole episode
    /// at once (nothing in `evaluateWindow` looks at whether the playhead has
    /// reached the span — only whether it is past its END, the late-detection
    /// rule), so one ingest produced one banner per window. d3g0 fixed exactly
    /// this shape on the suggest tier on 2026-07-31 and left this tier alone,
    /// under the belief — written into `SuggestBannerEntryGateTests`' own
    /// header — that the auto banner "was always playhead-driven". It was not.
    ///
    /// Promotion now ARMS. Presentation happens in `updatePlayheadTime`, and
    /// ONLY there, for the same reason d3g0 gives: `currentPlayheadTime` can be
    /// a stale 0 between `beginEpisode` and the first position observation, so
    /// a synchronous containment test at promotion time would banner a preroll
    /// at a listener resuming at 44 minutes. Waiting for the position path
    /// costs at most one observer tick and cannot be wrong.
    ///
    /// The gate that prevents re-fires is still `banneredWindowIds`, which is
    /// written at the same two `evaluateAndPush` sites as before — arming did
    /// not move it. So every downstream reader of that set behaves exactly as
    /// it did; the only thing this bead moved is WHEN `emitBannerItem` runs.
    ///
    /// An armed ID that never has its span entered stays here, inert, until the
    /// episode boundary clears it — the same deliberate non-pruning as
    /// `armedSuggestWindowIds`, and bounded by the episode's window count.
    private var armedAutoSkipBannerWindowIds: Set<String> = []

    /// playhead-2d6i: auto-skips the listener has NOT been shown a card for,
    /// keyed by window id.
    ///
    /// **playhead-8cjo REDEFINED WHEN A ROW LEAVES THIS DICTIONARY, and the
    /// membership rule is now the whole point.** 2d6i wrote a row only when
    /// both continuation dictionaries were empty, i.e. when "nobody is
    /// SUBSCRIBED". A subscriber is not a presentation:
    /// `NowPlayingViewModel.observeBanners` forwards each `.present(item)` to
    /// `AdBannerQueue.enqueue(_:hostGeneration:)`, which returns `false` and
    /// DROPS the item across the reattach window in
    /// `NowPlayingView.onChange(of: bannerPlaybackContext)`, after
    /// `discardAllOnHostDisappear`, or on an episode / lifecycle mismatch — and
    /// the observation `Task` can also be cancelled between the yield and the
    /// enqueue, in which case nothing reaches the queue at all. In every one of
    /// those the skip produced NEITHER a card the listener saw NOR a row.
    ///
    /// So a row is written for EVERY announced auto-skip, attached or not, and
    /// the only thing that removes it is
    /// `acknowledgeAutoSkippedBannerDelivery` — a host reporting that the
    /// QUEUE ACCEPTED the card. The bound on "no acknowledgement" is therefore
    /// ZERO rather than a grace period, which is why this needs no clock: the
    /// conservative answer is the state it is already in. Ask the question this
    /// repo's standing defect class demands — *what does this read if the
    /// acknowledgement path never runs at all?* — and the answer is "every skip
    /// is a correctable row", never "a skip nobody can see was booked as shown".
    ///
    /// ONE INTERVAL EXISTS IN THE OTHER DIRECTION, and it is deliberate: the
    /// row is written before the yield and removed two actor hops later, so a
    /// surface that reads the list in between sees a row for a card that is at
    /// that moment being presented. It is milliseconds, it self-corrects, and
    /// the alternative — writing the row only after a refusal is reported —
    /// is the design this bead exists to reject, because the commonest loss is
    /// an observation task cancelled before anything is reported at all.
    ///
    /// THE ASYMMETRY 2d6i CLOSED, kept because it is why the dictionary exists.
    /// `emitBannerItem` used to return without yielding when both continuation
    /// dictionaries were empty — and both the pre- and post-bwxi paths consume
    /// the window's one chance BEFORE that check (`banneredWindowIds.insert` in
    /// `evaluateAndPush`, then `armedAutoSkipBannerWindowIds.remove` in
    /// `emitAutoSkipBannersOnPlayheadEntry`). So an auto-skip that fired from
    /// the lock screen, from CarPlay, from a widget start, or during any locked
    /// stretch left no receipt at all and could never be corrected. The suggest
    /// tier has had `replayPendingSuggestBanners` since playhead-d3g0; the auto
    /// tier had nothing.
    ///
    /// KEYED BY WINDOW, WHICH IS THE DOUBLE-DELIVERY GUARANTEE. A dictionary
    /// keyed by `windowId` cannot hold two entries for one window, so "exactly
    /// one list entry, not one per subsequent attach" is a property of the
    /// STATE rather than of a delivery gate somebody could get wrong. The
    /// suggest tier needs a gate because it PUSHES on subscribe; this list is
    /// PULLED, so there is nothing to deliver twice.
    ///
    /// NOT PRUNED when a window is reverted, retired or corrected — the same
    /// deliberate non-pruning as `armedSuggestWindowIds`, and for a stronger
    /// reason here: `missedAutoSkipReceipts()` re-derives vetoability from
    /// `windows` at READ time, so an entry whose window no longer satisfies
    /// `denyAutoSkippedBanner`'s preconditions simply stops being listed. That
    /// is one predicate instead of a removal call at every retirement site, and
    /// a retirement site nobody remembered is exactly how a list comes to offer
    /// a correction the transaction will refuse. Bounded by the episode's
    /// window count and cleared at both episode boundaries.
    private var missedAutoSkipReceiptsByWindowId:
        [String: MissedAutoSkipReceipt] = [:]

    /// playhead-8cjo: window IDs whose auto-skip card a host reported the
    /// BANNER QUEUE ACCEPTED — which is NOT the same as "a card was shown",
    /// and the gap is stated here rather than discovered later.
    ///
    /// `AdBannerQueue.enqueue` returns `true` the moment the item is admitted
    /// to the lane. A card queued BEHIND another (an ad pod: two adjacent
    /// windows, entered a tick apart, which `canCoalesce` refuses to merge
    /// because their ids differ) is admitted and not yet presented, and
    /// `discardAllOnHostDisappear` destroys the pending lane. That card was
    /// acknowledged, so it leaves no row. The boundary that would close it is
    /// `AdBannerQueue.recordBannerShown(for:)`, whose own comment says
    /// "Queue-current is not the same as user-visible"; moving the seam there
    /// is a view-layer change and is filed rather than taken here.
    ///
    /// **NOT THE SAME SET AS `emittedAutoSkipBannerWindowIds`, and confusing
    /// the two is this bead.** That one means "reached the
    /// yield-to-subscriber path" — it is written the moment a continuation
    /// exists, which is precisely the claim that turned out not to be a
    /// presentation. This one is written only when the host comes back and
    /// says the queue took it, so `delivered ⊆ emitted` and the gap between
    /// them is exactly the population that used to vanish.
    ///
    /// **TEST-ONLY OBSERVABILITY, exactly like its sibling.** Production logic
    /// does not read it; the seam's own behaviour is the receipt REMOVAL.
    /// `deliveredAutoSkipCardWindowIDs()` is the only reader, and it exists so
    /// the partition rails can name a positive fact instead of deriving one
    /// (`entered \ list` would be unfalsifiable — it cannot distinguish a card
    /// from a receipt somebody forgot to write). Cleared at both episode
    /// boundaries: a leak would let the next episode's partition credit this
    /// episode's card, and window ids are not unique across episodes.
    private var deliveredAutoSkipCardWindowIds: Set<String> = []

    /// playhead-bwxi: has ANY position observation arrived for this episode?
    ///
    /// `currentPlayheadTime` is a stale 0 between `beginEpisode` and the first
    /// observation (the same hazard `armedSuggestWindowIds` documents), so
    /// stamping it onto a correction unobserved would assert the listener was
    /// at the top of the episode. That is the one reading this bead's column
    /// exists to make impossible, so an unobserved position is recorded as
    /// NULL — unknown — rather than as zero.
    private var hasObservedPlayheadThisEpisode = false

    /// The listener's position, or `nil` when nothing has observed it yet.
    /// Stamped onto every correction receipt this actor writes.
    private var observedPlayheadTimeForCorrection: TimeInterval? {
        hasObservedPlayheadThisEpisode ? currentPlayheadTime : nil
    }

    /// playhead-d3g0: worst-case wall-clock delay the suggest banner is allowed
    /// between the playhead crossing an ad span's start and the banner item
    /// reaching the stream.
    ///
    /// Dan's decision ("when it enters so I can skip") turns this from polish
    /// into correctness: the banner is a PROSPECTIVE skip affordance, so a
    /// banner that arrives three seconds late is a banner for audio the
    /// listener is already hearing. The budget is not a preference — the
    /// dominant term is `PlaybackService.periodicTimeObserverIntervalSeconds`
    /// (the orchestrator cannot learn the playhead moved sooner than the
    /// position observer says it did), and the rest is the actor hop from the
    /// position observer into this actor, which is sub-millisecond. Pinned
    /// against the transport constant by
    /// `SuggestBannerEntryGateTests.entryLatencyBudgetIsTiedToTheTransportTick`.
    static let suggestEntryLatencyBudgetSeconds: TimeInterval = 0.5

    /// playhead-gtt9.23: window IDs for which a `.suggest` tier banner has
    /// already been emitted. Tracked separately from `banneredWindowIds`
    /// (auto-skip-tier emissions) so the two paths don't collide on a
    /// gate-flip mid-episode (a window first seen as markOnly that later
    /// promotes to auto-skip is allowed to emit a fresh auto-skipped
    /// banner — the user-facing event "we just skipped this" is the new
    /// information, not a duplicate).
    private var suggestBanneredWindowIds: Set<String> = []

    /// playhead-gtt9.23: in-memory record of windows currently surfaced as
    /// suggest-tier markers. Keyed by `AdWindow.id`. We hold them here
    /// rather than in `windows` so the auto-skip evaluation loop never
    /// considers them — the tier is strictly a UI surface, not a skip
    /// candidate. Cleared at episode end.
    private var suggestWindows: [String: AdWindow] = [:]

    /// playhead-d3g0: suggestion IDs that are REGISTERED but have not yet had
    /// their banner emitted, because the playhead has not entered their span.
    ///
    /// This set is the whole bead. `registerSuggestedWindow` used to emit
    /// immediately from `receiveAdWindows` — the DETECTION delivery path — so
    /// the uncertain banner had no playhead gate at all. On 2026-07-31 that
    /// delivered three banners for spans 3.5, 44.5 and 80 minutes into an
    /// episode as one batch; all three were answered inside 20.1 s, none of
    /// them heard, and two were false positives that cost 210 s of show.
    ///
    /// Registration now ARMS. Emission happens in `updatePlayheadTime`, and
    /// ONLY there — deliberately never synchronously at registration, because
    /// `currentPlayheadTime` can be a stale 0 between `beginEpisode` and the
    /// first position observation, and a preloaded pre-roll would then banner
    /// for a listener who is resuming at 44 minutes. Waiting for the position
    /// path costs at most one observer tick (see
    /// `suggestEntryLatencyBudgetSeconds`) and cannot be wrong.
    ///
    /// Leaving the set is one-way per revision: an armed window fires once on
    /// entry and is never re-armed unless the producer delivers materially new
    /// material (`registerSuggestedWindow`'s revision-changed branch). That is
    /// what makes a backwards seek not re-ask an answered question, and it is
    /// the same "at most once per window per episode" guarantee the tier always
    /// had, now with a position precondition in front of it.
    ///
    /// IDs are deliberately NOT pruned when a suggestion leaves `suggestWindows`
    /// (veto, accept, retirement). An armed ID with no matching entry is inert —
    /// both readers resolve through `suggestWindows` — and keeping it is the
    /// consistent choice for the one path that puts a window BACK:
    /// `restoreSuggestionAfterFailedResolution`'s fallback re-insert. A window
    /// restored while still armed keeps waiting for its span; pruning would
    /// instead make it replay-eligible to a newly attached host without the
    /// playhead ever having reached it, which is the leak this bead closes.
    /// The set is bounded by the episode's suggestion count and cleared at both
    /// episode boundaries.
    private var armedSuggestWindowIds: Set<String> = []

    /// Revision identity for each producer suggestion ID. A producer may
    /// recompute a window in-place (same ID and playback lifecycle, different
    /// span or attribution). The UI token must therefore identify the current
    /// revision, not merely the producer ID.
    private var suggestRevisionTokensByWindowId: [String: String] = [:]

    /// Last producer value used to decide whether a same-ID delivery is an
    /// exact replay or a new actionable revision. Retained after a neutral or
    /// explicit exit so an identical late delivery keeps the old presentation
    /// gate, while a materially changed value gets a fresh token and card.
    private var lastSuggestRevisionByWindowId: [String: AdWindow] = [:]

    /// Revision tokens whose delivery reached the active banner host.
    /// Presentation acknowledgement is revision-scoped so a late ack from an
    /// older same-ID card cannot suppress replay of the current revision.
    private var acknowledgedSuggestRevisionTokens: Set<String> = []

    /// Suggestion IDs explicitly vetoed during the active episode. Unlike a
    /// presentation acknowledgement, a user "No" is authoritative for the
    /// producer row itself: neither an exact late replay nor a materially
    /// changed stale producer value with the same ID may recreate the card.
    private var vetoedSuggestWindowIds: Set<String> = []

    /// playhead-rfu-sad: tap-then-flip race guard. AdWindow ids that have
    /// already been promoted via `acceptSuggestedSkip` (the user tapped
    /// the suggest banner). A late-arriving ingest with the same id and
    /// any gate must NOT register a second managed or suggested window —
    /// the promoted UUID-keyed entry is already authoritative for that span.
    ///
    /// This set is intentionally unbounded within one playback lifecycle.
    /// It is cleared at both `beginEpisode` and `endEpisode`; evicting an
    /// accepted ID earlier would let a sufficiently late producer replay
    /// create a second durable promotion for the same explicit Yes.
    private var recentlyAcceptedSuggestIds: Set<String> = []

    /// Producer updates that arrive while an explicit suggest response is
    /// waiting for its transaction. The user's answer becomes terminal only
    /// after persistence succeeds; on failure, the newest buffered producer
    /// value—not the stale pre-tap revision—must resume ownership.
    private enum BufferedSuggestProducerUpdate {
        case adWindow(AdWindow)
        case decisionResult(AdDecisionResult)
        case retired
    }
    private var provisionallyResolvingSuggestWindowIds: Set<String> = []
    private var bufferedSuggestProducerUpdates:
        [String: BufferedSuggestProducerUpdate] = [:]

    /// A user's explicit Yes or No is terminal for that producer ID for the
    /// active episode, regardless of which ingress path or eligibility gate
    /// redelivers it afterward.
    private func hasTerminalSuggestResolution(_ windowId: String) -> Bool {
        vetoedSuggestWindowIds.contains(windowId)
            || recentlyAcceptedSuggestIds.contains(windowId)
    }

    /// The podcast ID for the current episode. Needed to populate banner items.
    private var activePodcastId: String?
    /// Latest-wins token for reentrant episode starts. `beginEpisode` performs
    /// several actor hops while hydrating trust and persisted windows; an older
    /// start must not resume and overwrite a newer episode's state.
    private var episodeLifecycleGeneration: UInt64 = 0
    /// Latest producer mutation admitted for each window ID. Catalog admission
    /// suspends on both AnalysisStore and AdCatalogStore; a newer same-ID
    /// revision or retirement can otherwise land during that suspension and
    /// then be overwritten when the older validation resumes. Generations are
    /// allocated per inbound batch so an older batch may still finish IDs a
    /// newer batch did not touch, while every shared ID is strict latest-wins.
    private var producerMutationGeneration: UInt64 = 0
    private var latestProducerMutationByWindowId: [String: UInt64] = [:]
    /// Exact producer revisions that reached a terminal persisted state during
    /// this playback lifecycle. Suppression is revision-scoped: genuinely new
    /// same-ID material may be reconsidered, but an older byte-identical
    /// candidate/eligible replay cannot resurrect the terminal row. Explicit
    /// user reverts remain ID-terminal through the dedicated ID fence below.
    private var terminalProducerRevisionsByWindowId:
        [String: [AdWindow]] = [:]
    /// Persisted `.reverted` producer rows represent an explicit user answer
    /// and therefore remain terminal for the whole producer ID, even if a
    /// stale delivery changes other material. Suppressed rows use only the
    /// exact-revision fence above.
    private var revertedProducerWindowIds: Set<String> = []
    /// Runtime playback request that owns the active episode transaction.
    /// Unlike canonical episode identity, this changes when the same episode
    /// is replayed or replaced and is stamped onto every banner emission.
    private var activePlaybackLifecycleGeneration: UInt64?

    /// The deterministic evidence catalog for the current episode's transcript,
    /// pushed by `AnalysisCoordinator` whenever new transcript material lands.
    /// Sliced per-window when emitting banner items so callers see only the
    /// evidence that overlaps the skipped span. `nil` when no catalog has
    /// been pushed for the active asset — the banner falls back to an empty
    /// `evidenceCatalogEntries` array, which the UI handles gracefully.
    private var activeEvidenceCatalog: EvidenceCatalog?

    /// Hasher used to stamp `auto_skip_fired` events with a per-install
    /// episode ID hash. Production passes a closure bound to the shared
    /// `SurfaceStatusInvariantLogger` instance so the hash is byte-
    /// identical to the one `EpisodeSurfaceStatusObserver` stamps on
    /// `ready_entered`. Tests can pin the hash to a known value
    /// independent of the logger's installId.
    private let episodeIdHasher: @Sendable (String) -> String

    /// The audit logger instance that `auto_skip_fired` events are
    /// written to. Shared with `EpisodeSurfaceStatusObserver` so both
    /// producers of the false_ready_rate pair land on the same file
    /// with the same installId.
    private let invariantLogger: SurfaceStatusInvariantLogger

    // MARK: - playhead-xr3t: inventory sanity filter

    /// playhead-xr3t: post-hoc filter applied to spans arriving at the
    /// fusion → user-visible-skip-decision boundary. Constructed from
    /// `LightweightInventoryChecksSettings` at init time. When the flag
    /// is OFF the filter is a no-op pass-through and behaviour is
    /// byte-identical to the pre-bead orchestrator.
    ///
    /// The filter is stateless; per-episode context (duration,
    /// declared chapters) is supplied at evaluation time from the
    /// orchestrator's `activeEpisodeDuration` / `activeDeclaredChapters`
    /// fields below.
    private let inventoryFilter: InventorySanityFilter

    /// playhead-xr3t: episode duration for the active episode in
    /// seconds. Set in `beginEpisode` from
    /// `AnalysisAsset.episodeDurationSec` (best-effort fetch) and
    /// updatable mid-episode via `setEpisodeDuration(_:)` when the
    /// duration-backfill probe rewrites the row. `nil` when the
    /// asset row carries no duration yet — the filter treats that
    /// as "tail edge unknown" and applies only the head-edge rule.
    private var activeEpisodeDuration: Double?

    /// playhead-xr3t: declared (publisher-provided) content chapters
    /// for the active episode. Loaded by AdDetectionService on its
    /// metadata fetch and pushed via `setDeclaredChapters(_:)`. Only
    /// creator-source ChapterEvidence (id3, pc20, rssInline) is
    /// stored here — `.inferred` chapters are filtered out by
    /// `setDeclaredChapters` so the inventory filter cannot
    /// accidentally consult them.
    private var activeDeclaredChapters: [ChapterEvidence] = []

    // MARK: - Init

    /// - Parameters:
    ///   - invariantLogger: The audit logger instance this orchestrator
    ///     writes `auto_skip_fired` events to. Defaults to a fresh
    ///     instance — test suites that don't inspect the log get an
    ///     isolated logger per orchestrator (no cross-test file races).
    ///     Production passes the runtime-shared instance so the companion
    ///     `ready_entered` producer (EpisodeSurfaceStatusObserver) lands
    ///     on the same file with the same installId.
    ///   - episodeIdHasher: Hasher for the `episode_id_hash` field.
    ///     When `nil`, derived from `invariantLogger.hashEpisodeId` so
    ///     production events naturally pair with the observer's. Tests
    ///     that want a pinned hash pass a deterministic closure.
    init(
        store: AnalysisStore,
        config: SkipPolicyConfig = .default,
        trustService: TrustScoringService? = nil,
        correctionStore: (any UserCorrectionStore)? = nil,
        adCatalogStore: AdCatalogStore? = nil,
        repeatedAdCache: RepeatedAdCacheService? = nil,
        invariantLogger: SurfaceStatusInvariantLogger = SurfaceStatusInvariantLogger(),
        episodeIdHasher: (@Sendable (String) -> String)? = nil,
        // playhead-b6r2: the default is the configuration PRODUCTION runs on
        // a fresh install, bound to it by construction rather than by two
        // literals that agree until they don't.
        //
        // It used to be `InventorySanityFilter(isEnabled: false)`, and the
        // xr3t review comment it carried named the hazard outright — "so
        // pre-existing test surface ... doesn't silently lose pre-roll /
        // post-roll spans to the head-/tail-edge rules". The review knew the
        // edge rules ate pre-rolls and turned the guard off in the OBSERVATION
        // surface while leaving it on in the field. Consequence: playhead-djl0
        // reproduced the 2026-08-01 field case exactly — same asset, same four
        // windows, same `start: 0, end: 45.1` — asserted the banner IS emitted,
        // and PASSED, for eleven weeks, because its orchestrator's filter was
        // off. Two investigations were lost to a hazard that had been routed
        // around instead of fixed.
        //
        // Still no `UserDefaults` dependency: this reads
        // `LightweightInventoryChecksSettings.defaultEnabled`, a constant, not
        // `.load()`. A suite that wants the filter OFF passes one explicitly,
        // which is a statement rather than an inheritance.
        inventoryFilter: InventorySanityFilter = .productionDefaultConfiguration
    ) {
        self.store = store
        self.adCatalogStore = adCatalogStore
        self.repeatedAdCache = repeatedAdCache
        self.config = config
        self.trustService = trustService
        self.correctionStore = correctionStore
        self.invariantLogger = invariantLogger
        self.episodeIdHasher = episodeIdHasher ?? { [invariantLogger] episodeId in
            invariantLogger.hashEpisodeId(episodeId)
        }
        self.inventoryFilter = inventoryFilter
    }

    // MARK: - Configuration

    /// Set the callback that pushes skip cues to PlaybackService.
    func setSkipCueHandler(_ handler: @escaping @Sendable ([CMTimeRange]) -> Void) {
        skipCueHandler = handler
    }

#if DEBUG
    func _setFalseSkipSignalHandlerForTesting(
        _ handler: (@Sendable (String) async -> Void)?
    ) {
        falseSkipSignalHandlerForTesting = handler
    }

    func _setFalseNegativeSignalHandlerForTesting(
        _ handler: (@Sendable (String) async -> Void)?
    ) {
        falseNegativeSignalHandlerForTesting = handler
    }

    func _setCorrectObservationHandlerForTesting(
        _ handler: (@Sendable (String, String, SkipDetectorClass) async -> Void)?
    ) {
        correctObservationHandlerForTesting = handler
    }

    func _setSuggestPersistenceBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        feedbackPersistenceBarrierForTesting = barrier
    }

    func _setFeedbackPersistenceBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        feedbackPersistenceBarrierForTesting = barrier
    }

    func _setAppliedPersistenceBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        appliedPersistenceBarrierForTesting = barrier
    }

    func _setCatalogAdmissionValidationBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        catalogAdmissionValidationBarrierForTesting = barrier
    }

    func _setBeginEpisodeHydrationBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        beginEpisodeHydrationBarrierForTesting = barrier
    }

    func _setCatalogLearningPersistenceBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        catalogLearningPersistenceBarrierForTesting = barrier
    }

    func _waitForRecurrenceBackgroundWorkForTesting() async {
        guard recurrenceBackgroundWorkCount > 0 else { return }
        await withCheckedContinuation { continuation in
            recurrenceBackgroundWorkWaitersForTesting.append(continuation)
        }
    }

    func _setRevertPersistenceBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        revertPersistenceBarrierForTesting = barrier
    }

    func _suggestWindowForTesting(id: String) -> AdWindow? {
        suggestWindows[id]
    }

    /// playhead-zxqj: the managed tier's live decision for `id`.
    ///
    /// The companion of `_suggestWindowForTesting`, added for the same reason:
    /// `activeWindowIDs()` cannot see this, because a reverted managed window
    /// stays IN `windows` carrying its terminal state — so membership is the
    /// wrong question and was going to be asked anyway.
    func _managedDecisionStateForTesting(id: String) -> SkipDecisionState? {
        windows[id]?.decisionState
    }

    func _isSuggestResolutionProvisionalForTesting(id: String) -> Bool {
        provisionallyResolvingSuggestWindowIds.contains(id)
    }
#endif

    // MARK: - Ad Segment Stream

    /// Returns an AsyncStream of applied ad segment ranges (in seconds).
    /// Each emission is the full current set. The stream ends when the
    /// continuation is cancelled or the orchestrator is deallocated.
    func appliedSegmentsStream() -> AsyncStream<[(start: Double, end: Double)]> {
        let id = UUID()
        return AsyncStream { [self] continuation in
            self.segmentContinuations[id] = continuation
            continuation.onTermination = { [weak self] _ in
                Task {
                    await self?.removeSegmentContinuation(id: id)
                }
            }
        }
    }

    private func removeSegmentContinuation(id: UUID) {
        segmentContinuations.removeValue(forKey: id)
    }

    // MARK: - Skip Mode Stream (playhead-usn1)

    /// Returns an `AsyncStream` of the active `SkipModeSnapshot`.
    ///
    /// The CURRENT snapshot is delivered immediately on subscribe, so a
    /// subscriber that attaches before, during or after `beginEpisode` all end
    /// up with the same answer. That replay is what makes this a replacement for
    /// the one-shot `currentSkipMode()` / `currentSkipModeResolution()` read at
    /// screen-appear time rather than an addition to it: the screen appears
    /// while `performPlayEpisode` is still awaiting the asset resolution that
    /// precedes `beginEpisode`, so the one-shot read observed the value
    /// `endEpisode` had just installed — `.shadow` / `.noActiveEpisode` — for a
    /// show whose identity resolves perfectly, and never looked again.
    func skipModeStream() -> AsyncStream<SkipModeSnapshot> {
        let id = UUID()
        return AsyncStream { [self] continuation in
            self.skipModeContinuations[id] = continuation
            continuation.onTermination = { [weak self] _ in
                Task {
                    await self?.removeSkipModeContinuation(id: id)
                }
            }
            continuation.yield(self.currentSkipModeSnapshot())
        }
    }

    private func removeSkipModeContinuation(id: UUID) {
        skipModeContinuations.removeValue(forKey: id)
    }

    /// The pair as it stands right now.
    func currentSkipModeSnapshot() -> SkipModeSnapshot {
        SkipModeSnapshot(mode: activeSkipMode, resolution: activeSkipModeResolution)
    }

    /// Broadcast the current pair to every subscriber.
    ///
    /// Called at EVERY transition that writes `activeSkipMode` or
    /// `activeSkipModeResolution` — `beginEpisode` (both the pre-suspension
    /// clear and the post-lookup verdict), `endEpisode`, and
    /// `setActiveSkipMode`. Missing one is how the surface goes stale again, so
    /// the mutation battery re-injects each omission separately.
    private func publishSkipMode() {
        let snapshot = currentSkipModeSnapshot()
        for (_, continuation) in skipModeContinuations {
            continuation.yield(snapshot)
        }
    }

    // MARK: - Banner Item Stream

    /// Returns an AsyncStream that emits an AdSkipBannerItem after an
    /// automatic skip is actually applied.
    /// Each window fires at most once per episode, regardless of subsequent state changes.
    func bannerItemStream() -> AsyncStream<AdSkipBannerItem> {
        let id = UUID()
        return AsyncStream { [self] continuation in
            self.bannerContinuations[id] = continuation
            continuation.onTermination = { [weak self] _ in
                Task {
                    await self?.removeBannerContinuation(id: id)
                }
            }
            // Suggestions can be produced while Now Playing is absent. Keep
            // them pending, but do not call them "bannered" until a real
            // subscriber exists. A newly attached host receives each pending
            // suggestion exactly once.
            self.replayPendingSuggestBanners(to: continuation)
        }
    }

    private func removeBannerContinuation(id: UUID) {
        bannerContinuations.removeValue(forKey: id)
    }

    /// Returns the ordered production banner event stream.
    func bannerEventStream() -> AsyncStream<AdBannerStreamEvent> {
        let id = UUID()
        return AsyncStream { [self] continuation in
            self.bannerEventContinuations[id] = continuation
            continuation.onTermination = { [weak self] _ in
                Task {
                    await self?.removeBannerEventContinuation(id: id)
                }
            }
            self.replayPendingSuggestBannerEvents(to: continuation)
        }
    }

    private func removeBannerEventContinuation(id: UUID) {
        bannerEventContinuations.removeValue(forKey: id)
    }

    // MARK: - Evidence Catalog (banner transparency)

    /// Push the deterministic evidence catalog for the active asset. The
    /// catalog is sliced per-window when emitting banners so each banner
    /// carries only the evidence overlapping its skipped span.
    ///
    /// Callers may push successively richer catalogs as transcript material
    /// arrives (e.g. fast pass first, then final). Late-arriving catalogs do
    /// NOT retroactively update banners already emitted — those carry the
    /// snapshot taken at emit time.
    ///
    /// Mismatched-asset catalogs are dropped silently: the orchestrator only
    /// retains a catalog whose `analysisAssetId` matches `activeAssetId`. This
    /// guards against late deliveries that race a podcast/episode change.
    func setEvidenceCatalog(_ catalog: EvidenceCatalog) {
        guard let activeAssetId, catalog.analysisAssetId == activeAssetId else {
            // playhead-rfu-sad: bumped from .debug to .info. The
            // common case here is benign: an asset-switch race where a
            // catalog finished building for the previous episode just
            // as the user moved to the next one. The catalog is
            // correctly dropped and `evidenceCatalogEntries` falls back
            // to empty for the new asset, which the UI handles.
            // The non-benign case is a real wiring regression (catalog
            // dispatcher routed to the wrong orchestrator instance,
            // duplicate active orchestrators, etc.). Both cases want
            // visibility: at .debug it is invisible in release; at
            // .info it surfaces in os_log without polluting
            // .notice/.error budgets so the wiring regression is
            // diagnosable from a user log without drowning the signal.
            let activeDescription = self.activeAssetId ?? "nil"
            logger.info(
                "Dropping evidence catalog for non-active asset \(catalog.analysisAssetId, privacy: .public) (active=\(activeDescription, privacy: .public))"
            )
            return
        }
        activeEvidenceCatalog = catalog
    }

    /// Announce an auto-skip: record its receipt, and offer it to every banner
    /// listener.
    ///
    /// playhead-2d6i: THIS IS THE WHOLE INVARIANT, WRITTEN ONCE.
    ///
    ///     a CARD  iff  the playhead is inside the window AND the banner queue
    ///                  ACCEPTED it;
    ///     otherwise exactly one LIST ENTRY.
    ///
    /// The containment half is the caller's
    /// (`emitAutoSkipBannersOnPlayheadEntry`, which is the only caller).
    ///
    /// **playhead-8cjo MOVED THE OTHER HALF OUT OF THIS FUNCTION, and that is
    /// the fix.** 2d6i wrote the second clause as `hasAttachedHost` — an
    /// `if/else` on whether any continuation existed — which reads "somebody is
    /// SUBSCRIBED" as "a card was PRESENTED". `AdBannerQueue.enqueue` can
    /// refuse the item after the yield (a stale host generation, an episode or
    /// lifecycle mismatch), and the observation `Task` can be cancelled before
    /// the enqueue happens at all; in that window the skip left neither card
    /// nor row, which is 2d6i's own defect one layer up.
    ///
    /// So there is no `else` any more: the receipt is written for EVERY
    /// announced skip, and `acknowledgeAutoSkippedBannerDelivery` is the only
    /// thing that takes it away. The branch that remains is about
    /// `emittedAutoSkipBannerWindowIds` and the yields, both of which are
    /// genuinely about a subscriber existing and nothing more.
    private func emitBannerItem(for managed: ManagedWindow) {
        let hasAttachedHost = !bannerContinuations.isEmpty
            || !bannerEventContinuations.isEmpty
        let adWindow = managed.adWindow
        let podcastId = activePodcastId ?? ""
        let entries = catalogEntries(overlapping: managed.snappedStart, end: managed.snappedEnd)
        let item = AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: adWindow.id,
            advertiser: adWindow.advertiser,
            product: adWindow.product,
            adStartTime: managed.snappedStart,
            adEndTime: managed.snappedEnd,
            metadataConfidence: adWindow.metadataConfidence,
            metadataSource: adWindow.metadataSource,
            podcastId: podcastId,
            episodeId: activeEpisodeId,
            playbackLifecycleGeneration:
                activePlaybackLifecycleGeneration,
            analysisAssetId: adWindow.analysisAssetId,
            windowMaterialRevisionToken:
                bannerMaterialRevisionToken(for: managed),
            evidenceCatalogEntries: entries,
            tier: .autoSkipped
        )
        // playhead-2d6i / playhead-8cjo: THE RECEIPT IS THE DEFAULT STATE OF
        // EVERY AUTO-SKIP, and the caller has already spent this window's one
        // chance. Keep it so the listener can still say No to a skip they never
        // saw; `acknowledgeAutoSkippedBannerDelivery` removes it if and only if
        // a host reports that the banner queue accepted the card.
        //
        // Deliberately BEFORE the `emittedAutoSkipBannerWindowIds` insert:
        // that set means "reached the yield-to-subscriber path" and is read
        // by `SkipOrchestratorPreloadTests` as exactly that. A missed
        // receipt is the opposite of an emission and must not enter it.
        // playhead-9don: THE SKIP IS RECORDED HERE, not when the cue was armed.
        //
        // `wasSkipped = 1` used to be written in the same statement as the
        // `.applied` transition, at the two sites that ARM a cue — so the
        // column named "was skipped" recorded "a cue was armed". The two come
        // apart whenever a window is armed far from the playhead, which is
        // exactly what a day-0 rediff landing mid-session does: two windows on
        // the 2026-08-15 pull were stamped ~31 and ~26 minutes of audio before
        // the playhead could reach either, and if the listener quit or seeked
        // past, the 1 stood.
        //
        // This site is reached ONLY from `emitAutoSkipBannersOnPlayheadEntry`,
        // itself reached only from `updatePlayheadTime` — the listener has
        // ARRIVED at the window. That is the same boundary playhead-bwxi moved
        // the card to, and for the same reason: hang the record off the event
        // rather than off a prediction of it.
        //
        // Fire-and-forget: a failed write must not cost the listener the skip
        // or the card. It under-claims (the column stays 0) rather than
        // claiming a cut that may not have happened, which is the direction
        // this bead exists to fix.
        let skippedWindowId = adWindow.id
        Task { [store] in
            do {
                // Guarded: only a row whose `.applied` transition is
                // durably on disk may be marked skipped. A blocked arm leaves
                // the column 0 rather than claiming a cut the store never
                // accepted.
                try await store.markAdWindowSkipExecuted(id: skippedWindowId)
            } catch {
                self.logger.warning(
                    "wasSkipped write failed for \(skippedWindowId, privacy: .public); the column under-claims"
                )
            }
        }
        missedAutoSkipReceiptsByWindowId[adWindow.id] =
            MissedAutoSkipReceipt(
                item: item,
                // Safe to read directly rather than through
                // `observedPlayheadTimeForCorrection`: the only caller is
                // `emitAutoSkipBannersOnPlayheadEntry`, reached only from
                // `updatePlayheadTime`, which sets both fields before
                // calling. An unobserved playhead cannot reach this line —
                // `armedAutoSkipBannerWindowIds` would still hold the id.
                playheadTimeAtSkip: currentPlayheadTime,
                occurredAt: Date()
            )
        // playhead-8cjo: A NEW ANNOUNCEMENT SUPERSEDES THE OLD DELIVERY RECORD,
        // and without this line the partition breaks MID-EPISODE.
        //
        // `removeNonRevertedManagedWindowIfPresent` does
        // `banneredWindowIds.remove(windowId)` when a producer revision replaces
        // a window's material, which UN-SPENDS that window's one chance: it is
        // re-promoted, re-armed, and announced again with a new material token.
        // The id would then sit in BOTH sets — a card the listener saw for the
        // OLD material, and a row for the NEW one — and `cards ∩ list == ∅`,
        // which the partition rails assert at every observation, would be false
        // for as long as the episode lasted.
        //
        // Removing it HERE rather than at each retirement site makes the
        // disjointness a property of the write, the same move playhead-2d6i made
        // for "one row per window" by keying the dictionary. A retirement site
        // nobody remembered is exactly how these two sets would drift apart.
        deliveredAutoSkipCardWindowIds.remove(adWindow.id)
        guard hasAttachedHost else { return }
        // Cycle-26 L-1 / Cycle-27 L-2: this insert is consumed by
        // `emittedAutoSkipBannersSnapshot()` from canary tests. The
        // production gate that prevents re-fires is `banneredWindowIds`
        // — NOT this set. The gate is written at four production sites
        // (pinned by source canary `BanneredWindowIdsInsertSiteCount`):
        // `evaluateAndPush`'s terminal-state branch and its promotion
        // branch (each before calling this method), `beginEpisode`'s
        // preload pre-population for `.applied` rows, and accepted
        // suggestions whose original card already collected feedback (the
        // latter two suppress without calling this method). Do not remove this line as
        // "dead state"; see field doc.
        emittedAutoSkipBannerWindowIds.insert(adWindow.id)
        for (_, continuation) in bannerContinuations {
            continuation.yield(item)
        }
        for (_, continuation) in bannerEventContinuations {
            continuation.yield(.present(item))
        }
    }

    /// playhead-bwxi: the auto tier's ARM step. A window whose skip decision
    /// has been made is not yet a banner; the banner belongs to the position
    /// path, exactly as the suggest tier's does since playhead-d3g0.
    ///
    /// Deliberately unconditional — this never emits, not even when
    /// `currentPlayheadTime` already looks like it is inside the span. See
    /// `armedAutoSkipBannerWindowIds` for why a synchronous containment test
    /// here is unsafe (stale 0 between `beginEpisode` and the first position
    /// observation).
    private func armAutoSkipBanner(for managed: ManagedWindow) {
        armedAutoSkipBannerWindowIds.insert(managed.adWindow.id)
    }

    /// playhead-bwxi: the auto tier's emit trigger, and the twin of
    /// `emitSuggestBannersOnPlayheadEntry`. Present every armed auto-skip
    /// window whose span the playhead has ENTERED.
    ///
    /// "Entered" is the half-open interval `[snappedStart, snappedEnd)` — the
    /// SAME predicate `PlaybackTransport.checkSkipCues` uses to fire the skip
    /// itself (`currentSeconds >= start, currentSeconds < end`), on the same
    /// `AVPlayer` periodic observation. The receipt and the skip therefore
    /// agree by construction rather than by coincidence: if the orchestrator
    /// never observes containment then the transport never fired a skip either,
    /// and there is nothing to announce.
    ///
    /// It survives the skip it announces. `adTrailingCushionSeconds` pulls the
    /// cue's trailing edge IN, so a skip lands at `end - cushion`, which is
    /// still inside `[start, end)`. A missed entry tick is therefore caught by
    /// the LANDING tick rather than lost.
    ///
    /// Leaving the set is one-way per episode, like the suggest tier's: a span
    /// announces itself once, and scrubbing back into it does not re-announce.
    private func emitAutoSkipBannersOnPlayheadEntry(at time: TimeInterval) {
        guard !armedAutoSkipBannerWindowIds.isEmpty else { return }
        let entered = armedAutoSkipBannerWindowIds
            .compactMap { windows[$0] }
            .filter { time >= $0.snappedStart && time < $0.snappedEnd }
            .sorted {
                if $0.snappedStart != $1.snappedStart {
                    return $0.snappedStart < $1.snappedStart
                }
                return $0.adWindow.id < $1.adWindow.id
            }
        for managed in entered {
            armedAutoSkipBannerWindowIds.remove(managed.adWindow.id)
            emitBannerItem(for: managed)
        }
    }

    /// Stable opaque identity for the exact producer material shown on an
    /// auto-skipped card. Length-prefixing strings avoids delimiter ambiguity;
    /// floating-point bit patterns preserve exact persisted values.
    private func bannerMaterialRevisionToken(
        for managed: ManagedWindow
    ) -> String {
        AdWindowMaterialIdentity.autoSkipToken(
            window: managed.adWindow,
            displayedStart: managed.snappedStart,
            displayedEnd: managed.snappedEnd
        )
    }

    /// playhead-gtt9.23: emit a suggest-tier banner for a markOnly window.
    /// Suggest banners ask whether Playhead's sponsor-break assessment was
    /// right; they never imply a skip has happened. Persistence and trust
    /// signals are deferred to the user's answer (handled by `acceptSuggestedSkip` /
    /// `declineSuggestedSkip` below).
    private func emitSuggestBanner(for adWindow: AdWindow) {
        guard !bannerContinuations.isEmpty
                || !bannerEventContinuations.isEmpty
        else {
            return
        }
        let item = makeSuggestBannerItem(for: adWindow)
        var terminatedContinuationIDs: [UUID] = []
        for (id, continuation) in bannerContinuations {
            switch continuation.yield(item) {
            case .enqueued, .dropped:
                break
            case .terminated:
                terminatedContinuationIDs.append(id)
            @unknown default:
                break
            }
        }
        for id in terminatedContinuationIDs {
            bannerContinuations.removeValue(forKey: id)
        }
        var terminatedEventContinuationIDs: [UUID] = []
        for (id, continuation) in bannerEventContinuations {
            switch continuation.yield(.present(item)) {
            case .enqueued, .dropped:
                break
            case .terminated:
                terminatedEventContinuationIDs.append(id)
            @unknown default:
                break
            }
        }
        for id in terminatedEventContinuationIDs {
            bannerEventContinuations.removeValue(forKey: id)
        }
    }

    private func makeSuggestBannerItem(
        for adWindow: AdWindow
    ) -> AdSkipBannerItem {
        let podcastId = activePodcastId ?? ""
        let entries = catalogEntries(overlapping: adWindow.startTime, end: adWindow.endTime)
        let revisionToken: String
        if let existing = suggestRevisionTokensByWindowId[adWindow.id] {
            revisionToken = existing
        } else {
            revisionToken =
                AdWindowMaterialIdentity.suggestionToken(adWindow)
            suggestRevisionTokensByWindowId[adWindow.id] = revisionToken
        }
        return AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: adWindow.id,
            advertiser: adWindow.advertiser,
            product: adWindow.product,
            adStartTime: adWindow.startTime,
            adEndTime: adWindow.endTime,
            metadataConfidence: adWindow.metadataConfidence,
            metadataSource: adWindow.metadataSource,
            podcastId: podcastId,
            episodeId: activeEpisodeId,
            playbackLifecycleGeneration:
                activePlaybackLifecycleGeneration,
            suggestionRevisionToken: revisionToken,
            evidenceCatalogEntries: entries,
            tier: .suggest,
            // playhead-d3g0: the card must not offer an action it cannot
            // perform. Resolved through the SAME helper `acceptSuggestedSkip`
            // uses, so the promise and the transaction cannot drift apart.
            confirmationSkipsPlayback: confirmationWouldSkip(adWindow)
        )
    }

    /// playhead-d3g0 / playhead-ynmk: would confirming this suggestion actually
    /// MOVE playback, or only record a mark?
    ///
    /// ynmk's answer, verbatim in one place: a confirmation asserts presence,
    /// not extent, so its extent stays the detector's and is governed by
    /// `AutoSkipEdgePadding`. A nil late-safe window means the confirmation is
    /// recorded as a MARK and no cue fires.
    ///
    /// This is deliberately the ONLY derivation of that answer. `acceptSuggestedSkip`
    /// decides `decisionState` / `wasSkipped` from it and the banner decides
    /// whether to offer a Skip from it — two readers, one policy call. A second
    /// copy of the expression is how a card comes to promise something the
    /// transaction will not do.
    ///
    /// Anchors come from `resolvedEdgeAnchors`, which falls back to the row's
    /// persisted provenance: a suggest-tier window returns out of
    /// `receiveAdWindows` before the ingest stamp site, so
    /// `edgeAnchorsByWindowId` has no entry for it.
    private func confirmationWouldSkip(_ window: AdWindow) -> Bool {
        let anchors = resolvedEdgeAnchors(for: window)
        return AutoSkipEdgePadding.skipWindow(
            spanStart: window.startTime,
            spanEnd: window.endTime,
            startAnchor: anchors.start,
            endAnchor: anchors.end,
            showKey: activePodcastId
        ) != nil
    }

    /// Register one mark-only producer value as the current suggest revision.
    /// Exact replays preserve their delivery gate. Material changes retire any
    /// older card before arming a fresh token-bound presentation.
    ///
    /// playhead-d3g0: registration ARMS, it does not emit. The banner is a
    /// prospective skip affordance ("when it enters so I can skip"), so the
    /// presentation belongs to the position path — see `armedSuggestWindowIds`
    /// and `emitSuggestBannersOnPlayheadEntry`. The orchestrator still needs to
    /// KNOW about the window at detection time, which is why everything else
    /// here is unchanged.
    private func registerSuggestedWindow(_ adWindow: AdWindow) {
        guard !hasTerminalSuggestResolution(adWindow.id) else {
            return
        }
        let priorRevision = lastSuggestRevisionByWindowId[adWindow.id]
        let revisionChanged = priorRevision.map {
            !Self.sameSuggestRevision($0, adWindow)
        } ?? true

        if revisionChanged {
            if suggestWindows[adWindow.id] != nil
                    || suggestBanneredWindowIds.contains(adWindow.id) {
                emitBannerRetirement(windowId: adWindow.id)
            }
            lastSuggestRevisionByWindowId[adWindow.id] = adWindow
            suggestRevisionTokensByWindowId[adWindow.id] =
                AdWindowMaterialIdentity.suggestionToken(adWindow)
            suggestBanneredWindowIds.remove(adWindow.id)
            suggestWindows[adWindow.id] = adWindow
            armedSuggestWindowIds.insert(adWindow.id)
            return
        }

        guard let revisionToken =
                suggestRevisionTokensByWindowId[adWindow.id],
              !acknowledgedSuggestRevisionTokens.contains(revisionToken)
        else {
            return
        }
        let isNewActiveSuggestion = suggestWindows[adWindow.id] == nil
        suggestWindows[adWindow.id] = adWindow
        if isNewActiveSuggestion {
            armedSuggestWindowIds.insert(adWindow.id)
        }
    }

    /// playhead-d3g0: the emit trigger. Fire every armed suggestion whose span
    /// the playhead has ENTERED.
    ///
    /// "Entered" is the half-open interval `[start, end)`. Inclusive at the
    /// start because that is the instant the skip becomes available and Dan
    /// asked for it there; EXCLUSIVE at the end because a span the playhead has
    /// left offers nothing to skip — asking about it is the "banner for audio
    /// already gone" half of the same field incident.
    ///
    /// Deliberately NOT gated on `skipSuppressedAfterSeek`: that suppression
    /// exists so an automatic skip does not fire on unstable post-seek
    /// confidence. A suggest banner skips nothing on its own, and someone who
    /// just scrubbed into an ad is precisely who wants the affordance.
    ///
    /// Emitting to zero subscribers still disarms. The gate is a fact about
    /// PLAYBACK, not about the UI: a suggestion whose span was played while
    /// Now Playing was absent has passed its precondition, and a host attaching
    /// afterwards receives it through `replayPendingSuggestBanners` — delivered
    /// exactly once, gated on position rather than dropped.
    private func emitSuggestBannersOnPlayheadEntry(at time: TimeInterval) {
        guard !armedSuggestWindowIds.isEmpty else { return }
        let entered = armedSuggestWindowIds
            .compactMap { suggestWindows[$0] }
            .filter { time >= $0.startTime && time < $0.endTime }
            .sorted {
                if $0.startTime != $1.startTime {
                    return $0.startTime < $1.startTime
                }
                return $0.id < $1.id
            }
        for window in entered {
            armedSuggestWindowIds.remove(window.id)
            emitSuggestBanner(for: window)
        }
    }

    /// Equality over every persisted producer field that can change the card,
    /// its correction attribution, or the span acted on by Yes/No.
    private static func sameSuggestRevision(
        _ lhs: AdWindow,
        _ rhs: AdWindow
    ) -> Bool {
        AdWindowMaterialIdentity.suggestionToken(lhs)
            == AdWindowMaterialIdentity.suggestionToken(rhs)
    }

    /// playhead-d3g0: replay is now the SECOND consumer of the playhead gate.
    /// A suggestion that is still armed has not had its span reached, so a host
    /// attaching late must not receive it — otherwise the entry gate would hold
    /// on the emit path and leak through this one, which is the same defect
    /// through a different door. Everything else about replay is unchanged: a
    /// suggestion produced while Now Playing was absent, whose span the
    /// listener has since played, is still delivered exactly once.
    private func replayPendingSuggestBanners(
        to continuation: AsyncStream<AdSkipBannerItem>.Continuation
    ) {
        let pending = suggestWindows.values
            .filter {
                guard !armedSuggestWindowIds.contains($0.id) else {
                    return false
                }
                guard let revisionToken =
                        suggestRevisionTokensByWindowId[$0.id]
                else {
                    return true
                }
                return !acknowledgedSuggestRevisionTokens
                    .contains(revisionToken)
                    && !banneredWindowIds.contains($0.id)
            }
            .sorted {
                if $0.startTime != $1.startTime {
                    return $0.startTime < $1.startTime
                }
                return $0.id < $1.id
            }

        for window in pending {
            switch continuation.yield(makeSuggestBannerItem(for: window)) {
            case .enqueued, .dropped:
                break
            case .terminated:
                return
            @unknown default:
                return
            }
        }
    }

    /// Event-stream twin of `replayPendingSuggestBanners`; the same playhead
    /// gate applies for the same reason.
    private func replayPendingSuggestBannerEvents(
        to continuation: AsyncStream<AdBannerStreamEvent>.Continuation
    ) {
        let pending = suggestWindows.values
            .filter {
                guard !armedSuggestWindowIds.contains($0.id) else {
                    return false
                }
                guard let revisionToken =
                        suggestRevisionTokensByWindowId[$0.id]
                else {
                    return true
                }
                return !acknowledgedSuggestRevisionTokens
                    .contains(revisionToken)
                    && !banneredWindowIds.contains($0.id)
            }
            .sorted {
                if $0.startTime != $1.startTime {
                    return $0.startTime < $1.startTime
                }
                return $0.id < $1.id
            }

        for window in pending {
            continuation.yield(.present(makeSuggestBannerItem(for: window)))
        }
    }

    /// Invalidates an orchestrator-owned banner without manufacturing a
    /// neutral or explicit user response.
    private func emitBannerRetirement(windowId: String) {
        let retirement = AdBannerRetirement(
            windowId: windowId,
            episodeId: activeEpisodeId,
            playbackLifecycleGeneration:
                activePlaybackLifecycleGeneration
        )
        for (_, continuation) in bannerEventContinuations {
            continuation.yield(.retireWindow(retirement))
        }
    }

    /// Atomically invalidates an actionable suggestion and tells the banner
    /// host to retire any presentation for it. Every producer-side invalidation
    /// path must use this helper before returning; otherwise a stale Yes action
    /// can promote a window after a newer precision or inventory decision has
    /// forbidden it.
    @discardableResult
    private func retireSuggestedWindowIfPresent(
        windowId: String
    ) -> Bool {
        invalidatePendingProducerMutation(windowId: windowId)
        guard suggestWindows.removeValue(forKey: windowId) != nil else {
            return false
        }
        emitBannerRetirement(windowId: windowId)
        return true
    }

    /// Removes a managed window whose latest producer decision no longer
    /// permits the active auto-skip path. Reverted state is terminal: a later
    /// producer update must never undo the user's correction.
    @discardableResult
    private func removeNonRevertedManagedWindowIfPresent(
        windowId: String
    ) -> Bool {
        guard let managed = windows[windowId],
              managed.decisionState != .reverted
        else {
            return false
        }
        windows.removeValue(forKey: windowId)
        banneredWindowIds.remove(windowId)
        edgeAnchorsByWindowId.removeValue(forKey: windowId)
        userInitiatedSkipWindowIds.remove(windowId)
        if managed.decisionState == .applied,
           !windows.values.contains(where: {
               $0.decisionState == .applied
           }) {
            inAdState = false
        }
        return true
    }

    /// Retires any managed and/or suggested representation for a producer
    /// window with one ordered UI invalidation event.
    @discardableResult
    private func retireAllNonRevertedWindowStateIfPresent(
        windowId: String
    ) -> Bool {
        invalidatePendingProducerMutation(windowId: windowId)
        let removedSuggestion =
            suggestWindows.removeValue(forKey: windowId) != nil
        let removedManaged =
            removeNonRevertedManagedWindowIfPresent(windowId: windowId)
        guard removedSuggestion || removedManaged else { return false }
        emitBannerRetirement(windowId: windowId)
        return true
    }

    /// Retires only representations of one exact producer revision. Suppressed
    /// rows are revision-terminal, so a late replay of an older suppression
    /// must not tear down genuinely new same-ID material.
    @discardableResult
    private func retireNonRevertedWindowStateIfMatching(
        _ expectedRevision: AdWindow
    ) -> Bool {
        let windowId = expectedRevision.id
        let removedSuggestion: Bool
        if let suggested = suggestWindows[windowId],
           AdWindowMaterialIdentity.sameProducerRevision(
               suggested,
               expectedRevision
           ) {
            suggestWindows.removeValue(forKey: windowId)
            removedSuggestion = true
        } else {
            removedSuggestion = false
        }

        let removedManaged: Bool
        if let managed = windows[windowId],
           AdWindowMaterialIdentity.sameProducerRevision(
               managed.adWindow,
               expectedRevision
           ) {
            removedManaged = removeNonRevertedManagedWindowIfPresent(
                windowId: windowId
            )
        } else {
            removedManaged = false
        }

        guard removedSuggestion || removedManaged else { return false }
        emitBannerRetirement(windowId: windowId)
        return true
    }

    /// Retires only the managed representation before routing the newest
    /// producer state into the suggest tier.
    @discardableResult
    private func retireManagedWindowIfPresent(
        windowId: String
    ) -> Bool {
        invalidatePendingProducerMutation(windowId: windowId)
        guard removeNonRevertedManagedWindowIfPresent(
            windowId: windowId
        ) else {
            return false
        }
        emitBannerRetirement(windowId: windowId)
        return true
    }

    /// Marks a suggestion as actually presented only after the host queue
    /// accepts the streamed item. `AsyncStream.yield(.enqueued)` proves only
    /// that the item entered the stream buffer; a canceled observer or stale
    /// episode generation may still reject it before any UI can answer.
    func acknowledgeSuggestedBannerDelivery(
        windowId: String,
        episodeId: String?,
        playbackLifecycleGeneration: UInt64?,
        suggestionRevisionToken expectedRevisionToken: String? = nil
    ) {
        guard activeEpisodeId == episodeId,
              activePlaybackLifecycleGeneration
                == playbackLifecycleGeneration,
              suggestWindows[windowId] != nil,
              let currentRevisionToken =
                suggestRevisionTokensByWindowId[windowId],
              expectedRevisionToken == nil
                || currentRevisionToken == expectedRevisionToken
        else {
            return
        }
        acknowledgedSuggestRevisionTokens.insert(currentRevisionToken)
        suggestBanneredWindowIds.insert(windowId)
    }

    /// playhead-8cjo: the AUTO tier's twin of the seam above, and the ONLY
    /// thing that turns a missed-skip receipt into a card.
    ///
    /// THE ASYMMETRY THIS CLOSES. The suggest tier has been able to tell
    /// "subscribed" from "presented" since its acknowledgement landed —
    /// `observeBanners` calls it only `if didAccept`, so its delivery gate is
    /// driven by the QUEUE. The auto tier had no seam at all, so
    /// `emitBannerItem` had to guess from the continuation dictionaries, and
    /// `AdBannerQueue.enqueue` binning the item afterwards left a skip with
    /// neither a card the listener saw nor a row they could correct.
    ///
    /// WHY IT ONLY EVER SUBTRACTS. A receipt is written for every announced
    /// skip, so this call has nothing to create and cannot invent a card: the
    /// `missedAutoSkipReceiptsByWindowId` lookup below is what makes
    /// `delivered ⊆ announced` true by construction. An acknowledgement for a
    /// window nobody announced, or for one already acknowledged, is a no-op.
    ///
    /// THE PRECONDITIONS ARE THE SUGGEST SEAM'S, plus the auto tier's own
    /// identity. Episode and playback generation reject an acknowledgement
    /// buffered across a transition; the material token rejects one for a
    /// DIFFERENT emission of the same window id — it is compared against the
    /// token on the receipt rather than against the window's current material,
    /// because the question is "did the host take the card I announced", not
    /// "is that card still current". A refusal leaves the row exactly where it
    /// was, which is the conservative direction: the listener keeps a
    /// correction they may not need, rather than losing one they do.
    func acknowledgeAutoSkippedBannerDelivery(
        windowId: String,
        episodeId: String?,
        playbackLifecycleGeneration: UInt64?,
        windowMaterialRevisionToken: String?
    ) {
        guard activeEpisodeId == episodeId,
              activePlaybackLifecycleGeneration
                == playbackLifecycleGeneration,
              let receipt = missedAutoSkipReceiptsByWindowId[windowId],
              receipt.item.windowMaterialRevisionToken
                == windowMaterialRevisionToken
        else {
            return
        }
        missedAutoSkipReceiptsByWindowId.removeValue(forKey: windowId)
        deliveredAutoSkipCardWindowIds.insert(windowId)
    }

    /// The catalog entries a banner for `[start, end]` may carry as evidence,
    /// each located on the mention that window can actually hear.
    /// Returns an empty array when no catalog is available or none are in range.
    ///
    /// playhead-rty3. This used to filter on
    /// `entry.coverageStartTime <= end && entry.coverageEndTime >= start` —
    /// the HULL, `firstTime`/`lastTime`, which brackets the first and last
    /// mention of a deduped (category, text) pair. A sponsor URL read twice in
    /// an episode therefore had a "coverage" span covering most of it and
    /// overlapped EVERY window. Both callers put the result on a card the
    /// listener reads (`AdSkipBannerItem.evidenceCatalogEntries` →
    /// `AdBannerView.evidenceLines`), so a mid-roll card could name a distant
    /// advertiser's URL as the reason — and on the SUGGEST card that is the
    /// question whose answer is banked, so a wrong card does not merely
    /// misinform, it teaches.
    ///
    /// MEASURED over the population this function actually serves —
    /// `BannerEvidenceWindowCorpusEvalTests` on the 2026-08-02 device pull, 31
    /// assets, 115 persisted `ad_windows` rows, 244 catalog entries of which 57
    /// are repeats. **28 of the 115 windows carried at least one entry no
    /// mention of which was inside the window, and 27 of those 28 rendered it
    /// as the card's FIRST line.** The nearest mention of a removed entry lies
    /// 7 s to 3,140 s outside its window (median 542 s), so it is not an edge
    /// effect; the widest hull in the corpus is 7,268 s of a 7,326 s episode.
    /// Twenty of the 28 cards had NO other evidence — their whole "why" was
    /// someone else's sponsor — and seven more were being CROWDED OUT, because
    /// `evidenceLines` caps at three and the wrong entry took a slot.
    ///
    /// (The bead's own figure, "ten of twenty-seven repeated ANCHORING entries
    /// span more than 300 s", names a different population: the four anchoring
    /// categories the projector uses. The banner renders all five, so it is the
    /// number above that bounds this function.)
    ///
    /// `revertNegativeAttribution` below has carried this argument in a comment
    /// since playhead-1mq1 and acted on it alone; ``EvidenceEntry/locatedInTimeWindow(start:end:)``
    /// is now where it lives, shared with the FM prompt selector playhead-ad9n
    /// built. The closed-interval property is unchanged and moved with it.
    ///
    /// Unrepeated entries are unaffected in both membership and content: with a
    /// single occurrence the hull IS that occurrence, and the selector returns
    /// `self` rather than a rebuilt copy.
    private func catalogEntries(overlapping start: Double, end: Double) -> [EvidenceEntry] {
        guard let catalog = activeEvidenceCatalog else { return [] }
        return catalog.entries.compactMap { entry in
            entry.locatedInTimeWindow(start: start, end: end)
        }
    }

    /// playhead-1mq1.2.1: strong ad evidence localized INSIDE a span the user
    /// is reverting, resolved into the partition that decides what the revert
    /// is allowed to teach. See `RevertEvidencePartition` for the rule.
    ///
    /// SUSPENSION CONTRACT — every caller must resolve this in the SYNCHRONOUS
    /// prefix of its gesture, before the first `await`, and pass the resulting
    /// partition down. It reads live actor state (`activeEvidenceCatalog`,
    /// `windows`, `suggestWindows`), so resolving it after the store hop would
    /// let a replacement episode's evidence decide what the previous episode's
    /// correction may teach — the defect class playhead-i08e and
    /// playhead-o4qr closed on the identity fields.
    ///
    /// `excluded` is the set of window IDs this gesture is itself reverting.
    /// They must not vouch for themselves: "this window says it is an ad" is
    /// precisely the claim the user just contradicted.
    private func revertNegativeAttribution(
        span: RevertEvidencePartition.Interval,
        analysisAssetId: String,
        excludingWindowIds excluded: Set<String>
    ) -> RevertEvidencePartition.Partition {
        var evidence: [RevertEvidencePartition.Interval] = []

        // OCCURRENCES, deliberately not `coverageStartTime`/`coverageEndTime`.
        // Those bracket the FIRST and LAST occurrence of a deduped
        // (category, text) pair, so a sponsor URL read twice in an episode has
        // a "coverage" span covering most of it. Clipping that hull into the
        // reverted window would mark editorial content as ad evidence and —
        // the dangerous direction — push the evidence-free remainder INTO the
        // real ad, attributing a negative to exactly the audio this guard
        // exists to protect. What the entry actually observed is its
        // representative occurrence plus the two endpoint timestamps.
        if let catalog = activeEvidenceCatalog,
           catalog.analysisAssetId == analysisAssetId {
            for entry in catalog.entries
            where RevertEvidencePartition.strongLexicalCategories
                .contains(entry.category) {
                evidence.append(
                    RevertEvidencePartition.Interval(
                        startTime: min(entry.startTime, entry.endTime),
                        endTime: max(entry.startTime, entry.endTime)
                    )
                )
                guard entry.count > 1 else { continue }
                evidence.append(
                    RevertEvidencePartition.Interval(
                        startTime: entry.firstTime,
                        endTime: entry.firstTime
                    )
                )
                evidence.append(
                    RevertEvidencePartition.Interval(
                        startTime: entry.lastTime,
                        endTime: entry.lastTime
                    )
                )
            }
        }

        // A DIFFERENT live ad window overlapping part of the reverted span is
        // subspan-scoped detection evidence: it was produced independently of
        // the material under correction, so admitting it is not the circular
        // reading excluded above.
        for (id, managed) in windows
        where !excluded.contains(id)
            && managed.adWindow.analysisAssetId == analysisAssetId
            && managed.decisionState != .reverted
            && managed.decisionState != .suppressed
            && managed.snappedStart < span.endTime
            && managed.snappedEnd > span.startTime {
            evidence.append(
                RevertEvidencePartition.Interval(
                    startTime: managed.snappedStart,
                    endTime: managed.snappedEnd
                )
            )
        }
        for (id, suggested) in suggestWindows
        where !excluded.contains(id)
            && suggested.analysisAssetId == analysisAssetId
            && suggested.startTime < span.endTime
            && suggested.endTime > span.startTime {
            evidence.append(
                RevertEvidencePartition.Interval(
                    startTime: suggested.startTime,
                    endTime: suggested.endTime
                )
            )
        }

        return RevertEvidencePartition.resolve(
            span: span,
            adEvidence: evidence
        )
    }

    /// Convenience for the single-window revert seams.
    ///
    /// The span is the PRODUCER row's own bounds, deliberately not the snapped
    /// display bounds: `revokeRecurrenceEvidence` fetches its feature windows
    /// over exactly this range, so partitioning over the same range keeps the
    /// attribution filter an exact identity whenever the partition is CLEAN.
    private func revertNegativeAttribution(
        for window: AdWindow
    ) -> RevertEvidencePartition.Partition {
        revertNegativeAttribution(
            span: RevertEvidencePartition.Interval(
                startTime: window.startTime,
                endTime: window.endTime
            ),
            analysisAssetId: window.analysisAssetId,
            excludingWindowIds: [window.id]
        )
    }

    /// Broadcast the current set of applied segments to all listeners.
    private func broadcastAppliedSegments() {
        let applied = windows.values
            .filter { $0.decisionState == .applied || $0.decisionState == .confirmed }
            .sorted { $0.snappedStart < $1.snappedStart }
            .map { (start: $0.snappedStart, end: $0.snappedEnd) }
        for (_, continuation) in segmentContinuations {
            continuation.yield(applied)
        }
    }

    // MARK: - Episode Lifecycle

    /// Begin orchestration for a new episode. Clears all prior state.
    /// - Parameters:
    ///   - analysisAssetId: The analysis asset being played. Continues to
    ///     key windows, decisions, and pre-materialized cue lookups.
    ///   - episodeId: The canonical episode key (the identity unit that
    ///     `EpisodeSurfaceStatusObserver` hashes onto `ready_entered`).
    ///     Required so `auto_skip_fired.episode_id_hash` byte-matches
    ///     `ready_entered.episode_id_hash` for the same episode —
    ///     `false_ready_rate` pairs the two by that hash.
    ///   - podcastId: The podcast's ID, used to load the per-show trust mode.
    func beginEpisode(
        analysisAssetId: String,
        episodeId: String,
        podcastId: String? = nil,
        playbackLifecycleGeneration: UInt64? = nil
    ) async {
        episodeLifecycleGeneration &+= 1
        let lifecycleGeneration = episodeLifecycleGeneration
        windows.removeAll()
        latestProducerMutationByWindowId.removeAll()
        terminalProducerRevisionsByWindowId.removeAll()
        revertedProducerWindowIds.removeAll()
        pendingListenRewindWindowIds.removeAll()
        pendingCatalogLearning.removeAll()
        revokedLearningSources.removeAll()
        catalogLearningGeneration &+= 1
        let normalizedPodcastId = normalizedCatalogShowId(podcastId)
        activeAssetId = analysisAssetId
        activeEpisodeId = episodeId
        activePodcastId = normalizedPodcastId
        activePlaybackLifecycleGeneration = playbackLifecycleGeneration
        activeEvidenceCatalog = nil
        // A direct episode replacement can re-enter this actor while trust and
        // persisted-window hydration suspend below. Default the new lifecycle
        // to the non-actioning mode before the first suspension so producer
        // input can never inherit the prior show's automatic authority.
        activeSkipMode = .shadow
        // playhead-djl0: the cause is cleared with the mode. Leaving the prior
        // episode's resolution in place would make the pill describe a show
        // that is no longer playing.
        activeSkipModeResolution = .noActiveEpisode
        // playhead-gard: the per-detector map is cleared with it. An empty map
        // falls back to `activeSkipMode`, which is `.shadow` here — so the
        // pre-suspension default stays non-actioning for every class,
        // including the show-trust-exempt one.
        activeDetectorSkipModes = .noActiveEpisode
        // playhead-xr3t: clear per-episode inventory-filter context.
        // Episode duration is rehydrated from the persisted asset row
        // immediately below; declared chapters arrive later via
        // `setDeclaredChapters(_:)` from the metadata fetch in
        // `AdDetectionService.runBackfill`.
        activeEpisodeDuration = nil
        activeDeclaredChapters = []
        inAdState = false
        lastSeekTime = nil
        skipSuppressedAfterSeek = false
        currentPlayheadTime = 0
        hasObservedPlayheadThisEpisode = false
        latestUserSeekOperationGeneration = 0
        decisionLog.removeAll()
        banneredWindowIds.removeAll()
        emittedAutoSkipBannerWindowIds.removeAll()
        // playhead-bwxi: an armed auto-skip banner is per-episode state, and a
        // leak here would present the previous episode's receipt against the
        // next episode's playhead.
        armedAutoSkipBannerWindowIds.removeAll()
        // playhead-2d6i: so is a MISSED receipt. A leak here would offer the
        // previous episode's uncorrected skip against the next episode's
        // windows — and `denyAutoSkippedBanner` would refuse it, leaving a row
        // whose only possible action does nothing. The `windows`-derived filter
        // in `missedAutoSkipReceipts()` would hide it, which is exactly why the
        // clear has to be here too: a leak that is invisible is still a leak,
        // and the next episode could legitimately reuse a window id.
        missedAutoSkipReceiptsByWindowId.removeAll()
        // playhead-8cjo: and so is the record of which cards a host ACCEPTED.
        // It is read only by the partition rails, so a leak here is invisible
        // in production and lethal to the one thing that can see this bead's
        // defect: the next episode's partition would credit a card this episode
        // delivered, and window ids are not unique across episodes.
        deliveredAutoSkipCardWindowIds.removeAll()
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
        armedSuggestWindowIds.removeAll()
        suggestRevisionTokensByWindowId.removeAll()
        lastSuggestRevisionByWindowId.removeAll()
        acknowledgedSuggestRevisionTokens.removeAll()
        vetoedSuggestWindowIds.removeAll()
        recentlyAcceptedSuggestIds.removeAll()
        provisionallyResolvingSuggestWindowIds.removeAll()
        bufferedSuggestProducerUpdates.removeAll()
        // playhead-98co: per-episode edge-padding state.
        edgeAnchorsByWindowId.removeAll()
        userInitiatedSkipWindowIds.removeAll()
        // playhead-isp5: per-episode ingest stamps. The per-cause COUNTS
        // deliberately survive — they are a process-lifetime tally, matching
        // djl0's `skipModeResolutionFailureCounts`.
        lastIngestOutcomeByWindowId.removeAll()
        // A direct episode switch does not call `endEpisode`. Publish the
        // cleared state synchronously so the transport and UI cannot retain
        // the prior episode's skip cues or segment markers while hydration
        // for the replacement episode is in flight.
        pushSkipCues()
        broadcastAppliedSegments()
        // playhead-usn1: the cleared pair is published too. A subscriber that
        // attached during the PREVIOUS episode must not keep rendering that
        // show's mode across the suspensions the lookup below is about to take.
        publishSkipMode()

        #if DEBUG
        await beginEpisodeHydrationBarrierForTesting?()
        guard episodeLifecycleGeneration == lifecycleGeneration else {
            return
        }
        #endif

        // Load per-show trust mode.
        //
        // playhead-djl0. This used to be a two-arm `if`, whose `else` swallowed
        // two different failures into `.shadow` with no log, no counter and no
        // user-visible difference from a show that is deliberately in shadow.
        // It is now three steps, each of which names what it found:
        //
        //   1. RECOVER the show identity when the caller had none. The caller's
        //      value comes from a single nullable in-memory hop; the durable
        //      job row is a second, lagging chance at the same answer.
        //   2. ASK the trust service, and keep the CAUSE it returns — three of
        //      its four exits are `.shadow` and only one of them is a verdict.
        //   3. RECORD any failure: count it, and write a coded line to the
        //      diagnostics session file the device pull reads.
        //
        // Behaviour for a resolvable show is unchanged, and every failure still
        // lands on `.shadow` — the non-actioning default remains the safe one.
        var resolvedShowId = normalizedPodcastId
        if resolvedShowId == nil {
            resolvedShowId = await recoverShowIdentity(episodeId: episodeId)
            guard episodeLifecycleGeneration == lifecycleGeneration else {
                return
            }
            if let resolvedShowId {
                activePodcastId = resolvedShowId
                logger.info(
                    "beginEpisode: show identity recovered from the durable job row for \(episodeId, privacy: .public)"
                )
            }
        }

        if let podcastId = resolvedShowId, let trustService {
            // playhead-gard: one lookup, both answers. `resolveDetectorModes`
            // returns the show mode (unchanged, for the pill and for an older
            // binary) alongside each detector class's own verdict.
            let resolved = await trustService.resolveDetectorModes(
                podcastId: podcastId
            )
            guard episodeLifecycleGeneration == lifecycleGeneration else {
                return
            }
            activeSkipMode = resolved.showMode
            activeDetectorSkipModes = resolved
            noteSkipModeResolution(resolved.resolution, episodeId: episodeId)
        } else if resolvedShowId != nil {
            activeSkipMode = .shadow
            activeDetectorSkipModes = .noActiveEpisode
            noteSkipModeResolution(.trustServiceUnavailable, episodeId: episodeId)
        } else {
            activeSkipMode = .shadow
            activeDetectorSkipModes = .noActiveEpisode
            noteSkipModeResolution(.unresolvedShowIdentity, episodeId: episodeId)
        }
        // playhead-usn1: the verdict reaches the surface HERE, not on the next
        // pull. This is the emission the Now Playing pill was missing.
        publishSkipMode()

        // playhead-xr3t: hydrate the inventory filter's episode duration
        // from the persisted asset row. Best-effort: an absent row /
        // absent duration leaves `activeEpisodeDuration = nil`, and the
        // filter degrades to "head-edge guard only" (the safer failure
        // mode — under-filter rather than mis-reject on unknown
        // duration). The duration is refreshable mid-episode via
        // `setEpisodeDuration(_:)` once `AnalysisCoordinator`'s
        // duration-backfill probe rewrites the row.
        do {
            let asset = try await store.fetchAsset(id: analysisAssetId)
            guard episodeLifecycleGeneration == lifecycleGeneration else {
                return
            }
            if let asset,
               let duration = asset.episodeDurationSec,
               duration > 0,
               duration.isFinite {
                activeEpisodeDuration = duration
            }
        } catch {
            logger.debug(
                "beginEpisode: episode-duration lookup failed for \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }

        // Bug 5 (skip-cues-deletion): the cross-launch preload — read
        // `ad_windows` directly and forward the admissible rows into
        // `receiveAdWindows`. The mechanism, the admission rule and why
        // `.applied` rows are pre-bannered all live on
        // `forwardPersistedAdWindows` below, which playhead-96ot's
        // mid-session ingest shares verbatim.
        _ = await forwardPersistedAdWindows(
            analysisAssetId: analysisAssetId,
            lifecycleGeneration: lifecycleGeneration,
            door: .crossLaunchPreload
        )
        guard episodeLifecycleGeneration == lifecycleGeneration else {
            return
        }

        logger.info("Begin episode: asset=\(analysisAssetId)")
    }

    /// playhead-djl0: second chance at the show's identity.
    ///
    /// `beginEpisode`'s `podcastId` argument comes from one nullable in-memory
    /// hop (`episode.podcast?.feedURL.absoluteString`, read on the MainActor in
    /// `PlayheadRuntime.performPlayEpisode`). When that hop yields nothing the
    /// `analysis_jobs` row for the episode may still know the answer.
    ///
    /// The recovered value goes through the SAME canonicalization the caller's
    /// value does. A stored identity in non-canonical spelling is not an
    /// identity we may key show-scoped evidence on, and admitting one here
    /// would retarget recurrence learning into a neighbouring namespace —
    /// strictly worse than resolving nothing.
    private func recoverShowIdentity(episodeId: String) async -> String? {
        do {
            let recorded = try await store.fetchRecordedPodcastId(
                forEpisodeId: episodeId
            )
            return normalizedCatalogShowId(recorded)
        } catch {
            logger.debug(
                "beginEpisode: show-identity recovery failed for \(episodeId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// playhead-djl0: install a resolution, and — when it is a failure — count
    /// it and leave a durable trace.
    ///
    /// The trace goes to `SurfaceStatusInvariantLogger`, the JSON Lines session
    /// file that already ships in the diagnostics bundle and that the device
    /// pull reads. That is deliberate: the field investigation this bead came
    /// from went looking for exactly such a line and found that the failure
    /// branch logged nothing at all.
    private func noteSkipModeResolution(
        _ resolution: SkipModeResolution,
        episodeId: String
    ) {
        activeSkipModeResolution = resolution
        guard resolution.isLookupFailure else { return }
        skipModeResolutionFailureCounts[resolution, default: 0] += 1

        let episodeIdHash = episodeIdHasher(episodeId)
        let code: InvariantViolation.Code = resolution.hasResolvedShowIdentity
            ? .skipModeTrustLookupFailed
            : .skipModeShowIdentityUnresolved
        invariantLogger.invariantViolated(
            code: code,
            description: """
                skip mode fell back to \(activeSkipMode.rawValue) \
                because \(resolution.rawValue) (episode \(episodeIdHash))
                """
        )
        logger.error(
            "beginEpisode: skip mode is \(self.activeSkipMode.rawValue, privacy: .public) because \(resolution.rawValue, privacy: .public) — this is NOT a deliberate shadow"
        )
    }

    // MARK: - playhead-isp5: the ingest audit trail

    /// Stamp one window's terminal disposition: bump the process counter and
    /// remember it so the door that forwarded the row can read it back.
    ///
    /// Deliberately called at EVERY terminal branch of `receiveAdWindows`,
    /// including the successful ones. A taxonomy that names only the failures
    /// cannot distinguish "dropped for a reason we forgot to name" from
    /// "delivered", which is exactly the ambiguity that kept this bead open.
    private func noteIngestOutcome(
        _ outcome: AdWindowIngestOutcome,
        windowId: String,
        detail: String? = nil
    ) {
        adWindowIngestOutcomeCounts[outcome, default: 0] += 1
        lastIngestOutcomeByWindowId[windowId] = (outcome, detail)
    }

    /// Record a whole-call outcome for a delivery that never reached
    /// `receiveAdWindows`, and write its audit row.
    ///
    /// - Parameter durable: pass `false` for the ONE outcome that has nothing
    ///   to account for — a preload whose store read returned no rows at all.
    ///   An episode with no persisted windows cannot have lost one, so the row
    ///   would carry no information, and `beginEpisode` runs on every episode
    ///   start: writing there would bootstrap a diagnostics session file for
    ///   every episode ever opened (and, in the test suite, for every
    ///   orchestrator ever constructed) to say nothing. The COUNT is still
    ///   kept — it is free, and it keeps the counter API's partition complete.
    private func noteIngestDoorOutcome(
        _ outcome: AdWindowIngestOutcome,
        door: AdWindowIngestDoor,
        analysisAssetId: String,
        detail: String? = nil,
        durable: Bool = true
    ) {
        adWindowIngestOutcomeCounts[outcome, default: 0] += 1
        guard durable else { return }
        recordIngestCensus(
            AdWindowIngestCensus(
                door: door,
                analysisAssetId: analysisAssetId,
                forwarded: 0,
                counts: [outcome: 1],
                details: detail.map { ["\(outcome.rawValue):\($0)": 1] } ?? [:]
            )
        )
    }

    /// Read back where each of `windowIds` ended up and write the delivery's
    /// audit row.
    ///
    /// An id with no stamp is counted as ``AdWindowIngestOutcome/droppedNoActiveEpisode``
    /// only when that is what happened; any other gap would be a missing
    /// instrumentation site, so it is left OUT of the counts and shows up as
    /// `forwarded > sum(counts)` in the row — visible, rather than silently
    /// attributed to a cause that did not occur.
    ///
    /// playhead-9v09, stated because it is a KNOWN and deliberately unclosed
    /// interleaving: `receiveAdWindows` suspends on the catalog actor, and this
    /// actor is reentrant, so a `setEpisodeDuration` arriving inside that
    /// suspension can run the retroactive sweep and re-stamp a row this
    /// delivery already armed. The delivery's row then reads
    /// `ingest_retired_reapplied_inventory_filter` where it would have read
    /// `ingest_armed_suggest`, and the sweep writes its own row as well. That
    /// is over-attribution, not silence — the outcome is named in both rows and
    /// the process COUNTER is bumped exactly once, by the sweep — so it is
    /// left visible rather than engineered around.
    private func recordIngestCensus(
        door: AdWindowIngestDoor,
        analysisAssetId: String,
        forwardedWindowIds: [String]
    ) {
        var counts: [AdWindowIngestOutcome: Int] = [:]
        var details: [String: Int] = [:]
        for id in forwardedWindowIds {
            guard let stamp = lastIngestOutcomeByWindowId[id] else { continue }
            counts[stamp.outcome, default: 0] += 1
            if let detail = stamp.detail {
                details["\(stamp.outcome.rawValue):\(detail)", default: 0] += 1
            }
        }
        recordIngestCensus(
            AdWindowIngestCensus(
                door: door,
                analysisAssetId: analysisAssetId,
                forwarded: forwardedWindowIds.count,
                counts: counts,
                details: details
            )
        )
    }

    /// The single write site. The row goes to the JSON Lines session file the
    /// diagnostics bundle ships (playhead-v7q6: the durable row IS the audit
    /// trail), and a mirrored `os_log` line stays for live Console debugging —
    /// the log is the convenience, the row is the evidence.
    private func recordIngestCensus(_ census: AdWindowIngestCensus) {
        invariantLogger.invariantViolated(
            code: .adWindowIngestCensus,
            description: census.auditDescription
        )
        logger.info(
            "ad window ingest: \(census.auditDescription, privacy: .public)"
        )
    }

    /// playhead-isp5: how many windows have hit `outcome` this process.
    func adWindowIngestOutcomeCount(_ outcome: AdWindowIngestOutcome) -> Int {
        adWindowIngestOutcomeCounts[outcome] ?? 0
    }

    /// playhead-zxqj: the single write site for a "Dismiss ad" audit row.
    ///
    /// Written on EVERY exit of `revertByTimeRange`, committed or refused, on
    /// the `recordIngestCensus` precedent and for the identical reason: the
    /// gesture's own side effects cannot distinguish "was refused" from "was
    /// never made", so only a line that is always present can. The row goes to
    /// the JSON Lines session file the diagnostics bundle ships; the `os_log`
    /// mirror is the convenience.
    private func noteManualVetoOutcome(
        _ outcome: ManualVetoOutcome,
        analysisAssetId: String,
        start: Double,
        end: Double,
        revertedWindows: Int = 0,
        liveTargets: Int = 0
    ) {
        manualVetoOutcomeCounts[outcome, default: 0] += 1
        let audit = ManualVetoOutcomeAudit(
            outcome: outcome,
            analysisAssetId: analysisAssetId,
            startTime: start,
            endTime: end,
            revertedWindows: revertedWindows,
            liveTargets: liveTargets
        )
        invariantLogger.invariantViolated(
            code: .manualVetoOutcome,
            description: audit.auditDescription
        )
        logger.info(
            "manual veto: \(audit.auditDescription, privacy: .public)"
        )
    }

    /// playhead-zxqj: how many dismiss gestures have reached `outcome` this
    /// process. The in-process counterpart of the audit row, for tests and for
    /// anyone reading live state rather than a pull.
    func manualVetoOutcomeCount(_ outcome: ManualVetoOutcome) -> Int {
        manualVetoOutcomeCounts[outcome] ?? 0
    }

    /// playhead-isp5: the terminal disposition the most recent delivery
    /// recorded for `windowId`, and its sub-cause where one exists. `nil` once
    /// the episode ends, or for an id no delivery has stamped.
    ///
    /// playhead-9v09: a retroactive retirement OVERWRITES the delivery's stamp,
    /// so an id that was armed and then swept back reads
    /// `.retiredReapplyInventoryFilter` here, not `.armedSuggest`. That is the
    /// answer the "where did window X go?" question wants; the fact that it was
    /// armed first is not lost, it is on the delivery's census row.
    func lastAdWindowIngestOutcome(
        forWindowId windowId: String
    ) -> (outcome: AdWindowIngestOutcome, detail: String?)? {
        lastIngestOutcomeByWindowId[windowId]
    }

    /// The rows `beginEpisode`'s cross-launch preload and the playhead-96ot
    /// mid-session ingest both admit.
    ///
    /// playhead-atr3: the floor reads `actuationConfidence` — detection AFTER
    /// calibration and after the user-correction factor — not raw `confidence`.
    /// Dan's principle: *the highest quality signal we have is a user
    /// correction*, so a gate that reads the raw number where a corrected one
    /// exists is discarding the best evidence it has.
    ///
    /// This is a RESTORATION, not a widening in principle. Before ar60's V47
    /// column split the fusion path wrote its actuation number INTO
    /// `confidence`, so this floor already compared 0.7 against actuation; the
    /// split silently switched it to detection, which for a corrected or
    /// calibration-discounted span is the higher of the two. Reading actuation
    /// is what the code did before, now made explicit and deliberate.
    ///
    /// WHAT MAKES IT SAFE — the scope, not a second threshold. ar60's own
    /// measurement was that five spans crossed this 0.7 floor on inflation from
    /// marks that did NOT overlap them, because the correction factor was one
    /// asset-wide scalar handed to every span. That was one good signal smeared
    /// onto the wrong spans, not a noisy correction. `CorrectionFactorSnapshot`
    /// now evaluates per span, so a reinforcement can only lift a span it
    /// actually overlaps. Fix the scope, then trust the number — deliberately
    /// NO second numeric bound on the boost (Dan considered and declined one).
    /// The rail that holds the scoping honest is
    /// `SkipOrchestratorPreloadActuationFloorTests`.
    ///
    /// playhead-ynmk: a user-asserted row bypasses the floor entirely, because
    /// the floor is a claim about DETECTOR quality and a user assertion is not
    /// a detector claim. Before that bead both user gestures wrote
    /// `confidence = 1.0`, so the floor never excluded them; now that a banner
    /// confirmation carries the MEASURED value (0.40 in the field case), a row
    /// the user answered Yes to would silently drop out of cross-launch
    /// continuity because the detector was unsure — losing exactly the anchored
    /// population qs0d wants to keep skipping. That bypass survives atr3
    /// unchanged: it is the same principle one layer up (the user's own
    /// gesture outranks any number the detector produced), and switching the
    /// numeric read must not quietly re-gate the asserted population.
    ///
    /// Admitting the row is not admitting the skip: `paddedCueSpan` re-derives
    /// the extent on every push, so an unanchored asserted row still cues
    /// nothing, and `evaluateWindow` applies the enter threshold to the same
    /// actuation quantity afterwards.
    private static func preloadAdmissibleWindows(_ rows: [AdWindow]) -> [AdWindow] {
        rows.filter {
            ($0.actuationConfidence >= preloadConfidenceThreshold
                || $0.userAssertion != nil)
                && $0.endTime > $0.startTime
                && preloadEligibleDecisionStates.contains($0.decisionState)
        }
    }

    /// Read `ad_windows` for `analysisAssetId` and forward the admissible rows
    /// through `receiveAdWindows`. Returns how many rows were forwarded — 0 for
    /// a store failure, an empty admissible set, or a lifecycle replacement
    /// under the store read.
    ///
    /// ONE implementation, two callers. `beginEpisode` reaches it for the
    /// cross-launch preload and `ingestPersistedAdWindows` for the playhead-96ot
    /// mid-session delivery, so the admission rule cannot drift between the two
    /// doors — a divergence here would be a silent veto bypass on one of them.
    ///
    /// Bug 5 (skip-cues-deletion): the rows are read directly from `ad_windows`,
    /// which replaced the now-deleted `skip_cues` table. The 0.7 threshold in
    /// `preloadAdmissibleWindows` takes its VALUE from the cue materializer's
    /// threshold, but it is applied to `actuationConfidence` (playhead-atr3),
    /// so the preload set is no longer claimed to be byte-identical to what the
    /// cue table held: a span the user has corrected is admitted or excluded on
    /// the corrected number, which is the whole point. The persisted
    /// `AdWindow` rows go to
    /// `receiveAdWindows` UNMODIFIED so the existing event-stream path applies
    /// its standard logic — eligibilityGate, banner state, decision-log dedup —
    /// and so any auto-skip / markOnly precision gate stamped at write time
    /// survives, which a synthesized "confirmed" shape would silently strip.
    ///
    /// Cycle-21 H-1: the filter excludes only `.suppressed` (terminal "no-skip"
    /// — replay wastes memory) and `.reverted` (the user explicitly chose
    /// "Listen" — replay would risk pushing a cue they rejected). `.candidate`
    /// and `.confirmed` are forwarded so the orchestrator can re-evaluate them.
    /// `.applied` is forwarded so a previously-skipped ad pushes its cue again
    /// on the next app launch — `evaluateAndPush`'s terminal-state branch (the
    /// `decisionState == .applied` arm) appends the row to `eligible` and
    /// `pushMergedCues` fires the skip cue. Without that forwarding,
    /// cross-launch auto-skip would silently regress: pre-pivot the `skip_cues`
    /// table re-cued every confidence-passing row at episode start; now the
    /// `ad_windows` rows must do the same job.
    ///
    /// Banner re-emission for the forwarded `.applied` rows is suppressed by
    /// pre-populating `banneredWindowIds` BEFORE the `receiveAdWindows` call.
    /// The terminal-state branch in `evaluateAndPush` only emits a banner when
    /// `!banneredWindowIds.contains(id)`; pre-populating the set turns the
    /// banner emission off for an already-skipped ad without affecting the cue
    /// push (the cue push happens via `eligible.append` regardless of the banner
    /// gate). Mid-session that pre-population is a no-op in the common case — an
    /// ad applied in THIS session is already in the set — and it is the right
    /// behaviour in the uncommon one: a durably-`.applied` row is an ad that was
    /// already skipped, so re-announcing it would be stale.
    @discardableResult
    private func forwardPersistedAdWindows(
        analysisAssetId: String,
        lifecycleGeneration: UInt64,
        door: AdWindowIngestDoor
    ) async -> Int {
        let preWindows: [AdWindow]
        do {
            preWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
        } catch {
            logger.warning("Failed to load preload ad_windows: \(error.localizedDescription)")
            noteIngestDoorOutcome(
                .doorDroppedStoreReadFailed,
                door: door,
                analysisAssetId: analysisAssetId
            )
            return 0
        }
        guard episodeLifecycleGeneration == lifecycleGeneration else {
            noteIngestDoorOutcome(
                .doorDroppedEpisodeReplaced,
                door: door,
                analysisAssetId: analysisAssetId
            )
            return 0
        }
        let eligible = Self.preloadAdmissibleWindows(preWindows)
        guard !eligible.isEmpty else {
            noteIngestDoorOutcome(
                .doorDroppedNoAdmissibleRows,
                door: door,
                analysisAssetId: analysisAssetId,
                detail: "read=\(preWindows.count)",
                // "the store had nothing" is not news; "the store had rows and
                // the admission rule took all of them" is exactly the news this
                // audit exists for.
                durable: !preWindows.isEmpty
            )
            return 0
        }
        let appliedRawValue = SkipDecisionState.applied.rawValue
        for window in eligible where window.decisionState == appliedRawValue {
            // Cycle-27 T-3 production-writer site (1 of 4): preload pre-population.
            banneredWindowIds.insert(window.id)
        }
        // playhead-isp5: the ids are captured BEFORE the call so the census
        // reads back exactly what THIS door forwarded, and cannot pick up rows
        // an interleaved producer pushed while `receiveAdWindows` was suspended
        // on the catalog actor.
        let forwardedWindowIds = eligible.map(\.id)
        await receiveAdWindows(eligible)
        recordIngestCensus(
            door: door,
            analysisAssetId: analysisAssetId,
            forwardedWindowIds: forwardedWindowIds
        )
        return eligible.count
    }

    /// playhead-96ot: re-read `ad_windows` for `analysisAssetId` and deliver
    /// them to the LIVE session, through the same door `beginEpisode`'s
    /// cross-launch preload uses. Returns how many rows were forwarded.
    ///
    /// WHY THIS EXISTS. `AdDetectionService.mintByteExactDayZeroMarks` persists
    /// its windows and returns; until this method, nothing handed them to the
    /// orchestrator until the NEXT `beginEpisode`. Day-0 rediff fires ~19 s into
    /// a FIRST listen — exactly when the ads it found are still AHEAD of the
    /// playhead — and playhead-qs0d had just made those windows genuinely
    /// auto-skippable. The user had to re-open the episode to get the skip the
    /// mechanism had already earned.
    ///
    /// NO-OP UNLESS `analysisAssetId` IS PLAYING. The check is this actor's
    /// `activeAssetId`, read inside the actor, so there is no window between
    /// "is it current?" and "ingest it" for an episode switch to slip through.
    /// A mint for some other episode is not lost — the rows are durable and the
    /// next `beginEpisode` preloads them, which is exactly the pre-96ot path and
    /// the right one for an episode nobody is listening to.
    ///
    /// DELIBERATELY NOT AN EMITTER. Ingest ARMS suggest-tier material and stops;
    /// `updatePlayheadTime` presents it (playhead-d3g0). Emitting here would
    /// reintroduce the hazard d3g0 avoided by not emitting at registration —
    /// `currentPlayheadTime` is a stale 0 between `beginEpisode` and the first
    /// position observation, so a synchronous emit would banner every span
    /// containing 0:00 regardless of where the listener actually is.
    @discardableResult
    func ingestPersistedAdWindows(analysisAssetId: String) async -> Int {
        guard activeAssetId == analysisAssetId else {
            logger.debug(
                "ingestPersistedAdWindows: dropping mismatched asset \(analysisAssetId, privacy: .public) (active=\(self.activeAssetId ?? "nil", privacy: .public))"
            )
            // playhead-isp5: this branch is why the bead stayed open. An
            // `os_log` line is not evidence a device pull can retrieve, so
            // "the ingest dropped it" and "the ingest never fired" were
            // indistinguishable after the fact. Now it is a durable row.
            noteIngestDoorOutcome(
                .doorDroppedNotPlaying,
                door: .midSessionIngest,
                analysisAssetId: analysisAssetId,
                detail: "active=\(activeAssetId ?? "nil")"
            )
            return 0
        }
        return await forwardPersistedAdWindows(
            analysisAssetId: analysisAssetId,
            lifecycleGeneration: episodeLifecycleGeneration,
            door: .midSessionIngest
        )
    }

    /// End orchestration for the current episode.
    func endEpisode() {
        episodeLifecycleGeneration &+= 1
        let windowCount = windows.count
        let appliedCount = windows.values.filter { $0.decisionState == .applied }.count
        logger.info("End episode: \(windowCount) windows, \(appliedCount) applied, \(self.decisionLog.count) decisions logged")

        windows.removeAll()
        latestProducerMutationByWindowId.removeAll()
        terminalProducerRevisionsByWindowId.removeAll()
        revertedProducerWindowIds.removeAll()
        pendingListenRewindWindowIds.removeAll()
        pendingCatalogLearning.removeAll()
        revokedLearningSources.removeAll()
        catalogLearningGeneration &+= 1
        activeAssetId = nil
        activeEpisodeId = nil
        activePodcastId = nil
        activePlaybackLifecycleGeneration = nil
        activeEvidenceCatalog = nil
        activeSkipMode = .shadow
        // playhead-djl0: nothing is playing, so no cause describes anything.
        // The per-cause failure COUNTS deliberately survive — they are a
        // process-lifetime tally, not per-episode state.
        activeSkipModeResolution = .noActiveEpisode
        activeDetectorSkipModes = .noActiveEpisode
        // playhead-usn1: publish the cleared pair so a mounted Now Playing
        // screen stops describing a show that is no longer playing.
        publishSkipMode()
        // playhead-xr3t: clear per-episode inventory-filter context so
        // the next episode doesn't inherit stale duration/chapters.
        activeEpisodeDuration = nil
        activeDeclaredChapters = []
        inAdState = false
        hasObservedPlayheadThisEpisode = false
        latestUserSeekOperationGeneration = 0
        banneredWindowIds.removeAll()
        emittedAutoSkipBannerWindowIds.removeAll()
        // playhead-bwxi: an armed auto-skip banner is per-episode state, and a
        // leak here would present the previous episode's receipt against the
        // next episode's playhead.
        armedAutoSkipBannerWindowIds.removeAll()
        // playhead-2d6i: so is a MISSED receipt. A leak here would offer the
        // previous episode's uncorrected skip against the next episode's
        // windows — and `denyAutoSkippedBanner` would refuse it, leaving a row
        // whose only possible action does nothing. The `windows`-derived filter
        // in `missedAutoSkipReceipts()` would hide it, which is exactly why the
        // clear has to be here too: a leak that is invisible is still a leak,
        // and the next episode could legitimately reuse a window id.
        missedAutoSkipReceiptsByWindowId.removeAll()
        // playhead-8cjo: and so is the record of which cards a host ACCEPTED.
        // It is read only by the partition rails, so a leak here is invisible
        // in production and lethal to the one thing that can see this bead's
        // defect: the next episode's partition would credit a card this episode
        // delivered, and window ids are not unique across episodes.
        deliveredAutoSkipCardWindowIds.removeAll()
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
        armedSuggestWindowIds.removeAll()
        suggestRevisionTokensByWindowId.removeAll()
        lastSuggestRevisionByWindowId.removeAll()
        acknowledgedSuggestRevisionTokens.removeAll()
        vetoedSuggestWindowIds.removeAll()
        recentlyAcceptedSuggestIds.removeAll()
        provisionallyResolvingSuggestWindowIds.removeAll()
        bufferedSuggestProducerUpdates.removeAll()
        // playhead-98co: clear per-episode edge-padding state here as well
        // as in `beginEpisode`, mirroring every sibling per-episode
        // collection above (see the endEpisode-clears regression pattern on
        // `emittedAutoSkipBannerWindowIds`) — stale anchor stamps or
        // exemption ids must not outlive the episode that produced them.
        edgeAnchorsByWindowId.removeAll()
        userInitiatedSkipWindowIds.removeAll()
        // playhead-isp5: mirrors the `beginEpisode` clear. The per-cause
        // COUNTS survive on purpose (process-lifetime tally).
        lastIngestOutcomeByWindowId.removeAll()
        pushSkipCues()
        broadcastAppliedSegments()
    }

    // MARK: - playhead-xr3t: inventory sanity context setters

    /// Update the episode duration used by the inventory sanity filter.
    /// Mirrors the `setEvidenceCatalog` pattern: late-arriving values
    /// are dropped when the active asset has switched (so a duration-
    /// backfill probe finishing mid-podcast-switch can't poison the
    /// new episode's tail-edge guard).
    ///
    /// Pass a non-positive or non-finite `duration` to clear the cached
    /// value (the filter will then degrade to head-edge-only behaviour).
    func setEpisodeDuration(_ duration: Double, analysisAssetId: String) {
        guard let activeAssetId, analysisAssetId == activeAssetId else {
            logger.debug(
                "setEpisodeDuration: dropping mismatched asset \(analysisAssetId, privacy: .public) (active=\(self.activeAssetId ?? "nil", privacy: .public))"
            )
            return
        }
        let previous = activeEpisodeDuration
        if duration > 0, duration.isFinite {
            activeEpisodeDuration = duration
        } else {
            activeEpisodeDuration = nil
        }
        // Re-evaluate managed windows against the freshest context.
        // Skip the pass when the new value is identical to what we
        // already had — nothing the filter sees would change.
        if previous != activeEpisodeDuration {
            reapplyInventoryFilterToManagedWindows()
        }
    }

    /// Push the publisher-declared content chapters for the active
    /// episode. Only creator-source ChapterEvidence (id3, pc20,
    /// rssInline) is retained — `.inferred` chapters are filtered out
    /// here as a defense-in-depth check so the filter cannot
    /// accidentally consult them, even if a future caller mixes
    /// sources.
    ///
    /// Chapters are sorted by `startTime` and any chapter missing an
    /// explicit `endTime` has it synthesized from the NEXT chapter's
    /// start time. The trailing chapter (last in the episode, no
    /// successor) is left open-ended — the filter explicitly does NOT
    /// reject overlap with an unbounded chapter, because over-rejection
    /// in xr3t means the user misses an expected ad-skip (e.g. a
    /// post-roll inside a publisher-declared "Outro" chapter).
    ///
    /// After the chapter context updates, currently-managed non-
    /// terminal windows are re-evaluated against the new filter
    /// context. Spans that the filter would now reject are removed
    /// from the active set (mirrors `retireAdWindows`). Without this
    /// pass, the `beginEpisode` preload (which runs BEFORE
    /// `AdDetectionService.runBackfill` pushes chapters) would let
    /// chapter-overlapping rows survive across launches even though
    /// the filter is supposed to reject them.
    ///
    /// Mismatched-asset pushes are dropped silently (asset-switch race
    /// guard), mirroring `setEvidenceCatalog`.
    func setDeclaredChapters(_ chapters: [ChapterEvidence], analysisAssetId: String) {
        guard let activeAssetId, analysisAssetId == activeAssetId else {
            logger.debug(
                "setDeclaredChapters: dropping mismatched asset \(analysisAssetId, privacy: .public) (active=\(self.activeAssetId ?? "nil", privacy: .public))"
            )
            return
        }
        activeDeclaredChapters = Self.normalizedDeclaredChapters(chapters)
        reapplyInventoryFilterToManagedWindows()
    }

    /// Sort, filter, and synthesize missing chapter end times from the
    /// next chapter's start. The trailing chapter (no successor) keeps
    /// `endTime == nil` and is treated as unbounded by the inventory
    /// filter (no rejection on overlap). See `setDeclaredChapters`
    /// for the rationale.
    private static func normalizedDeclaredChapters(
        _ chapters: [ChapterEvidence]
    ) -> [ChapterEvidence] {
        let creatorOnly = chapters.filter { $0.source.isCreatorSource }
        let sorted = creatorOnly.sorted { $0.startTime < $1.startTime }
        guard !sorted.isEmpty else { return [] }
        var result: [ChapterEvidence] = []
        result.reserveCapacity(sorted.count)
        for index in sorted.indices {
            let chapter = sorted[index]
            if chapter.endTime != nil {
                result.append(chapter)
                continue
            }
            // Synthesize end from the next chapter's start when it is
            // strictly greater than this chapter's start. If the next
            // chapter starts at the same time (degenerate), or the
            // current chapter is the last, leave `endTime` as nil —
            // the filter will skip rule (c) for unbounded chapters.
            let nextIndex = index + 1
            if nextIndex < sorted.count {
                let nextStart = sorted[nextIndex].startTime
                if nextStart > chapter.startTime, nextStart.isFinite {
                    let synthesized = ChapterEvidence(
                        startTime: chapter.startTime,
                        endTime: nextStart,
                        title: chapter.title,
                        source: chapter.source,
                        disposition: chapter.disposition,
                        qualityScore: chapter.qualityScore
                    )
                    result.append(synthesized)
                    continue
                }
            }
            result.append(chapter)
        }
        return result
    }

    /// Re-evaluate currently-managed and suggested windows against the inventory
    /// sanity filter using the freshest `activeEpisodeDuration` /
    /// `activeDeclaredChapters` context. Removes any window that the filter now
    /// rejects, subject to the managed-window user-already-acted guard below.
    /// Suggestions have not been acted on yet, so every rejected suggestion is
    /// retired immediately before a stale Yes can promote it.
    ///
    /// Called after `setDeclaredChapters` and `setEpisodeDuration` so
    /// preloaded windows (which entered the active set before the
    /// chapter / duration context was available) are belatedly
    /// reconciled with the filter.
    ///
    /// Safety rules:
    ///   * `.reverted` — user explicitly chose "Listen". Never touch.
    ///   * `.applied` — the orchestrator has decided to auto-skip. We
    ///     may still retire IF the playhead has not yet reached the
    ///     window's start: yanking a cue the user is past, or one
    ///     they're currently inside, would be a UX bug. The retire
    ///     is purely the rule "if the filter would have rejected this
    ///     before it ever became user-visible, do so now."
    ///   * `.candidate` / `.confirmed` / `.suppressed` — re-evaluate
    ///     freely; nothing user-visible has happened.
    ///
    /// playhead-9v09: EVERY retirement this pass performs is stamped
    /// `.retiredReapplyInventoryFilter` with the filter's rejection reason,
    /// counted for the process lifetime, and summarised into one
    /// `ad_window_ingest_census` row under
    /// `AdWindowIngestDoor.retroactiveInventorySweep`. Before that, a span the
    /// cross-launch preload armed and this pass took back left the census
    /// reading `ingest_armed_suggest = 1` with no counter-evidence anywhere —
    /// a silent retraction path in the instrument the mid-roll program is
    /// verified through, able to certify a fix that did not hold. The row is
    /// written ONLY when at least one window was actually retired: a sweep
    /// that changes nothing must say nothing, or the outcome fires on every
    /// duration update and stops being evidence.
    private func reapplyInventoryFilterToManagedWindows() {
        guard inventoryFilter.isEnabled,
              !windows.isEmpty || !suggestWindows.isEmpty
        else {
            return
        }
        // playhead-9v09: the REASON travels with the id, because the census row
        // has to carry it — `tooLate` and `overlapsDeclaredChapter` are two
        // different bugs here for exactly the reason they are two different
        // bugs on the ingest path (`droppedInventorySanity`).
        var rejectionReasonsById: [String: InventorySanityRejectionReason] = [:]
        for (id, managed) in windows {
            switch managed.decisionState {
            case .reverted:
                continue
            case .applied:
                // Only retire if the playhead has not yet reached the
                // window's start. Otherwise the user has either heard
                // it or is hearing it now — silently dropping the
                // skip cue mid-stream is worse than the false-positive
                // skip we're trying to avoid.
                guard currentPlayheadTime < managed.snappedStart else { continue }
            case .candidate, .confirmed, .suppressed:
                break
            }

            let verdict = inventoryFilter.evaluate(
                startTime: managed.adWindow.startTime,
                endTime: managed.adWindow.endTime,
                episodeDuration: activeEpisodeDuration,
                declaredChapters: activeDeclaredChapters
            )
            if case let .rejected(reason) = verdict {
                logger.info(
                    "AdWindow \(id, privacy: .public) retroactively rejected by inventory sanity filter: \(reason.rawValue, privacy: .public)"
                )
                rejectionReasonsById[id] = reason
            }
        }

        for (id, suggested) in suggestWindows {
            let verdict = inventoryFilter.evaluate(
                startTime: suggested.startTime,
                endTime: suggested.endTime,
                episodeDuration: activeEpisodeDuration,
                declaredChapters: activeDeclaredChapters
            )
            guard case let .rejected(reason) = verdict else { continue }
            logger.info(
                "Suggested AdWindow \(id, privacy: .public) retroactively rejected by inventory sanity filter: \(reason.rawValue, privacy: .public)"
            )
            // The managed verdict wins when both representations exist: it is
            // the one the `.applied` / `.reverted` guards above consulted, so
            // reporting the suggestion's reason instead could describe a
            // retirement that was decided on other grounds.
            if rejectionReasonsById[id] == nil {
                rejectionReasonsById[id] = reason
            }
        }

        guard !rejectionReasonsById.isEmpty else { return }
        // Sorted so the retirement events, and the audit row they produce, do
        // not depend on dictionary iteration order.
        var counts: [AdWindowIngestOutcome: Int] = [:]
        var details: [String: Int] = [:]
        var retiredCount = 0
        for id in rejectionReasonsById.keys.sorted() {
            // Every id here was read out of a live collection and cleared the
            // `.reverted` guard, so the removal succeeds; honouring the return
            // value anyway keeps the counter meaning "windows the listener
            // actually lost" rather than "windows we asked about".
            guard retireAllNonRevertedWindowStateIfPresent(windowId: id) else {
                continue
            }
            let reason = rejectionReasonsById[id]
            noteIngestOutcome(
                .retiredReapplyInventoryFilter,
                windowId: id,
                detail: reason?.rawValue
            )
            retiredCount += 1
            counts[.retiredReapplyInventoryFilter, default: 0] += 1
            if let reason {
                let key = "\(AdWindowIngestOutcome.retiredReapplyInventoryFilter.rawValue)"
                    + ":\(reason.rawValue)"
                details[key, default: 0] += 1
            }
        }
        if retiredCount > 0 {
            recordIngestCensus(
                AdWindowIngestCensus(
                    door: .retroactiveInventorySweep,
                    // `activeAssetId` is non-nil whenever a window is managed,
                    // but the fallback is spelled rather than assumed: a row
                    // that says "nil" is evidence, a row that never got written
                    // is the defect this bead closes.
                    analysisAssetId: activeAssetId ?? "nil",
                    forwarded: retiredCount,
                    counts: counts,
                    details: details
                )
            )
        }
        evaluateAndPush()
    }

    // MARK: - Ad Window Event Stream

    /// Receive new or updated AdWindows from AdDetectionService.
    /// This is the primary event-stream entry point. Called whenever
    /// the detection pipeline produces or updates windows.
    func receiveAdWindows(_ adWindows: [AdWindow]) async {
        guard let assetId = activeAssetId else {
            // playhead-isp5: every terminal branch below is stamped, including
            // this one — a delivery that arrived between episodes must be
            // distinguishable from one that arrived and was filtered.
            for adWindow in adWindows {
                noteIngestOutcome(.droppedNoActiveEpisode, windowId: adWindow.id)
            }
            return
        }
        let producerGeneration = nextProducerMutationGeneration()

        for adWindow in adWindows {
            guard adWindow.analysisAssetId == assetId else {
                noteIngestOutcome(.droppedForeignAsset, windowId: adWindow.id)
                continue
            }
            if isTerminalProducerRevision(adWindow) {
                // A known stale replay is not a newer producer mutation. In
                // particular, it must not supersede genuine same-ID material
                // that is suspended in catalog validation. First-seen
                // terminal material still claims below and performs the
                // conservative ID-wide disarm.
                retireNonRevertedWindowStateIfMatching(adWindow)
                noteIngestOutcome(
                    .droppedTerminalProducerReplay, windowId: adWindow.id
                )
                continue
            }
            guard claimProducerMutation(
                windowId: adWindow.id,
                generation: producerGeneration
            ) else {
                noteIngestOutcome(
                    .droppedStaleProducerRevision, windowId: adWindow.id
                )
                continue
            }
            // playhead-ar60: BOTH persisted confidences must be usable. The
            // existing call validates `confidence` (DETECTION); V47 added a
            // second number that `evaluateWindow` gates the skip on, so the
            // fail-closed door has to cover it too — otherwise a row could be
            // refused for a malformed detection score and admitted with a
            // malformed actuation one.
            guard Self.hasValidRuntimeWindowMaterial(
                id: adWindow.id,
                analysisAssetId: adWindow.analysisAssetId,
                startTime: adWindow.startTime,
                endTime: adWindow.endTime,
                confidence: adWindow.confidence
            ), adWindow.carriesUsableActuationConfidence else {
                retireAllNonRevertedWindowStateIfPresent(
                    windowId: adWindow.id
                )
                logger.warning(
                    "AdWindow \(adWindow.id, privacy: .public) has invalid runtime material — automatic admission refused"
                )
                noteIngestOutcome(.droppedInvalidMaterial, windowId: adWindow.id)
                continue
            }
            guard let incomingState = SkipDecisionState(
                rawValue: adWindow.decisionState
            ) else {
                retireAllNonRevertedWindowStateIfPresent(
                    windowId: adWindow.id
                )
                logger.warning(
                    "AdWindow \(adWindow.id, privacy: .public) has malformed decisionState — automatic admission refused"
                )
                noteIngestOutcome(
                    .droppedMalformedDecisionState, windowId: adWindow.id
                )
                continue
            }

            let decodedGate = adWindow.eligibilityGate.flatMap {
                SkipEligibilityGate(rawValue: $0)
            }
            let hasMalformedEligibilityGate =
                adWindow.eligibilityGate.map {
                    $0 != "autoSkip" && decodedGate == nil
                } ?? false
            let catalogClaimRequiresCurrentAuthority =
                adWindow.claimsCatalogMatch
                && !hasMalformedEligibilityGate
                && incomingState != .reverted
                && incomingState != .suppressed
                && !isTerminalProducerRevision(adWindow)
                && (decodedGate == nil || decodedGate == .eligible)
            let catalogProvenanceMustFailClosed: Bool
            if catalogClaimRequiresCurrentAuthority {
                let expectedEpisodeGeneration = episodeLifecycleGeneration
                let expectedShowId = activePodcastId
                let hasCurrentAuthority =
                    await hasCurrentCatalogMatchAuthority(
                        adWindow,
                        expectedShowId: expectedShowId
                    )
#if DEBUG
                if let barrier =
                    catalogAdmissionValidationBarrierForTesting {
                    await barrier()
                }
#endif
                guard isCurrentProducerMutation(
                    windowId: adWindow.id,
                    generation: producerGeneration
                ) else {
                    noteIngestOutcome(
                        .droppedStaleProducerRevision, windowId: adWindow.id
                    )
                    continue
                }
                // The catalog actor hop is an episode-lifecycle suspension
                // boundary. Never install the old episode's result after a
                // replacement became active while validation was in flight.
                guard episodeLifecycleGeneration
                        == expectedEpisodeGeneration,
                      activeAssetId == assetId,
                      activePodcastId == expectedShowId else {
                    noteIngestOutcome(
                        .droppedEpisodeReplaced, windowId: adWindow.id
                    )
                    return
                }
                catalogProvenanceMustFailClosed = !hasCurrentAuthority
            } else {
                catalogProvenanceMustFailClosed = false
            }

            if provisionallyResolvingSuggestWindowIds.contains(adWindow.id) {
                bufferedSuggestProducerUpdates[adWindow.id] =
                    .adWindow(adWindow)
                noteIngestOutcome(
                    .bufferedProvisionalResolution, windowId: adWindow.id
                )
                continue
            }

            // A user's explicit answer is authoritative for this producer ID
            // across both precision tiers. Check before gate decoding so No
            // cannot become an automatic skip and Yes cannot become another
            // suggestion if a stale producer revises the gate.
            if hasTerminalSuggestResolution(adWindow.id) {
                logger.debug(
                    "AdWindow \(adWindow.id, privacy: .public) ignored — suggestion already resolved by the user"
                )
                noteIngestOutcome(
                    .droppedUserResolvedSuggestion, windowId: adWindow.id
                )
                continue
            }

            let existingManaged = windows[adWindow.id]
            let existingState = existingManaged?.decisionState
            // playhead-wq34: hoisted from the edge-anchor stamp site at the end
            // of this loop, which is its only other reader. It is a pure
            // function of `existingManaged` and `adWindow`, neither of which is
            // rebound below, so the move is behaviour-neutral — and the tier
            // routing needs it, because the anchors the stamp will INSTALL are
            // what `evaluateWindow` will later classify on.
            let replacesManagedMaterial = existingManaged.map {
                !AdWindowMaterialIdentity.sameProducerRevision(
                    $0.adWindow,
                    adWindow
                )
            } ?? false

            // A user-reverted window is terminal. Applied windows remain
            // terminal only while the newest precision gate still permits the
            // managed path; markOnly/blocked updates below must disarm them.
            if existingState == .reverted
                || revertedProducerWindowIds.contains(adWindow.id) {
                retireSuggestedWindowIfPresent(windowId: adWindow.id)
                noteIngestOutcome(.droppedUserReverted, windowId: adWindow.id)
                continue
            }

            // Producer-terminal material cannot enter either precision tier.
            // Handle it before inventory/catalog demotions, because routing a
            // reverted or suppressed row through the mark-only branch would
            // manufacture an actionable Yes/No suggestion from a tombstone.
            // Record authority outside the actionable window map: suppression
            // fences this exact revision, while revert fences the producer ID.
            if incomingState == .reverted
                || incomingState == .suppressed {
                let isNewTerminalRevision =
                    rememberTerminalProducerRevision(adWindow)
                if incomingState == .reverted {
                    revertedProducerWindowIds.insert(adWindow.id)
                    _ = retireAllNonRevertedWindowStateIfPresent(
                        windowId: adWindow.id
                    )
                } else if isNewTerminalRevision {
                    _ = retireAllNonRevertedWindowStateIfPresent(
                        windowId: adWindow.id
                    )
                } else {
                    _ = retireNonRevertedWindowStateIfMatching(adWindow)
                }
                noteIngestOutcome(
                    .droppedProducerTerminalState,
                    windowId: adWindow.id,
                    detail: incomingState.rawValue
                )
                continue
            }

            // `nil` is a supported legacy stamp and `"autoSkip"` is the
            // production precision-gate literal. Any other non-nil value that
            // fails enum decoding is malformed persistence, not a future
            // permission grant. Retire an older same-ID cue before refusing
            // the revision so corrupt storage cannot preserve prior authority.
            if hasMalformedEligibilityGate {
                retireAllNonRevertedWindowStateIfPresent(
                    windowId: adWindow.id
                )
                logger.warning(
                    "AdWindow \(adWindow.id, privacy: .public) has malformed eligibilityGate — automatic admission refused"
                )
                noteIngestOutcome(
                    .droppedMalformedEligibilityGate, windowId: adWindow.id
                )
                continue
            }

            // playhead-xr3t: post-hoc inventory sanity filter. Runs
            // BEFORE the eligibility-gate decode below so a rejected
            // span never enters the active window set (and therefore
            // never reaches `evaluateAndPush` / banner emission). The
            // filter is a no-op when its feature flag is OFF —
            // identical pre-Phase-3 behaviour, asserted by the rollback
            // tests.
            //
            // An exact-geometry refresh may bypass the filter: if a window
            // made it into the active set on an earlier push, a context-order
            // change must not silently drop that same span. A materially
            // changed same-ID revision has never been validated, however, and
            // must pass the filter as a fresh span.
            let hasPreviouslyValidatedGeometry =
                existingManaged?.adWindow.startTime == adWindow.startTime
                && existingManaged?.adWindow.endTime == adWindow.endTime
            if !hasPreviouslyValidatedGeometry {
                let verdict = inventoryFilter.evaluate(
                    startTime: adWindow.startTime,
                    endTime: adWindow.endTime,
                    episodeDuration: activeEpisodeDuration,
                    declaredChapters: activeDeclaredChapters
                )
                if case let .rejected(reason) = verdict {
                    logger.info(
                        "AdWindow \(adWindow.id, privacy: .public) rejected by inventory sanity filter: \(reason.rawValue, privacy: .public)"
                    )
                    retireAllNonRevertedWindowStateIfPresent(
                        windowId: adWindow.id
                    )
                    // playhead-isp5: the REASON is part of the outcome. Four
                    // rejection reasons share this branch and they are four
                    // unrelated defects — `tooEarly` on a pre-roll is not the
                    // same news as `overlapsDeclaredChapter`.
                    noteIngestOutcome(
                        .droppedInventorySanity,
                        windowId: adWindow.id,
                        detail: reason.rawValue
                    )
                    continue
                }
            }

            // playhead-gtt9.11: precision gate. A window stamped
            // `eligibilityGate = "markOnly"` is visible in the UI as a
            // possible-ad marker but must never be promoted into the auto-
            // skip path. Mirror the blocked-gate check in
            // `receiveAdDecisionResults` so both entry points honor the
            // precision contract.
            //
            // playhead-gtt9.23: route markOnly windows into the suggest
            // tier so the user can see them and answer Yes to skip. The skip
            // path remains untouched — `suggestWindows` is stored
            // separately from `windows` and is never evaluated by
            // `evaluateAndPush()`. The only effect is one banner emission
            // (per window) on the existing `bannerItemStream`, tagged
            // `tier: .suggest` so the UI renders the medium-tier copy.
            //
            // Decode persisted enum stamps through `SkipEligibilityGate`
            // rather than string comparisons. `nil` and the intentional
            // producer literal `"autoSkip"` retain their legacy automatic
            // meaning; unknown non-nil values were rejected as malformed
            // above.
            //
            // Producer note: `AdWindow.eligibilityGate` has multiple
            // writers. The live precision-gate label
            // (`AdDetectionService.precisionGateLabel`, called from
            // both the hot-path post-classify site and the aggregator
            // promotion site) emits `"markOnly"` (which round-trips
            // as `SkipEligibilityGate.markOnly` — the case this
            // decode pins) and `"autoSkip"` (a literal that is NOT a
            // `SkipEligibilityGate` raw value and therefore decodes
            // to nil). Fusion stamps — the full `SkipEligibilityGate`
            // raw-value space, including `.eligible`, the blocked-*
            // cases, and `.blockedByFMConsensus` — originate only in
            // `AdDetectionService.runBackfill` via
            // `buildFusionAdWindow`, which writes
            // `decision.eligibilityGate.rawValue` directly. Those
            // fusion-stamped rows surface to every `receiveAdWindows`
            // caller, NOT just the preload + finalizeBackfill paths:
            //   - cross-launch preload (`beginEpisode`) reads them
            //     from the store on relaunch;
            //   - the final-pass backfill push delivers them
            //     in-memory immediately after `runBackfill`;
            //   - the hot-path push (`AnalysisCoordinator
            //     .handlePersistedTranscriptChunks`) delivers
            //     `runHotPathResult.windows` whose gate is normally
            //     the precision-gate literal, BUT
            //     `reconcileHotPathWindows` builds a `preservedWindow`
            //     that may retain an earlier recognized demotion only for
            //     byte-identical geometry. Fresh automatic/catalog authority
            //     always comes from the current run. A backfill row written
            //     with `policyAction == .autoSkipEligible` and
            //     `decision.eligibilityGate != .eligible` is persisted
            //     with `decisionState == .candidate` by
            //     `buildFusionAdWindow`'s `policyAction` switch — so
            //     an exact-geometry replay can retain its conservative
            //     fusion demotion on the hot-path push.
            // The decode here is the producer-aware first half of the
            // guard pair: this branch handles `.markOnly` (the live
            // precision-gate value that round-trips as a known case)
            // by routing to the suggest tier. The companion fusion-
            // blocked-gate guard immediately below — added in
            // playhead-bq70 — restores symmetry with
            // `receiveAdDecisionResults` (which hard-filters to
            // `eligibilityGate == .eligible`) by dropping all other
            // recognised non-eligible cases before they reach
            // `evaluateAndPush`. Only nil and the explicit `"autoSkip"`
            // literal use the non-enum producer contract.
            // playhead-wq34: THE MONOTONICITY FALLBACK. A row the managed tier
            // cannot act on takes the SAME door a mark-only row takes, so the
            // stronger stamp can never reach the listener with less than the
            // weaker one. See `managedTierWouldBeSilent` for the rule and for
            // why it is applied HERE rather than inside `evaluateWindow`'s mode
            // switch.
            let admissionMode = admissionSkipMode(
                for: adWindow,
                replacesManagedMaterial: replacesManagedMaterial
            )
            let silentManagedTier =
                (decodedGate == nil || decodedGate == .eligible)
                && managedTierWouldBeSilent(
                    mode: admissionMode,
                    incomingState: incomingState,
                    existingState: existingState
                )
            if decodedGate == .markOnly
                || silentManagedTier
                || (
                    catalogProvenanceMustFailClosed
                    && (decodedGate == nil || decodedGate == .eligible)
                ) {
                retireManagedWindowIfPresent(windowId: adWindow.id)
                if catalogProvenanceMustFailClosed {
                    logger.warning(
                        "AdWindow \(adWindow.id, privacy: .public) has untrusted catalog provenance — surfacing as suggest tier"
                    )
                } else if decodedGate == .markOnly {
                    logger.debug(
                        "AdWindow \(adWindow.id, privacy: .public) eligibilityGate=markOnly — surfacing as suggest tier"
                    )
                } else {
                    logger.debug(
                        "AdWindow \(adWindow.id, privacy: .public) is eligible but its detector class is \(admissionMode.rawValue, privacy: .public) — surfacing as suggest tier rather than silently confirming"
                    )
                }
                // playhead-bllt: the census carries WHY a row is in the suggest
                // tier rather than the managed one, when the reason is its
                // extent. A row whose edges nobody proved could not have been
                // auto-skipped whatever its score said, and after this bead the
                // hot path's rows are exactly that population — so `delivered=`
                // shifting from managed to suggest is a demotion the audit
                // trail can now name, instead of a banner that merely failed to
                // appear somewhere else. `nil` (and therefore no detail token)
                // for a fully-anchored row, deliberately: a detail that fires
                // on every delivery says nothing.
                //
                // playhead-wq34: when the reason is the MODE rather than the
                // extent, the detail says so. A byte-exact row demoted to
                // suggest because its class is no longer `.auto` is fully
                // anchored, so `censusDetail` returns nil for it and the row
                // would be indistinguishable in a device pull from an ordinary
                // mark-only delivery — the same "a value that names one thing
                // read as though it named another" shape the extent detail was
                // added to remove. The mode reason wins only when it is the
                // ONLY reason: a `.markOnly` row (or an untrusted catalog
                // claim) would be here regardless of mode, so it keeps the
                // extent detail it has always carried.
                let extentDetail: String?
                if decodedGate == .markOnly || catalogProvenanceMustFailClosed {
                    extentDetail = HotPathExtentGate.censusDetail(
                        for: resolvedExtentSupport(for: adWindow)
                    )
                } else {
                    extentDetail = "silent_managed_tier_\(admissionMode.rawValue)"
                }
                if banneredWindowIds.contains(adWindow.id) {
                    noteIngestOutcome(
                        .droppedAlreadyBannered,
                        windowId: adWindow.id,
                        detail: extentDetail
                    )
                } else {
                    registerSuggestedWindow(adWindow)
                    // playhead-isp5: `registerSuggestedWindow` DECLINES to arm
                    // an unchanged revision whose token the user already saw.
                    // Read the arming set rather than assuming the call armed —
                    // an outcome that reports what it hoped for is the same
                    // class of defect this bead exists to remove.
                    noteIngestOutcome(
                        armedSuggestWindowIds.contains(adWindow.id)
                            ? .armedSuggest
                            : .suggestReplayNotRearmed,
                        windowId: adWindow.id,
                        detail: extentDetail
                    )
                }
                continue
            }

            let incomingManaged = ManagedWindow(
                adWindow: adWindow,
                decisionState: incomingState,
                snappedStart: adWindow.startTime,
                snappedEnd: adWindow.endTime,
                idempotencyKey: idempotencyKey(
                    assetId: assetId,
                    windowId: adWindow.id
                ),
                cueActive: false
            )
            if existingState == .applied,
               decodedGate == nil || decodedGate == .eligible,
               let existing = windows[adWindow.id] {
                if bannerMaterialRevisionToken(for: existing)
                    == bannerMaterialRevisionToken(for: incomingManaged) {
                    // Candidate/confirmed replays of an exact applied
                    // producer revision are stale lifecycle snapshots: keep
                    // the durable applied receipt and its cue. Producer-
                    // terminal states were handled before every tier-routing
                    // branch above.
                    retireSuggestedWindowIfPresent(
                        windowId: adWindow.id
                    )
                    noteIngestOutcome(
                        .retainedAppliedReceipt, windowId: adWindow.id
                    )
                    continue
                }
                // A same-ID producer value is terminal only for its exact
                // material revision. Retire the old card/cue ownership before
                // installing the replacement so late Yes/No actions fail their
                // material token while the newest revision receives a fresh
                // presentation.
                _ = retireAllNonRevertedWindowStateIfPresent(
                    windowId: adWindow.id
                )
            }

            // playhead-bq70: symmetric blocked-gate guard. `receiveAdDecisionResults`
            // hard-filters its inputs to `eligibilityGate == .eligible`; this entry
            // point must honor the same precision contract for fusion-stamped rows
            // surfaced via the AdWindow path. The fusion stamps that originate in
            // `AdDetectionService.runBackfill` via `buildFusionAdWindow` write the
            // full `SkipEligibilityGate.rawValue` space — including the blocked
            // cases (`.blockedByEvidenceQuorum`, `.blockedByPolicy`,
            // `.blockedByUserCorrection`, `.blockedByFMConsensus`). These rows
            // surface to all three `receiveAdWindows` callers (cross-launch
            // preload, hot-path post-classify push, final-pass backfill push) —
            // see the producer-note block above. Without this guard a
            // `policyAction == .autoSkipEligible` row that fusion subsequently
            // demoted via `eligibilityGate != .eligible` (persisted as
            // `decisionState == .candidate`) would silently re-enter the
            // auto-skip path on any of the three callers, violating the
            // precision contract.
            //
            // Semantics chosen to match `receiveAdDecisionResults` exactly:
            // anything that decodes to a recognised `SkipEligibilityGate` case
            // OTHER than `.eligible` (the markOnly branch already returned
            // above) is dropped here. Nil and `"autoSkip"` preserve the
            // non-fusion producer contract; malformed non-nil values were
            // rejected above. See playhead-bq70 for the cycle history.
            if let decoded = decodedGate, decoded != .eligible {
                logger.debug(
                    "AdWindow \(adWindow.id, privacy: .public) eligibilityGate=\(decoded.rawValue, privacy: .public) — blocked, not adding to active windows"
                )
                retireAllNonRevertedWindowStateIfPresent(
                    windowId: adWindow.id
                )
                noteIngestOutcome(
                    .droppedBlockedGate,
                    windowId: adWindow.id,
                    detail: decoded.rawValue
                )
                continue
            }

            // playhead-rfu-sad: gate-flip race guard. A window first seen
            // as `markOnly` can later re-arrive with the gate cleared
            // (e.g. fusion now admits it as auto-skip eligible). Without
            // this clear, `suggestWindows[id]` would stay populated
            // while `windows[id]` also gets a parallel managed entry —
            // a still-visible suggest banner could re-fire
            // `acceptSuggestedSkip` and synthesize a duplicate managed
            // window via a fresh `UUID().uuidString` (see
            // `acceptSuggestedSkip`'s `promotedId`).
            if retireSuggestedWindowIfPresent(windowId: adWindow.id) {
                logger.debug(
                    "AdWindow \(adWindow.id, privacy: .public) gate flipped from markOnly — cleared suggest entry"
                )
            }

            // Build or update the managed window.
            let key = idempotencyKey(assetId: assetId, windowId: adWindow.id)

            let managed = ManagedWindow(
                adWindow: adWindow,
                decisionState: incomingState,
                snappedStart: adWindow.startTime,
                snappedEnd: adWindow.endTime,
                idempotencyKey: key,
                cueActive: false
            )
            windows[adWindow.id] = managed
            noteIngestOutcome(.admittedManaged, windowId: adWindow.id)

            // playhead-hdgk: stamp the per-edge anchor tier persisted on the
            // row into `edgeAnchorsByWindowId` at the SAME moment the window
            // is registered — BEFORE the single `evaluateAndPush()` below can
            // promote it to `.applied`. This honors the `setEdgeAnchors`
            // ordering caveat for all three `receiveAdWindows` callers (preload
            // / live backfill push / cross-launch): anchors are present before
            // the first promotion-capable evaluation. We populate the map
            // directly rather than via `setEdgeAnchors(...)` to avoid a second
            // `evaluateAndPush()` per window. An unknown persisted raw value
            // (older enum, corrupt row) decodes to `.unanchored` — the
            // conservative default under which flag-ON auto-skips nothing.
            // Flag-OFF: `paddedCueSpan` never reads this map, so the stamp is
            // inert and behavior stays byte-identical.
            //
            // Precedence: only stamp when NO entry exists yet, so that (a) an
            // explicit `setEdgeAnchors(...)` override made before ingest is
            // never clobbered by a persisted default, and (b) the FIRST (best-
            // provenance) stamp survives a later re-arrival of the same id
            // carrying weaker anchors (e.g. a hot-path reconcile that copies a
            // preloaded row's id but not its fusion-derived anchors). In
            // production there is no `setEdgeAnchors` caller, so the persisted
            // anchors win on first arrival — the common path.
            // playhead-wq34: `replacesManagedMaterial` is computed once, above
            // the tier-routing branch. Same expression, same value.
            if edgeAnchorsByWindowId[adWindow.id] == nil
                || replacesManagedMaterial {
                edgeAnchorsByWindowId[adWindow.id] = (
                    start: AutoSkipEdgeAnchor(rawValue: adWindow.startEdgeAnchor) ?? .unanchored,
                    end: AutoSkipEdgeAnchor(rawValue: adWindow.endEdgeAnchor) ?? .unanchored
                )
            }
        }

        // Re-evaluate all windows and push updated cues.
        evaluateAndPush()
    }

    func retireAdWindows(ids: Set<String>) async {
        guard !ids.isEmpty else { return }
        let producerGeneration = nextProducerMutationGeneration()

        for id in ids {
            guard claimProducerMutation(
                windowId: id,
                generation: producerGeneration
            ) else {
                continue
            }
            if provisionallyResolvingSuggestWindowIds.contains(id) {
                bufferedSuggestProducerUpdates[id] = .retired
                continue
            }
            // Producer retirement invalidates every non-reverted
            // representation, including an already-applied cue and its
            // completed-action banner. Keeping an applied window here would
            // leave it armed for a later seek after the producer withdrew it.
            retireAllNonRevertedWindowStateIfPresent(windowId: id)
        }

        evaluateAndPush()
    }

    /// Receive fusion-based AdDecisionResults from AdDetectionService.
    ///
    /// This is the Phase 6 production entry point (playhead-4my.6.4). Replaces the
    /// raw AdWindow path for backfill-sourced decisions. The eligibility gate is
    /// checked before adding windows; blocked results are never promoted to applied.
    ///
    /// - Parameter results: Fusion decisions from BackfillEvidenceFusion + DecisionMapper.
    func receiveAdDecisionResults(_ results: [AdDecisionResult]) async {
        guard !results.isEmpty, let assetId = activeAssetId else { return }
        let producerGeneration = nextProducerMutationGeneration()

        for result in results {
            guard result.analysisAssetId == assetId else { continue }
            let exactProducerRevision = result.producerRevision.flatMap {
                revision -> AdWindow? in
                guard revision.id == result.id,
                      revision.analysisAssetId == result.analysisAssetId,
                      revision.startTime == result.startTime,
                      revision.endTime == result.endTime,
                      // playhead-ar60: the envelope binds the durable row to
                      // the live decision by the ACTUATION number — that is
                      // what `AdDecisionResult.skipConfidence` holds. Reading
                      // `revision.confidence` here would compare a detection
                      // number against an actuation number and refuse every
                      // genuine fusion envelope.
                      revision.actuationConfidence == result.skipConfidence else {
                    return nil
                }
                return revision
            }
            let exactProducerState = exactProducerRevision.flatMap {
                SkipDecisionState(rawValue: $0.decisionState)
            }
            let hasTerminalProducerState =
                exactProducerState == .reverted
                || exactProducerState == .suppressed
            let isTerminalReplay = exactProducerRevision.map {
                isTerminalProducerRevision($0)
            } ?? false

            if isTerminalReplay,
               let producerRevision = exactProducerRevision {
                // A known stale replay is not a newer producer mutation. Do
                // not let it cancel a genuine same-ID replacement that is
                // suspended at the catalog-authority actor hop.
                retireNonRevertedWindowStateIfMatching(producerRevision)
                continue
            }

            guard claimProducerMutation(
                windowId: result.id,
                generation: producerGeneration
            ) else {
                continue
            }
            guard Self.hasValidRuntimeWindowMaterial(
                id: result.id,
                analysisAssetId: result.analysisAssetId,
                startTime: result.startTime,
                endTime: result.endTime,
                confidence: result.skipConfidence
            ) else {
                retireAllNonRevertedWindowStateIfPresent(
                    windowId: result.id
                )
                logger.warning(
                    "AdDecisionResult \(result.id, privacy: .public) has invalid runtime material — automatic admission refused"
                )
                continue
            }

            let catalogProvenanceMustFailClosed: Bool
            if result.eligibilityGate == .eligible,
               !hasTerminalProducerState,
               !isTerminalReplay,
               let producerRevision = exactProducerRevision,
               producerRevision.claimsCatalogMatch {
                let expectedEpisodeGeneration = episodeLifecycleGeneration
                let expectedShowId = activePodcastId
                let hasCurrentAuthority =
                    await hasCurrentCatalogMatchAuthority(
                        producerRevision,
                        expectedShowId: expectedShowId
                    )
#if DEBUG
                if let barrier =
                    catalogAdmissionValidationBarrierForTesting {
                    await barrier()
                }
#endif
                guard isCurrentProducerMutation(
                    windowId: result.id,
                    generation: producerGeneration
                ) else {
                    continue
                }
                guard episodeLifecycleGeneration
                        == expectedEpisodeGeneration,
                      activeAssetId == assetId,
                      activePodcastId == expectedShowId else {
                    return
                }
                catalogProvenanceMustFailClosed = !hasCurrentAuthority
            } else {
                catalogProvenanceMustFailClosed = false
            }

            if provisionallyResolvingSuggestWindowIds.contains(result.id) {
                bufferedSuggestProducerUpdates[result.id] =
                    .decisionResult(result)
                continue
            }

            // Mirror `receiveAdWindows`: explicit Yes/No is terminal for the
            // producer ID before fusion eligibility can reinterpret it.
            if hasTerminalSuggestResolution(result.id) {
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) ignored — suggestion already resolved by the user"
                )
                continue
            }

            let existingManaged = windows[result.id]
            let existingState = existingManaged?.decisionState

            // A user-reverted window is terminal. Applied windows remain
            // terminal only while the newest fusion decision stays eligible;
            // blocked updates below must disarm them.
            if existingState == .reverted
                || revertedProducerWindowIds.contains(result.id) {
                retireSuggestedWindowIfPresent(windowId: result.id)
                continue
            }

            if hasTerminalProducerState,
               let producerRevision = exactProducerRevision,
               let producerState = exactProducerState {
                let isNewTerminalRevision =
                    rememberTerminalProducerRevision(producerRevision)
                if producerState == .reverted {
                    revertedProducerWindowIds.insert(result.id)
                    _ = retireAllNonRevertedWindowStateIfPresent(
                        windowId: result.id
                    )
                } else if isNewTerminalRevision {
                    _ = retireAllNonRevertedWindowStateIfPresent(
                        windowId: result.id
                    )
                } else {
                    _ = retireNonRevertedWindowStateIfMatching(
                        producerRevision
                    )
                }
                continue
            }

            // Blocked gate: never add blocked results to the active window set.
            guard result.eligibilityGate == .eligible else {
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) gate=blocked — not adding to active windows"
                )
                retireAllNonRevertedWindowStateIfPresent(
                    windowId: result.id
                )
                continue
            }

            // Mirror `receiveAdWindows`: only an exact-geometry refresh may
            // reuse prior inventory validation. A same-ID revision with new
            // bounds is a fresh span and must be validated.
            let hasPreviouslyValidatedGeometry =
                existingManaged?.adWindow.startTime == result.startTime
                && existingManaged?.adWindow.endTime == result.endTime
            if !hasPreviouslyValidatedGeometry {
                let verdict = inventoryFilter.evaluate(
                    startTime: result.startTime,
                    endTime: result.endTime,
                    episodeDuration: activeEpisodeDuration,
                    declaredChapters: activeDeclaredChapters
                )
                if case let .rejected(reason) = verdict {
                    logger.info(
                        "AdDecisionResult \(result.id, privacy: .public) rejected by inventory sanity filter: \(reason.rawValue, privacy: .public)"
                    )
                    retireAllNonRevertedWindowStateIfPresent(
                        windowId: result.id
                    )
                    continue
                }
            }

            // Require the exact row persisted by AdDetectionService. This keeps
            // the durable producer-revision fence byte-for-byte aligned and
            // propagates catalog identity, lifecycle, and edge anchors through
            // the live decision handoff. Validate the redundant envelope so a
            // malformed producer cannot smuggle unrelated row material. A
            // missing/reload-failed row must not be synthesized from the lossy
            // decision envelope.
            guard let producerRevision = result.producerRevision,
                  producerRevision.id == result.id,
                  producerRevision.analysisAssetId == result.analysisAssetId,
                  producerRevision.startTime == result.startTime,
                  producerRevision.endTime == result.endTime,
                  // playhead-ar60: same binding, same reason as the envelope
                  // check in `receiveAdDecisionResults` — the live decision
                  // carries the ACTUATION number.
                  producerRevision.actuationConfidence == result.skipConfidence,
                  SkipDecisionState(
                      rawValue: producerRevision.decisionState
                  ).map({
                      $0 == .candidate
                          || $0 == .confirmed
                          || $0 == .applied
                  }) == true,
                  producerRevision.eligibilityGate
                    == SkipEligibilityGate.eligible.rawValue
            else {
                retireAllNonRevertedWindowStateIfPresent(
                    windowId: result.id
                )
                logger.warning(
                    "AdDecisionResult \(result.id, privacy: .public) lacks an exact persisted producer revision — automatic admission refused"
                )
                continue
            }

            // A supplied positive/invalid catalog score is an explicit claim
            // that learned evidence helped this decision. Never let a malformed
            // envelope, missing show, legacy fingerprint cohort, or incomplete
            // row/lifecycle provenance disappear into the compatibility
            // fallback and become an automatic skip.
            if catalogProvenanceMustFailClosed {
                retireManagedWindowIfPresent(windowId: result.id)
                if !banneredWindowIds.contains(result.id) {
                    registerSuggestedWindow(producerRevision)
                }
                logger.warning(
                    "AdDecisionResult \(result.id, privacy: .public) has untrusted catalog provenance — automatic admission refused"
                )
                continue
            }

            // playhead-wq34: the SAME monotonicity fallback as
            // `receiveAdWindows`, because this is the same admission decision
            // through a second door — the final-pass backfill's fusion handoff
            // (`AdDetectionService.runBackfill`). This door is the PUREST
            // instance of the defect: it admits ONLY `.eligible` rows, so
            // before this bead every row it delivered for a non-`.auto` class
            // was silent by construction. Leaving it would put the inversion
            // BETWEEN THE DOORS — the identical row carded when the preload
            // delivered it and vanished when backfill did — which is the
            // divergence `forwardPersistedAdWindows` is documented to exist to
            // prevent. One rule, stated once, asked twice.
            //
            // `replacesManagedMaterial` is resolved from `producerRevision`
            // (the exact durable row this decision is fenced against), matching
            // the stamp site at the end of this loop.
            let replacesManagedMaterial = existingManaged.map {
                !AdWindowMaterialIdentity.sameProducerRevision(
                    $0.adWindow,
                    producerRevision
                )
            } ?? false
            let admissionMode = admissionSkipMode(
                for: producerRevision,
                replacesManagedMaterial: replacesManagedMaterial
            )
            if managedTierWouldBeSilent(
                mode: admissionMode,
                // This door forces `.confirmed` when it builds its
                // `ManagedWindow`, so the durable row's own state is what an
                // applied receipt is legible in.
                incomingState: SkipDecisionState(
                    rawValue: producerRevision.decisionState
                ) ?? .confirmed,
                existingState: existingState
            ) {
                retireManagedWindowIfPresent(windowId: result.id)
                if !banneredWindowIds.contains(result.id) {
                    registerSuggestedWindow(producerRevision)
                }
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) is eligible but its detector class is \(admissionMode.rawValue, privacy: .public) — surfacing as suggest tier rather than silently confirming"
                )
                continue
            }

            // playhead-rfu-sad: symmetric gate-flip clear. If a fusion
            // result for this id arrives eligible after the same id was
            // first surfaced as a markOnly suggest entry, drop the
            // suggest bookkeeping so a still-visible banner can't
            // re-fire `acceptSuggestedSkip` against a now-managed
            // window. Mirrors the clear in `receiveAdWindows`.
            if retireSuggestedWindowIfPresent(windowId: result.id) {
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) gate flipped from markOnly — cleared suggest entry"
                )
            }

            let key = idempotencyKey(assetId: assetId, windowId: result.id)

            let managed = ManagedWindow(
                adWindow: producerRevision,
                decisionState: .confirmed,
                snappedStart: result.startTime,
                snappedEnd: result.endTime,
                idempotencyKey: key,
                cueActive: false
            )
            if existingState == .applied,
               let existing = windows[result.id] {
                if bannerMaterialRevisionToken(for: existing)
                    == bannerMaterialRevisionToken(for: managed) {
                    retireSuggestedWindowIfPresent(windowId: result.id)
                    continue
                }
                _ = retireAllNonRevertedWindowStateIfPresent(
                    windowId: result.id
                )
            }
            windows[result.id] = managed
            // playhead-wq34: `replacesManagedMaterial` is computed once, above
            // the tier-routing branch. Same expression, same value.
            if edgeAnchorsByWindowId[result.id] == nil
                || replacesManagedMaterial {
                edgeAnchorsByWindowId[result.id] = (
                    start: AutoSkipEdgeAnchor(
                        rawValue: producerRevision.startEdgeAnchor
                    ) ?? .unanchored,
                    end: AutoSkipEdgeAnchor(
                        rawValue: producerRevision.endEdgeAnchor
                    ) ?? .unanchored
                )
            }
        }

        evaluateAndPush()
    }

    // MARK: - Playback State Updates

    /// Update the current playhead position. Called from playback observer.
    func updatePlayheadTime(_ time: TimeInterval) {
        guard time.isFinite else { return }
        currentPlayheadTime = time
        hasObservedPlayheadThisEpisode = true

        // playhead-d3g0: the suggest banner's emit trigger. This is the ONLY
        // site that presents an armed suggestion; `receiveAdWindows` arms and
        // nothing else emits. Kept first so the affordance is not queued behind
        // the learning/seek bookkeeping below — Dan's decision makes this a
        // latency-sensitive path (`suggestEntryLatencyBudgetSeconds`).
        emitSuggestBannersOnPlayheadEntry(at: time)

        // playhead-bwxi: the auto-skip banner's emit trigger, and the ONLY site
        // that presents one. `evaluateAndPush` arms and nothing else emits. The
        // suggest tier goes first because it is a PROSPECTIVE affordance whose
        // whole value is being early enough to act on; the auto tier's card is
        // a receipt for a skip that is happening on this same observation.
        emitAutoSkipBannersOnPlayheadEntry(at: time)

        let consumed = pendingCatalogLearning.values.filter {
            time >= $0.eligiblePlayheadTime
        }
        for pending in consumed {
            pendingCatalogLearning.removeValue(
                forKey: RecurrenceSourceKey(pending.window)
            )
            scheduleConfirmedRecurrenceLearning(
                for: pending.window,
                showId: pending.showId,
                source: pending.source,
                lifecycle: pending.lifecycle,
                expectedLearningGeneration: pending.learningGeneration
            )
        }

        // Check if seek suppression should be lifted.
        if skipSuppressedAfterSeek, let seekTime = lastSeekTime {
            let elapsed = Date().timeIntervalSince(seekTime)
            if elapsed >= config.seekStabilitySeconds {
                skipSuppressedAfterSeek = false
                logger.info("Skip suppression lifted after \(elapsed, format: .fixed(precision: 1))s stability")
                evaluateAndPush()
            }
        }
    }

    /// Record a user-initiated seek. Suppresses auto-skip until confidence
    /// re-stabilizes.
    func recordUserSeek(to time: TimeInterval) {
        guard time.isFinite, time >= 0 else { return }
        // playhead-d3g0: a seek deliberately does NOT re-arm anything, and
        // deliberately does not emit either.
        //
        // Not re-arming: an armed suggestion fires once per revision, so
        // scrubbing backwards into a span that has already asked its question
        // does not ask it again. That is the once-per-window-per-episode
        // guarantee, and it is what keeps the affordance from becoming noise on
        // a rewind.
        //
        // Not emitting: a FORWARD seek that lands inside a still-armed span is
        // a genuine entry, but it is presented by the next position observation
        // rather than here. One trigger site is worth the ≤ one tick
        // (`suggestEntryLatencyBudgetSeconds` covers it), and `currentPlayheadTime`
        // set here is what that observation reads anyway.
        //
        // A user seek can jump over a proposed span without consuming the
        // orchestrator's skip. Invalidate pending positives, including an
        // applied-persistence task that has not re-entered the actor yet.
        catalogLearningGeneration &+= 1
        pendingCatalogLearning.removeAll()
        lastSeekTime = Date()
        skipSuppressedAfterSeek = true
        currentPlayheadTime = time
        hasObservedPlayheadThisEpisode = true
        logger.info("User seek to \(time, format: .fixed(precision: 1))s -- skip suppressed")

        // Do NOT remove existing cues ahead of the new position.
        // Just suppress firing new ones until stability returns.
    }

    private func normalizedCatalogShowId(_ value: String?) -> String? {
        RecurrenceMaterialIdentity.canonicalIdentifier(value)
    }

    private func nextProducerMutationGeneration() -> UInt64 {
        producerMutationGeneration &+= 1
        return producerMutationGeneration
    }

    /// Claim one ID for an inbound batch. A batch that began before a newer
    /// same-ID mutation is stale even if it has not reached this item yet.
    private func claimProducerMutation(
        windowId: String,
        generation: UInt64
    ) -> Bool {
        if let latest = latestProducerMutationByWindowId[windowId],
           latest > generation {
            return false
        }
        latestProducerMutationByWindowId[windowId] = generation
        return true
    }

    private func isCurrentProducerMutation(
        windowId: String,
        generation: UInt64
    ) -> Bool {
        latestProducerMutationByWindowId[windowId] == generation
    }

    @discardableResult
    private func rememberTerminalProducerRevision(
        _ window: AdWindow
    ) -> Bool {
        var revisions =
            terminalProducerRevisionsByWindowId[window.id] ?? []
        guard !revisions.contains(where: {
            AdWindowMaterialIdentity.sameProducerRevision($0, window)
        }) else {
            return false
        }
        revisions.append(window)
        terminalProducerRevisionsByWindowId[window.id] = revisions
        return true
    }

    private func isTerminalProducerRevision(_ window: AdWindow) -> Bool {
        terminalProducerRevisionsByWindowId[window.id]?.contains(where: {
            AdWindowMaterialIdentity.sameProducerRevision($0, window)
        }) == true
    }

    /// Invalidate an admission currently suspended for this ID. Retirement
    /// must claim the ID even when no representation has been installed yet.
    private func invalidatePendingProducerMutation(windowId: String) {
        latestProducerMutationByWindowId[windowId] =
            nextProducerMutationGeneration()
    }

    /// A structurally compatible persisted claim is not enough for automatic
    /// admission: the exact device-local row must still be active with the
    /// same version and authoritative lifecycle, and the currently persisted
    /// span features must still match it. Missing/corrupt stores, missing
    /// features, rewritten material, and rows revoked after detection all fail
    /// closed.
    private func hasCurrentCatalogMatchAuthority(
        _ window: AdWindow,
        expectedShowId: String?
    ) async -> Bool {
        guard window.hasCompatibleCatalogMatchProvenance(
                  expectedShowId: expectedShowId
              ),
              let adCatalogStore,
              let entryIdRaw = window.catalogMatchedEntryId,
              let entryId = UUID(uuidString: entryIdRaw),
              let showId = window.catalogMatchedShowId,
              let fingerprintVersionRaw = window.catalogFingerprintVersion,
              let fingerprintVersion = CatalogFingerprintVersion(
                  rawValue: fingerprintVersionRaw
              ),
              let learningSourceRaw = window.catalogMatchedLearningSource,
              let learningSource = CatalogLearningSource(
                  rawValue: learningSourceRaw
              ),
              let learningLifecycleRaw =
                window.catalogMatchedLearningLifecycle,
              let learningLifecycle = CatalogLearningLifecycle(
                  rawValue: learningLifecycleRaw
              ) else {
            return false
        }
        let featureWindows: [FeatureWindow]
        do {
            featureWindows = try await store.fetchFeatureWindows(
                assetId: window.analysisAssetId,
                from: window.startTime,
                to: window.endTime
            )
        } catch {
            logger.warning(
                "Catalog admission fingerprint unavailable for \(window.id, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return false
        }
        let candidateFingerprint = AcousticFingerprint.fromFeatureWindows(
            featureWindows
        )
        guard !candidateFingerprint.isZero else { return false }
        return await adCatalogStore.isActiveMatch(
            id: entryId,
            showId: showId,
            fingerprintVersion: fingerprintVersion,
            learningSource: learningSource,
            learningLifecycle: learningLifecycle,
            candidateFingerprint: candidateFingerprint
        )
    }

    /// Validate the small material envelope that can arm a runtime cue. This
    /// guard is independent of the optional inventory filter because corrupt
    /// persisted rows and same-ID replacements must fail closed even when that
    /// product feature is disabled or an existing row bypasses re-evaluation.
    private static func hasValidRuntimeWindowMaterial(
        id: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        confidence: Double
    ) -> Bool {
        RecurrenceMaterialIdentity.canonicalIdentifier(id) != nil
            && RecurrenceMaterialIdentity.canonicalIdentifier(
                analysisAssetId
            ) != nil
            && startTime.isFinite
            && endTime.isFinite
            && confidence.isFinite
            && startTime >= 0
            && endTime > startTime
            && (0...1).contains(confidence)
    }

    private func queueConsumedCatalogLearning(
        for window: AdWindow,
        showId: String?,
        expectedEpisodeGeneration: UInt64,
        expectedLearningGeneration: UInt64
    ) {
        guard episodeLifecycleGeneration == expectedEpisodeGeneration,
              recurrenceLearningIsCurrent(
                  window: window,
                  expectedGeneration: expectedLearningGeneration
              ),
              let showId = normalizedCatalogShowId(showId)
        else {
            return
        }
        let pending = PendingCatalogLearning(
            window: window,
            showId: showId,
            source: .consumedAutoSkip,
            lifecycle: .consumed,
            eligiblePlayheadTime:
                window.endTime + Self.catalogConsumptionDelaySeconds,
            learningGeneration: expectedLearningGeneration
        )
        if currentPlayheadTime >= pending.eligiblePlayheadTime {
            scheduleConfirmedRecurrenceLearning(
                for: pending.window,
                showId: pending.showId,
                source: pending.source,
                lifecycle: pending.lifecycle,
                expectedLearningGeneration: pending.learningGeneration
            )
        } else {
            pendingCatalogLearning[RecurrenceSourceKey(window)] = pending
        }
    }

    private func learnConfirmedRecurrence(
        for window: AdWindow,
        showId: String?,
        source: CatalogLearningSource,
        lifecycle: CatalogLearningLifecycle,
        expectedLearningGeneration: UInt64? = nil
    ) async {
        guard adCatalogStore != nil || repeatedAdCache != nil,
              let showId = normalizedCatalogShowId(showId),
              source.authoritativeLifecycle == lifecycle,
              recurrenceLearningIsCurrent(
                  window: window,
                  expectedGeneration: expectedLearningGeneration
              ),
              window.endTime > window.startTime
        else {
            return
        }
        do {
            let featureWindows = try await store.fetchFeatureWindows(
                assetId: window.analysisAssetId,
                from: window.startTime,
                to: window.endTime
            )
            let fingerprint = AcousticFingerprint.fromFeatureWindows(
                featureWindows
            )
            let repeatedFingerprint = RepeatedAdFingerprint.from(
                featureWindows: featureWindows
            )
            guard recurrenceLearningIsCurrent(
                window: window,
                expectedGeneration: expectedLearningGeneration
            ) else {
                return
            }
            var newlyInsertedCatalogEntry: CatalogEntry?
            if let adCatalogStore, !fingerprint.isZero {
                do {
                    let proposedEntry = CatalogEntry(
                        showId: showId,
                        episodePosition: .unknown,
                        durationSec: window.endTime - window.startTime,
                        acousticFingerprint: fingerprint,
                        transcriptSnippet: nil,
                        sponsorTokens: nil,
                        originalConfidence: window.confidence,
                        learningSource: source,
                        learningLifecycle: lifecycle,
                        sourceAssetId: window.analysisAssetId,
                        sourceWindowId: window.id,
                        sourceStartTime: window.startTime,
                        sourceEndTime: window.endTime,
                        confirmedAt: Date()
                    )
                    let persisted: CatalogEntry?
                    if lifecycle == .consumed {
                        persisted = try await adCatalogStore
                            .insertNewConsumedLearningIfAbsent(proposedEntry)
                    } else {
                        persisted = try await adCatalogStore.insert(
                            entry: proposedEntry
                        )
                    }
                    if persisted?.id == proposedEntry.id {
                        newlyInsertedCatalogEntry = persisted
                    }
#if DEBUG
                    if let barrier =
                        catalogLearningPersistenceBarrierForTesting {
                        await barrier()
                    }
#endif
                } catch {
                    logger.warning(
                        "Catalog learning failed for \(window.id, privacy: .public): \(error.localizedDescription, privacy: .public)"
                    )
                }
            }
            guard recurrenceLearningIsCurrent(
                window: window,
                expectedGeneration: expectedLearningGeneration
            ) else {
                if lifecycle == .consumed,
                   let newlyInsertedCatalogEntry,
                   let adCatalogStore {
                    do {
                        _ = try await adCatalogStore
                            .deleteConsumedLearningIfCurrent(
                                newlyInsertedCatalogEntry
                            )
                    } catch {
                        logger.warning(
                            "Catalog stale-write cleanup failed for \(window.id, privacy: .public): \(error.localizedDescription, privacy: .public)"
                        )
                    }
                }
                return
            }
            if let repeatedAdCache, !repeatedFingerprint.isZero {
                let producerRevision = UUID().uuidString
                do {
                    _ = try await repeatedAdCache.store(
                        showId: showId,
                        fingerprint: repeatedFingerprint,
                        boundaryStart: window.startTime,
                        boundaryEnd: window.endTime,
                        confidence: window.confidence,
                        learningSource: source,
                        learningLifecycle: lifecycle,
                        sourceAssetId: window.analysisAssetId,
                        sourceWindowId: window.id,
                        producerRevision: producerRevision
                    )
                } catch {
                    logger.warning(
                        "Repeated-ad learning failed for \(window.id, privacy: .public): \(error.localizedDescription, privacy: .public)"
                    )
                }
                guard recurrenceLearningIsCurrent(
                    window: window,
                    expectedGeneration: expectedLearningGeneration
                ) else {
                    do {
                        _ = try await repeatedAdCache
                            .deleteIfProducerRevisionMatches(
                                showId: showId,
                                fingerprint: repeatedFingerprint,
                                producerRevision: producerRevision
                            )
                    } catch {
                        logger.warning(
                            "Repeated-ad stale-write cleanup failed for \(window.id, privacy: .public): \(error.localizedDescription, privacy: .public)"
                        )
                    }
                    return
                }
            }
        } catch {
            logger.warning(
                "Recurrence fingerprint failed for \(window.id, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
    }

    private func scheduleConfirmedRecurrenceLearning(
        for window: AdWindow,
        showId: String?,
        source: CatalogLearningSource,
        lifecycle: CatalogLearningLifecycle,
        expectedLearningGeneration: UInt64? = nil
    ) {
        recurrenceBackgroundWorkCount += 1
        Task { [weak self] in
            guard let self else { return }
            await self.learnConfirmedRecurrence(
                for: window,
                showId: showId,
                source: source,
                lifecycle: lifecycle,
                expectedLearningGeneration: expectedLearningGeneration
            )
            await self.finishRecurrenceBackgroundWork()
        }
    }

    private func finishRecurrenceBackgroundWork() {
        precondition(recurrenceBackgroundWorkCount > 0)
        recurrenceBackgroundWorkCount -= 1
#if DEBUG
        if recurrenceBackgroundWorkCount == 0 {
            let waiters = recurrenceBackgroundWorkWaitersForTesting
            recurrenceBackgroundWorkWaitersForTesting.removeAll()
            for waiter in waiters {
                waiter.resume()
            }
        }
#endif
    }

    private func recurrenceLearningIsCurrent(
        window: AdWindow,
        expectedGeneration: UInt64?
    ) -> Bool {
        guard !revokedLearningSources.contains(
            RecurrenceSourceKey(window)
        ) else {
            return false
        }
        // A generation is supplied only for delayed consumed-auto-skip
        // learning. Unlike explicit user confirmation, that authority is
        // contingent on the exact applied producer revision remaining live.
        // Re-check after every actor suspension so retirement, a terminal
        // state, or a same-ID material replacement retracts a partially
        // completed writer and cannot populate either recurrence store.
        guard let expectedGeneration else {
            return true
        }
        guard expectedGeneration == catalogLearningGeneration,
              let current = windows[window.id],
              current.decisionState == .applied
        else {
            return false
        }
        return AdWindowMaterialIdentity.sameProducerRevision(
            current.adWindow,
            window
        )
    }

    /// - Parameter negativeAttribution: playhead-1mq1.2.1. The evidence
    ///   partition for the reverted span, resolved by the caller BEFORE its
    ///   first suspension (see `revertNegativeAttribution`). It scopes the
    ///   FUZZY similarity sweep below — the one part of this method that can
    ///   reach rows the user never saw. The exact source tombstone and the
    ///   exact matched-entry revocation are unaffected: both are retractions
    ///   of a specific row this window is durably tied to, not labels applied
    ///   to audio.
    private func revokeRecurrenceEvidence(
        for window: AdWindow,
        showId: String?,
        source: CatalogRevocationSource,
        negativeAttribution: RevertEvidencePartition.Partition
    ) async throws {
        let sourceKey = RecurrenceSourceKey(window)
        pendingCatalogLearning.removeValue(forKey: sourceKey)
        revokedLearningSources.insert(sourceKey)
        guard adCatalogStore != nil || repeatedAdCache != nil else {
            return
        }
        // The caller captures the active episode's show before its first
        // suspension. A persisted matched-show field is only diagnostic, and
        // reading activePodcastId here would let an episode switch redirect a
        // delayed correction into the replacement show's catalog/cache.
        // Missing captured identity still permits exact source/row revocation
        // and fails closed on fuzzy scope.
        //
        // playhead-o4qr, ACCEPT THE RECEIPT / REFUSE THE LEARNING: that last
        // sentence is what makes this call safe on an ANONYMOUS correction
        // (`showId == nil`), and why it is deliberately still made rather than
        // skipped. `normalizedCatalogShowId(nil)` is nil, and both stores then
        // take only their show-FREE branches: `AdCatalogStore.revoke` requires
        // a canonical show for BOTH the matched-entry and the
        // fingerprint-similarity targets, and
        // `RepeatedAdCacheService.revokeMatches` drops show+fingerprint scope
        // unless both are present. What is left is the exact source-window
        // tombstone keyed on this asset/window — not show-keyed, not
        // attributable to any other episode, and a RETRACTION of learning
        // rather than a write of it.
        //
        // Skipping the whole call for an anonymous correction would be the
        // strictly worse reading of the rule: the in-memory retraction above
        // (`pendingCatalogLearning` / `revokedLearningSources`) is what stops a
        // delayed consumed-skip learner from writing catalog evidence for the
        // very span the user just vetoed — and THAT write would be keyed to
        // the live show. Refusing the retraction would manufacture the
        // contamination the rule forbids.
        let matchingShowId = normalizedCatalogShowId(showId)
        var matchingFingerprint: AcousticFingerprint?
        var matchingRepeatedFingerprint: RepeatedAdFingerprint?
        var firstFailure: Error?
        do {
            let allFeatureWindows = try await store.fetchFeatureWindows(
                assetId: window.analysisAssetId,
                from: window.startTime,
                to: window.endTime
            )
            // playhead-1mq1.2.1: PER-SUBINTERVAL, NEVER WHOLESALE. The
            // fingerprints below drive `compatibleMatches` /
            // `RepeatedAdCacheService.revokeMatches`, which delete every row in
            // this show that resembles them. Derived over a MIXED window that
            // is mostly a real ad, they revoke the legitimately learned copy
            // for that ad. Only feature windows lying ENTIRELY inside an
            // attributable subspan may contribute, so material that straddles
            // the evidenced ad's edge cannot leak in. A CLEAN partition
            // attributes the whole span and this filter is the identity.
            let featureWindows = allFeatureWindows.filter { feature in
                negativeAttribution.allowsNegativeAttribution(
                    startTime: feature.startTime,
                    endTime: feature.endTime
                )
            }
            let fingerprint = AcousticFingerprint.fromFeatureWindows(
                featureWindows
            )
            if !fingerprint.isZero {
                matchingFingerprint = fingerprint
            }
            let repeatedFingerprint = RepeatedAdFingerprint.from(
                featureWindows: featureWindows
            )
            if !repeatedFingerprint.isZero {
                matchingRepeatedFingerprint = repeatedFingerprint
            }
        } catch {
            firstFailure = error
            logger.warning("Recurrence revocation fingerprint fetch failed")
        }
        if let adCatalogStore {
            do {
                let matchedEntryId = window.catalogMatchedEntryId
                    .flatMap(UUID.init)
                _ = try await adCatalogStore.revoke(
                    matchedEntryId: matchedEntryId,
                    sourceAssetId: window.analysisAssetId,
                    sourceWindowId: window.id,
                    sourceStartTime: window.startTime,
                    sourceEndTime: window.endTime,
                    source: source,
                    matchingFingerprint: matchingFingerprint,
                    showId: matchingShowId
                )
            } catch {
                if firstFailure == nil { firstFailure = error }
                logger.warning("Catalog revocation failed")
            }
        }
        if let repeatedAdCache {
            do {
                _ = try await repeatedAdCache.revokeMatches(
                    showId: matchingShowId,
                    fingerprint: matchingRepeatedFingerprint,
                    sourceAssetId: window.analysisAssetId,
                    sourceWindowId: window.id,
                    sourceStartTime: window.startTime,
                    sourceEndTime: window.endTime,
                    source: source
                )
            } catch {
                if firstFailure == nil { firstFailure = error }
                logger.warning("Repeated-ad revocation failed")
            }
        }
        if let firstFailure {
            throw firstFailure
        }
    }

    /// Captures the active episode transaction for a deferred user action.
    /// Episode identity alone is insufficient because replaying the same
    /// canonical episode advances the lifecycle without changing its ID.
    func episodeLifecycleGenerationSnapshot() -> UInt64 {
        episodeLifecycleGeneration
    }

    /// Records a seek only while the episode transaction captured by the
    /// caller is still active. This closes the actor-hop race where a banner
    /// action passed the runtime's episode check, then arrived here after
    /// `endEpisode`/`beginEpisode` installed a replacement lifecycle.
    @discardableResult
    func recordUserSeek(
        to time: TimeInterval,
        ifEpisodeLifecycleGeneration expectedGeneration: UInt64,
        userSeekOperationGeneration operationGeneration: UInt64? = nil
    ) async -> Bool {
        guard time.isFinite,
              time >= 0,
              episodeLifecycleGeneration == expectedGeneration else {
            return false
        }
        if let operationGeneration {
            guard operationGeneration
                    >= latestUserSeekOperationGeneration
            else {
                return false
            }
            latestUserSeekOperationGeneration = operationGeneration
        }
        #if DEBUG
        await userSeekEffectHookForTesting?()
        #endif
        guard episodeLifecycleGeneration == expectedGeneration,
              operationGeneration == nil
                || latestUserSeekOperationGeneration
                    == operationGeneration
        else {
            return false
        }
        recordUserSeek(to: time)
        return true
    }

    #if DEBUG
    func _setUserSeekEffectHookForTesting(
        _ hook: (@Sendable () async -> Void)?
    ) {
        userSeekEffectHookForTesting = hook
    }

    func _currentPlayheadTimeForTesting() -> TimeInterval {
        currentPlayheadTime
    }
    #endif

    /// Immediately retires only the in-memory cue for a banner Listen action.
    /// Durable state and the weak trust signal remain owned by
    /// `AdDetectionService.recordListenRewind`, avoiding duplicate feedback.
    @discardableResult
    func retireLiveSkipForListen(
        windowId: String,
        analysisAssetId expectedAssetId: String,
        startTime expectedStartTime: Double,
        endTime expectedEndTime: Double,
        podcastId expectedPodcastId: String,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64,
        ifWindowMaterialRevisionToken expectedMaterialToken: String
    ) -> Bool {
        retireLiveSkipRevisionForListen(
            windowId: windowId,
            analysisAssetId: expectedAssetId,
            startTime: expectedStartTime,
            endTime: expectedEndTime,
            podcastId: expectedPodcastId,
            ifCurrentEpisodeId: expectedEpisodeId,
            ifPlaybackLifecycleGeneration: expectedPlaybackGeneration,
            ifWindowMaterialRevisionToken: expectedMaterialToken
        ) != nil
    }

    /// Retire the cue and return the exact producer material represented by
    /// the banner. The durable Listen transaction uses this revision to reject
    /// a same-ID replacement rather than applying feedback to unseen content.
    func retireLiveSkipRevisionForListen(
        windowId: String,
        analysisAssetId expectedAssetId: String,
        startTime expectedStartTime: Double,
        endTime expectedEndTime: Double,
        podcastId expectedPodcastId: String,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64,
        ifWindowMaterialRevisionToken expectedMaterialToken: String
    ) -> AdWindow? {
        let validatedShow = exactFeedbackShowIdentity(
            requested: expectedPodcastId
        )
        guard activeEpisodeId == expectedEpisodeId,
              activePlaybackLifecycleGeneration
                == expectedPlaybackGeneration,
              validatedShow.isValid,
              validatedShow.showId == expectedPodcastId,
              let managed = windows[windowId],
              managed.adWindow.analysisAssetId == expectedAssetId,
              managed.snappedStart == expectedStartTime,
              managed.snappedEnd == expectedEndTime,
              bannerMaterialRevisionToken(for: managed)
                == expectedMaterialToken
        else {
            return nil
        }
        if pendingListenRewindWindowIds.contains(windowId) {
            return managed.adWindow
        }
        guard var managed = windows[windowId],
              managed.decisionState != .reverted,
              managed.decisionState != .suppressed
        else {
            return nil
        }

        let producerRevision = managed.adWindow
        managed.decisionState = .reverted
        managed.cueActive = false
        windows[windowId] = managed
        pendingListenRewindWindowIds.insert(windowId)
        logDecision(
            managed: managed,
            decision: .reverted,
            reason: "User tapped Listen (live cue retirement)"
        )
        evaluateAndPush()
        return producerRevision
    }

    /// Validate a correction caller's show identity against the active
    /// orchestrator lifecycle. A completely showless lifecycle remains valid
    /// for device-local exact-span corrections; once either side has a show,
    /// both values are mandatory, canonical, and exactly equal.
    ///
    /// playhead-o4qr — WHAT A FAILURE MEANS, because the two consumers read it
    /// differently and the difference is the whole contract:
    ///
    ///   • The four CORRECTION seams (`recordListenRevert`,
    ///     `revertByTimeRange`, `denyAutoSkippedBanner`, `revertWindow`) treat
    ///     `showId` as the LEARNING KEY, not as an admission gate. A nil
    ///     `showId` — whether from a showless lifecycle or from an outright
    ///     validation failure — still commits the user's durable receipt and
    ///     still returns true; it withholds only the show-keyed effects.
    ///     ACCEPT THE RECEIPT, REFUSE THE LEARNING.
    ///
    ///   • `retireLiveSkipRevisionForListen` keeps it as a hard gate, and that
    ///     is not an inconsistency: it takes a NON-optional `podcastId` from
    ///     the banner it is retiring and asserts `showId == expectedPodcastId`,
    ///     so a failure there means the caller is describing material this
    ///     lifecycle does not own. It writes no receipt and performs no
    ///     learning, so there is nothing to accept — refusing is the only
    ///     available behaviour.
    ///
    /// Note the deliberate asymmetry in the return value: an INVALID result
    /// always carries `showId == nil`, so callers never need to consult
    /// `isValid` to make the learning decision. `isValid` distinguishes
    /// "showless but agreed" from "disagreed" for the gate consumer only.
    private func exactFeedbackShowIdentity(
        requested requestedShowId: String?
    ) -> (isValid: Bool, showId: String?) {
        switch (activePodcastId, requestedShowId) {
        case (nil, nil):
            return (true, nil)
        case let (active?, requested?):
            guard normalizedCatalogShowId(active) == active,
                  normalizedCatalogShowId(requested) == requested,
                  requested == active else {
                return (false, nil)
            }
            return (true, active)
        default:
            return (false, nil)
        }
    }

    /// Marks the durable Listen receipt complete. The in-memory window remains
    /// reverted; only the retry reservation is released.
    func completeLiveSkipRetirementForListen(
        windowId: String,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64
    ) async {
        guard activeEpisodeId == expectedEpisodeId,
              activePlaybackLifecycleGeneration
                == expectedPlaybackGeneration
        else {
            return
        }
        let sourceShowId = activePodcastId
        pendingListenRewindWindowIds.remove(windowId)
        if let managed = windows[windowId] {
            let negativeAttribution = revertNegativeAttribution(
                for: managed.adWindow
            )
            do {
                try await revokeRecurrenceEvidence(
                    for: managed.adWindow,
                    showId: sourceShowId,
                    source: .listenRevert,
                    negativeAttribution: negativeAttribution
                )
            } catch {
                // `AdDetectionService.recordListenRewind` already completed
                // the mandatory pre-commit revocation. This duplicate,
                // in-memory retirement pass is best-effort and idempotent.
                logger.warning("Live Listen retirement revocation failed")
            }
        }
    }

    /// Record that the user tapped "Listen" to revert a skip.
    /// Also signals the trust engine (if wired) as a false-skip.
    @discardableResult
    func recordListenRevert(
        windowId: String,
        podcastId: String? = nil
    ) async -> Bool {
        // playhead-o4qr: ACCEPT THE RECEIPT, REFUSE THE LEARNING.
        //
        // An unusable show identity — absent, empty, non-canonical, or
        // disagreeing with the active episode — is NOT grounds to drop the
        // gesture. The durable `CorrectionEvent` is a record of what the
        // listener said; it is scoped to an exact asset + time span and needs
        // no show to be meaningful, so it is committed and the gesture reports
        // success. What an unusable identity DOES forbid is every show-KEYED
        // effect below (trust penalty, hard-negative bank, per-show threshold
        // controller, show-scoped recurrence revocation): those are
        // unattributable without a show, and letting them fall back to the
        // live `activePodcastId` — or to a null/global bucket — is exactly the
        // cross-contamination this bead exists to prevent. The dangerous part
        // was never the receipt.
        //
        // `exactFeedbackShowIdentity` already yields `showId == nil` for every
        // unusable case, so `sourceShowId` is the ONE predicate the effects
        // below test. Read `sourceShowId == nil` as "anonymous correction".
        //
        // Stated-invariant change more than a behaviour change: production
        // reaches this seam only through `PlayheadRuntime`, which guards on
        // `hasExactCurrentPodcastIdentity` upstream and always supplies the
        // exact show id.
        let validatedShow = exactFeedbackShowIdentity(requested: podcastId)
        guard let sourceAssetId = activeAssetId,
              let sourceEpisodeId = activeEpisodeId
        else {
            return false
        }
        let sourceLifecycleGeneration = episodeLifecycleGeneration
        let sourceShowId = validatedShow.showId
        guard let requestedManaged = windows[windowId],
              requestedManaged.adWindow.analysisAssetId == sourceAssetId,
              requestedManaged.decisionState != .reverted,
              requestedManaged.decisionState != .suppressed,
              requestedManaged.snappedStart.isFinite,
              requestedManaged.snappedEnd.isFinite,
              requestedManaged.snappedStart >= 0,
              requestedManaged.snappedEnd > requestedManaged.snappedStart
        else {
            return false
        }

        // playhead-1mq1.2.1: resolve what this revert may teach while the actor
        // is still synchronous. Every learning effect below — the fuzzy
        // recurrence sweep before persistence and the hard-negative ingest
        // after it — reads this captured partition, so a replacement episode's
        // evidence catalog can never redirect the answer.
        let sourceNegativeAttribution = revertNegativeAttribution(
            for: requestedManaged.adWindow
        )

        // Revoke recurrence material before making the correction terminal.
        // The source/creative tombstones prevent a delayed learner from
        // reopening the gap. If a separate store fails, leave the durable
        // window and UI retryable; successful earlier tombstones are
        // conservative and idempotent on retry.
        do {
            try await revokeRecurrenceEvidence(
                for: requestedManaged.adWindow,
                showId: sourceShowId,
                source: .listenRevert,
                negativeAttribution: sourceNegativeAttribution
            )
        } catch {
            logger.warning("Listen revert revocation failed")
            return false
        }
        let correction: CorrectionEvent?
        do {
            if correctionStore != nil {
                correction = try makeManualCorrectionVetoEvent(
                    startTime: requestedManaged.snappedStart,
                    endTime: requestedManaged.snappedEnd,
                    assetId: sourceAssetId,
                    podcastId: sourceShowId,
                    source: .listenRevert,
                    windowId: windowId,
                    detectionProjection:
                        ExplicitFeedbackDetectionProjection(
                            requestedManaged.adWindow
                        )
                )
            } else {
                correction = nil
            }
            if let barrier = feedbackPersistenceBarrierForTesting {
                await barrier()
            }
            guard let wasNewlyInserted =
                    try await store.persistRevertedAdWindowsIfCurrent(
                        expectedWindows: [requestedManaged.adWindow],
                        analysisAssetId: sourceAssetId,
                        expectedPodcastId: sourceShowId,
                        correction: correction,
                        expectedCorrectionSource: .listenRevert
                    )
            else {
                return false
            }
            if let correction {
                schedulePostCommitCorrectionLearning(
                    correction,
                    wasNewlyInserted: wasNewlyInserted
                )
            }
        } catch {
            logger.warning("Listen revert persistence failed")
            return false
        }

        if let barrier = revertPersistenceBarrierForTesting {
            await barrier()
        }

        // playhead-i08e: the calibration effects below belong to the CAPTURED
        // source show. Each is fire-and-forget over values read before the
        // first suspension, so an episode switch that landed during the
        // store/trust hops must not discard them — by that point this
        // gesture's durable receipt has ALREADY been committed, so dropping
        // the matching trust penalty / hard negative / controller sample would
        // leave trust and corrections permanently out of step. `revertWindow`
        // states the same policy; only the live cue state below is
        // lifecycle-scoped, which is why the ownership check that follows is a
        // scoped `if` around the cue work rather than an early return.

        // Retire live state only if the exact source lifecycle and producer
        // revision still own the window after the durable transaction.
        if activeAssetId == sourceAssetId,
           activeEpisodeId == sourceEpisodeId,
           episodeLifecycleGeneration == sourceLifecycleGeneration,
           var managed = windows[windowId],
           managed.decisionState != .reverted,
           managed.decisionState != .suppressed,
           AdWindowMaterialIdentity.sameProducerRevision(
               managed.adWindow,
               requestedManaged.adWindow
           ) {
            managed.decisionState = .reverted
            managed.cueActive = false
            windows[windowId] = managed
            logDecision(
                managed: managed,
                decision: .reverted,
                reason: "User tapped Listen"
            )
            evaluateAndPush()
        }

        if let sourceShowId, let trustService {
            // playhead-gard: blame the DETECTOR that drew this span, weighted
            // by how certain its extent was.
            await trustService.recordFalseSkipSignal(
                podcastId: sourceShowId,
                attributions: [
                    vetoAttribution(for: requestedManaged.adWindow)
                ]
            )
        }

        // playhead-xsdz.9: a Listen revert is a CONFIRMED false positive —
        // ingest the wrongly-flagged window's ad-copy text as a hard negative
        // so future episodes with the same copy are suppressed. No-op when no
        // bank is wired — and a bank is wired ONLY when the
        // `crossEpisodeMemoryEnabled` feature flag is on (see PlayheadRuntime),
        // so this is inert in the flag-OFF production default. Also a no-op
        // when `sourceShowId` is nil (playhead-o4qr: an anonymous correction
        // must not become a NULL-show negative that every show reads back).
        ingestNegativeFingerprint(
            text: requestedManaged.adWindow.evidenceText,
            podcastId: sourceShowId,
            negativeAttribution: sourceNegativeAttribution
        )

        // playhead-xsdz.11: a Listen revert of an auto-skip is the canonical
        // FALSE-POSITIVE signal — RAISE this show's auto-skip threshold (be more
        // conservative). No-op when no controller store is wired (flag-OFF
        // production default).
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: sourceShowId
        )
        return true
    }

    /// Revert all managed windows overlapping the given time range.
    /// Used by the "Not an ad" banner and "This isn't an ad" popover paths,
    /// which identify the ad by its time span rather than a specific windowId.
    ///
    /// playhead-hygc.1.8: also reverts overlapping `suggestWindows` (markOnly
    /// AdWindows surfaced as suggest-tier banners). Prior to this change
    /// `revertByTimeRange` only iterated `windows` (the auto-skip eligible
    /// dictionary), leaving algorithmic markOnly entries that the user has
    /// explicitly said weren't ads still visible on the timeline / available
    /// to be promoted via `acceptSuggestedSkip`. The May 6 dogfood eval found
    /// 8 of 12 falsePositive corrections were against markOnly windows — the
    /// suggest tier is the user-facing surface for borderline ads, so it must
    /// honor user vetoes the same way the auto-skip surface does.
    @discardableResult
    func revertByTimeRange(
        start: Double,
        end: Double,
        podcastId: String?
    ) async -> Bool {
        await revertByTimeRange(
            start: start,
            end: end,
            analysisAssetId: activeAssetId,
            podcastId: podcastId,
            ifCurrentEpisodeId: activeEpisodeId,
            ifPlaybackLifecycleGeneration: nil
        )
    }

    /// Playback-bound form used by deferred UI gestures. The complete source
    /// identity is captured when the sheet/card is created, and the store
    /// validates every exact producer revision in one transaction.
    @discardableResult
    func revertByTimeRange(
        start: Double,
        end: Double,
        analysisAssetId expectedAssetId: String?,
        podcastId: String?,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64?,
        correctionSpan: DecodedSpan? = nil
    ) async -> Bool {
        // playhead-o4qr: ACCEPT THE RECEIPT, REFUSE THE LEARNING — see the long
        // form above `recordListenRevert`'s call to
        // `exactFeedbackShowIdentity`. An unusable show identity leaves
        // `sourceShowId == nil`, which is the single predicate every show-keyed
        // effect in this seam already tests; it does not cancel the gesture.
        let validatedShow = exactFeedbackShowIdentity(requested: podcastId)
        // playhead-zxqj: the argument check and the context check are separated
        // so the audit row can say WHICH refused. They were one `guard`, and a
        // stale sheet (benign, expected) rendered identically to a malformed
        // range (a defect).
        guard start.isFinite,
              end.isFinite,
              start >= 0,
              end > start,
              let expectedAssetId,
              let expectedEpisodeId
        else {
            noteManualVetoOutcome(
                .refusedInvalidRequest,
                analysisAssetId: expectedAssetId ?? activeAssetId ?? "",
                start: start,
                end: end
            )
            return false
        }
        guard activeAssetId == expectedAssetId,
              activeEpisodeId == expectedEpisodeId,
              expectedPlaybackGeneration == nil
                || activePlaybackLifecycleGeneration
                    == expectedPlaybackGeneration
        else {
            noteManualVetoOutcome(
                .refusedStaleContext,
                analysisAssetId: expectedAssetId,
                start: start,
                end: end
            )
            return false
        }
        if let correctionSpan {
            guard correctionSpan.assetId == expectedAssetId,
                  !correctionSpan.id.isEmpty,
                  correctionSpan.firstAtomOrdinal >= 0,
                  correctionSpan.lastAtomOrdinal
                    >= correctionSpan.firstAtomOrdinal,
                  RecurrenceMaterialIdentity.canonicalTimeBitPattern(
                      correctionSpan.startTime
                  ) == RecurrenceMaterialIdentity.canonicalTimeBitPattern(
                      start
                  ),
                  RecurrenceMaterialIdentity.canonicalTimeBitPattern(
                      correctionSpan.endTime
                  ) == RecurrenceMaterialIdentity.canonicalTimeBitPattern(
                      end
                  )
            else {
                noteManualVetoOutcome(
                    .refusedInvalidRequest,
                    analysisAssetId: expectedAssetId,
                    start: start,
                    end: end
                )
                return false
            }
        }
        // playhead-u45d: THE VETO'S TARGETS COME FROM PERSISTED ANALYSIS, NOT
        // ONLY FROM THE LIVE SESSION.
        //
        // The listener is looking at the transcript, which renders persisted
        // `decoded_spans`. This seam used to resolve targets exclusively from
        // `windows` / `suggestWindows` — live playback state with a completely
        // different lifetime — and returned `false` when that came up empty.
        // The empty case is not exotic: `beginEpisode` only hydrates persisted
        // rows at `confidence >= 0.7`, so every mark-only-tier window is
        // displayed and untracked; so is any row belonging to an episode that
        // is not the one playing. The listener could therefore only dismiss an
        // ad the orchestrator happened to be holding at that moment, and the
        // rest failed silently after four deliberate taps.
        //
        // The read happens HERE, before the synchronous target/partition
        // block, so that block keeps the playhead-1mq1.2.1 property it was
        // given: the target set and the evidence partition are computed
        // together, with no suspension between them. A read failure degrades
        // to the live-only behaviour rather than cancelling the gesture.
        var persistedWindows: [AdWindow] = []
        var didReadPersistedWindows = true
        do {
            persistedWindows = try await store.fetchAdWindows(
                assetId: expectedAssetId
            )
        } catch {
            didReadPersistedWindows = false
            logger.warning(
                "Manual veto: persisted ad-window read failed; falling back to live session targets"
            )
        }

        let sourceLifecycleGeneration = episodeLifecycleGeneration
        let sourceShowId = validatedShow.showId
        let managedRevertTargets: [(id: String, managed: ManagedWindow)] =
            windows.compactMap { id, managed in
                guard managed.adWindow.analysisAssetId == expectedAssetId,
                      managed.decisionState != .reverted,
                      managed.decisionState != .suppressed,
                      managed.snappedStart < end,
                      managed.snappedEnd > start
                else {
                    return nil
                }
                return (id, managed)
            }
        let suggestRevertTargets: [(id: String, window: AdWindow)] =
            suggestWindows.compactMap { id, suggested in
                guard suggested.analysisAssetId == expectedAssetId,
                      suggested.startTime < end,
                      suggested.endTime > start
                else {
                    return nil
                }
                return (id, suggested)
            }
        // playhead-zxqj: recorded on every audit row so a refusal that happened
        // WHILE the session was holding windows over the range stays
        // distinguishable from one that happened over untracked material.
        let liveTargetCount =
            managedRevertTargets.count + suggestRevertTargets.count

        // playhead-zxqj: THE DURABLE TARGET SET IS WHAT THE STORE CAN ACCEPT,
        // AND NOTHING ELSE.
        //
        // `persistRevertedAdWindowsIfCurrent` is ALL-OR-NOTHING: one expected
        // row it will not take — a row that is already `reverted` or
        // `suppressed`, a row that no longer exists, a row whose material has
        // moved — rolls the WHOLE transaction back and the gesture returns
        // `false`. Until this bead the live dictionaries fed that transaction
        // directly, so ONE stale live entry made every "Dismiss ad" over any
        // range that touched it fail, including every other window the gesture
        // could have reverted. That is exactly the shape Dan reported: a window
        // he had already acted on made the REST of the region undismissable.
        //
        // The live entry is not authoritative about durable state and never
        // was. `suggestWindows` is not filtered by decision state at all, and a
        // committed revert can leave an entry behind (see the cleanup below),
        // so "the session is holding this id" says nothing about whether the
        // row can be reverted. The store's own row does, and it is already read
        // above. So the live dictionaries now decide only what LIVE state to
        // retire; the transaction is built from the persisted rows.
        //
        // This is not a new weakening of the CAS fence. `persistRevertedAd
        // WindowsIfCurrent` compares each expected row against the store row of
        // the same id, and playhead-u45d ALREADY passed the store's own row as
        // `expected` for every id the live session was not holding — the great
        // majority of them. What changes is that the ids the session happens to
        // hold are treated the same way as the ids it does not, rather than
        // being the one class that can poison the gesture. The identity this
        // gesture is fenced on is the episode/asset/lifecycle triple checked
        // above plus the exact time range pinned by `correctionSpan`.
        //
        // A READ FAILURE STILL DEGRADES TO LIVE-ONLY, which is the behaviour
        // playhead-u45d's comment promises two paragraphs up: with no persisted
        // rows to build from, refusing would cancel a gesture that used to
        // work.
        var exactTargetsByID: [String: AdWindow] = [:]
        if !didReadPersistedWindows {
            // playhead-95cf: the SAME rule on the degraded path. This branch
            // runs only when the persisted read failed, which is rare — and
            // rare is exactly where a silent loss is worst, because nobody is
            // watching. A hand-mark held in the live session is still a
            // hand-mark, and a sweep may not take it here either.
            for target in managedRevertTargets
            where Self.rangeVetoMaySweepUp(
                target.managed.adWindow, requestedStart: start, requestedEnd: end
            ) {
                exactTargetsByID[target.id] = target.managed.adWindow
            }
            for target in suggestRevertTargets
            where Self.rangeVetoMaySweepUp(
                target.window, requestedStart: start, requestedEnd: end
            ) {
                if let existing = exactTargetsByID[target.id],
                   !AdWindowMaterialIdentity.sameProducerRevision(
                       existing,
                       target.window
                   ) {
                    noteManualVetoOutcome(
                        .refusedLiveTargetConflict,
                        analysisAssetId: expectedAssetId,
                        start: start,
                        end: end,
                        liveTargets: managedRevertTargets.count
                            + suggestRevertTargets.count
                    )
                    return false
                }
                exactTargetsByID[target.id] = target.window
            }
        }

        // playhead-u45d: fold in every persisted row overlapping the range.
        //
        // The material pre-filter is not decoration. That store call validates
        // every expected row and returns nil — failing the WHOLE transaction —
        // if any one of them is malformed, so admitting a corrupt persisted
        // row here would turn a veto that used to succeed into a refusal.
        for window in persistedWindows
        where exactTargetsByID[window.id] == nil
            && window.analysisAssetId == expectedAssetId
            && !window.id.isEmpty
            && Self.userVetoRevertibleDecisionStates
                .contains(window.decisionState)
            && window.startTime.isFinite
            && window.endTime.isFinite
            && window.confidence.isFinite
            && window.startTime >= 0
            && window.endTime > window.startTime
            && (0...1).contains(window.confidence)
            && window.startTime < end
            && window.endTime > start
            // playhead-95cf: a hand-mark is never collateral. See
            // `rangeVetoMaySweepUp` — it is admitted only when this gesture
            // NAMES it, never when a wider range happens to cover it.
            && Self.rangeVetoMaySweepUp(
                window, requestedStart: start, requestedEnd: end
            ) {
            exactTargetsByID[window.id] = window
        }

        let exactTargets = exactTargetsByID.values.sorted {
            $0.id < $1.id
        }
        // playhead-u45d: NO LONGER A REFUSAL. A highlighted span with no
        // `ad_window` behind it — spans and windows are different populations,
        // written by different stages — has no row to revert but still carries
        // a correction worth recording. See the window-less branch below.
        let correctionOwner = exactTargets.first

        // playhead-1mq1.2.1: partition every target before the first
        // suspension, excluding this transaction's OWN window set — a window
        // the same gesture is reverting must not vouch for its neighbour.
        let revertedWindowIds = Set(exactTargets.map(\.id))
        let revocationTargets = exactTargets.map { target in
            (
                window: target,
                negativeAttribution: revertNegativeAttribution(
                    span: RevertEvidencePartition.Interval(
                        startTime: target.startTime,
                        endTime: target.endTime
                    ),
                    analysisAssetId: target.analysisAssetId,
                    excludingWindowIds: revertedWindowIds
                )
            )
        }

        do {
            for target in revocationTargets {
                try await revokeRecurrenceEvidence(
                    for: target.window,
                    showId: sourceShowId,
                    source: .manualVeto,
                    negativeAttribution: target.negativeAttribution
                )
            }
        } catch {
            logger.warning("Manual veto revocation failed")
            noteManualVetoOutcome(
                .refusedRevocationFailed,
                analysisAssetId: expectedAssetId,
                start: start,
                end: end,
                liveTargets: liveTargetCount
            )
            return false
        }

        let correction: CorrectionEvent?
        do {
            if correctionStore != nil {
                correction = try makeManualCorrectionVetoEvent(
                    startTime: start,
                    endTime: end,
                    assetId: expectedAssetId,
                    podcastId: sourceShowId,
                    source: .manualVeto,
                    windowId: correctionOwner?.id,
                    additionalWindowIds: correctionOwner == nil
                        ? nil
                        : exactTargets.dropFirst().map(\.id),
                    detectionProjection: correctionOwner.map {
                        ExplicitFeedbackDetectionProjection($0)
                    },
                    correctionProvenance:
                        correctionSpan?.anchorProvenance ?? []
                )
            } else {
                correction = nil
            }
            if let barrier = feedbackPersistenceBarrierForTesting {
                await barrier()
            }
            let wasNewlyInserted: Bool
            if exactTargets.isEmpty {
                // playhead-u45d: nothing to revert, everything to record. The
                // correction IS the durable artifact here — it is what
                // `BackfillEvidenceFusion` reads to suppress this material on
                // the next pass, and what the transcript's own read consults
                // to stop highlighting it.
                //
                // Without a correction store there is no `CorrectionEvent` at
                // all (the existing policy a few lines above), so this gesture
                // has nothing durable to write and must say so rather than
                // report a success it did not achieve.
                //
                // AND THERE MUST BE SOMETHING TO CORRECT. This seam cannot see
                // the transcript, so "a highlighted span with no window" and
                // "a tap on audio the app never flagged" arrive here
                // identically. A persisted `decoded_spans` row overlapping the
                // range is exactly what distinguishes them — it IS what the
                // transcript drew. Without one there is no claim of ours to
                // retract, and reporting success would be the same lie in the
                // other direction. Read includes already-vetoed rows so a
                // repeat veto stays idempotently truthful rather than
                // regressing to a refusal on the second tap.
                let overlappingSpans = (
                    try? await store.fetchDecodedSpansIncludingUserVetoed(
                        assetId: expectedAssetId
                    )
                ) ?? []
                guard overlappingSpans.contains(where: {
                    $0.startTime < end && $0.endTime > start
                }) else {
                    noteManualVetoOutcome(
                        .refusedNothingToCorrect,
                        analysisAssetId: expectedAssetId,
                        start: start,
                        end: end,
                        liveTargets: liveTargetCount
                    )
                    return false
                }
                guard let correction,
                      let inserted =
                        try await store
                            .persistUserVetoCorrectionWithoutWindows(
                                correction: correction,
                                analysisAssetId: expectedAssetId,
                                expectedPodcastId: sourceShowId
                            )
                else {
                    noteManualVetoOutcome(
                        .refusedDurableWriteRejected,
                        analysisAssetId: expectedAssetId,
                        start: start,
                        end: end,
                        liveTargets: liveTargetCount
                    )
                    return false
                }
                wasNewlyInserted = inserted
            } else {
                guard let inserted =
                        try await store.persistRevertedAdWindowsIfCurrent(
                            expectedWindows: exactTargets,
                            analysisAssetId: expectedAssetId,
                            expectedPodcastId: sourceShowId,
                            correction: correction
                        )
                else {
                    noteManualVetoOutcome(
                        .refusedDurableWriteRejected,
                        analysisAssetId: expectedAssetId,
                        start: start,
                        end: end,
                        revertedWindows: exactTargets.count,
                        liveTargets: liveTargetCount
                    )
                    return false
                }
                wasNewlyInserted = inserted
            }
            if let correction {
                schedulePostCommitCorrectionLearning(
                    correction,
                    wasNewlyInserted: wasNewlyInserted
                )
            }
        } catch {
            logger.warning("Manual veto persistence failed")
            noteManualVetoOutcome(
                .refusedPersistenceFailed,
                analysisAssetId: expectedAssetId,
                start: start,
                end: end,
                liveTargets: liveTargetCount
            )
            return false
        }

        if let barrier = revertPersistenceBarrierForTesting {
            await barrier()
        }

        // playhead-i08e: everything from here to `return true` is durable or
        // fire-and-forget work owed to the CAPTURED show, and none of it is
        // gated on the live lifecycle. `sourceLifecycleIsCurrent` scopes ONLY
        // the live cue/banner state a replacement episode now owns, as a
        // wrapping `if` rather than an early return, so a replacement landing
        // during the store hop above can no longer discard the gesture's trust
        // penalty or its per-show controller sample after its receipt has
        // already committed. Note this gate is independent of `trustService`
        // wiring, so it also holds for anonymous reverts that never reach the
        // trust engine.
        let revertedManagedAny = !managedRevertTargets.isEmpty
        let sourceLifecycleIsCurrent =
            activeAssetId == expectedAssetId
            && activeEpisodeId == expectedEpisodeId
            && episodeLifecycleGeneration == sourceLifecycleGeneration
            && (
                expectedPlaybackGeneration == nil
                    || activePlaybackLifecycleGeneration
                        == expectedPlaybackGeneration
            )

        if sourceLifecycleIsCurrent {
            // playhead-zxqj: the managed twin of the suggest rule below, and
            // the same argument. A producer update landing during the store
            // hop made this `continue`, so the live window kept a live CUE for
            // a span whose durable row now reads `reverted` — the app would go
            // on skipping audio the listener had just said was not an ad,
            // which is the "a user mark outranks an inferred one in BOTH
            // directions" rule pointed the wrong way. The revision check is
            // retained for the ids the transaction did not cover.
            for (id, expectedManaged) in managedRevertTargets {
                guard var managed = windows[id],
                      managed.decisionState != .reverted,
                      managed.decisionState != .suppressed,
                      revertedWindowIds.contains(id)
                        || AdWindowMaterialIdentity.sameProducerRevision(
                            managed.adWindow,
                            expectedManaged.adWindow
                        )
                else {
                    continue
                }
                managed.decisionState = .reverted
                managed.cueActive = false
                windows[id] = managed
                emitBannerRetirement(windowId: id)

                logDecision(
                    managed: managed,
                    decision: .reverted,
                    reason: "User correction: not an ad (time range)"
                )
            }

            // Mark-only windows use a separate UI dictionary.
            //
            // playhead-zxqj: A COMMITTED REVERT IS TERMINAL FOR THE PRODUCER
            // ID, not merely for the revision that was on screen. This loop
            // used to `continue` whenever the live entry no longer matched the
            // revision the gesture captured — which is precisely what happens
            // when a producer update lands during the store hop — and the
            // entry then survived with its durable row already `reverted`. Two
            // consequences, both field-visible: the listener could be offered a
            // Yes/No card for a window they had just dismissed, and (before the
            // target-set change above) every later dismiss touching that range
            // was refused outright.
            //
            // The rule applied here is the one this actor already states for
            // managed windows in `revertedProducerWindowIds`: "persisted
            // `.reverted` producer rows represent an explicit user answer and
            // therefore remain terminal for the whole producer ID, even if a
            // stale delivery changes other material." `declineSuggestedSkip`
            // has always worked this way. The revision check is retained for
            // the case the transaction did NOT cover — there the gesture has
            // established nothing about this id and must not silently drop a
            // live suggestion.
            for (id, expectedSuggestion) in suggestRevertTargets {
                guard let suggested = suggestWindows[id],
                      revertedWindowIds.contains(id)
                        || AdWindowMaterialIdentity.sameProducerRevision(
                            suggested,
                            expectedSuggestion
                        )
                else {
                    continue
                }
                emitBannerRetirement(windowId: id)
                suggestWindows.removeValue(forKey: id)
                suggestBanneredWindowIds.insert(id)
                vetoedSuggestWindowIds.insert(id)

                logger.info(
                    "Revert (suggest tier): id=\(id, privacy: .public) range=[\(suggested.startTime), \(suggested.endTime)]"
                )
            }

            evaluateAndPush()
        }

        // Signal trust engine once per committed user correction.
        //
        // playhead-u45d: `!exactTargets.isEmpty` was previously implied — the
        // seam returned early when no window was found, so this block was
        // unreachable without one. Now that a window-less veto commits its
        // correction and continues, the condition has to be stated. It is the
        // R10 pin's invariant and it is the right one on the merits: the trust
        // score measures how often the SKIP SURFACE was wrong, and a correction
        // over material that never produced a window never risked a skip. The
        // correction is still recorded; only the trust penalty is withheld —
        // the same ACCEPT THE RECEIPT, REFUSE THE LEARNING split playhead-o4qr
        // made one axis over.
        if !exactTargets.isEmpty, let sourceShowId, let trustService {
            // playhead-gard: a range veto can retract windows from several
            // detector classes at once. Every class it touched is named; the
            // per-show scalar still moves exactly once, as it always did.
            let attributions = vetoAttributions(for: exactTargets)
            if revertedManagedAny {
                await trustService.recordFalseSkipSignal(
                    podcastId: sourceShowId,
                    attributions: attributions
                )
            } else {
                await trustService.recordWeakFalseSkipSignal(
                    podcastId: sourceShowId,
                    attributions: attributions
                )
            }
        }

        // Only a managed auto-skip correction is strong enough to move the
        // per-show threshold controller.
        if revertedManagedAny {
            recordThresholdControlSignal(
                .falsePositive,
                podcastId: sourceShowId
            )
        }
        noteManualVetoOutcome(
            .committed,
            analysisAssetId: expectedAssetId,
            start: start,
            end: end,
            revertedWindows: exactTargets.count,
            liveTargets: liveTargetCount
        )
        return true
    }

    /// Persist Yes on an already-applied automatic skip. This is deliberately
    /// not an aggregate-only acknowledgement: the exact asset/window/span
    /// receipt is committed before the banner may count or dismiss it.
    @discardableResult
    func confirmAutoSkippedBanner(
        windowId: String,
        analysisAssetId expectedAssetId: String?,
        startTime expectedStartTime: Double,
        endTime expectedEndTime: Double,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64?,
        ifWindowMaterialRevisionToken expectedMaterialToken: String?
    ) async -> Bool {
        guard let expectedAssetId,
              let expectedEpisodeId,
              let expectedPlaybackGeneration,
              let expectedMaterialToken,
              activeEpisodeId == expectedEpisodeId,
              activePlaybackLifecycleGeneration
                == expectedPlaybackGeneration,
              let managed = windows[windowId],
              managed.adWindow.analysisAssetId == expectedAssetId,
              managed.decisionState == .applied,
              managed.snappedStart == expectedStartTime,
              managed.snappedEnd == expectedEndTime,
              bannerMaterialRevisionToken(for: managed)
                == expectedMaterialToken
        else {
            return false
        }
        // playhead-o4qr split audit: this seam takes NO caller-supplied show,
        // so `exactFeedbackShowIdentity` has nothing to validate and cannot
        // fail here. Its one show-keyed effect,
        // `scheduleConfirmedRecurrenceLearning`, already fails closed on an
        // absent show (`learnConfirmedRecurrence` requires
        // `normalizedCatalogShowId(showId)`), and the seam writes no trust,
        // controller or negative-bank signal at all. Nothing to split;
        // adding a guard here would be dead code.
        let sourcePodcastId = activePodcastId

        let receipt = CorrectionEvent(
            analysisAssetId: expectedAssetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: expectedAssetId,
                startTime: expectedStartTime,
                endTime: expectedEndTime
            ).serialized,
            source: .bannerAutoSkipConfirmed,
            podcastId: sourcePodcastId,
            correctionType: .falseNegative,
            // playhead-bwxi: WHERE THE LISTENER WAS. The three poisoned rows of
            // 2026-08-21 are indistinguishable from honest ones precisely
            // because this was not recorded.
            playheadTimeAtCorrection: observedPlayheadTimeForCorrection,
            targetRefs: CorrectionTargetRefs(
                adWindowId: windowId,
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(managed.adWindow),
                exactFeedbackSpan: ExactFeedbackSpan(
                    startTime: expectedStartTime,
                    endTime: expectedEndTime
                )
            )
        )
        do {
            if let barrier = feedbackPersistenceBarrierForTesting {
                await barrier()
            }
            guard let wasNewlyInserted =
                    try await store.persistConfirmedAutoSkip(
                        windowId: windowId,
                        analysisAssetId: expectedAssetId,
                        expectedEpisodeId: expectedEpisodeId,
                        expectedStartTime: expectedStartTime,
                        expectedEndTime: expectedEndTime,
                        expectedProducerRevision: managed.adWindow,
                        expectedMaterialToken: expectedMaterialToken,
                        correction: receipt
                    )
            else {
                return false
            }
            schedulePostCommitCorrectionLearning(
                receipt,
                wasNewlyInserted: wasNewlyInserted
            )
            scheduleConfirmedRecurrenceLearning(
                for: managed.adWindow,
                showId: sourcePodcastId,
                source: .confirmedAutoSkipBanner,
                lifecycle: .explicitConfirmation
            )
            return true
        } catch {
            // Operational only: never log the answer, asset/window identity,
            // span, timestamp, or persistence error text.
            logger.warning("Banner feedback persistence failed")
            return false
        }
    }

    /// Persist No only for the exact applied auto-skip material displayed by
    /// the caller. All card-owned identities are mandatory; generic
    /// `revertWindow` remains available for non-banner correction surfaces.
    ///
    /// `surface` says WHICH answer this is — the card's No, or a row of the
    /// passive missed-skip list (playhead-2d6i) — and is the ONLY thing that
    /// varies between them: identical preconditions, identical transaction,
    /// identical state mutation, one `correction_events.source` apart. It has
    /// no default on purpose (playhead-nq8z); see `AutoSkipDenialSurface`.
    ///
    /// It reaches exactly TWO places, and the third is a deliberate omission:
    ///
    ///   • the durable `CorrectionEvent`'s `source`, which is the discriminator
    ///     a corpus reader filters `playheadTimeAtCorrection` on;
    ///   • `persistDeniedAutoSkip`, which re-checks the correction it is
    ///     handed against the surface, so the store cannot commit a receipt
    ///     whose source disagrees with the door it came through;
    ///   • NOT `revokeRecurrenceEvidence`. See its call below.
    @discardableResult
    func denyAutoSkippedBanner(
        windowId: String,
        analysisAssetId expectedAssetId: String?,
        startTime expectedStartTime: Double,
        endTime expectedEndTime: Double,
        podcastId: String?,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64?,
        ifWindowMaterialRevisionToken expectedMaterialToken: String?,
        surface: AutoSkipDenialSurface
    ) async -> Bool {
        // playhead-o4qr: ACCEPT THE RECEIPT, REFUSE THE LEARNING — see the long
        // form above `recordListenRevert`'s call to
        // `exactFeedbackShowIdentity`. The banner No still commits its durable
        // receipt when the card's show identity is unusable; only the
        // show-keyed calibration below is withheld, via `sourceShowId == nil`.
        let validatedShow = exactFeedbackShowIdentity(requested: podcastId)
        guard let expectedAssetId,
              let expectedEpisodeId,
              let expectedPlaybackGeneration,
              let expectedMaterialToken,
              activeEpisodeId == expectedEpisodeId,
              activePlaybackLifecycleGeneration
                == expectedPlaybackGeneration,
              let requestedManaged = windows[windowId],
              requestedManaged.adWindow.analysisAssetId == expectedAssetId,
              requestedManaged.decisionState == .applied,
              requestedManaged.snappedStart == expectedStartTime,
              requestedManaged.snappedEnd == expectedEndTime,
              bannerMaterialRevisionToken(for: requestedManaged)
                == expectedMaterialToken
        else {
            return false
        }

        let sourceEpisodeId = activeEpisodeId
        let sourceLifecycleGeneration = episodeLifecycleGeneration
        let sourceShowId = validatedShow.showId
        let sourceNegativeAttribution = revertNegativeAttribution(
            for: requestedManaged.adWindow
        )
        do {
            // playhead-nq8z: `CatalogRevocationSource`, NOT `CorrectionSource`
            // — a PARALLEL enum with a same-spelled case, and it deliberately
            // does NOT gain a `missedAutoSkipListDenied` twin.
            //
            // It names the authoritative negative EVENT that revoked learned
            // catalog evidence, and both surfaces produce the same event: the
            // listener's explicit No about this exact window's material.
            // Nothing branches on which of its four cases a tombstone carries
            // — it is an audit column, read back only by `revocationTombstone`
            // and only to be returned — so a fifth case would put a value on
            // disk that changes no decision. And the question this bead exists
            // to answer cannot be asked of that table anyway: a tombstone
            // carries no playhead position, so there is nothing to filter.
            // The discriminator belongs in `correction_events`, which is the
            // corpus, and that is where it went.
            try await revokeRecurrenceEvidence(
                for: requestedManaged.adWindow,
                showId: sourceShowId,
                source: .bannerAutoSkipDenied,
                negativeAttribution: sourceNegativeAttribution
            )
        } catch {
            logger.warning("Banner feedback revocation failed")
            return false
        }
        let correction: CorrectionEvent
        do {
            correction = try makeManualCorrectionVetoEvent(
                startTime: expectedStartTime,
                endTime: expectedEndTime,
                assetId: expectedAssetId,
                podcastId: sourceShowId,
                source: surface.correctionSource,
                windowId: windowId,
                detectionProjection:
                    ExplicitFeedbackDetectionProjection(
                        requestedManaged.adWindow
                    )
            )
            // playhead-i08e: the same suspension `confirmAutoSkippedBanner`,
            // `acceptSuggestedSkip` and `declineSuggestedSkip` already take.
            // Without it this seam — one of only two that reach the threshold
            // controller in a shipped build — had no way to be interleaved
            // with an episode replacement, so the fact that its calibration
            // runs BEFORE the ownership guard below (deliberately: the
            // captured show is owed the feedback) could not be asserted.
            // Production leaves the barrier nil.
            if let barrier = feedbackPersistenceBarrierForTesting {
                await barrier()
            }
            guard let wasNewlyInserted =
                    try await store.persistDeniedAutoSkip(
                        windowId: windowId,
                        analysisAssetId: expectedAssetId,
                        expectedEpisodeId: expectedEpisodeId,
                        expectedStartTime: expectedStartTime,
                        expectedEndTime: expectedEndTime,
                        expectedProducerRevision:
                            requestedManaged.adWindow,
                        expectedMaterialToken: expectedMaterialToken,
                        surface: surface,
                        correction: correction
                    )
            else {
                return false
            }
            schedulePostCommitCorrectionLearning(
                correction,
                wasNewlyInserted: wasNewlyInserted
            )
        } catch {
            logger.warning("Banner feedback persistence failed")
            return false
        }

        recordThresholdControlSignal(
            .falsePositive,
            podcastId: sourceShowId
        )
        if let sourceShowId {
            if let handler = falseSkipSignalHandlerForTesting {
                Task {
                    await handler(sourceShowId)
                }
            } else if let trustService {
                let attributions = [
                    vetoAttribution(for: requestedManaged.adWindow)
                ]
                Task {
                    await trustService.recordFalseSkipSignal(
                        podcastId: sourceShowId,
                        attributions: attributions,
                        privacy: .explicitBannerFeedback
                    )
                }
            }
        }

        guard activeEpisodeId == sourceEpisodeId,
              episodeLifecycleGeneration == sourceLifecycleGeneration
        else {
            return true
        }
        guard var managed = windows[windowId],
              bannerMaterialRevisionToken(for: managed)
                == expectedMaterialToken,
              managed.decisionState == .applied
        else {
            return true
        }
        managed.decisionState = .reverted
        managed.cueActive = false
        windows[windowId] = managed
        evaluateAndPush()
        return true
    }

    /// Revert a specific window by ID using the manualVeto source.
    /// Same as recordListenRevert but uses .manualVeto correction source
    /// and does not imply a playback rewind.
    @discardableResult
    func revertWindow(windowId: String, podcastId: String? = nil) async -> Bool {
        await revertWindow(
            windowId: windowId,
            podcastId: podcastId,
            ifCurrentEpisodeId: activeEpisodeId
        )
    }

    /// Episode-bound generic form intended for deferred non-banner correction
    /// surfaces. The outer UI guard is not sufficient because this actor can
    /// be re-entered while its store and trust calls suspend.
    ///
    /// playhead-i08e: "intended for", not "used by" — no production caller
    /// exists yet. `NowPlayingView` has a closure parameter named
    /// `revertWindow`, but the closure bound to it calls
    /// `denyAutoSkippedBanner`. See the census above `declineSuggestedSkip`.
    @discardableResult
    func revertWindow(
        windowId: String,
        podcastId: String? = nil,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64? = nil
    ) async -> Bool {
        // playhead-o4qr: ACCEPT THE RECEIPT, REFUSE THE LEARNING — see the long
        // form above `recordListenRevert`'s call to
        // `exactFeedbackShowIdentity`. This is the seam
        // `anonymousRevertRecordsNoControllerSample` drives: a revert carrying
        // no show id keeps its durable receipt and returns true, while
        // `sourceShowId == nil` withholds every show-keyed effect.
        let validatedShow = exactFeedbackShowIdentity(requested: podcastId)
        guard activeEpisodeId == expectedEpisodeId,
              expectedPlaybackGeneration == nil
                || activePlaybackLifecycleGeneration
                    == expectedPlaybackGeneration
        else {
            return false
        }
        let sourceEpisodeId = activeEpisodeId
        let sourceLifecycleGeneration = episodeLifecycleGeneration
        let sourceShowId = validatedShow.showId
        guard let requestedManaged = windows[windowId] else { return false }
        guard requestedManaged.decisionState != .reverted,
              requestedManaged.decisionState != .suppressed else { return false }

        let sourceNegativeAttribution = revertNegativeAttribution(
            for: requestedManaged.adWindow
        )
        do {
            try await revokeRecurrenceEvidence(
                for: requestedManaged.adWindow,
                showId: sourceShowId,
                source: .manualVeto,
                negativeAttribution: sourceNegativeAttribution
            )
        } catch {
            logger.warning("Manual veto revocation failed")
            return false
        }

        // Commit the generic correction and authoritative row retirement
        // together. A failed row mutation must not leave behind a correction
        // that the calling surface still presents as retryable.
        do {
            let correction = try makeManualCorrectionVetoEvent(
                startTime: requestedManaged.snappedStart,
                endTime: requestedManaged.snappedEnd,
                assetId: requestedManaged.adWindow.analysisAssetId,
                podcastId: sourceShowId,
                source: .manualVeto,
                windowId: windowId,
                detectionProjection:
                    ExplicitFeedbackDetectionProjection(
                        requestedManaged.adWindow
                    )
            )
            if let barrier = feedbackPersistenceBarrierForTesting {
                await barrier()
            }
            guard let wasNewlyInserted =
                    try await store.persistRevertedAdWindowsIfCurrent(
                expectedWindows: [requestedManaged.adWindow],
                analysisAssetId: requestedManaged.adWindow.analysisAssetId,
                expectedPodcastId: sourceShowId,
                correction: correction
            ) else {
                return false
            }
            schedulePostCommitCorrectionLearning(
                correction,
                wasNewlyInserted: wasNewlyInserted
            )
        } catch {
            logger.warning("Manual veto persistence failed")
            return false
        }

        // These calibration effects belong to the captured source show. Start
        // them before checking live lifecycle identity so an episode switch
        // cannot silently discard valid old-episode feedback.
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: sourceShowId
        )
        if let sourceShowId {
            if let handler = falseSkipSignalHandlerForTesting {
                Task {
                    await handler(sourceShowId)
                }
            } else if let trustService {
                let attributions = [
                    vetoAttribution(for: requestedManaged.adWindow)
                ]
                Task {
                    await trustService.recordFalseSkipSignal(
                        podcastId: sourceShowId,
                        attributions: attributions
                    )
                }
            }
        }

        guard activeEpisodeId == sourceEpisodeId,
              episodeLifecycleGeneration == sourceLifecycleGeneration else {
            // The old episode's row was durably corrected while the actor was
            // suspended. Its UI has already retired, so no replacement state
            // may be mutated.
            return true
        }
        guard var managed = windows[windowId],
              managed.decisionState != .reverted,
              managed.decisionState != .suppressed,
              AdWindowMaterialIdentity.sameProducerRevision(
                  managed.adWindow,
                  requestedManaged.adWindow
              )
        else {
            return true
        }

        managed.decisionState = .reverted
        managed.cueActive = false
        windows[windowId] = managed
        logDecision(
            managed: managed,
            decision: .reverted,
            reason: "User correction: not an ad"
        )

        // The durable revert above is the primary user action. Republish cues
        // immediately so playback cannot enter a stale range while secondary
        // calibration storage suspends.
        evaluateAndPush()

        return true
    }

    /// playhead-rfu-sad: episode-scoped bookkeeping for the tap-then-flip
    /// race guard. Accepted producer IDs remain terminal until the lifecycle
    /// is replaced or ended.
    private func rememberAcceptedSuggestId(_ id: String) {
        recentlyAcceptedSuggestIds.insert(id)
    }

    /// Builds an exact-span correction for generic vetoes and explicit banner
    /// responses. The owning AnalysisStore transaction appends it together
    /// with the authoritative AdWindow mutation; post-commit derived learning
    /// is notified separately without appending again.
    ///
    /// playhead-i08e: deliberately does NOT require `correctionStore`. The
    /// receipt is made durable by the AnalysisStore transaction that commits
    /// it with the AdWindow mutation — `correctionStore` only receives the
    /// post-commit derived-learning notification, and that hop already
    /// no-ops when unwired (`schedulePostCommitCorrectionLearning`). Throwing
    /// on an unwired optional learning dependency aborted the whole gesture at
    /// its first statement, killing both the user's correction and the
    /// calibration signals (trust + per-show threshold controller) that follow
    /// it.
    ///
    /// Scope, stated precisely so this is not misread as a shipped user-facing
    /// defect: `PlayheadRuntime` is the only production construction site and
    /// it always injects a store, so the precondition never fired for a real
    /// user — it bought nothing there while silently disabling the seam in
    /// every other configuration, which is where the dead threshold-control
    /// write path was found.
    private func makeManualCorrectionVetoEvent(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource,
        windowId: String? = nil,
        additionalWindowIds: [String]? = nil,
        detectionProjection:
            ExplicitFeedbackDetectionProjection? = nil,
        correctionProvenance: [AnchorRef] = []
    ) throws -> CorrectionEvent {
        guard startTime.isFinite, endTime.isFinite else {
            throw SkipOrchestratorFeedbackError.invalidCorrectionRange
        }
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: min(startTime, endTime),
            endTime: max(startTime, endTime)
        )
        let inferredRefs = CausalInference.buildTargetRefs(
            provenance: correctionProvenance,
            ledgerEntries: []
        )
        return CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            source: source,
            podcastId: podcastId,
            correctionType: source.kind.correctionType,
            // playhead-bwxi: WHERE THE LISTENER WAS.
            playheadTimeAtCorrection: observedPlayheadTimeForCorrection,
            causalSource: correctionProvenance.isEmpty
                ? nil
                : CausalInference.inferCausalSource(
                    provenance: correctionProvenance,
                    ledgerEntries: []
                ),
            targetRefs: windowId.map {
                CorrectionTargetRefs(
                    adWindowId: $0,
                    adWindowIds: additionalWindowIds,
                    explicitFeedbackDetectionProjection:
                        detectionProjection,
                    exactFeedbackSpan: ExactFeedbackSpan(
                        startTime: min(startTime, endTime),
                        endTime: max(startTime, endTime)
                    ),
                    atomIds: inferredRefs?.atomIds,
                    evidenceRefs: inferredRefs?.evidenceRefs,
                    fingerprintId: inferredRefs?.fingerprintId,
                    domain: inferredRefs?.domain,
                    sponsorEntity: inferredRefs?.sponsorEntity
                )
            }
        )
    }

    /// Derived learning must never extend the primary banner transaction.
    /// The correction and authoritative AdWindow mutation are already
    /// committed atomically when this is called; materialization is
    /// idempotent and may safely finish after the user-visible action.
    private func schedulePostCommitCorrectionLearning(
        _ correction: CorrectionEvent,
        wasNewlyInserted: Bool
    ) {
        guard let correctionStore else { return }
        Task {
            await correctionStore.correctionDidPersistAtomically(
                correction,
                wasNewlyInserted: wasNewlyInserted
            )
        }
    }

    /// playhead-xsdz.9: confirmed-FP WRITE TRIGGER for the hard-negative bank.
    /// Called from the user-reversion seams (Listen revert / "not an ad")
    /// with the wrongly-flagged window's ad-copy text. Fire-and-forget; never
    /// throws (a bank-write failure must not break playback). No-op when no
    /// bank is wired or the text is empty.
    ///
    /// MEMORY-POLLUTION GUARD: this is the ONLY orchestrator seam that writes to
    /// the negative bank, and it is reached ONLY from reversion paths — never
    /// from the auto-skip-eligible path. The bank therefore ingests confirmed
    /// FPs exclusively.
    ///
    /// playhead-1mq1.2.1 MIXED-WIDTH GUARD: `text` is the WHOLE window's ad
    /// copy and there is no time index into it, so it cannot be sliced down to
    /// the part the user actually rejected. When the reverted window is MIXED
    /// — strong ad evidence in a proper part of it, substantial evidence-free
    /// remainder — this seam therefore DEFERS rather than guessing. Ingesting
    /// would bank a real ad's copy as a confirmed false positive and suppress
    /// that ad on this show forever; the gesture cannot distinguish
    /// "not an ad" from "wrong edges", so the safe reading of an ambiguous
    /// correction is to learn nothing from it. The receipt, the trust penalty
    /// and the controller sample are unaffected — see playhead-o4qr's
    /// ACCEPT THE RECEIPT, REFUSE THE LEARNING split, of which this is the
    /// same shape one axis over (span instead of show).
    private func ingestNegativeFingerprint(
        text: String?,
        podcastId: String?,
        negativeAttribution: RevertEvidencePartition.Partition
    ) {
        guard let bank = negativeFingerprintBank else { return }
        guard negativeAttribution.allowsWholeSpanNegativeLabel else { return }
        // playhead-o4qr: REFUSE THE LEARNING half of the anonymous-correction
        // contract, enforced here rather than at the call site so the bank has
        // ONE gate no future seam can route around. A nil/empty show writes a
        // NULL-show row, and `loadEntries(forShow:includeGlobal:)` returns
        // NULL-show negatives to EVERY show — so a single unattributable
        // correction would suppress matching copy across the whole library.
        // Deliberately mirrors `recordThresholdControlSignal`'s guard below:
        // both are per-show stores with nowhere to put an anonymous sample.
        guard let podcastId, !podcastId.isEmpty else { return }
        guard let text, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        let pid = podcastId
        Task {
            do {
                _ = try await bank.recordConfirmedFalsePositive(text: text, showId: pid)
            } catch {
                // Non-fatal: a hard-negative we failed to record just means a
                // future near-match isn't suppressed. Playback is unaffected.
            }
        }
    }

    /// playhead-xsdz.11: per-show PI-controller WRITE TRIGGER.
    ///
    /// Folds one correction signal into the show's threshold-controller state:
    ///   • `.falsePositive` — the user listened through / reverted an
    ///     auto-skipped (or markOnly) window → RAISE the show's threshold.
    ///   • `.miss` — the user accepted a suggested skip we did NOT auto-skip
    ///     ("this WAS an ad") → LOWER the show's threshold.
    ///
    /// Fire-and-forget; never throws (a controller-write failure must not break
    /// playback). No-op when no store is wired (the flag-OFF production default)
    /// or `podcastId` is absent — the controller is per-show, so an anonymous
    /// correction has nowhere to land.
    private func recordThresholdControlSignal(
        _ signal: ThresholdControlSignal,
        podcastId: String?
    ) {
        guard let store = perShowThresholdControllerStore else { return }
        guard let podcastId, !podcastId.isEmpty else { return }
        Task {
            do {
                _ = try await store.record(signal: signal, forShow: podcastId)
            } catch {
                // Non-fatal: a missed controller update just means this show's
                // threshold doesn't move this once. Playback is unaffected.
            }
        }
    }

    /// playhead-xsdz.11: the defined MISS-side API. The user scrubbed through /
    /// reported undetected ad content on this show — LOWER the threshold (be
    /// more aggressive). Currently wired at the `acceptSuggestedSkip` seam (the
    /// only "we missed an ad" gesture that reaches this layer). Public so the
    /// false-negative-report UI path can call it directly when wired.
    func recordThresholdControlMiss(podcastId: String?) {
        recordThresholdControlSignal(.miss, podcastId: podcastId)
    }

    private func recoverFailedProvisionalSuggestion(
        windowId: String,
        fallback: AdWindow,
        sourceEpisodeId: String?,
        sourceLifecycleGeneration: UInt64
    ) async {
        // A same-ID suggestion may already be resolving in a replacement
        // episode. Never clear that lifecycle's provisional reservation or
        // buffered producer value from an older transaction's completion.
        guard activeEpisodeId == sourceEpisodeId,
              episodeLifecycleGeneration == sourceLifecycleGeneration else {
            return
        }
        provisionallyResolvingSuggestWindowIds.remove(windowId)
        let buffered = bufferedSuggestProducerUpdates.removeValue(
            forKey: windowId
        )

        switch buffered {
        case .adWindow(let latest):
            await receiveAdWindows([latest])
        case .decisionResult(let latest):
            await receiveAdDecisionResults([latest])
        case .retired:
            emitBannerRetirement(windowId: windowId)
        case nil:
            if suggestWindows[windowId] == nil {
                suggestWindows[windowId] = fallback
            }
        }
    }

    /// playhead-gtt9.23: User tapped "Skip" on a suggest-tier banner.
    /// Promotes the markOnly window into the active skip path with a
    /// user-confirmed confidence so the existing skip-cue machinery handles
    /// playback transport and persistence. Also records a `.falseNegative`
    /// CorrectionEvent — the user has just told us "this WAS an ad we
    /// didn't auto-skip," which is exactly the calibration signal that
    /// future threshold tuning needs.
    ///
    /// No-op when the window is not in the suggest set (e.g. already
    /// auto-skipped, declined, or never registered as markOnly).
    @discardableResult
    func acceptSuggestedSkip(windowId: String) async -> Bool {
        await acceptSuggestedSkip(
            windowId: windowId,
            ifCurrentEpisodeId: activeEpisodeId
        )
    }

    /// Episode-bound form used by deferred banner actions. Every attribution
    /// value is captured before the first suspension; `beginEpisode` may
    /// otherwise replace the actor's active podcast while trust persistence is
    /// in flight.
    @discardableResult
    func acceptSuggestedSkip(
        windowId: String,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64? = nil,
        ifSuggestionRevisionToken expectedRevisionToken: String? = nil
    ) async -> Bool {
        guard let expectedEpisodeId,
              activeEpisodeId == expectedEpisodeId,
              expectedPlaybackGeneration == nil
                || activePlaybackLifecycleGeneration
                    == expectedPlaybackGeneration,
              let sourceMaterialToken =
                suggestRevisionTokensByWindowId[windowId],
              expectedRevisionToken == nil
                || sourceMaterialToken == expectedRevisionToken
        else {
            return false
        }
        let sourceEpisodeId = activeEpisodeId
        let sourceLifecycleGeneration = episodeLifecycleGeneration
        // playhead-o4qr split audit: no caller-supplied show, so the identity
        // check cannot fail here. Every show-keyed effect below is already
        // nil-refusing — `learnConfirmedRecurrence` requires a canonical show,
        // the trust hop is an `if let`, and `recordThresholdControlMiss` funnels
        // into `recordThresholdControlSignal`'s nil/empty guard. Nothing to
        // split; a guard here would be dead code.
        let sourcePodcastId = activePodcastId
        guard let suggested = suggestWindows.removeValue(forKey: windowId) else {
            return false
        }
        provisionallyResolvingSuggestWindowIds.insert(windowId)

        // playhead-rfu-sad: remember this id was promoted via tap so a
        // late-arriving ingest with the same id and a cleared gate
        // (tap-then-flip race) doesn't register a SECOND managed window
        // alongside the UUID-keyed entry produced below. See the LRU
        // check in `receiveAdWindows`.
        rememberAcceptedSuggestId(windowId)

        // playhead-ynmk: resolve the extent BEFORE building the row.
        //
        // The user answered "is this an ad?" — a PRESENCE claim over a span
        // whose edges the DETECTOR drew. Extent was never asked about, so it is
        // still owned by the evidence, and the evidence is the per-edge anchor
        // tier. `AutoSkipEdgePadding` returns the late-safe window, or nil when
        // no anchored start proves late-safety (derivation §5's verdict:
        // "spans without a hard start anchor stay markOnly"). nil means the
        // confirmation is recorded as a MARK and no skip fires — which is what
        // the field case needed: three both-edges-unanchored spans, 0 of 3
        // correct, 210 s of show cut by three taps.
        //
        // Anchors come from the persisted row: a suggest-tier window returns
        // out of `receiveAdWindows` before the ingest stamp site, so
        // `edgeAnchorsByWindowId` has no entry for it.
        //
        // playhead-d3g0 moved the expression into `confirmationWouldSkip` so
        // the BANNER can ask the same question before offering a Skip action.
        // Two readers, one policy call — a second copy is how a card comes to
        // promise something this transaction will not do. Still resolved BEFORE
        // the row is built, and still synchronous with `sourcePodcastId`'s
        // capture (no suspension between them), so the value is unchanged.
        let suggestedAnchors = resolvedEdgeAnchors(for: suggested)
        let extentIsSkippable = confirmationWouldSkip(suggested)

        // Build a fresh durable AdWindow with the suggest window's span, its
        // MEASURED confidence carried through unchanged, and the tap recorded
        // in `boundaryState` (`UserSpanAssertion.userConfirmedSuggested`) where
        // nothing reads it as certainty. The in-memory wrapper starts confirmed
        // so the explicit user-skip path owns the applied transition and cue.
        // We deliberately do not reuse the original markOnly window's id — its
        // eligibilityGate would block it again.
        let promotedId = UUID().uuidString
        let assetId = suggested.analysisAssetId
        let clearUntrustedCatalogProvenance =
            suggested.claimsCatalogMatch
            && !suggested.hasCompatibleCatalogMatchProvenance(
                expectedShowId: sourcePodcastId
            )
        let promoted = AdWindow(
            id: promotedId,
            analysisAssetId: assetId,
            startTime: suggested.startTime,
            endTime: suggested.endTime,
            // playhead-ynmk: NEVER 1.0. A tap does not measure anything — a
            // pure-show span confirmed by mistake would read 1.00 too.
            confidence: suggested.confidence,
            // playhead-ar60: carried through for the SAME reason as
            // `confidence` above — a tap is not a measurement, so the
            // promoted row keeps the actuation number the detector
            // produced rather than acquiring a fresh one. `nil` here would
            // silently PROMOTE a user-suppressed span, because
            // `actuationConfidence` would then fall back to the (higher)
            // detection score.
            skipConfidence: suggested.skipConfidence,
            boundaryState: UserSpanAssertion.userConfirmedSuggested.rawValue,
            decisionState: extentIsSkippable
                ? AdDecisionState.applied.rawValue
                : AdDecisionState.confirmed.rawValue,
            detectorVersion: suggested.detectorVersion,
            advertiser: suggested.advertiser,
            product: suggested.product,
            adDescription: suggested.adDescription,
            evidenceText: suggested.evidenceText,
            evidenceStartTime: suggested.evidenceStartTime,
            metadataSource: suggested.metadataSource,
            metadataConfidence: suggested.metadataConfidence,
            metadataPromptVersion: suggested.metadataPromptVersion,
            // playhead-ynmk: only true when a cue will actually be pushed.
            wasSkipped: extentIsSkippable,
            userDismissedBanner: false,
            evidenceSources: suggested.evidenceSources,
            catalogStoreMatchSimilarity:
                clearUntrustedCatalogProvenance
                    ? nil
                    : suggested.catalogStoreMatchSimilarity,
            catalogFingerprintVersion:
                clearUntrustedCatalogProvenance
                    ? nil
                    : suggested.catalogFingerprintVersion,
            catalogMatchedEntryId:
                clearUntrustedCatalogProvenance
                    ? nil
                    : suggested.catalogMatchedEntryId,
            catalogMatchedShowId:
                clearUntrustedCatalogProvenance
                    ? nil
                    : suggested.catalogMatchedShowId,
            catalogMatchedLearningSource:
                clearUntrustedCatalogProvenance
                    ? nil
                    : suggested.catalogMatchedLearningSource,
            catalogMatchedLearningLifecycle:
                clearUntrustedCatalogProvenance
                    ? nil
                    : suggested.catalogMatchedLearningLifecycle,
            startEdgeAnchor: suggested.startEdgeAnchor,
            endEdgeAnchor: suggested.endEdgeAnchor
        )

        let key = idempotencyKey(assetId: assetId, windowId: promotedId)
        let managed = ManagedWindow(
            adWindow: promoted,
            decisionState: .confirmed,
            snappedStart: promoted.startTime,
            snappedEnd: promoted.endTime,
            idempotencyKey: key,
            cueActive: false
        )
        // The persisted row is authoritative across reloads. Reserve the
        // producer ID above, but do not install a cue, emit calibration, count
        // the response, or dismiss its card until the original-row retirement
        // and promoted-row insert commit together.
        do {
            let correction = try makeManualCorrectionVetoEvent(
                startTime: suggested.startTime,
                endTime: suggested.endTime,
                assetId: assetId,
                podcastId: sourcePodcastId,
                source: .bannerSuggestionConfirmed,
                windowId: promotedId,
                additionalWindowIds: [windowId, promotedId],
                detectionProjection:
                    ExplicitFeedbackDetectionProjection(suggested)
            )
            if let barrier = feedbackPersistenceBarrierForTesting {
                await barrier()
            }
            guard let wasNewlyInserted =
                    try await store.persistAcceptedSuggestionIfCurrent(
                        originalWindowId: windowId,
                        originalAnalysisAssetId: assetId,
                        expectedEpisodeId: expectedEpisodeId,
                        expectedStartTime: suggested.startTime,
                        expectedEndTime: suggested.endTime,
                        expectedProducerRevision: suggested,
                        expectedMaterialToken: sourceMaterialToken,
                        promotedWindow: promoted,
                        correction: correction
                    )
            else {
                throw SkipOrchestratorFeedbackError
                    .staleDurableMaterial
            }
            schedulePostCommitCorrectionLearning(
                correction,
                wasNewlyInserted: wasNewlyInserted
            )
        } catch {
            logger.warning("Banner feedback persistence failed")
            // Release the tap-then-flip reservation and restore the exact
            // revision only while its source lifecycle is still current. A
            // producer update that arrived during the store hop owns the ID
            // and must never be overwritten by this failed attempt.
            if activeEpisodeId == sourceEpisodeId,
               episodeLifecycleGeneration == sourceLifecycleGeneration {
                recentlyAcceptedSuggestIds.remove(windowId)
            }
            await recoverFailedProvisionalSuggestion(
                windowId: windowId,
                fallback: suggested,
                sourceEpisodeId: sourceEpisodeId,
                sourceLifecycleGeneration: sourceLifecycleGeneration
            )
            return false
        }

        // The atomic suggestion receipt is the authoritative positive event.
        // Admit recurrence learning immediately from its captured source
        // material, even if `beginEpisode` replaces the live UI lifecycle
        // while the transaction is suspended.
        scheduleConfirmedRecurrenceLearning(
            for: promoted,
            showId: sourcePodcastId,
            source: .confirmedSuggestion,
            lifecycle: .explicitConfirmation
        )

        let sourceLifecycleIsCurrent =
            activeEpisodeId == sourceEpisodeId
            && episodeLifecycleGeneration == sourceLifecycleGeneration
        if sourceLifecycleIsCurrent {
            provisionallyResolvingSuggestWindowIds.remove(windowId)
            bufferedSuggestProducerUpdates.removeValue(forKey: windowId)
        }

        // Calibration belongs to the captured source show even if playback
        // moved to another episode during persistence.
        if let podcastId = sourcePodcastId {
            if let handler = falseNegativeSignalHandlerForTesting {
                Task {
                    await handler(podcastId)
                }
            } else if let trustService {
                Task {
                    await trustService.recordFalseNegativeSignal(
                        podcastId: podcastId,
                        privacy: .explicitBannerFeedback
                    )
                }
            }
            // playhead-gard: THE SAME TAP IS ALSO A CORRECT OBSERVATION, and
            // recording it is what makes `manual` escapable.
            //
            // The call above is a true statement about the SKIP SURFACE: this
            // span was mark-only, so the surface did miss it. It was the only
            // thing recorded, and it moves trust DOWN — so a user answering
            // "yes, that was an ad" made the show LESS trusted, and the counter
            // whose reaching zero is required to leave `manual` never moved at
            // all. Measured on this tree: `recordSuccessfulObservation` and
            // `decayFalseSignals` had ZERO production callers, so trust was a
            // one-way ratchet and `manual` was a one-way door.
            //
            // The banner asked "is this an ad?" about a span the DETECTOR drew.
            // A Yes affirms that detector's presence claim, so the credit goes
            // to the class that drew it — not to the show globally, and not to
            // `.userAsserted`, which is what the promoted row's `boundaryState`
            // now says. `suggested` is the pre-promotion row and still carries
            // the producer's own provenance.
            // playhead-fh5v: the episode this tap is about. `observationCount`
            // counts EPISODES through `trust_episode_observations`, so the
            // recorder needs the asset identity to claim against — without
            // it, four Yes taps in one episode bought four episodes of
            // credit and wrote no ledger row to disagree with. `assetId` is
            // `suggested.analysisAssetId`, captured above with the rest of
            // the source material so a `beginEpisode` during the awaits
            // cannot re-point it at a different episode.
            //
            // R3: HOISTED ABOVE THE TEST SEAM DELIBERATELY. It used to be
            // declared inside the `trustService` branch, so the handler branch
            // — the only one any test exercises — never saw it, and replacing
            // this value with `""` passed the entire suite. An empty id is not
            // a harmless default: `claimEpisodeTrustObservation` refuses it, so
            // `countsAsEpisode` would be false forever and a banner Yes could
            // never be the first witness for an episode, which is the whole
            // point of sharing the claim. Both branches now read ONE local, so
            // a test that asserts the seam's id pins production's.
            let observedAssetId = assetId
            if let handler = correctObservationHandlerForTesting {
                let detector = detectorClass(for: suggested)
                Task {
                    await handler(podcastId, observedAssetId, detector)
                }
            } else if let trustService {
                let detector = detectorClass(for: suggested)
                Task {
                    await trustService.recordCorrectObservation(
                        podcastId: podcastId,
                        analysisAssetId: observedAssetId,
                        detector: detector
                    )
                }
            }
        }
        recordThresholdControlMiss(podcastId: sourcePodcastId)

        guard sourceLifecycleIsCurrent else {
            return true
        }

        windows[promotedId] = managed
        // playhead-ynmk: the promotion runs under a FRESH uuid, so the ingest
        // stamp in `receiveAdWindows` never sees it and `paddedCueSpan` would
        // read the `.unanchored` default — refusing even a byte-exact
        // confirmation. Carry the suggestion's real per-edge provenance across.
        edgeAnchorsByWindowId[promotedId] = suggestedAnchors
        // The suggest card already presented this span and collected the
        // explicit Yes. Applying its promoted UUID must not immediately emit
        // a second feedback card for the same user decision.
        // Cycle-27 T-3 production-writer site (4 of 4): accepted suggest
        // presentation already collected the response.
        banneredWindowIds.insert(promotedId)

        if extentIsSkippable {
            applyManualSkip(
                windowId: promotedId,
                isExplicitBannerFeedback: true
            )
        } else {
            // The tap is honoured as FEEDBACK — the correction receipt, the
            // recurrence learning and the MISS calibration above all landed —
            // but nothing is skipped. Presence was asserted; extent was not,
            // and no anchored edge exists to supply it.
            //
            // playhead-v7q6: the AUDIT TRAIL for this refusal is the durable
            // row, not the decision log. The promoted `AdWindow` records it
            // unambiguously — `decisionState == .confirmed` with
            // `wasSkipped == false` is a refused confirmation and can be
            // nothing else. `logDecision(managed:)` would additionally write
            // the window's EXACT SPAN into the diagnostic logger, and
            // `testExplicitBannerFeedbackRoutesDoNotWriteDetailedLogs` forbids
            // every explicit-feedback route from doing that: exact feedback
            // receipts belong only in the durable correction store. So the
            // observability ynmk wanted is kept as an id-only os_log line —
            // enough to find the event, carrying no receipt.
            logger.info(
                "acceptSuggestedSkip: unanchored extent — presence recorded, markOnly (window \(promotedId, privacy: .public))"
            )
            evaluateAndPush()
        }

        return true
    }

    /// playhead-gtt9.23 / playhead-lc7z: User's suggest-tier banner exited
    /// without a Yes response. Two semantic exit categories, two behaviors:
    ///
    ///   • `isExplicitDenial == false` (neutral x dismissal or auto-fade
    ///     timeout): no feedback signal. The suggest window is dropped from
    ///     the in-memory set so its cue is gone from the UI, but NO veto is
    ///     recorded and NO trust signal fires. This preserves the conservative
    ///     reading for exits that do not answer the feedback question.
    ///
    ///   • `isExplicitDenial == true` (the user answered No): an explicit
    ///     "that isn't an ad." This is exactly the hard-negative
    ///     the correction→retrain flywheel (xsdz.69 brand-as-editorial,
    ///     xsdz.70 native ads) was being starved of. We persist a
    ///     `.falsePositive` CorrectionEvent over the window's span, stamp
    ///     `userDismissedBanner = 1` on the row, and flip its persisted
    ///     `decisionState` to `.reverted` so a relaunch/replay never
    ///     resurfaces the banner the user explicitly denied. No trust /
    ///     threshold signal is fired here — this seam is capture-only, because
    ///     the algorithm only OFFERED a banner and never altered playback, so
    ///     the disagreement is too weak to raise the auto-skip threshold.
    ///
    /// The full census of which seams DO calibrate the per-show threshold
    /// controller, since the paragraph above is easy to read as exhaustive and
    /// is not:
    ///   • FALSE-POSITIVE (raise) — every path that vetoes a MANAGED
    ///     (auto-skip-tier) window: `recordListenRevert`, `revertByTimeRange`
    ///     (only when `revertedManagedAny`), `revertWindow`, and
    ///     `denyAutoSkippedBanner`.
    ///
    ///     This is a census of SEAMS, not of shipped behaviour, and TWO of
    ///     those four are reachable only from tests today:
    ///       – `recordListenRevert` — the banner Listen tap runs
    ///         `retireLiveSkipForListen` plus
    ///         `AdDetectionService.recordListenRewind`, neither of which
    ///         calibrates.
    ///       – `revertWindow` — the only production reference is
    ///         `NowPlayingView`'s closure PARAMETER of the same name
    ///         (`BannerFeedbackProductionActions.revertWindow`), and the
    ///         closure bound to it at the single construction site calls
    ///         `denyAutoSkippedBanner`. The name is vestigial; nothing calls
    ///         this method outside tests.
    ///     Wiring either is tracked separately. What actually reaches the
    ///     controller in production is `revertByTimeRange` (the transcript
    ///     "This isn't an ad" gesture) and `denyAutoSkippedBanner` (the banner
    ///     No) — do not read this list as "what the controller is being fed".
    ///   • MISS (lower) — `acceptSuggestedSkip`. This is the one SUGGEST-tier
    ///     gesture that calibrates: the user saying "this WAS an ad" about
    ///     something we did not auto-skip is a false negative, which is a
    ///     signal the controller models.
    ///   • Capture-only — this seam, and `confirmAutoSkippedBanner`. Note the
    ///     reasons differ: this one because a suggest-tier No never altered
    ///     playback, `confirmAutoSkippedBanner` because agreement with a skip
    ///     is a TRUE positive and the controller models only false positives
    ///     (raise) and misses (lower), so there is no signal to fire.
    @discardableResult
    func declineSuggestedSkip(
        windowId: String,
        isExplicitDenial: Bool = false
    ) async -> Bool {
        await declineSuggestedSkip(
            windowId: windowId,
            isExplicitDenial: isExplicitDenial,
            ifCurrentEpisodeId: activeEpisodeId
        )
    }

    /// Episode-bound form used by deferred banner actions. Persisted row
    /// updates continue to target the captured source window if an episode
    /// transition interleaves, but no live state or attribution is read from
    /// the replacement episode.
    @discardableResult
    func declineSuggestedSkip(
        windowId: String,
        isExplicitDenial: Bool = false,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64? = nil,
        ifSuggestionRevisionToken expectedRevisionToken: String? = nil
    ) async -> Bool {
        guard let expectedEpisodeId,
              activeEpisodeId == expectedEpisodeId,
              expectedPlaybackGeneration == nil
                || activePlaybackLifecycleGeneration
                    == expectedPlaybackGeneration,
              let sourceMaterialToken =
                suggestRevisionTokensByWindowId[windowId],
              expectedRevisionToken == nil
                || sourceMaterialToken == expectedRevisionToken
        else {
            return false
        }
        let sourceEpisodeId = activeEpisodeId
        let sourceLifecycleGeneration = episodeLifecycleGeneration
        // playhead-o4qr split audit: no caller-supplied show, so the identity
        // check cannot fail here. This seam is capture-only — its sole
        // catalog touch is `revokeRecurrenceEvidence`, a retraction that takes
        // only its show-free exact-source branch when the show is absent (see
        // that method). Nothing to split.
        let sourcePodcastId = activePodcastId
        guard let suggested = suggestWindows.removeValue(forKey: windowId) else {
            return false
        }

        guard isExplicitDenial else {
            // Neutral x / auto-fade — no explicit feedback.
            logger.debug("Suggest banner exited without feedback")
            return true
        }

        provisionallyResolvingSuggestWindowIds.insert(windowId)
        vetoedSuggestWindowIds.insert(windowId)

        // Explicit No response. Deliberately KEEP the id in
        // `suggestBanneredWindowIds`: presence there is what suppresses a
        // re-emit if the same markOnly id is re-delivered in-session (e.g. a
        // final-pass backfill push) — the `receiveAdWindows` guard treats a
        // present id as "already shown, skip." Removing it would un-suppress
        // the very banner the user just waved off. The window is already out
        // of `suggestWindows` (removed above); the row is flipped to
        // `.reverted` below to cover cross-launch replay.
        // Persist `userDismissedBanner = 1` and flip the row to `.reverted`
        // so the vetoed suggestion does not resurface on the next launch.
        let sourceNegativeAttribution = revertNegativeAttribution(
            for: suggested
        )
        do {
            try await revokeRecurrenceEvidence(
                for: suggested,
                showId: sourcePodcastId,
                source: .bannerSuggestionDenied,
                negativeAttribution: sourceNegativeAttribution
            )
            let correction = makeSuggestDenialCorrection(
                window: suggested,
                podcastId: sourcePodcastId
            )
            if let barrier = feedbackPersistenceBarrierForTesting {
                await barrier()
            }
            guard let wasNewlyInserted =
                    try await store.persistDeclinedSuggestionIfCurrent(
                        windowId: windowId,
                        analysisAssetId: suggested.analysisAssetId,
                        expectedEpisodeId: expectedEpisodeId,
                        expectedStartTime: suggested.startTime,
                        expectedEndTime: suggested.endTime,
                        expectedProducerRevision: suggested,
                        expectedMaterialToken: sourceMaterialToken,
                        correction: correction
                    )
            else {
                throw SkipOrchestratorFeedbackError
                    .staleDurableMaterial
            }
            schedulePostCommitCorrectionLearning(
                correction,
                wasNewlyInserted: wasNewlyInserted
            )
        } catch {
            logger.warning("Banner feedback persistence failed")
            // The UI has not finalized its receipt yet. Restore the current
            // revision so the still-visible card can retry instead of claiming
            // a durable denial that was never written.
            if activeEpisodeId == sourceEpisodeId,
               episodeLifecycleGeneration == sourceLifecycleGeneration {
                vetoedSuggestWindowIds.remove(windowId)
            }
            await recoverFailedProvisionalSuggestion(
                windowId: windowId,
                fallback: suggested,
                sourceEpisodeId: sourceEpisodeId,
                sourceLifecycleGeneration: sourceLifecycleGeneration
            )
            return false
        }

        guard activeEpisodeId == sourceEpisodeId,
              episodeLifecycleGeneration == sourceLifecycleGeneration else {
            return true
        }
        provisionallyResolvingSuggestWindowIds.remove(windowId)
        bufferedSuggestProducerUpdates.removeValue(forKey: windowId)
        evaluateAndPush()
        return true
    }

    /// playhead-lc7z: build the `.falsePositive` CorrectionEvent for an
    /// explicitly denied suggest banner. This builds a fully-formed event so
    /// `causalSource` and `targetRefs` land on the row — the
    /// hard-negative miner needs both. The caller commits it atomically with
    /// the AdWindow denial.
    ///
    /// playhead-i08e: as with `makeManualCorrectionVetoEvent`, a wired
    /// `correctionStore` is not a precondition — the receipt's durability is
    /// owned by the AnalysisStore transaction, not by the derived-learning
    /// listener. See that method for why the precondition was unreachable in
    /// production yet disabled this seam everywhere else.
    private func makeSuggestDenialCorrection(
        window: AdWindow,
        podcastId: String?
    ) -> CorrectionEvent {
        let assetId = window.analysisAssetId
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: window.startTime,
            endTime: window.endTime
        )
        // Attribute the false positive to whatever composed the suggest
        // mark. `metadataSource` is the single reliable producer tag on a
        // mark-only window (the specialist stamps `specialist-v1`; FM-composed
        // marks stamp `foundationModels`). Unknown/absent tags default to
        // `.foundationModel`, matching `CausalInference.inferFromProvenance`.
        let causalSource = Self.causalSource(forMetadataSource: window.metadataSource)
        // Carry the brand (when present) so brand-as-editorial hard-negative
        // mining can key on it, plus the show id for show-level attribution.
        // The denied time span itself is carried by the scope above.
        let normalizedSponsor: String? = window.advertiser
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }
            .flatMap { $0.isEmpty ? nil : $0 }
        let targetRefs = CorrectionTargetRefs(
            domain: podcastId,
            sponsorEntity: normalizedSponsor
        )
        return CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            source: .bannerSuggestionDenied,
            podcastId: podcastId,
            correctionType: .falsePositive,
            // playhead-bwxi: WHERE THE LISTENER WAS.
            playheadTimeAtCorrection: observedPlayheadTimeForCorrection,
            causalSource: causalSource,
            targetRefs: CorrectionTargetRefs(
                adWindowId: window.id,
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(window),
                exactFeedbackSpan: ExactFeedbackSpan(
                    startTime: window.startTime,
                    endTime: window.endTime
                ),
                domain: targetRefs.domain,
                sponsorEntity: targetRefs.sponsorEntity
            )
        )
    }

    /// playhead-lc7z: map an `AdWindow.metadataSource` producer tag to the
    /// `CausalSource` most responsible for a suggest-tier mark. Kept static
    /// and pure so it is unit-testable without an orchestrator instance.
    static func causalSource(forMetadataSource metadataSource: String) -> CausalSource {
        switch metadataSource {
        case SpecialistMarkComposer.metadataSource:
            return .specialist
        case "foundationModels",
             // playhead-y3ya: a semantic-sweep mark IS a Foundation Model
             // verdict — the coarse `containsAd` that fusion could not attach.
             // It already resolved to `.foundationModel` through the default
             // below; stated explicitly so the attribution is a decision rather
             // than an inheritance.
             SemanticSweepMarkComposer.metadataSource:
            return .foundationModel
        default:
            // Absent / "none" / "fallback" tags carry no distinct producer
            // signal; default to FM, the most common mark-only composer —
            // the same fallback `CausalInference` uses when provenance is bare.
            return .foundationModel
        }
    }

    /// User tapped "Skip Ad" in manual mode. Promotes a confirmed window
    /// to applied and fires the skip cue.
    func applyManualSkip(
        windowId: String,
        isExplicitBannerFeedback: Bool = false
    ) {
        guard var managed = windows[windowId] else { return }
        guard managed.decisionState == .confirmed else { return }

        // playhead-ynmk: a banner confirmation asserts PRESENCE, not EXTENT.
        // When the derived per-edge policy yields no late-safe window there is
        // nothing safe to skip, so refuse — rather than flip to `.applied` and
        // persist `wasSkipped` for a cue that `paddedCueSpan` will drop.
        // `acceptSuggestedSkip` already declines to call us in that case; this
        // is the second door (a manual tap on the promoted row) closed too.
        if managed.adWindow.userAssertion == .userConfirmedSuggested,
           paddedCueSpan(for: managed) == nil {
            return
        }

        // playhead-98co: a manual "Skip Ad" tap on an ordinary detection row is
        // user-initiated — that row is exempt from edge padding so the span the
        // user acted on skips exactly. playhead-ynmk: NOT for a banner
        // confirmation, whose edges are the detector's; see
        // `isUserInitiatedSkip`, which ignores this set for asserted spans.
        if !isExplicitBannerFeedback {
            userInitiatedSkipWindowIds.insert(windowId)
        }

        managed.decisionState = .applied
        managed.cueActive = true
        windows[windowId] = managed

        if !isExplicitBannerFeedback {
            logDecision(
                managed: managed,
                decision: .applied,
                reason: "Manual skip by user"
            )
        }

        // playhead-rfu-sad: emit `auto_skip_fired` for the manual-skip path
        // too. The ol05 audit log treats every applied window as a real
        // skip — manual taps and auto-mode promotions are equivalent
        // skips from the user's perspective and from the
        // `false_ready_rate` denominator's perspective. Hashing routes
        // through the same `episodeIdHasher` as the auto path so events
        // pair byte-identically with `ready_entered`.
        //
        // Shadow mode is a "log-only, never actually skip" mode — the
        // auto path explicitly does NOT emit (see evaluateWindow's
        // .shadow case). Mirror that gate here so a manual-skip tap
        // delivered in shadow mode (e.g. test harness, dogfood
        // toggle) doesn't pollute the audit log with a real skip
        // event that never produced a real user-facing skip.
        // playhead-gard: mirrors the auto path, which is now per detector.
        if skipMode(for: managed.adWindow) != .shadow && !isExplicitBannerFeedback {
            emitAutoSkipFiredAuditEvent(for: managed)
        }

        // Persist.
        let id = managed.adWindow.id
        let analysisAssetId = managed.adWindow.analysisAssetId
        let expectedProducerRevision = managed.adWindow
        let catalogShowId = activePodcastId
        let appliedPersistenceBarrier =
            appliedPersistenceBarrierForTesting
        recurrenceBackgroundWorkCount += 1
        Task { [weak self, store, logger] in
            do {
                if let appliedPersistenceBarrier {
                    await appliedPersistenceBarrier()
                }
                let persisted = try await store.persistAppliedAdWindowIfEligible(
                    windowId: id,
                    analysisAssetId: analysisAssetId,
                    expectedProducerRevision: expectedProducerRevision
                )
                if persisted, !isExplicitBannerFeedback {
                    await self?.learnConfirmedRecurrence(
                        for: expectedProducerRevision,
                        showId: catalogShowId,
                        source: .manualSkip,
                        lifecycle: .explicitConfirmation
                    )
                }
            } catch {
                if isExplicitBannerFeedback {
                    logger.warning("Banner feedback follow-up failed")
                } else {
                    logger.warning(
                        "Failed to persist manual skip for \(id): \(error.localizedDescription)"
                    )
                }
            }
            await self?.finishRecurrenceBackgroundWork()
        }

        evaluateAndPush()
    }

    /// The active skip mode for the current episode.
    func currentSkipMode() -> SkipMode {
        activeSkipMode
    }

    /// playhead-djl0: WHY the active skip mode holds its current value.
    ///
    /// Always a companion to ``currentSkipMode()``, never a replacement: the
    /// skip policy reads the MODE, and this reads the CAUSE. Consumers use it
    /// to tell a deliberate shadow apart from a session that lost the show.
    func currentSkipModeResolution() -> SkipModeResolution {
        activeSkipModeResolution
    }

    /// playhead-djl0: how many episode starts have hit `resolution` this
    /// process. Always `0` for a resolution that is not a lookup failure — a
    /// successful lookup is not an event worth tallying, and counting it would
    /// make the failure numbers unreadable.
    func skipModeResolutionFailureCount(_ resolution: SkipModeResolution) -> Int {
        skipModeResolutionFailureCounts[resolution] ?? 0
    }

    /// Override the active skip mode for the current episode and re-evaluate pending windows.
    ///
    /// playhead-wq34 made this `async`, and the reason is a regression this bead
    /// would otherwise have shipped. `setActiveSkipMode` is the PRODUCTION pill
    /// (`PlayheadRuntime.setShowSkipMode`, the Settings / Now Playing control)
    /// and it is the only writer of `activeDetectorSkipModes` that does NOT
    /// clear `windows` — so it is the one place a live episode's rows can
    /// outlive the mode they were admitted under.
    ///
    /// Before wq34, a row delivered on a `.shadow` show sat silently in the
    /// managed tier, and flipping the pill to `.auto` promoted it on the spot.
    /// After wq34 that row is a suggest card instead, and `evaluateAndPush`
    /// iterates `windows` — which no longer holds it. Left there, "turn
    /// auto-skip on" would have stopped working for everything already
    /// delivered, which is exactly the "must not alter behaviour for a class
    /// that IS `.auto`" line this bead was told not to cross.
    ///
    /// The remedy is to ASK THE DOOR AGAIN rather than to re-route by hand —
    /// see `readmitModeDivertedSuggestions`.
    func setActiveSkipMode(_ mode: SkipMode) async {
        activeSkipMode = mode
        // playhead-gard: an explicit session choice governs EVERY detector,
        // including the show-trust-exempt one. `.rediffByteExact` is exempt
        // from the show's HISTORY, never from a live instruction — the whole
        // point of the control is that what it says is what happens.
        activeDetectorSkipModes = DetectorSkipModes(
            showMode: mode,
            resolution: .sessionOverride,
            byDetector: Dictionary(
                uniqueKeysWithValues: SkipDetectorClass.allCases.map {
                    ($0, mode)
                }
            )
        )
        // playhead-djl0: an explicit choice replaces whatever the lookup
        // concluded. Leaving a failure cause installed here would keep telling
        // the listener their show was unrecognized after they had answered the
        // question themselves.
        activeSkipModeResolution = .sessionOverride
        // playhead-usn1: an explicit choice is a transition like any other.
        publishSkipMode()
        evaluateAndPush()
        await readmitModeDivertedSuggestions()
    }

    /// playhead-wq34: re-run ADMISSION for the suggest-tier rows a previous
    /// mode may have put there, so an explicit instruction takes effect on the
    /// episode the listener is actually holding.
    ///
    /// **It re-delivers through `receiveAdWindows` rather than moving rows
    /// between the two maps**, and that is the whole design. A hand-rolled
    /// promotion would be a SECOND admission path — it would have to re-derive
    /// the catalog-authority check, the inventory filter, the terminal-state
    /// fences and the edge-anchor stamp, and every one of those is a place the
    /// two copies could come to disagree. Re-delivery gets all of them by
    /// construction, and three properties fall out for free rather than needing
    /// their own invariants:
    ///
    ///   * A row diverted by `catalogProvenanceMustFailClosed` FAILS CLOSED
    ///     AGAIN, because the check simply runs again. A user saying "auto-skip
    ///     on this show" is not permission to trust a catalog claim nobody can
    ///     authenticate, and no bookkeeping set is needed to remember which
    ///     rows those were.
    ///   * A card the listener has already been shown is not shown twice:
    ///     `registerSuggestedWindow` sees an exact replay and declines to
    ///     re-arm (`suggestReplayNotRearmed`).
    ///   * A row the user has already answered is refused by
    ///     `hasTerminalSuggestResolution`.
    ///
    /// `.markOnly` rows are excluded because their tier is a PRECISION verdict
    /// about the row, not a consequence of the mode — no instruction promotes
    /// them, and re-delivering them would only churn the ingest census.
    ///
    /// SCOPE, deliberately: this is the UPWARD direction only — the one wq34
    /// would otherwise have broken. The downward case (a row admitted to the
    /// managed tier under `.auto`, still un-applied when the listener turns the
    /// control down, going silent) is pre-existing and is filed as
    /// playhead-4xw4. Re-delivering managed rows would also resurrect ones the
    /// runtime has since `.suppressed`, since the producer row carries the
    /// original `decisionState` — a bigger change than this bead is allowed.
    ///
    /// ONE KNOWN NARROWNESS, stated rather than papered over: re-delivery takes
    /// a fresh producer-mutation generation, so a genuine producer revision for
    /// the same id that is suspended at the catalog-authority hop when the pill
    /// is tapped will find itself stale and be dropped. The listener loses one
    /// revision of one window, and the next hot-path or backfill push delivers
    /// it again. That is preferable to the alternative — skipping re-admission
    /// for ids with a delivery in flight — which would make whether the pill
    /// worked at all depend on timing the listener cannot see.
    private func readmitModeDivertedSuggestions() async {
        guard activeAssetId != nil else { return }
        let candidates = suggestWindows.values
            .filter { window in
                guard let raw = window.eligibilityGate else { return true }
                return SkipEligibilityGate(rawValue: raw) != .markOnly
            }
            .sorted { $0.id < $1.id }
        guard !candidates.isEmpty else { return }
        await receiveAdWindows(candidates)
    }

    /// The show identity this session actually resolved, including one
    /// recovered from the durable job row (playhead-djl0).
    ///
    /// playhead-usn1 promoted this out of `#if DEBUG`. djl0's recovery gave the
    /// ORCHESTRATOR an identity the runtime did not have, and nothing carried it
    /// back — so a session could hold a valid trust profile (pill selectable,
    /// because the resolution reports a resolved identity) while
    /// `PlayheadRuntime.currentPodcastId` stayed nil and every write silently
    /// went nowhere. That is precisely the "menu accepts a choice and forgets
    /// it" defect djl0 withheld the control to avoid; closing the loop is
    /// better than withholding.
    func activeShowIdentity() -> String? {
        activePodcastId
    }

    #if DEBUG
    /// Legacy spelling kept for the playhead-djl0 suites.
    func activePodcastIdForTesting() -> String? {
        activeShowIdentity()
    }
    #endif

    /// Windows in the confirmed state (available for manual skip UI).
    func confirmedWindows() -> [AdWindow] {
        windows.values
            .filter { $0.decisionState == .confirmed }
            .sorted { $0.snappedStart < $1.snappedStart }
            .map(\.adWindow)
    }

    // MARK: - Decision Log Access

    /// Return the decision log for the evaluation harness.
    func getDecisionLog() -> [SkipDecisionRecord] {
        decisionLog
    }

    func activeWindowIDs() -> Set<String> {
        Set(windows.keys)
    }

    /// playhead-hygc.1.8: snapshot of suggest-tier (markOnly) window IDs.
    /// Mirrors `activeWindowIDs()` for the auto-skip dictionary and is
    /// used by revert-tier coverage tests to assert that a user veto via
    /// `revertByTimeRange` actually clears suggest entries.
    func activeSuggestWindowIDs() -> Set<String> {
        Set(suggestWindows.keys)
    }

    /// playhead-bwxi: snapshot of auto-skip-tier windows whose banner is
    /// DECIDED but not yet PRESENTED (the playhead has not entered the span).
    ///
    /// Exists so a test can tell the two halves of this bead apart: "the skip
    /// decision was not made" and "the decision was made and the card is
    /// waiting for the playhead" look identical from the banner stream, and
    /// only one of them is a regression.
    func armedAutoSkipBannerWindowIDs() -> Set<String> {
        armedAutoSkipBannerWindowIds
    }

    /// playhead-2d6i: the passive list. Auto-skips that fired while no banner
    /// host was attached, in episode order, and STILL CORRECTABLE.
    ///
    /// THE FILTER IS THE FEATURE, not defensiveness. Dan's requirement is that
    /// a list entry can correct — "if correcting from the list cannot reach
    /// `confirmAutoSkippedBanner`'s counterpart, the feature is decorative" —
    /// so this returns exactly the receipts whose veto would be ACCEPTED. The
    /// predicate is `denyAutoSkippedBanner`'s own preconditions, re-derived
    /// from live state rather than remembered:
    ///
    ///   * the window is still managed and still `.applied` — a span already
    ///     reverted by a transcript veto, a listen-rewind or an earlier tap on
    ///     this same list has nothing left to correct;
    ///   * its material token still matches the one on the card — a producer
    ///     that recomputed the span in place must not have an old receipt
    ///     answered on its behalf (the same rule the card's Yes/No enforces).
    ///
    /// Deriving it here rather than pruning at each retirement site is what
    /// keeps the list honest without a removal call in every path that can
    /// retire a window. A row that would be refused is never shown.
    ///
    /// This is a PULL, deliberately: the suggest tier PUSHES on subscribe
    /// because its cards are a live affordance, and this is a record of
    /// something already done. Pulling is also what makes "exactly one entry,
    /// not one per attach" unfalsifiable — reading twice returns the same list.
    /// playhead-2d6i: TEST-ONLY OBSERVABILITY — the RAW row count, before the
    /// live-state filter.
    ///
    /// It exists because the two per-episode clears MASK EACH OTHER, and a rail
    /// that cannot see its own subject is not a rail. `missedAutoSkipReceipts()`
    /// derives vetoability from `windows`, and `endEpisode` clears `windows`
    /// too — so after `endEpisode` the public accessor returns an empty list
    /// whether or not the dictionary was cleared, and the only way back to a
    /// populated `windows` is `beginEpisode`, which clears the dictionary
    /// itself. Mutation MS06 (delete the `endEpisode` clear) therefore SURVIVED
    /// twice with every focused suite green, including the test named "Missed
    /// receipts do not survive an episode boundary" — an empty list read as
    /// evidence of a clear that had not run.
    ///
    /// Production must not read this: the filter is the contract, and a raw
    /// count includes rows no surface may offer.
    func _missedAutoSkipReceiptCountForTesting() -> Int {
        missedAutoSkipReceiptsByWindowId.count
    }

    func missedAutoSkipReceipts() -> [MissedAutoSkipReceipt] {
        missedAutoSkipReceiptsByWindowId.values
            .filter { receipt in
                guard let managed = windows[receipt.item.windowId],
                      managed.decisionState == .applied,
                      bannerMaterialRevisionToken(for: managed)
                        == receipt.item.windowMaterialRevisionToken
                else {
                    return false
                }
                return true
            }
            .sorted {
                if $0.item.adStartTime != $1.item.adStartTime {
                    return $0.item.adStartTime < $1.item.adStartTime
                }
                return $0.item.windowId < $1.item.windowId
            }
    }

    /// Snapshot of suggestions whose banner delivery was acknowledged by the
    /// active host. Used by lifecycle tests to prove a stale observer cannot
    /// suppress replay in a replacement transaction.
    func acknowledgedSuggestWindowIDs() -> Set<String> {
        suggestBanneredWindowIds
    }

    /// Snapshot of window IDs for which `emitBannerItem` actually
    /// reached the yield-to-subscriber path this episode. The set is
    /// populated ONLY by emission to active subscribers, never by
    /// pre-population — so a `.contains(id) == false` assertion proves
    /// no banner was emitted regardless of `evaluateAndPush` iteration
    /// order. Used by `testPreloadedAppliedWindowDoesNotEmitBanner` and
    /// `testEndEpisodeResetsEmittedAutoSkipBannersSet`.
    ///
    /// Cycle-25 L-2 precision: `emitBannerItem` early-returns when
    /// `bannerContinuations` is empty (no UI listening). The set is
    /// therefore populated only when emission has both a window AND
    /// a subscriber. Tests that rely on `.contains(id) == true` must
    /// subscribe to `bannerItemStream()` BEFORE `beginEpisode`,
    /// otherwise the emission never reaches the insert site and the
    /// snapshot stays empty (correctly — no banner was actually shown).
    ///
    /// Cycle-23 L-2: this is test-only observability — production
    /// callers must not couple to it.
    ///
    /// Cycle-28 L-C / Cycle-29 L-1 cross-reference: see the field doc
    /// on `emittedAutoSkipBannerWindowIds` for *why* this set exists
    /// instead of a snapshot of `banneredWindowIds`. The 4 production
    /// writers of `banneredWindowIds` are enumerated in `emitBannerItem`'s
    /// comment; of those, `beginEpisode`'s preload pre-population and the
    /// accepted-suggestion suppression insert WITHOUT a corresponding
    /// `emitBannerItem` call. A `banneredWindowIds`
    /// snapshot cannot distinguish "preload pre-populated this id" from
    /// "an existing card already handled this id" from "eval-loop emitted
    /// then inserted." This emission set is populated
    /// only by `emitBannerItem` and only after the subscriber-gate, so
    /// its absence/presence is unambiguous emission evidence regardless
    /// of preload state.
    func emittedAutoSkipBannersSnapshot() -> Set<String> {
        emittedAutoSkipBannerWindowIds
    }

    /// playhead-8cjo: snapshot of window IDs whose auto-skip card a host
    /// reported the BANNER QUEUE ACCEPTED.
    ///
    /// **This is the honest "the QUEUE TOOK IT", and the snapshot above is
    /// not even that.** `emittedAutoSkipBannersSnapshot()` says the item reached the
    /// yield-to-subscriber path, which is what this bead's defect read as a
    /// presentation: `AdBannerQueue.enqueue` can refuse the item afterwards,
    /// and the observation `Task` can be cancelled before the enqueue happens
    /// at all. `delivered ⊆ emitted` always, and the difference between the
    /// two is exactly the population that used to leave no trace anywhere.
    ///
    /// Test-only observability, like its sibling. Production reads neither.
    func deliveredAutoSkipCardWindowIDs() -> Set<String> {
        deliveredAutoSkipCardWindowIds
    }

    // MARK: - Core Skip Policy

    /// Evaluate all managed windows and determine which should have active
    /// skip cues. Applies hysteresis, merging, minimum span, seek suppression.
    private func evaluateAndPush() {
        guard activeAssetId != nil else { return }

        // 1. Collect eligible windows (confirmed or candidate with sufficient confidence).
        //    Sort by snappedStart so hysteresis (inAdState) is evaluated in temporal order.
        var eligible: [ManagedWindow] = []
        let sortedWindows = windows.sorted { $0.value.snappedStart < $1.value.snappedStart }
        for (id, var managed) in sortedWindows {
            // Skip already-terminal states.
            if managed.decisionState == .applied
                || managed.decisionState == .suppressed
                || managed.decisionState == .reverted {
                // Keep applied windows as active cues.
                if managed.decisionState == .applied {
                    // Emit a banner on first encounter (e.g. after applyManualSkip).
                    if !banneredWindowIds.contains(managed.adWindow.id) {
                        // Cycle-27 T-3 production-writer site (2 of 4): evaluateAndPush terminal-state branch.
                        banneredWindowIds.insert(managed.adWindow.id)
                        // playhead-bwxi: ARM, do not present. See
                        // `armedAutoSkipBannerWindowIds`.
                        armAutoSkipBanner(for: managed)
                    }
                    eligible.append(managed)
                }
                continue
            }

            let previousState = managed.decisionState
            let decision = evaluateWindow(&managed)
            if decision != previousState {
                managed.decisionState = decision
                windows[id] = managed
            }

            // Auto-tier copy promises that playback actually skipped this
            // span. A `.confirmed` decision in shadow/manual mode (or an
            // edge-padding veto) has not skipped anything and must not surface
            // as "Skipped …".
            if decision == .applied,
               !banneredWindowIds.contains(managed.adWindow.id) {
                // Cycle-27 T-3 production-writer site (3 of 4): evaluateAndPush promotion branch.
                banneredWindowIds.insert(managed.adWindow.id)
                // playhead-bwxi: ARM, do not present. See
                // `armedAutoSkipBannerWindowIds`.
                armAutoSkipBanner(for: managed)
            }

            if decision == .applied {
                eligible.append(managed)
            }
        }

        // 2. Compute each window's playback SKIP SPAN (playhead-98co:
        //    identity when edge padding is OFF — the default; the derived
        //    late-safe margins when ON), then merge adjacent spans with
        //    small gaps. Padding applies ONLY to the skip cues pushed to
        //    playback: banners, decision records, and the applied-segment
        //    broadcast below all keep the full snapped span.
        let skipSpans = eligible.compactMap { paddedCueSpan(for: $0) }
        let merged = mergeAdjacentWindows(skipSpans)

        // 3. Push skip cues to PlaybackService.
        pushMergedCues(merged)

        // 4. Broadcast updated segments to UI listeners.
        broadcastAppliedSegments()
    }

    // MARK: - playhead-98co: edge-padded skip spans

    /// The playback skip span for a managed window: the snapped span when
    /// edge padding is disabled or the skip is user-initiated; otherwise
    /// the `AutoSkipEdgePadding` late-safe window (shrink-only), or nil
    /// when no late-safe window exists (cue suppressed — the span keeps
    /// its banner/marker surfacing but is never auto-skipped).
    ///
    /// Also consulted by `evaluateWindow`'s auto-mode veto so a span with
    /// no late-safe window is demoted to `.confirmed` (markOnly behavior)
    /// BEFORE the `.applied` promotion — no `auto_skip_fired` audit event
    /// and no `inAdState` flip for a skip that will never fire.
    private func paddedCueSpan(for managed: ManagedWindow) -> (start: Double, end: Double)? {
        // playhead-hdgk: per-edge anchor provenance is derived at fusion time
        // (rediff `.rediffSlot` width ownership + `StingerRefiner` snap trace),
        // persisted on the `AdWindow` row, and stamped into
        // `edgeAnchorsByWindowId` at `receiveAdWindows` ingest — before any
        // promotion. An absent entry (a span that never flowed through ingest,
        // or a non-fusion producer) defaults both edges to `.unanchored`, under
        // which flag-ON auto-skips nothing — the intended conservative posture.
        //
        // playhead-qs0d: the anchors are resolved BEFORE the enable check
        // because the enable is now per-span. `AutoSkipEdgePadding.isActive`
        // answers the master switch verbatim for every anchor combination but
        // one: a span the byte differ owns on BOTH edges is padded even while
        // the Gate-2 master switch is off. Padding is shrink-or-suppress only,
        // so this can never admit a skip that would not otherwise fire.
        //
        // playhead-ynmk: a THIRD activation, on the same shrink-or-suppress
        // safety argument. A span whose PRESENCE the user asserted but whose
        // EDGES the detector drew (`UserSpanAssertion.assertsExtent == false`,
        // i.e. an accepted suggestion) is governed by this policy regardless of
        // the master switch and regardless of anchor tier. Before this bead it
        // was EXEMPT — and that exemption is how one tap skipped 150 s of show
        // over a 0.40-confidence span with neither edge anchored.
        //
        // Note the SHAPE of the expression, which the ynmk mutation battery
        // corrected. The user-initiated exemption is the OUTER term, applied
        // uniformly, rather than a second operand of the activation `||`. The
        // shorter `extentIsDetectorOwned || (isActive && !isUserInitiated)`
        // reads the same but short-circuits `isUserInitiatedSkip` away entirely
        // for exactly the spans this bead is about — which made both of that
        // function's ynmk guards unreachable, and made three separate mutations
        // of them survive with the suite green. Behaviour is identical either
        // way; only this shape is actually TESTED.
        let anchors = edgeAnchorsByWindowId[managed.adWindow.id]
            ?? (start: AutoSkipEdgeAnchor.unanchored, end: AutoSkipEdgeAnchor.unanchored)
        let extentIsDetectorOwned = managed.adWindow.userAssertion
            .map { !$0.assertsExtent } ?? false
        let policyGovernsThisSpan =
            !isUserInitiatedSkip(managed)
            && (
                extentIsDetectorOwned
                || AutoSkipEdgePadding.isActive(
                    masterEnabled: edgePaddingEnabled,
                    startAnchor: anchors.start,
                    endAnchor: anchors.end
                )
            )
        guard policyGovernsThisSpan else {
            return (start: managed.snappedStart, end: managed.snappedEnd)
        }
        return AutoSkipEdgePadding.skipWindow(
            spanStart: managed.snappedStart,
            spanEnd: managed.snappedEnd,
            startAnchor: anchors.start,
            endAnchor: anchors.end,
            showKey: activePodcastId
        )
    }

    /// Whether this window's skip EXTENT was chosen by the user — exempt from
    /// edge padding, because those edges are the user's own.
    ///
    /// playhead-ynmk narrowed this. It used to answer `true` for
    /// `userConfirmedSuggested` as well, on the premise that a banner Yes chose
    /// the edges. It did not: the banner asks "is this an ad?" about a span the
    /// DETECTOR drew. `UserSpanAssertion.assertsExtent` is the split, and it is
    /// consulted BEFORE `userInitiatedSkipWindowIds` so a confirmation can
    /// never be re-exempted through the id set by a later manual-tap path.
    private func isUserInitiatedSkip(_ managed: ManagedWindow) -> Bool {
        if let assertion = managed.adWindow.userAssertion {
            return assertion.assertsExtent
        }
        return userInitiatedSkipWindowIds.contains(managed.adWindow.id)
    }

    /// The per-edge anchor tier for a window: the runtime stamp when ingest
    /// recorded one, else the row's own persisted provenance.
    ///
    /// playhead-ynmk: `acceptSuggestedSkip` needs this because a suggest-tier
    /// window never reaches the `receiveAdWindows` stamp site (the markOnly
    /// branch returns first), so `edgeAnchorsByWindowId` has no entry for it.
    private func resolvedEdgeAnchors(
        for window: AdWindow
    ) -> (start: AutoSkipEdgeAnchor, end: AutoSkipEdgeAnchor) {
        if let stamped = edgeAnchorsByWindowId[window.id] {
            return stamped
        }
        // playhead-bllt: through the SHARED row decode, not a local re-spelling.
        let support = window.extentSupport
        return (start: support.startAnchor, end: support.endAnchor)
    }

    /// `resolvedEdgeAnchors` as the extent-support value the playhead-2350 /
    /// playhead-bllt rule is stated over.
    private func resolvedExtentSupport(for window: AdWindow) -> SpanExtentSupport {
        let anchors = resolvedEdgeAnchors(for: window)
        return SpanExtentSupport(
            startAnchor: anchors.start,
            endAnchor: anchors.end
        )
    }

    /// playhead-gard: which detector produced this window, using the runtime
    /// anchor stamp when ingest recorded one.
    ///
    /// Prefers `resolvedEdgeAnchors` over the row's own columns for the same
    /// reason `paddedCueSpan` does: ingest can know a provenance the persisted
    /// row has not been rewritten with yet, and a window classified as
    /// `.fusion` when it is really byte-exact would be gated by another
    /// detector's history — the defect, one level down.
    private func detectorClass(for window: AdWindow) -> SkipDetectorClass {
        let anchors = resolvedEdgeAnchors(for: window)
        return SkipDetectorClass.classify(
            boundaryState: window.boundaryState,
            startAnchor: anchors.start,
            endAnchor: anchors.end
        )
    }

    /// The skip mode governing this window's detector class.
    private func skipMode(for window: AdWindow) -> SkipMode {
        activeDetectorSkipModes.mode(for: detectorClass(for: window))
    }

    /// playhead-wq34: the mode `evaluateWindow` WILL read for this row, asked
    /// at ADMISSION — before the row has been stamped into
    /// `edgeAnchorsByWindowId`.
    ///
    /// Not `skipMode(for:)`, and the difference is the whole point. That reads
    /// the CURRENT stamp; both admission doors REPLACE an older stamp whenever
    /// the row is a materially changed producer revision. Routing on the
    /// pre-stamp anchors and evaluating on the post-stamp ones would be two
    /// expressions of one question — the playhead-6qvf shape — and a revision
    /// that changes a window's detector class (byte-exact edges replaced by
    /// unanchored ones, say) would slip back into the silent managed tier. This
    /// resolves the anchors exactly as the stamp site will, then classifies.
    private func admissionSkipMode(
        for adWindow: AdWindow,
        replacesManagedMaterial: Bool
    ) -> SkipMode {
        let anchors: (start: AutoSkipEdgeAnchor, end: AutoSkipEdgeAnchor)
        if let stamped = edgeAnchorsByWindowId[adWindow.id],
           !replacesManagedMaterial {
            anchors = stamped
        } else {
            // playhead-bllt: through the SHARED row decode, for the reason
            // `resolvedEdgeAnchors` states one screen down — the row→anchors
            // read had grown three spellings and was about to grow a fourth.
            // A router that exists to stop two expressions of one question
            // (see the doc above) must not be a fourth expression of another.
            let support = adWindow.extentSupport
            anchors = (start: support.startAnchor, end: support.endAnchor)
        }
        return activeDetectorSkipModes.mode(
            for: SkipDetectorClass.classify(
                boundaryState: adWindow.boundaryState,
                startAnchor: anchors.start,
                endAnchor: anchors.end
            )
        )
    }

    // MARK: - playhead-wq34: the eligibility ladder is an ORDINAL

    /// Would admitting this row to the managed tier produce NOTHING the
    /// listener can see or act on?
    ///
    /// THE DEFECT THIS EXISTS TO REMOVE. `eligibilityGate` is a ladder — a row
    /// that earned `.eligible` carries strictly more provenance than one
    /// stamped `.markOnly` — and until this bead the ladder INVERTED at the
    /// listener:
    ///
    ///   * `.markOnly` routes to the SUGGEST tier, which arms a card and fires
    ///     it when the playhead enters the span. The suggest path never reads
    ///     the skip mode, so the card appears in `.shadow`, `.manual` and
    ///     `.auto` alike.
    ///   * `.eligible` (and `nil`, and the `"autoSkip"` literal) routes to the
    ///     MANAGED tier, where `evaluateWindow` returns `.confirmed` for
    ///     `.shadow` and `.manual` and `evaluateAndPush` banners only on
    ///     `.applied`. For any class that is not `.auto`: no skip AND no card.
    ///
    /// Post-playhead-lqcp, `.segmentAggregated`, `.fusion` and `.userAsserted`
    /// cannot reach `.auto` without an explicit user override, so on the
    /// shipped default the pipeline's HIGHEST-confidence output was the one the
    /// listener never saw. A row that earned more produced less.
    ///
    /// **The rule: a row the managed tier cannot act on takes the suggest tier
    /// instead.** Not "emits a card of its own" — takes the same door, the same
    /// arming, the same emit trigger, the same Yes/No transaction. That is what
    /// makes the monotonicity claim airtight rather than approximately true:
    /// the two paths are not compared, they are THE SAME PATH.
    ///
    /// WHY HERE AND NOT IN `evaluateWindow`'S MODE SWITCH. The mode switch is
    /// where the decision's cause lives, so it looks like the natural home. It
    /// is not, for a reason that is not stylistic: `evaluateWindow` applies the
    /// late-detection check, seek suppression, `enterThreshold`/`stayThreshold`
    /// hysteresis, `minimumSpanSeconds` and `shortSpanOverrideConfidence`
    /// BEFORE it ever reaches the mode switch, and each of those RETURNS EARLY.
    /// The suggest tier applies none of them. So a fallback inside the mode
    /// switch would leave a sub-threshold `.eligible` row silent while the
    /// identical row stamped `.markOnly` still got a card — the same inversion,
    /// moved from the gate axis onto the confidence axis. There is also a
    /// structural reason: the two tiers are DISJOINT by construction
    /// (playhead-rfu-sad — a window in both `windows` and `suggestWindows` can
    /// re-fire `acceptSuggestedSkip` and synthesize a duplicate managed window
    /// under a fresh UUID), and tier membership is decided at exactly one place
    /// per door. A fallback inside an evaluation pass would either create that
    /// forbidden state or reach across from inside a loop that runs on every
    /// `evaluateAndPush` rather than once per producer revision.
    ///
    /// `.shadow` AND `.manual` ARE TREATED IDENTICALLY, and after this bead
    /// that is a requirement rather than the coincidence it used to be. A
    /// `.markOnly` row cards in BOTH, because the suggest path does not consult
    /// the mode at all. Making `.eligible` silent in `.shadow` and carded in
    /// `.manual` would therefore re-create the inversion for `.shadow`
    /// specifically. Monotonicity forces the two arms together; only the
    /// justification differs (in `.shadow` the answer is also the observation
    /// the ladder needs to learn from; in `.manual` the card IS the "awaiting
    /// user tap" that mode's own comment promises).
    ///
    /// THE TWO `.applied` EXCLUSIONS ARE NOT DEFENSIVENESS. An already-applied
    /// row is a durable receipt for a skip that HAPPENED, and both are
    /// reachable with a non-`.auto` mode — through `applyManualSkip`, through
    /// `acceptSuggestedSkip`'s promoted row, or through a class that was
    /// `.auto` when the row was applied in an earlier session and has since
    /// demoted. Diverting either would turn a receipt back into a question, and
    /// the incoming case is worse than cosmetic: `forwardPersistedAdWindows`
    /// pre-populates `banneredWindowIds` for durably-`.applied` rows, so such a
    /// row would land on `.droppedAlreadyBannered` — retired from the managed
    /// tier, no card, and no cue. Cross-launch auto-skip would silently
    /// regress. Nothing here changes behaviour for a class that IS `.auto`: the
    /// predicate is false for it, at both doors.
    private func managedTierWouldBeSilent(
        mode: SkipMode,
        incomingState: SkipDecisionState,
        existingState: SkipDecisionState?
    ) -> Bool {
        guard incomingState != .applied, existingState != .applied else {
            return false
        }
        switch mode {
        case .shadow, .manual:
            return true
        case .auto:
            return false
        }
    }

    /// playhead-gard: what a veto of this window is evidence about — the
    /// detector that drew it, and how certain that span's EXTENT was.
    private func vetoAttribution(
        for window: AdWindow
    ) -> DetectorVetoAttribution {
        let anchors = resolvedEdgeAnchors(for: window)
        let support = SpanExtentSupport(
            startAnchor: anchors.start,
            endAnchor: anchors.end
        )
        return DetectorVetoAttribution(
            detector: SkipDetectorClass.classify(
                boundaryState: window.boundaryState,
                startAnchor: anchors.start,
                endAnchor: anchors.end
            ),
            tier: support.tier
        )
    }

    /// Attributions for a gesture that retracted SEVERAL windows at once (the
    /// time-range veto). Deduplication and tier selection happen inside
    /// `TrustScoringService`; this only collects.
    private func vetoAttributions(
        for windows: [AdWindow]
    ) -> [DetectorVetoAttribution] {
        windows.map { vetoAttribution(for: $0) }
    }

    /// Evaluate a single window against skip policy. Returns the decision.
    private func evaluateWindow(_ managed: inout ManagedWindow) -> SkipDecisionState {
        // playhead-ar60: the auto-skip policy is the actuation decision, so it
        // reads the ACTUATION number. Every threshold below — `enterThreshold`,
        // `stayThreshold`, `shortSpanOverrideConfidence` — was calibrated
        // against `DecisionResult.skipConfidence`, which the fusion path used
        // to smuggle in through `confidence`. Since V47 that column carries
        // DETECTION, and comparing a detection score against these thresholds
        // would make every span the user vetoed (or that calibration
        // discounted) more skip-eager than the decision that produced it.
        // `actuationConfidence` is `confidence` for producers with a single
        // number, so hot-path, rediff, user-mark and pre-V47 rows evaluate
        // exactly as before.
        let confidence = managed.adWindow.actuationConfidence
        let span = managed.snappedEnd - managed.snappedStart
        // playhead-gard: resolved once, read twice (candidate promotion and the
        // trust gate). Two reads of a per-window policy value is how the two
        // come to disagree.
        let windowSkipMode = skipMode(for: managed.adWindow)

        // Late detection: if the playhead is already past this window, never skip.
        if managed.snappedEnd <= currentPlayheadTime {
            let decision = SkipDecisionState.suppressed
            logDecision(managed: managed, decision: decision, reason: "Late detection -- playhead past window end")
            return decision
        }

        // Seek suppression: if user recently seeked, suppress new skips.
        if skipSuppressedAfterSeek {
            // Don't change state -- just don't promote to applied yet.
            return managed.decisionState
        }

        // Hysteresis: different thresholds for entering vs staying in ad state.
        let threshold = inAdState ? config.stayThreshold : config.enterThreshold

        if confidence < threshold {
            // Below threshold -- suppress if it was candidate.
            if managed.decisionState == .candidate {
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Below hysteresis threshold (\(confidence) < \(threshold))")
                return decision
            }
            // Confirmed but below stay threshold -- exit ad state.
            if managed.decisionState == .confirmed && confidence < config.stayThreshold {
                inAdState = false
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Exiting ad state: confidence dropped below stay threshold")
                return decision
            }
            return managed.decisionState
        }

        // Minimum span check.
        if span < config.minimumSpanSeconds {
            // Allow short spans only with very strong evidence.
            if confidence < config.shortSpanOverrideConfidence {
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Span too short (\(span)s < \(config.minimumSpanSeconds)s) without strong evidence")
                return decision
            }
        }

        // Boundary stability: only skip if the window boundary is stable
        // (not still being refined by incoming detection events).
        // Confirmed windows are considered stable; candidates must wait
        // for confirmation unless confidence is exceptionally high.
        if managed.decisionState == .candidate {
            // Candidates need confirmation before skipping.
            // In auto mode (trusted show), promote candidates above the
            // enter threshold without waiting for backfill confirmation.
            // Otherwise, only override if confidence is very high.
            if windowSkipMode == .auto && confidence >= config.enterThreshold {
                // Promote to confirmed — fall through to trust mode gate.
                managed.decisionState = .confirmed
            } else if confidence < config.shortSpanOverrideConfidence {
                return managed.decisionState
            }
        }

        // Trust mode gate: shadow mode logs only; manual mode marks confirmed
        // but does not auto-skip (UI shows a manual "Skip Ad" button instead).
        //
        // playhead-gard: the mode is THIS DETECTOR'S, not the show's. Before
        // this bead a single scalar gated every window, so three vetoes of
        // 0.40-confidence `segmentAggregated` spans (both edges unanchored, 0
        // of 3 correct) demoted the show and suppressed byte-exact rediff on it
        // — a deterministic signal that was 2 of 2 on the same corpus. A
        // detector class's own error history governs its own eligibility, and
        // nothing else's.
        //
        // playhead-wq34: THE `.shadow` AND `.manual` ARMS ARE IDENTICAL BY
        // DESIGN — they differ only in a log string — and after wq34 a row that
        // reaches either one is a row the listener has ALREADY been offered as
        // a suggest-tier card, or one no producer door could route (an
        // `injectUserMarkedAd` in-session mark, a mid-episode `setActiveSkipMode`
        // demotion of a row admitted while the class was `.auto`). The silent
        // `.confirmed` these return is no longer the ONLY thing a non-`.auto`
        // class produces; see `managedTierWouldBeSilent`, which is where the
        // fallback lives and which explains why it cannot live here.
        switch windowSkipMode {
        case .shadow:
            let decision = SkipDecisionState.confirmed
            logDecision(managed: managed, decision: decision, reason: "Shadow mode -- detection logged, no skip fired")
            return decision
        case .manual:
            let decision = SkipDecisionState.confirmed
            logDecision(managed: managed, decision: decision, reason: "Manual mode -- confirmed, awaiting user tap")
            return decision
        case .auto:
            // playhead-98co: edge-padding eligibility veto. When the
            // policy is enabled and this span has no late-safe skip
            // window (start edge unanchored/demoted, or the derived
            // margins consume the span), keep it .confirmed — markOnly
            // behavior: banner surfaces, no skip cue, no auto_skip_fired
            // audit event, no inAdState flip. Flag OFF (the default) or a
            // user-initiated skip always passes (paddedCueSpan returns
            // the snapped span unchanged).
            //
            // A demoted `.confirmed` state remains silent: auto-tier copy is
            // reserved for `.applied`, and no skip will fire for this span.
            if paddedCueSpan(for: managed) == nil {
                let decision = SkipDecisionState.confirmed
                logDecision(
                    managed: managed,
                    decision: decision,
                    reason: "Edge padding: no late-safe skip window (start unanchored/demoted or margins consume span) -- markOnly"
                )
                return decision
            }
            break // Proceed to auto-skip below.
        }

        // All checks passed -- apply the skip.
        inAdState = true
        let decision = SkipDecisionState.applied
        logDecision(managed: managed, decision: decision, reason: "Skip policy accepted (auto mode)")

        // playhead-o45p: emit an auto_skip_fired event to the ol05 state-
        // transition log. Paired with readyEntered events on the same
        // episode_id_hash, this is the numerator/denominator source for
        // the Wave 4 false_ready_rate dogfood metric.
        emitAutoSkipFiredAuditEvent(for: managed)

        // Persist to SQLite (fire-and-forget from the actor).
        let windowId = managed.adWindow.id
        let analysisAssetId = managed.adWindow.analysisAssetId
        let expectedProducerRevision = managed.adWindow
        let catalogShowId = activePodcastId
        let expectedEpisodeGeneration = episodeLifecycleGeneration
        let expectedLearningGeneration = catalogLearningGeneration
        let appliedPersistenceBarrier =
            appliedPersistenceBarrierForTesting
        recurrenceBackgroundWorkCount += 1
        Task { [weak self, store, logger] in
            do {
                if let appliedPersistenceBarrier {
                    await appliedPersistenceBarrier()
                }
                let persisted = try await store.persistAppliedAdWindowIfEligible(
                    windowId: windowId,
                    analysisAssetId: analysisAssetId,
                    expectedProducerRevision: expectedProducerRevision
                )
                if persisted {
                    await self?.queueConsumedCatalogLearning(
                        for: expectedProducerRevision,
                        showId: catalogShowId,
                        expectedEpisodeGeneration: expectedEpisodeGeneration,
                        expectedLearningGeneration: expectedLearningGeneration
                    )
                }
            } catch {
                logger.warning("Failed to persist skip state for \(windowId): \(error.localizedDescription)")
            }
            await self?.finishRecurrenceBackgroundWork()
        }

        return decision
    }

    // MARK: - Audit Event Emission

    /// playhead-o45p / playhead-rfu-sad: emit an `auto_skip_fired` audit
    /// event for a window that has just transitioned to `.applied`.
    ///
    /// Hashing routes through `episodeIdHasher` so all skip-event
    /// producers (auto-mode evaluation, manual taps) stamp byte-identical
    /// episode hashes — `false_ready_rate` correlation breaks the moment
    /// hashes diverge across producers. Called from EVERY site that
    /// finalises a real skip:
    ///   - `evaluateWindow` auto-mode promotion
    ///   - `applyManualSkip` (manual user tap on a confirmed window)
    /// The suggested-skip path (`acceptSuggestedSkip`) builds a confirmed
    /// `ManagedWindow` and re-evaluates; whichever of the two sites above
    /// fires next picks up the emission.
    private func emitAutoSkipFiredAuditEvent(for managed: ManagedWindow) {
        guard let episodeId = activeEpisodeId else { return }
        let hashed = episodeIdHasher(episodeId)
        let startMs = Int((managed.snappedStart * 1000.0).rounded())
        let endMs = Int((managed.snappedEnd * 1000.0).rounded())
        invariantLogger.recordAutoSkipFired(
            episodeIdHash: hashed,
            windowStartMs: startMs,
            windowEndMs: endMs
        )
    }

    // MARK: - Window Merging

    /// Merge adjacent skip spans with gaps smaller than mergeGapSeconds.
    /// (playhead-98co: takes the already-computed skip spans — snapped
    /// bounds when edge padding is OFF, padded bounds when ON — so the
    /// merge semantics are identical in both states.)
    private func mergeAdjacentWindows(_ spans: [(start: Double, end: Double)]) -> [(start: Double, end: Double)] {
        let sorted = spans.sorted { $0.start < $1.start }
        guard let first = sorted.first else { return [] }

        var merged: [(start: Double, end: Double)] = []
        var currentStart = first.start
        var currentEnd = first.end

        for span in sorted.dropFirst() {
            if span.start <= currentEnd + config.mergeGapSeconds {
                // Merge: extend the current range.
                currentEnd = max(currentEnd, span.end)
            } else {
                // Gap too large: emit current range, start new one.
                merged.append((start: currentStart, end: currentEnd))
                currentStart = span.start
                currentEnd = span.end
            }
        }

        merged.append((start: currentStart, end: currentEnd))
        return merged
    }

    // MARK: - Cue Pushing

    /// Convert merged ranges to CMTimeRanges and push to PlaybackService.
    ///
    /// playhead-vn7n.2: each merged range's trailing edge is pulled in by
    /// `adTrailingCushionSeconds`, ceding a small sliver of ad-tail rather than
    /// risking a clip into program-start audio. Cushion is applied per pod
    /// (per merged range), not per individual ad — by construction of
    /// `mergeAdjacentWindows`, anything beyond a merged range is either
    /// program audio (gap > `mergeGapSeconds`) or end-of-episode, so it is
    /// safe to apply the cushion uniformly to every range end. End is
    /// clamped at the pod's start so the skip end never precedes the skip
    /// start (e.g., a 5 s ad with a 10 s cushion collapses to a zero-length
    /// cue at `adStart`).
    private func pushMergedCues(_ ranges: [(start: Double, end: Double)]) {
        // playhead-vn7n.1: diagnostic — log each cue we are about to push so we
        // can compare cue.end against the underlying detection AdWindow.endTime
        // (which has not been snap-expanded). The closest managed ad window
        // covering the cue range is selected as the underlying detection
        // reference (lookup is a single linear scan; cue lists are tiny).
        let sortedRanges = ranges.sorted { $0.start < $1.start }

        // Defensive: clamp to non-negative so a future misconfigured caller
        // can't invert the cushion (skip-end before ad-end).
        let cushion = max(0.0, config.adTrailingCushionSeconds)
        let cues = sortedRanges.map { range -> CMTimeRange in
            let cushionedEnd = max(range.start, range.end - cushion)
            let start = CMTime(seconds: range.start, preferredTimescale: 600)
            let duration = CMTime(seconds: cushionedEnd - range.start, preferredTimescale: 600)
            return CMTimeRange(start: start, duration: duration)
        }
        pushSkipCues(cues)
    }

    /// Look up the underlying detection-side AdWindow.endTime for a merged
    /// cue range. Picks the managed window whose snapped range overlaps the
    /// cue and whose snappedEnd is closest to the cue end. Returns -1.0
    /// when no overlap is found (logged as a sentinel rather than NaN to
    /// keep the field log line shape stable).
    ///
    /// playhead-rfu-sad: candidates are sorted by `(gap, adWindow.id)` so
    /// ties (gap == 0, two windows ending at exactly the same time) pick
    /// a deterministic winner instead of whichever order
    /// `windows.values` happens to yield. Diagnostic-only output, but
    /// nondeterministic logging makes flaky test diagnostics harder.
    private func nearestAdWindowEnd(forCueStart cueStart: Double, cueEnd: Double) -> Double {
        let candidates = windows.values
            .filter { $0.snappedStart < cueEnd && $0.snappedEnd > cueStart }
            .map { (gap: abs($0.snappedEnd - cueEnd), id: $0.adWindow.id, end: $0.adWindow.endTime) }
            .sorted { lhs, rhs in
                if lhs.gap != rhs.gap { return lhs.gap < rhs.gap }
                return lhs.id < rhs.id
            }
        return candidates.first?.end ?? -1.0
    }

    /// Push skip cues to PlaybackService via the handler. Defaults to empty.
    private func pushSkipCues(_ cues: [CMTimeRange] = []) {
        skipCueHandler?(cues)
    }

    // MARK: - User Correction Injection

    /// Inject a user-marked ad segment immediately into the skip orchestrator.
    /// Creates a ManagedWindow with confidence=1.0 and .confirmed state, then
    /// evaluates and pushes skip cues so the segment takes effect in real time.
    ///
    /// Called from PlayheadRuntime when the user taps "Hearing an ad" or marks
    /// transcript chunks as an ad.
    func injectUserMarkedAd(
        start: Double,
        end: Double,
        analysisAssetId: String,
        windowId: String = UUID().uuidString
    ) {
        guard activeAssetId == analysisAssetId,
              Self.hasValidRuntimeWindowMaterial(
                  id: windowId,
                  analysisAssetId: analysisAssetId,
                  startTime: start,
                  endTime: end,
                  confidence: 1
              ) else {
            return
        }

        // Synthesize an AdWindow for the user-marked region.
        let adWindow = AdWindow(
            id: windowId,
            analysisAssetId: analysisAssetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: start,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false,
            // playhead-o4qr: this stamp is MANDATORY, and its absence was a
            // real defect rather than a cosmetic gap.
            //
            // `AdDetectionService.recordUserMarkedAd` persists the durable row
            // for this same window with `eligibilityGate: .eligible` (see
            // playhead-527u: a manual mark is the highest-certainty "this IS an
            // ad" signal we have). This synthesized in-memory twin left it nil,
            // and `eligibilityGate` is part of
            // `AdWindowMaterialIdentity.producerRevisionToken` — so the two
            // representations of one window were NOT the same producer
            // revision.
            //
            // That was inert until o4qr routed the correction seams through
            // `persistRevertedAdWindowsIfCurrent`, which validates the exact
            // producer revision before mutating a row. From then on, vetoing a
            // freshly marked ad IN THE SAME SESSION failed the revision check
            // and silently did nothing — while the identical gesture AFTER a
            // relaunch worked, because `beginEpisode` preloads the durable row
            // (gate `.eligible`) and the in-memory copy then matches. A user
            // correction that works only after quitting the app is a bug, and
            // the shipped surface is `revertByTimeRange` (the transcript "This
            // isn't an ad" gesture), not just the test-only `revertWindow`.
            //
            // The fix is to make the synthesized twin tell the truth the
            // durable writer already tells. Keep these two constructions in
            // step: any field either one stamps must be stamped by both.
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )

        let key = idempotencyKey(assetId: analysisAssetId, windowId: windowId)

        let managed = ManagedWindow(
            adWindow: adWindow,
            decisionState: .confirmed,
            snappedStart: start,
            snappedEnd: end,
            idempotencyKey: key,
            cueActive: false
        )
        windows[windowId] = managed

        // Only `evaluateAndPush` may emit the auto tier, after the window
        // reaches `.applied`. Shadow/manual modes deliberately keep this
        // `.confirmed` window silent because no skip occurred.
        evaluateAndPush()
    }

    // MARK: - Idempotency

    /// Build the idempotency key for a skip decision.
    private func idempotencyKey(assetId: String, windowId: String) -> String {
        "\(assetId):\(windowId):\(config.policyVersion)"
    }

    // MARK: - Decision Logging

    private func logDecision(
        managed: ManagedWindow,
        decision: SkipDecisionState,
        reason: String
    ) {
        let record = SkipDecisionRecord(
            idempotencyKey: managed.idempotencyKey,
            adWindowId: managed.adWindow.id,
            analysisAssetId: managed.adWindow.analysisAssetId,
            policyVersion: config.policyVersion,
            decision: decision,
            reason: reason,
            originalStart: managed.adWindow.startTime,
            originalEnd: managed.adWindow.endTime,
            snappedStart: managed.snappedStart,
            snappedEnd: managed.snappedEnd,
            // playhead-ar60: the ACTUATION number — the one `evaluateWindow`
            // compared against the hysteresis thresholds. Recording
            // `confidence` here would make the log say the decision was taken
            // on a number it was not, which for a fusion row differs by ~400x.
            // That is this bead's own defect class pointed at a diagnostic.
            confidence: managed.adWindow.actuationConfidence,
            timestamp: Date().timeIntervalSince1970
        )
        decisionLog.append(record)
        if decisionLog.count > decisionLogCapacity {
            decisionLog.removeFirst(decisionLog.count - decisionLogCapacity)
        }

        logger.info("Decision: \(decision.rawValue) window=\(managed.adWindow.id) [\(managed.snappedStart, format: .fixed(precision: 1))s-\(managed.snappedEnd, format: .fixed(precision: 1))s] reason=\(reason)")
    }
}
