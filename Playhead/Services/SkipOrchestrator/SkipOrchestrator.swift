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
            skipConfidence: revision.confidence,
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

    /// Bug 5 (skip-cues-deletion): minimum confidence required for an
    /// `AdWindow` to be eligible for the `beginEpisode` preload that
    /// previously read from `skip_cues`. Mirrors the 0.7 threshold the
    /// (now-deleted) `SkipCueMaterializer` used. Kept private to this
    /// actor — the only consumer is `beginEpisode`.
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
    /// chose the span deliberately. User-marked and accepted-suggestion
    /// windows are exempted via their `boundaryState` stamps
    /// ("userMarked" / "userConfirmedSuggested") in
    /// `isUserInitiatedSkip(_:)`; this set covers the manual-tap path whose
    /// window is an ordinary detection row.
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

    /// Continuation-backed stream of applied ad segment time ranges (seconds).
    /// Consumers receive the full set of applied segments whenever the set changes.
    private var segmentContinuations: [UUID: AsyncStream<[(start: Double, end: Double)]>.Continuation] = [:]

    /// Continuation-backed stream of banner items.
    /// Emits once per window after an actual skip reaches `.applied`.
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
        // playhead-xr3t (review): default to a disabled no-op filter so
        // pre-existing test surface that constructs `SkipOrchestrator`
        // — and never sets episode-duration / declared-chapter context
        // — doesn't silently lose pre-roll/post-roll spans to the
        // head-/tail-edge rules. Production wires the real settings-
        // backed filter explicitly via `InventorySanityFilter
        // .production()` (see `PlayheadRuntime`), preserving the bead's
        // spec default ON for new builds. This avoids an implicit
        // `UserDefaults.standard` dependency leaking into every test
        // that didn't ask for one.
        inventoryFilter: InventorySanityFilter = InventorySanityFilter(isEnabled: false)
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

    /// Emit a banner item for the given managed window to all banner listeners.
    private func emitBannerItem(for managed: ManagedWindow) {
        guard !bannerContinuations.isEmpty
                || !bannerEventContinuations.isEmpty
        else {
            return
        }
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
            tier: .suggest
        )
    }

    /// Register one mark-only producer value as the current suggest revision.
    /// Exact replays preserve their delivery gate. Material changes retire any
    /// older card before emitting a fresh token-bound presentation.
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
            emitSuggestBanner(for: adWindow)
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
            emitSuggestBanner(for: adWindow)
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

    private func replayPendingSuggestBanners(
        to continuation: AsyncStream<AdSkipBannerItem>.Continuation
    ) {
        let pending = suggestWindows.values
            .filter {
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

    private func replayPendingSuggestBannerEvents(
        to continuation: AsyncStream<AdBannerStreamEvent>.Continuation
    ) {
        let pending = suggestWindows.values
            .filter {
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

    /// Slice catalog entries whose coverage span overlaps the skipped window.
    /// Returns an empty array when no catalog is available or none overlap.
    private func catalogEntries(overlapping start: Double, end: Double) -> [EvidenceEntry] {
        guard let catalog = activeEvidenceCatalog else { return [] }
        // Closed-interval overlap: an entry overlaps the window iff its
        // coverage span shares ANY point with [start, end], including the
        // boundaries themselves. We deliberately use `<=` on both sides so
        // zero-duration entries that fall exactly on a snapped boundary
        // still surface — typical for short FM-bounded ad windows where the
        // disclosure phrase straddles the snap edge.
        return catalog.entries.filter { entry in
            entry.coverageStartTime <= end && entry.coverageEndTime >= start
        }
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
        latestUserSeekOperationGeneration = 0
        decisionLog.removeAll()
        banneredWindowIds.removeAll()
        emittedAutoSkipBannerWindowIds.removeAll()
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
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
        // A direct episode switch does not call `endEpisode`. Publish the
        // cleared state synchronously so the transport and UI cannot retain
        // the prior episode's skip cues or segment markers while hydration
        // for the replacement episode is in flight.
        pushSkipCues()
        broadcastAppliedSegments()

        #if DEBUG
        await beginEpisodeHydrationBarrierForTesting?()
        guard episodeLifecycleGeneration == lifecycleGeneration else {
            return
        }
        #endif

        // Load per-show trust mode.
        if let podcastId = normalizedPodcastId, let trustService {
            let mode = await trustService.effectiveMode(podcastId: podcastId)
            guard episodeLifecycleGeneration == lifecycleGeneration else {
                return
            }
            activeSkipMode = mode
        } else {
            activeSkipMode = .shadow
        }

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

        // Bug 5 (skip-cues-deletion): pre-load directly from `ad_windows`,
        // filtered to confirmed-confidence rows. Replaces the prior path
        // that read from the now-deleted `skip_cues` table. The 0.7
        // threshold mirrors the cue materializer's threshold so the
        // preload set is byte-identical to what the cue table used to
        // contain at the same point in time. We forward the persisted
        // `AdWindow` rows directly to `receiveAdWindows` so the existing
        // event-stream path applies its standard logic — eligibilityGate,
        // banner state, decision-log dedup, etc. Forwarding the
        // unmodified row preserves any auto-skip / markOnly precision
        // gate stamped at write-time, which a synthesized "confirmed"
        // shape would silently strip.
        //
        // Cycle-21 H-1: preload `ad_windows` and forward through
        // `receiveAdWindows`. Filter excludes only `.suppressed`
        // (terminal "no-skip" — replay wastes memory) and `.reverted`
        // (user explicitly chose "Listen" — replay would risk pushing
        // a cue the user rejected). `.candidate` and `.confirmed` are
        // forwarded so the orchestrator can re-evaluate them.
        // `.applied` is forwarded so a previously-skipped ad pushes its
        // cue again on the next app launch — `evaluateAndPush`'s
        // terminal-state branch (the `decisionState == .applied` arm)
        // appends the row to `eligible` and `pushMergedCues` fires the
        // skip cue. Without that forwarding, cross-launch auto-skip
        // would silently regress: pre-pivot the `skip_cues` table re-
        // cued every confidence-passing row at episode start; now the
        // `ad_windows` rows must do the same job.
        //
        // Banner re-emission for the forwarded `.applied` rows is
        // suppressed by pre-populating `banneredWindowIds` BEFORE the
        // `receiveAdWindows` call. The terminal-state branch in
        // `evaluateAndPush` only emits a banner when
        // `!banneredWindowIds.contains(id)`; pre-populating the set
        // turns the banner emission off for an already-skipped ad
        // without affecting the cue push (the cue push happens via
        // `eligible.append` regardless of the banner gate).
        do {
            let preWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
            guard episodeLifecycleGeneration == lifecycleGeneration else {
                return
            }
            let eligible = preWindows.filter {
                $0.confidence >= Self.preloadConfidenceThreshold
                    && $0.endTime > $0.startTime
                    && Self.preloadEligibleDecisionStates.contains($0.decisionState)
            }
            if !eligible.isEmpty {
                let appliedRawValue = SkipDecisionState.applied.rawValue
                for window in eligible where window.decisionState == appliedRawValue {
                    // Cycle-27 T-3 production-writer site (1 of 4): preload pre-population.
                    banneredWindowIds.insert(window.id)
                }
                await receiveAdWindows(eligible)
                guard episodeLifecycleGeneration == lifecycleGeneration else {
                    return
                }
            }
        } catch {
            logger.warning("Failed to load preload ad_windows: \(error.localizedDescription)")
        }

        logger.info("Begin episode: asset=\(analysisAssetId)")
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
        // playhead-xr3t: clear per-episode inventory-filter context so
        // the next episode doesn't inherit stale duration/chapters.
        activeEpisodeDuration = nil
        activeDeclaredChapters = []
        inAdState = false
        latestUserSeekOperationGeneration = 0
        banneredWindowIds.removeAll()
        emittedAutoSkipBannerWindowIds.removeAll()
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
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
    private func reapplyInventoryFilterToManagedWindows() {
        guard inventoryFilter.isEnabled,
              !windows.isEmpty || !suggestWindows.isEmpty
        else {
            return
        }
        var idsToRetire: [String] = []
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
                idsToRetire.append(id)
            }
        }

        let suggestionIDsToRetire = suggestWindows.compactMap {
            id, suggested -> String? in
            let verdict = inventoryFilter.evaluate(
                startTime: suggested.startTime,
                endTime: suggested.endTime,
                episodeDuration: activeEpisodeDuration,
                declaredChapters: activeDeclaredChapters
            )
            guard case let .rejected(reason) = verdict else {
                return nil
            }
            logger.info(
                "Suggested AdWindow \(id, privacy: .public) retroactively rejected by inventory sanity filter: \(reason.rawValue, privacy: .public)"
            )
            return id
        }

        let allIDsToRetire =
            Set(idsToRetire).union(suggestionIDsToRetire)
        guard !allIDsToRetire.isEmpty else { return }
        for id in allIDsToRetire {
            retireAllNonRevertedWindowStateIfPresent(windowId: id)
        }
        evaluateAndPush()
    }

    // MARK: - Ad Window Event Stream

    /// Receive new or updated AdWindows from AdDetectionService.
    /// This is the primary event-stream entry point. Called whenever
    /// the detection pipeline produces or updates windows.
    func receiveAdWindows(_ adWindows: [AdWindow]) async {
        guard let assetId = activeAssetId else { return }
        let producerGeneration = nextProducerMutationGeneration()

        for adWindow in adWindows {
            guard adWindow.analysisAssetId == assetId else { continue }
            if isTerminalProducerRevision(adWindow) {
                // A known stale replay is not a newer producer mutation. In
                // particular, it must not supersede genuine same-ID material
                // that is suspended in catalog validation. First-seen
                // terminal material still claims below and performs the
                // conservative ID-wide disarm.
                retireNonRevertedWindowStateIfMatching(adWindow)
                continue
            }
            guard claimProducerMutation(
                windowId: adWindow.id,
                generation: producerGeneration
            ) else {
                continue
            }
            guard Self.hasValidRuntimeWindowMaterial(
                id: adWindow.id,
                analysisAssetId: adWindow.analysisAssetId,
                startTime: adWindow.startTime,
                endTime: adWindow.endTime,
                confidence: adWindow.confidence
            ) else {
                retireAllNonRevertedWindowStateIfPresent(
                    windowId: adWindow.id
                )
                logger.warning(
                    "AdWindow \(adWindow.id, privacy: .public) has invalid runtime material — automatic admission refused"
                )
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
                    continue
                }
                // The catalog actor hop is an episode-lifecycle suspension
                // boundary. Never install the old episode's result after a
                // replacement became active while validation was in flight.
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

            if provisionallyResolvingSuggestWindowIds.contains(adWindow.id) {
                bufferedSuggestProducerUpdates[adWindow.id] =
                    .adWindow(adWindow)
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
                continue
            }

            let existingManaged = windows[adWindow.id]
            let existingState = existingManaged?.decisionState

            // A user-reverted window is terminal. Applied windows remain
            // terminal only while the newest precision gate still permits the
            // managed path; markOnly/blocked updates below must disarm them.
            if existingState == .reverted
                || revertedProducerWindowIds.contains(adWindow.id) {
                retireSuggestedWindowIfPresent(windowId: adWindow.id)
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
            // cases, and `.cappedByFMSuppression` — originate only in
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
            if decodedGate == .markOnly
                || (
                    catalogProvenanceMustFailClosed
                    && (decodedGate == nil || decodedGate == .eligible)
                ) {
                retireManagedWindowIfPresent(windowId: adWindow.id)
                if catalogProvenanceMustFailClosed {
                    logger.warning(
                        "AdWindow \(adWindow.id, privacy: .public) has untrusted catalog provenance — surfacing as suggest tier"
                    )
                } else {
                    logger.debug(
                        "AdWindow \(adWindow.id, privacy: .public) eligibilityGate=markOnly — surfacing as suggest tier"
                    )
                }
                if !banneredWindowIds.contains(adWindow.id) {
                    registerSuggestedWindow(adWindow)
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
            // `.blockedByUserCorrection`, `.cappedByFMSuppression`). These rows
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
            let replacesManagedMaterial = existingManaged.map {
                !AdWindowMaterialIdentity.sameProducerRevision(
                    $0.adWindow,
                    adWindow
                )
            } ?? false
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
                      revision.confidence == result.skipConfidence else {
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
                  producerRevision.confidence == result.skipConfidence,
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
            let replacesManagedMaterial = existingManaged.map {
                !AdWindowMaterialIdentity.sameProducerRevision(
                    $0.adWindow,
                    producerRevision
                )
            } ?? false
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
        // A user seek can jump over a proposed span without consuming the
        // orchestrator's skip. Invalidate pending positives, including an
        // applied-persistence task that has not re-entered the actor yet.
        catalogLearningGeneration &+= 1
        pendingCatalogLearning.removeAll()
        lastSeekTime = Date()
        skipSuppressedAfterSeek = true
        currentPlayheadTime = time
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

    private func revokeRecurrenceEvidence(
        for window: AdWindow,
        showId: String?,
        source: CatalogRevocationSource
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
            let featureWindows = try await store.fetchFeatureWindows(
                assetId: window.analysisAssetId,
                from: window.startTime,
                to: window.endTime
            )
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
            do {
                try await revokeRecurrenceEvidence(
                    for: managed.adWindow,
                    showId: sourceShowId,
                    source: .listenRevert
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

        // Revoke recurrence material before making the correction terminal.
        // The source/creative tombstones prevent a delayed learner from
        // reopening the gap. If a separate store fails, leave the durable
        // window and UI retryable; successful earlier tombstones are
        // conservative and idempotent on retry.
        do {
            try await revokeRecurrenceEvidence(
                for: requestedManaged.adWindow,
                showId: sourceShowId,
                source: .listenRevert
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
            await trustService.recordFalseSkipSignal(podcastId: sourceShowId)
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
            podcastId: sourceShowId
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
        guard start.isFinite,
              end.isFinite,
              start >= 0,
              end > start,
              let expectedAssetId,
              let expectedEpisodeId,
              activeAssetId == expectedAssetId,
              activeEpisodeId == expectedEpisodeId,
              expectedPlaybackGeneration == nil
                || activePlaybackLifecycleGeneration
                    == expectedPlaybackGeneration
        else {
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
                return false
            }
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

        var exactTargetsByID: [String: AdWindow] = [:]
        for target in managedRevertTargets {
            exactTargetsByID[target.id] = target.managed.adWindow
        }
        for target in suggestRevertTargets {
            if let existing = exactTargetsByID[target.id],
               !AdWindowMaterialIdentity.sameProducerRevision(
                   existing,
                   target.window
               ) {
                return false
            }
            exactTargetsByID[target.id] = target.window
        }
        let exactTargets = exactTargetsByID.values.sorted {
            $0.id < $1.id
        }
        guard let correctionOwner = exactTargets.first else {
            return false
        }

        do {
            for target in exactTargets {
                try await revokeRecurrenceEvidence(
                    for: target,
                    showId: sourceShowId,
                    source: .manualVeto
                )
            }
        } catch {
            logger.warning("Manual veto revocation failed")
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
                    windowId: correctionOwner.id,
                    additionalWindowIds:
                        exactTargets.dropFirst().map(\.id),
                    detectionProjection:
                        ExplicitFeedbackDetectionProjection(correctionOwner),
                    correctionProvenance:
                        correctionSpan?.anchorProvenance ?? []
                )
            } else {
                correction = nil
            }
            if let barrier = feedbackPersistenceBarrierForTesting {
                await barrier()
            }
            guard let wasNewlyInserted =
                    try await store.persistRevertedAdWindowsIfCurrent(
                        expectedWindows: exactTargets,
                        analysisAssetId: expectedAssetId,
                        expectedPodcastId: sourceShowId,
                        correction: correction
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
            logger.warning("Manual veto persistence failed")
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
            for (id, expectedManaged) in managedRevertTargets {
                guard var managed = windows[id],
                      managed.decisionState != .reverted,
                      managed.decisionState != .suppressed,
                      AdWindowMaterialIdentity.sameProducerRevision(
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

            // Mark-only windows use a separate UI dictionary. Remove only the
            // exact revisions covered by the committed transaction.
            for (id, expectedSuggestion) in suggestRevertTargets {
                guard let suggested = suggestWindows[id],
                      AdWindowMaterialIdentity.sameProducerRevision(
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
        if let sourceShowId, let trustService {
            if revertedManagedAny {
                await trustService.recordFalseSkipSignal(
                    podcastId: sourceShowId
                )
            } else {
                await trustService.recordWeakFalseSkipSignal(
                    podcastId: sourceShowId
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
    @discardableResult
    func denyAutoSkippedBanner(
        windowId: String,
        analysisAssetId expectedAssetId: String?,
        startTime expectedStartTime: Double,
        endTime expectedEndTime: Double,
        podcastId: String?,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64?,
        ifWindowMaterialRevisionToken expectedMaterialToken: String?
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
        do {
            try await revokeRecurrenceEvidence(
                for: requestedManaged.adWindow,
                showId: sourceShowId,
                source: .bannerAutoSkipDenied
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
                source: .bannerAutoSkipDenied,
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
                Task {
                    await trustService.recordFalseSkipSignal(
                        podcastId: sourceShowId,
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

        do {
            try await revokeRecurrenceEvidence(
                for: requestedManaged.adWindow,
                showId: sourceShowId,
                source: .manualVeto
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
                Task {
                    await trustService.recordFalseSkipSignal(
                        podcastId: sourceShowId
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
    private func ingestNegativeFingerprint(text: String?, podcastId: String?) {
        guard let bank = negativeFingerprintBank else { return }
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

        // Build a fresh durable applied AdWindow with the suggest window's span
        // and confidence pinned to 1.0. The in-memory wrapper starts confirmed
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
            confidence: 1.0,
            boundaryState: "userConfirmedSuggested",
            decisionState: AdDecisionState.applied.rawValue,
            detectorVersion: suggested.detectorVersion,
            advertiser: suggested.advertiser,
            product: suggested.product,
            adDescription: suggested.adDescription,
            evidenceText: suggested.evidenceText,
            evidenceStartTime: suggested.evidenceStartTime,
            metadataSource: suggested.metadataSource,
            metadataConfidence: suggested.metadataConfidence,
            metadataPromptVersion: suggested.metadataPromptVersion,
            wasSkipped: true,
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
        }
        recordThresholdControlMiss(podcastId: sourcePodcastId)

        guard sourceLifecycleIsCurrent else {
            return true
        }

        windows[promotedId] = managed
        // The suggest card already presented this span and collected the
        // explicit Yes. Applying its promoted UUID must not immediately emit
        // a second feedback card for the same user decision.
        // Cycle-27 T-3 production-writer site (4 of 4): accepted suggest
        // presentation already collected the response.
        banneredWindowIds.insert(promotedId)

        applyManualSkip(
            windowId: promotedId,
            isExplicitBannerFeedback: true
        )

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
        do {
            try await revokeRecurrenceEvidence(
                for: suggested,
                showId: sourcePodcastId,
                source: .bannerSuggestionDenied
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
        case "foundationModels":
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

        // playhead-98co: a manual "Skip Ad" tap is user-initiated — the
        // window (an ordinary detection row) is exempt from edge padding
        // so the user's chosen span skips exactly.
        userInitiatedSkipWindowIds.insert(windowId)

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
        if activeSkipMode != .shadow && !isExplicitBannerFeedback {
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

    /// Override the active skip mode for the current episode and re-evaluate pending windows.
    func setActiveSkipMode(_ mode: SkipMode) {
        activeSkipMode = mode
        evaluateAndPush()
    }

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
                        emitBannerItem(for: managed)
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
                emitBannerItem(for: managed)
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
        guard edgePaddingEnabled, !isUserInitiatedSkip(managed) else {
            return (start: managed.snappedStart, end: managed.snappedEnd)
        }
        // playhead-hdgk: per-edge anchor provenance is derived at fusion time
        // (rediff `.rediffSlot` width ownership + `StingerRefiner` snap trace),
        // persisted on the `AdWindow` row, and stamped into
        // `edgeAnchorsByWindowId` at `receiveAdWindows` ingest — before any
        // promotion. An absent entry (a span that never flowed through ingest,
        // or a non-fusion producer) defaults both edges to `.unanchored`, under
        // which flag-ON auto-skips nothing — the intended conservative posture.
        let anchors = edgeAnchorsByWindowId[managed.adWindow.id]
            ?? (start: .unanchored, end: .unanchored)
        return AutoSkipEdgePadding.skipWindow(
            spanStart: managed.snappedStart,
            spanEnd: managed.snappedEnd,
            startAnchor: anchors.start,
            endAnchor: anchors.end,
            showKey: activePodcastId
        )
    }

    /// Whether this window's skip was explicitly user-initiated — exempt
    /// from edge padding (the user chose the span deliberately).
    private func isUserInitiatedSkip(_ managed: ManagedWindow) -> Bool {
        if userInitiatedSkipWindowIds.contains(managed.adWindow.id) {
            return true
        }
        switch managed.adWindow.boundaryState {
        case "userMarked", "userConfirmedSuggested":
            return true
        default:
            return false
        }
    }

    /// Evaluate a single window against skip policy. Returns the decision.
    private func evaluateWindow(_ managed: inout ManagedWindow) -> SkipDecisionState {
        let confidence = managed.adWindow.confidence
        let span = managed.snappedEnd - managed.snappedStart

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
            if activeSkipMode == .auto && confidence >= config.enterThreshold {
                // Promote to confirmed — fall through to trust mode gate.
                managed.decisionState = .confirmed
            } else if confidence < config.shortSpanOverrideConfidence {
                return managed.decisionState
            }
        }

        // Trust mode gate: shadow mode logs only; manual mode marks confirmed
        // but does not auto-skip (UI shows a manual "Skip Ad" button instead).
        switch activeSkipMode {
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
            wasSkipped: false, userDismissedBanner: false
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
            confidence: managed.adWindow.confidence,
            timestamp: Date().timeIntervalSince1970
        )
        decisionLog.append(record)
        if decisionLog.count > decisionLogCapacity {
            decisionLog.removeFirst(decisionLog.count - decisionLogCapacity)
        }

        logger.info("Decision: \(decision.rawValue) window=\(managed.adWindow.id) [\(managed.snappedStart, format: .fixed(precision: 1))s-\(managed.snappedEnd, format: .fixed(precision: 1))s] reason=\(reason)")
    }
}
