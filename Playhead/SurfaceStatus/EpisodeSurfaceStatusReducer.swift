// EpisodeSurfaceStatusReducer.swift
// The sole reducer for `EpisodeSurfaceStatus`. Given a pure snapshot of
// (persisted analysis state × attributed cause × eligibility × coverage
// × readiness anchor), return a single `EpisodeSurfaceStatus`.
//
// Scope: playhead-5bb3 (Phase 1.5 deliverable 1).
//
// Input-precedence ladder (distinct from CauseAttributionPolicy's ladder):
// the reducer picks exactly ONE of five possible surface outputs in this
// order when multiple input channels simultaneously indicate a
// surfaceable state:
//
//   1. eligibility-blocks       (AnalysisEligibility.isFullyEligible == false)
//   2. user-paused              (cause ∈ { .userPreempted, .userCancelled,
//                                           .appForceQuitRequiresRelaunch })
//   3. resource-blocks          (cause ∈ resource-exhausted tier)
//   4. transient-waits          (cause ∈ environmental-transient tier)
//   5. queued                   (nothing live)
//
// Note: within the already-selected `cause` argument, v11's
// `CauseAttributionPolicy` has already done its own cause-vs-cause
// precedence work. This reducer's job is to order the COMBINATION of
// eligibility + selected cause + state + coverage — not to rank causes
// against each other.

import Foundation

// MARK: - Reducer

/// Reduce the provided inputs to a single `EpisodeSurfaceStatus`. See
/// the file-level comment for the precedence ladder.
///
/// - Parameters:
///   - state: Stable value-object snapshot of the persisted episode
///     analysis metadata. Pass-by-value: the reducer never mutates.
///   - cause: The primary cause already resolved by
///     `CauseAttributionPolicy` (playhead-v11). `nil` means no live miss.
///   - eligibility: Device-level eligibility gate (playhead-2fd).
///   - coverage: Phase 2 `CoverageSummary` (stub in Phase 1.5). `nil`
///     means no coverage data yet; the reducer defaults
///     `playbackReadiness` to `.none`.
///   - readinessAnchor: Time anchor the UI uses when rendering the
///     "analyzed up to here" scrubber marker. Pass-through.
/// - Returns: A fully populated `EpisodeSurfaceStatus`. Never nil — the
///   reducer is total over every possible input combination.
func episodeSurfaceStatus(
    state: AnalysisState,
    cause: InternalMissCause?,
    eligibility: AnalysisEligibility,
    coverage: CoverageSummary?,
    readinessAnchor: TimeInterval?
) -> EpisodeSurfaceStatus {
    return _validatedEpisodeSurfaceStatus(
        _episodeSurfaceStatusCore(
            state: state,
            cause: cause,
            eligibility: eligibility,
            coverage: coverage,
            readinessAnchor: readinessAnchor
        ),
        cause: cause,
        eligibility: eligibility
    )
}

/// Tier-A + Tier-B invariant gate at the reducer's exit. Centralized so
/// every return path inside `_episodeSurfaceStatusCore` flows through one
/// validator — adding a new return path inside core does NOT require a
/// matching change here.
///
/// * Tier-A (DEBUG): fails fast on any invariant violation via
///   `precondition`, so dev builds and the dogfood Debug-build flavor
///   cannot ship a violating output.
/// * Tier-B (all builds): records each violation to the JSON Lines
///   audit log via `SurfaceStatusInvariantLogger`. Release builds rely
///   on this signal exclusively (no precondition crash).
private func _validatedEpisodeSurfaceStatus(
    _ status: EpisodeSurfaceStatus,
    cause: InternalMissCause?,
    eligibility: AnalysisEligibility
) -> EpisodeSurfaceStatus {
    let violations = SurfaceStatusInvariants.violations(of: status)
    if !violations.isEmpty {
        let context = SurfaceStateTransitionContext(
            episodeIdHash: nil,
            priorDisposition: nil,
            newDisposition: status.disposition,
            priorReason: nil,
            newReason: status.reason,
            cause: cause,
            eligibilitySnapshot: eligibility
        )
        SurfaceStatusInvariantLogger.recordViolations(violations, context: context)
        SurfaceStatusInvariants.enforce(violations)
    }
    return status
}

/// Pure core: the original reducer body, untouched except for the
/// rename. Every return below is gated through `_validatedEpisodeSurfaceStatus`.
private func _episodeSurfaceStatusCore(
    state: AnalysisState,
    cause: InternalMissCause?,
    eligibility: AnalysisEligibility,
    coverage: CoverageSummary?,
    readinessAnchor: TimeInterval?
) -> EpisodeSurfaceStatus {

    // Derived once up front so every branch below can consult the same
    // snapshot without re-evaluating CoverageSummary.
    let readiness = coverage?.readiness(anchor: readinessAnchor) ?? .none
    let unavailableReason = AnalysisUnavailableReason.derive(from: eligibility)

    // MARK: Rule 1 — eligibility-blocks
    //
    // When the device cannot run analysis at all, we short-circuit to
    // the `unavailable` disposition regardless of any other signal.
    // This is the highest-priority rule because it reflects a permanent
    // (or at least user-action-gated) condition that invalidates every
    // other cause: a `.thermal` wait is pointless on a device that
    // cannot run analysis in the first place.
    if !eligibility.isFullyEligible {
        return EpisodeSurfaceStatus(
            disposition: .unavailable,
            reason: .analysisUnavailable,
            hint: .enableAppleIntelligence,
            analysisUnavailableReason: unavailableReason,
            playbackReadiness: readiness,
            readinessAnchor: readinessAnchor
        )
    }

    // Rules 2–4 require a live cause; Rule 5 fires when there is none.
    guard let cause else {
        // MARK: Rule 5 — queued
        //
        // No live cause: the episode is simply queued waiting for its
        // turn. Hint is `.wait` because the user cannot do anything to
        // make it go faster; the scheduler will get to it when it can.
        return EpisodeSurfaceStatus(
            disposition: .queued,
            reason: .waitingForTime,
            hint: .wait,
            analysisUnavailableReason: nil,
            playbackReadiness: readiness,
            readinessAnchor: readinessAnchor
        )
    }

    // MARK: Rule 2 — user-paused
    //
    // User-initiated causes surface the user-facing pause reason
    // regardless of any resource / transient state. Emphasis is on the
    // USER source: the user took an action, so we must acknowledge
    // that action in the UI rather than dress it up as a resource wait.
    if isUserPaused(cause) {
        switch cause {
        case .userPreempted, .userCancelled:
            return EpisodeSurfaceStatus(
                disposition: .cancelled,
                reason: .cancelled,
                hint: .retry,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        case .appForceQuitRequiresRelaunch:
            return EpisodeSurfaceStatus(
                disposition: .paused,
                reason: .resumeInApp,
                hint: .openAppToResume,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        default:
            // Impossible by the guard above — `isUserPaused` only
            // returns true for the three cases handled above. We call
            // the invariant logger so ol05 can observe this path if the
            // ladder ever falls out of sync.
            SurfaceStatusInvariantLogger.invariantViolated(
                "user-paused rule matched an unknown cause: \(cause)"
            )
            return fallback(
                readiness: readiness,
                readinessAnchor: readinessAnchor
            )
        }
    }

    // MARK: Rule 3 — resource-blocks
    //
    // Resource-exhausted causes (mediaCap / analysisCap) surface the
    // storage reason. `taskExpired` is context-dependent — with retries
    // remaining it belongs to the transient tier (Rule 4); with the
    // budget exhausted it belongs here. The reducer does not have
    // retry-budget context in its signature, so we conservatively treat
    // `taskExpired` as a transient wait unless the persisted status
    // says the job has already moved to `.failed`.
    if isResourceBlock(cause: cause, state: state) {
        switch cause {
        case .mediaCap:
            return EpisodeSurfaceStatus(
                disposition: .failed,
                reason: .storageFull,
                hint: .freeUpStorage,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        case .analysisCap:
            return EpisodeSurfaceStatus(
                disposition: .failed,
                reason: .storageFull,
                hint: .freeUpStorage,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        case .taskExpired:
            // Only reached when the persisted status is `.failed`,
            // meaning retries are exhausted — see `isResourceBlock`.
            return EpisodeSurfaceStatus(
                disposition: .failed,
                reason: .couldntAnalyze,
                hint: .retry,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        default:
            SurfaceStatusInvariantLogger.invariantViolated(
                "resource-block rule matched an unknown cause: \(cause)"
            )
            return fallback(
                readiness: readiness,
                readinessAnchor: readinessAnchor
            )
        }
    }

    // MARK: Rule 4 — transient-waits
    //
    // Environmental-transient causes (thermal, noNetwork, etc.) surface
    // the paused/waiting reason. These are conditions that will resolve
    // themselves over time or with a small user action (plug in,
    // connect to Wi-Fi) without requiring a retry.
    if isTransientWait(cause: cause) {
        switch cause {
        case .thermal:
            return EpisodeSurfaceStatus(
                disposition: .paused,
                reason: .phoneIsHot,
                hint: .wait,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        case .lowPowerMode, .batteryLowUnplugged:
            return EpisodeSurfaceStatus(
                disposition: .paused,
                reason: .powerLimited,
                hint: .chargeDevice,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        case .noNetwork:
            return EpisodeSurfaceStatus(
                disposition: .paused,
                reason: .waitingForNetwork,
                hint: .wait,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        case .wifiRequired:
            return EpisodeSurfaceStatus(
                disposition: .paused,
                reason: .waitingForNetwork,
                hint: .connectToWiFi,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        case .taskExpired:
            // Retries remaining → surface as a normal wait.
            return EpisodeSurfaceStatus(
                disposition: .queued,
                reason: .waitingForTime,
                hint: .wait,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        case .modelTemporarilyUnavailable:
            // Runtime expected back without user action.
            return EpisodeSurfaceStatus(
                disposition: .queued,
                reason: .waitingForTime,
                hint: .wait,
                analysisUnavailableReason: nil,
                playbackReadiness: readiness,
                readinessAnchor: readinessAnchor
            )
        default:
            SurfaceStatusInvariantLogger.invariantViolated(
                "transient-wait rule matched an unknown cause: \(cause)"
            )
            return fallback(
                readiness: readiness,
                readinessAnchor: readinessAnchor
            )
        }
    }

    // MARK: Default — engine-error / unmapped / forward-compat unknown
    //
    // Remaining causes (engine errors, unsupported language, no runtime
    // grant, forward-compat unknown) surface a conservative "couldn't
    // analyze" failure. These are the rows CauseAttributionPolicy
    // marks as `engineError` or `eligibilityPermanent`, for which a
    // retry CTA is the correct Phase 1.5 affordance.
    //
    // playhead-o45p Gap D: when a `.unknown(_)` forward-compat sentinel
    // reaches this branch, emit an impossible-state log entry via the
    // ol05 invariant logger. The reducer's contract is total over the 16
    // canonical cases — reaching this default with `.unknown(raw)` means
    // a schema-evolved cause string escaped upstream validation. The
    // surface output is still a safe conservative triple; the log line
    // lets e2a3 aggregate unmapped-cause rates independently of the
    // surface behavior.
    if case .unknown(let raw) = cause {
        SurfaceStatusInvariantLogger.invariantViolated(
            "reducer received unmapped InternalMissCause.unknown(\(raw)); surfaced conservative .failed/.couldntAnalyze/.retry triple"
        )
    }
    return EpisodeSurfaceStatus(
        disposition: .failed,
        reason: .couldntAnalyze,
        hint: .retry,
        analysisUnavailableReason: nil,
        playbackReadiness: readiness,
        readinessAnchor: readinessAnchor
    )
}

// MARK: - Classifier helpers
//
// These are free functions rather than extensions on `InternalMissCause`
// so the lint contract (`SurfaceStatusUILintTests`) keeps the UI forbid-
// list simple: any file outside Services/SurfaceStatus that references
// `InternalMissCause` is flagged, period. Keeping these helpers here
// means no UI file needs to classify a cause — the reducer does it all.

/// `true` when the cause is a user-initiated pause / cancel.
private func isUserPaused(_ cause: InternalMissCause) -> Bool {
    switch cause {
    case .userPreempted,
         .userCancelled,
         .appForceQuitRequiresRelaunch:
        return true
    default:
        return false
    }
}

/// `true` when the cause is a resource-exhausted block that will not
/// resolve without user action.
///
/// `taskExpired` is deliberately context-dependent: with retries
/// remaining (the common case) it is a transient wait and belongs in
/// Rule 4. Only when the persisted status has already moved to
/// `.failed` do we treat it as a resource block.
private func isResourceBlock(cause: InternalMissCause, state: AnalysisState) -> Bool {
    switch cause {
    case .mediaCap, .analysisCap:
        return true
    case .taskExpired:
        return state.persistedStatus == .failed
    default:
        return false
    }
}

/// `true` when the cause is an environmental-transient that the system
/// will recover from on its own (possibly after a small user action
/// like plugging in).
private func isTransientWait(cause: InternalMissCause) -> Bool {
    switch cause {
    case .thermal,
         .lowPowerMode,
         .batteryLowUnplugged,
         .noNetwork,
         .wifiRequired:
        return true
    case .taskExpired:
        // Covered by `isResourceBlock` only when `.failed`. Here
        // we return true so the transient-wait rule handles the retries-
        // remaining path. (The reducer evaluates Rule 3 before Rule 4;
        // if `isResourceBlock` returned true, we never reach here.)
        return true
    case .modelTemporarilyUnavailable:
        // Matches CauseAttributionPolicy's tier: when the model is
        // currently unavailable but the system expects it back, surface
        // as a wait.
        return true
    default:
        return false
    }
}

/// Conservative fallback used only from the invariant-violation paths.
/// The reducer's contract is total — every case of every enum is
/// handled — so this should never execute in production. It exists so
/// the compiler can see that every branch returns a value.
private func fallback(
    readiness: PlaybackReadiness,
    readinessAnchor: TimeInterval?
) -> EpisodeSurfaceStatus {
    EpisodeSurfaceStatus(
        disposition: .failed,
        reason: .couldntAnalyze,
        hint: .retry,
        analysisUnavailableReason: nil,
        playbackReadiness: readiness,
        readinessAnchor: readinessAnchor
    )
}
