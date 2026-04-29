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
///   - invariantLogger: Sink for impossible-state violations detected
///     during reduction. `nil` silently drops violations — appropriate
///     for pure unit tests of the reducer's precedence ladder. Production
///     threads the composition root's logger through so Tier-B audit
///     entries land on disk.
/// - Returns: A fully populated `EpisodeSurfaceStatus`. Never nil — the
///   reducer is total over every possible input combination.
func episodeSurfaceStatus(
    state: AnalysisState,
    cause: InternalMissCause?,
    eligibility: AnalysisEligibility,
    coverage: CoverageSummary?,
    readinessAnchor: TimeInterval?,
    invariantLogger: SurfaceStatusInvariantLogger? = nil
) -> EpisodeSurfaceStatus {
    let status = _validatedEpisodeSurfaceStatus(
        _episodeSurfaceStatusCore(
            state: state,
            cause: cause,
            eligibility: eligibility,
            coverage: coverage,
            readinessAnchor: readinessAnchor,
            invariantLogger: invariantLogger
        ),
        cause: cause,
        eligibility: eligibility,
        invariantLogger: invariantLogger
    )
    // playhead-cthe: validate the derived (coverage, readiness) pair
    // against the spec's two impossible-state assertions ("complete
    // implies proximal" / "proximal requires firstCoveredOffset").
    // Emitted AFTER the core reducer has produced its real disposition/
    // reason so the JSON Lines violation context reflects the actual
    // surface output — not a fabricated `.queued / .waitingForTime` pair
    // the audit consumer would otherwise see next to a
    // `completeReadinessMissingCoverage` row. Violations flow through
    // the ol05 invariant channel (Tier-A precondition in DEBUG + Tier-B
    // JSON Lines in every build) so the same audit stream that records
    // taxonomy violations records coverage inconsistencies.
    let coverageViolations = SurfaceStatusInvariants.violations(
        coverage: coverage,
        readiness: status.playbackReadiness
    )
    if !coverageViolations.isEmpty {
        let coverageContext = SurfaceStateTransitionContext(
            episodeIdHash: nil,
            priorDisposition: nil,
            newDisposition: status.disposition,
            priorReason: nil,
            newReason: status.reason,
            cause: cause,
            eligibilitySnapshot: eligibility
        )
        invariantLogger?.recordViolations(
            coverageViolations,
            context: coverageContext
        )
        SurfaceStatusInvariants.enforce(coverageViolations)
    }
    return status
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
    eligibility: AnalysisEligibility,
    invariantLogger: SurfaceStatusInvariantLogger?
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
        invariantLogger?.recordViolations(violations, context: context)
        SurfaceStatusInvariants.enforce(violations)
    }
    return status
}

/// Pure core: the reducer's input-precedence ladder. Decides WHICH input
/// channel drives the surface (eligibility / user / resource / transient
/// / queued); for any cause-driven branch (Rules 2–4 + the default) it
/// delegates the cause→(disposition, reason, hint) triple to
/// `CauseAttributionPolicy.attribute(_:context:)`, which is the canonical
/// post-dfem source of truth. Every return below is gated through
/// `_validatedEpisodeSurfaceStatus`.
private func _episodeSurfaceStatusCore(
    state: AnalysisState,
    cause: InternalMissCause?,
    eligibility: AnalysisEligibility,
    coverage: CoverageSummary?,
    readinessAnchor: TimeInterval?,
    invariantLogger: SurfaceStatusInvariantLogger?
) -> EpisodeSurfaceStatus {

    // Derived once up front so every branch below can consult the same
    // snapshot without re-evaluating CoverageSummary. The corresponding
    // (coverage, readiness) impossible-state assertions are emitted by
    // the outer wrapper AFTER the core returns so the violation's logged
    // newDisposition/newReason match the actual surface output rather
    // than a fabricated pair.
    let readiness = derivePlaybackReadiness(
        coverage: coverage,
        anchor: readinessAnchor
    )
    let unavailableReason = AnalysisUnavailableReason.derive(from: eligibility)

    // Build the `CauseAttributionContext` once. The reducer does not have
    // a real retry-budget integer in scope — `state.persistedStatus` is
    // the only signal that distinguishes "the system will retry on its
    // own" from "retries are exhausted". Map `.failed` → 0 (no retries
    // remaining) and any other persisted status → 1 (some budget
    // remaining); this matches the policy's branching for `.taskExpired`
    // (`> 0` → environmental-transient queued wait, `== 0` →
    // resource-exhausted failure).
    let attributionContext = CauseAttributionContext(
        modelAvailableNow: eligibility.modelAvailableNow,
        retryBudgetRemaining: state.persistedStatus == .failed ? 0 : 1
    )

    // MARK: Rule 1 — eligibility-blocks
    //
    // When the device cannot run analysis at all, we short-circuit to
    // the `unavailable` disposition regardless of any other signal.
    // This is the highest-priority rule because it reflects a permanent
    // (or at least user-action-gated) condition that invalidates every
    // other cause: a `.thermal` wait is pointless on a device that
    // cannot run analysis in the first place.
    //
    // Rule 1 stays inside the reducer (NOT delegated to the policy)
    // because eligibility is a non-cause channel — the policy operates
    // on a single `InternalMissCause` and has no eligibility input.
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

    // Forward-compat sentinel: `.unknown(_)` is NOT a member of
    // `CauseAttributionPolicy.mappedCauses`, so calling
    // `attribute(.unknown(raw), context:)` would trip the H1 DEBUG
    // safety-net `assertionFailure`. We handle the sentinel inline,
    // emit the same conservative `(failed, couldntAnalyze, retry)`
    // triple the policy's `attributeCore` returns for `.unknown(_)`,
    // and log the impossible-state row through the ol05 invariant
    // channel so e2a3 can aggregate unmapped-cause rates independently
    // of the surface behavior.
    if case .unknown(let raw) = cause {
        invariantLogger?.invariantViolated(
            code: .unmappedForwardCompatCause,
            description: "reducer received unmapped InternalMissCause.unknown(\(raw)); surfaced conservative .failed/.couldntAnalyze/.retry triple"
        )
        return EpisodeSurfaceStatus(
            disposition: .failed,
            reason: .couldntAnalyze,
            hint: .retry,
            analysisUnavailableReason: nil,
            playbackReadiness: readiness,
            readinessAnchor: readinessAnchor
        )
    }

    // MARK: Rules 2–4 + default — delegate to CauseAttributionPolicy
    //
    // For every cause OTHER than the forward-compat sentinel handled
    // above, the cause→(disposition, reason, hint) triple is the
    // canonical mapping defined in `CauseAttributionPolicy.attribute`.
    // The reducer's input-precedence helpers (`isUserPaused`,
    // `isResourceBlock`, `isTransientWait`) still classify the cause
    // for ladder ordering and for ol05 invariant logging when a future
    // change makes a classifier fall out of sync with the policy — but
    // they no longer carry their own triple-emitting switch.
    //
    // Every classifier path (user-paused / resource-block / transient-
    // wait / default) routes through the same `attribute` call below,
    // so the four classifier branches differ only in the invariant
    // code they log when the classifier matches an unexpected cause.
    if isUserPaused(cause) {
        switch cause {
        case .userPreempted, .userCancelled, .appForceQuitRequiresRelaunch:
            break
        default:
            invariantLogger?.invariantViolated(
                code: .userPausedUnknownCause,
                description: "user-paused rule matched an unknown cause: \(cause)"
            )
        }
    } else if isResourceBlock(cause: cause, state: state) {
        switch cause {
        case .mediaCap, .analysisCap, .taskExpired:
            break
        default:
            invariantLogger?.invariantViolated(
                code: .resourceBlockUnknownCause,
                description: "resource-block rule matched an unknown cause: \(cause)"
            )
        }
    } else if isTransientWait(cause: cause) {
        switch cause {
        case .thermal,
             .lowPowerMode,
             .batteryLowUnplugged,
             .noNetwork,
             .wifiRequired,
             .taskExpired,
             .modelTemporarilyUnavailable:
            break
        default:
            invariantLogger?.invariantViolated(
                code: .transientWaitUnknownCause,
                description: "transient-wait rule matched an unknown cause: \(cause)"
            )
        }
    }
    // Default branch (engine errors / no-runtime-grant / unsupported-
    // episode-language) needs no classifier sentinel — `attribute`
    // covers every canonical case.

    let attribution = CauseAttributionPolicy.attribute(
        cause,
        context: attributionContext
    )
    return EpisodeSurfaceStatus(
        disposition: attribution.disposition,
        reason: attribution.reason,
        hint: attribution.hint,
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

