// CauseAttributionPolicy.swift
// Resolves a set of live `InternalMissCause`s to a single surfaced cause via
// a fixed precedence ladder, retaining the remaining causes as secondaries.
//
// Scope note:
// The full 16-row mapping from `InternalMissCause` to
// `(SurfaceDisposition, SurfaceReason, ResolutionHint)` lives in a later bead
// (playhead-dfem, Phase 1.5). This file implements:
//   * the precedence ladder (tiers + tie-break rule),
//   * `resolve(...)` for selecting the primary cause, and
//   * the three hardest context-dependent mappings as worked examples:
//     `model_temporarily_unavailable`, `task_expired`, and
//     `app_force_quit_requires_relaunch`.
// The 13 remaining rows are deliberately left as TODO hooks in
// `attribute(_:context:)` so dfem can fill them in without changing the
// public surface.

import Foundation

// MARK: - Context

/// Context inputs the attribution policy needs when the triple produced by a
/// cause depends on runtime state (e.g. whether the Foundation Models runtime
/// will be back without user intervention, whether the current task has any
/// retry budget remaining).
///
/// Keep the fields as plain primitives so this struct stays independent of
/// other beads. In particular, `modelAvailableNow` mirrors a field that
/// `AnalysisEligibility` will expose (playhead-2fd) but we take it as a
/// `Bool` here so the two beads can land in either order.
struct CauseAttributionContext: Sendable, Hashable {
    /// `true` when the Foundation Models runtime is currently ready to serve
    /// analysis (the user has Apple Intelligence enabled, the asset has
    /// loaded, the locale is supported, etc.). `false` when the user must
    /// take action to enable it.
    let modelAvailableNow: Bool

    /// Remaining attempts for the current task. `0` means the retry budget
    /// is exhausted; any positive value means the system will retry on its
    /// own without user intervention.
    let retryBudgetRemaining: Int

    init(modelAvailableNow: Bool, retryBudgetRemaining: Int) {
        self.modelAvailableNow = modelAvailableNow
        self.retryBudgetRemaining = retryBudgetRemaining
    }
}

// MARK: - Resolution

/// Result of evaluating the attribution policy across a set of live causes.
///
/// `primary` is the single cause the UI should surface; `secondary` is the
/// remaining live causes in the same order they were provided. Callers that
/// want to log the full set, or that want to display additional detail in a
/// debug UI, use `secondary`.
struct CauseResolution: Sendable, Hashable {
    let primary: InternalMissCause
    let secondary: [InternalMissCause]
    let attribution: SurfaceAttribution
}

// MARK: - Policy

enum CauseAttributionPolicy {

    /// Precedence tiers. Higher `rawValue` wins.
    ///
    /// `engineError` is not one of the four nominal tiers documented in the
    /// plan; it is the catch-all for `asr_failed` and `pipeline_error`, which
    /// should only surface when nothing else is live. Encoding it as the
    /// lowest tier makes the ladder total over every `InternalMissCause`.
    enum Tier: Int, Sendable, Comparable {
        case engineError = 0
        case eligibilityPermanent = 1
        case environmentalTransient = 2
        case resourceExhausted = 3
        case userInitiated = 4

        static func < (lhs: Tier, rhs: Tier) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }

    /// The tier a cause belongs to, modulo context.
    ///
    /// `task_expired` shifts tier based on whether retries remain: with
    /// retries remaining the system is in an "environmental transient" wait
    /// (it will try again on its own); with the budget exhausted it is a
    /// resource-exhausted failure that will not self-correct.
    ///
    /// `model_temporarily_unavailable` shifts tier based on
    /// `modelAvailableNow`: when the runtime is not currently available the
    /// user must act to enable it (eligibility), otherwise the system will
    /// recover on its own (environmental transient).
    static func tier(
        for cause: InternalMissCause,
        context: CauseAttributionContext
    ) -> Tier {
        switch cause {
        // user-initiated
        case .userCancelled,
             .userPreempted,
             .appForceQuitRequiresRelaunch:
            return .userInitiated

        // resource-exhausted
        case .mediaCap,
             .analysisCap:
            return .resourceExhausted
        case .taskExpired:
            return context.retryBudgetRemaining > 0
                ? .environmentalTransient
                : .resourceExhausted

        // environmental-transient
        case .thermal,
             .lowPowerMode,
             .batteryLowUnplugged,
             .noNetwork,
             .wifiRequired:
            return .environmentalTransient

        // eligibility-permanent
        case .unsupportedEpisodeLanguage,
             .noRuntimeGrant:
            return .eligibilityPermanent
        case .modelTemporarilyUnavailable:
            return context.modelAvailableNow
                ? .environmentalTransient
                : .eligibilityPermanent

        // engine errors — lowest priority unless nothing else is live
        case .asrFailed,
             .pipelineError:
            return .engineError
        }
    }

    /// Resolve a set of live causes to the one the UI should surface.
    ///
    /// - Parameters:
    ///   - causes: The live `InternalMissCause`s. Duplicates are collapsed
    ///     while preserving first occurrence order.
    ///   - context: Runtime state the policy needs for the context-dependent
    ///     tiers and mappings.
    /// - Returns: `nil` when `causes` is empty; otherwise the resolved
    ///   primary plus the remaining causes (ordered by the caller's input
    ///   order, minus the primary).
    ///
    /// Tie-break: when multiple causes share the top tier, the winner is the
    /// one whose declaration order in `InternalMissCause.allCases` comes
    /// first. This is deterministic and stable across runs; callers that
    /// need a different ordering should filter `causes` before calling.
    static func resolve(
        causes: [InternalMissCause],
        context: CauseAttributionContext
    ) -> CauseResolution? {
        let deduped = deduplicatePreservingOrder(causes)
        guard let primary = _internalSelectPrimary(deduped, context: context)
        else { return nil }

        let secondary = deduped.filter { $0 != primary }

        // Use the asserting variant: production UI surfaces consume
        // `resolve(...)` (not `attribute(...)` directly), so the DEBUG
        // safety net only fires if it stays on this path. Tests that
        // intentionally exercise unmapped causes must call
        // `attributeIgnoringPlaceholderAssertion(_:context:)` directly
        // (DEBUG-only) rather than going through `resolve`.
        let attribution = attribute(primary, context: context)
        return CauseResolution(
            primary: primary,
            secondary: secondary,
            attribution: attribution
        )
    }

    /// Run the dedup + precedence ladder and return the chosen primary cause.
    ///
    /// Production-callable from `resolve(...)` only. The result is JUST the
    /// primary cause — there is no `PrimarySelection` wrapper exposed to
    /// production code, so a future contributor cannot grab a primary cause
    /// from the ladder and wire it into UI without going through
    /// `attribute(...)` (which fires the H1 safety-net assertion for
    /// unmapped causes).
    ///
    /// Pre: `deduped` has already been run through
    /// ``deduplicatePreservingOrder(_:)`` — required because `max`'s
    /// tie-break is undefined for repeated identical entries, and the
    /// precedence ladder must be a stable total order over distinct causes.
    private static func _internalSelectPrimary(
        _ deduped: [InternalMissCause],
        context: CauseAttributionContext
    ) -> InternalMissCause? {
        guard !deduped.isEmpty else { return nil }

        let declarationIndex = declarationIndexMap()
        return deduped.max { a, b in
            let aTier = tier(for: a, context: context)
            let bTier = tier(for: b, context: context)
            if aTier != bTier {
                return aTier < bTier
            }
            // Same tier: lower declaration index wins. `max` returns the
            // element for which the closure is `true`, so we want `true` when
            // `a` should lose — i.e. when `a`'s index is greater than `b`'s.
            let aIndex = declarationIndex[a] ?? .max
            let bIndex = declarationIndex[b] ?? .max
            return aIndex > bIndex
        }
    }

    #if DEBUG
    /// Result of running the precedence ladder without computing the
    /// `SurfaceAttribution` triple. The two fields mirror the like-named
    /// fields on ``CauseResolution`` exactly — the only thing missing is
    /// the attribution mapping.
    ///
    /// Test-only; do not wire into UI. Compiled out of Release so production
    /// code cannot accidentally bypass the H1 safety net by surfacing a
    /// primary cause without going through ``attribute(_:context:)``.
    struct PrimarySelection: Sendable, Hashable {
        let primary: InternalMissCause
        let secondary: [InternalMissCause]
    }

    /// Test-only entrypoint for ladder-coverage tests. Production code MUST
    /// use ``resolve(causes:context:)`` so the H1 safety net fires for
    /// unmapped causes. Not compiled in Release.
    ///
    /// Returns the same primary the ladder picks inside `resolve`, plus the
    /// dedup'd remaining causes (computed identically to `resolve`'s
    /// `secondary`).
    static func selectPrimary(
        causes: [InternalMissCause],
        context: CauseAttributionContext
    ) -> PrimarySelection? {
        let deduped = deduplicatePreservingOrder(causes)
        guard let primary = _internalSelectPrimary(deduped, context: context)
        else { return nil }
        let secondary = deduped.filter { $0 != primary }
        return PrimarySelection(primary: primary, secondary: secondary)
    }
    #endif

    // MARK: - Attribution (three hardest mappings as worked examples)

    /// The set of `InternalMissCause`s for which `attribute(_:context:)` returns
    /// a real, reviewed mapping (vs the catch-all placeholder triple). Callers
    /// that wire `resolve(...)` into a UI surface MUST branch on this set and
    /// fall back to a generic "couldn't analyze" copy path for unmapped causes
    /// rather than rendering the placeholder verbatim — the placeholder is a
    /// sentinel for unfinished work, not a copy choice.
    ///
    /// Filled in incrementally: this bead lands the three worked examples;
    /// playhead-dfem will expand the set as it adds the remaining rows.
    static let mappedCauses: Set<InternalMissCause> = [
        .modelTemporarilyUnavailable,
        .taskExpired,
        .appForceQuitRequiresRelaunch,
    ]

    /// Map an `InternalMissCause` to its `SurfaceAttribution` triple, applying
    /// context where the triple is context-dependent.
    ///
    /// Only the three hardest rows are implemented as worked examples in this
    /// bead; the remaining 13 rows are filled in by playhead-dfem. For rows
    /// not yet implemented we return a conservative placeholder
    /// (`failed` + `couldntAnalyze` + `retry`) so the surface layer always has
    /// a valid triple to display. This placeholder is intentionally audible
    /// (it looks like a failure) so that missing-mapping bugs show up in
    /// review before ship.
    ///
    /// In DEBUG builds, hitting the placeholder path additionally fires an
    /// `assertionFailure` so any code that wires this result into UI without
    /// first branching on `mappedCauses` will crash loudly in dev. Tests that
    /// intentionally exercise the placeholder branch should call
    /// `attributeIgnoringPlaceholderAssertion(_:context:)` instead, which
    /// shares the same logic without the assertion.
    static func attribute(
        _ cause: InternalMissCause,
        context: CauseAttributionContext
    ) -> SurfaceAttribution {
        if !mappedCauses.contains(cause) {
            assertionFailure(
                "CauseAttributionPolicy.attribute placeholder hit for \(cause); fill in playhead-dfem"
            )
        }
        return attributeCore(cause, context: context)
    }

    #if DEBUG
    /// Test-only entrypoint: returns the same triple as `attribute(_:context:)`
    /// but never fires the DEBUG `assertionFailure`. Intended for tests that
    /// explicitly verify the placeholder behavior for unmapped causes.
    ///
    /// Compiled out of Release so production code cannot accidentally bypass
    /// the H1 safety net by routing through this helper.
    static func attributeIgnoringPlaceholderAssertion(
        _ cause: InternalMissCause,
        context: CauseAttributionContext
    ) -> SurfaceAttribution {
        attributeCore(cause, context: context)
    }
    #endif

    /// Shared mapping body for `attribute(_:context:)` and the test-only
    /// placeholder-tolerant variant. Keeping the switch in one place ensures
    /// the two entrypoints can never drift.
    private static func attributeCore(
        _ cause: InternalMissCause,
        context: CauseAttributionContext
    ) -> SurfaceAttribution {
        switch cause {
        // MARK: Worked example 1: model_temporarily_unavailable
        /// `retryBudgetRemaining` is intentionally not consulted here:
        /// availability of the FM runtime is gated on user/system action
        /// (Apple Intelligence enablement, asset load), not on retry
        /// attempts within a task. The branch is on `modelAvailableNow`
        /// because that is the only signal that distinguishes "system will
        /// recover on its own" from "user must take action to enable".
        case .modelTemporarilyUnavailable:
            if context.modelAvailableNow {
                // Runtime expected back without user action: present as a
                // normal wait.
                return SurfaceAttribution(
                    disposition: .queued,
                    reason: .waitingForTime,
                    hint: .wait
                )
            } else {
                return SurfaceAttribution(
                    disposition: .unavailable,
                    reason: .analysisUnavailable,
                    hint: .enableAppleIntelligence
                )
            }

        // MARK: Worked example 2: task_expired
        case .taskExpired:
            if context.retryBudgetRemaining > 0 {
                return SurfaceAttribution(
                    disposition: .queued,
                    reason: .waitingForTime,
                    hint: .wait
                )
            } else {
                return SurfaceAttribution(
                    disposition: .failed,
                    reason: .couldntAnalyze,
                    hint: .retry
                )
            }

        // MARK: Worked example 3: app_force_quit_requires_relaunch
        case .appForceQuitRequiresRelaunch:
            // Always the same triple. Never auto-retried — the user must
            // open the app to resume.
            return SurfaceAttribution(
                disposition: .paused,
                reason: .resumeInApp,
                hint: .openAppToResume
            )

        // MARK: Remaining 13 rows — deferred to playhead-dfem (Phase 1.5).
        //
        // Leaving these as a single default keeps the policy total without
        // prematurely freezing mappings that will be reviewed holistically
        // in dfem. See the bead plan §6 Phase 0 / Phase 1.5 split.
        case .noRuntimeGrant,
             .thermal,
             .lowPowerMode,
             .batteryLowUnplugged,
             .noNetwork,
             .wifiRequired,
             .mediaCap,
             .analysisCap,
             .userPreempted,
             .userCancelled,
             .unsupportedEpisodeLanguage,
             .asrFailed,
             .pipelineError:
            // TODO(playhead-dfem): fill in the remaining 13 rows.
            return SurfaceAttribution(
                disposition: .failed,
                reason: .couldntAnalyze,
                hint: .retry
            )
        }
    }

    // MARK: - Private helpers

    private static func deduplicatePreservingOrder(
        _ causes: [InternalMissCause]
    ) -> [InternalMissCause] {
        var seen: Set<InternalMissCause> = []
        var result: [InternalMissCause] = []
        result.reserveCapacity(causes.count)
        for cause in causes where seen.insert(cause).inserted {
            result.append(cause)
        }
        return result
    }

    private static func declarationIndexMap() -> [InternalMissCause: Int] {
        var map: [InternalMissCause: Int] = [:]
        map.reserveCapacity(InternalMissCause.allCases.count)
        for (index, cause) in InternalMissCause.allCases.enumerated() {
            map[cause] = index
        }
        return map
    }
}
