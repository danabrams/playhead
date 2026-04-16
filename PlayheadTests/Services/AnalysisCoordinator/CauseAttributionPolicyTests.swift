// CauseAttributionPolicyTests.swift
// Tests for the precedence ladder, secondary-cause retention, and the three
// worked-example contextual mappings.

import Foundation
import Testing

@testable import Playhead

@Suite("CauseAttributionPolicy")
struct CauseAttributionPolicyTests {

    // MARK: - Canonical context values

    /// Context in which `task_expired` still has retries and the model is
    /// available now. Used when neither of those flags should affect the
    /// outcome under test.
    private static let benignContext = CauseAttributionContext(
        modelAvailableNow: true,
        retryBudgetRemaining: 3
    )

    // MARK: - Empty input

    @Test("resolve returns nil for an empty cause set")
    func resolveEmptyReturnsNil() {
        let result = CauseAttributionPolicy.resolve(
            causes: [],
            context: Self.benignContext
        )
        #expect(result == nil)
    }

    // MARK: - Single cause pass-through

    @Test("resolve with a single cause returns that cause and empty secondary")
    func resolveSingleCause() {
        let result = CauseAttributionPolicy.resolve(
            causes: [.thermal],
            context: Self.benignContext
        )
        #expect(result?.primary == .thermal)
        #expect(result?.secondary.isEmpty == true)
    }

    // MARK: - Precedence ladder (tier wins against every lower tier)

    /// Representative cause per tier, chosen to avoid context-dependent
    /// behavior so each test exercises only the ladder.
    private struct TierSample {
        let tier: CauseAttributionPolicy.Tier
        let cause: InternalMissCause
    }

    private static let tierSamples: [TierSample] = [
        .init(tier: .userInitiated, cause: .userCancelled),
        .init(tier: .resourceExhausted, cause: .mediaCap),
        .init(tier: .environmentalTransient, cause: .thermal),
        .init(tier: .eligibilityPermanent, cause: .unsupportedEpisodeLanguage),
        .init(tier: .engineError, cause: .asrFailed),
    ]

    @Test("higher tier always beats lower tier")
    func higherTierBeatsLowerTier() {
        for high in Self.tierSamples {
            for low in Self.tierSamples where low.tier < high.tier {
                // Both orderings, to rule out accidental first-element bias.
                let forward = CauseAttributionPolicy.resolve(
                    causes: [high.cause, low.cause],
                    context: Self.benignContext
                )
                #expect(
                    forward?.primary == high.cause,
                    "\(high.cause) (\(high.tier)) should beat \(low.cause) (\(low.tier)) in [high, low] order"
                )
                #expect(forward?.secondary == [low.cause])

                let reverse = CauseAttributionPolicy.resolve(
                    causes: [low.cause, high.cause],
                    context: Self.benignContext
                )
                #expect(
                    reverse?.primary == high.cause,
                    "\(high.cause) (\(high.tier)) should beat \(low.cause) (\(low.tier)) in [low, high] order"
                )
                #expect(reverse?.secondary == [low.cause])
            }
        }
    }

    @Test("engine errors only win when nothing else is live")
    func engineErrorsLoseToEverythingElse() {
        let engineOnly = CauseAttributionPolicy.resolve(
            causes: [.asrFailed, .pipelineError],
            context: Self.benignContext
        )
        // Both are engine-tier; tie-break falls to declaration order.
        #expect(engineOnly?.primary == .asrFailed)
        #expect(engineOnly?.secondary == [.pipelineError])

        // With any non-engine cause present, engine errors lose.
        let mixed = CauseAttributionPolicy.resolve(
            causes: [.asrFailed, .thermal, .pipelineError],
            context: Self.benignContext
        )
        #expect(mixed?.primary == .thermal)
    }

    // MARK: - Tie-break inside a tier

    @Test("ties within a tier are broken by InternalMissCause declaration order")
    func intraTierTieBreakIsStable() {
        // user_preempted and user_cancelled are both user-initiated.
        // InternalMissCause declares user_preempted before user_cancelled,
        // so user_preempted should win regardless of input order.
        let forward = CauseAttributionPolicy.resolve(
            causes: [.userPreempted, .userCancelled],
            context: Self.benignContext
        )
        #expect(forward?.primary == .userPreempted)

        let reverse = CauseAttributionPolicy.resolve(
            causes: [.userCancelled, .userPreempted],
            context: Self.benignContext
        )
        #expect(reverse?.primary == .userPreempted)
    }

    // MARK: - Secondary cause retention

    @Test("secondary causes retain input order, minus the primary")
    func secondaryRetainsOrder() {
        let result = CauseAttributionPolicy.resolve(
            causes: [.thermal, .userCancelled, .asrFailed, .noNetwork],
            context: Self.benignContext
        )
        #expect(result?.primary == .userCancelled)
        // Original order minus user_cancelled.
        #expect(result?.secondary == [.thermal, .asrFailed, .noNetwork])
    }

    @Test("duplicate causes are collapsed in the output")
    func duplicatesAreCollapsed() {
        let result = CauseAttributionPolicy.resolve(
            causes: [.thermal, .thermal, .asrFailed, .thermal],
            context: Self.benignContext
        )
        #expect(result?.primary == .thermal)
        #expect(result?.secondary == [.asrFailed])
    }

    // MARK: - Worked example 1: model_temporarily_unavailable

    @Test("model_temporarily_unavailable maps to analysis_unavailable when model is not available now")
    func modelTempUnavailableMapsEligibilityWhenUnavailable() {
        let context = CauseAttributionContext(
            modelAvailableNow: false,
            retryBudgetRemaining: 3
        )
        let result = CauseAttributionPolicy.resolve(
            causes: [.modelTemporarilyUnavailable],
            context: context
        )
        #expect(
            result?.attribution == SurfaceAttribution(
                disposition: .unavailable,
                reason: .analysisUnavailable,
                hint: .enableAppleIntelligence
            )
        )
        // Also verify the tier reflects the eligibility interpretation.
        let tier = CauseAttributionPolicy.tier(
            for: .modelTemporarilyUnavailable,
            context: context
        )
        #expect(tier == .eligibilityPermanent)
    }

    @Test("model_temporarily_unavailable maps to waiting_for_time when model is available now")
    func modelTempUnavailableMapsTransientWhenAvailable() {
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 3
        )
        let result = CauseAttributionPolicy.resolve(
            causes: [.modelTemporarilyUnavailable],
            context: context
        )
        #expect(
            result?.attribution == SurfaceAttribution(
                disposition: .queued,
                reason: .waitingForTime,
                hint: .wait
            )
        )
        let tier = CauseAttributionPolicy.tier(
            for: .modelTemporarilyUnavailable,
            context: context
        )
        #expect(tier == .environmentalTransient)
    }

    // MARK: - Worked example 2: task_expired

    @Test("task_expired with retries remaining maps to waiting_for_time")
    func taskExpiredWithRetriesMapsToWait() {
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 2
        )
        let result = CauseAttributionPolicy.resolve(
            causes: [.taskExpired],
            context: context
        )
        #expect(
            result?.attribution == SurfaceAttribution(
                disposition: .queued,
                reason: .waitingForTime,
                hint: .wait
            )
        )
        let tier = CauseAttributionPolicy.tier(
            for: .taskExpired,
            context: context
        )
        #expect(tier == .environmentalTransient)
    }

    @Test("task_expired with exhausted retries maps to couldnt_analyze")
    func taskExpiredExhaustedMapsToFailed() {
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 0
        )
        let result = CauseAttributionPolicy.resolve(
            causes: [.taskExpired],
            context: context
        )
        #expect(
            result?.attribution == SurfaceAttribution(
                disposition: .failed,
                reason: .couldntAnalyze,
                hint: .retry
            )
        )
        let tier = CauseAttributionPolicy.tier(
            for: .taskExpired,
            context: context
        )
        #expect(tier == .resourceExhausted)
    }

    // MARK: - Worked example 3: app_force_quit_requires_relaunch

    @Test(
        "app_force_quit_requires_relaunch always maps to resume_in_app regardless of context",
        arguments: [
            CauseAttributionContext(modelAvailableNow: false, retryBudgetRemaining: 0),
            CauseAttributionContext(modelAvailableNow: false, retryBudgetRemaining: 5),
            CauseAttributionContext(modelAvailableNow: true, retryBudgetRemaining: 0),
            CauseAttributionContext(modelAvailableNow: true, retryBudgetRemaining: 5),
        ]
    )
    func forceQuitAlwaysMapsToResumeInApp(
        context: CauseAttributionContext
    ) {
        let result = CauseAttributionPolicy.resolve(
            causes: [.appForceQuitRequiresRelaunch],
            context: context
        )
        #expect(
            result?.attribution == SurfaceAttribution(
                disposition: .paused,
                reason: .resumeInApp,
                hint: .openAppToResume
            )
        )
        let tier = CauseAttributionPolicy.tier(
            for: .appForceQuitRequiresRelaunch,
            context: context
        )
        #expect(tier == .userInitiated)
    }

    // MARK: - Multi-cause scenario end-to-end

    @Test("multi-live scenario resolves to highest tier and preserves rest")
    func multiLiveScenarioResolves() {
        // Realistic cocktail: user force-quit while a thermal pause and an
        // engine error were both live. The ladder must pick the user-initiated
        // cause and return the appropriate surface attribution for it.
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 1
        )
        let result = CauseAttributionPolicy.resolve(
            causes: [.thermal, .asrFailed, .appForceQuitRequiresRelaunch],
            context: context
        )
        #expect(result?.primary == .appForceQuitRequiresRelaunch)
        #expect(result?.secondary == [.thermal, .asrFailed])
        #expect(
            result?.attribution == SurfaceAttribution(
                disposition: .paused,
                reason: .resumeInApp,
                hint: .openAppToResume
            )
        )
    }

    @Test("context flip moves a resource-exhausted cause below an environmental cause")
    func taskExpiredContextFlipReshufflesPrimary() {
        // With retries remaining, task_expired is environmental-transient,
        // which is below resource-exhausted (media_cap).
        let withRetries = CauseAttributionPolicy.resolve(
            causes: [.taskExpired, .mediaCap],
            context: CauseAttributionContext(
                modelAvailableNow: true,
                retryBudgetRemaining: 1
            )
        )
        #expect(withRetries?.primary == .mediaCap)

        // With the budget exhausted, task_expired is also resource-exhausted.
        // Tie-break falls to declaration order: task_expired declared before
        // media_cap, so task_expired wins.
        let exhausted = CauseAttributionPolicy.resolve(
            causes: [.taskExpired, .mediaCap],
            context: CauseAttributionContext(
                modelAvailableNow: true,
                retryBudgetRemaining: 0
            )
        )
        #expect(exhausted?.primary == .taskExpired)
    }
}
