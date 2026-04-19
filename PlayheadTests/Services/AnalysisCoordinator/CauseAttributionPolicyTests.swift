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
        // Mirror on the ladder-only entrypoint.
        #expect(
            CauseAttributionPolicy.selectPrimary(
                causes: [],
                context: Self.benignContext
            ) == nil
        )
    }

    // MARK: - Single cause pass-through

    @Test("selectPrimary with a single cause returns that cause and empty secondary")
    func selectPrimarySingleCause() {
        // Use `selectPrimary` instead of `resolve` because `.thermal` is an
        // unmapped cause: routing it through `resolve` would trip the H1
        // DEBUG assertion in `attribute(_:context:)`. The ladder behavior
        // is what this test cares about; attribution is covered by the
        // worked-example tests below.
        let result = CauseAttributionPolicy.selectPrimary(
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
        // Tier samples are unmapped causes (chosen for context-independent
        // tier behavior), so we exercise the ladder via `selectPrimary` to
        // avoid tripping the H1 DEBUG assertion.
        for high in Self.tierSamples {
            for low in Self.tierSamples where low.tier < high.tier {
                // Both orderings, to rule out accidental first-element bias.
                let forward = CauseAttributionPolicy.selectPrimary(
                    causes: [high.cause, low.cause],
                    context: Self.benignContext
                )
                #expect(
                    forward?.primary == high.cause,
                    "\(high.cause) (\(high.tier)) should beat \(low.cause) (\(low.tier)) in [high, low] order"
                )
                #expect(forward?.secondary == [low.cause])

                let reverse = CauseAttributionPolicy.selectPrimary(
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
        // `.asrFailed`, `.pipelineError`, `.thermal` are all unmapped, so
        // the ladder is verified via `selectPrimary`.
        let engineOnly = CauseAttributionPolicy.selectPrimary(
            causes: [.asrFailed, .pipelineError],
            context: Self.benignContext
        )
        // Both are engine-tier; tie-break falls to declaration order.
        #expect(engineOnly?.primary == .asrFailed)
        #expect(engineOnly?.secondary == [.pipelineError])

        // With any non-engine cause present, engine errors lose.
        let mixed = CauseAttributionPolicy.selectPrimary(
            causes: [.asrFailed, .thermal, .pipelineError],
            context: Self.benignContext
        )
        #expect(mixed?.primary == .thermal)
    }

    // MARK: - Tie-break inside a tier

    @Test("ties within a tier are broken by InternalMissCause declaration order")
    func intraTierTieBreakIsStable() {
        // user_preempted and user_cancelled are both user-initiated and
        // both unmapped; verify the tie-break via `selectPrimary`.
        // InternalMissCause declares user_preempted before user_cancelled,
        // so user_preempted should win regardless of input order.
        let forward = CauseAttributionPolicy.selectPrimary(
            causes: [.userPreempted, .userCancelled],
            context: Self.benignContext
        )
        #expect(forward?.primary == .userPreempted)

        let reverse = CauseAttributionPolicy.selectPrimary(
            causes: [.userCancelled, .userPreempted],
            context: Self.benignContext
        )
        #expect(reverse?.primary == .userPreempted)
    }

    // MARK: - Secondary cause retention

    @Test("secondary causes retain input order, minus the primary")
    func secondaryRetainsOrder() {
        // Primary (.userCancelled) and all secondaries are unmapped, so
        // verify ordering via `selectPrimary`.
        let result = CauseAttributionPolicy.selectPrimary(
            causes: [.thermal, .userCancelled, .asrFailed, .noNetwork],
            context: Self.benignContext
        )
        #expect(result?.primary == .userCancelled)
        // Original order minus user_cancelled.
        #expect(result?.secondary == [.thermal, .asrFailed, .noNetwork])
    }

    @Test("duplicate causes are collapsed in the output")
    func duplicatesAreCollapsed() {
        // `.thermal` and `.asrFailed` are both unmapped — exercise via
        // `selectPrimary`.
        let result = CauseAttributionPolicy.selectPrimary(
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

    // MARK: - Complete 16-row mapping table (playhead-dfem)

    /// Expected `SurfaceAttribution` for each context-free
    /// `InternalMissCause`. Context-dependent rows
    /// (`.modelTemporarilyUnavailable`, `.taskExpired`) are tested
    /// separately above because they branch on `CauseAttributionContext`;
    /// both branches are already covered by the worked-example tests.
    ///
    /// Order matches `InternalMissCause.allCases` to make a missing-row
    /// diff obvious against the enum declaration.
    struct MappingExpectation: Sendable, CustomStringConvertible {
        let cause: InternalMissCause
        let expected: SurfaceAttribution

        var description: String {
            "\(cause) -> \(expected.disposition.rawValue)/\(expected.reason.rawValue)/\(expected.hint.rawValue)"
        }
    }

    /// Context-free rows: output does not depend on
    /// `CauseAttributionContext`, so a single canonical context value
    /// drives every assertion. Note the two context-dependent causes
    /// (`.modelTemporarilyUnavailable`, `.taskExpired`) are absent from
    /// this list and covered by their own tests.
    private static let contextFreeExpectations: [MappingExpectation] = [
        .init(cause: .noRuntimeGrant,
              expected: SurfaceAttribution(disposition: .queued,
                                           reason: .waitingForTime,
                                           hint: .wait)),
        .init(cause: .thermal,
              expected: SurfaceAttribution(disposition: .paused,
                                           reason: .phoneIsHot,
                                           hint: .wait)),
        .init(cause: .lowPowerMode,
              expected: SurfaceAttribution(disposition: .paused,
                                           reason: .powerLimited,
                                           hint: .chargeDevice)),
        .init(cause: .batteryLowUnplugged,
              expected: SurfaceAttribution(disposition: .paused,
                                           reason: .powerLimited,
                                           hint: .chargeDevice)),
        .init(cause: .noNetwork,
              expected: SurfaceAttribution(disposition: .paused,
                                           reason: .waitingForNetwork,
                                           hint: .none)),
        .init(cause: .wifiRequired,
              expected: SurfaceAttribution(disposition: .paused,
                                           reason: .waitingForNetwork,
                                           hint: .connectToWiFi)),
        .init(cause: .mediaCap,
              expected: SurfaceAttribution(disposition: .failed,
                                           reason: .storageFull,
                                           hint: .freeUpStorage)),
        .init(cause: .analysisCap,
              expected: SurfaceAttribution(disposition: .failed,
                                           reason: .couldntAnalyze,
                                           hint: .retry)),
        .init(cause: .userPreempted,
              expected: SurfaceAttribution(disposition: .paused,
                                           reason: .cancelled,
                                           hint: .none)),
        .init(cause: .userCancelled,
              expected: SurfaceAttribution(disposition: .paused,
                                           reason: .cancelled,
                                           hint: .none)),
        .init(cause: .unsupportedEpisodeLanguage,
              expected: SurfaceAttribution(disposition: .unavailable,
                                           reason: .analysisUnavailable,
                                           hint: .none)),
        .init(cause: .asrFailed,
              expected: SurfaceAttribution(disposition: .failed,
                                           reason: .couldntAnalyze,
                                           hint: .retry)),
        .init(cause: .pipelineError,
              expected: SurfaceAttribution(disposition: .failed,
                                           reason: .couldntAnalyze,
                                           hint: .retry)),
        .init(cause: .appForceQuitRequiresRelaunch,
              expected: SurfaceAttribution(disposition: .paused,
                                           reason: .resumeInApp,
                                           hint: .openAppToResume)),
    ]

    /// Context matrix exercised by ``contextFreeRowsMapToCanonicalTriple``.
    /// Each context-free row must produce the SAME triple across all four
    /// contexts; if any row diverges across contexts, the row is not
    /// actually context-free and the test fails. This is the adversarial
    /// safety net for the "context-free" claim — it catches a future
    /// implementation that silently branches on context for a supposedly
    /// context-free cause.
    private static let contextFreeMatrix: [CauseAttributionContext] = [
        CauseAttributionContext(modelAvailableNow: true,
                                retryBudgetRemaining: 3),
        CauseAttributionContext(modelAvailableNow: true,
                                retryBudgetRemaining: 0),
        CauseAttributionContext(modelAvailableNow: false,
                                retryBudgetRemaining: 3),
        CauseAttributionContext(modelAvailableNow: false,
                                retryBudgetRemaining: 0),
    ]

    @Test("context-free InternalMissCause rows map to their canonical triple",
          arguments: CauseAttributionPolicyTests.contextFreeExpectations)
    func contextFreeRowsMapToCanonicalTriple(
        expectation: MappingExpectation
    ) {
        // Assert the same expected triple across every context in the
        // matrix — this machine-checks the "context-free" claim. A
        // divergence here means the row is context-dependent and the
        // expectation table (or the mapping) needs revisiting.
        for context in Self.contextFreeMatrix {
            let result = CauseAttributionPolicy.resolve(
                causes: [expectation.cause],
                context: context
            )
            #expect(
                result?.attribution == expectation.expected,
                "\(expectation.cause) under \(context) expected \(expectation.expected) but got \(String(describing: result?.attribution))"
            )
        }
    }

    @Test("mappedCauses covers exactly the 16 canonical InternalMissCause cases")
    func mappedCausesCoversAllCanonicalCases() {
        let mapped = CauseAttributionPolicy.mappedCauses
        let all = Set(InternalMissCause.allCases)
        // After playhead-dfem every canonical case is mapped; no gaps,
        // no overlaps with the `.unknown(_)` sentinel.
        #expect(mapped == all)
        #expect(mapped.count == 16)
    }

    @Test("every canonical InternalMissCause case produces a valid surface triple",
          arguments: InternalMissCause.allCases)
    func everyCanonicalCaseProducesValidTriple(
        cause: InternalMissCause
    ) {
        // The benign context maps context-dependent causes to their
        // "system will recover on its own" branch. The assertion here is
        // only that a triple exists and routes through `resolve` without
        // tripping the H1 DEBUG assertion — the exact triple shape is
        // pinned by the table-driven test above (context-free rows) or
        // the worked-example tests (context-dependent rows).
        let result = CauseAttributionPolicy.resolve(
            causes: [cause],
            context: Self.benignContext
        )
        #expect(result != nil, "resolve returned nil for \(cause)")
        #expect(result?.primary == cause)
        #expect(CauseAttributionPolicy.mappedCauses.contains(cause))
    }

    // MARK: - Bucket coverage (Up Next / Paused / Recently Finished /
    //         Analysis Unavailable)

    /// The four surface buckets Plan §6 Phase 1.5 deliverable 2 requires
    /// every `InternalMissCause` to land in. Each bucket is the set of
    /// `(SurfaceDisposition, SurfaceReason)` pairs valid for that
    /// bucket. `ResolutionHint` does not participate in bucketing.
    private struct SurfacePair: Sendable, Hashable {
        let disposition: SurfaceDisposition
        let reason: SurfaceReason
    }

    private static let upNextBucket: Set<SurfacePair> = [
        // "Up Next" is the queued-work surface.
        SurfacePair(disposition: .queued, reason: .waitingForTime),
    ]

    private static let pausedBucket: Set<SurfacePair> = [
        SurfacePair(disposition: .paused, reason: .phoneIsHot),
        SurfacePair(disposition: .paused, reason: .powerLimited),
        SurfacePair(disposition: .paused, reason: .waitingForNetwork),
        SurfacePair(disposition: .paused, reason: .cancelled),
        SurfacePair(disposition: .paused, reason: .resumeInApp),
    ]

    private static let recentlyFinishedBucket: Set<SurfacePair> = [
        // "Recently Finished" is where terminal failures land so the
        // user can see them and retry.
        SurfacePair(disposition: .failed, reason: .couldntAnalyze),
        SurfacePair(disposition: .failed, reason: .storageFull),
    ]

    private static let analysisUnavailableBucket: Set<SurfacePair> = [
        SurfacePair(disposition: .unavailable,
                    reason: .analysisUnavailable),
    ]

    /// The four buckets must partition the set of every
    /// `(disposition, reason)` pair produced by the 16 canonical causes
    /// across both possible context values.
    @Test("every InternalMissCause lands in exactly one of the four surface buckets")
    func everyCauseLandsInOneBucket() {
        let contexts: [CauseAttributionContext] = [
            CauseAttributionContext(modelAvailableNow: true,
                                    retryBudgetRemaining: 3),
            CauseAttributionContext(modelAvailableNow: true,
                                    retryBudgetRemaining: 0),
            CauseAttributionContext(modelAvailableNow: false,
                                    retryBudgetRemaining: 3),
            CauseAttributionContext(modelAvailableNow: false,
                                    retryBudgetRemaining: 0),
        ]
        let allBuckets: [Set<SurfacePair>] = [
            Self.upNextBucket,
            Self.pausedBucket,
            Self.recentlyFinishedBucket,
            Self.analysisUnavailableBucket,
        ]

        for cause in InternalMissCause.allCases {
            for ctx in contexts {
                let triple = CauseAttributionPolicy.attribute(
                    cause, context: ctx
                )
                let pair = SurfacePair(
                    disposition: triple.disposition,
                    reason: triple.reason
                )
                let matches = allBuckets.filter { $0.contains(pair) }
                #expect(
                    matches.count == 1,
                    "\(cause) under \(ctx) produced \(pair) which matched \(matches.count) buckets (expected exactly 1)"
                )
            }
        }
    }

    @Test("bucket sets are pairwise disjoint")
    func bucketsArePairwiseDisjoint() {
        let buckets: [(name: String, set: Set<SurfacePair>)] = [
            ("upNext", Self.upNextBucket),
            ("paused", Self.pausedBucket),
            ("recentlyFinished", Self.recentlyFinishedBucket),
            ("analysisUnavailable", Self.analysisUnavailableBucket),
        ]
        for i in 0..<buckets.count {
            for j in (i + 1)..<buckets.count {
                let intersection = buckets[i].set.intersection(buckets[j].set)
                #expect(
                    intersection.isEmpty,
                    "buckets \(buckets[i].name) and \(buckets[j].name) overlap on \(intersection)"
                )
            }
        }
    }

    // MARK: - Unknown-sentinel behavior (forward-compat)

    #if DEBUG
    @Test(".unknown(_) returns the generic-failure triple via the ignoring helper")
    func unknownSentinelReturnsGenericFailureTriple() {
        // `.unknown(_)` is the only input that now trips the H1 DEBUG
        // assertion in `attribute(_:context:)` — the switch in
        // `attributeCore` maps every canonical case, so the only
        // unmapped input is this forward-compat sentinel. The helper
        // exercises the same body without firing the assertion so the
        // test runner stays alive.
        let triple = CauseAttributionPolicy.attributeIgnoringPlaceholderAssertion(
            .unknown("futureCauseXYZ"),
            context: Self.benignContext
        )
        #expect(triple == SurfaceAttribution(
            disposition: .failed,
            reason: .couldntAnalyze,
            hint: .retry
        ))
        #expect(!CauseAttributionPolicy.mappedCauses.contains(.unknown("futureCauseXYZ")))
    }
    #endif

    #if DEBUG
    @Test("attributeIgnoringPlaceholderAssertion returns the same value as attribute for every canonical cause")
    func ignoringHelperMatchesAttributeForAllCanonicalCauses() {
        // Smoke check that the test-only helper doesn't drift from the
        // production switch now that every canonical case is mapped.
        // Wrapped in #if DEBUG because the helper itself is DEBUG-only.
        let contexts: [CauseAttributionContext] = [
            CauseAttributionContext(modelAvailableNow: true,
                                    retryBudgetRemaining: 3),
            CauseAttributionContext(modelAvailableNow: false,
                                    retryBudgetRemaining: 0),
        ]
        for cause in InternalMissCause.allCases {
            for ctx in contexts {
                let production = CauseAttributionPolicy.attribute(
                    cause, context: ctx
                )
                let helper = CauseAttributionPolicy.attributeIgnoringPlaceholderAssertion(
                    cause, context: ctx
                )
                #expect(production == helper, "drift on \(cause) / \(ctx)")
            }
        }
    }
    #endif

    // MARK: - L1: SurfaceReason.cancelled rawValue is explicit

    @Test("SurfaceReason.cancelled has explicit rawValue 'cancelled'")
    func surfaceReasonCancelledRawValueIsExplicit() {
        // This pins the wire format: copy keys downstream anchor on the raw
        // value, so a Swift autosynthesized rawValue (which would also be
        // "cancelled") is not the same contract as one that's explicitly
        // declared. Asserting it explicitly catches a future rename of the
        // case from accidentally re-deriving a different rawValue.
        #expect(SurfaceReason.cancelled.rawValue == "cancelled")
    }

    @Test("context flip moves a resource-exhausted cause below an environmental cause")
    func taskExpiredContextFlipReshufflesPrimary() {
        // The first scenario picks `.mediaCap` as primary, which is unmapped;
        // exercise the ladder via `selectPrimary` to avoid the H1 assertion.
        // With retries remaining, task_expired is environmental-transient,
        // which is below resource-exhausted (media_cap).
        let withRetries = CauseAttributionPolicy.selectPrimary(
            causes: [.taskExpired, .mediaCap],
            context: CauseAttributionContext(
                modelAvailableNow: true,
                retryBudgetRemaining: 1
            )
        )
        #expect(withRetries?.primary == .mediaCap)

        // With the budget exhausted, task_expired is also resource-exhausted.
        // Tie-break falls to declaration order: task_expired declared before
        // media_cap, so task_expired wins. Primary is mapped here, so we can
        // exercise the full `resolve` path.
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
