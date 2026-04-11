// TrustScoringServiceTests.swift
// Dedicated unit tests for the per-show trust scoring system (playhead-3cw).

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

/// Creates a TrustScoringService backed by an isolated temp store with an
/// optional pre-seeded PodcastProfile.
private func makeSUT(
    config: TrustScoringConfig = .default,
    seedProfile: PodcastProfile? = nil
) async throws -> (service: TrustScoringService, store: AnalysisStore) {
    let store = try await makeTestStore()
    if let profile = seedProfile {
        try await store.upsertProfile(profile)
    }
    let service = TrustScoringService(store: store, config: config)
    return (service, store)
}

private let testPodcastId = "trust-test-podcast"

/// Floating-point tolerance for trust score comparisons.
/// 0.1 + 0.2 != 0.3 in IEEE 754; this absorbs that.
private let scoreTolerance = 1e-10

private func expectScore(
    _ actual: Double?,
    equals expected: Double,
    sourceLocation: SourceLocation = #_sourceLocation
) {
    guard let actual else {
        Issue.record("Expected score \(expected) but profile was nil", sourceLocation: sourceLocation)
        return
    }
    #expect(
        abs(actual - expected) < scoreTolerance,
        "Expected score \(expected), got \(actual)",
        sourceLocation: sourceLocation
    )
}

private func makeProfile(
    mode: String = SkipMode.shadow.rawValue,
    trustScore: Double = 0.2,
    observations: Int = 0,
    falseSignals: Int = 0,
    falsePosCount: Int = 0
) -> PodcastProfile {
    PodcastProfile(
        podcastId: testPodcastId,
        sponsorLexicon: nil,
        normalizedAdSlotPriors: nil,
        repeatedCTAFragments: nil,
        jingleFingerprints: nil,
        implicitFalsePositiveCount: falsePosCount,
        skipTrustScore: trustScore,
        observationCount: observations,
        mode: mode,
        recentFalseSkipSignals: falseSignals
    )
}

// MARK: - Tests

@Suite("TrustScoringService", .serialized)
struct TrustScoringServiceTests {

    // MARK: AC 1 — New shows start in .shadow

    @Test("New show with no profile returns .shadow mode")
    func newShowDefaultsShadow() async throws {
        let (sut, _) = try await makeSUT()
        let mode = await sut.effectiveMode(podcastId: testPodcastId)
        #expect(mode == .shadow)
    }

    @Test("effectiveMode returns stored mode from existing profile")
    func effectiveModeReturnsStoredMode() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.50, observations: 5)
        let (sut, _) = try await makeSUT(seedProfile: seed)
        let mode = await sut.effectiveMode(podcastId: testPodcastId)
        #expect(mode == .manual)
    }

    @Test("First observation with low confidence creates .shadow profile")
    func firstObservationLowConfidenceCreatesShadow() async throws {
        let (sut, store) = try await makeSUT()
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.60)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile != nil)
        #expect(profile?.mode == SkipMode.shadow.rawValue)
        #expect(profile?.observationCount == 1)
        expectScore(profile?.skipTrustScore, equals: 0.2)
    }

    // MARK: AC 2 — Exceptional first episode

    @Test("Exceptional first episode (avgConfidence >= 0.92) starts .manual with score 0.5")
    func exceptionalFirstEpisodePromotesToManual() async throws {
        let (sut, store) = try await makeSUT()
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.95)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile != nil)
        #expect(profile?.mode == SkipMode.manual.rawValue)
        expectScore(profile?.skipTrustScore, equals: 0.5)
        #expect(profile?.observationCount == 1)
    }

    @Test("Confidence exactly at 0.92 threshold triggers exceptional path")
    func exceptionalAtExactThreshold() async throws {
        let (sut, store) = try await makeSUT()
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.92)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
        expectScore(profile?.skipTrustScore, equals: 0.5)
    }

    @Test("Confidence just below 0.92 does not trigger exceptional path")
    func justBelowExceptionalThreshold() async throws {
        let (sut, store) = try await makeSUT()
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.919)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.shadow.rawValue)
        expectScore(profile?.skipTrustScore, equals: 0.2)
    }

    // MARK: AC 3 — Promotion .shadow -> .manual

    @Test("Promotion shadow->manual after >= 3 observations with score >= 0.4")
    func promotionShadowToManual() async throws {
        // Seed: 2 observations, score 0.3 (needs one more obs to hit 3, score will become 0.4)
        let seed = makeProfile(mode: "shadow", trustScore: 0.3, observations: 2)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.70)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
        #expect(profile?.observationCount == 3)
        // 0.3 + 0.10 bonus = 0.4
        expectScore(profile?.skipTrustScore, equals: 0.4)
    }

    @Test("No promotion shadow->manual when observations < 3")
    func noPromotionShadowToManualInsufficientObservations() async throws {
        let seed = makeProfile(mode: "shadow", trustScore: 0.5, observations: 1)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.70)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.shadow.rawValue)
        #expect(profile?.observationCount == 2)
    }

    @Test("No promotion shadow->manual when score < 0.4")
    func noPromotionShadowToManualInsufficientScore() async throws {
        let seed = makeProfile(mode: "shadow", trustScore: 0.2, observations: 2)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.70)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        // 0.2 + 0.10 = 0.3 < 0.4 threshold
        #expect(profile?.mode == SkipMode.shadow.rawValue)
        expectScore(profile?.skipTrustScore, equals: 0.3)
    }

    @Test("No promotion shadow->manual when score just below 0.4 boundary")
    func noPromotionShadowToManualJustBelowScoreBoundary() async throws {
        // 0.29 + 0.10 = 0.39 < 0.4, even though obs threshold is met
        let seed = makeProfile(mode: "shadow", trustScore: 0.29, observations: 2)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.70)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.shadow.rawValue)
        #expect(profile?.observationCount == 3)
        expectScore(profile?.skipTrustScore, equals: 0.39)
    }

    // MARK: AC 4 — Promotion .manual -> .auto

    @Test("Promotion manual->auto after >= 8 observations with score >= 0.75 and zero false signals")
    func promotionManualToAuto() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.65, observations: 7, falseSignals: 0)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.85)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.auto.rawValue)
        #expect(profile?.observationCount == 8)
        // 0.65 + 0.10 = 0.75
        expectScore(profile?.skipTrustScore, equals: 0.75)
    }

    @Test("No promotion manual->auto when false signals > 0")
    func noPromotionManualToAutoWithFalseSignals() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.80, observations: 9, falseSignals: 1)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.90)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
    }

    @Test("No promotion manual->auto when observations < 8")
    func noPromotionManualToAutoInsufficientObservations() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.80, observations: 6)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.90)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
        #expect(profile?.observationCount == 7)
    }

    @Test("No promotion manual->auto when score < 0.75")
    func noPromotionManualToAutoInsufficientScore() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.60, observations: 9)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.90)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        // 0.60 + 0.10 = 0.70 < 0.75
        #expect(profile?.mode == SkipMode.manual.rawValue)
    }

    // MARK: AC 5 — Demotion .auto -> .manual

    @Test("Demotion auto->manual after 2 false-skip signals")
    func demotionAutoToManual() async throws {
        let seed = makeProfile(mode: "auto", trustScore: 0.80, observations: 10, falseSignals: 1)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
        #expect(profile?.recentFalseSkipSignals == 2)
    }

    @Test("No demotion auto->manual with only 1 false signal")
    func noDemotionAutoToManualOneFalseSignal() async throws {
        let seed = makeProfile(mode: "auto", trustScore: 0.80, observations: 10, falseSignals: 0)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.auto.rawValue)
        #expect(profile?.recentFalseSkipSignals == 1)
    }

    // MARK: AC 6 — Demotion .manual -> .shadow

    @Test("Demotion manual->shadow after 4 false-skip signals")
    func demotionManualToShadow() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.50, observations: 5, falseSignals: 3)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.shadow.rawValue)
        #expect(profile?.recentFalseSkipSignals == 4)
    }

    @Test("No demotion manual->shadow with only 3 false signals")
    func noDemotionManualToShadowThreeSignals() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.50, observations: 5, falseSignals: 2)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
        #expect(profile?.recentFalseSkipSignals == 3)
    }

    // MARK: AC 7 — Score decay: false signal count halved per clean episode

    @Test("Decay halves false signal count via integer division")
    func decayHalvesFalseSignals() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.50, observations: 5, falseSignals: 5)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.decayFalseSignals(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        // 5 / 2 = 2 (integer division)
        #expect(profile?.recentFalseSkipSignals == 2)
    }

    @Test("Decay of 1 false signal goes to 0")
    func decaySingleFalseSignalToZero() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.50, observations: 5, falseSignals: 1)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.decayFalseSignals(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        // 1 / 2 = 0 (integer division)
        #expect(profile?.recentFalseSkipSignals == 0)
    }

    @Test("Decay with 0 false signals is a no-op")
    func decayWithZeroFalseSignalsNoOp() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.50, observations: 5, falseSignals: 0)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.decayFalseSignals(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.recentFalseSkipSignals == 0)
        // Trust score should be unchanged
        expectScore(profile?.skipTrustScore, equals: 0.50)
    }

    @Test("Decay does not modify trust score or mode")
    func decayPreservesScoreAndMode() async throws {
        let seed = makeProfile(mode: "auto", trustScore: 0.85, observations: 10, falseSignals: 4)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.decayFalseSignals(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        expectScore(profile?.skipTrustScore, equals: 0.85)
        #expect(profile?.mode == SkipMode.auto.rawValue)
        #expect(profile?.recentFalseSkipSignals == 2)
    }

    // MARK: AC 8 — User override

    @Test("User override sets mode without changing trust score")
    func userOverrideSetsMode() async throws {
        let seed = makeProfile(mode: "shadow", trustScore: 0.30, observations: 2)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.setUserOverride(podcastId: testPodcastId, mode: .auto)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.auto.rawValue)
        expectScore(profile?.skipTrustScore, equals: 0.30)
    }

    @Test("User override does not prevent future automatic mode changes")
    func userOverrideDoesNotPreventFutureChanges() async throws {
        let seed = makeProfile(mode: "shadow", trustScore: 0.30, observations: 2)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        // User forces to auto
        await sut.setUserOverride(podcastId: testPodcastId, mode: .auto)

        // Two false signals should demote auto -> manual
        await sut.recordFalseSkipSignal(podcastId: testPodcastId)
        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
    }

    @Test("User override on nonexistent profile creates profile with that mode")
    func userOverrideCreatesProfile() async throws {
        let (sut, store) = try await makeSUT()

        await sut.setUserOverride(podcastId: testPodcastId, mode: .manual)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile != nil)
        #expect(profile?.mode == SkipMode.manual.rawValue)
        expectScore(profile?.skipTrustScore, equals: 0.5)
        #expect(profile?.observationCount == 0)
    }

    // MARK: AC 9 — Trust score bounded [0.0, 1.0]

    @Test("Trust score capped at 1.0 after many observations")
    func trustScoreCappedAtOne() async throws {
        let seed = makeProfile(mode: "auto", trustScore: 0.95, observations: 20)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.99)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        // 0.95 + 0.10 = 1.05 -> capped to 1.0
        expectScore(profile?.skipTrustScore, equals: 1.0)
    }

    @Test("Trust score floored at 0.0 after many false signals")
    func trustScoreFlooredAtZero() async throws {
        let seed = makeProfile(mode: "shadow", trustScore: 0.05, observations: 5, falseSignals: 0)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        // 0.05 - 0.10 = -0.05 -> clamped to 0.0
        expectScore(profile?.skipTrustScore, equals: 0.0)
    }

    // MARK: AC 11 — Each false signal decrements skipTrustScore by 0.10

    @Test("False signal decrements trust score by falseSignalPenalty (0.10)")
    func falseSignalDecrementsTrustScore() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.60, observations: 5, falseSignals: 0)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        expectScore(profile?.skipTrustScore, equals: 0.50)
    }

    @Test("Multiple false signals decrement cumulatively")
    func multipleFalseSignalsDecrementCumulatively() async throws {
        let seed = makeProfile(mode: "auto", trustScore: 0.80, observations: 10, falseSignals: 0)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)
        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        // 0.80 - 0.10 - 0.10 = 0.60
        expectScore(profile?.skipTrustScore, equals: 0.60)
    }

    // MARK: - Correct observation bonus

    @Test("Successful observation increments trust score by correctObservationBonus (0.10)")
    func successfulObservationIncrementsTrustScore() async throws {
        let seed = makeProfile(mode: "shadow", trustScore: 0.30, observations: 1)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.70)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        expectScore(profile?.skipTrustScore, equals: 0.40)
    }

    // MARK: - Edge cases

    @Test("False skip signal on nonexistent profile is a no-op")
    func falseSkipSignalOnMissingProfileIsNoOp() async throws {
        let (sut, store) = try await makeSUT()

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile == nil)
    }

    @Test("Decay on nonexistent profile is a no-op")
    func decayOnMissingProfileIsNoOp() async throws {
        let (sut, store) = try await makeSUT()

        await sut.decayFalseSignals(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile == nil)
    }

    @Test("Shadow mode cannot be demoted further but still records signal")
    func shadowCannotBeDemoted() async throws {
        let seed = makeProfile(mode: "shadow", trustScore: 0.30, observations: 2, falseSignals: 5)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.shadow.rawValue)
        // Verify the signal was still recorded even though mode didn't change
        #expect(profile?.recentFalseSkipSignals == 6)
        expectScore(profile?.skipTrustScore, equals: 0.20)
    }

    @Test("Auto mode cannot be promoted further but still increments score")
    func autoCannotBePromoted() async throws {
        let seed = makeProfile(mode: "auto", trustScore: 0.90, observations: 20)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.99)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.auto.rawValue)
        // Verify score was still incremented and observation counted
        expectScore(profile?.skipTrustScore, equals: 1.0)
        #expect(profile?.observationCount == 21)
    }

    @Test("implicitFalsePositiveCount increments with each false signal")
    func implicitFalsePositiveCountIncrements() async throws {
        let seed = makeProfile(mode: "manual", trustScore: 0.60, observations: 5, falseSignals: 0, falsePosCount: 3)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.implicitFalsePositiveCount == 4)
    }

    // MARK: - Full lifecycle

    @Test("Full lifecycle: shadow -> manual -> auto -> manual demotion")
    func fullLifecycle() async throws {
        let (sut, store) = try await makeSUT()

        // First observation creates shadow profile
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.70)
        var profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.shadow.rawValue)

        // Build up observations to promote shadow -> manual (need 3 obs, score >= 0.4)
        // After obs 1: score=0.2, obs=1
        // After obs 2: score=0.3, obs=2
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.70)
        // After obs 3: score=0.4, obs=3 -> promote!
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.70)
        profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)

        // Build up to auto: need 8 obs, score >= 0.75, 0 false signals
        // obs 4: 0.5, obs 5: 0.6, obs 6: 0.7, obs 7: 0.8 (>= 0.75 but only 7 obs)
        for _ in 4...7 {
            await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.90)
        }
        profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue) // not yet, only 7 obs

        // obs 8: 0.9, obs=8 -> promote!
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.90)
        profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.auto.rawValue)

        // Demotion: 2 false signals -> auto -> manual
        await sut.recordFalseSkipSignal(podcastId: testPodcastId)
        await sut.recordFalseSkipSignal(podcastId: testPodcastId)
        profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
    }

    // MARK: - Custom config

    @Test("Custom config thresholds are respected")
    func customConfigThresholds() async throws {
        let config = TrustScoringConfig(
            shadowToManualObservations: 1,
            shadowToManualTrustScore: 0.1,
            manualToAutoObservations: 2,
            manualToAutoTrustScore: 0.3,
            autoToManualFalseSignals: 1,
            manualToShadowFalseSignals: 1,
            falseSignalPenalty: 0.25,
            correctObservationBonus: 0.20,
            exceptionalFirstEpisodeConfidence: 0.99
        )
        let seed = makeProfile(mode: "shadow", trustScore: 0.0, observations: 0)
        let (sut, store) = try await makeSUT(config: config, seedProfile: seed)

        // One observation: 0.0 + 0.20 = 0.20 >= 0.1, obs=1 >= 1 -> manual
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.50)
        var profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)

        // Second observation: 0.40 >= 0.3, obs=2 >= 2 -> auto
        await sut.recordSuccessfulObservation(podcastId: testPodcastId, averageConfidence: 0.50)
        profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.auto.rawValue)

        // One false signal -> demote to manual (threshold = 1)
        await sut.recordFalseSkipSignal(podcastId: testPodcastId)
        profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue)
        // 0.40 - 0.25 = 0.15
        expectScore(profile?.skipTrustScore, equals: 0.15)

        // One more false signal -> demote to shadow (threshold = 1)
        await sut.recordFalseSkipSignal(podcastId: testPodcastId)
        profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.shadow.rawValue)
    }
}
