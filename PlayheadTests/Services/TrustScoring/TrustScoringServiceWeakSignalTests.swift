// TrustScoringServiceWeakSignalTests.swift
// playhead-q45f: pin the new `recordWeakFalseSkipSignal` API. Listen-rewind
// is a weaker false-positive signal than an explicit "Not an ad" tap, so it
// must keep its 0.05 magnitude (vs the 0.10 of `recordFalseSkipSignal`) but
// still run the demotion state machine — the q45f defect was that the
// pre-q45f path bypassed the state machine entirely, so multiple
// listen-rewinds accumulated `recentFalseSkipSignals` without ever
// triggering a mode transition.

import Foundation
import Testing
@testable import Playhead

private let testPodcastId = "trust-test-weak-podcast"
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
    podcastId: String = testPodcastId,
    mode: String = SkipMode.auto.rawValue,
    trustScore: Double = 0.90,
    observations: Int = 20,
    falseSignals: Int = 0,
    falsePosCount: Int = 0,
    traitProfileJSON: String? = nil,
    title: String? = nil,
    adDurationStatsJSON: String? = nil,
    networkId: String? = nil
) -> PodcastProfile {
    PodcastProfile(
        podcastId: podcastId,
        sponsorLexicon: nil,
        normalizedAdSlotPriors: nil,
        repeatedCTAFragments: nil,
        jingleFingerprints: nil,
        implicitFalsePositiveCount: falsePosCount,
        skipTrustScore: trustScore,
        observationCount: observations,
        mode: mode,
        recentFalseSkipSignals: falseSignals,
        traitProfileJSON: traitProfileJSON,
        title: title,
        adDurationStatsJSON: adDurationStatsJSON,
        networkId: networkId
    )
}

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

@Suite("TrustScoringService.recordWeakFalseSkipSignal (q45f)", .serialized)
struct TrustScoringServiceWeakSignalTests {

    // MARK: - Magnitude

    @Test("decrements skipTrustScore by weakFalseSignalPenalty (default 0.05)")
    func decrementsByDefaultWeakPenalty() async throws {
        let seed = makeProfile(mode: SkipMode.auto.rawValue, trustScore: 0.90)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordWeakFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        expectScore(profile?.skipTrustScore, equals: 0.85)
        #expect(profile?.recentFalseSkipSignals == 1)
        #expect(profile?.implicitFalsePositiveCount == 1)
        #expect(profile?.mode == SkipMode.auto.rawValue,
                "single signal must not demote (autoToManualFalseSignals=2)")
    }

    @Test("respects custom TrustScoringConfig.weakFalseSignalPenalty")
    func customConfigChangesMagnitude() async throws {
        let custom = TrustScoringConfig(
            shadowToManualObservations: 3,
            shadowToManualTrustScore: 0.4,
            manualToAutoObservations: 8,
            manualToAutoTrustScore: 0.75,
            autoToManualFalseSignals: 2,
            manualToShadowFalseSignals: 4,
            falseSignalPenalty: 0.10,
            correctObservationBonus: 0.10,
            exceptionalFirstEpisodeConfidence: 0.92,
            weakFalseSignalPenalty: 0.20
        )
        let seed = makeProfile(mode: SkipMode.auto.rawValue, trustScore: 0.80)
        let (sut, store) = try await makeSUT(config: custom, seedProfile: seed)

        await sut.recordWeakFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        expectScore(profile?.skipTrustScore, equals: 0.60)
    }

    @Test("floors skipTrustScore at 0 (no negative trust)")
    func floorsAtZero() async throws {
        let seed = makeProfile(mode: SkipMode.shadow.rawValue, trustScore: 0.03)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordWeakFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        expectScore(profile?.skipTrustScore, equals: 0.0)
    }

    // MARK: - Demotion (the q45f defect)

    @Test("demotes auto -> manual after autoToManualFalseSignals (=2) calls")
    func demotionAutoToManual() async throws {
        let seed = makeProfile(mode: SkipMode.auto.rawValue, trustScore: 0.90, falseSignals: 1)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordWeakFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.manual.rawValue,
                "two listen-rewinds in a row must demote auto -> manual (q45f defect closure)")
        #expect(profile?.recentFalseSkipSignals == 2)
    }

    @Test("demotes manual -> shadow after manualToShadowFalseSignals (=4) calls")
    func demotionManualToShadow() async throws {
        let seed = makeProfile(mode: SkipMode.manual.rawValue, trustScore: 0.50, falseSignals: 3)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordWeakFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.shadow.rawValue)
        #expect(profile?.recentFalseSkipSignals == 4)
    }

    @Test("no demotion at autoToManualFalseSignals - 1")
    func noDemotionBeforeThreshold() async throws {
        let seed = makeProfile(mode: SkipMode.auto.rawValue, trustScore: 0.90, falseSignals: 0)
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordWeakFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.mode == SkipMode.auto.rawValue)
        #expect(profile?.recentFalseSkipSignals == 1)
    }

    // MARK: - Missing profile

    @Test("no-op when profile is missing (no lazy create)")
    func noOpsWhenProfileMissing() async throws {
        let (sut, store) = try await makeSUT()

        await sut.recordWeakFalseSkipSignal(podcastId: "nonexistent-podcast")

        let profile = try await store.fetchProfile(podcastId: "nonexistent-podcast")
        #expect(profile == nil,
                "missing-profile path must not lazy-create — matches recordFalseSkipSignal precedent")
    }

    // MARK: - Carry-forward

    @Test("carries forward traitProfileJSON, title, adDurationStatsJSON, networkId")
    func carriesForwardOptionalFields() async throws {
        let seed = makeProfile(
            mode: SkipMode.auto.rawValue,
            trustScore: 0.90,
            traitProfileJSON: #"{"trait":"sample"}"#,
            title: "Sample Podcast",
            adDurationStatsJSON: #"{"avgMs":30000}"#,
            networkId: "net-abc"
        )
        let (sut, store) = try await makeSUT(seedProfile: seed)

        await sut.recordWeakFalseSkipSignal(podcastId: testPodcastId)

        let profile = try await store.fetchProfile(podcastId: testPodcastId)
        #expect(profile?.traitProfileJSON == #"{"trait":"sample"}"#)
        #expect(profile?.title == "Sample Podcast")
        #expect(profile?.adDurationStatsJSON == #"{"avgMs":30000}"#)
        #expect(profile?.networkId == "net-abc")
    }

    // MARK: - Magnitude policy invariant

    @Test("weak signal is exactly half the magnitude of recordFalseSkipSignal at default config")
    func weakIsHalfOfStrong() async throws {
        let podcastA = "weak-vs-strong-A"
        let podcastB = "weak-vs-strong-B"
        let initialTrust = 0.50

        let storeA = try await makeTestStore()
        try await storeA.upsertProfile(PodcastProfile(
            podcastId: podcastA, sponsorLexicon: nil, normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil, jingleFingerprints: nil,
            implicitFalsePositiveCount: 0, skipTrustScore: initialTrust,
            observationCount: 5, mode: SkipMode.manual.rawValue,
            recentFalseSkipSignals: 0
        ))
        let svcA = TrustScoringService(store: storeA)
        await svcA.recordWeakFalseSkipSignal(podcastId: podcastA)
        let profileA = try await storeA.fetchProfile(podcastId: podcastA)

        let storeB = try await makeTestStore()
        try await storeB.upsertProfile(PodcastProfile(
            podcastId: podcastB, sponsorLexicon: nil, normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil, jingleFingerprints: nil,
            implicitFalsePositiveCount: 0, skipTrustScore: initialTrust,
            observationCount: 5, mode: SkipMode.manual.rawValue,
            recentFalseSkipSignals: 0
        ))
        let svcB = TrustScoringService(store: storeB)
        await svcB.recordFalseSkipSignal(podcastId: podcastB)
        let profileB = try await storeB.fetchProfile(podcastId: podcastB)

        guard let weakScore = profileA?.skipTrustScore,
              let strongScore = profileB?.skipTrustScore else {
            Issue.record("Profiles must exist after recording signals")
            return
        }
        let weakDelta = initialTrust - weakScore
        let strongDelta = initialTrust - strongScore
        #expect(abs(weakDelta - strongDelta / 2) < scoreTolerance,
                "weak signal must be exactly half of strong: weakΔ=\(weakDelta), strongΔ=\(strongDelta)")
    }
}
