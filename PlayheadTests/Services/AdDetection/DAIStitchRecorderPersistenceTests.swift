import Foundation
import Testing

@testable import Playhead

// playhead-xsdz.71 (Signal 1): persistence / integration tests for the
// DAI-stitch recorder → `podcast_profiles` → read-accessor round trip. Proves
// the recorded DAI-EXPECTED prior + stitch network persist, read back, gate
// once-per-show, and survive a normal profile rebuild — all consistent with the
// existing `upsertProfile` path.
@Suite("DAIStitchRecorderPersistence", .serialized)
struct DAIStitchRecorderPersistenceTests {

    private static let themoveChain = [
        "pscrb.fm", "dts.podtrac.com", "mgln.ai", "clrtpod.com", "traffic.libsyn.com",
    ]

    /// Minimal base profile row (cold-start defaults, DAI-stitch unset).
    private func baseProfile(_ podcastId: String) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 1,
            mode: "shadow",
            recentFalseSkipSignals: 0
        )
    }

    // MARK: - Core round trip

    @Test("recording the THEMOVE chain persists megaphone + DAI-expected, readable back")
    func recordPersistsAndReadsBack() async throws {
        let store = try await makeTestStore()
        let podcastId = "pod-themove"
        try await store.upsertProfile(baseProfile(podcastId))

        let recorder = AnalysisStoreDAIStitchRecorder(store: store)
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: Self.themoveChain)

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile?.daiStitchNetwork == DAIStitchNetwork.megaphone.rawValue)
        #expect(profile?.daiExpected == true)

        // Read accessor decodes the two columns into a classification.
        let classification = profile?.daiStitchClassification
        #expect(classification?.stitchNetwork == .megaphone)
        #expect(classification?.daiExpected == true)
    }

    @Test("recording a clean direct-CDN chain persists unknown / not DAI-expected")
    func recordCleanChain() async throws {
        let store = try await makeTestStore()
        let podcastId = "pod-clean"
        try await store.upsertProfile(baseProfile(podcastId))

        let recorder = AnalysisStoreDAIStitchRecorder(store: store)
        await recorder.recordRedirectChain(
            podcastId: podcastId, hopHosts: ["media.example.com", "cdn.cloudprovider.net"]
        )

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile?.daiStitchNetwork == DAIStitchNetwork.unknown.rawValue)
        #expect(profile?.daiExpected == false)
        #expect(profile?.daiStitchClassification?.stitchNetwork == .unknown)
    }

    // MARK: - Once-per-show gate / idempotency

    @Test("re-recording a different chain does not overwrite the first classification")
    func oncePerShowGate() async throws {
        let store = try await makeTestStore()
        let podcastId = "pod-gate"
        try await store.upsertProfile(baseProfile(podcastId))

        let recorder = AnalysisStoreDAIStitchRecorder(store: store)
        // First observation: megaphone.
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: Self.themoveChain)
        // Second observation: adswizz-only — must be ignored (gate on IS NULL).
        await recorder.recordRedirectChain(
            podcastId: podcastId, hopHosts: ["stitcher.adswizz.com"]
        )

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile?.daiStitchNetwork == DAIStitchNetwork.megaphone.rawValue)
        #expect(profile?.daiExpected == true)
    }

    @Test("gate holds even after an unknown classification is recorded first")
    func gateHoldsAfterUnknown() async throws {
        let store = try await makeTestStore()
        let podcastId = "pod-unknown-first"
        try await store.upsertProfile(baseProfile(podcastId))

        let recorder = AnalysisStoreDAIStitchRecorder(store: store)
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: ["cdn.example.com"])
        // A later, richer chain cannot overwrite the recorded unknown.
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: Self.themoveChain)

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile?.daiStitchNetwork == DAIStitchNetwork.unknown.rawValue)
        #expect(profile?.daiExpected == false)
    }

    // MARK: - No-create + empty-input safety

    @Test("recording for a show with no profile row is a benign no-op")
    func noProfileRowIsNoOp() async throws {
        let store = try await makeTestStore()
        let podcastId = "pod-absent"

        let recorder = AnalysisStoreDAIStitchRecorder(store: store)
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: Self.themoveChain)

        // No row was created, and nothing threw.
        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile == nil)
    }

    @Test("empty hop-host list is a no-op (nothing observed)")
    func emptyHostsNoOp() async throws {
        let store = try await makeTestStore()
        let podcastId = "pod-empty"
        try await store.upsertProfile(baseProfile(podcastId))

        let recorder = AnalysisStoreDAIStitchRecorder(store: store)
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: [])

        let profile = try await store.fetchProfile(podcastId: podcastId)
        #expect(profile?.daiStitchNetwork == nil)
        #expect(profile?.daiStitchClassification == nil)
    }

    // MARK: - Consistency with the existing profile path

    @Test("a profile rebuild via upsertProfile preserves the recorded classification")
    func rebuildPreservesClassification() async throws {
        let store = try await makeTestStore()
        let podcastId = "pod-rebuild"
        try await store.upsertProfile(baseProfile(podcastId))

        let recorder = AnalysisStoreDAIStitchRecorder(store: store)
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: Self.themoveChain)

        // Simulate a later trust-scoring / priors rebuild that knows nothing
        // about DAI stitch (daiStitch fields default to nil) and writes the
        // whole row via upsertProfile.
        let rebuilt = PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "acme,globex",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 2,
            skipTrustScore: 0.8,
            observationCount: 5,
            mode: "active",
            recentFalseSkipSignals: 1
        )
        try await store.upsertProfile(rebuilt)

        let profile = try await store.fetchProfile(podcastId: podcastId)
        // Rebuilt columns applied…
        #expect(profile?.observationCount == 5)
        #expect(profile?.skipTrustScore == 0.8)
        #expect(profile?.mode == "active")
        // …and the DAI-stitch classification survived untouched.
        #expect(profile?.daiStitchNetwork == DAIStitchNetwork.megaphone.rawValue)
        #expect(profile?.daiExpected == true)
    }

    @Test("recording is idempotent: same chain twice yields the same persisted value")
    func idempotentSameChain() async throws {
        let store = try await makeTestStore()
        let podcastId = "pod-idempotent"
        try await store.upsertProfile(baseProfile(podcastId))

        let recorder = AnalysisStoreDAIStitchRecorder(store: store)
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: Self.themoveChain)
        let first = try await store.fetchProfile(podcastId: podcastId)
        await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: Self.themoveChain)
        let second = try await store.fetchProfile(podcastId: podcastId)

        #expect(first?.daiStitchNetwork == second?.daiStitchNetwork)
        #expect(first?.daiExpected == second?.daiExpected)
        #expect(second?.daiStitchNetwork == DAIStitchNetwork.megaphone.rawValue)
    }
}
