import Foundation
import Testing
@testable import Playhead

/// playhead-zeh0 — a show demoted to shadow by vetoes does not re-earn its
/// banner on self-observation while those vetoes are still outstanding.
///
/// The manual -> auto rung already read the veto counter (and is now closed
/// by playhead-lqcp). The shadow -> manual rung did not, and playhead-mn5e
/// added a caller that raises trust +0.10 per backfill WITHOUT decaying the
/// counter — so four vetoes (-0.40) were undone by four episodes going by.
@Suite("playhead-zeh0: the shadow rung is gated by outstanding vetoes")
struct ShadowRungVetoGateTests {

    private static let podcastId = "podcast-1"

    @Test("four outstanding vetoes hold a show in shadow through four successful observations")
    func outstandingVetoesHoldTheShowInShadow() async throws {
        let trust = try await makeSkipTestTrustService(
            mode: "shadow", trustScore: 0.5, observations: 10, falseSignals: 4
        )
        for _ in 0..<4 {
            await trust.recordSuccessfulObservation(
                podcastId: Self.podcastId, averageConfidence: 0.9, detectors: [.fusion]
            )
        }
        #expect(
            await trust.effectiveMode(podcastId: Self.podcastId) == .shadow,
            "trust is back above the rung's threshold but the listener's four answers have not been paid down"
        )
    }

    @Test("the control: with no outstanding vetoes the same observations promote to manual")
    func noVetoesPromoteToManual() async throws {
        let trust = try await makeSkipTestTrustService(
            mode: "shadow", trustScore: 0.5, observations: 10, falseSignals: 0
        )
        await trust.recordSuccessfulObservation(
            podcastId: Self.podcastId, averageConfidence: 0.9, detectors: [.fusion]
        )
        #expect(await trust.effectiveMode(podcastId: Self.podcastId) == .manual)
    }
}
