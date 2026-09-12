// StrandedScannedAssetPromotionTests.swift
// playhead-rxbm1: the pure decision behind promoting a stranded, fully-scanned
// `queued` asset to `.completeAdScanPartial`.
//
// The device pull showed 11 episodes scanned to 97-99 % whose backfill job had
// failed at the retry cap, still reading `analysisState = queued` — the surface
// renders that as "not analysed", the 74-of-77 failed/couldnt_analyze rows Dan
// hit. Asset state advances only on a session transition, and a backfill job
// dying at the cap drives none, so the coverage never becomes a terminal.
//
// A wrong `.promote` marks an unfinished episode as analysed, silently — the
// worst outcome for the "never lies" promise — so every guard is railed here.

import XCTest
@testable import Playhead

final class StrandedScannedAssetPromotionTests: XCTestCase {

    private func asset(state: SessionState, feature: Double?, duration: Double?) -> AnalysisAsset {
        AnalysisAsset(
            id: "A",
            episodeId: "ep-A",
            assetFingerprint: "fp-A",
            weakFingerprint: nil,
            sourceURL: "file:///A.m4a",
            featureCoverageEndTime: feature,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: state.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: duration
        )
    }

    private func verdict(
        state: SessionState = .queued,
        transcriptEnd: Double = 980,
        feature: Double? = 970,
        duration: Double? = 1000,
        inFlightBackfill: Bool = false,
        session: String? = nil
    ) -> AnalysisCoordinator.StrandedScannedPromotionVerdict {
        AnalysisCoordinator.strandedScannedAssetPromotionVerdict(
            asset: asset(state: state, feature: feature, duration: duration),
            transcriptCoverageEnd: transcriptEnd,
            featureCoverageEnd: feature,
            episodeDuration: duration,
            hasInFlightBackfillJob: inFlightBackfill,
            latestSessionState: session
        )
    }

    private func assertPromoted(
        _ v: AnalysisCoordinator.StrandedScannedPromotionVerdict,
        _ message: String = "",
        file: StaticString = #filePath, line: UInt = #line
    ) {
        guard case let .promote(state, reason) = v else {
            XCTFail("expected a promotion, got .leave. \(message)", file: file, line: line)
            return
        }
        XCTAssertEqual(state, .completeAdScanPartial, "the classifier's terminal for transcript+feature clear, ad scan unmeasured", file: file, line: line)
        XCTAssertTrue(reason.contains("promoted from queued"), "reason should name the transition: \(reason)", file: file, line: line)
    }

    // The case the bead is about: queued, nothing in flight, both axes clear
    // 0.95, no session → promote to completeAdScanPartial.
    func testAStrandedFullyScannedQueuedAssetIsPromoted() {
        assertPromoted(verdict(), "98%/97%-covered stranded queued asset")
    }

    // A terminal (not in-flight) session does not block promotion — a failed
    // session is exactly how these assets end up stranded.
    func testATerminalSessionDoesNotBlockPromotion() {
        assertPromoted(verdict(session: SessionState.failed.rawValue), "failed session is terminal, not in flight")
    }

    // Exactly at the floor promotes (the guard is `>=`, with 1e-9 slack).
    func testExactlyAtTheFloorPromotes() {
        assertPromoted(verdict(transcriptEnd: 950, feature: 950, duration: 1000), "0.950 / 0.950")
    }

    // MARK: - Every guard leaves the asset alone

    func testTranscriptUnderFloorLeaves() {
        XCTAssertEqual(verdict(transcriptEnd: 900), .leave)   // 0.90 transcript
    }

    func testFeatureUnderFloorLeaves() {
        XCTAssertEqual(verdict(feature: 900), .leave)         // 0.90 feature
    }

    func testJustUnderTheFloorLeaves() {
        XCTAssertEqual(verdict(transcriptEnd: 949, feature: 950, duration: 1000), .leave)
    }

    func testAnInFlightBackfillJobLeaves() {
        // Coverage is fine, but scanning is not finished.
        XCTAssertEqual(verdict(inFlightBackfill: true), .leave)
    }

    func testAnInFlightSessionLeaves() {
        for s in [SessionState.queued, .spooling, .featuresReady, .hotPathReady, .waitingForBackfill, .backfill] {
            XCTAssertEqual(verdict(session: s.rawValue), .leave, "\(s.rawValue) is in flight; must not promote")
        }
    }

    func testANonQueuedAssetLeaves() {
        // A completion terminal is the downgrade reconciler's; a failed terminal
        // is a real failure. Neither is this sweep's to touch.
        for s in [SessionState.completeAdScanPartial, .completeFull, .failed, .backfill] {
            XCTAssertEqual(verdict(state: s), .leave, "asset state \(s.rawValue) is not a promotion candidate")
        }
    }

    func testUnknownOrZeroDurationLeaves() {
        XCTAssertEqual(verdict(duration: nil), .leave)
        XCTAssertEqual(verdict(duration: 0), .leave)
    }
}
