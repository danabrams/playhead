// BackgroundRuntimeTests.swift
// Tests for background runtime strategy (beginBackgroundTask for T0, BGProcessing for T1+).

import Foundation
import Testing
@testable import Playhead

@Suite("Background Runtime Strategy")
struct BackgroundRuntimeTests {

    @Test("backgroundExpired is a valid StopReason")
    func testBackgroundExpiredStopReason() {
        let outcome = AnalysisOutcome(
            assetId: "test-asset",
            requestedCoverageSec: 90,
            featureCoverageSec: 45,
            transcriptCoverageSec: 30,
            cueCoverageSec: 0,
            newCueCount: 0,
            stopReason: .backgroundExpired
        )
        if case .backgroundExpired = outcome.stopReason {
            // Pass
        } else {
            Issue.record("Expected .backgroundExpired stop reason")
        }
    }

    @Test("BGProcessing identifier is registered")
    func testBGProcessingIdentifierRegistered() {
        #expect(BackgroundTaskID.preAnalysisRecovery == "com.playhead.app.preanalysis.recovery")
    }

    @Test("PreAnalysisConfig default T0 depth is 90s")
    func testConfigDefaultT0Depth() {
        let config = PreAnalysisConfig()
        #expect(config.defaultT0DepthSeconds == 90)
    }
}
