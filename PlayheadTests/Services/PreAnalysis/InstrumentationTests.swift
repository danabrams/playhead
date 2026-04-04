// InstrumentationTests.swift
// Verify that PreAnalysisInstrumentation signpost and logger APIs
// can be called without crashing. These are smoke tests -- the actual
// intervals are validated in Instruments during profiling sessions.

import XCTest
import OSLog
@testable import Playhead

final class InstrumentationTests: XCTestCase {

    // MARK: - Signpost Smoke Tests

    func testQueueWaitSignpostDoesNotCrash() {
        let state = PreAnalysisInstrumentation.beginQueueWait(jobId: "test-job-1")
        PreAnalysisInstrumentation.endQueueWait(state)
    }

    func testJobDurationSignpostDoesNotCrash() {
        let state = PreAnalysisInstrumentation.beginJobDuration(jobId: "test-job-2")
        PreAnalysisInstrumentation.endJobDuration(state)
    }

    func testStageSignpostDoesNotCrash() {
        let state = PreAnalysisInstrumentation.beginStage("decode")
        PreAnalysisInstrumentation.endStage(state)
    }

    func testMultipleStageSignpostsDoNotCrash() {
        let stages = ["decode", "features", "transcription", "ad_detection", "cue_materialization"]
        var states: [OSSignpostIntervalState] = []
        for stage in stages {
            states.append(PreAnalysisInstrumentation.beginStage(stage))
        }
        // End in reverse order (LIFO) to simulate nested stages.
        for state in states.reversed() {
            PreAnalysisInstrumentation.endStage(state)
        }
    }

    // MARK: - Diagnostic Logger Smoke Tests

    func testDiagnosticLoggerInitializes() {
        // Verify the logger and signposter are valid instances.
        XCTAssertNotNil(PreAnalysisInstrumentation.signposter)
        XCTAssertNotNil(PreAnalysisInstrumentation.logger)
    }

    func testCueReadinessLoggingDoesNotCrash() {
        PreAnalysisInstrumentation.logCueReadiness(episodeId: "ep-1", hadCues: true)
        PreAnalysisInstrumentation.logCueReadiness(episodeId: "ep-2", hadCues: false)
    }

    func testTimeToFirstCueLoggingDoesNotCrash() {
        PreAnalysisInstrumentation.logTimeToFirstCue(episodeId: "ep-1", seconds: 2.45)
    }

    func testTierCompletionLoggingDoesNotCrash() {
        PreAnalysisInstrumentation.logTierCompletion(tier: "90s", completed: true)
        PreAnalysisInstrumentation.logTierCompletion(tier: "300s", completed: false)
    }

    func testThermalPauseLoggingDoesNotCrash() {
        PreAnalysisInstrumentation.logThermalPause(duration: 15.5)
    }

    func testJobOutcomeLoggingDoesNotCrash() {
        PreAnalysisInstrumentation.logJobOutcome(
            jobId: "test-job-3",
            stopReason: "reachedTarget",
            coverageSec: 90.0
        )
    }
}
