// PipelineProgressStripViewTests.swift
// Pins the static `PipelineProgressStripView.format(_:)` formatter so the
// debug-strip output is contract-locked: nil → "--%", clamped 0...1, rounded
// to whole percent, no view construction needed.
//
// Scope: playhead-btoa.4 (debug pipeline strip on Activity — last bead in the
// btoa epic; see docs/plans/2026-04-27-activity-pipeline-debug-strip-design.md).

import Testing
@testable import Playhead

@Suite("PipelineProgressStripView formatter")
struct PipelineProgressStripViewTests {

    @Test("nil fraction renders as --%")
    func nilRendersDoubleDashPercent() {
        #expect(PipelineProgressStripView.format(nil) == "--%")
    }

    @Test("0.0 renders as 0%")
    func zeroRendersZeroPercent() {
        #expect(PipelineProgressStripView.format(0.0) == "0%")
    }

    @Test("0.876 rounds to 88%")
    func midFractionRoundsToNearestPercent() {
        #expect(PipelineProgressStripView.format(0.876) == "88%")
    }

    @Test("1.0 renders as 100%")
    func oneRendersHundredPercent() {
        #expect(PipelineProgressStripView.format(1.0) == "100%")
    }

    @Test("overflow above 1.0 clamps to 100%")
    func overflowClampsToHundred() {
        #expect(PipelineProgressStripView.format(1.05) == "100%")
    }

    @Test("underflow below 0.0 clamps to 0%")
    func underflowClampsToZero() {
        #expect(PipelineProgressStripView.format(-0.1) == "0%")
    }
}
