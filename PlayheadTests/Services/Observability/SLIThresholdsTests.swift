// SLIThresholdsTests.swift
// Pins down the numeric values of the defended SLI thresholds. These
// tests are intentionally low-magic: they exist so a careless edit to a
// threshold constant fails CI rather than silently changing the bar we
// ship against.
//
// If a threshold genuinely needs to change, update both the constant and
// the test in the same commit.

import Foundation
import Testing

@testable import Playhead

@Suite("SLI Thresholds")
struct SLIThresholdsTests {

    // MARK: - time_to_downloaded

    @Test("time_to_downloaded P50 is 15 minutes")
    func timeToDownloadedP50Is15Min() {
        #expect(TimeToDownloadedThresholds.p50Seconds == 15 * 60)
    }

    @Test("time_to_downloaded P90 is 60 minutes")
    func timeToDownloadedP90Is60Min() {
        #expect(TimeToDownloadedThresholds.p90Seconds == 60 * 60)
    }

    // MARK: - time_to_proximal_skip_ready

    @Test("time_to_proximal_skip_ready P50 is 45 minutes")
    func timeToProximalSkipReadyP50Is45Min() {
        #expect(TimeToProximalSkipReadyThresholds.p50Seconds == 45 * 60)
    }

    @Test("time_to_proximal_skip_ready P90 is 4 hours")
    func timeToProximalSkipReadyP90Is4Hours() {
        #expect(TimeToProximalSkipReadyThresholds.p90Seconds == 4 * 60 * 60)
    }

    // MARK: - ready_by_first_play_rate

    @Test("ready_by_first_play_rate floor is 0.85")
    func readyByFirstPlayRateFloor() {
        #expect(ReadyByFirstPlayRateThresholds.minRate == 0.85)
    }

    // MARK: - false_ready_rate

    @Test("false_ready_rate dogfood ceiling is 0.02")
    func falseReadyRateDogfoodCeiling() {
        #expect(FalseReadyRateThresholds.dogfoodMaxRate == 0.02)
    }

    @Test("false_ready_rate ship ceiling is 0.01")
    func falseReadyRateShipCeiling() {
        #expect(FalseReadyRateThresholds.shipMaxRate == 0.01)
    }

    @Test("false_ready_rate ship ceiling is stricter than dogfood ceiling")
    func falseReadyRateShipIsStricter() {
        #expect(FalseReadyRateThresholds.shipMaxRate < FalseReadyRateThresholds.dogfoodMaxRate)
    }

    // MARK: - unattributed_pause_rate

    @Test("unattributed_pause_rate harness ceiling is 0 (any unattributed pause fails replay)")
    func unattributedPauseRateHarnessCeiling() {
        #expect(UnattributedPauseRateThresholds.harnessMaxRate == 0.0)
    }

    @Test("unattributed_pause_rate field ceiling is 0.005")
    func unattributedPauseRateFieldCeiling() {
        #expect(UnattributedPauseRateThresholds.fieldMaxRate == 0.005)
    }

    // MARK: - SLI enum coverage

    @Test("SLI enum contains exactly the five canonical SLIs")
    func sliEnumCoverage() {
        let names = Set(SLI.allCases.map { $0.rawValue })
        #expect(names == [
            "time_to_downloaded",
            "time_to_proximal_skip_ready",
            "ready_by_first_play_rate",
            "false_ready_rate",
            "unattributed_pause_rate",
        ])
        // warm_resume_hit_rate is intentionally NOT present: it is a
        // secondary KPI, not an SLI.
        #expect(!names.contains("warm_resume_hit_rate"))
    }

    // MARK: - Units

    @Test("Latency SLIs are reported in durationSeconds")
    func latencyUnits() {
        #expect(SLI.timeToDownloaded.unit == .durationSeconds)
        #expect(SLI.timeToProximalSkipReady.unit == .durationSeconds)
    }

    @Test("Rate SLIs are reported as rate")
    func rateUnits() {
        #expect(SLI.readyByFirstPlayRate.unit == .rate)
        #expect(SLI.falseReadyRate.unit == .rate)
        #expect(SLI.unattributedPauseRate.unit == .rate)
    }
}
