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

    // MARK: - Documentation parity (M3)
    //
    // Loads docs/slis/phase-0-slis.md from the repo at test time and asserts
    // each threshold string in the markdown matches the constant in
    // SLI.swift exactly. Anchor: this test file's `#filePath` is at
    // `<repo>/PlayheadTests/Services/Observability/SLIThresholdsTests.swift`,
    // so 4 levels up from `#filePath` is the repo root. The doc lives at
    // `<repo>/docs/slis/phase-0-slis.md`.

    private static func phase0SLIsDocURL(filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent() // PlayheadTests/Services/Observability
            .deletingLastPathComponent() // PlayheadTests/Services
            .deletingLastPathComponent() // PlayheadTests
            .deletingLastPathComponent() // <repo>
            .appendingPathComponent("docs", isDirectory: true)
            .appendingPathComponent("slis", isDirectory: true)
            .appendingPathComponent("phase-0-slis.md")
    }

    @Test("phase-0-slis.md exists and is readable from tests")
    func phase0SLIsDocIsReadable() throws {
        let url = Self.phase0SLIsDocURL()
        let contents = try String(contentsOf: url, encoding: .utf8)
        #expect(!contents.isEmpty)
    }

    @Test("phase-0-slis.md threshold rows match SLI.swift constants exactly")
    func phase0SLIsDocMatchesConstants() throws {
        let url = Self.phase0SLIsDocURL()
        let doc = try String(contentsOf: url, encoding: .utf8)

        // Each entry: (substringRequiredInDoc, swiftConstantValueInSeconds, valueAsHumanString)
        // The substrings are deliberately verbatim — if the doc is reworded
        // the test should fail and force a coordinated edit.
        struct Row {
            let docSubstring: String
            let swiftValue: Double
            let label: String
        }

        let rows: [Row] = [
            // time_to_downloaded
            Row(
                docSubstring: "P50 ≤ 15 min",
                swiftValue: TimeToDownloadedThresholds.p50Seconds,
                label: "TimeToDownloadedThresholds.p50Seconds"
            ),
            Row(
                docSubstring: "P90 ≤ 60 min",
                swiftValue: TimeToDownloadedThresholds.p90Seconds,
                label: "TimeToDownloadedThresholds.p90Seconds"
            ),
            // time_to_proximal_skip_ready
            Row(
                docSubstring: "P50 ≤ 45 min",
                swiftValue: TimeToProximalSkipReadyThresholds.p50Seconds,
                label: "TimeToProximalSkipReadyThresholds.p50Seconds"
            ),
            Row(
                docSubstring: "P90 ≤ 4 h",
                swiftValue: TimeToProximalSkipReadyThresholds.p90Seconds,
                label: "TimeToProximalSkipReadyThresholds.p90Seconds"
            ),
            // ready_by_first_play_rate
            Row(
                docSubstring: "≥ 0.85",
                swiftValue: ReadyByFirstPlayRateThresholds.minRate,
                label: "ReadyByFirstPlayRateThresholds.minRate"
            ),
            // false_ready_rate
            Row(
                docSubstring: "dogfood ≤ 0.02",
                swiftValue: FalseReadyRateThresholds.dogfoodMaxRate,
                label: "FalseReadyRateThresholds.dogfoodMaxRate"
            ),
            Row(
                docSubstring: "ship ≤ 0.01",
                swiftValue: FalseReadyRateThresholds.shipMaxRate,
                label: "FalseReadyRateThresholds.shipMaxRate"
            ),
            // unattributed_pause_rate
            Row(
                docSubstring: "harness = 0",
                swiftValue: UnattributedPauseRateThresholds.harnessMaxRate,
                label: "UnattributedPauseRateThresholds.harnessMaxRate"
            ),
            Row(
                docSubstring: "field < 0.005",
                swiftValue: UnattributedPauseRateThresholds.fieldMaxRate,
                label: "UnattributedPauseRateThresholds.fieldMaxRate"
            ),
        ]

        // Expected Swift values (independently encoded so changing only one
        // side fails the test).
        let expectedSwift: [String: Double] = [
            "TimeToDownloadedThresholds.p50Seconds": 15 * 60,
            "TimeToDownloadedThresholds.p90Seconds": 60 * 60,
            "TimeToProximalSkipReadyThresholds.p50Seconds": 45 * 60,
            "TimeToProximalSkipReadyThresholds.p90Seconds": 4 * 60 * 60,
            "ReadyByFirstPlayRateThresholds.minRate": 0.85,
            "FalseReadyRateThresholds.dogfoodMaxRate": 0.02,
            "FalseReadyRateThresholds.shipMaxRate": 0.01,
            "UnattributedPauseRateThresholds.harnessMaxRate": 0.0,
            "UnattributedPauseRateThresholds.fieldMaxRate": 0.005,
        ]

        for row in rows {
            #expect(
                doc.contains(row.docSubstring),
                "phase-0-slis.md is missing the literal substring '\(row.docSubstring)' (anchor for \(row.label))"
            )
            let expected = expectedSwift[row.label]
            #expect(
                expected != nil,
                "test bug: missing expected value for \(row.label)"
            )
            if let expected {
                #expect(
                    row.swiftValue == expected,
                    "\(row.label) drifted: expected \(expected), got \(row.swiftValue) — update the doc + the constant together"
                )
            }
        }
    }

    @Test("phase-0-slis.md does NOT contain warm_resume_hit_rate as an SLI row")
    func phase0SLIsDocOmitsWarmResume() throws {
        let url = Self.phase0SLIsDocURL()
        let doc = try String(contentsOf: url, encoding: .utf8)
        // The doc may *mention* warm_resume_hit_rate in prose to clarify it
        // is intentionally excluded, but it must not appear inside the
        // canonical SLI table row prefix `| \`warm_resume_hit_rate\``.
        #expect(!doc.contains("| `warm_resume_hit_rate`"),
                "warm_resume_hit_rate must not be in the SLI table — it is a secondary KPI")
    }

    @Test("phase-0-slis.md `constrained` definition mentions Low Power Mode")
    func phase0SLIsDocConstrainedMentionsLPM() throws {
        // The Swift `ExecutionConditionClassifier` adds LPM=true as a
        // constrained predicate (H2). Pin the doc against that addition so a
        // future doc-only edit cannot drop the LPM clause without failing CI.
        let url = Self.phase0SLIsDocURL()
        let doc = try String(contentsOf: url, encoding: .utf8)
        // Locate the "constrained =" definition line and assert LPM appears
        // somewhere inside it.
        let lines = doc.split(separator: "\n", omittingEmptySubsequences: false)
        let constrainedLine = lines.first(where: {
            $0.contains("`constrained`") && $0.contains("=")
        })
        #expect(constrainedLine != nil,
                "could not locate the `constrained` definition line in phase-0-slis.md")
        if let constrainedLine {
            // Case-insensitive: a doc edit that lowercases "low power mode"
            // (or uppercases it) should not fail this anchor cryptically.
            // We pin only the *presence* of the LPM clause, not its casing.
            #expect(
                constrainedLine.range(of: "low power mode", options: .caseInsensitive) != nil,
                "phase-0-slis.md `constrained` line is missing the 'Low Power Mode' clause: \(constrainedLine)"
            )
        }
    }
}
