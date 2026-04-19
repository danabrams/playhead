// SurfaceReasonCopyTemplateTests.swift
// Pins each `SurfaceReason` case to exactly one approved copy template.
// Any drift — a UI surface inventing a different copy string for the
// same reason — must show up as a test failure here.
//
// Scope: playhead-ol05 (Phase 1.5 — contract test matrix item 3).
//
// Layered enforcement:
//   * This test file pins the canonical copy. A future bead changing
//     copy MUST update both the table in `SurfaceReasonCopyTemplates`
//     AND the expected map below — anything else fails the test.
//   * The companion `SurfaceStatusUILintTests` blocks UI files from
//     bypassing the table by referencing low-level taxonomy types
//     directly. The two tests together guarantee the only legitimate
//     copy source is `SurfaceReasonCopyTemplates.template(for:)`.

import Foundation
import Testing

@testable import Playhead

@Suite("SurfaceReasonCopyTemplates — one approved copy per reason (playhead-ol05)")
struct SurfaceReasonCopyTemplateTests {

    /// The pinned, approved copy template for every `SurfaceReason`. If
    /// product wants to change a string, change it HERE and in the
    /// `SurfaceReasonCopyTemplates.template(for:)` switch — the
    /// CaseIterable check below ensures both sides stay in lock-step.
    static let expected: [SurfaceReason: String] = [
        .waitingForTime: "Waiting to analyze",
        .phoneIsHot: "Paused — phone is too hot",
        .powerLimited: "Paused — low battery",
        .waitingForNetwork: "Waiting for network",
        .storageFull: "Storage is full",
        .analysisUnavailable: "Analysis unavailable on this device",
        .resumeInApp: "Open Playhead to resume",
        .cancelled: "Cancelled",
        .couldntAnalyze: "Couldn't analyze",
    ]

    @Test("Every SurfaceReason case has a pinned approved copy template")
    func allCasesHavePinnedCopy() {
        for reason in SurfaceReason.allCases {
            #expect(Self.expected[reason] != nil,
                    "SurfaceReason.\(reason) is missing from the pinned-copy table")
        }
    }

    @Test("Pinned copy table has no extra entries beyond SurfaceReason.allCases")
    func tableHasNoExtras() {
        let allCases = Set(SurfaceReason.allCases)
        let extras = Set(Self.expected.keys).subtracting(allCases)
        #expect(extras.isEmpty,
                "Pinned-copy table has stale entries: \(extras)")
    }

    @Test("Template lookup returns the pinned copy for every case")
    func templateMatchesPinnedCopy() {
        for reason in SurfaceReason.allCases {
            guard let pinned = Self.expected[reason] else {
                Issue.record(Comment(rawValue: "Reason \(reason) missing from pinned table"))
                continue
            }
            let actual = SurfaceReasonCopyTemplates.template(for: reason)
            #expect(actual == pinned,
                    "Reason .\(reason) — expected \"\(pinned)\", got \"\(actual)\"")
        }
    }
}
