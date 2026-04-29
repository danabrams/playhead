// EpisodeSurfaceStatusUnmappedCauseTests.swift
// Gap-D verification: when an `InternalMissCause.unknown(_)` forward-
// compat sentinel reaches the reducer's default path, the reducer must
// emit an impossible-state log entry via the ol05 invariant logger.
//
// Scope: playhead-o45p (false_ready_rate instrumentation — "Gap D"
// rolled into this bead's acceptance criteria).
//
// The reducer's surface output is still a safe conservative triple
// (`.failed` + `.couldntAnalyze` + `.retry`); the log entry exists
// independently so e2a3 can count unmapped-cause occurrences separately
// from overall surface behavior.

import Foundation
import Testing

@testable import Playhead

@Suite("Reducer — unmapped cause triggers impossible-state log (playhead-o45p Gap D)")
struct EpisodeSurfaceStatusUnmappedCauseTests {

    private static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )

    private static let queuedState = AnalysisState(
        persistedStatus: .queued,
        hasUserPreemptedJob: false,
        hasAppForceQuitFlag: false,
        pendingSinceEnqueuedAt: Date(timeIntervalSince1970: 1_700_000_000),
        hasAnyConfirmedAnalysis: false
    )

    private static func makeTempDirectory() -> URL {
        try! makeTempDir(prefix: "o45p-gap-d")
    }

    @Test(".unknown(_) cause triggers an impossible-state log entry")
    func unknownCauseEmitsInvariantViolation() async throws {
        let dir = Self.makeTempDirectory()
        let logger = SurfaceStatusInvariantLogger(directory: dir)

        // Feed the reducer a `.unknown(raw)` cause — the forward-compat
        // sentinel the Codable path lands in for a schema-evolved cause
        // string. The reducer falls through to the default triple AND
        // must fire the invariant logger.
        let surfaced = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .unknown("future_cause_xyz"),
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil,
            invariantLogger: logger
        )

        // Conservative surface triple per the reducer's default branch.
        #expect(surfaced.disposition == .failed)
        #expect(surfaced.reason == .couldntAnalyze)
        #expect(surfaced.hint == .retry)

        // Drain the logger and verify the session file contains a
        // unmapped_forward_compat_cause invariant entry mentioning the raw
        // cause string.
        //
        // playhead-glch: prior to this bead the code on this row was
        // hard-coded to `.reducerInternalBug`; the migration replaced
        // every reducer call-site with a specific code so e2a3's audit
        // can group meaningfully.
        logger.flushForTesting()

        let sessionURL = try #require(logger.currentSessionFileURL)
        var entries: [SurfaceStateTransitionEntry] = []
        for _ in 0..<10 {
            let data = try Data(contentsOf: sessionURL)
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            entries = try String(decoding: data, as: UTF8.self)
                .split(separator: "\n", omittingEmptySubsequences: true)
                .map { try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8)) }
            if !entries.isEmpty { break }
            logger.flushForTesting()
        }

        let violations = entries.filter {
            $0.eventType == .invariantViolation
                && $0.invariantViolation?.code == .unmappedForwardCompatCause
                && ($0.invariantViolation?.description.contains("future_cause_xyz") ?? false)
        }
        #expect(violations.count == 1)
    }
}

// MARK: - playhead-glch regression: per-call-site code mapping
//
// These tests pin the contract for the four reducer-impossibility codes
// introduced in `playhead-glch`. The three precedence-ladder defaults
// (`.userPausedUnknownCause`, `.resourceBlockUnknownCause`,
// `.transientWaitUnknownCause`) are dead code by current construction —
// the inner `switch` exhausts every cause the corresponding classifier
// returns true for, and the `default` arm only fires if a future change
// causes the classifier and the inner `switch` to fall out of sync. These
// tests exercise the logger's code-typed entry-point with each new code
// and verify the JSON Lines emission carries the right `code` value, so a
// future regression that hard-codes `.unknown` (or `.reducerInternalBug`,
// pre-glch) at the synthetic path would flip these to red. The unmapped
// forward-compat cause IS reachable via the public reducer surface and is
// covered by the suite above.
@Suite("playhead-glch — reducer-impossibility codes route through the logger correctly")
struct ReducerImpossibilityCodeMappingTests {

    private static func makeTempDirectory() -> URL {
        try! makeTempDir(prefix: "glch-code-mapping")
    }

    /// Round-trip a single synthetic violation through the logger and
    /// return the decoded `InvariantViolation` payload (or nil when the
    /// session file is empty / the entry has no violation).
    private static func emitAndDecode(
        code: InvariantViolation.Code,
        description: String
    ) throws -> InvariantViolation? {
        let dir = makeTempDirectory()
        let logger = SurfaceStatusInvariantLogger(directory: dir)
        logger.invariantViolated(code: code, description: description)
        logger.flushForTesting()

        let sessionURL = try #require(logger.currentSessionFileURL)
        // Drain with retry, identical to the unmapped-cause suite above.
        var entries: [SurfaceStateTransitionEntry] = []
        for _ in 0..<10 {
            let data = try Data(contentsOf: sessionURL)
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            entries = try String(decoding: data, as: UTF8.self)
                .split(separator: "\n", omittingEmptySubsequences: true)
                .map { try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8)) }
            if !entries.isEmpty { break }
            logger.flushForTesting()
        }
        return entries.first?.invariantViolation
    }

    @Test("Rule 2 (user-paused) impossibility emits .userPausedUnknownCause")
    func userPausedUnknownCauseRoundTrips() throws {
        let violation = try Self.emitAndDecode(
            code: .userPausedUnknownCause,
            description: "user-paused rule matched an unknown cause: <synthetic>"
        )
        let unwrapped = try #require(violation)
        #expect(unwrapped.code == .userPausedUnknownCause)
        #expect(unwrapped.code.rawValue == "user_paused_unknown_cause")
        #expect(unwrapped.description.contains("user-paused"))
    }

    @Test("Rule 3 (resource-block) impossibility emits .resourceBlockUnknownCause")
    func resourceBlockUnknownCauseRoundTrips() throws {
        let violation = try Self.emitAndDecode(
            code: .resourceBlockUnknownCause,
            description: "resource-block rule matched an unknown cause: <synthetic>"
        )
        let unwrapped = try #require(violation)
        #expect(unwrapped.code == .resourceBlockUnknownCause)
        #expect(unwrapped.code.rawValue == "resource_block_unknown_cause")
        #expect(unwrapped.description.contains("resource-block"))
    }

    @Test("Rule 4 (transient-wait) impossibility emits .transientWaitUnknownCause")
    func transientWaitUnknownCauseRoundTrips() throws {
        let violation = try Self.emitAndDecode(
            code: .transientWaitUnknownCause,
            description: "transient-wait rule matched an unknown cause: <synthetic>"
        )
        let unwrapped = try #require(violation)
        #expect(unwrapped.code == .transientWaitUnknownCause)
        #expect(unwrapped.code.rawValue == "transient_wait_unknown_cause")
        #expect(unwrapped.description.contains("transient-wait"))
    }

    @Test("recordUnknown(_:) tags the synthetic violation with .unknown")
    func recordUnknownTagsAsUnknown() throws {
        let dir = Self.makeTempDirectory()
        let logger = SurfaceStatusInvariantLogger(directory: dir)
        logger.recordUnknown("a future site without a real code")
        logger.flushForTesting()

        let sessionURL = try #require(logger.currentSessionFileURL)
        var entries: [SurfaceStateTransitionEntry] = []
        for _ in 0..<10 {
            let data = try Data(contentsOf: sessionURL)
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            entries = try String(decoding: data, as: UTF8.self)
                .split(separator: "\n", omittingEmptySubsequences: true)
                .map { try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8)) }
            if !entries.isEmpty { break }
            logger.flushForTesting()
        }
        let violation = try #require(entries.first?.invariantViolation)
        #expect(violation.code == .unknown)
        #expect(violation.code.rawValue == "unknown")
    }
}
