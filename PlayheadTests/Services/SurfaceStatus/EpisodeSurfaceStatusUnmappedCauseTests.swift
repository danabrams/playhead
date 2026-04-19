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

@Suite("Reducer — unmapped cause triggers impossible-state log (playhead-o45p Gap D)", .serialized)
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
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("o45p-gap-d-\(UUID().uuidString)", isDirectory: true)
        try? FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    @Test(".unknown(_) cause triggers an impossible-state log entry")
    func unknownCauseEmitsInvariantViolation() throws {
        let dir = Self.makeTempDirectory()
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        defer { SurfaceStatusInvariantLogger._resetForTesting() }

        // Feed the reducer a `.unknown(raw)` cause — the forward-compat
        // sentinel the Codable path lands in for a schema-evolved cause
        // string. The reducer falls through to the default triple AND
        // must fire the invariant logger.
        let surfaced = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .unknown("future_cause_xyz"),
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )

        // Conservative surface triple per the reducer's default branch.
        #expect(surfaced.disposition == .failed)
        #expect(surfaced.reason == .couldntAnalyze)
        #expect(surfaced.hint == .retry)

        // Drain the logger and verify the session file contains a
        // reducer_internal_bug invariant entry mentioning the raw cause
        // string.
        SurfaceStatusInvariantLogger._flushForTesting()

        let sessionURL = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())
        var entries: [SurfaceStateTransitionEntry] = []
        for _ in 0..<10 {
            let data = try Data(contentsOf: sessionURL)
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            entries = try String(decoding: data, as: UTF8.self)
                .split(separator: "\n", omittingEmptySubsequences: true)
                .map { try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8)) }
            if !entries.isEmpty { break }
            SurfaceStatusInvariantLogger._flushForTesting()
        }

        let violations = entries.filter {
            $0.eventType == .invariantViolation
                && $0.invariantViolation?.code == .reducerInternalBug
                && ($0.invariantViolation?.description.contains("future_cause_xyz") ?? false)
        }
        #expect(violations.count == 1)
    }
}
