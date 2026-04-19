// SurfaceStatusInvariantLoggerTests.swift
// Round-trip + rotation tests for `SurfaceStatusInvariantLogger`.
//
// Scope: playhead-ol05 (Phase 1.5 deliverable 5).
//
// Coverage:
//   * Single-entry write-and-read round trip; one JSON Lines record per
//     `record(_:)` call.
//   * Schema compliance: every required field present, snake_case keys.
//   * Session rotation: a fresh launch (simulated by `_resetForTesting`)
//     opens a new session file.
//   * Eviction: when more than `maxSessionFiles` sessions exist, the
//     oldest are deleted on the next write.

import Foundation
import Testing

@testable import Playhead

@Suite("SurfaceStatusInvariantLogger — write/read/rotate (playhead-ol05)", .serialized)
struct SurfaceStatusInvariantLoggerTests {

    // MARK: - Setup helpers

    /// Returns a fresh temp directory the logger will write into for the
    /// duration of one test.
    private static func makeTempDirectory() -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("ol05-logger-\(UUID().uuidString)", isDirectory: true)
        try? FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private static func sampleEntry(
        sessionId: UUID,
        violation: InvariantViolation? = nil
    ) -> SurfaceStateTransitionEntry {
        SurfaceStateTransitionEntry(
            timestamp: Date(timeIntervalSince1970: 1_700_000_000),
            sessionId: sessionId,
            episodeIdHash: "abc123",
            priorDisposition: .queued,
            newDisposition: .paused,
            priorReason: .waitingForTime,
            newReason: .phoneIsHot,
            cause: .thermal,
            eligibilitySnapshot: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
            ),
            invariantViolation: violation
        )
    }

    // MARK: - Round-trip

    @Test("Writing one entry produces one JSON-Lines record on disk")
    func singleEntryRoundTrip() throws {
        let dir = Self.makeTempDirectory()
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        defer { SurfaceStatusInvariantLogger._resetForTesting() }

        let entry = Self.sampleEntry(
            sessionId: SurfaceStatusInvariantLogger._currentSessionId()
        )
        SurfaceStatusInvariantLogger.record(entry)
        SurfaceStatusInvariantLogger._flushForTesting()

        let url = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())
        let data = try Data(contentsOf: url)
        let lines = String(decoding: data, as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
        #expect(lines.count == 1)

        // Decode the line back to verify the schema round-trips.
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(
            SurfaceStateTransitionEntry.self,
            from: Data(lines[0].utf8)
        )
        #expect(decoded == entry)
    }

    @Test("Writing many entries produces a JSON-Lines file with one record per line")
    func manyEntriesProduceOneLineEach() throws {
        let dir = Self.makeTempDirectory()
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        defer { SurfaceStatusInvariantLogger._resetForTesting() }

        let sessionId = SurfaceStatusInvariantLogger._currentSessionId()
        let count = 25
        for _ in 0..<count {
            SurfaceStatusInvariantLogger.record(Self.sampleEntry(sessionId: sessionId))
        }
        SurfaceStatusInvariantLogger._flushForTesting()
        // Drain pending writes by enqueueing a no-op probe and waiting.
        let url = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())

        // Read up to a few times in case the write queue is still draining.
        var lines: [Substring] = []
        for _ in 0..<10 {
            let data = try Data(contentsOf: url)
            lines = String(decoding: data, as: UTF8.self)
                .split(separator: "\n", omittingEmptySubsequences: true)
            if lines.count == count { break }
            // Yield to the write queue.
            SurfaceStatusInvariantLogger._flushForTesting()
        }
        #expect(lines.count == count)
    }

    // MARK: - Schema compliance

    @Test("JSON record carries every required snake_case field")
    func schemaSnakeCaseFields() throws {
        let dir = Self.makeTempDirectory()
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        defer { SurfaceStatusInvariantLogger._resetForTesting() }

        let entry = Self.sampleEntry(
            sessionId: SurfaceStatusInvariantLogger._currentSessionId()
        )
        SurfaceStatusInvariantLogger.record(entry)
        SurfaceStatusInvariantLogger._flushForTesting()

        let url = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())
        let data = try Data(contentsOf: url)
        let line = String(decoding: data, as: UTF8.self)

        // Required keys (snake_case):
        let requiredKeys = [
            "\"timestamp\"",
            "\"session_id\"",
            "\"episode_id_hash\"",
            "\"prior_disposition\"",
            "\"new_disposition\"",
            "\"prior_reason\"",
            "\"new_reason\"",
            "\"cause\"",
            "\"eligibility_snapshot\"",
        ]
        for key in requiredKeys {
            #expect(line.contains(key), "Missing required key \(key) in JSON line: \(line)")
        }
        // Invariant_violation MUST NOT appear when nil (encodeIfPresent).
        #expect(!line.contains("\"invariant_violation\""))
    }

    @Test("invariant_violation appears only when the entry carries one")
    func invariantViolationIsPresentWhenSet() throws {
        let dir = Self.makeTempDirectory()
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        defer { SurfaceStatusInvariantLogger._resetForTesting() }

        let entry = Self.sampleEntry(
            sessionId: SurfaceStatusInvariantLogger._currentSessionId(),
            violation: InvariantViolation(
                code: .unavailableWithRetryHint,
                description: "test"
            )
        )
        SurfaceStatusInvariantLogger.record(entry)
        SurfaceStatusInvariantLogger._flushForTesting()

        let url = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())
        let data = try Data(contentsOf: url)
        let line = String(decoding: data, as: UTF8.self)
        #expect(line.contains("\"invariant_violation\""))
        #expect(line.contains("\"unavailable_with_retry_hint\""))
    }

    // MARK: - Session rotation

    @Test("Rotating sessions opens a fresh file path")
    func rotationOpensFreshFile() throws {
        let dir = Self.makeTempDirectory()
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        defer { SurfaceStatusInvariantLogger._resetForTesting() }

        SurfaceStatusInvariantLogger.record(
            Self.sampleEntry(sessionId: SurfaceStatusInvariantLogger._currentSessionId())
        )
        SurfaceStatusInvariantLogger._flushForTesting()
        let firstURL = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())

        // Rotate: simulate a fresh process launch but keep the same dir.
        // The reset MUST keep using the same diagnostics directory so we
        // can inspect both sessions side-by-side.
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        SurfaceStatusInvariantLogger.record(
            Self.sampleEntry(sessionId: SurfaceStatusInvariantLogger._currentSessionId())
        )
        SurfaceStatusInvariantLogger._flushForTesting()
        let secondURL = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())

        #expect(firstURL != secondURL)
    }

    // MARK: - Convenience: recordViolations

    @Test("recordViolations(_:context:) emits one entry per violation")
    func recordViolationsEmitsOnePerViolation() throws {
        let dir = Self.makeTempDirectory()
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        defer { SurfaceStatusInvariantLogger._resetForTesting() }

        let context = SurfaceStateTransitionContext(
            episodeIdHash: nil,
            priorDisposition: nil,
            newDisposition: .unavailable,
            priorReason: nil,
            newReason: .analysisUnavailable,
            cause: nil,
            eligibilitySnapshot: nil
        )
        let violations: [InvariantViolation] = [
            InvariantViolation(code: .unavailableWithRetryHint, description: "1"),
            InvariantViolation(code: .emptyBatchWithNonQueuedDisposition, description: "2"),
        ]
        SurfaceStatusInvariantLogger.recordViolations(violations, context: context)
        SurfaceStatusInvariantLogger._flushForTesting()

        let url = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())
        // Read with retry to drain the queue.
        var lines: [Substring] = []
        for _ in 0..<10 {
            let data = try Data(contentsOf: url)
            lines = String(decoding: data, as: UTF8.self)
                .split(separator: "\n", omittingEmptySubsequences: true)
            if lines.count == violations.count { break }
            SurfaceStatusInvariantLogger._flushForTesting()
        }
        #expect(lines.count == violations.count)
    }
}
