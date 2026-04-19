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
        // The `invariant_violation` payload key MUST NOT appear when nil
        // (encodeIfPresent). Match on the KEY specifically — a colon follows
        // a JSON key, but the same string appears as a VALUE of event_type
        // after playhead-o45p so we disambiguate via the trailing ":".
        #expect(!line.contains("\"invariant_violation\":"))
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

    // MARK: - Schema round-trip (playhead-o45p)

    @Test("Pre-o45p JSON lines (no event_type) decode as invariant_violation entries")
    func legacyJsonLinesDecodeAsInvariantViolation() throws {
        // Simulate a JSON Lines line produced by the pre-o45p logger. The
        // line has no `event_type` key — after o45p we default to
        // `.invariantViolation` so e2a3's audit tooling keeps working
        // against historical session files.
        let legacyJson = """
        {
          "timestamp": "2023-11-14T22:13:20Z",
          "session_id": "3573D414-061A-43A1-8984-6CE1B4B85794",
          "episode_id_hash": "abc123",
          "prior_disposition": "queued",
          "new_disposition": "paused",
          "prior_reason": "waiting_for_time",
          "new_reason": "phone_is_hot",
          "cause": "thermal"
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let entry = try decoder.decode(
            SurfaceStateTransitionEntry.self,
            from: Data(legacyJson.utf8)
        )
        #expect(entry.eventType == .invariantViolation)
        #expect(entry.entryTrigger == nil)
        #expect(entry.windowStartMs == nil)
        #expect(entry.windowEndMs == nil)
    }

    @Test("Unknown event_type values decode as invariant_violation (forward-compat)")
    func unknownEventTypeDecodesAsInvariantViolation() throws {
        // A future logger might emit an event_type this build does not
        // recognize. The decoder must fall back to .invariantViolation so
        // pre-existing aggregation tools do not throw.
        let futureJson = """
        {
          "timestamp": "2023-11-14T22:13:20Z",
          "session_id": "3573D414-061A-43A1-8984-6CE1B4B85794",
          "new_disposition": "queued",
          "new_reason": "waiting_for_time",
          "event_type": "some_future_event_that_did_not_exist_yet"
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let entry = try decoder.decode(
            SurfaceStateTransitionEntry.self,
            from: Data(futureJson.utf8)
        )
        #expect(entry.eventType == .invariantViolation)
    }

    @Test("A readyEntered entry round-trips with its entry_trigger preserved")
    func readyEnteredRoundTrips() throws {
        let entry = SurfaceStateTransitionEntry(
            timestamp: Date(timeIntervalSince1970: 1_700_000_000),
            sessionId: UUID(uuidString: "3573D414-061A-43A1-8984-6CE1B4B85794")!,
            episodeIdHash: "episode-abc",
            priorDisposition: nil,
            newDisposition: .queued,
            priorReason: nil,
            newReason: .waitingForTime,
            cause: nil,
            eligibilitySnapshot: nil,
            invariantViolation: nil,
            eventType: .readyEntered,
            entryTrigger: .analysisCompleted,
            windowStartMs: nil,
            windowEndMs: nil
        )

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(entry)
        let line = String(decoding: data, as: UTF8.self)
        #expect(line.contains("\"event_type\":\"ready_entered\""))
        #expect(line.contains("\"entry_trigger\":\"analysis_completed\""))

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let roundTripped = try decoder.decode(SurfaceStateTransitionEntry.self, from: data)
        #expect(roundTripped == entry)
    }

    @Test("An autoSkipFired entry round-trips with its window bounds preserved")
    func autoSkipFiredRoundTrips() throws {
        let entry = SurfaceStateTransitionEntry(
            timestamp: Date(timeIntervalSince1970: 1_700_000_000),
            sessionId: UUID(uuidString: "3573D414-061A-43A1-8984-6CE1B4B85794")!,
            episodeIdHash: "episode-xyz",
            priorDisposition: nil,
            newDisposition: .queued,
            priorReason: nil,
            newReason: .waitingForTime,
            cause: nil,
            eligibilitySnapshot: nil,
            invariantViolation: nil,
            eventType: .autoSkipFired,
            entryTrigger: nil,
            windowStartMs: 42_000,
            windowEndMs: 47_500
        )

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(entry)
        let line = String(decoding: data, as: UTF8.self)
        #expect(line.contains("\"event_type\":\"auto_skip_fired\""))
        #expect(line.contains("\"window_start_ms\":42000"))
        #expect(line.contains("\"window_end_ms\":47500"))

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let roundTripped = try decoder.decode(SurfaceStateTransitionEntry.self, from: data)
        #expect(roundTripped == entry)
    }

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
