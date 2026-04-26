// BGTaskTelemetryLoggerTests.swift
// playhead-shpy: BG-task lifecycle telemetry logger.
//
// Tests pin the JSONL schema for every event type the production code
// emits — `submit`, `start`, `complete`, `expire`, `appPhase` — and
// the cross-row enrichment behavior (auto-fill of `timeSinceSubmitSec`
// from a prior submit row, and `timeInTaskSec` from a prior start row).
// Schema regressions break these tests deterministically; that is the
// whole point of having a logger that exists to be queried offline.

import Foundation
import Testing

@testable import Playhead

@Suite("BGTaskTelemetryLogger — playhead-shpy")
struct BGTaskTelemetryLoggerTests {

    // MARK: - Round-trip per event type

    @Test("submit event round-trips through JSONL with expected fields")
    func submitRoundTrips() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let event = BGTaskTelemetryEvent.submit(
            identifier: "com.playhead.app.analysis.backfill",
            succeeded: true,
            earliestBeginDelaySec: 60,
            scenePhase: "background",
            detail: "test-submit",
            now: Date(timeIntervalSince1970: 1_745_360_000)
        )
        await logger.record(event)
        await logger.flushAndClose()

        let line = try Self.readSingleLine(at: await logger.activeLogURL)
        let decoded = try Self.decode(line)
        #expect(decoded.event == "submit")
        #expect(decoded.identifier == "com.playhead.app.analysis.backfill")
        #expect(decoded.submitSucceeded == true)
        #expect(decoded.earliestBeginDelaySec == 60)
        #expect(decoded.scenePhase == "background")
        #expect(decoded.detail == "test-submit")
        #expect(decoded.schemaVersion == BGTaskTelemetryEvent.currentSchemaVersion)
    }

    @Test("submit failure carries the error description")
    func submitFailureCarriesError() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let event = BGTaskTelemetryEvent.submit(
            identifier: "com.playhead.app.feed-refresh",
            succeeded: false,
            error: "BGTaskScheduler.Error.unavailable",
            scenePhase: "background"
        )
        await logger.record(event)
        await logger.flushAndClose()

        let decoded = try Self.decode(Self.readSingleLine(at: await logger.activeLogURL))
        #expect(decoded.submitSucceeded == false)
        #expect(decoded.submitError == "BGTaskScheduler.Error.unavailable")
    }

    @Test("start event records identifier + taskInstanceID + scenePhase")
    func startRoundTrips() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let event = BGTaskTelemetryEvent.start(
            identifier: "com.playhead.app.analysis.backfill",
            taskInstanceID: "abc123",
            timeSinceSubmitSec: 12.5,
            scenePhase: "background"
        )
        await logger.record(event)
        await logger.flushAndClose()

        let decoded = try Self.decode(Self.readSingleLine(at: await logger.activeLogURL))
        #expect(decoded.event == "start")
        #expect(decoded.taskInstanceID == "abc123")
        #expect(decoded.timeSinceSubmitSec == 12.5)
        #expect(decoded.scenePhase == "background")
    }

    @Test("complete event records success bool + timeInTaskSec")
    func completeRoundTrips() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let event = BGTaskTelemetryEvent.complete(
            identifier: "com.playhead.app.analysis.backfill",
            taskInstanceID: "abc123",
            success: true,
            timeInTaskSec: 42.0,
            scenePhase: "background"
        )
        await logger.record(event)
        await logger.flushAndClose()

        let decoded = try Self.decode(Self.readSingleLine(at: await logger.activeLogURL))
        #expect(decoded.event == "complete")
        #expect(decoded.success == true)
        #expect(decoded.timeInTaskSec == 42.0)
    }

    @Test("expire event records detail + timeInTaskSec")
    func expireRoundTrips() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let event = BGTaskTelemetryEvent.expire(
            identifier: "com.playhead.app.analysis.backfill",
            taskInstanceID: "abc123",
            timeInTaskSec: 30.0,
            scenePhase: "background",
            detail: "backfill-task-expired"
        )
        await logger.record(event)
        await logger.flushAndClose()

        let decoded = try Self.decode(Self.readSingleLine(at: await logger.activeLogURL))
        #expect(decoded.event == "expire")
        #expect(decoded.timeInTaskSec == 30.0)
        #expect(decoded.detail == "backfill-task-expired")
    }

    @Test("appPhase event records from/to phase strings")
    func appPhaseRoundTrips() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let event = BGTaskTelemetryEvent.appPhase(
            from: "active",
            to: "background"
        )
        await logger.record(event)
        await logger.flushAndClose()

        let decoded = try Self.decode(Self.readSingleLine(at: await logger.activeLogURL))
        #expect(decoded.event == "appPhase")
        #expect(decoded.phaseFrom == "active")
        #expect(decoded.phaseTo == "background")
        // appPhase rows carry no identifier — they describe the app, not a task.
        #expect(decoded.identifier == nil)
    }

    // MARK: - Cross-row enrichment

    @Test("start auto-fills timeSinceSubmitSec from preceding submit")
    func startEnrichesFromSubmit() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let submitTime = Date(timeIntervalSince1970: 1_745_360_000)
        let startTime = Date(timeIntervalSince1970: 1_745_360_005) // +5s

        await logger.record(
            .submit(
                identifier: "com.playhead.app.analysis.backfill",
                succeeded: true,
                now: submitTime
            )
        )
        // Caller passes nil for timeSinceSubmitSec — the actor must
        // backfill it from the in-memory submit map.
        await logger.record(
            BGTaskTelemetryEvent(
                ts: startTime,
                event: "start",
                identifier: "com.playhead.app.analysis.backfill",
                taskInstanceID: "instance-1",
                timeSinceSubmitSec: nil,
                scenePhase: "background"
            )
        )
        await logger.flushAndClose()

        let lines = try Self.readAllLines(at: await logger.activeLogURL)
        #expect(lines.count == 2)
        let startDecoded = try Self.decode(lines[1])
        #expect(startDecoded.event == "start")
        #expect(startDecoded.timeSinceSubmitSec == 5.0)
    }

    @Test("complete auto-fills timeInTaskSec from preceding start")
    func completeEnrichesFromStart() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let startTime = Date(timeIntervalSince1970: 1_745_360_000)
        let completeTime = Date(timeIntervalSince1970: 1_745_360_042) // +42s

        await logger.record(
            .start(
                identifier: "com.playhead.app.analysis.backfill",
                taskInstanceID: "instance-1",
                timeSinceSubmitSec: nil,
                scenePhase: "background",
                now: startTime
            )
        )
        await logger.record(
            BGTaskTelemetryEvent(
                ts: completeTime,
                event: "complete",
                identifier: "com.playhead.app.analysis.backfill",
                taskInstanceID: "instance-1",
                scenePhase: "background",
                success: true,
                timeInTaskSec: nil
            )
        )
        await logger.flushAndClose()

        let lines = try Self.readAllLines(at: await logger.activeLogURL)
        let completeDecoded = try Self.decode(lines[1])
        #expect(completeDecoded.event == "complete")
        #expect(completeDecoded.timeInTaskSec == 42.0)
        #expect(completeDecoded.success == true)
    }

    @Test("expire auto-fills timeInTaskSec from preceding start")
    func expireEnrichesFromStart() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(directory: dir)

        let startTime = Date(timeIntervalSince1970: 1_745_360_000)
        let expireTime = Date(timeIntervalSince1970: 1_745_360_030) // +30s

        await logger.record(
            .start(
                identifier: "com.playhead.app.analysis.backfill",
                taskInstanceID: "instance-1",
                timeSinceSubmitSec: nil,
                scenePhase: "background",
                now: startTime
            )
        )
        await logger.record(
            BGTaskTelemetryEvent(
                ts: expireTime,
                event: "expire",
                identifier: "com.playhead.app.analysis.backfill",
                taskInstanceID: "instance-1",
                scenePhase: "background",
                timeInTaskSec: nil,
                detail: "backfill-task-expired"
            )
        )
        await logger.flushAndClose()

        let lines = try Self.readAllLines(at: await logger.activeLogURL)
        let expireDecoded = try Self.decode(lines[1])
        #expect(expireDecoded.event == "expire")
        #expect(expireDecoded.timeInTaskSec == 30.0)
    }

    // MARK: - Rotation

    @Test("rotation fires when active file crosses the threshold")
    func rotationFires() async throws {
        let dir = try makeTempDir(prefix: "BGTaskTelemetry")
        let logger = try BGTaskTelemetryLogger(
            directory: dir,
            rotationThresholdBytes: 256
        )

        let bigDetail = String(repeating: "x", count: 200)
        for i in 0..<6 {
            await logger.record(
                .submit(
                    identifier: "com.playhead.app.analysis.backfill",
                    succeeded: true,
                    detail: "\(bigDetail)-\(i)"
                )
            )
        }
        await logger.flushAndClose()

        let rotated = await logger.rotatedLogURLs()
        #expect(rotated.count >= 1, "Expected at least one rotated log after 6 oversized records")
    }

    // MARK: - NoOp logger

    @Test("NoOp logger does not throw and writes nothing")
    func noOpLoggerIsInert() async {
        let logger = NoOpBGTaskTelemetryLogger()
        await logger.record(
            .submit(
                identifier: "com.playhead.app.analysis.backfill",
                succeeded: true
            )
        )
        // No assertions: success is the absence of file I/O and crash.
    }

    // MARK: - bgTaskInstanceID helper

    @Test("bgTaskInstanceID is stable per instance")
    func bgTaskInstanceIDIsStable() {
        final class Ref {}
        let ref = Ref()
        let a = bgTaskInstanceID(for: ref)
        let b = bgTaskInstanceID(for: ref)
        #expect(a == b)

        let other = Ref()
        #expect(bgTaskInstanceID(for: other) != a, "Distinct instances must hash to distinct IDs")
    }

    // MARK: - Helpers

    private static func readSingleLine(at url: URL) throws -> String {
        let lines = try readAllLines(at: url)
        guard lines.count == 1 else {
            throw NSError(
                domain: "BGTaskTelemetryLoggerTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "expected 1 line, got \(lines.count)"]
            )
        }
        return lines[0]
    }

    private static func readAllLines(at url: URL) throws -> [String] {
        let data = try Data(contentsOf: url)
        let text = String(decoding: data, as: UTF8.self)
        return text
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map(String.init)
    }

    private static func decode(_ line: String) throws -> BGTaskTelemetryEvent {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(BGTaskTelemetryEvent.self, from: Data(line.utf8))
    }
}
