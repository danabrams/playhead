// MetricKitDiagnosticProjectorTests.swift
// Behaviour of the MetricKit payload → `StabilityDiagnosticRecord`
// projection.
//
// Scope: playhead-jw63.4 (crash + hang pipeline).
//
// Privacy behaviour is tested separately in
// `StabilityDiagnosticScrubbingTests`; this suite covers "does the
// pipeline actually capture the diagnostic, and capture it correctly".

import Foundation
import Testing

@testable import Playhead

@Suite("MetricKitDiagnosticProjector — payload → record (playhead-jw63.4)")
struct MetricKitDiagnosticProjectorTests {

    private static let receivedAt = Date(timeIntervalSince1970: 1_800_000_000)

    private func project(_ payload: [String: Any]) throws -> [StabilityDiagnosticRecord] {
        let data = try MetricKitPayloadFixture.payloadData(payload)
        return MetricKitDiagnosticProjector.records(
            fromPayloadJSON: data,
            receivedAt: Self.receivedAt
        )
    }

    // MARK: - Capture

    @Test("a crash payload produces one crash record carrying its triage codes")
    func crashCapture() throws {
        let records = try project(
            MetricKitPayloadFixture.payload(crashes: [MetricKitPayloadFixture.crashDiagnostic()])
        )
        #expect(records.count == 1)
        let record = try #require(records.first)
        #expect(record.kind == .crash)
        #expect(record.schemaVersion == StabilityDiagnosticRecord.currentSchemaVersion)
        #expect(record.receivedAt == Self.receivedAt.timeIntervalSince1970)
        #expect(record.signal == 11)
        #expect(record.exceptionType == 1)
        #expect(record.exceptionCode == 0)
        #expect(record.appVersion == "1.0.0")
        #expect(record.appBuildVersion == "42")
        #expect(record.osVersion == "iPhone OS 27.0 (25A123)")
        #expect(record.deviceType == "iPhone17,1")
        #expect(record.platformArchitecture == "arm64e")
        #expect(record.isTestFlight == true)
        // Kind-specific measurements stay nil on a crash. The fixture
        // seeds all three keys with sentinel values (see
        // `MetricKitPayloadFixture.baseMetadata`), so these assertions
        // fail if the projector's per-kind gates are removed — without
        // the seeding they passed on mere key absence.
        #expect(record.hangDurationMs == nil)
        #expect(record.writesCausedMb == nil)
        #expect(record.launchDurationMs == nil)
    }

    @Test("each kind reads only its OWN measurement, even when all three keys are present")
    func measurementsAreGatedByKind() throws {
        let records = try project(
            MetricKitPayloadFixture.payload(
                crashes: [MetricKitPayloadFixture.crashDiagnostic()],
                hangs: [MetricKitPayloadFixture.hangDiagnostic()],
                diskWrites: [MetricKitPayloadFixture.diskWriteDiagnostic()],
                cpuExceptions: [MetricKitPayloadFixture.cpuExceptionDiagnostic()],
                appLaunches: [MetricKitPayloadFixture.appLaunchDiagnostic()]
            )
        )
        #expect(records.count == 5)
        for record in records {
            #expect((record.hangDurationMs != nil) == (record.kind == .hang))
            #expect((record.writesCausedMb != nil) == (record.kind == .diskWriteException))
            #expect((record.launchDurationMs != nil) == (record.kind == .appLaunch))
            // The 9999 sentinels from baseMetadata must never win.
            #expect(record.hangDurationMs != 9_999)
            #expect(record.writesCausedMb != 9_999)
            #expect(record.launchDurationMs != 9_999)
        }
    }

    // MARK: - Kind coverage

    @Test("payloadKeyOrder and payloadKeys agree, and cover every declared kind")
    func kindTablesAreConsistent() {
        #expect(
            Set(MetricKitDiagnosticProjector.payloadKeyOrder)
                == Set(MetricKitDiagnosticProjector.payloadKeys.keys),
            "a key in the lookup table but missing from the ORDER array is silently never projected"
        )
        #expect(
            Set(MetricKitDiagnosticProjector.payloadKeys.values)
                == Set(StabilityDiagnosticKind.allCases),
            "every declared StabilityDiagnosticKind must be reachable from a payload key"
        )
    }

    @Test("a hang payload produces a hang record with the main-thread hang duration in ms")
    func hangCapture() throws {
        let records = try project(
            MetricKitPayloadFixture.payload(hangs: [MetricKitPayloadFixture.hangDiagnostic()])
        )
        let record = try #require(records.first)
        #expect(record.kind == .hang)
        #expect(record.hangDurationMs == 3_300)
        // A hang produces a stack too — that is what makes it
        // actionable rather than just a number.
        #expect(!record.frames.isEmpty)
    }

    @Test("hang duration parses the seconds-unit form as well as milliseconds")
    func hangDurationUnits() throws {
        let records = try project(
            MetricKitPayloadFixture.payload(
                hangs: [MetricKitPayloadFixture.hangDiagnostic(durationText: "2.75 s")]
            )
        )
        #expect(try #require(records.first).hangDurationMs == 2_750)
    }

    @Test("disk-write and app-launch payloads map to their own kinds and measurements")
    func otherKinds() throws {
        let records = try project(
            MetricKitPayloadFixture.payload(
                diskWrites: [MetricKitPayloadFixture.diskWriteDiagnostic()],
                appLaunches: [MetricKitPayloadFixture.appLaunchDiagnostic()]
            )
        )
        #expect(records.count == 2)
        let diskWrite = try #require(records.first { $0.kind == .diskWriteException })
        #expect(diskWrite.writesCausedMb == 1_024)
        let launch = try #require(records.first { $0.kind == .appLaunch })
        #expect(launch.launchDurationMs == 2_500)
    }

    @Test("a mixed payload projects in a deterministic crash → hang → disk → cpu → launch order")
    func deterministicOrder() throws {
        let payload = MetricKitPayloadFixture.payload(
            crashes: [MetricKitPayloadFixture.crashDiagnostic()],
            hangs: [MetricKitPayloadFixture.hangDiagnostic()],
            diskWrites: [MetricKitPayloadFixture.diskWriteDiagnostic()],
            cpuExceptions: [MetricKitPayloadFixture.cpuExceptionDiagnostic()],
            appLaunches: [MetricKitPayloadFixture.appLaunchDiagnostic()]
        )
        let kinds = try project(payload).map(\.kind)
        #expect(kinds == [.crash, .hang, .diskWriteException, .cpuException, .appLaunch])

        // Structural, not statistical. An earlier version of this test
        // looped five times "because Dictionary order varies per
        // process" — but Swift's per-process hash seed is FIXED within a
        // run, so repeating in-process proved nothing and would have
        // made the test pass or fail on the luck of the launch. The
        // ordering guarantee lives in the explicit array, so assert the
        // array.
        #expect(
            MetricKitDiagnosticProjector.payloadKeyOrder == [
                "crashDiagnostics",
                "hangDiagnostics",
                "diskWriteExceptionDiagnostics",
                "cpuExceptionDiagnostics",
                "appLaunchDiagnostics"
            ]
        )
    }

    // MARK: - Call stack

    @Test("frames come from the ATTRIBUTED thread, flattened depth-first with depth stamps")
    func attributedThreadFrames() throws {
        let records = try project(
            MetricKitPayloadFixture.payload(crashes: [MetricKitPayloadFixture.crashDiagnostic()])
        )
        let record = try #require(records.first)
        #expect(record.frames.count == 3)
        #expect(record.frameCount == 3)
        #expect(record.framesTruncated == false)
        #expect(record.frames.map(\.depth) == [0, 1, 2])
        #expect(record.frames.map(\.binaryName) == ["libdyld.dylib", "Playhead", "Playhead"])
        #expect(record.frames.map(\.offsetIntoBinaryTextSegment) == [4_096, 123_456, 789_012])
        // The unattributed thread's frame must not appear — picking the
        // wrong thread yields a stack that looks plausible and explains
        // nothing.
        #expect(!record.frames.contains { $0.binaryName == "notTheCrashingThread" })
    }

    @Test("binaryUUID is normalised to the uppercase canonical form dwarfdump prints")
    func binaryUUIDNormalisation() throws {
        // The fixture SUPPLIES lowercase; the record must CARRY
        // uppercase. When the fixture supplied uppercase this test
        // passed against a pass-through implementation and proved
        // nothing.
        #expect(
            MetricKitPayloadFixture.appBinaryUUIDAsSupplied
                != MetricKitPayloadFixture.appBinaryUUID,
            "vacuous unless the supplied form differs from the expected form"
        )
        let records = try project(
            MetricKitPayloadFixture.payload(crashes: [MetricKitPayloadFixture.crashDiagnostic()])
        )
        let record = try #require(records.first)
        #expect(record.frames.first?.binaryUUID == MetricKitPayloadFixture.systemBinaryUUID)
        #expect(record.frames.last?.binaryUUID == MetricKitPayloadFixture.appBinaryUUID)
    }

    @Test("a frame with a malformed binaryUUID keeps the frame but drops the UUID")
    func malformedBinaryUUIDIsDropped() throws {
        var diagnostic = MetricKitPayloadFixture.crashDiagnostic()
        diagnostic["callStackTree"] = MetricKitPayloadFixture.branchingCallStackTree()
        let record = try #require(
            try project(MetricKitPayloadFixture.payload(crashes: [diagnostic])).first
        )
        let root = try #require(record.frames.first)
        #expect(root.binaryName == "root")
        #expect(
            root.binaryUUID == nil,
            "a non-UUID string must be dropped, never passed through as free text"
        )
        // The frame is still useful: the offset survives.
        #expect(root.offsetIntoBinaryTextSegment == 1)
    }

    @Test("a branching stack is flattened in pre-order: root, childA, childA1, childB")
    func branchingStackPreOrder() throws {
        var diagnostic = MetricKitPayloadFixture.crashDiagnostic()
        diagnostic["callStackTree"] = MetricKitPayloadFixture.branchingCallStackTree()
        let record = try #require(
            try project(MetricKitPayloadFixture.payload(crashes: [diagnostic])).first
        )
        // A linear chain cannot tell correct child order from inverted;
        // this is the only fixture with a node that has two children.
        #expect(record.frames.map(\.binaryName) == ["root", "childA", "childA1", "childB"])
        #expect(record.frames.map(\.depth) == [0, 1, 2, 1])
        #expect(record.frames.map(\.offsetIntoBinaryTextSegment) == [1, 10, 11, 20])
    }

    @Test("with no thread flagged attributed, the first stack is used rather than dropping the record")
    func fallsBackToFirstStack() throws {
        var diagnostic = MetricKitPayloadFixture.crashDiagnostic()
        diagnostic["callStackTree"] = MetricKitPayloadFixture.callStackTree(attributed: false)
        let records = try project(MetricKitPayloadFixture.payload(crashes: [diagnostic]))
        let record = try #require(records.first)
        #expect(record.frames.first?.binaryName == "notTheCrashingThread")
    }

    @Test("frames are capped and the record says so, while frame_count keeps the true depth")
    func frameTruncation() throws {
        var diagnostic = MetricKitPayloadFixture.crashDiagnostic()
        // 40 levels: deep enough to exercise the cap, shallow enough
        // that JSONSerialization's recursive writer stays inside Swift
        // Testing's small per-test task stack.
        diagnostic["callStackTree"] = MetricKitPayloadFixture.deepCallStackTree(depth: 40)
        let data = try MetricKitPayloadFixture.payloadData(
            MetricKitPayloadFixture.payload(crashes: [diagnostic])
        )
        let records = MetricKitDiagnosticProjector.records(
            fromPayloadJSON: data,
            receivedAt: Self.receivedAt,
            maxFrames: 8
        )
        let record = try #require(records.first)
        #expect(record.frames.count == 8)
        #expect(record.frameCount == 40, "frame_count must report the TRUE depth, not the capped one")
        #expect(record.framesTruncated)
    }

    @Test("frame_count saturates at the internal walk bound rather than walking forever")
    func frameCountSaturatesAtWalkBound() throws {
        var diagnostic = MetricKitPayloadFixture.crashDiagnostic()
        // 200 levels, but handed in as an OBJECT — the JSON entry point
        // would recurse 200 deep inside JSONSerialization's writer and
        // overflow Swift Testing's task stack.
        diagnostic["callStackTree"] = MetricKitPayloadFixture.deepCallStackTree(depth: 200)
        let records = MetricKitDiagnosticProjector.records(
            fromPayloadObject: MetricKitPayloadFixture.payload(crashes: [diagnostic]),
            receivedAt: Self.receivedAt,
            maxFrames: 8
        )
        let record = try #require(records.first)
        #expect(record.frames.count == 8)
        // Walk bound is maxFrames * 8. The tree is deeper than that, so
        // frame_count reports the bound, not 200 — which is exactly what
        // the record's doc comment now says it does.
        #expect(record.frameCount == 64)
        #expect(record.framesTruncated)
    }

    @Test("an absurd frame cap does not trap the walk-limit arithmetic")
    func absurdFrameCapDoesNotTrap() throws {
        // The walk limit is derived from `maxFrames`; computing it as a
        // plain `maxFrames * 8` traps on overflow, and a trap inside the
        // crash reporter is the one failure this subsystem exists to
        // avoid. Before the saturating guard this test crashed the host.
        let data = try MetricKitPayloadFixture.payloadData(
            MetricKitPayloadFixture.payload(crashes: [MetricKitPayloadFixture.crashDiagnostic()])
        )
        let records = MetricKitDiagnosticProjector.records(
            fromPayloadJSON: data,
            receivedAt: Self.receivedAt,
            maxFrames: .max
        )
        #expect(records.first?.frames.count == 3)
        #expect(records.first?.framesTruncated == false)
    }

    @Test("a diagnostic with no call stack still produces a record")
    func missingCallStackStillCaptured() throws {
        var diagnostic = MetricKitPayloadFixture.crashDiagnostic()
        diagnostic.removeValue(forKey: "callStackTree")
        let record = try #require(
            try project(MetricKitPayloadFixture.payload(crashes: [diagnostic])).first
        )
        #expect(record.kind == .crash)
        #expect(record.frames.isEmpty)
        #expect(record.frameCount == 0)
        #expect(record.framesTruncated == false)
        // Losing the stack must not lose the triage codes.
        #expect(record.signal == 11)
    }

    // MARK: - Robustness

    @Test("malformed input yields no records rather than throwing")
    func malformedInputIsInert() {
        let garbage = Data("not json at all".utf8)
        #expect(MetricKitDiagnosticProjector.records(
            fromPayloadJSON: garbage, receivedAt: Self.receivedAt
        ).isEmpty)

        let jsonArray = Data("[1, 2, 3]".utf8)
        #expect(MetricKitDiagnosticProjector.records(
            fromPayloadJSON: jsonArray, receivedAt: Self.receivedAt
        ).isEmpty)

        let emptyPayload = Data("{}".utf8)
        #expect(MetricKitDiagnosticProjector.records(
            fromPayloadJSON: emptyPayload, receivedAt: Self.receivedAt
        ).isEmpty)
    }

    @Test("a diagnostic entry that is not an object is skipped, not fatal")
    func nonObjectEntrySkipped() throws {
        let payload: [String: Any] = [
            "crashDiagnostics": ["a string, not a diagnostic", MetricKitPayloadFixture.crashDiagnostic()]
        ]
        let records = try project(payload)
        #expect(records.count == 1)
        #expect(records.first?.kind == .crash)
    }

    @Test("a payload batch is capped at maxRecordsPerPayload")
    func payloadRecordCap() throws {
        let many = Array(
            repeating: MetricKitPayloadFixture.crashDiagnostic(),
            count: MetricKitDiagnosticProjector.maxRecordsPerPayload + 10
        )
        let records = try project(MetricKitPayloadFixture.payload(crashes: many))
        #expect(records.count == MetricKitDiagnosticProjector.maxRecordsPerPayload)
    }

    @Test("unknown top-level payload arrays are ignored entirely")
    func unknownArraysIgnored() throws {
        var payload = MetricKitPayloadFixture.payload(
            crashes: [MetricKitPayloadFixture.crashDiagnostic()]
        )
        payload["someFutureDiagnostics"] = [MetricKitPayloadFixture.crashDiagnostic()]
        let records = try project(payload)
        #expect(records.count == 1, "a future MetricKit array must not be read by an allowlist reader")
    }
}
