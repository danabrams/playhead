// MetricKitPayloadFixture.swift
// Synthetic `MXDiagnosticPayload.jsonRepresentation()` blobs for the
// crash + hang pipeline tests.
//
// Scope: playhead-jw63.4.
//
// ----- Why a fixture and not the real type -----
//
// `MXCrashDiagnostic`, `MXHangDiagnostic` and friends have no public
// initialiser and are only ever vended by iOS on a real device, so no
// test can construct one. What CAN be constructed is the JSON Apple
// documents those types serialising to — which is exactly the input
// `MetricKitDiagnosticProjector` consumes. Driving the projector with
// this fixture therefore tests the real production code path end to
// end, minus Apple's delivery.
//
// ----- The hostile fixture -----
//
// The `leaky:` variants deliberately stuff the payload's free-text
// fields with sentinels shaped like the things the on-device mandate
// forbids leaving the device: transcript text, an episode title, a feed
// URL, and an absolute container path to an audio file. The privacy
// tests then assert those sentinels do not survive projection. A
// scrubbing claim tested only against benign input is not a test.

import Foundation

enum MetricKitPayloadFixture {

    // MARK: - Sentinels

    /// Ad-adjacent transcript text — the exact class of content the
    /// on-device mandate forbids exporting.
    static let sentinelTranscript =
        "SENTINELTRANSCRIPT and today the show is brought to you by a mattress company"

    /// An episode title, as it would appear if interpolated into an
    /// assertion message.
    static let sentinelEpisodeTitle = "SENTINELEPISODE Diary of a CEO 431"

    /// A feed URL — forbidden because it identifies what the listener
    /// subscribes to.
    static let sentinelFeedURL = "https://sentinelfeed.example.com/rss/podcast.xml"

    /// An absolute container path whose last component embeds an
    /// episode identifier.
    static let sentinelPath =
        "/private/var/mobile/Containers/Data/Application/SENTINELPATH/Library/Caches/ep-431.mp3"

    /// Every sentinel, for sweep assertions.
    static let allSentinels: [String] = [
        sentinelTranscript,
        sentinelEpisodeTitle,
        sentinelFeedURL,
        sentinelPath
    ]

    // MARK: - Frames

    static let appBinaryUUID = "A1B2C3D4-E5F6-4708-9A0B-1C2D3E4F5061"
    static let systemBinaryUUID = "B1B2C3D4-E5F6-4708-9A0B-1C2D3E4F5062"

    /// A three-deep linear chain: root → sub → sub, the shape a real
    /// crash stack has.
    static func frameChain() -> [String: Any] {
        [
            "binaryUUID": systemBinaryUUID,
            "offsetIntoBinaryTextSegment": 4_096,
            "binaryName": "libdyld.dylib",
            "address": 6_442_455_040,
            "sampleCount": 1,
            "subFrames": [
                [
                    "binaryUUID": appBinaryUUID,
                    "offsetIntoBinaryTextSegment": 123_456,
                    "binaryName": "Playhead",
                    "address": 4_303_876_096,
                    "sampleCount": 1,
                    "subFrames": [
                        [
                            "binaryUUID": appBinaryUUID,
                            "offsetIntoBinaryTextSegment": 789_012,
                            "binaryName": "Playhead",
                            "address": 4_303_999_999,
                            "sampleCount": 1
                        ]
                    ]
                ]
            ]
        ]
    }

    /// A call-stack tree with an unattributed thread FIRST, so a test
    /// that passes only because the projector took `first` would fail.
    static func callStackTree(attributed: Bool = true) -> [String: Any] {
        var stacks: [[String: Any]] = [
            [
                "threadAttributed": false,
                "callStackRootFrames": [
                    [
                        "binaryUUID": systemBinaryUUID,
                        "offsetIntoBinaryTextSegment": 999,
                        "binaryName": "notTheCrashingThread",
                        "sampleCount": 1
                    ]
                ]
            ]
        ]
        stacks.append([
            "threadAttributed": attributed,
            "callStackRootFrames": [frameChain()]
        ])
        return ["callStackPerThread": true, "callStacks": stacks]
    }

    /// A synthetic deep chain of `depth` nested frames, for the
    /// truncation test.
    ///
    /// Keep `depth` modest at the call site. `JSONSerialization` writes
    /// nested containers recursively (two C frames per level here: the
    /// `subFrames` array and the frame object), and Swift Testing runs
    /// each test on a task with a small stack — a 200-deep tree
    /// overflowed it and took the whole test host down with
    /// `EXC_BAD_ACCESS / excessive recursion`, which surfaces as EVERY
    /// test in the process failing at once.
    static func deepCallStackTree(depth: Int) -> [String: Any] {
        var node: [String: Any] = [
            "binaryUUID": appBinaryUUID,
            "offsetIntoBinaryTextSegment": depth,
            "binaryName": "Playhead",
            "sampleCount": 1
        ]
        for level in stride(from: depth - 1, through: 1, by: -1) {
            node = [
                "binaryUUID": appBinaryUUID,
                "offsetIntoBinaryTextSegment": level,
                "binaryName": "Playhead",
                "sampleCount": 1,
                "subFrames": [node]
            ]
        }
        return [
            "callStackPerThread": true,
            "callStacks": [
                ["threadAttributed": true, "callStackRootFrames": [node]]
            ]
        ]
    }

    // MARK: - Metadata

    /// Benign metadata common to every diagnostic kind.
    static func baseMetadata() -> [String: Any] {
        [
            "appVersion": "1.0.0",
            "appBuildVersion": "42",
            "osVersion": "iPhone OS 27.0 (25A123)",
            "deviceType": "iPhone17,1",
            "platformArchitecture": "arm64e",
            "isTestFlightApp": true,
            "regionFormat": "US",
            "lowPowerModeEnabled": false
        ]
    }

    /// Metadata seeded with every sentinel, in the four free-text
    /// fields a real payload can carry them in.
    static func leakyMetadataAdditions() -> [String: Any] {
        [
            // The `NSException` reason. `assertionFailure("no span for
            // \(episode.title)")` lands here verbatim.
            "exceptionReason": [
                "composedMessage": "\(sentinelTranscript) — \(sentinelEpisodeTitle)",
                "formatString": "no ad span for %@",
                "arguments": [sentinelEpisodeTitle, sentinelFeedURL],
                "className": "PlayheadAnalysisCoordinator",
                "exceptionName": "NSInternalInconsistencyException",
                "exceptionType": "NSException"
            ],
            // Real devices put the app's container path in here.
            "terminationReason": "Namespace SIGNAL, Code 0x8badf00d \(sentinelPath)",
            // A memory-map dump; embeds absolute paths.
            "virtualMemoryRegionInfo": "0 is not in any region. \(sentinelPath)",
            "bundleIdentifier": "com.playhead.app",
            "pid": 4_242
        ]
    }

    // MARK: - Diagnostics

    static func crashDiagnostic(leaky: Bool = false) -> [String: Any] {
        var metadata = baseMetadata()
        metadata["signal"] = 11
        metadata["exceptionType"] = 1
        metadata["exceptionCode"] = 0
        if leaky {
            metadata.merge(leakyMetadataAdditions()) { _, new in new }
        } else {
            metadata["terminationReason"] = "Namespace SIGNAL, Code 0xb"
        }
        return [
            "version": "1.0.0",
            "diagnosticMetaData": metadata,
            "callStackTree": callStackTree()
        ]
    }

    static func hangDiagnostic(durationText: String = "3300 ms") -> [String: Any] {
        var metadata = baseMetadata()
        metadata["hangDuration"] = durationText
        return [
            "version": "1.0.0",
            "diagnosticMetaData": metadata,
            "callStackTree": callStackTree()
        ]
    }

    static func diskWriteDiagnostic(writesText: String = "1024 MB") -> [String: Any] {
        var metadata = baseMetadata()
        metadata["writesCaused"] = writesText
        return [
            "version": "1.0.0",
            "diagnosticMetaData": metadata,
            "callStackTree": callStackTree()
        ]
    }

    static func appLaunchDiagnostic(durationText: String = "2.5 s") -> [String: Any] {
        var metadata = baseMetadata()
        metadata["launchDuration"] = durationText
        return [
            "version": "1.0.0",
            "diagnosticMetaData": metadata,
            "callStackTree": callStackTree()
        ]
    }

    // MARK: - Payloads

    /// Assemble a payload from per-kind diagnostic arrays.
    static func payload(
        crashes: [[String: Any]] = [],
        hangs: [[String: Any]] = [],
        diskWrites: [[String: Any]] = [],
        cpuExceptions: [[String: Any]] = [],
        appLaunches: [[String: Any]] = []
    ) -> [String: Any] {
        var out: [String: Any] = [
            "timeStampBegin": "2026-07-26 00:00:00",
            "timeStampEnd": "2026-07-27 00:00:00"
        ]
        if !crashes.isEmpty { out["crashDiagnostics"] = crashes }
        if !hangs.isEmpty { out["hangDiagnostics"] = hangs }
        if !diskWrites.isEmpty { out["diskWriteExceptionDiagnostics"] = diskWrites }
        if !cpuExceptions.isEmpty { out["cpuExceptionDiagnostics"] = cpuExceptions }
        if !appLaunches.isEmpty { out["appLaunchDiagnostics"] = appLaunches }
        return out
    }

    /// Serialise a payload object exactly as
    /// `MXDiagnosticPayload.jsonRepresentation()` would hand it over.
    static func payloadData(_ object: [String: Any]) throws -> Data {
        try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    }
}
