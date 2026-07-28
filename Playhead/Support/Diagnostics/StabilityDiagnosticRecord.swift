// StabilityDiagnosticRecord.swift
// Wire shape for one MetricKit stability diagnostic (crash, hang,
// disk-write exception, CPU exception, or slow app launch) as it is
// persisted on device and later projected into the support-safe
// diagnostics bundle.
//
// Scope: playhead-jw63.4 (crash + hang pipeline).
//
// ----- The closed-shape argument -----
//
// This is a CLOSED Codable type: every field is either a number, a
// boolean, an enum rawValue this repo defines, or a `String` that
// `DiagnosticTextSanitizer` has already validated against a character
// allowlist. There is no `metadata: [String: String]`, no
// `rawPayload: Data`, no free-text `message`, and no `reason` — those
// are precisely the shapes that let arbitrary application strings ride
// out of the device.
//
// The privacy claim that follows from that is checkable rather than
// aspirational: encode any record produced by
// `MetricKitDiagnosticProjector` and every string in the resulting JSON
// tree matches the allowlist. `StabilityDiagnosticScrubbingTests` walks
// the encoded tree and asserts exactly that, so adding a free-text
// field to this struct turns the gate red.
//
// ----- What is deliberately absent -----
//
//   * `exceptionReason.composedMessage` / `formatString` / `arguments` —
//     the interpolated `NSException` reason. This is the single most
//     likely place for an episode title or an ad-candidate string to
//     appear (`assertionFailure("no ad span for \(episode.title)")`).
//     Never read, never stored.
//   * `terminationReason` — free text that on real devices carries the
//     app bundle path and RunningBoard explanations. Decomposed into
//     `termination_namespace` + `termination_code` and otherwise
//     discarded.
//   * `virtualMemoryRegionInfo` — a memory map dump that embeds
//     absolute container paths. Never read.
//   * Frame `address` — absolute, ASLR-dependent, and adds nothing:
//     symbolication runs off `binary_uuid` + `offset_into_binary_text_segment`.
//
// ----- Why the per-record version/build fields -----
//
// MetricKit delivers a payload up to 24 h after the incident, which is
// routinely AFTER the user has taken an app update. The bundle-level
// `app_version` therefore describes the app that EXPORTED the bundle,
// not the app that crashed. Each record carries its own
// `app_version` / `app_build_version` so the correct dSYM can be picked
// (see `scripts/symbolicate-stability-diagnostics.sh`).

import Foundation

// MARK: - Kind

/// Which MetricKit diagnostic stream a record came from. Explicit
/// raw values (rather than a free-form String) so adding a stream is a
/// deliberate edit here and in `MetricKitDiagnosticProjector`.
enum StabilityDiagnosticKind: String, Codable, Sendable, Hashable, CaseIterable {
    /// `MXCrashDiagnostic` — the process died.
    case crash = "crash"
    /// `MXHangDiagnostic` — the main thread was unresponsive long
    /// enough for iOS to sample it. Nothing died; trust did.
    case hang = "hang"
    /// `MXDiskWriteExceptionDiagnostic` — excessive disk writes.
    case diskWriteException = "disk_write_exception"
    /// `MXCPUExceptionDiagnostic` — sustained CPU over budget.
    case cpuException = "cpu_exception"
    /// `MXAppLaunchDiagnostic` — launch exceeded the OS budget.
    case appLaunch = "app_launch"
}

// MARK: - Call-stack frame

/// One frame of the attributed (crashing / hanging) thread's stack,
/// reduced to the three values `atos` needs plus its depth.
struct StabilityCallStackFrame: Codable, Sendable, Equatable {
    /// Mach-O image name (`Playhead`, `libswiftCore.dylib`). `nil` when
    /// the payload's value failed the sanitiser allowlist.
    let binaryName: String?
    /// Canonical uppercase UUID of the image, matching
    /// `dwarfdump --uuid` output. `nil` when the payload's value was
    /// not a well-formed UUID.
    let binaryUUID: String?
    /// Byte offset of the return address from the start of the image's
    /// `__TEXT` segment — the value `atos -l 0` consumes directly.
    let offsetIntoBinaryTextSegment: Int
    /// 0 for the root frame, increasing towards the leaf.
    let depth: Int

    /// `CaseIterable` so `StabilityDiagnosticScrubbingTests` can assert
    /// the encoded key set is exactly the declared one.
    enum CodingKeys: String, CodingKey, CaseIterable {
        case binaryName = "binary_name"
        case binaryUUID = "binary_uuid"
        case offsetIntoBinaryTextSegment = "offset_into_binary_text_segment"
        case depth
    }

    init(
        binaryName: String?,
        binaryUUID: String?,
        offsetIntoBinaryTextSegment: Int,
        depth: Int
    ) {
        self.binaryName = binaryName
        self.binaryUUID = binaryUUID
        self.offsetIntoBinaryTextSegment = offsetIntoBinaryTextSegment
        self.depth = depth
    }

    /// Re-sanitises on decode for the same reason
    /// ``StabilityDiagnosticRecord/init(from:)`` does: the ring buffer
    /// outlives app versions, so the exporter must not trust bytes
    /// written by an older binary.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.binaryName = DiagnosticTextSanitizer.identifier(
            try container.decodeIfPresent(String.self, forKey: .binaryName)
        )
        self.binaryUUID = DiagnosticTextSanitizer.binaryUUID(
            try container.decodeIfPresent(String.self, forKey: .binaryUUID)
        )
        self.offsetIntoBinaryTextSegment = try container.decodeIfPresent(
            Int.self, forKey: .offsetIntoBinaryTextSegment
        ) ?? 0
        self.depth = try container.decodeIfPresent(Int.self, forKey: .depth) ?? 0
    }
}

// MARK: - Record

/// One stability incident, scrubbed and bounded.
struct StabilityDiagnosticRecord: Codable, Sendable, Equatable {

    /// Bumped when a field is added or its meaning changes, so a
    /// support engineer reading an old bundle knows which fields to
    /// expect. Additive changes keep older readers working because the
    /// decoder is `decodeIfPresent`-tolerant.
    static let currentSchemaVersion = 1

    /// Hard ceiling on frames retained per record. Deep enough for the
    /// app frames that matter, shallow enough that fifty records stay a
    /// few hundred kilobytes.
    static let maxFramesPerRecord = 64

    let schemaVersion: Int
    let kind: StabilityDiagnosticKind
    /// When this device INGESTED the payload (epoch seconds). MetricKit
    /// does not expose a per-diagnostic incident timestamp in the
    /// payload JSON — only the payload's own begin/end window — so this
    /// is honestly named "received", not "occurred".
    let receivedAt: Double

    // Provenance of the build that produced the incident.
    let appVersion: String?
    let appBuildVersion: String?
    let osVersion: String?
    let deviceType: String?
    let platformArchitecture: String?
    let isTestFlight: Bool?

    // Crash triage codes.
    let signal: Int?
    let exceptionType: Int?
    let exceptionCode: Int?
    let terminationNamespace: String?
    let terminationCode: String?
    /// `NSException` NAME only (e.g. `NSInvalidArgumentException`) — the
    /// reason string that accompanies it is never read.
    let objcExceptionName: String?
    /// `NSException` raising class name only.
    let objcExceptionClassName: String?

    // Per-kind measurements.
    let hangDurationMs: Int?
    let writesCausedMb: Double?
    let launchDurationMs: Int?

    // Call stack.
    /// Frames observed on the attributed thread BEFORE truncation, so a
    /// support engineer can tell a genuinely shallow stack from a
    /// truncated one.
    ///
    /// Saturates at the projector's internal walk bound (8x the frame
    /// cap, i.e. 512 by default) — a tree deeper than that reports the
    /// bound, not its true depth. `frames_truncated` stays correct
    /// either way.
    let frameCount: Int
    let framesTruncated: Bool
    let frames: [StabilityCallStackFrame]

    /// `CaseIterable` so `StabilityDiagnosticScrubbingTests` can assert
    /// the encoded key set is exactly the declared one — the check that
    /// makes "this type has no free-text field" a gate failure rather
    /// than a code-review hope.
    enum CodingKeys: String, CodingKey, CaseIterable {
        case schemaVersion = "schema_version"
        case kind
        case receivedAt = "received_at"
        case appVersion = "app_version"
        case appBuildVersion = "app_build_version"
        case osVersion = "os_version"
        case deviceType = "device_type"
        case platformArchitecture = "platform_architecture"
        case isTestFlight = "is_test_flight"
        case signal
        case exceptionType = "exception_type"
        case exceptionCode = "exception_code"
        case terminationNamespace = "termination_namespace"
        case terminationCode = "termination_code"
        case objcExceptionName = "objc_exception_name"
        case objcExceptionClassName = "objc_exception_class_name"
        case hangDurationMs = "hang_duration_ms"
        case writesCausedMb = "writes_caused_mb"
        case launchDurationMs = "launch_duration_ms"
        case frameCount = "frame_count"
        case framesTruncated = "frames_truncated"
        case frames
    }

    init(
        schemaVersion: Int = StabilityDiagnosticRecord.currentSchemaVersion,
        kind: StabilityDiagnosticKind,
        receivedAt: Double,
        appVersion: String? = nil,
        appBuildVersion: String? = nil,
        osVersion: String? = nil,
        deviceType: String? = nil,
        platformArchitecture: String? = nil,
        isTestFlight: Bool? = nil,
        signal: Int? = nil,
        exceptionType: Int? = nil,
        exceptionCode: Int? = nil,
        terminationNamespace: String? = nil,
        terminationCode: String? = nil,
        objcExceptionName: String? = nil,
        objcExceptionClassName: String? = nil,
        hangDurationMs: Int? = nil,
        writesCausedMb: Double? = nil,
        launchDurationMs: Int? = nil,
        frameCount: Int = 0,
        framesTruncated: Bool = false,
        frames: [StabilityCallStackFrame] = []
    ) {
        self.schemaVersion = schemaVersion
        self.kind = kind
        self.receivedAt = receivedAt
        self.appVersion = appVersion
        self.appBuildVersion = appBuildVersion
        self.osVersion = osVersion
        self.deviceType = deviceType
        self.platformArchitecture = platformArchitecture
        self.isTestFlight = isTestFlight
        self.signal = signal
        self.exceptionType = exceptionType
        self.exceptionCode = exceptionCode
        self.terminationNamespace = terminationNamespace
        self.terminationCode = terminationCode
        self.objcExceptionName = objcExceptionName
        self.objcExceptionClassName = objcExceptionClassName
        self.hangDurationMs = hangDurationMs
        self.writesCausedMb = writesCausedMb
        self.launchDurationMs = launchDurationMs
        self.frameCount = frameCount
        self.framesTruncated = framesTruncated
        self.frames = frames
    }

    /// Tolerant decode so a record written by an older schema version
    /// still loads out of the on-disk ring buffer after an app update
    /// (the buffer survives upgrades; a strict decoder would silently
    /// discard every pre-upgrade crash — exactly the ones worth
    /// reading).
    ///
    /// **Every string is re-sanitised on the way IN, not just on the way
    /// out of the projector.** The buffer outlives app versions, so the
    /// bytes the exporter reads were written by some *earlier* build
    /// whose sanitiser may have been laxer — and the export-time
    /// invariant ("everything in `stability_diagnostics` passed the
    /// allowlist") has to hold for the file actually on disk, not for
    /// the binary that happens to be running. Re-validating here makes
    /// it unconditional.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.schemaVersion = try container.decodeIfPresent(Int.self, forKey: .schemaVersion)
            ?? Self.currentSchemaVersion
        self.kind = try container.decode(StabilityDiagnosticKind.self, forKey: .kind)
        self.receivedAt = try container.decode(Double.self, forKey: .receivedAt)
        self.appVersion = DiagnosticTextSanitizer.versionToken(
            try container.decodeIfPresent(String.self, forKey: .appVersion)
        )
        self.appBuildVersion = DiagnosticTextSanitizer.versionToken(
            try container.decodeIfPresent(String.self, forKey: .appBuildVersion)
        )
        self.osVersion = DiagnosticTextSanitizer.versionToken(
            try container.decodeIfPresent(String.self, forKey: .osVersion)
        )
        self.deviceType = DiagnosticTextSanitizer.deviceModel(
            try container.decodeIfPresent(String.self, forKey: .deviceType)
        )
        self.platformArchitecture = DiagnosticTextSanitizer.versionToken(
            try container.decodeIfPresent(String.self, forKey: .platformArchitecture)
        )
        self.isTestFlight = try container.decodeIfPresent(Bool.self, forKey: .isTestFlight)
        self.signal = try container.decodeIfPresent(Int.self, forKey: .signal)
        self.exceptionType = try container.decodeIfPresent(Int.self, forKey: .exceptionType)
        self.exceptionCode = try container.decodeIfPresent(Int.self, forKey: .exceptionCode)
        self.terminationNamespace = DiagnosticTextSanitizer.identifier(
            try container.decodeIfPresent(String.self, forKey: .terminationNamespace)
        ).flatMap { DiagnosticTextSanitizer.knownTerminationNamespaces.contains($0) ? $0 : nil }
        self.terminationCode = DiagnosticTextSanitizer.identifier(
            try container.decodeIfPresent(String.self, forKey: .terminationCode)
        )
        self.objcExceptionName = DiagnosticTextSanitizer.identifier(
            try container.decodeIfPresent(String.self, forKey: .objcExceptionName)
        )
        self.objcExceptionClassName = DiagnosticTextSanitizer.identifier(
            try container.decodeIfPresent(String.self, forKey: .objcExceptionClassName)
        )
        self.hangDurationMs = try container.decodeIfPresent(Int.self, forKey: .hangDurationMs)
        self.writesCausedMb = try container.decodeIfPresent(Double.self, forKey: .writesCausedMb)
        self.launchDurationMs = try container.decodeIfPresent(Int.self, forKey: .launchDurationMs)
        self.frameCount = try container.decodeIfPresent(Int.self, forKey: .frameCount) ?? 0
        self.framesTruncated = try container.decodeIfPresent(Bool.self, forKey: .framesTruncated)
            ?? false
        self.frames = try container.decodeIfPresent(
            [StabilityCallStackFrame].self, forKey: .frames
        ) ?? []
    }
}
