// MetricKitDiagnosticProjector.swift
// Pure transform from an `MXDiagnosticPayload.jsonRepresentation()` blob
// into `[StabilityDiagnosticRecord]`.
//
// Scope: playhead-jw63.4 (crash + hang pipeline).
//
// ----- Why parse the JSON instead of the typed MetricKit objects -----
//
// `MXCrashDiagnostic` / `MXHangDiagnostic` have no public initialiser
// and cannot be constructed in a test. Projecting from the payload's
// own `jsonRepresentation()` — the documented, stable serialisation
// Apple ships for exactly this purpose — moves the entire projection
// (and therefore the entire privacy boundary) into pure code that the
// fast test gate can drive with a fixture. `MetricKitDiagnosticsSubscriber`
// is then a nine-line adapter with nothing to get wrong.
//
// ----- The privacy boundary lives here -----
//
// This file is an ALLOWLIST reader. `payloadKeys` / `metadataKeys` name
// every key it will ever look at; any other key in the payload — present
// today, or added by a future iOS — is never read and therefore cannot
// reach a record. That is a stronger guarantee than scrubbing, because
// it does not require anyone to anticipate what the next leaky field
// will be called.
//
// The keys deliberately NOT in the allowlist, and why:
//
//   exceptionReason.composedMessage  interpolated NSException text —
//   exceptionReason.formatString     the most plausible carrier of an
//   exceptionReason.arguments        episode title or ad-candidate string
//   terminationReason (verbatim)     carries the app's container path
//   virtualMemoryRegionInfo          memory map, embeds absolute paths
//   address                          ASLR-dependent; offset is what atos wants
//   bundleIdentifier / pid           no triage value
//
// `terminationReason` IS read, but only by
// `DiagnosticTextSanitizer.terminationNamespace(from:)` /
// `terminationCode(from:)`, which return an allowlisted token and a hex
// literal respectively. The raw string never lands in a record field.
//
// Every string that does survive is passed through
// `DiagnosticTextSanitizer`, so the encoded record satisfies a
// checkable invariant: no `/`, no `:`, no quotes, no non-ASCII —
// i.e. no URL, no POSIX path, and no prose. See
// `StabilityDiagnosticScrubbingTests`.

import Foundation

enum MetricKitDiagnosticProjector {

    // MARK: - Payload-level key allowlist

    /// The five diagnostic arrays MetricKit vends, mapped to the kind
    /// each produces. Iterating this dictionary is what makes the
    /// reader an allowlist: an array under any other key is invisible.
    static let payloadKeys: [String: StabilityDiagnosticKind] = [
        "crashDiagnostics": .crash,
        "hangDiagnostics": .hang,
        "diskWriteExceptionDiagnostics": .diskWriteException,
        "cpuExceptionDiagnostics": .cpuException,
        "appLaunchDiagnostics": .appLaunch
    ]

    /// Deterministic iteration order for `payloadKeys`, so a payload
    /// carrying several kinds always projects to the same sequence.
    /// `Dictionary` order is not stable across launches.
    static let payloadKeyOrder: [String] = [
        "crashDiagnostics",
        "hangDiagnostics",
        "diskWriteExceptionDiagnostics",
        "cpuExceptionDiagnostics",
        "appLaunchDiagnostics"
    ]

    /// Keys read from `diagnosticMetaData`. Named here as documentation
    /// AND as the fixture that `metadataAllowlistIsExhaustive` checks the
    /// reader against, so a future field cannot be read without being
    /// declared.
    static let metadataKeys: Set<String> = [
        "appVersion",
        "appBuildVersion",
        "osVersion",
        "deviceType",
        "platformArchitecture",
        "isTestFlightApp",
        "signal",
        "exceptionType",
        "exceptionCode",
        "terminationReason",
        "exceptionReason",
        "hangDuration",
        "writesCaused",
        "launchDuration"
    ]

    /// The only two sub-keys read from the `exceptionReason` object.
    /// Both are type-name shaped and pass the identifier allowlist; the
    /// message-bearing siblings are absent by construction.
    static let exceptionReasonKeys: Set<String> = [
        "exceptionName",
        "className"
    ]

    /// Cap on diagnostics projected from a single payload. MetricKit
    /// batches up to 24 h of incidents; a device stuck in a crash loop
    /// can produce a lot of them and the ring buffer is small anyway.
    static let maxRecordsPerPayload = 25

    // MARK: - Entry point

    /// Project a payload blob into records.
    ///
    /// - Parameters:
    ///   - data: the bytes from `MXDiagnosticPayload.jsonRepresentation()`.
    ///   - receivedAt: ingestion time, stamped onto every record.
    ///   - maxFrames: per-record frame cap.
    /// - Returns: one record per diagnostic, in `payloadKeyOrder`. An
    ///   unparseable or unexpectedly-shaped payload yields `[]` — a
    ///   diagnostics pipeline must never be the thing that throws.
    static func records(
        fromPayloadJSON data: Data,
        receivedAt: Date,
        maxFrames: Int = StabilityDiagnosticRecord.maxFramesPerRecord
    ) -> [StabilityDiagnosticRecord] {
        guard let object = try? JSONSerialization.jsonObject(with: data, options: []),
              let payload = object as? [String: Any] else {
            return []
        }
        return records(fromPayloadObject: payload, receivedAt: receivedAt, maxFrames: maxFrames)
    }

    /// Object-level entry point, split out so tests can hand in a
    /// dictionary literal without a JSON round-trip.
    static func records(
        fromPayloadObject payload: [String: Any],
        receivedAt: Date,
        maxFrames: Int = StabilityDiagnosticRecord.maxFramesPerRecord
    ) -> [StabilityDiagnosticRecord] {
        let stamp = receivedAt.timeIntervalSince1970
        var out: [StabilityDiagnosticRecord] = []
        for key in payloadKeyOrder {
            guard let kind = payloadKeys[key],
                  let entries = payload[key] as? [Any] else { continue }
            for entry in entries {
                guard out.count < maxRecordsPerPayload else { return out }
                guard let diagnostic = entry as? [String: Any] else { continue }
                out.append(
                    record(
                        kind: kind,
                        diagnostic: diagnostic,
                        receivedAt: stamp,
                        maxFrames: maxFrames
                    )
                )
            }
        }
        return out
    }

    // MARK: - Single diagnostic

    private static func record(
        kind: StabilityDiagnosticKind,
        diagnostic: [String: Any],
        receivedAt: Double,
        maxFrames: Int
    ) -> StabilityDiagnosticRecord {
        let metadata = diagnostic["diagnosticMetaData"] as? [String: Any] ?? [:]
        let exceptionReason = metadata["exceptionReason"] as? [String: Any]
        let terminationReason = metadata["terminationReason"] as? String

        let stack = callStack(
            from: diagnostic["callStackTree"] as? [String: Any],
            maxFrames: maxFrames
        )

        return StabilityDiagnosticRecord(
            kind: kind,
            receivedAt: receivedAt,
            appVersion: DiagnosticTextSanitizer.versionToken(metadata["appVersion"] as? String),
            appBuildVersion: DiagnosticTextSanitizer.versionToken(
                metadata["appBuildVersion"] as? String
            ),
            osVersion: DiagnosticTextSanitizer.versionToken(metadata["osVersion"] as? String),
            deviceType: DiagnosticTextSanitizer.deviceModel(metadata["deviceType"] as? String),
            platformArchitecture: DiagnosticTextSanitizer.versionToken(
                metadata["platformArchitecture"] as? String
            ),
            isTestFlight: DiagnosticTextSanitizer.boolean(from: metadata["isTestFlightApp"]),
            signal: DiagnosticTextSanitizer.integer(from: metadata["signal"]),
            exceptionType: DiagnosticTextSanitizer.integer(from: metadata["exceptionType"]),
            exceptionCode: DiagnosticTextSanitizer.integer(from: metadata["exceptionCode"]),
            terminationNamespace: DiagnosticTextSanitizer.terminationNamespace(
                from: terminationReason
            ),
            terminationCode: DiagnosticTextSanitizer.terminationCode(from: terminationReason),
            objcExceptionName: DiagnosticTextSanitizer.identifier(
                exceptionReason?["exceptionName"] as? String
            ),
            objcExceptionClassName: DiagnosticTextSanitizer.identifier(
                exceptionReason?["className"] as? String
            ),
            hangDurationMs: kind == .hang
                ? DiagnosticTextSanitizer.milliseconds(from: metadata["hangDuration"])
                : nil,
            writesCausedMb: kind == .diskWriteException
                ? DiagnosticTextSanitizer.megabytes(from: metadata["writesCaused"])
                : nil,
            launchDurationMs: kind == .appLaunch
                ? DiagnosticTextSanitizer.milliseconds(from: metadata["launchDuration"])
                : nil,
            frameCount: stack.totalFrames,
            framesTruncated: stack.truncated,
            frames: stack.frames
        )
    }

    // MARK: - Call-stack projection

    /// Flattened stack plus the pre-truncation frame count.
    struct CallStackProjection: Sendable, Equatable {
        let frames: [StabilityCallStackFrame]
        /// Frames walked before the cap was applied. Equals
        /// `frames.count` when nothing was dropped.
        let totalFrames: Int
        let truncated: Bool

        static let empty = CallStackProjection(frames: [], totalFrames: 0, truncated: false)
    }

    /// Project the ATTRIBUTED thread only.
    ///
    /// `callStackTree.callStacks[]` holds one entry per thread; the one
    /// with `threadAttributed == true` is the thread that crashed or
    /// hung. Shipping only that thread keeps a record small and is the
    /// only stack a triage actually starts from. When no thread is
    /// flagged (some hang payloads), the first stack is used.
    static func callStack(
        from tree: [String: Any]?,
        maxFrames: Int
    ) -> CallStackProjection {
        guard let tree, let stacks = tree["callStacks"] as? [Any] else { return .empty }
        let dictionaries = stacks.compactMap { $0 as? [String: Any] }
        let attributed = dictionaries.first {
            DiagnosticTextSanitizer.boolean(from: $0["threadAttributed"]) == true
        }
        guard let chosen = attributed ?? dictionaries.first,
              let roots = chosen["callStackRootFrames"] as? [Any] else {
            return .empty
        }

        var frames: [StabilityCallStackFrame] = []
        var total = 0
        // Stop walking once we are well past the cap, so a runaway or
        // cyclic tree cannot spin here. `frame_count` claims to be the
        // observed depth up to this bound and no more.
        //
        // Saturating rather than `maxFrames * 8`: an absurd `maxFrames`
        // would otherwise TRAP on overflow, and a crash in the crash
        // reporter is the one failure mode this whole subsystem exists
        // to avoid.
        let walkLimit = maxFrames > Int.max / 8 ? Int.max : maxFrames * 8

        // Explicit stack (depth-first, roots in order) rather than
        // recursion: MetricKit frame trees can be deep, and recursing to
        // their depth is how the test host was already brought down once
        // (JSONSerialization's recursive writer on a 200-deep fixture).
        var pending: [(node: [String: Any], depth: Int)] = roots
            .compactMap { $0 as? [String: Any] }
            .reversed()
            .map { ($0, 0) }

        while let (node, depth) = pending.popLast() {
            total += 1
            if frames.count < maxFrames {
                frames.append(frame(from: node, depth: depth))
            }
            guard total < walkLimit else { break }
            if let subFrames = node["subFrames"] as? [Any] {
                for sub in subFrames.compactMap({ $0 as? [String: Any] }).reversed() {
                    pending.append((sub, depth + 1))
                }
            }
        }

        return CallStackProjection(
            frames: frames,
            totalFrames: total,
            truncated: total > frames.count
        )
    }

    private static func frame(from node: [String: Any], depth: Int) -> StabilityCallStackFrame {
        StabilityCallStackFrame(
            binaryName: DiagnosticTextSanitizer.identifier(node["binaryName"] as? String),
            binaryUUID: DiagnosticTextSanitizer.binaryUUID(node["binaryUUID"] as? String),
            offsetIntoBinaryTextSegment: DiagnosticTextSanitizer.integer(
                from: node["offsetIntoBinaryTextSegment"]
            ) ?? 0,
            depth: depth
        )
    }
}
