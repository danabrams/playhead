// BGTaskTelemetryLogger.swift
// playhead-shpy: Direct telemetry for every BGTaskScheduler /
// BGProcessingTask lifecycle event so the next dogfood incident is a
// 5-second jq query rather than a 700-word forensic root-cause.
//
// Why this logger exists:
//   The 2026-04-25 device snapshot showed background processing stalled
//   all day with zero forward progress on backfill. We had to *infer*
//   the failure mode from activity-blackout shape in
//   `decision-log.jsonl` because no log carried direct
//   `BGTaskScheduler` / `BGProcessingTask` events. The actual cause
//   was a missing fix already shipped (`gjz6`), not a regression — but
//   we had no way to tell that without the inference. This logger
//   closes that observability gap.
//
// Design (mirrors `AssetLifecycleLogger` and `DecisionLogger`):
//   - Actor-serialized append-only writer at
//     `Documents/bg-task-log.jsonl`.
//   - Rotates to `bg-task-log.N.jsonl` at 5 MB; rotation is
//     crash-safe via `FileManager.replaceItemAt` and idempotent across
//     warm starts (the next index is seeded from disk at init).
//   - Stable sorted-keys JSON encoding so the file is diff-friendly.
//   - Non-blocking: callers `await` only the actor hop; file I/O is
//     serialized inside the actor.
//   - Best-effort: every call site fire-and-forgets via
//     `Task { await logger.record(...) }` so a logger failure can
//     never block the BG-task code path. A FileManager failure is
//     logged at the os_log subsystem and the in-process record is
//     dropped.
//
// Schema version: 1.
//
// Event taxonomy (the `event` discriminator on every record):
//   - "submit"      : a `BGTaskScheduler.submit(_:)` call (success or failure)
//   - "start"       : the launch handler fired (OS dispatched the task)
//   - "complete"    : `setTaskCompleted(success:)` was called
//   - "expire"      : the OS fired the `expirationHandler` closure
//   - "appPhase"    : a `UIApplication` scene-phase transition (correlate
//                     submitted-vs-fired against background entry/exit)
//
// jq cookbook:
//
//   # submitted-vs-fired ratio per task identifier (denominator: submits,
//   # numerator: starts):
//   #
//   jq -s '
//     map(select(.event == "submit" or .event == "start"))
//     | group_by(.identifier)
//     | map({
//         identifier: .[0].identifier,
//         submits: map(select(.event == "submit" and .submitSucceeded == true)) | length,
//         starts:  map(select(.event == "start")) | length
//       })
//   ' Documents/bg-task-log.jsonl
//
//   # expiration rate (expirations / starts):
//   #
//   jq -s '
//     def cnt(e): map(select(.event == e)) | length;
//     { starts: cnt("start"), expires: cnt("expire") }
//   ' Documents/bg-task-log.jsonl
//
//   # mean wall-clock seconds spent inside a BG task (start → complete or
//   # start → expire), grouped by identifier:
//   #
//   jq -s '
//     [ .[] | select(.event == "start" or .event == "complete" or .event == "expire") ]
//     | group_by(.taskInstanceID)
//     | map(select(length >= 2))
//     | map({
//         identifier: (.[0].identifier),
//         duration: ((.[-1].ts | fromdateiso8601) - (.[0].ts | fromdateiso8601))
//       })
//     | group_by(.identifier)
//     | map({
//         identifier: .[0].identifier,
//         meanSec: (map(.duration) | add / length)
//       })
//   ' Documents/bg-task-log.jsonl
//
//   # last 12h timeline (human-readable):
//   #
//   jq -s -r '
//     map(select((.ts | fromdateiso8601) > (now - 12*3600)))
//     | sort_by(.ts)
//     | .[]
//     | "\(.ts) \(.event) \(.identifier // "-") \(.detail // "")"
//   ' Documents/bg-task-log.jsonl

import Foundation
import OSLog
#if canImport(UIKit) && os(iOS)
import UIKit
#endif

// MARK: - BGTaskTelemetryLogging

/// Protocol seam so production code can be tested with a NoOp/recording
/// logger without standing up a real file-backed actor. All call sites
/// hold `any BGTaskTelemetryLogging` and never depend on the concrete
/// `BGTaskTelemetryLogger`.
protocol BGTaskTelemetryLogging: Sendable {
    /// Append a single telemetry record. Must not block the caller
    /// beyond the actor hop; file I/O is serialized inside the actor.
    func record(_ event: BGTaskTelemetryEvent) async
}

/// No-op logger for release builds, tests that do not exercise BG-task
/// telemetry, and the early-launch window before the real logger is
/// wired (e.g. the `registerTaskIdentifiers()` static fallback path).
struct NoOpBGTaskTelemetryLogger: BGTaskTelemetryLogging {
    func record(_ event: BGTaskTelemetryEvent) async {
        // intentionally blank
    }
}

// MARK: - BGTaskTelemetryEvent

/// Schema-versioned, Codable record for one BG-task lifecycle moment.
///
/// All fields except `schemaVersion`, `ts`, and `event` are optional
/// because different event types carry different payloads. The
/// `ObjectIdentifier`-derived `taskInstanceID` lets a downstream tool
/// pair a `start` with its eventual `complete` / `expire` for the same
/// underlying `BGProcessingTask` instance even when several tasks share
/// the same `identifier`.
struct BGTaskTelemetryEvent: Codable, Equatable, Sendable {

    /// Schema version. Increment on breaking changes.
    let schemaVersion: Int

    /// ISO-8601 wall-clock time of the event.
    let ts: Date

    /// Discriminator: "submit" | "start" | "complete" | "expire" | "appPhase".
    let event: String

    /// BGTaskScheduler identifier (e.g. "com.playhead.app.analysis.backfill").
    /// Nil only on `appPhase` events.
    let identifier: String?

    /// Per-instance pointer-derived ID stringified from
    /// `ObjectIdentifier(bgTask).hashValue`. Stable for the lifetime of
    /// a single `BGProcessingTask` so `start`/`complete`/`expire` rows
    /// for the same instance share a key. Nil on `submit` / `appPhase`.
    let taskInstanceID: String?

    // MARK: submit

    /// Whether `BGTaskScheduler.submit(_:)` returned without throwing.
    /// Only present on `event == "submit"`.
    let submitSucceeded: Bool?

    /// Localized error description if `submit` threw. Captured for the
    /// failure path so the dogfood log records WHY iOS rejected the
    /// request (e.g. permitted-identifiers mismatch).
    let submitError: String?

    /// `BGTaskRequest.earliestBeginDate?.timeIntervalSinceNow` at submit
    /// time. Negative means iOS may dispatch immediately. Nil when the
    /// caller did not set an earliest-begin date.
    let earliestBeginDelaySec: Double?

    // MARK: start

    /// Wall-clock seconds between the most recent `submit` for this
    /// identifier and the `start` callback. Nil when no submit was
    /// observed in this process (warm-launch fire after a cold restart).
    let timeSinceSubmitSec: Double?

    /// Snapshot of the iOS scene phase at the moment the event fired.
    /// "active" | "inactive" | "background" | "unknown". Captured for
    /// every event because every event is interesting against the
    /// foreground/background axis.
    let scenePhase: String?

    // MARK: complete / expire

    /// `setTaskCompleted(success:)` argument. Present on
    /// `event == "complete"`.
    let success: Bool?

    /// Wall-clock seconds the task ran before the event. Computed from
    /// the `start` row's `ts` for the same `taskInstanceID`. Present on
    /// `complete` and `expire` when a start row was seen in-process.
    let timeInTaskSec: Double?

    // MARK: appPhase

    /// Old → new phase transition for `event == "appPhase"`.
    let phaseFrom: String?
    let phaseTo: String?

    /// Free-form detail string. Used for `submit(reason:)` annotations
    /// (e.g. "feed-refresh-rearm") and any other context the call site
    /// wants to pin in the log without a typed field.
    let detail: String?

    static let currentSchemaVersion: Int = 1

    /// Designated init. Defaults every optional to nil so call sites
    /// only specify what they have.
    init(
        schemaVersion: Int = BGTaskTelemetryEvent.currentSchemaVersion,
        ts: Date,
        event: String,
        identifier: String? = nil,
        taskInstanceID: String? = nil,
        submitSucceeded: Bool? = nil,
        submitError: String? = nil,
        earliestBeginDelaySec: Double? = nil,
        timeSinceSubmitSec: Double? = nil,
        scenePhase: String? = nil,
        success: Bool? = nil,
        timeInTaskSec: Double? = nil,
        phaseFrom: String? = nil,
        phaseTo: String? = nil,
        detail: String? = nil
    ) {
        self.schemaVersion = schemaVersion
        self.ts = ts
        self.event = event
        self.identifier = identifier
        self.taskInstanceID = taskInstanceID
        self.submitSucceeded = submitSucceeded
        self.submitError = submitError
        self.earliestBeginDelaySec = earliestBeginDelaySec
        self.timeSinceSubmitSec = timeSinceSubmitSec
        self.scenePhase = scenePhase
        self.success = success
        self.timeInTaskSec = timeInTaskSec
        self.phaseFrom = phaseFrom
        self.phaseTo = phaseTo
        self.detail = detail
    }

    // MARK: - Convenience constructors

    static func submit(
        identifier: String,
        succeeded: Bool,
        error: String? = nil,
        earliestBeginDelaySec: Double? = nil,
        scenePhase: String? = nil,
        detail: String? = nil,
        now: Date = Date()
    ) -> BGTaskTelemetryEvent {
        BGTaskTelemetryEvent(
            ts: now,
            event: "submit",
            identifier: identifier,
            submitSucceeded: succeeded,
            submitError: error,
            earliestBeginDelaySec: earliestBeginDelaySec,
            scenePhase: scenePhase,
            detail: detail
        )
    }

    static func start(
        identifier: String,
        taskInstanceID: String,
        timeSinceSubmitSec: Double?,
        scenePhase: String?,
        now: Date = Date()
    ) -> BGTaskTelemetryEvent {
        BGTaskTelemetryEvent(
            ts: now,
            event: "start",
            identifier: identifier,
            taskInstanceID: taskInstanceID,
            timeSinceSubmitSec: timeSinceSubmitSec,
            scenePhase: scenePhase
        )
    }

    static func complete(
        identifier: String,
        taskInstanceID: String,
        success: Bool,
        timeInTaskSec: Double?,
        scenePhase: String?,
        now: Date = Date()
    ) -> BGTaskTelemetryEvent {
        BGTaskTelemetryEvent(
            ts: now,
            event: "complete",
            identifier: identifier,
            taskInstanceID: taskInstanceID,
            scenePhase: scenePhase,
            success: success,
            timeInTaskSec: timeInTaskSec
        )
    }

    static func expire(
        identifier: String,
        taskInstanceID: String,
        timeInTaskSec: Double?,
        scenePhase: String?,
        detail: String? = nil,
        now: Date = Date()
    ) -> BGTaskTelemetryEvent {
        BGTaskTelemetryEvent(
            ts: now,
            event: "expire",
            identifier: identifier,
            taskInstanceID: taskInstanceID,
            scenePhase: scenePhase,
            timeInTaskSec: timeInTaskSec,
            detail: detail
        )
    }

    static func appPhase(
        from oldPhase: String,
        to newPhase: String,
        now: Date = Date()
    ) -> BGTaskTelemetryEvent {
        BGTaskTelemetryEvent(
            ts: now,
            event: "appPhase",
            phaseFrom: oldPhase,
            phaseTo: newPhase
        )
    }
}

// MARK: - BGTaskTelemetryLogger

/// Actor-backed JSONL writer for BG-task lifecycle events.
///
/// Same I/O shape as `AssetLifecycleLogger` (rolling file + crash-safe
/// rotation). Tracks per-identifier submit timestamps so the `start`
/// event can compute `timeSinceSubmitSec` without the call site having
/// to remember when it submitted, and per-instance start timestamps so
/// `complete` / `expire` can compute `timeInTaskSec`.
actor BGTaskTelemetryLogger: BGTaskTelemetryLogging {

    /// Default rotation threshold (5 MB). Smaller than the
    /// AssetLifecycleLogger threshold because BG-task events are sparse
    /// — we'd rather have 4–5 historical files than one bloated active
    /// file when forensics need to span multiple days of dogfooding.
    static let defaultRotationThresholdBytes: Int = 5 * 1024 * 1024

    /// Active log file basename.
    static let activeLogFilename: String = "bg-task-log.jsonl"

    /// Prefix for rotated files: `bg-task-log.N.jsonl`.
    static let rotatedPrefix: String = "bg-task-log"
    static let rotatedSuffix: String = ".jsonl"

    /// playhead-jncn: lazy-resolved directory. The convenience init
    /// (no `directory:` arg) used to call
    /// `FileManager.url(.documentDirectory, create: true)` synchronously
    /// inside `PlayheadRuntime.init`; that lookup now defers to
    /// `ensureBootstrapped()`.
    private let directoryOverride: URL?
    private var resolvedDirectory: URL?

    private let rotationThresholdBytes: Int
    private let encoder: JSONEncoder
    private let logger = Logger(subsystem: "com.playhead", category: "BGTaskTelemetry")

    /// Next rotation index; seeded from disk on first use. Optional
    /// until then so we can distinguish "haven't bootstrapped" from
    /// "no rotated files on disk" (which produces 1).
    private var nextRotationIndex: Int?
    private var fileHandle: FileHandle?

    /// Most recent successful-submit wall-clock time per BGTask identifier,
    /// so the `start` callback can derive `timeSinceSubmitSec` without the
    /// call site needing to plumb it. Only populated on submit-success — a
    /// failed submit cannot have produced the eventual start.
    private var lastSubmitAt: [String: Date] = [:]

    /// Wall-clock time when each in-flight BG task instance started, so
    /// `complete`/`expire` rows can fill `timeInTaskSec`. Cleared on
    /// terminal events for the same instance to bound memory.
    private var startAtByInstance: [String: Date] = [:]

    // MARK: - Init

    /// Convenience init targeting the app's Documents directory.
    ///
    /// playhead-jncn: the Documents lookup + directory create are
    /// deferred to first use. Production callers
    /// `await logger.migrate()` from `PlayheadRuntime`'s deferred init
    /// Task to warm the path off-main.
    init(
        rotationThresholdBytes: Int = BGTaskTelemetryLogger.defaultRotationThresholdBytes
    ) throws {
        // playhead-jncn: store rotation threshold + encoder only; defer
        // the Documents lookup, directory create, and rotation-index
        // scan to `ensureBootstrapped()`.
        self.directoryOverride = nil
        self.rotationThresholdBytes = rotationThresholdBytes
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder
        self.resolvedDirectory = nil
        self.nextRotationIndex = nil
    }

    /// Designated init. Tests pass an arbitrary directory and a small
    /// rotation threshold.
    ///
    /// playhead-jncn: directory create + scanNextRotationIndex are
    /// deferred to `ensureBootstrapped()`.
    init(
        directory: URL,
        rotationThresholdBytes: Int = BGTaskTelemetryLogger.defaultRotationThresholdBytes
    ) throws {
        self.directoryOverride = directory
        self.rotationThresholdBytes = rotationThresholdBytes
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder
        self.resolvedDirectory = nil
        self.nextRotationIndex = nil
    }

    /// playhead-jncn: lazy first-use bootstrap. Resolves the directory
    /// (Documents lookup for the convenience init), creates it, and
    /// seeds `nextRotationIndex` from disk. Idempotent.
    /// `PlayheadRuntime` calls this from the deferred init Task so the
    /// expensive setup runs off-main.
    func migrate() throws {
        try ensureBootstrapped()
    }

    /// Lazy bootstrap shared by `migrate()` and the write path.
    /// Idempotent on the (resolvedDirectory, nextRotationIndex) tuple.
    private func ensureBootstrapped() throws {
        if resolvedDirectory == nil {
            let dir: URL
            if let override = directoryOverride {
                dir = override
            } else {
                dir = try FileManager.default.url(
                    for: .documentDirectory,
                    in: .userDomainMask,
                    appropriateFor: nil,
                    create: true
                )
            }
            try FileManager.default.createDirectory(
                at: dir, withIntermediateDirectories: true
            )
            self.resolvedDirectory = dir
        }
        if nextRotationIndex == nil, let dir = resolvedDirectory {
            self.nextRotationIndex = Self.scanNextRotationIndex(in: dir)
        }
    }

    // MARK: - Public API

    func record(_ event: BGTaskTelemetryEvent) async {
        // Backfill computed fields (timeSinceSubmitSec / timeInTaskSec)
        // BEFORE updating correlation state, because terminal events
        // (`complete` / `expire`) want to read the matching `start`'s
        // timestamp out of `startAtByInstance` and `rememberCorrelation`
        // is the thing that clears it on terminal observation.
        let enriched = enrich(event)
        rememberCorrelation(for: event)

        do {
            try ensureBootstrapped()
            try appendEntry(enriched)
            try rotateIfNeeded()
        } catch {
            logger.warning("BGTaskTelemetryLogger.record failed: \(error.localizedDescription, privacy: .public)")
        }
    }

    // MARK: - Test hooks

    /// Returns the currently-scheduled rotation index. Tests only.
    /// Triggers the lazy bootstrap so tests that probe pre-write
    /// observe the seeded value.
    func currentNextRotationIndex() -> Int {
        try? ensureBootstrapped()
        return nextRotationIndex ?? 1
    }

    /// Force-close the handle; tests call this before reading the file
    /// back through `Data(contentsOf:)` so pending writes flush.
    func flushAndClose() {
        closeHandle()
    }

    /// Absolute path to the currently-active log file. Triggers the
    /// lazy bootstrap on first call so callers can rely on the path.
    var activeLogURL: URL {
        try? ensureBootstrapped()
        let dir = resolvedDirectory ?? directoryOverride ?? URL(fileURLWithPath: NSTemporaryDirectory())
        return dir.appendingPathComponent(Self.activeLogFilename)
    }

    /// All rotated log URLs, ordered by numeric index.
    func rotatedLogURLs() -> [URL] {
        try? ensureBootstrapped()
        guard let dir = resolvedDirectory else { return [] }
        return Self.listRotatedLogs(in: dir)
    }

    // MARK: - Internal: correlation

    private func rememberCorrelation(for event: BGTaskTelemetryEvent) {
        switch event.event {
        case "submit":
            if event.submitSucceeded == true, let id = event.identifier {
                lastSubmitAt[id] = event.ts
            }
        case "start":
            if let key = event.taskInstanceID {
                startAtByInstance[key] = event.ts
            }
        case "complete", "expire":
            if let key = event.taskInstanceID {
                startAtByInstance.removeValue(forKey: key)
            }
        default:
            break
        }
    }

    private func enrich(_ event: BGTaskTelemetryEvent) -> BGTaskTelemetryEvent {
        switch event.event {
        case "start":
            guard event.timeSinceSubmitSec == nil,
                  let id = event.identifier,
                  let submittedAt = lastSubmitAt[id]
            else { return event }
            return BGTaskTelemetryEvent(
                schemaVersion: event.schemaVersion,
                ts: event.ts,
                event: event.event,
                identifier: event.identifier,
                taskInstanceID: event.taskInstanceID,
                submitSucceeded: event.submitSucceeded,
                submitError: event.submitError,
                earliestBeginDelaySec: event.earliestBeginDelaySec,
                timeSinceSubmitSec: event.ts.timeIntervalSince(submittedAt),
                scenePhase: event.scenePhase,
                success: event.success,
                timeInTaskSec: event.timeInTaskSec,
                phaseFrom: event.phaseFrom,
                phaseTo: event.phaseTo,
                detail: event.detail
            )
        case "complete", "expire":
            // `record` calls enrich BEFORE rememberCorrelation, so the
            // matching start timestamp is still in the dict at this point.
            guard event.timeInTaskSec == nil,
                  let key = event.taskInstanceID,
                  let startedAt = startAtByInstance[key]
            else { return event }
            return BGTaskTelemetryEvent(
                schemaVersion: event.schemaVersion,
                ts: event.ts,
                event: event.event,
                identifier: event.identifier,
                taskInstanceID: event.taskInstanceID,
                submitSucceeded: event.submitSucceeded,
                submitError: event.submitError,
                earliestBeginDelaySec: event.earliestBeginDelaySec,
                timeSinceSubmitSec: event.timeSinceSubmitSec,
                scenePhase: event.scenePhase,
                success: event.success,
                timeInTaskSec: event.ts.timeIntervalSince(startedAt),
                phaseFrom: event.phaseFrom,
                phaseTo: event.phaseTo,
                detail: event.detail
            )
        default:
            return event
        }
    }

    // MARK: - Internal: I/O

    private func appendEntry(_ entry: BGTaskTelemetryEvent) throws {
        let data = try encoder.encode(entry)
        var line = Data()
        line.reserveCapacity(data.count + 1)
        line.append(data)
        line.append(0x0A) // newline
        try write(line)
    }

    private func write(_ data: Data) throws {
        let url = activeLogURL
        if fileHandle == nil {
            if !FileManager.default.fileExists(atPath: url.path) {
                FileManager.default.createFile(atPath: url.path, contents: nil)
            }
            fileHandle = try FileHandle(forWritingTo: url)
            try fileHandle?.seekToEnd()
        }
        try fileHandle?.write(contentsOf: data)
    }

    private func rotateIfNeeded() throws {
        let url = activeLogURL
        let attrs = try FileManager.default.attributesOfItem(atPath: url.path)
        let size = (attrs[.size] as? NSNumber)?.intValue ?? 0
        guard size >= rotationThresholdBytes else { return }

        // Same livelock guard as DecisionLogger / AssetLifecycleLogger.
        if try lineCount(at: url) <= 1 {
            logger.warning(
                "BGTaskTelemetryLogger: active log exceeds threshold but has \u{2264}1 line; skipping rotation"
            )
            return
        }
        try rotateNow()
    }

    private func lineCount(at url: URL) throws -> Int {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var count = 0
        while let chunk = try handle.read(upToCount: 64 * 1024), !chunk.isEmpty {
            for byte in chunk where byte == 0x0A {
                count += 1
                if count >= 2 { return count }
            }
        }
        return count
    }

    private func rotateNow() throws {
        // playhead-jncn: callers come through `record(_:)` which always
        // calls `ensureBootstrapped()` first, but re-run defensively.
        try ensureBootstrapped()
        guard let dir = resolvedDirectory, let idx = nextRotationIndex else {
            return
        }
        let src = activeLogURL
        let dstName = "\(Self.rotatedPrefix).\(idx)\(Self.rotatedSuffix)"
        let dst = dir.appendingPathComponent(dstName)

        closeHandle()

        let fm = FileManager.default
        if fm.fileExists(atPath: dst.path) {
            _ = try fm.replaceItemAt(dst, withItemAt: src)
        } else {
            try fm.moveItem(at: src, to: dst)
        }
        nextRotationIndex = idx + 1
        logger.info("BGTaskTelemetryLogger: rotated active log to \(dstName, privacy: .public)")
    }

    private func closeHandle() {
        if let handle = fileHandle {
            try? handle.close()
            fileHandle = nil
        }
    }

    // MARK: - Static helpers

    fileprivate static func scanNextRotationIndex(in directory: URL) -> Int {
        listRotatedLogs(in: directory)
            .compactMap { extractRotationIndex(from: $0.lastPathComponent) }
            .max()
            .map { $0 + 1 } ?? 1
    }

    fileprivate static func listRotatedLogs(in directory: URL) -> [URL] {
        guard let items = try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else { return [] }
        let matches = items.filter { url in
            extractRotationIndex(from: url.lastPathComponent) != nil
        }
        return matches.sorted { lhs, rhs in
            let li = extractRotationIndex(from: lhs.lastPathComponent) ?? 0
            let ri = extractRotationIndex(from: rhs.lastPathComponent) ?? 0
            return li < ri
        }
    }

    fileprivate static func extractRotationIndex(from name: String) -> Int? {
        let prefix = rotatedPrefix + "."
        let suffix = rotatedSuffix
        guard name.hasPrefix(prefix), name.hasSuffix(suffix) else { return nil }
        let middle = name.dropFirst(prefix.count).dropLast(suffix.count)
        return Int(middle)
    }
}

// MARK: - Helpers shared across BG-task call sites

/// Stable, short string for an `ObjectIdentifier`-keyed BG task. Used
/// by the call sites that emit `start`/`complete`/`expire` events so
/// every row in the log can be paired against its peers without leaking
/// raw memory addresses into the on-disk format.
@inlinable
func bgTaskInstanceID(for task: AnyObject) -> String {
    String(ObjectIdentifier(task).hashValue, radix: 16)
}

/// playhead-shpy: small UIKit-isolated helper so non-MainActor BG-task
/// call sites can stamp every telemetry row with the current scene
/// phase without each emit site reimplementing the MainActor hop.
///
/// Returns one of "active" / "inactive" / "background" / "unknown".
/// Defined as an enum-namespace rather than a free function to keep
/// the import surface (`UIKit`) confined to one place; non-iOS test
/// hosts compile this as a stub returning "unknown".
enum BGTaskTelemetryScenePhase {

    #if canImport(UIKit) && os(iOS)
    @MainActor
    static func currentMainActor() -> String {
        switch UIApplication.shared.applicationState {
        case .active:     return "active"
        case .inactive:   return "inactive"
        case .background: return "background"
        @unknown default: return "unknown"
        }
    }

    static func current() async -> String {
        await MainActor.run { currentMainActor() }
    }
    #else
    static func current() async -> String { "unknown" }
    #endif
}
