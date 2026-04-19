// SurfaceStatusInvariantLogger.swift
// Tier-B (production-safe, all-builds) logging channel for impossible-
// state violations and state-transition audit entries from the surface-
// status reducer.
//
// Scope: playhead-ol05 (Phase 1.5 — "State-transition audit + impossible-
// state assertions + cross-target contract test").
//
// ----- Overview -----
//
// The logger writes JSON Lines to a per-session file under
// `Caches/Diagnostics/`. Each line is exactly one
// `SurfaceStateTransitionEntry` (with optional `invariantViolation`
// payload). Files rotate per session — a new launch picks a fresh
// timestamped filename — and the logger evicts older sessions on
// startup so disk usage cannot grow without bound across long-lived
// dogfood sessions.
//
// Retention matches `playhead-ghon`'s scheduler_events 200-row tail:
// the most recent session files are kept, older ones are deleted on
// launch. (The exact numeric ceiling lives in `maxSessionFiles` below
// and may diverge from ghon's row-count if a per-row vs per-session
// distinction matters; today they share the same magic number for
// convenience.)
//
// Thread-safety: every write goes through a serial `DispatchQueue` and
// is fire-and-forget from the caller's perspective. The reducer / batch
// validator does not block on the logger — log emission is best-effort.
//
// ----- Schema (owned by this bead) -----
//
//   {
//     "timestamp": <ISO-8601 UTC string>,
//     "session_id": <UUID string>,
//     "episode_id_hash": <hex string | null>,
//     "prior_disposition": <SurfaceDisposition.rawValue | null>,
//     "new_disposition": <SurfaceDisposition.rawValue>,
//     "prior_reason": <SurfaceReason.rawValue | null>,
//     "new_reason": <SurfaceReason.rawValue>,
//     "cause": <InternalMissCause.rawValue | null>,
//     "eligibility_snapshot": { five Bool fields + capturedAt },
//     "invariant_violation": <InvariantViolation | null>
//   }
//
// `episodeId_hash` is SHA-256 of `installID || episodeId`, hex-encoded.
// The installID salt is per-install; an attacker without the salt
// cannot reverse the hash to recover the episode ID. The hasher is
// implemented locally in this module because `playhead-ghon` (which
// owns the canonical hasher) is being landed concurrently — the two
// implementations share the same contract and either can be wired up
// once both land.
//
// ----- API surface -----
//
//   * `SurfaceStatusInvariantLogger.record(_:)` — append one entry.
//   * `SurfaceStatusInvariantLogger.recordViolations(_:context:)` —
//     emit one entry per violation; convenience for invariant-only
//     emission paths.
//   * `SurfaceStatusInvariantLogger.invariantViolated(_:)` — legacy
//     string-only entry-point, retained so the reducer's existing call-
//     sites do not have to change in this bead. Wraps the message in a
//     synthetic violation entry.
//
// All entry-points are namespaced static methods on the enum (no
// instance, no reference counting on the hot path).

import Foundation

// MARK: - SurfaceStatusInvariantLogger

/// Tier-B production logging channel. Writes JSON Lines to a per-session
/// file under `Caches/Diagnostics/`. See the file-level overview for the
/// schema and retention policy.
enum SurfaceStatusInvariantLogger {

    // MARK: - Configuration

    /// Maximum number of session files to retain. Older files are
    /// evicted on startup. Matches `playhead-ghon`'s scheduler_events
    /// 200-row tail cadence — the two retention policies share a magic
    /// number for ease of reasoning even though one is per-row and the
    /// other is per-session.
    static let maxSessionFiles: Int = 200

    /// Directory name under `Caches/` that holds the JSON Lines files.
    /// Co-located with other diagnostics for easy support-bundle
    /// extraction.
    static let diagnosticsDirectoryName: String = "Diagnostics"

    /// Filename prefix for session files. The full filename is
    /// `surface-status-<sessionTimestamp>-<sessionId>.jsonl`; the
    /// timestamp is what we sort by during eviction.
    static let sessionFilenamePrefix: String = "surface-status-"

    /// Filename extension. JSON Lines convention.
    static let sessionFilenameExtension: String = "jsonl"

    // MARK: - State

    /// Backing storage for the per-process logger state. Lazily
    /// initialized on first use so unit tests that never touch the
    /// logger pay no I/O cost.
    private static let state: LoggerState = LoggerState()

    // MARK: - Public API

    /// Append a single state-transition entry to the current session
    /// file. Fire-and-forget — the call returns as soon as the entry is
    /// enqueued on the serial write queue.
    static func record(_ entry: SurfaceStateTransitionEntry) {
        state.enqueueWrite(entry)
    }

    /// Convenience: emit one entry per violation in the supplied list,
    /// reusing the supplied `context` for the non-violation fields.
    /// When `violations` is empty this is a no-op.
    static func recordViolations(
        _ violations: [InvariantViolation],
        context: SurfaceStateTransitionContext
    ) {
        for violation in violations {
            let entry = SurfaceStateTransitionEntry(
                timestamp: context.timestamp,
                sessionId: state.sessionId,
                episodeIdHash: context.episodeIdHash,
                priorDisposition: context.priorDisposition,
                newDisposition: context.newDisposition,
                priorReason: context.priorReason,
                newReason: context.newReason,
                cause: context.cause,
                eligibilitySnapshot: context.eligibilitySnapshot,
                invariantViolation: violation
            )
            record(entry)
        }
    }

    /// Legacy string-only entry-point retained so the reducer's existing
    /// call-sites do not need to change. Wraps the message in a
    /// synthetic `unavailableWithRetryHint`-coded violation purely so
    /// the JSON shape stays consistent — callers that want the right
    /// code should migrate to `record(_:)` or
    /// `recordViolations(_:context:)`.
    ///
    /// TODO(playhead-ol05.followup): migrate the reducer's three
    /// `invariantViolated(_:)` call-sites to `recordViolations` with a
    /// real context, so the synthetic-code workaround can be retired.
    static func invariantViolated(_ message: String) {
        let entry = SurfaceStateTransitionEntry(
            timestamp: Date(),
            sessionId: state.sessionId,
            episodeIdHash: nil,
            priorDisposition: nil,
            newDisposition: .failed,
            priorReason: nil,
            newReason: .couldntAnalyze,
            cause: nil,
            eligibilitySnapshot: nil,
            invariantViolation: InvariantViolation(
                code: .unavailableWithRetryHint,
                description: message
            )
        )
        record(entry)
    }

    // MARK: - Test-only introspection

    #if DEBUG
    /// Test hook: force the logger to point at a temporary directory
    /// so unit tests can write/read entries without polluting the real
    /// `Caches/`. Returns the directory the logger is now writing to.
    /// Compiled out of Release.
    @discardableResult
    internal static func _resetForTesting(directory: URL? = nil) -> URL {
        return state.resetForTesting(directory: directory)
    }

    /// Test hook: synchronously drain pending writes. Use after
    /// `record(_:)` to ensure the file reflects every emitted entry
    /// before reading it back.
    internal static func _flushForTesting() {
        state.flushForTesting()
    }

    /// Test hook: read the path of the current session file.
    internal static func _currentSessionFileURL() -> URL? {
        return state.currentSessionFileURL
    }

    /// Test hook: read the session ID the logger is using.
    internal static func _currentSessionId() -> UUID {
        return state.sessionId
    }
    #endif
}

// MARK: - LoggerState

/// Backing state for `SurfaceStatusInvariantLogger`. Encapsulates the
/// serial write queue, current session file, and install-ID salt used
/// by the hasher.
///
/// Marked `@unchecked Sendable` because mutation is gated by the serial
/// `writeQueue` — every property accessed on more than one thread is
/// either immutable (`sessionId`, `installId`) or only mutated inside
/// `writeQueue`'s closure body.
private final class LoggerState: @unchecked Sendable {

    /// Serial queue: every write hops onto this before touching the file
    /// system. Sync-on-demand via `flushForTesting()`.
    private let writeQueue: DispatchQueue

    /// Per-session UUID stamped onto every entry and incorporated into
    /// the session filename so two simultaneously-launched processes
    /// (test runner + app) cannot collide. Mutable ONLY via
    /// `resetForTesting` — the real logger never rotates mid-process.
    private(set) var sessionId: UUID

    /// Per-install salt for the episode-ID hasher. Generated once and
    /// persisted in the diagnostics directory; subsequent launches read
    /// the same value so cross-session correlation works.
    private(set) var installId: String

    /// The session file URL. Lazily realized on first write so that a
    /// process that never logs anything does not create empty files.
    private(set) var currentSessionFileURL: URL?

    /// File handle for the current session. Lazily opened.
    private var currentFileHandle: FileHandle?

    /// Diagnostics directory the logger writes into. Defaults to
    /// `Caches/Diagnostics/`; tests can override via
    /// `resetForTesting(directory:)`.
    private var diagnosticsDirectory: URL

    init() {
        self.writeQueue = DispatchQueue(
            label: "playhead.surface-status-invariant-logger",
            qos: .utility
        )
        self.sessionId = UUID()
        self.diagnosticsDirectory = LoggerState.defaultDiagnosticsDirectory()
        self.installId = LoggerState.loadOrCreateInstallId(
            in: self.diagnosticsDirectory
        )
    }

    // MARK: - Write path

    func enqueueWrite(_ entry: SurfaceStateTransitionEntry) {
        writeQueue.async { [weak self] in
            self?.writeLocked(entry)
        }
    }

    /// Must run on `writeQueue`.
    private func writeLocked(_ entry: SurfaceStateTransitionEntry) {
        do {
            let handle = try ensureSessionFileLocked()
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.sortedKeys]
            encoder.dateEncodingStrategy = .iso8601
            var data = try encoder.encode(entry)
            data.append(0x0A) // newline — JSON Lines convention
            try handle.write(contentsOf: data)
        } catch {
            // Best-effort: the logger never throws to the caller. Drop
            // the entry on the floor and continue. A future iteration
            // could add an os_log fallback here.
        }
    }

    /// Must run on `writeQueue`. Opens (and rotates) the session file
    /// on first call; returns the cached handle on subsequent calls.
    private func ensureSessionFileLocked() throws -> FileHandle {
        if let handle = currentFileHandle { return handle }

        try FileManager.default.createDirectory(
            at: diagnosticsDirectory,
            withIntermediateDirectories: true
        )

        // Eviction: keep only the most recent N session files. Done
        // lazily on first write rather than at init so a process that
        // never logs anything does no I/O.
        evictOldSessionFilesLocked()

        let timestamp = LoggerState.filenameTimestamp(Date())
        let filename = "\(SurfaceStatusInvariantLogger.sessionFilenamePrefix)\(timestamp)-\(sessionId.uuidString).\(SurfaceStatusInvariantLogger.sessionFilenameExtension)"
        let fileURL = diagnosticsDirectory.appendingPathComponent(filename)
        FileManager.default.createFile(atPath: fileURL.path, contents: nil)
        let handle = try FileHandle(forWritingTo: fileURL)
        try handle.seekToEnd()
        self.currentFileHandle = handle
        self.currentSessionFileURL = fileURL
        return handle
    }

    /// Eviction policy: keep the most-recent `maxSessionFiles` session
    /// files by lexicographic ordering of the filename (the timestamp
    /// prefix sorts chronologically). Older files are deleted.
    ///
    /// Must run on `writeQueue`.
    private func evictOldSessionFilesLocked() {
        let fm = FileManager.default
        guard let entries = try? fm.contentsOfDirectory(
            at: diagnosticsDirectory,
            includingPropertiesForKeys: nil
        ) else {
            return
        }
        let sessionFiles = entries
            .filter { $0.lastPathComponent.hasPrefix(SurfaceStatusInvariantLogger.sessionFilenamePrefix) }
            .filter { $0.pathExtension == SurfaceStatusInvariantLogger.sessionFilenameExtension }
            .sorted { $0.lastPathComponent > $1.lastPathComponent } // newest-first
        let cap = SurfaceStatusInvariantLogger.maxSessionFiles
        // Keep cap-1 existing files (we are about to create the cap-th)
        // so steady-state is exactly `maxSessionFiles` after the new
        // file lands.
        guard sessionFiles.count >= cap else { return }
        for url in sessionFiles.dropFirst(max(cap - 1, 0)) {
            try? fm.removeItem(at: url)
        }
    }

    // MARK: - Hasher

    /// Hash an episode ID into the opaque token that lands in the
    /// `episode_id_hash` field of every JSON Lines entry.
    ///
    /// SHA-256(installID || episodeId), hex-encoded. Matches the
    /// `playhead-ghon` contract — when ghon's hasher lands the two
    /// implementations should produce byte-identical output for the
    /// same (installID, episodeId) pair.
    func hashEpisodeId(_ episodeId: String) -> String {
        return SurfaceStatusEpisodeIdHasher.hash(
            installId: installId,
            episodeId: episodeId
        )
    }

    // MARK: - Test-only helpers

    func resetForTesting(directory: URL? = nil) -> URL {
        return writeQueue.sync {
            self.currentFileHandle?.closeFile()
            self.currentFileHandle = nil
            self.currentSessionFileURL = nil
            // Simulate a fresh process launch: new session UUID (so
            // rotation tests see a distinct filename even when the
            // timestamp clock hasn't ticked a full second).
            self.sessionId = UUID()
            if let directory {
                self.diagnosticsDirectory = directory
            } else {
                self.diagnosticsDirectory = LoggerState.defaultDiagnosticsDirectory()
            }
            self.installId = LoggerState.loadOrCreateInstallId(
                in: self.diagnosticsDirectory
            )
            return self.diagnosticsDirectory
        }
    }

    func flushForTesting() {
        writeQueue.sync {
            self.currentFileHandle?.synchronizeFile()
        }
    }

    // MARK: - Static helpers

    /// `Caches/Diagnostics/` for the current process, created on
    /// demand. Falls back to `NSTemporaryDirectory()` if `Caches` is
    /// unavailable (test environments occasionally lack it).
    private static func defaultDiagnosticsDirectory() -> URL {
        let caches = (try? FileManager.default.url(
            for: .cachesDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )) ?? URL(fileURLWithPath: NSTemporaryDirectory())
        return caches.appendingPathComponent(
            SurfaceStatusInvariantLogger.diagnosticsDirectoryName,
            isDirectory: true
        )
    }

    /// Filename-safe ISO-8601 timestamp: `yyyyMMddTHHmmssZ`.
    private static func filenameTimestamp(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        return formatter.string(from: date)
    }

    /// Per-install salt loader. Persists to a hidden file in the
    /// diagnostics directory so subsequent launches can deterministically
    /// re-hash the same episode IDs.
    private static func loadOrCreateInstallId(in directory: URL) -> String {
        let fm = FileManager.default
        try? fm.createDirectory(at: directory, withIntermediateDirectories: true)
        let saltURL = directory.appendingPathComponent(".surface-status-install-id")
        if let data = try? Data(contentsOf: saltURL),
           let existing = String(data: data, encoding: .utf8),
           !existing.isEmpty {
            return existing
        }
        let new = UUID().uuidString
        try? new.data(using: .utf8)?.write(to: saltURL, options: [.atomic])
        return new
    }
}
