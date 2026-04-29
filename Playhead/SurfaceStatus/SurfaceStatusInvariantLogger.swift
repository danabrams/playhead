// SurfaceStatusInvariantLogger.swift
// Tier-B (production-safe, all-builds) logging channel for impossible-
// state violations from the surface-status reducer.
//
// Scope: playhead-ol05 (Phase 1.5 — "State-transition audit + impossible-
// state assertions + cross-target contract test").
//
// ----- Overview -----
//
// Anomaly-only: records entries ONLY when the reducer produces an
// invariant violation. Successful transitions are NOT logged — the
// JSON Lines file only ever contains violation entries. Consumed by
// `playhead-e2a3`'s anomaly-rate audit (pass criterion 1: "no
// impossible-state entries observed in production telemetry"), which
// measures the RATE of anomalies, not the full transition stream.
//
// The logger writes JSON Lines to a per-session file under
// `Caches/Diagnostics/`. Each (anomaly) line is exactly one
// `SurfaceStateTransitionEntry` whose `invariantViolation` payload is
// always populated. Files rotate per session — a new launch picks a
// fresh timestamped filename — and the logger evicts older sessions on
// startup so disk usage cannot grow without bound across long-lived
// dogfood sessions.
//
// Retention matches `playhead-ghon`'s scheduler_events 200-row tail:
// the most recent session files are kept, older ones are deleted on
// launch. (The exact numeric ceiling lives in `maxSessionFiles` below.)
//
// Thread-safety: every write goes through a serial `DispatchQueue` and
// is fire-and-forget from the caller's perspective. The reducer / batch
// validator does not block on the logger — log emission is best-effort.
//
// ----- Instance-based design (post-refactor) -----
//
// This logger is a REFERENCE TYPE constructed and owned by the
// composition root (`PlayheadRuntime`) and injected into every
// producer that emits to the JSONL stream (`SkipOrchestrator`,
// `EpisodeSurfaceStatusObserver`, the reducer's invariant paths).
// Each test that wants to read back from the stream constructs its
// OWN instance pointing at a temp directory — there is no shared
// mutable state across tests, so the cross-suite races that
// previously required a process-wide mutex are gone at the source.
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
// `episode_id_hash` is SHA-256 of `installID || episodeId`, hex-encoded.
// The installID salt is per-install (in production) / per-instance (in
// tests); an attacker without the salt cannot reverse the hash to
// recover the episode ID.

import Foundation

// MARK: - SurfaceStatusInvariantLogger

/// Tier-B production logging channel. Writes JSON Lines to a per-session
/// file under `Caches/Diagnostics/`. See the file-level overview for the
/// schema and retention policy.
///
/// Constructed by `PlayheadRuntime` (one instance per app) and injected
/// into every producer that writes to the surface-status JSONL stream.
/// Every instance owns its own write queue, session file, and install-ID
/// salt — there is no process-global logger state.
final class SurfaceStatusInvariantLogger: @unchecked Sendable {

    // MARK: - Configuration (shared by every instance)

    /// Maximum number of session files to retain. Older files are
    /// evicted on startup.
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

    private let state: LoggerState

    // MARK: - Init

    /// Construct a logger writing to `directory`. When nil, writes to
    /// `Caches/Diagnostics/`. Tests typically pass a unique temp dir so
    /// each test owns its own session file, install-ID salt, and write
    /// queue with no overlap across tests.
    ///
    /// playhead-jncn: this init body holds zero synchronous FileManager
    /// / FileHandle / `Data.write` calls. The directory create, the
    /// install-ID salt load, and the session-file open all defer to
    /// first use through `LoggerState`'s lazy bootstrap.
    /// `PlayheadRuntime` calls `await migrate()` from its deferred init
    /// Task to warm the bootstrap path off-main.
    init(directory: URL? = nil) {
        self.state = LoggerState(directory: directory)
    }

    /// playhead-jncn: lazy first-use bootstrap. Forces the install-ID
    /// salt load and the diagnostics-directory create + eviction sweep
    /// to run now (off-main, when called from the deferred init Task)
    /// rather than at the next `record(_:)` / `hashEpisodeId(_:)` call
    /// site. Idempotent — every subsequent call is a no-op.
    func migrate() {
        state.migrate()
    }

    // MARK: - Public API

    /// Append a single state-transition entry to the current session
    /// file. Fire-and-forget — the call returns as soon as the entry is
    /// enqueued on the serial write queue.
    ///
    /// Note: `entry.sessionId` will be OVERWRITTEN with the logger's
    /// current session ID.
    func record(_ entry: SurfaceStateTransitionEntry) {
        state.enqueueWriteWithCurrentSession(entry: entry)
    }

    /// Convenience: emit one entry per violation in the supplied list,
    /// reusing the supplied `context` for the non-violation fields.
    /// When `violations` is empty this is a no-op.
    func recordViolations(
        _ violations: [InvariantViolation],
        context: SurfaceStateTransitionContext
    ) {
        for violation in violations {
            state.enqueueViolation(context: context, violation: violation)
        }
    }

    /// Code-typed entry-point. The reducer's invariant call-sites pass a
    /// specific `InvariantViolation.Code` plus a human-readable
    /// `description`; the logger wraps both into a synthetic JSON Lines
    /// entry whose `invariantViolation` payload carries the supplied code
    /// verbatim. `playhead-e2a3`'s dogfood audit groups by code, so each
    /// caller MUST pass the code that actually describes its condition.
    ///
    /// playhead-glch: this replaced the pre-glch string-only entry-point
    /// that hard-coded `.reducerInternalBug` regardless of the message —
    /// silently miscategorizing every reducer-precedence impossibility
    /// into a single bucket. After glch, every reducer call-site passes a
    /// real code (`.userPausedUnknownCause`,
    /// `.resourceBlockUnknownCause`, `.transientWaitUnknownCause`,
    /// `.unmappedForwardCompatCause`).
    func invariantViolated(
        code: InvariantViolation.Code,
        description: String
    ) {
        state.enqueueSyntheticViolation(code: code, message: description)
    }

    /// Explicit "I don't have a code for this condition" fallback. Emits
    /// a synthetic violation tagged with `.unknown` so `playhead-e2a3`'s
    /// audit can surface the bucket as a follow-up signal — anyone whose
    /// site reaches `recordUnknown` should be filing a bead to introduce
    /// a real code.
    ///
    /// As of `playhead-glch` this has zero callers in production code.
    /// It exists ONLY so a future code path that genuinely lacks a code
    /// can opt in deliberately rather than silently re-introducing
    /// hard-coded miscategorization.
    func recordUnknown(_ description: String) {
        state.enqueueSyntheticViolation(code: .unknown, message: description)
    }

    // MARK: - playhead-o45p event emitters

    /// Append a `ready_entered` event to the session log. The reducer's
    /// consumer calls this when `EpisodeSurfaceStatus` transitions INTO
    /// a ready-for-playback disposition (queued + no blocking cause).
    /// Fire-and-forget.
    func recordReadyEntered(
        episodeIdHash: String?,
        trigger: SurfaceStateTransitionEntryTrigger?,
        timestamp: Date = Date()
    ) {
        state.enqueueReadyEntered(
            episodeIdHash: episodeIdHash,
            trigger: trigger,
            timestamp: timestamp
        )
    }

    /// Append an `auto_skip_fired` event to the session log.
    /// `SkipOrchestrator` calls this when its policy decides to apply an
    /// auto-skip at playhead-time. Fire-and-forget.
    func recordAutoSkipFired(
        episodeIdHash: String?,
        windowStartMs: Int,
        windowEndMs: Int,
        timestamp: Date = Date()
    ) {
        state.enqueueAutoSkipFired(
            episodeIdHash: episodeIdHash,
            windowStartMs: windowStartMs,
            windowEndMs: windowEndMs,
            timestamp: timestamp
        )
    }

    /// Hash an episode ID using this logger's install-ID salt. Every
    /// producer that stamps `episode_id_hash` onto an entry must route
    /// through the SAME logger instance so `false_ready_rate`'s
    /// numerator/denominator pairing by hash is byte-identical across
    /// producers.
    func hashEpisodeId(_ episodeId: String) -> String {
        return state.hashEpisodeId(episodeId)
    }

    // MARK: - Test-only introspection

    #if DEBUG
    /// Test hook: synchronously drain pending writes. Use after
    /// `record(_:)` to ensure the file reflects every emitted entry
    /// before reading it back.
    func flushForTesting() {
        state.flushForTesting()
    }

    /// Test hook: read the path of the current session file.
    var currentSessionFileURL: URL? {
        state.currentSessionFileURL
    }

    /// Test hook: read the session ID the logger is using. Reads on
    /// the serial write queue so the value is a consistent snapshot
    /// relative to any in-flight writes.
    var currentSessionId: UUID {
        state.currentSessionIdForTesting()
    }
    #endif
}

// MARK: - LoggerState

/// Backing state for `SurfaceStatusInvariantLogger`. Encapsulates the
/// serial write queue, current session file, and install-ID salt used
/// by the hasher.
///
/// Marked `@unchecked Sendable` because every mutable property is
/// accessed ONLY on the serial `writeQueue`. The install-ID salt is
/// immutable for the instance's lifetime.
private final class LoggerState: @unchecked Sendable {

    /// Serial queue: every write hops onto this before touching the file
    /// system. Sync-on-demand via `flushForTesting()`.
    private let writeQueue: DispatchQueue

    /// Per-session UUID stamped onto every entry and incorporated into
    /// the session filename so two simultaneously-launched processes
    /// cannot collide. Immutable for the instance's lifetime.
    private let sessionId: UUID

    /// playhead-jncn: lazy-resolved per-install salt. Loaded under
    /// `writeQueue` on first need (write or `hashEpisodeId`). The
    /// salt's value is stable for the instance's lifetime once
    /// resolved.
    private var resolvedInstallId: String?

    /// The session file URL. Lazily realized on first write so that a
    /// process that never logs anything does not create empty files.
    private(set) var currentSessionFileURL: URL?

    /// File handle for the current session. Lazily opened.
    private var currentFileHandle: FileHandle?

    /// playhead-jncn: lazy-resolved diagnostics directory. The
    /// `Caches/Diagnostics/` lookup (`FileManager.url(... create: true)`)
    /// runs on first write rather than at construction time so the
    /// `PlayheadRuntime.init` synchronous flow stays off-disk.
    private let directoryOverride: URL?
    private var resolvedDiagnosticsDirectory: URL?

    init(directory: URL?) {
        // playhead-jncn: store overrides only. Defer the
        // `Caches/Diagnostics/` resolution + install-ID salt load to
        // `migrate()` / `resolveDiagnosticsDirectoryLocked()` /
        // `resolveInstallIdLocked()` so this init body holds no
        // synchronous FileManager / FileHandle / Data write calls.
        self.writeQueue = DispatchQueue(
            label: "playhead.surface-status-invariant-logger",
            qos: .utility
        )
        self.sessionId = UUID()
        self.directoryOverride = directory
        self.resolvedDiagnosticsDirectory = nil
        self.resolvedInstallId = nil
    }

    /// playhead-jncn: bootstrap entry point. Forces the diagnostics
    /// directory + install-ID salt load to run now (off-main, when
    /// called from the deferred init Task) rather than at the next
    /// write. Idempotent. Runs on `writeQueue` so it serializes against
    /// every other reader/writer of the lazy state.
    func migrate() {
        writeQueue.sync {
            _ = self.resolveDiagnosticsDirectoryLocked()
            _ = self.resolveInstallIdLocked()
        }
    }

    /// Must run on `writeQueue`. Resolves (and caches) the diagnostics
    /// directory URL. Does NOT create the directory on disk — that is
    /// `ensureSessionFileLocked()`'s job. The lookup itself
    /// (`FileManager.url(.cachesDirectory, create: true)`) DOES create
    /// `Caches/` if it does not exist, but that is unavoidable on iOS.
    private func resolveDiagnosticsDirectoryLocked() -> URL {
        if let resolved = resolvedDiagnosticsDirectory { return resolved }
        let url = directoryOverride ?? LoggerState.defaultDiagnosticsDirectory()
        resolvedDiagnosticsDirectory = url
        return url
    }

    /// Must run on `writeQueue`. Resolves (and caches) the install-ID
    /// salt. Reads from disk on first call; subsequent calls return
    /// the cached value.
    private func resolveInstallIdLocked() -> String {
        if let resolved = resolvedInstallId { return resolved }
        let dir = resolveDiagnosticsDirectoryLocked()
        let id = LoggerState.loadOrCreateInstallId(in: dir)
        resolvedInstallId = id
        return id
    }

    // MARK: - Write path

    /// Enqueue a pre-built entry, overwriting its `sessionId` with the
    /// logger's current session ID.
    ///
    /// The rebuilt entry preserves every field the caller supplied,
    /// including the playhead-o45p additions (`eventType`, `entryTrigger`,
    /// `windowStartMs`, `windowEndMs`). Dropping any of them here would
    /// silently convert `readyEntered` / `autoSkipFired` events into
    /// default `.invariantViolation` entries on disk, collapsing the
    /// false_ready_rate metric's ability to distinguish event types.
    func enqueueWriteWithCurrentSession(entry: SurfaceStateTransitionEntry) {
        writeQueue.async { [weak self] in
            guard let self else { return }
            let stamped = SurfaceStateTransitionEntry(
                timestamp: entry.timestamp,
                sessionId: self.sessionId,
                episodeIdHash: entry.episodeIdHash,
                priorDisposition: entry.priorDisposition,
                newDisposition: entry.newDisposition,
                priorReason: entry.priorReason,
                newReason: entry.newReason,
                cause: entry.cause,
                eligibilitySnapshot: entry.eligibilitySnapshot,
                invariantViolation: entry.invariantViolation,
                eventType: entry.eventType,
                entryTrigger: entry.entryTrigger,
                windowStartMs: entry.windowStartMs,
                windowEndMs: entry.windowEndMs
            )
            self.writeLocked(stamped)
        }
    }

    /// Build + write a violation entry.
    func enqueueViolation(
        context: SurfaceStateTransitionContext,
        violation: InvariantViolation
    ) {
        writeQueue.async { [weak self] in
            guard let self else { return }
            let entry = SurfaceStateTransitionEntry(
                timestamp: context.timestamp,
                sessionId: self.sessionId,
                episodeIdHash: context.episodeIdHash,
                priorDisposition: context.priorDisposition,
                newDisposition: context.newDisposition,
                priorReason: context.priorReason,
                newReason: context.newReason,
                cause: context.cause,
                eligibilitySnapshot: context.eligibilitySnapshot,
                invariantViolation: violation
            )
            self.writeLocked(entry)
        }
    }

    /// Append a `ready_entered` event. Uses placeholder disposition/reason
    /// values (`.queued` / `.waitingForTime`) since ready-entered events
    /// are always associated with the "queued + no blocking cause"
    /// state — the `eventType` discriminator is what matters for audit
    /// aggregation.
    func enqueueReadyEntered(
        episodeIdHash: String?,
        trigger: SurfaceStateTransitionEntryTrigger?,
        timestamp: Date
    ) {
        writeQueue.async { [weak self] in
            guard let self else { return }
            let entry = SurfaceStateTransitionEntry(
                timestamp: timestamp,
                sessionId: self.sessionId,
                episodeIdHash: episodeIdHash,
                priorDisposition: nil,
                newDisposition: .queued,
                priorReason: nil,
                newReason: .waitingForTime,
                cause: nil,
                eligibilitySnapshot: nil,
                invariantViolation: nil,
                eventType: .readyEntered,
                entryTrigger: trigger,
                windowStartMs: nil,
                windowEndMs: nil
            )
            self.writeLocked(entry)
        }
    }

    /// Append an `auto_skip_fired` event. Uses placeholder disposition/reason
    /// values (`.queued` / `.waitingForTime`) since the event carries its
    /// own payload in `window_start_ms`/`window_end_ms`.
    func enqueueAutoSkipFired(
        episodeIdHash: String?,
        windowStartMs: Int,
        windowEndMs: Int,
        timestamp: Date
    ) {
        writeQueue.async { [weak self] in
            guard let self else { return }
            let entry = SurfaceStateTransitionEntry(
                timestamp: timestamp,
                sessionId: self.sessionId,
                episodeIdHash: episodeIdHash,
                priorDisposition: nil,
                newDisposition: .queued,
                priorReason: nil,
                newReason: .waitingForTime,
                cause: nil,
                eligibilitySnapshot: nil,
                invariantViolation: nil,
                eventType: .autoSkipFired,
                entryTrigger: nil,
                windowStartMs: windowStartMs,
                windowEndMs: windowEndMs
            )
            self.writeLocked(entry)
        }
    }

    /// Synthetic-violation entry point. Used by
    /// `SurfaceStatusInvariantLogger.invariantViolated(code:description:)`
    /// (and `recordUnknown(_:)`) to wrap a code + free-form message in a
    /// real JSON-shaped entry.
    ///
    /// playhead-glch: prior to this bead the code argument was hard-coded
    /// to `.reducerInternalBug`. Now every caller must supply the real
    /// code so e2a3's audit can group by condition.
    func enqueueSyntheticViolation(
        code: InvariantViolation.Code,
        message: String
    ) {
        let timestamp = Date()
        writeQueue.async { [weak self] in
            guard let self else { return }
            let entry = SurfaceStateTransitionEntry(
                timestamp: timestamp,
                sessionId: self.sessionId,
                episodeIdHash: nil,
                priorDisposition: nil,
                newDisposition: .failed,
                priorReason: nil,
                newReason: .couldntAnalyze,
                cause: nil,
                eligibilitySnapshot: nil,
                invariantViolation: InvariantViolation(
                    code: code,
                    description: message
                )
            )
            self.writeLocked(entry)
        }
    }

    /// Read the current sessionId. Immutable for the instance's
    /// lifetime, so no synchronization is needed — the sync hop is
    /// preserved only so callers see any in-flight writes' side effects
    /// before returning.
    func currentSessionIdForTesting() -> UUID {
        return writeQueue.sync { self.sessionId }
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

        let diagnosticsDirectory = resolveDiagnosticsDirectoryLocked()

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
        let diagnosticsDirectory = resolveDiagnosticsDirectoryLocked()
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
    /// `playhead-ghon` contract — both implementations produce
    /// byte-identical output for the same (installID, episodeId) pair.
    ///
    /// playhead-jncn: the install-ID salt is loaded lazily under
    /// `writeQueue.sync` so the synchronous call site contract
    /// (`hashEpisodeId(_:) -> String`) is preserved while the
    /// `LoggerState.init` body stays free of disk I/O. The first
    /// caller pays the load; subsequent calls return the cached salt.
    func hashEpisodeId(_ episodeId: String) -> String {
        let installId = writeQueue.sync { self.resolveInstallIdLocked() }
        return SurfaceStatusEpisodeIdHasher.hash(
            installId: installId,
            episodeId: episodeId
        )
    }

    // MARK: - Test-only helpers

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
