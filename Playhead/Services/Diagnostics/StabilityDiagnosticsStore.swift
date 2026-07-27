// StabilityDiagnosticsStore.swift
// Bounded, on-device ring buffer of `StabilityDiagnosticRecord`s.
//
// Scope: playhead-jw63.4 (crash + hang pipeline).
//
// ----- Local-only, by decision -----
//
// Nothing in this file transmits. The v1 destination for a crash or
// hang record is a file on the device, surfaced through the diagnostics
// bundle the user already chooses to send (`DiagnosticsExportCoordinator`).
// That is a deliberate choice, not an unfinished one:
//
//   * Playhead's on-device mandate makes "upload to a server" a legal
//     decision rather than a default. A crash reporter that phones home
//     is a second egress path to keep honest forever.
//   * Apple already runs the aggregate path for free. Users who leave
//     "Share With App Developers" on send crash + hang diagnostics to
//     App Store Connect, symbolicated, with no code and no consent
//     prompt of ours. That covers the population; this ring buffer
//     covers the individual who writes in.
//
// Consequence to be honest about: a crash on a device whose owner never
// exports a bundle is invisible here. App Store Connect is what closes
// that gap once the app is distributed.
//
// ----- Storage shape -----
//
// JSON Lines under `Application Support/Diagnostics/`, one record per
// line, most recent last, capped at `maxRecords`.
//
//   * Application Support, not Caches: the OS evicts Caches under disk
//     pressure, and disk pressure is precisely when a crash record is
//     most interesting.
//   * JSONL, not one JSON array: a truncated tail (power loss mid-write)
//     costs one record instead of the whole buffer, because the reader
//     skips undecodable lines.
//   * `.completeUntilFirstUserAuthentication`, matching `AnalysisStore`
//     and the download cache — MetricKit can deliver a payload during a
//     background launch when the device is locked but has been unlocked
//     once since boot.
//
// ----- Isolation -----
//
// An `actor`, so concurrent MetricKit delivery and a concurrent
// diagnostics export cannot interleave a read against a rewrite. The
// init performs NO file I/O (playhead-jncn convention): the directory
// is created lazily on first write.

import Foundation
import OSLog

actor StabilityDiagnosticsStore {

    // MARK: - Configuration

    /// Records retained. Fifty incidents is several weeks of a healthy
    /// app and still only a few hundred kilobytes at the 64-frame cap.
    static let maxRecords = 50

    /// Directory under `Application Support/`. Mirrors the `Diagnostics`
    /// name the Caches-based surface-status logger uses so the two are
    /// obviously siblings.
    static let directoryName = "Diagnostics"

    static let filename = "stability-diagnostics.jsonl"

    /// Production handle. Constructed lazily and never touches the file
    /// system until something is written or read, so referencing it from
    /// the launch path costs nothing.
    ///
    /// Tests MUST NOT use this — every test constructs its own instance
    /// against a temp directory, the same isolation rule
    /// `SurfaceStatusInvariantLogger` follows.
    static let shared = StabilityDiagnosticsStore()

    // MARK: - State

    private let explicitDirectory: URL?
    private let fileManager: FileManager
    private let logger = Logger(subsystem: "com.playhead", category: "StabilityDiagnostics")

    /// Cached after the first successful resolution so a repeated
    /// export does not re-derive the URL.
    private var resolvedDirectory: URL?

    // MARK: - Init

    /// - Parameter directory: when nil, resolves to
    ///   `Application Support/Diagnostics/` on first use. Tests pass a
    ///   unique temp directory.
    init(directory: URL? = nil, fileManager: FileManager = .default) {
        self.explicitDirectory = directory
        self.fileManager = fileManager
    }

    // MARK: - Read

    /// Most-recent-first records, newest at index 0.
    ///
    /// Ordering note: the file is written oldest-first (append order);
    /// this reverses so a support engineer reading the bundle sees the
    /// latest incident at the top, matching `scheduler_events`.
    func recent(limit: Int = StabilityDiagnosticsStore.maxRecords) -> [StabilityDiagnosticRecord] {
        guard limit > 0 else { return [] }
        return Array(loadAll().suffix(limit).reversed())
    }

    /// Everything in the buffer, oldest first. Exposed for tests and for
    /// callers that want append order.
    func all() -> [StabilityDiagnosticRecord] {
        loadAll()
    }

    // MARK: - Write

    /// Append records and re-apply the cap, keeping the newest
    /// `maxRecords`.
    ///
    /// Best-effort by design: an I/O failure is logged and swallowed. A
    /// diagnostics sink that throws into a MetricKit callback would turn
    /// a report about instability into a source of it.
    func append(_ records: [StabilityDiagnosticRecord]) {
        guard !records.isEmpty else { return }
        let merged = (loadAll() + records).suffix(Self.maxRecords)
        write(Array(merged))
    }

    /// Drop everything. Used by tests and available if a future settings
    /// affordance wants a "clear diagnostics" action.
    func removeAll() {
        write([])
    }

    // MARK: - Paths

    /// Resolve (and remember) the directory this store writes to.
    /// Returns nil only when `Application Support` itself cannot be
    /// located, which on iOS means the container is unavailable.
    private func directoryURL() -> URL? {
        if let resolvedDirectory { return resolvedDirectory }
        let url: URL?
        if let explicitDirectory {
            url = explicitDirectory
        } else {
            url = try? fileManager.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: false
            ).appendingPathComponent(Self.directoryName, isDirectory: true)
        }
        resolvedDirectory = url
        return url
    }

    private func fileURL() -> URL? {
        directoryURL()?.appendingPathComponent(Self.filename, isDirectory: false)
    }

    // MARK: - Serialisation

    private func loadAll() -> [StabilityDiagnosticRecord] {
        guard let url = fileURL(), let data = try? Data(contentsOf: url) else { return [] }
        let decoder = JSONDecoder()
        var out: [StabilityDiagnosticRecord] = []
        for line in data.split(separator: UInt8(ascii: "\n")) where !line.isEmpty {
            // Skip, don't fail: one corrupt line (a torn tail from a
            // power loss, or a record from a schema too new to read)
            // must not cost the whole buffer.
            guard let record = try? decoder.decode(
                StabilityDiagnosticRecord.self, from: Data(line)
            ) else { continue }
            out.append(record)
        }
        return out
    }

    private func write(_ records: [StabilityDiagnosticRecord]) {
        guard let directory = directoryURL(), let url = fileURL() else { return }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]

        var buffer = Data()
        for record in records {
            guard let line = try? encoder.encode(record) else { continue }
            buffer.append(line)
            buffer.append(UInt8(ascii: "\n"))
        }

        do {
            try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
            try buffer.write(to: url, options: [.atomic])
            // Re-apply protection after every write: `.atomic` replaces
            // the inode, so an attribute set once at create time would
            // silently stop applying on the next append.
            try fileManager.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: url.path
            )
        } catch {
            logger.error(
                "stability diagnostics write failed: \(error.localizedDescription, privacy: .public)"
            )
        }
    }
}
