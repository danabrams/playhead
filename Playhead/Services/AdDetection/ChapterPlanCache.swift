// ChapterPlanCache.swift
// playhead-au2v.1.1: Content-hash-keyed on-device cache for `ChapterPlan`
// artifacts produced by the chapter generation phase.
//
// Storage model: one JSON file per `episodeContentHash`, written under a
// dedicated subdirectory of `Application Support/`. Files are written
// atomically (`.atomicWrite`) so a partially-written plan never gets
// observed.
//
// Schema versioning: `ChapterPlan.schemaVersion` is checked on read. A
// mismatch is treated as a cache miss (we return `nil`); the chapter
// generation phase will then regenerate the plan and `put` the new
// version. We do not delete the stale file proactively — the next
// `put` for that key overwrites it atomically.
//
// Decode failure: a corrupted file (e.g. user copied bytes in, or an
// older incompatible version that didn't bump the schema) is treated
// as a cache miss. The error is logged at `.error` and the call returns
// `nil` rather than throwing — chapter plan retrieval is best-effort.
//
// Concurrency: the cache is an `actor`, so all reads/writes are
// serialized. Multiple concurrent `put`s for distinct keys are
// serialized but well-formed; concurrent `put`s for the same key
// produce one of the writes (last-writer-wins under actor ordering),
// never a corrupted file thanks to atomic write.
//
// The directory layout (one file per content hash) mirrors the
// `FoundationModelsFeedbackStore` pattern: lazy resolution on first
// use, optional injection for tests, `migrate()` for hot-path eager
// init from runtime.

import Foundation
import OSLog

actor ChapterPlanCache {

    // MARK: - Storage

    /// Directory injected by tests; nil in production triggers
    /// Application Support resolution on first use.
    private let directoryOverride: URL?
    private let fileManager: FileManager
    private let logger: Logger

    /// Lazily resolved directory URL. Set on first use to avoid the
    /// synchronous Application Support lookup at init time
    /// (matches `FoundationModelsFeedbackStore`'s `playhead-jncn`
    /// pattern).
    private var resolvedDirectory: URL?
    private var didEnsureDirectory = false

    /// JSON encoder/decoder are lazily-initialized constants. Default
    /// configuration matches existing on-device FM artifact caches.
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    // MARK: - Init

    init(
        directory: URL? = nil,
        fileManager: FileManager = .default,
        logger: Logger = Logger(subsystem: "com.playhead", category: "ChapterPlanCache")
    ) {
        self.directoryOverride = directory
        self.fileManager = fileManager
        self.logger = logger
        self.resolvedDirectory = nil
        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
    }

    /// Eagerly resolve the on-disk directory and create it. Idempotent.
    /// Production callers `await cache.migrate()` once at runtime
    /// startup to amortize the first-use cost off the hot path.
    func migrate() {
        _ = resolveDirectoryLocked()
        try? ensureDirectoryExists()
    }

    /// Read-only accessor for tests.
    var directory: URL { resolveDirectoryLocked() }

    // MARK: - Public API

    /// Look up a cached plan by `contentHash`.
    ///
    /// Returns `nil` for any of:
    /// - missing file (cache miss),
    /// - schema-version mismatch (treat as miss; phase will regenerate),
    /// - JSON decode failure (treat as miss; logged).
    func get(contentHash: String) -> ChapterPlan? {
        let url = fileURL(forContentHash: contentHash)
        guard fileManager.fileExists(atPath: url.path) else {
            return nil
        }

        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            logger.error(
                "chapterplan.cache.read_failed hash=\(contentHash, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
            return nil
        }

        let plan: ChapterPlan
        do {
            plan = try decoder.decode(ChapterPlan.self, from: data)
        } catch {
            logger.error(
                "chapterplan.cache.decode_failed hash=\(contentHash, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
            return nil
        }

        guard plan.schemaVersion == ChapterPlan.currentSchemaVersion else {
            logger.notice(
                "chapterplan.cache.schema_mismatch hash=\(contentHash, privacy: .public) found=\(plan.schemaVersion, privacy: .public) want=\(ChapterPlan.currentSchemaVersion, privacy: .public)"
            )
            return nil
        }

        return plan
    }

    /// Persist a plan keyed by its `episodeContentHash`. The
    /// `contentHash` parameter must match `plan.episodeContentHash`;
    /// a mismatch is logged and the call is a no-op so we never
    /// store a plan under a key that disagrees with its embedded hash.
    @discardableResult
    func put(contentHash: String, plan: ChapterPlan) -> Bool {
        guard contentHash == plan.episodeContentHash else {
            logger.error(
                "chapterplan.cache.put_key_mismatch key=\(contentHash, privacy: .public) plan=\(plan.episodeContentHash, privacy: .public)"
            )
            return false
        }

        do {
            try ensureDirectoryExists()
        } catch {
            logger.error(
                "chapterplan.cache.directory_failed error=\(error.localizedDescription, privacy: .public)"
            )
            return false
        }

        let url = fileURL(forContentHash: contentHash)
        let data: Data
        do {
            data = try encoder.encode(plan)
        } catch {
            logger.error(
                "chapterplan.cache.encode_failed hash=\(contentHash, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
            return false
        }

        do {
            try data.write(to: url, options: .atomic)
        } catch {
            logger.error(
                "chapterplan.cache.write_failed hash=\(contentHash, privacy: .public) path=\(url.path, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
            return false
        }

        logger.debug(
            "chapterplan.cache.put hash=\(contentHash, privacy: .public) chapters=\(plan.chapters.count, privacy: .public) bytes=\(data.count, privacy: .public)"
        )
        return true
    }

    /// Remove the cached plan for `contentHash`, if any. Missing
    /// files are not an error.
    func invalidate(contentHash: String) {
        let url = fileURL(forContentHash: contentHash)
        do {
            try fileManager.removeItem(at: url)
            logger.debug(
                "chapterplan.cache.invalidate hash=\(contentHash, privacy: .public)"
            )
        } catch {
            if let cocoa = error as? CocoaError, cocoa.code == .fileNoSuchFile {
                return
            }
            logger.error(
                "chapterplan.cache.invalidate_failed hash=\(contentHash, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
        }
    }

    // MARK: - Private

    private func resolveDirectoryLocked() -> URL {
        if let resolved = resolvedDirectory { return resolved }
        let url: URL
        if let override = directoryOverride {
            url = override
        } else {
            let base = (try? fileManager.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )) ?? fileManager.temporaryDirectory
            url = base.appendingPathComponent("ChapterPlanCache", isDirectory: true)
        }
        resolvedDirectory = url
        return url
    }

    private func ensureDirectoryExists() throws {
        if didEnsureDirectory { return }
        let dir = resolveDirectoryLocked()
        if !fileManager.fileExists(atPath: dir.path) {
            try fileManager.createDirectory(at: dir, withIntermediateDirectories: true)
        }
        didEnsureDirectory = true
    }

    /// Map a content-hash string to a stable on-disk file URL. The
    /// hash is sanitized before being used as a path component so
    /// callers cannot accidentally smuggle path separators (`/`),
    /// parent-dir tokens (`..`), or hidden-file leaders (`.`) through
    /// to the filesystem. We keep ASCII alphanumerics, `-`, and `_`;
    /// every other byte is folded to `_`. Empty hashes after
    /// sanitization fall back to `__empty__` so we never end up with
    /// a dotfile or an unnamed file.
    private func fileURL(forContentHash contentHash: String) -> URL {
        let safe = Self.sanitize(contentHash: contentHash)
        return resolveDirectoryLocked()
            .appendingPathComponent("\(safe).json", isDirectory: false)
    }

    /// Internal so tests can pin the sanitization contract.
    static func sanitize(contentHash: String) -> String {
        let allowed = Set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
        let mapped = contentHash.unicodeScalars.map { scalar -> Character in
            let char = Character(scalar)
            return allowed.contains(char) ? char : "_"
        }
        let result = String(mapped)
        return result.isEmpty ? "__empty__" : result
    }
}
