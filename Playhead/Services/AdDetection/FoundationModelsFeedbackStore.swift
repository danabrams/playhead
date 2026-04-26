// FoundationModelsFeedbackStore.swift
//
// Owns the lifecycle of `LanguageModelSession.logFeedbackAttachment` payloads
// captured when Apple's on-device safety classifier rejects a benign podcast
// advertising window or the refinement pass fails to decode structured output.
//
// Why this exists
// ---------------
// iOS 26.4 reintroduced an aggressive guardrail in `FoundationModels` that
// rejects benign commercial content (CVS pre-roll, vaccine reminders, retail
// pharmacy spots). The same content classified successfully on iOS 26.0–26.3,
// so this is a regression. Apple's `LanguageModelSession.GenerationError`
// message specifically asks callers to invoke `logFeedbackAttachment(...)` so
// the FoundationModels team gets a machine-readable record of the model state
// at the moment of refusal.
//
// `logFeedbackAttachment` is synchronous, non-throwing, and returns a
// `Foundation.Data` blob (NOT a URL — the framework does not write the
// attachment to disk for us). This store takes that `Data`, names it, writes
// it to a sandboxed directory under `Application Support/`, and tracks the
// resulting URLs so a DEBUG-only debug surface can hand them to the standard
// share sheet for attachment to Feedback Assistant reports.
//
// Privacy
// -------
// The attachment includes the FM transcript, which includes podcast prompt
// content. Per the project's on-device mandate that data must stay on-device.
// Apple's `logFeedbackAttachment` writes nothing to disk on its own — only
// this store does — and the only path that can export it is a DEBUG-only
// share sheet that the developer initiates explicitly.
//
// Release builds
// --------------
// Release builds do not instantiate this store at all (see `PlayheadRuntime`).
// The classifier accepts an optional `FoundationModelsFeedbackStore?` and
// no-ops capture when it is `nil`.

import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels
#endif

actor FoundationModelsFeedbackStore {

    /// Why a refusal/decode-failure attachment was captured. Used in the
    /// generated filename so listing the directory shows the cause without
    /// having to open each blob.
    enum CaptureKind: String, Sendable {
        case coarseRefusal = "coarse-refusal"
        case refinementDecodeFailure = "refinement-decode-failure"
        case refinementRefusal = "refinement-refusal"
    }

    /// playhead-jncn: lazily resolved on first write so the synchronous
    /// `Application Support` lookup (which calls `FileManager.url(...,
    /// create: true)` and triggers a directory create on first launch)
    /// does not run inside `PlayheadRuntime.init`. `directoryOverride`
    /// captures the test-supplied path; production paths defer the
    /// resolution to `resolveDirectoryLocked()`.
    private let directoryOverride: URL?
    private let fileManager: FileManager
    private let logger: Logger
    private var resolvedDirectory: URL?
    private var capturedURLs: [URL] = []
    private var didEnsureDirectory = false

    /// Read-only accessor used by tests and the in-actor write path.
    /// The first call may pay the Application Support lookup cost; once
    /// resolved the value is cached.
    var directory: URL {
        resolveDirectoryLocked()
    }

    init(
        directory: URL? = nil,
        fileManager: FileManager = .default,
        logger: Logger = Logger(subsystem: "com.playhead", category: "FoundationModelsFeedback")
    ) {
        // playhead-jncn: store overrides only. Defer the
        // Application Support lookup + directory creation to first use
        // via `migrate()` / `resolveDirectoryLocked()`. Mirrors the
        // AdCatalogStore.ensureOpen() pattern.
        self.directoryOverride = directory
        self.fileManager = fileManager
        self.logger = logger
    }

    /// playhead-jncn: lazy first-use bootstrap. Resolves the on-disk
    /// directory (Application Support lookup) and creates it. Idempotent.
    /// Production callers `await store.migrate()` from
    /// `PlayheadRuntime`'s deferred init Task so the heavy work runs
    /// off-main; tests that exercise `storeAttachment` directly hit the
    /// same path lazily through `ensureDirectoryExists()`.
    func migrate() {
        _ = resolveDirectoryLocked()
        try? ensureDirectoryExists()
    }

    /// Resolve the on-disk directory URL. First call resolves
    /// Application Support; subsequent calls return the cached URL.
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
            url = base.appendingPathComponent("FoundationModelsFeedback", isDirectory: true)
        }
        resolvedDirectory = url
        return url
    }

    /// Persist `data` (the return value of `LanguageModelSession.logFeedbackAttachment`)
    /// to disk and remember the URL. Failures are logged at `.error` and
    /// swallowed — feedback capture is best-effort and must never break
    /// production paths.
    func storeAttachment(
        _ data: Data,
        kind: CaptureKind,
        windowContext: String
    ) {
        guard !data.isEmpty else {
            logger.notice("fm.feedback.skip kind=\(kind.rawValue, privacy: .public) reason=empty-data")
            return
        }

        do {
            try ensureDirectoryExists()
            let filename = makeFilename(kind: kind, windowContext: windowContext)
            let url = directory.appendingPathComponent(filename)
            try data.write(to: url, options: .atomic)
            capturedURLs.append(url)
            logger.notice(
                "fm.feedback.captured kind=\(kind.rawValue, privacy: .public) window=\(windowContext, privacy: .public) bytes=\(data.count, privacy: .public) path=\(url.path, privacy: .public)"
            )
        } catch {
            logger.error(
                "fm.feedback.store_failed kind=\(kind.rawValue, privacy: .public) window=\(windowContext, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
        }
    }

    /// Returns all captured attachment file URLs in chronological order.
    func capturedAttachmentURLs() -> [URL] {
        refreshCapturedURLsFromDisk()
        return capturedURLs
    }

    /// Removes captured attachments from disk and from the in-memory list.
    /// Errors are reported per-file via the logger but otherwise swallowed —
    /// the in-memory list is always cleared so the UI never gets stuck.
    func clearCapturedAttachments() {
        refreshCapturedURLsFromDisk()
        for url in capturedURLs {
            do {
                try fileManager.removeItem(at: url)
            } catch {
                if let cocoaError = error as? CocoaError, cocoaError.code == .fileNoSuchFile {
                    continue
                }
                logger.error(
                    "fm.feedback.clear_failed path=\(url.path, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
                )
            }
        }
        capturedURLs.removeAll()
    }

    /// Default `desiredOutput` text for coarse-pass refusals. The string
    /// describes what the classifier should have produced so Apple's
    /// FoundationModels team can see the intent.
    static let coarseRefusalDesiredOutput = """
        Should classify the window as advertising/promotional content. \
        Podcast ads about pharmacies, vaccines, healthcare, retail, and \
        consumer products are routine commercial content that the previous \
        FoundationModels release (iOS 26.0–26.3) handled correctly. The \
        on-device classifier should return a CoarseScreeningSchema with \
        disposition=containsAd, not refuse the request.
        """

    /// Default `desiredOutput` text for refinement-pass decode failures.
    static let refinementDecodeFailureDesiredOutput = """
        Should produce a valid RefinementWindowSchema with structured ad \
        spans. The model emitted output that could not be decoded against \
        the @Generable schema, which indicates either a truncation or a \
        schema-conformance failure inside the on-device runtime.
        """

    /// Default `desiredOutput` text for refinement-pass refusals.
    static let refinementRefusalDesiredOutput = """
        Should produce a valid RefinementWindowSchema for the refined ad \
        span. The upstream coarse pass selected this window for refinement \
        because it may contain advertising or promotional content, but the \
        on-device safety classifier refused the refinement prompt anyway.
        """

    // MARK: - Private

    private func ensureDirectoryExists() throws {
        if didEnsureDirectory { return }
        if !fileManager.fileExists(atPath: directory.path) {
            try fileManager.createDirectory(
                at: directory,
                withIntermediateDirectories: true
            )
        }
        didEnsureDirectory = true
    }

    /// Rebuild the in-memory URL cache from the on-disk attachments.
    /// The directory is the source of truth so a new store instance after
    /// relaunch can enumerate attachments captured by a previous process.
    private func refreshCapturedURLsFromDisk() {
        do {
            guard fileManager.fileExists(atPath: directory.path) else {
                capturedURLs = []
                return
            }
            let urls = try fileManager.contentsOfDirectory(
                at: directory,
                includingPropertiesForKeys: [.isRegularFileKey],
                options: [.skipsHiddenFiles]
            )
            let filtered = urls.filter { url in
                guard url.lastPathComponent.hasSuffix(".feedbackAttachment") else { return false }
                let values = try? url.resourceValues(forKeys: [.isRegularFileKey])
                return values?.isRegularFile ?? false
            }
            capturedURLs = filtered.sorted { lhs, rhs in
                let lhsDate = try? lhs.resourceValues(forKeys: [.creationDateKey]).creationDate
                let rhsDate = try? rhs.resourceValues(forKeys: [.creationDateKey]).creationDate
                switch (lhsDate, rhsDate) {
                case let (lhsDate?, rhsDate?) where lhsDate != rhsDate:
                    return lhsDate < rhsDate
                default:
                    return lhs.lastPathComponent < rhs.lastPathComponent
                }
            }
        } catch {
            logger.error(
                "fm.feedback.refresh_failed path=\(self.directory.path, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
        }
    }

    private func makeFilename(kind: CaptureKind, windowContext: String) -> String {
        let timestamp = Self.filenameTimestampFormatter.string(from: Date())
        let safeContext = windowContext
            .replacingOccurrences(of: " ", with: "_")
            .replacingOccurrences(of: "/", with: "-")
            .replacingOccurrences(of: ":", with: "-")
        let unique = UUID().uuidString.prefix(8)
        return "\(timestamp)_\(kind.rawValue)_\(safeContext)_\(unique).feedbackAttachment"
    }

    private static let filenameTimestampFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "UTC")
        formatter.dateFormat = "yyyyMMdd-HHmmss"
        return formatter
    }()
}
