// OPMLImportExportViewModel.swift
// playhead-2jo: transient state for the Settings OPML row group.
//
// The view model orchestrates an `OPMLService` invocation and tracks
// `isImporting` / `progressFraction` / `lastImportSummary` /
// `lastImportError` / `exportedFileURL`. It stays free of SwiftData and
// network types — production wiring binds the seams in SettingsView.
//
// `@Observable` is the project convention (mirrors `SettingsViewModel`,
// `BrowseViewModel`). `@MainActor` because the View consumes it
// directly.

import Foundation
import Observation

@Observable
@MainActor
public final class OPMLImportExportViewModel {

    // MARK: - Import state

    public private(set) var isImporting: Bool = false
    public private(set) var progressFraction: Double = 0
    public private(set) var progressDone: Int = 0
    public private(set) var progressTotal: Int = 0
    public private(set) var lastImportSummary: OPMLImportResult?
    public private(set) var lastImportError: String?

    // MARK: - Export state

    public private(set) var isExporting: Bool = false
    public private(set) var exportedFileURL: URL?
    public private(set) var lastExportError: String?

    public init() {}

    // MARK: - Reset

    /// Surface a file-picker / file-read failure into the same error
    /// slot `runImport` writes to, so the View only has one place to
    /// look.
    public func setImportError(_ reason: String) {
        lastImportError = reason
        lastImportSummary = nil
    }

    /// Clear all transient state. Settings calls this when the user
    /// dismisses the result-summary sheet.
    public func reset() {
        lastImportSummary = nil
        lastImportError = nil
        lastExportError = nil
        exportedFileURL = nil
        progressDone = 0
        progressTotal = 0
        progressFraction = 0
    }

    // MARK: - Import

    /// Run the full import pipeline against an in-memory OPML document.
    ///
    /// The seams are deliberately simple closures so `SettingsView` can
    /// bind them to the production `PodcastDiscoveryService` +
    /// `ModelContext` without forcing this view-model to import either.
    public func runImport(
        data: Data,
        exists: @escaping @Sendable (URL) async -> Bool,
        resolve: @escaping @Sendable (URL) async -> OPMLService.ResolveOutcome,
        persist: @escaping @Sendable (OPMLFeed) async -> Void,
        service: OPMLService = OPMLService()
    ) async {
        isImporting = true
        defer { isImporting = false }
        lastImportSummary = nil
        lastImportError = nil
        progressDone = 0
        progressTotal = 0
        progressFraction = 0

        let feeds: [OPMLFeed]
        do {
            feeds = try service.parseOPML(from: data)
        } catch {
            // `OPMLError.localizedDescription` is the user-facing copy.
            lastImportError = (error as? OPMLError)?.errorDescription
                ?? error.localizedDescription
            return
        }

        progressTotal = feeds.count
        let onProgress: @Sendable (Int, Int) -> Void = { [weak self] done, total in
            Task { @MainActor [weak self] in
                guard let self else { return }
                self.progressDone = done
                self.progressTotal = total
                self.progressFraction = total > 0 ? Double(done) / Double(total) : 0
            }
        }
        let result = await service.importFeeds(
            feeds,
            exists: exists,
            resolve: resolve,
            persist: persist,
            progress: onProgress
        )
        lastImportSummary = result
    }

    // MARK: - Export

    /// Serialize the supplied feeds into an OPML file under the system
    /// temp directory and store the URL in `exportedFileURL` so a
    /// SwiftUI `ShareLink(item:)` can pick it up.
    public func runExport(
        feeds: [OPMLFeed],
        documentTitle: String = "Playhead Subscriptions",
        service: OPMLService = OPMLService(),
        fileManager: FileManager = .default,
        directoryProvider: @escaping () -> URL = { FileManager.default.temporaryDirectory }
    ) async {
        isExporting = true
        defer { isExporting = false }
        lastExportError = nil
        exportedFileURL = nil

        let data = service.serializeOPML(feeds: feeds, documentTitle: documentTitle)

        let dir = directoryProvider().appendingPathComponent("opml-export", isDirectory: true)
        do {
            try fileManager.createDirectory(
                at: dir,
                withIntermediateDirectories: true
            )
        } catch {
            lastExportError = "Could not create export directory: \(error.localizedDescription)"
            return
        }

        // Stable filename so re-exports overwrite the previous file
        // (per the spec's "single rolling export" intent and the
        // `reExportOverwrites` test). The timestamp is embedded only in
        // the in-bundle title, not in the filename.
        let url = dir.appendingPathComponent("PlayheadSubscriptions.opml")
        do {
            try data.write(to: url, options: .atomic)
            exportedFileURL = url
        } catch {
            lastExportError = "Could not write OPML file: \(error.localizedDescription)"
        }
    }
}
