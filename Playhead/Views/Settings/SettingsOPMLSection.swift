// SettingsOPMLSection.swift
// playhead-2jo: Subscriptions group on Settings — OPML import/export.
//
// The section attaches to `SettingsView` via an extension so it can
// reuse the file-scoped @State / @Environment plumbing already there.
// Production wiring binds the OPMLImportExportViewModel seams to the
// real PodcastDiscoveryService + the @Query Podcast list; tests use the
// view-model directly without rendering this view.

import SwiftUI
import SwiftData
import UniformTypeIdentifiers

extension SettingsView {

    /// OPML import/export section. Sits between Storage and Purchase in
    /// the Settings list.
    var opmlSection: some View {
        Section {
            opmlImportRow
            opmlExportRow
            if let summary = opmlViewModel.lastImportSummary {
                opmlImportSummaryRow(summary)
            }
            if let error = opmlViewModel.lastImportError {
                Text(error)
                    .font(AppTypography.caption)
                    .foregroundStyle(.red)
                    .listRowBackground(AppColors.surface)
                    .accessibilityIdentifier("Settings.opml.importError")
            }
            if let exportError = opmlViewModel.lastExportError {
                Text(exportError)
                    .font(AppTypography.caption)
                    .foregroundStyle(.red)
                    .listRowBackground(AppColors.surface)
                    .accessibilityIdentifier("Settings.opml.exportError")
            }
        } header: {
            Text("Subscriptions")
                .font(AppTypography.sans(size: 13, weight: .semibold))
                .foregroundStyle(AppColors.textSecondary)
                .textCase(nil)
        } footer: {
            Text("OPML carries only feed subscriptions — playback positions and downloads are not included.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
        }
    }

    // MARK: - Rows

    @ViewBuilder
    private var opmlImportRow: some View {
        Button {
            opmlImporterPresented = true
        } label: {
            HStack {
                Label("Import from OPML", systemImage: "square.and.arrow.down")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.accent)
                Spacer()
                if opmlViewModel.isImporting {
                    if opmlViewModel.progressTotal > 0 {
                        Text("\(opmlViewModel.progressDone)/\(opmlViewModel.progressTotal)")
                            .font(AppTypography.caption)
                            .foregroundStyle(AppColors.textTertiary)
                            .accessibilityIdentifier("Settings.opml.progressLabel")
                    }
                    ProgressView().controlSize(.small)
                }
            }
        }
        .buttonStyle(.plain)
        .disabled(opmlViewModel.isImporting)
        .listRowBackground(AppColors.surface)
        .accessibilityIdentifier("Settings.opml.importButton")
        .fileImporter(
            isPresented: $opmlImporterPresented,
            allowedContentTypes: SettingsOPMLContentTypes.allowed,
            allowsMultipleSelection: false
        ) { result in
            handleOPMLImporterResult(result)
        }
    }

    @ViewBuilder
    private var opmlExportRow: some View {
        if let url = opmlViewModel.exportedFileURL {
            ShareLink(item: url) {
                HStack {
                    Label("Share Subscriptions OPML", systemImage: "square.and.arrow.up")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)
                    Spacer()
                    Image(systemName: "doc")
                        .foregroundStyle(AppColors.textTertiary)
                        .accessibilityHidden(true)
                }
            }
            .listRowBackground(AppColors.surface)
            .accessibilityIdentifier("Settings.opml.shareButton")
        } else {
            Button {
                Task { await runOPMLExport() }
            } label: {
                HStack {
                    Label("Export Subscriptions", systemImage: "square.and.arrow.up")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)
                    Spacer()
                    if opmlViewModel.isExporting {
                        ProgressView().controlSize(.small)
                    }
                }
            }
            .buttonStyle(.plain)
            .disabled(opmlViewModel.isExporting)
            .listRowBackground(AppColors.surface)
            .accessibilityIdentifier("Settings.opml.exportButton")
        }
    }

    @ViewBuilder
    private func opmlImportSummaryRow(_ summary: OPMLImportResult) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(opmlSummaryHeadline(summary))
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textPrimary)
            if !summary.failed.isEmpty {
                ForEach(Array(summary.failed.prefix(3).enumerated()), id: \.offset) { _, failure in
                    Text("• \(failure.url.absoluteString) — \(failure.reason)")
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textTertiary)
                        .lineLimit(2)
                        .truncationMode(.middle)
                }
                if summary.failed.count > 3 {
                    Text("… and \(summary.failed.count - 3) more")
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textTertiary)
                }
            }
        }
        .listRowBackground(AppColors.surface)
        .accessibilityElement(children: .combine)
        .accessibilityIdentifier("Settings.opml.summary")
    }

    /// Headline copy for the import-summary row.
    /// Pure function so it can be re-used in unit tests.
    func opmlSummaryHeadline(_ summary: OPMLImportResult) -> String {
        switch (summary.imported, summary.skippedDuplicate, summary.failed.count) {
        case (let i, 0, 0):
            return "Imported \(i) podcast\(i == 1 ? "" : "s")."
        case (let i, let d, 0):
            return "Imported \(i), \(d) already in library."
        case (let i, 0, let f):
            return "Imported \(i). \(f) could not be found."
        case (let i, let d, let f):
            return "Imported \(i), \(d) duplicate, \(f) failed."
        }
    }

    // MARK: - Actions

    private func handleOPMLImporterResult(_ result: Result<[URL], Error>) {
        switch result {
        case .success(let urls):
            guard let url = urls.first else { return }
            Task { await runOPMLImport(from: url) }
        case .failure(let error):
            opmlViewModel.setImportError(error.localizedDescription)
        }
    }

    @MainActor
    private func runOPMLImport(from url: URL) async {
        // Security-scoped resource: required because file pickers hand
        // back URLs outside our sandbox.
        let scoped = url.startAccessingSecurityScopedResource()
        defer { if scoped { url.stopAccessingSecurityScopedResource() } }

        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            opmlViewModel.setImportError("Could not read file: \(error.localizedDescription)")
            return
        }

        let bridge = SettingsOPMLImportBridge(
            modelContext: self.modelContext,
            discoveryService: PodcastDiscoveryService(),
            iCloudSyncCoordinator: runtime.iCloudSyncCoordinator
        )

        await opmlViewModel.runImport(
            data: data,
            exists: { feedURL in await bridge.exists(feedURL: feedURL) },
            resolve: { feedURL in await bridge.resolve(feedURL: feedURL) },
            persist: { feed in await bridge.persist(feed: feed) }
        )
    }

    @MainActor
    private func runOPMLExport() async {
        let descriptor = FetchDescriptor<Podcast>()
        let podcasts = (try? modelContext.fetch(descriptor)) ?? []
        let feeds = podcasts.map {
            OPMLFeed(title: $0.title.isEmpty ? nil : $0.title, xmlUrl: $0.feedURL)
        }
        await opmlViewModel.runExport(feeds: feeds)
    }
}

// MARK: - Import Bridge

/// MainActor-bound bridge that exposes the `exists` / `resolve` /
/// `persist` seams `OPMLImportExportViewModel.runImport` expects.
///
/// We hide `ModelContext` (which is not Sendable) behind this MainActor
/// class. The view-model captures the bridge by reference, and every
/// seam call hops back to the main actor via the implicit MainActor
/// isolation, so the captured ModelContext never crosses an actor
/// boundary unprotected.
@MainActor
final class SettingsOPMLImportBridge {
    let modelContext: ModelContext
    let discoveryService: PodcastDiscoveryService
    /// playhead-5c1t: optional so previews / unit tests that don't
    /// exercise the iCloud writer-tap can construct the bridge without
    /// a coordinator. Production wiring always passes the runtime's
    /// coordinator; the writer-tap fires fire-and-forget when present.
    let iCloudSyncCoordinator: ICloudSyncCoordinator?

    init(
        modelContext: ModelContext,
        discoveryService: PodcastDiscoveryService,
        iCloudSyncCoordinator: ICloudSyncCoordinator? = nil
    ) {
        self.modelContext = modelContext
        self.discoveryService = discoveryService
        self.iCloudSyncCoordinator = iCloudSyncCoordinator
    }

    func exists(feedURL: URL) -> Bool {
        let descriptor = FetchDescriptor<Podcast>()
        let all = (try? modelContext.fetch(descriptor)) ?? []
        return all.contains { $0.feedURL == feedURL }
    }

    func resolve(feedURL: URL) async -> OPMLService.ResolveOutcome {
        do {
            _ = try await discoveryService.fetchFeed(url: feedURL)
            return .success(())
        } catch {
            return .failure(error.localizedDescription)
        }
    }

    func persist(feed: OPMLFeed) async {
        // Re-parse via the discoveryService.persist path so the
        // upsert + episode-merge logic stays in one place. Network
        // flake between resolve and persist is swallowed — the user's
        // progress count already advanced, and a second import attempt
        // will retry the missing feed via the dedup logic.
        do {
            let parsed = try await discoveryService.fetchFeed(url: feed.xmlUrl)
            let podcast = discoveryService.persist(parsed, from: feed.xmlUrl, in: modelContext)
            // playhead-5c1t: writer-tap. Each OPML-imported feed is
            // also pushed to iCloud so the import propagates to the
            // user's other devices. Fire-and-forget; the coordinator
            // handles offline / not-signed-in queueing.
            if let coordinator = iCloudSyncCoordinator {
                let record = SubscriptionLibrary.subscribedRecord(for: podcast)
                Task.detached {
                    _ = try? await coordinator.upsertSubscriptionMerging(record)
                }
            }
        } catch {
            // Intentional swallow.
        }
    }
}

// MARK: - Content Types

/// Centralized UTType list so test code can re-use it without
/// re-importing UniformTypeIdentifiers.
enum SettingsOPMLContentTypes {
    static let allowed: [UTType] = {
        // The `.opml` UTI may not be registered on iOS, so we fall back
        // to declaring it ourselves. Permissive xml fallback ensures
        // exports from apps that hand out the file as `text/xml` still
        // pass the picker filter.
        var types: [UTType] = []
        if let opml = UTType(filenameExtension: "opml") {
            types.append(opml)
        }
        types.append(.xml)
        return types
    }()
}

