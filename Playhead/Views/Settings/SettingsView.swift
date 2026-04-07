// SettingsView.swift
// Settings screen with all user-configurable options.
//
// Sections:
// - Speech model selection with download management and size indicators
// - Ad skip behavior (auto/manual/off)
// - Playback defaults (speed, skip intervals)
// - Storage management (transcript cache, model files, cached audio)
// - Background processing preferences
// - Restore Purchases

import SwiftUI
import SwiftData
#if canImport(Speech)
import Speech
#endif

// MARK: - SettingsView

struct SettingsView: View {

    @Query private var allPreferences: [UserPreferences]
    @Environment(\.modelContext) private var modelContext
    @Environment(PlayheadRuntime.self) private var runtime

    @State private var viewModel = SettingsViewModel()

    /// Resolved preferences, loaded in .onAppear to avoid SwiftData
    /// inserts during body evaluation.
    @State private var preferences: UserPreferences?

    #if DEBUG
    /// Active debug export ready to share via ShareLink sheet.
    @State private var debugExport: DebugEpisodeExport?
    @State private var debugExportInProgress = false
    /// bd-fmfb: cached list of `LanguageModelSession.logFeedbackAttachment`
    /// payload URLs captured by the feedback store. Refreshed via `.task`.
    @State private var fmFeedbackAttachmentURLs: [URL] = []
    #endif

    /// Injected dependencies — set via environment or passed directly.
    var inventory: ModelInventory?
    var assetProvider: AssetProvider?
    var entitlementManager: EntitlementManager?

    var body: some View {
        NavigationStack {
            List {
                if let prefs = preferences {
                    modelSection
                    adSkipSection(prefs)
                    playbackSection(prefs)
                    backgroundSection(prefs)
                    storageSection
                    purchasesSection
                    #if DEBUG
                    debugSection
                    fmFeedbackSection
                    #endif
                }
            }
            .listStyle(.insetGrouped)
            .scrollContentBackground(.hidden)
            .background(AppColors.background)
            .navigationTitle("Settings")
            .navigationBarTitleDisplayMode(.large)
            .onAppear {
                if preferences == nil {
                    if let existing = allPreferences.first {
                        preferences = existing
                    } else {
                        let fresh = UserPreferences()
                        modelContext.insert(fresh)
                        preferences = fresh
                    }
                }
            }
            .task {
                await viewModel.computeStorageSizes()
                if let inventory {
                    await viewModel.refreshModelStatuses(inventory: inventory)
                }
            }
        }
    }

}

// MARK: - Model Selection Section

private extension SettingsView {

    var modelSection: some View {
        Section {
            if usesSystemSpeechAssets {
                VStack(alignment: .leading, spacing: Spacing.xxs) {
                    Text("Apple Speech")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.text)
                    Text("Speech assets are managed by iOS and stay on device.")
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.secondary)
                }
                .padding(.vertical, Spacing.xxs)
                .listRowBackground(AppColors.surface)
            } else if viewModel.modelStatuses.isEmpty {
                Text("No models in manifest")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.secondary)
            } else {
                ForEach(viewModel.modelStatuses, id: \.0.id) { entry, status in
                    modelRow(entry: entry, status: status)
                }
            }
        } header: {
            sectionHeader("Models")
        } footer: {
            Text("On-device analysis assets never leave your device.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.metadata)
        }
    }

    @ViewBuilder
    func modelRow(entry: ModelEntry, status: ModelStatus) -> some View {
        HStack {
            VStack(alignment: .leading, spacing: Spacing.xxs) {
                Text(entry.displayName)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.text)

                HStack(spacing: Spacing.xs) {
                    Text(entry.role.rawValue)
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.metadata)

                    Text(SettingsViewModel.formattedSize(entry.uncompressedSizeBytes))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.metadata)
                }
            }

            Spacer()

            modelStatusBadge(entry: entry, status: status)
        }
        .padding(.vertical, Spacing.xxs)
        .listRowBackground(AppColors.surface)
        .accessibilityElement(children: .combine)
    }

    @ViewBuilder
    func modelStatusBadge(entry: ModelEntry, status: ModelStatus) -> some View {
        switch status {
        case .missing:
            Button {
                Task {
                    await downloadAndActivate(entry: entry)
                }
            } label: {
                Label("Download", systemImage: "arrow.down.circle")
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.accent)
            }
            .buttonStyle(.plain)
            .accessibilityLabel("Download \(entry.displayName)")

        case .downloading(let progress):
            HStack(spacing: Spacing.xs) {
                ProgressView(value: progress)
                    .tint(AppColors.accent)
                    .frame(width: 60)
                Text("\(Int(progress * 100))%")
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.metadata)
            }
            .accessibilityValue("Downloading: \(Int(progress * 100)) percent")

        case .staged:
            Button {
                Task {
                    await promoteStaged(entry: entry)
                }
            } label: {
                Label("Activate", systemImage: "checkmark.circle")
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.accent)
            }
            .buttonStyle(.plain)
            .accessibilityLabel("Activate \(entry.displayName)")

        case .ready(let version):
            HStack(spacing: Spacing.xs) {
                Text("v\(version)")
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.secondary)

                if viewModel.isDeletingModel == entry.id {
                    ProgressView()
                        .controlSize(.small)
                } else {
                    Button {
                        Task {
                            guard let inventory else { return }
                            await viewModel.deleteModel(entry: entry, inventory: inventory)
                        }
                    } label: {
                        Image(systemName: "trash")
                            .font(AppTypography.caption)
                            .foregroundStyle(.red.opacity(0.8))
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel("Delete \(entry.displayName)")
                }
            }

        case .updateAvailable(let current, let new):
            VStack(alignment: .trailing, spacing: 4) {
                Text("v\(current)")
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.metadata)
                Text("v\(new) available")
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.accent)
                Button {
                    Task {
                        await promoteStaged(entry: entry)
                    }
                } label: {
                    Label("Update", systemImage: "arrow.up.circle")
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.accent)
                }
                .buttonStyle(.plain)
                .accessibilityLabel("Update \(entry.displayName) to version \(new)")
            }
        }
    }

    func downloadAndActivate(entry: ModelEntry) async {
        guard let assetProvider, let inventory else { return }
        do {
            try await assetProvider.download(entry: entry)
            try await assetProvider.promote(modelId: entry.id)
        } catch {
            // The inventory refresh below will surface the latest state.
        }

        await viewModel.refreshModelStatuses(inventory: inventory)
        await viewModel.computeStorageSizes()
    }

    func promoteStaged(entry: ModelEntry) async {
        guard let assetProvider, let inventory else { return }
        do {
            try await assetProvider.promote(modelId: entry.id)
        } catch {
            // A missing staged file or similar lifecycle issue will be
            // reflected by the refreshed inventory state below.
        }

        await viewModel.refreshModelStatuses(inventory: inventory)
        await viewModel.computeStorageSizes()
    }

    var usesSystemSpeechAssets: Bool {
#if canImport(Speech)
        let env = ProcessInfo.processInfo.environment
        let usesStubSpeech =
            env["XCTestConfigurationFilePath"] != nil ||
            env["XCODE_RUNNING_FOR_PREVIEWS"] == "1" ||
            env["PLAYHEAD_USE_STUB_SPEECH"] == "1"
        return !usesStubSpeech
#else
        return false
#endif
    }
}

// MARK: - Ad Skip Section

private extension SettingsView {

    func adSkipSection(_ prefs: UserPreferences) -> some View {
        Section {
            Picker(selection: Binding(
                get: { prefs.skipBehavior },
                set: { prefs.skipBehavior = $0 }
            )) {
                ForEach(SkipBehavior.allCases, id: \.self) { behavior in
                    Text(behavior.displayName)
                        .tag(behavior)
                }
            } label: {
                Label("Ad Skip", systemImage: "forward.fill")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.text)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityLabel("Ad skip mode")
            .accessibilityValue(prefs.skipBehavior.displayName)
        } header: {
            sectionHeader("Ad Detection")
        } footer: {
            Text(skipBehaviorFooter(prefs))
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.metadata)
        }
    }

    func skipBehaviorFooter(_ prefs: UserPreferences) -> String {
        switch prefs.skipBehavior {
        case .auto:
            "Detected ads are skipped automatically during playback."
        case .manual:
            "Ad segments are highlighted on the timeline. Tap to skip."
        case .off:
            "Ad detection is disabled. Playback is uninterrupted."
        }
    }
}

// MARK: - Playback Defaults Section

private extension SettingsView {

    func playbackSection(_ prefs: UserPreferences) -> some View {
        Section {
            // Playback speed
            VStack(alignment: .leading, spacing: Spacing.xs) {
                HStack {
                    Label("Playback Speed", systemImage: "gauge.with.needle")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.text)
                    Spacer()
                    Text(String(format: "%.1fx", prefs.playbackSpeed))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.accent)
                }

                Slider(
                    value: Binding(
                        get: { prefs.playbackSpeed },
                        set: { prefs.playbackSpeed = $0 }
                    ),
                    in: 0.5...3.0,
                    step: 0.1
                )
                .tint(AppColors.accent)
                .accessibilityValue(String(format: "%.1f times", prefs.playbackSpeed))
            }
            .listRowBackground(AppColors.surface)

            // Forward skip interval
            HStack {
                Label("Skip Forward", systemImage: "goforward")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.text)
                Spacer()
                Picker("", selection: Binding(
                    get: { prefs.skipIntervals.forwardSeconds },
                    set: { prefs.skipIntervals.forwardSeconds = $0 }
                )) {
                    ForEach(skipIntervalOptions, id: \.self) { seconds in
                        Text("\(Int(seconds))s").tag(seconds)
                    }
                }
                .pickerStyle(.menu)
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityLabel("Skip forward interval")
            .accessibilityValue("\(Int(prefs.skipIntervals.forwardSeconds)) seconds")

            // Backward skip interval
            HStack {
                Label("Skip Back", systemImage: "gobackward")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.text)
                Spacer()
                Picker("", selection: Binding(
                    get: { prefs.skipIntervals.backwardSeconds },
                    set: { prefs.skipIntervals.backwardSeconds = $0 }
                )) {
                    ForEach(skipIntervalOptions, id: \.self) { seconds in
                        Text("\(Int(seconds))s").tag(seconds)
                    }
                }
                .pickerStyle(.menu)
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityLabel("Skip back interval")
            .accessibilityValue("\(Int(prefs.skipIntervals.backwardSeconds)) seconds")
        } header: {
            sectionHeader("Playback")
        }
    }

    var skipIntervalOptions: [TimeInterval] {
        [5, 10, 15, 30, 45, 60]
    }
}

// MARK: - Background Processing Section

private extension SettingsView {

    func backgroundSection(_ prefs: UserPreferences) -> some View {
        Section {
            Toggle(isOn: Binding(
                get: { prefs.backgroundProcessingEnabled },
                set: { prefs.backgroundProcessingEnabled = $0 }
            )) {
                Label("Background Processing", systemImage: "arrow.triangle.2.circlepath")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.text)
            }
            .tint(AppColors.accent)
            .listRowBackground(AppColors.surface)
        } header: {
            sectionHeader("Processing")
        } footer: {
            Text("When enabled, episodes are transcribed and analyzed in the background. Requires sufficient battery and may use network for model downloads.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.metadata)
        }
    }
}

// MARK: - Storage Section

private extension SettingsView {

    var storageSection: some View {
        Section {
            storageRow(
                label: "Model Files",
                icon: "cpu",
                size: viewModel.modelFilesSize
            )

            storageRow(
                label: "Transcript Cache",
                icon: "doc.text",
                size: viewModel.transcriptCacheSize,
                clearAction: {
                    Task { await viewModel.clearTranscriptCache() }
                }
            )

            storageRow(
                label: "Cached Audio",
                icon: "waveform",
                size: viewModel.cachedAudioSize,
                clearAction: {
                    Task { await viewModel.clearAudioCache() }
                }
            )
        } header: {
            sectionHeader("Storage")
        }
    }

    func storageRow(
        label: String,
        icon: String,
        size: Int64,
        clearAction: (() -> Void)? = nil
    ) -> some View {
        HStack {
            Label(label, systemImage: icon)
                .font(AppTypography.body)
                .foregroundStyle(AppColors.text)

            Spacer()

            Text(SettingsViewModel.formattedSize(size))
                .font(AppTypography.timestamp)
                .foregroundStyle(AppColors.metadata)

            if let clearAction, size > 0 {
                Button("Clear", action: clearAction)
                    .font(AppTypography.caption)
                    .foregroundStyle(.red.opacity(0.8))
                    .buttonStyle(.plain)
                    .accessibilityLabel("Clear \(label)")
            }
        }
        .listRowBackground(AppColors.surface)
        .accessibilityElement(children: .combine)
    }
}

// MARK: - Purchases Section

private extension SettingsView {

    var purchasesSection: some View {
        Section {
            Button {
                Task {
                    guard let entitlementManager else { return }
                    await viewModel.restorePurchases(entitlementManager: entitlementManager)
                }
            } label: {
                HStack {
                    Label("Restore Purchases", systemImage: "arrow.clockwise")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)

                    Spacer()

                    if viewModel.isRestoring {
                        ProgressView()
                            .controlSize(.small)
                    } else if viewModel.restoreSucceeded {
                        Image(systemName: "checkmark.circle.fill")
                            .foregroundStyle(.green)
                            .accessibilityLabel("Restore successful")
                    }
                }
            }
            .buttonStyle(.plain)
            .disabled(viewModel.isRestoring)
            .listRowBackground(AppColors.surface)
            .accessibilityLabel("Restore purchases")

            if let error = viewModel.restoreError {
                Text(error)
                    .font(AppTypography.caption)
                    .foregroundStyle(.red)
                    .listRowBackground(AppColors.surface)
            }
        } header: {
            sectionHeader("Purchases")
        }
    }
}

// MARK: - Debug Section (DEBUG builds only)

#if DEBUG
private extension SettingsView {

    var debugSection: some View {
        Section {
            Button {
                Task { await generateDebugExport() }
            } label: {
                HStack {
                    Label("Export Current Episode", systemImage: "square.and.arrow.up")
                        .font(AppTypography.body)
                        .foregroundStyle(hasCurrentEpisode ? AppColors.accent : AppColors.secondary)

                    Spacer()

                    if debugExportInProgress {
                        ProgressView().controlSize(.small)
                    }
                }
            }
            .buttonStyle(.plain)
            .disabled(!hasCurrentEpisode || debugExportInProgress)
            .listRowBackground(AppColors.surface)

            Button {
                Task { await generateLibraryExport() }
            } label: {
                HStack {
                    Label("Export Entire Library", systemImage: "square.and.arrow.up.on.square")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)

                    Spacer()

                    if debugExportInProgress {
                        ProgressView().controlSize(.small)
                    }
                }
            }
            .buttonStyle(.plain)
            .disabled(debugExportInProgress)
            .listRowBackground(AppColors.surface)

            if let export = debugExport {
                ShareLink(
                    item: export,
                    preview: SharePreview(
                        "Playhead Debug Export",
                        image: Image(systemName: "doc.text")
                    )
                ) {
                    Label("Share Export", systemImage: "square.and.arrow.up.circle.fill")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)
                }
                .listRowBackground(AppColors.surface)

                Text(export.filename)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.metadata)
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .listRowBackground(AppColors.surface)
            }

            if !hasCurrentEpisode {
                Text("Start playing an episode to enable export.")
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.metadata)
                    .listRowBackground(AppColors.surface)
            }
        } header: {
            sectionHeader("Debug")
        } footer: {
            Text("Exports transcript, detected ads, evidence catalog, feature summary, and acoustic breaks for the current episode. DEBUG builds only.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.metadata)
        }
    }

    var hasCurrentEpisode: Bool {
        runtime.currentAnalysisAssetId != nil
    }

    @MainActor
    func generateDebugExport() async {
        guard
            let assetId = runtime.currentAnalysisAssetId,
            let episodeId = runtime.currentEpisodeId
        else { return }

        debugExportInProgress = true
        defer { debugExportInProgress = false }

        let title = runtime.currentEpisodeTitle ?? "Unknown Episode"
        let podcast = runtime.currentPodcastTitle ?? "Unknown Podcast"

        debugExport = await DebugEpisodeExporter.build(
            episodeTitle: title,
            podcastTitle: podcast,
            analysisAssetId: assetId,
            episodeId: episodeId,
            store: runtime.analysisStore
        )
    }

    @MainActor
    func generateLibraryExport() async {
        debugExportInProgress = true
        defer { debugExportInProgress = false }

        debugExport = await DebugEpisodeExporter.buildLibraryExport(
            store: runtime.analysisStore
        )
    }

    // MARK: - bd-fmfb: FoundationModels feedback attachments

    /// DEBUG-only: surface `LanguageModelSession.logFeedbackAttachment`
    /// payloads captured automatically when Apple's iOS 26.4 on-device
    /// safety classifier rejects benign podcast advertising or the
    /// refinement pass fails to decode structured output. The user (the
    /// developer) can share the captured `.feedbackAttachment` files via
    /// the standard share sheet and attach them to a Feedback Assistant
    /// report so the FoundationModels team has machine-readable evidence.
    var fmFeedbackSection: some View {
        Section {
            Text("\(fmFeedbackAttachmentURLs.count) attachment\(fmFeedbackAttachmentURLs.count == 1 ? "" : "s") captured")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.text)
                .listRowBackground(AppColors.surface)

            if !fmFeedbackAttachmentURLs.isEmpty {
                ShareLink(
                    items: fmFeedbackAttachmentURLs
                ) {
                    Label("Share via Feedback Assistant", systemImage: "square.and.arrow.up")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)
                }
                .listRowBackground(AppColors.surface)

                Button(role: .destructive) {
                    Task { await clearFMFeedback() }
                } label: {
                    Label("Clear all", systemImage: "trash")
                        .font(AppTypography.body)
                }
                .listRowBackground(AppColors.surface)
            }

            Button {
                Task { await refreshFMFeedback() }
            } label: {
                Label("Refresh", systemImage: "arrow.clockwise")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)
        } header: {
            sectionHeader("Apple FoundationModels Feedback")
        } footer: {
            Text("Captured automatically when the on-device model refuses a classification or fails to produce structured output. Tap Share to attach to a Feedback Assistant report.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.metadata)
        }
        .task {
            await refreshFMFeedback()
        }
    }

    @MainActor
    func refreshFMFeedback() async {
        guard let store = runtime.feedbackStore else {
            fmFeedbackAttachmentURLs = []
            return
        }
        fmFeedbackAttachmentURLs = await store.capturedAttachmentURLs()
    }

    @MainActor
    func clearFMFeedback() async {
        guard let store = runtime.feedbackStore else { return }
        await store.clearCapturedAttachments()
        fmFeedbackAttachmentURLs = []
    }
}
#endif

// MARK: - Helpers

private extension SettingsView {

    func sectionHeader(_ title: String) -> some View {
        Text(title)
            .font(AppTypography.sans(size: 13, weight: .semibold))
            .foregroundStyle(AppColors.secondary)
            .textCase(nil)
    }
}

// MARK: - SkipBehavior Display

private extension SkipBehavior {
    var displayName: String {
        switch self {
        case .auto: "Auto Skip"
        case .manual: "Manual"
        case .off: "Off"
        }
    }
}

// MARK: - Preview

#Preview("Settings") {
    SettingsView()
        .preferredColorScheme(.dark)
        .modelContainer(
            for: [Podcast.self, Episode.self, UserPreferences.self],
            inMemory: true
        )
}
