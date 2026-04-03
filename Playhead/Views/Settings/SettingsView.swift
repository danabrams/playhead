// SettingsView.swift
// Settings screen with all user-configurable options.
//
// Sections:
// - WhisperKit model selection with download management and size indicators
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

    @State private var viewModel = SettingsViewModel()

    /// Injected dependencies — set via environment or passed directly.
    var inventory: ModelInventory?
    var assetProvider: AssetProvider?
    var entitlementManager: EntitlementManager?

    /// Resolved preferences: fetches the first row or creates a default.
    private var preferences: UserPreferences {
        if let existing = allPreferences.first {
            return existing
        }
        let fresh = UserPreferences()
        modelContext.insert(fresh)
        return fresh
    }

    var body: some View {
        NavigationStack {
            List {
                modelSection
                adSkipSection
                playbackSection
                backgroundSection
                storageSection
                purchasesSection
            }
            .listStyle(.insetGrouped)
            .scrollContentBackground(.hidden)
            .background(AppColors.background)
            .navigationTitle("Settings")
            .navigationBarTitleDisplayMode(.large)
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

        case .downloading(let progress):
            HStack(spacing: Spacing.xs) {
                ProgressView(value: progress)
                    .tint(AppColors.accent)
                    .frame(width: 60)
                Text("\(Int(progress * 100))%")
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.metadata)
            }

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
        return !usesStubSpeech && SpeechTranscriber.isAvailable
#else
        return false
#endif
    }
}

// MARK: - Ad Skip Section

private extension SettingsView {

    var adSkipSection: some View {
        Section {
            Picker(selection: Binding(
                get: { preferences.skipBehavior },
                set: { preferences.skipBehavior = $0 }
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
        } header: {
            sectionHeader("Ad Detection")
        } footer: {
            Text(skipBehaviorFooter)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.metadata)
        }
    }

    var skipBehaviorFooter: String {
        switch preferences.skipBehavior {
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

    var playbackSection: some View {
        Section {
            // Playback speed
            VStack(alignment: .leading, spacing: Spacing.xs) {
                HStack {
                    Label("Playback Speed", systemImage: "gauge.with.needle")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.text)
                    Spacer()
                    Text(String(format: "%.1fx", preferences.playbackSpeed))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.accent)
                }

                Slider(
                    value: Binding(
                        get: { preferences.playbackSpeed },
                        set: { preferences.playbackSpeed = $0 }
                    ),
                    in: 0.5...3.0,
                    step: 0.1
                )
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)

            // Forward skip interval
            HStack {
                Label("Skip Forward", systemImage: "goforward")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.text)
                Spacer()
                Picker("", selection: Binding(
                    get: { preferences.skipIntervals.forwardSeconds },
                    set: { preferences.skipIntervals.forwardSeconds = $0 }
                )) {
                    ForEach(skipIntervalOptions, id: \.self) { seconds in
                        Text("\(Int(seconds))s").tag(seconds)
                    }
                }
                .pickerStyle(.menu)
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)

            // Backward skip interval
            HStack {
                Label("Skip Back", systemImage: "gobackward")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.text)
                Spacer()
                Picker("", selection: Binding(
                    get: { preferences.skipIntervals.backwardSeconds },
                    set: { preferences.skipIntervals.backwardSeconds = $0 }
                )) {
                    ForEach(skipIntervalOptions, id: \.self) { seconds in
                        Text("\(Int(seconds))s").tag(seconds)
                    }
                }
                .pickerStyle(.menu)
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)
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

    var backgroundSection: some View {
        Section {
            Toggle(isOn: Binding(
                get: { preferences.backgroundProcessingEnabled },
                set: { preferences.backgroundProcessingEnabled = $0 }
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
            }
        }
        .listRowBackground(AppColors.surface)
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
                    }
                }
            }
            .buttonStyle(.plain)
            .disabled(viewModel.isRestoring)
            .listRowBackground(AppColors.surface)

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
