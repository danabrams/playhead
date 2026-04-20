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
#if canImport(UIKit) && os(iOS)
import UIKit
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
    /// playhead-ct2q: transient UI state for the "Send diagnostics" hatch.
    @State private var sendDiagnosticsInProgress = false
    @State private var sendDiagnosticsLastResult: String?
    #endif

    /// Injected dependencies — set via environment or passed directly.
    var inventory: ModelInventory?
    var assetProvider: AssetProvider?
    var entitlementManager: EntitlementManager?

    /// playhead-l274: optional deep-link router. When non-nil, the view
    /// scrolls to the matching group anchor on appearance or whenever
    /// `pending` changes (e.g. hkg8 "Free up space" tap). Kept optional
    /// so existing call sites (preview, tests) continue to compile.
    var router: SettingsRouter?

    /// playhead-l274: Downloads group state, loaded from UserDefaults.
    @State private var downloadsSettings: DownloadsSettings = .init()

    /// playhead-l274: Storage cap picker selection, derived from the
    /// persisted `StorageBudgetSettings.mediaCapBytes` (playhead-h7r).
    @State private var episodeStorageCap: EpisodeStorageCap = .defaultValue

    /// playhead-l274: "Keep analysis when removing downloads" toggle.
    /// Persisted via UserDefaults under a dedicated key.
    @State private var keepAnalysisWhenRemoving: Bool = false

    /// playhead-l274: placeholder feature-flag toggles. Defaults OFF per
    /// spec; backing storage lands when the flag-implementation beads
    /// (xr3t, zx6i, 2hpn, 43ed) close.
    @State private var featureFlagValues: [String: Bool] = FeatureFlagPlaceholders.defaultValues

    /// playhead-l274: scheduler-event tail (up to 50 entries) for the
    /// Diagnostics group. Loaded lazily on section appearance.
    @State private var schedulerEvents: [WorkJournalEntry] = []

    var body: some View {
        NavigationStack {
            ScrollViewReader { proxy in
                List {
                    if let prefs = preferences {
                        modelSection
                        adSkipSection(prefs)
                        playbackSection(prefs)
                        // playhead-l274: new Phase 2 groups inserted after
                        // Playback per the UI design doc §F.
                        downloadsSection
                            .id(SettingsRoute.downloads.anchorId)
                        storageSettingsSection
                            .id(SettingsRoute.storage.anchorId)
                        diagnosticsSection
                            .id(SettingsRoute.diagnostics.anchorId)
                        backgroundSection(prefs)
                        storageSection
                        purchasesSection
                        #if DEBUG
                        debugSection
                        sendDiagnosticsSection
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
                    // playhead-l274: load persisted l274 state.
                    downloadsSettings = DownloadsSettings.load()
                    episodeStorageCap = EpisodeStorageCap.from(
                        bytes: StorageBudgetSettings.load().mediaCapBytes
                    )
                    keepAnalysisWhenRemoving = UserDefaults.standard.bool(
                        forKey: keepAnalysisKey
                    )
                }
                .task {
                    await viewModel.computeStorageSizes()
                    if let inventory {
                        await viewModel.refreshModelStatuses(inventory: inventory)
                    }
                    await refreshSchedulerEvents()
                }
                .onChange(of: router?.pending) { _, newRoute in
                    guard let newRoute else { return }
                    withAnimation {
                        proxy.scrollTo(newRoute.anchorId, anchor: .top)
                    }
                    router?.consume()
                }
            }
        }
    }

    /// playhead-l274: persistence key for the "Keep analysis when
    /// removing downloads" toggle. Stored as a plain Bool so a future
    /// relocation to SwiftData is a single read-path change.
    private var keepAnalysisKey: String { "SettingsL274.storage.keepAnalysisWhenRemoving" }

    /// Single shared `HH:mm:ss` formatter for the Diagnostics scheduler
    /// rows. Hoisted to a `static let` (per code-review I2) so the
    /// `ForEach` body does not allocate a fresh formatter on every row
    /// render — a read-only DateFormatter is safe to share across
    /// threads per Apple's documented thread-safety guarantees for
    /// NSDateFormatter.
    static let schedulerEventTimeFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm:ss"
        return f
    }()

    /// playhead-l274: hydrate the scheduler-events tail. Failures are
    /// swallowed — the Diagnostics row renders empty rather than erroring
    /// in a support-surface panel.
    @MainActor
    private func refreshSchedulerEvents() async {
        do {
            schedulerEvents = try await runtime.analysisStore
                .fetchRecentWorkJournalEntries(limit: 50)
        } catch {
            schedulerEvents = []
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
                        .foregroundStyle(AppColors.textPrimary)
                    Text("Speech assets are managed by iOS and stay on device.")
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textSecondary)
                }
                .padding(.vertical, Spacing.xxs)
                .listRowBackground(AppColors.surface)
            } else if viewModel.modelStatuses.isEmpty {
                Text("No models in manifest")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
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
                .foregroundStyle(AppColors.textTertiary)
        }
    }

    @ViewBuilder
    func modelRow(entry: ModelEntry, status: ModelStatus) -> some View {
        HStack {
            VStack(alignment: .leading, spacing: Spacing.xxs) {
                Text(entry.displayName)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)

                HStack(spacing: Spacing.xs) {
                    Text(entry.role.rawValue)
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textTertiary)

                    Text(SettingsViewModel.formattedSize(entry.uncompressedSizeBytes))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.textTertiary)
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
                    .foregroundStyle(AppColors.textTertiary)
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
                    .foregroundStyle(AppColors.textSecondary)

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
                    .foregroundStyle(AppColors.textTertiary)
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
                    .foregroundStyle(AppColors.textPrimary)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityLabel("Ad skip mode")
            .accessibilityValue(prefs.skipBehavior.displayName)
        } header: {
            sectionHeader("Ad Detection")
        } footer: {
            Text(skipBehaviorFooter(prefs))
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
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
                        .foregroundStyle(AppColors.textPrimary)
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
                    .foregroundStyle(AppColors.textPrimary)
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
                    .foregroundStyle(AppColors.textPrimary)
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
                    .foregroundStyle(AppColors.textPrimary)
            }
            .tint(AppColors.accent)
            .listRowBackground(AppColors.surface)
        } header: {
            sectionHeader("Processing")
        } footer: {
            Text("When enabled, episodes are transcribed and analyzed in the background. Requires sufficient battery and may use network for model downloads.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
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
                .foregroundStyle(AppColors.textPrimary)

            Spacer()

            Text(SettingsViewModel.formattedSize(size))
                .font(AppTypography.timestamp)
                .foregroundStyle(AppColors.textTertiary)

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
                        .foregroundStyle(hasCurrentEpisode ? AppColors.accent : AppColors.textSecondary)

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
                    .foregroundStyle(AppColors.textTertiary)
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .listRowBackground(AppColors.surface)
            }

            if !hasCurrentEpisode {
                Text("Start playing an episode to enable export.")
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textTertiary)
                    .listRowBackground(AppColors.surface)
            }
        } header: {
            sectionHeader("Debug")
        } footer: {
            Text("Exports transcript, detected ads, evidence catalog, feature summary, and acoustic breaks for the current episode. DEBUG builds only.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
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

    // MARK: - playhead-ct2q: Send diagnostics (dogfood #if DEBUG hatch)

    /// Minimal call site for `DiagnosticsExportCoordinator`. Phase 1.5
    /// Wave 4 dogfooders need a way to push a support-safe diagnostics
    /// bundle back to the team BEFORE playhead-l274 ships the full
    /// Phase 2 Settings screen; this section is that hatch.
    ///
    /// Scope (from bead spec):
    ///   * No per-episode opt-in UI — `optInEpisodes: []` in the hatch.
    ///   * Default (non-opt-in) bundle only.
    ///   * DEBUG builds only. Release builds exclude the entire
    ///     `#if DEBUG` block in `SettingsView.body`, which removes the
    ///     section from the Settings screen and prevents the
    ///     `runDebugDiagnosticsExport(...)` symbol from being
    ///     referenced — that symbol itself is `#if DEBUG`-gated in
    ///     `DebugDiagnosticsHatch.swift`.
    var sendDiagnosticsSection: some View {
        Section {
            Button {
                Task { await sendDiagnostics() }
            } label: {
                HStack {
                    Label("Send diagnostics", systemImage: "envelope.badge")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)

                    Spacer()

                    if sendDiagnosticsInProgress {
                        ProgressView().controlSize(.small)
                    }
                }
            }
            .buttonStyle(.plain)
            .disabled(sendDiagnosticsInProgress)
            .listRowBackground(AppColors.surface)

            if let outcome = sendDiagnosticsLastResult {
                Text(outcome)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textTertiary)
                    .listRowBackground(AppColors.surface)
            }
        } header: {
            sectionHeader("Send Diagnostics")
        } footer: {
            Text("Emails a support-safe diagnostics bundle to the team. Default bundle only: no raw transcripts, no episode IDs. DEBUG builds only.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
        }
    }

    @MainActor
    func sendDiagnostics() async {
        sendDiagnosticsInProgress = true
        defer { sendDiagnosticsInProgress = false }

        do {
            let result = try await runDebugDiagnosticsExport(
                runtime: runtime,
                modelContext: modelContext
            )
            sendDiagnosticsLastResult = "Last: \(describe(result))"
        } catch {
            sendDiagnosticsLastResult = "Last: error — \(error.localizedDescription)"
        }
    }

    private func describe(_ result: DiagnosticsMailComposeResult) -> String {
        switch result {
        case .sent:      return "sent"
        case .saved:     return "saved"
        case .cancelled: return "cancelled"
        case .failed:    return "failed"
        }
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
                .foregroundStyle(AppColors.textPrimary)
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
                .foregroundStyle(AppColors.textTertiary)
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

// MARK: - playhead-l274: Downloads Section

private extension SettingsView {

    var downloadsSection: some View {
        Section {
            // Auto-download on subscribe
            HStack {
                Text(SettingsL274Copy.autoDownloadOnSubscribeLabel)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Picker("", selection: Binding(
                    get: { downloadsSettings.autoDownloadOnSubscribe },
                    set: { newValue in
                        downloadsSettings.autoDownloadOnSubscribe = newValue
                        downloadsSettings.save()
                    }
                )) {
                    ForEach(AutoDownloadOnSubscribe.allCases, id: \.self) { option in
                        Text(option.displayLabel).tag(option)
                    }
                }
                .pickerStyle(.menu)
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityLabel(SettingsL274Copy.autoDownloadOnSubscribeLabel)
            .accessibilityValue(downloadsSettings.autoDownloadOnSubscribe.displayLabel)

            // Download over cellular
            HStack {
                Text(SettingsL274Copy.downloadOverCellularLabel)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Picker("", selection: Binding(
                    get: { downloadsSettings.cellularPolicy },
                    set: { newValue in
                        downloadsSettings.cellularPolicy = newValue
                        downloadsSettings.save()
                    }
                )) {
                    ForEach(CellularPolicy.allCases, id: \.self) { option in
                        Text(option.displayLabel).tag(option)
                    }
                }
                .pickerStyle(.menu)
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityLabel(SettingsL274Copy.downloadOverCellularLabel)
            .accessibilityValue(downloadsSettings.cellularPolicy.displayLabel)

            // "Download Next N" default count
            HStack {
                Text(SettingsL274Copy.downloadNextDefaultCountLabel)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Picker("", selection: Binding(
                    get: { downloadsSettings.downloadNextDefaultCount },
                    set: { newValue in
                        downloadsSettings.downloadNextDefaultCount = newValue
                        downloadsSettings.save()
                    }
                )) {
                    ForEach(DownloadNextDefaultCount.allCases, id: \.self) { option in
                        Text(option.displayLabel).tag(option)
                    }
                }
                .pickerStyle(.menu)
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityLabel(SettingsL274Copy.downloadNextDefaultCountLabel)
            .accessibilityValue(downloadsSettings.downloadNextDefaultCount.displayLabel)
        } header: {
            sectionHeader(SettingsL274Copy.downloadsHeader)
        }
    }
}

// MARK: - playhead-l274: Storage Section

private extension SettingsView {

    var storageSettingsSection: some View {
        Section {
            // Episode storage cap picker
            HStack {
                Text(SettingsL274Copy.episodeStorageCapLabel)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Picker("", selection: Binding(
                    get: { episodeStorageCap },
                    set: { newValue in
                        episodeStorageCap = newValue
                        // Persist to StorageBudgetSettings so the next
                        // admission-control read observes the new cap
                        // (playhead-h7r). No relaunch required — the
                        // admission path reads `.load()` on every check.
                        var budget = StorageBudgetSettings.load()
                        budget.mediaCapBytes = newValue.bytes
                        budget.save()
                    }
                )) {
                    ForEach(EpisodeStorageCap.allCases, id: \.self) { option in
                        Text(option.displayLabel).tag(option)
                    }
                }
                .pickerStyle(.menu)
                .tint(AppColors.accent)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityLabel(SettingsL274Copy.episodeStorageCapLabel)
            .accessibilityValue(episodeStorageCap.displayLabel)

            // Current usage bar (media usage vs. cap)
            storageUsageRow

            // Keep-analysis toggle with sub-line
            VStack(alignment: .leading, spacing: Spacing.xxs) {
                Toggle(isOn: Binding(
                    get: { keepAnalysisWhenRemoving },
                    set: { newValue in
                        keepAnalysisWhenRemoving = newValue
                        UserDefaults.standard.set(newValue, forKey: keepAnalysisKey)
                    }
                )) {
                    Text(SettingsL274Copy.keepAnalysisToggleLabel)
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.textPrimary)
                }
                .tint(AppColors.accent)

                Text(SettingsL274Copy.keepAnalysisSubLine)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textTertiary)
            }
            .listRowBackground(AppColors.surface)

            // Analysis cap display (read-only)
            HStack {
                Text(SettingsL274Copy.analysisCapLabel)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Text(SettingsViewModel.formattedSize(analysisCapBytes))
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityElement(children: .combine)

            // Auto-evict policy line (read-only)
            Text(SettingsL274Copy.autoEvictPolicyLine)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
                .listRowBackground(AppColors.surface)
        } header: {
            sectionHeader(SettingsL274Copy.storageHeader)
        }
    }

    @ViewBuilder
    var storageUsageRow: some View {
        let cap = episodeStorageCap.bytes
        let used = viewModel.cachedAudioSize
        let fraction: Double = {
            guard cap > 0, cap != Int64.max else { return 0 }
            return max(0, min(1, Double(used) / Double(cap)))
        }()

        VStack(alignment: .leading, spacing: Spacing.xxs) {
            HStack {
                Text(SettingsL274Copy.currentUsageLabel)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Text("\(SettingsViewModel.formattedSize(used)) / \(episodeStorageCap.displayLabel)")
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
            }
            if episodeStorageCap != .unlimited {
                ProgressView(value: fraction)
                    .tint(AppColors.accent)
            }
        }
        .listRowBackground(AppColors.surface)
        .accessibilityElement(children: .combine)
    }
}

// MARK: - playhead-l274: Diagnostics Section

private extension SettingsView {

    var diagnosticsSection: some View {
        Section {
            let versions = DiagnosticsVersions.current()
            // Pipeline / model / policy / feature-schema versions
            diagnosticsKV(SettingsL274Copy.pipelineVersionLabel, versions.pipelineVersion)
            diagnosticsKV(SettingsL274Copy.modelVersionsLabel, "transcript=\(versions.transcriptModelVersion), ad=\(versions.adDetectionModelVersion)")
            diagnosticsKV(SettingsL274Copy.policyVersionLabel, versions.policyVersion)
            diagnosticsKV(SettingsL274Copy.featureSchemaVersionLabel, versions.featureSchemaVersion)

            // Last 50 scheduler events
            DisclosureGroup(SettingsL274Copy.schedulerEventsLabel) {
                if schedulerEvents.isEmpty {
                    Text("No recent scheduler events.")
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textTertiary)
                } else {
                    ForEach(Array(schedulerEvents.enumerated()), id: \.offset) { _, entry in
                        schedulerEventRow(entry)
                    }
                }
            }
            .listRowBackground(AppColors.surface)

            // Per-show capability profile (h6a6 is OPEN — hide when no data)
            // No producer API exists yet; the row stays hidden. When h6a6
            // lands, add the provider call here and render when non-empty.
            // (Explicit no-op keeps the scope discipline visible.)
            // TODO(bd playhead-h6a6): render per-show capability profile when producer lands

            // Feature-flag placeholder toggles (all default OFF)
            DisclosureGroup(SettingsL274Copy.featureFlagsLabel) {
                ForEach(FeatureFlagPlaceholders.orderedSlugs, id: \.self) { slug in
                    Toggle(isOn: Binding(
                        get: { featureFlagValues[slug] ?? false },
                        set: { newValue in
                            featureFlagValues[slug] = newValue
                            // TODO(bd playhead-l274): wire actual flag storage
                        }
                    )) {
                        Text("playhead-\(slug)")
                            .font(AppTypography.body)
                            .foregroundStyle(AppColors.textPrimary)
                    }
                    .tint(AppColors.accent)
                }
            }
            .listRowBackground(AppColors.surface)

            // Send diagnostics button (mail composer only, never network)
            Button {
                Task { await sendDiagnosticsViaSettings() }
            } label: {
                HStack {
                    Label(SettingsL274Copy.sendDiagnosticsButtonLabel, systemImage: "envelope.badge")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)
                    Spacer()
                }
            }
            .buttonStyle(.plain)
            .listRowBackground(AppColors.surface)
        } header: {
            sectionHeader(SettingsL274Copy.diagnosticsHeader)
        } footer: {
            Text(SettingsL274Copy.sendDiagnosticsFooter)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
        }
    }

    @ViewBuilder
    func diagnosticsKV(_ label: String, _ value: String) -> some View {
        HStack(alignment: .firstTextBaseline) {
            Text(label)
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textPrimary)
            Spacer()
            Text(value)
                .font(AppTypography.timestamp)
                .foregroundStyle(AppColors.textTertiary)
                .multilineTextAlignment(.trailing)
                .lineLimit(2)
                .truncationMode(.middle)
        }
        .listRowBackground(AppColors.surface)
        .accessibilityElement(children: .combine)
    }

    @ViewBuilder
    func schedulerEventRow(_ entry: WorkJournalEntry) -> some View {
        let time = Date(timeIntervalSince1970: entry.timestamp)
        let hashedEpisode = String(entry.episodeId.prefix(8))
        let missCause = entry.cause?.rawValue ?? "-"
        VStack(alignment: .leading, spacing: 2) {
            HStack {
                Text(Self.schedulerEventTimeFormatter.string(from: time))
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
                Text(entry.eventType.rawValue)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textSecondary)
                Spacer()
                Text(hashedEpisode)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
            }
            if entry.cause != nil {
                Text("cause: \(missCause)")
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
            }
        }
        .accessibilityElement(children: .combine)
    }

    @MainActor
    func sendDiagnosticsViaSettings() async {
        // Release-safe entry: route through the Phase 1.5 coordinator. In
        // DEBUG we still prefer the existing hatch so the DEBUG-only
        // `sendDiagnosticsSection` and this Diagnostics-group button share
        // a single code path (retains the canary-guarded
        // `runDebugDiagnosticsExport` invocation). In Release we assemble
        // the same coordinator graph inline here from Release-compatible
        // types — the coordinator + presenter + sink are all already
        // Release-safe, only the DEBUG `runDebugDiagnosticsExport` helper
        // file is DEBUG-gated. Under no path do we initiate a network
        // upload; the mail composer (or iPad activity fallback) is the
        // sole delivery surface.
        #if DEBUG
        _ = try? await runDebugDiagnosticsExport(
            runtime: runtime,
            modelContext: modelContext
        )
        #elseif canImport(UIKit) && os(iOS)
        _ = try? await runReleaseDiagnosticsExport()
        #endif
    }

    #if !DEBUG && canImport(UIKit) && os(iOS)
    /// Release-build sibling of `runDebugDiagnosticsExport`. Inlined here
    /// (instead of as a sibling to the DEBUG hatch file) because
    /// `DebugDiagnosticsHatch.swift` is guarded at the file level by
    /// `#if DEBUG` and a source-canary test asserts that guard.
    ///
    /// The coordinator graph mirrors the DEBUG hatch one-for-one:
    ///   1. `InstallIDProvider(context: modelContext).installID()`
    ///   2. `DiagnosticsExportEnvironment` from `Bundle.main`,
    ///      `ProcessInfo`, `DeviceClass.detect()`, `BuildType.detect()`,
    ///      the live `CapabilitySnapshot`, and the install UUID.
    ///   3. `journalFetch` adapter over `runtime.analysisStore`.
    ///   4. `SwiftDataDiagnosticsOptInSink(context: modelContext)`.
    ///   5. `UIKitDiagnosticsPresenter` with a key-window host provider.
    @MainActor
    private func runReleaseDiagnosticsExport() async throws -> DiagnosticsMailComposeResult {
        let installID = try InstallIDProvider(context: modelContext).installID()
        let snapshot = await runtime.capabilitiesService.currentSnapshot
        let now = Date()
        let eligibility = AnalysisEligibility(
            hardwareSupported: snapshot.foundationModelsAvailable,
            appleIntelligenceEnabled: snapshot.appleIntelligenceEnabled,
            regionSupported: true,
            languageSupported: snapshot.foundationModelsLocaleSupported,
            modelAvailableNow: snapshot.foundationModelsUsable,
            capturedAt: now
        )
        let appVersion = (Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String) ?? "unknown"
        let osv = ProcessInfo.processInfo.operatingSystemVersion
        let osVersion = "\(osv.majorVersion).\(osv.minorVersion).\(osv.patchVersion)"

        let environment = DiagnosticsExportEnvironment(
            appVersion: appVersion,
            osVersion: osVersion,
            deviceClass: DeviceClass.detect(),
            buildType: BuildType.detect(),
            eligibility: eligibility,
            installID: installID,
            now: now
        )

        let store = runtime.analysisStore
        let journalFetch: DiagnosticsJournalFetch = { [store] in
            try await store.fetchRecentWorkJournalEntries(limit: 200)
        }

        let presenter = UIKitDiagnosticsPresenter(hostProvider: Self.releaseHostProvider)
        let coordinator = DiagnosticsExportCoordinator(
            environment: environment,
            presenter: presenter,
            journalFetch: journalFetch,
            optInSink: SwiftDataDiagnosticsOptInSink(context: modelContext),
            optInEpisodes: []
        )
        return try await coordinator.exportAndPresent()
    }

    /// Release-build default host provider. Walks to the foreground-active
    /// window's rootViewController, then follows the presented-chain so
    /// the composer lands on top of any modally presented sheet
    /// (Settings is itself typically presented modally). Mirrors the
    /// DEBUG hatch's `defaultHostProvider`.
    @MainActor
    private static let releaseHostProvider: @MainActor () -> UIViewController? = {
        let scene = UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .first { $0.activationState == .foregroundActive }
            ?? UIApplication.shared.connectedScenes
                .compactMap { $0 as? UIWindowScene }
                .first
        guard let root = scene?.windows.first(where: \.isKeyWindow)?.rootViewController
            ?? scene?.windows.first?.rootViewController
        else { return nil }
        var current = root
        while let presented = current.presentedViewController {
            current = presented
        }
        return current
    }
    #endif
}

// MARK: - Helpers

private extension SettingsView {

    func sectionHeader(_ title: String) -> some View {
        Text(title)
            .font(AppTypography.sans(size: 13, weight: .semibold))
            .foregroundStyle(AppColors.textSecondary)
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
