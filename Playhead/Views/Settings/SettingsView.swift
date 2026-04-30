// SettingsView.swift
// Settings screen with all user-configurable options.
//
// Sections:
// - Speech model status (Apple Speech, system-managed)
// - Ad skip behavior (auto/manual/off)
// - Playback defaults (speed, skip intervals)
// - Storage management (transcript cache, cached audio)
// - Background processing preferences
// - Restore Purchases

import SwiftUI
import SwiftData
#if canImport(UIKit) && os(iOS)
import UIKit
#endif

// MARK: - SettingsView

struct SettingsView: View {

    @Query private var allPreferences: [UserPreferences]
    @Environment(\.modelContext) var modelContext
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
    /// Escape hatch: copy the AnalysisStore SQLite DB into Documents/ so
    /// Xcode's "Download Container" can pull it down (the production DB is
    /// FileProtectionType.complete and is therefore opaque to Xcode).
    @State private var analysisStoreExportInProgress = false
    @State private var analysisStoreExportLastResult: String?
    /// playhead-dgzw (narE): transient UI state for "Export corpus log".
    @State private var corpusExportInProgress = false
    @State private var corpusExportResult: CorpusExportResult?
    @State private var corpusExportError: String?
    #endif

    /// Injected dependencies — set via environment or passed directly.
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

    /// playhead-btoa.4: persisted toggle that drives the per-row
    /// `PipelineProgressStripView` on the Activity screen. Default is
    /// `false`; the same `@AppStorage` key (`debug.showPipelineStrip`)
    /// is read by `NowRowView` / `UpNextRowView` / `PausedRowView`. The
    /// toggle row is intentionally NOT `#if DEBUG`-gated so it is
    /// flippable in TestFlight builds for dogfood-only debugging.
    @AppStorage(DebugFlagKeys.showPipelineStrip) private var showPipelineStrip = false

    /// playhead-2jo: OPML import/export state. Owns the file picker /
    /// share-sheet plumbing for the Subscriptions group; the actual
    /// parse + import work lives on `OPMLImportExportViewModel`.
    /// Internal access so the cross-file `SettingsOPMLSection` extension
    /// can read/bind these fields.
    @State var opmlViewModel = OPMLImportExportViewModel()
    @State var opmlImporterPresented = false

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
                        //
                        // I4 (code-review): SwiftUI's `proxy.scrollTo(id, anchor:)`
                        // on a `Section` directly inside a `List` is known to
                        // be unreliable because the SwiftUI runtime associates
                        // the ID with the Section's first row rather than its
                        // header — anchoring with `.top` lands inside the
                        // section header. The deterministic pattern is to
                        // attach the route ID to a zero-height invisible
                        // anchor row that lives at the top of each section.
                        // `proxy.scrollTo(anchorId, anchor: .top)` then lands
                        // exactly at the section's first content row, which
                        // is what the deep-link UX expects (see
                        // `SettingsRouterDeepLinkTests.freeUpSpaceLandsOnStorage`).
                        // Real-device smoke-test is still the definitive
                        // check; the anchor row is the most-portable
                        // fallback if a future iOS revision changes List
                        // section anchoring semantics.
                        downloadsSection
                        storageSettingsSection
                        diagnosticsSection
                        backgroundSection(prefs)
                        episodeSummariesSection(prefs)
                        storageSection
                        opmlSection
                        purchasesSection
                        aboutSection
                        // playhead-btoa.4: always-visible debug-toggles
                        // section. Currently holds only the Activity
                        // pipeline-strip flag; new always-on debug
                        // toggles should land here too.
                        debugTogglesSection
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
                    // playhead-l274 + playhead-24cm: the 24cm flag is
                    // already wired (persisted via `PreAnalysisConfig`
                    // and applied live via `DownloadManager`), so
                    // initialize its toggle value from the persisted
                    // config rather than the `FeatureFlagPlaceholders`
                    // default. The other four slugs remain placeholder
                    // shims until their beads land.
                    featureFlagValues["24cm"] = PreAnalysisConfig.load().useDualBackgroundSessions
                }
                .task {
                    await viewModel.computeStorageSizes()
                    await refreshSchedulerEvents()
                    // playhead-j2u: hydrate the Models section's
                    // status-only readout from the live evaluator. The
                    // call is non-blocking by contract.
                    viewModel.refreshEligibility(
                        using: runtime.analysisEligibilityEvaluator
                    )
                    // playhead-5c1t: read the iCloud sync status from the
                    // coordinator BEFORE the (suspending) premium-stream
                    // observation below — that observation never returns,
                    // so anything queued after it would never run.
                    await viewModel.observeICloudSyncStatus(runtime.iCloudSyncCoordinator)
                    // playhead-j2u: subscribe to premium-status updates
                    // so the Purchases section reflects transactions
                    // arriving from other devices / restores.
                    if let entitlementManager {
                        await viewModel.observePremiumStatus(entitlementManager)
                    }
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

// MARK: - Model Status Section (playhead-j2u)
//
// Status-only readout. NO download/delete buttons, NO model-selection
// picker, NO size displays — playhead-c6r removed external model
// manifests and on-device speech assets are managed by iOS itself. The
// row simply surfaces the verdict from `AnalysisEligibilityEvaluator
// .evaluate()` so the user knows whether on-device analysis is
// currently available on their device.

private extension SettingsView {

    var modelSection: some View {
        Section {
            HStack(alignment: .firstTextBaseline) {
                Text("On-device transcription")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Text(modelStatusText)
                    .font(AppTypography.caption)
                    .foregroundStyle(modelStatusColor)
                    .accessibilityIdentifier("Settings.models.statusValue")
            }
            .listRowBackground(AppColors.surface)
            .accessibilityElement(children: .combine)
            .accessibilityLabel("On-device transcription status")
            .accessibilityValue(modelStatusText)
        } header: {
            sectionHeader("Models")
        } footer: {
            Text("On-device analysis assets never leave your device.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
        }
    }

    /// playhead-j2u: derive the status string from the cached
    /// `AnalysisEligibility` snapshot. Returns "Checking…" until the
    /// first evaluation lands so we never lie about the verdict.
    var modelStatusText: String {
        guard let eligibility = viewModel.eligibility else {
            return "Checking…"
        }
        return eligibility.isFullyEligible ? "Available" : "Unavailable"
    }

    var modelStatusColor: Color {
        guard let eligibility = viewModel.eligibility else {
            return AppColors.textTertiary
        }
        return eligibility.isFullyEligible
            ? AppColors.textSecondary
            : AppColors.accent
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
            Text("When enabled, episodes are transcribed and analyzed in the background. Requires sufficient battery.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
        }
    }

    /// playhead-jzik: toggle for the on-device episode-summary feature.
    /// Default ON. Disabling halts the backfill coordinator on its
    /// next pass and leaves any previously-generated rows in place
    /// (re-enabling resumes generation against rows still missing or
    /// stale). The footer copy intentionally does not reach for the
    /// "AI" framing — the user-facing language is about what the
    /// feature does, not how.
    func episodeSummariesSection(_ prefs: UserPreferences) -> some View {
        Section {
            Toggle(isOn: Binding(
                get: { prefs.episodeSummariesEnabled },
                set: { newValue in
                    prefs.episodeSummariesEnabled = newValue
                    // playhead-jzik: mirror into the UserDefaults
                    // `UserPreferencesSnapshot` slot so the
                    // EpisodeSummaryBackfillCoordinator (which lives
                    // off the main actor and has no SwiftData hop)
                    // can read the toggle synchronously.
                    UserPreferencesSnapshot.save(episodeSummariesEnabled: newValue)
                }
            )) {
                Label("Episode Summaries", systemImage: "text.alignleft")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
            }
            .tint(AppColors.accent)
            .listRowBackground(AppColors.surface)
            .accessibilityIdentifier("Settings.episodeSummaries.toggle")
        } header: {
            sectionHeader("Episode Summaries")
        } footer: {
            Text("Generate short, on-device summaries for episodes you've finished analyzing. Tap an episode in the library to expand and read the summary.")
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
                label: "Cached Audio",
                icon: "waveform",
                size: viewModel.storage.cachedAudioBytes,
                isClearing: viewModel.isClearingAudioCache,
                clearAction: {
                    Task { await viewModel.clearAudioCache() }
                }
            )

            storageRow(
                label: "Transcript DB",
                icon: "doc.text",
                size: viewModel.storage.transcriptDatabaseBytes,
                isClearing: viewModel.isClearingTranscriptCache,
                clearAction: {
                    Task { await viewModel.clearTranscriptCache() }
                }
            )

            // playhead-j2u: total + remaining device storage rows.
            // Both are read-only — they live alongside the per-category
            // breakdown so the user can see the absolute footprint and
            // how much room remains on the device.
            HStack {
                Text("Total used by Playhead")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Text(SettingsViewModel.formattedSize(viewModel.storage.totalBytes))
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityElement(children: .combine)
            .accessibilityIdentifier("Settings.storage.total")

            HStack {
                Text("Available on device")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Text(deviceAvailableText)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
            }
            .listRowBackground(AppColors.surface)
            .accessibilityElement(children: .combine)
            .accessibilityIdentifier("Settings.storage.deviceAvailable")
        } header: {
            sectionHeader("Storage")
        }
    }

    /// playhead-j2u: format the device-available figure, falling back
    /// to a quiet "—" when the volume metadata is unavailable (older OS
    /// or simulator with stripped capacity keys).
    var deviceAvailableText: String {
        guard let bytes = viewModel.storage.deviceAvailableBytes else { return "—" }
        return SettingsViewModel.formattedSize(bytes)
    }

    func storageRow(
        label: String,
        icon: String,
        size: Int64,
        isClearing: Bool = false,
        clearAction: (() -> Void)? = nil
    ) -> some View {
        HStack {
            Label(label, systemImage: icon)
                .font(AppTypography.body)
                .foregroundStyle(isClearing ? AppColors.textTertiary : AppColors.textPrimary)

            Spacer()

            Text(SettingsViewModel.formattedSize(size))
                .font(AppTypography.timestamp)
                .foregroundStyle(AppColors.textTertiary)

            if isClearing {
                // playhead-j2u: Quiet Instrument — slim trailing progress
                // indicator (no percentage, no bar) while the clear runs.
                ProgressView()
                    .controlSize(.small)
                    .accessibilityLabel("Clearing \(label)")
            } else if let clearAction, size > 0 {
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
            // playhead-j2u: premium status line. The Restore button only
            // appears when the user is NOT premium per the bead spec —
            // showing it on a premium account would invite confusion.
            HStack {
                Text("Status")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Text(premiumStatusText)
                    .font(AppTypography.caption)
                    .foregroundStyle(viewModel.isPremium ? AppColors.accent : AppColors.textSecondary)
                    .accessibilityIdentifier("Settings.purchase.statusValue")
            }
            .listRowBackground(AppColors.surface)
            .accessibilityElement(children: .combine)
            .accessibilityLabel("Premium status")
            .accessibilityValue(premiumStatusText)

            if !viewModel.isPremium {
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
            }
        } header: {
            sectionHeader("Purchase")
        }
    }

    /// playhead-j2u: short status string per the bead spec. "Premium —
    /// purchased" / "Free preview" — no pricing flow on the Settings
    /// surface.
    var premiumStatusText: String {
        viewModel.isPremium ? "Premium — purchased" : "Free preview"
    }
}

// MARK: - About Section (playhead-j2u)
//
// Three rows: app version, build number, and the verbatim privacy
// statement. No diagnostic links, no acknowledgements — the Diagnostics
// section already covers support flows.

private extension SettingsView {

    var aboutSection: some View {
        Section {
            HStack {
                Text("Version")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Text(aboutVersionText)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
                    .accessibilityIdentifier("Settings.about.version")
            }
            .listRowBackground(AppColors.surface)
            .accessibilityElement(children: .combine)

            HStack {
                Text("Build")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                Spacer()
                Text(aboutBuildText)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
                    .accessibilityIdentifier("Settings.about.build")
            }
            .listRowBackground(AppColors.surface)
            .accessibilityElement(children: .combine)

            Text(SettingsAboutCopy.privacyStatement)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
                .listRowBackground(AppColors.surface)
                .accessibilityIdentifier("Settings.about.privacy")
        } header: {
            sectionHeader("About")
        } footer: {
            // playhead-5c1t: quiet "Synced via iCloud" / "iCloud sync
            // paused" footer. Peace-of-mind, not metrics — no badge, no
            // animation, no quantified counter. Single tertiary-color
            // line, hidden entirely when the runtime hasn't loaded the
            // status yet so we never lie about the state.
            iCloudSyncFooter
        }
    }

    /// playhead-5c1t: footer text for the About section. Reflects the
    /// `ICloudSyncCoordinator.isSyncEnabled` state observed from the
    /// view model. Empty string while the first observation lands —
    /// avoids flashing the wrong value at launch.
    @ViewBuilder
    var iCloudSyncFooter: some View {
        if let enabled = viewModel.iCloudSyncEnabled {
            Text(enabled ? SettingsAboutCopy.iCloudSyncedFooter : SettingsAboutCopy.iCloudPausedFooter)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
                .accessibilityIdentifier("Settings.about.iCloudFooter")
        } else {
            EmptyView()
        }
    }

    var aboutVersionText: String {
        (Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String) ?? "—"
    }

    var aboutBuildText: String {
        (Bundle.main.infoDictionary?["CFBundleVersion"] as? String) ?? "—"
    }
}

// MARK: - About copy (playhead-j2u)

/// Verbatim user-facing copy for the About section. Pinned in tests so
/// any future edit requires touching the spec + the assertion together.
enum SettingsAboutCopy {
    /// Privacy statement — verbatim per playhead-j2u.
    static let privacyStatement: String = "Your podcasts never leave your device."

    /// playhead-5c1t: footer text shown when iCloud sync is available.
    /// Quiet, single-line, peace-of-mind — no badge, no animation.
    static let iCloudSyncedFooter: String = "Synced via iCloud."

    /// playhead-5c1t: footer text shown when iCloud sync is paused
    /// (signed-out, restricted, or temporarily unavailable). Avoids the
    /// word "error" — the user's local data is fine; only the sync is
    /// paused.
    static let iCloudPausedFooter: String = "iCloud sync paused."
}

// MARK: - Debug Toggles Section (always visible)

private extension SettingsView {

    /// playhead-btoa.4: always-visible debug-toggles section. Lives
    /// outside the `#if DEBUG` block so dogfood TestFlight builds can
    /// flip these flags without a custom build. Currently exposes a
    /// single toggle: `debug.showPipelineStrip`, which lights up the
    /// per-row `PipelineProgressStripView` on the Activity screen.
    /// Wrap a future toggle in `#if DEBUG` only when it must not ship.
    var debugTogglesSection: some View {
        Section {
            Toggle(isOn: $showPipelineStrip) {
                Text("Show pipeline progress on Activity")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
            }
            .tint(AppColors.accent)
            .listRowBackground(AppColors.surface)
            .accessibilityIdentifier("Settings.debug.showPipelineStrip")
        } header: {
            sectionHeader("Debug Overlays")
        } footer: {
            Text("Renders DL / TX / AN per-episode progress under each Activity row.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
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

            // Escape hatch for Xcode "Download Container": copies the live
            // AnalysisStore SQLite DB into Documents/ (the production path is
            // FileProtectionType.complete, which Xcode refuses to transfer).
            Button {
                Task { await exportAnalysisStoreToDocuments() }
            } label: {
                HStack {
                    Label("Export Analysis DB to Documents", systemImage: "externaldrive.badge.icloud")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)

                    Spacer()

                    if analysisStoreExportInProgress {
                        ProgressView().controlSize(.small)
                    }
                }
            }
            .buttonStyle(.plain)
            .disabled(analysisStoreExportInProgress)
            .listRowBackground(AppColors.surface)

            if let outcome = analysisStoreExportLastResult {
                Text(outcome)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textTertiary)
                    .lineLimit(2)
                    .truncationMode(.middle)
                    .listRowBackground(AppColors.surface)
            }

            // playhead-dgzw (narE): corpus-export action. Writes JSONL to
            // Documents/ so it lands in the file-sharing-visible directory
            // (UIFileSharingEnabled + LSSupportsOpeningDocumentsInPlace —
            // dev builds only). Unlike the text-export ShareLink above,
            // this one is retrieved via Files.app or Finder and is
            // designed to compose with narL's decision-log.jsonl into a
            // single corpus bundle.
            Button {
                Task { await generateCorpusExport() }
            } label: {
                HStack {
                    Label("Export Corpus Log", systemImage: "doc.append")
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.accent)

                    Spacer()

                    if corpusExportInProgress {
                        ProgressView().controlSize(.small)
                    }
                }
            }
            .buttonStyle(.plain)
            .disabled(corpusExportInProgress)
            .listRowBackground(AppColors.surface)

            if let result = corpusExportResult {
                VStack(alignment: .leading, spacing: 2) {
                    Text(result.fileURL.lastPathComponent)
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textSecondary)
                        .lineLimit(1)
                        .truncationMode(.middle)
                    Text("\(result.assetCount) assets · \(result.spanCount) spans · \(result.correctionCount) corrections"
                        + (result.skippedCorrectionCount > 0
                            ? " · \(result.skippedCorrectionCount) skipped"
                            : ""))
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textTertiary)
                    if let manifest = result.decisionLogManifestURL {
                        Text("paired with \(manifest.lastPathComponent)")
                            .font(AppTypography.caption)
                            .foregroundStyle(AppColors.textTertiary)
                    }
                    if let shadow = result.shadowManifestURL {
                        Text("shadow sidecar: \(shadow.lastPathComponent) (\(result.shadowRowCount) rows)")
                            .font(AppTypography.caption)
                            .foregroundStyle(AppColors.textTertiary)
                    }
                }
                .listRowBackground(AppColors.surface)
            }

            if let error = corpusExportError {
                Text(error)
                    .font(AppTypography.caption)
                    .foregroundStyle(.red)
                    .listRowBackground(AppColors.surface)
            }

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

        debugExport = await DebugEpisodeExportService.build(
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

        debugExport = await DebugEpisodeExportService.buildLibraryExport(
            store: runtime.analysisStore
        )
    }

    /// DEBUG-only: copies the live AnalysisStore SQLite DB into
    /// `Documents/ExportedAnalysisStore/analysis.sqlite` using SQLite's
    /// `VACUUM INTO`. The snapshot is a single standalone file (no
    /// `-wal`/`-shm`), safe to take while the DB is open, and lives in
    /// Documents/ (default protection) so Xcode's Download Container can
    /// pull it down for offline inspection.
    @MainActor
    func exportAnalysisStoreToDocuments() async {
        analysisStoreExportInProgress = true
        defer { analysisStoreExportInProgress = false }

        let fm = FileManager.default
        let docs = fm.urls(for: .documentDirectory, in: .userDomainMask)[0]
        let dir = docs.appendingPathComponent("ExportedAnalysisStore", isDirectory: true)
        let destFile = dir.appendingPathComponent("analysis.sqlite")

        do {
            // Wipe and recreate so re-exports are clean. VACUUM INTO
            // also refuses to overwrite an existing file.
            try? fm.removeItem(at: dir)
            try fm.createDirectory(at: dir, withIntermediateDirectories: true)

            try await runtime.analysisStore.vacuumInto(destinationURL: destFile)

            let size = (try? fm.attributesOfItem(atPath: destFile.path)[.size] as? Int64) ?? 0
            analysisStoreExportLastResult = "Exported \(SettingsViewModel.formattedSize(size)) to Documents/ExportedAnalysisStore/analysis.sqlite"
        } catch {
            analysisStoreExportLastResult = "Export failed: \(error.localizedDescription)"
        }
    }

    /// playhead-dgzw (narE): write `Documents/corpus-export.<ts>.jsonl` via
    /// `CorpusExporter`. The file lives in the UIFileSharingEnabled Documents
    /// directory so it can be pulled off-device via Finder / Files.app without
    /// the Xcode "Download Container" dance.
    @MainActor
    func generateCorpusExport() async {
        corpusExportInProgress = true
        corpusExportError = nil
        defer { corpusExportInProgress = false }

        let fm = FileManager.default
        guard let docs = fm.urls(for: .documentDirectory, in: .userDomainMask).first else {
            corpusExportError = "Could not locate Documents directory."
            return
        }

        do {
            let result = try await CorpusExporter.export(
                store: runtime.analysisStore,
                documentsURL: docs
            )
            corpusExportResult = result
        } catch {
            corpusExportError = "Export failed: \(error.localizedDescription)"
            corpusExportResult = nil
        }
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
            scrollAnchor(SettingsRoute.downloads.anchorId)
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
            scrollAnchor(SettingsRoute.storage.anchorId)
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
            scrollAnchor(SettingsRoute.diagnostics.anchorId)
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
            // lands, add the provider call here and render the row using
            // `SettingsL274Copy.perShowCapabilityProfileLabel` as the
            // row label (already test-pinned in `SettingsL274CopyTests`
            // so the copy is locked-in for the future landing).
            // (Explicit no-op keeps the scope discipline visible.)
            // TODO(bd playhead-h6a6): render per-show capability profile
            //   row using `SettingsL274Copy.perShowCapabilityProfileLabel`
            //   when the producer API lands.

            // Feature-flag toggles. `24cm` is wired to real storage
            // (`PreAnalysisConfig.useDualBackgroundSessions`) and applied
            // to the live `DownloadManager` on change. The other four
            // slugs are placeholder shims that persist to in-memory state
            // only — their real storage lands with their respective beads
            // (xr3t, zx6i, 2hpn, 43ed). All default OFF per spec.
            DisclosureGroup(SettingsL274Copy.featureFlagsLabel) {
                ForEach(FeatureFlagPlaceholders.orderedSlugs, id: \.self) { slug in
                    Toggle(isOn: Binding(
                        get: { featureFlagValues[slug] ?? false },
                        set: { newValue in
                            featureFlagValues[slug] = newValue
                            if slug == "24cm" {
                                var config = PreAnalysisConfig.load()
                                config.useDualBackgroundSessions = newValue
                                config.save()
                                Task { @MainActor in
                                    await DownloadManager.shared?.setUseDualBackgroundSessions(newValue)
                                }
                            }
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
        // DEBUG we call the DEBUG hatch so both dogfood surfaces share a
        // single code path (retains the canary-guarded
        // `runDebugDiagnosticsExport` invocation). In Release we call
        // the Release hatch, which assembles an identical coordinator
        // graph — see `ReleaseDiagnosticsHatch.swift`. Under no path do
        // we initiate a network upload; the mail composer (or iPad
        // activity fallback) is the sole delivery surface.
        #if DEBUG
        _ = try? await runDebugDiagnosticsExport(
            runtime: runtime,
            modelContext: modelContext
        )
        #elseif canImport(UIKit) && os(iOS)
        _ = try? await runReleaseDiagnosticsExport(
            runtime: runtime,
            modelContext: modelContext
        )
        #endif
    }
}

// MARK: - Helpers

private extension SettingsView {

    func sectionHeader(_ title: String) -> some View {
        Text(title)
            .font(AppTypography.sans(size: 13, weight: .semibold))
            .foregroundStyle(AppColors.textSecondary)
            .textCase(nil)
    }

    /// playhead-l274 I4 (code-review): zero-height invisible List row
    /// used as a deterministic deep-link target for `proxy.scrollTo(id,
    /// anchor: .top)`. SwiftUI's List loses the section-level `.id()`
    /// modifier when the section's first row's anchor is requested,
    /// because the runtime associates the ID with the first row's
    /// position rather than the header. Attaching the ID to a
    /// dedicated, hidden row at the top of the section keeps the scroll
    /// target stable across List layout passes and matches the pattern
    /// used elsewhere in SwiftUI deep-link surfaces.
    @ViewBuilder
    func scrollAnchor(_ id: String) -> some View {
        Color.clear
            .frame(height: 0)
            .listRowBackground(Color.clear)
            .listRowInsets(EdgeInsets())
            .listRowSeparator(.hidden)
            .accessibilityHidden(true)
            .id(id)
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
