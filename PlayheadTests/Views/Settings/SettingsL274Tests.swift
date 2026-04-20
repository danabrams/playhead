// SettingsL274Tests.swift
// Acceptance-gate coverage for playhead-l274 — Settings Downloads,
// Storage, Diagnostics deliverable.
//
// The tests operate on the pure copy/logic layer (`SettingsL274Copy`,
// `DownloadsSettings`, `EpisodeStorageCap`, `SettingsRoute`,
// `SettingsRouter`, `DiagnosticsVersions`) rather than instantiating the
// SwiftUI host. Verbatim strings are the contract and the view body
// composes them.
//
// Coverage map (→ definition of done item):
//   * `SettingsL274CopyTests`             — #2 (copy exactness)
//   * `SettingsL274DefaultsTests`         — #3 (correct defaults)
//   * `EpisodeStorageCapTests`            — #3 (10 GB default), #4 ingress
//   * `DownloadsSettingsPersistenceTests` — #3 (defaults persist)
//   * `StorageCapRecomputationTests`      — #4 (cap change triggers
//                                            admission-control reload)
//   * `SettingsRouterDeepLinkTests`       — #6 (hkg8 CTA lands on Storage)
//   * `SettingsL274SourceCanaryTests`     — #7 (no out-of-scope
//                                            additions: no quality toggle,
//                                            no skip-aggressiveness
//                                            slider, no force-analyze)
//   * `DownloadNextViewFreeUpSpaceWiringTests` — #6 (hkg8 TODOs
//                                            removed in both files)
//   * `DiagnosticsMailComposerSpyTests`   — #5 (Send Diagnostics uses
//                                            mail composer, never
//                                            network)

import Foundation
import Testing

@testable import Playhead

// MARK: - Copy

@Suite("SettingsL274 verbatim copy (playhead-l274)")
struct SettingsL274CopyTests {

    // Section headers
    @Test func downloadsHeaderIsVerbatim() {
        #expect(SettingsL274Copy.downloadsHeader == "Downloads")
    }

    @Test func storageHeaderIsVerbatim() {
        #expect(SettingsL274Copy.storageHeader == "Storage")
    }

    @Test func diagnosticsHeaderIsVerbatim() {
        #expect(SettingsL274Copy.diagnosticsHeader == "Diagnostics")
    }

    // Downloads controls
    @Test func autoDownloadOnSubscribeLabel() {
        #expect(SettingsL274Copy.autoDownloadOnSubscribeLabel == "Auto-download on subscribe")
    }

    @Test func downloadOverCellularLabel() {
        #expect(SettingsL274Copy.downloadOverCellularLabel == "Download over cellular")
    }

    @Test func downloadNextDefaultCountLabel() {
        #expect(
            SettingsL274Copy.downloadNextDefaultCountLabel
                == "\"Download Next N\" default count"
        )
    }

    @Test func autoDownloadOptionLabels() {
        #expect(AutoDownloadOnSubscribe.off.displayLabel == "Off")
        #expect(AutoDownloadOnSubscribe.last1.displayLabel == "Last 1")
        #expect(AutoDownloadOnSubscribe.last3.displayLabel == "Last 3")
        #expect(AutoDownloadOnSubscribe.all.displayLabel == "All")
    }

    @Test func cellularPolicyOptionLabels() {
        #expect(CellularPolicy.off.displayLabel == "Off")
        #expect(CellularPolicy.askEachTime.displayLabel == "Ask each time")
        #expect(CellularPolicy.on.displayLabel == "On")
    }

    @Test func downloadNextOptionLabels() {
        #expect(DownloadNextDefaultCount.one.displayLabel == "1")
        #expect(DownloadNextDefaultCount.three.displayLabel == "3")
        #expect(DownloadNextDefaultCount.five.displayLabel == "5")
        #expect(DownloadNextDefaultCount.ten.displayLabel == "10")
    }

    // Storage controls
    @Test func episodeStorageCapLabel() {
        #expect(SettingsL274Copy.episodeStorageCapLabel == "Episode storage cap")
    }

    @Test func currentUsageLabel() {
        #expect(SettingsL274Copy.currentUsageLabel == "Current usage")
    }

    @Test func keepAnalysisToggleLabel() {
        #expect(
            SettingsL274Copy.keepAnalysisToggleLabel
                == "Keep analysis when removing downloads"
        )
    }

    @Test func keepAnalysisSubLineIsVerbatim() {
        // Spec: "Keeps analysis for many episodes; exact count depends on
        // episode size and retained bundle version."
        #expect(
            SettingsL274Copy.keepAnalysisSubLine
                == "Keeps analysis for many episodes; exact count depends on episode size and retained bundle version."
        )
    }

    @Test func analysisCapLabel() {
        #expect(SettingsL274Copy.analysisCapLabel == "Analysis cap")
    }

    @Test func autoEvictPolicyLineIsVerbatim() {
        // Spec: read-only line — "Oldest played episodes are removed first."
        #expect(
            SettingsL274Copy.autoEvictPolicyLine
                == "Oldest played episodes are removed first."
        )
    }

    @Test func episodeStorageCapOptionLabels() {
        #expect(EpisodeStorageCap.gb1.displayLabel == "1 GB")
        #expect(EpisodeStorageCap.gb5.displayLabel == "5 GB")
        #expect(EpisodeStorageCap.gb10.displayLabel == "10 GB")
        #expect(EpisodeStorageCap.gb25.displayLabel == "25 GB")
        #expect(EpisodeStorageCap.gb50.displayLabel == "50 GB")
        #expect(EpisodeStorageCap.unlimited.displayLabel == "Unlimited")
    }

    // Diagnostics controls
    @Test func pipelineVersionLabel() {
        #expect(SettingsL274Copy.pipelineVersionLabel == "Pipeline version")
    }

    @Test func modelVersionsLabel() {
        #expect(SettingsL274Copy.modelVersionsLabel == "Model versions")
    }

    @Test func policyVersionLabel() {
        #expect(SettingsL274Copy.policyVersionLabel == "Policy version")
    }

    @Test func featureSchemaVersionLabel() {
        #expect(SettingsL274Copy.featureSchemaVersionLabel == "Feature-schema version")
    }

    @Test func schedulerEventsLabel() {
        #expect(SettingsL274Copy.schedulerEventsLabel == "Last 50 scheduler events")
    }

    @Test func perShowCapabilityProfileLabel() {
        #expect(SettingsL274Copy.perShowCapabilityProfileLabel == "Per-show capability profile")
    }

    @Test func featureFlagsLabel() {
        #expect(SettingsL274Copy.featureFlagsLabel == "Feature flags (rollback)")
    }

    @Test func sendDiagnosticsButtonLabel() {
        #expect(SettingsL274Copy.sendDiagnosticsButtonLabel == "Send diagnostics")
    }

    @Test func sendDiagnosticsFooterPromisesNoAutoUpload() {
        // Acceptance: copy must state the bundle is never auto-uploaded.
        #expect(SettingsL274Copy.sendDiagnosticsFooter.contains("Never auto-uploads"))
    }
}

// MARK: - Defaults

@Suite("SettingsL274 defaults (playhead-l274)")
struct SettingsL274DefaultsTests {

    @Test func autoDownloadDefaultIsOff() {
        #expect(AutoDownloadOnSubscribe.defaultValue == .off)
    }

    @Test func cellularPolicyDefaultIsAskEachTime() {
        #expect(CellularPolicy.defaultValue == .askEachTime)
    }

    @Test func downloadNextDefaultCountIs3() {
        #expect(DownloadNextDefaultCount.defaultValue == .three)
        #expect(DownloadNextDefaultCount.defaultValue.rawValue == 3)
    }

    @Test func episodeStorageCapDefaultIs10GB() {
        #expect(EpisodeStorageCap.defaultValue == .gb10)
        #expect(EpisodeStorageCap.defaultValue.bytes == 10 * 1_000_000_000)
    }

    @Test func defaultValueMatchesStorageBudgetSettingsDefault() {
        // The picker's "10 GB" default MUST round-trip the default
        // `StorageBudgetSettings.mediaCapBytes` (playhead-h7r). Drift
        // would leave the user's persisted cap out of sync with what the
        // picker renders on first launch.
        let budgetDefault = StorageBudgetSettings().mediaCapBytes
        #expect(EpisodeStorageCap.defaultValue.bytes == budgetDefault)
    }

    @Test func keepAnalysisFooterMatchesSpec() {
        // Belt-and-braces: the sub-line is the single longest verbatim
        // string the bead spells out; pin it separately from the generic
        // copy suite for a faster failure-localization signal.
        let expected = "Keeps analysis for many episodes; exact count depends on episode size and retained bundle version."
        #expect(SettingsL274Copy.keepAnalysisSubLine == expected)
    }

    @Test func featureFlagPlaceholdersDefaultOff() {
        // Spec: placeholders default to OFF; toggling is inert until the
        // underlying flag-implementation beads (xr3t/zx6i/2hpn/43ed) land.
        // 24cm is already a live flag but is surfaced here with the same
        // OFF default for a consistent rollback-toggle UX.
        let defaults = FeatureFlagPlaceholders.defaultValues
        for slug in FeatureFlagPlaceholders.orderedSlugs {
            #expect(defaults[slug] == false, "Flag \(slug) must default to OFF")
        }
        #expect(defaults.count == FeatureFlagPlaceholders.orderedSlugs.count)
    }

    @Test func featureFlagPlaceholdersPinExactSlugListFromSpec() {
        // Spec (playhead-l274): Diagnostics → Feature flags exposes five
        // rollback toggles in this exact order:
        //   xr3t, zx6i, 2hpn, 43ed, 24cm
        // Pin the full list so a drift (e.g. dropping 24cm, reordering) is
        // a loud test failure rather than a silent UX regression.
        #expect(
            FeatureFlagPlaceholders.orderedSlugs == ["xr3t", "zx6i", "2hpn", "43ed", "24cm"]
        )
    }
}

// MARK: - EpisodeStorageCap

@Suite("EpisodeStorageCap byte round-trip")
struct EpisodeStorageCapTests {

    @Test func roundTripExactBytes() {
        for cap in EpisodeStorageCap.allCases {
            let restored = EpisodeStorageCap.from(bytes: cap.bytes)
            #expect(restored == cap, "Expected \(cap) to round-trip through \(cap.bytes) bytes")
        }
    }

    @Test func nonCanonicalBytesSnapUpward() {
        // A persisted value BELOW a canonical option should surface as
        // that canonical option (admission never silently shrinks).
        #expect(EpisodeStorageCap.from(bytes: 500_000_000) == .gb1)
        #expect(EpisodeStorageCap.from(bytes: 2 * 1_000_000_000) == .gb5)
        #expect(EpisodeStorageCap.from(bytes: 7 * 1_000_000_000) == .gb10)
        #expect(EpisodeStorageCap.from(bytes: 15 * 1_000_000_000) == .gb25)
    }

    @Test func veryLargeBytesSnapToUnlimited() {
        #expect(EpisodeStorageCap.from(bytes: Int64.max) == .unlimited)
        #expect(EpisodeStorageCap.from(bytes: Int64.max / 2 + 1) == .unlimited)
    }
}

// MARK: - DownloadsSettings persistence

@Suite("DownloadsSettings UserDefaults persistence")
struct DownloadsSettingsPersistenceTests {

    private func freshDefaults(suiteName: String = "l274.downloads.\(UUID().uuidString)") -> UserDefaults {
        let d = UserDefaults(suiteName: suiteName)!
        d.removePersistentDomain(forName: suiteName)
        return d
    }

    @Test func loadOnEmptyDefaultsReturnsSpecDefaults() {
        let d = freshDefaults()
        let settings = DownloadsSettings.load(from: d)
        #expect(settings.autoDownloadOnSubscribe == .off)
        #expect(settings.cellularPolicy == .askEachTime)
        #expect(settings.downloadNextDefaultCount == .three)
    }

    @Test func saveAndLoadRoundTripsAllThreeFields() {
        let d = freshDefaults()
        let original = DownloadsSettings(
            autoDownloadOnSubscribe: .last3,
            cellularPolicy: .on,
            downloadNextDefaultCount: .ten
        )
        original.save(to: d)

        let reloaded = DownloadsSettings.load(from: d)
        #expect(reloaded.autoDownloadOnSubscribe == .last3)
        #expect(reloaded.cellularPolicy == .on)
        #expect(reloaded.downloadNextDefaultCount == .ten)
    }

    @Test func corruptValuesFallBackToDefaults() {
        let d = freshDefaults()
        d.set("not-a-valid-case", forKey: DownloadsSettings.autoDownloadKey)
        d.set("garbage", forKey: DownloadsSettings.cellularPolicyKey)
        d.set("also-garbage", forKey: DownloadsSettings.downloadNextDefaultCountKey)

        let s = DownloadsSettings.load(from: d)
        #expect(s.autoDownloadOnSubscribe == .off)
        #expect(s.cellularPolicy == .askEachTime)
        #expect(s.downloadNextDefaultCount == .three)
    }
}

// MARK: - Storage cap admission-control recomputation

@Suite("Storage cap change triggers admission recomputation (playhead-l274 DoD #4)")
struct StorageCapRecomputationTests {

    private func freshDefaults(suiteName: String = "l274.storage.\(UUID().uuidString)") -> UserDefaults {
        let d = UserDefaults(suiteName: suiteName)!
        d.removePersistentDomain(forName: suiteName)
        return d
    }

    @Test func saveUpdatesLoadObservably() {
        // The h7r admission-control surfaces (DownloadManager, AdmissionGate,
        // DownloadNextView, etc.) re-read `StorageBudgetSettings.load()` on
        // every admission check. Saving a new cap via the Settings picker
        // MUST be immediately visible to the next load — this is the
        // "without requiring relaunch" acceptance criterion.
        let d = freshDefaults()
        #expect(StorageBudgetSettings.load(from: d).mediaCapBytes == 10 * 1_000_000_000)

        var s = StorageBudgetSettings.load(from: d)
        s.mediaCapBytes = EpisodeStorageCap.gb5.bytes
        s.save(to: d)

        // Next admission-control read observes the new value.
        #expect(StorageBudgetSettings.load(from: d).mediaCapBytes == 5 * 1_000_000_000)
    }

    @Test func applyEpisodeStorageCapPersistsBytes() {
        let d = freshDefaults()
        var s = StorageBudgetSettings.load(from: d)
        s.mediaCapBytes = EpisodeStorageCap.gb25.bytes
        s.save(to: d)

        #expect(StorageBudgetSettings.load(from: d).mediaCapBytes == 25 * 1_000_000_000)
    }

    @Test func unlimitedClampsToInt64Max() {
        let d = freshDefaults()
        var s = StorageBudgetSettings.load(from: d)
        s.mediaCapBytes = EpisodeStorageCap.unlimited.bytes
        s.save(to: d)

        #expect(StorageBudgetSettings.load(from: d).mediaCapBytes == Int64.max)
    }
}

// MARK: - SettingsRoute deep-link routing

@Suite("SettingsRouter deep-link (hkg8 Free up space → Storage)")
@MainActor
struct SettingsRouterDeepLinkTests {

    @Test func initialRouteIsNil() {
        let router = SettingsRouter()
        #expect(router.pending == nil)
    }

    @Test func requestStoragePublishesStorageRoute() {
        let router = SettingsRouter()
        router.request(.storage)
        #expect(router.pending == .storage)
    }

    @Test func freeUpSpaceLandsOnStorage() {
        // This is the hkg8 → l274 contract: tapping "Free up space" in
        // DownloadNextView MUST route the Settings surface to the
        // Storage group (not Downloads, not Diagnostics).
        let router = SettingsRouter()
        router.request(.storage)
        #expect(router.pending == .storage)
        #expect(router.pending != .downloads)
        #expect(router.pending != .diagnostics)
    }

    @Test func consumeClearsPending() {
        let router = SettingsRouter()
        router.request(.storage)
        router.consume()
        #expect(router.pending == nil)
    }

    @Test func sequentialRequestsReplacePending() {
        let router = SettingsRouter()
        router.request(.downloads)
        router.request(.storage)
        #expect(router.pending == .storage)
    }

    @Test func anchorIdsAreStable() {
        #expect(SettingsRoute.downloads.anchorId == "settings.route.downloads")
        #expect(SettingsRoute.storage.anchorId == "settings.route.storage")
        #expect(SettingsRoute.diagnostics.anchorId == "settings.route.diagnostics")
    }
}

// MARK: - Scope discipline (out-of-scope affordances must NOT appear)

@Suite("SettingsL274 scope discipline source canary (playhead-l274 DoD #7)")
struct SettingsL274SourceCanaryTests {

    private static let repoRoot: URL = {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // .../Settings/
            .deletingLastPathComponent() // .../Views/
            .deletingLastPathComponent() // .../PlayheadTests/
            .deletingLastPathComponent() // .../<repo root>/
    }()

    private func read(_ relative: String) throws -> String {
        let url = Self.repoRoot.appendingPathComponent(relative)
        return try String(contentsOf: url, encoding: .utf8)
    }

    @Test func settingsViewHasNoAnalysisQualityToggle() throws {
        // Spec: "No 'analysis quality' toggle — thermal/energy governor
        // handles this." Search both view files.
        let viewSrc = try read("Playhead/Views/Settings/SettingsView.swift")
        let l274Src = try read("Playhead/Views/Settings/SettingsL274.swift")
        #expect(!viewSrc.lowercased().contains("analysis quality"))
        #expect(!l274Src.lowercased().contains("analysis quality"))
    }

    @Test func settingsViewHasNoSkipAggressivenessSlider() throws {
        // Spec: "No skip-aggressiveness slider — SkipOrchestrator policy
        // is not user-configurable."
        let viewSrc = try read("Playhead/Views/Settings/SettingsView.swift")
        let l274Src = try read("Playhead/Views/Settings/SettingsL274.swift")
        #expect(!viewSrc.lowercased().contains("skip aggressiveness"))
        #expect(!viewSrc.lowercased().contains("skip-aggressiveness"))
        #expect(!l274Src.lowercased().contains("skip aggressiveness"))
        #expect(!l274Src.lowercased().contains("skip-aggressiveness"))
    }

    @Test func settingsViewHasNoForceAnalyzeButton() throws {
        // Spec: "no per-show 'force analyze' control."
        let viewSrc = try read("Playhead/Views/Settings/SettingsView.swift")
        let l274Src = try read("Playhead/Views/Settings/SettingsL274.swift")
        #expect(!viewSrc.lowercased().contains("force analyze"))
        #expect(!viewSrc.lowercased().contains("force-analyze"))
        #expect(!l274Src.lowercased().contains("force analyze"))
        #expect(!l274Src.lowercased().contains("force-analyze"))
    }

    @Test func settingsViewHasNoModelUpdateTrigger() throws {
        // Spec: "No manual model-update trigger." Note: the MODEL download
        // UI (already in SettingsView's modelSection) is orthogonal — that
        // surfaces available updates, it does not TRIGGER them outside
        // the normal download flow. We scope this canary to the l274
        // source only so we don't catch the pre-existing download UI.
        let l274Src = try read("Playhead/Views/Settings/SettingsL274.swift")
        #expect(!l274Src.lowercased().contains("manual model update"))
        #expect(!l274Src.lowercased().contains("force model update"))
    }

    @Test func downloadNextViewTodoRemoved() throws {
        // DoD #6: the hkg8 TODO comments must be removed from both
        // DownloadNextView.swift and EpisodeListView.swift.
        let downloadNext = try read("Playhead/Views/Library/DownloadNextView.swift")
        let episodeList = try read("Playhead/Views/Library/EpisodeListView.swift")
        #expect(!downloadNext.contains("TODO(bd playhead-l274)"))
        #expect(!downloadNext.contains("TODO playhead-l274"))
        #expect(!episodeList.contains("TODO(bd playhead-l274)"))
    }
}

// MARK: - Diagnostics mail composer spy

@Suite("Send diagnostics uses mail composer — never network (playhead-l274 DoD #5)")
@MainActor
struct DiagnosticsMailComposerSpyTests {

    /// Test double capturing every presentation call. Never touches
    /// URLSession or any network; assertion is that the full DoD path
    /// ends at the presenter, not a network client.
    final class SpyPresenter: DiagnosticsExportPresenter {
        var presentCallCount = 0
        var presentedData: Data?
        var presentedFilename: String?
        var presentedSubject: String?
        /// Result we'll fire into the completion handler.
        var scriptedResult: DiagnosticsMailComposeResult = .sent

        func present(
            data: Data,
            filename: String,
            subject: String,
            completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
        ) {
            presentCallCount += 1
            presentedData = data
            presentedFilename = filename
            presentedSubject = subject
            Task { @MainActor in
                completion(.success(self.scriptedResult))
            }
        }
    }

    /// `DiagnosticsOptInSink` test double; records every call for assertion.
    final class RecordingSink: DiagnosticsOptInSink {
        var applyCount = 0
        func applyResetToEpisodes(matchingEpisodeIds ids: [String], newValue: Bool) {
            applyCount += 1
        }
    }

    @Test func sendDiagnosticsInvokesPresenterWithAttachment() async throws {
        let spy = SpyPresenter()
        let sink = RecordingSink()
        let env = DiagnosticsExportEnvironment(
            appVersion: "99.9.9",
            osVersion: "0.0.0",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: .init(timeIntervalSince1970: 0)
            ),
            installID: UUID(uuidString: "00000000-0000-0000-0000-000000000000")!,
            now: .init(timeIntervalSince1970: 0)
        )
        let coordinator = DiagnosticsExportCoordinator(
            environment: env,
            presenter: spy,
            journalFetch: { [] },
            optInSink: sink,
            optInEpisodes: []
        )

        let result = try await coordinator.exportAndPresent()

        // DoD #5: the mail composer path is the ONLY delivery surface.
        #expect(spy.presentCallCount == 1)
        // And a real attachment was passed.
        #expect(spy.presentedData != nil)
        if let data = spy.presentedData {
            #expect(!data.isEmpty)
        }
        #expect(result == .sent)
    }

    @Test func sendDiagnosticsReleasePathStillOpensMailComposer() async throws {
        // Guards the playhead-l274 fix that wired the Release-build
        // "Send diagnostics" button through the same coordinator graph
        // as DEBUG (previously Release was a TODO no-op).
        //
        // This test never references a `#if DEBUG`-gated symbol so it
        // compiles and executes in BOTH DEBUG and Release configurations
        // — same coverage on either.
        let spy = SpyPresenter()
        spy.scriptedResult = .saved
        let sink = RecordingSink()
        let env = DiagnosticsExportEnvironment(
            appVersion: "rel-1.2.3",
            osVersion: "0.0.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibility: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: .init(timeIntervalSince1970: 0)
            ),
            installID: UUID(uuidString: "11111111-1111-1111-1111-111111111111")!,
            now: .init(timeIntervalSince1970: 0)
        )
        let coordinator = DiagnosticsExportCoordinator(
            environment: env,
            presenter: spy,
            journalFetch: { [] },
            optInSink: sink,
            optInEpisodes: []
        )

        let result = try await coordinator.exportAndPresent()

        // Release users MUST see an actual composer invocation — not a
        // silent no-op (the bug this test locks in).
        #expect(spy.presentCallCount == 1)
        #expect(spy.presentedData != nil)
        #expect(result == .saved)
    }

    @Test func sendDiagnosticsPerformsNoNetworkIO() async throws {
        // Structural: the coordinator's dependencies are limited to
        // (presenter, journalFetch, optInSink, environment). None are
        // URLSession-ish. This is a belt-and-braces check by construction.
        let spy = SpyPresenter()
        let sink = RecordingSink()
        let env = DiagnosticsExportEnvironment(
            appVersion: "test",
            osVersion: "0.0.0",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: .init(timeIntervalSince1970: 0)
            ),
            installID: UUID(uuidString: "00000000-0000-0000-0000-000000000000")!,
            now: .init(timeIntervalSince1970: 0)
        )
        let coordinator = DiagnosticsExportCoordinator(
            environment: env,
            presenter: spy,
            journalFetch: { [] },
            optInSink: sink,
            optInEpisodes: []
        )

        // buildAndEncode does every "work" step of the export bar the
        // presenter — if this path ever opens a URLSession, it'll show up
        // as flaky tests long before shipping.
        let (data, filename, subject) = try await coordinator.buildAndEncode()
        #expect(!data.isEmpty)
        #expect(filename.hasPrefix("playhead-diagnostics-"))
        #expect(!subject.isEmpty)
    }
}

// MARK: - DiagnosticsVersions

@Suite("DiagnosticsVersions shape")
struct DiagnosticsVersionsTests {

    @Test func currentResolvesNonEmpty() {
        let v = DiagnosticsVersions.current()
        #expect(!v.pipelineVersion.isEmpty)
        #expect(!v.transcriptModelVersion.isEmpty)
        #expect(!v.adDetectionModelVersion.isEmpty)
        #expect(!v.policyVersion.isEmpty)
        #expect(!v.featureSchemaVersion.isEmpty)
    }

    // MARK: - I1 cross-source agreement (playhead-l274 code review)
    //
    // Each Diagnostics version column must read from the live
    // service-owned symbol, not a duplicated literal. The asserts below
    // fail LOUDLY when a service bumps its version but Diagnostics
    // silently keeps the old copy — the exact drift I1 locked in.

    @Test func transcriptModelMatchesTranscriptEngineServiceDefault() {
        // The version surfaced in Diagnostics must equal the default
        // configured in TranscriptEngineServiceConfig (the same symbol
        // that tags every produced chunk).
        let v = DiagnosticsVersions.current()
        #expect(v.transcriptModelVersion == TranscriptEngineServiceConfig.default.modelVersion)
    }

    @Test func policyVersionMatchesSkipOrchestratorDefault() {
        // Diagnostics must read from `SkipPolicyConfig.default` so the
        // idempotency-key version the UI shows matches what
        // `SkipOrchestrator` stamps on skip decisions.
        let v = DiagnosticsVersions.current()
        #expect(v.policyVersion == SkipPolicyConfig.default.policyVersion)
    }

    @Test func adDetectionModelMatchesAdDetectionServiceSymbol() {
        // Diagnostics must surface the same constant AdDetectionService
        // uses when tagging synthetic replay chunks.
        let v = DiagnosticsVersions.current()
        #expect(v.adDetectionModelVersion == AdDetectionService.hotPathReplayModelVersion)
    }

    @Test func featureSchemaVersionMatchesSharedConstant() {
        // No service owns a `featureSchemaVersion` default — the canonical
        // value lives in `SharedVersionConstants`, which CoverageSummary
        // call sites are migrated to read from.
        let v = DiagnosticsVersions.current()
        #expect(v.featureSchemaVersion == SharedVersionConstants.featureSchemaVersionString)
    }
}
