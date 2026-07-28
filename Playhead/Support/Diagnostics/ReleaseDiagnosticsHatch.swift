// ReleaseDiagnosticsHatch.swift
// Release-build sibling of `DebugDiagnosticsHatch.swift`.
//
// Scope (playhead-l274 code-review I3): the Release-build "Send
// diagnostics" path previously lived inline as a 60-line method on
// `SettingsView`. Extracting it here mirrors the DEBUG hatch's shape
// (one free function + an `enum` namespace for helpers) so the two
// code paths are easier to read side-by-side and the view body stays
// slim.
//
// Design shape (mirrors DebugDiagnosticsHatch):
//   * File-level compilation guard: `#if !DEBUG && canImport(UIKit) && os(iOS)`.
//     Release iOS builds are the only configuration where the Release
//     coordinator assembly is needed — in DEBUG the existing
//     `runDebugDiagnosticsExport` covers the same path.
//   * One free function — `runReleaseDiagnosticsExport(...)` — is the
//     only view-visible surface. `SettingsView` fires it from a Task
//     inside the same `#if !DEBUG && canImport(UIKit) && os(iOS)` block.
//   * Helpers (environment construction, default host provider) live on
//     the internal `ReleaseDiagnosticsHatch` namespace so tests can
//     invoke each piece in isolation without standing up a presenter.
//
// The coordinator graph is byte-for-byte identical to DEBUG:
//   1. `InstallIDProvider(context: modelContext).installID()`
//   2. `DiagnosticsExportEnvironment` from `Bundle.main`, `ProcessInfo`,
//      `DeviceClass.detect()`, `BuildType.detect()`, the live
//      `CapabilitySnapshot`, and the install UUID.
//   3. `journalFetch` adapter over `runtime.analysisStore`.
//   4. `SwiftDataDiagnosticsOptInSink(context: modelContext)`.
//   5. `UIKitDiagnosticsPresenter` with a key-window host provider.
//
// Placement note: lives under `Playhead/Support/Diagnostics/` (not
// `Playhead/Views/Settings/`) for the same reason as the DEBUG sibling —
// it references the persistence-layer `AnalysisStore` type directly when
// building the journal-fetch adapter, and `SurfaceStatusUILintTests`
// (playhead-ol05) forbids that reference from any `Playhead/Views/`
// source file. The caller in `SettingsView` sees only the top-level
// `runReleaseDiagnosticsExport(runtime:modelContext:)` entry point.

#if !DEBUG && canImport(UIKit) && os(iOS)

import Foundation
import SwiftData
import UIKit

// MARK: - Entry point

/// Build, present, and reset-apply a Release-path diagnostics bundle
/// from the current app session. Release sibling of
/// `runDebugDiagnosticsExport`.
///
/// Expected caller: `SettingsView.diagnosticsSection`'s "Send
/// diagnostics" button, wrapped in `#if !DEBUG && canImport(UIKit) && os(iOS)`.
/// The Task invocation is fire-and-forget: errors bubble up only for
/// "we never got to presentation" conditions
/// (`DiagnosticsExportError.missingHostViewController` and the like);
/// composer outcomes (`.cancelled`, `.failed`) are returned as
/// `DiagnosticsMailComposeResult` values, not thrown.
///
/// - Returns: the final `DiagnosticsMailComposeResult` from the presenter,
///   which mirrors what `DiagnosticsExportCoordinator.exportAndPresent()`
///   surfaces.
@MainActor
@discardableResult
func runReleaseDiagnosticsExport(
    runtime: PlayheadRuntime,
    modelContext: ModelContext,
    hostProvider: @MainActor @escaping () -> UIViewController? = ReleaseDiagnosticsHatch.defaultHostProvider
) async throws -> DiagnosticsMailComposeResult {
    let environment = try await ReleaseDiagnosticsHatch.buildEnvironment(
        runtime: runtime,
        modelContext: modelContext
    )
    let journalFetch = ReleaseDiagnosticsHatch.makeJournalFetch(store: runtime.analysisStore)
    let optInSink = SwiftDataDiagnosticsOptInSink(context: modelContext)
    let learnedDeviceProfilesFetch =
        ReleaseDiagnosticsHatch.makeLearnedDeviceProfilesFetch(modelContext: modelContext)

    // playhead-2hpn: source live `ShowMusicBedProfile` snapshots from
    // the SwiftData container so the diagnostics bundle's
    // `music_bed_profiles` field reflects whatever the runtime has
    // observed. Constructed here (not on the runtime) so the
    // ModelContainer dependency stays local to the App-scope wiring
    // surface, matching how `SwiftDataDiagnosticsOptInSink` is built.
    let musicBedStore = ShowMusicBedProfileStore(
        modelContainer: modelContext.container
    )
    let musicBedProfilesFetch: DiagnosticsMusicBedProfilesFetch = {
        await musicBedStore.allSnapshots()
    }

    let presenter = UIKitDiagnosticsPresenter(hostProvider: hostProvider)
    let coordinator = DiagnosticsExportCoordinator(
        environment: environment,
        presenter: presenter,
        journalFetch: journalFetch,
        musicBedProfilesFetch: musicBedProfilesFetch,
        learnedDeviceProfilesFetch: learnedDeviceProfilesFetch,
        stabilityFetch: ReleaseDiagnosticsHatch.stabilityFetch,
        bannerTalliesFetch: ReleaseDiagnosticsHatch.bannerTalliesFetch,
        rediffFetch: ReleaseDiagnosticsHatch.makeRediffFetch(store: runtime.analysisStore),
        optInSink: optInSink,
        optInEpisodes: []
    )
    return try await coordinator.exportAndPresent()
}

// MARK: - Hatch helpers (internal for tests)

/// Namespace for the Release-only hatch helpers. Mirrors
/// `DebugDiagnosticsHatch` one-for-one so the two files read
/// side-by-side.
@MainActor
enum ReleaseDiagnosticsHatch {

    /// Cap the tail fetch at 200 rows — matches
    /// `DiagnosticsBundleBuilder.schedulerEventsCap`.
    static let journalFetchLimit = 200

    // MARK: Journal adapter

    /// Adapter from `AnalysisStore.fetchRecentWorkJournalEntries(limit:)`
    /// to the `DiagnosticsJournalFetch` closure the coordinator consumes.
    /// The store returns rows DESC-sorted; the builder sorts ASC
    /// internally before taking the suffix, so DESC input is correct.
    static func makeJournalFetch(store: AnalysisStore) -> DiagnosticsJournalFetch {
        { [store] in
            try await store.fetchRecentWorkJournalEntries(limit: journalFetchLimit)
        }
    }

    // MARK: Learned device-profile adapter (playhead-beh3)

    /// Adapter that snapshots every `LearnedDeviceProfile` row through
    /// `SwiftDataLearnedDeviceProfileStore.snapshotSync()`, then maps
    /// each state to a wire-shape `LearnedDeviceProfileDiagnosticRecord`.
    /// Mirrors `DebugDiagnosticsHatch.makeLearnedDeviceProfilesFetch`
    /// (same isolation pattern; both files keep their helpers in
    /// lock-step).
    static func makeLearnedDeviceProfilesFetch(
        modelContext: ModelContext
    ) -> DiagnosticsLearnedDeviceProfilesFetch {
        let container = modelContext.container
        return { @Sendable in
            await MainActor.run { () -> [LearnedDeviceProfileDiagnosticRecord] in
                let context = ModelContext(container)
                let store = SwiftDataLearnedDeviceProfileStore(context: context)
                let snapshots = store.snapshotSync()
                return snapshots.map { LearnedDeviceProfileDiagnosticRecord.from(snapshot: $0) }
            }
        }
    }

    // MARK: Stability-diagnostics adapter (playhead-jw63.4)

    /// Reads the local MetricKit crash + hang ring buffer, newest first.
    /// `StabilityDiagnosticsStore.shared` is the process-wide handle the
    /// `MXMetricManagerSubscriber` writes into; there is no runtime or
    /// ModelContext dependency to thread through, so unlike the other
    /// adapters this is a stored closure rather than a factory.
    static let stabilityFetch: DiagnosticsStabilityFetch = {
        await StabilityDiagnosticsStore.shared.recent()
    }

    // MARK: Banner-tally adapter (playhead-bfq7)

    /// Reads the local per-episode banner-card tally, oldest session
    /// first. Like `stabilityFetch` this is a stored closure rather
    /// than a factory: `BannerTallyStore.shared` is the process-wide
    /// handle the production banner queue writes into, so there is no
    /// runtime or ModelContext dependency to thread through.
    static let bannerTalliesFetch: DiagnosticsBannerTalliesFetch = {
        await MainActor.run { BannerTallyStore.shared.sessions }
    }

    // MARK: Rediff-lane adapter (playhead-p70f)

    /// Cap on the `background_task_runs` rows fetched for the rediff entry
    /// point. Matches `DiagnosticsBundleBuilder.rediffBackgroundRunCap`.
    static let rediffBackgroundRunFetchLimit = 25

    /// Cap on per-asset rediff rows fetched. Matches
    /// `DiagnosticsBundleBuilder.rediffRowCap`, with headroom so the
    /// builder's own sort picks the newest rather than the store's.
    static let rediffRowFetchLimit = 200

    /// Adapter from the four rediff tables to the `DiagnosticsRediffFetch`
    /// closure the coordinator consumes (playhead-p70f change 4).
    ///
    /// Every read is independently `try?`-guarded and falls back to the empty
    /// value: a diagnostics export that fails because one rediff counter could
    /// not be read is a worse outcome than one that ships the other three.
    /// Rows carry the RAW `analysisAssetId`; the builder hashes it.
    static func makeRediffFetch(store: AnalysisStore) -> DiagnosticsRediffFetch {
        { [store] in
            async let bandwidth = try? await store.fetchRediffBandwidthTotals()
            async let states = try? await store.fetchRediffRefetchStates()
            async let dayZero = try? await store.fetchRediffDayZeroAttempts(limit: rediffRowFetchLimit)
            async let runs = try? await store.fetchRecentBackgroundTaskRuns(
                entryPoint: .rediffRefetch, limit: rediffBackgroundRunFetchLimit
            )
            return DiagnosticsRediffSnapshot(
                bandwidth: await bandwidth ?? RediffBandwidthTotals(),
                refetchStates: await states ?? [],
                dayZeroAttempts: await dayZero ?? [],
                backgroundRuns: await runs ?? []
            )
        }
    }

    // MARK: Environment construction

    /// Build a `DiagnosticsExportEnvironment` from the live runtime +
    /// model context. Async because it awaits the current
    /// `CapabilitySnapshot` off the `CapabilitiesService` actor and
    /// provisions the install UUID from SwiftData.
    static func buildEnvironment(
        runtime: PlayheadRuntime,
        modelContext: ModelContext,
        now: Date = .now
    ) async throws -> DiagnosticsExportEnvironment {
        let installID = try InstallIDProvider(context: modelContext).installID()
        let snapshot = await runtime.capabilitiesService.currentSnapshot
        let eligibility = eligibility(from: snapshot, now: now)
        return DiagnosticsExportEnvironment(
            appVersion: appVersionString(),
            osVersion: osVersionString(),
            deviceClass: DeviceClass.detect(),
            buildType: BuildType.detect(),
            eligibility: eligibility,
            installID: installID,
            now: now
        )
    }

    // MARK: CapabilitySnapshot → AnalysisEligibility mapping

    /// Mirrors `DebugDiagnosticsHatch.eligibility(from:now:)` — kept as a
    /// separate symbol so the Release code path has no `#if DEBUG` type
    /// dependencies. Field-by-field mapping is identical.
    static func eligibility(
        from snapshot: CapabilitySnapshot,
        now: Date = .now
    ) -> AnalysisEligibility {
        AnalysisEligibility(
            hardwareSupported: snapshot.foundationModelsAvailable,
            appleIntelligenceEnabled: snapshot.appleIntelligenceEnabled,
            regionSupported: LocaleRegionSupportProvider().isRegionSupported(),
            languageSupported: snapshot.foundationModelsLocaleSupported,
            modelAvailableNow: snapshot.foundationModelsUsable,
            capturedAt: now
        )
    }

    // MARK: Bundle version / OS helpers

    /// `CFBundleShortVersionString` or `"unknown"`.
    static func appVersionString() -> String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "unknown"
    }

    /// `"<major>.<minor>.<patch>"` from `ProcessInfo.operatingSystemVersion`.
    static func osVersionString() -> String {
        let v = ProcessInfo.processInfo.operatingSystemVersion
        return "\(v.majorVersion).\(v.minorVersion).\(v.patchVersion)"
    }

    // MARK: Default host provider

    /// Resolve the foreground-active window's rootmost-presented view
    /// controller. Returns `nil` when no active scene exists (background
    /// launch, scene-disconnected tests). The presenter surfaces
    /// `DiagnosticsExportError.missingHostViewController` to the caller
    /// in that case.
    static let defaultHostProvider: @MainActor () -> UIViewController? = {
        let scene = UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .first { $0.activationState == .foregroundActive }
            ?? UIApplication.shared.connectedScenes
                .compactMap { $0 as? UIWindowScene }
                .first
        guard let root = scene?.windows.first(where: \.isKeyWindow)?.rootViewController
            ?? scene?.windows.first?.rootViewController
        else { return nil }
        // Walk the presentation chain so the composer lands on top of
        // any modally presented sheet (e.g. Settings sheet).
        var current = root
        while let presented = current.presentedViewController {
            current = presented
        }
        return current
    }
}

#endif // !DEBUG && canImport(UIKit) && os(iOS)
