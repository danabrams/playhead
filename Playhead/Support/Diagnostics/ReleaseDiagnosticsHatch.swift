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

    let presenter = UIKitDiagnosticsPresenter(hostProvider: hostProvider)
    let coordinator = DiagnosticsExportCoordinator(
        environment: environment,
        presenter: presenter,
        journalFetch: journalFetch,
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
