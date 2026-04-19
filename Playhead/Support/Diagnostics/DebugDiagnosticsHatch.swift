// DebugDiagnosticsHatch.swift
// Debug-only Settings → Send diagnostics hatch for Phase 1.5 Wave 4
// dogfooders.
//
// Scope: playhead-ct2q. The full Phase 2 Settings screen (Downloads,
// Storage, per-episode opt-in UI, Release-build visibility) ships in
// playhead-l274. This file is the MINIMAL call site that lets dogfood
// push a support-safe diagnostics bundle back to the team NOW — it
// wires the coordinator + presenter + sink + journal adapter + install
// ID provider that playhead-ghon landed but left without a production
// call site.
//
// Design shape:
//   * #if DEBUG compilation guard on the entire file. Release builds
//     cannot compile-reference any symbol in here — that guarantee is
//     tested by `DebugDiagnosticsHatchSourceCanaryTests` (grep of this
//     file + the Settings call site), and exercised at runtime by the
//     `#if DEBUG` wrapper around the Settings button itself.
//   * One free function — `runDebugDiagnosticsExport(...)` — is the
//     only surface. `SettingsView` fires it from a Task on button tap.
//   * The function wires the five ghon dependencies:
//        1. `InstallIDProvider(context: modelContext)` — closes ghon I4.
//        2. `DiagnosticsExportEnvironment` built from `Bundle.main`,
//           `ProcessInfo`, `DeviceClass.detect()`, `BuildType.detect()`,
//           `CapabilitiesService.currentSnapshot`, and the install UUID.
//        3. `journalFetch = { try await runtime.analysisStore
//                              .fetchRecentWorkJournalEntries(limit: 200) }`
//        4. `optInSink = SwiftDataDiagnosticsOptInSink(context: modelContext)`
//        5. `presenter = UIKitDiagnosticsPresenter(hostProvider: ...)`
//     and invokes `coordinator.exportAndPresent()`.
//   * `optInEpisodes: []` — dogfood uses the default (non-opt-in)
//     bundle per the ct2q spec. `Episode.diagnosticsOptIn` stays false.
//   * The journalFetch adapter calls the existing actor-isolated
//     `AnalysisStore.fetchRecentWorkJournalEntries(limit:)` which
//     returns DESC-sorted rows. The `DiagnosticsBundleBuilder` sorts
//     its input ASC internally (see ghon b04acec), so passing
//     DESC-sorted rows through produces spec-compliant output.
//
// Eligibility shape: the ct2q hatch does not have a first-class
// `AnalysisEligibilityEvaluator` wired into `PlayheadRuntime` (l274
// will add one). To avoid a unilateral architectural expansion we map
// the existing `CapabilitySnapshot` fields into an `AnalysisEligibility`
// struct for the bundle's eligibility_snapshot. The mapping is
// approximate but honest for dogfood triage: false fields mean
// "features gated right now", which is exactly what the support
// engineer needs to see.
//
// Placement note: this file lives under `Playhead/Support/Diagnostics/`
// (not `Playhead/Views/Settings/`) because it references the
// persistence-layer `AnalysisStore` type directly when building the
// journal-fetch adapter, and `SurfaceStatusUILintTests` (playhead-ol05)
// forbids that reference from any `Playhead/Views/` source file. The
// caller in `SettingsView` sees only the top-level
// `runDebugDiagnosticsExport(runtime:modelContext:)` entry point, which
// is the narrowest possible view surface and keeps `Views/` clean of
// scheduler/persistence types.

#if DEBUG

import Foundation
import SwiftData

#if canImport(UIKit) && os(iOS)
import UIKit

// MARK: - Entry point (iOS only)

/// Build, present, and reset-apply a debug diagnostics bundle from the
/// current app session.
///
/// Expected caller: `SettingsView.sendDiagnosticsSection`'s button, wrapped
/// in its own `#if DEBUG`. The Task invocation is fire-and-forget: errors
/// bubble up to the caller only for "we never got to presentation"
/// conditions (`DiagnosticsExportError.missingHostViewController` and the
/// like); composer outcomes (`.cancelled`, `.failed`) are returned as
/// `DiagnosticsMailComposeResult` values, not thrown.
///
/// - Returns: the final `DiagnosticsMailComposeResult` from the presenter,
///   which mirrors what `DiagnosticsExportCoordinator.exportAndPresent()`
///   surfaces.
@MainActor
@discardableResult
func runDebugDiagnosticsExport(
    runtime: PlayheadRuntime,
    modelContext: ModelContext,
    hostProvider: @MainActor @escaping () -> UIViewController? = DebugDiagnosticsHatch.defaultHostProvider
) async throws -> DiagnosticsMailComposeResult {
    let environment = try await DebugDiagnosticsHatch.buildEnvironment(
        runtime: runtime,
        modelContext: modelContext
    )
    let journalFetch = DebugDiagnosticsHatch.makeJournalFetch(store: runtime.analysisStore)
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
#endif

// MARK: - Hatch helpers (internal for tests)

/// Namespace for the debug-only hatch helpers. Exposed at internal
/// access so `DebugDiagnosticsHatchTests` can assert each piece in
/// isolation without driving the presenter (simulator-hostile for a
/// true UI test).
@MainActor
enum DebugDiagnosticsHatch {

    /// Cap the tail fetch at 200 rows — matches
    /// `DiagnosticsBundleBuilder.schedulerEventsCap`, which is the
    /// largest window either projection consumes. The builder's
    /// `work_journal_tail` projection takes the most-recent 50 rows from
    /// this same input (bounded by `workJournalTailCap`), so 200 is
    /// adequate for both.
    static let journalFetchLimit = 200

    // MARK: Journal adapter

    /// Adapter from `AnalysisStore.fetchRecentWorkJournalEntries(limit:)`
    /// to the `DiagnosticsJournalFetch` closure the coordinator
    /// consumes. The store returns rows DESC-sorted by `(timestamp,
    /// rowid)`; the builder sorts ASC internally before taking the
    /// suffix, so DESC input is correct (see ghon b04acec).
    static func makeJournalFetch(store: AnalysisStore) -> DiagnosticsJournalFetch {
        { [store] in
            try await store.fetchRecentWorkJournalEntries(limit: journalFetchLimit)
        }
    }

    // MARK: Environment construction

    /// Build a `DiagnosticsExportEnvironment` from the live runtime +
    /// model context. Async because it needs to (1) await the current
    /// `CapabilitySnapshot` off the `CapabilitiesService` actor, and
    /// (2) provision the install UUID from SwiftData.
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

    /// Derive an `AnalysisEligibility` from the live `CapabilitySnapshot`.
    /// Approximate but honest: the goal is for the diagnostics JSON to
    /// tell the support engineer which gates were blocking analysis at
    /// export time, and `CapabilitySnapshot` already tracks those gates.
    ///
    /// Field-by-field:
    /// - `hardwareSupported` ← `snapshot.foundationModelsAvailable`
    ///   (the framework-available flag is the closest proxy for "the
    ///   SoC meets the minimum bar" that the runtime surfaces today).
    /// - `appleIntelligenceEnabled` ← `snapshot.appleIntelligenceEnabled`
    ///   (direct match).
    /// - `regionSupported` ← `true` (dogfood is US-only; l274 will
    ///   replace this with a real region provider).
    /// - `languageSupported` ← `snapshot.foundationModelsLocaleSupported`
    ///   (direct match — locale support is the language gate).
    /// - `modelAvailableNow` ← `snapshot.foundationModelsUsable` (the
    ///   live-probe flag, which flips false when the model is not
    ///   currently loadable).
    static func eligibility(
        from snapshot: CapabilitySnapshot,
        now: Date = .now
    ) -> AnalysisEligibility {
        AnalysisEligibility(
            hardwareSupported: snapshot.foundationModelsAvailable,
            appleIntelligenceEnabled: snapshot.appleIntelligenceEnabled,
            regionSupported: true,
            languageSupported: snapshot.foundationModelsLocaleSupported,
            modelAvailableNow: snapshot.foundationModelsUsable,
            capturedAt: now
        )
    }

    // MARK: Bundle version / OS helpers

    /// `CFBundleShortVersionString` or `"unknown"`. Matches the format
    /// used elsewhere in the codebase (`ScanCohort.production()`).
    static func appVersionString() -> String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "unknown"
    }

    /// `"<major>.<minor>.<patch>"` from `ProcessInfo.operatingSystemVersion`,
    /// matching the shape `ScanCohort.production()` uses.
    static func osVersionString() -> String {
        let v = ProcessInfo.processInfo.operatingSystemVersion
        return "\(v.majorVersion).\(v.minorVersion).\(v.patchVersion)"
    }

    // MARK: Default host provider

    #if canImport(UIKit) && os(iOS)
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
    #else
    static let defaultHostProvider: @MainActor () -> UIViewController? = { nil }
    #endif
}

#endif // DEBUG
