// AnalysisStoreRecoveryHatch.swift
// The view-visible entry points for the analysis-store recovery choice.
//
// Scope: playhead-wvdz.
//
// ----- Why a hatch rather than calls from the view -----
//
// Same reason `DebugDiagnosticsHatch` and `ReleaseDiagnosticsHatch` live
// here rather than under `Playhead/Views/Settings/`: they reference the
// persistence layer, and `SurfaceStatusUILintTests` (playhead-ol05)
// forbids that from any `Playhead/Views/` source file. `SettingsView`
// sees three free functions and never learns that an `AnalysisStore`
// exists.
//
// ----- Why the actions live behind an explicit gesture -----
//
// `quarantineAndRebuild` is the only thing in the app that moves the
// listener's analysis library, and playhead-wvdz's whole point is that
// the app must not reach it on its own. Keeping it behind a hatch that
// only a button calls is a structural statement of that: there is no
// timer, no launch path, and no background task that can get here.

import Foundation

// MARK: - Read

/// The durable record of whether the analysis database opened.
@MainActor
func loadAnalysisStoreHealth(runtime: PlayheadRuntime) async -> AnalysisStoreHealthState {
    await runtime.analysisStoreRecovery.currentState()
}

// MARK: - Listener choices

/// "Try again." Clears the escalation counter and re-attempts the open
/// immediately, so the listener gets an answer now instead of on a
/// relaunch. Destroys nothing on any branch.
@MainActor
func retryAnalysisStoreOpen(runtime: PlayheadRuntime) async -> AnalysisStoreLaunchOutcome {
    await runtime.analysisStoreRecovery.retryAtListenerRequest(runtime.analysisStore)
}

/// "Start fresh." Moves the existing store aside — it stays on the
/// device, readable, under a recorded name — and rebuilds an empty one.
///
/// Throws `AnalysisStoreRecoveryError.quarantineFailed` when the move
/// could not happen, in which case nothing changed. The caller must
/// surface that rather than swallowing it: silently doing nothing after
/// the listener chose something is how the original bug felt from the
/// outside.
@MainActor
func startFreshAnalysisStore(runtime: PlayheadRuntime) async throws -> AnalysisStoreLaunchOutcome {
    try await runtime.analysisStoreRecovery.quarantineAndRebuild(runtime.analysisStore)
}
