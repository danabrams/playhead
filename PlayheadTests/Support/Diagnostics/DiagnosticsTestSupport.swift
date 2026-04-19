// DiagnosticsTestSupport.swift
// Shared helpers for the diagnostics test suites.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Rationale: several diagnostics suites (`EpisodeDiagnosticsOptInTests`,
// `SwiftDataDiagnosticsOptInSinkTests`) need the same in-memory
// `ModelContext` setup. Centralizing the factory here keeps the
// schema + `isStoredInMemoryOnly` configuration in one place so future
// schema additions only need one edit across the test target.

import Foundation
import SwiftData

@testable import Playhead

/// Builds a fresh in-memory `ModelContext` suitable for diagnostics
/// tests. The schema mirrors the production model set that
/// `Episode.diagnosticsOptIn` depends on (`Podcast`, `Episode`,
/// `UserPreferences`). Each call returns a brand-new container so
/// suites do not share state.
@MainActor
func makeDiagnosticsInMemoryContext() throws -> ModelContext {
    let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
    let config = ModelConfiguration(isStoredInMemoryOnly: true)
    let container = try ModelContainer(for: schema, configurations: [config])
    return ModelContext(container)
}
