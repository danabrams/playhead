// EpisodeDiagnosticsOptInTests.swift
// Verifies the non-destructive SwiftData migration that adds
// `Episode.diagnosticsOptIn: Bool` defaulting to `false`.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Migration shape under test:
//   * The flag is `false` by default for every fresh `Episode`.
//   * The flag is mutable (toggling preserves until explicitly reset).
//   * The reset policy lives in `DiagnosticsOptInResetPolicy` (separate
//     suite); this file only covers the SwiftData-level invariant.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("Episode.diagnosticsOptIn — additive SwiftData migration (playhead-ghon)")
@MainActor
struct EpisodeDiagnosticsOptInTests {

    // MARK: - Default false

    @Test("a freshly-constructed Episode has diagnosticsOptIn == false")
    func defaultsFalse() {
        let ep = Episode(
            feedItemGUID: "guid-1",
            feedURL: URL(string: "https://example.com/rss")!,
            title: "T",
            audioURL: URL(string: "https://example.com/a.mp3")!
        )
        #expect(ep.diagnosticsOptIn == false)
    }

    // MARK: - Mutability

    @Test("setting diagnosticsOptIn = true persists across in-memory store reads")
    func mutableAndPersisted() throws {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true)
        let container = try ModelContainer(for: schema, configurations: [config])
        let ctx = ModelContext(container)

        let ep = Episode(
            feedItemGUID: "guid-1",
            feedURL: URL(string: "https://example.com/rss")!,
            title: "T",
            audioURL: URL(string: "https://example.com/a.mp3")!
        )
        ctx.insert(ep)
        ep.diagnosticsOptIn = true
        try ctx.save()

        let descriptor = FetchDescriptor<Episode>()
        let rows = try ctx.fetch(descriptor)
        #expect(rows.count == 1)
        #expect(rows.first?.diagnosticsOptIn == true)
    }
}
