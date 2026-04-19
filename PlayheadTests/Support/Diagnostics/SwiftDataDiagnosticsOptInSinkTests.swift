// SwiftDataDiagnosticsOptInSinkTests.swift
// Exercises the SwiftData-backed opt-in sink in an in-memory store.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("SwiftDataDiagnosticsOptInSink (playhead-ghon)")
@MainActor
struct SwiftDataDiagnosticsOptInSinkTests {

    private func makeContext() throws -> ModelContext {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true)
        let container = try ModelContainer(for: schema, configurations: [config])
        return ModelContext(container)
    }

    private func makeEpisode(
        guid: String,
        feedURL: URL = URL(string: "https://example.com/rss")!,
        diagnosticsOptIn: Bool
    ) -> Episode {
        Episode(
            feedItemGUID: guid,
            feedURL: feedURL,
            title: "T-\(guid)",
            audioURL: URL(string: "https://example.com/\(guid).mp3")!,
            diagnosticsOptIn: diagnosticsOptIn
        )
    }

    @Test("reset clears diagnosticsOptIn for matching canonical keys, leaves others untouched")
    func resetsOnlyMatchingEpisodes() throws {
        let ctx = try makeContext()
        let feed = URL(string: "https://example.com/rss")!
        let a = makeEpisode(guid: "A", feedURL: feed, diagnosticsOptIn: true)
        let b = makeEpisode(guid: "B", feedURL: feed, diagnosticsOptIn: true)
        let c = makeEpisode(guid: "C", feedURL: feed, diagnosticsOptIn: true)
        ctx.insert(a)
        ctx.insert(b)
        ctx.insert(c)
        try ctx.save()

        let sink = SwiftDataDiagnosticsOptInSink(context: ctx)
        sink.applyResetToEpisodes(
            matchingEpisodeIds: [a.canonicalEpisodeKey, c.canonicalEpisodeKey],
            newValue: false
        )

        let rows = try ctx.fetch(FetchDescriptor<Episode>())
        let byKey = Dictionary(uniqueKeysWithValues: rows.map { ($0.canonicalEpisodeKey, $0) })
        #expect(byKey[a.canonicalEpisodeKey]?.diagnosticsOptIn == false)
        #expect(byKey[b.canonicalEpisodeKey]?.diagnosticsOptIn == true)
        #expect(byKey[c.canonicalEpisodeKey]?.diagnosticsOptIn == false)
    }

    @Test("empty input list is a no-op — store is untouched")
    func emptyInputIsNoop() throws {
        let ctx = try makeContext()
        let a = makeEpisode(guid: "A", diagnosticsOptIn: true)
        ctx.insert(a)
        try ctx.save()

        let sink = SwiftDataDiagnosticsOptInSink(context: ctx)
        sink.applyResetToEpisodes(matchingEpisodeIds: [], newValue: false)

        let row = try ctx.fetch(FetchDescriptor<Episode>()).first
        #expect(row?.diagnosticsOptIn == true)
    }

    @Test("unknown episode IDs are silently skipped (idempotent against concurrent delete)")
    func unknownIdsSkipped() throws {
        let ctx = try makeContext()
        let a = makeEpisode(guid: "A", diagnosticsOptIn: true)
        ctx.insert(a)
        try ctx.save()

        let sink = SwiftDataDiagnosticsOptInSink(context: ctx)
        sink.applyResetToEpisodes(
            matchingEpisodeIds: ["never-existed", a.canonicalEpisodeKey],
            newValue: false
        )

        let rows = try ctx.fetch(FetchDescriptor<Episode>())
        #expect(rows.count == 1)
        #expect(rows.first?.diagnosticsOptIn == false)
    }
}
