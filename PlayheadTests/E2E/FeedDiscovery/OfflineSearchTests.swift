// OfflineSearchTests.swift
// playhead-8u1 — E2E proof of offline-mode search behaviour.
//
// Scenario 6 from the bead:
//   1. With network "off", search for a previously-cached term →
//      cached results are returned (no error, no crash)
//   2. Search for a NEW term → a regular Swift error is surfaced to
//      the caller, no crash
//
// We simulate "no network" by configuring the URLProtocol to fail
// every request with `URLError(.notConnectedToInternet)` — the same
// error iOS dispatches when the device has no connection. Discovery's
// in-memory query cache is what makes the first scenario succeed.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-8u1 - offline search E2E", .serialized)
struct OfflineSearchTests {

    @Test("Cached query returns prior results when network is off")
    func cachedQuerySucceedsOffline() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let json = ITunesSearchStubFactory.responseJSON(items: [
            .init(
                collectionId: 42,
                collectionName: "Daily Driver",
                artistName: "Driver Author",
                feedURL: URL(string: "https://example.com/daily-driver.xml")!,
                artworkURL: nil,
                genre: "News",
                trackCount: 5
            ),
        ])

        // Stage 1: online — register a successful response so the first
        // search populates the cache.
        let phase = AtomicCounter()
        stub.register(.init(
            matches: { req in
                req.url?.absoluteString.contains("itunes.apple.com/search") ?? false
            },
            response: { _ in
                let n = phase.incrementAndGet()
                if n == 1 {
                    return .success(json, statusCode: 200)
                } else {
                    // Subsequent calls pretend the device went offline.
                    return .failure(URLError(.notConnectedToInternet))
                }
            }
        ))

        let service = PodcastDiscoveryService(session: session)

        // First call: populates the cache.
        let online = try await service.searchPodcasts(query: "Daily Driver")
        #expect(online.first?.title == "Daily Driver")

        // Second call (offline now): cache hit — no network call needed.
        let offline = try await service.searchPodcasts(query: "Daily Driver")
        #expect(offline.first?.title == "Daily Driver")
        #expect(offline == online)

        // Sanity: only one outbound request actually fired (second was
        // served from the in-memory cache and never touched the stub).
        let recorded = stub.recordedURLs()
        let searchHits = recorded.filter {
            $0.absoluteString.contains("itunes.apple.com/search")
        }
        #expect(searchHits.count == 1)
    }

    @Test("New query while offline surfaces an error, no crash")
    func newQueryOfflineSurfacesError() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        // Every search request fails with notConnectedToInternet.
        stub.registerError(
            whereURLContains: "itunes.apple.com/search",
            error: URLError(.notConnectedToInternet)
        )

        let service = PodcastDiscoveryService(session: session)

        await #expect(throws: Error.self) {
            try await service.searchPodcasts(query: "Brand New Search")
        }
    }
}
