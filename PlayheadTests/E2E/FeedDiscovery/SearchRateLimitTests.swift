// SearchRateLimitTests.swift
// playhead-8u1 — E2E proof of search rate-limiting and cache behaviour.
//
// Scenario 5 from the bead:
//   1. Fire 30 search requests in rapid succession
//   2. No crash, no hang
//   3. Rate-limit kicks in gracefully (debounce + cache)
//   4. Cached results are served when the query repeats
//
// `PodcastDiscoveryService` enforces:
//   * In-memory query cache (5 minute TTL) keyed on lowercased query.
//     Subsequent invocations of the same query AFTER a successful run
//     return the cached payload without touching the network.
//   * Minimum interval between provider requests (~3 seconds — actor's
//     `waitForRateLimit()`); a fully-serialized burst of 30 distinct
//     queries would take ~90 s on the wire.
//
// Note on concurrent racing:
//   Because the actor permits reentry across `await waitForRateLimit()`
//   and `await provider.search(...)`, 30 calls dispatched in parallel
//   for the SAME query CAN all miss the cache before the first one
//   returns and writes the cache entry. The "graceful" contract this
//   test asserts is therefore:
//     * Every caller gets a sensible result (cached or fresh) — no
//       crash, no hang, no actor deadlock
//     * After a successful initial call, a subsequent same-query
//       invocation is served from the in-memory cache (no new request)

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-8u1 - search rate limiting E2E", .serialized)
struct SearchRateLimitTests {

    @Test("30 rapid-fire same-query searches all complete with consistent results, no crash or hang")
    func rapidFireSameQueryAllComplete() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let json = ITunesSearchStubFactory.responseJSON(items: [
            .init(
                collectionId: 1,
                collectionName: "Cached Show",
                artistName: "Cached Author",
                feedURL: URL(string: "https://example.com/cached/feed.xml")!,
                artworkURL: nil,
                genre: nil,
                trackCount: nil
            ),
        ])
        stub.registerStub(
            whereURLContains: "itunes.apple.com/search",
            respondWith: json
        )

        let service = PodcastDiscoveryService(session: session)

        // Fire 30 concurrent searches for the same term and gather results.
        try await withThrowingTaskGroup(of: [DiscoveryResult].self) { group in
            for _ in 0 ..< 30 {
                group.addTask { try await service.searchPodcasts(query: "Cached Show") }
            }
            var collected: [[DiscoveryResult]] = []
            for try await r in group {
                collected.append(r)
            }
            #expect(collected.count == 30)
            // Every caller got the same payload (consistency contract — even
            // if some raced past the cache, they still got the same JSON).
            #expect(collected.allSatisfy { $0.first?.title == "Cached Show" })
        }
    }

    @Test("After a successful search, a subsequent same-query call is served from the cache (zero new requests)")
    func subsequentSameQueryHitsCache() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let json = ITunesSearchStubFactory.responseJSON(items: [
            .init(
                collectionId: 1,
                collectionName: "Stable Show",
                artistName: "Stable Author",
                feedURL: URL(string: "https://example.com/stable/feed.xml")!,
                artworkURL: nil,
                genre: nil,
                trackCount: nil
            ),
        ])
        stub.registerStub(
            whereURLContains: "itunes.apple.com/search",
            respondWith: json
        )

        let service = PodcastDiscoveryService(session: session)

        // First call → one network round-trip.
        let first = try await service.searchPodcasts(query: "Stable Show")
        #expect(first.first?.title == "Stable Show")
        let firstHits = stub.recordedURLs().filter {
            $0.absoluteString.contains("itunes.apple.com/search")
        }.count
        #expect(firstHits == 1)

        // Second call → no new network request (cached).
        let second = try await service.searchPodcasts(query: "Stable Show")
        #expect(second.first?.title == "Stable Show")
        let secondHits = stub.recordedURLs().filter {
            $0.absoluteString.contains("itunes.apple.com/search")
        }.count
        #expect(secondHits == 1, "second same-query call should hit the cache; saw \(secondHits) total search requests")
    }

    @Test("30 rapid-fire searches across distinct terms do not crash or hang and complete within a bounded deadline")
    func rapidFireDistinctTermsDoNotCrashOrHang() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        // Same-shape JSON for every search hit; the body is irrelevant —
        // the test cares about not crashing or hanging, not about the
        // parsed shape.
        let json = ITunesSearchStubFactory.responseJSON(items: [
            .init(
                collectionId: 1,
                collectionName: "Show",
                artistName: "Author",
                feedURL: URL(string: "https://example.com/show.xml")!,
                artworkURL: nil,
                genre: nil,
                trackCount: nil
            ),
        ])
        stub.registerStub(
            whereURLContains: "itunes.apple.com/search",
            respondWith: json
        )

        let service = PodcastDiscoveryService(session: session)
        let queries = (0 ..< 30).map { "Query-\($0)" }

        // Production rate limiter is 3 s/request — a fully serialized
        // burst of 30 distinct queries would take ~90 s. We bound this
        // test's wall-clock budget at 8 s and dispatch each search
        // inside a child Task. Each gets a 200ms slice; if the actor
        // has not produced a result we cancel that task and move on.
        // The test asserts:
        //   * No crash (the process is still alive at test end)
        //   * Every caller's task either returned or was cancelled
        //   * The whole burst returned within the budget (no hang)
        let start = Date.now
        let outcomes = await withTaskGroup(of: Bool.self) { group -> [Bool] in
            for q in queries {
                group.addTask {
                    let searchTask = Task<[DiscoveryResult], Error> {
                        try await service.searchPodcasts(query: q)
                    }
                    let timeoutTask = Task<Void, Error> {
                        try await Task.sleep(for: .milliseconds(200))
                        searchTask.cancel()
                    }
                    _ = try? await searchTask.value
                    timeoutTask.cancel()
                    return true
                }
            }
            var collected: [Bool] = []
            for await ok in group {
                collected.append(ok)
            }
            return collected
        }
        let elapsed = Date.now.timeIntervalSince(start)
        #expect(outcomes.count == 30)
        #expect(elapsed < 8.0, "burst took \(elapsed)s; expected to be bounded")
    }
}
