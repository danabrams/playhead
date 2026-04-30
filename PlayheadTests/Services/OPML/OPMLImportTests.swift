// OPMLImportTests.swift
// playhead-2jo: integration tests for the OPML import pipeline.
//
// The pipeline is:
//   1. parseOPML(from:) → [OPMLFeed]
//   2. importFeeds(_:exists:resolve:persist:progress:) → ImportResult
//
// `exists`, `resolve`, `persist` are seams so tests do not need SwiftData
// or the network. The production wiring binds these to the real
// PodcastDiscoveryService + ModelContext via SettingsView; that wiring
// is exercised by the SettingsView snapshot test.

import Foundation
import Testing
@testable import Playhead

@Suite("OPMLService – Import")
struct OPMLImportTests {

    private func feed(_ title: String, _ url: String) -> OPMLFeed {
        OPMLFeed(title: title, xmlUrl: URL(string: url)!)
    }

    @Test("Imports all feeds when none are duplicates and resolution succeeds")
    func importsAllNew() async {
        let feeds = [
            feed("A", "https://example.com/a.rss"),
            feed("B", "https://example.com/b.rss"),
            feed("C", "https://example.com/c.rss"),
        ]
        let result = await OPMLService().importFeeds(
            feeds,
            exists: { _ in false },
            resolve: { _ in .success(()) },
            persist: { _ in },
            progress: { _, _ in }
        )
        #expect(result.imported == 3)
        #expect(result.skippedDuplicate == 0)
        #expect(result.failed.isEmpty)
        #expect(result.attempted == 3)
    }

    @Test("Existing podcasts increment skippedDuplicate, not imported")
    func skipsDuplicates() async {
        let feeds = [
            feed("A", "https://example.com/a.rss"),
            feed("B", "https://example.com/b.rss"),
        ]
        let result = await OPMLService().importFeeds(
            feeds,
            exists: { url in url.absoluteString.hasSuffix("a.rss") },
            resolve: { _ in .success(()) },
            persist: { _ in },
            progress: { _, _ in }
        )
        #expect(result.imported == 1)
        #expect(result.skippedDuplicate == 1)
        #expect(result.failed.isEmpty)
    }

    @Test("Resolver failures land in `failed` with reason text")
    func resolverFailureRecorded() async {
        let feeds = [
            feed("OK", "https://example.com/ok.rss"),
            feed("Bad", "https://example.com/bad.rss"),
        ]
        let result = await OPMLService().importFeeds(
            feeds,
            exists: { _ in false },
            resolve: { url in
                if url.absoluteString.contains("bad") {
                    return .failure("HTTP 404")
                }
                return .success(())
            },
            persist: { _ in },
            progress: { _, _ in }
        )
        #expect(result.imported == 1)
        #expect(result.failed.count == 1)
        #expect(result.failed[0].url.absoluteString == "https://example.com/bad.rss")
        #expect(result.failed[0].reason == "HTTP 404")
    }

    @Test("Progress callback fires once per feed, total stays constant, peaks at count")
    func progressCallbackFires() async {
        let feeds = (0..<10).map { feed("F\($0)", "https://example.com/f\($0).rss") }
        // Synchronous lock-protected recorder. We don't dispatch to a
        // Task because that would let progress events arrive
        // out-of-order relative to their logical position in the
        // import, and we want to assert on the actual numbers the
        // service reports.
        final class Recorder: @unchecked Sendable {
            private let lock = NSLock()
            private var _events: [(Int, Int)] = []
            func record(_ done: Int, _ total: Int) {
                lock.lock(); defer { lock.unlock() }
                _events.append((done, total))
            }
            var events: [(Int, Int)] {
                lock.lock(); defer { lock.unlock() }
                return _events
            }
        }
        let recorder = Recorder()
        _ = await OPMLService().importFeeds(
            feeds,
            exists: { _ in false },
            resolve: { _ in .success(()) },
            persist: { _ in },
            progress: { done, total in recorder.record(done, total) }
        )
        let events = recorder.events
        #expect(events.count == feeds.count)
        #expect(events.allSatisfy { $0.1 == feeds.count })
        // Reported `done` values cover [1, feeds.count] exactly once each.
        let dones = events.map { $0.0 }.sorted()
        #expect(dones == Array(1...feeds.count))
    }

    @Test("Persist is called only for resolved-non-duplicate feeds")
    func persistCalledForResolvedOnly() async {
        let feeds = [
            feed("Existing", "https://example.com/existing.rss"),
            feed("BadFetch", "https://example.com/bad.rss"),
            feed("Fresh", "https://example.com/fresh.rss"),
        ]
        actor Persisted {
            var urls: [URL] = []
            func add(_ url: URL) { urls.append(url) }
        }
        let persisted = Persisted()
        _ = await OPMLService().importFeeds(
            feeds,
            exists: { url in url.absoluteString.contains("existing") },
            resolve: { url in
                url.absoluteString.contains("bad")
                    ? .failure("offline")
                    : .success(())
            },
            persist: { feed in await persisted.add(feed.xmlUrl) },
            progress: { _, _ in }
        )
        let urls = await persisted.urls
        #expect(urls.count == 1)
        #expect(urls.first?.absoluteString == "https://example.com/fresh.rss")
    }

    @Test("Concurrency is bounded — no more than `maxConcurrent` resolves in flight at once")
    func concurrencyBounded() async {
        let feeds = (0..<20).map { feed("F\($0)", "https://example.com/\($0).rss") }
        actor InFlightTracker {
            var active = 0
            var peak = 0
            func enter() { active += 1; peak = max(peak, active) }
            func leave() { active -= 1 }
        }
        let tracker = InFlightTracker()
        _ = await OPMLService().importFeeds(
            feeds,
            exists: { _ in false },
            resolve: { _ in
                await tracker.enter()
                // Tiny sleep to guarantee overlap between concurrent
                // resolves — without it the resolves return so fast
                // that the in-flight counter never exceeds 1.
                try? await Task.sleep(for: .milliseconds(20))
                await tracker.leave()
                return .success(())
            },
            persist: { _ in },
            progress: { _, _ in },
            maxConcurrent: 5
        )
        let peak = await tracker.peak
        #expect(peak <= 5)
        // Sanity: bound was actually exercised, not vacuously satisfied.
        #expect(peak >= 2)
    }

    @Test("Feeds with the same URL across the input list are imported once")
    func internalDeduplication() async {
        let feeds = [
            feed("A", "https://example.com/dup.rss"),
            feed("A again", "https://example.com/dup.rss"),
            feed("B", "https://example.com/b.rss"),
        ]
        actor Counter {
            var resolves = 0
            func bump() { resolves += 1 }
        }
        let counter = Counter()
        let result = await OPMLService().importFeeds(
            feeds,
            exists: { _ in false },
            resolve: { _ in
                await counter.bump()
                return .success(())
            },
            persist: { _ in },
            progress: { _, _ in }
        )
        let resolves = await counter.resolves
        #expect(resolves == 2)
        #expect(result.imported == 2)
        #expect(result.attempted == 2)
    }
}
