// FeedDiscoveryTestSupport.swift
// playhead-8u1 — shared test harness for the Feed & Discovery E2E suite.
//
// Two collaborators:
//   1. `StubbingURLProtocol` — a `URLProtocol` subclass that maps requests
//      to pre-canned `(Data, HTTPURLResponse)` or `Error` responses, so
//      a real `URLSession` can drive `PodcastDiscoveryService` without
//      hitting the network. Pattern mirrors the one already used by
//      `RecordingURLProtocol` (NetworkIsolationTests) and
//      `ChapterPipelineNetworkTrapProtocol` (ChapterEvidencePipelineRegressionTests).
//   2. `makeStubbedSession()` — builds a `URLSession` with the stub class
//      registered on its configuration so the stub only intercepts
//      requests for THIS session (not the global `URLSession.shared`).
//   3. `makeFeedDiscoveryContainer()` — fresh in-memory `ModelContainer`
//      restricted to the Podcast/Episode schema for SwiftData assertions.
//
// All scenarios in this directory drive the REAL
// `PodcastDiscoveryService` actor, mocking only the network boundary.

import Foundation
import SwiftData
import Testing
@testable import Playhead

// MARK: - Stubbing URLProtocol

/// Test-only `URLProtocol` that maps incoming requests to scripted
/// responses. Each request is matched against the registered handlers
/// in insertion order; the first handler whose `matches` predicate
/// returns true wins. If no handler matches, the request fails with a
/// 599 synthetic error so the test sees the unexpected URL clearly.
///
/// Per-session isolation:
///   * Every `URLSession` built by `makeStubbedSession()` is given a
///     unique ID injected into `httpAdditionalHeaders["X-FD-Stub-Session"]`.
///   * Handlers and request logs are bucketed by that ID, so concurrent
///     test runs (Swift Testing parallelism) don't clobber each other's
///     state even when the same `URLProtocol` subclass is shared
///     process-wide.
///   * Tests register handlers via the `FeedDiscoveryStub` instance
///     returned alongside the session.
final class StubbingURLProtocol: URLProtocol, @unchecked Sendable {

    /// A scripted response: either a successful `(Data, HTTPURLResponse)`
    /// pair or a synthetic `Error`.
    enum Response: Sendable {
        case success(Data, statusCode: Int)
        case failure(Error)
    }

    /// A registered handler — match predicate plus the response to return.
    struct Handler: @unchecked Sendable {
        let matches: @Sendable (URLRequest) -> Bool
        let response: @Sendable (URLRequest) -> Response
    }

    /// Per-session state.
    final class SessionBucket: @unchecked Sendable {
        var handlers: [Handler] = []
        var requestLog: [URL] = []
    }

    nonisolated(unsafe) static let lock = NSLock()
    nonisolated(unsafe) static var buckets: [String: SessionBucket] = [:]

    static let sessionHeader = "X-FD-Stub-Session"

    /// Lookup or create the per-session bucket.
    static func bucket(for sessionID: String) -> SessionBucket {
        lock.lock(); defer { lock.unlock() }
        if let existing = buckets[sessionID] { return existing }
        let fresh = SessionBucket()
        buckets[sessionID] = fresh
        return fresh
    }

    /// Drop a session bucket once the test that owns it tears down.
    static func releaseBucket(_ sessionID: String) {
        lock.lock(); defer { lock.unlock() }
        buckets.removeValue(forKey: sessionID)
    }

    // MARK: - URLProtocol overrides

    override class func canInit(with request: URLRequest) -> Bool {
        // Only intercept requests that carry a stub session header.
        // Any other request (e.g. system-issued without our session
        // configuration) falls through to the default chain.
        request.value(forHTTPHeaderField: sessionHeader) != nil
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let sessionID = request.value(forHTTPHeaderField: Self.sessionHeader) else {
            client?.urlProtocol(self, didFailWithError: NSError(
                domain: "FeedDiscoveryStubbingURLProtocol",
                code: 599,
                userInfo: [NSLocalizedDescriptionKey: "request missing stub session header"]
            ))
            return
        }

        let bucket = Self.bucket(for: sessionID)
        let url = request.url
        Self.lock.lock()
        if let url { bucket.requestLog.append(url) }
        let matched = bucket.handlers.first { $0.matches(request) }
        Self.lock.unlock()

        guard let handler = matched, let url else {
            client?.urlProtocol(self, didFailWithError: NSError(
                domain: "FeedDiscoveryStubbingURLProtocol",
                code: 599,
                userInfo: [NSLocalizedDescriptionKey: "no stub registered for \(url?.absoluteString ?? "<no url>")"]
            ))
            return
        }

        switch handler.response(request) {
        case .success(let data, let statusCode):
            let resp = HTTPURLResponse(
                url: url,
                statusCode: statusCode,
                httpVersion: "HTTP/1.1",
                headerFields: ["Content-Type": "application/xml"]
            )!
            client?.urlProtocol(self, didReceive: resp, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        case .failure(let error):
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

// MARK: - Per-Test Stub Handle

/// Handle returned alongside the stubbed `URLSession`. Provides
/// per-session handler registration and request inspection. Use one
/// per test to keep handlers isolated from concurrent suites.
struct FeedDiscoveryStub: Sendable {
    let sessionID: String

    /// Append a handler.
    func register(_ handler: StubbingURLProtocol.Handler) {
        let bucket = StubbingURLProtocol.bucket(for: sessionID)
        StubbingURLProtocol.lock.lock(); defer { StubbingURLProtocol.lock.unlock() }
        bucket.handlers.append(handler)
    }

    /// Convenience: return any data + status code for any URL whose
    /// host/path matches a substring.
    func registerStub(
        whereURLContains substring: String,
        respondWith data: Data,
        statusCode: Int = 200
    ) {
        register(StubbingURLProtocol.Handler(
            matches: { req in
                req.url.map { $0.absoluteString.contains(substring) } ?? false
            },
            response: { _ in .success(data, statusCode: statusCode) }
        ))
    }

    /// Convenience: fail any matching request with the given error.
    func registerError(
        whereURLContains substring: String,
        error: Error
    ) {
        register(StubbingURLProtocol.Handler(
            matches: { req in
                req.url.map { $0.absoluteString.contains(substring) } ?? false
            },
            response: { _ in .failure(error) }
        ))
    }

    /// Snapshot the URLs every intercepted request was issued against
    /// (just for THIS session).
    func recordedURLs() -> [URL] {
        let bucket = StubbingURLProtocol.bucket(for: sessionID)
        StubbingURLProtocol.lock.lock(); defer { StubbingURLProtocol.lock.unlock() }
        return bucket.requestLog
    }

    /// Drop the session bucket. Call from a `defer` at the start of
    /// each test to free the entry once the test exits.
    func release() {
        StubbingURLProtocol.releaseBucket(sessionID)
    }
}

// MARK: - URLSession Factory

/// Build a `URLSession` whose configuration registers
/// `StubbingURLProtocol` FIRST in its protocol chain and tags every
/// request with a unique session header. Returns the session paired
/// with a `FeedDiscoveryStub` handle scoped to that session.
func makeStubbedSession() -> (session: URLSession, stub: FeedDiscoveryStub) {
    let sessionID = UUID().uuidString
    let config = URLSessionConfiguration.ephemeral
    config.protocolClasses = [StubbingURLProtocol.self] + (config.protocolClasses ?? [])
    config.urlCache = nil
    config.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
    config.httpAdditionalHeaders = [StubbingURLProtocol.sessionHeader: sessionID]
    let session = URLSession(configuration: config)
    return (session, FeedDiscoveryStub(sessionID: sessionID))
}

// MARK: - SwiftData Container Factory

/// Build a fresh in-memory `ModelContainer` configured for the
/// Feed & Discovery scenarios. Includes only the models the discovery
/// pipeline writes through (`Podcast`, `Episode`) to keep schema setup
/// minimal and test boot fast.
///
/// MainActor-isolated to match the `ModelContainer` initializer's
/// requirement and the `@MainActor`-isolated production persist path.
@MainActor
func makeFeedDiscoveryContainer() throws -> ModelContainer {
    let schema = Schema([Podcast.self, Episode.self])
    let config = ModelConfiguration(
        "FeedDiscoveryE2ETests-\(UUID().uuidString)",
        schema: schema,
        isStoredInMemoryOnly: true
    )
    return try ModelContainer(for: schema, configurations: [config])
}

// MARK: - iTunes Search Response Builder

/// Build a JSON document mimicking the iTunes Search API response shape
/// the production `ITunesSearchProvider` decodes. Only the fields the
/// production decoder consumes are populated.
enum ITunesSearchStubFactory {

    struct Item: Sendable {
        let collectionId: Int
        let collectionName: String
        let artistName: String
        let feedURL: URL
        let artworkURL: URL?
        let genre: String?
        let trackCount: Int?
    }

    static func responseJSON(items: [Item]) -> Data {
        let entries: [[String: Any]] = items.map { item in
            var dict: [String: Any] = [
                "collectionId": item.collectionId,
                "collectionName": item.collectionName,
                "artistName": item.artistName,
                "feedUrl": item.feedURL.absoluteString,
            ]
            if let art = item.artworkURL {
                dict["artworkUrl600"] = art.absoluteString
                dict["artworkUrl100"] = art.absoluteString
            }
            if let g = item.genre { dict["primaryGenreName"] = g }
            if let t = item.trackCount { dict["trackCount"] = t }
            return dict
        }
        let body: [String: Any] = [
            "resultCount": entries.count,
            "results": entries,
        ]
        return try! JSONSerialization.data(withJSONObject: body, options: [])
    }
}

// MARK: - RSS Feed Builder

/// Build an RSS feed XML body containing the supplied episodes.
/// Channel-level metadata defaults are filled in for tests that don't
/// care about them; episode-level fields are caller-controlled so each
/// scenario can twist exactly the field it wants to assert on.
enum RSSFeedStubFactory {

    struct Episode: Sendable {
        let title: String
        let guid: String?
        let enclosureURL: URL?
        let pubDate: String?
        let durationSeconds: Int?

        init(
            title: String,
            guid: String?,
            enclosureURL: URL?,
            pubDate: String? = "Mon, 01 Jan 2024 12:00:00 GMT",
            durationSeconds: Int? = 1800
        ) {
            self.title = title
            self.guid = guid
            self.enclosureURL = enclosureURL
            self.pubDate = pubDate
            self.durationSeconds = durationSeconds
        }
    }

    static func feedXML(
        title: String = "Test Podcast",
        author: String = "Test Author",
        episodes: [Episode]
    ) -> Data {
        var xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"
             xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
             xmlns:content="http://purl.org/rss/1.0/modules/content/">
          <channel>
            <title>\(title)</title>
            <description>Test feed body</description>
            <itunes:author>\(author)</itunes:author>
            <itunes:image href="https://example.com/art.jpg"/>
            <itunes:category text="Test"/>

        """
        for ep in episodes {
            xml += "    <item>\n"
            xml += "      <title>\(ep.title)</title>\n"
            if let guid = ep.guid {
                xml += "      <guid>\(guid)</guid>\n"
            }
            if let url = ep.enclosureURL {
                xml += "      <enclosure url=\"\(url.absoluteString)\" type=\"audio/mpeg\" length=\"100\"/>\n"
            }
            if let pubDate = ep.pubDate {
                xml += "      <pubDate>\(pubDate)</pubDate>\n"
            }
            if let dur = ep.durationSeconds {
                xml += "      <itunes:duration>\(dur)</itunes:duration>\n"
            }
            xml += "    </item>\n"
        }
        xml += "  </channel>\n</rss>\n"
        return Data(xml.utf8)
    }
}
