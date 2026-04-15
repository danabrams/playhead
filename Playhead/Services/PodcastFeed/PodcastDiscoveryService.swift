// PodcastDiscoveryService.swift
// Search and discovery for podcasts using iTunes Search API.
// Provider-abstracted so Podcast Index can be added later.

import Foundation
import OSLog
import SwiftData

// MARK: - Discovery Provider Protocol

/// Abstraction over search backends (iTunes, Podcast Index, etc.).
protocol DiscoveryProvider: Sendable {
    /// Search for podcasts matching a text query.
    func search(query: String, limit: Int) async throws -> [DiscoveryResult]

    /// Fetch the feed URL for a podcast by its provider-specific ID.
    func lookupFeedURL(providerID: String) async throws -> URL?
}

/// A search result from a discovery provider, decoupled from SwiftData.
struct DiscoveryResult: Sendable, Equatable, Identifiable {
    let id: String
    let title: String
    let author: String
    let feedURL: URL?
    let artworkURL: URL?
    let genre: String?
    let episodeCount: Int?
}

// MARK: - iTunes Search Provider

/// Searches the Apple iTunes Search API for podcasts.
struct ITunesSearchProvider: DiscoveryProvider {
    private static let searchEndpoint = URL(string: "https://itunes.apple.com/search")!
    private static let lookupEndpoint = URL(string: "https://itunes.apple.com/lookup")!

    private let session: URLSession

    init(session: URLSession = .shared) {
        self.session = session
    }

    func search(query: String, limit: Int) async throws -> [DiscoveryResult] {
        var components = URLComponents(url: Self.searchEndpoint, resolvingAgainstBaseURL: false)!
        components.queryItems = [
            URLQueryItem(name: "term", value: query),
            URLQueryItem(name: "media", value: "podcast"),
            URLQueryItem(name: "entity", value: "podcast"),
            URLQueryItem(name: "limit", value: String(min(limit, 50))),
        ]
        guard let url = components.url else {
            throw DiscoveryError.invalidQuery(query)
        }

        let (data, response) = try await session.data(from: url)
        try Self.validateHTTPResponse(response, context: "search")

        let decoded = try JSONDecoder().decode(ITunesSearchResponse.self, from: data)
        return decoded.results.compactMap { item in
            guard let feedURLString = item.feedUrl,
                  let feedURL = URL(string: feedURLString) else { return nil }
            return DiscoveryResult(
                id: String(item.collectionId),
                title: item.collectionName,
                author: item.artistName,
                feedURL: feedURL,
                artworkURL: Self.highResArtworkURL(item.artworkUrl600 ?? item.artworkUrl100),
                genre: item.primaryGenreName,
                episodeCount: item.trackCount
            )
        }
    }

    func lookupFeedURL(providerID: String) async throws -> URL? {
        var components = URLComponents(url: Self.lookupEndpoint, resolvingAgainstBaseURL: false)!
        components.queryItems = [
            URLQueryItem(name: "id", value: providerID),
            URLQueryItem(name: "entity", value: "podcast"),
        ]
        guard let url = components.url else { return nil }

        let (data, response) = try await session.data(from: url)
        try Self.validateHTTPResponse(response, context: "lookup")

        let decoded = try JSONDecoder().decode(ITunesSearchResponse.self, from: data)
        guard let feedURLString = decoded.results.first?.feedUrl else { return nil }
        return URL(string: feedURLString)
    }

    // MARK: - Helpers

    private static func validateHTTPResponse(_ response: URLResponse, context: String) throws {
        guard let http = response as? HTTPURLResponse else {
            throw DiscoveryError.networkError("Non-HTTP response during \(context)")
        }
        switch http.statusCode {
        case 200...299:
            return
        case 403, 429:
            throw DiscoveryError.rateLimited
        default:
            throw DiscoveryError.networkError("HTTP \(http.statusCode) during \(context)")
        }
    }

    private static func highResArtworkURL(_ urlString: String?) -> URL? {
        guard let urlString else { return nil }
        return URL(string: urlString)
    }
}

// MARK: - iTunes API Response

private struct ITunesSearchResponse: Decodable {
    let resultCount: Int
    let results: [ITunesItem]
}

private struct ITunesItem: Decodable {
    let collectionId: Int
    let collectionName: String
    let artistName: String
    let feedUrl: String?
    let artworkUrl100: String?
    let artworkUrl600: String?
    let primaryGenreName: String?
    let trackCount: Int?
}

// MARK: - PodcastDiscoveryService

/// High-level service for podcast search, feed fetching, and episode refresh.
///
/// Wraps a ``DiscoveryProvider`` (default: iTunes Search API) with:
/// - Debounce-friendly query caching (recent results kept in memory)
/// - Rate limit awareness (~20 req/min for iTunes)
/// - SwiftData integration for persisting results
actor PodcastDiscoveryService {
    private let logger = Logger(subsystem: "com.playhead", category: "Discovery")

    private let provider: DiscoveryProvider
    private let session: URLSession

    /// In-memory cache of recent search results, keyed by lowercased query.
    private var searchCache: [String: CachedSearchResult] = [:]

    /// Maximum age of cached search results before they are stale.
    private static let cacheMaxAge: TimeInterval = 300 // 5 minutes

    /// Maximum number of cached queries to keep.
    private static let maxCachedQueries = 20

    /// Rate limiter: minimum interval between network requests.
    private static let minRequestInterval: TimeInterval = 3.0 // ~20/min

    /// Timestamp of the last network request to the provider.
    private var lastRequestTime: Date = .distantPast

    // MARK: - Lifecycle

    init(
        provider: DiscoveryProvider? = nil,
        session: URLSession = .shared
    ) {
        self.provider = provider ?? ITunesSearchProvider(session: session)
        self.session = session
    }

    // MARK: - Search

    /// Search for podcasts matching a text query.
    ///
    /// Returns cached results if the same query was made recently.
    /// Automatically rate-limits requests to the backing provider.
    func searchPodcasts(query: String, limit: Int = 25) async throws -> [DiscoveryResult] {
        let normalizedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !normalizedQuery.isEmpty else { return [] }

        // Check cache first.
        if let cached = searchCache[normalizedQuery],
           Date.now.timeIntervalSince(cached.timestamp) < Self.cacheMaxAge {
            logger.debug("Cache hit for query: \(normalizedQuery)")
            return cached.results
        }

        // Rate limit.
        await waitForRateLimit()

        let results = try await provider.search(query: normalizedQuery, limit: limit)
        lastRequestTime = .now

        // Update cache, evicting oldest if over limit.
        searchCache[normalizedQuery] = CachedSearchResult(
            results: results, timestamp: .now
        )
        evictStaleCacheEntries()

        logger.info("Search for '\(normalizedQuery)' returned \(results.count) results")
        return results
    }

    // MARK: - Feed Fetching

    /// Fetch and parse a podcast feed, returning a ``ParsedFeed``.
    ///
    /// Does not persist to SwiftData — callers use ``persist(_:from:in:)``
    /// to convert and save.
    func fetchFeed(url: URL) async throws -> ParsedFeed {
        await waitForRateLimit()

        let (data, response) = try await session.data(from: url)
        lastRequestTime = .now

        guard let http = response as? HTTPURLResponse,
              (200...299).contains(http.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            throw DiscoveryError.networkError("Feed fetch failed: HTTP \(code)")
        }

        let parser = FeedParser()
        let feed = try parser.parse(data: data, baseURL: url)
        logger.info("Fetched feed '\(feed.title)' with \(feed.episodes.count) episodes")
        return feed
    }

    /// Persist a parsed feed into SwiftData as a ``Podcast`` with ``Episode`` children.
    ///
    /// Upserts: if a Podcast with the same feedURL exists, updates it and
    /// merges episodes by GUID.
    @MainActor
    func persist(
        _ feed: ParsedFeed,
        from feedURL: URL,
        in context: ModelContext
    ) -> Podcast {
        // SwiftData predicates cannot descend through stored URL properties
        // (for example `feedURL.absoluteString`), so match on the URL value
        // after fetching the current library snapshot.
        let existing = (try? context.fetch(FetchDescriptor<Podcast>()))?.first {
            $0.feedURL == feedURL
        }

        let podcast: Podcast
        if let existing {
            existing.title = feed.title
            existing.author = feed.author
            existing.artworkURL = feed.artworkURL
            podcast = existing
        } else {
            podcast = Podcast(
                feedURL: feedURL,
                title: feed.title,
                author: feed.author,
                artworkURL: feed.artworkURL
            )
            context.insert(podcast)
        }

        // Merge episodes: insert new, update existing by GUID.
        let existingGUIDs = Set(podcast.episodes.map(\.feedItemGUID))

        for parsedEp in feed.episodes {
            guard !parsedEp.guid.isEmpty else { continue }

            let metadata = FeedTextNormalizer.makeMetadata(
                rawDescription: parsedEp.description,
                rawSummary: parsedEp.showNotes
            )

            if existingGUIDs.contains(parsedEp.guid) {
                // Update existing episode metadata (title, duration, etc.)
                if let ep = podcast.episodes.first(where: { $0.feedItemGUID == parsedEp.guid }) {
                    ep.title = parsedEp.title
                    ep.duration = parsedEp.duration
                    ep.publishedAt = parsedEp.pubDate
                    if let url = parsedEp.enclosureURL {
                        ep.audioURL = url
                    }
                    // Shadow: update feed metadata if source changed
                    if let metadata, ep.feedMetadata?.sourceHashes != metadata.sourceHashes {
                        ep.feedMetadata = metadata
                    }
                }
            } else if let audioURL = parsedEp.enclosureURL {
                let episode = Episode(
                    feedItemGUID: parsedEp.guid,
                    feedURL: feedURL,
                    podcast: podcast,
                    title: parsedEp.title,
                    audioURL: audioURL,
                    duration: parsedEp.duration,
                    publishedAt: parsedEp.pubDate,
                    feedMetadata: metadata
                )
                context.insert(episode)
            }
        }

        return podcast
    }

    // MARK: - Episode Refresh

    /// Re-fetches the feed for a podcast and merges new episodes.
    ///
    /// Returns the list of newly-added episodes (episodes that were not
    /// previously in SwiftData).
    @MainActor
    func refreshEpisodes(
        for podcast: Podcast,
        in context: ModelContext
    ) async throws -> [Episode] {
        let existingGUIDs = Set(podcast.episodes.map(\.feedItemGUID))

        let feed = try await fetchFeed(url: podcast.feedURL)
        let _ = persist(feed, from: podcast.feedURL, in: context)

        // Return only the episodes that are genuinely new.
        let newEpisodes = podcast.episodes.filter { !existingGUIDs.contains($0.feedItemGUID) }
        logger.info("Refreshed '\(podcast.title)': \(newEpisodes.count) new episodes")
        return newEpisodes
    }

    // MARK: - Rate Limiting

    /// Waits if necessary to respect rate limits.
    private func waitForRateLimit() async {
        let elapsed = Date.now.timeIntervalSince(lastRequestTime)
        let remaining = Self.minRequestInterval - elapsed
        if remaining > 0 {
            logger.debug("Rate limit: waiting \(String(format: "%.1f", remaining))s")
            try? await Task.sleep(for: .milliseconds(Int(remaining * 1000)))
        }
    }

    // MARK: - Cache Management

    private func evictStaleCacheEntries() {
        // Remove stale entries.
        let now = Date.now
        searchCache = searchCache.filter {
            now.timeIntervalSince($0.value.timestamp) < Self.cacheMaxAge
        }
        // If still over limit, remove oldest.
        if searchCache.count > Self.maxCachedQueries {
            let sorted = searchCache.sorted { $0.value.timestamp < $1.value.timestamp }
            let toRemove = sorted.prefix(searchCache.count - Self.maxCachedQueries)
            for (key, _) in toRemove {
                searchCache.removeValue(forKey: key)
            }
        }
    }
}

// MARK: - Cache Types

private struct CachedSearchResult {
    let results: [DiscoveryResult]
    let timestamp: Date
}

// MARK: - Errors

enum DiscoveryError: Error, LocalizedError {
    case invalidQuery(String)
    case networkError(String)
    case rateLimited
    case feedNotFound(String)

    var errorDescription: String? {
        switch self {
        case .invalidQuery(let q): "Invalid search query: '\(q)'"
        case .networkError(let msg): msg
        case .rateLimited: "Rate limited by search provider. Try again shortly."
        case .feedNotFound(let id): "No feed found for provider ID: \(id)"
        }
    }
}
