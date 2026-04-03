// PodcastFeedParser.swift
// RSS/Atom feed parsing for podcast discovery and episode metadata.

import Foundation

// MARK: - Parsed Feed Types (intermediate, decoupled from SwiftData)

/// Intermediate representation of a parsed podcast feed.
struct ParsedFeed: Sendable, Equatable {
    var title: String
    var author: String
    var description: String
    var artworkURL: URL?
    var language: String?
    var categories: [String]
    var episodes: [ParsedEpisode]
}

/// Intermediate representation of a parsed episode.
struct ParsedEpisode: Sendable, Equatable {
    var title: String
    var guid: String
    var enclosureURL: URL?
    var enclosureType: String?
    var enclosureLength: Int64?
    var pubDate: Date?
    var duration: TimeInterval?
    var description: String?
    var showNotes: String?
    var chapters: [ParsedChapter]
    var itunesAuthor: String?
    var itunesImageURL: URL?
    var itunesEpisodeNumber: Int?

    /// Normalized enclosure identity for asset fingerprinting.
    var enclosureIdentity: String? {
        guard let url = enclosureURL else { return nil }
        let type = enclosureType ?? "unknown"
        let length = enclosureLength.map(String.init) ?? "0"
        return "\(url.absoluteString)|\(type)|\(length)"
    }
}

/// A chapter marker from Podcasting 2.0 `<podcast:chapters>` or inline
/// `<podcast:chapter>` elements.
struct ParsedChapter: Sendable, Equatable {
    var startTime: TimeInterval
    var title: String?
    var url: URL?
    var imageURL: URL?
}

// MARK: - Feed Parser Errors

enum FeedParserError: Error, LocalizedError, Equatable {
    case emptyData
    case xmlParsingFailed(String)
    case noFeedFound

    var errorDescription: String? {
        switch self {
        case .emptyData: "Feed data is empty"
        case .xmlParsingFailed(let reason): "XML parsing failed: \(reason)"
        case .noFeedFound: "No RSS or Atom feed found in data"
        }
    }
}

// MARK: - FeedParser

/// Parses RSS 2.0 and Atom podcast feeds using Foundation XMLParser.
/// Handles iTunes namespace extensions and Podcasting 2.0 chapter tags.
/// - Important: Instances must not be shared across threads. Callers should
///   create a fresh `FeedParser` for each parse operation.
final class FeedParser: NSObject, XMLParserDelegate {

    // MARK: - Namespace URIs

    private static let itunesNS = "http://www.itunes.com/dtds/podcast-1.0.dtd"
    private static let podcastNS = "https://podcastindex.org/namespace/1.0"
    private static let contentNS = "http://purl.org/rss/1.0/modules/content/"
    private static let atomNS = "http://www.w3.org/2005/Atom"

    // MARK: - Parse State

    private var feed = ParsedFeed(
        title: "", author: "", description: "",
        artworkURL: nil, language: nil,
        categories: [], episodes: []
    )

    private var currentEpisode: ParsedEpisode?
    private var currentChapter: ParsedChapter?
    private var currentText = ""
    private var insideChannel = false
    private var insideItem = false
    private var insideAtomEntry = false
    private var isAtomFeed = false
    private var seenGUIDs: Set<String> = []
    private var parseError: Error?
    private var feedBaseURL: URL?

    // MARK: - Public API

    /// Parse feed data, returning a `ParsedFeed` on success.
    func parse(data: Data, baseURL: URL? = nil) throws -> ParsedFeed {
        guard !data.isEmpty else { throw FeedParserError.emptyData }

        feedBaseURL = baseURL
        let parser = XMLParser(data: data)
        parser.delegate = self
        parser.shouldProcessNamespaces = true
        parser.shouldReportNamespacePrefixes = false

        guard parser.parse() else {
            if let error = parseError {
                throw error
            }
            let msg = parser.parserError?.localizedDescription ?? "unknown"
            throw FeedParserError.xmlParsingFailed(msg)
        }

        if feed.title.isEmpty && feed.episodes.isEmpty {
            throw FeedParserError.noFeedFound
        }

        return feed
    }

    // MARK: - XMLParserDelegate

    func parser(
        _ parser: XMLParser,
        didStartElement elementName: String,
        namespaceURI: String?,
        qualifiedName: String?,
        attributes: [String: String]
    ) {
        currentText = ""
        let local = elementName
        let ns = namespaceURI ?? ""

        // RSS channel / Atom feed
        if local == "channel" {
            insideChannel = true
            return
        }
        if local == "feed" && ns == Self.atomNS {
            isAtomFeed = true
            insideChannel = true
            return
        }

        // RSS item / Atom entry
        if local == "item" {
            insideItem = true
            currentEpisode = makeEmptyEpisode()
            return
        }
        if local == "entry" && isAtomFeed {
            insideAtomEntry = true
            insideItem = true
            currentEpisode = makeEmptyEpisode()
            return
        }

        // Enclosure (RSS)
        if local == "enclosure" && insideItem {
            currentEpisode?.enclosureURL = resolveURL(attributes["url"])
            currentEpisode?.enclosureType = attributes["type"]
            if let len = attributes["length"], let n = Int64(len) {
                currentEpisode?.enclosureLength = n
            }
            return
        }

        // Atom link with enclosure rel
        if local == "link" && isAtomFeed && insideItem {
            if attributes["rel"] == "enclosure",
               let href = attributes["href"] {
                currentEpisode?.enclosureURL = resolveURL(href)
                currentEpisode?.enclosureType = attributes["type"]
                if let len = attributes["length"], let n = Int64(len) {
                    currentEpisode?.enclosureLength = n
                }
            }
            return
        }

        // Atom link for feed-level artwork
        if local == "link" && isAtomFeed && !insideItem {
            if attributes["rel"] == "icon" || attributes["rel"] == "logo",
               let href = attributes["href"] {
                feed.artworkURL = resolveURL(href)
            }
            return
        }

        // iTunes image
        if local == "image" && ns == Self.itunesNS {
            if let href = attributes["href"] {
                let url = resolveURL(href)
                if insideItem {
                    currentEpisode?.itunesImageURL = url
                } else if insideChannel {
                    feed.artworkURL = url
                }
            }
            return
        }

        // iTunes category
        if local == "category" && ns == Self.itunesNS {
            if let text = attributes["text"], !text.isEmpty {
                feed.categories.append(text)
            }
            return
        }

        // Podcasting 2.0 chapters link (external JSON)
        if local == "chapters" && ns == Self.podcastNS && insideItem {
            // We note the chapters URL but do not fetch it here.
            return
        }

        // Podcasting 2.0 inline chapter
        if local == "chapter" && ns == Self.podcastNS && insideItem {
            var ch = ParsedChapter(startTime: 0, title: nil, url: nil, imageURL: nil)
            if let startStr = attributes["startTime"] {
                ch.startTime = parseDuration(startStr) ?? 0
            }
            ch.title = attributes["title"]
            if let href = attributes["href"] { ch.url = resolveURL(href) }
            if let img = attributes["img"] { ch.imageURL = resolveURL(img) }
            currentChapter = ch
            return
        }
    }

    func parser(
        _ parser: XMLParser,
        didEndElement elementName: String,
        namespaceURI: String?,
        qualifiedName: String?
    ) {
        let local = elementName
        let ns = namespaceURI ?? ""
        let text = currentText.trimmingCharacters(in: .whitespacesAndNewlines)

        // End of item / entry
        if (local == "item") || (local == "entry" && isAtomFeed) {
            finalizeEpisode()
            insideItem = false
            insideAtomEntry = false
            return
        }

        if local == "channel" { insideChannel = false; return }

        // Inline chapter end
        if local == "chapter" && ns == Self.podcastNS && insideItem {
            if let ch = currentChapter {
                currentEpisode?.chapters.append(ch)
            }
            currentChapter = nil
            return
        }

        // Route text to the right field
        if insideItem {
            handleItemElement(local: local, ns: ns, text: text)
        } else if insideChannel {
            handleChannelElement(local: local, ns: ns, text: text)
        }
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, foundCDATA CDATABlock: Data) {
        if let str = String(data: CDATABlock, encoding: .utf8) {
            currentText += str
        }
    }

    func parser(_ parser: XMLParser, parseErrorOccurred error: Error) {
        parseError = error
    }

    // MARK: - Element Routing

    private func handleChannelElement(local: String, ns: String, text: String) {
        guard !text.isEmpty else { return }
        switch (local, ns) {
        case ("title", ""):
            if feed.title.isEmpty { feed.title = text }
        case ("title", Self.atomNS):
            if feed.title.isEmpty { feed.title = text }
        case ("description", ""):
            feed.description = text
        case ("subtitle", Self.atomNS):
            if feed.description.isEmpty { feed.description = text }
        case ("summary", Self.itunesNS):
            if feed.description.isEmpty { feed.description = text }
        case ("author", Self.itunesNS):
            feed.author = text
        case ("author", ""):
            if feed.author.isEmpty { feed.author = text }
        case ("name", Self.atomNS):
            if feed.author.isEmpty { feed.author = text }
        case ("language", ""):
            feed.language = text
        default:
            break
        }
    }

    private func handleItemElement(local: String, ns: String, text: String) {
        guard !text.isEmpty || local == "guid" || local == "id" else { return }
        switch (local, ns) {
        case ("title", ""), ("title", Self.atomNS):
            currentEpisode?.title = text
        case ("guid", ""), ("id", Self.atomNS):
            currentEpisode?.guid = text
        case ("pubDate", ""), ("published", Self.atomNS), ("updated", Self.atomNS):
            if currentEpisode?.pubDate == nil {
                currentEpisode?.pubDate = parseDate(text)
            }
        case ("description", ""):
            currentEpisode?.description = text
        case ("summary", Self.itunesNS), ("summary", Self.atomNS):
            if currentEpisode?.description?.isEmpty ?? true {
                currentEpisode?.description = text
            }
        case ("encoded", Self.contentNS):
            currentEpisode?.showNotes = text
        case ("content", Self.atomNS):
            if currentEpisode?.showNotes == nil {
                currentEpisode?.showNotes = text
            }
        case ("duration", Self.itunesNS):
            currentEpisode?.duration = parseDuration(text)
        case ("author", Self.itunesNS):
            currentEpisode?.itunesAuthor = text
        case ("episode", Self.itunesNS):
            currentEpisode?.itunesEpisodeNumber = Int(text)
        default:
            break
        }
    }

    // MARK: - Episode Finalization

    private func finalizeEpisode() {
        guard var ep = currentEpisode else { return }

        // Synthesize GUID if missing
        if ep.guid.isEmpty {
            if let url = ep.enclosureURL {
                ep.guid = url.absoluteString
            } else {
                ep.guid = "\(feed.title)::\(ep.title)"
            }
        }

        // Deduplicate by GUID
        guard !seenGUIDs.contains(ep.guid) else {
            currentEpisode = nil
            return
        }
        seenGUIDs.insert(ep.guid)

        // showNotes fallback
        if ep.showNotes == nil { ep.showNotes = ep.description }

        feed.episodes.append(ep)
        currentEpisode = nil
    }

    // MARK: - Helpers

    private func makeEmptyEpisode() -> ParsedEpisode {
        ParsedEpisode(
            title: "", guid: "",
            enclosureURL: nil, enclosureType: nil, enclosureLength: nil,
            pubDate: nil, duration: nil,
            description: nil, showNotes: nil,
            chapters: [],
            itunesAuthor: nil, itunesImageURL: nil,
            itunesEpisodeNumber: nil
        )
    }

    /// Resolve potentially relative URLs against the feed base URL.
    private func resolveURL(_ string: String?) -> URL? {
        guard let string, !string.isEmpty else { return nil }
        if let abs = URL(string: string), abs.scheme != nil {
            return abs
        }
        if let base = feedBaseURL {
            return URL(string: string, relativeTo: base)?.absoluteURL
        }
        return URL(string: string)
    }

    // MARK: - Date Parsing

    /// Parses RFC 2822 and ISO 8601 dates commonly found in podcast feeds.
    private func parseDate(_ string: String) -> Date? {
        let trimmed = string.trimmingCharacters(in: .whitespacesAndNewlines)
        // Try ISO 8601 first (Atom feeds)
        let iso = ISO8601DateFormatter()
        iso.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let d = iso.date(from: trimmed) { return d }
        iso.formatOptions = [.withInternetDateTime]
        if let d = iso.date(from: trimmed) { return d }

        // RFC 2822 variants (RSS feeds)
        for fmt in Self.rfc2822Formats {
            let df = DateFormatter()
            df.locale = Locale(identifier: "en_US_POSIX")
            df.dateFormat = fmt
            if let d = df.date(from: trimmed) { return d }
        }
        return nil
    }

    private static let rfc2822Formats = [
        "EEE, dd MMM yyyy HH:mm:ss zzz",
        "EEE, dd MMM yyyy HH:mm:ss Z",
        "dd MMM yyyy HH:mm:ss zzz",
        "dd MMM yyyy HH:mm:ss Z",
        "EEE, d MMM yyyy HH:mm:ss zzz",
        "EEE, d MMM yyyy HH:mm:ss Z",
        "yyyy-MM-dd'T'HH:mm:ssZ",
        "yyyy-MM-dd",
    ]

    // MARK: - Duration Parsing

    /// Parses duration from "HH:MM:SS", "MM:SS", or raw seconds.
    private func parseDuration(_ string: String) -> TimeInterval? {
        let trimmed = string.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { return nil }

        let parts = trimmed.split(separator: ":")
        switch parts.count {
        case 3:
            guard let h = Double(parts[0]),
                  let m = Double(parts[1]),
                  let s = Double(parts[2]) else { return nil }
            return h * 3600 + m * 60 + s
        case 2:
            guard let m = Double(parts[0]),
                  let s = Double(parts[1]) else { return nil }
            return m * 60 + s
        case 1:
            return Double(trimmed)
        default:
            return nil
        }
    }
}
