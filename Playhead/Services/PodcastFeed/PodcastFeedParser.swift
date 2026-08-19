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
    /// The channel-level `<link>` — RSS 2.0's "URL of the website
    /// corresponding to the channel" (Atom: the feed-level `rel="alternate"`
    /// link). playhead-e8mg: captured because `OwnershipGraph` documents it as
    /// a structural ownership source and nothing was ever feeding it.
    ///
    /// READ THIS BEFORE TREATING IT AS "THE SHOW'S SITE". Measured over 918
    /// real feeds (2026-08-19): 859 carry one, **175 of them point at the
    /// feed's own host** and 57 % land on a domain shared by two or more
    /// different shows — iheart.com, art19.com, spotify.com, siriusxm.com,
    /// wondery.com, libsyn.com. It says where the show is PUBLISHED, which is
    /// often the network or the hosting platform. `OwnershipGraph` is what
    /// decides whether it may be believed, not this field.
    var siteURL: URL? = nil
    /// The `<itunes:owner><itunes:email>` address, verbatim. Not a domain —
    /// the eTLD+1 extraction belongs to `DomainNormalizer`, and keeping the
    /// address means a later consumer that wants the local part still can.
    var ownerEmail: String? = nil
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
    /// playhead-gtt9.22: URL of the Podcasting 2.0 `<podcast:chapters>`
    /// JSON document, when the feed advertises one. Captured but NOT
    /// fetched at parse time — fetching is opt-in (it introduces a new
    /// network call) and is mediated by `ChapterEvidenceParser
    /// .parsePodcasting20Chapters(from:)` if/when the runtime opts in.
    var chaptersFeedURL: URL?
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
        categories: [], siteURL: nil, ownerEmail: nil,
        episodes: []
    )

    private var currentEpisode: ParsedEpisode?
    private var currentChapter: ParsedChapter?
    private var currentText = ""
    private var insideChannel = false
    private var insideItem = false
    private var insideAtomEntry = false
    /// Depth inside an RSS `<image>` block. It has its own `<link>`, and that
    /// link is NOT the channel link (playhead-e8mg). Measured on the real
    /// Diary of a CEO feed, whose only `<link>` element anywhere in the
    /// channel is `<image><link>` — and it holds the feed's own URL, so a
    /// parser that misses this distinction hands the ownership graph
    /// `flightcast.com`, the hosting platform, as the show's website. A
    /// counter rather than a Bool because `<itunes:image>` can nest.
    private var imageDepth = 0
    /// True between `<itunes:owner>` and `</itunes:owner>`. The email element
    /// is `<itunes:email>` either way, and only the one INSIDE `<itunes:owner>`
    /// is the owner's.
    private var insideITunesOwner = false
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
        Self.applySecurityHardening(to: parser)

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

    /// Apply the namespace + entity-resolution flags every feed parse
    /// operation depends on. Extracted as a static seam so tests can
    /// directly assert the configured properties on a parser instance
    /// without driving a full parse — defense in depth against a future
    /// refactor silently dropping the XXE / billion-laughs hardening.
    /// (Reviewer suggestion / rfu-mn.)
    static func applySecurityHardening(to parser: XMLParser) {
        parser.shouldProcessNamespaces = true
        parser.shouldReportNamespacePrefixes = false
        // Defense in depth against XXE / billion-laughs entity expansion.
        // Foundation's XMLParser defaults are safe today (external entities
        // are not resolved over the network), but a future refactor —
        // including adopting a different parser that reuses these flags —
        // could quietly regress. Pin both knobs explicitly so anyone
        // auditing this constructor sees the hardening.
        parser.shouldResolveExternalEntities = false
        parser.externalEntityResolvingPolicy = .never
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

        // RSS `<image>` (and `<itunes:image>`) open a scope that has its OWN
        // `<link>`. Counted unconditionally so the decrement in
        // `didEndElement` is unconditionally symmetric — a self-closing
        // `<itunes:image href="…"/>` reports both a start and an end.
        if local == "image" {
            imageDepth += 1
            // fall through: the iTunes-artwork branch below still runs
        }

        // `<itunes:owner>` scope. Its `<itunes:email>` is the owner's; an
        // `<itunes:email>` anywhere else is not (measured 2026-08-19: zero of
        // 918 real feeds carry one outside `<itunes:owner>`, so requiring the
        // wrapper costs nothing and cannot mistake a different address for it).
        if local == "owner" && ns == Self.itunesNS {
            insideITunesOwner = true
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

        // Atom link at feed level: artwork, or the site link.
        if local == "link" && isAtomFeed && !insideItem {
            let rel = attributes["rel"]
            if rel == "icon" || rel == "logo", let href = attributes["href"] {
                feed.artworkURL = resolveURL(href)
                return
            }
            // playhead-e8mg: Atom's site link is `rel="alternate"`, and the
            // attribute is OPTIONAL — RFC 4287 says a missing `rel` means
            // `alternate`. `self` (the feed's own address) and `hub`
            // (WebSub) are explicitly NOT it. First one wins, matching the
            // RSS branch below.
            if rel == nil || rel == "alternate", imageDepth == 0,
               feed.siteURL == nil, let href = attributes["href"] {
                feed.siteURL = resolveURL(href)
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
            // playhead-gtt9.22: Capture the URL into ParsedEpisode so the
            // runtime can opt-in to fetching the JSON document via
            // `ChapterEvidenceParser.parsePodcasting20Chapters`. We do not
            // fetch here — keeping the parser pure (no network calls from
            // XML parsing). When the JSON `type` attribute is absent we
            // still capture the URL; downstream code is responsible for
            // type validation (handled by HTTP content-negotiation + JSON
            // decode error suppression in the parser).
            if let href = attributes["url"] {
                currentEpisode?.chaptersFeedURL = resolveURL(href)
            }
            return
        }

        // iTunes namespace chapters link (rare; documented for parity).
        // Most podcasts using iTunes-style chapters embed them in ID3 CHAP
        // frames inside the audio file rather than a separate XML element,
        // so this branch is mostly here so we do not silently lump
        // iTunes-namespace `<chapters>` into the generic catch-all later.
        if local == "chapters" && ns == Self.itunesNS && insideItem {
            if let href = attributes["url"], currentEpisode?.chaptersFeedURL == nil {
                currentEpisode?.chaptersFeedURL = resolveURL(href)
            }
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

        // Mirror of the two scope counters opened in `didStartElement`.
        if local == "image" {
            imageDepth = max(0, imageDepth - 1)
            return
        }
        if local == "owner" && ns == Self.itunesNS {
            insideITunesOwner = false
            return
        }

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
        case ("link", ""):
            // playhead-e8mg. Two guards, and each is a measured defect class:
            //  * `imageDepth == 0` — `<image><link>` is the artwork's link,
            //    not the channel's. On the real Diary of a CEO feed that is
            //    the ONLY `<link>` in the whole channel and it holds the feed
            //    URL, so without this the show's "website" is flightcast.com.
            //  * first-one-wins — RSS orders `<link>` before `<image>`, and
            //    the same rule already governs `title` and `author` here.
            if feed.siteURL == nil, imageDepth == 0 {
                feed.siteURL = resolveURL(text)
            }
        case ("email", Self.itunesNS):
            if insideITunesOwner, feed.ownerEmail == nil {
                feed.ownerEmail = text
            }
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
            chaptersFeedURL: nil,
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
