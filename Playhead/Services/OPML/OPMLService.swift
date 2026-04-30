// OPMLService.swift
// playhead-2jo: OPML subscription import / export.
//
// OPML (Outline Processor Markup Language) is the de-facto standard for
// exchanging podcast subscription lists between players. This service
// covers three responsibilities:
//
//   1. `parseOPML(from:)`     — Data → [OPMLFeed]   (pure, sync)
//   2. `serializeOPML(...)`   — [OPMLFeed] → Data   (pure, sync)
//   3. `importFeeds(...)`     — runs feed validation through
//                               PodcastDiscoveryService and persists
//                               results into SwiftData
//
// The parser uses Foundation's XMLParser (delegate-based, streaming) so
// large OPML files do not balloon memory. The whole document is still
// read into Data first because Foundation's XMLParser does not expose a
// chunked-input API.

import Foundation

// MARK: - Public Types

/// A single feed entry parsed from an OPML document.
///
/// OPML carries no episode state — only feed identity and a display
/// title. The title is optional because some exports omit it; callers
/// (UI, importer) substitute a sensible fallback.
public struct OPMLFeed: Sendable, Equatable, Hashable {
    public let title: String?
    public let xmlUrl: URL

    public init(title: String?, xmlUrl: URL) {
        self.title = title
        self.xmlUrl = xmlUrl
    }
}

/// Outcome of an import run.
public struct OPMLImportResult: Sendable, Equatable {
    /// Successfully resolved + persisted feeds.
    public let imported: Int
    /// Feeds skipped because the same `feedURL` is already in the library.
    public let skippedDuplicate: Int
    /// Feeds the importer could not resolve, with a human-readable reason.
    public let failed: [Failure]

    public struct Failure: Sendable, Equatable {
        public let url: URL
        public let reason: String

        public init(url: URL, reason: String) {
            self.url = url
            self.reason = reason
        }
    }

    public init(imported: Int, skippedDuplicate: Int, failed: [Failure]) {
        self.imported = imported
        self.skippedDuplicate = skippedDuplicate
        self.failed = failed
    }

    /// Total number of feeds attempted.
    public var attempted: Int { imported + skippedDuplicate + failed.count }
}

/// Errors surfaced by the OPML pipeline. UI layers translate these into
/// user-facing copy.
public enum OPMLError: Error, LocalizedError, Equatable {
    /// XML failed to parse, or the root document is not OPML.
    case invalidFormat
    /// XML parsed cleanly but contained no `<outline>` elements with a
    /// usable `xmlUrl`.
    case emptyFile
    /// File-system read failed (file picker handed us a URL we could not
    /// open). Wraps the underlying NSError.
    case fileReadFailed(String)

    public var errorDescription: String? {
        switch self {
        case .invalidFormat:
            "This file doesn't appear to be an OPML subscription list."
        case .emptyFile:
            "The OPML file contains no podcast feeds."
        case .fileReadFailed(let detail):
            "Could not read OPML file: \(detail)"
        }
    }
}

// MARK: - OPMLService

/// Stateless service for OPML parse / serialize. Marked as a `struct`
/// rather than an `actor` because both operations are pure functions of
/// their inputs — no shared mutable state. Future `importFeeds` work
/// (resolving + persisting feeds via `PodcastDiscoveryService`) lives on
/// a separate actor seam to keep this type free of @MainActor coupling.
public struct OPMLService: Sendable {

    public init() {}

    // MARK: - Parse

    /// Parse an OPML document into a flat list of feed entries.
    ///
    /// - Throws: ``OPMLError/invalidFormat`` if the XML is malformed or
    ///   the root element is not `<opml>`. ``OPMLError/emptyFile`` if the
    ///   document has no usable `<outline>` entries.
    public func parseOPML(from data: Data) throws -> [OPMLFeed] {
        guard !data.isEmpty else { throw OPMLError.invalidFormat }

        let parser = XMLParser(data: data)
        // Defense in depth against XXE / billion-laughs entity expansion,
        // mirroring `FeedParser.applySecurityHardening`.
        parser.shouldProcessNamespaces = false
        parser.shouldResolveExternalEntities = false
        parser.externalEntityResolvingPolicy = .never

        let delegate = OPMLParserDelegate()
        parser.delegate = delegate

        guard parser.parse() else {
            throw OPMLError.invalidFormat
        }
        if !delegate.sawOPMLRoot {
            throw OPMLError.invalidFormat
        }
        let unique = OPMLParserDelegate.dedup(delegate.feeds)
        if unique.isEmpty {
            throw OPMLError.emptyFile
        }
        return unique
    }

    // MARK: - Serialize

    /// Serialize a list of feeds into an OPML 2.0 document.
    ///
    /// The output is deterministic (same input → same bytes) so callers
    /// can rely on byte-equality across runs. Attribute values are
    /// XML-escaped per `&`, `<`, `>`, `"`. UTF-8 characters are written
    /// verbatim (the document is UTF-8 encoded).
    public func serializeOPML(
        feeds: [OPMLFeed],
        documentTitle: String = "Playhead Subscriptions"
    ) -> Data {
        var xml = ""
        xml.reserveCapacity(160 + feeds.count * 96)
        xml.append(#"<?xml version="1.0" encoding="UTF-8"?>"#)
        xml.append("\n")
        xml.append(#"<opml version="2.0">"#)
        xml.append("\n  <head><title>")
        xml.append(Self.escapeXML(documentTitle))
        xml.append("</title></head>\n")
        xml.append("  <body>\n")
        for feed in feeds {
            let title = feed.title?.trimmingCharacters(in: .whitespacesAndNewlines)
            let displayTitle = (title?.isEmpty ?? true)
                ? feed.xmlUrl.absoluteString
                : title!
            xml.append("    <outline ")
            xml.append(#"type="rss" "#)
            xml.append(#"text=""#)
            xml.append(Self.escapeXML(displayTitle))
            xml.append(#"" "#)
            xml.append(#"xmlUrl=""#)
            xml.append(Self.escapeXML(feed.xmlUrl.absoluteString))
            xml.append(#""/>"#)
            xml.append("\n")
        }
        xml.append("  </body>\n")
        xml.append("</opml>\n")
        return Data(xml.utf8)
    }

    /// XML attribute-value escape. Only the five characters that change
    /// the meaning of a quoted attribute need escaping; everything else
    /// (including UTF-8) passes through verbatim.
    private static func escapeXML(_ s: String) -> String {
        var out = ""
        out.reserveCapacity(s.count)
        for ch in s {
            switch ch {
            case "&": out.append("&amp;")
            case "<": out.append("&lt;")
            case ">": out.append("&gt;")
            case "\"": out.append("&quot;")
            case "'": out.append("&apos;")
            default: out.append(ch)
            }
        }
        return out
    }
}

// MARK: - XMLParser Delegate

/// Streaming OPML reader. Walks every `<outline>` element and emits one
/// `OPMLFeed` per leaf with a usable `xmlUrl` attribute. Folder outlines
/// (no `xmlUrl`) are recursed into but not emitted.
private final class OPMLParserDelegate: NSObject, XMLParserDelegate {
    var feeds: [OPMLFeed] = []
    var sawOPMLRoot = false

    func parser(
        _ parser: XMLParser,
        didStartElement elementName: String,
        namespaceURI: String?,
        qualifiedName qName: String?,
        attributes: [String: String]
    ) {
        let local = elementName.lowercased()
        if local == "opml" {
            sawOPMLRoot = true
            return
        }
        guard local == "outline" else { return }

        // OPML attribute lookup is case-insensitive in the wild — both
        // `xmlUrl` (RFC) and `xmlurl` are seen. Lower-case the keys once.
        let attrs = Dictionary(
            uniqueKeysWithValues: attributes.map { ($0.key.lowercased(), $0.value) }
        )

        guard let raw = attrs["xmlurl"]?.trimmingCharacters(in: .whitespacesAndNewlines),
              !raw.isEmpty,
              let url = URL(string: raw),
              url.scheme != nil
        else {
            // Folder outline (no xmlUrl) — children may still be feeds, so
            // we just skip emitting and let the parser recurse naturally.
            return
        }

        // Title fallback chain: `title` → `text` → nil. Both are explicit
        // OPML attributes; `title` is the spec-preferred display name and
        // `text` is what most exporters populate.
        let rawTitle = (attrs["title"] ?? attrs["text"])?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let title = (rawTitle?.isEmpty ?? true) ? nil : rawTitle

        feeds.append(OPMLFeed(title: title, xmlUrl: url))
    }

    /// Keep the first occurrence of each `xmlUrl`. Order-preserving so
    /// callers can rely on document order. Compares URLs by their
    /// `absoluteString` to side-step `URL.==` quirks around fragments.
    static func dedup(_ feeds: [OPMLFeed]) -> [OPMLFeed] {
        var seen: Set<String> = []
        var result: [OPMLFeed] = []
        result.reserveCapacity(feeds.count)
        for feed in feeds {
            let key = feed.xmlUrl.absoluteString
            if seen.insert(key).inserted {
                result.append(feed)
            }
        }
        return result
    }
}
