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

    // MARK: - Import

    /// Result type for the per-feed resolve seam. Strings carry
    /// human-readable reasons (e.g. "HTTP 404", "DNS lookup failed") so
    /// the result-summary UI can surface them verbatim.
    public enum ResolveOutcome: Sendable {
        case success(Void)
        case failure(String)
    }

    /// Run the OPML feeds through the given resolve / persist seam.
    ///
    /// - Parameters:
    ///   - feeds: Parsed OPML entries. Order is preserved in the
    ///     returned counts.
    ///   - exists: Synchronous predicate that returns `true` if a
    ///     podcast with this `feedURL` is already in the library.
    ///     Production binds this to a SwiftData fetch.
    ///   - resolve: Async closure that validates / fetches feed
    ///     metadata. Returns `.success` if the feed is reachable,
    ///     `.failure(reason)` otherwise.
    ///   - persist: Async closure invoked exactly once per
    ///     successfully-resolved, non-duplicate feed. Production binds
    ///     this to `PodcastDiscoveryService.persist`.
    ///   - progress: Reports `(completed, total)` after each feed
    ///     finishes (resolved, duplicate-skipped, or failed).
    ///   - maxConcurrent: Upper bound on concurrent resolves. Defaults
    ///     to 5, matching the spec's "5 simultaneous requests" rule.
    public func importFeeds(
        _ feeds: [OPMLFeed],
        exists: @escaping @Sendable (URL) async -> Bool,
        resolve: @escaping @Sendable (URL) async -> ResolveOutcome,
        persist: @escaping @Sendable (OPMLFeed) async -> Void,
        progress: @escaping @Sendable (Int, Int) -> Void,
        maxConcurrent: Int = 5
    ) async -> OPMLImportResult {
        // De-duplicate the input list itself so a malformed OPML with
        // repeated entries cannot inflate the counts.
        let deduped = OPMLParserDelegate.dedup(feeds)
        let total = deduped.count
        guard total > 0 else {
            return OPMLImportResult(imported: 0, skippedDuplicate: 0, failed: [])
        }

        let agg = OPMLImportAggregator()

        // Fan out work in chunks of `maxConcurrent` rather than spawning
        // `total` tasks at once. A chunked TaskGroup is a simpler and
        // more predictable concurrency cap than a homemade semaphore.
        let limit = max(1, maxConcurrent)
        await withTaskGroup(of: Void.self) { group in
            var index = 0
            // Prime the group with up to `limit` workers.
            while index < deduped.count, index < limit {
                let feed = deduped[index]
                index += 1
                group.addTask {
                    await Self.processOne(
                        feed: feed,
                        total: total,
                        exists: exists,
                        resolve: resolve,
                        persist: persist,
                        progress: progress,
                        aggregator: agg
                    )
                }
            }
            // Each time a worker finishes, kick off the next feed.
            while await group.next() != nil {
                if index < deduped.count {
                    let feed = deduped[index]
                    index += 1
                    group.addTask {
                        await Self.processOne(
                            feed: feed,
                            total: total,
                            exists: exists,
                            resolve: resolve,
                            persist: persist,
                            progress: progress,
                            aggregator: agg
                        )
                    }
                }
            }
        }

        let snapshot = await agg.snapshot()
        return OPMLImportResult(
            imported: snapshot.imported,
            skippedDuplicate: snapshot.skipped,
            failed: snapshot.failed
        )
    }

    /// Per-feed worker. Extracted so both the priming loop and the
    /// drain loop in `importFeeds` add identical tasks.
    private static func processOne(
        feed: OPMLFeed,
        total: Int,
        exists: @escaping @Sendable (URL) async -> Bool,
        resolve: @escaping @Sendable (URL) async -> ResolveOutcome,
        persist: @escaping @Sendable (OPMLFeed) async -> Void,
        progress: @escaping @Sendable (Int, Int) -> Void,
        aggregator: OPMLImportAggregator
    ) async {
        // Each branch returns the unique `done` value the increment
        // produced — this guarantees the progress callback sees a
        // distinct count per feed even under concurrent execution.
        // (Reading `completed()` in a separate hop is racy: two
        // increments can both observe the same post-second-increment
        // value and emit duplicate progress events.)
        let done: Int
        if await exists(feed.xmlUrl) {
            done = await aggregator.recordSkippedReturningDone()
        } else {
            switch await resolve(feed.xmlUrl) {
            case .success:
                await persist(feed)
                done = await aggregator.recordImportedReturningDone()
            case .failure(let reason):
                done = await aggregator.recordFailedReturningDone(
                    feed.xmlUrl, reason
                )
            }
        }
        progress(done, total)
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

// MARK: - Import Aggregator

/// Concurrent-safe accumulator for `importFeeds`. File-private (rather
/// than nested inside `importFeeds`) so the static `processOne` worker
/// can name its parameter type without resorting to `any AnyActor`.
final actor OPMLImportAggregator {
    private(set) var imported = 0
    private(set) var skipped = 0
    private(set) var failed: [OPMLImportResult.Failure] = []
    private(set) var done = 0

    func recordImported() { imported += 1; done += 1 }
    func recordSkipped() { skipped += 1; done += 1 }
    func recordFailed(_ url: URL, _ reason: String) {
        failed.append(.init(url: url, reason: reason))
        done += 1
    }

    /// Atomic record + read so concurrent feeds get distinct `done` values.
    func recordImportedReturningDone() -> Int {
        recordImported()
        return done
    }
    func recordSkippedReturningDone() -> Int {
        recordSkipped()
        return done
    }
    func recordFailedReturningDone(_ url: URL, _ reason: String) -> Int {
        recordFailed(url, reason)
        return done
    }

    func completed() -> Int { done }

    func snapshot() -> Snapshot {
        Snapshot(imported: imported, skipped: skipped, failed: failed)
    }

    struct Snapshot: Sendable {
        let imported: Int
        let skipped: Int
        let failed: [OPMLImportResult.Failure]
    }
}
