// NetworkIdentity.swift
// ef2.5.2: Network identity derived from RSS metadata.
//
// Derives a normalized network identifier from multiple RSS metadata
// sources (iTunes author, managing editor, feed URL domain, publisher,
// title prefix). Confidence increases when multiple sources agree.
//
// Consumed by PriorHierarchyResolver (ef2.5.3).

import Foundation

// MARK: - NetworkIdentitySource

/// Which RSS metadata field contributed to a network identity derivation.
enum NetworkIdentitySource: String, Sendable, Codable, CaseIterable {
    case itunesAuthor
    case managingEditor
    case feedDomain
    case publisher
    case titlePrefix
}

// MARK: - NetworkIdentity

/// A derived network identity for a podcast, representing which network
/// or publisher produced the show. Confidence is higher when multiple
/// independent metadata sources agree on the same identity.
struct NetworkIdentity: Sendable, Equatable, Codable {
    /// Normalized lowercase identifier suitable for grouping (e.g. "npr", "gimlet").
    let networkId: String
    /// Human-readable network name preserving original casing.
    let networkName: String
    /// Which metadata fields contributed to this derivation.
    let derivedFrom: Set<NetworkIdentitySource>
    /// Confidence in [0, 1]. Higher when multiple sources agree.
    let confidence: Float
}

// MARK: - NetworkIdentityExtractor

/// Stateless extractor that derives a NetworkIdentity from podcast feed metadata.
/// Uses an enum namespace (no instances) following the project convention for
/// pure-function utility types.
enum NetworkIdentityExtractor {

    // MARK: - Public API

    /// Derive a network identity from available feed metadata.
    ///
    /// - Parameters:
    ///   - itunesAuthor: Value of `<itunes:author>` from the feed.
    ///   - managingEditor: Value of `<managingEditor>` from the feed.
    ///   - feedURL: The feed's URL (domain used as a signal).
    ///   - publisher: Value of a `<publisher>` or similar field.
    ///   - title: The podcast title (checked for network prefix patterns like "NPR: ...").
    /// - Returns: A `NetworkIdentity` if at least one source yields a usable signal, nil otherwise.
    static func extractIdentity(
        itunesAuthor: String? = nil,
        managingEditor: String? = nil,
        feedURL: URL? = nil,
        publisher: String? = nil,
        title: String? = nil
    ) -> NetworkIdentity? {
        var candidates: [(name: String, normalized: String, source: NetworkIdentitySource)] = []

        if let author = itunesAuthor, !author.trimmingCharacters(in: .whitespaces).isEmpty {
            let trimmed = author.trimmingCharacters(in: .whitespaces)
            candidates.append((trimmed, normalize(trimmed), .itunesAuthor))
        }

        if let editor = managingEditor, !editor.trimmingCharacters(in: .whitespaces).isEmpty {
            let trimmed = extractNameFromEmail(editor)
            if !trimmed.isEmpty {
                candidates.append((trimmed, normalize(trimmed), .managingEditor))
            }
        }

        if let url = feedURL {
            if let domainName = extractNetworkFromDomain(url) {
                candidates.append((domainName, normalize(domainName), .feedDomain))
            }
        }

        if let pub = publisher, !pub.trimmingCharacters(in: .whitespaces).isEmpty {
            let trimmed = pub.trimmingCharacters(in: .whitespaces)
            candidates.append((trimmed, normalize(trimmed), .publisher))
        }

        if let t = title {
            if let prefix = extractNetworkPrefix(t) {
                candidates.append((prefix, normalize(prefix), .titlePrefix))
            }
        }

        guard !candidates.isEmpty else { return nil }

        // Group candidates by normalized ID and pick the largest cluster.
        var clusters: [String: [(name: String, source: NetworkIdentitySource)]] = [:]
        for c in candidates {
            clusters[c.normalized, default: []].append((c.name, c.source))
        }

        // Pick the cluster with the most sources.
        let best = clusters.max(by: { $0.value.count < $1.value.count })!
        let sources = Set(best.value.map(\.source))

        // Prefer the longest human-readable name from the winning cluster.
        let displayName = best.value.max(by: { $0.name.count < $1.name.count })!.name

        let confidence = computeConfidence(sourceCount: sources.count)

        return NetworkIdentity(
            networkId: best.key,
            networkName: displayName,
            derivedFrom: sources,
            confidence: confidence
        )
    }

    // MARK: - Normalization

    /// Normalize a name to a stable lowercase identifier, stripping common
    /// suffixes like "media", "podcasts", "network", punctuation, and
    /// collapsing whitespace.
    static func normalize(_ name: String) -> String {
        var s = name.lowercased()
        // Strip common corporate suffixes. Loop until no more suffixes match,
        // so compound names like "Gimlet Podcast Network" fully reduce.
        let suffixes = [" media", " podcasts", " podcast", " network", " studios",
                        " entertainment", " inc", " llc", " ltd", " corp"]
        var changed = true
        while changed {
            changed = false
            for suffix in suffixes {
                if s.hasSuffix(suffix) {
                    s = String(s.dropLast(suffix.count))
                    changed = true
                }
            }
        }
        // Remove non-alphanumeric except spaces, then collapse whitespace.
        s = s.unicodeScalars
            .filter { CharacterSet.alphanumerics.contains($0) || $0 == " " }
            .map { String($0) }
            .joined()
        s = s.split(separator: " ").joined(separator: " ")
        s = s.trimmingCharacters(in: .whitespaces)
        return s
    }

    // MARK: - Helpers

    /// Extract a human-readable name from an RFC 822 email field.
    /// `"Jane Doe (NPR)" → "NPR"`, `"editor@npr.org" → "npr"`.
    static func extractNameFromEmail(_ field: String) -> String {
        let trimmed = field.trimmingCharacters(in: .whitespaces)

        // Check for parenthesized name: "someone (Network Name)"
        if let openParen = trimmed.firstIndex(of: "("),
           let closeParen = trimmed.firstIndex(of: ")"),
           openParen < closeParen {
            let start = trimmed.index(after: openParen)
            let name = String(trimmed[start..<closeParen]).trimmingCharacters(in: .whitespaces)
            if !name.isEmpty { return name }
        }

        // Check for angle-bracket format: "Network Name <email>"
        if let angleBracket = trimmed.firstIndex(of: "<") {
            let name = String(trimmed[trimmed.startIndex..<angleBracket]).trimmingCharacters(in: .whitespaces)
            if !name.isEmpty { return name }
        }

        // Plain email: extract domain minus TLD.
        if trimmed.contains("@") {
            let parts = trimmed.split(separator: "@")
            if parts.count == 2 {
                let domain = String(parts[1])
                return extractDomainLabel(domain)
            }
        }

        return trimmed
    }

    /// Extract a network name from a feed URL domain.
    /// Strips common podcast hosting domains (libsyn, megaphone, etc.)
    /// and returns the meaningful domain label.
    static func extractNetworkFromDomain(_ url: URL) -> String? {
        guard let host = url.host?.lowercased() else { return nil }

        // Skip generic podcast hosting domains — they don't indicate a network.
        let hostingDomains = [
            "feeds.libsyn.com", "feeds.megaphone.fm", "anchor.fm",
            "feeds.buzzsprout.com", "feeds.simplecast.com",
            "feeds.transistor.fm", "feeds.podbean.com", "rss.art19.com",
            "feeds.acast.com", "omnycontent.com", "podtrac.com",
            "feeds.fireside.fm", "pinecast.com", "spreaker.com",
            "soundcloud.com", "feedburner.com", "feeds.feedburner.com"
        ]
        for hosting in hostingDomains {
            if host == hosting || host.hasSuffix(".\(hosting)") {
                return nil
            }
        }

        return extractDomainLabel(host)
    }

    /// Given "feeds.npr.org" → "npr", "podcasts.vox.com" → "vox".
    static func extractDomainLabel(_ host: String) -> String {
        let components = host.split(separator: ".")
        // Strip known subdomains and TLDs to find the meaningful label.
        let skipPrefixes: Set<String> = ["feeds", "rss", "feed", "podcasts", "podcast", "www", "audio"]
        let skipSuffixes: Set<String> = ["com", "org", "net", "fm", "io", "co", "us", "uk", "media"]

        let meaningful = components.filter { part in
            let s = String(part)
            return !skipPrefixes.contains(s) && !skipSuffixes.contains(s)
        }

        if let label = meaningful.first {
            return String(label)
        }
        // Fallback: second-to-last component (the domain name).
        if components.count >= 2 {
            return String(components[components.count - 2])
        }
        return host
    }

    /// Extract a network prefix from a podcast title.
    /// Patterns: "NPR: Fresh Air", "Vox | The Weeds", "Gimlet - Reply All".
    static func extractNetworkPrefix(_ title: String) -> String? {
        let separators: [Character] = [":", "|", "–", "—"]
        for sep in separators {
            if let idx = title.firstIndex(of: sep) {
                let prefix = String(title[title.startIndex..<idx]).trimmingCharacters(in: .whitespaces)
                // Only treat as network if prefix is short (likely a network name, not a subtitle).
                if !prefix.isEmpty && prefix.count <= 30 && prefix.count >= 2 {
                    return prefix
                }
            }
        }
        // Check for " - " (hyphen with spaces, distinct from intra-word hyphens).
        if let range = title.range(of: " - ") {
            let prefix = String(title[title.startIndex..<range.lowerBound]).trimmingCharacters(in: .whitespaces)
            if !prefix.isEmpty && prefix.count <= 30 && prefix.count >= 2 {
                return prefix
            }
        }
        return nil
    }

    /// Compute confidence based on how many independent sources agree.
    static func computeConfidence(sourceCount: Int) -> Float {
        // Single source: 0.4 base. Each additional agreeing source adds 0.2, capped at 1.0.
        let base: Float = 0.4
        let perSource: Float = 0.2
        let raw = base + perSource * Float(sourceCount - 1)
        return min(1.0, raw)
    }
}
