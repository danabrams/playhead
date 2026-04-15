// SponsorEntityGraph.swift
// playhead-ef2.1.1: eTLD+1 normalization, cross-entity linking,
// path-shape features, co-occurrence alias discovery, and canonical
// sponsor identity resolution.
//
// This is a pure-logic layer on top of SponsorKnowledgeStore. It does
// not modify the existing store; instead it reads entries and builds
// an in-memory identity graph connecting sponsors, URLs, CTAs, and
// disclosures into canonical sponsor identities.

import Foundation

// MARK: - DomainNormalizer

/// URL-aware domain normalization: eTLD+1 extraction, tracking parameter
/// stripping, and path-shape feature extraction. Operates on raw URL
/// strings extracted from transcripts (which may lack scheme prefixes).
enum DomainNormalizer {

    // MARK: - eTLD+1 Extraction

    /// Known second-level TLDs that form part of the effective TLD.
    /// This is a minimal on-device list covering common podcast sponsor
    /// domains; not a full Public Suffix List (which would require
    /// network access or a large embedded database).
    private static let multiPartTLDs: Set<String> = [
        "co.uk", "co.jp", "co.kr", "co.nz", "co.za", "co.in",
        "com.au", "com.br", "com.cn", "com.mx", "com.tw",
        "org.uk", "net.au", "ac.uk", "gov.uk",
    ]

    /// Tracking / analytics query parameters to strip.
    private static let trackingParams: Set<String> = [
        "utm_source", "utm_medium", "utm_campaign", "utm_term",
        "utm_content", "utm_id", "fbclid", "gclid", "gclsrc",
        "dclid", "msclkid", "twclid", "ref", "source",
        "affiliate", "aff", "clickid", "irclickid",
    ]

    /// Extract the eTLD+1 from a URL string. Returns the registrable
    /// domain (e.g., "squarespace.com" from "www.squarespace.com/podcast?utm_source=foo").
    /// Returns nil if the input cannot be parsed as a domain.
    static func etld1(from urlString: String) -> String? {
        let host = extractHost(from: urlString)
        guard let host, !host.isEmpty else { return nil }

        // Reject IP addresses (v4 and v6).
        if host.allSatisfy({ $0.isNumber || $0 == "." }) { return nil }
        if host.contains(":") { return nil } // IPv6

        let labels = host.lowercased().split(separator: ".").map(String.init)
        guard labels.count >= 2 else { return nil }

        // Check for multi-part TLD (e.g., "co.uk").
        if labels.count >= 3 {
            let lastTwo = labels.suffix(2).joined(separator: ".")
            if multiPartTLDs.contains(lastTwo) {
                return labels.suffix(3).joined(separator: ".")
            }
        }

        // Standard case: last two labels.
        return labels.suffix(2).joined(separator: ".")
    }

    /// Strip tracking/UTM parameters from a URL string, returning a
    /// cleaned URL. Non-tracking query parameters are preserved.
    static func stripTrackingParams(from urlString: String) -> String {
        // Ensure we have a scheme for URLComponents parsing.
        let withScheme: String
        if urlString.lowercased().hasPrefix("http://") || urlString.lowercased().hasPrefix("https://") {
            withScheme = urlString
        } else {
            withScheme = "https://" + urlString
        }

        guard var components = URLComponents(string: withScheme) else {
            return urlString
        }

        if let queryItems = components.queryItems {
            let filtered = queryItems.filter { item in
                !trackingParams.contains(item.name.lowercased())
            }
            components.queryItems = filtered.isEmpty ? nil : filtered
        }

        // Strip the scheme we may have added.
        guard let result = components.string else { return urlString }
        if !urlString.lowercased().hasPrefix("http") && result.hasPrefix("https://") {
            return String(result.dropFirst("https://".count))
        }
        return result
    }

    /// Full URL normalization: lowercase host, strip tracking params,
    /// remove trailing slashes, drop default port numbers.
    static func normalizeURL(_ urlString: String) -> String {
        let stripped = stripTrackingParams(from: urlString)

        // Ensure scheme for parsing.
        let withScheme: String
        if stripped.lowercased().hasPrefix("http://") || stripped.lowercased().hasPrefix("https://") {
            withScheme = stripped
        } else {
            withScheme = "https://" + stripped
        }

        guard var components = URLComponents(string: withScheme) else {
            return stripped.lowercased()
        }

        components.host = components.host?.lowercased()

        // Remove default ports.
        if components.port == 80 || components.port == 443 {
            components.port = nil
        }

        // Remove trailing slash from path.
        if components.path.hasSuffix("/") && components.path.count > 1 {
            components.path = String(components.path.dropLast())
        }

        guard let result = components.string else { return stripped.lowercased() }
        if !urlString.lowercased().hasPrefix("http") && result.hasPrefix("https://") {
            return String(result.dropFirst("https://".count))
        }
        return result
    }

    // MARK: - Path-Shape Features

    /// Extract a path-shape feature string from a URL. The shape
    /// normalizes variable path segments (e.g., show-specific slugs)
    /// into placeholders while preserving structural patterns.
    ///
    /// Examples:
    ///   "/podcast"            -> "/podcast"
    ///   "/podcast/my-show"    -> "/podcast/*"
    ///   "/offer/code/ABC123"  -> "/offer/code/*"
    ///   "/"                   -> "/"
    static func pathShape(from urlString: String) -> String {
        let withScheme: String
        if urlString.lowercased().hasPrefix("http://") || urlString.lowercased().hasPrefix("https://") {
            withScheme = urlString
        } else {
            withScheme = "https://" + urlString
        }

        guard let components = URLComponents(string: withScheme) else {
            return "/"
        }

        let path = components.path
        guard !path.isEmpty, path != "/" else { return "/" }

        let segments = path.split(separator: "/").map(String.init)
        guard !segments.isEmpty else { return "/" }

        // Known static path segments common in podcast sponsor URLs.
        let staticSegments: Set<String> = [
            "podcast", "podcasts", "offer", "offers", "promo",
            "code", "deal", "deals", "try", "start", "go",
            "special", "partner", "ref", "show", "radio",
        ]

        let shaped = segments.map { segment -> String in
            if staticSegments.contains(segment.lowercased()) {
                return segment.lowercased()
            }
            return "*"
        }

        return "/" + shaped.joined(separator: "/")
    }

    // MARK: - Private Helpers

    /// Extract the host from a URL string that may or may not have a scheme.
    private static func extractHost(from urlString: String) -> String? {
        let withScheme: String
        if urlString.lowercased().hasPrefix("http://") || urlString.lowercased().hasPrefix("https://") {
            withScheme = urlString
        } else {
            withScheme = "https://" + urlString
        }

        guard let components = URLComponents(string: withScheme) else {
            return nil
        }

        return components.host
    }
}

// MARK: - SponsorIdentityNode

/// A node in the sponsor identity graph, representing a single
/// canonical sponsor with all linked entities.
struct SponsorIdentityNode: Sendable, Equatable {
    /// Stable canonical ID for this sponsor identity. Derived from
    /// the earliest-seen sponsor entry's ID.
    let canonicalSponsorId: String

    /// Primary display name for the sponsor.
    let displayName: String

    /// All known names/aliases for this sponsor.
    let names: Set<String>

    /// All linked domain eTLD+1 values.
    let domains: Set<String>

    /// All linked promo codes / CTAs.
    let promoCodes: Set<String>

    /// Path shapes observed across linked URLs.
    let pathShapes: Set<String>

    /// IDs of all SponsorKnowledgeEntry records that compose this node.
    let entryIds: Set<String>
}

// MARK: - CoOccurrenceRecord

/// Tracks co-occurrence of two entity values within the same ad read
/// (same analysis asset + overlapping atom ordinals).
struct CoOccurrenceRecord: Sendable, Equatable {
    let valueA: String
    let valueB: String
    var count: Int
}

// MARK: - SponsorEntityGraph

/// Builds an in-memory identity graph from SponsorKnowledgeStore entries.
/// Connects sponsors, URLs, CTAs, and disclosures into canonical identity
/// nodes based on domain overlap, co-occurrence, and alias relationships.
///
/// Usage:
///   let graph = SponsorEntityGraph(entries: entries, coOccurrences: records)
///   let node = graph.canonicalNode(forEntryId: someEntry.id)
///   let id = graph.canonicalSponsorId(forEntryId: someEntry.id)
struct SponsorEntityGraph: Sendable {

    /// All identity nodes in the graph.
    let nodes: [SponsorIdentityNode]

    /// Lookup: entry ID -> canonical sponsor ID.
    private let entryToCanonical: [String: String]

    /// Lookup: normalized name -> canonical sponsor ID.
    private let nameToCanonical: [String: String]

    /// Lookup: eTLD+1 domain -> canonical sponsor ID.
    private let domainToCanonical: [String: String]

    // MARK: - Build

    /// Build the identity graph from a set of knowledge entries and
    /// optional co-occurrence records.
    ///
    /// Linking rules (applied in order):
    /// 1. Entries sharing a domain eTLD+1 are linked.
    /// 2. Sponsor entries whose aliases overlap are linked.
    /// 3. Co-occurring entity pairs above threshold are linked.
    ///
    /// - Parameters:
    ///   - entries: All knowledge entries for a podcast (any state/type).
    ///   - coOccurrences: Co-occurrence records from alias discovery.
    ///   - coOccurrenceThreshold: Minimum co-occurrence count to link.
    init(
        entries: [SponsorKnowledgeEntry],
        coOccurrences: [CoOccurrenceRecord] = [],
        coOccurrenceThreshold: Int = 2
    ) {
        // Union-Find for merging entries into groups.
        let uf = UnionFind(count: entries.count)

        // Index entries by position for union-find.
        let indexById: [String: Int] = Dictionary(
            uniqueKeysWithValues: entries.enumerated().map { ($1.id, $0) }
        )

        // --- Rule 1: Domain eTLD+1 linking ---
        // For each URL-type entry, extract eTLD+1 and group entries
        // sharing the same domain.
        var domainToIndices: [String: [Int]] = [:]
        for (i, entry) in entries.enumerated() {
            if entry.entityType == .url {
                if let domain = DomainNormalizer.etld1(from: entry.entityValue) {
                    domainToIndices[domain, default: []].append(i)
                }
            }
        }
        for (_, indices) in domainToIndices {
            for j in 1..<indices.count {
                uf.union(indices[0], indices[j])
            }
        }

        // --- Rule 2: Alias overlap linking ---
        // Sponsor entries with overlapping aliases belong together.
        var normalizedNameToIndex: [String: Int] = [:]
        for (i, entry) in entries.enumerated() where entry.entityType == .sponsor {
            let allNames = [entry.normalizedValue] + entry.aliases.map {
                $0.lowercased().trimmingCharacters(in: .whitespaces)
            }
            for name in allNames {
                if let existing = normalizedNameToIndex[name] {
                    uf.union(existing, i)
                } else {
                    normalizedNameToIndex[name] = i
                }
            }
        }

        // --- Rule 3: Co-occurrence linking ---
        // Link entries whose entity values co-occur frequently.
        let valueToIndex: [String: Int] = Dictionary(
            entries.enumerated().map { ($1.normalizedValue, $0) },
            uniquingKeysWith: { first, _ in first }
        )
        for record in coOccurrences where record.count >= coOccurrenceThreshold {
            if let iA = valueToIndex[record.valueA],
               let iB = valueToIndex[record.valueB] {
                uf.union(iA, iB)
            }
        }

        // --- Build nodes from union-find groups ---
        var groups: [Int: [Int]] = [:]
        for i in 0..<entries.count {
            let root = uf.find(i)
            groups[root, default: []].append(i)
        }

        var builtNodes: [SponsorIdentityNode] = []
        var builtEntryToCanonical: [String: String] = [:]
        var builtNameToCanonical: [String: String] = [:]
        var builtDomainToCanonical: [String: String] = [:]

        for (_, memberIndices) in groups {
            let memberEntries = memberIndices.map { entries[$0] }

            // Canonical ID: earliest-seen sponsor entry, or earliest entry.
            let sponsorEntries = memberEntries.filter { $0.entityType == .sponsor }
            let anchor = (sponsorEntries.min(by: { $0.firstSeenAt < $1.firstSeenAt })
                ?? memberEntries.min(by: { $0.firstSeenAt < $1.firstSeenAt }))!
            let canonId = anchor.id

            // Collect names.
            var names = Set<String>()
            for entry in memberEntries where entry.entityType == .sponsor {
                names.insert(entry.normalizedValue)
                for alias in entry.aliases {
                    let n = alias.lowercased().trimmingCharacters(in: .whitespaces)
                    if !n.isEmpty { names.insert(n) }
                }
            }

            // Collect domains and path shapes.
            var domains = Set<String>()
            var pathShapes = Set<String>()
            for entry in memberEntries where entry.entityType == .url {
                if let d = DomainNormalizer.etld1(from: entry.entityValue) {
                    domains.insert(d)
                }
                let shape = DomainNormalizer.pathShape(from: entry.entityValue)
                pathShapes.insert(shape)
            }

            // Collect promo codes.
            var promoCodes = Set<String>()
            for entry in memberEntries where entry.entityType == .cta {
                promoCodes.insert(entry.normalizedValue)
            }

            // Display name: prefer the first sponsor entry's entityValue.
            let displayName = sponsorEntries.min(by: { $0.firstSeenAt < $1.firstSeenAt })?.entityValue
                ?? anchor.entityValue

            let node = SponsorIdentityNode(
                canonicalSponsorId: canonId,
                displayName: displayName,
                names: names,
                domains: domains,
                promoCodes: promoCodes,
                pathShapes: pathShapes,
                entryIds: Set(memberEntries.map(\.id))
            )
            builtNodes.append(node)

            // Build lookups.
            for entry in memberEntries {
                builtEntryToCanonical[entry.id] = canonId
            }
            for name in names {
                builtNameToCanonical[name] = canonId
            }
            for domain in domains {
                builtDomainToCanonical[domain] = canonId
            }
        }

        self.nodes = builtNodes
        self.entryToCanonical = builtEntryToCanonical
        self.nameToCanonical = builtNameToCanonical
        self.domainToCanonical = builtDomainToCanonical
    }

    // MARK: - Query

    /// Get the canonical sponsor ID for a knowledge entry.
    func canonicalSponsorId(forEntryId entryId: String) -> String? {
        entryToCanonical[entryId]
    }

    /// Get the full identity node for a knowledge entry.
    func canonicalNode(forEntryId entryId: String) -> SponsorIdentityNode? {
        guard let canonId = entryToCanonical[entryId] else { return nil }
        return nodes.first { $0.canonicalSponsorId == canonId }
    }

    /// Get the canonical sponsor ID by normalized sponsor name.
    func canonicalSponsorId(forName name: String) -> String? {
        nameToCanonical[name.lowercased().trimmingCharacters(in: .whitespaces)]
    }

    /// Get the canonical sponsor ID by domain eTLD+1.
    func canonicalSponsorId(forDomain domain: String) -> String? {
        domainToCanonical[domain.lowercased()]
    }
}

// MARK: - CoOccurrenceTracker

/// Tracks co-occurrence of entity values within the same ad read window.
/// Fed by candidate events sharing the same analysis asset and overlapping
/// atom ordinals, enabling discovery of aliases like "AG1" ↔ "Athletic Greens".
struct CoOccurrenceTracker: Sendable {

    private var counts: [String: Int] = [:]

    /// Record a co-occurrence between two entity values.
    mutating func record(valueA: String, valueB: String) {
        let a = valueA.lowercased().trimmingCharacters(in: .whitespaces)
        let b = valueB.lowercased().trimmingCharacters(in: .whitespaces)
        guard a != b else { return }
        let key = Self.pairKey(a, b)
        counts[key, default: 0] += 1
    }

    /// Build co-occurrence records from tracked candidate events.
    /// Groups events by analysis asset, then records co-occurrences
    /// for pairs with overlapping atom ordinal ranges.
    mutating func ingestEvents(_ events: [KnowledgeCandidateEvent]) {
        // Group by analysis asset.
        var byAsset: [String: [KnowledgeCandidateEvent]] = [:]
        for event in events {
            byAsset[event.analysisAssetId, default: []].append(event)
        }

        for (_, assetEvents) in byAsset {
            // For each pair of events in the same asset, check ordinal overlap.
            for i in 0..<assetEvents.count {
                for j in (i + 1)..<assetEvents.count {
                    let a = assetEvents[i]
                    let b = assetEvents[j]
                    if ordinalsOverlap(a.sourceAtomOrdinals, b.sourceAtomOrdinals) {
                        record(valueA: a.entityValue, valueB: b.entityValue)
                    }
                }
            }
        }
    }

    /// Get all co-occurrence records above a minimum count.
    func records(minCount: Int = 1) -> [CoOccurrenceRecord] {
        counts.compactMap { key, count in
            guard count >= minCount else { return nil }
            let parts = key.split(separator: "\t").map(String.init)
            guard parts.count == 2 else { return nil }
            return CoOccurrenceRecord(valueA: parts[0], valueB: parts[1], count: count)
        }
    }

    // MARK: - Private

    /// Create a stable pair key (alphabetically ordered).
    private static func pairKey(_ a: String, _ b: String) -> String {
        a < b ? "\(a)\t\(b)" : "\(b)\t\(a)"
    }

    /// Check if two ordinal arrays have any overlap or are adjacent
    /// (within 5 atoms, typical for same ad read).
    private func ordinalsOverlap(_ a: [Int], _ b: [Int]) -> Bool {
        guard let aMin = a.min(), let aMax = a.max(),
              let bMin = b.min(), let bMax = b.max() else { return false }
        // Overlapping or within 5 atoms of each other.
        return aMin <= bMax + 5 && bMin <= aMax + 5
    }
}

// MARK: - Union-Find (internal)

/// Simple union-find / disjoint-set for graph component merging.
private final class UnionFind: @unchecked Sendable {
    private var parent: [Int]
    private var rank: [Int]

    init(count: Int) {
        parent = Array(0..<count)
        rank = Array(repeating: 0, count: count)
    }

    func find(_ x: Int) -> Int {
        if parent[x] != x {
            parent[x] = find(parent[x])
        }
        return parent[x]
    }

    func union(_ x: Int, _ y: Int) {
        let px = find(x)
        let py = find(y)
        guard px != py else { return }
        if rank[px] < rank[py] {
            parent[px] = py
        } else if rank[px] > rank[py] {
            parent[py] = px
        } else {
            parent[py] = px
            rank[px] += 1
        }
    }
}
