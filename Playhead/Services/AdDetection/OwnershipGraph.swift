// OwnershipGraph.swift
// Phase ef2.1.2: Domain-level ownership resolution.
// Persistent per-show structure mapping domains -> AdOwnership labels.
//
// Sources:
//   - RSS <link>, <itunes:owner>, feed URL domain patterns
//   - High-frequency show-notes domains (frequency = showOwned signal)
//   - Explicit sponsor domain registrations
//
// Integration:
//   - SponsorEntityGraph: canonicalSponsorId(forDomain:) lookups
//   - DomainNormalizer: eTLD+1 extraction from SponsorEntityGraph.swift
//   - Shared by metadata parsing, lexical scanning, corrections, priors

import Foundation

// MARK: - DomainOwnershipLabel

/// Classifies a domain's relationship to the show.
enum DomainOwnershipLabel: String, Sendable, Codable, Hashable, CaseIterable {
    /// Domain belongs to the show itself (e.g., myshow.com, anchor.fm/myshow).
    case showOwned
    /// Domain belongs to an external sponsor (e.g., betterhelp.com/podcast).
    case sponsorOwned
    /// Domain belongs to the podcast network (e.g., wondery.com).
    case networkOwned
    /// Insufficient signal to classify.
    case unknown
}

// MARK: - DomainOwnershipEntry

/// A single domain -> ownership mapping with provenance metadata.
struct DomainOwnershipEntry: Sendable, Equatable {
    /// Normalized domain (eTLD+1 via DomainNormalizer).
    let domain: String
    /// Ownership classification.
    let label: DomainOwnershipLabel
    /// How this entry was determined.
    let source: DomainOwnershipSource
    /// Number of times this domain appeared across episodes (show-notes frequency).
    let frequency: Int
    /// Linked canonical sponsor ID from SponsorEntityGraph, if sponsorOwned.
    let canonicalSponsorId: String?
}

// MARK: - DomainOwnershipSource

/// Provenance for a domain ownership classification.
enum DomainOwnershipSource: String, Sendable, Codable, Hashable, CaseIterable {
    /// RSS <link> element pointing to show's website.
    case rssLink
    /// RSS <itunes:owner> or <itunes:author> domain.
    case itunesOwner
    /// RSS feed URL domain itself.
    case feedURL
    /// High-frequency domain in show notes across episodes.
    case showNotesFrequency
    /// Explicit sponsor domain registration (from SponsorKnowledgeStore).
    case sponsorRegistration
    /// Manual override (user correction).
    case userOverride
}

// MARK: - OwnershipGraphConfig

/// Tuning knobs for domain ownership classification.
struct OwnershipGraphConfig: Sendable {
    /// Minimum show-notes appearances for a domain to qualify as showOwned
    /// via frequency signal alone.
    let showOwnedFrequencyThreshold: Int

    /// Domains that appear in every episode (ratio >= this) are almost
    /// certainly show-owned infrastructure.
    let ubiquitousPresenceRatio: Double

    static let `default` = OwnershipGraphConfig(
        showOwnedFrequencyThreshold: 3,
        ubiquitousPresenceRatio: 0.8
    )
}

// MARK: - OwnershipGraph

/// Per-show domain ownership graph. Maps eTLD+1 domains to AdOwnership
/// labels, integrating RSS metadata signals, show-notes frequency analysis,
/// and sponsor entity linking.
///
/// Value type. For shared mutable access across pipeline stages, wrap
/// in an actor or use the OwnershipGraphStore actor below.
struct OwnershipGraph: Sendable, Equatable {

    /// The podcast this graph belongs to.
    let podcastId: String

    /// Domain -> ownership entry mappings, keyed by eTLD+1 domain.
    private(set) var entries: [String: DomainOwnershipEntry] = [:]

    /// Configuration knobs.
    let config: OwnershipGraphConfig

    init(
        podcastId: String,
        config: OwnershipGraphConfig = .default
    ) {
        self.podcastId = podcastId
        self.config = config
    }

    // MARK: - Query

    /// Look up the ownership label for a raw domain/URL string.
    /// Returns nil if the domain is not in the graph.
    func ownership(for rawDomain: String) -> AdOwnership? {
        guard let domain = DomainNormalizer.etld1(from: rawDomain) else { return nil }
        if let entry = entries[domain] {
            return entry.label.toAdOwnership
        }
        return nil
    }

    /// Look up the canonical sponsor ID for a sponsor-owned domain.
    func sponsorId(for rawDomain: String) -> String? {
        guard let domain = DomainNormalizer.etld1(from: rawDomain) else { return nil }
        return entries[domain]?.canonicalSponsorId
    }

    /// All domains classified with a given label.
    func domains(withLabel label: DomainOwnershipLabel) -> [String] {
        entries.values.filter { $0.label == label }.map(\.domain)
    }

    /// All show-owned domains.
    var showOwnedDomains: [String] {
        domains(withLabel: .showOwned)
    }

    /// All sponsor-owned domains.
    var sponsorOwnedDomains: [String] {
        domains(withLabel: .sponsorOwned)
    }

    // MARK: - Ingest: RSS Signals

    /// Ingest the RSS <link> element domain as show-owned.
    mutating func ingestRSSLink(_ url: String) {
        guard let domain = DomainNormalizer.etld1(from: url) else { return }
        setEntry(domain: domain, label: .showOwned, source: .rssLink)
    }

    /// Ingest the RSS feed URL domain. The feed host is typically the
    /// hosting platform or the show itself.
    mutating func ingestFeedURL(_ url: String) {
        guard let domain = DomainNormalizer.etld1(from: url) else { return }
        setEntry(domain: domain, label: .showOwned, source: .feedURL)
    }

    /// Ingest <itunes:owner> email domain as show-owned.
    mutating func ingestITunesOwner(email: String) {
        // Extract domain from email
        guard let atIndex = email.firstIndex(of: "@") else { return }
        let domainPart = String(email[email.index(after: atIndex)...])
        guard let domain = DomainNormalizer.etld1(from: domainPart) else { return }
        setEntry(domain: domain, label: .showOwned, source: .itunesOwner)
    }

    // MARK: - Ingest: Show Notes Domains

    /// Record a domain appearance from show notes. Call once per domain
    /// per episode. When frequency crosses the threshold, the domain
    /// becomes show-owned.
    mutating func recordShowNotesDomain(_ rawDomain: String, episodeCount: Int = 0) {
        guard let domain = DomainNormalizer.etld1(from: rawDomain) else { return }

        let existing = entries[domain]
        let newFrequency = (existing?.frequency ?? 0) + 1

        // If already explicitly classified (RSS, sponsor, override), just bump frequency
        if let existing = existing,
           existing.source != .showNotesFrequency {
            entries[domain] = DomainOwnershipEntry(
                domain: domain,
                label: existing.label,
                source: existing.source,
                frequency: newFrequency,
                canonicalSponsorId: existing.canonicalSponsorId
            )
            return
        }

        // Frequency-based classification
        let label: DomainOwnershipLabel
        if newFrequency >= config.showOwnedFrequencyThreshold {
            label = .showOwned
        } else {
            label = existing?.label ?? .unknown
        }

        entries[domain] = DomainOwnershipEntry(
            domain: domain,
            label: label,
            source: .showNotesFrequency,
            frequency: newFrequency,
            canonicalSponsorId: nil
        )
    }

    // MARK: - Ingest: Sponsor Domains

    /// Register a domain as sponsor-owned, optionally linking to a
    /// canonical sponsor ID from SponsorEntityGraph.
    ///
    /// When a SponsorEntityGraph is available, pass its
    /// `canonicalSponsorId(forDomain:)` result as `canonicalSponsorId`.
    mutating func registerSponsorDomain(
        _ rawDomain: String,
        canonicalSponsorId: String? = nil
    ) {
        guard let domain = DomainNormalizer.etld1(from: rawDomain) else { return }

        let existing = entries[domain]
        // Don't override user overrides
        if existing?.source == .userOverride { return }

        entries[domain] = DomainOwnershipEntry(
            domain: domain,
            label: .sponsorOwned,
            source: .sponsorRegistration,
            frequency: existing?.frequency ?? 0,
            canonicalSponsorId: canonicalSponsorId
        )
    }

    /// Register a domain as network-owned.
    mutating func registerNetworkDomain(_ rawDomain: String) {
        guard let domain = DomainNormalizer.etld1(from: rawDomain) else { return }
        let existing = entries[domain]
        if existing?.source == .userOverride { return }
        setEntry(domain: domain, label: .networkOwned, source: .sponsorRegistration)
    }

    // MARK: - Ingest: From SponsorEntityGraph

    /// Bulk-register sponsor domains from a built SponsorEntityGraph.
    /// For each node in the graph, registers all its domains as sponsorOwned
    /// with the node's canonical sponsor ID.
    mutating func ingestSponsorEntityGraph(_ graph: SponsorEntityGraph) {
        for node in graph.nodes {
            for domain in node.domains {
                registerSponsorDomain(domain, canonicalSponsorId: node.canonicalSponsorId)
            }
        }
    }

    // MARK: - User Override

    /// Apply a user correction to override domain ownership.
    /// User overrides take precedence over all other sources.
    mutating func applyUserOverride(_ rawDomain: String, label: DomainOwnershipLabel) {
        guard let domain = DomainNormalizer.etld1(from: rawDomain) else { return }
        let existing = entries[domain]
        entries[domain] = DomainOwnershipEntry(
            domain: domain,
            label: label,
            source: .userOverride,
            frequency: existing?.frequency ?? 0,
            canonicalSponsorId: existing?.canonicalSponsorId
        )
    }

    // MARK: - Bulk Ingest

    /// Ingest all signals from an RSS feed in one call.
    mutating func ingestRSSFeed(
        feedURL: String?,
        linkURL: String?,
        itunesOwnerEmail: String?
    ) {
        if let url = feedURL { ingestFeedURL(url) }
        if let url = linkURL { ingestRSSLink(url) }
        if let email = itunesOwnerEmail { ingestITunesOwner(email: email) }
    }

    /// Batch-record show-notes domains from a single episode.
    /// Extracts domains from an array of raw URLs found in show notes.
    mutating func ingestShowNotesDomains(_ rawURLs: [String]) {
        // Deduplicate per-episode: only count each domain once per call
        var seen = Set<String>()
        for url in rawURLs {
            guard let domain = DomainNormalizer.etld1(from: url) else { continue }
            guard seen.insert(domain).inserted else { continue }
            recordShowNotesDomain(url)
        }
    }

    // MARK: - Private

    private mutating func setEntry(
        domain: String,
        label: DomainOwnershipLabel,
        source: DomainOwnershipSource
    ) {
        let existing = entries[domain]
        // Don't override user overrides with automatic sources
        if existing?.source == .userOverride { return }
        entries[domain] = DomainOwnershipEntry(
            domain: domain,
            label: label,
            source: source,
            frequency: existing?.frequency ?? 0,
            canonicalSponsorId: existing?.canonicalSponsorId
        )
    }

    // MARK: - Equatable

    static func == (lhs: OwnershipGraph, rhs: OwnershipGraph) -> Bool {
        lhs.podcastId == rhs.podcastId && lhs.entries == rhs.entries
    }
}

// MARK: - DomainOwnershipLabel -> AdOwnership

extension DomainOwnershipLabel {

    /// Map domain ownership label to the existing AdOwnership enum.
    var toAdOwnership: AdOwnership {
        switch self {
        case .showOwned:    return .show
        case .sponsorOwned: return .thirdParty
        case .networkOwned: return .network
        case .unknown:      return .unknown
        }
    }
}
