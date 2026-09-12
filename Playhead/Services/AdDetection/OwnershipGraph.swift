// OwnershipGraph.swift
// Phase ef2.1.2: Domain-level ownership resolution.
// Persistent per-show structure mapping domains -> AdOwnership labels.
//
// Sources:
//   - RSS <link> and <itunes:owner>, MINUS the feed's own host (playhead-e8mg
//     — see `feedHostDomain`; the feed URL is no longer a source at all) AND
//     MINUS free-mail/platform domains the owner address cannot prove
//     (playhead-uv8v — see `freeMailAndPlatformDomains`)
//   - High-frequency show-notes domains (frequency = RECURRENCE, NOT ownership
//     — see `recordShowNotesDomain` and playhead-kmw4)
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
    /// High-frequency domain in show notes across episodes. Provenance only:
    /// since playhead-kmw4 this source can never carry a `.showOwned` label.
    case showNotesFrequency
    /// Explicit sponsor domain registration (from SponsorKnowledgeStore).
    case sponsorRegistration
    /// Manual override (user correction).
    case userOverride
}

// MARK: - OwnershipGraphConfig

/// Tuning knobs for domain ownership classification.
struct OwnershipGraphConfig: Sendable {
    /// Minimum show-notes appearances at which a domain counts as RECURRING
    /// in this show's notes.
    ///
    /// playhead-kmw4: this used to be `showOwnedFrequencyThreshold` and it
    /// PROMOTED a domain to `.showOwned` — "seen in >= N show-note blocks,
    /// therefore the show owns it". It does not follow, and the counterexample
    /// is the common case: a recurring SPONSOR is precisely the thing that
    /// recurs in show notes. Measured on the 2026-08-18 device pull,
    /// `linkedin.com` (69 episodes), `shopify.com` (40) and `ketone.com` (44)
    /// — all three Diary of a CEO sponsors — cleared this bar and were then
    /// injected into the lexical scanner as NEGATIVE evidence, i.e. hearing
    /// the sponsor's own domain in the transcript argued the segment was LESS
    /// likely to be an ad. Frequency cannot separate sponsor from owner at ANY
    /// threshold or window size, so the promotion is gone.
    ///
    /// What the threshold means now: crossing it marks the domain
    /// `ownership-undetermined` (`recurringShowNotesDomains`), which
    /// downstream cue extraction treats as "say nothing in either direction".
    /// The VALUE is unchanged (3) — only the conclusion drawn from it is.
    let showNotesRecurrenceThreshold: Int

    static let `default` = OwnershipGraphConfig(
        showNotesRecurrenceThreshold: 3
    )
}

// MARK: - OwnershipGraph

/// Per-show domain ownership graph. Maps eTLD+1 domains to AdOwnership
/// labels, integrating RSS metadata signals, show-notes frequency analysis,
/// and sponsor entity linking.
///
/// Value type. For shared mutable access across pipeline stages, wrap
/// in an actor (persistence layer is planned for a future ef2.x phase).
struct OwnershipGraph: Sendable, Equatable {

    /// The podcast this graph belongs to.
    let podcastId: String

    /// eTLD+1 of the feed's own URL — the ONE domain no structural signal may
    /// claim for this show (playhead-e8mg). Nil means "unknown", which admits
    /// everything; it is not a licence, it is an absence.
    ///
    /// WHY A FEED HOST IS NEVER EVIDENCE OF OWNERSHIP, measured 2026-08-19
    /// over 918 real podcast feeds sampled from the iTunes search API:
    /// **884 of them (96.3 %) sit on a feed host shared by at least two
    /// different shows in that sample alone**, across just 67 distinct
    /// domains — megaphone.fm carries 235 shows, simplecast.com 96,
    /// omnycontent.com 77, anchor.fm 61. A domain thousands of shows publish
    /// through says nothing about who owns THIS one, and `.showOwned` is
    /// NEGATIVE lexical evidence, so a wrong entry here argues that hearing
    /// the domain makes a segment LESS likely to be an ad.
    ///
    /// It bites the other two routes as well, which is why the exclusion
    /// lives here rather than at the one deleted call site: **175 of the 918
    /// carry a channel `<link>` that resolves to their own feed host**, and
    /// 95 carry an `<itunes:owner>` address there. The Diary Of A CEO is a
    /// live instance — the only `<link>` element anywhere in its channel is
    /// `<image><link>`, holding the feed URL itself.
    let feedHostDomain: String?

    /// Domain -> ownership entry mappings, keyed by eTLD+1 domain.
    private(set) var entries: [String: DomainOwnershipEntry] = [:]

    /// Configuration knobs.
    let config: OwnershipGraphConfig

    init(
        podcastId: String,
        feedHostDomain: String? = nil,
        config: OwnershipGraphConfig = .default
    ) {
        self.podcastId = podcastId
        self.feedHostDomain = feedHostDomain
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

    /// Domains that RECUR in this show's notes and that no structural or
    /// explicit signal has classified — ownership undetermined.
    ///
    /// playhead-kmw4: this is the population that used to be promoted to
    /// `.showOwned` by frequency alone. Recurrence is real (it is measured);
    /// what it does not license is a conclusion about ownership, because a
    /// recurring sponsor and a show-owned domain are indistinguishable by
    /// count. Callers use this to make such a domain contribute NOTHING —
    /// neither the negative "the show owns it" evidence it used to produce,
    /// nor the positive "an external domain in the notes is a sponsor" it
    /// would otherwise fall through to. Both directions need corpus
    /// measurement before either can ship.
    ///
    /// Entries whose source is anything other than `.showNotesFrequency`
    /// (RSS link, iTunes owner, sponsor registration, user override) are
    /// excluded: those carry a real classification and keep it, however often
    /// they also appear in the notes.
    var recurringShowNotesDomains: [String] {
        entries.values
            .filter {
                $0.source == .showNotesFrequency
                    && $0.label == .unknown
                    && $0.frequency >= config.showNotesRecurrenceThreshold
            }
            .map(\.domain)
    }

    // MARK: - Ingest: RSS Signals

    /// Ingest the RSS `<link>` element domain as show-owned.
    ///
    /// Refuses the feed's own host — see `feedHostDomain`.
    mutating func ingestRSSLink(_ url: String) {
        guard let domain = structuralDomain(from: url) else { return }
        setEntry(domain: domain, label: .showOwned, source: .rssLink)
    }

    /// Ingest `<itunes:owner><itunes:email>`'s domain as show-owned.
    ///
    /// Refuses the feed's own host (see `feedHostDomain`) and refuses a
    /// free-mail or platform domain (see `freeMailAndPlatformDomains`) —
    /// neither can be proof that THIS show owns the domain.
    mutating func ingestITunesOwner(email: String) {
        guard let domainPart = Self.domain(ofEmail: email) else { return }
        guard let domain = structuralDomain(from: domainPart) else { return }
        guard !Self.isFreeMailOrPlatformDomain(domain) else { return }
        setEntry(domain: domain, label: .showOwned, source: .itunesOwner)
    }

    /// Normalize a structural signal's raw domain/URL and REFUSE the feed's
    /// own host. One function so both routes are governed by one rule: a
    /// second copy of this guard is a second place for it to go missing.
    private func structuralDomain(from raw: String) -> String? {
        guard let domain = DomainNormalizer.etld1(from: raw) else { return nil }
        guard domain != feedHostDomain else { return nil }
        return domain
    }

    // MARK: - Free-Mail / Platform Owner Domains (playhead-uv8v)

    /// Domains an `<itunes:owner>` address routinely sits at that say
    /// NOTHING about who owns the show.
    ///
    /// HOUSE LENS: `<itunes:owner><itunes:email>`'s domain names one thing —
    /// where the publisher RECEIVES MAIL — and `ingestITunesOwner` reads it as
    /// another — the show's OWN web domain. Those two coincide for a
    /// publisher on a custom domain (`host@myshow.com`), but a free-mail or
    /// platform address is shared by every registrant of that provider, so
    /// the coincidence cannot hold: it is a value that names one thing read
    /// as though it named another.
    ///
    /// MEASURED 2026-08-19 over 918 real podcast feeds (playhead-uv8v, filed
    /// against playhead-e8mg): 807 carried an owner address, and 127 of them
    /// (15.7 %) resolved to `gmail.com` alone — a domain no single show can
    /// own. `.showOwned` is NEGATIVE lexical evidence (see
    /// `MetadataLexiconInjector`, which compiles both the written and the
    /// SPOKEN form — `gmail.com` becomes `\bgmail com\b`), so admitting it
    /// here means a host reading "email us at theshow at gmail dot com"
    /// suppresses ad detection for that span, for gmail.com and for every
    /// other show that also happens to mail from it.
    ///
    /// A CLOSED set, not a heuristic: each entry hosts millions of unrelated
    /// mailboxes or thousands of unrelated shows, so it can never single out
    /// ONE show. Extend it by adding a domain here — never by loosening the
    /// match to a substring, prefix, or TLD pattern, which risks excluding a
    /// legitimate custom domain that merely resembles one of these.
    ///
    /// Two families:
    ///   - Free/consumer webmail: the address is the PUBLISHER's personal
    ///     mailbox, not the show's site.
    ///   - Podcast hosting platforms: `feedHostDomain` (playhead-e8mg) already
    ///     refuses the platform a show is ITSELF hosted on, but an owner
    ///     address at a DIFFERENT platform's domain (e.g. a megaphone.fm show
    ///     whose owner address is `@acast.com`) slips past that guard — this
    ///     list catches it too. Platform entries here are deliberately the
    ///     same names `MetadataShowOwnedDomainDevicePullEvalTests`/e8mg's
    ///     survey already treats as hosting platforms, not sponsors.
    private static let freeMailAndPlatformDomains: Set<String> = [
        // Free / consumer webmail
        "gmail.com", "googlemail.com",
        "yahoo.com",
        "hotmail.com", "outlook.com",
        "icloud.com", "me.com", "mac.com",
        "aol.com",
        "proton.me", "protonmail.com",
        "gmx.com", "gmx.net", "gmx.de", "gmx.at",
        // Podcast hosting platforms (see comment above)
        "acast.com", "anchor.fm", "spotify.com", "megaphone.fm",
        "simplecast.com", "libsyn.com", "buzzsprout.com", "podbean.com",
        "transistor.fm",
    ]

    /// True if `domain` (already normalized to eTLD+1) is a free-mail or
    /// platform domain an `<itunes:owner>` address cannot prove ownership of.
    private static func isFreeMailOrPlatformDomain(_ domain: String) -> Bool {
        freeMailAndPlatformDomains.contains(domain)
    }

    /// Filter a candidate owner domain through the free-mail/platform
    /// denylist, collapsing a disqualified domain to nil. Shared by
    /// `ingestITunesOwner` (via `isFreeMailOrPlatformDomain` directly) and
    /// `ingestRSSFeed`'s `<link>` agreement check below — a free-mail address
    /// must not be usable EITHER to claim its own domain as show-owned OR to
    /// veto a `<link>` domain it disagrees with, since it names no party the
    /// `<link>` could disagree with in the first place.
    private static func filteredOwnerDomain(_ domain: String?) -> String? {
        guard let domain, !isFreeMailOrPlatformDomain(domain) else { return nil }
        return domain
    }

    // MARK: - Ingest: Show Notes Domains

    /// Record a domain appearance from show notes. Call once per domain
    /// per episode.
    ///
    /// playhead-kmw4: frequency NEVER classifies. A domain whose only signal
    /// is "it appears in a lot of this show's notes" stays `.unknown` at every
    /// count; crossing `showNotesRecurrenceThreshold` puts it in
    /// `recurringShowNotesDomains` and nothing else. An entry that already
    /// carries a real classification (RSS link, iTunes owner, sponsor
    /// registration, user override) keeps its label and just accrues the count.
    mutating func recordShowNotesDomain(_ rawDomain: String, episodeCount: Int = 0) {
        guard let domain = DomainNormalizer.etld1(from: rawDomain) else { return }

        let existing = entries[domain]
        let newFrequency = (existing?.frequency ?? 0) + 1

        // If already explicitly classified (RSS, sponsor, override), just bump frequency.
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

        // Frequency records, it does not classify (playhead-kmw4). Every
        // entry reaching this point either does not exist yet or already has
        // source `.showNotesFrequency`, and such an entry can only ever be
        // `.unknown` — the branch above returns for every other source. So
        // `.unknown` is a statement of what is known, not a fallback.
        entries[domain] = DomainOwnershipEntry(
            domain: domain,
            label: .unknown,
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

    /// Ingest every structural signal an RSS feed carries, in one call.
    ///
    /// **This is the entry point production uses, and the two `ingest…`
    /// methods above are its primitives.** It is not a convenience wrapper:
    /// it is the only place that holds both declarations at once, which is
    /// what the precedence rule below needs. Calling the primitives directly
    /// bypasses that rule — `MetadataOwnershipWiringSourceCanaryTests` is what
    /// stops production drifting back to doing so.
    ///
    /// The feed URL is deliberately NOT a parameter (playhead-e8mg). It used
    /// to be, and it promoted the hosting platform to `.showOwned` on 96 % of
    /// real feeds; what it is good for now is `feedHostDomain`, i.e. saying
    /// which domain the other two routes may not claim.
    ///
    /// ## THE `<link>` IS REFUSED WHEN `<itunes:owner>` NAMES A DIFFERENT PARTY
    ///
    /// Two structural declarations that name different registrable domains
    /// cannot both be the show. When they disagree, the owner address wins and
    /// the `<link>` is dropped — not relabelled, DROPPED, so the domain falls
    /// through to whatever the show-notes population already says about it
    /// (usually `ownershipUndetermined`, i.e. "say nothing in either
    /// direction" — playhead-kmw4's disposition for exactly this situation).
    ///
    /// Which one wins is not a coin toss. `<itunes:owner>` is the address
    /// Apple uses to verify who is entitled to the feed; `<link>` is "the URL
    /// of the website corresponding to the channel", which for a distributed
    /// show is the distributor's site. Measured over 918 real feeds
    /// (2026-08-19, `scripts/e8mg-feed-structure-survey.py`): 776 carry both
    /// and **they disagree on 511 of them (65.9 %)**, and the `<link>`
    /// domains that recur across shows are networks
    /// and platforms — iheart.com (41 shows), art19.com (38), spotify.com
    /// (23), siriusxm.com (22), wondery.com (17), libsyn.com (17),
    /// acast.com (14), simplecast.com (14).
    ///
    /// Measured on the device pull rather than argued (2026-08-18-t3, 15
    /// analysed assets / 154 decoded spans): Conan's `<link>` is
    /// `https://www.siriusxm.com` while its owner address is
    /// `conaf@teamcoco.com`. Admitting the `<link>` puts `siriusxm.com` —
    /// which appears in **724 of 724** episode descriptions, in the line
    /// "Subscribe to SiriusXM Podcasts+ … visiting siriusxm.com/podcastsplus",
    /// i.e. a paid-subscription READ — into the scanner as NEGATIVE evidence.
    /// That is a HARD suppressor of `.lexicalAutoAd`: it takes the spans
    /// carrying a negative hit from **1 of 154 to 18 of 154, and all 18 carry
    /// a promotion hit**. Seventeen of those eighteen are bought by the
    /// `<link>` alone, in exchange for one domain that is not the show's.
    ///
    /// When there is no owner address the `<link>` is admitted: it is then the
    /// only structural declaration and nothing contradicts it — **83 of the
    /// 918 are in that position**, and 26 of those 83 name the feed's own
    /// host, which the exclusion refuses anyway.
    ///
    /// A free-mail or platform owner address (playhead-uv8v) is filtered out
    /// of this agreement check entirely — it is treated as "no owner
    /// address" for BOTH purposes. It cannot become `.showOwned` (see
    /// `ingestITunesOwner`), and it must not be allowed to veto a legitimate
    /// `<link>` either: `gmail.com` disagreeing with `theshow.com` is not two
    /// parties in conflict, it is one party's mailbox provider next to their
    /// actual site.
    mutating func ingestRSSFeed(
        linkURL: String?,
        itunesOwnerEmail: String?
    ) {
        let ownerDomain = Self.filteredOwnerDomain(
            itunesOwnerEmail.flatMap(Self.domain(ofEmail:))
                .flatMap { DomainNormalizer.etld1(from: $0) }
        )
        if let email = itunesOwnerEmail { ingestITunesOwner(email: email) }

        guard let url = linkURL else { return }
        if let ownerDomain, DomainNormalizer.etld1(from: url) != ownerDomain {
            return
        }
        ingestRSSLink(url)
    }

    /// The domain half of an email address, or nil if there is no `@`.
    private static func domain(ofEmail email: String) -> String? {
        guard let atIndex = email.firstIndex(of: "@") else { return nil }
        return String(email[email.index(after: atIndex)...])
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

    /// Config is intentionally excluded: it is a construction-time tuning knob, not graph state.
    /// Two graphs with the same podcastId and entries represent the same ownership knowledge.
    ///
    /// `feedHostDomain` is excluded for the same reason and one more: it is a
    /// FILTER on what may enter, so two graphs that admitted the same entries
    /// hold the same knowledge however they were filtered.
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
