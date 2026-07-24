// DAIStitchClassifier.swift
// playhead-xsdz.71 (Signal 1): structural DAI detection via the enclosure
// download's redirect chain.
//
// When an episode's audio enclosure is fetched, the download frequently follows
// a chain of HTTP redirects before landing on the byte stream. That chain names
// the ad-serving / dynamic-ad-insertion (DAI) stitching network the show is wired
// through. THEMOVE, for example, resolves through
// `pscrb.fm → podtrac → megaphone/mgln.ai → claritas/clrtpod → traffic.libsyn`.
// The presence of a KNOWN DAI-stitch host in that chain is a cheap, show-level
// prior that DAI is present for the show ("DAI-EXPECTED").
//
// This classifier is PURE and table-driven: given the ordered redirect-chain hop
// hosts, it returns which known stitch network was identified plus a DAI-EXPECTED
// boolean. An unknown chain (no known stitch host) classifies conservatively as
// `.unknown` / not-expected.
//
// SCOPE: this is Signal 1 ONLY. It OBSERVES and RECORDS — see
// `DAIStitchRecorder` for persistence and `PodcastProfile.daiStitchClassification`
// for the read accessor. It is DELIBERATELY not wired to change any existing
// behavior (no detector/scorer/banner/rediff consumes it yet); that is a
// follow-on bead.
//
// This is a DISTINCT classifier from `NetworkIdentityExtractor` (which derives
// the EDITORIAL network identity from RSS metadata and explicitly SKIPS
// hosting/DAI domains). Editorial identity answers "who produces this show";
// DAI-stitch identity answers "which network stitches this show's ads".

import Foundation

// MARK: - DAIStitchNetwork

/// A known dynamic-ad-insertion / ad-serving stitch (or routing/measurement)
/// network identifiable from an enclosure download's redirect chain. `.unknown`
/// is the conservative default for a chain containing no recognized host.
///
/// Raw values are stable persistence keys (stored on `podcast_profiles`), so
/// renames are migrations — append new cases, do not repurpose existing raws.
enum DAIStitchNetwork: String, Sendable, Equatable, Codable, CaseIterable {
    /// Megaphone (Spotify) — hosts `*.megaphone.fm`, tracker `mgln.ai`.
    case megaphone
    /// AdsWizz — client-pinned dynamic ad fill (`*.adswizz.com`).
    case adswizz
    /// ART19 (Amazon) — `*.art19.com`.
    case art19
    /// Omny Studio (SoundStack) — `*.omny.fm`, `*.omnycontent.com`.
    case omny
    /// Simplecast (SiriusXM) — `*.simplecast.com` / `*.simplecastaudio.com`.
    case simplecast
    /// Podtrac — prefix measurement / redirect (`dts.podtrac.com`).
    case podtrac
    /// Podscribe — attribution prefix (`pscrb.fm`).
    case podscribe
    /// Claritas / Blubrry Podcast targeting (`*.clrtpod.com`, `claritas`).
    case claritas
    /// Libsyn — hosting + dynamic ad enclosures (`*.libsyn.com`).
    case libsyn
    /// No recognized DAI-stitch host in the chain — conservative default.
    case unknown
}

// MARK: - DAIStitchClassification

/// The result of classifying an enclosure download's redirect chain: which
/// stitch network was identified, whether DAI is expected for the show, and the
/// specific hop host that matched (diagnostics).
struct DAIStitchClassification: Sendable, Equatable, Codable {
    /// The highest-priority known stitch network found in the chain, or
    /// `.unknown`.
    let stitchNetwork: DAIStitchNetwork
    /// True when a known DAI-stitch host was present in the chain ⇒ DAI is
    /// expected for the show. False for an unknown/clean chain (conservative).
    let daiExpected: Bool
    /// The first hop host that matched a known pattern (for diagnostics /
    /// legibility of an exported profile). `nil` for an unknown chain.
    let matchedHost: String?

    /// Conservative sentinel: no known stitch host observed.
    static let unknown = DAIStitchClassification(
        stitchNetwork: .unknown,
        daiExpected: false,
        matchedHost: nil
    )
}

// MARK: - DAIStitchClassifier

/// Stateless, table-driven classifier that maps an ordered list of redirect-chain
/// hop hosts to a `DAIStitchClassification`. Uses an enum namespace (no
/// instances), matching the project convention for pure-function utility types
/// (`NetworkIdentityExtractor`, `RediffFetchRequest`).
enum DAIStitchClassifier {

    // MARK: - Host pattern table

    /// Host substring → network, in PRIORITY order. The true ad-STITCH /
    /// insertion networks come first, then routing/measurement redirects, then
    /// hosting — so a chain that threads several known hosts (THEMOVE routes
    /// through podtrac, megaphone, claritas AND libsyn) classifies as the most
    /// DAI-indicative one (megaphone), not the trailing hosting/CDN host.
    ///
    /// Patterns are matched as case-insensitive substrings of each hop host, so
    /// `megaphone.fm` matches `dcs.megaphone.fm` and `traffic.megaphone.fm`
    /// alike. Order within a single network's aliases does not matter (they map
    /// to the same case); order ACROSS networks is the priority.
    private static let hostPatterns: [(pattern: String, network: DAIStitchNetwork)] = [
        // — ad-stitch / insertion networks (highest priority) —
        ("megaphone.fm", .megaphone),
        ("mgln.ai", .megaphone),
        ("adswizz", .adswizz),
        ("art19", .art19),
        ("omny.fm", .omny),
        ("omnycontent", .omny),
        ("simplecast", .simplecast),
        // — routing / measurement redirects —
        ("podtrac", .podtrac),
        ("pscrb.fm", .podscribe),
        ("clrtpod", .claritas),
        ("claritas", .claritas),
        // — hosting (lowest priority; often the final CDN hop) —
        ("libsyn", .libsyn),
    ]

    // MARK: - Public API

    /// Classify an ordered list of redirect-chain hop hosts.
    ///
    /// - Parameter redirectChainHosts: the hosts visited during the enclosure
    ///   download, in order (e.g. `["pscrb.fm", "dts.podtrac.com",
    ///   "mgln.ai", "clrtpod.com", "traffic.libsyn.com"]`). Case and surrounding
    ///   whitespace are normalized; empty entries are ignored.
    /// - Returns: the highest-priority known stitch network with
    ///   `daiExpected == true`, or `.unknown` (conservative) when no known host
    ///   is present or the chain is empty.
    static func classify(redirectChainHosts: [String]) -> DAIStitchClassification {
        let hosts = redirectChainHosts
            .map { $0.lowercased().trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
        guard !hosts.isEmpty else { return .unknown }

        // Iterate the table in priority order; the first pattern that matches any
        // hop host wins, so a chain with several known hosts resolves to the most
        // DAI-indicative network rather than whichever appears first in the chain.
        for entry in hostPatterns {
            if let matched = hosts.first(where: { $0.contains(entry.pattern) }) {
                return DAIStitchClassification(
                    stitchNetwork: entry.network,
                    daiExpected: true,
                    matchedHost: matched
                )
            }
        }
        return .unknown
    }
}
