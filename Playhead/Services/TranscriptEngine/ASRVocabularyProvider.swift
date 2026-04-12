// ASRVocabularyProvider.swift
// Compiles ASR contextual strings from real per-show sources.
//
// The active SpeechAnalyzer path consumes contextualStrings via
// AnalysisContext. The provider keeps the source ordering explicit so the
// highest-value sponsor terms stay ahead of lower-priority lexicon entries
// if Apple applies any internal cap.

import Foundation
import OSLog
#if canImport(Speech)
import Speech
#endif

// MARK: - ASRVocabularySource

enum ASRVocabularySource: Int, Sendable, Comparable {
    case activeSponsorKnowledge = 0
    case podcastSponsorLexicon = 1

    static func < (lhs: ASRVocabularySource, rhs: ASRVocabularySource) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

// MARK: - ASRVocabularyEntry

struct ASRVocabularyEntry: Sendable, Equatable {
    let text: String
    let source: ASRVocabularySource
}

// MARK: - ASRVocabularyProvider

/// Builds ordered ASR vocabulary from the real data stores.
///
/// Source priority:
/// 1. Active sponsor knowledge entries (canonical name + aliases)
/// 2. PodcastProfile.sponsorLexicon
struct ASRVocabularyProvider: Sendable {
    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "ASRVocabularyProvider")

    init(store: AnalysisStore) {
        self.store = store
    }

    /// Loads the current per-show vocabulary and returns an ordered list of
    /// contextual strings.
    func contextualStrings(forPodcastId podcastId: String) async -> [String] {
        let activeSponsorEntries = await loadActiveSponsorEntries(forPodcastId: podcastId)
        let sponsorLexicon = await loadSponsorLexicon(forPodcastId: podcastId)
        let entries = Self.compiledEntries(
            activeSponsorEntries: activeSponsorEntries,
            sponsorLexicon: sponsorLexicon
        )
        return entries.map(\.text)
    }

    /// Pure compiler used by tests and by the runtime loader.
    static func compiledEntries(
        activeSponsorEntries: [SponsorKnowledgeEntry],
        sponsorLexicon: String?
    ) -> [ASRVocabularyEntry] {
        var ordered: [ASRVocabularyEntry] = []
        var seen = Set<String>()

        func append(_ text: String, source: ASRVocabularySource) {
            let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { return }

            let key = trimmed.lowercased()
            guard seen.insert(key).inserted else { return }

            ordered.append(ASRVocabularyEntry(text: trimmed, source: source))
        }

        let sponsorEntries = activeSponsorEntries
            .filter { $0.state == .active && $0.entityType == .sponsor }

        for entry in sponsorEntries {
            append(entry.entityValue, source: .activeSponsorKnowledge)
            for alias in entry.aliases {
                append(alias, source: .activeSponsorKnowledge)
            }
        }

        for term in Self.parseSponsorLexicon(sponsorLexicon) {
            append(term, source: .podcastSponsorLexicon)
        }

        return ordered
    }

    static func parseSponsorLexicon(_ sponsorLexicon: String?) -> [String] {
        guard let sponsorLexicon, !sponsorLexicon.isEmpty else { return [] }

        var seen = Set<String>()
        var parsed: [String] = []
        for rawTerm in sponsorLexicon.split(whereSeparator: { $0 == "," || $0.isNewline }) {
            let trimmed = rawTerm.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { continue }
            let canonical = trimmed

            if !appendIfNew(canonical, into: &parsed, seen: &seen) {
                continue
            }

            // Expand explicit URL-like terms into spoken URL templates that
            // match natural dictation ("example dot com", "example dot com slash
            // show"). This helps ASR biasing catch URLs even when advertisers
            // don't explicitly include the spoken form in sponsor lexicon.
            let urlTemplateTerms = domainSpokenTemplates(forTerm: canonical)
            for term in urlTemplateTerms {
                _ = appendIfNew(term, into: &parsed, seen: &seen)
            }
        }
        return parsed
    }

    private static func appendIfNew(
        _ text: String,
        into output: inout [String],
        seen: inout Set<String>
    ) -> Bool {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return false }

        let key = trimmed.lowercased()
        guard seen.insert(key).inserted else { return false }
        output.append(trimmed)
        return true
    }

    private static func domainSpokenTemplates(forTerm term: String) -> [String] {
        guard let template = parseDomainTemplate(from: term) else { return [] }
        let tld = template.tldParts.joined(separator: " dot ")

        var templates: [String] = []
        for stem in template.stems {
            templates.append("\(stem) dot \(tld)")

            if !template.pathSegments.isEmpty {
                let pathTemplate = template.pathSegments.joined(separator: " slash ")
                templates.append("\(stem) dot \(tld) slash \(pathTemplate)")
            }
        }
        return templates
    }

    private static func parseDomainTemplate(from term: String) -> (stems: [String], tldParts: [String], pathSegments: [String])? {
        guard let url = makeDomainURL(from: term) else { return nil }
        guard var host = url.host else { return nil }

        host = Self.normalizeHost(host)
        if host.isEmpty { return nil }

        let hostParts = host.split(separator: ".").map(String.init)
        guard hostParts.count >= 2 else { return nil }

        let tldStart = Self.tldStartIndex(in: hostParts)
        guard tldStart > 0 && tldStart < hostParts.count else { return nil }

        let domainLabels = Array(hostParts[..<(tldStart)])
        guard !domainLabels.isEmpty else { return nil }

        var stems: [String] = []
        if let primaryStem = domainLabels.last {
            let stemToken = normalizeSpokenToken(primaryStem)
            if !stemToken.isEmpty {
                stems.append(stemToken)
            }
        }

        let longStem = normalizeSpokenToken(domainLabels.joined(separator: " "))
        if !longStem.isEmpty && longStem != stems.first {
            stems.append(longStem)
        }

        let pathSegments = url.path
            .split(separator: "/")
            .map(String.init)
            .compactMap { normalizeSpokenToken($0).isEmpty ? nil : normalizeSpokenToken($0) }

        let tldParts = Array(hostParts[tldStart...].map { String($0) })
        if stems.isEmpty || tldParts.isEmpty { return nil }

        return (stems: stems, tldParts: tldParts, pathSegments: pathSegments)
    }

    private static func makeDomainURL(from term: String) -> URL? {
        let trimmed = term.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.contains(" ") { return nil }

        if let direct = URL(string: trimmed), direct.host != nil {
            return direct
        }

        if trimmed.contains("/") || trimmed.contains(".") {
            return URL(string: "https://\(trimmed)")
        }

        return nil
    }

    private static func tldStartIndex(in hostParts: [String]) -> Int {
        // Common second-level tld patterns that commonly appear in spoken ads.
        // We still return a fallback split for standard single-part TLDs.
        let twoPartKnown = Set([
            "co.uk", "co.jp", "com.au", "com.br", "com.mx", "co.in", "co.nz",
            "co.za", "co.th", "org.uk", "gov.uk", "net.au"
        ])

        if hostParts.count >= 3 {
            let lastTwo = hostParts[hostParts.count - 2] + "." + hostParts[hostParts.count - 1]
            if twoPartKnown.contains(lastTwo) {
                return hostParts.count - 2
            }
        }
        return hostParts.count - 1
    }

    private static func normalizeHost(_ host: String) -> String {
        host.lowercased()
            .trimmingCharacters(in: CharacterSet(charactersIn: "/"))
            .replacingOccurrences(of: "www.", with: "")
    }

    private static func normalizeSpokenToken(_ token: String) -> String {
        String(token.lowercased().compactMap { char in
            if char == "-" || char == "_" {
                return " "
            }
            if char.isLetter || char.isNumber || char == " " {
                return char
            }
            return nil
        })
        .split(whereSeparator: { $0 == " " })
        .filter { !$0.isEmpty }
        .joined(separator: " ")
    }

    // MARK: - Loaders

    private func loadActiveSponsorEntries(forPodcastId podcastId: String) async -> [SponsorKnowledgeEntry] {
        do {
            return try await store.loadKnowledgeEntries(podcastId: podcastId, state: .active)
        } catch {
            logger.warning("ASR vocabulary: failed to load active sponsor entries for podcast \(podcastId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            return []
        }
    }

    private func loadSponsorLexicon(forPodcastId podcastId: String) async -> String? {
        do {
            return try await store.fetchProfile(podcastId: podcastId)?.sponsorLexicon
        } catch {
            logger.warning("ASR vocabulary: failed to load sponsor lexicon for podcast \(podcastId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            return nil
        }
    }
}

#if canImport(Speech)
@available(iOS 26.0, *)
extension ASRVocabularyProvider {
    /// Creates a SpeechAnalyzer analysis context populated with the ordered
    /// contextual strings, or nil when the provider has nothing useful.
    func analysisContext(forPodcastId podcastId: String) async -> AnalysisContext? {
        let contextualStrings = await contextualStrings(forPodcastId: podcastId)
        guard !contextualStrings.isEmpty else { return nil }

        let context = AnalysisContext()
        context.contextualStrings[.general] = contextualStrings
        return context
    }
}
#endif
