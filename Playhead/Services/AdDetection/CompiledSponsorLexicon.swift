// CompiledSponsorLexicon.swift
// Phase 8 (playhead-4my.8.2): Fast string matcher compiled from active
// SponsorKnowledgeStore entries. Integrated into LexicalScanner as a
// second sponsor-term source alongside PodcastProfile.sponsorLexicon.
//
// Recompiled when the knowledge store changes. Only active entries
// contribute patterns — blocked, decayed, candidate, and quarantined
// entries are excluded.

import Foundation

// MARK: - CompiledSponsorLexicon

/// A pre-compiled set of regex patterns derived from active sponsor
/// knowledge entries. Each entry's `normalizedValue` and `aliases` are
/// compiled into word-boundary regexes, matching the same pattern style
/// used by `LexicalScanner.compileSponsorLexicon(from:)`.
///
/// Thread-safe: all state is immutable after init.
struct CompiledSponsorLexicon: Sendable {

    /// Compiled word-boundary patterns for active sponsor entities.
    let patterns: [NSRegularExpression]

    /// Patterns sourced from locally confirmed active entries.
    let localPatterns: [NSRegularExpression]

    /// Patterns sourced from verified shared artifacts. Scanner call sites use
    /// these at candidate weight only, so shared-origin facts cannot inherit
    /// local hot-path authority without local confirmation.
    let sharedCandidatePatterns: [NSRegularExpression]

    /// Number of active entries that contributed patterns.
    let entryCount: Int

    /// Number of verified shared entries that contributed candidate patterns.
    /// These entries are shared-origin/quarantined locally: they can help form
    /// candidates, but they are not counted as active hot-path skip entries.
    let sharedEntryCount: Int

    /// Number of entries eligible for active local hot-path behavior.
    let skipEligibleEntryCount: Int

    /// Build a compiled lexicon from a set of knowledge entries.
    /// Only entries with `.active` state are included. Blocked, decayed,
    /// candidate, and quarantined entries are excluded.
    ///
    /// Each active entry contributes its `normalizedValue` plus all
    /// `aliases`, each compiled as `\b<escaped-term>\b` with
    /// case-insensitive matching.
    ///
    /// - Parameter entries: All knowledge entries for a podcast (any state).
    /// - Returns: A compiled lexicon containing patterns for active entries only.
    init(entries: [SponsorKnowledgeEntry]) {
        let activeEntries = Self.activeSkipEligibleEntries(from: entries)
        self.entryCount = activeEntries.count
        self.sharedEntryCount = 0
        self.skipEligibleEntryCount = activeEntries.count
        self.localPatterns = Self.compilePatterns(fromKnowledgeEntries: activeEntries)
        self.sharedCandidatePatterns = []

        self.patterns = localPatterns
    }

    /// Build a compiled lexicon from local knowledge plus a verified shared
    /// artifact. Local active entries stay hot-path eligible. Shared entries
    /// contribute candidate-detection patterns only when consumption is enabled;
    /// local blocked/corrected entries and explicit negative-memory terms
    /// suppress matching shared facts.
    init(
        localEntries: [SponsorKnowledgeEntry],
        sharedArtifact: VerifiedSharedSponsorLexiconArtifact?,
        settings: SharedSponsorLexiconSettings = SharedSponsorLexiconSettings(),
        additionalLocalOverrideTerms: [String] = []
    ) {
        let activeEntries = Self.activeSkipEligibleEntries(from: localEntries)
        let localOverrideTerms = Self.localOverrideTerms(
            from: localEntries,
            additionalTerms: additionalLocalOverrideTerms
        )

        var sharedPlan = SharedMergePlan()
        if settings.consumptionEnabled, let sharedArtifact {
            sharedPlan = Self.sharedMergePlan(
                entries: sharedArtifact.entries,
                localOverrideTerms: localOverrideTerms
            )
        }

        self.entryCount = activeEntries.count
        self.sharedEntryCount = sharedPlan.entryCount
        self.skipEligibleEntryCount = activeEntries.count
        self.localPatterns = Self.compilePatterns(fromKnowledgeEntries: activeEntries)
        self.sharedCandidatePatterns = Self.compilePatterns(fromTerms: sharedPlan.patternTerms)
        self.patterns = localPatterns
    }

    /// Empty lexicon — no patterns, no entries.
    static let empty = CompiledSponsorLexicon(entries: [])

    static func sharedMergeCounts(
        localEntries: [SponsorKnowledgeEntry],
        sharedArtifact: VerifiedSharedSponsorLexiconArtifact?,
        settings: SharedSponsorLexiconSettings = SharedSponsorLexiconSettings(),
        additionalLocalOverrideTerms: [String] = []
    ) -> SharedSponsorLexiconMergeCounts {
        let activeEntries = activeSkipEligibleEntries(from: localEntries)
        guard settings.consumptionEnabled, let sharedArtifact else {
            return SharedSponsorLexiconMergeCounts(
                localActive: activeEntries.count,
                sharedCandidate: 0,
                sharedSuppressedByLocalOverride: 0
            )
        }
        let localOverrideTerms = localOverrideTerms(
            from: localEntries,
            additionalTerms: additionalLocalOverrideTerms
        )
        let sharedPlan = sharedMergePlan(
            entries: sharedArtifact.entries,
            localOverrideTerms: localOverrideTerms
        )
        return SharedSponsorLexiconMergeCounts(
            localActive: activeEntries.count,
            sharedCandidate: sharedPlan.entryCount,
            sharedSuppressedByLocalOverride: sharedPlan.suppressedEntryCount
        )
    }

    private struct SharedMergePlan {
        var entryCount: Int = 0
        var suppressedEntryCount: Int = 0
        var patternTerms: [String] = []
    }

    private struct LocalOverrideTerms {
        var identitySuppressing: Set<String> = []
        var patternSuppressing: Set<String> = []
    }

    private static func compilePatterns(fromKnowledgeEntries entries: [SponsorKnowledgeEntry]) -> [NSRegularExpression] {
        var terms: [String] = []
        for entry in entries {
            terms.append(entry.normalizedValue)
            terms.append(contentsOf: entry.aliases.map {
                $0.lowercased().trimmingCharacters(in: .whitespaces)
            })
        }
        return compilePatterns(fromTerms: terms)
    }

    private static func compilePatterns(fromTerms rawTerms: [String]) -> [NSRegularExpression] {
        var compiled: [NSRegularExpression] = []
        var seen = Set<String>()
        for rawTerm in rawTerms {
            let term = rawTerm.lowercased().trimmingCharacters(in: .whitespaces)
            guard !term.isEmpty, seen.insert(term).inserted else { continue }
            let escaped = NSRegularExpression.escapedPattern(for: term)
            if let regex = try? NSRegularExpression(
                pattern: #"\b"# + escaped + #"\b"#,
                options: [.caseInsensitive]
            ) {
                compiled.append(regex)
            }
        }
        return compiled
    }

    private static func localOverrideTerms(
        from entries: [SponsorKnowledgeEntry],
        additionalTerms: [String] = []
    ) -> LocalOverrideTerms {
        let overrideEntries = entries.filter { entry in
            entry.state == .blocked || isLocallyCorrected(entry)
        }
        var overrides = LocalOverrideTerms()
        for entry in overrideEntries {
            let terms = comparableTerms([entry.normalizedValue] + entry.aliases)
            overrides.patternSuppressing.formUnion(terms)
            if isIdentityOverride(entry) {
                overrides.identitySuppressing.formUnion(terms)
            }
        }
        let additionalComparableTerms = comparableTerms(additionalTerms)
        overrides.patternSuppressing.formUnion(additionalComparableTerms)
        return overrides
    }

    private static func sharedMergePlan(
        entries: [SharedSponsorLexiconEntry],
        localOverrideTerms: LocalOverrideTerms
    ) -> SharedMergePlan {
        var plan = SharedMergePlan()
        for entry in entries {
            if sharedIdentityTerms(entry).contains(where: localOverrideTerms.identitySuppressing.contains) {
                plan.suppressedEntryCount += 1
                continue
            }

            let retainedTerms = entry.publicTerms.filter { term in
                Set(comparableTerms([term])).isDisjoint(with: localOverrideTerms.patternSuppressing)
            }
            guard !retainedTerms.isEmpty else {
                plan.suppressedEntryCount += 1
                continue
            }

            plan.entryCount += 1
            plan.patternTerms.append(contentsOf: retainedTerms.flatMap { term in
                [
                    SharedSponsorLexiconEntry.normalize(term),
                    TranscriptEngineService.normalizeText(term),
                ]
            })
        }
        return plan
    }

    private static func sharedIdentityTerms(_ entry: SharedSponsorLexiconEntry) -> [String] {
        Array(SharedSponsorLexiconTermVariants.identityComparableTerms(
            canonicalName: entry.canonicalName,
            aliases: entry.aliases,
            vanityURLs: entry.vanityURLs
        ))
    }

    private static func activeSkipEligibleEntries(from entries: [SponsorKnowledgeEntry]) -> [SponsorKnowledgeEntry] {
        entries.filter {
            $0.state == .active
                && !isLocallyCorrected($0)
                && (!isSharedOrigin($0) || $0.confirmationCount > 0)
        }
    }

    private static func isLocallyCorrected(_ entry: SponsorKnowledgeEntry) -> Bool {
        entry.metadata?["corrected"] == "true" || entry.metadata?["localOverride"] == "corrected"
    }

    private static func isSharedOrigin(_ entry: SponsorKnowledgeEntry) -> Bool {
        entry.metadata?["origin"] == "shared"
    }

    private static func isIdentityOverride(_ entry: SponsorKnowledgeEntry) -> Bool {
        switch entry.entityType {
        case .sponsor, .url:
            return true
        case .cta, .disclosure:
            return false
        }
    }

    private static func comparableTerms(_ terms: [String]) -> [String] {
        Array(SharedSponsorLexiconTermVariants.comparableTerms(terms))
    }
}
