// CompiledSponsorLexiconTests.swift
// Phase 8 (playhead-4my.8.2): Tests for CompiledSponsorLexicon compilation
// and LexicalScanner integration.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeEntry(
    entityType: KnowledgeEntityType = .sponsor,
    entityValue: String,
    state: KnowledgeState = .active,
    aliases: [String] = []
) -> SponsorKnowledgeEntry {
    SponsorKnowledgeEntry(
        podcastId: "pod-test",
        entityType: entityType,
        entityValue: entityValue,
        state: state,
        aliases: aliases
    )
}

private func makeChunk(
    text: String,
    normalizedText: String? = nil,
    startTime: Double = 0.0,
    endTime: Double = 10.0,
    weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil
) -> TranscriptChunk {
    TranscriptChunk(
        id: UUID().uuidString,
        analysisAssetId: "asset-test",
        segmentFingerprint: UUID().uuidString,
        chunkIndex: 0,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: normalizedText ?? text.lowercased(),
        pass: "final",
        modelVersion: "test",
        transcriptVersion: nil,
        atomOrdinal: nil,
        weakAnchorMetadata: weakAnchorMetadata
    )
}

// MARK: - CompiledSponsorLexicon Construction

@Suite("CompiledSponsorLexicon — Construction")
struct CompiledSponsorLexiconConstructionTests {

    @Test("Contains patterns from active entries")
    func activeEntriesIncluded() {
        let entries = [
            makeEntry(entityValue: "Squarespace"),
            makeEntry(entityValue: "BetterHelp"),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        #expect(lexicon.entryCount == 2)
        #expect(!lexicon.patterns.isEmpty)
    }

    @Test("Excludes blocked entries")
    func blockedExcluded() {
        let entries = [
            makeEntry(entityValue: "Squarespace", state: .active),
            makeEntry(entityValue: "BlockedSponsor", state: .blocked),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        #expect(lexicon.entryCount == 1)
    }

    @Test("Excludes decayed entries")
    func decayedExcluded() {
        let entries = [
            makeEntry(entityValue: "Squarespace", state: .active),
            makeEntry(entityValue: "DecayedSponsor", state: .decayed),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        #expect(lexicon.entryCount == 1)
    }

    @Test("Excludes candidate and quarantined entries")
    func candidateAndQuarantinedExcluded() {
        let entries = [
            makeEntry(entityValue: "Candidate", state: .candidate),
            makeEntry(entityValue: "Quarantined", state: .quarantined),
            makeEntry(entityValue: "Active", state: .active),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        #expect(lexicon.entryCount == 1)
    }

    @Test("Includes aliases as additional patterns")
    func aliasesCompiled() {
        let entries = [
            makeEntry(entityValue: "Squarespace", aliases: ["square space", "SquareSpace Pro"]),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        #expect(lexicon.entryCount == 1)
        // normalizedValue "squarespace" + "square space" + "squarespace pro" = 3 patterns
        #expect(lexicon.patterns.count == 3)
    }

    @Test("Includes sponsor, cta, url, and disclosure entity types")
    func allEntityTypes() {
        let entries = [
            makeEntry(entityType: .sponsor, entityValue: "Squarespace"),
            makeEntry(entityType: .cta, entityValue: "use code podcast"),
            makeEntry(entityType: .url, entityValue: "squarespace.com/podcast"),
            makeEntry(entityType: .disclosure, entityValue: "brought to you by"),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        #expect(lexicon.entryCount == 4)
        #expect(lexicon.patterns.count == 4)
    }

    @Test("Empty entries produce empty lexicon")
    func emptyEntries() {
        let lexicon = CompiledSponsorLexicon(entries: [])
        #expect(lexicon.entryCount == 0)
        #expect(lexicon.patterns.isEmpty)
    }

    @Test("Static empty lexicon has no patterns")
    func staticEmpty() {
        #expect(CompiledSponsorLexicon.empty.entryCount == 0)
        #expect(CompiledSponsorLexicon.empty.patterns.isEmpty)
    }

    @Test("Duplicate alias is deduplicated")
    func duplicateAlias() {
        // normalizedValue is "squarespace", alias "Squarespace" normalizes to same
        let entries = [
            makeEntry(entityValue: "Squarespace", aliases: ["Squarespace"]),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        // Should be 1 pattern (deduped), not 2
        #expect(lexicon.patterns.count == 1)
    }
}

// MARK: - LexicalScanner Integration

@Suite("CompiledSponsorLexicon — LexicalScanner Integration")
struct CompiledSponsorLexiconScannerTests {

    @Test("Scanner produces hits from compiled lexicon")
    func scannerFindsCompiledLexiconHits() {
        let entries = [
            makeEntry(entityValue: "Squarespace"),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        let chunk = makeChunk(text: "this episode is brought to you by Squarespace")
        let hits = scanner.scanChunk(chunk)

        let sponsorHits = hits.filter { $0.category == .sponsor }
        let squarespaceHits = sponsorHits.filter {
            $0.matchedText.lowercased().contains("squarespace")
        }
        #expect(!squarespaceHits.isEmpty, "Should find Squarespace via compiled lexicon")
    }

    @Test("Compiled lexicon hits have weight 1.5")
    func compiledLexiconHitsWeight() {
        let entries = [
            makeEntry(entityValue: "BetterHelp"),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        // Use text that won't trigger built-in patterns — only the
        // compiled lexicon should match "betterhelp" here.
        let chunk = makeChunk(text: "and BetterHelp is great for therapy")
        let hits = scanner.scanChunk(chunk)

        let betterHelpHits = hits.filter {
            $0.matchedText.lowercased().contains("betterhelp") && $0.category == .sponsor
        }
        #expect(!betterHelpHits.isEmpty)
        for hit in betterHelpHits {
            #expect(hit.weight == 1.5, "Compiled lexicon hits must use boosted weight 1.5")
        }
    }

    @Test("Compiled lexicon coexists with showSponsorPatterns")
    func coexistsWithShowSponsorPatterns() {
        let entries = [
            makeEntry(entityValue: "BetterHelp"),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)

        // Create a profile with a different sponsor in the lexicon
        let profile = PodcastProfile(
            podcastId: "pod-test",
            sponsorLexicon: "Squarespace",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 1.0,
            observationCount: 1,
            mode: "active",
            recentFalseSkipSignals: 0
        )
        let scanner = LexicalScanner(
            podcastProfile: profile,
            compiledLexicon: lexicon
        )

        let chunk = makeChunk(
            text: "Squarespace and BetterHelp sponsor this show"
        )
        let hits = scanner.scanChunk(chunk)

        let sponsorHits = hits.filter { $0.category == .sponsor }
        let squarespaceHits = sponsorHits.filter {
            $0.matchedText.lowercased().contains("squarespace")
        }
        let betterHelpHits = sponsorHits.filter {
            $0.matchedText.lowercased().contains("betterhelp")
        }

        #expect(!squarespaceHits.isEmpty, "showSponsorPatterns should still produce hits")
        #expect(!betterHelpHits.isEmpty, "compiledLexicon should also produce hits")

        // Both should have weight 1.5
        for hit in squarespaceHits {
            #expect(hit.weight == 1.5)
        }
        for hit in betterHelpHits {
            #expect(hit.weight == 1.5)
        }
    }

    @Test("Scanner works without compiled lexicon (nil)")
    func nilCompiledLexicon() {
        let scanner = LexicalScanner()
        let chunk = makeChunk(text: "this episode is sponsored by someone")
        let hits = scanner.scanChunk(chunk)
        // Should still work via built-in patterns
        let sponsorHits = hits.filter { $0.category == .sponsor }
        #expect(!sponsorHits.isEmpty, "Built-in patterns should still match")
    }

    @Test("Scanner with empty compiled lexicon produces no extra hits")
    func emptyCompiledLexicon() {
        let lexicon = CompiledSponsorLexicon.empty
        let scanner = LexicalScanner(compiledLexicon: lexicon)
        let chunk = makeChunk(text: "hello world no sponsors here")
        let hits = scanner.scanChunk(chunk)
        let sponsorHits = hits.filter { $0.category == .sponsor }
        #expect(sponsorHits.isEmpty)
    }

    @Test("Compiled lexicon hits use .sponsor category")
    func compiledLexiconCategory() {
        let entries = [
            makeEntry(entityType: .url, entityValue: "example.com/offer"),
        ]
        let lexicon = CompiledSponsorLexicon(entries: entries)
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        let chunk = makeChunk(text: "go to example.com/offer now")
        let hits = scanner.scanChunk(chunk)

        // The compiled lexicon emits as .sponsor category regardless of
        // the knowledge entry's entityType
        let lexiconHits = hits.filter {
            $0.matchedText.lowercased().contains("example.com/offer")
        }
        for hit in lexiconHits {
            #expect(hit.category == .sponsor)
            #expect(hit.weight == 1.5)
        }
    }
}

@Suite("LexicalScanner — weak-anchor rescans")
struct LexicalScannerWeakAnchorRescanTests {

    @Test("rescanAlternatives scans only weak-anchor sources inside the requested window")
    func rescansScopedWeakAnchorSources() {
        let scanner = LexicalScanner()
        let nearChunk = makeChunk(
            text: "visit bitterhelp for details",
            startTime: 100,
            endTime: 101,
            weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                averageConfidence: 0.48,
                minimumConfidence: 0.31,
                alternativeTexts: [
                    "visit betterhelp.com/podcast for details",
                ],
                lowConfidencePhrases: [
                    .init(
                        text: "use code save10 at checkout",
                        startTime: 100.2,
                        endTime: 100.8,
                        confidence: 0.31
                    ),
                ]
            )
        )
        let farChunk = makeChunk(
            text: "visit squarespce for templates",
            startTime: 300,
            endTime: 301,
            weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                averageConfidence: 0.44,
                minimumConfidence: 0.29,
                alternativeTexts: ["visit squarespace.com/templates"],
                lowConfidencePhrases: []
            )
        )

        let hits = scanner.rescanAlternatives(
            chunks: [nearChunk, farChunk],
            nearTime: 100.5,
            radius: 2
        )

        #expect(hits.contains {
            $0.category == .urlCTA && $0.matchedText.lowercased().contains("betterhelp.com")
        })
        #expect(hits.contains {
            $0.category == .promoCode && $0.matchedText == "use code save10"
        })
        #expect(!hits.contains {
            $0.matchedText.lowercased().contains("squarespace.com")
        }, "far-away alternatives must not be globally rescanned")
    }
}
