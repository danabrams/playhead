// MetadataActivationTests.swift
// ef2.4.7: Tests for MetadataActivationConfig, MetadataLexiconInjector,
// MetadataPriorShift, and MetadataSeededRegion (BackfillJobPhase).

import Foundation
import Testing
@testable import Playhead

// MARK: - MetadataActivationConfig

@Suite("MetadataActivationConfig — Gating")
struct MetadataActivationConfigTests {

    @Test("Default config activates lexical injection only")
    func defaultActivatesLexicalOnly() {
        let config = MetadataActivationConfig.default
        #expect(config.isLexicalInjectionActive)
        #expect(!config.isClassifierPriorShiftActive)
        #expect(!config.isFMSchedulingActive)
    }

    @Test("allEnabled config has all consumption points active")
    func allEnabled() {
        let config = MetadataActivationConfig.allEnabled
        #expect(config.isLexicalInjectionActive)
        #expect(config.isClassifierPriorShiftActive)
        #expect(config.isFMSchedulingActive)
    }

    @Test("Counterfactual gate blocks all consumption points")
    func counterfactualGateBlocks() {
        let config = MetadataActivationConfig(
            lexicalInjectionEnabled: true,
            lexicalInjectionMinTrust: 0.0,
            lexicalInjectionDiscount: 0.75,
            classifierPriorShiftEnabled: true,
            classifierPriorShiftMinTrust: 0.08,
            classifierShiftedMidpoint: MetadataActivationConfig.default.classifierShiftedMidpoint,
            classifierBaselineMidpoint: 0.37,
            fmSchedulingEnabled: true,
            fmSchedulingMinTrust: 0.0,
            counterfactualGateOpen: false  // gate closed
        )
        #expect(!config.isLexicalInjectionActive)
        #expect(!config.isClassifierPriorShiftActive)
        #expect(!config.isFMSchedulingActive)
    }

    @Test("Individual flags can be toggled independently")
    func independentFlags() {
        let lexicalOnly = MetadataActivationConfig(
            lexicalInjectionEnabled: true,
            lexicalInjectionMinTrust: 0.0,
            lexicalInjectionDiscount: 0.75,
            classifierPriorShiftEnabled: false,
            classifierPriorShiftMinTrust: 0.08,
            classifierShiftedMidpoint: MetadataActivationConfig.default.classifierShiftedMidpoint,
            classifierBaselineMidpoint: 0.37,
            fmSchedulingEnabled: false,
            fmSchedulingMinTrust: 0.0,
            counterfactualGateOpen: true
        )
        #expect(lexicalOnly.isLexicalInjectionActive)
        #expect(!lexicalOnly.isClassifierPriorShiftActive)
        #expect(!lexicalOnly.isFMSchedulingActive)
    }

    @Test("Default discount is 0.75")
    func discountValue() {
        let config = MetadataActivationConfig.default
        #expect(config.lexicalInjectionDiscount == 0.75)
    }

    @Test("Default classifier prior shift min trust is 0.08")
    func priorShiftMinTrust() {
        let config = MetadataActivationConfig.default
        #expect(config.classifierPriorShiftMinTrust == 0.08)
    }

    @Test("Default shifted midpoint is 0.345 (playhead-gtt9.3 option a retune)")
    func shiftedMidpoint() {
        let config = MetadataActivationConfig.default
        #expect(config.classifierShiftedMidpoint == 0.345)
    }

    @Test("Default baseline midpoint is 0.37 (playhead-gtt9.3 retune)")
    func baselineMidpoint() {
        let config = MetadataActivationConfig.default
        #expect(config.classifierBaselineMidpoint == 0.37)
    }

    // MARK: - playhead-sqhj / playhead-narl activation default pins
    //
    // The 2026-04-24 spike under gtt9.4 documented that
    // `counterfactualGateOpen=false` was a master kill on every NARL
    // metadata-activation knob — every per-gate flag (`lexicalInjectionEnabled`,
    // `classifierPriorShiftEnabled`, `fmSchedulingEnabled`) goes through
    // `(gateOpen && enabled)` in the corresponding `is*Active` computed
    // property, so closing the master made all the per-gate tuning
    // inert regardless of how it was set.
    //
    // The fix in playhead-sqhj flipped the master open by default so
    // downstream gate-tuning beads could flip a single per-gate flag and
    // immediately see effects. playhead-narl consumes that path for the
    // first graduated production activation: lexical injection only.
    //
    // These pins lock that posture in place. Future changes to the default
    // must edit them deliberately so activation does not drift back to a
    // master-killed state or accidentally enable the gates that the corpus
    // did not justify.

    @Test("Default master gate is OPEN (playhead-sqhj/playhead-narl)")
    func defaultMasterGateOpen() {
        #expect(MetadataActivationConfig.default.counterfactualGateOpen == true,
                "Master `counterfactualGateOpen` must default to true so per-gate tuning can take effect; flipping this back to false silently kills every NARL activation knob.")
    }

    @Test("Default per-gate flags enable only lexical injection (playhead-narl)")
    func defaultPerGateFlagsEnableOnlyLexicalInjection() {
        let config = MetadataActivationConfig.default
        #expect(config.lexicalInjectionEnabled == true,
                "lexicalInjectionEnabled is the only per-gate default enabled by the playhead-narl corpus decision")
        #expect(config.classifierPriorShiftEnabled == false,
                "classifierPriorShiftEnabled must stay off until a dedicated corpus decision proves no FP regression")
        #expect(config.fmSchedulingEnabled == false,
                "fmSchedulingEnabled must stay off until shadow coverage justifies production scheduling")
        #expect(config.isLexicalInjectionActive)
        #expect(!config.isClassifierPriorShiftActive)
        #expect(!config.isFMSchedulingActive)
    }

    @Test("Counterfactual replay baseline keeps all metadata gates off")
    func counterfactualBaselineKeepsAllMetadataGatesOff() {
        let config = MetadataActivationConfig.counterfactualBaseline
        #expect(config.counterfactualGateOpen == true,
                "Replay baseline keeps the master open so per-gate deltas stay attributable")
        #expect(config.lexicalInjectionEnabled == false)
        #expect(config.classifierPriorShiftEnabled == false)
        #expect(config.fmSchedulingEnabled == false)
        #expect(!config.isLexicalInjectionActive)
        #expect(!config.isClassifierPriorShiftActive)
        #expect(!config.isFMSchedulingActive)
    }

    @Test("Backfill lexical injection and fusion share one prior snapshot")
    func backfillLexicalInjectionAndFusionShareOnePriorSnapshot() throws {
        let source = try String(
            contentsOf: URL(fileURLWithPath: #filePath)
                .deletingLastPathComponent()
                .deletingLastPathComponent()
                .deletingLastPathComponent()
                .deletingLastPathComponent()
                .appendingPathComponent("Playhead/Services/AdDetection/AdDetectionService.swift"),
            encoding: .utf8
        )
        let resolveNeedle = "let resolvedEpisodePriors = await resolveEpisodePriors()"
        let injectionNeedle = "let metadataLexiconEntries = metadataLexiconEntries("
        guard let resolveRange = source.range(of: resolveNeedle),
              let injectionRange = source.range(of: injectionNeedle) else {
            Issue.record("Expected runBackfill prior-snapshot and lexical-injection call sites")
            return
        }

        #expect(resolveRange.lowerBound < injectionRange.lowerBound,
                "runBackfill must snapshot priors before lexical injection so metadata trust cannot drift before fusion")
        #expect(source.range(of: resolveNeedle, range: resolveRange.upperBound..<source.endIndex) == nil,
                "runBackfill should not resolve episode priors a second time after lexical injection")
    }

    @Test("Hot path metadata lexicon skips prior resolution when cues are empty")
    func hotPathMetadataLexiconSkipsPriorResolutionWhenCuesAreEmpty() throws {
        let source = try String(
            contentsOf: URL(fileURLWithPath: #filePath)
                .deletingLastPathComponent()
                .deletingLastPathComponent()
                .deletingLastPathComponent()
                .deletingLastPathComponent()
                .appendingPathComponent("Playhead/Services/AdDetection/AdDetectionService.swift"),
            encoding: .utf8
        )
        let asyncHelperNeedle = """
            private func metadataLexiconEntries(
                from cues: [EpisodeMetadataCue]
            ) async -> [MetadataLexiconEntry] {
        """
        let syncHelperNeedle = """
            private func metadataLexiconEntries(
                from cues: [EpisodeMetadataCue],
        """
        guard let helperStart = source.range(of: asyncHelperNeedle)?.upperBound,
              let helperEnd = source.range(of: syncHelperNeedle, range: helperStart..<source.endIndex)?.lowerBound else {
            Issue.record("Expected async and sync metadataLexiconEntries helpers")
            return
        }

        let helperBody = source[helperStart..<helperEnd]
        guard let emptyGuardRange = helperBody.range(of: "guard !cues.isEmpty else { return [] }"),
              let resolveRange = helperBody.range(of: "let priors = await resolveEpisodePriors()") else {
            Issue.record("Expected empty-cue guard and prior-resolution call in async metadataLexiconEntries helper")
            return
        }

        #expect(emptyGuardRange.lowerBound < resolveRange.lowerBound,
                "hot-path metadata lexicon injection must return before resolving priors when there are no cues")
    }

    @Test("Ownership snapshot includes recurring show-notes domains")
    func ownershipSnapshotIncludesRecurringShowNotesDomains() throws {
        let recentMetadata = [
            FeedDescriptionMetadata(
                feedDescription: "More at https://mypodcast.com/one.",
                feedSummary: nil,
                sourceHashes: .init(descriptionHash: 1, summaryHash: nil)
            ),
            FeedDescriptionMetadata(
                feedDescription: "Extras at https://mypodcast.com/two.",
                feedSummary: nil,
                sourceHashes: .init(descriptionHash: 2, summaryHash: nil)
            ),
            FeedDescriptionMetadata(
                feedDescription: "Links at https://mypodcast.com/three.",
                feedSummary: nil,
                sourceHashes: .init(descriptionHash: 3, summaryHash: nil)
            ),
            FeedDescriptionMetadata(
                feedDescription: "Sponsor offer at https://betterhelp.com/play.",
                feedSummary: nil,
                sourceHashes: .init(descriptionHash: 4, summaryHash: nil)
            ),
        ]

        let domains = EpisodeMetadataSnapshot.showOwnedDomains(
            feedURL: try #require(URL(string: "https://feeds.example.com/rss")),
            recentMetadata: recentMetadata,
            podcastId: "podcast-ownership-test"
        )

        #expect(domains.contains("mypodcast.com"))
        #expect(domains.contains("example.com"))
        #expect(!domains.contains("betterhelp.com"))
    }
}

// MARK: - MetadataLexiconInjector

@Suite("MetadataLexiconInjector — Injection")
struct MetadataLexiconInjectorTests {

    static let enabledConfig = MetadataActivationConfig.allEnabled

    @Test("Empty cues produce no entries")
    func emptyCues() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let entries = injector.inject(cues: [], metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("Disabled config produces no entries even with valid cues")
    func disabledConfig() {
        let injector = MetadataLexiconInjector(config: .counterfactualBaseline)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("Default config injects lexical metadata entries (playhead-narl)")
    func defaultConfigInjectsEntries() {
        let injector = MetadataLexiconInjector(config: .default)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]

        let entries = injector.inject(cues: cues, metadataTrust: 0.5)

        #expect(entries.count == 1)
        #expect(entries[0].isMetadataOrigin)
        #expect(!entries[0].isNegativePattern)
    }

    @Test("External domain cue produces URL CTA entry with correct weight")
    func externalDomainWeight() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let metadataTrust: Float = 0.4
        let entries = injector.inject(cues: cues, metadataTrust: metadataTrust)
        #expect(entries.count == 1)

        let entry = entries[0]
        #expect(entry.category == .urlCTA)
        #expect(!entry.isNegativePattern)
        #expect(entry.isMetadataOrigin)
        // Weight = baseCategoryWeight(urlCTA=0.8) * metadataTrust(0.4) * 0.75
        let expectedWeight = 0.8 * 0.4 * 0.75
        #expect(abs(entry.weight - expectedWeight) < 0.001)
    }

    @Test("Sponsor alias cue produces sponsor entry with correct weight")
    func sponsorAliasWeight() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "squarespace",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let metadataTrust: Float = 0.6
        let entries = injector.inject(cues: cues, metadataTrust: metadataTrust)
        #expect(entries.count == 1)

        let entry = entries[0]
        #expect(entry.category == .sponsor)
        #expect(!entry.isNegativePattern)
        #expect(entry.isMetadataOrigin)
        // Weight = baseCategoryWeight(sponsor=1.0) * metadataTrust(0.6) * 0.75
        let expectedWeight = 1.0 * 0.6 * 0.75
        #expect(abs(entry.weight - expectedWeight) < 0.001)
    }

    @Test("Show-owned domain produces negative pattern")
    func showOwnedDomainNegative() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "teamcoco.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.count == 1)

        let entry = entries[0]
        #expect(entry.isNegativePattern)
        #expect(entry.isMetadataOrigin)
        #expect(entry.weight < 0.0, "Negative patterns should have negative weight")
    }

    @Test("Disclosure cues are skipped (covered by built-in patterns)")
    func disclosureSkipped() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .disclosure,
                normalizedValue: "brought to you by acme",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("PromoCode cues are skipped (covered by built-in patterns)")
    func promoCodeSkipped() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .promoCode,
                normalizedValue: "SAVE20",
                sourceField: .description,
                confidence: 0.9,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("NetworkOwnedDomain cues are skipped")
    func networkOwnedSkipped() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .networkOwnedDomain,
                normalizedValue: "earwolf.com",
                sourceField: .description,
                confidence: 0.9,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("All metadata entries have isMetadataOrigin set to true")
    func metadataOriginFlag() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "squarespace",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        for entry in entries {
            #expect(entry.isMetadataOrigin, "All metadata entries must have isMetadataOrigin = true")
        }
    }

    @Test("Trust below minimum produces no entries")
    func trustBelowMinimum() {
        let config = MetadataActivationConfig(
            lexicalInjectionEnabled: true,
            lexicalInjectionMinTrust: 0.3,
            lexicalInjectionDiscount: 0.75,
            classifierPriorShiftEnabled: false,
            classifierPriorShiftMinTrust: 0.08,
            classifierShiftedMidpoint: MetadataActivationConfig.default.classifierShiftedMidpoint,
            classifierBaselineMidpoint: 0.37,
            fmSchedulingEnabled: false,
            fmSchedulingMinTrust: 0.0,
            counterfactualGateOpen: true
        )
        let injector = MetadataLexiconInjector(config: config)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.2)
        #expect(entries.isEmpty)
    }

    @Test("Domain entry pattern matches spoken form in transcript text")
    func domainPatternMatches() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.count == 1)

        let pattern = entries[0].pattern
        let text = "go to betterhelp com for a free trial" as NSString
        let range = NSRange(location: 0, length: text.length)
        let matches = pattern.matches(in: text as String, range: range)
        #expect(matches.count == 1, "Pattern should match spoken domain form")
    }

    @Test("Multiple cue types produce correct entry mix")
    func multipleCueTypes() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "squarespace",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "teamcoco.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.count == 3)

        let positives = entries.filter { !$0.isNegativePattern }
        let negatives = entries.filter { $0.isNegativePattern }
        #expect(positives.count == 2)
        #expect(negatives.count == 1)
    }
}

// MARK: - MetadataLexiconInjector — 2-Hit Rule

@Suite("MetadataLexiconInjector — 2-Hit Rule Enforcement")
struct MetadataLexiconTwoHitRuleTests {

    @Test("Metadata-only hit group is not promoted to candidate")
    func metadataOnlyGroupNotPromoted() {
        let injector = MetadataLexiconInjector(config: .allEnabled)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "betterhelp",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        let chunk = makeMetadataActivationChunk(
            text: "betterhelp com betterhelp",
            normalizedText: "betterhelp com betterhelp"
        )
        let candidates = LexicalScanner().scan(
            chunks: [chunk],
            analysisAssetId: "asset-metadata-only",
            metadataEntries: entries
        )

        #expect(entries.count == 2)
        #expect(entries.allSatisfy { $0.isMetadataOrigin })
        #expect(candidates.isEmpty)
    }

    @Test("Metadata hit supplements one in-audio lexical hit")
    func metadataSupplementsTranscriptHit() {
        let injector = MetadataLexiconInjector(config: .default)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "acme.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        let chunk = makeMetadataActivationChunk(
            text: "Visit acme com.",
            normalizedText: "visit acme com"
        )
        let scanner = LexicalScanner()

        let baseline = scanner.scan(
            chunks: [chunk],
            analysisAssetId: "asset-baseline"
        )
        let withMetadata = scanner.scan(
            chunks: [chunk],
            analysisAssetId: "asset-with-metadata",
            metadataEntries: entries
        )

        #expect(baseline.isEmpty,
                "the built-in 'visit <domain> com' hit alone stays below the two-hit threshold")
        #expect(withMetadata.count == 1)
        #expect(withMetadata[0].hitCount == 2)
    }

    @Test("Duplicate metadata entries from description and summary count once")
    func duplicateMetadataEntriesCountOnce() throws {
        let injector = MetadataLexiconInjector(config: .default)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "acme.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "acme.com",
                sourceField: .summary,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        let chunk = makeMetadataActivationChunk(
            text: "Visit acme com.",
            normalizedText: "visit acme com"
        )

        let candidates = LexicalScanner().scan(
            chunks: [chunk],
            analysisAssetId: "asset-duplicate-metadata",
            metadataEntries: entries
        )

        let candidate = try #require(candidates.first)
        #expect(entries.count == 2)
        #expect(candidate.hitCount == 2,
                "one transcript hit plus one unique metadata supplement should not count duplicate feed fields twice")
    }

    @Test("Negative metadata hit does not promote a one-hit transcript group")
    func negativeMetadataHitDoesNotPromoteTranscriptHit() {
        let injector = MetadataLexiconInjector(config: .default)
        let cues = [
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "mypodcast.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        let chunk = makeMetadataActivationChunk(
            text: "Visit mypodcast com.",
            normalizedText: "visit mypodcast com"
        )

        let candidates = LexicalScanner().scan(
            chunks: [chunk],
            analysisAssetId: "asset-negative-metadata",
            metadataEntries: entries
        )

        #expect(entries.count == 1)
        #expect(entries[0].isNegativePattern)
        #expect(candidates.isEmpty,
                "negative metadata reduces score but must not count as promotion evidence")
    }

    @Test("Negative metadata does not bridge separate positive hit groups")
    func negativeMetadataDoesNotBridgeSeparatePositiveHitGroups() {
        let injector = MetadataLexiconInjector(config: .default)
        let cues = [
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "mypodcast.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 1.0)
        let chunks = [
            makeMetadataActivationChunk(
                text: "Visit acme com.",
                normalizedText: "visit acme com",
                analysisAssetId: "asset-negative-bridge",
                startTime: 0,
                endTime: 1
            ),
            makeMetadataActivationChunk(
                text: "mypodcast com",
                normalizedText: "mypodcast com",
                analysisAssetId: "asset-negative-bridge",
                startTime: 25,
                endTime: 26
            ),
            makeMetadataActivationChunk(
                text: "free trial",
                normalizedText: "free trial",
                analysisAssetId: "asset-negative-bridge",
                startTime: 55,
                endTime: 56
            ),
        ]

        let candidates = LexicalScanner().scan(
            chunks: chunks,
            analysisAssetId: "asset-negative-bridge",
            metadataEntries: entries
        )

        #expect(candidates.isEmpty,
                "negative metadata may reduce nearby candidate score, but must not merge otherwise separate positive one-hit groups")
    }

    @Test("Negative metadata reduces confidence without inflating candidate evidence")
    func negativeMetadataReducesConfidenceWithoutInflatingEvidence() throws {
        let injector = MetadataLexiconInjector(config: .default)
        let cues = [
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "mypodcast.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 1.0)
        let chunk = makeMetadataActivationChunk(
            text: "Visit acme com for a free trial. mypodcast com mypodcast com mypodcast com.",
            normalizedText: "visit acme com for a free trial mypodcast com mypodcast com mypodcast com"
        )
        let scanner = LexicalScanner()

        let baseline = scanner.scan(
            chunks: [chunk],
            analysisAssetId: "asset-negative-baseline"
        )
        let withNegativeMetadata = scanner.scan(
            chunks: [chunk],
            analysisAssetId: "asset-negative-scored",
            metadataEntries: entries
        )

        let baselineCandidate = try #require(baseline.first)
        let negativeCandidate = try #require(withNegativeMetadata.first)
        #expect(baselineCandidate.hitCount == 2)
        #expect(negativeCandidate.hitCount == 2,
                "negative metadata should not inflate candidate hit count")
        #expect(negativeCandidate.confidence >= 0)
        #expect(negativeCandidate.confidence < baselineCandidate.confidence,
                "negative metadata should reduce confidence when the candidate is otherwise justified")
    }

    @Test("Production hot-path candidates consume default lexical metadata")
    func productionHotPathConsumesDefaultLexicalMetadata() async throws {
        let store = try await makeTestStore()
        let provider = StaticEpisodeMetadataProvider(metadata: FeedDescriptionMetadata(
            feedDescription: "This episode is supported by Acme. Visit acme.com for details.",
            feedSummary: nil,
            sourceHashes: .init(descriptionHash: 1, summaryHash: 0)
        ))
        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            episodeMetadataProvider: provider
        )
        let chunks = [
            makeMetadataActivationChunk(
                text: "Visit acme com.",
                normalizedText: "visit acme com",
                analysisAssetId: "asset-hotpath-metadata"
            ),
        ]

        let candidates = try await service.hotPathCandidatesForTesting(
            from: chunks,
            analysisAssetId: "asset-hotpath-metadata"
        )

        #expect(candidates.count == 1,
                "production hot-path scanning should use lexical metadata from the episode provider")
        #expect(candidates[0].hitCount == 2)
    }

    @Test("Production hot-path treats show-owned metadata domains as negative")
    func productionHotPathTreatsShowOwnedDomainsAsNegative() async throws {
        let store = try await makeTestStore()
        let provider = StaticEpisodeMetadataProvider(
            metadata: FeedDescriptionMetadata(
                feedDescription: "Find links and extras at https://mypodcast.com/episode.",
                feedSummary: nil,
                sourceHashes: .init(descriptionHash: 1, summaryHash: 0)
            ),
            showOwnedDomains: ["mypodcast.com"]
        )
        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            episodeMetadataProvider: provider
        )
        let chunks = [
            makeMetadataActivationChunk(
                text: "Visit mypodcast com.",
                normalizedText: "visit mypodcast com",
                analysisAssetId: "asset-hotpath-show-owned"
            ),
        ]

        let candidates = try await service.hotPathCandidatesForTesting(
            from: chunks,
            analysisAssetId: "asset-hotpath-show-owned"
        )

        #expect(candidates.isEmpty,
                "show-owned metadata domains must not promote a transcript mention")
    }
}

private func makeMetadataActivationChunk(
    text: String,
    normalizedText: String,
    analysisAssetId: String = "asset",
    startTime: Double = 10,
    endTime: Double = 20
) -> TranscriptChunk {
    TranscriptChunk(
        id: UUID().uuidString,
        analysisAssetId: analysisAssetId,
        segmentFingerprint: UUID().uuidString,
        chunkIndex: 0,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: normalizedText,
        pass: "fast",
        modelVersion: "metadata-activation-test",
        transcriptVersion: nil,
        atomOrdinal: nil
    )
}

private struct StaticEpisodeMetadataProvider: EpisodeMetadataProvider {
    let metadata: FeedDescriptionMetadata?
    var showOwnedDomains: Set<String> = []
    var networkOwnedDomains: Set<String> = []

    func metadata(for analysisAssetId: String) async -> FeedDescriptionMetadata? {
        metadata
    }

    func metadataSnapshot(for analysisAssetId: String) async -> EpisodeMetadataSnapshot? {
        guard let metadata else { return nil }
        return EpisodeMetadataSnapshot(
            feedMetadata: metadata,
            showOwnedDomains: showOwnedDomains,
            networkOwnedDomains: networkOwnedDomains
        )
    }
}

// MARK: - MetadataPriorShift

@Suite("MetadataPriorShift — Sigmoid Midpoint")
struct MetadataPriorShiftTests {

    @Test("Baseline midpoint returned when prior-shift flag is disabled")
    func priorShiftDisabled() {
        let shift = MetadataPriorShift(config: .default)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.5)
        #expect(mid == 0.37)
    }

    @Test("Baseline midpoint returned when trust is below threshold")
    func trustBelowThreshold() {
        let shift = MetadataPriorShift(config: .allEnabled)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.07)
        #expect(mid == 0.37, "Trust 0.07 < 0.08 threshold should return baseline")
    }

    @Test("Shifted midpoint returned when trust meets threshold")
    func trustMeetsThreshold() {
        let shift = MetadataPriorShift(config: .allEnabled)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.08)
        #expect(mid == 0.345, "Trust 0.08 >= 0.08 threshold should return shifted midpoint")
    }

    @Test("Shifted midpoint returned when trust exceeds threshold")
    func trustExceedsThreshold() {
        let shift = MetadataPriorShift(config: .allEnabled)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.5)
        #expect(mid == 0.345)
    }

    @Test("isShiftActive returns false when prior-shift flag is disabled")
    func shiftActivePriorShiftDisabled() {
        let shift = MetadataPriorShift(config: .default)
        #expect(!shift.isShiftActive(metadataTrust: 0.5))
    }

    @Test("isShiftActive returns false when trust below threshold")
    func shiftActiveTrustBelow() {
        let shift = MetadataPriorShift(config: .allEnabled)
        #expect(!shift.isShiftActive(metadataTrust: 0.07))
    }

    @Test("isShiftActive returns true when trust meets threshold")
    func shiftActiveTrustMeets() {
        let shift = MetadataPriorShift(config: .allEnabled)
        #expect(shift.isShiftActive(metadataTrust: 0.08))
    }

    @Test("Shifted midpoint is strictly less than baseline")
    func shiftedLessThanBaseline() {
        let config = MetadataActivationConfig.allEnabled
        #expect(config.classifierShiftedMidpoint < config.classifierBaselineMidpoint)
    }

    @Test("Prior shift with zero trust returns baseline")
    func zeroTrust() {
        let shift = MetadataPriorShift(config: .allEnabled)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.0)
        #expect(mid == 0.37, "Zero trust should return baseline midpoint")
    }

    @Test("Classifier prior shift only with gate open and individual flag on")
    func classifierPriorShiftGating() {
        // Gate open but individual flag off
        let noShift = MetadataActivationConfig(
            lexicalInjectionEnabled: true,
            lexicalInjectionMinTrust: 0.0,
            lexicalInjectionDiscount: 0.75,
            classifierPriorShiftEnabled: false,
            classifierPriorShiftMinTrust: 0.08,
            classifierShiftedMidpoint: MetadataActivationConfig.default.classifierShiftedMidpoint,
            classifierBaselineMidpoint: 0.37,
            fmSchedulingEnabled: true,
            fmSchedulingMinTrust: 0.0,
            counterfactualGateOpen: true
        )
        let shift = MetadataPriorShift(config: noShift)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.5)
        #expect(mid == 0.37, "Individual flag disabled should return baseline")
    }
}

// MARK: - MetadataPriorShift real-data band (playhead-gtt9.3)

/// playhead-gtt9.3: the default midpoint band must overlap the real-data
/// confidence distribution measured on 2026-04-23. Histogram mode was
/// [0.30, 0.40) (53% of 147 scored windows); the old 0.22 / 0.25 band
/// contained zero windows, making PriorShift inert. The first retune
/// landed `(0.33, 0.37]` at mid-mode (~30 real windows). Per-add diagnostic
/// logging then showed the lower edge concentrated FP-driving adds at
/// ~0.343, so option (a) narrowed to `(0.345, 0.37]` — the current band,
/// covering ~12 real windows on the eval corpus.
///
/// These tests lock in that behavior at the defaults level so a future
/// rebase does not silently revert the retune.
@Suite("MetadataPriorShift — real-data band (gtt9.3)")
struct MetadataPriorShiftRealDataBandTests {

    /// A confidence in the mode of the real-data histogram. Chosen as the
    /// center of the new band so a single value exercises both sides of the
    /// half-open interval `(shifted, baseline]`.
    private static let realDataBandCenter: Double = 0.35

    @Test("Band center (0.35) sits in (shifted, baseline] under defaults — counterfactual flips decision")
    func bandCenterFlipsUnderCounterfactual() {
        // For a window whose fused classifier confidence is the band
        // center, baseline classification says "not ad" (confidence ≤
        // baseline midpoint) while shifted classification says "ad"
        // (confidence > shifted midpoint). That is exactly the flip the
        // priorShift counterfactual is supposed to produce.
        let baseline = MetadataActivationConfig.default.classifierBaselineMidpoint
        let shifted = MetadataActivationConfig.default.classifierShiftedMidpoint
        let c = Self.realDataBandCenter

        #expect(c > shifted,
                "Band-center confidence must exceed shifted midpoint; otherwise priorShift cannot flip it.")
        #expect(c <= baseline,
                "Band-center confidence must sit at or below baseline midpoint; otherwise the window was already an ad.")
    }

    @Test("Band invariants: baseline > shifted, both inside (0, 1)")
    func bandInvariants() {
        let config = MetadataActivationConfig.default
        #expect(config.classifierBaselineMidpoint > config.classifierShiftedMidpoint,
                "Baseline must exceed shifted; otherwise the band inverts and priorShift becomes a no-op.")
        #expect(config.classifierShiftedMidpoint > 0.0)
        #expect(config.classifierBaselineMidpoint < 1.0)
    }

    @Test("Midpoints stay inside the candidate/confirmation envelope")
    func midpointsInsideDetectionEnvelope() {
        // The classifier midpoints are sigmoid-level thresholds; they must
        // sit strictly below AdDetectionConfig.candidateThreshold so
        // priorShift-flipped windows can still be filtered by the candidate
        // stage without the midpoint itself being above the emit threshold,
        // and strictly above suppressionThreshold so no flipped window
        // triggers suppression. Regression guard against future retunes
        // that cross either rail.
        let config = MetadataActivationConfig.default
        let detection = AdDetectionConfig.default
        #expect(config.classifierBaselineMidpoint <= detection.candidateThreshold,
                "Baseline midpoint must not exceed candidateThreshold — flipped windows would be pre-filtered.")
        #expect(config.classifierShiftedMidpoint >= detection.suppressionThreshold,
                "Shifted midpoint must stay at or above suppressionThreshold — flipped windows would be suppressed.")
        #expect(config.classifierBaselineMidpoint < detection.confirmationThreshold,
                "Baseline midpoint must sit well below confirmationThreshold.")
    }

    @Test("Option-a guard: observed FP-driving cluster at ~0.343 sits at or below the shifted midpoint (excluded from band)")
    func optionAExcludesObserved343Cluster() {
        // Per-add diagnostic logging on the eval corpus (2026-05-06) showed
        // priorShift fires concentrated in (0.34, 0.36], with two ~0.343 adds
        // driving DoaC strict-τ regressions. Option (a) raised the lower band
        // edge from 0.33 to 0.345 to exclude that cluster. If a future retune
        // drops the shifted midpoint below 0.343, the cluster re-enters the
        // band and the regressions return — this guard fails first so the
        // author has to delete it deliberately.
        let cfg = MetadataActivationConfig.default
        #expect(0.343 <= cfg.classifierShiftedMidpoint,
                "Observed FP-driving cluster at ~0.343 must NOT exceed the shifted midpoint, else option-a regresses.")
    }
}

// MARK: - MetadataSeededRegion (BackfillJobPhase)

@Suite("MetadataSeededRegion — FM Scheduling Phase")
struct MetadataSeededRegionTests {

    @Test("metadataSeededRegion is a valid BackfillJobPhase case")
    func phaseExists() {
        let phase = BackfillJobPhase.metadataSeededRegion
        #expect(phase.rawValue == "metadataSeededRegion")
    }

    @Test("metadataSeededRegion round-trips via Codable")
    func codableRoundTrip() throws {
        let phase = BackfillJobPhase.metadataSeededRegion
        let encoder = JSONEncoder()
        let data = try encoder.encode(phase)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(BackfillJobPhase.self, from: data)
        #expect(decoded == phase)
    }

    @Test("metadataSeededRegion is included in allCases")
    func inAllCases() {
        #expect(BackfillJobPhase.allCases.contains(.metadataSeededRegion))
    }

    @Test("BackfillJob can be created with metadataSeededRegion phase")
    func backfillJobWithPhase() {
        let job = BackfillJob(
            jobId: "test-job",
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            phase: .metadataSeededRegion,
            coveragePolicy: .targetedWithAudit,
            priority: 5,
            progressCursor: nil,
            retryCount: 0,
            deferReason: nil,
            status: .queued,
            scanCohortJSON: nil,
            createdAt: 1000.0
        )
        #expect(job.phase == .metadataSeededRegion)
    }
}

// MARK: - LexicalScannerCategoryWeights

@Suite("LexicalScannerCategoryWeights — Consistency")
struct LexicalScannerCategoryWeightsTests {

    @Test("Weights match LexicalScanner.categoryWeight for all categories")
    func weightsMatchScanner() {
        // These weights must match the private categoryWeight in LexicalScanner.
        // If they drift, metadata injection weights will be wrong.
        #expect(LexicalScannerCategoryWeights.weight(for: .sponsor) == 1.0)
        #expect(LexicalScannerCategoryWeights.weight(for: .promoCode) == 1.2)
        #expect(LexicalScannerCategoryWeights.weight(for: .urlCTA) == 0.8)
        #expect(LexicalScannerCategoryWeights.weight(for: .purchaseLanguage) == 0.9)
        #expect(LexicalScannerCategoryWeights.weight(for: .transitionMarker) == 0.3)
    }
}

// MARK: - Weight Formula Verification

@Suite("Weight Formula — baseCategoryWeight x metadataTrust x 0.75")
struct WeightFormulaTests {

    @Test("Weight formula: sponsor at trust 1.0")
    func sponsorFullTrust() {
        let injector = MetadataLexiconInjector(config: .allEnabled)
        let cues = [
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "acme",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 1.0)
        #expect(entries.count == 1)
        // 1.0 * 1.0 * 0.75 = 0.75
        #expect(abs(entries[0].weight - 0.75) < 0.001)
    }

    @Test("Weight formula: external domain at trust 0.2")
    func externalDomainLowTrust() {
        let injector = MetadataLexiconInjector(config: .allEnabled)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "example.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.2)
        #expect(entries.count == 1)
        // 0.8 * 0.2 * 0.75 = 0.12
        #expect(abs(entries[0].weight - 0.12) < 0.001)
    }

    @Test("Weight formula: show-owned domain produces negative weight")
    func showOwnedNegativeWeight() {
        let injector = MetadataLexiconInjector(config: .allEnabled)
        let cues = [
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "mypodcast.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.count == 1)
        // -(0.8 * 0.5 * 0.75) = -0.30
        let expectedWeight = -(0.8 * 0.5 * 0.75)
        #expect(abs(entries[0].weight - expectedWeight) < 0.001)
    }
}
