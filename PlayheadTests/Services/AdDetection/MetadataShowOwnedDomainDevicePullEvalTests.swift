// MetadataShowOwnedDomainDevicePullEvalTests.swift
// What the show-owned domain population does to a real device's pipeline.
//
// playhead-kmw4 wrote this file to measure ONE transition — removing the
// show-notes-frequency promotion — and that transition has merged. playhead-e8mg
// retargets the same machinery at the NEXT one, because the question the file
// answers is not "what did kmw4 do" but "what is `showOwned` costing this
// device today". The arms are now:
//
//   BEFORE = main @ 8929cbf1. `showOwned` is the FEED URL's eTLD+1 and nothing
//            else: `simplecast.com` for Conan, `flightcast.com` for Diary Of A
//            CEO — the hosting platform in both cases.
//   AFTER  = playhead-e8mg. `showOwned` is the channel `<link>` and the
//            `<itunes:owner>` email domain, minus the feed's own host. The feed
//            URL survives only as that exclusion.
//
// WHY THIS IS A TEST AND NOT A SCRIPT (kmw4's reason, unchanged). Every number
// here is produced by production types — `FeedParser`, `OwnershipGraph`,
// `MetadataCueExtractor`, `MetadataLexiconInjector`, `LexicalScanner`,
// `LexicalAutoAdEvidenceBuilder`. A Python re-implementation would measure the
// re-implementation. The corpus arrives as JSON
// (`scripts/kmw4-export-device-corpus.py`, unchanged) and the real code works.
//
// WHERE THE OWNERSHIP SIGNALS COME FROM, which is the part e8mg adds. The
// device pull predates the schema change, so `Playhead.store` has no siteURL
// or ownerEmail column to export. They are recovered instead by running the
// production `FeedParser` over the two subscribed feeds' REAL channel heads in
// `PlayheadTests/Fixtures/RealFeeds`, matched by feed URL. So the eval's
// ownership inputs are publisher bytes parsed by shipping code, not values
// typed into a fixture by the person measuring.
//
// THE DIRECTION REVERSES, AND THE INVARIANTS REVERSE WITH IT. kmw4 REMOVED
// negative evidence, so confidence could only rise and the auto-ad rule could
// only un-block. e8mg ADDS negative entries, so confidence can only fall and a
// span can only lose `.lexicalAutoAd`. Both are asserted, because an assertion
// in the wrong direction is a test that cannot fail.
//
// WHAT IT CANNOT SEE — read before quoting any number:
//  * It measures the LEXICAL half of the pipeline (metadata lexicon entries,
//    the hits they produce, the candidates they weight, and the auto-ad rule
//    they suppress). A `.showOwnedDomain` cue ALSO carries a 0.05 cue-type
//    weight in `FeedDescriptionEvidenceBuilder`; that delta is measured here,
//    but the fusion gate consuming it is not re-run.
//  * `ad_windows` on the device were produced by the shipped pipeline. Nothing
//    here re-derives them, so "a decision changes" means "an input to the
//    decision changes by this much", not "the device would have skipped".
//
// STAGING: write the JSON export's path into `<repo>/.kmw4-corpus-path` (both
// pointer files are gitignored). The corpus is NOT in the repo — 17 MB of a
// device's transcripts — so with no pointer this SKIPS. A path in
// `<repo>/.kmw4-report-path` writes the full report as JSON. See `corpusPath()`
// for why this is a file and not an exported variable.

import Foundation
import XCTest
@testable import Playhead

final class MetadataShowOwnedDomainDevicePullEvalTests: XCTestCase {

    // MARK: - Corpus shape (mirrors scripts/kmw4-export-device-corpus.py)

    struct Corpus: Decodable {
        let source: String
        let shows: [Show]
        let assets: [Asset]

        struct Show: Decodable {
            let title: String?
            let feedURL: String?
            let podcastId: String?
            let episodes: [Episode]
        }

        struct Episode: Decodable {
            let canonicalEpisodeKey: String
            let feedDescription: String?
            let feedSummary: String?
        }

        struct Asset: Decodable {
            let id: String
            let episodeId: String
            let episodeTitle: String?
            let showIndex: Int
            let feedDescription: String?
            let feedSummary: String?
            let chunks: [Chunk]
            let spans: [Span]
        }

        struct Chunk: Decodable {
            let id: String
            let chunkIndex: Int
            let startTime: Double
            let endTime: Double
            let text: String
            let normalizedText: String
            let `pass`: String
            let atomOrdinal: Int?
        }

        struct Span: Decodable {
            let id: String
            let firstAtomOrdinal: Int
            let lastAtomOrdinal: Int
            let startTime: Double
            let endTime: Double
        }
    }

    /// The `metadataTrust` values a device actually resolves. 0.724 and 0.764
    /// are the two shows' pre-V57 readings recorded in `AnalysisStore`'s
    /// migration note; 0.500 is what both fall to once playhead-g7ln's trait
    /// repair deactivates the trait tier. Entry and hit counts do not depend
    /// on trust at all (it scales weight, it does not gate); only candidate
    /// confidence does, which is why the sweep exists.
    static let trustSweep: [Float] = [0.500, 0.724, 0.764]

    // MARK: - Report shape

    struct ShowReport: Encodable {
        let title: String?
        let feedURL: String?
        let feedDomain: String?
        let siteURL: String?
        let siteDomain: String?
        let ownerEmail: String?
        let ownerDomain: String?
        let episodes: Int
        let showOwnedBefore: [String]
        let showOwnedAfter: [String]
        /// Which of the two surviving routes produced each AFTER entry.
        let showOwnedAfterBySource: [String: String]
        let ownershipUndeterminedBefore: [String]
        let ownershipUndeterminedAfter: [String]
        let addedToShowOwned: [String]
        let droppedFromShowOwned: [String]
        /// A THIRD arm, and the reason it exists: `<link>` and
        /// `<itunes:owner>` disagree on 65 % of real feeds (265 of the 757 of
        /// 918 that carry both agree), and on Conan the `<link>` names the
        /// DISTRIBUTOR. Measuring the owner route alone is the only way to
        /// say what the `<link>` route costs ON TOP of it rather than
        /// attributing the pair's total to both.
        let showOwnedOwnerOnly: [String]
    }

    struct AssetReport: Encodable {
        let assetId: String
        let title: String?
        let chunks: Int
        let spans: Int
        let metadataEntriesBefore: Int
        let metadataEntriesAfter: Int
        let negativeEntriesBefore: Int
        let negativeEntriesAfter: Int
        let negativeEntrySourcesBefore: [String]
        let negativeEntrySourcesAfter: [String]
        let metadataHitsBefore: Int
        let metadataHitsAfter: Int
        let negativeHitsBefore: Int
        let negativeHitsAfter: Int
        let negativeHitDomainsAfter: [String]
        let candidatesBefore: Int
        let candidatesAfter: Int
        /// Candidates whose confidence moved at all, per trust value.
        let candidatesChangedByTrust: [String: Int]
        /// Most-negative confidence movement, per trust value. Adding negative
        /// evidence can only push a confidence DOWN, so this is <= 0.
        let minConfidenceDeltaByTrust: [String: Double]
        let autoAdSpansBefore: Int
        let autoAdSpansAfter: Int
        /// Spans that fired `.lexicalAutoAd` before and no longer do. This is
        /// the decision-level threshold crossing this change can reach, and
        /// the direction it can only move in.
        let autoAdSpansSuppressed: [String]
        /// EXPOSURE, because `autoAdSpansSuppressed` has no power when nothing
        /// fired to begin with. A negative hit anywhere in a span is a HARD
        /// suppressor of `.lexicalAutoAd`, whether or not the span would
        /// otherwise have fired — so this is the population the change puts
        /// under the suppressor, and `spansWithPromotionHits` is the subset
        /// where that suppression could ever have mattered.
        let spansHardSuppressedBefore: Int
        let spansHardSuppressedAfter: Int
        let spansWithPromotionHits: Int
        /// Spans newly hard-suppressed that ALSO carry a promotion hit — the
        /// only population in which this change can cost a real detection.
        let spansNewlySuppressedWithPromotionHits: [String]
        /// The same exposure under the OWNER-ONLY arm. The difference between
        /// this and `spansHardSuppressedAfter` is what the `<link>` route
        /// costs by itself.
        let spansHardSuppressedOwnerOnly: Int
        let negativeHitsOwnerOnly: Int
        let metadataLedgerWeightBefore: Double
        let metadataLedgerWeightAfter: Double
    }

    // MARK: - The eval

    func testDevicePullStructuralOwnership() throws {
        let corpus = try Self.loadCorpus()
        let feeds = try Self.realFeedSignals()

        var showReports: [ShowReport] = []
        var beforeOwnership: [Set<String>] = []
        var beforeUndetermined: [Set<String>] = []
        var afterOwnership: [Set<String>] = []
        var afterUndetermined: [Set<String>] = []
        var ownerOnlyOwnership: [Set<String>] = []
        var ownerOnlyUndetermined: [Set<String>] = []

        for show in corpus.shows {
            let feedURLString = show.feedURL
            let podcastId = show.podcastId ?? show.feedURL ?? "unknown"
            let recentMetadata = show.episodes.map { episode in
                FeedDescriptionMetadata(
                    feedDescription: episode.feedDescription,
                    feedSummary: episode.feedSummary,
                    sourceHashes: .init()
                )
            }
            let signals = feedURLString.flatMap { feeds[$0] }
            XCTAssertNotNil(
                signals,
                "no real-feed fixture for \(feedURLString ?? "nil") — the eval's "
                + "ownership inputs must come from publisher bytes, so a missing "
                + "fixture is a hole, not a default"
            )

            // AFTER: what production now builds.
            let after = EpisodeMetadataSnapshot.domainOwnership(
                feedURL: feedURLString.flatMap(URL.init(string:)),
                siteURL: signals?.siteURL,
                ownerEmail: signals?.ownerEmail,
                recentMetadata: recentMetadata,
                podcastId: podcastId
            )

            // OWNER-ONLY: the `<itunes:owner>` route with no `<link>`.
            let ownerOnly = EpisodeMetadataSnapshot.domainOwnership(
                feedURL: feedURLString.flatMap(URL.init(string:)),
                siteURL: nil,
                ownerEmail: signals?.ownerEmail,
                recentMetadata: recentMetadata,
                podcastId: podcastId
            )

            // BEFORE: main @ 8929cbf1, reconstructed and then PROVEN below.
            let before = try Self.preE8mgOwnership(
                feedURL: feedURLString,
                recentMetadata: recentMetadata,
                podcastId: podcastId
            )

            beforeOwnership.append(before.showOwned)
            beforeUndetermined.append(before.ownershipUndetermined)
            afterOwnership.append(after.showOwned)
            afterUndetermined.append(after.ownershipUndetermined)
            ownerOnlyOwnership.append(ownerOnly.showOwned)
            ownerOnlyUndetermined.append(ownerOnly.ownershipUndetermined)

            var bySource: [String: String] = [:]
            if let domain = signals?.siteURL.flatMap({ DomainNormalizer.etld1(from: $0.absoluteString) }),
               after.showOwned.contains(domain) {
                bySource[domain] = "rssLink"
            }
            if let domain = signals?.ownerEmail.flatMap({ DomainNormalizer.etld1(from: $0) }),
               after.showOwned.contains(domain) {
                bySource[domain] = bySource[domain].map { "\($0)+itunesOwner" } ?? "itunesOwner"
            }

            showReports.append(ShowReport(
                title: show.title,
                feedURL: show.feedURL,
                feedDomain: show.feedURL.flatMap { DomainNormalizer.etld1(from: $0) },
                siteURL: signals?.siteURL?.absoluteString,
                siteDomain: signals?.siteURL.flatMap { DomainNormalizer.etld1(from: $0.absoluteString) },
                ownerEmail: signals?.ownerEmail,
                ownerDomain: signals?.ownerEmail.flatMap { DomainNormalizer.etld1(from: $0) },
                episodes: show.episodes.count,
                showOwnedBefore: before.showOwned.sorted(),
                showOwnedAfter: after.showOwned.sorted(),
                showOwnedAfterBySource: bySource,
                ownershipUndeterminedBefore: before.ownershipUndetermined.sorted(),
                ownershipUndeterminedAfter: after.ownershipUndetermined.sorted(),
                addedToShowOwned: after.showOwned.subtracting(before.showOwned).sorted(),
                droppedFromShowOwned: before.showOwned.subtracting(after.showOwned).sorted(),
                showOwnedOwnerOnly: ownerOnly.showOwned.sorted()
            ))
        }

        let scanner = LexicalScanner()
        let autoAdBuilder = LexicalAutoAdEvidenceBuilder()
        let ledgerBuilder = FeedDescriptionEvidenceBuilder()
        let injector = MetadataLexiconInjector(config: .default)

        var assetReports: [AssetReport] = []

        for asset in corpus.assets {
            guard asset.showIndex >= 0, asset.showIndex < corpus.shows.count else {
                XCTFail("asset \(asset.id) has no show — the export lost the episode join")
                continue
            }

            let extractorBefore = MetadataCueExtractor(
                showOwnedDomains: beforeOwnership[asset.showIndex],
                networkOwnedDomains: [],
                ownershipUndeterminedDomains: beforeUndetermined[asset.showIndex]
            )
            let extractorAfter = MetadataCueExtractor(
                showOwnedDomains: afterOwnership[asset.showIndex],
                networkOwnedDomains: [],
                ownershipUndeterminedDomains: afterUndetermined[asset.showIndex]
            )
            let cuesBefore = extractorBefore.extractCues(
                description: asset.feedDescription,
                summary: asset.feedSummary
            )
            let cuesAfter = extractorAfter.extractCues(
                description: asset.feedDescription,
                summary: asset.feedSummary
            )

            // Entries. Trust only scales weight, so any positive value gives
            // the same population; the sweep below is for confidence.
            let entriesBefore = injector.inject(cues: cuesBefore, metadataTrust: 0.724)
            let entriesAfter = injector.inject(cues: cuesAfter, metadataTrust: 0.724)
            let negativeBefore = entriesBefore.filter(\.isNegativePattern)
            let negativeAfter = entriesAfter.filter(\.isNegativePattern)

            let chunks = TranscriptChunkCanonicalizer.canonicalize(
                asset.chunks.map { chunk in
                    TranscriptChunk(
                        id: chunk.id,
                        analysisAssetId: asset.id,
                        segmentFingerprint: "",
                        chunkIndex: chunk.chunkIndex,
                        startTime: chunk.startTime,
                        endTime: chunk.endTime,
                        text: chunk.text,
                        normalizedText: chunk.normalizedText,
                        pass: chunk.pass,
                        modelVersion: "device-pull",
                        transcriptVersion: nil,
                        atomOrdinal: chunk.atomOrdinal
                    )
                }
            ).chunks.sorted(by: TranscriptChunkCanonicalizer.canonicalTimeOrder)

            let extractorOwnerOnly = MetadataCueExtractor(
                showOwnedDomains: ownerOnlyOwnership[asset.showIndex],
                networkOwnedDomains: [],
                ownershipUndeterminedDomains: ownerOnlyUndetermined[asset.showIndex]
            )
            let cuesOwnerOnly = extractorOwnerOnly.extractCues(
                description: asset.feedDescription,
                summary: asset.feedSummary
            )
            let hitsOwnerOnly = scanner.collectHits(
                chunks: chunks,
                metadataEntries: injector.inject(cues: cuesOwnerOnly, metadataTrust: 0.724)
            )

            let hitsBefore = scanner.collectHits(chunks: chunks, metadataEntries: entriesBefore)
            let hitsAfter = scanner.collectHits(chunks: chunks, metadataEntries: entriesAfter)
            let metaHitsBefore = hitsBefore.filter(\.isMetadataOrigin)
            let metaHitsAfter = hitsAfter.filter(\.isMetadataOrigin)
            let negHitsBefore = hitsBefore.filter(\.isNegativePattern)
            let negHitsAfter = hitsAfter.filter(\.isNegativePattern)

            // Candidates: the negative stream is excluded from grouping and
            // from the hit count, so the SET must be identical and only the
            // confidence can move. That is an assertion, not an assumption.
            var changedByTrust: [String: Int] = [:]
            var minDeltaByTrust: [String: Double] = [:]
            var candidatesBefore = 0
            var candidatesAfter = 0
            for trust in Self.trustSweep {
                let before = scanner.scan(
                    chunks: chunks,
                    analysisAssetId: asset.id,
                    metadataEntries: injector.inject(cues: cuesBefore, metadataTrust: trust)
                )
                let after = scanner.scan(
                    chunks: chunks,
                    analysisAssetId: asset.id,
                    metadataEntries: injector.inject(cues: cuesAfter, metadataTrust: trust)
                )
                candidatesBefore = before.count
                candidatesAfter = after.count
                XCTAssertEqual(
                    before.count, after.count,
                    "asset \(asset.id): negative metadata hits must not change the candidate SET"
                )
                var changed = 0
                var minDelta = 0.0
                for (lhs, rhs) in zip(before, after) {
                    let delta = rhs.confidence - lhs.confidence
                    if abs(delta) > 1e-12 { changed += 1 }
                    minDelta = min(minDelta, delta)
                    XCTAssertLessThanOrEqual(
                        delta, 1e-12,
                        "asset \(asset.id): ADDING negative evidence cannot raise a confidence"
                    )
                }
                let key = String(format: "%.3f", trust)
                changedByTrust[key] = changed
                minDeltaByTrust[key] = minDelta
            }

            // The decision-level threshold this change can actually reach: a
            // negative-pattern hit anywhere in a span is a HARD suppressor of
            // `.lexicalAutoAd`, which is the only lexical route to auto-skip.
            var autoAdBefore = 0
            var autoAdAfter = 0
            var suppressed: [String] = []
            var hardBefore = 0
            var hardAfter = 0
            var withPromotion = 0
            var hardOwnerOnly = 0
            var newlySuppressedWithPromotion: [String] = []
            for span in asset.spans {
                let decoded = DecodedSpan(
                    id: span.id,
                    assetId: asset.id,
                    firstAtomOrdinal: span.firstAtomOrdinal,
                    lastAtomOrdinal: span.lastAtomOrdinal,
                    startTime: span.startTime,
                    endTime: span.endTime,
                    anchorProvenance: []
                )
                let firedBefore = !autoAdBuilder.buildEntries(hits: hitsBefore, for: decoded).isEmpty
                let firedAfter = !autoAdBuilder.buildEntries(hits: hitsAfter, for: decoded).isEmpty
                if firedBefore { autoAdBefore += 1 }
                if firedAfter { autoAdAfter += 1 }
                if firedBefore && !firedAfter { suppressed.append(span.id) }
                XCTAssertFalse(
                    !firedBefore && firedAfter,
                    "asset \(asset.id) span \(span.id): adding a suppressor cannot make the "
                    + "auto-ad rule fire"
                )

                // Exposure. Same overlap predicate the builder uses — a hit
                // overlaps when its interval intersects the span's.
                func overlapping(_ hits: [LexicalHit]) -> [LexicalHit] {
                    hits.filter { $0.startTime <= span.endTime && $0.endTime >= span.startTime }
                }
                let negBefore = overlapping(hitsBefore).contains(where: \.isNegativePattern)
                let overlapAfter = overlapping(hitsAfter)
                let negAfter = overlapAfter.contains(where: \.isNegativePattern)
                let promotion = overlapAfter.contains {
                    !$0.isNegativePattern && !$0.isMetadataOrigin
                }
                let negOwnerOnly = hitsOwnerOnly.contains {
                    $0.isNegativePattern
                        && $0.startTime <= span.endTime && $0.endTime >= span.startTime
                }
                if negOwnerOnly { hardOwnerOnly += 1 }
                if negBefore { hardBefore += 1 }
                if negAfter { hardAfter += 1 }
                if promotion { withPromotion += 1 }
                if !negBefore && negAfter && promotion {
                    newlySuppressedWithPromotion.append(span.id)
                }
            }

            // The other consumer of a `.showOwnedDomain` cue: its 0.05
            // cue-type weight in the metadata evidence ledger. Measured on a
            // whole-asset span because this builder is feed-level.
            let wholeAsset = DecodedSpan(
                id: "\(asset.id)-whole",
                assetId: asset.id,
                firstAtomOrdinal: 0,
                lastAtomOrdinal: 0,
                startTime: chunks.first?.startTime ?? 0,
                endTime: chunks.last?.endTime ?? 0,
                anchorProvenance: []
            )
            let ledgerBefore = ledgerBuilder.buildEntries(cues: cuesBefore, for: wholeAsset)
                .reduce(0.0) { $0 + $1.weight }
            let ledgerAfter = ledgerBuilder.buildEntries(cues: cuesAfter, for: wholeAsset)
                .reduce(0.0) { $0 + $1.weight }

            assetReports.append(AssetReport(
                assetId: asset.id,
                title: asset.episodeTitle,
                chunks: chunks.count,
                spans: asset.spans.count,
                metadataEntriesBefore: entriesBefore.count,
                metadataEntriesAfter: entriesAfter.count,
                negativeEntriesBefore: negativeBefore.count,
                negativeEntriesAfter: negativeAfter.count,
                negativeEntrySourcesBefore: negativeBefore.map(\.sourceValue).sorted(),
                negativeEntrySourcesAfter: negativeAfter.map(\.sourceValue).sorted(),
                metadataHitsBefore: metaHitsBefore.count,
                metadataHitsAfter: metaHitsAfter.count,
                negativeHitsBefore: negHitsBefore.count,
                negativeHitsAfter: negHitsAfter.count,
                negativeHitDomainsAfter: negHitsAfter.map { $0.matchedText.lowercased() }.sorted(),
                candidatesBefore: candidatesBefore,
                candidatesAfter: candidatesAfter,
                candidatesChangedByTrust: changedByTrust,
                minConfidenceDeltaByTrust: minDeltaByTrust,
                autoAdSpansBefore: autoAdBefore,
                autoAdSpansAfter: autoAdAfter,
                autoAdSpansSuppressed: suppressed,
                spansHardSuppressedBefore: hardBefore,
                spansHardSuppressedAfter: hardAfter,
                spansWithPromotionHits: withPromotion,
                spansNewlySuppressedWithPromotionHits: newlySuppressedWithPromotion,
                spansHardSuppressedOwnerOnly: hardOwnerOnly,
                negativeHitsOwnerOnly: hitsOwnerOnly.filter(\.isNegativePattern).count,
                metadataLedgerWeightBefore: ledgerBefore,
                metadataLedgerWeightAfter: ledgerAfter
            ))
        }

        // The bead's headline claim, asserted rather than narrated: the show's
        // OWN domain is structurally recognisable again. Nothing on main
        // classifies `teamcoco.com`, and after this change something does.
        let recovered = showReports.contains { $0.showOwnedAfter.contains("teamcoco.com") }
        XCTAssertTrue(recovered, "teamcoco.com is still classified by nothing")
        let platformSurvives = showReports.contains {
            guard let feedDomain = $0.feedDomain else { return false }
            return $0.showOwnedAfter.contains(feedDomain)
        }
        XCTAssertFalse(platformSurvives, "a feed host is still being called show-owned")

        Self.emit(corpus: corpus, shows: showReports, assets: assetReports)
    }

    // MARK: - Before-arm fidelity

    /// The BEFORE arm is a reconstruction — `ingestFeedURL` no longer exists —
    /// so it is proven rather than asserted in prose. The deleted method did
    /// exactly one thing: `setEntry(domain:label:.showOwned, source:.feedURL)`
    /// for the feed URL's eTLD+1. The only door left with that behaviour is
    /// `ingestRSSLink` on a graph with no feed-host exclusion, and it differs
    /// only in the `source` tag — which neither reader of this graph looks at.
    /// `showOwnedDomains` reads the LABEL; `recurringShowNotesDomains` reads
    /// `source == .showNotesFrequency`, and both `.feedURL` and `.rssLink`
    /// fail that test identically. The checks below pin all three facts.
    private static func preE8mgOwnership(
        feedURL: String?,
        recentMetadata: [FeedDescriptionMetadata],
        podcastId: String
    ) throws -> EpisodeMetadataSnapshot.ShowDomainOwnership {
        var graph = OwnershipGraph(podcastId: podcastId)
        if let feedURL {
            graph.ingestRSSLink(feedURL)
        }
        for metadata in recentMetadata {
            var episodeDomains = Set<String>()
            for text in [metadata.feedDescription, metadata.feedSummary].compactMap(\.self) {
                episodeDomains.formUnion(MetadataCueExtractor.extractDomains(from: text))
            }
            for domain in episodeDomains {
                graph.recordShowNotesDomain(domain)
            }
        }

        let expectedFeedDomain = feedURL.flatMap { DomainNormalizer.etld1(from: $0) }
        let showOwned = Set(graph.showOwnedDomains)
        XCTAssertEqual(
            showOwned, Set([expectedFeedDomain].compactMap(\.self)),
            "the reconstructed pre-e8mg show-owned set must be exactly the feed host"
        )
        if let expectedFeedDomain {
            let entry = graph.entries[expectedFeedDomain]
            XCTAssertEqual(entry?.label, .showOwned)
            XCTAssertNotEqual(
                entry?.source, .showNotesFrequency,
                "the feed-host entry must not be a frequency entry, or the undetermined "
                + "set would differ from what shipped"
            )
        }
        let threshold = OwnershipGraphConfig.default.showNotesRecurrenceThreshold
        let historical = Set(graph.entries.values
            .filter { $0.source == .showNotesFrequency && $0.frequency >= threshold }
            .map(\.domain))
        XCTAssertEqual(
            historical, Set(graph.recurringShowNotesDomains),
            "the reconstructed pre-e8mg undetermined set does not match the frequency table"
        )

        return EpisodeMetadataSnapshot.ShowDomainOwnership(
            showOwned: showOwned,
            ownershipUndetermined: Set(graph.recurringShowNotesDomains)
        )
    }

    // MARK: - Real feed signals

    struct RealFeedSignals {
        let siteURL: URL?
        let ownerEmail: String?
    }

    /// Parse the committed channel heads with the PRODUCTION parser, keyed by
    /// feed URL. If this ever returns a value the shipped parser would not
    /// produce, the eval is measuring a fiction.
    static func realFeedSignals() throws -> [String: RealFeedSignals] {
        struct Manifest: Decodable {
            let feeds: [Entry]
            struct Entry: Decodable {
                let feedURL: String
                let file: String
            }
        }
        let directory = repositoryRoot().appendingPathComponent("PlayheadTests/Fixtures/RealFeeds")
        let manifestData = try Data(contentsOf: directory.appendingPathComponent("manifest.json"))
        let manifest = try JSONDecoder().decode(Manifest.self, from: manifestData)

        var result: [String: RealFeedSignals] = [:]
        for entry in manifest.feeds {
            let data = try Data(contentsOf: directory.appendingPathComponent(entry.file))
            let feed = try FeedParser().parse(data: data, baseURL: URL(string: entry.feedURL))
            result[entry.feedURL] = RealFeedSignals(
                siteURL: feed.siteURL,
                ownerEmail: feed.ownerEmail
            )
        }
        return result
    }

    // MARK: - Staging

    /// Staging is a POINTER FILE, not an environment variable, and that is
    /// not a preference. A unit test hosted in the app runs in the simulator;
    /// `xcodebuild`'s `TEST_RUNNER_*` build settings reach a UI-test runner
    /// and NOT this host, so an exported shell variable arrives as `nil` and
    /// the lane silently SKIPS while reporting success — measured on this box
    /// twice before the file route replaced it. The repo root is resolved from
    /// `#filePath`, which is the same mechanism the NARL FrozenTrace fixtures
    /// use to read host files from a simulator test.
    ///
    /// `PLAYHEAD_KMW4_CORPUS` is still honoured first, so a test plan that
    /// declares the variable keeps working.
    static func corpusPath() -> String? {
        if let path = ProcessInfo.processInfo.environment["PLAYHEAD_KMW4_CORPUS"],
           !path.isEmpty {
            return path
        }
        let pointer = repositoryRoot().appendingPathComponent(".kmw4-corpus-path")
        guard let raw = try? String(contentsOf: pointer, encoding: .utf8) else { return nil }
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    /// `#filePath` is `<repo>/PlayheadTests/Services/AdDetection/<this file>`.
    static func repositoryRoot() -> URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()   // AdDetection
            .deletingLastPathComponent()   // Services
            .deletingLastPathComponent()   // PlayheadTests
            .deletingLastPathComponent()   // <repo>
    }

    static func reportPath() -> String? {
        if let path = ProcessInfo.processInfo.environment["PLAYHEAD_KMW4_OUT"], !path.isEmpty {
            return path
        }
        let pointer = repositoryRoot().appendingPathComponent(".kmw4-report-path")
        guard let raw = try? String(contentsOf: pointer, encoding: .utf8) else { return nil }
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    private static func loadCorpus() throws -> Corpus {
        guard let path = corpusPath() else {
            throw XCTSkip(
                "no corpus staged — write its path into <repo>/.kmw4-corpus-path; "
                + "see scripts/kmw4-export-device-corpus.py"
            )
        }
        guard FileManager.default.fileExists(atPath: path) else {
            throw XCTSkip("staged corpus is missing: \(path)")
        }
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        return try JSONDecoder().decode(Corpus.self, from: data)
    }

    private static func emit(
        corpus: Corpus,
        shows: [ShowReport],
        assets: [AssetReport]
    ) {
        struct Report: Encodable {
            let source: String
            let shows: [ShowReport]
            let assets: [AssetReport]
            let totals: Totals

            struct Totals: Encodable {
                let assets: Int
                let spans: Int
                let metadataEntriesBefore: Int
                let metadataEntriesAfter: Int
                let negativeEntriesBefore: Int
                let negativeEntriesAfter: Int
                let metadataHitsBefore: Int
                let metadataHitsAfter: Int
                let negativeHitsBefore: Int
                let negativeHitsAfter: Int
                let candidatesBefore: Int
                let candidatesAfter: Int
                let autoAdSpansBefore: Int
                let autoAdSpansAfter: Int
                let autoAdSpansSuppressed: Int
                let spansHardSuppressedBefore: Int
                let spansHardSuppressedAfter: Int
                let spansWithPromotionHits: Int
                let spansNewlySuppressedWithPromotionHits: Int
                let spansHardSuppressedOwnerOnly: Int
                let negativeHitsOwnerOnly: Int
                let metadataLedgerWeightBefore: Double
                let metadataLedgerWeightAfter: Double
            }
        }

        let totals = Report.Totals(
            assets: assets.count,
            spans: assets.reduce(0) { $0 + $1.spans },
            metadataEntriesBefore: assets.reduce(0) { $0 + $1.metadataEntriesBefore },
            metadataEntriesAfter: assets.reduce(0) { $0 + $1.metadataEntriesAfter },
            negativeEntriesBefore: assets.reduce(0) { $0 + $1.negativeEntriesBefore },
            negativeEntriesAfter: assets.reduce(0) { $0 + $1.negativeEntriesAfter },
            metadataHitsBefore: assets.reduce(0) { $0 + $1.metadataHitsBefore },
            metadataHitsAfter: assets.reduce(0) { $0 + $1.metadataHitsAfter },
            negativeHitsBefore: assets.reduce(0) { $0 + $1.negativeHitsBefore },
            negativeHitsAfter: assets.reduce(0) { $0 + $1.negativeHitsAfter },
            candidatesBefore: assets.reduce(0) { $0 + $1.candidatesBefore },
            candidatesAfter: assets.reduce(0) { $0 + $1.candidatesAfter },
            autoAdSpansBefore: assets.reduce(0) { $0 + $1.autoAdSpansBefore },
            autoAdSpansAfter: assets.reduce(0) { $0 + $1.autoAdSpansAfter },
            autoAdSpansSuppressed: assets.reduce(0) { $0 + $1.autoAdSpansSuppressed.count },
            spansHardSuppressedBefore: assets.reduce(0) { $0 + $1.spansHardSuppressedBefore },
            spansHardSuppressedAfter: assets.reduce(0) { $0 + $1.spansHardSuppressedAfter },
            spansWithPromotionHits: assets.reduce(0) { $0 + $1.spansWithPromotionHits },
            spansNewlySuppressedWithPromotionHits: assets.reduce(0) {
                $0 + $1.spansNewlySuppressedWithPromotionHits.count
            },
            spansHardSuppressedOwnerOnly: assets.reduce(0) { $0 + $1.spansHardSuppressedOwnerOnly },
            negativeHitsOwnerOnly: assets.reduce(0) { $0 + $1.negativeHitsOwnerOnly },
            metadataLedgerWeightBefore: assets.reduce(0.0) { $0 + $1.metadataLedgerWeightBefore },
            metadataLedgerWeightAfter: assets.reduce(0.0) { $0 + $1.metadataLedgerWeightAfter }
        )

        print("[e8mg] source=\(corpus.source)")
        for show in shows {
            print("[e8mg] show \"\(show.title ?? "?")\" feed=\(show.feedURL ?? "?") "
                  + "feedDomain=\(show.feedDomain ?? "nil") "
                  + "siteDomain=\(show.siteDomain ?? "nil") "
                  + "ownerDomain=\(show.ownerDomain ?? "nil") episodes=\(show.episodes)")
            print("[e8mg]   showOwned BEFORE (\(show.showOwnedBefore.count)): \(show.showOwnedBefore)")
            print("[e8mg]   showOwned AFTER  (\(show.showOwnedAfter.count)): \(show.showOwnedAfter) "
                  + "bySource=\(show.showOwnedAfterBySource)")
            print("[e8mg]   showOwned OWNER-ONLY (\(show.showOwnedOwnerOnly.count)): "
                  + "\(show.showOwnedOwnerOnly)")
            print("[e8mg]   added \(show.addedToShowOwned) dropped \(show.droppedFromShowOwned)")
            print("[e8mg]   undetermined \(show.ownershipUndeterminedBefore.count)"
                  + "->\(show.ownershipUndeterminedAfter.count)")
        }
        for asset in assets where asset.negativeEntriesAfter > 0 || asset.negativeEntriesBefore > 0 {
            print("[e8mg] asset \(asset.assetId) \"\(asset.title ?? "?")\" "
                  + "negEntries \(asset.negativeEntriesBefore)->\(asset.negativeEntriesAfter) "
                  + "\(asset.negativeEntrySourcesAfter) "
                  + "negHits \(asset.negativeHitsBefore)->\(asset.negativeHitsAfter) "
                  + "\(asset.negativeHitDomainsAfter) "
                  + "cand \(asset.candidatesBefore)/\(asset.candidatesAfter) "
                  + "changed \(asset.candidatesChangedByTrust) minΔ \(asset.minConfidenceDeltaByTrust) "
                  + "autoAd \(asset.autoAdSpansBefore)->\(asset.autoAdSpansAfter) "
                  + "ledger \(asset.metadataLedgerWeightBefore)->\(asset.metadataLedgerWeightAfter)")
        }
        print("[e8mg] TOTALS assets=\(totals.assets) spans=\(totals.spans)")
        print("[e8mg] TOTALS metadataEntries \(totals.metadataEntriesBefore)->\(totals.metadataEntriesAfter) "
              + "negativeEntries \(totals.negativeEntriesBefore)->\(totals.negativeEntriesAfter)")
        print("[e8mg] TOTALS metadataHits \(totals.metadataHitsBefore)->\(totals.metadataHitsAfter) "
              + "negativeHits \(totals.negativeHitsBefore)->\(totals.negativeHitsAfter)")
        print("[e8mg] TOTALS candidates \(totals.candidatesBefore)->\(totals.candidatesAfter) "
              + "autoAdSpans \(totals.autoAdSpansBefore)->\(totals.autoAdSpansAfter) "
              + "suppressed=\(totals.autoAdSpansSuppressed)")
        print("[e8mg] TOTALS EXPOSURE spans=\(totals.spans) "
              + "hardSuppressed \(totals.spansHardSuppressedBefore)"
              + "->\(totals.spansHardSuppressedAfter) "
              + "spansWithPromotionHits=\(totals.spansWithPromotionHits) "
              + "newlySuppressedWithPromotion="
              + "\(totals.spansNewlySuppressedWithPromotionHits)")
        print("[e8mg] TOTALS OWNER-ONLY ARM negativeHits=\(totals.negativeHitsOwnerOnly) "
              + "hardSuppressed=\(totals.spansHardSuppressedOwnerOnly) "
              + "(the <link> route costs the difference against "
              + "\(totals.spansHardSuppressedAfter))")
        print("[e8mg] TOTALS metadataLedgerWeight \(totals.metadataLedgerWeightBefore)"
              + "->\(totals.metadataLedgerWeightAfter)")

        guard let out = reportPath() else { return }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let report = Report(source: corpus.source, shows: shows, assets: assets, totals: totals)
        do {
            try encoder.encode(report).write(to: URL(fileURLWithPath: out))
            print("[e8mg] wrote \(out)")
        } catch {
            print("[e8mg] could not write \(out): \(error)")
        }
    }
}
