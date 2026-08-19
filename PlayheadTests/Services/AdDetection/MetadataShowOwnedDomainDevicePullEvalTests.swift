// MetadataShowOwnedDomainDevicePullEvalTests.swift
// playhead-kmw4: what removing the show-notes-frequency -> `.showOwned`
// promotion actually does to a real device's pipeline.
//
// WHY THIS EXISTS AS A TEST AND NOT A SCRIPT. Every number this bead is
// answerable for is produced by production types — `OwnershipGraph`,
// `MetadataCueExtractor`, `MetadataLexiconInjector`, `LexicalScanner`,
// `LexicalAutoAdEvidenceBuilder`. A Python re-implementation of the scanner
// would measure the re-implementation. So the corpus comes in as JSON
// (`scripts/kmw4-export-device-corpus.py`) and the real code does the work.
//
// HOW THE "BEFORE" ARM IS BUILT, because this is the one place the eval could
// lie. It does NOT keep a copy of the old code. The old `recordShowNotesDomain`
// promoted exactly those show-notes domains whose frequency reached the
// threshold, and left every other entry alone — so the old show-owned set is
// EXACTLY `showOwned ∪ ownershipUndetermined` from today's
// `EpisodeMetadataSnapshot.domainOwnership`. `assertBeforeArmMatchesHistory`
// pins that identity against the graph's own frequency table rather than
// asserting it in prose.
//
// STAGING: write the JSON export's path into `<repo>/.kmw4-corpus-path` (both
// pointer files are gitignored). The corpus is NOT in the repo — 17.8 MB of a
// device's transcripts — so with no pointer this SKIPS, the same shape as the
// `PLAYHEAD_X7RK_CORPUS` / `PLAYHEAD_RTY3_CORPUS` corpus lanes. A path in
// `<repo>/.kmw4-report-path` writes the full report as JSON. See
// `corpusPath()` for why this is a file and not an exported variable.
//
// WHAT IT CANNOT SEE — read before quoting any number:
//  * It measures the LEXICAL half of the pipeline (metadata lexicon entries,
//    the hits they produce, the candidates they weight, and the auto-ad rule
//    they suppress). Removing a `.showOwnedDomain` cue ALSO removes its
//    contribution to `FeedDescriptionEvidenceBuilder` (cue-type weight 0.05,
//    an ad-evidence term). That is measured separately here as a ledger-weight
//    delta, but the fusion gate that consumes it is not re-run.
//  * `ad_windows` on the device were produced by the shipped pipeline. Nothing
//    here re-derives them, so "a decision changes" means "an input to the
//    decision changes by this much", not "the device would have skipped".

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
        let episodes: Int
        let showOwnedBefore: [String]
        let showOwnedAfter: [String]
        let ownershipUndetermined: [String]
        let droppedFromShowOwned: [String]
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
        let negativeEntrySources: [String]
        let metadataHitsBefore: Int
        let metadataHitsAfter: Int
        let negativeHitsBefore: Int
        let negativeHitsAfter: Int
        let negativeHitDomains: [String]
        let candidatesBefore: Int
        let candidatesAfter: Int
        /// Candidates whose confidence moved at all, per trust value.
        let candidatesChangedByTrust: [String: Int]
        let maxConfidenceDeltaByTrust: [String: Double]
        let autoAdSpansBefore: Int
        let autoAdSpansAfter: Int
        let autoAdSpansUnblocked: [String]
        let metadataLedgerWeightBefore: Double
        let metadataLedgerWeightAfter: Double
    }

    // MARK: - The eval

    func testDevicePullShowOwnedRemoval() throws {
        let corpus = try Self.loadCorpus()

        var showReports: [ShowReport] = []
        var beforeOwnership: [Set<String>] = []
        var afterOwnership: [Set<String>] = []
        var undetermined: [Set<String>] = []

        for show in corpus.shows {
            let feedURL = show.feedURL.flatMap(URL.init(string:))
            let recentMetadata = show.episodes.map { episode in
                FeedDescriptionMetadata(
                    feedDescription: episode.feedDescription,
                    feedSummary: episode.feedSummary,
                    sourceHashes: .init()
                )
            }
            let ownership = EpisodeMetadataSnapshot.domainOwnership(
                feedURL: feedURL,
                recentMetadata: recentMetadata,
                podcastId: show.podcastId ?? show.feedURL ?? "unknown"
            )
            let before = ownership.showOwned.union(ownership.ownershipUndetermined)

            try Self.assertBeforeArmMatchesHistory(
                feedURL: feedURL,
                recentMetadata: recentMetadata,
                podcastId: show.podcastId ?? show.feedURL ?? "unknown",
                reconstructedBefore: before
            )

            beforeOwnership.append(before)
            afterOwnership.append(ownership.showOwned)
            undetermined.append(ownership.ownershipUndetermined)
            showReports.append(ShowReport(
                title: show.title,
                feedURL: show.feedURL,
                feedDomain: show.feedURL.flatMap { DomainNormalizer.etld1(from: $0) },
                episodes: show.episodes.count,
                showOwnedBefore: before.sorted(),
                showOwnedAfter: ownership.showOwned.sorted(),
                ownershipUndetermined: ownership.ownershipUndetermined.sorted(),
                droppedFromShowOwned: before.subtracting(ownership.showOwned).sorted()
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
                networkOwnedDomains: []
            )
            let extractorAfter = MetadataCueExtractor(
                showOwnedDomains: afterOwnership[asset.showIndex],
                networkOwnedDomains: [],
                ownershipUndeterminedDomains: undetermined[asset.showIndex]
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
            var maxDeltaByTrust: [String: Double] = [:]
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
                var maxDelta = 0.0
                for (lhs, rhs) in zip(before, after) {
                    let delta = rhs.confidence - lhs.confidence
                    if abs(delta) > 1e-12 { changed += 1 }
                    maxDelta = max(maxDelta, delta)
                    XCTAssertGreaterThanOrEqual(
                        delta, -1e-12,
                        "asset \(asset.id): removing NEGATIVE evidence cannot lower a confidence"
                    )
                }
                let key = String(format: "%.3f", trust)
                changedByTrust[key] = changed
                maxDeltaByTrust[key] = maxDelta
            }

            // The decision-level threshold this change can actually reach: a
            // negative-pattern hit anywhere in a span is a HARD suppressor of
            // `.lexicalAutoAd`, which is the only lexical route to auto-skip.
            var autoAdBefore = 0
            var autoAdAfter = 0
            var unblocked: [String] = []
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
                if !firedBefore && firedAfter { unblocked.append(span.id) }
                XCTAssertFalse(
                    firedBefore && !firedAfter,
                    "asset \(asset.id) span \(span.id): removing a suppressor cannot un-fire the auto-ad rule"
                )
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
                negativeEntrySources: negativeBefore.map(\.sourceValue).sorted(),
                metadataHitsBefore: metaHitsBefore.count,
                metadataHitsAfter: metaHitsAfter.count,
                negativeHitsBefore: negHitsBefore.count,
                negativeHitsAfter: negHitsAfter.count,
                negativeHitDomains: negHitsBefore.map { $0.matchedText.lowercased() }.sorted(),
                candidatesBefore: candidatesBefore,
                candidatesAfter: candidatesAfter,
                candidatesChangedByTrust: changedByTrust,
                maxConfidenceDeltaByTrust: maxDeltaByTrust,
                autoAdSpansBefore: autoAdBefore,
                autoAdSpansAfter: autoAdAfter,
                autoAdSpansUnblocked: unblocked,
                metadataLedgerWeightBefore: ledgerBefore,
                metadataLedgerWeightAfter: ledgerAfter
            ))
        }

        // The bead's headline claim, asserted rather than narrated: after the
        // change, NOTHING on this device injects a negative metadata pattern.
        let negAfter = assetReports.reduce(0) { $0 + $1.negativeEntriesAfter }
        XCTAssertEqual(negAfter, 0, "a negative metadata entry survived the removal")

        Self.emit(corpus: corpus, shows: showReports, assets: assetReports)
    }

    // MARK: - Before-arm fidelity

    /// The "before" set is reconstructed, so prove the reconstruction against
    /// the graph's own frequency table: the old code promoted a domain iff its
    /// show-notes frequency reached the threshold, so the reconstructed set
    /// must be exactly {feed-URL domain} ∪ {domains at or above threshold}.
    private static func assertBeforeArmMatchesHistory(
        feedURL: URL?,
        recentMetadata: [FeedDescriptionMetadata],
        podcastId: String,
        reconstructedBefore: Set<String>
    ) throws {
        var graph = OwnershipGraph(podcastId: podcastId)
        if let feedURL {
            graph.ingestFeedURL(feedURL.absoluteString)
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
        let threshold = OwnershipGraphConfig.default.showNotesRecurrenceThreshold
        var historical = Set(graph.entries.values
            .filter { $0.source == .showNotesFrequency && $0.frequency >= threshold }
            .map(\.domain))
        historical.formUnion(graph.entries.values
            .filter { $0.label == .showOwned }
            .map(\.domain))
        XCTAssertEqual(
            historical, reconstructedBefore,
            "the reconstructed pre-kmw4 show-owned set does not match the frequency table"
        )
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
                let autoAdSpansUnblocked: Int
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
            autoAdSpansUnblocked: assets.reduce(0) { $0 + $1.autoAdSpansUnblocked.count },
            metadataLedgerWeightBefore: assets.reduce(0.0) { $0 + $1.metadataLedgerWeightBefore },
            metadataLedgerWeightAfter: assets.reduce(0.0) { $0 + $1.metadataLedgerWeightAfter }
        )

        print("[kmw4] source=\(corpus.source)")
        for show in shows {
            print("[kmw4] show \"\(show.title ?? "?")\" feed=\(show.feedURL ?? "?") "
                  + "feedDomain=\(show.feedDomain ?? "nil") episodes=\(show.episodes)")
            print("[kmw4]   showOwned BEFORE (\(show.showOwnedBefore.count)) "
                  + "AFTER (\(show.showOwnedAfter.count)): \(show.showOwnedAfter)")
            print("[kmw4]   dropped (\(show.droppedFromShowOwned.count)): \(show.droppedFromShowOwned)")
        }
        for asset in assets where asset.negativeEntriesBefore > 0 || asset.negativeHitsBefore > 0 {
            print("[kmw4] asset \(asset.assetId) \"\(asset.title ?? "?")\" "
                  + "negEntries \(asset.negativeEntriesBefore)->\(asset.negativeEntriesAfter) "
                  + "\(asset.negativeEntrySources) "
                  + "negHits \(asset.negativeHitsBefore)->\(asset.negativeHitsAfter) "
                  + "\(asset.negativeHitDomains) "
                  + "cand \(asset.candidatesBefore)/\(asset.candidatesAfter) "
                  + "changed \(asset.candidatesChangedByTrust) maxΔ \(asset.maxConfidenceDeltaByTrust) "
                  + "autoAd \(asset.autoAdSpansBefore)->\(asset.autoAdSpansAfter) "
                  + "ledger \(asset.metadataLedgerWeightBefore)->\(asset.metadataLedgerWeightAfter)")
        }
        print("[kmw4] TOTALS assets=\(totals.assets) spans=\(totals.spans)")
        print("[kmw4] TOTALS metadataEntries \(totals.metadataEntriesBefore)->\(totals.metadataEntriesAfter) "
              + "negativeEntries \(totals.negativeEntriesBefore)->\(totals.negativeEntriesAfter)")
        print("[kmw4] TOTALS metadataHits \(totals.metadataHitsBefore)->\(totals.metadataHitsAfter) "
              + "negativeHits \(totals.negativeHitsBefore)->\(totals.negativeHitsAfter)")
        print("[kmw4] TOTALS candidates \(totals.candidatesBefore)->\(totals.candidatesAfter) "
              + "autoAdSpans \(totals.autoAdSpansBefore)->\(totals.autoAdSpansAfter) "
              + "unblocked=\(totals.autoAdSpansUnblocked)")
        print("[kmw4] TOTALS metadataLedgerWeight \(totals.metadataLedgerWeightBefore)"
              + "->\(totals.metadataLedgerWeightAfter)")

        guard let out = reportPath() else { return }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let report = Report(source: corpus.source, shows: shows, assets: assets, totals: totals)
        do {
            try encoder.encode(report).write(to: URL(fileURLWithPath: out))
            print("[kmw4] wrote \(out)")
        } catch {
            print("[kmw4] could not write \(out): \(error)")
        }
    }
}
