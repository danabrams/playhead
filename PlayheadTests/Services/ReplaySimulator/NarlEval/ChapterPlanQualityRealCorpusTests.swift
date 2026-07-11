// ChapterPlanQualityRealCorpusTests.swift
// playhead-au2v.1.22: real-corpus scaffold for the ChapterPlan quality
// evaluator. The dogfood goldens under
// `PlayheadTests/Fixtures/ChapterPlanGoldenSet/dogfood/` are auto-converted
// from the hand-labeled annotations in `TestFixtures/Corpus/Annotations/`
// by `Scripts/convert_annotations_to_chapter_goldens.py`. Topic labels
// are anonymized to ad_type / "editorial content" — no advertiser,
// product, or confidence-note text is committed (au2v.1.22 privacy rule).
//
// playhead-au2v.1.23: harness flipped from the empty-plan placeholder
// to real threshold assertions. When a captured pipeline-output
// `ChapterPlan` exists at
// `PlayheadTests/Fixtures/ChapterPlanGoldenSet/pipeline-snapshot/<episode_id>.json`,
// the harness runs the evaluator against that plan and asserts
// per-episode and aggregate floors (recall ≥ 0.6, precision ≥ 0.5,
// disposition accuracy ≥ 0.7). When the snapshot is absent for an
// episode, the harness falls back to the empty-plan zero-contract
// assertions for THAT one episode so checkouts without captured
// snapshots still run the suite green (capture is gated on a separate
// env var; see `ChapterPlanPipelineSnapshotCaptureTests`).
//
// Suites in this file:
//   * ChapterPlanGoldenSetDogfoodCorpusTests — corpus hygiene: every
//     dogfood fixture decodes, episodeIds and contentHashes are unique,
//     contentHash is a SHA-256 hex string, chapters are sorted by
//     startTimeSeconds, and the anonymization invariant holds (topic
//     labels come from a fixed allowlist).
//   * ChapterPlanPipelineSnapshotHygieneTests — snapshot hygiene: any
//     captured snapshot on disk roundtrips through the loader, its
//     `episodeContentHash` matches the paired golden, and its
//     chapter `title` strings do not include words from the
//     advertiser/product allowlist (privacy guard for the snapshot
//     output — see test docstring for the invariant we can actually
//     assert at this layer).
//   * ChapterPlanQualityRealCorpusHarnessTests — end-to-end harness.
//     Per-episode: when a captured snapshot is present, evaluate and
//     assert the threshold floors; when absent, fall back to the
//     empty-plan zero-contract pin. Aggregate: when at least one
//     snapshot is present, assert the aggregate thresholds across the
//     snapshot subset.

import Darwin
import Foundation
import Testing
@testable import Playhead

// MARK: - Anonymized topic-label allowlist
//
// `Scripts/convert_annotations_to_chapter_goldens.py` emits topic labels
// from this exact set. Asserting membership here is the privacy
// regression guard: if the converter (or a hand edit) ever leaks raw
// advertiser/product text into a committed fixture, this set check
// will catch it.
private let anonymizedTopicLabels: Set<String> = [
    "host-read sponsor",
    "blended host-read",
    "dynamic insertion",
    "pre-recorded spot",
    "advertisement",
    "editorial content"
]

private let sha256HexCharacterSet: CharacterSet = {
    let set = CharacterSet(charactersIn: "0123456789abcdef")
    return set
}()

// MARK: - Threshold floors (au2v.1.23)

/// Per-episode and aggregate quality thresholds the harness asserts
/// when a captured pipeline-output snapshot is available. Chosen from
/// the au2v.1.23 spec; tighten via a follow-up bead once the dogfood
/// corpus grows and the baseline distribution is observable.
///
/// These are intentionally lower than the synthetic-fixture thresholds
/// in `ChapterPlanQualityEvalTests` (which exercise perfect / near-
/// perfect plans) — the dogfood corpus includes structurally hard
/// episodes (no ads, very short ads, blended host-reads) where a real
/// FM-driven pipeline will inevitably miss boundaries.
enum ChapterPlanRealCorpusThresholds {
    /// Minimum boundary recall (matched goldens / total goldens).
    static let minRecall: Double = 0.6
    /// Minimum boundary precision (matched candidates / total
    /// candidates).
    static let minPrecision: Double = 0.5
    /// Minimum disposition accuracy across matched pairs.
    static let minDispositionAccuracy: Double = 0.7
}

// MARK: - Corpus hygiene

@Suite("ChapterPlanGoldenSet / dogfood corpus hygiene")
struct ChapterPlanGoldenSetDogfoodCorpusTests {

    @Test
    func dogfoodCorpus_matchesCurrentEligibleGoldCount() throws {
        let eligible = try CorpusAnnotationLoader()
            .loadAll(verifyAudioFingerprints: false)
            .filter(\.isEligibleForGoldEvaluation)
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        #expect(fixtures.count == eligible.count)
    }

    @Test
    func dogfoodCorpus_exactlyMatchesAllGoldAnnotations() throws {
        let loader = CorpusAnnotationLoader()
        let expectedCount = try loader.loadAll(verifyAudioFingerprints: false)
            .filter(\.isEligibleForGoldEvaluation)
            .count
        let fixtures = try ChapterPlanGoldenSetLoader.canonicalDogfoodFixtures(
            corpusLoader: loader
        )
        #expect(fixtures.count == expectedCount)
    }

    @Test
    func dogfoodReaderRejectsAliasedPointerAndMembers() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            "l2f-dogfood-reader-\(UUID().uuidString)",
            isDirectory: true
        )
        defer { try? FileManager.default.removeItem(at: root) }
        let filePath = root
            .appendingPathComponent("PlayheadTests/Services/ReplaySimulator/NarlEval")
            .appendingPathComponent("Fixture.swift")
        let parent = root
            .appendingPathComponent("PlayheadTests/Fixtures/ChapterPlanGoldenSet")
        try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
        let pointer = parent.appendingPathComponent("dogfood")
        let external = root.appendingPathComponent("external", isDirectory: true)
        try FileManager.default.createDirectory(at: external, withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(at: pointer, withDestinationURL: external)

        #expect(throws: CanonicalDogfoodCorpusError.self) {
            _ = try ChapterPlanGoldenSetLoader.allDogfoodFixtures(filePath.path)
        }

        try FileManager.default.removeItem(at: pointer)
        let generation = parent
            .appendingPathComponent(".dogfood-generations", isDirectory: true)
            .appendingPathComponent("empty", isDirectory: true)
        try FileManager.default.createDirectory(at: generation, withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(
            atPath: pointer.path,
            withDestinationPath: ".dogfood-generations/empty"
        )

        let retainedGeneration = generation.deletingLastPathComponent()
            .appendingPathComponent("retained", isDirectory: true)
        let swappedFixture = external.appendingPathComponent("swapped.json")
        try Data("{}".utf8).write(to: swappedFixture)
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures(
            filePath.path,
            afterGenerationOpen: {
                try FileManager.default.moveItem(at: generation, to: retainedGeneration)
                try FileManager.default.createSymbolicLink(
                    at: generation,
                    withDestinationURL: external
                )
            }
        )
        #expect(fixtures.isEmpty)
        try FileManager.default.removeItem(at: swappedFixture)
        try FileManager.default.removeItem(at: generation)
        try FileManager.default.moveItem(at: retainedGeneration, to: generation)

        #expect {
            _ = try ChapterPlanGoldenSetLoader.loadDogfood(
                named: "../../external/poison",
                filePath: filePath.path
            )
        } throws: { error in
            guard let corpusError = error as? CanonicalDogfoodCorpusError else {
                return false
            }
            return corpusError.detail.contains("unsafe dogfood golden fixture name")
        }
        let externalFixture = external.appendingPathComponent("poison.json")
        try Data("{}".utf8).write(to: externalFixture)
        try FileManager.default.createSymbolicLink(
            at: generation.appendingPathComponent("poison.json"),
            withDestinationURL: externalFixture
        )

        #expect(throws: CanonicalDogfoodCorpusError.self) {
            _ = try ChapterPlanGoldenSetLoader.allDogfoodFixtures(filePath.path)
        }
        #expect {
            _ = try ChapterPlanGoldenSetLoader.loadDogfood(
                named: "poison",
                filePath: filePath.path
            )
        } throws: { error in
            guard let corpusError = error as? CanonicalDogfoodCorpusError else {
                return false
            }
            return corpusError.detail.contains("not a regular file")
        }

        let fifo = generation.appendingPathComponent("blocking.json")
        #expect(mkfifo(fifo.path, S_IRUSR | S_IWUSR) == 0)
        #expect(throws: CanonicalDogfoodCorpusError.self) {
            _ = try ChapterPlanGoldenSetLoader.loadDogfood(
                named: "blocking",
                filePath: filePath.path
            )
        }

        let schemaFixture = generation.appendingPathComponent("schema.json")
        var fixture: [String: Any] = [
            "episodeId": "schema",
            "episodeContentHash": String(repeating: "a", count: 64),
            "chapters": [],
            "notes": ChapterPlanGoldenSetLoader.canonicalDogfoodNotes,
            "advertiser": "must never enter a dogfood golden",
        ]
        try JSONSerialization.data(withJSONObject: fixture).write(to: schemaFixture)
        #expect(throws: CanonicalDogfoodCorpusError.self) {
            _ = try ChapterPlanGoldenSetLoader.loadDogfood(
                named: "schema",
                filePath: filePath.path
            )
        }

        fixture.removeValue(forKey: "advertiser")
        fixture["notes"] = "private annotation notes"
        try JSONSerialization.data(withJSONObject: fixture).write(to: schemaFixture)
        #expect(throws: CanonicalDogfoodCorpusError.self) {
            _ = try ChapterPlanGoldenSetLoader.loadDogfood(
                named: "schema",
                filePath: filePath.path
            )
        }
    }

    @Test
    func dogfoodCorpus_rejectsDuplicateAliasesAndStaleLabels() throws {
        let loader = CorpusAnnotationLoader()
        let annotations = try loader.loadAll(verifyAudioFingerprints: false)
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        guard let first = fixtures.first else {
            #expect(annotations.allSatisfy { !$0.isEligibleForGoldEvaluation })
            #expect(try ChapterPlanGoldenSetLoader.validateCanonicalDogfoodFixtures(
                fixtures: [], annotations: annotations
            ).isEmpty)
            return
        }

        #expect(throws: CanonicalDogfoodCorpusError.self) {
            _ = try ChapterPlanGoldenSetLoader.validateCanonicalDogfoodFixtures(
                fixtures: fixtures + [first],
                annotations: annotations
            )
        }

        let aliasedURL = first.url.deletingLastPathComponent()
            .appendingPathComponent("stale-alias.json")
        #expect(throws: CanonicalDogfoodCorpusError.self) {
            _ = try ChapterPlanGoldenSetLoader.validateCanonicalDogfoodFixtures(
                fixtures: fixtures + [(url: aliasedURL, set: first.set)],
                annotations: annotations
            )
        }

        let changedFirstChapter = GoldenChapter(
            startTimeSeconds: first.set.chapters[0].startTimeSeconds + 1,
            expectedDisposition: first.set.chapters[0].expectedDisposition,
            expectedTopicLabel: first.set.chapters[0].expectedTopicLabel
        )
        let staleSet = GoldenChapterSet(
            episodeId: first.set.episodeId,
            episodeContentHash: first.set.episodeContentHash,
            chapters: [changedFirstChapter] + Array(first.set.chapters.dropFirst()),
            notes: first.set.notes
        )
        let staleFixtures = fixtures.map { fixture in
            fixture.url == first.url ? (url: fixture.url, set: staleSet) : fixture
        }
        #expect(throws: CanonicalDogfoodCorpusError.self) {
            _ = try ChapterPlanGoldenSetLoader.validateCanonicalDogfoodFixtures(
                fixtures: staleFixtures,
                annotations: annotations
            )
        }
    }

    @Test
    func dogfoodCorpus_episodeIdsAreUnique() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        let ids = fixtures.map(\.set.episodeId)
        #expect(Set(ids).count == ids.count, "duplicate episodeId in dogfood corpus")
    }

    @Test
    func dogfoodCorpus_contentHashesAreUniqueAndSha256() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        let hashes = fixtures.map(\.set.episodeContentHash)
        #expect(Set(hashes).count == hashes.count, "duplicate episodeContentHash in dogfood corpus")
        for hash in hashes {
            #expect(hash.count == 64, "expected 64-char SHA-256 hex, got \(hash.count): \(hash)")
            let invalid = hash.unicodeScalars.first { !sha256HexCharacterSet.contains($0) }
            #expect(invalid == nil, "non-hex character in contentHash: \(hash)")
        }
    }

    @Test
    func dogfoodCorpus_chaptersAreSortedByStartTime() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        for (url, set) in fixtures {
            let starts = set.chapters.map(\.startTimeSeconds)
            let sorted = starts.sorted()
            #expect(starts == sorted, "chapters not sorted in \(url.lastPathComponent)")
        }
    }

    @Test
    func dogfoodCorpus_topicLabelsAreAnonymized() throws {
        // Privacy regression guard. Every emitted topic label must be
        // drawn from the converter's anonymized allowlist; a label
        // outside that set means raw advertiser/product text leaked.
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        for (url, set) in fixtures {
            for chapter in set.chapters {
                guard let label = chapter.expectedTopicLabel else { continue }
                #expect(
                    anonymizedTopicLabels.contains(label),
                    "non-allowlisted topic label '\(label)' in \(url.lastPathComponent) — converter privacy invariant violated"
                )
            }
        }
    }

    @Test
    func dogfoodCorpus_eachChapterDispositionIsValid() throws {
        // Defense in depth: if the source annotation grows a new
        // ad_window field or the converter is hand-edited, ensure the
        // emitted disposition decodes to one of the three known cases.
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        let valid: Set<ChapterDisposition> = [.adBreak, .content, .ambiguous]
        for (url, set) in fixtures {
            for chapter in set.chapters {
                #expect(
                    valid.contains(chapter.expectedDisposition),
                    "unknown disposition in \(url.lastPathComponent): \(chapter.expectedDisposition)"
                )
            }
        }
    }
}

// MARK: - Snapshot hygiene (au2v.1.23)

@Suite("ChapterPlanGoldenSet / pipeline-snapshot hygiene")
struct ChapterPlanPipelineSnapshotHygieneTests {

    /// A few known advertiser / product brand tokens that MUST NEVER
    /// appear in a committed snapshot. The set is intentionally small
    /// and case-insensitive — we are guarding against the obvious
    /// "FM emitted a raw advertiser name into the topicDescriptor"
    /// failure mode, not enumerating every brand in existence. The
    /// list mirrors the advertisers referenced by the un-committed
    /// annotations under `TestFixtures/Corpus/Annotations/`; expand if
    /// future capture runs surface new ones.
    ///
    /// IMPORTANT: this list itself must not appear in any committed
    /// JSON. The strings live in the test source only.
    private static let advertiserBrandTokens: [String] = [
        "squarespace",
        "athletic greens",
        "bombas",
        "shopify",
        "betterhelp",
        "factor",
        "stamps",
        "indeed",
        "rocket money",
        "noom"
    ]

    /// Hygiene: each captured snapshot decodes and its content hash
    /// matches the paired golden. If a checkout has no captured
    /// snapshots, the test is vacuously true — that's intentional, the
    /// capture is a separate gated step.
    @Test
    func snapshots_decodeAndAlignToGoldens() throws {
        let pairs = try ChapterPlanGoldenSetLoader.allDogfoodFixturesWithSnapshots()
        for pair in pairs {
            if pair.snapshotPresentButMismatched {
                Issue.record(
                    """
                    Snapshot \(pair.snapshotURL.lastPathComponent) decoded but its \
                    episodeContentHash does not match the paired golden at \
                    \(pair.goldenURL.lastPathComponent). The snapshot is stale; \
                    re-run the capture (PLAYHEAD_CHAPTER_SNAPSHOT_CAPTURE=1).
                    """
                )
            }
        }
    }

    /// Privacy: even though `ChapterPlan` is derived from pipeline
    /// output (not from the annotation file), the FM labeler is free
    /// to emit a topic descriptor that echoes an advertiser brand it
    /// recognized in the transcript. We assert that no committed
    /// snapshot's chapter `title` contains any known advertiser brand
    /// token. The assertion is best-effort — a brand that isn't on the
    /// allowlist would slip past. The right place to fully enforce
    /// this is the capture test itself (which should refuse to commit
    /// snapshots that would trip this guard), but having a regression
    /// gate here surfaces the failure deterministically on every test
    /// run as well.
    @Test
    func snapshots_topicTitlesDoNotLeakAdvertiserBrands() throws {
        let pairs = try ChapterPlanGoldenSetLoader.allDogfoodFixturesWithSnapshots()
        for pair in pairs {
            guard let snapshot = pair.snapshot else { continue }
            for (index, chapter) in snapshot.chapters.enumerated() {
                guard let title = chapter.title, !title.isEmpty else { continue }
                let lowered = title.lowercased()
                for token in Self.advertiserBrandTokens {
                    if lowered.contains(token) {
                        Issue.record(
                            """
                            Snapshot \(pair.snapshotURL.lastPathComponent) chapter[\(index)] \
                            title='\(title)' contains advertiser token '\(token)'. \
                            Privacy invariant violated; abort the commit and recapture.
                            """
                        )
                    }
                }
            }
        }
    }
}

// MARK: - Harness wiring against real corpus

@Suite("ChapterPlanQualityEval / real-corpus harness")
struct ChapterPlanQualityRealCorpusHarnessTests {

    /// Per-episode harness: when a captured pipeline snapshot exists,
    /// evaluate against it and assert the threshold floors. When
    /// missing, retain the empty-plan zero-contract pin from au2v.1.22
    /// so the loader → evaluator path stays exercised even before
    /// snapshots are captured.
    @Test
    func eachDogfoodGolden_meetsThresholdsWhenSnapshotPresent_zeroContractOtherwise() throws {
        let pairs = try ChapterPlanGoldenSetLoader.allDogfoodFixturesWithSnapshots()
        guard !pairs.isEmpty else {
            let annotations = try CorpusAnnotationLoader().loadAll(verifyAudioFingerprints: false)
            #expect(annotations.allSatisfy { !$0.isEligibleForGoldEvaluation })
            return
        }

        let evaluator = ChapterPlanQualityEval()

        for pair in pairs {
            if let snapshot = pair.snapshot {
                assertSnapshotPerEpisodeThresholds(
                    evaluator: evaluator,
                    plan: snapshot,
                    golden: pair.golden,
                    snapshotName: pair.snapshotURL.lastPathComponent
                )
            } else {
                assertEmptyPlanZeroContract(
                    evaluator: evaluator,
                    golden: pair.golden,
                    goldenName: pair.goldenURL.lastPathComponent,
                    snapshotPresentButMismatched: pair.snapshotPresentButMismatched
                )
            }
        }
    }

    /// Aggregate harness: when at least one snapshot is present, sum
    /// counts across that subset and assert the same threshold floors.
    /// The aggregate gate catches a regression where ONE episode
    /// passes the per-episode floor narrowly but the corpus as a whole
    /// has slipped — e.g. precision is 0.51 on every episode but the
    /// aggregate is just above 0.5; a future model regression that
    /// drops aggregate precision below 0.5 fires this gate before any
    /// single per-episode gate would.
    ///
    /// When NO snapshots are present, the test is vacuously skipped
    /// (we have nothing to aggregate). The per-episode test above
    /// still pins the empty-plan contract for every fixture, so
    /// loader correctness remains under test.
    @Test
    func aggregate_acrossCapturedSnapshots_meetsThresholds() throws {
        let pairs = try ChapterPlanGoldenSetLoader.allDogfoodFixturesWithSnapshots()
        let captured = pairs.compactMap { pair -> (plan: ChapterPlan, golden: GoldenChapterSet, name: String)? in
            guard let snapshot = pair.snapshot else { return nil }
            return (plan: snapshot, golden: pair.golden, name: pair.snapshotURL.lastPathComponent)
        }
        guard !captured.isEmpty else {
            // No snapshots captured yet — nothing to aggregate. The
            // capture-test docstring documents how to produce them.
            return
        }

        let evaluator = ChapterPlanQualityEval()
        let report = evaluator.evaluate(pairs: captured.map { ($0.plan, $0.golden) })

        let recallFraction = report.boundaryRecall.fraction
        let precisionFraction = report.boundaryPrecision.fraction
        let dispositionFraction = report.dispositionAccuracy
        let names = captured.map(\.name).sorted().joined(separator: ", ")

        #expect(
            recallFraction >= ChapterPlanRealCorpusThresholds.minRecall,
            """
            Aggregate boundary recall \(recallFraction) below floor \
            \(ChapterPlanRealCorpusThresholds.minRecall) across snapshots [\(names)]. \
            matched=\(report.boundaryRecall.matched)/total=\(report.boundaryRecall.total). \
            Run the capture and inspect per-episode breakdowns in the failure output \
            of the per-episode test to triage.
            """
        )
        #expect(
            precisionFraction >= ChapterPlanRealCorpusThresholds.minPrecision,
            """
            Aggregate boundary precision \(precisionFraction) below floor \
            \(ChapterPlanRealCorpusThresholds.minPrecision) across snapshots [\(names)]. \
            matched=\(report.boundaryPrecision.matched)/total=\(report.boundaryPrecision.total).
            """
        )
        #expect(
            dispositionFraction >= ChapterPlanRealCorpusThresholds.minDispositionAccuracy,
            """
            Aggregate disposition accuracy \(dispositionFraction) below floor \
            \(ChapterPlanRealCorpusThresholds.minDispositionAccuracy) across snapshots [\(names)]. \
            agreed=\(report.dispositionMatchedAgreed)/pairs=\(report.dispositionMatchedPairs).
            """
        )
    }

    // MARK: - Per-fixture helpers

    /// Threshold-mode assertions for one (plan, golden) pair. Each
    /// failure embeds the snapshot filename and the underlying
    /// matched/total counts so failure output is diff-actionable on a
    /// real regression.
    private func assertSnapshotPerEpisodeThresholds(
        evaluator: ChapterPlanQualityEval,
        plan: ChapterPlan,
        golden: GoldenChapterSet,
        snapshotName: String
    ) {
        let perEp = evaluator.evaluateEpisode(plan: plan, golden: golden)
        let recall = perEp.boundaryRecall.fraction
        let precision = perEp.boundaryPrecision.fraction
        let disposition = perEp.dispositionAccuracy

        #expect(
            recall >= ChapterPlanRealCorpusThresholds.minRecall,
            """
            Per-episode boundary recall \(recall) below floor \
            \(ChapterPlanRealCorpusThresholds.minRecall) for snapshot \(snapshotName). \
            matched=\(perEp.boundaryRecall.matched)/total=\(perEp.boundaryRecall.total) \
            missed=\(perEp.missedBoundaries) fp=\(perEp.falsePositiveBoundaries) \
            pairs=\(perEp.dispositionMatchedPairs) agreed=\(perEp.dispositionMatchedAgreed).
            """
        )
        #expect(
            precision >= ChapterPlanRealCorpusThresholds.minPrecision,
            """
            Per-episode boundary precision \(precision) below floor \
            \(ChapterPlanRealCorpusThresholds.minPrecision) for snapshot \(snapshotName). \
            matched=\(perEp.boundaryPrecision.matched)/total=\(perEp.boundaryPrecision.total) \
            missed=\(perEp.missedBoundaries) fp=\(perEp.falsePositiveBoundaries).
            """
        )
        #expect(
            disposition >= ChapterPlanRealCorpusThresholds.minDispositionAccuracy,
            """
            Per-episode disposition accuracy \(disposition) below floor \
            \(ChapterPlanRealCorpusThresholds.minDispositionAccuracy) for snapshot \(snapshotName). \
            agreed=\(perEp.dispositionMatchedAgreed)/pairs=\(perEp.dispositionMatchedPairs). \
            confusion=\(perEp.perDispositionConfusion).
            """
        )
    }

    /// Empty-plan zero-contract pin (au2v.1.22 baseline). Used for any
    /// episode where the capture has not yet produced a committed
    /// snapshot. Continues to exercise loader→evaluator wiring without
    /// requiring captured snapshots.
    private func assertEmptyPlanZeroContract(
        evaluator: ChapterPlanQualityEval,
        golden: GoldenChapterSet,
        goldenName: String,
        snapshotPresentButMismatched: Bool
    ) {
        if snapshotPresentButMismatched {
            Issue.record(
                """
                Snapshot for \(goldenName) is present on disk but its \
                episodeContentHash mismatches the golden. Falling back to \
                empty-plan contract for this episode; recapture to recover \
                threshold-mode coverage.
                """
            )
        }
        let emptyPlan = ChapterPlan(
            episodeContentHash: golden.episodeContentHash,
            chapters: [],
            planConfidence: ChapterPlan.computePlanConfidence([]),
            generatedAt: Date(timeIntervalSince1970: 0)
        )
        let report = evaluator.evaluate(pairs: [(emptyPlan, golden)])

        #expect(
            report.boundaryRecall.matched == 0,
            "non-zero recall on empty plan for \(goldenName)"
        )
        #expect(
            report.boundaryRecall.total == golden.chapters.count,
            "recall denominator should be golden chapter count for \(goldenName)"
        )
        #expect(
            report.boundaryPrecision.matched == 0,
            "non-zero precision on empty plan for \(goldenName)"
        )
        #expect(
            report.boundaryPrecision.total == 0,
            "non-zero precision denominator on empty plan for \(goldenName)"
        )
        #expect(
            report.dispositionMatchedPairs == 0,
            "matched pairs should be zero with empty plan for \(goldenName)"
        )
    }
}

// MARK: - Loader-dispatch unit coverage (au2v.1.23)

/// Tests of the test infrastructure: pin the three-way dispatch in
/// `ChapterPlanGoldenSetLoader.pairFixturesWithSnapshots` so a future
/// edit to the loader cannot silently merge or mis-classify the
/// missing / matched-hash / mismatched-hash cases.
///
/// Uses a per-test temp directory and a hand-built `GoldenChapterSet`
/// so we exercise the dispatch without committing fake snapshots to
/// `Fixtures/`. The committed dogfood goldens stay untouched; the
/// production tests above continue to use them.
@Suite("ChapterPlanGoldenSetLoader / snapshot dispatch")
struct ChapterPlanGoldenSetSnapshotDispatchTests {

    /// Build a deterministic minimal `GoldenChapterSet` for a synthetic
    /// episode id, written to a temp file. Returns the tuple shape the
    /// loader's `pairFixturesWithSnapshots` accepts.
    private func makeGoldenFixture(
        episodeId: String,
        contentHash: String,
        in dir: URL
    ) throws -> (url: URL, set: GoldenChapterSet) {
        let set = GoldenChapterSet(
            episodeId: episodeId,
            episodeContentHash: contentHash,
            chapters: [
                GoldenChapter(
                    startTimeSeconds: 0.0,
                    expectedDisposition: .content,
                    expectedTopicLabel: "editorial content"
                )
            ],
            notes: nil
        )
        let url = dir.appendingPathComponent("\(episodeId).json", isDirectory: false)
        let data = try JSONEncoder().encode(set)
        try data.write(to: url, options: .atomic)
        return (url: url, set: set)
    }

    /// Build a `ChapterPlan` whose `episodeContentHash` is exactly the
    /// argument, write it to `snapshotDir/<episodeId>.json`, and return
    /// the URL so tests can assert on it.
    private func writeSnapshot(
        episodeId: String,
        contentHash: String,
        in snapshotDir: URL
    ) throws -> URL {
        let plan = ChapterPlan(
            episodeContentHash: contentHash,
            chapters: [],
            planConfidence: 0.0,
            generatedAt: Date(timeIntervalSince1970: 0)
        )
        let url = snapshotDir.appendingPathComponent("\(episodeId).json", isDirectory: false)
        let data = try JSONEncoder().encode(plan)
        try data.write(to: url, options: .atomic)
        return url
    }

    /// Build a `(goldenDir, snapshotDir)` pair under a unique temp
    /// directory so concurrent test runs don't collide. The `root` URL is
    /// returned so the caller can `defer FileManager.default.removeItem`
    /// the whole tree after the test body and avoid leaking UUID-named
    /// directories under `NSTemporaryDirectory()` across runs.
    private func makeTempDirs() throws -> (root: URL, golden: URL, snapshot: URL) {
        let root = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("ChapterPlanGoldenSetSnapshotDispatch", isDirectory: true)
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let golden = root.appendingPathComponent("dogfood", isDirectory: true)
        let snapshot = root.appendingPathComponent("pipeline-snapshot", isDirectory: true)
        try FileManager.default.createDirectory(at: golden, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: snapshot, withIntermediateDirectories: true)
        return (root: root, golden: golden, snapshot: snapshot)
    }

    /// State 1: snapshot file is absent → `snapshot == nil` AND
    /// `snapshotPresentButMismatched == false`. The "no capture yet"
    /// case must be cleanly distinguishable from the "stale capture"
    /// case so harness output can tell operators what to do.
    @Test
    func dispatch_snapshotMissing() throws {
        let dirs = try makeTempDirs()
        defer { try? FileManager.default.removeItem(at: dirs.root) }
        let golden = try makeGoldenFixture(
            episodeId: "synthetic-missing",
            contentHash: "synthetic-missing-hash",
            in: dirs.golden
        )

        let paired = try ChapterPlanGoldenSetLoader.pairFixturesWithSnapshots(
            fixtures: [golden],
            snapshotDir: dirs.snapshot
        )

        try #require(paired.count == 1)
        let pair = paired[0]
        #expect(pair.snapshot == nil, "snapshot should be nil when file is absent")
        #expect(pair.snapshotPresentButMismatched == false, "absence != mismatch")
        #expect(pair.golden == golden.set, "golden round-trips through pairing")
    }

    /// State 2: snapshot file is present and its `episodeContentHash`
    /// matches the golden's → `snapshot != nil` AND the snapshot's hash
    /// equals the golden's. This is the happy path the threshold-mode
    /// harness consumes.
    @Test
    func dispatch_snapshotPresentMatchingHash() throws {
        let dirs = try makeTempDirs()
        defer { try? FileManager.default.removeItem(at: dirs.root) }
        let hash = "synthetic-match-hash"
        let golden = try makeGoldenFixture(
            episodeId: "synthetic-match",
            contentHash: hash,
            in: dirs.golden
        )
        _ = try writeSnapshot(
            episodeId: "synthetic-match",
            contentHash: hash,
            in: dirs.snapshot
        )

        let paired = try ChapterPlanGoldenSetLoader.pairFixturesWithSnapshots(
            fixtures: [golden],
            snapshotDir: dirs.snapshot
        )

        try #require(paired.count == 1)
        let pair = paired[0]
        let snapshot = try #require(pair.snapshot, "snapshot should decode and be returned")
        #expect(snapshot.episodeContentHash == hash)
        #expect(pair.snapshotPresentButMismatched == false)
    }

    /// State 3: snapshot file is present but its `episodeContentHash`
    /// differs from the golden's → `snapshot == nil` AND
    /// `snapshotPresentButMismatched == true`. Critical invariant: a
    /// stale snapshot must NOT silently feed into threshold assertions
    /// — the loader nulls the snapshot out and the boolean flag makes
    /// the mismatch observable to the harness.
    @Test
    func dispatch_snapshotPresentMismatchingHash() throws {
        let dirs = try makeTempDirs()
        defer { try? FileManager.default.removeItem(at: dirs.root) }
        let golden = try makeGoldenFixture(
            episodeId: "synthetic-mismatch",
            contentHash: "golden-hash-aaaaaaaa",
            in: dirs.golden
        )
        _ = try writeSnapshot(
            episodeId: "synthetic-mismatch",
            contentHash: "snapshot-hash-bbbbbbbb",
            in: dirs.snapshot
        )

        let paired = try ChapterPlanGoldenSetLoader.pairFixturesWithSnapshots(
            fixtures: [golden],
            snapshotDir: dirs.snapshot
        )

        try #require(paired.count == 1)
        let pair = paired[0]
        #expect(pair.snapshot == nil, "stale snapshot must NOT surface as authoritative")
        #expect(pair.snapshotPresentButMismatched == true, "mismatch flag must surface")
    }

    /// Mixed batch: missing + matching + mismatched in one call. Pins
    /// that the dispatch is per-fixture, not all-or-nothing — a stale
    /// snapshot for ONE episode does not poison the result for the
    /// others.
    @Test
    func dispatch_mixedBatchHandlesEachFixtureIndependently() throws {
        let dirs = try makeTempDirs()
        defer { try? FileManager.default.removeItem(at: dirs.root) }
        let missing = try makeGoldenFixture(
            episodeId: "synthetic-missing",
            contentHash: "h-missing",
            in: dirs.golden
        )
        let matched = try makeGoldenFixture(
            episodeId: "synthetic-matched",
            contentHash: "h-matched",
            in: dirs.golden
        )
        let mismatched = try makeGoldenFixture(
            episodeId: "synthetic-mismatched",
            contentHash: "h-golden",
            in: dirs.golden
        )
        _ = try writeSnapshot(
            episodeId: "synthetic-matched",
            contentHash: "h-matched",
            in: dirs.snapshot
        )
        _ = try writeSnapshot(
            episodeId: "synthetic-mismatched",
            contentHash: "h-stale",
            in: dirs.snapshot
        )

        let paired = try ChapterPlanGoldenSetLoader.pairFixturesWithSnapshots(
            fixtures: [missing, matched, mismatched],
            snapshotDir: dirs.snapshot
        )
        let byId = Dictionary(uniqueKeysWithValues: paired.map { ($0.golden.episodeId, $0) })

        let pMissing = try #require(byId["synthetic-missing"])
        #expect(pMissing.snapshot == nil)
        #expect(pMissing.snapshotPresentButMismatched == false)

        let pMatched = try #require(byId["synthetic-matched"])
        #expect(pMatched.snapshot != nil)
        #expect(pMatched.snapshotPresentButMismatched == false)

        let pMismatched = try #require(byId["synthetic-mismatched"])
        #expect(pMismatched.snapshot == nil)
        #expect(pMismatched.snapshotPresentButMismatched == true)
    }

    /// A snapshot file that exists but contains malformed JSON must
    /// surface as a decode error (not silently become a "missing"
    /// state). This guards against a half-written capture run producing
    /// a stale-but-decodable result on the next test run.
    @Test
    func dispatch_snapshotPresentButMalformedThrows() throws {
        let dirs = try makeTempDirs()
        defer { try? FileManager.default.removeItem(at: dirs.root) }
        let golden = try makeGoldenFixture(
            episodeId: "synthetic-bad",
            contentHash: "h-bad",
            in: dirs.golden
        )
        let snapshotURL = dirs.snapshot
            .appendingPathComponent("synthetic-bad.json", isDirectory: false)
        try Data("not valid json".utf8).write(to: snapshotURL, options: .atomic)

        #expect(throws: (any Error).self) {
            _ = try ChapterPlanGoldenSetLoader.pairFixturesWithSnapshots(
                fixtures: [golden],
                snapshotDir: dirs.snapshot
            )
        }
    }
}
