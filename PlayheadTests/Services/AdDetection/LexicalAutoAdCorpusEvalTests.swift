// LexicalAutoAdCorpusEvalTests.swift
// playhead-xsdz.1: Deterministic corpus recall/precision eval for the
// high-precision lexical auto-ad rule.
//
// What this measures (and why it needs no FM)
// -------------------------------------------
// The lexical auto-ad rule is a PURE local regex/timing computation. So this
// eval is fully deterministic: it loads the dogfood-corpus whisper
// transcripts (`TestFixtures/Corpus/Transcripts/<id>.json` via
// `CorpusTranscriptLoader`) and golden ad windows
// (`CorpusAnnotation.adWindows` via `CorpusAnnotationLoader`), runs the
// `LexicalScanner` + `LexicalAutoAdEvidenceBuilder` over tiled candidate
// spans, and scores predicted-vs-golden coverage with the existing
// `SpanMetrics` / `FusionLiftScoring` harness.
//
// It demonstrates the keystone claim of epic playhead-xsdz: lexical evidence,
// converted into auto-ad decisions, recovers ad-region recall MATERIALLY
// above the ~21% FM ad-region-recall baseline — while keeping precision
// controlled (no false-positive collapse).
//
// Gating: the corpus transcripts are git-ignored and may not be staged
// locally. When `TestFixtures/Corpus/Transcripts/` has no usable sidecars the
// test SKIPS cleanly (it is NOT a green-gate failure). The always-on coverage
// for the rule is the hermetic `LexicalAutoAdEvidenceBuilderTests`.

import Foundation
import XCTest
@testable import Playhead

final class LexicalAutoAdCorpusEvalTests: XCTestCase {

    /// FM ad-region recall baseline from the 2026-04-23 real-data eval that
    /// motivated epic playhead-xsdz. The lexical auto-ad rule must beat this
    /// by a material margin to justify the keystone.
    private static let fmAdRegionRecallBaseline = 0.21

    /// Ad-region (span) recall floor we assert: materially above the FM
    /// baseline. Set conservatively below the observed value so transcript
    /// jitter does not flake the gate, but well above 0.21 so "materially
    /// above" is real.
    private static let recallFloor = 0.30

    /// Coverage-precision floor. Precision is paramount: a false-positive ad
    /// means real content gets wrongly skipped. We require the predicted ad
    /// seconds to be majority-inside golden ad windows.
    private static let coveragePrecisionFloor = 0.50

    /// Repo root derived from THIS file's `#filePath`. We compute it
    /// explicitly rather than relying on `CorpusAnnotationLoader`'s default
    /// `#filePath` parent-walk: that default binds the parent count to the
    /// loader file's directory depth, and this test file lives at a DIFFERENT
    /// depth (`PlayheadTests/Services/AdDetection/`), so the default walk
    /// would over-shoot the repo root by one level. Walking up the right
    /// number of parents here keeps the corpus resolution correct regardless
    /// of where this test file sits.
    private static func repoRoot(filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()  // AdDetection/
            .deletingLastPathComponent()  // Services/
            .deletingLastPathComponent()  // PlayheadTests/
            .deletingLastPathComponent()  // <repo root>
    }

    /// Gold is evaluated only on episodes whose entire label set is gold.
    /// Otherwise a prediction over a real silver/proposal window would be
    /// counted as a gold false positive.
    private static func evaluationTiers(
        for annotation: CorpusAnnotation
    ) -> Set<CorpusAnnotation.LabelTier> {
        var tiers = Set(annotation.adWindows.map { annotation.labelTier(for: $0) })
        if annotation.isEligibleForGoldEvaluation {
            tiers.insert(.gold) // Retain legacy no-ad episodes as controls.
        } else {
            tiers.remove(.gold)
        }
        return tiers
    }

    /// A checkout may omit the corpus entirely, but a present corpus must
    /// satisfy the canonical-manifest contract. Do not turn manifest drift or
    /// corruption into a green skip.
    private static func annotationURLsForEvaluation(
        using loader: CorpusAnnotationLoader
    ) throws -> [URL] {
        do {
            return try loader.annotationFileURLs()
        } catch CorpusAnnotationLoaderError.directoryNotFound(let url) {
            throw XCTSkip("corpus annotations dir not present: \(url.path)")
        }
    }

    private static func verifyDiagnosticAudioBinding(
        annotation: CorpusAnnotation,
        annotationURL: URL,
        loader: CorpusAnnotationLoader
    ) throws {
        try loader.verify(audioFingerprintFor: annotation, jsonURL: annotationURL)
    }

    /// Per-tier diagnostics are recall-only because one episode can contain
    /// labels from multiple non-gold tiers. Precision against only one tier
    /// would misclassify detections of the episode's other valid ads as false
    /// positives.
    private static func recallDiagnosticLine(
        tier: CorpusAnnotation.LabelTier,
        groundTruth: [MetricGroundTruthAd],
        detections: [MetricDetectedAd],
        episodeCount: Int
    ) -> String {
        let batch = MetricsBatch.pair(groundTruth: groundTruth, detections: detections)
        let spanRecall = SpanF1(batch: batch).recall
        let coverageRecall = batch.computeCoverageRecall()
        return "\(tier.rawValue): episodes=\(episodeCount) "
            + "windows=\(groundTruth.count) predictions=\(detections.count) "
            + "spanRecall=\(spanRecall.map { String(format: "%.3f", $0) } ?? "n/a") "
            + "coverageRecall=\(coverageRecall.map { String(format: "%.3f", $0) } ?? "n/a")"
    }

    func testMixedQualityEpisodeCannotEnterGoldEvaluation() {
        let gold = CorpusAnnotation.AdWindow(
            startSeconds: 10,
            endSeconds: 20,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil
        )
        let silver = CorpusAnnotation.AdWindow(
            startSeconds: 30,
            endSeconds: 40,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil,
            autoPromoted: true,
            provenance: ["rediff"],
            auditPriority: 3
        )
        let mixed = CorpusAnnotation(
            episodeId: "mixed",
            showName: "Mixed",
            durationSeconds: 50,
            adWindows: [gold, silver],
            contentWindows: [],
            variantOf: nil,
            audioFingerprint: "sha256:" + String(repeating: "a", count: 64)
        )

        XCTAssertEqual(Self.evaluationTiers(for: mixed), [.silver])
    }

    func testMixedTierDiagnosticsRemainRecallOnly() {
        let silverWindow = CorpusAnnotation.AdWindow(
            startSeconds: 10,
            endSeconds: 20,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil,
            autoPromoted: true,
            provenance: ["drafter", "rediff"],
            auditPriority: 3
        )
        let detections = [
            MetricDetectedAd(
                id: "silver-detection",
                podcastId: "Mixed",
                episodeId: "mixed",
                startTime: 10,
                endTime: 20,
                path: .backfill,
                firstConfirmationTime: nil,
                confidence: 1
            ),
            MetricDetectedAd(
                id: "proposal-detection",
                podcastId: "Mixed",
                episodeId: "mixed",
                startTime: 30,
                endTime: 40,
                path: .backfill,
                firstConfirmationTime: nil,
                confidence: 1
            ),
        ]
        let groundTruth = [
            MetricGroundTruthAd(
                annotationWindow: silverWindow,
                id: "silver-ground-truth",
                podcastId: "Mixed",
                episodeId: "mixed"
            ),
        ]

        let line = Self.recallDiagnosticLine(
            tier: .silver,
            groundTruth: groundTruth,
            detections: detections,
            episodeCount: 1
        )

        XCTAssertTrue(line.contains("spanRecall=1.000"))
        XCTAssertTrue(line.contains("coverageRecall=1.000"))
        XCTAssertFalse(line.contains("Precision"))
    }

    func testCanonicalManifestFailuresCannotBecomeEvaluationSkips() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            "lexical-corpus-manifest-\(UUID().uuidString)"
        )
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(
            at: root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath),
            withIntermediateDirectories: true
        )
        try Data("gitdir: /tmp/not-used\n".utf8).write(to: root.appendingPathComponent(".git"))

        XCTAssertThrowsError(
            try Self.annotationURLsForEvaluation(using: CorpusAnnotationLoader(repoRoot: root))
        ) { error in
            guard case CorpusAnnotationLoaderError.manifestMissing = error else {
                return XCTFail("expected manifestMissing, got \(error)")
            }
        }
    }

    func testDiagnosticTiersRejectStaleAnnotationCoordinates() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            "lexical-corpus-binding-\(UUID().uuidString)"
        )
        defer { try? FileManager.default.removeItem(at: root) }
        let audioDirectory = root.appendingPathComponent(
            CorpusAnnotationLoader.audioRelativePath,
            isDirectory: true
        )
        try FileManager.default.createDirectory(
            at: audioDirectory,
            withIntermediateDirectories: true
        )
        try Data("current B audio".utf8).write(
            to: audioDirectory.appendingPathComponent("silver.mp3")
        )
        let staleFingerprint = CorpusAudioFingerprint.fingerprint(
            of: Data("old A audio".utf8)
        )
        let annotation = CorpusAnnotation(
            episodeId: "silver",
            showName: "Silver",
            durationSeconds: 10,
            adWindows: [],
            contentWindows: [
                .init(startSeconds: 0, endSeconds: 10, notes: nil),
            ],
            variantOf: nil,
            audioFingerprint: staleFingerprint,
            provenance: ["human_first_pass"]
        )
        let annotationURL = root.appendingPathComponent("silver.json")

        XCTAssertThrowsError(
            try Self.verifyDiagnosticAudioBinding(
                annotation: annotation,
                annotationURL: annotationURL,
                loader: CorpusAnnotationLoader(repoRoot: root)
            )
        ) { error in
            guard case CorpusAnnotationLoaderError.fingerprintMismatch = error else {
                return XCTFail("expected exact annotation/audio mismatch, got \(error)")
            }
        }
    }

    func testLexicalAutoAdRecallAndPrecisionOnGoldenCorpus() throws {
        let loader = CorpusAnnotationLoader(repoRoot: Self.repoRoot())

        // Enumerate annotations; skip cleanly if the corpus dir is absent.
        let annotationURLs = try Self.annotationURLsForEvaluation(using: loader)
        try XCTSkipIf(annotationURLs.isEmpty, "no corpus annotations staged")
        let canonicalAnnotations = try loader.loadAll(verifyAudioFingerprints: false)
        if canonicalAnnotations.allSatisfy({ !$0.isEligibleForGoldEvaluation }) {
            XCTAssertThrowsError(
                try loader.preflightGoldEvaluationInputs(annotationURLs: annotationURLs)
            ) { error in
                guard case CorpusAnnotationLoaderError.evaluationCohortIncomplete(let detail) = error else {
                    return XCTFail("expected explicit no-gold preflight failure, got \(error)")
                }
                XCTAssertTrue(detail.contains("no explicitly human-reviewed gold"))
            }
            return
        }
        try loader.preflightGoldEvaluationInputs(annotationURLs: annotationURLs)

        let scanner = LexicalScanner()
        let builder = LexicalAutoAdEvidenceBuilder()

        var groundTruthByTier = Dictionary(
            uniqueKeysWithValues: CorpusAnnotation.LabelTier.allCases.map { ($0, [MetricGroundTruthAd]()) }
        )
        var detectionsByTier = Dictionary(
            uniqueKeysWithValues: CorpusAnnotation.LabelTier.allCases.map { ($0, [MetricDetectedAd]()) }
        )
        var scoredEpisodesByTier = Dictionary(
            uniqueKeysWithValues: CorpusAnnotation.LabelTier.allCases.map { ($0, Set<String>()) }
        )
        var skipped: [(String, String)] = []

        for url in annotationURLs {
            let episodeId = url.deletingPathExtension().lastPathComponent

            let annotation = try loader.loadAndValidate(at: url)

            // Load transcript; absent/empty → skip this episode cleanly.
            let transcript = try CorpusTranscriptLoader.load(
                episodeId: episodeId,
                repoRoot: loader.repoRoot
            )
            guard !transcript.isEmpty else {
                skipped.append((episodeId, "transcript sidecar empty/absent"))
                continue
            }
            try Self.verifyDiagnosticAudioBinding(
                annotation: annotation,
                annotationURL: url,
                loader: loader
            )

            // Ground truth is stratified by provenance. A legacy/manual
            // no-ad episode remains in the gold cohort as a negative control.
            let episodeTiers = Self.evaluationTiers(for: annotation)
            for (i, window) in annotation.adWindows.enumerated() {
                let tier = annotation.labelTier(for: window)
                guard episodeTiers.contains(tier) else { continue }
                groundTruthByTier[tier, default: []].append(MetricGroundTruthAd(
                    annotationWindow: window,
                    id: "\(episodeId)-gt-\(i)",
                    podcastId: annotation.showName,
                    episodeId: episodeId
                ))
            }

            // Predictions: mirror the production seam. `LexicalScanner.scan`
            // merges raw hits into `LexicalCandidate` regions with real
            // boundaries (the same regions the live backfill builds spans
            // from). For each candidate region, run the auto-ad rule against a
            // span covering that region; where it fires, emit a predicted ad
            // span with the candidate's `[startTime, endTime]`. This avoids
            // arbitrary fixed tiles and gives the rule a realistic ad-extent
            // to be scored on — boundary tightness is a SEPARATE bead
            // (playhead-xsdz.2), so the headline metric here is region/seconds
            // coverage of the rule's *detections*, not pixel-perfect edges.
            let hits = scanner.collectHits(chunks: transcript)
            let candidates = scanner.scan(chunks: transcript, analysisAssetId: episodeId)
            var predicted: [(Double, Double)] = []
            for candidate in candidates {
                let span = DecodedSpan(
                    id: "\(episodeId)-cand-\(candidate.id)",
                    assetId: episodeId,
                    firstAtomOrdinal: 0,
                    lastAtomOrdinal: 1,
                    startTime: candidate.startTime,
                    endTime: candidate.endTime,
                    anchorProvenance: []
                )
                if !builder.buildEntries(hits: hits, for: span).isEmpty {
                    predicted.append((candidate.startTime, candidate.endTime))
                }
            }
            let merged = MetricsBatch.mergedIntervals(predicted)
            for (j, interval) in merged.enumerated() {
                for tier in episodeTiers {
                    detectionsByTier[tier, default: []].append(MetricDetectedAd(
                        id: "\(episodeId)-\(tier.rawValue)-det-\(j)",
                        podcastId: annotation.showName,
                        episodeId: episodeId,
                        startTime: interval.0,
                        endTime: interval.1,
                        path: .backfill,
                        firstConfirmationTime: nil,
                        confidence: 1.0
                    ))
                }
            }
            for tier in episodeTiers {
                scoredEpisodesByTier[tier, default: []].insert(episodeId)
            }
        }

        let scoredEpisodes = scoredEpisodesByTier[.gold, default: []]
        try XCTSkipIf(
            scoredEpisodes.isEmpty,
            """
            No corpus episodes had a staged transcript sidecar — \
            lexical auto-ad corpus eval skipped. (This is expected when \
            TestFixtures/Corpus/Transcripts/ is not populated; the always-on \
            coverage is LexicalAutoAdEvidenceBuilderTests.) skipped=\(skipped.count)
            """
        )

        // Only explicit-human gold drives quality gates. Silver and R3
        // boundary proposals are printed independently for diagnosis.
        let groundTruth = groundTruthByTier[.gold, default: []]
        let detections = detectionsByTier[.gold, default: []]
        let batch = MetricsBatch.pair(groundTruth: groundTruth, detections: detections)
        let coverageRecall = batch.computeCoverageRecall()
        let coveragePrecision = batch.computeCoveragePrecision()
        let spanF1 = SpanF1(batch: batch)

        let stratumLines = CorpusAnnotation.LabelTier.allCases.map { tier in
            let tierGroundTruth = groundTruthByTier[tier, default: []]
            let tierDetections = detectionsByTier[tier, default: []]
            return Self.recallDiagnosticLine(
                tier: tier,
                groundTruth: tierGroundTruth,
                detections: tierDetections,
                episodeCount: scoredEpisodesByTier[tier, default: []].count
            )
        }.joined(separator: "\n")

        print("""
        ── playhead-xsdz.1 lexical auto-ad corpus eval ──
        episodes scored=\(scoredEpisodes.count): \(scoredEpisodes.sorted().joined(separator: ", "))
        skipped=\(skipped.count): \(skipped.map { "\($0.0): \($0.1)" }.joined(separator: " | "))
        ground-truth ad windows=\(groundTruth.count)  predicted spans=\(detections.count)
        FM ad-region recall baseline = \(Self.fmAdRegionRecallBaseline)
        coverageRecall    = \(coverageRecall.map { String(format: "%.4f", $0) } ?? "n/a")
        coveragePrecision = \(coveragePrecision.map { String(format: "%.4f", $0) } ?? "n/a")
        span precision=\(spanF1.precision.map { String(format: "%.3f", $0) } ?? "n/a") \
        recall=\(spanF1.recall.map { String(format: "%.3f", $0) } ?? "n/a") \
        f1=\(spanF1.f1.map { String(format: "%.3f", $0) } ?? "n/a") \
        (tp=\(spanF1.truePositives) fp=\(spanF1.falsePositives) miss=\(spanF1.misses))
        strata (only gold is gated):
        \(stratumLines)
        """)

        // Headline metric: SPAN-LEVEL ad-region recall — the fraction of
        // golden ad windows the rule detected (paired a prediction with).
        // This is the apples-to-apples comparison to the FM "~21% ad-region
        // recall" baseline (both ask "did we find the ad region?"). Coverage
        // recall (SECONDS overlap) is reported for transparency but is NOT
        // the gate here: it conflates detection with boundary tightness, and
        // boundary tightness is the explicit job of the separate bead
        // playhead-xsdz.2 (tighter ad-candidate regions). Gating on seconds
        // here would mis-attribute that bead's work to this one.
        let regionRecall = try XCTUnwrap(
            spanF1.recall,
            "span recall undefined — no ground-truth ad windows?"
        )
        let precision = try XCTUnwrap(
            coveragePrecision,
            "coverage precision undefined — no predicted seconds?"
        )

        // Ad-region recall materially above the FM baseline.
        XCTAssertGreaterThan(
            regionRecall, Self.fmAdRegionRecallBaseline,
            "lexical auto-ad ad-region (span) recall (\(regionRecall)) must beat the FM baseline (\(Self.fmAdRegionRecallBaseline))"
        )
        XCTAssertGreaterThanOrEqual(
            regionRecall, Self.recallFloor,
            "lexical auto-ad ad-region (span) recall (\(regionRecall)) must be MATERIALLY above the FM baseline (floor \(Self.recallFloor))"
        )

        // Precision floor: no false-positive collapse. Coverage precision
        // (predicted seconds that land inside a golden ad window) is the
        // right precision lens — a false-positive ad means real content gets
        // wrongly skipped, and that cost is measured in seconds.
        XCTAssertGreaterThanOrEqual(
            precision, Self.coveragePrecisionFloor,
            "lexical auto-ad coverage precision (\(precision)) collapsed below the floor (\(Self.coveragePrecisionFloor)) — false-positive ad rate too high"
        )
    }
}
