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

    func testLexicalAutoAdRecallAndPrecisionOnGoldenCorpus() throws {
        let loader = CorpusAnnotationLoader(repoRoot: Self.repoRoot())

        // Enumerate annotations; skip cleanly if the corpus dir is absent.
        let annotationURLs: [URL]
        do {
            annotationURLs = try loader.annotationFileURLs()
        } catch {
            throw XCTSkip("corpus annotations dir not present: \(error)")
        }
        try XCTSkipIf(annotationURLs.isEmpty, "no corpus annotations staged")

        let scanner = LexicalScanner()
        let builder = LexicalAutoAdEvidenceBuilder()

        var groundTruth: [MetricGroundTruthAd] = []
        var detections: [MetricDetectedAd] = []
        var scoredEpisodes: [String] = []
        var skipped: [(String, String)] = []

        for url in annotationURLs {
            let episodeId = url.deletingPathExtension().lastPathComponent

            let annotation: CorpusAnnotation
            do {
                annotation = try loader.decode(at: url)
            } catch {
                skipped.append((episodeId, "annotation decode failed: \(error)"))
                continue
            }

            // Load transcript; absent/empty → skip this episode cleanly.
            let transcript: [TranscriptChunk]
            do {
                transcript = try CorpusTranscriptLoader.load(
                    episodeId: episodeId,
                    repoRoot: loader.repoRoot
                )
            } catch {
                skipped.append((episodeId, "transcript decode failed: \(error)"))
                continue
            }
            guard !transcript.isEmpty else {
                skipped.append((episodeId, "transcript sidecar empty/absent"))
                continue
            }

            // Ground truth: this episode's golden ad windows.
            for (i, window) in annotation.adWindows.enumerated() {
                groundTruth.append(MetricGroundTruthAd(
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
                detections.append(MetricDetectedAd(
                    id: "\(episodeId)-det-\(j)",
                    podcastId: annotation.showName,
                    episodeId: episodeId,
                    startTime: interval.0,
                    endTime: interval.1,
                    path: .backfill,
                    firstConfirmationTime: nil,
                    confidence: 1.0
                ))
            }
            scoredEpisodes.append(episodeId)
        }

        try XCTSkipIf(
            scoredEpisodes.isEmpty,
            """
            No corpus episodes had a staged transcript sidecar — \
            lexical auto-ad corpus eval skipped. (This is expected when \
            TestFixtures/Corpus/Transcripts/ is not populated; the always-on \
            coverage is LexicalAutoAdEvidenceBuilderTests.) skipped=\(skipped.count)
            """
        )

        // Score.
        let batch = MetricsBatch.pair(groundTruth: groundTruth, detections: detections)
        let coverageRecall = batch.computeCoverageRecall()
        let coveragePrecision = batch.computeCoveragePrecision()
        let spanF1 = SpanF1(batch: batch)

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
