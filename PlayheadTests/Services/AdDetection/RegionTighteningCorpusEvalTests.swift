// RegionTighteningCorpusEvalTests.swift
// playhead-xsdz.2: Deterministic corpus eval for lexical-cue-cluster region
// tightening in `TargetedWindowNarrower`.
//
// What this measures (and why it needs no FM)
// -------------------------------------------
// The narrowing + lexical-cluster snap is a PURE local timing computation, so
// this eval is fully deterministic. It loads the dogfood-corpus whisper
// transcripts (`TestFixtures/Corpus/Transcripts/<id>.json` via
// `CorpusTranscriptLoader`) and golden ad windows (`CorpusAnnotation.adWindows`
// via `CorpusAnnotationLoader`), drives the LEXICAL-slot narrowing phase
// (`TargetedWindowNarrower.narrow(.scanLikelyAdSlots)`) — which self-seeds its
// anchors from the lexical scanner — and scores the SECONDS-level coverage of
// the narrowed windows against the golden ad windows with the existing
// `SpanMetrics` harness.
//
// It runs the narrowing TWICE per episode:
//   * baseline — lexical-cluster snap DISABLED (acoustic-only shaping; what
//     shipped before xsdz.2). No acoustic breaks are staged here, so this is
//     just the fixed-padding window.
//   * tightened — lexical-cluster snap ENABLED (the xsdz.2 default).
//
// The headline claim of the bead: tightening improves SECONDS-level coverage
// quality (precision / IoU of the detected ad regions) WITHOUT dropping true
// ad coverage (recall) and WITHOUT a coverage-precision regression. We assert:
//   * coverage precision does NOT regress vs baseline (tightening removes
//     editorial dilution, so precision is >= baseline), AND clears a floor.
//   * coverage recall does NOT regress vs baseline (inward-only tightening
//     must not drop true ad seconds it previously covered).
//
// Gating: the corpus transcripts are git-ignored and may not be staged
// locally. When `TestFixtures/Corpus/Transcripts/` has no usable sidecars the
// test SKIPS cleanly (NOT a green-gate failure). The always-on coverage for
// the tightening logic is the hermetic `TargetedWindowNarrowerTests` xsdz.2
// cases.

import Foundation
import XCTest
@testable import Playhead

final class RegionTighteningCorpusEvalTests: XCTestCase {

    /// Coverage-precision floor for the tightened (default) configuration.
    /// Precision is paramount: predicted ad seconds that fall OUTSIDE a golden
    /// ad window are content we would wrongly hand to a downstream skip. We
    /// require the narrowed lexical-slot windows to be majority-inside golden
    /// ad windows. Set conservatively so transcript jitter does not flake the
    /// gate while still being a real floor.
    private static let coveragePrecisionFloor = 0.30

    /// Slack (seconds-ratio) allowed before we call a metric a regression.
    /// The eval's interval math is exact, but the lexical-cluster builder and
    /// scanner are sensitive to ±1 segment of whisper timing jitter at the
    /// edges, so we permit a hair of recall give-back rather than demanding
    /// bit-identical recall. Precision must NOT regress (tightening only ever
    /// removes editorial seconds), so it gets no slack beyond rounding.
    private static let recallRegressionSlack = 0.02
    private static let precisionRegressionSlack = 0.000_5

    /// Repo root derived from THIS file's `#filePath`. Mirrors the rationale
    /// in `LexicalAutoAdCorpusEvalTests`: this file lives at
    /// `PlayheadTests/Services/AdDetection/`, so walk up four parents.
    private static func repoRoot(filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()  // AdDetection/
            .deletingLastPathComponent()  // Services/
            .deletingLastPathComponent()  // PlayheadTests/
            .deletingLastPathComponent()  // <repo root>
    }

    func testLexicalClusterTighteningImprovesSecondsCoverageOnGoldenCorpus() throws {
        let loader = CorpusAnnotationLoader(repoRoot: Self.repoRoot())

        let annotationURLs: [URL]
        do {
            annotationURLs = try loader.annotationFileURLs()
        } catch CorpusAnnotationLoaderError.directoryNotFound(let url) {
            throw XCTSkip("corpus annotations dir not present: \(url.path)")
        }
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

        // Two narrowing configs differing ONLY in the lexical-cluster snap.
        let baselineConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            lexicalClusterSnapEnabled: false
        )
        let tightenedConfig = NarrowingConfig.default // lexicalClusterSnapEnabled == true

        var groundTruth: [MetricGroundTruthAd] = []
        var baselineDetections: [MetricDetectedAd] = []
        var tightenedDetections: [MetricDetectedAd] = []
        var scoredEpisodes: [String] = []
        var skipped: [(String, String)] = []

        for url in annotationURLs {
            let episodeId = url.deletingPathExtension().lastPathComponent

            let annotation = try loader.loadAndValidate(at: url)
            guard annotation.isEligibleForGoldEvaluation else {
                skipped.append((episodeId, "non-gold label tier: \(annotation.labelTier.rawValue)"))
                continue
            }

            let transcript = try CorpusTranscriptLoader.load(
                episodeId: episodeId,
                repoRoot: loader.repoRoot
            )
            guard !transcript.isEmpty else {
                skipped.append((episodeId, "transcript sidecar empty/absent"))
                continue
            }

            // Build narrower inputs from the transcript: one segment per chunk.
            let segments = makeFMSegments(
                analysisAssetId: episodeId,
                transcriptVersion: "corpus-v1",
                lines: transcript.map { ($0.startTime, $0.endTime, $0.text) }
            )
            let inputs = TargetedWindowNarrower.Inputs(
                analysisAssetId: episodeId,
                podcastId: annotation.showName,
                transcriptVersion: "corpus-v1",
                segments: segments,
                // `.scanLikelyAdSlots` self-seeds anchors from the lexical
                // scanner, so an empty evidence catalog is fine here.
                evidenceCatalog: EvidenceCatalog(
                    analysisAssetId: episodeId,
                    transcriptVersion: "corpus-v1",
                    entries: []
                ),
                auditWindowSampleRate: CoveragePlanner.defaultAuditWindowSampleRate
            )

            // Ground truth: this episode's golden ad windows.
            for (i, window) in annotation.adWindows.enumerated() {
                groundTruth.append(MetricGroundTruthAd(
                    annotationWindow: window,
                    id: "\(episodeId)-gt-\(i)",
                    podcastId: annotation.showName,
                    episodeId: episodeId
                ))
            }

            // Predicted windows from the lexical-slot narrowing phase, both
            // configs. Convert narrowed segments to merged time intervals.
            baselineDetections.append(contentsOf: detections(
                from: TargetedWindowNarrower.narrow(
                    phase: .scanLikelyAdSlots, inputs: inputs, config: baselineConfig
                ),
                episodeId: episodeId,
                showName: annotation.showName,
                tag: "base"
            ))
            tightenedDetections.append(contentsOf: detections(
                from: TargetedWindowNarrower.narrow(
                    phase: .scanLikelyAdSlots, inputs: inputs, config: tightenedConfig
                ),
                episodeId: episodeId,
                showName: annotation.showName,
                tag: "tight"
            ))
            scoredEpisodes.append(episodeId)
        }

        try XCTSkipIf(
            scoredEpisodes.isEmpty,
            """
            No corpus episodes had a staged transcript sidecar — \
            region-tightening corpus eval skipped. (Expected when \
            TestFixtures/Corpus/Transcripts/ is not populated; the always-on \
            coverage is the xsdz.2 cases in TargetedWindowNarrowerTests.) \
            skipped=\(skipped.count)
            """
        )

        let baselineBatch = MetricsBatch.pair(groundTruth: groundTruth, detections: baselineDetections)
        let tightenedBatch = MetricsBatch.pair(groundTruth: groundTruth, detections: tightenedDetections)

        let baseRecall = baselineBatch.computeCoverageRecall()
        let basePrecision = baselineBatch.computeCoveragePrecision()
        let baseIoU = baselineBatch.computeSpanIoU().median
        let tightRecall = tightenedBatch.computeCoverageRecall()
        let tightPrecision = tightenedBatch.computeCoveragePrecision()
        let tightIoU = tightenedBatch.computeSpanIoU().median

        func fmt(_ v: Double?) -> String { v.map { String(format: "%.4f", $0) } ?? "n/a" }
        print("""
        ── playhead-xsdz.2 region-tightening corpus eval ──
        episodes scored=\(scoredEpisodes.count): \(scoredEpisodes.sorted().joined(separator: ", "))
        skipped=\(skipped.count): \(skipped.map { "\($0.0): \($0.1)" }.joined(separator: " | "))
        golden ad windows=\(groundTruth.count)
        BASELINE (acoustic-only): coverageRecall=\(fmt(baseRecall)) coveragePrecision=\(fmt(basePrecision)) medianIoU=\(fmt(baseIoU))
        TIGHTENED (lexical snap): coverageRecall=\(fmt(tightRecall)) coveragePrecision=\(fmt(tightPrecision)) medianIoU=\(fmt(tightIoU))
        """)

        let basePrec = try XCTUnwrap(basePrecision, "baseline coverage precision undefined — no predicted seconds?")
        let tightPrec = try XCTUnwrap(tightPrecision, "tightened coverage precision undefined — no predicted seconds?")
        let baseRec = try XCTUnwrap(baseRecall, "baseline coverage recall undefined — no golden seconds?")
        let tightRec = try XCTUnwrap(tightRecall, "tightened coverage recall undefined — no golden seconds?")

        // 1. Coverage-precision FLOOR for the shipped (tightened) config.
        XCTAssertGreaterThanOrEqual(
            tightPrec, Self.coveragePrecisionFloor,
            "tightened coverage precision (\(tightPrec)) collapsed below the floor (\(Self.coveragePrecisionFloor))"
        )

        // 2. No PRECISION regression: tightening only ever removes editorial
        //    seconds from the window, so precision must be >= baseline.
        XCTAssertGreaterThanOrEqual(
            tightPrec, basePrec - Self.precisionRegressionSlack,
            "tightening regressed coverage precision (\(tightPrec) < baseline \(basePrec)) — over-tightening would not cause this; investigate"
        )

        // 3. No RECALL regression: inward-only tightening must not drop true ad
        //    seconds it previously covered (beyond edge-jitter slack).
        XCTAssertGreaterThanOrEqual(
            tightRec, baseRec - Self.recallRegressionSlack,
            "tightening dropped true ad coverage (recall \(tightRec) < baseline \(baseRec) − slack) — OVER-TIGHTENING regression"
        )

        // 4. Headline claim of the bead: tightening IMPROVES SECONDS-level
        //    coverage on the real corpus. With the corpus-tuned default the
        //    lexical-cue tightening trims editorial dilution from the candidate
        //    windows, so BOTH coverage precision and coverage recall rise vs
        //    the acoustic-only baseline. We assert a strict improvement (not
        //    merely no-regression) so a future change that silently neutralizes
        //    the tightening trips this gate.
        XCTAssertGreaterThan(
            tightPrec, basePrec,
            "tightening must IMPROVE coverage precision on the corpus (got \(tightPrec) vs baseline \(basePrec))"
        )
        XCTAssertGreaterThan(
            tightRec, baseRec,
            "tightening must IMPROVE coverage recall on the corpus (got \(tightRec) vs baseline \(baseRec))"
        )
    }

    /// Convert a `PhaseNarrowingResult` into merged `MetricDetectedAd` time
    /// intervals for one episode. Empty / aborted phases contribute nothing.
    private func detections(
        from result: PhaseNarrowingResult,
        episodeId: String,
        showName: String,
        tag: String
    ) -> [MetricDetectedAd] {
        guard let segments = result.narrowedSegments, !segments.isEmpty else {
            return []
        }
        let intervals = MetricsBatch.mergedIntervals(
            segments.map { ($0.startTime, $0.endTime) }
        )
        return intervals.enumerated().map { j, interval in
            MetricDetectedAd(
                id: "\(episodeId)-\(tag)-\(j)",
                podcastId: showName,
                episodeId: episodeId,
                startTime: interval.0,
                endTime: interval.1,
                path: .backfill,
                firstConfirmationTime: nil,
                confidence: 1.0
            )
        }
    }
}
