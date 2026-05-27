// AuditWindowNominationCoverageEvalTests.swift
// playhead-xsdz.3: Deterministic SIM coverage-proxy for lexically-nominated
// audit-window selection in `TargetedWindowNarrower`.
//
// What this measures (and why it needs NO FoundationModels)
// ---------------------------------------------------------
// xsdz.3 inverts the FM audit-scan selection from "spend the budget on a RANDOM
// contiguous block" to "point the SAME budget at the regions the lexical scanner
// flags as ad-likely" (the `.scanRandomAuditWindows` phase). Whether pointing the
// FM at nominated windows actually LIFTS end-to-end ad precision/recall is a
// LIVE-FM question that can only be answered by a Catalyst A/B (the au2v.1.27
// harness) — and an analogous chapter-signal audit-steering experiment showed
// ZERO end-to-end lift, so the FM-confirm efficacy MUST be Catalyst-validated
// before it is trusted. This eval does NOT claim that lift.
//
// What it DOES prove, deterministically and on-device, is the necessary (not
// sufficient) coverage PROXY: at an identical audit budget, the lexically-
// nominated audit windows cover the golden adBreak spans materially better than
// the pure-random baseline. If the nominated selection did NOT improve ad-span
// coverage, pointing the FM at it could not help; this proxy is the gate that
// justifies spending a Catalyst A/B on it.
//
// Hermetic golden corpus
// ----------------------
// The eval is fully self-contained: it SYNTHESIZES a golden corpus of episodes,
// each with planted ad spans (dense sponsor / promo-code / URL-CTA cue text) at
// known times and neutral editorial content everywhere else. The golden ad
// spans are the ground truth; the lexical scanner has to re-discover them from
// the cue text exactly as it would on a real transcript. Because the corpus is
// generated in-test, this eval ALWAYS runs (no git-ignored fixtures to skip),
// and is deterministic (the audit seed and lexical scan are pure functions).
//
// Both arms run at the SAME budget (`auditWindowSampleRate`), differing ONLY in
// `lexicalClusterSnapEnabled`:
//   * baseline — snap DISABLED → the pre-xsdz.3 random contiguous block.
//   * nominated — snap ENABLED (the xsdz.3 default) → lexical nomination.
// Equal budget means the no-FP-widening property is structural: neither arm can
// audit more segment-seconds than the other.

import Foundation
import XCTest

@testable import Playhead

final class AuditWindowNominationCoverageEvalTests: XCTestCase {

    /// One synthetic episode: total segment count and the planted golden ad
    /// spans (segment-index ranges, inclusive) whose text carries dense ad cues.
    private struct SyntheticEpisode {
        let id: String
        let segmentCount: Int
        /// Inclusive `[lo, hi]` segment-index ranges that ARE ads.
        let adRanges: [(Int, Int)]
    }

    /// A spread of episode layouts that exercise the proxy across ad
    /// placements a single random block would systematically miss:
    /// late-episode ads, early ads, and multi-pod episodes. The random block's
    /// seed-driven start lands deterministically; these layouts are chosen so
    /// the random arm covers some-but-not-all golden seconds, leaving headroom
    /// the nomination arm can recover.
    private static let episodes: [SyntheticEpisode] = [
        SyntheticEpisode(id: "ep-late", segmentCount: 80, adRanges: [(64, 70)]),
        SyntheticEpisode(id: "ep-early", segmentCount: 80, adRanges: [(6, 12)]),
        SyntheticEpisode(id: "ep-two-pod", segmentCount: 120, adRanges: [(20, 26), (90, 96)]),
        SyntheticEpisode(id: "ep-mid", segmentCount: 100, adRanges: [(46, 52)]),
        SyntheticEpisode(id: "ep-three-pod", segmentCount: 150, adRanges: [(12, 17), (70, 75), (130, 136)]),
    ]

    func testNominatedAuditWindowsCoverGoldenAdSpansBetterThanRandom() {
        let podcastId = "synthetic-audit-corpus"

        var groundTruth: [MetricGroundTruthAd] = []
        var randomDetections: [MetricDetectedAd] = []
        var nominatedDetections: [MetricDetectedAd] = []

        // Baseline (random) vs nominated differ ONLY in the lexical snap flag,
        // so the audit budget (`auditWindowSampleRate`) is identical in both.
        let randomConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            lexicalClusterSnapEnabled: false
        )
        let nominatedConfig = NarrowingConfig.default // lexicalClusterSnapEnabled == true

        for episode in Self.episodes {
            let inputs = makeEpisodeInputs(episode: episode, podcastId: podcastId)

            // Ground truth: the planted golden ad spans (segment time bounds).
            for (i, range) in episode.adRanges.enumerated() {
                let startTime = Double(range.0)
                let endTime = Double(range.1 + 1) // segment [hi] ends at hi+1
                groundTruth.append(MetricGroundTruthAd(
                    id: "\(episode.id)-gt-\(i)",
                    podcastId: podcastId,
                    episodeId: episode.id,
                    startTime: startTime,
                    endTime: endTime,
                    format: .hostRead,
                    seedFired: true
                ))
            }

            randomDetections.append(contentsOf: detections(
                from: TargetedWindowNarrower.narrow(
                    phase: .scanRandomAuditWindows, inputs: inputs, config: randomConfig
                ),
                episodeId: episode.id,
                podcastId: podcastId,
                tag: "rand"
            ))
            nominatedDetections.append(contentsOf: detections(
                from: TargetedWindowNarrower.narrow(
                    phase: .scanRandomAuditWindows, inputs: inputs, config: nominatedConfig
                ),
                episodeId: episode.id,
                podcastId: podcastId,
                tag: "nom"
            ))
        }

        let randomBatch = MetricsBatch.pair(groundTruth: groundTruth, detections: randomDetections)
        let nominatedBatch = MetricsBatch.pair(groundTruth: groundTruth, detections: nominatedDetections)

        let randomCoverage = randomBatch.computeCoverageRecall()
        let nominatedCoverage = nominatedBatch.computeCoverageRecall()

        // Audited segment-seconds per arm (the budget proxy). Equal budget means
        // neither arm audits MORE seconds — nomination just aims the same spend.
        let randomAuditedSeconds = auditedSeconds(randomDetections)
        let nominatedAuditedSeconds = auditedSeconds(nominatedDetections)

        func fmt(_ v: Double?) -> String { v.map { String(format: "%.4f", $0) } ?? "n/a" }
        print("""
        ── playhead-xsdz.3 audit-window nomination coverage proxy (hermetic) ──
        episodes=\(Self.episodes.count) golden ad spans=\(groundTruth.count)
        RANDOM  (snap off): adSpanCoverage=\(fmt(randomCoverage)) auditedSeconds=\(randomAuditedSeconds)
        NOMINATED (snap on): adSpanCoverage=\(fmt(nominatedCoverage)) auditedSeconds=\(nominatedAuditedSeconds)
        NOTE: this is the SIM coverage proxy only. End-to-end FM-confirm efficacy
        (does steering the FM at nominated windows lift ad precision/recall) is
        Catalyst-gated (au2v.1.27 A/B); an analogous chapter-signal audit-steering
        showed ZERO lift, so the proxy is necessary but NOT sufficient.
        """)

        let randomCov = try! XCTUnwrap(randomCoverage, "random arm produced no golden coverage signal")
        let nominatedCov = try! XCTUnwrap(nominatedCoverage, "nominated arm produced no golden coverage signal")

        // Headline coverage-proxy claim: lexically-nominated audit windows cover
        // the golden ad spans MATERIALLY better than random at equal budget.
        // "Materially" = a strict, non-trivial improvement. The synthetic corpus
        // is built so the random block systematically misses much of the planted
        // ad seconds while nomination targets them directly.
        XCTAssertGreaterThan(
            nominatedCov, randomCov,
            "nominated audit coverage (\(nominatedCov)) must exceed random (\(randomCov)) at equal budget"
        )
        XCTAssertGreaterThanOrEqual(
            nominatedCov - randomCov, 0.20,
            "improvement must be MATERIAL (>= 0.20 absolute ad-span coverage), got \(nominatedCov - randomCov)"
        )

        // No-FP-widening property: nomination stays within budget — it audits no
        // more segment-seconds than the random baseline (the budget is identical,
        // so this is exact up to dedup of overlapping nominated/random segments).
        XCTAssertLessThanOrEqual(
            nominatedAuditedSeconds, randomAuditedSeconds,
            "nomination must not widen the audit budget (\(nominatedAuditedSeconds) > \(randomAuditedSeconds))"
        )

        // Conservatism: nomination must NEVER reduce ad-span coverage vs random.
        // (Subsumed by the strict-improvement assert above, but stated explicitly
        // as the bead's no-regression guarantee.)
        XCTAssertGreaterThanOrEqual(
            nominatedCov, randomCov,
            "nomination must never reduce ad-span coverage vs the random baseline"
        )
    }

    // MARK: - Helpers

    /// Build narrower inputs for a synthetic episode: 1-second segments, dense
    /// ad-cue text inside the golden ad ranges, neutral text everywhere else.
    /// No evidence anchor is seeded — the `.scanRandomAuditWindows` phase does
    /// not consult the evidence catalog.
    private func makeEpisodeInputs(
        episode: SyntheticEpisode,
        podcastId: String
    ) -> TargetedWindowNarrower.Inputs {
        let transcriptVersion = "tx-audit-corpus-v1"
        var adSet = Set<Int>()
        for (lo, hi) in episode.adRanges {
            for idx in lo...hi { adSet.insert(idx) }
        }
        // Rotate a small bank of dense ad-cue lines through the ad segments so
        // each ad span carries multiple distinct sponsor/promo/URL hits — the
        // lexical scanner needs real cues to form a cluster.
        let cueBank = [
            "this episode is brought to you by acme tools",
            "use code SAVE at checkout for a free trial",
            "visit acmetools.com for this special offer",
            "promo code SAVE gets you a money back guarantee",
            "sign up now at acmetools.com slash deal",
            "that offer again is acmetools.com slash deal",
            "head to acmetools.com slash podcast today",
        ]
        let lines: [(start: Double, end: Double, text: String)] =
            (0..<episode.segmentCount).map { idx in
                let text: String
                if adSet.contains(idx) {
                    text = cueBank[idx % cueBank.count]
                } else {
                    text = "neutral conversation line \(idx) about the topic at hand"
                }
                return (Double(idx), Double(idx + 1), text)
            }
        let segments = makeFMSegments(
            analysisAssetId: episode.id,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        return TargetedWindowNarrower.Inputs(
            analysisAssetId: episode.id,
            podcastId: podcastId,
            transcriptVersion: transcriptVersion,
            segments: segments,
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: episode.id,
                transcriptVersion: transcriptVersion,
                entries: []
            ),
            auditWindowSampleRate: CoveragePlanner.defaultAuditWindowSampleRate
        )
    }

    /// Convert a `PhaseNarrowingResult` into merged detected intervals.
    private func detections(
        from result: PhaseNarrowingResult,
        episodeId: String,
        podcastId: String,
        tag: String
    ) -> [MetricDetectedAd] {
        guard let segments = result.narrowedSegments, !segments.isEmpty else { return [] }
        let intervals = MetricsBatch.mergedIntervals(
            segments.map { ($0.startTime, $0.endTime) }
        )
        return intervals.enumerated().map { j, interval in
            MetricDetectedAd(
                id: "\(episodeId)-\(tag)-\(j)",
                podcastId: podcastId,
                episodeId: episodeId,
                startTime: interval.0,
                endTime: interval.1,
                path: .backfill,
                firstConfirmationTime: nil,
                confidence: 1.0
            )
        }
    }

    /// Total audited seconds across all detections (the budget proxy).
    private func auditedSeconds(_ detections: [MetricDetectedAd]) -> Double {
        detections.reduce(0) { $0 + $1.duration }
    }
}
