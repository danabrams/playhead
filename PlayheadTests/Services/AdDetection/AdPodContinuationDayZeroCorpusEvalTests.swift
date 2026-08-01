// AdPodContinuationDayZeroCorpusEvalTests.swift
// playhead-evc1: the corpus measurement for the DAY-0 SEED CARVE-OUT, and an
// explicit statement of what it can and cannot establish.
//
// WHY THE EXISTING LANE CANNOT ANSWER THIS
// ---------------------------------------
// `AdPodContinuationCorpusEvalTests` reports an IDENTICAL number before and
// after this change, and would have done so for any version of it. Its pipeline
// dump carries 132 of 132 `confirmed`/`eligible` windows and ZERO day-0 rows —
// `grep -c dayZeroRediffByteExact` is 0 across every diagnostics artifact in the
// repo. That is exactly why the eks2 A/B could not catch the defect this bead
// fixes, and it is why a green run of that lane is not evidence here.
// `dayZeroRowsAreAbsentFromTheShippedCorpus` asserts that absence rather than
// leaving it as a claim in a commit message.
//
// WHAT THIS LANE DOES INSTEAD, AND THE ONE ASSUMPTION IT MAKES
// -----------------------------------------------------------
// It RECONSTRUCTS the day-0 population from the byte-derived rediff dump. A
// day-0 byte-exact mark is, by construction, a slot over which the origin served
// DIFFERENT BYTES on a re-fetch, mapped to the played A timeline — the same
// object the rediff dump records. So for each rediff slot this lane mints the
// row `mintByteExactDayZeroMarks` would write for a STRICT slot (day-0
// `boundaryState`, `.candidate`, confidence 1.00, `.rediffByteExact` on both
// edges, mark-only) and measures what the carve-out then claims.
//
// THE ASSUMPTION, stated plainly: that a day-0 double-fetch WOULD have revealed
// each slot. It would not have revealed all of them — on a client-PINNED show
// only one persona diverges and some fetches collide outright — so the real
// day-0 set is a SUBSET of the slots modelled here. This lane therefore
// OVER-states how often the carve-out fires. That is the conservative direction
// for the risk figure and the optimistic direction for recall, and the recall
// figure below should not be read as a production yield.
//
// The A/B is exact, though, and that is the part worth trusting: both arms are
// composed from the SAME window set, differing only in whether those rows carry
// day-0 provenance or the aggregator's. Nothing else moves.
//
// TWO MORE HONEST LIMITS
//   • "Outside every rediff slot" is NOT "show". A rediff slot sees DAI
//     insertions only. Dan's own ad 2 — the creative this whole bead exists to
//     mark — was byte-IDENTICAL in both fetches and would therefore not be a
//     rediff slot at all, so this metric would score recovering it as an
//     out-of-slot claim. The number is an upper bound on risk, not a count of
//     mistakes, and the marks are printed so the claims can be read against the
//     transcript by hand.
//   • The dump carries no FM scan rows, so the FM `noAds` content barrier is
//     absent and only the spoken-return-marker barrier is exercised. Barriers
//     only ever SHRINK recovery, so every figure here is the worst case.
//
// STAGING (env-gated, skips cleanly) — the same three artifacts, and the same
// defaults pointing at the MAIN checkout, as `AdPodContinuationCorpusEvalTests`.

import Foundation
import XCTest
@testable import Playhead

final class AdPodContinuationDayZeroCorpusEvalTests: XCTestCase {

    // MARK: - Corpus JSON shapes

    private struct PipelineDump: Decodable {
        struct Episode: Decodable {
            struct Window: Decodable {
                let startTime: Double
                let endTime: Double
                let decisionState: String
                let eligibilityGate: String?
                let boundaryState: String?
            }
            let episodeId: String
            let episodeDurationSeconds: Double
            let adWindows: [Window]
        }
        let episodes: [Episode]
    }

    private struct RediffDump: Decodable {
        struct Episode: Decodable {
            struct Slot: Decodable {
                let startSeconds: Double
                let endSeconds: Double
            }
            let episodeId: String
            let adSlots: [Slot]
        }
        let episodes: [Episode]
    }

    private struct WhisperTranscript: Decodable {
        struct Segment: Decodable {
            struct Offsets: Decodable {
                let from: Int
                let to: Int
            }
            let text: String
            let offsets: Offsets
        }
        let transcription: [Segment]
    }

    // MARK: - Interval helpers (independent of the production code)

    private typealias Interval = (start: Double, end: Double)

    private func union(_ intervals: [Interval]) -> [Interval] {
        let sorted = intervals
            .filter { $0.end > $0.start }
            .sorted { $0.start < $1.start }
        var result: [Interval] = []
        for interval in sorted {
            if let last = result.last, interval.start <= last.end {
                result[result.count - 1] = (start: last.start, end: max(last.end, interval.end))
            } else {
                result.append(interval)
            }
        }
        return result
    }

    private func clip(_ intervals: [Interval], to bounds: Interval) -> [Interval] {
        intervals.compactMap {
            let start = max($0.start, bounds.start)
            let end = min($0.end, bounds.end)
            return end > start ? (start: start, end: end) : nil
        }
    }

    private func totalLength(_ intervals: [Interval]) -> Double {
        union(intervals).reduce(0.0) { $0 + ($1.end - $1.start) }
    }

    private func difference(_ intervals: [Interval], minus subtrahend: [Interval]) -> [Interval] {
        var result: [Interval] = []
        for interval in union(intervals) {
            var cursor = interval.start
            for block in union(clip(subtrahend, to: interval)) {
                if block.start > cursor { result.append((start: cursor, end: block.start)) }
                cursor = max(cursor, block.end)
            }
            if cursor < interval.end { result.append((start: cursor, end: interval.end)) }
        }
        return result
    }

    // MARK: - Fixture plumbing

    private static let pipelinePathDefault =
        "/Users/dabrams/playhead/playhead-dogfood-diagnostics-pipeline-dump-new9.json"
    private static let rediffPathDefault =
        "/Users/dabrams/playhead/playhead-dogfood-diagnostics-tier-a-rediff.json"
    private static let transcriptDirectoryDefault =
        "/Users/dabrams/playhead/TestFixtures/Corpus/Transcripts"

    private func stagedPaths() -> (pipeline: String, rediff: String, transcripts: String) {
        let environment = ProcessInfo.processInfo.environment
        return (
            environment["PLAYHEAD_POD_EVAL_PIPELINE"] ?? Self.pipelinePathDefault,
            environment["PLAYHEAD_POD_EVAL_REDIFF"] ?? Self.rediffPathDefault,
            environment["PLAYHEAD_POD_EVAL_TRANSCRIPTS"] ?? Self.transcriptDirectoryDefault
        )
    }

    private func chunks(from transcript: WhisperTranscript, assetId: String) -> [TranscriptChunk] {
        transcript.transcription.enumerated().map { index, segment in
            TranscriptChunk(
                id: "\(assetId)-\(index)",
                analysisAssetId: assetId,
                segmentFingerprint: "corpus-\(index)",
                chunkIndex: index,
                startTime: Double(segment.offsets.from) / 1000.0,
                endTime: Double(segment.offsets.to) / 1000.0,
                text: segment.text,
                normalizedText: segment.text.lowercased(),
                pass: "final",
                modelVersion: "whisper-corpus",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func pipelineWindows(from episode: PipelineDump.Episode) -> [AdWindow] {
        episode.adWindows.enumerated().map { index, window in
            AdWindow(
                id: "\(episode.episodeId)-w\(index)",
                analysisAssetId: episode.episodeId,
                startTime: window.startTime,
                endTime: window.endTime,
                confidence: 0.9,
                boundaryState: window.boundaryState
                    ?? AdBoundaryState.acousticRefined.rawValue,
                decisionState: window.decisionState,
                detectorVersion: "detection-v1",
                advertiser: nil,
                product: nil,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: nil,
                metadataSource: "fusion-v1",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                eligibilityGate: window.eligibilityGate
            )
        }
    }

    /// The row `mintByteExactDayZeroMarks` writes for a STRICT byte-exact slot,
    /// or — when `dayZeroProvenance` is false — the identical row carrying the
    /// aggregator's provenance instead. The A/B arm is this one boolean.
    private func reconstructedDayZeroRow(
        episodeId: String,
        index: Int,
        slot: Interval,
        dayZeroProvenance: Bool
    ) -> AdWindow {
        AdWindow(
            id: "\(episodeId)-day0-\(index)",
            analysisAssetId: episodeId,
            startTime: slot.start,
            endTime: slot.end,
            confidence: 1.0,
            boundaryState: dayZeroProvenance
                ? AdDetectionService.dayZeroRediffByteExactBoundaryState
                : AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: slot.start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
        )
    }

    // MARK: - The claim this lane exists to refute

    /// The shipped corpus carries no day-0 rows at all, so
    /// `AdPodContinuationCorpusEvalTests` is SILENT on this change — it reports
    /// the same number before and after. Asserted here rather than asserted in
    /// prose, because "the corpus says nothing" is the load-bearing reason this
    /// second lane exists and the reason the bead's evidence is a field case.
    func testDayZeroRowsAreAbsentFromTheShippedCorpus() throws {
        let paths = stagedPaths()
        try XCTSkipUnless(
            FileManager.default.fileExists(atPath: paths.pipeline),
            "pipeline dump not staged: \(paths.pipeline)"
        )
        let raw = try String(
            contentsOf: URL(fileURLWithPath: paths.pipeline), encoding: .utf8
        )
        let occurrences = raw.components(
            separatedBy: AdDetectionService.dayZeroRediffByteExactBoundaryState
        ).count - 1
        let pipeline = try JSONDecoder().decode(
            PipelineDump.self, from: try Data(contentsOf: URL(fileURLWithPath: paths.pipeline))
        )
        let windowCount = pipeline.episodes.reduce(0) { $0 + $1.adWindows.count }
        let seedableStates = pipeline.episodes
            .flatMap { $0.adWindows.map(\.decisionState) }
            .filter { AdPodContinuation.seedDecisionStates.contains($0) }
            .count
        print(
            """

            == playhead-evc1: what the shipped corpus contains =================
            windows in dump                    \(windowCount)
            of which seed on decisionState     \(seedableStates)
            occurrences of the day-0 literal   \(occurrences)
            ===================================================================

            """
        )
        XCTAssertEqual(
            occurrences,
            0,
            """
            The corpus now carries day-0 rows. That is GOOD NEWS and this \
            assertion is the notification: re-run the eks2 corpus A/B, which \
            could not see this population and was therefore silent on \
            playhead-evc1.
            """
        )
    }

    // MARK: - The reconstructed measurement

    func testDayZeroSeedingOnReconstructedCorpus() throws {
        let paths = stagedPaths()
        for path in [paths.pipeline, paths.rediff, paths.transcripts] {
            try XCTSkipUnless(
                FileManager.default.fileExists(atPath: path),
                "pod-continuation corpus eval artifact not staged: \(path)"
            )
        }
        let decoder = JSONDecoder()
        let pipeline = try decoder.decode(
            PipelineDump.self,
            from: try Data(contentsOf: URL(fileURLWithPath: paths.pipeline))
        )
        let rediff = try decoder.decode(
            RediffDump.self,
            from: try Data(contentsOf: URL(fileURLWithPath: paths.rediff))
        )
        let slotsByEpisode = Dictionary(
            uniqueKeysWithValues: rediff.episodes
                .filter { !$0.adSlots.isEmpty }
                .map { episode in
                    (
                        episode.episodeId,
                        episode.adSlots.map { (start: $0.startSeconds, end: $0.endSeconds) }
                    )
                }
        )

        let scanner = LexicalScanner()

        /// One arm of the measurement.
        struct Outcome {
            var episodes = 0
            var seeds = 0
            var marks = 0
            var claimedSeconds = 0.0
            var claimedOutsideSlots = 0.0
            var lines: [String] = []
            var gateViolations: [String] = []
        }
        /// `firstListen` models the state a day-0 mint leaves behind before any
        /// analysis has run: the reconstructed rows are the ONLY windows. `mixed`
        /// adds the pipeline's own output, which is the state a later backfill
        /// sees. Both are measured because the carve-out fires in both, and the
        /// second is where the pass has more material to dedupe against.
        enum Population: String, CaseIterable {
            case firstListen = "first listen (day-0 rows only)"
            case mixed = "mixed (pipeline + day-0 rows)"
        }
        var outcomes: [String: Outcome] = [:]
        var episodesEvaluated = 0

        for episode in pipeline.episodes {
            guard let slots = slotsByEpisode[episode.episodeId] else { continue }
            let transcriptURL = URL(fileURLWithPath: paths.transcripts)
                .appendingPathComponent("\(episode.episodeId).json")
            guard let data = try? Data(contentsOf: transcriptURL),
                  let transcript = try? decoder.decode(WhisperTranscript.self, from: data),
                  !transcript.transcription.isEmpty else {
                continue
            }
            episodesEvaluated += 1

            let episodeChunks = chunks(from: transcript, assetId: episode.episodeId)
            let hits = scanner.collectHits(chunks: episodeChunks)
            let links = AdPodContinuation.mergeLinks(
                AdPodContinuation.adCopyLinks(chunks: episodeChunks, hits: hits)
                    + AdPodContinuation.rhetoricalLinks(chunks: episodeChunks)
            )
            let barriers = AdPodContinuation.contentBarriers(
                semanticScanResults: [],
                lexicalHits: hits,
                chunks: episodeChunks
            )
            let baseline = pipelineWindows(from: episode)

            for population in Population.allCases {
                for dayZero in [true, false] {
                    let reconstructed = slots.enumerated().map {
                        reconstructedDayZeroRow(
                            episodeId: episode.episodeId,
                            index: $0.offset,
                            slot: $0.element,
                            dayZeroProvenance: dayZero
                        )
                    }
                    let existing = population == .firstListen
                        ? reconstructed
                        : baseline + reconstructed
                    let marks = AdPodContinuation.compose(
                        existingWindows: existing,
                        adCopyLinks: links,
                        contentBarriers: barriers,
                        protectedRegions: [],
                        episodeDuration: episode.episodeDurationSeconds,
                        analysisAssetId: episode.episodeId
                    )
                    let name = "\(population.rawValue) — "
                        + (dayZero ? "day-0 provenance (SHIPPING)" : "aggregator provenance (control)")
                    var outcome = outcomes[name] ?? Outcome()
                    outcome.episodes += 1
                    outcome.seeds += existing.filter(AdPodContinuation.isSeed).count
                    outcome.marks += marks.count
                    let existingIntervals: [Interval] = existing.map {
                        (start: $0.startTime, end: $0.endTime)
                    }
                    let markIntervals: [Interval] = marks.map {
                        (start: $0.startTime, end: $0.endTime)
                    }
                    let newSeconds = difference(markIntervals, minus: existingIntervals)
                    outcome.claimedSeconds += totalLength(newSeconds)
                    outcome.claimedOutsideSlots += totalLength(
                        difference(newSeconds, minus: slots)
                    )
                    for mark in marks {
                        if mark.eligibilityGate != SkipEligibilityGate.markOnly.rawValue
                            || mark.startEdgeAnchor != AutoSkipEdgeAnchor.unanchored.rawValue
                            || mark.endEdgeAnchor != AutoSkipEdgeAnchor.unanchored.rawValue
                            || mark.decisionState != AdDecisionState.candidate.rawValue {
                            outcome.gateViolations.append(
                                "\(episode.episodeId) \(mark.startTime)-\(mark.endTime)"
                            )
                        }
                        let outside = totalLength(
                            difference([(start: mark.startTime, end: mark.endTime)], minus: slots)
                        )
                        outcome.lines.append(
                            String(
                                format: "    %-44@ %8.1f-%8.1f (%5.1fs, %5.1fs outside a slot)",
                                String(episode.episodeId.prefix(44)) as NSString,
                                mark.startTime,
                                mark.endTime,
                                mark.endTime - mark.startTime,
                                outside
                            )
                        )
                    }
                    outcomes[name] = outcome
                }
            }
        }

        try XCTSkipIf(
            episodesEvaluated == 0,
            "no corpus episode had a rediff slot list, a dump and a transcript"
        )

        var report = """

            == playhead-evc1 day-0 seed corpus eval (RECONSTRUCTED population) ==
            episodes evaluated \(episodesEvaluated)

            READ THE HEADER BEFORE QUOTING ANY NUMBER HERE. The day-0 rows are
            reconstructed from the byte-derived rediff dump, not observed; the
            real day-0 set is a SUBSET (pinned shows collide), so recall is
            over-stated. "Outside a slot" is an upper bound on risk, not a count
            of mistakes: a host-read or byte-identical creative is not a rediff
            slot, and marking one is the point of this bead.

            """
        for name in outcomes.keys.sorted() {
            guard let outcome = outcomes[name] else { continue }
            report += """

                -- ARM: \(name)
                seeds admitted                        \(outcome.seeds)
                marks emitted                         \(outcome.marks)
                newly claimed seconds                 \(String(format: "%.1f", outcome.claimedSeconds))
                  outside every rediff slot           \(String(format: "%.1f", outcome.claimedOutsideSlots))
                banner-tier violations                \(outcome.gateViolations.count)
                \(outcome.lines.joined(separator: "\n"))

                """
        }
        report += "=====================================================================\n"
        print(report)

        // The A/B, asserted. The control arm carries the SAME rows with the
        // aggregator's provenance, so any difference is the carve-out and
        // nothing else.
        for population in Population.allCases {
            let shipping = try XCTUnwrap(
                outcomes["\(population.rawValue) — day-0 provenance (SHIPPING)"]
            )
            let control = try XCTUnwrap(
                outcomes["\(population.rawValue) — aggregator provenance (control)"]
            )
            XCTAssertGreaterThan(
                shipping.seeds,
                control.seeds,
                "\(population.rawValue): the carve-out admitted no seed the control did not — the arms are not measuring it"
            )
            XCTAssertTrue(
                shipping.gateViolations.isEmpty,
                "\(population.rawValue): a day-0 seeded mark was not banner-tier: \(shipping.gateViolations)"
            )
        }
        // playhead-2350's property on this population: whatever the carve-out
        // claims, none of it can auto-skip. Checked above per arm; restated here
        // as the single sentence the bead is allowed to promise.
        XCTAssertTrue(
            outcomes.values.allSatisfy { $0.gateViolations.isEmpty },
            "a mark was not banner-tier in some arm"
        )
    }
}
