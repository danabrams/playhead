// AdPodContinuationCorpusEvalTests.swift
// playhead-xsdz.65: the ACCEPTANCE MEASUREMENT for ad-pod continuation, run
// against rediff-confirmed pod boundaries on the dogfood corpus.
//
// WHAT IT MEASURES, AND WHY THE GROUND TRUTH IS TRUSTWORTHY
// --------------------------------------------------------
// A rediff slot is byte-derived: the origin served DIFFERENT BYTES over that
// range on a re-fetch, which is what a DAI insertion IS. It owes nothing to
// human labelling, so it measures pod COMPLETENESS directly — which is exactly
// why this bead's old "needs gold extension to measure" blocker no longer
// applies (gold under-labels whole pods; rediff cannot).
//
// For every rediff slot the BASELINE pipeline overlapped at all, the eval takes
// the largest contiguous UNCOVERED run inside the slot, before and after the
// pass. The headline number is the count of detected slots whose largest
// uncovered run exceeds 30 s — the bead's operational split between a missing
// pod NEIGHBOUR (this bead) and a single-span edge that stopped short
// (playhead-4xqf).
//
// The cohort is pinned to the BASELINE's detected slots so it cannot drift: a
// slot the pass newly touches does not enter the denominator and cannot flatter
// the ratio.
//
// It also measures the number that actually decides whether this ships: how many
// NEWLY-CLAIMED seconds fall OUTSIDE every rediff slot for that episode. Those
// are the seconds most at risk of being show content.
//
// STAGING (env-gated, skips cleanly)
// ----------------------------------
//   PLAYHEAD_POD_EVAL_PIPELINE     playhead-dogfood-diagnostics-pipeline-dump-new9.json
//   PLAYHEAD_POD_EVAL_REDIFF       playhead-dogfood-diagnostics-tier-a-rediff.json
//   PLAYHEAD_POD_EVAL_TRANSCRIPTS  directory of whisper `<episodeId>.json` sidecars
//
// All three are git-ignored capture artifacts that live in the MAIN checkout and
// are not copied into a bead worktree, so the paths default there and are
// overridable via `TEST_RUNNER_PLAYHEAD_POD_EVAL_*` (the same shape as
// `CorpusAudioFixtures`). When an artifact is absent the eval SKIPS. The
// always-on coverage for the logic is `AdPodContinuationTests` (hermetic) and
// `AdPodContinuationWireInTests` (through `runBackfill`).
//
// HONEST LIMITS OF THIS LANE, stated rather than buried:
//   • The dump carries no FM scan rows, so the FM `noAds` content barrier is
//     ABSENT here and only the spoken-return-marker barrier is exercised.
//     Barriers only ever SHRINK the recovery, so the recovered-seconds figure
//     this lane reports is an UPPER bound and the out-of-slot figure is a
//     WORST case. That is the conservative direction for the number that
//     matters.
//   • Rediff slots see DAI insertions. A host-read ad is not a rediff slot, so a
//     newly-claimed second outside every slot is "not provably a DAI ad" — NOT
//     "provably show". Out-of-slot seconds must be hand-audited against the
//     transcript, not assumed benign.
//   • N is small (9 episodes with both sides staged) and the capture predates
//     later merges. Trust the shape.

import Foundation
import XCTest
@testable import Playhead

final class AdPodContinuationCorpusEvalTests: XCTestCase {

    /// Uncovered-run threshold separating a missing pod NEIGHBOUR from a
    /// single-span edge that stopped short (playhead-4xqf's territory).
    private static let neighbourHoleThreshold = 30.0

    // MARK: - Corpus JSON shapes

    private struct PipelineDump: Decodable {
        struct Episode: Decodable {
            struct Window: Decodable {
                let startTime: Double
                let endTime: Double
                let decisionState: String
                let eligibilityGate: String?
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

    /// Largest contiguous uncovered run inside `slot`, given `covered`.
    private func largestHole(in slot: Interval, covered: [Interval]) -> Double {
        var largest = 0.0
        var cursor = slot.start
        for block in union(clip(covered, to: slot)) {
            largest = max(largest, block.start - cursor)
            cursor = max(cursor, block.end)
        }
        return max(largest, slot.end - cursor)
    }

    private func totalLength(_ intervals: [Interval]) -> Double {
        union(intervals).reduce(0.0) { $0 + ($1.end - $1.start) }
    }

    /// `intervals` minus the union of `subtrahend`.
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

    private func adWindows(
        from episode: PipelineDump.Episode
    ) -> [AdWindow] {
        episode.adWindows.enumerated().map { index, window in
            AdWindow(
                id: "\(episode.episodeId)-w\(index)",
                analysisAssetId: episode.episodeId,
                startTime: window.startTime,
                endTime: window.endTime,
                confidence: 0.9,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
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

    /// Ascending, duplicate-free gap list. Two arms sharing a NAME would
    /// accumulate into one bucket and double every count — which is exactly what
    /// happened the first time the production default moved onto a swept value.
    private func uniqueGaps(_ gaps: [Double], excluding: [Double]) -> [Double] {
        Array(Set(gaps).subtracting(Set(excluding))).sorted()
    }

    // MARK: - The eval

    /// One measurement arm: which link sources are enabled.
    private struct Arm {
        let name: String
        let lexical: Bool
        let rhetorical: Bool
        let singleStrongKind: Bool
        let gap: Double
    }

    private struct Outcome {
        var detectedSlots = 0
        var holesOver30Before = 0
        var holesOver30After = 0
        var uncoveredSecondsBefore = 0.0
        var uncoveredSecondsAfter = 0.0
        var recoveredSeconds = 0.0
        var recoveredOutsideSlots = 0.0
        var worsened: [String] = []
        var outOfSlotClaims: [String] = []
        var slotLines: [String] = []
        var markLines: [String] = []
    }

    func testPodContinuationReducesUncoveredRunsInsideDetectedSlots() throws {
        let environment = ProcessInfo.processInfo.environment
        // Defaults point at the MAIN checkout, mirroring `CorpusAudioFixtures`:
        // these three artifacts are git-ignored captures that live there and are
        // not copied into a bead worktree. Override with
        // `TEST_RUNNER_PLAYHEAD_POD_EVAL_*` when running from elsewhere.
        let pipelinePath = environment["PLAYHEAD_POD_EVAL_PIPELINE"]
            ?? "/Users/dabrams/playhead/playhead-dogfood-diagnostics-pipeline-dump-new9.json"
        let rediffPath = environment["PLAYHEAD_POD_EVAL_REDIFF"]
            ?? "/Users/dabrams/playhead/playhead-dogfood-diagnostics-tier-a-rediff.json"
        let transcriptDirectory = environment["PLAYHEAD_POD_EVAL_TRANSCRIPTS"]
            ?? "/Users/dabrams/playhead/TestFixtures/Corpus/Transcripts"
        for path in [pipelinePath, rediffPath, transcriptDirectory] {
            try XCTSkipUnless(
                FileManager.default.fileExists(atPath: path),
                "pod-continuation corpus eval artifact not staged: \(path)"
            )
        }

        let decoder = JSONDecoder()
        let pipeline = try decoder.decode(
            PipelineDump.self,
            from: try Data(contentsOf: URL(fileURLWithPath: pipelinePath))
        )
        let rediff = try decoder.decode(
            RediffDump.self,
            from: try Data(contentsOf: URL(fileURLWithPath: rediffPath))
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
        // ARMS. The two link sources are measured SEPARATELY so the effect can be
        // attributed, and so "the metric moved" cannot hide "it moved for a
        // different reason than the one claimed".
        let defaultGap = AdPodContinuation.Configuration.default.maxLinkGapSeconds
        var arms = [
            Arm(name: "A lexical only, two-kind bar, gap=30", lexical: true, rhetorical: false,
                singleStrongKind: false, gap: 30.0),
            Arm(name: "B rhetorical only, gap=30", lexical: false, rhetorical: true,
                singleStrongKind: false, gap: 30.0),
            Arm(name: "C lexical + rhetorical, two-kind bar, gap=30", lexical: true,
                rhetorical: true, singleStrongKind: false, gap: 30.0)
        ]
        // Calibration matrix: the two levers that the per-slot detail says are
        // binding — the chain GAP (a cue-free creative between two cued ones is
        // ~50 s wide) and the link BAR (several holes carry exactly one strong
        // kind). Measured rather than guessed, with the out-of-slot figure as the
        // show-eating proxy.
        // Deduplicated: the production default is one of the swept values, and a
        // repeated arm NAME would accumulate into the same bucket and silently
        // double every count.
        for gap in uniqueGaps([45.0, defaultGap, 60.0, 90.0], excluding: [30.0]) {
            arms.append(
                Arm(name: "D gap=\(Int(gap)) two-kind bar", lexical: true,
                    rhetorical: true, singleStrongKind: false, gap: gap)
            )
        }
        for gap in uniqueGaps([30.0, 45.0, defaultGap, 60.0, 90.0], excluding: []) {
            arms.append(
                Arm(name: "E gap=\(Int(gap)) + single-strong-kind links", lexical: true,
                    rhetorical: true, singleStrongKind: true, gap: gap)
            )
        }
        // The SHIPPING arm must be byte-equal to what `runBackfill` Step 18b
        // composes — both link sources, the default bar, the default gap — so the
        // assertions below gate exactly what a flag flip would turn on. Getting
        // this wrong once already measured a configuration production does not
        // run: `rhetoricalLinks` was in every arm and in NO production call site,
        // so the reported "13 -> 12" described a strict superset of the shipped
        // link set. The uniqueness assertion below is the other half of that
        // lesson.
        let shippingArmName = "E gap=\(Int(defaultGap)) + single-strong-kind links"
        XCTAssertEqual(
            Set(arms.map(\.name)).count,
            arms.count,
            "two arms share a name — they would accumulate into one bucket and double every count"
        )
        XCTAssertTrue(
            arms.contains {
                $0.name == shippingArmName
                    && $0.lexical && $0.rhetorical
                    && $0.singleStrongKind
                    && $0.gap == defaultGap
            },
            "the shipping arm must match the production configuration exactly"
        )
        var outcomes: [String: Outcome] = [:]
        var episodesEvaluated = 0
        var episodesSkipped = 0
        var totalSlotCount = 0

        for episode in pipeline.episodes {
            guard let slots = slotsByEpisode[episode.episodeId] else { continue }
            totalSlotCount += slots.count
            guard !episode.adWindows.isEmpty else { continue }

            let transcriptURL = URL(fileURLWithPath: transcriptDirectory)
                .appendingPathComponent("\(episode.episodeId).json")
            guard let data = try? Data(contentsOf: transcriptURL),
                  let transcript = try? decoder.decode(WhisperTranscript.self, from: data),
                  !transcript.transcription.isEmpty else {
                episodesSkipped += 1
                continue
            }
            episodesEvaluated += 1

            let episodeChunks = chunks(from: transcript, assetId: episode.episodeId)
            let hits = scanner.collectHits(chunks: episodeChunks)
            // Conservative arm: the stricter two-distinct-kinds bar, passed
            // EXPLICITLY so this arm keeps measuring the strict rule even after
            // the production default flipped.
            let twoKindLinks = AdPodContinuation.adCopyLinks(
                chunks: episodeChunks,
                hits: hits,
                allowSingleStrongKindLinks: false
            )
            // Production default.
            let singleStrongLinks = AdPodContinuation.adCopyLinks(
                chunks: episodeChunks,
                hits: hits
            )
            let rhetoricalLinks = AdPodContinuation.rhetoricalLinks(chunks: episodeChunks)
            // FM scan rows are not in this dump; the spoken-return-marker barrier
            // is. Fewer barriers ⇒ MORE recovery, so every figure below is the
            // pessimistic (worst-case) end for safety and the optimistic end for
            // coverage. Stated in the header; do not read the coverage delta as a
            // floor.
            let barriers = AdPodContinuation.contentBarriers(
                semanticScanResults: [],
                lexicalHits: hits,
                chunks: episodeChunks
            )
            let baseline = adWindows(from: episode)
            let baselineIntervals: [Interval] = baseline.map {
                (start: $0.startTime, end: $0.endTime)
            }

            for arm in arms {
                var links: [AdPodContinuation.AdCopyLink] = []
                if arm.lexical { links += arm.singleStrongKind ? singleStrongLinks : twoKindLinks }
                if arm.rhetorical { links += rhetoricalLinks }
                let marks = AdPodContinuation.compose(
                    existingWindows: baseline,
                    adCopyLinks: AdPodContinuation.mergeLinks(links),
                    contentBarriers: barriers,
                    protectedRegions: [],
                    episodeDuration: episode.episodeDurationSeconds,
                    analysisAssetId: episode.episodeId,
                    config: AdPodContinuation.Configuration(maxLinkGapSeconds: arm.gap)
                )
                let markIntervals: [Interval] = marks.map {
                    (start: $0.startTime, end: $0.endTime)
                }
                var outcome = outcomes[arm.name] ?? Outcome()

                for mark in marks {
                    outcome.markLines.append(
                        String(
                            format: "    %-44@ %8.1f-%8.1f (%5.1fs)",
                            String(episode.episodeId.prefix(44)) as NSString,
                            mark.startTime,
                            mark.endTime,
                            mark.endTime - mark.startTime
                        )
                    )
                }
                let newSeconds = difference(markIntervals, minus: baselineIntervals)
                outcome.recoveredSeconds += totalLength(newSeconds)
                let outside = difference(newSeconds, minus: slots)
                outcome.recoveredOutsideSlots += totalLength(outside)
                for interval in outside where interval.end - interval.start >= 1.0 {
                    outcome.outOfSlotClaims.append(
                        String(
                            format: "    %@  %.1f-%.1f (%.1fs)",
                            String(episode.episodeId.prefix(44)),
                            interval.start,
                            interval.end,
                            interval.end - interval.start
                        )
                    )
                }

                // Cohort pinned to the BASELINE's detected slots.
                for slot in slots {
                    guard !clip(baselineIntervals, to: slot).isEmpty else { continue }
                    outcome.detectedSlots += 1
                    let before = largestHole(in: slot, covered: baselineIntervals)
                    let after = largestHole(in: slot, covered: baselineIntervals + markIntervals)
                    let uncoveredBefore = (slot.end - slot.start)
                        - totalLength(clip(baselineIntervals, to: slot))
                    let uncoveredAfter = (slot.end - slot.start)
                        - totalLength(clip(baselineIntervals + markIntervals, to: slot))
                    outcome.uncoveredSecondsBefore += uncoveredBefore
                    outcome.uncoveredSecondsAfter += uncoveredAfter
                    if before > Self.neighbourHoleThreshold { outcome.holesOver30Before += 1 }
                    if after > Self.neighbourHoleThreshold { outcome.holesOver30After += 1 }
                    if after > before + 0.000_1 {
                        outcome.worsened.append(
                            String(
                                format: "%@ slot %.1f-%.1f: hole %.1f -> %.1f",
                                episode.episodeId, slot.start, slot.end, before, after
                            )
                        )
                    }
                    outcome.slotLines.append(
                        String(
                            format: "    %-44@ slot %7.1f-%7.1f  hole %6.1f -> %6.1f  uncovered %6.1f -> %6.1f",
                            String(episode.episodeId.prefix(44)) as NSString,
                            slot.start,
                            slot.end,
                            before,
                            after,
                            uncoveredBefore,
                            uncoveredAfter
                        )
                    )
                }
                outcomes[arm.name] = outcome
            }
        }

        try XCTSkipIf(episodesEvaluated == 0, "no corpus episode had both a dump and a transcript")

        var report = """

            == playhead-xsdz.65 pod-continuation corpus eval =====================
            episodes evaluated \(episodesEvaluated)   skipped \(episodesSkipped)
            rediff slots (all) \(totalSlotCount)

            """
        for arm in arms {
            guard let outcome = outcomes[arm.name] else { continue }
            report += """

                -- ARM: \(arm.name)
                detected slots (baseline cohort)      \(outcome.detectedSlots)
                slots with uncovered run > 30 s       \(outcome.holesOver30Before) -> \(outcome.holesOver30After)
                uncovered seconds in detected slots   \(String(format: "%.0f", outcome.uncoveredSecondsBefore)) -> \(String(format: "%.0f", outcome.uncoveredSecondsAfter))
                newly claimed seconds                 \(String(format: "%.0f", outcome.recoveredSeconds))
                  OUTSIDE every rediff slot           \(String(format: "%.1f", outcome.recoveredOutsideSlots))
                slots that got WORSE                  \(outcome.worsened.count)
                \(outcome.worsened.map { "    " + $0 }.joined(separator: "\n"))
                \(outcome.outOfSlotClaims.isEmpty ? "" : "out-of-slot claims (>= 1 s):\n" + outcome.outOfSlotClaims.joined(separator: "\n"))

                """
        }
        let shipping = try XCTUnwrap(outcomes[shippingArmName])
        report += """

            per-slot detail (shipping arm):
            \(shipping.slotLines.joined(separator: "\n"))

            every mark emitted by the shipping arm (for hand-audit):
            \(shipping.markLines.joined(separator: "\n"))
            =====================================================================

            """
        print(report)

        // THE NUMBER THAT DECIDES WHETHER THIS SHIPS, asserted rather than printed.
        // A frozen budget of zero: at the shipping configuration every newly
        // claimed second landed inside a byte-confirmed DAI insertion, and there
        // is no reason for that to loosen. It was printed-only for one round, and
        // a printed number gates nothing — a change that started claiming minutes
        // of out-of-slot audio would have passed green.
        XCTAssertEqual(
            shipping.recoveredOutsideSlots,
            0.0,
            accuracy: 0.05,
            "newly claimed seconds outside every rediff slot must stay at the measured zero"
        )
        XCTAssertTrue(
            shipping.worsened.isEmpty,
            "no detected slot may lose coverage: \(shipping.worsened)"
        )
        XCTAssertLessThan(
            shipping.uncoveredSecondsAfter,
            shipping.uncoveredSecondsBefore,
            "the pass must reduce uncovered ad seconds inside detected slots"
        )
        XCTAssertLessThan(
            shipping.holesOver30After,
            shipping.holesOver30Before,
            "the pass must reduce the count of detected slots carrying a >30 s uncovered run"
        )
    }
}
