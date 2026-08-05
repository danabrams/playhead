// SemanticBackfillAdmissionTests.swift
// playhead-i7qe: the predicate that decides whether a pre-analysis run may
// SKIP the semantic ad-scan backfill.
//
// Why this file exists. Before the fix the predicate was
// `!wroteNewChunks && hasExistingWindows && !hasCandidateWindows`, and both of
// its terms measure work that already exists rather than audio that was never
// read. `!wroteNewChunks` is a transcript-coverage term and is permanently true
// once the transcript completes; `!hasCandidateWindows` is a quantity that names
// an ABSENCE — candidate windows are produced BY the semantic scan, so their
// absence in the unscanned part of an episode is the absence of scanning, not
// evidence that scanning finished. Together they made the semantic scan
// unreachable for exactly the shape playhead-i7qe describes.
//
// The fixtures below are real rows from the product owner's device
// (analysis.sqlite, pulled 2026-07-29), with ad-scan fractions computed by the
// production definition (`AnalysisCoverageSummary.adScanFraction`).

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisJobRunner.shouldSkipSemanticBackfill — playhead-i7qe")
struct SemanticBackfillAdmissionTests {

    // MARK: - The bead's fixture, exactly

    /// Asset 820134BF, the audited episode. 2,113 s of audio; fast transcript
    /// reaches 2,113 s (100%); the coverage-lane semantic scan examined 820 s
    /// (0.388); `confirmedAdCoverageEndTime` was NULL when the bead was filed;
    /// and the asset carries three `applied`, two `confirmed` and three
    /// `reverted` ad windows — so windows exist and NONE of them is a
    /// candidate. That combination is what made the run skip.
    @Test("820134BF's exact shape is admitted, not skipped")
    func auditedEpisodeShapeIsAdmitted() {
        let skip = AnalysisJobRunner.shouldSkipSemanticBackfill(
            wroteNewChunks: false,     // transcript already complete
            hasExistingWindows: true,  // 8 windows on the row
            hasCandidateWindows: false, // none of them a candidate
            adScanFraction: 0.388      // measured coverage-lane area
        )
        #expect(skip == false, "an episode 61% unread for ads must not be skipped")
    }

    /// The bead's other stalled assets, plus the two `completeFull` rows whose
    /// scan never finished. Every one of them has the same "no new chunks, no
    /// candidates" signature, so every one of them was unreachable.
    @Test(
        "every stalled device row is admitted",
        arguments: [ReachRatio]([
            0.095,  // 1A9616D1 — transcript 100%, 3 passA rows
            0.542,  // B5786B41 — transcript 100%
            0.275,  // 144C8A80 — completeFull at 27.5%
            0.187,  // 8FECFDDE — completeFull at 18.7%
            0.026,  // B10C7BC8 — completeFull at 2.6%
            0.0,    // 7A481794 — passA rows exist, examined nothing
        ])
    )
    func stalledDeviceRowsAreAdmitted(adScanFraction: ReachRatio) {
        #expect(
            AnalysisJobRunner.shouldSkipSemanticBackfill(
                wroteNewChunks: false,
                hasExistingWindows: true,
                hasCandidateWindows: false,
                adScanFraction: adScanFraction
            ) == false
        )
    }

    // MARK: - The skip is still reachable

    /// A fix that made the skip unreachable would burn a Foundation Models
    /// pass on every job run for every already-scanned episode. The skip must
    /// still fire once the audio has demonstrably been read.
    @Test("a fully-scanned idle asset is still skipped")
    func fullyScannedIdleAssetIsSkipped() {
        #expect(
            AnalysisJobRunner.shouldSkipSemanticBackfill(
                wroteNewChunks: false,
                hasExistingWindows: true,
                hasCandidateWindows: false,
                adScanFraction: 1.0
            )
        )
    }

    @Test("the skip floor is the same number the library checkmark uses")
    func skipFloorMatchesReadinessThreshold() {
        // Two different floors would produce episodes the pipeline considers
        // finished and the surface still renders ◐, with nothing able to close
        // the gap.
        #expect(
            AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
                == episodePreparationCompleteThreshold
        )
        #expect(
            AnalysisJobRunner.shouldSkipSemanticBackfill(
                wroteNewChunks: false,
                hasExistingWindows: true,
                hasCandidateWindows: false,
                adScanFraction: episodePreparationCompleteThreshold
            )
        )
        #expect(
            AnalysisJobRunner.shouldSkipSemanticBackfill(
                wroteNewChunks: false,
                hasExistingWindows: true,
                hasCandidateWindows: false,
                adScanFraction: ReachRatio(episodePreparationCompleteThreshold.rawValue - 0.001)
            ) == false
        )
    }

    // MARK: - Unmeasurable must not license a skip

    /// `nil` arrives for real: no coverage-lane row at all, a missing or
    /// non-positive duration, or a duration the transcript's own reach
    /// contradicts. In every case we do not know that the audio was read, and
    /// the honest response is to run the scan.
    @Test("unmeasurable ad-scan coverage never licenses a skip")
    func unmeasurableCoverageNeverSkips() {
        #expect(
            AnalysisJobRunner.shouldSkipSemanticBackfill(
                wroteNewChunks: false,
                hasExistingWindows: true,
                hasCandidateWindows: false,
                adScanFraction: nil
            ) == false
        )
    }

    @Test("non-finite ad-scan fractions never license a skip")
    func nonFiniteCoverageNeverSkips() {
        for poisoned: ReachRatio in [ReachRatio(.nan), ReachRatio(.infinity), ReachRatio(-.infinity)] {
            #expect(
                AnalysisJobRunner.shouldSkipSemanticBackfill(
                    wroteNewChunks: false,
                    hasExistingWindows: true,
                    hasCandidateWindows: false,
                    adScanFraction: poisoned
                ) == false,
                "\(poisoned) must not license a skip"
            )
        }
    }

    // MARK: - The pre-existing terms are preserved

    /// The three original short-circuits still hold — this bead ADDS a term,
    /// it does not relax the others. Each row below would have been admitted
    /// before the change and must still be admitted, even at full ad-scan
    /// coverage, because there is other work to do.
    @Test(
        "the original admission terms are unchanged",
        arguments: [
            (true, true, false),    // new chunks landed → re-run detection
            (true, false, false),   // new chunks, no windows yet
            (false, false, false),  // no windows at all → first detection pass
            (false, true, true),    // candidate windows outstanding → resolve them
        ]
    )
    func originalAdmissionTermsUnchanged(
        wroteNewChunks: Bool,
        hasExistingWindows: Bool,
        hasCandidateWindows: Bool
    ) {
        #expect(
            AnalysisJobRunner.shouldSkipSemanticBackfill(
                wroteNewChunks: wroteNewChunks,
                hasExistingWindows: hasExistingWindows,
                hasCandidateWindows: hasCandidateWindows,
                adScanFraction: 1.0
            ) == false
        )
    }

    /// Exhaustive over the three booleans at both a covered and an uncovered
    /// fraction: the ONLY combination that skips is "nothing else to do AND
    /// the audio has been read". Written as a truth table so a future
    /// re-ordering of the guards cannot quietly widen the skip.
    @Test("exactly one combination skips")
    func onlyOneCombinationSkips() {
        var skipped: [(Bool, Bool, Bool, ReachRatio)] = []
        for wroteNewChunks in [false, true] {
            for hasExistingWindows in [false, true] {
                for hasCandidateWindows in [false, true] {
                    for fraction: ReachRatio in [0.388, 1.0] {
                        if AnalysisJobRunner.shouldSkipSemanticBackfill(
                            wroteNewChunks: wroteNewChunks,
                            hasExistingWindows: hasExistingWindows,
                            hasCandidateWindows: hasCandidateWindows,
                            adScanFraction: fraction
                        ) {
                            skipped.append(
                                (wroteNewChunks, hasExistingWindows, hasCandidateWindows, fraction)
                            )
                        }
                    }
                }
            }
        }
        #expect(skipped.count == 1)
        #expect(skipped.first?.0 == false)
        #expect(skipped.first?.1 == true)
        #expect(skipped.first?.2 == false)
        #expect(skipped.first?.3 == 1.0)
    }
}
