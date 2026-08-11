import Foundation
import Testing

@testable import Playhead

/// playhead-15d0: the ad scan's resume must consult the DURABLE SCREENED ROWS,
/// not only the contiguous cursor.
///
/// THE FIELD CASE these pin, measured on the 2026-08-10 device pull
/// (`db-evening2`, schema 47). Asset 66D32039 carries a transcript spanning
/// 1.0–1919.0 s and persisted, reusable `passA` rows covering 1.0–194.0 and
/// 196.0–1919.0 — the whole transcribed extent — while its
/// `backfill_jobs.progressCursor.lastProcessedUpperBoundSec` reads **194.46**,
/// because the plan-list contiguity walk stops at a 1.2 s pause in speech
/// (the transcript's last atom before the hole ends at 194.46 and the next
/// begins at 195.66). Narrowing by that cursor alone re-handed `planPassA`
/// 1,723 s of the 1,918 s transcript — 89.8 % — of which ZERO seconds was
/// unscreened.
///
/// The suite is deliberately weighted toward the REFUSALS rather than the
/// drops. Dropping a segment is irreversible for the life of the asset: the
/// planner never sees it again, so a row that wrongly licenses a drop strands
/// that audio exactly the way playhead-pmp9's hole did. Every predicate clause
/// therefore gets its own negative test, and each one is written so that
/// deleting that single clause turns it red.
@Suite("playhead-15d0: screened-window resume narrowing")
struct BackfillScreenedWindowResumeTests {
    // MARK: - The field case

    @Test("the measured asset: a 1.2 s speech pause no longer costs 1,723 s of re-planning")
    func fieldCaseDropsEverythingTheRowsAlreadyScreened() throws {
        // The two rows the device actually carries, at their real bounds.
        let rows = [
            makeRow(id: "r1", start: 1.0, end: 194.46),
            makeRow(id: "r2", start: 195.66, end: 1919.0)
        ]
        // Segments spanning the transcript, INCLUDING one on each side of the
        // pause. Nothing lies inside the pause itself — that is what makes the
        // cursor's stop an artifact rather than a hole.
        let inputs = makeInputs(segments: [
            makeSegment(index: 0, start: 1.0, end: 115.0),
            makeSegment(index: 1, start: 116.0, end: 194.46),
            makeSegment(index: 2, start: 195.66, end: 276.0),
            makeSegment(index: 3, start: 277.0, end: 1919.0)
        ])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        // Every segment is inside a screened span, so the pass has nothing to
        // do and reaches the empty-segments short-circuit — zero `tokenCount`
        // XPC round trips, zero FM inference, and the drain moves on.
        #expect(narrowed.segments.isEmpty)
    }

    @Test("the cursor's own narrowing is unchanged — this composes with it, it does not replace it")
    func cursorNarrowingStillAppliesBelowTheWatermark() throws {
        // Only the LOW half is screened. A resume that consulted the rows but
        // stopped honouring the cursor would be a different function; this
        // pins that the rows are additive.
        let rows = [makeRow(id: "r1", start: 0.0, end: 100.0)]
        let inputs = makeInputs(segments: [
            makeSegment(index: 0, start: 0.0, end: 100.0),
            makeSegment(index: 1, start: 100.0, end: 200.0)
        ])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.map(\.segmentIndex) == [1])
    }

    // MARK: - The refusals

    @Test("a segment STRADDLING a gap between two screened spans is kept")
    func straddlingSegmentSurvives() throws {
        // 100.0 -> 101.0 is a real hole: nothing screened it. A segment lying
        // across it must not be dropped on the strength of its neighbours.
        let rows = [
            makeRow(id: "r1", start: 0.0, end: 100.0),
            makeRow(id: "r2", start: 101.0, end: 200.0)
        ]
        let inputs = makeInputs(segments: [
            makeSegment(index: 0, start: 50.0, end: 150.0)
        ])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == 1)
    }

    @Test("spans that TOUCH merge, so a segment across an exact boundary is dropped")
    func touchingSpansMerge() throws {
        let rows = [
            makeRow(id: "r1", start: 0.0, end: 100.0),
            makeRow(id: "r2", start: 100.0, end: 200.0)
        ]
        let inputs = makeInputs(segments: [
            makeSegment(index: 0, start: 50.0, end: 150.0)
        ])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.isEmpty)
    }

    @Test("a segment only PARTIALLY covered at the top edge is kept")
    func partiallyCoveredSegmentSurvives() throws {
        let rows = [makeRow(id: "r1", start: 0.0, end: 100.0)]
        let inputs = makeInputs(segments: [
            makeSegment(index: 0, start: 90.0, end: 110.0)
        ])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == 1)
    }

    @Test("a row that did NOT examine its window licenses nothing")
    func nonExaminingStatusesLicenseNothing() throws {
        // One row per status the coverage reader also refuses. Each spans the
        // whole segment, so if the `didExamineWindow` clause were dropped the
        // segment would vanish and every case would fail.
        for status in [
            SemanticScanStatus.refusal,
            .guardrailViolation,
            .cancelled,
            .rateLimited,
            .inferenceTimeout,
            .exceededContextWindow,
            .decodingFailure,
            .unavailable,
            .thermalDeferred,
            .failedTransient
        ] {
            let rows = [makeRow(id: "r-\(status.rawValue)", start: 0.0, end: 100.0, status: status)]
            let inputs = makeInputs(segments: [makeSegment(index: 0, start: 10.0, end: 90.0)])

            let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
                inputs,
                screenedRows: rows,
                scanCohortJSON: Self.cohort,
                jobPhase: .fullEpisodeScan
            )

            #expect(narrowed.segments.count == 1, "\(status.rawValue) must not license a drop")
        }
    }

    @Test("the NO-WORK SENTINEL licenses nothing, though its status is a verdict")
    func noWorkSentinelLicensesNothing() throws {
        // This is the one that a status-only predicate gets wrong: the sentinel
        // is written with `.noAds`, whose `didExamineWindow` is true, and it
        // spans the whole attempted range while explicitly meaning "no work was
        // performed". It is exactly the row the empty-segments short-circuit
        // writes — so a scan that narrowed to nothing once would, on the next
        // attempt, read its own sentinel as proof the episode was screened.
        let rows = [
            makeRow(
                id: "r-sentinel",
                start: 0.0,
                end: 1919.0,
                status: .noAds,
                errorContext: "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)emptySegments"
            )
        ]
        let inputs = makeInputs(segments: [makeSegment(index: 0, start: 10.0, end: 90.0)])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == 1)
    }

    @Test("a row at a STALE transcript version licenses nothing")
    func staleTranscriptVersionLicensesNothing() throws {
        // The device transcribes continuously (+768 chunks over the measured
        // 4 h 20 m span), so a growing transcript re-segments and re-windows.
        // Credit must not survive that.
        let rows = [makeRow(id: "r1", start: 0.0, end: 100.0, transcriptVersion: "tx-OLD")]
        let inputs = makeInputs(segments: [makeSegment(index: 0, start: 10.0, end: 90.0)])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == 1)
    }

    @Test("a row from a DIFFERENT scan cohort licenses nothing")
    func differentScanCohortLicensesNothing() throws {
        let rows = [makeRow(id: "r1", start: 0.0, end: 100.0, scanCohortJSON: #"{"promptVersion":"OTHER"}"#)]
        let inputs = makeInputs(segments: [makeSegment(index: 0, start: 10.0, end: 90.0)])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == 1)
    }

    @Test("a passB row licenses nothing — refinement is not screening")
    func refinementRowLicensesNothing() throws {
        // passB runs only INSIDE a window passA already called `containsAd`,
        // and it is asked WHERE the edges are. Its span is not evidence that
        // the span was screened for PRESENCE.
        let rows = [makeRow(id: "r1", start: 0.0, end: 100.0, scanPass: "passB")]
        let inputs = makeInputs(segments: [makeSegment(index: 0, start: 10.0, end: 90.0)])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == 1)
    }

    @Test("a row from ANOTHER PHASE licenses nothing, however examined it is")
    func otherPhaseRowsLicenseNothing() throws {
        // R1 review. The call site already declines to narrow a TARGETED job.
        // This is the other direction, and it is the one that loses audio: a
        // `.fullEpisodeScan` job reading the spans a targeted phase persisted.
        //
        // Those spans OVER-CLAIM by construction. `TargetedWindowNarrower`
        // hands the classifier the union of DISJOINT per-anchor intervals, and
        // `planPassA` packs each window from a contiguous slice of that array
        // while stamping min(start)/max(end) over the slice — so a window
        // straddling two intervals persists a span covering every segment
        // between them, none of which was ever in a prompt. A random-audit
        // sample is the worst case: a handful of scattered windows whose
        // merged span is nearly the whole episode.
        //
        // The row below is otherwise perfect — examined, current cohort,
        // current transcript version, passA — so ONLY the phase clause can
        // refuse it.
        for phase in [
            BackfillJobPhase.scanRandomAuditWindows,
            .scanHarvesterProposals,
            .scanLikelyAdSlots,
            .metadataSeededRegion,
            .specialistHostReadScan
        ] {
            let rows = [makeRow(id: "r-\(phase.rawValue)", start: 0.0, end: 1919.0, jobPhase: phase)]
            let inputs = makeInputs(segments: [makeSegment(index: 0, start: 10.0, end: 90.0)])

            let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
                inputs,
                screenedRows: rows,
                scanCohortJSON: Self.cohort,
                jobPhase: .fullEpisodeScan
            )

            #expect(
                narrowed.segments.count == 1,
                "\(phase.rawValue) rows must not license a fullEpisodeScan drop"
            )
        }
    }

    @Test("the legacy unattributed jobPhase sentinel licenses nothing")
    func legacyShadowJobPhaseLicensesNothing() throws {
        // Rows written before the phase column was attributed carry the string
        // `"shadow"`. Under-claiming on them costs one repeated FM call;
        // crediting them would credit a population nobody can identify.
        let rows = [
            SemanticScanResult(
                id: "r-legacy",
                analysisAssetId: "asset-15d0",
                windowFirstAtomOrdinal: 0,
                windowLastAtomOrdinal: 1,
                windowStartTime: 0.0,
                windowEndTime: 1919.0,
                scanPass: "passA",
                transcriptQuality: .good,
                disposition: .noAds,
                spansJSON: "[]",
                status: .success,
                attemptCount: 1,
                errorContext: nil,
                inputTokenCount: nil,
                outputTokenCount: nil,
                latencyMs: nil,
                prewarmHit: false,
                scanCohortJSON: Self.cohort,
                transcriptVersion: Self.transcriptVersion
                // `jobPhase` left at its "shadow" default on purpose.
            )
        ]
        let inputs = makeInputs(segments: [makeSegment(index: 0, start: 10.0, end: 90.0)])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == 1)
    }

    @Test("a zero-width or inverted window licenses nothing")
    func degenerateWindowLicensesNothing() throws {
        let rows = [
            makeRow(id: "r-zero", start: 50.0, end: 50.0),
            makeRow(id: "r-inverted", start: 100.0, end: 0.0)
        ]
        let inputs = makeInputs(segments: [makeSegment(index: 0, start: 10.0, end: 90.0)])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: rows,
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == 1)
    }

    @Test("no qualifying row is an IDENTITY, not a rebuild")
    func noQualifyingRowsIsIdentity() throws {
        let inputs = makeInputs(segments: [
            makeSegment(index: 0, start: 0.0, end: 100.0),
            makeSegment(index: 1, start: 100.0, end: 200.0)
        ])

        let narrowed = BackfillJobRunner.narrowedForScreenedWindows(
            inputs,
            screenedRows: [],
            scanCohortJSON: Self.cohort,
            jobPhase: .fullEpisodeScan
        )

        #expect(narrowed.segments.count == inputs.segments.count)
        #expect(narrowed.transcriptVersion == inputs.transcriptVersion)
        #expect(narrowed.analysisAssetId == inputs.analysisAssetId)
        #expect(narrowed.podcastId == inputs.podcastId)
    }

    // MARK: - The span walk itself

    @Test("screenedSpans merges into ascending disjoint spans and never bridges a gap")
    func spanMergeIsDisjointAndNeverBridges() throws {
        // Deliberately out of order, with an overlap, a touch, and a gap.
        let rows = [
            makeRow(id: "c", start: 300.0, end: 400.0),
            makeRow(id: "a", start: 0.0, end: 100.0),
            makeRow(id: "b", start: 50.0, end: 150.0),
            makeRow(id: "d", start: 150.0, end: 200.0)
        ]

        let spans = BackfillJobRunner.screenedSpans(
            rows: rows,
            scanCohortJSON: Self.cohort,
            transcriptVersion: Self.transcriptVersion,
            jobPhase: .fullEpisodeScan
        )

        // [0,100] ∪ [50,150] ∪ [150,200] collapse; [300,400] stays separate
        // because 200 -> 300 is a genuine hole.
        #expect(spans.count == 2)
        #expect(spans[0].start == 0.0)
        #expect(spans[0].end == 200.0)
        #expect(spans[1].start == 300.0)
        #expect(spans[1].end == 400.0)
    }

    @Test("a fully NESTED span does not shorten the span that contains it")
    func nestedSpanDoesNotShortenItsContainer() throws {
        // `merged.end = max(last.end, span.end)` is what makes this hold; a
        // plain `= span.end` would truncate 0–400 to 0–100 and silently
        // un-screen 300 s of audio.
        let rows = [
            makeRow(id: "outer", start: 0.0, end: 400.0),
            makeRow(id: "inner", start: 50.0, end: 100.0)
        ]

        let spans = BackfillJobRunner.screenedSpans(
            rows: rows,
            scanCohortJSON: Self.cohort,
            transcriptVersion: Self.transcriptVersion,
            jobPhase: .fullEpisodeScan
        )

        #expect(spans.count == 1)
        #expect(spans[0].end == 400.0)
    }

    // MARK: - Fixtures

    private static let transcriptVersion = "tx-v47"
    private static let cohort = #"{"promptVersion":"coarse-v1"}"#

    private func makeInputs(segments: [AdTranscriptSegment]) -> BackfillJobRunner.AssetInputs {
        BackfillJobRunner.AssetInputs(
            analysisAssetId: "asset-15d0",
            podcastId: "podcast-15d0",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: "asset-15d0",
                transcriptVersion: Self.transcriptVersion
            ),
            transcriptVersion: Self.transcriptVersion,
            plannerContext: CoveragePlannerContext(
                observedEpisodeCount: 0,
                stableRecall: false,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: 0,
                periodicFullRescanIntervalEpisodes: 10
            )
        )
    }

    private func makeSegment(index: Int, start: Double, end: Double) -> AdTranscriptSegment {
        AdTranscriptSegment(
            atoms: [
                TranscriptAtom(
                    atomKey: TranscriptAtomKey(
                        analysisAssetId: "asset-15d0",
                        transcriptVersion: Self.transcriptVersion,
                        atomOrdinal: index
                    ),
                    contentHash: "hash-\(index)",
                    startTime: start,
                    endTime: end,
                    text: "segment \(index)",
                    chunkIndex: index
                )
            ],
            segmentIndex: index
        )
    }

    private func makeRow(
        id: String,
        start: Double,
        end: Double,
        status: SemanticScanStatus = .success,
        scanPass: String = "passA",
        transcriptVersion: String = BackfillScreenedWindowResumeTests.transcriptVersion,
        scanCohortJSON: String = BackfillScreenedWindowResumeTests.cohort,
        errorContext: String? = nil,
        jobPhase: BackfillJobPhase = .fullEpisodeScan
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: "asset-15d0",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: errorContext,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: transcriptVersion,
            jobPhase: jobPhase.rawValue,
            createdAt: 1_700_000_000.0
        )
    }
}
