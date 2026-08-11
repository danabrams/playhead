import Foundation
import Testing

@testable import Playhead

/// playhead-15d0: the ad scan's resume must consult the DURABLE SCREENED ROWS,
/// not only the contiguous cursor.
///
/// THE FIELD CASE these pin, measured on the 2026-08-10 device pull
/// (`db-evening2`, schema 47). Asset 66D32039 carries a transcript spanning
/// 0.78–1919.4 s and ten `passA` rows, while its
/// `backfill_jobs.progressCursor.lastProcessedUpperBoundSec` reads **194.46** —
/// two windows into the four its first attempt banked. Narrowing by that cursor
/// alone re-handed `planPassA` 1,724.94 s of the 1,918.62 s transcript
/// (89.9 %), of which the rows above the cursor already covered 1,721.28 s:
/// **99.8 %**.
///
/// **THAT 99.8 % IS WHAT THIS SUITE'S SUBJECT WOULD HAVE SAVED HAD THE
/// VERSIONS MATCHED. ON THAT ASSET IT SAVES ZERO.** The rows carry transcript
/// versions `9afa5627…` and `200ebdd5…`; the asset's chunks currently hash to
/// `9e574deb…` (R2 recomputed it the way `runBackfill` does and reproduced
/// `backfill_jobs.attemptTranscriptVersion` byte-for-byte), so
/// ``BackfillJobRunner/screenedSpans(rows:scanCohortJSON:transcriptVersion:jobPhase:)``
/// qualifies none of them.
///
/// **IT IS NOT INERT, though — the yield is whatever the LAST attempt banked
/// after transcription stopped moving.** Applying this predicate to every
/// coverage job on three pulls: 3 of 22 fire on `db-evening2` +
/// `db-vtjxverdict` (715.7 s of 60,594.2 s above-cursor audio, 1.18 %), and
/// FOUR of five on `db-overnight5` — a virgin install run overnight, pulled
/// 2026-08-11 — for 726.9 s of 6,472.1 s (11.2 %), or 343.4 s of 4,747.2 s
/// (7.2 %) counting only the jobs that can still resume. 66D32039 itself went
/// from zero qualifying rows to seven between those two pulls, so R1's
/// "self-priming" is confirmed; it simply cannot complete before transcription
/// does, because `transcriptVersion` is a per-EPISODE hash and one appended
/// chunk invalidates every row for the asset. The fixtures below hold the
/// version fixed on purpose — that is the state being specified, not the state
/// observed.
///
/// The rows also do not merge into two spans. They merge into SIX —
/// 0.78–115.26, 115.68–194.46, 195.66–276.18, 276.54–941.64, 943.32–1902.78,
/// 1903.2–1919.4 — because the `<=` touch-or-overlap rule bridges none of the
/// five gaps (0.42, 1.20, 0.36, 1.68, 0.42 s). Same outcome, since no segment
/// straddles those pauses; the earlier "1.0–194.0 and 196.0–1919.0, the whole
/// transcribed extent" overstated the contiguity. And the 194.46 cursor is not
/// the walk stopping at a pause in speech: `coarseCheckpointWalk` never
/// examines time adjacency, it stops at the first plan that was not fully
/// covered.
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

    /// R2: the SIX rows the device carries at version `200ebdd5…`, at their real
    /// bounds — not the two-span summary the earlier fixture used. The title
    /// says "at the version these rows carry" because that is the fixture's
    /// premise and it is NOT the device's current state: 66D32039 has since
    /// re-hashed to `9e574deb…`, so on the phone today this drops nothing.
    @Test("the measured asset: its six banked spans leave the resume nothing to plan, at the version those rows carry")
    func fieldCaseDropsEverythingTheRowsAlreadyScreened() throws {
        let rows = [
            makeRow(id: "r1", start: 0.78, end: 115.26),
            makeRow(id: "r2", start: 115.68, end: 194.46),
            makeRow(id: "r3", start: 195.66, end: 276.18),
            makeRow(id: "r4", start: 276.54, end: 941.64),
            makeRow(id: "r5", start: 943.32, end: 1902.78),
            makeRow(id: "r6", start: 1903.2, end: 1919.4)
        ]
        // The five inter-row gaps (0.42, 1.20, 0.36, 1.68, 0.42 s) are real
        // pauses in speech, and NOTHING lies inside them — that is what makes
        // the cursor's stop at 194.46 cost re-planning rather than describe a
        // hole. Each segment sits wholly inside one span.
        let inputs = makeInputs(segments: [
            makeSegment(index: 0, start: 0.78, end: 115.26),
            makeSegment(index: 1, start: 115.68, end: 194.46),
            makeSegment(index: 2, start: 195.66, end: 276.18),
            makeSegment(index: 3, start: 276.54, end: 941.64),
            makeSegment(index: 4, start: 943.32, end: 1902.78),
            makeSegment(index: 5, start: 1903.2, end: 1919.4)
        ])

        // The spans stay SIX. A merge rule that bridged a 1.68 s pause would
        // claim 4.08 s of audio no row covers, and would also make the
        // "two spans covering the whole transcribed extent" prose true by
        // changing the code to match it.
        let spans = BackfillJobRunner.screenedSpans(
            rows: rows,
            scanCohortJSON: Self.cohort,
            transcriptVersion: Self.transcriptVersion,
            jobPhase: .fullEpisodeScan
        )
        #expect(spans.count == 6, "the five inter-row pauses must not be bridged, got \(spans)")

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
        //
        // R2: this clause is also THE reason the yield is single-digit percent
        // rather than 99.8 %. `transcriptVersion` hashes the whole transcript's
        // text (`TranscriptAtomizer.versionHash`, a SHA-256 over the
        // length-prefixed `normalizedText` of every canonical chunk), so a
        // single appended chunk invalidates every row for the entire episode —
        // including rows over audio the append did not touch. That is a
        // per-EPISODE identity answering a per-WINDOW question.
        //
        // PROVEN, not argued: R2 re-implemented that chain from source
        // (canonicalize -> canonicalTimeOrder -> versionHash) and it reproduces
        // `backfill_jobs.attemptTranscriptVersion` BYTE-FOR-BYTE on 4 of the 5
        // assets in `db-overnight5` and 2 of the 3 in `db-evening2`; every
        // mismatch is an asset whose transcript was still growing. And
        // 66D32039's dead `9afa5627…` is byte-exact the hash of the 299 fast
        // chunks ending at or below 300 s — a scan that ran on a 300 s prefix,
        // invalidated wholesale by the rest of the episode arriving.
        //
        // Not widened here: the fix is a per-window content hash, a design
        // change rather than a clause. Refusing is still the correct direction —
        // a stale row costs one repeated FM call, a wrongly-credited one
        // strands audio.
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
