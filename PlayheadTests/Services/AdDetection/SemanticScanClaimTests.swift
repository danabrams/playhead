// SemanticScanClaimTests.swift
// playhead-fil5: a transcript that finishes must leave behind a DURABLE
// request for the semantic ad scan, and every gate that refuses to dispatch
// one must say so in a row somebody can query.
//
// **The measurement.** 2026-08-03 device pull: 12 assets, only 3 with ANY
// semantic ad scan, ZERO at >=98 %. FOUR assets — 48E903D7, FCDDB309,
// 4FF3A238, 2C5C3699 — had zero `backfill_jobs` rows. The bead names three of
// them and gives 48E903D7 as "95 % transcript"; that figure is its
// `fastTranscriptCoverageEndTime` WATERMARK (2010 s of 2113 s) and its chunks
// disagree — they stop at 1440 s and cover 36.9 % as a bridged area, which is
// why this suite's fixtures never treat it as claimable. The two assets that
// are both transcribed and stranded are FCDDB309 (98.8 % bridged) and
// 4FF3A238 (98.9 %), and FCDDB309's `decision_events` prove `runBackfill`
// ran for it TWICE and minted nothing. Which of four gates dropped the work
// was recorded nowhere, and `playhead-onn6`'s re-drive sweep starts from
// assets that already HAVE resumable rows, so zero rows meant zero mints,
// forever.
//
// Two guarantees are pinned here:
//   1. every `runShadowFMPhase` bail leaves a named, durable row; and
//   2. an asset that reached the transcript finalize floor with no
//      coverage-lane row of any kind gets a claim, and the SAME reconcile
//      pass turns it into a dispatchable re-drive.

import Foundation
import os
import Testing

@testable import Playhead

// MARK: - The pure decisions

@Suite("Semantic scan claim — the predicates (playhead-fil5)")
struct SemanticScanClaimPredicateTests {

    /// UNMEASURED is not SUFFICIENT, and for this bead that is the load-bearing
    /// case rather than an edge: an asset that has never been scanned has no
    /// `semantic_scan_results` rows, so `adScanFraction` is `nil` — never a
    /// synthetic 0. Reading `nil` as covered would make the never-scanned asset
    /// the one case a claim is never minted for.
    @Test("an unmeasured ad scan is owed a scan")
    func unmeasuredIsOwed() {
        #expect(SemanticScanClaim.isOwed(adScanFraction: nil))
        #expect(SemanticScanClaim.isOwed(adScanFraction: ReachRatio(.nan)))
        #expect(SemanticScanClaim.isOwed(adScanFraction: ReachRatio(.infinity)))
    }

    /// The floor is the runner's own skip floor. If the two diverge, claims
    /// solicit passes the runner then declines to run.
    @Test("the claim floor is exactly the runner's skip floor")
    func claimFloorMatchesSkipFloor() {
        let floor = AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
        #expect(floor == episodePreparationCompleteThreshold)
        #expect(SemanticScanClaim.isOwed(adScanFraction: floor) == false)
        #expect(SemanticScanClaim.isOwed(adScanFraction: ReachRatio(floor.rawValue + 0.01)) == false)
        #expect(SemanticScanClaim.isOwed(adScanFraction: ReachRatio(floor.rawValue - 0.001)))
    }

    /// **The three MEASURED field shapes, plus two synthetic bounds — and the
    /// list used to claim all five were measured.** Re-derived from the
    /// 2026-08-03 pull in R5: exactly three of the twelve assets own any
    /// `semantic_scan_results` row, and their `adScanFraction` (coverage-lane
    /// `passA` windows that examined, intersected with the 5 s-bridged
    /// TRANSCRIBED region, over the declared duration) is
    /// **0.014** (53FC53E3 — one 35.8 s window at 2490–2525.8 s),
    /// **0.207** (AD5F3A0A, 885.3 s of 4280.9 s) and **0.380** (DE0784D8,
    /// 2097.2 s of 5522.7 s).
    ///
    /// The last is not just a replication: the device wrote
    /// `ad scan 0.380 < 0.980 (decodeFailure)` into that asset's own
    /// `analysis_assets.terminalReason`, so the number has a witness in the
    /// pull rather than only in this file. The values this list carried before
    /// R5 — `0.026` and `0.47` — match nothing in the pull.
    ///
    /// playhead-x0lb: 53FC53E3's figure read **0.000** here, and that number was
    /// correct under the definition in force when it was written — the bound was
    /// the FAST union alone, and this window lies entirely outside 53FC53E3's
    /// fast transcript, which stops dead at 2490.0 s. playhead-9y9e widened the
    /// bound to BOTH passes, and the asset has 32 final chunks spanning
    /// 2490.0–2525.82 s — exactly the window — so the intersection is now
    /// 35.82 s and the fraction is 0.0142, the figure playhead-41mu and
    /// playhead-5pyq both quote. (R1 review: this said the final chunks span
    /// 2490–**2493.4** s, which is the same defect one layer down — a 3.4 s span
    /// cannot produce a 35.82 s intersection, so the stated evidence
    /// contradicted the corrected number it was there to justify. Re-derived
    /// from `transcript_chunks`: `MIN(startTime)=2490.0`, `MAX(endTime)=2525.82`,
    /// 32 rows.) Re-derived both ways against
    /// `scratchpad/db-aug3-work/analysis.sqlite`: fast-only 0.0000, canonical
    /// 0.0142. A number computed under one definition and quoted under another is
    /// instance 7 of playhead-x0lb's catalogue, so it is corrected rather than
    /// left to be re-discovered. The argument list keeps `0.0` — it is a
    /// legitimate synthetic bound, and every value here is owed either way.
    ///
    /// `0.9` and `floor - epsilon` are deliberately synthetic: they pin the
    /// approach to the floor, which no field asset is anywhere near.
    @Test(
        "the device's unscanned assets are all owed a scan",
        arguments: [
            ReachRatio(0.0), ReachRatio(0.207), ReachRatio(0.380),
            ReachRatio(0.9),
            ReachRatio(AnalysisJobRunner.semanticBackfillSufficientAdScanFraction.rawValue - 0.001)
        ]
    )
    func fieldShapesAreOwed(fraction: ReachRatio) {
        #expect(SemanticScanClaim.isOwed(adScanFraction: fraction))
    }

    /// **The transcript floor is 0.95, not 0.98, and the difference decides
    /// real assets.** 0.95 is `finalizeBackfillMinCoverageRatio` — the
    /// TRANSCRIPT number, calibrated for the seconds a decoder chops off the
    /// end. Borrowing the ad-scan floor (`episodePreparationCompleteThreshold`,
    /// 0.98) would demand a transcript be more complete than the scan it is a
    /// prerequisite for, and on the 2026-08-03 pull it would drop both stranded
    /// assets the sweep actually reaches: FCDDB309 measures 98.8 % and
    /// 4FF3A238 98.9 % of their duration as bridged transcript area, so 0.98 is
    /// inside the noise of a decoder's tail while 0.95 is not.
    ///
    /// **What this floor does NOT admit, contrary to the claim it carried until
    /// R3.** 48E903D7 was described here as "transcribed to 95 %" and named as
    /// the asset the 0.95 choice rescues. 95 % is its `fastTranscriptCoverageEndTime`
    /// WATERMARK; its fast-pass transcript area is 33.1 % raw / 36.9 % bridged,
    /// because the 2010 s of reach comes from FINAL-pass chunks and its fast
    /// pass covers 699 s of a 2113 s episode. No numerator this gate is allowed
    /// to use admits it, at 0.95 or any other constant — which is correct, not
    /// a regression: `adScanCoveredSec` is intersected with the same fast-pass
    /// region, so a claim for 48E903D7 would mint a re-drive whose scan
    /// fraction could never reach the floor that retires it. It stays
    /// playhead-9y9e's problem.
    @Test("the transcript gate uses the finalize floor, which admits 95%")
    func transcriptFloorAdmits95Percent() {
        #expect(AnalysisCoordinator.finalizeBackfillMinCoverageRatio == 0.95)
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: 0.95 * 2_113, episodeDurationSec: 2_113
        ))
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: 2_113, episodeDurationSec: 2_113
        ))
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: 0.94 * 2_113, episodeDurationSec: 2_113
        ) == false)
    }

    /// A transcript whose chunks are speech runs separated by breaths — i.e.
    /// every real transcript. Speech 0.75 s, gap 0.11 s, which is FCDDB309's
    /// measured shape (2,240 chunks, 620 gaps, median gap 0.120 s).
    static func speechRanges(
        count: Int,
        speech: Double = 0.75,
        gap: Double = 0.11
    ) -> [(start: Double, end: Double)] {
        (0..<count).map { index in
            let start = Double(index) * (speech + gap)
            return (start: start, end: start + speech)
        }
    }

    /// Duration of an episode exactly filled by ``speechRanges(count:speech:gap:)``.
    static func speechSpan(count: Int, speech: Double = 0.75, gap: Double = 0.11) -> Double {
        Double(count) * (speech + gap) - gap
    }

    /// **The defect that made this sweep mint nothing in the field, in one
    /// test.** A `transcript_chunks` row spans first word to last word, so the
    /// RAW interval union of a COMPLETE transcript is only its speech density —
    /// 87 % here, and 87.3 % / 89.2 % for the two field assets the bead is named
    /// after. Comparing that to a 0.95 wall-clock floor is not a strict gate, it
    /// is an unreachable one: measured on the 2026-08-03 pull, **zero of twelve**
    /// assets cleared it, maximum 93.8 %.
    ///
    /// Bridging sub-ad-width gaps (playhead-pz32's
    /// ``AnalysisCoverageMath/adScanBridgeableGapSec``, the same 5 s the ad-scan
    /// area already uses) restores the reach reading. Both halves are asserted:
    /// without the raw expectation this passes for a fixture that was never
    /// gappy, and without the bridged one it passes for a gate that admits
    /// nothing.
    @Test("a complete transcript reads under the floor RAW and over it bridged")
    func interUtteranceGapsMustBeBridged() {
        let ranges = Self.speechRanges(count: 400)
        let duration = Self.speechSpan(count: 400)

        let raw = AnalysisCoverageMath.unionedSeconds(ranges)
        #expect(raw / duration < 0.95,
                "fixture is not gappy enough to prove anything: \(raw / duration)")
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: raw, episodeDurationSec: duration
        ) == false)

        let bridged = SemanticScanClaim.bridgedTranscriptCoveredSec(
            region: CoverageRegionFixtures.transcribed(ranges)
        )
        #expect(bridged > raw)
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: bridged, episodeDurationSec: duration
        ), "a fully transcribed episode must clear the transcript floor")
    }

    /// The other direction, and the reason bridging is safe: 5 s coalesces a
    /// breath and nothing else. A genuine untranscribed BLOCK — a music bed, an
    /// outro no pass read, a real untranscribed block — survives it,
    /// so the gate still refuses an asset whose transcript has real holes.
    @Test("bridging closes breaths, not blocks")
    func bridgingDoesNotConcealUntranscribedBlocks() {
        // 200 speech runs, then a 400 s hole, then 200 more.
        let head = Self.speechRanges(count: 200)
        let holeEnd = Self.speechSpan(count: 200) + 400
        let tail = Self.speechRanges(count: 200).map {
            (start: $0.start + holeEnd, end: $0.end + holeEnd)
        }
        let duration = holeEnd + Self.speechSpan(count: 200)

        let bridged = SemanticScanClaim.bridgedTranscriptCoveredSec(
            region: CoverageRegionFixtures.transcribed(head + tail)
        )
        #expect(bridged < 0.95 * duration,
                "a 400 s hole must not be bridged away")
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: bridged, episodeDurationSec: duration
        ) == false)
        // …and the gap really is wider than the bridge, or this proves nothing.
        #expect(400 > AnalysisCoverageMath.adScanBridgeableGapSec)
    }

    /// This gate SUPPRESSES a mint, so its unmeasurable direction is the
    /// opposite of `isOwed`'s: an asset whose transcript reach cannot be
    /// established has not been shown ready, and the transcript lane owns it.
    @Test("an unmeasurable transcript never clears the floor")
    func unmeasurableTranscriptDoesNotClear() {
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: nil, episodeDurationSec: 2_113) == false)
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: 2_113, episodeDurationSec: nil) == false)
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: 2_113, episodeDurationSec: 0) == false)
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: 2_113, episodeDurationSec: -1) == false)
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: .nan, episodeDurationSec: 2_113) == false)
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: -5, episodeDurationSec: 2_113) == false)
    }

    /// The prefix IS the device-pull query (`deferReason LIKE 'scan_claim:%'`),
    /// so every gate has to carry it and no two may share a string — a
    /// collision is an unattributable missing scan, the exact state this
    /// replaces.
    @Test("every gate carries the query prefix and a distinct reason")
    func gatesCarryDistinctPrefixedReasons() {
        for gate in SemanticScanClaim.Gate.allCases {
            #expect(gate.deferReason.hasPrefix(SemanticScanClaim.deferReasonPrefix))
        }
        #expect(SemanticScanClaim.Gate.allCases.count == 5)
        #expect(Set(SemanticScanClaim.Gate.allCases.map(\.deferReason)).count == 5)
    }

    /// The prefix has to own its namespace. `backfill_jobs.deferReason` is also
    /// written by the admission controller, and if one of ITS reasons began
    /// with `scan_claim:` the device-pull query would report a thermal defer as
    /// a closed gate.
    @Test("no admission defer reason collides with the claim prefix")
    func admissionReasonsDoNotCollide() {
        for reason in AdmissionDeferReason.allCases {
            #expect(reason.rawValue.hasPrefix(SemanticScanClaim.deferReasonPrefix) == false,
                    "\(reason.rawValue) is an admission defer, not a scan claim")
        }
    }

    // MARK: - Identity

    /// **The claim must name the job the runner would derive**, or it is an
    /// orphan sitting next to the row that actually did the work.
    ///
    /// The expectation is derived from ``CoveragePlanner/plan(for:)`` rather
    /// than restating `(.fullEpisodeScan, 0)`, and that is the point of the
    /// test rather than a flourish. `SemanticScanClaim.jobId` hardcodes that
    /// tuple; the runner derives its own from `plan.phases.enumerated()`. The
    /// two agree only because the `fullCoverage` plan happens to be exactly
    /// `[.fullEpisodeScan]` today, and nothing else in the suite says so — a
    /// version of this test that wrote the tuple on both sides would keep
    /// passing while every claim row on disk silently orphaned the moment the
    /// plan grew a phase ahead of `fullEpisodeScan`. Written this way, that
    /// change lands here.
    @Test("the claim's id is the runner's fullCoverage job id")
    func claimIdIsTheRunnersId() throws {
        // A cold-start podcast, which is the branch that returns fullCoverage.
        let plan = CoveragePlanner().plan(for: CoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        ))
        #expect(plan.policy == .fullCoverage)
        #expect(
            plan.phases == [.fullEpisodeScan],
            "the claim's hardcoded (.fullEpisodeScan, offset 0) is the runner's id only while this is the whole plan"
        )

        // Derived exactly the way `runPendingBackfill` derives it: the phase and
        // its index in the plan.
        let planned = plan.phases.enumerated().map { offset, phase in
            BackfillJobRunner.makeJobIdForTesting(
                analysisAssetId: "asset-1",
                transcriptVersion: "tv-1",
                phase: phase,
                offset: offset
            )
        }
        let expected = try #require(planned.first)
        #expect(SemanticScanClaim.jobId(analysisAssetId: "asset-1", transcriptVersion: "tv-1")
                == expected)
        // …and it is genuinely keyed on both halves.
        #expect(SemanticScanClaim.jobId(analysisAssetId: "asset-1", transcriptVersion: "tv-2")
                != expected)
        #expect(SemanticScanClaim.jobId(analysisAssetId: "asset-2", transcriptVersion: "tv-1")
                != expected)
    }

    /// **The version must be the CANONICALIZED one.** `runBackfill` atomizes
    /// `TranscriptChunkCanonicalizer.canonicalize(...)` output, in which final
    /// chunks REPLACE the fast coverage they overlap. Hashing the raw persisted
    /// rows would produce a different id for the same asset the moment a final
    /// pass had run — a claim nothing ever resolves.
    @Test("the persisted-chunk version matches what runBackfill atomizes")
    func persistedVersionMatchesCanonicalAtomization() {
        let raw = mixedPassChunks(assetId: "asset-mixed")
        let canonical = TranscriptChunkCanonicalizer.canonicalize(raw).chunks
        let atomized = TranscriptAtomizer.atomize(
            chunks: canonical,
            analysisAssetId: "asset-mixed",
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        ).version.transcriptVersion

        #expect(SemanticScanClaim.transcriptVersion(forPersistedChunks: raw) == atomized)
        // The fixture has to actually exercise the difference, or the test is
        // vacuous: canonicalization must drop something.
        #expect(canonical.count < raw.count,
                "fixture must have overlapping fast+final chunks for this to prove anything")
        #expect(TranscriptAtomizer.transcriptVersionHash(chunks: raw) != atomized,
                "raw-vs-canonical must differ, else the canonicalization step is untested")
    }

    /// The extracted hash is the SAME hash `atomize` reports — extraction, not
    /// a second implementation.
    @Test("the standalone version hash equals atomize's")
    func standaloneHashEqualsAtomize() {
        let chunks = simpleChunks(assetId: "asset-h")
        let viaAtomize = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: "asset-h",
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        ).version.transcriptVersion
        #expect(TranscriptAtomizer.transcriptVersionHash(chunks: chunks) == viaAtomize)
        // Order-independent (both sides sort), content-dependent.
        #expect(TranscriptAtomizer.transcriptVersionHash(chunks: chunks.reversed())
                == viaAtomize)
        #expect(TranscriptAtomizer.transcriptVersionHash(chunks: Array(chunks.dropLast()))
                != viaAtomize)
    }

    // MARK: - The row

    /// `retryCount: 0` is load-bearing. Every resumability query also demands
    /// `retryCount < AdmissionController.maxRetries`, so charging a closed gate
    /// as a failed attempt would let three of them permanently retire an asset
    /// that has never once been scanned.
    @Test("the claim row is resumable, unbudgeted, and names its gate")
    func claimRowShape() {
        let row = SemanticScanClaim.claimRow(
            analysisAssetId: "asset-1",
            podcastId: "pod-1",
            transcriptVersion: "tv-1",
            gate: .foundationModelsUnavailable,
            createdAt: 1_000
        )
        #expect(row.status == .deferred)
        #expect(row.retryCount == 0)
        #expect(row.retryCount < AdmissionController.maxRetries)
        #expect(row.phase == .fullEpisodeScan)
        #expect(row.coveragePolicy == .fullCoverage)
        #expect(row.deferReason == "scan_claim:fm_unavailable")
        #expect(row.progressCursor == nil)
        #expect(row.createdAt == 1_000)
        // The priority the runner itself stamps — not a second table.
        #expect(row.priority == BackfillJobRunner.phasePriority(.fullEpisodeScan))
    }

    /// An empty id is the ABSENCE of a podcast, not a podcast. Persisting ""
    /// would hand the planner-state lookup a key that matches nothing and reads
    /// as a real miss — the same confusion that closes the `podcastIdMissing`
    /// gate in the first place.
    @Test("an empty podcastId persists as nil, not as an empty-string podcast")
    func emptyPodcastIdBecomesNil() {
        #expect(SemanticScanClaim.claimRow(
            analysisAssetId: "a", podcastId: "", transcriptVersion: "tv",
            gate: .podcastIdMissing, createdAt: 0
        ).podcastId == nil)
        #expect(SemanticScanClaim.claimRow(
            analysisAssetId: "a", podcastId: nil, transcriptVersion: "tv",
            gate: .podcastIdMissing, createdAt: 0
        ).podcastId == nil)
        #expect(SemanticScanClaim.claimRow(
            analysisAssetId: "a", podcastId: "pod", transcriptVersion: "tv",
            gate: .podcastIdMissing, createdAt: 0
        ).podcastId == "pod")
    }
}

// MARK: - Persistence

@Suite("Semantic scan claim — persistence (playhead-fil5)")
struct SemanticScanClaimPersistenceTests {

    private let logger = Logger(subsystem: "com.playhead.tests", category: "fil5")

    private func seed(
        _ store: AnalysisStore,
        assetId: String,
        durationSec: Double = 2_113,
        transcriptEndSec: Double = 2_113,
        scannedSec: Double? = nil
    ) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: assetId,
                episodeId: "ep-\(assetId)",
                assetFingerprint: "fp-\(assetId)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(assetId).m4a",
                featureCoverageEndTime: transcriptEndSec,
                fastTranscriptCoverageEndTime: transcriptEndSec,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: durationSec
            )
        )
        _ = try await store.insertTranscriptChunks([
            claimTestChunk(assetId: assetId, index: 0, start: 0, end: transcriptEndSec)
        ])
        if let scannedSec {
            try await store.insertSemanticScanResult(
                claimTestScan(assetId: assetId, index: 0, start: 0, end: scannedSec)
            )
        }
    }

    /// The whole point: after a closed gate, the database says a scan is owed
    /// and which gate refused it.
    @Test("a bail mints one durable, resumable, named row")
    func bailMintsDurableRow() async throws {
        let store = try await makeTestStore()
        try await seed(store, assetId: "a-mint")

        let outcome = await SemanticScanClaim.record(
            gate: .foundationModelsUnavailable,
            analysisAssetId: "a-mint",
            podcastId: "pod-1",
            transcriptVersion: "tv-1",
            store: store,
            clock: { 5_000 },
            logger: logger
        )
        #expect(outcome == .minted)

        let jobId = SemanticScanClaim.jobId(analysisAssetId: "a-mint", transcriptVersion: "tv-1")
        let row = try #require(try await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .deferred)
        #expect(row.deferReason == SemanticScanClaim.Gate.foundationModelsUnavailable.deferReason)
        #expect(row.createdAt == 5_000)
        // And the asset is now visible to the re-drive sweep, which is the
        // structural fix: it selects on rows that used not to exist.
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10) == ["a-mint"])
        #expect(try await store.countResumableBackfillJobs(assetId: "a-mint") == 1)
    }

    /// A gate that closes on every pass must not manufacture rows or spend the
    /// retry budget. Deterministic ids make the second write a refresh.
    @Test("repeated bails refresh one row and never charge a retry")
    func repeatedBailsAreIdempotent() async throws {
        let store = try await makeTestStore()
        try await seed(store, assetId: "a-idem")

        #expect(await SemanticScanClaim.record(
            gate: .fmModeOff, analysisAssetId: "a-idem", podcastId: nil,
            transcriptVersion: "tv-1", store: store, clock: { 1 }, logger: logger
        ) == .minted)
        for _ in 0..<4 {
            #expect(await SemanticScanClaim.record(
                gate: .foundationModelsUnavailable, analysisAssetId: "a-idem", podcastId: nil,
                transcriptVersion: "tv-1", store: store, clock: { 2 }, logger: logger
            ) == .refreshed)
        }

        #expect(try await store.countResumableBackfillJobs(assetId: "a-idem") == 1)
        let jobId = SemanticScanClaim.jobId(analysisAssetId: "a-idem", transcriptVersion: "tv-1")
        let row = try #require(try await store.fetchBackfillJob(byId: jobId))
        #expect(row.retryCount == 0, "a closed gate is not a failed attempt")
        // The most recent cause is the one visible.
        #expect(row.deferReason == SemanticScanClaim.Gate.foundationModelsUnavailable.deferReason)
    }

    /// The third rail on the empty-podcast normalization, added because the
    /// SC09 mutant survived: the pure constructor test pins the decision, the
    /// gate wire-in pins that the caller does not pre-empt it, and this pins
    /// that the value actually reaches SQLite as NULL. A `TEXT` column will
    /// take `''` happily, and every later reader that asks `podcastId != nil`
    /// would then get the wrong answer.
    @Test("an empty podcastId reaches the database as NULL")
    func emptyPodcastIdPersistsAsNull() async throws {
        let store = try await makeTestStore()
        try await seed(store, assetId: "a-empty-pod")
        #expect(await SemanticScanClaim.record(
            gate: .podcastIdMissing, analysisAssetId: "a-empty-pod", podcastId: "",
            transcriptVersion: "tv-1", store: store, logger: logger
        ) == .minted)
        let jobId = SemanticScanClaim.jobId(
            analysisAssetId: "a-empty-pod", transcriptVersion: "tv-1"
        )
        #expect(try await store.fetchBackfillJob(byId: jobId)?.podcastId == nil)
    }

    /// A scan that has already read the episode is not owed another one. If
    /// this fired anyway the claim would say "wanted" about work that is done.
    @Test("no claim when the measured ad scan already clears the floor")
    func sufficientCoverageMintsNothing() async throws {
        let store = try await makeTestStore()
        try await seed(store, assetId: "a-done", scannedSec: 2_113)

        #expect(await SemanticScanClaim.record(
            gate: .foundationModelsUnavailable, analysisAssetId: "a-done", podcastId: nil,
            transcriptVersion: "tv-1", store: store, logger: logger
        ) == .notOwed)
        #expect(try await store.countResumableBackfillJobs(assetId: "a-done") == 0)
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10).isEmpty)
    }

    /// A short scan IS owed one — the counterpart to the test above, so the
    /// coverage read is proven to discriminate rather than always allow.
    @Test("a short measured ad scan still mints")
    func shortCoverageMints() async throws {
        let store = try await makeTestStore()
        try await seed(store, assetId: "a-short", scannedSec: 1_000)
        #expect(await SemanticScanClaim.record(
            gate: .runnerFactoryMissing, analysisAssetId: "a-short", podcastId: nil,
            transcriptVersion: "tv-1", store: store, logger: logger
        ) == .minted)
    }

    /// A completed scan row under the same transcript version means the work
    /// this claim would request has already been done.
    @Test("a complete row is left alone")
    func completeRowIsSatisfied() async throws {
        let store = try await makeTestStore()
        try await seed(store, assetId: "a-complete")
        let jobId = SemanticScanClaim.jobId(analysisAssetId: "a-complete", transcriptVersion: "tv-1")
        try await store.insertBackfillJob(makeBackfillJob(
            jobId: jobId, analysisAssetId: "a-complete", status: .complete
        ))

        #expect(await SemanticScanClaim.record(
            gate: .fmModeOff, analysisAssetId: "a-complete", podcastId: nil,
            transcriptVersion: "tv-1", store: store, logger: logger
        ) == .alreadySatisfied)
        #expect(try await store.fetchBackfillJob(byId: jobId)?.status == .complete)
    }

    /// `running` belongs to whoever holds the admission ticket, and `failed`
    /// carries the reason the job actually lost — which is more specific than a
    /// gate name. Overwriting either would destroy information.
    @Test("running and failed rows keep their own state and reason")
    func inFlightAndFailedRowsAreLeftInPlace() async throws {
        let store = try await makeTestStore()
        for (assetId, status) in [("a-running", BackfillJobStatus.running),
                                  ("a-failed", .failed)] {
            try await seed(store, assetId: assetId)
            let jobId = SemanticScanClaim.jobId(
                analysisAssetId: assetId, transcriptVersion: "tv-1"
            )
            try await store.insertBackfillJob(makeBackfillJob(
                jobId: jobId, analysisAssetId: assetId,
                deferReason: "daemon_throttled", status: status
            ))

            #expect(await SemanticScanClaim.record(
                gate: .fmModeOff, analysisAssetId: assetId, podcastId: nil,
                transcriptVersion: "tv-1", store: store, logger: logger
            ) == .leftInPlace)
            let row = try #require(try await store.fetchBackfillJob(byId: jobId))
            #expect(row.status == status)
            #expect(row.deferReason == "daemon_throttled")
        }
    }

    /// Best-effort by contract. A store that cannot take the write must not
    /// throw into a detection pass whose invariant is that the shadow lane
    /// never affects cue computation.
    @Test("a store refusal is reported, not thrown")
    func storeRefusalIsSwallowed() async throws {
        let store = try await makeTestStore()
        // No asset row: the FK on `backfill_jobs.analysisAssetId` rejects the
        // insert. The claim reports the failure instead of propagating it.
        #expect(await SemanticScanClaim.record(
            gate: .fmModeOff, analysisAssetId: "ghost", podcastId: nil,
            transcriptVersion: "tv-1", store: store, logger: logger
        ) == .failed)
    }
}

// MARK: - Shared fixtures

func claimTestChunk(
    assetId: String,
    index: Int,
    start: Double,
    end: Double,
    pass: String = "fast",
    text: String? = nil
) -> TranscriptChunk {
    let body = text ?? "segment \(index)"
    return TranscriptChunk(
        id: "\(assetId)-\(pass)-chunk-\(index)",
        analysisAssetId: assetId,
        segmentFingerprint: "\(assetId)-seg-\(index)",
        chunkIndex: index,
        startTime: start,
        endTime: end,
        text: body,
        normalizedText: body,
        pass: pass,
        modelVersion: "test-asr",
        transcriptVersion: "tx-v1",
        atomOrdinal: index
    )
}

func claimTestScan(
    assetId: String,
    index: Int,
    start: Double,
    end: Double
) -> SemanticScanResult {
    SemanticScanResult(
        id: "\(assetId)-scan-\(index)",
        analysisAssetId: assetId,
        windowFirstAtomOrdinal: index * 10,
        windowLastAtomOrdinal: index * 10 + 9,
        windowStartTime: start,
        windowEndTime: end,
        scanPass: SemanticScanCoverage.coverageScanPass,
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
        scanCohortJSON: makeCohortJSON(promptLabel: "fil5"),
        transcriptVersion: "tx-v1",
        reuseScope: "\(assetId)-scan-\(index)"
    )
}

/// A single-pass transcript.
private func simpleChunks(assetId: String) -> [TranscriptChunk] {
    (0..<4).map { claimTestChunk(
        assetId: assetId, index: $0,
        start: Double($0) * 30, end: Double($0 + 1) * 30
    ) }
}

/// A MIXED fast/final transcript: the final chunks overlap — and therefore
/// replace — two of the fast ones, so canonicalization is not a passthrough.
private func mixedPassChunks(assetId: String) -> [TranscriptChunk] {
    let fast = (0..<4).map { claimTestChunk(
        assetId: assetId, index: $0,
        start: Double($0) * 30, end: Double($0 + 1) * 30,
        pass: "fast", text: "fast body \($0)"
    ) }
    let final = (1..<3).map { claimTestChunk(
        assetId: assetId, index: $0,
        start: Double($0) * 30, end: Double($0 + 1) * 30,
        pass: "final", text: "final body \($0)"
    ) }
    return fast + final
}
