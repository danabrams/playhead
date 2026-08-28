// SemanticScanClaimRetranscriptionTests.swift
// playhead-5q8l — a re-transcription must not cost the scan claim its row or
// the cursor an earlier pass earned.
//
// **The bead this rails, and why it is a rail rather than a fix.**
// playhead-5q8l was filed against a `SemanticScanClaim.jobId` that took a
// `transcriptVersion` and delegated to a runner id that hashed one, so the
// four-session sequence below minted a SECOND row and restarted the
// full-episode FM pass at zero:
//
//     session 1  transcript v1  gate closed  -> claim C1(v1), cursor NULL
//     session 2  transcript v1  gate open    -> C1 re-driven, earns a cursor
//     session 3  transcript v2  gate closed  -> claim C2(v2), cursor NULL
//     session 4  transcript v2  gate open    -> the pass RESTARTS at 0 and
//                                               C1's cursor is abandoned
//
// playhead-wxsv removed `transcriptVersion` from the id (`makeJobId` is
// `f(asset, phase, offset)`; the version became the `attemptTranscriptVersion`
// COLUMN) and deleted the unfinished-row fallback the bead's third option was
// written around. That is the bead's own option 3, shipped — so there was
// nothing left to fix, and what was missing was a rail. Nothing in the suite
// drove the claim path and the runner's M-5 branch TOGETHER across a version
// change: `SemanticScanClaimTests` pins the id in isolation, the wire-in suite
// pins the gates, and the runner suites never mint a claim. Each half can stay
// green while the composition regresses.
//
// EVIDENCE NOTE: fixture only. `TestFMRuntime` supplies canned model
// responses; nothing here is a device measurement. The FIELD reading that
// motivated the rail is in the bead comment — eight device pulls spanning
// 2026-08-08..2026-08-21, 137 `backfill_jobs` rows over 137 distinct assets
// and not one asset carrying a second row.

import Foundation
import SQLite3
import os
import Testing

@testable import Playhead

@Suite("playhead-5q8l: a re-transcription keeps ONE claim row and its cursor")
struct SemanticScanClaimRetranscriptionTests {

    private let logger = Logger(subsystem: "com.playhead.tests", category: "5q8l")

    private static let pinnedClock = Date(timeIntervalSince1970: 1_700_000_000)

    // MARK: - Fixtures

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: 90,
            fastTranscriptCoverageEndTime: 90,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.completeFull.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            // 3,600 s of episode against 90 s of transcript, so `adScanFraction`
            // cannot climb to the floor and `SemanticScanClaim.isOwed` stays TRUE
            // for the whole sequence. A short episode the fixture scans end to
            // end would send session 3 down the `.notOwed` arm, which returns
            // before the by-id lookup — the branch this file exists to exercise
            // would never run and every assertion would still be green.
            episodeDurationSec: 3_600
        )
    }

    /// Two transcripts over the same audio, differing in the third line and in
    /// the version they declare — which is what a re-transcription IS.
    ///
    /// The version is supplied rather than re-derived on purpose: how
    /// `transcriptVersion` is COMPUTED is pinned by
    /// `SemanticScanClaimTests.persistedVersionMatchesCanonicalAtomization`,
    /// and this file's subject is what the claim path and the runner DO with a
    /// version that moved. Deriving it here would make the rail depend on the
    /// hash's stability for no gain.
    private func makeInputs(
        assetId: String,
        version: String,
        tail: String
    ) -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: version,
            lines: [
                (0, 30, "Welcome to the show. Today we're discussing podcasts."),
                (30, 60, "Use code SHOW for 20 percent off at example dot com."),
                (60, 90, tail)
            ]
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "pod-\(assetId)",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: assetId,
                transcriptVersion: version
            ),
            transcriptVersion: version,
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

    private func makeRunner(store: AnalysisStore, runtime: FoundationModelClassifier.Runtime) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime, config: .default),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Self.pinnedClock },
            scenePhaseProvider: { ScanScenePhase.background.rawValue }
        )
    }

    /// The row count straight out of SQLite. `countResumableBackfillJobs` is
    /// NOT this quantity — it filters to resumable statuses inside the newest
    /// enqueue batch window, so a duplicate outside that window reads as zero.
    /// The defect this file rails is a SECOND ROW, so the assertion has to be
    /// about the rows on disk.
    private func rowCount(assetId: String, in directory: URL) throws -> Int {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw AnalysisStoreError.queryFailed("open failed")
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        // Interpolated rather than bound: `assetId` is this file's own literal,
        // and the surrounding suites read raw SQLite the same way.
        let sql = "SELECT COUNT(*) FROM backfill_jobs WHERE analysisAssetId = '\(assetId)'"
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw AnalysisStoreError.queryFailed("prepare failed")
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else {
            throw AnalysisStoreError.queryFailed("no row")
        }
        return Int(sqlite3_column_int(stmt, 0))
    }

    // MARK: - The bead's own four sessions

    /// **The acceptance rail.** Drive the bead's sequence verbatim through the
    /// real claim path and the real runner, and assert on the rows ON DISK
    /// after every session: there is exactly ONE, and it is the claim's.
    ///
    /// A regression that re-couples the job id to the transcript shows up here
    /// as `2` at session 4 — one row the runner drives from zero and one
    /// orphaned claim holding the cursor.
    @Test("the bead's four sessions leave exactly one row, at the claim's id")
    func fourSessionsLeaveOneRow() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        let assetId = "asset-5q8l-sessions"
        try await store.insertAsset(makeAsset(id: assetId))

        let v1 = makeInputs(
            assetId: assetId, version: "tx-5q8l-1",
            tail: "Now back to the interview with our guest."
        )
        let v2 = makeInputs(
            assetId: assetId, version: "tx-5q8l-2",
            tail: "Now back to our conversation with today's guest, live."
        )

        // ANTI-VACUITY: if the two transcripts hashed the same the whole test
        // would be one version driven twice, and every assertion below would
        // hold for reasons that have nothing to do with the bead.
        try #require(
            v1.transcriptVersion != v2.transcriptVersion,
            "the fixture must actually re-transcribe: two identical versions test nothing"
        )

        // The claim hard-codes (.fullEpisodeScan, offset 0). That is only the
        // runner's id while a fullCoverage plan is the whole plan.
        let plan = CoveragePlanner().plan(for: v1.plannerContext)
        try #require(plan.phases == [.fullEpisodeScan])
        let claimId = SemanticScanClaim.jobId(analysisAssetId: assetId)
        try #require(
            claimId == BackfillJobRunner.makeJobId(
                analysisAssetId: assetId, phase: .fullEpisodeScan, offset: 0
            ),
            "the claim must occupy the row the runner derives, or the rest of this proves nothing"
        )

        // ---- Session 1: transcript v1, gate CLOSED.
        #expect(await SemanticScanClaim.record(
            gate: .foundationModelsUnavailable,
            analysisAssetId: assetId,
            podcastId: "pod-\(assetId)",
            store: store,
            clock: { 1_000 },
            logger: logger
        ) == .minted)
        #expect(try rowCount(assetId: assetId, in: dir) == 1)
        let minted = try #require(try await store.fetchBackfillJob(byId: claimId))
        #expect(minted.status == .deferred)
        #expect(
            minted.attemptTranscriptVersion == nil,
            "a claim is a REQUEST — nothing has read a transcript yet, so no version may be stamped"
        )

        // ---- Session 2: transcript v1, gate OPEN. The runner must find the
        // claim BY ID and re-drive it, not mint beside it.
        let runner = makeRunner(store: store, runtime: TestFMRuntime().runtime)
        _ = try await runner.runPendingBackfill(for: v1)
        #expect(
            try rowCount(assetId: assetId, in: dir) == 1,
            "session 2 minted a second row: the claim is not the row the runner drives"
        )
        let afterV1 = try #require(try await store.fetchBackfillJob(byId: claimId))
        #expect(
            afterV1.attemptTranscriptVersion == v1.transcriptVersion,
            """
            The claim was not re-driven — it carries \
            \(afterV1.attemptTranscriptVersion ?? "no version") after a run at \
            \(v1.transcriptVersion). A claim the runner declines to drive is the \
            orphan playhead-fil5 exists to prevent.
            """
        )

        // ---- Session 3: transcript v2, gate CLOSED again. Whatever session 2
        // left the row in, a second closed gate must not add a row.
        let reclaim = await SemanticScanClaim.record(
            gate: .foundationModelsUnavailable,
            analysisAssetId: assetId,
            podcastId: "pod-\(assetId)",
            store: store,
            clock: { 2_000 },
            logger: logger
        )
        #expect(
            [.refreshed, .leftInPlace, .alreadySatisfied].contains(reclaim),
            """
            Session 3 reached \(reclaim). `.minted` is the bead verbatim — a gate \
            closing at a new transcript minting a SECOND claim. `.failed` is a store \
            error and `.notOwed` means the fixture let coverage reach the floor, \
            which would return before the by-id lookup and leave this leg vacuous; \
            neither is evidence either way.
            """
        )
        #expect(try rowCount(assetId: assetId, in: dir) == 1)

        // ---- Session 4: transcript v2, gate OPEN. Still one row, and it is
        // now stamped with the NEW version, i.e. the same row carried on.
        _ = try await runner.runPendingBackfill(for: v2)
        #expect(
            try rowCount(assetId: assetId, in: dir) == 1,
            """
            Session 4 left more than one backfill_jobs row for one asset. That is \
            the cursor-less duplicate full-episode pass playhead-5q8l was filed \
            for, back on a tree that fixed it.
            """
        )
        let afterV2 = try #require(try await store.fetchBackfillJob(byId: claimId))
        #expect(
            afterV2.attemptTranscriptVersion == v2.transcriptVersion,
            "the surviving row must be the one that ran at v2, not a fossil at v1"
        )
        #expect(
            afterV2.createdAt == minted.createdAt,
            "the row is the CLAIM's, carried forward — a changed createdAt means it was replaced"
        )
    }

    // MARK: - The cursor

    /// **The other half of the bead's cost: "abandoning a cursor an earlier row
    /// had already earned".**
    ///
    /// With the id stable, the only way a re-transcription can still cost the
    /// cursor is if the claim path CLEARS it on the way past.
    /// `SemanticScanClaim.record` refreshes an existing deferred row through
    /// `markBackfillJobDeferred`, which writes `status` and `deferReason` and
    /// nothing else — so the cursor, the retry budget and `createdAt` all
    /// survive. That is a property of one UPDATE statement's column list, which
    /// is exactly the kind of thing a later edit takes out without noticing.
    @Test("a second closed gate does not cost the cursor, the budget, or the createdAt")
    func aSecondClosedGateKeepsTheCursor() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-5q8l-cursor"
        try await store.insertAsset(makeAsset(id: assetId))

        #expect(await SemanticScanClaim.record(
            gate: .foundationModelsUnavailable,
            analysisAssetId: assetId,
            podcastId: "pod",
            store: store,
            clock: { 1_000 },
            logger: logger
        ) == .minted)

        // An earlier admitted pass got this far and deferred. Checkpointed
        // through the production writer rather than assembled, so the row is in
        // the state a real pass leaves it in.
        let claimId = SemanticScanClaim.jobId(analysisAssetId: assetId)
        let cursor = BackfillProgressCursor(
            processedPhaseCount: 1,
            lastProcessedUpperBoundSec: EpisodeSeconds(61.5)
        )
        try await store.checkpointBackfillJobProgress(jobId: claimId, progressCursor: cursor)
        try #require(
            try await store.fetchBackfillJob(byId: claimId)?.progressCursor != nil,
            "fixture precondition: the row must actually hold a cursor"
        )

        // The gate closes again — a different gate, so the refresh is visible.
        #expect(await SemanticScanClaim.record(
            gate: .fmModeOff,
            analysisAssetId: assetId,
            podcastId: "pod",
            store: store,
            clock: { 9_999 },
            logger: logger
        ) == .refreshed)

        let row = try #require(try await store.fetchBackfillJob(byId: claimId))
        #expect(
            row.progressCursor?.lastProcessedUpperBoundSec == EpisodeSeconds(61.5),
            """
            The refresh dropped the cursor (now \
            \(String(describing: row.progressCursor))). Every closed gate would then \
            cost the next pass everything the last one read — playhead-5q8l's stated \
            cost, arriving through the store instead of through the id.
            """
        )
        #expect(row.retryCount == 0, "a closed gate is not a failed attempt")
        #expect(row.createdAt == 1_000, "the refresh must not restamp the row's age")
        #expect(row.deferReason == SemanticScanClaim.Gate.fmModeOff.deferReason)
    }

    // MARK: - The mirror

    /// The bead's sequence starts with the CLAIM. The mirror — a row the RUNNER
    /// minted, then a gate closing at a different transcript — is the direction
    /// nothing drove, and it is the one that decides whether `record` can ever
    /// insert beside a real job.
    ///
    /// Not merely `record`'s own idempotence (`SemanticScanClaimTests` pins
    /// that against claims it minted itself): the row here was written by the
    /// runner, under a different code path, and the gate closes at a version
    /// that row never saw.
    @Test("a gate closing after a real run adds no row, whatever the transcript")
    func aGateClosingAfterARunAddsNoRow() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        let assetId = "asset-5q8l-mirror"
        try await store.insertAsset(makeAsset(id: assetId))

        let v1 = makeInputs(
            assetId: assetId, version: "tx-5q8l-mirror-1",
            tail: "Now back to the interview with our guest."
        )
        let v2 = makeInputs(
            assetId: assetId, version: "tx-5q8l-mirror-2",
            tail: "Now back to our conversation with today's guest, live."
        )
        try #require(v1.transcriptVersion != v2.transcriptVersion)

        let runner = makeRunner(store: store, runtime: TestFMRuntime().runtime)
        _ = try await runner.runPendingBackfill(for: v1)
        try #require(
            try rowCount(assetId: assetId, in: dir) == 1,
            "fixture precondition: the runner minted exactly one coverage-lane row"
        )
        // …and the transcript really moves on, so the row the gate meets was
        // stamped by a run at a version the claim path never saw. Asserted
        // rather than assumed: a row still reading v1 would make the rest of
        // this test a second copy of `record`'s own idempotence.
        _ = try await runner.runPendingBackfill(for: v2)
        let runnerRow = try #require(try await store.fetchBackfillJob(
            byId: SemanticScanClaim.jobId(analysisAssetId: assetId)
        ))
        try #require(
            runnerRow.attemptTranscriptVersion == v2.transcriptVersion,
            "fixture precondition: the row must carry the SECOND transcript's version"
        )

        // Only now does the gate close.
        let outcome = await SemanticScanClaim.record(
            gate: .podcastIdMissing,
            analysisAssetId: assetId,
            podcastId: nil,
            store: store,
            clock: { 5_000 },
            logger: logger
        )
        #expect(
            [.refreshed, .leftInPlace, .alreadySatisfied].contains(outcome),
            """
            The claim path reached \(outcome) against a row the RUNNER minted and \
            ran at a later transcript. `.minted` would be a second coverage-lane \
            row for one asset; `.failed` is a store error; `.notOwed` returns \
            before the by-id lookup and would leave this rail vacuous.
            """
        )
        #expect(
            try rowCount(assetId: assetId, in: dir) == 1,
            "one asset, one coverage-lane row, across a re-transcription and a closed gate"
        )
        #expect(
            try await store.fetchBackfillJob(
                byId: SemanticScanClaim.jobId(analysisAssetId: assetId)
            ) != nil,
            "and the single row is findable at the claim's id — the two halves share one identity"
        )
    }
}
