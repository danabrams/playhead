// BackfillCoverageTerminalTests.swift
// playhead-41mu: a `fullEpisodeScan` job may call itself `complete` only when
// the episode's MEASURED ad-scan coverage clears the sufficiency floor. Before
// this bead the terminal fired whenever `runJob` returned without a rate-limit
// hole, where "done" meant "I swept the segments I was handed" — and what it
// was handed is whatever chunk array its DISPATCHER chose.
//
// The two field witnesses, from the 2026-08-03 device pull, are the only two
// `fullEpisodeScan` rows that ever reached `complete` on that device. R1 review:
// they have DIFFERENT causes, and only this terminal is common to both.
//
//   53FC53E3  2,528 s episode, complete 23 s after creation, ONE 36 s window
//             durably examined (2,490–2,525.8). adScanFraction 0.0142. Its
//             cursor reads 2,525.82, asserting the episode is read. Its
//             `countResumableBackfillJobs` is ZERO, so the re-drive that would
//             have rescued it cannot mint. Its transcript was NOT early —
//             2,917 fast chunks over [0, 2490] were already on disk, and the
//             job's persisted transcriptVersion is byte-exact the hash of the
//             32 FINAL chunks alone, i.e. the dispatcher DISCARDED the fast
//             pass (`retryShadowFMPhaseForSession`, playhead-3ort — fixed in
//             playhead-iu0t, whose rails are in
//             `ShadowRetryCanonicalReplayTests`). The two beads are
//             complementary, not redundant: iu0t stops the discarding, this
//             terminal is what refuses to call the result `complete` if any
//             future dispatcher narrows the input again.
//   AD5F3A0A  4,281 s episode, complete with windows spanning 3–900 s — this
//             one IS the early-transcript shape (the 900 s tier, playhead-9new).
//             adScanFraction 0.2068. Its first segment starts at 2.8 s, so its
//             cursor of 900 IS a genuine episode prefix.
//
// The two shapes exercise the two branches of the cursor rule, and the suite is
// built around them. None boot the real Foundation Models stack.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-41mu: the coverage-lane terminal is measured, not claimed")
struct BackfillCoverageTerminalTests {

    // MARK: - Fixtures

    /// The same windowing setup `BackfillRateLimitDeferTests` uses: exactly one
    /// coarse window per 10 s segment.
    private static let contextSize = 431
    private static let coarseSchemaTokenCount = 4

    private func windowingTokenRule() -> @Sendable (String) -> Int {
        { prompt in prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 8 }
    }

    private func windowingConfig() -> FoundationModelClassifier.Config {
        FoundationModelClassifier.Config(
            safetyMarginTokens: 5,
            coarseMaximumResponseTokens: 6,
            refinementMaximumResponseTokens: 12,
            interWindowPacingNanos: 0
        )
    }

    private func makeAsset(id: String, episodeDurationSec: Double) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    }

    private func makeChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        pass: String
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(pass)-\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp-\(pass)-\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "t",
            normalizedText: "t",
            pass: pass,
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    private func makeInputs(
        assetId: String,
        lines: [(start: Double, end: Double, text: String)],
        transcriptVersion: String = "tx-41mu-v1"
    ) -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion
        )
        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-41mu",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: transcriptVersion,
            plannerContext: plannerContext
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime, config: windowingConfig()),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
    }

    private func makeRuntime() -> TestFMRuntime {
        TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
    }

    /// The 53FC53E3 shape at test scale: a 100 s episode whose transcript is
    /// complete in the store (fast `[0,90]` + final `[90,100]`), dispatched with
    /// ONLY the final-pass tail as segments. The job sweeps everything it was
    /// handed and has still read 10 % of the episode.
    private func makeHeadHoleStore(assetId: String) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 100))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 90, pass: "fast"),
            makeChunk(assetId: assetId, index: 1, start: 90, end: 100, pass: "final")
        ])
        return store
    }

    private func headHoleInputs(assetId: String) -> BackfillJobRunner.AssetInputs {
        makeInputs(
            assetId: assetId,
            lines: [(90, 100, "Closing remarks and outro material for the show.")]
        )
    }

    // MARK: - 1. The defect: a swept-what-I-was-handed pass does not complete

    @available(iOS 26.0, *)
    @Test("THE 53FC53E3 CASE — a pass that swept every segment it was handed and still read 1/10th of the episode DEFERS, it does not complete")
    func underCoveredFullEpisodeScanDefersInsteadOfCompleting() async throws {
        let assetId = "asset-41mu-headhole"
        let store = try await makeHeadHoleStore(assetId: assetId)
        let runtime = makeRuntime()

        let result = try await makeRunner(store: store, runtime: runtime.runtime)
            .runPendingBackfill(for: headHoleInputs(assetId: assetId))

        let jobId = try #require(result.admittedJobIds.first)
        // Every window it was given succeeded — this is NOT a rate-limit, a
        // guardrail or an expiry. The pass did exactly what it was asked.
        #expect(await runtime.coarseCallCount == 1)
        #expect(result.deferredJobIds.contains(jobId))

        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .deferred, "the terminal must be measured, not claimed")
        #expect(row.status != .complete)
        #expect(row.deferReason == "underCoverage-fullEpisodeScan",
                "the drop must leave a durable, queryable cause distinct from rateLimited-backoff and cancelled-during-…")

        // And the number the decision was made on is the pipeline's one ruler.
        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([assetId])[assetId]
        )
        let fraction = try #require(summary.adScanFraction)
        #expect(abs(fraction.rawValue - 0.10) < 0.001, "10 s of a 100 s episode")
        #expect(fraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction)
    }

    // MARK: - 2. The cursor must not bury the unscanned head

    @available(iOS 26.0, *)
    @Test("the under-coverage defer does NOT publish a cursor over audio the job never held — the 2,525.82-on-a-2,528 s-episode shape")
    func underCoverageDeferDoesNotPublishACursorOverTheUnscannedHead() async throws {
        let assetId = "asset-41mu-cursor"
        let store = try await makeHeadHoleStore(assetId: assetId)

        let result = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: headHoleInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        // The coarse walk's contiguous upper bound for this run is 100 (its one
        // plan was covered). Publishing it would assert [0,100] is read and
        // `narrowedForResume` would drop every segment of the next attempt —
        // the same permanent lockout, in a different costume.
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == nil,
                "a hole at the head means the contiguous prefix of the EPISODE has not moved")
    }

    @available(iOS 26.0, *)
    @Test("THE AD5F3A0A CASE — when the run DID start at the head, the under-coverage defer publishes the honest cursor so the resume scans only the remainder")
    func underCoverageDeferPublishesTheCursorWhenTheRunStartedAtTheHead() async throws {
        let assetId = "asset-41mu-prefix"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 100))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 100, pass: "fast")
        ])
        // Segments start at 2.8 s — leading silence, exactly AD5F3A0A's shape —
        // and cover 30 s of a 100 s episode.
        let inputs = makeInputs(
            assetId: assetId,
            lines: [
                (2.8, 10, "Window zero editorial content about the topic."),
                (10, 20, "Window one sponsor break maybe present here."),
                (20, 30, "Window two back to the show conversation.")
            ]
        )

        let result = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: inputs)
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        #expect(row.status == .deferred)
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30,
                "2.8 s of leading silence is inside the coverage reader's bridge tolerance, so this IS a genuine episode prefix")
    }

    @available(iOS 26.0, *)
    @Test("R2 — the head test measures the RESUME's own first plan, so a hole immediately above the prior cursor freezes it on attempt TWO as well as attempt one")
    func resumeDoesNotPublishACursorOverAHoleAboveThePriorCursor() async throws {
        let assetId = "asset-41mu-resume-hole"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 100))
        // The transcript is [0,10] ∪ [80,100]: a 70 s untranscribed hole, which
        // is AD5F3A0A's 900 → 3,270 s shape at test scale.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 10, pass: "fast"),
            makeChunk(assetId: assetId, index: 1, start: 80, end: 100, pass: "fast")
        ])

        // Attempt 1 is dispatched with the head tier alone and defers with an
        // honest cursor of 10 — a genuine episode prefix.
        let first = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: makeInputs(
                assetId: assetId,
                lines: [(0, 10, "Opening remarks before the first sponsor break.")]
            ))
        let jobId = try #require(first.admittedJobIds.first)
        let afterFirst = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(afterFirst.progressCursor?.lastProcessedUpperBoundSec == 10)
        // playhead-e6d3: attempt 1 advanced the covered prefix (nil → 10), so it
        // is not a failed attempt and costs nothing. Under the flat rule this
        // read 1.
        #expect(afterFirst.retryCount == 0)

        // Attempt 2 is dispatched with the WHOLE transcript. `narrowedForResume`
        // drops [0,10], so the run's first PLAN starts at 80 — 70 s above the
        // cursor. The pre-narrowing list still starts at 0, and reading THAT as
        // "where the run began" is what made the head test unfirable on every
        // resume: 0 - 10 is negative, so the cursor sailed to 100 and the next
        // attempt would drop the entire episode.
        let second = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: makeInputs(
                assetId: assetId,
                lines: [
                    (0, 10, "Opening remarks before the first sponsor break."),
                    (80, 90, "Closing thoughts on the subject at hand today."),
                    (90, 100, "Outro credits and the usual sign-off material.")
                ]
            ))
        #expect(second.admittedJobIds.first == jobId, "the same row, re-driven by M-5")
        let afterSecond = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(afterSecond.status == .deferred)
        // playhead-e6d3: attempt 2 did NOT advance the cursor (that is this
        // test's whole subject), so it is the FIRST barren attempt and costs
        // exactly one. Attempt 1 did advance — nil → 10 — and now costs nothing,
        // which is why this reads 1 rather than the flat rule's 2.
        #expect(afterSecond.retryCount == 1)
        #expect(afterSecond.progressCursor?.lastProcessedUpperBoundSec == 10,
                "the 70 s hole above the cursor is unscanned audio — the cursor must not speak for it")
    }

    // MARK: - 2b. playhead-e6d3: a converging job is not retired for converging

    @available(iOS 26.0, *)
    @Test("playhead-e6d3 — an episode whose coverage climbs on EVERY attempt outlives the budget and stays visible to the coarse phase")
    func aConvergingJobSurvivesPastTheFlatBudget() async throws {
        let assetId = "asset-e6d3-converging"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 100))
        // The whole episode is transcribed, so the ceiling is 1.0 and nothing
        // but the budget can stop this job.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 100, pass: "fast")
        ])

        // Each attempt is dispatched with ten more seconds of transcript than
        // the last — the shape of an episode whose scan is genuinely advancing
        // and is still nowhere near the 0.98 floor. Under the FLAT budget the
        // third of these retired the row with
        // `underCoverageBudgetSpent-fullEpisodeScan` at `retryCount = 3`, and
        // `retryCount < maxRetries` then removed the ASSET from both coarse
        // candidate queries and from the ad-scan re-drive, permanently.
        let lines: [(start: Double, end: Double, text: String)] = [
            (0, 10, "Window zero editorial content about the topic."),
            (10, 20, "Window one sponsor break maybe present here."),
            (20, 30, "Window two back to the show conversation."),
            (30, 40, "Window three more discussion of the same subject.")
        ]

        var jobId: String?
        for attempt in 1...lines.count {
            let run = try await makeRunner(store: store, runtime: makeRuntime().runtime)
                .runPendingBackfill(for: makeInputs(assetId: assetId, lines: Array(lines[0..<attempt])))
            let id = try #require(run.admittedJobIds.first)
            if let jobId { #expect(id == jobId, "the same row, re-driven by M-5") }
            jobId = id

            let row = try #require(await store.fetchBackfillJob(byId: id))
            #expect(row.status == .deferred,
                    "attempt \(attempt) advanced the covered prefix — it is not a failed attempt")
            #expect(row.status != .failed)
            #expect(row.retryCount == 0,
                    "attempt \(attempt) banked new audio, so the run of barren attempts is zero")
            #expect(row.progressCursor?.lastProcessedUpperBoundSec == EpisodeSeconds(Double(attempt) * 10))
        }

        // The consequence the bead is named for: after MORE attempts than the
        // budget has retries, the asset is still reachable by the query the
        // coarse phase and the ad-scan re-drive both start from.
        let id = try #require(jobId)
        #expect(lines.count > AdmissionController.maxRetries,
                "the fixture must outlast the budget or it proves nothing")
        #expect(try await store.countResumableBackfillJobs(assetId: assetId) > 0)
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10).contains(assetId),
                "an exhausted row is invisible to the coarse phase FOREVER — that is the defect")
        let row = try #require(await store.fetchBackfillJob(byId: id))
        #expect(row.deferReason == "underCoverage-fullEpisodeScan",
                "still the non-terminal cause, never the budget-spent one")
    }

    // MARK: - 2c. playhead-wogi: the cursor may not stride over an interior hole

    @available(iOS 26.0, *)
    // NOTE: no ';' in this display name — see the note on
    // `theUnderCoveredJobStaysResumableSoTheRedriveCanMint`.
    @Test("THE 3C2FFE10 CASE — a run handed a transcript with a hole in the middle publishes the end of the CONTIGUOUS part, so the audio that lands later is still plannable")
    func interiorHoleDoesNotBuryTheAudioThatArrivesLater() async throws {
        let assetId = "asset-wogi-interior-hole"
        let store = try await makeTestStore()
        // 3C2FFE10's proportions at test scale: an 800 s episode whose scan runs
        // ahead of its transcription, exactly as the device's did.
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 800))

        // ATTEMPT 1 — only the head is transcribed. The pass sweeps it and
        // publishes a genuine episode prefix.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 208, pass: "fast")
        ])
        let first = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: makeInputs(
                assetId: assetId,
                lines: [(0, 200, "Opening remarks before the first sponsor break.")],
                transcriptVersion: "tx-wogi-1"
            ))
        let jobId = try #require(first.admittedJobIds.first)
        #expect(try #require(await store.fetchBackfillJob(byId: jobId))
            .progressCursor?.lastProcessedUpperBoundSec == 200)

        // ATTEMPT 2 — the FINAL pass lands the outro. The job is now dispatched
        // with `[0,66] ∪ [794,800]`: a 728 s hole nobody has transcribed yet.
        // `planPassA` partitions the list it is handed, so the two sides of the
        // hole are ADJACENT by `segmentIndex` and every plan is covered —
        // `fullyCovered` reads true and no plan-side guard has anything to say.
        // The shipped walk therefore published 800 on an 800 s episode.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 1, start: 794, end: 800, pass: "final")
        ])
        let second = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: makeInputs(
                assetId: assetId,
                lines: [
                    (0, 200, "Opening remarks before the first sponsor break."),
                    (200, 208, "A little more of the opening conversation here."),
                    (794, 800, "Outro credits and the usual sign-off material.")
                ],
                transcriptVersion: "tx-wogi-2"
            ))
        #expect(second.admittedJobIds.first == jobId, "the same row, re-driven by M-5")
        let afterSecond = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(
            afterSecond.progressCursor?.lastProcessedUpperBoundSec == 208,
            "the cursor reads \(String(describing: afterSecond.progressCursor?.lastProcessedUpperBoundSec)) — everything above 208 is audio no plan ever held"
        )

        // ATTEMPT 3 — transcription finishes and the middle finally exists.
        // THIS is where the defect was fatal: `narrowedForResume` is
        // `segments.filter { $0.endTime > cursor }`, so a cursor of 800 deletes
        // every one of these segments, `runJob`'s empty-segments guard writes a
        // `noWork:emptySegments` sentinel, and the attempt runs no inference at
        // all. Seventeen such rows are on the 2026-08-14 pull.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 2, start: 208, end: 794, pass: "fast")
        ])
        let thirdRuntime = makeRuntime()
        let third = try await makeRunner(store: store, runtime: thirdRuntime.runtime)
            .runPendingBackfill(for: makeInputs(
                assetId: assetId,
                lines: [
                    (0, 200, "Opening remarks before the first sponsor break."),
                    (200, 208, "A little more of the opening conversation here."),
                    (208, 400, "The long middle stretch of the episode discussion."),
                    (400, 794, "The second half of that same long conversation."),
                    (794, 800, "Outro credits and the usual sign-off material.")
                ],
                transcriptVersion: "tx-wogi-3"
            ))
        #expect(third.admittedJobIds.first == jobId)

        #expect(
            await thirdRuntime.coarseCallCount > 0,
            "the attempt ran NO inference — the cursor had already deleted the whole transcript"
        )
        let rows = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(
            rows.allSatisfy { $0.errorContext != "noWork:emptySegments" },
            "an attempt that could not have scanned anything still held an admission ticket and a background slot"
        )

        // And the quantity the whole lane is judged on: the episode is now
        // genuinely read, which is unreachable while 728 s of it sits below a
        // cursor nobody earned.
        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([assetId])[assetId]
        )
        let fraction = try #require(summary.adScanFraction)
        #expect(fraction >= AnalysisJobRunner.semanticBackfillSufficientAdScanFraction,
                "measured \(fraction) — the middle of the episode was never scanned")
    }

    @available(iOS 26.0, *)
    @Test("playhead-wogi — a hole punched by playhead-15d0's row-side narrowing is audio we HAVE read, and must not stop the cursor")
    func screenedWindowHolesDoNotStopTheCursor() async throws {
        let assetId = "asset-wogi-screened-hole"
        let store = try await makeTestStore()
        // The transcript is complete at [0,800] but the episode is declared at
        // 1,000 s, so the ad-scan floor is unreachable and every attempt takes
        // the under-coverage path — which is the one that publishes the walk's
        // bound. Without that the completion cursor would answer instead and the
        // case would pass for the wrong reason.
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 1_000))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 800, pass: "fast")
        ])

        // Attempt 1 is dispatched with a MIDDLE band — the shape a dispatcher
        // that hands over part of a transcript produces, and 53FC53E3's shape
        // one band along. Its plans begin at 200, far above the (absent) cursor,
        // so playhead-41mu's head rule correctly publishes nothing.
        let first = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: makeInputs(
                assetId: assetId,
                lines: [
                    (200, 300, "The first band this dispatcher handed over here."),
                    (300, 400, "The second band of that same handed-over range.")
                ]
            ))
        let jobId = try #require(first.admittedJobIds.first)
        #expect(try #require(await store.fetchBackfillJob(byId: jobId))
            .progressCursor?.lastProcessedUpperBoundSec == nil)

        // Attempt 2 gets the WHOLE transcript. `narrowedForResume` trims
        // nothing (there is no cursor), and then playhead-15d0's row-side
        // narrowing deletes [200,400] because attempt 1's rows already screened
        // it — leaving a 200 s hole in the list the planner is handed.
        //
        // That hole means "we have READ this", the exact opposite of the hole
        // this bead is about. Measuring the interior-hole rule over the planner's
        // list rather than the resume's would cap the cursor at 200, and the
        // NEXT attempt's first plan would then start at 400 — 200 s above the
        // cursor, where the head rule refuses to promote anything at all. The
        // cursor would never move again on an episode that is being scanned
        // correctly.
        let second = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: makeInputs(
                assetId: assetId,
                lines: (0..<8).map { index in
                    (Double(index) * 100, Double(index + 1) * 100,
                     "Window \(index) of the episode's own running conversation.")
                }
            ))
        #expect(second.admittedJobIds.first == jobId, "the same row, re-driven by M-5")
        let afterSecond = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(afterSecond.status == .deferred, "0.8 of a 1,000 s episode is under the floor")
        #expect(
            afterSecond.progressCursor?.lastProcessedUpperBoundSec == 800,
            "the cursor reads \(String(describing: afterSecond.progressCursor?.lastProcessedUpperBoundSec)) — a hole the row-side narrowing punched was read as unscanned audio"
        )
    }

    // MARK: - 3. The payoff: the rescue is no longer blocked

    @available(iOS 26.0, *)
    // NOTE: no ';' in this display name. `mutation-battery.sh` splits its
    // expectation field on ';', so a test whose name contains one can never be
    // matched and every mutation naming it prints SURVIVED against a working
    // rail. The battery's baseline caught exactly that here.
    @Test("THE CONSEQUENCE — a non-completing terminal leaves resumable work, so the ad-scan re-drive can mint where the old `complete` left ZERO and blocked it")
    func theUnderCoveredJobStaysResumableSoTheRedriveCanMint() async throws {
        let assetId = "asset-41mu-redrive"
        let store = try await makeHeadHoleStore(assetId: assetId)

        _ = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: headHoleInputs(assetId: assetId))

        let resumable = try await store.countResumableBackfillJobs(assetId: assetId)
        #expect(resumable > 0, "a `complete` row is not resumable — on the pull 53FC53E3's count is 0 and it is the one asset with no path to another scan")

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([assetId])[assetId]
        )
        #expect(AnalysisWorkScheduler.shouldMintAdScanRedrive(
            adScanFraction: summary.adScanFraction,
            resumableCoverageJobCount: resumable
        ), "the whole point: the rescue that would fix this episode can now be minted")
    }

    // MARK: - 4. The vacuity control

    @available(iOS 26.0, *)
    @Test("VACUITY CONTROL — a pass that genuinely reads the episode still COMPLETES, with its full-coverage cursor")
    func fullyScannedEpisodeStillCompletes() async throws {
        let assetId = "asset-41mu-complete"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 30))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 30, pass: "fast")
        ])
        let inputs = makeInputs(
            assetId: assetId,
            lines: [
                (0, 10, "Window zero editorial content about the topic."),
                (10, 20, "Window one sponsor break maybe present here."),
                (20, 30, "Window two back to the show conversation.")
            ]
        )

        let result = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: inputs)
        let jobId = try #require(result.admittedJobIds.first)

        #expect(result.deferredJobIds.isEmpty, "a genuinely complete scan must never defer")
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete)
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30)

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([assetId])[assetId]
        )
        #expect(summary.adScanFraction == 1.0)
    }

    // MARK: - 5. The bound

    @available(iOS 26.0, *)
    @Test("the deferral is BOUNDED — an episode that keeps coming back under-covered terminates with a named cause instead of re-driving forever")
    func repeatedUnderCoverageTerminatesAtTheAttemptBudget() async throws {
        let assetId = "asset-41mu-budget"
        let store = try await makeHeadHoleStore(assetId: assetId)
        let inputs = headHoleInputs(assetId: assetId)

        // Attempt 1 and 2: deferred, retryCount advancing.
        for expectedRetry in 1...(AdmissionController.maxRetries - 1) {
            let run = try await makeRunner(store: store, runtime: makeRuntime().runtime)
                .runPendingBackfill(for: inputs)
            let jobId = try #require(run.admittedJobIds.first)
            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.status == .deferred)
            #expect(row.retryCount == expectedRetry)
        }

        // Attempt 3 spends the budget: a TERMINAL row that names why.
        let final = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: inputs)
        let jobId = try #require(final.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed, "it must stop, and it must not stop by claiming success")
        #expect(row.status != .complete)
        #expect(row.deferReason == "underCoverageBudgetSpent-fullEpisodeScan")
        #expect(row.retryCount == AdmissionController.maxRetries)
    }

    // MARK: - 6. The pure decision

    /// The expected verdict, spelled out at every call site rather than compared
    /// against a bare case, because playhead-e6d3's whole subject is the NUMBER
    /// this value carries.
    private static func underCovered(
        retires: Bool,
        retryCount: Int,
        constraint: BackfillJobRunner.UnderCoverageConstraint = .scanBudget
    ) -> BackfillJobRunner.CoverageTerminalDecision {
        .underCovered(BackfillJobRunner.UnderCoverageVerdict(
            retires: retires,
            retryCount: retryCount,
            constraint: constraint
        ))
    }

    @Test("the floor is the pipeline's floor, and the comparison is strict at the boundary")
    func decisionUsesTheSharedFloor() {
        let floor = AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .measured(floor), ceiling: nil, retryCount: 0, cursorAdvanced: false
        ) == .complete)
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan,
            measurement: .measured(ReachRatio(floor.rawValue - 0.001)),
            ceiling: nil,
            retryCount: 0,
            cursorAdvanced: false
        ) == Self.underCovered(retires: false, retryCount: 1))
        // The two field fractions, verbatim.
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .measured(0.0142), ceiling: nil, retryCount: 0, cursorAdvanced: false
        ) == Self.underCovered(retires: false, retryCount: 1))
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .measured(0.2068), ceiling: nil, retryCount: 0, cursorAdvanced: false
        ) == Self.underCovered(retires: false, retryCount: 1))
    }

    @Test("ONLY the phase that claims the whole episode is judged against an episode-wide floor")
    func narrowPhasesAreNotGated() {
        for phase in BackfillJobPhase.allCases where phase != .fullEpisodeScan {
            #expect(BackfillJobRunner.coverageTerminalDecision(
                phase: phase, measurement: .measured(0), ceiling: nil, retryCount: 0, cursorAdvanced: false
            ) == .complete, "\(phase.rawValue) never claimed to read the episode")
        }
    }

    @Test("not-measurable and un-readable are different facts with opposite answers")
    func unmeasurableCompletesAndUnreadableDoesNot() {
        // A missing denominator is the duration-backfill sweep's bug, not this
        // one, and refusing here would strand every legacy row.
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .notMeasurable, ceiling: nil, retryCount: 0, cursorAdvanced: false
        ) == .complete)
        // A read that THREW is not evidence the episode was read.
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .unreadable, ceiling: nil, retryCount: 0, cursorAdvanced: false
        ) == Self.underCovered(retires: false, retryCount: 1))
    }

    @Test("R1 — a non-finite MEASUREMENT is an absence, and under-claims like every other reader of it")
    func nonFiniteMeasurementDoesNotComplete() {
        for garbage in [Double.nan, .infinity, -.infinity] {
            #expect(BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan,
                measurement: .measured(ReachRatio(garbage)),
                ceiling: nil,
                retryCount: 0,
                cursorAdvanced: false
            ) == Self.underCovered(retires: false, retryCount: 1),
            "\(garbage) is not evidence the episode was read")
        }
        // The three call sites this now agrees with, asserted rather than
        // asserted-about: all of them read a non-finite fraction as NOT read.
        #expect(SemanticScanClaim.isOwed(adScanFraction: ReachRatio(.nan)))
        #expect(AnalysisWorkScheduler.shouldMintAdScanRedrive(
            adScanFraction: ReachRatio(.nan), resumableCoverageJobCount: 1
        ))
    }

    @Test("the attempt budget is the shared one, and it terminates rather than deferring forever")
    func decisionTerminatesAtTheBudget() {
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan,
            measurement: .measured(0),
            ceiling: nil,
            retryCount: AdmissionController.maxRetries - 2,
            cursorAdvanced: false
        ) == Self.underCovered(retires: false, retryCount: AdmissionController.maxRetries - 1))
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan,
            measurement: .measured(0),
            ceiling: nil,
            retryCount: AdmissionController.maxRetries - 1,
            cursorAdvanced: false
        ) == Self.underCovered(retires: true, retryCount: AdmissionController.maxRetries))
    }

    // MARK: - 6b. playhead-e6d3: the budget counts CONSECUTIVE barren attempts

    @Test("playhead-e6d3 — an attempt that advanced the covered prefix spends NO retry, at every point in the budget")
    func anAdvancingAttemptCostsNothing() {
        // The row that matters: one retry short of retirement, and converging.
        // Under the flat rule this was `.failUnderCoverage` at `maxRetries` —
        // the disposition that emptied the coarse phase's candidate set on the
        // 2026-08-14 pull.
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan,
            measurement: .measured(0.2068),
            ceiling: nil,
            retryCount: AdmissionController.maxRetries - 1,
            cursorAdvanced: true
        ) == Self.underCovered(retires: false, retryCount: 0),
        "an attempt that banked new audio is not a FAILED attempt, and `maxRetries` counts failed attempts")

        // A RESET, not a "do not increment": the run of barren attempts ends.
        for prior in 0...(AdmissionController.maxRetries + 2) {
            #expect(BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan,
                measurement: .measured(0),
                ceiling: nil,
                retryCount: prior,
                cursorAdvanced: true
            ) == Self.underCovered(retires: false, retryCount: 0),
            "prior=\(prior) must reset to 0, not persist \(prior)")
        }
    }

    @Test("playhead-e6d3 — the bound survives: three CONSECUTIVE barren attempts still retire the job")
    func consecutiveBarrenAttemptsStillRetire() {
        // Advance, then three barren ones. The reset does not buy an extra
        // attempt after the run of barren ones has started.
        var retryCount = BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan,
            measurement: .measured(0.5),
            ceiling: nil,
            retryCount: AdmissionController.maxRetries - 1,
            cursorAdvanced: true
        )
        #expect(retryCount == Self.underCovered(retires: false, retryCount: 0))

        var carried = 0
        for expected in 1...AdmissionController.maxRetries {
            retryCount = BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan,
                measurement: .measured(0.5),
                ceiling: nil,
                retryCount: carried,
                cursorAdvanced: false
            )
            #expect(retryCount == Self.underCovered(
                retires: expected >= AdmissionController.maxRetries,
                retryCount: expected
            ))
            carried = expected
        }
    }

    @Test("playhead-e6d3 — advancing does not resurrect a COVERED episode: the floor still decides `complete`")
    func advancingDoesNotOverrideTheFloor() {
        // The reset lives BELOW the floor test, so a covered episode completes
        // whether or not this attempt advanced anything.
        for advanced in [true, false] {
            #expect(BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan,
                measurement: .measured(AnalysisJobRunner.semanticBackfillSufficientAdScanFraction),
                ceiling: nil,
                retryCount: 0,
                cursorAdvanced: advanced
            ) == .complete)
        }
    }

    // MARK: - 7. The pure cursor rule

    @Test("a cursor is an assertion about the EPISODE, so a hole at the head freezes it")
    func cursorDoesNotAdvanceOverAHoleAtTheHead() {
        // 53FC53E3, verbatim: nothing banked, plans start at 2,490, walk reaches
        // 2,525.82 on a 2,528 s episode.
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, coverage: outcome(scanned: 2525.82, firstPlanned: 2490)
        ) == nil)
        // A prior cursor is preserved, never regressed and never inflated.
        let prior = BackfillProgressCursor(processedPhaseCount: 0, lastProcessedUpperBoundSec: 100)
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: prior, coverage: outcome(scanned: 2525.82, firstPlanned: 2490)
        ) == prior)
    }

    @Test("a run that starts at the head publishes the honest bound, and leading silence is not a hole")
    func cursorAdvancesWhenTheRunIsAGenuinePrefix() {
        // AD5F3A0A, verbatim: first segment at 2.8 s, walk reaches 900.
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, coverage: outcome(scanned: 900, firstPlanned: 2.8)
        )?.lastProcessedUpperBoundSec == 900)
        // Resuming from a prior cursor is a prefix too: the head is already read.
        let prior = BackfillProgressCursor(processedPhaseCount: 0, lastProcessedUpperBoundSec: 900)
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: prior, coverage: outcome(scanned: 1800, firstPlanned: 900)
        )?.lastProcessedUpperBoundSec == 1800)
        // …and it is MONOTONIC: a stale walk can never drag the row backwards.
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: prior, coverage: outcome(scanned: 300, firstPlanned: 0)
        )?.lastProcessedUpperBoundSec == 900)
    }

    @Test("the hole threshold is the coverage reader's own bridge tolerance, not a fresh constant")
    func cursorHoleUsesTheSharedBridgeTolerance() {
        let gap = AnalysisCoverageMath.adScanBridgeableGapSec
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, coverage: outcome(scanned: 500, firstPlanned: PlanListSeconds(gap.rawValue))
        )?.lastProcessedUpperBoundSec == 500, "a gap AT the tolerance is bridged, exactly as the numerator's own reader bridges it")
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, coverage: outcome(scanned: 500, firstPlanned: PlanListSeconds(gap.rawValue + 0.001))
        ) == nil, "past it, the audio is genuinely unscanned and the cursor must not speak for it")
    }

    @Test("nothing scanned this run leaves the prior cursor exactly as it was")
    func cursorIsUnchangedWhenNothingWasScanned() {
        let prior = BackfillProgressCursor(processedPhaseCount: 0, lastProcessedUpperBoundSec: 42)
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: prior, coverage: outcome(scanned: nil, firstPlanned: 0)
        ) == prior)
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, coverage: outcome(scanned: nil, firstPlanned: 0)
        ) == nil)
    }

    /// playhead-x0lb: both halves of the cursor rule now come from the pass's
    /// own ``BackfillJobRunner/CoverageOutcome``, so a test cannot supply them
    /// from two different lists any more than production can.
    private func outcome(
        scanned: PlanListSeconds?,
        firstPlanned: PlanListSeconds?
    ) -> BackfillJobRunner.CoverageOutcome {
        BackfillJobRunner.CoverageOutcome(
            coarseIncompleteDeferReason: nil,
            lastCoveredUpperBoundSec: scanned,
            firstPlannedSegmentStartSec: firstPlanned
        )
    }

    // MARK: - 8. playhead-nffz: the floor's denominator is the EPISODE, and the
    //           numerator's ceiling is the TRANSCRIPT

    /// C065AD03's shape at test scale, and the ONLY difference from
    /// ``makeHeadHoleStore(assetId:)`` is where the transcript is.
    ///
    /// Head-hole store: a 100 s episode transcribed end to end, dispatched with
    /// the last 10 s. Ceiling 1.00, measured 0.10 — the floor is REACHABLE and a
    /// better dispatch would reach it, so its budget really is what retires it.
    ///
    /// This store: a 100 s episode whose transcript exists only over `[56, 100]`,
    /// dispatched with exactly that. Ceiling 0.44, measured 0.44 — every second
    /// of transcript has been read and the floor is 0.98. No scan can retire it.
    ///
    /// Two fixtures, one variable, two causes. That is what makes the cause a
    /// measurement rather than a label.
    private func makeCeilingBoundStore(assetId: String) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 100))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 56, end: 100, pass: "fast")
        ])
        return store
    }

    private func ceilingBoundInputs(assetId: String) -> BackfillJobRunner.AssetInputs {
        makeInputs(
            assetId: assetId,
            lines: [(56, 100, "The only stretch of this episode anything ever transcribed.")]
        )
    }

    @available(iOS 26.0, *)
    @Test("THE C065AD03 CASE — a job whose TRANSCRIPT cannot support the floor retires naming the transcript, not the budget")
    func ceilingBoundJobRetiresNamingTheTranscript() async throws {
        let assetId = "asset-nffz-ceiling"
        let store = try await makeCeilingBoundStore(assetId: assetId)
        let inputs = ceilingBoundInputs(assetId: assetId)

        for expectedRetry in 1...(AdmissionController.maxRetries - 1) {
            let run = try await makeRunner(store: store, runtime: makeRuntime().runtime)
                .runPendingBackfill(for: inputs)
            let jobId = try #require(run.admittedJobIds.first)
            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.status == .deferred)
            #expect(row.retryCount == expectedRetry,
                    "the ceiling must not change the budget — only what the terminal CALLS it")
        }

        let final = try await makeRunner(store: store, runtime: makeRuntime().runtime)
            .runPendingBackfill(for: inputs)
        let jobId = try #require(final.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed)
        #expect(row.retryCount == AdmissionController.maxRetries,
                "same budget, same count — this bead changes the cause, not the policy")
        #expect(row.deferReason == "transcriptCeilingBelowFloor-fullEpisodeScan",
                """
                THE DOES-IT-RUN DIRECTION. This is the whole diff, end to end: the store \
                measured a ceiling, the actor read it off the SAME summary as the fraction, \
                the pure decision compared it to the SAME floor, and the write CONSUMED the \
                verdict's constraint. A mutant that drops any one of those four links leaves \
                this row saying `underCoverageBudgetSpent-fullEpisodeScan`, which is the \
                sentence playhead-se0x was filed on.
                """)

        // And the two quantities, so a reader can see the mismatch that is the bug.
        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([assetId])[assetId]
        )
        let fraction = try #require(summary.adScanFraction)
        let ceiling = try #require(summary.adScanCeilingFraction)
        #expect(abs(fraction.rawValue - 0.44) < 0.001)
        #expect(abs(ceiling.rawValue - 0.44) < 0.001,
                "every transcribed second was read — the scan is AT its ceiling")
        #expect(ceiling < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction,
                "and the ceiling is under the floor, so no scan could ever have retired it")
    }

    @available(iOS 26.0, *)
    @Test("playhead-nffz — the MIRROR: a reachable ceiling still names the budget, so the cause discriminates")
    func reachableCeilingStillNamesTheBudget() async throws {
        // Identical driving loop to the test above, on the head-hole store —
        // transcribed end to end, so the ceiling is 1.0 and the budget IS what
        // retired it. Without this the new cause could be written on every row.
        let assetId = "asset-nffz-reachable"
        let store = try await makeHeadHoleStore(assetId: assetId)
        let inputs = headHoleInputs(assetId: assetId)

        var lastJobId: String?
        for _ in 1...AdmissionController.maxRetries {
            let run = try await makeRunner(store: store, runtime: makeRuntime().runtime)
                .runPendingBackfill(for: inputs)
            lastJobId = run.admittedJobIds.first ?? lastJobId
        }
        let jobId = try #require(lastJobId)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed)
        #expect(row.deferReason == "underCoverageBudgetSpent-fullEpisodeScan")

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([assetId])[assetId]
        )
        #expect(summary.adScanCeilingFraction == 1.0,
                "the transcript covers the whole episode; nothing about it binds")
        #expect(try #require(summary.adScanFraction).rawValue < 0.98)
    }

    @Test("playhead-nffz — the ceiling is judged by the SAME floor, strictly")
    func ceilingIsJudgedByTheSharedFloorStrictly() {
        let floor = AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
        func constraint(_ ceiling: ReachRatio?) -> BackfillJobRunner.UnderCoverageConstraint? {
            guard case let .underCovered(verdict) = BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan,
                measurement: .measured(0.1),
                ceiling: ceiling,
                retryCount: 0,
                cursorAdvanced: false
            ) else { return nil }
            return verdict.constraint
        }
        // AT the floor the transcript is sufficient — strict `<`, the same
        // comparison `shouldSkipSemanticBackfill` and the library ✓ make.
        #expect(constraint(floor) == .scanBudget)
        #expect(constraint(ReachRatio(floor.rawValue - 0.001)) == .transcriptCeiling)
        #expect(constraint(1.0) == .scanBudget)
        // The five field ceilings, verbatim, measured with the shipped reader on
        // db-pull10 (2026-08-14) and db-prewipe6 (2026-08-11).
        for measured in [0.4436, 0.8961, 0.9341, 0.0697, 0.9274] {
            #expect(constraint(ReachRatio(measured)) == .transcriptCeiling, "\(measured)")
        }
    }

    @Test("playhead-nffz — an ABSENT or non-finite ceiling blames nothing")
    func absentCeilingBlamesNothing() {
        func constraint(_ ceiling: ReachRatio?) -> BackfillJobRunner.UnderCoverageConstraint? {
            guard case let .underCovered(verdict) = BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan,
                measurement: .unreadable,
                ceiling: ceiling,
                retryCount: 0,
                cursorAdvanced: false
            ) else { return nil }
            return verdict.constraint
        }
        #expect(constraint(nil) == .scanBudget, "no evidence is not evidence of a short transcript")
        for garbage in [Double.nan, .infinity, -.infinity] {
            #expect(constraint(ReachRatio(garbage)) == .scanBudget,
                    "\(garbage) is an absence wearing a quantity's clothes")
        }
    }

    @Test("playhead-nffz — THE CEILING CHANGES THE CAUSE AND NOTHING ELSE: same retire flag, same count, everywhere")
    func theCeilingChangesOnlyTheCause() {
        // The failure mode of this fix is losing coverage: retiring a
        // ceiling-bound job EARLY deletes the transcribed-but-unscanned audio it
        // could still read (AA6CD430 has 58.62 s of it, and an ad can live
        // there). So the retire flag and the persisted count must be BYTE-FOR-BYTE
        // what they were before the ceiling existed, at every point of the budget
        // and on both sides of the cursor rule.
        let ceilings: [ReachRatio?] = [nil, 0.0, 0.4436, 0.9341, 0.98, 1.0]
        let measurements: [BackfillJobRunner.AdScanMeasurement] = [
            .measured(0), .measured(0.5), .unreadable
        ]
        for prior in 0...(AdmissionController.maxRetries + 1) {
            for advanced in [true, false] {
                for measurement in measurements {
                    var seen: Set<String> = []
                    for ceiling in ceilings {
                        let decision = BackfillJobRunner.coverageTerminalDecision(
                            phase: .fullEpisodeScan,
                            measurement: measurement,
                            ceiling: ceiling,
                            retryCount: prior,
                            cursorAdvanced: advanced
                        )
                        guard case let .underCovered(verdict) = decision else {
                            Issue.record("expected under-covered for \(measurement)")
                            continue
                        }
                        seen.insert("\(verdict.retires)/\(verdict.retryCount)")
                    }
                    #expect(seen.count == 1,
                            "prior=\(prior) advanced=\(advanced): the ceiling moved the BUDGET, and it must only move the CAUSE — saw \(seen.sorted())")
                }
            }
        }
    }

    @Test("playhead-nffz — the ceiling never turns a COMPLETING case into a non-completing one")
    func theCeilingDoesNotWidenTheNonCompletingSet() {
        // `.notMeasurable` completes by a policy this bead did not touch
        // (playhead-w4rd owns whether it should), and a ceiling below the floor
        // must not quietly start refusing it: that would be a COVERAGE change
        // wearing a naming change's clothes, and it would strand every legacy
        // duration-less row. Same for a fraction at or above the floor.
        let floor = AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
        for ceiling in [nil, ReachRatio(0), ReachRatio(0.4436), ReachRatio(1)] as [ReachRatio?] {
            #expect(BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan,
                measurement: .notMeasurable,
                ceiling: ceiling,
                retryCount: 0,
                cursorAdvanced: false
            ) == .complete, "ceiling \(String(describing: ceiling)) must not refuse an unmeasurable row")
            #expect(BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan,
                measurement: .measured(floor),
                ceiling: ceiling,
                retryCount: 0,
                cursorAdvanced: false
            ) == .complete, "a scan AT the floor completes whatever its ceiling says")
        }
    }

    @Test("playhead-nffz — the DEFERRAL cause is deliberately constraint-independent")
    func theDeferralCauseIsUnchanged() {
        // A deferred row is still resumable, so what it needs to say is "under
        // coverage" — the constraint is a statement about why the job STOPPED,
        // and a deferred job has not stopped. Pinned so that widening it later is
        // a decision somebody makes rather than a side effect.
        for phase in BackfillJobPhase.allCases {
            #expect(BackfillJobRunner.underCoverageDeferReason(phase: phase)
                    == "underCoverage-\(phase.rawValue)")
        }
    }

    @Test("playhead-nffz — the two causes are distinct, greppable and phase-qualified")
    func theTwoCausesAreDistinct() {
        for phase in BackfillJobPhase.allCases {
            let budget = BackfillJobRunner.underCoverageExpiryReason(
                phase: phase, constraint: .scanBudget
            )
            let ceiling = BackfillJobRunner.underCoverageExpiryReason(
                phase: phase, constraint: .transcriptCeiling
            )
            #expect(budget != ceiling)
            #expect(budget == "underCoverageBudgetSpent-\(phase.rawValue)",
                    "the budget string is PERSISTED and the V50 repair migration matches it — it must not drift")
            #expect(ceiling == "transcriptCeilingBelowFloor-\(phase.rawValue)")
            // Neither answers to the other's prefix grep, which is the whole
            // point of splitting them: `underCoverageBudgetSpent-%` is what the
            // budget-repair migration hands a fresh budget to.
            #expect(!ceiling.hasPrefix("underCoverageBudgetSpent-"))
            #expect(!budget.hasPrefix("transcriptCeilingBelowFloor-"))
            #expect(!ceiling.hasPrefix(BackfillJobRunner.underCoverageDeferReason(phase: phase)))
        }
    }
}
