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
//             pass (`retryShadowFMPhaseForSession`, playhead-3ort).
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
        #expect(abs(fraction - 0.10) < 0.001, "10 s of a 100 s episode")
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

    @Test("the floor is the pipeline's floor, and the comparison is strict at the boundary")
    func decisionUsesTheSharedFloor() {
        let floor = AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .measured(floor), retryCount: 0
        ) == .complete)
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .measured(floor - 0.001), retryCount: 0
        ) == .deferUnderCoverage)
        // The two field fractions, verbatim.
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .measured(0.0142), retryCount: 0
        ) == .deferUnderCoverage)
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .measured(0.2068), retryCount: 0
        ) == .deferUnderCoverage)
    }

    @Test("ONLY the phase that claims the whole episode is judged against an episode-wide floor")
    func narrowPhasesAreNotGated() {
        for phase in BackfillJobPhase.allCases where phase != .fullEpisodeScan {
            #expect(BackfillJobRunner.coverageTerminalDecision(
                phase: phase, measurement: .measured(0), retryCount: 0
            ) == .complete, "\(phase.rawValue) never claimed to read the episode")
        }
    }

    @Test("not-measurable and un-readable are different facts with opposite answers")
    func unmeasurableCompletesAndUnreadableDoesNot() {
        // A missing denominator is the duration-backfill sweep's bug, not this
        // one, and refusing here would strand every legacy row.
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .notMeasurable, retryCount: 0
        ) == .complete)
        // A read that THREW is not evidence the episode was read.
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan, measurement: .unreadable, retryCount: 0
        ) == .deferUnderCoverage)
    }

    @Test("R1 — a non-finite MEASUREMENT is an absence, and under-claims like every other reader of it")
    func nonFiniteMeasurementDoesNotComplete() {
        for garbage in [Double.nan, .infinity, -.infinity] {
            #expect(BackfillJobRunner.coverageTerminalDecision(
                phase: .fullEpisodeScan, measurement: .measured(garbage), retryCount: 0
            ) == .deferUnderCoverage,
            "\(garbage) is not evidence the episode was read")
        }
        // The three call sites this now agrees with, asserted rather than
        // asserted-about: all of them read a non-finite fraction as NOT read.
        #expect(SemanticScanClaim.isOwed(adScanFraction: .nan))
        #expect(AnalysisWorkScheduler.shouldMintAdScanRedrive(
            adScanFraction: .nan, resumableCoverageJobCount: 1
        ))
    }

    @Test("the attempt budget is the shared one, and it terminates rather than deferring forever")
    func decisionTerminatesAtTheBudget() {
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan,
            measurement: .measured(0),
            retryCount: AdmissionController.maxRetries - 2
        ) == .deferUnderCoverage)
        #expect(BackfillJobRunner.coverageTerminalDecision(
            phase: .fullEpisodeScan,
            measurement: .measured(0),
            retryCount: AdmissionController.maxRetries - 1
        ) == .failUnderCoverage)
    }

    // MARK: - 7. The pure cursor rule

    @Test("a cursor is an assertion about the EPISODE, so a hole at the head freezes it")
    func cursorDoesNotAdvanceOverAHoleAtTheHead() {
        // 53FC53E3, verbatim: nothing banked, plans start at 2,490, walk reaches
        // 2,525.82 on a 2,528 s episode.
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, scannedUpperBoundSec: 2525.82, firstSegmentStartSec: 2490
        ) == nil)
        // A prior cursor is preserved, never regressed and never inflated.
        let prior = BackfillProgressCursor(processedPhaseCount: 0, lastProcessedUpperBoundSec: 100)
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: prior, scannedUpperBoundSec: 2525.82, firstSegmentStartSec: 2490
        ) == prior)
    }

    @Test("a run that starts at the head publishes the honest bound, and leading silence is not a hole")
    func cursorAdvancesWhenTheRunIsAGenuinePrefix() {
        // AD5F3A0A, verbatim: first segment at 2.8 s, walk reaches 900.
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, scannedUpperBoundSec: 900, firstSegmentStartSec: 2.8
        )?.lastProcessedUpperBoundSec == 900)
        // Resuming from a prior cursor is a prefix too: the head is already read.
        let prior = BackfillProgressCursor(processedPhaseCount: 0, lastProcessedUpperBoundSec: 900)
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: prior, scannedUpperBoundSec: 1800, firstSegmentStartSec: 900
        )?.lastProcessedUpperBoundSec == 1800)
        // …and it is MONOTONIC: a stale walk can never drag the row backwards.
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: prior, scannedUpperBoundSec: 300, firstSegmentStartSec: 0
        )?.lastProcessedUpperBoundSec == 900)
    }

    @Test("the hole threshold is the coverage reader's own bridge tolerance, not a fresh constant")
    func cursorHoleUsesTheSharedBridgeTolerance() {
        let gap = AnalysisCoverageMath.adScanBridgeableGapSec
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, scannedUpperBoundSec: 500, firstSegmentStartSec: gap
        )?.lastProcessedUpperBoundSec == 500, "a gap AT the tolerance is bridged, exactly as the numerator's own reader bridges it")
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, scannedUpperBoundSec: 500, firstSegmentStartSec: gap + 0.001
        ) == nil, "past it, the audio is genuinely unscanned and the cursor must not speak for it")
    }

    @Test("nothing scanned this run leaves the prior cursor exactly as it was")
    func cursorIsUnchangedWhenNothingWasScanned() {
        let prior = BackfillProgressCursor(processedPhaseCount: 0, lastProcessedUpperBoundSec: 42)
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: prior, scannedUpperBoundSec: nil, firstSegmentStartSec: 0
        ) == prior)
        #expect(BackfillJobRunner.underCoverageCursor(
            prior: nil, scannedUpperBoundSec: nil, firstSegmentStartSec: 0
        ) == nil)
    }
}
