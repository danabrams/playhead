// CoarseScanStrandedSweepTests.swift
// playhead-1e86: a hard-killed backfill job is invisible to the NEXT background
// grant, because both candidate queries exclude `status='running'` and nothing
// in `handleBackfillTask` ran the reaper.
//
// ----- The shape -----
//
// `BackfillJobRunner` writes `status='running'` immediately before dispatching
// FM and only clears it on a terminal transition. When the window ends by a
// HARD kill (jetsam / suspend) rather than a graceful `expirationHandler`
// cancellation, no terminal is ever written and the row stays `running`. From
// that moment the asset is absent from the FM-first phase in BOTH directions:
//
//   * `fetchAssetIdsWithResumableBackfillJobs` binds `status <> 'running'`
//   * `fetchAssetIdsMissingCoverageLaneJobs` requires `NOT EXISTS` any row
//
// so the asset is neither resumable nor row-less. It is nothing at all.
//
// ----- Why this was a real gap and not a theoretical one -----
//
// The only thing that flips a stale `running` back to `queued` is
// `AnalysisStore.resetStrandedBackfillJobs`, and before this bead its three
// production callers were app launch (`PlayheadRuntime`), the pre-analysis
// recovery handler (`BackgroundProcessingService`) and scene activation
// (`PlayheadApp`). `handleBackfillTask` called none of them.
//
// The bead filed that consequence as NOT ESTABLISHED — it turned on whether two
// backfill grants can in fact be separated by none of those three events. They
// can, and it is the common case. Measured over the 933 `background_task_runs`
// rows preserved across 21 device captures (13 distinct states), spanning
// 2026-07-31 → 2026-08-15:
//
//   * 656 backfill grants, 276 pre-analysis-recovery grants, 22 launch markers
//     (`lastErrorCode='orphan_at_launch'`, whose `finishedAt` IS a launch).
//   * 390 of 655 consecutive backfill-grant pairs — 59.5 % — have NO recovery
//     grant and NO launch marker between them.
//   * 188 of those 390 are separated by more than the reaper's own 600 s
//     freshness floor, which is the window in which a stranded row is both past
//     the floor AND still `running` when the next grant asks who the candidates
//     are.
//   * A first pass read 385/183: it ALSO excluded pairs straddling an
//     `analysis_sessions` row, on the belief that one marked an app launch.
//     That table is a PER-ASSET analysis session (id, analysisAssetId, state,
//     startedAt, …), so the exclusion was a value that names one thing read as
//     though it named another — this bead's own subject, in its own census.
//     Conservative, and still wrong. Use 390/188.
//
// Three distinct jobIds have been observed AT `status='running'` in a preserved
// capture — `fm-9330e821aeb36a0d`, `fm-a7cb7d748c9d58c1`, `fm-df75eb5558560ce2`
// — all `fullEpisodeScan`, all carrying a non-empty `progressCursor`, all stale
// past the floor at the instant of capture.
//
// ----- What the captures CANNOT show, stated because it bounds the claim -----
//
// Zero of the three has a grant recorded AFTER it went stale: in every capture
// the row's `updatedAt` is within ~50 s of the database's own last recorded
// activity. That is a property of snapshots, not of the defect — a pull ends
// the timeline exactly where the evidence would be. So 390 is an OPPORTUNITY
// count, an UPPER bound on firings, not a firing count; a firing additionally
// requires that the first grant of the pair really left a row stranded, and
// nothing durable recorded that. `reaped=` exists so the next pull can answer
// it.
//
// And 390 is an upper bound for a second reason that cannot be measured away:
// SCENE ACTIVATION runs the reaper (`PlayheadApp`) and leaves no
// `background_task_runs` row, because a BGTask grant is by construction the
// only thing that writes one — all 932 rows carry `scenePhase='background'`.
// A foregrounded moment between two grants is invisible here.
//
// ----- The fix, and where it lives -----
//
// `AnalysisCoordinator.runPendingCoarseScans` sweeps before it asks. Not the
// handler, for two reasons: the reaper now sits adjacent to the two predicates
// that exclude what it repairs so they cannot drift apart, and `store` is
// non-optional there while `analysisJobReconciler` is injected late and is
// `nil` on a cold BGTask wake with no scene — the sceneless-launch class that
// has already shipped five times in this repository.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-1e86: the coarse phase reaps stranded `running` rows before it asks")
struct CoarseScanStrandedSweepTests {

    private static let assetId = "asset-1e86"
    private static let episodeId = "https://rss.libsyn.com/shows/101338/x.xml::guid-1e86"
    private static let jobId = "fm-1e86-stranded"
    private static let episodeDuration: Double = 90

    /// Comfortably past `AnalysisStore.strandedJobFreshnessSeconds` (600).
    private static var strandedStamp: Int {
        Int(Date().timeIntervalSince1970) - 3_600
    }

    // MARK: - Fixtures

    private func chunks() -> [TranscriptChunk] {
        (0..<3).map { idx in
            TranscriptChunk(
                id: "c\(idx)-\(Self.assetId)",
                analysisAssetId: Self.assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: "Segment \(idx) of the episode under test.",
                normalizedText: "segment \(idx) of the episode under test.",
                pass: "fast",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: idx
            )
        }
    }

    /// An asset with a transcript and exactly one `backfill_jobs` row, left in
    /// `status` with the given `updatedAt`. `progressCursor` is populated
    /// because the field witnesses all carry one — a stranded row is a row that
    /// banked real work and then lost its process, which is precisely why
    /// leaving it invisible is expensive rather than merely untidy.
    private func storeHoldingOneRow(
        status: BackfillJobStatus,
        updatedAt: Int
    ) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: "fp-\(Self.assetId)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(Self.assetId).m4a",
                featureCoverageEndTime: Self.episodeDuration,
                fastTranscriptCoverageEndTime: Self.episodeDuration,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.episodeDuration
            )
        )
        try await store.insertTranscriptChunks(chunks())
        try await store.insertBackfillJob(makeBackfillJob(
            jobId: Self.jobId,
            analysisAssetId: Self.assetId,
            phase: .fullEpisodeScan,
            status: .queued
        ))
        try await store.forceBackfillJobStateForTesting(
            jobId: Self.jobId,
            status: status,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 0,
                lastProcessedUpperBoundSec: EpisodeSeconds(435.72)
            ),
            updatedAtOverride: updatedAt
        )
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: SpeechService(recognizer: StubSpeechRecognizer()),
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                classifier: RuleBasedClassifier(),
                metadataExtractor: FallbackExtractor(),
                config: AdDetectionConfig(
                    candidateThreshold: 0.40,
                    confirmationThreshold: 0.70,
                    suppressionThreshold: 0.25,
                    hotPathLookahead: 90.0,
                    detectorVersion: "detection-v1",
                    fmBackfillMode: .shadow
                ),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { true }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
    }

    /// Runs the phase with an ALREADY-EXPIRED budget, so the loop publishes
    /// `.floor` and returns without starting an asset. The census report — the
    /// one this bead is about — is published BEFORE that guard, so nothing here
    /// depends on the scan machinery.
    @discardableResult
    private func census(
        _ store: AnalysisStore,
        report log: ReportBox
    ) async -> Int {
        let coordinator = makeCoordinator(store: store)
        return await coordinator.runPendingCoarseScans(
            deadline: ContinuousClock.now,
            minimumWindowBudget: .seconds(60),
            report: { log.note($0) }
        )
    }

    // MARK: - The bead's case

    /// **THE DEFECT, driven end to end.** The anti-vacuity control comes first
    /// and is the load-bearing half: it proves the two candidate queries really
    /// cannot see this row, so that the census seeing it afterwards is
    /// attributable to the sweep rather than to a permissive query.
    ///
    /// It also pins the ORDERING, which is the whole fix. A sweep moved to
    /// AFTER the candidate queries still reports `reaped=1` — it really did
    /// reap a row — while the census it accompanies reads `candidates=0`. Only
    /// asserting both in the SAME report distinguishes the two.
    @Test("a `running` row stranded past the floor is reaped, and the SAME grant sees it",
          .timeLimit(.minutes(1)))
    func strandedRowBecomesACandidateInTheGrantThatReapsIt() async throws {
        let store = try await storeHoldingOneRow(status: .running, updatedAt: Self.strandedStamp)

        // CONTROL. Both candidate queries, asked directly, are blind to it.
        #expect(
            try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 50).isEmpty,
            "`status <> 'running'` excludes it — this is half the defect"
        )
        #expect(
            try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 50).isEmpty,
            "`NOT EXISTS` any row excludes it too — the asset owns one. This is the other half"
        )

        let log = ReportBox()
        await census(store, report: log)

        let first = try #require(log.value.first)
        #expect(first.reaped == 1, "the phase's own sweep must have flipped exactly this row")
        #expect(
            first.candidates == 1,
            """
            …and the candidate census taken AFTER that sweep must contain the \
            asset. `reaped=1` with `candidates=0` is the sweep running too \
            late, which is the mutant this assertion exists for.
            """
        )
        #expect(
            try await store.fetchBackfillJob(byId: Self.jobId)?.status == .queued,
            "the reaper's own contract: `running` -> `queued`, re-admissible"
        )
    }

    /// The 600 s freshness floor is what keeps this sweep from being
    /// destructive in-process, and it is SCHEDULING POLICY — not this bead's to
    /// move. A row touched recently belongs to a live runner: it must stay
    /// `running`, and it must stay out of the candidate list.
    @Test("a FRESH `running` row is left alone, and stays invisible",
          .timeLimit(.minutes(1)))
    func freshRunningRowIsNotReaped() async throws {
        let store = try await storeHoldingOneRow(
            status: .running,
            updatedAt: Int(Date().timeIntervalSince1970)
        )
        let log = ReportBox()
        await census(store, report: log)

        let first = try #require(log.value.first)
        #expect(first.reaped == 0, "measured zero, not unmeasured — the sweep ran and matched nothing")
        #expect(first.candidates == 0, "a live runner's asset must not be handed to a second one")
        #expect(
            try await store.fetchBackfillJob(byId: Self.jobId)?.status == .running,
            "reaping a fresh row would rewind a job whose owner is still holding it"
        )
    }

    /// A sweep that could not RUN has not shown that nothing was stranded.
    /// Reached through a fault-injection seam because the distinction lives
    /// entirely inside one `catch` arm, and a `catch` nothing can enter is a
    /// `catch` no test can reach — the same argument playhead-8ljj's candidate
    /// query seam was added on.
    @Test("a throwing sweep is recorded as UNMEASURED, never as zero",
          .timeLimit(.minutes(1)))
    func throwingSweepIsUnmeasuredNotZero() async throws {
        let store = try await storeHoldingOneRow(status: .running, updatedAt: Self.strandedStamp)
        await store.setStrandedBackfillSweepFaultInjectionForTesting(true)

        let log = ReportBox()
        // The phase must SURVIVE the throw and RETURN, because the drain behind
        // it still needs the window — exactly as it does when a candidate query
        // throws. Reaching this line at all is that assertion.
        let scanned = await census(store, report: log)
        #expect(scanned == 0)

        let first = try #require(log.value.first)
        #expect(first.reaped == nil, "`0` here would claim a measurement nobody took")
        #expect(first.ledgerReason.contains("reaped=?"))
        #expect(!first.ledgerReason.contains("reaped=0"))
        #expect(
            first.candidates == 0,
            "the row is genuinely still hidden — the phase must not pretend otherwise"
        )
        #expect(
            first.verdict == .empty,
            """
            A sweep that threw leaves the candidate list genuinely empty, so \
            the phase short-circuits at its own `candidates.isEmpty` guard and \
            publishes exactly this one report. The verdict must be `empty` and \
            not `inflight`: nothing is in flight.
            """
        )
    }

    /// The control for the test above, and the reason it is not vacuous: the
    /// identical fixture WITHOUT the injection reads `reaped=0`, so the `nil`
    /// is attributable to the throw and not to the fixture.
    @Test("the same fixture without the injection reads a measured zero",
          .timeLimit(.minutes(1)))
    func uninjectedControlReadsMeasuredZero() async throws {
        let store = try await storeHoldingOneRow(
            status: .complete,
            updatedAt: Self.strandedStamp
        )
        let log = ReportBox()
        await census(store, report: log)

        let first = try #require(log.value.first)
        #expect(first.reaped == 0)
        #expect(first.ledgerReason.contains("reaped=0"))
    }

    /// The count travels with EVERY report the phase publishes, not just the
    /// census — for the same reason `unreadable` does. Most granted windows end
    /// by an OS reclaim mid-phase, so the report a reader actually finds in
    /// `background_task_runs.deferReason` is rarely the census one.
    @Test("the reaped count rides every report the phase publishes",
          .timeLimit(.minutes(1)))
    func reapedCountRidesEveryReport() async throws {
        let store = try await storeHoldingOneRow(status: .running, updatedAt: Self.strandedStamp)
        let log = ReportBox()
        await census(store, report: log)

        #expect(log.value.count > 1, "census plus at least one terminal verdict")
        #expect(
            log.value.allSatisfy { $0.reaped == 1 },
            "a terminal verdict that dropped the count would erase the only durable evidence"
        )
    }

    // MARK: - The rendered contract

    /// `reaped=` is rendered on EVERY report, including zero. That is the
    /// anti-vacuity contract `PersistedStateInvariants` states in full: only a
    /// value that is ALWAYS present can distinguish "the sweep ran and found
    /// nothing" from "the sweep never ran". Its absence is now meaningful — it
    /// identifies a row written by a build that did not sweep.
    @Test("the ledger reason always names the sweep, and the three readings differ")
    func ledgerReasonAlwaysNamesTheSweep() {
        let zero = CoarseScanPhaseReport(verdict: .empty, scanned: 0, candidates: 0,
                                         bankedRows: 0, reaped: 0)
        let some = CoarseScanPhaseReport(verdict: .drove, scanned: 1, candidates: 1,
                                         bankedRows: 3, reaped: 2)
        let unmeasured = CoarseScanPhaseReport(verdict: .empty, scanned: 0, candidates: 0,
                                               bankedRows: 0, reaped: nil)

        #expect(zero.ledgerReason == "coarse=empty(0/0) banked=0 reaped=0")
        #expect(some.ledgerReason == "coarse=drove(1/1) banked=3 reaped=2")
        #expect(unmeasured.ledgerReason == "coarse=empty(0/0) banked=0 reaped=?")

        // Three readings, three strings. A renderer that collapsed any pair
        // would be exactly the ledger collapse this whole family of beads
        // exists to undo.
        #expect(Set([zero, some, unmeasured].map(\.ledgerReason)).count == 3)

        // And the modifier ordering is stable, because `deferReason` is grepped
        // by hand off device pulls.
        let withUnread = CoarseScanPhaseReport(verdict: .empty, scanned: 0, candidates: 0,
                                               unreadable: .resumable, bankedRows: nil, reaped: 1)
        #expect(withUnread.ledgerReason == "coarse=empty(0/0) banked=? reaped=1 unread=resumable")
    }

    // MARK: - Helpers

    private final class ReportBox: @unchecked Sendable {
        private let lock = NSLock()
        private var entries: [CoarseScanPhaseReport] = []
        func note(_ report: CoarseScanPhaseReport) {
            lock.lock(); defer { lock.unlock() }
            entries.append(report)
        }
        var value: [CoarseScanPhaseReport] {
            lock.lock(); defer { lock.unlock() }
            return entries
        }
    }
}
