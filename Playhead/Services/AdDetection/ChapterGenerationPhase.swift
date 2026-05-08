// ChapterGenerationPhase.swift
// playhead-au2v.1.10: Shell of the chapter-generation phase
// (admission, snapshot-race, cancellation, lifecycle diagnostics).
// playhead-au2v.1.11: Creator-chapter precedence short-circuit added.
// playhead-au2v.1.12: Parallelized per-candidate FM labeling capped at
// `maxFMConcurrency`, plan-level assembler integration (op-rate gate +
// high-unclear warning), and the `ChapterPlanReadyEvent` emission
// contract (fired exactly once on a successful plan write, never on
// any failure / abort path).
//
// Phase contract (across both beads):
//
//   1. ChapterSignalMode gate (`.off` → no work, no diagnostic, no ready
//      event).
//   2. Admission control (delegates to an injected
//      `ChapterPhaseAdmissionPolicy`; matches the spirit of the
//      `CapabilitySnapshot.canUseFoundationModels` gate used by
//      `FoundationModelExtractor` — the policy here is a thin DI
//      seam so the phase can be tested without spinning up the live
//      capability service).
//   3. Creator-chapter precedence (au2v.1.11). Before any FM cost is
//      incurred (no boundary detection, no labeling), the shell asks
//      the injected `CreatorChapterProviding` whether the episode
//      already has at least one `ChapterEvidence` with a creator
//      `ChapterSource` (`.id3`, `.pc20`, `.rssInline`). If so the
//      phase exits early with `chapter_phase_skipped_creator_chapters`
//      and writes nothing — creator chapters are near-ground-truth and
//      we will not pay FM cost when ground truth is available. Low
//      quality scores, partial coverage, and all-`.content`
//      dispositions still trigger the skip; the spec is explicit that
//      even imperfect creator chapters beat statistical inference, and
//      shadow eval can revisit that policy via a follow-up bead.
//      `.inferred` chapters are NOT a creator source and never trigger
//      the skip.
//   4. Transcript-snapshot race protection: a content hash is captured
//      at phase entry, then re-fetched immediately before the cache
//      write. A mismatch aborts the run, discards the plan, and emits
//      a `chapter_phase_preempted` diagnostic (the same event used by
//      explicit cancellation — both express "the input we built this
//      plan against is no longer current"; the inline comment at the
//      recheck-mismatch branch in `run()` explains why we collapse
//      cancellation and recheck-mismatch onto the same wire event.
//      The `.raceAborted` Outcome lets in-process callers distinguish
//      the two without parsing the OS log.
//   5. Cooperative cancellation honoring task cancellation. We use
//      `Task.isCancelled` checks at every yield point (rather than
//      `try Task.checkCancellation()`) so the shell can collapse a
//      cancellation into the structured `Outcome.preempted` return —
//      keeping the call site exhaustive over the `Outcome` enum
//      without forcing callers into a `do/catch CancellationError`.
//      Throws of `CancellationError` from the detector or labeler are
//      ALSO honored (caught and routed through the same `preempt`
//      helper) — the `FoundationModelClassifier` pattern of throwing
//      cancellation is still respected at the seam. The parallel
//      labeling is wrapped in a TaskGroup whose `cancelAll()` fans the
//      cancel out to every in-flight FM call; cancellation discards
//      partial state — no plan write, no ready event.
//   6. Per-candidate FM labeling, dispatched through a TaskGroup
//      capped at `maxFMConcurrency` (see static constant). Per-call
//      operational failures (non-cancellation throws) DO NOT abort the
//      batch — the survivors flow into `ChapterPlanAssembler` which
//      runs the (>30%) operational-unclear gate AND the (>50%)
//      high-unclear warning. The op-rate gate produces an abort
//      Outcome AND its own diagnostic event; no plan is written and
//      no ready event fires.
//   7. Cache write into `ChapterPlanCache` (bead .1) on a successful
//      assembly, followed by the `chapter_phase_completed` diagnostic
//      AND the `ChapterPlanReadyEvent` for in-process consumers (a
//      coverage-plan refresh worker etc., wired in a later bead).
//      When the assembler flags high-unclear, the warning event fires
//      BEFORE `chapter_phase_completed` so support engineers see the
//      reduced-confidence signal aligned with the run that produced
//      it. If the cache write itself fails (`ChapterPlanCache.put`
//      returns `false` — disk full, directory creation refused,
//      encoder crash), neither `chapter_phase_completed` nor
//      `ChapterPlanReadyEvent` fires; the run emits
//      `chapter_phase_preempted` and returns `.preempted`. This
//      preserves the consumer-facing invariant that
//      "ChapterPlanReady ⇒ a fresh plan is observable in
//      `ChapterPlanCache`".
//
// Out of scope (later beads):
//   * Wiring into `AdDetectionService`'s backfill path (.13).
//   * Consumers subscribing to `ChapterPlanReadyEvent` (the event is
//     informational for now; bead 12 only guarantees the publisher
//     contract).
//
// Logging discipline:
//   * Every exit path EXCEPT the `.off` short-circuit emits exactly
//     one phase-completion diagnostic (success or specific failure).
//     "Phase-completion" here means terminal events:
//     `.skippedAdmission`, `.skippedCreatorChapters`, `.noCandidates`,
//     `.preempted`, `.operationalUnclearRateExceeded`, `.completed`.
//     There is never more than one of those per run.
//     Note that `transcriptUnavailable` exits early but DOES emit
//     `.noCandidates` — we still want telemetry on "phase admitted
//     but had no transcript" for dogfood mis-scheduling debugging.
//   * One `.started` event fires at phase entry (after the entry
//     transcript-hash snapshot succeeds, since the started payload
//     requires that hash). Paths that exit *before* the snapshot
//     succeeds (`.off` / admission deny / creator-chapter skip /
//     transcript unavailable) do NOT emit `.started` — those exits
//     are "phase never truly began" and the bead .3 diagnostic
//     schema agrees with that reading.
//   * The `.highUnclearRate` event, when emitted, sits BETWEEN
//     `.started` and `.completed` — it is a non-terminal warning, not
//     a phase-outcome event.
//   * `ChapterSignalMode.off` emits NO diagnostic at all — the
//     feature is fully off, and we want zero surface area in shipped
//     bundles when the flag is off.
//
// FM concurrency cap rationale:
//   The bead spec says "capped at the existing FM concurrency limit
//   (~2 on-device for FoundationModels)". The repo does NOT currently
//   define an explicit project-wide constant — `FoundationModelClassifier`
//   relies on Apple's per-`LanguageModelSession` serialization (a
//   second concurrent request inside the same session surfaces as
//   `.concurrentRequests`, which `ChapterLabelingService.classify`
//   folds into a `.rateLimited` operational error). Other on-device
//   FM consumers (`ShadowCaptureConfig.laneAMaxInFlight`) document the
//   same constraint: "FoundationModels is serialized per-session on
//   device, so concurrency above 1 offers little throughput gain
//   while multiplying peak memory."
//
//   For the chapter generation phase we use a fresh
//   `LanguageModelSession` per call (see `ChapterLabelingService.live`
//   — bd-34e Fix B avoids per-call context-window bloat), so two
//   sessions CAN run in parallel without tripping
//   `.concurrentRequests`. Going above 2 burns peak memory + thermal
//   budget for diminishing throughput. We cap at 2 here as the
//   conservative default — `maxFMConcurrency` is `static`, so a
//   future tuning experiment can adjust it in one place if the
//   measured trade-off shifts.

import Foundation
import OSLog

// MARK: - Public DI seams

/// Decision returned by `ChapterPhaseAdmissionPolicy`. The deny case
/// carries a snake_case reason string that flows verbatim into
/// `chapter_phase_skipped_admission`'s `deny_reason` payload field.
/// Examples: `thermal_pressure`, `fm_unavailable`, `region_unsupported`,
/// `hardware_unsupported`. The vocabulary is intentionally open here —
/// the policy implementation chooses the granularity, the diagnostic
/// just records it.
enum ChapterPhaseAdmissionDecision: Sendable, Hashable, Equatable {
    case admit
    case deny(reason: String)
}

/// Pluggable admission policy. Production wiring (deferred to bead .13)
/// will adapt this to the live `CapabilitySnapshot` + thermal /
/// charging signals; tests inject a canned decision.
protocol ChapterPhaseAdmissionPolicy: Sendable {
    func decide() async -> ChapterPhaseAdmissionDecision
}

/// Boundary-detection seam. Bead .4 + .5 provide the real
/// implementation; this bead exercises the call site through a stub.
protocol ChapterBoundaryDetecting: Sendable {
    func detect() async throws -> [ChapterBoundaryCandidate]
}

/// Labeling seam. Bead .7 provides the real FM-backed implementation
/// (`ChapterLabelingService`). The shell calls this once per
/// candidate, in parallel under a TaskGroup capped at
/// `maxFMConcurrency`.
///
/// Returns:
///  * `LabelingResult` — every successful FM call (confident,
///    semantic-unclear, or operationally-failed-with-known-vocabulary).
///    The phase passes the result list to `ChapterPlanAssembler` so
///    plan-level gates can fire.
///  * `nil` — the labeler chose to silently skip this candidate (e.g.
///    region too short to embed in the prompt). Skipped candidates
///    contribute neither to the assembler's denominator nor to the
///    plan; they are simply absent.
///
/// Throws:
///  * `CancellationError` — propagated up so the TaskGroup tears down
///    cleanly. The phase collapses this into `.preempted`.
///  * Any other `Error` — treated as an operational failure for that
///    one candidate. The phase synthesizes an operational
///    `LabelingResult` so the assembler's op-rate gate can see it.
protocol ChapterLabeling: Sendable {
    func label(
        candidate: ChapterBoundaryCandidate
    ) async throws -> LabelingResult?
}

/// Source of the "current transcript content hash". Called twice per
/// run: once on entry (snapshot), once before the cache write
/// (race re-check). `nil` on entry → `.transcriptUnavailable`; `nil`
/// or different hash on recheck → `.raceAborted`.
protocol TranscriptHashProviding: Sendable {
    func currentTranscriptHash() async -> String?
}

/// Source of pre-existing creator-supplied chapters (bead .11).
///
/// The phase calls this AFTER admission and BEFORE any FM cost is
/// incurred. If the source returns one or more `ChapterEvidence` whose
/// `source` is a creator origin (`.id3`, `.pc20`, `.rssInline`), the
/// phase short-circuits with `chapter_phase_skipped_creator_chapters`.
///
/// `.inferred` chapters returned by this seam are ignored for the
/// purpose of the precedence skip — only `.id3`, `.pc20`, and
/// `.rssInline` are creator sources. (The shell filters on
/// `ChapterSource.isCreatorSource`; if the seam returns a mixed
/// payload that includes inferred chapters it is silently filtered.)
///
/// Production wiring is deferred to bead .13 (the production adapter
/// will bridge to whichever cache/parser layer holds creator chapters
/// for an episode); the spec acknowledges that creator-chapter
/// storage may live in a different cache layer than the ad-detection
/// artifacts cache, and this seam intentionally hides that detail.
/// Tests inject a mock that returns a canned creator-chapter set per
/// scenario.
///
/// The seam is keyed by raw `episodeId` rather than a transcript
/// content hash because creator chapters are sourced from episode
/// metadata (RSS/ID3/PC20) — they exist independently of (and
/// typically before) the transcript pipeline producing a hashable
/// snapshot. This also lets the phase short-circuit for episodes
/// where the transcript is not yet available but creator chapters
/// already are. The adapter is free to derive a content hash
/// internally if its underlying cache requires one.
protocol CreatorChapterProviding: Sendable {
    /// Returns the set of `ChapterEvidence` already known for this
    /// episode from creator-supplied sources (and possibly other
    /// origins; the phase filters on `ChapterSource.isCreatorSource`).
    /// An empty array means "no creator chapters present" and the
    /// phase proceeds to FM generation.
    func creatorChapters(episodeId: String) async -> [ChapterEvidence]
}

/// Sink for `ChapterPhaseEvent`s. Production wiring (bead .13 / a
/// later persistence bead) will route these into the diagnostics
/// store; tests inject an in-memory recorder.
///
/// Implementations must be safe to call from any task / actor context.
protocol ChapterPhaseEventSink: Sendable {
    func record(_ event: ChapterPhaseEvent) async
}

/// Lightweight candidate record for a single boundary the detector
/// proposes. Owned by the phase shell so detector and labeler can be
/// developed against a stable contract.
struct ChapterBoundaryCandidate: Sendable, Hashable, Equatable {
    let startTime: TimeInterval
    let endTime: TimeInterval?

    init(startTime: TimeInterval, endTime: TimeInterval? = nil) {
        self.startTime = startTime
        self.endTime = endTime
    }
}

// MARK: - ChapterGenerationPhase

/// Stateless phase shell. Constructed once (or per run — both are
/// cheap, no actor, no caches) and `run()` invoked to drive the
/// pipeline.
///
/// Concurrency: the phase itself is *not* actor-isolated — it is a
/// plain `struct` whose dependencies are all `Sendable`. Per-candidate
/// labeling is dispatched through a TaskGroup capped at
/// `maxFMConcurrency`; cache writes hop onto `ChapterPlanCache`'s
/// actor automatically.
struct ChapterGenerationPhase: Sendable {

    // MARK: - Tunables

    /// Maximum number of FM labeling tasks the phase will run in
    /// parallel. Documented at the top of the file: 2 reflects the
    /// "FoundationModels serialized per-session" constraint plus the
    /// memory / thermal trade-off from running multiple sessions.
    /// `static` so a future tuning experiment can adjust the value in
    /// one place; not parameterized on the init() because every
    /// production caller wants the same value.
    static let maxFMConcurrency: Int = 2

    // MARK: Dependencies

    private let admissionPolicy: ChapterPhaseAdmissionPolicy
    private let creatorChapterProvider: CreatorChapterProviding
    private let boundaryDetector: ChapterBoundaryDetecting
    private let labeler: ChapterLabeling
    private let transcriptHashProvider: TranscriptHashProviding
    private let cache: ChapterPlanCache
    private let eventSink: ChapterPhaseEventSink
    private let planReadySink: ChapterPlanReadyEventSink
    private let assembler: ChapterPlanAssembler
    private let clock: @Sendable () -> Date
    private let logger: Logger

    // MARK: Init

    init(
        admissionPolicy: ChapterPhaseAdmissionPolicy,
        creatorChapterProvider: CreatorChapterProviding,
        boundaryDetector: ChapterBoundaryDetecting,
        labeler: ChapterLabeling,
        transcriptHashProvider: TranscriptHashProviding,
        cache: ChapterPlanCache,
        eventSink: ChapterPhaseEventSink,
        planReadySink: ChapterPlanReadyEventSink = NoopChapterPlanReadyEventSink(),
        assembler: ChapterPlanAssembler = ChapterPlanAssembler(),
        clock: @escaping @Sendable () -> Date = { Date() },
        logger: Logger = Logger(subsystem: "com.playhead", category: "ChapterGenerationPhase")
    ) {
        self.admissionPolicy = admissionPolicy
        self.creatorChapterProvider = creatorChapterProvider
        self.boundaryDetector = boundaryDetector
        self.labeler = labeler
        self.transcriptHashProvider = transcriptHashProvider
        self.cache = cache
        self.eventSink = eventSink
        self.planReadySink = planReadySink
        self.assembler = assembler
        self.clock = clock
        self.logger = logger
    }

    // MARK: Public entry

    /// Outcome enum the caller can branch on. `cached` is the success
    /// case; the others are the explicit short-circuits a backfill
    /// scheduler may want to surface.
    enum Outcome: Sendable, Hashable, Equatable {
        case modeOff
        case admissionDenied(reason: String)
        /// Phase short-circuited because the episode already has at
        /// least one creator-supplied chapter (`.id3` / `.pc20` /
        /// `.rssInline`). No FM cost was incurred and no
        /// `ChapterPlan` was written. The associated value mirrors
        /// the diagnostic payload's count so callers can surface a
        /// quick "we already had N creator chapters" message without
        /// re-querying the provider.
        case skippedCreatorChapters(creatorChapterCount: Int)
        case noCandidates
        case transcriptUnavailable
        case raceAborted
        /// Returned for any "no observable plan landed" non-race
        /// terminal state: explicit cancellation, labeler throwing
        /// `CancellationError`, OR a cache-put failure (disk full,
        /// directory creation refused, encoder crash). Bead-12
        /// callers cannot today distinguish those three sub-cases —
        /// if a future bead needs that, split this case rather than
        /// reading the diagnostic event log.
        case preempted
        /// Plan-level operational-unclear rate exceeded the assembler's
        /// strict 30% threshold; no plan was written.
        case operationalRateExceeded(rate: Double, threshold: Double)
        case cached(chapterCount: Int, planConfidence: Double)
    }

    /// Run the phase end-to-end.
    func run(
        mode: ChapterSignalMode,
        episodeId: String,
        installID: UUID
    ) async -> Outcome {
        // 1. Mode gate — `.off` is the only path that emits no
        //    diagnostic. The feature is fully off; no surface area.
        guard mode.runsChapterGeneration else {
            return .modeOff
        }

        let startedAtTimestamp = clock().timeIntervalSince1970

        // 2. Admission. Checked BEFORE the entry-hash snapshot so a
        //    denied phase never even reads the transcript cache.
        let admission = await admissionPolicy.decide()
        if case .deny(let reason) = admission {
            await recordSkippedAdmission(
                installID: installID,
                episodeId: episodeId,
                timestamp: startedAtTimestamp,
                denyReason: reason
            )
            return .admissionDenied(reason: reason)
        }

        if Task.isCancelled {
            return await preempt(installID: installID, episodeId: episodeId)
        }

        // 3. Creator-chapter precedence (au2v.1.11). If the episode
        //    already exposes at least one creator-supplied chapter
        //    (`.id3` / `.pc20` / `.rssInline`), short-circuit BEFORE
        //    boundary detection or labeling — both incur FM cost we
        //    refuse to pay when ground truth is available. The check
        //    runs after admission but before the transcript-hash
        //    snapshot, so this exit (like the `.off` and admission-
        //    deny exits) emits no `.started` event: the phase never
        //    truly began. The diagnostic
        //    `chapter_phase_skipped_creator_chapters` is the single
        //    terminal event for this exit path.
        //
        //    Edge-case policy (encoded by FILTERING and not by extra
        //    branches, so each rule is exercised by an explicit
        //    test):
        //      * Low qualityScore (< 0.5) — STILL skip. Even imperfect
        //        creator chapters beat statistical inference; shadow
        //        eval can revisit if this proves wrong.
        //      * Partial coverage (only first half of episode) —
        //        STILL skip. Mixed creator+inferred plans are a
        //        follow-up bead.
        //      * All `.content` disposition (no ads) — STILL skip.
        //        Trust the creator's "no ads here" implicit signal.
        //      * `.inferred` chapters returned by the provider —
        //        ignored; only creator sources count via
        //        `ChapterSource.isCreatorSource`. A provider that
        //        accidentally mixes inferred+creator chapters still
        //        triggers the skip iff creator chapters exist.
        let allChapters = await creatorChapterProvider.creatorChapters(
            episodeId: episodeId
        )
        let creatorChapters = allChapters.filter { $0.source.isCreatorSource }
        if !creatorChapters.isEmpty {
            await recordSkippedCreatorChapters(
                installID: installID,
                episodeId: episodeId,
                timestamp: clock().timeIntervalSince1970,
                creatorChapters: creatorChapters
            )
            return .skippedCreatorChapters(
                creatorChapterCount: creatorChapters.count
            )
        }

        if Task.isCancelled {
            return await preempt(installID: installID, episodeId: episodeId)
        }

        // 4. Transcript snapshot capture. A `nil` here means the
        //    transcript pipeline has not produced anything to hash —
        //    typically the phase was invoked too early. Emit
        //    `.noCandidates` (the catch-all "phase ran, produced
        //    nothing" event) and exit with `.transcriptUnavailable`
        //    so in-process callers can distinguish the two cases.
        guard let entryHash = await transcriptHashProvider.currentTranscriptHash() else {
            logger.notice(
                "chapterphase.transcript_unavailable_at_entry — emitting no_candidates"
            )
            await emitNoCandidates(installID: installID, episodeId: episodeId)
            return .transcriptUnavailable
        }

        // The phase has now "truly begun": admission passed, the
        // creator-chapter precedence check did not short-circuit,
        // AND we have a transcript snapshot to anchor the run. Emit
        // the single `.started` lifecycle event — the bead .3 schema
        // requires this be paired with exactly one terminal event
        // (`.noCandidates` / `.preempted` /
        // `.operationalUnclearRateExceeded` / `.completed`) per run
        // from this point onward. The earlier short-circuit exits
        // (`.modeOff` / `.skippedAdmission` / `.skippedCreatorChapters`)
        // are themselves single-terminal-event paths that bypass
        // `.started` entirely (the phase never truly began on those).
        //
        // Timestamp note: we stamp `.started` with `startedAtTimestamp`
        // captured BEFORE admission, not the wall-clock at this emit
        // line. This makes `completed.latency_ms` =
        // `completed.timestamp - started.timestamp` exactly, which
        // is the contract telemetry consumers expect.
        await recordStarted(
            installID: installID,
            episodeId: episodeId,
            timestamp: startedAtTimestamp,
            mode: mode.rawValue,
            transcriptSnapshotHash: entryHash
        )

        if Task.isCancelled {
            return await preempt(installID: installID, episodeId: episodeId)
        }

        // 5. Boundary detection. The detector itself may throw
        //    `CancellationError` on a cooperative cancel; treat that
        //    as a preempt. Any other thrown error is logged and
        //    surfaced as `noCandidates` (the bead .3 wire offers no
        //    "detector errored" event today; aborting cleanly with
        //    `noCandidates` keeps the consumer contract simple — the
        //    OS log carries the underlying failure).
        let candidates: [ChapterBoundaryCandidate]
        do {
            candidates = try await boundaryDetector.detect()
        } catch is CancellationError {
            return await preempt(installID: installID, episodeId: episodeId)
        } catch {
            logger.error(
                "chapterphase.boundary_detection_failed: \(error.localizedDescription, privacy: .public)"
            )
            await emitNoCandidates(installID: installID, episodeId: episodeId)
            return .noCandidates
        }

        if Task.isCancelled {
            return await preempt(installID: installID, episodeId: episodeId)
        }

        guard !candidates.isEmpty else {
            await emitNoCandidates(installID: installID, episodeId: episodeId)
            return .noCandidates
        }

        // 6. Parallel labeling pass.
        //
        //    A TaskGroup runs at most `maxFMConcurrency` FM calls in
        //    parallel. Each task carries its candidate's input INDEX
        //    so out-of-order completions can be reassembled into
        //    start-time order before the assembler runs.
        //
        //    Per-call outcomes:
        //      * `LabelingResult?` returned → the index slot for that
        //        candidate is set to that result (or left `nil` for
        //        skip-without-result).
        //      * non-cancellation throw → synthesized into an
        //        operational `LabelingResult` so the assembler's
        //        op-rate gate can see it. The synthesis MIRRORS
        //        `ChapterLabelingService.operationalResult(...)`:
        //        `disposition = .ambiguous`, `qualityScore = 0`,
        //        `failureMode = .operational`, `attempts = 1`.
        //      * `CancellationError` → re-thrown out of the inner
        //        task; the TaskGroup tears down via `cancelAll()` and
        //        the phase returns `.preempted`. Partial state is
        //        discarded.
        let labelingOutcomes: [LabelingResult?]
        do {
            labelingOutcomes = try await runParallelLabeling(candidates: candidates)
        } catch is CancellationError {
            return await preempt(installID: installID, episodeId: episodeId)
        } catch {
            // No other error path is possible from runParallelLabeling
            // (per-call non-cancellation throws are folded into
            // operational results inside the group). Defense-in-depth
            // logging in case a future change introduces a leak.
            logger.error(
                "chapterphase.parallel_labeling_unexpected_error: \(error.localizedDescription, privacy: .public)"
            )
            return await preempt(installID: installID, episodeId: episodeId)
        }

        if Task.isCancelled {
            return await preempt(installID: installID, episodeId: episodeId)
        }

        // 7. Race re-check. We re-fetch the transcript hash and
        //    compare it to the snapshot taken in step 4. If the
        //    transcript changed under us (re-transcription, edit,
        //    user re-imported), discard the plan; do NOT cache. We
        //    emit `preempted` rather than inventing a new event
        //    type — both this case and explicit cancellation share
        //    the same actionable shape ("the input we computed
        //    against is no longer current; nothing was persisted").
        //    The in-process Outcome is `.raceAborted` so callers can
        //    still distinguish race-vs-cancellation without parsing
        //    the OS log.
        let recheckHash = await transcriptHashProvider.currentTranscriptHash()
        guard let recheckHash, recheckHash == entryHash else {
            logger.notice(
                "chapterphase.transcript_changed_during_run entry=\(entryHash, privacy: .public) recheck=\(recheckHash ?? "nil", privacy: .public)"
            )
            await recordPreempted(
                installID: installID,
                episodeId: episodeId,
                timestamp: clock().timeIntervalSince1970
            )
            return .raceAborted
        }

        // 8. Compact the labeling outcomes (drop `nil` skip slots,
        //    retain order) and run the plan-level assembler.
        //
        //    `.aborted` from the assembler → emit
        //    `chapter_phase_operational_unclear_rate_exceeded`, NO
        //    cache write, NO ready event. Returns
        //    `.operationalRateExceeded(rate, threshold)` so the
        //    caller can branch on the abort reason.
        //
        //    `.assembled` → write the plan, conditionally emit the
        //    high-unclear warning, emit `.completed`, fire the
        //    `ChapterPlanReadyEvent`.
        //
        //    The total FM-call count surfaced in the `.completed`
        //    payload is the number of slots that produced a
        //    LabelingResult (including operational synthesis) —
        //    skipped (`nil`) slots are NOT counted, since "FM cost
        //    incurred for this completed plan" is the metric the
        //    payload field documents.
        let labelingResults = labelingOutcomes.compactMap { $0 }
        let fmCallCount = labelingResults.count

        let assemblyResult = assembler.assemble(
            results: labelingResults,
            episodeContentHash: entryHash,
            candidatesDetected: candidates.count,
            candidatesKept: candidates.count,
            generatedAt: clock()
        )

        switch assemblyResult {
        case .aborted(let abortInfo):
            await recordOperationalRateExceeded(
                installID: installID,
                episodeId: episodeId,
                timestamp: clock().timeIntervalSince1970,
                info: abortInfo
            )
            return .operationalRateExceeded(
                rate: abortInfo.operationalUnclearRate,
                threshold: abortInfo.threshold
            )

        case .assembled(let plan, let warnings):
            // Persist BEFORE emitting `.completed` and the ready
            // event — both events advertise that the plan is
            // observable in the cache. If the persistence layer
            // fails (disk full, directory creation refused, encoder
            // crash), the bead-12 contract says ChapterPlanReady
            // MUST NOT fire (subscribers rely on "ready ⇒
            // observable"). We still emit `.preempted` rather than
            // `.completed` because the run did not produce an
            // observable plan — preempted is the existing "no plan
            // landed" terminal event and avoids inventing a new
            // diagnostic for what is otherwise a rare disk-pressure
            // failure.
            let persisted = await cache.put(contentHash: entryHash, plan: plan)
            guard persisted else {
                logger.error(
                    "chapterphase.cache_put_failed hash=\(entryHash, privacy: .public) chapters=\(plan.chapters.count, privacy: .public) — emitting preempted, suppressing ready event"
                )
                await recordPreempted(
                    installID: installID,
                    episodeId: episodeId,
                    timestamp: clock().timeIntervalSince1970
                )
                return .preempted
            }

            // High-unclear warning is non-terminal: it precedes
            // `.completed` so support engineers see them in order.
            if warnings.highUnclearRateExceeded {
                await recordHighUnclearRate(
                    installID: installID,
                    episodeId: episodeId,
                    timestamp: clock().timeIntervalSince1970,
                    warnings: warnings
                )
            }

            let completedAt = clock()
            let latencyMs =
                (completedAt.timeIntervalSince1970 - startedAtTimestamp) * 1_000.0
            await recordCompleted(
                installID: installID,
                episodeId: episodeId,
                timestamp: completedAt.timeIntervalSince1970,
                chapterCount: plan.chapters.count,
                planConfidence: plan.planConfidence,
                fmCallCount: fmCallCount,
                latencyMs: latencyMs
            )

            // Ready event fires AFTER `.completed` so subscribers
            // observing both can rely on the diagnostic having
            // landed first. Bead 12 contract: this is the ONLY path
            // that emits `ChapterPlanReady`, and ONLY when the
            // cache write succeeded above.
            await planReadySink.record(
                ChapterPlanReadyEvent(
                    episodeContentHash: plan.episodeContentHash,
                    planConfidence: plan.planConfidence,
                    chapterCount: plan.chapters.count,
                    generatedAt: plan.generatedAt
                )
            )

            return .cached(
                chapterCount: plan.chapters.count,
                planConfidence: plan.planConfidence
            )
        }
    }

    // MARK: - Parallel labeling

    /// Run the parallel labeling pass. Returns a same-length array of
    /// optional `LabelingResult` aligned to the input candidate
    /// order: `result[i]` corresponds to `candidates[i]`.
    ///
    /// `nil` slots are silent skips (labeler returned `nil`);
    /// non-cancellation throws are folded into a synthesized
    /// `.operational` `LabelingResult` so the assembler's op-rate gate
    /// can see the failure.
    ///
    /// Throws `CancellationError` if the parent task was cancelled OR
    /// any inner labeling call threw `CancellationError`. The
    /// TaskGroup tears down with `cancelAll()` and partial state is
    /// discarded by the caller (no plan write, no ready event). The
    /// cancellation throw from a labeler is the documented signal
    /// that "this run is being torn down" — collapsing it into an
    /// operational result would let the assembler write a plan
    /// against a cancelled run, which violates the bead-12 contract
    /// "cancellation discards partial state".
    private func runParallelLabeling(
        candidates: [ChapterBoundaryCandidate]
    ) async throws -> [LabelingResult?] {
        let cap = max(1, Self.maxFMConcurrency)
        let n = candidates.count

        // The result array is filled by index so out-of-order task
        // completion still produces a correctly-aligned slice.
        var results: [LabelingResult?] = Array(repeating: nil, count: n)

        try await withThrowingTaskGroup(
            of: (Int, LabelingResult?).self
        ) { group in
            // Submit the first `cap` tasks, then submit a new one for
            // every completion until the candidate queue drains. This
            // is the standard "rate-limited TaskGroup" pattern (see
            // Apple sample code; the equivalent of a semaphore-limited
            // dispatcher with structured concurrency cancellation).
            var nextIndex = 0
            let initialBatch = Swift.min(cap, n)
            for _ in 0..<initialBatch {
                let i = nextIndex
                nextIndex += 1
                let candidate = candidates[i]
                let labeler = self.labeler
                group.addTask {
                    try await Self.labelOnce(
                        labeler: labeler,
                        candidate: candidate,
                        index: i
                    )
                }
            }

            do {
                while let next = try await group.nextWithCancellationPropagation() {
                    let (index, value) = next
                    results[index] = value
                    if Task.isCancelled {
                        group.cancelAll()
                        throw CancellationError()
                    }
                    if nextIndex < n {
                        let i = nextIndex
                        nextIndex += 1
                        let candidate = candidates[i]
                        let labeler = self.labeler
                        group.addTask {
                            try await Self.labelOnce(
                                labeler: labeler,
                                candidate: candidate,
                                index: i
                            )
                        }
                    }
                }
            } catch {
                // Either a labeler threw `CancellationError` or the
                // parent task was cancelled. Either way, fan the
                // cancel out to the remaining in-flight tasks and
                // surface the error so the caller can short-circuit
                // to the `.preempted` outcome. We swallow the
                // remaining task results (they're operational
                // synthesizations and we're discarding partial state
                // anyway).
                group.cancelAll()
                throw error
            }
        }

        if Task.isCancelled {
            throw CancellationError()
        }

        return results
    }

    /// Run one labeling call. `CancellationError` is re-thrown so the
    /// TaskGroup observes cancellation and the phase short-circuits
    /// to `.preempted` (partial state is discarded by the caller).
    /// Any other error is folded into an `.operational`
    /// `LabelingResult` so the assembler's op-rate gate can see the
    /// failure as a per-call operational issue rather than
    /// terminating the whole batch.
    private static func labelOnce(
        labeler: ChapterLabeling,
        candidate: ChapterBoundaryCandidate,
        index: Int
    ) async throws -> (Int, LabelingResult?) {
        do {
            let result = try await labeler.label(candidate: candidate)
            return (index, result)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            return (index, synthesizedOperationalResult(for: candidate))
        }
    }

    /// Synthesize an `.operational` `LabelingResult` for a candidate
    /// whose labeler call threw a non-cancellation error. Mirrors
    /// `ChapterLabelingService.operationalResult(...)` so the
    /// assembler sees the same shape for in-service vs. shell-
    /// synthesized failures.
    private static func synthesizedOperationalResult(
        for candidate: ChapterBoundaryCandidate
    ) -> LabelingResult {
        let evidence = ChapterEvidence(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            title: nil,
            source: .inferred,
            disposition: .ambiguous,
            qualityScore: 0
        )
        return LabelingResult(
            chapter: evidence,
            labelDisposition: .unclear,
            topicDescriptor: nil,
            failureMode: .operational,
            attempts: 1
        )
    }

    // MARK: - Cancellation / no-candidates collapse helpers

    private func preempt(
        installID: UUID,
        episodeId: String
    ) async -> Outcome {
        await recordPreempted(
            installID: installID,
            episodeId: episodeId,
            timestamp: clock().timeIntervalSince1970
        )
        return .preempted
    }

    private func emitNoCandidates(
        installID: UUID,
        episodeId: String
    ) async {
        await recordNoCandidates(
            installID: installID,
            episodeId: episodeId,
            timestamp: clock().timeIntervalSince1970
        )
    }

    // MARK: - Diagnostic emit helpers (one per lifecycle event)

    private func recordStarted(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        mode: String,
        transcriptSnapshotHash: String
    ) async {
        await eventSink.record(
            .started(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp,
                mode: mode,
                transcriptSnapshotHash: transcriptSnapshotHash
            )
        )
    }

    private func recordSkippedAdmission(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        denyReason: String
    ) async {
        await eventSink.record(
            .skippedAdmission(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp,
                denyReason: denyReason
            )
        )
    }

    /// Records the `chapter_phase_skipped_creator_chapters` event for
    /// the au2v.1.11 short-circuit. Computes the deduplicated, sorted
    /// `creator_chapter_sources` list and the min/max/avg quality
    /// score across the supplied creator chapter set. The caller is
    /// responsible for filtering out non-creator sources before
    /// invoking this helper (we assert the invariant in DEBUG so a
    /// drift in the call site is loud).
    ///
    /// `creatorChapters` MUST be non-empty; the call site guards on
    /// `!isEmpty` before calling, so an empty array here is a
    /// programmer error and we trap with a precondition rather than
    /// silently emit a divide-by-zero average.
    private func recordSkippedCreatorChapters(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        creatorChapters: [ChapterEvidence]
    ) async {
        precondition(
            !creatorChapters.isEmpty,
            "recordSkippedCreatorChapters called with empty creator chapters"
        )
        #if DEBUG
        assert(
            creatorChapters.allSatisfy { $0.source.isCreatorSource },
            "recordSkippedCreatorChapters received non-creator-source chapter — caller must pre-filter"
        )
        #endif

        // Deduplicate sources, normalize to snake_case wire form, and
        // sort alphabetically so the wire shape is deterministic
        // across runs (matters for golden tests + bundle diff review).
        let sources = Set(creatorChapters.map { $0.source })
        let sourceWireValues = sources
            .map { ChapterPhaseEvent.snakeCaseSourceName($0) }
            .sorted()

        // Compute quality stats. `qualityScore` is a `Float` clamped
        // to `[0, 1]` by the scorer; we widen to `Double` for the
        // wire payload (the rest of the diagnostic surface uses
        // `Double` for fractional fields). We compute on the typed
        // values rather than pre-widening so the loop body stays
        // allocation-free.
        var minScore: Float = 1.0
        var maxScore: Float = 0.0
        var sum: Double = 0.0
        for chapter in creatorChapters {
            let score = chapter.qualityScore
            if score < minScore { minScore = score }
            if score > maxScore { maxScore = score }
            sum += Double(score)
        }
        let avgScore = sum / Double(creatorChapters.count)

        await eventSink.record(
            .skippedCreatorChapters(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp,
                creatorChapterCount: creatorChapters.count,
                creatorChapterSources: sourceWireValues,
                creatorQualityScoreMin: Double(minScore),
                creatorQualityScoreMax: Double(maxScore),
                creatorQualityScoreAvg: avgScore
            )
        )
    }

    private func recordNoCandidates(
        installID: UUID,
        episodeId: String,
        timestamp: Double
    ) async {
        await eventSink.record(
            .noCandidates(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp
            )
        )
    }

    private func recordPreempted(
        installID: UUID,
        episodeId: String,
        timestamp: Double
    ) async {
        await eventSink.record(
            .preempted(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp
            )
        )
    }

    private func recordOperationalRateExceeded(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        info: ChapterPlanAssembler.AbortInfo
    ) async {
        await eventSink.record(
            .operationalUnclearRateExceeded(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp,
                labeledCount: info.labeledCount,
                operationalUnclearCount: info.operationalUnclearCount,
                operationalUnclearRate: info.operationalUnclearRate,
                threshold: info.threshold
            )
        )
    }

    private func recordHighUnclearRate(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        warnings: ChapterPlanAssembler.AssemblyWarnings
    ) async {
        await eventSink.record(
            .highUnclearRate(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp,
                labeledCount: warnings.labeledCount,
                operationalUnclearCount: warnings.operationalUnclearCount,
                semanticUnclearCount: warnings.semanticUnclearCount,
                totalUnclearRate: warnings.totalUnclearRate,
                threshold: warnings.threshold
            )
        )
    }

    private func recordCompleted(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        chapterCount: Int,
        planConfidence: Double,
        fmCallCount: Int,
        latencyMs: Double
    ) async {
        await eventSink.record(
            .completed(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp,
                chapterCount: chapterCount,
                planConfidence: planConfidence,
                fmCallCount: fmCallCount,
                latencyMs: latencyMs
            )
        )
    }
}

// MARK: - TaskGroup cancellation propagation helper

private extension ThrowingTaskGroup
where ChildTaskResult: Sendable, Failure == any Error {
    /// Wrapper around `next()` that surfaces parent-task cancellation
    /// at the loop boundary. Without this guard, if a child task has
    /// ALREADY completed and is sitting on the group's internal queue
    /// when the parent cancels, the surrounding `while let next =
    /// try await group.next()` loop will read that completed value
    /// without observing cancellation, then fall through to add a new
    /// task — which goes against the bead-12 contract that
    /// "cancellation discards partial state". We check
    /// `Task.isCancelled` explicitly before each pull so the loop
    /// short-circuits and the caller can throw `CancellationError`
    /// from a single, well-marked site.
    mutating func nextWithCancellationPropagation() async throws -> ChildTaskResult? {
        if Task.isCancelled {
            cancelAll()
            throw CancellationError()
        }
        return try await self.next()
    }
}
