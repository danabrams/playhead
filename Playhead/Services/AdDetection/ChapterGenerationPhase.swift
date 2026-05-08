// ChapterGenerationPhase.swift
// playhead-au2v.1.10: Shell of the chapter-generation phase.
//
// This file is the *skeleton* that ties together the (future) boundary
// detector (playhead-au2v.1.4 / .5) and the (future) chapter labeler
// (playhead-au2v.1.7 / .8). The shell owns:
//
//   1. ChapterSignalMode gate (`.off` → no work, no diagnostic).
//   2. Admission control (delegates to an injected
//      `ChapterPhaseAdmissionPolicy`; matches the spirit of the
//      `CapabilitySnapshot.canUseFoundationModels` gate used by
//      `FoundationModelExtractor` — the policy here is a thin DI
//      seam so the phase can be tested without spinning up the live
//      capability service).
//   3. Transcript-snapshot race protection: a content hash is captured
//      at phase entry, then re-fetched immediately before the cache
//      write. A mismatch aborts the run, discards the plan, and emits
//      a `chapter_phase_preempted` diagnostic (the same event used by
//      explicit cancellation — both express "the input we built this
//      plan against is no longer current"; the inline comment at the
//      recheck-mismatch branch in `run()` explains why we collapse
//      cancellation and recheck-mismatch onto the same wire event).
//   4. Cooperative cancellation honoring task cancellation. We use
//      `Task.isCancelled` checks at every yield point (rather than
//      `try Task.checkCancellation()`) so the shell can collapse a
//      cancellation into the structured `Outcome.preempted` return —
//      keeping the call site exhaustive over the `Outcome` enum
//      without forcing callers into a `do/catch CancellationError`.
//      Throws of `CancellationError` from the detector or labeler are
//      ALSO honored (caught and routed through the same `preempt`
//      helper) — the `FoundationModelClassifier` pattern of throwing
//      cancellation is still respected at the seam.
//   5. Cache write into `ChapterPlanCache` (bead .1) on success, with
//      a `chapter_phase_completed` diagnostic.
//
// Out of scope (later beads):
//   * Real boundary detection (`.4` + `.5`), real FM labeling (`.7` +
//     `.8`), creator-chapter short-circuit (`.11`), per-chapter
//     parallelism (`.12`), wiring into `AdDetectionService`'s backfill
//     path (`.13`), end-to-end integration tests (`.13`).
//   * Per-call retry / op-vs-semantic failure classification — those
//     emit `chapter_phase_label_failed`, which is the labeling
//     service's responsibility (`.7` / `.8`).
//
// Logging discipline:
//   * Every exit path EXCEPT the `.off` short-circuit emits exactly
//     one phase-completion diagnostic (success or specific failure).
//     "Phase-completion" here means terminal events:
//     `.skippedAdmission`, `.noCandidates`, `.preempted`,
//     `.completed`. There is never more than one of those per run.
//     Note that `transcriptUnavailable` exits early but DOES emit
//     `.noCandidates` — we still want telemetry on "phase admitted
//     but had no transcript" for dogfood mis-scheduling debugging.
//   * One `.started` event fires at phase entry (after the entry
//     transcript-hash snapshot succeeds, since the started payload
//     requires that hash). Paths that exit *before* the snapshot
//     succeeds (`.off` / admission deny / transcript unavailable)
//     do NOT emit `.started` — those exits are "phase never truly
//     began" and the bead .3 diagnostic schema agrees with that
//     reading.
//   * `ChapterSignalMode.off` emits NO diagnostic at all — the
//     feature is fully off, and we want zero surface area in shipped
//     bundles when the flag is off.

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
///
/// The seam returns `[ChapterBoundaryCandidate]` (a thin local struct
/// owned by this shell) rather than `[ChapterEvidence]` so the shell
/// stays decoupled from the labeler's output type — the labeler's job
/// is to turn each candidate into a labeled `ChapterEvidence`.
protocol ChapterBoundaryDetecting: Sendable {
    func detect() async throws -> [ChapterBoundaryCandidate]
}

/// Labeling seam. Bead .7 + .8 provide the real FM-backed
/// implementation. The shell calls this serially per candidate;
/// per-chapter parallelism is bead .12's job and stays intentionally
/// out of scope here.
///
/// `nil` return is allowed: a candidate the labeler cannot classify
/// is skipped silently from this shell's perspective. Real label
/// failures (operational vs semantic) emit
/// `chapter_phase_label_failed` from inside the labeling service —
/// not from the shell — so callers control retry/failure vocabulary.
protocol ChapterLabeling: Sendable {
    func label(
        candidate: ChapterBoundaryCandidate
    ) async throws -> ChapterEvidence?
}

/// Source of the "current transcript content hash". The shell calls
/// this twice per run: once on entry (snapshot), once before the
/// cache write (race re-check). `nil` indicates the transcript is
/// not yet available; the shell treats `nil` on entry as
/// `Outcome.transcriptUnavailable` (emits the bead .3 `.noCandidates`
/// event, since that's the catch-all "ran but produced nothing"
/// signal in the wire vocabulary) and `nil` on the re-check as a
/// mismatch (`Outcome.raceAborted` with a `.preempted` event — the
/// input went away under us).
protocol TranscriptHashProviding: Sendable {
    func currentTranscriptHash() async -> String?
}

/// Sink for `ChapterPhaseEvent`s. Production wiring (bead .13 / a
/// later persistence bead) will route these into the diagnostics
/// store; tests inject an in-memory recorder.
///
/// Implementations must be safe to call from any task/actor context.
/// The shell awaits each `record` so emit ordering matches phase
/// progress; sinks are free to enqueue and persist asynchronously.
protocol ChapterPhaseEventSink: Sendable {
    func record(_ event: ChapterPhaseEvent) async
}

/// Lightweight candidate record for a single boundary the detector
/// proposes. Owned by the phase shell so detector and labeler can be
/// developed against a stable contract.
///
/// The shell does not consume any field: `startTime` is the only
/// field with semantic meaning to a labeler in the current contract,
/// and even that is opaque to this file. `endTime` is `nil` for the
/// last candidate (matches `ChapterEvidence.endTime`'s open-ended
/// semantics).
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
/// plain `struct` whose dependencies are all `Sendable`. Cache writes
/// hop onto `ChapterPlanCache`'s actor automatically. Per-chapter
/// parallelism is deferred to bead .12.
struct ChapterGenerationPhase: Sendable {

    // MARK: Dependencies

    private let admissionPolicy: ChapterPhaseAdmissionPolicy
    private let boundaryDetector: ChapterBoundaryDetecting
    private let labeler: ChapterLabeling
    private let transcriptHashProvider: TranscriptHashProviding
    private let cache: ChapterPlanCache
    private let eventSink: ChapterPhaseEventSink
    private let clock: @Sendable () -> Date
    private let logger: Logger

    // MARK: Init

    init(
        admissionPolicy: ChapterPhaseAdmissionPolicy,
        boundaryDetector: ChapterBoundaryDetecting,
        labeler: ChapterLabeling,
        transcriptHashProvider: TranscriptHashProviding,
        cache: ChapterPlanCache,
        eventSink: ChapterPhaseEventSink,
        clock: @escaping @Sendable () -> Date = { Date() },
        logger: Logger = Logger(subsystem: "com.playhead", category: "ChapterGenerationPhase")
    ) {
        self.admissionPolicy = admissionPolicy
        self.boundaryDetector = boundaryDetector
        self.labeler = labeler
        self.transcriptHashProvider = transcriptHashProvider
        self.cache = cache
        self.eventSink = eventSink
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
        case noCandidates
        case transcriptUnavailable
        case raceAborted
        case preempted
        case cached(chapterCount: Int, planConfidence: Double)
    }

    /// Run the phase end-to-end.
    ///
    /// - Parameters:
    ///   - mode: Tri-state gate from `AdDetectionConfig.chapterSignalMode`.
    ///     `.off` causes an immediate, silent no-op.
    ///   - episodeId: Raw episode identifier; hashed with `installID`
    ///     before any diagnostic ships.
    ///   - installID: Per-install salt for the episode-id hash. Same
    ///     value used by `scheduler_events`.
    /// - Returns: An `Outcome` describing the terminal exit path.
    func run(
        mode: ChapterSignalMode,
        episodeId: String,
        installID: UUID
    ) async -> Outcome {
        // 1. Mode gate — `.off` is the *only* path that emits no
        //    diagnostic. The feature is fully off; no surface area.
        guard mode.runsChapterGeneration else {
            return .modeOff
        }

        let startedAtTimestamp = clock().timeIntervalSince1970

        // 2. Admission. We check admission BEFORE the entry-hash
        //    snapshot so a denied phase never even reads the
        //    transcript cache — matches the "never burn FM cost on a
        //    denied phase" intent.
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

        // Cancellation can fire at any await; honor it after every
        // yield point. Each `preempt` call below collapses to
        // `recordPreempted` + `return .preempted`.
        if Task.isCancelled {
            return await preempt(installID: installID, episodeId: episodeId)
        }

        // 3. Transcript snapshot capture. A `nil` here means the
        //    transcript pipeline has not produced anything to hash —
        //    typically the phase was invoked too early (an
        //    orchestrator scheduling bug, not the shell's fault).
        //    We emit `.noCandidates` so dogfood telemetry can surface
        //    mis-scheduled invocations (an "admitted, no transcript,
        //    zero bytes cached" run should be visible) and we exit
        //    with `Outcome.transcriptUnavailable` so in-process
        //    callers can distinguish this from a real "ran, found
        //    nothing" result.
        guard let entryHash = await transcriptHashProvider.currentTranscriptHash() else {
            // The bead .3 wire has no dedicated "transcript unavailable"
            // event; `.noCandidates` is the catch-all for "phase ran,
            // produced nothing" and the supplementary phase log
            // (below) carries the distinction for engineers who need it.
            logger.notice(
                "chapterphase.transcript_unavailable_at_entry — emitting no_candidates"
            )
            await emitNoCandidates(installID: installID, episodeId: episodeId)
            return .transcriptUnavailable
        }

        // The phase has now "truly begun": admission passed AND we
        // have a transcript snapshot to anchor the run. Emit the
        // single `.started` lifecycle event — the bead .3 schema
        // requires this be paired with exactly one terminal event
        // (`.skippedAdmission` / `.noCandidates` / `.preempted` /
        // `.completed`) per run from this point onward.
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

        // 4. Boundary detection. The detector itself may throw
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

        // 5. Serial labeling pass. Per-call parallelism is bead .12.
        //    A `nil` from the labeler is silently dropped; a non-
        //    cancellation throw is logged and skips that candidate
        //    (the labeling service owns `chapter_phase_label_failed`
        //    vocabulary, not the shell). A `CancellationError`
        //    throw collapses the whole run into a preempt — we
        //    discard partial state rather than persist a half-
        //    labeled plan.
        var labeled: [ChapterEvidence] = []
        labeled.reserveCapacity(candidates.count)
        var fmCallCount = 0
        for candidate in candidates {
            if Task.isCancelled {
                return await preempt(installID: installID, episodeId: episodeId)
            }
            do {
                let evidenceOrNil = try await labeler.label(candidate: candidate)
                // Only count calls that returned (success or nil)
                // — throws (cancellation or labeler failures) are
                // tracked through their own paths and would muddle
                // the "FM cost incurred for this completed plan"
                // semantic of `completed.fm_call_count`.
                fmCallCount += 1
                if let evidence = evidenceOrNil {
                    labeled.append(evidence)
                }
            } catch is CancellationError {
                return await preempt(installID: installID, episodeId: episodeId)
            } catch {
                // Per-call failure: log and keep going. The labeling
                // service is the proper emitter of
                // `chapter_phase_label_failed`; the shell's job is
                // to keep moving and avoid losing other chapters.
                logger.error(
                    "chapterphase.labeling_failed: \(error.localizedDescription, privacy: .public)"
                )
                continue
            }
        }

        if Task.isCancelled {
            return await preempt(installID: installID, episodeId: episodeId)
        }

        // 6. Race re-check. We re-fetch the transcript hash and
        //    compare it to the snapshot taken in step 3. If the
        //    transcript changed under us (re-transcription, edit,
        //    user re-imported), discard the plan; do NOT cache. We
        //    emit `preempted` rather than inventing a new event
        //    type — both this case and explicit cancellation share
        //    the same actionable shape ("the input we computed
        //    against is no longer current; nothing was persisted").
        let recheckHash = await transcriptHashProvider.currentTranscriptHash()
        guard let recheckHash, recheckHash == entryHash else {
            logger.notice(
                "chapterphase.transcript_changed_during_run entry=\(entryHash, privacy: .public) recheck=\(recheckHash ?? "nil", privacy: .public)"
            )
            // Same `.preempted` event as explicit cancellation, but
            // the in-process Outcome is `.raceAborted` so callers can
            // distinguish "the user cancelled us" from "the input
            // changed under us" without parsing the OS log.
            await recordPreempted(
                installID: installID,
                episodeId: episodeId,
                timestamp: clock().timeIntervalSince1970
            )
            return .raceAborted
        }

        // 7. Build the plan and persist.
        let planConfidence = ChapterPlan.computePlanConfidence(labeled)
        let plan = ChapterPlan(
            episodeContentHash: entryHash,
            chapters: labeled,
            planConfidence: planConfidence,
            generatedAt: clock(),
            generationDiagnostics: ChapterPlanDiagnostics(
                candidatesDetected: candidates.count,
                candidatesKept: labeled.count
            )
        )
        // Cache write returns false on bad input; we still emit
        // `completed` because the run produced a valid plan —
        // persistence health is logged, not surfaced as a phase
        // outcome here (the support engineer queries
        // `chapterplan.cache.write_failed` for that signal).
        _ = await cache.put(contentHash: entryHash, plan: plan)

        let completedAt = clock()
        let latencyMs =
            (completedAt.timeIntervalSince1970 - startedAtTimestamp) * 1_000.0
        await recordCompleted(
            installID: installID,
            episodeId: episodeId,
            timestamp: completedAt.timeIntervalSince1970,
            chapterCount: labeled.count,
            planConfidence: planConfidence,
            fmCallCount: fmCallCount,
            latencyMs: latencyMs
        )

        return .cached(
            chapterCount: labeled.count,
            planConfidence: planConfidence
        )
    }

    // MARK: - Cancellation / no-candidates collapse helpers

    /// Records a `.preempted` event with a fresh timestamp and returns
    /// the matching `.preempted` `Outcome`. Used by every site in
    /// `run()` that observes cancellation — both explicit
    /// `Task.isCancelled` checks AND `catch is CancellationError`
    /// branches — so the timestamp/emit pattern lives in one place.
    /// The recheck-mismatch branch does NOT use this helper because
    /// its outcome is `.raceAborted`, not `.preempted`.
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

    /// Records a `.noCandidates` event with a fresh timestamp. The
    /// caller still chooses the matching `Outcome` (either
    /// `.noCandidates` or `.transcriptUnavailable`) — this helper
    /// owns the diagnostic emit only.
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
