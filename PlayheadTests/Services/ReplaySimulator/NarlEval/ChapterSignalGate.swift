// ChapterSignalGate.swift
// playhead-au2v.1.18: Replay-side counterfactual gate for the chapter-signal
// pipeline. Mirrors the `Q45fReplayGate` pattern (see
// `Q45fReplayGate.swift:15-110`): a value-type enum + pure replay function
// that runs over an existing `FrozenTrace` and produces a deterministic
// result struct, so the narl-eval harness can compute lift across modes:
//
//   off      — phase never runs; result is a structural zero. Detection
//              behavior must match today's narl-eval byte-for-byte.
//   shadow   — phase runs and emits telemetry, but consumers do NOT read
//              the plan. Used for plan-quality eval and FM-cost telemetry
//              without affecting detection. Detection equivalence to
//              `.off` is the same contract `ChapterSignalMode.shadow`
//              promises in production.
//   enabled  — phase runs AND consumers read.
//
// Why mirror Q45fReplayGate (not FMBackfillMode):
//   - Q45fReplayGate is the existing replay-side counterfactual pattern
//     in this directory: pure value type, no I/O, no production side
//     effects, returns a value struct that downstream beads (q45f.3)
//     diff across modes. au2v.1.19 / .20 / .21 build on the same shape
//     for chapter signal. The production-side mode field on
//     `AdDetectionConfig` (au2v.1.2) is intentionally `FMBackfillMode`-
//     shaped because that's the production gate idiom; this file is
//     the harness-side replay gate, so it borrows the harness-side
//     idiom from Q45fReplayGate.
//
// SCAFFOLDING NOTE (drop when bead 4 + bead 12/13 land):
//   `ChapterPlan`, `ChapterBoundaryDetector`, and `ChapterLabelingService`
//   are not yet on main. The gate currently uses a deterministic local
//   stub for boundary detection and labelling so it compiles, runs at all
//   three modes today, and produces stable outputs for the harness wiring
//   (au2v.1.19+) to consume. When the real services land, swap the stub
//   in `runShadowOrEnabled(...)` for the real call sites — the result
//   struct shape is intentionally aligned with the bead spec so callers
//   do NOT need to migrate.

import Foundation
@testable import Playhead

// MARK: - ChapterSignalGate

enum ChapterSignalGate {

    // MARK: - Result types

    /// Per-episode outcome of one replay invocation. Carries enough state
    /// to drive case-study extraction (bead 20) without re-walking the
    /// trace, plus enough numeric counters that aggregate metrics
    /// (bead 19) can sum across episodes without inspecting any
    /// non-additive field.
    struct EpisodeOutcome: Equatable, Sendable {
        let episodeId: String
        let podcastId: String
        let mode: ChapterSignalMode
        /// `true` iff the phase ran and emitted a plan in this mode.
        /// Always `false` when `mode == .off`.
        let planGenerated: Bool
        /// `true` iff the phase short-circuited because the trace already
        /// carries creator-supplied chapters (id3 / pc20 / rssInline).
        let skippedByCreatorChapters: Bool
        /// `true` iff `ChapterPhaseEventType.operationalUnclearRateExceeded`
        /// would have fired given the trace's labelling outcomes. The
        /// stub never trips this; reserved for the real labelling wiring.
        let abortedByOperationalRate: Bool
        /// `true` iff `ChapterPhaseEventType.pathologicalRate` would have
        /// fired given the trace's atom density. The stub never trips
        /// this; reserved for the real boundary-detector wiring.
        let abortedByPathologicalRate: Bool
        /// Number of FM calls the labelling phase consumed for this
        /// episode. Always `0` when the phase was skipped or aborted, and
        /// always `0` when `mode == .off`.
        let fmCallsForChapterLabeling: Int
        /// Synthetic phase latency in milliseconds. Computed from a
        /// deterministic per-call cost model so tests are reproducible
        /// across runs and toolchains. Always `0` when `mode == .off`.
        let phaseLatencyMs: Double
    }

    /// Aggregate result of one full replay (single mode, one or more
    /// episodes). All counters are sums of the corresponding
    /// `EpisodeOutcome` flags / counters; latency is summed in ms.
    /// Comparing two `ChapterSignalReplayResult`s across modes is the
    /// lift-quantification primitive bead 19 builds on.
    struct ChapterSignalReplayResult: Equatable, Sendable {
        let mode: ChapterSignalMode
        let episodesProcessed: Int
        let planGeneratedCount: Int
        let planAbortedByOperationalRate: Int
        let planAbortedByPathologicalRate: Int
        let skippedByCreatorChapters: Int
        let totalFMCallsForChapterLabeling: Int
        let aggregateLatencyMs: Double
        let perEpisodeOutcomes: [EpisodeOutcome]

        /// Structural zero result for `mode == .off`. Carries one
        /// `EpisodeOutcome` per input episode with every numeric field
        /// at 0 / every flag at `false`. Returned BYTE-FOR-BYTE
        /// identically across runs given the same input traces — this is
        /// the contract the harness depends on for "off baseline matches
        /// today's narl-eval behavior".
        // Visibility: this is `fileprivate` not `private` because the
        // call site is in the outer enum's `replay(traces:mode:)` —
        // `private` would scope to the enclosing struct's body and
        // block the outer call. `fileprivate` is the tightest the
        // language allows here, and it confines new call sites to
        // this file.
        fileprivate static func offResult(traces: [FrozenTrace]) -> ChapterSignalReplayResult {
            let outcomes = traces.map { trace in
                EpisodeOutcome(
                    episodeId: trace.episodeId,
                    podcastId: trace.podcastId,
                    mode: .off,
                    planGenerated: false,
                    skippedByCreatorChapters: false,
                    abortedByOperationalRate: false,
                    abortedByPathologicalRate: false,
                    fmCallsForChapterLabeling: 0,
                    phaseLatencyMs: 0.0
                )
            }
            return ChapterSignalReplayResult(
                mode: .off,
                episodesProcessed: traces.count,
                planGeneratedCount: 0,
                planAbortedByOperationalRate: 0,
                planAbortedByPathologicalRate: 0,
                skippedByCreatorChapters: 0,
                totalFMCallsForChapterLabeling: 0,
                aggregateLatencyMs: 0.0,
                perEpisodeOutcomes: outcomes
            )
        }
    }

    // MARK: - Config

    /// Knobs for the deterministic replay model. All values are FROZEN
    /// for replay reproducibility: changing any of them is a wire-
    /// breaking change that must bump fixture digests in bead 19.
    ///
    /// `creatorChaptersPresent` is a closure rather than a per-trace
    /// bool because `FrozenTrace` v3 does not yet carry a creator-
    /// chapter signal — bead 4 will add one and the closure will be
    /// retired in favour of reading it directly. Defaulting to
    /// `{ _ in false }` keeps the gate compile-and-run-clean today.
    struct Config: Sendable {
        /// Synthetic per-FM-call cost in milliseconds, used to compute
        /// `EpisodeOutcome.phaseLatencyMs`. Pinned to `25.0` so test
        /// arithmetic is hand-checkable (4 calls = 100 ms).
        let syntheticFMCallLatencyMs: Double
        /// Flat per-episode admission/decision overhead in ms, applied
        /// once when the phase actually runs.
        let perEpisodeOverheadMs: Double
        /// Number of synthesized boundary candidates per episode in the
        /// stub. Bead 4 replaces this with a real
        /// `ChapterBoundaryDetector` call; the stub is deterministic so
        /// `mode=.shadow` and `mode=.enabled` can produce stable counts
        /// today.
        let stubChapterCount: @Sendable (FrozenTrace) -> Int
        /// Caller-supplied creator-chapter detector. Returns `true` iff
        /// the trace's source episode has `ChapterEvidence` from id3,
        /// pc20, or rssInline. When `true`, the phase short-circuits
        /// without invoking FM.
        let creatorChaptersPresent: @Sendable (FrozenTrace) -> Bool

        init(
            syntheticFMCallLatencyMs: Double = 25.0,
            perEpisodeOverheadMs: Double = 5.0,
            stubChapterCount: @Sendable @escaping (FrozenTrace) -> Int = Self.defaultStubChapterCount,
            creatorChaptersPresent: @Sendable @escaping (FrozenTrace) -> Bool = { _ in false }
        ) {
            self.syntheticFMCallLatencyMs = syntheticFMCallLatencyMs
            self.perEpisodeOverheadMs = perEpisodeOverheadMs
            self.stubChapterCount = stubChapterCount
            self.creatorChaptersPresent = creatorChaptersPresent
        }

        /// Default stub chapter count: clamp(#atoms / 50, 1, 12). Picked
        /// to land in the 4–12 range the design doc cites for typical
        /// 60-min episodes (atoms count ≈ 200–600 in real fixtures), and
        /// to never return zero (the stub's job is to produce a non-
        /// degenerate "would have run" signal so the harness can
        /// validate the plumbing). The lower clamp at 1 holds for empty
        /// traces too, by design — an empty trace at the default config
        /// still produces `planGenerated == true`. Callers who want to
        /// model the production `chapter_phase_no_candidates` event must
        /// supply a custom `stubChapterCount` that returns 0; see
        /// `zeroStubChapterCountProducesNoPlan` in the gate tests.
        static func defaultStubChapterCount(_ trace: FrozenTrace) -> Int {
            let raw = trace.atoms.count / 50
            return min(12, max(1, raw))
        }

        static let `default` = Config()
    }

    // MARK: - Replay (single mode)

    /// Replay a frozen trace under one `ChapterSignalMode`.
    ///
    /// Preconditions: none beyond `FrozenTrace` validity.
    ///
    /// In `.shadow`/`.enabled`:
    /// - The default `Config.stubChapterCount` returns at least 1 for
    ///   any trace (clamped to `[1, 12]`), so empty atom arrays still
    ///   yield `planGenerated == true`.
    /// - A custom `stubChapterCount` may legitimately return `0`,
    ///   modelling the production `chapter_phase_no_candidates` event:
    ///   the phase ran, the boundary detector produced no candidates,
    ///   and no plan is emitted. In that case `planGenerated == false`
    ///   and `fmCallsForChapterLabeling == 0`, but the per-episode
    ///   overhead latency is still charged (the boundary detector ran).
    ///
    /// `mode == .off` is a fast path that does NOT inspect trace
    /// contents beyond `episodeId` / `podcastId` and produces the
    /// structural-zero result documented on `offResult(traces:)`. This is
    /// the source of the "byte-for-byte match" guarantee.
    static func replay(
        trace: FrozenTrace,
        mode: ChapterSignalMode,
        config: Config = .default
    ) -> ChapterSignalReplayResult {
        replay(traces: [trace], mode: mode, config: config)
    }

    /// Replay an array of frozen traces under one mode. Sums per-episode
    /// outcomes into the aggregate counters on the result.
    static func replay(
        traces: [FrozenTrace],
        mode: ChapterSignalMode,
        config: Config = .default
    ) -> ChapterSignalReplayResult {
        if mode == .off {
            return .offResult(traces: traces)
        }

        var outcomes: [EpisodeOutcome] = []
        outcomes.reserveCapacity(traces.count)

        for trace in traces {
            outcomes.append(runShadowOrEnabled(trace: trace, mode: mode, config: config))
        }

        var planGenerated = 0
        var skippedCreator = 0
        var abortedOp = 0
        var abortedPath = 0
        var totalFM = 0
        var totalLatency: Double = 0
        for outcome in outcomes {
            if outcome.planGenerated {
                planGenerated += 1
            }
            if outcome.skippedByCreatorChapters {
                skippedCreator += 1
            }
            if outcome.abortedByOperationalRate {
                abortedOp += 1
            }
            if outcome.abortedByPathologicalRate {
                abortedPath += 1
            }
            totalFM += outcome.fmCallsForChapterLabeling
            totalLatency += outcome.phaseLatencyMs
        }

        return ChapterSignalReplayResult(
            mode: mode,
            episodesProcessed: traces.count,
            planGeneratedCount: planGenerated,
            planAbortedByOperationalRate: abortedOp,
            planAbortedByPathologicalRate: abortedPath,
            skippedByCreatorChapters: skippedCreator,
            totalFMCallsForChapterLabeling: totalFM,
            aggregateLatencyMs: totalLatency,
            perEpisodeOutcomes: outcomes
        )
    }

    // MARK: - Replay (mode comparison)

    /// Replay one trace across multiple modes; returns a dictionary
    /// keyed by mode. This is the primitive bead 19 builds aggregate
    /// metrics on (off-vs-shadow, shadow-vs-enabled diffs).
    ///
    /// Order of `modes` does not affect output values because each mode
    /// is replayed independently against the input trace; the gate has
    /// no carry-forward state across modes (unlike Q45fReplayGate's
    /// per-podcast trust threading, which mutates state across
    /// episodes).
    ///
    /// Duplicates in `modes` are deduplicated before iteration so each
    /// mode is replayed exactly once. Result-equality across duplicates
    /// is also a property the gate provides (given pure caller
    /// closures), but deduplication makes the cost behavior obvious
    /// rather than requiring callers to argue from "every replay for
    /// the same mode is observationally identical".
    static func replay(
        trace: FrozenTrace,
        modes: [ChapterSignalMode],
        config: Config = .default
    ) -> [ChapterSignalMode: ChapterSignalReplayResult] {
        replay(traces: [trace], modes: modes, config: config)
    }

    /// Multi-trace, multi-mode replay. Same shape as the single-trace
    /// variant; each mode aggregates across all traces.
    ///
    /// Determinism: for any pair of replays with the same input
    /// `traces`, `modes`, and `config`, the returned dictionary is
    /// `Equatable`-equal — provided the caller-supplied closures
    /// (`config.stubChapterCount`, `config.creatorChaptersPresent`)
    /// are themselves deterministic. Tests of the gate use pure
    /// closures and pin the property; production wiring (bead 19)
    /// must do the same.
    static func replay(
        traces: [FrozenTrace],
        modes: [ChapterSignalMode],
        config: Config = .default
    ) -> [ChapterSignalMode: ChapterSignalReplayResult] {
        // Deduplicate while preserving first-seen order. We don't use
        // `Set(modes)` directly because Set iteration order is
        // unspecified — and although the result is a dictionary
        // (so iteration order at the call site is also unspecified),
        // doing the work in deterministic order keeps replay logs
        // (e.g., printf-style telemetry someone might add later)
        // stable across runs.
        var seen: Set<ChapterSignalMode> = []
        var unique: [ChapterSignalMode] = []
        unique.reserveCapacity(modes.count)
        for mode in modes where seen.insert(mode).inserted {
            unique.append(mode)
        }

        var results: [ChapterSignalMode: ChapterSignalReplayResult] = [:]
        results.reserveCapacity(unique.count)
        for mode in unique {
            results[mode] = replay(traces: traces, mode: mode, config: config)
        }
        return results
    }

    // MARK: - Internal

    /// Defensive upper bound on caller-supplied `stubChapterCount`
    /// outputs. Set far above any plausible per-episode chapter count
    /// so well-behaved callers never hit it. See `runShadowOrEnabled`
    /// for the rationale; this constant is exposed at the enum's
    /// internal scope so tests can pin the clamp value without
    /// reaching into the private `runShadowOrEnabled` body.
    static let maxStubChapterCount: Int = 10_000

    /// Core stub for any non-`.off` mode. Encapsulates the synthetic
    /// chapter-generation pipeline so the public API stays narrow.
    /// When bead 4 / 12 / 13 land, this is the only function that
    /// changes — the result-struct shape is the public contract.
    ///
    /// New-mode default: if a future `ChapterSignalMode` case is added
    /// (e.g., a `.canary` or `.experimental` mode), the public
    /// `replay(traces:mode:)` will route it here unchanged because the
    /// fast path is keyed on `mode == .off`. That is intentional — the
    /// gate's job is to "run the phase or not", and every non-off mode
    /// runs the phase by definition. Mode-specific consumer behavior
    /// (e.g., whether to read the plan) lives in
    /// `ChapterSignalMode.consumersReadChapterPlan` (au2v.1.2 / bead 14
    /// / bead 16), NOT in this gate. If you add a non-off mode that
    /// SHOULD NOT run the phase, the gate's contract has shifted — you
    /// must update the fast-path predicate and the doc comment on
    /// `ChapterSignalReplayResult.offResult` together.
    private static func runShadowOrEnabled(
        trace: FrozenTrace,
        mode: ChapterSignalMode,
        config: Config
    ) -> EpisodeOutcome {
        precondition(
            mode != .off,
            "runShadowOrEnabled must not be called for mode=.off; the .off fast path is the public-API guarantee."
        )

        // Creator-chapter precedence: design doc §Reconciliation
        // (2026-05-06) — "when an episode already has any
        // ChapterEvidence with source ∈ {id3, pc20, rssInline},
        // ChapterGenerationPhase exits early without invoking FM."
        //
        // Latency = 0.0 here is a deliberate asymmetry vs the
        // no-candidates path (which charges `perEpisodeOverheadMs`).
        // Rationale: the creator-chapter check is essentially a cache
        // / metadata lookup that can fire BEFORE any phase setup work,
        // so the "phase ran enough to write the skip event" cost is
        // microsecond-class and modeled as zero in the synthetic
        // latency model. The no-candidates path, by contrast, requires
        // running the full boundary-detector pipeline before
        // discovering there are no candidates — so it charges the
        // per-episode overhead. Bead 19's cost-lift metrics depend on
        // this asymmetry being explicit.
        if config.creatorChaptersPresent(trace) {
            return EpisodeOutcome(
                episodeId: trace.episodeId,
                podcastId: trace.podcastId,
                mode: mode,
                planGenerated: false,
                skippedByCreatorChapters: true,
                abortedByOperationalRate: false,
                abortedByPathologicalRate: false,
                fmCallsForChapterLabeling: 0,
                phaseLatencyMs: 0.0
            )
        }

        let chapterCount = config.stubChapterCount(trace)
        // Defensive clamp on caller-supplied output:
        //   - Lower bound 0: a buggy custom closure cannot drive
        //     negative FM cost into the aggregate.
        //   - Upper bound `maxStubChapterCount`: a buggy custom closure
        //     returning `Int.max` cannot overflow the per-episode latency
        //     (`Double(safeCount) * syntheticFMCallLatencyMs` → ∞) or the
        //     aggregate `totalFMCallsForChapterLabeling` Int counter.
        //     The cap is set far above any plausible per-episode chapter
        //     count (the default stub clamps at 12; real production
        //     boundary detectors emit on the order of single-digits to
        //     tens), so it is invisible to well-behaved callers and
        //     only catches bugs.
        let safeCount = min(Self.maxStubChapterCount, max(0, chapterCount))

        // When boundary detection returns 0 candidates, production
        // emits `chapter_phase_no_candidates` and writes NO plan (see
        // ChapterPhaseDiagnostics.swift `noCandidates` event). Mirror
        // that contract here so `planGenerated` truthfully reflects
        // "a plan was emitted", not just "the phase ran". Latency is
        // still charged at the per-episode-overhead rate because the
        // boundary detector ran (and would have computed candidates
        // even if it produced none).
        let planEmitted = safeCount > 0
        let latency = planEmitted
            ? config.perEpisodeOverheadMs + Double(safeCount) * config.syntheticFMCallLatencyMs
            : config.perEpisodeOverheadMs

        return EpisodeOutcome(
            episodeId: trace.episodeId,
            podcastId: trace.podcastId,
            mode: mode,
            planGenerated: planEmitted,
            skippedByCreatorChapters: false,
            abortedByOperationalRate: false,
            abortedByPathologicalRate: false,
            fmCallsForChapterLabeling: safeCount,
            phaseLatencyMs: latency
        )
    }
}
