// ChapterSignalGateTests.swift
// playhead-au2v.1.18: TDD pin for the replay-side ChapterSignalGate.
//
// Mirrors `Q45fReplayGateTests.swift` — the gate is a pure value-type
// enum + replay function with three modes (off / shadow / enabled), so
// every assertion below is structural:
//
//   * mode=.off must be a structural zero, byte-for-byte identical
//     across runs given the same input traces. This is the contract the
//     narl-eval harness depends on for "off baseline matches today's
//     narl-eval behavior."
//   * mode=.shadow and mode=.enabled produce identical phase-side
//     outcomes given identical inputs (the gate models the production
//     contract that "detection behavior must be byte-for-byte identical
//     to .off in shadow mode" by emitting the same plan in both
//     non-off modes; the consumer-side difference lives in
//     `ChapterSignalMode.consumersReadChapterPlan`, which the gate
//     itself does not exercise — that's bead 14 / 16 / 19's territory).
//   * Aggregate counters must equal the sum of per-episode flags so
//     bead 19 can build precision/recall/F1 by summing over results.
//   * Determinism: replay(...) twice on the same input == identical
//     output. This is the test that pins "byte-for-byte parity" for
//     mode=.off as a property, not just a comment in the source.

import Foundation
import Testing
@testable import Playhead

@Suite("ChapterSignalGate")
struct ChapterSignalGateTests {

    // MARK: - Fixture helpers

    private static func makeTrace(
        episodeId: String = "ep-au2v-test",
        podcastId: String = "pod-au2v-test",
        atomCount: Int = 250
    ) -> FrozenTrace {
        let atoms: [FrozenTrace.FrozenAtom] = (0..<atomCount).map { i in
            FrozenTrace.FrozenAtom(
                startTime: Double(i) * 2.0,
                endTime: Double(i) * 2.0 + 2.0,
                text: "atom-\(i)"
            )
        }
        return FrozenTrace(
            episodeId: episodeId,
            podcastId: podcastId,
            episodeDuration: Double(atomCount) * 2.0,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000),
            featureWindows: [],
            atoms: atoms,
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training
        )
    }

    // MARK: - mode=.off contract

    @Test(".off result has zero plans, zero FM calls, zero latency, zero aborts")
    func offIsStructuralZero() {
        let trace = Self.makeTrace()
        let result = ChapterSignalGate.replay(trace: trace, mode: .off)

        #expect(result.mode == .off)
        #expect(result.episodesProcessed == 1)
        #expect(result.planGeneratedCount == 0)
        #expect(result.planAbortedByOperationalRate == 0)
        #expect(result.planAbortedByPathologicalRate == 0)
        #expect(result.skippedByCreatorChapters == 0)
        #expect(result.totalFMCallsForChapterLabeling == 0)
        #expect(result.aggregateLatencyMs == 0.0)
        #expect(result.perEpisodeOutcomes.count == 1)

        let outcome = result.perEpisodeOutcomes[0]
        #expect(outcome.mode == .off)
        #expect(outcome.episodeId == trace.episodeId)
        #expect(outcome.podcastId == trace.podcastId)
        #expect(!outcome.planGenerated)
        #expect(!outcome.skippedByCreatorChapters)
        #expect(!outcome.abortedByOperationalRate)
        #expect(!outcome.abortedByPathologicalRate)
        #expect(outcome.fmCallsForChapterLabeling == 0)
        #expect(outcome.phaseLatencyMs == 0.0)
    }

    @Test(".off result is byte-for-byte deterministic across runs (identical input → Equatable equal output)")
    func offDeterminism() {
        let trace = Self.makeTrace()
        let r1 = ChapterSignalGate.replay(trace: trace, mode: .off)
        let r2 = ChapterSignalGate.replay(trace: trace, mode: .off)
        #expect(r1 == r2,
                "mode=.off must be byte-for-byte identical across replays — this is the harness baseline-match contract.")
    }

    @Test(".off ignores trace BODY (atoms, evidence) and caller-supplied closures — only episode/podcast id reach the output")
    func offIgnoresTraceContents() {
        let trace = Self.makeTrace(atomCount: 1)
        // Even with a config that *would* fire the creator-chapters
        // branch and a stub-count override that would multiply FM cost,
        // mode=.off must still return the structural zero. Note: `.off`
        // does read `trace.episodeId` and `trace.podcastId` for outcome
        // identification — the byte-for-byte contract is that no
        // CONTENT-bearing trace field (atoms, featureWindows, evidence,
        // corrections, decisionEvents) and no CALLER closure
        // (stubChapterCount, creatorChaptersPresent) is consulted in
        // `.off`. The closure-non-invocation half of that contract is
        // pinned by `offDoesNotInvokeCallerClosures`; this test pins the
        // counter side: nothing the caller supplies in `Config` can
        // bleed into the `.off` output values.
        let weirdConfig = ChapterSignalGate.Config(
            syntheticFMCallLatencyMs: 999.0,
            perEpisodeOverheadMs: 999.0,
            stubChapterCount: { _ in 999 },
            creatorChaptersPresent: { _ in true }
        )
        let result = ChapterSignalGate.replay(trace: trace, mode: .off, config: weirdConfig)
        #expect(result.totalFMCallsForChapterLabeling == 0)
        #expect(result.aggregateLatencyMs == 0.0)
        #expect(result.skippedByCreatorChapters == 0)
        #expect(result.planGeneratedCount == 0)
    }

    // MARK: - mode=.shadow / .enabled produce a generated plan

    @Test(".shadow on a vanilla trace generates exactly one plan with deterministic FM-call count")
    func shadowGeneratesOnePlan() {
        // 250 atoms / 50 = 5 chapters → 5 FM calls.
        let trace = Self.makeTrace(atomCount: 250)
        let result = ChapterSignalGate.replay(trace: trace, mode: .shadow)

        #expect(result.mode == .shadow)
        #expect(result.episodesProcessed == 1)
        #expect(result.planGeneratedCount == 1)
        #expect(result.totalFMCallsForChapterLabeling == 5)
        #expect(result.skippedByCreatorChapters == 0)
        #expect(result.planAbortedByOperationalRate == 0)
        #expect(result.planAbortedByPathologicalRate == 0)
        // Latency = perEpisodeOverhead (5) + 5 * 25 = 130
        #expect(result.aggregateLatencyMs == 130.0)

        let outcome = result.perEpisodeOutcomes[0]
        #expect(outcome.planGenerated)
        #expect(outcome.fmCallsForChapterLabeling == 5)
        #expect(outcome.phaseLatencyMs == 130.0)
    }

    @Test(".enabled on the same trace produces the same phase-side outcome as .shadow")
    func shadowAndEnabledHaveIdenticalPhaseOutcomes() {
        let trace = Self.makeTrace(atomCount: 250)
        let shadow = ChapterSignalGate.replay(trace: trace, mode: .shadow)
        let enabled = ChapterSignalGate.replay(trace: trace, mode: .enabled)

        // The PHASE behavior is identical in shadow vs enabled —
        // ChapterSignalMode only diverges at the consumer-read step,
        // which is bead 14 / 16 / 19's responsibility, not the gate's.
        // The gate's job is to faithfully report what the phase saw.
        #expect(shadow.episodesProcessed == enabled.episodesProcessed)
        #expect(shadow.planGeneratedCount == enabled.planGeneratedCount)
        #expect(shadow.totalFMCallsForChapterLabeling == enabled.totalFMCallsForChapterLabeling)
        #expect(shadow.aggregateLatencyMs == enabled.aggregateLatencyMs)
        #expect(shadow.skippedByCreatorChapters == enabled.skippedByCreatorChapters)
        // The mode field itself differs, of course.
        #expect(shadow.mode == .shadow)
        #expect(enabled.mode == .enabled)
        // Length-aware equality on per-episode outcomes. We previously
        // used `zip(...)` which silently truncates when lengths differ;
        // assert lengths first, then walk in lockstep so a length
        // regression surfaces here.
        #expect(shadow.perEpisodeOutcomes.count == enabled.perEpisodeOutcomes.count,
                "perEpisodeOutcomes lengths must match between shadow and enabled.")
        for (s, e) in zip(shadow.perEpisodeOutcomes, enabled.perEpisodeOutcomes) {
            #expect(s.episodeId == e.episodeId)
            #expect(s.podcastId == e.podcastId)
            #expect(s.planGenerated == e.planGenerated)
            #expect(s.fmCallsForChapterLabeling == e.fmCallsForChapterLabeling)
            #expect(s.phaseLatencyMs == e.phaseLatencyMs)
            #expect(s.skippedByCreatorChapters == e.skippedByCreatorChapters)
            #expect(s.abortedByOperationalRate == e.abortedByOperationalRate)
            #expect(s.abortedByPathologicalRate == e.abortedByPathologicalRate)
            #expect(s.mode == .shadow)
            #expect(e.mode == .enabled)
        }
    }

    // MARK: - Creator-chapter precedence

    @Test("Creator chapters detected → phase short-circuits with skippedByCreatorChapters=true and zero FM cost")
    func creatorChaptersShortCircuit() {
        let trace = Self.makeTrace(atomCount: 500)
        let cfg = ChapterSignalGate.Config(creatorChaptersPresent: { _ in true })
        let result = ChapterSignalGate.replay(trace: trace, mode: .shadow, config: cfg)

        #expect(result.skippedByCreatorChapters == 1)
        #expect(result.planGeneratedCount == 0,
                "Creator-chapters short-circuit must not generate a plan — that's the design's 'creator labels are near-ground-truth' rule.")
        #expect(result.totalFMCallsForChapterLabeling == 0,
                "FM cost must be zero on the creator-chapter path; that's the whole point of the short-circuit.")
        #expect(result.aggregateLatencyMs == 0.0,
                "Latency must be zero on the creator-chapter path — no overhead is charged for a no-op.")
        let outcome = result.perEpisodeOutcomes[0]
        #expect(outcome.skippedByCreatorChapters)
        #expect(!outcome.planGenerated)
        #expect(outcome.fmCallsForChapterLabeling == 0)
    }

    @Test("Creator-chapter detector is per-trace, not global: only flagged traces short-circuit")
    func creatorChaptersAreScopedPerTrace() throws {
        let withCreator = Self.makeTrace(episodeId: "ep-creator", atomCount: 250)
        let withoutCreator = Self.makeTrace(episodeId: "ep-inferred", atomCount: 250)
        let cfg = ChapterSignalGate.Config(creatorChaptersPresent: { trace in
            trace.episodeId == "ep-creator"
        })
        let result = ChapterSignalGate.replay(
            traces: [withCreator, withoutCreator],
            mode: .shadow,
            config: cfg
        )

        #expect(result.episodesProcessed == 2)
        #expect(result.skippedByCreatorChapters == 1)
        #expect(result.planGeneratedCount == 1)
        #expect(result.totalFMCallsForChapterLabeling == 5)

        // Identify which outcome is which without relying on array order.
        // `try #require` on the lookups so a future bug that drops or
        // collapses outcomes surfaces as a clear failure here rather
        // than a silent pass via Optional?.field == value (which is
        // nil-tolerant in unsafe ways).
        let creator = try #require(
            result.perEpisodeOutcomes.first { $0.episodeId == "ep-creator" },
            "creator-chapter outcome must be present in perEpisodeOutcomes; missing it would mask a regression."
        )
        let inferred = try #require(
            result.perEpisodeOutcomes.first { $0.episodeId == "ep-inferred" },
            "inferred-chapter outcome must be present in perEpisodeOutcomes; missing it would mask a regression."
        )
        #expect(creator.skippedByCreatorChapters)
        #expect(!creator.planGenerated)
        #expect(!inferred.skippedByCreatorChapters)
        #expect(inferred.planGenerated)
    }

    // MARK: - Mode-comparison helper

    @Test("replay(trace:modes:) returns one entry per mode with the documented per-mode shapes")
    func modeComparisonHelper() throws {
        let trace = Self.makeTrace(atomCount: 250)
        let results = ChapterSignalGate.replay(
            trace: trace,
            modes: [.off, .shadow, .enabled]
        )

        #expect(results.count == 3)
        #expect(results[.off]?.planGeneratedCount == 0)
        #expect(results[.shadow]?.planGeneratedCount == 1)
        #expect(results[.enabled]?.planGeneratedCount == 1)
        #expect(results[.off]?.totalFMCallsForChapterLabeling == 0)
        #expect(results[.shadow]?.totalFMCallsForChapterLabeling == 5)
        #expect(results[.enabled]?.totalFMCallsForChapterLabeling == 5)

        // Lift comparison primitive: shadow - off ≥ 0 across all
        // additive counters. Bead 19 builds on this exact invariant.
        let off = try #require(results[.off])
        let shadow = try #require(results[.shadow])
        #expect(shadow.totalFMCallsForChapterLabeling >= off.totalFMCallsForChapterLabeling)
        #expect(shadow.aggregateLatencyMs >= off.aggregateLatencyMs)
        #expect(shadow.planGeneratedCount >= off.planGeneratedCount)
    }

    @Test("replay(trace:modes:) with duplicate modes collapses deterministically (same value, dictionary semantics)")
    func modeComparisonHelperHandlesDuplicates() {
        let trace = Self.makeTrace(atomCount: 250)
        let results = ChapterSignalGate.replay(
            trace: trace,
            modes: [.shadow, .shadow, .enabled]
        )
        // Dictionary-keyed: duplicates collapse to one entry per mode.
        #expect(results.count == 2)
        #expect(results.keys.contains(.shadow))
        #expect(results.keys.contains(.enabled))
    }

    @Test("replay(trace:modes:) with duplicate modes invokes caller closures only once per unique mode (cost discipline)")
    func modeComparisonDeduplicatesWork() {
        let trace = Self.makeTrace(atomCount: 250)
        let stubCalls = ClosureCallCounter()
        let creatorCalls = ClosureCallCounter()
        let cfg = ChapterSignalGate.Config(
            stubChapterCount: { _ in stubCalls.increment(); return 5 },
            creatorChaptersPresent: { _ in creatorCalls.increment(); return false }
        )
        // 5 entries: .off (closures not called), .shadow ×2 (one call
        // each), .enabled ×2 (one call each). After deduplication the
        // caller closures should be invoked once per unique non-.off
        // mode = 2 calls total per closure.
        _ = ChapterSignalGate.replay(
            trace: trace,
            modes: [.off, .shadow, .shadow, .enabled, .enabled],
            config: cfg
        )
        #expect(stubCalls.count == 2,
                "stubChapterCount must be invoked once per unique non-.off mode after deduplication; observed \(stubCalls.count) calls.")
        #expect(creatorCalls.count == 2,
                "creatorChaptersPresent must be invoked once per unique non-.off mode after deduplication; observed \(creatorCalls.count) calls.")
    }

    @Test("replay(trace:modes:) with all-.off duplicate modes still produces one structural-zero entry")
    func modeComparisonDuplicateOffStillProducesEntry() {
        // Edge case: deduplication must not erase `.off` from the
        // result dictionary. `[.off, .off, .off]` → 1 entry, not 0.
        // The fast-path inside `replay(traces:mode:)` short-circuits
        // before invoking caller closures, so this test also pins
        // that the closures stay un-invoked even with multiple `.off`
        // entries (the fast path runs once after dedup).
        let trace = Self.makeTrace(atomCount: 250)
        let stubCalls = ClosureCallCounter()
        let creatorCalls = ClosureCallCounter()
        let cfg = ChapterSignalGate.Config(
            stubChapterCount: { _ in stubCalls.increment(); return 5 },
            creatorChaptersPresent: { _ in creatorCalls.increment(); return false }
        )
        let results = ChapterSignalGate.replay(
            trace: trace,
            modes: [.off, .off, .off],
            config: cfg
        )
        #expect(results.count == 1)
        #expect(results.keys.contains(.off))
        #expect(stubCalls.count == 0,
                "All-.off mode list must not invoke stubChapterCount; observed \(stubCalls.count) calls.")
        #expect(creatorCalls.count == 0,
                "All-.off mode list must not invoke creatorChaptersPresent; observed \(creatorCalls.count) calls.")
    }

    @Test("replay(trace:modes:) with empty modes returns empty dictionary")
    func modeComparisonHelperEmptyModes() {
        let trace = Self.makeTrace()
        let results = ChapterSignalGate.replay(trace: trace, modes: [])
        #expect(results.isEmpty)
    }

    // MARK: - Multi-trace aggregation

    @Test("Multi-trace replay sums per-episode counters into aggregate counters")
    func multiTraceAggregation() {
        let traces = (0..<5).map { i in
            Self.makeTrace(episodeId: "ep-\(i)", atomCount: 250)
        }
        let result = ChapterSignalGate.replay(traces: traces, mode: .shadow)

        #expect(result.episodesProcessed == 5)
        #expect(result.planGeneratedCount == 5)
        #expect(result.totalFMCallsForChapterLabeling == 5 * 5)
        #expect(result.aggregateLatencyMs == 5.0 * 130.0)
        #expect(result.perEpisodeOutcomes.count == 5)

        // Aggregate counters MUST equal the sum of per-episode flags;
        // this is the harness's invariant for bead 19's metric
        // computation.
        let computedFM = result.perEpisodeOutcomes.reduce(0) { $0 + $1.fmCallsForChapterLabeling }
        let computedLatency = result.perEpisodeOutcomes.reduce(0.0) { $0 + $1.phaseLatencyMs }
        let computedPlans = result.perEpisodeOutcomes.filter(\.planGenerated).count
        #expect(result.totalFMCallsForChapterLabeling == computedFM)
        #expect(result.aggregateLatencyMs == computedLatency)
        #expect(result.planGeneratedCount == computedPlans)
    }

    @Test("Multi-trace replay preserves input order in perEpisodeOutcomes")
    func multiTracePreservesOrder() {
        let traces = (0..<5).map { i in
            Self.makeTrace(episodeId: "ep-\(i)", atomCount: 250)
        }
        let result = ChapterSignalGate.replay(traces: traces, mode: .shadow)
        let observedIds = result.perEpisodeOutcomes.map(\.episodeId)
        #expect(observedIds == ["ep-0", "ep-1", "ep-2", "ep-3", "ep-4"])
    }

    @Test("Empty trace array yields zero counters in every mode")
    func emptyTracesYieldsZeroCounters() {
        for mode in ChapterSignalMode.allCases {
            let result = ChapterSignalGate.replay(traces: [], mode: mode)
            #expect(result.episodesProcessed == 0)
            #expect(result.planGeneratedCount == 0)
            #expect(result.totalFMCallsForChapterLabeling == 0)
            #expect(result.aggregateLatencyMs == 0.0)
            #expect(result.perEpisodeOutcomes.isEmpty)
            #expect(result.mode == mode)
        }
    }

    // MARK: - Stub-config wiring

    @Test("Custom stubChapterCount changes FM-call count and latency proportionally")
    func customStubChapterCountIsRespected() {
        let trace = Self.makeTrace(atomCount: 250)
        let cfg = ChapterSignalGate.Config(stubChapterCount: { _ in 8 })
        let result = ChapterSignalGate.replay(trace: trace, mode: .shadow, config: cfg)
        #expect(result.totalFMCallsForChapterLabeling == 8)
        // Latency = 5 + 8*25 = 205
        #expect(result.aggregateLatencyMs == 205.0)
    }

    @Test("Negative stubChapterCount is clamped to 0 and produces planGenerated=false (mirrors production chapter_phase_no_candidates)")
    func negativeStubChapterCountClampsToZero() {
        let trace = Self.makeTrace(atomCount: 250)
        let cfg = ChapterSignalGate.Config(stubChapterCount: { _ in -3 })
        let result = ChapterSignalGate.replay(trace: trace, mode: .shadow, config: cfg)
        #expect(result.totalFMCallsForChapterLabeling == 0)
        // Latency = perEpisodeOverhead only (5), since safeCount==0 and
        // the boundary detector still ran (produced no candidates).
        #expect(result.aggregateLatencyMs == 5.0)
        // planGenerated must be FALSE: production emits
        // `chapter_phase_no_candidates` and writes NO plan when the
        // boundary detector returns 0 candidates (see
        // ChapterPhaseDiagnostics.swift `noCandidates`). The replay gate
        // mirrors that semantic so `planGeneratedCount` truthfully
        // reflects "a plan was emitted" rather than "the phase ran".
        #expect(result.planGeneratedCount == 0)
        let outcome = result.perEpisodeOutcomes[0]
        #expect(!outcome.planGenerated)
        #expect(outcome.fmCallsForChapterLabeling == 0)
    }

    @Test("Zero stubChapterCount produces planGenerated=false (no-candidates path is the production contract)")
    func zeroStubChapterCountProducesNoPlan() {
        // Distinct from the negative-count test: a stub that explicitly
        // returns 0 (the legitimate "no candidates" outcome) must be
        // observationally identical to the clamp path. This pins the
        // production parity for the no-candidates event class.
        let trace = Self.makeTrace(atomCount: 250)
        let cfg = ChapterSignalGate.Config(stubChapterCount: { _ in 0 })
        let result = ChapterSignalGate.replay(trace: trace, mode: .shadow, config: cfg)
        #expect(result.planGeneratedCount == 0)
        #expect(result.totalFMCallsForChapterLabeling == 0)
        #expect(result.aggregateLatencyMs == 5.0)
        let outcome = result.perEpisodeOutcomes[0]
        #expect(!outcome.planGenerated)
        // It is also NOT classified as creator-skipped or aborted: the
        // bucket is "phase ran, no plan". This is the (currently empty)
        // "nothing" bucket the outcomeAccountingInvariant test treats
        // as legal.
        #expect(!outcome.skippedByCreatorChapters)
        #expect(!outcome.abortedByOperationalRate)
        #expect(!outcome.abortedByPathologicalRate)
    }

    @Test("Default stubChapterCount is bounded to [1, 12] across atom-count extremes")
    func defaultStubChapterCountIsBounded() {
        // Empty atoms: clamp to 1.
        let empty = Self.makeTrace(atomCount: 0)
        #expect(ChapterSignalGate.Config.defaultStubChapterCount(empty) == 1)

        // Tiny: still 1.
        let tiny = Self.makeTrace(atomCount: 5)
        #expect(ChapterSignalGate.Config.defaultStubChapterCount(tiny) == 1)

        // Mid: 250 / 50 = 5.
        let mid = Self.makeTrace(atomCount: 250)
        #expect(ChapterSignalGate.Config.defaultStubChapterCount(mid) == 5)

        // Pathologically large: clamp at 12.
        let huge = Self.makeTrace(atomCount: 10_000)
        #expect(ChapterSignalGate.Config.defaultStubChapterCount(huge) == 12)
    }

    // MARK: - Cross-mode invariants (R1 review)

    @Test("Outcome accounting invariant: every episode falls in at most one bucket (skipped | aborted-op | aborted-path | planGenerated)")
    func outcomeAccountingInvariant() {
        // Mix three traces: one creator-chapter, two normal.
        let creator = Self.makeTrace(episodeId: "ep-creator", atomCount: 250)
        let plain1 = Self.makeTrace(episodeId: "ep-plain-1", atomCount: 250)
        let plain2 = Self.makeTrace(episodeId: "ep-plain-2", atomCount: 250)
        let cfg = ChapterSignalGate.Config(creatorChaptersPresent: { trace in
            trace.episodeId == "ep-creator"
        })
        let result = ChapterSignalGate.replay(
            traces: [creator, plain1, plain2],
            mode: .shadow,
            config: cfg
        )

        // Each EpisodeOutcome must be in at most one of {skipped, aborted-op,
        // aborted-path, planGenerated}. The "nothing" bucket is the
        // implicit "phase ran with no output" — currently empty in the
        // stub, but the invariant must hold across all combinations.
        for outcome in result.perEpisodeOutcomes {
            let buckets = [
                outcome.skippedByCreatorChapters,
                outcome.abortedByOperationalRate,
                outcome.abortedByPathologicalRate,
                outcome.planGenerated
            ]
            let trueCount = buckets.filter { $0 }.count
            #expect(trueCount <= 1,
                    "Outcome \(outcome.episodeId) is in \(trueCount) buckets; must be ≤ 1.")
        }

        // Aggregate buckets must not exceed episodesProcessed (a sanity
        // upper bound for bead 19's fraction-style metrics).
        let sumOfBuckets = result.skippedByCreatorChapters
            + result.planAbortedByOperationalRate
            + result.planAbortedByPathologicalRate
            + result.planGeneratedCount
        #expect(sumOfBuckets <= result.episodesProcessed,
                "Aggregate buckets (\(sumOfBuckets)) must not exceed episodesProcessed (\(result.episodesProcessed)).")
    }

    @Test("Mode order in replay(trace:modes:) does not affect any individual mode's value (carry-forward-free design)")
    func modeOrderIndependence() {
        let trace = Self.makeTrace(atomCount: 250)
        let resultsA = ChapterSignalGate.replay(trace: trace, modes: [.off, .shadow, .enabled])
        let resultsB = ChapterSignalGate.replay(trace: trace, modes: [.enabled, .off, .shadow])
        let resultsC = ChapterSignalGate.replay(trace: trace, modes: [.shadow, .enabled, .off])

        // Each mode's per-replay result must be Equatable-equal across
        // the three orderings — the guarantee the carry-forward-free
        // design promises in the gate's doc comment.
        for mode in ChapterSignalMode.allCases {
            #expect(resultsA[mode] == resultsB[mode],
                    "mode=\(mode) result drifted between orderings A and B.")
            #expect(resultsA[mode] == resultsC[mode],
                    "mode=\(mode) result drifted between orderings A and C.")
        }
    }

    @Test("Shadow and enabled produce phase-side-identical EpisodeOutcomes after mode normalization")
    func shadowAndEnabledArePhaseSideIdentical() {
        let traces = (0..<4).map { i in
            Self.makeTrace(episodeId: "ep-\(i)", atomCount: 250)
        }
        let shadow = ChapterSignalGate.replay(traces: traces, mode: .shadow)
        let enabled = ChapterSignalGate.replay(traces: traces, mode: .enabled)

        // Re-stamp the mode field on each outcome so we can compare
        // EpisodeOutcomes directly via Equatable. Phase-side equivalence
        // is exactly: every other field must match. The consumer-side
        // divergence (consumersReadChapterPlan) lives in beads 14 / 16,
        // not in this gate.
        func normalize(_ outcomes: [ChapterSignalGate.EpisodeOutcome]) -> [ChapterSignalGate.EpisodeOutcome] {
            outcomes.map { o in
                ChapterSignalGate.EpisodeOutcome(
                    episodeId: o.episodeId,
                    podcastId: o.podcastId,
                    mode: .shadow,
                    planGenerated: o.planGenerated,
                    skippedByCreatorChapters: o.skippedByCreatorChapters,
                    abortedByOperationalRate: o.abortedByOperationalRate,
                    abortedByPathologicalRate: o.abortedByPathologicalRate,
                    fmCallsForChapterLabeling: o.fmCallsForChapterLabeling,
                    phaseLatencyMs: o.phaseLatencyMs
                )
            }
        }
        #expect(normalize(shadow.perEpisodeOutcomes) == normalize(enabled.perEpisodeOutcomes),
                "Shadow and enabled must produce phase-side-identical outcomes; the only divergence lives in the consumer-read step (bead 14 / 16), which the gate does not exercise.")
    }

    // MARK: - Multi-trace × multi-mode

    @Test("replay(traces:modes:) aggregates correctly across multiple traces AND multiple modes simultaneously")
    func multiTraceMultiModeAggregation() throws {
        let traces = (0..<3).map { i in
            Self.makeTrace(episodeId: "ep-\(i)", atomCount: 250)
        }
        let results = ChapterSignalGate.replay(
            traces: traces,
            modes: [.off, .shadow, .enabled]
        )

        #expect(results.count == 3)

        // .off across 3 traces: structural zero, 3 outcomes. `try
        // #require` instead of `!` so a missing key surfaces as a
        // typed Issue rather than a fatalError trap that obscures the
        // failing mode.
        let off = try #require(results[.off])
        #expect(off.episodesProcessed == 3)
        #expect(off.planGeneratedCount == 0)
        #expect(off.totalFMCallsForChapterLabeling == 0)
        #expect(off.aggregateLatencyMs == 0.0)
        #expect(off.perEpisodeOutcomes.count == 3)

        // .shadow across 3 traces: 5 chapters per trace × 3 traces = 15 FM calls.
        // (130.0 × 3.0 is exact in IEEE 754; no float-fuzz needed.)
        let shadow = try #require(results[.shadow])
        #expect(shadow.episodesProcessed == 3)
        #expect(shadow.planGeneratedCount == 3)
        #expect(shadow.totalFMCallsForChapterLabeling == 15)
        #expect(shadow.aggregateLatencyMs == 3.0 * 130.0)

        // .enabled mirrors .shadow on the phase side (consumer-side
        // divergence is bead 14 / 16 / 19, not the gate's territory).
        let enabled = try #require(results[.enabled])
        #expect(enabled.episodesProcessed == shadow.episodesProcessed)
        #expect(enabled.planGeneratedCount == shadow.planGeneratedCount)
        #expect(enabled.totalFMCallsForChapterLabeling == shadow.totalFMCallsForChapterLabeling)
        #expect(enabled.aggregateLatencyMs == shadow.aggregateLatencyMs)

        // Pairwise alignment: `perEpisodeOutcomes` must come back in
        // input-trace order across every mode. Bead 19 zips outcomes
        // across modes to compute per-episode lift; if `.off` and
        // `.shadow` disagreed on ordering the zip would silently mis-
        // attribute lift to the wrong episode. Pin the alignment here.
        let inputIds = traces.map(\.episodeId)
        #expect(off.perEpisodeOutcomes.map(\.episodeId) == inputIds,
                ".off perEpisodeOutcomes must preserve input trace order.")
        #expect(shadow.perEpisodeOutcomes.map(\.episodeId) == inputIds,
                ".shadow perEpisodeOutcomes must preserve input trace order.")
        #expect(enabled.perEpisodeOutcomes.map(\.episodeId) == inputIds,
                ".enabled perEpisodeOutcomes must preserve input trace order.")
    }

    @Test("replay(traces:modes:) with empty traces returns one result per mode, each at structural zero")
    func multiTraceMultiModeEmptyTraces() throws {
        // Empty traces × non-empty modes: dictionary keyed by mode,
        // every value is a structural zero. This pins the shape so
        // bead 19 can always assume `results[mode] != nil` for every
        // mode in the request. Use `allCases` for both the request and
        // the assertion loop so adding a new ChapterSignalMode case
        // forces both halves of the test to update in lockstep
        // (otherwise `r != nil` would silently fail on the new case
        // and the failure message would be cryptic).
        let allModes = Array(ChapterSignalMode.allCases)
        let results = ChapterSignalGate.replay(traces: [], modes: allModes)
        #expect(results.count == allModes.count)
        for mode in allModes {
            let r = try #require(results[mode],
                                 "missing entry for mode=\(mode); replay(traces:modes:) must return one result per requested mode.")
            #expect(r.episodesProcessed == 0)
            #expect(r.planGeneratedCount == 0)
            #expect(r.totalFMCallsForChapterLabeling == 0)
            #expect(r.perEpisodeOutcomes.isEmpty)
        }
    }

    @Test("replay(traces:modes:) with empty modes returns empty dictionary regardless of trace count")
    func multiTraceMultiModeEmptyModes() {
        let traces = (0..<5).map { i in
            Self.makeTrace(episodeId: "ep-\(i)", atomCount: 250)
        }
        let results = ChapterSignalGate.replay(traces: traces, modes: [])
        #expect(results.isEmpty)
    }

    // MARK: - Determinism (the byte-for-byte guarantee)

    @Test("Identical input → identical output across all three modes (Equatable equality)")
    func deterministicAcrossAllModes() {
        let trace = Self.makeTrace(atomCount: 250)
        for mode in ChapterSignalMode.allCases {
            let r1 = ChapterSignalGate.replay(trace: trace, mode: mode)
            let r2 = ChapterSignalGate.replay(trace: trace, mode: mode)
            #expect(r1 == r2,
                    "mode=\(mode) must be deterministic — replay twice yields identical Equatable output.")
        }
    }

    @Test("mode=.off is a fast path that does not invoke any caller-supplied closure (provability of the byte-for-byte claim)")
    func offDoesNotInvokeCallerClosures() {
        // If `.off` reads the trace at all we'd be at risk of drifting
        // away from "today's narl-eval behavior" the moment a future
        // bead adds a new trace field. Pin the contract: `.off` MUST
        // NOT call into either of the caller-configurable closures.
        let trace = Self.makeTrace()
        let stubCalled = ClosureCallCounter()
        let creatorCalled = ClosureCallCounter()
        let cfg = ChapterSignalGate.Config(
            stubChapterCount: { _ in stubCalled.increment(); return 1 },
            creatorChaptersPresent: { _ in creatorCalled.increment(); return false }
        )
        _ = ChapterSignalGate.replay(trace: trace, mode: .off, config: cfg)
        #expect(stubCalled.count == 0,
                "mode=.off must NOT invoke stubChapterCount — that's how byte-for-byte parity stays inviolable across future trace-shape changes.")
        #expect(creatorCalled.count == 0,
                "mode=.off must NOT invoke creatorChaptersPresent — same reason.")
    }
}

// MARK: - Test support

/// Reference-typed counter so tests can observe whether a closure was
/// called from inside a value-type config struct. The gate's replay path
/// is single-threaded today, but the closures themselves are `@Sendable`
/// — meaning a future cross-task call site is permitted by the type
/// system. Lock the counter so this test stays correct under that
/// future shape rather than encoding "single-threaded today" into the
/// test's failure mode. Local to the test file (no production parity
/// required).
private final class ClosureCallCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var _count: Int = 0
    var count: Int {
        lock.lock(); defer { lock.unlock() }
        return _count
    }
    func increment() {
        lock.lock(); defer { lock.unlock() }
        _count += 1
    }
}
