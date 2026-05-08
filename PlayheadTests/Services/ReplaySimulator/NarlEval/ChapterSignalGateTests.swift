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

    @Test(".off ignores trace contents (creator chapters, atom count, custom config) — only episode/podcast id appear in output")
    func offIgnoresTraceContents() {
        let trace = Self.makeTrace(atomCount: 1)
        // Even with a config that *would* fire the creator-chapters
        // branch and a stub-count override that would multiply FM cost,
        // mode=.off must still return the structural zero.
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
        // Per-episode outcomes carry their own mode but otherwise match.
        for (s, e) in zip(shadow.perEpisodeOutcomes, enabled.perEpisodeOutcomes) {
            #expect(s.episodeId == e.episodeId)
            #expect(s.podcastId == e.podcastId)
            #expect(s.planGenerated == e.planGenerated)
            #expect(s.fmCallsForChapterLabeling == e.fmCallsForChapterLabeling)
            #expect(s.phaseLatencyMs == e.phaseLatencyMs)
            #expect(s.skippedByCreatorChapters == e.skippedByCreatorChapters)
            #expect(s.mode == .shadow || s.mode == .enabled)
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
    func creatorChaptersAreScopedPerTrace() {
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
        let creator = result.perEpisodeOutcomes.first { $0.episodeId == "ep-creator" }
        let inferred = result.perEpisodeOutcomes.first { $0.episodeId == "ep-inferred" }
        #expect(creator?.skippedByCreatorChapters == true)
        #expect(creator?.planGenerated == false)
        #expect(inferred?.skippedByCreatorChapters == false)
        #expect(inferred?.planGenerated == true)
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

    @Test("Negative stubChapterCount is clamped to 0 (defensive against caller bugs)")
    func negativeStubChapterCountClampsToZero() {
        let trace = Self.makeTrace(atomCount: 250)
        let cfg = ChapterSignalGate.Config(stubChapterCount: { _ in -3 })
        let result = ChapterSignalGate.replay(trace: trace, mode: .shadow, config: cfg)
        #expect(result.totalFMCallsForChapterLabeling == 0)
        // Latency = perEpisodeOverhead only (5), since safeCount==0.
        #expect(result.aggregateLatencyMs == 5.0)
        // But planGenerated is still true — the phase ran, it just
        // produced an empty plan. (A real labelling failure would
        // surface as abortedByOperationalRate/PathologicalRate.)
        #expect(result.planGeneratedCount == 1)
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

/// Trivial reference-typed counter so tests can observe whether a
/// closure was called from inside a value-type config struct. Local to
/// the test file (no production parity required).
private final class ClosureCallCounter: @unchecked Sendable {
    private(set) var count: Int = 0
    func increment() { count += 1 }
}
