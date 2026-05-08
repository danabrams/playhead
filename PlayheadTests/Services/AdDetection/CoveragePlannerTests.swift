// CoveragePlannerTests.swift
// Regression tests for phase 3 coverage-policy selection.

import Foundation
import Testing

@testable import Playhead

private func makeCoveragePlannerContext(
    observedEpisodeCount: Int = 6,
    stableRecall: Bool = true,
    isFirstEpisodeAfterCohortInvalidation: Bool = false,
    recallDegrading: Bool = false,
    sponsorDriftDetected: Bool = false,
    auditMissDetected: Bool = false,
    episodesSinceLastFullRescan: Int = 1,
    periodicFullRescanIntervalEpisodes: Int = 10,
    chapterSignalMode: ChapterSignalMode = .off,
    chapterEvidence: [ChapterEvidence]? = nil
) -> CoveragePlannerContext {
    CoveragePlannerContext(
        observedEpisodeCount: observedEpisodeCount,
        stableRecall: stableRecall,
        isFirstEpisodeAfterCohortInvalidation: isFirstEpisodeAfterCohortInvalidation,
        recallDegrading: recallDegrading,
        sponsorDriftDetected: sponsorDriftDetected,
        auditMissDetected: auditMissDetected,
        episodesSinceLastFullRescan: episodesSinceLastFullRescan,
        periodicFullRescanIntervalEpisodes: periodicFullRescanIntervalEpisodes,
        chapterSignalMode: chapterSignalMode,
        chapterEvidence: chapterEvidence
    )
}

// au2v.1.14 fixture helpers — produce ChapterEvidence values with
// hand-picked qualityScores and dispositions so the planner's
// chapter-informed branches can be exercised deterministically.
private func adChapter(
    start: TimeInterval,
    end: TimeInterval,
    quality: Float,
    source: ChapterSource = .pc20
) -> ChapterEvidence {
    ChapterEvidence(
        startTime: start,
        endTime: end,
        title: "Sponsor break",
        source: source,
        disposition: .adBreak,
        qualityScore: quality
    )
}

private func contentChapter(
    start: TimeInterval,
    end: TimeInterval,
    quality: Float,
    source: ChapterSource = .pc20
) -> ChapterEvidence {
    ChapterEvidence(
        startTime: start,
        endTime: end,
        title: "Discussion segment",
        source: source,
        disposition: .content,
        qualityScore: quality
    )
}

private func ambiguousChapter(
    start: TimeInterval,
    end: TimeInterval,
    quality: Float = 0.2
) -> ChapterEvidence {
    ChapterEvidence(
        startTime: start,
        endTime: end,
        title: nil,
        source: .pc20,
        disposition: .ambiguous,
        qualityScore: quality
    )
}

@Suite("CoveragePlanner")
struct CoveragePlannerTests {

    @Test("fullCoverage selected for cold-start, invalidation, recall degradation, and audit misses")
    func testFullCoverageTriggers() {
        let planner = CoveragePlanner()
        let cases = [
            makeCoveragePlannerContext(observedEpisodeCount: 4),
            makeCoveragePlannerContext(isFirstEpisodeAfterCohortInvalidation: true),
            makeCoveragePlannerContext(recallDegrading: true),
            makeCoveragePlannerContext(auditMissDetected: true),
        ]

        for context in cases {
            let plan = planner.plan(for: context)
            #expect(plan.policy == .fullCoverage)
            #expect(plan.phases == [.fullEpisodeScan])
            #expect(plan.auditWindowSampleRate == nil)
        }
    }

    @Test("mature stable shows use targetedWithAudit with mandatory audit sampling")
    func testTargetedWithAuditForMatureStableShows() throws {
        let planner = CoveragePlanner()
        let plan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            stableRecall: true,
            episodesSinceLastFullRescan: 3
        ))

        #expect(plan.policy == .targetedWithAudit)
        #expect(plan.phases == [
            .scanHarvesterProposals,
            .scanLikelyAdSlots,
            .scanRandomAuditWindows,
        ])
        let sampleRate = try #require(plan.auditWindowSampleRate)
        #expect(sampleRate == CoveragePlanner.defaultAuditWindowSampleRate)
        #expect(sampleRate >= 0.10 && sampleRate <= 0.15)
    }

    @Test("periodicFullRescan triggers on episode interval or sponsor drift")
    func testPeriodicFullRescanTriggers() {
        let planner = CoveragePlanner()
        let cases = [
            makeCoveragePlannerContext(episodesSinceLastFullRescan: 10, periodicFullRescanIntervalEpisodes: 10),
            makeCoveragePlannerContext(sponsorDriftDetected: true),
        ]

        for context in cases {
            let plan = planner.plan(for: context)
            #expect(plan.policy == .periodicFullRescan)
            #expect(plan.phases == [.fullEpisodeScan])
            #expect(plan.auditWindowSampleRate == nil)
        }
    }

    // M12: auditWindowSampleRate clamp documentation + behavior
    @Test("auditWindowSampleRate is clamped into [0.10, 0.15]")
    func testAuditWindowSampleRateClamp() {
        let high = CoveragePlanner(auditWindowSampleRate: 0.5)
        #expect(high.auditWindowSampleRate == 0.15)

        let low = CoveragePlanner(auditWindowSampleRate: 0.0)
        #expect(low.auditWindowSampleRate == 0.10)

        let inRange = CoveragePlanner(auditWindowSampleRate: 0.12)
        #expect(inRange.auditWindowSampleRate == 0.12)
    }

    // M11/H8: reset feedback loop for periodic rescan
    @Test("reset(context:) zeroes episodesSinceLastFullRescan")
    func testResetClearsEpisodesSinceLastFullRescan() {
        let planner = CoveragePlanner()
        let context = makeCoveragePlannerContext(
            episodesSinceLastFullRescan: 10,
            periodicFullRescanIntervalEpisodes: 10
        )
        let reset = planner.reset(context: context)

        #expect(reset.episodesSinceLastFullRescan == 0)
        // Other fields preserved.
        #expect(reset.observedEpisodeCount == context.observedEpisodeCount)
        #expect(reset.periodicFullRescanIntervalEpisodes == context.periodicFullRescanIntervalEpisodes)
    }

    @Test("only the threshold-crossing call returns periodicFullRescan when reset is honored")
    func testPeriodicRescanProgressionWithReset() {
        let planner = CoveragePlanner(periodicFullRescanIntervalEpisodes: 10)

        // interval - 1 → targeted (mature stable show)
        let preThreshold = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 9,
            periodicFullRescanIntervalEpisodes: 10
        )
        #expect(planner.plan(for: preThreshold).policy == .targetedWithAudit)

        // interval → periodic
        let atThreshold = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 10,
            periodicFullRescanIntervalEpisodes: 10
        )
        #expect(planner.plan(for: atThreshold).policy == .periodicFullRescan)

        // After consuming the periodic rescan, caller resets the counter.
        let afterReset = planner.reset(context: atThreshold)
        #expect(planner.plan(for: afterReset).policy == .targetedWithAudit)

        // Without reset, the next call would still return periodic (proves the
        // reset contract is what gates the loop).
        let withoutReset = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 11,
            periodicFullRescanIntervalEpisodes: 10
        )
        #expect(planner.plan(for: withoutReset).policy == .periodicFullRescan)
    }

    // #11: precedence — cold-start beats periodic when both apply.
    @Test("cold-start full coverage wins over periodic full rescan when both trigger")
    func testColdStartWinsOverPeriodic() {
        let planner = CoveragePlanner()
        let context = makeCoveragePlannerContext(
            observedEpisodeCount: 1,
            episodesSinceLastFullRescan: 99,
            periodicFullRescanIntervalEpisodes: 10
        )

        let plan = planner.plan(for: context)
        #expect(plan.policy == .fullCoverage)
    }

    // #12: off-by-one and zero-config tests
    @Test("episodesSinceLastFullRescan == interval - 1 stays targeted")
    func testOffByOnePreThreshold() {
        let planner = CoveragePlanner()
        let plan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 9,
            periodicFullRescanIntervalEpisodes: 10
        ))
        #expect(plan.policy == .targetedWithAudit)
    }

    @Test("episodesSinceLastFullRescan == interval triggers periodic rescan")
    func testOffByOneAtThreshold() {
        let planner = CoveragePlanner()
        let plan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 10,
            periodicFullRescanIntervalEpisodes: 10
        ))
        #expect(plan.policy == .periodicFullRescan)
    }

    @Test("periodicFullRescanIntervalEpisodes == 0 falls back to planner default")
    func testPeriodicIntervalZeroFallsBackToDefault() {
        let planner = CoveragePlanner(periodicFullRescanIntervalEpisodes: 7)

        // Below the planner default (7) but above the bogus context-supplied 0.
        let below = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 6,
            periodicFullRescanIntervalEpisodes: 0
        )
        #expect(planner.plan(for: below).policy == .targetedWithAudit)

        // At the planner default fallback.
        let atDefault = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 7,
            periodicFullRescanIntervalEpisodes: 0
        )
        #expect(planner.plan(for: atDefault).policy == .periodicFullRescan)
    }

    @Test("coldStartEpisodeThreshold == 0 means cold-start never fires")
    func testColdStartThresholdZeroDisablesColdStart() {
        let planner = CoveragePlanner(coldStartEpisodeThreshold: 0)

        // observedEpisodeCount: 0 — would be cold-start under the default
        // threshold of 5, but with threshold 0 the cold-start branch must
        // never fire. Stable precision routes to targetedWithAudit.
        let plan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: true,
            episodesSinceLastFullRescan: 1
        ))
        #expect(plan.policy == .targetedWithAudit)
    }

    // MARK: - Chapter-informed audit selection (playhead-au2v.1.14)

    /// Reusable mature-stable context builder. The four edge-case tests
    /// below assert that turning off chapter consultation (in any of
    /// four ways) yields a plan that is BYTE-IDENTICAL to today's
    /// random-only plan — i.e. the chapter-aware code path leaves no
    /// trace. The "byte-identical" claim covers every observable plan
    /// field: policy, phases, auditWindowSampleRate, AND
    /// chapterInformedAudit (which must stay `nil`).
    private func matureStableContext(
        chapterSignalMode: ChapterSignalMode = .off,
        chapterEvidence: [ChapterEvidence]? = nil
    ) -> CoveragePlannerContext {
        makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            stableRecall: true,
            episodesSinceLastFullRescan: 3,
            chapterSignalMode: chapterSignalMode,
            chapterEvidence: chapterEvidence
        )
    }

    @Test("chapter-informed: nil chapterEvidence falls back to today's random plan (no trace)")
    func testChapterInformedNoEvidenceFallback() {
        let planner = CoveragePlanner()
        let baseline = planner.plan(for: matureStableContext())
        let withModeOnly = planner.plan(for: matureStableContext(chapterSignalMode: .enabled))
        // Same observable plan: policy + phases + sample rate.
        #expect(withModeOnly.policy == baseline.policy)
        #expect(withModeOnly.phases == baseline.phases)
        #expect(withModeOnly.auditWindowSampleRate == baseline.auditWindowSampleRate)
        // No chapter-informed audit on either plan.
        #expect(baseline.chapterInformedAudit == nil)
        #expect(withModeOnly.chapterInformedAudit == nil)
        // Diagnostic surface: baseline (mode .off) records `mode_disabled`;
        // mode-only-no-evidence records `no_chapter_evidence`. Both are
        // `.skipped` — neither produces an `.informed` payload.
        switch baseline.chapterAuditDiagnostic {
        case .skipped(let reason, let count):
            #expect(reason == .modeDisabled)
            #expect(count == 0)
        default:
            Issue.record("Expected baseline diagnostic .skipped(modeDisabled), got \(String(describing: baseline.chapterAuditDiagnostic))")
        }
        switch withModeOnly.chapterAuditDiagnostic {
        case .skipped(let reason, let count):
            #expect(reason == .noChapterEvidence)
            #expect(count == 0)
        default:
            Issue.record("Expected withModeOnly diagnostic .skipped(noChapterEvidence), got \(String(describing: withModeOnly.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: shadow mode does NOT consult evidence (consumer parity with .off)")
    func testChapterInformedShadowModeFallback() {
        let planner = CoveragePlanner()
        let chapters = [
            adChapter(start: 60, end: 120, quality: 0.9),
            contentChapter(start: 200, end: 600, quality: 0.85)
        ]
        let baseline = planner.plan(for: matureStableContext())
        let shadowed = planner.plan(for: matureStableContext(
            chapterSignalMode: .shadow,
            chapterEvidence: chapters
        ))
        #expect(shadowed.policy == baseline.policy)
        #expect(shadowed.phases == baseline.phases)
        #expect(shadowed.auditWindowSampleRate == baseline.auditWindowSampleRate)
        #expect(shadowed.chapterInformedAudit == nil)
        // Diagnostic should be `.skipped(modeDisabled)` even with rich
        // evidence — shadow mode means consumers don't read the plan.
        switch shadowed.chapterAuditDiagnostic {
        case .skipped(let reason, let count):
            #expect(reason == .modeDisabled)
            #expect(count == chapters.count)
        default:
            Issue.record("Expected shadow .skipped(modeDisabled, \(chapters.count)), got \(String(describing: shadowed.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: only ambiguous chapters falls back to random (no informed selection)")
    func testChapterInformedAmbiguousOnlyFallback() {
        let planner = CoveragePlanner()
        let chapters = [
            ambiguousChapter(start: 0, end: 60),
            ambiguousChapter(start: 60, end: 180)
        ]
        let baseline = planner.plan(for: matureStableContext())
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        ))
        // Byte-identical observable plan to baseline (policy / phases /
        // sample rate / nil chapterInformedAudit).
        #expect(plan.policy == baseline.policy)
        #expect(plan.phases == baseline.phases)
        #expect(plan.auditWindowSampleRate == baseline.auditWindowSampleRate)
        #expect(plan.chapterInformedAudit == nil)
        switch plan.chapterAuditDiagnostic {
        case .skipped(let reason, let count):
            #expect(reason == .noUsableChapters)
            #expect(count == chapters.count)
        default:
            Issue.record("Expected .skipped(noUsableChapters), got \(String(describing: plan.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: low planConfidence falls back to random")
    func testChapterInformedLowPlanConfidenceFallback() {
        // Pin the floor at 0.95 so realistic ad-disposition chapter
        // confidence cannot meet it.
        let planner = CoveragePlanner(minPlanConfidence: 0.95)
        let chapters = [
            adChapter(start: 60, end: 120, quality: 0.5)
        ]
        let baseline = planner.plan(for: matureStableContext())
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        ))
        // Byte-identical observable plan to baseline (policy / phases /
        // sample rate / nil chapterInformedAudit) — the
        // chapter-informed code path leaves no trace below the
        // confidence floor.
        #expect(plan.policy == baseline.policy)
        #expect(plan.phases == baseline.phases)
        #expect(plan.auditWindowSampleRate == baseline.auditWindowSampleRate)
        #expect(plan.chapterInformedAudit == nil)
        switch plan.chapterAuditDiagnostic {
        case .skipped(let reason, _):
            #expect(reason == .lowPlanConfidence)
        default:
            Issue.record("Expected .skipped(lowPlanConfidence), got \(String(describing: plan.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: chapter with nil endTime takes the 60s nominal-duration fallback")
    func testChapterInformedNilEndTimeFallback() throws {
        let planner = CoveragePlanner()
        // Ad chapter without an explicit endTime — the planner should
        // still consider it (quality clears the floor) and synthesise
        // a 60 s end time from `startTime + 60`.
        let openEnded = ChapterEvidence(
            startTime: 100,
            endTime: nil,
            title: "Sponsor break",
            source: .pc20,
            disposition: .adBreak,
            qualityScore: 0.9
        )
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: [openEnded]
        ))
        let informed = try #require(plan.chapterInformedAudit)
        #expect(informed.includes.count == 1)
        #expect(informed.includes.first?.startTime == 100)
        #expect(informed.includes.first?.endTime == 160)
    }

    @Test("chapter-informed: corrupt time bounds and NaN qualityScore are dropped before the gates")
    func testChapterInformedDefensiveFiltering() {
        let planner = CoveragePlanner()
        // None of these are usable: NaN start, end-before-start (==
        // and < cases), non-finite end, NaN quality. Keep the gate
        // honest by mixing in a single ambiguous chapter so
        // `chapterEvidence` is non-empty but produces zero usable
        // intervals.
        let chapters: [ChapterEvidence] = [
            ChapterEvidence(
                startTime: .nan, endTime: 60, title: nil,
                source: .pc20, disposition: .adBreak, qualityScore: 0.9
            ),
            ChapterEvidence(
                startTime: 30, endTime: 30, title: nil,
                source: .pc20, disposition: .adBreak, qualityScore: 0.9
            ),
            ChapterEvidence(
                startTime: 60, endTime: 30, title: nil,
                source: .pc20, disposition: .adBreak, qualityScore: 0.9
            ),
            ChapterEvidence(
                startTime: 30, endTime: .infinity, title: nil,
                source: .pc20, disposition: .adBreak, qualityScore: 0.9
            ),
            ChapterEvidence(
                startTime: 30, endTime: 60, title: nil,
                source: .pc20, disposition: .adBreak, qualityScore: .nan
            ),
            ambiguousChapter(start: 100, end: 200)
        ]
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        ))
        #expect(plan.chapterInformedAudit == nil)
        switch plan.chapterAuditDiagnostic {
        case .skipped(let reason, let count):
            #expect(reason == .noUsableChapters)
            #expect(count == chapters.count)
        default:
            Issue.record("Expected .skipped(noUsableChapters), got \(String(describing: plan.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: includes/excludes preserve input iteration order")
    func testChapterInformedPreservesInputOrder() throws {
        let planner = CoveragePlanner()
        // Hand chapters to the planner in a deliberately non-monotonic
        // order — both temporally and by disposition. The contract on
        // ChapterInformedAuditSelection says "preserve iteration
        // order"; verify it.
        let chapters: [ChapterEvidence] = [
            adChapter(start: 500, end: 560, quality: 0.9),       // includes[0]
            contentChapter(start: 100, end: 240, quality: 0.85), // excludes[0]
            ambiguousChapter(start: 800, end: 900),              // dropped
            adChapter(start: 50, end: 80, quality: 0.95),        // includes[1]
            contentChapter(start: 700, end: 750, quality: 0.8),  // excludes[1]
        ]
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        ))
        let informed = try #require(plan.chapterInformedAudit)
        #expect(informed.includes.count == 2)
        #expect(informed.includes[0].startTime == 500)
        #expect(informed.includes[1].startTime == 50)
        #expect(informed.excludes.count == 2)
        #expect(informed.excludes[0].startTime == 100)
        #expect(informed.excludes[1].startTime == 700)
    }

    @Test("chapter-informed: enabled mode + usable evidence emits chapter-informed selection")
    func testChapterInformedHappyPath() throws {
        let planner = CoveragePlanner()
        // qualityScore 0.9 clears the 0.4 ad-include floor; content
        // qualityScore 0.85 clears the 0.7 content-exclude floor.
        let chapters = [
            adChapter(start: 60, end: 180, quality: 0.9),
            contentChapter(start: 200, end: 600, quality: 0.85),
            ambiguousChapter(start: 700, end: 800)
        ]
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        ))
        let informed = try #require(plan.chapterInformedAudit,
                                    "Expected chapter-informed selection on enabled+evidence path")
        #expect(informed.includes.count == 1)
        #expect(informed.excludes.count == 1)
        #expect(informed.replacementFraction == CoveragePlanner.defaultReplacementFraction)
        #expect(informed.replacementFraction == 0.5)
        #expect(informed.evidenceCount == chapters.count)
        // Ad chapter included with its original quality.
        #expect(informed.includes.first?.kind == .adChapter)
        #expect(informed.includes.first?.qualityScore == 0.9)
        #expect(informed.includes.first?.startTime == 60)
        #expect(informed.includes.first?.endTime == 180)
        // Content chapter excluded with its original quality.
        #expect(informed.excludes.first?.kind == .contentExcluded)
        #expect(informed.excludes.first?.qualityScore == 0.85)
        // Diagnostic mirrors the selection.
        switch plan.chapterAuditDiagnostic {
        case .informed(let selection):
            #expect(selection == informed)
        default:
            Issue.record("Expected .informed diagnostic, got \(String(describing: plan.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: low-quality ad/content chapters do not survive the per-disposition gates")
    func testChapterInformedQualityGates() {
        let planner = CoveragePlanner()
        // Ad chapter at qualityScore == 0.4 (NOT > 0.4) must be filtered
        // out — the gate is strict-greater-than. Content chapter at
        // qualityScore == 0.7 (NOT > 0.7) must NOT be excluded — same
        // strict-greater-than gate.
        let chapters = [
            adChapter(start: 30, end: 90, quality: 0.4),
            contentChapter(start: 200, end: 400, quality: 0.7)
        ]
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        ))
        // Both filtered → no usable chapters → skipped.
        #expect(plan.chapterInformedAudit == nil)
        switch plan.chapterAuditDiagnostic {
        case .skipped(let reason, _):
            #expect(reason == .noUsableChapters)
        default:
            Issue.record("Expected .skipped(noUsableChapters), got \(String(describing: plan.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: source-agnostic — inferred chapters flow through identically to creator chapters")
    func testChapterInformedSourceAgnostic() throws {
        let planner = CoveragePlanner()
        let creatorChapters = [
            adChapter(start: 60, end: 120, quality: 0.9, source: .pc20)
        ]
        let inferredChapters = [
            adChapter(start: 60, end: 120, quality: 0.9, source: .inferred)
        ]
        let creatorPlan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: creatorChapters
        ))
        let inferredPlan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: inferredChapters
        ))
        // Identical plan output — the planner must NOT branch on source.
        let creatorInformed = try #require(creatorPlan.chapterInformedAudit)
        let inferredInformed = try #require(inferredPlan.chapterInformedAudit)
        #expect(creatorInformed == inferredInformed)
        #expect(creatorPlan.chapterAuditDiagnostic == inferredPlan.chapterAuditDiagnostic)
    }

    @Test("event(for:installID:episodeId:timestamp:) projects informed/skipped diagnostics into ChapterPhaseEvent")
    func testEventProjectionOnBothPaths() throws {
        let planner = CoveragePlanner()
        let installID = UUID(uuidString: "33333333-3333-4333-8333-333333333333")!
        // Informed path
        let informedPlan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: [adChapter(start: 60, end: 180, quality: 0.9)]
        ))
        let informedEvent = try #require(CoveragePlanner.event(
            for: informedPlan,
            installID: installID,
            episodeId: "ep-test",
            timestamp: 1_700_000_000
        ))
        #expect(informedEvent.eventType == .coveragePlanChapterInformed)
        #expect(informedEvent.timestamp == 1_700_000_000)
        // Payload-shape assertion: the projection must wire the
        // selection counts + planConfidence + replacementFraction
        // through to the diagnostic event — a regression that swapped
        // any of these fields would not be caught by the eventType
        // check alone.
        switch informedEvent.payload {
        case .coveragePlanChapterInformed(let p):
            let informed = try #require(informedPlan.chapterInformedAudit)
            #expect(p.fractionReplaced == informed.replacementFraction)
            #expect(p.adChapterIncludedCount == informed.includes.count)
            #expect(p.contentChapterExcludedCount == informed.excludes.count)
            #expect(p.planConfidence == informed.planConfidence)
        default:
            Issue.record("Expected coverage_plan_chapter_informed payload, got \(String(describing: informedEvent.payload))")
        }

        // Skipped path
        let skippedPlan = planner.plan(for: matureStableContext(
            chapterSignalMode: .off,
            chapterEvidence: [adChapter(start: 60, end: 180, quality: 0.9)]
        ))
        let skippedEvent = try #require(CoveragePlanner.event(
            for: skippedPlan,
            installID: installID,
            episodeId: "ep-test",
            timestamp: 1_700_000_000
        ))
        #expect(skippedEvent.eventType == .coveragePlanChapterSkipped)
        switch skippedEvent.payload {
        case .coveragePlanChapterSkipped(let p):
            #expect(p.reason == ChapterAuditSkipReason.modeDisabled.rawValue)
            #expect(p.evidenceCount == 1)
        default:
            Issue.record("Expected coverage_plan_chapter_skipped payload, got \(String(describing: skippedEvent.payload))")
        }

        // Non-targeted plan (cold start → fullCoverage) emits NO event.
        let coldPlan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 1
        ))
        #expect(coldPlan.policy == .fullCoverage)
        #expect(CoveragePlanner.event(
            for: coldPlan,
            installID: installID,
            episodeId: "ep-test",
            timestamp: 1_700_000_000
        ) == nil)
    }

    @Test("periodicFullRescan plans never carry a chapter-audit diagnostic, even with evidence")
    func testPeriodicFullRescanIgnoresChapterEvidence() {
        let planner = CoveragePlanner(periodicFullRescanIntervalEpisodes: 10)
        let chapters = [adChapter(start: 60, end: 180, quality: 0.9)]
        let context = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 10,
            periodicFullRescanIntervalEpisodes: 10,
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        )
        let plan = planner.plan(for: context)
        #expect(plan.policy == .periodicFullRescan)
        #expect(plan.chapterInformedAudit == nil)
        #expect(plan.chapterAuditDiagnostic == nil)
        // Projection must also return nil — periodicFullRescan plans
        // do not run audit selection.
        let installID = UUID(uuidString: "44444444-4444-4444-8444-444444444444")!
        #expect(CoveragePlanner.event(
            for: plan,
            installID: installID,
            episodeId: "ep-test",
            timestamp: 1_700_000_000
        ) == nil)
    }

    @Test("chapter-informed config tunables clamp into [0, 1]")
    func testChapterInformedConfigClamping() {
        let high = CoveragePlanner(
            replacementFraction: 5.0,
            adChapterMinQualityForAuditInclusion: 5.0,
            contentChapterMinQualityForExclusion: 5.0,
            minPlanConfidence: 5.0
        )
        #expect(high.replacementFraction == 1.0)
        #expect(high.adChapterMinQualityForAuditInclusion == 1.0)
        #expect(high.contentChapterMinQualityForExclusion == 1.0)
        #expect(high.minPlanConfidence == 1.0)

        let low = CoveragePlanner(
            replacementFraction: -1.0,
            adChapterMinQualityForAuditInclusion: -1.0,
            contentChapterMinQualityForExclusion: -1.0,
            minPlanConfidence: -1.0
        )
        #expect(low.replacementFraction == 0.0)
        #expect(low.adChapterMinQualityForAuditInclusion == 0.0)
        #expect(low.contentChapterMinQualityForExclusion == 0.0)
        #expect(low.minPlanConfidence == 0.0)

        // In-range round-trip — guards against a regression where the
        // clamp accidentally rounds in-range values to the bounds.
        let inRange = CoveragePlanner(
            replacementFraction: 0.7,
            adChapterMinQualityForAuditInclusion: 0.55,
            contentChapterMinQualityForExclusion: 0.65,
            minPlanConfidence: 0.42
        )
        #expect(inRange.replacementFraction == 0.7)
        #expect(inRange.adChapterMinQualityForAuditInclusion == 0.55)
        #expect(inRange.contentChapterMinQualityForExclusion == 0.65)
        #expect(inRange.minPlanConfidence == 0.42)
    }

    @Test("chapter-informed: custom replacementFraction propagates onto the emitted selection")
    func testChapterInformedReplacementFractionPropagates() throws {
        let planner = CoveragePlanner(replacementFraction: 0.3)
        let chapters = [adChapter(start: 60, end: 180, quality: 0.9)]
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        ))
        let informed = try #require(plan.chapterInformedAudit)
        #expect(informed.replacementFraction == 0.3)
        // Defensive: must NOT be the static default after a custom
        // override — guards a regression where the planner accidentally
        // emits the type's default rather than the configured value.
        #expect(informed.replacementFraction != CoveragePlanner.defaultReplacementFraction)
    }

    // MARK: - playhead-au2v.1.15 gap-fill tests
    //
    // au2v.1.14 already shipped happy-path / mode-gate / quality-gate /
    // confidence / projection / clamp / replacement-fraction /
    // periodic-rescan / nil-end / defensive-filtering / order-preserving
    // tests. The cases below fill the residual gaps the bead-15 spec
    // enumerates:
    //
    //   1. `.off` + non-empty evidence parity (companion to the
    //      pre-existing `.shadow` parity test).
    //   2. The canonical 5-chapter scenario (2 ad: 0.6/0.8, 2 content:
    //      0.9/0.5, 1 ambiguous) with strict-`>` gate semantics.
    //   3. `replacementFraction` boundary values 0.0 and 1.0.
    //   4. `planConfidence` boundary at exactly `minPlanConfidence`
    //      (the gate is `>=`).
    //   5. Source-agnostic parity extended to `.id3` and `.rssInline`
    //      (au2v.1.14 only covered `.pc20` vs `.inferred`).
    //   6. Empty `[]` evidence (distinct from `nil`).
    //   7. Pin behaviour for sparse coverage of the episode (planner
    //      does NOT consult episode duration).

    @Test("chapter-informed: .off mode + non-empty evidence yields parity with baseline (companion to shadow parity)")
    func testChapterInformedOffModeWithEvidenceParity() {
        let planner = CoveragePlanner()
        let chapters = [
            adChapter(start: 60, end: 120, quality: 0.9),
            contentChapter(start: 200, end: 600, quality: 0.85)
        ]
        let baseline = planner.plan(for: matureStableContext())
        let off = planner.plan(for: matureStableContext(
            chapterSignalMode: .off,
            chapterEvidence: chapters
        ))
        // Byte-identical observable plan — `.off` must NOT consult
        // chapter evidence even when it's available, mirroring the
        // existing `.shadow` parity contract.
        #expect(off.policy == baseline.policy)
        #expect(off.phases == baseline.phases)
        #expect(off.auditWindowSampleRate == baseline.auditWindowSampleRate)
        #expect(off.chapterInformedAudit == nil)
        // Diagnostic: `.skipped(modeDisabled)` with the FULL evidence
        // count — the planner short-circuits before filtering, so
        // `evidenceCount` reflects the raw input (chapters.count == 2),
        // not the post-filter count.
        switch off.chapterAuditDiagnostic {
        case .skipped(let reason, let count):
            #expect(reason == .modeDisabled)
            #expect(count == chapters.count)
        default:
            Issue.record("Expected .skipped(modeDisabled, \(chapters.count)), got \(String(describing: off.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: canonical 5-chapter scenario (2 ad / 2 content / 1 ambiguous) honours strict-> gates")
    func testChapterInformedCanonicalFiveChapterScenario() throws {
        let planner = CoveragePlanner()
        // Canonical scenario from the bead spec:
        //   - 2 ad-disposition  : qualityScore 0.6 and 0.8 — both
        //                         clear the 0.4 strict-> include floor.
        //   - 2 content-disposition: qualityScore 0.9 (excluded; > 0.7)
        //                         and 0.5 (NOT excluded; gate is
        //                         strict-`>` so 0.5 passes through to
        //                         the consumer's random selection).
        //   - 1 ambiguous       : passes through (omitted from both
        //                         lists by design).
        // Plan-confidence math (duration-weighted; non-overlapping):
        //   ad   60→180  q=0.6  d=120 w= 72
        //   ad  200→320  q=0.8  d=120 w= 96
        //   ct  400→600  q=0.9  d=200 w=180
        //   ct  700→900  q=0.5  d=200 w=100
        //   amb 1000→1100 q=0.5 d=100 w= 50
        //   Σd = 740, Σw = 498  ⇒ planConfidence ≈ 0.673
        // Comfortably above the default 0.3 floor.
        let chapters: [ChapterEvidence] = [
            adChapter(start: 60, end: 180, quality: 0.6),
            adChapter(start: 200, end: 320, quality: 0.8),
            contentChapter(start: 400, end: 600, quality: 0.9),
            contentChapter(start: 700, end: 900, quality: 0.5),
            ambiguousChapter(start: 1000, end: 1100, quality: 0.5)
        ]
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        ))
        let informed = try #require(plan.chapterInformedAudit)
        // Both ad chapters appear in includes, in input order.
        #expect(informed.includes.count == 2)
        #expect(informed.includes[0].kind == .adChapter)
        #expect(informed.includes[0].qualityScore == 0.6)
        #expect(informed.includes[0].startTime == 60)
        #expect(informed.includes[1].qualityScore == 0.8)
        #expect(informed.includes[1].startTime == 200)
        // Only the high-quality content chapter appears in excludes —
        // the 0.5-quality content chapter does NOT (gate is strict-`>`).
        #expect(informed.excludes.count == 1)
        #expect(informed.excludes[0].kind == .contentExcluded)
        #expect(informed.excludes[0].qualityScore == 0.9)
        #expect(informed.excludes[0].startTime == 400)
        // evidenceCount counts every supplied chapter (including the
        // ambiguous and the 0.5-quality content one) so the diagnostic
        // can be reproduced from the inputs alone.
        #expect(informed.evidenceCount == chapters.count)
        // planConfidence is finite, in [0,1], and clears the default
        // floor by a sensible margin — assert the documented formula
        // numerically rather than the >= floor alone, otherwise a
        // regression that drifted the formula by 10% could still pass.
        #expect(abs(informed.planConfidence - (498.0 / 740.0)) < 1e-9)
        #expect(informed.planConfidence > CoveragePlanner.defaultMinPlanConfidence)
    }

    @Test("chapter-informed: replacementFraction == 0.0 propagates verbatim onto the selection")
    func testChapterInformedReplacementFractionZero() throws {
        // Bead-15 boundary: 0.0 means "no replacement" — the audit
        // window narrower (later bead) will multiply slot count by
        // this and produce zero replaced slots. The PLANNER's only
        // job is to faithfully propagate the configured value.
        let planner = CoveragePlanner(replacementFraction: 0.0)
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: [adChapter(start: 60, end: 180, quality: 0.9)]
        ))
        let informed = try #require(plan.chapterInformedAudit)
        #expect(informed.replacementFraction == 0.0)
        // The planner must NOT short-circuit when the fraction is 0 —
        // emitting the selection (and the diagnostic) is still useful
        // signal even if the consumer ultimately allocates zero
        // chapter-informed slots.
        #expect(!informed.includes.isEmpty)
    }

    @Test("chapter-informed: replacementFraction == 1.0 propagates verbatim onto the selection")
    func testChapterInformedReplacementFractionOne() throws {
        // Bead-15 boundary: 1.0 means "all slots may be replaced" —
        // identical propagation contract as 0.0; just on the other
        // end of the clamp range.
        let planner = CoveragePlanner(replacementFraction: 1.0)
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: [adChapter(start: 60, end: 180, quality: 0.9)]
        ))
        let informed = try #require(plan.chapterInformedAudit)
        #expect(informed.replacementFraction == 1.0)
    }

    @Test("chapter-informed: planConfidence == minPlanConfidence is on the inclusive side of the gate (>= semantics)")
    func testChapterInformedPlanConfidenceBoundaryInclusive() throws {
        // The planner gates the chapter-informed path on
        //   `planConfidence >= minPlanConfidence`.
        // Pin the boundary: with `minPlanConfidence = 0.5` and a single
        // ad chapter at qualityScore 0.5, computePlanConfidence returns
        // exactly 0.5 (single chapter ⇒ weighted average = quality).
        // The chapter clears the 0.4 strict-> include floor (0.5 > 0.4),
        // so it survives filtering. The boundary case `0.5 >= 0.5` MUST
        // produce `.informed`, not `.skipped(lowPlanConfidence)`.
        let planner = CoveragePlanner(minPlanConfidence: 0.5)
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: [adChapter(start: 60, end: 180, quality: 0.5)]
        ))
        let informed = try #require(
            plan.chapterInformedAudit,
            "planConfidence == minPlanConfidence must use the plan (>= semantics)"
        )
        #expect(informed.planConfidence == 0.5)
    }

    @Test("chapter-informed: planConfidence just below minPlanConfidence falls back to random")
    func testChapterInformedPlanConfidenceJustBelowFloor() {
        // Companion to the boundary test: the same planner config with
        // a chapter whose quality is BELOW the floor must produce
        // `.skipped(lowPlanConfidence)`. Quality 0.45 still clears the
        // 0.4 strict-> include filter (so the gate is reached on
        // confidence, not on the include filter), but planConfidence
        // 0.45 fails the `0.45 >= 0.5` floor.
        let planner = CoveragePlanner(minPlanConfidence: 0.5)
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: [adChapter(start: 60, end: 180, quality: 0.45)]
        ))
        #expect(plan.chapterInformedAudit == nil)
        switch plan.chapterAuditDiagnostic {
        case .skipped(let reason, let count):
            #expect(reason == .lowPlanConfidence)
            #expect(count == 1)
        default:
            Issue.record("Expected .skipped(lowPlanConfidence), got \(String(describing: plan.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: every ChapterSource (id3/pc20/rssInline/inferred) yields identical selection output")
    func testChapterInformedSourceAgnosticAllSources() throws {
        // Bead-15 source-agnostic parity, extended from au2v.1.14's
        // pc20-vs-inferred coverage to ALL four `ChapterSource` cases.
        // The planner must NOT branch on source — `qualityScore` is
        // the only trust signal.
        let planner = CoveragePlanner()
        let plans = ChapterSource.allCases.map { source in
            planner.plan(for: matureStableContext(
                chapterSignalMode: .enabled,
                chapterEvidence: [adChapter(start: 60, end: 120, quality: 0.9, source: source)]
            ))
        }
        // Every source must yield a populated selection, and every
        // selection must equal the first one. Comparing all-vs-first
        // surfaces both "source X regresses" and "the test fixture
        // accidentally lined up". `ChapterSource.allCases` is the
        // belt-and-suspenders against a future case being added
        // without touching this test.
        #expect(ChapterSource.allCases.count >= 4,
                "Expected at least the four documented sources (id3/pc20/rssInline/inferred)")
        let first = try #require(plans.first?.chapterInformedAudit)
        for plan in plans {
            let informed = try #require(plan.chapterInformedAudit)
            #expect(informed == first)
            #expect(plan.chapterAuditDiagnostic == plans[0].chapterAuditDiagnostic)
        }
    }

    @Test("chapter-informed: empty `[]` chapter evidence is treated as no-evidence (distinct from nil)")
    func testChapterInformedEmptyArrayEvidence() {
        // `nil` chapterEvidence is covered by
        // `testChapterInformedNoEvidenceFallback`; this sibling test
        // pins the empty-array path, which threads through the same
        // `noChapterEvidence` skip reason but via a different code
        // line (`!chapters.isEmpty` rather than `let chapters = …`).
        let planner = CoveragePlanner()
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: []
        ))
        #expect(plan.chapterInformedAudit == nil)
        switch plan.chapterAuditDiagnostic {
        case .skipped(let reason, let count):
            #expect(reason == .noChapterEvidence)
            #expect(count == 0)
        default:
            Issue.record("Expected .skipped(noChapterEvidence, 0), got \(String(describing: plan.chapterAuditDiagnostic))")
        }
    }

    @Test("chapter-informed: planner does NOT consult episode duration — sparse coverage is honoured verbatim")
    func testChapterInformedSparseCoverageBehavior() throws {
        // The bead-15 spec asks us to PIN behaviour for chapter
        // evidence that covers <50% of the episode duration. Today's
        // production CoveragePlanner has no episode-duration input
        // (`CoveragePlannerContext` does not carry one), so it
        // CANNOT compute coverage at all. Behaviour is: it processes
        // whatever chapters it receives, wherever they fall on the
        // timeline. This test pins that contract so a future
        // duration-aware refactor must update both production code
        // and this assertion in lockstep.
        //
        // Concretely: a single 60-second ad chapter at the start of
        // an episode "covers" a tiny fraction of, say, an hour-long
        // episode. The planner still produces the chapter-informed
        // selection — it does not gate on coverage.
        let planner = CoveragePlanner()
        let plan = planner.plan(for: matureStableContext(
            chapterSignalMode: .enabled,
            chapterEvidence: [adChapter(start: 0, end: 60, quality: 0.9)]
        ))
        let informed = try #require(
            plan.chapterInformedAudit,
            "Sparse coverage must NOT cause the planner to fall back; episode duration is not an input"
        )
        #expect(informed.includes.count == 1)
        #expect(informed.includes.first?.endTime == 60)
        // Defensive: the planner does not synthesise a fictional
        // "uncovered region" exclude. Excludes are derived only from
        // high-quality content-disposition chapters, of which there
        // are none here.
        #expect(informed.excludes.isEmpty)
    }
}
