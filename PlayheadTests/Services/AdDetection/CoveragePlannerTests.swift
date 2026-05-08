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
        // None of these are usable: NaN start, end-before-start,
        // non-finite end, NaN quality. Keep the gate honest by
        // mixing in a single ambiguous chapter so `chapterEvidence`
        // is non-empty but produces zero usable intervals.
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
    }
}
