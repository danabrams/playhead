// ChapterPlanAssemblerTests.swift
// playhead-au2v.1.8: Unit tests for `ChapterPlanAssembler`.
//
// Coverage maps to the bead acceptance criteria:
//
// - 0% operational unclear → assembled normally; semantic unclears retained
//   as `.ambiguous` chapters with the FM-reported confidence preserved.
// - 35% operational unclear → abort signal; no plan; the result carries the
//   numbers the caller needs to emit
//   `chapter_phase_operational_unclear_rate_exceeded`.
// - 25% operational + 30% semantic → assembled (operational below 30%).
// - 60% total unclear (25% operational + 40% semantic) → assembled with
//   `highUnclearRateWarning` flagged so the caller can emit a warning
//   diagnostic.
// - Operational unclears REMOVED from the assembled plan (semantic
//   unclears KEPT with disposition=`.ambiguous`).
// - `planConfidence` duration-weighted: uniform 0.8 → 0.8;
//   `(0.9, 5min) + (0.1, 1min)` → ~0.767.
// - Threshold strictness: `> 0.3`, NOT `>= 0.3`. Exactly 30% does NOT
//   abort. Strictly above (e.g., 35%) does.
// - Empty input: assembled with `chapters == []`, `planConfidence == 0`
//   (no divide-by-zero).
// - Single-chapter inputs (operational, semantic, confident).
// - Zero-duration episode: doesn't divide by zero; surfaces a sane
//   confidence (0).

import Foundation
import Testing
@testable import Playhead

@Suite("ChapterPlanAssembler")
struct ChapterPlanAssemblerTests {

    // MARK: - Helpers

    /// Build a confident-success `LabelingResult` (no `failureMode`).
    private static func confident(
        start: TimeInterval,
        end: TimeInterval?,
        confidence: Double,
        disposition: ChapterDispositionRaw = .content,
        title: String? = nil
    ) -> LabelingResult {
        let evidence = ChapterEvidence(
            startTime: start,
            endTime: end,
            title: title,
            source: .inferred,
            disposition: disposition.mappedDisposition,
            qualityScore: Float(confidence)
        )
        return LabelingResult(
            chapter: evidence,
            labelDisposition: disposition,
            topicDescriptor: title,
            failureMode: nil,
            attempts: 1
        )
    }

    /// Build a semantic `.unclear` `LabelingResult`. The chapter is kept;
    /// disposition is `.ambiguous` and qualityScore is the FM-reported
    /// (typically low) confidence.
    private static func semanticUnclear(
        start: TimeInterval,
        end: TimeInterval?,
        confidence: Double,
        title: String? = nil
    ) -> LabelingResult {
        let evidence = ChapterEvidence(
            startTime: start,
            endTime: end,
            title: title,
            source: .inferred,
            disposition: .ambiguous,
            qualityScore: Float(confidence)
        )
        return LabelingResult(
            chapter: evidence,
            labelDisposition: .unclear,
            topicDescriptor: title,
            failureMode: .semantic,
            attempts: 1
        )
    }

    /// Build an operational-failure `LabelingResult`. Mirrors the shape
    /// `ChapterLabelingService.operationalResult(...)` produces:
    /// `disposition = .ambiguous`, `qualityScore = 0`.
    private static func operationalFailure(
        start: TimeInterval,
        end: TimeInterval?,
        attempts: Int = 2
    ) -> LabelingResult {
        let evidence = ChapterEvidence(
            startTime: start,
            endTime: end,
            title: nil,
            source: .inferred,
            disposition: .ambiguous,
            qualityScore: 0.0
        )
        return LabelingResult(
            chapter: evidence,
            labelDisposition: .unclear,
            topicDescriptor: nil,
            failureMode: .operational,
            attempts: attempts
        )
    }

    private static let assembler = ChapterPlanAssembler()
    private static let testHash = "test-content-hash"
    private static let fixedDate = Date(timeIntervalSince1970: 1_700_000_000)

    // MARK: - Empty / minimal input

    @Test("empty input → assembled with zero chapters and zero confidence (no divide-by-zero)")
    func emptyInput() {
        let result = Self.assembler.assemble(
            results: [],
            episodeContentHash: Self.testHash,
            candidatesDetected: 0,
            candidatesKept: 0,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, let warnings):
            #expect(plan.chapters.isEmpty)
            #expect(plan.planConfidence == 0.0)
            #expect(plan.episodeContentHash == Self.testHash)
            #expect(plan.generationDiagnostics.operationalUnclearCount == 0)
            #expect(plan.generationDiagnostics.semanticUnclearCount == 0)
            #expect(warnings.highUnclearRateExceeded == false)
        case .aborted:
            Issue.record("expected assembled, got aborted")
        }
    }

    @Test("single confident chapter → assembled; planConfidence equals chapter quality")
    func singleConfidentChapter() {
        let results = [
            Self.confident(start: 0, end: 60, confidence: 0.9, disposition: .content)
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 1,
            candidatesKept: 1,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            #expect(plan.chapters.count == 1)
            #expect(abs(plan.planConfidence - 0.9) < 1e-6)
        case .aborted:
            Issue.record("expected assembled, got aborted")
        }
    }

    @Test("single semantic unclear → kept in plan as .ambiguous with FM confidence")
    func singleSemanticUnclear() {
        let results = [
            Self.semanticUnclear(start: 0, end: 60, confidence: 0.2)
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 1,
            candidatesKept: 1,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, let warnings):
            #expect(plan.chapters.count == 1)
            #expect(plan.chapters[0].disposition == .ambiguous)
            #expect(abs(Double(plan.chapters[0].qualityScore) - 0.2) < 1e-6)
            #expect(plan.generationDiagnostics.semanticUnclearCount == 1)
            // 1/1 = 100% total unclear (operational below 30%, but
            // semantic alone pushes total over 50%). High-unclear
            // warning MUST trip even though we did NOT abort.
            #expect(warnings.highUnclearRateExceeded == true)
            #expect(warnings.totalUnclearCount == 1)
            #expect(abs(warnings.totalUnclearRate - 1.0) < 1e-6)
        case .aborted:
            Issue.record("expected assembled, got aborted")
        }
    }

    @Test("single operational unclear → REMOVED; plan is empty; rate (1/1=100%) triggers abort")
    func singleOperationalAborts() {
        // 1/1 = 100% > 30% → abort.
        let results = [
            Self.operationalFailure(start: 0, end: 60)
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 1,
            candidatesKept: 1,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .aborted(let info):
            #expect(info.labeledCount == 1)
            #expect(info.operationalUnclearCount == 1)
            #expect(abs(info.operationalUnclearRate - 1.0) < 1e-6)
            #expect(info.threshold == ChapterPlanAssembler.operationalUnclearRateThreshold)
        case .assembled:
            Issue.record("expected aborted, got assembled")
        }
    }

    // MARK: - 0% operational

    @Test("0% operational, all confident → assembled normally; planConfidence is duration-weighted")
    func allConfidentZeroOperational() {
        let results = [
            Self.confident(start: 0, end: 60, confidence: 0.8),
            Self.confident(start: 60, end: 120, confidence: 0.8),
            Self.confident(start: 120, end: 180, confidence: 0.8),
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 3,
            candidatesKept: 3,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, let warnings):
            #expect(plan.chapters.count == 3)
            #expect(abs(plan.planConfidence - 0.8) < 1e-6)
            #expect(warnings.highUnclearRateExceeded == false)
            #expect(plan.generationDiagnostics.operationalUnclearCount == 0)
            #expect(plan.generationDiagnostics.semanticUnclearCount == 0)
        case .aborted:
            Issue.record("expected assembled, got aborted")
        }
    }

    @Test("0% operational, mixed semantic + confident → assembled; semantic kept as .ambiguous")
    func mixedSemanticAndConfident() {
        let results = [
            Self.confident(start: 0, end: 60, confidence: 0.9, disposition: .hostReadAd),
            Self.semanticUnclear(start: 60, end: 120, confidence: 0.3),
            Self.confident(start: 120, end: 180, confidence: 0.85, disposition: .content),
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 3,
            candidatesKept: 3,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            #expect(plan.chapters.count == 3)
            #expect(plan.chapters[0].disposition == .adBreak)
            #expect(plan.chapters[1].disposition == .ambiguous)
            #expect(plan.chapters[2].disposition == .content)
            #expect(plan.generationDiagnostics.semanticUnclearCount == 1)
            #expect(plan.generationDiagnostics.operationalUnclearCount == 0)
        case .aborted:
            Issue.record("expected assembled, got aborted")
        }
    }

    // MARK: - Threshold strictness (> 0.3, NOT >= 0.3)

    @Test("exactly 30% operational → does NOT abort (threshold is strict `>`, not `>=`)")
    func exactly30PercentDoesNotAbort() {
        // 3/10 = exactly 30%; must NOT trip the gate.
        let confidentChapters = (0..<7).map { i in
            Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.7
            )
        }
        let opChapters = (7..<10).map { i in
            Self.operationalFailure(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60)
            )
        }
        let results = confidentChapters + opChapters
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 10,
            candidatesKept: 10,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            // Operational unclears are REMOVED → 7 chapters left.
            #expect(plan.chapters.count == 7)
            #expect(plan.generationDiagnostics.operationalUnclearCount == 3)
        case .aborted:
            Issue.record("30% should not abort (strict `>`); got aborted")
        }
    }

    @Test("35% operational → ABORTS; no plan; abort info carries the rate")
    func thirtyFivePercentAborts() {
        // 7/20 = 35%; > 30% → abort.
        let confidentChapters = (0..<13).map { i in
            Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.8
            )
        }
        let opChapters = (13..<20).map { i in
            Self.operationalFailure(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60)
            )
        }
        let results = confidentChapters + opChapters
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 20,
            candidatesKept: 20,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .aborted(let info):
            #expect(info.labeledCount == 20)
            #expect(info.operationalUnclearCount == 7)
            #expect(abs(info.operationalUnclearRate - 0.35) < 1e-6)
            #expect(info.threshold == 0.30)
        case .assembled:
            Issue.record("35% must abort; got assembled")
        }
    }

    @Test("50% operational → ABORTS")
    func fiftyPercentAborts() {
        let confidentChapters = (0..<5).map { i in
            Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.8
            )
        }
        let opChapters = (5..<10).map { i in
            Self.operationalFailure(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60)
            )
        }
        let result = Self.assembler.assemble(
            results: confidentChapters + opChapters,
            episodeContentHash: Self.testHash,
            candidatesDetected: 10,
            candidatesKept: 10,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .aborted(let info):
            #expect(abs(info.operationalUnclearRate - 0.5) < 1e-6)
        case .assembled:
            Issue.record("50% must abort")
        }
    }

    // MARK: - Mixed operational + semantic

    @Test("25% operational + 30% semantic → assembled (operational below abort threshold)")
    func twentyFivePctOpThirtyPctSemantic() {
        // 20 results: 5 operational (25%), 6 semantic (30%), 9 confident.
        // Operational < 30% → no abort.
        // Total unclear (op + semantic) / total = 11/20 = 55% → high warning.
        var results: [LabelingResult] = []
        for i in 0..<9 {
            results.append(Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.85
            ))
        }
        for i in 9..<15 {
            results.append(Self.semanticUnclear(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.2
            ))
        }
        for i in 15..<20 {
            results.append(Self.operationalFailure(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60)
            ))
        }
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 20,
            candidatesKept: 20,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, let warnings):
            // Operational removed → 9 confident + 6 semantic = 15 chapters.
            #expect(plan.chapters.count == 15)
            #expect(plan.generationDiagnostics.operationalUnclearCount == 5)
            #expect(plan.generationDiagnostics.semanticUnclearCount == 6)
            // 11/20 = 55% > 50% → warning.
            #expect(warnings.highUnclearRateExceeded == true)
        case .aborted:
            Issue.record("expected assembled, got aborted")
        }
    }

    @Test("60% total unclear (25% op + 40% semantic but op below 30%) → assembled with warning")
    func sixtyPctUnclearWarning() {
        // 20 results: 5 operational (25%), 8 semantic (40%), 7 confident.
        // 25% op below abort. 13/20 = 65% total unclear → warning.
        var results: [LabelingResult] = []
        for i in 0..<7 {
            results.append(Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.85
            ))
        }
        for i in 7..<15 {
            results.append(Self.semanticUnclear(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.15
            ))
        }
        for i in 15..<20 {
            results.append(Self.operationalFailure(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60)
            ))
        }
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 20,
            candidatesKept: 20,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, let warnings):
            #expect(plan.chapters.count == 15) // 7 confident + 8 semantic
            #expect(warnings.highUnclearRateExceeded == true)
            #expect(warnings.totalUnclearCount == 13)
            #expect(warnings.labeledCount == 20)
        case .aborted:
            Issue.record("expected assembled (op below threshold)")
        }
    }

    @Test("exactly 50% total unclear → does NOT trip the high-unclear warning (strict `>`)")
    func exactlyFiftyPercentNoWarning() {
        // 10 results: 0 op, 5 semantic, 5 confident → 50% total unclear.
        // Threshold is `> 0.50`, so equality must not warn.
        var results: [LabelingResult] = []
        for i in 0..<5 {
            results.append(Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.85
            ))
        }
        for i in 5..<10 {
            results.append(Self.semanticUnclear(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.2
            ))
        }
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 10,
            candidatesKept: 10,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(_, let warnings):
            #expect(warnings.highUnclearRateExceeded == false)
        case .aborted:
            Issue.record("50% total unclear must not abort or trip warning")
        }
    }

    // MARK: - planConfidence math

    @Test("uniform 0.8 quality across mixed durations → planConfidence == 0.8")
    func uniformQuality() {
        let results = [
            Self.confident(start: 0, end: 60, confidence: 0.8),
            Self.confident(start: 60, end: 600, confidence: 0.8),
            Self.confident(start: 600, end: 900, confidence: 0.8),
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 3,
            candidatesKept: 3,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            #expect(abs(plan.planConfidence - 0.8) < 1e-6)
        case .aborted:
            Issue.record("expected assembled")
        }
    }

    @Test("(0.9, 5min) + (0.1, 1min) → planConfidence ≈ 0.767")
    func mixedQualityDurationWeighted() {
        // Per the bead spec: a long high-confidence chapter dominates a
        // short low-confidence one.
        // (0.9 * 300 + 0.1 * 60) / 360 = (270 + 6) / 360 = 276 / 360 = 0.7667.
        let results = [
            Self.confident(start: 0, end: 300, confidence: 0.9),
            Self.confident(start: 300, end: 360, confidence: 0.1),
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 2,
            candidatesKept: 2,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            let expected = (0.9 * 300.0 + 0.1 * 60.0) / 360.0
            #expect(abs(plan.planConfidence - expected) < 1e-6)
            #expect(abs(plan.planConfidence - 0.7666666) < 1e-3)
        case .aborted:
            Issue.record("expected assembled")
        }
    }

    @Test("zero-duration episode (all chapters malformed end ≤ start) → planConfidence is 0, no NaN")
    func zeroDurationEpisode() {
        // Build chapters with end == start so effectiveDuration is 0
        // for every chapter; the plan-confidence guard must return 0
        // rather than dividing by zero.
        let evidence = [
            ChapterEvidence(
                startTime: 100, endTime: 100, title: nil,
                source: .inferred, disposition: .content, qualityScore: 0.9
            ),
        ]
        let results = [
            LabelingResult(
                chapter: evidence[0],
                labelDisposition: .content,
                topicDescriptor: nil,
                failureMode: nil,
                attempts: 1
            )
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 1,
            candidatesKept: 1,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            #expect(plan.planConfidence.isFinite)
            #expect(plan.planConfidence == 0.0)
        case .aborted:
            Issue.record("expected assembled")
        }
    }

    // MARK: - Operational unclears removed (re-indexing semantics)

    @Test("operational unclears are REMOVED from final plan; semantic and confident retained in original order")
    func operationalRemoved() {
        // Sequence: confident, op, semantic, op, confident
        // After removal: confident, semantic, confident (3 chapters,
        // preserving original startTime / endTime / title).
        let r0 = Self.confident(start: 0, end: 60, confidence: 0.9, title: "ch0")
        let r1 = Self.operationalFailure(start: 60, end: 120)
        let r2 = Self.semanticUnclear(start: 120, end: 180, confidence: 0.2, title: "ch2")
        let r3 = Self.operationalFailure(start: 180, end: 240)
        let r4 = Self.confident(start: 240, end: 300, confidence: 0.85, title: "ch4")

        // 2/5 operational = 40% > 30% → would abort. Adjust by adding
        // confident chapters so the rate drops below 30%.
        // 2/8 = 25% → assembled.
        let extras = (5..<8).map { i in
            Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.8,
                title: "ex\(i)"
            )
        }
        let result = Self.assembler.assemble(
            results: [r0, r1, r2, r3, r4] + extras,
            episodeContentHash: Self.testHash,
            candidatesDetected: 8,
            candidatesKept: 8,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            // 8 - 2 operational = 6 chapters retained.
            #expect(plan.chapters.count == 6)
            // None of the kept chapters should be the operational ones
            // (originally at 60-120 and 180-240).
            for chapter in plan.chapters {
                let isOp = (chapter.startTime == 60 && chapter.endTime == 120) ||
                           (chapter.startTime == 180 && chapter.endTime == 240)
                #expect(isOp == false)
            }
            // Semantic chapter (originally at 120-180) IS retained.
            let semantic = plan.chapters.first { $0.startTime == 120 }
            #expect(semantic != nil)
            #expect(semantic?.disposition == .ambiguous)
        case .aborted:
            Issue.record("expected assembled (op rate 25%)")
        }
    }

    @Test("operational rows are EXCLUDED from planConfidence (their qualityScore=0 does not drag the average down)")
    func operationalRowsExcludedFromConfidence() {
        // 10 chapters: 2 operational (20%, below abort threshold) + 8
        // confident at uniform quality 0.9 with uniform 60s durations.
        // Operational rows have qualityScore=0; if they leaked into the
        // confidence computation, the duration-weighted average would
        // be (8 × 0.9 × 60 + 2 × 0 × 60) / (10 × 60) = 0.72.
        // After dropping operational rows, the average over the 8 kept
        // chapters is 0.9. Pinning that distinguishes "filter then
        // compute" from "compute then filter".
        let opChapters = (0..<2).map { i in
            Self.operationalFailure(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60)
            )
        }
        let confidentChapters = (2..<10).map { i in
            Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.9
            )
        }
        let result = Self.assembler.assemble(
            results: opChapters + confidentChapters,
            episodeContentHash: Self.testHash,
            candidatesDetected: 10,
            candidatesKept: 10,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            #expect(plan.chapters.count == 8)
            #expect(abs(plan.planConfidence - 0.9) < 1e-6)
        case .aborted:
            Issue.record("20% operational must not abort")
        }
    }

    @Test("removal preserves order; first kept chapter is the first non-operational result")
    func removalPreservesOrder() {
        // 3 op + 7 confident = 30% (does NOT abort under strict `>`).
        let opFirst = (0..<3).map { i in
            Self.operationalFailure(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60)
            )
        }
        let confidentRest = (3..<10).map { i in
            Self.confident(
                start: TimeInterval(i * 60),
                end: TimeInterval((i + 1) * 60),
                confidence: 0.8,
                title: "c\(i)"
            )
        }
        let result = Self.assembler.assemble(
            results: opFirst + confidentRest,
            episodeContentHash: Self.testHash,
            candidatesDetected: 10,
            candidatesKept: 10,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            #expect(plan.chapters.count == 7)
            #expect(plan.chapters.first?.title == "c3")
            #expect(plan.chapters.last?.title == "c9")
        case .aborted:
            Issue.record("30% must not abort (strict >)")
        }
    }

    // MARK: - Diagnostics counts

    @Test("generationDiagnostics carries accurate operational/semantic counts and survives Codable")
    func diagnosticsCounts() throws {
        let results = [
            Self.confident(start: 0, end: 60, confidence: 0.8),
            Self.confident(start: 60, end: 120, confidence: 0.8),
            Self.confident(start: 120, end: 180, confidence: 0.8),
            Self.semanticUnclear(start: 180, end: 240, confidence: 0.2),
            Self.semanticUnclear(start: 240, end: 300, confidence: 0.3),
            Self.operationalFailure(start: 300, end: 360),
        ]
        let result = Self.assembler.assemble(
            results: results,
            episodeContentHash: Self.testHash,
            candidatesDetected: 8,
            candidatesKept: 6,
            generatedAt: Self.fixedDate
        )
        switch result {
        case .assembled(let plan, _):
            #expect(plan.generationDiagnostics.operationalUnclearCount == 1)
            #expect(plan.generationDiagnostics.semanticUnclearCount == 2)
            #expect(plan.generationDiagnostics.candidatesDetected == 8)
            #expect(plan.generationDiagnostics.candidatesKept == 6)
            // Codable round-trip.
            let data = try JSONEncoder().encode(plan)
            let decoded = try JSONDecoder().decode(ChapterPlan.self, from: data)
            #expect(decoded == plan)
        case .aborted:
            Issue.record("expected assembled")
        }
    }

    // MARK: - Threshold constants

    @Test("threshold constants are pinned at 0.30 and 0.50")
    func thresholdConstants() {
        #expect(ChapterPlanAssembler.operationalUnclearRateThreshold == 0.30)
        #expect(ChapterPlanAssembler.highUnclearRateWarningThreshold == 0.50)
    }
}

// MARK: - High-unclear-rate diagnostic event

@Suite("ChapterPhaseEvent.highUnclearRate")
struct ChapterPhaseEventHighUnclearRateTests {

    @Test("event factory produces correct event_type and snake_case payload keys")
    func factoryShape() throws {
        let installID = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        let event = ChapterPhaseEvent.highUnclearRate(
            installID: installID,
            episodeId: "episode-abc",
            timestamp: 1_700_000_000.0,
            labeledCount: 20,
            operationalUnclearCount: 5,
            semanticUnclearCount: 8,
            totalUnclearRate: 0.65,
            threshold: 0.50
        )
        #expect(event.eventType == .highUnclearRate)
        guard case let .highUnclearRate(payload) = event.payload else {
            Issue.record("expected highUnclearRate payload")
            return
        }
        #expect(payload.labeledCount == 20)
        #expect(payload.operationalUnclearCount == 5)
        #expect(payload.semanticUnclearCount == 8)
        #expect(abs(payload.totalUnclearRate - 0.65) < 1e-6)
        #expect(payload.threshold == 0.50)

        // Wire-shape pin: encoded JSON uses snake_case for the payload
        // discriminator and field names.
        let data = try JSONEncoder().encode(event)
        let json = String(data: data, encoding: .utf8) ?? ""
        #expect(json.contains("\"event_type\":\"chapter_phase_high_unclear_rate\""))
        #expect(json.contains("\"high_unclear_rate\""))
        #expect(json.contains("\"labeled_count\":20"))
        #expect(json.contains("\"operational_unclear_count\":5"))
        #expect(json.contains("\"semantic_unclear_count\":8"))
        #expect(json.contains("\"total_unclear_rate\""))
        #expect(json.contains("\"threshold\""))
    }

    @Test("highUnclearRate event round-trips through Codable")
    func codableRoundTrip() throws {
        let installID = UUID(uuidString: "00000000-0000-0000-0000-000000000002")!
        let event = ChapterPhaseEvent.highUnclearRate(
            installID: installID,
            episodeId: "episode-xyz",
            timestamp: 1_700_000_500.0,
            labeledCount: 10,
            operationalUnclearCount: 2,
            semanticUnclearCount: 4,
            totalUnclearRate: 0.6,
            threshold: 0.5
        )
        let data = try JSONEncoder().encode(event)
        let decoded = try JSONDecoder().decode(ChapterPhaseEvent.self, from: data)
        #expect(decoded == event)
    }
}
